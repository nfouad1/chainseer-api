"""Monitoring, calibration, and pre-trade controls for Chainseer.

This module deliberately contains no wallet, private-key, transaction-signing,
or broadcast capability.  Its strongest action is to emit a short-lived,
Timechain-sealed TradePermit after a fresh, block-pinned revalidation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import secrets
import socket
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


CONTROL_SCHEMA_VERSION = "1.0"
PERMIT_VERSION = "1.0"
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
TX_HASH_RE = re.compile(r"^0x[a-fA-F0-9]{64}$")
ZERO_ADDRESS = "0x" + "0" * 40
DEAD_ADDRESS = "0x" + "0" * 36 + "dead"

TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
OWNERSHIP_TRANSFERRED_TOPIC = (
    "0x8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0"
)
UPGRADED_TOPIC = (
    "0xbc7cd75a20ee27fd9adebab32041f755214dbc6bffa90cc0225b39da2e5c2d3b"
)
ADMIN_CHANGED_TOPIC = (
    "0x7e644d79422f17c01e4894b5f4f588d331ebfa28653d42ae832dc59e38c9798f"
)
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
BASE58_INDEX = {char: index for index, char in enumerate(BASE58_ALPHABET)}


def utc_now_iso(now: float | None = None) -> str:
    return datetime.fromtimestamp(
        time.time() if now is None else now, tz=timezone.utc
    ).isoformat()


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) else default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _parse_time(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    os.replace(temp, path)


def _topic_address(address: str) -> str:
    return "0x" + address.lower().replace("0x", "").zfill(64)


def _is_solana_pubkey(value: str) -> bool:
    """Validate canonical 32-byte base58 keys without importing an adapter."""
    text = str(value or "").strip()
    if not SOLANA_ADDRESS_RE.fullmatch(text):
        return False
    number = 0
    try:
        for char in text:
            number = number * 58 + BASE58_INDEX[char]
    except KeyError:
        return False
    raw = (
        number.to_bytes((number.bit_length() + 7) // 8, "big")
        if number
        else b""
    )
    leading_zeroes = len(text) - len(text.lstrip("1"))
    return len(b"\x00" * leading_zeroes + raw) == 32


def normalize_cross_chain_records(records: Any) -> list[dict[str, Any]]:
    """Validate provider-attested cross-chain records without overstating them."""
    if not isinstance(records, list):
        return []
    normalized = []
    for item in records[:100]:
        if not isinstance(item, dict):
            continue
        source_chain = str(item.get("source_chain") or "").strip()
        destination_chain = str(item.get("destination_chain") or "").strip()
        source_tx = str(item.get("source_tx_hash") or "").strip()
        destination_tx = str(item.get("destination_tx_hash") or "").strip()
        provider = str(item.get("provider") or "").strip()
        if not source_chain or not destination_chain or not provider:
            continue
        confidence = max(0.0, min(1.0, _safe_float(item.get("confidence"))))
        normalized.append(
            {
                "source_chain": source_chain,
                "destination_chain": destination_chain,
                "source_tx_hash": source_tx if TX_HASH_RE.fullmatch(source_tx) else None,
                "destination_tx_hash": (
                    destination_tx
                    if TX_HASH_RE.fullmatch(destination_tx)
                    else None
                ),
                "bridge": str(item.get("bridge") or "unknown"),
                "asset": str(item.get("asset") or "unknown"),
                "amount_usd": max(0.0, _safe_float(item.get("amount_usd"))),
                "observed_at": str(item.get("observed_at") or ""),
                "provider": provider,
                "confidence": confidence,
                "evidence_status": (
                    "provider_attested"
                    if source_tx or destination_tx
                    else "context_only"
                ),
            }
        )
    return normalized


def build_extended_evidence(
    data: dict[str, Any],
    *,
    primary_chain_id: str = "robinhood",
) -> dict[str, Any]:
    """Build separate low-trust, cross-chain, and execution evidence channels."""
    dexscreener = data.get("dexscreener") or {}
    pairs = dexscreener.get("pairs") or []
    local_pairs = [
        pair
        for pair in pairs
        if str(pair.get("chainId") or "").lower() == primary_chain_id.lower()
    ]
    primary = max(
        local_pairs or [{}],
        key=lambda pair: _safe_float((pair.get("liquidity") or {}).get("usd")),
    )
    info = primary.get("info") or {}
    socials = info.get("socials") or []
    websites = info.get("websites") or []
    provider_kol = data.get("social_kol_records") or []
    normalized_kol = []
    for record in provider_kol[:50] if isinstance(provider_kol, list) else []:
        if not isinstance(record, dict):
            continue
        normalized_kol.append(
            {
                "platform": str(record.get("platform") or "unknown"),
                "account": str(record.get("account") or ""),
                "observed_at": str(record.get("observed_at") or ""),
                "provider": str(record.get("provider") or "unknown"),
                "confidence": max(
                    0.0, min(1.0, _safe_float(record.get("confidence")))
                ),
                "engagement": max(0, _safe_int(record.get("engagement"))),
                "evidence_status": "provider_attested",
            }
        )

    boosts = max(0, _safe_int((primary.get("boosts") or {}).get("active")))
    bounded_social_score = min(
        70,
        50
        + min(8, len(socials) * 2)
        + min(4, len(websites) * 2)
        + min(8, boosts),
    )
    social = {
        "status": "observed" if socials or websites or normalized_kol else "limited",
        "trust": "low",
        "bounded_score": bounded_social_score,
        "channels": [
            {
                "type": str(item.get("type") or "unknown"),
                "url": str(item.get("url") or ""),
            }
            for item in socials[:10]
            if isinstance(item, dict)
        ],
        "websites": [
            str(item.get("url") or "")
            for item in websites[:10]
            if isinstance(item, dict)
        ],
        "dexscreener_boosts": boosts,
        "kol_records": normalized_kol,
        "can_trigger_hard_stop": False,
        "caveat": (
            "Social and KOL attention is manipulable context. It cannot override "
            "on-chain hard stops or independently establish legitimacy."
        ),
    }

    foreign_pairs = [
        pair
        for pair in pairs
        if str(pair.get("chainId") or "").lower() != primary_chain_id.lower()
    ]
    foreign_by_chain: dict[str, dict[str, Any]] = {}
    for pair in foreign_pairs:
        chain = str(pair.get("chainId") or "unknown")
        bucket = foreign_by_chain.setdefault(
            chain,
            {"chain": chain, "pairs": 0, "liquidity_usd": 0.0, "volume_24h_usd": 0.0},
        )
        bucket["pairs"] += 1
        bucket["liquidity_usd"] += _safe_float(
            (pair.get("liquidity") or {}).get("usd")
        )
        bucket["volume_24h_usd"] += _safe_float(
            (pair.get("volume") or {}).get("h24")
        )
    flow_records = normalize_cross_chain_records(
        data.get("cross_chain_flow_records")
    )
    cross_chain = {
        "status": (
            "provider_attested"
            if flow_records
            else "market_context_only" if foreign_pairs else "not_observed"
        ),
        "foreign_markets": sorted(
            foreign_by_chain.values(),
            key=lambda item: item["liquidity_usd"],
            reverse=True,
        ),
        "flow_records": flow_records,
        "verified_flow_count": sum(
            item["evidence_status"] == "provider_attested"
            and item["confidence"] >= 0.8
            for item in flow_records
        ),
        "can_trigger_hard_stop": False,
        "caveat": (
            "A same-address market on another chain is not proof of bridged "
            "fund flow. Flow records remain provider-attested until both chain "
            "transactions are independently replayed."
        ),
    }

    mev = {
        "status": "pre_trade_quote_required",
        "risk_level": "Indeterminate",
        "hard_stops": [],
        "warnings": [
            "Static token analysis cannot determine route-specific sandwich exposure."
        ],
        "required_pre_trade_inputs": [
            "observed_block",
            "pair_address",
            "amount_in",
            "amount_out",
            "min_amount_out",
            "price_impact_bps",
            "slippage_bps",
            "route",
        ],
        "scoring_scope": "execution_risk_only",
    }
    return {
        "social_attention": social,
        "cross_chain": cross_chain,
        "mev_exposure": mev,
    }


class MEVExposureAssessor:
    """Assess quote-specific execution risk without changing token legitimacy."""

    REQUIRED = {
        "observed_block",
        "pair_address",
        "amount_in",
        "amount_out",
        "min_amount_out",
        "price_impact_bps",
        "slippage_bps",
        "route",
        "source",
    }

    @classmethod
    def assess(
        cls,
        quote: dict[str, Any],
        *,
        validated_block: int,
        max_quote_age_blocks: int = 2,
        private_routing: bool = False,
    ) -> dict[str, Any]:
        if not isinstance(quote, dict):
            return {
                "status": "invalid",
                "risk_level": "Critical",
                "score": 0,
                "hard_stops": ["EXECUTABLE_QUOTE_MISSING"],
                "warnings": [],
            }
        missing = sorted(cls.REQUIRED - set(quote))
        hard_stops: list[str] = []
        warnings: list[str] = []
        if missing:
            hard_stops.append("QUOTE_FIELDS_MISSING:" + ",".join(missing))
        quote_block = _safe_int(quote.get("observed_block"), -1)
        age = validated_block - quote_block
        if quote_block < 0 or age < 0 or age > max_quote_age_blocks:
            hard_stops.append("QUOTE_STALE_OR_FUTURE")
        price_impact = max(0.0, _safe_float(quote.get("price_impact_bps")))
        slippage = max(0.0, _safe_float(quote.get("slippage_bps")))
        amount_out = _safe_float(quote.get("amount_out"))
        min_out = _safe_float(quote.get("min_amount_out"))
        if amount_out <= 0 or min_out <= 0 or min_out > amount_out:
            hard_stops.append("QUOTE_OUTPUT_INVALID")
        if price_impact > 1_000:
            hard_stops.append("PRICE_IMPACT_EXTREME")
        elif price_impact > 300:
            warnings.append("Price impact exceeds 3%.")
        if slippage > 1_000:
            hard_stops.append("SLIPPAGE_EXTREME")
        elif slippage > 300:
            warnings.append("Slippage tolerance increases sandwich exposure.")
        if not private_routing:
            warnings.append(
                "Public routing may expose the transaction to sandwiching or back-running."
            )
        penalty = min(70.0, price_impact / 20.0 + slippage / 40.0)
        if not private_routing:
            penalty += 10
        score = max(0, round(100 - penalty))
        if hard_stops:
            risk = "Critical"
        elif score < 50:
            risk = "High"
        elif score < 75:
            risk = "Medium"
        else:
            risk = "Low"
        return {
            "status": "complete",
            "risk_level": risk,
            "score": score,
            "hard_stops": hard_stops,
            "warnings": warnings,
            "quote_age_blocks": age,
            "private_routing": bool(private_routing),
            "scoring_scope": "execution_risk_only",
        }


@dataclass(frozen=True)
class WatchConfig:
    confirmations: int = 2
    poll_seconds: float = 3.0
    holder_rescan_blocks: int = 12
    sellability_rescan_blocks: int = 5
    max_rescan_blocks: int = 120
    score_alert_delta: float = 5.0
    outcome_baseline_interval_seconds: int = 24 * 3600
    outcome_horizons_seconds: tuple[int, ...] = (
        3600,
        6 * 3600,
        24 * 3600,
        7 * 24 * 3600,
        30 * 24 * 3600,
    )

    def __post_init__(self) -> None:
        if self.confirmations < 0:
            raise ValueError("confirmations cannot be negative")
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        if (
            self.holder_rescan_blocks < 1
            or self.sellability_rescan_blocks < 1
            or self.max_rescan_blocks < 1
        ):
            raise ValueError("rescan block intervals must be positive")
        if self.outcome_baseline_interval_seconds < 3600:
            raise ValueError("outcome baseline interval must be at least one hour")


@dataclass(frozen=True)
class SolanaWatchConfig:
    """Noise-aware trigger thresholds for confirmed Solana observations."""

    poll_seconds: float = 3.0
    signature_limit: int = 25
    minimum_rescan_slots: int = 8
    sellability_probe_slots: int = 75
    max_reconcile_slots: int = 900
    liquidity_delta_bps: int = 1_500
    price_delta_bps: int = 2_500
    holder_delta_bps: int = 500
    recent_signature_capacity: int = 100

    def __post_init__(self) -> None:
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        if not 1 <= self.signature_limit <= 1_000:
            raise ValueError("signature_limit must be between 1 and 1000")
        if (
            self.minimum_rescan_slots < 1
            or self.sellability_probe_slots < 1
            or self.max_reconcile_slots < 1
        ):
            raise ValueError("Solana rescan slot intervals must be positive")
        if self.minimum_rescan_slots > self.max_reconcile_slots:
            raise ValueError(
                "minimum_rescan_slots cannot exceed max_reconcile_slots"
            )
        for value in (
            self.liquidity_delta_bps,
            self.price_delta_bps,
            self.holder_delta_bps,
        ):
            if value < 1:
                raise ValueError("Solana event thresholds must be positive")
        if self.recent_signature_capacity < self.signature_limit:
            raise ValueError(
                "recent_signature_capacity must cover signature_limit"
            )


class SingleWriterLease:
    """Coordinate with chainseer_api.py's one-writer Timechain lease."""

    def __init__(self, chain_root: str | Path):
        self.path = Path(chain_root) / ".chainseer-api.lock"
        self.handle = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self.handle = self.path.open("r+b")
        try:
            self.handle.seek(0, os.SEEK_END)
            if self.handle.tell() == 0:
                self.handle.write(b" ")
                self.handle.flush()
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(
                    self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB
                )
        except (OSError, BlockingIOError) as exc:
            self.handle.close()
            self.handle = None
            raise RuntimeError(
                "Chainseer Timechain is already owned by another writer"
            ) from exc
        # Keep byte zero intact for the lifetime of the Windows byte-range
        # lock. Truncating the locked byte makes a later LK_UNLCK fail on
        # Windows once the lease file already contains metadata.
        self.handle.seek(1)
        self.handle.truncate()
        self.handle.write(
            (
                f"pid={os.getpid()} host={socket.gethostname()} "
                f"acquired={utc_now_iso()}\n"
            ).encode("utf-8")
        )
        self.handle.flush()

    def release(self) -> None:
        if self.handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


class WatchStore:
    """Atomic watcher state plus append-only alert history."""

    def __init__(self, root: str | Path, network: str = "robinhood"):
        if network not in {"robinhood", "base"}:
            raise ValueError("unsupported EVM watcher network")
        self.root = Path(root)
        self.network = network
        prefix = "" if network == "robinhood" else f"{network}_"
        self.state_path = self.root / f"{prefix}watcher_state.json"
        self.alert_path = self.root / f"{prefix}watcher_alerts.jsonl"

    def load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {
                "schema_version": CONTROL_SCHEMA_VERSION,
                "subscriptions": {},
                "updated_at": None,
            }
        value = json.loads(self.state_path.read_text(encoding="utf-8"))
        if value.get("schema_version") != CONTROL_SCHEMA_VERSION:
            raise ValueError("unsupported watcher state schema")
        value.setdefault("subscriptions", {})
        return value

    def save(self, state: dict[str, Any]) -> None:
        state["schema_version"] = CONTROL_SCHEMA_VERSION
        state["updated_at"] = utc_now_iso()
        _atomic_write_json(self.state_path, state)

    def append_alert(self, alert: dict[str, Any]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with self.alert_path.open("a", encoding="utf-8") as handle:
            handle.write(canonical_json(alert) + "\n")

    @staticmethod
    def _subscriber(value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not re.fullmatch(r"[a-f0-9]{64}", normalized):
            raise ValueError("invalid watcher subscriber identity")
        return normalized

    @staticmethod
    def public_subscription(value: dict[str, Any]) -> dict[str, Any]:
        return {
            "network": value.get("network", "robinhood"),
            "token_address": value.get("token_address"),
            "created_at": value.get("created_at"),
            "subscriber_count": len(value.get("subscribers") or []),
            "manual_subscription": bool(
                value.get("manual_subscription", True)
            ),
            "last_processed_block": value.get("last_processed_block"),
            "last_observed_slot": value.get("last_observed_slot"),
            "last_full_scan_block": value.get("last_full_scan_block"),
            "last_full_scan_slot": value.get("last_full_scan_slot"),
            "latest_analysis": value.get("latest_analysis"),
            "last_error": value.get("last_error"),
        }

    def subscribe(
        self,
        token: str,
        subscriber: str | None = None,
    ) -> dict[str, Any]:
        if not ADDRESS_RE.fullmatch(token):
            raise ValueError("invalid token address")
        subscriber = self._subscriber(subscriber)
        state = self.load()
        key = token.lower()
        value = state["subscriptions"].setdefault(
            key,
            {
                "network": self.network,
                "token_address": token,
                "created_at": utc_now_iso(),
                "manual_subscription": subscriber is None,
                "subscribers": [],
                "last_processed_block": None,
                "last_processed_hash": None,
                "last_full_scan_block": None,
                "quick_fingerprint": None,
                "latest_analysis": None,
                "analyses": [],
                "completed_outcomes": [],
            },
        )
        value.setdefault("manual_subscription", subscriber is None)
        value.setdefault("subscribers", [])
        if subscriber and subscriber not in value["subscribers"]:
            value["subscribers"].append(subscriber)
            value["subscribers"] = value["subscribers"][-1_000:]
        elif subscriber is None:
            value["manual_subscription"] = True
        self.save(state)
        return value

    def is_subscribed(self, token: str, subscriber: str) -> bool:
        subscriber = self._subscriber(subscriber)
        value = self.load()["subscriptions"].get(token.lower()) or {}
        return bool(subscriber in (value.get("subscribers") or []))

    def unsubscribe(
        self,
        token: str,
        subscriber: str | None = None,
    ) -> bool:
        subscriber = self._subscriber(subscriber)
        state = self.load()
        key = token.lower()
        value = state["subscriptions"].get(key)
        if value is None:
            return False
        if subscriber is None:
            state["subscriptions"].pop(key, None)
            removed = True
        else:
            subscribers = value.setdefault("subscribers", [])
            removed = subscriber in subscribers
            if removed:
                subscribers.remove(subscriber)
            if not subscribers and not value.get("manual_subscription", True):
                state["subscriptions"].pop(key, None)
        if removed:
            self.save(state)
        return removed

    def read_alerts(
        self,
        token: str,
        *,
        after: str | None = None,
        limit: int = 50,
        critical_only: bool = True,
    ) -> list[dict[str, Any]]:
        if not self.alert_path.exists():
            return []
        results: list[dict[str, Any]] = []
        with self.alert_path.open("r", encoding="utf-8") as handle:
            lines = deque(handle, maxlen=2_000)
        for line in lines:
            try:
                alert = json.loads(line)
            except (TypeError, ValueError):
                continue
            alert_token = str(alert.get("token_address") or "")
            if alert_token.lower() != token.lower():
                continue
            observed_at = str(alert.get("observed_at") or "")
            if after and observed_at <= after:
                continue
            critical_events = alert.get("critical_events") or []
            if critical_only and not critical_events:
                continue
            results.append(_public_alert(alert))
        return results[-max(1, min(int(limit), 100)):]


class SolanaWatchStore(WatchStore):
    """Separate atomic cursors and append-only alerts for Solana mints."""

    def __init__(self, root: str | Path):
        super().__init__(root)
        self.state_path = self.root / "solana_watcher_state.json"
        self.alert_path = self.root / "solana_watcher_alerts.jsonl"

    def subscribe(
        self,
        mint: str,
        subscriber: str | None = None,
    ) -> dict[str, Any]:
        if not _is_solana_pubkey(mint):
            raise ValueError("invalid Solana mint address")
        subscriber = self._subscriber(subscriber)
        state = self.load()
        value = state["subscriptions"].setdefault(
            mint,
            {
                "network": "solana",
                "token_address": mint,
                "created_at": utc_now_iso(),
                "manual_subscription": subscriber is None,
                "subscribers": [],
                "last_observed_slot": None,
                "last_full_scan_slot": None,
                "last_signature": None,
                "recent_signatures": [],
                "quick_fingerprint": None,
                "quick_snapshot": None,
                "latest_analysis": None,
                "analyses": [],
            },
        )
        value.setdefault("manual_subscription", subscriber is None)
        value.setdefault("subscribers", [])
        if subscriber and subscriber not in value["subscribers"]:
            value["subscribers"].append(subscriber)
            value["subscribers"] = value["subscribers"][-1_000:]
        elif subscriber is None:
            value["manual_subscription"] = True
        self.save(state)
        return value

    def is_subscribed(self, mint: str, subscriber: str) -> bool:
        subscriber = self._subscriber(subscriber)
        value = self.load()["subscriptions"].get(mint) or {}
        return bool(subscriber in (value.get("subscribers") or []))

    def unsubscribe(
        self,
        mint: str,
        subscriber: str | None = None,
    ) -> bool:
        subscriber = self._subscriber(subscriber)
        state = self.load()
        value = state["subscriptions"].get(mint)
        if value is None:
            return False
        if subscriber is None:
            state["subscriptions"].pop(mint, None)
            removed = True
        else:
            subscribers = value.setdefault("subscribers", [])
            removed = subscriber in subscribers
            if removed:
                subscribers.remove(subscriber)
            if not subscribers and not value.get("manual_subscription", True):
                state["subscriptions"].pop(mint, None)
        if removed:
            self.save(state)
        return removed


def _analysis_view(report: dict[str, Any]) -> dict[str, Any]:
    data = report.get("data") or {}
    analysis = report.get("analysis") or {}
    dex = data.get("dex_pairs") or {}
    contract = data.get("contract_audit") or {}
    source = data.get("source_code") or {}
    custody = data.get("lp_lock") or {}
    tax = data.get("goplus_security") or {}
    implementations = source.get("implementations") or []
    implementation_identity = sorted(
        str(
            item.get("address")
            or item.get("implementation_address")
            or item.get("name")
            or ""
        ).lower()
        for item in implementations
        if isinstance(item, dict)
    )
    return {
        "analysis_ring": report.get("analysis_ring"),
        "analysis_ring_hash": report.get("analysis_ring_hash"),
        "block_pin": (report.get("provenance") or {}).get("block_pin"),
        "timestamp": report.get("timestamp"),
        "risk_level": analysis.get("risk_level"),
        "score": analysis.get("legitimacy_score"),
        "hard_stop_codes": sorted(
            str(item.get("code"))
            for item in analysis.get("hard_stop_overrides") or []
            if isinstance(item, dict) and item.get("code")
        ),
        "price_usd": dex.get("primary_price_usd"),
        "liquidity_usd": dex.get("primary_liquidity_usd"),
        "holder_count": (
            (analysis.get("holder_assessment") or {}).get("holder_count")
        ),
        "owner": contract.get("owner"),
        "bytecode_hash": contract.get("bytecode_hash"),
        "proxy_type": source.get("proxy_type"),
        "implementation_hash": (
            canonical_hash(implementation_identity)
            if implementation_identity
            else None
        ),
        "implementation_verified": source.get("implementation_verified"),
        "custody_state": custody.get("state"),
        "buy_tax": tax.get("buy_tax"),
        "sell_tax": tax.get("sell_tax"),
        "buy_tax_pct": _safe_float(tax.get("buy_tax")) * 100,
        "sell_tax_pct": _safe_float(tax.get("sell_tax")) * 100,
        "pair_address": dex.get("primary_pair_address"),
        "extended_evidence": analysis.get("extended_evidence") or {},
    }


def _diff_views(before: dict[str, Any] | None, after: dict[str, Any]) -> dict[str, Any]:
    if not before:
        return {"initial": True, "changes": {}}
    keys = (
        "risk_level",
        "score",
        "hard_stop_codes",
        "price_usd",
        "liquidity_usd",
        "holder_count",
        "owner",
        "bytecode_hash",
        "proxy_type",
        "implementation_hash",
        "implementation_verified",
        "custody_state",
        "buy_tax",
        "sell_tax",
        "buy_tax_pct",
        "sell_tax_pct",
        "pair_address",
    )
    changes = {
        key: {"before": before.get(key), "after": after.get(key)}
        for key in keys
        if before.get(key) != after.get(key)
    }
    return {"initial": False, "changes": changes}


def _solana_analysis_view(report: dict[str, Any]) -> dict[str, Any]:
    data = report.get("data") or {}
    analysis = report.get("analysis") or {}
    basic = data.get("basic_info") or {}
    dex = data.get("dex_pairs") or {}
    concentration = data.get("holder_concentration") or {}
    execution = data.get("execution_evidence") or {}
    return {
        "analysis_ring": report.get("analysis_ring"),
        "analysis_ring_hash": report.get("analysis_ring_hash"),
        "slot_anchor": (report.get("provenance") or {}).get("block_pin"),
        "timestamp": report.get("timestamp"),
        "risk_level": analysis.get("risk_level"),
        "score": analysis.get("legitimacy_score"),
        "hard_stop_codes": sorted(
            str(item.get("code"))
            for item in analysis.get("hard_stop_overrides") or []
            if isinstance(item, dict) and item.get("code")
        ),
        "mint_authority": basic.get("mint_authority"),
        "freeze_authority": basic.get("freeze_authority"),
        "owner_program": basic.get("owner_program"),
        "supply_raw": basic.get("supply_raw"),
        "extensions": sorted(str(value) for value in basic.get("extensions") or []),
        "holder_count": basic.get("jupiter_holder_count"),
        "top1_total_supply_pct": concentration.get(
            "top1_total_supply_pct"
        ),
        "top10_total_supply_pct": concentration.get(
            "top10_total_supply_pct"
        ),
        "pair_address": dex.get("primary_pair"),
        "dex_id": dex.get("primary_amm_version"),
        "price_usd": dex.get("primary_price_usd"),
        "liquidity_usd": dex.get("total_liquidity_usd"),
        "market_cap": dex.get("market_cap"),
        "roundtrip_retention_pct": execution.get(
            "roundtrip_retention_pct"
        ),
    }


def _critical_event(
    category: str,
    code: str,
    message: str,
    *,
    before: Any = None,
    after: Any = None,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    value = {
        "category": category,
        "code": code,
        "message": message,
        "before": before,
        "after": after,
        "evidence": evidence or {},
    }
    value["event_hash"] = canonical_hash(value)
    return value


def _evm_critical_events(
    before: dict[str, Any],
    after: dict[str, Any],
    observed_events: dict[str, Any],
    new_stops: list[str],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    event_types = {
        str(item.get("event_type"))
        for item in observed_events.get("critical_events") or []
        if isinstance(item, dict)
    }
    if (
        before.get("pair_address")
        and not after.get("pair_address")
    ):
        findings.append(
            _critical_event(
                "liquidity",
                "primary_liquidity_market_disappeared",
                "The previously observed primary liquidity market disappeared.",
                before=before.get("pair_address"),
                after=None,
            )
        )
    old_liquidity = _safe_float(before.get("liquidity_usd"), 0.0)
    new_liquidity = _safe_float(after.get("liquidity_usd"), 0.0)
    if old_liquidity > 0:
        removed_pct = 100 * max(0.0, old_liquidity - new_liquidity) / old_liquidity
        if removed_pct >= 30:
            findings.append(
                _critical_event(
                    "liquidity",
                    "liquidity_removed",
                    f"Observed liquidity fell by {removed_pct:.1f}%.",
                    before=before.get("liquidity_usd"),
                    after=after.get("liquidity_usd"),
                    evidence={"removed_pct": round(removed_pct, 2)},
                )
            )
    unsafe_custody = {
        "creator_withdrawable",
        "withdrawal_verified",
        "unlocked",
    }
    if (
        after.get("custody_state") in unsafe_custody
        and before.get("custody_state") != after.get("custody_state")
    ) or "UNLOCKED_LP" in new_stops:
        findings.append(
            _critical_event(
                "liquidity",
                "liquidity_custody_deteriorated",
                "Liquidity custody changed to a creator-withdrawable or unlocked state.",
                before=before.get("custody_state"),
                after=after.get("custody_state"),
            )
        )

    if (
        before.get("owner") != after.get("owner")
        or "ownership_transferred" in event_types
        or "admin_changed" in event_types
    ):
        findings.append(
            _critical_event(
                "authority",
                "privileged_authority_changed",
                "Contract ownership or proxy administration changed.",
                before=before.get("owner"),
                after=after.get("owner"),
                evidence={"event_types": sorted(event_types)},
            )
        )

    implementation_changed = (
        before.get("implementation_hash")
        and before.get("implementation_hash") != after.get("implementation_hash")
    )
    bytecode_changed = (
        before.get("bytecode_hash")
        and before.get("bytecode_hash") != after.get("bytecode_hash")
    )
    if (
        implementation_changed
        or bytecode_changed
        or "implementation_upgraded" in event_types
        or "UNVERIFIED_PROXY" in new_stops
    ):
        findings.append(
            _critical_event(
                "upgrade",
                "contract_implementation_changed",
                "Proxy implementation or deployed bytecode changed.",
                before={
                    "implementation_hash": before.get("implementation_hash"),
                    "bytecode_hash": before.get("bytecode_hash"),
                },
                after={
                    "implementation_hash": after.get("implementation_hash"),
                    "bytecode_hash": after.get("bytecode_hash"),
                },
                evidence={"event_types": sorted(event_types)},
            )
        )

    sellability_stops = {"HONEYPOT", "SELL_RESTRICTED"}
    new_sellability_stops = sorted(sellability_stops.intersection(new_stops))
    old_sell_tax = _safe_float(before.get("sell_tax_pct"), 0.0)
    new_sell_tax = _safe_float(after.get("sell_tax_pct"), 0.0)
    tax_crossed = new_sell_tax > 10 and (
        old_sell_tax <= 10 or new_sell_tax - old_sell_tax >= 10
    )
    if new_sellability_stops or tax_crossed:
        findings.append(
            _critical_event(
                "sellability",
                (
                    "sell_restriction_detected"
                    if new_sellability_stops
                    else "sell_tax_increased"
                ),
                (
                    "A new honeypot or sell-restriction hard stop was detected."
                    if new_sellability_stops
                    else f"Observed sell tax increased to {new_sell_tax:.1f}%."
                ),
                before={"sell_tax_pct": old_sell_tax},
                after={
                    "sell_tax_pct": new_sell_tax,
                    "new_hard_stops": new_sellability_stops,
                },
            )
        )
    return list({item["event_hash"]: item for item in findings}.values())


def _solana_critical_events(
    before: dict[str, Any],
    after: dict[str, Any],
    observed_events: list[dict[str, Any]],
    new_stops: list[str],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for event in observed_events:
        event_type = str(event.get("event_type") or "")
        if event_type in {
            "mint_authority_changed",
            "freeze_authority_changed",
            "owner_program_changed",
            "token_extensions_changed",
        }:
            findings.append(
                _critical_event(
                    "authority",
                    event_type,
                    "A Solana mint authority, owner program, or token extension changed.",
                    before=event.get("before"),
                    after=event.get("after"),
                    evidence={"source_event_hash": event.get("event_hash")},
                )
            )
        elif event_type in {"market_disappeared", "primary_market_changed"}:
            findings.append(
                _critical_event(
                    "liquidity",
                    event_type,
                    "The primary Solana liquidity market disappeared or changed.",
                    before=event.get("before"),
                    after=event.get("after"),
                    evidence={"source_event_hash": event.get("event_hash")},
                )
            )
        elif event_type == "liquidity_removed":
            findings.append(
                _critical_event(
                    "liquidity",
                    event_type,
                    "Observed Solana liquidity fell by at least 30%.",
                    before=event.get("before"),
                    after=event.get("after"),
                    evidence=event.get("evidence") or {},
                )
            )
    sellability_stops = {
        "jupiter_roundtrip_retention_low",
        "jupiter_buy_price_impact_high",
    }
    new_sellability_stops = sorted(sellability_stops.intersection(new_stops))
    old_retention = _safe_float(before.get("roundtrip_retention_pct"), 100.0)
    new_retention = _safe_float(after.get("roundtrip_retention_pct"), 100.0)
    if new_sellability_stops or (
        old_retention >= 72 and new_retention < 72
    ):
        findings.append(
            _critical_event(
                "sellability",
                "jupiter_sellability_deteriorated",
                "Jupiter route evidence now indicates unsafe sellability or price impact.",
                before={"roundtrip_retention_pct": old_retention},
                after={
                    "roundtrip_retention_pct": new_retention,
                    "new_hard_stops": new_sellability_stops,
                },
            )
        )
    return list({item["event_hash"]: item for item in findings}.values())


def _public_alert(alert: dict[str, Any]) -> dict[str, Any]:
    critical_events = alert.get("critical_events") or []
    categories = list(
        dict.fromkeys(
            str(item.get("category"))
            for item in critical_events
            if isinstance(item, dict) and item.get("category")
        )
    )
    primary = critical_events[0] if critical_events else {}
    network = str(alert.get("network") or "robinhood")
    anchor = alert.get("slot") if network == "solana" else alert.get("block")
    title_category = categories[0].title() if categories else "Critical"
    return {
        "schema_version": CONTROL_SCHEMA_VERSION,
        "alert_hash": alert.get("alert_hash") or canonical_hash(alert),
        "network": network,
        "token_address": alert.get("token_address"),
        "severity": "critical",
        "categories": categories,
        "title": f"Critical {title_category} alert",
        "message": primary.get("message") or "A critical token state changed.",
        "critical_events": critical_events,
        "anchor": anchor,
        "anchor_type": "confirmed_slot" if network == "solana" else "confirmed_block",
        "observed_at": alert.get("observed_at"),
        "new_hard_stops": alert.get("new_hard_stops") or [],
        "analysis_ring": alert.get("analysis_ring"),
        "analysis_ring_hash": alert.get("analysis_ring_hash"),
        "timechain": alert.get("timechain"),
    }


def _solana_diff_views(
    before: dict[str, Any] | None,
    after: dict[str, Any],
) -> dict[str, Any]:
    if not before:
        return {"initial": True, "changes": {}}
    keys = (
        "risk_level",
        "score",
        "hard_stop_codes",
        "mint_authority",
        "freeze_authority",
        "owner_program",
        "supply_raw",
        "extensions",
        "holder_count",
        "top1_total_supply_pct",
        "top10_total_supply_pct",
        "pair_address",
        "dex_id",
        "price_usd",
        "liquidity_usd",
        "market_cap",
        "roundtrip_retention_pct",
    )
    return {
        "initial": False,
        "changes": {
            key: {"before": before.get(key), "after": after.get(key)}
            for key in keys
            if before.get(key) != after.get(key)
        },
    }


def _relative_delta_bps(before: Any, after: Any) -> float | None:
    left = _safe_float(before, -1.0)
    right = _safe_float(after, -1.0)
    if left <= 0 or right < 0:
        return None
    return abs(right - left) / left * 10_000


def credential_safe_error(error: Any, *, limit: int = 320) -> str:
    """Return a bounded diagnostic that is safe to persist or expose."""
    text = str(error)
    text = re.sub(r"https?://[^\s)\]\"']+", "<redacted-url>", text)
    text = re.sub(
        r"(?i)\b(api[-_]?key|access[-_]?token|token|secret|authorization)"
        r"\s*[=:]\s*[^\s,;]+",
        r"\1=<redacted>",
        text,
    )
    return text[:limit]


class OutcomeCollector:
    """Convert monitored drift into deduplicated, horizon-labelled outcomes."""

    @staticmethod
    def due_horizons(
        subscription: dict[str, Any],
        *,
        now: float,
        horizons: tuple[int, ...],
    ) -> list[tuple[int, dict[str, Any]]]:
        completed = set(subscription.get("completed_outcomes") or [])
        due: list[tuple[int, dict[str, Any]]] = []
        for baseline in subscription.get("analyses") or []:
            started = _parse_time(baseline.get("timestamp"))
            ring = baseline.get("analysis_ring")
            if started is None or ring is None:
                continue
            for horizon in horizons:
                key = f"{ring}:{horizon}"
                if key not in completed and now - started >= horizon:
                    due.append((horizon, baseline))
        return due

    @staticmethod
    def outcomes(
        baseline: dict[str, Any],
        current: dict[str, Any],
        *,
        horizon_seconds: int,
    ) -> dict[str, Any]:
        base_price = _safe_float(baseline.get("price_usd"))
        current_price = _safe_float(current.get("price_usd"))
        base_liquidity = _safe_float(baseline.get("liquidity_usd"))
        current_liquidity = _safe_float(current.get("liquidity_usd"))
        price_return = (
            ((current_price / base_price) - 1) * 100
            if base_price > 0 and current_price >= 0
            else None
        )
        liquidity_removed = (
            max(0.0, (1 - current_liquidity / base_liquidity) * 100)
            if base_liquidity > 0
            else None
        )
        current_stops = set(current.get("hard_stop_codes") or [])
        base_stops = set(baseline.get("hard_stop_codes") or [])
        result: dict[str, Any] = {
            "horizon_seconds": horizon_seconds,
            "new_hard_stop_codes": sorted(current_stops - base_stops),
        }
        if price_return is not None:
            result["price_return_pct"] = round(price_return, 4)
        if liquidity_removed is not None:
            result["liquidity_removed_pct"] = round(liquidity_removed, 4)
        if "HONEYPOT" in current_stops:
            result["honeypot_observed"] = True
        if "SCAM_FLAG" in current_stops:
            result["rug_pull"] = True
        if baseline.get("owner") and current.get("owner") != baseline.get("owner"):
            result["owner_privilege_used"] = True
        if (
            baseline.get("buy_tax") != current.get("buy_tax")
            or baseline.get("sell_tax") != current.get("sell_tax")
        ):
            result["tax_changed"] = True
        return result

    def collect(
        self,
        agent: Any,
        subscription: dict[str, Any],
        current_report: dict[str, Any],
        *,
        now: float,
        horizons: tuple[int, ...],
    ) -> list[dict[str, Any]]:
        current = _analysis_view(current_report)
        emitted = []
        completed = set(subscription.get("completed_outcomes") or [])
        for horizon, baseline in self.due_horizons(
            subscription, now=now, horizons=horizons
        ):
            key = f"{baseline['analysis_ring']}:{horizon}"
            outcome = self.outcomes(
                baseline, current, horizon_seconds=horizon
            )
            reflection = agent.reflect_on_analysis(
                int(baseline["analysis_ring"]),
                outcome,
                observed_at=utc_now_iso(now),
            )
            completed.add(key)
            emitted.append(
                {
                    "key": key,
                    "analysis_ring": baseline["analysis_ring"],
                    "outcome_ring": reflection["ring"].get("index"),
                    "horizon_seconds": horizon,
                    "calibration": reflection["calibration"],
                }
            )
        subscription["completed_outcomes"] = sorted(completed)
        return emitted


@dataclass(frozen=True)
class CalibrationPolicy:
    version: str = "1.0.0"
    min_trade_score: float = 80.0
    allowed_risk_levels: tuple[str, ...] = ("Low",)
    max_false_negative_rate: float = 0.05
    min_outcomes: int = 20
    max_permit_block_drift: int = 2
    max_quote_age_blocks: int = 2
    permit_ttl_seconds: int = 90

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "CalibrationPolicy":
        return cls(
            version=str(value.get("version") or "1.0.0"),
            min_trade_score=_safe_float(value.get("min_trade_score"), 80.0),
            allowed_risk_levels=tuple(value.get("allowed_risk_levels") or ("Low",)),
            max_false_negative_rate=_safe_float(
                value.get("max_false_negative_rate"), 0.05
            ),
            min_outcomes=max(1, _safe_int(value.get("min_outcomes"), 20)),
            max_permit_block_drift=max(
                0, _safe_int(value.get("max_permit_block_drift"), 2)
            ),
            max_quote_age_blocks=max(
                0, _safe_int(value.get("max_quote_age_blocks"), 2)
            ),
            permit_ttl_seconds=max(
                15, min(300, _safe_int(value.get("permit_ttl_seconds"), 90))
            ),
        )


class CalibrationEngine:
    """Measure outcome calibration and propose tighten-only policy updates."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.policy_path = self.root / "calibration_policy.json"
        self.proposal_path = self.root / "calibration_proposal.json"

    def policy(self) -> CalibrationPolicy:
        if not self.policy_path.exists():
            return CalibrationPolicy()
        return CalibrationPolicy.from_dict(
            json.loads(self.policy_path.read_text(encoding="utf-8"))
        )

    @staticmethod
    def summarize(rings: list[dict[str, Any]]) -> dict[str, Any]:
        outcomes = [
            ring
            for ring in rings
            if ring.get("ring_type") == "analysis_outcome"
        ]
        # A later horizon supersedes an earlier label for the same analysis.
        latest: dict[int, dict[str, Any]] = {}
        for ring in outcomes:
            payload = ring.get("payload") or {}
            analysis_ring = _safe_int(payload.get("analysis_ring"), -1)
            if analysis_ring < 0:
                continue
            horizon = _safe_int(
                (payload.get("other_outcomes") or {}).get("horizon_seconds"),
                0,
            )
            previous = latest.get(analysis_ring)
            previous_horizon = _safe_int(
                ((previous or {}).get("payload") or {})
                .get("other_outcomes", {})
                .get("horizon_seconds"),
                -1,
            )
            if previous is None or horizon >= previous_horizon:
                latest[analysis_ring] = ring

        tp = fp = tn = fn = 0
        market_returns = []
        for ring in latest.values():
            payload = ring.get("payload") or {}
            calibration = payload.get("calibration") or {}
            adverse = bool(calibration.get("adverse_security_event"))
            risk = str(calibration.get("original_risk_level") or "Unknown")
            predicted_adverse = risk in {"High", "Critical"}
            if adverse and predicted_adverse:
                tp += 1
            elif adverse:
                fn += 1
            elif predicted_adverse:
                fp += 1
            else:
                tn += 1
            value = (payload.get("market_outcomes") or {}).get("price_return_pct")
            if value is not None:
                market_returns.append(_safe_float(value))
        actual_positive = tp + fn
        predicted_positive = tp + fp
        total = tp + fp + tn + fn
        return {
            "sample_size": total,
            "true_positive": tp,
            "false_positive": fp,
            "true_negative": tn,
            "false_negative": fn,
            "false_negative_rate": (
                fn / actual_positive if actual_positive else None
            ),
            "false_positive_rate": (
                fp / predicted_positive if predicted_positive else None
            ),
            "mean_market_return_pct": (
                round(sum(market_returns) / len(market_returns), 4)
                if market_returns
                else None
            ),
            "market_return_samples": len(market_returns),
            "security_and_market_labels_separated": True,
        }

    def propose(self, rings: list[dict[str, Any]]) -> dict[str, Any]:
        policy = self.policy()
        metrics = self.summarize(rings)
        sample = metrics["sample_size"]
        fnr = metrics["false_negative_rate"]
        if sample < policy.min_outcomes:
            proposal = {
                "schema_version": CONTROL_SCHEMA_VERSION,
                "status": "insufficient_data",
                "current_policy": asdict(policy),
                "metrics": metrics,
                "required_outcomes": policy.min_outcomes,
                "created_at": utc_now_iso(),
            }
        elif fnr is not None and fnr > policy.max_false_negative_rate:
            increment = max(
                1.0,
                min(5.0, math.ceil((fnr - policy.max_false_negative_rate) * 20)),
            )
            parts = policy.version.split(".")
            patch = _safe_int(parts[-1], 0) + 1
            next_version = ".".join(parts[:-1] + [str(patch)])
            proposed = asdict(policy)
            proposed["version"] = next_version
            proposed["min_trade_score"] = min(
                95.0, policy.min_trade_score + increment
            )
            proposed["allowed_risk_levels"] = ["Low"]
            proposal = {
                "schema_version": CONTROL_SCHEMA_VERSION,
                "status": "proposed",
                "direction": "tighten_only",
                "reason": "observed_false_negative_rate_exceeds_policy",
                "current_policy": asdict(policy),
                "proposed_policy": proposed,
                "metrics": metrics,
                "created_at": utc_now_iso(),
            }
        else:
            proposal = {
                "schema_version": CONTROL_SCHEMA_VERSION,
                "status": "no_change",
                "current_policy": asdict(policy),
                "metrics": metrics,
                "created_at": utc_now_iso(),
            }
        _atomic_write_json(self.proposal_path, proposal)
        return proposal

    def adopt(self, proposal: dict[str, Any], agent: Any) -> dict[str, Any]:
        if proposal.get("status") != "proposed":
            raise ValueError("only a proposed calibration can be adopted")
        current = self.policy()
        proposed = CalibrationPolicy.from_dict(
            proposal.get("proposed_policy") or {}
        )
        if proposed.min_trade_score < current.min_trade_score:
            raise ValueError("calibration adoption cannot loosen min_trade_score")
        if not set(proposed.allowed_risk_levels).issubset(
            set(current.allowed_risk_levels)
        ):
            raise ValueError("calibration adoption cannot broaden allowed risks")
        # propose() never touches these fields, but adopt() takes an
        # arbitrary proposal file path (not something bound to propose()'s
        # own output), so a hand-edited proposal must still be rejected if it
        # loosens any dimension TradePermitGuard relies on for freshness or
        # data-quality, not just the two fields checked above.
        if proposed.max_false_negative_rate > current.max_false_negative_rate:
            raise ValueError(
                "calibration adoption cannot raise max_false_negative_rate"
            )
        if proposed.min_outcomes < current.min_outcomes:
            raise ValueError("calibration adoption cannot lower min_outcomes")
        if proposed.max_permit_block_drift > current.max_permit_block_drift:
            raise ValueError(
                "calibration adoption cannot raise max_permit_block_drift"
            )
        if proposed.max_quote_age_blocks > current.max_quote_age_blocks:
            raise ValueError(
                "calibration adoption cannot raise max_quote_age_blocks"
            )
        if proposed.permit_ttl_seconds > current.permit_ttl_seconds:
            raise ValueError(
                "calibration adoption cannot raise permit_ttl_seconds"
            )
        metrics = proposal.get("metrics") or {}
        if _safe_int(metrics.get("sample_size")) < current.min_outcomes:
            raise ValueError("calibration adoption lacks the required outcomes")
        verdict, ring = agent.poq_module.gate_and_seal(
            agent.tc,
            (
                f"Adopt Chainseer calibration policy {proposed.version}; "
                "the update is tighten-only and outcome-grounded."
            ),
            context=canonical_json(proposal),
            ring_type="calibration_policy",
            external_scores={
                "coherence": 245,
                "relevance": 250,
                "novelty": 225,
                "consistency": 245,
                "depth": 240,
                "covenant": 255,
            },
            declared_evidence=max(1, _safe_int(metrics.get("sample_size"))),
            extra_payload={
                "policy": asdict(proposed),
                "metrics": metrics,
                "proposal_hash": canonical_hash(proposal),
            },
        )
        if ring is None:
            raise RuntimeError(
                f"PoQ refused calibration adoption: {verdict.get('decision')}"
            )
        _atomic_write_json(self.policy_path, asdict(proposed))
        return {
            "policy": asdict(proposed),
            "ring": ring.get("index"),
            "ring_hash": ring.get("ring_hash"),
            "verdict": verdict,
        }


class TradePermitGuard:
    """Issue and validate non-signing, short-lived execution authorizations."""

    _consume_lock = threading.Lock()

    def __init__(
        self,
        agent: Any,
        *,
        control_root: str | Path | None = None,
        policy: CalibrationPolicy | None = None,
    ):
        self.agent = agent
        root = Path(control_root or agent.chain_root) / "controls"
        self.control_root = root
        self.consumed_path = root / "consumed_permits.json"
        self.engine = CalibrationEngine(root)
        self.policy = policy or self.engine.policy()

    def _consumed(self) -> dict[str, Any]:
        if not self.consumed_path.exists():
            return {
                "schema_version": CONTROL_SCHEMA_VERSION,
                "permits": {},
            }
        value = json.loads(self.consumed_path.read_text(encoding="utf-8"))
        if value.get("schema_version") != CONTROL_SCHEMA_VERSION:
            raise ValueError("unsupported consumed-permit schema")
        value.setdefault("permits", {})
        return value

    def authorize(
        self,
        token: str,
        *,
        amount_in: str,
        recipient: str,
        quote: dict[str, Any],
        confirmations: int = 2,
        private_routing: bool = False,
        now: float | None = None,
    ) -> dict[str, Any]:
        if not ADDRESS_RE.fullmatch(token):
            raise ValueError("invalid token address")
        if not ADDRESS_RE.fullmatch(recipient):
            raise ValueError("invalid recipient address")
        if _safe_int(amount_in, 0) <= 0:
            raise ValueError("amount_in must be a positive integer string")
        now = time.time() if now is None else now
        head = self.agent.rpc.get_block_number()
        validated_block = max(0, head - max(0, confirmations))
        report = self.agent.analyze_token(
            token, full_report=False, block_pin=validated_block
        )
        if report.get("error"):
            raise RuntimeError(f"pre-trade analysis failed: {report['error']}")
        analysis = report.get("analysis") or {}
        hard_stops = analysis.get("hard_stop_overrides") or []
        if hard_stops:
            raise PermissionError("trade refused: analysis hard-stop is active")
        if analysis.get("risk_level") not in self.policy.allowed_risk_levels:
            raise PermissionError(
                "trade refused: risk level is outside the adopted policy"
            )
        if _safe_float(analysis.get("legitimacy_score")) < self.policy.min_trade_score:
            raise PermissionError(
                "trade refused: legitimacy score is below the adopted policy"
            )
        if analysis.get("confidence_grade") == "LIMITED":
            raise PermissionError("trade refused: analysis confidence is limited")

        pair = str(
            ((report.get("data") or {}).get("dex_pairs") or {}).get(
                "primary_pair_address"
            )
            or ""
        )
        if not ADDRESS_RE.fullmatch(pair):
            raise PermissionError("trade refused: canonical pair is unavailable")
        if str(quote.get("pair_address") or "").lower() != pair.lower():
            raise PermissionError("trade refused: quote pair is not canonical")
        if str(quote.get("amount_in")) != str(amount_in):
            raise PermissionError("trade refused: quote amount does not match")

        mev = MEVExposureAssessor.assess(
            quote,
            validated_block=validated_block,
            max_quote_age_blocks=self.policy.max_quote_age_blocks,
            private_routing=private_routing,
        )
        if mev["hard_stops"]:
            raise PermissionError(
                "trade refused by execution-risk gate: "
                + ", ".join(mev["hard_stops"])
            )
        expires_at = now + self.policy.permit_ttl_seconds
        critical_state = {
            "code_hash": (
                ((report.get("data") or {}).get("contract_audit") or {}).get(
                    "bytecode_hash"
                )
            ),
            "owner": (
                ((report.get("data") or {}).get("contract_audit") or {}).get(
                    "owner"
                )
            ),
            "liquidity_custody": (
                ((report.get("data") or {}).get("lp_lock") or {}).get("state")
            ),
            "hard_stop_codes": [],
        }
        payload = {
            "permit_version": PERMIT_VERSION,
            "nonce": secrets.token_hex(16),
            "chain_id": self.agent.chain_id,
            "token_address": token,
            "pair_address": pair,
            "analysis_ring": report.get("analysis_ring"),
            "analysis_ring_hash": report.get("analysis_ring_hash"),
            "analysis_block": (report.get("provenance") or {}).get("block_pin"),
            "validated_block": validated_block,
            "max_block_drift": self.policy.max_permit_block_drift,
            "issued_at": utc_now_iso(now),
            "expires_at": utc_now_iso(expires_at),
            "amount_in": str(amount_in),
            "min_amount_out": str(quote.get("min_amount_out")),
            "recipient": recipient,
            "quote_hash": canonical_hash(quote),
            "quote_source": str(quote.get("source")),
            "route": quote.get("route"),
            "policy_version": self.policy.version,
            "critical_state_hash": canonical_hash(critical_state),
            "execution_risk": mev,
            "signing_capability": False,
            "broadcast_capability": False,
        }
        permit_hash = canonical_hash(payload)
        verdict, ring = self.agent.poq_module.gate_and_seal(
            self.agent.tc,
            (
                f"Authorize non-signing TradePermit {permit_hash[:12]} for "
                f"{token} at block {validated_block}."
            ),
            context=canonical_json(
                {
                    "permit": payload,
                    "analysis": analysis,
                    "quote": quote,
                }
            ),
            ring_type="trade_permit",
            external_scores={
                "coherence": 250,
                "relevance": 255,
                "novelty": 235,
                "consistency": 250,
                "depth": 245,
                "covenant": 255,
            },
            declared_evidence=max(
                1, _safe_int((report.get("provenance") or {}).get("fact_count"))
            ),
            extra_payload={
                "permit_hash": permit_hash,
                "permit": payload,
                "analysis_ring": report.get("analysis_ring"),
                "analysis_ring_hash": report.get("analysis_ring_hash"),
            },
        )
        if ring is None:
            raise PermissionError(
                f"trade refused: PoQ decision {verdict.get('decision')}"
            )
        return {
            **payload,
            "permit_hash": permit_hash,
            "permit_ring": ring.get("index"),
            "permit_ring_hash": ring.get("ring_hash"),
            "poq_decision": verdict.get("decision"),
        }

    def verify(
        self,
        permit: dict[str, Any],
        *,
        current_block: int | None = None,
        now: float | None = None,
        consume: bool = False,
    ) -> dict[str, Any]:
        now = time.time() if now is None else now
        body = {
            key: value
            for key, value in permit.items()
            if key
            not in {
                "permit_hash",
                "permit_ring",
                "permit_ring_hash",
                "poq_decision",
            }
        }
        reasons = []
        expected_hash = canonical_hash(body)
        if expected_hash != permit.get("permit_hash"):
            reasons.append("permit hash mismatch")
        expires = _parse_time(permit.get("expires_at"))
        if expires is None or now > expires:
            reasons.append("permit expired")
        if permit.get("chain_id") != self.agent.chain_id:
            reasons.append("chain id mismatch")
        block = (
            self.agent.rpc.get_block_number()
            if current_block is None
            else current_block
        )
        if block < _safe_int(permit.get("validated_block")):
            reasons.append("current block precedes validation")
        if block - _safe_int(permit.get("validated_block")) > _safe_int(
            permit.get("max_block_drift")
        ):
            reasons.append("permit block drift exceeded")
        # A stored ring_hash field is only meaningful if the chain it lives in
        # is actually intact: without this, an attacker able to write
        # rings.jsonl could append one fabricated trade_permit ring with any
        # self-consistent hash and pass every check below. Recomputing and
        # walking the whole chain is the same cost as tc.load() below plus a
        # hash per ring, and this guard is not on the analysis hot path.
        chain_ok, _chain_report = self.agent.tc.verify()
        if not chain_ok:
            reasons.append("timechain integrity check failed")
        rings = self.agent.tc.load()
        ring = next(
            (
                item
                for item in rings
                if item.get("index") == permit.get("permit_ring")
                and item.get("ring_type") == "trade_permit"
            ),
            None,
        )
        if ring is None:
            reasons.append("permit ring unavailable")
        elif not chain_ok:
            pass  # already refused above; an unverified chain cannot be
            # trusted to confirm this ring's hash or its binding to the permit
        elif ring.get("ring_hash") != permit.get("permit_ring_hash"):
            reasons.append("permit ring hash mismatch")
        elif (ring.get("payload") or {}).get("permit_hash") != permit.get(
            "permit_hash"
        ):
            reasons.append("permit hash not bound to ring")
        consumed = self._consumed()
        if permit.get("permit_hash") in consumed["permits"]:
            reasons.append("permit already consumed")
        consumption = None
        if consume and not reasons:
            with self._consume_lock:
                consumed = self._consumed()
                if permit.get("permit_hash") in consumed["permits"]:
                    reasons.append("permit already consumed")
                else:
                    verdict, consumed_ring = self.agent.poq_module.gate_and_seal(
                        self.agent.tc,
                        (
                            "Consume one-time TradePermit "
                            f"{permit.get('permit_hash', '')[:12]}."
                        ),
                        context=canonical_json(
                            {
                                "permit_hash": permit.get("permit_hash"),
                                "permit_ring": permit.get("permit_ring"),
                                "current_block": block,
                            }
                        ),
                        ring_type="trade_permit_consumed",
                        external_scores={
                            "coherence": 250,
                            "relevance": 255,
                            "novelty": 220,
                            "consistency": 250,
                            "depth": 235,
                            "covenant": 255,
                        },
                        declared_evidence=1,
                        extra_payload={
                            "permit_hash": permit.get("permit_hash"),
                            "permit_ring": permit.get("permit_ring"),
                            "consumed_at": utc_now_iso(now),
                            "consumed_block": block,
                        },
                    )
                    if consumed_ring is None:
                        reasons.append(
                            "PoQ refused one-time permit consumption"
                        )
                    else:
                        consumption = {
                            "consumed_at": utc_now_iso(now),
                            "consumed_block": block,
                            "ring": consumed_ring.get("index"),
                            "ring_hash": consumed_ring.get("ring_hash"),
                            "decision": verdict.get("decision"),
                        }
                        consumed["permits"][permit["permit_hash"]] = consumption
                        _atomic_write_json(self.consumed_path, consumed)
        return {
            "valid": not reasons,
            "reasons": reasons,
            "current_block": block,
            "permit_hash": permit.get("permit_hash"),
            "consumed": consumption,
        }


class ChainseerWatcher:
    """Confirmed-block log watcher with debounced full rescans and outcomes."""

    def __init__(
        self,
        agent: Any,
        *,
        control_root: str | Path | None = None,
        config: WatchConfig | None = None,
        clock: Callable[[], float] = time.time,
        network: str | None = None,
    ):
        self.agent = agent
        self.network = network or getattr(
            agent, "network_key", "robinhood"
        )
        if self.network not in {"robinhood", "base"}:
            raise ValueError("unsupported EVM watcher network")
        self.config = config or WatchConfig()
        self.clock = clock
        root = Path(control_root or agent.chain_root) / "controls"
        self.store = WatchStore(root, self.network)
        self.outcomes = OutcomeCollector()
        self.calibration = CalibrationEngine(root)

    def _unbind(self) -> None:
        if hasattr(self.agent.rpc, "unbind_context"):
            self.agent.rpc.unbind_context()
        else:
            self.agent.rpc.context = None
            self.agent.rpc.ledger = None

    def _block(self, number: int) -> dict[str, Any]:
        self._unbind()
        return self.agent.rpc.get_block(number)

    def _quick_snapshot(
        self, token: str, block: int, pair: str | None
    ) -> dict[str, Any]:
        self._unbind()
        code = self.agent.rpc.get_code(token, block=block) or "0x"
        try:
            owner = self.agent.rpc.erc20_owner(token, block=block)
        except Exception:
            owner = None
        try:
            total_supply = self.agent.rpc.erc20_total_supply(token, block=block)
        except Exception:
            total_supply = None
        pair_code_hash = None
        if pair and ADDRESS_RE.fullmatch(pair):
            try:
                pair_code = self.agent.rpc.get_code(pair, block=block) or "0x"
                pair_code_hash = hashlib.sha256(
                    pair_code.encode("utf-8")
                ).hexdigest()
            except Exception:
                pair_code_hash = None
        snapshot = {
            "block": block,
            "code_hash": hashlib.sha256(code.encode("utf-8")).hexdigest(),
            "owner": owner,
            "total_supply": total_supply,
            "pair_address": pair,
            "pair_code_hash": pair_code_hash,
        }
        snapshot["fingerprint"] = canonical_hash(snapshot)
        return snapshot

    def _events(
        self,
        token: str,
        pair: str | None,
        from_block: int,
        to_block: int,
    ) -> dict[str, Any]:
        self._unbind()
        token_logs = self.agent.rpc.get_logs(
            from_block,
            to_block,
            address=token,
        )
        token_logs = [
            item for item in (token_logs or []) if isinstance(item, dict)
        ]
        critical_topics = {
            OWNERSHIP_TRANSFERRED_TOPIC,
            UPGRADED_TOPIC,
            ADMIN_CHANGED_TOPIC,
        }
        critical = [
            item
            for item in token_logs
            if str((item.get("topics") or [""])[0]).lower()
            in critical_topics
        ]
        transfers = [
            item
            for item in token_logs
            if str((item.get("topics") or [""])[0]).lower()
            == TRANSFER_TOPIC
        ]
        contract_activity = [
            item
            for item in token_logs
            if str((item.get("topics") or [""])[0]).lower()
            not in critical_topics | {TRANSFER_TOPIC}
        ]
        lp_burns = []
        if pair and ADDRESS_RE.fullmatch(pair):
            lp_burns = self.agent.rpc.get_logs(
                from_block,
                to_block,
                address=pair,
                topics=[
                    TRANSFER_TOPIC,
                    None,
                    [_topic_address(ZERO_ADDRESS), _topic_address(DEAD_ADDRESS)],
                ],
            )
        event_names = {
            OWNERSHIP_TRANSFERRED_TOPIC: "ownership_transferred",
            UPGRADED_TOPIC: "implementation_upgraded",
            ADMIN_CHANGED_TOPIC: "admin_changed",
        }
        critical_events = []
        for item in (critical or [])[:50]:
            if not isinstance(item, dict):
                continue
            topics = item.get("topics") or []
            topic0 = str(topics[0]).lower() if topics else ""
            critical_events.append(
                {
                    "event_type": event_names.get(topic0, "privileged_event"),
                    "block": item.get("blockNumber"),
                    "transaction_hash": item.get("transactionHash"),
                    "log_index": item.get("logIndex"),
                    "event_hash": canonical_hash(item),
                }
            )
        return {
            "critical_count": len(critical or []),
            "critical_events": critical_events,
            "transfer_count": len(transfers or []),
            "contract_activity_count": len(contract_activity),
            "lp_burn_count": len(lp_burns or []),
            "critical_log_hash": canonical_hash(critical or []),
            "transfer_log_hash": canonical_hash(transfers or []),
            "contract_activity_log_hash": canonical_hash(
                contract_activity
            ),
            "lp_burn_log_hash": canonical_hash(lp_burns or []),
        }

    def _seal_transition(self, alert: dict[str, Any]) -> dict[str, Any] | None:
        verdict, ring = self.agent.poq_module.gate_and_seal(
            self.agent.tc,
            (
                f"Watcher detected {alert['reason']} for "
                f"{alert['token_address']} at block {alert['block']}."
            ),
            context=canonical_json(alert),
            ring_type="watch_transition",
            external_scores={
                "coherence": 240,
                "relevance": 250,
                "novelty": 220,
                "consistency": 245,
                "depth": 230,
                "covenant": 250,
            },
            declared_evidence=1,
            extra_payload=alert,
        )
        if ring is None:
            return None
        return {
            "ring": ring.get("index"),
            "ring_hash": ring.get("ring_hash"),
            "decision": verdict.get("decision"),
        }

    def run_once(self) -> dict[str, Any]:
        now = self.clock()
        state = self.store.load()
        if not state["subscriptions"]:
            return {
                "head": None,
                "safe_block": None,
                "subscriptions": 0,
                "rescans": 0,
                "alerts": 0,
                "outcomes": 0,
                "errors": [],
                "calibration": self.calibration.propose(
                    self.agent.tc.load()
                ),
            }
        self._unbind()
        head = self.agent.rpc.get_block_number()
        safe_block = max(0, head - self.config.confirmations)
        summary = {
            "head": head,
            "safe_block": safe_block,
            "subscriptions": len(state["subscriptions"]),
            "rescans": 0,
            "alerts": 0,
            "outcomes": 0,
            "errors": [],
        }
        for key, subscription in state["subscriptions"].items():
            token = subscription["token_address"]
            try:
                last = subscription.get("last_processed_block")
                if last is not None and last > safe_block:
                    # A provider moving backwards is treated as reorg/rollback.
                    subscription["last_processed_block"] = max(
                        0, safe_block - self.config.confirmations
                    )
                    subscription["last_processed_hash"] = None
                    last = subscription["last_processed_block"]
                if last is not None and subscription.get("last_processed_hash"):
                    canonical = self._block(int(last))
                    if canonical.get("hash") != subscription["last_processed_hash"]:
                        alert = {
                            "schema_version": CONTROL_SCHEMA_VERSION,
                            "network": self.network,
                            "type": "reorg",
                            "reason": "confirmed_block_hash_changed",
                            "token_address": token,
                            "block": int(last),
                            "observed_at": utc_now_iso(now),
                        }
                        alert["timechain"] = self._seal_transition(alert)
                        self.store.append_alert(alert)
                        summary["alerts"] += 1
                        subscription["last_processed_block"] = max(
                            0, int(last) - self.config.confirmations
                        )
                        subscription["last_processed_hash"] = None
                        last = subscription["last_processed_block"]
                if last is not None and int(last) >= safe_block:
                    continue

                latest = subscription.get("latest_analysis") or {}
                pair = latest.get("pair_address")
                from_block = safe_block if last is None else int(last) + 1
                events = self._events(token, pair, from_block, safe_block)
                quick = self._quick_snapshot(token, safe_block, pair)
                fingerprint_changed = (
                    subscription.get("quick_fingerprint") is not None
                    and subscription.get("quick_fingerprint")
                    != quick["fingerprint"]
                )
                last_full = subscription.get("last_full_scan_block")
                due = self.outcomes.due_horizons(
                    subscription,
                    now=now,
                    horizons=self.config.outcome_horizons_seconds,
                )
                reasons = []
                if latest == {}:
                    reasons.append("initial_baseline")
                if events["critical_count"] or events["lp_burn_count"]:
                    reasons.append("critical_event")
                if events["contract_activity_count"]:
                    reasons.append("contract_activity_sellability_probe")
                if fingerprint_changed:
                    reasons.append("critical_fingerprint_changed")
                if (
                    events["transfer_count"]
                    and (
                        last_full is None
                        or safe_block - int(last_full)
                        >= min(
                            self.config.holder_rescan_blocks,
                            self.config.sellability_rescan_blocks,
                        )
                    )
                ):
                    reasons.append("activity_sellability_probe")
                if (
                    last_full is None
                    or safe_block - int(last_full)
                    >= self.config.max_rescan_blocks
                ):
                    reasons.append("maximum_refresh_interval")
                if due:
                    reasons.append("outcome_horizon_due")

                if reasons:
                    report = self.agent.analyze_token(
                        token, full_report=False, block_pin=safe_block
                    )
                    if report.get("error"):
                        raise RuntimeError(report["error"])
                    current = _analysis_view(report)
                    current_pair = current.get("pair_address")
                    if current_pair != pair:
                        quick = self._quick_snapshot(
                            token, safe_block, current_pair
                        )
                    drift = _diff_views(latest or None, current)
                    emitted = self.outcomes.collect(
                        self.agent,
                        subscription,
                        report,
                        now=now,
                        horizons=self.config.outcome_horizons_seconds,
                    )
                    summary["outcomes"] += len(emitted)
                    summary["rescans"] += 1
                    subscription["latest_analysis"] = current
                    subscription["last_full_scan_block"] = safe_block

                    score_delta = abs(
                        _safe_float(current.get("score"))
                        - _safe_float(latest.get("score"))
                    )
                    new_stops = sorted(
                        set(current.get("hard_stop_codes") or [])
                        - set(latest.get("hard_stop_codes") or [])
                    )
                    material = (
                        "initial_baseline" not in reasons
                        and (
                            new_stops
                            or score_delta >= self.config.score_alert_delta
                            or "critical_event" in reasons
                            or "critical_fingerprint_changed" in reasons
                        )
                    )
                    baselines = subscription.setdefault("analyses", [])
                    last_baseline_at = (
                        _parse_time(baselines[-1].get("timestamp"))
                        if baselines
                        else None
                    )
                    baseline_due = (
                        not baselines
                        or material
                        or last_baseline_at is None
                        or now - last_baseline_at
                        >= self.config.outcome_baseline_interval_seconds
                    )
                    if baseline_due:
                        baselines.append(current)
                        subscription["analyses"] = baselines[-200:]
                    if material:
                        critical_events = _evm_critical_events(
                            latest,
                            current,
                            events,
                            new_stops,
                        )
                        alert = {
                            "schema_version": CONTROL_SCHEMA_VERSION,
                            "network": self.network,
                            "type": "state_change",
                            "reason": ",".join(reasons),
                            "token_address": token,
                            "block": safe_block,
                            "observed_at": utc_now_iso(now),
                            "events": events,
                            "drift": drift,
                            "new_hard_stops": new_stops,
                            "critical_events": critical_events,
                            "analysis_ring": report.get("analysis_ring"),
                            "analysis_ring_hash": report.get("analysis_ring_hash"),
                        }
                        alert["alert_hash"] = canonical_hash(alert)
                        alert["timechain"] = self._seal_transition(alert)
                        self.store.append_alert(alert)
                        summary["alerts"] += 1

                block_info = self._block(safe_block)
                subscription["last_processed_block"] = safe_block
                subscription["last_processed_hash"] = block_info.get("hash")
                subscription["quick_fingerprint"] = quick["fingerprint"]
                subscription["quick_snapshot"] = quick
                subscription["last_events"] = events
            except Exception as exc:
                summary["errors"].append(
                    {"token_address": token, "error": str(exc)}
                )
                subscription["last_error"] = {
                    "at": utc_now_iso(now),
                    "message": str(exc),
                }
        self.store.save(state)
        summary["calibration"] = self.calibration.propose(
            self.agent.tc.load()
        )
        return summary

    def run_forever(self) -> None:
        while True:
            summary = self.run_once()
            print(canonical_json(summary), flush=True)
            time.sleep(self.config.poll_seconds)


class SolanaEventWatcher:
    """Confirmed Solana event index with material-delta rescans and reconciliation."""

    CRITICAL_EVENT_TYPES = {
        "owner_program_changed",
        "mint_authority_changed",
        "freeze_authority_changed",
        "token_extensions_changed",
        "supply_changed",
        "market_disappeared",
        "primary_market_changed",
        "liquidity_removed",
    }

    def __init__(
        self,
        analyzer: Any,
        *,
        timechain_agent: Any | None = None,
        control_root: str | Path,
        config: SolanaWatchConfig | None = None,
        clock: Callable[[], float] = time.time,
    ):
        self.analyzer = analyzer
        self.timechain_agent = (
            timechain_agent
            if timechain_agent is not None
            else getattr(analyzer, "timechain_agent", None)
        )
        self.config = config or SolanaWatchConfig()
        self.clock = clock
        self.store = SolanaWatchStore(Path(control_root) / "controls")

    @staticmethod
    def _event(
        event_type: str,
        *,
        severity: str,
        before: Any,
        after: Any,
        triggers_rescan: bool,
        evidence: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        value = {
            "event_type": event_type,
            "severity": severity,
            "before": before,
            "after": after,
            "triggers_rescan": triggers_rescan,
            "evidence": evidence or {},
        }
        value["event_hash"] = canonical_hash(value)
        return value

    def _quick_snapshot(
        self,
        mint: str,
        *,
        observed_slot: int,
        seen_signatures: set[str],
        previous: dict[str, Any] | None = None,
    ) -> tuple[
        dict[str, Any],
        list[dict[str, Any]],
        list[str],
        list[dict[str, str]],
    ]:
        infrastructure_indeterminate: list[dict[str, str]] = []
        account_result = (
            self.analyzer.rpc.get_account_info(mint, encoding="jsonParsed")
            or {}
        )
        account_value = account_result.get("value")
        if not account_value:
            raise RuntimeError("confirmed Solana mint account is unavailable")
        parsed = ((account_value.get("data") or {}).get("parsed") or {})
        info = parsed.get("info") or {}
        if parsed.get("type") != "mint":
            raise RuntimeError("watched Solana address is no longer a mint")

        supply_result = self.analyzer.rpc.get_token_supply(mint) or {}
        supply_value = supply_result.get("value") or {}
        supply_raw = _safe_int(
            supply_value.get("amount"),
            _safe_int(info.get("supply")),
        )
        previous = previous or {}
        try:
            largest_result = (
                self.analyzer.rpc.get_token_largest_accounts(mint) or {}
            )
            largest = largest_result.get("value") or []
            normalized_accounts = [
                {
                    "address": str(item.get("address") or ""),
                    "amount": _safe_int(item.get("amount")),
                }
                for item in largest[:20]
                if isinstance(item, dict)
            ]
            top1_raw = sum(
                item["amount"] for item in normalized_accounts[:1]
            )
            top10_raw = sum(
                item["amount"] for item in normalized_accounts[:10]
            )
            top1_pct = (
                round(100 * top1_raw / supply_raw, 6)
                if supply_raw > 0
                else None
            )
            top10_pct = (
                round(100 * top10_raw / supply_raw, 6)
                if supply_raw > 0
                else None
            )
            largest_accounts_hash = canonical_hash(normalized_accounts)
            holder_slot = _safe_int(
                (largest_result.get("context") or {}).get("slot"),
                observed_slot,
            )
        except Exception as exc:
            message = credential_safe_error(exc)
            infrastructure_indeterminate.append(
                {
                    "source": "solana_rpc.getTokenLargestAccounts",
                    "message": message,
                }
            )
            top1_pct = previous.get("top1_total_supply_pct")
            top10_pct = previous.get("top10_total_supply_pct")
            largest_accounts_hash = previous.get(
                "largest_accounts_hash"
            )
            holder_slot = previous.get("holder_slot")

        try:
            pairs = self.analyzer.dexscreener.token_pairs(mint)
            pair = self.analyzer._market_pair(mint, pairs)
            base = (pair or {}).get("baseToken") or {}
            market = {
                "pair_address": (pair or {}).get("pairAddress"),
                "dex_id": (pair or {}).get("dexId"),
                "price_usd": (
                    _safe_float((pair or {}).get("priceUsd"), None)
                    if base.get("address") == mint
                    else None
                ),
                "liquidity_usd": _safe_float(
                    ((pair or {}).get("liquidity") or {}).get("usd"),
                    None,
                ),
                "market_cap": _safe_float(
                    (pair or {}).get("marketCap"),
                    None,
                ),
                "fdv": _safe_float((pair or {}).get("fdv"), None),
            }
        except Exception as exc:
            message = credential_safe_error(exc)
            infrastructure_indeterminate.append(
                {
                    "source": "dexscreener.token_pairs",
                    "message": message,
                }
            )
            market = dict(previous.get("market") or {})
        extensions = sorted(
            str(value)
            for value in self.analyzer._extension_names(info)
        )
        snapshot = {
            "observed_slot": observed_slot,
            "account_slot": _safe_int(
                (account_result.get("context") or {}).get("slot"),
                observed_slot,
            ),
            "supply_slot": _safe_int(
                (supply_result.get("context") or {}).get("slot"),
                observed_slot,
            ),
            "holder_slot": holder_slot,
            "owner_program": account_value.get("owner"),
            "mint_authority": info.get("mintAuthority"),
            "freeze_authority": info.get("freezeAuthority"),
            "extensions": extensions,
            "decimals": _safe_int(
                supply_value.get("decimals"),
                _safe_int(info.get("decimals")),
            ),
            "supply_raw": supply_raw,
            "top1_total_supply_pct": top1_pct,
            "top10_total_supply_pct": top10_pct,
            "largest_accounts_hash": largest_accounts_hash,
            "market": market,
        }
        fingerprint_payload = {
            key: value
            for key, value in snapshot.items()
            if key
            not in {
                "observed_slot",
                "account_slot",
                "supply_slot",
                "holder_slot",
            }
        }
        snapshot["fingerprint"] = canonical_hash(fingerprint_payload)

        try:
            signatures = self.analyzer.rpc.get_signatures_for_address(
                mint,
                limit=self.config.signature_limit,
            )
        except Exception as exc:
            message = credential_safe_error(exc)
            infrastructure_indeterminate.append(
                {
                    "source": "solana_rpc.getSignaturesForAddress",
                    "message": message,
                }
            )
            signatures = []
        new_signatures = [
            {
                "signature": str(item.get("signature") or ""),
                "slot": _safe_int(item.get("slot")),
                "confirmation_status": item.get("confirmationStatus"),
                "failed": item.get("err") is not None,
                "block_time": item.get("blockTime"),
            }
            for item in signatures or []
            if isinstance(item, dict)
            and item.get("signature")
            and str(item.get("signature")) not in seen_signatures
        ]
        signature_order = [
            str(item.get("signature"))
            for item in signatures or []
            if isinstance(item, dict) and item.get("signature")
        ]
        snapshot["infrastructure_indeterminate"] = (
            infrastructure_indeterminate
        )
        return (
            snapshot,
            new_signatures,
            signature_order,
            infrastructure_indeterminate,
        )

    def _classify_events(
        self,
        before: dict[str, Any] | None,
        after: dict[str, Any],
        new_signatures: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        if not before:
            return []
        events: list[dict[str, Any]] = []

        def changed(
            field: str,
            event_type: str,
            severity: str = "high",
        ) -> None:
            if before.get(field) != after.get(field):
                events.append(
                    self._event(
                        event_type,
                        severity=severity,
                        before=before.get(field),
                        after=after.get(field),
                        triggers_rescan=True,
                    )
                )

        changed("owner_program", "owner_program_changed", "critical")
        changed("mint_authority", "mint_authority_changed", "critical")
        changed("freeze_authority", "freeze_authority_changed", "critical")
        changed("extensions", "token_extensions_changed", "critical")
        changed("supply_raw", "supply_changed", "high")

        old_market = before.get("market") or {}
        new_market = after.get("market") or {}
        old_pair = old_market.get("pair_address")
        new_pair = new_market.get("pair_address")
        if old_pair != new_pair:
            if old_pair and not new_pair:
                event_type, severity = "market_disappeared", "critical"
            elif not old_pair and new_pair:
                event_type, severity = "market_created", "high"
            else:
                event_type, severity = "primary_market_changed", "critical"
            events.append(
                self._event(
                    event_type,
                    severity=severity,
                    before=old_pair,
                    after=new_pair,
                    triggers_rescan=True,
                    evidence={
                        "before_dex": old_market.get("dex_id"),
                        "after_dex": new_market.get("dex_id"),
                    },
                )
            )

        liquidity_delta = _relative_delta_bps(
            old_market.get("liquidity_usd"),
            new_market.get("liquidity_usd"),
        )
        liquidity_removed_bps = None
        old_liquidity = _safe_float(old_market.get("liquidity_usd"), 0.0)
        new_liquidity = _safe_float(new_market.get("liquidity_usd"), 0.0)
        if old_liquidity > 0 and new_liquidity < old_liquidity:
            liquidity_removed_bps = (
                (old_liquidity - new_liquidity) / old_liquidity * 10_000
            )
        if (
            old_pair == new_pair
            and liquidity_removed_bps is not None
            and liquidity_removed_bps >= 3_000
        ):
            events.append(
                self._event(
                    "liquidity_removed",
                    severity="critical",
                    before=old_market.get("liquidity_usd"),
                    after=new_market.get("liquidity_usd"),
                    triggers_rescan=True,
                    evidence={
                        "removed_bps": round(liquidity_removed_bps, 2)
                    },
                )
            )
        elif (
            old_pair == new_pair
            and liquidity_delta is not None
            and liquidity_delta >= self.config.liquidity_delta_bps
        ):
            events.append(
                self._event(
                    "liquidity_changed",
                    severity="high",
                    before=old_market.get("liquidity_usd"),
                    after=new_market.get("liquidity_usd"),
                    triggers_rescan=True,
                    evidence={"delta_bps": round(liquidity_delta, 2)},
                )
            )

        price_delta = _relative_delta_bps(
            old_market.get("price_usd"),
            new_market.get("price_usd"),
        )
        if (
            old_pair == new_pair
            and price_delta is not None
            and price_delta >= self.config.price_delta_bps
        ):
            events.append(
                self._event(
                    "price_moved",
                    severity="medium",
                    before=old_market.get("price_usd"),
                    after=new_market.get("price_usd"),
                    triggers_rescan=True,
                    evidence={"delta_bps": round(price_delta, 2)},
                )
            )

        holder_delta = max(
            abs(
                _safe_float(after.get(field))
                - _safe_float(before.get(field))
            )
            * 100
            for field in (
                "top1_total_supply_pct",
                "top10_total_supply_pct",
            )
        )
        holder_set_changed = (
            before.get("largest_accounts_hash")
            != after.get("largest_accounts_hash")
        )
        if holder_delta >= self.config.holder_delta_bps:
            events.append(
                self._event(
                    "holder_concentration_changed",
                    severity="high",
                    before={
                        "top1": before.get("top1_total_supply_pct"),
                        "top10": before.get("top10_total_supply_pct"),
                    },
                    after={
                        "top1": after.get("top1_total_supply_pct"),
                        "top10": after.get("top10_total_supply_pct"),
                    },
                    triggers_rescan=True,
                    evidence={"maximum_delta_bps": round(holder_delta, 2)},
                )
            )
        elif holder_set_changed:
            events.append(
                self._event(
                    "largest_accounts_rotated",
                    severity="observational",
                    before=before.get("largest_accounts_hash"),
                    after=after.get("largest_accounts_hash"),
                    triggers_rescan=False,
                )
            )

        if new_signatures:
            events.append(
                self._event(
                    "confirmed_mint_activity",
                    severity="observational",
                    before=None,
                    after=len(new_signatures),
                    triggers_rescan=False,
                    evidence={
                        "signature_count": len(new_signatures),
                        "signature_hash": canonical_hash(new_signatures),
                        "failed_count": sum(
                            bool(item.get("failed"))
                            for item in new_signatures
                        ),
                    },
                )
            )
        return events

    def _seal_transition(
        self,
        alert: dict[str, Any],
    ) -> dict[str, Any] | None:
        agent = self.timechain_agent
        if agent is None:
            return None
        verdict, ring = agent.poq_module.gate_and_seal(
            agent.tc,
            (
                f"Solana watcher detected {alert['reason']} for "
                f"{alert['token_address']} at confirmed slot "
                f"{alert['slot']}."
            ),
            context=canonical_json(alert),
            ring_type="solana_watch_transition",
            external_scores={
                "coherence": 245,
                "relevance": 252,
                "novelty": 235,
                "consistency": 245,
                "depth": 242,
                "covenant": 252,
            },
            declared_evidence=max(1, len(alert.get("events") or [])),
            extra_payload=alert,
        )
        if ring is None:
            return None
        return {
            "ring": ring.get("index"),
            "ring_hash": ring.get("ring_hash"),
            "decision": verdict.get("decision"),
        }

    def run_once(self) -> dict[str, Any]:
        now = self.clock()
        state = self.store.load()
        if not state["subscriptions"]:
            return {
                "head_slot": None,
                "subscriptions": 0,
                "events_observed": 0,
                "material_events": 0,
                "rescans": 0,
                "alerts": 0,
                "infrastructure_indeterminate": [],
                "errors": [],
            }

        head_slot = self.analyzer.rpc.get_slot()
        summary = {
            "head_slot": head_slot,
            "subscriptions": len(state["subscriptions"]),
            "events_observed": 0,
            "material_events": 0,
            "rescans": 0,
            "alerts": 0,
            "infrastructure_indeterminate": [],
            "errors": [],
        }
        for mint, subscription in state["subscriptions"].items():
            try:
                last_slot = subscription.get("last_observed_slot")
                if last_slot is not None and int(last_slot) >= head_slot:
                    continue
                seen = set(subscription.get("recent_signatures") or [])
                (
                    quick,
                    new_signatures,
                    signature_order,
                    infrastructure_indeterminate,
                ) = self._quick_snapshot(
                    mint,
                    observed_slot=head_slot,
                    seen_signatures=seen,
                    previous=subscription.get("quick_snapshot"),
                )
                summary["infrastructure_indeterminate"].extend(
                    {
                        "token_address": mint,
                        **item,
                    }
                    for item in infrastructure_indeterminate
                )
                events = self._classify_events(
                    subscription.get("quick_snapshot"),
                    quick,
                    new_signatures,
                )
                summary["events_observed"] += len(events)
                material = [
                    event
                    for event in events
                    if event.get("triggers_rescan")
                ]
                summary["material_events"] += len(material)

                pending_by_hash = {
                    str(event.get("event_hash")): event
                    for event in subscription.get("pending_events") or []
                    if isinstance(event, dict) and event.get("event_hash")
                }
                for event in material:
                    pending_by_hash[event["event_hash"]] = event
                pending = list(pending_by_hash.values())[-50:]

                latest = subscription.get("latest_analysis") or {}
                last_full = subscription.get("last_full_scan_slot")
                reconcile_due = (
                    last_full is None
                    or head_slot - int(last_full)
                    >= self.config.max_reconcile_slots
                )
                critical_pending = any(
                    event.get("event_type") in self.CRITICAL_EVENT_TYPES
                    for event in pending
                )
                cooldown_clear = (
                    last_full is None
                    or head_slot - int(last_full)
                    >= self.config.minimum_rescan_slots
                )
                activity_probe_due = (
                    bool(new_signatures)
                    and (
                        last_full is None
                        or head_slot - int(last_full)
                        >= self.config.sellability_probe_slots
                    )
                )
                should_rescan = (
                    not latest
                    or reconcile_due
                    or activity_probe_due
                    or (bool(pending) and (critical_pending or cooldown_clear))
                )

                if should_rescan:
                    report = self.analyzer.analyze_token(mint)
                    current = _solana_analysis_view(report)
                    drift = _solana_diff_views(latest or None, current)
                    summary["rescans"] += 1
                    subscription["latest_analysis"] = current
                    subscription["last_full_scan_slot"] = head_slot
                    baselines = subscription.setdefault("analyses", [])
                    baselines.append(current)
                    subscription["analyses"] = baselines[-200:]

                    new_stops = sorted(
                        set(current.get("hard_stop_codes") or [])
                        - set(latest.get("hard_stop_codes") or [])
                    )
                    score_delta = abs(
                        _safe_float(current.get("score"))
                        - _safe_float(latest.get("score"))
                    )
                    reconciliation_drift = (
                        bool(latest)
                        and not pending
                        and (bool(new_stops) or score_delta >= 5.0)
                    )
                    if pending or reconciliation_drift:
                        reasons = [
                            str(event.get("event_type"))
                            for event in pending
                        ]
                        if reconciliation_drift:
                            reasons.append("reconciliation_drift")
                        alert = {
                            "schema_version": CONTROL_SCHEMA_VERSION,
                            "network": "solana",
                            "type": "state_change",
                            "reason": ",".join(dict.fromkeys(reasons)),
                            "token_address": mint,
                            "slot": head_slot,
                            "observed_at": utc_now_iso(now),
                            "events": pending,
                            "observational_events": [
                                event
                                for event in events
                                if not event.get("triggers_rescan")
                            ],
                            "drift": drift,
                            "new_hard_stops": new_stops,
                            "critical_events": _solana_critical_events(
                                latest,
                                current,
                                pending,
                                new_stops,
                            ),
                            "analysis_ring": report.get("analysis_ring"),
                            "analysis_ring_hash": report.get(
                                "analysis_ring_hash"
                            ),
                        }
                        alert["alert_hash"] = canonical_hash(alert)
                        alert["timechain"] = self._seal_transition(alert)
                        self.store.append_alert(alert)
                        summary["alerts"] += 1
                    pending = []

                recent = list(
                    dict.fromkeys(
                        signature_order
                        + list(subscription.get("recent_signatures") or [])
                    )
                )[: self.config.recent_signature_capacity]
                subscription["recent_signatures"] = recent
                subscription["last_signature"] = recent[0] if recent else None
                subscription["last_observed_slot"] = head_slot
                subscription["quick_snapshot"] = quick
                subscription["quick_fingerprint"] = quick["fingerprint"]
                subscription["pending_events"] = pending
                subscription["last_events"] = events
                subscription["last_infrastructure_indeterminate"] = (
                    infrastructure_indeterminate
                )
                subscription["last_error"] = None
            except Exception as exc:
                message = credential_safe_error(exc)
                summary["errors"].append(
                    {"token_address": mint, "error": message}
                )
                subscription["last_error"] = {
                    "at": utc_now_iso(now),
                    "message": message,
                }
        self.store.save(state)
        return summary

    def run_forever(self) -> None:
        while True:
            print(canonical_json(self.run_once()), flush=True)
            time.sleep(self.config.poll_seconds)


def _load_json_argument(value: str) -> dict[str, Any]:
    path = Path(value)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return json.loads(value)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chainseer watcher, calibration, and TradePermit controls"
    )
    parser.add_argument("--rpc-url", default=None)
    parser.add_argument("--chain-root", default=None)
    parser.add_argument("--control-root", default=None)
    commands = parser.add_subparsers(dest="command", required=True)

    watch = commands.add_parser("watch")
    watch_commands = watch.add_subparsers(dest="watch_command", required=True)
    watch_add = watch_commands.add_parser("add")
    watch_add.add_argument("token")
    watch_remove = watch_commands.add_parser("remove")
    watch_remove.add_argument("token")
    watch_commands.add_parser("status")
    watch_commands.add_parser("once")
    watch_run = watch_commands.add_parser("run")
    watch_run.add_argument("--poll-seconds", type=float, default=3.0)
    watch_run.add_argument("--confirmations", type=int, default=2)

    calibration = commands.add_parser("calibration")
    calibration_commands = calibration.add_subparsers(
        dest="calibration_command", required=True
    )
    calibration_commands.add_parser("status")
    calibration_commands.add_parser("propose")
    adopt = calibration_commands.add_parser("adopt")
    adopt.add_argument("proposal")

    permit = commands.add_parser("permit")
    permit_commands = permit.add_subparsers(dest="permit_command", required=True)
    create = permit_commands.add_parser("create")
    create.add_argument("token")
    create.add_argument("--amount-in", required=True)
    create.add_argument("--recipient", required=True)
    create.add_argument("--quote", required=True)
    create.add_argument("--private-routing", action="store_true")
    verify = permit_commands.add_parser("verify")
    verify.add_argument("permit")
    verify.add_argument("--consume", action="store_true")
    args = parser.parse_args()

    from chainseer import Chainseer, RPC_URL

    chain_root = Path(
        args.chain_root
        or (Path(__file__).resolve().parent / "chainseer_chain")
    ).resolve()
    lease = SingleWriterLease(chain_root)
    lease.acquire()
    try:
        agent = Chainseer(
            rpc_url=args.rpc_url or RPC_URL,
            chain_root=str(chain_root),
        )
        watcher = ChainseerWatcher(
            agent,
            control_root=args.control_root,
            config=WatchConfig(
                poll_seconds=getattr(args, "poll_seconds", 3.0),
                confirmations=getattr(args, "confirmations", 2),
            ),
        )
        if args.command == "watch":
            if args.watch_command == "add":
                result = watcher.store.subscribe(args.token)
            elif args.watch_command == "remove":
                result = {"removed": watcher.store.unsubscribe(args.token)}
            elif args.watch_command == "status":
                result = watcher.store.load()
            elif args.watch_command == "once":
                result = watcher.run_once()
            else:
                try:
                    watcher.run_forever()
                except KeyboardInterrupt:
                    print("watcher stopped")
                return
        elif args.command == "calibration":
            if args.calibration_command == "status":
                result = {
                    "policy": asdict(watcher.calibration.policy()),
                    "metrics": watcher.calibration.summarize(agent.tc.load()),
                }
            elif args.calibration_command == "propose":
                result = watcher.calibration.propose(agent.tc.load())
            else:
                result = watcher.calibration.adopt(
                    _load_json_argument(args.proposal), agent
                )
        else:
            guard = TradePermitGuard(
                agent, control_root=args.control_root
            )
            if args.permit_command == "create":
                result = guard.authorize(
                    args.token,
                    amount_in=args.amount_in,
                    recipient=args.recipient,
                    quote=_load_json_argument(args.quote),
                    private_routing=args.private_routing,
                )
            else:
                result = guard.verify(
                    _load_json_argument(args.permit),
                    consume=args.consume,
                )
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
    finally:
        lease.release()


if __name__ == "__main__":
    main()
