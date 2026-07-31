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
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import chainseer_alerts


CONTROL_SCHEMA_VERSION = "1.0"
PERMIT_VERSION = "1.0"
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
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
        if self.holder_rescan_blocks < 1 or self.max_rescan_blocks < 1:
            raise ValueError("rescan block intervals must be positive")
        if self.outcome_baseline_interval_seconds < 3600:
            raise ValueError("outcome baseline interval must be at least one hour")


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

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.state_path = self.root / "watcher_state.json"
        self.alert_path = self.root / "watcher_alerts.jsonl"

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

    def subscribe(self, token: str) -> dict[str, Any]:
        if not ADDRESS_RE.fullmatch(token):
            raise ValueError("invalid token address")
        state = self.load()
        key = token.lower()
        state["subscriptions"].setdefault(
            key,
            {
                "token_address": token,
                "created_at": utc_now_iso(),
                "last_processed_block": None,
                "last_processed_hash": None,
                "last_full_scan_block": None,
                "quick_fingerprint": None,
                "latest_analysis": None,
                "analyses": [],
                "completed_outcomes": [],
            },
        )
        self.save(state)
        return state["subscriptions"][key]

    def unsubscribe(self, token: str) -> bool:
        state = self.load()
        removed = state["subscriptions"].pop(token.lower(), None) is not None
        if removed:
            self.save(state)
        return removed


def _analysis_view(report: dict[str, Any]) -> dict[str, Any]:
    data = report.get("data") or {}
    analysis = report.get("analysis") or {}
    dex = data.get("dex_pairs") or {}
    contract = data.get("contract_audit") or {}
    custody = data.get("lp_lock") or {}
    tax = data.get("goplus_security") or {}
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
        "custody_state": custody.get("state"),
        "buy_tax": tax.get("buy_tax"),
        "sell_tax": tax.get("sell_tax"),
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
        "custody_state",
        "buy_tax",
        "sell_tax",
        "pair_address",
    )
    changes = {
        key: {"before": before.get(key), "after": after.get(key)}
        for key in keys
        if before.get(key) != after.get(key)
    }
    return {"initial": False, "changes": changes}


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
        self.reliability_path = self.root / "calibration_reliability.json"

    def policy(self) -> CalibrationPolicy:
        if not self.policy_path.exists():
            return CalibrationPolicy()
        return CalibrationPolicy.from_dict(
            json.loads(self.policy_path.read_text(encoding="utf-8"))
        )

    @staticmethod
    def _latest_outcomes(rings: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
        """Dedupe analysis_outcome rings to one per analysis_ring, keeping
        whichever has the latest (largest) outcome horizon -- a later horizon
        supersedes an earlier label for the same original analysis. Shared by
        summarize() (confusion-matrix metrics) and reliability_curve()
        (score-vs-outcome calibration) so both use identical outcome selection.
        """
        outcomes = [
            ring
            for ring in rings
            if ring.get("ring_type") == "analysis_outcome"
        ]
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
        return latest

    @staticmethod
    def summarize(rings: list[dict[str, Any]]) -> dict[str, Any]:
        latest = CalibrationEngine._latest_outcomes(rings)
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

    @staticmethod
    def reliability_curve(
        rings: list[dict[str, Any]], *, bins: int = 10
    ) -> dict[str, Any]:
        """Bucket each analysis's predicted legitimacy_score against whether
        an adverse security event was later observed, so a score of "80" can
        be checked against what actually happened to the tokens that scored
        near 80 -- not just whether the current policy's single threshold
        looks right in aggregate (that's what summarize()/propose() already
        do). This is a reliability diagram plus a Brier score, not a
        replacement for the tighten-only confusion-matrix policy: it never
        changes calibration_policy.json by itself.

        Score -> "predicted probability the token stays safe" is score/100.
        The event scored by Brier is the adverse outcome, so predicted
        probability of THAT event is 1 - score/100. A well-calibrated agent's
        bins should show observed_safe_rate close to mean_predicted_safe_pct
        in every bin; a bin where the gap is large and sample_size is
        non-trivial is where the agent is over- or under-confident.
        """
        latest = CalibrationEngine._latest_outcomes(rings)
        samples: list[tuple[float, bool]] = []
        for ring in latest.values():
            payload = ring.get("payload") or {}
            calibration = payload.get("calibration") or {}
            score = calibration.get("original_legitimacy_score")
            if score is None:
                continue
            score = _safe_float(score, -1.0)
            if score < 0 or score > 100:
                continue
            adverse = bool(calibration.get("adverse_security_event"))
            samples.append((score, adverse))

        bins = max(1, int(bins))
        width = 100.0 / bins
        buckets: list[dict[str, Any]] = [
            {
                "bin_index": i,
                "score_range": [round(i * width, 1), round((i + 1) * width, 1)],
                "sample_size": 0,
                "adverse_count": 0,
                "mean_predicted_score": None,
                "observed_safe_rate": None,
                "mean_predicted_safe_pct": None,
                "calibration_gap_pct": None,
            }
            for i in range(bins)
        ]
        for score, adverse in samples:
            index = min(bins - 1, int(score // width))
            bucket = buckets[index]
            bucket["sample_size"] += 1
            bucket["adverse_count"] += 1 if adverse else 0
            bucket.setdefault("_score_sum", 0.0)
            bucket["_score_sum"] += score

        for bucket in buckets:
            n = bucket.pop("sample_size")
            score_sum = bucket.pop("_score_sum", 0.0)
            adverse_count = bucket["adverse_count"]
            bucket["sample_size"] = n
            if n == 0:
                continue
            mean_score = score_sum / n
            observed_safe_rate = 1.0 - (adverse_count / n)
            bucket["mean_predicted_score"] = round(mean_score, 2)
            bucket["observed_safe_rate"] = round(observed_safe_rate, 4)
            bucket["mean_predicted_safe_pct"] = round(mean_score, 2)
            bucket["calibration_gap_pct"] = round(
                mean_score - (observed_safe_rate * 100), 2
            )

        total = len(samples)
        if total == 0:
            brier_score = None
            baseline_brier_score = None
            skill_vs_naive_baseline = None
        else:
            base_rate_adverse = sum(1 for _, adverse in samples if adverse) / total
            brier_score = round(
                sum(
                    ((1.0 - score / 100.0) - (1.0 if adverse else 0.0)) ** 2
                    for score, adverse in samples
                )
                / total,
                4,
            )
            # A naive predictor that always outputs the observed base rate,
            # for context: an agent whose Brier score is not meaningfully
            # better than this baseline is not adding predictive skill, no
            # matter how confident-looking its individual scores are.
            baseline_brier_score = round(
                sum(
                    (base_rate_adverse - (1.0 if adverse else 0.0)) ** 2
                    for _, adverse in samples
                )
                / total,
                4,
            )
            skill_vs_naive_baseline = (
                round(1.0 - (brier_score / baseline_brier_score), 4)
                if baseline_brier_score
                else None
            )

        largest_gap = max(
            (b for b in buckets if b["sample_size"] > 0),
            key=lambda b: abs(b["calibration_gap_pct"] or 0.0),
            default=None,
        )
        return {
            "schema_version": CONTROL_SCHEMA_VERSION,
            "sample_size": total,
            "bins": buckets,
            "brier_score": brier_score,
            "baseline_brier_score": baseline_brier_score,
            "skill_vs_naive_baseline": skill_vs_naive_baseline,
            "most_miscalibrated_bin": (
                {
                    "score_range": largest_gap["score_range"],
                    "sample_size": largest_gap["sample_size"],
                    "calibration_gap_pct": largest_gap["calibration_gap_pct"],
                }
                if largest_gap is not None
                else None
            ),
            "interpretation": (
                "brier_score: 0=perfect, 0.25=uninformative on a balanced "
                "sample, 1=worst possible. skill_vs_naive_baseline: fraction "
                "of the naive constant-prediction error this agent removes; "
                "<=0 means the score is not adding predictive value over "
                "just guessing the historical adverse-event rate. "
                "calibration_gap_pct: mean_predicted_score minus "
                "observed_safe_rate*100 per bin -- positive means "
                "overconfident (score implies safer than reality), negative "
                "means underconfident."
            ),
            "created_at": utc_now_iso(),
        }

    def reliability_report(self, rings: list[dict[str, Any]], *, bins: int = 10) -> dict[str, Any]:
        report = self.reliability_curve(rings, bins=bins)
        _atomic_write_json(self.reliability_path, report)
        return report

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
    ):
        self.agent = agent
        self.config = config or WatchConfig()
        self.clock = clock
        root = Path(control_root or agent.chain_root) / "controls"
        self.store = WatchStore(root)
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
        critical = self.agent.rpc.get_logs(
            from_block,
            to_block,
            address=token,
            topics=[
                [
                    OWNERSHIP_TRANSFERRED_TOPIC,
                    UPGRADED_TOPIC,
                    ADMIN_CHANGED_TOPIC,
                ]
            ],
        )
        transfers = self.agent.rpc.get_logs(
            from_block,
            to_block,
            address=token,
            topics=[TRANSFER_TOPIC],
        )
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
        return {
            "critical_count": len(critical or []),
            "transfer_count": len(transfers or []),
            "lp_burn_count": len(lp_burns or []),
            "critical_log_hash": canonical_hash(critical or []),
            "transfer_log_hash": canonical_hash(transfers or []),
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
                if fingerprint_changed:
                    reasons.append("critical_fingerprint_changed")
                if (
                    events["transfer_count"]
                    and (
                        last_full is None
                        or safe_block - int(last_full)
                        >= self.config.holder_rescan_blocks
                    )
                ):
                    reasons.append("holder_transfer_threshold_window")
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
                        alert = {
                            "schema_version": CONTROL_SCHEMA_VERSION,
                            "type": "state_change",
                            "reason": ",".join(reasons),
                            "token_address": token,
                            "block": safe_block,
                            "observed_at": utc_now_iso(now),
                            "events": events,
                            "drift": drift,
                            "new_hard_stops": new_stops,
                            "analysis_ring": report.get("analysis_ring"),
                            "analysis_ring_hash": report.get("analysis_ring_hash"),
                        }
                        alert["timechain"] = self._seal_transition(alert)
                        self.store.append_alert(alert)
                        summary["alerts"] += 1
                        if new_stops:
                            # Best-effort external push (Discord/Slack/Telegram/
                            # generic webhook). No-ops unless
                            # CHAINSEER_ALERT_WEBHOOK_URL is configured, and
                            # never raises into the watcher loop -- see
                            # chainseer_alerts.py for the fail-open contract.
                            chainseer_alerts.send_alert(
                                {
                                    "summary": f"{token}: new hard stop(s) "
                                    + ",".join(new_stops),
                                    "hard_stops": new_stops,
                                    "reason": alert["reason"],
                                    "block": safe_block,
                                },
                                chain="robinhood",
                                token_address=token,
                                event_type="hard_stop",
                            )

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
    reliability = calibration_commands.add_parser("reliability")
    reliability.add_argument("--bins", type=int, default=10)
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
                rings = agent.tc.load()
                result = {
                    "policy": asdict(watcher.calibration.policy()),
                    "metrics": watcher.calibration.summarize(rings),
                    "reliability": watcher.calibration.reliability_curve(rings),
                }
            elif args.calibration_command == "propose":
                result = watcher.calibration.propose(agent.tc.load())
            elif args.calibration_command == "reliability":
                result = watcher.calibration.reliability_report(
                    agent.tc.load(), bins=args.bins
                )
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
