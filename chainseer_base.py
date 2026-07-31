"""Safe Base prototype for Chainseer launch observation and paper trading.

This module deliberately contains no private-key, approval, signing, or
transaction-broadcast code.  It discovers Base launches through Virtuals'
official API, verifies that the listed token has on-chain code, enriches the
candidate with Base security sources, and feeds deterministic paper positions.

Official Virtuals SDK provenance (inspected 2026-07-21):
  https://github.com/Virtual-Protocol/vp-trade-sdk
  commit c475c35264c40028ab7e79ec20e09b9db58e8c06
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from decimal import Decimal, localcontext
from pathlib import Path
from typing import Callable, Iterable

from chainseer import (
    ADDRESS_RE,
    ProvenanceLedger,
    RobinhoodRPC,
    ScanContext,
    _get_skill_dir,
    _http_get_json,
    _load_skill_module,
    _load_timechain_module,
    ensure_utf8_runtime,
)


BASE_CHAIN_ID = 8453
BASE_RPC_URL = os.environ.get("CHAINSEER_BASE_RPC_URL", "https://mainnet.base.org")
BASE_BLOCKSCOUT_API = "https://base.blockscout.com/api/v2"
GOPLUS_API = "https://api.gopluslabs.io/api/v1"
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex"
VIRTUALS_API = os.environ.get("CHAINSEER_VIRTUALS_API", "https://api.virtuals.io")
VIRTUALS_API_V2 = os.environ.get("CHAINSEER_VIRTUALS_API_V2", "https://vp-api.virtuals.io")

# Canonical Base constants from the official Virtuals vp-trade-sdk commit above.
VIRTUALS_TOKEN_ADDRESS = "0x0b3e328455c4059EEb9e3f84b5543F74E24e7E1b"
VIRTUALS_ROUTER_ADDRESS = "0x8292B43aB73EfAC11FAF357419C38ACF448202C5"
VIRTUALS_BONDING_ADDRESS = "0xF66DeA7b3e897cD44A5a231c61B6B4423d613259"
BASE_UNISWAP_V2_ROUTER = "0x4752ba5DBc23f44D87826276BF6Fd6b1C372aD24"
VIRTUALS_SDK_COMMIT = "c475c35264c40028ab7e79ec20e09b9db58e8c06"

# BONDING_V5 pair view selectors, verified against deployed Base bytecode.
PAIR_GET_RESERVES = "0x0902f1ac"  # getReserves() -> (uint256,uint256)
PAIR_TOKEN_A = "0x0fc63d10"      # tokenA() -> address
PAIR_TOKEN_B = "0x5f64b55b"      # tokenB() -> address
MAX_PAPER_QUOTE_RESERVE_FRACTION = 0.0025
LEARNING_HORIZONS = (
    ("5m", 5 * 60),
    ("1h", 60 * 60),
    ("6h", 6 * 60 * 60),
    ("24h", 24 * 60 * 60),
    ("7d", 7 * 24 * 60 * 60),
    ("30d", 30 * 24 * 60 * 60),
)
LEARNING_REFRESH_SECONDS = 5 * 60
LEARNING_TRIGGER_SECONDS = 2 * 60
LEARNING_LOCK_STALE_SECONDS = 30 * 60
SHADOW_SUMMARY_SCHEMA_VERSION = 2
SHADOW_STALE_MARK_SECONDS = LEARNING_REFRESH_SECONDS * 6
MAX_SHADOW_PRIORITY_REFRESHES = 4
MAX_ADAPTIVE_SHADOW_PRIORITY_REFRESHES = 5
ADAPTIVE_HEADROOM_MAX_PREVIOUS_DURATION_SECONDS = 75.0
SHADOW_REVIEW_MIN_CLOSED_POSITIONS = 30

PROTOTYPE_STATUS = 1
SENTIENT_STATUS = 2
ZERO_ADDRESS = "0x" + "0" * 40


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _safe_float(value, default=0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _timestamp_seconds(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _safe_int(value, default=0) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _flag(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes"}


def _atomic_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(temporary, path)


def _read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


@dataclass(frozen=True)
class BaseLaunchCandidate:
    launch_id: int
    name: str
    symbol: str
    lifecycle: str
    token_address: str
    pair_address: str
    created_at: str
    holder_count: int
    market_cap_virtual: float
    total_supply: float = 0.0
    launched_at: str = ""
    factory: str = ""
    launch_mode: int | None = None
    anti_sniper_tax_type: int | None = None
    project_verified: bool = False
    description: str = ""
    source: str = "virtuals_official_api"
    source_chain: str = "BASE"
    raw: dict = field(default_factory=dict, compare=False, repr=False)

    @property
    def is_prototype(self) -> bool:
        return self.lifecycle == "prototype"

    @property
    def implied_price_virtual(self) -> float | None:
        if self.market_cap_virtual > 0 and self.total_supply > 0:
            return self.market_cap_virtual / self.total_supply
        return None

    def to_dict(self) -> dict:
        """Return the persistable public view; never retain the upstream raw record."""
        return {key: value for key, value in asdict(self).items() if key != "raw"}


@dataclass
class BaseRiskDecision:
    token_address: str
    block_pin: int
    score: float
    risk_level: str
    paper_entry_allowed: bool
    live_entry_allowed: bool
    hard_stops: list[str]
    warnings: list[str]
    green_flags: list[str]
    coverage: dict
    canonicality: dict
    market: dict
    security: dict
    provenance: dict
    analyzed_at: str = field(default_factory=_utc_now)
    timechain_ring: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PaperPolicy:
    amount_virtual: float = 10.0
    minimum_score: float = 70.0
    observation_seconds: int = 120
    maximum_positions: int = 5
    assumed_slippage_bps: int = 100
    prototype_tax_bps: int = 100
    sentient_tax_bps: int = 100
    stop_loss_multiple: float = 0.65
    trailing_activation_multiple: float = 2.0
    trailing_drawdown: float = 0.25
    maximum_hold_hours: float = 72.0
    take_profit_tiers: tuple[tuple[float, float], ...] = (
        (2.0, 0.35),
        (3.0, 0.20),
        (5.0, 0.20),
        (10.0, 0.25),
    )

    def __post_init__(self):
        if self.amount_virtual <= 0:
            raise ValueError("amount_virtual must be positive")
        if not 0 <= self.assumed_slippage_bps <= 5_000:
            raise ValueError("assumed_slippage_bps must be between 0 and 5000")
        tier_fraction = sum(fraction for _, fraction in self.take_profit_tiers)
        if tier_fraction > 1.000001:
            raise ValueError("take-profit fractions cannot exceed 100%")


class LiveExecutionDisabledError(RuntimeError):
    """Raised whenever code attempts to cross the paper/live safety boundary."""


class BaseRPC(RobinhoodRPC):
    """The existing RPC implementation is EVM-generic despite its legacy name."""

    @staticmethod
    def _word_address(raw: str) -> str:
        if not raw or raw == "0x" or len(raw) < 66:
            return ZERO_ADDRESS
        return "0x" + raw[-40:]

    def virtuals_pair_snapshot(self, pair_address: str, token_address: str) -> dict:
        """Read and validate a BONDING_V5 pair at the currently pinned block."""
        token_a = self._word_address(self.call(pair_address, PAIR_TOKEN_A))
        token_b = self._word_address(self.call(pair_address, PAIR_TOKEN_B))
        raw_reserves = self.call(pair_address, PAIR_GET_RESERVES)
        reserve_hex = (raw_reserves or "").removeprefix("0x")
        if len(reserve_hex) < 128:
            raise ValueError("BONDING_V5 pair returned malformed reserves")
        reserve_a = int(reserve_hex[0:64], 16)
        reserve_b = int(reserve_hex[64:128], 16)

        expected = token_address.lower()
        virtuals = VIRTUALS_TOKEN_ADDRESS.lower()
        if token_a.lower() == expected and token_b.lower() == virtuals:
            token_reserve, virtual_reserve = reserve_a, reserve_b
        elif token_b.lower() == expected and token_a.lower() == virtuals:
            token_reserve, virtual_reserve = reserve_b, reserve_a
        else:
            return {
                "pair_address": pair_address,
                "token_a": token_a,
                "token_b": token_b,
                "binding_verified": False,
                "token_reserve_raw": None,
                "virtual_reserve_raw": None,
                "price_virtual": None,
            }

        token_decimals = self.erc20_decimals(token_address)
        virtual_decimals = self.erc20_decimals(VIRTUALS_TOKEN_ADDRESS)
        if token_reserve <= 0 or virtual_reserve <= 0:
            price_virtual = None
        else:
            with localcontext() as context:
                context.prec = 50
                token_units = Decimal(token_reserve) / (Decimal(10) ** token_decimals)
                virtual_units = Decimal(virtual_reserve) / (Decimal(10) ** virtual_decimals)
                price_virtual = float(virtual_units / token_units)

        return {
            "pair_address": pair_address,
            "token_a": token_a,
            "token_b": token_b,
            "binding_verified": True,
            "token_reserve_raw": token_reserve,
            "virtual_reserve_raw": virtual_reserve,
            "token_decimals": token_decimals,
            "virtual_decimals": virtual_decimals,
            "token_reserve": token_reserve / (10 ** token_decimals),
            "virtual_reserve": virtual_reserve / (10 ** virtual_decimals),
            "price_virtual": price_virtual,
            "block_pin": self.context.block_pin if self.context is not None else None,
            "method": "bonding_v5_pair_reserve_ratio",
        }


class VirtualsBaseObserver:
    """Discover launches only through Virtuals' official Base-filtered API."""

    def __init__(self, ledger: ProvenanceLedger | None = None,
                 http_get: Callable = _http_get_json):
        self.ledger = ledger
        self.http_get = http_get

    @staticmethod
    def normalize(item: dict) -> BaseLaunchCandidate | None:
        if str(item.get("chain", "")).upper() != "BASE":
            return None
        status = str(item.get("status", "")).upper()
        is_prototype = status in {"UNDERGRAD", "PROTOTYPE", "1"}
        lifecycle = "prototype" if is_prototype else "sentient"
        token_address = item.get("preToken") if is_prototype else item.get("tokenAddress")
        token_address = token_address or item.get("tokenAddress") or item.get("preToken") or ""
        pair_address = (
            item.get("preTokenPair") if is_prototype else item.get("lpAddress")
        ) or item.get("lpAddress") or item.get("preTokenPair") or ""
        if not ADDRESS_RE.fullmatch(str(token_address)):
            return None
        if pair_address and not ADDRESS_RE.fullmatch(str(pair_address)):
            pair_address = ""
        launch_info = item.get("launchInfo") if isinstance(item.get("launchInfo"), dict) else {}
        return BaseLaunchCandidate(
            launch_id=_safe_int(item.get("id")),
            name=str(item.get("name") or "Unknown"),
            symbol=str(item.get("symbol") or "???"),
            lifecycle=lifecycle,
            token_address=token_address,
            pair_address=pair_address,
            created_at=str(item.get("createdAt") or ""),
            holder_count=_safe_int(item.get("holderCount")),
            market_cap_virtual=_safe_float(item.get("mcapInVirtual")),
            total_supply=_safe_float(item.get("totalSupply")),
            launched_at=str(item.get("launchedAt") or item.get("createdAt") or ""),
            factory=str(item.get("factory") or ""),
            launch_mode=_safe_int(launch_info.get("launchMode"), None),
            anti_sniper_tax_type=_safe_int(launch_info.get("antiSniperTaxType"), None),
            project_verified=bool(item.get("isVerified")),
            description=str(item.get("description") or ""),
            raw=item,
        )

    def fetch_launches(self, limit: int = 20, include_sentient: bool = False) -> list[BaseLaunchCandidate]:
        statuses = [PROTOTYPE_STATUS] + ([SENTIENT_STATUS] if include_sentient else [])
        candidates: list[BaseLaunchCandidate] = []
        for status in statuses:
            payload, _, _ = self.http_get(
                f"{VIRTUALS_API}/api/virtuals",
                params={
                    "filters[status]": str(status),
                    "filters[chain]": "BASE",
                    "sort[0]": "createdAt:desc",
                    "populate[0]": "image",
                    "pagination[page]": "1",
                    "pagination[pageSize]": str(max(1, min(limit, 100))),
                },
                ledger=None,
            )
            normalized_for_evidence: list[dict] = []
            for item in payload.get("data", []) if isinstance(payload, dict) else []:
                candidate = self.normalize(item)
                if candidate is not None:
                    candidates.append(candidate)
                    normalized_for_evidence.append(candidate.to_dict())
            if self.ledger is not None:
                self.ledger.record(
                    "http_redacted",
                    {
                        "url": f"{VIRTUALS_API}/api/virtuals",
                        "params": {"status": str(status), "chain": "BASE", "limit": limit},
                    },
                    {"candidates": normalized_for_evidence},
                )
        candidates.sort(key=lambda item: item.created_at, reverse=True)
        return candidates[:limit]

    def fetch_launch_by_id(self, launch_id: int) -> BaseLaunchCandidate | None:
        """Refresh one tracked project regardless of its current lifecycle stage."""
        payload, _, _ = self.http_get(
            f"{VIRTUALS_API}/api/virtuals/{int(launch_id)}",
            params=None,
            ledger=None,
        )
        item = payload.get("data") if isinstance(payload, dict) else None
        candidate = self.normalize(item) if isinstance(item, dict) else None
        if self.ledger is not None:
            self.ledger.record(
                "http_redacted",
                {"url": f"{VIRTUALS_API}/api/virtuals/{int(launch_id)}"},
                {"candidate": candidate.to_dict() if candidate else None},
            )
        return candidate

    def latest_trades(self, token_address: str, limit: int = 20) -> list[dict]:
        payload, _, _ = self.http_get(
            f"{VIRTUALS_API_V2}/vp-api/trades",
            params={
                "tokenAddress": token_address,
                "limit": str(max(1, min(limit, 100))),
                "chainID": "0",  # Official SDK KLINE_CHAIN_ID.BASE
                "txSender": "",
            },
            ledger=None,
        )
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        trades = data.get("Trades", []) if isinstance(data, dict) else []
        if self.ledger is not None:
            self.ledger.record(
                "http_redacted",
                {"url": f"{VIRTUALS_API_V2}/vp-api/trades", "tokenAddress": token_address},
                {"prices": [_safe_float(trade.get("price"), None) for trade in trades[:20]]},
            )
        return trades

    def latest_price_virtual(self, token_address: str) -> float | None:
        trades = self.latest_trades(token_address, limit=5)
        for trade in trades:
            price = _safe_float(trade.get("price"), -1)
            if price > 0:
                return price
        return None


class BaseRiskAnalyzer:
    """Fast Base risk gate for a launch candidate; conservative on missing data."""

    def __init__(self, rpc_url: str = BASE_RPC_URL, evidence_root: str | Path | None = None,
                 rpc: BaseRPC | None = None, http_get: Callable = _http_get_json):
        self.rpc = rpc or BaseRPC(rpc_url)
        self.http_get = http_get
        self.evidence_root = Path(evidence_root) if evidence_root else None

    def _fetch(self, url: str, *, params: dict | None, ledger: ProvenanceLedger) -> dict:
        payload, _, _ = self.http_get(url, params=params, ledger=ledger)
        return payload if isinstance(payload, dict) else {}

    def analyze(self, candidate: BaseLaunchCandidate, quote_virtual: float = 1.0) -> BaseRiskDecision:
        evidence_dir = self.evidence_root / candidate.token_address.lower() if self.evidence_root else None
        ledger = ProvenanceLedger(evidence_dir)
        block_pin = self.rpc.get_block_number()
        ledger.block_pin = block_pin
        self.rpc.bind_context(ScanContext(BASE_CHAIN_ID, block_pin, ledger))

        hard_stops: list[str] = []
        warnings: list[str] = []
        green_flags: list[str] = []
        coverage = {"virtuals": True, "rpc": False, "goplus": False,
                    "blockscout": False, "dexscreener": False, "bonding_pair": False}

        code = self.rpc.get_code(candidate.token_address)
        has_code = bool(code and code != "0x")
        coverage["rpc"] = has_code
        code_sha256 = hashlib.sha256((code or "").encode("ascii", "ignore")).hexdigest() if code else None
        if not has_code:
            hard_stops.append("No contract bytecode exists at the API-listed token address")
        else:
            green_flags.append("Official API-listed address has on-chain bytecode")

        go_plus = {}
        try:
            gp_payload = self._fetch(
                f"{GOPLUS_API}/token_security/{BASE_CHAIN_ID}",
                params={"contract_addresses": candidate.token_address},
                ledger=ledger,
            )
            go_plus = (gp_payload.get("result") or {}).get(candidate.token_address.lower(), {})
            if not go_plus and isinstance(gp_payload.get("result"), dict):
                go_plus = next(iter(gp_payload["result"].values()), {})
            coverage["goplus"] = bool(go_plus)
        except Exception as exc:
            warnings.append(f"GoPlus unavailable: {exc}")

        if go_plus:
            if _flag(go_plus.get("is_honeypot")):
                hard_stops.append("GoPlus identifies the token as a honeypot")
            if _flag(go_plus.get("cannot_buy")):
                hard_stops.append("Buying is restricted")
            if _flag(go_plus.get("cannot_sell_all")):
                hard_stops.append("Selling the full position is restricted")
            if _flag(go_plus.get("is_blacklisted")):
                hard_stops.append("Blacklist controls are present")
            if _flag(go_plus.get("is_mintable")):
                warnings.append("Token is reported mintable")
            buy_tax = _safe_float(go_plus.get("buy_tax")) * 100
            sell_tax = _safe_float(go_plus.get("sell_tax")) * 100
            if buy_tax > 10:
                hard_stops.append(f"Reported buy tax is {buy_tax:.1f}%")
            if sell_tax > 10:
                hard_stops.append(f"Reported sell tax is {sell_tax:.1f}%")
        else:
            buy_tax = sell_tax = None
            warnings.append("Token restrictions are not confirmed by GoPlus")

        blockscout_address = {}
        source = {}
        try:
            blockscout_address = self._fetch(
                f"{BASE_BLOCKSCOUT_API}/addresses/{candidate.token_address}", params=None, ledger=ledger)
            source = self._fetch(
                f"{BASE_BLOCKSCOUT_API}/smart-contracts/{candidate.token_address}", params=None, ledger=ledger)
            coverage["blockscout"] = bool(blockscout_address)
        except Exception as exc:
            warnings.append(f"Blockscout unavailable: {exc}")

        verified_source = bool(source.get("is_verified"))
        if verified_source:
            green_flags.append("Contract source is verified on Base Blockscout")
        else:
            warnings.append("Contract source is not verified on Base Blockscout")
        if blockscout_address.get("is_scam"):
            hard_stops.append("Base Blockscout marks the address as a scam")

        dex_pairs: list[dict] = []
        try:
            dex_payload = self._fetch(
                f"{DEXSCREENER_API}/tokens/{candidate.token_address}", params=None, ledger=ledger)
            dex_pairs = [
                pair for pair in (dex_payload.get("pairs") or [])
                if str(pair.get("chainId", "")).lower() == "base"
            ]
            coverage["dexscreener"] = bool(dex_pairs)
        except Exception as exc:
            warnings.append(f"DexScreener unavailable: {exc}")

        liquidity_usd = sum(_safe_float((pair.get("liquidity") or {}).get("usd")) for pair in dex_pairs)
        volume_24h = sum(_safe_float((pair.get("volume") or {}).get("h24")) for pair in dex_pairs)
        price_usd = max((_safe_float(pair.get("priceUsd")) for pair in dex_pairs), default=0.0)
        bonding_spot = None
        pair_binding_verified = False
        if candidate.is_prototype:
            green_flags.append("Prototype is identified through the official Base-filtered Virtuals listing")
            if candidate.factory == "BONDING_V5":
                green_flags.append("Official metadata identifies the current Virtuals BONDING_V5 factory")
            elif candidate.factory:
                hard_stops.append(f"Unexpected Virtuals factory metadata: {candidate.factory}")
            if candidate.anti_sniper_tax_type not in {None, 0}:
                warnings.append(
                    "Anti-sniper tax metadata is active; the paper trader enforces a 98-minute wait"
                )
            if not candidate.pair_address:
                warnings.append("Prototype pair address is missing from the official listing")
            else:
                try:
                    bonding_spot = self.rpc.virtuals_pair_snapshot(
                        candidate.pair_address, candidate.token_address
                    )
                    pair_binding_verified = bool(bonding_spot.get("binding_verified"))
                    coverage["bonding_pair"] = pair_binding_verified
                    if not pair_binding_verified:
                        hard_stops.append(
                            "Official prototype pair does not bind the candidate token to VIRTUAL"
                        )
                    elif not bonding_spot.get("price_virtual"):
                        warnings.append("BONDING_V5 pair reserves cannot produce a positive spot price")
                    else:
                        virtual_reserve = _safe_float(bonding_spot.get("virtual_reserve"))
                        reserve_fraction = quote_virtual / virtual_reserve if virtual_reserve > 0 else None
                        bonding_spot["paper_amount_virtual"] = quote_virtual
                        bonding_spot["paper_amount_reserve_fraction"] = reserve_fraction
                        bonding_spot["paper_quote_eligible"] = bool(
                            reserve_fraction is not None
                            and reserve_fraction <= MAX_PAPER_QUOTE_RESERVE_FRACTION
                        )
                        green_flags.append(
                            "Official pair binds the token to VIRTUAL and exposes block-pinned reserves"
                        )
                        if not bonding_spot["paper_quote_eligible"]:
                            warnings.append(
                                "Requested paper amount is too large relative to the bonding reserve"
                            )
                except Exception as exc:
                    warnings.append(f"BONDING_V5 reserve price unavailable: {exc}")
        elif liquidity_usd < 10_000:
            hard_stops.append(f"Graduated token liquidity is only ${liquidity_usd:,.0f}")
        elif liquidity_usd >= 50_000:
            green_flags.append(f"Graduated liquidity is ${liquidity_usd:,.0f}")

        canonicality = {
            "official_api": True,
            "api_chain": candidate.source_chain,
            "onchain_code": has_code,
            "factory_binding_verified": False,
            "pair_binding_verified": pair_binding_verified,
            "virtuals_sdk_commit": VIRTUALS_SDK_COMMIT,
            "virtuals_token": VIRTUALS_TOKEN_ADDRESS,
            "bonding_contract": VIRTUALS_BONDING_ADDRESS,
            "router_contract": VIRTUALS_ROUTER_ADDRESS,
            "code_sha256": code_sha256,
        }
        warnings.append("On-chain factory binding is not yet independently decoded")

        score = 100.0
        score -= 35 * len(hard_stops)
        score -= min(30, 5 * len(warnings))
        if not verified_source:
            score -= 5
        if not coverage["goplus"]:
            score -= 10
        score = round(max(0.0, min(100.0, score)), 1)
        if hard_stops or score < 40:
            risk_level = "Critical"
        elif score < 55:
            risk_level = "High"
        elif score < 85:
            risk_level = "Medium"
        else:
            risk_level = "Low"

        paper_allowed = not hard_stops and has_code and score >= 70
        return BaseRiskDecision(
            token_address=candidate.token_address,
            block_pin=block_pin,
            score=score,
            risk_level=risk_level,
            paper_entry_allowed=paper_allowed,
            live_entry_allowed=False,
            hard_stops=hard_stops,
            warnings=warnings,
            green_flags=green_flags,
            coverage=coverage,
            canonicality=canonicality,
            market={
                "liquidity_usd": round(liquidity_usd, 2),
                "volume_24h_usd": round(volume_24h, 2),
                "price_usd": price_usd or None,
                "market_cap_virtual": candidate.market_cap_virtual,
                "bonding_spot": bonding_spot,
            },
            security={
                "buy_tax_pct": buy_tax,
                "sell_tax_pct": sell_tax,
                "source_verified": verified_source,
                "goplus": go_plus,
            },
            provenance=ledger.to_dict(),
        )


class PaperTradeLedger:
    """Append-only, hash-linked operational record for prototype paper trades."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        records = []
        with self.path.open("r", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    records.append(json.loads(line))
        return records

    def append(self, event_type: str, payload: dict) -> dict:
        records = self.load()
        previous_hash = records[-1]["event_hash"] if records else "0" * 64
        record = {
            "index": len(records),
            "event_type": event_type,
            "timestamp": _utc_now(),
            "previous_hash": previous_hash,
            "payload": payload,
        }
        record["event_hash"] = hashlib.sha256(_canonical_json(record).encode("utf-8")).hexdigest()
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")
        return record

    def verify(self) -> tuple[bool, str]:
        previous_hash = "0" * 64
        for expected_index, record in enumerate(self.load()):
            if record.get("index") != expected_index:
                return False, f"index mismatch at {expected_index}"
            if record.get("previous_hash") != previous_hash:
                return False, f"previous_hash mismatch at {expected_index}"
            expected = hashlib.sha256(
                _canonical_json({k: v for k, v in record.items() if k != "event_hash"}).encode("utf-8")
            ).hexdigest()
            if record.get("event_hash") != expected:
                return False, f"event_hash mismatch at {expected_index}"
            previous_hash = expected
        return True, f"verified {len(self.load())} paper events"


class BasePaperTrader:
    def __init__(self, state_path: str | Path, ledger: PaperTradeLedger,
                 policy: PaperPolicy | None = None, *,
                 event_namespace: str = "paper",
                 enforce_position_limit: bool = True):
        if event_namespace not in {"paper", "shadow"}:
            raise ValueError("event_namespace must be 'paper' or 'shadow'")
        self.state_path = Path(state_path)
        self.ledger = ledger
        self.policy = policy or PaperPolicy()
        self.event_namespace = event_namespace
        self.enforce_position_limit = enforce_position_limit
        self.state = self._load_state()

    def _load_state(self) -> dict:
        if not self.state_path.exists():
            return {"positions": {}, "realized_virtual": 0.0}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def _save(self) -> None:
        _atomic_json(self.state_path, self.state)

    @property
    def open_positions(self) -> list[dict]:
        return [position for position in self.state["positions"].values() if position["status"] == "open"]

    def entry_blockers(self, candidate: BaseLaunchCandidate, decision: BaseRiskDecision,
                       price_virtual: float, now: float | None = None) -> list[str]:
        blockers: list[str] = []
        if not decision.paper_entry_allowed or decision.score < self.policy.minimum_score:
            blockers.append("risk_gate")
        if price_virtual <= 0:
            blockers.append("non_positive_price")
        if candidate.token_address.lower() in self.state["positions"]:
            blockers.append("position_already_recorded")
        if (
            self.enforce_position_limit
            and len(self.open_positions) >= self.policy.maximum_positions
        ):
            blockers.append("position_limit")
        now = time.time() if now is None else now
        try:
            launch_time = candidate.launched_at or candidate.created_at
            created = datetime.fromisoformat(launch_time.replace("Z", "+00:00")).timestamp()
        except (ValueError, AttributeError):
            blockers.append("invalid_launch_time")
            return blockers
        age = now - created
        if (
            candidate.anti_sniper_tax_type not in {None, 0}
            and age < 98 * 60
        ):
            blockers.append("anti_sniper_wait")
        elif age < self.policy.observation_seconds:
            blockers.append("observation_wait")
        return blockers

    def enter(self, candidate: BaseLaunchCandidate, decision: BaseRiskDecision,
              price_virtual: float, now: float | None = None,
              price_source: str | None = None) -> dict | None:
        if decision.live_entry_allowed:
            raise LiveExecutionDisabledError("Base prototype cannot authorize live entry")
        now = time.time() if now is None else now
        if self.entry_blockers(candidate, decision, price_virtual, now):
            return None

        tax_bps = self.policy.prototype_tax_bps if candidate.is_prototype else self.policy.sentient_tax_bps
        retained = (1 - tax_bps / 10_000) * (1 - self.policy.assumed_slippage_bps / 10_000)
        quantity = self.policy.amount_virtual * retained / price_virtual
        position = {
            "token_address": candidate.token_address,
            "launch_id": candidate.launch_id,
            "name": candidate.name,
            "symbol": candidate.symbol,
            "lifecycle": candidate.lifecycle,
            "status": "open",
            "entry_timestamp": now,
            "entry_price_virtual": price_virtual,
            "initial_quantity": quantity,
            "remaining_quantity": quantity,
            "cost_virtual": self.policy.amount_virtual,
            "realized_virtual": 0.0,
            "high_multiple": 1.0,
            "tiers_filled": [],
            "analysis_score": decision.score,
            "analysis_ring": decision.timechain_ring,
            "analysis_risk_level": decision.risk_level,
            "source_verified": decision.security.get("source_verified"),
            "entry_price_source": price_source,
            "last_mark_price_virtual": price_virtual,
            "last_mark_price_source": price_source,
            "last_mark_timestamp": now,
            "simulation": self.event_namespace,
        }
        self.state["positions"][candidate.token_address.lower()] = position
        event = self.ledger.append(
            f"{self.event_namespace}_buy",
            {**position, "paper_only": True},
        )
        position["entry_event_hash"] = event["event_hash"]
        self._save()
        return position

    def _sell(self, position: dict, price_virtual: float, fraction_of_initial: float,
              reason: str, now: float) -> dict | None:
        quantity = min(
            position["remaining_quantity"],
            position["initial_quantity"] * max(0.0, fraction_of_initial),
        )
        if quantity <= 0:
            return None
        tax_bps = self.policy.prototype_tax_bps if position["lifecycle"] == "prototype" else self.policy.sentient_tax_bps
        retained = (1 - tax_bps / 10_000) * (1 - self.policy.assumed_slippage_bps / 10_000)
        proceeds = quantity * price_virtual * retained
        position["remaining_quantity"] -= quantity
        position["realized_virtual"] += proceeds
        self.state["realized_virtual"] += proceeds
        if position["remaining_quantity"] <= position["initial_quantity"] * 1e-12:
            position["remaining_quantity"] = 0.0
            position["status"] = "closed"
            position["closed_at"] = now
            position["close_reason"] = reason
        event = self.ledger.append(f"{self.event_namespace}_sell", {
            "token_address": position["token_address"],
            "quantity": quantity,
            "price_virtual": price_virtual,
            "proceeds_virtual": proceeds,
            "reason": reason,
            "remaining_quantity": position["remaining_quantity"],
            "simulation": self.event_namespace,
            "paper_only": True,
        })
        self._save()
        return event

    def mark(self, token_address: str, price_virtual: float,
             decision: BaseRiskDecision | None = None, now: float | None = None,
             price_source: str | None = None) -> list[dict]:
        position = self.state["positions"].get(token_address.lower())
        if not position or position["status"] != "open" or price_virtual <= 0:
            return []
        now = now or time.time()
        position["last_mark_price_virtual"] = price_virtual
        position["last_mark_price_source"] = price_source
        position["last_mark_timestamp"] = now
        if decision is not None:
            position["last_mark_risk_level"] = decision.risk_level
            position["last_mark_score"] = decision.score
            position["last_mark_hard_stops"] = list(decision.hard_stops)
        current_value = position["remaining_quantity"] * price_virtual
        realized_and_open = position["realized_virtual"] + current_value
        multiple = realized_and_open / position["cost_virtual"]
        position["high_multiple"] = max(position["high_multiple"], multiple)
        events: list[dict] = []

        suspicious = bool(decision and (decision.hard_stops or decision.risk_level in {"High", "Critical"}))
        expired = now - position["entry_timestamp"] >= self.policy.maximum_hold_hours * 3600
        trailed = (
            position["high_multiple"] >= self.policy.trailing_activation_multiple
            and multiple <= position["high_multiple"] * (1 - self.policy.trailing_drawdown)
        )
        if suspicious or multiple <= self.policy.stop_loss_multiple or expired or trailed:
            reason = (
                "risk_signal" if suspicious else "stop_loss" if multiple <= self.policy.stop_loss_multiple
                else "maximum_hold" if expired else "trailing_exit"
            )
            event = self._sell(position, price_virtual, 1.0, reason, now)
            return [event] if event else []

        for target, fraction in self.policy.take_profit_tiers:
            tier_key = str(target)
            if multiple >= target and tier_key not in position["tiers_filled"]:
                event = self._sell(position, price_virtual, fraction, f"take_profit_{target:g}x", now)
                position["tiers_filled"].append(tier_key)
                if event:
                    events.append(event)
                if position["status"] == "closed":
                    break
        self._save()
        return events

    def broadcast_live_trade(self, *_args, **_kwargs):
        raise LiveExecutionDisabledError(
            "Live signing and broadcast are intentionally absent from the Base prototype")


class BaseTimechainRecorder:
    """PoQ-gated Base prototype observations in the existing project chain."""

    def __init__(self, chain_root: str | Path):
        skill_dir = _get_skill_dir()
        tc_module = _load_timechain_module(skill_dir)
        self.poq_module = _load_skill_module(skill_dir, "poq")
        self.tc = tc_module.Timechain(root=Path(chain_root))
        if self.tc.height() == 0:
            self.tc.genesis(name="Chainseer")
        ok, report = self.tc.verify()
        if not ok:
            raise RuntimeError(f"Timechain verification failed: {report}")

    def find_idempotency_ring(self, idempotency_key: str | None) -> dict | None:
        if not idempotency_key:
            return None
        for ring in self.tc.iter_rings():
            if (ring.get("payload") or {}).get("idempotency_key") == idempotency_key:
                return ring
        return None

    def _ring_by_index(self, index: int | None) -> dict | None:
        if index is None:
            return None
        return next((ring for ring in self.tc.iter_rings() if ring.get("index") == index), None)

    def seal_analysis(self, candidate: BaseLaunchCandidate, decision: BaseRiskDecision,
                      idempotency_key: str | None = None) -> int:
        existing = self.find_idempotency_ring(idempotency_key)
        if existing is not None:
            decision.timechain_ring = existing["index"]
            return existing["index"]
        summary = (
            f"Base launch {candidate.symbol} ({candidate.token_address}) assessed as "
            f"{decision.risk_level} risk with score {decision.score}/100; "
            f"paper entry {'allowed' if decision.paper_entry_allowed else 'refused'} and live entry disabled."
        )
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            summary,
            context=_canonical_json({"candidate": candidate.to_dict(), "decision": decision.to_dict()}),
            ring_type="base_launch_analysis",
            external_scores={
                "coherence": 235,
                "relevance": 245,
                "novelty": 220,
                "consistency": 235 if decision.provenance.get("fact_count") else 170,
                "depth": min(245, 165 + 15 * sum(bool(v) for v in decision.coverage.values())),
                "covenant": 250,
            },
            frame="assertion",
            evidence_texts=[_canonical_json(decision.provenance), _canonical_json(decision.canonicality)],
            extra_payload={
                "chain_id": BASE_CHAIN_ID,
                "candidate": candidate.to_dict(),
                "decision": decision.to_dict(),
                "idempotency_key": idempotency_key,
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(f"PoQ refused Base analysis seal: {verdict.get('decision')}")
        decision.timechain_ring = ring["index"]
        return ring["index"]

    def seal_outcome(self, project_id: int, candidate: BaseLaunchCandidate,
                     decision: BaseRiskDecision, analysis_ring: int | None,
                     horizon: str, outcome: dict, idempotency_key: str) -> int:
        existing = self.find_idempotency_ring(idempotency_key)
        if existing is not None:
            return existing["index"]
        original = self._ring_by_index(analysis_ring)
        relevant = [original] if original else None
        summary = (
            f"Base learning checkpoint {horizon} for project {project_id} ({candidate.symbol}): "
            f"{decision.risk_level} risk, price source {outcome.get('price_source') or 'unavailable'}, "
            f"return {outcome.get('price_return_pct')}."
        )
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            summary,
            context="Delayed Base paper outcome linked without rewriting the original prediction",
            ring_type="base_learning_outcome",
            external_scores={"coherence": 245, "relevance": 250, "novelty": 230,
                             "consistency": 245, "depth": 235, "covenant": 255},
            frame="assertion",
            relevant_rings=relevant,
            declared_evidence=1 if original else None,
            evidence_texts=[_canonical_json(decision.provenance)],
            extra_payload={
                "idempotency_key": idempotency_key,
                "project_id": project_id,
                "analysis_ring": analysis_ring,
                "analysis_ring_hash": original.get("ring_hash") if original else None,
                "horizon": horizon,
                "candidate": candidate.to_dict(),
                "decision": decision.to_dict(),
                "outcome": outcome,
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(f"PoQ refused Base outcome seal: {verdict.get('decision')}")
        return ring["index"]

    def seal_migration(self, project_id: int, previous: BaseLaunchCandidate,
                       current: BaseLaunchCandidate, analysis_ring: int | None,
                       decision: BaseRiskDecision, idempotency_key: str) -> int:
        existing = self.find_idempotency_ring(idempotency_key)
        if existing is not None:
            return existing["index"]
        original = self._ring_by_index(analysis_ring)
        relevant = [original] if original else None
        summary = (
            f"Virtuals Base project {project_id} transitioned from {previous.lifecycle} "
            f"{previous.token_address} to {current.lifecycle} {current.token_address}; "
            "the new contract was independently re-analyzed."
        )
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            summary,
            context="Base project-identity continuity across Virtuals graduation",
            ring_type="base_graduation_migration",
            external_scores={"coherence": 250, "relevance": 255, "novelty": 245,
                             "consistency": 245, "depth": 245, "covenant": 255},
            frame="assertion",
            relevant_rings=relevant,
            declared_evidence=1 if original else None,
            evidence_texts=[_canonical_json(decision.provenance)],
            extra_payload={
                "idempotency_key": idempotency_key,
                "project_id": project_id,
                "analysis_ring": analysis_ring,
                "previous_candidate": previous.to_dict(),
                "current_candidate": current.to_dict(),
                "post_migration_decision": decision.to_dict(),
                "paper_position_migration": "pending_verified_conversion_ratio",
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(f"PoQ refused Base migration seal: {verdict.get('decision')}")
        return ring["index"]


class BasePrototypeEngine:
    def __init__(self, root: str | Path = "base_prototype", rpc_url: str = BASE_RPC_URL,
                 policy: PaperPolicy | None = None, record_timechain: bool = True,
                 chain_root: str | Path = "chainseer_chain"):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.observer_ledger = ProvenanceLedger(self.root / "observer_evidence")
        self.observer = VirtualsBaseObserver(self.observer_ledger)
        self.analyzer = BaseRiskAnalyzer(rpc_url, self.root / "analysis_evidence")
        self.paper_ledger = PaperTradeLedger(self.root / "paper_events.jsonl")
        self.trader = BasePaperTrader(self.root / "paper_state.json", self.paper_ledger, policy)
        self.shadow_ledger = PaperTradeLedger(self.root / "shadow_events.jsonl")
        self.shadow_trader = BasePaperTrader(
            self.root / "shadow_state.json",
            self.shadow_ledger,
            policy,
            event_namespace="shadow",
            enforce_position_limit=False,
        )
        self.timechain = BaseTimechainRecorder(chain_root) if record_timechain else None

    def evaluate_candidate(self, candidate: BaseLaunchCandidate, enter: bool = False,
                           shadow_enter: bool = False,
                           seal_analysis: bool = True,
                           linked_analysis_ring: int | None = None,
                           idempotency_key: str | None = None) -> dict:
        decision = self.analyzer.analyze(
            candidate, quote_virtual=self.trader.policy.amount_virtual
        )
        if self.timechain and seal_analysis:
            self.timechain.seal_analysis(
                candidate, decision, idempotency_key=idempotency_key
            )
        elif linked_analysis_ring is not None:
            decision.timechain_ring = linked_analysis_ring

        trade_price = None
        try:
            trade_price = self.observer.latest_price_virtual(candidate.token_address)
        except Exception as exc:
            decision.warnings.append(f"Virtuals trade price unavailable: {exc}")

        bonding_spot = decision.market.get("bonding_spot") or {}
        bonding_price = _safe_float(bonding_spot.get("price_virtual"), None)
        if candidate.is_prototype and bonding_price:
            price = bonding_price
            price_source = "onchain_bonding_reserve_spot"
            if trade_price:
                divergence = abs(trade_price - bonding_price) / bonding_price
                decision.market["latest_trade_price_virtual"] = trade_price
                decision.market["trade_to_reserve_divergence"] = divergence
                if divergence > 0.05:
                    decision.warnings.append(
                        f"Latest indexed trade differs from on-chain reserve spot by {divergence:.1%}"
                    )
        elif trade_price:
            price = trade_price
            price_source = "virtuals_latest_trade"
        else:
            price = None
            price_source = None
        if not price:
            price = candidate.implied_price_virtual
            if price:
                price_source = "official_api_implied_market_cap"
                decision.warnings.append(
                    "No trade quote was available; price is an analytical market-cap estimate"
                )

        position = None
        shadow_position = None
        paper_price_eligible = (
            price_source == "virtuals_latest_trade"
            or (
                price_source == "onchain_bonding_reserve_spot"
                and bool(bonding_spot.get("paper_quote_eligible"))
            )
        )
        policy_blockers = (
            self.trader.entry_blockers(candidate, decision, price)
            if price and paper_price_eligible else []
        )
        shadow_blockers = (
            self.shadow_trader.entry_blockers(candidate, decision, price)
            if price and paper_price_eligible else []
        )
        if enter and price and paper_price_eligible:
            position = self.trader.enter(
                candidate, decision, price, price_source=price_source
            )
        if shadow_enter and price and paper_price_eligible:
            shadow_position = self.shadow_trader.enter(
                candidate, decision, price, price_source=price_source
            )
        if not decision.paper_entry_allowed:
            paper_action = "risk_gate_refused"
        elif not enter:
            paper_action = "observation_only"
        elif not paper_price_eligible:
            paper_action = "waiting_for_paper_eligible_price"
        elif position is None:
            paper_action = "waiting_for_policy_conditions:" + ",".join(policy_blockers)
        else:
            paper_action = "paper_position_opened"
        if not decision.paper_entry_allowed:
            shadow_action = "risk_gate_refused"
        elif not shadow_enter:
            shadow_action = "observation_only"
        elif not paper_price_eligible:
            shadow_action = "waiting_for_paper_eligible_price"
        elif shadow_position is None:
            shadow_action = "waiting_for_policy_conditions:" + ",".join(shadow_blockers)
        else:
            shadow_action = "shadow_position_opened"
        return {
            "candidate": candidate.to_dict(),
            "decision": decision.to_dict(),
            "price_virtual": price,
            "price_source": price_source,
            "paper_price_eligible": paper_price_eligible,
            "paper_action": paper_action,
            "paper_position": position,
            "shadow_action": shadow_action,
            "shadow_position": shadow_position,
        }

    def run_once(self, limit: int = 10, enter: bool = False) -> list[dict]:
        results = [
            self.evaluate_candidate(candidate, enter=enter)
            for candidate in self.observer.fetch_launches(limit=limit)
        ]
        _atomic_json(self.root / "last_run.json", results)
        return results


class LearningRunLockedError(RuntimeError):
    """Raised when a previous learn-once cycle is still active."""


class LearningRunLock:
    def __init__(self, path: str | Path, stale_seconds: int = LEARNING_LOCK_STALE_SECONDS):
        self.path = Path(path)
        self.stale_seconds = stale_seconds
        self.fd: int | None = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(2):
            try:
                self.fd = os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(self.fd, _canonical_json({
                    "pid": os.getpid(), "started_at": _utc_now()
                }).encode("utf-8"))
                return self
            except FileExistsError:
                age = time.time() - self.path.stat().st_mtime
                if attempt == 0 and age > self.stale_seconds:
                    self.path.unlink(missing_ok=True)
                    continue
                raise LearningRunLockedError(
                    f"learn-once is already running (lock: {self.path})"
                )
        raise LearningRunLockedError(f"could not acquire learning lock: {self.path}")

    def __exit__(self, _exc_type, _exc, _tb):
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None
        self.path.unlink(missing_ok=True)


class BaseLearningStore:
    """SQLite state for restart-safe predictions, checkpoints, migrations and runs."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _initialize(self):
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS projects (
                    project_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    lifecycle TEXT NOT NULL,
                    initial_token_address TEXT NOT NULL,
                    current_token_address TEXT NOT NULL,
                    pair_address TEXT,
                    candidate_json TEXT NOT NULL,
                    first_seen_at REAL NOT NULL,
                    last_seen_at REAL NOT NULL,
                    next_refresh_at REAL NOT NULL,
                    initial_analysis_ring INTEGER,
                    initial_score REAL,
                    initial_price_virtual REAL,
                    initial_price_source TEXT,
                    initial_analyzed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS checkpoints (
                    project_id INTEGER NOT NULL,
                    horizon TEXT NOT NULL,
                    due_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    observed_at TEXT,
                    lifecycle TEXT,
                    token_address TEXT,
                    price_virtual REAL,
                    price_source TEXT,
                    price_return_pct REAL,
                    comparable_price INTEGER,
                    risk_level TEXT,
                    score REAL,
                    hard_stops_json TEXT,
                    warnings_json TEXT,
                    timechain_ring INTEGER,
                    PRIMARY KEY (project_id, horizon),
                    FOREIGN KEY (project_id) REFERENCES projects(project_id)
                );
                CREATE TABLE IF NOT EXISTS migrations (
                    project_id INTEGER NOT NULL,
                    previous_address TEXT NOT NULL,
                    current_address TEXT NOT NULL,
                    previous_lifecycle TEXT NOT NULL,
                    current_lifecycle TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    timechain_ring INTEGER,
                    PRIMARY KEY (project_id, current_address),
                    FOREIGN KEY (project_id) REFERENCES projects(project_id)
                );
                CREATE TABLE IF NOT EXISTS learning_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    discovered INTEGER NOT NULL DEFAULT 0,
                    new_projects INTEGER NOT NULL DEFAULT 0,
                    outcomes INTEGER NOT NULL DEFAULT 0,
                    migrations INTEGER NOT NULL DEFAULT 0,
                    paper_events INTEGER NOT NULL DEFAULT 0,
                    shadow_events INTEGER NOT NULL DEFAULT 0,
                    errors_json TEXT
                );
                CREATE TABLE IF NOT EXISTS learning_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            """)
            run_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(learning_runs)")
            }
            if "shadow_events" not in run_columns:
                connection.execute(
                    "ALTER TABLE learning_runs ADD COLUMN shadow_events "
                    "INTEGER NOT NULL DEFAULT 0"
                )

    def bind_policy(self, policy: PaperPolicy):
        """Prevent one learning cohort from silently mixing strategy policies."""
        signature = _canonical_json(asdict(policy))
        with self._connect() as connection:
            row = connection.execute(
                "SELECT value FROM learning_meta WHERE key='paper_policy'"
            ).fetchone()
            if row is None:
                connection.execute(
                    "INSERT INTO learning_meta(key,value) VALUES ('paper_policy',?)",
                    (signature,),
                )
            elif row["value"] != signature:
                raise ValueError(
                    "learning root is bound to a different paper policy; "
                    "use the original options or a new --root"
                )

    def start_run(self) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                "INSERT INTO learning_runs(started_at, status) VALUES (?, 'running')",
                (_utc_now(),),
            )
            return int(cursor.lastrowid)

    def finish_run(self, run_id: int, status: str, counters: dict, errors: list[str]):
        with self._connect() as connection:
            connection.execute("""
                UPDATE learning_runs SET completed_at=?, status=?, discovered=?,
                    new_projects=?, outcomes=?, migrations=?, paper_events=?,
                    shadow_events=?, errors_json=?
                WHERE run_id=?
            """, (
                _utc_now(), status, counters["discovered"], counters["new_projects"],
                counters["outcomes"], counters["migrations"], counters["paper_events"],
                counters["shadow_events"], _canonical_json(errors), run_id,
            ))

    def get_project(self, project_id: int) -> dict | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM projects WHERE project_id=?", (project_id,)
            ).fetchone()
            return dict(row) if row else None

    def add_project(self, candidate: BaseLaunchCandidate, result: dict, now: float,
                    horizons: tuple[tuple[str, int], ...]) -> bool:
        decision = result["decision"]
        with self._connect() as connection:
            cursor = connection.execute("""
                INSERT OR IGNORE INTO projects(
                    project_id,name,symbol,lifecycle,initial_token_address,
                    current_token_address,pair_address,candidate_json,first_seen_at,
                    last_seen_at,next_refresh_at,initial_analysis_ring,initial_score,
                    initial_price_virtual,initial_price_source,initial_analyzed_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                candidate.launch_id, candidate.name, candidate.symbol, candidate.lifecycle,
                candidate.token_address.lower(), candidate.token_address.lower(),
                candidate.pair_address, _canonical_json(candidate.to_dict()), now, now,
                now + LEARNING_REFRESH_SECONDS, decision.get("timechain_ring"),
                decision.get("score"), result.get("price_virtual"),
                result.get("price_source"), decision.get("analyzed_at") or _utc_now(),
            ))
            inserted = cursor.rowcount == 1
            if inserted:
                connection.executemany(
                    "INSERT INTO checkpoints(project_id,horizon,due_at) VALUES (?,?,?)",
                    [(candidate.launch_id, name, now + seconds) for name, seconds in horizons],
                )
            return inserted

    def update_project(self, candidate: BaseLaunchCandidate, now: float):
        with self._connect() as connection:
            connection.execute("""
                UPDATE projects SET name=?, symbol=?, lifecycle=?, current_token_address=?,
                    pair_address=?, candidate_json=?, last_seen_at=?, next_refresh_at=?
                WHERE project_id=?
            """, (
                candidate.name, candidate.symbol, candidate.lifecycle,
                candidate.token_address.lower(), candidate.pair_address,
                _canonical_json(candidate.to_dict()), now,
                now + LEARNING_REFRESH_SECONDS, candidate.launch_id,
            ))

    def defer_project(self, project_id: int, next_refresh_at: float):
        with self._connect() as connection:
            connection.execute(
                "UPDATE projects SET next_refresh_at=? WHERE project_id=?",
                (next_refresh_at, project_id),
            )

    def due_project_ids(self, now: float, limit: int, *,
                        priority_project_ids: Iterable[int] = (),
                        priority_target: int = 0,
                        priority_addition_limit: int | None = None) -> list[int]:
        """Plan regular work first, then fill a deduplicated priority deficit.

        ``limit`` continues to bound the original checkpoint/general lane.
        ``priority_target`` is the desired number of priority projects across
        both lanes. Regular/checkpoint overlap counts toward it.
        ``priority_addition_limit`` bounds only projects added beyond the
        regular lane. With the default arguments, behavior is unchanged.
        """
        limit = max(0, int(limit))
        priority_target = max(0, int(priority_target))
        if priority_addition_limit is not None:
            priority_addition_limit = max(0, int(priority_addition_limit))
        ordered_priority = list(dict.fromkeys(
            int(project_id) for project_id in priority_project_ids
        ))
        priority_set = set(ordered_priority)
        with self._connect() as connection:
            checkpoint_ids = [row[0] for row in connection.execute("""
                SELECT project_id, MIN(due_at) AS oldest_due FROM checkpoints
                WHERE status='pending' AND due_at<=? GROUP BY project_id
                ORDER BY oldest_due, project_id LIMIT ?
            """, (now, limit)).fetchall()]
            refresh_ids = [row[0] for row in connection.execute("""
                SELECT project_id FROM projects WHERE next_refresh_at<=?
                ORDER BY next_refresh_at, project_id
            """, (now,)).fetchall()]

        checkpoint_ids = list(dict.fromkeys(checkpoint_ids))
        regular_ids: list[int] = []
        provisional_set = set(checkpoint_ids)
        due_refresh_set = set(refresh_ids)
        regular_needed = max(0, limit - len(checkpoint_ids))
        for project_id in refresh_ids:
            if regular_needed <= 0:
                break
            if project_id not in provisional_set:
                regular_ids.append(project_id)
                provisional_set.add(project_id)
                regular_needed -= 1

        selected_priority = sum(
            project_id in priority_set for project_id in provisional_set
        )
        priority_needed = max(0, priority_target - selected_priority)
        if priority_addition_limit is not None:
            priority_needed = min(priority_needed, priority_addition_limit)
        priority_ids: list[int] = []
        for project_id in ordered_priority:
            if priority_needed <= 0:
                break
            if project_id in due_refresh_set and project_id not in provisional_set:
                priority_ids.append(project_id)
                provisional_set.add(project_id)
                priority_needed -= 1

        # Due checkpoints remain first. Added priority work then runs oldest
        # first, followed by the remaining general refreshes.
        return checkpoint_ids + priority_ids + regular_ids

    def pending_checkpoints(self, project_id: int, now: float) -> list[dict]:
        with self._connect() as connection:
            rows = connection.execute("""
                SELECT * FROM checkpoints WHERE project_id=? AND status='pending'
                    AND due_at<=? ORDER BY due_at
            """, (project_id, now)).fetchall()
            return [dict(row) for row in rows]

    def complete_checkpoint(self, project_id: int, horizon: str, outcome: dict,
                            timechain_ring: int | None):
        with self._connect() as connection:
            connection.execute("""
                UPDATE checkpoints SET status='complete', observed_at=?, lifecycle=?,
                    token_address=?, price_virtual=?, price_source=?, price_return_pct=?,
                    comparable_price=?, risk_level=?, score=?, hard_stops_json=?,
                    warnings_json=?, timechain_ring=?
                WHERE project_id=? AND horizon=? AND status='pending'
            """, (
                outcome["observed_at"], outcome["lifecycle"], outcome["token_address"],
                outcome.get("price_virtual"), outcome.get("price_source"),
                outcome.get("price_return_pct"), int(bool(outcome["comparable_price"])),
                outcome["risk_level"], outcome["score"],
                _canonical_json(outcome["hard_stops"]),
                _canonical_json(outcome["warnings"]), timechain_ring,
                project_id, horizon,
            ))

    def migration_exists(self, project_id: int, current_address: str) -> bool:
        with self._connect() as connection:
            return connection.execute(
                "SELECT 1 FROM migrations WHERE project_id=? AND current_address=?",
                (project_id, current_address.lower()),
            ).fetchone() is not None

    def record_migration(self, project_id: int, previous: BaseLaunchCandidate,
                         current: BaseLaunchCandidate, ring: int | None):
        with self._connect() as connection:
            connection.execute("""
                INSERT OR IGNORE INTO migrations(project_id,previous_address,current_address,
                    previous_lifecycle,current_lifecycle,observed_at,timechain_ring)
                VALUES (?,?,?,?,?,?,?)
            """, (
                project_id, previous.token_address.lower(), current.token_address.lower(),
                previous.lifecycle, current.lifecycle, _utc_now(), ring,
            ))

    def latest_comparable_marks(self) -> dict[int, dict]:
        """Return the newest usable checkpoint mark for each tracked project."""
        with self._connect() as connection:
            rows = connection.execute("""
                SELECT * FROM checkpoints
                WHERE status='complete' AND comparable_price=1
                    AND price_virtual IS NOT NULL AND price_virtual>0
                ORDER BY observed_at DESC, due_at DESC
            """).fetchall()
        marks: dict[int, dict] = {}
        for row in rows:
            project_id = int(row["project_id"])
            marks.setdefault(project_id, dict(row))
        return marks

    def project_count(self) -> int:
        with self._connect() as connection:
            return int(connection.execute("SELECT COUNT(*) FROM projects").fetchone()[0])

    def summary(self) -> dict:
        with self._connect() as connection:
            projects = connection.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
            pending = connection.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE status='pending'"
            ).fetchone()[0]
            complete = connection.execute(
                "SELECT COUNT(*) FROM checkpoints WHERE status='complete'"
            ).fetchone()[0]
            migrations = connection.execute("SELECT COUNT(*) FROM migrations").fetchone()[0]
            latest = connection.execute(
                "SELECT * FROM learning_runs ORDER BY run_id DESC LIMIT 1"
            ).fetchone()
        return {
            "generated_at": _utc_now(),
            "projects": projects,
            "checkpoints_pending": pending,
            "checkpoints_complete": complete,
            "migrations": migrations,
            "latest_run": dict(latest) if latest else None,
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def verify(self) -> tuple[bool, str]:
        with self._connect() as connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            orphan_checkpoints = connection.execute("""
                SELECT COUNT(*) FROM checkpoints c LEFT JOIN projects p
                    ON p.project_id=c.project_id WHERE p.project_id IS NULL
            """).fetchone()[0]
            orphan_migrations = connection.execute("""
                SELECT COUNT(*) FROM migrations m LEFT JOIN projects p
                    ON p.project_id=m.project_id WHERE p.project_id IS NULL
            """).fetchone()[0]
        ok = integrity == "ok" and orphan_checkpoints == 0 and orphan_migrations == 0
        return ok, (
            f"integrity={integrity}, orphan_checkpoints={orphan_checkpoints}, "
            f"orphan_migrations={orphan_migrations}"
        )


class ShadowPerformanceReporter:
    """Build a friction-aware, evidence-labeled view of the shadow cohort."""

    def __init__(self, trader: BasePaperTrader, ledger: PaperTradeLedger,
                 store: BaseLearningStore,
                 timechain: BaseTimechainRecorder | None = None):
        self.trader = trader
        self.ledger = ledger
        self.store = store
        self.timechain = timechain

    @staticmethod
    def _known_bool(value) -> bool | None:
        if isinstance(value, bool):
            return value
        if value in {0, "0", "false", "False"}:
            return False
        if value in {1, "1", "true", "True"}:
            return True
        return None

    def _analysis_metadata(self) -> dict[int, dict]:
        if self.timechain is None:
            return {}
        metadata: dict[int, dict] = {}
        try:
            for ring in self.timechain.tc.iter_rings():
                payload = ring.get("payload") or {}
                decision = payload.get("decision") or {}
                security = decision.get("security") or {}
                metadata[int(ring["index"])] = {
                    "source_verified": self._known_bool(
                        security.get("source_verified")
                    ),
                    "risk_level": decision.get("risk_level"),
                    "score": decision.get("score"),
                }
        except Exception:
            # The report remains usable without enrichment. Ledger and Timechain
            # verification are still surfaced independently to the caller.
            return {}
        return metadata

    @staticmethod
    def _score_bucket(score: float) -> str:
        if score >= 90:
            return "90-100"
        if score >= 80:
            return "80-89"
        if score >= 70:
            return "70-79"
        return "below-70"

    def _exit_retained(self, position: dict) -> float:
        tax_bps = (
            self.trader.policy.prototype_tax_bps
            if position.get("lifecycle") == "prototype"
            else self.trader.policy.sentient_tax_bps
        )
        return (
            (1 - tax_bps / 10_000)
            * (1 - self.trader.policy.assumed_slippage_bps / 10_000)
        )

    @staticmethod
    def _aggregate(rows: list[dict]) -> dict:
        valued = [row for row in rows if row.get("modeled_total_value_virtual") is not None]
        valued_cost = sum(row["cost_virtual"] for row in valued)
        value = sum(row["modeled_total_value_virtual"] for row in valued)
        return {
            "positions": len(rows),
            "open": sum(row["status"] == "open" for row in rows),
            "closed": sum(row["status"] == "closed" for row in rows),
            "valued_positions": len(valued),
            "cost_virtual": valued_cost,
            "modeled_value_virtual": value,
            "return_pct_on_valued_positions": (
                (value / valued_cost - 1) * 100 if valued_cost else None
            ),
        }

    @staticmethod
    def _nearest_rank(values: list[float], percentile: float) -> float | None:
        """Return a deterministic nearest-rank percentile for sorted samples."""
        if not values:
            return None
        ordered = sorted(values)
        rank = max(1, math.ceil(len(ordered) * percentile))
        return ordered[min(rank - 1, len(ordered) - 1)]

    def build(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        events = self.ledger.load()
        ledger_ok, ledger_report = self.ledger.verify()
        checkpoint_marks = self.store.latest_comparable_marks()
        ring_metadata = self._analysis_metadata()
        position_rows: list[dict] = []

        for key, raw_position in sorted(self.trader.state.get("positions", {}).items()):
            position = dict(raw_position)
            project_id = int(position.get("launch_id") or 0)
            token_address = str(position.get("token_address") or key)
            state_mark_price = _safe_float(
                position.get("last_mark_price_virtual"), None
            )
            state_mark_at = _timestamp_seconds(position.get("last_mark_timestamp"))
            mark = checkpoint_marks.get(project_id)
            checkpoint_price = None
            checkpoint_at = None
            if (
                mark
                and str(mark.get("token_address") or "").lower()
                == token_address.lower()
            ):
                checkpoint_price = _safe_float(mark.get("price_virtual"), None)
                checkpoint_at = _timestamp_seconds(mark.get("observed_at"))

            choices: list[tuple[float, float, str | None, str]] = []
            entry_price = _safe_float(position.get("entry_price_virtual"), None)
            entry_at = _timestamp_seconds(position.get("entry_timestamp"))
            if entry_price and entry_at is not None:
                choices.append((
                    entry_at,
                    entry_price,
                    position.get("entry_price_source"),
                    "entry_fallback",
                ))
            if state_mark_price and state_mark_at is not None:
                choices.append((
                    state_mark_at,
                    state_mark_price,
                    position.get("last_mark_price_source"),
                    "trader_state",
                ))
            if checkpoint_price and checkpoint_at is not None:
                choices.append((
                    checkpoint_at,
                    checkpoint_price,
                    mark.get("price_source"),
                    "learning_checkpoint",
                ))
            if choices:
                mark_at, mark_price, mark_source, mark_origin = max(
                    choices, key=lambda item: item[0]
                )
            else:
                mark_price = None
                mark_at = None
                mark_source = None
                mark_origin = "unavailable"

            cost = _safe_float(position.get("cost_virtual"), 0.0)
            realized = _safe_float(position.get("realized_virtual"), 0.0)
            remaining = _safe_float(position.get("remaining_quantity"), 0.0)
            status = str(position.get("status") or "unknown")
            open_liquidation = None
            total_value = None
            if status == "closed":
                open_liquidation = 0.0
                total_value = realized
            elif mark_price:
                open_liquidation = remaining * mark_price * self._exit_retained(position)
                total_value = realized + open_liquidation

            ring_index = position.get("analysis_ring")
            ring_info = ring_metadata.get(int(ring_index)) if ring_index is not None else None
            source_verified = self._known_bool(position.get("source_verified"))
            if source_verified is None and ring_info:
                source_verified = ring_info.get("source_verified")
            source_bucket = (
                "verified" if source_verified is True
                else "unverified" if source_verified is False
                else "unknown"
            )
            score = _safe_float(
                position.get("analysis_score"),
                _safe_float((ring_info or {}).get("score"), 0.0),
            )
            risk_level = (
                position.get("analysis_risk_level")
                or (ring_info or {}).get("risk_level")
                or "Unknown"
            )
            mark_age = max(0.0, now - mark_at) if mark_at is not None else None
            position_rows.append({
                "launch_id": project_id,
                "symbol": position.get("symbol"),
                "token_address": token_address,
                "status": status,
                "entry_timestamp": position.get("entry_timestamp"),
                "age_hours": max(0.0, now - entry_at) / 3600 if entry_at else None,
                "analysis_ring": ring_index,
                "analysis_score": score,
                "score_bucket": self._score_bucket(score),
                "entry_risk_level": risk_level,
                "source_verification": source_bucket,
                "cost_virtual": cost,
                "realized_virtual": realized,
                "remaining_quantity": remaining,
                "mark_price_virtual": mark_price,
                "mark_price_source": mark_source,
                "mark_origin": mark_origin,
                "mark_age_seconds": mark_age,
                "mark_stale": mark_age is None or mark_age > SHADOW_STALE_MARK_SECONDS,
                "gross_price_return_pct": (
                    (mark_price / entry_price - 1) * 100
                    if mark_price and entry_price else None
                ),
                "modeled_open_liquidation_virtual": open_liquidation,
                "modeled_total_value_virtual": total_value,
                "modeled_total_multiple": total_value / cost if total_value is not None and cost else None,
                "modeled_return_pct": (
                    (total_value / cost - 1) * 100
                    if total_value is not None and cost else None
                ),
            })

        opened = len(position_rows)
        open_rows = [row for row in position_rows if row["status"] == "open"]
        closed_rows = [row for row in position_rows if row["status"] == "closed"]
        valued_rows = [
            row for row in position_rows
            if row["modeled_total_value_virtual"] is not None
        ]
        total_cost = sum(row["cost_virtual"] for row in valued_rows)
        total_value = sum(row["modeled_total_value_virtual"] for row in valued_rows)
        gross_marked_open = [
            row for row in open_rows if row["gross_price_return_pct"] is not None
        ]
        gross_open_cost = sum(row["cost_virtual"] for row in gross_marked_open)
        weighted_gross_open_return = (
            sum(
                row["cost_virtual"] * row["gross_price_return_pct"]
                for row in gross_marked_open
            ) / gross_open_cost
            if gross_open_cost else None
        )
        closed_wins = sum(
            row["modeled_total_value_virtual"] > row["cost_virtual"]
            for row in closed_rows
        )
        closed_losses = sum(
            row["modeled_total_value_virtual"] < row["cost_virtual"]
            for row in closed_rows
        )
        closed_breakeven = len(closed_rows) - closed_wins - closed_losses

        sell_events = [event for event in events if event.get("event_type") == "shadow_sell"]
        exit_reasons: dict[str, dict] = {}
        for event in sell_events:
            payload = event.get("payload") or {}
            reason = str(payload.get("reason") or "unknown")
            item = exit_reasons.setdefault(
                reason, {"events": 0, "proceeds_virtual": 0.0}
            )
            item["events"] += 1
            item["proceeds_virtual"] += _safe_float(
                payload.get("proceeds_virtual"), 0.0
            )

        score_groups = {
            bucket: self._aggregate([
                row for row in position_rows if row["score_bucket"] == bucket
            ])
            for bucket in ("90-100", "80-89", "70-79", "below-70")
            if any(row["score_bucket"] == bucket for row in position_rows)
        }
        source_groups = {
            bucket: self._aggregate([
                row for row in position_rows
                if row["source_verification"] == bucket
            ])
            for bucket in ("verified", "unverified", "unknown")
            if any(row["source_verification"] == bucket for row in position_rows)
        }
        risk_groups = {
            risk: self._aggregate([
                row for row in position_rows if row["entry_risk_level"] == risk
            ])
            for risk in sorted({row["entry_risk_level"] for row in position_rows})
        }

        tracked_projects = self.store.project_count()
        shadow_projects = len({row["launch_id"] for row in position_rows if row["launch_id"]})
        marked = len(valued_rows)
        stale = sum(row["mark_stale"] for row in open_rows)
        open_mark_ages = [
            float(row["mark_age_seconds"])
            for row in open_rows
            if row["mark_age_seconds"] is not None
        ]
        median_open_mark_age = self._nearest_rank(open_mark_ages, 0.50)
        p95_open_mark_age = self._nearest_rank(open_mark_ages, 0.95)
        oldest_open_mark_age = max(open_mark_ages, default=None)
        source_known = sum(
            row["source_verification"] != "unknown" for row in position_rows
        )
        mature_counts = {
            "at_least_5m": sum((row["age_hours"] or 0) >= 5 / 60 for row in position_rows),
            "at_least_1h": sum((row["age_hours"] or 0) >= 1 for row in position_rows),
            "at_least_6h": sum((row["age_hours"] or 0) >= 6 for row in position_rows),
            "at_least_24h": sum((row["age_hours"] or 0) >= 24 for row in position_rows),
            "at_least_72h": sum((row["age_hours"] or 0) >= 72 for row in position_rows),
        }

        caveats = [
            "Counterfactual paper cohort only; no signing, broadcasting, or live capital.",
            "Open values use the latest available spot mark and modeled exit tax/slippage, not a guaranteed executable fill.",
            "Coverage versus all tracked projects is not an eligibility rate; some projects were correctly blocked by risk, timing, or price evidence.",
        ]
        if not closed_rows:
            caveats.append(
                "No shadow positions have closed, so realized win rate and closed-return evidence are unavailable."
            )
        elif len(closed_rows) < SHADOW_REVIEW_MIN_CLOSED_POSITIONS:
            caveats.append(
                "The closed-position sample is still small and must not be treated as a live-deployment signal."
            )
        if stale:
            caveats.append(
                f"{stale} open position mark(s) are older than {SHADOW_STALE_MARK_SECONDS // 60} minutes."
            )
        if not ledger_ok:
            caveats.append("The shadow event ledger failed integrity verification.")

        ranked = sorted(
            valued_rows,
            key=lambda row: row["modeled_total_multiple"],
            reverse=True,
        )
        compact_position = lambda row: {
            key: row[key] for key in (
                "launch_id", "symbol", "status", "analysis_score",
                "source_verification", "modeled_total_multiple",
                "modeled_return_pct", "mark_age_seconds", "mark_stale",
            )
        }
        report = {
            "schema_version": SHADOW_SUMMARY_SCHEMA_VERSION,
            "generated_at": datetime.fromtimestamp(now, tz=timezone.utc).isoformat(),
            "mode": "counterfactual_shadow",
            "paper_only": True,
            "live_execution_enabled": False,
            "accounting": {
                "positions_opened": opened,
                "positions_open": len(open_rows),
                "positions_closed": len(closed_rows),
                "buy_events": sum(
                    event.get("event_type") == "shadow_buy" for event in events
                ),
                "sell_events": len(sell_events),
                "valued_positions": marked,
                "unvalued_positions": opened - marked,
                "cost_virtual_on_valued_positions": total_cost,
                "realized_proceeds_virtual": sum(
                    row["realized_virtual"] for row in position_rows
                ),
                "modeled_liquidation_value_virtual": total_value,
                "modeled_return_pct_on_valued_positions": (
                    (total_value / total_cost - 1) * 100 if total_cost else None
                ),
                "weighted_gross_price_return_pct_open": weighted_gross_open_return,
                "closed_cost_virtual": sum(row["cost_virtual"] for row in closed_rows),
                "closed_realized_virtual": sum(
                    row["modeled_total_value_virtual"] for row in closed_rows
                ),
                "closed_realized_return_pct": (
                    (
                        sum(row["modeled_total_value_virtual"] for row in closed_rows)
                        / sum(row["cost_virtual"] for row in closed_rows) - 1
                    ) * 100
                    if closed_rows and sum(row["cost_virtual"] for row in closed_rows)
                    else None
                ),
                "closed_wins": closed_wins,
                "closed_losses": closed_losses,
                "closed_breakeven": closed_breakeven,
                "closed_win_rate_pct": (
                    closed_wins / len(closed_rows) * 100 if closed_rows else None
                ),
            },
            "coverage": {
                "tracked_projects": tracked_projects,
                "shadow_projects": shadow_projects,
                "tracked_project_coverage_pct": (
                    shadow_projects / tracked_projects * 100 if tracked_projects else None
                ),
                "marked_positions": marked,
                "mark_coverage_pct": marked / opened * 100 if opened else None,
                "stale_open_marks": stale,
                "median_open_mark_age_seconds": median_open_mark_age,
                "p95_open_mark_age_seconds": p95_open_mark_age,
                "oldest_open_mark_age_seconds": oldest_open_mark_age,
                "source_verification_known": source_known,
                "source_verification_coverage_pct": (
                    source_known / opened * 100 if opened else None
                ),
            },
            "maturity": mature_counts,
            "review_readiness": {
                "status": (
                    "reviewable_sample"
                    if len(closed_rows) >= SHADOW_REVIEW_MIN_CLOSED_POSITIONS
                    else "collecting"
                ),
                "closed_positions": len(closed_rows),
                "minimum_closed_positions_for_strategy_review": SHADOW_REVIEW_MIN_CLOSED_POSITIONS,
                "live_deployment_signal": False,
            },
            "exit_reasons": exit_reasons,
            "performance_by_score": score_groups,
            "performance_by_source_verification": source_groups,
            "performance_by_entry_risk_level": risk_groups,
            "leaders": [compact_position(row) for row in ranked[:5]],
            "laggards": [compact_position(row) for row in list(reversed(ranked))[:5]],
            "data_quality": {
                "shadow_ledger_verified": ledger_ok,
                "shadow_ledger_report": ledger_report,
                "stale_mark_after_seconds": SHADOW_STALE_MARK_SECONDS,
                "caveats": caveats,
            },
            "friction_baselines": {
                "prototype_flat_price_round_trip_return_pct": (
                    (
                        (1 - self.trader.policy.prototype_tax_bps / 10_000)
                        * (1 - self.trader.policy.assumed_slippage_bps / 10_000)
                    ) ** 2 - 1
                ) * 100,
                "sentient_flat_price_round_trip_return_pct": (
                    (
                        (1 - self.trader.policy.sentient_tax_bps / 10_000)
                        * (1 - self.trader.policy.assumed_slippage_bps / 10_000)
                    ) ** 2 - 1
                ) * 100,
            },
            "position_marks": position_rows,
        }
        return report


class BaseLearningLoop:
    """One restart-safe learning cycle suitable for Task Scheduler."""

    def __init__(self, engine: BasePrototypeEngine,
                 horizons: tuple[tuple[str, int], ...] = LEARNING_HORIZONS):
        self.engine = engine
        self.horizons = horizons
        self.store = BaseLearningStore(engine.root / "learning.sqlite3")
        self.store.bind_policy(engine.trader.policy)
        self.lock_path = engine.root / ".learn_once.lock"

    @staticmethod
    def _candidate_from_project(project: dict) -> BaseLaunchCandidate:
        return BaseLaunchCandidate(**json.loads(project["candidate_json"]))

    @staticmethod
    def _decision_from_result(result: dict) -> BaseRiskDecision:
        return BaseRiskDecision(**result["decision"])

    def _open_shadow_project_ids_oldest_first(self) -> list[int]:
        checkpoint_marks = self.store.latest_comparable_marks()

        def newest_mark_timestamp(position: dict) -> float:
            project_id = int(position["launch_id"])
            token_address = str(position.get("token_address") or "").lower()
            candidates = [
                _timestamp_seconds(position.get("entry_timestamp")),
                _timestamp_seconds(position.get("last_mark_timestamp")),
            ]
            checkpoint = checkpoint_marks.get(project_id)
            if (
                checkpoint
                and str(checkpoint.get("token_address") or "").lower() == token_address
            ):
                candidates.append(_timestamp_seconds(checkpoint.get("observed_at")))
            return max((stamp for stamp in candidates if stamp is not None), default=0.0)

        positions = [
            position
            for position in self.engine.shadow_trader.state.get("positions", {}).values()
            if position.get("status") == "open" and position.get("launch_id") is not None
        ]
        positions.sort(key=lambda position: (
            newest_mark_timestamp(position),
            int(position["launch_id"]),
        ))
        return list(dict.fromkeys(int(position["launch_id"]) for position in positions))

    @staticmethod
    def _shadow_refresh_quota(open_count: int,
                              configured_limit: int | None = None) -> tuple[int, int, int]:
        required = (
            math.ceil(open_count * LEARNING_TRIGGER_SECONDS / SHADOW_STALE_MARK_SECONDS)
            if open_count > 0 else 0
        )
        target = required if configured_limit is None else max(0, configured_limit)
        target = min(target, MAX_SHADOW_PRIORITY_REFRESHES)
        return required, target, max(0, required - target)

    def _adaptive_headroom_safety(self) -> tuple[bool, str, float | None]:
        previous = _read_json(self.engine.root / "learning_summary.json", {}) or {}
        cycle = previous.get("cycle") or {}
        duration = _safe_float(cycle.get("duration_seconds"), None)
        errors = cycle.get("errors")
        if duration is None:
            return False, "no_previous_cycle_runtime", None
        if errors:
            return False, "previous_cycle_had_errors", duration
        if duration > ADAPTIVE_HEADROOM_MAX_PREVIOUS_DURATION_SECONDS:
            return False, "previous_cycle_runtime_guard", duration
        return True, "previous_cycle_within_runtime_guard", duration

    def run_once(self, limit: int = 10, refresh_limit: int = 3,
                 shadow_refresh_limit: int | None = None,
                 now: float | None = None) -> dict:
        started_monotonic = time.monotonic()
        now = time.time() if now is None else now
        counters = {"discovered": 0, "new_projects": 0, "outcomes": 0,
                    "migrations": 0, "paper_events": 0, "shadow_events": 0,
                    "refreshed": 0, "shadow_refreshed": 0,
                    "shadow_refresh_required": 0,
                    "shadow_refresh_target": 0,
                    "shadow_refresh_capacity_shortfall": 0,
                    "shadow_refresh_completion_shortfall": 0,
                    "shadow_refresh_priority_target_gap": 0,
                    "shadow_refresh_candidates": 0,
                    "shadow_refresh_selected": 0,
                    "shadow_refresh_baseline_selected": 0,
                    "shadow_refresh_baseline_shortfall": 0,
                    "adaptive_headroom_eligible": False,
                    "adaptive_headroom_used": False,
                    "adaptive_headroom_added_refreshes": 0,
                    "adaptive_headroom_reason": "not_evaluated",
                    "adaptive_headroom_previous_duration_seconds": None}
        errors: list[str] = []
        with LearningRunLock(self.lock_path):
            run_id = self.store.start_run()
            try:
                candidates = self.engine.observer.fetch_launches(limit=limit)
                counters["discovered"] = len(candidates)
                for candidate in candidates:
                    project = self.store.get_project(candidate.launch_id)
                    if project is not None:
                        # Discovery only deduplicates known IDs. Existing projects retain
                        # their scheduled refresh time; due checkpoints are independently
                        # prioritized by due_project_ids().
                        continue
                    try:
                        result = self.engine.evaluate_candidate(
                            candidate,
                            enter=True,
                            shadow_enter=True,
                            seal_analysis=True,
                            idempotency_key=f"base:project:{candidate.launch_id}:prediction:v1",
                        )
                        if result.get("paper_position") is not None:
                            # enter() already appended one paper_buy to the ledger.
                            counters["paper_events"] += 1
                        if result.get("shadow_position") is not None:
                            counters["shadow_events"] += 1
                        if self.store.add_project(candidate, result, now, self.horizons):
                            counters["new_projects"] += 1
                    except Exception as exc:
                        errors.append(f"new project {candidate.launch_id}: {exc}")

                shadow_priority_ids = self._open_shadow_project_ids_oldest_first()
                shadow_priority_set = set(shadow_priority_ids)
                required, baseline_target, _baseline_target_gap = self._shadow_refresh_quota(
                    len(shadow_priority_ids), shadow_refresh_limit
                )
                counters["shadow_refresh_required"] = required
                counters["shadow_refresh_candidates"] = len(shadow_priority_ids)
                baseline_refresh_ids = self.store.due_project_ids(
                    now,
                    refresh_limit,
                    priority_project_ids=shadow_priority_ids,
                    priority_target=required,
                    priority_addition_limit=baseline_target,
                )
                baseline_selected = sum(
                    project_id in shadow_priority_set
                    for project_id in baseline_refresh_ids
                )
                baseline_shortfall = max(0, required - baseline_selected)
                counters["shadow_refresh_baseline_selected"] = baseline_selected
                counters["shadow_refresh_baseline_shortfall"] = baseline_shortfall

                target = baseline_target
                refresh_ids = baseline_refresh_ids
                if shadow_refresh_limit is not None:
                    headroom_reason = "configured_limit_disables_adaptation"
                    safe = False
                    previous_duration = None
                elif baseline_shortfall <= 0:
                    headroom_reason = "baseline_capacity_sufficient"
                    safe = False
                    previous_duration = None
                else:
                    safe, headroom_reason, previous_duration = (
                        self._adaptive_headroom_safety()
                    )
                    counters["adaptive_headroom_eligible"] = safe
                    if safe:
                        target = min(
                            required, MAX_ADAPTIVE_SHADOW_PRIORITY_REFRESHES
                        )
                        refresh_ids = self.store.due_project_ids(
                            now,
                            refresh_limit,
                            priority_project_ids=shadow_priority_ids,
                            priority_target=required,
                            priority_addition_limit=target,
                        )

                counters["shadow_refresh_target"] = target
                counters["shadow_refresh_priority_target_gap"] = max(
                    0, required - target
                )
                counters["adaptive_headroom_reason"] = headroom_reason
                counters["adaptive_headroom_previous_duration_seconds"] = (
                    previous_duration
                )
                baseline_id_set = set(baseline_refresh_ids)
                counters["adaptive_headroom_added_refreshes"] = sum(
                    project_id not in baseline_id_set for project_id in refresh_ids
                )
                counters["shadow_refresh_selected"] = sum(
                    project_id in shadow_priority_set for project_id in refresh_ids
                )
                counters["adaptive_headroom_used"] = bool(
                    counters["adaptive_headroom_added_refreshes"]
                )
                # Regular/checkpoint work can overlap the open-shadow cohort.
                # Capacity must therefore be judged after the lanes are merged,
                # not from the dedicated priority target alone.
                counters["shadow_refresh_capacity_shortfall"] = max(
                    0, required - counters["shadow_refresh_selected"]
                )

                for project_id in refresh_ids:
                    project = self.store.get_project(project_id)
                    if project is None:
                        continue
                    try:
                        shadow_mark_updated = False
                        current = self.engine.observer.fetch_launch_by_id(project_id)
                        if current is None:
                            raise RuntimeError("official project lookup returned no Base candidate")
                        previous = self._candidate_from_project(project)
                        changed = (
                            previous.lifecycle != current.lifecycle
                            or previous.token_address.lower() != current.token_address.lower()
                        )
                        result = self.engine.evaluate_candidate(
                            current,
                            enter=True,
                            shadow_enter=True,
                            seal_analysis=False,
                            linked_analysis_ring=project.get("initial_analysis_ring"),
                        )
                        if result.get("paper_position") is not None:
                            # A candidate can first become entry-eligible on refresh.
                            counters["paper_events"] += 1
                        if result.get("shadow_position") is not None:
                            counters["shadow_events"] += 1
                        decision = self._decision_from_result(result)

                        if changed and not self.store.migration_exists(
                            project_id, current.token_address
                        ):
                            migration_ring = None
                            if self.engine.timechain:
                                migration_ring = self.engine.timechain.seal_migration(
                                    project_id, previous, current,
                                    project.get("initial_analysis_ring"), decision,
                                    f"base:project:{project_id}:migration:{current.token_address.lower()}",
                                )
                            self.store.record_migration(
                                project_id, previous, current, migration_ring
                            )
                            counters["migrations"] += 1

                        price = result.get("price_virtual")
                        if price and previous.token_address.lower() == current.token_address.lower():
                            counters["paper_events"] += len(
                                self.engine.trader.mark(
                                    current.token_address,
                                    price,
                                    decision,
                                    price_source=result.get("price_source"),
                                )
                            )
                            shadow_key = current.token_address.lower()
                            shadow_position_before = (
                                self.engine.shadow_trader.state.get("positions", {})
                                .get(shadow_key)
                            )
                            shadow_events = self.engine.shadow_trader.mark(
                                current.token_address,
                                price,
                                decision,
                                price_source=result.get("price_source"),
                            )
                            counters["shadow_events"] += len(shadow_events)
                            shadow_mark_updated = bool(
                                shadow_position_before
                                and shadow_position_before.get("status") in {"open", "closed"}
                            )

                        for checkpoint in self.store.pending_checkpoints(project_id, now):
                            comparable = (
                                bool(project.get("initial_price_virtual"))
                                and bool(price)
                                and current.token_address.lower()
                                == project["initial_token_address"].lower()
                            )
                            price_return = (
                                (price / project["initial_price_virtual"] - 1) * 100
                                if comparable else None
                            )
                            outcome = {
                                "observed_at": _utc_now(),
                                "lifecycle": current.lifecycle,
                                "token_address": current.token_address,
                                "price_virtual": price,
                                "price_source": result.get("price_source"),
                                "price_return_pct": price_return,
                                "comparable_price": comparable,
                                "comparison_note": (
                                    "same_contract" if comparable
                                    else "address_changed_or_initial_price_unavailable"
                                ),
                                "risk_level": decision.risk_level,
                                "score": decision.score,
                                "hard_stops": decision.hard_stops,
                                "warnings": decision.warnings,
                                "adverse_security_signal": bool(decision.hard_stops),
                                "paper_action": result.get("paper_action"),
                                "shadow_action": result.get("shadow_action"),
                            }
                            outcome_ring = None
                            if self.engine.timechain:
                                outcome_ring = self.engine.timechain.seal_outcome(
                                    project_id, current, decision,
                                    project.get("initial_analysis_ring"),
                                    checkpoint["horizon"], outcome,
                                    f"base:project:{project_id}:outcome:{checkpoint['horizon']}:v1",
                                )
                            self.store.complete_checkpoint(
                                project_id, checkpoint["horizon"], outcome, outcome_ring
                            )
                            counters["outcomes"] += 1
                        self.store.update_project(current, now)
                        counters["refreshed"] += 1
                        if project_id in shadow_priority_set and shadow_mark_updated:
                            counters["shadow_refreshed"] += 1
                    except Exception as exc:
                        errors.append(f"tracked project {project_id}: {exc}")
                        self.store.defer_project(project_id, now + LEARNING_REFRESH_SECONDS)

                counters["shadow_refresh_completion_shortfall"] = max(
                    0, required - counters["shadow_refreshed"]
                )
                status = "complete" if not errors else "complete_with_errors"
                self.store.finish_run(run_id, status, counters, errors)
            except Exception as exc:
                errors.append(f"learning cycle: {exc}")
                self.store.finish_run(run_id, "failed", counters, errors)
                raise

        counters["duration_seconds"] = round(time.monotonic() - started_monotonic, 3)
        summary = self.store.summary()
        summary["cycle"] = {**counters, "errors": errors}
        summary["portfolio_open_positions"] = len(self.engine.trader.open_positions)
        summary["shadow_open_positions"] = len(self.engine.shadow_trader.open_positions)
        shadow_report = ShadowPerformanceReporter(
            self.engine.shadow_trader,
            self.engine.shadow_ledger,
            self.store,
            self.engine.timechain,
        ).build(now=now)
        _atomic_json(self.engine.root / "shadow_performance.json", shadow_report)
        summary["shadow_performance"] = {
            "positions_opened": shadow_report["accounting"]["positions_opened"],
            "positions_closed": shadow_report["accounting"]["positions_closed"],
            "modeled_return_pct": shadow_report["accounting"][
                "modeled_return_pct_on_valued_positions"
            ],
            "mark_coverage_pct": shadow_report["coverage"]["mark_coverage_pct"],
            "stale_open_marks": shadow_report["coverage"]["stale_open_marks"],
            "median_open_mark_age_seconds": shadow_report["coverage"][
                "median_open_mark_age_seconds"
            ],
            "p95_open_mark_age_seconds": shadow_report["coverage"][
                "p95_open_mark_age_seconds"
            ],
            "oldest_open_mark_age_seconds": shadow_report["coverage"][
                "oldest_open_mark_age_seconds"
            ],
            "review_status": shadow_report["review_readiness"]["status"],
        }
        summary["refresh_policy"] = {
            "regular_limit": refresh_limit,
            "shadow_limit_mode": (
                "auto" if shadow_refresh_limit is None else "configured"
            ),
            # ``shadow_target`` is retained as a compatibility alias. It is the
            # dedicated priority target, not a ceiling on total shadow marks.
            "shadow_target": target,
            "shadow_priority_target": counters["shadow_refresh_target"],
            "shadow_required_for_freshness": required,
            "shadow_baseline_selected": counters[
                "shadow_refresh_baseline_selected"
            ],
            "shadow_baseline_shortfall": counters[
                "shadow_refresh_baseline_shortfall"
            ],
            "shadow_selected": counters["shadow_refresh_selected"],
            "shadow_completed": counters["shadow_refreshed"],
            "shadow_capacity_shortfall": counters[
                "shadow_refresh_capacity_shortfall"
            ],
            "shadow_completion_shortfall": counters[
                "shadow_refresh_completion_shortfall"
            ],
            "shadow_priority_target_gap": counters[
                "shadow_refresh_priority_target_gap"
            ],
            "adaptive_headroom_eligible": counters[
                "adaptive_headroom_eligible"
            ],
            "adaptive_headroom_used": counters["adaptive_headroom_used"],
            "adaptive_headroom_added_refreshes": counters[
                "adaptive_headroom_added_refreshes"
            ],
            "adaptive_headroom_reason": counters["adaptive_headroom_reason"],
            "adaptive_headroom_previous_duration_seconds": counters[
                "adaptive_headroom_previous_duration_seconds"
            ],
            "freshness_target_seconds": SHADOW_STALE_MARK_SECONDS,
            "scheduler_interval_seconds": LEARNING_TRIGGER_SECONDS,
            "maximum_shadow_priority_refreshes": MAX_SHADOW_PRIORITY_REFRESHES,
            "maximum_adaptive_shadow_priority_refreshes": (
                MAX_ADAPTIVE_SHADOW_PRIORITY_REFRESHES
            ),
            "adaptive_runtime_guard_seconds": (
                ADAPTIVE_HEADROOM_MAX_PREVIOUS_DURATION_SECONDS
            ),
        }
        _atomic_json(self.engine.root / "learning_summary.json", summary)
        return summary


def _print_results(results: Iterable[dict]) -> None:
    for item in results:
        candidate = item["candidate"]
        decision = item["decision"]
        print(
            f"{candidate['symbol']:<10} {decision['risk_level']:<8} "
            f"score={decision['score']:>5.1f} eligible={'YES' if decision['paper_entry_allowed'] else 'NO '} "
            f"price={item.get('price_virtual') or '?'} source={item.get('price_source') or 'unavailable'}"
        )
        print(f"  PAPER ACTION: {item.get('paper_action', 'unknown')}")
        for stop in decision["hard_stops"]:
            print(f"  STOP: {stop}")
        if item.get("paper_position"):
            print(f"  PAPER BUY: {item['paper_position']['cost_virtual']} VIRTUAL")


def _print_shadow_performance(report: dict) -> None:
    accounting = report["accounting"]
    coverage = report["coverage"]
    readiness = report["review_readiness"]
    marked_return = accounting["modeled_return_pct_on_valued_positions"]
    gross_open_return = accounting["weighted_gross_price_return_pct_open"]
    closed_return = accounting["closed_realized_return_pct"]
    win_rate = accounting["closed_win_rate_pct"]
    print("CHAINSEER SHADOW COHORT - COUNTERFACTUAL PAPER ONLY")
    print(
        f"Positions: opened={accounting['positions_opened']} "
        f"open={accounting['positions_open']} closed={accounting['positions_closed']}"
    )
    print(
        f"Modeled value: {accounting['modeled_liquidation_value_virtual']:.6f} VIRTUAL "
        f"on {accounting['cost_virtual_on_valued_positions']:.6f} VIRTUAL cost "
        f"(return={marked_return:.2f}%)"
        if marked_return is not None
        else "Modeled value: unavailable - no valued positions"
    )
    if gross_open_return is not None:
        friction_baseline = report["friction_baselines"][
            "prototype_flat_price_round_trip_return_pct"
        ]
        print(
            f"Open price move: {gross_open_return:.2f}% gross; "
            f"flat-price prototype friction baseline={friction_baseline:.2f}%"
        )
    print(
        f"Closed results: return={closed_return:.2f}% win_rate={win_rate:.1f}%"
        if closed_return is not None and win_rate is not None
        else "Closed results: unavailable - no closed shadow positions yet"
    )
    print(
        f"Coverage: shadow_projects={coverage['shadow_projects']}/"
        f"{coverage['tracked_projects']} tracked; marks={coverage['marked_positions']}/"
        f"{accounting['positions_opened']}; stale_open_marks={coverage['stale_open_marks']}"
    )
    p95_age = coverage.get("p95_open_mark_age_seconds")
    oldest_age = coverage.get("oldest_open_mark_age_seconds")
    if p95_age is not None and oldest_age is not None:
        print(
            f"Open-mark freshness: p95={p95_age / 60:.1f}m "
            f"oldest={oldest_age / 60:.1f}m "
            f"target<={SHADOW_STALE_MARK_SECONDS / 60:.0f}m"
        )
    print(
        f"Strategy review: {readiness['status']} "
        f"({readiness['closed_positions']}/"
        f"{readiness['minimum_closed_positions_for_strategy_review']} closed)"
    )
    for caveat in report["data_quality"]["caveats"]:
        print(f"  NOTE: {caveat}")


def main() -> None:
    ensure_utf8_runtime()
    parser = argparse.ArgumentParser(description="Chainseer Base observer and paper trader")
    parser.add_argument(
        "command",
        choices=[
            "observe", "paper-run", "learn-once", "positions",
            "shadow-positions", "shadow-summary", "verify",
        ],
    )
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--amount-virtual", type=float, default=10.0)
    parser.add_argument("--root", default="base_prototype")
    parser.add_argument("--chain-root", default="chainseer_chain")
    parser.add_argument("--rpc-url", default=BASE_RPC_URL)
    parser.add_argument("--refresh-limit", type=int, default=3)
    parser.add_argument(
        "--shadow-refresh-limit",
        type=int,
        default=None,
        help=(
            "open-shadow priority target per cycle; default derives it from "
            "cohort size and the 30-minute freshness target (safety-capped at 4)"
        ),
    )
    parser.add_argument("--no-timechain", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()

    policy = PaperPolicy(amount_virtual=args.amount_virtual)
    engine = BasePrototypeEngine(
        root=args.root,
        rpc_url=args.rpc_url,
        policy=policy,
        record_timechain=not args.no_timechain,
        chain_root=args.chain_root,
    )
    if args.command in {"observe", "paper-run"}:
        _print_results(engine.run_once(args.limit, enter=args.command == "paper-run"))
    elif args.command == "learn-once":
        summary = BaseLearningLoop(engine).run_once(
            limit=args.limit,
            refresh_limit=max(1, args.refresh_limit),
            shadow_refresh_limit=(
                max(0, args.shadow_refresh_limit)
                if args.shadow_refresh_limit is not None else None
            ),
        )
        cycle = summary["cycle"]
        shadow_return = summary["shadow_performance"]["modeled_return_pct"]
        shadow_return_text = (
            f"{shadow_return:.2f}%" if shadow_return is not None else "unavailable"
        )
        p95_mark_age = summary["shadow_performance"]["p95_open_mark_age_seconds"]
        oldest_mark_age = summary["shadow_performance"][
            "oldest_open_mark_age_seconds"
        ]
        p95_mark_text = (
            f"{p95_mark_age / 60:.1f}m" if p95_mark_age is not None else "unavailable"
        )
        oldest_mark_text = (
            f"{oldest_mark_age / 60:.1f}m"
            if oldest_mark_age is not None else "unavailable"
        )
        headroom_status = (
            "used" if cycle["adaptive_headroom_used"]
            else "eligible" if cycle["adaptive_headroom_eligible"]
            else "off"
        )
        print(
            "learn-once: "
            f"projects={summary['projects']} new={cycle['new_projects']} "
            f"refreshed={cycle['refreshed']} outcomes={cycle['outcomes']} "
            f"shadow_marks={cycle['shadow_refreshed']} "
            f"required={cycle['shadow_refresh_required']} "
            f"priority_target={cycle['shadow_refresh_target']} "
            f"freshness_shortfall={cycle['shadow_refresh_completion_shortfall']} "
            f"headroom={headroom_status} "
            f"migrations={cycle['migrations']} "
            f"paper_events={cycle['paper_events']} "
            f"shadow_events={cycle['shadow_events']} "
            f"shadow_open={summary['shadow_open_positions']} "
            f"shadow_closed={summary['shadow_performance']['positions_closed']} "
            f"shadow_return={shadow_return_text} "
            f"stale_marks={summary['shadow_performance']['stale_open_marks']} "
            f"p95_mark={p95_mark_text} oldest_mark={oldest_mark_text} "
            f"duration={cycle['duration_seconds']:.1f}s "
            f"errors={len(cycle['errors'])}"
        )
        for error in cycle["errors"]:
            print(f"  WARNING: {error}")
    elif args.command == "positions":
        print(json.dumps(engine.trader.state, indent=2, sort_keys=True))
    elif args.command == "shadow-positions":
        print(json.dumps(engine.shadow_trader.state, indent=2, sort_keys=True))
    elif args.command == "shadow-summary":
        store = BaseLearningStore(engine.root / "learning.sqlite3")
        report = ShadowPerformanceReporter(
            engine.shadow_trader,
            engine.shadow_ledger,
            store,
            engine.timechain,
        ).build()
        _atomic_json(engine.root / "shadow_performance.json", report)
        if args.json_output:
            print(json.dumps(report, indent=2, sort_keys=True))
        else:
            _print_shadow_performance(report)
    else:
        ok, report = engine.paper_ledger.verify()
        print(f"paper ledger: {'PASS' if ok else 'FAIL'} - {report}")
        shadow_ok, shadow_report = engine.shadow_ledger.verify()
        print(
            f"shadow ledger: {'PASS' if shadow_ok else 'FAIL'} - {shadow_report}"
        )
        learning_ok = True
        learning_path = engine.root / "learning.sqlite3"
        if learning_path.exists():
            learning_ok, learning_report = BaseLearningStore(learning_path).verify()
            print(
                f"learning store: {'PASS' if learning_ok else 'FAIL'} - {learning_report}"
            )
        if engine.timechain:
            tc_ok, tc_report = engine.timechain.tc.verify()
            print(f"Timechain: {'PASS' if tc_ok else 'FAIL'} - {tc_report}")
        if not ok or not shadow_ok or not learning_ok:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
