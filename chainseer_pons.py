"""Chainseer Pons paper/shadow adapter for Robinhood Chain.

This module is deliberately incapable of live trading. It contains no private
key handling, token approvals, transaction construction, signing, or broadcast
path. Every modeled fill comes from the deployed Quoter V2 at a pinned block
and every launch is bound through the Pons factory event, factory state,
canonical V3 pool, and locked position NFT before paper eligibility.

Official integration surface:
    https://docs.ponsfamily.com/
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import math
import os
import shutil
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urlparse

import requests

from chainseer import (
    ADDRESS_RE,
    ProvenanceLedger,
    RPCError,
    RobinhoodRPC,
    ScanContext,
    _get_skill_dir,
    _http_get_json,
    _load_skill_module,
    _load_timechain_module,
    ensure_utf8_runtime,
)
from chainseer_base import (
    LearningRunLock,
    LearningRunLockedError,
    LiveExecutionDisabledError,
    PaperTradeLedger,
)
from chainseer_outcome_ledger import (
    analysis_evidence_binding,
    build_outcome_record,
)
from chainseer_governance import (
    TIGHTEN_ONLY_POLICY_VERSION,
    cognitive_only_effect_manifest,
    migrate_cognitive_faculty_governance,
    seal_registry_mutation,
    register_faculty_governance,
    verify_governance_registry,
)


PONS_CHAIN_ID = 4663
#: Mirrors chainseer.py: only promotion/waking put a faculty into grown.json
#: and owe a governance record, while a birth still changes emergent.json
#: and so needs a fresh epoch. The two sets are deliberately different.
_REGISTRY_GOVERNED_ACTIONS = frozenset({"promoted", "woken"})
_REGISTRY_EPOCH_ACTIONS = frozenset({"born", "promoted", "woken"})
PONS_RPC_URL = os.environ.get(
    "CHAINSEER_PONS_RPC_URL", "https://rpc.mainnet.chain.robinhood.com"
)
PONS_BLOCKSCOUT_API = "https://robinhoodchain.blockscout.com/api/v2"
PONS_DEXSCREENER_API = "https://api.dexscreener.com/latest/dex"
PONS_WETH = "0x0Bd7D308f8E1639FAb988df18A8011f41EAcAD73"
PONS_V3_FACTORY = "0x1f7d7550B1b028f7571e69a784071F0205FD2EfA"
PONS_POSITION_MANAGER = "0x73991a25C818Bf1f1128dEAaB1492D45638DE0D3"
PONS_QUOTER_V2 = "0x33e885eD0Ec9bF04EcfB19341582aADCb4c8A9E7"
PONS_POOL_FEE = 10_000
PONS_FIXED_SUPPLY_RAW = 1_000_000_000 * 10**18
# A full learn-once cycle was measured at ~290s and grows with the catalog,
# so a 6-minute stale window was already 82% consumed. Once a live cycle
# outlives the window every overlapping guard pass tries to reclaim a lock
# whose holder is still running -- needless work at best, and on Windows it
# used to crash outright. Keep the window comfortably above the longest
# real cycle.
# Security-outcome horizons for Pons. Deliberately NOT Base's set: Pons is a
# high-frequency launch specialist whose positions are force-closed at
# maximum_hold_hours (72h), so 7d and 30d checkpoints would schedule
# measurements taken long after any position is gone -- the same lateness that
# already mislabelled most of the existing outcome corpus. The last horizon
# matches the maximum hold so the longest outcome still describes a window the
# system actually acts within.
#
# 5m and 1h were removed as UNSERVICEABLE, not as unimportant. An outcome is
# only judgeable if a second observation of the same candidate lands within
# horizon + tolerance, where tolerance is max(15 min, 25% of horizon). That
# gives a 20-minute window for 5m and 75 minutes for 1h. Covering ~950 tracked
# candidates at that cadence needs tens of thousands of admission refreshes per
# day, which no plausible budget reaches. Sealing a record at a horizon the
# schedule can never service manufactures rows the lateness gate is guaranteed
# to exclude -- which is how the existing corpus became mostly ungradeable.
# Declaring only what can be measured keeps the ledger honest.
# Admission refreshes granted per learn cycle. This is what makes a horizon
# measurable: an outcome is judgeable only if a SECOND observation of the same
# candidate lands inside horizon + tolerance, so the revisit rate -- not the
# discovery rate -- decides how much of the corpus can ever be graded. At 1
# refresh per cycle the mean revisit across ~950 tracked candidates was ~175h,
# wider than the widest horizon window (90h), so nothing was gradeable.
PONS_ADMISSION_REFRESH_LIMIT = 6

PONS_OUTCOME_HORIZONS: tuple[tuple[str, int], ...] = (
    ("6h", 6 * 60 * 60),
    ("24h", 24 * 60 * 60),
    ("72h", 72 * 60 * 60),
)


def _pons_observed_at(value) -> datetime | None:
    """Parse an observation timestamp, treating an unusable one as absent.

    An unparsable timestamp must not be silently coerced to "now" or to the
    epoch: either would place the observation at a horizon it never measured.
    """
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def pons_security_outcomes(
    baseline: dict, current: dict, *, horizon_seconds: int
) -> dict:
    """Diff two admission observations into a security-outcome record.

    Security facts only: whether the TOKEN turned out dangerous, which holds
    whether or not Pons ever traded it. That is the judgment worth validating
    for an admission-gated specialist -- 92% of analyses never trade, and a rug
    in a refused token is the strongest evidence the gate worked.

    Absent evidence stays absent: a field neither observation measured is
    omitted rather than reported as a benign zero.
    """
    result: dict = {"horizon_seconds": int(horizon_seconds)}
    base_stops = set(baseline.get("hard_stops") or [])
    now_stops = set(current.get("hard_stops") or [])
    new_stops = sorted(now_stops - base_stops)
    if new_stops:
        result["new_hard_stop_codes"] = new_stops

    base_liq = _safe_float(baseline.get("liquidity_usd"))
    now_liq = _safe_float(current.get("liquidity_usd"))
    if base_liq and base_liq > 0 and now_liq is not None:
        removed = max(0.0, (1 - (now_liq / base_liq)) * 100)
        result["liquidity_removed_pct"] = round(removed, 4)

    lowered = {code.lower() for code in now_stops}
    if any("honeypot" in code for code in lowered):
        result["honeypot_observed"] = True
    if any("rug" in code or "scam" in code for code in lowered):
        result["rug_pull"] = True
    if any("owner" in code or "authority" in code for code in lowered):
        result["owner_privilege_used"] = True

    base_holder = _safe_float(baseline.get("largest_real_holder_pct"))
    now_holder = _safe_float(current.get("largest_real_holder_pct"))
    if base_holder is not None and now_holder is not None:
        result["largest_holder_delta_pct"] = round(now_holder - base_holder, 4)

    result["observed_risk_level"] = current.get("risk_level")
    result["observed_score"] = current.get("score")
    return result


def pons_due_outcomes(
    observations: list[dict],
    completed: set[str],
    *,
    horizons: tuple[tuple[str, int], ...] = PONS_OUTCOME_HORIZONS,
    tolerance_fraction: float = 0.25,
    tolerance_floor_seconds: int = 15 * 60,
) -> list[dict]:
    """Select observations that genuinely measure each horizon.

    An observation is only evidence for the horizon it CLAIMS: it must fall at
    or after the horizon, and no later than the outcome-ledger tolerance would
    accept, or the sealed record would be excluded as observed-too-late anyway.
    Emitting it regardless would manufacture records that can never train
    anything.
    """
    ordered = [
        item for item in sorted(
            observations or [], key=lambda o: str(o.get("observed_at") or "")
        )
        if item.get("observed_at") and item.get("analysis_ring") is not None
    ]
    if len(ordered) < 2:
        return []
    baseline = ordered[0]
    start = _pons_observed_at(baseline.get("observed_at"))
    if start is None:
        return []
    due: list[dict] = []
    for name, seconds in horizons:
        key = f"{baseline.get('analysis_ring')}:{name}"
        if key in completed:
            continue
        tolerance = max(
            float(tolerance_floor_seconds), tolerance_fraction * float(seconds)
        )
        for candidate in ordered[1:]:
            moment = _pons_observed_at(candidate.get("observed_at"))
            if moment is None:
                continue
            elapsed = (moment - start).total_seconds()
            if elapsed < seconds:
                continue
            if elapsed - seconds > tolerance:
                break        # every later observation is later still
            due.append({
                "key": key,
                "horizon": name,
                "horizon_seconds": seconds,
                "baseline": baseline,
                "current": candidate,
                "elapsed_seconds": round(elapsed, 3),
            })
            break
    return due


PONS_RUN_LOCK_STALE_SECONDS = 20 * 60

# Longest guard pass observed in production. The guard and the learn cycle
# share .learn_once.lock, so this is the figure the wait budget has to clear.
# It is NOT the old ~53s: the guard grew as the catalog did, and 53s was
# measured back when it was being killed at a 2-minute scheduler limit before
# it could finish -- so the old number was an artifact of truncation, not a
# real runtime.
PONS_OBSERVED_GUARD_SECONDS = 232

# How long a cycle will wait for a busy lock before giving up. This must
# exceed the longest guard pass, or the learn cycle gives up while the guard
# is still legitimately working and exits 1. At 90s against guard runs of
# 145-232s that is exactly what happened: learn failed repeatedly, ~110s in,
# every time it started during a guard pass.
PONS_RUN_LOCK_WAIT_SECONDS = 300
ZERO_ADDRESS = "0x" + "0" * 40
DEAD_ADDRESS = "0x000000000000000000000000000000000000dead"

TOKEN_LAUNCHED_TOPIC = (
    "0xdb51ea9ad51ab453a65a4cb7e60c3cb378c9501bb002609f8f97778fb6c4235a"
)

# Selectors were derived with web3_sha3 on the official Robinhood Chain RPC
# from the verified ABI signatures and are covered by unit/live smoke tests.
GET_LAUNCHED_TOKEN = "0x3cf28b5a"
GRADUATION_STATUS = "0x98d652f1"
LIQUIDITY_POOL = "0x665a11ca"
V3_GET_POOL = "0x1698ee82"
OWNER_OF = "0x6352211e"
SLOT0 = "0x3850c7bd"
TOKEN0 = "0x0dfe1681"
TOKEN1 = "0xd21220a7"
POOL_FEE = "0xddca3f43"
POOL_LIQUIDITY = "0x1a686502"
QUOTE_EXACT_INPUT_SINGLE = "0xc6a5026a"
FACTORY_LOCKER = "0xd7b96d4e"
MAX_WALLET_AMOUNT = "0xaa4bde28"
MAX_TX_AMOUNT = "0x8c0b5e22"
RESTRICTION_END_BLOCK = "0x0861ac61"


@dataclass(frozen=True)
class PonsFactoryConfig:
    label: str
    factory: str
    locker: str
    start_block: int
    end_block: int | None = None


PONS_FACTORIES = (
    PonsFactoryConfig(
        "active",
        "0xA5aAb3F0c6EeadF30Ef1D3Eb997108E976351feB",
        "0x736D76699C26D0d966744cAe304C000d471f7F35",
        8_991_118,
    ),
    PonsFactoryConfig(
        "legacy",
        "0x0c37a24F5D23A486FA692d1500881d698B1F77a4",
        "0x31ca5E101941A93A7DD6d0497928700625CF54B5",
        8_600_612,
        8_991_117,
    ),
)
PONS_FACTORY_BY_ADDRESS = {
    item.factory.lower(): item for item in PONS_FACTORIES
}


from chainseer_core import (
    schedule_with_live_state,
    utc_now as _utc_now,
    canonical_json as _canonical_json,
    safe_float as _safe_float,
    atomic_json_write as _atomic_json,
    read_json as _read_json,
)
# _canonical_json/_atomic_json/_read_json/_safe_float/_utc_now moved to
# chainseer_core.py (shared with chainseer_base.py / chainseer_solana.py).
# _canonical_json here keeps its original ensure_ascii=True, default=str
# behavior (chainseer_core's defaults) so historical event_hash values in this
# adapter's ledger keep re-verifying. Do not add ensure_ascii=False here.


def _percentile(values: Iterable[float], quantile: float) -> float | None:
    ordered = sorted(float(value) for value in values)
    if not ordered:
        return None
    position = max(0.0, min(1.0, float(quantile))) * (len(ordered) - 1)
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _performance_distribution(
    outcomes: Iterable[dict],
    *,
    cost_key: str = "cost_eth",
    value_key: str = "value_eth",
    multiple_key: str = "multiple",
) -> dict:
    """Return raw and concentration-resistant cohort performance metrics."""
    priced = []
    for item in outcomes:
        cost = _safe_float(item.get(cost_key), None)
        value = _safe_float(item.get(value_key), None)
        multiple = _safe_float(item.get(multiple_key), None)
        if (
            cost is None
            or value is None
            or multiple is None
            or cost <= 0
        ):
            continue
        priced.append({
            "item": item,
            "cost": cost,
            "value": value,
            "multiple": multiple,
            "profit": value - cost,
        })

    total_cost = sum(item["cost"] for item in priced)
    total_value = sum(item["value"] for item in priced)
    multiples = [item["multiple"] for item in priced]
    modeled_return = (
        (total_value / total_cost - 1) * 100 if total_cost > 0 else None
    )
    best = max(priced, key=lambda item: item["profit"]) if priced else None
    remaining_cost = total_cost - (best["cost"] if best else 0.0)
    remaining_value = total_value - (best["value"] if best else 0.0)
    return_without_best = (
        (remaining_value / remaining_cost - 1) * 100
        if remaining_cost > 0 else None
    )

    trimmed = sorted(priced, key=lambda item: item["multiple"])
    if len(trimmed) >= 5:
        trimmed = trimmed[1:-1]
    trimmed_cost = sum(item["cost"] for item in trimmed)
    trimmed_value = sum(item["value"] for item in trimmed)
    trimmed_return = (
        (trimmed_value / trimmed_cost - 1) * 100
        if trimmed_cost > 0 else None
    )

    positive_profits = [
        max(0.0, item["profit"]) for item in priced
        if item["profit"] > 0
    ]
    total_positive_profit = sum(positive_profits)
    best_positive_profit_share = (
        max(positive_profits) / total_positive_profit * 100
        if total_positive_profit > 0 else None
    )
    profitable = sum(item["multiple"] >= 1 for item in priced)
    winner_rate = (
        profitable / len(priced) * 100 if priced else None
    )
    concentration_warning = bool(
        modeled_return is not None
        and modeled_return > 0
        and (
            profitable < 3
            or return_without_best is None
            or return_without_best <= 0
            or (
                best_positive_profit_share is not None
                and best_positive_profit_share > 60
            )
        )
    )
    best_item = (best or {}).get("item") or {}
    return {
        "cost_eth": total_cost,
        "value_eth": total_value,
        "modeled_return_pct": modeled_return,
        "return_without_best_pct": return_without_best,
        "trimmed_return_pct": trimmed_return,
        "median_multiple": _percentile(multiples, 0.5),
        "p25_multiple": _percentile(multiples, 0.25),
        "p75_multiple": _percentile(multiples, 0.75),
        "profitable": profitable,
        "losses": len(priced) - profitable,
        "winner_rate_pct": winner_rate,
        "best_positive_profit_share_pct": best_positive_profit_share,
        "best_position_value_share_pct": (
            best["value"] / total_value * 100
            if best and total_value > 0 else None
        ),
        "best_position_symbol": best_item.get("symbol"),
        "best_position_multiple": best["multiple"] if best else None,
        "concentration_warning": concentration_warning,
    }


def _iso_from_seconds(value: float) -> str:
    return datetime.fromtimestamp(float(value), timezone.utc).isoformat()


def _word_address(word: str) -> str:
    clean = str(word or "").removeprefix("0x").rjust(64, "0")
    return "0x" + clean[-40:]


def _address_word(address: str) -> str:
    if not ADDRESS_RE.fullmatch(str(address)):
        raise ValueError(f"invalid EVM address: {address}")
    return address.lower().removeprefix("0x").rjust(64, "0")


def _uint_word(value: int) -> str:
    if int(value) < 0 or int(value) >= 2**256:
        raise ValueError("ABI uint256 value out of range")
    return f"{int(value):064x}"


def _decode_words(raw: str, minimum: int = 1) -> list[str]:
    clean = str(raw or "").removeprefix("0x")
    if len(clean) < minimum * 64 or len(clean) % 64:
        raise ValueError(
            f"malformed ABI response: expected at least {minimum} words, got {len(clean) // 64}"
        )
    return [clean[index:index + 64] for index in range(0, len(clean), 64)]


def _hex_int(value) -> int:
    if isinstance(value, int):
        return value
    return int(str(value or "0x0"), 16)


def _get_pons_skill_dir() -> str:
    """Prefer the active Codex installation while honoring an explicit pin."""
    if os.environ.get("CHAINSEER_SKILL_DIR", "").strip():
        return _get_skill_dir()
    codex_skill = (
        Path.home() / ".codex" / "skills" / "cypher-tempre-self-model"
    )
    if (codex_skill / "timechain.py").is_file():
        return str(codex_skill)
    return _get_skill_dir()


@dataclass
class PonsLaunchCandidate:
    token_address: str
    deployer: str
    dex_factory: str
    pair_token: str
    pool_address: str
    factory_address: str
    factory_label: str
    expected_locker: str
    launch_block: int
    transaction_hash: str
    log_index: int
    dex_id: int
    launch_config_id: int
    position_id: int
    restrictions_end_block: int
    initial_buy_amount_raw: int
    name: str = "Unknown"
    symbol: str = "???"
    decimals: int = 18
    total_supply_raw: int = 0
    discovered_at: str = field(default_factory=_utc_now)

    @property
    def launch_id(self) -> str:
        return (
            f"{self.factory_address.lower()}:{self.transaction_hash.lower()}:"
            f"{self.log_index}"
        )

    def to_dict(self) -> dict:
        return asdict(self) | {"launch_id": self.launch_id}

    @classmethod
    def from_dict(cls, value: dict) -> "PonsLaunchCandidate":
        allowed = {item.name for item in cls.__dataclass_fields__.values()}
        return cls(**{key: value[key] for key in allowed if key in value})


@dataclass
class PonsQuote:
    token_in: str
    token_out: str
    amount_in_raw: int
    amount_out_raw: int
    sqrt_price_x96_after: int
    initialized_ticks_crossed: int
    gas_estimate: int
    block_pin: int
    quoter: str = PONS_QUOTER_V2
    pool_fee: int = PONS_POOL_FEE
    provenance: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PonsRiskDecision:
    token_address: str
    block_pin: int
    score: float
    risk_level: str
    paper_entry_allowed: bool
    live_entry_allowed: bool
    analysis_status: str
    infrastructure_errors: list[str]
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
    cognitive_ring: int | None = None
    cognition: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class PonsPaperPolicy:
    amount_eth: float = 0.01
    minimum_score: float = 75.0
    observation_blocks: int = 3
    maximum_positions: int = 5
    assumed_slippage_bps: int = 100
    modeled_gas_eth: float = 0.00002
    maximum_round_trip_loss_pct: float = 15.0
    maximum_quote_deviation_pct: float = 12.0
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
        if self.amount_eth <= 0:
            raise ValueError("amount_eth must be positive")
        if self.minimum_score < 0 or self.minimum_score > 100:
            raise ValueError("minimum_score must be between 0 and 100")
        if not 0 <= self.assumed_slippage_bps <= 5_000:
            raise ValueError("assumed_slippage_bps must be between 0 and 5000")
        if sum(fraction for _, fraction in self.take_profit_tiers) > 1.000001:
            raise ValueError("take-profit fractions cannot exceed 100%")


@dataclass(frozen=True)
class PonsAdmissionPolicy:
    """Paper-only pre-entry survival gate, separate from trade policy state."""

    minimum_observations: int = 2
    minimum_age_seconds: float = 300.0
    minimum_observation_spacing_seconds: float = 60.0
    maximum_round_trip_loss_pct: float = 8.0
    maximum_round_trip_deterioration_pct: float = 2.5
    maximum_liquidity_drop_pct: float = 20.0
    minimum_liquidity_usd: float = 0.0
    maximum_pending_hours: float = 24.0

    def __post_init__(self):
        if self.minimum_observations < 1:
            raise ValueError("minimum_observations must be at least one")
        if self.minimum_age_seconds < 0:
            raise ValueError("minimum_age_seconds cannot be negative")
        if self.minimum_observation_spacing_seconds < 0:
            raise ValueError(
                "minimum_observation_spacing_seconds cannot be negative"
            )
        if self.maximum_round_trip_loss_pct <= 0:
            raise ValueError("maximum_round_trip_loss_pct must be positive")
        if not 0 <= self.maximum_liquidity_drop_pct <= 100:
            raise ValueError(
                "maximum_liquidity_drop_pct must be between 0 and 100"
            )
        if self.minimum_liquidity_usd < 0:
            raise ValueError("minimum_liquidity_usd cannot be negative")
        if self.maximum_pending_hours <= 0:
            raise ValueError("maximum_pending_hours must be positive")


@dataclass(frozen=True)
class PonsAdmissionSchedulerPolicy:
    """Operational refresh policy; never changes the admission risk gate."""

    clean_candidate_delay_seconds: float = 60.0
    unsafe_cooldown_seconds: float = 900.0
    infrastructure_backoff_base_seconds: float = 120.0
    maximum_infrastructure_backoff_seconds: float = 3_600.0

    def __post_init__(self):
        for name, value in asdict(self).items():
            if value <= 0:
                raise ValueError(f"{name} must be positive")


@dataclass(frozen=True)
class PonsManagedPortfolioPolicy:
    """Capital-style limits for the bounded paper portfolio.

    These limits govern only the managed paper cohort. The shadow cohort stays
    uncapped so it can continue collecting counterfactual research evidence.
    """

    starting_capital_eth: float = 0.10
    maximum_concurrent_positions: int = 3
    maximum_gross_exposure_eth: float = 0.03
    maximum_daily_entries: int = 3
    maximum_daily_realized_loss_eth: float = 0.01
    maximum_drawdown_pct: float = 10.0
    maximum_consecutive_losses: int = 3
    cooldown_hours: float = 24.0
    require_promotable_active_policy: bool = True

    def __post_init__(self):
        if self.starting_capital_eth <= 0:
            raise ValueError("starting_capital_eth must be positive")
        if self.maximum_concurrent_positions < 1:
            raise ValueError(
                "maximum_concurrent_positions must be at least one"
            )
        if not 0 < self.maximum_gross_exposure_eth <= self.starting_capital_eth:
            raise ValueError(
                "maximum_gross_exposure_eth must be positive and no greater "
                "than starting capital"
            )
        if self.maximum_daily_entries < 1:
            raise ValueError("maximum_daily_entries must be at least one")
        if self.maximum_daily_realized_loss_eth <= 0:
            raise ValueError(
                "maximum_daily_realized_loss_eth must be positive"
            )
        if not 0 < self.maximum_drawdown_pct < 100:
            raise ValueError(
                "maximum_drawdown_pct must be between zero and 100"
            )
        if self.maximum_consecutive_losses < 1:
            raise ValueError(
                "maximum_consecutive_losses must be at least one"
            )
        if self.cooldown_hours <= 0:
            raise ValueError("cooldown_hours must be positive")


@dataclass
class PonsRPCHealth:
    """In-memory RPC quality telemetry shared by observer and analyzer."""

    total_attempts: int = 0
    successful_calls: int = 0
    retries: int = 0
    transient_failures: int = 0
    terminal_failures: int = 0
    last_error: str | None = None
    last_error_at: str | None = None
    last_success_at: str | None = None

    def record_success(self) -> None:
        self.successful_calls += 1
        self.last_success_at = _utc_now()

    def record_failure(self, exc: Exception, *, transient: bool) -> None:
        if transient:
            self.transient_failures += 1
        else:
            self.terminal_failures += 1
        self.last_error = str(exc)
        self.last_error_at = _utc_now()

    def snapshot(self) -> dict:
        return {
            **asdict(self),
            "success_rate_pct": (
                self.successful_calls / self.total_attempts * 100
                if self.total_attempts else None
            ),
            "paper_only": True,
            "live_execution_enabled": False,
        }


class PonsInfrastructureError(RuntimeError):
    """Required evidence could not be observed due to infrastructure."""

    def __init__(
        self,
        category: str,
        message: str,
        *,
        retryable: bool,
        attempts: int,
    ):
        self.category = category
        self.retryable = bool(retryable)
        self.attempts = int(attempts)
        super().__init__(
            f"{category} after {attempts} attempt(s): {message}"
        )


def _is_infrastructure_error(exc: Exception) -> bool:
    if isinstance(exc, PonsInfrastructureError):
        return True
    request_exception = getattr(
        getattr(requests, "exceptions", None),
        "RequestException",
        None,
    )
    if (
        isinstance(request_exception, type)
        and isinstance(exc, request_exception)
    ):
        return True
    if isinstance(exc, RPCError) and exc.code in {-1, -2}:
        return True
    text = str(exc).lower()
    return any(
        marker in text
        for marker in (
            "too many requests",
            "http 429",
            "429 client error",
            "timed out",
            "timeout",
            "cannot connect",
            "connection reset",
            "connection aborted",
            "temporarily unavailable",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        )
    )


class PonsRPC(RobinhoodRPC):
    """Pinned-block Pons and Uniswap V3 ABI reads."""

    TRANSIENT_HTTP_STATUS = {429, 500, 502, 503, 504}

    def __init__(
        self,
        rpc_url: str = PONS_RPC_URL,
        timeout: int = 30,
        ledger: ProvenanceLedger | None = None,
        *,
        health: PonsRPCHealth | None = None,
        maximum_attempts: int = 3,
        base_backoff_seconds: float = 0.5,
        sleeper: Callable[[float], None] = time.sleep,
    ):
        super().__init__(rpc_url, timeout, ledger)
        self.health = health or PonsRPCHealth()
        self.maximum_attempts = max(1, int(maximum_attempts))
        self.base_backoff_seconds = max(0.0, float(base_backoff_seconds))
        self._sleep = sleeper

    @staticmethod
    def _http_status(exc: Exception) -> int | None:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        try:
            return int(status) if status is not None else None
        except (TypeError, ValueError):
            return None

    def _retry_delay(self, exc: Exception, attempt: int) -> float:
        response = getattr(exc, "response", None)
        retry_after = (
            getattr(response, "headers", {}).get("Retry-After")
            if response is not None else None
        )
        try:
            if retry_after is not None:
                return min(10.0, max(0.0, float(retry_after)))
        except (TypeError, ValueError):
            pass
        deterministic_jitter = 0.05 * (1 + self._req_id % 3)
        return min(
            10.0,
            self.base_backoff_seconds * 2**max(0, attempt - 1)
            + deterministic_jitter,
        )

    def _call(self, method: str, params: list = None) -> dict:
        for attempt in range(1, self.maximum_attempts + 1):
            self.health.total_attempts += 1
            try:
                result = super()._call(method, params)
                self.health.record_success()
                return result
            except Exception as exc:
                status = self._http_status(exc)
                connection_error = getattr(
                    getattr(requests, "exceptions", None),
                    "ConnectionError",
                    None,
                )
                timeout_error = getattr(
                    getattr(requests, "exceptions", None),
                    "Timeout",
                    None,
                )
                transient = (
                    status in self.TRANSIENT_HTTP_STATUS
                    or (
                        isinstance(connection_error, type)
                        and isinstance(exc, connection_error)
                    )
                    or (
                        isinstance(timeout_error, type)
                        and isinstance(exc, timeout_error)
                    )
                    or (
                        isinstance(exc, RPCError)
                        and exc.code in {-1, -2}
                    )
                )
                infrastructure = transient or status is not None
                self.health.record_failure(exc, transient=transient)
                if transient and attempt < self.maximum_attempts:
                    self.health.retries += 1
                    self._sleep(self._retry_delay(exc, attempt))
                    continue
                if infrastructure:
                    category = (
                        f"rpc_http_{status}"
                        if status is not None else "rpc_transport"
                    )
                    raise PonsInfrastructureError(
                        category,
                        str(exc),
                        retryable=transient,
                        attempts=attempt,
                    ) from exc
                raise
        raise AssertionError("unreachable RPC retry state")

    def block_timestamp(self, block_number: int) -> int:
        block = self._call("eth_getBlockByNumber", [hex(block_number), False])
        if not isinstance(block, dict):
            raise ValueError(f"block {block_number} is unavailable")
        return _hex_int(block.get("timestamp"))

    def get_launched_token(self, factory: str, token: str) -> dict:
        words = _decode_words(
            self.call(factory, GET_LAUNCHED_TOKEN + _address_word(token)), 13
        )
        return {
            "token": _word_address(words[0]),
            "deployer": _word_address(words[1]),
            "paired_token": _word_address(words[2]),
            "position_manager": _word_address(words[3]),
            "position_id": int(words[4], 16),
            "dex_id": int(words[5], 16),
            "launch_config_id": int(words[6], 16),
            "restrictions_end_block": int(words[7], 16),
            "supply_raw": int(words[8], 16),
            "is_token0": bool(int(words[9], 16)),
            "pool_fee": int(words[10], 16),
            "exists": bool(int(words[11], 16)),
            "initial_buy_amount_raw": int(words[12], 16),
        }

    def graduation_status(self, factory: str, token: str) -> dict:
        words = _decode_words(
            self.call(factory, GRADUATION_STATUS + _address_word(token)), 3
        )
        principal = int(words[0], 16)
        threshold = int(words[1], 16)
        return {
            "paired_principal_raw": principal,
            "threshold_raw": threshold,
            "graduated": bool(int(words[2], 16)),
            "progress": principal / threshold if threshold else None,
        }

    def token_liquidity_pool(self, token: str) -> str:
        return _word_address(_decode_words(self.call(token, LIQUIDITY_POOL), 1)[0])

    def token_restriction_limits(self, token: str) -> dict:
        return {
            "max_wallet_amount_raw": int(
                _decode_words(self.call(token, MAX_WALLET_AMOUNT), 1)[0], 16
            ),
            "max_tx_amount_raw": int(
                _decode_words(self.call(token, MAX_TX_AMOUNT), 1)[0], 16
            ),
            "restriction_end_block": int(
                _decode_words(self.call(token, RESTRICTION_END_BLOCK), 1)[0],
                16,
            ),
        }

    def factory_locker(self, factory: str) -> str:
        return _word_address(_decode_words(self.call(factory, FACTORY_LOCKER), 1)[0])

    def v3_factory_pool(self, token_a: str, token_b: str, fee: int) -> str:
        data = (
            V3_GET_POOL + _address_word(token_a) + _address_word(token_b)
            + _uint_word(fee)
        )
        return _word_address(_decode_words(self.call(PONS_V3_FACTORY, data), 1)[0])

    def owner_of(self, token_id: int) -> str:
        data = OWNER_OF + _uint_word(token_id)
        return _word_address(
            _decode_words(self.call(PONS_POSITION_MANAGER, data), 1)[0]
        )

    def pool_snapshot(self, pool: str, is_token0: bool) -> dict:
        slot_words = _decode_words(self.call(pool, SLOT0), 7)
        sqrt_price_x96 = int(slot_words[0], 16)
        token0 = _word_address(_decode_words(self.call(pool, TOKEN0), 1)[0])
        token1 = _word_address(_decode_words(self.call(pool, TOKEN1), 1)[0])
        fee = int(_decode_words(self.call(pool, POOL_FEE), 1)[0], 16)
        liquidity = int(
            _decode_words(self.call(pool, POOL_LIQUIDITY), 1)[0], 16
        )
        ratio = sqrt_price_x96 / 2**96
        token1_per_token0 = ratio * ratio
        price_weth = (
            token1_per_token0 if is_token0
            else (1 / token1_per_token0 if token1_per_token0 else None)
        )
        return {
            "sqrt_price_x96": sqrt_price_x96,
            "tick": int(slot_words[1], 16)
            - (2**256 if int(slot_words[1], 16) >= 2**255 else 0),
            "observation_index": int(slot_words[2], 16),
            "observation_cardinality": int(slot_words[3], 16),
            "observation_cardinality_next": int(slot_words[4], 16),
            "fee_protocol": int(slot_words[5], 16),
            "unlocked": bool(int(slot_words[6], 16)),
            "token0": token0,
            "token1": token1,
            "fee": fee,
            "liquidity": liquidity,
            "price_weth": price_weth,
            "block_pin": self.context.block_pin if self.context else None,
        }

    def quote_exact_input_single(
        self, token_in: str, token_out: str, amount_in_raw: int, fee: int
    ) -> PonsQuote:
        data = (
            QUOTE_EXACT_INPUT_SINGLE
            + _address_word(token_in)
            + _address_word(token_out)
            + _uint_word(amount_in_raw)
            + _uint_word(fee)
            + _uint_word(0)
        )
        words = _decode_words(self.call(PONS_QUOTER_V2, data), 4)
        return PonsQuote(
            token_in=token_in,
            token_out=token_out,
            amount_in_raw=int(amount_in_raw),
            amount_out_raw=int(words[0], 16),
            sqrt_price_x96_after=int(words[1], 16),
            initialized_ticks_crossed=int(words[2], 16),
            gas_estimate=int(words[3], 16),
            block_pin=self.context.block_pin if self.context else 0,
            pool_fee=fee,
        )


class PonsObserver:
    """Restart-safe bounded factory-event indexer."""

    def __init__(
        self,
        rpc: PonsRPC,
        root: str | Path,
        *,
        block_chunk: int = 1_000,
        initial_lookback: int = 10_000,
        include_legacy_backfill: bool = False,
    ):
        self.rpc = rpc
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.cursor_path = self.root / "factory_cursors.json"
        self.catalog_path = self.root / "launch_catalog.json"
        self.block_chunk = max(100, int(block_chunk))
        self.initial_lookback = max(self.block_chunk, int(initial_lookback))
        self.include_legacy_backfill = bool(include_legacy_backfill)

    @staticmethod
    def decode_launch_log(log: dict, config: PonsFactoryConfig) -> PonsLaunchCandidate:
        topics = log.get("topics") or []
        if len(topics) != 4 or str(topics[0]).lower() != TOKEN_LAUNCHED_TOPIC:
            raise ValueError("not a Pons TokenLaunched log")
        words = _decode_words(log.get("data"), 7)
        return PonsLaunchCandidate(
            token_address=_word_address(topics[1]),
            deployer=_word_address(topics[2]),
            dex_factory=_word_address(topics[3]),
            pair_token=_word_address(words[0]),
            pool_address=_word_address(words[1]),
            factory_address=config.factory,
            factory_label=config.label,
            expected_locker=config.locker,
            launch_block=_hex_int(log.get("blockNumber")),
            transaction_hash=str(log.get("transactionHash") or ""),
            log_index=_hex_int(log.get("logIndex")),
            dex_id=int(words[2], 16),
            launch_config_id=int(words[3], 16),
            position_id=int(words[4], 16),
            restrictions_end_block=int(words[5], 16),
            initial_buy_amount_raw=int(words[6], 16),
        )

    def _enrich(self, candidate: PonsLaunchCandidate) -> PonsLaunchCandidate:
        try:
            candidate.name = self.rpc.erc20_name(candidate.token_address) or "Unknown"
        except Exception:
            candidate.name = "Unknown"
        try:
            candidate.symbol = self.rpc.erc20_symbol(candidate.token_address) or "???"
        except Exception:
            candidate.symbol = "???"
        try:
            candidate.decimals = self.rpc.erc20_decimals(candidate.token_address)
        except Exception:
            candidate.decimals = 18
        try:
            candidate.total_supply_raw = self.rpc.erc20_total_supply(
                candidate.token_address
            )
        except Exception:
            candidate.total_supply_raw = 0
        return candidate

    def sync(self, *, max_chunks: int = 50) -> list[PonsLaunchCandidate]:
        self.rpc.context = None
        self.rpc.ledger = None
        latest = self.rpc.get_block_number()
        ledger = ProvenanceLedger(self.root / "observer_evidence")
        ledger.block_pin = latest
        ledger.record(
            "rpc",
            {"method": "eth_blockNumber", "params": []},
            {"result": hex(latest)},
        )
        self.rpc.bind_context(ScanContext(PONS_CHAIN_ID, latest, ledger))
        cursors = _read_json(self.cursor_path, {})
        catalog = _read_json(self.catalog_path, {})
        for config in PONS_FACTORIES:
            cursor_key = config.factory.lower()
            stored = cursors.get(cursor_key)
            if (
                config.label == "legacy"
                and stored is None
                and not self.include_legacy_backfill
            ):
                continue
            factory_tip = min(latest, config.end_block or latest)
            start = (
                int(stored) + 1 if stored is not None
                else max(
                    config.start_block,
                    factory_tip - self.initial_lookback + 1,
                )
            )
            chunks = 0
            while start <= factory_tip and chunks < max(1, int(max_chunks)):
                end = min(factory_tip, start + self.block_chunk - 1)
                logs = self.rpc.get_logs(
                    start, end, address=config.factory,
                    topics=[TOKEN_LAUNCHED_TOPIC],
                )
                for raw in logs or []:
                    candidate = self._enrich(self.decode_launch_log(raw, config))
                    catalog[candidate.launch_id] = candidate.to_dict()
                cursors[cursor_key] = end
                # A killed process loses at most the in-flight chunk.
                _atomic_json(self.cursor_path, cursors)
                _atomic_json(self.catalog_path, catalog)
                _atomic_json(
                    self.root / "last_sync_provenance.json", ledger.to_dict()
                )
                start = end + 1
                chunks += 1
        _atomic_json(self.cursor_path, cursors)
        _atomic_json(self.catalog_path, catalog)
        _atomic_json(self.root / "last_sync_provenance.json", ledger.to_dict())
        candidates = [
            PonsLaunchCandidate.from_dict(value) for value in catalog.values()
        ]
        candidates.sort(
            key=lambda item: (item.launch_block, item.log_index), reverse=True
        )
        return candidates

    def fetch_launches(
        self, limit: int = 10, *, sync: bool = True, max_chunks: int = 50
    ) -> list[PonsLaunchCandidate]:
        candidates = (
            self.sync(max_chunks=max_chunks) if sync
            else [
                PonsLaunchCandidate.from_dict(value)
                for value in _read_json(self.catalog_path, {}).values()
            ]
        )
        candidates.sort(
            key=lambda item: (item.launch_block, item.log_index), reverse=True
        )
        return candidates[:max(1, int(limit))]

    def by_token(self, token: str) -> PonsLaunchCandidate | None:
        values = _read_json(self.catalog_path, {}).values()
        matches = [
            PonsLaunchCandidate.from_dict(value)
            for value in values
            if str(value.get("token_address", "")).lower() == token.lower()
        ]
        return max(matches, key=lambda item: item.launch_block) if matches else None


class PonsVerifiedSourceCache:
    """Bounded, hash-checked disk cache for positively verified source payloads.

    Market and holder data deliberately stay on the short process-local cache.
    Only positive contract-verification responses are durable because they are
    comparatively stable and are repeatedly requested for pending candidates.
    """

    SCHEMA_VERSION = 2

    def __init__(
        self,
        path: str | Path,
        *,
        ttl_seconds: int = 21_600,
        max_entries: int | None = None,
        max_payload_bytes: int | None = None,
    ):
        self.path = Path(path)
        self.ttl_seconds = max(1, int(ttl_seconds))
        self.max_entries = max(
            1,
            int(
                max_entries
                if max_entries is not None
                else os.environ.get("PONS_SOURCE_CACHE_SIZE", "256")
            ),
        )
        self.max_payload_bytes = max(
            1,
            int(
                max_payload_bytes
                if max_payload_bytes is not None
                else os.environ.get(
                    "PONS_SOURCE_CACHE_MAX_PAYLOAD_BYTES", "1000000"
                )
            ),
        )
        self._lock = threading.Lock()

    @staticmethod
    def _payload_hash(payload: dict) -> str:
        return hashlib.sha256(
            _canonical_json(payload).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _verified(payload: dict) -> bool:
        return isinstance(payload, dict) and bool(
            payload.get("is_verified") or payload.get("source_code")
        )

    def _load(self) -> dict:
        value = _read_json(self.path, {})
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != self.SCHEMA_VERSION
            or not isinstance(value.get("entries"), dict)
        ):
            return {
                "schema_version": self.SCHEMA_VERSION,
                "entries": {},
            }
        return value

    def get(self, url: str, *, now: float | None = None):
        now = time.time() if now is None else float(now)
        with self._lock:
            state = self._load()
            item = state["entries"].get(url)
            if not isinstance(item, dict):
                return None
            fetched_at = _safe_float(item.get("fetched_at"), None)
            evidence_path = item.get("evidence_path")
            if (
                fetched_at is None
                or now - fetched_at > self.ttl_seconds
                or not evidence_path
            ):
                return None
            try:
                path = Path(evidence_path)
                raw = path.read_bytes()
                if (
                    hashlib.sha256(raw).hexdigest()
                    != item.get("payload_sha256")
                ):
                    return None
                payload = json.loads(raw.decode("utf-8"))
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                return None
            if not self._verified(payload):
                return None
            return payload, fetched_at

    def set(
        self,
        url: str,
        payload: dict,
        *,
        fetched_at: float | None = None,
        evidence_path: str | Path | None = None,
    ) -> bool:
        if not self._verified(payload):
            return False
        canonical = _canonical_json(payload)
        canonical_bytes = canonical.encode("utf-8")
        if (
            len(canonical_bytes) > self.max_payload_bytes
            or evidence_path is None
        ):
            return False
        evidence_path = Path(evidence_path)
        try:
            retained = evidence_path.read_bytes()
        except OSError:
            return False
        payload_hash = hashlib.sha256(canonical_bytes).hexdigest()
        if hashlib.sha256(retained).hexdigest() != payload_hash:
            return False
        fetched_at = time.time() if fetched_at is None else float(fetched_at)
        with self._lock:
            state = self._load()
            entries = state["entries"]
            entries[url] = {
                "fetched_at": fetched_at,
                "payload_sha256": payload_hash,
                "evidence_path": str(evidence_path.resolve()),
            }
            if len(entries) > self.max_entries:
                oldest = sorted(
                    entries,
                    key=lambda key: _safe_float(
                        entries[key].get("fetched_at"), 0.0
                    ),
                )
                for key in oldest[:len(entries) - self.max_entries]:
                    entries.pop(key, None)
            _atomic_json(self.path, state)
        return True


class PonsRiskAnalyzer:
    """Conservative Pons-specific canonicality and executable-quote gate."""

    def __init__(
        self,
        rpc_url: str = PONS_RPC_URL,
        evidence_root: str | Path | None = None,
        *,
        rpc: PonsRPC | None = None,
        http_get: Callable = _http_get_json,
        policy: PonsPaperPolicy | None = None,
        source_cache_path: str | Path | None = None,
        source_cache_ttl_seconds: int | None = None,
        http_workers: int | None = None,
    ):
        self.rpc = rpc or PonsRPC(rpc_url)
        self.evidence_root = Path(evidence_root) if evidence_root else None
        self.http_get = http_get
        self.policy = policy or PonsPaperPolicy()
        if source_cache_path is None and self.evidence_root is not None:
            source_cache_path = (
                self.evidence_root.parent
                / "http_cache"
                / "verified_source.json"
            )
        self.source_cache = (
            PonsVerifiedSourceCache(
                source_cache_path,
                ttl_seconds=(
                    source_cache_ttl_seconds
                    if source_cache_ttl_seconds is not None
                    else int(os.environ.get(
                        "PONS_SOURCE_CACHE_TTL_SECONDS", "21600"
                    ))
                ),
            )
            if source_cache_path is not None else None
        )
        self.http_workers = max(
            1,
            min(
                5,
                int(
                    http_workers
                    if http_workers is not None
                    else os.environ.get("PONS_HTTP_EVIDENCE_WORKERS", "5")
                ),
            ),
        )

    @staticmethod
    def _required_failure(
        label: str,
        exc: Exception,
        hard_stops: list[str],
        infrastructure_errors: list[str],
    ) -> None:
        message = f"{label}: {exc}"
        if _is_infrastructure_error(exc):
            infrastructure_errors.append(message)
        else:
            hard_stops.append(message)

    @staticmethod
    def _indeterminate_decision(
        candidate: PonsLaunchCandidate,
        ledger: ProvenanceLedger,
        exc: Exception | None = None,
        *,
        block_pin: int = 0,
        infrastructure_errors: list[str] | None = None,
        coverage: dict | None = None,
        canonicality: dict | None = None,
    ) -> PonsRiskDecision:
        errors = list(infrastructure_errors or [])
        if exc is not None:
            errors.append(f"Block pin could not be established: {exc}")
        return PonsRiskDecision(
            token_address=candidate.token_address,
            block_pin=int(block_pin),
            score=0.0,
            risk_level="Indeterminate",
            paper_entry_allowed=False,
            live_entry_allowed=False,
            analysis_status="infrastructure_indeterminate",
            infrastructure_errors=errors,
            hard_stops=[],
            warnings=[
                "Required onchain evidence was unavailable; token risk was not classified"
            ],
            green_flags=[],
            coverage=coverage or {
                "factory_event": True,
                "factory_state": False,
                "pool_state": False,
                "locker": False,
                "token_restrictions": False,
                "quoter": False,
                "blockscout_source": False,
                "blockscout_holders": False,
                "dexscreener_canonical_pool": False,
            },
            canonicality=canonicality or {},
            market={},
            security={},
            provenance=ledger.to_dict(),
        )

    def _fetch(self, url: str, ledger: ProvenanceLedger) -> dict:
        payload, _, _ = self.http_get(url, params=None, ledger=ledger)
        return payload if isinstance(payload, dict) else {}

    def _source_status(
        self, address: str, ledger: ProvenanceLedger
    ) -> dict:
        url = f"{PONS_BLOCKSCOUT_API}/smart-contracts/{address}"
        try:
            cached = self.source_cache.get(url) if self.source_cache else None
            if cached is not None:
                value, fetched_at = cached
                ledger.record(
                    "http",
                    {
                        "url": url,
                        "cache_layer": "persistent_verified_source",
                    },
                    value,
                    cache_hit=True,
                    fetched_at=fetched_at,
                )
            else:
                value = self._fetch(url, ledger)
                if self.source_cache:
                    facts = ledger.to_dict().get("facts") or []
                    fact = facts[-1] if facts else {}
                    self.source_cache.set(
                        url,
                        value,
                        fetched_at=(
                            datetime.fromisoformat(
                                fact["fetched_at"]
                            ).timestamp()
                            if fact.get("fetched_at") else None
                        ),
                        evidence_path=fact.get("evidence_path"),
                    )
            return {
                "available": True,
                "is_verified": bool(
                    value.get("is_verified") or value.get("source_code")
                ),
                "name": value.get("name"),
                "compiler_version": value.get("compiler_version"),
                "source_code_sha256": (
                    hashlib.sha256(
                        str(value.get("source_code")).encode("utf-8")
                    ).hexdigest()
                    if value.get("source_code") else None
                ),
            }
        except Exception as exc:
            return {
                "available": False,
                "is_verified": False,
                "error": str(exc),
            }

    def _canonical_market(
        self, candidate: PonsLaunchCandidate, ledger: ProvenanceLedger
    ) -> dict:
        try:
            payload = self._fetch(
                f"{PONS_DEXSCREENER_API}/tokens/{candidate.token_address}",
                ledger,
            )
            matches = [
                pair for pair in (payload.get("pairs") or [])
                if str(pair.get("pairAddress", "")).lower()
                == candidate.pool_address.lower()
            ]
            if not matches:
                return {"available": False, "canonical_pool_only": True}
            pair = matches[0]
            return {
                "available": True,
                "canonical_pool_only": True,
                "pair_address": pair.get("pairAddress"),
                "price_usd": _safe_float(pair.get("priceUsd"), None),
                "liquidity_usd": _safe_float(
                    (pair.get("liquidity") or {}).get("usd"), None
                ),
                "volume_24h_usd": _safe_float(
                    (pair.get("volume") or {}).get("h24"), None
                ),
                "buys_24h": int(
                    _safe_float(((pair.get("txns") or {}).get("h24") or {}).get("buys"))
                ),
                "sells_24h": int(
                    _safe_float(((pair.get("txns") or {}).get("h24") or {}).get("sells"))
                ),
            }
        except Exception as exc:
            return {
                "available": False,
                "canonical_pool_only": True,
                "error": str(exc),
            }

    def _holder_concentration(
        self, candidate: PonsLaunchCandidate, locker: str,
        total_supply_raw: int, ledger: ProvenanceLedger,
    ) -> dict:
        try:
            payload = self._fetch(
                f"{PONS_BLOCKSCOUT_API}/tokens/{candidate.token_address}/holders",
                ledger,
            )
            holders = payload.get("items") or []
            excluded = {
                ZERO_ADDRESS.lower(),
                DEAD_ADDRESS.lower(),
                candidate.pool_address.lower(),
                locker.lower(),
            }
            ranked = []
            for item in holders[:50]:
                address_value = item.get("address") or {}
                address = str(
                    address_value.get("hash")
                    if isinstance(address_value, dict) else address_value
                )
                if address.lower() in excluded:
                    continue
                balance = int(str(item.get("value") or "0"))
                ranked.append({
                    "address": address,
                    "balance_raw": balance,
                    "share_pct": (
                        balance / total_supply_raw * 100
                        if total_supply_raw else None
                    ),
                })
            ranked.sort(key=lambda item: item["balance_raw"], reverse=True)
            return {
                "available": True,
                "excluded_addresses": sorted(excluded),
                "largest_real_holder_pct": (
                    ranked[0]["share_pct"] if ranked else None
                ),
                "top_real_holders": ranked[:10],
            }
        except Exception as exc:
            return {"available": False, "error": str(exc)}

    def _collect_http_evidence(
        self,
        candidate: PonsLaunchCandidate,
        config: PonsFactoryConfig,
        locker: str,
        total_supply_raw: int,
        ledger: ProvenanceLedger,
    ) -> dict:
        """Fetch independent HTTP evidence concurrently without shared writes."""
        specifications = (
            (
                "token_source",
                lambda isolated: self._source_status(
                    candidate.token_address, isolated
                ),
            ),
            (
                "factory_source",
                lambda isolated: self._source_status(
                    candidate.factory_address, isolated
                ),
            ),
            (
                "locker_source",
                lambda isolated: self._source_status(
                    config.locker, isolated
                ),
            ),
            (
                "holder_concentration",
                lambda isolated: self._holder_concentration(
                    candidate, locker, total_supply_raw, isolated
                ),
            ),
            (
                "canonical_market",
                lambda isolated: self._canonical_market(
                    candidate, isolated
                ),
            ),
        )

        def run(specification):
            name, operation = specification
            isolated = ProvenanceLedger(ledger.evidence_dir)
            isolated.block_pin = ledger.block_pin
            return name, operation(isolated), isolated

        if self.http_workers == 1:
            completed = [run(specification) for specification in specifications]
        else:
            with ThreadPoolExecutor(
                max_workers=self.http_workers,
                thread_name_prefix="pons-http-evidence",
            ) as executor:
                futures = [
                    executor.submit(run, specification)
                    for specification in specifications
                ]
                # Consume in specification order so fact IDs stay deterministic.
                completed = [future.result() for future in futures]

        evidence = {}
        for name, value, isolated in completed:
            ledger.absorb(isolated)
            evidence[name] = value
        return evidence

    def analyze(self, candidate: PonsLaunchCandidate) -> PonsRiskDecision:
        evidence_dir = (
            self.evidence_root / candidate.token_address.lower()
            if self.evidence_root else None
        )
        ledger = ProvenanceLedger(evidence_dir)
        self.rpc.context = None
        self.rpc.ledger = None
        try:
            block_pin = self.rpc.get_block_number()
        except Exception as exc:
            if _is_infrastructure_error(exc):
                return self._indeterminate_decision(candidate, ledger, exc)
            raise
        ledger.block_pin = block_pin
        self.rpc.bind_context(ScanContext(PONS_CHAIN_ID, block_pin, ledger))

        hard_stops: list[str] = []
        infrastructure_errors: list[str] = []
        warnings: list[str] = []
        green_flags: list[str] = []
        coverage = {
            "factory_event": True,
            "factory_state": False,
            "pool_state": False,
            "locker": False,
            "token_restrictions": False,
            "quoter": False,
            "blockscout_source": False,
            "blockscout_holders": False,
            "dexscreener_canonical_pool": False,
        }

        config = PONS_FACTORY_BY_ADDRESS.get(candidate.factory_address.lower())
        if config is None:
            hard_stops.append("Factory is not in the pinned Pons allowlist")
            config = PonsFactoryConfig(
                "unknown", candidate.factory_address, ZERO_ADDRESS, 0
            )
        elif candidate.launch_block < config.start_block:
            hard_stops.append("Launch event predates the configured factory start block")
        else:
            green_flags.append(
                f"TokenLaunched was emitted by the pinned {config.label} Pons factory"
            )

        code_status = {}
        for label, address in (
            ("token", candidate.token_address),
            ("pool", candidate.pool_address),
            ("factory", candidate.factory_address),
            ("expected_locker", config.locker),
            ("quoter_v2", PONS_QUOTER_V2),
        ):
            try:
                code = self.rpc.get_code(address)
                present = bool(code and code != "0x")
                code_status[label] = {
                    "present": present,
                    "sha256": (
                        hashlib.sha256(code.encode("ascii", "ignore")).hexdigest()
                        if code else None
                    ),
                }
                if not present:
                    hard_stops.append(f"No bytecode at required {label} address")
            except Exception as exc:
                code_status[label] = {"present": False, "error": str(exc)}
                self._required_failure(
                    f"Could not verify {label} bytecode",
                    exc,
                    hard_stops,
                    infrastructure_errors,
                )
        if infrastructure_errors:
            return self._indeterminate_decision(
                candidate,
                ledger,
                block_pin=block_pin,
                infrastructure_errors=infrastructure_errors,
                coverage=coverage,
                canonicality={"code": code_status},
            )

        launched = {}
        graduation = {}
        restriction_limits = {}
        resolved_pool = token_pool = resolved_locker = position_owner = ZERO_ADDRESS
        pool = {}
        try:
            launched = self.rpc.get_launched_token(
                candidate.factory_address, candidate.token_address
            )
            coverage["factory_state"] = True
            checks = {
                "exists": launched["exists"],
                "token_matches": (
                    launched["token"].lower() == candidate.token_address.lower()
                ),
                "deployer_matches": (
                    launched["deployer"].lower() == candidate.deployer.lower()
                ),
                "paired_token_is_weth": (
                    launched["paired_token"].lower() == PONS_WETH.lower()
                    and candidate.pair_token.lower() == PONS_WETH.lower()
                ),
                "position_manager_matches": (
                    launched["position_manager"].lower()
                    == PONS_POSITION_MANAGER.lower()
                ),
                "position_id_matches": (
                    launched["position_id"] == candidate.position_id
                ),
                "pool_fee_matches": launched["pool_fee"] == PONS_POOL_FEE,
                "supply_matches": (
                    launched["supply_raw"] == PONS_FIXED_SUPPLY_RAW
                ),
                "restrictions_match": (
                    launched["restrictions_end_block"]
                    == candidate.restrictions_end_block
                ),
            }
            for label, passed in checks.items():
                if not passed:
                    hard_stops.append(
                        f"Factory-state canonicality check failed: {label}"
                    )
            if all(checks.values()):
                green_flags.append(
                    "Factory state independently matches the launch event and fixed template"
                )
            graduation = self.rpc.graduation_status(
                candidate.factory_address, candidate.token_address
            )
        except Exception as exc:
            checks = {}
            self._required_failure(
                "Factory state could not be decoded",
                exc,
                hard_stops,
                infrastructure_errors,
            )
        if infrastructure_errors:
            return self._indeterminate_decision(
                candidate,
                ledger,
                block_pin=block_pin,
                infrastructure_errors=infrastructure_errors,
                coverage=coverage,
                canonicality={
                    "factory_state_checks": checks,
                    "code": code_status,
                },
            )

        try:
            token_pool = self.rpc.token_liquidity_pool(candidate.token_address)
            resolved_pool = self.rpc.v3_factory_pool(
                candidate.token_address, PONS_WETH, PONS_POOL_FEE
            )
            is_token0 = bool(launched.get("is_token0"))
            pool = self.rpc.pool_snapshot(candidate.pool_address, is_token0)
            coverage["pool_state"] = True
            pool_tokens = {pool["token0"].lower(), pool["token1"].lower()}
            pool_checks = {
                "event_pool_matches_token": (
                    token_pool.lower() == candidate.pool_address.lower()
                ),
                "event_pool_matches_v3_factory": (
                    resolved_pool.lower() == candidate.pool_address.lower()
                ),
                "pool_assets_match": pool_tokens
                == {candidate.token_address.lower(), PONS_WETH.lower()},
                "pool_fee_matches": pool["fee"] == PONS_POOL_FEE,
                "pool_has_active_liquidity": pool["liquidity"] > 0,
                "pool_is_unlocked": pool["unlocked"],
            }
            for label, passed in pool_checks.items():
                if not passed:
                    hard_stops.append(
                        f"Canonical V3 pool check failed: {label}"
                    )
            if all(pool_checks.values()):
                green_flags.append(
                    "Token, event, and V3 factory resolve to the same active WETH pool"
                )
        except Exception as exc:
            pool_checks = {}
            self._required_failure(
                "Canonical pool state could not be decoded",
                exc,
                hard_stops,
                infrastructure_errors,
            )
        if infrastructure_errors:
            return self._indeterminate_decision(
                candidate,
                ledger,
                block_pin=block_pin,
                infrastructure_errors=infrastructure_errors,
                coverage=coverage,
                canonicality={
                    "factory_state_checks": checks,
                    "pool_checks": pool_checks,
                    "code": code_status,
                },
            )

        try:
            resolved_locker = self.rpc.factory_locker(candidate.factory_address)
            position_owner = self.rpc.owner_of(candidate.position_id)
            coverage["locker"] = True
            locker_checks = {
                "factory_locker_matches_allowlist": (
                    resolved_locker.lower() == config.locker.lower()
                ),
                "position_nft_owned_by_locker": (
                    position_owner.lower() == resolved_locker.lower()
                ),
            }
            for label, passed in locker_checks.items():
                if not passed:
                    hard_stops.append(f"Liquidity-lock check failed: {label}")
            if all(locker_checks.values()):
                green_flags.append(
                    "The canonical V3 position NFT is owned by the factory-resolved locker"
                )
        except Exception as exc:
            locker_checks = {}
            self._required_failure(
                "V3 position lock could not be verified",
                exc,
                hard_stops,
                infrastructure_errors,
            )
        if infrastructure_errors:
            return self._indeterminate_decision(
                candidate,
                ledger,
                block_pin=block_pin,
                infrastructure_errors=infrastructure_errors,
                coverage=coverage,
                canonicality={
                    "factory_state_checks": checks,
                    "pool_checks": pool_checks,
                    "locker_checks": locker_checks,
                    "code": code_status,
                },
            )

        entry_quote = exit_quote = None
        quote_metrics = {}
        amount_eth_raw = max(1, int(self.policy.amount_eth * 10**18))
        try:
            entry_quote = self.rpc.quote_exact_input_single(
                PONS_WETH, candidate.token_address, amount_eth_raw, PONS_POOL_FEE
            )
            if entry_quote.amount_out_raw <= 0:
                raise ValueError("entry quote returned zero tokens")
            exit_quote = self.rpc.quote_exact_input_single(
                candidate.token_address, PONS_WETH,
                entry_quote.amount_out_raw, PONS_POOL_FEE,
            )
            if exit_quote.amount_out_raw <= 0:
                raise ValueError("round-trip exit quote returned zero WETH")
            coverage["quoter"] = True
            round_trip_loss_pct = max(
                0.0, (1 - exit_quote.amount_out_raw / amount_eth_raw) * 100
            )
            price_weth = _safe_float(pool.get("price_weth"), None)
            expected_out = (
                amount_eth_raw / price_weth if price_weth and price_weth > 0
                else None
            )
            quote_deviation_pct = (
                max(
                    0.0,
                    (1 - entry_quote.amount_out_raw / expected_out) * 100,
                )
                if expected_out else None
            )
            quote_metrics = {
                "amount_eth": self.policy.amount_eth,
                "entry": entry_quote.to_dict(),
                "immediate_exit": exit_quote.to_dict(),
                "round_trip_loss_pct": round_trip_loss_pct,
                "quote_deviation_from_slot0_pct": quote_deviation_pct,
            }
            restriction_limits = self.rpc.token_restriction_limits(
                candidate.token_address
            )
            coverage["token_restrictions"] = True
            restrictions_active = (
                block_pin <= restriction_limits["restriction_end_block"]
            )
            quote_within_max_tx = (
                entry_quote.amount_out_raw
                <= restriction_limits["max_tx_amount_raw"]
            )
            quote_within_max_wallet = (
                entry_quote.amount_out_raw
                <= restriction_limits["max_wallet_amount_raw"]
            )
            restriction_limits.update({
                "active": restrictions_active,
                "quote_within_max_tx": quote_within_max_tx,
                "quote_within_max_wallet": quote_within_max_wallet,
                "modeled_wallet_existing_balance_raw": 0,
                "eligible_for_modeled_buy": (
                    block_pin > candidate.launch_block
                    and (
                        not restrictions_active
                        or (quote_within_max_tx and quote_within_max_wallet)
                    )
                ),
            })
            if (
                restriction_limits["restriction_end_block"]
                != candidate.restrictions_end_block
            ):
                hard_stops.append(
                    "Token restriction end block does not match factory state"
                )
            if restrictions_active and not quote_within_max_tx:
                hard_stops.append(
                    "Executable entry quote exceeds the token's onchain max-transaction limit"
                )
            if restrictions_active and not quote_within_max_wallet:
                hard_stops.append(
                    "Executable entry quote exceeds the modeled wallet's onchain max-wallet limit"
                )
            if round_trip_loss_pct > self.policy.maximum_round_trip_loss_pct:
                hard_stops.append(
                    "Executable round-trip loss "
                    f"{round_trip_loss_pct:.2f}% exceeds "
                    f"{self.policy.maximum_round_trip_loss_pct:.2f}%"
                )
            if (
                quote_deviation_pct is not None
                and quote_deviation_pct > self.policy.maximum_quote_deviation_pct
            ):
                hard_stops.append(
                    "Entry quote deviation "
                    f"{quote_deviation_pct:.2f}% exceeds "
                    f"{self.policy.maximum_quote_deviation_pct:.2f}%"
                )
            if not hard_stops:
                green_flags.append(
                    "Entry and immediate-exit executable quotes passed the size gate"
                )
            if restrictions_active and restriction_limits[
                "eligible_for_modeled_buy"
            ]:
                green_flags.append(
                    "Launch protection is active, but the exact paper quote is below "
                    "the token's onchain transaction and wallet caps"
                )
        except Exception as exc:
            self._required_failure(
                "Executable Quoter V2 path unavailable",
                exc,
                hard_stops,
                infrastructure_errors,
            )
        if infrastructure_errors:
            return self._indeterminate_decision(
                candidate,
                ledger,
                block_pin=block_pin,
                infrastructure_errors=infrastructure_errors,
                coverage=coverage,
                canonicality={
                    "factory_state_checks": checks,
                    "pool_checks": pool_checks,
                    "locker_checks": locker_checks,
                    "code": code_status,
                },
            )

        total_supply_raw = (
            int(launched.get("supply_raw") or 0)
            or int(candidate.total_supply_raw or 0)
        )
        http_evidence = self._collect_http_evidence(
            candidate,
            config,
            resolved_locker,
            total_supply_raw,
            ledger,
        )
        token_source = http_evidence["token_source"]
        factory_source = http_evidence["factory_source"]
        locker_source = http_evidence["locker_source"]
        coverage["blockscout_source"] = any(
            value.get("available")
            for value in (token_source, factory_source, locker_source)
        )
        if token_source.get("is_verified"):
            green_flags.append("Launch token source is verified on Blockscout")
        else:
            warnings.append("Launch token source is not verified on Blockscout")
        if not factory_source.get("is_verified"):
            warnings.append("Factory source verification is unavailable")
        if not locker_source.get("is_verified"):
            warnings.append("Locker source verification is unavailable")

        concentration = http_evidence["holder_concentration"]
        coverage["blockscout_holders"] = concentration.get("available", False)
        top_pct = _safe_float(
            concentration.get("largest_real_holder_pct"), None
        )
        if top_pct is None:
            warnings.append("Real-holder concentration is currently unavailable")
        elif top_pct >= 35:
            hard_stops.append(
                f"Largest real holder controls {top_pct:.2f}% of fixed supply"
            )
        elif top_pct >= 15:
            warnings.append(
                f"Largest real holder controls {top_pct:.2f}% of fixed supply"
            )
        else:
            green_flags.append(
                f"Largest real holder is {top_pct:.2f}% after pool/locker exclusions"
            )

        canonical_market = http_evidence["canonical_market"]
        coverage["dexscreener_canonical_pool"] = canonical_market.get(
            "available", False
        )
        if not canonical_market.get("available"):
            warnings.append(
                "Canonical-pool DexScreener metrics are unavailable; no other pools were substituted"
            )

        if (
            restriction_limits.get("active")
            and restriction_limits.get("eligible_for_modeled_buy")
        ):
            warnings.append(
                "Launch protection remains active; eligibility assumes a fresh paper "
                "wallet with zero pre-existing token balance"
            )
        if graduation.get("graduated"):
            green_flags.append(
                "Graduation threshold reached; trading remains in the same canonical pool"
            )

        score = 100.0
        score -= min(80.0, 30.0 * len(hard_stops))
        score -= min(25.0, 3.0 * len(warnings))
        if not coverage["blockscout_holders"]:
            score -= 5
        if not token_source.get("is_verified"):
            score -= 5
        score = round(max(0.0, min(100.0, score)), 1)
        if infrastructure_errors:
            risk_level = "Indeterminate"
        elif hard_stops or score < 40:
            risk_level = "Critical"
        elif score < 60:
            risk_level = "High"
        elif score < 85:
            risk_level = "Medium"
        else:
            risk_level = "Low"
        paper_allowed = (
            not infrastructure_errors
            and not hard_stops
            and score >= self.policy.minimum_score
            and coverage["quoter"]
        )
        analysis_status = (
            "infrastructure_indeterminate"
            if infrastructure_errors else
            "complete_safe" if paper_allowed else
            "complete_unsafe"
        )
        canonicality = {
            "event_factory_allowlisted": config.label != "unknown",
            "factory_state_checks": checks,
            "pool_checks": pool_checks,
            "locker_checks": locker_checks,
            "event_pool": candidate.pool_address,
            "token_reported_pool": token_pool,
            "v3_factory_reported_pool": resolved_pool,
            "factory_reported_locker": resolved_locker,
            "position_nft_owner": position_owner,
            "position_id": candidate.position_id,
            "code": code_status,
        }
        return PonsRiskDecision(
            token_address=candidate.token_address,
            block_pin=block_pin,
            score=score,
            risk_level=risk_level,
            paper_entry_allowed=paper_allowed,
            live_entry_allowed=False,
            analysis_status=analysis_status,
            infrastructure_errors=infrastructure_errors,
            hard_stops=hard_stops,
            warnings=warnings,
            green_flags=green_flags,
            coverage=coverage,
            canonicality=canonicality,
            market={
                "slot0": pool,
                "executable_quote": quote_metrics,
                "canonical_pool_market": canonical_market,
                "graduation": graduation,
            },
            security={
                "token_source": token_source,
                "factory_source": factory_source,
                "locker_source": locker_source,
                "holder_concentration": concentration,
                "restriction_limits": restriction_limits,
                "fixed_supply_expected_raw": PONS_FIXED_SUPPLY_RAW,
                "launch_restrictions_end_block": candidate.restrictions_end_block,
                "http_evidence_pipeline": {
                    "workers": self.http_workers,
                    "candidate_parallelism": False,
                    "mutable_cache_ttl_seconds": getattr(
                        getattr(self.http_get, "__self__", None),
                        "ttl_seconds",
                        int(os.environ.get("CHAINSEER_API_CACHE_TTL", "30")),
                    ),
                    "verified_source_cache": bool(self.source_cache),
                    "verified_source_cache_ttl_seconds": (
                        self.source_cache.ttl_seconds
                        if self.source_cache else 0
                    ),
                },
            },
            provenance=ledger.to_dict(),
        )


class PonsAdmissionQuarantine:
    """Persistent multi-observation gate in front of every paper entry."""

    SCHEMA_VERSION = 2
    TERMINAL_HARD_STOP_MARKERS = (
        "Factory is not in the pinned Pons allowlist",
        "Launch event predates the configured factory start block",
        "No bytecode at required",
        "Factory-state canonicality check failed",
        "Canonical V3 pool check failed: event_pool_matches",
        "Canonical V3 pool check failed: pool_assets_match",
        "Canonical V3 pool check failed: pool_fee_matches",
        "Liquidity-lock check failed",
        "Token restriction end block does not match factory state",
    )

    def __init__(
        self,
        state_path: str | Path,
        policy: PonsAdmissionPolicy | None = None,
        scheduler_policy: PonsAdmissionSchedulerPolicy | None = None,
    ):
        self.state_path = Path(state_path)
        self.policy = policy or PonsAdmissionPolicy()
        self.scheduler_policy = (
            scheduler_policy or PonsAdmissionSchedulerPolicy()
        )
        self.state = self._load()
        self._reconcile_state()

    def _policy_signature(self, policy: PonsAdmissionPolicy | None = None) -> str:
        return hashlib.sha256(
            _canonical_json(asdict(policy or self.policy)).encode("utf-8")
        ).hexdigest()

    def _scheduler_signature(self) -> str:
        return hashlib.sha256(
            _canonical_json(asdict(self.scheduler_policy)).encode("utf-8")
        ).hexdigest()

    @staticmethod
    def _legacy_analysis_status(observation: dict) -> str:
        explicit = observation.get("analysis_status")
        if explicit in {
            "complete_safe",
            "complete_unsafe",
            "infrastructure_indeterminate",
        }:
            return explicit
        errors = list(observation.get("infrastructure_errors") or [])
        combined = " ".join(
            errors + list(observation.get("hard_stops") or [])
        ).lower()
        infrastructure_markers = (
            "too many requests",
            "429 client error",
            "http 429",
            "timed out",
            "cannot connect",
            "connection reset",
            "connection aborted",
            "service unavailable",
            "bad gateway",
            "gateway timeout",
        )
        if any(marker in combined for marker in infrastructure_markers):
            return "infrastructure_indeterminate"
        return (
            "complete_safe"
            if observation.get("paper_entry_allowed")
            else "complete_unsafe"
        )

    def _load(self) -> dict:
        default = {
            "schema_version": self.SCHEMA_VERSION,
            "protocol": "pons",
            "chain_id": PONS_CHAIN_ID,
            "policy": asdict(self.policy),
            "policy_sha256": self._policy_signature(),
            "scheduler_policy": asdict(self.scheduler_policy),
            "scheduler_policy_sha256": self._scheduler_signature(),
            "scheduler": {
                "last_planned_at": None,
                "selected": [],
                "due": 0,
                "deferred": 0,
            },
            "migrations": [],
            "candidates": {},
            "paper_only": True,
            "live_execution_enabled": False,
        }
        value = _read_json(self.state_path, default)
        existing = value.get("policy_sha256")
        if existing and existing != self._policy_signature():
            raise ValueError(
                "Existing Pons admission cohort uses a different policy; "
                "choose a new root or explicitly migrate it"
            )
        existing_scheduler = value.get("scheduler_policy_sha256")
        if (
            existing_scheduler
            and existing_scheduler != self._scheduler_signature()
        ):
            raise ValueError(
                "Existing Pons admission scheduler uses a different policy; "
                "choose a new root or explicitly migrate it"
            )
        migrated = int(value.get("schema_version") or 1) < self.SCHEMA_VERSION
        value = default | value
        value["schema_version"] = self.SCHEMA_VERSION
        value["scheduler_policy"] = asdict(self.scheduler_policy)
        value["scheduler_policy_sha256"] = self._scheduler_signature()
        for record in value.get("candidates", {}).values():
            record.setdefault("scheduler", {})
            for observation in record.get("observations") or []:
                status = self._legacy_analysis_status(observation)
                observation["analysis_status"] = status
                observation.setdefault("infrastructure_errors", [])
                if status == "infrastructure_indeterminate":
                    infrastructure_stops = [
                        item
                        for item in (observation.get("hard_stops") or [])
                        if _is_infrastructure_error(RuntimeError(item))
                    ]
                    observation["infrastructure_errors"] = list(dict.fromkeys(
                        list(observation["infrastructure_errors"])
                        + infrastructure_stops
                    ))
                    observation["hard_stops"] = [
                        item
                        for item in (observation.get("hard_stops") or [])
                        if item not in infrastructure_stops
                    ]
        if migrated:
            value.setdefault("migrations", []).append({
                "from": 1,
                "to": self.SCHEMA_VERSION,
                "migrated_at": _utc_now(),
                "reason": (
                    "separate infrastructure-indeterminate evidence and "
                    "initialize admission scheduler v2"
                ),
            })
            _atomic_json(self.state_path, value)
        return value

    def _save(self) -> None:
        _atomic_json(self.state_path, self.state)

    @staticmethod
    def _observation(
        candidate: dict,
        decision: dict,
        *,
        now: float,
        analysis_ring: int | None = None,
    ) -> dict:
        market = decision.get("market") or {}
        quote = market.get("executable_quote") or {}
        entry = quote.get("entry") or {}
        immediate_exit = quote.get("immediate_exit") or {}
        security = decision.get("security") or {}
        concentration = security.get("holder_concentration") or {}
        canonical_market = market.get("canonical_pool_market") or {}
        return {
            "observed_at": _iso_from_seconds(now),
            "observed_timestamp": float(now),
            "block_pin": int(decision.get("block_pin") or 0),
            # Carry the provenance this observation was derived from. Without
            # it an outcome built from this observation has no evidence
            # manifest, so it can be sealed and audited but never becomes
            # learning-eligible. Like the analysis binding, it cannot be added
            # afterwards -- a hash attached later proves nothing about what was
            # observed at this block.
            "provenance": decision.get("provenance") or {},
            "analysis_status": (
                decision.get("analysis_status")
                or (
                    "complete_safe"
                    if decision.get("paper_entry_allowed")
                    else "complete_unsafe"
                )
            ),
            "infrastructure_errors": list(
                decision.get("infrastructure_errors") or []
            ),
            "score": _safe_float(decision.get("score"), None),
            "risk_level": decision.get("risk_level"),
            "paper_entry_allowed": bool(
                decision.get("paper_entry_allowed")
            ),
            "hard_stops": list(decision.get("hard_stops") or []),
            "round_trip_loss_pct": _safe_float(
                quote.get("round_trip_loss_pct"), None
            ),
            "quote_deviation_pct": _safe_float(
                quote.get("quote_deviation_from_slot0_pct"), None
            ),
            "liquidity_usd": _safe_float(
                canonical_market.get("liquidity_usd"), None
            ),
            "largest_real_holder_pct": _safe_float(
                concentration.get("largest_real_holder_pct"), None
            ),
            "token_source_verified": bool(
                (security.get("token_source") or {}).get("is_verified")
            ),
            "entry_amount_in_raw": int(entry.get("amount_in_raw") or 0),
            "entry_amount_out_raw": int(entry.get("amount_out_raw") or 0),
            "immediate_exit_amount_out_raw": int(
                immediate_exit.get("amount_out_raw") or 0
            ),
            "analysis_ring": (
                analysis_ring
                if analysis_ring is not None
                else decision.get("timechain_ring")
            ),
            "cognitive_ring": decision.get("cognitive_ring"),
        }

    @staticmethod
    def evaluate_observations(
        observations: list[dict],
        policy: PonsAdmissionPolicy,
        *,
        first_seen_timestamp: float | None = None,
    ) -> dict:
        ordered = sorted(
            observations,
            key=lambda item: (
                float(item.get("observed_timestamp") or 0),
                int(item.get("block_pin") or 0),
            ),
        )
        complete = [
            item for item in ordered
            if PonsAdmissionQuarantine._legacy_analysis_status(item)
            != "infrastructure_indeterminate"
        ]
        indeterminate = [
            item for item in ordered
            if PonsAdmissionQuarantine._legacy_analysis_status(item)
            == "infrastructure_indeterminate"
        ]
        blockers: list[str] = []
        latest_observation = ordered[-1] if ordered else {}
        latest = complete[-1] if complete else latest_observation
        latest_is_indeterminate = bool(
            latest_observation
            and PonsAdmissionQuarantine._legacy_analysis_status(
                latest_observation
            ) == "infrastructure_indeterminate"
        )
        first_seen = (
            float(complete[0].get("observed_timestamp") or 0)
            if complete
            else float(first_seen_timestamp)
            if first_seen_timestamp is not None
            else float(
                (ordered[0] if ordered else {}).get(
                    "observed_timestamp", 0
                )
            )
        )
        age_seconds = (
            max(
                0.0,
                float(
                    latest_observation.get("observed_timestamp") or 0
                ) - first_seen,
            )
            if ordered else 0.0
        )
        if not complete:
            blockers.append("admission_evidence_indeterminate")
        if latest_is_indeterminate:
            blockers.append("admission_latest_evidence_indeterminate")
        if len(complete) < policy.minimum_observations:
            blockers.append(
                "admission_min_observations_"
                f"{len(complete)}_of_{policy.minimum_observations}"
            )
        if age_seconds < policy.minimum_age_seconds:
            blockers.append(
                "admission_min_age_"
                f"{int(age_seconds)}_of_{int(policy.minimum_age_seconds)}s"
            )
        recent = complete[-policy.minimum_observations:]
        if recent and any(
            not item.get("paper_entry_allowed") for item in recent
        ):
            blockers.append("admission_recent_risk_gate")
        round_trips = [
            _safe_float(item.get("round_trip_loss_pct"), None)
            for item in recent
        ]
        if recent and any(value is None for value in round_trips):
            blockers.append("admission_missing_executable_quote")
        if any(
            value is not None
            and value > policy.maximum_round_trip_loss_pct
            for value in round_trips
        ):
            blockers.append("admission_round_trip_limit")
        for before, after in zip(round_trips, round_trips[1:]):
            if (
                before is not None
                and after is not None
                and after - before
                > policy.maximum_round_trip_deterioration_pct
            ):
                blockers.append("admission_round_trip_deterioration")
                break
        for before, after in zip(recent, recent[1:]):
            spacing = (
                float(after.get("observed_timestamp") or 0)
                - float(before.get("observed_timestamp") or 0)
            )
            if spacing < policy.minimum_observation_spacing_seconds:
                blockers.append("admission_observations_too_close")
                break
        liquidity = _safe_float(latest.get("liquidity_usd"), None)
        if policy.minimum_liquidity_usd > 0 and (
            liquidity is None or liquidity < policy.minimum_liquidity_usd
        ):
            blockers.append("admission_liquidity_floor")
        for before, after in zip(recent, recent[1:]):
            before_liquidity = _safe_float(
                before.get("liquidity_usd"), None
            )
            after_liquidity = _safe_float(
                after.get("liquidity_usd"), None
            )
            if (
                before_liquidity is not None
                and after_liquidity is not None
                and before_liquidity > 0
                and (
                    (before_liquidity - after_liquidity)
                    / before_liquidity
                    * 100
                ) > policy.maximum_liquidity_drop_pct
            ):
                blockers.append("admission_liquidity_deterioration")
                break
        expired = age_seconds > policy.maximum_pending_hours * 3600
        if expired:
            blockers.append("admission_window_expired")
        blockers = list(dict.fromkeys(blockers))
        latest_hard_stops = list(latest.get("hard_stops") or [])
        terminal_hard_stop = any(
            marker in item
            for item in latest_hard_stops
            for marker in PonsAdmissionQuarantine.TERMINAL_HARD_STOP_MARKERS
        )
        disposition = (
            "terminal"
            if expired or terminal_hard_stop else
            "infrastructure_retry"
            if latest_is_indeterminate or not complete else
            "promising"
            if latest.get("paper_entry_allowed") and not any(
                item
                for item in blockers
                if not (
                    item.startswith("admission_min_observations_")
                    or item.startswith("admission_min_age_")
                    or item == "admission_observations_too_close"
                )
            ) else
            "unsafe_cooldown"
        )
        return {
            "allowed": not blockers,
            "blockers": blockers,
            "observation_count": len(ordered),
            "complete_observation_count": len(complete),
            "indeterminate_observation_count": len(indeterminate),
            "latest_analysis_status": (
                PonsAdmissionQuarantine._legacy_analysis_status(
                    latest_observation
                )
                if latest_observation else None
            ),
            "disposition": disposition,
            "age_seconds": age_seconds,
            "latest_block_pin": latest_observation.get("block_pin"),
            "latest_round_trip_loss_pct": latest.get(
                "round_trip_loss_pct"
            ),
            "latest_liquidity_usd": latest.get("liquidity_usd"),
            "policy": asdict(policy),
            "policy_sha256": hashlib.sha256(
                _canonical_json(asdict(policy)).encode("utf-8")
            ).hexdigest(),
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def _schedule_record(
        self,
        record: dict,
        evaluation: dict,
        *,
        now: float,
    ) -> None:
        scheduler = record.setdefault("scheduler", {})
        latest_status = evaluation.get("latest_analysis_status")
        consecutive = int(
            scheduler.get("consecutive_indeterminate") or 0
        )
        if latest_status == "infrastructure_indeterminate":
            consecutive += 1
        else:
            consecutive = 0
        scheduler["consecutive_indeterminate"] = consecutive
        scheduler.setdefault("selection_count", 0)
        scheduler.setdefault("last_selected_at", None)
        scheduler.setdefault("last_selected_timestamp", None)
        scheduler["disposition"] = evaluation.get("disposition")

        if record.get("status") == "admitted":
            scheduler["lane"] = "admitted"
            scheduler["priority_score"] = 0.0
            scheduler["next_refresh_at"] = None
            scheduler["next_refresh_timestamp"] = None
            return
        if evaluation.get("allowed"):
            record["status"] = "ready"
            scheduler["lane"] = "ready"
            scheduler["priority_score"] = 1_000.0
            next_timestamp = now
        elif evaluation.get("disposition") == "terminal":
            record["status"] = "terminal"
            scheduler["lane"] = "terminal"
            scheduler["priority_score"] = 0.0
            next_timestamp = None
        elif evaluation.get("disposition") == "infrastructure_retry":
            record["status"] = "cooldown"
            scheduler["lane"] = "infrastructure_retry"
            scheduler["priority_score"] = max(
                250.0, 450.0 - 20.0 * consecutive
            )
            delay = min(
                self.scheduler_policy.maximum_infrastructure_backoff_seconds,
                self.scheduler_policy.infrastructure_backoff_base_seconds
                * 2**max(0, consecutive - 1),
            )
            next_timestamp = now + delay
        elif evaluation.get("disposition") == "promising":
            record["status"] = "pending"
            scheduler["lane"] = "promising"
            scheduler["priority_score"] = (
                800.0
                + 25.0 * int(
                    evaluation.get("complete_observation_count") or 0
                )
            )
            complete = [
                item for item in (record.get("observations") or [])
                if self._legacy_analysis_status(item)
                != "infrastructure_indeterminate"
            ]
            first_complete = float(
                (complete[0] if complete else {}).get(
                    "observed_timestamp", now
                )
            )
            next_timestamp = max(
                now + self.scheduler_policy.clean_candidate_delay_seconds,
                first_complete + self.policy.minimum_age_seconds,
            )
        else:
            record["status"] = "cooldown"
            scheduler["lane"] = "unsafe_cooldown"
            scheduler["priority_score"] = 100.0
            next_timestamp = (
                now + self.scheduler_policy.unsafe_cooldown_seconds
            )
        scheduler["next_refresh_timestamp"] = next_timestamp
        scheduler["next_refresh_at"] = (
            _iso_from_seconds(next_timestamp)
            if next_timestamp is not None else None
        )

    def _reconcile_state(self) -> None:
        changed = False
        for record in self.state.get("candidates", {}).values():
            observations = record.get("observations") or []
            evaluation = self.evaluate_observations(
                observations,
                self.policy,
                first_seen_timestamp=record.get("first_seen_timestamp"),
            )
            before = _canonical_json({
                "status": record.get("status"),
                "scheduler": record.get("scheduler"),
                "latest_evaluation": record.get("latest_evaluation"),
            })
            record["latest_evaluation"] = evaluation
            self._schedule_record(
                record,
                evaluation,
                now=float(
                    record.get("last_seen_timestamp")
                    or record.get("first_seen_timestamp")
                    or time.time()
                ),
            )
            after = _canonical_json({
                "status": record.get("status"),
                "scheduler": record.get("scheduler"),
                "latest_evaluation": record.get("latest_evaluation"),
            })
            changed = changed or before != after
        if changed:
            self._save()

    def _record_mapping(
        self,
        candidate: dict,
        decision: dict,
        *,
        now: float,
        analysis_ring: int | None = None,
        persist: bool = True,
    ) -> dict:
        token = str(candidate.get("token_address") or "").lower()
        if not ADDRESS_RE.fullmatch(token):
            raise ValueError("invalid Pons admission token address")
        record = self.state["candidates"].setdefault(
            token,
            {
                "token_address": candidate.get("token_address"),
                "symbol": candidate.get("symbol") or "???",
                "launch_id": candidate.get("launch_id"),
                "launch_block": candidate.get("launch_block"),
                "first_seen_at": _iso_from_seconds(now),
                "first_seen_timestamp": float(now),
                "last_seen_at": None,
                "last_seen_timestamp": None,
                "status": "pending",
                "admitted_simulations": [],
                "observations": [],
            },
        )
        if float(record.get("first_seen_timestamp") or now) > now:
            record["first_seen_timestamp"] = float(now)
            record["first_seen_at"] = _iso_from_seconds(now)
        observation = self._observation(
            candidate,
            decision,
            now=now,
            analysis_ring=analysis_ring,
        )
        duplicate = next(
            (
                item
                for item in record["observations"]
                if observation["block_pin"] > 0
                and int(item.get("block_pin") or 0)
                == observation["block_pin"]
            ),
            None,
        )
        if duplicate is None:
            record["observations"].append(observation)
        record["observations"].sort(
            key=lambda item: (
                float(item.get("observed_timestamp") or 0),
                int(item.get("block_pin") or 0),
            )
        )
        record["last_seen_at"] = observation["observed_at"]
        record["last_seen_timestamp"] = observation["observed_timestamp"]
        evaluation = self.evaluate_observations(
            record["observations"],
            self.policy,
            first_seen_timestamp=record["first_seen_timestamp"],
        )
        record["latest_evaluation"] = evaluation
        self._schedule_record(record, evaluation, now=now)
        if persist:
            self._save()
        return {
            "token_address": record["token_address"],
            "symbol": record["symbol"],
            "status": record["status"],
            "scheduler": dict(record.get("scheduler") or {}),
            **evaluation,
        }

    def record(
        self,
        candidate: PonsLaunchCandidate,
        decision: PonsRiskDecision,
        *,
        now: float | None = None,
    ) -> dict:
        return self._record_mapping(
            candidate.to_dict(),
            decision.to_dict(),
            now=time.time() if now is None else float(now),
            analysis_ring=decision.timechain_ring,
        )

    def backfill_timechain(self, rings: Iterable[dict]) -> int:
        added = 0
        before = sum(
            len(item.get("observations") or [])
            for item in self.state["candidates"].values()
        )
        for ring in rings:
            if ring.get("ring_type") != "pons_launch_analysis":
                continue
            payload = ring.get("payload") or {}
            candidate = payload.get("candidate") or {}
            decision = payload.get("decision") or {}
            if not candidate.get("token_address") or not decision:
                continue
            try:
                observed = datetime.fromisoformat(
                    str(ring.get("timestamp")).replace("Z", "+00:00")
                ).timestamp()
                self._record_mapping(
                    candidate,
                    decision,
                    now=observed,
                    analysis_ring=ring.get("index"),
                    persist=False,
                )
            except (TypeError, ValueError):
                continue
        after = sum(
            len(item.get("observations") or [])
            for item in self.state["candidates"].values()
        )
        added = max(0, after - before)
        if added:
            self._save()
        return added

    def mark_admitted(self, token: str, simulation: str) -> None:
        record = self.state["candidates"].get(token.lower())
        if not record:
            return
        simulations = record.setdefault("admitted_simulations", [])
        changed = False
        if simulation not in simulations:
            simulations.append(simulation)
            changed = True
        if record.get("status") != "admitted":
            record["status"] = "admitted"
            record.setdefault("admitted_at", _utc_now())
            changed = True
        scheduler = record.setdefault("scheduler", {})
        if (
            scheduler.get("lane") != "admitted"
            or scheduler.get("next_refresh_timestamp") is not None
        ):
            scheduler.update({
                "lane": "admitted",
                "priority_score": 0.0,
                "next_refresh_at": None,
                "next_refresh_timestamp": None,
            })
            changed = True
        if changed:
            self._save()

    def filter_discovery_candidates(
        self,
        candidates: Iterable[PonsLaunchCandidate],
        *,
        now: float | None = None,
    ) -> tuple[list[PonsLaunchCandidate], int]:
        """Admit new launches and due refreshes; suppress cooldown bypasses."""
        now = time.time() if now is None else float(now)
        selected = []
        skipped = 0
        changed = False
        for candidate in candidates:
            record = self.state["candidates"].get(
                candidate.token_address.lower()
            )
            if record is None:
                selected.append(candidate)
                continue
            first_seen = _safe_float(
                record.get("first_seen_timestamp"), None
            )
            if (
                record.get("status") != "admitted"
                and first_seen is not None
                and now - first_seen
                > self.policy.maximum_pending_hours * 3600
            ):
                record["status"] = "terminal"
                record.setdefault("scheduler", {}).update({
                    "lane": "terminal",
                    "disposition": "terminal",
                    "priority_score": 0.0,
                    "next_refresh_at": None,
                    "next_refresh_timestamp": None,
                })
                changed = True
            scheduler = record.get("scheduler") or {}
            next_refresh = _safe_float(
                scheduler.get("next_refresh_timestamp"), None
            )
            if (
                record.get("status") in {"admitted", "terminal"}
                or (
                    next_refresh is not None
                    and next_refresh > now
                )
            ):
                skipped += 1
                continue
            selected.append(candidate)
        if changed:
            self._save()
        return selected, skipped

    def refresh_plan(
        self,
        *,
        limit: int,
        exclude: Iterable[str] = (),
        now: float | None = None,
    ) -> list[dict]:
        """Rank due candidates by evidence value, not simply oldest mark."""
        now = time.time() if now is None else float(now)
        excluded = {str(item).lower() for item in exclude}
        records = []
        changed = False
        for item in self.state["candidates"].values():
            token = str(item.get("token_address") or "").lower()
            if token in excluded or item.get("status") in {
                "admitted",
                "terminal",
            }:
                continue
            first_seen = _safe_float(
                item.get("first_seen_timestamp"), None
            )
            if (
                first_seen is not None
                and now - first_seen
                > self.policy.maximum_pending_hours * 3600
            ):
                item["status"] = "terminal"
                item.setdefault("latest_evaluation", {}).setdefault(
                    "blockers", []
                )
                blockers = item["latest_evaluation"]["blockers"]
                if "admission_window_expired" not in blockers:
                    blockers.append("admission_window_expired")
                scheduler = item.setdefault("scheduler", {})
                scheduler.update({
                    "lane": "terminal",
                    "disposition": "terminal",
                    "priority_score": 0.0,
                    "next_refresh_at": None,
                    "next_refresh_timestamp": None,
                })
                changed = True
                continue
            scheduler = item.get("scheduler") or {}
            next_refresh = _safe_float(
                scheduler.get("next_refresh_timestamp"), None
            )
            if next_refresh is not None and next_refresh > now:
                continue
            records.append(item)

        records.sort(key=lambda item: (
            -_safe_float(
                (item.get("scheduler") or {}).get("priority_score")
            ),
            int(
                (item.get("scheduler") or {}).get("selection_count")
                or 0
            ),
            _safe_float(
                (item.get("scheduler") or {}).get(
                    "next_refresh_timestamp"
                )
            ),
            _safe_float(item.get("last_seen_timestamp")),
        ))
        selected = records[:max(0, int(limit))]
        selected_tokens = []
        for item in selected:
            scheduler = item.setdefault("scheduler", {})
            scheduler["selection_count"] = (
                int(scheduler.get("selection_count") or 0) + 1
            )
            scheduler["last_selected_timestamp"] = now
            scheduler["last_selected_at"] = _iso_from_seconds(now)
            selected_tokens.append(str(item.get("token_address")))
            changed = True
        self.state["scheduler"] = {
            "last_planned_at": _iso_from_seconds(now),
            "selected": selected_tokens,
            "due": len(records),
            "deferred": max(0, len(records) - len(selected)),
        }
        if changed or selected:
            self._save()
        return selected

    def refresh_candidates(
        self,
        observer: PonsObserver,
        *,
        limit: int,
        exclude: Iterable[str] = (),
        now: float | None = None,
    ) -> list[PonsLaunchCandidate]:
        values = []
        for record in self.refresh_plan(
            limit=limit,
            exclude=exclude,
            now=now,
        ):
            candidate = observer.by_token(record["token_address"])
            if candidate is not None:
                values.append(candidate)
        return values

    def summary(self) -> dict:
        counts = {
            "pending": 0,
            "ready": 0,
            "cooldown": 0,
            "terminal": 0,
            "admitted": 0,
        }
        records = []
        complete_observations = 0
        indeterminate_observations = 0
        for item in self.state["candidates"].values():
            status = item.get("status", "pending")
            counts[status] = counts.get(status, 0) + 1
            evaluation = item.get("latest_evaluation") or {}
            complete_observations += int(
                evaluation.get("complete_observation_count") or 0
            )
            indeterminate_observations += int(
                evaluation.get("indeterminate_observation_count") or 0
            )
            records.append({
                "token_address": item.get("token_address"),
                "symbol": item.get("symbol"),
                "status": status,
                "observation_count": len(item.get("observations") or []),
                "complete_observation_count": evaluation.get(
                    "complete_observation_count"
                ),
                "indeterminate_observation_count": evaluation.get(
                    "indeterminate_observation_count"
                ),
                "analysis_status": evaluation.get(
                    "latest_analysis_status"
                ),
                "scheduler": dict(item.get("scheduler") or {}),
                "age_seconds": evaluation.get("age_seconds"),
                "blockers": evaluation.get("blockers") or [],
                "round_trip_loss_pct": evaluation.get(
                    "latest_round_trip_loss_pct"
                ),
                "liquidity_usd": evaluation.get("latest_liquidity_usd"),
                "last_seen_at": item.get("last_seen_at"),
            })
        records.sort(
            key=lambda item: item.get("last_seen_at") or "", reverse=True
        )
        return {
            **counts,
            "total": len(records),
            "complete_observations": complete_observations,
            "indeterminate_observations": indeterminate_observations,
            "policy": asdict(self.policy),
            "policy_sha256": self._policy_signature(),
            "scheduler_policy": asdict(self.scheduler_policy),
            "scheduler_policy_sha256": self._scheduler_signature(),
            "scheduler": dict(self.state.get("scheduler") or {}),
            "recent": records[:20],
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def verify(self) -> tuple[bool, str]:
        if self.state.get("schema_version") != self.SCHEMA_VERSION:
            return False, "admission schema version mismatch"
        if self.state.get("policy_sha256") != self._policy_signature():
            return False, "admission policy signature mismatch"
        if (
            self.state.get("scheduler_policy_sha256")
            != self._scheduler_signature()
        ):
            return False, "admission scheduler policy signature mismatch"
        if not self.state.get("paper_only"):
            return False, "admission state lost paper-only boundary"
        if self.state.get("live_execution_enabled"):
            return False, "admission state claims live execution"
        for token, record in self.state.get("candidates", {}).items():
            if token != str(record.get("token_address") or "").lower():
                return False, f"admission token key mismatch: {token}"
            blocks = [
                int(item.get("block_pin") or 0)
                for item in (record.get("observations") or [])
                if int(item.get("block_pin") or 0) > 0
            ]
            if len(blocks) != len(set(blocks)):
                return False, f"duplicate admission block pin: {token}"
            for observation in record.get("observations") or []:
                if self._legacy_analysis_status(observation) not in {
                    "complete_safe",
                    "complete_unsafe",
                    "infrastructure_indeterminate",
                }:
                    return False, f"invalid evidence status: {token}"
            if record.get("status") not in {
                "pending",
                "ready",
                "cooldown",
                "terminal",
                "admitted",
            }:
                return False, f"invalid admission status: {token}"
        return (
            True,
            f"verified {len(self.state.get('candidates', {}))} "
            "admission candidates",
        )


class PonsCounterfactualPolicyLearner:
    """Predeclared policy grid; recommendations never change live policy."""

    MINIMUM_CLOSED_FOR_RECOMMENDATION = 30
    MINIMUM_VALIDATION_CLOSED = 10
    MINIMUM_VALIDATION_WINNERS = 3
    MAXIMUM_BEST_WINNER_PROFIT_SHARE_PCT = 60.0
    MINIMUM_CONTROL_RETURN_DELTA_PCT = 10.0

    def __init__(
        self,
        state_path: str | Path,
        paper_policy: PonsPaperPolicy,
        admission_policy: PonsAdmissionPolicy,
    ):
        self.state_path = Path(state_path)
        self.paper_policy = paper_policy
        self.admission_policy = admission_policy
        self.variants = {
            "immediate_control_v1": PonsAdmissionPolicy(
                minimum_observations=1,
                minimum_age_seconds=0,
                minimum_observation_spacing_seconds=0,
                maximum_round_trip_loss_pct=15,
                maximum_round_trip_deterioration_pct=100,
                maximum_liquidity_drop_pct=100,
                minimum_liquidity_usd=0,
            ),
            "stability_v1": admission_policy,
            "stability_liquidity_3000_v1": PonsAdmissionPolicy(
                **(asdict(admission_policy) | {
                    "minimum_liquidity_usd": 3_000.0
                })
            ),
            "stability_liquidity_5000_v1": PonsAdmissionPolicy(
                **(asdict(admission_policy) | {
                    "minimum_liquidity_usd": 5_000.0
                })
            ),
        }

    def policy_grid_signature(self) -> str:
        return hashlib.sha256(
            _canonical_json({
                name: asdict(policy)
                for name, policy in self.variants.items()
            }).encode("utf-8")
        ).hexdigest()

    def verify(self, snapshot: dict) -> tuple[bool, str]:
        if not snapshot:
            return True, "policy learning has not produced a snapshot"
        if snapshot.get("schema_version") not in {1, 2}:
            return False, "counterfactual policy schema mismatch"
        if snapshot.get("policy_grid_sha256") != self.policy_grid_signature():
            return False, "counterfactual policy grid signature mismatch"
        if not snapshot.get("paper_only"):
            return False, "policy learner lost paper-only boundary"
        if snapshot.get("live_execution_enabled"):
            return False, "policy learner claims live execution"
        recommendation = snapshot.get("recommendation") or {}
        if recommendation.get("auto_adopted"):
            return False, "policy learner attempted automatic adoption"
        if not recommendation.get("requires_human_approval"):
            return False, "policy learner removed human approval"
        return (
            True,
            f"verified {len(snapshot.get('cohorts') or {})} "
            "counterfactual cohorts",
        )

    def _modeled_outcome(
        self,
        observations: list[dict],
        entry_index: int,
    ) -> dict | None:
        entry = observations[entry_index]
        entry_in = int(entry.get("entry_amount_in_raw") or 0)
        entry_tokens = int(entry.get("entry_amount_out_raw") or 0)
        if entry_in <= 0 or entry_tokens <= 0:
            return None
        later = observations[entry_index + 1:]
        close_mark = next(
            (
                item for item in later
                if not item.get("paper_entry_allowed")
                or item.get("hard_stops")
            ),
            None,
        )
        mark = close_mark or observations[-1]
        mark_tokens = int(mark.get("entry_amount_out_raw") or 0)
        mark_exit = int(mark.get("immediate_exit_amount_out_raw") or 0)
        if mark_tokens <= 0 or mark_exit <= 0:
            return None
        retained = 1 - self.paper_policy.assumed_slippage_bps / 10_000
        acquired_tokens = entry_tokens * retained
        gross_exit_eth = (
            acquired_tokens * (mark_exit / mark_tokens) / 10**18
        )
        value_eth = max(
            0.0,
            gross_exit_eth * retained - self.paper_policy.modeled_gas_eth,
        )
        cost_eth = entry_in / 10**18 + self.paper_policy.modeled_gas_eth
        multiple = value_eth / cost_eth if cost_eth > 0 else None
        return {
            "entry_timestamp": entry.get("observed_timestamp"),
            "mark_timestamp": mark.get("observed_timestamp"),
            "cost_eth": cost_eth,
            "value_eth": value_eth,
            "multiple": multiple,
            "closed": close_mark is not None,
            "close_reason": (
                "risk_signal" if close_mark is not None else None
            ),
            "evidence": "quote_ratio_counterfactual_not_executable_fill",
        }

    @staticmethod
    def _aggregate(outcomes: list[dict]) -> dict:
        priced = [item for item in outcomes if item.get("multiple") is not None]
        distribution = _performance_distribution(priced)
        return {
            "admitted": len(outcomes),
            "priced": len(priced),
            "closed": sum(bool(item.get("closed")) for item in priced),
            "worst_multiple": (
                min(float(item["multiple"]) for item in priced)
                if priced else None
            ),
            **distribution,
        }

    @classmethod
    def _promotion_blockers(
        cls,
        overall: dict,
        validation: dict,
        control_return: float | None,
    ) -> list[str]:
        blockers = []
        if (
            int(overall.get("closed") or 0)
            < cls.MINIMUM_CLOSED_FOR_RECOMMENDATION
        ):
            blockers.append("overall_closed_evidence_floor")
        if (
            int(validation.get("closed") or 0)
            < cls.MINIMUM_VALIDATION_CLOSED
        ):
            blockers.append("validation_closed_evidence_floor")
        if (
            int(validation.get("profitable") or 0)
            < cls.MINIMUM_VALIDATION_WINNERS
        ):
            blockers.append("validation_profitable_breadth")
        variant_return = _safe_float(
            validation.get("modeled_return_pct"), None
        )
        if variant_return is None or variant_return <= 0:
            blockers.append("validation_return_non_positive")
        robust_return = _safe_float(
            validation.get("return_without_best_pct"), None
        )
        if robust_return is None or robust_return <= 0:
            blockers.append("validation_return_without_best_non_positive")
        concentration = _safe_float(
            validation.get("best_positive_profit_share_pct"), None
        )
        if (
            concentration is None
            or concentration
            > cls.MAXIMUM_BEST_WINNER_PROFIT_SHARE_PCT
        ):
            blockers.append("validation_profit_concentration")
        if (
            control_return is None
            or variant_return is None
            or variant_return
            < control_return + cls.MINIMUM_CONTROL_RETURN_DELTA_PCT
        ):
            blockers.append("validation_control_delta")
        return blockers

    def evaluate(self, quarantine: PonsAdmissionQuarantine) -> dict:
        by_variant: dict[str, list[dict]] = {
            name: [] for name in self.variants
        }
        tokens_by_variant: dict[str, dict[str, dict]] = {
            name: {} for name in self.variants
        }
        for token, record in quarantine.state["candidates"].items():
            observations = sorted(
                [
                    item
                    for item in (record.get("observations") or [])
                    if quarantine._legacy_analysis_status(item)
                    != "infrastructure_indeterminate"
                ],
                key=lambda item: float(
                    item.get("observed_timestamp") or 0
                ),
            )
            if not observations:
                continue
            for name, policy in self.variants.items():
                entry_index = None
                for index in range(len(observations)):
                    evaluation = (
                        PonsAdmissionQuarantine.evaluate_observations(
                            observations[:index + 1],
                            policy,
                            first_seen_timestamp=record.get(
                                "first_seen_timestamp"
                            ),
                        )
                    )
                    if evaluation["allowed"]:
                        entry_index = index
                        break
                if entry_index is None:
                    continue
                outcome = self._modeled_outcome(observations, entry_index)
                if outcome is None:
                    continue
                outcome = {
                    "token_address": token,
                    "symbol": record.get("symbol"),
                    **outcome,
                }
                by_variant[name].append(outcome)
                tokens_by_variant[name][token] = outcome

        control = tokens_by_variant["immediate_control_v1"]
        cohorts = {}
        for name, outcomes in by_variant.items():
            outcomes.sort(
                key=lambda item: float(item.get("entry_timestamp") or 0)
            )
            split = max(0, math.floor(len(outcomes) * 0.7))
            validation = outcomes[split:]
            admitted_tokens = tokens_by_variant[name]
            cohorts[name] = {
                "policy": asdict(self.variants[name]),
                "overall": self._aggregate(outcomes),
                "validation": self._aggregate(validation),
                "avoided_control_losses": sum(
                    1
                    for token, item in control.items()
                    if token not in admitted_tokens
                    and item.get("multiple") is not None
                    and float(item["multiple"]) < 1
                ),
                "missed_control_winners": sum(
                    1
                    for token, item in control.items()
                    if token not in admitted_tokens
                    and item.get("multiple") is not None
                    and float(item["multiple"]) >= 1
                ),
            }

        control_return = (
            cohorts["immediate_control_v1"]["validation"].get(
                "modeled_return_pct"
            )
        )
        candidates = []
        for name, cohort in cohorts.items():
            if name == "immediate_control_v1":
                continue
            overall = cohort["overall"]
            validation = cohort["validation"]
            blockers = self._promotion_blockers(
                overall, validation, control_return
            )
            cohort["promotion_blockers"] = blockers
            if not blockers:
                candidates.append((
                    float(validation["return_without_best_pct"]),
                    float(validation["modeled_return_pct"]),
                    name,
                ))
        candidates.sort(reverse=True)
        recommendation = {
            "status": (
                "human_review_required" if candidates
                else "insufficient_out_of_sample_evidence"
            ),
            "candidate_policy": candidates[0][2] if candidates else None,
            "minimum_closed_positions": (
                self.MINIMUM_CLOSED_FOR_RECOMMENDATION
            ),
            "minimum_validation_closed": self.MINIMUM_VALIDATION_CLOSED,
            "minimum_validation_winners": (
                self.MINIMUM_VALIDATION_WINNERS
            ),
            "maximum_best_winner_profit_share_pct": (
                self.MAXIMUM_BEST_WINNER_PROFIT_SHARE_PCT
            ),
            "requires_positive_return_without_best": True,
            "requires_human_approval": True,
            "auto_adopted": False,
            "reason": (
                "A predeclared variant passed the bounded walk-forward, "
                "profitable-breadth, and concentration-resistance gates."
                if candidates else
                "No policy can be promoted until closed, validation, "
                "profitable-breadth, and concentration-resistance gates "
                "are all met."
            ),
        }
        snapshot = {
            "schema_version": 2,
            "protocol": "pons",
            "generated_at": _utc_now(),
            "evaluation_mode": "predeclared_policy_grid_walk_forward",
            "valuation_evidence": (
                "quote_ratio_counterfactual_not_executable_fill"
            ),
            "policy_grid_sha256": self.policy_grid_signature(),
            "cohorts": cohorts,
            "recommendation": recommendation,
            "paper_only": True,
            "live_execution_enabled": False,
        }
        _atomic_json(self.state_path, snapshot)
        return snapshot


class PonsPaperTrader:
    """ETH-denominated paper execution using exact deployed-Quoter outputs."""

    def __init__(
        self,
        state_path: str | Path,
        ledger: PaperTradeLedger,
        policy: PonsPaperPolicy | None = None,
        *,
        event_namespace: str = "paper",
        enforce_position_limit: bool = True,
    ):
        if event_namespace not in {"paper", "shadow"}:
            raise ValueError("event_namespace must be 'paper' or 'shadow'")
        self.state_path = Path(state_path)
        self.ledger = ledger
        self.policy = policy or PonsPaperPolicy()
        self.event_namespace = event_namespace
        self.enforce_position_limit = enforce_position_limit
        self.state = self._load_state()

    def _policy_signature(self) -> str:
        return hashlib.sha256(
            _canonical_json(asdict(self.policy)).encode("utf-8")
        ).hexdigest()

    def _load_state(self) -> dict:
        default = {
            "schema_version": 1,
            "protocol": "pons",
            "chain_id": PONS_CHAIN_ID,
            "denomination": "ETH",
            "positions": {},
            "realized_eth": 0.0,
            "paper_only": True,
            "live_execution_enabled": False,
            "policy_sha256": self._policy_signature(),
        }
        value = _read_json(self.state_path, default)
        existing = value.get("policy_sha256")
        if existing and existing != self._policy_signature():
            raise ValueError(
                "Existing Pons cohort uses a different paper policy; choose a new root"
            )
        return default | value

    def _save(self) -> None:
        _atomic_json(self.state_path, self.state)

    @property
    def open_positions(self) -> list[dict]:
        return [
            value for value in self.state["positions"].values()
            if value.get("status") == "open"
        ]

    def entry_blockers(
        self,
        candidate: PonsLaunchCandidate,
        decision: PonsRiskDecision,
    ) -> list[str]:
        blockers: list[str] = []
        if decision.analysis_status == "infrastructure_indeterminate":
            blockers.append("evidence_indeterminate")
        if (
            not decision.paper_entry_allowed
            or decision.score < self.policy.minimum_score
        ):
            blockers.append("risk_gate")
        if candidate.token_address.lower() in self.state["positions"]:
            blockers.append("position_already_recorded")
        if (
            self.enforce_position_limit
            and len(self.open_positions) >= self.policy.maximum_positions
        ):
            blockers.append("position_limit")
        first_safe_block = candidate.launch_block + self.policy.observation_blocks
        if decision.block_pin < first_safe_block:
            blockers.append(f"launch_protection_wait_until_block_{first_safe_block}")
        restrictions = decision.security.get("restriction_limits") or {}
        if (
            restrictions.get("active")
            and not restrictions.get("eligible_for_modeled_buy")
        ):
            blockers.append("onchain_launch_limit")
        quote = decision.market.get("executable_quote") or {}
        entry = quote.get("entry") or {}
        if int(entry.get("amount_out_raw") or 0) <= 0:
            blockers.append("missing_executable_entry_quote")
        return blockers

    def enter(
        self,
        candidate: PonsLaunchCandidate,
        decision: PonsRiskDecision,
        *,
        now: float | None = None,
    ) -> dict | None:
        if decision.live_entry_allowed:
            raise LiveExecutionDisabledError(
                "Pons adapter cannot authorize live entry"
            )
        blockers = self.entry_blockers(candidate, decision)
        if blockers:
            return None
        now = time.time() if now is None else float(now)
        entry = decision.market["executable_quote"]["entry"]
        gross_quantity_raw = int(entry["amount_out_raw"])
        retained = 1 - self.policy.assumed_slippage_bps / 10_000
        quantity_raw = max(0, math.floor(gross_quantity_raw * retained))
        if quantity_raw <= 0:
            return None
        amount_eth = int(entry["amount_in_raw"]) / 10**18
        cost_basis_eth = amount_eth + self.policy.modeled_gas_eth
        immediate_exit = (
            decision.market.get("executable_quote") or {}
        ).get("immediate_exit") or {}
        immediate_exit_raw = int(
            immediate_exit.get("amount_out_raw") or 0
        )
        immediate_exit_eth = (
            max(
                0.0,
                immediate_exit_raw
                / 10**18
                * (1 - self.policy.assumed_slippage_bps / 10_000)
                - self.policy.modeled_gas_eth,
            )
            if immediate_exit_raw > 0 else None
        )
        initial_multiple = (
            immediate_exit_eth / cost_basis_eth
            if immediate_exit_eth is not None and cost_basis_eth > 0
            else None
        )
        position = {
            "token_address": candidate.token_address,
            "launch_id": candidate.launch_id,
            "name": candidate.name,
            "symbol": candidate.symbol,
            "pool_address": candidate.pool_address,
            "factory_address": candidate.factory_address,
            "position_nft_id": candidate.position_id,
            "status": "open",
            "entry_timestamp": now,
            "entry_block": decision.block_pin,
            "initial_quantity_raw": quantity_raw,
            "remaining_quantity_raw": quantity_raw,
            "entry_amount_eth": amount_eth,
            "cost_basis_eth": cost_basis_eth,
            "realized_eth": 0.0,
            "realized_cost_basis_eth": 0.0,
            "realized_pnl_eth": 0.0,
            "high_multiple": 1.0,
            "tiers_filled": [],
            "analysis_score": decision.score,
            "analysis_risk_level": decision.risk_level,
            "analysis_ring": decision.timechain_ring,
            "cognitive_ring": decision.cognitive_ring,
            "entry_quote": entry,
            "entry_slippage_haircut_bps": self.policy.assumed_slippage_bps,
            "last_mark_timestamp": now,
            "last_mark_block": decision.block_pin,
            "last_modeled_exit_eth": immediate_exit_eth,
            "last_total_multiple": initial_multiple,
            "simulation": self.event_namespace,
            "paper_only": True,
            "live_execution_enabled": False,
        }
        self.state["positions"][candidate.token_address.lower()] = position
        event = self.ledger.append(
            f"pons_{self.event_namespace}_buy", dict(position)
        )
        position["entry_event_hash"] = event["event_hash"]
        self._save()
        return position

    def _effective_proceeds_eth(self, quote: PonsQuote) -> float:
        gross = quote.amount_out_raw / 10**18
        retained = 1 - self.policy.assumed_slippage_bps / 10_000
        return max(0.0, gross * retained - self.policy.modeled_gas_eth)

    def _sell(
        self,
        position: dict,
        quantity_raw: int,
        quote: PonsQuote,
        reason: str,
        now: float,
    ) -> dict | None:
        quantity_raw = min(
            int(position["remaining_quantity_raw"]), max(0, int(quantity_raw))
        )
        if quantity_raw <= 0:
            return None
        if quote.amount_in_raw != quantity_raw:
            raise ValueError("sell quote amount does not match requested quantity")
        proceeds_eth = self._effective_proceeds_eth(quote)
        initial_quantity_raw = max(
            1, int(position.get("initial_quantity_raw") or quantity_raw)
        )
        allocated_cost_basis_eth = (
            float(position["cost_basis_eth"])
            * quantity_raw
            / initial_quantity_raw
        )
        realized_pnl_eth = proceeds_eth - allocated_cost_basis_eth
        position["remaining_quantity_raw"] -= quantity_raw
        position["realized_eth"] += proceeds_eth
        position["realized_cost_basis_eth"] = (
            float(position.get("realized_cost_basis_eth") or 0.0)
            + allocated_cost_basis_eth
        )
        position["realized_pnl_eth"] = (
            float(position.get("realized_pnl_eth") or 0.0)
            + realized_pnl_eth
        )
        self.state["realized_eth"] += proceeds_eth
        if position["remaining_quantity_raw"] <= 0:
            position["remaining_quantity_raw"] = 0
            position["status"] = "closed"
            position["closed_at"] = now
            position["close_reason"] = reason
        event = self.ledger.append(
            f"pons_{self.event_namespace}_sell",
            {
                "token_address": position["token_address"],
                "quantity_raw": quantity_raw,
                "quote": quote.to_dict(),
                "proceeds_eth_after_slippage_and_gas": proceeds_eth,
                "allocated_cost_basis_eth": allocated_cost_basis_eth,
                "realized_pnl_eth": realized_pnl_eth,
                "reason": reason,
                "remaining_quantity_raw": position["remaining_quantity_raw"],
                "simulation": self.event_namespace,
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        self._save()
        return event

    def mark(
        self,
        token_address: str,
        decision: PonsRiskDecision,
        quote_sell: Callable[[int], PonsQuote],
        *,
        now: float | None = None,
    ) -> list[dict]:
        position = self.state["positions"].get(token_address.lower())
        if not position or position.get("status") != "open":
            return []
        now = time.time() if now is None else float(now)
        if decision.analysis_status == "infrastructure_indeterminate":
            position["last_indeterminate_analysis_at"] = now
            position["last_indeterminate_analysis_block"] = decision.block_pin
            position["last_infrastructure_errors"] = list(
                decision.infrastructure_errors
            )
            self._save()
            return []
        remaining_raw = int(position["remaining_quantity_raw"])
        if remaining_raw <= 0:
            return []
        try:
            full_quote = quote_sell(remaining_raw)
        except Exception as exc:
            position["last_quote_error"] = str(exc)
            position["last_quote_attempt_timestamp"] = now
            position["last_quote_attempt_block"] = decision.block_pin
            self._save()
            return []
        return self._mark_from_quote(
            position,
            full_quote,
            quote_sell,
            now=now,
            block_pin=decision.block_pin,
            risk_level=decision.risk_level,
            score=decision.score,
            hard_stops=decision.hard_stops,
            allow_risk_signal=True,
        )

    def mark_quote_guard(
        self,
        token_address: str,
        quote_sell: Callable[[int], PonsQuote],
        *,
        block_pin: int,
        now: float | None = None,
    ) -> list[dict]:
        """Price an open position without rerunning the full token analysis.

        The guard is deliberately narrower than ``mark``: it can enforce
        executable-quote stop loss, trailing exit, take-profit, and maximum
        hold rules, but it cannot invent a token-risk signal. Full canonical
        analysis remains the authority for suspicious-contract exits.
        """
        position = self.state["positions"].get(token_address.lower())
        if not position or position.get("status") != "open":
            return []
        now = time.time() if now is None else float(now)
        remaining_raw = int(position["remaining_quantity_raw"])
        if remaining_raw <= 0:
            return []
        try:
            full_quote = quote_sell(remaining_raw)
        except Exception as exc:
            position["last_quote_error"] = str(exc)
            position["last_quote_attempt_timestamp"] = now
            position["last_quote_attempt_block"] = int(block_pin)
            self._save()
            return []
        return self._mark_from_quote(
            position,
            full_quote,
            quote_sell,
            now=now,
            block_pin=int(block_pin),
            allow_risk_signal=False,
        )

    def _mark_from_quote(
        self,
        position: dict,
        full_quote: PonsQuote,
        quote_sell: Callable[[int], PonsQuote],
        *,
        now: float,
        block_pin: int,
        risk_level: str | None = None,
        score: float | None = None,
        hard_stops: Iterable[str] = (),
        allow_risk_signal: bool,
    ) -> list[dict]:
        remaining_raw = int(position["remaining_quantity_raw"])
        if full_quote.amount_in_raw != remaining_raw:
            raise ValueError(
                "full-position mark quote amount does not match remaining quantity"
            )
        open_value_eth = self._effective_proceeds_eth(full_quote)
        total_value_eth = float(position["realized_eth"]) + open_value_eth
        multiple = total_value_eth / float(position["cost_basis_eth"])
        position["high_multiple"] = max(
            float(position.get("high_multiple") or 1.0), multiple
        )
        position["last_mark_timestamp"] = now
        position["last_mark_block"] = int(block_pin)
        position["last_quote_attempt_timestamp"] = now
        position["last_quote_attempt_block"] = int(block_pin)
        position.pop("last_quote_error", None)
        position["last_exit_quote"] = full_quote.to_dict()
        position["last_modeled_exit_eth"] = open_value_eth
        position["last_total_multiple"] = multiple
        if score is not None:
            position["last_mark_score"] = score
        if risk_level is not None:
            position["last_mark_risk_level"] = risk_level
        hard_stops = list(hard_stops)
        if allow_risk_signal:
            position["last_mark_hard_stops"] = hard_stops

        suspicious = allow_risk_signal and bool(
            hard_stops or risk_level in {"High", "Critical"}
        )
        expired = (
            now - float(position["entry_timestamp"])
            >= self.policy.maximum_hold_hours * 3600
        )
        trailed = (
            position["high_multiple"]
            >= self.policy.trailing_activation_multiple
            and multiple
            <= position["high_multiple"] * (1 - self.policy.trailing_drawdown)
        )
        if (
            suspicious
            or multiple <= self.policy.stop_loss_multiple
            or expired
            or trailed
        ):
            reason = (
                "risk_signal" if suspicious
                else "stop_loss" if multiple <= self.policy.stop_loss_multiple
                else "maximum_hold" if expired
                else "trailing_exit"
            )
            event = self._sell(
                position, remaining_raw, full_quote, reason, now
            )
            return [event] if event else []

        events: list[dict] = []
        initial_raw = int(position["initial_quantity_raw"])
        for target, fraction in self.policy.take_profit_tiers:
            tier_key = str(target)
            if (
                multiple >= target
                and tier_key not in position["tiers_filled"]
                and position["status"] == "open"
            ):
                quantity_raw = min(
                    int(position["remaining_quantity_raw"]),
                    max(1, math.floor(initial_raw * fraction)),
                )
                quote = (
                    full_quote if quantity_raw == remaining_raw
                    else quote_sell(quantity_raw)
                )
                event = self._sell(
                    position, quantity_raw, quote,
                    f"take_profit_{target:g}x", now,
                )
                position["tiers_filled"].append(tier_key)
                if event:
                    events.append(event)
        self._save()
        return events

    def broadcast_live_trade(self, *_args, **_kwargs):
        raise LiveExecutionDisabledError(
            "Live signing and broadcast do not exist in the Pons adapter"
        )


class PonsManagedPortfolioController:
    """Fail-closed portfolio gate for the bounded managed-paper cohort."""

    SCHEMA_VERSION = 1
    ACTIVE_POLICY_COHORT = "stability_v1"

    def __init__(
        self,
        state_path: str | Path,
        policy: PonsManagedPortfolioPolicy | None = None,
    ):
        self.state_path = Path(state_path)
        self.policy = policy or PonsManagedPortfolioPolicy()
        self.state = self._load()

    def _policy_signature(self) -> str:
        return hashlib.sha256(
            _canonical_json(asdict(self.policy)).encode("utf-8")
        ).hexdigest()

    def _load(self) -> dict:
        default = {
            "schema_version": self.SCHEMA_VERSION,
            "protocol": "pons",
            "chain_id": PONS_CHAIN_ID,
            "policy": asdict(self.policy),
            "policy_sha256": self._policy_signature(),
            "peak_equity_eth": self.policy.starting_capital_eth,
            "cooldown_until": None,
            "last_breaker_reasons": [],
            "last_evaluation": {},
            "paper_only": True,
            "live_execution_enabled": False,
        }
        value = _read_json(self.state_path, default)
        if value.get("schema_version") != self.SCHEMA_VERSION:
            raise ValueError(
                "managed portfolio state schema mismatch; choose a new root"
            )
        existing = value.get("policy_sha256")
        if existing and existing != self._policy_signature():
            raise ValueError(
                "Existing managed portfolio uses a different policy; "
                "choose a new root"
            )
        if not value.get("paper_only") or value.get(
            "live_execution_enabled"
        ):
            raise ValueError(
                "managed portfolio state lost its paper-only boundary"
            )
        return default | value

    def _save(self) -> None:
        _atomic_json(self.state_path, self.state)

    @staticmethod
    def _event_day(timestamp: str | None) -> str | None:
        if not timestamp:
            return None
        try:
            return datetime.fromisoformat(
                str(timestamp).replace("Z", "+00:00")
            ).astimezone(timezone.utc).date().isoformat()
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _remaining_cost(position: dict) -> float:
        initial = max(
            1, int(position.get("initial_quantity_raw") or 0)
        )
        remaining = max(
            0, int(position.get("remaining_quantity_raw") or 0)
        )
        return (
            float(position.get("cost_basis_eth") or 0.0)
            * remaining
            / initial
        )

    def metrics(
        self,
        trader: PonsPaperTrader,
        ledger: PaperTradeLedger,
        *,
        now: float | None = None,
    ) -> dict:
        now = time.time() if now is None else float(now)
        today = datetime.fromtimestamp(
            now, tz=timezone.utc
        ).date().isoformat()
        positions = list(trader.state.get("positions", {}).values())
        open_positions = [
            item for item in positions if item.get("status") == "open"
        ]
        gross_exposure = sum(
            self._remaining_cost(item) for item in open_positions
        )
        realized_pnl = sum(
            float(item.get("realized_pnl_eth") or 0.0)
            for item in positions
        )
        unrealized_pnl = 0.0
        unpriced_open = 0
        for position in open_positions:
            remaining_cost = self._remaining_cost(position)
            open_value = position.get("last_modeled_exit_eth")
            if open_value is None:
                unpriced_open += 1
                open_value = remaining_cost
            unrealized_pnl += float(open_value) - remaining_cost
        equity = (
            self.policy.starting_capital_eth
            + realized_pnl
            + unrealized_pnl
        )
        peak = max(
            float(
                self.state.get("peak_equity_eth")
                or self.policy.starting_capital_eth
            ),
            equity,
        )
        self.state["peak_equity_eth"] = peak
        drawdown_pct = (
            max(0.0, (peak - equity) / peak * 100)
            if peak > 0 else 100.0
        )
        events = ledger.load()
        daily_entries = sum(
            event.get("event_type") == "pons_paper_buy"
            and self._event_day(event.get("timestamp")) == today
            for event in events
        )
        daily_realized_pnl = sum(
            float((event.get("payload") or {}).get("realized_pnl_eth") or 0.0)
            for event in events
            if event.get("event_type") == "pons_paper_sell"
            and self._event_day(event.get("timestamp")) == today
        )
        consecutive_losses = 0
        closed = sorted(
            (
                item for item in positions
                if item.get("status") == "closed"
            ),
            key=lambda item: float(item.get("closed_at") or 0),
            reverse=True,
        )
        for position in closed:
            if float(position.get("realized_pnl_eth") or 0.0) < 0:
                consecutive_losses += 1
            else:
                break
        return {
            "starting_capital_eth": self.policy.starting_capital_eth,
            "equity_eth": equity,
            "peak_equity_eth": peak,
            "drawdown_pct": drawdown_pct,
            "gross_exposure_eth": gross_exposure,
            "open_positions": len(open_positions),
            "unpriced_open_positions": unpriced_open,
            "daily_entries": daily_entries,
            "daily_realized_pnl_eth": daily_realized_pnl,
            "consecutive_losses": consecutive_losses,
            "realized_pnl_eth": realized_pnl,
            "unrealized_pnl_eth": unrealized_pnl,
        }

    def evaluate(
        self,
        trader: PonsPaperTrader,
        ledger: PaperTradeLedger,
        policy_learning: dict,
        *,
        prospective_cost_eth: float = 0.0,
        now: float | None = None,
    ) -> dict:
        now = time.time() if now is None else float(now)
        metrics = self.metrics(trader, ledger, now=now)
        blockers: list[str] = []
        active_cohort = (
            (policy_learning.get("cohorts") or {}).get(
                self.ACTIVE_POLICY_COHORT
            ) or {}
        )
        promotion_blockers = list(
            active_cohort.get("promotion_blockers") or []
        )
        if self.policy.require_promotable_active_policy:
            if not active_cohort:
                blockers.append("managed_policy_evidence_missing")
            elif promotion_blockers:
                blockers.append(
                    "managed_active_policy_not_promotable"
                )
        if metrics["unpriced_open_positions"]:
            blockers.append("managed_unpriced_open_positions")
        if (
            metrics["open_positions"]
            >= self.policy.maximum_concurrent_positions
        ):
            blockers.append("managed_position_limit")
        if (
            metrics["gross_exposure_eth"] + prospective_cost_eth
            > self.policy.maximum_gross_exposure_eth + 1e-12
        ):
            blockers.append("managed_exposure_limit")
        if (
            metrics["daily_entries"]
            >= self.policy.maximum_daily_entries
        ):
            blockers.append("managed_daily_entry_limit")
        risk_breakers = []
        if (
            metrics["daily_realized_pnl_eth"]
            <= -self.policy.maximum_daily_realized_loss_eth
        ):
            risk_breakers.append("managed_daily_loss_breaker")
        if metrics["drawdown_pct"] >= self.policy.maximum_drawdown_pct:
            risk_breakers.append("managed_drawdown_breaker")
        if (
            metrics["consecutive_losses"]
            >= self.policy.maximum_consecutive_losses
        ):
            risk_breakers.append("managed_loss_streak_breaker")
        cooldown_until = _safe_float(
            self.state.get("cooldown_until"), None
        )
        if risk_breakers and (
            cooldown_until is None or cooldown_until <= now
        ):
            cooldown_until = now + self.policy.cooldown_hours * 3600
            self.state["cooldown_until"] = cooldown_until
            self.state["last_breaker_reasons"] = risk_breakers
        blockers.extend(risk_breakers)
        if cooldown_until is not None and cooldown_until > now:
            blockers.append("managed_circuit_breaker_cooldown")
        blockers = list(dict.fromkeys(blockers))
        result = {
            "allowed": not blockers,
            "status": "ready" if not blockers else "paused",
            "blockers": blockers,
            "policy": asdict(self.policy),
            "policy_sha256": self._policy_signature(),
            "active_policy_cohort": self.ACTIVE_POLICY_COHORT,
            "active_policy_promotion_blockers": promotion_blockers,
            "metrics": metrics,
            "prospective_cost_eth": prospective_cost_eth,
            "cooldown_until": cooldown_until,
            "evaluated_at": _iso_from_seconds(now),
            "paper_only": True,
            "live_execution_enabled": False,
        }
        self.state["last_evaluation"] = result
        self._save()
        return result

    def verify(self) -> tuple[bool, str]:
        if self.state.get("schema_version") != self.SCHEMA_VERSION:
            return False, "managed portfolio schema mismatch"
        if self.state.get("policy_sha256") != self._policy_signature():
            return False, "managed portfolio policy signature mismatch"
        if not self.state.get("paper_only"):
            return False, "managed portfolio lost paper-only boundary"
        if self.state.get("live_execution_enabled"):
            return False, "managed portfolio claims live execution"
        peak = _safe_float(self.state.get("peak_equity_eth"), None)
        if peak is None or peak <= 0:
            return False, "managed portfolio peak equity is invalid"
        return True, "verified managed paper portfolio state"


class PonsCognitiveLoop:
    """Private persistent senses/modalities registry for Pons analyses."""

    REGISTRIES = ("senses.json", "modalities.json")

    def __init__(self, chain_root: str | Path, skill_dir: str):
        self.root = Path(chain_root)
        registry = self.root / "registry"
        source = Path(skill_dir) / "registry"
        present = {
            name: (registry / name).is_file() for name in self.REGISTRIES
        }
        if any(present.values()) and not all(present.values()):
            missing = ", ".join(
                name for name, exists in present.items() if not exists
            )
            raise RuntimeError(
                f"Pons faculty registry is incomplete; refusing repair: {missing}"
            )
        if not any(present.values()):
            registry.mkdir(parents=True, exist_ok=True)
            for name in self.REGISTRIES:
                if not (source / name).is_file():
                    raise RuntimeError(f"Skill registry is missing {name}")
                shutil.copy2(source / name, registry / name)
        self.recall_module = _load_skill_module(skill_dir, "recall")
        self.cambium_module = _load_skill_module(skill_dir, "cambium")
        self.epochs_module = _load_skill_module(skill_dir, "epochs")
        self.immune_module = _load_skill_module(skill_dir, "immune")
        _load_skill_module(skill_dir, "modality_ops")
        self.recall = self.recall_module.Recall(
            self.root, registry_root=self.root
        )
        self.immune = self.immune_module.Immune(self.root)
        ok, report = self.epochs_module.check_epoch(self.root)
        if not ok:
            raise RuntimeError(
                "Pons faculty registry integrity failed before governance "
                "migration: " + "; ".join(report)
            )
        migration: dict = {}
        seal_registry_mutation(
            self.epochs_module,
            self.root,
            reason="Chainseer Pons tighten-only faculty governance migration",
            write=lambda: migration.update(
                migrate_cognitive_faculty_governance(self.root)
            ),
        )
        self.verify_registry()

    def verify_registry(self) -> None:
        ok, report = self.epochs_module.check_epoch(self.root)
        if not ok:
            raise RuntimeError(
                "Pons faculty registry integrity failed: " + "; ".join(report)
            )
        governance_ok, governance_report = verify_governance_registry(self.root)
        if not governance_ok:
            raise RuntimeError(
                "Pons faculty governance failed: "
                + "; ".join(governance_report)
            )

    @staticmethod
    def _safe_input(
        candidate: PonsLaunchCandidate, decision: PonsRiskDecision
    ) -> str:
        quote = decision.market.get("executable_quote") or {}
        concentration = (
            decision.security.get("holder_concentration") or {}
        )
        value = {
            "task": "Pons onchain launch risk and paper execution analysis",
            "chain_id": PONS_CHAIN_ID,
            "token_address": candidate.token_address,
            "factory_label": candidate.factory_label,
            "launch_block": candidate.launch_block,
            "block_pin": decision.block_pin,
            "risk_level": decision.risk_level,
            "score": decision.score,
            "paper_entry_allowed": decision.paper_entry_allowed,
            "analysis_status": decision.analysis_status,
            "infrastructure_error_count": len(
                decision.infrastructure_errors
            ),
            "hard_stop_types": sorted(decision.hard_stops),
            "warning_count": len(decision.warnings),
            "canonicality": {
                key: value
                for section in (
                    decision.canonicality.get("factory_state_checks") or {},
                    decision.canonicality.get("pool_checks") or {},
                    decision.canonicality.get("locker_checks") or {},
                )
                for key, value in section.items()
            },
            "round_trip_loss_pct": quote.get("round_trip_loss_pct"),
            "quote_deviation_pct": quote.get(
                "quote_deviation_from_slot0_pct"
            ),
            "largest_real_holder_pct": concentration.get(
                "largest_real_holder_pct"
            ),
            "graduated": (
                decision.market.get("graduation") or {}
            ).get("graduated"),
            "evidence_fact_count": decision.provenance.get("fact_count", 0),
            "live_execution_enabled": False,
        }
        return _canonical_json(value)

    @staticmethod
    def _public_labels(labels: dict) -> dict:
        return {
            "senses": [
                {"id": item.get("id"), "name": item.get("name")}
                for item in (labels.get("senses") or [])
            ],
            "modalities": [
                {"id": item.get("id"), "name": item.get("name")}
                for item in (labels.get("modalities") or [])
            ],
            "salience": labels.get("salience"),
            "dissonance": labels.get("dissonance"),
            "frames": list(labels.get("frames") or []),
            "computed": labels.get("computed") or {},
            "retrieved_faculties": list(labels.get("retrieved") or []),
        }

    def prepare(
        self, candidate: PonsLaunchCandidate, decision: PonsRiskDecision
    ) -> tuple[str, dict]:
        self.verify_registry()
        safe_input = self._safe_input(candidate, decision)
        screened = self.immune.screen(safe_input)
        if screened.get("blocked"):
            raise RuntimeError(
                "Pons cognitive intake refused by the covenant membrane"
            )
        recalled = self.recall.retrieve(
            safe_input,
            context=(
                "Pons canonical risk analysis; deterministic onchain evidence "
                "and executable quotes remain authoritative."
            ),
            budget_tokens=650,
            max_blocks=5,
            neighbors=0,
            use_index=True,
        )
        labels = recalled.get("query_labels") or self.recall.label(safe_input)
        cognition = {
            "version": "pons-1.0",
            "status": "prepared",
            "input_policy": "trusted_structured_fields_only",
            **self._public_labels(labels),
            "relevant_rings": [
                block.get("index") for block in (recalled.get("blocks") or [])
                if isinstance(block.get("index"), int)
            ],
            "immune": {
                "status": "admitted",
                "covenant": screened.get("covenant"),
            },
            "growth": [],
            "authority": "cognitive_advisory_only",
            "tighten_only_policy": TIGHTEN_ONLY_POLICY_VERSION,
        }
        if not cognition["senses"] and not cognition["modalities"]:
            raise RuntimeError("Pons cognitive loop produced no active faculties")
        return safe_input, cognition

    def finalize(
        self,
        candidate: PonsLaunchCandidate,
        decision: PonsRiskDecision,
        safe_input: str,
        cognition: dict,
        analysis_ring: dict,
    ) -> dict:
        guard = self.immune_module.guard_turn(
            self.root,
            analysis_ring["index"],
            input_text=safe_input,
            lesson="Chainseer Pons paper analysis covenant guard",
        )
        if guard.get("action") not in {None, "none", "clean"}:
            raise RuntimeError(
                "Pons cognitive guard rejected the analysis: "
                + str(guard.get("action"))
            )
        prior_salience = os.environ.get("CT_TURN_SALIENCE")
        salience = cognition.get("salience")
        if isinstance(salience, (int, float)):
            os.environ["CT_TURN_SALIENCE"] = str(int(salience))
        # Authorize the registry mutation from the verified baseline BEFORE
        # Cambium writes to it. Cambium owns the write, so the ticket has to be
        # taken here; sealing afterwards would ask the epoch layer to bless a
        # registry it has not seen change.
        _begin = getattr(self.epochs_module, "begin_mutation", None)
        growth_ticket = _begin(self.root) if callable(_begin) else None
        try:
            growth = self.cambium_module.fill_gap(
                self.root,
                self._safe_input(candidate, decision),
                context="Chainseer Pons canonical-risk capability gap",
                both=True,
                registry_root=self.root,
            )
        finally:
            if prior_salience is None:
                os.environ.pop("CT_TURN_SALIENCE", None)
            else:
                os.environ["CT_TURN_SALIENCE"] = prior_salience
        cognition["growth"] = [
            {
                "action": item.get("action"),
                "faculty": (item.get("faculty") or {}).get("name"),
                "kind": (item.get("faculty") or {}).get("kind"),
                "reason": item.get("reason"),
            }
            for item in (growth or [])
        ]
        # Same invariant as chainseer.py: governance registration and epoch
        # sealing must key off the SAME activation action set, or born/woken
        # faculties get sealed into an epoch with no governance record and
        # fail verification afterwards.
        governed_identities = {
            (
                (item.get("faculty") or {}).get("kind"),
                (item.get("faculty") or {}).get("name"),
            )
            for item in (growth or [])
            if item.get("action") in _REGISTRY_GOVERNED_ACTIONS
        }
        if governed_identities:
            grown = json.loads(
                (self.root / "registry" / "grown.json").read_text(
                    encoding="utf-8"
                )
            )
            governed_definitions = []
            for key, kind in (("senses", "sense"), ("modalities", "modality")):
                for definition in grown.get(key) or []:
                    if (kind, definition.get("name")) in governed_identities:
                        governed_definitions.append({**definition, "kind": kind})
            if len(governed_definitions) != len(governed_identities):
                raise RuntimeError(
                    "An activated Pons faculty lacks an active registry definition"
                )
            register_faculty_governance(
                self.root,
                governed_definitions,
                source=f"pons_cambium_after_analysis:{analysis_ring['index']}",
                default_manifest=cognitive_only_effect_manifest(),
            )
        if any(
            item.get("action") in _REGISTRY_EPOCH_ACTIONS
            for item in (growth or [])
        ):
            _reason = (
                "Pons faculty change after analysis ring "
                f"{analysis_ring['index']}"
            )
            if growth_ticket is not None:
                self.epochs_module.seal_epoch(
                    self.root, reason=_reason, expected_previous=growth_ticket
                )
            else:
                self.epochs_module.seal_epoch(self.root, reason=_reason)
        self.verify_registry()
        cognition["status"] = "complete"
        cognition["analysis_ring"] = analysis_ring["index"]
        completion = self.recall.tc.seal(
            "pons_cognitive_completion",
            {
                "summary": (
                    "Chainseer Pons cognitive loop completed for analysis "
                    f"ring {analysis_ring['index']}"
                ),
                "frame": "assertion",
                "analysis_ring": analysis_ring["index"],
                "analysis_ring_hash": analysis_ring["ring_hash"],
                "cognitive_loop": cognition,
                "paper_only": True,
                "live_execution_enabled": False,
            },
            poq={
                "coherence": 240,
                "relevance": 250,
                "novelty": 225,
                "consistency": 245,
                "depth": 240,
                "covenant": 255,
            },
        )
        ok, report = self.recall.tc.verify()
        if not ok:
            raise RuntimeError(
                "Timechain failed after Pons cognitive completion: "
                + "; ".join(report)
            )
        decision.cognitive_ring = completion["index"]
        return cognition


class PonsTimechainRecorder:
    """PoQ seal plus non-bypassable senses/modalities/Cambium completion."""

    def __init__(self, chain_root: str | Path):
        self.chain_root = Path(chain_root)
        skill_dir = _get_pons_skill_dir()
        tc_module = _load_timechain_module(skill_dir)
        self.poq_module = _load_skill_module(skill_dir, "poq")
        self.tc = tc_module.Timechain(root=self.chain_root)
        if self.tc.height() == 0:
            self.tc.genesis(name="Chainseer Pons")
        self.cognitive = PonsCognitiveLoop(self.chain_root, skill_dir)
        ok, report = self.tc.verify()
        if not ok:
            raise RuntimeError(f"Timechain verification failed: {report}")

    def _find(self, idempotency_key: str | None) -> dict | None:
        if not idempotency_key:
            return None
        return next(
            (
                ring for ring in self.tc.iter_rings()
                if (ring.get("payload") or {}).get("idempotency_key")
                == idempotency_key
            ),
            None,
        )

    def seal_analysis(
        self,
        candidate: PonsLaunchCandidate,
        decision: PonsRiskDecision,
        *,
        idempotency_key: str | None = None,
    ) -> int:
        existing = self._find(idempotency_key)
        if existing is not None:
            decision.timechain_ring = existing["index"]
            return existing["index"]
        safe_input, cognition = self.cognitive.prepare(candidate, decision)
        summary = (
            f"Pons launch {candidate.token_address} assessed as "
            f"{decision.analysis_status} ({decision.risk_level}) with "
            f"score {decision.score}/100; "
            f"paper entry {'allowed' if decision.paper_entry_allowed else 'refused'} "
            "and live execution disabled."
        )
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            summary,
            context=_canonical_json({
                "candidate": candidate.to_dict(),
                "decision": decision.to_dict(),
            }),
            ring_type="pons_launch_analysis",
            external_scores={
                "coherence": 245,
                "relevance": 255,
                "novelty": 235,
                "consistency": (
                    250 if decision.provenance.get("fact_count") else 170
                ),
                "depth": min(
                    250,
                    175 + 10 * sum(bool(v) for v in decision.coverage.values()),
                ),
                "covenant": 255,
            },
            frame="assertion",
            evidence_texts=[
                _canonical_json(decision.provenance),
                _canonical_json(decision.canonicality),
            ],
            extra_payload={
                "chain_id": PONS_CHAIN_ID,
                "protocol": "pons",
                "network": "pons",
                "token_address": candidate.token_address,
                "candidate": candidate.to_dict(),
                "decision": decision.to_dict(),
                "cognitive_loop": cognition,
                # Seal the evidence identity INTO the ring, exactly as Base and
                # Robinhood do. An outcome can only ever be bound to an analysis
                # whose evidence manifest was sealed at analysis time -- a hash
                # added later proves nothing about what was actually observed.
                # Without this, every Pons analysis is permanently
                # analysis_evidence_incomplete and can never become recallable
                # or carry a canonical outcome.
                **analysis_evidence_binding(
                    decision.provenance,
                    anchor_type="block_pin",
                    anchor_value=decision.block_pin,
                ),
                "idempotency_key": idempotency_key,
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(
                f"PoQ refused Pons analysis seal: {verdict.get('decision')}"
            )
        decision.timechain_ring = ring["index"]
        decision.cognition = self.cognitive.finalize(
            candidate, decision, safe_input, cognition, ring
        )
        return ring["index"]

    def seal_security_outcome(
        self,
        analysis_ring_index: int,
        outcomes: dict,
        *,
        observed_at: str,
        outcome_provenance: dict | None,
        horizon: str,
    ) -> dict | None:
        """Bind a later observation to the analysis that predicted it.

        The original forecast is never rewritten -- the outcome is a NEW ring
        citing the analysis ring, its sealed evidence hash, and its block pin.
        Idempotent per (analysis ring, horizon) so a replayed cycle cannot
        double-count the same measurement.
        """
        key = f"pons:outcome:{analysis_ring_index}:{horizon}"
        existing = self._find(key)
        if existing is not None:
            return existing
        original = next(
            (
                ring for ring in self.tc.iter_rings()
                if ring.get("index") == analysis_ring_index
            ),
            None,
        )
        if original is None:
            return None
        record = build_outcome_record(
            original,
            outcomes,
            observed_at=observed_at,
            outcome_provenance=outcome_provenance,
            calibration={
                "analysis_version": "pons-adapter-v1",
                "original_risk_level": (
                    (original.get("payload") or {}).get("decision") or {}
                ).get("risk_level"),
                "original_score": (
                    (original.get("payload") or {}).get("decision") or {}
                ).get("score"),
                # Pons is admission-gated: whether it REFUSED the token is the
                # judgment this outcome validates, and 92% of analyses never
                # trade, so this is usually the only signal available.
                "paper_entry_allowed": (
                    (original.get("payload") or {}).get("decision") or {}
                ).get("paper_entry_allowed"),
            },
        )
        reference = record["analysis_reference"]
        summary = (
            f"Pons {horizon} security outcome for analysis ring "
            f"{analysis_ring_index}: "
            f"{'adverse' if record.get('security_outcomes') else 'no adverse'} "
            "security event observed; live execution remains disabled."
        )
        ring = self.tc.seal(
            "pons_security_outcome",
            {
                "summary": summary,
                "analysis_ring": analysis_ring_index,
                "analysis_ring_hash": original.get("ring_hash"),
                "original_evidence_hash": reference["original_evidence_hash"],
                "anchor_type": reference["anchor_type"],
                "anchor_value": reference["anchor_value"],
                "horizon": horizon,
                "observed_at": observed_at,
                "outcome_record": record,
                "idempotency_key": key,
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        return ring

    def seal_trade_event(self, event: dict, *, simulation: str) -> int:
        event_hash = str(event.get("event_hash") or "")
        key = f"pons:{simulation}:event:{event_hash}"
        existing = self._find(key)
        if existing is not None:
            return existing["index"]
        payload = event.get("payload") or {}
        ring = self.tc.seal(
            "pons_paper_event",
            {
                "summary": (
                    f"Pons {simulation} event {event.get('event_type')} "
                    f"sealed from operational hash {event_hash[:16]}."
                ),
                "frame": "assertion",
                "idempotency_key": key,
                "simulation": simulation,
                "event_type": event.get("event_type"),
                "event_hash": event_hash,
                "token_address": payload.get("token_address"),
                "reason": payload.get("reason"),
                "paper_only": True,
                "live_execution_enabled": False,
            },
            poq={
                "coherence": 240,
                "relevance": 250,
                "novelty": 220,
                "consistency": 250,
                "depth": 235,
                "covenant": 255,
            },
        )
        return ring["index"]

    def seal_admission(self, admission: dict) -> int:
        key = (
            f"pons:admission:{admission.get('token_address')}:"
            f"{admission.get('latest_block_pin')}:"
            f"{admission.get('observation_count')}:"
            f"{admission.get('latest_analysis_status')}:"
            f"{admission.get('policy_sha256')}"
        )
        existing = self._find(key)
        if existing is not None:
            return existing["index"]
        ring = self.tc.seal(
            "pons_admission_observation",
            {
                "summary": (
                    f"Pons admission observation for "
                    f"{admission.get('token_address')} is "
                    f"{'ready' if admission.get('allowed') else 'quarantined'} "
                    f"after {admission.get('complete_observation_count')} "
                    "complete observation(s)."
                ),
                "frame": "assertion",
                "idempotency_key": key,
                "token_address": admission.get("token_address"),
                "status": admission.get("status"),
                "allowed": bool(admission.get("allowed")),
                "blockers": list(admission.get("blockers") or []),
                "observation_count": admission.get("observation_count"),
                "complete_observation_count": admission.get(
                    "complete_observation_count"
                ),
                "indeterminate_observation_count": admission.get(
                    "indeterminate_observation_count"
                ),
                "latest_analysis_status": admission.get(
                    "latest_analysis_status"
                ),
                "scheduler": admission.get("scheduler"),
                "age_seconds": admission.get("age_seconds"),
                "latest_block_pin": admission.get("latest_block_pin"),
                "latest_round_trip_loss_pct": admission.get(
                    "latest_round_trip_loss_pct"
                ),
                "latest_liquidity_usd": admission.get(
                    "latest_liquidity_usd"
                ),
                "policy_sha256": admission.get("policy_sha256"),
                "paper_only": True,
                "live_execution_enabled": False,
            },
            poq={
                "coherence": 245,
                "relevance": 255,
                "novelty": 230,
                "consistency": 250,
                "depth": 240,
                "covenant": 255,
            },
        )
        return ring["index"]

    def seal_policy_evaluation(self, snapshot: dict) -> int:
        digest_value = {
            key: value
            for key, value in snapshot.items()
            if key != "generated_at"
        }
        digest = hashlib.sha256(
            _canonical_json(digest_value).encode("utf-8")
        ).hexdigest()
        key = f"pons:policy-evaluation:{digest}"
        existing = self._find(key)
        if existing is not None:
            return existing["index"]
        cohorts = {
            name: {
                "overall": value.get("overall"),
                "validation": value.get("validation"),
                "avoided_control_losses": value.get(
                    "avoided_control_losses"
                ),
                "missed_control_winners": value.get(
                    "missed_control_winners"
                ),
            }
            for name, value in (snapshot.get("cohorts") or {}).items()
        }
        ring = self.tc.seal(
            "pons_policy_evaluation",
            {
                "summary": (
                    "Pons counterfactual policy grid evaluated; "
                    f"recommendation status is "
                    f"{(snapshot.get('recommendation') or {}).get('status')}."
                ),
                "frame": "assertion",
                "idempotency_key": key,
                "snapshot_sha256": digest,
                "evaluation_mode": snapshot.get("evaluation_mode"),
                "valuation_evidence": snapshot.get("valuation_evidence"),
                "policy_grid_sha256": snapshot.get("policy_grid_sha256"),
                "cohorts": cohorts,
                "recommendation": snapshot.get("recommendation"),
                "paper_only": True,
                "live_execution_enabled": False,
            },
            poq={
                "coherence": 245,
                "relevance": 255,
                "novelty": 240,
                "consistency": 245,
                "depth": 245,
                "covenant": 255,
            },
        )
        return ring["index"]

    def seal_guard_cycle(self, snapshot: dict) -> int:
        digest_value = {
            key: value
            for key, value in snapshot.items()
            if key not in {"completed_at", "duration_seconds"}
        }
        digest = hashlib.sha256(
            _canonical_json(digest_value).encode("utf-8")
        ).hexdigest()
        key = f"pons:quote-guard:{digest}"
        existing = self._find(key)
        if existing is not None:
            return existing["index"]
        ring = self.tc.seal(
            "pons_quote_guard",
            {
                "summary": (
                    "Pons fast quote guard checked "
                    f"{snapshot.get('marked', 0)} open position(s) at "
                    f"block {snapshot.get('block_pin')} and emitted "
                    f"{snapshot.get('events', 0)} risk-control event(s)."
                ),
                "frame": "assertion",
                "idempotency_key": key,
                "snapshot_sha256": digest,
                "block_pin": snapshot.get("block_pin"),
                "attempted": snapshot.get("attempted"),
                "marked": snapshot.get("marked"),
                "events": snapshot.get("events"),
                "errors": snapshot.get("errors"),
                "paper": snapshot.get("paper"),
                "shadow": snapshot.get("shadow"),
                "paper_only": True,
                "live_execution_enabled": False,
            },
            poq={
                "coherence": 245,
                "relevance": 255,
                "novelty": 235,
                "consistency": 250,
                "depth": 240,
                "covenant": 255,
            },
        )
        return ring["index"]

    def seal_managed_portfolio(self, snapshot: dict) -> int:
        digest_value = {
            key: value
            for key, value in snapshot.items()
            if key != "evaluated_at"
        }
        digest = hashlib.sha256(
            _canonical_json(digest_value).encode("utf-8")
        ).hexdigest()
        key = f"pons:managed-portfolio:{digest}"
        existing = self._find(key)
        if existing is not None:
            return existing["index"]
        ring = self.tc.seal(
            "pons_managed_portfolio",
            {
                "summary": (
                    "Pons managed paper portfolio is "
                    f"{snapshot.get('status')} with "
                    f"{len(snapshot.get('blockers') or [])} blocker(s); "
                    "live execution remains disabled."
                ),
                "frame": "assertion",
                "idempotency_key": key,
                "snapshot_sha256": digest,
                "status": snapshot.get("status"),
                "allowed": snapshot.get("allowed"),
                "blockers": snapshot.get("blockers"),
                "metrics": snapshot.get("metrics"),
                "policy_sha256": snapshot.get("policy_sha256"),
                "active_policy_cohort": snapshot.get(
                    "active_policy_cohort"
                ),
                "active_policy_promotion_blockers": snapshot.get(
                    "active_policy_promotion_blockers"
                ),
                "cooldown_until": snapshot.get("cooldown_until"),
                "paper_only": True,
                "live_execution_enabled": False,
            },
            poq={
                "coherence": 250,
                "relevance": 255,
                "novelty": 240,
                "consistency": 250,
                "depth": 250,
                "covenant": 255,
            },
        )
        return ring["index"]


class PonsPrototypeEngine:
    def __init__(
        self,
        root: str | Path = "pons_prototype",
        rpc_url: str = PONS_RPC_URL,
        policy: PonsPaperPolicy | None = None,
        admission_policy: PonsAdmissionPolicy | None = None,
        managed_portfolio_policy: PonsManagedPortfolioPolicy | None = None,
        *,
        record_timechain: bool = True,
        chain_root: str | Path = "pons_chain",
        rpc: PonsRPC | None = None,
        http_get: Callable = _http_get_json,
        include_legacy_backfill: bool = False,
    ):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.policy = policy or PonsPaperPolicy()
        self.admission_policy = admission_policy or PonsAdmissionPolicy()
        self.rpc_health = (
            getattr(rpc, "health")
            if rpc is not None and hasattr(rpc, "health")
            else PonsRPCHealth()
        )
        if rpc is not None:
            self.observer_rpc = rpc
            self.rpc = rpc
        else:
            self.observer_rpc = PonsRPC(
                rpc_url, health=self.rpc_health
            )
            self.rpc = PonsRPC(rpc_url, health=self.rpc_health)
        self.observer = PonsObserver(
            self.observer_rpc,
            self.root,
            include_legacy_backfill=include_legacy_backfill,
        )
        self.analyzer = PonsRiskAnalyzer(
            rpc_url,
            self.root / "analysis_evidence",
            rpc=self.rpc,
            http_get=http_get,
            policy=self.policy,
        )
        self.paper_ledger = PaperTradeLedger(
            self.root / "paper_events.jsonl"
        )
        self.trader = PonsPaperTrader(
            self.root / "paper_state.json",
            self.paper_ledger,
            self.policy,
        )
        self.shadow_ledger = PaperTradeLedger(
            self.root / "shadow_events.jsonl"
        )
        self.shadow_trader = PonsPaperTrader(
            self.root / "shadow_state.json",
            self.shadow_ledger,
            self.policy,
            event_namespace="shadow",
            enforce_position_limit=False,
        )
        self.timechain = (
            PonsTimechainRecorder(chain_root) if record_timechain else None
        )
        self.admission = PonsAdmissionQuarantine(
            self.root / "admission_state.json",
            self.admission_policy,
        )
        self.policy_learner = PonsCounterfactualPolicyLearner(
            self.root / "policy_learning.json",
            self.policy,
            self.admission_policy,
        )
        self.managed_portfolio = PonsManagedPortfolioController(
            self.root / "managed_portfolio.json",
            managed_portfolio_policy,
        )
        if self.timechain:
            self.admission.backfill_timechain(
                self.timechain.tc.iter_rings()
            )
        for simulation, trader in (
            ("paper", self.trader),
            ("shadow", self.shadow_trader),
        ):
            for position in trader.state.get("positions", {}).values():
                self.admission.mark_admitted(
                    position.get("token_address", ""),
                    simulation,
                )

    @staticmethod
    def analysis_pipeline() -> dict:
        return {
            "entry_authority": "pons_canonical_risk_v1",
            "stages": [
                "pons_factory_event_and_state_binding",
                "canonical_v3_pool_and_locked_position_nft",
                "source_holder_and_restriction_checks",
                "pinned_executable_buy_and_exit_quotes",
                "tri_state_evidence_quality_and_rpc_backoff",
                "timechain_senses_modalities_poq_and_cambium",
                "opportunity_ranked_multi_observation_quarantine",
                "decoupled_fast_executable_quote_risk_guard",
                "managed_paper_exposure_drawdown_and_loss_breakers",
            ],
            "full_chainseer_analysis_run": False,
            "full_chainseer_analysis_role": (
                "optional enrichment only; not the Pons entry authority"
            ),
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def _save_rpc_health(self) -> dict:
        snapshot = self.rpc_health.snapshot()
        snapshot["generated_at"] = _utc_now()
        _atomic_json(self.root / "rpc_health.json", snapshot)
        return snapshot

    def _seal_new_events(
        self, ledger: PaperTradeLedger, start_index: int, simulation: str
    ) -> None:
        if not self.timechain:
            return
        for event in ledger.load()[start_index:]:
            self.timechain.seal_trade_event(event, simulation=simulation)

    def evaluate_candidate(
        self,
        candidate: PonsLaunchCandidate,
        *,
        enter: bool = False,
        shadow_enter: bool = False,
        seal_analysis: bool = True,
    ) -> dict:
        decision = self.analyzer.analyze(candidate)
        if self.timechain and seal_analysis:
            self.timechain.seal_analysis(
                candidate,
                decision,
                idempotency_key=(
                    f"pons:{candidate.launch_id}:analysis:"
                    f"{decision.block_pin}:v1"
                ),
            )
        admission = self.admission.record(candidate, decision)
        if self.timechain:
            self.timechain.seal_admission(admission)
        paper_blockers = self.trader.entry_blockers(candidate, decision)
        shadow_blockers = self.shadow_trader.entry_blockers(
            candidate, decision
        )
        managed_portfolio = self.managed_portfolio.evaluate(
            self.trader,
            self.paper_ledger,
            _read_json(self.root / "policy_learning.json", {}),
            prospective_cost_eth=(
                self.policy.amount_eth + self.policy.modeled_gas_eth
            ),
        )
        if self.timechain:
            managed_portfolio["timechain_ring"] = (
                self.timechain.seal_managed_portfolio(
                    managed_portfolio
                )
            )
        paper_blockers.extend(managed_portfolio["blockers"])
        if not admission["allowed"]:
            paper_blockers.extend(admission["blockers"])
            shadow_blockers.extend(admission["blockers"])
        paper_start = len(self.paper_ledger.load())
        shadow_start = len(self.shadow_ledger.load())
        paper_position = (
            self.trader.enter(candidate, decision)
            if (
                enter
                and admission["allowed"]
                and managed_portfolio["allowed"]
            ) else None
        )
        shadow_position = (
            self.shadow_trader.enter(candidate, decision)
            if shadow_enter and admission["allowed"] else None
        )
        if paper_position:
            self.admission.mark_admitted(
                candidate.token_address, "paper"
            )
        if shadow_position:
            self.admission.mark_admitted(
                candidate.token_address, "shadow"
            )
        self._seal_new_events(self.paper_ledger, paper_start, "paper")
        self._seal_new_events(self.shadow_ledger, shadow_start, "shadow")
        if decision.analysis_status == "infrastructure_indeterminate":
            paper_action = shadow_action = "evidence_indeterminate_retry"
        elif not decision.paper_entry_allowed:
            paper_action = shadow_action = "risk_gate_refused"
        elif not admission["allowed"]:
            paper_action = shadow_action = (
                "admission_quarantine:"
                + ",".join(admission["blockers"])
            )
        else:
            paper_action = (
                "paper_position_opened" if paper_position
                else "waiting_for_policy_conditions:" + ",".join(paper_blockers)
                if enter else "observation_only"
            )
            shadow_action = (
                "shadow_position_opened" if shadow_position
                else "waiting_for_policy_conditions:" + ",".join(shadow_blockers)
                if shadow_enter else "observation_only"
            )
        return {
            "candidate": candidate.to_dict(),
            "decision": decision.to_dict(),
            "paper_action": paper_action,
            "paper_position": paper_position,
            "shadow_action": shadow_action,
            "shadow_position": shadow_position,
            "admission": admission,
            "managed_portfolio": managed_portfolio,
            "analysis_pipeline": self.analysis_pipeline(),
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def run_once(
        self,
        limit: int = 10,
        *,
        enter: bool = False,
        shadow_enter: bool = False,
        max_chunks: int = 50,
        admission_refresh_limit: int = PONS_ADMISSION_REFRESH_LIMIT,
        update_policy_learning: bool = True,
    ) -> list[dict]:
        now = time.time()
        raw_discovered = self.observer.fetch_launches(
            limit, sync=True, max_chunks=max_chunks
        )
        known_before = set(self.admission.state["candidates"])
        discovered, discovery_skipped = (
            self.admission.filter_discovery_candidates(
                raw_discovered, now=now
            )
        )
        pending = self.admission.refresh_candidates(
            self.observer,
            limit=admission_refresh_limit,
            exclude=(item.token_address for item in discovered),
            now=now,
        )
        candidates = discovered + pending
        pending_tokens = {
            item.token_address.lower() for item in pending
        }
        results = []
        for candidate in candidates:
            result = self.evaluate_candidate(
                candidate, enter=enter, shadow_enter=shadow_enter
            )
            result["candidate_lane"] = (
                "admission_priority"
                if candidate.token_address.lower() in pending_tokens
                else "new_discovery"
                if candidate.token_address.lower() not in known_before
                else "due_discovery_refresh"
            )
            results.append(result)
        evidence_counts = {
            status: sum(
                (item.get("decision") or {}).get("analysis_status")
                == status
                for item in results
            )
            for status in (
                "complete_safe",
                "complete_unsafe",
                "infrastructure_indeterminate",
            )
        }
        self.admission.state["scheduler"]["discovery_skipped"] = (
            discovery_skipped
        )
        self.admission.state["scheduler"]["evidence_counts"] = (
            evidence_counts
        )
        self.admission._save()
        _atomic_json(self.root / "last_run.json", results)
        if update_policy_learning:
            self._evaluate_policy_learning()
        self._save_rpc_health()
        return results

    def _evaluate_policy_learning(self) -> dict:
        snapshot = self.policy_learner.evaluate(self.admission)
        if self.timechain:
            self.timechain.seal_policy_evaluation(snapshot)
        return snapshot

    def _quote_position_at_block(
        self,
        token: str,
        quantity_raw: int,
        block_pin: int,
        *,
        evidence_namespace: str,
    ) -> PonsQuote:
        ledger = ProvenanceLedger(
            self.root / evidence_namespace / token.lower()
        )
        ledger.block_pin = int(block_pin)
        self.rpc.bind_context(
            ScanContext(
                PONS_CHAIN_ID,
                int(block_pin),
                ledger,
            )
        )
        quote = self.rpc.quote_exact_input_single(
            token, PONS_WETH, quantity_raw, PONS_POOL_FEE
        )
        quote.provenance = ledger.to_dict()
        return quote

    def _quote_sell(
        self, decision: PonsRiskDecision, token: str, quantity_raw: int
    ) -> PonsQuote:
        return self._quote_position_at_block(
            token,
            quantity_raw,
            decision.block_pin,
            evidence_namespace="mark_evidence",
        )

    def mark_positions(
        self,
        trader: PonsPaperTrader,
        ledger: PaperTradeLedger,
        *,
        limit: int = 5,
    ) -> dict:
        marked = events_count = errors = indeterminate = 0
        simulation = trader.event_namespace
        positions = sorted(
            trader.open_positions,
            key=lambda item: float(item.get("last_mark_timestamp") or 0),
        )[:max(0, int(limit))]
        for position in positions:
            candidate = self.observer.by_token(position["token_address"])
            if candidate is None:
                errors += 1
                continue
            try:
                decision = self.analyzer.analyze(candidate)
                if self.timechain:
                    self.timechain.seal_analysis(
                        candidate,
                        decision,
                        idempotency_key=(
                            f"pons:{candidate.launch_id}:mark:"
                            f"{decision.block_pin}:v1"
                        ),
                    )
                admission = self.admission.record(candidate, decision)
                if self.timechain:
                    self.timechain.seal_admission(admission)
                if (
                    decision.analysis_status
                    == "infrastructure_indeterminate"
                ):
                    indeterminate += 1
                start_index = len(ledger.load())
                events = trader.mark(
                    candidate.token_address,
                    decision,
                    lambda quantity, d=decision, token=candidate.token_address:
                        self._quote_sell(d, token, quantity),
                )
                self._seal_new_events(ledger, start_index, simulation)
                marked += 1
                events_count += len(events)
            except Exception:
                errors += 1
        return {
            "marked": marked,
            "events": events_count,
            "errors": errors,
            "indeterminate": indeterminate,
        }

    def guard_positions(
        self,
        trader: PonsPaperTrader,
        ledger: PaperTradeLedger,
        *,
        block_pin: int,
        limit: int,
    ) -> dict:
        """Run the fast executable-quote lifecycle guard for one cohort."""
        marked = events_count = errors = 0
        simulation = trader.event_namespace
        positions = sorted(
            trader.open_positions,
            key=lambda item: float(item.get("last_mark_timestamp") or 0),
        )[:max(0, int(limit))]
        for position in positions:
            token = position["token_address"]
            start_index = len(ledger.load())
            try:
                events = trader.mark_quote_guard(
                    token,
                    lambda quantity, t=token: self._quote_position_at_block(
                        t,
                        quantity,
                        block_pin,
                        evidence_namespace="guard_evidence",
                    ),
                    block_pin=block_pin,
                )
                self._seal_new_events(ledger, start_index, simulation)
                marked += 1
                events_count += len(events)
            except Exception:
                errors += 1
        return {
            "attempted": len(positions),
            "marked": marked,
            "events": events_count,
            "errors": errors,
            "open": len(trader.open_positions),
        }

    def guard_once(self, *, limit: int = 25) -> dict:
        """Fast paper/shadow risk-control pass with no discovery or admission."""
        started = time.time()
        with LearningRunLock(
            self.root / ".learn_once.lock",
            stale_seconds=PONS_RUN_LOCK_STALE_SECONDS,
        ):
            self._seal_new_events(self.paper_ledger, 0, "paper")
            self._seal_new_events(self.shadow_ledger, 0, "shadow")
            block_pin = self.rpc.get_block_number()
            paper = self.guard_positions(
                self.trader,
                self.paper_ledger,
                block_pin=block_pin,
                limit=min(self.policy.maximum_positions, max(0, int(limit))),
            )
            remaining = max(0, int(limit) - paper["attempted"])
            shadow = self.guard_positions(
                self.shadow_trader,
                self.shadow_ledger,
                block_pin=block_pin,
                limit=remaining,
            )
            summary = {
                "protocol": "pons",
                "chain_id": PONS_CHAIN_ID,
                "completed_at": _utc_now(),
                "duration_seconds": round(time.time() - started, 3),
                "block_pin": block_pin,
                "attempted": paper["attempted"] + shadow["attempted"],
                "marked": paper["marked"] + shadow["marked"],
                "events": paper["events"] + shadow["events"],
                "errors": paper["errors"] + shadow["errors"],
                "paper": paper,
                "shadow": shadow,
                "rpc_health": self._save_rpc_health(),
                "mode": "fast_executable_quote_guard",
                "full_risk_analysis_run": False,
                "paper_only": True,
                "live_execution_enabled": False,
            }
            if self.timechain:
                summary["timechain_ring"] = (
                    self.timechain.seal_guard_cycle(summary)
                )
            _atomic_json(self.root / "guard_summary.json", summary)
            return summary

    def collect_security_outcomes(self, *, limit: int = 25) -> dict:
        """Seal security outcomes for analyses whose horizons have come due.

        Track A of the Pons outcome design: every analysis gets security
        outcomes, traded or not. Pons refuses ~92% of what it analyses, and
        those refusals are exactly the decisions worth validating -- a rug in a
        refused token is the strongest evidence the admission gate worked, and
        no trade-shaped outcome could ever record it.
        """
        summary = {"due": 0, "sealed": 0, "skipped_no_ring": 0, "errors": []}
        if not self.timechain:
            return summary
        # Use the LIVE in-memory state, not a fresh _load(). The store loads
        # once in __init__ and _save() writes self.state, so re-loading here
        # and assigning it back would discard admission changes made earlier
        # in this same learn cycle.
        state = self.admission.state
        candidates = state.get("candidates") or {}
        completed = set(state.get("completed_outcomes") or [])
        changed = False
        for record in candidates.values():
            if summary["sealed"] >= max(0, int(limit)):
                break
            due = pons_due_outcomes(record.get("observations") or [], completed)
            summary["due"] += len(due)
            for item in due:
                if summary["sealed"] >= max(0, int(limit)):
                    break
                baseline, current = item["baseline"], item["current"]
                try:
                    ring = self.timechain.seal_security_outcome(
                        int(baseline["analysis_ring"]),
                        pons_security_outcomes(
                            baseline,
                            current,
                            horizon_seconds=item["horizon_seconds"],
                        ),
                        observed_at=str(current.get("observed_at")),
                        outcome_provenance={
                            **(current.get("provenance") or {}),
                            "block_pin": current.get("block_pin"),
                            "anchor_type": "block_pin",
                        },
                        horizon=item["horizon"],
                    )
                except Exception as exc:      # never abort a learn cycle
                    summary["errors"].append(
                        f"{item['key']}: {type(exc).__name__}: {exc}"[:200]
                    )
                    continue
                if ring is None:
                    summary["skipped_no_ring"] += 1
                    continue
                completed.add(item["key"])
                summary["sealed"] += 1
                changed = True
        if changed:
            state["completed_outcomes"] = sorted(completed)
            self.admission._save()
        return summary

    def learn_once(
        self,
        *,
        limit: int = 10,
        mark_limit: int = 5,
        max_chunks: int = 50,
        admission_refresh_limit: int = PONS_ADMISSION_REFRESH_LIMIT,
    ) -> dict:
        started = time.time()
        with LearningRunLock(
            self.root / ".learn_once.lock",
            stale_seconds=PONS_RUN_LOCK_STALE_SECONDS,
            # Only the learn cycle waits. It runs every ~10 minutes, so a
            # skipped attempt costs real discovery and admission work,
            # whereas the guard re-runs within 2 minutes and can cheaply
            # skip. Making the guard wait instead would just queue it
            # behind a ~290s learn cycle for no benefit.
            wait_seconds=PONS_RUN_LOCK_WAIT_SECONDS,
        ):
            # Reconcile any operational events left unsealed by an interrupted
            # prior cycle. seal_trade_event is idempotent by event hash.
            self._seal_new_events(self.paper_ledger, 0, "paper")
            self._seal_new_events(self.shadow_ledger, 0, "shadow")
            before_catalog = len(_read_json(self.observer.catalog_path, {}))
            results = self.run_once(
                limit,
                enter=True,
                shadow_enter=True,
                max_chunks=max_chunks,
                admission_refresh_limit=admission_refresh_limit,
                update_policy_learning=False,
            )
            after_catalog = len(_read_json(self.observer.catalog_path, {}))
            paper_marks = self.mark_positions(
                self.trader,
                self.paper_ledger,
                limit=min(
                    max(0, int(mark_limit)),
                    self.policy.maximum_positions,
                ),
            )
            marks = self.mark_positions(
                self.shadow_trader,
                self.shadow_ledger,
                limit=mark_limit,
            )
            policy_learning = self._evaluate_policy_learning()
            managed_portfolio = self.managed_portfolio.evaluate(
                self.trader,
                self.paper_ledger,
                policy_learning,
            )
            if self.timechain:
                managed_portfolio["timechain_ring"] = (
                    self.timechain.seal_managed_portfolio(
                        managed_portfolio
                    )
                )
            # Track A: security-horizon outcomes for EVERY analysis, traded
            # or not. Runs after observe() so it sees this cycle's fresh
            # observations, and before summary() so its writes are included.
            security_outcomes = self.collect_security_outcomes()
            admission_summary = self.admission.summary()
            summary = {
                "protocol": "pons",
                "chain_id": PONS_CHAIN_ID,
                "completed_at": _utc_now(),
                "duration_seconds": round(time.time() - started, 3),
                "catalog_size": after_catalog,
                "new_launches": max(0, after_catalog - before_catalog),
                "analyzed": len(results),
                "admission_refreshed": sum(
                    item.get("candidate_lane") == "admission_priority"
                    for item in results
                ),
                "admission_pending": admission_summary["pending"],
                "admission_ready": admission_summary["ready"],
                "admission_cooldown": admission_summary["cooldown"],
                "admission_terminal": admission_summary["terminal"],
                "admission_admitted": admission_summary["admitted"],
                "evidence_complete_safe": sum(
                    item["decision"].get("analysis_status")
                    == "complete_safe"
                    for item in results
                ),
                "evidence_complete_unsafe": sum(
                    item["decision"].get("analysis_status")
                    == "complete_unsafe"
                    for item in results
                ),
                "evidence_indeterminate": sum(
                    item["decision"].get("analysis_status")
                    == "infrastructure_indeterminate"
                    for item in results
                ),
                "shadow_opened": sum(
                    item.get("shadow_position") is not None for item in results
                ),
                "paper_opened": sum(
                    item.get("paper_position") is not None for item in results
                ),
                "paper_marked": paper_marks["marked"],
                "paper_events": paper_marks["events"],
                "paper_errors": paper_marks["errors"],
                "paper_indeterminate": paper_marks["indeterminate"],
                "paper_open": len(self.trader.open_positions),
                "paper_closed": sum(
                    item.get("status") == "closed"
                    for item in self.trader.state["positions"].values()
                ),
                "shadow_marked": marks["marked"],
                "shadow_events": marks["events"],
                "shadow_errors": marks["errors"],
                "shadow_indeterminate": marks["indeterminate"],
                "shadow_open": len(self.shadow_trader.open_positions),
                "shadow_closed": sum(
                    item.get("status") == "closed"
                    for item in self.shadow_trader.state["positions"].values()
                ),
                "shadow_realized_eth": self.shadow_trader.state["realized_eth"],
                "policy_recommendation": policy_learning[
                    "recommendation"
                ],
                "managed_portfolio": managed_portfolio,
                "admission_scheduler": admission_summary["scheduler"],
                "security_outcomes": security_outcomes,
                "rpc_health": self._save_rpc_health(),
                "analysis_pipeline": self.analysis_pipeline(),
                "paper_only": True,
                "live_execution_enabled": False,
            }
            _atomic_json(self.root / "learning_summary.json", summary)
            return summary

    def verify(self) -> dict:
        paper_ok, paper_report = self.paper_ledger.verify()
        shadow_ok, shadow_report = self.shadow_ledger.verify()
        self.admission.state = self.admission._load()
        admission_ok, admission_report = self.admission.verify()
        policy_snapshot = _read_json(
            self.root / "policy_learning.json", {}
        )
        policy_ok, policy_report = self.policy_learner.verify(
            policy_snapshot
        )
        managed_ok, managed_report = self.managed_portfolio.verify()
        timechain_ok = True
        timechain_report = "disabled"
        if self.timechain:
            timechain_ok, report = self.timechain.tc.verify()
            timechain_report = "; ".join(report)
        return {
            "ok": (
                paper_ok
                and shadow_ok
                and admission_ok
                and policy_ok
                and managed_ok
                and timechain_ok
            ),
            "paper_ledger": paper_report,
            "shadow_ledger": shadow_report,
            "admission_state": admission_report,
            "policy_learning": policy_report,
            "managed_portfolio": managed_report,
            "timechain": timechain_report,
            "live_execution_enabled": False,
        }


def _dashboard_cohort(trader: PonsPaperTrader) -> dict:
    now = time.time()
    positions = []
    total_cost = 0.0
    priced_cost = 0.0
    total_value = 0.0
    mark_ages = []
    priced_positions = 0
    for position in trader.state.get("positions", {}).values():
        cost = _safe_float(position.get("cost_basis_eth"))
        realized = _safe_float(position.get("realized_eth"))
        is_open = position.get("status") == "open"
        has_mark = (
            not is_open
            or position.get("last_modeled_exit_eth") is not None
        )
        open_value = (
            _safe_float(position.get("last_modeled_exit_eth"))
            if is_open and has_mark else 0.0
        )
        modeled_value = realized + open_value if has_mark else None
        multiple = None
        if has_mark:
            multiple = (
                _safe_float(position.get("last_total_multiple"), None)
                if is_open else (
                    modeled_value / cost if cost > 0 else None
                )
            )
        mark_timestamp = _safe_float(
            position.get("last_mark_timestamp"), None
        )
        mark_age = (
            max(0.0, now - mark_timestamp)
            if mark_timestamp is not None else None
        )
        if is_open and mark_age is not None:
            mark_ages.append(mark_age)
        total_cost += cost
        if has_mark:
            priced_positions += 1
            priced_cost += cost
            total_value += modeled_value
        positions.append({
            "token_address": position.get("token_address"),
            "symbol": position.get("symbol") or "???",
            "status": position.get("status"),
            "score": position.get("last_mark_score")
            if position.get("last_mark_score") is not None
            else position.get("analysis_score"),
            "risk_level": position.get("last_mark_risk_level")
            or position.get("analysis_risk_level"),
            "entry_timestamp": position.get("entry_timestamp"),
            "last_mark_timestamp": mark_timestamp,
            "mark_age_seconds": mark_age,
            "cost_basis_eth": cost,
            "modeled_value_eth": modeled_value,
            "realized_eth": realized,
            "multiple": multiple,
            "close_reason": position.get("close_reason"),
        })
    positions.sort(
        key=lambda item: (
            item["status"] != "open",
            -(item["last_mark_timestamp"] or 0),
        )
    )
    opened = len(positions)
    open_count = sum(item["status"] == "open" for item in positions)
    closed_count = opened - open_count
    sorted_ages = sorted(mark_ages)
    p95_age = (
        sorted_ages[min(
            len(sorted_ages) - 1,
            math.ceil(len(sorted_ages) * 0.95) - 1,
        )]
        if sorted_ages else None
    )
    distribution = _performance_distribution(
        positions,
        cost_key="cost_basis_eth",
        value_key="modeled_value_eth",
    )
    return {
        "opened": opened,
        "open": open_count,
        "closed": closed_count,
        "cost_basis_eth": total_cost,
        "priced_cost_basis_eth": priced_cost,
        "modeled_value_eth": total_value,
        "modeled_return_pct": distribution["modeled_return_pct"],
        "return_without_best_pct": distribution[
            "return_without_best_pct"
        ],
        "trimmed_return_pct": distribution["trimmed_return_pct"],
        "median_multiple": distribution["median_multiple"],
        "p25_multiple": distribution["p25_multiple"],
        "p75_multiple": distribution["p75_multiple"],
        "profitable_positions": distribution["profitable"],
        "losing_positions": distribution["losses"],
        "winner_rate_pct": distribution["winner_rate_pct"],
        "best_positive_profit_share_pct": distribution[
            "best_positive_profit_share_pct"
        ],
        "best_position_value_share_pct": distribution[
            "best_position_value_share_pct"
        ],
        "best_position_symbol": distribution["best_position_symbol"],
        "best_position_multiple": distribution["best_position_multiple"],
        "concentration_warning": distribution["concentration_warning"],
        "priced_positions": priced_positions,
        "unpriced_positions": opened - priced_positions,
        "realized_eth": _safe_float(trader.state.get("realized_eth")),
        "p95_mark_age_seconds": p95_age,
        "stale_open_marks": sum(age > 30 * 60 for age in mark_ages),
        "positions": positions,
    }


def _dashboard_snapshot(engine: PonsPrototypeEngine) -> dict:
    # The scheduler runs in another process. Reload atomically-written cohort
    # files on every request so the long-lived dashboard never serves a stale
    # in-memory snapshot.
    # Only what the remaining panels actually need.
    #
    # This request path used to reload admission_state.json (14.7 MB and
    # growing), run a full engine.verify() over every ring in the chain, and
    # read both event ledgers end to end -- roughly 21s per request when idle
    # and 77-113s while a learn cycle held the state lock. The page polls on a
    # timer, so requests arrived faster than they completed and piled up until
    # the dashboard stopped answering at all.
    #
    # The panels those fed (admission quarantine, counterfactual policy lab,
    # shadow cohort, evidence & integrity path, sealed activity) have been
    # removed, so the work goes with them. A dashboard must never be
    # expensive enough to queue behind itself.
    engine.trader.state = engine.trader._load_state()
    engine.shadow_trader.state = engine.shadow_trader._load_state()
    engine.managed_portfolio.state = engine.managed_portfolio._load()
    catalog = _read_json(engine.root / "launch_catalog.json", {})
    schedule = _read_json(engine.root / "schedule.json", {})
    scheduler = _read_json(engine.root / "scheduler_status.json", {})
    if schedule.get("installed") and schedule.get("enabled") is False:
        scheduler = {
            **scheduler,
            "stale_status": scheduler.get("status"),
            "status": "disabled",
        }
    guard_schedule = _read_json(
        engine.root / "guard_schedule.json", {}
    )
    guard_scheduler = _read_json(
        engine.root / "guard_status.json", {}
    )
    if (
        guard_schedule.get("installed")
        and guard_schedule.get("enabled") is False
    ):
        guard_scheduler = {
            **guard_scheduler,
            "stale_status": guard_scheduler.get("status"),
            "status": "disabled",
        }
    learning = _read_json(engine.root / "learning_summary.json", {})
    managed_portfolio = (
        engine.managed_portfolio.state.get("last_evaluation") or {
            "status": "not_evaluated",
            "allowed": False,
            "blockers": ["managed_policy_evidence_missing"],
            "policy": asdict(engine.managed_portfolio.policy),
            "metrics": {},
            "paper_only": True,
            "live_execution_enabled": False,
        }
    )
    guard = _read_json(engine.root / "guard_summary.json", {})
    rpc_health = _read_json(engine.root / "rpc_health.json", {})
    return {
        "generated_at": _utc_now(),
        "protocol": "pons",
        "chain_id": PONS_CHAIN_ID,
        "paper_only": True,
        "live_execution_enabled": False,
        "catalog_size": len(catalog),
        "paper": _dashboard_cohort(engine.trader),
        "shadow": _dashboard_cohort(engine.shadow_trader),
        "managed_portfolio": managed_portfolio,
        "rpc_health": rpc_health,
        "analysis_pipeline": engine.analysis_pipeline(),
        # Real scheduler state, not schedule.json's declared value: that file
        # is only written by manage_chainseer_pons_learning_task.ps1, so any
        # other way of enabling the task leaves it stale and the dashboard
        # reports a paused learner that is actually running.
        "schedule": schedule_with_live_state(schedule),
        "scheduler": scheduler,
        "learning": learning,
        "guard": guard,
        "guard_schedule": schedule_with_live_state(guard_schedule),
        "guard_scheduler": guard_scheduler,
    }


def serve_dashboard(
    engine: PonsPrototypeEngine,
    *,
    host: str = "127.0.0.1",
    port: int = 8766,
) -> None:
    dashboard_path = Path(__file__).with_name("pons_dashboard.html")
    if not dashboard_path.is_file():
        raise FileNotFoundError(f"Pons dashboard was not found: {dashboard_path}")
    dashboard_html = dashboard_path.read_bytes()

    class DashboardHandler(BaseHTTPRequestHandler):
        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                "script-src 'self' 'unsafe-inline'; connect-src 'self'; "
                "img-src 'self' data:; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            route = urlparse(self.path).path
            if route == "/":
                self._send(
                    200, "text/html; charset=utf-8", dashboard_html
                )
                return
            if route == "/api/status":
                try:
                    payload = _canonical_json(
                        _dashboard_snapshot(engine)
                    ).encode("utf-8")
                    self._send(
                        200, "application/json; charset=utf-8", payload
                    )
                except Exception as exc:
                    payload = _canonical_json({
                        "error": str(exc),
                        "generated_at": _utc_now(),
                    }).encode("utf-8")
                    self._send(
                        500, "application/json; charset=utf-8", payload
                    )
                return
            if route == "/health":
                self._send(
                    200,
                    "application/json; charset=utf-8",
                    b'{"status":"ok","read_only":true}',
                )
                return
            self._send(404, "text/plain; charset=utf-8", b"Not found")

        def log_message(self, _format, *_args):
            return

    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError(
            "The Pons dashboard is local-only; bind to 127.0.0.1 or localhost"
        )
    server = ThreadingHTTPServer((host, int(port)), DashboardHandler)
    print(f"Pons state & integrity dashboard: http://{host}:{port}")
    print("Read-only local view. Press Ctrl+C to stop.")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _print_results(results: Iterable[dict]) -> None:
    for item in results:
        candidate = item["candidate"]
        decision = item["decision"]
        quote = (decision.get("market") or {}).get("executable_quote") or {}
        round_trip = quote.get("round_trip_loss_pct")
        round_trip_text = (
            f"{round_trip:.2f}%" if isinstance(round_trip, (int, float))
            else "unavailable"
        )
        graduation = (
            (decision.get("market") or {}).get("graduation") or {}
        ).get("graduated")
        print(
            f"{candidate['symbol']:<12} {decision['risk_level']:<8} "
            f"score={decision['score']:>5.1f} "
            f"eligible={'YES' if decision['paper_entry_allowed'] else 'NO '} "
            f"round_trip={round_trip_text} graduated={'YES' if graduation else 'NO'}"
        )
        print(
            f"  EVIDENCE: {decision.get('analysis_status', 'unknown')} "
            f"infrastructure_errors="
            f"{len(decision.get('infrastructure_errors') or [])}"
        )
        print(
            f"  {candidate['token_address']} pool={candidate['pool_address']}"
        )
        print(
            f"  PAPER: {item['paper_action']} | SHADOW: {item['shadow_action']}"
        )
        admission = item.get("admission") or {}
        print(
            "  ADMISSION: "
            f"{'READY' if admission.get('allowed') else 'QUARANTINED'} "
            f"observations={admission.get('observation_count', 0)} "
            f"age={admission.get('age_seconds', 0):.0f}s"
        )
        for stop in decision["hard_stops"]:
            print(f"  STOP: {stop}")


def main() -> None:
    ensure_utf8_runtime()
    parser = argparse.ArgumentParser(
        description="Chainseer Pons paper/shadow adapter (live execution absent)"
    )
    parser.add_argument(
        "command",
        choices=[
            "observe",
            "paper-run",
            "shadow-run",
            "learn-once",
            "guard-once",
            "dashboard",
            "admission",
            "policy-learning",
            "managed-portfolio",
            "pipeline",
            "positions",
            "shadow-positions",
            "verify",
        ],
    )
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--mark-limit", type=int, default=5)
    parser.add_argument(
        "--guard-limit",
        type=int,
        default=25,
        help="maximum open positions checked by guard-once",
    )
    parser.add_argument("--admission-refresh-limit", type=int, default=3)
    parser.add_argument("--max-chunks", type=int, default=50)
    parser.add_argument("--amount-eth", type=float, default=0.01)
    parser.add_argument("--root", default="pons_prototype")
    parser.add_argument("--chain-root", default="pons_chain")
    parser.add_argument("--rpc-url", default=PONS_RPC_URL)
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="local dashboard bind address (dashboard command only)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8766,
        help="local dashboard port (dashboard command only)",
    )
    parser.add_argument(
        "--include-legacy",
        action="store_true",
        help="also backfill the retired legacy factory's final block window",
    )
    parser.add_argument("--no-timechain", action="store_true")
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args()

    engine = PonsPrototypeEngine(
        root=args.root,
        rpc_url=args.rpc_url,
        policy=PonsPaperPolicy(amount_eth=args.amount_eth),
        record_timechain=not args.no_timechain,
        chain_root=args.chain_root,
        include_legacy_backfill=args.include_legacy,
    )
    if args.command in {"observe", "paper-run", "shadow-run"}:
        results = engine.run_once(
            args.limit,
            enter=args.command == "paper-run",
            shadow_enter=args.command == "shadow-run",
            max_chunks=max(1, args.max_chunks),
            admission_refresh_limit=max(
                0, args.admission_refresh_limit
            ),
        )
        if args.json_output:
            print(json.dumps(results, indent=2, sort_keys=True))
        else:
            _print_results(results)
    elif args.command == "learn-once":
        summary = engine.learn_once(
            limit=args.limit,
            mark_limit=args.mark_limit,
            max_chunks=max(1, args.max_chunks),
            admission_refresh_limit=max(
                0, args.admission_refresh_limit
            ),
        )
        if args.json_output:
            print(json.dumps(summary, indent=2, sort_keys=True))
        else:
            print(
                "pons-learn-once: "
                f"catalog={summary['catalog_size']} "
                f"new={summary['new_launches']} "
                f"analyzed={summary['analyzed']} "
                f"quarantine={summary['admission_pending']} "
                f"ready={summary['admission_ready']} "
                f"cooldown={summary['admission_cooldown']} "
                f"terminal={summary['admission_terminal']} "
                f"admitted={summary['admission_admitted']} "
                f"indeterminate={summary['evidence_indeterminate']} "
                f"paper_opened={summary['paper_opened']} "
                f"paper_marked={summary['paper_marked']} "
                f"paper_events={summary['paper_events']} "
                f"paper_open={summary['paper_open']} "
                f"paper_closed={summary['paper_closed']} "
                f"shadow_opened={summary['shadow_opened']} "
                f"shadow_marked={summary['shadow_marked']} "
                f"shadow_events={summary['shadow_events']} "
                f"shadow_open={summary['shadow_open']} "
                f"shadow_closed={summary['shadow_closed']} "
                f"errors={summary['shadow_errors']} "
                f"duration={summary['duration_seconds']:.1f}s"
            )
    elif args.command == "guard-once":
        try:
            summary = engine.guard_once(limit=max(0, args.guard_limit))
        except LearningRunLockedError as exc:
            summary = {
                "status": "skipped_busy",
                "reason": str(exc),
                "paper_only": True,
                "live_execution_enabled": False,
            }
        if args.json_output:
            print(json.dumps(summary, indent=2, sort_keys=True))
        elif summary.get("status") == "skipped_busy":
            print(
                "pons-guard-once: skipped_busy "
                "because the full learner owns the state lock"
            )
        else:
            print(
                "pons-guard-once: "
                f"block={summary['block_pin']} "
                f"attempted={summary['attempted']} "
                f"marked={summary['marked']} "
                f"events={summary['events']} "
                f"errors={summary['errors']} "
                f"duration={summary['duration_seconds']:.1f}s "
                "paper_only=YES"
            )
    elif args.command == "dashboard":
        serve_dashboard(engine, host=args.host, port=args.port)
    elif args.command == "admission":
        print(
            json.dumps(engine.admission.summary(), indent=2, sort_keys=True)
        )
    elif args.command == "policy-learning":
        print(json.dumps(
            _read_json(engine.root / "policy_learning.json", {}),
            indent=2,
            sort_keys=True,
        ))
    elif args.command == "managed-portfolio":
        result = engine.managed_portfolio.evaluate(
            engine.trader,
            engine.paper_ledger,
            _read_json(engine.root / "policy_learning.json", {}),
        )
        if engine.timechain:
            result["timechain_ring"] = (
                engine.timechain.seal_managed_portfolio(result)
            )
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "pipeline":
        print(json.dumps(
            engine.analysis_pipeline(), indent=2, sort_keys=True
        ))
    elif args.command == "positions":
        print(json.dumps(engine.trader.state, indent=2, sort_keys=True))
    elif args.command == "shadow-positions":
        print(
            json.dumps(engine.shadow_trader.state, indent=2, sort_keys=True)
        )
    elif args.command == "verify":
        result = engine.verify()
        print(json.dumps(result, indent=2, sort_keys=True))
        if not result["ok"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
