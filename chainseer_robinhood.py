"""Robinhood Chain launch learning and counterfactual paper trading.

Paper-only by construction: this module contains no private-key, signing,
approval, transaction-submission, or broadcast path. Discovery is isolated
from expensive analysis so new-pair intake remains bounded and observable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import re
import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests

from chainseer import (
    ADDRESS_RE,
    ROBINHOOD_NETWORK,
    UNISWAP_V2_FACTORY,
    UNISWAP_V4_POOL_MANAGER,
    WETH_ADDRESS,
    Chainseer,
    RobinhoodRPC,
    _load_timechain_module,
    ensure_utf8_runtime,
)
from chainseer_base import LearningRunLock
from chainseer_core import atomic_json_write, read_json, safe_float, safe_int
from chainseer_outcome_ledger import (
    analysis_evidence_binding,
    build_outcome_correction,
    build_outcome_record,
    canonical_hash,
    verify_outcome_record,
    verify_outcome_rings,
)
from chainseer_temporal_graph import TemporalGraphStore
from chainseer_robinhood_reflection import (
    RobinhoodReflectionCoordinator,
    default_skill_root,
)


PAIR_CREATED_TOPIC = (
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
)
USDG_ADDRESS = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
UNISWAP_V4_STATE_VIEW = "0xf3334192d15450cdd385c8b70e03f9a6bd9e673b"
UNISWAP_V4_QUOTER = "0x8dc178efb8111bb0973dd9d722ebeff267c98f94"
UNISWAP_V4_POSITION_MANAGER = "0x58daec3116aae6d93017baaea7749052e8a04fa7"
V4_INITIALIZE_TOPIC = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
V4_MODIFY_LIQUIDITY_TOPIC = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
V4_SWAP_TOPIC = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
V4_POSITION_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
V4_GET_LIQUIDITY_SELECTOR = "fa6793d5"
V4_GET_SLOT0_SELECTOR = "c815641c"
V4_QUOTE_EXACT_INPUT_SINGLE_SELECTOR = "aa9d21cb"
V4_POSITION_INFO_SELECTOR = "89097a6a"
V4_POSITION_LIQUIDITY_SELECTOR = "1efeed33"
ERC721_OWNER_OF_SELECTOR = "6352211e"
ERC721_GET_APPROVED_SELECTOR = "081812fc"
SOURCE_V2 = "uniswap_v2"
SOURCE_V3 = "uniswap_v3"
SOURCE_V4 = "uniswap_v4"
HORIZONS = (
    ("15m", 15 * 60),
    ("1h", 60 * 60),
    ("6h", 6 * 60 * 60),
    ("24h", 24 * 60 * 60),
    ("7d", 7 * 24 * 60 * 60),
)
DEFAULT_ROOT = "robinhood_learning"
DEFAULT_CHAIN_ROOT = "robinhood_learning_chain"
DEFAULT_DASHBOARD_PORT = 8769
# How often the dashboard rebuilds its snapshot off the request path.
DASHBOARD_SNAPSHOT_REFRESH_SECONDS = 30.0
DEFAULT_DISCOVERY_LOOKBACK_BLOCKS = 5_000
DEFAULT_DISCOVERY_BLOCK_LIMIT = 5_000
DEFAULT_ANALYSIS_LIMIT = 1
DEFAULT_OUTCOME_LIMIT = 12
DEFAULT_OUTCOME_RECOVERY_LIMIT = 4
DEFAULT_MARKET_RECHECK_LIMIT = 4
DEFAULT_CYCLE_BUDGET_SECONDS = 255.0
ANALYSIS_START_RESERVE_SECONDS = 45.0
OUTCOME_STAGE_BUDGET_SECONDS = 60.0
MISSED_EXPIRATION_LIMIT = 250
# Robinhood currently produces roughly ten blocks per second. Flow V1 stays
# block-relative so historical catch-up scans do not pretend ingestion time is
# event time. The estimate and identity limitation remain explicit telemetry.
# Widened 450 -> 1350 (~45s -> ~135s of chain). This is NOT a threshold
# change: a window still needs 6 swaps from 4 distinct traders with net buy
# pressure. It samples a longer interval to find them. In 92 attempts no
# window ever qualified because the median window held 1 swap from 1 trader;
# measured over the busiest eight pools, 450 blocks clears the swap minimum
# for 7 of 8 but leaves participants short, while 1350 clears both. Wider
# still (2700+) would approach the 5-minute cycle and overlap windows.
FLOW_WINDOW_BLOCKS = 1350
FLOW_MINIMUM_SWAPS = 6
FLOW_MINIMUM_SENDER_HINTS = 4
FLOW_MINIMUM_NET_ANCHOR_FRACTION = 0.10
# Retained for the capping behaviour and for continuity of the recorded
# series, NOT as a promotion bar -- see _refresh_v4_flow_signal. The score is
# anti-predictive within the observed population (p=0.0135, n=59).
FLOW_SHADOW_SCORE_THRESHOLD = 70.0
# Identity enrichment is the binding stage for Flow qualification. Measured on
# a clean v2 corpus: 176 of 176 fresh windows failed minimum_identity_coverage
# and minimum_unique_participants, while only 33 failed the price-direction
# gate -- so every window arrives at qualification unenriched, not badly
# shaped. The near-head pass ingests a full 450-block window each cycle and
# the old budget could not resolve it.
#
# Cycles were using 86-115s of a 300s budget, so the headroom is real: this
# roughly triples the work per cycle and still leaves margin. Raised together
# because a larger row limit inside a 30s budget would simply time out.
# Raised from 250/25/50/30s. Tripling it took the cycle from 115s to 223s of
# a 300s budget and moved identity coverage not at all -- 267 of 267 windows
# still failed it -- so the budget was never the binding constraint and the
# extra time bought nothing but risk of overrunning the cycle. Held at roughly
# double the original: enough to clear a near-head window, with real headroom.
FLOW_ORIGIN_RESOLUTION_LIMIT = 500
FLOW_HISTORICAL_ORIGIN_RESERVE = 50
# Smaller batches bound how far a single unit of work can overshoot the
# deadline: the check happens between batches, so the batch IS the resolution
# of the deadline. 75 calls per batch made the minimum overshoot large.
FLOW_ORIGIN_BATCH_SIZE = 25
FLOW_IDENTITY_STAGE_BUDGET_SECONDS = 60.0
FLOW_ORIGIN_MAXIMUM_ATTEMPTS = 3
FLOW_MINIMUM_IDENTITY_COVERAGE = 0.80
FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS = 120
# Enrichment targeting is a SEPARATE bound from decision freshness. Applying
# the 120-block decision bound to a pool's last swap matched 1 of 1,895 pools,
# because these pools trade a few times per 1,350 blocks -- so the prospective
# cohort was ~1 transaction per cycle and near-head swaps were never attempted
# at all (0 of 18 had a transaction_origins row, while resolution was
# succeeding on 43,722 of 43,722 rows it was asked about). A pool is worth
# enriching while its window still overlaps the near-head region; whether the
# resulting signal is fresh enough to ACT on stays governed by the bound above.
FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS = FLOW_WINDOW_BLOCKS
# Enrichment of the window being sealed, run inside the near-head pass. The
# ingest head jumps 7,114-11,614 blocks between cycles against a 1,350-block
# window, so consecutive windows are DISJOINT: origins resolved after a seal
# land on transactions that have already left the window by the next cycle.
# Measured: 11 windows at identity_coverage 1.00 in one cycle, 1 in the next,
# with one pool going from 7 resolved origins to 0. Live near-head sets have
# been 17-18 transactions, so the cap is generous; it exists only so a burst
# window cannot consume the cycle.
# Observation freshness asks whether the window was near-head WHEN OBSERVED.
# It was measured against the 120-block decision bound applied to the pool's
# most recent swap, so a window observed the moment it was built was called
# `historical` whenever the pool had not traded in the last 120 blocks -- which
# for pools trading a few times per 1,350 blocks is most of them. That excluded
# the observation from research as well as from trading. Decision freshness,
# which is what gates paper entry, stays at 120.
FLOW_MAXIMUM_OBSERVATION_HEAD_LAG_BLOCKS = FLOW_WINDOW_BLOCKS
# Decision freshness is no longer a block count. Measured across 7 near-head
# passes, drift is exactly pass duration times ~9.95 blocks/second, and pass
# duration does not track workload at all -- 1,575 logs took 53s while 2,113
# logs took 867s -- so it is RPC latency, not work, and no code change reduces
# it. A 120-block bound needs a 12-second pass; the fastest of seven was 49s
# and the median ~300s. The bound was unreachable rather than demanding, and
# it held every observation at research-only.
#
# What it was protecting against is acting on a signal after the market moved.
# That is a price question, so it is asked about the price: the entry quote is
# re-taken at decision time and must still be verifiable. The drift between
# the sealed price and the decision price is RECORDED, not gated -- there is
# no evidence yet for the right tolerance, and inventing one would repeat the
# error this session has been correcting. Set the threshold from the cohort.
FLOW_DECISION_DRIFT_THRESHOLD_BPS = None   # unset until the cohort measures it
# A decision quote costs an RPC round trip, and the classifier sweeps every
# observation for the policy version each cycle -- 392 in one measured sweep.
# Quoting all of them would spend hundreds of calls to re-price observations
# that are research-only whatever the price says. So a quote is taken only
# where it could change the verdict, and even then bounded per cycle.
FLOW_DECISION_QUOTE_LIMIT = 20
# Can the position be exited at all? The entry quote already answers it: buy
# and sell at the SAME block, before any time passes. Measured over 65 sealed
# observations that round trip loses a median 22.5%, a minimum of 100% (pools
# with $0 liquidity), and a mean of 42.10% -- against a mean realised 15-minute
# return of -41.37%. Price movement contributes +0.73%. The returns this
# project has been analysing were almost entirely the fee-and-slippage
# structure of the pools, not token behaviour.
#
# The bound below is NOT a profitability threshold and must not be read as
# one: friction of 2% still swamps the only edge yet measured (+0.73%). It is
# the weaker claim that a position which cannot be round-tripped within 2% is
# not a trade at all. The figure is recorded on every observation so the bound
# can be set from the distribution instead of argued about.
FLOW_MAXIMUM_ROUND_TRIP_LOSS = 0.02
# Backfill catch-up. The crawler is bounded per cycle by wall clock rather
# than by a chunk count: the constraint is RPC latency on an endpoint that
# returns 429s, not block arithmetic. At 5,000 blocks a chunk these bounds
# allow up to 200,000 blocks a cycle, so a 500,000-block backlog closes in a
# few cycles instead of never.
FLOW_DISCOVERY_CATCHUP_SECONDS = 120.0
FLOW_DISCOVERY_MAXIMUM_PASSES = 40
FLOW_NEAR_HEAD_ENRICHMENT_LIMIT = 300
FLOW_NEAR_HEAD_ENRICHMENT_BUDGET_SECONDS = 30.0
FLOW_MAXIMUM_PARTICIPANT_SHARE = 0.50
# Bumped with the append-only window history. Every prior measurement came
# from flow_signals, a latest-state table that preserves each pool at its
# terminal window, so the v1 corpus is survivorship-biased and its 18%
# direction anti-correlation is not evidence about the market. The version is
# part of both the cohort hash and the flow_signal_windows primary key, so
# bumping it starts a clean cohort and keeps the new series separate from the
# old rows rather than blending them.
FLOW_EVIDENCE_POLICY_VERSION = "flow-evidence-v3"
#: Immutable cohort identity. The 397 v2 observations were sealed before the
#: provisional-observation ordering was verified end to end, so they are a
#: pilot: preserved for diagnostics, excluded from any promotion decision.
#: Promotion reads this cohort only.
# cohort-002. The first 1,034 observations were sealed while the entry-quote
# verification read a key the payload does not carry, so every one recorded
# quote_verified 0 and can never resolve as exitable. They are immutable, so
# the cohort restarts rather than being repaired -- a cohort whose first
# thousand members can never resolve is not one you want to explain later.
# cohort-003: a 1350-block window is a different measurement from a 450-block
# one, so its observations are not comparable with cohort-002's. The 92
# control observations there stay as the 450-block baseline.
# cohort-004: closed cohort-003 at 914 observations (653 resolved, 238
# pending, 23 non-exitable, all 914 matched_control) because the POPULATION
# changed, not the policy. Batch discovery ran 477,270 blocks behind the head
# -- about 13 hours -- and admitted no pool inside that gap, so every
# cohort-003 observation came from a pool at least 13 hours old when it was
# first seen. Admitting pools on sight cut that latency to 383 blocks, roughly
# 38 seconds, so observations sealed from here can be minutes old.
#
# Those are different experiments. signal_versus_control() comparing across
# them would pair arms drawn from two selection regimes and call the
# difference an effect. cohort-003's 653 resolved outcomes remain the
# thirteen-hour-old-pool baseline and keep their diagnostic value; they are
# simply not comparable with what follows.
#
# Expect worse friction here, not better: pools minutes old are thin by
# construction, so the exitability gate should reject most of them. A signal
# arm that stays near zero is that gate working, not this change failing.
# The 700-resolved target was computed from cohort-003's return variance and
# must be recomputed once cohort-004 can estimate its own.
FLOW_EVIDENCE_COHORT_ID = "flow-evidence-v3-cohort-004"
#: Reflection at 15 COMPLETED primary-horizon observations, stronger evaluation
#: at 30+, and no tier comparison until each compared tier has its own minimum.
#: Scheduled outcomes are not completed outcomes -- 1,985 were scheduled while
#: zero had resolved, and counting them as evidence was the error this guards.
FLOW_PRIMARY_HORIZON_LABEL = "15m"
FLOW_COHORT_REFLECTION_AT = 15
FLOW_COHORT_EVALUATION_AT = 30
FLOW_COHORT_MINIMUM_TIER_SAMPLES = 8
FLOW_EVIDENCE_HORIZONS = (
    ("1m", 60),
    ("5m", 5 * 60),
    ("15m", 15 * 60),
    ("1h", 60 * 60),
    ("6h", 6 * 60 * 60),
)
FLOW_EVIDENCE_FRICTION_BPS = 100.0
FLOW_EVIDENCE_MINIMUM_PROMOTION_SIGNALS = 100
FLOW_EVIDENCE_MAXIMUM_NONEXIT_RATE = 0.10
FLOW_EVIDENCE_MAXIMUM_CATASTROPHIC_RATE = 0.20
FLOW_EVIDENCE_REFLECTION_INTERVAL = 15
FLOW_SIGNAL_EVENT_COOLDOWN_BLOCKS = FLOW_WINDOW_BLOCKS
DASHBOARD_MARKET_CACHE_SECONDS = 20.0
ANCHOR_PRICE_CACHE_SECONDS = 60.0
MAXIMUM_ANALYSIS_ATTEMPTS = 3
OUTCOME_RETRY_SECONDS = 60
V4_CUSTODY_POSITION_REFRESH_LIMIT = 64
V4_CUSTODY_BLOCK_LIMIT = 500
# The live lane must cover at least one normal five-minute block interval.
# Historical custody reconstruction remains deliberately smaller and slower.
V4_CUSTODY_LIVE_BLOCK_LIMIT = 5_000
V4_CUSTODY_MINIMUM_SYNC_INTERVAL_SECONDS = 30 * 60
V4_CUSTODY_START_RESERVE_SECONDS = 60.0
V4_CUSTODY_MAX_POSITION_STATE_LAG_BLOCKS = 120
V4_CUSTODY_MINIMUM_MANAGED_ACTIVE_COVERAGE = 0.95
V4_CUSTODY_MINIMUM_LOCKED_ACTIVE_FRACTION = 0.90
V4_CUSTODY_MINIMUM_UNLOCK_HORIZON_SECONDS = 30 * 24 * 60 * 60
V4_CUSTODY_MINIMUM_PROSPECTIVE_POOLS = 30

# Two nominal learn cycles. See OutcomeStore.tolerance for why one was not
# enough: a 15m checkpoint had a 300s window against a 300s median cadence.
CHECKPOINT_TOLERANCE_MINIMUM_SECONDS = 10 * 60
# A mark landing later than this fraction of its horizon is recorded but not
# learned from -- a 15m outcome observed at 27m is measuring something else.
CHECKPOINT_LEARNING_LATENESS_FRACTION = 0.5
MINIMUM_ENTRY_SCORE = 70.0
MINIMUM_ENTRY_LIQUIDITY_USD = 10_000.0
ENTRY_PRIORITY_PENALTY_MARKET_CAP_USD = 5_000_000.0
MAXIMUM_ENTRY_MARKET_CAP_USD = 10_000_000.0
REENTRY_MOMENTUM_MULTIPLE = 1.05
MOMENTUM_PRIORITY_MULTIPLE = 2.0
MOMENTUM_PRIORITY_MINIMUM_HORIZON_SECONDS = 60 * 60
MOMENTUM_PRIORITY_MAXIMUM_MARKET_CAP_USD = 100_000_000_000.0
MOMENTUM_PRIORITY_MAXIMUM_CAP_TO_LIQUIDITY = 1_000.0
EXECUTABLE_MARKET_RECHECK_SECONDS = 15 * 60
EXECUTABLE_MARKET_WATCH_SECONDS = 7 * 24 * 60 * 60
EXECUTABLE_MARKET_MAXIMUM_POOLS_PER_TOKEN = 8
TEMPORARY_EXECUTION_STOPS = {"V4_MARKET_STATE_UNVERIFIED"}
PAPER_DECISION_ADMITTED = "admitted"
PAPER_DECISION_REJECTED = "rejected"
PAPER_DECISION_WATCHING = "watching_for_executable_market"
PAPER_DECISION_EXPIRED = "expired_no_executable_market"
PAPER_DECISION_ABOVE_CAP = "observing_above_entry_ceiling"
PAPER_DECISION_REENTRY = "waiting_for_reentry_momentum"
PAPER_DECISION_V4_SHADOW = "v4_shadow_only"
PAPER_DECISION_SOURCE_QUARANTINE = "source_risk_quarantine"
SOURCE_RISK_LOOKBACK_CLOSES = 20
SOURCE_RISK_MINIMUM_CLOSES = 5
SOURCE_RISK_MAXIMUM_TOTAL_LOSS_RATE = 0.35
# V4 paper entries remain observable counterfactuals until admission is based
# on an executable, pool-keyed round-trip quote rather than virtual liquidity.
V4_PAPER_ADMISSION_ENABLED = False
# A position whose price has fallen to this fraction of entry is not a market
# to exit, it is a token that stopped existing. Held separately from
# STOP_LOSS_MULTIPLE so the two causes stay distinguishable in every downstream
# audit: a stop loss is a decline the exit rules are meant to catch, a price
# collapse is a rug the entry rules should have refused.
PRICE_COLLAPSE_MULTIPLE = 0.02

PAPER_COST_USD = 100.0
V4_QUOTE_PROBE_USD = 100.0
V4_MAXIMUM_ROUND_TRIP_LOSS = 0.25
MAXIMUM_POSITIONS = 50
STOP_LOSS_MULTIPLE = 0.65
STAGE_ONE_MULTIPLE = 2.0
STAGE_TWO_MULTIPLE = 3.0
STAGE_ONE_ORIGINAL_FRACTION = 0.50
STAGE_TWO_ORIGINAL_FRACTION = 0.25
RUNNER_TRAILING_DRAWDOWN = 0.35
STAGNATION_MINIMUM_MULTIPLE = 1.25
STAGNATION_EXIT_SECONDS = 24 * 60 * 60
MAXIMUM_HOLD_SECONDS = 7 * 24 * 60 * 60
SHADOW_POLICY_FIXED_3X = "fixed_3x"
SHADOW_POLICY_PURE_TRAILING = "pure_trailing"
PURE_TRAILING_ACTIVATION_MULTIPLE = 1.25
ZERO_ADDRESS = "0x" + "0" * 40
ERC20_BALANCE_OF_SELECTOR = "70a08231"
V4_IRRECOVERABLE_NFT_OWNERS = {
    "0x0000000000000000000000000000000000000001",
    "0x000000000000000000000000000000000000dead",
}
V4_TIMELOCK_PROBES = {
    "Unicrypt": "52db191f",
    "Team Finance": "69bb28cd",
    "PinkLock": "b50c0410",
    "TrustSwap": "5aeca126",
}
# Populated only after bytecode and control-path review on Robinhood Chain.
# Interface-shape matches alone remain unverified contract custody.
V4_VERIFIED_LOCKER_CODE_HASHES: dict[str, str] = {}

#: Shadow admission rules. RECORDED, NEVER ENFORCED -- they exist to be
#: measured against real outcomes before anyone is allowed to act on them.
#:
#: Measured over 39 fee_tier=100/tick_spacing=1 V4 pools with usable market-cap
#: history: the 12 that peaked at 2x or better all had market cap / entry
#: liquidity between 0.57 and 1.29, while 16 of the 17 we held that went to
#: zero sat at 1.35 or above. A cut at 1.30 separated them. That threshold was
#: chosen by looking at those same 39 pools, so it is a hypothesis fitted in
#: sample, not a measured hit rate -- which is precisely why it ships as a
#: shadow rule and why nothing reads it to make a decision.
SHADOW_MAXIMUM_MCAP_LIQUIDITY_RATIO = 1.30
#: The hook stop blocked 9 of those 12 runners while every pool that rugged was
#: hook-free. Shadowed to find out whether the stop is inverted; we have never
#: held a hooked pool, so there is no exit evidence on them at all.
V4_HOOK_STOP_CODE = "V4_HOOK_UNAUDITED"
REMOTE_RETRY_ATTEMPTS = 4
REMOTE_RETRY_BASE_SECONDS = 0.25
REMOTE_RETRY_MAX_SECONDS = 2.0


def _utc_now_minus(seconds: float) -> str:
    """ISO timestamp `seconds` in the past, for time-bounded telemetry."""
    return (
        datetime.now(timezone.utc) - timedelta(seconds=max(0.0, seconds))
    ).isoformat()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _topic_address(value: str) -> str:
    text = str(value or "").removeprefix("0x")
    if len(text) != 64:
        return ZERO_ADDRESS
    return "0x" + text[-40:]


def _data_address(value: str, word: int = 0) -> str:
    text = str(value or "").removeprefix("0x")
    start = word * 64
    if len(text) < start + 64:
        return ZERO_ADDRESS
    return "0x" + text[start + 24:start + 64]


def _data_word(value: str, word: int = 0) -> int:
    text = str(value or "").removeprefix("0x")
    start = word * 64
    return int(text[start:start + 64], 16) if len(text) >= start + 64 else 0


def _signed_word(value: str, word: int, bits: int) -> int:
    raw = _data_word(value, word) & ((1 << bits) - 1)
    return raw - (1 << bits) if raw & (1 << (bits - 1)) else raw


def _timestamp(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _remote_call(operation: str, callback, *, attempts: int = REMOTE_RETRY_ATTEMPTS):
    """Retry a bounded remote read without changing any durable cursor state."""
    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
        try:
            return callback()
        except Exception as exc:
            last_error = exc
            # Retrying an oversized eth_getLogs window cannot change the
            # provider's deterministic result. Callers that support adaptive
            # windowing can split immediately after this error is wrapped.
            if "exceeds limit of 10000" in str(exc).lower():
                break
            if attempt + 1 >= max(1, attempts):
                break
            ceiling = min(
                REMOTE_RETRY_MAX_SECONDS,
                REMOTE_RETRY_BASE_SECONDS * (2**attempt),
            )
            time.sleep(ceiling + random.uniform(0.0, ceiling * 0.2))
    raise RuntimeError(
        f"{operation} failed after {max(1, attempts)} attempts: {last_error}"
    ) from last_error


class HashEventLedger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def append(self, event_type: str, payload: dict) -> dict:
        rows = self.load()
        event = {
            "index": len(rows),
            "event_type": event_type,
            "timestamp": _utc_now(),
            "previous_hash": rows[-1]["event_hash"] if rows else "0" * 64,
            "payload": payload,
        }
        event["event_hash"] = hashlib.sha256(
            _canonical(event).encode("utf-8")
        ).hexdigest()
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
        return event

    def verify(self) -> tuple[bool, str]:
        previous = "0" * 64
        rows = self.load()
        for index, event in enumerate(rows):
            if event.get("index") != index:
                return False, f"index mismatch at {index}"
            if event.get("previous_hash") != previous:
                return False, f"previous hash mismatch at {index}"
            expected = hashlib.sha256(
                _canonical({k: v for k, v in event.items() if k != "event_hash"}).encode("utf-8")
            ).hexdigest()
            if event.get("event_hash") != expected:
                return False, f"event hash mismatch at {index}"
            previous = expected
        return True, f"verified {len(rows)} Robinhood learning events"


class RobinhoodLearningTimechainRecorder:
    """Evidence-bound producer ledger isolated from the user-analysis lane.

    The scheduled learner is already a background process. These seals use a
    dedicated root and contain no cognitive calibration, so they cannot take
    the General Chainseer Timechain lock or delay an interactive scan.
    """

    ANALYSIS_VERSION = "robinhood-paper-learning-v1"

    def __init__(self, chain_root: str | Path, *, skill_root: str | Path):
        self.root = Path(chain_root)
        self.root.mkdir(parents=True, exist_ok=True)
        module = _load_timechain_module(str(skill_root))
        self.tc = module.Timechain(self.root)
        if self.tc.height() == 0:
            self.tc.genesis(name="Chainseer Robinhood Learning")
        ok, report = self.tc.verify()
        if not ok:
            raise RuntimeError(
                "Robinhood learning Timechain verification failed: "
                + "; ".join(report)
            )
        # The projection is disposable, but keeping it current from the first
        # prospective ring makes this producer immediately Memory-Core ready.
        TemporalGraphStore(self.root).refresh(self.tc.load())

    def _find(self, idempotency_key: str) -> dict | None:
        return next((
            ring for ring in reversed(list(self.tc.iter_rings()))
            if (ring.get("payload") or {}).get("idempotency_key")
                == idempotency_key
        ), None)

    def _ring(self, index: int | None, expected_hash: str | None) -> dict | None:
        if index is None:
            return None
        ring = next((
            item for item in self.tc.iter_rings()
            if item.get("index") == int(index)
        ), None)
        if ring is None or (
            expected_hash and ring.get("ring_hash") != expected_hash
        ):
            return None
        return ring

    def seal_analysis(
        self, candidate: dict, report: dict, market: dict,
        *, priority_reason: str | None,
    ) -> dict:
        analysis = dict(report.get("analysis") or {})
        provenance = dict(report.get("provenance") or {})
        block_pin = safe_int(
            provenance.get("block_pin"),
            safe_int(candidate.get("block_number"), 0),
        )
        binding = analysis_evidence_binding(
            provenance, anchor_type="block_pin", anchor_value=block_pin,
        )
        identity = canonical_hash({
            "token": str(candidate.get("token_address") or "").lower(),
            "block_pin": block_pin,
            "evidence_hash": binding["evidence_hash"],
            "analysis": analysis,
            "market": market,
        })
        key = f"robinhood-learning-analysis:{identity}"
        existing = self._find(key)
        if existing is not None:
            return existing
        hard_stops = list(analysis.get("hard_stop_overrides") or [])
        token = str(candidate.get("token_address") or "").lower()
        payload = {
            "summary": (
                f"Robinhood paper learner analyzed {token} as "
                f"{analysis.get('risk_level') or 'Unknown'} risk; "
                "live execution remained disabled."
            ),
            "analysis_version": self.ANALYSIS_VERSION,
            "network": "robinhood",
            "chain_id": ROBINHOOD_NETWORK.chain_id,
            "token_address": token,
            "legitimacy_score": analysis.get("legitimacy_score"),
            "risk_level": analysis.get("risk_level"),
            "action_label": analysis.get("action_label"),
            "hard_stop_overrides": hard_stops,
            "component_scores": analysis.get("component_scores") or {},
            "analysis": analysis,
            "market": dict(market or {}),
            "candidate": {
                "token_address": token,
                "pool_id": candidate.get("pool_id"),
                "pair_address": candidate.get("pair_address"),
                "source_version": candidate.get("source_version"),
                "block_number": candidate.get("block_number"),
            },
            "producer": {
                "producer_id": "robinhood-paper-learner",
                "priority_reason": priority_reason,
                "separate_user_analysis_lane": True,
            },
            "provenance": provenance,
            **binding,
            "idempotency_key": key,
            "paper_only": True,
            "live_execution_enabled": False,
        }
        ring = self.tc.seal("token_analysis", payload)
        TemporalGraphStore(self.root).refresh(self.tc.load())
        return ring

    def seal_checkpoint_outcome(
        self, candidate: dict, checkpoint: dict, market: dict,
        *, observation_block: int | None,
    ) -> dict | None:
        original = self._ring(
            candidate.get("producer_analysis_ring_index"),
            candidate.get("producer_analysis_ring_hash"),
        )
        if original is None:
            return None
        token = str(candidate.get("token_address") or "").lower()
        horizon = str(checkpoint.get("horizon_label") or "")
        key = (
            f"robinhood-learning-outcome:{original['ring_hash']}:"
            f"{horizon}:{checkpoint.get('target_at')}"
        )
        existing = self._find(key)
        if existing is not None:
            return existing
        observed_at = str(checkpoint.get("observed_at") or _utc_now())
        outcome_provenance = None
        fact_ids: list[str] = []
        if observation_block is not None:
            query = {
                "token_address": token,
                "pair_address": candidate.get("pair_address"),
                "horizon": horizon,
                "target_at": checkpoint.get("target_at"),
                "forecast_anchor_type": checkpoint.get("anchor_type"),
                "forecast_anchor_at": checkpoint.get("anchor_at"),
            }
            fact_id = "robinhood-market-" + canonical_hash(query)[:20]
            fact_ids = [fact_id]
            outcome_provenance = {
                "anchor_type": "observation_block_pin",
                "block_pin": int(observation_block),
                "fact_count": 1,
                "facts": [{
                    "fact_id": fact_id,
                    "source": "robinhood_market_checkpoint",
                    "query_hash": canonical_hash(query),
                    "response_hash": canonical_hash(market or {}),
                    "block": int(observation_block),
                    "fetched_at": observed_at,
                    "cache_hit": False,
                }],
            }
        multiple = checkpoint.get("market_cap_multiple")
        outcomes = {
            "horizon_seconds": safe_int(checkpoint.get("horizon_seconds"), 0),
            "forecast_anchor_type": checkpoint.get("anchor_type"),
            "forecast_anchor_at": checkpoint.get("anchor_at"),
            "price_return_pct": (
                (safe_float(multiple, 1.0) - 1.0) * 100
                if multiple is not None else None
            ),
            "infrastructure_indeterminate": checkpoint.get("status") != "observed",
            "market_cap_usd": checkpoint.get("market_cap_usd"),
            "market_cap_multiple": multiple,
            "liquidity_usd": checkpoint.get("liquidity_usd"),
        }
        record = build_outcome_record(
            original, outcomes, observed_at=observed_at,
            outcome_provenance=outcome_provenance,
            evidence_fact_ids=fact_ids,
            calibration={
                "analysis_version": self.ANALYSIS_VERSION,
                "original_risk_level": (
                    (original.get("payload") or {}).get("risk_level")
                ),
                "original_legitimacy_score": (
                    (original.get("payload") or {}).get("legitimacy_score")
                ),
                "paper_only": True,
            },
            learning_exclusion_reason=(
                "market_outcome_not_observed"
                if checkpoint.get("status") != "observed"
                else (
                    "checkpoint_outside_learner_tolerance"
                    if not checkpoint.get("learning_eligible") else None
                )
            ),
        )
        reference = record["analysis_reference"]
        ring = self.tc.seal("robinhood_learning_outcome", {
            "summary": (
                f"Robinhood paper-learning {horizon} checkpoint for {token}; "
                f"learning eligibility is {record['learning']['eligible']}."
            ),
            "analysis_ring": original["index"],
            "analysis_ring_hash": original["ring_hash"],
            "original_evidence_hash": reference["original_evidence_hash"],
            "anchor_type": reference["anchor_type"],
            "anchor_value": reference["anchor_value"],
            "horizon": horizon,
            "token_address": token,
            "outcome_record": record,
            "idempotency_key": key,
            "paper_only": True,
            "live_execution_enabled": False,
        })
        TemporalGraphStore(self.root).refresh(self.tc.load())
        return ring

    def repair_invalid_outcomes(self) -> list[dict]:
        """Append canonical corrections for the historical post-hash defect."""
        rings = list(self.tc.load())
        by_index = {ring.get("index"): ring for ring in rings}
        already_superseded = {
            (ring.get("payload") or {}).get("outcome_correction", {}).get(
                "supersedes_ring"
            )
            for ring in rings
            if isinstance(
                (ring.get("payload") or {}).get("outcome_correction"), dict
            )
        }
        repaired: list[dict] = []
        for ring in rings:
            if ring.get("index") in already_superseded:
                continue
            payload = ring.get("payload") or {}
            record = payload.get("outcome_record")
            if not isinstance(record, dict):
                continue
            reference = record.get("analysis_reference") or {}
            analysis = by_index.get(reference.get("ring"))
            ok, reason = verify_outcome_record(record, analysis)
            if ok or reason != "outcome record hash mismatch":
                continue
            infrastructure = record.get("infrastructure_outcomes") or {}
            old_learning_reason = (record.get("learning") or {}).get("reason")
            if infrastructure.get("infrastructure_indeterminate"):
                exclusion_reason = "market_outcome_not_observed"
            elif old_learning_reason == "checkpoint_outside_learner_tolerance":
                exclusion_reason = "checkpoint_outside_learner_tolerance"
            else:
                # Refuse to guess at any other hash mismatch. It remains an
                # explicit verifier error until a bounded repair is designed.
                continue
            corrected, correction = build_outcome_correction(
                ring, analysis,
                learning_exclusion_reason=exclusion_reason,
            )
            correction_ring = self.tc.seal(
                "robinhood_learning_outcome_correction",
                {
                    "summary": (
                        "Append-only integrity correction for Robinhood "
                        f"outcome ring {ring.get('index')}."
                    ),
                    "analysis_ring": reference.get("ring"),
                    "analysis_ring_hash": reference.get("ring_hash"),
                    "token_address": payload.get("token_address"),
                    "horizon": payload.get("horizon"),
                    "outcome_record": corrected,
                    "outcome_correction": correction,
                    # Reuse the logical operation key. _find() searches newest
                    # first, so retries now resolve to the corrected ring.
                    "idempotency_key": payload.get("idempotency_key"),
                    "paper_only": True,
                    "live_execution_enabled": False,
                },
            )
            repaired.append(correction_ring)
        if repaired:
            TemporalGraphStore(self.root).refresh(self.tc.load())
        return repaired

    def verify(self) -> tuple[bool, str]:
        ok, report = self.tc.verify()
        return ok, "; ".join(report)


#: Refusal thresholds for the safety shadow rules. RECORDED, NEVER ENFORCED.
#: Every one of the 22 total losses was admitted carrying NO hard stop at all,
#: at Low (10) or Medium (12) risk -- EXTREME_CONCENTRATION never fired on a
#: single one. The gate reads legitimacy_score, risk_level and hard_stops, and
#: the score is a weighted blend in which holder_distribution carries 0.07 and
#: lp_lock 0.09, so a token can score 73.7 with holder_distribution at 35.
#: These rules read the component directly instead of the blend.
SHADOW_MAXIMUM_TOP_HOLDER_PCT = 25.0
SHADOW_MINIMUM_HOLDER_DISTRIBUTION_SCORE = 50.0


def extract_safety_signals(analysis: dict, data: dict | None = None) -> dict:
    """Pull the sub-signals out of an analysis payload, flat and typed.

    The analyzer already computes holder concentration, liquidity custody and
    sellability -- component_scores, holder_assessment, red_flags and the
    lp_lock data block all carry it -- and then blends them into one number
    the entry gate reads. Nothing here is new evidence; it is the evidence
    that was already being discarded at the point of decision.
    """
    components = dict(analysis.get("component_scores") or {})
    holders = dict(analysis.get("holder_assessment") or {})
    lp_lock = dict((data or {}).get("lp_lock") or {})

    top_holder_pct = None
    # The percentage is stated in the concentration stop and in red_flags; the
    # structured field is not always present, so parse whichever exists.
    for item in analysis.get("hard_stop_overrides") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("code")) == "EXTREME_CONCENTRATION":
            match = re.search(r"(\d+(?:\.\d+)?)\s*%", str(item.get("reason") or ""))
            if match:
                top_holder_pct = safe_float(match.group(1), 0.0) or None
    if top_holder_pct is None:
        for flag in analysis.get("red_flags") or []:
            match = re.search(
                r"top[^%]{0,40}?(\d+(?:\.\d+)?)\s*%", str(flag), re.IGNORECASE
            )
            if match:
                top_holder_pct = safe_float(match.group(1), 0.0) or None
                break
    if top_holder_pct is None:
        top_holder_pct = safe_float(holders.get("top_holder_pct"), 0.0) or None

    return {
        "top_holder_pct": top_holder_pct,
        "holder_distribution_score": safe_float(
            components.get("holder_distribution"), -1.0
        ) if "holder_distribution" in components else None,
        "lp_lock_score": safe_float(
            components.get("lp_lock"), -1.0
        ) if "lp_lock" in components else None,
        "honeypot_safety_score": safe_float(
            components.get("honeypot_safety"), -1.0
        ) if "honeypot_safety" in components else None,
        "security_score": safe_float(
            components.get("security"), -1.0
        ) if "security" in components else None,
        "holder_count": safe_float(holders.get("holder_count"), 0.0) or None,
        "lp_locked": lp_lock.get("locked"),
        "lp_lock_source": lp_lock.get("source"),
        "red_flag_count": len(analysis.get("red_flags") or []),
    }


#: Weights for the entry deliberation. Deliberately NOT the analyzer's own
#: weights, which put holder_distribution at 0.07 and lp_lock at 0.09 and let
#: a token with 85% top-holder concentration score 73.7. Measured on 30
#: observed closes the blended score is anti-predictive (r = -0.21, and the
#: 80-85 bucket returned 0.125x over 5 closes with zero winners), so the
#: blend earns almost no weight here and the raw safety tags carry the call.
#: A single wallet at or above this share can zero the token unilaterally, so
#: it overrides the weighted vote instead of joining it.
ENTRY_DELIBERATION_CONCENTRATION_VETO_PCT = 50.0
ENTRY_DELIBERATION_WEIGHTS = {
    "holder_concentration": 0.40,
    "liquidity_custody": 0.30,
    "sellability": 0.20,
    "blended_score": 0.10,
}


def deliberate_entry(
    safety: dict, *, score: float, risk_level: str, allowed: bool,
    enforced: bool = False,
) -> dict:
    """Weigh the individual safety tags at the entry decision. Records only.

    The flat gate asks three yes/no questions -- score above floor, risk in
    {Low, Medium}, no hard stops -- and every one of the 22 total losses
    answered all three correctly. This forks the decision across the tags the
    analyzer already produced and lets them disagree, so that a token which
    is nominally clean but 85%-concentrated and unlocked is visibly a
    different proposition from one that is neither.

    Returns the perspectives with their verdicts rather than only the
    conclusion, so a later collapse can be audited against what each tag
    actually said. Nothing here gates an entry.
    """
    perspectives: list[dict] = []

    def _add(name, kind, verdict, weight, note):
        perspectives.append({
            "name": name, "kind": kind, "verdict": verdict,
            "weight": weight, "note": note,
        })

    top_holder = safety.get("top_holder_pct")
    distribution = safety.get("holder_distribution_score")
    if top_holder is None and distribution is None:
        _add("Holder concentration", "unknown", "abstain",
             ENTRY_DELIBERATION_WEIGHTS["holder_concentration"],
             "no concentration evidence was produced for this token")
    else:
        concentrated = (
            (top_holder is not None and top_holder > SHADOW_MAXIMUM_TOP_HOLDER_PCT)
            or (
                distribution is not None
                and distribution < SHADOW_MINIMUM_HOLDER_DISTRIBUTION_SCORE
            )
        )
        _add("Holder concentration", "safety",
             "refuse" if concentrated else "admit",
             ENTRY_DELIBERATION_WEIGHTS["holder_concentration"],
             f"top holder {top_holder}%, distribution score {distribution}")

    locked = safety.get("lp_locked")
    if locked is None:
        _add("Liquidity custody", "unknown", "abstain",
             ENTRY_DELIBERATION_WEIGHTS["liquidity_custody"],
             "LP custody could not be verified")
    else:
        _add("Liquidity custody", "safety", "admit" if locked else "refuse",
             ENTRY_DELIBERATION_WEIGHTS["liquidity_custody"],
             f"lp_locked={locked} via {safety.get('lp_lock_source') or 'unknown source'}")

    honeypot = safety.get("honeypot_safety_score")
    if honeypot is None:
        _add("Sellability", "unknown", "abstain",
             ENTRY_DELIBERATION_WEIGHTS["sellability"],
             "no honeypot/sellability score was produced")
    else:
        _add("Sellability", "safety", "admit" if honeypot >= 80.0 else "refuse",
             ENTRY_DELIBERATION_WEIGHTS["sellability"],
             f"honeypot_safety={honeypot}")

    _add("Blended score", "legacy",
         "admit" if allowed else "refuse",
         ENTRY_DELIBERATION_WEIGHTS["blended_score"],
         f"score {score} at risk {risk_level}; anti-predictive on measured closes")

    refuse = sum(p["weight"] for p in perspectives if p["verdict"] == "refuse")
    admit = sum(p["weight"] for p in perspectives if p["verdict"] == "admit")
    abstain = sum(p["weight"] for p in perspectives if p["verdict"] == "abstain")
    # Severe concentration is a veto rather than a vote. A wallet holding most
    # of the supply can take the price to zero on its own, and locked
    # liquidity or a clean sellability check does not constrain it -- so those
    # tags cannot outvote it the way a plain weighted sum would allow.
    veto = bool(
        top_holder is not None
        and top_holder >= ENTRY_DELIBERATION_CONCENTRATION_VETO_PCT
    )
    # Unknown safety evidence is not consent. A token whose tags could not be
    # read is held out rather than waved through on the blend alone, which is
    # exactly how the flat gate admitted a book of clean-looking rugs.
    if veto:
        judgment = "refuse"
    elif abstain >= 0.5:
        judgment = "abstain"
    elif refuse > admit:
        judgment = "refuse"
    else:
        judgment = "admit"
    return {
        "perspectives": perspectives,
        "concentration_veto": veto,
        "weight_refuse": round(refuse, 4),
        "weight_admit": round(admit, 4),
        "weight_abstain": round(abstain, 4),
        "judgment": judgment,
        "refusal_reasons": (
            ["Holder concentration (veto)"] if veto else []
        ) + [
            p["name"] for p in perspectives
            if p["verdict"] == "refuse" and not (
                veto and p["name"] == "Holder concentration"
            )
        ],
        "agrees_with_gate": (judgment == "admit") == bool(allowed),
        "enforced": bool(enforced),
    }


def shadow_admission(
    *,
    market_cap: float | None,
    liquidity: float,
    hooks_present: bool | None,
    hard_stops: list,
    token_quality_passes: bool,
    allowed: bool,
    counterfactual_allowed: bool | None = None,
    backfilled: bool = False,
    safety: dict | None = None,
) -> dict:
    """What competing admission rules WOULD have decided. Never enforced.

    Each field is a counterfactual on the same candidate at the same instant,
    so the comparison against the real outcome is like-for-like. Recording
    them costs nothing and is the only way to find out whether a rule fitted
    on 39 pools survives contact with pools it has not seen.

    The rules under test, in the order they would be adopted:
      ratio_pass          -- market cap / entry liquidity within the cut
      hook_relaxed        -- the V4 hook stop demoted from block to penalty
      combined            -- both, which is the Phase 3 candidate policy
    """
    basis_allowed = bool(
        allowed if counterfactual_allowed is None else counterfactual_allowed
    )
    ratio = (
        market_cap / liquidity
        if market_cap and liquidity and liquidity > 0 else None
    )
    ratio_pass = ratio is not None and ratio <= SHADOW_MAXIMUM_MCAP_LIQUIDITY_RATIO
    codes = {
        (item.get("code") or item.get("reason")) if isinstance(item, dict) else str(item)
        for item in hard_stops or []
    }
    stops_without_hook = codes - {V4_HOOK_STOP_CODE}
    hook_was_the_only_stop = bool(codes) and not stops_without_hook
    # "Would admit" reuses the live preconditions and swaps one term at a time,
    # so a difference in the result is attributable to the rule and not to some
    # other gate moving underneath it.
    base_ok = bool(token_quality_passes and liquidity >= MINIMUM_ENTRY_LIQUIDITY_USD)
    # --- safety sub-signal rules -------------------------------------------
    # Absent evidence is NOT a pass. Every rug was admitted on a clean-looking
    # record, so a rule that treats "not measured" as "fine" would have
    # admitted all 22 of them too and learned nothing.
    signals = dict(safety or {})
    top_holder = signals.get("top_holder_pct")
    distribution = signals.get("holder_distribution_score")
    concentration_known = top_holder is not None or distribution is not None
    concentration_pass = bool(
        concentration_known
        and (top_holder is None or top_holder <= SHADOW_MAXIMUM_TOP_HOLDER_PCT)
        and (
            distribution is None
            or distribution >= SHADOW_MINIMUM_HOLDER_DISTRIBUTION_SCORE
        )
    )
    lp_locked = signals.get("lp_locked")
    liquidity_custody_pass = lp_locked is True
    return {
        "mcap_liquidity_ratio": ratio,
        "ratio_pass": ratio_pass,
        "hooks_present": hooks_present,
        "hook_was_the_only_stop": hook_was_the_only_stop,
        "actual_admitted": bool(allowed),
        "would_admit_legacy_gate": basis_allowed,
        "would_admit_ratio_rule": bool(basis_allowed and ratio_pass),
        "would_admit_hook_relaxed": bool(
            base_ok and not stops_without_hook and market_cap is not None
        ),
        "would_admit_combined": bool(
            base_ok and not stops_without_hook and market_cap is not None
            and ratio_pass
        ),
        # --- safety variants -----------------------------------------------
        "top_holder_pct": top_holder,
        "holder_distribution_score": distribution,
        "concentration_evidence_present": concentration_known,
        "concentration_pass": concentration_pass,
        "liquidity_custody_pass": liquidity_custody_pass,
        "would_admit_concentration_rule": bool(basis_allowed and concentration_pass),
        "would_admit_liquidity_lock_rule": bool(
            basis_allowed and liquidity_custody_pass
        ),
        "would_admit_safety_combined": bool(
            basis_allowed and concentration_pass and liquidity_custody_pass
        ),
        "safety_thresholds": {
            "maximum_top_holder_pct": SHADOW_MAXIMUM_TOP_HOLDER_PCT,
            "minimum_holder_distribution_score":
                SHADOW_MINIMUM_HOLDER_DISTRIBUTION_SCORE,
        },
        "threshold": SHADOW_MAXIMUM_MCAP_LIQUIDITY_RATIO,
        "enforced": False,
        # The threshold was chosen by looking at the candidates that already
        # existed when it was written, so their verdicts are fitted, not
        # predicted. Marking them keeps the two populations from being added
        # together -- the whole value of this exercise is the out-of-sample
        # count, and it starts at zero.
        "backfilled": bool(backfilled),
    }


def v4_shadow_gates(market: dict, *, hooks_present: bool) -> dict:
    """Keep custody, hooks and executability as independent shadow gates."""
    custody = dict(market.get("v4_custody") or {})
    quote = dict(market.get("execution_quote") or {})
    coverage = safe_float(custody.get("managed_active_coverage"), 0.0)
    locked = safe_float(custody.get("verified_locked_active_fraction"), 0.0)
    approved = safe_float(custody.get("approved_active_fraction"), 0.0)
    gates = {
        "custody_snapshot_present": bool(custody),
        "managed_active_coverage": bool(
            custody and coverage >= V4_CUSTODY_MINIMUM_MANAGED_ACTIVE_COVERAGE
        ),
        "verified_locked_active_share": bool(
            custody and locked >= V4_CUSTODY_MINIMUM_LOCKED_ACTIVE_FRACTION
        ),
        "no_token_specific_approval": bool(custody and approved == 0),
        "hook_absent": not hooks_present,
        "round_trip_quote_verified": bool(
            market.get("executable_quote_verified")
        ),
        "round_trip_quote_within_limit": bool(
            quote.get("passes_round_trip_limit")
        ),
    }
    return {
        "policy_version": "v4-position-custody-shadow-v1",
        "gates": gates,
        "counterfactual_pass": all(gates.values()),
        # Enabling a canary is a later, explicit policy decision. Evidence can
        # satisfy every gate without changing paper admission in this build.
        "paper_canary_enabled": False,
        "custody": custody,
        "thresholds": {
            "minimum_managed_active_coverage": (
                V4_CUSTODY_MINIMUM_MANAGED_ACTIVE_COVERAGE
            ),
            "minimum_verified_locked_active_fraction": (
                V4_CUSTODY_MINIMUM_LOCKED_ACTIVE_FRACTION
            ),
            "maximum_round_trip_loss": V4_MAXIMUM_ROUND_TRIP_LOSS,
        },
    }


class OutcomeLedgerObservationMissing(KeyError):
    """Classification referenced an observation that was never sealed."""


class RobinhoodLearningStore:
    def __init__(self, path: str | Path, *, read_only: bool = False):
        """read_only opens the database for reading and runs no migrations.

        The dashboard announces itself as read-only but was constructing a
        full store, and a store constructor MIGRATES -- it creates tables,
        adds columns and builds indexes. Those are writes, so opening the
        dashboard while a learning cycle held the write lock blocked until
        the cycle finished. Measured: dashboard_snapshot took 43.6 seconds
        and /api/status timed out entirely, while the underlying queries all
        returned in under 2 seconds when run against a read-only connection.
        The slowness was never the queries; it was waiting for a writer.
        """
        self.path = Path(path)
        self.read_only = bool(read_only)
        if not self.read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._initialize()

    def _connect(self) -> sqlite3.Connection:
        if self.read_only:
            # A reader in WAL mode never blocks on a writer, so the dashboard
            # stays responsive mid-cycle instead of queueing behind it.
            connection = sqlite3.connect(
                f"file:{self.path.as_posix()}?mode=ro", uri=True, timeout=10,
            )
        else:
            connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextmanager
    def connection(self):
        connection = self._connect()
        try:
            if self.read_only:
                # `with connection` opens a transaction that COMMITs on exit,
                # which a read-only handle cannot do.
                yield connection
            else:
                with connection:
                    yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connection() as connection:
            # WAL mode persists at the database level. Setting it on every
            # read connection can itself request an exclusive schema lock and
            # make the read-only dashboard collide with a learner write.
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS candidates (
                    token_address TEXT PRIMARY KEY,
                    pair_address TEXT NOT NULL,
                    factory_address TEXT NOT NULL,
                    block_number INTEGER NOT NULL,
                    block_timestamp REAL NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    log_index INTEGER NOT NULL,
                    name TEXT,
                    symbol TEXT,
                    analysis_status TEXT NOT NULL DEFAULT 'pending',
                    analysis_attempts INTEGER NOT NULL DEFAULT 0,
                    analyzed_at TEXT,
                    outcome_anchor_at REAL,
                    score REAL,
                    risk_level TEXT,
                    action_label TEXT,
                    hard_stops_json TEXT NOT NULL DEFAULT '[]',
                    paper_entry_allowed INTEGER,
                    entry_price_usd REAL,
                    entry_liquidity_usd REAL,
                    first_market_cap_usd REAL,
                    peak_market_cap_usd REAL,
                    peak_fdv_usd REAL,
                    last_observed_at TEXT,
                    last_outcome_attempt_at REAL,
                    producer_analysis_ring_index INTEGER,
                    producer_analysis_ring_hash TEXT,
                    producer_evidence_hash TEXT,
                    discovered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS checkpoints (
                    token_address TEXT NOT NULL,
                    horizon_label TEXT NOT NULL,
                    horizon_seconds INTEGER NOT NULL,
                    target_at REAL NOT NULL,
                    anchor_type TEXT NOT NULL DEFAULT 'legacy_launch',
                    anchor_at REAL,
                    observed_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    learning_eligible INTEGER NOT NULL,
                    lateness_seconds REAL NOT NULL,
                    price_usd REAL,
                    liquidity_usd REAL,
                    market_cap_usd REAL,
                    fdv_usd REAL,
                    market_cap_multiple REAL,
                    maximum_favorable_excursion_pct REAL,
                    market_evidence_json TEXT NOT NULL DEFAULT '{}',
                    producer_outcome_ring_index INTEGER,
                    producer_outcome_ring_hash TEXT,
                    producer_outcome_record_hash TEXT,
                    PRIMARY KEY (token_address, horizon_label)
                );
                CREATE TABLE IF NOT EXISTS positions (
                    token_address TEXT PRIMARY KEY,
                    symbol TEXT,
                    status TEXT NOT NULL,
                    opened_at REAL NOT NULL,
                    entry_price_usd REAL NOT NULL,
                    entry_liquidity_usd REAL NOT NULL,
                    cost_usd REAL NOT NULL,
                    quantity REAL NOT NULL,
                    entry_friction_bps REAL NOT NULL,
                    high_multiple REAL NOT NULL DEFAULT 1,
                    last_price_usd REAL,
                    last_liquidity_usd REAL,
                    last_mark_at REAL,
                    verified_mark_count INTEGER NOT NULL DEFAULT 0,
                    unverified_marks INTEGER NOT NULL DEFAULT 0,
                    consecutive_unverified_marks INTEGER NOT NULL DEFAULT 0,
                    last_unverified_mark_at REAL,
                    last_verified_mark_at REAL,
                    exit_price_usd REAL,
                    exit_value_usd REAL,
                    exit_reason TEXT,
                    closed_at REAL,
                    net_multiple REAL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    summary_json TEXT
                );
                CREATE TABLE IF NOT EXISTS position_policy_states (
                    token_address TEXT NOT NULL,
                    policy TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    high_multiple REAL NOT NULL DEFAULT 1,
                    exit_price_usd REAL,
                    exit_value_usd REAL,
                    exit_reason TEXT,
                    closed_at REAL,
                    net_multiple REAL,
                    last_mark_at REAL,
                    PRIMARY KEY (token_address, policy)
                );
                CREATE TABLE IF NOT EXISTS v4_pools (
                    pool_id TEXT PRIMARY KEY,
                    currency0 TEXT NOT NULL,
                    currency1 TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    anchor_address TEXT NOT NULL,
                    fee_tier INTEGER NOT NULL,
                    tick_spacing INTEGER NOT NULL,
                    hooks_address TEXT NOT NULL,
                    initialized_block INTEGER NOT NULL,
                    modified_block INTEGER,
                    swapped_block INTEGER,
                    swap_timestamp REAL,
                    transaction_hash TEXT,
                    log_index INTEGER,
                    sqrt_price_x96 TEXT,
                    active_liquidity TEXT,
                    last_tick INTEGER,
                    promoted INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS swap_observations (
                    source_version TEXT NOT NULL,
                    pool_id TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    block_number INTEGER NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    log_index INTEGER NOT NULL,
                    sender_hint TEXT,
                    sender_identity_kind TEXT NOT NULL,
                    resolved_participant TEXT,
                    participant_identity_kind TEXT,
                    identity_resolved_at TEXT,
                    amount0_raw TEXT NOT NULL,
                    amount1_raw TEXT NOT NULL,
                    anchor_delta_raw TEXT NOT NULL,
                    token_delta_raw TEXT NOT NULL,
                    side TEXT NOT NULL,
                    sqrt_price_x96 TEXT,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY (source_version,pool_id,transaction_hash,log_index)
                );
                CREATE TABLE IF NOT EXISTS flow_signals (
                    source_version TEXT NOT NULL,
                    pool_id TEXT PRIMARY KEY,
                    token_address TEXT NOT NULL,
                    computed_at TEXT NOT NULL,
                    window_blocks INTEGER NOT NULL,
                    window_start_block INTEGER NOT NULL,
                    window_end_block INTEGER NOT NULL,
                    swap_count INTEGER NOT NULL,
                    buy_count INTEGER NOT NULL,
                    sell_count INTEGER NOT NULL,
                    unique_sender_hints INTEGER NOT NULL,
                    unique_resolved_participants INTEGER NOT NULL DEFAULT 0,
                    identity_coverage REAL NOT NULL DEFAULT 0,
                    buy_ratio REAL NOT NULL,
                    net_anchor_flow_fraction REAL NOT NULL,
                    price_multiple REAL,
                    uncapped_shadow_score REAL NOT NULL DEFAULT 0,
                    shadow_score REAL NOT NULL,
                    shadow_qualified INTEGER NOT NULL,
                    confidence TEXT NOT NULL,
                    qualification_gaps_json TEXT NOT NULL DEFAULT '[]',
                    limitations_json TEXT NOT NULL,
                    features_json TEXT NOT NULL
                );
                -- Append-only history of every computed window.
                --
                -- flow_signals is keyed on pool_id alone and upserts, so it is
                -- a bounded LATEST-STATE table: 773 rows for 773 pools, each
                -- overwritten every cycle. That is the right shape for
                -- dashboards and identity work, but it means the row for a
                -- pool always sits at that pool's most recent swap, so a dead
                -- pool is preserved at its terminal window while live pools
                -- keep moving. Retrospective analysis over those rows is
                -- survivorship-biased and produced a spurious 18% direction
                -- anti-correlation.
                --
                -- This table keeps the series. INSERT OR IGNORE on the
                -- composite key, so recomputing a window is idempotent and the
                -- latest-state table keeps its one-row-per-pool contract.
                CREATE TABLE IF NOT EXISTS flow_signal_windows (
                    policy_version TEXT NOT NULL,
                    pool_id TEXT NOT NULL,
                    source_version TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    computed_at TEXT NOT NULL,
                    window_blocks INTEGER NOT NULL,
                    window_start_block INTEGER NOT NULL,
                    window_end_block INTEGER NOT NULL,
                    swap_count INTEGER NOT NULL,
                    buy_count INTEGER NOT NULL,
                    sell_count INTEGER NOT NULL,
                    unique_sender_hints INTEGER NOT NULL,
                    unique_resolved_participants INTEGER NOT NULL,
                    identity_coverage REAL NOT NULL,
                    buy_ratio REAL NOT NULL,
                    net_anchor_flow_fraction REAL NOT NULL,
                    price_multiple REAL,
                    uncapped_shadow_score REAL NOT NULL,
                    shadow_score REAL NOT NULL,
                    shadow_qualified INTEGER NOT NULL,
                    confidence TEXT NOT NULL,
                    qualification_gaps_json TEXT NOT NULL,
                    features_json TEXT NOT NULL,
                    PRIMARY KEY (policy_version, pool_id, window_end_block)
                );
                CREATE INDEX IF NOT EXISTS idx_flow_windows_recent
                    ON flow_signal_windows(window_end_block);
                CREATE INDEX IF NOT EXISTS idx_flow_windows_qualified
                    ON flow_signal_windows(shadow_qualified,window_end_block);
                -- The dashboard's three slowest reads all group or filter
                -- swap_observations by transaction_hash across 487k rows with
                -- no index for it: pending_transaction_origin_counts took
                -- 21.2s, flow_summary 19.6s and v4_custody_summary 14.7s,
                -- which is why /api/status timed out.
                CREATE INDEX IF NOT EXISTS idx_swap_observation_transaction
                    ON swap_observations(transaction_hash);
                CREATE INDEX IF NOT EXISTS idx_swap_observation_participant
                    ON swap_observations(resolved_participant);
                -- Sealed the instant a near-head window is detected, BEFORE
                -- any enrichment. Nothing in this table is ever updated: an
                -- observation is a claim about a moment, and a claim that can
                -- be edited after its outcome is known is not evidence.
                -- Classification lands in a separate table so the original
                -- cannot be rewritten by what we later learn.
                CREATE TABLE IF NOT EXISTS flow_observations (
                    observation_id TEXT PRIMARY KEY,
                    policy_version TEXT NOT NULL,
                    pool_id TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    observation_head INTEGER NOT NULL,
                    observation_head_lag_blocks INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    observed_at_epoch REAL NOT NULL,
                    window_start_block INTEGER NOT NULL,
                    window_end_block INTEGER NOT NULL,
                    transaction_set_hash TEXT NOT NULL,
                    transaction_count INTEGER NOT NULL,
                    features_json TEXT NOT NULL,
                    quote_json TEXT NOT NULL,
                    quote_block INTEGER,
                    quote_verified INTEGER NOT NULL DEFAULT 0,
                    sealed_at TEXT NOT NULL,
                    cohort_id TEXT NOT NULL DEFAULT 'pilot'
                );
                CREATE TABLE IF NOT EXISTS flow_observation_classifications (
                    observation_id TEXT PRIMARY KEY,
                    classified_at TEXT NOT NULL,
                    decision_head INTEGER NOT NULL,
                    decision_head_lag_blocks INTEGER NOT NULL,
                    identity_tier TEXT NOT NULL,
                    identity_coverage REAL,
                    arm TEXT NOT NULL,
                    gates_json TEXT NOT NULL,
                    research_eligible INTEGER NOT NULL,
                    paper_eligible INTEGER NOT NULL,
                    decision_quote_verified INTEGER,
                    decision_price_drift_bps REAL,
                    FOREIGN KEY(observation_id)
                        REFERENCES flow_observations(observation_id)
                );
                CREATE TABLE IF NOT EXISTS flow_observation_outcomes (
                    observation_id TEXT NOT NULL,
                    horizon_label TEXT NOT NULL,
                    horizon_seconds INTEGER NOT NULL,
                    target_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    observed_at REAL,
                    quote_block INTEGER,
                    net_return REAL,
                    exit_valid INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (observation_id, horizon_label),
                    FOREIGN KEY(observation_id)
                        REFERENCES flow_observations(observation_id)
                );
                CREATE INDEX IF NOT EXISTS idx_flow_obs_due
                    ON flow_observation_outcomes(status,target_at);
                CREATE TABLE IF NOT EXISTS flow_signal_events (
                    event_id TEXT PRIMARY KEY,
                    policy_version TEXT NOT NULL,
                    cohort_id TEXT NOT NULL,
                    source_version TEXT NOT NULL,
                    pool_id TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    signal_role TEXT NOT NULL,
                    matched_signal_event_id TEXT,
                    window_start_block INTEGER NOT NULL,
                    window_end_block INTEGER NOT NULL,
                    head_block INTEGER NOT NULL,
                    head_lag_blocks INTEGER NOT NULL,
                    freshness TEXT NOT NULL,
                    eligible_for_evaluation INTEGER NOT NULL,
                    signaled_at REAL NOT NULL,
                    snapshot_json TEXT NOT NULL,
                    quote_status TEXT NOT NULL DEFAULT 'pending',
                    quote_block INTEGER,
                    quote_json TEXT,
                    quote_verified INTEGER NOT NULL DEFAULT 0,
                    quote_exitable INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    UNIQUE(policy_version,pool_id,window_end_block,signal_role)
                );
                CREATE TABLE IF NOT EXISTS flow_signal_outcomes (
                    event_id TEXT NOT NULL,
                    horizon_label TEXT NOT NULL,
                    horizon_seconds INTEGER NOT NULL,
                    target_at REAL NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    observed_at REAL,
                    quote_block INTEGER,
                    quote_json TEXT,
                    quote_verified INTEGER NOT NULL DEFAULT 0,
                    exit_valid INTEGER NOT NULL DEFAULT 0,
                    net_return REAL,
                    maximum_favorable_excursion REAL,
                    maximum_adverse_excursion REAL,
                    liquidity_usd REAL,
                    error TEXT,
                    PRIMARY KEY(event_id,horizon_label),
                    FOREIGN KEY(event_id) REFERENCES flow_signal_events(event_id)
                );
                CREATE INDEX IF NOT EXISTS idx_flow_event_evaluation
                    ON flow_signal_events(eligible_for_evaluation,signal_role,signaled_at);
                CREATE INDEX IF NOT EXISTS idx_flow_outcome_due
                    ON flow_signal_outcomes(status,target_at);
                CREATE TABLE IF NOT EXISTS transaction_origins (
                    transaction_hash TEXT PRIMARY KEY,
                    origin_address TEXT,
                    destination_address TEXT,
                    block_number INTEGER,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at REAL NOT NULL,
                    resolved_at TEXT,
                    error TEXT
                );
                CREATE TABLE IF NOT EXISTS v4_positions (
                    token_id TEXT PRIMARY KEY,
                    pool_id TEXT NOT NULL,
                    tick_lower INTEGER NOT NULL,
                    tick_upper INTEGER NOT NULL,
                    owner_address TEXT,
                    approved_address TEXT,
                    liquidity_raw TEXT NOT NULL DEFAULT '0',
                    in_active_range INTEGER NOT NULL DEFAULT 0,
                    custody_class TEXT NOT NULL DEFAULT 'unverified',
                    locker_platform TEXT,
                    unlock_timestamp REAL,
                    owner_code_sha256 TEXT,
                    last_transfer_block INTEGER,
                    last_checked_block INTEGER,
                    last_checked_at TEXT,
                    evidence_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(pool_id) REFERENCES v4_pools(pool_id)
                );
                CREATE TABLE IF NOT EXISTS v4_position_transfers (
                    transaction_hash TEXT NOT NULL,
                    log_index INTEGER NOT NULL,
                    token_id TEXT NOT NULL,
                    pool_id TEXT NOT NULL,
                    from_address TEXT NOT NULL,
                    to_address TEXT NOT NULL,
                    block_number INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    PRIMARY KEY(transaction_hash,log_index)
                );
                CREATE TABLE IF NOT EXISTS v4_custody_snapshots (
                    pool_id TEXT NOT NULL,
                    observed_block INTEGER NOT NULL,
                    observed_at TEXT NOT NULL,
                    current_tick INTEGER,
                    core_active_liquidity_raw TEXT NOT NULL,
                    tracked_positions INTEGER NOT NULL,
                    tracked_active_positions INTEGER NOT NULL,
                    managed_active_liquidity_raw TEXT NOT NULL,
                    verified_locked_active_liquidity_raw TEXT NOT NULL,
                    contract_unverified_active_liquidity_raw TEXT NOT NULL,
                    eoa_active_liquidity_raw TEXT NOT NULL,
                    approved_active_liquidity_raw TEXT NOT NULL,
                    managed_active_coverage REAL NOT NULL,
                    verified_locked_active_fraction REAL NOT NULL,
                    approved_active_fraction REAL NOT NULL,
                    custody_verdict TEXT NOT NULL,
                    evidence_json TEXT NOT NULL,
                    PRIMARY KEY(pool_id,observed_block)
                );
                CREATE INDEX IF NOT EXISTS idx_candidates_analysis
                    ON candidates(analysis_status, block_number);
                CREATE INDEX IF NOT EXISTS idx_checkpoints_status
                    ON checkpoints(status, horizon_label);
                CREATE INDEX IF NOT EXISTS idx_positions_status
                    ON positions(status, opened_at);
                CREATE INDEX IF NOT EXISTS idx_position_policy_status
                    ON position_policy_states(status, policy);
                CREATE INDEX IF NOT EXISTS idx_v4_activation
                    ON v4_pools(promoted, modified_block, swapped_block);
                CREATE INDEX IF NOT EXISTS idx_swap_observation_pool_block
                    ON swap_observations(pool_id,block_number);
                CREATE INDEX IF NOT EXISTS idx_flow_signal_priority
                    ON flow_signals(shadow_qualified,shadow_score);
                CREATE INDEX IF NOT EXISTS idx_transaction_origin_status
                    ON transaction_origins(status,last_attempt_at);
                CREATE INDEX IF NOT EXISTS idx_v4_positions_pool
                    ON v4_positions(pool_id,in_active_range,custody_class);
                CREATE INDEX IF NOT EXISTS idx_v4_positions_refresh
                    ON v4_positions(last_checked_block,token_id);
                CREATE INDEX IF NOT EXISTS idx_v4_custody_observed
                    ON v4_custody_snapshots(observed_at,pool_id);
                """
            )
            columns = {row[1] for row in connection.execute("PRAGMA table_info(candidates)")}
            migrations = {
                "source_version": "TEXT NOT NULL DEFAULT 'uniswap_v2'",
                "pool_id": "TEXT",
                "hooks_address": "TEXT",
                "fee_tier": "INTEGER",
                "tick_spacing": "INTEGER",
                "market_evidence_json": "TEXT NOT NULL DEFAULT '{}'",
                "analysis_priority_reason": "TEXT",
                "analysis_queue_age_seconds": "REAL",
                "paper_decision": "TEXT",
                "market_watch_started_at": "REAL",
                "market_watch_last_checked_at": "REAL",
                "market_watch_checks": "INTEGER NOT NULL DEFAULT 0",
                "market_watch_expires_at": "REAL",
                "market_watch_reason": "TEXT",
                "market_watch_reference_price_usd": "REAL",
                "market_watch_reference_market_cap_usd": "REAL",
                "market_watch_reference_at": "REAL",
                "mcap_liquidity_ratio": "REAL",
                "hooks_present": "INTEGER",
                "shadow_admission_json": "TEXT",
                "safety_signals_json": "TEXT",
                "top_holder_pct": "REAL",
                "holder_distribution_score": "REAL",
                "lp_lock_score": "REAL",
                "producer_analysis_ring_index": "INTEGER",
                "producer_analysis_ring_hash": "TEXT",
                "producer_evidence_hash": "TEXT",
                "outcome_anchor_at": "REAL",
            }
            for name, declaration in migrations.items():
                if name not in columns:
                    connection.execute(f"ALTER TABLE candidates ADD COLUMN {name} {declaration}")
            observation_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_observations)"
                )
            }
            for name, decl in {
                # Every near-head window is sealed, qualified or not, which is
                # correct -- unqualified windows ARE the control arm. But they
                # were unlabelled, and 43 resolved observations with a -0.70
                # mean were read as a Flow result when not one of them had
                # qualified. Role makes that impossible to misread.
                "role": "TEXT NOT NULL DEFAULT 'matched_control'",
                "matched_observation_id": "TEXT",
                "qualification_gap_count": "INTEGER",
                # Same-block buy-and-sell on the sealed quote: pure friction.
                "round_trip_return": "REAL",
            }.items():
                if name not in observation_columns:
                    connection.execute(
                        f"ALTER TABLE flow_observations ADD COLUMN {name} {decl}"
                    )
            if "cohort_id" not in observation_columns:
                # Observations sealed before the cohort existed are the pilot:
                # preserved for diagnostics, excluded from promotion.
                connection.execute(
                    "ALTER TABLE flow_observations ADD COLUMN cohort_id"
                    " TEXT NOT NULL DEFAULT 'pilot'"
                )
            classification_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_observation_classifications)"
                )
            }
            for name, decl in {
                # Decision freshness stopped being a block count: the entry
                # price is re-taken at decision time instead. Both facts are
                # recorded so a threshold can later be set from the cohort's
                # own drift distribution rather than guessed.
                "decision_quote_verified": "INTEGER",
                "decision_price_drift_bps": "REAL",
            }.items():
                if name not in classification_columns:
                    connection.execute(
                        "ALTER TABLE flow_observation_classifications"
                        f" ADD COLUMN {name} {decl}"
                    )
            outcome_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_observation_outcomes)"
                )
            }
            for name, decl in {
                # The residual -- realised minus the same-block friction the
                # position started with -- is the only place an edge could
                # live, and it was being re-derived by hand from the sealed
                # quote every time the question came up. Three separate
                # reconstructions in one night is three chances to disagree.
                "friction_return": "REAL",
                "residual_return": "REAL",
            }.items():
                if name not in outcome_columns:
                    connection.execute(
                        "ALTER TABLE flow_observation_outcomes"
                        f" ADD COLUMN {name} {decl}"
                    )
            event_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_signal_events)"
                )
            }
            # The stale arm: windows that WERE inside the freshness bound when
            # observed and outside it by the time the decision ran. They were
            # being discarded, which threw away the only evidence that could
            # say whether the 120-block bound earns its cost -- six per cycle,
            # on identical qualification criteria. They are now captured and
            # scheduled for outcomes so fresh and stale can be compared, but
            # held out of paper entry, which is a separate permission.
            for name, declaration in {
                "arm": "TEXT NOT NULL DEFAULT 'fresh'",
                "paper_eligible": "INTEGER NOT NULL DEFAULT 0",
                "ingest_head_block": "INTEGER",
                "ingest_head_lag_blocks": "INTEGER",
            }.items():
                if name not in event_columns:
                    connection.execute(
                        f"ALTER TABLE flow_signal_events ADD COLUMN {name} {declaration}"
                    )
            # Indexed after the ALTER, since arm does not exist until then.
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_flow_events_arm"
                " ON flow_signal_events(arm,signaled_at)"
            )
            position_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(positions)")
            }
            position_migrations = {
                "entry_market_cap_usd": "REAL",
                "last_market_cap_usd": "REAL",
                "last_market_observed_at": "TEXT",
                "original_quantity": "REAL",
                "realized_value_usd": "REAL NOT NULL DEFAULT 0",
                "stage_one_sold_at": "REAL",
                "stage_two_sold_at": "REAL",
                "runner_high_multiple": "REAL",
                "entry_policy_version": "TEXT",
                "grandfathered_above_cap": "INTEGER NOT NULL DEFAULT 0",
                "entry_pool_reserve_fraction": "REAL",
                "last_pool_reserve_fraction": "REAL",
                "minimum_pool_reserve_fraction": "REAL",
                "verified_mark_count": "INTEGER NOT NULL DEFAULT 0",
                "unverified_marks": "INTEGER NOT NULL DEFAULT 0",
                "consecutive_unverified_marks": "INTEGER NOT NULL DEFAULT 0",
                "last_unverified_mark_at": "REAL",
                "last_verified_mark_at": "REAL",
            }
            for name, declaration in position_migrations.items():
                if name not in position_columns:
                    connection.execute(
                        f"ALTER TABLE positions ADD COLUMN {name} {declaration}"
                    )
            swap_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(swap_observations)"
                )
            }
            for name, declaration in {
                "resolved_participant": "TEXT",
                "participant_identity_kind": "TEXT",
                "identity_resolved_at": "TEXT",
            }.items():
                if name not in swap_columns:
                    connection.execute(
                        f"ALTER TABLE swap_observations ADD COLUMN {name} {declaration}"
                    )
            flow_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_signals)"
                )
            }
            for name, declaration in {
                "unique_resolved_participants": "INTEGER NOT NULL DEFAULT 0",
                "identity_coverage": "REAL NOT NULL DEFAULT 0",
                "uncapped_shadow_score": "REAL NOT NULL DEFAULT 0",
                "qualification_gaps_json": "TEXT NOT NULL DEFAULT '[]'",
            }.items():
                if name not in flow_columns:
                    connection.execute(
                        f"ALTER TABLE flow_signals ADD COLUMN {name} {declaration}"
                    )
            checkpoint_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(checkpoints)"
                )
            }
            for name, declaration in {
                "market_evidence_json": "TEXT NOT NULL DEFAULT '{}'",
                "producer_outcome_ring_index": "INTEGER",
                "producer_outcome_ring_hash": "TEXT",
                "producer_outcome_record_hash": "TEXT",
                "anchor_type": "TEXT NOT NULL DEFAULT 'legacy_launch'",
                "anchor_at": "REAL",
            }.items():
                if name not in checkpoint_columns:
                    connection.execute(
                        f"ALTER TABLE checkpoints ADD COLUMN {name} {declaration}"
                    )
            connection.execute(
                """
                UPDATE flow_signals
                SET uncapped_shadow_score=shadow_score
                WHERE uncapped_shadow_score=0 AND shadow_score<>0
                """
            )
            connection.execute(
                """
                UPDATE candidates
                SET outcome_anchor_at=CAST(strftime('%s',analyzed_at) AS REAL)
                WHERE outcome_anchor_at IS NULL
                  AND producer_analysis_ring_index IS NOT NULL
                  AND analysis_status='complete' AND analyzed_at IS NOT NULL
                """
            )
            connection.execute(
                """
                UPDATE positions SET
                    original_quantity=COALESCE(original_quantity,quantity),
                    realized_value_usd=COALESCE(realized_value_usd,0),
                    runner_high_multiple=COALESCE(runner_high_multiple,high_multiple,1),
                    entry_policy_version=COALESCE(entry_policy_version,'legacy_grandfathered'),
                    grandfathered_above_cap=CASE
                        WHEN entry_market_cap_usd>? THEN 1
                        ELSE COALESCE(grandfathered_above_cap,0)
                    END
                """,
                (MAXIMUM_ENTRY_MARKET_CAP_USD,),
            )
            # Older rows used last_mark_at for both the entry mark and later
            # observations. A mark more than one second after opening is the
            # conservative evidence available for migrating historical rows.
            connection.execute(
                """
                UPDATE positions SET verified_mark_count=1,
                    last_verified_mark_at=COALESCE(last_verified_mark_at,last_mark_at)
                WHERE verified_mark_count=0 AND last_mark_at IS NOT NULL
                  AND last_mark_at-opened_at>1
                """
            )
            # Flow V1 requires economically positive anchor inflow, not just
            # more buy-sized events. Keep pre-gate rows safely shadow-negative
            # when opening a database created by an earlier model revision.
            connection.execute(
                """
                UPDATE flow_signals SET shadow_qualified=0,
                    shadow_score=MIN(shadow_score,?)
                WHERE net_anchor_flow_fraction<?
                """,
                (
                    FLOW_SHADOW_SCORE_THRESHOLD - 1,
                    FLOW_MINIMUM_NET_ANCHOR_FRACTION,
                ),
            )
            connection.execute(
                """
                UPDATE flow_signals SET shadow_qualified=0,
                    shadow_score=MIN(shadow_score,?)
                WHERE unique_resolved_participants<? OR identity_coverage<?
                """,
                (
                    FLOW_SHADOW_SCORE_THRESHOLD - 1,
                    FLOW_MINIMUM_SENDER_HINTS,
                    FLOW_MINIMUM_IDENTITY_COVERAGE,
                ),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO position_policy_states (
                    token_address,policy,status,high_multiple,last_mark_at
                ) SELECT token_address,?,status,high_multiple,last_mark_at FROM positions
                """,
                (SHADOW_POLICY_FIXED_3X,),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO position_policy_states (
                    token_address,policy,status,high_multiple,last_mark_at
                ) SELECT token_address,?,status,high_multiple,last_mark_at FROM positions
                """,
                (SHADOW_POLICY_PURE_TRAILING,),
            )
            connection.execute(
                """
                UPDATE candidates SET paper_decision=CASE
                    WHEN paper_entry_allowed=1 THEN ?
                    WHEN paper_entry_allowed=0 THEN ?
                    ELSE paper_decision
                END
                WHERE paper_decision IS NULL
                """,
                (PAPER_DECISION_ADMITTED, PAPER_DECISION_REJECTED),
            )
            # Earlier versions treated a temporarily unverifiable V4 market as
            # a terminal rejection. Preserve the analysis result but migrate
            # otherwise-qualified, hook-free candidates into a bounded market
            # watch. Any eventual entry is priced at its future executable
            # snapshot; this never creates a retroactive paper fill.
            now = time.time()
            legacy_watch_rows = connection.execute(
                """
                SELECT token_address,hard_stops_json FROM candidates
                WHERE analysis_status='complete' AND paper_entry_allowed=0
                  AND paper_decision=? AND score>=?
                  AND risk_level IN ('Low','Medium')
                  AND source_version=? AND LOWER(COALESCE(hooks_address,?))=?
                """,
                (
                    PAPER_DECISION_REJECTED, MINIMUM_ENTRY_SCORE, SOURCE_V4,
                    ZERO_ADDRESS, ZERO_ADDRESS,
                ),
            ).fetchall()
            for row in legacy_watch_rows:
                try:
                    stops = set(json.loads(row["hard_stops_json"] or "[]"))
                except (TypeError, ValueError):
                    stops = set()
                if stops and stops <= TEMPORARY_EXECUTION_STOPS:
                    connection.execute(
                        """
                        UPDATE candidates SET paper_decision=?,
                            market_watch_started_at=COALESCE(market_watch_started_at,?),
                            market_watch_expires_at=COALESCE(market_watch_expires_at,?),
                            market_watch_reason=? WHERE token_address=?
                        """,
                        (
                            PAPER_DECISION_WATCHING, now,
                            now + EXECUTABLE_MARKET_WATCH_SECONDS,
                            "market_state_unverified", row["token_address"],
                        ),
                    )
            # Positions created before entry-market-cap persistence can be
            # reconstructed from the exact analysis-time market evidence used
            # to open them. Never substitute the earlier launch checkpoint.
            legacy = connection.execute(
                """
                SELECT p.token_address,c.market_evidence_json,p.opened_at
                FROM positions p JOIN candidates c USING(token_address)
                WHERE p.entry_market_cap_usd IS NULL
                """
            ).fetchall()
            for row in legacy:
                try:
                    evidence = json.loads(row["market_evidence_json"] or "{}")
                except (TypeError, ValueError):
                    evidence = {}
                market_cap = safe_float(evidence.get("market_cap_usd"), 0.0) or None
                connection.execute(
                    """
                    UPDATE positions SET entry_market_cap_usd=?,
                        last_market_cap_usd=COALESCE(last_market_cap_usd,?),
                        last_market_observed_at=COALESCE(last_market_observed_at,?)
                    WHERE token_address=?
                    """,
                    (market_cap, market_cap, _utc_now(), row["token_address"]),
                )

    def add_candidates(self, candidates: list[dict]) -> int:
        if not candidates:
            return 0
        with self.connection() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO candidates (
                    token_address, pair_address, factory_address, block_number,
                    block_timestamp, transaction_hash, log_index, name, symbol,
                    source_version, pool_id, hooks_address, fee_tier, tick_spacing,
                    discovered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [(
                    row["token_address"].lower(), row["pair_address"].lower(),
                    row["factory_address"].lower(), row["block_number"],
                    row["block_timestamp"], row["transaction_hash"], row["log_index"],
                    row.get("name") or "", row.get("symbol") or "",
                    row.get("source_version") or SOURCE_V2, row.get("pool_id"),
                    row.get("hooks_address"), row.get("fee_tier"), row.get("tick_spacing"),
                    _utc_now(), _utc_now(),
                ) for row in candidates],
            )
            return connection.total_changes - before

    def apply_v4_events(self, events: list[dict]) -> None:
        """Persist V4 lifecycle evidence without creating analysis work yet.

        Windows are computed for every touched pool and kept: they are
        diagnostic and retrospective evidence, and suppressing them to protect
        the enrichment budget would trade away research data to work around a
        selection problem. The dilution belongs to the enrichment SELECTOR --
        267 stale latest-state rows still satisfy its active_window test, so it
        spreads a fixed budget across windows that can never meet the
        120-block freshness bound. That needs a head-relative condition in the
        selector, not fewer windows here.
        """
        with self.connection() as connection:
            affected_flow_pools = set()
            for event in events:
                kind = event["kind"]
                if kind == "initialize":
                    connection.execute(
                        """
                        INSERT INTO v4_pools (
                            pool_id,currency0,currency1,token_address,anchor_address,
                            fee_tier,tick_spacing,hooks_address,initialized_block,
                            sqrt_price_x96,last_tick,updated_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(pool_id) DO UPDATE SET
                            sqrt_price_x96=excluded.sqrt_price_x96,
                            last_tick=excluded.last_tick,updated_at=excluded.updated_at
                        """,
                        (event["pool_id"],event["currency0"],event["currency1"],
                         event["token_address"],event["anchor_address"],event["fee_tier"],
                         event["tick_spacing"],event["hooks_address"],event["block_number"],
                         str(event["sqrt_price_x96"]),event["tick"],_utc_now()),
                    )
                elif kind == "modify":
                    connection.execute(
                        "UPDATE v4_pools SET modified_block=COALESCE(modified_block,?),updated_at=? WHERE pool_id=?",
                        (event["block_number"],_utc_now(),event["pool_id"]),
                    )
                elif kind == "swap":
                    connection.execute(
                        """
                        UPDATE v4_pools SET swapped_block=COALESCE(swapped_block,?),
                            swap_timestamp=COALESCE(swap_timestamp,?),transaction_hash=?,log_index=?,
                            sqrt_price_x96=?,active_liquidity=?,last_tick=?,updated_at=?
                        WHERE pool_id=?
                        """,
                        (event["block_number"],event["block_timestamp"],event["transaction_hash"],
                         event["log_index"],str(event["sqrt_price_x96"]),
                         str(event["active_liquidity"]),event["tick"],_utc_now(),event["pool_id"]),
                    )
                    pool = connection.execute(
                        "SELECT * FROM v4_pools WHERE pool_id=?",
                        (event["pool_id"],),
                    ).fetchone()
                    if not pool:
                        continue
                    amount0 = int(event.get("amount0_raw") or 0)
                    amount1 = int(event.get("amount1_raw") or 0)
                    token_is_currency0 = (
                        pool["token_address"].lower() == pool["currency0"].lower()
                    )
                    token_delta = amount0 if token_is_currency0 else amount1
                    anchor_delta = amount1 if token_is_currency0 else amount0
                    side = "buy" if anchor_delta > 0 else (
                        "sell" if anchor_delta < 0 else "indeterminate"
                    )
                    connection.execute(
                        """
                        INSERT OR IGNORE INTO swap_observations (
                            source_version,pool_id,token_address,block_number,
                            transaction_hash,log_index,sender_hint,
                            sender_identity_kind,amount0_raw,amount1_raw,
                            anchor_delta_raw,token_delta_raw,side,sqrt_price_x96,
                            observed_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            SOURCE_V4,event["pool_id"],pool["token_address"],
                            event["block_number"],event["transaction_hash"],
                            event["log_index"],event.get("sender_hint"),
                            "event_sender_may_be_router",str(amount0),str(amount1),
                            str(anchor_delta),str(token_delta),side,
                            str(event.get("sqrt_price_x96") or ""),_utc_now(),
                        ),
                    )
                    affected_flow_pools.add(event["pool_id"])
            for pool_id in affected_flow_pools:
                self._refresh_v4_flow_signal(connection, pool_id)

    @staticmethod
    def _bounded_fraction(value: float) -> float:
        return max(0.0, min(1.0, value))

    def _refresh_v4_flow_signal(
        self, connection: sqlite3.Connection, pool_id: str,
    ) -> None:
        pool = connection.execute(
            "SELECT * FROM v4_pools WHERE pool_id=?", (pool_id,)
        ).fetchone()
        latest = connection.execute(
            "SELECT MAX(block_number) FROM swap_observations WHERE pool_id=?",
            (pool_id,),
        ).fetchone()[0]
        if not pool or latest is None:
            return
        start = max(int(pool["swapped_block"] or latest), int(latest) - FLOW_WINDOW_BLOCKS + 1)
        rows = connection.execute(
            """
            SELECT * FROM swap_observations
            WHERE pool_id=? AND block_number>=? AND block_number<=?
            ORDER BY block_number,log_index
            """,
            (pool_id, start, latest),
        ).fetchall()
        buys = [row for row in rows if row["side"] == "buy"]
        sells = [row for row in rows if row["side"] == "sell"]
        directional = len(buys) + len(sells)
        buy_ratio = len(buys) / directional if directional else 0.0
        buy_anchor = sum(max(0, int(row["anchor_delta_raw"])) for row in rows)
        sell_anchor = sum(max(0, -int(row["anchor_delta_raw"])) for row in rows)
        gross_anchor = buy_anchor + sell_anchor
        net_fraction = (
            (buy_anchor - sell_anchor) / gross_anchor if gross_anchor else 0.0
        )
        sender_hints = {
            str(row["sender_hint"]).lower() for row in rows if row["sender_hint"]
        }
        participants = {
            str(row["resolved_participant"]).lower()
            for row in rows if row["resolved_participant"]
        }
        resolved_swaps = sum(bool(row["resolved_participant"]) for row in rows)
        identity_coverage = resolved_swaps / len(rows) if rows else 0.0
        participant_counts: dict[str, int] = {}
        for row in rows:
            participant = str(row["resolved_participant"] or "").lower()
            if participant:
                participant_counts[participant] = participant_counts.get(participant, 0) + 1
        maximum_participant_share = (
            max(participant_counts.values()) / resolved_swaps
            if participant_counts and resolved_swaps else 0.0
        )
        price_multiple = None
        priced = [
            int(row["sqrt_price_x96"]) for row in rows
            if str(row["sqrt_price_x96"] or "").isdigit()
            and int(row["sqrt_price_x96"]) > 0
        ]
        if len(priced) >= 2:
            raw_multiple = (priced[-1] / priced[0]) ** 2
            price_multiple = (
                raw_multiple
                if pool["token_address"].lower() == pool["currency0"].lower()
                else 1 / raw_multiple
            )

        # Count DISTINCT TRADERS from ONE source: resolved origins whenever any
        # exist, sender hints only when none do.
        #
        # Hints are not trader identities on this chain. 187 distinct hints
        # cover 487,247 swaps, one of them sent 306,879 across 1,182 pools, and
        # the top three account for 89.4% -- they are routers. So a hint count
        # is a count of routers, and it is wrong in BOTH directions: it
        # undercounts busy pools (one window showed 1 hint against 7 resolved
        # origins) and can overcount thin ones (3 hints against 2 origins).
        # Taking max() of the two therefore picked whichever error happened to
        # favour passing the gate.
        #
        # Hints are kept as a last-resort fallback so an unenriched window is
        # scored on weak evidence rather than on absence, and the source is
        # recorded either way. A hint-backed window has zero identity coverage
        # by construction, so its tier is `unresolved` and it can never reach
        # paper entry -- it informs research only.
        if participants:
            distinct_traders = len(participants)
            participant_evidence = "resolved_origins"
        elif sender_hints:
            distinct_traders = len(sender_hints)
            participant_evidence = "sender_hints_may_be_routers"
        else:
            distinct_traders = 0
            participant_evidence = "none"

        pressure_quality = self._bounded_fraction((buy_ratio - 0.5) / 0.35)
        participant_quality = self._bounded_fraction(
            distinct_traders / FLOW_MINIMUM_SENDER_HINTS
        )
        velocity_quality = self._bounded_fraction(
            len(rows) / FLOW_MINIMUM_SWAPS
        )
        # The minimum, rather than a sum, prevents one burst from being
        # counted three times as ratio + participants + velocity.
        core = 75.0 * min(
            pressure_quality, participant_quality, velocity_quality
        )
        net_bonus = 15.0 * self._bounded_fraction(net_fraction)
        price_bonus = 10.0 * self._bounded_fraction(
            ((price_multiple or 1.0) - 1.0) / 0.12
        )
        uncapped_score = round(min(100.0, core + net_bonus + price_bonus), 1)
        adverse_price_direction = bool(
            price_multiple is not None and price_multiple < 1.0
        )
        participant_concentration_pass = bool(
            maximum_participant_share <= FLOW_MAXIMUM_PARTICIPANT_SHARE
        )
        qualification_gaps = []
        identity_unverified = False
        if len(rows) < FLOW_MINIMUM_SWAPS:
            qualification_gaps.append("minimum_swaps")
        if distinct_traders < FLOW_MINIMUM_SENDER_HINTS:
            qualification_gaps.append("minimum_unique_participants")
        if identity_coverage < FLOW_MINIMUM_IDENTITY_COVERAGE:
            # Recorded, not gated. Identity coverage classifies how much of a
            # window's flow is attributable, which is a confidence attribute
            # worth stratifying research on -- it is NOT a permit. Paper entry
            # requires it separately, so an unresolved window can inform
            # learning without ever being tradeable.
            identity_unverified = True
        if net_fraction < FLOW_MINIMUM_NET_ANCHOR_FRACTION:
            qualification_gaps.append("positive_net_anchor_flow")
        if not participant_concentration_pass:
            qualification_gaps.append("bounded_participant_concentration")
        if adverse_price_direction:
            qualification_gaps.append("non_adverse_price_direction")
        enough_evidence = not qualification_gaps
        score = uncapped_score
        if not enough_evidence:
            score = min(score, FLOW_SHADOW_SCORE_THRESHOLD - 1)
        # The score no longer promotes anything. Measured on 59 resolved
        # observations, the high-score half returned -0.5678 against -0.2876
        # for the low-score half: a gap of -0.2801 at permutation p=0.0135
        # over 4,000 shuffles. Requiring a HIGH score therefore selected the
        # WORSE half of an already-losing population. The check for
        # survivorship came back clean (resolved 12.32, pending 13.02,
        # non_exitable 14.10 mean score), so the inversion is not an artefact
        # of which observations happen to resolve.
        #
        # Qualification now rests on the evidence gates alone, which are
        # separately testable claims about a window rather than a weighted
        # composite. The score is still computed and recorded, because a
        # reliably inverted quantity is information -- it simply must not be
        # the thing that says yes.
        qualified = bool(enough_evidence)
        limitations = [
            "sender_identity_is_event_hint_and_may_be_router",
            "qualification_uses_transaction_from_not_event_sender",
            "window_uses_estimated_45_second_block_span",
            "shadow_only_not_an_admission_rule",
            "qualification_requires_positive_net_anchor_inflow",
            "qualification_rejects_adverse_price_direction",
            "qualification_caps_single_participant_share",
        ]
        features = {
            "pressure_quality": pressure_quality,
            "participant_quality": participant_quality,
            "velocity_quality": velocity_quality,
            "gross_anchor_raw": str(gross_anchor),
            "buy_anchor_raw": str(buy_anchor),
            "sell_anchor_raw": str(sell_anchor),
            "net_anchor_raw": str(buy_anchor - sell_anchor),
            "identity_coverage": identity_coverage,
            "unique_sender_hints": len(sender_hints),
            "unique_resolved_participants": len(participants),
            "maximum_participant_share": maximum_participant_share,
            "participant_concentration_pass": participant_concentration_pass,
            "adverse_price_direction": adverse_price_direction,
            "identity_unverified": identity_unverified,
            "distinct_traders": distinct_traders,
            "participant_evidence": participant_evidence,
            "uncapped_shadow_score": uncapped_score,
            "qualification_gaps": qualification_gaps,
            "distance_to_qualification_threshold": round(
                FLOW_SHADOW_SCORE_THRESHOLD - uncapped_score, 1
            ),
            "score_components": {
                "joint_flow_core": round(core, 3),
                "net_flow_bonus": round(net_bonus, 3),
                "price_confirmation_bonus": round(price_bonus, 3),
            },
        }
        connection.execute(
            """
            INSERT INTO flow_signals (
                source_version,pool_id,token_address,computed_at,window_blocks,
                window_start_block,window_end_block,swap_count,buy_count,
                sell_count,unique_sender_hints,unique_resolved_participants,
                identity_coverage,buy_ratio,
                net_anchor_flow_fraction,price_multiple,uncapped_shadow_score,
                shadow_score,shadow_qualified,confidence,
                qualification_gaps_json,limitations_json,features_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(pool_id) DO UPDATE SET
                computed_at=excluded.computed_at,
                window_start_block=excluded.window_start_block,
                window_end_block=excluded.window_end_block,
                swap_count=excluded.swap_count,buy_count=excluded.buy_count,
                sell_count=excluded.sell_count,
                unique_sender_hints=excluded.unique_sender_hints,
                unique_resolved_participants=excluded.unique_resolved_participants,
                identity_coverage=excluded.identity_coverage,
                buy_ratio=excluded.buy_ratio,
                net_anchor_flow_fraction=excluded.net_anchor_flow_fraction,
                price_multiple=excluded.price_multiple,
                uncapped_shadow_score=excluded.uncapped_shadow_score,
                shadow_score=excluded.shadow_score,
                shadow_qualified=excluded.shadow_qualified,
                confidence=excluded.confidence,
                qualification_gaps_json=excluded.qualification_gaps_json,
                limitations_json=excluded.limitations_json,
                features_json=excluded.features_json
            """,
            (
                SOURCE_V4,pool_id,pool["token_address"],_utc_now(),
                FLOW_WINDOW_BLOCKS,start,int(latest),len(rows),len(buys),
                len(sells),len(sender_hints),len(participants),identity_coverage,
                buy_ratio,net_fraction,price_multiple,
                uncapped_score,score,int(qualified),(
                    "transaction_origins_verified"
                    if identity_coverage >= FLOW_MINIMUM_IDENTITY_COVERAGE
                    else "identity_resolution_incomplete"
                ),
                _canonical(qualification_gaps),_canonical(limitations),
                _canonical(features),
            ),
        )
        # Same window, appended to the history. IGNORE keeps recomputation of
        # an already-recorded window idempotent, so a cycle that revisits a
        # pool cannot inflate the series.
        connection.execute(
            """
            INSERT OR IGNORE INTO flow_signal_windows (
                policy_version,pool_id,source_version,token_address,computed_at,
                window_blocks,window_start_block,window_end_block,swap_count,
                buy_count,sell_count,unique_sender_hints,
                unique_resolved_participants,identity_coverage,buy_ratio,
                net_anchor_flow_fraction,price_multiple,uncapped_shadow_score,
                shadow_score,shadow_qualified,confidence,
                qualification_gaps_json,features_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                FLOW_EVIDENCE_POLICY_VERSION,pool_id,SOURCE_V4,
                pool["token_address"],_utc_now(),FLOW_WINDOW_BLOCKS,start,
                int(latest),len(rows),len(buys),len(sells),len(sender_hints),
                len(participants),identity_coverage,buy_ratio,net_fraction,
                price_multiple,uncapped_score,score,int(qualified),(
                    "transaction_origins_verified"
                    if identity_coverage >= FLOW_MINIMUM_IDENTITY_COVERAGE
                    else "identity_resolution_incomplete"
                ),
                _canonical(qualification_gaps),_canonical(features),
            ),
        )

    def flow_window_history(
        self, limit: int = 500, *, qualified_only: bool = False,
        policy_version: str | None = None,
    ) -> list[dict]:
        """Read the append-only window series, newest first.

        Use this and never flow_signals for any retrospective question. The
        latest-state table holds one row per pool at that pool's final swap,
        so measuring across it selects terminal windows of dead pools.
        """
        clauses = ["policy_version=?"]
        params: list = [policy_version or FLOW_EVIDENCE_POLICY_VERSION]
        if qualified_only:
            clauses.append("shadow_qualified=1")
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                f"""
                SELECT * FROM flow_signal_windows WHERE {' AND '.join(clauses)}
                ORDER BY window_end_block DESC, pool_id LIMIT ?
                """,
                (*params, max(0, limit)),
            )]

    def prune_flow_windows(self, keep_blocks: int = 250_000) -> int:
        """Drop old NON-QUALIFIED windows; qualified ones are evidence forever.

        The series grows by roughly one row per pool per cycle, so it needs a
        bound. Qualified windows are never pruned: they are the population any
        promotion decision rests on, and losing them would silently shrink the
        denominator the way the closed-position audit once did.
        """
        with self.connection() as connection:
            head = connection.execute(
                "SELECT MAX(window_end_block) FROM flow_signal_windows"
            ).fetchone()[0] or 0
            if head <= keep_blocks:
                return 0
            cursor = connection.execute(
                """
                DELETE FROM flow_signal_windows
                WHERE shadow_qualified=0 AND window_end_block < ?
                """,
                (head - keep_blocks,),
            )
            return cursor.rowcount or 0

    def flow_summary(self) -> dict:
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) pools,COALESCE(SUM(swap_count),0) window_swaps,
                       COALESCE(SUM(shadow_qualified),0) shadow_qualified,
                       MAX(shadow_score) maximum_shadow_score,
                       MAX(uncapped_shadow_score) maximum_uncapped_shadow_score,
                       COALESCE(SUM(CASE WHEN shadow_qualified=0
                           AND uncapped_shadow_score>=? THEN 1 ELSE 0 END),0)
                           near_threshold_unqualified
                FROM flow_signals
                """,
                (FLOW_SHADOW_SCORE_THRESHOLD,),
            ).fetchone()
            raw = connection.execute(
                "SELECT COUNT(*) FROM swap_observations"
            ).fetchone()[0]
            resolved = connection.execute(
                "SELECT COUNT(*) FROM swap_observations WHERE resolved_participant IS NOT NULL"
            ).fetchone()[0]
            active = connection.execute(
                """
                SELECT COUNT(*) raw,
                       COALESCE(SUM(CASE WHEN so.resolved_participant IS NOT NULL
                                         THEN 1 ELSE 0 END),0) resolved
                FROM swap_observations so
                JOIN flow_signals fs ON fs.pool_id=so.pool_id
                WHERE so.block_number BETWEEN fs.window_start_block
                                          AND fs.window_end_block
                """
            ).fetchone()
            pending = self.pending_transaction_origin_counts()
        return {
            "raw_swaps": raw, "pools": row["pools"] or 0,
            "identity_resolved_swaps": resolved,
            "identity_coverage": resolved / raw if raw else 0.0,
            "active_identity_coverage": (
                active["resolved"] / active["raw"] if active["raw"] else 0.0
            ),
            "active_identity_resolved_swaps": int(active["resolved"] or 0),
            "active_identity_swaps": int(active["raw"] or 0),
            "identity_pending_active": pending["active"],
            "identity_pending_historical": pending["historical"],
            "window_swaps": row["window_swaps"] or 0,
            "shadow_qualified": row["shadow_qualified"] or 0,
            "maximum_shadow_score": row["maximum_shadow_score"],
            "maximum_uncapped_shadow_score": row["maximum_uncapped_shadow_score"],
            # Short alias retained for the dashboard/API consumer.
            "maximum_uncapped_score": row["maximum_uncapped_shadow_score"],
            "near_threshold_unqualified": row["near_threshold_unqualified"] or 0,
            "admission_enabled": False,
            "scope": "uniswap_v4_shadow_v1",
        }

    def recent_flow_signals(self, limit: int = 25) -> list[dict]:
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT fs.*,c.name,c.symbol,c.analysis_status,c.paper_decision
                FROM flow_signals fs
                LEFT JOIN candidates c USING(token_address)
                ORDER BY fs.shadow_score DESC,fs.window_end_block DESC LIMIT ?
                """,
                (max(0, limit),),
            ).fetchall()
        values = []
        for row in rows:
            value = dict(row)
            value["shadow_qualified"] = bool(value["shadow_qualified"])
            value["qualification_gaps"] = json.loads(
                value.pop("qualification_gaps_json") or "[]"
            )
            value["limitations"] = json.loads(value.pop("limitations_json") or "[]")
            value["features"] = json.loads(value.pop("features_json") or "{}")
            values.append(value)
        return values

    @staticmethod
    def _flow_policy() -> dict:
        """Return the frozen, versioned policy used by one evidence cohort."""
        return {
            "version": FLOW_EVIDENCE_POLICY_VERSION,
            "minimum_swaps": FLOW_MINIMUM_SWAPS,
            "minimum_participants": FLOW_MINIMUM_SENDER_HINTS,
            "minimum_identity_coverage": FLOW_MINIMUM_IDENTITY_COVERAGE,
            "minimum_net_anchor_fraction": FLOW_MINIMUM_NET_ANCHOR_FRACTION,
            "maximum_participant_share": FLOW_MAXIMUM_PARTICIPANT_SHARE,
            "reject_adverse_price_direction": True,
            "score_threshold": FLOW_SHADOW_SCORE_THRESHOLD,
            "maximum_head_lag_blocks": FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
            "signal_cooldown_blocks": FLOW_SIGNAL_EVENT_COOLDOWN_BLOCKS,
            "friction_bps": FLOW_EVIDENCE_FRICTION_BPS,
        }

    @classmethod
    def _flow_cohort_id(cls) -> str:
        return hashlib.sha256(_canonical(cls._flow_policy()).encode()).hexdigest()[:16]

    @staticmethod
    def _flow_event_id(*parts) -> str:
        return hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest()

    def seal_flow_observation(
        self, *, pool_id: str, token_address: str, observation_head: int,
        window_start_block: int, window_end_block: int,
        transaction_hashes: list[str], features: dict, quote: dict,
        quote_block: int | None, now: float, role: str = "matched_control",
        gap_count: int | None = None,
    ) -> str | None:
        """Seal an observation and schedule its outcomes, atomically, at ingest.

        Sealing BEFORE enrichment is what makes the record evidence: the claim
        and its future outcomes both exist before anything is known about how
        it turns out. Enrichment afterwards may classify the observation but
        can never alter it -- classification lives in its own table.

        Returns None if this exact observation was already sealed, so a
        repeated pass is idempotent rather than duplicating the claim.
        """
        digest = hashlib.sha256(
            "|".join(sorted(transaction_hashes)).encode("utf-8")
        ).hexdigest()
        observation_id = hashlib.sha256(
            "|".join((
                FLOW_EVIDENCE_POLICY_VERSION, str(pool_id),
                str(window_end_block), digest,
            )).encode("utf-8")
        ).hexdigest()
        lag = max(0, int(observation_head) - int(window_end_block))
        # The market payload carries execution_quote.verified and
        # executable_quote_verified -- there is no top-level "verified" key,
        # so checking one recorded every entry quote as unverified and would
        # have made every outcome non-exitable regardless of the exit price.
        # Same shape as the exit-side defect; both came from asserting a dict
        # shape instead of reading the one production returns.
        entry_execution = dict((quote or {}).get("execution_quote") or {})
        verified = bool(
            entry_execution.get("verified")
            or (quote or {}).get("executable_quote_verified")
        )
        with self.connection() as connection:
            result = connection.execute(
                """
                INSERT OR IGNORE INTO flow_observations (
                    observation_id,policy_version,pool_id,token_address,
                    observation_head,observation_head_lag_blocks,observed_at,
                    observed_at_epoch,window_start_block,window_end_block,
                    transaction_set_hash,transaction_count,features_json,
                    quote_json,quote_block,quote_verified,sealed_at,cohort_id,
                    role,qualification_gap_count,round_trip_return
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    observation_id, FLOW_EVIDENCE_POLICY_VERSION, pool_id,
                    token_address, int(observation_head), lag, _utc_now(),
                    float(now), int(window_start_block), int(window_end_block),
                    digest, len(transaction_hashes), _canonical(features),
                    _canonical(quote or {}),
                    int(quote_block) if quote_block else None,
                    int(verified), _utc_now(), FLOW_EVIDENCE_COHORT_ID,
                    str(role), gap_count,
                    # Pure friction, pinned at seal: what the position would
                    # return if bought and sold in the same block.
                    self._round_trip_return(quote),
                ),
            )
            if not result.rowcount:
                return None
            # Outcomes scheduled in the SAME transaction, so an observation
            # can never exist without the future it promised to measure.
            connection.executemany(
                """
                INSERT OR IGNORE INTO flow_observation_outcomes (
                    observation_id,horizon_label,horizon_seconds,target_at
                ) VALUES (?,?,?,?)
                """,
                [
                    (observation_id, label, seconds, float(now) + seconds)
                    for label, seconds in FLOW_EVIDENCE_HORIZONS
                ],
            )
        return observation_id

    def due_flow_observation_outcomes(self, now: float, limit: int = 50) -> list[dict]:
        """Scheduled outcomes whose horizon has arrived."""
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """
                SELECT o.*, obs.pool_id, obs.token_address, obs.quote_json,
                       obs.quote_verified, obs.cohort_id, obs.window_end_block
                FROM flow_observation_outcomes o
                JOIN flow_observations obs USING(observation_id)
                WHERE o.status='pending' AND o.target_at<=?
                  AND obs.quote_verified=1
                ORDER BY o.target_at LIMIT ?
                """,
                (float(now), max(0, limit)),
            )]

    def record_flow_observation_outcome(
        self, due: dict, exit_quote: dict, quote_block: int, now: float,
    ) -> dict:
        """Resolve one scheduled outcome against a verified exit quote.

        A strategy that cannot exit has lost the deployed notional for
        evaluation purposes -- recording it as missing would quietly select
        rugs out of the sample, which is how a 67% total-loss rate once read
        as clean.
        """
        try:
            entry = json.loads(due.get("quote_json") or "{}")
        except (TypeError, ValueError):
            entry = {}
        entry_anchor = safe_float(
            (entry.get("execution_quote") or {}).get("anchor_in_raw"),
            safe_float(entry.get("anchor_in_raw"), 0.0),
        )
        exit_anchor = safe_float(exit_quote.get("anchor_out_raw"), 0.0)
        verified = bool(due.get("quote_verified") and exit_quote.get("verified"))
        exit_valid = bool(verified and entry_anchor > 0 and exit_anchor > 0)
        if not bool(due.get("quote_verified")) or entry_anchor <= 0:
            # No valid entry price, so nothing is measurable from here. This
            # is NOT a total loss: scoring it -1.0 would inject fabricated
            # losses into every return statistic -- the same absence-as-value
            # error that made a broken reader look like a 67% rug rate.
            net_return = None
            status = "unpriceable"
            exit_valid = False
        elif exit_valid:
            friction = 1 - FLOW_EVIDENCE_FRICTION_BPS / 10_000
            net_return = exit_anchor / entry_anchor * friction - 1
            status = "resolved"
        else:
            # Priced at entry but no exit available -- a real total loss for
            # evaluation, and distinct from unpriceable above.
            net_return = -1.0
            status = "non_exitable"
        # The position's own friction, read from the entry quote it was sealed
        # with: buy and sell at the same block, no time, no price movement.
        # Measured at n=236 this is 96% of the realised return, so the part
        # left for price to explain must be recorded separately or every
        # analysis has to reconstruct it.
        friction_return = self._round_trip_return(
            json.loads(due.get("quote_json") or "{}")
        )
        residual_return = (
            net_return - friction_return
            if net_return is not None and friction_return is not None else None
        )
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE flow_observation_outcomes
                SET status=?,observed_at=?,quote_block=?,net_return=?,exit_valid=?,
                    friction_return=?,residual_return=?
                WHERE observation_id=? AND horizon_label=? AND status='pending'
                """,
                # Friction is what the position cost before anything moved;
                # the residual is what is left for price to explain.
                (status, float(now), int(quote_block), net_return,
                 int(exit_valid), friction_return, residual_return,
                 due["observation_id"], due["horizon_label"]),
            )
        return {
            "observation_id": due["observation_id"],
            "horizon_label": due["horizon_label"],
            "status": status, "net_return": net_return,
            "exit_valid": exit_valid,
        }

    def cohort_progress(self, cohort_id: str | None = None) -> dict:
        """Completed primary-horizon observations, by identity tier.

        Counts COMPLETED outcomes only. Scheduled outcomes are not evidence,
        and the gates below never open on them.
        """
        cohort = cohort_id or FLOW_EVIDENCE_COHORT_ID
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                """
                SELECT c.identity_tier, c.arm, o.status, o.net_return,
                       o.exit_valid
                FROM flow_observations obs
                JOIN flow_observation_outcomes o USING(observation_id)
                LEFT JOIN flow_observation_classifications c USING(observation_id)
                WHERE obs.cohort_id=? AND o.horizon_label=?
                  AND o.status NOT IN ('pending','unpriceable')
                """,
                (cohort, FLOW_PRIMARY_HORIZON_LABEL),
            )]
        tiers: dict[str, dict] = {}
        arms: dict[str, int] = {}
        for row in rows:
            tier = tiers.setdefault(
                str(row["identity_tier"] or "unclassified"),
                {"completed": 0, "exitable": 0, "returns": []},
            )
            tier["completed"] += 1
            tier["exitable"] += int(bool(row["exit_valid"]))
            value = safe_float(row["net_return"], None)
            if value is not None:
                tier["returns"].append(value)
            arms[str(row["arm"] or "unclassified")] = (
                arms.get(str(row["arm"] or "unclassified"), 0) + 1
            )
        for tier in tiers.values():
            returns = tier.pop("returns")
            tier["mean_net_return"] = (
                round(sum(returns) / len(returns), 6) if returns else None
            )
            tier["samples"] = len(returns)
        completed = len(rows)
        comparable = [
            name for name, tier in tiers.items()
            if tier["completed"] >= FLOW_COHORT_MINIMUM_TIER_SAMPLES
        ]
        return {
            "cohort_id": cohort,
            "primary_horizon": FLOW_PRIMARY_HORIZON_LABEL,
            "completed_primary_observations": completed,
            "by_identity_tier": tiers,
            "by_arm": arms,
            "reflection_due": completed >= FLOW_COHORT_REFLECTION_AT,
            "evaluation_due": completed >= FLOW_COHORT_EVALUATION_AT,
            "tiers_with_minimum_samples": comparable,
            "tier_comparison_permitted": len(comparable) >= 2,
            "fresh_versus_stale_permitted": (
                arms.get("fresh", 0) >= FLOW_COHORT_MINIMUM_TIER_SAMPLES
                and arms.get("decision_stale", 0) >= FLOW_COHORT_MINIMUM_TIER_SAMPLES
            ),
            "note": (
                "completed outcomes only; scheduled outcomes are not evidence "
                "and no gate here opens on them"
            ),
        }

    def pair_matched_controls(self, cohort_id: str | None = None) -> int:
        """Pair each signal with a control chosen WITHOUT future information.

        The control is the unqualified observation nearest in observation time
        from a different pool. Nearest-in-time uses only what was known when
        both were sealed, so the pairing cannot be influenced by how either
        turned out.
        """
        cohort = cohort_id or FLOW_EVIDENCE_COHORT_ID
        paired = 0
        with self.connection() as connection:
            signals = [dict(r) for r in connection.execute(
                """
                SELECT observation_id, pool_id, observed_at_epoch
                FROM flow_observations
                WHERE cohort_id=? AND role='signal'
                  AND matched_observation_id IS NULL
                """, (cohort,),
            )]
            controls = [dict(r) for r in connection.execute(
                """
                SELECT observation_id, pool_id, observed_at_epoch
                FROM flow_observations
                WHERE cohort_id=? AND role='matched_control'
                """, (cohort,),
            )]
            taken: set[str] = set()
            for signal in signals:
                candidates = [
                    c for c in controls
                    if c["pool_id"] != signal["pool_id"]
                    and c["observation_id"] not in taken
                ]
                if not candidates:
                    continue
                best = min(
                    candidates,
                    key=lambda c: abs(
                        safe_float(c["observed_at_epoch"], 0.0)
                        - safe_float(signal["observed_at_epoch"], 0.0)
                    ),
                )
                taken.add(best["observation_id"])
                connection.execute(
                    "UPDATE flow_observations SET matched_observation_id=?"
                    " WHERE observation_id=?",
                    (best["observation_id"], signal["observation_id"]),
                )
                paired += 1
        return paired

    def signal_versus_control(self, cohort_id: str | None = None) -> dict:
        """Compare signal returns against matched controls.

        Without this the estate measured 43 observations at -0.70 and read it
        as a Flow result when NONE had qualified -- it was the baseline. A
        signal is only worth anything if it differs from the control.
        """
        cohort = cohort_id or FLOW_EVIDENCE_COHORT_ID
        with self.connection() as connection:
            rows = [dict(r) for r in connection.execute(
                """
                SELECT o.role, o.token_address, x.net_return
                FROM flow_observations o
                JOIN flow_observation_outcomes x USING(observation_id)
                WHERE o.cohort_id=? AND x.horizon_label=? AND x.status='resolved'
                """, (cohort, FLOW_PRIMARY_HORIZON_LABEL),
            )]
        arms: dict[str, dict] = {}
        for row in rows:
            arm = arms.setdefault(
                str(row["role"]), {"returns": [], "by_token": {}}
            )
            value = safe_float(row["net_return"], None)
            if value is None:
                continue
            arm["returns"].append(value)
            # One observation per token, earliest wins, so a single dying
            # token cannot dominate the mean the way one supplied 20 of 41.
            arm["by_token"].setdefault(row["token_address"], value)
        report = {}
        for name, arm in arms.items():
            raw = arm["returns"]
            deduped = sorted(arm["by_token"].values())
            report[name] = {
                "n_raw": len(raw),
                "n_tokens": len(deduped),
                "mean_net_return": (
                    round(sum(deduped) / len(deduped), 6) if deduped else None
                ),
                "median_net_return": (
                    round(deduped[len(deduped) // 2], 6) if deduped else None
                ),
                "positive": sum(1 for v in deduped if v > 0),
            }
        signal = (report.get("signal") or {}).get("mean_net_return")
        control = (report.get("matched_control") or {}).get("mean_net_return")
        return {
            "cohort_id": cohort,
            "arms": report,
            "incremental_expectancy": (
                round(signal - control, 6)
                if signal is not None and control is not None else None
            ),
            "comparison_permitted": bool(
                (report.get("signal") or {}).get("n_tokens", 0)
                >= FLOW_COHORT_MINIMUM_TIER_SAMPLES
                and (report.get("matched_control") or {}).get("n_tokens", 0)
                >= FLOW_COHORT_MINIMUM_TIER_SAMPLES
            ),
            "note": (
                "means are one-observation-per-token; incremental expectancy "
                "is meaningless until comparison_permitted is true"
            ),
        }

    @staticmethod
    def _round_trip_return(quote: dict | None) -> float | None:
        """What $1 in becomes if bought and sold at the same block.

        This is pure friction -- fee plus price impact both ways -- with no
        time and therefore no price movement in it. A pool with no liquidity
        returns -1.0 here, which is the honest answer rather than a missing
        value: the position cannot be exited.
        """
        payload = (quote or {}).get("execution_quote") or (quote or {})
        anchor_in = safe_float(payload.get("anchor_in_raw"), 0.0)
        anchor_out = safe_float(payload.get("anchor_out_raw"), -1.0)
        if anchor_in <= 0 or anchor_out < 0:
            return None
        return anchor_out / anchor_in - 1.0

    @staticmethod
    def _quote_price(quote: dict | None) -> float | None:
        """Anchor paid per token out -- the only comparable figure.

        anchor_in_raw alone is the notional being quoted, which is constant by
        construction, so comparing it would report zero drift forever.
        """
        payload = (quote or {}).get("execution_quote") or (quote or {})
        anchor = safe_float(payload.get("anchor_in_raw"), 0.0)
        tokens = safe_float(payload.get("token_out_raw"), 0.0)
        return anchor / tokens if anchor > 0 and tokens > 0 else None

    def classify_flow_observation(
        self, observation_id: str, *, decision_head: int,
        identity_coverage: float | None, gates: list, now: float | None = None,
        decision_quote: dict | None = None,
    ) -> dict:
        """Attach a verdict WITHOUT touching the sealed observation.

        decision_quote is the entry price re-taken at decision time. It
        replaces the block-count freshness bound: a signal is actionable when
        its price can still be verified now, not when the chain happens to
        have produced fewer than 120 blocks since the window closed. Passing
        None keeps the observation research-only, which is the safe default
        for every caller with no quote to offer.
        """
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM flow_observations WHERE observation_id=?",
                (observation_id,),
            ).fetchone()
            if not row:
                raise OutcomeLedgerObservationMissing(observation_id)
            observation_lag = int(row["observation_head_lag_blocks"])
            decision_lag = max(0, int(decision_head) - int(row["window_end_block"]))
            observation_fresh = (
                observation_lag <= FLOW_MAXIMUM_OBSERVATION_HEAD_LAG_BLOCKS
            )
            # Still measured and recorded -- it is the honest description of
            # how far the chain moved -- but it no longer decides anything,
            # because what it was measuring is RPC latency.
            decision_fresh = (
                decision_lag <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            )
            execution = (decision_quote or {}).get("execution_quote") or (
                decision_quote or {})
            decision_quote_verified = bool(execution.get("verified"))
            entry_price = self._quote_price(
                json.loads(row["quote_json"] or "{}"))
            decision_price = self._quote_price(decision_quote)
            drift_bps = (
                abs(decision_price - entry_price) / entry_price * 10_000
                if entry_price and decision_price else None
            )
            drift_within_threshold = (
                True if FLOW_DECISION_DRIFT_THRESHOLD_BPS is None
                else bool(drift_bps is not None
                          and drift_bps <= FLOW_DECISION_DRIFT_THRESHOLD_BPS)
            )
            # An unexitable position is not a trade whatever the flow said.
            # Prefer the decision-time quote, which describes the pool as it
            # is now, and fall back to the sealed one.
            round_trip = self._round_trip_return(decision_quote)
            if round_trip is None:
                round_trip = self._round_trip_return(
                    json.loads(row["quote_json"] or "{}"))
            exitable = bool(
                round_trip is not None
                and round_trip >= -FLOW_MAXIMUM_ROUND_TRIP_LOSS
            )
            decision_actionable = bool(
                decision_quote_verified and drift_within_threshold and exitable
            )
            coverage = safe_float(identity_coverage, 0.0)
            tier = (
                "verified" if coverage >= FLOW_MINIMUM_IDENTITY_COVERAGE
                else "partial" if coverage > 0 else "unresolved"
            )
            arm = (
                "fresh" if observation_fresh and decision_fresh
                else "decision_stale" if observation_fresh
                else "historical"
            )
            research = observation_fresh
            paper = bool(
                observation_fresh and decision_actionable
                and tier == "verified" and not list(gates or [])
            )
            verdict = {
                "observation_id": observation_id,
                "classified_at": _utc_now(),
                "decision_head": int(decision_head),
                "decision_head_lag_blocks": decision_lag,
                "identity_tier": tier, "identity_coverage": coverage,
                "arm": arm, "gates": list(gates or []),
                "research_eligible": research, "paper_eligible": paper,
                "decision_quote_verified": decision_quote_verified,
                "decision_price_drift_bps": drift_bps,
                "round_trip_return": round_trip,
                "exitable": exitable,
            }
            connection.execute(
                """
                INSERT INTO flow_observation_classifications (
                    observation_id,classified_at,decision_head,
                    decision_head_lag_blocks,identity_tier,identity_coverage,
                    arm,gates_json,research_eligible,paper_eligible,
                    decision_quote_verified,decision_price_drift_bps
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(observation_id) DO UPDATE SET
                    classified_at=excluded.classified_at,
                    decision_head=excluded.decision_head,
                    decision_head_lag_blocks=excluded.decision_head_lag_blocks,
                    identity_tier=excluded.identity_tier,
                    identity_coverage=excluded.identity_coverage,
                    arm=excluded.arm,gates_json=excluded.gates_json,
                    research_eligible=excluded.research_eligible,
                    paper_eligible=excluded.paper_eligible,
                    decision_quote_verified=excluded.decision_quote_verified,
                    decision_price_drift_bps=excluded.decision_price_drift_bps
                """,
                (
                    observation_id, verdict["classified_at"], int(decision_head),
                    decision_lag, tier, coverage, arm, _canonical(list(gates or [])),
                    int(research), int(paper),
                    int(decision_quote_verified), drift_bps,
                ),
            )
        return verdict

    def capture_flow_signal_events(
        self, head_block: int, now: float, *, ingest_head_block: int | None = None,
    ) -> dict:
        """Freeze current signal rows into immutable prospective observations.

        Catch-up rows remain useful diagnostic history, but only rows close to
        the observed chain head can schedule outcomes or influence promotion.
        A matched control is chosen without looking at future outcomes.
        """
        head_block = max(0, int(head_block))
        cohort_id = self._flow_cohort_id()
        policy = self._flow_policy()
        created: list[str] = []
        qualified_created = 0
        controls_created = 0
        historical_created = 0
        stale_arm_created = 0
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM flow_signals ORDER BY window_end_block,pool_id"
            )]
            for row in rows:
                row["qualification_gaps"] = json.loads(
                    row.pop("qualification_gaps_json") or "[]"
                )
                row["limitations"] = json.loads(row.pop("limitations_json") or "[]")
                row["features"] = json.loads(row.pop("features_json") or "{}")
            fresh = [
                row for row in rows
                if 0 <= head_block - int(row["window_end_block"])
                <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            ]
            qualified = [row for row in rows if bool(row["shadow_qualified"])]
            fresh_controls = [
                row for row in fresh if not bool(row["shadow_qualified"])
            ]
            last_qualified_blocks = {
                row["pool_id"]: int(row["last_block"])
                for row in connection.execute(
                    """
                    SELECT pool_id,MAX(window_end_block) last_block
                    FROM flow_signal_events
                    WHERE policy_version=? AND signal_role='qualified'
                    GROUP BY pool_id
                    """,
                    (FLOW_EVIDENCE_POLICY_VERSION,),
                )
            }

            def insert_event(row: dict, role: str, matched: str | None = None) -> str | None:
                nonlocal qualified_created, controls_created, historical_created
                nonlocal stale_arm_created
                lag = max(0, head_block - int(row["window_end_block"]))
                ingest_lag = (
                    max(0, int(ingest_head_block) - int(row["window_end_block"]))
                    if ingest_head_block else None
                )
                # Three arms, not two. A window inside the bound at observation
                # and outside it at decision is not "historical" -- it is the
                # experiment: same qualification, same outcome schedule, and a
                # paired comparison that says whether freshness affects
                # outcome at all. Only the fresh arm may enter paper trading.
                if lag <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS:
                    freshness = "fresh"
                elif (
                    ingest_lag is not None
                    and ingest_lag <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
                ):
                    freshness = "decision_stale"
                else:
                    freshness = "historical"
                # Research eligibility follows OBSERVATION freshness, so a
                # signal that was near-head when seen still informs learning.
                eligible = freshness in {"fresh", "decision_stale"}
                # Paper eligibility is strictly narrower: fresh at the decision
                # AND identity verified. Every other safety gate already
                # applied upstream at qualification.
                identity_ok = safe_float(
                    row.get("identity_coverage"), 0.0
                ) >= FLOW_MINIMUM_IDENTITY_COVERAGE
                paper_eligible = freshness == "fresh" and identity_ok
                event_id = self._flow_event_id(
                    FLOW_EVIDENCE_POLICY_VERSION, row["pool_id"],
                    row["window_end_block"], role, matched or "",
                )
                snapshot = dict(row)
                snapshot["policy"] = policy
                result = connection.execute(
                    """
                    INSERT OR IGNORE INTO flow_signal_events (
                        event_id,policy_version,cohort_id,source_version,pool_id,
                        token_address,signal_role,matched_signal_event_id,
                        window_start_block,window_end_block,head_block,
                        head_lag_blocks,freshness,eligible_for_evaluation,
                        signaled_at,snapshot_json,created_at,
                        arm,paper_eligible,ingest_head_block,ingest_head_lag_blocks
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        event_id,FLOW_EVIDENCE_POLICY_VERSION,cohort_id,
                        row["source_version"],row["pool_id"],row["token_address"],
                        role,matched,int(row["window_start_block"]),
                        int(row["window_end_block"]),head_block,lag,freshness,
                        int(eligible),now,_canonical(snapshot),_utc_now(),
                        freshness,int(paper_eligible),ingest_head_block,ingest_lag,
                    ),
                )
                if not result.rowcount:
                    return None
                created.append(event_id)
                historical_created += freshness == "historical"
                stale_arm_created += freshness == "decision_stale"
                qualified_created += role == "qualified"
                controls_created += role == "matched_control"
                if eligible:
                    connection.executemany(
                        """
                        INSERT OR IGNORE INTO flow_signal_outcomes (
                            event_id,horizon_label,horizon_seconds,target_at
                        ) VALUES (?,?,?,?)
                        """,
                        [
                            (event_id, label, seconds, now + seconds)
                            for label, seconds in FLOW_EVIDENCE_HORIZONS
                        ],
                    )
                return event_id

            used_controls: set[str] = set()
            for signal in qualified:
                previous_block = last_qualified_blocks.get(signal["pool_id"])
                if (
                    previous_block is not None
                    and int(signal["window_end_block"]) - previous_block
                    < FLOW_SIGNAL_EVENT_COOLDOWN_BLOCKS
                ):
                    continue
                signal_id = insert_event(signal, "qualified")
                if signal_id is None:
                    continue
                last_qualified_blocks[signal["pool_id"]] = int(
                    signal["window_end_block"]
                )
                if signal not in fresh:
                    continue
                candidates = [
                    row for row in fresh_controls if row["pool_id"] not in used_controls
                ]
                if not candidates:
                    continue
                control = min(
                    candidates,
                    key=lambda row: (
                        abs(float(row["shadow_score"]) - float(signal["shadow_score"])),
                        abs(int(row["swap_count"]) - int(signal["swap_count"])),
                        abs(int(row["window_end_block"]) - int(signal["window_end_block"])),
                        row["pool_id"],
                    ),
                )
                control_id = insert_event(control, "matched_control", signal_id)
                if control_id:
                    used_controls.add(control["pool_id"])
        return {
            "created": len(created), "event_ids": created,
            "qualified_created": qualified_created,
            "controls_created": controls_created,
            "historical_created": historical_created,
            "stale_arm_created": stale_arm_created,
            "head_block": head_block, "cohort_id": cohort_id,
        }

    def pending_flow_quotes(self, limit: int = 20) -> list[dict]:
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """
                SELECT e.*,p.currency0,p.currency1,p.anchor_address,p.fee_tier,
                       p.tick_spacing,p.hooks_address
                FROM flow_signal_events e JOIN v4_pools p USING(pool_id)
                WHERE e.eligible_for_evaluation=1 AND e.quote_status='pending'
                ORDER BY e.signaled_at,e.event_id LIMIT ?
                """,
                (max(0, limit),),
            )]

    def record_flow_entry_quote(
        self, event_id: str, market: dict, quote_block: int,
    ) -> None:
        quote = dict(market.get("execution_quote") or {})
        verified = bool(quote.get("verified"))
        exitable = bool(verified and quote.get("passes_round_trip_limit"))
        status = "verified" if verified else "failed"
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE flow_signal_events SET quote_status=?,quote_block=?,
                    quote_json=?,quote_verified=?,quote_exitable=?
                WHERE event_id=? AND quote_status='pending'
                """,
                (status,int(quote_block),_canonical({
                    "market": market, "captured_at": _utc_now(),
                    "friction_bps": FLOW_EVIDENCE_FRICTION_BPS,
                }),int(verified),int(exitable),event_id),
            )

    def due_flow_outcomes(self, now: float, limit: int = 30) -> list[dict]:
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """
                SELECT o.*,e.pool_id,e.token_address,e.signal_role,
                       e.matched_signal_event_id,e.quote_json entry_quote_json,
                       e.quote_verified entry_quote_verified
                FROM flow_signal_outcomes o
                JOIN flow_signal_events e USING(event_id)
                WHERE o.status='pending' AND o.target_at<=?
                ORDER BY o.target_at,o.event_id LIMIT ?
                """,
                (now,max(0,limit)),
            )]

    def record_flow_outcome(
        self, due: dict, market: dict, quote_block: int, now: float,
    ) -> None:
        entry = json.loads(due.get("entry_quote_json") or "{}")
        entry_quote = dict(entry.get("market", {}).get("execution_quote") or {})
        exit_quote = dict(market.get("paper_exit_quote") or {})
        entry_anchor = safe_float(entry_quote.get("anchor_in_raw"), 0.0)
        exit_anchor = safe_float(exit_quote.get("anchor_out_raw"), 0.0)
        verified = bool(due.get("entry_quote_verified") and exit_quote.get("verified"))
        exit_valid = bool(verified and entry_anchor > 0 and exit_anchor > 0)
        if exit_valid:
            friction = 1 - FLOW_EVIDENCE_FRICTION_BPS / 10_000
            net_return = exit_anchor / entry_anchor * friction - 1
        else:
            # A strategy that cannot exit has lost the deployed notional for
            # evaluation purposes; treating it as missing would select rugs.
            net_return = -1.0
        with self.connection() as connection:
            prior = connection.execute(
                """
                SELECT net_return FROM flow_signal_outcomes
                WHERE event_id=? AND status='observed'
                """,
                (due["event_id"],),
            ).fetchall()
            values = [safe_float(row[0], -1.0) for row in prior] + [net_return]
            connection.execute(
                """
                UPDATE flow_signal_outcomes SET status='observed',observed_at=?,
                    quote_block=?,quote_json=?,quote_verified=?,exit_valid=?,
                    net_return=?,maximum_favorable_excursion=?,
                    maximum_adverse_excursion=?,liquidity_usd=?,error=NULL
                WHERE event_id=? AND horizon_label=? AND status='pending'
                """,
                (
                    now,int(quote_block),_canonical(market),int(verified),
                    int(exit_valid),net_return,max(values),min(values),
                    safe_float(market.get("liquidity_usd"),0.0) or None,
                    due["event_id"],due["horizon_label"],
                ),
            )

    def flow_arm_comparison(self) -> dict:
        """Paired fresh vs decision-stale outcomes on identical criteria.

        The 120-block bound has never been tested. Six windows per cycle were
        inside it at observation and outside it at decision, and were being
        discarded -- which is exactly the population that can say whether
        freshness affects outcome. Both arms now qualify identically and are
        scheduled identically; only paper entry is restricted to the fresh arm.

        If the arms perform the same, the freshness architecture under
        discussion is unnecessary. If the stale arm is worse, the bound is
        justified by evidence rather than assumption.
        """
        arms: dict[str, dict] = {}
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT e.arm, o.horizon_label, o.status, o.net_return,
                       o.exit_valid
                FROM flow_signal_events e
                JOIN flow_signal_outcomes o USING(event_id)
                WHERE e.signal_role='qualified'
                """
            ).fetchall()
        for row in rows:
            arm = arms.setdefault(
                str(row["arm"] or "fresh"),
                {"events": 0, "resolved": 0, "exitable": 0, "returns": []},
            )
            arm["events"] += 1
            if row["status"] and row["status"] != "pending":
                arm["resolved"] += 1
                arm["exitable"] += int(bool(row["exit_valid"]))
                value = safe_float(row["net_return"], None)
                if value is not None:
                    arm["returns"].append(value)
        report = {}
        for name, arm in arms.items():
            returns = sorted(arm.pop("returns"))
            arm["mean_net_return"] = (
                round(sum(returns) / len(returns), 6) if returns else None
            )
            arm["median_net_return"] = (
                round(returns[len(returns) // 2], 6) if returns else None
            )
            arm["samples"] = len(returns)
            report[name] = arm
        fresh = report.get("fresh", {}).get("mean_net_return")
        stale = report.get("decision_stale", {}).get("mean_net_return")
        return {
            "arms": report,
            "freshness_advantage": (
                round(fresh - stale, 6)
                if fresh is not None and stale is not None else None
            ),
            "bound_blocks": FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
            "note": (
                "both arms qualify identically; only the fresh arm may enter "
                "paper trading. A freshness_advantage near zero means the "
                "bound is not earning its cost."
            ),
        }

    def pool_discovery_latency(self) -> dict:
        """Two different numbers that were being reported as one.

        The dashboard showed the batch crawler's cursor -- 493,004 blocks
        behind -- which read as "blind for 493,004 blocks". Since pools are
        admitted on sight from the near-head window, the newest pool is
        typically within a handful of blocks of the head. But the crawler gap
        is not harmless either: a pool BORN inside it that starts trading
        today is still invisible, because on-sight admission only sees
        Initialize logs in the near-head window. So both are reported, named
        for what they actually are.
        """
        with self.connection() as connection:
            head = connection.execute(
                "SELECT MAX(block_number) FROM swap_observations"
            ).fetchone()[0] or 0
            newest = connection.execute(
                "SELECT MAX(initialized_block) FROM v4_pools"
            ).fetchone()[0] or 0
            recent = connection.execute(
                "SELECT COUNT(*) FROM v4_pools WHERE initialized_block > ?",
                (head - FLOW_WINDOW_BLOCKS,),
            ).fetchone()[0]
        return {
            "head_block": head,
            "newest_pool_block": newest,
            "new_pool_latency_blocks": max(0, head - newest) if newest else None,
            "pools_born_in_last_window": recent,
        }

    def round_trip_summary(self) -> dict:
        """What a position costs to enter and leave, before anything moves.

        The dashboard showed a -41% average with no way to see that ~42 points
        of it is toll rather than token behaviour. Measured over the cohort,
        the same-block round trip averages -42.10% while the realised
        15-minute return averages -41.37%; the residual is +0.0073 at
        sign-flip p=0.7572, which is zero. Surfacing this turns an
        inexplicable loss into a legible one.
        """
        with self.connection() as connection:
            rows = [row[0] for row in connection.execute(
                "SELECT round_trip_return FROM flow_observations"
                " WHERE cohort_id=? AND round_trip_return IS NOT NULL",
                (FLOW_EVIDENCE_COHORT_ID,),
            )]
        if not rows:
            return {"measured": 0, "bound": FLOW_MAXIMUM_ROUND_TRIP_LOSS}
        rows.sort()
        exitable = [x for x in rows if x >= -FLOW_MAXIMUM_ROUND_TRIP_LOSS]
        return {
            "measured": len(rows),
            "bound": FLOW_MAXIMUM_ROUND_TRIP_LOSS,
            "median_round_trip": rows[len(rows) // 2],
            "worst_round_trip": rows[0],
            "best_round_trip": rows[-1],
            "exitable": len(exitable),
            "exitable_fraction": len(exitable) / len(rows),
            "unexitable": len(rows) - len(exitable),
        }

    def flow_evidence_summary(self) -> dict:
        """Return conservative, frozen-policy promotion evidence."""
        with self.connection() as connection:
            event_rows = [dict(row) for row in connection.execute(
                "SELECT * FROM flow_signal_events"
            )]
            outcome_rows = [dict(row) for row in connection.execute(
                "SELECT * FROM flow_signal_outcomes WHERE status='observed'"
            )]
        events = {row["event_id"]: row for row in event_rows}
        outcome_lookup = {
            (row["event_id"], row["horizon_label"]): row for row in outcome_rows
        }
        by_horizon: dict[str, dict] = {}
        for label, _seconds in FLOW_EVIDENCE_HORIZONS:
            rows = [row for row in outcome_rows if row["horizon_label"] == label]
            grouped = {}
            for role in ("qualified", "matched_control"):
                values = [
                    safe_float(row["net_return"], -1.0) for row in rows
                    if events.get(row["event_id"], {}).get("signal_role") == role
                ]
                grouped[role] = {
                    "n": len(values),
                    "mean_net_return": sum(values) / len(values) if values else None,
                    "catastrophic_rate": (
                        sum(value <= -0.90 for value in values) / len(values)
                        if values else None
                    ),
                }
            signal_mean = grouped["qualified"]["mean_net_return"]
            control_mean = grouped["matched_control"]["mean_net_return"]
            grouped["incremental_expectancy"] = (
                signal_mean - control_mean
                if signal_mean is not None and control_mean is not None else None
            )
            paired_differences = []
            for control_event in event_rows:
                signal_id = control_event.get("matched_signal_event_id")
                if control_event.get("signal_role") != "matched_control" or not signal_id:
                    continue
                control_outcome = outcome_lookup.get((control_event["event_id"], label))
                signal_outcome = outcome_lookup.get((signal_id, label))
                if not control_outcome or not signal_outcome:
                    continue
                paired_differences.append(
                    safe_float(signal_outcome["net_return"], -1.0)
                    - safe_float(control_outcome["net_return"], -1.0)
                )
            confidence_interval = None
            if len(paired_differences) >= 2:
                rng = random.Random(f"{self._flow_cohort_id()}:{label}")
                boot = []
                for _ in range(1000):
                    sample = [
                        paired_differences[rng.randrange(len(paired_differences))]
                        for _index in paired_differences
                    ]
                    boot.append(sum(sample) / len(sample))
                boot.sort()
                confidence_interval = [
                    boot[int(0.025 * (len(boot) - 1))],
                    boot[int(0.975 * (len(boot) - 1))],
                ]
            grouped["paired_n"] = len(paired_differences)
            grouped["paired_incremental_expectancy"] = (
                sum(paired_differences) / len(paired_differences)
                if paired_differences else None
            )
            grouped["paired_incremental_ci_95"] = confidence_interval
            by_horizon[label] = grouped
        eligible_signals = [
            row for row in event_rows
            if row["signal_role"] == "qualified" and row["eligible_for_evaluation"]
        ]
        completed_15m = [
            row for row in outcome_rows
            if row["horizon_label"] == "15m"
            and events.get(row["event_id"], {}).get("signal_role") == "qualified"
        ]
        nonexit_rate = (
            sum(not bool(row["exit_valid"]) for row in completed_15m) / len(completed_15m)
            if completed_15m else None
        )
        catastrophic_rate = (
            sum(safe_float(row["net_return"], -1.0) <= -0.90 for row in completed_15m)
            / len(completed_15m) if completed_15m else None
        )
        incremental = by_horizon["15m"]["paired_incremental_expectancy"]
        incremental_ci = by_horizon["15m"]["paired_incremental_ci_95"]
        gates = {
            "minimum_sample": len(completed_15m) >= FLOW_EVIDENCE_MINIMUM_PROMOTION_SIGNALS,
            "positive_incremental_expectancy": bool(
                incremental is not None and incremental > 0
                and incremental_ci is not None and incremental_ci[0] > 0
            ),
            "bounded_nonexit_rate": nonexit_rate is not None and nonexit_rate <= FLOW_EVIDENCE_MAXIMUM_NONEXIT_RATE,
            "bounded_catastrophic_rate": catastrophic_rate is not None and catastrophic_rate <= FLOW_EVIDENCE_MAXIMUM_CATASTROPHIC_RATE,
            "source_not_quarantined": not self.source_entry_risk(SOURCE_V4)["quarantined"],
            "policy_frozen": True,
        }
        return {
            "policy": self._flow_policy(), "cohort_id": self._flow_cohort_id(),
            "events": len(event_rows), "eligible_signals": len(eligible_signals),
            "historical_signals": sum(
                row["signal_role"] == "qualified" and not row["eligible_for_evaluation"]
                for row in event_rows
            ),
            "matched_controls": sum(row["signal_role"] == "matched_control" for row in event_rows),
            "entry_quotes_verified": sum(bool(row["quote_verified"]) for row in event_rows),
            "completed_15m": len(completed_15m), "nonexit_rate_15m": nonexit_rate,
            "catastrophic_rate_15m": catastrophic_rate,
            "by_horizon": by_horizon, "promotion_gates": gates,
            "promotion_ready": all(gates.values()),
            "next_reflection_at": (
                (len(eligible_signals) // FLOW_EVIDENCE_REFLECTION_INTERVAL + 1)
                * FLOW_EVIDENCE_REFLECTION_INTERVAL
            ),
            "admission_enabled": False,
        }

    def recent_flow_evidence_events(self, limit: int = 20) -> list[dict]:
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                """
                SELECT e.*,c.name,c.symbol
                FROM flow_signal_events e
                LEFT JOIN candidates c USING(token_address)
                ORDER BY e.signaled_at DESC,e.event_id DESC LIMIT ?
                """,
                (max(0,limit),),
            )]
        for row in rows:
            row["eligible_for_evaluation"] = bool(row["eligible_for_evaluation"])
            row["quote_verified"] = bool(row["quote_verified"])
            row["quote_exitable"] = bool(row["quote_exitable"])
            row["snapshot"] = json.loads(row.pop("snapshot_json") or "{}")
            row["quote"] = json.loads(row.pop("quote_json") or "{}")
        return rows

    def near_head_pending_origins(
        self, limit: int, *, head_block: int,
    ) -> list[str]:
        """Near-head unresolved transactions, without the range join.

        pending_transaction_origins builds one CTE that joins every
        swap_observations row against every flow_signals window on
        `block_number BETWEEN window_start AND window_end`. That is a
        cross-product no B-tree can serve, and it cost 44.7-56.9s per scope
        against a 60-second stage budget -- bounding it by block changed
        nothing, because the join is still evaluated.

        The join only ever existed to decide which window a swap belongs to.
        Asked the other way round it is two indexed lookups: flow_signals is
        small (2,176 rows) so the near-head windows come back immediately, and
        each window's transactions are a pool_id + block range, which
        idx_swap_observation_pool_block already serves exactly.

        Windows are taken newest-first and whole. A window at 70% identity
        coverage fails the 0.80 floor exactly as completely as one at 0%, so
        a partial window is wasted work.
        """
        floor = int(head_block) - FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS
        hashes: list[str] = []
        seen: set[str] = set()
        with self.connection() as connection:
            windows = connection.execute(
                """
                SELECT pool_id,window_start_block,window_end_block
                FROM flow_signals WHERE window_end_block >= ?
                ORDER BY window_end_block DESC
                """,
                (floor,),
            ).fetchall()
            for window in windows:
                if len(hashes) >= limit:
                    break
                for row in connection.execute(
                    """
                    SELECT so.transaction_hash
                    FROM swap_observations so
                    LEFT JOIN transaction_origins tx USING(transaction_hash)
                    WHERE so.pool_id=?
                      AND so.block_number BETWEEN ? AND ?
                      AND so.transaction_hash<>''
                      AND (tx.transaction_hash IS NULL OR (
                          tx.status<>'resolved' AND tx.attempts<?
                      ))
                    """,
                    (window["pool_id"], window["window_start_block"],
                     window["window_end_block"], FLOW_ORIGIN_MAXIMUM_ATTEMPTS),
                ):
                    digest = row[0]
                    if digest not in seen:
                        seen.add(digest)
                        hashes.append(digest)
        return hashes[:limit]

    def pending_transaction_origins(
        self, limit: int = FLOW_ORIGIN_RESOLUTION_LIMIT, *, scope: str = "all",
        head_block: int | None = None,
    ) -> list[str]:
        """Unresolved swap transactions, prospective cohort first.

        active_window alone is not a freshness test: a pool's latest-state row
        keeps whatever window it last had, so all 267 stale rows satisfied it
        and a fixed budget spread across windows that could never meet the
        120-block bound. None reached the 0.80 identity-coverage floor while
        origin resolution itself was succeeding on 33,522 of 33,522 attempts.

        The prospective scope adds the missing head-relative condition: only
        pools whose latest window ENDS within head-120..head, and then every
        transaction across those pools' full 450-block windows, so a fresh
        window is finished rather than half-covered.
        """
        if scope not in {"all", "active", "historical", "prospective", "background"}:
            raise ValueError(f"unsupported transaction-origin scope: {scope}")
        if scope in {"prospective", "background"} and head_block is None:
            raise ValueError(f"scope {scope!r} requires head_block")
        scope_clause = {
            "all": "1=1",
            "active": "active_window=1",
            "historical": "active_window=0",
            "prospective": "prospective=1",
            "background": "prospective=0",
        }[scope]
        fresh_floor = (
            int(head_block) - FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS
            if head_block is not None else None
        )
        # Bound the SCAN, not just the ranking. The CTE below range-joins every
        # swap_observations row against every flow_signals window on a BETWEEN,
        # and nothing confined it by block: with a 600,193-row backlog the
        # selection alone outran the whole 60-second stage budget, so the
        # identity stage reported planned=500, selected=0, resolved=0 -- it
        # spent 472.8 seconds deciding what to do and then did none of it.
        #
        # The live scopes only ever want near-head transactions, so they get a
        # floor. `historical` and `all` deliberately keep the full range: their
        # whole purpose is the backlog, and silently truncating it would turn a
        # slow query into a wrong one.
        #
        # The floor is interpolated as an int rather than bound as a parameter
        # because this statement's placeholders are positional and shared with
        # two other clauses; adding one would risk binding the wrong value to
        # the wrong slot, which SQLite accepts silently.
        # TWICE the bound, not once. A prospective window may END up to
        # FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS behind the head and SPANS that far
        # again backwards, so a transaction legitimately belonging to it sits
        # up to 2x behind. Flooring at 1x silently dropped the older half of
        # every window -- and a half-covered window fails the 0.80 identity
        # floor exactly as completely as an empty one, which is the whole
        # reason the scope takes entire windows rather than recent swaps.
        scan_floor = (
            int(fresh_floor) - FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS
            if fresh_floor is not None else None
        )
        floor_clause = (
            f"AND so.block_number >= {scan_floor}"
            if scan_floor is not None
            # NOT background: that scope exists to reach the backlog, and a
            # floor would make it return nothing at all.
            and scope in {"prospective", "active"}
            else ""
        )
        with self.connection() as connection:
            return [row[0] for row in connection.execute(
                f"""
                WITH pending AS (
                    SELECT so.transaction_hash,
                           MAX(CASE WHEN fs.pool_id IS NOT NULL
                                    AND so.block_number BETWEEN
                                        fs.window_start_block AND fs.window_end_block
                               THEN 1 ELSE 0 END) active_window,
                           MAX(CASE WHEN fs.pool_id IS NOT NULL
                                    AND so.block_number BETWEEN
                                        fs.window_start_block AND fs.window_end_block
                               THEN fs.swap_count ELSE 0 END) active_swap_count,
                           MAX(CASE WHEN fs.pool_id IS NOT NULL
                                    AND so.block_number BETWEEN
                                        fs.window_start_block AND fs.window_end_block
                               THEN fs.net_anchor_flow_fraction ELSE -2 END) active_net_flow,
                           MAX(CASE WHEN fs.pool_id IS NOT NULL
                                    AND ? IS NOT NULL
                                    AND fs.window_end_block >= ?
                                    AND so.block_number BETWEEN
                                        fs.window_start_block AND fs.window_end_block
                               THEN 1 ELSE 0 END) prospective,
                           MIN(so.block_number) oldest_block,
                           MAX(so.block_number) newest_block
                    FROM swap_observations so
                    LEFT JOIN flow_signals fs ON fs.pool_id=so.pool_id
                    LEFT JOIN transaction_origins tx USING(transaction_hash)
                    WHERE so.transaction_hash<>''
                      {floor_clause}
                      AND (tx.transaction_hash IS NULL OR (
                          tx.status<>'resolved' AND tx.attempts<?
                      ))
                    GROUP BY so.transaction_hash
                )
                SELECT transaction_hash FROM pending
                WHERE {scope_clause}
                ORDER BY prospective DESC,active_window DESC,
                         active_swap_count DESC,active_net_flow DESC,
                         CASE WHEN active_window=1 THEN newest_block END DESC,
                         oldest_block,transaction_hash
                LIMIT ?
                """,
                (fresh_floor, fresh_floor, FLOW_ORIGIN_MAXIMUM_ATTEMPTS,
                 max(0, limit)),
            )]

    def prospective_enrichment_counts(self, head_block: int) -> dict:
        """Queue depths split by cohort, plus fresh-pool completion.

        Reported separately because a single pending total cannot distinguish
        a starved prospective cohort from a large background backlog, and the
        two call for opposite responses.
        """
        floor = int(head_block) - FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
        with self.connection() as connection:
            fresh_pools = [dict(row) for row in connection.execute(
                """
                SELECT pool_id, window_start_block, window_end_block,
                       identity_coverage
                FROM flow_signals WHERE window_end_block >= ?
                """,
                (floor,),
            )]
            pending = connection.execute(
                """
                SELECT
                  SUM(CASE WHEN fresh.pool_id IS NOT NULL THEN 1 ELSE 0 END) prospective,
                  SUM(CASE WHEN fresh.pool_id IS NULL THEN 1 ELSE 0 END) background
                FROM (
                    SELECT DISTINCT so.transaction_hash, so.pool_id, so.block_number
                    FROM swap_observations so
                    LEFT JOIN transaction_origins tx USING(transaction_hash)
                    WHERE so.transaction_hash<>''
                      AND (tx.transaction_hash IS NULL
                           OR (tx.status<>'resolved' AND tx.attempts<?))
                ) s
                LEFT JOIN (
                    SELECT pool_id, window_start_block, window_end_block
                    FROM flow_signals WHERE window_end_block >= ?
                ) fresh
                  ON fresh.pool_id = s.pool_id
                 AND s.block_number BETWEEN fresh.window_start_block
                                        AND fresh.window_end_block
                """,
                (FLOW_ORIGIN_MAXIMUM_ATTEMPTS, floor),
            ).fetchone()
            resolved = connection.execute(
                """
                SELECT COUNT(*) FROM transaction_origins tx
                JOIN swap_observations so USING(transaction_hash)
                JOIN flow_signals fs ON fs.pool_id=so.pool_id
                WHERE tx.status='resolved' AND fs.window_end_block >= ?
                  AND so.block_number BETWEEN fs.window_start_block
                                          AND fs.window_end_block
                """,
                (floor,),
            ).fetchone()[0]
        completed = sum(
            1 for pool in fresh_pools
            if safe_float(pool.get("identity_coverage"), 0.0)
            >= FLOW_MINIMUM_IDENTITY_COVERAGE
        )
        return {
            "head_block": int(head_block),
            "fresh_window_floor_block": floor,
            "fresh_pools": len(fresh_pools),
            "prospective_pending": int(pending["prospective"] or 0),
            "prospective_resolved": int(resolved or 0),
            "background_pending": int(pending["background"] or 0),
            "fresh_pools_completed_80pct": completed,
            "identity_coverage_floor": FLOW_MINIMUM_IDENTITY_COVERAGE,
        }

    def pending_transaction_origin_counts(self) -> dict:
        with self.connection() as connection:
            row = connection.execute(
                """
                WITH pending AS (
                    SELECT so.transaction_hash,
                           MAX(CASE WHEN fs.pool_id IS NOT NULL
                                    AND so.block_number BETWEEN
                                        fs.window_start_block AND fs.window_end_block
                               THEN 1 ELSE 0 END) active_window
                    FROM swap_observations so
                    LEFT JOIN flow_signals fs ON fs.pool_id=so.pool_id
                    LEFT JOIN transaction_origins tx USING(transaction_hash)
                    WHERE so.transaction_hash<>''
                      AND (tx.transaction_hash IS NULL OR (
                          tx.status<>'resolved' AND tx.attempts<?
                      ))
                    GROUP BY so.transaction_hash
                )
                SELECT COUNT(*) total,
                       COALESCE(SUM(active_window),0) active,
                       COALESCE(SUM(1-active_window),0) historical
                FROM pending
                """,
                (FLOW_ORIGIN_MAXIMUM_ATTEMPTS,),
            ).fetchone()
        return {key: int(row[key] or 0) for key in ("total", "active", "historical")}

    def flow_window_coverage(self, pool_ids: list[str]) -> dict:
        """Summarise the flow windows belonging to one named set of pools.

        flow_signals is written by two different generating processes -- the
        wide batch discovery recompute and the near-head pass -- so a coverage
        figure read off the whole table belongs to neither. One cycle was read
        as 1 of 42 windows at full coverage when the near-head pass had in fact
        produced 15 of 15; the batch rows were the ones counted. Scoping the
        summary to an explicit pool set is what makes the number attributable.
        """
        wanted = [p for p in dict.fromkeys(pool_ids) if p]
        if not wanted:
            return {"windows": 0, "at_full_identity_coverage": 0,
                    "qualified": 0, "evidence": {}}
        placeholders = ",".join("?" * len(wanted))
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT identity_coverage, qualification_gaps_json, features_json"
                " FROM flow_signals WHERE pool_id IN (" + placeholders + ")",
                wanted,
            ).fetchall()
        evidence: dict[str, int] = {}
        full = qualified = 0
        for row in rows:
            if float(row["identity_coverage"] or 0.0) >= 0.999:
                full += 1
            if not json.loads(row["qualification_gaps_json"] or "[]"):
                qualified += 1
            label = str(json.loads(row["features_json"] or "{}").get(
                "participant_evidence") or "none")
            evidence[label] = evidence.get(label, 0) + 1
        return {
            "windows": len(rows), "at_full_identity_coverage": full,
            "qualified": qualified, "evidence": evidence,
        }

    def unresolved_transaction_hashes(self, hashes: list[str]) -> list[str]:
        """Filter a caller-supplied hash list down to the ones still pending.

        The near-head window is re-ingested every cycle, so most of its
        transactions are already resolved; re-requesting them would spend the
        budget re-proving what is known.
        """
        wanted = [h for h in dict.fromkeys(hashes) if h]
        if not wanted:
            return []
        placeholders = ",".join("?" * len(wanted))
        with self.connection() as connection:
            resolved = {row[0] for row in connection.execute(
                "SELECT transaction_hash FROM transaction_origins"
                " WHERE status='resolved' AND transaction_hash IN"
                " (" + placeholders + ")",
                wanted,
            )}
        return [h for h in wanted if h not in resolved]

    def record_transaction_origins(self, records: list[dict]) -> dict:
        resolved = unavailable = 0
        affected_pools = set()
        with self.connection() as connection:
            for record in records:
                transaction_hash = str(record.get("transaction_hash") or "")
                transaction = record.get("transaction") or {}
                origin = str(transaction.get("from") or "").lower()
                destination = str(transaction.get("to") or "").lower() or None
                valid_origin = bool(ADDRESS_RE.fullmatch(origin))
                status = "resolved" if valid_origin else "unavailable"
                error = record.get("error")
                error_text = (
                    _canonical(error)[:500] if error else (
                        None if valid_origin else "transaction envelope unavailable"
                    )
                )
                block_number = safe_int(transaction.get("blockNumber"), 0)
                if isinstance(transaction.get("blockNumber"), str):
                    try:
                        block_number = int(transaction["blockNumber"], 16)
                    except ValueError:
                        block_number = 0
                connection.execute(
                    """
                    INSERT INTO transaction_origins (
                        transaction_hash,origin_address,destination_address,
                        block_number,status,attempts,last_attempt_at,resolved_at,error
                    ) VALUES (?,?,?,?,?,1,?,?,?)
                    ON CONFLICT(transaction_hash) DO UPDATE SET
                        origin_address=excluded.origin_address,
                        destination_address=excluded.destination_address,
                        block_number=excluded.block_number,status=excluded.status,
                        attempts=transaction_origins.attempts+1,
                        last_attempt_at=excluded.last_attempt_at,
                        resolved_at=excluded.resolved_at,error=excluded.error
                    """,
                    (
                        transaction_hash,origin if valid_origin else None,
                        destination,block_number or None,status,time.time(),
                        _utc_now() if valid_origin else None,error_text,
                    ),
                )
                pools = connection.execute(
                    "SELECT DISTINCT pool_id FROM swap_observations WHERE transaction_hash=?",
                    (transaction_hash,),
                ).fetchall()
                affected_pools.update(row[0] for row in pools)
                if valid_origin:
                    connection.execute(
                        """
                        UPDATE swap_observations SET resolved_participant=?,
                            participant_identity_kind=CASE
                                WHEN LOWER(COALESCE(sender_hint,''))=?
                                  THEN 'transaction_from_matches_event_sender'
                                ELSE 'transaction_from_via_intermediary'
                            END,
                            identity_resolved_at=?
                        WHERE transaction_hash=?
                        """,
                        (origin,origin,_utc_now(),transaction_hash),
                    )
                    resolved += 1
                else:
                    unavailable += 1
            for pool_id in affected_pools:
                self._refresh_v4_flow_signal(connection, pool_id)
        return {
            "resolved": resolved, "unavailable": unavailable,
            "affected_pools": len(affected_pools),
        }

    def pending_v4_activations(self) -> list[dict]:
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """SELECT * FROM v4_pools WHERE promoted=0 AND modified_block IS NOT NULL
                   AND swapped_block IS NOT NULL ORDER BY swapped_block,pool_id"""
            )]

    def known_v4_pool_ids(self) -> set[str]:
        with self.connection() as connection:
            return {row[0] for row in connection.execute("SELECT pool_id FROM v4_pools")}

    def mark_v4_promoted(self, pool_ids: list[str]) -> None:
        if not pool_ids:
            return
        with self.connection() as connection:
            connection.executemany(
                "UPDATE v4_pools SET promoted=1,updated_at=? WHERE pool_id=?",
                [(_utc_now(), pool_id) for pool_id in pool_ids],
            )

    def v4_pool(self, pool_id: str) -> dict | None:
        with self.connection() as connection:
            row = connection.execute("SELECT * FROM v4_pools WHERE pool_id=?", (pool_id.lower(),)).fetchone()
            return dict(row) if row else None

    def known_v4_pools(self) -> list[dict]:
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                "SELECT * FROM v4_pools ORDER BY initialized_block,pool_id"
            )]

    def record_v4_position_transfer(
        self, *, token_id: int, pool_id: str, tick_lower: int,
        tick_upper: int, transaction_hash: str, log_index: int,
        from_address: str, to_address: str, block_number: int,
    ) -> None:
        """Persist only transfers whose NFT maps to a known full V4 pool id."""
        now = _utc_now()
        token_text = str(token_id)
        with self.connection() as connection:
            connection.execute(
                """
                INSERT INTO v4_positions (
                    token_id,pool_id,tick_lower,tick_upper,owner_address,
                    last_transfer_block,created_at,updated_at
                ) VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(token_id) DO UPDATE SET
                    pool_id=excluded.pool_id,tick_lower=excluded.tick_lower,
                    tick_upper=excluded.tick_upper,
                    owner_address=excluded.owner_address,
                    last_transfer_block=MAX(
                        COALESCE(v4_positions.last_transfer_block,0),
                        excluded.last_transfer_block
                    ),updated_at=excluded.updated_at
                """,
                (
                    token_text,pool_id.lower(),tick_lower,tick_upper,
                    to_address.lower(),block_number,now,now,
                ),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO v4_position_transfers (
                    transaction_hash,log_index,token_id,pool_id,from_address,
                    to_address,block_number,observed_at
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    transaction_hash,log_index,token_text,pool_id.lower(),
                    from_address.lower(),to_address.lower(),block_number,now,
                ),
            )

    def v4_positions_for_refresh(self, limit: int) -> list[dict]:
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """
                SELECT p.*,v.last_tick current_tick
                FROM v4_positions p JOIN v4_pools v USING(pool_id)
                ORDER BY COALESCE(p.last_checked_block,-1),
                         COALESCE(p.last_transfer_block,-1),p.token_id
                LIMIT ?
                """,
                (max(0, limit),),
            )]

    def update_v4_position_state(
        self, token_id: int, *, owner_address: str | None,
        approved_address: str | None, liquidity_raw: int, in_active_range: bool,
        custody_class: str, locker_platform: str | None,
        unlock_timestamp: float | None, owner_code_sha256: str | None,
        checked_block: int, evidence: dict,
    ) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE v4_positions SET owner_address=?,approved_address=?,
                    liquidity_raw=?,in_active_range=?,custody_class=?,
                    locker_platform=?,unlock_timestamp=?,owner_code_sha256=?,
                    last_checked_block=?,last_checked_at=?,evidence_json=?,
                    updated_at=? WHERE token_id=?
                """,
                (
                    owner_address,approved_address,str(max(0, liquidity_raw)),
                    int(in_active_range),custody_class,locker_platform,
                    unlock_timestamp,owner_code_sha256,checked_block,_utc_now(),
                    _canonical(evidence),_utc_now(),str(token_id),
                ),
            )

    def record_v4_custody_snapshot(
        self, pool_id: str, *, observed_block: int,
    ) -> dict | None:
        with self.connection() as connection:
            pool = connection.execute(
                "SELECT * FROM v4_pools WHERE pool_id=?", (pool_id.lower(),)
            ).fetchone()
            if not pool:
                return None
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM v4_positions WHERE pool_id=? ORDER BY token_id",
                (pool_id.lower(),),
            )]
            fresh = [
                row for row in rows
                if safe_int(row.get("last_checked_block"), -1)
                    >= observed_block - V4_CUSTODY_MAX_POSITION_STATE_LAG_BLOCKS
            ]
            active = [row for row in fresh if row.get("in_active_range")]
            core = max(0, safe_int(pool["active_liquidity"], 0))
            if core <= 0:
                return None
            managed = sum(safe_int(row.get("liquidity_raw"), 0) for row in active)
            locked = sum(
                safe_int(row.get("liquidity_raw"), 0) for row in active
                if row.get("custody_class") == "verified_locked"
            )
            contract_unverified = sum(
                safe_int(row.get("liquidity_raw"), 0) for row in active
                if row.get("custody_class") == "contract_custody_unverified"
            )
            eoa = sum(
                safe_int(row.get("liquidity_raw"), 0) for row in active
                if row.get("custody_class") == "eoa_controlled"
            )
            approved = sum(
                safe_int(row.get("liquidity_raw"), 0) for row in active
                if str(row.get("approved_address") or ZERO_ADDRESS).lower()
                    != ZERO_ADDRESS
            )
            coverage = min(1.0, managed / core) if core else 0.0
            locked_fraction = min(1.0, locked / core) if core else 0.0
            approved_fraction = min(1.0, approved / core) if core else 0.0
            if coverage < V4_CUSTODY_MINIMUM_MANAGED_ACTIVE_COVERAGE:
                verdict = "insufficient_position_coverage"
            elif approved_fraction > 0:
                verdict = "approved_delegate_can_withdraw"
            elif locked_fraction >= V4_CUSTODY_MINIMUM_LOCKED_ACTIVE_FRACTION:
                verdict = "verified_locked"
            elif contract_unverified:
                verdict = "contract_custody_unverified"
            else:
                verdict = "eoa_controlled"
            evidence = {
                "schema_version": 1,
                "position_manager": UNISWAP_V4_POSITION_MANAGER,
                "fresh_positions": len(fresh),
                "stale_positions_excluded": len(rows) - len(fresh),
                "coverage_is_relative_to_core_active_liquidity": True,
                "maximum_position_state_lag_blocks": (
                    V4_CUSTODY_MAX_POSITION_STATE_LAG_BLOCKS
                ),
                "unmanaged_liquidity_is_never_assumed_locked": True,
                "operator_approval_enumeration_limit": (
                    "token-specific getApproved is checked; arbitrary "
                    "setApprovalForAll operators cannot be enumerated on-chain"
                ),
            }
            snapshot = {
                "pool_id": pool_id.lower(),
                "observed_block": observed_block,
                "observed_at": _utc_now(),
                "current_tick": pool["last_tick"],
                "core_active_liquidity_raw": str(core),
                "tracked_positions": len(rows),
                "tracked_active_positions": len(active),
                "managed_active_liquidity_raw": str(managed),
                "verified_locked_active_liquidity_raw": str(locked),
                "contract_unverified_active_liquidity_raw": str(contract_unverified),
                "eoa_active_liquidity_raw": str(eoa),
                "approved_active_liquidity_raw": str(approved),
                "managed_active_coverage": coverage,
                "verified_locked_active_fraction": locked_fraction,
                "approved_active_fraction": approved_fraction,
                "custody_verdict": verdict,
                "evidence": evidence,
            }
            connection.execute(
                """
                INSERT INTO v4_custody_snapshots (
                    pool_id,observed_block,observed_at,current_tick,
                    core_active_liquidity_raw,tracked_positions,
                    tracked_active_positions,managed_active_liquidity_raw,
                    verified_locked_active_liquidity_raw,
                    contract_unverified_active_liquidity_raw,
                    eoa_active_liquidity_raw,approved_active_liquidity_raw,
                    managed_active_coverage,verified_locked_active_fraction,
                    approved_active_fraction,custody_verdict,evidence_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(pool_id,observed_block) DO UPDATE SET
                    observed_at=excluded.observed_at,current_tick=excluded.current_tick,
                    core_active_liquidity_raw=excluded.core_active_liquidity_raw,
                    tracked_positions=excluded.tracked_positions,
                    tracked_active_positions=excluded.tracked_active_positions,
                    managed_active_liquidity_raw=excluded.managed_active_liquidity_raw,
                    verified_locked_active_liquidity_raw=excluded.verified_locked_active_liquidity_raw,
                    contract_unverified_active_liquidity_raw=excluded.contract_unverified_active_liquidity_raw,
                    eoa_active_liquidity_raw=excluded.eoa_active_liquidity_raw,
                    approved_active_liquidity_raw=excluded.approved_active_liquidity_raw,
                    managed_active_coverage=excluded.managed_active_coverage,
                    verified_locked_active_fraction=excluded.verified_locked_active_fraction,
                    approved_active_fraction=excluded.approved_active_fraction,
                    custody_verdict=excluded.custody_verdict,
                    evidence_json=excluded.evidence_json
                """,
                (
                    snapshot["pool_id"],snapshot["observed_block"],
                    snapshot["observed_at"],snapshot["current_tick"],
                    snapshot["core_active_liquidity_raw"],
                    snapshot["tracked_positions"],
                    snapshot["tracked_active_positions"],
                    snapshot["managed_active_liquidity_raw"],
                    snapshot["verified_locked_active_liquidity_raw"],
                    snapshot["contract_unverified_active_liquidity_raw"],
                    snapshot["eoa_active_liquidity_raw"],
                    snapshot["approved_active_liquidity_raw"],
                    snapshot["managed_active_coverage"],
                    snapshot["verified_locked_active_fraction"],
                    snapshot["approved_active_fraction"],
                    snapshot["custody_verdict"],_canonical(evidence),
                ),
            )
        return snapshot

    def latest_v4_custody(self, pool_id: str) -> dict | None:
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT * FROM v4_custody_snapshots WHERE pool_id=?
                ORDER BY observed_block DESC LIMIT 1
                """,
                (pool_id.lower(),),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        result["evidence"] = json.loads(result.pop("evidence_json") or "{}")
        return result

    def v4_custody_summary(self, limit: int = 12) -> dict:
        with self.connection() as connection:
            all_rows = [dict(row) for row in connection.execute(
                """
                SELECT s.*,v.token_address,c.name,c.symbol
                FROM v4_custody_snapshots s
                JOIN v4_pools v USING(pool_id)
                LEFT JOIN candidates c ON c.pool_id=s.pool_id
                WHERE s.observed_block=(
                    SELECT MAX(s2.observed_block)
                    FROM v4_custody_snapshots s2 WHERE s2.pool_id=s.pool_id
                )
                  AND s.core_active_liquidity_raw<>'0'
                ORDER BY s.observed_at DESC,s.pool_id
                """,
            )]
            rows = all_rows[:max(0, limit)]
            prospective_pools = connection.execute(
                """SELECT COUNT(DISTINCT pool_id) FROM v4_custody_snapshots
                   WHERE core_active_liquidity_raw<>'0'"""
            ).fetchone()[0]
        complete = sum(
            safe_float(row.get("managed_active_coverage"), 0.0)
                >= V4_CUSTODY_MINIMUM_MANAGED_ACTIVE_COVERAGE
            for row in all_rows
        )
        locked = sum(
            row.get("custody_verdict") == "verified_locked" for row in all_rows
        )
        return {
            "policy_version": "v4-position-custody-shadow-v1",
            "paper_admission_enabled": V4_PAPER_ADMISSION_ENABLED,
            "position_manager": UNISWAP_V4_POSITION_MANAGER,
            "prospective_pools": prospective_pools,
            "latest_pools": len(all_rows),
            "coverage_complete_pools": complete,
            "verified_locked_pools": locked,
            "minimum_prospective_pools": V4_CUSTODY_MINIMUM_PROSPECTIVE_POOLS,
            "evidence_ready_for_canary_review": bool(
                complete >= V4_CUSTODY_MINIMUM_PROSPECTIVE_POOLS
            ),
            "historical_replay": {
                "status": "not_qualified",
                "reason": "archived closes lack block-pinned V4 position ownership evidence",
            },
            "pools": rows,
        }

    def pending_analysis(self, limit: int) -> list[dict]:
        limit = max(0, limit)
        if not limit:
            return []
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                """
                SELECT c.*,
                    MAX(fs.shadow_score) flow_shadow_score,
                    MAX(fs.shadow_qualified) flow_shadow_qualified,
                    MAX(CASE
                        WHEN cp.status='observed'
                         AND cp.learning_eligible=1
                         AND cp.horizon_seconds>=?
                         AND cp.market_cap_multiple>=?
                         AND cp.liquidity_usd>=?
                         AND cp.market_cap_usd>0
                         AND cp.market_cap_usd<=?
                         AND cp.market_cap_usd/cp.liquidity_usd<=?
                        THEN cp.market_cap_multiple * CASE
                            WHEN cp.market_cap_usd>=? THEN 0.5 ELSE 1.0 END
                    END) momentum_priority_multiple
                FROM candidates c
                LEFT JOIN checkpoints cp ON cp.token_address=c.token_address
                LEFT JOIN flow_signals fs ON fs.pool_id=c.pool_id
                WHERE c.analysis_status IN ('pending','retry')
                  AND c.analysis_attempts < ?
                GROUP BY c.token_address
                """,
                (
                    MOMENTUM_PRIORITY_MINIMUM_HORIZON_SECONDS,
                    MOMENTUM_PRIORITY_MULTIPLE,
                    MINIMUM_ENTRY_LIQUIDITY_USD,
                    MAXIMUM_ENTRY_MARKET_CAP_USD,
                    MOMENTUM_PRIORITY_MAXIMUM_CAP_TO_LIQUIDITY,
                    ENTRY_PRIORITY_PENALTY_MARKET_CAP_USD,
                    MAXIMUM_ANALYSIS_ATTEMPTS,
                ),
            )]
        oldest = sorted(
            rows,
            key=lambda row: (
                0 if row["analysis_status"] == "pending" else 1,
                row["block_number"],
                row["log_index"],
            ),
        )
        momentum = sorted(
            (row for row in rows if row.get("momentum_priority_multiple") is not None),
            key=lambda row: (
                0 if row["analysis_status"] == "pending" else 1,
                -safe_float(row.get("momentum_priority_multiple"), 0.0),
                row["block_number"],
            ),
        )
        flow = sorted(
            (row for row in rows if row.get("flow_shadow_qualified")),
            key=lambda row: (
                0 if row["analysis_status"] == "pending" else 1,
                -safe_float(row.get("flow_shadow_score"), 0.0),
                row["block_number"],
            ),
        )
        selected = []
        if flow:
            selected.append(flow[0])
        if momentum and len(selected) < limit:
            if not any(
                item["token_address"] == momentum[0]["token_address"]
                for item in selected
            ):
                selected.append(momentum[0])
        for row in oldest:
            if len(selected) >= limit:
                break
            if any(item["token_address"] == row["token_address"] for item in selected):
                continue
            selected.append(row)
        return selected

    def source_entry_risk(self, source_version: str | None) -> dict:
        """Return a rolling realized-loss circuit breaker for one DEX source."""
        if not source_version:
            return {
                "source_version": source_version, "closed_sample": 0,
                "total_losses": 0, "total_loss_rate": 0.0,
                "quarantined": False,
            }
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT p.net_multiple,p.exit_reason
                FROM positions p JOIN candidates c USING(token_address)
                WHERE p.status='closed' AND p.net_multiple IS NOT NULL
                  AND c.source_version=?
                ORDER BY p.closed_at DESC,p.token_address
                LIMIT ?
                """,
                (source_version, SOURCE_RISK_LOOKBACK_CLOSES),
            ).fetchall()
        total_losses = sum(
            safe_float(row["net_multiple"], 0.0) <= 0.01
            or row["exit_reason"] == "price_collapse"
            for row in rows
        )
        rate = total_losses / len(rows) if rows else 0.0
        return {
            "source_version": source_version,
            "closed_sample": len(rows),
            "total_losses": total_losses,
            "total_loss_rate": round(rate, 4),
            "quarantined": bool(
                len(rows) >= SOURCE_RISK_MINIMUM_CLOSES
                and rate >= SOURCE_RISK_MAXIMUM_TOTAL_LOSS_RATE
            ),
            "minimum_closes": SOURCE_RISK_MINIMUM_CLOSES,
            "maximum_total_loss_rate": SOURCE_RISK_MAXIMUM_TOTAL_LOSS_RATE,
            "lookback_closes": SOURCE_RISK_LOOKBACK_CLOSES,
        }

    def record_analysis(
        self,
        token: str,
        analysis: dict,
        market: dict,
        *,
        priority_reason: str | None = None,
        queue_age_seconds: float | None = None,
        report_data: dict | None = None,
    ) -> None:
        hard_stops = [
            (item.get("code") or item.get("reason")) if isinstance(item, dict) else str(item)
            for item in analysis.get("hard_stop_overrides") or []
        ]
        score = safe_float(analysis.get("legitimacy_score"), 0.0)
        risk = str(analysis.get("risk_level") or "Unknown")
        liquidity = safe_float(market.get("liquidity_usd"), 0.0)
        price = safe_float(market.get("price_usd"), 0.0)
        market_cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        fdv = safe_float(market.get("fdv_usd"), 0.0) or None
        existing = self.candidate(token) or {}
        source_version = existing.get("source_version")
        # The global adaptive score proved non-predictive and hid three of the
        # four observed winners below its raised floor. Keep the analyzer score
        # as a base safety threshold, but do not let the old global override
        # leak across source-specific policies.
        minimum_entry_score = MINIMUM_ENTRY_SCORE
        token_quality_passes = bool(
            score >= minimum_entry_score and risk in {"Low", "Medium"}
        )
        permanent_stops = set(hard_stops) - TEMPORARY_EXECUTION_STOPS
        above_cap = bool(
            market_cap is not None
            and market_cap > MAXIMUM_ENTRY_MARKET_CAP_USD
        )
        counterfactual_allowed = bool(
            token_quality_passes
            and not hard_stops
            and liquidity >= MINIMUM_ENTRY_LIQUIDITY_USD
            and price > 0
            and market_cap is not None
            and not above_cap
        )
        v4_shadow_only = bool(
            source_version == SOURCE_V4 and not V4_PAPER_ADMISSION_ENABLED
        )
        source_risk = self.source_entry_risk(source_version)
        source_quarantined = bool(source_risk["quarantined"])
        preliminary_allowed = bool(
            counterfactual_allowed and not v4_shadow_only
            and not source_quarantined
        )
        safety = extract_safety_signals(analysis, report_data)
        deliberation = deliberate_entry(
            safety, score=score, risk_level=risk,
            allowed=preliminary_allowed, enforced=True,
        )
        # Only an explicit refusal is enforced. Missing evidence remains an
        # auditable abstention while coverage improves, rather than freezing
        # the complete V2/V3 learning lane.
        safety_refused = deliberation["judgment"] == "refuse"
        allowed = bool(preliminary_allowed and not safety_refused)
        watching = bool(
            token_quality_passes and not permanent_stops and not allowed
            and not source_quarantined and not safety_refused
            and (
                not price or liquidity < MINIMUM_ENTRY_LIQUIDITY_USD
                or market_cap is None
            )
        )
        observing_above_cap = bool(
            token_quality_passes and not permanent_stops and above_cap
            and not source_quarantined and not safety_refused
            and liquidity >= MINIMUM_ENTRY_LIQUIDITY_USD and price > 0
        )
        watched = watching or observing_above_cap
        decision = (
            PAPER_DECISION_ADMITTED if allowed else
            PAPER_DECISION_V4_SHADOW if v4_shadow_only and counterfactual_allowed else
            PAPER_DECISION_SOURCE_QUARANTINE
                if source_quarantined and counterfactual_allowed else
            PAPER_DECISION_ABOVE_CAP if observing_above_cap else
            PAPER_DECISION_WATCHING if watching else PAPER_DECISION_REJECTED
        )
        hooks_address = str(existing.get("hooks_address") or "").lower()
        hooks_present = (
            bool(hooks_address and hooks_address != ZERO_ADDRESS)
            if existing.get("source_version") == SOURCE_V4 else None
        )
        safety["entry_deliberation"] = deliberation
        safety["source_entry_risk"] = source_risk
        safety["raw_safety_refusal_enforced"] = safety_refused
        shadow = shadow_admission(
            market_cap=market_cap, liquidity=liquidity,
            hooks_present=hooks_present, hard_stops=analysis.get("hard_stop_overrides") or [],
            token_quality_passes=token_quality_passes, allowed=allowed,
            counterfactual_allowed=counterfactual_allowed,
            safety=safety,
        )
        shadow["v4_shadow_only"] = v4_shadow_only
        shadow["minimum_entry_score"] = minimum_entry_score
        shadow["source_entry_risk"] = source_risk
        shadow["raw_safety_refusal_enforced"] = safety_refused
        if source_version == SOURCE_V4:
            execution_quote = dict(market.get("execution_quote") or {})
            shadow["execution_quote_verified"] = bool(
                market.get("executable_quote_verified")
            )
            shadow["execution_round_trip_ratio"] = execution_quote.get(
                "round_trip_ratio"
            )
            shadow["execution_quote_passes"] = bool(
                execution_quote.get("passes_round_trip_limit")
            )
            shadow["v4_position_custody"] = v4_shadow_gates(
                market, hooks_present=bool(hooks_present),
            )
        now = time.time()
        analyzed_at = datetime.fromtimestamp(now, timezone.utc).isoformat()
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET analysis_status='complete',
                    analysis_attempts=analysis_attempts+1, analyzed_at=?,
                    outcome_anchor_at=COALESCE(outcome_anchor_at,?), score=?,
                    risk_level=?, action_label=?, hard_stops_json=?,
                    paper_entry_allowed=?, entry_price_usd=?,
                    entry_liquidity_usd=?, first_market_cap_usd=COALESCE(first_market_cap_usd,?),
                    peak_market_cap_usd=MAX(COALESCE(peak_market_cap_usd,0),COALESCE(?,0)),
                    peak_fdv_usd=MAX(COALESCE(peak_fdv_usd,0),COALESCE(?,0)), updated_at=?
                    ,market_evidence_json=?,analysis_priority_reason=?,
                    analysis_queue_age_seconds=?,paper_decision=?,
                    market_watch_started_at=CASE WHEN ? THEN COALESCE(market_watch_started_at,?) ELSE market_watch_started_at END,
                    market_watch_last_checked_at=market_watch_last_checked_at,
                    market_watch_checks=market_watch_checks,
                    market_watch_expires_at=CASE WHEN ? THEN COALESCE(market_watch_expires_at,?) ELSE market_watch_expires_at END,
                    market_watch_reason=CASE WHEN ? THEN ? ELSE market_watch_reason END,
                    market_watch_reference_price_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_price_usd END,
                    market_watch_reference_market_cap_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_market_cap_usd END,
                    market_watch_reference_at=CASE WHEN ? THEN ? ELSE market_watch_reference_at END,
                    mcap_liquidity_ratio=?, hooks_present=?, shadow_admission_json=?,
                    safety_signals_json=?, top_holder_pct=?,
                    holder_distribution_score=?, lp_lock_score=?
                WHERE token_address=?
                """,
                (
                    analyzed_at, now, score, risk, analysis.get("action_label"),
                    _canonical(hard_stops), int(allowed), price or None,
                    liquidity or None, market_cap, market_cap, fdv, _utc_now(),
                    _canonical(market), priority_reason, queue_age_seconds, decision,
                    int(watched), now, int(watched),
                    now + EXECUTABLE_MARKET_WATCH_SECONDS, int(watched),
                    (
                        "above_entry_market_cap_ceiling" if observing_above_cap
                        else "market_unverified" if not price
                        else "market_cap_unavailable" if market_cap is None
                        else "liquidity_below_minimum"
                    ),
                    int(observing_above_cap), price or None,
                    int(observing_above_cap), market_cap,
                    int(observing_above_cap), now,
                    shadow["mcap_liquidity_ratio"],
                    None if hooks_present is None else int(hooks_present),
                    _canonical(shadow),
                    _canonical(safety), safety.get("top_holder_pct"),
                    safety.get("holder_distribution_score"),
                    safety.get("lp_lock_score"),
                    token.lower(),
                ),
            )

    def record_analysis_producer_reference(
        self, token: str, ring: dict, evidence_hash: str | None,
    ) -> None:
        """Bind the mutable learner projection to its immutable producer ring."""
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET producer_analysis_ring_index=?,
                    producer_analysis_ring_hash=?,producer_evidence_hash=?,updated_at=?
                WHERE token_address=? AND producer_analysis_ring_index IS NULL
                """,
                (
                    ring.get("index"), ring.get("ring_hash"), evidence_hash,
                    _utc_now(), token.lower(),
                ),
            )

    def pending_market_watches(self, now: float, limit: int) -> list[dict]:
        if limit <= 0:
            return []
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET paper_decision=?,market_watch_reason='watch_expired',
                    updated_at=? WHERE paper_decision IN (?,?,?) AND market_watch_expires_at<=?
                """,
                (
                    PAPER_DECISION_EXPIRED, _utc_now(), PAPER_DECISION_WATCHING,
                    PAPER_DECISION_ABOVE_CAP, PAPER_DECISION_REENTRY, now,
                ),
            )
            return [dict(row) for row in connection.execute(
                """
                SELECT * FROM candidates WHERE paper_decision IN (?,?,?)
                  AND market_watch_expires_at>?
                  AND (market_watch_last_checked_at IS NULL OR market_watch_last_checked_at<=?)
                ORDER BY COALESCE(market_watch_last_checked_at,0),market_watch_started_at,token_address
                LIMIT ?
                """,
                (
                    PAPER_DECISION_WATCHING, PAPER_DECISION_ABOVE_CAP,
                    PAPER_DECISION_REENTRY, now,
                    now - EXECUTABLE_MARKET_RECHECK_SECONDS, limit,
                ),
            )]

    def record_market_watch_check(
        self, token: str, market: dict | None = None, *, reason: str | None = None,
        update_reference: bool = False, decision: str | None = None,
    ) -> None:
        evidence = _canonical(market or {})
        reason = reason or (
            "executable_market_found" if market else "no_executable_market"
        )
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET market_watch_last_checked_at=?,
                    market_watch_checks=market_watch_checks+1,market_watch_reason=?,
                    market_evidence_json=CASE WHEN ? THEN ? ELSE market_evidence_json END,
                    market_watch_reference_price_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_price_usd END,
                    market_watch_reference_market_cap_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_market_cap_usd END,
                    market_watch_reference_at=CASE WHEN ? THEN ? ELSE market_watch_reference_at END,
                    paper_decision=COALESCE(?,paper_decision),
                    updated_at=? WHERE token_address=?
                """,
                (
                    time.time(), reason, int(bool(market)), evidence,
                    int(update_reference), safe_float((market or {}).get("price_usd"), 0.0) or None,
                    int(update_reference), safe_float((market or {}).get("market_cap_usd"), 0.0) or None,
                    int(update_reference), time.time(), decision, _utc_now(), token.lower(),
                ),
            )

    def select_execution_pool(self, token: str, market: dict) -> None:
        source_version = str(market.get("source_version") or SOURCE_V2)
        pair_address = str(market.get("pair_address") or "").lower()
        pool_id = pair_address if source_version == SOURCE_V4 else None
        hooks_address = ZERO_ADDRESS
        fee_tier = tick_spacing = None
        if pool_id:
            pool = self.v4_pool(pool_id)
            if pool:
                hooks_address = pool.get("hooks_address") or ZERO_ADDRESS
                fee_tier = pool.get("fee_tier")
                tick_spacing = pool.get("tick_spacing")
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET pair_address=?,source_version=?,pool_id=?,
                    hooks_address=?,fee_tier=?,tick_spacing=?,updated_at=?
                WHERE token_address=?
                """,
                (
                    pair_address, source_version, pool_id, hooks_address,
                    fee_tier, tick_spacing, _utc_now(), token.lower(),
                ),
            )

    def record_analysis_failure(self, token: str, reason: str) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET
                    analysis_status=CASE
                        WHEN analysis_attempts+1 >= ? THEN 'failed'
                        ELSE 'retry'
                    END,
                    analysis_attempts=analysis_attempts+1, updated_at=?
                WHERE token_address=?
                """, (MAXIMUM_ANALYSIS_ATTEMPTS, _utc_now(), token.lower())
            )

    def candidate(self, token: str) -> dict | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM candidates WHERE token_address=?", (token.lower(),)
            ).fetchone()
            return dict(row) if row else None

    @staticmethod
    def tolerance(horizon: int) -> float:
        """How late a mark may land and still be recorded rather than expired.

        The old floor was 5 minutes, which for the 15m horizon is a window
        exactly ONE learn cycle wide (measured cadence: median 300s, max
        1,125s). A single slow cycle therefore expired the checkpoint outright,
        and the miss rate tracked the window width exactly: 24.6% at 15m, 12.1%
        at 1h, 0.3% at 6h. RH-REFLECT-MARKET-COVERAGE has reported this as a
        31.7% observation-failure ratio since checkpoint 15.

        The floor is now two nominal cycles, so ordinary jitter no longer
        destroys an observation. Widening a tolerance normally trades accuracy
        for coverage -- a mark arriving late measures a later moment than the
        horizon names -- so it is paired with the learning-eligibility cutoff
        below: late marks are RECORDED but excluded from learning. Coverage and
        measurement integrity are separate concerns and are now separately
        controlled.
        """
        return float(max(CHECKPOINT_TOLERANCE_MINIMUM_SECONDS, horizon // 4))

    @staticmethod
    def learning_eligible(horizon: int, lateness: float) -> int:
        """Whether a recorded mark is timely enough to learn from.

        Previously hardcoded to 1 on every observation, so a mark that landed
        near the edge of its window fed calibration identically to one on time.
        A 15m outcome measured at 27m is not a 15m outcome.
        """
        if horizon <= 0:
            return 1
        return int(
            max(0.0, float(lateness))
            <= horizon * CHECKPOINT_LEARNING_LATENESS_FRACTION
        )

    def expire_missed(self, now: float, limit: int = MISSED_EXPIRATION_LIMIT) -> int:
        """Record irrecoverably late checkpoints without an unbounded write burst."""
        expired = 0
        with self.connection() as connection:
            for label, horizon in HORIZONS:
                remaining = max(0, int(limit) - expired)
                if remaining <= 0:
                    break
                before = connection.total_changes
                cursor = connection.execute(
                    """
                    WITH overdue AS (
                        SELECT token_address FROM candidates
                        WHERE analysis_status='complete'
                          AND COALESCE(outcome_anchor_at,block_timestamp)+?+? < ?
                          AND NOT EXISTS (
                              SELECT 1 FROM checkpoints p
                              WHERE p.token_address=candidates.token_address
                                AND p.horizon_label=?
                          )
                        ORDER BY COALESCE(outcome_anchor_at,block_timestamp),token_address
                        LIMIT ?
                    )
                    INSERT OR IGNORE INTO checkpoints (
                        token_address,horizon_label,horizon_seconds,target_at,
                        anchor_type,anchor_at,observed_at,status,
                        learning_eligible,lateness_seconds
                    )
                    SELECT c.token_address,?,?,
                           COALESCE(c.outcome_anchor_at,c.block_timestamp)+?,
                           CASE WHEN c.outcome_anchor_at IS NOT NULL
                                THEN 'analysis' ELSE 'legacy_launch' END,
                           COALESCE(c.outcome_anchor_at,c.block_timestamp),
                           ?,'missed',0,
                           ?-(COALESCE(c.outcome_anchor_at,c.block_timestamp)+?)
                    FROM candidates c JOIN overdue o USING(token_address)
                    """,
                    (
                        horizon, self.tolerance(horizon), now, label, remaining,
                        label, horizon, horizon, _utc_now(), now, horizon,
                    ),
                )
                del cursor
                expired += connection.total_changes - before
        return expired

    def missed_backlog(self, now: float) -> int:
        with self.connection() as connection:
            return sum(
                connection.execute(
                    """
                    SELECT COUNT(*) FROM candidates c
                    WHERE c.analysis_status='complete'
                      AND COALESCE(c.outcome_anchor_at,c.block_timestamp)+?+? < ?
                      AND NOT EXISTS (
                          SELECT 1 FROM checkpoints p
                          WHERE p.token_address=c.token_address
                            AND p.horizon_label=?
                      )
                    """,
                    (horizon, self.tolerance(horizon), now, label),
                ).fetchone()[0]
                for label, horizon in HORIZONS
            )

    def due_outcomes(
        self, now: float, limit: int,
        recovery_limit: int = DEFAULT_OUTCOME_RECOVERY_LIMIT,
    ) -> list[dict]:
        """Return timely checkpoints first plus a separately bounded late slice.

        A late-but-still-recordable checkpoint is useful for coverage, but it
        is not learning eligible. It must never starve a fresh checkpoint whose
        value still corresponds to the named horizon.
        """
        values = []
        fetch_limit = max(1, (max(0, limit) + max(0, recovery_limit)) * 2)
        with self.connection() as connection:
            for label, horizon in HORIZONS:
                rows = connection.execute(
                    """
                    SELECT c.*, ? horizon_label, ? horizon_seconds,
                           CASE WHEN c.outcome_anchor_at IS NOT NULL
                                THEN 'analysis' ELSE 'legacy_launch' END
                                outcome_anchor_type,
                           COALESCE(c.outcome_anchor_at,c.block_timestamp)
                                outcome_anchor_at_resolved,
                           COALESCE(c.outcome_anchor_at,c.block_timestamp)+?
                                target_at
                    FROM candidates c LEFT JOIN checkpoints p
                      ON p.token_address=c.token_address AND p.horizon_label=?
                    WHERE p.token_address IS NULL
                      AND c.analysis_status='complete'
                      AND COALESCE(c.outcome_anchor_at,c.block_timestamp)+? <= ?
                      AND COALESCE(c.outcome_anchor_at,c.block_timestamp)+?+? >= ?
                      AND (c.last_outcome_attempt_at IS NULL OR c.last_outcome_attempt_at<=?)
                    ORDER BY target_at, c.token_address LIMIT ?
                    """,
                    (
                        label, horizon, horizon, label, horizon, now, horizon,
                        self.tolerance(horizon), now, now-OUTCOME_RETRY_SECONDS,
                        fetch_limit,
                    ),
                )
                for row in rows:
                    value = dict(row)
                    lateness = max(0.0, now - value["target_at"])
                    value["outcome_queue"] = (
                        "fresh" if self.learning_eligible(horizon, lateness)
                        else "recovery"
                    )
                    values.append(value)
        fresh = sorted(
            (row for row in values if row["outcome_queue"] == "fresh"),
            key=lambda row: (row["target_at"], row["token_address"]),
        )
        recovery = sorted(
            (row for row in values if row["outcome_queue"] == "recovery"),
            key=lambda row: (row["target_at"], row["token_address"]),
        )
        selected, seen = [], set()
        for queue, queue_limit in ((fresh, max(0, limit)),
                                   (recovery, max(0, recovery_limit))):
            if queue_limit <= 0:
                continue
            added = 0
            for row in queue:
                if row["token_address"] in seen:
                    continue
                selected.append(row)
                seen.add(row["token_address"])
                added += 1
                if added >= queue_limit:
                    break
        return selected

    def record_outcome(self, due: dict, market: dict, now: float) -> dict:
        token = due["token_address"]
        cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        fdv = safe_float(market.get("fdv_usd"), 0.0) or None
        status = "observed" if cap or fdv else "no_market"
        observed_at = _utc_now()
        lateness = max(0, now-due["target_at"])
        eligible = self.learning_eligible(due["horizon_seconds"], lateness)
        with self.connection() as connection:
            current = connection.execute(
                "SELECT * FROM candidates WHERE token_address=?", (token,)
            ).fetchone()
            first = current["first_market_cap_usd"] or cap
            peak = max(filter(None, [current["peak_market_cap_usd"], cap]), default=None)
            peak_fdv = max(filter(None, [current["peak_fdv_usd"], fdv]), default=None)
            multiple = cap / first if cap and first else None
            mfe = (peak / first - 1) * 100 if peak and first else None
            connection.execute(
                """
                INSERT INTO checkpoints (
                    token_address,horizon_label,horizon_seconds,target_at,
                    anchor_type,anchor_at,
                    observed_at,status,learning_eligible,lateness_seconds,
                    price_usd,liquidity_usd,market_cap_usd,fdv_usd,
                    market_cap_multiple,maximum_favorable_excursion_pct,
                    market_evidence_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    token,due["horizon_label"],due["horizon_seconds"],due["target_at"],
                    due.get("outcome_anchor_type") or "legacy_launch",
                    due.get("outcome_anchor_at_resolved"),
                    observed_at,status,eligible,lateness,
                    market.get("price_usd"),market.get("liquidity_usd"),cap,fdv,
                    multiple,mfe,_canonical(market),
                ),
            )
            connection.execute(
                """
                UPDATE candidates SET first_market_cap_usd=?,peak_market_cap_usd=?,
                    peak_fdv_usd=?,last_observed_at=?,last_outcome_attempt_at=?,updated_at=?
                WHERE token_address=?
                """, (first,peak,peak_fdv,observed_at,now,observed_at,token)
            )
        return {
            "token_address": token,
            "horizon_label": due["horizon_label"],
            "horizon_seconds": due["horizon_seconds"],
            "target_at": due["target_at"],
            "anchor_type": (
                due.get("outcome_anchor_type") or "legacy_launch"
            ),
            "anchor_at": due.get("outcome_anchor_at_resolved"),
            "observed_at": observed_at,
            "status": status,
            "learning_eligible": eligible,
            "lateness_seconds": lateness,
            "price_usd": market.get("price_usd"),
            "liquidity_usd": market.get("liquidity_usd"),
            "market_cap_usd": cap,
            "fdv_usd": fdv,
            "market_cap_multiple": multiple,
            "maximum_favorable_excursion_pct": mfe,
        }

    def record_outcome_producer_reference(
        self, token: str, horizon_label: str, ring: dict, record_hash: str,
    ) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE checkpoints SET producer_outcome_ring_index=?,
                    producer_outcome_ring_hash=?,producer_outcome_record_hash=?
                WHERE token_address=? AND horizon_label=?
                """,
                (
                    ring.get("index"), ring.get("ring_hash"), record_hash,
                    token.lower(), horizon_label,
                ),
            )

    def outcome_failure(self, token: str, now: float) -> None:
        with self.connection() as connection:
            connection.execute(
                "UPDATE candidates SET last_outcome_attempt_at=?,updated_at=? WHERE token_address=?",
                (now,_utc_now(),token.lower()),
            )

    def open_position(self, candidate: dict, market: dict) -> bool:
        if not candidate.get("paper_entry_allowed"):
            return False
        if self.source_entry_risk(
            candidate.get("source_version")
        )["quarantined"]:
            return False
        try:
            safety = json.loads(candidate.get("safety_signals_json") or "{}")
        except (TypeError, ValueError):
            safety = {}
        if (
            safety.get("raw_safety_refusal_enforced") is True
            or (safety.get("entry_deliberation") or {}).get("judgment") == "refuse"
        ):
            return False
        if (
            candidate.get("source_version") == SOURCE_V4
            and not V4_PAPER_ADMISSION_ENABLED
        ):
            return False
        if market.get("current_state_verified") is False:
            return False
        price = safe_float(market.get("price_usd"), 0.0)
        liquidity = safe_float(market.get("liquidity_usd"), 0.0)
        market_cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        reserve_fraction = market.get("pool_reserve_fraction")
        if (
            price <= 0 or liquidity < MINIMUM_ENTRY_LIQUIDITY_USD
            or market_cap is None or market_cap > MAXIMUM_ENTRY_MARKET_CAP_USD
        ):
            return False
        with self.connection() as connection:
            if connection.execute("SELECT 1 FROM positions WHERE token_address=?", (candidate["token_address"],)).fetchone():
                return False
            open_count = connection.execute("SELECT COUNT(*) FROM positions WHERE status='open'").fetchone()[0]
            if open_count >= MAXIMUM_POSITIONS:
                return False
            friction_bps = min(5_000.0, 100.0 + PAPER_COST_USD / max(1.0, 2*liquidity) * 10_000)
            retained = 1-friction_bps/10_000
            quantity = PAPER_COST_USD*retained/price
            connection.execute(
                """
                INSERT INTO positions (
                    token_address,symbol,status,opened_at,entry_price_usd,
                    entry_liquidity_usd,cost_usd,quantity,entry_friction_bps,
                    entry_market_cap_usd,high_multiple,last_price_usd,
                    last_liquidity_usd,last_market_cap_usd,last_market_observed_at,
                    last_mark_at,original_quantity,realized_value_usd,
                    runner_high_multiple,entry_policy_version,grandfathered_above_cap,
                    entry_pool_reserve_fraction,last_pool_reserve_fraction,
                    minimum_pool_reserve_fraction
                ) VALUES (?,?,'open',?,?,?,?,?,?,?,1,?,?,?,?,?,?,0,1,?,0,?,?,?)
                """,
                (
                    candidate["token_address"],candidate.get("symbol") or "",time.time(),
                    price,liquidity,PAPER_COST_USD,quantity,friction_bps,
                    market_cap,price,liquidity,market_cap,_utc_now(),time.time(),
                    quantity,"staged_v1",
                    reserve_fraction, reserve_fraction, reserve_fraction,
                ),
            )
            connection.executemany(
                """
                INSERT INTO position_policy_states (
                    token_address,policy,status,high_multiple,last_mark_at
                ) VALUES (?,?,'open',1,?)
                """,
                [
                    (candidate["token_address"], SHADOW_POLICY_FIXED_3X, time.time()),
                    (candidate["token_address"], SHADOW_POLICY_PURE_TRAILING, time.time()),
                ],
            )
            return True

    def _effective_minimum_entry_score(self) -> float:
        """Module constant, or a higher floor set by the closed-position audit.

        Clamped with max() so this can only tighten. A missing, unreadable or
        malformed file falls back to the constant rather than failing open.
        """
        try:
            # self.path is the sqlite file; the policy sits beside it in
            # the learning root.
            state = read_json(
                self.path.parent / "adaptive_policy.json", {}
            ) or {}
            adaptive = safe_float(state.get("minimum_entry_score"), 0.0)
        except Exception:
            return MINIMUM_ENTRY_SCORE
        return max(MINIMUM_ENTRY_SCORE, adaptive)

    @staticmethod
    def _liquidation_value(quantity: float, price: float, liquidity: float) -> float:
        gross = max(0.0, quantity) * max(0.0, price)
        impact_bps = min(
            5_000.0,
            100.0 + gross / max(1.0, 2 * liquidity) * 10_000,
        )
        return gross * (1 - impact_bps / 10_000)

    def _mark_shadow_policies(
        self, connection, position, price: float, liquidity: float, now: float,
    ) -> None:
        full_value = self._liquidation_value(
            safe_float(position["original_quantity"], position["quantity"]),
            price, liquidity,
        )
        multiple = full_value / max(0.01, position["cost_usd"])
        age = now - position["opened_at"]
        for state in connection.execute(
            "SELECT * FROM position_policy_states WHERE token_address=? AND status='open'",
            (position["token_address"],),
        ).fetchall():
            high = max(safe_float(state["high_multiple"], 1.0), multiple)
            reason = None
            # Same cause-before-ordering classification as the live path; the
            # shadow policies exist to be compared against it, so they have to
            # label an exit the same way or the comparison measures taxonomy
            # rather than policy.
            price_multiple = price / max(1e-30, position["entry_price_usd"])
            if price_multiple <= PRICE_COLLAPSE_MULTIPLE:
                reason = "price_collapse"
            elif multiple <= STOP_LOSS_MULTIPLE:
                reason = "stop_loss"
            elif liquidity < MINIMUM_ENTRY_LIQUIDITY_USD:
                reason = "liquidity_below_minimum"
            elif age >= MAXIMUM_HOLD_SECONDS:
                reason = "maximum_hold"
            elif state["policy"] == SHADOW_POLICY_FIXED_3X and multiple >= 3.0:
                reason = "fixed_take_profit_3x"
            elif (
                state["policy"] == SHADOW_POLICY_PURE_TRAILING
                and high >= PURE_TRAILING_ACTIVATION_MULTIPLE
                and multiple <= high * (1 - RUNNER_TRAILING_DRAWDOWN)
            ):
                reason = "pure_trailing_stop"
            if reason:
                connection.execute(
                    """
                    UPDATE position_policy_states SET status='closed',high_multiple=?,
                        exit_price_usd=?,exit_value_usd=?,exit_reason=?,closed_at=?,
                        net_multiple=?,last_mark_at=? WHERE token_address=? AND policy=?
                    """,
                    (
                        high, price, full_value, reason, now, multiple, now,
                        position["token_address"], state["policy"],
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE position_policy_states SET high_multiple=?,last_mark_at=?
                    WHERE token_address=? AND policy=?
                    """,
                    (high, now, position["token_address"], state["policy"]),
                )

    def mark_position(self, token: str, market: dict, now: float) -> dict | None:
        price = safe_float(market.get("price_usd"), 0.0)
        liquidity = safe_float(market.get("liquidity_usd"), 0.0)
        market_cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        reserve_fraction = market.get("pool_reserve_fraction")
        with self.connection() as connection:
            position = connection.execute(
                "SELECT * FROM positions WHERE token_address=? AND status='open'", (token.lower(),)
            ).fetchone()
            if not position:
                return None
            verified = (
                market.get("current_state_verified") is not False
                and price > 0
                and (
                    not market.get("paper_exit_quote_required")
                    or market.get("paper_exit_quote_verified") is True
                )
            )
            if not verified:
                connection.execute(
                    """
                    UPDATE positions SET unverified_marks=unverified_marks+1,
                        consecutive_unverified_marks=consecutive_unverified_marks+1,
                        last_unverified_mark_at=?
                    WHERE token_address=?
                    """,
                    (now, token.lower()),
                )
                return {
                    "token_address": token.lower(),
                    "verified": False,
                    "observation_status": "unverified",
                    "reason": None,
                    "market_reason": market.get("reason") or (
                        "paper_exit_quote_unverified"
                        if market.get("paper_exit_quote_required")
                        and market.get("paper_exit_quote_verified") is not True
                        else
                        "non_positive_price" if price <= 0 else "state_unverified"
                    ),
                    "partial_exits": [],
                }
            self._mark_shadow_policies(connection, position, price, liquidity, now)
            original = safe_float(position["original_quantity"], position["quantity"])
            remaining = safe_float(position["quantity"], 0.0)
            realized = safe_float(position["realized_value_usd"], 0.0)
            quoted_quantity = safe_float(
                market.get("paper_exit_quantity"), 0.0
            )
            quoted_exit_value = safe_float(
                market.get("paper_exit_value_usd"), 0.0
            )

            def liquidation_value(quantity: float) -> float:
                if (
                    quoted_quantity > 0 and quoted_exit_value >= 0
                    and abs(quantity - quoted_quantity)
                        <= max(1e-12, quoted_quantity * 1e-9)
                ):
                    return quoted_exit_value
                return self._liquidation_value(quantity, price, liquidity)

            marked_remaining = liquidation_value(remaining)
            value = realized + marked_remaining
            multiple = value / max(0.01, position["cost_usd"])
            high = max(safe_float(position["high_multiple"], 1.0), multiple)
            age = now-position["opened_at"]
            price_multiple = price / max(1e-30, position["entry_price_usd"])
            reason = None
            partial_exits = []
            # Classify by CAUSE, not by which branch happens to be first.
            #
            # A rug satisfies both the liquidity and the stop-loss condition at
            # once, so ordering alone decided the label: liquidity_below_minimum
            # was checked first and absorbed every one. Measured on the first 8
            # closes, 5 exited with that label at exactly 0.0x and
            # high_multiple 1.0 -- entry liquidity $51k-$155k, price straight to
            # zero, never a cent above entry. Those are rugs, not thin markets,
            # and stop_loss had never once fired, so the stop was untested
            # rather than working.
            #
            # Confirmed on-chain: for every one of those tokens the V4
            # PoolManager holds 0.00-0.20% of supply, against 2.2-13.0% for the
            # positions still open. The pools really were drained.
            #
            # price_multiple is the discriminator: it is the token's own price
            # against entry, independent of pool depth and of the size-based
            # slippage in _liquidation_value (capped at 50%, so slippage alone
            # can never reach zero). A collapsed price with drained liquidity is
            # a rug; healthy price with thin liquidity is an exit-liquidity
            # problem; a real decline is a stop loss.
            if price_multiple <= PRICE_COLLAPSE_MULTIPLE:
                reason = "price_collapse"
            elif multiple <= STOP_LOSS_MULTIPLE:
                reason = "stop_loss"
            elif liquidity < MINIMUM_ENTRY_LIQUIDITY_USD:
                reason = "liquidity_below_minimum"
            elif age >= STAGNATION_EXIT_SECONDS and high < STAGNATION_MINIMUM_MULTIPLE:
                reason = "stagnation_24h"
            elif age >= MAXIMUM_HOLD_SECONDS:
                reason = "maximum_hold"
            if not reason and position["stage_one_sold_at"] is None and price_multiple >= STAGE_ONE_MULTIPLE:
                sold = min(remaining, original * STAGE_ONE_ORIGINAL_FRACTION)
                proceeds = self._liquidation_value(sold, price, liquidity)
                remaining -= sold; realized += proceeds
                partial_exits.append({"stage":"recover_principal_2x","quantity":sold,"value_usd":proceeds})
            if not reason and position["stage_two_sold_at"] is None and price_multiple >= STAGE_TWO_MULTIPLE:
                sold = min(remaining, original * STAGE_TWO_ORIGINAL_FRACTION)
                proceeds = self._liquidation_value(sold, price, liquidity)
                remaining -= sold; realized += proceeds
                partial_exits.append({"stage":"take_profit_3x","quantity":sold,"value_usd":proceeds})
            runner_high = max(
                safe_float(position["runner_high_multiple"], 1.0), price_multiple
            )
            stage_two_at = position["stage_two_sold_at"] or (
                now if any(item["stage"] == "take_profit_3x" for item in partial_exits) else None
            )
            if (
                not reason and stage_two_at is not None
                and price_multiple <= runner_high * (1 - RUNNER_TRAILING_DRAWDOWN)
            ):
                reason = "runner_trailing_stop"
            if reason:
                realized += liquidation_value(remaining)
                remaining = 0.0
                value = realized
                multiple = value / max(0.01, position["cost_usd"])
                connection.execute(
                    """
                    UPDATE positions SET status='closed',high_multiple=?,last_price_usd=?,
                        last_liquidity_usd=?,last_market_cap_usd=?,
                        last_market_observed_at=?,last_mark_at=?,exit_price_usd=?,exit_value_usd=?,
                        exit_reason=?,closed_at=?,net_multiple=?,quantity=?,realized_value_usd=?,
                        last_pool_reserve_fraction=COALESCE(?,last_pool_reserve_fraction),
                        minimum_pool_reserve_fraction=MIN(
                            COALESCE(?,minimum_pool_reserve_fraction,1.0),
                            COALESCE(minimum_pool_reserve_fraction,1.0)
                        ),
                        stage_one_sold_at=COALESCE(stage_one_sold_at,?),
                        stage_two_sold_at=COALESCE(stage_two_sold_at,?),runner_high_multiple=?,
                        verified_mark_count=verified_mark_count+1,
                        consecutive_unverified_marks=0,last_verified_mark_at=?
                    WHERE token_address=?
                    """, (high,price,liquidity,market_cap,_utc_now(),now,price,value,
                            reason,now,multiple,remaining,realized,
                            reserve_fraction, reserve_fraction,
                            now if any(item["stage"] == "recover_principal_2x" for item in partial_exits) else None,
                            now if any(item["stage"] == "take_profit_3x" for item in partial_exits) else None,
                            runner_high,now,token.lower())
                )
            else:
                value = realized + self._liquidation_value(remaining, price, liquidity)
                multiple = value / max(0.01, position["cost_usd"])
                connection.execute(
                    """
                    UPDATE positions SET high_multiple=?,last_price_usd=?,
                        last_liquidity_usd=?,last_market_cap_usd=?,
                        last_market_observed_at=?,last_mark_at=?,quantity=?,realized_value_usd=?,
                        last_pool_reserve_fraction=COALESCE(?,last_pool_reserve_fraction),
                        minimum_pool_reserve_fraction=MIN(
                            COALESCE(?,minimum_pool_reserve_fraction,1.0),
                            COALESCE(minimum_pool_reserve_fraction,1.0)
                        ),
                        stage_one_sold_at=COALESCE(stage_one_sold_at,?),
                        stage_two_sold_at=COALESCE(stage_two_sold_at,?),runner_high_multiple=?,
                        verified_mark_count=verified_mark_count+1,
                        consecutive_unverified_marks=0,last_verified_mark_at=?
                    WHERE token_address=?
                    """, (
                        high,price,liquidity,market_cap,_utc_now(),now,remaining,realized,
                        reserve_fraction, reserve_fraction,
                        now if any(item["stage"] == "recover_principal_2x" for item in partial_exits) else None,
                        now if any(item["stage"] == "take_profit_3x" for item in partial_exits) else None,
                        runner_high,now,token.lower(),
                    ))
            return {
                "token_address":token.lower(),"multiple":multiple,"reason":reason,
                "value_usd":value,"partial_exits":partial_exits,
                "remaining_fraction":remaining/max(original,1e-30),
                "verified": True,"observation_status":"verified",
            }

    def summary(self) -> dict:
        with self.connection() as connection:
            candidate = connection.execute(
                """
                SELECT COUNT(*) total,
                  SUM(analysis_status='complete') analyzed,
                  SUM(analysis_status IN ('pending','retry')) pending,
                  SUM(analysis_status='failed') failed,
                  SUM(paper_entry_allowed=1) admitted,
                  SUM(paper_decision='rejected') rejected,
                  SUM(paper_decision='watching_for_executable_market') watching,
                  SUM(paper_decision='observing_above_entry_ceiling') above_entry_ceiling,
                  SUM(paper_decision='waiting_for_reentry_momentum') waiting_reentry_momentum,
                  SUM(paper_decision='v4_shadow_only') v4_shadow_only,
                  SUM(paper_decision='source_risk_quarantine') source_risk_quarantine,
                  SUM(paper_decision='expired_no_executable_market') watch_expired,
                  SUM(peak_market_cap_usd>=1000000) million_peak
                FROM candidates
                """
            ).fetchone()
            statuses = {row[0]:row[1] for row in connection.execute(
                "SELECT status,COUNT(*) FROM checkpoints GROUP BY status"
            )}
            positions = connection.execute(
                """
                SELECT COUNT(*) opened,SUM(status='open') open,SUM(status='closed') closed,
                  SUM(CASE WHEN status='closed' AND net_multiple>1 THEN 1 ELSE 0 END) winners,
                  AVG(CASE WHEN status='closed' THEN net_multiple END) average_multiple,
                  SUM(stage_one_sold_at IS NOT NULL) stage_one_exits,
                  SUM(stage_two_sold_at IS NOT NULL) stage_two_exits,
                  SUM(grandfathered_above_cap=1) grandfathered_above_cap
                FROM positions
                """
            ).fetchone()
            sources = {row[0]: row[1] for row in connection.execute(
                "SELECT source_version,COUNT(*) FROM candidates GROUP BY source_version"
            )}
        source_entry_risk = {
            source: self.source_entry_risk(source)
            for source in (SOURCE_V2, SOURCE_V3, SOURCE_V4)
        }
        return {
            "candidates": {key:(candidate[key] or 0) for key in candidate.keys()},
            "checkpoints": statuses,
            "positions": {key:(positions[key] or 0) for key in positions.keys()},
            "sources": sources,
            "source_entry_risk": source_entry_risk,
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def backfill_shadow_admission(self) -> int:
        """Compute shadow verdicts for candidates analysed before the rules existed.

        Reconstructed from what was stored at analysis time, so the verdicts
        are faithful to that moment. They are still marked backfilled, because
        the threshold was picked after seeing these outcomes and a rule cannot
        be tested on its own training set.
        """
        updated = 0
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT token_address, score, risk_level, hard_stops_json,
                       entry_liquidity_usd, first_market_cap_usd, hooks_address,
                       source_version, paper_entry_allowed
                FROM candidates
                WHERE analysis_status='complete' AND shadow_admission_json IS NULL
                """
            ).fetchall()
            floor = self._effective_minimum_entry_score()
            for row in rows:
                try:
                    stops = json.loads(row["hard_stops_json"] or "[]")
                except (TypeError, ValueError):
                    stops = []
                hooks = str(row["hooks_address"] or "").lower()
                shadow = shadow_admission(
                    market_cap=safe_float(row["first_market_cap_usd"], 0.0) or None,
                    liquidity=safe_float(row["entry_liquidity_usd"], 0.0),
                    hooks_present=(
                        bool(hooks and hooks != ZERO_ADDRESS)
                        if row["source_version"] == SOURCE_V4 else None
                    ),
                    hard_stops=stops,
                    token_quality_passes=bool(
                        safe_float(row["score"], 0.0) >= floor
                        and str(row["risk_level"]) in {"Low", "Medium"}
                    ),
                    allowed=bool(row["paper_entry_allowed"]),
                    backfilled=True,
                )
                connection.execute(
                    """
                    UPDATE candidates SET mcap_liquidity_ratio=?, hooks_present=?,
                        shadow_admission_json=? WHERE token_address=?
                    """,
                    (
                        shadow["mcap_liquidity_ratio"],
                        None if shadow["hooks_present"] is None
                        else int(shadow["hooks_present"]),
                        _canonical(shadow), row["token_address"],
                    ),
                )
                updated += 1
        return updated

    def flow_gate_telemetry(
        self, since_seconds: float = 3600.0, *, head_block: int | None = None,
    ) -> dict:
        """Per-gate failure counts for recent flow windows, plus the two
        blockers that no gate tally can show.

        Flow Signal v1 has produced 0 qualified events from 652 windows. The
        per-gate counts alone are misleading, because two conditions sit
        OUTSIDE the gate list and each is independently sufficient to make a
        prospective event impossible:

          head lag -- a prospective event requires the window to end within
          FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS (120) of the chain head.
          Robinhood produces roughly 3,000 blocks per five-minute cycle, so
          120 blocks is about twelve seconds. A learner batching every five
          minutes is structurally ~2,500 blocks behind when it computes, and
          can never satisfy that bound regardless of how good the flow is.

          score ceiling -- FLOW_SHADOW_SCORE_THRESHOLD is 70.0 and the best
          score ever recorded across 652 windows is 69.0.

        Reports both alongside the gates so the next reader sees the real
        constraint instead of concluding the thresholds are merely strict.
        """
        cutoff = _utc_now_minus(since_seconds)
        gates: dict[str, int] = {}
        windows = qualified = 0
        best_score = 0.0
        gapless_unscored = 0
        with self.connection() as connection:
            # Read the append-only series, not flow_signals: the latest-state
            # table holds each pool at its terminal window, so gate counts
            # taken from it describe dead pools rather than recent activity.
            rows = connection.execute(
                """
                SELECT shadow_qualified, shadow_score, features_json,
                       window_end_block
                FROM flow_signal_windows
                WHERE policy_version=? AND computed_at >= ?
                """,
                (FLOW_EVIDENCE_POLICY_VERSION, cutoff),
            ).fetchall()
            ingested = connection.execute(
                "SELECT MAX(block_number) FROM swap_observations"
            ).fetchone()[0] or 0
        # Lag must be measured against the CHAIN head. Measuring against our
        # own ingestion high-water mark compares the reader with itself and
        # reports 0 no matter how far behind the chain it actually is.
        head = int(head_block) if head_block else ingested
        head_source = "chain_head" if head_block else "ingestion_high_water_mark"
        for row in rows:
            windows += 1
            qualified += bool(row["shadow_qualified"])
            best_score = max(best_score, safe_float(row["shadow_score"], 0.0))
            try:
                features = json.loads(row["features_json"] or "{}")
            except (TypeError, ValueError):
                features = {}
            reported = features.get("qualification_gaps")
            if reported is None:
                # No gap list was computed for this window. That is NOT a pass;
                # 96 windows carried an empty gaps column for exactly this
                # reason and read as though they had cleared every gate.
                gapless_unscored += 1
                continue
            for gate in reported:
                gates[str(gate)] = gates.get(str(gate), 0) + 1
        newest = max((row["window_end_block"] or 0) for row in rows) if rows else 0
        return {
            "windows": windows,
            "qualified": qualified,
            "gates_failed": dict(sorted(gates.items(), key=lambda kv: -kv[1])),
            "windows_without_gate_evaluation": gapless_unscored,
            "best_shadow_score": best_score,
            "score_threshold": FLOW_SHADOW_SCORE_THRESHOLD,
            "score_threshold_reached": best_score >= FLOW_SHADOW_SCORE_THRESHOLD,
            "newest_window_end_block": newest,
            "observed_head_block": head,
            "head_block_source": head_source,
            "ingestion_high_water_mark": ingested,
            "head_lag_blocks": max(0, head - newest),
            "maximum_prospective_head_lag_blocks":
                FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
            "head_lag_within_prospective_bound": bool(
                head_block and newest
                and head - newest <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            ),
            "window_seconds": since_seconds,
        }

    def portfolio_metrics(self) -> dict:
        """Win rate, average result, and win rate BY SCORE BUCKET.

        The headline numbers existed only as ad-hoc SQL, so nothing reported
        that 3 of 42 closes were winners at an average of 0.45x while an
        autonomous policy raised the entry floor eight times. Bucketing by
        score is the part that matters: the floor is only defensible if a
        higher score buys a better outcome, and the correlation reported here
        is the direct test of that.

        Observation-indeterminate positions -- closed without a single
        observed mark -- are held out, because a position nobody watched
        cannot testify about the score that admitted it.
        """
        empty = {
            "closed": 0, "winners": 0, "win_rate": 0.0, "average_multiple": None,
            "median_multiple": None, "total_loss": 0, "total_loss_rate": 0.0,
            "observation_indeterminate": 0, "by_score_bucket": {},
            "score_outcome_correlation": None, "open": 0, "open_median_mark": None,
        }
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                """
                SELECT p.status, p.net_multiple, p.cost_usd, p.realized_value_usd,
                       p.entry_price_usd, p.last_price_usd, c.score,
                       -- Only a verified post-entry mark can label an outcome.
                       -- Entry timestamps and failed V4 reads are observations
                       -- of the watcher, not observations of the market.
                       (p.verified_mark_count>0) observed_marks
                FROM positions p LEFT JOIN candidates c USING(token_address)
                """
            )]
        if not rows:
            return empty
        closed, scores, multiples = [], [], []
        indeterminate = 0
        for row in rows:
            if row["status"] == "open":
                continue
            if not int(row["observed_marks"] or 0):
                indeterminate += 1
                continue
            multiple = safe_float(row["net_multiple"], None)
            if multiple is None:
                cost = safe_float(row["cost_usd"], 0.0) or 1.0
                multiple = safe_float(row["realized_value_usd"], 0.0) / cost
            closed.append(multiple)
            if row["score"] is not None:
                scores.append(safe_float(row["score"], 0.0))
                multiples.append(multiple)
        if not closed:
            empty["observation_indeterminate"] = indeterminate
            return empty

        buckets: dict[str, dict] = {}
        for score, multiple in zip(scores, multiples):
            low = int(score // 5 * 5)
            key = f"{low}-{low + 5}"
            bucket = buckets.setdefault(
                key, {"n": 0, "winners": 0, "total_loss": 0, "sum": 0.0}
            )
            bucket["n"] += 1
            bucket["sum"] += multiple
            bucket["winners"] += multiple > 1.0
            bucket["total_loss"] += multiple <= 0.01
        for bucket in buckets.values():
            bucket["win_rate"] = round(bucket["winners"] / bucket["n"], 4)
            bucket["average_multiple"] = round(bucket["sum"] / bucket["n"], 4)
            bucket.pop("sum")

        correlation = None
        if len(scores) >= 3:
            mean_s = sum(scores) / len(scores)
            mean_m = sum(multiples) / len(multiples)
            cov = sum(
                (s - mean_s) * (m - mean_m) for s, m in zip(scores, multiples)
            )
            var_s = sum((s - mean_s) ** 2 for s in scores) ** 0.5
            var_m = sum((m - mean_m) ** 2 for m in multiples) ** 0.5
            if var_s and var_m:
                correlation = round(cov / (var_s * var_m), 4)

        open_marks = [
            safe_float(row["last_price_usd"], 0.0) / safe_float(row["entry_price_usd"], 1.0)
            for row in rows
            if row["status"] == "open" and row["last_price_usd"]
            and safe_float(row["entry_price_usd"], 0.0) > 0
        ]
        ordered = sorted(closed)
        winners = sum(1 for value in closed if value > 1.0)
        losses = sum(1 for value in closed if value <= 0.01)
        return {
            "closed": len(closed),
            "winners": winners,
            "win_rate": round(winners / len(closed), 4),
            "average_multiple": round(sum(closed) / len(closed), 4),
            "median_multiple": round(ordered[len(ordered) // 2], 4),
            "total_loss": losses,
            "total_loss_rate": round(losses / len(closed), 4),
            "observation_indeterminate": indeterminate,
            "by_score_bucket": dict(sorted(buckets.items())),
            # Negative means a higher score bought a WORSE outcome, which
            # would make every tighten so far actively counterproductive.
            "score_outcome_correlation": correlation,
            "open": sum(1 for row in rows if row["status"] == "open"),
            "open_median_mark": (
                round(sorted(open_marks)[len(open_marks) // 2], 4)
                if open_marks else None
            ),
        }

    def closed_performance(self) -> dict:
        """Return all-time closed-book accounting for the dashboard.

        The recent-positions endpoint is intentionally capped for display, so
        aggregate P&L must be computed independently over the complete closed
        book. ``net_multiple`` is the canonical friction-adjusted result used
        by the per-token gain display.
        """
        with self.connection() as connection:
            row = connection.execute(
                """
                SELECT COUNT(*) closed,
                  SUM(net_multiple IS NOT NULL) valued_closed,
                  SUM(CASE WHEN net_multiple>1 THEN 1 ELSE 0 END) winners,
                  SUM(CASE WHEN net_multiple IS NOT NULL THEN cost_usd ELSE 0 END)
                    invested_usd,
                  SUM(CASE WHEN net_multiple IS NOT NULL
                    THEN cost_usd*net_multiple ELSE 0 END) returned_usd
                FROM positions WHERE status='closed'
                """
            ).fetchone()
        closed = int(row["closed"] or 0)
        valued = int(row["valued_closed"] or 0)
        winners = int(row["winners"] or 0)
        invested = safe_float(row["invested_usd"], 0.0)
        returned = safe_float(row["returned_usd"], 0.0)
        return {
            "closed": closed,
            "valued_closed": valued,
            "unvalued_closed": closed - valued,
            "winners": winners,
            "win_rate": winners / valued if valued else 0.0,
            "invested_usd": invested,
            "returned_usd": returned,
            "net_pnl_usd": returned - invested,
            "portfolio_multiple": returned / invested if invested else None,
        }

    def shadow_admission_report(self) -> dict:
        """Score each shadow rule against outcomes it did not influence.

        Two outcome sources, kept separate because they are not equally good
        evidence. A HELD outcome is a real exit we paid for. A PEAK outcome is
        market cap reaching 2x on a token we never bought, which proves the
        move existed but not that it was capturable. Rules are judged on both
        and the counts are reported side by side rather than blended.
        """
        rules = (
            "actual_admitted", "would_admit_legacy_gate",
            "would_admit_ratio_rule",
            "would_admit_hook_relaxed", "would_admit_combined",
            "would_admit_concentration_rule", "would_admit_liquidity_lock_rule",
            "would_admit_safety_combined",
        )
        def _empty():
            return {
                rule: {"admitted": 0, "held_rugs": 0, "held_winners": 0,
                       "held_other": 0, "unheld_peaked_2x": 0,
                       "observation_indeterminate": 0}
                for rule in rules
            }
        in_sample, out_of_sample = _empty(), _empty()
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT c.shadow_admission_json, c.first_market_cap_usd,
                       c.peak_market_cap_usd, p.status, p.net_multiple,
                       (p.last_mark_at IS NOT NULL) observed_marks
                FROM candidates c LEFT JOIN positions p USING(token_address)
                WHERE c.shadow_admission_json IS NOT NULL
                """
            ).fetchall()
        evaluated = 0
        for row in rows:
            try:
                shadow = json.loads(row["shadow_admission_json"] or "{}")
            except (TypeError, ValueError):
                continue
            evaluated += 1
            # Same holdout as portfolio_metrics: a close with no observed mark
            # cannot say whether the token rugged or was merely never watched,
            # so it must not be scored for or against any rule. Without this
            # the two ledgers disagree -- 2 unobserved zeros were counted as
            # rugs here while the portfolio held them out.
            observed = int(row["observed_marks"] or 0)
            held = (
                row["status"] == "closed"
                and row["net_multiple"] is not None
                and observed > 0
            )
            indeterminate = (
                row["status"] == "closed"
                and row["net_multiple"] is not None
                and not observed
            )
            multiple = safe_float(row["net_multiple"], 0.0)
            first = safe_float(row["first_market_cap_usd"], 0.0)
            peak = safe_float(row["peak_market_cap_usd"], 0.0)
            # A $2 first-market-cap denominator produces a millionfold "gain";
            # require a real starting valuation before believing the ratio.
            peaked = bool(first > 100 and peak / first >= 2.0)
            target = in_sample if shadow.get("backfilled") else out_of_sample
            for rule in rules:
                if not shadow.get(rule):
                    continue
                bucket = target[rule]
                bucket["admitted"] += 1
                if indeterminate:
                    bucket["observation_indeterminate"] += 1
                elif held:
                    if multiple <= 0.01:
                        bucket["held_rugs"] += 1
                    elif multiple > 1:
                        bucket["held_winners"] += 1
                    else:
                        bucket["held_other"] += 1
                elif peaked:
                    bucket["unheld_peaked_2x"] += 1
        return {
            "candidates_evaluated": evaluated,
            "threshold": SHADOW_MAXIMUM_MCAP_LIQUIDITY_RATIO,
            "enforced": False,
            "in_sample_fitted": in_sample,
            "out_of_sample": out_of_sample,
            "note": (
                "shadow only -- no rule here gates any entry. in_sample_fitted "
                "is the population the threshold was chosen on and proves "
                "nothing; only out_of_sample counts as evidence. held outcomes "
                "are paid exits, unheld_peaked_2x is a move we did not buy"
            ),
        }

    def recent_positions(
        self,
        # Split open from closed downstream, so one shared cap silently
        # truncated the closed table once the book passed 50 positions.
        limit: int = 250,
        live_markets: dict[str, dict] | None = None,
    ) -> list[dict]:
        """Return browser-safe paper marks, newest first.

        Open gains use the same estimated exit friction as mark_position, so the
        dashboard cannot display an optimistic friction-free percentage.
        """
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT p.*, c.name, c.pair_address, c.source_version,
                       c.first_market_cap_usd launch_market_cap_usd
                FROM positions p
                JOIN candidates c ON c.token_address=p.token_address
                ORDER BY p.opened_at DESC LIMIT ?
                """,
                (max(0, limit),),
            ).fetchall()
            policy_rows = connection.execute(
                "SELECT * FROM position_policy_states"
            ).fetchall()
        policies_by_token: dict[str, list[dict]] = {}
        for policy in policy_rows:
            policies_by_token.setdefault(policy["token_address"], []).append(dict(policy))
        positions = []
        live_markets = live_markets or {}
        for raw in rows:
            row = dict(raw)
            live = (
                live_markets.get(row["token_address"], {})
                if row.get("status") == "open"
                else {}
            )
            current_price = safe_float(
                live.get("price_usd") or row.get("last_price_usd"), 0.0
            )
            current_liquidity = safe_float(
                live.get("liquidity_usd") or row.get("last_liquidity_usd"), 0.0
            )
            current_market_cap = (
                safe_float(live.get("market_cap_usd"), 0.0)
                or row.get("last_market_cap_usd")
            )
            if row.get("status") == "closed" and row.get("net_multiple") is not None:
                multiple = safe_float(row.get("net_multiple"), 0.0)
            elif (
                live.get("paper_exit_quote_verified") is True
                and live.get("paper_exit_value_usd") is not None
            ):
                marked_value = safe_float(
                    row.get("realized_value_usd"), 0.0
                ) + safe_float(live.get("paper_exit_value_usd"), 0.0)
                multiple = marked_value / max(
                    0.01, safe_float(row.get("cost_usd"), PAPER_COST_USD)
                )
            elif current_price > 0:
                marked_value = safe_float(row.get("realized_value_usd"), 0.0) + self._liquidation_value(
                    safe_float(row.get("quantity"), 0.0), current_price,
                    current_liquidity,
                )
                multiple = marked_value / max(0.01, safe_float(row.get("cost_usd"), PAPER_COST_USD))
            else:
                multiple = None
            policy_results = []
            for policy in policies_by_token.get(row["token_address"], []):
                policy_multiple = policy.get("net_multiple")
                if policy_multiple is None and current_price > 0:
                    policy_multiple = self._liquidation_value(
                        safe_float(row.get("original_quantity"), row.get("quantity")),
                        current_price, current_liquidity,
                    ) / max(0.01, safe_float(row.get("cost_usd"), PAPER_COST_USD))
                policy_results.append({
                    "policy": policy["policy"], "status": policy["status"],
                    "multiple": policy_multiple,
                    "gain_pct": (policy_multiple - 1) * 100 if policy_multiple is not None else None,
                    "high_multiple": policy.get("high_multiple"),
                    "exit_reason": policy.get("exit_reason"),
                })
            original_quantity = safe_float(
                row.get("original_quantity"), row.get("quantity")
            )
            positions.append({
                "token_address": row["token_address"],
                "pair_address": row.get("pair_address"),
                "source_version": row.get("source_version"),
                "name": row.get("name") or "",
                "symbol": row.get("symbol") or "",
                "status": row.get("status"),
                "opened_at": row.get("opened_at"),
                "entry_price_usd": row.get("entry_price_usd"),
                "exit_price_usd": row.get("exit_price_usd"),
                "exit_value_usd": row.get("exit_value_usd"),
                "closed_at": row.get("closed_at"),
                "current_price_usd": current_price or None,
                "launch_market_cap_usd": row.get("launch_market_cap_usd"),
                "entry_market_cap_usd": row.get("entry_market_cap_usd"),
                "current_market_cap_usd": current_market_cap,
                "gain_pct": (multiple - 1) * 100 if multiple is not None else None,
                "multiple": multiple,
                "high_multiple": row.get("high_multiple"),
                "remaining_fraction": safe_float(row.get("quantity"), 0.0) / max(original_quantity, 1e-30),
                "realized_value_usd": row.get("realized_value_usd"),
                "stage_one_sold": row.get("stage_one_sold_at") is not None,
                "stage_two_sold": row.get("stage_two_sold_at") is not None,
                "entry_policy_version": row.get("entry_policy_version"),
                "grandfathered_above_cap": bool(row.get("grandfathered_above_cap")),
                "counterfactual_policies": policy_results,
                "last_mark_at": row.get("last_mark_at"),
                "verified_mark_count": row.get("verified_mark_count") or 0,
                "unverified_marks": row.get("unverified_marks") or 0,
                "consecutive_unverified_marks": (
                    row.get("consecutive_unverified_marks") or 0
                ),
                "market_observation_verified": (
                    live.get("current_state_verified") is not False
                    if live else bool(row.get("verified_mark_count"))
                ),
                "market_observed_at": (
                    live.get("observed_at") or row.get("last_market_observed_at")
                ),
                "market_source": live.get("source") or "stored_paper_mark",
                "exit_reason": row.get("exit_reason"),
                "paper_only": True,
            })
        return positions

    #: Decisions that mean a token is still waiting for entry. Admitted
    #: candidates are deliberately absent -- they already hold a position and
    #: are listed there, so repeating them makes the pipeline look busier than
    #: it is.
    PENDING_ENTRY_DECISIONS = (
        PAPER_DECISION_WATCHING,
        PAPER_DECISION_ABOVE_CAP,
        PAPER_DECISION_REENTRY,
    )

    def recent_analyzed_tokens(
        self, limit: int = 100, *, include_rejected: bool = False,
    ) -> list[dict]:
        """Tokens still waiting for an entry decision to become actionable.

        Rejections are the overwhelming majority -- 546 of 639 analysed -- and
        listing them buries the handful of tokens the system is actually
        deciding about. They stay queryable in the store for audits; they are
        simply not what a status view is for.
        """
        placeholders = ",".join("?" * len(self.PENDING_ENTRY_DECISIONS))
        clause = "" if include_rejected else (
            f" AND paper_decision IN ({placeholders})"
        )
        with self.connection() as connection:
            rows = connection.execute(
                f"""
                SELECT token_address,pair_address,name,symbol,analyzed_at,score,
                       risk_level,action_label,paper_entry_allowed,source_version,pool_id,
                       paper_decision,hard_stops_json,market_watch_checks,
                       market_watch_last_checked_at,market_watch_expires_at,
                       market_watch_reason,entry_liquidity_usd,
                       first_market_cap_usd,peak_market_cap_usd,
                       mcap_liquidity_ratio
                FROM candidates
                WHERE analysis_status='complete'{clause}
                ORDER BY analyzed_at DESC,block_number DESC LIMIT ?
                """,
                (
                    () if include_rejected else self.PENDING_ENTRY_DECISIONS
                ) + (max(0, limit),),
            ).fetchall()
        return [
            {
                "token_address": row["token_address"],
                "pair_address": row["pair_address"],
                "name": row["name"] or "",
                "symbol": row["symbol"] or "",
                "analyzed_at": row["analyzed_at"],
                "score": row["score"],
                "risk_level": row["risk_level"],
                "action_label": row["action_label"],
                "paper_entry_allowed": bool(row["paper_entry_allowed"]),
                "paper_decision": row["paper_decision"] or (
                    PAPER_DECISION_ADMITTED if row["paper_entry_allowed"] else PAPER_DECISION_REJECTED
                ),
                "hard_stops": json.loads(row["hard_stops_json"] or "[]"),
                "market_watch_checks": row["market_watch_checks"] or 0,
                "market_watch_last_checked_at": row["market_watch_last_checked_at"],
                "market_watch_expires_at": row["market_watch_expires_at"],
                "market_watch_reason": row["market_watch_reason"],
                "source_version": row["source_version"],
                "pool_id": row["pool_id"],
                "entry_liquidity_usd": row["entry_liquidity_usd"],
                "entry_market_cap_usd": row["first_market_cap_usd"],
                "peak_market_cap_usd": row["peak_market_cap_usd"],
                "mcap_liquidity_ratio": row["mcap_liquidity_ratio"],
                "mcap_liquidity_limit": SHADOW_MAXIMUM_MCAP_LIQUIDITY_RATIO,
            }
            for row in rows
        ]


class RobinhoodPairObserver:
    def __init__(self, rpc: RobinhoodRPC, state_path: str | Path):
        self.rpc = rpc
        self.state_path = Path(state_path)

    def _state(self) -> dict:
        return read_json(self.state_path, {}) or {}

    def sync(self, *, block_limit: int, lookback: int) -> tuple[list[dict], dict]:
        latest = _remote_call("Robinhood latest block", self.rpc.get_block_number)
        state = self._state()
        start = safe_int(state.get("next_block"), max(0,latest-lookback))
        start = min(start, latest)
        end = min(latest,start+max(1,block_limit)-1)
        logs = _remote_call(
            f"Robinhood V2 logs {start}-{end}",
            lambda: self.rpc.get_logs(
                start, end, address=UNISWAP_V2_FACTORY,
                topics=[PAIR_CREATED_TOPIC],
            ),
        )
        block_times = {}
        candidates = []
        wrapped = WETH_ADDRESS.lower()
        for log in logs or []:
            topics = log.get("topics") or []
            if len(topics)<3:
                continue
            token0,token1 = _topic_address(topics[1]),_topic_address(topics[2])
            if token0.lower()==wrapped:
                token=token1
            elif token1.lower()==wrapped:
                token=token0
            else:
                continue
            pair=_data_address(log.get("data"),0)
            if token==ZERO_ADDRESS or pair==ZERO_ADDRESS:
                continue
            block_number=int(str(log.get("blockNumber") or "0x0"),16)
            if block_number not in block_times:
                block = _remote_call(
                    f"Robinhood block {block_number}",
                    lambda block_number=block_number: self.rpc.get_block(block_number),
                )
                block_times[block_number]=int(str(block.get("timestamp") or "0x0"),16)
            try:
                name=self.rpc.erc20_name(token,block=block_number)
                symbol=self.rpc.erc20_symbol(token,block=block_number)
            except Exception:
                name=symbol=""
            candidates.append({
                "token_address":token,
                "pair_address":pair,
                "factory_address":UNISWAP_V2_FACTORY,
                "block_number":block_number,
                "block_timestamp":block_times[block_number],
                "transaction_hash":log.get("transactionHash") or "",
                "log_index":int(str(log.get("logIndex") or "0x0"),16),
                "name":name,"symbol":symbol,
            })
        coverage={
            "from_block":start,"to_block":end,"latest_block":latest,
            "blocks_scanned":end-start+1,"logs_seen":len(logs or []),
            "wrapped_native_pairs_found":len(candidates),
            "caught_up":end>=latest,"blocks_behind":max(0,latest-end),
            "scope":"uniswap_v2_wrapped_native_pairs_only",
            "measured_at":_utc_now(),
        }
        atomic_json_write(self.state_path,{"next_block":end+1,"coverage":coverage,"updated_at":_utc_now()})
        return candidates,coverage


class RobinhoodV4Observer:
    """Observe V4 pools cheaply and activate only after liquidity and a swap."""
    def __init__(self, rpc: RobinhoodRPC, store: RobinhoodLearningStore, state_path: str | Path):
        self.rpc = rpc
        self.store = store
        self.state_path = Path(state_path)

    def _state(self) -> dict:
        return read_json(self.state_path, {}) or {}

    def _adaptive_logs(self, start: int, end: int) -> tuple[list[dict], int]:
        """Split deterministic provider-limit failures without moving the cursor."""
        pending = [(start, end)]
        logs: list[dict] = []
        successful_windows = 0
        while pending:
            window_start, window_end = pending.pop()
            try:
                window_logs = _remote_call(
                    f"Robinhood V4 logs {window_start}-{window_end}",
                    lambda window_start=window_start, window_end=window_end: self.rpc.get_logs(
                        window_start,
                        window_end,
                        address=UNISWAP_V4_POOL_MANAGER,
                        topics=[[
                            V4_INITIALIZE_TOPIC,
                            V4_MODIFY_LIQUIDITY_TOPIC,
                            V4_SWAP_TOPIC,
                        ]],
                    ),
                )
            except RuntimeError as exc:
                if (
                    "exceeds limit of 10000" not in str(exc).lower()
                    or window_start >= window_end
                ):
                    raise
                midpoint = (window_start + window_end) // 2
                # LIFO order keeps requests and accumulated events chronological.
                pending.append((midpoint + 1, window_end))
                pending.append((window_start, midpoint))
                continue
            logs.extend(window_logs or [])
            successful_windows += 1
        return logs, successful_windows

    def sync(self, *, block_limit: int, lookback: int) -> tuple[list[dict], dict]:
        latest = _remote_call("Robinhood latest block", self.rpc.get_block_number)
        state = self._state()
        start = safe_int(state.get("next_block"), max(0, latest - lookback))
        start = min(start, latest)
        end = min(latest, start + max(1, block_limit) - 1)
        logs, rpc_windows = self._adaptive_logs(start, end)
        anchors = {WETH_ADDRESS.lower(), USDG_ADDRESS.lower()}
        known_pool_ids = self.store.known_v4_pool_ids()
        events = []
        counts = {"initialize": 0, "modify": 0, "swap": 0}
        ordered = sorted(logs or [], key=lambda row: (
            int(str(row.get("blockNumber") or "0x0"), 16),
            int(str(row.get("logIndex") or "0x0"), 16),
        ))
        for log in ordered:
            topics = log.get("topics") or []
            if len(topics) < 2:
                continue
            event_topic = str(topics[0]).lower()
            pool_id = str(topics[1]).lower()
            block_number = int(str(log.get("blockNumber") or "0x0"), 16)
            if event_topic == V4_INITIALIZE_TOPIC:
                if len(topics) < 4:
                    continue
                currency0 = _topic_address(topics[2]).lower()
                currency1 = _topic_address(topics[3]).lower()
                if (currency0 in anchors) == (currency1 in anchors):
                    continue
                token = currency1 if currency0 in anchors else currency0
                anchor = currency0 if currency0 in anchors else currency1
                events.append({
                    "kind": "initialize", "pool_id": pool_id,
                    "currency0": currency0, "currency1": currency1,
                    "token_address": token, "anchor_address": anchor,
                    "fee_tier": _data_word(log.get("data"), 0) & ((1 << 24) - 1),
                    "tick_spacing": _signed_word(log.get("data"), 1, 24),
                    "hooks_address": _data_address(log.get("data"), 2).lower(),
                    "sqrt_price_x96": _data_word(log.get("data"), 3),
                    "tick": _signed_word(log.get("data"), 4, 24),
                    "block_number": block_number,
                })
                known_pool_ids.add(pool_id)
                counts["initialize"] += 1
            elif event_topic == V4_MODIFY_LIQUIDITY_TOPIC:
                if pool_id not in known_pool_ids:
                    continue
                events.append({"kind": "modify", "pool_id": pool_id, "block_number": block_number})
                counts["modify"] += 1
            elif event_topic == V4_SWAP_TOPIC:
                if pool_id not in known_pool_ids:
                    continue
                events.append({
                    "kind": "swap", "pool_id": pool_id, "block_number": block_number,
                    "block_timestamp": None,
                    "transaction_hash": log.get("transactionHash") or "",
                    "log_index": int(str(log.get("logIndex") or "0x0"), 16),
                    "sender_hint": (
                        _topic_address(topics[2]).lower()
                        if len(topics) > 2 else None
                    ),
                    "amount0_raw": _signed_word(log.get("data"), 0, 128),
                    "amount1_raw": _signed_word(log.get("data"), 1, 128),
                    "sqrt_price_x96": _data_word(log.get("data"), 2),
                    "active_liquidity": _data_word(log.get("data"), 3),
                    "tick": _signed_word(log.get("data"), 4, 24),
                })
                counts["swap"] += 1
        self.store.apply_v4_events(events)
        activations = self.store.pending_v4_activations()
        candidates = []
        activation_block_times: dict[int, int] = {}
        for pool in activations:
            token = pool["token_address"]
            activation_block = pool["swapped_block"]
            if activation_block not in activation_block_times:
                block = _remote_call(
                    f"Robinhood block {activation_block}",
                    lambda activation_block=activation_block: self.rpc.get_block(
                        activation_block
                    ),
                )
                activation_block_times[activation_block] = int(str(block.get("timestamp") or "0x0"), 16)
            try:
                name = self.rpc.erc20_name(token, block=pool["swapped_block"])
                symbol = self.rpc.erc20_symbol(token, block=pool["swapped_block"])
            except Exception:
                name = symbol = ""
            candidates.append({
                "token_address": token, "pair_address": UNISWAP_V4_POOL_MANAGER,
                "factory_address": UNISWAP_V4_POOL_MANAGER,
                "block_number": activation_block, "block_timestamp": activation_block_times[activation_block],
                "transaction_hash": pool["transaction_hash"] or "", "log_index": pool["log_index"] or 0,
                "name": name, "symbol": symbol, "source_version": SOURCE_V4,
                "pool_id": pool["pool_id"], "hooks_address": pool["hooks_address"],
                "fee_tier": pool["fee_tier"], "tick_spacing": pool["tick_spacing"],
            })
        coverage = {
            "from_block": start, "to_block": end, "latest_block": latest,
            "blocks_scanned": end-start+1, "logs_seen": len(logs or []),
            "rpc_windows": rpc_windows,
            "initialize_events": counts["initialize"], "liquidity_events": counts["modify"],
            "swap_events": counts["swap"], "activated_pools": len(candidates),
            "caught_up": end >= latest, "blocks_behind": max(0, latest-end),
            "scope": "uniswap_v4_weth_or_usdg_first_swap_activation", "measured_at": _utc_now(),
        }
        atomic_json_write(self.state_path, {"next_block": end+1, "coverage": coverage, "updated_at": _utc_now()})
        return candidates, coverage


class RobinhoodV4CustodyVerifier:
    """Build prospective, pool-specific V4 position custody evidence.

    The verifier follows canonical PositionManager ERC-721 transfers, maps the
    packed position info back to a known full pool id, and measures only
    currently active in-range liquidity. Unknown routers and unmanaged core
    liquidity reduce coverage; they are never silently classified as locked.
    """

    def __init__(
        self, rpc: RobinhoodRPC, store: RobinhoodLearningStore,
        state_path: str | Path,
    ):
        self.rpc = rpc
        self.store = store
        self.state_path = Path(state_path)

    @staticmethod
    def _uint_word(value: int) -> str:
        return f"{value & ((1 << 256) - 1):064x}"

    @staticmethod
    def _decode_address(raw: str | None) -> str:
        text = str(raw or "").removeprefix("0x")
        if len(text) < 64:
            return ZERO_ADDRESS
        return "0x" + text[-40:].lower()

    @staticmethod
    def _decode_tick(value: int) -> int:
        value &= (1 << 24) - 1
        return value - (1 << 24) if value & (1 << 23) else value

    def _position_identity(
        self, token_id: int, pools_by_prefix: dict[str, dict], *, block: int,
    ) -> tuple[dict, int, int] | None:
        try:
            raw = self.rpc.call(
                UNISWAP_V4_POSITION_MANAGER,
                "0x" + V4_POSITION_INFO_SELECTOR + self._uint_word(token_id),
                block=block,
            )
            packed = int(raw or "0x0", 16)
        except Exception:
            return None
        if packed <= 0:
            return None
        pool = pools_by_prefix.get(f"{packed >> 56:050x}")
        if not pool:
            return None
        tick_lower = self._decode_tick(packed >> 8)
        tick_upper = self._decode_tick(packed >> 32)
        return pool, tick_lower, tick_upper

    def _position_identities(
        self, token_ids: list[int], pools_by_prefix: dict[str, dict],
        *, block: int,
    ) -> dict[int, tuple[dict, int, int]]:
        unique = list(dict.fromkeys(token_ids))
        if not unique:
            return {}
        if not callable(getattr(self.rpc, "calls", None)):
            return {
                token_id: identity for token_id in unique
                if (identity := self._position_identity(
                    token_id,pools_by_prefix,block=block,
                ))
            }
        requests_ = [
            (
                UNISWAP_V4_POSITION_MANAGER,
                "0x" + V4_POSITION_INFO_SELECTOR + self._uint_word(token_id),
            )
            for token_id in unique
        ]
        responses = self.rpc.calls(requests_, block=block)
        identities = {}
        for token_id, response in zip(unique, responses):
            try:
                packed = int(response.get("result") or "0x0", 16)
            except (TypeError, ValueError):
                continue
            if packed <= 0:
                continue
            pool = pools_by_prefix.get(f"{packed >> 56:050x}")
            if not pool:
                continue
            identities[token_id] = (
                pool,self._decode_tick(packed >> 8),
                self._decode_tick(packed >> 32),
            )
        return identities

    def _timelock_evidence(
        self, owner: str, token_id: int, now: float,
    ) -> dict:
        result = {
            "verified": False, "platform": None,
            "unlock_timestamp": None,
            "minimum_unlock_horizon_seconds": (
                V4_CUSTODY_MINIMUM_UNLOCK_HORIZON_SECONDS
            ),
        }
        for platform, selector in V4_TIMELOCK_PROBES.items():
            try:
                raw = self.rpc.call(
                    owner, "0x" + selector + self._uint_word(token_id)
                )
                unlock = int(raw or "0x0", 16)
            except Exception:
                continue
            if (
                now + V4_CUSTODY_MINIMUM_UNLOCK_HORIZON_SECONDS <= unlock
                < 4_102_444_800  # 2100-01-01; rejects arbitrary uint values.
            ):
                result.update({
                    "verified": True, "platform": platform,
                    "unlock_timestamp": float(unlock),
                })
                break
        return result

    def _store_position_state(
        self, row: dict, latest: int, *, owner: str, liquidity: int,
        approved: str, code: str, timelock: dict, now: float,
    ) -> bool:
        token_id = safe_int(row.get("token_id"), 0)
        bytecode = str(code or "0x").removeprefix("0x")
        code_hash = (
            hashlib.sha256(bytes.fromhex(bytecode)).hexdigest()
            if bytecode and len(bytecode) % 2 == 0 else None
        )
        verified_locker = (
            V4_VERIFIED_LOCKER_CODE_HASHES.get(code_hash or "")
        )
        if liquidity <= 0:
            custody_class = "empty_position"
        elif owner in V4_IRRECOVERABLE_NFT_OWNERS and approved == ZERO_ADDRESS:
            custody_class = "verified_locked"
            timelock = {
                "verified": True, "platform": "irrecoverable_nft_owner",
                "unlock_timestamp": None,
            }
        elif approved != ZERO_ADDRESS:
            custody_class = "approved_delegate"
        elif timelock.get("verified") and verified_locker:
            custody_class = "verified_locked"
        elif bytecode:
            custody_class = "contract_custody_unverified"
        else:
            custody_class = "eoa_controlled"
        current_tick = safe_int(row.get("current_tick"), 0)
        in_range = bool(
            liquidity > 0
            and safe_int(row.get("tick_lower"), 0) <= current_tick
            < safe_int(row.get("tick_upper"), 0)
        )
        evidence = {
            "schema_version": 1,
            "owner": owner,
            "token_specific_approved": approved,
            "owner_has_code": bool(bytecode),
            "owner_code_sha256": code_hash,
            "locker_bytecode_allowlisted": bool(verified_locker),
            "verified_locker_name": verified_locker,
            "timelock": timelock,
            "tick_lower": safe_int(row.get("tick_lower"), 0),
            "tick_upper": safe_int(row.get("tick_upper"), 0),
            "current_tick": current_tick,
            "in_active_range": in_range,
            "liquidity_raw": str(liquidity),
            "operator_approval_enumerated": False,
        }
        self.store.update_v4_position_state(
            token_id,owner_address=owner,approved_address=approved,
            liquidity_raw=liquidity,in_active_range=in_range,
            custody_class=custody_class,
            locker_platform=timelock.get("platform"),
            unlock_timestamp=timelock.get("unlock_timestamp"),
            owner_code_sha256=code_hash,checked_block=latest,evidence=evidence,
        )
        return True

    def _refresh_position(self, row: dict, latest: int, now: float) -> bool:
        token_id = safe_int(row.get("token_id"), 0)
        argument = self._uint_word(token_id)
        stored_owner = str(row.get("owner_address") or ZERO_ADDRESS).lower()
        try:
            owner = self._decode_address(self.rpc.call(
                UNISWAP_V4_POSITION_MANAGER,
                "0x" + ERC721_OWNER_OF_SELECTOR + argument,block=latest,
            ))
        except Exception:
            owner = stored_owner
        try:
            liquidity = int(self.rpc.call(
                UNISWAP_V4_POSITION_MANAGER,
                "0x" + V4_POSITION_LIQUIDITY_SELECTOR + argument,block=latest,
            ) or "0x0", 16)
        except Exception:
            liquidity = 0
        try:
            approved = self._decode_address(self.rpc.call(
                UNISWAP_V4_POSITION_MANAGER,
                "0x" + ERC721_GET_APPROVED_SELECTOR + argument,block=latest,
            ))
        except Exception:
            approved = ZERO_ADDRESS
        try:
            code = self.rpc.get_code(owner, block=latest) if owner else "0x"
        except Exception:
            code = "0x"
        timelock = (
            self._timelock_evidence(owner, token_id, now)
            if str(code or "0x") not in {"", "0x"} else {
                "verified": False,"platform": None,"unlock_timestamp": None,
            }
        )
        return self._store_position_state(
            row,latest,owner=owner,liquidity=liquidity,approved=approved,
            code=code,timelock=timelock,now=now,
        )

    def _refresh_positions(
        self, rows: list[dict], latest: int, now: float,
    ) -> int:
        if not rows:
            return 0
        if (
            not callable(getattr(self.rpc, "calls", None))
            or not callable(getattr(self.rpc, "get_codes", None))
        ):
            return sum(
                self._refresh_position(row,latest,now) for row in rows
            )
        reads = []
        for row in rows:
            argument = self._uint_word(safe_int(row.get("token_id"), 0))
            reads.extend([
                (UNISWAP_V4_POSITION_MANAGER,
                 "0x" + ERC721_OWNER_OF_SELECTOR + argument),
                (UNISWAP_V4_POSITION_MANAGER,
                 "0x" + V4_POSITION_LIQUIDITY_SELECTOR + argument),
                (UNISWAP_V4_POSITION_MANAGER,
                 "0x" + ERC721_GET_APPROVED_SELECTOR + argument),
            ])
        responses = self.rpc.calls(reads, block=latest)
        resolved = []
        owners = []
        for index, row in enumerate(rows):
            owner_response,liquidity_response,approved_response = responses[
                index * 3:index * 3 + 3
            ]
            owner = self._decode_address(owner_response.get("result"))
            if owner == ZERO_ADDRESS:
                owner = str(row.get("owner_address") or ZERO_ADDRESS).lower()
            try:
                liquidity = int(liquidity_response.get("result") or "0x0", 16)
            except (TypeError, ValueError):
                liquidity = 0
            approved = self._decode_address(approved_response.get("result"))
            resolved.append((row,owner,liquidity,approved))
            if owner != ZERO_ADDRESS:
                owners.append(owner)
        unique_owners = list(dict.fromkeys(owners))
        code_responses = self.rpc.get_codes(unique_owners, block=latest)
        codes = {
            owner: str(response.get("result") or "0x")
            for owner, response in zip(unique_owners, code_responses)
        }
        probe_requests = []
        probe_metadata = []
        for row, owner, _liquidity, _approved in resolved:
            if str(codes.get(owner) or "0x") in {"", "0x"}:
                continue
            token_id = safe_int(row.get("token_id"), 0)
            for platform, selector in V4_TIMELOCK_PROBES.items():
                probe_requests.append((
                    owner,"0x" + selector + self._uint_word(token_id),
                ))
                probe_metadata.append((token_id,platform))
        timelocks: dict[int, dict] = {}
        if probe_requests:
            probe_responses = self.rpc.calls(probe_requests, block=latest)
            for metadata, response in zip(probe_metadata,probe_responses):
                token_id, platform = metadata
                if token_id in timelocks:
                    continue
                try:
                    unlock = int(response.get("result") or "0x0", 16)
                except (TypeError, ValueError):
                    continue
                if (
                    now + V4_CUSTODY_MINIMUM_UNLOCK_HORIZON_SECONDS <= unlock
                    < 4_102_444_800
                ):
                    timelocks[token_id] = {
                        "verified": True,"platform": platform,
                        "unlock_timestamp": float(unlock),
                        "minimum_unlock_horizon_seconds": (
                            V4_CUSTODY_MINIMUM_UNLOCK_HORIZON_SECONDS
                        ),
                    }
        refreshed = 0
        for row, owner, liquidity, approved in resolved:
            refreshed += self._store_position_state(
                row,latest,owner=owner,liquidity=liquidity,approved=approved,
                code=codes.get(owner,"0x"),
                timelock=timelocks.get(safe_int(row.get("token_id"), 0), {
                    "verified": False,"platform": None,
                    "unlock_timestamp": None,
                }),now=now,
            )
        return refreshed

    def _transfer_logs(self, start: int, end: int) -> tuple[list[dict], int]:
        """Read a custody range without advancing either durable cursor.

        A provider-limit split is deterministic and only the caller commits a
        cursor after every sub-window has succeeded.
        """
        if start > end:
            return [], 0
        pending = [(start, end)]
        logs: list[dict] = []
        successful_windows = 0
        while pending:
            window_start, window_end = pending.pop()
            try:
                rows = _remote_call(
                    f"Robinhood V4 PositionManager transfers "
                    f"{window_start}-{window_end}",
                    lambda window_start=window_start, window_end=window_end: (
                        self.rpc.get_logs(
                            window_start, window_end,
                            address=UNISWAP_V4_POSITION_MANAGER,
                            topics=[V4_POSITION_TRANSFER_TOPIC],
                        )
                    ),
                )
            except RuntimeError as exc:
                if (
                    "exceeds limit of 10000" not in str(exc).lower()
                    or window_start >= window_end
                ):
                    raise
                midpoint = (window_start + window_end) // 2
                pending.append((midpoint + 1, window_end))
                pending.append((window_start, midpoint))
                continue
            logs.extend(rows or [])
            successful_windows += 1
        return sorted(logs, key=lambda item: (
            int(str(item.get("blockNumber") or "0x0"), 16),
            int(str(item.get("logIndex") or "0x0"), 16),
        )), successful_windows

    def _apply_transfer_logs(
        self, logs: list[dict], pools_by_prefix: dict[str, dict], latest: int,
    ) -> tuple[int, int, set[str]]:
        token_ids = [
            int(str((log.get("topics") or [None, None, None, "0x0"])[3]), 16)
            for log in logs if len(log.get("topics") or []) >= 4
            and str((log.get("topics") or [""])[0]).lower()
                == V4_POSITION_TRANSFER_TOPIC
        ]
        identities = self._position_identities(
            token_ids, pools_by_prefix, block=latest,
        )
        relevant = unmapped = 0
        affected: set[str] = set()
        for log in logs:
            topics = log.get("topics") or []
            if (
                len(topics) < 4
                or str(topics[0]).lower() != V4_POSITION_TRANSFER_TOPIC
            ):
                continue
            token_id = int(str(topics[3]), 16)
            identity = identities.get(token_id)
            if not identity:
                unmapped += 1
                continue
            pool, tick_lower, tick_upper = identity
            pool_id = str(pool["pool_id"]).lower()
            self.store.record_v4_position_transfer(
                token_id=token_id, pool_id=pool_id,
                tick_lower=tick_lower, tick_upper=tick_upper,
                transaction_hash=str(log.get("transactionHash") or ""),
                log_index=int(str(log.get("logIndex") or "0x0"), 16),
                from_address=_topic_address(topics[1]),
                to_address=_topic_address(topics[2]),
                block_number=int(str(log.get("blockNumber") or "0x0"), 16),
            )
            relevant += 1
            affected.add(pool_id)
        return relevant, unmapped, affected

    def sync(
        self, *, block_limit: int, lookback: int,
        live_block_limit: int | None = None,
    ) -> dict:
        pools = self.store.known_v4_pools()
        if not pools or not hasattr(self.rpc, "call") or not hasattr(self.rpc, "get_code"):
            return {
                "supported": False, "reason": "no_known_pools_or_rpc_read_support",
                "transfers_seen": 0, "positions_refreshed": 0,
            }
        state = read_json(self.state_path, {}) or {}
        latest = _remote_call("Robinhood V4 custody head", self.rpc.get_block_number)
        earliest_pool_block = min(
            safe_int(pool.get("initialized_block"), latest) for pool in pools
        )
        pools_by_prefix = {
            str(pool["pool_id"]).lower().removeprefix("0x")[:50]: pool
            for pool in pools
        }
        live_limit = max(1, int(live_block_limit or block_limit))
        # Migration keeps the legacy next_block as historical backfill while
        # immediately opening a second cursor close to the current head.
        live_default = max(earliest_pool_block, latest - live_limit + 1)
        live_start = safe_int(state.get("live_next_block"), live_default)
        live_start = max(earliest_pool_block, min(live_start, latest + 1))
        live_end = min(latest, live_start + live_limit - 1)
        live_logs, live_rpc_windows = self._transfer_logs(live_start, live_end)
        live_relevant, live_unmapped, live_affected = self._apply_transfer_logs(
            live_logs, pools_by_prefix, latest,
        )

        backfill_start = safe_int(
            state.get("backfill_next_block", state.get("next_block")),
            earliest_pool_block,
        )
        backfill_start = max(earliest_pool_block, min(backfill_start, latest + 1))
        backfill_target = min(latest, live_start - 1)
        last_backfill = _timestamp(
            state.get("backfill_updated_at") or state.get("updated_at")
        )
        backfill_due = bool(
            last_backfill is None
            or time.time() - last_backfill
                >= V4_CUSTODY_MINIMUM_SYNC_INTERVAL_SECONDS
        )
        backfill_end = backfill_start - 1
        backfill_logs: list[dict] = []
        backfill_rpc_windows = 0
        backfill_relevant = backfill_unmapped = 0
        backfill_affected: set[str] = set()
        backfill_error = None
        if backfill_due and backfill_start <= backfill_target:
            backfill_end = min(
                backfill_target,
                backfill_start + max(1, int(block_limit)) - 1,
            )
            try:
                backfill_logs, backfill_rpc_windows = self._transfer_logs(
                    backfill_start, backfill_end,
                )
                (
                    backfill_relevant,
                    backfill_unmapped,
                    backfill_affected,
                ) = self._apply_transfer_logs(
                    backfill_logs, pools_by_prefix, latest,
                )
            except Exception as exc:
                # The live cursor remains useful even when historical RPC
                # reconstruction is throttled. Never advance the failed lane.
                backfill_error = str(exc)[:500]
                backfill_end = backfill_start - 1
                backfill_logs = []
                backfill_rpc_windows = 0

        relevant_transfers = live_relevant + backfill_relevant
        unmapped_transfers = live_unmapped + backfill_unmapped
        affected_pools: set[str] = set()
        affected_pools.update(live_affected)
        affected_pools.update(backfill_affected)

        now = time.time()
        refresh_rows = self.store.v4_positions_for_refresh(
            V4_CUSTODY_POSITION_REFRESH_LIMIT
        )
        try:
            refreshed = self._refresh_positions(refresh_rows,latest,now)
            affected_pools.update(
                str(row["pool_id"]).lower() for row in refresh_rows
            )
        except Exception:
            refreshed = 0
        # A zero-coverage snapshot is meaningful: it prevents pools created by
        # unknown routers from being mistaken for verified PositionManager LP.
        snapshots = []
        for pool in pools:
            snapshot = self.store.record_v4_custody_snapshot(
                pool["pool_id"],observed_block=latest,
            )
            if snapshot:
                snapshots.append(snapshot)
        live_blocks_scanned = max(0, live_end - live_start + 1)
        backfill_blocks_scanned = max(0, backfill_end - backfill_start + 1)
        historical_next = (
            backfill_end + 1 if backfill_end >= backfill_start
            else backfill_start
        )
        live_behind = max(0, latest - live_end)
        historical_behind = max(0, backfill_target - (historical_next - 1))
        coverage = {
            "supported": True,
            "schema_version": 2,
            "from_block": min(live_start, backfill_start),
            "to_block": max(live_end, backfill_end),
            "latest_block": latest,
            "blocks_scanned": live_blocks_scanned + backfill_blocks_scanned,
            "logs_seen": len(live_logs) + len(backfill_logs),
            "relevant_transfers": relevant_transfers,
            "unmapped_transfers": unmapped_transfers,
            "positions_refreshed": refreshed,
            "position_refresh_limit": V4_CUSTODY_POSITION_REFRESH_LIMIT,
            "pools_snapshotted": len(snapshots),
            "caught_up": live_behind == 0 and historical_behind == 0,
            "blocks_behind": historical_behind,
            "live_caught_up": live_behind == 0,
            "live_blocks_behind": live_behind,
            "historical_caught_up": historical_behind == 0,
            "historical_blocks_behind": historical_behind,
            "live": {
                "from_block": live_start, "to_block": live_end,
                "blocks_scanned": live_blocks_scanned,
                "logs_seen": len(live_logs),
                "rpc_windows": live_rpc_windows,
                "caught_up": live_behind == 0,
                "blocks_behind": live_behind,
            },
            "backfill": {
                "from_block": backfill_start,
                "to_block": backfill_end if backfill_blocks_scanned else None,
                "target_block": backfill_target,
                "blocks_scanned": backfill_blocks_scanned,
                "logs_seen": len(backfill_logs),
                "rpc_windows": backfill_rpc_windows,
                "due": backfill_due,
                "error": backfill_error,
                "caught_up": historical_behind == 0,
                "blocks_behind": historical_behind,
                "minimum_sync_interval_seconds": (
                    V4_CUSTODY_MINIMUM_SYNC_INTERVAL_SECONDS
                ),
            },
            "scope": "canonical_v4_position_manager_shadow_custody_v1",
            "measured_at": _utc_now(),
        }
        next_state = {
            "schema_version": 2,
            "live_next_block": live_end + 1,
            "backfill_next_block": historical_next,
            "coverage": coverage,
            "updated_at": _utc_now(),
            "backfill_updated_at": (
                _utc_now()
                if backfill_due and backfill_error is None
                else state.get("backfill_updated_at") or state.get("updated_at")
            ),
        }
        atomic_json_write(self.state_path, next_state)
        return coverage


class RobinhoodMarketClient:
    def __init__(self, timeout: float = 10.0):
        self.timeout=timeout
        self.session=requests.Session()
        self._anchor_cache: tuple[float, str, float] | None = None
        self._anchor_lock = threading.Lock()

    @staticmethod
    def _matches_by_token(pairs: list[dict], tokens: set[str]) -> dict[str, list[dict]]:
        matches = {token: [] for token in tokens}
        for pair in pairs:
            if str(pair.get("chainId") or "").lower()!="robinhood":
                continue
            base=(pair.get("baseToken") or {}).get("address","").lower()
            # DexScreener priceUsd is the base token's price. A quote-only
            # match would silently mark the wrapped-native asset as this token.
            if base not in tokens:
                continue
            labels = [str(value).lower() for value in pair.get("labels") or []]
            version = (
                SOURCE_V4 if "v4" in labels else
                SOURCE_V3 if "v3" in labels else SOURCE_V2
            )
            matches[base].append({
                "pair_address":pair.get("pairAddress"),"dex_id":pair.get("dexId"),
                "price_usd":safe_float(pair.get("priceUsd"),0.0) or None,
                "liquidity_usd":safe_float((pair.get("liquidity") or {}).get("usd"),0.0) or None,
                "market_cap_usd":safe_float(pair.get("marketCap"),0.0) or None,
                "fdv_usd":safe_float(pair.get("fdv"),0.0) or None,
                "pair_symbol":(pair.get("baseToken") or {}).get("symbol"),
                "source":"dexscreener_exact_chain_token_pair",
                "source_version": version,
                "labels": labels,
            })
        return matches

    def snapshots_many(self, tokens: list[str]) -> dict[str, list[dict]]:
        """Fetch up to a cycle's token markets with one request per 30 tokens."""
        normalized = list(dict.fromkeys(str(token).lower() for token in tokens if token))
        results = {token: [] for token in normalized}
        for offset in range(0, len(normalized), 30):
            chunk = normalized[offset:offset + 30]
            response = _remote_call(
                "DexScreener token market batch",
                lambda chunk=chunk: self.session.get(
                    "https://api.dexscreener.com/latest/dex/tokens/" + ",".join(chunk),
                    timeout=self.timeout,
                ),
            )
            response.raise_for_status()
            parsed = self._matches_by_token(
                response.json().get("pairs") or [], set(chunk)
            )
            for token, matches in parsed.items():
                results[token].extend(matches)
        return results

    def snapshots(self, token: str) -> list[dict]:
        return self.snapshots_many([token]).get(token.lower(), [])

    @staticmethod
    def _geckoterminal_matches_by_token(
        rows: list[dict], tokens: set[str], *, observed_at: str,
    ) -> dict[str, list[dict]]:
        """Normalize exact-token fallback data for outcome observation only."""
        matches = {token: [] for token in tokens}
        for row in rows:
            attributes = row.get("attributes") or {}
            address = str(attributes.get("address") or "").lower()
            if address not in tokens:
                continue
            matches[address].append({
                "pair_address": None,
                "dex_id": None,
                "price_usd": safe_float(attributes.get("price_usd"), 0.0) or None,
                # GeckoTerminal's token response exposes aggregate reserves,
                # not executable liquidity for one pair. Never put that value
                # in liquidity_usd, which is consumed by entry logic.
                "liquidity_usd": None,
                "aggregate_reserve_usd": (
                    safe_float(attributes.get("total_reserve_in_usd"), 0.0)
                    or None
                ),
                "market_cap_usd": (
                    safe_float(attributes.get("market_cap_usd"), 0.0) or None
                ),
                "fdv_usd": safe_float(attributes.get("fdv_usd"), 0.0) or None,
                "pair_symbol": attributes.get("symbol"),
                "source": "geckoterminal_robinhood_token_fallback",
                "source_scope": "outcome_observation_only",
                "entry_eligible_evidence": False,
                "liquidity_scope": "aggregate_token_reserve_not_pair_liquidity",
                "provider_observed_at": observed_at,
            })
        return matches

    def geckoterminal_snapshots_many(
        self, tokens: list[str],
    ) -> dict[str, list[dict]]:
        """Fetch independent fallback token observations in bounded batches."""
        normalized = list(dict.fromkeys(
            str(token).lower() for token in tokens if token
        ))
        results = {token: [] for token in normalized}
        observed_at = _utc_now()
        for offset in range(0, len(normalized), 30):
            chunk = normalized[offset:offset + 30]
            response = _remote_call(
                "GeckoTerminal Robinhood token market batch",
                lambda chunk=chunk: self.session.get(
                    "https://api.geckoterminal.com/api/v2/networks/"
                    "robinhood/tokens/multi/" + ",".join(chunk),
                    timeout=self.timeout,
                ),
            )
            response.raise_for_status()
            parsed = self._geckoterminal_matches_by_token(
                response.json().get("data") or [], set(chunk),
                observed_at=observed_at,
            )
            for token, matches in parsed.items():
                results[token].extend(matches)
        return results

    @staticmethod
    def _provider_error(exc: Exception) -> dict:
        return {
            "error_type": type(exc).__name__,
            "message": str(exc)[:500],
        }

    @staticmethod
    def outcome_value_available(market: dict | None) -> bool:
        market = market or {}
        return bool(
            safe_float(market.get("market_cap_usd"), 0.0)
            or safe_float(market.get("fdv_usd"), 0.0)
        )

    def outcome_snapshots_many(
        self, candidates: list[dict],
    ) -> tuple[dict[str, dict], dict]:
        """Resolve outcome values across independent, non-entry providers.

        DexScreener remains primary. GeckoTerminal is consulted once per
        bounded batch and is selected only when the primary lacks a market-cap
        value. Provider failures remain visible in each checkpoint's evidence.
        """
        tokens = list(dict.fromkeys(
            str(row.get("token_address") or "").lower()
            for row in candidates if row.get("token_address")
        ))
        primary: dict[str, list[dict]] = {token: [] for token in tokens}
        fallback: dict[str, list[dict]] = {token: [] for token in tokens}
        telemetry = {
            "schema_version": 1,
            "purpose": "outcome_observation_only",
            "tokens": len(tokens),
            "providers": {
                "dexscreener": {"state": "not_attempted", "matched": 0},
                "geckoterminal": {"state": "not_attempted", "matched": 0},
            },
            "fallback_selected": 0,
            "confirmed_no_market": 0,
            "provider_unavailable": 0,
        }
        try:
            primary = self.snapshots_many(tokens)
            telemetry["providers"]["dexscreener"].update({
                "state": "responded",
                "matched": sum(bool(value) for value in primary.values()),
            })
        except Exception as exc:
            telemetry["providers"]["dexscreener"].update({
                "state": "failed", "error": self._provider_error(exc),
            })
        try:
            fallback = self.geckoterminal_snapshots_many(tokens)
            telemetry["providers"]["geckoterminal"].update({
                "state": "responded",
                "matched": sum(bool(value) for value in fallback.values()),
            })
        except Exception as exc:
            telemetry["providers"]["geckoterminal"].update({
                "state": "failed", "error": self._provider_error(exc),
            })

        resolved: dict[str, dict] = {}
        by_token = {
            str(row.get("token_address") or "").lower(): row
            for row in candidates
        }
        for token in tokens:
            candidate = by_token.get(token) or {}
            primary_market = self.select_snapshot(
                primary.get(token, []), candidate.get("pair_address")
            )
            fallback_market = self.select_snapshot(fallback.get(token, []))
            if self.outcome_value_available(primary_market):
                market = dict(primary_market)
                winner = "dexscreener"
            elif self.outcome_value_available(fallback_market):
                market = dict(fallback_market)
                winner = "geckoterminal"
                telemetry["fallback_selected"] += 1
            elif primary_market:
                market = dict(primary_market)
                winner = "dexscreener_incomplete"
            elif fallback_market:
                market = dict(fallback_market)
                winner = "geckoterminal_incomplete"
            else:
                market = {}
                winner = None
            provider_states = {
                name: value["state"]
                for name, value in telemetry["providers"].items()
            }
            any_response = "responded" in provider_states.values()
            retryable = not market and not any_response
            if not market and any_response:
                telemetry["confirmed_no_market"] += 1
            if retryable:
                telemetry["provider_unavailable"] += 1
            market["market_resolution"] = {
                "schema_version": 1,
                "purpose": "outcome_observation_only",
                "winner": winner,
                "provider_states": provider_states,
                "fallback_used": winner in {
                    "geckoterminal", "geckoterminal_incomplete"
                },
                "confirmed_no_market": bool(not market or len(market) == 1)
                and any_response,
                "retryable_provider_failure": retryable,
                "entry_eligible_evidence": False,
            }
            resolved[token] = market
        return resolved, telemetry

    @staticmethod
    def select_snapshot(matches: list[dict], pair_address: str | None = None) -> dict:
        if not matches:
            return {}
        exact = [
            market for market in matches
            if pair_address and str(market.get("pair_address") or "").lower() == pair_address.lower()
        ]
        choices = exact or matches
        return max(
            choices,
            key=lambda market: safe_float(market.get("liquidity_usd"), 0.0),
        )

    def snapshot(self, token: str, pair_address: str | None = None) -> dict:
        return self.select_snapshot(self.snapshots(token), pair_address)

    def wrapped_native_usd(self) -> tuple[float, str]:
        """Resolve ETH/USD independently of Robinhood pair indexing."""
        with self._anchor_lock:
            cached = self._anchor_cache
            if cached and time.monotonic() - cached[2] < ANCHOR_PRICE_CACHE_SECONDS:
                return cached[0], cached[1]
            resolved = self._resolve_wrapped_native_usd()
            if resolved[0] > 0:
                self._anchor_cache = (resolved[0], resolved[1], time.monotonic())
            return resolved

    def _resolve_wrapped_native_usd(self) -> tuple[float, str]:
        try:
            market = self.snapshot(WETH_ADDRESS)
            price = safe_float(market.get("price_usd"), 0.0)
            if price > 0:
                return price, str(market.get("source") or "dexscreener")
        except Exception:
            pass
        response = _remote_call(
            "Coinbase ETH/USD spot",
            lambda: self.session.get(
                "https://api.coinbase.com/v2/prices/ETH-USD/spot",
                timeout=self.timeout,
            ),
        )
        response.raise_for_status()
        price = safe_float(((response.json().get("data") or {}).get("amount")), 0.0)
        return price, "coinbase_eth_usd_spot" if price > 0 else "unavailable"


class RobinhoodV4MarketClient:
    """Read current V4 pool state and derive explicitly-labelled market evidence."""
    def __init__(self, rpc: RobinhoodRPC, anchors: RobinhoodMarketClient, store: RobinhoodLearningStore):
        self.rpc = rpc
        self.anchors = anchors
        self.store = store

    def singleton_balance_fraction(self, token: str, total_supply: int) -> float | None:
        """Diagnostic token balance at the V4 singleton, never pool reserves.

        PoolManager custody is shared across pools and hooks can use virtual
        accounting. Its aggregate ERC-20 balance therefore cannot establish
        the inventory or exit capacity of one pool. Keep the read as explicitly
        labelled telemetry; no admission, liquidation, or reflection rule may
        treat it as pool-specific liquidity.
        """
        if total_supply <= 0:
            return None
        try:
            raw = self.rpc.call(
                token,
                "0x" + ERC20_BALANCE_OF_SELECTOR + "0" * 24
                + UNISWAP_V4_POOL_MANAGER.removeprefix("0x"),
            )
        except Exception:
            return None
        held = int(raw or "0x0", 16)
        return held / total_supply

    @staticmethod
    def _decode_slot0(raw: str) -> tuple[int, int]:
        text = str(raw or "").removeprefix("0x")
        if len(text) < 128:
            return 0, 0
        sqrt_price = int(text[:64], 16) & ((1 << 160) - 1)
        tick_raw = int(text[64:128], 16) & ((1 << 24) - 1)
        tick = tick_raw - (1 << 24) if tick_raw & (1 << 23) else tick_raw
        return sqrt_price, tick

    @staticmethod
    def _abi_word(value: int) -> str:
        if value < 0:
            value += 1 << 256
        return f"{value & ((1 << 256) - 1):064x}"

    @staticmethod
    def _abi_address(value: str) -> str:
        return "0" * 24 + str(value).lower().removeprefix("0x")

    def _quote_exact_input_single(
        self, pool: dict, *, zero_for_one: bool, exact_amount: int,
        block: int | str | None = None,
    ) -> tuple[int, int]:
        """Execute the official V4Quoter call as an eth_call simulation."""
        # The sole tuple argument is dynamic because hookData is bytes. Its
        # head contains the five static PoolKey words, direction, amount, and
        # an offset to an empty bytes tail.
        words = [
            self._abi_word(32),
            self._abi_address(pool["currency0"]),
            self._abi_address(pool["currency1"]),
            self._abi_word(safe_int(pool["fee_tier"], 0)),
            self._abi_word(safe_int(pool["tick_spacing"], 0)),
            self._abi_address(pool.get("hooks_address") or ZERO_ADDRESS),
            self._abi_word(int(zero_for_one)),
            self._abi_word(exact_amount),
            self._abi_word(8 * 32),
            self._abi_word(0),
        ]
        raw = self.rpc.call(
            UNISWAP_V4_QUOTER,
            "0x" + V4_QUOTE_EXACT_INPUT_SINGLE_SELECTOR + "".join(words),
            block=block,
        )
        text = str(raw or "").removeprefix("0x")
        if len(text) < 128:
            raise ValueError("V4 quoter returned a truncated result")
        return int(text[:64], 16), int(text[64:128], 16)

    def execution_quote(
        self,
        pool: dict,
        *,
        token: str,
        token_decimals: int,
        anchor_decimals: int,
        anchor_usd: float,
        token_usd: float,
        block: int | str | None = None,
    ) -> dict:
        """Simulate a $100 buy and immediate sell against the exact pool key."""
        anchor = str(pool["anchor_address"]).lower()
        currency0 = str(pool["currency0"]).lower()
        buy_zero_for_one = currency0 == anchor
        anchor_in = int(
            V4_QUOTE_PROBE_USD / max(anchor_usd, 1e-30) * (10 ** anchor_decimals)
        )
        if anchor_in <= 0:
            return {"verified": False, "reason": "invalid_probe_amount"}
        try:
            token_out, buy_gas = self._quote_exact_input_single(
                pool, zero_for_one=buy_zero_for_one, exact_amount=anchor_in,
                block=block,
            )
            anchor_out, sell_gas = self._quote_exact_input_single(
                pool, zero_for_one=not buy_zero_for_one, exact_amount=token_out,
                block=block,
            )
        except Exception as exc:
            return {
                "verified": False,
                "reason": "v4_quoter_failed",
                "error": str(exc)[:500],
                "quoter": UNISWAP_V4_QUOTER,
            }
        expected_token_out = (
            V4_QUOTE_PROBE_USD / max(token_usd, 1e-30) * (10 ** token_decimals)
        )
        round_trip_ratio = anchor_out / max(1, anchor_in)
        return {
            "verified": bool(token_out > 0 and anchor_out > 0),
            "quoter": UNISWAP_V4_QUOTER,
            "probe_usd": V4_QUOTE_PROBE_USD,
            "anchor_in_raw": str(anchor_in),
            "token_out_raw": str(token_out),
            "anchor_out_raw": str(anchor_out),
            "buy_gas_estimate": buy_gas,
            "sell_gas_estimate": sell_gas,
            "buy_price_impact_fraction": max(
                0.0, 1 - token_out / max(1.0, expected_token_out)
            ),
            "round_trip_ratio": round_trip_ratio,
            "round_trip_loss_fraction": max(0.0, 1 - round_trip_ratio),
            "passes_round_trip_limit": bool(
                token_out > 0 and anchor_out > 0
                and round_trip_ratio >= 1 - V4_MAXIMUM_ROUND_TRIP_LOSS
            ),
            "quote_block": block,
            "token_decimals": token_decimals,
            "anchor_decimals": anchor_decimals,
            "pool_key": {
                "currency0": pool["currency0"],
                "currency1": pool["currency1"],
                "fee": pool["fee_tier"],
                "tick_spacing": pool["tick_spacing"],
                "hooks": pool.get("hooks_address") or ZERO_ADDRESS,
            },
        }

    def snapshot(
        self, candidate: dict, *, include_execution_quote: bool = True,
        quote_block: int | str | None = None,
    ) -> dict:
        pool_id = str(candidate.get("pool_id") or "")
        if len(pool_id.removeprefix("0x")) != 64:
            return {}
        argument = pool_id.removeprefix("0x")
        liquidity_raw = self.rpc.call(
            UNISWAP_V4_STATE_VIEW,
            "0x" + V4_GET_LIQUIDITY_SELECTOR + argument,
            block=quote_block,
        )
        slot0_raw = self.rpc.call(
            UNISWAP_V4_STATE_VIEW,
            "0x" + V4_GET_SLOT0_SELECTOR + argument,
            block=quote_block,
        )
        active_liquidity = int(liquidity_raw or "0x0", 16)
        sqrt_price, tick = self._decode_slot0(slot0_raw)
        if active_liquidity <= 0 or sqrt_price <= 0:
            return {"source": "uniswap_v4_state_view", "current_state_verified": False}
        pool = self.store.v4_pool(pool_id)
        if not pool:
            return {}
        token = candidate["token_address"]
        anchor = pool["anchor_address"]
        token_decimals = self.rpc.erc20_decimals(token, block=quote_block)
        anchor_decimals = self.rpc.erc20_decimals(anchor, block=quote_block)
        total_supply = self.rpc.erc20_total_supply(token, block=quote_block)
        if str(anchor).lower() == USDG_ADDRESS.lower():
            anchor_usd, anchor_source = 1.0, "usdg_par_assumption"
        else:
            anchor_usd, anchor_source = self.anchors.wrapped_native_usd()
        if anchor_usd <= 0:
            return {"source": "uniswap_v4_state_view", "current_state_verified": False,
                    "reason": "anchor_usd_price_unavailable"}
        raw_ratio = (sqrt_price / (1 << 96)) ** 2
        human_ratio = raw_ratio * (10 ** (token_decimals-anchor_decimals) if pool["currency0"].lower()==token.lower() else 10 ** (anchor_decimals-token_decimals))
        if pool["currency0"].lower() == token.lower():
            token_usd = anchor_usd * human_ratio
        else:
            token_usd = anchor_usd / human_ratio if human_ratio else 0.0
        token_scale = 10 ** token_decimals
        anchor_scale = 10 ** anchor_decimals
        liquidity_usd = 2 * active_liquidity * (token_usd * anchor_usd) ** 0.5 / (token_scale * anchor_scale) ** 0.5
        supply = total_supply / token_scale
        quote = (
            self.execution_quote(
                pool,
                token=token,
                token_decimals=token_decimals,
                anchor_decimals=anchor_decimals,
                anchor_usd=anchor_usd,
                token_usd=token_usd,
                block=quote_block,
            )
            if include_execution_quote else None
        )
        paper_quantity = safe_float(candidate.get("paper_quantity"), 0.0)
        paper_exit = None
        if paper_quantity > 0:
            try:
                token_in = int(paper_quantity * token_scale)
                sell_zero_for_one = pool["currency0"].lower() == token.lower()
                anchor_out, gas_estimate = self._quote_exact_input_single(
                    pool,
                    zero_for_one=sell_zero_for_one,
                    exact_amount=token_in,
                    block=quote_block,
                )
                paper_exit = {
                    "verified": anchor_out > 0,
                    "token_in_raw": str(token_in),
                    "anchor_out_raw": str(anchor_out),
                    "value_usd": anchor_out / anchor_scale * anchor_usd,
                    "gas_estimate": gas_estimate,
                }
            except Exception as exc:
                paper_exit = {
                    "verified": False,
                    "reason": "v4_paper_exit_quote_failed",
                    "error": str(exc)[:500],
                }
        custody = self.store.latest_v4_custody(pool_id)
        return {
            "pool_id": pool_id, "source": "uniswap_v4_state_view",
            "price_usd": token_usd or None, "liquidity_usd": liquidity_usd or None,
            "market_cap_usd": token_usd*supply if token_usd and supply else None,
            "fdv_usd": token_usd*supply if token_usd and supply else None,
            "active_liquidity_raw": str(active_liquidity), "sqrt_price_x96": str(sqrt_price),
            "tick": tick, "anchor_price_source": anchor_source,
            "quote_block": quote_block,
            "current_state_verified": True,
            "liquidity_model": "active_concentrated_liquidity_estimate_not_quote",
            "anchor_price_usd": anchor_usd,
            "executable_quote_verified": bool(quote and quote.get("verified")),
            "execution_quote": quote,
            "paper_exit_quote_required": paper_quantity > 0,
            "paper_exit_quote_verified": bool(
                paper_exit and paper_exit.get("verified")
            ),
            "paper_exit_quantity": paper_quantity or None,
            "paper_exit_value_usd": (
                paper_exit.get("value_usd") if paper_exit else None
            ),
            "paper_exit_quote": paper_exit,
            "estimated_liquidity_usd": liquidity_usd or None,
            "pool_reserve_fraction": None,
            "singleton_token_balance_fraction": (
                self.singleton_balance_fraction(token, total_supply)
                if include_execution_quote else None
            ),
            "v4_custody": custody,
        }

def _market_from_report(report: dict, token: str, pair_address: str) -> dict:
    pairs=((report.get("data") or {}).get("dexscreener") or {}).get("pairs") or []
    matches=[]
    for pair in pairs:
        if str(pair.get("chainId") or "").lower()!="robinhood":
            continue
        base=str((pair.get("baseToken") or {}).get("address") or "").lower()
        if token.lower() != base:
            continue
        exact=str(pair.get("pairAddress") or "").lower()==pair_address.lower()
        matches.append((int(exact),pair))
    if not matches:
        return {}
    pair=max(matches,key=lambda item:(item[0],safe_float((item[1].get("liquidity") or {}).get("usd"),0.0)))[1]
    return {
        "pair_address":pair.get("pairAddress"),"dex_id":pair.get("dexId"),
        "price_usd":safe_float(pair.get("priceUsd"),0.0) or None,
        "liquidity_usd":safe_float((pair.get("liquidity") or {}).get("usd"),0.0) or None,
        "market_cap_usd":safe_float(pair.get("marketCap"),0.0) or None,
        "fdv_usd":safe_float(pair.get("fdv"),0.0) or None,
    }


class RobinhoodLearningEngine:
    def __init__(
        self, root: str | Path = DEFAULT_ROOT, *, rpc=None, analyzer=None,
        market=None, chain_root: str | Path | None = None,
        skill_root: str | Path | None = None,
        timechain_recorder: RobinhoodLearningTimechainRecorder | None = None,
    ):
        self.root=Path(root)
        self.root.mkdir(parents=True,exist_ok=True)
        self.rpc=rpc or RobinhoodRPC(ROBINHOOD_NETWORK.rpc_url)
        self.store=RobinhoodLearningStore(self.root/"learning.sqlite3")
        self.ledger=HashEventLedger(self.root/"events.jsonl")
        self.observer=RobinhoodPairObserver(self.rpc,self.root/"discovery_cursor.json")
        self.v4_observer=RobinhoodV4Observer(self.rpc,self.store,self.root/"discovery_v4_cursor.json")
        self.v4_custody=RobinhoodV4CustodyVerifier(
            self.rpc,self.store,self.root/"v4_custody_cursor.json"
        )
        self.market=market or RobinhoodMarketClient()
        self.v4_market=RobinhoodV4MarketClient(self.rpc,self.market,self.store)
        self.analyzer=analyzer
        self.timechain_recorder = timechain_recorder
        if self.timechain_recorder is None and chain_root is not None:
            self.timechain_recorder = RobinhoodLearningTimechainRecorder(
                chain_root, skill_root=skill_root or default_skill_root(),
            )

    def _analyzer(self):
        if self.analyzer is None:
            self.analyzer=Chainseer(
                rpc_url=ROBINHOOD_NETWORK.rpc_url,
                chain_root=str(Path(DEFAULT_CHAIN_ROOT)),
                network=ROBINHOOD_NETWORK,
            )
        return self.analyzer

    def seal_analysis_memory(
        self, candidate: dict, report: dict, market: dict,
        *, priority_reason: str | None,
    ) -> dict:
        """Seal one prospective learner analysis without touching the user lane."""
        if self.timechain_recorder is None:
            return {"status": "disabled"}
        if candidate.get("producer_analysis_ring_index") is not None:
            # A checkpoint row has one canonical analysis parent. Rechecks may
            # update the mutable market projection, but never move that parent
            # after the first producer ring was bound.
            return {
                "status": "already_bound",
                "ring": candidate.get("producer_analysis_ring_index"),
                "ring_hash": candidate.get("producer_analysis_ring_hash"),
            }
        ring = self.timechain_recorder.seal_analysis(
            candidate, report, market, priority_reason=priority_reason,
        )
        evidence_hash = (ring.get("payload") or {}).get("evidence_hash")
        self.store.record_analysis_producer_reference(
            candidate["token_address"], ring, evidence_hash,
        )
        return {
            "status": "sealed", "ring": ring.get("index"),
            "ring_hash": ring.get("ring_hash"),
            "evidence_hash": evidence_hash,
        }

    def resolve_flow_participants(
        self, limit: int = FLOW_ORIGIN_RESOLUTION_LIMIT,
        *, deadline_monotonic: float | None = None,
        head_block: int | None = None,
    ) -> dict:
        """Resolve origins, prospective cohort first.

        With head_block supplied, every transaction across the fresh pools'
        full windows is taken before any background work, so a near-head
        window is FINISHED rather than partly covered -- a window at 70%
        coverage fails the 0.80 gate exactly as completely as one at 0%.
        """
        limit = max(0, limit)
        historical_reserve = min(FLOW_HISTORICAL_ORIGIN_RESERVE, limit // 10)
        prospective_hashes: list[str] = []
        if head_block:
            # Two indexed lookups instead of a range join: 0.19s against the
            # 44.7s the prospective scope was costing, for the same answer.
            prospective_hashes = self.store.near_head_pending_origins(
                limit - historical_reserve, head_block=head_block,
            )
        active_hashes = [
            h for h in self.store.pending_transaction_origins(
                max(0, limit - historical_reserve - len(prospective_hashes)),
                scope="active",
            ) if h not in set(prospective_hashes)
        ]
        active_hashes = prospective_hashes + active_hashes
        # Give unused live-window capacity back to historical backfill. During
        # catch-up scans, however, old transactions can never crowd the active
        # signal window out of the bounded RPC budget.
        historical_hashes = self.store.pending_transaction_origins(
            limit - len(active_hashes), scope="historical"
        )
        hashes = active_hashes + historical_hashes
        resolved = unavailable = failures = affected_pools = attempted = 0
        deadline_stops = 0
        if hashes and not hasattr(self.rpc, "get_transactions"):
            return {
                "selected": len(hashes), "resolved": 0, "unavailable": 0,
                "failures": len(hashes), "affected_pools": 0,
                "selected_active": len(active_hashes),
                "selected_historical": len(historical_hashes),
                "pending_after": self.store.pending_transaction_origin_counts()["total"],
                "supported": False,
            }
        # The deadline was checked only BETWEEN batches, so a batch of 75
        # remote calls starting one second before the deadline still ran to
        # completion. Measured: a 60-second budget overran to 166.9 seconds,
        # and the resulting head drift was 1,676 blocks against a 120-block
        # freshness bound, expiring 9 otherwise-qualified windows in a cycle.
        #
        # A deadline is only honoured if the work permitted after the last
        # check fits inside it, so refuse to START a batch unless the time
        # remaining covers what a batch has actually been costing. The first
        # batch has no history and is admitted on the raw deadline.
        batch = self._resolve_origin_batches(hashes, deadline_monotonic)
        batch_seconds = batch["observed_batch_seconds"]
        deadline_stops = batch["stopped_at_deadline"]
        attempted = batch["attempted"]
        resolved = batch["resolved"]
        unavailable = batch["unavailable"]
        failures = batch["failures"]
        affected_pools = batch["affected_pools"]
        # Queue-depth census, not work. Measured at 85.1s against a 60-second
        # stage budget: the batch loop honoured its deadline and then this ran
        # regardless, so the stage came in at 268.4s. Diagnostics must not
        # outweigh the thing they describe -- skipped past the deadline, and
        # the skip is reported rather than silently returning zeros.
        if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
            pending = {"total": None, "active": None, "historical": None,
                       "census_skipped_past_deadline": True}
        else:
            pending = self.store.pending_transaction_origin_counts()
        attempted_active = min(attempted, len(active_hashes))
        attempted_historical = max(0, attempted - attempted_active)
        return {
            "planned": len(hashes), "selected": attempted,
            "deferred_for_deadline": len(hashes) - attempted,
            "stopped_at_deadline": deadline_stops,
            "observed_batch_seconds": round(batch_seconds, 3),
            "resolved": resolved,
            "unavailable": unavailable, "failures": failures,
            "affected_pools": affected_pools,
            "selected_active": attempted_active,
            "selected_historical": attempted_historical,
            "pending_after": pending["total"],
            "pending_active_after": pending["active"],
            "pending_historical_after": pending["historical"],
            "supported": True,
            "batch_size": FLOW_ORIGIN_BATCH_SIZE,
        }

    def _resolve_origin_batches(
        self, hashes: list[str], deadline_monotonic: float | None,
    ) -> dict:
        """Resolve origins for an explicit hash list, honouring the deadline.

        A deadline is only honoured if the work permitted after the last check
        fits inside it, so a batch is refused unless the time remaining covers
        what a batch has actually been costing. The first batch has no history
        and is admitted on the raw deadline.
        """
        resolved = unavailable = failures = affected_pools = attempted = 0
        deadline_stops = 0
        batch_seconds = 0.0
        for offset in range(0, len(hashes), FLOW_ORIGIN_BATCH_SIZE):
            if deadline_monotonic is not None:
                remaining = deadline_monotonic - time.monotonic()
                if remaining <= 0 or (batch_seconds and remaining < batch_seconds):
                    deadline_stops += 1
                    break
            chunk = hashes[offset:offset + FLOW_ORIGIN_BATCH_SIZE]
            attempted += len(chunk)
            batch_started = time.monotonic()
            try:
                records = _remote_call(
                    f"Robinhood transaction origins {offset + 1}-{offset + len(chunk)}",
                    lambda chunk=chunk: self.rpc.get_transactions(chunk),
                )
            except Exception:
                failures += len(chunk)
                continue
            # Exponential-ish tracking: react to a slow batch immediately,
            # decay back down as fast batches follow.
            observed = time.monotonic() - batch_started
            batch_seconds = max(observed, batch_seconds * 0.5)
            result = self.store.record_transaction_origins(records)
            resolved += result["resolved"]
            unavailable += result["unavailable"]
            affected_pools += result["affected_pools"]
        return {
            "attempted": attempted, "resolved": resolved,
            "unavailable": unavailable, "failures": failures,
            "affected_pools": affected_pools,
            "stopped_at_deadline": deadline_stops,
            "observed_batch_seconds": round(batch_seconds, 3),
        }

    def enrich_near_head_window(self, events: list[dict]) -> dict:
        """Resolve origins for the window ABOUT TO BE SEALED.

        Running this after the seal was measurably useless: the window moves
        5-9x its own width per cycle, so the enriched transactions were outside
        every subsequent window and no observation was ever sealed with the
        identity evidence that existed for it. record_transaction_origins
        refreshes the flow signal for each affected pool, so the recomputed
        participant counts are in place before the caller seals.
        """
        if not events:
            return {"supported": True, "candidates": 0, "attempted": 0,
                    "resolved": 0, "window_fully_enriched": True}
        if not hasattr(self.rpc, "get_transactions"):
            return {"supported": False, "candidates": 0, "attempted": 0,
                    "resolved": 0, "window_fully_enriched": False}
        candidates = self.store.unresolved_transaction_hashes(
            [str(event.get("transaction_hash") or "") for event in events]
        )
        # Truncation is recorded rather than hidden: a partly-enriched window
        # fails the coverage floor exactly as completely as an empty one, and
        # the seal must be able to say which it was.
        truncated = max(0, len(candidates) - FLOW_NEAR_HEAD_ENRICHMENT_LIMIT)
        selected = candidates[:FLOW_NEAR_HEAD_ENRICHMENT_LIMIT]
        deadline = time.monotonic() + FLOW_NEAR_HEAD_ENRICHMENT_BUDGET_SECONDS
        batch = self._resolve_origin_batches(selected, deadline)
        return {
            "supported": True,
            "candidates": len(candidates),
            "truncated": truncated,
            "attempted": batch["attempted"],
            "resolved": batch["resolved"],
            "unavailable": batch["unavailable"],
            "failures": batch["failures"],
            "affected_pools": batch["affected_pools"],
            "stopped_at_deadline": batch["stopped_at_deadline"],
            "window_fully_enriched": bool(
                not truncated and batch["attempted"] == len(selected)
                and not batch["failures"]
            ),
        }

    def near_head_flow_pass(self) -> dict:
        """Ingest the newest FLOW_WINDOW_BLOCKS so a signal can be fresh.

        The batch cycle scans a wide range and finishes thousands of blocks
        behind the chain, so every flow window it produced ended far outside
        FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS (120, roughly twelve seconds
        here). Measured before this existed: window end 37,904,751 against a
        head of 37,905,545 -- 794 blocks late, and no window in 653 was ever
        eligible. The bound was never the problem; nothing was ever computed
        near enough to the head to test it.

        So this runs LAST in the cycle and scans only the final window. It
        deliberately re-reads the head afterwards rather than reusing the head
        it started from: reporting lag against the same number used to build
        the window would make freshness self-certifying, which is the error
        the telemetry already had to be corrected for once.
        """
        started = time.monotonic()
        try:
            head = int(self.rpc.get_block_number())
        except Exception as error:
            return {"supported": False, "reason": str(error)[:200]}
        if head <= 0:
            return {"supported": False, "reason": "no_head_block"}
        # Scan only what is NEW. The window is computed from the database, not
        # from the scan, so re-fetching the whole 1,350 blocks every cycle was
        # re-downloading roughly 1,200 blocks of logs already ingested:
        # measured 1,643 logs seen against 9 swaps ingested, ~50 seconds, and
        # 487 blocks of drift against a 120-block decision bound. The cursor
        # floor is still the full window, so an interrupted or long-delayed
        # cycle re-scans normally rather than leaving a hole.
        window_floor = max(0, head - FLOW_WINDOW_BLOCKS + 1)
        cursor_path = self.root / "near_head_cursor.json"
        cursor = read_json(cursor_path, {}) or {}
        last_scanned = safe_int(cursor.get("last_scanned_block"), 0)
        from_block = window_floor
        incremental = False
        if last_scanned and last_scanned + 1 > window_floor:
            from_block = last_scanned + 1
            incremental = True
        if from_block > head:
            # No new blocks. The windows already in the database stand; there
            # is nothing to ingest and nothing to enrich.
            logs = []
        else:
            try:
                logs = self.rpc.get_logs(
                    from_block, head, address=UNISWAP_V4_POOL_MANAGER,
                    topics=[[V4_SWAP_TOPIC]],
                ) or []
            except Exception as error:
                # The cursor is NOT advanced on failure, so the blocks this
                # scan missed are picked up by the next one.
                return {
                    "supported": True, "scanned": False,
                    "from_block": from_block, "to_block": head,
                    "incremental": incremental, "reason": str(error)[:200],
                }
        # Register pools BORN in this window before filtering swaps by
        # membership. Batch discovery runs 477,270 blocks behind the head --
        # about 13 hours -- and 0 pools were known inside that gap, so a token
        # launched today had its swaps seen and discarded on every cycle until
        # the crawler eventually reached its Initialize block. Measured
        # signature: 1,967 logs seen against 11 swaps ingested, because 99.5%
        # of near-head trading happens in pools this learner had never heard
        # of. Two tokens the operator asked about were absent from every table
        # for exactly this reason -- never rejected, never analysed, never
        # seen.
        #
        # The Initialize log carries everything v4_pools needs, so the pass
        # can admit a pool on sight rather than waiting for a crawler that has
        # never caught up.
        known = self.store.known_v4_pool_ids()
        anchors = {WETH_ADDRESS.lower(), USDG_ADDRESS.lower()}
        born = []
        init_logs = []
        try:
            # No new blocks means no new pools; skip the round trip entirely.
            if from_block <= head:
                init_logs = self.rpc.get_logs(
                    from_block, head, address=UNISWAP_V4_POOL_MANAGER,
                    topics=[[V4_INITIALIZE_TOPIC]],
                ) or []
        except Exception:
            # A failed Initialize scan must not lose the swap pass; the window
            # simply stays as blind as it was before.
            init_logs = []
        for log in init_logs:
            topics = log.get("topics") or []
            if len(topics) < 4:
                continue
            pool_id = str(topics[1]).lower()
            if pool_id in known:
                continue
            currency0 = _topic_address(topics[2]).lower()
            currency1 = _topic_address(topics[3]).lower()
            # Exactly one side must be an anchor, same rule the crawler uses:
            # a pool with no anchor cannot be priced, and one with two is not
            # a token listing.
            if (currency0 in anchors) == (currency1 in anchors):
                continue
            born.append({
                "kind": "initialize", "pool_id": pool_id,
                "currency0": currency0, "currency1": currency1,
                "token_address": currency1 if currency0 in anchors else currency0,
                "anchor_address": currency0 if currency0 in anchors else currency1,
                "fee_tier": _data_word(log.get("data"), 0) & ((1 << 24) - 1),
                "tick_spacing": _signed_word(log.get("data"), 1, 24),
                "hooks_address": _data_address(log.get("data"), 2).lower(),
                "sqrt_price_x96": _data_word(log.get("data"), 3),
                "tick": _signed_word(log.get("data"), 4, 24),
                "block_number": int(str(log.get("blockNumber") or "0x0"), 16),
            })
        if born:
            self.store.apply_v4_events(born)
            known = self.store.known_v4_pool_ids()
        events = []
        for log in logs:
            topics = log.get("topics") or []
            if not topics:
                continue
            pool_id = str(topics[1]).lower() if len(topics) > 1 else ""
            if pool_id not in known:
                continue
            events.append({
                "kind": "swap", "pool_id": pool_id,
                "block_number": int(str(log.get("blockNumber") or "0x0"), 16),
                "block_timestamp": None,
                "transaction_hash": log.get("transactionHash") or "",
                "log_index": int(str(log.get("logIndex") or "0x0"), 16),
                "sender_hint": (
                    _topic_address(topics[2]).lower() if len(topics) > 2 else None
                ),
                "amount0_raw": _signed_word(log.get("data"), 0, 128),
                "amount1_raw": _signed_word(log.get("data"), 1, 128),
                "sqrt_price_x96": _data_word(log.get("data"), 2),
                "active_liquidity": _data_word(log.get("data"), 3),
                "tick": _signed_word(log.get("data"), 4, 24),
            })
        if events:
            self.store.apply_v4_events(events)
        # Enrich BETWEEN computing the window and sealing it. apply_v4_events
        # has just recomputed the flow signal with zero resolved participants;
        # record_transaction_origins recomputes it again with the real ones, so
        # the seal that follows carries the identity evidence that actually
        # existed at observation time.
        enrichment = self.enrich_near_head_window(events)
        atomic_json_write(cursor_path, {"last_scanned_block": int(head)})
        try:
            head_after = int(self.rpc.get_block_number())
        except Exception:
            head_after = head
        touched = sorted({event["pool_id"] for event in events})
        return {
            "supported": True, "scanned": True,
            "from_block": from_block, "to_block": head,
            "incremental": incremental,
            "scan_blocks": max(0, head - from_block + 1),
            "logs_seen": len(logs), "swaps_ingested": len(events),
            "initialize_logs_seen": len(init_logs),
            "pools_admitted_on_sight": len(born),
            "pools_touched": len(touched),
            # Attributed to THIS pass's pools, never read off the whole table.
            "window_coverage": self.store.flow_window_coverage(touched),
            "enrichment": enrichment,
            "head_block_after": head_after,
            "elapsed_blocks": max(0, head_after - head),
            "duration_seconds": round(time.monotonic() - started, 3),
            "within_prospective_bound": bool(
                head_after - head <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            ),
            "scope": "near_head_flow_window_v1",
        }

    def seal_near_head_observations(self, head_block: int, now: float) -> dict:
        """Seal every near-head window, pinned to the OBSERVATION head.

        head_block must be the head the window was built from, never a head
        re-read after the pass. Passing the later head compares a window
        against a chain position that did not exist when it was observed, and
        since the pass runs longer than its own window is wide, that rejects
        every window: measured windows_considered 0, sealed_this_cycle 0.

        Sealing takes a block-pinned quote at that observation head, so the
        claim carries the price it was actually made against -- not the price
        that survived 94 seconds of identity resolution. How stale the window
        had become by decision time is a separate question, recorded by
        classify_flow_observation, which is what gates paper eligibility.
        """
        # A window is observable while it OVERLAPS the near-head region, not
        # only when the pool traded in the last 120 blocks. window_end_block is
        # a pool's most recent swap, and these pools trade a few times per
        # 1,350 blocks -- the same mistake that made the origin-targeting
        # cohort 1 pool of 1,895 until FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS split
        # the two bounds apart. Whether the window is fresh enough to ACT on
        # remains the 120-block question, asked by classify_flow_observation.
        floor = int(head_block) - FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS
        sealed: list[str] = []
        failures = 0
        with self.store.connection() as connection:
            windows = [dict(row) for row in connection.execute(
                "SELECT * FROM flow_signals WHERE window_end_block >= ?", (floor,),
            )]
        for window in windows:
            with self.store.connection() as connection:
                hashes = [
                    row[0] for row in connection.execute(
                        """
                        SELECT transaction_hash FROM swap_observations
                        WHERE pool_id=? AND block_number BETWEEN ? AND ?
                        """,
                        (window["pool_id"], window["window_start_block"],
                         window["window_end_block"]),
                    )
                ]
            try:
                market = self.v4_market.snapshot(
                    {"pool_id": window["pool_id"],
                     "token_address": window["token_address"]},
                    quote_block=int(window["window_end_block"]),
                )
            except Exception:
                market = {"verified": False, "reason": "observation_quote_failed"}
                failures += 1
            try:
                features = json.loads(window.get("features_json") or "{}")
            except (TypeError, ValueError):
                features = {}
            gaps = features.get("qualification_gaps")
            gap_count = None if gaps is None else len(gaps)
            # An unexitable pool is not a signal, whatever its flow looked
            # like. The five qualification gates -- swaps, participants, net
            # flow, price direction, concentration -- say nothing about
            # whether the pool holds any money, so the first two windows ever
            # labelled `signal` were pools with $2.11 and $0.39 of liquidity,
            # 98.96% and 99.81% buy impact, and identical returns at 1m, 5m,
            # 15m and 1h of -0.9951 and -0.9995: pure friction, no price
            # movement at all.
            #
            # They qualified because the shadow-score bar was removed (their
            # scores were 21.9 and 24.6, both under the old 70). Removing it
            # was right -- the score is anti-predictive at p=0.0000 -- but it
            # had been excluding empty pools as a side effect, and that work
            # needs doing explicitly rather than by accident.
            #
            # This matters beyond the labels: signal_versus_control() needs 8
            # per arm, and a signal arm filled with empty pools would produce
            # a comparison that looks like a result.
            round_trip = self.store._round_trip_return(market)
            exitable = bool(
                round_trip is not None
                and round_trip >= -FLOW_MAXIMUM_ROUND_TRIP_LOSS
            )
            if gap_count == 0 and not exitable:
                gaps = list(gaps or []) + ["exitable_round_trip"]
                gap_count = len(gaps)
                features = dict(features)
                features["qualification_gaps"] = gaps
                features["round_trip_return"] = round_trip
            role = "signal" if gap_count == 0 else "matched_control"
            observation_id = self.store.seal_flow_observation(
                role=role, gap_count=gap_count,
                pool_id=window["pool_id"], token_address=window["token_address"],
                observation_head=int(head_block),
                window_start_block=int(window["window_start_block"]),
                window_end_block=int(window["window_end_block"]),
                transaction_hashes=hashes, features=features,
                quote=market, quote_block=int(window["window_end_block"]),
                now=now,
            )
            if observation_id:
                sealed.append(observation_id)
        with self.store.connection() as connection:
            cumulative = connection.execute(
                "SELECT COUNT(*) FROM flow_observations WHERE policy_version=?",
                (FLOW_EVIDENCE_POLICY_VERSION,),
            ).fetchone()[0]
        return {
            "windows_considered": len(windows),
            "sealed_this_cycle": len(sealed),
            "cumulative_observations": cumulative,
            "quote_failures": failures, "observation_head": int(head_block),
        }

    def classify_sealed_observations(self, decision_head: int) -> dict:
        """Classify sealed observations after enrichment. Never mutates them."""
        # Cumulative totals read as a rate unless the delta is stated beside
        # them: a backlog sweep classifying 392 observations while the cycle
        # sealed 6 was misread as 47 verified per cycle when the true figure
        # was 12% of a cumulative 392. Every count below is labelled.
        tiers: dict[str, int] = {}
        research = paper = 0
        newly_classified = 0
        with self.store.connection() as connection:
            already = {
                row[0] for row in connection.execute(
                    "SELECT observation_id FROM flow_observation_classifications"
                )
            }
        with self.store.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                """
                SELECT o.observation_id, o.pool_id, o.token_address,
                       fs.identity_coverage, fs.qualification_gaps_json
                FROM flow_observations o
                LEFT JOIN flow_signals fs ON fs.pool_id=o.pool_id
                WHERE o.policy_version=?
                """,
                (FLOW_EVIDENCE_POLICY_VERSION,),
            )]
        quotes_taken = quote_failures = 0
        for row in rows:
            try:
                gates = json.loads(row.get("qualification_gaps_json") or "[]")
            except (TypeError, ValueError):
                gates = []
            # Re-price ONLY a paper candidate. An observation with a failing
            # gate or unverified identity is research-only however the price
            # moved, so quoting it buys nothing and costs a round trip. With
            # zero qualified windows this spends zero calls, and it starts
            # spending exactly when there is something to spend it on.
            decision_quote = None
            candidate = bool(
                not gates
                and safe_float(row.get("identity_coverage"), 0.0)
                    >= FLOW_MINIMUM_IDENTITY_COVERAGE
            )
            if candidate and quotes_taken < FLOW_DECISION_QUOTE_LIMIT:
                try:
                    decision_quote = self.v4_market.snapshot(
                        {"pool_id": row["pool_id"],
                         "token_address": row["token_address"]},
                        quote_block=int(decision_head),
                    )
                    quotes_taken += 1
                except Exception:
                    # A failed re-quote is not a pass. classify treats None as
                    # unactionable, which keeps the observation research-only.
                    quote_failures += 1
            verdict = self.store.classify_flow_observation(
                row["observation_id"], decision_head=int(decision_head),
                identity_coverage=row.get("identity_coverage"), gates=gates,
                decision_quote=decision_quote,
            )
            tiers[verdict["identity_tier"]] = tiers.get(verdict["identity_tier"], 0) + 1
            research += verdict["research_eligible"]
            paper += verdict["paper_eligible"]
            newly_classified += row["observation_id"] not in already
        verified = tiers.get("verified", 0)
        return {
            "classified_this_cycle": newly_classified,
            "reclassified_this_cycle": len(rows) - newly_classified,
            "decision_quotes_taken": quotes_taken,
            "decision_quote_failures": quote_failures,
            "decision_quote_limit": FLOW_DECISION_QUOTE_LIMIT,
            "cumulative_classified": len(rows),
            "cumulative_identity_tiers": tiers,
            "cumulative_verified_fraction": (
                round(verified / len(rows), 4) if rows else None
            ),
            "cumulative_research_eligible": research,
            "cumulative_paper_eligible": paper,
            "decision_head": int(decision_head),
            "note": (
                "counts prefixed cumulative_ are totals over the whole cohort, "
                "not a per-cycle rate; use classified_this_cycle for rate"
            ),
        }

    def capture_flow_evidence(
        self, head_block: int, now: float, *, ingest_head_block: int | None = None,
    ) -> dict:
        capture = self.store.capture_flow_signal_events(
            head_block, now, ingest_head_block=ingest_head_block,
        )
        quoted = 0
        quote_failures = 0
        for event in self.store.pending_flow_quotes():
            candidate = {
                "pool_id": event["pool_id"],
                "token_address": event["token_address"],
            }
            try:
                market = self.v4_market.snapshot(
                    candidate, quote_block=int(event["window_end_block"]),
                )
            except Exception as exc:
                market = {
                    "current_state_verified": False,
                    "execution_quote": {
                        "verified": False, "reason": "signal_quote_failed",
                        "error": str(exc)[:500],
                    },
                }
            self.store.record_flow_entry_quote(
                event["event_id"], market, int(event["window_end_block"]),
            )
            if (market.get("execution_quote") or {}).get("verified"):
                quoted += 1
            else:
                quote_failures += 1
        capture.update({"entry_quotes_verified": quoted, "quote_failures": quote_failures})
        return capture

    def observe_flow_observation_outcomes(
        self, now: float, limit: int = 40,
    ) -> dict:
        """Resolve due observation horizons against a block-pinned exit quote.

        This stage was built, tested and then never called, so 5,125 horizons
        sat pending across nine hours and the cohort could not reach its first
        reflection. An unresolvable exit is recorded as a total loss rather
        than skipped -- skipping would quietly remove the worst outcomes from
        the sample.
        """
        # v4_market.snapshot() only produces a paper_exit_quote when the
        # candidate carries paper_quantity -- omitting it returned a bare
        # market dict with no "verified" key, so exit_valid was always False
        # and forty pilot outcomes were written as fabricated total losses.
        # The quantity comes from the sealed entry quote, so the exit is
        # priced for the position the observation actually claimed.
        due_rows = self.store.due_flow_observation_outcomes(now, limit)
        resolved = non_exitable = failures = 0
        for due in due_rows:
            try:
                head = int(self.rpc.get_block_number())
            except Exception:
                head = int(due.get("window_end_block") or 0)
            try:
                entry = json.loads(due.get("quote_json") or "{}")
            except (TypeError, ValueError):
                entry = {}
            entry_quote = dict(entry.get("execution_quote") or {})
            decimals = safe_int(entry_quote.get("token_decimals"), 18)
            token_out_raw = safe_int(entry_quote.get("token_out_raw"), 0)
            try:
                market = self.v4_market.snapshot(
                    {
                        "pool_id": due["pool_id"],
                        "token_address": due["token_address"],
                        "paper_quantity": token_out_raw / (10 ** decimals),
                    },
                    quote_block=head,
                )
                exit_quote = dict(market.get("paper_exit_quote") or {})
            except Exception as exc:
                exit_quote = {
                    "verified": False, "reason": "exit_quote_failed",
                    "error": str(exc)[:200],
                }
                failures += 1
            try:
                result = self.store.record_flow_observation_outcome(
                    due, exit_quote, quote_block=head, now=now,
                )
            except Exception:
                failures += 1
                continue
            resolved += result["status"] == "resolved"
            non_exitable += result["status"] == "non_exitable"
        return {
            "due": len(due_rows), "resolved_this_cycle": resolved,
            "non_exitable_this_cycle": non_exitable, "failures": failures,
            "limit": limit,
        }

    def observe_flow_evidence_outcomes(
        self, now: float, head_block: int, limit: int = 30,
    ) -> dict:
        due_rows = self.store.due_flow_outcomes(now, limit)
        observed = 0
        non_exitable = 0
        for due in due_rows:
            entry = json.loads(due.get("entry_quote_json") or "{}")
            entry_quote = dict(entry.get("market", {}).get("execution_quote") or {})
            token_decimals = safe_int(entry_quote.get("token_decimals"), 18)
            token_out_raw = safe_int(entry_quote.get("token_out_raw"), 0)
            candidate = {
                "pool_id": due["pool_id"],
                "token_address": due["token_address"],
                "paper_quantity": token_out_raw / (10 ** token_decimals),
            }
            try:
                market = self.v4_market.snapshot(
                    candidate, quote_block=int(head_block),
                )
            except Exception as exc:
                market = {
                    "current_state_verified": False,
                    "paper_exit_quote": {
                        "verified": False, "reason": "checkpoint_quote_failed",
                        "error": str(exc)[:500],
                    },
                }
            self.store.record_flow_outcome(due, market, int(head_block), now)
            observed += 1
            non_exitable += not bool(
                (market.get("paper_exit_quote") or {}).get("verified")
            )
        return {
            "selected": len(due_rows), "observed": observed,
            "non_exitable": non_exitable,
        }

    def _best_executable_market(self, candidate: dict) -> dict:
        """Find the most liquid safely identifiable venue for a watched token."""
        choices = []
        for market in self.market.snapshots(candidate["token_address"]):
            version = market.get("source_version")
            if version == SOURCE_V4:
                pool = self.store.v4_pool(str(market.get("pair_address") or ""))
                # Unknown V4 pools cannot be assumed hook-free.
                if not pool or str(pool.get("hooks_address") or ZERO_ADDRESS).lower() != ZERO_ADDRESS:
                    continue
            if (
                safe_float(market.get("price_usd"), 0.0) > 0
                and safe_float(market.get("liquidity_usd"), 0.0)
                    >= MINIMUM_ENTRY_LIQUIDITY_USD
            ):
                choices.append(market)
        if not choices:
            return {}
        return max(
            choices,
            key=lambda market: safe_float(market.get("liquidity_usd"), 0.0),
        )

    def recheck_executable_markets(self, now: float, limit: int) -> dict:
        checked = executable = entries = failures = 0
        memory_sealed = memory_failures = 0
        for candidate in self.store.pending_market_watches(now, limit):
            checked += 1
            try:
                market = self._best_executable_market(candidate)
                if not market:
                    self.store.record_market_watch_check(candidate["token_address"])
                    continue
                market_cap = safe_float(market.get("market_cap_usd"), 0.0)
                price = safe_float(market.get("price_usd"), 0.0)
                if market_cap <= 0 or market_cap > MAXIMUM_ENTRY_MARKET_CAP_USD:
                    self.store.record_market_watch_check(
                        candidate["token_address"], market,
                        reason="above_entry_market_cap_ceiling",
                        update_reference=True, decision=PAPER_DECISION_ABOVE_CAP,
                    )
                    continue
                if candidate.get("paper_decision") in {
                    PAPER_DECISION_ABOVE_CAP, PAPER_DECISION_REENTRY,
                }:
                    reference_price = safe_float(
                        candidate.get("market_watch_reference_price_usd"), 0.0
                    )
                    reference_cap = safe_float(
                        candidate.get("market_watch_reference_market_cap_usd"), 0.0
                    )
                    if (
                        reference_cap > MAXIMUM_ENTRY_MARKET_CAP_USD
                        or reference_price <= 0
                        or price < reference_price * REENTRY_MOMENTUM_MULTIPLE
                    ):
                        self.store.record_market_watch_check(
                            candidate["token_address"], market,
                            reason="waiting_for_reentry_momentum",
                            update_reference=True, decision=PAPER_DECISION_REENTRY,
                        )
                        continue
                self.store.record_market_watch_check(
                    candidate["token_address"], market,
                    reason="executable_market_found",
                )
                executable += 1
                self.store.select_execution_pool(candidate["token_address"], market)
                current = self.store.candidate(candidate["token_address"])
                report = self._analyzer().analyze_token(
                    current["token_address"], seal=False, defer_cognition=True,
                )
                if report.get("error"):
                    raise RuntimeError(report["error"])
                analysis = dict(report.get("analysis") or {})
                stops = list(analysis.get("hard_stop_overrides") or [])
                if current.get("source_version") == SOURCE_V4:
                    pool = self.store.v4_pool(current.get("pool_id") or "")
                    if not pool or str(pool.get("hooks_address") or ZERO_ADDRESS).lower() != ZERO_ADDRESS:
                        stops.append({
                            "code": "V4_HOOK_UNAUDITED", "severity": "High",
                            "reason": "The selected V4 pool hook could not be verified as absent",
                            "action": "AVOID",
                        })
                analysis["hard_stop_overrides"] = stops
                self.store.record_analysis(
                    current["token_address"], analysis, market,
                    priority_reason="executable_market_recheck",
                    queue_age_seconds=max(
                        0.0, now - safe_float(current.get("market_watch_started_at"), now)
                    ),
                    report_data=report.get("data") or {},
                )
                try:
                    memory = self.seal_analysis_memory(
                        self.store.candidate(current["token_address"]),
                        {**report, "analysis": analysis}, market,
                        priority_reason="executable_market_recheck",
                    )
                    memory_sealed += memory.get("status") == "sealed"
                except Exception:
                    # The durable SQLite analysis remains usable and the next
                    # market recheck can retry the idempotent producer seal.
                    memory_failures += 1
                latest = self.store.candidate(current["token_address"])
                if self.store.open_position(latest, market):
                    entries += 1
                    self.ledger.append("robinhood_paper_buy", {
                        "token_address": current["token_address"],
                        "symbol": latest.get("symbol"),
                        "score": latest.get("score"),
                        "price_usd": market.get("price_usd"),
                        "liquidity_usd": market.get("liquidity_usd"),
                        "market_cap_usd": market.get("market_cap_usd"),
                        "entry_trigger": "future_executable_market_recheck",
                        "paper_only": True,
                    })
            except Exception:
                failures += 1
        return {
            "checked": checked, "executable_markets_found": executable,
            "paper_entries": entries, "failures": failures, "limit": limit,
            "producer_analyses_sealed": memory_sealed,
            "producer_failures": memory_failures,
        }

    def observe_outcomes(
        self, now: float, limit: int,
        recovery_limit: int = DEFAULT_OUTCOME_RECOVERY_LIMIT,
        *, deadline_monotonic: float | None = None,
    ) -> dict:
        started = time.monotonic()
        observation_block = None
        if self.timechain_recorder is not None:
            try:
                observation_block = int(self.rpc.get_block_number())
            except Exception:
                # The checkpoint remains in SQLite, but without an immutable
                # observation pin it must not be promoted to training memory.
                observation_block = None
        expiration_started = time.monotonic()
        expired = self.store.expire_missed(now)
        expiration_seconds = time.monotonic() - expiration_started
        selection_started = time.monotonic()
        due_outcomes = self.store.due_outcomes(now, limit, recovery_limit)
        selection_seconds = time.monotonic() - selection_started
        batch_started = time.monotonic()
        standard_due = [
            due for due in due_outcomes
            if due.get("source_version") != SOURCE_V4
        ]
        batch_markets: dict[str, dict] = {}
        batch_failed = False
        market_provider_telemetry = {
            "schema_version": 1,
            "purpose": "outcome_observation_only",
            "providers": {},
            "fallback_selected": 0,
            "confirmed_no_market": 0,
            "provider_unavailable": 0,
            "final_winners": {},
        }
        if due_outcomes and hasattr(self.market, "outcome_snapshots_many"):
            try:
                batch_markets, market_provider_telemetry = (
                    self.market.outcome_snapshots_many(due_outcomes)
                )
                batch_failed = (
                    (market_provider_telemetry.get("providers") or {})
                    .get("dexscreener", {}).get("state") == "failed"
                )
            except Exception as exc:
                batch_failed = True
                batch_markets = {}
                market_provider_telemetry["providers"]["resolver"] = {
                    "state": "failed",
                    "error": RobinhoodMarketClient._provider_error(exc),
                }
        elif standard_due:
            try:
                grouped = self.market.snapshots_many([
                    due["token_address"] for due in standard_due
                ])
                for due in standard_due:
                    token = due["token_address"].lower()
                    batch_markets[token] = self.market.select_snapshot(
                        grouped.get(token, []), due.get("pair_address")
                    )
            except Exception:
                # Preserve the independent retry behavior for injected/legacy
                # clients which do not implement the outcome resolver.
                batch_failed = True
                batch_markets = {}
        batch_seconds = time.monotonic() - batch_started
        observed=no_market=failures=marks=deferred=0
        producer_outcomes_sealed = producer_legacy_unbound = producer_failures = 0
        fresh_observed = recovery_observed = 0
        fresh_selected = sum(row.get("outcome_queue") == "fresh" for row in due_outcomes)
        recovery_selected = len(due_outcomes) - fresh_selected
        for index, due in enumerate(due_outcomes):
            if deadline_monotonic is not None and time.monotonic() >= deadline_monotonic:
                deferred = len(due_outcomes) - index
                break
            v4_state_responded = False
            try:
                if due.get("source_version") == SOURCE_V4:
                    v4_telemetry = market_provider_telemetry["providers"].setdefault(
                        "uniswap_v4_state_view",
                        {"state": "attempted", "attempted": 0, "matched": 0,
                         "insufficient": 0, "failed": 0},
                    )
                    v4_telemetry["attempted"] += 1
                    v4_market = self.v4_market.snapshot(
                        due, include_execution_quote=False,
                        quote_block=observation_block,
                    )
                    v4_state_responded = True
                    if v4_telemetry.get("failed"):
                        v4_telemetry["state"] = "partial_failure"
                    else:
                        v4_telemetry["state"] = "responded"
                    provider_market = batch_markets.get(
                        due["token_address"].lower(), {}
                    )
                    if RobinhoodMarketClient.outcome_value_available(v4_market):
                        v4_telemetry["matched"] += 1
                        market = dict(v4_market)
                        resolution = dict(
                            provider_market.get("market_resolution") or {}
                        )
                        resolution.update({
                            "winner": "uniswap_v4_state_view",
                            "v4_state": "responded",
                            "fallback_used": False,
                            "retryable_provider_failure": False,
                        })
                        market["market_resolution"] = resolution
                    elif RobinhoodMarketClient.outcome_value_available(
                        provider_market
                    ):
                        v4_telemetry["insufficient"] += 1
                        market = dict(provider_market)
                        market["market_resolution"]["v4_state"] = (
                            "responded_without_outcome_value"
                        )
                    else:
                        v4_telemetry["insufficient"] += 1
                        market = dict(v4_market or provider_market or {})
                        resolution = dict(
                            provider_market.get("market_resolution") or {}
                        )
                        resolution.update({
                            "v4_state": "responded_without_outcome_value",
                            "retryable_provider_failure": False,
                        })
                        market["market_resolution"] = resolution
                elif due["token_address"].lower() in batch_markets:
                    market = batch_markets.get(due["token_address"].lower(), {})
                else:
                    market = self.market.snapshot(
                        due["token_address"], due["pair_address"]
                    )
            except Exception as exc:
                if due.get("source_version") == SOURCE_V4:
                    v4_telemetry = market_provider_telemetry["providers"].setdefault(
                        "uniswap_v4_state_view",
                        {"state": "attempted", "attempted": 1, "matched": 0,
                         "insufficient": 0, "failed": 0},
                    )
                    v4_telemetry["failed"] += 1
                    v4_telemetry["state"] = (
                        "failed"
                        if v4_telemetry["failed"] == v4_telemetry["attempted"]
                        else "partial_failure"
                    )
                    v4_telemetry["last_error"] = (
                        RobinhoodMarketClient._provider_error(exc)
                    )
                    provider_market = batch_markets.get(
                        due["token_address"].lower(), {}
                    )
                    market = dict(provider_market)
                    resolution = dict(market.get("market_resolution") or {})
                    resolution.update({
                        "v4_state": "failed",
                        "v4_error": RobinhoodMarketClient._provider_error(exc),
                        "retryable_provider_failure": bool(
                            resolution.get("retryable_provider_failure", True)
                        ),
                    })
                    market["market_resolution"] = resolution
                else:
                    failures+=1
                    self.store.outcome_failure(due["token_address"],now)
                    continue
            resolution = market.get("market_resolution") or {}
            retryable_failure = bool(
                resolution.get("retryable_provider_failure")
                and not v4_state_responded
            )
            if retryable_failure:
                failures += 1
                self.store.outcome_failure(due["token_address"], now)
                continue
            checkpoint = self.store.record_outcome(due,market,now)
            winner = str(
                (market.get("market_resolution") or {}).get("winner")
                or market.get("source") or "none"
            )
            winners = market_provider_telemetry.setdefault("final_winners", {})
            winners[winner] = int(winners.get(winner, 0)) + 1
            if self.timechain_recorder is not None:
                try:
                    candidate = self.store.candidate(due["token_address"])
                    ring = self.timechain_recorder.seal_checkpoint_outcome(
                        candidate or due, checkpoint, market,
                        observation_block=observation_block,
                    )
                    if ring is None:
                        # Expected for rows analyzed before producer-ring
                        # binding was introduced; never relabel legacy data.
                        producer_legacy_unbound += 1
                    else:
                        record_hash = canonical_hash(
                            (ring.get("payload") or {}).get("outcome_record") or {}
                        )
                        self.store.record_outcome_producer_reference(
                            due["token_address"], due["horizon_label"],
                            ring, record_hash,
                        )
                        producer_outcomes_sealed += 1
                except Exception:
                    producer_failures += 1
            observed+=1
            if due.get("outcome_queue") == "recovery":
                recovery_observed += 1
            else:
                fresh_observed += 1
            no_market += checkpoint.get("status") != "observed"
        return {
            "observed": observed, "fresh_observed": fresh_observed,
            "recovery_observed": recovery_observed,
            "fresh_selected": fresh_selected,
            "recovery_selected": recovery_selected,
            "deferred_for_deadline": deferred,
            "no_market": no_market, "failures": failures,
            "expired_missed": expired,
            "missed_backlog_remaining": self.store.missed_backlog(now),
            "missed_expiration_limit": MISSED_EXPIRATION_LIMIT,
            "position_marks": marks, "limit": limit,
            "recovery_limit": recovery_limit,
            "duration_seconds": round(time.monotonic() - started, 3),
            "expiration_duration_seconds": round(expiration_seconds, 3),
            "selection_duration_seconds": round(selection_seconds, 3),
            "market_batch_duration_seconds": round(batch_seconds, 3),
            "market_batch_candidates": len(standard_due),
            "market_batch_requests": (len(standard_due) + 29) // 30,
            "market_batch_failed": batch_failed,
            "market_provider_telemetry": market_provider_telemetry,
            "producer_outcomes_sealed": producer_outcomes_sealed,
            "producer_outcomes_legacy_unbound": producer_legacy_unbound,
            "producer_failures": producer_failures,
            "producer_observation_block": observation_block,
        }

    def evaluate_open_positions(self, now: float) -> dict:
        """Evaluate every open paper position independently of outcome horizons."""
        with self.store.connection() as connection:
            candidates = [dict(row) for row in connection.execute(
                """
                SELECT c.*,p.quantity paper_quantity
                FROM candidates c JOIN positions p USING(token_address)
                WHERE p.status='open' ORDER BY p.opened_at
                """
            )]
        checked = marked = unverified = closed = partial_exits = failures = 0
        for candidate in candidates:
            checked += 1
            try:
                market = (
                    self.v4_market.snapshot(candidate)
                    if candidate.get("source_version") == SOURCE_V4
                    else self.market.snapshot(
                        candidate["token_address"], candidate["pair_address"]
                    )
                )
                mark = self.store.mark_position(
                    candidate["token_address"], market, now
                )
                if not mark:
                    continue
                if not mark.get("verified", True):
                    unverified += 1
                    self.ledger.append("robinhood_paper_mark_unverified", mark)
                    continue
                marked += 1
                closed += bool(mark.get("reason"))
                partial_exits += len(mark.get("partial_exits") or [])
                self.ledger.append("robinhood_paper_mark", mark)
            except Exception:
                failures += 1
        return {
            "checked": checked, "marked": marked, "unverified": unverified,
            "closed": closed,
            "partial_exits": partial_exits, "failures": failures,
            "cadence": "every_learning_cycle",
        }

    def run_once(self, *, discovery_block_limit=DEFAULT_DISCOVERY_BLOCK_LIMIT,
                 analysis_limit=DEFAULT_ANALYSIS_LIMIT,outcome_limit=DEFAULT_OUTCOME_LIMIT,
                 outcome_recovery_limit=DEFAULT_OUTCOME_RECOVERY_LIMIT,
                 market_recheck_limit=DEFAULT_MARKET_RECHECK_LIMIT,
                 cycle_budget_seconds=DEFAULT_CYCLE_BUDGET_SECONDS,
                 lookback=DEFAULT_DISCOVERY_LOOKBACK_BLOCKS,now:float|None=None) -> dict:
        started = time.monotonic()
        now = time.time() if now is None else now
        with LearningRunLock(self.root / ".learn_once.lock"):
            with self.store.connection() as connection:
                run_id = connection.execute(
                    "INSERT INTO runs(started_at,status) VALUES (?,'running')",
                    (_utc_now(),),
                ).lastrowid
            try:
                stage_timings = {}
                stage_started = time.monotonic()
                position_evaluations = self.evaluate_open_positions(now)
                stage_timings["position_evaluations"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                outcome_deadline = min(
                    started + float(cycle_budget_seconds),
                    stage_started + OUTCOME_STAGE_BUDGET_SECONDS,
                )
                outcomes = self.observe_outcomes(
                    now, outcome_limit, outcome_recovery_limit,
                    deadline_monotonic=outcome_deadline,
                )
                stage_timings["outcomes"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                discovered, coverage = self.observer.sync(
                    block_limit=discovery_block_limit, lookback=lookback
                )
                stage_timings["uniswap_v2_discovery"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                # The crawler advanced ONE chunk per cycle -- at most 5,000
                # blocks -- while the chain produces 3,000-9,000 in the same
                # span, so it tied at best and lost at worst: measured drifting
                # from 493,004 to 500,034 blocks behind in a quarter of an
                # hour, and it had never once reported caught_up.
                #
                # Nothing about backfill needs to be one chunk. It runs in a
                # loop now until it catches the head or its budget expires,
                # which lets it burn through the backlog over a few cycles
                # instead of receding forever. The budget is wall-clock rather
                # than a chunk count because the constraint is RPC latency,
                # not block arithmetic -- 429s are frequent on this endpoint.
                v4_discovered, v4_coverage = [], {}
                catchup_deadline = (
                    time.monotonic() + FLOW_DISCOVERY_CATCHUP_SECONDS
                )
                catchup_passes = 0
                while True:
                    chunk, v4_coverage = self.v4_observer.sync(
                        block_limit=discovery_block_limit, lookback=lookback
                    )
                    v4_discovered.extend(chunk)
                    catchup_passes += 1
                    if v4_coverage.get("caught_up"):
                        break
                    if catchup_passes >= FLOW_DISCOVERY_MAXIMUM_PASSES:
                        break
                    if time.monotonic() >= catchup_deadline:
                        break
                v4_coverage = dict(v4_coverage)
                v4_coverage["catchup_passes"] = catchup_passes
                stage_timings["uniswap_v4_discovery"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                # The newest window must be ingested BEFORE identity
                # resolution, or its swaps reach qualification with no resolved
                # participants and fail minimum_identity_coverage -- measured
                # at 30 of 31 fresh windows when this ran in the other order.
                near_head_flow = self.near_head_flow_pass()
                # Seal against to_block -- the head the window was actually
                # built from -- not head_block_after. The pass takes 703
                # seconds to scan its 1,350-block window and the chain moves
                # 7,012 blocks meanwhile, so measuring the window's freshness
                # against the LATER head put every window 7,012 blocks past a
                # 120-block floor: windows_considered 0, sealed_this_cycle 0.
                # The pass enriched windows to full identity coverage and then
                # threw them away, which is why the cohort sat at 12.
                #
                # head_block_after keeps doing the job it was added for --
                # measuring drift so freshness cannot certify itself -- and
                # classify_flow_observation still refuses paper eligibility on
                # that drift. Sealing against the later head never made an
                # observation more honest; it made it nonexistent.
                observation_seal = self.seal_near_head_observations(
                    int(near_head_flow.get("to_block") or 0), now,
                ) if near_head_flow.get("to_block") else {}
                observation_seal["decision_head_lag_blocks"] = max(
                    0,
                    int(near_head_flow.get("head_block_after") or 0)
                    - int(near_head_flow.get("to_block") or 0),
                )
                # The richest diagnostic in the cycle existed only as a return
                # value and was discarded, so every question about whether the
                # pass ran, what it enriched, and which windows the resulting
                # coverage belonged to had to be inferred from database side
                # effects -- which is how a batch-path figure came to be read
                # as a near-head result. Seal it instead.
                self.ledger.append("robinhood_near_head_flow", {
                    **{key: value for key, value in near_head_flow.items()
                       if key != "enrichment"},
                    "enrichment": near_head_flow.get("enrichment") or {},
                    "observation_seal": observation_seal,
                    "population": "near_head_pass",
                })
                stage_timings["near_head_flow"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                identity_deadline = min(
                    started + float(cycle_budget_seconds)
                        - ANALYSIS_START_RESERVE_SECONDS,
                    stage_started + FLOW_IDENTITY_STAGE_BUDGET_SECONDS,
                )
                identity_resolution = self.resolve_flow_participants(
                    deadline_monotonic=identity_deadline,
                    head_block=int(
                        near_head_flow.get("head_block_after") or 0
                    ) or None,
                )
                # 65.1s of census on top of an already-spent budget. Same
                # rule: report the skip instead of paying for the number.
                identity_resolution["queues"] = (
                    self.store.prospective_enrichment_counts(
                        int(near_head_flow.get("head_block_after") or 0)
                    )
                    if near_head_flow.get("head_block_after")
                    and time.monotonic() < identity_deadline
                    else {"census_skipped_past_deadline":
                          bool(near_head_flow.get("head_block_after"))}
                )
                stage_timings["flow_identity_resolution"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                new_candidates = self.store.add_candidates(discovered + v4_discovered)
                self.store.mark_v4_promoted(
                    [row["pool_id"] for row in v4_discovered]
                )
                stage_timings["candidate_persistence"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                flow_head_block = int(
                    near_head_flow.get("head_block_after")
                    or v4_coverage.get("latest_block")
                    or v4_coverage.get("to_block") or 0
                )
                # Measure the observation-to-decision gap before designing
                # around it. The 120-block bound is ~12s at ~10 blocks/second
                # while the identity stage MAY take 60s, but a ceiling is not
                # consumption -- this records what each cycle actually spends,
                # so the risk becomes an observation.
                head_at_capture = flow_head_block
                try:
                    head_at_capture = int(self.rpc.get_block_number())
                except Exception:
                    pass
                ingest_head = int(near_head_flow.get("head_block_after") or 0)
                decision_drift = max(0, head_at_capture - ingest_head) if ingest_head else None
                # Classify against the head as it is AT THE DECISION, not the
                # head at ingest. Passing the ingest head made lag the ingest
                # lag, so every window inside the bound when observed was
                # labelled fresh and paper-eligible even after 1,600 blocks of
                # drift -- the stale arm could never populate and decision-
                # stale signals were marked tradeable.
                flow_evidence_capture = self.capture_flow_evidence(
                    head_at_capture, now, ingest_head_block=ingest_head or None,
                )
                self.store.pair_matched_controls()
                observation_classification = self.classify_sealed_observations(
                    head_at_capture
                )
                flow_evidence_capture["observations"] = {
                    "sealed": observation_seal,
                    "classified": observation_classification,
                }
                flow_evidence_capture["freshness_timing"] = {
                    "head_at_ingest": ingest_head or None,
                    "head_at_capture": head_at_capture,
                    "decision_head_lag_blocks": decision_drift,
                    "prospective_bound_blocks":
                        FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
                    "decision_within_bound": (
                        None if decision_drift is None
                        else decision_drift <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
                    ),
                    "identity_stage_seconds":
                        stage_timings.get("flow_identity_resolution"),
                    "near_head_stage_seconds": stage_timings.get("near_head_flow"),
                    # Windows that were inside the bound when observed but
                    # outside it by the time the decision ran. These are the
                    # signals the current ordering loses.
                    "windows_expired_during_processing": sum(
                        1 for row in self.store.flow_window_history(limit=200)
                        if ingest_head
                        and ingest_head - int(row["window_end_block"])
                            <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
                        and head_at_capture - int(row["window_end_block"])
                            > FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
                    ),
                }
                flow_evidence_capture["near_head_pass"] = near_head_flow
                flow_evidence_outcomes = self.observe_flow_evidence_outcomes(
                    now, flow_head_block
                )
                observation_outcomes = self.observe_flow_observation_outcomes(now)

                stage_timings["flow_evidence"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                market_rechecks = self.recheck_executable_markets(
                    now, market_recheck_limit
                )
                stage_timings["market_rechecks"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                analyses = analysis_failures = entries = momentum_analyses = 0
                producer_analyses_sealed = producer_failures = 0
                flow_shadow_analyses = 0
                analyses_deferred_for_deadline = 0
                queue_ages = []
                for candidate in self.store.pending_analysis(analysis_limit):
                    remaining_budget = (
                        float(cycle_budget_seconds) - (time.monotonic() - started)
                    )
                    if remaining_budget < ANALYSIS_START_RESERVE_SECONDS:
                        analyses_deferred_for_deadline += 1
                        continue
                    try:
                        priority_reason = (
                            "flow_shadow_priority"
                            if candidate.get("flow_shadow_qualified") else (
                                "liquid_momentum"
                                if candidate.get("momentum_priority_multiple") is not None
                                else "oldest_fairness"
                            )
                        )
                        discovered_at = _timestamp(candidate.get("discovered_at"))
                        queue_age = (
                            max(0.0, time.time() - discovered_at)
                            if discovered_at is not None else None
                        )
                        report = self._analyzer().analyze_token(
                            candidate["token_address"],
                            seal=False,
                            defer_cognition=True,
                        )
                        if report.get("error"):
                            raise RuntimeError(report["error"])
                        analysis = report.get("analysis") or {}
                        if candidate.get("source_version") == SOURCE_V4:
                            market = self.v4_market.snapshot(candidate)
                            stops = list(analysis.get("hard_stop_overrides") or [])
                            if str(candidate.get("hooks_address") or ZERO_ADDRESS).lower() != ZERO_ADDRESS:
                                stops.append({
                                    "code": "V4_HOOK_UNAUDITED", "severity": "High",
                                    "reason": "The V4 pool uses an unaudited hook", "action": "AVOID",
                                })
                            if not market.get("current_state_verified"):
                                stops.append({
                                    "code": "V4_MARKET_STATE_UNVERIFIED", "severity": "High",
                                    "reason": "Current V4 liquidity and price could not be verified", "action": "AVOID",
                                })
                            analysis = dict(analysis)
                            analysis["hard_stop_overrides"] = stops
                            analysis["v4_market_evidence"] = {
                                "pool_id": candidate.get("pool_id"),
                                "fee_tier": candidate.get("fee_tier"),
                                "tick_spacing": candidate.get("tick_spacing"),
                                "hooks_address": candidate.get("hooks_address"),
                                "activation": "initialize_then_modify_liquidity_then_first_swap",
                            }
                        else:
                            market = _market_from_report(
                                report,
                                candidate["token_address"],
                                candidate["pair_address"],
                            )
                        self.store.record_analysis(
                            candidate["token_address"], analysis, market,
                            priority_reason=priority_reason,
                            queue_age_seconds=queue_age,
                            report_data=report.get("data") or {},
                        )
                        try:
                            memory = self.seal_analysis_memory(
                                self.store.candidate(candidate["token_address"]),
                                {**report, "analysis": analysis}, market,
                                priority_reason=priority_reason,
                            )
                            producer_analyses_sealed += (
                                memory.get("status") == "sealed"
                            )
                        except Exception:
                            # Producer sealing is idempotent and isolated. A
                            # failure is visible in the cycle summary but does
                            # not misclassify a completed token analysis.
                            producer_failures += 1
                        momentum_analyses += priority_reason == "liquid_momentum"
                        flow_shadow_analyses += priority_reason == "flow_shadow_priority"
                        if queue_age is not None:
                            queue_ages.append(queue_age)
                        latest = self.store.candidate(candidate["token_address"])
                        if self.store.open_position(latest, market):
                            entries += 1
                            self.ledger.append("robinhood_paper_buy", {
                                "token_address": candidate["token_address"],
                                "symbol": latest.get("symbol"),
                                "score": latest.get("score"),
                                "price_usd": market.get("price_usd"),
                                "liquidity_usd": market.get("liquidity_usd"),
                                "paper_only": True,
                            })
                        analyses += 1
                    except Exception as exc:
                        analysis_failures += 1
                        self.store.record_analysis_failure(
                            candidate["token_address"], str(exc)
                        )
                stage_timings["analyses"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                remaining_budget = (
                    float(cycle_budget_seconds) - (time.monotonic() - started)
                )
                if remaining_budget < V4_CUSTODY_START_RESERVE_SECONDS:
                    v4_custody = {
                        "supported": True,"deferred": True,
                        "reason": "critical_learning_work_used_cycle_budget",
                        "remaining_budget_seconds": round(remaining_budget, 3),
                    }
                else:
                    try:
                        v4_custody = self.v4_custody.sync(
                            block_limit=min(
                                discovery_block_limit,V4_CUSTODY_BLOCK_LIMIT,
                            ),
                            lookback=min(lookback,V4_CUSTODY_BLOCK_LIMIT),
                            live_block_limit=min(
                                discovery_block_limit,
                                V4_CUSTODY_LIVE_BLOCK_LIMIT,
                            ),
                        )
                    except Exception as exc:
                        # Custody collection is prospective shadow telemetry.
                        # RPC throttling must never stop launch discovery,
                        # analysis, outcomes, or paper positions.
                        v4_custody = {
                            "supported": False,"deferred": True,
                            "reason": "custody_rpc_unavailable",
                            "error": str(exc)[:500],
                        }
                stage_timings["v4_position_custody"] = round(
                    time.monotonic() - stage_started, 3
                )
                stage_started = time.monotonic()
                learning_summary = self.store.summary()
                stage_timings["learning_summary"] = round(
                    time.monotonic() - stage_started, 3
                )
                summary = {
                    "schema_version": 2, "timestamp": _utc_now(),
                    "network": "robinhood", "chain_id": ROBINHOOD_NETWORK.chain_id,
                    "cycle": {
                        "new_candidates": new_candidates,
                        "pair_events_seen": len(discovered),
                        "v4_activations_seen": len(v4_discovered),
                        "analyses": analyses,
                        "momentum_analyses": momentum_analyses,
                        "flow_shadow_analyses": flow_shadow_analyses,
                        "flow_shadow": self.store.flow_summary(),
                        "flow_evidence_capture": flow_evidence_capture,
                        "flow_evidence_outcomes": flow_evidence_outcomes,
                        "flow_identity_resolution": identity_resolution,
                        "v4_position_custody": v4_custody,
                        "maximum_analysis_queue_age_seconds": (
                            round(max(queue_ages), 3) if queue_ages else None
                        ),
                        "analysis_failures": analysis_failures,
                        "producer_memory": {
                            "enabled": self.timechain_recorder is not None,
                            "analyses_sealed": (
                                producer_analyses_sealed
                                + market_rechecks["producer_analyses_sealed"]
                            ),
                            "analysis_failures": (
                                producer_failures
                                + market_rechecks["producer_failures"]
                            ),
                            "outcomes_sealed": outcomes.get(
                                "producer_outcomes_sealed", 0
                            ),
                            "outcomes_legacy_unbound": outcomes.get(
                                "producer_outcomes_legacy_unbound", 0
                            ),
                            "outcome_failures": outcomes.get(
                                "producer_failures", 0
                            ),
                        },
                        "analyses_deferred_for_deadline": analyses_deferred_for_deadline,
                        "cycle_budget_seconds": cycle_budget_seconds,
                        "paper_entries": entries + market_rechecks["paper_entries"],
                        "market_rechecks": market_rechecks,
                        "position_evaluations": position_evaluations,
                        "outcomes": outcomes,
                        "stage_timings_seconds": stage_timings,
                        "duration_seconds": round(time.monotonic() - started, 3),
                    },
                    "discovery_coverage": coverage,
                    "discovery_coverage_by_source": {
                        "uniswap_v2": coverage, "uniswap_v4": v4_coverage,
                    },
                    "learning": learning_summary,
                    "flow_evidence": self.store.flow_evidence_summary(),
                    "flow_freshness": flow_evidence_capture.get(
                        "freshness_timing", {}
                    ),
                    "flow_arms": self.store.flow_arm_comparison(),
                    "flow_observations": flow_evidence_capture.get("observations", {}),
                    "flow_cohort": self.store.cohort_progress(),
                    "flow_signal_vs_control": self.store.signal_versus_control(),
                    "flow_observation_outcomes": observation_outcomes,

                    # Per-cycle gate counts. Reconstructing these by hand cost
                    # most of a session; emitting them means the next reader
                    # sees which gate is actually blocking without forensics.
                    "flow_gates": self.store.flow_gate_telemetry(
                        head_block=int(
                            near_head_flow.get("head_block_after") or 0
                        ) or None,
                    ),
                    "v4_custody": self.store.v4_custody_summary(),
                    "paper_only": True, "live_execution_enabled": False,
                }
                atomic_json_write(self.root / "learning_summary.json", summary)
                with self.store.connection() as connection:
                    connection.execute(
                        "UPDATE runs SET completed_at=?,status='complete',summary_json=? WHERE id=?",
                        (_utc_now(), _canonical(summary), run_id),
                    )
                return summary
            except Exception as exc:
                failure = {
                    "schema_version": 1, "timestamp": _utc_now(),
                    "status": "failed", "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                with self.store.connection() as connection:
                    connection.execute(
                        "UPDATE runs SET completed_at=?,status='failed',summary_json=? WHERE id=?",
                        (_utc_now(), _canonical(failure), run_id),
                    )
                raise

    def verify(self) -> dict:
        ledger_ok,ledger_report=self.ledger.verify()
        with self.store.connection() as connection:
            sqlite_ok=connection.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
        timechain_ok = True
        timechain_report = "disabled"
        if self.timechain_recorder is not None:
            timechain_ok, timechain_report = self.timechain_recorder.verify()
        return {
            "ok": ledger_ok and sqlite_ok and timechain_ok,
            "ledger": ledger_report,
            "sqlite_integrity": sqlite_ok,
            "producer_timechain": timechain_report,
            "producer_timechain_ok": timechain_ok,
            "paper_only": True,
        }

    def repair_outcome_integrity(self) -> dict:
        if self.timechain_recorder is None:
            raise RuntimeError("producer Timechain is disabled")
        repaired = self.timechain_recorder.repair_invalid_outcomes()
        for ring in repaired:
            payload = ring.get("payload") or {}
            record = payload.get("outcome_record") or {}
            token = str(payload.get("token_address") or "").lower()
            horizon = str(payload.get("horizon") or "")
            if token and horizon:
                self.store.record_outcome_producer_reference(
                    token, horizon, ring, canonical_hash(record),
                )
        ledger = verify_outcome_rings(self.timechain_recorder.tc.load())
        return {
            "status": "repaired" if repaired else "no_changes",
            "repaired": len(repaired),
            "correction_rings": [ring.get("index") for ring in repaired],
            "outcome_ledger": ledger,
        }


def dashboard_snapshot(
    root: str | Path,
    *,
    store: RobinhoodLearningStore | None = None,
    live_position_markets: dict[str, dict] | None = None,
    market_refresh_errors: dict[str, str] | None = None,
) -> dict:
    root=Path(root)
    store=store or RobinhoodLearningStore(root/"learning.sqlite3")
    summary=read_json(root/"learning_summary.json",{}) or {}
    cursor=read_json(root/"discovery_cursor.json",{}) or {}
    v4_cursor=read_json(root/"discovery_v4_cursor.json",{}) or {}
    reflection_state=read_json(root/"reflection_state.json",{}) or {}
    flow_reflection_state=read_json(root/"flow_reflection_state.json",{}) or {}
    reflection_result=read_json(
        reflection_state.get("latest_result") or root/"reflection_missing.json", {}
    ) or {}
    counterfactual_audit=read_json(root/"counterfactual_audit.json",{}) or {}
    positions = store.recent_positions(live_markets=live_position_markets)
    audit_summary = {
        key: value for key, value in counterfactual_audit.items()
        if key != "headline_review"
    }
    return {
        "timestamp":_utc_now(),"network":"robinhood","chain_id":ROBINHOOD_NETWORK.chain_id,
        "learning":store.summary(),
        "positions":[position for position in positions if position.get("status") == "open"],
        "closed_positions":[
            position for position in positions if position.get("status") == "closed"
        ][:12],
        "closed_performance":store.closed_performance(),
        # A bounded audit feed includes every decision class. The browser keeps
        # it collapsed, time-filtered and paged so rejected-token growth cannot
        # dominate either the snapshot or the dashboard.
        "analyzed_tokens":store.recent_analyzed_tokens(
            limit=100, include_rejected=True,
        ),
        "flow_shadow": store.flow_summary(),
        "flow_signals": store.recent_flow_signals(limit=12),
        "flow_evidence": store.flow_evidence_summary(),
        # Friction is the largest component of every return recorded here.
        "round_trip": store.round_trip_summary(),
        # Distinct from discovery_coverage, which is the BACKFILL cursor.
        "pool_discovery": store.pool_discovery_latency(),
        "flow_evidence_events": store.recent_flow_evidence_events(limit=16),
        "flow_origin_queue": store.pending_transaction_origin_counts(),
        "v4_custody": store.v4_custody_summary(),
        "last_cycle":summary.get("cycle") or {},
        "discovery_coverage":cursor.get("coverage") or summary.get("discovery_coverage") or {},
        "discovery_coverage_by_source":{
            "uniswap_v2":cursor.get("coverage") or {},
            "uniswap_v4":v4_cursor.get("coverage") or {},
        },
        "paper_only":True,"live_execution_enabled":False,
        "market_refresh_errors": market_refresh_errors or {},
        "reflection": {
            "last_checkpoint": reflection_state.get("last_checkpoint", 0),
            "next_checkpoint": reflection_state.get("next_checkpoint", 15),
            "winner": reflection_result.get("winner"),
            "recommendations": reflection_result.get("recommendations") or [],
            "counterfactual_audit": audit_summary,
        },
        "flow_reflection": flow_reflection_state,
    }


class RobinhoodDashboardMarketRefresher:
    """Coalesce exact-pool refreshes behind a short stale-safe cache."""

    def __init__(self, root: str | Path, *, engine=None, read_only: bool = False):
        # read_only skips the engine entirely: constructing one migrates the
        # database, which is exactly the write the dashboard must not make.
        self.read_only = bool(read_only) and engine is None
        if self.read_only:
            self.root = Path(root)
            self.store = RobinhoodLearningStore(
                self.root / "learning.sqlite3", read_only=True,
            )
            self.engine = None
        else:
            self.engine = engine or RobinhoodLearningEngine(root)
            self.root = self.engine.root
            self.store = self.engine.store
        self.lock = threading.Lock()
        self._markets: dict[str, dict] = {}
        self._errors: dict[str, str] = {}
        self._refreshed_monotonic = 0.0
        self._refreshed_at: str | None = None

    def snapshot(self) -> dict:
        with self.lock:
            age = time.monotonic() - self._refreshed_monotonic
            if self._refreshed_monotonic and age < DASHBOARD_MARKET_CACHE_SECONDS:
                snapshot = dashboard_snapshot(
                    self.root,
                    store=self.store,
                    live_position_markets=self._markets,
                    market_refresh_errors=self._errors,
                )
                snapshot["market_cache"] = {
                    "refreshed_at": self._refreshed_at,
                    "age_seconds": round(age, 3),
                    "ttl_seconds": DASHBOARD_MARKET_CACHE_SECONDS,
                }
                return snapshot
            markets: dict[str, dict] = {}
            errors: dict[str, str] = {}
            if self.read_only:
                # Live position re-pricing needs an engine and its RPC. A
                # read-only dashboard serves the sealed record instead of
                # making network calls on the viewer's behalf.
                self._refreshed_monotonic = time.monotonic()
                self._refreshed_at = _utc_now()
                snapshot = dashboard_snapshot(self.root, store=self.store)
                snapshot["market_cache"] = {
                    "refreshed_at": self._refreshed_at, "age_seconds": 0.0,
                    "ttl_seconds": DASHBOARD_MARKET_CACHE_SECONDS,
                    "live_refresh": False,
                }
                return snapshot
            with self.store.connection() as connection:
                candidates = [dict(row) for row in connection.execute(
                    """
                    SELECT c.*,p.quantity paper_quantity
                    FROM candidates c JOIN positions p USING(token_address)
                    WHERE p.status='open' ORDER BY p.opened_at
                    """
                )]
            for candidate in candidates:
                token = candidate["token_address"]
                try:
                    market = (
                        self.engine.v4_market.snapshot(candidate)
                        if candidate.get("source_version") == SOURCE_V4
                        else self.engine.market.snapshot(token, candidate["pair_address"])
                    )
                    if market:
                        markets[token] = {**market, "observed_at": _utc_now()}
                except Exception as exc:
                    errors[token] = str(exc)
            self._markets = markets
            self._errors = errors
            self._refreshed_monotonic = time.monotonic()
            self._refreshed_at = _utc_now()
            snapshot = dashboard_snapshot(
                self.root,
                store=self.store,
                live_position_markets=markets,
                market_refresh_errors=errors,
            )
            snapshot["market_cache"] = {
                "refreshed_at": self._refreshed_at,
                "age_seconds": 0.0,
                "ttl_seconds": DASHBOARD_MARKET_CACHE_SECONDS,
            }
            return snapshot


def serve_dashboard(root: str | Path, host: str, port: int) -> None:
    if host not in {"127.0.0.1","localhost"}:
        raise ValueError("Robinhood dashboard is local-only")
    html_path=Path(__file__).with_name("robinhood_dashboard.html")
    refresher=RobinhoodDashboardMarketRefresher(root, read_only=True)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in {"/","/index.html"}:
                content=html_path.read_bytes(); content_type="text/html; charset=utf-8"
            elif self.path=="/api/status":
                # Never block: say "warming" rather than hang for 70 seconds.
                payload=cached["payload"] or {
                    "warming": True, "timestamp": _utc_now(),
                    "detail": "First snapshot is still building.",
                    "error": cached.get("error"),
                }
                if cached["payload"] is not None:
                    payload=dict(payload)
                    payload["snapshot_built_at"]=cached["built_at"]
                content=json.dumps(payload).encode(); content_type="application/json"
            else:
                self.send_error(404); return
            self.send_response(200); self.send_header("Content-Type",content_type)
            self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(content)))
            self.end_headers(); self.wfile.write(content)
        def log_message(self, *_args):
            return
    # The snapshot costs ~70 seconds: flow_summary, v4_custody_summary and
    # pending_transaction_origin_counts each range-join 487k swap rows against
    # 2,176 signal windows on a BETWEEN, which no index serves. Computing that
    # on the request path made /api/status time out in the browser, so it is
    # computed off the request path instead and every request is served the
    # last completed build. Stale by up to a refresh interval, never hanging.
    cached: dict = {"payload": None, "built_at": None, "building": False}

    def rebuild() -> None:
        while True:
            try:
                started = time.monotonic()
                payload = refresher.snapshot()
                payload["snapshot_build_seconds"] = round(
                    time.monotonic() - started, 1
                )
                cached["payload"] = payload
                cached["built_at"] = _utc_now()
            except Exception as error:
                cached["error"] = str(error)[:200]
            time.sleep(DASHBOARD_SNAPSHOT_REFRESH_SECONDS)

    threading.Thread(target=rebuild, daemon=True).start()
    server=ThreadingHTTPServer((host,port),Handler)
    print(f"Robinhood learning dashboard: http://{host}:{port}")
    print("READ-ONLY: no write endpoints exist. Press Ctrl+C to stop.")
    try: server.serve_forever(poll_interval=.5)
    except KeyboardInterrupt: pass
    finally: server.server_close()


def main() -> None:
    ensure_utf8_runtime()
    parser=argparse.ArgumentParser(description="Robinhood Chain paper learning")
    parser.add_argument(
        "command",
        choices=(
            "learn-once", "status", "dashboard", "verify", "reflect",
            "audit", "repair-outcomes",
        ),
    )
    parser.add_argument("--root",default=DEFAULT_ROOT)
    parser.add_argument("--host",default="127.0.0.1")
    parser.add_argument("--port",type=int,default=DEFAULT_DASHBOARD_PORT)
    parser.add_argument("--discovery-block-limit",type=int,default=DEFAULT_DISCOVERY_BLOCK_LIMIT)
    parser.add_argument("--analysis-limit",type=int,default=DEFAULT_ANALYSIS_LIMIT)
    parser.add_argument("--outcome-limit",type=int,default=DEFAULT_OUTCOME_LIMIT)
    parser.add_argument(
        "--outcome-recovery-limit", type=int,
        default=DEFAULT_OUTCOME_RECOVERY_LIMIT,
    )
    parser.add_argument(
        "--market-recheck-limit", type=int,
        default=DEFAULT_MARKET_RECHECK_LIMIT,
    )
    parser.add_argument(
        "--cycle-budget-seconds", type=float,
        default=DEFAULT_CYCLE_BUDGET_SECONDS,
    )
    parser.add_argument("--lookback",type=int,default=DEFAULT_DISCOVERY_LOOKBACK_BLOCKS)
    parser.add_argument("--chain-root",default=DEFAULT_CHAIN_ROOT)
    parser.add_argument(
        "--todo", default=str(Path(__file__).with_name("TODO.md"))
    )
    parser.add_argument("--skill-root",default=str(default_skill_root()))
    args=parser.parse_args()
    if args.command=="dashboard":
        serve_dashboard(args.root,args.host,args.port); return
    if args.command=="status":
        print(json.dumps(dashboard_snapshot(args.root),indent=2)); return
    engine=RobinhoodLearningEngine(
        args.root, chain_root=args.chain_root, skill_root=args.skill_root,
    )
    if args.command=="verify":
        result=engine.verify(); print(json.dumps(result,indent=2)); raise SystemExit(0 if result["ok"] else 1)
    if args.command=="repair-outcomes":
        result = engine.repair_outcome_integrity()
        print(json.dumps(result, indent=2))
        raise SystemExit(0 if result["outcome_ledger"]["ok"] else 1)
    reflection = RobinhoodReflectionCoordinator(
        args.root,
        engine.store,
        todo_path=args.todo,
        skill_root=args.skill_root,
    )
    if args.command=="reflect":
        print(json.dumps(reflection.run_if_due(),indent=2)); return
    if args.command=="audit":
        print(json.dumps(reflection.audit_all(),indent=2)); return
    summary=engine.run_once(
        discovery_block_limit=max(1,args.discovery_block_limit),
        analysis_limit=max(0,args.analysis_limit),outcome_limit=max(0,args.outcome_limit),
        outcome_recovery_limit=max(0,args.outcome_recovery_limit),
        market_recheck_limit=max(0,args.market_recheck_limit),
        cycle_budget_seconds=max(60.0,args.cycle_budget_seconds),
        lookback=max(1,args.lookback),
    )
    try:
        summary["reflection"] = reflection.run_if_due()
    except Exception as exc:
        # Reflection is retried from the same durable checkpoint next cycle. A
        # sealing/tooling failure must not turn completed paper learning into a
        # failed analysis cycle.
        summary["reflection"] = {
            "status": "retry_pending",
            "error": str(exc),
        }
    try:
        summary["flow_reflection"] = reflection.run_flow_if_due()
    except Exception as exc:
        summary["flow_reflection"] = {
            "status": "retry_pending", "error": str(exc),
        }
    print(json.dumps(summary,indent=2))


if __name__=="__main__":
    main()
