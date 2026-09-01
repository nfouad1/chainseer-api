"""Robinhood Chain launch learning and counterfactual paper trading.

Paper-only by construction: this module contains no private-key, signing,
approval, transaction-submission, or broadcast path. Discovery is isolated
from expensive analysis so new-pair intake remains bounded and observable.
"""

from __future__ import annotations

import time

# Earliest Python-visible startup marker.  The supervisor's absolute
# monotonic deadline starts before process creation; this marker separates
# OS/Python bootstrap from module-import time instead of reporting both as an
# opaque ``startup_consumed_seconds`` outlier.
PROCESS_MODULE_IMPORT_STARTED_MONOTONIC = time.monotonic()

import argparse
import ctypes
from ctypes import wintypes
import hashlib
import json
import math
import os
import random
import re
import sqlite3
import subprocess
import sys
import threading
import uuid
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import requests

from chainseer import (
    ADDRESS_RE,
    ROBINHOOD_NETWORK,
    UNISWAP_V2_FACTORY,
    UNISWAP_V4_POOL_MANAGER,
    WETH_ADDRESS,
    Chainseer,
    RPCError,
    RobinhoodRPC,
    _load_timechain_module,
    ensure_utf8_runtime,
)
from chainseer_base import LearningRunLock, _process_is_running
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
from chainseer_robinhood_commitments import (
    DecisionCommitmentStore,
    DecisionCommitmentError,
    evaluate_integrity_certificate,
    load_integrity_certificate,
)
from chainseer_robinhood_gate import ExecutionGate, build_commitment_spec
from chainseer_robinhood_reflection import (
    RobinhoodReflectionCoordinator,
    default_skill_root,
)

PROCESS_IMPORTS_COMPLETED_MONOTONIC = time.monotonic()


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
V3_SLOT0_SELECTOR = "3850c7bd"
V3_LIQUIDITY_SELECTOR = "1a686502"
V3_TOKEN0_SELECTOR = "0dfe1681"
V3_TOKEN1_SELECTOR = "d21220a7"
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
# The dashboard has two deliberately independent freshness domains. Operational
# state must stay useful while historical range joins are still running.
DASHBOARD_OPERATIONAL_REFRESH_SECONDS = 5.0
# Historical aggregation scans millions of swap-origin rows.  Refreshing it
# every 30 seconds meant the dashboard spent almost its entire lifetime in a
# new full-corpus read, competing with the latency-critical live writer for
# CPU and disk.  Historical research does not need operational cadence; the
# independently cached operational payload still refreshes every five seconds.
DASHBOARD_SNAPSHOT_REFRESH_SECONDS = 15 * 60.0
DASHBOARD_OPERATIONAL_STALE_SECONDS = 20.0
DASHBOARD_HISTORICAL_STALE_SECONDS = 15 * 60.0
# The full 5.5 GB check is deliberately daily, not per learner cycle.  A
# two-hour display margin prevents a certificate from flickering red while
# the next low-priority maintenance pass is still reading the database.
DASHBOARD_INTEGRITY_MAX_AGE_SECONDS = 26 * 60 * 60
FULL_INTEGRITY_MAX_AGE_SECONDS = 8 * 24 * 60 * 60
FULL_VERIFICATION_REFRESH_SECONDS = 24 * 60 * 60
DEFAULT_DISCOVERY_LOOKBACK_BLOCKS = 5_000
DEFAULT_DISCOVERY_BLOCK_LIMIT = 5_000
DEFAULT_ANALYSIS_LIMIT = 1
DEFAULT_OUTCOME_LIMIT = 12
DEFAULT_OUTCOME_RECOVERY_LIMIT = 4
DEFAULT_MARKET_RECHECK_LIMIT = 4
DEFAULT_CYCLE_BUDGET_SECONDS = 255.0
# Position marking and flow ingestion are SEPARATE critical lanes. Sharing one
# 25-second budget, each stage costing ~10-11s, left no headroom: over the last
# 200 combined runs 197 hit the deadline and 2 completed -- a 1.0% completion
# rate whose p95 of 22.2s was computed on two survivors and therefore looked
# healthy. They also have different failure domains: marking depends on an
# external price API, ingestion on chain RPC, so a price stall was consuming
# the budget blockchain freshness needed and neither could be sized alone.
# Marking needs more than the live lane could ever have spared: run alone it
# still exceeded 20s at position_mark_commit. That is the finding the split
# produced -- in the combined lane it was consuming most of the 25s and
# leaving ingestion to fail. It is also not latency-critical in the way chain
# freshness is: a mark that is 60s old is fine, a window 60s behind the head
# is not. Sized generously and separately, which is the whole point.
MARKS_LANE_BUDGET_SECONDS = 90.0
#: Every lane, in one place. Three separate hardcoded tuples had already
#: drifted from the lane configuration once; anything iterating lanes reads
#: this or the config dict, never its own copy.
SUPERVISED_LANE_NAMES = (
    "live", "marks", "evidence", "analysis", "backfill")
LANE_NAMES = (*SUPERVISED_LANE_NAMES, "verification")
#: How long past its deadline the supervisor lets a lane run before killing
#: it. A censored attempt is charged deadline + this, so a reliability failure
#: stays a reliability failure without corrupting the latency SLO.
LANE_TERMINATION_GRACE_SECONDS = 3.0
MARKS_LANE_CADENCE_SECONDS = 45.0
LIVE_LANE_BUDGET_SECONDS = 25.0
ANALYSIS_LANE_BUDGET_SECONDS = 120.0
EVIDENCE_LANE_BUDGET_SECONDS = 90.0
# Remote evidence work must stop before the supervisor deadline so the child
# can persist its summary and release SQLite cleanly. The RPC-priority soak
# measured a worker entering ``observation_outcomes`` with exactly 0.0s left;
# it was then killed even though the expensive work had already completed.
EVIDENCE_COMPLETION_RESERVE_SECONDS = 8.0
# No evidence stream may consume the whole lane. Production measured the
# entry-quote stage at 85.3s, after which observation quotes began with 0.0s
# and the worker missed its 90s deadline. Four bounded slices preserve useful
# progress across every durable stream; the last slice is also constrained by
# the shared completion reserve above.
EVIDENCE_STAGE_MAX_SECONDS = 20.0
BACKFILL_LANE_BUDGET_SECONDS = 120.0
# Operational verification performs bounded schema/readability/critical-table
# health checks plus complete ledger and producer-Timechain verification.
# Both SQLite quick_check and integrity_check exceeded practical maintenance
# bounds on this 5.5 GB, low-memory corpus, so exhaustive proof has a distinct
# weekly offline task and certificate. Neither may run in the 285-second live
# supervisor, and operational health is never labelled full.
VERIFICATION_LANE_BUDGET_SECONDS = 75 * 60.0
FULL_VERIFICATION_LANE_BUDGET_SECONDS = 3 * 60 * 60.0
BACKFILL_LANE_IDENTITY_LIMIT = 25
BACKFILL_V4_ACTIVATION_LIMIT = 25
# Keep enough of the lane budget after durable gap recovery to commit its
# summary/cursor and release SQLite cleanly.  Gap recovery receives a child
# deadline ending this many seconds before the lane deadline, so a final slow
# RPC cannot consume the completion reserve merely because it started while
# one second technically remained.
BACKFILL_COMPLETION_RESERVE_SECONDS = 10.0
# Durable skipped ranges are cursor-committed at this quantum regardless of a
# wider secondary-discovery batch setting. The v4 acceptance trace disproved
# 1,000 as a reliable operating point on the live provider: one of six calls
# succeeded, while every 500-block fallback and a read-only 750-block probe
# succeeded. Cap the v5 controller at the measured middle point instead of
# repeatedly spending a scheduled opportunity rediscovering the same limit.
BACKFILL_GAP_CHUNK_BLOCKS = 750
BACKFILL_GAP_INITIAL_CHUNK_BLOCKS = 750
BACKFILL_GAP_MINIMUM_CHUNK_BLOCKS = 125
BACKFILL_RPC_CHUNK_STATE_KEY = "backfill_rpc_chunk_v1"
BACKFILL_RPC_CHUNK_MODEL_EPOCH = 2
BACKFILL_RPC_PROBE_STEP_BLOCKS = 125
BACKFILL_RPC_SUCCESSES_BEFORE_PROBE = 5
# The v3 cohort proved request COUNT was binding during a retry burst; v4 then
# proved that range density also matters once request count is fixed at one.
# Preserve one attempt per atomic chunk and require repeated success before an
# additive upward size probe. A failed probe falls back to the last proven
# size, rather than oscillating 1,000 -> 500 -> 1,000 after every success.
# The v5 cohort measured the whole scheduler, not merely an isolated RPC:
# three backfill launches per five-minute supervisor window recovered 4,750
# blocks while 9,754 new skipped blocks arrived.  The 750-block query shape
# itself is proven (219 consecutive successes), so changing the range size or
# retrying a failed request would attack the wrong constraint.  Permit one
# additional independently committed chunk after a successful first chunk.
# A throttle still stops the cycle immediately, and every completed chunk has
# already advanced its durable cursor before the next provider call begins.
BACKFILL_MAXIMUM_CHUNKS_PER_CYCLE = 6
BACKFILL_REMOTE_ATTEMPTS_PER_CHUNK = 1
BACKFILL_RECOVERY_TARGET_RATIO = 1.10
BACKFILL_QUEUE_MAINTENANCE_MINIMUM_SECONDS = 8.0
BACKFILL_RPC_ATTEMPT_BUDGET_SECONDS = 20.0
BACKFILL_LAUNCH_MINIMUM_LIVE_WINDOW_SECONDS = 12.0
BACKFILL_RPC_URL_ENV = "CHAINSEER_ROBINHOOD_BACKFILL_RPC_URL"
# The supervised live cadence is 30 seconds and this chain has recently
# produced roughly ten blocks/second. Scan a bounded newest-head slice on the
# decision path; any older prefix is durably re-anchored into backfill.
# Cohort evidence showed 199-212 blocks still cost 7.8-10.1 seconds and put
# 3/5 early decisions beyond 120 blocks. A 100-block newest-head slice leaves
# the remaining lag budget for sealing/classification; history is recovered
# by the independently scheduled backfill lane.
LIVE_LANE_SCAN_BLOCKS = 100
LIVE_LANE_ENRICHMENT_LIMIT = 60
LIVE_LANE_ENRICHMENT_BUDGET_SECONDS = 6.0
# Promotion evidence must be prospective, but its remote quotes and outcome
# observations are not latency-critical.  The live lane therefore commits
# only bounded metadata while a separate supervised lane owns every remote
# evidence call.  These limits are throughput bounds, never admission gates.
EVIDENCE_LANE_CADENCE_SECONDS = 60.0
EVIDENCE_ENTRY_QUOTE_LIMIT = 8
EVIDENCE_EVENT_OUTCOME_LIMIT = 12
EVIDENCE_OBSERVATION_OUTCOME_LIMIT = 12
EVIDENCE_OBSERVATION_QUOTE_LIMIT = 8
#: Headroom one outcome needs to finish once started. Each resolution takes a
#: block-pinned exit quote -- a remote call -- so an item begun with less than
#: this left runs past the lane deadline and is killed by the supervisor,
#: discarding every resolution the run had already completed. Measured: 90 of
#: 98 evidence-lane failures terminated inside observation_outcomes, whose p95
#: is 69.6s against a 90s budget at 12 items, i.e. ~5.8s per outcome.
#: Deferring the next item instead costs one outcome; overrunning costs all of
#: them, which is why the effective resolution rate sat near three quarters of
#: nominal while the backlog diverged.
EVIDENCE_OUTCOME_ITEM_RESERVE_SECONDS = 8.0
# The live lane is for a fresh decision, not historical queue throughput.
# The first clean 100-attempt cohort completed 100/100, but only 81 decisions
# landed within 120 blocks because the quote stage handled eight windows. A
# two-window cap keeps the prospective sample while the backfill lane owns the
# durable historical queue below.
LIVE_LANE_OBSERVATION_LIMIT = 2
#: Held back from sealing for decision-head retrieval and classification.
#: Attributed failures showed the shape exactly: of 64 deadline_exceeded live
#: cycles, 46 died inside sealing and 18 died inside classification having
#: spent only 0.26-2.82s there -- sealing had already eaten the budget and
#: left classification a remainder too small to finish in. Sealing is the one
#: stage that can always yield, because a deferred window is queued rather
#: than lost; classification cannot, because an unclassified observation has
#: no decision attached to it.
LIVE_LANE_DECISION_RESERVE_SECONDS = 5.0
#: Cold-start per-window seal cost, replaced by a measured EWMA after the
#: first pass. 2.0s is the observed 16.4s median over the 8-window limit.
SEAL_WINDOW_COST_SECONDS_DEFAULT = 2.0
#: Weight on the newest measurement. High enough to track a provider slowing
#: down within a few cycles, low enough that one stalled window does not
#: collapse admission to a single observation.
SEAL_COST_SMOOTHING = 0.3
#: Two-part observation-cost model, stored durably under
#: SCHEDULER_STATE key "seal_cost_model_v2". The old single scalar could not
#: represent fixed cycle work, per-window work and the downstream reserve as
#: separate populations, which made safe admission impossible to audit.
SEAL_FIXED_OBSERVATION_COST_SECONDS_DEFAULT = 0.0
#: Queue completion is part of fixed cost but must also remain explicitly
#: reserved while the per-window loop is running; otherwise the final quote
#: can consume the headroom needed to make deferral durable.
QUEUE_SETTLEMENT_COST_SECONDS_DEFAULT = 1.0
#: Cold start assumes ZERO fixed cost -- the first cycle's measurement
#: replaces it immediately, and an optimistic first cycle beats a controller
#: that refuses to ever seal while it has no data.
SEAL_PER_WINDOW_COST_SECONDS_DEFAULT = SEAL_WINDOW_COST_SECONDS_DEFAULT
#: Downstream reserve: decision-head retrieval + classification + ledger
#: completion -- work that CANNOT yield once sealing has spent the budget.
#: Cold-starts at the lane's own decision reserve until measured.
DOWNSTREAM_RESERVE_SECONDS_DEFAULT = LIVE_LANE_DECISION_RESERVE_SECONDS
SEAL_COST_MODEL_STATE_KEY = "seal_cost_model_v2"
#: Raw successful/censored timing samples retained for the observation
#: admission model.  A scalar EWMA labelled "p95" is not a percentile and,
#: worse, cannot distinguish a healthy distribution from one lucky pass.
#: Keeping a bounded durable window makes the percentile reproducible after a
#: restart without letting the scheduler-state row grow without bound.
SEAL_COST_SAMPLE_WINDOW = 128
#: Stall threshold for fixed-cost samples: a selection/prefetch pass this
#: many times the window's median is a STALL (provider hang, lock wait), not
#: the normal cost of the work. Stalls are counted and reported separately
#: as a reliability metric -- never winsorized into the estimate, which
#: would erase exactly the evidence a post-mortem needs.
SEAL_FIXED_COST_STALL_RATIO = 8.0
#: Before a healthy median exists, a pass above this absolute bound is still
#: a stall.  Without a cold-start bound the very first 60s provider hang is
#: accepted as the baseline and poisons the epoch it was meant to protect.
SEAL_FIXED_COST_STALL_ABSOLUTE_SECONDS = 8.0
#: A stall population above one percent is not healthy enough for normal
#: multi-window admission.  The paper learner retains one bounded probe so
#: the estimator can demonstrate recovery; readiness remains fail-closed.
SEAL_STALL_RATE_MAX = 0.01
SEAL_STALL_GUARD_MIN_SAMPLES = 20
#: Queue entries older than this are irrecoverably stale: the window can
#: never again overlap the near-head region, so sealing one would pin a
#: quote to an observation head that no longer exists. They first become
#: expired_stale and are then retired terminally as expired_unsealed.  Their
#: immutable snapshot stays in the queue for audit, but no evidence/cohort
#: record is fabricated after the fact.
SEAL_QUEUE_STALE_SECONDS = 3 * 3600.0
#: Estimator epoch: bumping this starts a fresh sample window so a poisoned
#: distribution from an older code revision cannot leak into new estimates.
#: Samples carry the epoch they were recorded under; only current-epoch
#: samples drive admission.
SEAL_COST_MODEL_EPOCH = 4
def _workspace_revision(root: Path | None = None) -> str:
    """Read the checkout HEAD without spawning Git or trusting process env."""
    workspace = (root or Path(__file__).resolve().parent).resolve()
    git_dir = workspace / ".git"
    try:
        if git_dir.is_file():
            marker = git_dir.read_text(encoding="utf-8").strip()
            if not marker.lower().startswith("gitdir:"):
                return "unknown"
            git_dir = (workspace / marker.split(":", 1)[1].strip()).resolve()
        head = (git_dir / "HEAD").read_text(encoding="ascii").strip()
        if not head.startswith("ref:"):
            return head[:12] if head else "unknown"
        ref = head.split(":", 1)[1].strip()
        ref_path = git_dir / ref
        if ref_path.exists():
            value = ref_path.read_text(encoding="ascii").strip()
            return value[:12] if value else "unknown"
        packed = git_dir / "packed-refs"
        if packed.exists():
            for line in packed.read_text(encoding="ascii").splitlines():
                if not line or line.startswith(("#", "^")):
                    continue
                value, name = line.split(" ", 1)
                if name.strip() == ref:
                    return value[:12]
    except (OSError, ValueError):
        return "unknown"
    return "unknown"


#: Code revision stamped onto every timing sample for provenance. The runner
#: exports it so a process keeps naming the code it imported even if the
#: checkout moves underneath it. Ad-hoc processes fall back to actual HEAD.
_CONFIGURED_CODE_REVISION = os.environ.get("CHAINSEER_CODE_REVISION", "")
CODE_REVISION = (
    _CONFIGURED_CODE_REVISION
    if _CONFIGURED_CODE_REVISION not in {"", "unknown"}
    else _workspace_revision()
)


_SOURCE_DIGEST_CACHE: str | None = None


def _worktree_source_digest() -> str:
    """Hash of the source actually on disk, not the revision it claims to be.

    Computed once per process. A running process cannot change the modules it
    already imported, so re-hashing 1.1MB on every begin_run bought nothing
    the import-time value does not already carry -- measured at 7.3ms median,
    26ms p95, 41ms max on the decision-critical path.

    That cost turned out NOT to explain the decision-lag drop it was suspected
    of causing (0.15 blocks median at the planning rate, against a 14-point
    conformance change). It is cached because the work is pointless, not
    because it was the culprit.

    A revision pin cannot see uncommitted edits. A cohort can therefore sit at
    `revision_mismatches: 0` while every attempt runs modified code -- the pin
    records what git was last told, and the process runs the working tree.
    That is the same class of error as the 1,777/1,786 contaminated cohort,
    but invisible to the check built to catch it.

    Digests the modules that decide behaviour. Missing files are recorded as
    absent rather than skipped, so deleting one changes the digest instead of
    quietly preserving it.
    """
    global _SOURCE_DIGEST_CACHE
    if _SOURCE_DIGEST_CACHE is not None:
        return _SOURCE_DIGEST_CACHE
    digest = hashlib.sha256()
    root = Path(__file__).resolve().parent
    for name in sorted(SOURCE_DIGEST_MODULES):
        digest.update(name.encode("utf-8"))
        try:
            digest.update((root / name).read_bytes())
        except OSError:
            digest.update(b"<absent>")
    _SOURCE_DIGEST_CACHE = digest.hexdigest()[:16]
    return _SOURCE_DIGEST_CACHE
#: Modules whose content defines live-lane behaviour for acceptance purposes.
SOURCE_DIGEST_MODULES = (
    "chainseer_robinhood.py",
    "chainseer_robinhood_commitments.py",
    "chainseer_robinhood_gate.py",
    "chainseer.py",
    "chainseer_core.py",
)
ACCEPTANCE_COHORT_SCHEMA_VERSION = 2
ACCEPTANCE_COHORT_POLICY_VERSION = "robinhood-operational-v11"
ACCEPTANCE_DECISION_SAMPLE_FRACTION = 0.50
ACCEPTANCE_POSITION_MARK_SAMPLE_FRACTION = 0.40


def _seal_cost_defaults() -> dict:
    """Cold-start defaults per model component."""
    return {
        "fixed_observation_cost_p95":
            SEAL_FIXED_OBSERVATION_COST_SECONDS_DEFAULT,
        "queue_settlement_p95": QUEUE_SETTLEMENT_COST_SECONDS_DEFAULT,
        "per_window_cost_p95": SEAL_PER_WINDOW_COST_SECONDS_DEFAULT,
        "downstream_reserve_p95": DOWNSTREAM_RESERVE_SECONDS_DEFAULT,
    }


def defaults_for(key: str) -> float:
    return _seal_cost_defaults().get(
        key, DOWNSTREAM_RESERVE_SECONDS_DEFAULT)


SEAL_COST_SAMPLE_FIELDS = {
    "fixed_observation_cost_p95": "fixed_observation_samples",
    "queue_settlement_p95": "queue_settlement_samples",
    "per_window_cost_p95": "per_window_samples",
    "downstream_reserve_p95": "downstream_samples",
}

# Selection/prefetch and queue settlement are both once-per-cycle costs. A
# lock/provider stall in either component is reliability evidence, not the
# normal price of one healthy cycle. Keeping those stalls out of the p95
# prevents a single exceptional pass from starving all subsequent live work;
# the combined stall guard still tightens admission when they recur.
SEAL_ONCE_PER_CYCLE_STALL_COMPONENTS = {
    "fixed_observation_cost_p95",
    "queue_settlement_p95",
}


def _nearest_rank_p95(values: list[float], fallback: float = 0.0) -> float:
    """Deterministic nearest-rank p95 shared by every estimator path."""
    if not values:
        return round(float(fallback), 4)
    ordered = sorted(float(value) for value in values)
    index = min(
        len(ordered) - 1,
        max(0, math.ceil(0.95 * len(ordered)) - 1),
    )
    return round(ordered[index], 4)


def _nearest_rank_p99(values: list[float], fallback: float = 0.0) -> float:
    """Deterministic nearest-rank p99 for the 99% decision-lag SLO."""
    if not values:
        return round(float(fallback), 4)
    ordered = sorted(float(value) for value in values)
    index = min(
        len(ordered) - 1,
        max(0, math.ceil(0.99 * len(ordered)) - 1),
    )
    return round(ordered[index], 4)


def _nearest_rank(values: list[float], quantile: float,
                  fallback: float = 0.0) -> float:
    """Deterministic nearest-rank quantile at an arbitrary level.

    _nearest_rank_p99 stays as-is for the decision-lag SLO, which genuinely
    wants the 99th percentile. The block-tail reserve does not: at every
    sample count its window can hold, nearest-rank p99 lands on the maximum
    or the value beside it (n=82 -> max, n=128 -> second largest), so it
    reserves the worst case ever seen rather than a percentile.
    """
    if not values:
        return round(float(fallback), 4)
    ordered = sorted(float(value) for value in values)
    index = min(
        len(ordered) - 1,
        max(0, math.ceil(float(quantile) * len(ordered)) - 1),
    )
    return round(ordered[index], 4)


def _valid_seal_sample_records(raw: object) -> list[dict]:
    """Return validated provenance records; legacy scalars never drive v2."""
    if not isinstance(raw, list):
        return []
    records: list[dict] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        value = item.get("value")
        if not isinstance(value, (int, float)):
            continue
        value = float(value)
        if not math.isfinite(value) or value < 0:
            continue
        status = str(item.get("status") or "")
        if status not in {"success", "censored", "stalled"}:
            continue
        records.append({**item, "value": value, "status": status})
    return records[-SEAL_COST_SAMPLE_WINDOW * 2:]


def _seal_cost_model_from_state(stored: dict) -> dict:
    """Derive the effective model from one canonical provenance schema.

    Successful current-epoch samples define the normative percentile.
    Censored samples are lower bounds and can only raise the effective cost.
    Stalls stay out of the percentile but activate a tighten-only admission
    guard, so excluding them cannot make the system pretend they did not
    happen.
    """
    epoch_matches = safe_int(stored.get("epoch"), 0) == SEAL_COST_MODEL_EPOCH
    epoch_model_initialized = bool(
        epoch_matches and any(
            key in stored for key in (
                *SEAL_COST_SAMPLE_FIELDS.keys(),
                *SEAL_COST_SAMPLE_FIELDS.values(),
            )))
    derived: dict[str, object] = {
        "epoch": SEAL_COST_MODEL_EPOCH,
        "epoch_model_initialized": epoch_model_initialized,
        "revision": str(stored.get("revision") or CODE_REVISION),
        "censored_samples": safe_int(stored.get("censored_samples"), 0),
        "stall_samples": safe_int(stored.get("stall_samples"), 0),
    }
    component_records: dict[str, list[dict]] = {}
    for scalar, field in SEAL_COST_SAMPLE_FIELDS.items():
        records = _valid_seal_sample_records(stored.get(field))
        current = [
            record for record in records
            if epoch_matches
            and safe_int(record.get("epoch"), -1) == SEAL_COST_MODEL_EPOCH
        ]
        component_records[field] = current
        successful = [
            record["value"] for record in current
            if record["status"] == "success"
        ][-SEAL_COST_SAMPLE_WINDOW:]
        censored = [
            record["value"] for record in current
            if record["status"] == "censored"
        ][-SEAL_COST_SAMPLE_WINDOW:]
        successful_at = [
            safe_float(record.get("at"), 0.0) for record in current
            if record["status"] == "success"
        ]
        censored_at = [
            safe_float(record.get("at"), 0.0) for record in current
            if record["status"] == "censored"
        ]
        # Scalar-only current-epoch fixtures and migrations remain readable,
        # but once provenance records exist the records are authoritative.
        fallback = defaults_for(scalar)
        if epoch_matches and not current:
            fallback = safe_float(stored.get(scalar), fallback)
        successful_p95 = _nearest_rank_p95(successful, fallback)
        censored_floor = _nearest_rank_p95(censored, 0.0)
        derived[scalar] = round(max(successful_p95, censored_floor), 4)
        derived[f"{scalar}_successful_p95"] = successful_p95
        derived[f"{scalar}_censored_floor"] = censored_floor
        derived[f"{scalar}_latest_success_at"] = max(
            successful_at, default=0.0)
        derived[f"{scalar}_latest_censored_at"] = max(
            censored_at, default=0.0)
        derived[field.replace("samples", "sample_count")] = len(successful)
        derived[field.replace("samples", "censored_count")] = len(censored)

    fixed = component_records["fixed_observation_samples"]
    settlement = component_records["queue_settlement_samples"]
    fixed_success = sum(record["status"] == "success" for record in fixed)
    fixed_stalls = sum(record["status"] == "stalled" for record in fixed)
    fixed_population = fixed_success + fixed_stalls
    fixed_stall_rate = (
        fixed_stalls / fixed_population if fixed_population else 0.0)
    settlement_success = sum(
        record["status"] == "success" for record in settlement)
    settlement_stalls = sum(
        record["status"] == "stalled" for record in settlement)
    settlement_population = settlement_success + settlement_stalls
    seal_stalls = fixed_stalls + settlement_stalls
    seal_stall_population = fixed_population + settlement_population
    seal_stall_rate = (
        seal_stalls / seal_stall_population if seal_stall_population else 0.0)
    derived.update({
        "fixed_stall_count": fixed_stalls,
        "fixed_stall_population": fixed_population,
        "fixed_stall_rate": round(fixed_stall_rate, 6),
        "queue_settlement_stall_count": settlement_stalls,
        "queue_settlement_stall_population": settlement_population,
        "seal_stall_count": seal_stalls,
        "seal_stall_population": seal_stall_population,
        "seal_stall_rate": round(seal_stall_rate, 6),
        "stall_guard_active": bool(
            seal_stall_population >= SEAL_STALL_GUARD_MIN_SAMPLES
            and seal_stall_rate > SEAL_STALL_RATE_MAX),
        "stall_rate_limit": SEAL_STALL_RATE_MAX,
    })
    # Compatibility names used by existing dashboards/tests.
    derived["fixed_sample_count"] = derived.get(
        "fixed_observation_sample_count", 0)
    derived["queue_settlement_sample_count"] = derived.get(
        "queue_settlement_sample_count", 0)
    derived["per_window_sample_count"] = derived.get(
        "per_window_sample_count", 0)
    derived["downstream_sample_count"] = derived.get(
        "downstream_sample_count", 0)
    return derived


def _seal_live_planning_model(effective: dict) -> dict:
    """Build live admission costs without erasing censored evidence.

    Censored lower bounds remain authoritative for reliability telemetry, but
    using them literally as a recurring reservation can permanently starve a
    25-second lane. Planning uses successful current-epoch p95s and turns a
    newer censored sample into a tighten-only, one-window recovery guard.
    """
    planning = dict(effective)
    quarantined: dict[str, dict] = {}
    guarded: list[str] = []
    raw_effective: dict[str, float] = {}
    for scalar in SEAL_COST_SAMPLE_FIELDS:
        successful = max(0.0, safe_float(
            effective.get(
                f"{scalar}_successful_p95", effective.get(scalar)),
            defaults_for(scalar),
        ))
        raw = max(0.0, safe_float(effective.get(scalar), successful))
        censored_floor = max(0.0, safe_float(
            effective.get(f"{scalar}_censored_floor"), 0.0))
        latest_success = safe_float(
            effective.get(f"{scalar}_latest_success_at"), 0.0)
        latest_censored = safe_float(
            effective.get(f"{scalar}_latest_censored_at"), 0.0)
        planning[scalar] = round(successful, 4)
        raw_effective[scalar] = round(raw, 4)
        if censored_floor > successful:
            quarantined[scalar] = {
                "successful_p95": round(successful, 4),
                "censored_floor": round(censored_floor, 4),
                "raw_effective": round(raw, 4),
            }
        if latest_censored > latest_success:
            guarded.append(scalar)
    planning.update({
        "planning_view": "successful_p95_with_censored_recovery_guard",
        "raw_effective_costs": raw_effective,
        "quarantined_censored_components": quarantined,
        "censored_guard_active": bool(guarded),
        "censored_guard_components": guarded,
    })
    return planning


def _append_seal_cost_records(
    stored: dict, updates: dict, *, status: str, run_id: str,
    revision: str, recorded_at: float, sample_count: int = 0,
) -> dict:
    """Canonical writer used by child success and supervisor termination."""
    if status not in {"success", "censored", "stalled"}:
        raise ValueError(f"invalid seal cost sample status: {status}")
    if safe_int(stored.get("epoch"), 0) != SEAL_COST_MODEL_EPOCH:
        stored = {"epoch": SEAL_COST_MODEL_EPOCH}
    payload = dict(stored)
    current = _seal_cost_model_from_state(payload)
    stalls_added = 0
    for scalar, measured_values in updates.items():
        field = SEAL_COST_SAMPLE_FIELDS[scalar]
        records = _valid_seal_sample_records(payload.get(field))
        incoming = (
            list(measured_values)
            if isinstance(measured_values, (list, tuple))
            else [measured_values]
        )
        clean = [
            max(0.0, float(value)) for value in incoming
            if isinstance(value, (int, float))
            and math.isfinite(float(value))
        ]
        history = [
            record["value"] for record in records
            if safe_int(record.get("epoch"), -1) == SEAL_COST_MODEL_EPOCH
            and record.get("status") == "success"
        ]
        median = (
            sorted(history)[len(history) // 2] if history else None)
        stall_threshold = max(
            SEAL_FIXED_COST_STALL_ABSOLUTE_SECONDS,
            (median or 0.0) * SEAL_FIXED_COST_STALL_RATIO,
        )
        for value in clean:
            record_status = status
            if (status == "success"
                    and scalar in SEAL_ONCE_PER_CYCLE_STALL_COMPONENTS):
                if value > stall_threshold:
                    record_status = "stalled"
                    stalls_added += 1
            if record_status == "censored":
                value = max(float(current.get(scalar, defaults_for(scalar))), value)
            records.append({
                "value": round(value, 4),
                "epoch": SEAL_COST_MODEL_EPOCH,
                "run_id": str(run_id or ""),
                "at": float(recorded_at),
                "revision": str(revision or "unknown"),
                "status": record_status,
            })
        payload[field] = records[-SEAL_COST_SAMPLE_WINDOW * 2:]
    payload.update({
        "epoch": SEAL_COST_MODEL_EPOCH,
        "revision": str(revision or "unknown"),
        "censored": status == "censored",
        "censored_samples": (
            safe_int(stored.get("censored_samples"), 0)
            + (1 if status == "censored" else 0)),
        "stall_samples": safe_int(stored.get("stall_samples"), 0)
        + stalls_added + (1 if status == "stalled" else 0),
        "last_sample_status": status,
        "last_sample_at": float(recorded_at),
    })
    if updates:
        payload["last_measured"] = {
            key: round(max(value) if isinstance(value, (list, tuple))
                       else float(value), 4)
            for key, value in updates.items()
        }
    if sample_count:
        payload["samples_this_cycle"] = int(sample_count)
    derived = _seal_cost_model_from_state(payload)
    for scalar in SEAL_COST_SAMPLE_FIELDS:
        payload[scalar] = derived[scalar]
    payload["stall_guard_active"] = derived["stall_guard_active"]
    payload["fixed_stall_rate"] = derived["fixed_stall_rate"]
    return payload


#: Cold-start per-observation classification cost (p95 estimate), replaced
#: by a measured p95 after enough samples. Classification is admission-
#: controlled like sealing: an observation admitted without enough budget
#: to classify it would die mid-stage with no decision attached.
CLASSIFICATION_COST_SECONDS_DEFAULT = 1.7
CLASSIFICATION_COST_P95_MIN_SAMPLES = 8
#: Completion/ledger headroom reserved OUTSIDE the per-candidate estimate:
#: the cycle must still append its ledger entry and write summaries after
#: classification returns.
CLASSIFICATION_COMPLETION_RESERVE_SECONDS = 3.0
#: Weight on the newest p95 sample for the classification cost estimate.
CLASSIFICATION_COST_SMOOTHING = 0.3
#: Ceiling on how many queued windows one live cycle may pull ahead of fresh
#: ones. The queue must drain, but a deep backlog must never starve the head.
SEAL_QUEUE_DRAIN_LIMIT = 4
# Retirement performs no quote, hash, observation or classification work; its
# cost is one bounded SQLite update after the writer lock is acquired.  The old
# value (120) was sized for remote sealing and merely held a 17k-row terminal
# backlog flat.  A 1,000-row batch amortizes the same lock acquisition while
# remaining short and independently bounded.
BACKFILL_STALE_RETIRE_LIMIT = 1_000
#: Hard ceiling on unclassified rows one classify pass may materialize when
#: no admission limit is supplied (legacy callers). The full backlog is never
#: loaded into Python on any path.
CLASSIFICATION_BACKLOG_SCAN_LIMIT = 500
CLASSIFICATION_COUNTERS_STATE_KEY = "classification_cumulative_counters"
LIVE_LANE_CADENCE_SECONDS = 30.0
ANALYSIS_LANE_CADENCE_SECONDS = 60.0
# A one-minute worker cadence plus a two-chunk ceiling is rate shaping rather
# than an unbounded drain. Raw requests are additionally serialized and yield
# to the 30-second live reservation, so catch-up capacity cannot turn into a
# provider burst on the decision path.
BACKFILL_LANE_CADENCE_SECONDS = 60.0
# Do not start another worker while the latency-critical live worker is
# starting/running or about to become due.  The failed acceptance cohort
# measured a 21.95s live startup while the analysis lane was at its hard
# deadline; launching more interpreters in the same window makes that tail
# worse and is never freshness-positive.
LIVE_LANE_LAUNCH_GUARD_SECONDS = 8.0
# Raw JSON-RPC requests from background lanes are serialized and admitted
# only when the supervisor reports a safe window before the next live pass.
# The socket timeout ends this far before the live reservation, so both the
# in-flight request and the provider's rolling rate bucket can drain before a
# decision-head call.  A two-second guard serialized requests but still let
# two live attempts receive HTTP 429 after back-to-back evidence workers.
BACKGROUND_RPC_LIVE_GUARD_SECONDS = 5.0
BACKGROUND_RPC_MINIMUM_WINDOW_SECONDS = 5.0
# Serialization is not rate limiting. Production measured 83 evidence
# requests admitted in a 90-second lane, bunched into the short windows
# between live passes; live then received two consecutive 429s. Pace all
# background lanes through one shared completion-to-start interval while live
# remains unpaced and preemptive.
BACKGROUND_RPC_MINIMUM_INTERVAL_SECONDS = 0.5
BACKGROUND_RPC_PRIORITY_POLL_SECONDS = 0.1
BACKGROUND_RPC_PRIORITY_MAX_STATE_AGE_SECONDS = 3.0
BACKGROUND_RPC_PRIORITY_STATE_FILE = "rpc_priority_state.json"
BACKGROUND_RPC_RATE_STATE_FILE = "rpc_request_rate_state.json"
PROVIDER_RPC_THROTTLE_STATE_FILE = "rpc_provider_throttle_state.json"
# A 429 is shared provider state, not a property of the worker that happened
# to observe it.  All supervised workers publish and honor one short adaptive
# cooldown.  A live decision-head read may retry after the cooldown; background
# work remains deferred to its next independently scheduled unit.
PROVIDER_RPC_RATE_LIMIT_BASE_COOLDOWN_SECONDS = 1.0
PROVIDER_RPC_RATE_LIMIT_MAX_COOLDOWN_SECONDS = 8.0
PROVIDER_RPC_RATE_LIMIT_STREAK_WINDOW_SECONDS = 60.0
LIVE_DECISION_HEAD_MAXIMUM_ATTEMPTS = 3
BACKGROUND_RPC_SERIALIZED_LANES = frozenset({
    "analysis", "backfill", "evidence", "marks",
})
# A low-priority hard kill can leave Windows closing handles and SQLite
# releasing locks for a short interval.  Delay (not skip) a due live launch
# through that cleanup window so it receives a fresh full budget.
LIVE_LANE_POST_KILL_QUIET_SECONDS = 3.0
# Starting marks, analysis and backfill interpreters together was another
# avoidable import/SQLite burst.  Only the most-overdue background lane may
# start in a tick, and background starts are spaced before the live guard.
BACKGROUND_LANE_LAUNCH_SPACING_SECONDS = 2.0
# Ingestion gets a child deadline which expires this much before the derived
# observation/decision tail.  It may safely retry because the cursor advances
# only after the whole pass commits; it may not consume the completion tail.
LIVE_LANE_INGESTION_MARGIN_SECONDS = 0.5
INGESTION_COST_MODEL_STATE_KEY = "live_ingestion_cost_v1"
INGESTION_COST_SAMPLE_WINDOW = 128
INGEST_EVENT_CHUNK_SIZE = 100
LANE_HEARTBEAT_SECONDS = 5.0
#: How often the supervisor re-runs orphan recovery inside its scheduling
#: loop. The startup-only sweep left rows stale for as long as a supervisor
#: session lasted; one was measured at 90 minutes.
ORPHAN_SWEEP_INTERVAL_SECONDS = 60.0
#: Bounded retry for the seal-queue settlement write when a higher-priority
#: lane holds the database lock. busy_timeout is already 10s, so these are
#: extra waits on top of it, spent only when the lane deadline can afford them.
#: Headroom one outcome observation needs once started. The loop previously
#: checked expiry -- true only when the budget is already gone -- so the item
#: it admitted was the one that overran, and a killed run discards every
#: observation it had already recorded.
ANALYSIS_OUTCOME_ITEM_RESERVE_SECONDS = 6.0
#: Left for the stages AFTER outcomes. Outcomes was handed the whole lane
#: deadline, so on a slow pass it consumed all 120s and `analyses`,
#: `market_rechecks` and `deferred_seals` were killed rather than run.
#: Measured p95: outcomes 78.9s, analyses 71.1s (median 0.13s -- a rare but
#: very long tail), certificate_refresh 18.9s, rechecks 3.9s.
ANALYSIS_DOWNSTREAM_RESERVE_SECONDS = 20.0
#: Blocks per second used to convert the freshness bound into a time budget.
#: The planning rate (20.5) is a deliberate MINIMUM for capacity sizing; using
#: it here yields a 5.85s budget that no configuration can meet. Observed lag
#: against observed stage times puts the real rate near 10/s.
FLOW_OBSERVED_BLOCKS_PER_SECOND = 10.0
#: Ingestion may be squeezed by the freshness budget but never to nothing. A
#: bad estimate must degrade scan width, not stop the lane -- an earlier
#: version without this floor computed zero and would have deferred forever.
LIVE_LANE_MINIMUM_INGESTION_SECONDS = 3.0
#: Ingestion's bulk event write competes with every other lane for the single
#: SQLite writer. busy_timeout is already 10s and the lock still outlived it
#: twice in one 728-attempt cohort, on the decision-critical path.
INGEST_LOCK_RETRY_ATTEMPTS = 3
INGEST_LOCK_RETRY_BACKOFF_SECONDS = 0.4
POSITION_TERMINAL_NO_MARKET_OBSERVATIONS = 3
SETTLEMENT_LOCK_RETRY_ATTEMPTS = 3
SETTLEMENT_LOCK_RETRY_BACKOFF_SECONDS = 0.5
#: A controlled deferral is healthy, but a lane that defers most of its cycles
#: is not producing decisions even though nothing crashed. Reliability counts
#: deferrals as healthy only while they stay under this share.
LIVE_DEFERRAL_RATE_MAX = 0.20
LANE_TERMINATION_GRACE_SECONDS = 3.0
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
# Cohort 5 proved that the Cohort 4 reserve was not conservative enough across
# chain conditions: the post-ingestion tail reached 67 blocks at p99/max.
# Reserve that measured tail before optional origin enrichment; the public
# 120-block decision gate is deliberately unchanged.
FLOW_DOWNSTREAM_HEAD_RESERVE_BLOCKS = 67
FLOW_PRESEAL_MAXIMUM_HEAD_LAG_BLOCKS = (
    FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
    - FLOW_DOWNSTREAM_HEAD_RESERVE_BLOCKS
)
# Final observation admission is also block-budgeted. The first clean v2
# cohort measured the complete post-ingestion tail at p99/max: 36 blocks for
# one observation and 59 for two. A single 67-block reserve for both would
# have pushed controlled deferrals above their frozen 20% limit. These
# baselines are tighten-only; a bounded live p99 can raise, never lower, them.
DECISION_TAIL_BLOCK_MODEL_STATE_KEY = "live_decision_tail_blocks_v1"
DECISION_TAIL_BLOCK_MODEL_EPOCH = 1
DECISION_TAIL_BLOCK_SAMPLE_WINDOW = 128
DECISION_TAIL_BLOCK_DEFAULTS = {0: 25, 1: 36, 2: 59}
DECISION_TAIL_CIRCUIT_STATE_KEY = "live_decision_tail_circuit_v1"
DECISION_TAIL_CIRCUIT_EPOCH = 1
DECISION_MULTI_OBSERVATION_COOLDOWN_ATTEMPTS = 20
DECISION_SINGLE_PROBE_SUCCESSES_REQUIRED = 3
#: Quantile for the block-tail reserve. LOWERED from p99 on explicit operator
#: approval, 2026-08-29, after measuring that nearest-rank p99 cannot behave
#: as a percentile on this window: at n=82 it selects the maximum and at the
#: full n=128 the second largest. The reserve had ratcheted to 118 blocks
#: against a 120-block bound on a median cost of 20 -- set by exactly one
#: sample -- so at most one observation was ever admissible and usually none.
#: Measured across 82 and 33 samples: p90 gives 71 and 96, p95 gives 82 and
#: 113, p99 gives 118 and 157.
#:
#: This LOOSENS an admission gate. The max(baseline, ...) floor below keeps it
#: from ever falling under the author's static defaults.
DECISION_TAIL_BLOCK_QUANTILE = 0.90
# Two v3 decisions breached the 120-block SLO at 125 and 190 blocks after a
# two-observation batch was admitted at 60 + 59: one nominal block of slack.
# Counterfactual replay shows a ten-block margin would have reduced 30 such
# batches to one observation without creating any additional zero-admission
# cycles.  Apply it only to multi-observation batches; the one-observation
# reserve already covered the cohort maximum (36 reserved, 35 observed).
DECISION_MULTI_OBSERVATION_SAFETY_BLOCKS = 10
# The old blanket five-second pre-head cutoff duplicated downstream reserves
# and discarded three finishable decisions. Head retrieval receives its own
# bounded allowance; classification and ledger completion are sized below.
# The normal read remains small. Adaptive throttle retries are admitted only
# from spare headroom *above* the independently protected downstream reserve;
# charging their rare worst case up front starved ingestion and reduced its
# socket timeout to 2.04s in every cycle.
DECISION_HEAD_RPC_RESERVE_SECONDS = 0.5
# A rate floor prevents a quiet first few seconds from making the plan
# optimistic. Cohort 5's enrichment-rate LOWER BOUND was 18.51 blocks/s at
# p95 and 20.11 at max (using the admitted budget as the denominator, which
# can only understate the real rate). Round upward rather than fitting it.
FLOW_MINIMUM_PLANNING_BLOCKS_PER_SECOND = 20.5
# A first origin batch has no local cost history. Cohort 4 observed a 2.156s
# maximum batch, so a smaller allowance cannot honestly claim to bound it.
# LOWERED 2.25 -> 0.5 on operator approval, 2026-08-30, after measuring what
# a batch actually costs. Live enrichment passes that completed: 60 origins
# resolved in 0.203-0.250s with zero failures, five times out of six. At
# FLOW_ORIGIN_BATCH_SIZE 25 that puts one batch near 0.08-0.10s, so the old
# floor demanded roughly 20x a batch and 10x the whole 60-origin stage.
#
# The consequence was not a slow gate but a closed one. The budget is
# (53 - lag) / 20.5 seconds, peaking at 2.585s with zero lag, so a 2.25s floor
# admitted enrichment only below 7 blocks of head lag -- against an observed
# median of 15. It ran 2 times in 187 cycles, and every window sealed in that
# period carries no resolved participants, leaving the participant gate
# evaluating absent evidence.
#
# 0.5s still refuses beyond ~42 blocks of lag, so a genuinely late cycle
# starts nothing. The one slow pass observed (1.969s for 25 origins on a slow
# provider) is what the old floor was sized for; the deadline and
# deferred_for_deadline accounting bound that case rather than this floor.
FLOW_MINIMUM_FIRST_ORIGIN_BATCH_SECONDS = 0.5
# Cold-start enrichment has no trustworthy cost sample.  A five-origin probe
# bounds the first provider call; successful probes may then use the normal
# batch size.  This keeps the live lane preemptible without turning a slow
# first request into evidence loss -- results fetched after the child deadline
# are discarded and remain unresolved for a later lane.
FLOW_ORIGIN_COLD_BATCH_SIZE = 5
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
#: What an outcome that could not be priced at its horizon is worth to the
#: evaluation. The store already records these as total losses -- a strategy
#: that cannot exit has lost the notional -- and comparisons must use the same
#: number, or an arm with heavy attrition scores itself on its survivors.
FLOW_UNEXITABLE_RETURN = -1.0
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
# How far back each near-head pass FETCHES. Distinct from FLOW_WINDOW_BLOCKS,
# which is how wide an analytic window is -- conflating them would change the
# measurement and invalidate cohort-004, exactly as a 450-to-1350 change
# retired cohort-002.
#
# Measured across 195 sealed passes spanning 1,714,814 blocks: each pass
# scanned exactly 1,350 blocks while the median gap to the next pass was 6,960
# and the maximum 22,515. Total coverage 15.1% -- five blocks in six were read
# by nothing, and the backfill crawler meant to cover them is over a million
# blocks behind. A pool registered on sight then trading outside a scanned
# stretch is never observed, which is what happened to every pool of the token
# the operator asked about: discovered, zero swaps ingested, no window, no
# observation.
#
# Sized to the observed stride rather than a round number: median 6,960, and
# gaps ran 9,000-13,000 once cycles lengthened. 12,000 covers the median with
# room and overlaps rather than gapping, since re-reading a block is harmless
# and missing one is not.
# Raised from 12,000 after the cap fired on EVERY pass: consecutive passes
# left gaps of 618, 981, 1,020, 1,040 and 10,605 blocks, so the stride exceeds
# 12,000 and the cap was silently truncating the oldest end of each range.
# Observed gaps across 195 passes: median 6,960, p90 12,691, max 22,515 --
# 12,000 leaves 10.3% uncovered, 20,000 leaves 1.0%, 25,000 leaves none.
# Chunked fetch keeps the cost flat: 25,000 blocks measured in a few seconds.
class CycleDeadline:
    """One monotonic hard deadline, shared by every stage of a cycle.

    Stages previously each computed their own bound from wall clock and
    checked it only BETWEEN units of work, so a deadline prevented the next
    step without interrupting the current one. Measured consequences: an
    identity stage with a 60-second budget ran 268.4s and then 472.8s; a
    queue census with no bound at all cost 150s of that; a single get_logs
    could overrun any budget it started inside.

    monotonic, not wall clock: the system clock moved during this project and
    a wall-clock deadline would have jumped with it.

    remaining() is what a caller passes to a socket or an RPC timeout so the
    bound reaches the blocking call itself. expired() is the cheap check for
    loop heads. sqlite_guard() installs a progress handler so a long query is
    ABORTED rather than merely not started -- that is the difference between
    a deadline and a suggestion.
    """

    __slots__ = ("started", "seconds", "deadline", "_expired_at")

    def __init__(self, seconds: float,
                 deadline_monotonic: float | None = None):
        self.seconds = max(0.0, float(seconds))
        now = time.monotonic()
        if deadline_monotonic is None:
            self.started = now
            self.deadline = now + self.seconds
        else:
            # The supervisor starts this clock BEFORE spawning the child.
            # Reconstructing started from the shared absolute monotonic
            # deadline makes import/database startup part of the same budget
            # instead of giving the child a second, later 25-second window.
            self.deadline = float(deadline_monotonic)
            self.started = self.deadline - self.seconds
        self._expired_at: float | None = None

    def remaining(self) -> float:
        return max(0.0, self.deadline - time.monotonic())

    def expired(self) -> bool:
        if self.remaining() > 0:
            return False
        if self._expired_at is None:
            self._expired_at = time.monotonic()
        return True

    def raise_if_expired(self, stage: str) -> None:
        if self.expired():
            raise CycleDeadlineExceeded(stage)

    @contextmanager
    def sqlite_guard(self, connection, instructions: int = 50_000):
        """Abort an in-flight query when the deadline passes.

        SQLite calls the progress handler every `instructions` VM steps and
        aborts the statement if it returns non-zero. Without this a single
        range-join could hold a stage long past its budget -- one measured at
        44.7s inside a 60-second cycle stage that also had other work to do.
        """
        def _abort():
            return 1 if self.expired() else 0

        connection.set_progress_handler(_abort, instructions)
        try:
            yield connection
        finally:
            connection.set_progress_handler(None, instructions)


class CycleDeadlineExceeded(RuntimeError):
    """A stage was interrupted because the cycle deadline passed."""


class BackgroundRpcPriorityDeferred(RuntimeError):
    """Background RPC work could not fit outside a live reservation."""


def _background_rpc_priority_window(
    state: dict, *, now_monotonic: float | None = None,
) -> dict:
    """Classify one supervisor-published background RPC window.

    The state is deliberately tiny and monotonic-clock based. A stale or
    missing publication fails closed for background traffic; live traffic
    never consults it. ``available_seconds`` already excludes the guard that
    belongs exclusively to the upcoming live request.
    """
    now = time.monotonic() if now_monotonic is None else float(now_monotonic)
    published = safe_float(state.get("published_monotonic"), 0.0)
    next_live = safe_float(state.get("next_live_monotonic"), 0.0)
    age = now - published if published > 0 else None
    available = (
        next_live - now - BACKGROUND_RPC_LIVE_GUARD_SECONDS
        if next_live > 0 else 0.0)
    if age is None or age < -1.0 or age > (
            BACKGROUND_RPC_PRIORITY_MAX_STATE_AGE_SECONDS):
        reason = "priority_state_stale"
    elif bool(state.get("live_active")):
        reason = "live_lane_active"
    elif available < BACKGROUND_RPC_MINIMUM_WINDOW_SECONDS:
        reason = "live_lane_imminent"
    else:
        reason = "safe_background_window"
    return {
        "admitted": reason == "safe_background_window",
        "reason": reason,
        "available_seconds": round(max(0.0, available), 6),
        "state_age_seconds": (
            round(age, 6) if age is not None else None),
        "next_live_monotonic": next_live or None,
    }


def _next_lane_launch_after_start(now_monotonic: float, cadence: float) -> float:
    """Rebase cadence on the actual launch; never replay scheduling debt.

    A delayed lane used to retain the old phase grid. A live pass delayed to
    second 43 would therefore launch again at second 60: only 17 seconds
    later. Production then measured 12-24 second live start intervals, RPC
    bursts, slow interpreter startup and SQLite commit contention. Missed
    cadence is telemetry, not work that can be recovered by starting sooner.
    """
    return float(now_monotonic) + max(0.001, float(cadence))


FLOW_NEAR_HEAD_SCAN_BLOCKS = 25_000
# One get_logs call cannot span the whole range. The endpoint rejects a query
# whose RESULT SET is too large -- "[RPC -32000] logs matched by query exceeds
# limit" -- and a 12,000-block span returns roughly 23,000 swap logs against a
# 5,000-block span's measured 9,533. Widening the scan without chunking took
# coverage from 15.1% to ZERO for an hour: every pass failed and ingested
# nothing. The benchmark that justified the widening measured 5,000 blocks and
# I extrapolated the span without checking the log ceiling.
#
# So the range is fetched in chunks that stay under the limit and concatenated.
# The cursor still drives the total span; this only bounds each request.
FLOW_NEAR_HEAD_FETCH_CHUNK_BLOCKS = 4_000
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
#: Horizons scheduled for OBSERVATIONS. Deliberately narrower than
#: FLOW_EVIDENCE_HORIZONS, which still governs the event path.
#:
#: Nothing reads the other four. flow_observation_outcomes has exactly two
#: consumers -- cohort_progress and signal_versus_control -- and both filter to
#: FLOW_PRIMARY_HORIZON_LABEL. The promotion gates do not read this table at
#: all; they read flow_signal_outcomes on the event path. So 1m, 5m, 1h and 6h
#: were scheduled, resolved at real cost, and consumed by no decision.
#:
#: Five horizons meant 3,605 seals a day promised 18,025 outcome prices
#: against ~3,072-4,008 of measured capacity: the backlog grew +16,438/day to
#: 301,208, oldest 9.9 days past due, and worsened after two correct fixes
#: because restoring fresh sealing raised the promise faster than repairing
#: the evidence lane raised delivery. One horizon promises 3,605/day, which
#: capacity covers.
#:
#: This changes no contract: every sealed observation still gets a measurable
#: outcome. What is lost is the return CURVE across horizons -- diagnostic
#: only, though genuinely useful once: returns being flat from 1m to 6h is
#: what identified friction-dominance. Widen this tuple to recover it, at the
#: cost of convergence, and knowing capacity must rise with it.
FLOW_OBSERVATION_HORIZONS = (
    (FLOW_PRIMARY_HORIZON_LABEL, 15 * 60),
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
ERC20_DECIMALS_SELECTOR = "313ce567"
#: Distinguishes "the prime batch cached a null result" from "not cached".
#: None is a legitimate eth_call result here, so it cannot serve as the miss.
_CACHE_MISS = object()
ERC20_TOTAL_SUPPLY_SELECTOR = "18160ddd"
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


def operational_acceptance_policy(sample_target: int = 100) -> dict:
    """The immutable safety/SLO policy pinned to an acceptance cohort.

    A cohort is evidence only when its target and thresholds cannot drift
    underneath it.  This is deliberately operational policy; strategy
    promotion remains a separate fail-closed gate.
    """
    target = max(1, int(sample_target))
    return {
        "schema_version": ACCEPTANCE_COHORT_SCHEMA_VERSION,
        "policy_version": ACCEPTANCE_COHORT_POLICY_VERSION,
        "sample_target": target,
        "paper_only": True,
        "live_execution_enabled": False,
        "live_lane_budget_seconds": LIVE_LANE_BUDGET_SECONDS,
        "live_lane_cadence_seconds": LIVE_LANE_CADENCE_SECONDS,
        "lane_cadence_debt_replay": False,
        "live_scan_blocks": LIVE_LANE_SCAN_BLOCKS,
        "live_enrichment_limit": LIVE_LANE_ENRICHMENT_LIMIT,
        "live_enrichment_budget_seconds": (
            LIVE_LANE_ENRICHMENT_BUDGET_SECONDS),
        "backfill_lane_cadence_seconds": BACKFILL_LANE_CADENCE_SECONDS,
        "scheduled_backfill_block_limit": BACKFILL_GAP_CHUNK_BLOCKS,
        "background_rpc_priority_gate_version": 2,
        "background_rpc_serialized": True,
        "background_rpc_live_guard_seconds":
            BACKGROUND_RPC_LIVE_GUARD_SECONDS,
        "background_rpc_minimum_window_seconds":
            BACKGROUND_RPC_MINIMUM_WINDOW_SECONDS,
        "background_rpc_minimum_interval_seconds":
            BACKGROUND_RPC_MINIMUM_INTERVAL_SECONDS,
        "provider_rpc_shared_throttle_state": True,
        "provider_rpc_rate_limit_base_cooldown_seconds":
            PROVIDER_RPC_RATE_LIMIT_BASE_COOLDOWN_SECONDS,
        "provider_rpc_rate_limit_max_cooldown_seconds":
            PROVIDER_RPC_RATE_LIMIT_MAX_COOLDOWN_SECONDS,
        "live_decision_head_maximum_attempts":
            LIVE_DECISION_HEAD_MAXIMUM_ATTEMPTS,
        "decision_head_rpc_reserve_seconds":
            DECISION_HEAD_RPC_RESERVE_SECONDS,
        "evidence_completion_reserve_seconds":
            EVIDENCE_COMPLETION_RESERVE_SECONDS,
        "evidence_stage_max_seconds": EVIDENCE_STAGE_MAX_SECONDS,
        "live_all_attempt_p95_target_seconds": 30.0,
        "decision_lag_maximum_blocks":
            FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
        "decision_lag_minimum_rate": 0.99,
        "decision_usefulness_minimum_rate": 0.99,
        # The cohort boundary is exactly N terminal LIVE attempts. Decisions
        # are conditional on a qualifying opportunity and marks have their
        # own slower cadence, so requiring N of either inside N live attempts
        # is impossible by construction. Freeze independent minimums instead
        # of changing denominators after observing a cohort.
        "decision_minimum_samples": max(
            1, math.ceil(target * ACCEPTANCE_DECISION_SAMPLE_FRACTION)),
        "position_mark_minimum_samples": max(
            1, math.ceil(
                target * ACCEPTANCE_POSITION_MARK_SAMPLE_FRACTION)),
        "position_mark_minimum_rate": 1.0,
        "seal_stall_rate_maximum": SEAL_STALL_RATE_MAX,
        "seal_stall_minimum_samples": SEAL_STALL_GUARD_MIN_SAMPLES,
        "seal_cost_model_epoch": SEAL_COST_MODEL_EPOCH,
        "decision_tail_block_model_epoch":
            DECISION_TAIL_BLOCK_MODEL_EPOCH,
        "decision_tail_circuit_epoch": DECISION_TAIL_CIRCUIT_EPOCH,
        "decision_head_rate_limit_policy":
            "fail_fast_when_observations_are_sealed",
        "decision_multi_observation_cooldown_attempts":
            DECISION_MULTI_OBSERVATION_COOLDOWN_ATTEMPTS,
        "decision_single_probe_successes_required":
            DECISION_SINGLE_PROBE_SUCCESSES_REQUIRED,
        "decision_multi_observation_safety_blocks":
            DECISION_MULTI_OBSERVATION_SAFETY_BLOCKS,
        "backfill_maximum_chunks_per_cycle":
            BACKFILL_MAXIMUM_CHUNKS_PER_CYCLE,
        "backfill_remote_attempts_per_chunk":
            BACKFILL_REMOTE_ATTEMPTS_PER_CHUNK,
        "backfill_recovery_target_ratio":
            BACKFILL_RECOVERY_TARGET_RATIO,
        "backfill_rpc_attempt_budget_seconds":
            BACKFILL_RPC_ATTEMPT_BUDGET_SECONDS,
        "backfill_launch_minimum_live_window_seconds":
            BACKFILL_LAUNCH_MINIMUM_LIVE_WINDOW_SECONDS,
        "backfill_rpc_isolation_supported": True,
        "backfill_rpc_chunk_model_epoch":
            BACKFILL_RPC_CHUNK_MODEL_EPOCH,
        "backfill_initial_chunk_blocks":
            BACKFILL_GAP_INITIAL_CHUNK_BLOCKS,
        "backfill_probe_step_blocks":
            BACKFILL_RPC_PROBE_STEP_BLOCKS,
        "backfill_successes_before_probe":
            BACKFILL_RPC_SUCCESSES_BEFORE_PROBE,
        "backfill_state_revision_bound": True,
        "backfill_minimum_chunk_blocks":
            BACKFILL_GAP_MINIMUM_CHUNK_BLOCKS,
        "live_observation_limit": LIVE_LANE_OBSERVATION_LIMIT,
        "live_decision_reserve_seconds":
            LIVE_LANE_DECISION_RESERVE_SECONDS,
    }


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


def _spread(values: list[float]) -> dict:
    """A distribution, never a mean.

    One stalled window and seven fast ones produce the same total as eight
    even ones, and the two need opposite fixes -- bound the stall, or admit
    fewer windows. p95 is reported beside the median for the same reason the
    lane SLO is stated as a percentile: the tail is what breaks the budget.
    """
    if not values:
        return {"count": 0, "total": 0.0, "median": None, "p95": None,
                "slowest": None}
    ordered = sorted(values)
    p95_index = min(len(ordered) - 1, max(0, math.ceil(0.95 * len(ordered)) - 1))
    return {
        "count": len(ordered),
        "total": round(sum(ordered), 3),
        "median": round(ordered[len(ordered) // 2], 3),
        "p95": round(ordered[p95_index], 3),
        "slowest": round(ordered[-1], 3),
    }


def _timestamp(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _rpc_rate_limited(error: BaseException | str) -> bool:
    """Return whether retrying now would amplify provider pressure."""
    text = str(error).lower()
    return bool(
        "rpc -429" in text
        or "response failed (429)" in text
        or "too many requests" in text
    )


def _provider_cooldown_remaining(
    state: dict, *, now_monotonic: float | None = None,
) -> float:
    """Return a reboot-safe shared provider cooldown remainder."""
    now = time.monotonic() if now_monotonic is None else float(now_monotonic)
    until = safe_float(state.get("cooldown_until_monotonic"), 0.0)
    remaining = until - now
    # Persisted monotonic timestamps belong to one OS boot.  A negative value
    # is expired; an implausibly large value is from a previous boot/corruption
    # and must not stop the learner indefinitely.
    if remaining <= 0 or remaining > (
            PROVIDER_RPC_RATE_LIMIT_MAX_COOLDOWN_SECONDS + 1.0):
        return 0.0
    return remaining


def _transient_rpc_failure(error: BaseException | str) -> bool:
    """Classify transport/provider failures without hiding deterministic bugs."""
    text = str(error).lower()
    return bool(
        isinstance(error, BackgroundRpcPriorityDeferred)
        or _rpc_rate_limited(error)
        or "rpc -2" in text
        or "timed out" in text
        or "timeout" in text
        or "cannot connect" in text
        or "transport failed" in text
        or "connection" in text
        or " eof" in f" {text}"
        or "connection reset" in text
        or "remote host closed" in text
    )


def _remote_call(operation: str, callback, *, attempts: int = REMOTE_RETRY_ATTEMPTS):
    """Retry a bounded remote read without changing any durable cursor state."""
    last_error: Exception | None = None
    attempts_made = 0
    for attempt in range(max(1, attempts)):
        attempts_made = attempt + 1
        try:
            return callback()
        except BackgroundRpcPriorityDeferred:
            # Scheduling policy is not a provider failure. Retrying inside the
            # same live reservation would only burn the lane deadline and
            # erase the controlled-deferral type used by the supervisor.
            raise
        except Exception as exc:
            last_error = exc
            # A 429 is an instruction to send LESS traffic. Immediate retries
            # consumed the provider bucket and collided with authoritative
            # live decision-head reads. The scheduled next cycle is the retry.
            if _rpc_rate_limited(exc):
                break
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
        f"{operation} failed after {attempts_made} attempts: {last_error}"
    ) from last_error


class _HashLedgerAppendLock:
    """Blocking OS byte-range lock shared by all lane processes."""

    def __init__(self, path: Path, timeout_seconds: float = 30.0):
        self.path = path
        self.timeout_seconds = float(timeout_seconds)
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + self.timeout_seconds
        # The byte being locked must exist before any process opens it for
        # locking.  The old touch/open/size/write sequence let two Windows
        # processes both observe an empty file; one then locked byte 0 while
        # the other flushed its initializer into that locked range and died
        # with PermissionError.  O_EXCL gives exactly one bootstrap writer;
        # contenders wait until its one-byte initialization is durable.
        try:
            descriptor = os.open(
                self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            descriptor = None
        if descriptor is not None:
            try:
                os.write(descriptor, b" ")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        while True:
            try:
                if self.path.stat().st_size >= 1:
                    self.handle = self.path.open("r+b")
                    break
            except (FileNotFoundError, PermissionError, OSError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError(
                    f"event-ledger lock initialization timed out: {self.path}")
            time.sleep(0.01)
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    self.handle.seek(0)
                    msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(
                        self.handle.fileno(),
                        fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (OSError, BlockingIOError):
                if time.monotonic() >= deadline:
                    self.handle.close()
                    self.handle = None
                    raise TimeoutError(
                        f"event-ledger append lock timed out: {self.path}")
                time.sleep(0.05)

    def __exit__(self, _exc_type, _exc, _tb):
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


class HashEventLedger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = self.path.with_name(self.path.name + ".append.lock")
        self.integrity_epoch_path = self.path.with_name(
            self.path.name + ".integrity_epoch.json")

    def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def _tail_event(self) -> dict | None:
        """Read the last complete JSONL record without loading 48+ MB."""
        if not self.path.exists() or self.path.stat().st_size == 0:
            return None
        with self.path.open("rb") as handle:
            handle.seek(0, os.SEEK_END)
            end = handle.tell()
            position = end
            data = b""
            while position > 0:
                step = min(65536, position)
                position -= step
                handle.seek(position)
                data = handle.read(step) + data
                for raw in reversed(data.split(b"\n")):
                    if not raw.strip():
                        continue
                    try:
                        return json.loads(raw.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        break
        raise ValueError("event ledger tail is not valid UTF-8 JSONL")

    def append(self, event_type: str, payload: dict) -> dict:
        # Each supervised lane is a separate process. Without this shared
        # lock, two writers can read the same tail and append sibling events
        # with duplicate indices. The 2026-08-23 production ledger contains
        # exactly that historical fork; this prevents any new one.
        with _HashLedgerAppendLock(self.lock_path):
            tail = self._tail_event()
            event = {
                "index": int(tail["index"]) + 1 if tail else 0,
                "event_type": event_type,
                "timestamp": _utc_now(),
                "previous_hash": (
                    tail["event_hash"] if tail else "0" * 64),
                "payload": payload,
            }
            event["event_hash"] = hashlib.sha256(
                _canonical(event).encode("utf-8")
            ).hexdigest()
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, sort_keys=True) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            return event

    def verify(self) -> tuple[bool, str]:
        if not self.path.exists():
            return True, "verified 0 Robinhood learning events"
        epoch = read_json(self.integrity_epoch_path, {}) or {}
        legacy_count = safe_int(epoch.get("legacy_event_count"), 0)
        legacy_digest = hashlib.sha256()
        seen_hashes: set[str] = set()
        child_counts: dict[str, int] = {}
        previous_physical_hash = "0" * 64
        previous_event_index = -1
        legacy_anomalies = 0
        legacy_tail_hash = None
        count = 0
        with self.path.open("rb") as handle:
            for physical_index, raw in enumerate(handle):
                if not raw.strip():
                    continue
                if physical_index < legacy_count:
                    legacy_digest.update(raw)
                try:
                    event = json.loads(raw.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return False, f"invalid JSONL at {physical_index}"
                expected = hashlib.sha256(_canonical({
                    key: value for key, value in event.items()
                    if key != "event_hash"
                }).encode("utf-8")).hexdigest()
                event_hash = str(event.get("event_hash") or "")
                if event_hash != expected:
                    return False, f"event hash mismatch at {physical_index}"
                predecessor = str(event.get("previous_hash") or "")
                if physical_index == 0:
                    if predecessor != "0" * 64:
                        return False, "genesis previous hash mismatch"
                elif predecessor not in seen_hashes:
                    return False, f"unknown previous hash at {physical_index}"
                event_index = safe_int(event.get("index"), -1)
                index_anomaly = not (
                    previous_event_index <= event_index <= physical_index)
                link_anomaly = (
                    physical_index > 0
                    and predecessor != previous_physical_hash)
                if index_anomaly or link_anomaly or event_index != physical_index:
                    if not epoch or physical_index >= legacy_count:
                        return False, (
                            f"non-linear event after integrity epoch at "
                            f"{physical_index}")
                    legacy_anomalies += 1
                child_counts[predecessor] = child_counts.get(predecessor, 0) + 1
                seen_hashes.add(event_hash)
                previous_physical_hash = event_hash
                previous_event_index = event_index
                count = physical_index + 1
                if count == legacy_count:
                    legacy_tail_hash = event_hash
        if epoch:
            if count < legacy_count:
                return False, "event ledger shorter than integrity epoch"
            if legacy_digest.hexdigest() != str(
                    epoch.get("legacy_prefix_sha256") or ""):
                return False, "legacy event prefix digest mismatch"
            if legacy_tail_hash != str(epoch.get("legacy_tail_event_hash") or ""):
                return False, "legacy event tail mismatch"
        forks = sum(1 for amount in child_counts.values() if amount > 1)
        suffix = (
            f"; anchored legacy anomalies={legacy_anomalies}, forks={forks}"
            if legacy_anomalies else "")
        return True, (
            f"verified {count} Robinhood learning events" + suffix)


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

    def tail_ring_count(self) -> int:
        """Return the true durable producer ring count in O(1) time.

        Timechain's tail reader seeks backward from the JSONL file end and
        therefore observes rings written by this process, a restarted
        process, or another legitimate writer without a full-chain scan.
        """
        head = self.tc._current_head()
        return int(head["index"]) + 1 if head is not None else 0

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

    def _connect(self, busy_timeout_ms: int | None = None) -> sqlite3.Connection:
        if self.read_only:
            # A reader in WAL mode never blocks on a writer, so the dashboard
            # stays responsive mid-cycle instead of queueing behind it.
            connection = sqlite3.connect(
                f"file:{self.path.as_posix()}?mode=ro", uri=True, timeout=10,
            )
        else:
            connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        timeout_ms = 10000 if busy_timeout_ms is None else max(
            0, int(busy_timeout_ms))
        connection.execute(f"PRAGMA busy_timeout={timeout_ms}")
        return connection

    @contextmanager
    def connection(self, busy_timeout_ms: int | None = None):
        connection = self._connect(busy_timeout_ms=busy_timeout_ms)
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
                    decision_commitment_id TEXT,
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
                CREATE TABLE IF NOT EXISTS acceptance_cohorts (
                    cohort_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    started_at TEXT NOT NULL,
                    revision TEXT NOT NULL,
                    source_digest TEXT,
                    sample_target INTEGER NOT NULL,
                    policy_json TEXT NOT NULL,
                    policy_hash TEXT NOT NULL,
                    status TEXT NOT NULL,
                    closed_at TEXT,
                    close_reason TEXT
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
                -- Remote execution quotes are evidence enrichment, not part
                -- of observing the chain head.  Keeping them in a companion
                -- table preserves the immutable observation while allowing
                -- the evidence lane to attach block-pinned entry and
                -- decision quotes asynchronously.
                CREATE TABLE IF NOT EXISTS flow_observation_quotes (
                    observation_id TEXT PRIMARY KEY,
                    entry_quote_json TEXT NOT NULL,
                    entry_quote_block INTEGER,
                    entry_quote_verified INTEGER NOT NULL DEFAULT 0,
                    decision_quote_json TEXT,
                    decision_quote_block INTEGER,
                    decision_quote_verified INTEGER NOT NULL DEFAULT 0,
                    captured_at TEXT NOT NULL,
                    FOREIGN KEY(observation_id)
                        REFERENCES flow_observations(observation_id)
                );
                CREATE INDEX IF NOT EXISTS idx_flow_observation_quotes_entry
                    ON flow_observation_quotes(entry_quote_verified,captured_at);
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
                -- Classification selection walks "unclassified observations
                -- for this policy, oldest sealed first". Without this index
                -- that ordering scans the whole cohort; with it the bounded
                -- LIMIT ? query touches only the rows it returns.
                CREATE INDEX IF NOT EXISTS idx_flow_observations_policy_sealed
                    ON flow_observations(policy_version, sealed_at,
                                         observation_id);
                -- "Is this window already sealed?" is asked by the window
                -- selection query, the seal queue drain and the queue depth,
                -- and every one of them expressed it as a correlated NOT
                -- EXISTS with nothing to answer it from. The only index on
                -- this table was the implicit one on observation_id, so each
                -- ask cost a full scan: measured at 141 seconds for 443 queue
                -- rows against 17,098 observations, inside a 25-second cycle.
                -- The queue did not create that cost, it only asked the
                -- question often enough to expose it.
                CREATE INDEX IF NOT EXISTS idx_flow_obs_window
                    ON flow_observations(pool_id,window_end_block,
                                         policy_version);
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
                # Why this observation does or does not carry a scheduled
                # outcome. An observation with no outcome must never be
                # mistakable for one whose outcome was lost.
                "outcome_schedule_state": "TEXT",
            }.items():
                if name not in observation_columns:
                    connection.execute(
                        f"ALTER TABLE flow_observations ADD COLUMN {name} {decl}"
                    )
            # AFTER the ALTER above, never in the CREATE block: `role` is a
            # migrated column, so indexing it beside the table definition
            # raises "no such column: role" on every fresh database. That
            # exact mistake shipped once before with the headroom columns and
            # broke the whole test suite.
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_flow_obs_role"
                " ON flow_observations(role)")
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
            connection.executescript(
                """
                -- Blocks the LIVE lane deliberately skipped so it could stay
                -- near the head. Today the cap drops them: scan_capped fires,
                -- blocks_skipped_by_cap is recorded, and the range is gone.
                -- Recording a number is not a queue -- nothing can consume it,
                -- so the live lane's only options are to fall behind or to
                -- lose history. Enqueueing makes re-anchoring safe: the live
                -- lane jumps to the head and the range survives for a slower
                -- lane to drain.
                CREATE TABLE IF NOT EXISTS flow_backfill_queue (
                    from_block INTEGER NOT NULL,
                    to_block INTEGER NOT NULL,
                    enqueued_at REAL NOT NULL,
                    reason TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    completed_at REAL,
                    PRIMARY KEY (from_block, to_block)
                );
                CREATE INDEX IF NOT EXISTS idx_backfill_pending
                    ON flow_backfill_queue(completed_at, enqueued_at);
                -- Windows the seal stage admitted but could not reach before
                -- the cycle reserve. Without this they are only implicitly
                -- retried: selection re-finds an unsealed window while it
                -- still sits above the observation floor, and silently stops
                -- finding it once the head moves on. That is a lossy retry
                -- dressed as a durable one -- the same shape as the block
                -- ranges above, where recording a skipped count was mistaken
                -- for queueing the range.
                --
                -- The row carries the WINDOW, not a pointer to it.
                -- flow_signals is keyed by pool_id alone -- one row per pool,
                -- replaced on every recompute -- so a queued
                -- (pool_id, window_end_block) stopped resolving as soon as
                -- that pool traded again. Measured: 432 entries pending, the
                -- oldest 113 minutes old, and zero drained, because not one
                -- of them still matched a flow_signals row. A queue whose
                -- entries cannot be acted on is the leak it was built to
                -- prevent, so features_json is snapshotted at defer time and
                -- the entry is self-sufficient.
                CREATE TABLE IF NOT EXISTS flow_seal_queue (
                    pool_id TEXT NOT NULL,
                    window_end_block INTEGER NOT NULL,
                    token_address TEXT,
                    window_start_block INTEGER,
                    features_json TEXT,
                    enqueued_at REAL NOT NULL,
                    reason TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_attempt_at REAL,
                    completed_at REAL,
                    queue_state TEXT NOT NULL DEFAULT 'pending',
                    PRIMARY KEY (pool_id, window_end_block)
                );
                CREATE INDEX IF NOT EXISTS idx_seal_queue_pending
                    ON flow_seal_queue(completed_at, enqueued_at);
                -- Small scalars the scheduler needs before it can do any
                -- work, kept beside the data they describe rather than in a
                -- file next to it: admission has to read the cost estimate on
                -- every cycle, and a second durability story for one float is
                -- one more thing that can disagree with the database.
                CREATE TABLE IF NOT EXISTS flow_scheduler_state (
                    key TEXT PRIMARY KEY,
                    value_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS seal_cost_samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    recorded_at REAL NOT NULL,
                    epoch INTEGER NOT NULL,
                    run_id TEXT,
                    revision TEXT,
                    acceptance_cohort_id TEXT,
                    component TEXT NOT NULL,
                    status TEXT NOT NULL,
                    seconds REAL,
                    sample_count INTEGER NOT NULL DEFAULT 1
                );
                CREATE INDEX IF NOT EXISTS idx_seal_cost_cohort
                    ON seal_cost_samples(
                        acceptance_cohort_id,revision,run_id,component,status);
                CREATE TABLE IF NOT EXISTS lane_state (
                    lane TEXT PRIMARY KEY,
                    run_id TEXT,
                    pid INTEGER,
                    status TEXT NOT NULL,
                    started_at REAL,
                    heartbeat_at REAL,
                    completed_at REAL,
                    deadline_seconds REAL,
                    cursor_json TEXT NOT NULL DEFAULT '{}',
                    backlog_json TEXT NOT NULL DEFAULT '{}',
                    summary_json TEXT NOT NULL DEFAULT '{}',
                    last_error TEXT,
                    -- Persisted BEFORE entering each blocking operation. A
                    -- local variable cannot survive a hard kill: the
                    -- supervisor terminates the child without its exception
                    -- handler ever running, so the only stage attribution
                    -- that survives is one already committed to the database.
                    current_stage TEXT,
                    stage_started_at REAL,
                    -- Headroom at stage start, and what earlier stages spent.
                    -- Added to CREATE TABLE as well as the migration: a live
                    -- ALTER made this work in production while every fresh
                    -- database -- including every test database -- lacked the
                    -- columns entirely.
                    deadline_remaining_at_stage_start REAL,
                    completed_stage_seconds_json TEXT NOT NULL DEFAULT '{}',
                    -- Sub-stage attribution INSIDE a stage: which window,
                    -- which pool, which sub-operation the process was in
                    -- when it was killed. A stage name alone cannot
                    -- distinguish "stalled on window 3 of 8" from "slow
                    -- provider on all eight".
                    stage_detail_json TEXT NOT NULL DEFAULT '{}'
                );
                """
            )
            backfill_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_backfill_queue)")
            }
            for name, decl in {
                "next_block": "INTEGER",
                "last_attempt_at": "REAL",
                "last_error": "TEXT",
            }.items():
                if name not in backfill_columns:
                    connection.execute(
                        "ALTER TABLE flow_backfill_queue"
                        f" ADD COLUMN {name} {decl}")
            seal_queue_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_seal_queue)")
            }
            if seal_queue_columns and "features_json" not in seal_queue_columns:
                connection.execute(
                    "ALTER TABLE flow_seal_queue ADD COLUMN features_json TEXT")
                # Entries queued before the snapshot existed cannot be sealed:
                # the features that defined those windows were overwritten by
                # the next recompute of the same pool. Retiring them says so,
                # instead of leaving a backlog that can only ever grow.
                connection.execute(
                    """UPDATE flow_seal_queue
                       SET completed_at=?, reason='superseded_before_snapshot'
                       WHERE completed_at IS NULL AND features_json IS NULL""",
                    (time.time(),))
            if seal_queue_columns and "queue_state" not in seal_queue_columns:
                connection.execute(
                    "ALTER TABLE flow_seal_queue ADD COLUMN queue_state TEXT"
                    " NOT NULL DEFAULT 'pending'")
                # Older builds represented expiry by completing the row.  It
                # must remain available to the research/backfill consumer,
                # while the live lane excludes it by queue_state.
                connection.execute(
                    """UPDATE flow_seal_queue
                       SET queue_state='expired_stale', completed_at=NULL
                       WHERE reason LIKE '%expired_stale%'""")
            lane_state_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(lane_state)")
            }
            if lane_state_columns and "stage_detail_json" not in lane_state_columns:
                # Sub-stage attribution for databases created before the
                # column existed -- same migration rule as the headroom
                # columns above: live ALTER plus CREATE TABLE coverage.
                connection.execute(
                    "ALTER TABLE lane_state ADD COLUMN stage_detail_json"
                    " TEXT NOT NULL DEFAULT '{}'")
            cohort_columns = {
                row[1] for row in connection.execute(
                    "PRAGMA table_info(acceptance_cohorts)")
            }
            if cohort_columns and "source_digest" not in cohort_columns:
                connection.execute(
                    "ALTER TABLE acceptance_cohorts ADD COLUMN"
                    " source_digest TEXT")
            run_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(runs)")
            }
            for name, decl in {
                # A rowid identifies a row, not a RUN: it cannot say which
                # process owns it, whether that process is alive, or whether
                # the status you are reading belongs to the cycle now
                # executing. External status was written to a file that any
                # skipped invocation could overwrite, so "running" was not
                # authoritative and could not be trusted.
                "run_id": "TEXT",
                "pid": "INTEGER",
                "host": "TEXT",
                "heartbeat_at": "REAL",
                "deadline_seconds": "REAL",
                "lane": "TEXT NOT NULL DEFAULT 'legacy'",
                "revision": "TEXT",
                # What the process actually executed, as opposed to what the
                # revision claims. Uncommitted edits move this and not that.
                "source_digest": "TEXT",
                "acceptance_cohort_id": "TEXT",
            }.items():
                if name not in run_columns:
                    connection.execute(
                        f"ALTER TABLE runs ADD COLUMN {name} {decl}")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_run_id ON runs(run_id)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_lane_status"
                " ON runs(lane,status,heartbeat_at)")
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_runs_acceptance_cohort"
                " ON runs(acceptance_cohort_id,lane,id)")
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

            # Horizons already scheduled that no consumer will ever read.
            # Retired rather than deleted: the row still records that the
            # horizon was promised and why it was withdrawn, so the backlog
            # becomes honest instead of merely smaller. Excluded from the due
            # queue and from every reader by status, and from cohort_progress
            # by horizon_label as well.
            connection.execute(
                """UPDATE flow_observation_outcomes SET status='retired_horizon'
                   WHERE status='pending' AND horizon_label<>?""",
                (FLOW_PRIMARY_HORIZON_LABEL,))
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
                "decision_commitment_id": "TEXT",
            }
            for name, declaration in position_migrations.items():
                if name not in position_columns:
                    connection.execute(
                        f"ALTER TABLE positions ADD COLUMN {name} {declaration}"
                    )
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS"
                " idx_positions_decision_commitment"
                " ON positions(decision_commitment_id)"
                " WHERE decision_commitment_id IS NOT NULL"
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

    def apply_v4_events(
        self, events: list[dict], *, deadline: CycleDeadline | None = None,
        chunk_size: int = INGEST_EVENT_CHUNK_SIZE,
    ) -> None:
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

        # Retried on lock contention, not abandoned.
        #
        # Every failure in the 728-attempt cohort at ac55e69 attributed to
        # ingestion/log_fetch, and two of the five were `database is locked`
        # -- the same contention fixed for the backfill settlement write in
        # 11d73ed. That fix was scoped to one call site, which is why this
        # one recurred somewhere else; the retry belongs where the write is,
        # so every caller inherits it.
        #
        # Safe to repeat: swaps are INSERT OR IGNORE and pools are an upsert
        # (ON CONFLICT DO UPDATE) whose values come from the same event, so a
        # partially-applied batch re-applies to the same state.
        #
        # A lock that never clears must still surface: only contention is
        # absorbed, and only a bounded number of times.
        if not events:
            return
        chunk_size = max(1, int(chunk_size))
        for offset in range(0, len(events), chunk_size):
            if deadline is not None:
                deadline.raise_if_expired("near_head_ingest_commit")
            chunk = events[offset:offset + chunk_size]
            for attempt in range(1, INGEST_LOCK_RETRY_ATTEMPTS + 1):
                if deadline is not None:
                    deadline.raise_if_expired("near_head_ingest_commit")
                    # SQLite's lock wait must fit inside the shared lane
                    # deadline.  A short retry is preferable to a ten-second
                    # invisible hold that the supervisor can only hard-kill.
                    busy_ms = max(1, min(
                        1000, int(max(0.0, deadline.remaining() - 0.05) * 1000)))
                else:
                    busy_ms = None
                try:
                    self._apply_v4_events(chunk, busy_timeout_ms=busy_ms)
                    break
                except sqlite3.OperationalError as error:
                    locked = "database is locked" in str(error).lower()
                    if not locked or attempt >= INGEST_LOCK_RETRY_ATTEMPTS:
                        raise
                    backoff = INGEST_LOCK_RETRY_BACKOFF_SECONDS
                    if deadline is not None:
                        if deadline.remaining() <= backoff:
                            raise CycleDeadlineExceeded(
                                "near_head_ingest_commit") from error
                        backoff = min(backoff, deadline.remaining())
                    time.sleep(backoff)

    def _apply_v4_events(
        self, events: list[dict], *, busy_timeout_ms: int | None = None,
    ) -> None:
        """The write itself. Wrapped by apply_v4_events for lock retry."""
        with self.connection(busy_timeout_ms=busy_timeout_ms) as connection:
            affected_flow_pools = set()
            event_pool_ids = sorted({
                str(event.get("pool_id") or "") for event in events
                if event.get("pool_id")
            })
            pool_cache: dict[str, sqlite3.Row | dict] = {}
            if event_pool_ids:
                placeholders = ",".join("?" for _ in event_pool_ids)
                pool_cache = {
                    row["pool_id"]: row for row in connection.execute(
                        f"SELECT * FROM v4_pools WHERE pool_id IN "
                        f"({placeholders})", event_pool_ids)
                }
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
                    # A mixed discovery batch may initialize and swap the
                    # same pool.  Keep immutable identity fields available
                    # without issuing one SELECT for every following swap.
                    pool_cache[event["pool_id"]] = {
                        "pool_id": event["pool_id"],
                        "token_address": event["token_address"],
                        "currency0": event["currency0"],
                        "currency1": event["currency1"],
                    }
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
                    pool = pool_cache.get(event["pool_id"])
                    if pool is None:
                        pool = connection.execute(
                            "SELECT * FROM v4_pools WHERE pool_id=?",
                            (event["pool_id"],),
                        ).fetchone()
                        if pool is not None:
                            pool_cache[event["pool_id"]] = pool
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
            self._refresh_v4_flow_signals(connection, affected_flow_pools)

    @staticmethod
    def _bounded_fraction(value: float) -> float:
        return max(0.0, min(1.0, value))

    def _refresh_v4_flow_signals(
        self, connection: sqlite3.Connection, pool_ids,
    ) -> None:
        """Refresh a touched pool set with three reads, not three per pool.

        The live pass normally touches 20-30 pools.  The old loop performed a
        pool lookup, MAX lookup and range read for each pool while holding the
        writer transaction; production measured 5-7 seconds in this phase and
        26 ``near_head_commit`` deferrals in 90 attempts.  The same indexed
        evidence is loaded in batches here, then the existing projection code
        is reused unchanged for every pool.
        """
        identifiers = sorted({str(value) for value in pool_ids if value})
        if not identifiers:
            return
        placeholders = ",".join("?" for _ in identifiers)
        pools = {
            row["pool_id"]: row for row in connection.execute(
                f"SELECT * FROM v4_pools WHERE pool_id IN ({placeholders})",
                identifiers,
            )
        }
        latest_by_pool = {
            row["pool_id"]: int(row["latest_block"])
            for row in connection.execute(
                f"SELECT pool_id,MAX(block_number) latest_block "
                f"FROM swap_observations WHERE pool_id IN ({placeholders}) "
                "GROUP BY pool_id",
                identifiers,
            )
            if row["latest_block"] is not None
        }
        starts = {
            pool_id: max(
                int(pools[pool_id]["swapped_block"] or latest),
                int(latest) - FLOW_WINDOW_BLOCKS + 1,
            )
            for pool_id, latest in latest_by_pool.items()
            if pool_id in pools
        }
        rows_by_pool: dict[str, list[sqlite3.Row]] = {
            pool_id: [] for pool_id in starts
        }
        if starts:
            minimum_start = min(starts.values())
            for row in connection.execute(
                f"SELECT * FROM swap_observations "
                f"WHERE pool_id IN ({placeholders}) AND block_number>=? "
                "ORDER BY pool_id,block_number,log_index",
                (*identifiers, minimum_start),
            ):
                pool_id = row["pool_id"]
                if (
                    pool_id in starts
                    and starts[pool_id] <= int(row["block_number"])
                    <= latest_by_pool[pool_id]
                ):
                    rows_by_pool[pool_id].append(row)
        for pool_id in identifiers:
            if pool_id not in pools or pool_id not in latest_by_pool:
                continue
            self._refresh_v4_flow_signal(
                connection, pool_id,
                preloaded_pool=pools[pool_id],
                preloaded_latest=latest_by_pool[pool_id],
                preloaded_rows=rows_by_pool.get(pool_id, []),
            )

    def _refresh_v4_flow_signal(
        self, connection: sqlite3.Connection, pool_id: str, *,
        preloaded_pool: sqlite3.Row | dict | None = None,
        preloaded_latest: int | None = None,
        preloaded_rows: list[sqlite3.Row] | None = None,
    ) -> None:
        pool = preloaded_pool
        if pool is None:
            pool = connection.execute(
                "SELECT * FROM v4_pools WHERE pool_id=?", (pool_id,)
            ).fetchone()
        latest = preloaded_latest
        if latest is None:
            latest = connection.execute(
                "SELECT MAX(block_number) FROM swap_observations WHERE pool_id=?",
                (pool_id,),
            ).fetchone()[0]
        if not pool or latest is None:
            return
        start = max(int(pool["swapped_block"] or latest), int(latest) - FLOW_WINDOW_BLOCKS + 1)
        rows = preloaded_rows
        if rows is None:
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

    def flow_summary(self, pending: dict | None = None) -> dict:
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
            # pending_transaction_origin_counts already walks the current
            # pool windows. Repeating the same multi-million-row range join
            # here doubled dashboard I/O while adding no evidence.
            pending = pending or self.pending_transaction_origin_counts()
        return {
            "raw_swaps": raw, "pools": row["pools"] or 0,
            "identity_resolved_swaps": resolved,
            "identity_coverage": resolved / raw if raw else 0.0,
            "active_identity_coverage": (
                pending["active_resolved_swaps"] / pending["active_swaps"]
                if pending["active_swaps"] else 0.0
            ),
            "active_identity_resolved_swaps": int(
                pending["active_resolved_swaps"] or 0),
            "active_identity_swaps": int(pending["active_swaps"] or 0),
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
        timings: dict[str, float] | None = None,
    ) -> str | None:
        """Seal an observation and schedule its outcomes, atomically, at ingest.

        Sealing BEFORE enrichment is what makes the record evidence: the claim
        and its future outcomes both exist before anything is known about how
        it turns out. Enrichment afterwards may classify the observation but
        can never alter it -- classification lives in its own table.

        Returns None if this exact observation was already sealed, so a
        repeated pass is idempotent rather than duplicating the claim.
        """
        hash_started = time.monotonic()
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
        # Canonical JSON of the features and quote payloads is part of forming
        # the evidence, not of writing it, so it is charged to hashing.
        features_json = _canonical(features)
        quote_json = _canonical(quote or {})
        round_trip = self._round_trip_return(quote)
        if timings is not None:
            timings["evidence_hash"] = time.monotonic() - hash_started
        commit_started = time.monotonic()
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
                    digest, len(transaction_hashes), features_json,
                    quote_json,
                    int(quote_block) if quote_block else None,
                    int(verified), _utc_now(), FLOW_EVIDENCE_COHORT_ID,
                    str(role), gap_count,
                    # Pure friction, pinned at seal: what the position would
                    # return if bought and sold in the same block.
                    round_trip,
                ),
            )
            if not result.rowcount:
                if timings is not None:
                    timings["database_commit"] = (
                        time.monotonic() - commit_started)
                return None
            # Outcomes scheduled in the SAME transaction, so an observation
            # can never exist without the future it promised to measure --
            # unless the window is too stale for that promise to mean
            # anything, which is recorded rather than left implicit.
            #
            # Verified before narrowing: ZERO stale observations have ever
            # produced a resolved outcome. At the primary horizon the resolved
            # population is 3,844 fresh controls and 17 fresh signals, and 0
            # stale of either arm. They scheduled work that never completed
            # and fed no consumer, at 10,718 of the 12,698 observations sealed
            # per day -- 85% of a load that resolution capacity (~2,793-4,008)
            # could not absorb, while the backlog grew +9,905/day.
            #
            # It is also corrective. Neither consumer filters on staleness, so
            # a stale outcome that DID resolve would enter the control arm and
            # produce fresh signals compared against stale controls -- not a
            # matched comparison. And 0 of 24,453 observations beyond this
            # bound have ever qualified, so the population cannot contribute a
            # signal in the first place.
            #
            # Deterministic in the observation, not a sampling coin flip: that
            # is what separates this from the seal-time control sampling that
            # was tried, voided the seal contract for a random 80% of
            # controls, and was reverted.
            schedules_outcome = lag <= FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS
            connection.execute(
                "UPDATE flow_observations SET outcome_schedule_state=?"
                " WHERE observation_id=?",
                ("scheduled" if schedules_outcome
                 else "skipped_stale_window", observation_id),
            )

            if schedules_outcome:
                connection.executemany(
                    """
                    INSERT OR IGNORE INTO flow_observation_outcomes (
                        observation_id,horizon_label,horizon_seconds,target_at
                    ) VALUES (?,?,?,?)
                    """,
                    [
                        (observation_id, label, seconds, float(now) + seconds)
                        for label, seconds in FLOW_OBSERVATION_HORIZONS
                    ],
                )
        if timings is not None:
            timings["database_commit"] = time.monotonic() - commit_started
        return observation_id

    def pending_flow_observation_quotes(self, limit: int = 8) -> list[dict]:
        """Unquoted immutable observations, actionable candidates first."""
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """SELECT o.observation_id,o.pool_id,o.token_address,
                          o.window_end_block,c.decision_head,c.identity_coverage,
                          c.identity_tier,c.gates_json
                   FROM flow_observations o
                   LEFT JOIN flow_observation_classifications c
                     USING(observation_id)
                   LEFT JOIN flow_observation_quotes q USING(observation_id)
                   WHERE o.policy_version=? AND q.observation_id IS NULL
                   ORDER BY CASE WHEN c.identity_tier='verified'
                                      AND c.gates_json='[]' THEN 0 ELSE 1 END,
                            o.sealed_at ASC LIMIT ?""",
                (FLOW_EVIDENCE_POLICY_VERSION, max(0, int(limit))),
            )]

    def record_flow_observation_quotes(
        self, observation_id: str, *, entry_quote: dict,
        entry_quote_block: int, decision_quote: dict | None,
        decision_quote_block: int | None,
    ) -> None:
        entry_execution = (entry_quote or {}).get("execution_quote") or (
            entry_quote or {})
        decision_execution = (decision_quote or {}).get(
            "execution_quote") or (decision_quote or {})
        with self.connection() as connection:
            connection.execute(
                """INSERT INTO flow_observation_quotes (
                       observation_id,entry_quote_json,entry_quote_block,
                       entry_quote_verified,decision_quote_json,
                       decision_quote_block,decision_quote_verified,captured_at
                   ) VALUES (?,?,?,?,?,?,?,?)
                   ON CONFLICT(observation_id) DO UPDATE SET
                       entry_quote_json=excluded.entry_quote_json,
                       entry_quote_block=excluded.entry_quote_block,
                       entry_quote_verified=excluded.entry_quote_verified,
                       decision_quote_json=excluded.decision_quote_json,
                       decision_quote_block=excluded.decision_quote_block,
                       decision_quote_verified=excluded.decision_quote_verified,
                       captured_at=excluded.captured_at""",
                (observation_id, json.dumps(entry_quote or {}, sort_keys=True),
                 int(entry_quote_block), bool(entry_execution.get("verified")),
                 json.dumps(decision_quote or {}, sort_keys=True),
                 None if decision_quote_block is None
                 else int(decision_quote_block),
                 bool(decision_execution.get("verified")), _utc_now()),
            )

    def due_flow_observation_outcomes(self, now: float, limit: int = 50) -> list[dict]:
        """Scheduled outcomes whose horizon has arrived, signal arm first.

        Strict oldest-first ordering made the role='signal' gate unauditable.
        The due-but-unresolved backlog reached 297,569 with its oldest entry
        9.9 days past due, so every newly sealed signal was placed behind
        roughly 300,000 older rows in a queue growing by 12,171 a day. The
        measured consequence: 67 of 75 signal-arm outcomes still pending, and
        the 8 that ever resolved all belong to ONE pool -- no signal-versus-
        control comparison has ever been possible in the project's life.

        Ordering by arm changes only WHICH due outcome is measured first. It
        is not a threshold, an eligibility rule, or an execution path: no
        observation becomes eligible that was not already eligible, and
        nothing is skipped. A control outcome deferred here is deferred by
        exactly the amount a signal outcome is advanced.

        This tightens rather than loosens: an unmeasured gate is an unverified
        gate, and an unverified gate cannot be trusted to refuse.
        """
        # Three cheap queries, never one clever one. Every attempt to
        # express "signal arm first" as a single statement made the planner
        # drive from the outcome table and walk ~297,000 due rows hunting the
        # 67 that belong to the signal arm: 22.5s as an ORDER BY key, 28.1s
        # as a join filter, 41.9s with the sort removed. The baseline
        # oldest-first query is under a second, and this lane already fails
        # 25% of its runs on deadline -- a slower selection would widen the
        # very backlog it exists to drain.
        #
        # Starting from the 15 signal observations instead makes the lookup
        # trivial, and the remainder still runs the original indexed path.
        budget = max(0, limit)
        columns = """
                SELECT o.*, obs.pool_id, obs.token_address,
                       COALESCE(q.entry_quote_json,obs.quote_json) quote_json,
                       CASE WHEN q.observation_id IS NOT NULL
                            THEN q.entry_quote_verified
                            ELSE obs.quote_verified END quote_verified,
                       obs.cohort_id, obs.window_end_block
                FROM flow_observation_outcomes o
                JOIN flow_observations obs USING(observation_id)
                LEFT JOIN flow_observation_quotes q USING(observation_id)
                WHERE o.status='pending' AND o.target_at<=?
                  AND COALESCE(q.entry_quote_verified,obs.quote_verified)=1
                """
        with self.connection() as connection:
            signal_ids = [row[0] for row in connection.execute(
                """SELECT obs.observation_id FROM flow_observations obs
                   LEFT JOIN flow_observation_quotes q USING(observation_id)
                   WHERE obs.role='signal'
                     AND COALESCE(q.entry_quote_verified,obs.quote_verified)=1""")]
            rows: list[dict] = []
            if signal_ids and budget:
                # Sorted in Python: the candidate set is bounded by the signal
                # arm's size, so an ORDER BY buys nothing and costs the
                # planner's choice of driving table.
                rows = sorted(
                    (dict(row) for row in connection.execute(
                        columns + " AND o.observation_id IN ("
                        + ",".join("?" * len(signal_ids)) + ")",
                        (float(now), *signal_ids))),
                    key=lambda row: safe_float(row.get("target_at"), 0.0),
                )[:budget]
            remaining = budget - len(rows)
            if remaining > 0:
                # Everything the signal pass did not take, in the original
                # order and on the original index.
                rows.extend(dict(row) for row in connection.execute(
                    columns
                    + " AND obs.role<>'signal' ORDER BY o.target_at LIMIT ?",
                    (float(now), remaining),
                ))
        return rows

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
            # non_exitable is INCLUDED. Filtering to status='resolved' drops
            # every outcome that could not be priced at its horizon, and the
            # arms lose them at very different rates: measured 2026-08-27, the
            # signal arm was 81% non-exitable against 31% for controls. On
            # survivors alone the signal arm read +0.04% to +0.54% against
            # -1.50% to -1.71% for controls, i.e. it looked like the first
            # edge this project had ever found. Counting the unexitable at
            # the -1.0 the store already records for them, the same data says
            # signal -81.2% against control -40.2%: twice as bad, not better.
            #
            # The gate checks exitability once, at seal. These pools pass it
            # then and die before the horizon, so the filter was selecting
            # exactly the observations that make the arm look good.
            rows = [dict(r) for r in connection.execute(
                """
                SELECT o.role, o.token_address, x.net_return, x.status
                FROM flow_observations o
                JOIN flow_observation_outcomes x USING(observation_id)
                WHERE o.cohort_id=? AND x.horizon_label=?
                  AND x.status IN ('resolved','non_exitable')
                """, (cohort, FLOW_PRIMARY_HORIZON_LABEL),
            )]
        arms: dict[str, dict] = {}
        for row in rows:
            arm = arms.setdefault(
                str(row["role"]),
                {"returns": [], "by_token": {}, "survivors": [],
                 "survivors_by_token": {}, "unexitable": 0},
            )
            unexitable = str(row["status"]) == "non_exitable"
            value = (
                FLOW_UNEXITABLE_RETURN if unexitable
                else safe_float(row["net_return"], None)
            )
            if value is None:
                continue
            arm["returns"].append(value)
            # One observation per token, earliest wins, so a single dying
            # token cannot dominate the mean the way one supplied 20 of 41.
            arm["by_token"].setdefault(row["token_address"], value)
            if unexitable:
                arm["unexitable"] += 1
            else:
                arm["survivors"].append(value)
                arm["survivors_by_token"].setdefault(
                    row["token_address"], value)
        report = {}
        for name, arm in arms.items():
            raw = arm["returns"]
            deduped = sorted(arm["by_token"].values())
            survivors = sorted(arm["survivors_by_token"].values())
            report[name] = {
                "n_raw": len(raw),
                "n_tokens": len(deduped),
                # The headline INCLUDES unexitable outcomes. A mean that
                # silently drops them is a mean over the survivors of a
                # selection the arms do not share.
                "mean_net_return": (
                    round(sum(deduped) / len(deduped), 6) if deduped else None
                ),
                "median_net_return": (
                    round(deduped[len(deduped) // 2], 6) if deduped else None
                ),
                "positive": sum(1 for v in deduped if v > 0),
                # Published beside it so the gap is visible rather than
                # discoverable. These two differing by 80 points IS the
                # finding, and it is invisible if only one is reported.
                "unexitable_outcomes": arm["unexitable"],
                "exit_attrition_rate": (
                    round(arm["unexitable"] / len(raw), 4) if raw else None
                ),
                "mean_net_return_survivors_only": (
                    round(sum(survivors) / len(survivors), 6)
                    if survivors else None
                ),
                "n_tokens_survivors_only": len(survivors),
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
        entry_quote: dict | None = None,
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
            immutable_quote = json.loads(row["quote_json"] or "{}")
            effective_entry_quote = entry_quote or immutable_quote
            entry_price = self._quote_price(effective_entry_quote)
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
                round_trip = self._round_trip_return(effective_entry_quote)
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
                observation_fresh and decision_fresh and decision_actionable
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
        pool_ids: list[str] | None = None,
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
        scoped_pool_ids = (
            None if pool_ids is None else list(dict.fromkeys(
                str(pool_id) for pool_id in pool_ids if pool_id
            ))[:100]
        )
        with self.connection() as connection:
            if scoped_pool_ids is None:
                rows = [dict(row) for row in connection.execute(
                    "SELECT * FROM flow_signals"
                    " ORDER BY window_end_block,pool_id"
                )]
            else:
                # The live path supplies only pools touched by its near-head
                # pass.  Scanning the cumulative signal table here would put
                # historical work back on the critical path and recreate the
                # coupling the lane split removed.  An empty explicit scope
                # means no work; it must never fall back to the whole table.
                if scoped_pool_ids:
                    placeholders = ",".join("?" * len(scoped_pool_ids))
                    rows = [dict(row) for row in connection.execute(
                        "SELECT * FROM flow_signals WHERE pool_id IN ("
                        + placeholders
                        + ") ORDER BY window_end_block,pool_id",
                        scoped_pool_ids,
                    )]
                else:
                    rows = []
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
            "capture_scope": (
                "all_flow_signals" if pool_ids is None else "touched_pools"
            ),
            "capture_scope_pools": (
                None if scoped_pool_ids is None else len(scoped_pool_ids)
            ),
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

    def lane_health(self, sample: int = 200) -> dict:
        """Per-lane duration AND completion rate, always together.

        p95 alone is gameable and was in fact misread: runs are killed at the
        deadline, so their durations cluster just under it and never form a
        tail. A live lane reporting p95 27.0s looked inside a 30s bar while
        only 850 of 1,564 runs -- 54.3% -- actually completed. The censored
        distribution IS the signature of truncation, and reading it as
        "no outliers" inverts the finding.

        Readiness therefore needs both: p95 under budget AND >=99% complete.
        Reporting them apart lets a lane that kills everything at the cutoff
        score perfectly while doing no work.
        """
        import json as _json
        out: dict[str, dict] = {}
        with self.connection() as connection:
            lanes = [r[0] for r in connection.execute(
                "SELECT DISTINCT lane FROM runs WHERE lane IS NOT NULL")]
            for lane in lanes:
                rows = connection.execute(
                    """SELECT status, summary_json FROM runs
                       WHERE lane=? ORDER BY id DESC LIMIT ?""",
                    (lane, int(sample)),
                ).fetchall()
                if not rows:
                    continue
                terminal = [r for r in rows if r["status"] != "running"]
                complete = [r for r in terminal if r["status"] == "complete"]
                durations = []
                for row in complete:
                    try:
                        value = (_json.loads(row["summary_json"] or "{}")
                                 .get("duration_seconds"))
                    except (TypeError, ValueError):
                        value = None
                    if isinstance(value, (int, float)):
                        durations.append(float(value))
                durations.sort()
                rate = len(complete) / len(terminal) if terminal else None
                out[lane] = {
                    "sampled": len(rows),
                    "completed": len(complete),
                    "completion_rate": round(rate, 4) if rate is not None else None,
                    # Stated explicitly: these describe SURVIVORS only.
                    "duration_median_seconds": (
                        durations[len(durations) // 2] if durations else None),
                    "duration_p95_seconds": (
                        durations[max(0, int(0.95 * len(durations)) - 1)]
                        if durations else None),
                    "durations_are_survivors_only": True,
                    "ready": bool(
                        rate is not None and rate >= 0.99 and durations
                        and durations[max(0, int(0.95 * len(durations)) - 1)] < 30.0
                    ),
                }
        return out

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
        """Count origin debt without the unbounded window cross-product.

        ``flow_signals`` is one row per pool.  Driving the active-window join
        from that small table lets ``idx_swap_observation_pool_block`` seek
        directly into each window.  The former query drove from every swap
        and tested it against a BETWEEN join; the dashboard then ran it twice
        per refresh.  On the production corpus the replacement was bounded by
        the current windows instead of millions-of-swaps times all windows.
        """
        with self.connection() as connection:
            distinct_hashes = int(connection.execute(
                """SELECT COUNT(DISTINCT transaction_hash)
                   FROM swap_observations WHERE transaction_hash<>''"""
            ).fetchone()[0] or 0)
            origin_status = connection.execute(
                """
                SELECT COALESCE(SUM(status='resolved'),0) resolved,
                       COALESCE(SUM(status<>'resolved' AND attempts>=?),0)
                           exhausted
                FROM transaction_origins
                """,
                (FLOW_ORIGIN_MAXIMUM_ATTEMPTS,),
            ).fetchone()
            active = connection.execute(
                """
                SELECT COUNT(*) active_swaps,
                       COALESCE(SUM(so.resolved_participant IS NOT NULL),0)
                           active_resolved_swaps,
                       COUNT(DISTINCT CASE WHEN so.transaction_hash<>''
                                           THEN so.transaction_hash END)
                           active_hashes,
                       COUNT(DISTINCT CASE
                           WHEN so.transaction_hash<>''
                            AND so.resolved_participant IS NOT NULL
                           THEN so.transaction_hash END) active_resolved_hashes
                FROM flow_signals fs
                CROSS JOIN swap_observations so
                    INDEXED BY idx_swap_observation_pool_block
                WHERE so.pool_id=fs.pool_id
                  AND so.block_number BETWEEN fs.window_start_block
                                          AND fs.window_end_block
                """
            ).fetchone()
            # Exhausted/unavailable origins are intentionally not pending.
            # They are normally empty, but count the active subset exactly
            # when present instead of silently assuming success forever.
            exhausted_hashes = [row[0] for row in connection.execute(
                """SELECT transaction_hash FROM transaction_origins
                   WHERE status<>'resolved' AND attempts>=?""",
                (FLOW_ORIGIN_MAXIMUM_ATTEMPTS,),
            )]
            active_exhausted = 0
            for digest in exhausted_hashes:
                active_exhausted += int(bool(connection.execute(
                    """
                    SELECT 1 FROM swap_observations so
                    JOIN flow_signals fs ON fs.pool_id=so.pool_id
                    WHERE so.transaction_hash=?
                      AND so.block_number BETWEEN fs.window_start_block
                                              AND fs.window_end_block
                    LIMIT 1
                    """,
                    (digest,),
                ).fetchone()))
        total = max(
            0, distinct_hashes - int(origin_status["resolved"] or 0)
            - int(origin_status["exhausted"] or 0))
        active_pending = max(
            0, int(active["active_hashes"] or 0)
            - int(active["active_resolved_hashes"] or 0)
            - active_exhausted)
        return {
            "total": total,
            "active": active_pending,
            "historical": max(0, total - active_pending),
            "active_swaps": int(active["active_swaps"] or 0),
            "active_resolved_swaps": int(
                active["active_resolved_swaps"] or 0),
        }

    def enqueue_backfill(self, from_block: int, to_block: int, reason: str) -> bool:
        """Record a range the live lane skipped, so it is deferred not lost."""
        if to_block < from_block:
            return False
        with self.connection() as connection:
            cursor = connection.execute(
                """INSERT OR IGNORE INTO flow_backfill_queue
                       (from_block,to_block,enqueued_at,reason)
                   VALUES (?,?,?,?)""",
                (int(from_block), int(to_block), time.time(), str(reason)),
            )
        return bool(cursor.rowcount)

    def scheduler_state(self, key: str) -> dict:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT value_json FROM flow_scheduler_state WHERE key=?",
                (str(key),)).fetchone()
        if not row:
            return {}
        try:
            value = json.loads(row["value_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def set_scheduler_state(self, key: str, value: dict) -> None:
        with self.connection() as connection:
            connection.execute(
                """INSERT INTO flow_scheduler_state(key,value_json,updated_at)
                   VALUES (?,?,?)
                   ON CONFLICT(key) DO UPDATE
                     SET value_json=excluded.value_json,
                         updated_at=excluded.updated_at""",
                (str(key), _canonical(value), _utc_now()),
            )

    def record_seal_cost_samples(
        self, updates: dict, *, status: str, run_id: str,
        epoch: int, revision: str, sample_count: int = 1,
    ) -> None:
        """Append cohort-scoped seal samples beyond the rolling estimator."""
        records: list[tuple] = []
        with self.connection() as connection:
            run = connection.execute(
                """SELECT acceptance_cohort_id,revision FROM runs
                   WHERE run_id=? ORDER BY id DESC LIMIT 1""",
                (str(run_id),),
            ).fetchone()
            cohort_id = run["acceptance_cohort_id"] if run else None
            run_revision = str(run["revision"] or revision) if run else revision
            for component, raw in updates.items():
                values = raw if isinstance(raw, list) else [raw]
                for value in values:
                    if not isinstance(value, (int, float)):
                        continue
                    measured = float(value)
                    if not math.isfinite(measured) or measured < 0:
                        continue
                    records.append((
                        time.time(), int(epoch), str(run_id), run_revision,
                        cohort_id, str(component).removesuffix("_p95"),
                        str(status), measured, max(1, int(sample_count or 1)),
                    ))
            if records:
                connection.executemany(
                    """INSERT INTO seal_cost_samples (
                           recorded_at,epoch,run_id,revision,
                           acceptance_cohort_id,component,status,seconds,
                           sample_count) VALUES (?,?,?,?,?,?,?,?,?)""",
                    records,
                )

    def acceptance_cohort(self) -> dict:
        """Return the newest durable operational acceptance boundary."""
        try:
            with self.connection() as connection:
                row = connection.execute(
                    """SELECT * FROM acceptance_cohorts
                       ORDER BY started_at DESC, cohort_id DESC LIMIT 1"""
                ).fetchone()
                if not row:
                    return {}
                result = dict(row)
                counts = connection.execute(
                    """SELECT COUNT(*) terminal_attempts,
                              COALESCE(SUM(status='complete'),0) complete_attempts,
                              COALESCE(SUM(status='deadline_exceeded'),0) timeouts,
                              COALESCE(SUM(status='failed'),0) failures,
                              COALESCE(SUM(revision!=?),0) revision_mismatches,
                              COALESCE(SUM(COALESCE(source_digest,'')!=?),0)
                                  source_mismatches
                       FROM runs
                       WHERE acceptance_cohort_id=? AND lane='live'
                         AND status!='running'""",
                    (result["revision"],
                     result.get("source_digest") or "",
                     result["cohort_id"]),
                ).fetchone()
        except sqlite3.OperationalError as error:
            # A read-only dashboard may start before the first writer has
            # migrated an older database.  Absence means no cohort yet; every
            # actual cohort start uses a writable initialized store.
            if "no such table" in str(error).lower():
                return {}
            raise
        try:
            result["policy"] = json.loads(result.pop("policy_json"))
        except (TypeError, ValueError, json.JSONDecodeError):
            result["policy"] = {}
            result.pop("policy_json", None)
        result.update({
            key: int(counts[key] or 0) for key in (
                "terminal_attempts", "complete_attempts", "timeouts",
                "failures", "revision_mismatches", "source_mismatches",
            )
        })
        checkout_revision = _workspace_revision()
        result["checkout_revision"] = checkout_revision
        result["checkout_revision_mismatch"] = bool(
            checkout_revision not in {"", "unknown"}
            and str(result.get("revision") or "") != checkout_revision
        )
        target = max(1, safe_int(result.get("sample_target"), 100))
        result["collection_complete"] = (
            result["terminal_attempts"] >= target)
        result["remaining_attempts"] = max(
            0, target - result["terminal_attempts"])
        return result

    def acceptance_scheduler_progress(self) -> dict:
        """Durable cohort pacing state shared across supervisor sessions."""
        with self.connection() as connection:
            row = connection.execute(
                """SELECT cohort_id,revision,sample_target,policy_json
                   FROM acceptance_cohorts WHERE status='collecting'
                   ORDER BY started_at DESC,cohort_id DESC LIMIT 1"""
            ).fetchone()
            if not row:
                return {"collecting": False}
            policy = json.loads(row["policy_json"] or "{}")
            counts = connection.execute(
                """SELECT lane,status,COUNT(*) count FROM runs
                   WHERE acceptance_cohort_id=? AND revision=?
                   GROUP BY lane,status""",
                (row["cohort_id"], row["revision"]),
            ).fetchall()
        by_lane_status = {
            (str(item["lane"]), str(item["status"])): int(item["count"])
            for item in counts
        }
        live_terminal = sum(
            count for (lane, status), count in by_lane_status.items()
            if lane == "live" and status != "running")
        mark_terminal = sum(
            count for (lane, status), count in by_lane_status.items()
            if lane == "marks" and status != "running")
        target = max(1, int(row["sample_target"]))
        minimum_marks = max(1, safe_int(
            policy.get("position_mark_minimum_samples"),
            math.ceil(target * ACCEPTANCE_POSITION_MARK_SAMPLE_FRACTION),
        ))
        # Five attempts is a two-mark lead at the frozen 40% ratio. It makes
        # the independent sample obligation complete before the 100th live
        # attempt instead of racing a final marks worker at cohort closure.
        paced_live = min(target, live_terminal + 5)
        mark_pace_target = min(
            minimum_marks,
            math.ceil(
                paced_live * ACCEPTANCE_POSITION_MARK_SAMPLE_FRACTION),
        )
        return {
            "collecting": True,
            "cohort_id": str(row["cohort_id"]),
            "revision": str(row["revision"]),
            "sample_target": target,
            "live_terminal": live_terminal,
            "mark_terminal": mark_terminal,
            "mark_running": by_lane_status.get(("marks", "running"), 0),
            "mark_minimum": minimum_marks,
            "mark_pace_target": mark_pace_target,
            "mark_deficit": max(0, mark_pace_target - mark_terminal),
            "closing_live_blocked": bool(
                live_terminal >= target - 1
                and mark_terminal < minimum_marks),
        }

    def backfill_recovery_pressure(self, limit: int = 10) -> dict:
        """Measure whether durable recovery is losing to new gap arrivals."""
        with self.connection() as connection:
            rows = connection.execute(
                """SELECT summary_json FROM runs
                   WHERE lane='backfill' AND status='complete'
                   ORDER BY id DESC LIMIT ?""", (max(3, int(limit)),)
            ).fetchall()
        summaries = []
        for row in reversed(rows):
            try:
                summaries.append(json.loads(row["summary_json"] or "{}"))
            except (TypeError, ValueError, json.JSONDecodeError):
                continue
        backlogs = [safe_int(
            (summary.get("backlog") or {}).get("pending_blocks"), 0)
            for summary in summaries]
        recovered = sum(safe_int(
            (summary.get("durable_gap_recovery") or {}).get(
                "blocks_scanned"), 0)
            for summary in summaries[1:])
        net_change = (
            backlogs[-1] - backlogs[0] if len(backlogs) >= 2 else None)
        arrivals = (
            max(0, net_change + recovered)
            if net_change is not None else None)
        ratio = (
            recovered / arrivals if arrivals else (
                None if arrivals is None else 1.0))
        return {
            "samples": len(summaries),
            "recovered_blocks": recovered,
            "inferred_arrival_blocks": arrivals,
            "net_change_blocks": net_change,
            "recovery_to_arrival_ratio": (
                round(ratio, 4) if ratio is not None else None),
            "target_ratio": BACKFILL_RECOVERY_TARGET_RATIO,
            "priority": bool(
                len(summaries) >= 3 and backlogs
                and backlogs[-1] > 0 and ratio is not None
                and ratio < BACKFILL_RECOVERY_TARGET_RATIO),
        }

    def start_acceptance_cohort(
        self, *, revision: str, sample_target: int = 100,
        cohort_id: str | None = None,
        checkout_revision: str | None = None,
    ) -> dict:
        """Open a frozen revision/policy cohort while no lane owns a run.

        Old cohorts are retained and explicitly superseded.  Each future run
        is stamped with this cohort id in ``begin_run``; a rolling window can
        therefore never wash an early failure out of the acceptance result.
        """
        revision = str(revision or "").strip()
        if not revision or revision == "unknown":
            raise ValueError("a concrete code revision is required")
        actual_checkout = str(
            checkout_revision or _workspace_revision() or "unknown").strip()
        if (actual_checkout not in {"", "unknown"}
                and revision != actual_checkout):
            raise ValueError(
                "acceptance cohort revision does not match checkout HEAD: "
                f"requested={revision} checkout={actual_checkout}")
        ownership = self.running_run_audit()
        if ownership["active"] or ownership["stale"]:
            raise RuntimeError(
                "cannot start an acceptance cohort while a lane run exists")
        target = max(1, int(sample_target))
        policy = operational_acceptance_policy(target)
        policy_hash = hashlib.sha256(
            _canonical(policy).encode("utf-8")).hexdigest()
        started_at = _utc_now()
        if not cohort_id:
            stamp = re.sub(r"[^0-9]", "", started_at)[:14]
            clean_revision = re.sub(r"[^0-9A-Za-z._-]", "", revision)[:12]
            cohort_id = f"live-acceptance-{stamp}-{clean_revision}"
        with self.connection() as connection:
            connection.execute(
                """UPDATE acceptance_cohorts
                   SET status='superseded',closed_at=?,close_reason=?
                   WHERE status='collecting'""",
                (started_at, f"superseded_by:{cohort_id}"),
            )
            connection.execute(
                """INSERT INTO acceptance_cohorts
                   (cohort_id,schema_version,started_at,revision,source_digest,
                    sample_target,policy_json,policy_hash,status)
                   VALUES (?,?,?,?,?,?,?,?,'collecting')""",
                (str(cohort_id), ACCEPTANCE_COHORT_SCHEMA_VERSION, started_at,
                 revision, _worktree_source_digest(), target,
                 _canonical(policy), policy_hash),
            )
        return self.acceptance_cohort()

    def enqueue_seal(self, windows: list[dict], reason: str) -> int:
        """Defer windows durably rather than hoping selection re-finds them."""
        rows = []
        for window in windows:
            pool_id = window.get("pool_id")
            end_block = window.get("window_end_block")
            if not pool_id or end_block is None:
                continue
            rows.append((
                str(pool_id), int(end_block), window.get("token_address"),
                safe_int(window.get("window_start_block"), 0),
                window.get("features_json") or "{}",
                time.time(), str(reason),
            ))
        if not rows:
            return 0
        with self.connection() as connection:
            # A window already queued keeps its ORIGINAL enqueued_at, so
            # repeated deferral ages the entry instead of refreshing it. An
            # entry that keeps losing the admission race has to become the
            # oldest one eventually, or the queue starves its own tail.
            connection.executemany(
                """INSERT OR IGNORE INTO flow_seal_queue
                       (pool_id,window_end_block,token_address,
                        window_start_block,features_json,enqueued_at,reason)
                   VALUES (?,?,?,?,?,?,?)""", rows)
        return len(rows)

    def expire_stale_seal_queue(
        self, *, head_block: int | None = None,
        freshness_blocks: int = FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
        stale_seconds: float | None = None,
    ) -> int:
        """Mark irrecoverably stale queue entries expired_stale.

        Live freshness is a BLOCK invariant, not a wall-clock guess.  When a
        head is supplied (the production path), every window below the exact
        120-block floor is marked as research-only queue debt.  The time
        fallback exists only for offline maintenance callers that have no RPC
        head. Entries are never deleted or represented as sealed. A separate
        terminal transition preserves their snapshots as ``expired_unsealed``;
        neither the live lane nor backfill is allowed to seal them afterward.
        """
        now = time.time()
        with self.connection() as connection:
            if head_block is not None:
                floor = max(0, int(head_block) - max(0, int(freshness_blocks)))
                cursor = connection.execute(
                    """UPDATE flow_seal_queue
                       SET queue_state='expired_stale',
                           reason=reason || '|expired_stale:block_lag'
                       WHERE completed_at IS NULL AND queue_state='pending'
                         AND window_end_block < ?""",
                    (floor,))
            else:
                cutoff = now - float(
                    SEAL_QUEUE_STALE_SECONDS if stale_seconds is None
                    else stale_seconds)
                cursor = connection.execute(
                    """UPDATE flow_seal_queue
                       SET queue_state='expired_stale',
                           reason=reason || '|expired_stale:wall_clock_fallback'
                       WHERE completed_at IS NULL AND queue_state='pending'
                         AND enqueued_at < ?""",
                    (cutoff,))
            return cursor.rowcount or 0

    def retire_expired_seal_queue(self, limit: int = 500) -> int:
        """Terminally retire stale snapshots without creating observations.

        Expiry is an evidence fact, not an invitation to reconstruct a late
        observation. The original snapshot, enqueue time and reason remain in
        ``flow_seal_queue`` for audit; only the lifecycle fields change.  A
        bounded batch keeps this housekeeping write short beside the live
        lane's higher-priority SQLite work.
        """
        batch_limit = max(0, int(limit))
        if batch_limit <= 0:
            return 0
        now = time.time()
        with self.connection() as connection:
            cursor = connection.execute(
                """UPDATE flow_seal_queue
                   SET completed_at=?, queue_state='expired_unsealed',
                       reason=CASE
                         WHEN reason LIKE '%|retired_without_observation%'
                           THEN reason
                         ELSE reason || '|retired_without_observation'
                       END
                   WHERE rowid IN (
                     SELECT rowid FROM flow_seal_queue
                     WHERE completed_at IS NULL
                       AND queue_state='expired_stale'
                     ORDER BY enqueued_at LIMIT ?
                   )""",
                (now, batch_limit),
            )
            return cursor.rowcount or 0

    def pending_seal_windows(self, limit: int = 25) -> list[dict]:
        """Queued windows, oldest first, ready to seal without a lookup.

        Each row is shaped like the flow_signals row it came from, because
        flow_signals cannot supply it a second time: that table is keyed by
        pool_id alone and is overwritten whenever the pool trades again.

        Only ``pending`` rows are eligible.  Expired rows intentionally have
        no opt-in escape hatch here: making a stale record observable through
        the sealing API is how the backfill lane accidentally fabricated
        active-cohort observations from terminal history.

        The join against flow_observations is what keeps the queue honest: a
        window sealed by any other path (a wider batch pass, a later cycle
        that reached it) leaves an entry behind, and returning it would spend
        the live lane's scarcest seconds re-quoting settled evidence.
        """
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """SELECT q.* FROM flow_seal_queue q
                   WHERE q.completed_at IS NULL AND q.queue_state='pending'
                     AND NOT EXISTS (
                     SELECT 1 FROM flow_observations o
                     WHERE o.pool_id=q.pool_id
                       AND o.window_end_block=q.window_end_block
                       AND o.policy_version=?
                   ) ORDER BY q.enqueued_at LIMIT ?""",
                (FLOW_EVIDENCE_POLICY_VERSION, int(limit)))]

    def complete_seal_queue(self, windows: list[dict]) -> int:
        """Close entries for windows this cycle actually sealed."""
        rows = [
            (time.time(), str(window.get("pool_id")),
             int(window.get("window_end_block")))
            for window in windows
            if window.get("pool_id") and window.get("window_end_block") is not None
        ]
        if not rows:
            return 0
        with self.connection() as connection:
            connection.executemany(
                """UPDATE flow_seal_queue
                   SET completed_at=?, queue_state='sealed'
                   WHERE pool_id=? AND window_end_block=?
                     AND completed_at IS NULL""", rows)
        return len(rows)

    def seal_queue_backlog(self) -> dict:
        """Depth AND age. A queue that only grows is a leak, not a buffer."""
        with self.connection() as connection:
            row = connection.execute(
                """SELECT
                          SUM(CASE WHEN q.queue_state='pending'
                                   THEN 1 ELSE 0 END) pending_windows,
                          SUM(CASE WHEN q.queue_state='expired_stale'
                                   THEN 1 ELSE 0 END) expired_stale_windows,
                          MIN(q.enqueued_at) oldest,
                          MAX(q.attempts) attempts
                   FROM flow_seal_queue q
                   WHERE q.completed_at IS NULL
                     AND q.queue_state IN ('pending','expired_stale')
                     AND NOT EXISTS (
                     SELECT 1 FROM flow_observations o
                     WHERE o.pool_id=q.pool_id
                       AND o.window_end_block=q.window_end_block
                       AND o.policy_version=?
                   )""", (FLOW_EVIDENCE_POLICY_VERSION,)
            ).fetchone()
        oldest = row["oldest"]
        return {
            "pending_windows": int(row["pending_windows"] or 0),
            "expired_stale_windows": int(
                row["expired_stale_windows"] or 0),
            "max_attempts": int(row["attempts"] or 0),
            "oldest_age_seconds": (
                round(time.time() - oldest, 1) if oldest else None),
        }

    def pending_backfill(self, limit: int = 10) -> list[dict]:
        """Oldest first: the deepest hole is the one most likely to be lost."""
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """SELECT * FROM flow_backfill_queue WHERE completed_at IS NULL
                   ORDER BY enqueued_at LIMIT ?""", (int(limit),))]

    def complete_backfill(self, from_block: int, to_block: int) -> None:
        with self.connection() as connection:
            connection.execute(
                """UPDATE flow_backfill_queue SET completed_at=?
                   WHERE from_block=? AND to_block=?""",
                (time.time(), int(from_block), int(to_block)),
            )

    def advance_backfill(
        self, from_block: int, to_block: int, next_block: int,
        *, error: str | None = None,
    ) -> None:
        with self.connection() as connection:
            connection.execute(
                """UPDATE flow_backfill_queue
                   SET next_block=?,attempts=attempts+1,last_attempt_at=?,last_error=?
                   WHERE from_block=? AND to_block=? AND completed_at IS NULL""",
                (int(next_block), time.time(), error,
                 int(from_block), int(to_block)),
            )

    def backfill_backlog(self) -> dict:
        """Backlog AGE, not just depth: a queue that never drains is a leak."""
        with self.connection() as connection:
            row = connection.execute(
                """SELECT COUNT(*) ranges,
                          COALESCE(SUM(to_block-COALESCE(next_block,from_block)+1),0) blocks,
                          MIN(enqueued_at) oldest
                   FROM flow_backfill_queue WHERE completed_at IS NULL"""
            ).fetchone()
        oldest = row["oldest"]
        return {
            "pending_ranges": int(row["ranges"] or 0),
            "pending_blocks": int(row["blocks"] or 0),
            "oldest_age_seconds": (
                round(time.time() - oldest, 1) if oldest else None),
        }

    def begin_run(
        self, run_id: str, deadline_seconds: float, lane: str = "legacy",
    ) -> int:
        """Claim a run row stamped with who owns it and when it last breathed."""
        import os as _os, socket as _socket
        now = time.time()
        with self.connection() as connection:
            cohort = connection.execute(
                """SELECT cohort_id,sample_target FROM acceptance_cohorts
                   WHERE status='collecting'
                   ORDER BY started_at DESC LIMIT 1"""
            ).fetchone()
            cohort_id = cohort["cohort_id"] if cohort else None
            if cohort_id and lane == "live":
                live_terminal = connection.execute(
                    """SELECT COUNT(*) FROM runs
                       WHERE acceptance_cohort_id=? AND lane='live'
                         AND status!='running'""", (cohort_id,),
                ).fetchone()[0]
                if int(live_terminal) >= int(cohort["sample_target"]):
                    # The cohort can remain open solely for its independent
                    # marks obligation. Preserve the exact live sample while
                    # that durable evidence catches up.
                    cohort_id = None
            row_id = connection.execute(
                """INSERT INTO runs(started_at,status,run_id,pid,host,
                       heartbeat_at,deadline_seconds,lane,revision,
                       source_digest,acceptance_cohort_id)
                   VALUES (?,'running',?,?,?,?,?,?,?,?,?)""",
                (_utc_now(), run_id, _os.getpid(), _socket.gethostname(),
                 now, float(deadline_seconds), str(lane), CODE_REVISION,
                 _worktree_source_digest(), cohort_id),
            ).lastrowid
            if lane != "legacy":
                connection.execute(
                    """INSERT INTO lane_state
                       (lane,run_id,pid,status,started_at,heartbeat_at,
                        completed_at,deadline_seconds,last_error)
                       VALUES (?,?,?,'running',?,?,NULL,?,NULL)
                       ON CONFLICT(lane) DO UPDATE SET
                       run_id=excluded.run_id,pid=excluded.pid,
                       status='running',started_at=excluded.started_at,
                       heartbeat_at=excluded.heartbeat_at,completed_at=NULL,
                       deadline_seconds=excluded.deadline_seconds,last_error=NULL,
                       current_stage=NULL,stage_started_at=NULL,
                       deadline_remaining_at_stage_start=NULL,
                       completed_stage_seconds_json='{}',stage_detail_json='{}'""",
                    (str(lane), run_id, _os.getpid(), now, now,
                     float(deadline_seconds)),
                )
        return row_id

    def heartbeat_run(self, run_id: str) -> None:
        """Liveness, so a crashed run is distinguishable from a slow one.

        Without this a `running` row is indistinguishable from an abandoned
        one, and the only recovery is a human noticing. A stale heartbeat is
        evidence; an old started_at is not.
        """
        with self.connection() as connection:
            now = time.time()
            connection.execute(
                "UPDATE runs SET heartbeat_at=? WHERE run_id=? AND status='running'",
                (now, run_id),
            )
            connection.execute(
                """UPDATE lane_state SET heartbeat_at=?
                   WHERE run_id=? AND status='running'""", (now, run_id))

    @staticmethod
    def _close_cohort_if_target(connection, cohort_id: str | None) -> None:
        if not cohort_id:
            return
        cohort = connection.execute(
            """SELECT sample_target,status FROM acceptance_cohorts
               WHERE cohort_id=?""", (cohort_id,)).fetchone()
        if not cohort or cohort["status"] != "collecting":
            return
        terminal = connection.execute(
            """SELECT COUNT(*) FROM runs
               WHERE acceptance_cohort_id=? AND lane='live'
                 AND status!='running'""", (cohort_id,)).fetchone()[0]
        policy_row = connection.execute(
            "SELECT policy_json FROM acceptance_cohorts WHERE cohort_id=?",
            (cohort_id,),
        ).fetchone()
        policy = json.loads(policy_row["policy_json"] or "{}")
        minimum_marks = max(1, safe_int(
            policy.get("position_mark_minimum_samples"),
            math.ceil(
                int(cohort["sample_target"])
                * ACCEPTANCE_POSITION_MARK_SAMPLE_FRACTION),
        ))
        mark_terminal = connection.execute(
            """SELECT COUNT(*) FROM runs
               WHERE acceptance_cohort_id=? AND lane='marks'
                 AND status!='running'""", (cohort_id,),
        ).fetchone()[0]
        if (
            int(terminal) >= int(cohort["sample_target"])
            and int(mark_terminal) >= minimum_marks
        ):
            connection.execute(
                """UPDATE acceptance_cohorts
                   SET status='complete',closed_at=?,close_reason=?
                   WHERE cohort_id=? AND status='collecting'""",
                (_utc_now(), "sample_target_reached", cohort_id),
            )

    def finish_run(
        self, run_id: str, status: str, *, summary: dict | None = None,
        error: str | None = None, cursor: dict | None = None,
        backlog: dict | None = None,
    ) -> None:
        """Only the owning run may close its own row.

        Scoping the write to run_id is what stops a skipped overlapping
        invocation from marking the ACTIVE run finished -- the failure the
        file-based status had, where any invocation could overwrite any
        other's state.
        """
        with self.connection() as connection:
            now = time.time()
            connection.execute(
                """UPDATE runs SET status=?, completed_at=?, summary_json=?
                   WHERE run_id=?""",
                (status, _utc_now(), _canonical(summary or {}), run_id),
            )
            connection.execute(
                """UPDATE lane_state SET status=?,heartbeat_at=?,completed_at=?,
                       summary_json=?,last_error=?,
                       cursor_json=COALESCE(?,cursor_json),
                       backlog_json=COALESCE(?,backlog_json)
                   WHERE run_id=?""",
                (status, now, now, _canonical(summary or {}), error,
                 _canonical(cursor) if cursor is not None else None,
                 _canonical(backlog) if backlog is not None else None,
                 run_id),
            )
            run = connection.execute(
                """SELECT lane,acceptance_cohort_id FROM runs
                   WHERE run_id=? ORDER BY id DESC LIMIT 1""",
                (run_id,),
            ).fetchone()
            if (run and run["lane"] in {"live", "marks"}
                    and run["acceptance_cohort_id"]
                    and status != "running"):
                self._close_cohort_if_target(
                    connection, run["acceptance_cohort_id"])

    def active_run(
        self, stale_seconds: float = 300.0, lane: str | None = None,
    ) -> dict | None:
        """The authoritative answer to 'is a cycle running right now?'

        A row is active only if its heartbeat is recent. That makes the
        question answerable from the database rather than from a lock file
        whose owner may have been killed without releasing it.
        """
        with self.connection() as connection:
            lane_clause = " AND lane=?" if lane else ""
            parameters: list = [time.time() - float(stale_seconds)]
            if lane:
                parameters.append(str(lane))
            row = connection.execute(
                """SELECT * FROM runs WHERE status='running'
                     AND heartbeat_at IS NOT NULL
                     AND heartbeat_at > ?""" + lane_clause + """
                   ORDER BY heartbeat_at DESC LIMIT 1""",
                parameters,
            ).fetchone()
        return dict(row) if row else None

    def recover_abandoned_runs(
        self, process_is_running=_process_is_running,
    ) -> list[dict]:
        """Close historical ``running`` rows that cannot still own work.

        ``lane_state`` is intentionally one row per lane.  That makes current
        status authoritative, but it also means a killed worker can be
        superseded before its historical ``runs`` row is closed.  Reconciling
        only ``lane_state`` therefore left an ever-growing set of audit rows
        claiming to run forever.  Recovery examines every running row and is
        deliberately conservative: a row is closed only when its owner is
        dead, its heartbeat has exceeded its own deadline plus grace, it has
        been superseded by the lane's authoritative run, or it predates the
        run-id/heartbeat schema and consequently cannot be authoritative.
        """
        import socket as _socket

        now = time.time()
        local_host = _socket.gethostname()
        recovered: list[dict] = []
        states = self.lane_states()
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM runs WHERE status='running' ORDER BY id"
            )]
            for row in rows:
                lane = str(row.get("lane") or "legacy")
                run_id = row.get("run_id")
                pid = safe_int(row.get("pid"), 0)
                heartbeat = safe_float(row.get("heartbeat_at"), 0.0)
                deadline = max(0.0, safe_float(
                    row.get("deadline_seconds"), 0.0))
                state = states.get(lane) or {}
                superseded = bool(
                    lane != "legacy" and state.get("run_id") != run_id)
                pre_authoritative = not run_id or not heartbeat
                heartbeat_age = now - heartbeat if heartbeat else None
                stale = bool(
                    heartbeat_age is not None
                    and heartbeat_age > max(
                        30.0, deadline + 2 * LANE_HEARTBEAT_SECONDS,
                    )
                )
                local_owner = not row.get("host") or row.get("host") == local_host
                owner_dead = bool(
                    pid > 0 and local_owner
                    and process_is_running(pid) is False
                )
                if not (superseded or pre_authoritative or stale or owner_dead):
                    continue
                if pre_authoritative:
                    reason = "pre_authoritative_run_recovered"
                elif superseded:
                    reason = "superseded_run_recovered"
                elif owner_dead:
                    reason = "dead_owner_run_recovered"
                else:
                    reason = "stale_heartbeat_run_recovered"
                payload = {
                    "schema_version": 1,
                    "status": "abandoned_recovered",
                    "reason": reason,
                    "lane": lane,
                    "run_id": run_id,
                    "pid": pid or None,
                    "recovered_at": _utc_now(),
                    "previous_heartbeat_age_seconds": (
                        round(heartbeat_age, 3)
                        if heartbeat_age is not None else None
                    ),
                }
                changed = connection.execute(
                    """UPDATE runs SET status='abandoned_recovered',
                              completed_at=?,summary_json=?
                       WHERE id=? AND status='running'""",
                    (_utc_now(), _canonical(payload), int(row["id"])),
                ).rowcount
                if not changed:
                    continue
                if state.get("run_id") == run_id:
                    connection.execute(
                        """UPDATE lane_state
                           SET status='abandoned_recovered',heartbeat_at=?,
                               completed_at=?,summary_json=?,last_error=?
                           WHERE lane=? AND run_id=? AND status='running'""",
                        (now, now, _canonical(payload), reason, lane, run_id),
                    )
                if lane == "live":
                    self._close_cohort_if_target(
                        connection, row.get("acceptance_cohort_id"))
                recovered.append(payload)
        return recovered

    def running_run_audit(self) -> dict:
        """Report active and abandoned-looking run rows without mutating."""
        import socket as _socket

        now = time.time()
        local_host = _socket.gethostname()
        states = self.lane_states()
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM runs WHERE status='running' ORDER BY id"
            )]
        active: list[dict] = []
        stale: list[dict] = []
        by_lane: dict[str, int] = {}
        for row in rows:
            lane = str(row.get("lane") or "legacy")
            run_id = row.get("run_id")
            pid = safe_int(row.get("pid"), 0)
            heartbeat = safe_float(row.get("heartbeat_at"), 0.0)
            age = now - heartbeat if heartbeat else None
            current = bool(
                lane != "legacy"
                and (states.get(lane) or {}).get("run_id") == run_id
            )
            local_owner = not row.get("host") or row.get("host") == local_host
            owner_alive = (
                _process_is_running(pid) if pid > 0 and local_owner else None)
            fresh = bool(
                run_id and heartbeat and age <= max(
                    30.0,
                    max(0.0, safe_float(row.get("deadline_seconds"), 0.0))
                    + 2 * LANE_HEARTBEAT_SECONDS,
                )
            )
            item = {
                "id": int(row["id"]), "lane": lane, "run_id": run_id,
                "pid": pid or None,
                "heartbeat_age_seconds": round(age, 3) if age is not None else None,
                "authoritative_lane_run": current,
                "owner_alive": owner_alive,
            }
            if fresh and current and owner_alive is not False:
                active.append(item)
                by_lane[lane] = by_lane.get(lane, 0) + 1
            else:
                stale.append(item)
        return {
            "running_rows": len(rows), "active": len(active),
            "stale": len(stale), "active_by_lane": by_lane,
            "overlap_lanes": sorted(
                lane for lane, count in by_lane.items() if count > 1),
            "stale_rows": stale,
        }

    def lane_states(self) -> dict[str, dict]:
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                "SELECT * FROM lane_state ORDER BY lane")]
        for row in rows:
            for key in ("cursor_json", "backlog_json", "summary_json"):
                try:
                    row[key.removesuffix("_json")] = json.loads(row.pop(key) or "{}")
                except (TypeError, ValueError):
                    row[key.removesuffix("_json")] = {}
        return {row["lane"]: row for row in rows}

    def mark_lane_stage(
        self, lane: str, stage: str, run_id: str | None = None,
        remaining: float | None = None, completed: dict | None = None,
        detail: dict | None = None,
    ) -> None:
        """Also record HEADROOM, not just which stage was running.

        Attribution alone misleads: 5 of 6 post-change live failures died in
        classification, but four spent only 0.48-4.07s inside it. That is not
        a slow classification, it is an empty budget by the time classification
        started. deadline_remaining_at_stage_start distinguishes "this stage is
        slow" from "earlier stages left it nothing", and the completed-stage
        durations say which earlier stage took it.
        """
        """Commit the stage BEFORE the blocking call it names.

        Ordering is the whole point: written after, it records what already
        finished; written before, it records what the process was inside when
        it was killed. Called ahead of ingestion, sealing, decision-head
        retrieval and classification -- and ahead of each observation
        sub-stage, with `detail` carrying the window index and pool id so a
        forced termination names the exact window it died on.
        """
        with self.connection() as connection:
            connection.execute(
                """UPDATE lane_state
                   SET current_stage=?, stage_started_at=?,
                       deadline_remaining_at_stage_start=?,
                       completed_stage_seconds_json=?,
                       stage_detail_json=?
                   WHERE lane=? AND status='running'
                     AND (? IS NULL OR run_id=?)""",
                (str(stage), time.time(), remaining,
                 _canonical(completed or {}), _canonical(detail or {}),
                 str(lane), run_id, run_id),
            )

    def lane_failure_stage(self, lane: str, run_id: str) -> dict:
        """The persisted stage for a run, for child-side failure payloads."""
        with self.connection() as connection:
            row = connection.execute(
                """SELECT current_stage, stage_started_at,
                          deadline_remaining_at_stage_start,
                          completed_stage_seconds_json,
                          COALESCE(stage_detail_json, '{}') AS stage_detail
                   FROM lane_state WHERE lane=? AND run_id=?""",
                (str(lane), str(run_id)),
            ).fetchone()
        if not row or not row["current_stage"]:
            return {"failure_stage": None, "stage_elapsed_seconds": None}
        started = row["stage_started_at"]
        return {
            "failure_stage": row["current_stage"],
            "deadline_remaining_at_stage_start": row[
                "deadline_remaining_at_stage_start"],
            "completed_stage_seconds": json.loads(
                row["completed_stage_seconds_json"] or "{}"),
            "failure_substage_detail": json.loads(
                row["stage_detail"] or "{}"),
            "stage_elapsed_seconds": (
                round(time.time() - started, 3) if started else None),
        }

    def terminate_lane(
        self, lane: str, pid: int, reason: str,
        attempt_started_at: float | None = None,
    ) -> None:
        """Close only the run owned by the process the supervisor terminated."""
        now = time.time()
        failure = {
            "lane": str(lane), "status": "deadline_exceeded",
            "pid": int(pid), "error": str(reason), "timestamp": _utc_now(),
        }
        if attempt_started_at is not None:
            failure["duration_seconds"] = round(
                max(0.0, now - float(attempt_started_at)), 3)
        with self.connection() as connection:
            row = connection.execute(
                """SELECT run_id, current_stage, stage_started_at,
                          deadline_seconds,
                          deadline_remaining_at_stage_start,
                          completed_stage_seconds_json,
                          COALESCE(stage_detail_json, '{}') AS stage_detail_json
                   FROM lane_state
                   WHERE lane=? AND pid=? AND status='running'""",
                (str(lane), int(pid)),
            ).fetchone()
            if row:
                # The stage the process was INSIDE, read from the database
                # rather than from a dead process's memory.
                failure["failure_stage"] = row["current_stage"]
                failure["deadline_remaining_at_stage_start"] = row[
                    "deadline_remaining_at_stage_start"]
                try:
                    failure["completed_stage_seconds"] = json.loads(
                        row["completed_stage_seconds_json"] or "{}")
                except (TypeError, ValueError):
                    failure["completed_stage_seconds"] = {}
                try:
                    failure["failure_substage_detail"] = json.loads(
                        row["stage_detail_json"] or "{}")
                except (TypeError, ValueError):
                    failure["failure_substage_detail"] = {}
                started = row["stage_started_at"]
                failure["stage_elapsed_seconds"] = (
                    round(now - started, 3) if started else None)
                failure["termination_reason"] = str(reason)
                # A hard-killed worker cannot execute run_live_lane's except
                # block, so the SUPERVISOR must commit the censored latency
                # sample.  Attribute it to the persisted substage and clamp
                # it upward: a killed duration is a lower bound, never proof
                # that the operation was fast.
                stage = str(row["current_stage"] or "")
                elapsed = safe_float(failure["stage_elapsed_seconds"], 0.0)
                if (str(lane) == "live" and elapsed > 0
                        and stage.startswith(
                            "fresh_quote_and_observation")):
                    state_row = connection.execute(
                        "SELECT value_json FROM flow_scheduler_state WHERE key=?",
                        (SEAL_COST_MODEL_STATE_KEY,),
                    ).fetchone()
                    try:
                        model_state = json.loads(
                            state_row[0] if state_row else "{}")
                    except (TypeError, ValueError):
                        model_state = {}
                    if stage.endswith("queue_settlement"):
                        # Settlement is its own reserved component. Charging
                        # it to fixed cost here would restore the double count
                        # removed from the successful child path.
                        targets = ("queue_settlement_p95",)
                    elif stage.endswith(("observation_selection",
                                         "quote_prefetch")):
                        targets = ("fixed_observation_cost_p95",)
                    else:
                        targets = ("per_window_cost_p95",)
                    censor_cap = max(
                        0.05,
                        safe_float(row["deadline_seconds"],
                                   LIVE_LANE_BUDGET_SECONDS)
                        + LANE_TERMINATION_GRACE_SECONDS,
                    )
                    bounded_elapsed = min(elapsed, censor_cap)
                    model_state = _append_seal_cost_records(
                        model_state,
                        {target: bounded_elapsed for target in targets},
                        status="censored", run_id=str(row["run_id"]),
                        revision=CODE_REVISION, recorded_at=now,
                    )
                    model_state["last_censored_stage"] = stage
                    model_state["last_censored_seconds"] = round(
                        bounded_elapsed, 4)
                    model_state["last_censored_raw_seconds"] = round(
                        elapsed, 4)
                    model_state["last_censored_cap_seconds"] = round(
                        censor_cap, 4)
                    model_state["last_censored_quarantined"] = bool(
                        elapsed > censor_cap)
                    connection.execute(
                        """INSERT INTO flow_scheduler_state(key,value_json,updated_at)
                           VALUES (?,?,?)
                           ON CONFLICT(key) DO UPDATE SET
                             value_json=excluded.value_json,
                             updated_at=excluded.updated_at""",
                        (SEAL_COST_MODEL_STATE_KEY,
                         _canonical(model_state), _utc_now()),
                    )
                    run_meta = connection.execute(
                        """SELECT acceptance_cohort_id,revision FROM runs
                           WHERE run_id=? ORDER BY id DESC LIMIT 1""",
                        (str(row["run_id"]),),
                    ).fetchone()
                    for target in targets:
                        connection.execute(
                            """INSERT INTO seal_cost_samples (
                                   recorded_at,epoch,run_id,revision,
                                   acceptance_cohort_id,component,status,
                                   seconds,sample_count)
                               VALUES (?,?,?,?,?,?,?,?,1)""",
                            (now, SEAL_COST_MODEL_EPOCH, str(row["run_id"]),
                             str(run_meta["revision"] or CODE_REVISION)
                             if run_meta else CODE_REVISION,
                             run_meta["acceptance_cohort_id"]
                             if run_meta else None,
                             str(target).removesuffix("_p95"), "censored",
                             bounded_elapsed),
                        )
            if not row:
                return
            connection.execute(
                """UPDATE runs SET status='deadline_exceeded',completed_at=?,
                       summary_json=? WHERE run_id=? AND status='running'""",
                (_utc_now(), _canonical(failure), row["run_id"]),
            )
            connection.execute(
                """UPDATE lane_state SET status='deadline_exceeded',
                       heartbeat_at=?,completed_at=?,summary_json=?,last_error=?
                   WHERE lane=? AND pid=? AND status='running'""",
                (now, now, _canonical(failure), str(reason),
                 str(lane), int(pid)),
            )
            if str(lane) == "live":
                cohort_row = connection.execute(
                    """SELECT acceptance_cohort_id FROM runs
                       WHERE run_id=? ORDER BY id DESC LIMIT 1""",
                    (str(row["run_id"]),),
                ).fetchone()
                self._close_cohort_if_target(
                    connection,
                    cohort_row["acceptance_cohort_id"]
                    if cohort_row else None)

    def lane_performance(
        self, limit: int = 100, *, cohort_id: str | None = None,
        revision: str | None = None,
    ) -> dict[str, dict]:
        """Measured lane latency/reliability, including terminated attempts.

        A supervisor-terminated worker may never get to write duration_seconds.
        Excluding those rows made the displayed p95 success-biased precisely
        when the lane was least reliable. Terminal wall-clock timestamps are a
        conservative duration for those censored attempts; successful latency
        remains available separately for diagnosis.
        """
        result: dict[str, dict] = {}
        with self.connection() as connection:
            for lane in LANE_NAMES:
                filters = ["lane=?"]
                parameters: list[object] = [lane]
                order = "DESC"
                if cohort_id:
                    filters.extend([
                        "acceptance_cohort_id=?", "status!='running'"])
                    parameters.append(str(cohort_id))
                    order = "ASC"
                if revision:
                    filters.append("revision=?")
                    parameters.append(str(revision))
                parameters.append(int(limit))
                rows = connection.execute(
                    """SELECT status,summary_json,started_at,completed_at,
                              deadline_seconds
                       FROM runs WHERE """ + " AND ".join(filters)
                    + f" ORDER BY id {order} LIMIT ?", parameters,
                ).fetchall()
                durations: list[float] = []
                successful_durations: list[float] = []
                timestamp_derived = 0
                statuses: dict[str, int] = {}
                for row in rows:
                    statuses[row["status"]] = statuses.get(row["status"], 0) + 1
                    try:
                        duration = safe_float(
                            json.loads(row["summary_json"] or "{}").get(
                                "duration_seconds"), -1.0)
                    except (TypeError, ValueError):
                        duration = -1.0
                    if duration < 0 and row["status"] != "running":
                        # A recovered run's completed_at is when the SWEEP
                        # noticed it, not when it stopped working. Deriving
                        # duration from it charged recovery latency to the
                        # SLO: all-attempt p95 read ~240s for live and ~295s
                        # for marks while successful p95 was 24.6s and 50.1s.
                        # A censored observation is bounded by the deadline it
                        # was killed at, plus the grace the supervisor allows
                        # before terminating -- not by when anyone looked.
                        if row["status"] in {"abandoned_recovered",
                                             "deadline_exceeded"}:
                            budget = safe_float(row["deadline_seconds"], 0.0)
                            duration = (
                                budget + LANE_TERMINATION_GRACE_SECONDS
                                if budget > 0 else -1.0)
                            if duration >= 0:
                                timestamp_derived += 1
                        else:
                            started_at = _timestamp(row["started_at"])
                            completed_at = _timestamp(row["completed_at"])
                            if started_at is not None and completed_at is not None:
                                duration = max(0.0, completed_at - started_at)
                                timestamp_derived += 1
                    if duration >= 0:
                        durations.append(duration)
                        if row["status"] == "complete":
                            successful_durations.append(duration)

                def percentile_95(values: list[float]) -> float | None:
                    ordered = sorted(values)
                    if not ordered:
                        return None
                    index = max(0, min(
                        len(ordered) - 1,
                        int((len(ordered) * 0.95) + 0.999999) - 1,
                    ))
                    return round(ordered[index], 3)

                p95 = percentile_95(durations)
                success_p95 = percentile_95(successful_durations)
                terminal = sum(
                    count for status, count in statuses.items()
                    if status != "running"
                )
                completed = statuses.get("complete", 0)
                result[lane] = {
                    "sample_size": len(rows),
                    "terminal_sample_size": terminal,
                    "durations_measured": len(durations),
                    "timestamp_derived_durations": timestamp_derived,
                    "all_attempt_p95_seconds": p95,
                    "p95_scope": "all_terminal_attempts",
                    "success_p95_seconds": success_p95,
                    "completion_rate": (
                        round(completed / terminal, 4) if terminal else None
                    ),
                    "statuses": statuses,
                    "target_seconds": 30.0 if lane == "live" else None,
                    "target_met": (
                        p95 < 30.0 if lane == "live" and p95 is not None else None),
                }
        return result

    def stabilization_summary(
        self, *, integrity: dict | None = None, sample_target: int = 100,
    ) -> dict:
        """Fail-closed operational readiness, separate from strategy promotion."""
        cohort = self.acceptance_cohort()
        cohort_id = str(cohort.get("cohort_id") or "") or None
        cohort_revision = str(cohort.get("revision") or "") or None
        target = max(1, int(
            cohort.get("sample_target") if cohort_id else sample_target))
        cohort_policy = dict(cohort.get("policy") or {})
        # Missing values mean the cohort predates independent denominators.
        # Fall back to its original live-attempt target so a completed v2
        # cohort is never retroactively relabelled by v3 code.
        decision_sample_target = max(1, safe_int(
            cohort_policy.get("decision_minimum_samples"), target))
        mark_sample_target = max(1, safe_int(
            cohort_policy.get("position_mark_minimum_samples"), target))
        decision_lag_maximum = max(0, safe_int(
            cohort_policy.get("decision_lag_maximum_blocks"),
            FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS))
        decision_lag_minimum_rate = safe_float(
            cohort_policy.get("decision_lag_minimum_rate"), 0.99)
        decision_usefulness_minimum_rate = safe_float(
            cohort_policy.get("decision_usefulness_minimum_rate"), 0.99)
        position_mark_minimum_rate = safe_float(
            cohort_policy.get("position_mark_minimum_rate"), 1.0)
        performance = self.lane_performance(
            limit=target, cohort_id=cohort_id, revision=cohort_revision)
        ownership = self.running_run_audit()
        with self.connection() as connection:
            if cohort_id:
                attempt_rows = [dict(row) for row in connection.execute(
                    """SELECT run_id,status,summary_json FROM runs
                       WHERE lane='live' AND status!='running'
                         AND acceptance_cohort_id=? AND revision=?
                       ORDER BY id ASC LIMIT ?""",
                    (cohort_id, cohort_revision, target),
                )]
                live_rows = [
                    row for row in attempt_rows
                    if row["status"] == "complete"]
                recent_live_statuses = attempt_rows
                # Reliability is an immutable first-N attempt cohort. Useful
                # decisions are a different population: idle cycles are not
                # opportunities, so inspect enough cohort attempts to collect
                # N actual decision-bearing opportunities. Limiting this to
                # the first N attempts made a 100-opportunity target
                # mathematically impossible whenever even one cycle was idle.
                decision_rows = [dict(row) for row in connection.execute(
                    """SELECT run_id,status,summary_json FROM runs
                       WHERE lane='live' AND status!='running'
                         AND acceptance_cohort_id=? AND revision=?
                       ORDER BY id ASC LIMIT ?""",
                    (cohort_id, cohort_revision, max(1_000, target * 20)),
                )]
                mark_rows = [dict(row) for row in connection.execute(
                    """SELECT status,summary_json FROM runs
                       WHERE lane='marks' AND status!='running'
                         AND acceptance_cohort_id=? AND revision=?
                       ORDER BY id ASC LIMIT ?""",
                    (cohort_id, cohort_revision, target),
                )]
            else:
                live_rows = [dict(row) for row in connection.execute(
                    """SELECT status,summary_json FROM runs
                       WHERE lane='live' AND status='complete'
                       ORDER BY id DESC LIMIT ?""", (target,)
                )]
                recent_live_statuses = [dict(row) for row in connection.execute(
                    """SELECT status,summary_json FROM runs WHERE lane='live'
                       ORDER BY id DESC LIMIT ?""", (target,)
                )]
                decision_rows = [dict(row) for row in connection.execute(
                    """SELECT status,summary_json FROM runs
                       WHERE lane='live' AND status!='running'
                       ORDER BY id DESC LIMIT ?""", (max(1_000, target * 10),)
                )]
                mark_rows = []
            identity_violations = int(connection.execute(
                """SELECT COUNT(*) FROM flow_observation_classifications
                   WHERE paper_eligible=1 AND identity_tier!='verified'"""
            ).fetchone()[0])

            def backlog_series(lane: str, key: str) -> list[int]:
                filters = ["lane=?", "status='complete'"]
                parameters: list[object] = [lane]
                order = "DESC"
                if cohort_id:
                    filters.extend([
                        "acceptance_cohort_id=?", "revision=?"])
                    parameters.extend([cohort_id, cohort_revision])
                parameters.append(10)
                rows = connection.execute(
                    "SELECT summary_json FROM runs WHERE "
                    + " AND ".join(filters)
                    + f" ORDER BY id {order} LIMIT ?", parameters,
                ).fetchall()
                values: list[int] = []
                for item in reversed(rows):
                    try:
                        value = (json.loads(item["summary_json"] or "{}").get(
                            "backlog") or {}).get(key)
                        if value is not None:
                            values.append(max(0, int(value)))
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue
                return values

            backfill_values = backlog_series("backfill", "pending_blocks")
            analysis_values = backlog_series("analysis", "pending_analysis")
            backfill_summary_filters = ["lane='backfill'", "status='complete'"]
            backfill_summary_parameters: list[object] = []
            if cohort_id:
                backfill_summary_filters.extend([
                    "acceptance_cohort_id=?", "revision=?"])
                backfill_summary_parameters.extend([
                    cohort_id, cohort_revision])
            backfill_summary_parameters.append(10)
            backfill_summary_rows = connection.execute(
                "SELECT summary_json FROM runs WHERE "
                + " AND ".join(backfill_summary_filters)
                + " ORDER BY id DESC LIMIT ?",
                backfill_summary_parameters,
            ).fetchall()
            backfill_summaries: list[dict] = []
            for item in reversed(backfill_summary_rows):
                try:
                    backfill_summaries.append(json.loads(
                        item["summary_json"] or "{}"))
                except (TypeError, ValueError, json.JSONDecodeError):
                    backfill_summaries.append({})

        lags: list[float] = []
        # Reported beside the rate: an exclusion nobody can see is
        # indistinguishable from a population that was never contaminated.
        lag_excluded_no_decision = 0
        lag_excluded_stale_research = 0
        marks_complete = 0
        for row in live_rows:
            try:
                summary = json.loads(row.get("summary_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                summary = {}
            # Only cycles that actually produced a decision.
            #
            # decision_head_lag_blocks is recorded whenever the head is read,
            # including on cycles that sealed nothing -- 266 of 1,309 entries
            # in the cohort at 9a88bdc. A cycle with no sealed observation
            # made no decision, so its lag measures how stale a decision
            # WOULD have been had one existed. Counting it answers a
            # counterfactual, not the SLO.
            #
            # It is not a large distortion: 97.33% combined against 97.60%
            # scoped, and the rule still fails. The point is that the
            # population now matches what the rule claims to measure.
            if not cohort_id:
                marks = summary.get("position_evaluations") or {}
                if (
                    safe_int(marks.get("checked"), 0)
                    == safe_int(marks.get("marked"), 0)
                    and safe_int(marks.get("failures"), 0) == 0
                    and safe_int(marks.get("unverified"), 0) == 0
                ):
                    marks_complete += 1
        if cohort_id:
            for row in mark_rows:
                try:
                    summary = json.loads(row.get("summary_json") or "{}")
                except (TypeError, ValueError, json.JSONDecodeError):
                    summary = {}
                marks = summary.get("position_evaluations") or {}
                if (
                    row["status"] == "complete"
                    and safe_int(marks.get("checked"), 0)
                    == safe_int(marks.get("marked"), 0)
                    and safe_int(marks.get("failures"), 0) == 0
                    and safe_int(marks.get("unverified"), 0) == 0
                ):
                    marks_complete += 1

        def trend(values: list[int]) -> dict:
            current = values[-1] if values else None
            oldest = values[0] if values else None
            enough = len(values) >= 3
            decreasing = bool(
                enough and current is not None and oldest is not None
                and (current == 0 or current < oldest)
            )
            return {
                "samples": len(values), "oldest": oldest, "current": current,
                "decreasing": decreasing,
            }

        completed = len(live_rows)
        terminal_attempts = len(recent_live_statuses)
        lag_rate = (
            sum(value <= decision_lag_maximum
                for value in lags) / len(lags)
            if lags else None
        )
        mark_samples = len(mark_rows) if cohort_id else completed
        mark_rate = marks_complete / mark_samples if mark_samples else None
        live_perf = performance.get("live") or {}
        # A controlled deferral is the lane declining to act without adequate
        # headroom -- exactly the fail-closed behaviour policy asks for -- and
        # it is set only on designed yields: the ingestion child-deadline
        # preemption. A lane-deadline breach becomes deadline_exceeded and an
        # unexpected exception becomes failed, both on separate paths.
        #
        # Counting it as unreliability made correct behaviour score as
        # breakage: 117 complete of 127 terminal read 92.1% and FAILED, where
        # 117 of 118 non-deferred is 99.2%. Worse, it made the measure fight
        # its own remedy -- tightening the decision-head headroom converts
        # completions into controlled deferrals, so the honest fix for
        # decision lag would have LOWERED this score.
        #
        # The deferral rate still needs its own bound. A lane deferring nine
        # cycles in ten is not healthy merely because none of them crashed,
        # so it is reported and capped rather than folded silently into a pass.
        healthy = {"complete", "deferred"}
        deferred_count = sum(
            row["status"] == "deferred" for row in recent_live_statuses)
        deferral_rate = (
            deferred_count / len(recent_live_statuses)
            if recent_live_statuses else None
        )
        reliability_pass = bool(
            len(recent_live_statuses) >= target
            and all(row["status"] in healthy for row in recent_live_statuses)
            and deferral_rate is not None
            and deferral_rate <= LIVE_DEFERRAL_RATE_MAX
        )
        decision_opportunities = 0
        useful_decisions = 0
        for row in decision_rows:
            try:
                summary = json.loads(row.get("summary_json") or "{}")
            except (TypeError, ValueError, json.JSONDecodeError):
                summary = {}
            classification = summary.get("classification") or {}
            observation = summary.get("observation_seal") or {}
            processed = safe_int(
                classification.get("scoped_rows_processed"), 0)
            selected = safe_int(
                classification.get("scoped_rows_selected"), 0)
            sealed = safe_int(observation.get("sealed_this_cycle"), 0)
            lag = observation.get("decision_head_lag_blocks")
            stale_research = bool(observation.get("research_only_stale"))
            actual_decision = bool(
                row["status"] == "complete" and processed > 0 and sealed > 0
                and not stale_research)
            if lag is not None and actual_decision and len(lags) < target:
                lags.append(safe_float(lag, float("inf")))
            elif lag is not None and stale_research:
                lag_excluded_stale_research += 1
            elif lag is not None and not actual_decision:
                lag_excluded_no_decision += 1
            failure_stage = str(summary.get("failure_stage") or "")
            opportunity = bool(
                not stale_research and (
                    selected > 0 or sealed > 0
                or failure_stage.startswith((
                    "fresh_quote_and_observation", "decision_head",
                    "classification"))))
            if not opportunity:
                continue
            decision_opportunities += 1
            if row["status"] == "complete" and processed > 0:
                useful_decisions += 1
            # The usefulness and lag criteria are intentionally sampled from
            # the same ordered cohort scan, but they have distinct
            # populations.  Do not stop after N opportunities while the lag
            # criterion has only a handful of decisions -- that produced a
            # statistically green 1/1 or 5/5 result labelled as 100 attempts.
            if decision_opportunities >= target and len(lags) >= target:
                break
        useful_decision_rate = (
            useful_decisions / decision_opportunities
            if decision_opportunities else None)
        lag_rate = (
            sum(value <= decision_lag_maximum
                for value in lags) / len(lags)
            if lags else None
        )
        seal_model = _seal_cost_model_from_state(
            self.scheduler_state(SEAL_COST_MODEL_STATE_KEY))
        if cohort_id:
            # The rolling estimator intentionally forgets old samples; an
            # acceptance cohort must not.  Aggregate the append-only table by
            # run so selection and settlement together count once per cycle.
            with self.connection() as connection:
                durable = [dict(row) for row in connection.execute(
                    """SELECT run_id,
                              MAX(status='stalled') AS stalled,
                              MAX(status='success') AS succeeded
                       FROM seal_cost_samples
                       WHERE acceptance_cohort_id=? AND revision=?
                         AND epoch=? AND component IN (
                             'fixed_observation_cost','queue_settlement')
                       GROUP BY run_id""",
                    (cohort_id, cohort_revision, SEAL_COST_MODEL_EPOCH),
                )]
            fixed_stalls = sum(bool(row["stalled"]) for row in durable)
            fixed_success = sum(
                bool(row["succeeded"]) and not bool(row["stalled"])
                for row in durable)
            stall_population = fixed_success + fixed_stalls
            stall_rate = (
                fixed_stalls / stall_population if stall_population else 0.0)
            stall_guard_active = bool(
                stall_population >= SEAL_STALL_GUARD_MIN_SAMPLES
                and stall_rate > SEAL_STALL_RATE_MAX)
        else:
            stall_population = safe_int(
                seal_model.get("fixed_stall_population"), 0)
            stall_rate = safe_float(
                seal_model.get("fixed_stall_rate"), 0.0)
            stall_guard_active = bool(seal_model.get("stall_guard_active"))
        stall_guard_pass = bool(
            stall_population >= SEAL_STALL_GUARD_MIN_SAMPLES
            and not stall_guard_active)
        backfill_trend = trend(backfill_values)
        # Net queue depth is the acceptance invariant, but it did not explain
        # whether a red result meant a dead worker or incoming gaps outrunning
        # useful recovery.  Decompose the SAME oldest->current interval into
        # gross recovered blocks and inferred arrivals.  Exclude the first
        # summary's recovery because ``oldest`` is its post-run backlog.
        interval_backfill = backfill_summaries[1:]
        recovered_blocks = sum(safe_int(
            (summary.get("durable_gap_recovery") or {}).get(
                "blocks_scanned"), 0)
            for summary in interval_backfill)
        productive_runs = sum(
            safe_int((summary.get("durable_gap_recovery") or {}).get(
                "blocks_scanned"), 0) > 0
            for summary in interval_backfill)
        provider_deferrals = sum(bool(
            (summary.get("durable_gap_recovery") or {}).get(
                "provider_deferred"))
            for summary in interval_backfill)
        net_backlog_change = (
            backfill_trend["current"] - backfill_trend["oldest"]
            if backfill_trend["current"] is not None
            and backfill_trend["oldest"] is not None else None)
        inferred_arrivals = (
            max(0, net_backlog_change + recovered_blocks)
            if net_backlog_change is not None else None)
        backfill_trend.update({
            "net_change_blocks": net_backlog_change,
            "gross_recovered_blocks": recovered_blocks,
            "inferred_arrival_blocks": inferred_arrivals,
            "productive_runs": productive_runs,
            "provider_deferrals": provider_deferrals,
            "recovery_to_arrival_ratio": (
                round(recovered_blocks / inferred_arrivals, 4)
                if inferred_arrivals else (
                    None if inferred_arrivals is None else 1.0)),
        })
        analysis_trend = trend(analysis_values)
        integrity = dict(integrity or {})
        integrity_pass = bool(integrity.get("ok"))
        criteria = {
            "live_cycle_sample": {
                "pass": terminal_attempts >= target,
                "value": terminal_attempts,
                "target": target, "label": "Terminal live attempts",
            },
            "live_p95": {
                "pass": bool(live_perf.get("target_met")),
                "value": live_perf.get("all_attempt_p95_seconds"), "target": "<30s",
                "label": "Live-lane p95",
            },
            "decision_lag": {
                "pass": bool(
                    len(lags) >= decision_sample_target
                    and lag_rate is not None
                    and lag_rate >= decision_lag_minimum_rate),
                "value": lag_rate, "samples": len(lags),
                "sample_target": decision_sample_target,
                "target": (
                    f">={decision_lag_minimum_rate:.0%} "
                    f"<={decision_lag_maximum} blocks across "
                    f">={decision_sample_target} decisions"),
                # Cycles that read a head but sealed nothing. They made no
                # decision, so they are outside the SLO -- but the count is
                # published, because a silent exclusion cannot be audited.
                "excluded_no_decision": lag_excluded_no_decision,
                "excluded_stale_research":
                    lag_excluded_stale_research,
                "label": "Decision-lag SLO",
            },
            "position_marks": {
                "pass": bool(
                    mark_rate is not None
                    and mark_rate >= position_mark_minimum_rate
                    and (not cohort_id
                         or mark_samples >= mark_sample_target)),
                "value": mark_rate, "samples": mark_samples,
                "sample_target": mark_sample_target,
                "target": (
                    f">={position_mark_minimum_rate:.0%} across "
                    f">={mark_sample_target} cohort marks"
                    if cohort_id else "100%"),
                "label": "Complete position marks",
            },
            "run_ownership": {
                "pass": ownership["stale"] == 0 and not ownership["overlap_lanes"],
                "value": ownership["stale"], "target": "0 stale / 0 overlaps",
                "label": "Run ownership",
            },
            "live_reliability": {
                "pass": reliability_pass,
                "value": (sum(row["status"] in healthy
                              for row in recent_live_statuses)),
                "samples": len(recent_live_statuses),
                "target": f"{target}/{target}",
                # Published separately so a lane that defers its way to a
                # passing score is visible rather than merely compliant.
                "completed": sum(row["status"] == "complete"
                                 for row in recent_live_statuses),
                "controlled_deferrals": deferred_count,
                "controlled_deferral_rate": (
                    None if deferral_rate is None else round(deferral_rate, 4)),
                "controlled_deferral_rate_limit": LIVE_DEFERRAL_RATE_MAX,
                "label": "Live-cycle reliability",
            },
            "decision_usefulness": {
                "pass": bool(
                    decision_opportunities >= decision_sample_target
                    and useful_decision_rate is not None
                    and useful_decision_rate
                    >= decision_usefulness_minimum_rate),
                "value": useful_decision_rate,
                "samples": decision_opportunities,
                "useful": useful_decisions,
                "sample_target": decision_sample_target,
                "target": (
                    f">={decision_usefulness_minimum_rate:.0%} useful across "
                    f">={decision_sample_target} opportunities"),
                "label": "Decision-bearing usefulness",
            },
            "seal_stall_guard": {
                "pass": stall_guard_pass,
                "value": stall_rate,
                "samples": stall_population,
                "active": stall_guard_active,
                "target": (
                    f"<={SEAL_STALL_RATE_MAX:.1%} after "
                    f">={SEAL_STALL_GUARD_MIN_SAMPLES} samples"),
                "label": "Seal-stage stall rate",
            },
            "backfill_convergence": {
                "pass": backfill_trend["decreasing"], "value": backfill_trend,
                "target": "net decreasing (gross recovery shown separately)",
                "label": "Backfill convergence",
            },
            "analysis_convergence": {
                "pass": analysis_trend["decreasing"], "value": analysis_trend,
                "target": "decreasing", "label": "Analysis backlog",
            },
            "identity_fail_closed": {
                "pass": identity_violations == 0, "value": identity_violations,
                "target": 0, "label": "Identity-incomplete admissions",
            },
            "integrity": {
                "pass": integrity_pass, "value": integrity,
                "target": "all verified", "label": "Ledger / DB / Timechain",
            },
        }
        if cohort_id:
            expected_policy_hash = hashlib.sha256(
                _canonical(cohort.get("policy") or {}).encode("utf-8")
            ).hexdigest()
            provenance_ok = bool(
                cohort.get("policy_hash") == expected_policy_hash
                and safe_int(cohort.get("revision_mismatches"), 0) == 0
                # Both, not either. A clean revision with a moved worktree is
                # exactly as contaminated and far harder to notice.
                and safe_int(cohort.get("source_mismatches"), 0) == 0
                and not bool(cohort.get("checkout_revision_mismatch")))
            criteria["cohort_provenance"] = {
                "pass": provenance_ok,
                "value": {
                    "cohort_id": cohort_id,
                    "revision": cohort_revision,
                    "revision_mismatches": safe_int(
                        cohort.get("revision_mismatches"), 0),
                    "source_mismatches": safe_int(
                        cohort.get("source_mismatches"), 0),
                    "checkout_revision": cohort.get("checkout_revision"),
                    "checkout_revision_mismatch": bool(
                        cohort.get("checkout_revision_mismatch")),
                    "policy_hash": cohort.get("policy_hash"),
                },
                "target": "pinned revision / unchanged policy",
                "label": "Frozen cohort provenance",
            }
        all_pass = all(item["pass"] for item in criteria.values())
        critical_keys = [
            "run_ownership", "identity_fail_closed", "integrity"]
        if cohort_id:
            critical_keys.append("cohort_provenance")
        critical_fail = any(
            not criteria[key]["pass"] for key in critical_keys)
        operational_fail = any(not criteria[key]["pass"] for key in (
            "live_p95", "position_marks", "live_reliability",
            "decision_usefulness", "seal_stall_guard",
        ))
        if all_pass:
            status = "STABILIZED"
        elif critical_fail:
            status = "DEGRADED"
        elif terminal_attempts < target:
            status = "COLLECTING_DATA"
        elif operational_fail:
            status = "DEGRADED"
        else:
            status = "STABILIZING"
        return {
            "schema_version": 1, "status": status,
            "stabilized": all_pass, "sample_target": target,
            "completed_live_cycles": completed,
            "terminal_live_attempts": terminal_attempts,
            "acceptance_cohort": cohort,
            "criteria_passed": sum(item["pass"] for item in criteria.values()),
            "criteria_total": len(criteria), "criteria": criteria,
            "run_ownership": ownership,
            "note": (
                "Operational stabilization is independent of paper-strategy "
                "promotion readiness. No result enables live execution."
            ),
        }

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
            # Do not predicate on status here.  SQLite selected
            # idx_transaction_origin_status and scanned every resolved origin
            # before applying a 12-hash IN list: 7.7s inside a 9.2s freshness
            # budget.  Starting from the PRIMARY KEY turns this into exactly
            # N point lookups.  Status and attempts are then interpreted from
            # that bounded result set in Python.
            known = {
                row["transaction_hash"]: row
                for row in connection.execute(
                    "SELECT transaction_hash,status,attempts"
                    " FROM transaction_origins"
                    " WHERE transaction_hash IN (" + placeholders + ")",
                    wanted,
                )
            }
        return [
            digest for digest in wanted
            if digest not in known or (
                known[digest]["status"] != "resolved"
                and safe_int(known[digest]["attempts"], 0)
                    < FLOW_ORIGIN_MAXIMUM_ATTEMPTS
            )
        ]

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

    def open_position(
        self, candidate: dict, market: dict, *,
        decision_commitment_id: str | None = None,
    ) -> bool:
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
                    token_address,decision_commitment_id,symbol,status,
                    opened_at,entry_price_usd,
                    entry_liquidity_usd,cost_usd,quantity,entry_friction_bps,
                    entry_market_cap_usd,high_multiple,last_price_usd,
                    last_liquidity_usd,last_market_cap_usd,last_market_observed_at,
                    last_mark_at,original_quantity,realized_value_usd,
                    runner_high_multiple,entry_policy_version,grandfathered_above_cap,
                    entry_pool_reserve_fraction,last_pool_reserve_fraction,
                    minimum_pool_reserve_fraction
                ) VALUES (?,?,?,'open',?,?,?,?,?,?,?,1,?,?,?,?,?,?,0,1,?,0,?,?,?)
                """,
                (
                    candidate["token_address"], decision_commitment_id,
                    candidate.get("symbol") or "", time.time(),
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

    def position_effect_state(
        self, commitment_id: str, token_address: str,
    ) -> dict:
        """Return causal action evidence for crash recovery.

        A position proves this commitment executed only when it carries the
        exact commitment id. Its current status is irrelevant: a position
        that opened and later closed still proves the original action.
        Database errors intentionally propagate so the caller can preserve
        an indeterminate state instead of fabricating absence.
        """
        with self.connection() as connection:
            row = connection.execute(
                """SELECT token_address,status FROM positions
                   WHERE decision_commitment_id=?""",
                (str(commitment_id),),
            ).fetchone()
        if row is None:
            return {"state": "absent", "detail": "no_commitment_link"}
        if str(row["token_address"] or "").lower() != \
                str(token_address or "").lower():
            return {
                "state": "indeterminate",
                "detail": "commitment_link_token_mismatch",
            }
        return {
            "state": "executed",
            "detail": f"linked_position_status:{row['status']}",
        }

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
                resolution = market.get("market_resolution") or {}
                authoritative_no_market = bool(
                    resolution.get("authoritative_no_liquidity")
                    or resolution.get("confirmed_no_market")
                    or market.get("confirmed_no_market"))
                next_unverified = safe_int(
                    position["consecutive_unverified_marks"], 0) + 1
                connection.execute(
                    """
                    UPDATE positions SET unverified_marks=unverified_marks+1,
                        consecutive_unverified_marks=consecutive_unverified_marks+1,
                        last_unverified_mark_at=?
                    WHERE token_address=?
                    """,
                    (now, token.lower()),
                )
                if (authoritative_no_market
                        and next_unverified
                        >= POSITION_TERMINAL_NO_MARKET_OBSERVATIONS):
                    realized = safe_float(
                        position["realized_value_usd"], 0.0)
                    multiple = realized / max(
                        0.01, safe_float(position["cost_usd"], 0.0))
                    reason = "market_disappeared"
                    connection.execute(
                        """UPDATE positions SET status='closed',quantity=0,
                               last_price_usd=0,last_liquidity_usd=0,
                               last_market_observed_at=?,last_mark_at=?,
                               exit_price_usd=0,exit_value_usd=?,exit_reason=?,
                               closed_at=?,net_multiple=?,
                               consecutive_unverified_marks=0,
                               last_verified_mark_at=?
                           WHERE token_address=?""",
                        (_utc_now(), now, realized, reason, now, multiple,
                         now, token.lower()),
                    )
                    connection.execute(
                        """UPDATE position_policy_states SET status='closed',
                               exit_price_usd=0,exit_value_usd=?,exit_reason=?,
                               closed_at=?,net_multiple=?,last_mark_at=?
                           WHERE token_address=? AND status='open'""",
                        (realized, reason, now, multiple, now, token.lower()),
                    )
                    return {
                        "token_address": token.lower(), "verified": True,
                        "observation_status": "terminal_confirmed",
                        "reason": reason, "market_reason": reason,
                        "net_multiple": multiple, "partial_exits": [],
                    }
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
    def __init__(
        self, rpc: RobinhoodRPC, store: RobinhoodLearningStore,
        state_path: str | Path, *,
        remote_attempts: int = REMOTE_RETRY_ATTEMPTS,
    ):
        self.rpc = rpc
        self.store = store
        self.state_path = Path(state_path)
        self.remote_attempts = max(1, int(remote_attempts))

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
                    attempts=self.remote_attempts,
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

    def sync(
        self, *, block_limit: int, lookback: int,
        activation_limit: int | None = None,
    ) -> tuple[list[dict], dict]:
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
        activations_available = len(activations)
        if activation_limit is not None:
            activations = activations[:max(0, int(activation_limit))]
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
            "activations_available": activations_available,
            "activations_deferred": max(
                0, activations_available - len(activations)),
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
            except CycleDeadlineExceeded as exc:
                # Distinct from a crash: the cycle was interrupted on purpose.
                self.store.finish_run(cycle_run_id, "deadline_exceeded")
                raise
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
        #: Reads a batched prime resolved ahead of the per-window seal loop.
        #: Keyed by (to, calldata, block tag) and cleared on every prime, so a
        #: `latest` read can never survive the cycle that fetched it.
        self._quote_cache: dict[tuple[str, str, str], str | None] = {}

    @staticmethod
    def _quote_cache_key(to_address, data, block) -> tuple[str, str, str]:
        return (
            str(to_address).lower(), str(data).lower(),
            hex(block) if isinstance(block, int) else str(block),
        )

    def _cached_call(self, to_address: str, data: str, block=None) -> str:
        """An eth_call the prime batch may already have answered.

        A miss is not an error -- it falls through to the live call -- so
        priming stays a latency optimisation and never a correctness
        dependency. That matters because the batch is best-effort: a provider
        that rejects batching, or one call that errors inside an accepted
        batch, must degrade to the sequential path rather than to no quote.
        """
        cached = self._quote_cache.get(
            self._quote_cache_key(to_address, data, block), _CACHE_MISS)
        if cached is not _CACHE_MISS:
            return cached
        return self.rpc.call(to_address, data, block=block)

    def prime_window_quotes(
        self, windows: list[dict], *, deadline: "CycleDeadline | None" = None,
    ) -> dict:
        """Resolve every window's independent reads in one batched round trip.

        snapshot() issues roughly six eth_calls per window and five of them
        depend on nothing but the pool and the pinned block: getLiquidity,
        getSlot0, decimals(token), decimals(anchor), totalSupply(token), plus
        the singleton balanceOf at latest. Run per window they are sequential
        HTTP round trips -- about 48 of them for the 8-window live limit,
        which is where the seal stage's 16.4s median (19.9s p95, against a
        25s cycle budget) came from.

        Batching is the parallel form that fits this client: one HTTP request
        carrying every independent call, rather than threads over a session
        whose request-id counter and provenance ledger are not synchronised.
        The quoter's two calls per window stay sequential because the second
        consumes the first's output; the sub-stage timers now report whether
        that residue is worth restructuring.
        """
        self._quote_cache = {}
        if not windows:
            return {"batched": 0, "resolved": 0, "failed": 0, "windows": 0}
        if deadline is not None and deadline.expired():
            return {"batched": 0, "resolved": 0, "failed": 0,
                    "windows": len(windows), "skipped": "deadline"}
        singleton = "0x" + ERC20_BALANCE_OF_SELECTOR + "0" * 24 +             UNISWAP_V4_POOL_MANAGER.removeprefix("0x")
        decimals_data = "0x" + ERC20_DECIMALS_SELECTOR + "0" * 56
        supply_data = "0x" + ERC20_TOTAL_SUPPLY_SELECTOR + "0" * 56
        # Deduplicated: windows share anchors (nearly always WETH or USDG) and
        # a token can hold several pools, so the same call would otherwise be
        # issued once per window.
        planned: dict[tuple[str, str, str], tuple[str, str, object]] = {}

        def plan(to_address, data, block):
            if not to_address:
                return
            planned.setdefault(
                self._quote_cache_key(to_address, data, block),
                (to_address, data, block))

        for window in windows:
            pool_id = str(window.get("pool_id") or "")
            if len(pool_id.removeprefix("0x")) != 64:
                continue
            token = window.get("token_address")
            block = window.get("window_end_block")
            block = int(block) if block is not None else None
            argument = pool_id.removeprefix("0x")
            pool = self.store.v4_pool(pool_id) or {}
            plan(UNISWAP_V4_STATE_VIEW,
                 "0x" + V4_GET_LIQUIDITY_SELECTOR + argument, block)
            plan(UNISWAP_V4_STATE_VIEW,
                 "0x" + V4_GET_SLOT0_SELECTOR + argument, block)
            plan(token, decimals_data, block)
            plan(pool.get("anchor_address"), decimals_data, block)
            plan(token, supply_data, block)
            plan(token, singleton, None)

        ordered = list(planned.items())
        try:
            responses = self.rpc.calls_at_blocks(
                [call for _, call in ordered])
        except Exception as error:
            # Batching refused or the transport failed. Every read falls back
            # to its sequential path, so the cycle is slower but not wrong.
            return {"batched": len(ordered), "resolved": 0,
                    "failed": len(ordered), "windows": len(windows),
                    "error": str(error)[:200]}
        resolved = failed = 0
        for (key, _), response in zip(ordered, responses):
            if response.get("error") is not None:
                failed += 1
                continue
            self._quote_cache[key] = response.get("result")
            resolved += 1
        return {"batched": len(ordered), "resolved": resolved,
                "failed": failed, "windows": len(windows)}

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
            raw = self._cached_call(
                token,
                "0x" + ERC20_BALANCE_OF_SELECTOR + "0" * 24
                + UNISWAP_V4_POOL_MANAGER.removeprefix("0x"),
            )
        except Exception:
            return None
        held = int(raw or "0x0", 16)
        return held / total_supply

    def _cached_decimals(self, token: str, block=None) -> int:
        """Cache hit decodes; a miss delegates to the RPC client's accessor.

        The miss path must call erc20_decimals rather than rebuild its
        calldata. Reconstructing it here silently bypasses any override on the
        client -- which is not hypothetical: it broke every test whose fake
        RPC answers at the accessor, and any deployment wrapping the accessor
        would have been bypassed just as quietly in production.
        """
        raw = self._quote_cache.get(
            self._quote_cache_key(
                token, "0x" + ERC20_DECIMALS_SELECTOR + "0" * 56, block),
            _CACHE_MISS)
        if raw is _CACHE_MISS:
            return self.rpc.erc20_decimals(token, block=block)
        return int(raw, 16) if raw else 18

    def _cached_total_supply(self, token: str, block=None) -> int:
        raw = self._quote_cache.get(
            self._quote_cache_key(
                token, "0x" + ERC20_TOTAL_SUPPLY_SELECTOR + "0" * 56, block),
            _CACHE_MISS)
        if raw is _CACHE_MISS:
            return self.rpc.erc20_total_supply(token, block=block)
        return int(raw, 16) if raw else 0

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
        liquidity_raw = self._cached_call(
            UNISWAP_V4_STATE_VIEW,
            "0x" + V4_GET_LIQUIDITY_SELECTOR + argument,
            block=quote_block,
        )
        slot0_raw = self._cached_call(
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
        token_decimals = self._cached_decimals(token, quote_block)
        anchor_decimals = self._cached_decimals(anchor, quote_block)
        total_supply = self._cached_total_supply(token, quote_block)
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
        rpc_isolated: bool = False,
    ):
        self.root=Path(root)
        self.root.mkdir(parents=True,exist_ok=True)
        self.rpc=rpc or RobinhoodRPC(ROBINHOOD_NETWORK.rpc_url)
        self.backfill_rpc_isolated = bool(rpc_isolated)
        self.store=RobinhoodLearningStore(self.root/"learning.sqlite3")
        self._active_lane: str | None = None
        self._active_lane_deadline: CycleDeadline | None = None
        self._rpc_gate_deadline: CycleDeadline | None = None
        self._rpc_priority_telemetry = {
            "admissions": 0, "deferrals": 0,
            "wait_seconds": 0.0, "rate_wait_seconds": 0.0,
            "provider_cooldown_wait_seconds": 0.0,
            "provider_rate_limits": 0,
            "last_reason": None,
        }
        # RobinhoodRPC invokes this context at the raw HTTP boundary for
        # _call AND _batch_call. Test doubles without a session deliberately
        # retain their existing behavior.
        if not self.backfill_rpc_isolated:
            self._bind_rpc_request_gate(self.rpc)
        self.ledger=HashEventLedger(self.root/"events.jsonl")
        self.observer=RobinhoodPairObserver(self.rpc,self.root/"discovery_cursor.json")
        self.v4_observer=RobinhoodV4Observer(self.rpc,self.store,self.root/"discovery_v4_cursor.json")
        self.v4_custody=RobinhoodV4CustodyVerifier(
            self.rpc,self.store,self.root/"v4_custody_cursor.json"
        )
        self.market=market or RobinhoodMarketClient()
        self.v4_market=RobinhoodV4MarketClient(self.rpc,self.market,self.store)
        self.analyzer=analyzer
        self._bind_rpc_request_gate(getattr(self.analyzer, "rpc", None))
        # A run id for cycles that act outside a lane context (standalone
        # market rechecks); lanes overwrite it with their own run uuid.
        self.cycle_run_uuid = "engine-" + uuid.uuid4().hex[:16]
        self.timechain_recorder = timechain_recorder
        if self.timechain_recorder is None and chain_root is not None:
            self.timechain_recorder = RobinhoodLearningTimechainRecorder(
                chain_root, skill_root=skill_root or default_skill_root(),
            )
        # Deferred Timechain sealing: the decision path only appends
        # commitments; this store is the durable queue the analysis lane --
        # the single authoritative Timechain writer -- drains.
        self.commitments = DecisionCommitmentStore(
            self.root / "decision_commitments.sqlite3")
        self.execution_gate = ExecutionGate(self.commitments)

    # ------------------------------------------------------------------ #
    # Production entry boundary: EVERY paper buy passes through here.      #
    # ------------------------------------------------------------------ #
    def guarded_paper_entry(
        self, candidate: dict, market: dict, *, run_id: str,
        priority_reason: str | None = None,
    ) -> dict:
        """The ONLY sanctioned path from an analysis verdict to a paper
        position: evaluation -> durable commitment -> FRESH revalidation
        snapshot -> atomic authorization -> open_position -> confirmed
        execution. Returns a result dict; never raises for refusals.

        Fail-closed: any gate refusal, missing certificate or excessive
        seal debt prevents the entry. This is not optional -- calling
        store.open_position directly bypasses pre-action authorization and
        is treated as a defect.

        Revalidation is fresh at the action boundary: a new RPC head and
        quote are acquired, the current stored hard-stop state is bound,
        and paper-mode quote sanity is checked. This does not claim to be a
        signed-transaction simulation. The commitment's original quote is
        what the fresh quote must MATCH.
        """
        token = str(candidate.get("token_address") or "").lower()
        from chainseer_robinhood_commitments import (
            _quote_projection,
            acquire_revalidation_snapshot,
            canonical_hard_stop_digest,
        )
        # Eligibility gate BEFORE any commitment: an ineligible candidate
        # must never be committed as BUY_ELIGIBLE -- that would corrupt
        # provenance and future training data.
        if not candidate.get("paper_entry_allowed"):
            return {"entered": False,
                    "reason": "candidate_not_paper_eligible"}
        try:
            evidence = {
                "analysis": candidate.get("shadow_admission_json") or {},
                "score": candidate.get("score"),
                "risk_level": candidate.get("risk_level"),
                "hard_stops_json": candidate.get("hard_stops_json") or "[]",
            }
            quote = {key: market[key] for key in (
                "price_usd", "liquidity_usd", "market_cap_usd",
                "pool_reserve_fraction", "current_state_verified")
                if market.get(key) is not None}
            quote = _quote_projection(
                quote, tuple(quote.keys()))
            hard_stops = json.loads(candidate.get("hard_stops_json") or "[]")
        except (TypeError, ValueError):
            hard_stops = []
        # Registry epoch from CACHED metadata: the decision path never
        # scans the producer chain (a full pass measured ~6.3 s).
        registry_epoch = self._registry_epoch_cached()
        verified_head = self.commitments.verified_head()

        # ---- FRESH revalidation snapshot FIRST: the commitment pins the
        # same block the snapshot acquired, so authorization compares the
        # fresh quote against a pin taken at the same instant.
        certificate = load_integrity_certificate(self.root)
        rings = safe_int(certificate.get("ring_count"), 0)

        def _simulate(fresh_quote: dict) -> bool:
            # Paper QUOTE-SANITY check against the CURRENT quote. This is
            # NOT a transaction simulation: paper mode has no execution
            # path to simulate. The observed result is stored on the
            # commitment as quote_sanity_ok -- never assumed.
            return bool(
                safe_float(fresh_quote.get("price_usd"), 0.0) > 0
                and safe_float(fresh_quote.get("liquidity_usd"), 0.0)
                >= MINIMUM_ENTRY_LIQUIDITY_USD
                and fresh_quote.get("current_state_verified") is not False)

        def _market_snapshot(token_arg, pair_arg=None):
            # V4 client takes the candidate; V2 client takes (token,pair).
            if self.v4_market is not None and candidate.get("pool_id"):
                return self.v4_market.snapshot(candidate)
            return self.market.snapshot(token_arg, pair_arg)

        try:
            # Read the producer's TRUE durable tail in O(1), after the
            # remote quote work and immediately before authorization. This
            # catches restart and external-writer growth without a full
            # verification or chain materialization on the decision path.
            snapshot = acquire_revalidation_snapshot(
                rpc=self.rpc, market_client=SimpleNamespace(
                    snapshot=_market_snapshot),
                candidate=candidate, token_address=token,
                hard_stops=hard_stops,
                quote_fields=tuple(quote.keys()),
                run_pre_trade_simulation=_simulate,
                producer_chain_rings=None,
                producer_ring_count_reader=(
                    self.timechain_recorder.tail_ring_count
                    if self.timechain_recorder is not None else lambda: 0),
                certificate_ring_count=rings or None,
            )
        except Exception:
            return {"entered": False, "reason": "revalidation_failed"}
        if not snapshot.valid:
            return {"entered": False,
                    "reason": f"revalidation_failed_{snapshot.source}"}
        block_pin = snapshot.block
        spec = build_commitment_spec(
            run_id=run_id, network="robinhood",
            token_address=token,
            evidence=evidence,
            evidence_block_pin=block_pin,
            quote=quote,
            quote_block=block_pin,
            decision="buy_eligible",
            hard_stops=hard_stops,
            policy_version=candidate.get("entry_policy_version")
            or "robinhood-paper-v1",
            faculty_registry_epoch=registry_epoch,
            verified_head=verified_head,
            # The OBSERVED quote-sanity result from the fresh snapshot --
            # never assumed True. The gate re-checks it at authorization.
            simulation_ok=snapshot.simulation_ok,
            risk_score=candidate.get("score"),
            # Idempotency identifies the EXACT analysis/quote attempt --
            # not merely the token -- so each recheck of the same token
            # with fresh evidence is its own decision.
            idempotency_key=(
                f"paper-entry:{token}:{block_pin}:"
                + canonical_hash(quote)),
        )
        try:
            record = self.execution_gate.commit(spec)
        except DecisionCommitmentError as exc:
            return {"entered": False, "reason": exc.reason}
        if record.get("duplicate"):
            return {"entered": False, "reason": "duplicate_commitment"}

        # Integrity input: MANDATORY with a configured Timechain, but
        # NEVER synchronously verified here. A stale/missing certificate
        # refuses the entry; refresh happens on the analysis lane.
        if self.timechain_recorder is not None:
            from chainseer_robinhood_commitments import (
                evaluate_integrity_certificate as _eval_cert,
            )
            integrity_ok, integrity_reason, _ = _eval_cert(certificate)
            if not integrity_ok:
                self.execution_gate.record_action_result(
                    record["commitment_id"], False, integrity_reason)
                return {"entered": False, "reason": integrity_reason}
            if (snapshot.producer_tail or {}).get("lag_excessive"):
                reason = "producer_tail_lag_exceeded"
                self.execution_gate.record_action_result(
                    record["commitment_id"], False, reason)
                return {"entered": False, "reason": reason}

        authorized = self.execution_gate.authorize(
            record["commitment_id"],
            **snapshot.as_authorize_kwargs(),
            current_ring_count=snapshot.producer_tail.get("ring_count", 0),
            head_index=safe_int((verified_head or {}).get("head_index"), 0),
            head_hash=str((verified_head or {}).get("head_hash")),
            registry_epoch=spec["faculty_registry_epoch"],
            integrity_enforced=self.timechain_recorder is not None,
        )
        if not authorized["allowed"]:
            # Authorization refused: resolve the claim as aborted so no
            # ring ever seals it as executed.
            self.execution_gate.record_action_result(
                record["commitment_id"], False, authorized["reason"])
            return {"entered": False, "reason": authorized["reason"]}

        # Action attempt: only a REAL open_position success confirms
        # execution; failure aborts the commitment instead.
        if self.store.open_position(
                candidate, market,
                decision_commitment_id=record["commitment_id"]):
            self.execution_gate.record_executed(
                record["commitment_id"],
                f"paper entry via {priority_reason or 'analysis'}")
            self.ledger.append("robinhood_paper_buy", {
                "token_address": token,
                "symbol": candidate.get("symbol"),
                "score": candidate.get("score"),
                "commitment_id": record["commitment_id"],
                "priority_reason": priority_reason,
                "paper_only": True,
            })
            return {"entered": True, "reason": "authorized",
                    "commitment_id": record["commitment_id"]}
        resolved = self.execution_gate.record_action_result(
            record["commitment_id"], False, "position_store_refused")
        return {"entered": False, "reason": "position_store_refused",
                "resolved": resolved.get("resolved")}

    def _timechain_ring_count(self) -> int:
        """Ring count from the last published certificate -- NEVER a live
        full verification on this path."""
        certificate = load_integrity_certificate(self.root)
        return safe_int(certificate.get("ring_count"), 0)

    def _registry_epoch_cached(self) -> str:
        """Registry epoch from the CACHED certificate -- never a chain
        scan. Falls back to the last known identifier so the decision
        path stays O(1) in chain length."""
        certificate = load_integrity_certificate(self.root)
        epoch = str(certificate.get("registry_epoch") or "")
        if epoch:
            return epoch
        # Certificate not published yet: reuse the last-seen value.
        if getattr(self, "_last_registry_epoch", None):
            return self._last_registry_epoch
        return "unknown-chain"

    def drain_deferred_seals(self, *, limit: int = 4,
                             deadline: "CycleDeadline | None" = None) -> dict:
        """Consume due seal jobs and create the full Timechain rings.

        Called ONLY from run_analysis_lane. Each job links evidence ->
        decision commitment -> action/avoidance events -> outcome context
        in one immutable ring; a BUY commitment is not sealed until it has
        reached a terminal state (executed or aborted) so the ring carries
        the full chain, not just the decision.
        """
        started = time.monotonic()
        self.commitments.recover_expired_leases()
        # Lifecycle recovery (review F2/P0): expire stale commitments and
        # reconcile claimed-but-unconfirmed ones against the position
        # store BEFORE aborting -- a crash between open_position success
        # and confirm_action must resolve as executed, not aborted.
        recovered = self.commitments.recover_expired_commitments(
            position_reconciler=self._reconcile_position_effect)
        self.commitments.requeue_retrying()
        claimed = self.commitments.claim_seal_batch(limit=limit)
        sealed = failed = deferred_not_terminal = 0
        last_ring = None
        graph_refreshed = False
        for job in claimed:
            if deadline is not None and deadline.remaining() <= 0.0:
                # Bounded: release the un-claimed work back for the next
                # analysis cycle instead of running past the lane budget.
                self.commitments.fail_seal(
                    job["job_id"], "drain deadline", attempts=0)
                continue
            commitment = self.commitments.get(job["commitment_id"])
            if commitment is None:
                self.commitments.fail_seal(
                    job["job_id"], "commitment row missing")
                failed += 1
                continue
            events = self.commitments.latest_events(commitment[
                "commitment_id"])
            statuses = {event["status"] for event in events}
            terminal = bool(statuses & {
                "executed", "executed_outcome", "aborted", "superseded"})
            if commitment["decision"] != "BUY_ELIGIBLE" and not terminal:
                # A REJECT has no action to await: it is terminal at
                # creation (its avoidance IS the outcome).
                terminal = True
            if not terminal:
                # A BUY that has neither executed nor been aborted has no
                # action to link yet. Leave it queued (without burning a
                # retry attempt) so the ring carries the FULL chain:
                # evidence -> decision -> action -> outcome.
                self.commitments.fail_seal(
                    job["job_id"], "awaiting terminal state",
                    attempts=max(0, int(job.get("attempts") or 1) - 1))
                deferred_not_terminal += 1
                continue
            payload = {
                "summary": (
                    f"Deferred Timechain seal for decision commitment "
                    f"{commitment['commitment_id']} "
                    f"({commitment['decision']}) on "
                    f"{commitment['token_address']}; full ring created "
                    "asynchronously by the analysis lane."
                ),
                "event": "robinhood_decision_commitment_sealed",
                "network": commitment["network"],
                "token_address": commitment["token_address"],
                "decision_commitment": {
                    key: value for key, value in commitment.items()
                    if key != "id"
                },
                "decision_events": events,
                # The link that makes the ring tamper-evident against the
                # fast path: identical to what the commitment carried.
                "evidence_hash": commitment["evidence_hash"],
                "evidence_block_pin": commitment["evidence_block_pin"],
                "quote_hash": commitment["quote_hash"],
                "hard_stop_digest": commitment["hard_stop_digest"],
                "policy_version": commitment["policy_version"],
                "faculty_registry_epoch":
                    commitment["faculty_registry_epoch"],
                "paper_only": True,
                "live_execution_enabled": False,
            }
            try:
                if self.timechain_recorder is None:
                    raise RuntimeError("producer Timechain unavailable")
                existing = self.timechain_recorder._find(
                    "robinhood-decision-commitment:"
                    + commitment["commitment_hash"])
                if existing is not None:
                    ring = existing
                else:
                    payload["idempotency_key"] = (
                        "robinhood-decision-commitment:"
                        + commitment["commitment_hash"])
                    ring = self.timechain_recorder.tc.seal(
                        "decision_commitment", payload)
                    if not graph_refreshed:
                        # Throttled: at most ONE full temporal-graph
                        # rebuild per drain, never one per ring.
                        TemporalGraphStore(self.root).refresh(
                            self.timechain_recorder.tc.load())
                        graph_refreshed = True
                self.commitments.complete_seal(
                    job["job_id"],
                    int(ring.get("index") or 0),
                    str(ring.get("ring_hash") or ""),
                )
                sealed += 1
                last_ring = {
                    "index": int(ring.get("index") or 0),
                    "ring_hash": str(ring.get("ring_hash") or ""),
                    "commitment_id": commitment["commitment_id"],
                    "decision": commitment["decision"],
                }
            except Exception as exc:
                state = self.commitments.fail_seal(
                    job["job_id"], f"{type(exc).__name__}: {exc}")
                failed += 1
                if state == "dead_letter":
                    self.commitments.record_event(
                        commitment["commitment_id"], "seal_dead_letter",
                        str(exc)[:200],
                    )
        return {
            "claimed": len(claimed), "sealed": sealed, "failed": failed,
            "deferred_not_terminal": deferred_not_terminal,
            "lifecycle_recovered": recovered,
            "last_ring": last_ring,
            "duration_seconds": round(time.monotonic() - started, 3),
        }

    def _reconcile_position_effect(
        self, commitment_id: str, token_address: str,
    ) -> dict:
        """Tri-state, commitment-linked recovery evidence."""
        try:
            return self.store.position_effect_state(
                commitment_id, token_address)
        except Exception as exc:
            return {
                "state": "indeterminate",
                "detail": f"{type(exc).__name__}: {exc}",
            }

    def publish_integrity_certificate(self) -> dict:
        """Run full-chain verification OFF the critical path and publish the
        atomic cached certificate the execution gate consumes."""
        if self.timechain_recorder is None:
            return {"published": False, "reason": "timechain_disabled"}
        ok, report = self.timechain_recorder.verify()
        rings = list(self.timechain_recorder.tc.iter_rings())
        if not rings:
            # An empty chain is NOT a verified chain: never publish a pass
            # certificate over nothing (that would fail open).
            return {"published": False, "reason": "empty_chain"}
        epoch = self._registry_epoch_identifier(rings)
        certificate = self.commitments.publish_verified_head(
            head_index=int(rings[-1]["index"]) if rings else 0,
            head_hash=str(rings[-1]["ring_hash"]) if rings else "",
            chain_root=canonical_hash([
                ring.get("ring_hash") for ring in rings]),
            registry_epoch=epoch,
            ring_count=len(rings),
            verification_result="pass" if ok else "fail",
            verifier_version="robinhood-deferred-sealing-v1",
        )
        self._last_registry_epoch = epoch
        return {"published": True, "verification_ok": bool(ok),
                "ring_count": len(rings), "certificate": certificate}

    def _registry_epoch_identifier(self, rings: list) -> str:
        """The sealed registry-epoch ring for this producer chain, or the
        genesis hash when no epoch exists (pre-epoch chains)."""
        for ring in reversed(rings):
            ring_type = ring.get("ring_type")
            if ring_type == "epoch":
                return f"epoch-ring:{ring.get('index')}"
            if ring_type == "genesis":
                payload = ring.get("payload") or {}
                if payload.get("event") == "genesis":
                    return f"genesis:{str(ring.get('ring_hash', ''))[:16]}"
        return "unknown-chain"

    def _bind_rpc_request_gate(self, rpc) -> None:
        if (rpc is not None and hasattr(rpc, "_session")
                and hasattr(rpc, "_call")):
            rpc.request_gate = self._rpc_request_guard
            if hasattr(rpc, "response_observer"):
                rpc.response_observer = self._observe_rpc_response

    def _observe_rpc_response(self, status_code: int, headers: dict) -> None:
        """Publish provider throttling as shared inter-process state."""
        now_mono = time.monotonic()
        now_wall = time.time()
        path = self.root / PROVIDER_RPC_THROTTLE_STATE_FILE
        previous = read_json(path, {}) or {}
        status = int(status_code or 0)
        if status == 429:
            previous_at = safe_float(previous.get("last_429_at"), 0.0)
            streak = (
                max(0, safe_int(previous.get("consecutive_429"), 0)) + 1
                if previous_at > 0 and now_wall - previous_at <= (
                    PROVIDER_RPC_RATE_LIMIT_STREAK_WINDOW_SECONDS)
                else 1
            )
            retry_after = 0.0
            for key, value in (headers or {}).items():
                if str(key).lower() != "retry-after":
                    continue
                try:
                    retry_after = max(0.0, float(value))
                except (TypeError, ValueError):
                    retry_after = 0.0
                break
            cooldown = min(
                PROVIDER_RPC_RATE_LIMIT_MAX_COOLDOWN_SECONDS,
                max(
                    retry_after,
                    PROVIDER_RPC_RATE_LIMIT_BASE_COOLDOWN_SECONDS
                    * (2 ** max(0, streak - 1)),
                ),
            )
            payload = {
                "schema_version": 1,
                "status_code": status,
                "consecutive_429": streak,
                "last_429_at": now_wall,
                "last_429_monotonic": now_mono,
                "cooldown_seconds": cooldown,
                "cooldown_until_monotonic": now_mono + cooldown,
                "retry_after_seconds": retry_after or None,
                "lane": self._active_lane,
                "updated_at": _utc_now(),
            }
            self._rpc_priority_telemetry["provider_rate_limits"] += 1
        elif 200 <= status < 300:
            payload = {
                "schema_version": 1,
                "status_code": status,
                "consecutive_429": 0,
                "last_429_at": previous.get("last_429_at"),
                "last_429_monotonic": previous.get("last_429_monotonic"),
                "cooldown_seconds": 0.0,
                "cooldown_until_monotonic": 0.0,
                "retry_after_seconds": None,
                "lane": self._active_lane,
                "updated_at": _utc_now(),
            }
        else:
            return
        atomic_json_write(path, payload)

    @contextmanager
    def _rpc_request_guard(self):
        """Serialize raw RPC requests and reserve the provider for live.

        The supervisor is the authority for live activity and next cadence.
        Background processes fail closed when that publication is stale.
        Rechecking after the mutex is acquired closes the race where another
        background process waited through a safe window and acquired the lock
        only after live became due.
        """
        lane = str(self._active_lane or "")
        deadline = self._rpc_gate_deadline or self._active_lane_deadline
        required = os.environ.get(
            "CHAINSEER_RPC_PRIORITY_REQUIRED", "") == "1"
        if not required or not lane:
            yield deadline.remaining() if deadline is not None else None
            return
        if deadline is None:
            raise BackgroundRpcPriorityDeferred(
                "RPC priority gate has no authoritative lane deadline")

        lock_path = self.root / "rpc_request_priority.lock"
        wait_started = time.monotonic()
        if lane == "live":
            try:
                with _HashLedgerAppendLock(
                    lock_path,
                    timeout_seconds=max(0.1, deadline.remaining()),
                ):
                    provider_state = read_json(
                        self.root / PROVIDER_RPC_THROTTLE_STATE_FILE, {}) or {}
                    cooldown = _provider_cooldown_remaining(provider_state)
                    while cooldown > 0 and deadline.remaining() > 0.1:
                        sleep_for = min(
                            cooldown + 0.002,
                            max(0.0, deadline.remaining() - 0.1))
                        self._rpc_priority_telemetry[
                            "provider_cooldown_wait_seconds"] += sleep_for
                        self._rpc_priority_telemetry["last_reason"] = (
                            "provider_rate_limit_cooldown")
                        time.sleep(sleep_for)
                        cooldown = _provider_cooldown_remaining(provider_state)
                    if cooldown > 0:
                        raise RPCError(
                            "provider rate-limit cooldown exceeded the "
                            "live lane deadline", -429)
                    waited = time.monotonic() - wait_started
                    self._rpc_priority_telemetry["admissions"] += 1
                    self._rpc_priority_telemetry["wait_seconds"] += waited
                    self._rpc_priority_telemetry["last_reason"] = (
                        "live_priority")
                    yield max(0.1, deadline.remaining())
                return
            except TimeoutError as exc:
                raise RPCError(
                    "live RPC priority lock exceeded the lane deadline", -2
                ) from exc

        if lane not in BACKGROUND_RPC_SERIALIZED_LANES:
            yield max(0.1, deadline.remaining())
            return

        last_reason = "priority_state_unavailable"
        while deadline.remaining() > 0.1:
            rate_sleep_seconds = 0.0
            state = read_json(
                self.root / BACKGROUND_RPC_PRIORITY_STATE_FILE, {}) or {}
            window = _background_rpc_priority_window(state)
            last_reason = str(window["reason"])
            if window["admitted"]:
                try:
                    request_lock = _HashLedgerAppendLock(
                        lock_path,
                        timeout_seconds=min(
                            0.25, max(0.1, deadline.remaining())),
                    )
                    request_lock.__enter__()
                except TimeoutError:
                    time.sleep(min(
                        BACKGROUND_RPC_PRIORITY_POLL_SECONDS,
                        max(0.0, deadline.remaining())))
                    continue
                try:
                    # The lock wait may have crossed into the reservation.
                    state = read_json(
                        self.root / BACKGROUND_RPC_PRIORITY_STATE_FILE,
                        {}) or {}
                    window = _background_rpc_priority_window(state)
                    last_reason = str(window["reason"])
                    maximum = min(
                        deadline.remaining(),
                        safe_float(window.get("available_seconds"), 0.0),
                    )
                    rate_state = read_json(
                        self.root / BACKGROUND_RPC_RATE_STATE_FILE, {}) or {}
                    last_completed = safe_float(
                        rate_state.get(
                            "last_background_completed_monotonic"), 0.0)
                    since_completed = (
                        time.monotonic() - last_completed
                        if last_completed > 0 else None)
                    if since_completed is not None and since_completed < 0:
                        # monotonic clocks reset across a machine reboot;
                        # a persisted pre-reboot stamp must not defer forever.
                        since_completed = None
                    rate_sleep_seconds = max(
                        0.0,
                        BACKGROUND_RPC_MINIMUM_INTERVAL_SECONDS - (
                            since_completed
                            if since_completed is not None
                            else BACKGROUND_RPC_MINIMUM_INTERVAL_SECONDS),
                    )
                    provider_state = read_json(
                        self.root / PROVIDER_RPC_THROTTLE_STATE_FILE, {}) or {}
                    provider_sleep_seconds = _provider_cooldown_remaining(
                        provider_state)
                    if provider_sleep_seconds > rate_sleep_seconds:
                        rate_sleep_seconds = provider_sleep_seconds
                        last_reason = "provider_rate_limit_cooldown"
                    if rate_sleep_seconds > 0:
                        if last_reason != "provider_rate_limit_cooldown":
                            last_reason = "background_rate_paced"
                    elif window["admitted"] and maximum >= (
                            BACKGROUND_RPC_MINIMUM_WINDOW_SECONDS):
                        waited = time.monotonic() - wait_started
                        self._rpc_priority_telemetry["admissions"] += 1
                        self._rpc_priority_telemetry[
                            "wait_seconds"] += waited
                        self._rpc_priority_telemetry["last_reason"] = (
                            "safe_background_window")
                        request_failed = True
                        try:
                            yield maximum
                            request_failed = False
                        finally:
                            atomic_json_write(
                                self.root / BACKGROUND_RPC_RATE_STATE_FILE,
                                {
                                    "schema_version": 1,
                                    "last_background_completed_monotonic":
                                        time.monotonic(),
                                    "last_background_completed_at":
                                        time.time(),
                                    "lane": lane,
                                    "request_failed": request_failed,
                                    "minimum_interval_seconds":
                                        BACKGROUND_RPC_MINIMUM_INTERVAL_SECONDS,
                                },
                            )
                        return
                finally:
                    request_lock.__exit__(None, None, None)
            if rate_sleep_seconds > 0:
                sleep_for = min(
                    rate_sleep_seconds,
                    BACKGROUND_RPC_PRIORITY_POLL_SECONDS,
                    max(0.0, deadline.remaining()),
                )
                self._rpc_priority_telemetry[
                    "rate_wait_seconds"] += sleep_for
                if last_reason == "provider_rate_limit_cooldown":
                    self._rpc_priority_telemetry[
                        "provider_cooldown_wait_seconds"] += sleep_for
                time.sleep(sleep_for)
                continue
            time.sleep(min(
                BACKGROUND_RPC_PRIORITY_POLL_SECONDS,
                max(0.0, deadline.remaining())))

        waited = time.monotonic() - wait_started
        self._rpc_priority_telemetry["deferrals"] += 1
        self._rpc_priority_telemetry["wait_seconds"] += waited
        self._rpc_priority_telemetry["last_reason"] = last_reason
        raise BackgroundRpcPriorityDeferred(
            f"background RPC deferred for live priority: {last_reason}")

    @contextmanager
    def _rpc_deadline(
        self, deadline: CycleDeadline, *,
        retry_attempts: int = REMOTE_RETRY_ATTEMPTS,
    ):
        """Push the monotonic budget into this lane's blocking RPC socket."""
        original_deadline = getattr(self, "_rpc_gate_deadline", None)
        self._rpc_gate_deadline = deadline
        if not hasattr(self.rpc, "timeout"):
            try:
                yield
            finally:
                self._rpc_gate_deadline = original_deadline
            return
        original = self.rpc.timeout
        remaining = max(0.5, deadline.remaining())
        # _remote_call may retry a failed request. Giving every attempt the
        # whole remaining budget made one stage consume 3x its deadline.
        per_attempt = remaining / max(1, int(retry_attempts) + 1)
        self.rpc.timeout = max(
            0.5, min(float(original), max(0.5, per_attempt)))
        try:
            yield
        finally:
            self.rpc.timeout = original
            self._rpc_gate_deadline = original_deadline

    @contextmanager
    def _market_deadline(self, deadline: CycleDeadline):
        """Push the lane deadline into blocking market-provider requests."""
        if not hasattr(self.market, "timeout"):
            yield
            return
        original = self.market.timeout
        remaining = max(0.5, deadline.remaining())
        per_attempt = remaining / max(1, REMOTE_RETRY_ATTEMPTS + 1)
        self.market.timeout = max(
            0.5, min(float(original), max(0.5, per_attempt)))
        try:
            yield
        finally:
            self.market.timeout = original

    def _analyzer(self):
        if self.analyzer is None:
            self.analyzer=Chainseer(
                rpc_url=ROBINHOOD_NETWORK.rpc_url,
                chain_root=str(Path(DEFAULT_CHAIN_ROOT)),
                network=ROBINHOOD_NETWORK,
            )
            self._bind_rpc_request_gate(
                getattr(self.analyzer, "rpc", None))
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
        discarded_after_deadline = 0
        deadline_stops = 0
        batch_seconds = 0.0
        remote_seconds = 0.0
        commit_seconds = 0.0
        offset = 0
        first_batch = True
        previous_batch_size = 0
        while offset < len(hashes):
            if deadline_monotonic is not None:
                remaining = deadline_monotonic - time.monotonic()
                next_size = min(
                    FLOW_ORIGIN_BATCH_SIZE, len(hashes) - offset)
                estimated_next = (
                    batch_seconds * next_size / previous_batch_size
                    if batch_seconds and previous_batch_size else batch_seconds)
                if remaining <= 0 or (
                        estimated_next and remaining < estimated_next):
                    deadline_stops += 1
                    break
            batch_size = (
                FLOW_ORIGIN_COLD_BATCH_SIZE
                if first_batch and deadline_monotonic is not None
                else FLOW_ORIGIN_BATCH_SIZE)
            chunk = hashes[offset:offset + batch_size]
            attempted += len(chunk)
            batch_started = time.monotonic()
            try:
                if deadline_monotonic is None:
                    records = _remote_call(
                        f"Robinhood transaction origins {offset + 1}-{offset + len(chunk)}",
                        lambda chunk=chunk: self.rpc.get_transactions(chunk),
                        attempts=1,
                    )
                else:
                    child = CycleDeadline(
                        max(0.0, deadline_monotonic - time.monotonic()),
                        deadline_monotonic=deadline_monotonic)
                    with self._rpc_deadline(child):
                        records = _remote_call(
                            f"Robinhood transaction origins {offset + 1}-{offset + len(chunk)}",
                            lambda chunk=chunk: self.rpc.get_transactions(chunk),
                            attempts=1,
                        )
            except Exception:
                # A failed remote batch consumed real freshness budget too.
                # Previously only successful calls updated ``batch_seconds``;
                # after an expensive failure the next batch therefore saw a
                # zero estimate and was admitted with fictitious headroom.
                # Cohort 3 measured exactly that signature: 25 failed origins,
                # a second batch, 128-block decision lag, and a separate
                # near_head_commit deferral after ingestion exceeded its p95.
                observed = time.monotonic() - batch_started
                batch_seconds = max(observed, batch_seconds * 0.5)
                remote_seconds = max(observed, remote_seconds * 0.5)
                failures += len(chunk)
                first_batch = False
                previous_batch_size = len(chunk)
                offset += len(chunk)
                continue
            remote_observed = time.monotonic() - batch_started
            # Never let a response that arrived after the observation budget
            # mutate identity evidence.  The unresolved queue is the durable
            # retry mechanism, so discarding is safe and scientifically
            # preferable to publishing a decision with post-deadline facts.
            if (deadline_monotonic is not None
                    and time.monotonic() >= deadline_monotonic):
                discarded_after_deadline += len(chunk)
                deadline_stops += 1
                batch_seconds = max(remote_observed, batch_seconds * 0.5)
                remote_seconds = max(remote_observed, remote_seconds * 0.5)
                break
            commit_started = time.monotonic()
            result = self.store.record_transaction_origins(records)
            commit_observed = time.monotonic() - commit_started
            # The scheduled unit is RPC PLUS the database write and flow
            # recomputation. Cohort 5 showed the old clock stopped before
            # record_transaction_origins: admission priced only the remote
            # call, started another batch, then 34/100 cycles expired at
            # near_head_commit while unmeasured commit work consumed the
            # child deadline. Track the complete atomic unit, while retaining
            # its substages so future diagnosis cannot repeat that mistake.
            observed = time.monotonic() - batch_started
            batch_seconds = max(observed, batch_seconds * 0.5)
            remote_seconds = max(remote_observed, remote_seconds * 0.5)
            commit_seconds = max(commit_observed, commit_seconds * 0.5)
            resolved += result["resolved"]
            unavailable += result["unavailable"]
            affected_pools += result["affected_pools"]
            first_batch = False
            previous_batch_size = len(chunk)
            offset += len(chunk)
        return {
            "attempted": attempted, "resolved": resolved,
            "unavailable": unavailable, "failures": failures,
            "affected_pools": affected_pools,
            "stopped_at_deadline": deadline_stops,
            "discarded_after_deadline": discarded_after_deadline,
            "observed_batch_seconds": round(batch_seconds, 3),
            "observed_remote_seconds": round(remote_seconds, 3),
            "observed_commit_seconds": round(commit_seconds, 3),
        }

    def _near_head_logs(
        self, start: int, end: int, topic: str,
        *, deadline: CycleDeadline | None = None,
    ) -> list[dict]:
        """Fetch a block range, halving on a provider result-set refusal.

        The limit is on ROWS RETURNED, not blocks queried, so no fixed chunk
        width is safe: a single 12,000-block request took coverage to zero for
        an hour, and after chunking to 4,000 the same
        "[RPC -32000] logs matched by query exceeds limit" still killed two
        passes on a busy stretch. Each failure abandoned the whole pass, and
        because the cursor does not advance on failure the head ran away --
        one outage cost a 54,503-block hole that the next pass had to skip.

        Splitting on refusal makes the chunk width self-correcting instead of
        a standing guess. This mirrors _adaptive_logs on the V4 observer,
        which has had it all along; the near-head path simply never used it.
        A range that cannot be split further re-raises, so a genuine outage
        still surfaces rather than looping.
        """
        pending = [(start, end)]
        logs: list[dict] = []
        while pending:
            if deadline is not None:
                deadline.raise_if_expired("near_head_rpc")
            lower, upper = pending.pop()
            try:
                logs.extend(self.rpc.get_logs(
                    lower, upper, address=UNISWAP_V4_POOL_MANAGER,
                    topics=[[topic]],
                ) or [])
            except CycleDeadlineExceeded:
                raise
            except Exception as error:
                oversized = "exceeds limit" in str(error).lower()
                if not oversized or lower >= upper:
                    raise
                midpoint = (lower + upper) // 2
                # LIFO keeps the requests chronological.
                pending.append((midpoint + 1, upper))
                pending.append((lower, midpoint))
        return logs

    def enrich_near_head_window(
        self, events: list[dict], *,
        limit: int = FLOW_NEAR_HEAD_ENRICHMENT_LIMIT,
        budget_seconds: float = FLOW_NEAR_HEAD_ENRICHMENT_BUDGET_SECONDS,
        deadline: CycleDeadline | None = None,
    ) -> dict:
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
        limit = max(0, int(limit))
        truncated = max(0, len(candidates) - limit)
        selected = candidates[:limit]
        local_deadline = time.monotonic() + max(0.0, float(budget_seconds))
        if deadline is not None:
            local_deadline = min(local_deadline, time.monotonic() + deadline.remaining())
        batch = self._resolve_origin_batches(selected, local_deadline)
        return {
            "supported": True,
            "candidates": len(candidates),
            "truncated": truncated,
            "attempted": batch["attempted"],
            "deferred_for_deadline": max(
                0, len(selected) - batch["attempted"]),
            "resolved": batch["resolved"],
            "unavailable": batch["unavailable"],
            "failures": batch["failures"],
            "affected_pools": batch["affected_pools"],
            "stopped_at_deadline": batch["stopped_at_deadline"],
            "discarded_after_deadline": batch.get(
                "discarded_after_deadline", 0),
            "observed_batch_seconds": batch["observed_batch_seconds"],
            "observed_remote_seconds": batch["observed_remote_seconds"],
            "observed_commit_seconds": batch["observed_commit_seconds"],
            "window_fully_enriched": bool(
                not truncated and batch["attempted"] == len(selected)
                and not batch["failures"]
                and not batch.get("discarded_after_deadline", 0)
            ),
        }

    @staticmethod
    def near_head_enrichment_admission(
        *, observation_head: int, current_head: int | None,
        elapsed_seconds: float, configured_seconds: float,
    ) -> dict:
        """Plan optional enrichment in block-time, failing closed on doubt.

        A seconds-only budget cannot protect a block-based freshness covenant:
        Cohort 4 completed quickly but missed freshness when block production
        accelerated. This plan reserves the measured downstream block tail,
        estimates the rate already observed in the pass, and admits enrichment
        only when at least one empirically bounded first batch fits.
        """
        configured = max(0.0, float(configured_seconds))
        elapsed = max(0.001, float(elapsed_seconds))
        if current_head is None:
            return {
                "admitted_seconds": 0.0,
                "configured_seconds": round(configured, 3),
                "reason": "current_head_unavailable",
                "observation_head": int(observation_head),
                "current_head": None,
                "head_lag_blocks": None,
                "observed_blocks_per_second": None,
                "planning_blocks_per_second": (
                    FLOW_MINIMUM_PLANNING_BLOCKS_PER_SECOND),
                "preseal_lag_limit_blocks": (
                    FLOW_PRESEAL_MAXIMUM_HEAD_LAG_BLOCKS),
                "downstream_reserve_blocks": (
                    FLOW_DOWNSTREAM_HEAD_RESERVE_BLOCKS),
            }
        lag = max(0, int(current_head) - int(observation_head))
        observed_rate = lag / elapsed
        planning_rate = max(
            FLOW_MINIMUM_PLANNING_BLOCKS_PER_SECOND, observed_rate)
        remaining_blocks = max(
            0, FLOW_PRESEAL_MAXIMUM_HEAD_LAG_BLOCKS - lag)
        raw_seconds = remaining_blocks / planning_rate
        bounded_seconds = min(configured, raw_seconds)
        if lag >= FLOW_PRESEAL_MAXIMUM_HEAD_LAG_BLOCKS:
            admitted = 0.0
            reason = "preseal_headroom_exhausted"
        elif bounded_seconds < FLOW_MINIMUM_FIRST_ORIGIN_BATCH_SECONDS:
            admitted = 0.0
            reason = "insufficient_first_batch_headroom"
        else:
            admitted = bounded_seconds
            reason = (
                "configured_budget_admitted"
                if admitted >= configured
                else "block_budget_capped"
            )
        return {
            "admitted_seconds": round(admitted, 3),
            "raw_block_budget_seconds": round(raw_seconds, 3),
            "configured_seconds": round(configured, 3),
            "reason": reason,
            "observation_head": int(observation_head),
            "current_head": int(current_head),
            "head_lag_blocks": lag,
            "remaining_preseal_blocks": remaining_blocks,
            "observed_blocks_per_second": round(observed_rate, 3),
            "planning_blocks_per_second": round(planning_rate, 3),
            "preseal_lag_limit_blocks": FLOW_PRESEAL_MAXIMUM_HEAD_LAG_BLOCKS,
            "downstream_reserve_blocks": FLOW_DOWNSTREAM_HEAD_RESERVE_BLOCKS,
            "minimum_first_batch_seconds": (
                FLOW_MINIMUM_FIRST_ORIGIN_BATCH_SECONDS),
        }

    def near_head_flow_pass(
        self, *, deadline: CycleDeadline | None = None,
        cursor_name: str = "near_head_cursor.json",
        max_scan_blocks: int = FLOW_NEAR_HEAD_SCAN_BLOCKS,
        enrichment_limit: int = FLOW_NEAR_HEAD_ENRICHMENT_LIMIT,
        enrichment_budget_seconds: float = FLOW_NEAR_HEAD_ENRICHMENT_BUDGET_SECONDS,
    ) -> dict:
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
        ingest_phase: dict[str, float] = {}
        ingest_mark = [time.monotonic(), "start"]

        def _close_ingest_phase() -> dict:
            """Charge the final open sub-stage, then hand back the totals."""
            now = time.monotonic()
            ingest_phase[ingest_mark[1]] = round(
                ingest_phase.get(ingest_mark[1], 0.0) + now - ingest_mark[0], 3)
            ingest_mark[0] = now
            return dict(ingest_phase)

        def _ingest_substage(name: str) -> None:
            """WHERE inside ingestion are we?

            Four of five cohort failures terminated in `ingestion` with no
            finer detail, and their causes were three different things --
            supervisor_hard_deadline_exceeded, live_completion, and a
            `database is locked`. A stage name covering head read, log fetch,
            decode, enrichment and cursor commit cannot separate them, so any
            fix sized against it would be a guess.

            Telemetry only: a store without the method, or a failure writing
            one, must never break ingestion.
            """
            # Close the previous sub-stage before opening this one. Marking
            # position alone could not separate a slow RPC from a contended
            # write: deferred cycles ingested 9.92s against 5.95s on identical
            # work (53 vs 55 swaps, same 66-block scan), and nothing recorded
            # WHICH part took the extra four seconds.
            now = time.monotonic()
            ingest_phase[ingest_mark[1]] = round(
                ingest_phase.get(ingest_mark[1], 0.0) + now - ingest_mark[0], 3)
            ingest_mark[0], ingest_mark[1] = now, name
            marker = getattr(self.store, "mark_lane_stage", None)
            if marker is None:
                return
            try:
                marker("live", f"ingestion/{name}",
                       run_id=getattr(self, "cycle_run_uuid", None),
                       remaining=(
                           None if deadline is None else deadline.remaining()),
                       completed={})
            except Exception:
                pass

        _ingest_substage("head_read")
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
        # The CURSOR drives the range; the width is only a cap.
        #
        # Previously the cursor was used only when the gap since the last scan
        # happened to fit inside a fixed lookback, and otherwise the pass fell
        # back to that lookback and left a hole. At a 1,350-block width the
        # fallback fired on 194 of 195 observed gaps -- the incremental path
        # was effectively dead code and coverage was 15.1% of the chain. At
        # 12,000 the fallback would still fire on 10.3% of gaps overall and on
        # 5 of the last 17, because cycles have been lengthening: median gap
        # 6,960, p90 12,691, max 22,515.
        #
        # A fallback that fires is a hole in the record, and no width removes
        # it -- it only makes it rarer. So the cursor now drives the range
        # unconditionally: scan from the block after the last one scanned,
        # whatever that span turns out to be. Coverage becomes complete by
        # construction rather than complete-until-a-cycle-runs-long.
        #
        # FLOW_NEAR_HEAD_SCAN_BLOCKS survives as a CAP, not a window: it bounds
        # a single fetch when the cursor is absent (first run) or absurdly
        # stale (a long outage), so one pass cannot try to read a million
        # blocks. Being capped is recorded, because a capped pass DOES leave a
        # hole and the next reader must be able to see that it did.
        _ingest_substage("cursor_read")
        cursor_path = self.root / cursor_name
        cursor = read_json(cursor_path, {}) or {}
        last_scanned = safe_int(cursor.get("last_scanned_block"), 0)
        max_scan_blocks = max(1, int(max_scan_blocks))
        window_floor = max(0, head - max_scan_blocks + 1)
        _ingest_substage("log_fetch")
        incremental = bool(last_scanned)
        from_block = last_scanned + 1 if last_scanned else window_floor
        scan_capped = bool(from_block < window_floor)
        if scan_capped:
            # RE-ANCHOR. The live lane must observe the PRESENT, so it jumps to
            # the newest cap-width span rather than grinding through history
            # first. The skipped range is enqueued, not dropped: recording a
            # count told the operator blocks were lost but gave nothing the
            # power to recover them.
            self.store.enqueue_backfill(
                last_scanned + 1, window_floor - 1, "live_lane_reanchor")
            from_block = window_floor
        if from_block > head:
            # No new blocks. The windows already in the database stand; there
            # is nothing to ingest and nothing to enrich.
            logs = []
        else:
            try:
                logs = []
                span = from_block
                while span <= head:
                    if deadline is not None:
                        deadline.raise_if_expired("near_head_swaps")
                    upper = min(head, span + FLOW_NEAR_HEAD_FETCH_CHUNK_BLOCKS - 1)
                    logs.extend(
                        self._near_head_logs(
                            span, upper, V4_SWAP_TOPIC, deadline=deadline))
                    span = upper + 1
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
            span = from_block
            while span <= head:
                if deadline is not None:
                    deadline.raise_if_expired("near_head_initializes")
                upper = min(head, span + FLOW_NEAR_HEAD_FETCH_CHUNK_BLOCKS - 1)
                init_logs.extend(
                    self._near_head_logs(
                        span, upper, V4_INITIALIZE_TOPIC, deadline=deadline))
                span = upper + 1
        except CycleDeadlineExceeded:
            raise
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
            _ingest_substage("apply_pool_events")
            self.store.apply_v4_events(born, deadline=deadline)
            known = self.store.known_v4_pool_ids()
        _ingest_substage("decode_swaps")
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
            _ingest_substage("apply_swap_events")
            self.store.apply_v4_events(events, deadline=deadline)
        # The scan and its raw events are durable at this point. Advance the
        # cursor BEFORE optional origin enrichment so an expensive identity
        # lookup cannot make the next cycle ingest the same blocks again.
        # Identity evidence still fails closed: unresolved origins retain
        # incomplete coverage and cannot become paper eligible. The durable
        # origin queue lets the analysis/backfill lanes revisit them without
        # putting historical recovery on the live decision path.
        atomic_json_write(cursor_path, {"last_scanned_block": int(head)})
        cursor_committed_before_enrichment = True
        # Enrich BETWEEN computing the window and sealing it. apply_v4_events
        # has just recomputed the flow signal with zero resolved participants;
        # record_transaction_origins recomputes it again with the real ones, so
        # the seal that follows carries the identity evidence that actually
        # existed at observation time.
        try:
            head_before_enrichment = int(self.rpc.get_block_number())
        except Exception:
            head_before_enrichment = None
        freshness_admission = self.near_head_enrichment_admission(
            observation_head=head,
            current_head=head_before_enrichment,
            elapsed_seconds=time.monotonic() - started,
            configured_seconds=enrichment_budget_seconds,
        )
        enrichment = self.enrich_near_head_window(
            events, limit=enrichment_limit,
            budget_seconds=freshness_admission["admitted_seconds"],
            deadline=deadline,
        )
        enrichment["freshness_admission"] = freshness_admission
        if deadline is not None:
            deadline.raise_if_expired("near_head_commit")
        try:
            head_after = int(self.rpc.get_block_number())
        except Exception:
            head_after = head
        touched = sorted({event["pool_id"] for event in events})
        return {
            "supported": True, "scanned": True,
            "from_block": from_block, "to_block": head,
            "incremental": incremental,
            "scan_capped": scan_capped,
            "backfill_backlog": self.store.backfill_backlog(),
            "blocks_skipped_by_cap": (
                max(0, window_floor - (last_scanned + 1)) if scan_capped else 0
            ),
            "scan_blocks": max(0, head - from_block + 1),
            # Seconds per ingestion sub-stage. Published, not merely recorded:
            # a timer nothing reads is the defect this project keeps finding,
            # and the question it exists to answer -- why deferred cycles
            # ingest 9.92s against 5.95s on identical work -- can only be
            # settled from the persisted summary.
            "ingest_phase_seconds": _close_ingest_phase(),
            "logs_seen": len(logs), "swaps_ingested": len(events),
            "initialize_logs_seen": len(init_logs),
            "pools_admitted_on_sight": len(born),
            "cursor_committed_before_enrichment": (
                cursor_committed_before_enrichment),
            "pools_touched": len(touched),
            "touched_pool_ids": touched,
            # Attributed to THIS pass's pools, never read off the whole table.
            "window_coverage": self.store.flow_window_coverage(touched),
            "enrichment": enrichment,
            "head_block_before_enrichment": head_before_enrichment,
            "head_block_after": head_after,
            "elapsed_blocks": max(0, head_after - head),
            "duration_seconds": round(time.monotonic() - started, 3),
            "within_prospective_bound": bool(
                head_after - head <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            ),
            "scope": "near_head_flow_window_v1",
        }

    def ingestion_cost_model(self) -> dict:
        """Durable nearest-rank p95 for completed/censored ingestion.

        The value is used to scale the newest-block scan when startup has
        already consumed unusual headroom.  Raw bounded samples make the
        decision reproducible after restart; censored samples are lower
        bounds and therefore may only raise, never lower, the estimate.
        """
        stored = self.store.scheduler_state(INGESTION_COST_MODEL_STATE_KEY)
        raw = stored.get("samples")
        samples = [
            float(value) for value in (raw if isinstance(raw, list) else [])
            if isinstance(value, (int, float))
            and math.isfinite(float(value)) and float(value) >= 0
        ][-INGESTION_COST_SAMPLE_WINDOW:]
        if samples:
            ordered = sorted(samples)
            p95 = ordered[min(
                len(ordered) - 1,
                max(0, math.ceil(0.95 * len(ordered)) - 1),
            )]
        else:
            # The live corpus measured 14.719s across the latest 128
            # completed attempts.  This is planning telemetry, not a risk
            # threshold; the first completed pass begins replacing it.
            p95 = safe_float(stored.get("p95_seconds"), 14.719)
        return {
            "p95_seconds": round(float(p95), 4),
            "sample_count": len(samples),
            "censored_samples": safe_int(stored.get("censored_samples"), 0),
            "samples": samples,
        }

    def record_ingestion_cost(
        self, seconds: float, *, censored: bool = False,
    ) -> dict:
        """Append one ingestion sample without rewarding a timeout."""
        current = self.ingestion_cost_model()
        measured = max(0.0, float(seconds))
        if censored:
            measured = max(measured, current["p95_seconds"])
        samples = [*current["samples"], measured][
            -INGESTION_COST_SAMPLE_WINDOW:]
        ordered = sorted(samples)
        p95 = ordered[min(
            len(ordered) - 1,
            max(0, math.ceil(0.95 * len(ordered)) - 1),
        )]
        payload = {
            "samples": samples,
            "p95_seconds": round(float(p95), 4),
            "last_measured_seconds": round(measured, 4),
            "last_sample_censored": bool(censored),
            "censored_samples": (
                current["censored_samples"] + int(bool(censored))),
        }
        self.store.set_scheduler_state(INGESTION_COST_MODEL_STATE_KEY, payload)
        return self.ingestion_cost_model()

    def ingestion_tail_reserve(self) -> float:
        """Headroom ingestion must leave for observation and completion."""
        model = self.live_planning_seal_cost_model()
        return round(
            max(LIVE_LANE_DECISION_RESERVE_SECONDS,
                model["downstream_reserve_p95"])
            + model["fixed_observation_cost_p95"]
            + model["queue_settlement_p95"]
            + LIVE_LANE_INGESTION_MARGIN_SECONDS,
            4,
        )

    def seal_cost_model(self) -> dict:
        """Durable two-part observation-cost model.

        fixed_observation_cost_p95 is paid once per cycle whatever the
        admission count (selection + prefetch); queue settlement is a
        separate reserved component;
        per_window_cost_p95 scales with each window sealed (hashes + quote
        RPC + evidence hash + database commit); downstream_reserve_p95 is
        the decision-critical tail (decision-head retrieval +
        classification + ledger completion) that must NEVER be consumed by
        sealing, because none of it can yield once started.
        """
        stored = self.store.scheduler_state(SEAL_COST_MODEL_STATE_KEY)
        return _seal_cost_model_from_state(stored)

    def live_planning_seal_cost_model(self) -> dict:
        """Admission model isolated from censored reliability lower bounds."""
        return _seal_live_planning_model(self.seal_cost_model())

    def seal_cost_estimate(self) -> float:
        """Per-window cost from the two-part model (compatibility shim)."""
        return self.seal_cost_model()["per_window_cost_p95"]

    def _blend_seal_model(self, updates: dict, censored: bool = False,
                          sample_count: int = 0) -> dict:
        """Append raw timing samples with provenance; derive nearest-rank p95.

        Kept under the historical method name for compatibility with callers,
        but no EWMA blending occurs.  Each sample is stored as a provenance
        record -- epoch, run_id, timestamp, code revision, stage status
        (success/censored/stalled) -- so a percentile can be recomputed from
        exactly the population that should drive it: current-epoch successful
        samples only.  A censored value is a lower bound and is clamped to
        the current p95 so recording a timeout can never lower admission's
        estimate.  Stalled samples are EXCLUDED from the estimate (they are
        reliability events, not cost data) but counted for reporting.
        """
        stored = self.store.scheduler_state(SEAL_COST_MODEL_STATE_KEY)
        if (stored and safe_int(stored.get("epoch"), 0)
                != SEAL_COST_MODEL_EPOCH):
            # Preserve the complete superseded population for post-mortem
            # research instead of silently rewriting estimator history.
            archive_key = (
                f"{SEAL_COST_MODEL_STATE_KEY}:archive:"
                f"epoch-{safe_int(stored.get('epoch'), 0)}:"
                f"{int(time.time())}")
            self.store.set_scheduler_state(archive_key, stored)
        run_id = str(getattr(self, "cycle_run_uuid", "") or "")
        payload = _append_seal_cost_records(
            stored, updates,
            status="censored" if censored else "success",
            run_id=run_id, revision=CODE_REVISION,
            recorded_at=time.time(), sample_count=sample_count,
        )
        self.store.set_scheduler_state(SEAL_COST_MODEL_STATE_KEY, payload)
        for scalar in updates:
            field = SEAL_COST_SAMPLE_FIELDS[scalar]
            for record in _valid_seal_sample_records(payload.get(field)):
                if (str(record.get("run_id") or "") != run_id
                        or abs(safe_float(record.get("at"), 0.0)
                               - safe_float(payload.get("last_sample_at"), 0.0))
                        > 0.001):
                    continue
                self.store.record_seal_cost_samples(
                    {scalar: record["value"]}, status=record["status"],
                    run_id=run_id, epoch=SEAL_COST_MODEL_EPOCH,
                    revision=CODE_REVISION, sample_count=sample_count,
                )
        return self.seal_cost_model()

    def record_seal_cost(
        self, phase: dict[str, list[float]], sealed: int,
    ) -> float | None:
        """Fold this pass into the two-part model. Returns per-window cost.

        Charged against the per-window phases only. The prefetch batch and
        the selection query are FIXED cost -- paid once per cycle whatever
        the admission count -- so folding them into the per-window figure
        would make every window look more expensive as the cycle admitted
        fewer of them: an estimate that gets worse exactly when headroom is
        tightest.

        Queue settlement is tracked as its OWN component
        (queue_settlement_p95) and deliberately EXCLUDED from fixed cost:
        admission reserves it separately, and counting it in both places
        would double-charge the budget by its full amount every cycle.
        """
        fixed_names = {"selection", "prefetch"}
        fixed_seconds = sum(
            sum(values) for name, values in phase.items()
            if name in fixed_names)
        raw_window_samples = [
            float(value) for value in phase.get("window_total", [])
            if isinstance(value, (int, float)) and float(value) >= 0
        ]
        if not raw_window_samples and sealed > 0:
            raw_window_samples = [sum(
                sum(values) for name, values in phase.items()
                if name not in fixed_names
            ) / sealed]
        updates = {}
        if fixed_seconds > 0:
            updates["fixed_observation_cost_p95"] = fixed_seconds
        queue_samples = [
            float(value) for value in phase.get("queue_settle", [])
            if isinstance(value, (int, float)) and float(value) >= 0
        ]
        if queue_samples:
            updates["queue_settlement_p95"] = queue_samples
        if raw_window_samples:
            updates["per_window_cost_p95"] = raw_window_samples
        if not updates:
            return None
        self._blend_seal_model(updates, sample_count=int(sealed))
        # Report the actual nearest-rank p95 of this pass, not the historical
        # model and not a per-window mean.
        ordered = sorted(raw_window_samples)
        if not ordered:
            return 0.0
        return round(ordered[min(
            len(ordered) - 1,
            max(0, math.ceil(0.95 * len(ordered)) - 1),
        )], 4)

    def record_seal_cost_censored(self, elapsed_seconds: float) -> dict:
        """Charge a timed-out attempt as a conservative censored sample.

        A cycle that timed out spent at least `elapsed_seconds` in the stage.
        Treating that lower bound as a per-window sample is intentionally
        conservative and may overestimate; it can never make admission more
        aggressive. Supervisor hard kills record a more precisely attributed
        censored sample in ``terminate_lane`` because child cleanup cannot run.
        """
        elapsed = max(0.05, float(elapsed_seconds))
        return self._blend_seal_model(
            {"per_window_cost_p95": elapsed}, censored=True)

    def downstream_reserve_estimate(self) -> float:
        return self.seal_cost_model()["downstream_reserve_p95"]

    def record_downstream_reserve(self, seconds: float) -> float:
        """Fold a completed cycle's decision-tail duration into the p95."""
        model = self._blend_seal_model({
            "downstream_reserve_p95": max(0.0, float(seconds))})
        return model["downstream_reserve_p95"]

    def decision_tail_block_model(self) -> dict:
        """Return a tighten-only measured block reserve by observation count."""
        stored = self.store.scheduler_state(
            DECISION_TAIL_BLOCK_MODEL_STATE_KEY)
        epoch_matches = safe_int(stored.get("epoch"), 0) == (
            DECISION_TAIL_BLOCK_MODEL_EPOCH)
        raw = stored.get("samples") if epoch_matches else []
        records: list[dict] = []
        if isinstance(raw, list):
            for item in raw:
                if not isinstance(item, dict):
                    continue
                count = safe_int(item.get("observations"), -1)
                blocks = safe_int(item.get("blocks"), -1)
                if count < 0 or blocks < 0:
                    continue
                records.append({
                    **item, "observations": count, "blocks": blocks})
        records = records[-DECISION_TAIL_BLOCK_SAMPLE_WINDOW:]
        reserves: dict[int, int] = {}
        sample_counts: dict[int, int] = {}
        historical_breached_counts: list[int] = []
        for count in range(0, LIVE_LANE_OBSERVATION_LIMIT + 1):
            values = [
                record["blocks"] for record in records
                if record["observations"] == count
            ]
            successful_values = [
                value for value in values
                if value <= FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            ]
            baseline = DECISION_TAIL_BLOCK_DEFAULTS.get(
                count, FLOW_DOWNSTREAM_HEAD_RESERVE_BLOCKS)
            # Successful tails tighten the steady-state reserve. A breached
            # tail belongs to the attempt-clock circuit below; folding it
            # into a monotonic reserve would keep the batch impossible even
            # after the circuit's recovery proof had completed.
            reserves[count] = int(max(
                baseline,
                _nearest_rank(successful_values, DECISION_TAIL_BLOCK_QUANTILE,
                              baseline)))
            sample_counts[count] = len(values)
            # A quantile is the right steady-state estimator, but it cannot
            # erase an observed policy violation. Quarantine only the batch
            # size that breached the prospective bound until that sample
            # naturally ages out of the bounded model window. Smaller
            # batches continue producing evidence, so one chain/provider
            # spike cannot cause permanent observation starvation.
            if any(value > FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
                   for value in values):
                historical_breached_counts.append(count)
        circuit = self.decision_tail_circuit()
        return {
            "epoch": DECISION_TAIL_BLOCK_MODEL_EPOCH,
            "quantile": f"nearest_rank_p{int(DECISION_TAIL_BLOCK_QUANTILE*100)}",
            "reserves": reserves,
            "sample_counts": sample_counts,
            "historical_breached_counts": historical_breached_counts,
            "circuit": circuit,
            "samples": records,
        }

    def decision_tail_circuit(self) -> dict:
        """Attempt-clock circuit state; progress never depends on samples."""
        stored = self.store.scheduler_state(DECISION_TAIL_CIRCUIT_STATE_KEY)
        if safe_int(stored.get("epoch"), 0) != DECISION_TAIL_CIRCUIT_EPOCH:
            stored = {}
        attempt = max(0, safe_int(stored.get("attempt_sequence"), 0))
        breach = max(0, safe_int(stored.get("last_breach_attempt"), 0))
        successes = max(0, safe_int(
            stored.get("single_probe_successes"), 0))
        attempts_since = attempt - breach if breach else None
        multi_blocked = bool(
            breach and (
                attempts_since < DECISION_MULTI_OBSERVATION_COOLDOWN_ATTEMPTS
                or successes < DECISION_SINGLE_PROBE_SUCCESSES_REQUIRED
            )
        )
        return {
            "epoch": DECISION_TAIL_CIRCUIT_EPOCH,
            "attempt_sequence": attempt,
            "last_breach_attempt": breach or None,
            "attempts_since_breach": attempts_since,
            "single_probe_successes": successes,
            "multi_observation_blocked": multi_blocked,
            "blocked_counts": [2] if multi_blocked else [],
            "cooldown_attempts":
                DECISION_MULTI_OBSERVATION_COOLDOWN_ATTEMPTS,
            "probe_successes_required":
                DECISION_SINGLE_PROBE_SUCCESSES_REQUIRED,
            "seeded_from_history": bool(stored.get("seeded_from_history")),
        }

    def begin_decision_tail_attempt(self) -> dict:
        """Advance the durable circuit clock once per live attempt."""
        state = self.decision_tail_circuit()
        attempt = state["attempt_sequence"] + 1
        breach = state.get("last_breach_attempt")
        seeded = state.get("seeded_from_history", False)
        if breach is None and not seeded:
            model_state = self.store.scheduler_state(
                DECISION_TAIL_BLOCK_MODEL_STATE_KEY)
            historical_breach = any(
                safe_int(item.get("blocks"), 0)
                    > FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
                for item in (model_state.get("samples") or [])
                if isinstance(item, dict)
            )
            if historical_breach:
                breach = attempt
            seeded = True
        self._decision_tail_attempt_sequence = attempt
        self.store.set_scheduler_state(
            DECISION_TAIL_CIRCUIT_STATE_KEY, {
                "epoch": DECISION_TAIL_CIRCUIT_EPOCH,
                "revision": CODE_REVISION,
                "attempt_sequence": attempt,
                "last_breach_attempt": breach,
                "single_probe_successes": state[
                    "single_probe_successes"],
                "seeded_from_history": seeded,
            })
        return self.decision_tail_circuit()

    def _record_decision_tail_circuit(
        self, observations: int, blocks: int,
    ) -> dict:
        state = self.decision_tail_circuit()
        attempt = max(
            state["attempt_sequence"],
            safe_int(getattr(
                self, "_decision_tail_attempt_sequence", 0), 0),
        )
        # Keep the public recorder correct for maintenance calls and tests as
        # well as the normal live path, which explicitly begins an attempt.
        if attempt <= 0:
            state = self.begin_decision_tail_attempt()
            attempt = state["attempt_sequence"]
        breach = state.get("last_breach_attempt")
        successes = state["single_probe_successes"]
        if int(blocks) > FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS:
            breach = attempt
            successes = 0
        elif int(observations) == 1 and breach:
            successes += 1
        elif int(observations) > 1 and breach:
            breach = None
            successes = 0
        self.store.set_scheduler_state(
            DECISION_TAIL_CIRCUIT_STATE_KEY, {
                "epoch": DECISION_TAIL_CIRCUIT_EPOCH,
                "revision": CODE_REVISION,
                "attempt_sequence": attempt,
                "last_breach_attempt": breach,
                "single_probe_successes": successes,
                "seeded_from_history": True,
            })
        return self.decision_tail_circuit()

    def record_decision_tail_blocks(
        self, observations: int, blocks: int,
    ) -> dict:
        """Append one successful decision-tail measurement."""
        model = self.decision_tail_block_model()
        samples = list(model["samples"])
        samples.append({
            "observations": max(0, int(observations)),
            "blocks": max(0, int(blocks)),
            "run_id": str(getattr(self, "cycle_run_uuid", "") or ""),
            "revision": CODE_REVISION,
            "at": time.time(),
            "epoch": DECISION_TAIL_BLOCK_MODEL_EPOCH,
        })
        self.store.set_scheduler_state(
            DECISION_TAIL_BLOCK_MODEL_STATE_KEY, {
                "epoch": DECISION_TAIL_BLOCK_MODEL_EPOCH,
                "revision": CODE_REVISION,
                "samples": samples[-DECISION_TAIL_BLOCK_SAMPLE_WINDOW:],
            })
        self._record_decision_tail_circuit(observations, blocks)
        return self.decision_tail_block_model()

    def observation_freshness_admission(
        self, *, observation_head: int, post_ingest_head: int | None,
        requested: int,
    ) -> dict:
        """Admit the largest batch whose reserve and safety margin still fit."""
        requested = max(0, min(
            int(requested), LIVE_LANE_OBSERVATION_LIMIT))
        model = self.decision_tail_block_model()
        model_report = {
            "epoch": model["epoch"], "quantile": model["quantile"],
            "reserves": model["reserves"],
            "sample_counts": model["sample_counts"],
            "historical_breached_counts":
                model["historical_breached_counts"],
            "circuit": model["circuit"],
            "multi_observation_safety_blocks":
                DECISION_MULTI_OBSERVATION_SAFETY_BLOCKS,
        }
        if post_ingest_head is None:
            return {
                "requested": requested, "admitted": 0,
                "reason": "post_ingest_head_unavailable",
                "observation_head": int(observation_head),
                "post_ingest_head": None, "post_ingest_lag_blocks": None,
                "tail_reserve_blocks": None,
                "safety_blocks": None,
                "prospective_bound_blocks":
                    FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
                "model": model_report,
            }
        lag = max(0, int(post_ingest_head) - int(observation_head))
        admitted = 0
        reserve = int(model["reserves"].get(0, 0))
        safety_blocks = 0
        for count in range(requested, 0, -1):
            if count in model["circuit"]["blocked_counts"]:
                continue
            candidate_reserve = int(model["reserves"].get(
                count, FLOW_DOWNSTREAM_HEAD_RESERVE_BLOCKS))
            candidate_safety = (
                DECISION_MULTI_OBSERVATION_SAFETY_BLOCKS
                if count > 1 else 0
            )
            if lag + candidate_reserve + candidate_safety <= (
                FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            ):
                admitted = count
                reserve = candidate_reserve
                safety_blocks = candidate_safety
                break
        return {
            "requested": requested, "admitted": admitted,
            "reason": (
                "requested_batch_fits" if admitted == requested
                else "batch_reduced_for_freshness" if admitted > 0
                else "decision_tail_headroom_exhausted"),
            "observation_head": int(observation_head),
            "post_ingest_head": int(post_ingest_head),
            "post_ingest_lag_blocks": lag,
            "tail_reserve_blocks": reserve,
            "safety_blocks": safety_blocks,
            "prospective_bound_blocks":
                FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
            "model": model_report,
        }

    def decision_head_admission(
        self, observations: int, remaining_seconds: float,
    ) -> dict:
        """Reserve actual downstream work instead of a flat five seconds."""
        observations = max(0, int(observations))
        classification_seconds = (
            self.classification_cost_estimate() * observations)
        downstream = (
            CLASSIFICATION_COMPLETION_RESERVE_SECONDS
            + classification_seconds)
        required = downstream + DECISION_HEAD_RPC_RESERVE_SECONDS
        return {
            "observations": observations,
            "remaining_seconds": round(max(0.0, remaining_seconds), 3),
            "classification_estimate_seconds": round(
                classification_seconds, 3),
            "completion_reserve_seconds":
                CLASSIFICATION_COMPLETION_RESERVE_SECONDS,
            "head_rpc_reserve_seconds":
                DECISION_HEAD_RPC_RESERVE_SECONDS,
            "required_seconds": round(required, 3),
            "admitted": bool(remaining_seconds > required),
        }

    def read_authoritative_decision_head(
        self, deadline: CycleDeadline, *, downstream_required_seconds: float,
        allow_rate_limit_retry: bool = True,
    ) -> tuple[int, dict]:
        """Read the decision head with bounded, cooldown-aware 429 recovery."""
        attempts = 0
        rate_limit_retries = 0
        while True:
            attempts += 1
            try:
                with self._rpc_deadline(deadline, retry_attempts=0):
                    head = int(self.rpc.get_block_number())
                return head, {
                    "attempts": attempts,
                    "rate_limit_retries": rate_limit_retries,
                }
            except RPCError as exc:
                can_retry = bool(
                    allow_rate_limit_retry and _rpc_rate_limited(exc)
                    and attempts < LIVE_DECISION_HEAD_MAXIMUM_ATTEMPTS
                    and deadline.remaining() > (
                        max(0.0, float(downstream_required_seconds)) + 0.1)
                )
                if not can_retry:
                    setattr(exc, "decision_head_attempts", attempts)
                    setattr(
                        exc, "decision_head_rate_limit_retries",
                        rate_limit_retries)
                    raise
                # RobinhoodRPC's response observer published the cooldown
                # before raising. The next raw request waits under the shared
                # request mutex; test doubles simply exercise the retry bound.
                rate_limit_retries += 1

    def classification_cost_estimate(self) -> float:
        """Measured seconds per classified observation (EWMA p95), or a
        conservative cold-start default.

        Classification is admission-controlled exactly like sealing: an
        observation admitted into a cycle that cannot finish classifying
        it would die mid-stage with no decision attached -- the one thing
        sealing's yield cannot fix.
        """
        stored = self.store.scheduler_state("classification_observation_cost")
        value = safe_float(stored.get("per_observation_seconds"), 0.0)
        if value <= 0:
            return CLASSIFICATION_COST_SECONDS_DEFAULT
        return value

    def record_classification_cost(
        self, durations: list[float],
    ) -> float | None:
        """Fold this cycle's per-observation durations into the p95
        estimate. Returns the measured p95, or None with no samples.

        Single-sample cycles ARE learned from -- conservatively: a lone
        observation is treated as a worst-case sample and blended at a
        reduced weight, because admission must plan for the slow case and
        the alternative (discarding the cycle) means the estimator never
        learns when admission keeps allowing one observation.
        """
        if not durations:
            return None
        ordered = sorted(durations)
        if len(ordered) >= 2:
            index = min(
                len(ordered) - 1,
                max(0, int(round(0.95 * (len(ordered) - 1)))))
            measured = float(ordered[index])
            weight = CLASSIFICATION_COST_SMOOTHING
        else:
            # Conservative single-sample update: assume it IS the p95,
            # but move the estimate only part of the way.
            measured = float(ordered[0])
            weight = CLASSIFICATION_COST_SMOOTHING / 2
        previous = self.classification_cost_estimate()
        blended = (
            weight * measured + (1 - weight) * previous)
        self.store.set_scheduler_state("classification_observation_cost", {
            "per_observation_seconds": round(blended, 4),
            "last_measured_p95_seconds": round(measured, 4),
            "samples_this_cycle": len(ordered),
        })
        return measured

    def _classification_counters(self, seed: bool = False) -> dict:
        stored = self.store.scheduler_state(CLASSIFICATION_COUNTERS_STATE_KEY)
        if not stored and seed:
            # One-time reconciliation against the FULL aggregate. This is the
            # expensive GROUP BY and it runs in the analysis / dashboard lane
            # only -- never on the live decision path.
            summary = self.classification_cohort_summary()
            stored = {
                "identity_tiers": summary["identity_tiers"],
                "total": summary["total"],
                "research_eligible": summary["research_eligible"],
                "paper_eligible": summary["paper_eligible"],
            }
            self.store.set_scheduler_state(
                CLASSIFICATION_COUNTERS_STATE_KEY, stored)
        return {
            "identity_tiers": dict(stored.get("identity_tiers") or {}),
            "total": safe_int(stored.get("total"), 0),
            "research_eligible": safe_int(stored.get("research_eligible"), 0),
            "paper_eligible": safe_int(stored.get("paper_eligible"), 0),
        }

    def _bump_classification_counters(
        self, tiers: dict[str, int], *, research_delta: int,
        paper_delta: int, total_delta: int,
    ) -> dict:
        counters = self._classification_counters(seed=False)
        for tier, count in tiers.items():
            counters["identity_tiers"][tier] = (
                counters["identity_tiers"].get(tier, 0) + int(count))
        counters["total"] += int(total_delta)
        counters["research_eligible"] += int(research_delta)
        counters["paper_eligible"] += int(paper_delta)
        self.store.set_scheduler_state(
            CLASSIFICATION_COUNTERS_STATE_KEY, counters)
        return counters

    def classification_cohort_summary(self) -> dict:
        """FULL cumulative cohort aggregation.

        The analysis/dashboard lane's replacement for the aggregate the
        classification loop used to run every cycle. Expensive by design and
        bounded to no lane budget.
        """
        with self.store.connection() as connection:
            rows = connection.execute(
                """SELECT c.identity_tier,
                          COUNT(*) AS tier_count,
                          COALESCE(SUM(c.research_eligible),0) AS research,
                          COALESCE(SUM(c.paper_eligible),0) AS paper
                   FROM flow_observation_classifications c
                   JOIN flow_observations o USING(observation_id)
                   WHERE o.policy_version=? GROUP BY c.identity_tier""",
                (FLOW_EVIDENCE_POLICY_VERSION,),
            ).fetchall()
        tiers = {row["identity_tier"]: int(row["tier_count"]) for row in rows}
        research = sum(int(row["research"]) for row in rows)
        paper = sum(int(row["paper"]) for row in rows)
        total = sum(tiers.values())
        return {
            "identity_tiers": tiers, "total": total,
            "research_eligible": research, "paper_eligible": paper,
            "verified_fraction": (
                round(tiers.get("verified", 0) / total, 4) if total else None),
        }

    def classification_admission(
        self, candidates: int, remaining_seconds: float,
    ) -> dict:
        """Decide how many observations classification may take NOW.

        Budget math: remaining headroom minus the completion/ledger
        reserve, divided by the estimated per-observation cost. Anything
        above the admitted count is durably deferred -- it stays an
        unclassified sealed observation and is classified by a later
        cycle; nothing is dropped.

        The unexplained pre-classification gap is recorded separately so
        the estimate can be tuned against real stage timings.
        """
        cost = self.classification_cost_estimate()
        usable = max(
            0.0,
            remaining_seconds - CLASSIFICATION_COMPLETION_RESERVE_SECONDS)
        if cost <= 0 or usable <= 0:
            admitted = 0
        else:
            admitted = int(usable // cost)
        deferred = max(0, candidates - admitted)
        return {
            "candidates": candidates,
            "admitted": min(candidates, admitted),
            "deferred": deferred,
            "estimated_per_observation_seconds": round(cost, 3),
            "remaining_seconds_at_admission": round(remaining_seconds, 3),
            "completion_reserve_seconds":
                CLASSIFICATION_COMPLETION_RESERVE_SECONDS,
        }

    def seal_near_head_observations(
        self, head_block: int, now: float, *,
        pool_ids: list[str] | None = None,
        deadline: CycleDeadline | None = None,
        limit: int | None = None,
        reserve_seconds: float = 0.0,
        queue_drain_limit: int = SEAL_QUEUE_DRAIN_LIMIT,
        include_fresh: bool = True,
        stage_lane: str = "live",
        defer_quotes: bool = False,
    ) -> dict:
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
        sealed_windows: list[dict] = []
        failures = 0
        # Sub-stage timing, kept as distributions. A stage total cannot
        # distinguish eight steady windows from seven fast ones and a stall,
        # and those need opposite responses: the first wants fewer windows
        # admitted, the second wants the stalling call bounded.
        phase: dict[str, list[float]] = {}

        def record(name: str, started: float) -> None:
            phase.setdefault(name, []).append(time.monotonic() - started)

        selection_started = time.monotonic()
        # Sub-stage attribution begins HERE: the selection query is the first
        # thing that can stall inside sealing, and a kill during it must be
        # distinguishable from a kill mid-quote.
        self.store.mark_lane_stage(
            stage_lane, "fresh_quote_and_observation/observation_selection",
            run_id=getattr(self, "cycle_run_uuid", None),
            remaining=None if deadline is None else deadline.remaining(),
            completed={}, detail={"head_block": int(head_block)})
        # Stale-entry expiry runs BEFORE the queue drain: windows past the
        # near-head overlap can never be sealed honestly, and draining them
        # first keeps the drain from wasting its limit on dead entries.
        expired_stale = self.store.expire_stale_seal_queue(
            head_block=int(head_block),
            freshness_blocks=FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
        )
        # Queued first: these are windows an earlier cycle admitted and could
        # not reach. They are older than anything fresh by construction, and
        # they are the ones that age below `floor` and vanish if not drained.
        # Used as-is. Re-reading flow_signals here is what made the queue
        # undrainable: that table keys on pool_id alone and is replaced on
        # every recompute, so the window a queued row named no longer existed.
        queued_rows = (
            [dict(entry) for entry in self.store.pending_seal_windows(
                max(0, int(queue_drain_limit)))]
            if queue_drain_limit > 0 else []
        )
        with self.store.connection() as connection:
            if not include_fresh:
                windows = []
            elif deadline is not None:
                # The shared deadline ABORTS an in-flight selection query
                # rather than merely refusing to start it.
                with deadline.sqlite_guard(connection):
                    if pool_ids is None:
                        windows = [dict(row) for row in connection.execute(
                            "SELECT * FROM flow_signals"
                            " WHERE window_end_block >= ?", (floor,),
                        )]
                    elif pool_ids:
                        placeholders = ",".join("?" * len(pool_ids))
                        windows = [dict(row) for row in connection.execute(
                            """SELECT fs.* FROM flow_signals fs
                               WHERE fs.window_end_block >= ? AND (
                                 fs.pool_id IN (""" + placeholders + """)
                                 OR NOT EXISTS (
                                   SELECT 1 FROM flow_observations o
                                   WHERE o.pool_id=fs.pool_id
                                     AND o.window_end_block=fs.window_end_block
                                     AND o.policy_version=?
                                 )
                               ) ORDER BY fs.window_end_block DESC LIMIT 100""",
                            [floor, *pool_ids, FLOW_EVIDENCE_POLICY_VERSION],
                        )]
                    else:
                        windows = [dict(row) for row in connection.execute(
                            """SELECT fs.* FROM flow_signals fs
                               WHERE fs.window_end_block >= ? AND NOT EXISTS (
                                 SELECT 1 FROM flow_observations o
                                 WHERE o.pool_id=fs.pool_id
                                   AND o.window_end_block=fs.window_end_block
                                   AND o.policy_version=?
                               ) ORDER BY fs.window_end_block DESC LIMIT 100""",
                            (floor, FLOW_EVIDENCE_POLICY_VERSION),
                        )]
            else:
                if pool_ids is None:
                    windows = [dict(row) for row in connection.execute(
                        "SELECT * FROM flow_signals WHERE window_end_block >= ?",
                        (floor,),
                    )]
                elif pool_ids:
                    placeholders = ",".join("?" * len(pool_ids))
                    windows = [dict(row) for row in connection.execute(
                        """SELECT fs.* FROM flow_signals fs
                           WHERE fs.window_end_block >= ? AND (
                             fs.pool_id IN (""" + placeholders + """)
                             OR NOT EXISTS (
                               SELECT 1 FROM flow_observations o
                               WHERE o.pool_id=fs.pool_id
                                 AND o.window_end_block=fs.window_end_block
                                 AND o.policy_version=?
                             )
                           ) ORDER BY fs.window_end_block DESC LIMIT 100""",
                        [floor, *pool_ids, FLOW_EVIDENCE_POLICY_VERSION],
                    )]
                else:
                    windows = [dict(row) for row in connection.execute(
                        """SELECT fs.* FROM flow_signals fs
                           WHERE fs.window_end_block >= ? AND NOT EXISTS (
                             SELECT 1 FROM flow_observations o
                             WHERE o.pool_id=fs.pool_id
                               AND o.window_end_block=fs.window_end_block
                               AND o.policy_version=?
                           ) ORDER BY fs.window_end_block DESC LIMIT 100""",
                        (floor, FLOW_EVIDENCE_POLICY_VERSION),
                    )]
        seen: set[tuple[str, int]] = set()
        ordered: list[dict] = []
        for window in [*queued_rows, *windows]:
            key = (str(window["pool_id"]), int(window["window_end_block"]))
            if key in seen:
                continue
            seen.add(key)
            ordered.append(window)
        windows = ordered
        # PRE-ENQUEUE every resolved target before the first blocking remote
        # call.  A supervisor hard kill does not execute Python finally
        # blocks; queuing only during settlement therefore lost fresh windows
        # if the OS terminated the worker.  INSERT OR IGNORE keeps this
        # idempotent for entries already pulled from the durable queue.
        prequeued_now = self.store.enqueue_seal(
            windows, "live_lane_preclaim") if windows else 0
        record("selection", selection_started)
        windows_available = len(windows)

        # Admission by the two-part cost model, not by a fixed count. A static
        # limit of 8 spends the same time whether the cycle has 20 seconds
        # left or 6, which is how sealing came to overrun 46 of 64 failed
        # cycles. usable = remaining - downstream_reserve - fixed cost; what
        # is left must buy whole windows at per_window_cost_p95.
        static_limit = len(windows) if limit is None else max(0, int(limit))
        model = self.live_planning_seal_cost_model()
        fixed_cost = model["fixed_observation_cost_p95"]
        settlement_reserve = model["queue_settlement_p95"]
        cost = model["per_window_cost_p95"]
        downstream_reserve = max(
            float(reserve_seconds), model["downstream_reserve_p95"])
        admitted = static_limit
        headroom = None
        usable = None
        if deadline is not None:
            headroom = max(0.0, deadline.remaining())
            # Queue settlement is reserved SEPARATELY (it has its own
            # component) -- not folded into fixed cost. Counting it in both
            # places double-charged the budget by its full amount.
            usable = max(
                0.0, headroom - downstream_reserve - settlement_reserve
                - fixed_cost)
            if usable <= 0 or cost <= 0:
                admitted = 0
            else:
                admitted = min(
                    static_limit,
                    int(usable // max(cost, 0.05)))
            # A probe is owed whenever admission has collapsed to zero with
            # budget still on the table -- not only before the model exists.
            #
            # Restricting it to an uninitialised epoch left a closed loop: an
            # inflated p95 admits nothing, admitting nothing measures nothing,
            # and an estimate with no new samples never falls. Measured live:
            # per_window_cost_p95 3.266s against 10.468s of headroom, with
            # measured_cost_seconds 0.0 and 39 of 77 recent cycles admitting
            # zero windows. Fresh sealing fell from 13,283/day to 494/day and
            # signal production stopped entirely on 25-26 August, because a
            # window can only qualify while it is fresh.
            #
            # The p95 inflates from the stall tail (231 stalls, 2.16%), so the
            # typical window still costs ~0.34s -- the estimate is not wrong
            # about its tail, it is simply unable to notice the tail passing.
            # One window is already the established safe floor: it is exactly
            # what the stall guard permits, and it fits inside the reserve by
            # construction because `usable` is what remains after every
            # reserve is subtracted.
            uninitialised = not bool(model.get("epoch_model_initialized"))
            cold_start_probe = bool(
                admitted == 0 and static_limit > 0 and usable > 0)
            probe_kind = (
                None if not cold_start_probe
                else "cold_start" if uninitialised else "estimator_recovery")
            if cold_start_probe:
                # An epoch reset has no normative timing distribution yet, and
                # a collapsed one has no way to earn a better distribution.
                # One bounded probe answers both; admitting the entire batch
                # would weaken policy, while admitting zero forever makes the
                # estimator impossible to bootstrap OR to recover.
                admitted = 1
        else:
            cold_start_probe = False
            probe_kind = None
        stall_guard_active = bool(model.get("stall_guard_active"))
        stall_guard_action = "normal"
        if stall_guard_active and admitted > 1:
            # Tighten-only recovery probe: the live lane may measure one
            # bounded window, but it may not resume normal throughput until
            # the current-epoch stall rate is back within policy.
            admitted = 1
            stall_guard_action = "cap_one_recovery_probe"
        elif stall_guard_active:
            stall_guard_action = "already_within_probe_cap"
        censored_guard_active = bool(model.get("censored_guard_active"))
        censored_guard_action = "normal"
        if censored_guard_active and admitted > 1:
            admitted = 1
            censored_guard_action = "cap_one_censored_recovery_probe"
        elif censored_guard_active:
            censored_guard_action = "already_within_probe_cap"
        admitted = min(admitted, static_limit)
        deferred = windows[admitted:]
        windows = windows[:admitted]

        def persist_substage(substage: str, window_detail: dict | None = None,
                             **extra) -> None:
            """Durable sub-stage marker: WHERE inside sealing are we?

            Committed BEFORE the blocking call it names so a forced
            termination records the exact window -- index and pool -- the
            process was on when it was killed.
            """
            self.store.mark_lane_stage(
                stage_lane, f"fresh_quote_and_observation/{substage}",
                run_id=getattr(self, "cycle_run_uuid", None),
                remaining=(
                    None if deadline is None else deadline.remaining()),
                completed={}, detail={**(window_detail or {}), **extra})

        prefetch_started = time.monotonic()
        persist_substage("quote_prefetch", windows=len(windows),
                         deferred=len(deferred))
        # Optional by construction: a market client that cannot batch still
        # seals correctly, one round trip at a time. Reported rather than
        # assumed, because a silent fall-back to the sequential path is
        # exactly the 16.4s regression this stage was built to remove, and it
        # would otherwise look identical to a slow provider.
        primer = getattr(getattr(self, "v4_market", None),
                         "prime_window_quotes", None)
        if defer_quotes:
            prime = {"supported": True, "deferred_to_evidence_lane": True,
                     "batched": 0, "windows": len(windows)}
        elif primer is None:
            prime = {"supported": False, "reason": "client_cannot_batch"}
        elif not windows:
            prime = {"supported": True, "batched": 0, "windows": 0}
        else:
            prime = dict(primer(windows, deadline=deadline))
            prime["supported"] = True
        phase.setdefault("prefetch", []).append(time.monotonic() - prefetch_started)

        def settle_queue() -> int:
            """Durably queue every unsealed window. Runs even on failure."""
            settle_started = time.monotonic()
            persist_substage("queue_settlement",
                             sealed=len(sealed_windows), deferred=len(deferred))
            # Retried on lock contention, not abandoned.
            #
            # The backfill lane failed 3 of 20 runs here with "database is
            # locked" -- not on deadline (47.7s used of 120s, 76.5s left at
            # stage start) but by losing a write race to the live lane, which
            # holds priority by design. Each loss recorded the whole run
            # `failed` and discarded work already done: one had sealed 47
            # observations and deferred 68 before the settlement was refused.
            #
            # busy_timeout is already 10s, so the lock outlived it; a longer
            # global timeout would make every writer wait longer for the same
            # outcome. Retrying only this write, only on a lock, and only
            # while the deadline allows, is the bounded version.
            #
            # Safe to repeat: complete_seal_queue guards on
            # `completed_at IS NULL` and enqueue_seal is INSERT OR IGNORE, so
            # a partially-applied settlement re-applies to the same state.
            settle_attempts = 0
            while True:
                settle_attempts += 1
                try:
                    self.store.complete_seal_queue(sealed_windows)
                    # Idempotent belt-and-suspenders write for callers that
                    # supplied a malformed row during preclaim. enqueue_seal
                    # historically returns rows attempted (not rows inserted),
                    # so it must not be added to the preclaim count or
                    # telemetry double-counts every deferred window.
                    self.store.enqueue_seal(deferred, "live_lane_headroom")
                    break
                except sqlite3.OperationalError as error:
                    contended = "database is locked" in str(error).lower()
                    exhausted = (
                        settle_attempts >= SETTLEMENT_LOCK_RETRY_ATTEMPTS)
                    no_time = (
                        deadline is not None
                        and deadline.remaining()
                        <= SETTLEMENT_LOCK_RETRY_BACKOFF_SECONDS)
                    # A lock that never clears is a real problem and must
                    # still fail the run; only transient contention is
                    # absorbed, and only while there is budget to absorb it.
                    if not contended or exhausted or no_time:
                        raise
                    time.sleep(SETTLEMENT_LOCK_RETRY_BACKOFF_SECONDS)
            if settle_attempts > 1:
                phase.setdefault("queue_settle_retries", []).append(
                    float(settle_attempts - 1))
            phase.setdefault("queue_settle", []).append(
                time.monotonic() - settle_started)
            per_window_cost = self.record_seal_cost(
                phase, len(sealed_windows))
            # Public telemetry keeps its historical meaning: how many
            # selected windows remain deferred after this pass.  Preclaim may
            # have attempted more inserts, but sealed entries are completed
            # before this count is returned.
            queued_now = len(deferred)
            settlement["queued_now"] = queued_now
            settlement["measured_cost_seconds"] = per_window_cost
            return queued_now

        settlement: dict = {}
        try:
            for index, window in enumerate(windows):
                window_started = time.monotonic()
                # The reserve, not expiry: stopping when the deadline has
                # already passed leaves nothing for the stages that cannot
                # yield.  Queue settlement is separately reserved because it
                # is what makes deferral durable. Checked BEFORE each remote
                # call...
                if (deadline is not None
                        and deadline.remaining() <= (
                            downstream_reserve + settlement_reserve)):
                    deferred.extend(windows[index:])
                    del windows[index:]
                    break
                window_detail = {
                    "window_index": index, "pool_id": str(window["pool_id"]),
                    "window_end_block": int(window["window_end_block"]),
                }
                hashes_started = time.monotonic()
                with self.store.connection() as connection:
                    hashes = [
                        row[0] for row in connection.execute(
                            """
                            SELECT transaction_hash FROM swap_observations
                            WHERE pool_id=? AND block_number BETWEEN ? AND ?
                            """,
                            (window["pool_id"],
                             window["window_start_block"],
                             window["window_end_block"]),
                        )
                    ]
                phase.setdefault("transaction_hashes", []).append(
                    time.monotonic() - hashes_started)
                persist_substage("window_quote_rpc", window_detail,
                                 deferred=bool(defer_quotes))
                quote_started = time.monotonic()
                try:
                    market = (
                        {"verified": False,
                         "quote_status": "deferred_to_evidence_lane"}
                        if defer_quotes else self.v4_market.snapshot(
                            {"pool_id": window["pool_id"],
                             "token_address": window["token_address"]},
                            quote_block=int(window["window_end_block"]),
                        ))
                except CycleDeadlineExceeded:
                    # The deadline fired INSIDE the remote call. This is not
                    # a bad quote: every remaining window is over budget and
                    # must queue in the finally-block, and the exception
                    # must reach the supervisor so the run is recorded as
                    # deadline_exceeded rather than silently completing.
                    deferred.extend(windows[index:])
                    del windows[index:]
                    raise
                except Exception:
                    market = {"verified": False,
                              "reason": "observation_quote_failed"}
                    failures += 1
                quote_elapsed = time.monotonic() - quote_started
                phase.setdefault("quote_rpc", []).append(quote_elapsed)
                # ...and AFTER it: an individual stalled call that returned
                # late has already consumed budget the remaining windows
                # cannot plan around.
                if (deadline is not None
                        and deadline.remaining() <= (
                            downstream_reserve + settlement_reserve)):
                    deferred.append(window)
                    deferred.extend(windows[index + 1:])
                    del windows[index:]
                    prime["interrupted_after_window"] = window_detail
                    break
                persist_substage("observation_commit", window_detail)
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
                if gap_count == 0 and not exitable and not defer_quotes:
                    gaps = list(gaps or []) + ["exitable_round_trip"]
                    gap_count = len(gaps)
                    features = dict(features)
                    features["qualification_gaps"] = gaps
                    features["round_trip_return"] = round_trip
                role = "signal" if gap_count == 0 else "matched_control"
                seal_timings: dict[str, float] = {}
                observation_id = self.store.seal_flow_observation(
                    role=role, gap_count=gap_count, timings=seal_timings,
                    pool_id=window["pool_id"],
                    token_address=window["token_address"],
                    observation_head=int(head_block),
                    window_start_block=int(window["window_start_block"]),
                    window_end_block=int(window["window_end_block"]),
                    transaction_hashes=hashes, features=features,
                    quote=market, quote_block=int(window["window_end_block"]),
                    now=now,
                )
                for name, value in seal_timings.items():
                    phase.setdefault(name, []).append(value)
                sealed_windows.append(window)
                if observation_id:
                    sealed.append(observation_id)
                phase.setdefault("window_total", []).append(
                    time.monotonic() - window_started)
        finally:
            # Close what this cycle sealed, queue what it did not -- ALWAYS,
            # including when the deadline kill lands mid-loop. Both halves
            # matter: without the first the queue never empties, without the
            # second a window that fell below `floor` is never seen again,
            # and a hard termination must leave every unprocessed window in
            # the durable queue rather than silently dropped.
            queued_now = settle_queue()
        backlog = self.store.seal_queue_backlog()
        with self.store.connection() as connection:
            cumulative = connection.execute(
                "SELECT COUNT(*) FROM flow_observations WHERE policy_version=?",
                (FLOW_EVIDENCE_POLICY_VERSION,),
            ).fetchone()[0]
        return {
            "windows_considered": len(windows),
            "windows_available": windows_available,
            "windows_admitted": admitted,
            # Windows this cycle did not seal, each of which is now queued.
            # The old figure subtracted sealed from available, which counted
            # an already-sealed duplicate as a deferral.
            "windows_deferred": len(deferred),
            "windows_queued": queued_now,
            "windows_expired_stale": expired_stale,
            "seal_queue_backlog": backlog,
            "admission": {
                "static_limit": static_limit,
                "headroom_seconds": (
                    None if headroom is None else round(headroom, 3)),
                "usable_seconds": (
                    None if usable is None else round(usable, 3)),
                "reserve_seconds": round(float(downstream_reserve), 3),
                "fixed_observation_cost_estimate_seconds": round(fixed_cost, 3),
                "queue_settlement_reserve_seconds": round(
                    settlement_reserve, 3),
                "cost_estimate_seconds": round(cost, 3),
                "downstream_reserve_estimate_seconds": round(
                    model["downstream_reserve_p95"], 3),
                "measured_cost_seconds": next(
                    (round(settlement[k], 3)
                     for k in ("measured_cost_seconds",)
                     if settlement.get(k) is not None), None),
                "queue_drained": len(queued_rows),
                "model_censored_samples": safe_int(
                    model.get("censored_samples"), 0),
                "censored_guard_active": censored_guard_active,
                "censored_guard_action": censored_guard_action,
                "censored_guard_components": list(
                    model.get("censored_guard_components") or []),
                "quarantined_censored_components": dict(
                    model.get("quarantined_censored_components") or {}),
                "raw_effective_costs": dict(
                    model.get("raw_effective_costs") or {}),
                "stall_guard_active": stall_guard_active,
                "stall_guard_action": stall_guard_action,
                "cold_start_probe": cold_start_probe,
                # WHICH probe: bootstrapping a new epoch, or recovering an
                # estimate that had locked admission at zero. They look
                # identical in the count and need opposite follow-up.
                "probe_kind": probe_kind,
                "fixed_stall_rate": safe_float(
                    model.get("fixed_stall_rate"), 0.0),
                "fixed_stall_population": safe_int(
                    model.get("fixed_stall_population"), 0),
                "seal_stall_rate": safe_float(
                    model.get("seal_stall_rate"), 0.0),
                "seal_stall_population": safe_int(
                    model.get("seal_stall_population"), 0),
                "stall_rate_limit": SEAL_STALL_RATE_MAX,
                "windows_preclaimed": int(prequeued_now),
            },
            "prefetch": prime,
            "phase_seconds": {
                name: _spread(values) for name, values in phase.items()
            },
            "sealed_this_cycle": len(sealed),
            "observation_ids": sealed,
            "cumulative_observations": cumulative,
            "quote_failures": failures, "observation_head": int(head_block),
        }

    def classify_sealed_observations(
        self, decision_head: int, *, observation_ids: list[str] | None = None,
        deadline: CycleDeadline | None = None,
        admission_limit: int | None = None,
        allow_remote_quotes: bool = True,
    ) -> dict:
        """Classify sealed observations after enrichment. Never mutates them.

        ``admission_limit`` caps how many observations this cycle may
        classify (from the admission controller). Observations beyond the
        cap are NOT touched: they remain unclassified sealed observations
        and a later cycle classifies them. Nothing is dropped.
        """
        # Cumulative totals read as a rate unless the delta is stated beside
        # them: a backlog sweep classifying 392 observations while the cycle
        # sealed 6 was misread as 47 verified per cycle when the true figure
        # was 12% of a cumulative 392. Every count below is labelled.
        scoped_tiers: dict[str, int] = {}
        research = paper = 0
        newly_classified = 0
        processed = 0
        # BOUNDED selection. The old query materialized EVERY unclassified
        # observation -- thousands of rows -- plus Python-side re-sorting,
        # then the cumulative GROUP BY aggregated the whole cohort: all of it
        # on the decision-critical path. Now: current-cycle ids first (they
        # are known by primary key), then the oldest deferred rows fill the
        # remaining admission slots via SQL ORDER BY ... LIMIT ? -- the
        # backlog is never loaded into Python beyond what this cycle will
        # actually process.
        limit = (
            None if admission_limit is None else max(0, int(admission_limit)))
        rows: list[dict] = []
        current_selected = 0
        with self.store.connection() as connection:
            base_columns = """
                    SELECT o.observation_id, o.pool_id, o.token_address,
                           fs.identity_coverage, fs.qualification_gaps_json,
                           o.sealed_at
                    FROM flow_observations o
                    LEFT JOIN flow_signals fs ON fs.pool_id=o.pool_id
                    WHERE o.policy_version=?
                      AND NOT EXISTS (
                          SELECT 1 FROM flow_observation_classifications c
                          WHERE c.observation_id = o.observation_id)
                    """
            order = " ORDER BY o.sealed_at ASC, o.observation_id ASC "
            if observation_ids:
                # Current-cycle observations classify FIRST so a decision
                # attaches while evidence is freshest. They arrive by primary
                # key from the just-sealed batch (a handful of rows), so
                # selecting them uncapped by the limit is bounded by
                # construction -- and the admission report still needs to see
                # them all to account for what was deferred.
                ids = [str(i) for i in observation_ids]
                chunk = max(1, min(len(ids), 500))
                for start in range(0, len(ids), chunk):
                    group = ids[start:start + chunk]
                    placeholders = ",".join("?" * len(group))
                    rows.extend(dict(row) for row in connection.execute(
                        base_columns
                        + f" AND o.observation_id IN ({placeholders})" + order,
                        [FLOW_EVIDENCE_POLICY_VERSION, *group]))
                current_selected = len(rows)
            remaining_slots = None if limit is None else max(
                0, limit - current_selected)
            if limit is None or remaining_slots > 0:
                deferred_query = (
                    base_columns + order + " LIMIT ?")
                params: list = [FLOW_EVIDENCE_POLICY_VERSION]
                if limit is not None:
                    params.append(int(remaining_slots))
                else:
                    # No admission limit (legacy/analysis callers): still
                    # bounded -- the full backlog is never materialized in
                    # one query result.
                    params.append(CLASSIFICATION_BACKLOG_SCAN_LIMIT)
                rows.extend(dict(row) for row in connection.execute(
                    deferred_query, params))
            unclassified_total = connection.execute(
                """SELECT COUNT(*) FROM flow_observations o
                   WHERE o.policy_version=? AND NOT EXISTS (
                       SELECT 1 FROM flow_observation_classifications c
                       WHERE c.observation_id=o.observation_id)""",
                (FLOW_EVIDENCE_POLICY_VERSION,),
            ).fetchone()[0]
        admitted_rows = rows
        admission_exceeded = 0
        if limit is not None:
            admitted_rows = rows[:limit]
            # Counted, never materialized: the deferral figure covers the
            # WHOLE remaining backlog, not just the rows this query returned.
            admission_exceeded = max(0, int(unclassified_total) - len(admitted_rows))
        quotes_taken = quote_failures = 0
        durations: list[float] = []
        for row in admitted_rows:
            started = time.monotonic()
            if deadline is not None and deadline.expired():
                break
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
            if (allow_remote_quotes and candidate
                    and quotes_taken < FLOW_DECISION_QUOTE_LIMIT):
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
                # A stalled individual re-quote must not hand its overrun to
                # the next row: re-check AFTER the remote call returns.
                if deadline is not None and deadline.expired():
                    break
            verdict = self.store.classify_flow_observation(
                row["observation_id"], decision_head=int(decision_head),
                identity_coverage=row.get("identity_coverage"), gates=gates,
                decision_quote=decision_quote,
            )
            scoped_tiers[verdict["identity_tier"]] = (
                scoped_tiers.get(verdict["identity_tier"], 0) + 1)
            research += verdict["research_eligible"]
            paper += verdict["paper_eligible"]
            # Selection excludes already-classified observations, so every
            # processed row is newly classified by construction -- without
            # loading the whole classifications table to double-check.
            newly_classified += 1
            processed += 1
            durations.append(time.monotonic() - started)
        # Fold this cycle's per-observation durations into the p95 cost
        # estimate the admission controller plans with.
        measured_p95 = self.record_classification_cost(durations)
        admission_summary = {
            "admission_limit": admission_limit,
            "admission_exceeded": admission_exceeded,
            "measured_p95_seconds": (
                round(measured_p95, 3) if measured_p95 is not None else None),
            "estimate_seconds": self.classification_cost_estimate(),
        }
        # Cumulative cohort aggregation MOVED OFF the live path. The old
        # GROUP BY over the whole classifications join cost real seconds per
        # cycle; these counters are maintained incrementally instead and are
        # reconciled against the full aggregate only in the analysis /
        # dashboard lane (classification_cohort_summary).
        cumulative = self._bump_classification_counters(
            scoped_tiers, research_delta=research, paper_delta=paper,
            total_delta=newly_classified)
        tiers = dict(cumulative["identity_tiers"])
        cumulative_total = int(cumulative["total"])
        cumulative_research = int(cumulative["research_eligible"])
        cumulative_paper = int(cumulative["paper_eligible"])
        verified = tiers.get("verified", 0)
        return {
            "classified_this_cycle": newly_classified,
            "reclassified_this_cycle": processed - newly_classified,
            "scoped_rows_selected": len(rows),
            "scoped_rows_processed": processed,
            # Counted, never materialized: the deferral figure covers the
            # WHOLE remaining backlog, not just the rows this query returned.
            "scoped_rows_deferred": max(
                0, int(unclassified_total) - processed),
            "admission": admission_summary,
            "scoped_identity_tiers": scoped_tiers,
            "scoped_research_eligible": research,
            "scoped_paper_eligible": paper,
            "decision_quotes_taken": quotes_taken,
            "decision_quote_failures": quote_failures,
            "decision_quote_limit": FLOW_DECISION_QUOTE_LIMIT,
            "cumulative_classified": cumulative_total,
            "cumulative_identity_tiers": tiers,
            "cumulative_verified_fraction": (
                round(verified / cumulative_total, 4) if cumulative_total else None
            ),
            "cumulative_research_eligible": cumulative_research,
            "cumulative_paper_eligible": cumulative_paper,
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
        capture.update(self.quote_pending_flow_evidence())
        return capture

    def quote_pending_flow_evidence(
        self, *, limit: int = EVIDENCE_ENTRY_QUOTE_LIMIT,
        deadline: CycleDeadline | None = None,
    ) -> dict:
        """Price already-captured events outside the freshness-critical lane.

        Event metadata is committed prospectively by ``run_live_lane``.  The
        quote is still pinned to the event's immutable window end, so moving
        this remote call to the evidence lane changes latency ownership, not
        the evidence being measured.  Pending rows are durable and retryable.
        """
        quoted = 0
        quote_failures = 0
        selected = self.store.pending_flow_quotes(max(0, int(limit)))
        deferred = 0
        for index, event in enumerate(selected):
            if deadline is not None and deadline.expired():
                deferred = len(selected) - index
                break
            candidate = {
                "pool_id": event["pool_id"],
                "token_address": event["token_address"],
            }
            try:
                market = self.v4_market.snapshot(
                    candidate, quote_block=int(event["window_end_block"]),
                )
            except BackgroundRpcPriorityDeferred:
                # A stage slice ending is scheduling, not evidence that the
                # pool failed to quote. Leave this and later rows pending.
                deferred = len(selected) - index
                break
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
        return {
            "entry_quotes_selected": len(selected),
            "entry_quotes_verified": quoted,
            "quote_failures": quote_failures,
            "entry_quotes_deferred": deferred,
            "limit": max(0, int(limit)),
        }

    def quote_pending_flow_observations(
        self, *, limit: int = EVIDENCE_OBSERVATION_QUOTE_LIMIT,
        deadline: CycleDeadline | None = None,
    ) -> dict:
        """Attach block-pinned quotes outside the freshness-critical lane."""
        selected = self.store.pending_flow_observation_quotes(
            max(0, int(limit)))
        quoted = failures = decision_quoted = deferred = 0
        for index, row in enumerate(selected):
            if deadline is not None and deadline.expired():
                deferred = len(selected) - index
                break
            candidate = {
                "pool_id": row["pool_id"],
                "token_address": row["token_address"],
            }
            try:
                entry_quote = self.v4_market.snapshot(
                    candidate, quote_block=int(row["window_end_block"]))
                entry_execution = (entry_quote or {}).get(
                    "execution_quote") or (entry_quote or {})
                if not entry_execution.get("verified"):
                    failures += 1
                    continue
                if deadline is not None:
                    deadline.raise_if_expired("observation_entry_quote")
                try:
                    gates = json.loads(row.get("gates_json") or "[]")
                except (TypeError, ValueError, json.JSONDecodeError):
                    gates = ["invalid_classification_gates"]
                decision_quote = None
                decision_head = safe_int(row.get("decision_head"), 0)
                if row.get("identity_tier") == "verified" and not gates:
                    decision_head = int(self.rpc.get_block_number())
                    decision_quote = self.v4_market.snapshot(
                        candidate, quote_block=decision_head)
                    decision_quoted += 1
                    if deadline is not None:
                        deadline.raise_if_expired(
                            "observation_decision_quote")
                self.store.record_flow_observation_quotes(
                    row["observation_id"], entry_quote=entry_quote,
                    entry_quote_block=int(row["window_end_block"]),
                    decision_quote=decision_quote,
                    decision_quote_block=(
                        decision_head if decision_quote is not None else None),
                )
                verdict = self.store.classify_flow_observation(
                    row["observation_id"], decision_head=decision_head,
                    identity_coverage=row.get("identity_coverage"),
                    gates=gates, decision_quote=decision_quote,
                    entry_quote=entry_quote,
                )
                if verdict.get("paper_eligible"):
                    self._bump_classification_counters(
                        {}, research_delta=0, paper_delta=1,
                        total_delta=0)
                quoted += 1
            except CycleDeadlineExceeded:
                deferred = len(selected) - index
                break
            except BackgroundRpcPriorityDeferred:
                deferred = len(selected) - index
                break
            except Exception:
                failures += 1
        return {
            "selected": len(selected), "quoted": quoted,
            "decision_quotes": decision_quoted,
            "quote_failures": failures, "deferred": deferred,
        }

    def observe_flow_observation_outcomes(
        self, now: float, limit: int = 40,
        *, deadline: CycleDeadline | None = None,
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
        deferred = 0
        for index, due in enumerate(due_rows):
            # Reserve, not expiry. `expired()` is only true once the budget is
            # already spent, so the item it admits is the one that overruns --
            # and a killed run discards the resolutions it had finished, not
            # just the one in flight.
            if (deadline is not None
                    and deadline.remaining()
                    < EVIDENCE_OUTCOME_ITEM_RESERVE_SECONDS):
                deferred = len(due_rows) - index
                break
            try:
                head = int(self.rpc.get_block_number())
            except BackgroundRpcPriorityDeferred:
                deferred = len(due_rows) - index
                break
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
            except BackgroundRpcPriorityDeferred:
                deferred = len(due_rows) - index
                break
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
            "deferred": deferred, "limit": limit,
        }

    def observe_flow_evidence_outcomes(
        self, now: float, head_block: int, limit: int = 30,
        *, deadline: CycleDeadline | None = None,
    ) -> dict:
        due_rows = self.store.due_flow_outcomes(now, limit)
        observed = 0
        non_exitable = 0
        deferred = 0
        for index, due in enumerate(due_rows):
            # Reserve one measured remote-item tail. Merely checking expiry
            # admits the request that consumes the last seconds of the lane.
            if (deadline is not None
                    and deadline.remaining()
                    < EVIDENCE_OUTCOME_ITEM_RESERVE_SECONDS):
                deferred = len(due_rows) - index
                break
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
            except BackgroundRpcPriorityDeferred:
                deferred = len(due_rows) - index
                break
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
            "non_exitable": non_exitable, "deferred": deferred,
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
                entry = self.guarded_paper_entry(
                    latest, market, run_id=self.cycle_run_uuid,
                    priority_reason="executable_market_recheck",
                )
                if entry.get("entered"):
                    entries += 1
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
        # Per-observation cost, which nothing measured. The stage reported
        # 145.156s while its three named sub-timers -- selection 0.078s,
        # expiration 0.375s, market batch 0.984s -- accounted for 1.4s of it.
        # The remaining 143s was two outcomes at roughly 70s each, invisible.
        # Same shape as the near-head telemetry and the identity census: the
        # work being timed was not the work costing the time.
        observation_seconds: list[float] = []
        for index, due in enumerate(due_outcomes):
            observation_started = time.monotonic()
            # Reserve, not expiry. Stopping when the budget has already gone
            # admits the item that overruns it, and the whole run is then
            # killed -- discarding every outcome it had already observed.
            if (deadline_monotonic is not None
                    and time.monotonic()
                    >= deadline_monotonic - ANALYSIS_OUTCOME_ITEM_RESERVE_SECONDS):
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
            observation_seconds.append(time.monotonic() - observation_started)
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
            # Reported as a distribution, not a mean: one 70-second outlier
            # and ten fast rows describe very different problems.
            "observation_seconds_total": round(sum(observation_seconds), 3),
            "observation_seconds_slowest": (
                round(max(observation_seconds), 3) if observation_seconds else None
            ),
            "observation_seconds_median": (
                round(sorted(observation_seconds)[len(observation_seconds) // 2], 3)
                if observation_seconds else None
            ),
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

    def v3_market_snapshot(self, candidate: dict) -> dict:
        """Authoritative V3 state fallback for paper-position marking."""
        pair = str(candidate.get("pair_address") or "").lower()
        token = str(candidate.get("token_address") or "").lower()
        if len(pair.removeprefix("0x")) != 40:
            return {}
        slot0_raw = self.rpc.call(pair, "0x" + V3_SLOT0_SELECTOR)
        liquidity_raw = self.rpc.call(pair, "0x" + V3_LIQUIDITY_SELECTOR)
        sqrt_price, _tick = RobinhoodV4MarketClient._decode_slot0(slot0_raw)
        liquidity = int(str(liquidity_raw or "0x0"), 16)
        if sqrt_price <= 0 or liquidity <= 0:
            return {
                "source": "uniswap_v3_onchain",
                "current_state_verified": False,
                "confirmed_no_market": True,
                "reason": "v3_zero_active_liquidity",
                "market_resolution": {
                    "winner": "uniswap_v3_onchain",
                    "authoritative_no_liquidity": True,
                    "confirmed_no_market": True,
                    "retryable_provider_failure": False,
                },
            }

        def address_result(raw: str) -> str:
            return "0x" + str(raw or "").removeprefix("0x")[-40:].lower()

        token0 = address_result(self.rpc.call(pair, "0x" + V3_TOKEN0_SELECTOR))
        token1 = address_result(self.rpc.call(pair, "0x" + V3_TOKEN1_SELECTOR))
        if token not in {token0, token1}:
            return {}
        anchor = token1 if token0 == token else token0
        if anchor not in {USDG_ADDRESS.lower(), WETH_ADDRESS.lower()}:
            return {}
        token_decimals = self.rpc.erc20_decimals(token)
        anchor_decimals = self.rpc.erc20_decimals(anchor)
        total_supply = self.rpc.erc20_total_supply(token)
        if anchor == USDG_ADDRESS.lower():
            anchor_usd = 1.0
        else:
            anchor_usd, _source = self.market.wrapped_native_usd()
        if anchor_usd <= 0:
            return {}
        raw_ratio = (sqrt_price / (1 << 96)) ** 2
        # raw_ratio is raw token1 per raw token0. Convert it to human units,
        # then invert when the learned token is token1.
        human_token1_per_token0 = raw_ratio * (
            10 ** (token_decimals - anchor_decimals)
            if token0 == token else
            10 ** (anchor_decimals - token_decimals))
        token_usd = (
            anchor_usd * human_token1_per_token0
            if token0 == token else
            anchor_usd / human_token1_per_token0
            if human_token1_per_token0 else 0.0)
        if token_usd <= 0:
            return {}
        token_scale = 10 ** token_decimals
        anchor_scale = 10 ** anchor_decimals
        liquidity_usd = (
            2 * liquidity * (token_usd * anchor_usd) ** 0.5
            / (token_scale * anchor_scale) ** 0.5)
        supply = total_supply / token_scale
        return {
            "source": "uniswap_v3_onchain",
            "current_state_verified": True,
            "price_usd": token_usd,
            "liquidity_usd": liquidity_usd,
            "market_cap_usd": token_usd * supply,
            "fdv_usd": token_usd * supply,
            "market_resolution": {
                "winner": "uniswap_v3_onchain",
                "retryable_provider_failure": False,
            },
        }

    def evaluate_open_positions(
        self, now: float, *, deadline: CycleDeadline | None = None,
    ) -> dict:
        """Evaluate every open paper position, batching shared market reads."""
        with self.store.connection() as connection:
            candidates = [dict(row) for row in connection.execute(
                """
                SELECT c.*,p.quantity paper_quantity
                FROM candidates c JOIN positions p USING(token_address)
                WHERE p.status='open' ORDER BY p.opened_at
                """
            )]
        checked = len(candidates)
        marked = unverified = closed = partial_exits = failures = 0
        standard = [
            candidate for candidate in candidates
            if candidate.get("source_version") != SOURCE_V4
        ]
        v4 = [
            candidate for candidate in candidates
            if candidate.get("source_version") == SOURCE_V4
        ]
        resolved: dict[str, dict] = {}
        market_batch_requests = 0
        market_batch_failed = False
        market_batch_error = None
        failure_details: list[dict] = []
        if standard:
            try:
                if deadline is not None:
                    deadline.raise_if_expired("position_market_batch")
                tokens = [candidate["token_address"] for candidate in standard]
                if hasattr(self.market, "snapshots_many"):
                    context = (
                        self._market_deadline(deadline)
                        if deadline is not None else nullcontext()
                    )
                    with context:
                        grouped = self.market.snapshots_many(tokens)
                    market_batch_requests = (len(set(
                        token.lower() for token in tokens
                    )) + 29) // 30
                    for candidate in standard:
                        token = candidate["token_address"].lower()
                        resolved[token] = self.market.select_snapshot(
                            grouped.get(token, []), candidate.get("pair_address")
                        )
                else:
                    # Compatibility for injected test/offline clients. The
                    # production client always takes the batched path.
                    for candidate in standard:
                        resolved[candidate["token_address"].lower()] = (
                            self.market.snapshot(
                                candidate["token_address"],
                                candidate.get("pair_address"),
                            )
                        )
                if deadline is not None:
                    deadline.raise_if_expired("position_market_batch")
            except Exception as exc:
                market_batch_failed = True
                market_batch_error = RobinhoodMarketClient._provider_error(exc)
        for candidate in candidates:
            if deadline is not None:
                deadline.raise_if_expired("position_mark_commit")
            try:
                if candidate.get("source_version") == SOURCE_V4:
                    market = self.v4_market.snapshot(candidate)
                else:
                    market = resolved.get(candidate["token_address"].lower(), {})
                    if (candidate.get("source_version") == SOURCE_V3
                            and not market):
                        market = self.v3_market_snapshot(candidate)
                    elif market_batch_failed and not market:
                        raise RuntimeError(
                            "batched position market provider failed")
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
            except Exception as exc:
                failures += 1
                if len(failure_details) < 10:
                    failure_details.append({
                        "token_address": candidate.get("token_address"),
                        "source_version": candidate.get("source_version"),
                        **RobinhoodMarketClient._provider_error(exc),
                    })
        return {
            "checked": checked, "marked": marked, "unverified": unverified,
            "closed": closed,
            "partial_exits": partial_exits, "failures": failures,
            "market_batch_candidates": len(standard),
            "market_batch_requests": market_batch_requests,
            "market_batch_failed": market_batch_failed,
            "market_batch_error": market_batch_error,
            "individual_v4_quotes": len(v4),
            "failure_details": failure_details,
            "cadence": "every_learning_cycle",
        }

    def _startup_phase_telemetry(
        self, deadline: CycleDeadline, *, execute_entered: float,
        lane_registered: float,
    ) -> dict:
        """Partition shared-deadline startup into actionable phases."""
        milestones = getattr(self, "_startup_milestones", {}) or {}
        if not milestones.get("shared_deadline"):
            return {
                "shared_deadline": False,
                "total_seconds": round(
                    max(0.0, lane_registered - deadline.started), 3),
            }

        launch = deadline.started
        module_started = max(
            launch, safe_float(
                milestones.get("module_import_started"), launch))
        imports_completed = max(
            module_started, safe_float(
                milestones.get("imports_completed"), module_started))
        engine_started = max(
            imports_completed, safe_float(
                milestones.get("engine_initialization_started"),
                imports_completed))
        engine_completed = max(
            engine_started, safe_float(
                milestones.get("engine_initialization_completed"),
                engine_started))
        execute_entered = max(engine_completed, float(execute_entered))
        lane_registered = max(execute_entered, float(lane_registered))
        return {
            "shared_deadline": True,
            "python_bootstrap_seconds": round(module_started - launch, 3),
            "module_import_seconds": round(
                imports_completed - module_started, 3),
            "pre_engine_gap_seconds": round(
                engine_started - imports_completed, 3),
            "engine_initialization_seconds": round(
                engine_completed - engine_started, 3),
            "pre_execute_gap_seconds": round(
                execute_entered - engine_completed, 3),
            "lane_registration_seconds": round(
                lane_registered - execute_entered, 3),
            "total_seconds": round(lane_registered - launch, 3),
        }

    def _execute_lane(self, lane: str, budget_seconds: float, worker) -> dict:
        """Run one independently owned lane with an authoritative heartbeat."""
        inherited_deadline = safe_float(
            os.environ.get("CHAINSEER_LANE_DEADLINE_MONOTONIC"), 0.0)
        deadline = CycleDeadline(
            float(budget_seconds),
            deadline_monotonic=(
                inherited_deadline if inherited_deadline > 0 else None),
        )
        started = deadline.started
        execute_entered = time.monotonic()
        run_uuid = uuid.uuid4().hex
        self.cycle_run_uuid = run_uuid
        self._active_lane = lane
        self._active_lane_deadline = deadline
        self._rpc_gate_deadline = None
        self._rpc_priority_telemetry = {
            "admissions": 0, "deferrals": 0,
            "wait_seconds": 0.0, "rate_wait_seconds": 0.0,
            "provider_cooldown_wait_seconds": 0.0,
            "provider_rate_limits": 0,
            "last_reason": None,
        }
        stop_heartbeat = threading.Event()
        with LearningRunLock(self.root / f".{lane}_once.lock"):
            self.store.begin_run(run_uuid, budget_seconds, lane=lane)
            lane_registered = time.monotonic()
            startup_phases = self._startup_phase_telemetry(
                deadline, execute_entered=execute_entered,
                lane_registered=lane_registered)
            startup_consumed = startup_phases["total_seconds"]

            def beat() -> None:
                while not stop_heartbeat.wait(LANE_HEARTBEAT_SECONDS):
                    try:
                        self.store.heartbeat_run(run_uuid)
                    except Exception:
                        # A heartbeat is observability, never permission to
                        # continue. The supervisor's process deadline remains
                        # authoritative if SQLite is temporarily unavailable.
                        pass

            heartbeat = threading.Thread(target=beat, daemon=True)
            heartbeat.start()
            try:
                deadline.raise_if_expired(f"{lane}_startup")
                payload = worker(deadline)
                deadline.raise_if_expired(f"{lane}_completion")
                terminal_status = (
                    "deferred" if payload.get("controlled_deferral")
                    else "complete")
                summary = {
                    "schema_version": 1,
                    "timestamp": _utc_now(),
                    "lane": lane,
                    "run_id": run_uuid,
                    "status": terminal_status,
                    "deadline_seconds": float(budget_seconds),
                    "startup_consumed_seconds": round(startup_consumed, 3),
                    "startup_phases": startup_phases,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "rpc_priority": {
                        **self._rpc_priority_telemetry,
                        "wait_seconds": round(
                            safe_float(self._rpc_priority_telemetry.get(
                                "wait_seconds"), 0.0), 3),
                    },
                    "paper_only": True,
                    "live_execution_enabled": False,
                    **payload,
                }
                stop_heartbeat.set()
                heartbeat.join(timeout=1.0)
                self.store.finish_run(
                    run_uuid, terminal_status, summary=summary,
                    cursor=summary.get("cursor"),
                    backlog=summary.get("backlog"),
                )
                atomic_json_write(self.root / f"{lane}_lane_summary.json", summary)
                return summary
            except Exception as exc:
                stop_heartbeat.set()
                heartbeat.join(timeout=1.0)
                priority_deferred = isinstance(
                    exc, BackgroundRpcPriorityDeferred)
                provider_deferred = _rpc_rate_limited(exc)
                controlled_deferred = priority_deferred or provider_deferred
                terminal_status = (
                    "deferred" if controlled_deferred else (
                        "deadline_exceeded"
                        if isinstance(exc, CycleDeadlineExceeded)
                        else "failed"))
                failure = {
                    "schema_version": 1, "timestamp": _utc_now(),
                    "lane": lane, "run_id": run_uuid,
                    "status": terminal_status,
                    "error_type": type(exc).__name__, "error": str(exc)[:1000],
                    # Read the PERSISTED stage, not a local: a child that
                    # raises and one that is hard-killed must attribute the
                    # same way, or half the failures stay unattributable.
                    **self.store.lane_failure_stage(lane, run_uuid),
                    "deadline_seconds": float(budget_seconds),
                    "startup_consumed_seconds": round(startup_consumed, 3),
                    "startup_phases": startup_phases,
                    "duration_seconds": round(time.monotonic() - started, 3),
                    "controlled_deferral": controlled_deferred,
                    "deferral_stage": (
                        "background_rpc_priority"
                        if priority_deferred else (
                            "provider_rpc" if provider_deferred else None)),
                    "deferral_reason": (
                        "live_rpc_reservation"
                        if priority_deferred else (
                            "provider_rate_limited"
                            if provider_deferred else None)),
                    "rpc_priority": {
                        **self._rpc_priority_telemetry,
                        "wait_seconds": round(
                            safe_float(self._rpc_priority_telemetry.get(
                                "wait_seconds"), 0.0), 3),
                    },
                    "paper_only": True, "live_execution_enabled": False,
                }
                self.store.finish_run(
                    run_uuid, terminal_status,
                    summary=failure,
                    error=(
                        None if controlled_deferred else failure["error"]))
                atomic_json_write(self.root / f"{lane}_lane_summary.json", failure)
                if controlled_deferred:
                    return failure
                raise
            finally:
                self._active_lane = None
                self._active_lane_deadline = None
                self._rpc_gate_deadline = None

    def run_marks_lane(
        self, *, budget_seconds: float = MARKS_LANE_BUDGET_SECONDS,
        now: float | None = None,
    ) -> dict:
        """Mark open positions, and nothing else.

        Split out of the live lane so a slow market mark cannot delay
        blockchain-event freshness. Its own budget, cursor-free by nature,
        and its own completion rate -- which is what makes the two
        independently sizeable instead of jointly failing.
        """
        observed_at = time.time() if now is None else float(now)

        def work(deadline: CycleDeadline) -> dict:
            stage = time.monotonic()
            with self._rpc_deadline(deadline):
                positions = self.evaluate_open_positions(
                    observed_at, deadline=deadline)
            return {
                "position_evaluations": positions,
                "stage_timings_seconds": {
                    "position_marks": round(time.monotonic() - stage, 3)},
            }

        return self._execute_lane("marks", budget_seconds, work)

    def run_evidence_lane(
        self, *, budget_seconds: float = EVIDENCE_LANE_BUDGET_SECONDS,
        now: float | None = None,
        entry_quote_limit: int = EVIDENCE_ENTRY_QUOTE_LIMIT,
        event_outcome_limit: int = EVIDENCE_EVENT_OUTCOME_LIMIT,
        observation_outcome_limit: int = EVIDENCE_OBSERVATION_OUTCOME_LIMIT,
    ) -> dict:
        """Price and follow prospective evidence, never execute positions.

        The split-lane supervisor orphaned these stages in the legacy
        ``run_once`` path: observations continued for four days while the
        promotion event ledger stopped at 14 rows.  Fresh event identity is
        now committed by the live lane; this independently killable lane owns
        every remote quote and outcome call so a provider stall cannot spend
        the live lane's 25-second freshness budget.
        """
        observed_at = time.time() if now is None else float(now)

        def work(deadline: CycleDeadline) -> dict:
            # Each durable stream receives an independent bounded slice. A
            # single shared remote deadline let entry quotes consume 85.3s,
            # after which every later stream began at zero. Recompute from the
            # parent before each stage so unused time remains available while
            # the completion reserve can never be allocated to remote work.
            def next_stage_deadline() -> CycleDeadline:
                return CycleDeadline(max(
                    0.0,
                    min(
                        EVIDENCE_STAGE_MAX_SECONDS,
                        deadline.remaining()
                        - EVIDENCE_COMPLETION_RESERVE_SECONDS,
                    ),
                ))

            timings: dict[str, float] = {}

            stage = time.monotonic()
            stage_deadline = next_stage_deadline()
            if stage_deadline.expired():
                entry_quotes = {
                    "entry_quotes_selected": 0,
                    "entry_quotes_verified": 0, "quote_failures": 0,
                    "entry_quotes_deferred": 0,
                    "admission_deferred": True,
                    "deferred_count_known": False,
                    "reason": "completion_reserve_reached",
                    "limit": max(0, int(entry_quote_limit)),
                }
            else:
                self.store.mark_lane_stage(
                    "evidence", "entry_quotes", run_id=self.cycle_run_uuid,
                    remaining=stage_deadline.remaining(),
                    completed=dict(timings))
                with self._rpc_deadline(stage_deadline):
                    entry_quotes = self.quote_pending_flow_evidence(
                        limit=max(0, int(entry_quote_limit)),
                        deadline=stage_deadline)
            timings["entry_quotes_seconds"] = round(
                time.monotonic() - stage, 3)

            stage = time.monotonic()
            stage_deadline = next_stage_deadline()
            if stage_deadline.expired():
                observation_quotes = {
                    "selected": 0, "quoted": 0, "decision_quotes": 0,
                    "quote_failures": 0, "deferred": 0,
                    "admission_deferred": True,
                    "deferred_count_known": False,
                    "reason": "completion_reserve_reached",
                }
            else:
                self.store.mark_lane_stage(
                    "evidence", "observation_quotes",
                    run_id=self.cycle_run_uuid,
                    remaining=stage_deadline.remaining(),
                    completed=dict(timings))
                with self._rpc_deadline(stage_deadline):
                    observation_quotes = (
                        self.quote_pending_flow_observations(
                            limit=EVIDENCE_OBSERVATION_QUOTE_LIMIT,
                            deadline=stage_deadline)
                    )
            timings["observation_quotes_seconds"] = round(
                time.monotonic() - stage, 3)

            # The v3 cohort consumes observation outcomes. Run them before
            # the legacy event outcome stream so the research loop cannot be
            # starved by a perpetual event backlog.
            stage = time.monotonic()
            stage_deadline = next_stage_deadline()
            if stage_deadline.remaining() < (
                    EVIDENCE_OUTCOME_ITEM_RESERVE_SECONDS):
                observation_outcomes = {
                    "due": 0, "resolved_this_cycle": 0,
                    "non_exitable_this_cycle": 0, "failures": 0,
                    "deferred": 0, "admission_deferred": True,
                    "deferred_count_known": False,
                    "reason": "completion_reserve_reached",
                    "limit": max(0, int(observation_outcome_limit)),
                }
            else:
                self.store.mark_lane_stage(
                    "evidence", "observation_outcomes",
                    run_id=self.cycle_run_uuid,
                    remaining=stage_deadline.remaining(),
                    completed=dict(timings))
                with self._rpc_deadline(stage_deadline):
                    observation_outcomes = (
                        self.observe_flow_observation_outcomes(
                            observed_at,
                            limit=max(0, int(observation_outcome_limit)),
                            deadline=stage_deadline,
                        )
                    )
            timings["observation_outcomes_seconds"] = round(
                time.monotonic() - stage, 3)

            stage = time.monotonic()
            stage_deadline = next_stage_deadline()
            event_outcomes: dict
            if stage_deadline.remaining() < (
                    EVIDENCE_OUTCOME_ITEM_RESERVE_SECONDS):
                # Never run the due-row selector without one item's reserve.
                # It alone measured 5.1s on the production corpus.
                event_outcomes = {
                    "selected": 0, "observed": 0, "non_exitable": 0,
                    "deferred": 0, "admission_deferred": True,
                    "deferred_count_known": False,
                    "reason": "completion_reserve_reached",
                }
            else:
                self.store.mark_lane_stage(
                    "evidence", "event_outcomes",
                    run_id=self.cycle_run_uuid,
                    remaining=stage_deadline.remaining(),
                    completed=dict(timings))
                try:
                    with self._rpc_deadline(stage_deadline):
                        outcome_head = int(self.rpc.get_block_number())
                        event_outcomes = self.observe_flow_evidence_outcomes(
                            observed_at, outcome_head,
                            limit=max(0, int(event_outcome_limit)),
                            deadline=stage_deadline,
                        )
                    event_outcomes["head_block"] = outcome_head
                except Exception as exc:
                    if not (
                        isinstance(exc, BackgroundRpcPriorityDeferred)
                        or _rpc_rate_limited(exc)
                    ):
                        raise
                    event_outcomes = {
                        "selected": 0, "observed": 0, "non_exitable": 0,
                        "deferred": 0, "admission_deferred": True,
                        "deferred_count_known": False,
                        "reason": (
                            "provider_rate_limited"
                            if _rpc_rate_limited(exc)
                            else "stage_rpc_budget_exhausted"),
                    }
            timings["event_outcomes_seconds"] = round(
                time.monotonic() - stage, 3)
            return {
                "entry_quotes": entry_quotes,
                "observation_quotes": observation_quotes,
                "event_outcomes": event_outcomes,
                "observation_outcomes": observation_outcomes,
                "stage_timings_seconds": timings,
                "completion_reserve_seconds":
                    EVIDENCE_COMPLETION_RESERVE_SECONDS,
                "stage_max_seconds": EVIDENCE_STAGE_MAX_SECONDS,
                "paper_only": True,
                "live_execution_enabled": False,
                "timechain_writer": None,
                "source": "prospective_flow_evidence",
            }

        return self._execute_lane("evidence", budget_seconds, work)

    def run_live_lane(
        self, *, budget_seconds: float = LIVE_LANE_BUDGET_SECONDS,
        now: float | None = None,
    ) -> dict:
        """Critical path: marks, current-head ingestion, identity and quotes.

        Historical discovery is intentionally absent. When the cursor is too
        old, the skipped prefix is committed to flow_backfill_queue and this
        lane re-anchors to the current head instead of paying historical debt.
        """
        observed_at = time.time() if now is None else float(now)

        def work(deadline: CycleDeadline) -> dict:
            timings: dict[str, float] = {}
            decision_tail_circuit = self.begin_decision_tail_attempt()
            # Position marking now runs in its own lane (run_marks_lane).
            # Leaving it here would keep an external price API on the path
            # that must stay near the chain head.
            positions = {"delegated_to": "marks_lane"}
            # Six NON-OVERLAPPING stage clocks. The old `seal_and_fresh_quote`
            # clock started before sealing and stopped after classification,
            # so sealing's overrun was booked to whichever stage followed it
            # and no single number could be compared against an estimate.
            # Each boundary below starts exactly where the previous ended.
            stage = time.monotonic()
            ingestion_model = self.ingestion_cost_model()
            ingestion_tail_reserve = self.ingestion_tail_reserve()
            # Also bounded by FRESHNESS, with a floor.
            #
            # Every decision-lag breach is ingestion-dominated: measured on
            # the 12 breaches in 338 cycles, ingestion ran 8.3-14.2s while
            # sealing was 0.03-4.8s and classification 1.6-5.0s. Median
            # ingestion on passing cycles is 5.4s. The cycle deadline is 25s,
            # so a pass can finish comfortably inside it and still be far too
            # stale to act on -- the deadline was never the binding limit.
            #
            # Derived from the OBSERVED downstream cost (3.2s median), not
            # from the reserve. An earlier attempt subtracted the full 8.73s
            # tail reserve from a 5.85s budget, got zero, and would have
            # deferred every cycle forever -- the reserve is what the lane
            # sets aside, not what it spends.
            #
            # The floor is what makes this safe: ingestion can be squeezed
            # but never to nothing, so a bad estimate degrades scan width
            # instead of stopping the lane.
            freshness_seconds = (
                FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
                / max(1.0, FLOW_OBSERVED_BLOCKS_PER_SECOND))
            downstream_observed = max(
                0.0,
                self.live_planning_seal_cost_model()[
                    "downstream_reserve_p95"],
            )
            # The floor guards the FRESHNESS term only. Applying it to the
            # whole expression let a cycle with no time left still claim 3s
            # of ingestion, which removed the deadline's ability to force a
            # deferral -- the lane must still be able to decline when it is
            # genuinely out of budget, not merely out of freshness.
            freshness_allowance = max(
                LIVE_LANE_MINIMUM_INGESTION_SECONDS,
                freshness_seconds - downstream_observed)
            ingestion_available = max(
                0.0,
                min(deadline.remaining() - ingestion_tail_reserve,
                    freshness_allowance))
            predicted_ingestion = max(
                0.05, ingestion_model["p95_seconds"])
            scan_fraction = min(
                1.0, max(0.15, ingestion_available / predicted_ingestion))
            admitted_scan_blocks = max(
                1, min(
                    LIVE_LANE_SCAN_BLOCKS,
                    int(LIVE_LANE_SCAN_BLOCKS * scan_fraction)))
            ingestion_admission = {
                "remaining_seconds": round(deadline.remaining(), 3),
                "tail_reserve_seconds": ingestion_tail_reserve,
                "available_seconds": round(ingestion_available, 3),
                "estimated_p95_seconds": round(predicted_ingestion, 3),
                "configured_scan_blocks": LIVE_LANE_SCAN_BLOCKS,
                "admitted_scan_blocks": admitted_scan_blocks,
                "scan_fraction": round(scan_fraction, 4),
            }
            self.store.mark_lane_stage(
                    "live", "ingestion", run_id=self.cycle_run_uuid,
                    remaining=deadline.remaining(), completed=dict(timings),
                    detail={"admission": ingestion_admission})
            if ingestion_available <= 0:
                near_head = {
                    "supported": True, "scanned": False,
                    "reason": "insufficient_ingestion_headroom",
                    "controlled_deferral": True,
                }
                timings["head_ingestion_and_identity_seconds"] = 0.0
                return {
                    "controlled_deferral": True,
                    "deferral_stage": "ingestion_admission",
                    "deferral_reason": near_head["reason"],
                    "ingestion_admission": ingestion_admission,
                    "position_evaluations": positions,
                    "near_head_flow": near_head,
                    "observation_seal": {}, "classification": {},
                    "stage_timings_seconds": timings,
                    "cursor": read_json(
                        self.root / "live_lane_cursor.json", {}) or {},
                    "backlog": self.store.backfill_backlog(),
                    "no_historical_scanning": True,
                }

            # Give ingestion an EARLIER child deadline.  A stalled getLogs
            # call is interrupted while the root deadline still owns enough
            # time to write an authoritative deferred result.  The cursor is
            # committed only at the end of near_head_flow_pass, so retrying
            # the partial range is idempotent and no blocks disappear.
            ingestion_deadline = CycleDeadline(ingestion_available)
            try:
                with self._rpc_deadline(
                    ingestion_deadline, retry_attempts=0,
                ):
                    near_head = self.near_head_flow_pass(
                        deadline=ingestion_deadline,
                        cursor_name="live_lane_cursor.json",
                        max_scan_blocks=admitted_scan_blocks,
                        enrichment_limit=LIVE_LANE_ENRICHMENT_LIMIT,
                        enrichment_budget_seconds=min(
                            LIVE_LANE_ENRICHMENT_BUDGET_SECONDS,
                            max(0.0, ingestion_available / 2.0)),
                    )
            except CycleDeadlineExceeded as exc:
                elapsed = time.monotonic() - stage
                self.record_ingestion_cost(elapsed, censored=True)
                timings["head_ingestion_and_identity_seconds"] = round(
                    elapsed, 3)
                return {
                    "controlled_deferral": True,
                    "deferral_stage": "ingestion",
                    "deferral_reason": str(exc),
                    "ingestion_admission": ingestion_admission,
                    "position_evaluations": positions,
                    "near_head_flow": {
                        "supported": True, "scanned": False,
                        "reason": str(exc), "controlled_deferral": True,
                    },
                    "observation_seal": {}, "classification": {},
                    "stage_timings_seconds": timings,
                    "cursor": read_json(
                        self.root / "live_lane_cursor.json", {}) or {},
                    "backlog": self.store.backfill_backlog(),
                    "no_historical_scanning": True,
                }
            ingestion_elapsed = time.monotonic() - stage
            timings["head_ingestion_and_identity_seconds"] = round(
                ingestion_elapsed, 3)
            if ingestion_deadline.expired() and not near_head.get("scanned"):
                self.record_ingestion_cost(ingestion_elapsed, censored=True)
                return {
                    "controlled_deferral": True,
                    "deferral_stage": "ingestion",
                    "deferral_reason": (
                        near_head.get("reason") or "ingestion_deadline"),
                    "ingestion_admission": ingestion_admission,
                    "position_evaluations": positions,
                    "near_head_flow": {
                        **near_head, "controlled_deferral": True},
                    "observation_seal": {}, "classification": {},
                    "stage_timings_seconds": timings,
                    "cursor": read_json(
                        self.root / "live_lane_cursor.json", {}) or {},
                    "backlog": self.store.backfill_backlog(),
                    "no_historical_scanning": True,
                }
            if near_head.get("scanned"):
                self.record_ingestion_cost(ingestion_elapsed)
            # Renamed: this stage captures a fresh block-pinned quote and
            # persists an immutable SQLite observation. It is NOT Timechain
            # sealing -- that happens asynchronously in the analysis lane.
            self.store.mark_lane_stage(
                    "live", "fresh_quote_and_observation",
                    run_id=self.cycle_run_uuid,
                    remaining=deadline.remaining(), completed=dict(timings))
            if not near_head.get("scanned"):
                return {
                    "position_evaluations": positions,
                    "ingestion_admission": ingestion_admission,
                    "near_head_flow": near_head,
                    "observation_seal": {}, "classification": {},
                    "stage_timings_seconds": timings,
                    "cursor": read_json(
                        self.root / "live_lane_cursor.json", {}) or {},
                    "backlog": self.store.backfill_backlog(),
                    "no_historical_scanning": True,
                }

            stage = time.monotonic()
            touched = list(near_head.get("touched_pool_ids") or [])
            observation_model = self.live_planning_seal_cost_model()
            observation_head = int(near_head.get("to_block") or 0)
            post_ingest_head = (
                int(near_head["head_block_after"])
                if near_head.get("head_block_after") is not None else None)
            freshness_admission = self.observation_freshness_admission(
                observation_head=observation_head,
                post_ingest_head=post_ingest_head,
                requested=LIVE_LANE_OBSERVATION_LIMIT,
            )
            observation_minimum = (
                max(LIVE_LANE_DECISION_RESERVE_SECONDS,
                    observation_model["downstream_reserve_p95"])
                + observation_model["fixed_observation_cost_p95"]
                + observation_model["queue_settlement_p95"])
            if deadline.remaining() <= observation_minimum:
                # Selection itself is a measured fixed cost.  Starting it
                # when that cost cannot fit caused the second failed soak
                # attempt to enter classification with 0.313s left even
                # though sealing correctly admitted zero remote windows.
                # Fresh rows remain discoverable through NOT EXISTS and
                # queued rows remain in flow_seal_queue, so skipping the
                # selection query loses nothing.
                observation = {
                    "windows_considered": 0,
                    "sealed_this_cycle": 0,
                    "observation_ids": [],
                    "controlled_deferral": True,
                    "reason": "insufficient_observation_headroom",
                    "remaining_seconds": round(deadline.remaining(), 3),
                    "required_seconds": round(observation_minimum, 3),
                }
            else:
                try:
                    with self._rpc_deadline(deadline):
                        observation = self.seal_near_head_observations(
                            observation_head, observed_at,
                            pool_ids=touched, deadline=deadline,
                            limit=freshness_admission["admitted"],
                            reserve_seconds=LIVE_LANE_DECISION_RESERVE_SECONDS,
                            # Historical durable work must not consume the
                            # freshness budget. It is drained by backfill.
                            queue_drain_limit=0,
                            include_fresh=True,
                            stage_lane="live",
                            defer_quotes=True,
                        )
                except CycleDeadlineExceeded:
                    # CENSORED sample: this attempt was killed inside sealing.
                    # Its elapsed time is a lower bound on what the admitted
                    # windows actually cost; folding it in (upward-only) keeps
                    # admission learning from failures instead of successes only.
                    self.record_seal_cost_censored(time.monotonic() - stage)
                    raise
            observation["freshness_admission"] = freshness_admission
            timings["fresh_quote_and_observation_seconds"] = round(
                time.monotonic() - stage, 3)
            stage = time.monotonic()
            self.store.mark_lane_stage(
                "live", "decision_head", run_id=self.cycle_run_uuid,
                remaining=deadline.remaining(), completed=dict(timings))

            def defer_decision_head(
                reason: str, *, infrastructure_indeterminate: bool = False,
                error: str | None = None,
            ) -> dict:
                timings["decision_head_seconds"] = round(
                    time.monotonic() - stage, 3)
                return {
                    "controlled_deferral": True,
                    "deferral_stage": "decision_head",
                    "deferral_reason": reason,
                    "infrastructure_indeterminate":
                        bool(infrastructure_indeterminate),
                    "infrastructure_error": error,
                    "position_evaluations": positions,
                    "ingestion_admission": ingestion_admission,
                    "near_head_flow": near_head,
                    "observation_seal": observation,
                    "classification": {
                        "scoped_rows_selected": 0,
                        "scoped_rows_processed": 0,
                        "scoped_rows_deferred": len(
                            observation.get("observation_ids") or []),
                    },
                    "stage_timings_seconds": timings,
                    "cursor": read_json(
                        self.root / "live_lane_cursor.json", {}) or {},
                    "backlog": self.store.backfill_backlog(),
                    "no_historical_scanning": True,
                }

            decision_head_remaining = deadline.remaining()
            decision_admission = self.decision_head_admission(
                len(observation.get("observation_ids") or []),
                decision_head_remaining,
            )
            observation["decision_head_admission"] = decision_admission
            if not decision_admission["admitted"]:
                return defer_decision_head(
                    "insufficient_decision_head_headroom")
            downstream_required = max(
                0.0,
                safe_float(decision_admission.get("required_seconds"))
                - DECISION_HEAD_RPC_RESERVE_SECONDS,
            )
            try:
                decision_head, decision_rpc = (
                    self.read_authoritative_decision_head(
                        deadline,
                        downstream_required_seconds=downstream_required,
                        allow_rate_limit_retry=not bool(
                            observation.get("observation_ids")),
                    )
                )
            except RPCError as exc:
                # A transport failure says nothing about token safety. The
                # observation is durable; leave it unclassified for a later
                # authoritative decision rather than guessing at the head.
                observation["decision_head_attempts"] = safe_int(
                    getattr(exc, "decision_head_attempts", 1), 1)
                observation["decision_head_rate_limit_retries"] = safe_int(
                    getattr(
                        exc, "decision_head_rate_limit_retries", 0), 0)
                return defer_decision_head(
                    "decision_head_infrastructure_indeterminate",
                    infrastructure_indeterminate=True, error=str(exc))
            observation["decision_head_attempts"] = decision_rpc["attempts"]
            observation["decision_head_rate_limit_retries"] = (
                decision_rpc["rate_limit_retries"])
            timings["decision_head_seconds"] = round(
                time.monotonic() - stage, 3)
            post_ingest_tail_blocks = max(
                0, decision_head - int(post_ingest_head or observation_head))
            observation["decision_tail_blocks"] = post_ingest_tail_blocks
            decision_head_lag_blocks = max(
                0, decision_head - int(near_head.get("to_block") or 0))
            observation["decision_head_lag_blocks"] = (
                decision_head_lag_blocks)
            research_only_stale = bool(
                observation.get("observation_ids")
                and decision_head_lag_blocks
                    > FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS
            )
            observation["research_only_stale"] = research_only_stale
            if observation.get("observation_ids"):
                decision_tail_model = self.record_decision_tail_blocks(
                    len(observation["observation_ids"]),
                    post_ingest_tail_blocks,
                )
                observation["decision_tail_model_after"] = {
                    "epoch": decision_tail_model["epoch"],
                    "quantile": decision_tail_model["quantile"],
                    "reserves": decision_tail_model["reserves"],
                    "sample_counts": decision_tail_model["sample_counts"],
                    "circuit": decision_tail_model["circuit"],
                }
            stage = time.monotonic()
            self.store.mark_lane_stage(
                "live", "flow_evidence_capture",
                run_id=self.cycle_run_uuid,
                remaining=deadline.remaining(), completed=dict(timings),
                detail={"touched_pools": len(touched)},
            )
            # Prospective identity belongs at decision time.  Only bounded
            # SQLite metadata for pools touched by this near-head pass is
            # committed here: no Timechain call and no remote quote/outcome
            # call is allowed on the live path.  The evidence lane prices and
            # follows these durable events asynchronously.
            flow_evidence_capture = self.store.capture_flow_signal_events(
                decision_head, time.time(),
                ingest_head_block=int(near_head.get("to_block") or 0) or None,
                pool_ids=touched,
            )
            flow_evidence_capture["remote_work_delegated_to"] = "evidence_lane"
            timings["flow_evidence_capture_seconds"] = round(
                time.monotonic() - stage, 3)
            stage = time.monotonic()
            self.store.mark_lane_stage(
                "live", "classification", run_id=self.cycle_run_uuid,
                remaining=deadline.remaining(), completed=dict(timings))
            # Admission controller: classify only what safely fits the
            # remaining budget (p95 per-observation estimate minus a
            # completion/ledger reserve). The rest is durably deferred,
            # never dropped.
            remaining_at_admission = deadline.remaining()
            admission = self.classification_admission(
                len(observation.get("observation_ids") or []),
                remaining_at_admission,
            )
            timings["classification_remaining_at_admission"] = round(
                remaining_at_admission, 3)
            classification = self.classify_sealed_observations(
                decision_head,
                observation_ids=list(observation.get("observation_ids") or []),
                deadline=deadline,
                admission_limit=admission["admitted"],
                allow_remote_quotes=False,
            )
            classification["admission_decision"] = admission
            timings["classification_seconds"] = round(
                time.monotonic() - stage, 3)
            stage = time.monotonic()
            self.ledger.append("robinhood_live_lane", {
                "run_id": self.cycle_run_uuid if hasattr(self, "cycle_run_uuid") else None,
                "near_head_flow": {
                    key: value for key, value in near_head.items()
                    if key not in {"touched_pool_ids"}
                },
                "observation_seal": observation,
                "classification": classification,
                "paper_only": True,
            })
            timings["ledger_append_seconds"] = round(
                time.monotonic() - stage, 3)
            timings["seal_stage_headroom_seconds"] = round(
                deadline.remaining(), 3)
            # DEPRECATED total: now DERIVED as the sum of the five disjoint
            # stage clocks rather than measured across them. Kept because
            # dashboards and alerts still read it; it can never exceed the
            # budget by double-counting again.
            timings["seal_and_fresh_quote"] = round(sum([
                timings["head_ingestion_and_identity_seconds"],
                timings["fresh_quote_and_observation_seconds"],
                timings["decision_head_seconds"],
                timings["flow_evidence_capture_seconds"],
                timings["classification_seconds"],
                timings["ledger_append_seconds"],
            ]), 3)
            timings["seal_and_fresh_quote_is_derived_total"] = True
            # Fold THIS cycle's actual downstream tail into the reserve
            # estimate the next cycle's admission will subtract.
            self.record_downstream_reserve(
                timings["decision_head_seconds"]
                + timings["flow_evidence_capture_seconds"]
                + timings["classification_seconds"]
                + timings["ledger_append_seconds"])
            quote_delay = timings["seal_and_fresh_quote"]
            return {
                "controlled_deferral": research_only_stale,
                "deferral_stage": (
                    "decision_freshness" if research_only_stale else None),
                "deferral_reason": (
                    "decision_head_stale_research_only"
                    if research_only_stale else None),
                "position_evaluations": positions,
                "ingestion_admission": ingestion_admission,
                "near_head_flow": near_head,
                "observation_seal": observation,
                "classification": classification,
                "flow_evidence_capture": flow_evidence_capture,
                "ingestion_to_fresh_quote_seconds": quote_delay,
                "stage_timings_seconds": timings,
                "cursor": read_json(
                    self.root / "live_lane_cursor.json", {}) or {},
                "backlog": self.store.backfill_backlog(),
                "no_historical_scanning": True,
            }

        return self._execute_lane("live", budget_seconds, work)

    def _analyze_candidates(
        self, now: float, limit: int, deadline: CycleDeadline,
    ) -> dict:
        analyses = failures = entries = producer_sealed = producer_failures = 0
        deferred = momentum = flow_shadow = 0
        queue_ages: list[float] = []
        for candidate in self.store.pending_analysis(limit):
            if deadline.remaining() < ANALYSIS_START_RESERVE_SECONDS:
                deferred += 1
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
                    candidate["token_address"], seal=False,
                    defer_cognition=True,
                )
                if report.get("error"):
                    raise RuntimeError(report["error"])
                analysis = dict(report.get("analysis") or {})
                if candidate.get("source_version") == SOURCE_V4:
                    market = self.v4_market.snapshot(candidate)
                    stops = list(analysis.get("hard_stop_overrides") or [])
                    if str(candidate.get("hooks_address") or ZERO_ADDRESS).lower() != ZERO_ADDRESS:
                        stops.append({
                            "code": "V4_HOOK_UNAUDITED", "severity": "High",
                            "reason": "The V4 pool uses an unaudited hook",
                            "action": "AVOID",
                        })
                    if not market.get("current_state_verified"):
                        stops.append({
                            "code": "V4_MARKET_STATE_UNVERIFIED", "severity": "High",
                            "reason": "Current V4 liquidity and price could not be verified",
                            "action": "AVOID",
                        })
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
                        report, candidate["token_address"],
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
                    producer_sealed += memory.get("status") == "sealed"
                except Exception:
                    producer_failures += 1
                momentum += priority_reason == "liquid_momentum"
                flow_shadow += priority_reason == "flow_shadow_priority"
                if queue_age is not None:
                    queue_ages.append(queue_age)
                latest = self.store.candidate(candidate["token_address"])
                entry = self.guarded_paper_entry(
                    latest, market, run_id=self.cycle_run_uuid,
                    priority_reason=priority_reason,
                )
                if entry.get("entered"):
                    entries += 1
                analyses += 1
            except Exception as exc:
                failures += 1
                self.store.record_analysis_failure(
                    candidate["token_address"], str(exc))
        return {
            "analyses": analyses, "analysis_failures": failures,
            "paper_entries": entries, "momentum_analyses": momentum,
            "flow_shadow_analyses": flow_shadow,
            "producer_analyses_sealed": producer_sealed,
            "producer_failures": producer_failures,
            "deferred_for_deadline": deferred,
            "maximum_queue_age_seconds": (
                round(max(queue_ages), 3) if queue_ages else None),
        }

    def run_analysis_lane(
        self, *, budget_seconds: float = ANALYSIS_LANE_BUDGET_SECONDS,
        analysis_limit: int = DEFAULT_ANALYSIS_LIMIT,
        outcome_limit: int = DEFAULT_OUTCOME_LIMIT,
        outcome_recovery_limit: int = DEFAULT_OUTCOME_RECOVERY_LIMIT,
        market_recheck_limit: int = DEFAULT_MARKET_RECHECK_LIMIT,
        now: float | None = None,
    ) -> dict:
        """Outcome and expensive-analysis lane; the only Timechain writer."""
        observed_at = time.time() if now is None else float(now)

        def work(deadline: CycleDeadline) -> dict:
            timings: dict[str, float] = {}

            def _mark_analysis_stage(name: str) -> None:
                """Attribution is telemetry: it may never break the lane.

                Every one of 76 deadline failures reported failure_stage
                None, so this lane could not say where it died. Recording it
                must not become a new way to die -- a store without the
                method simply goes unattributed, exactly as before.
                """
                marker = getattr(self.store, "mark_lane_stage", None)
                if marker is None:
                    return
                try:
                    marker("analysis", name,
                           run_id=getattr(self, "cycle_run_uuid", None),
                           remaining=deadline.remaining(),
                           completed=dict(timings))
                except Exception:
                    pass

            # Integrity refresh owns the first reservation in this lane.
            # Entry-capable rechecks and analyses must never run ahead of a
            # due certificate refresh and consume the budget it requires.
            certificate_refresh = None
            certificate = load_integrity_certificate(self.root)
            cert_age = time.time() - safe_float(
                certificate.get("published_epoch"), 0.0)
            cert_max_age = safe_float(
                certificate.get("expires_at"), time.time() + 1.0
            ) - safe_float(
                certificate.get("published_epoch"), time.time())
            needs_refresh = (
                not certificate
                or cert_age > max(0.0, cert_max_age * 0.5))
            if self.timechain_recorder is not None and needs_refresh:
                stage = time.monotonic()
                _mark_analysis_stage("certificate_refresh")
                try:
                    certificate_refresh = self.publish_integrity_certificate()
                except Exception as exc:
                    # Analysis may continue, but any exposure-increasing
                    # action remains fail-closed on the stale certificate.
                    certificate_refresh = {
                        "published": False, "reason": str(exc)[:200]}
                timings["certificate_refresh"] = round(
                    time.monotonic() - stage, 3)
                deadline.raise_if_expired("certificate_refresh")
            stage = time.monotonic()
            with self._rpc_deadline(deadline):
                # Bounded by what outcomes may spend, not by the whole lane
                # budget. Handing it deadline.remaining() let a slow pass
                # consume everything and leave the following stages to be
                # killed instead of run -- 76 of the last 200 analysis runs
                # ended deadline_exceeded, none of them with an attributable
                # stage because this lane recorded none.
                _mark_analysis_stage("outcomes")
                outcomes = self.observe_outcomes(
                    observed_at, outcome_limit, outcome_recovery_limit,
                    deadline_monotonic=(
                        time.monotonic()
                        + max(0.0, deadline.remaining()
                              - ANALYSIS_DOWNSTREAM_RESERVE_SECONDS)),
                )
            timings["outcomes"] = round(time.monotonic() - stage, 3)
            deadline.raise_if_expired("outcomes")
            stage = time.monotonic()
            with self._rpc_deadline(deadline):
                rechecks = self.recheck_executable_markets(
                    observed_at, market_recheck_limit)
            timings["market_rechecks"] = round(time.monotonic() - stage, 3)
            deadline.raise_if_expired("market_rechecks")
            stage = time.monotonic()
            with self._rpc_deadline(deadline):
                analyses = self._analyze_candidates(
                    observed_at, analysis_limit, deadline)
            timings["analyses"] = round(time.monotonic() - stage, 3)
            stage = time.monotonic()
            deferred_seals = self.drain_deferred_seals(deadline=deadline)
            timings["deferred_seals"] = round(time.monotonic() - stage, 3)
            # The expensive full-cohort aggregation lives HERE (analysis /
            # dashboard lane), never in classification's live path: this
            # seeds/reconciles the incremental counters the live cycle reads.
            try:
                cohort_summary = self._classification_counters(seed=True)
                cohort_summary["reconciled_at"] = _utc_now()
            except Exception as exc:
                cohort_summary = {"error": str(exc)[:200]}
            learning = self.store.summary()
            return {
                "outcomes": outcomes, "market_rechecks": rechecks,
                "candidate_analyses": analyses,
                "deferred_seals": deferred_seals,
                "certificate_refresh": certificate_refresh,
                "classification_cohort": cohort_summary,
                "stage_timings_seconds": timings,
                "cursor": {"analysis_queue": "oldest_fairness_with_priorities"},
                "backlog": {
                    "pending_analysis": safe_int(
                        (learning.get("candidates") or {}).get("pending"), 0),
                },
                "timechain_writer": "analysis_lane_only",
            }

        return self._execute_lane("analysis", budget_seconds, work)

    def backfill_rpc_chunk_plan(self, requested_limit: int) -> dict:
        """Return the durable next-cycle range, bounded by policy."""
        configured = min(
            BACKFILL_GAP_CHUNK_BLOCKS, max(1, int(requested_limit)))
        state = self.store.scheduler_state(BACKFILL_RPC_CHUNK_STATE_KEY)
        epoch_matches = safe_int(state.get("epoch"), 0) == (
            BACKFILL_RPC_CHUNK_MODEL_EPOCH)
        revision_matches = str(state.get("revision") or "") == CODE_REVISION
        state_usable = epoch_matches and revision_matches
        initial = min(configured, BACKFILL_GAP_INITIAL_CHUNK_BLOCKS)
        stored = safe_int(
            state.get("next_chunk_blocks") if state_usable else None,
            initial,
        )
        minimum = min(configured, BACKFILL_GAP_MINIMUM_CHUNK_BLOCKS)
        stable = safe_int(
            state.get("stable_chunk_blocks") if state_usable else None, 0)
        stable = max(0, min(configured, stable))
        success_streak = max(0, safe_int(
            state.get("success_streak") if state_usable else None, 0))
        return {
            "epoch": BACKFILL_RPC_CHUNK_MODEL_EPOCH,
            "state_revision": state.get("revision") if epoch_matches else None,
            "state_revision_matches": revision_matches,
            "configured_chunk_blocks": configured,
            "chunk_blocks": max(minimum, min(configured, stored)),
            "minimum_chunk_blocks": minimum,
            "stable_chunk_blocks": stable,
            "success_streak": success_streak,
            "successes_before_probe":
                BACKFILL_RPC_SUCCESSES_BEFORE_PROBE,
            "probe_step_blocks": BACKFILL_RPC_PROBE_STEP_BLOCKS,
            "probe_pending": bool(stable and stored > stable),
            "previous_result": state.get("previous_result")
                if state_usable else None,
        }

    def record_backfill_rpc_chunk_result(
        self, attempted_blocks: int, *, error: BaseException | str | None,
    ) -> dict:
        """Adapt the next cycle using a durable stable-size hysteresis."""
        configured = BACKFILL_GAP_CHUNK_BLOCKS
        attempted = max(1, min(
            configured, int(attempted_blocks)))
        minimum = min(
            configured,
            BACKFILL_GAP_MINIMUM_CHUNK_BLOCKS,
        )
        state = self.store.scheduler_state(BACKFILL_RPC_CHUNK_STATE_KEY)
        epoch_matches = safe_int(state.get("epoch"), 0) == (
            BACKFILL_RPC_CHUNK_MODEL_EPOCH)
        revision_matches = str(state.get("revision") or "") == CODE_REVISION
        state_usable = epoch_matches and revision_matches
        stable = max(0, min(configured, safe_int(
            state.get("stable_chunk_blocks") if state_usable else None, 0)))
        success_streak = max(0, safe_int(
            state.get("success_streak") if state_usable else None, 0))
        probe_pending = bool(stable and attempted > stable)
        if error is None:
            # A successful probe becomes the new stable size. Ordinary
            # successes stay at that size until a real streak earns exactly
            # one additive probe; one sparse range can no longer double the
            # next request back into a repeatedly failing operating point.
            if attempted != stable:
                stable = attempted
                success_streak = 1
                result = "probe_success" if probe_pending else "success"
            else:
                success_streak += 1
                result = "success"
            if (
                stable < configured
                and success_streak >= BACKFILL_RPC_SUCCESSES_BEFORE_PROBE
            ):
                next_blocks = min(
                    configured, stable + BACKFILL_RPC_PROBE_STEP_BLOCKS)
                success_streak = 0
            else:
                next_blocks = stable
        elif _rpc_rate_limited(error):
            # 429 is about request rate, not range size. Keep the range and
            # let the scheduled cadence provide the cooldown.
            next_blocks = attempted
            result = "provider_rate_limited"
        else:
            # A log-query timeout may be range-density dependent. A failed
            # upward probe returns to the last proven size. If the stable size
            # itself fails on a denser range, step down additively and require
            # new success evidence before probing upward again.
            if 0 < stable < attempted:
                next_blocks = stable
            else:
                next_blocks = max(
                    minimum, attempted - BACKFILL_RPC_PROBE_STEP_BLOCKS * 2)
                stable = 0
            success_streak = 0
            result = "provider_timeout"
        payload = {
            "epoch": BACKFILL_RPC_CHUNK_MODEL_EPOCH,
            "revision": CODE_REVISION,
            "configured_chunk_blocks": configured,
            "attempted_chunk_blocks": attempted,
            "next_chunk_blocks": next_blocks,
            "stable_chunk_blocks": stable,
            "success_streak": success_streak,
            "successes_before_probe":
                BACKFILL_RPC_SUCCESSES_BEFORE_PROBE,
            "probe_step_blocks": BACKFILL_RPC_PROBE_STEP_BLOCKS,
            "probe_pending": bool(stable and next_blocks > stable),
            "previous_result": result,
            "updated_at": _utc_now(),
        }
        self.store.set_scheduler_state(
            BACKFILL_RPC_CHUNK_STATE_KEY, payload)
        return payload

    def drain_flow_backfill(
        self, deadline: CycleDeadline, *, block_limit: int,
    ) -> dict:
        """Drain one durable skipped range without touching the live cursor."""
        pending = self.store.pending_backfill(limit=1)
        if not pending:
            return {"ranges_selected": 0, "blocks_scanned": 0,
                    "candidates_added": 0, "backlog": self.store.backfill_backlog()}
        row = pending[0]
        start = safe_int(row.get("next_block"), 0) or int(row["from_block"])
        upper_bound = int(row["to_block"])
        if start > upper_bound:
            self.store.complete_backfill(row["from_block"], row["to_block"])
            return {"ranges_selected": 1, "blocks_scanned": 0,
                    "candidates_added": 0, "completed": True,
                    "backlog": self.store.backfill_backlog()}
        deadline.raise_if_expired("flow_backfill")
        end = min(upper_bound, start + max(1, int(block_limit)) - 1)
        cursor_path = self.root / "flow_backfill_worker_cursor.json"
        atomic_json_write(cursor_path, {
            "next_block": start, "range_from": int(row["from_block"]),
            "range_to": upper_bound, "updated_at": _utc_now(),
        })
        observer = RobinhoodV4Observer(
            self.rpc, self.store, cursor_path,
            remote_attempts=BACKFILL_REMOTE_ATTEMPTS_PER_CHUNK,
        )
        try:
            candidates, coverage = observer.sync(
                block_limit=end - start + 1, lookback=1,
                activation_limit=0,
            )
            added = self.store.add_candidates(candidates)
            self.store.mark_v4_promoted([
                candidate["pool_id"] for candidate in candidates
                if candidate.get("pool_id")
            ])
            next_block = int(coverage.get("to_block") or end) + 1
            self.store.advance_backfill(
                row["from_block"], row["to_block"], next_block)
            completed = next_block > upper_bound
            if completed:
                self.store.complete_backfill(
                    row["from_block"], row["to_block"])
            return {
                "ranges_selected": 1,
                "range": [int(row["from_block"]), upper_bound],
                "from_block": start, "to_block": next_block - 1,
                "blocks_scanned": max(0, next_block - start),
                "candidates_added": added, "completed": completed,
                "backlog": self.store.backfill_backlog(),
            }
        except Exception as exc:
            self.store.advance_backfill(
                row["from_block"], row["to_block"], start,
                error=str(exc)[:500])
            raise

    def drain_flow_backfill_until_reserve(
        self, deadline: CycleDeadline, *, block_limit: int,
        reserve_seconds: float = BACKFILL_COMPLETION_RESERVE_SECONDS,
    ) -> dict:
        """Commit consecutive durable chunks until only reserve remains.

        ``drain_flow_backfill`` is intentionally one atomic chunk: its cursor
        advances immediately after a successful scan. The coordinator may run
        a small bounded sequence of those commits. A failed/throttled request
        stops the sequence without retry, while already committed chunks remain
        useful progress. The independent low-priority lane and child deadline
        keep this historical work away from the latency-critical live scan.
        """
        before = self.store.backfill_backlog()
        chunk_plan = self.backfill_rpc_chunk_plan(block_limit)
        chunk_limit = int(chunk_plan["chunk_blocks"])
        available = max(0.0, deadline.remaining() - max(0.0, reserve_seconds))
        if available <= 0:
            return {
                "ranges_selected": 0, "chunks_processed": 0,
                "cursor_commits": 0, "blocks_scanned": 0,
                "candidates_added": 0, "completed_ranges": 0,
                "chunk_limit_blocks": chunk_limit,
                "completion_reserve_seconds": float(reserve_seconds),
                "stopped_reason": "completion_reserve_reached",
                "backlog_before": before, "backlog": before,
                "pending_blocks_delta": 0, "backlog_shrinking": False,
            }

        recovery_deadline = CycleDeadline(available)
        chunks: list[dict] = []
        stopped_reason = "no_pending_ranges"
        provider_deferral: dict | None = None
        next_chunk_state: dict | None = None
        for _ in range(BACKFILL_MAXIMUM_CHUNKS_PER_CYCLE):
            if recovery_deadline.expired():
                stopped_reason = "completion_reserve_reached"
                break
            chunk_deadline = CycleDeadline(min(
                recovery_deadline.remaining(),
                BACKFILL_RPC_ATTEMPT_BUDGET_SECONDS,
            ))
            try:
                with self._rpc_deadline(chunk_deadline):
                    chunk = self.drain_flow_backfill(
                        chunk_deadline, block_limit=chunk_limit)
            except RuntimeError as exc:
                if not _transient_rpc_failure(exc):
                    raise
                scheduler_deferred = isinstance(
                    exc, BackgroundRpcPriorityDeferred)
                next_chunk_state = (
                    self.backfill_rpc_chunk_plan(block_limit)
                    if scheduler_deferred
                    else self.record_backfill_rpc_chunk_result(
                        chunk_limit, error=exc)
                )
                next_chunk_blocks = safe_int(
                    next_chunk_state.get("next_chunk_blocks"),
                    safe_int(next_chunk_state.get("chunk_blocks"), chunk_limit),
                )
                # Cursor remains on the attempted block. A provider throttle
                # is a controlled no-progress cycle, not evidence corruption
                # and not permission to launch secondary RPC discovery.
                provider_deferral = {
                    "deferred": True,
                    "reason": (
                        "rpc_priority_deferred" if scheduler_deferred
                        else "provider_rate_limited"
                            if _rpc_rate_limited(exc)
                            else "provider_temporarily_unavailable"
                    ),
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:500],
                    "attempts_per_chunk":
                        BACKFILL_REMOTE_ATTEMPTS_PER_CHUNK,
                    "attempted_chunk_blocks": chunk_limit,
                    "next_chunk_blocks": next_chunk_blocks,
                    "attempt_budget_seconds":
                        BACKFILL_RPC_ATTEMPT_BUDGET_SECONDS,
                    "cursor_advanced": False,
                }
                stopped_reason = provider_deferral["reason"]
                break
            if safe_int(chunk.get("ranges_selected"), 0) <= 0:
                stopped_reason = "no_pending_ranges"
                break
            chunks.append(dict(chunk))
            next_chunk_state = self.record_backfill_rpc_chunk_result(
                chunk_limit, error=None)
            progressed = (
                safe_int(chunk.get("blocks_scanned"), 0) > 0
                or bool(chunk.get("completed"))
            )
            if not progressed:
                # A provider/store response claiming selection without cursor
                # progress would otherwise spin until the wall-clock bound.
                stopped_reason = "selected_without_progress"
                break
        else:
            stopped_reason = "maximum_chunks_reached"

        after = self.store.backfill_backlog()
        before_blocks = safe_int(before.get("pending_blocks"), 0)
        after_blocks = safe_int(after.get("pending_blocks"), 0)
        first = chunks[0] if chunks else {}
        last = chunks[-1] if chunks else {}
        return {
            "ranges_selected": sum(
                safe_int(row.get("ranges_selected"), 0) for row in chunks),
            "chunks_processed": len(chunks),
            # advance_backfill is committed inside every successful chunk.
            "cursor_commits": len(chunks),
            "blocks_scanned": sum(
                safe_int(row.get("blocks_scanned"), 0) for row in chunks),
            "candidates_added": sum(
                safe_int(row.get("candidates_added"), 0) for row in chunks),
            "completed_ranges": sum(
                1 for row in chunks if bool(row.get("completed"))),
            "first_from_block": first.get("from_block"),
            "last_to_block": last.get("to_block"),
            "chunk_limit_blocks": chunk_limit,
            "chunk_plan": chunk_plan,
            "next_chunk_blocks": (
                next_chunk_state.get("next_chunk_blocks")
                if next_chunk_state else chunk_limit
            ),
            "completion_reserve_seconds": float(reserve_seconds),
            "stopped_reason": stopped_reason,
            "provider_deferred": bool(provider_deferral),
            "provider_deferral": provider_deferral,
            "backlog_before": before, "backlog": after,
            "pending_blocks_delta": after_blocks - before_blocks,
            "backlog_shrinking": after_blocks < before_blocks,
        }

    def retire_stale_observation_queue(
        self, deadline: CycleDeadline,
    ) -> dict:
        """Bounded SQLite housekeeping performed only after gap recovery."""
        queue_before = self.store.seal_queue_backlog()
        retirement_limit = BACKFILL_STALE_RETIRE_LIMIT
        retired_existing = self.store.retire_expired_seal_queue(
            retirement_limit)
        remaining_retirement = max(
            0, retirement_limit - retired_existing)
        expired_this_cycle = 0
        queue_head = None
        after_existing = self.store.seal_queue_backlog()
        if after_existing.get("pending_windows", 0):
            with self._rpc_deadline(deadline):
                queue_head = int(self.rpc.get_block_number())
            expired_this_cycle = self.store.expire_stale_seal_queue(
                head_block=queue_head,
                freshness_blocks=
                    FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
            )
        retired_new = self.store.retire_expired_seal_queue(
            remaining_retirement)
        retired_total = retired_existing + retired_new
        queue_after = self.store.seal_queue_backlog()
        return {
            "terminal_state": "expired_unsealed",
            "queue_head_block": queue_head,
            "expired_this_cycle": expired_this_cycle,
            "stale_retired_this_cycle": retired_total,
            "quote_calls_avoided": retired_total,
            "observations_created": 0,
            "active_cohort_observations_created": 0,
            "classifications_created": 0,
            "outcomes_scheduled": 0,
            "paper_entries_created": 0,
            "snapshots_preserved_for_audit": retired_total,
            "retirement_limit": retirement_limit,
            "expired_stale_backlog_delta": -int(retired_total),
            "stale_backlog_shrinking": retired_total > 0,
            "backlog_before": queue_before,
            "backlog_after": queue_after,
        }

    def run_backfill_lane(
        self, *, budget_seconds: float = BACKFILL_LANE_BUDGET_SECONDS,
        discovery_block_limit: int = DEFAULT_DISCOVERY_BLOCK_LIMIT,
        identity_limit: int = BACKFILL_LANE_IDENTITY_LIMIT,
        lookback: int = DEFAULT_DISCOVERY_LOOKBACK_BLOCKS,
        now: float | None = None,
    ) -> dict:
        """Historical discovery, enrichment and durable gap recovery lane."""
        _ = time.time() if now is None else float(now)

        def work(deadline: CycleDeadline) -> dict:
            timings: dict[str, float] = {}
            # The acceptance trace measured recovery below arrival while
            # SQLite retirement consumed up to 29 seconds first. Debt service
            # therefore owns the beginning of the lane; housekeeping receives
            # only the remainder and can never reduce committed recovery.
            stage = time.monotonic()
            gap_recovery = self.drain_flow_backfill_until_reserve(
                deadline, block_limit=discovery_block_limit,
                reserve_seconds=BACKFILL_COMPLETION_RESERVE_SECONDS,
            )
            timings["durable_gap_recovery"] = round(
                time.monotonic() - stage, 3)
            deadline.raise_if_expired("durable_gap_recovery")
            gap_active = bool(
                safe_int(gap_recovery.get("ranges_selected"), 0) > 0
                or gap_recovery.get("provider_deferred"))
            if (
                not gap_active
                and deadline.remaining() >=
                    BACKFILL_QUEUE_MAINTENANCE_MINIMUM_SECONDS
            ):
                stage = time.monotonic()
                seal_queue = self.retire_stale_observation_queue(deadline)
                timings["durable_observation_queue"] = round(
                    time.monotonic() - stage, 3)
                deadline.raise_if_expired("durable_observation_queue")
            else:
                seal_queue = {
                    "deferred": True,
                    "reason": (
                        "durable_gap_recovery_priority" if gap_active
                        else "insufficient_maintenance_budget"),
                    "minimum_seconds":
                        BACKFILL_QUEUE_MAINTENANCE_MINIMUM_SECONDS,
                    "remaining_seconds": round(deadline.remaining(), 3),
                }
                timings["durable_observation_queue"] = 0.0
            if (
                safe_int(gap_recovery.get("ranges_selected"), 0) > 0
                or bool(gap_recovery.get("provider_deferred"))
            ):
                # A committed gap chunk is a complete unit of work. Continuing
                # into two discovery cursors and historical identity made the
                # process hit its deadline after useful progress, so every run
                # looked failed and the successful cursor advance was hidden.
                # Drain oldest-first until the durable queue is empty; then a
                # later cycle resumes secondary discovery and enrichment.
                backlog = self.store.backfill_backlog()
                return {
                    "new_candidates": safe_int(
                        gap_recovery.get("candidates_added"), 0),
                    "v2_discovery": {
                        "deferred": True,
                        "reason": (
                            "backfill_provider_deferred"
                            if gap_recovery.get("provider_deferred")
                            else "durable_gap_recovery_priority"
                        ),
                    },
                    "v4_discovery": {
                        "deferred": True,
                        "reason": (
                            "backfill_provider_deferred"
                            if gap_recovery.get("provider_deferred")
                            else "durable_gap_recovery_priority"
                        ),
                    },
                    "durable_gap_recovery": gap_recovery,
                    "durable_observation_queue": seal_queue,
                    "historical_identity_resolution": {
                        "deferred": True,
                        "reason": (
                            "backfill_provider_deferred"
                            if gap_recovery.get("provider_deferred")
                            else "durable_gap_recovery_priority"
                        ),
                    },
                    "stage_timings_seconds": timings,
                    "cursor": {
                        "gap": read_json(
                            self.root / "flow_backfill_worker_cursor.json", {}
                        ) or {},
                    },
                    "backlog": backlog,
                    "historical_only": True,
                    "backfill_rpc_isolated":
                        self.backfill_rpc_isolated,
                    "priority_mode": "oldest_durable_gap_first",
                }

            stage = time.monotonic()
            with self._rpc_deadline(deadline):
                discovered, v2_coverage = self.observer.sync(
                    block_limit=discovery_block_limit, lookback=lookback)
            timings["uniswap_v2_discovery"] = round(
                time.monotonic() - stage, 3)
            deadline.raise_if_expired("uniswap_v2_discovery")

            stage = time.monotonic()
            with self._rpc_deadline(deadline):
                v4_discovered, v4_coverage = self.v4_observer.sync(
                    block_limit=discovery_block_limit, lookback=lookback,
                    activation_limit=BACKFILL_V4_ACTIVATION_LIMIT,
                )
            timings["uniswap_v4_discovery"] = round(
                time.monotonic() - stage, 3)
            new_candidates = self.store.add_candidates(
                discovered + v4_discovered)
            self.store.mark_v4_promoted([
                row["pool_id"] for row in v4_discovered if row.get("pool_id")
            ])
            deadline.raise_if_expired("candidate_persistence")

            stage = time.monotonic()
            with self._rpc_deadline(deadline):
                identity = self.resolve_flow_participants(
                    limit=max(0, int(identity_limit)),
                    deadline_monotonic=time.monotonic() + min(
                        deadline.remaining(), FLOW_IDENTITY_STAGE_BUDGET_SECONDS),
                    head_block=None,
                )
            timings["historical_identity"] = round(
                time.monotonic() - stage, 3)
            return {
                "new_candidates": new_candidates,
                "v2_discovery": v2_coverage,
                "v4_discovery": v4_coverage,
                "durable_gap_recovery": gap_recovery,
                "durable_observation_queue": seal_queue,
                "historical_identity_resolution": identity,
                "stage_timings_seconds": timings,
                "cursor": {
                    "v2": read_json(self.root / "discovery_cursor.json", {}) or {},
                    "v4": read_json(self.root / "discovery_v4_cursor.json", {}) or {},
                    "gap": read_json(
                        self.root / "flow_backfill_worker_cursor.json", {}) or {},
                },
                "backlog": self.store.backfill_backlog(),
                "historical_only": True,
                "backfill_rpc_isolated": self.backfill_rpc_isolated,
            }

        return self._execute_lane("backfill", budget_seconds, work)

    def run_once(self, *, discovery_block_limit=DEFAULT_DISCOVERY_BLOCK_LIMIT,
                 analysis_limit=DEFAULT_ANALYSIS_LIMIT,outcome_limit=DEFAULT_OUTCOME_LIMIT,
                 outcome_recovery_limit=DEFAULT_OUTCOME_RECOVERY_LIMIT,
                 market_recheck_limit=DEFAULT_MARKET_RECHECK_LIMIT,
                 cycle_budget_seconds=DEFAULT_CYCLE_BUDGET_SECONDS,
                 lookback=DEFAULT_DISCOVERY_LOOKBACK_BLOCKS,now:float|None=None) -> dict:
        started = time.monotonic()
        now = time.time() if now is None else now
        # ONE monotonic deadline for the whole cycle, handed to every stage
        # rather than each recomputing its own from wall clock.
        deadline = CycleDeadline(float(cycle_budget_seconds))
        self.cycle_deadline = deadline
        cycle_run_id = uuid.uuid4().hex
        self.cycle_run_uuid = cycle_run_id
        with LearningRunLock(self.root / ".learn_once.lock"):
            run_id = self.store.begin_run(cycle_run_id, float(cycle_budget_seconds))
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
                if deadline.expired():
                    v4_coverage = {"skipped": "cycle_deadline_exceeded"}
                    stage_timings["uniswap_v4_discovery_skipped"] = True
                catchup_deadline = (
                    time.monotonic() + FLOW_DISCOVERY_CATCHUP_SECONDS
                )
                catchup_passes = 0
                while not deadline.expired():
                    chunk, v4_coverage = self.v4_observer.sync(
                        block_limit=discovery_block_limit, lookback=lookback
                    )
                    v4_discovered.extend(chunk)
                    catchup_passes += 1
                    if v4_coverage.get("caught_up"):
                        break
                    if catchup_passes >= FLOW_DISCOVERY_MAXIMUM_PASSES:
                        break
                    if time.monotonic() >= catchup_deadline or deadline.expired():
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
                # The deadline must reach the stage, not merely exist beside
                # it: it was created and never read, so stages ran unbounded.
                # An expired deadline SKIPS the stage and records the skip --
                # it does not abort the cycle, because the existing contract
                # is graceful degradation (analysis defers rather than
                # failing). Interrupting work already in flight is a separate
                # mechanism: sqlite_guard aborts a running statement.
                if deadline.expired():
                    near_head_flow = {"supported": True, "scanned": False,
                                      "reason": "cycle_deadline_exceeded"}
                    stage_timings["near_head_flow_skipped"] = True
                else:
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
                # The integrity certificate must be fresh BEFORE any paper
                # entry in this cycle: the execution gate fails closed on a
                # missing certificate, so refresh it here (off the live
                # lane) whenever a recorder is configured.
                certificate_refresh = None
                if self.timechain_recorder is not None:
                    stage_started = time.monotonic()
                    try:
                        certificate_refresh = (
                            self.publish_integrity_certificate())
                    except Exception as exc:
                        certificate_refresh = {
                            "published": False, "reason": str(exc)[:200]}
                    stage_timings["certificate_refresh"] = round(
                        time.monotonic() - stage_started, 3)
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
                        entry = self.guarded_paper_entry(
                            latest, market, run_id=self.cycle_run_uuid,
                            priority_reason=priority_reason,
                        )
                        if entry.get("entered"):
                            entries += 1
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
                self.store.heartbeat_run(cycle_run_id)
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
                        "UPDATE runs SET completed_at=?,status='complete',summary_json=?"
                        " WHERE id=? AND run_id=?",
                        (_utc_now(), _canonical(summary), run_id, cycle_run_id),
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
                        "UPDATE runs SET completed_at=?,status='failed',summary_json=?"
                        " WHERE id=? AND run_id=?",
                        (_utc_now(), _canonical(failure), run_id, cycle_run_id),
                    )
                raise

    def _verify(self, *, full: bool) -> dict:
        if not full:
            full_path = self.root / "full_verification_status.json"
            legacy = read_json(self.root / "verification_status.json", {}) or {}
            if (
                not full_path.exists()
                and legacy.get("sqlite_integrity") is not None
                and legacy.get("sqlite_operational_health") is None
            ):
                # Preserve the last genuine pre-split full certificate before
                # the operational certificate replaces its legacy filename.
                atomic_json_write(full_path, legacy)
        ledger_ok,ledger_report=self.ledger.verify()
        with self.store.connection() as connection:
            if full:
                sqlite_ok = (
                    connection.execute(
                        "PRAGMA integrity_check").fetchone()[0] == "ok")
                sqlite_report = {"check": "integrity_check", "ok": sqlite_ok}
            else:
                required = {
                    "runs", "candidates", "flow_observations", "positions",
                }
                present = {
                    str(row[0]) for row in connection.execute(
                        "SELECT name FROM sqlite_schema"
                        " WHERE type='table' AND name IN (?,?,?,?)",
                        tuple(sorted(required)),
                    ).fetchall()
                }
                readable = {}
                for table in sorted(required & present):
                    connection.execute(
                        f'SELECT * FROM "{table}" LIMIT 1').fetchone()
                    readable[table] = True
                schema_version = int(connection.execute(
                    "PRAGMA schema_version").fetchone()[0])
                page_count = int(connection.execute(
                    "PRAGMA page_count").fetchone()[0])
                journal_mode = str(connection.execute(
                    "PRAGMA journal_mode").fetchone()[0]).lower()
                sqlite_ok = bool(
                    present == required and len(readable) == len(required)
                    and schema_version >= 0 and page_count > 0
                    and journal_mode in {"wal", "delete"})
                sqlite_report = {
                    "check": "bounded_operational_health",
                    "ok": sqlite_ok,
                    "required_tables": sorted(required),
                    "present_tables": sorted(present),
                    "readable_tables": sorted(readable),
                    "schema_version": schema_version,
                    "page_count": page_count,
                    "journal_mode": journal_mode,
                }
        timechain_ok = True
        timechain_report = "disabled"
        if self.timechain_recorder is not None:
            timechain_ok, timechain_report = self.timechain_recorder.verify()
        result = {
            "ok": ledger_ok and sqlite_ok and timechain_ok,
            "ledger": ledger_report,
            "event_ledger_ok": ledger_ok,
            "sqlite_integrity": sqlite_ok if full else None,
            "sqlite_operational_health": sqlite_ok if not full else None,
            "sqlite_report": sqlite_report,
            "verification_level": "full" if full else "operational",
            "producer_timechain": timechain_report,
            "producer_timechain_ok": timechain_ok,
            "paper_only": True,
            "checked_at": _utc_now(),
            "revision": CODE_REVISION,
            "source_digest": _worktree_source_digest(),
        }
        atomic_json_write(
            self.root / (
                "full_verification_status.json"
                if full else "verification_status.json"),
            result,
        )
        return result

    def verify(self) -> dict:
        """Run the exhaustive offline certificate (may take hours)."""
        return self._verify(full=True)

    def verify_operational(self) -> dict:
        """Run the bounded daily certificate without claiming full DB proof."""
        return self._verify(full=False)

    def run_verification_lane(
        self, *, budget_seconds: float = VERIFICATION_LANE_BUDGET_SECONDS,
    ) -> dict:
        """Refresh the daily operational certificate off every critical lane."""
        def work(_deadline: CycleDeadline) -> dict:
            result = self.verify_operational()
            return {"verification": result, "integrity_ok": bool(result["ok"])}

        return self._execute_lane("verification", budget_seconds, work)

    def run_full_verification_lane(
        self, *, budget_seconds: float = FULL_VERIFICATION_LANE_BUDGET_SECONDS,
    ) -> dict:
        """Refresh the exhaustive weekly certificate in offline maintenance."""
        def work(_deadline: CycleDeadline) -> dict:
            result = self.verify()
            return {"verification": result, "integrity_ok": bool(result["ok"])}

        return self._execute_lane("full_verification", budget_seconds, work)

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


class _WindowsLaneJob:
    """Own lane children without nesting the supervisor's existing job.

    Task Scheduler may already place the supervisor in a job. Assigning the
    supervisor to another job can suspend startup, while relying on the task's
    job does not reliably kill descendants on Stop-ScheduledTask. A dedicated
    job containing only the lane workers gives the desired invariant: when the
    supervisor exits abruptly, Windows closes its handle and kills every lane.
    """

    def __init__(self) -> None:
        self.handle = None
        if os.name != "nt":
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        kernel32.CreateJobObjectW.restype = wintypes.HANDLE
        kernel32.SetInformationJobObject.argtypes = [
            wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
        ]
        kernel32.SetInformationJobObject.restype = wintypes.BOOL
        kernel32.AssignProcessToJobObject.argtypes = [
            wintypes.HANDLE, wintypes.HANDLE,
        ]
        kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
        kernel32.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel32.CloseHandle.restype = wintypes.BOOL
        handle = kernel32.CreateJobObjectW(None, None)
        if not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        # JOBOBJECT_EXTENDED_LIMIT_INFORMATION is 144 bytes on this x64
        # runtime; ctypes arrays are zero-initialized, so no accidental native
        # flags or limits can leak into the structure.
        information = (ctypes.c_ubyte * 144)()
        ctypes.c_uint32.from_buffer(information, 16).value = 0x00002000
        if not kernel32.SetInformationJobObject(
            handle, 9, ctypes.byref(information), ctypes.sizeof(information),
        ):
            kernel32.CloseHandle(handle)
            raise ctypes.WinError(ctypes.get_last_error())
        self.handle = handle
        self._kernel32 = kernel32

    def assign(self, process: subprocess.Popen) -> None:
        if self.handle is None:
            return
        if not self._kernel32.AssignProcessToJobObject(
            self.handle, wintypes.HANDLE(int(process._handle)),
        ):
            error = ctypes.WinError(ctypes.get_last_error())
            try:
                process.kill()
            finally:
                raise error

    def close(self) -> None:
        if self.handle is not None:
            self._kernel32.CloseHandle(self.handle)
            self.handle = None


def _reconcile_dead_lane_state(
    store: RobinhoodLearningStore, process_is_running=_process_is_running,
) -> list[str]:
    """Recover every abandoned run, not only the latest row per lane."""
    recovered = store.recover_abandoned_runs(process_is_running)
    return sorted({
        str(item.get("lane")) for item in recovered
        if item.get("lane") and item.get("lane") != "legacy"
    })


def _lane_launch_fits(
    remaining_window_seconds: float, budget_seconds: float,
    grace_seconds: float = LANE_TERMINATION_GRACE_SECONDS,
) -> bool:
    """A worker may start only when its whole kill-safe window still fits."""
    return float(remaining_window_seconds) >= (
        float(budget_seconds) + float(grace_seconds)
    )


def _lane_creation_flags(lane: str) -> int:
    """Windows scheduler priority is part of the lane isolation contract."""
    if os.name != "nt":
        return 0
    flags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))
    if lane == "live":
        flags |= int(getattr(subprocess, "ABOVE_NORMAL_PRIORITY_CLASS", 0))
    elif lane in {"analysis", "backfill", "verification"}:
        flags |= int(getattr(subprocess, "BELOW_NORMAL_PRIORITY_CLASS", 0))
    return flags


def _low_priority_launch_blocked(
    lane: str, active_lanes, *, now: float, next_live: float,
    guard_seconds: float = LIVE_LANE_LAUNCH_GUARD_SECONDS,
) -> bool:
    """Reserve worker-startup capacity around every live launch."""
    if lane == "live":
        return False
    if "live" in active_lanes:
        return True
    seconds_to_live = float(next_live) - float(now)
    return 0.0 <= seconds_to_live <= float(guard_seconds)


def _select_background_candidate(
    due_background: list[tuple[str, dict]], launches: dict[str, int],
    *, cohort_progress: dict | None = None,
    backfill_pressure: dict | None = None,
) -> str | None:
    """Serve durable cohort and recovery deficits before ordinary debt."""
    if not due_background:
        return None
    due_by_name = {lane: schedule for lane, schedule in due_background}
    progress = cohort_progress or {}
    if progress.get("collecting"):
        mark_target = safe_int(progress.get("mark_pace_target"), 0)
        mark_count = safe_int(progress.get("mark_terminal"), 0)
    else:
        mark_target = math.ceil(
            max(0, int(launches.get("live", 0)))
            * ACCEPTANCE_POSITION_MARK_SAMPLE_FRACTION)
        mark_count = int(launches.get("marks", 0))
    if (
        "marks" in due_by_name
        and mark_count < mark_target
    ):
        return "marks"
    if bool((backfill_pressure or {}).get("priority")):
        # Recovery below arrivals is an exclusive background mode. Launching
        # analysis/evidence while a backfill worker waits for the shared RPC
        # gate consumed 88 of its 120 seconds before its single request. Live
        # and paced marks continue; other background RPC work resumes once the
        # durable ten-run ratio reaches its frozen target.
        return "backfill" if "backfill" in due_by_name else None
    return max(
        due_background,
        key=lambda item: time.monotonic() - float(item[1]["next"]),
    )[0]


def _full_verification_due(
    root: str | Path, *, now: float | None = None,
) -> bool:
    status = read_json(Path(root) / "verification_status.json", {}) or {}
    checked = _timestamp(status.get("checked_at"))
    current = time.time() if now is None else float(now)
    return bool(
        not status.get("ok") or checked is None
        or str(status.get("revision") or "") != CODE_REVISION
        or str(status.get("source_digest") or "")
            != _worktree_source_digest()
        or current - checked >= FULL_VERIFICATION_REFRESH_SECONDS)


def supervise_lanes(
    root: str | Path, *, chain_root: str | Path,
    skill_root: str | Path, duration_seconds: float,
    discovery_block_limit: int, analysis_limit: int,
    outcome_limit: int, outcome_recovery_limit: int,
    market_recheck_limit: int,
    live_cadence_seconds: float = LIVE_LANE_CADENCE_SECONDS,
    marks_cadence_seconds: float = MARKS_LANE_CADENCE_SECONDS,
    evidence_cadence_seconds: float = EVIDENCE_LANE_CADENCE_SECONDS,
    analysis_cadence_seconds: float = ANALYSIS_LANE_CADENCE_SECONDS,
    backfill_cadence_seconds: float = BACKFILL_LANE_CADENCE_SECONDS,
) -> dict:
    """Supervise independently killable, paper-only lane processes."""
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    log_root = root / "logs"
    log_root.mkdir(parents=True, exist_ok=True)
    started_mono = time.monotonic()
    started_wall = time.time()
    stop_at = started_mono + max(5.0, float(duration_seconds))
    lanes = {
        "live": {
            "command": "live-once", "cadence": max(10.0, live_cadence_seconds),
            "budget": LIVE_LANE_BUDGET_SECONDS, "next": started_mono,
        },
        # Marking is its own lane: measured at 21.75s it cannot share the
        # live lane's budget, and it depends on an external price API rather
        # than chain RPC, so a price stall must not spend the budget that
        # blockchain freshness needs. Offset by 1.5s so it does not start in
        # lockstep with the live lane every cycle.
        "marks": {
            "command": "marks-once",
            "cadence": max(15.0, marks_cadence_seconds),
            "budget": MARKS_LANE_BUDGET_SECONDS, "next": started_mono + 1.5,
        },
        "evidence": {
            "command": "evidence-once",
            "cadence": max(30.0, evidence_cadence_seconds),
            "budget": EVIDENCE_LANE_BUDGET_SECONDS,
            "next": started_mono + 2.25,
        },
        "analysis": {
            "command": "analysis-once",
            "cadence": max(30.0, analysis_cadence_seconds),
            "budget": ANALYSIS_LANE_BUDGET_SECONDS, "next": started_mono + 3.0,
        },
        "backfill": {
            "command": "backfill-once",
            "cadence": max(60.0, backfill_cadence_seconds),
            "budget": BACKFILL_LANE_BUDGET_SECONDS, "next": started_mono + 6.0,
        },
    }
    active: dict[str, dict] = {}
    launches = {lane: 0 for lane in lanes}
    timeouts = {lane: 0 for lane in lanes}
    failures = {lane: 0 for lane in lanes}
    tail_skips = {lane: 0 for lane in lanes}
    priority_deferrals = {lane: 0 for lane in lanes}
    live_quiet_until = started_mono
    last_background_launch = started_mono - (
        BACKGROUND_LANE_LAUNCH_SPACING_SECONDS)
    status_path = root / "scheduler_status.json"
    rpc_priority_path = root / BACKGROUND_RPC_PRIORITY_STATE_FILE
    supervisor_store = RobinhoodLearningStore(root / "learning.sqlite3")
    recovered_dead_lanes = _reconcile_dead_lane_state(supervisor_store)
    last_orphan_sweep = time.monotonic()
    last_deficit_refresh = float("-inf")
    cohort_progress: dict = {"collecting": False}
    backfill_pressure: dict = {"priority": False}
    lane_job = _WindowsLaneJob()
    worker_python = str(getattr(sys, "_base_executable", None) or sys.executable)
    worker_environment = os.environ.copy()
    venv_site_packages = [
        entry for entry in sys.path
        if "site-packages" in str(entry).lower()
    ]
    if venv_site_packages:
        existing_pythonpath = worker_environment.get("PYTHONPATH", "")
        worker_environment["PYTHONPATH"] = os.pathsep.join([
            *venv_site_packages,
            *([existing_pythonpath] if existing_pythonpath else []),
        ])

    def command_for(lane: str) -> list[str]:
        command = [
            worker_python, "-X", "utf8", str(Path(__file__).resolve()),
            lanes[lane]["command"], "--root", str(root),
            "--chain-root", str(chain_root), "--skill-root", str(skill_root),
            "--lane-budget-seconds", str(lanes[lane]["budget"]),
        ]
        if lane == "analysis":
            command += [
                "--analysis-limit", str(analysis_limit),
                "--outcome-limit", str(outcome_limit),
                "--outcome-recovery-limit", str(outcome_recovery_limit),
                "--market-recheck-limit", str(market_recheck_limit),
            ]
        elif lane == "backfill":
            command += [
                "--discovery-block-limit", str(discovery_block_limit)]
        return command

    def publish(status: str = "running") -> None:
        rpc_priority = {
            "schema_version": 2,
            "status": status,
            "published_monotonic": time.monotonic(),
            "published_at": time.time(),
            "next_live_monotonic": float(lanes["live"]["next"]),
            "live_active": "live" in active,
            "live_pid": (
                active["live"]["process"].pid
                if "live" in active else None),
            "live_cadence_seconds": float(lanes["live"]["cadence"]),
            "guard_seconds": BACKGROUND_RPC_LIVE_GUARD_SECONDS,
            "minimum_window_seconds":
                BACKGROUND_RPC_MINIMUM_WINDOW_SECONDS,
            "minimum_interval_seconds":
                BACKGROUND_RPC_MINIMUM_INTERVAL_SECONDS,
            "background_serialized": True,
        }
        atomic_json_write(rpc_priority_path, rpc_priority)
        atomic_json_write(status_path, {
            "schema_version": 2, "status": status,
            "mode": "lane_supervisor", "started_at": started_wall,
            "pid": os.getpid(), "heartbeat_at": time.time(),
            "active_lanes": {
                lane: {
                    "pid": item["process"].pid,
                    "started_at": item["wall_started"],
                    "hard_deadline_at": item["wall_deadline"],
                    "priority": item["priority"],
                } for lane, item in active.items()
            },
            "lane_state": supervisor_store.lane_states(),
            "recovered_dead_lanes": recovered_dead_lanes,
            "launches": launches, "timeouts": timeouts, "failures": failures,
            "tail_skips": tail_skips,
            "priority_deferrals": priority_deferrals,
            "cohort_scheduler": cohort_progress,
            "backfill_pressure": backfill_pressure,
            "rpc_priority": rpc_priority,
            "paper_only": True, "live_execution_enabled": False,
        })

    try:
        while time.monotonic() < stop_at:
            now_mono = time.monotonic()
            # The startup sweep cannot reclaim a run that dies AFTER it. A
            # supervisor session outlives many lane runs, so a worker killed
            # mid-session held its `running` row until the next supervisor
            # restart -- measured at 90 minutes on an analysis row whose
            # heartbeat had stopped, while run_ownership is a critical-tier
            # audit criterion that forces DEGRADED on a single stale row.
            #
            # The sweep is conservative and idempotent: it closes only rows
            # whose owner is dead, whose heartbeat exceeded its own deadline
            # plus grace, or which the lane has already superseded. Running it
            # on a cadence costs one indexed query and removes the dependency
            # on restart timing.
            if now_mono - last_orphan_sweep >= ORPHAN_SWEEP_INTERVAL_SECONDS:
                last_orphan_sweep = now_mono
                try:
                    _reconcile_dead_lane_state(supervisor_store)
                except Exception:
                    # Recovery is housekeeping; never let it stop scheduling.
                    pass
            for lane, item in list(active.items()):
                process = item["process"]
                code = process.poll()
                if code is not None:
                    item["stdout"].close()
                    item["stderr"].close()
                    failures[lane] += int(code != 0)
                    active.pop(lane, None)
                elif now_mono >= item["deadline"]:
                    process.kill()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        pass
                    item["stdout"].close()
                    item["stderr"].close()
                    timeouts[lane] += 1
                    supervisor_store.terminate_lane(
                        lane, process.pid,
                        "supervisor_hard_deadline_exceeded",
                        attempt_started_at=item["wall_started"])
                    if lane != "live":
                        live_quiet_until = max(
                            live_quiet_until,
                            time.monotonic()
                            + LIVE_LANE_POST_KILL_QUIET_SECONDS)
                    active.pop(lane, None)
            if now_mono - last_deficit_refresh >= 5.0:
                last_deficit_refresh = now_mono
                try:
                    cohort_progress = (
                        supervisor_store.acceptance_scheduler_progress())
                    backfill_pressure = (
                        supervisor_store.backfill_recovery_pressure())
                except sqlite3.Error:
                    # Deficit telemetry may defer a preference, but it may
                    # never stop the supervisor or manufacture state.
                    pass
            # Iterate the CONFIGURATION. A hardcoded list beside a lanes dict
            # is a second source of truth, and it silently dropped `marks`:
            # the lane was defined, budgeted and given a cadence, and never
            # launched once. Positions then went unmarked entirely, because
            # marking had already been removed from the live lane.
            due_background = [
                (lane, schedule) for lane, schedule in lanes.items()
                if lane != "live" and lane not in active
                and now_mono >= float(schedule["next"])
            ]
            background_candidate = _select_background_candidate(
                due_background, launches,
                cohort_progress=cohort_progress,
                backfill_pressure=backfill_pressure)
            for lane, schedule in lanes.items():
                if lane in active or now_mono < schedule["next"]:
                    continue
                if (
                    lane == "live"
                    and bool(cohort_progress.get("closing_live_blocked"))
                ):
                    priority_deferrals[lane] += 1
                    continue
                if lane != "live" and lane != background_candidate:
                    priority_deferrals[lane] += 1
                    continue
                if (lane != "live" and now_mono - last_background_launch
                        < BACKGROUND_LANE_LAUNCH_SPACING_SECONDS):
                    priority_deferrals[lane] += 1
                    continue
                if lane == "live" and now_mono < live_quiet_until:
                    priority_deferrals[lane] += 1
                    continue
                if _low_priority_launch_blocked(
                    lane, active, now=now_mono,
                    next_live=float(lanes["live"]["next"]),
                    guard_seconds=(
                        BACKFILL_LAUNCH_MINIMUM_LIVE_WINDOW_SECONDS
                        if lane == "backfill"
                        else LIVE_LANE_LAUNCH_GUARD_SECONDS),
                ):
                    priority_deferrals[lane] += 1
                    continue
                remaining_window = max(0.0, stop_at - now_mono)
                if not _lane_launch_fits(
                    remaining_window, float(schedule["budget"]),
                ):
                    tail_skips[lane] += 1
                    while schedule["next"] <= now_mono:
                        schedule["next"] += float(schedule["cadence"])
                    continue
                stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                stdout = open(
                    log_root / f"{lane}-{stamp}.log", "a", encoding="utf-8")
                stderr = open(
                    log_root / f"{lane}-{stamp}.error.log", "a", encoding="utf-8")
                child_deadline = now_mono + float(schedule["budget"])
                lane_environment = dict(worker_environment)
                lane_environment[
                    "CHAINSEER_LANE_DEADLINE_MONOTONIC"] = repr(child_deadline)
                lane_environment["CHAINSEER_RPC_PRIORITY_REQUIRED"] = (
                    "0" if lane == "backfill" and str(
                        lane_environment.get(BACKFILL_RPC_URL_ENV) or ""
                    ).strip() else "1")
                process = subprocess.Popen(
                    command_for(lane), cwd=str(Path(__file__).resolve().parent),
                    stdout=stdout, stderr=stderr, env=lane_environment,
                    creationflags=_lane_creation_flags(lane),
                )
                lane_job.assign(process)
                wall_started = time.time()
                active[lane] = {
                    "process": process, "stdout": stdout, "stderr": stderr,
                    "deadline": child_deadline + LANE_TERMINATION_GRACE_SECONDS,
                    "wall_started": wall_started,
                    "wall_deadline": wall_started + float(schedule["budget"]) + (
                        LANE_TERMINATION_GRACE_SECONDS),
                    "priority": (
                        "above_normal" if lane == "live" else (
                            "below_normal" if lane in {"analysis", "backfill"}
                            else "normal")),
                }
                launches[lane] += 1
                if lane != "live":
                    last_background_launch = now_mono
                schedule["next"] = _next_lane_launch_after_start(
                    now_mono, float(schedule["cadence"]))
            publish()
            time.sleep(1.0)
    finally:
        for lane, item in list(active.items()):
            process = item["process"]
            if process.poll() is None:
                process.kill()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
                supervisor_store.terminate_lane(
                    lane, process.pid, "supervisor_window_closed",
                    attempt_started_at=item["wall_started"])
            item["stdout"].close()
            item["stderr"].close()
        active.clear()
        publish("complete")
        lane_job.close()
    return {
        "status": "complete", "mode": "lane_supervisor",
        "launches": launches, "timeouts": timeouts, "failures": failures,
        "tail_skips": tail_skips,
        "priority_deferrals": priority_deferrals,
        "duration_seconds": round(time.monotonic() - started_mono, 3),
        "paper_only": True, "live_execution_enabled": False,
    }


def _dashboard_integrity(
    root: str | Path, *, chain_root: str | Path | None = None,
    skill_root: str | Path | None = None,
) -> dict:
    """Combine fresh operational and weekly full certificates without I/O."""
    root = Path(root)
    certificate = read_json(root / "verification_status.json", {}) or {}
    full_certificate = read_json(
        root / "full_verification_status.json", {}) or {}
    # Compatibility for the last pre-split full certificate. It is accepted
    # only as full evidence and only inside the explicit full-age window.
    if not full_certificate and certificate.get("sqlite_integrity") is not None:
        full_certificate = certificate
    checked_epoch = _timestamp(certificate.get("checked_at"))
    age = time.time() - checked_epoch if checked_epoch is not None else None
    fresh = bool(
        age is not None and 0 <= age <= DASHBOARD_INTEGRITY_MAX_AGE_SECONDS)
    full_checked_epoch = _timestamp(full_certificate.get("checked_at"))
    full_age = (
        time.time() - full_checked_epoch
        if full_checked_epoch is not None else None)
    full_fresh = bool(
        full_age is not None and 0 <= full_age <= FULL_INTEGRITY_MAX_AGE_SECONDS)
    result = {
        "sqlite": False, "sqlite_operational": False, "sqlite_full": False,
        "event_ledger": False, "producer_timechain": False,
        "checked_at": certificate.get("checked_at"),
        "age_seconds": round(age, 1) if age is not None else None,
        "maximum_age_seconds": DASHBOARD_INTEGRITY_MAX_AGE_SECONDS,
        "fresh": fresh,
        "full_checked_at": full_certificate.get("checked_at"),
        "full_age_seconds": (
            round(full_age, 1) if full_age is not None else None),
        "full_maximum_age_seconds": FULL_INTEGRITY_MAX_AGE_SECONDS,
        "full_fresh": full_fresh,
    }
    if certificate:
        result["sqlite_operational"] = bool(
            certificate.get("sqlite_operational_health"))
        result["event_ledger"] = bool(certificate.get("event_ledger_ok"))
        result["producer_timechain"] = bool(
            certificate.get("producer_timechain_ok"))
        result["event_ledger_report"] = certificate.get("ledger")
        result["producer_timechain_report"] = certificate.get(
            "producer_timechain")
    else:
        result["status"] = "verification_required"
    result["sqlite_full"] = bool(
        full_certificate.get("sqlite_integrity"))
    result["sqlite"] = bool(
        fresh and full_fresh and result["sqlite_operational"]
        and result["sqlite_full"])
    result["ok"] = bool(
        fresh and result["sqlite"] and result["event_ledger"]
        and result["producer_timechain"]
    )
    return result


def _dashboard_decision_gate(
    root: str | Path,
) -> dict:
    """Decision-gate observability for the dashboard/API.

    Reads only committed state (no Timechain access, no full-chain
    verification) so it stays safe to call from request handlers.
    """
    root = Path(root)
    store_path = root / "decision_commitments.sqlite3"
    if not store_path.exists():
        return {
            "state": "DEGRADED",
            "explanation": [
                "decision commitment store has never been created; "
                "no pre-action authorization is possible yet"],
            "metrics": {}, "seal_debt": None, "integrity": None,
        }
    store = DecisionCommitmentStore(store_path)
    gate = ExecutionGate(store)
    return gate.snapshot()


def dashboard_operational_snapshot(
    root: str | Path,
    *,
    store: RobinhoodLearningStore | None = None,
    live_position_markets: dict[str, dict] | None = None,
    market_refresh_errors: dict[str, str] | None = None,
    chain_root: str | Path | None = None,
    skill_root: str | Path | None = None,
    integrity: dict | None = None,
) -> dict:
    """Build the bounded state needed to operate the learner safely.

    Nothing here may depend on the historical flow range joins.  This payload
    is what lets a viewer see lane ownership, reliability, positions and
    integrity within seconds even when the research corpus takes minutes to
    aggregate.
    """
    root=Path(root)
    store=store or RobinhoodLearningStore(root/"learning.sqlite3")
    summary=read_json(root/"learning_summary.json",{}) or {}
    lane_summaries = {
        lane: read_json(root / f"{lane}_lane_summary.json", {}) or {}
        for lane in LANE_NAMES
    }
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
    lane_performance = store.lane_performance()
    if integrity is None:
        integrity = _dashboard_integrity(
            root, chain_root=chain_root, skill_root=skill_root)
    stabilization = store.stabilization_summary(integrity=integrity)
    return {
        "timestamp":_utc_now(),"network":"robinhood","chain_id":ROBINHOOD_NETWORK.chain_id,
        "learning":store.summary(),
        "positions":[position for position in positions if position.get("status") == "open"],
        "closed_positions":[
            position for position in positions if position.get("status") == "closed"
        ][:12],
        "closed_performance":store.closed_performance(),
        "last_cycle":summary.get("cycle") or {},
        "lanes": {
            "state": store.lane_states(),
            "performance": lane_performance,
            "latest": lane_summaries,
            "backfill_backlog": store.backfill_backlog(),
            # Deferred windows are only durable if something reads the queue.
            # The block-range queue spent a session reporting a skipped COUNT
            # that nothing could consume; publishing the depth beside the
            # lanes is what makes a queue that never drains visible as a leak.
            "seal_queue_backlog": store.seal_queue_backlog(),
            "acceptance": {
                "live_deadline_seconds": LIVE_LANE_BUDGET_SECONDS,
                "live_decision_reserve_seconds":
                    LIVE_LANE_DECISION_RESERVE_SECONDS,
                "live_target_p95_seconds": 30.0,
                "seal_stage_target_p95_seconds": 12.0,
                "live_cycle_target_p95_seconds": 20.0,
                "minimum_deadline_headroom_seconds": 5.0,
                "no_historical_scanning_on_live_path": True,
                "position_marks_first_on_every_live_cycle": True,
                "timechain_writer": "analysis_lane_only",
            },
        },
        "stabilization": stabilization,
        "decision_gate": _dashboard_decision_gate(root),
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


def dashboard_historical_snapshot(
    root: str | Path, *, store: RobinhoodLearningStore | None = None,
) -> dict:
    """Build research aggregates that are allowed to finish asynchronously."""
    root = Path(root)
    store = store or RobinhoodLearningStore(root / "learning.sqlite3")
    # This is the only full origin census in a historical refresh.  The old
    # payload called it once here and again inside flow_summary, turning one
    # expensive research query into permanent disk pressure on the live lane.
    flow_origin_queue = store.pending_transaction_origin_counts()
    return {
        "flow_shadow": store.flow_summary(pending=flow_origin_queue),
        "flow_signals": store.recent_flow_signals(limit=12),
        "flow_evidence": store.flow_evidence_summary(),
        # Friction is the largest component of every return recorded here.
        "round_trip": store.round_trip_summary(),
        # Duration and completion rate together; either alone misleads.
        "lane_health": store.lane_health(),
        # Distinct from discovery_coverage, which is the BACKFILL cursor.
        "pool_discovery": store.pool_discovery_latency(),
        "flow_evidence_events": store.recent_flow_evidence_events(limit=16),
        "flow_origin_queue": flow_origin_queue,
    }


def live_lane_reliability_snapshot(
    root: str | Path, *, store: RobinhoodLearningStore | None = None,
    window: int = 300,
) -> dict:
    """Reliability telemetry for the live lane, aggregated from durable state.

    Completion rate counts deadline_exceeded cycles as the failures they are
    (a killed cycle produced no decision for its observations); latency is
    reported as an ALL-ATTEMPT p95, not a completed-only one -- a p95 over
    successes only describes the easy 6% of runs.
    """
    root = Path(root)
    store = store or RobinhoodLearningStore(root / "learning.sqlite3")
    durations: list[float] = []
    statuses: dict[str, int] = {}
    stage_timeouts: dict[str, int] = {}
    total = 0
    useful_completions = 0
    idle_completions = 0
    decision_opportunities = 0
    with store.connection() as connection:
        rows = connection.execute(
            """SELECT status, summary_json, started_at, completed_at,
                      deadline_seconds FROM runs
               WHERE lane='live' ORDER BY started_at DESC LIMIT ?""",
            (int(window),),
        ).fetchall()
        oldest_deferred = connection.execute(
            """SELECT MIN(o.sealed_at) FROM flow_observations o
               WHERE o.policy_version=?
                 AND NOT EXISTS (
                     SELECT 1 FROM flow_observation_classifications c
                     WHERE c.observation_id=o.observation_id)""",
            (FLOW_EVIDENCE_POLICY_VERSION,),
        ).fetchone()[0]
    for row in rows:
        if row["status"] == "running":
            # A snapshot taken mid-cycle must not turn an unfinished attempt
            # into an uncontrolled failure or change the terminal denominator.
            continue
        total += 1
        statuses[row["status"]] = statuses.get(row["status"], 0) + 1
        try:
            summary = json.loads(row["summary_json"] or "{}")
        except (TypeError, ValueError):
            summary = {}
        classification_summary = summary.get("classification") or {}
        observation_summary = summary.get("observation_seal") or {}
        processed = safe_int(
            classification_summary.get("scoped_rows_processed"), 0)
        selected = safe_int(
            classification_summary.get("scoped_rows_selected"), 0)
        sealed = safe_int(observation_summary.get("sealed_this_cycle"), 0)
        stale_research = bool(
            observation_summary.get("research_only_stale"))
        failure_stage = str(summary.get("failure_stage") or "")
        decision_opportunity = bool(
            not stale_research and (
                selected > 0 or sealed > 0
            or failure_stage.startswith((
                "fresh_quote_and_observation", "decision_head",
                "classification"))))
        if decision_opportunity:
            decision_opportunities += 1
        if row["status"] == "complete" and processed > 0:
            useful_completions += 1
        elif row["status"] == "complete":
            idle_completions += 1
        duration = safe_float(summary.get("duration_seconds"), 0.0)
        if duration <= 0:
            started = _timestamp(row["started_at"])
            completed = _timestamp(row["completed_at"])
            if started is not None and completed is not None:
                duration = max(0.0, completed - started)
            elif row["status"] == "deadline_exceeded":
                duration = max(
                    0.0, safe_float(row["deadline_seconds"], 0.0)
                    + LANE_TERMINATION_GRACE_SECONDS)
        if duration > 0:
            durations.append(duration)
        if row["status"] == "deadline_exceeded":
            stage = str(summary.get("failure_stage") or "unattributed")
            stage_timeouts[stage] = stage_timeouts.get(stage, 0) + 1

    def _p95(values: list[float]) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, int(round(0.95 * (len(ordered) - 1)))))
        return round(ordered[index], 3)

    model_store = store
    model_state = model_store.scheduler_state(SEAL_COST_MODEL_STATE_KEY)
    model = _seal_cost_model_from_state(model_state)
    ingestion_model = model_store.scheduler_state(
        INGESTION_COST_MODEL_STATE_KEY)
    class_cost = model_store.scheduler_state("classification_observation_cost")
    counters = model_store.scheduler_state(CLASSIFICATION_COUNTERS_STATE_KEY)
    last_summary: dict = {}
    try:
        last_summary = read_json(root / "live_lane_summary.json", {}) or {}
    except Exception:
        last_summary = {}
    timings = last_summary.get("stage_timings_seconds") or {}
    observation = last_summary.get("observation_seal") or {}
    classification = last_summary.get("classification") or {}
    return {
        "window_runs": total,
        "statuses": statuses,
        "completion_rate": (
            round(statuses.get("complete", 0) / total, 4) if total else None),
        # deadline_exceeded IS a failure here; complete/total already treats
        # it as one because only 'complete' counts toward the numerator.
        "all_attempt_p95_seconds": _p95(durations),
        "timeout_count_by_stage": stage_timeouts,
        "timeout_total": sum(stage_timeouts.values()),
        # Decision-aware acceptance. Complete-but-idle cycles are healthy
        # operationally but did not produce a decision, so they cannot be
        # called useful completions. The opportunity denominator consists of
        # attempts that selected/sealed decision work or failed inside the
        # decision path; ingestion deferrals remain a separate SLO.
        "useful_completions": useful_completions,
        "useful_completion_rate": (
            round(useful_completions / decision_opportunities, 4)
            if decision_opportunities else None),
        "productive_cycle_rate": (
            round(useful_completions / total, 4) if total else None),
        "decision_opportunities": decision_opportunities,
        "idle_completions": idle_completions,
        "controlled_deferrals": statuses.get("deferred", 0),
        "controlled_deferral_rate": (
            round(statuses.get("deferred", 0) / total, 4) if total else None),
        "uncontrolled_failures": (
            statuses.get("deadline_exceeded", 0) + statuses.get("failed", 0)
            + sum(count for status, count in statuses.items()
                  if status not in {
                      "complete", "deferred", "running", "deadline_exceeded",
                      "failed"})),
        "uncontrolled_failure_rate": None if not total else round(
            1.0 - (statuses.get("complete", 0) + statuses.get("deferred", 0))
            / total, 4),
        "controlled_deferral_total": statuses.get("deferred", 0),
        "ingestion_cost_model": {
            "p95_seconds": safe_float(
                ingestion_model.get("p95_seconds"), 14.719),
            "sample_count": len(
                ingestion_model.get("samples")
                if isinstance(ingestion_model.get("samples"), list) else []),
            "censored_samples": safe_int(
                ingestion_model.get("censored_samples"), 0),
        },
        "seal_cost_model": {
            "fixed_observation_cost_p95": safe_float(
                model.get("fixed_observation_cost_p95"),
                SEAL_FIXED_OBSERVATION_COST_SECONDS_DEFAULT),
            "queue_settlement_p95": safe_float(
                model.get("queue_settlement_p95"),
                QUEUE_SETTLEMENT_COST_SECONDS_DEFAULT),
            "per_window_cost_p95": safe_float(
                model.get("per_window_cost_p95"),
                SEAL_PER_WINDOW_COST_SECONDS_DEFAULT),
            "downstream_reserve_p95": safe_float(
                model.get("downstream_reserve_p95"),
                DOWNSTREAM_RESERVE_SECONDS_DEFAULT),
            "censored_samples": safe_int(model.get("censored_samples"), 0),
            "fixed_sample_count": safe_int(
                model.get("fixed_sample_count"), 0),
            "queue_settlement_sample_count": safe_int(
                model.get("queue_settlement_sample_count"), 0),
            "per_window_sample_count": safe_int(
                model.get("per_window_sample_count"), 0),
            "downstream_sample_count": safe_int(
                model.get("downstream_sample_count"), 0),
            "epoch": safe_int(model.get("epoch"), 0),
            "revision": str(model.get("revision") or "unknown"),
            "stall_samples": safe_int(model.get("stall_samples"), 0),
            "fixed_stall_count": safe_int(
                model.get("fixed_stall_count"), 0),
            "fixed_stall_population": safe_int(
                model.get("fixed_stall_population"), 0),
            "fixed_stall_rate": safe_float(
                model.get("fixed_stall_rate"), 0.0),
            "queue_settlement_stall_count": safe_int(
                model.get("queue_settlement_stall_count"), 0),
            "seal_stall_count": safe_int(
                model.get("seal_stall_count"), 0),
            "seal_stall_population": safe_int(
                model.get("seal_stall_population"), 0),
            "seal_stall_rate": safe_float(
                model.get("seal_stall_rate"), 0.0),
            "stall_guard_active": bool(
                model.get("stall_guard_active")),
            "stall_rate_limit": SEAL_STALL_RATE_MAX,
        },
        "classification_cost_estimate_seconds": safe_float(
            class_cost.get("per_observation_seconds"),
            CLASSIFICATION_COST_SECONDS_DEFAULT),
        "remaining_budget_at_admission_seconds": timings.get(
            "classification_remaining_at_admission"),
        "startup_phases": last_summary.get("startup_phases") or {},
        "ingestion_admission": last_summary.get("ingestion_admission") or {},
        "windows_available": observation.get("windows_available"),
        "windows_admitted": observation.get("windows_admitted"),
        "windows_deferred": observation.get("windows_deferred"),
        "windows_queued": observation.get("windows_queued"),
        "classification_selected": classification.get(
            "scoped_rows_selected"),
        "classification_processed": classification.get(
            "scoped_rows_processed"),
        "classification_deferred": classification.get(
            "scoped_rows_deferred"),
        "cumulative_classification_counters": counters,
        "oldest_deferred_observation_age_seconds": (
            max(0.0, time.time() - _timestamp(oldest_deferred))
            if _timestamp(oldest_deferred) is not None else None),
    }


def dashboard_snapshot(
    root: str | Path,
    *,
    store: RobinhoodLearningStore | None = None,
    live_position_markets: dict[str, dict] | None = None,
    market_refresh_errors: dict[str, str] | None = None,
    chain_root: str | Path | None = None,
    skill_root: str | Path | None = None,
) -> dict:
    """Build the complete blocking snapshot used by CLI exports and tests."""
    root = Path(root)
    store = store or RobinhoodLearningStore(root / "learning.sqlite3")
    snapshot = dashboard_operational_snapshot(
        root,
        store=store,
        live_position_markets=live_position_markets,
        market_refresh_errors=market_refresh_errors,
        chain_root=chain_root,
        skill_root=skill_root,
    )
    snapshot.update(dashboard_historical_snapshot(root, store=store))
    try:
        snapshot["live_lane_reliability"] = live_lane_reliability_snapshot(
            root, store=store)
    except Exception:
        # Telemetry must never take the dashboard down with it.
        snapshot["live_lane_reliability"] = {"error": "unavailable"}
    return snapshot


class RobinhoodDashboardMarketRefresher:
    """Coalesce exact-pool refreshes behind a short stale-safe cache."""

    def __init__(
        self, root: str | Path, *, engine=None, read_only: bool = False,
        chain_root: str | Path | None = None,
        skill_root: str | Path | None = None,
    ):
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
        self.chain_root = chain_root
        self.skill_root = skill_root
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
                    chain_root=self.chain_root,
                    skill_root=self.skill_root,
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
                snapshot = dashboard_snapshot(
                    self.root, store=self.store,
                    chain_root=self.chain_root, skill_root=self.skill_root)
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
                chain_root=self.chain_root,
                skill_root=self.skill_root,
            )
            snapshot["market_cache"] = {
                "refreshed_at": self._refreshed_at,
                "age_seconds": 0.0,
                "ttl_seconds": DASHBOARD_MARKET_CACHE_SECONDS,
            }
            return snapshot


def _dashboard_cached_payload(cached: dict) -> dict:
    """Compose one non-blocking response from independently refreshed caches."""
    now = time.time()
    operational = cached.get("operational")
    historical = cached.get("historical")
    payload = dict(operational or {})
    if historical:
        payload.update(historical)

    def cache_state(
        name: str, value: dict | None, built_at: str | None,
        error: str | None, stale_after: float,
    ) -> dict:
        built_epoch = _timestamp(built_at)
        age = max(0.0, now - built_epoch) if built_epoch is not None else None
        if value is None:
            status = "error" if error else "warming"
        elif error:
            status = "error"
        elif age is not None and age > stale_after:
            status = "stale"
        else:
            status = "ready"
        state = {
            "status": status,
            "data_available": value is not None,
            "built_at": built_at,
            "age_seconds": round(age, 1) if age is not None else None,
            "stale_after_seconds": stale_after,
            "last_error": error,
        }
        started_at = cached.get(f"{name}_started_at")
        if started_at:
            state["build_started_at"] = started_at
        build_seconds = cached.get(f"{name}_build_seconds")
        if build_seconds is not None:
            state["last_build_seconds"] = build_seconds
        state["building"] = bool(cached.get(f"{name}_building"))
        return state

    operational_state = cache_state(
        "operational", operational, cached.get("operational_built_at"),
        cached.get("operational_error"), DASHBOARD_OPERATIONAL_STALE_SECONDS,
    )
    historical_state = cache_state(
        "historical", historical, cached.get("historical_built_at"),
        cached.get("historical_error"), DASHBOARD_HISTORICAL_STALE_SECONDS,
    )
    payload.update({
        "warming": historical is None,
        "response_at": _utc_now(),
        "snapshot_built_at": cached.get("historical_built_at"),
        "operational_state": operational_state,
        "historical_state": historical_state,
        "paper_only": True,
        "live_execution_enabled": False,
    })
    if historical is None:
        payload["detail"] = (
            "Operational data is available. Historical analytics are still building."
            if operational is not None else "Dashboard snapshots are still building."
        )
    elif historical_state["status"] == "stale":
        payload["detail"] = "Historical analytics are stale; the last completed snapshot is shown."
    elif historical_state["status"] == "error":
        payload["detail"] = "The last historical snapshot is shown; its refresh failed."
    else:
        payload.pop("detail", None)
    return payload


def serve_dashboard(
    root: str | Path, host: str, port: int, *,
    chain_root: str | Path | None = None,
    skill_root: str | Path | None = None,
) -> None:
    if host not in {"127.0.0.1","localhost"}:
        raise ValueError("Robinhood dashboard is local-only")
    html_path=Path(__file__).with_name("robinhood_dashboard.html")
    refresher=RobinhoodDashboardMarketRefresher(
        root, read_only=True, chain_root=chain_root, skill_root=skill_root)
    initial_integrity = _dashboard_integrity(
        root, chain_root=chain_root, skill_root=skill_root)
    operational_started = time.monotonic()
    initial_operational = dashboard_operational_snapshot(
        root,
        store=refresher.store,
        chain_root=chain_root,
        skill_root=skill_root,
        integrity=initial_integrity,
    )
    operational_built_at = _utc_now()
    cached: dict = {
        "operational": initial_operational,
        "operational_built_at": operational_built_at,
        "operational_build_seconds": round(
            time.monotonic() - operational_started, 1),
        "operational_error": None,
        "operational_building": False,
        "operational_started_at": None,
        "historical": None,
        "historical_built_at": None,
        "historical_build_seconds": None,
        "historical_error": None,
        "historical_building": False,
        "historical_started_at": None,
    }
    cache_lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in {"/","/index.html"}:
                content=html_path.read_bytes(); content_type="text/html; charset=utf-8"
            elif self.path=="/api/status":
                # Never block on SQLite aggregation. Operational and historical
                # state have separate caches, freshness and failure semantics.
                with cache_lock:
                    payload = _dashboard_cached_payload(dict(cached))
                content=json.dumps(payload).encode(); content_type="application/json"
            else:
                self.send_error(404); return
            self.send_response(200); self.send_header("Content-Type",content_type)
            self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(content)))
            self.end_headers(); self.wfile.write(content)
        def log_message(self, *_args):
            return
    # The historical snapshot costs minutes: flow_summary and
    # pending_transaction_origin_counts range-join 487k swap rows against
    # 2,176 signal windows on a BETWEEN, which no index serves. Computing that
    # on the request path made /api/status time out in the browser, so it is
    # computed off the request path instead and every request is served the
    # last completed build. Stale by up to a refresh interval, never hanging.
    def rebuild_operational() -> None:
        while True:
            time.sleep(DASHBOARD_OPERATIONAL_REFRESH_SECONDS)
            started_at = _utc_now()
            with cache_lock:
                cached["operational_building"] = True
                cached["operational_started_at"] = started_at
            try:
                started = time.monotonic()
                integrity = _dashboard_integrity(
                    root, chain_root=chain_root, skill_root=skill_root)
                payload = dashboard_operational_snapshot(
                    root,
                    store=refresher.store,
                    chain_root=chain_root,
                    skill_root=skill_root,
                    integrity=integrity,
                )
                with cache_lock:
                    cached["operational"] = payload
                    cached["operational_built_at"] = _utc_now()
                    cached["operational_build_seconds"] = round(
                        time.monotonic() - started, 1)
                    cached["operational_error"] = None
            except Exception as error:
                with cache_lock:
                    cached["operational_error"] = str(error)[:200]
            finally:
                with cache_lock:
                    cached["operational_building"] = False
                    cached["operational_started_at"] = None

    def rebuild_historical() -> None:
        while True:
            started_at = _utc_now()
            with cache_lock:
                cached["historical_building"] = True
                cached["historical_started_at"] = started_at
            try:
                started = time.monotonic()
                payload = dashboard_historical_snapshot(
                    root, store=refresher.store)
                with cache_lock:
                    cached["historical"] = payload
                    cached["historical_built_at"] = _utc_now()
                    cached["historical_build_seconds"] = round(
                        time.monotonic() - started, 1)
                    cached["historical_error"] = None
            except Exception as error:
                with cache_lock:
                    cached["historical_error"] = str(error)[:200]
            finally:
                with cache_lock:
                    cached["historical_building"] = False
                    cached["historical_started_at"] = None
            time.sleep(DASHBOARD_SNAPSHOT_REFRESH_SECONDS)

    threading.Thread(target=rebuild_operational, daemon=True).start()
    threading.Thread(target=rebuild_historical, daemon=True).start()
    class _ExclusiveServer(ThreadingHTTPServer):
        """Refuse to share the port instead of silently joining it.

        HTTPServer sets allow_reuse_address, and on Windows SO_REUSEADDR lets
        SEVERAL sockets bind one listening port -- connections then go to an
        arbitrary one. Three instances were bound to 8769 at once, including
        one started three days earlier, so every "restart" appeared to succeed
        while requests kept being answered by pre-lane code. Hours went into
        diagnosing a payload that was missing keys the running code emitted.

        Binding exclusively turns that into an immediate "address already in
        use", which names the problem at the moment it happens.
        """

        allow_reuse_address = False

    try:
        server = _ExclusiveServer((host, port), Handler)
    except OSError as error:
        raise SystemExit(
            f"Port {port} is already served by another dashboard process "
            f"({error}). Stop it first -- do not start a second one, because "
            f"both would appear to work."
        ) from error
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
            "learn-once", "marks-once", "live-once", "evidence-once",
            "analysis-once", "backfill-once", "verification-once",
            "full-verification-once",
            "lanes", "status", "dashboard", "verify", "reflect",
            "audit", "repair-outcomes", "cohort-start", "cohort-status",
        ),
    )
    parser.add_argument(
        "--marks-cadence-seconds", type=float,
        default=MARKS_LANE_CADENCE_SECONDS)
    parser.add_argument(
        "--evidence-cadence-seconds", type=float,
        default=EVIDENCE_LANE_CADENCE_SECONDS)
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
    parser.add_argument("--lane-budget-seconds", type=float, default=None)
    parser.add_argument(
        "--backfill-identity-limit", type=int,
        default=BACKFILL_LANE_IDENTITY_LIMIT)
    parser.add_argument("--duration-seconds", type=float, default=285.0)
    parser.add_argument(
        "--live-cadence-seconds", type=float,
        default=LIVE_LANE_CADENCE_SECONDS)
    parser.add_argument(
        "--analysis-cadence-seconds", type=float,
        default=ANALYSIS_LANE_CADENCE_SECONDS)
    parser.add_argument(
        "--backfill-cadence-seconds", type=float,
        default=BACKFILL_LANE_CADENCE_SECONDS)
    parser.add_argument("--lookback",type=int,default=DEFAULT_DISCOVERY_LOOKBACK_BLOCKS)
    parser.add_argument("--chain-root",default=DEFAULT_CHAIN_ROOT)
    parser.add_argument("--cohort-target",type=int,default=100)
    parser.add_argument("--cohort-id",default=None)
    parser.add_argument("--cohort-revision",default=CODE_REVISION)
    parser.add_argument(
        "--todo", default=str(Path(__file__).with_name("TODO.md"))
    )
    parser.add_argument("--skill-root",default=str(default_skill_root()))
    args=parser.parse_args()
    if args.command=="dashboard":
        serve_dashboard(
            args.root,args.host,args.port,
            chain_root=args.chain_root,skill_root=args.skill_root); return
    if args.command=="status":
        print(json.dumps(dashboard_snapshot(
            args.root,chain_root=args.chain_root,skill_root=args.skill_root
        ),indent=2)); return
    if args.command in {"cohort-start", "cohort-status"}:
        store = RobinhoodLearningStore(Path(args.root) / "learning.sqlite3")
        if args.command == "cohort-start":
            result = store.start_acceptance_cohort(
                revision=args.cohort_revision,
                sample_target=max(1, args.cohort_target),
                cohort_id=args.cohort_id,
            )
        else:
            result = store.acceptance_cohort()
        print(json.dumps(result, indent=2)); return
    if args.command=="lanes":
        result = supervise_lanes(
            args.root, chain_root=args.chain_root,
            skill_root=args.skill_root,
            duration_seconds=max(5.0, args.duration_seconds),
            discovery_block_limit=max(1, args.discovery_block_limit),
            analysis_limit=max(0, args.analysis_limit),
            outcome_limit=max(0, args.outcome_limit),
            outcome_recovery_limit=max(0, args.outcome_recovery_limit),
            market_recheck_limit=max(0, args.market_recheck_limit),
            live_cadence_seconds=max(10.0, args.live_cadence_seconds),
            marks_cadence_seconds=max(
                15.0, getattr(args, "marks_cadence_seconds", None)
                or MARKS_LANE_CADENCE_SECONDS),
            evidence_cadence_seconds=max(
                30.0, getattr(args, "evidence_cadence_seconds", None)
                or EVIDENCE_LANE_CADENCE_SECONDS),
            analysis_cadence_seconds=max(30.0, args.analysis_cadence_seconds),
            backfill_cadence_seconds=max(60.0, args.backfill_cadence_seconds),
        )
        print(json.dumps(result, indent=2)); return
    producer_chain = (
        args.chain_root
        if args.command in {
            "learn-once", "analysis-once", "verify", "repair-outcomes",
            "reflect", "audit", "verification-once",
            "full-verification-once",
        }
        else None
    )
    engine_initialization_started = time.monotonic()
    backfill_rpc_url = (
        str(os.environ.get(BACKFILL_RPC_URL_ENV) or "").strip()
        if args.command == "backfill-once" else "")
    lane_rpc = (
        RobinhoodRPC(backfill_rpc_url) if backfill_rpc_url else None)
    engine=RobinhoodLearningEngine(
        args.root, rpc=lane_rpc,
        chain_root=producer_chain, skill_root=args.skill_root,
        rpc_isolated=bool(backfill_rpc_url),
    )
    engine._startup_milestones = {
        "shared_deadline": bool(safe_float(
            os.environ.get("CHAINSEER_LANE_DEADLINE_MONOTONIC"), 0.0) > 0),
        "module_import_started": PROCESS_MODULE_IMPORT_STARTED_MONOTONIC,
        "imports_completed": PROCESS_IMPORTS_COMPLETED_MONOTONIC,
        "engine_initialization_started": engine_initialization_started,
        "engine_initialization_completed": time.monotonic(),
    }
    if args.command=="verify":
        result=engine.verify(); print(json.dumps(result,indent=2)); raise SystemExit(0 if result["ok"] else 1)
    if args.command=="verification-once":
        summary = engine.run_verification_lane(
            budget_seconds=max(
                30.0, args.lane_budget_seconds
                or VERIFICATION_LANE_BUDGET_SECONDS))
        print(json.dumps(summary, indent=2)); return
    if args.command=="full-verification-once":
        summary = engine.run_full_verification_lane(
            budget_seconds=max(
                30.0, args.lane_budget_seconds
                or FULL_VERIFICATION_LANE_BUDGET_SECONDS))
        print(json.dumps(summary, indent=2)); return
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
    if args.command=="marks-once":
        summary = engine.run_marks_lane(
            budget_seconds=max(
                5.0, args.lane_budget_seconds or MARKS_LANE_BUDGET_SECONDS))
        print(json.dumps(summary, indent=2)); return
    if args.command=="live-once":
        summary = engine.run_live_lane(
            budget_seconds=max(
                5.0, args.lane_budget_seconds or LIVE_LANE_BUDGET_SECONDS))
        print(json.dumps(summary, indent=2)); return
    if args.command=="evidence-once":
        summary = engine.run_evidence_lane(
            budget_seconds=max(
                30.0, args.lane_budget_seconds
                or EVIDENCE_LANE_BUDGET_SECONDS))
        print(json.dumps(summary, indent=2)); return
    if args.command=="analysis-once":
        summary = engine.run_analysis_lane(
            budget_seconds=max(
                30.0, args.lane_budget_seconds or ANALYSIS_LANE_BUDGET_SECONDS),
            analysis_limit=max(0, args.analysis_limit),
            outcome_limit=max(0, args.outcome_limit),
            outcome_recovery_limit=max(0, args.outcome_recovery_limit),
            market_recheck_limit=max(0, args.market_recheck_limit),
        )
        try:
            summary["reflection"] = reflection.run_if_due()
            summary["flow_reflection"] = reflection.run_flow_if_due()
        except Exception as exc:
            summary["reflection"] = {
                "status": "retry_pending", "error": str(exc)}
        print(json.dumps(summary, indent=2)); return
    if args.command=="backfill-once":
        summary = engine.run_backfill_lane(
            budget_seconds=max(
                30.0, args.lane_budget_seconds or BACKFILL_LANE_BUDGET_SECONDS),
            discovery_block_limit=max(1, args.discovery_block_limit),
            identity_limit=max(0, args.backfill_identity_limit),
            lookback=max(1, args.lookback),
        )
        print(json.dumps(summary, indent=2)); return
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
