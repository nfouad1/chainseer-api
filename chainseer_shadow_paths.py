"""Prospective exact-block shadow paths and exit-policy evaluation.

The collector lives in :mod:`chainseer_robinhood`; this module owns the frozen
experiment definition and the offline evaluator.  It deliberately contains no
execution, signing, policy-mutation, or network path.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import statistics
import time
from pathlib import Path
from typing import Any

from chainseer_core import atomic_json_write


POLICY_VERSION = "flow-shadow-path-v1"
ARTIFACT_NAME = "flow_shadow_exit_v1.json"
LEDGER_NAME = "flow_shadow_exit_v1.sqlite3"
GENESIS_HASH = "0" * 64

# Pair-aware deterministic sampling keeps the additional archive-quote promise
# below measured evidence-lane capacity.  Changing it is a new experiment, not
# a runtime tuning knob, so it is covered by the policy hash.
SAMPLE_BASIS_POINTS = 2_000
SAMPLE_BUCKETS = 10_000
BLOCKS_PER_SECOND = 10.0
FINALITY_BLOCKS = 20
MARKS_PER_EVIDENCE_CYCLE = 4
RETRY_SECONDS = 15 * 60.0
FRICTION_BPS = 100.0

# Freeze the probe in anchor base units at signal capture.  Resolving an old
# entry with a current ETH/USD quote would otherwise let future information
# change position size. RPC-verified decimals: WETH=18 and USDG=6.
# A later anchor family requires a new policy version.
ENTRY_ANCHOR_IN_RAW = {
    "wrapped_native": "30000000000000000",   # 0.03 WETH
    "stable": "100000000",                  # 100 USDG
}

# Dense where launch paths move fastest, sparse over the long hold.  These are
# exact BLOCK checkpoints; the labels are nominal wall-clock equivalents at
# the frozen ten-block/second convention.  The evaluator never claims a touch
# between two checkpoints.
SCHEDULE = (
    ("entry", 0),
    ("15s", 15),
    ("30s", 30),
    ("1m", 60),
    ("2m", 2 * 60),
    ("3m", 3 * 60),
    ("5m", 5 * 60),
    ("8m", 8 * 60),
    ("12m", 12 * 60),
    ("15m", 15 * 60),
    ("30m", 30 * 60),
    ("1h", 60 * 60),
    ("2h", 2 * 60 * 60),
    ("6h", 6 * 60 * 60),
    ("12h", 12 * 60 * 60),
    ("24h", 24 * 60 * 60),
    ("3d", 3 * 24 * 60 * 60),
    ("7d", 7 * 24 * 60 * 60),
)

MINIMUM_TRAIN_PATHS = 100
MINIMUM_HOLDOUT_PATHS = 50
MINIMUM_PAIRED_HOLDOUT_PATHS = 30
MAXIMUM_NONEXIT_RATE = 0.10
MAXIMUM_CATASTROPHIC_RATE = 0.20
MAXIMUM_RETURN = 10.0

STOP_LOSS_MULTIPLE = 0.65
PRICE_COLLAPSE_MULTIPLE = 0.02
STAGE_ONE_MULTIPLE = 2.0
STAGE_TWO_MULTIPLE = 3.0
STAGE_ONE_FRACTION = 0.50
STAGE_TWO_FRACTION = 0.25
TRAILING_DRAWDOWN = 0.35
TRAILING_ACTIVATION_MULTIPLE = 1.25
STAGNATION_MULTIPLE = 1.25

POLICIES = {
    "hold_15m": {
        "kind": "hold", "terminal_label": "15m",
        "description": "Exit the whole position at the exact 15m block checkpoint.",
    },
    "staged_2x_3x_runner_7d": {
        "kind": "staged_runner", "terminal_label": "7d",
        "description": (
            "Sell 50% at the first observed >=2x checkpoint, 25% at >=3x, "
            "then trail the runner by 35%; 0.65x stop, 24h stagnation, 7d cap."
        ),
    },
    "fixed_3x_7d": {
        "kind": "fixed_3x", "terminal_label": "7d",
        "description": "Exit all at the first observed >=3x checkpoint; 0.65x stop and 7d cap.",
    },
    "pure_trailing_7d": {
        "kind": "pure_trailing", "terminal_label": "7d",
        "description": (
            "After a first observed >=1.25x checkpoint, exit on a 35% drawdown; "
            "0.65x stop and 7d cap."
        ),
    },
}


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def policy_definition() -> dict:
    return {
        "schema_version": 1,
        "policy_version": POLICY_VERSION,
        "population": "prospective_flow_signal_events",
        "selection": {
            "rule": "sha256(policy_version:qualified_event_id) modulo 10000",
            "sample_basis_points": SAMPLE_BASIS_POINTS,
            "controls_inherit_signal_selection": True,
        },
        "source": {
            "supported_source_version": "uniswap_v4",
            "entry_anchor_in_raw": ENTRY_ANCHOR_IN_RAW,
            "entry_notional_is_frozen_before_quote_resolution": True,
        },
        "clock": {
            "kind": "exact_block",
            "entry_block": "flow_signal_event.head_block",
            "blocks_per_second": BLOCKS_PER_SECOND,
            "finality_blocks": FINALITY_BLOCKS,
            "wall_clock_labels_are_nominal": True,
        },
        "schedule": [
            {
                "step_index": index,
                "label": label,
                "nominal_offset_seconds": seconds,
                "offset_blocks": int(round(seconds * BLOCKS_PER_SECOND)),
            }
            for index, (label, seconds) in enumerate(SCHEDULE)
        ],
        "measurement": {
            "entry": "archive execution quote at event head block",
            "exit": "archive full-position exit quote at exact target block",
            "friction_bps": FRICTION_BPS,
            "provider_failures_are_not_market_evidence": True,
            "unexitable_exit_return": -1.0,
            "checkpoint_touch_claim_only": True,
            "strategy_trigger_basis": (
                "block-pinned executable liquidation multiple after friction"
            ),
            "spot_price_and_usd_liquidity_are_diagnostic_only": True,
        },
        "evaluation": {
            "policy_family": POLICIES,
            "exit_parameters": {
                "stop_loss_multiple": STOP_LOSS_MULTIPLE,
                "price_collapse_multiple": PRICE_COLLAPSE_MULTIPLE,
                "stage_one_multiple": STAGE_ONE_MULTIPLE,
                "stage_two_multiple": STAGE_TWO_MULTIPLE,
                "stage_one_fraction": STAGE_ONE_FRACTION,
                "stage_two_fraction": STAGE_TWO_FRACTION,
                "trailing_drawdown": TRAILING_DRAWDOWN,
                "trailing_activation_multiple": TRAILING_ACTIVATION_MULTIPLE,
                "stagnation_multiple": STAGNATION_MULTIPLE,
            },
            "partial_exit_model": "pro rata full-position liquidation quote",
            "modeled_friction_is_not_a_measured_gas_or_fill_cost": True,
            "selection_freeze": (
                "select once from the first 100 complete qualified paths; "
                "seal the policy before any holdout path is born"
            ),
            "forward_pool_disjoint_holdout": True,
            "holdout_admission": (
                "signaled_at strictly after selection frozen_at and pool absent "
                "from the frozen training set"
            ),
            "minimum_train_paths": MINIMUM_TRAIN_PATHS,
            "minimum_holdout_paths": MINIMUM_HOLDOUT_PATHS,
            "minimum_paired_holdout_paths": MINIMUM_PAIRED_HOLDOUT_PATHS,
            "maximum_nonexit_rate": MAXIMUM_NONEXIT_RATE,
            "maximum_catastrophic_rate": MAXIMUM_CATASTROPHIC_RATE,
            "return_bounds": [-1.0, MAXIMUM_RETURN],
        },
        "governance": {
            "shadow_only": True,
            "promotion_enabled": False,
            "live_execution_enabled": False,
            "source_admission_unchanged": True,
        },
    }


def policy_json() -> str:
    return canonical(policy_definition())


def policy_hash() -> str:
    return hashlib.sha256(policy_json().encode("utf-8")).hexdigest()


def sample_bucket(qualified_event_id: str) -> int:
    material = f"{POLICY_VERSION}:{qualified_event_id}".encode("utf-8")
    return int(hashlib.sha256(material).hexdigest(), 16) % SAMPLE_BUCKETS


def selected(qualified_event_id: str) -> bool:
    return sample_bucket(qualified_event_id) < SAMPLE_BASIS_POINTS


def path_id(event_id: str) -> str:
    return hashlib.sha256(f"{POLICY_VERSION}:{event_id}".encode("utf-8")).hexdigest()


def schedule_rows(path: str, entry_block: int, signaled_at: float) -> list[dict]:
    rows = []
    definition_hash = policy_hash()
    for index, (label, seconds) in enumerate(SCHEDULE):
        row = {
            "path_id": path,
            "step_index": index,
            "label": label,
            "nominal_offset_seconds": seconds,
            "offset_blocks": int(round(seconds * BLOCKS_PER_SECOND)),
            "target_block": int(entry_block)
            + int(round(seconds * BLOCKS_PER_SECOND)),
            "target_at": float(signaled_at) + seconds,
            "kind": "entry" if index == 0 else "exit",
            "policy_hash": definition_hash,
        }
        row["schedule_hash"] = hashlib.sha256(
            canonical(row).encode("utf-8")
        ).hexdigest()
        rows.append(row)
    return rows


def measurement_hash(previous_hash: str, schedule_hash: str, payload: dict) -> str:
    material = previous_hash + schedule_hash + canonical(payload)
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def _safe_float(value: Any, default: float | None = 0.0) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    return number if math.isfinite(number) else default


def _percentile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def _mean_lower_95(values: list[float]) -> float | None:
    if not values:
        return None
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean
    standard_error = statistics.stdev(values) / math.sqrt(len(values))
    return mean - 1.96 * standard_error


def _metrics(results: list[dict]) -> dict:
    values = [
        max(-1.0, min(MAXIMUM_RETURN, float(row["net_return"])))
        for row in results
    ]
    if not values:
        return {
            "samples": 0, "pools": 0, "mean_return": None,
            "median_return": None, "mean_return_lower_95": None,
            "loss_rate": None, "non_exit_rate": None,
            "catastrophic_rate": None, "objective": None,
        }
    nonexit = sum(bool(row.get("non_exit")) for row in results) / len(results)
    catastrophic = sum(value <= -0.8 for value in values) / len(values)
    mean = statistics.fmean(values)
    return {
        "samples": len(values),
        "pools": len({row["pool_id"] for row in results}),
        "mean_return": round(mean, 6),
        "median_return": round(statistics.median(values), 6),
        "mean_return_lower_95": round(_mean_lower_95(values), 6),
        "loss_rate": round(sum(value < 0 for value in values) / len(values), 6),
        "non_exit_rate": round(nonexit, 6),
        "catastrophic_rate": round(catastrophic, 6),
        "objective": round(mean - nonexit - catastrophic, 6),
    }


def _paired_metrics(signal_results: list[dict], controls: dict[str, dict]) -> dict:
    differences = []
    for row in signal_results:
        matched = row.get("matched_path_id")
        control = controls.get(str(matched or ""))
        if control is not None:
            differences.append(row["net_return"] - control["net_return"])
    return {
        "samples": len(differences),
        "mean_incremental_return": (
            round(statistics.fmean(differences), 6) if differences else None),
        "median_incremental_return": (
            round(statistics.median(differences), 6) if differences else None),
        "mean_incremental_lower_95": (
            round(_mean_lower_95(differences), 6) if differences else None),
    }


def _terminal_index(label: str) -> int:
    labels = [name for name, _seconds in SCHEDULE]
    return labels.index(label)


def _simulate(path: dict, policy_name: str) -> dict | None:
    definition = POLICIES[policy_name]
    terminal_index = _terminal_index(definition["terminal_label"])
    marks = path["marks"]
    if any(index not in marks for index in range(terminal_index + 1)):
        return None
    entry = marks[0]
    if entry["status"] != "observed" or not entry["exit_valid"]:
        return None

    if definition["kind"] == "hold":
        mark = marks[terminal_index]
        value = 0.0 if not mark["exit_valid"] else 1.0 + mark["net_return"]
        return {
            "path_id": path["path_id"], "pool_id": path["pool_id"],
            "matched_path_id": path.get("matched_path_id"),
            "net_return": max(-1.0, min(MAXIMUM_RETURN, value - 1.0)),
            "non_exit": not mark["exit_valid"],
            "exit_reason": "hold_checkpoint" if mark["exit_valid"] else "non_exitable",
        }

    remaining = 1.0
    realised = 0.0
    high_price = 1.0
    stage_one = False
    stage_two = False
    non_exit = False
    exit_reason = None
    for index in range(1, terminal_index + 1):
        mark = marks[index]
        if not mark["exit_valid"]:
            non_exit = True
            remaining = 0.0
            exit_reason = "non_exitable"
            break
        liquidation_multiple = max(0.0, 1.0 + mark["net_return"])
        # Policy triggers use the executable liquidation quote, not a spot
        # price estimate.  Historical native/USD conversion is not block
        # pinned, while anchor_out/anchor_in is; using the latter keeps the
        # strategy claim inside the exact-block evidence boundary.
        trigger_multiple = liquidation_multiple
        high_price = max(high_price, trigger_multiple)
        if trigger_multiple <= PRICE_COLLAPSE_MULTIPLE:
            realised += remaining * liquidation_multiple
            remaining = 0.0
            exit_reason = "price_collapse"
            break
        if realised + remaining * liquidation_multiple <= STOP_LOSS_MULTIPLE:
            realised += remaining * liquidation_multiple
            remaining = 0.0
            exit_reason = "stop_loss"
            break
        kind = definition["kind"]
        if kind == "fixed_3x" and trigger_multiple >= STAGE_TWO_MULTIPLE:
            realised += remaining * liquidation_multiple
            remaining = 0.0
            exit_reason = "fixed_take_profit_3x"
            break
        if kind == "pure_trailing":
            if (high_price >= TRAILING_ACTIVATION_MULTIPLE
                    and trigger_multiple <= high_price * (1 - TRAILING_DRAWDOWN)):
                realised += remaining * liquidation_multiple
                remaining = 0.0
                exit_reason = "pure_trailing_stop"
                break
        if kind == "staged_runner":
            if not stage_one and trigger_multiple >= STAGE_ONE_MULTIPLE:
                realised += STAGE_ONE_FRACTION * liquidation_multiple
                remaining -= STAGE_ONE_FRACTION
                stage_one = True
            if not stage_two and trigger_multiple >= STAGE_TWO_MULTIPLE:
                sold = min(remaining, STAGE_TWO_FRACTION)
                realised += sold * liquidation_multiple
                remaining -= sold
                stage_two = True
            if (stage_two
                    and trigger_multiple <= high_price * (1 - TRAILING_DRAWDOWN)):
                realised += remaining * liquidation_multiple
                remaining = 0.0
                exit_reason = "runner_trailing_stop"
                break
            if (mark["label"] == "24h"
                    and high_price < STAGNATION_MULTIPLE):
                realised += remaining * liquidation_multiple
                remaining = 0.0
                exit_reason = "stagnation_24h"
                break

        if index == terminal_index:
            realised += remaining * liquidation_multiple
            remaining = 0.0
            exit_reason = "maximum_hold"
            break

    total = max(0.0, realised)
    return {
        "path_id": path["path_id"], "pool_id": path["pool_id"],
        "matched_path_id": path.get("matched_path_id"),
        "net_return": max(-1.0, min(MAXIMUM_RETURN, total - 1.0)),
        "non_exit": non_exit, "exit_reason": exit_reason,
    }


def verify_measurement_ledger(
    database_path: str | Path, *, connection: sqlite3.Connection | None = None,
) -> dict:
    path = Path(database_path)
    if not path.exists():
        return {"ok": True, "records": 0, "head": GENESIS_HASH,
                "status": "not_initialized"}
    own_connection = connection is None
    connection = connection or sqlite3.connect(
        f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    if own_connection:
        connection.execute("BEGIN")
    try:
        tables = {row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        required = {
            "flow_shadow_paths", "flow_shadow_path_schedule",
            "flow_shadow_path_measurements", "flow_shadow_path_attempts",
        }
        if not required.issubset(tables):
            return {"ok": True, "records": 0, "head": GENESIS_HASH,
                    "status": "not_initialized"}
        paths = [dict(row) for row in connection.execute(
            "SELECT * FROM flow_shadow_paths ORDER BY created_at,path_id")]
        schedules = [dict(row) for row in connection.execute(
            "SELECT * FROM flow_shadow_path_schedule ORDER BY path_id,step_index")]
        measurements = [dict(row) for row in connection.execute(
            "SELECT * FROM flow_shadow_path_measurements ORDER BY sequence")]
        orphan_paths = connection.execute(
            """
            SELECT COUNT(*) FROM flow_shadow_paths p
            LEFT JOIN flow_signal_events e ON e.event_id=p.event_id
            WHERE e.event_id IS NULL
            """).fetchone()[0]
        path_event_mismatches = connection.execute(
            """
            SELECT COUNT(*) FROM flow_shadow_paths p
            JOIN flow_signal_events e ON e.event_id=p.event_id
            WHERE p.entry_block<>e.head_block
               OR p.cohort_id<>e.cohort_id
               OR p.source_version<>e.source_version
               OR p.pool_id<>e.pool_id
               OR p.token_address<>e.token_address
               OR p.signal_role<>e.signal_role
               OR p.arm<>e.arm
               OR e.eligible_for_evaluation<>1
            """).fetchone()[0]
        orphan_schedules = connection.execute(
            """
            SELECT COUNT(*) FROM flow_shadow_path_schedule s
            LEFT JOIN flow_shadow_paths p ON p.path_id=s.path_id
            WHERE p.path_id IS NULL
            """).fetchone()[0]
        orphan_measurements = connection.execute(
            """
            SELECT COUNT(*) FROM flow_shadow_path_measurements m
            LEFT JOIN flow_shadow_path_schedule s
              ON s.path_id=m.path_id AND s.step_index=m.step_index
            WHERE s.path_id IS NULL OR m.schedule_hash<>s.schedule_hash
            """).fetchone()[0]
        integrity_failures = connection.execute(
            "SELECT COUNT(*) FROM flow_shadow_path_attempts"
            " WHERE failure_class='evidence_integrity_failure'"
        ).fetchone()[0]
    finally:
        if own_connection:
            connection.close()

    if orphan_paths:
        return {"ok": False, "reason": "path_event_orphan",
                "orphan_paths": int(orphan_paths)}
    if path_event_mismatches:
        return {"ok": False, "reason": "path_event_binding_mismatch",
                "mismatches": int(path_event_mismatches)}
    if orphan_schedules:
        return {"ok": False, "reason": "schedule_path_orphan",
                "orphan_schedules": int(orphan_schedules)}
    if orphan_measurements:
        return {"ok": False, "reason": "measurement_schedule_mismatch",
                "orphan_measurements": int(orphan_measurements)}
    if integrity_failures:
        return {"ok": False, "reason": "collector_integrity_failure",
                "failures": int(integrity_failures)}
    path_index = {str(row["path_id"]): row for row in paths}
    schedules_by_path: dict[str, list[dict]] = {}
    for row in schedules:
        schedules_by_path.setdefault(str(row["path_id"]),[]).append(row)
    for row in paths:
        try:
            definition = json.loads(str(row["policy_json"]))
        except (TypeError, ValueError, json.JSONDecodeError):
            return {"ok": False, "reason": "path_policy_json_invalid",
                    "path_id": row["path_id"]}
        if hashlib.sha256(str(row["policy_json"]).encode("utf-8")).hexdigest() != row["policy_hash"]:
            return {"ok": False, "reason": "path_policy_hash_mismatch",
                    "path_id": row["path_id"]}
        if (definition.get("policy_version") != row["policy_version"]
                or row["policy_hash"] != policy_hash()
                or str(row["policy_json"]) != policy_json()
                or row["path_id"] != path_id(row["event_id"])
                or int(row["sample_bucket"])
                    != sample_bucket(row["sampled_by_event_id"])
                or int(row["sample_bucket"]) >= SAMPLE_BASIS_POINTS):
            return {"ok": False, "reason": "path_membership_mismatch",
                    "path_id": row["path_id"]}
        if (row["source_version"] != "uniswap_v4"
                or row["entry_anchor_kind"] not in ENTRY_ANCHOR_IN_RAW
                or str(row["entry_anchor_in_raw"])
                != ENTRY_ANCHOR_IN_RAW.get(row["entry_anchor_kind"])):
            return {"ok": False, "reason": "entry_notional_binding_mismatch",
                    "path_id": row["path_id"]}
        matched_path_id = str(row.get("matched_path_id") or "")
        if matched_path_id:
            matched = path_index.get(matched_path_id)
            if (matched is None
                    or str(matched.get("matched_path_id") or "") != row["path_id"]
                    or matched["signal_role"] == row["signal_role"]
                    or matched["sampled_by_event_id"] != row["sampled_by_event_id"]
                    or int(matched["sample_bucket"]) != int(row["sample_bucket"])):
                return {"ok": False,
                        "reason": "matched_path_binding_mismatch",
                        "path_id": row["path_id"]}
        expected_schedule = schedule_rows(
            row["path_id"],int(row["entry_block"]),float(row["signaled_at"]))
        actual_schedule = schedules_by_path.get(str(row["path_id"]),[])
        if len(actual_schedule) != len(expected_schedule):
            return {"ok": False, "reason": "path_schedule_incomplete",
                    "path_id": row["path_id"],
                    "actual": len(actual_schedule),
                    "expected": len(expected_schedule)}
        for actual, expected_row in zip(actual_schedule,expected_schedule):
            for key in (
                    "step_index","label","nominal_offset_seconds",
                    "offset_blocks","target_block","target_at","kind",
                    "policy_hash","schedule_hash"):
                if actual[key] != expected_row[key]:
                    return {
                        "ok": False, "reason": "schedule_hash_mismatch",
                        "path_id": row["path_id"],
                        "step_index": actual["step_index"], "field": key,
                    }
    schedule_index = {
        (str(row["path_id"]),int(row["step_index"])): row
        for row in schedules
    }
    previous = GENESIS_HASH
    for row in measurements:
        schedule = schedule_index.get(
            (str(row["path_id"]),int(row["step_index"])))
        status = str(row["status"])
        if schedule is None or status not in {
                "observed","non_exitable","entry_unmarketable"}:
            return {"ok": False, "reason": "measurement_semantics_invalid",
                    "sequence": row["sequence"]}
        quoted = not (
            status == "entry_unmarketable" and int(row["step_index"]) > 0)
        if quoted:
            try:
                quote_payload = json.loads(row["quote_json"])
            except (TypeError,ValueError,json.JSONDecodeError):
                return {"ok": False, "reason": "measurement_quote_invalid",
                        "sequence": row["sequence"]}
            try:
                payload_block = int(quote_payload.get("quote_block"))
            except (TypeError,ValueError):
                payload_block = -1
            if (int(row["quote_block"] or -1) != int(schedule["target_block"])
                    or payload_block != int(schedule["target_block"])):
                return {"ok": False, "reason": "measurement_block_mismatch",
                        "sequence": row["sequence"]}
            if int(row["step_index"]) == 0:
                entry_quote = dict(quote_payload.get("execution_quote") or {})
                if entry_quote.get("verified"):
                    try:
                        entry_block = int(entry_quote.get("quote_block"))
                    except (TypeError,ValueError):
                        entry_block = -1
                    if entry_block != int(schedule["target_block"]):
                        return {
                            "ok": False,
                            "reason": "entry_quote_block_mismatch",
                            "sequence": row["sequence"],
                        }
                    if str(entry_quote.get("anchor_in_raw")) != str(
                            path_index[str(row["path_id"])]["entry_anchor_in_raw"]):
                        return {"ok": False, "reason": "entry_quote_notional_mismatch",
                                "sequence": row["sequence"]}
        elif row["quote_block"] is not None or row["quote_json"] is not None:
            return {"ok": False, "reason": "cascade_contains_quote",
                    "sequence": row["sequence"]}
        if status == "observed" and not (
                bool(row["quote_verified"]) and bool(row["exit_valid"])):
            return {"ok": False, "reason": "observed_quote_not_executable",
                    "sequence": row["sequence"]}
        if status == "non_exitable" and (
                bool(row["exit_valid"])
                or _safe_float(row["net_return"],None) != -1.0):
            return {"ok": False, "reason": "nonexit_return_invalid",
                    "sequence": row["sequence"]}
        if status == "entry_unmarketable" and (
                bool(row["exit_valid"]) or row["net_return"] is not None
                and int(row["step_index"]) > 0):
            return {"ok": False, "reason": "entry_attrition_invalid",
                    "sequence": row["sequence"]}
        payload = {
            key: row[key] for key in (
                "path_id", "step_index", "status", "observed_at",
                "quote_block", "quote_json", "quote_verified", "exit_valid",
                "net_return", "price_multiple", "liquidity_usd", "error",
            )
        }
        expected = measurement_hash(previous, row["schedule_hash"], payload)
        if row["previous_hash"] != previous or row["record_hash"] != expected:
            return {"ok": False, "reason": "measurement_chain_mismatch",
                    "sequence": row["sequence"], "head": previous}
        previous = row["record_hash"]
    return {"ok": True, "records": len(measurements), "head": previous,
            "paths": len(paths), "schedules": len(schedules), "status": "verified"}


def _load_paths(
    database_path: Path, *, connection: sqlite3.Connection | None = None,
) -> list[dict]:
    own_connection = connection is None
    connection = connection or sqlite3.connect(
        f"file:{database_path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        path_rows = [dict(row) for row in connection.execute(
            "SELECT * FROM flow_shadow_paths ORDER BY signaled_at,path_id")]
        measurements = [dict(row) for row in connection.execute(
            """
            SELECT m.*,s.label,s.nominal_offset_seconds
            FROM flow_shadow_path_measurements m
            JOIN flow_shadow_path_schedule s
              ON s.path_id=m.path_id AND s.step_index=m.step_index
            ORDER BY m.path_id,m.step_index
            """)]
    except sqlite3.OperationalError:
        return []
    finally:
        if own_connection:
            connection.close()
    by_path = {row["path_id"]: {**row, "marks": {}} for row in path_rows}
    for row in measurements:
        path = by_path.get(row["path_id"])
        if path is None:
            continue
        path["marks"][int(row["step_index"])] = {
            "label": row["label"], "status": row["status"],
            "quote_verified": bool(row["quote_verified"]),
            "exit_valid": bool(row["exit_valid"]),
            "net_return": _safe_float(row["net_return"], -1.0),
            "price_multiple": _safe_float(row["price_multiple"], None),
            "liquidity_usd": _safe_float(row["liquidity_usd"], None),
        }
    return list(by_path.values())


def _ensure_evaluation_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
            CREATE TABLE IF NOT EXISTS shadow_exit_experiments (
                experiment_id TEXT PRIMARY KEY,
                policy_version TEXT NOT NULL,
                policy_hash TEXT NOT NULL UNIQUE,
                frozen_at REAL NOT NULL,
                payload_json TEXT NOT NULL,
                record_hash TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS shadow_exit_reports (
                sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at REAL NOT NULL,
                evidence_hash TEXT NOT NULL UNIQUE,
                previous_hash TEXT NOT NULL,
                record_hash TEXT NOT NULL UNIQUE,
                report_json TEXT NOT NULL
            );
            CREATE TRIGGER IF NOT EXISTS shadow_exit_experiments_no_update
            BEFORE UPDATE ON shadow_exit_experiments BEGIN
                SELECT RAISE(ABORT,'shadow exit experiments are append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS shadow_exit_experiments_no_delete
            BEFORE DELETE ON shadow_exit_experiments BEGIN
                SELECT RAISE(ABORT,'shadow exit experiments are append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS shadow_exit_reports_no_update
            BEFORE UPDATE ON shadow_exit_reports BEGIN
                SELECT RAISE(ABORT,'shadow exit reports are append-only');
            END;
            CREATE TRIGGER IF NOT EXISTS shadow_exit_reports_no_delete
            BEFORE DELETE ON shadow_exit_reports BEGIN
                SELECT RAISE(ABORT,'shadow exit reports are append-only');
            END;
        """)


def _experiment_id() -> str:
    return hashlib.sha256(
        f"shadow-exit-selection:{policy_hash()}".encode("utf-8")
    ).hexdigest()


def _load_experiment(root: Path) -> dict | None:
    ledger_path = root / LEDGER_NAME
    if not ledger_path.exists():
        return None
    connection = sqlite3.connect(ledger_path)
    connection.row_factory = sqlite3.Row
    try:
        _ensure_evaluation_schema(connection)
        row = connection.execute(
            "SELECT * FROM shadow_exit_experiments WHERE policy_hash=?",
            (policy_hash(),),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    payload = json.loads(row["payload_json"])
    payload["record_hash"] = row["record_hash"]
    return payload


def _freeze_experiment(
    root: Path, training_paths: list[dict], selected_policy: str,
    train_metrics: dict, *, source_revision: str, source_digest: str,
) -> dict:
    """Seal policy selection before any admissible holdout path is born."""
    frozen_at = time.time()
    boundary = max(
        (float(row["signaled_at"]),str(row["path_id"]))
        for row in training_paths
    )
    payload = {
        "schema_version": 1,
        "experiment_id": _experiment_id(),
        "policy_version": POLICY_VERSION,
        "policy_hash": policy_hash(),
        "frozen_at": frozen_at,
        "selected_policy": selected_policy,
        "train_path_ids": [str(row["path_id"]) for row in training_paths],
        "train_pool_ids": sorted({str(row["pool_id"]) for row in training_paths}),
        "training_boundary": {
            "signaled_at": boundary[0], "path_id": boundary[1]},
        "train_metrics": train_metrics,
        "source_revision": source_revision,
        "source_digest": source_digest,
        "holdout_rule": (
            "path signaled after frozen_at and pool absent from training pools"
        ),
    }
    body = canonical(payload)
    record_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
    root.mkdir(parents=True,exist_ok=True)
    connection = sqlite3.connect(root / LEDGER_NAME)
    connection.row_factory = sqlite3.Row
    try:
        _ensure_evaluation_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        connection.execute(
            """
            INSERT OR IGNORE INTO shadow_exit_experiments (
                experiment_id,policy_version,policy_hash,frozen_at,
                payload_json,record_hash
            ) VALUES (?,?,?,?,?,?)
            """,
            (
                payload["experiment_id"],POLICY_VERSION,policy_hash(),
                frozen_at,body,record_hash,
            ),
        )
        row = connection.execute(
            "SELECT * FROM shadow_exit_experiments WHERE policy_hash=?",
            (policy_hash(),),
        ).fetchone()
        connection.commit()
    finally:
        connection.close()
    sealed = json.loads(row["payload_json"])
    sealed["record_hash"] = row["record_hash"]
    return sealed


def _append_report(root: Path, report: dict) -> dict:
    ledger_path = root / LEDGER_NAME
    connection = sqlite3.connect(ledger_path)
    try:
        _ensure_evaluation_schema(connection)
        connection.execute("BEGIN IMMEDIATE")
        previous_row = connection.execute(
            "SELECT record_hash FROM shadow_exit_reports ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous = previous_row[0] if previous_row else GENESIS_HASH
        body = canonical(report)
        evidence_hash = report["source"]["evidence_hash"]
        existing = connection.execute(
            "SELECT report_json FROM shadow_exit_reports WHERE evidence_hash=?",
            (evidence_hash,),
        ).fetchone()
        if existing:
            return json.loads(existing[0])
        record = hashlib.sha256(
            (previous + evidence_hash + body).encode("utf-8")
        ).hexdigest()
        connection.execute(
            "INSERT INTO shadow_exit_reports"
            " (created_at,evidence_hash,previous_hash,record_hash,report_json)"
            " VALUES (?,?,?,?,?)",
            (time.time(), evidence_hash, previous, record, body),
        )
        connection.commit()
    finally:
        connection.close()
    return report


def verify_evaluation_ledger(root: str | Path) -> dict:
    path = Path(root) / LEDGER_NAME
    if not path.exists():
        return {"ok": True, "records": 0, "head": GENESIS_HASH}
    connection = sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        experiments = connection.execute(
            "SELECT * FROM shadow_exit_experiments ORDER BY frozen_at"
        ).fetchall()
        rows = connection.execute(
            "SELECT * FROM shadow_exit_reports ORDER BY sequence").fetchall()
    except sqlite3.OperationalError:
        return {"ok": False, "records": 0, "head": GENESIS_HASH,
                "reason": "evaluation_schema_incomplete"}
    finally:
        connection.close()
    for experiment in experiments:
        try:
            payload = json.loads(experiment["payload_json"])
        except (TypeError, ValueError, json.JSONDecodeError):
            return {"ok": False, "records": len(rows), "head": GENESIS_HASH,
                    "reason": "experiment_payload_invalid"}
        expected = hashlib.sha256(
            experiment["payload_json"].encode("utf-8")).hexdigest()
        if (experiment["record_hash"] != expected
                or payload.get("experiment_id") != experiment["experiment_id"]
                or payload.get("policy_hash") != experiment["policy_hash"]
                or payload.get("policy_version") != experiment["policy_version"]):
            return {"ok": False, "records": len(rows), "head": GENESIS_HASH,
                    "reason": "experiment_binding_mismatch"}
    previous = GENESIS_HASH
    for row in rows:
        expected = hashlib.sha256(
            (previous + row["evidence_hash"] + row["report_json"]).encode("utf-8")
        ).hexdigest()
        if row["previous_hash"] != previous or row["record_hash"] != expected:
            return {"ok": False, "records": len(rows),
                    "failed_sequence": row["sequence"], "head": previous}
        previous = row["record_hash"]
    return {"ok": True, "records": len(rows), "head": previous,
            "experiments": len(experiments)}


def run_shadow_exit_evaluation(
    root: str | Path, *, source_revision: str = "unknown",
    source_digest: str = "unknown", source_permissions: dict[str, bool] | None = None,
    operational_evidence: dict | None = None,
) -> dict:
    root = Path(root)
    database_path = root / "learning.sqlite3"
    prior_evaluation_integrity = verify_evaluation_ledger(root)
    if database_path.exists():
        # Verification and evaluation must see the same SQLite snapshot while
        # the evidence worker continues appending measurements in WAL mode.
        connection = sqlite3.connect(
            f"file:{database_path.as_posix()}?mode=ro", uri=True)
        try:
            connection.execute("BEGIN")
            integrity = verify_measurement_ledger(
                database_path,connection=connection)
            paths = (_load_paths(database_path,connection=connection)
                     if integrity.get("ok") and prior_evaluation_integrity.get("ok")
                     else [])
        finally:
            connection.close()
    else:
        integrity = verify_measurement_ledger(database_path)
        paths = (_load_paths(database_path)
                 if integrity.get("status") == "verified" else [])
    permissions = {str(k): bool(v) for k, v in (source_permissions or {}).items()}
    operational = dict(operational_evidence or {})

    path_by_id = {row["path_id"]: row for row in paths}
    simulations: dict[str, dict[str, dict]] = {}
    policy_metrics: dict[str, dict] = {}
    for name in POLICIES:
        by_path = {}
        for path in paths:
            result = _simulate(path, name)
            if result is not None:
                by_path[path["path_id"]] = result
        simulations[name] = by_path
        policy_metrics[name] = _metrics([
            result for candidate_path_id,result in by_path.items()
            if path_by_id[candidate_path_id]["signal_role"] == "qualified"
        ])

    common_policy_names = list(POLICIES)
    common_signals = [
        path for path in paths
        if path["signal_role"] == "qualified"
        and all(path["path_id"] in simulations[name] for name in common_policy_names)
    ]
    common_signals.sort(key=lambda row: (float(row["signaled_at"]), row["path_id"]))
    experiment = (_load_experiment(root)
                  if prior_evaluation_integrity.get("ok") else None)
    if experiment is None:
        # Exploratory metrics may be displayed while collecting, but policy
        # selection happens exactly once.  The immutable experiment record is
        # written before any path born after its timestamp can become holdout.
        train_candidates = common_signals[:MINIMUM_TRAIN_PATHS]
        train_metrics = {
            name: _metrics([
                simulations[name][row["path_id"]] for row in train_candidates
            ]) for name in common_policy_names
        }
        selected_policy = None
        if len(train_candidates) >= MINIMUM_TRAIN_PATHS:
            selected_policy = max(
                common_policy_names,
                key=lambda name: (
                    train_metrics[name]["objective"],
                    train_metrics[name]["mean_return_lower_95"],
                    name,
                ),
            )
            experiment = _freeze_experiment(
                root,train_candidates,selected_policy,train_metrics,
                source_revision=source_revision,source_digest=source_digest)
    if experiment is not None:
        train_ids = [str(value) for value in experiment["train_path_ids"]]
        train_candidates = [
            path_by_id[value] for value in train_ids if value in path_by_id]
        train_metrics = dict(experiment["train_metrics"])
        selected_policy = str(experiment["selected_policy"])
        train_pools = {str(value) for value in experiment["train_pool_ids"]}
        train_id_set = set(train_ids)
        frozen_at = float(experiment["frozen_at"])
        post_freeze = [
            row for row in common_signals
            if float(row["signaled_at"]) > frozen_at
        ]
        holdout = [row for row in post_freeze if row["pool_id"] not in train_pools]
        crossing_excluded = len(post_freeze) - len(holdout)
        pre_freeze_excluded = sum(
            row["path_id"] not in train_id_set
            and float(row["signaled_at"]) <= frozen_at
            for row in common_signals
        )
    else:
        train_pools = {row["pool_id"] for row in train_candidates}
        holdout = []
        crossing_excluded = 0
        pre_freeze_excluded = max(
            0,len(common_signals) - len(train_candidates))
    evaluation_integrity = verify_evaluation_ledger(root)
    selection_integrity = bool(
        evaluation_integrity.get("ok")
        and (experiment is None or (
            len(train_candidates) == MINIMUM_TRAIN_PATHS
            and selected_policy in common_policy_names
            and all(
                row["path_id"] in simulations[name]
                for row in train_candidates for name in common_policy_names
            )
        ))
    )

    selected_holdout_results = (
        [simulations[selected_policy][row["path_id"]] for row in holdout]
        if selected_policy else [])
    selected_holdout_metrics = _metrics(selected_holdout_results)
    holdout_policy_metrics = {
        name: _metrics([simulations[name][row["path_id"]] for row in holdout])
        for name in common_policy_names
    }
    baseline_results = holdout_policy_metrics["hold_15m"]
    controls = simulations.get(selected_policy or "", {})
    selected_paired = _paired_metrics(selected_holdout_results, controls)

    selected_sources = sorted({row["source_version"] for row in holdout})
    source_pass = bool(selected_sources) and all(
        permissions.get(source, False) for source in selected_sources)
    operational_pass = bool(
        operational.get("stabilized")
        and operational.get("criteria_total")
        and operational.get("criteria_passed") == operational.get("criteria_total"))
    gates = {
        "measurement_integrity": {"pass": bool(integrity.get("ok")), "value": integrity},
        "selection_integrity": {
            "pass": selection_integrity, "value": evaluation_integrity},
        "minimum_train_paths": {
            "pass": experiment is not None
            and len(train_candidates) == MINIMUM_TRAIN_PATHS,
            "value": {
                "actual": len(train_candidates), "minimum": MINIMUM_TRAIN_PATHS,
                "selection_frozen": experiment is not None,
            },
        },
        "minimum_holdout_paths": {
            "pass": len(holdout) >= MINIMUM_HOLDOUT_PATHS,
            "value": {"actual": len(holdout), "minimum": MINIMUM_HOLDOUT_PATHS},
        },
        "minimum_paired_holdout_paths": {
            "pass": selected_paired["samples"] >= MINIMUM_PAIRED_HOLDOUT_PATHS,
            "value": {"actual": selected_paired["samples"],
                      "minimum": MINIMUM_PAIRED_HOLDOUT_PATHS},
        },
        "positive_absolute_return_lower_bound": {
            "pass": selected_holdout_metrics["mean_return_lower_95"] is not None
            and selected_holdout_metrics["mean_return_lower_95"] > 0,
            "value": selected_holdout_metrics["mean_return_lower_95"],
        },
        "positive_paired_edge_lower_bound": {
            "pass": selected_paired["mean_incremental_lower_95"] is not None
            and selected_paired["mean_incremental_lower_95"] > 0,
            "value": selected_paired["mean_incremental_lower_95"],
        },
        "bounded_non_exit_rate": {
            "pass": selected_holdout_metrics["non_exit_rate"] is not None
            and selected_holdout_metrics["non_exit_rate"] <= MAXIMUM_NONEXIT_RATE,
            "value": {"actual": selected_holdout_metrics["non_exit_rate"],
                      "maximum": MAXIMUM_NONEXIT_RATE},
        },
        "bounded_catastrophic_rate": {
            "pass": selected_holdout_metrics["catastrophic_rate"] is not None
            and selected_holdout_metrics["catastrophic_rate"] <= MAXIMUM_CATASTROPHIC_RATE,
            "value": {"actual": selected_holdout_metrics["catastrophic_rate"],
                      "maximum": MAXIMUM_CATASTROPHIC_RATE},
        },
        "improves_hold_15m_objective": {
            "pass": selected_holdout_metrics["objective"] is not None
            and baseline_results["objective"] is not None
            and selected_holdout_metrics["objective"] > baseline_results["objective"],
            "value": {"selected": selected_holdout_metrics["objective"],
                      "hold_15m": baseline_results["objective"]},
        },
    }
    economic_pass = all(value["pass"] for value in gates.values())
    if not integrity.get("ok") or not selection_integrity:
        status = "measurement_integrity_failed"
    elif experiment is None:
        status = "collecting_training_paths"
    elif len(holdout) < MINIMUM_HOLDOUT_PATHS:
        status = "collecting_prospective_holdout"
    elif not economic_pass:
        status = "exit_challenger_not_economically_valid"
    elif not operational_pass:
        status = "operational_proof_required"
    elif not source_pass:
        status = "source_admission_blocked"
    else:
        status = "shadow_exit_candidate_ready_for_acceptance"

    evidence_hash = hashlib.sha256(canonical({
        "measurement_head": integrity.get("head"),
        "integrity": integrity,
        "path_count": len(paths),
        "policy_hash": policy_hash(),
        "source_revision": source_revision,
        "source_digest": source_digest,
        "experiment_hash": (
            experiment.get("record_hash") if experiment else None),
        "source_permissions": permissions,
        "operational_evidence": operational,
    }).encode("utf-8")).hexdigest()
    report = {
        "schema_version": 1, "policy_version": POLICY_VERSION,
        "created_at": time.time(), "status": status,
        "source": {
            "revision": source_revision, "source_digest": source_digest,
            "evidence_hash": evidence_hash,
            "measurement_head": integrity.get("head"),
            "policy_hash": policy_hash(),
            "experiment_hash": (
                experiment.get("record_hash") if experiment else None),
        },
        "collection": {
            "paths": len(paths),
            "qualified_paths": sum(row["signal_role"] == "qualified" for row in paths),
            "control_paths": sum(row["signal_role"] == "matched_control" for row in paths),
            "measurement_records": integrity.get("records", 0),
            "common_complete_paths": len(common_signals),
            "entry_terminal_paths": sum(0 in row["marks"] for row in paths),
            "entry_marketable_paths": sum(
                0 in row["marks"]
                and row["marks"][0]["status"] == "observed"
                and row["marks"][0]["exit_valid"] for row in paths),
            "entry_unmarketable_paths": sum(
                0 in row["marks"]
                and row["marks"][0]["status"] == "entry_unmarketable"
                for row in paths),
            "train_paths": len(train_candidates), "holdout_paths": len(holdout),
            "crossing_pool_rows_excluded": crossing_excluded,
            "pre_freeze_complete_paths_excluded": pre_freeze_excluded,
            "strategy_population": "entry_marketable_paths_only",
            "selection_frozen": experiment is not None,
            "selection_frozen_at": (
                experiment.get("frozen_at") if experiment else None),
            "split_rule": "post-freeze forward-time pool-disjoint holdout",
        },
        "integrity": integrity,
        "policy_metrics_available": policy_metrics,
        "selection": {
            "selected_policy": selected_policy,
            "experiment": experiment,
            "train_metrics": train_metrics,
            "holdout_policy_metrics": holdout_policy_metrics,
            "selected_holdout_metrics": selected_holdout_metrics,
            "paired_holdout_metrics": selected_paired,
            "holdout_was_used_for_selection": False,
        },
        "governance": {
            "economic_evidence_pass": economic_pass,
            "economic_gates": gates,
            "operational_13_of_13_pass": operational_pass,
            "source_admission_pass": source_pass,
            "selected_sources": selected_sources,
            "source_permissions": permissions,
            "promotion_enabled": False,
            "live_execution_path_present": False,
            "active_policy_changed": False,
            "claim_scope": "modeled checkpoint exits using block-pinned quotes",
            "limitations": [
                "partial fills are modeled pro rata from full-position quotes",
                "friction is assumed; gas, latency and MEV are not measured fills",
                "checkpoint labels use nominal ten-blocks-per-second timing",
                "confidence intervals are exploratory and repeated-pool dependence remains",
            ],
        },
    }
    root.mkdir(parents=True, exist_ok=True)
    sealed = _append_report(root, report)
    atomic_json_write(root / ARTIFACT_NAME,sealed)
    return sealed


def latest_shadow_exit_evaluation(root: str | Path) -> dict:
    path = Path(root) / ARTIFACT_NAME
    if not path.exists():
        return {
            "schema_version": 1, "policy_version": POLICY_VERSION,
            "status": "not_started", "collection": {},
            "governance": {"promotion_enabled": False,
                           "live_execution_path_present": False},
        }
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return {
            "schema_version": 1, "policy_version": POLICY_VERSION,
            "status": "artifact_unreadable", "collection": {},
            "governance": {"promotion_enabled": False,
                           "live_execution_path_present": False},
        }
    return value if isinstance(value, dict) else {}
