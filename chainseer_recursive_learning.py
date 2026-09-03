"""Shadow-only recursive policy learning for Chainseer Robinhood.

This module is intentionally incapable of changing the active strategy.  It
reads immutable Flow observations/outcomes, creates a bounded challenger, and
appends an auditable report to a separate ledger.  Promotion belongs to a
future, explicitly enabled governance stage after operational stabilization.
"""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import statistics
import time
import uuid
from pathlib import Path
from typing import Any


SCHEMA_VERSION = 1
POLICY_VERSION = "recursive-learning-shadow-v1"
HORIZON = "15m"
MAXIMUM_SOURCE_ROWS = 50_000
TRAIN_FRACTION = 0.70
MINIMUM_TRAIN_ROWS = 500
MINIMUM_HOLDOUT_ROWS = 200
MINIMUM_EXECUTABLE_HOLDOUT_ROWS = 100
MINIMUM_SELECTED_ROWS = 30
MAXIMUM_ABSOLUTE_WEIGHT = 0.25
NON_EXITABLE_RETURN = -1.0
RETURN_FLOOR = -1.0
RETURN_CEILING = 3.0
FEATURES = (
    "uncapped_shadow_score",
    "velocity_quality",
    "participant_quality",
    "pressure_quality",
    "identity_coverage",
    "maximum_participant_share",
    "adverse_price_direction",
)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _open_read_only(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True,
        timeout=10.0,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _open_ledger(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=10.0)
    connection.row_factory = sqlite3.Row
    connection.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS shadow_learning_runs (
            run_id TEXT PRIMARY KEY,
            created_at REAL NOT NULL,
            schema_version INTEGER NOT NULL,
            policy_version TEXT NOT NULL,
            source_policy_version TEXT NOT NULL,
            source_revision TEXT NOT NULL,
            source_digest TEXT NOT NULL DEFAULT 'unknown',
            learner_digest TEXT NOT NULL DEFAULT 'unknown',
            evidence_hash TEXT NOT NULL UNIQUE,
            cutoff_epoch REAL,
            train_rows INTEGER NOT NULL,
            holdout_rows INTEGER NOT NULL,
            executable_holdout_rows INTEGER NOT NULL,
            status TEXT NOT NULL,
            report_json TEXT NOT NULL,
            promotion_enabled INTEGER NOT NULL CHECK(promotion_enabled=0)
        );
        CREATE TRIGGER IF NOT EXISTS shadow_learning_runs_no_update
        BEFORE UPDATE ON shadow_learning_runs
        BEGIN SELECT RAISE(ABORT, 'shadow learning ledger is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS shadow_learning_runs_no_delete
        BEFORE DELETE ON shadow_learning_runs
        BEGIN SELECT RAISE(ABORT, 'shadow learning ledger is append-only'); END;
    """)
    columns = {
        row[1] for row in connection.execute(
            "PRAGMA table_info(shadow_learning_runs)")
    }
    if "source_digest" not in columns:
        connection.execute(
            "ALTER TABLE shadow_learning_runs ADD COLUMN "
            "source_digest TEXT NOT NULL DEFAULT 'unknown'")
    if "learner_digest" not in columns:
        connection.execute(
            "ALTER TABLE shadow_learning_runs ADD COLUMN "
            "learner_digest TEXT NOT NULL DEFAULT 'unknown'")
    return connection


def _load_rows(database: Path) -> tuple[list[dict], dict]:
    excluded = {
        "pending": 0, "unpriceable": 0, "unsampled": 0,
        "retired_horizon": 0, "invalid": 0,
    }
    connection = _open_read_only(database)
    try:
        for row in connection.execute(
            """SELECT status,COUNT(*) count
               FROM flow_observation_outcomes
               WHERE horizon_label=? GROUP BY status""", (HORIZON,)
        ):
            if row["status"] in excluded:
                excluded[row["status"]] = int(row["count"])
        source = connection.execute(
            """SELECT o.observation_id,o.pool_id,o.observed_at_epoch,
                      o.features_json,c.identity_tier,c.arm,c.gates_json,
                      c.research_eligible,c.paper_eligible,
                      q.status outcome_status,q.net_return,q.exit_valid
               FROM flow_observation_outcomes q
               JOIN flow_observations o USING(observation_id)
               JOIN flow_observation_classifications c USING(observation_id)
               WHERE q.horizon_label=?
                 AND q.status IN ('resolved','non_exitable')
                 AND c.research_eligible=1
               ORDER BY o.observed_at_epoch DESC LIMIT ?""",
            (HORIZON, MAXIMUM_SOURCE_ROWS),
        ).fetchall()
    finally:
        connection.close()
    rows: list[dict] = []
    for raw in source:
        try:
            features = json.loads(raw["features_json"] or "{}")
            gates = json.loads(raw["gates_json"] or "[]")
            if not isinstance(features, dict) or not isinstance(gates, list):
                raise ValueError("invalid evidence JSON")
        except (TypeError, ValueError, json.JSONDecodeError):
            excluded["invalid"] += 1
            continue
        outcome_status = str(raw["outcome_status"])
        target = (
            NON_EXITABLE_RETURN if outcome_status == "non_exitable"
            else _safe_float(raw["net_return"], float("nan")))
        if not math.isfinite(target):
            excluded["invalid"] += 1
            continue
        target = min(RETURN_CEILING, max(RETURN_FLOOR, target))
        epoch = _safe_float(raw["observed_at_epoch"], 0.0)
        if epoch <= 0:
            excluded["invalid"] += 1
            continue
        vector = {
            name: _safe_float(features.get(name), 0.0)
            for name in FEATURES
        }
        rows.append({
            "observation_id": str(raw["observation_id"]),
            "pool_id": str(raw["pool_id"]),
            "observed_at_epoch": epoch,
            "features": vector,
            "identity_tier": str(raw["identity_tier"] or "unresolved"),
            "arm": str(raw["arm"] or "historical"),
            "gates": sorted(str(item) for item in gates),
            "paper_eligible": bool(raw["paper_eligible"]),
            "outcome_status": outcome_status,
            "target_return": target,
        })
    rows.sort(key=lambda row: (
        row["observed_at_epoch"], row["observation_id"]))
    return rows, excluded


def _deduplicate_windows(rows: list[dict]) -> list[dict]:
    """Keep one observation per pool per outcome horizon.

    Overlapping windows from one busy pool are not independent trades. The
    earliest observation in each 15-minute pool bucket is deterministic and
    prevents that pool from dominating the learner.
    """
    chosen: dict[tuple[str, int], dict] = {}
    for row in rows:
        key = (row["pool_id"], int(row["observed_at_epoch"] // 900))
        chosen.setdefault(key, row)
    return sorted(chosen.values(), key=lambda row: (
        row["observed_at_epoch"], row["observation_id"]))


def _temporal_split(rows: list[dict]) -> tuple[list[dict], list[dict], dict]:
    if not rows:
        return [], [], {"cutoff_epoch": None, "purged_crossing_pools": 0}
    cutoff_index = min(
        len(rows) - 1, max(0, int(len(rows) * TRAIN_FRACTION) - 1))
    cutoff = rows[cutoff_index]["observed_at_epoch"]
    before = {row["pool_id"] for row in rows
              if row["observed_at_epoch"] <= cutoff}
    after = {row["pool_id"] for row in rows
             if row["observed_at_epoch"] > cutoff}
    crossing = before & after
    train = [row for row in rows
             if row["observed_at_epoch"] <= cutoff
             and row["pool_id"] not in crossing]
    holdout = [row for row in rows
               if row["observed_at_epoch"] > cutoff
               and row["pool_id"] not in crossing]
    return train, holdout, {
        "cutoff_epoch": cutoff,
        "purged_crossing_pools": len(crossing),
        "split_rule": "forward_time_with_pool_overlap_purged",
    }


def _fit(train: list[dict]) -> dict:
    target = [row["target_return"] for row in train]
    target_mean = statistics.fmean(target) if target else 0.0
    raw: dict[str, float] = {}
    normalization: dict[str, dict] = {}
    for name in FEATURES:
        values = [row["features"][name] for row in train]
        mean = statistics.fmean(values) if values else 0.0
        deviation = statistics.pstdev(values) if len(values) > 1 else 0.0
        if deviation <= 1e-12:
            coefficient = 0.0
        else:
            covariance = statistics.fmean([
                (value - mean) * (result - target_mean)
                for value, result in zip(values, target)
            ])
            coefficient = covariance / deviation
        raw[name] = coefficient
        normalization[name] = {"mean": mean, "stddev": deviation}
    scale = sum(abs(value) for value in raw.values()) or 1.0
    weights = {
        name: max(-MAXIMUM_ABSOLUTE_WEIGHT, min(
            MAXIMUM_ABSOLUTE_WEIGHT, value / scale))
        for name, value in raw.items()
    }
    return {"weights": weights, "normalization": normalization}


def _score(row: dict, model: dict) -> float:
    result = 0.0
    for name, weight in model["weights"].items():
        normal = model["normalization"][name]
        deviation = _safe_float(normal["stddev"], 0.0)
        z_value = 0.0 if deviation <= 1e-12 else (
            row["features"][name] - normal["mean"]) / deviation
        result += weight * max(-4.0, min(4.0, z_value))
    return result


def _metrics(rows: list[dict]) -> dict:
    returns = [row["target_return"] for row in rows]
    if not returns:
        return {"samples": 0, "mean_return": None, "median_return": None,
                "loss_rate": None, "catastrophic_rate": None,
                "mean_return_lower_95": None, "objective": None}
    mean = statistics.fmean(returns)
    deviation = statistics.stdev(returns) if len(returns) > 1 else 0.0
    loss_rate = sum(value < 0 for value in returns) / len(returns)
    catastrophic = sum(value <= -0.80 for value in returns) / len(returns)
    lower = mean - 1.96 * deviation / math.sqrt(len(returns))
    return {
        "samples": len(returns),
        "mean_return": round(mean, 6),
        "median_return": round(statistics.median(returns), 6),
        "loss_rate": round(loss_rate, 6),
        "catastrophic_rate": round(catastrophic, 6),
        "mean_return_lower_95": round(lower, 6),
        "objective": round(mean - 0.5 * loss_rate - catastrophic, 6),
    }


def _executable(row: dict) -> bool:
    return bool(
        row["identity_tier"] == "verified"
        and row["arm"] == "fresh"
        and not row["gates"]
    )


def _choose_threshold(train: list[dict], model: dict) -> dict:
    scored = sorted(
        ((_score(row, model), row) for row in train),
        key=lambda item: (item[0], item[1]["observation_id"]),
    )
    candidates = []
    for quantile in (0.70, 0.80, 0.90):
        index = min(len(scored) - 1, max(0, int(len(scored) * quantile)))
        threshold = scored[index][0] if scored else 0.0
        selected = [row for score, row in scored if score >= threshold]
        metrics = _metrics(selected)
        candidates.append({
            "quantile": quantile, "threshold": threshold,
            "metrics": metrics,
        })
    eligible = [item for item in candidates
                if item["metrics"]["samples"] >= MINIMUM_SELECTED_ROWS]
    winner = max(eligible or candidates, key=lambda item: (
        item["metrics"]["objective"]
        if item["metrics"]["objective"] is not None else float("-inf"),
        item["metrics"]["samples"],
    ))
    return {"selected": winner, "candidates": candidates}


def run_shadow_learning(
    root: str | Path, *, source_policy_version: str,
    source_revision: str, source_digest: str = "unknown",
) -> dict:
    root = Path(root)
    started = time.time()
    rows, excluded = _load_rows(root / "learning.sqlite3")
    deduplicated = _deduplicate_windows(rows)
    train, holdout, split = _temporal_split(deduplicated)
    evidence_manifest = [{
        "observation_id": row["observation_id"],
        "pool_id": row["pool_id"],
        "observed_at_epoch": row["observed_at_epoch"],
        "feature_hash": _hash(row["features"]),
        "outcome_status": row["outcome_status"],
        "target_return": row["target_return"],
    } for row in deduplicated]
    learner_digest = hashlib.sha256(
        Path(__file__).resolve().read_bytes()).hexdigest()
    evidence_hash = _hash({
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "source_policy_version": source_policy_version,
        "source_revision": source_revision,
        "source_digest": source_digest,
        "learner_digest": learner_digest,
        "manifest": evidence_manifest,
    })
    sufficient_research = bool(
        len(train) >= MINIMUM_TRAIN_ROWS
        and len(holdout) >= MINIMUM_HOLDOUT_ROWS)
    model = _fit(train) if train else {
        "weights": {name: 0.0 for name in FEATURES},
        "normalization": {
            name: {"mean": 0.0, "stddev": 0.0} for name in FEATURES},
    }
    threshold = _choose_threshold(train, model)
    boundary = threshold["selected"]["threshold"]
    challenger_holdout = [row for row in holdout
                          if _score(row, model) >= boundary]
    champion_executable = [row for row in holdout if _executable(row)]
    challenger_executable = [row for row in challenger_holdout
                              if _executable(row)]
    challenger_metrics = _metrics(challenger_holdout)
    champion_metrics = _metrics(champion_executable)
    challenger_executable_metrics = _metrics(challenger_executable)
    executable_sufficient = bool(
        champion_metrics["samples"] >= MINIMUM_EXECUTABLE_HOLDOUT_ROWS
        and challenger_executable_metrics["samples"]
            >= MINIMUM_EXECUTABLE_HOLDOUT_ROWS)
    evidence_improves = bool(
        sufficient_research and executable_sufficient
        and challenger_executable_metrics["objective"] is not None
        and champion_metrics["objective"] is not None
        and challenger_executable_metrics["objective"]
            > champion_metrics["objective"]
        and challenger_executable_metrics["catastrophic_rate"]
            <= champion_metrics["catastrophic_rate"]
        and challenger_executable_metrics["mean_return_lower_95"] > 0)
    status = (
        "shadow_candidate_ready" if evidence_improves
        else "insufficient_executable_evidence"
        if not executable_sufficient else "challenger_not_better")
    report = {
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "created_at": time.time(),
        "source": {
            "policy_version": source_policy_version,
            "revision": source_revision,
            "source_digest": source_digest,
            "learner_digest": learner_digest,
            "horizon": HORIZON,
            "evidence_hash": evidence_hash,
            "target_policy": {
                "resolved": "winsorized_net_return",
                "non_exitable": NON_EXITABLE_RETURN,
                "return_bounds": [RETURN_FLOOR, RETURN_CEILING],
            },
        },
        "data_quality": {
            "source_rows": len(rows),
            "deduplicated_pool_horizon_rows": len(deduplicated),
            "excluded_by_status": excluded,
            **split,
            "train_rows": len(train),
            "holdout_rows": len(holdout),
            "executable_holdout_rows": champion_metrics["samples"],
            "minimum_train_rows": MINIMUM_TRAIN_ROWS,
            "minimum_holdout_rows": MINIMUM_HOLDOUT_ROWS,
            "minimum_executable_holdout_rows":
                MINIMUM_EXECUTABLE_HOLDOUT_ROWS,
        },
        "challenger": {
            "model": model,
            "threshold_search": threshold,
            "holdout_metrics": challenger_metrics,
            "executable_holdout_metrics": challenger_executable_metrics,
            "bounded_absolute_weight": MAXIMUM_ABSOLUTE_WEIGHT,
            "hard_gates_mutable": False,
        },
        "champion": {"executable_holdout_metrics": champion_metrics},
        "governance": {
            "status": status,
            "research_evidence_sufficient": sufficient_research,
            "executable_evidence_sufficient": executable_sufficient,
            "evidence_supports_improvement": evidence_improves,
            "promotion_enabled": False,
            "promotion_eligible": False,
            "promotion_disabled_reason": (
                "recursive-learning-v1-is-shadow-only; operational 13/13 "
                "and a separate challenger acceptance cohort are required"),
            "active_policy_changed": False,
            "single_trade_updates_allowed": False,
            "rollback_required_before_promotion": True,
        },
        "duration_seconds": round(time.time() - started, 3),
    }
    run_id = uuid.uuid4().hex
    ledger = _open_ledger(root / "recursive_learning_v1.sqlite3")
    try:
        ledger.execute(
            """INSERT OR IGNORE INTO shadow_learning_runs
               (run_id,created_at,schema_version,policy_version,
                source_policy_version,source_revision,source_digest,
                learner_digest,evidence_hash,
                cutoff_epoch,train_rows,holdout_rows,executable_holdout_rows,
                status,report_json,promotion_enabled)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
            (run_id, report["created_at"], SCHEMA_VERSION, POLICY_VERSION,
             source_policy_version, source_revision, source_digest,
             learner_digest, evidence_hash,
             split["cutoff_epoch"], len(train), len(holdout),
             champion_metrics["samples"], status, _canonical(report)),
        )
        ledger.commit()
    finally:
        ledger.close()
    _atomic_json(root / "recursive_learning_v1.json", report)
    return report


def latest_shadow_learning(root: str | Path) -> dict:
    path = Path(root) / "recursive_learning_v1.json"
    if not path.exists():
        return {"policy_version": POLICY_VERSION, "status": "not_run",
                "promotion_enabled": False}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return {"policy_version": POLICY_VERSION, "status": "unreadable",
                "promotion_enabled": False}
