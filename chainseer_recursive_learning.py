"""Fail-closed recursive economic learning for Chainseer Robinhood.

Operational cohorts prove that Chainseer can observe reliably.  This module
asks a separate question: does a frozen Flow policy have prospective,
marketable evidence for a challenger?  It cannot mutate an active policy,
open a position, sign, or broadcast a transaction.

V2 learns from immutable ``flow_signal_events`` and
``flow_signal_outcomes``.  V1 used broad observation windows even though the
trade hypothesis and matched controls live on the signal-event path.
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


SCHEMA_VERSION = 2
POLICY_VERSION = "recursive-learning-shadow-v2"
POPULATION_KIND = "prospective_flow_signal_events"
HORIZON = "15m"
MAXIMUM_SOURCE_ROWS = 50_000
TRAIN_FRACTION = 0.70
MINIMUM_TRAIN_ROWS = 500
MINIMUM_HOLDOUT_ROWS = 200
MINIMUM_SELECTED_TRAIN_ROWS = 100
MINIMUM_SELECTED_HOLDOUT_ROWS = 100
MINIMUM_PAIRED_HOLDOUT_ROWS = 100
MAXIMUM_ABSOLUTE_WEIGHT = 0.25
TAIL_RISK_PENALTY = 0.50
RETURN_FLOOR = -1.0
RETURN_CEILING = 3.0
CATASTROPHIC_RETURN = -0.90
MAXIMUM_NONEXIT_RATE = 0.10
MAXIMUM_CATASTROPHIC_RATE = 0.20
ARTIFACT_NAME = "recursive_learning_v2.json"
LEGACY_ARTIFACT_NAME = "recursive_learning_v1.json"
LEDGER_NAME = "recursive_learning_v2.sqlite3"

# Every feature exists at signal/entry-quote time.  Outcome price, future
# liquidity and MFE/MAE are deliberately absent to prevent label leakage.
FEATURES = (
    "swap_count",
    "buy_ratio",
    "identity_coverage",
    "net_anchor_flow_fraction",
    "price_multiple",
    "uncapped_shadow_score",
    "unique_resolved_participants",
    "maximum_participant_share",
    "velocity_quality",
    "participant_quality",
    "pressure_quality",
    "log_estimated_liquidity_usd",
    "log_market_cap_usd",
    "singleton_balance_fraction",
    "round_trip_loss_fraction",
    "buy_price_impact_fraction",
    "log_pool_fee",
)


def _canonical(value: Any) -> str:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
        allow_nan=False,
    )


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        result = float(value)
        return result if math.isfinite(result) else default
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any) -> float | None:
    result = _safe_float(value, float("nan"))
    return result if math.isfinite(result) else None


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
        f"file:{path.resolve().as_posix()}?mode=ro", uri=True, timeout=10.0,
    )
    connection.row_factory = sqlite3.Row
    return connection


def _target(status: Any, net_return: Any, exit_valid: Any) -> tuple[float, bool] | None:
    status = str(status or "")
    if status == "non_exitable":
        return RETURN_FLOOR, True
    value = _optional_float(net_return)
    if status not in {"observed", "resolved"} or value is None:
        return None
    if not bool(exit_valid):
        return RETURN_FLOOR, True
    return min(RETURN_CEILING, max(RETURN_FLOOR, value)), False


def _entry_features(snapshot_text: Any, quote_text: Any) -> tuple[dict, dict] | None:
    try:
        snapshot = json.loads(snapshot_text or "{}")
        quote = json.loads(quote_text or "{}")
        if not isinstance(snapshot, dict) or not isinstance(quote, dict):
            return None
        nested = snapshot.get("features") or {}
        market = quote.get("market") or quote
        execution = market.get("execution_quote") or {}
        pool_key = execution.get("pool_key") or market.get("pool_key") or {}
        if not all(isinstance(item, dict) for item in (
                nested, market, execution, pool_key)):
            return None
    except (TypeError, ValueError, json.JSONDecodeError):
        return None

    def first(*values: Any) -> float | None:
        for value in values:
            parsed = _optional_float(value)
            if parsed is not None:
                return parsed
        return None

    liquidity = first(
        market.get("estimated_liquidity_usd"), market.get("liquidity_usd"))
    market_cap = first(market.get("market_cap_usd"), market.get("fdv_usd"))
    singleton = first(market.get("singleton_token_balance_fraction"))
    round_trip = first(execution.get("round_trip_loss_fraction"))
    price_impact = first(execution.get("buy_price_impact_fraction"))
    pool_fee = first(pool_key.get("fee"))
    required_quote = (liquidity, market_cap, singleton, round_trip,
                      price_impact, pool_fee)
    if any(value is None for value in required_quote):
        return None
    if not (
        liquidity > 0 and market_cap > 0
        and 0.0 <= singleton <= 1.0
        and round_trip >= 0.0 and price_impact >= 0.0 and pool_fee >= 0.0
    ):
        return None
    direct = {
        "swap_count": snapshot.get("swap_count"),
        "buy_ratio": snapshot.get("buy_ratio"),
        "identity_coverage": snapshot.get("identity_coverage"),
        "net_anchor_flow_fraction": snapshot.get("net_anchor_flow_fraction"),
        "price_multiple": snapshot.get("price_multiple"),
        "uncapped_shadow_score": snapshot.get("uncapped_shadow_score"),
        "unique_resolved_participants":
            snapshot.get("unique_resolved_participants"),
    }
    nested_names = (
        "maximum_participant_share", "velocity_quality",
        "participant_quality", "pressure_quality",
    )
    parsed_direct = {name: _optional_float(value)
                     for name, value in direct.items()}
    parsed_nested = {name: _optional_float(nested.get(name))
                     for name in nested_names}
    if any(value is None for value in (*parsed_direct.values(),
                                       *parsed_nested.values())):
        return None
    features = {
        **parsed_direct,
        **parsed_nested,
        "log_estimated_liquidity_usd": math.log1p(max(0.0, liquidity)),
        "log_market_cap_usd": math.log1p(max(0.0, market_cap)),
        "singleton_balance_fraction": singleton,
        "round_trip_loss_fraction": round_trip,
        "buy_price_impact_fraction": price_impact,
        "log_pool_fee": math.log1p(max(0.0, pool_fee)),
    }
    policy = snapshot.get("policy") or {}
    if not isinstance(policy, dict):
        return None
    minimum_identity = _safe_float(policy.get("minimum_identity_coverage"), 1.0)
    qualification_gaps = snapshot.get("qualification_gaps") or []
    if not isinstance(qualification_gaps, list):
        return None
    paper_eligible = bool(
        not nested.get("identity_unverified", True)
        and features["identity_coverage"] >= minimum_identity
        and not qualification_gaps
    )
    return features, {
        "paper_eligible": paper_eligible,
        "qualification_gaps": sorted(str(value) for value in qualification_gaps),
    }


def _latest_population(connection: sqlite3.Connection) -> tuple[str, str] | None:
    row = connection.execute(
        """SELECT e.policy_version,e.cohort_id
           FROM flow_signal_events e
           WHERE e.signal_role='qualified'
             AND e.eligible_for_evaluation=1
           ORDER BY e.signaled_at DESC,e.event_id DESC LIMIT 1""",
    ).fetchone()
    return (str(row["policy_version"]), str(row["cohort_id"])) if row else None


def _load_rows(database: Path, source_permissions: dict[str, bool]) -> tuple[list[dict], dict]:
    excluded = {
        "outside_latest_population": 0,
        "pending_or_unpriceable_outcome": 0,
        "invalid_outcome": 0,
        "incomplete_entry_snapshot_or_quote": 0,
    }
    connection = _open_read_only(database)
    try:
        connection.execute("BEGIN")
        population = _latest_population(connection)
        if population is None:
            return [], {**excluded, "flow_policy_version": None,
                        "flow_cohort_id": None}
        policy_version, cohort_id = population
        excluded["outside_latest_population"] = int(connection.execute(
            """SELECT COUNT(*) FROM flow_signal_events
               WHERE signal_role='qualified' AND eligible_for_evaluation=1
                 AND NOT(policy_version=? AND cohort_id=?)""",
            population,
        ).fetchone()[0])
        excluded["pending_or_unpriceable_outcome"] = int(connection.execute(
            """SELECT COUNT(*) FROM flow_signal_events e
               JOIN flow_signal_outcomes o USING(event_id)
               WHERE e.policy_version=? AND e.cohort_id=?
                 AND e.signal_role='qualified' AND e.eligible_for_evaluation=1
                 AND o.horizon_label=?
                 AND o.status NOT IN ('observed','resolved','non_exitable')""",
            (*population, HORIZON),
        ).fetchone()[0])
        raw_rows = connection.execute(
            """WITH controls AS (
                   SELECT matched_signal_event_id,MIN(event_id) AS event_id
                   FROM flow_signal_events
                   WHERE signal_role='matched_control'
                     AND matched_signal_event_id IS NOT NULL
                   GROUP BY matched_signal_event_id
               ), recent AS (
                   SELECT e.*,o.status AS outcome_status,
                          o.net_return,o.exit_valid
                   FROM flow_signal_events e
                   JOIN flow_signal_outcomes o USING(event_id)
                   WHERE e.policy_version=? AND e.cohort_id=?
                     AND e.signal_role='qualified'
                     AND e.eligible_for_evaluation=1
                     AND o.horizon_label=?
                     AND o.status IN ('observed','resolved','non_exitable')
                   ORDER BY e.signaled_at DESC,e.event_id DESC LIMIT ?
               )
               SELECT r.*,c.event_id AS control_event_id,
                      ce.quote_verified AS control_quote_verified,
                      ce.quote_exitable AS control_quote_exitable,
                      co.status AS control_outcome_status,
                      co.net_return AS control_net_return,
                      co.exit_valid AS control_exit_valid
               FROM recent r
               LEFT JOIN controls c ON c.matched_signal_event_id=r.event_id
               LEFT JOIN flow_signal_events ce ON ce.event_id=c.event_id
               LEFT JOIN flow_signal_outcomes co
                 ON co.event_id=c.event_id AND co.horizon_label=?""",
            (*population, HORIZON, MAXIMUM_SOURCE_ROWS, HORIZON),
        ).fetchall()
    finally:
        connection.close()

    rows: list[dict] = []
    for raw in raw_rows:
        outcome = _target(raw["outcome_status"], raw["net_return"],
                          raw["exit_valid"])
        if outcome is None:
            excluded["invalid_outcome"] += 1
            continue
        extracted = _entry_features(raw["snapshot_json"], raw["quote_json"])
        if extracted is None or not bool(raw["quote_verified"]):
            excluded["incomplete_entry_snapshot_or_quote"] += 1
            continue
        features, eligibility = extracted
        control = _target(
            raw["control_outcome_status"], raw["control_net_return"],
            raw["control_exit_valid"],
        )
        target_return, non_exitable = outcome
        source = str(raw["source_version"] or "unknown")
        marketable = bool(raw["quote_verified"] and raw["quote_exitable"])
        capital_executable = bool(
            marketable and str(raw["freshness"]) == "fresh"
            and eligibility["paper_eligible"]
            and source_permissions.get(source, False)
        )
        rows.append({
            "event_id": str(raw["event_id"]),
            "pool_id": str(raw["pool_id"]),
            "source_version": source,
            "signaled_at": _safe_float(raw["signaled_at"]),
            "freshness": str(raw["freshness"]),
            "features": features,
            "target_return": target_return,
            "non_exitable": non_exitable,
            "entry_marketable": marketable,
            "paper_eligible": eligibility["paper_eligible"],
            "capital_executable": capital_executable,
            "control_event_id": raw["control_event_id"],
            "control_target_return": control[0] if control else None,
            "control_non_exitable": control[1] if control else None,
            "control_entry_marketable": bool(
                raw["control_quote_verified"]
                and raw["control_quote_exitable"]),
        })
    rows.sort(key=lambda row: (row["signaled_at"], row["event_id"]))
    return rows, {
        **excluded,
        "flow_policy_version": policy_version,
        "flow_cohort_id": cohort_id,
    }


def _deduplicate_windows(rows: list[dict]) -> list[dict]:
    """Keep the first signal per pool per 15-minute bucket."""
    chosen: dict[tuple[str, int], dict] = {}
    for row in rows:
        key = (row["pool_id"], int(row["signaled_at"] // 900))
        chosen.setdefault(key, row)
    return sorted(chosen.values(), key=lambda row: (
        row["signaled_at"], row["event_id"]))


def _temporal_split(rows: list[dict]) -> tuple[list[dict], list[dict], dict]:
    """Forward-time split whose holdout contains only unseen pools.

    Past rows from a pool that later crosses the boundary remain valid train
    evidence.  Only its future rows are excluded, preventing pool identity
    from leaking from train into holdout without throwing away its history.
    """
    if not rows:
        return [], [], {
            "cutoff_epoch": None, "crossing_pool_future_rows_excluded": 0,
            "crossing_pools": 0,
            "split_rule": "forward_time_holdout_pool_disjoint",
        }
    cutoff_index = min(
        len(rows) - 1, max(0, int(len(rows) * TRAIN_FRACTION) - 1))
    cutoff = rows[cutoff_index]["signaled_at"]
    train = [row for row in rows if row["signaled_at"] <= cutoff]
    train_pools = {row["pool_id"] for row in train}
    future = [row for row in rows if row["signaled_at"] > cutoff]
    holdout = [row for row in future if row["pool_id"] not in train_pools]
    crossing = {row["pool_id"] for row in future
                if row["pool_id"] in train_pools}
    return train, holdout, {
        "cutoff_epoch": cutoff,
        "crossing_pool_future_rows_excluded": len(future) - len(holdout),
        "crossing_pools": len(crossing),
        "split_rule": "forward_time_holdout_pool_disjoint",
    }


def _empty_model() -> dict:
    return {
        "weights": {name: 0.0 for name in FEATURES},
        "normalization": {
            name: {"mean": 0.0, "stddev": 0.0} for name in FEATURES
        },
        "fit_target": "net_return_minus_tail_risk_penalty",
    }


def _fit(train: list[dict]) -> dict:
    if not train:
        return _empty_model()
    fit_targets = [
        row["target_return"] - TAIL_RISK_PENALTY * float(
            row["non_exitable"]
            or row["target_return"] <= CATASTROPHIC_RETURN)
        for row in train
    ]
    target_mean = statistics.fmean(fit_targets)
    raw: dict[str, float] = {}
    normalization: dict[str, dict] = {}
    for name in FEATURES:
        values = [row["features"][name] for row in train]
        mean = statistics.fmean(values)
        deviation = statistics.pstdev(values) if len(values) > 1 else 0.0
        covariance = 0.0 if deviation <= 1e-12 else statistics.fmean([
            (value - mean) * (target - target_mean)
            for value, target in zip(values, fit_targets)
        ])
        raw[name] = 0.0 if deviation <= 1e-12 else covariance / deviation
        normalization[name] = {"mean": mean, "stddev": deviation}
    scale = sum(abs(value) for value in raw.values()) or 1.0
    return {
        "weights": {
            name: max(-MAXIMUM_ABSOLUTE_WEIGHT, min(
                MAXIMUM_ABSOLUTE_WEIGHT, value / scale))
            for name, value in raw.items()
        },
        "normalization": normalization,
        "fit_target": "net_return_minus_tail_risk_penalty",
    }


def _score(row: dict, model: dict) -> float:
    score = 0.0
    for name, weight in model["weights"].items():
        normal = model["normalization"][name]
        deviation = _safe_float(normal["stddev"])
        z_value = 0.0 if deviation <= 1e-12 else (
            row["features"][name] - normal["mean"]) / deviation
        score += weight * max(-4.0, min(4.0, z_value))
    return score


def _metrics(rows: list[dict]) -> dict:
    returns = [row["target_return"] for row in rows]
    if not returns:
        return {
            "samples": 0, "pools": 0, "mean_return": None,
            "median_return": None, "loss_rate": None,
            "non_exit_rate": None, "catastrophic_rate": None,
            "mean_return_lower_95": None, "objective": None,
        }
    mean = statistics.fmean(returns)
    deviation = statistics.stdev(returns) if len(returns) > 1 else 0.0
    non_exit_rate = sum(row["non_exitable"] for row in rows) / len(rows)
    catastrophic_rate = sum(
        value <= CATASTROPHIC_RETURN for value in returns) / len(returns)
    return {
        "samples": len(rows),
        "pools": len({row["pool_id"] for row in rows}),
        "mean_return": round(mean, 6),
        "median_return": round(statistics.median(returns), 6),
        "loss_rate": round(sum(value < 0 for value in returns) / len(rows), 6),
        "non_exit_rate": round(non_exit_rate, 6),
        "catastrophic_rate": round(catastrophic_rate, 6),
        "mean_return_lower_95": round(
            mean - 1.96 * deviation / math.sqrt(len(rows)), 6),
        "objective": round(
            mean - TAIL_RISK_PENALTY * (
                non_exit_rate + catastrophic_rate), 6),
    }


def _paired_metrics(rows: list[dict], *, require_marketable_control: bool) -> dict:
    paired = [row for row in rows
              if row["control_target_return"] is not None
              and (not require_marketable_control
                   or row["control_entry_marketable"])]
    differences = [
        row["target_return"] - row["control_target_return"] for row in paired
    ]
    if not differences:
        return {
            "samples": 0, "mean_incremental_return": None,
            "median_incremental_return": None,
            "mean_incremental_lower_95": None,
            "control_marketability_required": require_marketable_control,
        }
    mean = statistics.fmean(differences)
    deviation = statistics.stdev(differences) if len(differences) > 1 else 0.0
    return {
        "samples": len(differences),
        "mean_incremental_return": round(mean, 6),
        "median_incremental_return": round(
            statistics.median(differences), 6),
        "mean_incremental_lower_95": round(
            mean - 1.96 * deviation / math.sqrt(len(differences)), 6),
        "control_marketability_required": require_marketable_control,
    }


def _choose_threshold(train: list[dict], model: dict) -> dict:
    """Choose solely on training data; holdout is never inspected here."""
    scored = sorted(
        ((_score(row, model), row) for row in train),
        key=lambda item: (item[0], item[1]["event_id"]),
    )
    if not scored:
        return {"selected": {"quantile": None, "threshold": 0.0,
                             "metrics": _metrics([])}, "candidates": []}
    candidates = []
    for quantile in (0.0, 0.25, 0.50, 0.60, 0.70, 0.80, 0.90):
        index = min(len(scored) - 1, int((len(scored) - 1) * quantile))
        threshold = scored[index][0]
        selected = [row for score, row in scored if score >= threshold]
        candidates.append({
            "quantile": quantile, "threshold": round(threshold, 12),
            "metrics": _metrics(selected),
        })
    eligible = [candidate for candidate in candidates
                if candidate["metrics"]["samples"]
                >= MINIMUM_SELECTED_TRAIN_ROWS]
    winner = max(eligible or candidates, key=lambda candidate: (
        candidate["metrics"]["objective"]
        if candidate["metrics"]["objective"] is not None
        else float("-inf"),
        candidate["metrics"]["samples"],
    ))
    return {"selected": winner, "candidates": candidates}


GENESIS_HASH = "0" * 64


def _open_ledger(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path, timeout=10.0)
    connection.row_factory = sqlite3.Row
    connection.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS shadow_learning_runs (
            sequence INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL UNIQUE,
            created_at REAL NOT NULL,
            schema_version INTEGER NOT NULL,
            policy_version TEXT NOT NULL,
            population_kind TEXT NOT NULL,
            flow_policy_version TEXT,
            flow_cohort_id TEXT,
            source_policy_version TEXT NOT NULL,
            source_revision TEXT NOT NULL,
            source_digest TEXT NOT NULL,
            learner_digest TEXT NOT NULL,
            evidence_hash TEXT NOT NULL UNIQUE,
            operational_cohort_id TEXT,
            cutoff_epoch REAL,
            train_rows INTEGER NOT NULL,
            holdout_rows INTEGER NOT NULL,
            marketable_holdout_rows INTEGER NOT NULL,
            capital_executable_holdout_rows INTEGER NOT NULL,
            status TEXT NOT NULL,
            report_json TEXT NOT NULL,
            previous_hash TEXT NOT NULL,
            record_hash TEXT NOT NULL UNIQUE,
            promotion_enabled INTEGER NOT NULL CHECK(promotion_enabled=0)
        );
        CREATE TRIGGER IF NOT EXISTS shadow_learning_runs_no_update
        BEFORE UPDATE ON shadow_learning_runs
        BEGIN SELECT RAISE(ABORT, 'shadow learning ledger is append-only'); END;
        CREATE TRIGGER IF NOT EXISTS shadow_learning_runs_no_delete
        BEFORE DELETE ON shadow_learning_runs
        BEGIN SELECT RAISE(ABORT, 'shadow learning ledger is append-only'); END;
    """)
    return connection


def _record_hash(previous_hash: str, evidence_hash: str,
                 report_json: str) -> str:
    report_hash = hashlib.sha256(report_json.encode("utf-8")).hexdigest()
    return hashlib.sha256(
        f"{previous_hash}:{evidence_hash}:{report_hash}".encode("ascii")
    ).hexdigest()


def _append_report(path: Path, report: dict) -> dict:
    ledger = _open_ledger(path)
    report_json = _canonical(report)
    evidence_hash = report["source"]["evidence_hash"]
    try:
        ledger.execute("BEGIN IMMEDIATE")
        existing = ledger.execute(
            "SELECT report_json FROM shadow_learning_runs WHERE evidence_hash=?",
            (evidence_hash,),
        ).fetchone()
        if existing:
            ledger.rollback()
            return json.loads(existing["report_json"])
        previous = ledger.execute(
            "SELECT record_hash FROM shadow_learning_runs "
            "ORDER BY sequence DESC LIMIT 1"
        ).fetchone()
        previous_hash = str(previous["record_hash"]) if previous else GENESIS_HASH
        record_hash = _record_hash(previous_hash, evidence_hash, report_json)
        ledger.execute(
            """INSERT INTO shadow_learning_runs
               (run_id,created_at,schema_version,policy_version,
                population_kind,flow_policy_version,flow_cohort_id,
                source_policy_version,source_revision,source_digest,
                learner_digest,evidence_hash,operational_cohort_id,
                cutoff_epoch,train_rows,holdout_rows,
                marketable_holdout_rows,capital_executable_holdout_rows,
                status,report_json,previous_hash,record_hash,promotion_enabled)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0)""",
            (
                uuid.uuid4().hex, report["created_at"], SCHEMA_VERSION,
                POLICY_VERSION, POPULATION_KIND,
                report["source"]["flow_policy_version"],
                report["source"]["flow_cohort_id"],
                report["source"]["operational_policy_version"],
                report["source"]["revision"],
                report["source"]["source_digest"],
                report["source"]["learner_digest"], evidence_hash,
                report["operational_evidence"].get("cohort_id"),
                report["data_quality"]["cutoff_epoch"],
                report["data_quality"]["train_rows"],
                report["data_quality"]["holdout_rows"],
                report["champion"]["marketable_holdout_metrics"]["samples"],
                report["challenger"]["capital_executable_holdout_metrics"]["samples"],
                report["governance"]["status"], report_json,
                previous_hash, record_hash,
            ),
        )
        ledger.commit()
        return report
    except Exception:
        ledger.rollback()
        raise
    finally:
        ledger.close()


def verify_shadow_ledger(root: str | Path) -> dict:
    path = Path(root) / LEDGER_NAME
    if not path.exists():
        return {"ok": True, "records": 0, "head": GENESIS_HASH}
    ledger = _open_read_only(path)
    try:
        rows = ledger.execute(
            "SELECT sequence,evidence_hash,report_json,previous_hash,record_hash "
            "FROM shadow_learning_runs ORDER BY sequence"
        ).fetchall()
    finally:
        ledger.close()
    previous = GENESIS_HASH
    for row in rows:
        expected = _record_hash(
            previous, str(row["evidence_hash"]), str(row["report_json"]))
        if row["previous_hash"] != previous or row["record_hash"] != expected:
            return {
                "ok": False, "records": len(rows),
                "failed_sequence": int(row["sequence"]), "head": previous,
            }
        previous = str(row["record_hash"])
    return {"ok": True, "records": len(rows), "head": previous}


def _gate(value: bool, detail: Any) -> dict:
    return {"pass": bool(value), "value": detail}


def run_shadow_learning(
    root: str | Path, *, source_policy_version: str,
    source_revision: str, source_digest: str = "unknown",
    source_permissions: dict[str, bool] | None = None,
    operational_evidence: dict | None = None,
) -> dict:
    """Evaluate one immutable shadow challenger and append one audit record."""
    root = Path(root)
    started = time.time()
    permissions = {
        str(source): bool(permitted)
        for source, permitted in (source_permissions or {}).items()
    }
    operational = {
        "stabilized": bool((operational_evidence or {}).get("stabilized")),
        "cohort_id": (operational_evidence or {}).get("cohort_id"),
        "revision": (operational_evidence or {}).get("revision"),
        "policy_hash": (operational_evidence or {}).get("policy_hash"),
        "criteria_passed": int(
            (operational_evidence or {}).get("criteria_passed") or 0),
        "criteria_total": int(
            (operational_evidence or {}).get("criteria_total") or 0),
    }
    rows, load = _load_rows(root / "learning.sqlite3", permissions)
    deduplicated = _deduplicate_windows(rows)
    train, holdout, split = _temporal_split(deduplicated)
    train_marketable = [row for row in train if row["entry_marketable"]]
    holdout_marketable = [row for row in holdout if row["entry_marketable"]]
    model = _fit(train_marketable)
    threshold_search = _choose_threshold(train_marketable, model)
    boundary = threshold_search["selected"]["threshold"]
    challenger_holdout = [
        row for row in holdout_marketable if _score(row, model) >= boundary
    ]
    challenger_capital = [
        row for row in challenger_holdout if row["capital_executable"]
    ]
    champion_capital = [
        row for row in holdout_marketable if row["capital_executable"]
    ]
    champion_metrics = _metrics(holdout_marketable)
    challenger_metrics = _metrics(challenger_holdout)
    challenger_capital_metrics = _metrics(challenger_capital)
    paired_research = _paired_metrics(
        challenger_holdout, require_marketable_control=False)
    paired_marketable = _paired_metrics(
        challenger_holdout, require_marketable_control=True)

    economic_gates = {
        "minimum_marketable_train_sample": _gate(
            len(train_marketable) >= MINIMUM_TRAIN_ROWS,
            {"actual": len(train_marketable), "minimum": MINIMUM_TRAIN_ROWS}),
        "minimum_marketable_holdout_sample": _gate(
            len(holdout_marketable) >= MINIMUM_HOLDOUT_ROWS,
            {"actual": len(holdout_marketable), "minimum": MINIMUM_HOLDOUT_ROWS}),
        "minimum_selected_holdout_sample": _gate(
            challenger_metrics["samples"] >= MINIMUM_SELECTED_HOLDOUT_ROWS,
            {"actual": challenger_metrics["samples"],
             "minimum": MINIMUM_SELECTED_HOLDOUT_ROWS}),
        "minimum_marketable_paired_sample": _gate(
            paired_marketable["samples"] >= MINIMUM_PAIRED_HOLDOUT_ROWS,
            {"actual": paired_marketable["samples"],
             "minimum": MINIMUM_PAIRED_HOLDOUT_ROWS}),
        "positive_absolute_return_lower_bound": _gate(
            challenger_metrics["mean_return_lower_95"] is not None
            and challenger_metrics["mean_return_lower_95"] > 0,
            challenger_metrics["mean_return_lower_95"]),
        "positive_paired_edge_lower_bound": _gate(
            paired_marketable["mean_incremental_lower_95"] is not None
            and paired_marketable["mean_incremental_lower_95"] > 0,
            paired_marketable["mean_incremental_lower_95"]),
        "bounded_non_exit_rate": _gate(
            challenger_metrics["non_exit_rate"] is not None
            and challenger_metrics["non_exit_rate"] <= MAXIMUM_NONEXIT_RATE,
            {"actual": challenger_metrics["non_exit_rate"],
             "maximum": MAXIMUM_NONEXIT_RATE}),
        "bounded_catastrophic_rate": _gate(
            challenger_metrics["catastrophic_rate"] is not None
            and challenger_metrics["catastrophic_rate"]
            <= MAXIMUM_CATASTROPHIC_RATE,
            {"actual": challenger_metrics["catastrophic_rate"],
             "maximum": MAXIMUM_CATASTROPHIC_RATE}),
        "objective_improves_on_champion": _gate(
            challenger_metrics["objective"] is not None
            and champion_metrics["objective"] is not None
            and challenger_metrics["objective"] > champion_metrics["objective"],
            {"challenger": challenger_metrics["objective"],
             "champion": champion_metrics["objective"]}),
    }
    economic_pass = all(gate["pass"] for gate in economic_gates.values())
    operational_pass = bool(
        operational["stabilized"]
        and operational["criteria_total"] > 0
        and operational["criteria_passed"] == operational["criteria_total"])
    population_sources = sorted({row["source_version"] for row in deduplicated})
    selected_sources = sorted({row["source_version"]
                               for row in challenger_holdout})
    source_pass = bool(selected_sources) and all(
        permissions.get(source, False) for source in selected_sources)
    if not all(economic_gates[name]["pass"] for name in (
            "minimum_marketable_train_sample",
            "minimum_marketable_holdout_sample",
            "minimum_selected_holdout_sample",
            "minimum_marketable_paired_sample")):
        status = "insufficient_prospective_evidence"
    elif not economic_pass:
        status = "challenger_not_economically_valid"
    elif not operational_pass:
        status = "operational_proof_required"
    elif not source_pass:
        status = "source_admission_blocked"
    else:
        status = "shadow_candidate_ready_for_acceptance"

    lower = challenger_metrics["mean_return_lower_95"]
    if len(train_marketable) < MINIMUM_TRAIN_ROWS or (
            len(holdout_marketable) < MINIMUM_HOLDOUT_ROWS):
        next_capability = "collect_more_marketable_prospective_outcomes"
    elif lower is None or lower <= 0:
        next_capability = "ordered_prospective_shadow_price_paths"
    elif paired_marketable["mean_incremental_lower_95"] is None or (
            paired_marketable["mean_incremental_lower_95"] <= 0):
        next_capability = "improve_marketable_matched_control_design"
    elif not source_pass:
        next_capability = "v4_custody_hook_and_source_admission_proof"
    else:
        next_capability = "frozen_prospective_challenger_acceptance_cohort"

    manifest = [{
        "event_id": row["event_id"],
        "pool_id": row["pool_id"],
        "signaled_at": row["signaled_at"],
        "source_version": row["source_version"],
        "feature_hash": _hash(row["features"]),
        "target_return": row["target_return"],
        "non_exitable": row["non_exitable"],
        "entry_marketable": row["entry_marketable"],
        "capital_executable": row["capital_executable"],
        "control_event_id": row["control_event_id"],
        "control_target_return": row["control_target_return"],
        "control_entry_marketable": row["control_entry_marketable"],
    } for row in deduplicated]
    learner_digest = hashlib.sha256(
        Path(__file__).resolve().read_bytes()).hexdigest()
    evidence_hash = _hash({
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "flow_policy_version": load["flow_policy_version"],
        "flow_cohort_id": load["flow_cohort_id"],
        "source_policy_version": source_policy_version,
        "source_revision": source_revision,
        "source_digest": source_digest,
        "learner_digest": learner_digest,
        "source_permissions": permissions,
        "operational_evidence": operational,
        "manifest": manifest,
    })
    report = {
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "population_kind": POPULATION_KIND,
        "created_at": time.time(),
        "status": status,
        "promotion_enabled": False,
        "source": {
            "operational_policy_version": source_policy_version,
            "revision": source_revision,
            "source_digest": source_digest,
            "learner_digest": learner_digest,
            "flow_policy_version": load["flow_policy_version"],
            "flow_cohort_id": load["flow_cohort_id"],
            "horizon": HORIZON,
            "evidence_hash": evidence_hash,
            "evidence_manifest_rows": len(manifest),
            "target_policy": {
                "resolved": "winsorized_net_return",
                "invalid_or_non_exitable_exit": RETURN_FLOOR,
                "return_bounds": [RETURN_FLOOR, RETURN_CEILING],
            },
        },
        "operational_evidence": operational,
        "data_quality": {
            "source_rows_with_complete_entry_features": len(rows),
            "deduplicated_pool_horizon_rows": len(deduplicated),
            "excluded": {key: value for key, value in load.items()
                         if key not in {"flow_policy_version", "flow_cohort_id"}},
            **split,
            "train_rows": len(train),
            "holdout_rows": len(holdout),
            "marketable_train_rows": len(train_marketable),
            "marketable_holdout_rows": len(holdout_marketable),
            "minimum_marketable_train_rows": MINIMUM_TRAIN_ROWS,
            "minimum_marketable_holdout_rows": MINIMUM_HOLDOUT_ROWS,
            "features_are_signal_time_only": True,
            "holdout_was_used_for_threshold_selection": False,
        },
        "champion": {
            "definition": "all marketable qualified Flow signals",
            "marketable_holdout_metrics": champion_metrics,
            "capital_executable_holdout_metrics": _metrics(champion_capital),
        },
        "challenger": {
            "model": model,
            "threshold_search": threshold_search,
            "marketable_holdout_metrics": challenger_metrics,
            "capital_executable_holdout_metrics": challenger_capital_metrics,
            "paired_research_metrics": paired_research,
            "paired_marketable_control_metrics": paired_marketable,
            "bounded_absolute_weight": MAXIMUM_ABSOLUTE_WEIGHT,
            "hard_gates_mutable": False,
        },
        "execution_boundary": {
            "entry_marketable_definition":
                "verified same-block entry quote and bounded round-trip exit",
            "capital_executable_definition":
                "entry marketable, fresh, identity/policy eligible, and source admitted",
            "population_sources": population_sources,
            "selected_sources": selected_sources,
            "source_permissions": permissions,
            "all_selected_sources_admitted": source_pass,
            "selected_marketable_rows": len(challenger_holdout),
            "selected_capital_executable_rows": len(challenger_capital),
        },
        "governance": {
            "status": status,
            "economic_gates": economic_gates,
            "economic_evidence_pass": economic_pass,
            "operational_13_of_13_pass": operational_pass,
            "source_admission_pass": source_pass,
            "promotion_enabled": False,
            "promotion_eligible": False,
            "active_policy_changed": False,
            "single_trade_updates_allowed": False,
            "live_execution_path_present": False,
            "next_required_capability": next_capability,
            "promotion_disabled_reason": (
                "recursive-learning-v2 is shadow-only; a separately approved, "
                "frozen challenger acceptance cohort is required"),
        },
        "duration_seconds": round(time.time() - started, 3),
    }
    stored_report = _append_report(root / LEDGER_NAME, report)
    _atomic_json(root / ARTIFACT_NAME, stored_report)
    return stored_report


def _not_run(status: str = "not_run") -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "policy_version": POLICY_VERSION,
        "status": status,
        "promotion_enabled": False,
        "governance": {
            "status": status,
            "promotion_enabled": False,
            "promotion_eligible": False,
            "active_policy_changed": False,
        },
    }


def latest_shadow_learning(root: str | Path) -> dict:
    root = Path(root)
    for name in (ARTIFACT_NAME, LEGACY_ARTIFACT_NAME):
        path = root / name
        if not path.exists():
            continue
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(report, dict):
                raise ValueError("shadow report is not an object")
            report["promotion_enabled"] = False
            governance = report.setdefault("governance", {})
            governance["promotion_enabled"] = False
            report.setdefault("status", governance.get("status", "unknown"))
            report["artifact_version"] = "v2" if name == ARTIFACT_NAME else "legacy_v1"
            return report
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return _not_run("unreadable")
    return _not_run()
