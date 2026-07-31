"""Deterministic, evidence-aware benchmark evaluation for Chainseer.

The benchmark never calls a live analyzer. It evaluates immutable prediction
records against outcomes observed later, so every analyzer can be compared on
the same evidence cutoff. The generated report contains canonical input hashes
and a compact seal payload that can be committed to the Chainseer Timechain by
the cognitive runner after review and PoQ gating.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


BENCHMARK_SCHEMA_VERSION = "1.0"
CASEBANK_SCHEMA_VERSION = "1.0"
SUPPORTED_PUBLIC_REPORT_SCHEMAS = {"1.1", "1.2"}
MIN_BENIGN_OBSERVATION_SECONDS = 7 * 24 * 60 * 60
NETWORKS = {"robinhood", "solana"}
LABELS = {"adverse_security", "benign", "infrastructure_failure"}
SPLITS = {"train", "validation", "test"}
POSITIVE_RISKS = {"critical", "high"}
POSITIVE_ACTIONS = {
    "avoid",
    "reject",
    "hard_stop",
    "hard stop",
    "do_not_buy",
    "do not buy",
}
HASH_RE = re.compile(r"^[a-fA-F0-9]{64}$")


class BenchmarkValidationError(ValueError):
    """The dataset or prediction bank violates benchmark invariants."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _parse_iso(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise BenchmarkValidationError(f"{field} must be an ISO-8601 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BenchmarkValidationError(
            f"{field} must be an ISO-8601 timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise BenchmarkValidationError(f"{field} must include a timezone")
    return parsed


def _finite_float(
    value: Any,
    field: str,
    *,
    minimum: float | None = None,
    maximum: float | None = None,
    optional: bool = False,
) -> float | None:
    if value is None and optional:
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise BenchmarkValidationError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise BenchmarkValidationError(f"{field} must be finite")
    if minimum is not None and result < minimum:
        raise BenchmarkValidationError(f"{field} must be >= {minimum}")
    if maximum is not None and result > maximum:
        raise BenchmarkValidationError(f"{field} must be <= {maximum}")
    return result


def load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    source = Path(path)
    if not source.exists():
        return []
    records: list[dict[str, Any]] = []
    for line_number, raw in enumerate(
        source.read_text(encoding="utf-8").splitlines(),
        start=1,
    ):
        if not raw.strip():
            continue
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise BenchmarkValidationError(
                f"{path}:{line_number} is not valid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise BenchmarkValidationError(
                f"{path}:{line_number} must contain a JSON object"
            )
        records.append(value)
    return records


def _record_hash(record: dict[str, Any], field: str) -> str:
    return canonical_hash(
        {key: value for key, value in record.items() if key != field}
    )


def _append_jsonl(path: str | Path, record: dict[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    encoded = (canonical_json(record) + "\n").encode("utf-8")
    descriptor = os.open(
        destination,
        os.O_APPEND
        | os.O_CREAT
        | os.O_WRONLY
        | getattr(os, "O_BINARY", 0),
        0o600,
    )
    try:
        remaining = encoded
        while remaining:
            written = os.write(descriptor, remaining)
            if written <= 0:
                raise OSError("append-only JSONL write made no progress")
            remaining = remaining[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_new_jsonl(
    path: str | Path,
    records: Iterable[dict[str, Any]],
) -> None:
    destination = Path(path)
    if destination.exists():
        raise BenchmarkValidationError(
            f"refusing to overwrite immutable benchmark snapshot: {destination}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(destination.name + ".tmp")
    if temporary.exists():
        temporary.unlink()
    with temporary.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(canonical_json(record) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, destination)


def _validate_prediction_fields(
    prediction: dict[str, Any],
    prefix: str,
    expected_cutoff: datetime,
) -> None:
    analyzer = str(prediction.get("analyzer") or "").strip()
    version = str(prediction.get("analyzer_version") or "").strip()
    if not analyzer or not version:
        raise BenchmarkValidationError(
            f"{prefix}.analyzer and analyzer_version are required"
        )
    analyzed_at = _parse_iso(
        prediction.get("analyzed_at"),
        f"{prefix}.analyzed_at",
    )
    cutoff = _parse_iso(
        prediction.get("evidence_cutoff"),
        f"{prefix}.evidence_cutoff",
    )
    if cutoff != expected_cutoff:
        raise BenchmarkValidationError(
            f"{prefix}.evidence_cutoff must exactly match its case"
        )
    if analyzed_at < cutoff:
        raise BenchmarkValidationError(
            f"{prefix}.analyzed_at cannot predate evidence_cutoff"
        )
    if prediction.get("legitimacy_score") is not None:
        _finite_float(
            prediction["legitimacy_score"],
            f"{prefix}.legitimacy_score",
            minimum=0,
            maximum=100,
        )
    if prediction.get("risk_probability") is not None:
        _finite_float(
            prediction["risk_probability"],
            f"{prefix}.risk_probability",
            minimum=0,
            maximum=1,
        )
    _finite_float(
        prediction.get("latency_ms"),
        f"{prefix}.latency_ms",
        minimum=0,
    )
    _finite_float(
        prediction.get("evidence_age_seconds"),
        f"{prefix}.evidence_age_seconds",
        minimum=0,
    )
    if not isinstance(prediction.get("infrastructure_indeterminate"), bool):
        raise BenchmarkValidationError(
            f"{prefix}.infrastructure_indeterminate must be boolean"
        )
    report_hash = prediction.get("report_hash")
    if report_hash is not None and not HASH_RE.fullmatch(str(report_hash)):
        raise BenchmarkValidationError(
            f"{prefix}.report_hash must be a SHA-256 hex digest"
        )


def validate_cases(records: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    cases = list(records)
    seen: set[str] = set()
    for index, case in enumerate(cases):
        prefix = f"case[{index}]"
        case_id = str(case.get("case_id") or "").strip()
        if not case_id:
            raise BenchmarkValidationError(f"{prefix}.case_id is required")
        if case_id in seen:
            raise BenchmarkValidationError(f"duplicate case_id: {case_id}")
        seen.add(case_id)
        if case.get("network") not in NETWORKS:
            raise BenchmarkValidationError(
                f"{prefix}.network must be one of {sorted(NETWORKS)}"
            )
        if case.get("label") not in LABELS:
            raise BenchmarkValidationError(
                f"{prefix}.label must be one of {sorted(LABELS)}"
            )
        if case.get("split") not in SPLITS:
            raise BenchmarkValidationError(
                f"{prefix}.split must be one of {sorted(SPLITS)}"
            )
        if not str(case.get("cohort") or "").strip():
            raise BenchmarkValidationError(f"{prefix}.cohort is required")
        if not str(case.get("token_address") or "").strip():
            raise BenchmarkValidationError(
                f"{prefix}.token_address is required"
            )
        cutoff = _parse_iso(case.get("evidence_cutoff"), f"{prefix}.evidence_cutoff")
        outcome = _parse_iso(
            case.get("outcome_observed_at"),
            f"{prefix}.outcome_observed_at",
        )
        if outcome <= cutoff:
            raise BenchmarkValidationError(
                f"{prefix}.outcome_observed_at must be after evidence_cutoff"
            )
        if (
            case.get("label") == "benign"
            and (outcome - cutoff).total_seconds()
            < MIN_BENIGN_OBSERVATION_SECONDS
        ):
            raise BenchmarkValidationError(
                f"{prefix} benign outcomes require seven days of observation"
            )
        refs = case.get("outcome_evidence_refs")
        if (
            not isinstance(refs, list)
            or not refs
            or any(not str(item).strip() for item in refs)
        ):
            raise BenchmarkValidationError(
                f"{prefix}.outcome_evidence_refs must be a non-empty list"
            )
    return cases


def validate_predictions(
    records: Iterable[dict[str, Any]],
    cases: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    predictions = list(records)
    case_by_id = {case["case_id"]: case for case in cases}
    seen: set[tuple[str, str, str]] = set()
    for index, prediction in enumerate(predictions):
        prefix = f"prediction[{index}]"
        case_id = str(prediction.get("case_id") or "").strip()
        if case_id not in case_by_id:
            raise BenchmarkValidationError(
                f"{prefix}.case_id does not exist in the case bank"
            )
        analyzer = str(prediction.get("analyzer") or "").strip()
        version = str(prediction.get("analyzer_version") or "").strip()
        if not analyzer or not version:
            raise BenchmarkValidationError(
                f"{prefix}.analyzer and analyzer_version are required"
            )
        identity = (analyzer, version, case_id)
        if identity in seen:
            raise BenchmarkValidationError(
                f"duplicate prediction for {analyzer}@{version}:{case_id}"
            )
        seen.add(identity)
        case_cutoff = _parse_iso(
            case_by_id[case_id]["evidence_cutoff"],
            f"case[{case_id}].evidence_cutoff",
        )
        _validate_prediction_fields(prediction, prefix, case_cutoff)
    return predictions


def _normalize_network(public_report: dict[str, Any]) -> str:
    token = public_report.get("token") or {}
    chain = str(token.get("chain") or "").strip().lower()
    chain_id = str(token.get("chain_id") or "").strip().lower()
    if "solana" in chain or chain_id == "mainnet-beta":
        return "solana"
    if "robinhood" in chain or chain_id == "4663":
        return "robinhood"
    raise BenchmarkValidationError(
        "public report does not identify a supported benchmark network"
    )


def _derive_evidence_age_seconds(
    public_report: dict[str, Any],
    cutoff: datetime,
) -> float | None:
    timestamps = []
    for fact in (public_report.get("evidence") or {}).get("facts") or []:
        value = fact.get("timestamp")
        if not value:
            continue
        try:
            observed = _parse_iso(value, "evidence.fact.timestamp")
        except BenchmarkValidationError:
            continue
        if observed <= cutoff:
            timestamps.append(observed)
    if not timestamps:
        return None
    return max(0.0, (cutoff - max(timestamps)).total_seconds())


def build_observation_from_report(
    document: dict[str, Any],
    *,
    cohort: str,
    split: str,
    analyzer: str,
    analyzer_version: str,
    latency_ms: float | None = None,
    evidence_age_seconds: float | None = None,
    risk_probability: float | None = None,
    case_id: str | None = None,
    captured_at: str | None = None,
) -> dict[str, Any]:
    """Freeze one public report as a future-outcome benchmark observation."""
    wrapper = document if isinstance(document.get("result"), dict) else {}
    report = wrapper.get("result") or document
    if not isinstance(report, dict):
        raise BenchmarkValidationError("report document must be a JSON object")
    if str(report.get("schema_version") or "") not in (
        SUPPORTED_PUBLIC_REPORT_SCHEMAS
    ):
        raise BenchmarkValidationError(
            "capture requires a supported Chainseer public report schema: "
            + ", ".join(sorted(SUPPORTED_PUBLIC_REPORT_SCHEMAS))
        )
    if split not in SPLITS:
        raise BenchmarkValidationError(
            f"split must be one of {sorted(SPLITS)}"
        )
    cohort = str(cohort or "").strip()
    if not cohort:
        raise BenchmarkValidationError("cohort is required")
    analyzer = str(analyzer or "").strip()
    analyzer_version = str(analyzer_version or "").strip()
    if not analyzer or not analyzer_version:
        raise BenchmarkValidationError(
            "analyzer and analyzer_version are required"
        )

    token = report.get("token") or {}
    token_address = str(token.get("address") or "").strip()
    if not token_address:
        raise BenchmarkValidationError(
            "public report token.address is required"
        )
    network = _normalize_network(report)
    cutoff_text = str(report.get("analyzed_at") or "").strip()
    cutoff = _parse_iso(cutoff_text, "report.analyzed_at")

    if latency_ms is None and wrapper:
        started = _parse_iso(wrapper.get("started_at"), "job.started_at")
        finished = _parse_iso(wrapper.get("finished_at"), "job.finished_at")
        latency_ms = max(0.0, (finished - started).total_seconds() * 1000)
    if latency_ms is None:
        raise BenchmarkValidationError(
            "latency_ms is required when the report is not wrapped in a job response"
        )
    latency_ms = _finite_float(
        latency_ms,
        "latency_ms",
        minimum=0,
    )

    if evidence_age_seconds is None:
        evidence_age_seconds = _derive_evidence_age_seconds(report, cutoff)
    if evidence_age_seconds is None:
        raise BenchmarkValidationError(
            "evidence_age_seconds is required when fact timestamps are unavailable"
        )
    evidence_age_seconds = _finite_float(
        evidence_age_seconds,
        "evidence_age_seconds",
        minimum=0,
    )
    if risk_probability is not None:
        risk_probability = _finite_float(
            risk_probability,
            "risk_probability",
            minimum=0,
            maximum=1,
        )

    captured_text = captured_at or datetime.now(timezone.utc).isoformat()
    captured = _parse_iso(captured_text, "captured_at")
    analyzed_text = str(wrapper.get("finished_at") or cutoff_text)
    analyzed = _parse_iso(analyzed_text, "analyzed_at")
    if analyzed < cutoff:
        raise BenchmarkValidationError(
            "job.finished_at cannot predate report.analyzed_at"
        )
    if captured < analyzed:
        raise BenchmarkValidationError(
            "captured_at cannot predate analyzed_at"
        )

    identity = {
        "network": network,
        "token_address": (
            token_address.lower() if network == "robinhood" else token_address
        ),
        "evidence_cutoff": cutoff_text,
    }
    resolved_case_id = str(case_id or "").strip()
    if not resolved_case_id:
        resolved_case_id = f"case-{canonical_hash(identity)[:16]}"

    decision = report.get("decision") or {}
    hard_stops = decision.get("hard_stops") or []
    infrastructure = (
        (report.get("evidence") or {}).get("infrastructure_indeterminate")
        or []
    )
    observation = {
        "record_type": "prediction_observation",
        "casebank_schema_version": CASEBANK_SCHEMA_VERSION,
        "case_id": resolved_case_id,
        "network": network,
        "cohort": cohort,
        "token_address": identity["token_address"],
        "split": split,
        "evidence_cutoff": cutoff_text,
        "analyzer": analyzer,
        "analyzer_version": analyzer_version,
        "analyzed_at": analyzed_text,
        "risk_level": decision.get("risk_level") or "Unknown",
        "action": decision.get("action") or "REVIEW",
        "hard_stop_count": len(hard_stops),
        "legitimacy_score": decision.get("legitimacy_score"),
        "infrastructure_indeterminate": bool(infrastructure),
        "latency_ms": round(float(latency_ms), 3),
        "evidence_age_seconds": round(float(evidence_age_seconds), 3),
        "report_hash": canonical_hash(report),
        "captured_at": captured_text,
        "source_schema_version": report.get("schema_version"),
        "anchor_type": (report.get("evidence") or {}).get("anchor_type"),
        "anchor_value": (report.get("evidence") or {}).get("block_pin"),
        "timechain_ring": (report.get("timechain") or {}).get("ring"),
        "timechain_ring_hash": (report.get("timechain") or {}).get(
            "ring_hash"
        ),
    }
    if risk_probability is not None:
        observation["risk_probability"] = risk_probability
    observation["observation_hash"] = _record_hash(
        observation,
        "observation_hash",
    )
    validate_observations([observation])
    return observation


def validate_observations(
    records: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    observations = list(records)
    identities: set[tuple[str, str, str]] = set()
    case_metadata: dict[str, tuple[Any, ...]] = {}
    for index, observation in enumerate(observations):
        prefix = f"observation[{index}]"
        if observation.get("record_type") != "prediction_observation":
            raise BenchmarkValidationError(
                f"{prefix}.record_type must be prediction_observation"
            )
        if observation.get("casebank_schema_version") != CASEBANK_SCHEMA_VERSION:
            raise BenchmarkValidationError(
                f"{prefix}.casebank_schema_version is unsupported"
            )
        case_id = str(observation.get("case_id") or "").strip()
        if not case_id:
            raise BenchmarkValidationError(f"{prefix}.case_id is required")
        if observation.get("network") not in NETWORKS:
            raise BenchmarkValidationError(
                f"{prefix}.network must be one of {sorted(NETWORKS)}"
            )
        if observation.get("split") not in SPLITS:
            raise BenchmarkValidationError(
                f"{prefix}.split must be one of {sorted(SPLITS)}"
            )
        if not str(observation.get("cohort") or "").strip():
            raise BenchmarkValidationError(f"{prefix}.cohort is required")
        if not str(observation.get("token_address") or "").strip():
            raise BenchmarkValidationError(
                f"{prefix}.token_address is required"
            )
        cutoff = _parse_iso(
            observation.get("evidence_cutoff"),
            f"{prefix}.evidence_cutoff",
        )
        _validate_prediction_fields(observation, prefix, cutoff)
        captured = _parse_iso(
            observation.get("captured_at"),
            f"{prefix}.captured_at",
        )
        analyzed = _parse_iso(
            observation.get("analyzed_at"),
            f"{prefix}.analyzed_at",
        )
        if captured < analyzed:
            raise BenchmarkValidationError(
                f"{prefix}.captured_at cannot predate analyzed_at"
            )
        observation_hash = str(
            observation.get("observation_hash") or ""
        )
        if not HASH_RE.fullmatch(observation_hash):
            raise BenchmarkValidationError(
                f"{prefix}.observation_hash must be a SHA-256 hex digest"
            )
        if observation_hash != _record_hash(
            observation,
            "observation_hash",
        ):
            raise BenchmarkValidationError(
                f"{prefix}.observation_hash does not match record content"
            )
        identity = (
            str(observation.get("analyzer")),
            str(observation.get("analyzer_version")),
            case_id,
        )
        if identity in identities:
            raise BenchmarkValidationError(
                f"duplicate observation for {identity[0]}@{identity[1]}:{case_id}"
            )
        identities.add(identity)
        metadata = (
            observation.get("network"),
            observation.get("cohort"),
            observation.get("token_address"),
            observation.get("split"),
            observation.get("evidence_cutoff"),
            observation.get("anchor_type"),
            observation.get("anchor_value"),
        )
        previous = case_metadata.setdefault(case_id, metadata)
        if previous != metadata:
            raise BenchmarkValidationError(
                f"case {case_id} has conflicting observation metadata"
            )
    return observations


def append_observation(
    path: str | Path,
    observation: dict[str, Any],
) -> dict[str, Any]:
    existing = load_jsonl(path)
    validate_observations([*existing, observation])
    _append_jsonl(path, observation)
    return observation


def build_outcome_record(
    observations: Iterable[dict[str, Any]],
    *,
    case_id: str,
    label: str,
    outcome_observed_at: str,
    evidence_refs: Iterable[str],
    reviewer: str,
    notes: str | None = None,
) -> dict[str, Any]:
    checked = validate_observations(observations)
    case_observations = [
        item for item in checked if item["case_id"] == case_id
    ]
    if not case_observations:
        raise BenchmarkValidationError(
            f"no captured observation exists for case {case_id}"
        )
    if label not in LABELS:
        raise BenchmarkValidationError(
            f"label must be one of {sorted(LABELS)}"
        )
    reviewer = str(reviewer or "").strip()
    if not reviewer:
        raise BenchmarkValidationError("reviewer is required")
    refs = [str(item).strip() for item in evidence_refs if str(item).strip()]
    if not refs:
        raise BenchmarkValidationError(
            "at least one outcome evidence reference is required"
        )
    cutoff = _parse_iso(
        case_observations[0]["evidence_cutoff"],
        "evidence_cutoff",
    )
    observed = _parse_iso(outcome_observed_at, "outcome_observed_at")
    if observed <= cutoff:
        raise BenchmarkValidationError(
            "outcome_observed_at must be after evidence_cutoff"
        )
    window_seconds = (observed - cutoff).total_seconds()
    if (
        label == "benign"
        and window_seconds < MIN_BENIGN_OBSERVATION_SECONDS
    ):
        raise BenchmarkValidationError(
            "benign outcomes require at least seven days of observation"
        )
    outcome = {
        "record_type": "outcome_observation",
        "casebank_schema_version": CASEBANK_SCHEMA_VERSION,
        "case_id": case_id,
        "label": label,
        "outcome_observed_at": outcome_observed_at,
        "outcome_evidence_refs": refs,
        "reviewer": reviewer,
        "observation_window_seconds": round(window_seconds, 3),
    }
    if notes:
        outcome["notes"] = str(notes)
    outcome["outcome_hash"] = _record_hash(outcome, "outcome_hash")
    validate_outcomes([outcome], checked)
    return outcome


def validate_outcomes(
    records: Iterable[dict[str, Any]],
    observations: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    outcomes = list(records)
    checked_observations = validate_observations(observations)
    case_by_id = {
        item["case_id"]: item for item in checked_observations
    }
    seen: set[str] = set()
    for index, outcome in enumerate(outcomes):
        prefix = f"outcome[{index}]"
        if outcome.get("record_type") != "outcome_observation":
            raise BenchmarkValidationError(
                f"{prefix}.record_type must be outcome_observation"
            )
        if outcome.get("casebank_schema_version") != CASEBANK_SCHEMA_VERSION:
            raise BenchmarkValidationError(
                f"{prefix}.casebank_schema_version is unsupported"
            )
        case_id = str(outcome.get("case_id") or "").strip()
        if case_id not in case_by_id:
            raise BenchmarkValidationError(
                f"{prefix}.case_id has no captured observation"
            )
        if case_id in seen:
            raise BenchmarkValidationError(
                f"duplicate outcome for case {case_id}"
            )
        seen.add(case_id)
        if outcome.get("label") not in LABELS:
            raise BenchmarkValidationError(
                f"{prefix}.label must be one of {sorted(LABELS)}"
            )
        cutoff = _parse_iso(
            case_by_id[case_id]["evidence_cutoff"],
            f"case[{case_id}].evidence_cutoff",
        )
        observed = _parse_iso(
            outcome.get("outcome_observed_at"),
            f"{prefix}.outcome_observed_at",
        )
        if observed <= cutoff:
            raise BenchmarkValidationError(
                f"{prefix}.outcome_observed_at must be after evidence_cutoff"
            )
        expected_window = (observed - cutoff).total_seconds()
        declared_window = _finite_float(
            outcome.get("observation_window_seconds"),
            f"{prefix}.observation_window_seconds",
            minimum=0,
        )
        if abs(float(declared_window) - expected_window) > 0.001:
            raise BenchmarkValidationError(
                f"{prefix}.observation_window_seconds is inconsistent"
            )
        if (
            outcome.get("label") == "benign"
            and expected_window < MIN_BENIGN_OBSERVATION_SECONDS
        ):
            raise BenchmarkValidationError(
                f"{prefix} benign outcomes require seven days of observation"
            )
        refs = outcome.get("outcome_evidence_refs")
        if (
            not isinstance(refs, list)
            or not refs
            or any(not str(item).strip() for item in refs)
        ):
            raise BenchmarkValidationError(
                f"{prefix}.outcome_evidence_refs must be a non-empty list"
            )
        if not str(outcome.get("reviewer") or "").strip():
            raise BenchmarkValidationError(f"{prefix}.reviewer is required")
        outcome_hash = str(outcome.get("outcome_hash") or "")
        if not HASH_RE.fullmatch(outcome_hash):
            raise BenchmarkValidationError(
                f"{prefix}.outcome_hash must be a SHA-256 hex digest"
            )
        if outcome_hash != _record_hash(outcome, "outcome_hash"):
            raise BenchmarkValidationError(
                f"{prefix}.outcome_hash does not match record content"
            )
    return outcomes


def append_outcome(
    path: str | Path,
    observations: Iterable[dict[str, Any]],
    outcome: dict[str, Any],
) -> dict[str, Any]:
    existing = load_jsonl(path)
    validate_outcomes([*existing, outcome], observations)
    _append_jsonl(path, outcome)
    return outcome


def _prediction_from_observation(
    observation: dict[str, Any],
) -> dict[str, Any]:
    fields = (
        "case_id",
        "analyzer",
        "analyzer_version",
        "analyzed_at",
        "evidence_cutoff",
        "risk_level",
        "action",
        "hard_stop_count",
        "legitimacy_score",
        "risk_probability",
        "infrastructure_indeterminate",
        "latency_ms",
        "evidence_age_seconds",
        "report_hash",
        "anchor_type",
        "anchor_value",
        "timechain_ring",
        "timechain_ring_hash",
    )
    return {
        field: observation[field]
        for field in fields
        if field in observation
    }


def materialize_case_bank(
    observations: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    checked_observations = validate_observations(observations)
    checked_outcomes = validate_outcomes(outcomes, checked_observations)
    observation_by_case: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for observation in checked_observations:
        observation_by_case[observation["case_id"]].append(observation)
    outcome_by_case = {
        outcome["case_id"]: outcome for outcome in checked_outcomes
    }
    cases: list[dict[str, Any]] = []
    predictions: list[dict[str, Any]] = []
    for case_id in sorted(outcome_by_case):
        outcome = outcome_by_case[case_id]
        source = observation_by_case[case_id][0]
        cases.append(
            {
                "case_id": case_id,
                "network": source["network"],
                "cohort": source["cohort"],
                "token_address": source["token_address"],
                "split": source["split"],
                "evidence_cutoff": source["evidence_cutoff"],
                "anchor_type": source.get("anchor_type"),
                "anchor_value": source.get("anchor_value"),
                "outcome_observed_at": outcome["outcome_observed_at"],
                "label": outcome["label"],
                "outcome_evidence_refs": outcome[
                    "outcome_evidence_refs"
                ],
                "reviewer": outcome["reviewer"],
                "observation_window_seconds": outcome[
                    "observation_window_seconds"
                ],
                "outcome_hash": outcome["outcome_hash"],
            }
        )
        predictions.extend(
            _prediction_from_observation(item)
            for item in sorted(
                observation_by_case[case_id],
                key=lambda value: (
                    value["analyzer"],
                    value["analyzer_version"],
                ),
            )
        )
    if not cases:
        raise BenchmarkValidationError(
            "no finalized outcomes are available to materialize"
        )
    validate_cases(cases)
    validate_predictions(predictions, cases)
    return cases, predictions


def case_bank_status(
    observations: Iterable[dict[str, Any]],
    outcomes: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    checked_observations = validate_observations(observations)
    checked_outcomes = validate_outcomes(outcomes, checked_observations)
    case_ids = {item["case_id"] for item in checked_observations}
    finalized_ids = {item["case_id"] for item in checked_outcomes}
    return {
        "casebank_schema_version": CASEBANK_SCHEMA_VERSION,
        "observations": len(checked_observations),
        "cases": len(case_ids),
        "pending_cases": len(case_ids - finalized_ids),
        "finalized_cases": len(finalized_ids),
        "analyzers": sorted(
            {
                f"{item['analyzer']}@{item['analyzer_version']}"
                for item in checked_observations
            }
        ),
        "networks": {
            network: len(
                {
                    item["case_id"]
                    for item in checked_observations
                    if item["network"] == network
                }
            )
            for network in sorted(NETWORKS)
        },
        "cohorts": {
            cohort: len(
                {
                    item["case_id"]
                    for item in checked_observations
                    if item["cohort"] == cohort
                }
            )
            for cohort in sorted(
                {item["cohort"] for item in checked_observations}
            )
        },
        "observation_ledger_hash": canonical_hash(
            sorted(
                checked_observations,
                key=lambda item: item["observation_hash"],
            )
        ),
        "outcome_ledger_hash": canonical_hash(
            sorted(
                checked_outcomes,
                key=lambda item: item["outcome_hash"],
            )
        ),
        "minimum_benign_observation_hours": (
            MIN_BENIGN_OBSERVATION_SECONDS // 3600
        ),
    }


def _rate(numerator: int, denominator: int) -> float | None:
    return round(numerator / denominator, 6) if denominator else None


def _wilson_interval(
    successes: int,
    total: int,
    z: float = 1.959963984540054,
) -> list[float] | None:
    if total <= 0:
        return None
    proportion = successes / total
    denominator = 1 + z * z / total
    centre = proportion + z * z / (2 * total)
    margin = z * math.sqrt(
        (proportion * (1 - proportion) + z * z / (4 * total)) / total
    )
    return [
        round(max(0.0, (centre - margin) / denominator), 6),
        round(min(1.0, (centre + margin) / denominator), 6),
    ]


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return round(ordered[0], 3)
    position = (len(ordered) - 1) * percentile
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 3)
    weight = position - lower
    return round(
        ordered[lower] * (1 - weight) + ordered[upper] * weight,
        3,
    )


def _expected_calibration_error(
    observations: list[tuple[float, int]],
    bins: int = 10,
) -> float | None:
    if not observations:
        return None
    total = len(observations)
    error = 0.0
    for bin_index in range(bins):
        lower = bin_index / bins
        upper = (bin_index + 1) / bins
        bucket = [
            item
            for item in observations
            if lower <= item[0] < upper
            or (bin_index == bins - 1 and item[0] == 1.0)
        ]
        if not bucket:
            continue
        confidence = sum(item[0] for item in bucket) / len(bucket)
        frequency = sum(item[1] for item in bucket) / len(bucket)
        error += len(bucket) / total * abs(confidence - frequency)
    return round(error, 6)


def _predicted_adverse(prediction: dict[str, Any]) -> bool:
    risk = str(prediction.get("risk_level") or "").strip().lower()
    action = str(prediction.get("action") or "").strip().lower()
    return (
        risk in POSITIVE_RISKS
        or action in POSITIVE_ACTIONS
        or int(prediction.get("hard_stop_count") or 0) > 0
    )


def _metric_block(
    cases: list[dict[str, Any]],
    predictions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    counts = {
        "cases": len(cases),
        "adverse": 0,
        "benign": 0,
        "infrastructure": 0,
        "missing_predictions": 0,
        "true_positive": 0,
        "true_negative": 0,
        "false_positive": 0,
        "dangerous_false_negative": 0,
        "adverse_abstention": 0,
        "benign_abstention": 0,
        "infrastructure_correct_abstention": 0,
        "infrastructure_wrong_certainty": 0,
    }
    latency: list[float] = []
    freshness: list[float] = []
    probability_observations: list[tuple[float, int]] = []

    for case in cases:
        label = case["label"]
        if label == "adverse_security":
            counts["adverse"] += 1
        elif label == "benign":
            counts["benign"] += 1
        else:
            counts["infrastructure"] += 1
        prediction = predictions.get(case["case_id"])
        if prediction is None:
            counts["missing_predictions"] += 1
            if label == "adverse_security":
                counts["adverse_abstention"] += 1
            elif label == "benign":
                counts["benign_abstention"] += 1
            continue

        latency.append(float(prediction["latency_ms"]))
        freshness.append(float(prediction["evidence_age_seconds"]))
        indeterminate = prediction["infrastructure_indeterminate"]
        if label == "infrastructure_failure":
            if indeterminate:
                counts["infrastructure_correct_abstention"] += 1
            else:
                counts["infrastructure_wrong_certainty"] += 1
            continue
        if indeterminate:
            counts[
                "adverse_abstention"
                if label == "adverse_security"
                else "benign_abstention"
            ] += 1
            continue

        predicted_adverse = _predicted_adverse(prediction)
        if label == "adverse_security":
            counts[
                "true_positive"
                if predicted_adverse
                else "dangerous_false_negative"
            ] += 1
        else:
            counts[
                "false_positive"
                if predicted_adverse
                else "true_negative"
            ] += 1
        if prediction.get("risk_probability") is not None:
            probability_observations.append(
                (
                    float(prediction["risk_probability"]),
                    1 if label == "adverse_security" else 0,
                )
            )

    tp = counts["true_positive"]
    tn = counts["true_negative"]
    fp = counts["false_positive"]
    fn = counts["dangerous_false_negative"]
    adverse_total = counts["adverse"]
    benign_total = counts["benign"]
    token_total = adverse_total + benign_total
    determinate_token = tp + tn + fp + fn
    infrastructure_total = counts["infrastructure"]
    brier = (
        round(
            sum(
                (probability - outcome) ** 2
                for probability, outcome in probability_observations
            )
            / len(probability_observations),
            6,
        )
        if probability_observations
        else None
    )

    return {
        "counts": counts,
        "rates": {
            "precision": _rate(tp, tp + fp),
            "recall": _rate(tp, tp + fn),
            "specificity": _rate(tn, tn + fp),
            "dangerous_false_negative_rate": _rate(fn, tp + fn),
            "false_positive_rate": _rate(fp, tn + fp),
            "token_evidence_coverage": _rate(determinate_token, token_total),
            "adverse_abstention_rate": _rate(
                counts["adverse_abstention"], adverse_total
            ),
            "benign_abstention_rate": _rate(
                counts["benign_abstention"], benign_total
            ),
            "effective_adverse_miss_rate": _rate(
                fn + counts["adverse_abstention"],
                adverse_total,
            ),
            "infrastructure_correct_abstention_rate": _rate(
                counts["infrastructure_correct_abstention"],
                infrastructure_total,
            ),
        },
        "confidence_intervals_95": {
            "recall": _wilson_interval(tp, tp + fn),
            "specificity": _wilson_interval(tn, tn + fp),
            "token_evidence_coverage": _wilson_interval(
                determinate_token, token_total
            ),
        },
        "probability_calibration": {
            "sample_size": len(probability_observations),
            "brier_score": brier,
            "expected_calibration_error_10_bin": (
                _expected_calibration_error(probability_observations)
            ),
            "caveat": (
                "Probability metrics are null unless the analyzer supplies an "
                "explicit calibrated risk_probability. Legitimacy scores are "
                "not treated as probabilities."
            ),
        },
        "latency_ms": {
            "sample_size": len(latency),
            "p50": _percentile(latency, 0.5),
            "p95": _percentile(latency, 0.95),
        },
        "evidence_age_seconds": {
            "sample_size": len(freshness),
            "p50": _percentile(freshness, 0.5),
            "p95": _percentile(freshness, 0.95),
        },
    }


def evaluate(
    cases: Iterable[dict[str, Any]],
    predictions: Iterable[dict[str, Any]],
    *,
    splits: Iterable[str] = ("validation", "test"),
) -> dict[str, Any]:
    checked_cases = validate_cases(cases)
    checked_predictions = validate_predictions(predictions, checked_cases)
    selected_splits = tuple(dict.fromkeys(splits))
    if not selected_splits or any(item not in SPLITS for item in selected_splits):
        raise BenchmarkValidationError(
            f"splits must be selected from {sorted(SPLITS)}"
        )
    selected_cases = [
        case for case in checked_cases if case["split"] in selected_splits
    ]
    if not selected_cases:
        raise BenchmarkValidationError(
            "no benchmark cases remain after split selection"
        )
    selected_case_ids = {case["case_id"] for case in selected_cases}
    banks: dict[tuple[str, str], dict[str, dict[str, Any]]] = defaultdict(dict)
    for prediction in checked_predictions:
        if prediction["case_id"] in selected_case_ids:
            banks[
                (prediction["analyzer"], prediction["analyzer_version"])
            ][prediction["case_id"]] = prediction
    if not banks:
        raise BenchmarkValidationError(
            "no predictions remain after split selection"
        )

    analyzer_reports = []
    for (analyzer, version), bank in sorted(banks.items()):
        network_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        cohort_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        split_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for case in selected_cases:
            network_groups[case["network"]].append(case)
            cohort_groups[case["cohort"]].append(case)
            split_groups[case["split"]].append(case)
        analyzer_reports.append(
            {
                "analyzer": analyzer,
                "analyzer_version": version,
                "prediction_count": len(bank),
                "overall": _metric_block(selected_cases, bank),
                "by_network": {
                    key: _metric_block(value, bank)
                    for key, value in sorted(network_groups.items())
                },
                "by_cohort": {
                    key: _metric_block(value, bank)
                    for key, value in sorted(cohort_groups.items())
                },
                "by_split": {
                    key: _metric_block(value, bank)
                    for key, value in sorted(split_groups.items())
                },
            }
        )

    covered_sets = [
        set(bank).intersection(selected_case_ids) for bank in banks.values()
    ]
    matched = set.intersection(*covered_sets) if covered_sets else set()
    generated_at = datetime.now().astimezone().isoformat()
    report = {
        "benchmark_schema_version": BENCHMARK_SCHEMA_VERSION,
        "generated_at": generated_at,
        "evaluated_splits": list(selected_splits),
        "case_bank_hash": canonical_hash(
            sorted(checked_cases, key=lambda item: item["case_id"])
        ),
        "prediction_bank_hash": canonical_hash(
            sorted(
                checked_predictions,
                key=lambda item: (
                    item["analyzer"],
                    item["analyzer_version"],
                    item["case_id"],
                ),
            )
        ),
        "selected_case_count": len(selected_cases),
        "matched_case_count": len(matched),
        "all_analyzers_cover_selected_cases": (
            len(matched) == len(selected_cases)
        ),
        "analyzers": analyzer_reports,
        "comparison_caveat": (
            "Analyzer rankings are valid only on matched cases with the same "
            "evidence cutoff. Vendor outputs must be captured at that cutoff; "
            "current re-scans cannot substitute for historical predictions."
        ),
    }
    report["seal_payload"] = {
        "event": "chainseer_benchmark",
        "benchmark_schema_version": BENCHMARK_SCHEMA_VERSION,
        "generated_at": generated_at,
        "case_bank_hash": report["case_bank_hash"],
        "prediction_bank_hash": report["prediction_bank_hash"],
        "selected_case_count": report["selected_case_count"],
        "matched_case_count": report["matched_case_count"],
        "analyzers": [
            {
                "analyzer": item["analyzer"],
                "analyzer_version": item["analyzer_version"],
                "overall": item["overall"],
            }
            for item in analyzer_reports
        ],
    }
    report["report_hash"] = canonical_hash(report)
    return report


def _write_report(path: str | Path, report: dict[str, Any]) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def _load_json_document(path: str | Path) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise BenchmarkValidationError(
            f"{path} is not valid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise BenchmarkValidationError(
            f"{path} must contain a JSON object"
        )
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Capture and evaluate time-separated Chainseer benchmark records"
        )
    )
    commands = parser.add_subparsers(dest="command", required=True)

    capture = commands.add_parser(
        "capture",
        help="append one immutable public-report prediction observation",
    )
    capture.add_argument("--report", required=True)
    capture.add_argument("--observations", required=True)
    capture.add_argument("--cohort", required=True)
    capture.add_argument("--split", choices=sorted(SPLITS), required=True)
    capture.add_argument("--analyzer", default="chainseer")
    capture.add_argument("--analyzer-version", required=True)
    capture.add_argument("--latency-ms", type=float)
    capture.add_argument("--evidence-age-seconds", type=float)
    capture.add_argument("--risk-probability", type=float)
    capture.add_argument("--case-id")
    capture.add_argument("--captured-at")

    label = commands.add_parser(
        "label",
        help="append a later independently reviewed outcome",
    )
    label.add_argument("--observations", required=True)
    label.add_argument("--outcomes", required=True)
    label.add_argument("--case-id", required=True)
    label.add_argument("--label", choices=sorted(LABELS), required=True)
    label.add_argument("--observed-at", required=True)
    label.add_argument(
        "--evidence-ref",
        action="append",
        required=True,
        dest="evidence_refs",
    )
    label.add_argument("--reviewer", required=True)
    label.add_argument("--notes")

    status_command = commands.add_parser(
        "status",
        help="validate and summarize the append-only case-bank ledgers",
    )
    status_command.add_argument("--observations", required=True)
    status_command.add_argument("--outcomes", required=True)

    materialize = commands.add_parser(
        "materialize",
        help="create immutable evaluator snapshots from finalized cases",
    )
    materialize.add_argument("--observations", required=True)
    materialize.add_argument("--outcomes", required=True)
    materialize.add_argument("--cases", required=True)
    materialize.add_argument("--predictions", required=True)

    validate_command = commands.add_parser("validate")
    validate_command.add_argument("--cases", required=True)
    validate_command.add_argument("--predictions", required=True)
    run = commands.add_parser("evaluate")
    run.add_argument("--cases", required=True)
    run.add_argument("--predictions", required=True)
    run.add_argument("--output", required=True)
    run.add_argument(
        "--splits",
        nargs="+",
        default=["validation", "test"],
        choices=sorted(SPLITS),
    )
    args = parser.parse_args()

    if args.command == "capture":
        observation = build_observation_from_report(
            _load_json_document(args.report),
            cohort=args.cohort,
            split=args.split,
            analyzer=args.analyzer,
            analyzer_version=args.analyzer_version,
            latency_ms=args.latency_ms,
            evidence_age_seconds=args.evidence_age_seconds,
            risk_probability=args.risk_probability,
            case_id=args.case_id,
            captured_at=args.captured_at,
        )
        append_observation(args.observations, observation)
        print(
            canonical_json(
                {
                    "captured": True,
                    "case_id": observation["case_id"],
                    "observation_hash": observation["observation_hash"],
                    "report_hash": observation["report_hash"],
                }
            )
        )
        return 0

    if args.command == "label":
        observations = validate_observations(
            load_jsonl(args.observations)
        )
        outcome = build_outcome_record(
            observations,
            case_id=args.case_id,
            label=args.label,
            outcome_observed_at=args.observed_at,
            evidence_refs=args.evidence_refs,
            reviewer=args.reviewer,
            notes=args.notes,
        )
        append_outcome(args.outcomes, observations, outcome)
        print(
            canonical_json(
                {
                    "labeled": True,
                    "case_id": outcome["case_id"],
                    "label": outcome["label"],
                    "outcome_hash": outcome["outcome_hash"],
                    "observation_window_seconds": outcome[
                        "observation_window_seconds"
                    ],
                }
            )
        )
        return 0

    if args.command == "status":
        print(
            canonical_json(
                case_bank_status(
                    load_jsonl(args.observations),
                    load_jsonl(args.outcomes),
                )
            )
        )
        return 0

    if args.command == "materialize":
        cases, predictions = materialize_case_bank(
            load_jsonl(args.observations),
            load_jsonl(args.outcomes),
        )
        if Path(args.cases).resolve() == Path(args.predictions).resolve():
            raise BenchmarkValidationError(
                "cases and predictions must use different paths"
            )
        _write_new_jsonl(args.cases, cases)
        _write_new_jsonl(args.predictions, predictions)
        print(
            canonical_json(
                {
                    "materialized": True,
                    "cases": len(cases),
                    "predictions": len(predictions),
                    "case_bank_hash": canonical_hash(cases),
                    "prediction_bank_hash": canonical_hash(predictions),
                }
            )
        )
        return 0

    cases = validate_cases(load_jsonl(args.cases))
    predictions = validate_predictions(load_jsonl(args.predictions), cases)
    if args.command == "validate":
        print(
            canonical_json(
                {
                    "valid": True,
                    "cases": len(cases),
                    "predictions": len(predictions),
                    "case_bank_hash": canonical_hash(
                        sorted(cases, key=lambda item: item["case_id"])
                    ),
                    "prediction_bank_hash": canonical_hash(
                        sorted(
                            predictions,
                            key=lambda item: (
                                item["analyzer"],
                                item["analyzer_version"],
                                item["case_id"],
                            ),
                        )
                    ),
                }
            )
        )
        return 0
    report = evaluate(cases, predictions, splits=args.splits)
    _write_report(args.output, report)
    print(
        canonical_json(
            {
                "report": str(Path(args.output).resolve()),
                "report_hash": report["report_hash"],
                "selected_cases": report["selected_case_count"],
                "matched_cases": report["matched_case_count"],
                "analyzers": len(report["analyzers"]),
            }
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
