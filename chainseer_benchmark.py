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
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable


BENCHMARK_SCHEMA_VERSION = "1.0"
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
    records: list[dict[str, Any]] = []
    for line_number, raw in enumerate(
        Path(path).read_text(encoding="utf-8").splitlines(),
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
        refs = case.get("outcome_evidence_refs")
        if not isinstance(refs, list) or not refs:
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
        analyzed_at = _parse_iso(
            prediction.get("analyzed_at"),
            f"{prefix}.analyzed_at",
        )
        cutoff = _parse_iso(
            prediction.get("evidence_cutoff"),
            f"{prefix}.evidence_cutoff",
        )
        case_cutoff = _parse_iso(
            case_by_id[case_id]["evidence_cutoff"],
            f"case[{case_id}].evidence_cutoff",
        )
        if cutoff != case_cutoff:
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
        if not isinstance(
            prediction.get("infrastructure_indeterminate"), bool
        ):
            raise BenchmarkValidationError(
                f"{prefix}.infrastructure_indeterminate must be boolean"
            )
        report_hash = prediction.get("report_hash")
        if report_hash is not None and not HASH_RE.fullmatch(str(report_hash)):
            raise BenchmarkValidationError(
                f"{prefix}.report_hash must be a SHA-256 hex digest"
            )
    return predictions


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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate time-separated Chainseer benchmark records"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    validate = commands.add_parser("validate")
    validate.add_argument("--cases", required=True)
    validate.add_argument("--predictions", required=True)
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
