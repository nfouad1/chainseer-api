import tempfile
import unittest
from pathlib import Path

from chainseer_benchmark import (
    BenchmarkValidationError,
    append_observation,
    append_outcome,
    build_observation_from_report,
    build_outcome_record,
    case_bank_status,
    evaluate,
    load_jsonl,
    materialize_case_bank,
    validate_cases,
    validate_observations,
)


def case(
    case_id,
    label,
    *,
    network="robinhood",
    cohort="launch",
    split="validation",
):
    return {
        "case_id": case_id,
        "network": network,
        "cohort": cohort,
        "token_address": f"token-{case_id}",
        "split": split,
        "evidence_cutoff": "2026-01-01T00:00:00+00:00",
        "outcome_observed_at": "2026-02-01T00:00:00+00:00",
        "label": label,
        "outcome_evidence_refs": [f"evidence:{case_id}"],
    }


def prediction(
    case_id,
    *,
    analyzer="chainseer",
    version="1.0",
    risk="Low",
    action="WATCHLIST",
    indeterminate=False,
    hard_stops=0,
    probability=None,
):
    value = {
        "case_id": case_id,
        "analyzer": analyzer,
        "analyzer_version": version,
        "analyzed_at": "2026-01-01T00:00:01+00:00",
        "evidence_cutoff": "2026-01-01T00:00:00+00:00",
        "risk_level": risk,
        "action": action,
        "hard_stop_count": hard_stops,
        "legitimacy_score": 75,
        "infrastructure_indeterminate": indeterminate,
        "latency_ms": 100,
        "evidence_age_seconds": 2,
        "report_hash": "a" * 64,
    }
    if probability is not None:
        value["risk_probability"] = probability
    return value


def public_report():
    return {
        "schema_version": "1.1",
        "token": {
            "address": "0x" + "1" * 40,
            "chain": "Robinhood Chain",
            "chain_id": 4663,
        },
        "decision": {
            "risk_level": "Medium",
            "action": "REVIEW",
            "legitimacy_score": 71.5,
            "hard_stops": [],
        },
        "evidence": {
            "block_pin": 123,
            "anchor_type": "block_pin",
            "infrastructure_indeterminate": [],
            "facts": [
                {
                    "timestamp": "2026-01-01T00:00:00+00:00",
                    "response_hash": "1" * 64,
                }
            ],
        },
        "timechain": {
            "ring": 10,
            "ring_hash": "2" * 64,
        },
        "analyzed_at": "2026-01-01T00:00:01+00:00",
    }


class BenchmarkValidationTests(unittest.TestCase):
    def test_requires_outcome_after_evidence_cutoff(self):
        invalid = case("bad-time", "benign")
        invalid["outcome_observed_at"] = invalid["evidence_cutoff"]
        with self.assertRaises(BenchmarkValidationError):
            validate_cases([invalid])

    def test_rejects_duplicate_case_ids(self):
        with self.assertRaises(BenchmarkValidationError):
            validate_cases(
                [
                    case("duplicate", "benign"),
                    case("duplicate", "adverse_security"),
                ]
            )


class ProductionCaseBankTests(unittest.TestCase):
    def observation(self):
        return build_observation_from_report(
            public_report(),
            cohort="launch",
            split="validation",
            analyzer="chainseer",
            analyzer_version="commit-deadbeef",
            latency_ms=850,
            captured_at="2026-01-01T00:00:02+00:00",
        )

    def test_capture_is_hashed_and_duplicate_append_is_refused(self):
        observation = self.observation()
        self.assertEqual(observation["evidence_age_seconds"], 1.0)
        self.assertEqual(observation["network"], "robinhood")
        self.assertEqual(observation["anchor_type"], "block_pin")
        self.assertEqual(observation["anchor_value"], 123)
        self.assertEqual(len(observation["observation_hash"]), 64)

        tampered = dict(observation)
        tampered["action"] = "AVOID"
        with self.assertRaises(BenchmarkValidationError):
            validate_observations([tampered])

        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / "observations.jsonl"
            append_observation(ledger, observation)
            self.assertEqual(len(load_jsonl(ledger)), 1)
            with self.assertRaises(BenchmarkValidationError):
                append_observation(ledger, observation)

    def test_job_wrapper_derives_analysis_latency(self):
        observation = build_observation_from_report(
            {
                "started_at": "2026-01-01T00:00:00+00:00",
                "finished_at": "2026-01-01T00:00:03+00:00",
                "result": public_report(),
            },
            cohort="launch",
            split="test",
            analyzer="chainseer",
            analyzer_version="commit-deadbeef",
            captured_at="2026-01-01T00:00:04+00:00",
        )
        self.assertEqual(observation["latency_ms"], 3000.0)
        self.assertEqual(
            observation["analyzed_at"],
            "2026-01-01T00:00:03+00:00",
        )

    def test_benign_outcome_requires_seven_days_and_materializes(self):
        observation = self.observation()
        with self.assertRaises(BenchmarkValidationError):
            build_outcome_record(
                [observation],
                case_id=observation["case_id"],
                label="benign",
                outcome_observed_at="2026-01-02T00:00:01+00:00",
                evidence_refs=["review:test:too-early"],
                reviewer="reviewer-1",
            )
        outcome = build_outcome_record(
            [observation],
            case_id=observation["case_id"],
            label="benign",
            outcome_observed_at="2026-01-09T00:00:01+00:00",
            evidence_refs=["review:test:seven-day-window"],
            reviewer="reviewer-1",
        )
        cases, predictions = materialize_case_bank(
            [observation],
            [outcome],
        )
        self.assertEqual(len(cases), 1)
        self.assertEqual(len(predictions), 1)
        self.assertEqual(cases[0]["outcome_hash"], outcome["outcome_hash"])
        self.assertEqual(cases[0]["anchor_value"], 123)
        self.assertEqual(predictions[0]["timechain_ring"], 10)
        self.assertNotIn("captured_at", predictions[0])

    def test_status_keeps_pending_and_finalized_cases_separate(self):
        observation = self.observation()
        pending = case_bank_status([observation], [])
        self.assertEqual(pending["pending_cases"], 1)
        self.assertEqual(pending["finalized_cases"], 0)
        with self.assertRaises(BenchmarkValidationError):
            materialize_case_bank([observation], [])

        outcome = build_outcome_record(
            [observation],
            case_id=observation["case_id"],
            label="adverse_security",
            outcome_observed_at="2026-01-01T00:10:01+00:00",
            evidence_refs=["review:test:adverse-event"],
            reviewer="reviewer-1",
        )
        with tempfile.TemporaryDirectory() as directory:
            outcomes_path = Path(directory) / "outcomes.jsonl"
            append_outcome(outcomes_path, [observation], outcome)
            status = case_bank_status(
                [observation],
                load_jsonl(outcomes_path),
            )
        self.assertEqual(status["pending_cases"], 0)
        self.assertEqual(status["finalized_cases"], 1)


class BenchmarkMetricTests(unittest.TestCase):
    def setUp(self):
        self.cases = [
            case("adverse-hit", "adverse_security"),
            case("adverse-miss", "adverse_security", network="solana"),
            case("benign-clear", "benign"),
            case("benign-abstain", "benign", network="solana"),
            case("rpc-failure", "infrastructure_failure"),
        ]
        self.chainseer_predictions = [
            prediction(
                "adverse-hit",
                risk="High",
                action="AVOID",
                hard_stops=1,
                probability=0.9,
            ),
            prediction(
                "adverse-miss",
                risk="Low",
                action="REVIEW",
                probability=0.2,
            ),
            prediction(
                "benign-clear",
                risk="Low",
                action="WATCHLIST",
                probability=0.1,
            ),
            prediction("benign-abstain", indeterminate=True),
            prediction("rpc-failure", indeterminate=True),
        ]

    def test_separates_false_negatives_from_abstentions(self):
        report = evaluate(self.cases, self.chainseer_predictions)
        metrics = report["analyzers"][0]["overall"]
        self.assertEqual(metrics["counts"]["true_positive"], 1)
        self.assertEqual(metrics["counts"]["dangerous_false_negative"], 1)
        self.assertEqual(metrics["counts"]["benign_abstention"], 1)
        self.assertEqual(
            metrics["counts"]["infrastructure_correct_abstention"], 1
        )
        self.assertEqual(
            metrics["rates"]["dangerous_false_negative_rate"], 0.5
        )
        self.assertEqual(metrics["rates"]["token_evidence_coverage"], 0.75)
        self.assertEqual(
            metrics["probability_calibration"]["sample_size"], 3
        )
        self.assertIsNotNone(
            metrics["probability_calibration"]["brier_score"]
        )

    def test_legitimacy_score_is_not_treated_as_probability(self):
        predictions = [
            {
                key: value
                for key, value in item.items()
                if key != "risk_probability"
            }
            for item in self.chainseer_predictions
        ]
        report = evaluate(self.cases, predictions)
        calibration = report["analyzers"][0]["overall"][
            "probability_calibration"
        ]
        self.assertEqual(calibration["sample_size"], 0)
        self.assertIsNone(calibration["brier_score"])

    def test_comparison_reports_only_matched_cases(self):
        competitor = [
            prediction(
                item["case_id"],
                analyzer="competitor",
                version="2026-01",
                risk=item["risk_level"],
                action=item["action"],
                indeterminate=item["infrastructure_indeterminate"],
                hard_stops=item["hard_stop_count"],
            )
            for item in self.chainseer_predictions[:-1]
        ]
        report = evaluate(
            self.cases,
            self.chainseer_predictions + competitor,
        )
        self.assertEqual(len(report["analyzers"]), 2)
        self.assertEqual(report["selected_case_count"], 5)
        self.assertEqual(report["matched_case_count"], 4)
        self.assertFalse(report["all_analyzers_cover_selected_cases"])
        self.assertEqual(len(report["report_hash"]), 64)
        self.assertEqual(
            report["seal_payload"]["case_bank_hash"],
            report["case_bank_hash"],
        )

    def test_training_cases_are_excluded_by_default(self):
        cases = self.cases + [
            case("train-only", "adverse_security", split="train")
        ]
        predictions = self.chainseer_predictions + [
            prediction("train-only", risk="High", action="AVOID")
        ]
        report = evaluate(cases, predictions)
        self.assertEqual(report["selected_case_count"], 5)
        self.assertNotIn(
            "train",
            report["analyzers"][0]["by_split"],
        )


if __name__ == "__main__":
    unittest.main()
