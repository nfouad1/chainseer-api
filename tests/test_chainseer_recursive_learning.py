from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import chainseer_recursive_learning as recursive


class RecursiveLearningV1Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        connection = sqlite3.connect(self.root / "learning.sqlite3")
        connection.executescript("""
            CREATE TABLE flow_observations (
                observation_id TEXT PRIMARY KEY, pool_id TEXT,
                observed_at_epoch REAL, features_json TEXT);
            CREATE TABLE flow_observation_classifications (
                observation_id TEXT PRIMARY KEY, identity_tier TEXT,
                arm TEXT, gates_json TEXT, research_eligible INTEGER,
                paper_eligible INTEGER);
            CREATE TABLE flow_observation_outcomes (
                observation_id TEXT, horizon_label TEXT, status TEXT,
                net_return REAL, exit_valid INTEGER);
        """)
        for index in range(1_000):
            observation_id = f"obs-{index:04d}"
            features = {
                "uncapped_shadow_score": float(index % 100),
                "velocity_quality": (index % 10) / 10,
                "participant_quality": (index % 5) / 5,
                "pressure_quality": (index % 7) / 7,
                "identity_coverage": 1.0,
                "maximum_participant_share": 0.25,
                "adverse_price_direction": 0.0,
            }
            connection.execute(
                "INSERT INTO flow_observations VALUES (?,?,?,?)",
                (observation_id, f"pool-{index:04d}", 1_000.0 + index,
                 json.dumps(features)),
            )
            connection.execute(
                "INSERT INTO flow_observation_classifications VALUES "
                "(?,?,?,?,?,?)",
                (observation_id, "verified", "fresh", "[]", 1, 1),
            )
            status = "non_exitable" if index % 20 == 0 else "resolved"
            value = None if status == "non_exitable" else (
                0.2 if index % 3 else -0.1)
            connection.execute(
                "INSERT INTO flow_observation_outcomes VALUES (?,?,?,?,?)",
                (observation_id, "15m", status, value, 1),
            )
        connection.commit()
        connection.close()

    def tearDown(self):
        self.temporary.cleanup()

    def test_shadow_run_never_enables_or_changes_policy(self):
        report = recursive.run_shadow_learning(
            self.root, source_policy_version="operational-test",
            source_revision="revision-test", source_digest="source-test")
        governance = report["governance"]
        self.assertFalse(governance["promotion_enabled"])
        self.assertFalse(governance["promotion_eligible"])
        self.assertFalse(governance["active_policy_changed"])
        self.assertFalse(governance["single_trade_updates_allowed"])
        self.assertEqual(report["source"]["source_digest"], "source-test")
        self.assertEqual(len(report["source"]["learner_digest"]), 64)
        self.assertGreaterEqual(
            report["data_quality"]["train_rows"],
            recursive.MINIMUM_TRAIN_ROWS)
        self.assertGreaterEqual(
            report["data_quality"]["holdout_rows"],
            recursive.MINIMUM_HOLDOUT_ROWS)

    def test_evidence_digest_is_deduplicated_and_ledger_is_append_only(self):
        first = recursive.run_shadow_learning(
            self.root, source_policy_version="operational-test",
            source_revision="revision-test")
        second = recursive.run_shadow_learning(
            self.root, source_policy_version="operational-test",
            source_revision="revision-test")
        self.assertEqual(
            first["source"]["evidence_hash"],
            second["source"]["evidence_hash"])
        ledger = sqlite3.connect(
            self.root / "recursive_learning_v1.sqlite3")
        self.assertEqual(ledger.execute(
            "SELECT COUNT(*) FROM shadow_learning_runs").fetchone()[0], 1)
        with self.assertRaises(sqlite3.IntegrityError):
            ledger.execute(
                "UPDATE shadow_learning_runs SET status='tampered'")
        ledger.close()

    def test_latest_status_is_safe_before_first_run(self):
        empty = Path(self.temporary.name) / "empty"
        self.assertEqual(
            recursive.latest_shadow_learning(empty)["status"], "not_run")
        self.assertFalse(
            recursive.latest_shadow_learning(empty)["promotion_enabled"])


if __name__ == "__main__":
    unittest.main()
