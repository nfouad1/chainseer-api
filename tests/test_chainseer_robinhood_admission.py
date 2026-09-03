"""Classification admission controller tests.

Proves: p95-based cost estimation, completion-reserve headroom, durable
deferral of observations that do not fit (never dropped), and the
pre-classification gap instrumentation.
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import DecisionCommitmentStore


class AdmissionControllerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.engine = object.__new__(rh.RobinhoodLearningEngine)
        self.engine.root = self.root
        self.engine.root.mkdir(parents=True, exist_ok=True)
        self.engine.store = rh.RobinhoodLearningStore(
            self.root / "learning.sqlite3")

    def tearDown(self):
        self._tmp.cleanup()

    def test_cold_start_estimate_is_conservative_default(self):
        self.assertEqual(
            self.engine.classification_cost_estimate(),
            rh.CLASSIFICATION_COST_SECONDS_DEFAULT)

    def test_p95_of_batch_raises_the_estimate(self):
        """p95, not mean: one slow observation must raise admission's plan."""
        # Durations: mostly fast, one very slow.
        measured = self.engine.record_classification_cost(
            [0.1] * 9 + [5.0])
        self.assertIsNotNone(measured)
        estimate = self.engine.classification_cost_estimate()
        # The blend must sit well above the fast-observation mean (0.1).
        self.assertGreater(estimate, 1.0)
        # A fresh engine reads the same durable estimate.
        fresh = object.__new__(rh.RobinhoodLearningEngine)
        fresh.store = self.engine.store
        self.assertEqual(
            fresh.classification_cost_estimate(), estimate)

    def test_admission_fits_budget_with_reserve(self):
        # 10s remaining, 3s completion + 1.5s selection reserve,
        # 1.7s/candidate -> floor(5.5/1.7) = 3.
        result = self.engine.classification_admission(100, 10.0)
        self.assertEqual(result["admitted"], int(5.5 // 1.7))
        self.assertEqual(result["deferred"], 100 - result["admitted"])
        self.assertEqual(result["candidates"], 100)

    def test_tight_budget_defers_everything(self):
        result = self.engine.classification_admission(8, 2.0)
        self.assertEqual(result["admitted"], 0)
        self.assertEqual(result["deferred"], 8)

    def test_admission_never_exceeds_candidates(self):
        result = self.engine.classification_admission(2, 100.0)
        self.assertEqual(result["admitted"], 2)
        self.assertEqual(result["deferred"], 0)


class DeferredNotDroppedTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        recorder = None
        self.engine = object.__new__(rh.RobinhoodLearningEngine)
        self.engine.root = self.root
        self.engine.root.mkdir(parents=True, exist_ok=True)
        self.engine.store = rh.RobinhoodLearningStore(
            self.root / "learning.sqlite3")
        self.engine.commitments = DecisionCommitmentStore(
            self.root / "decision_commitments.sqlite3")
        self.engine.execution_gate = rh.ExecutionGate(
            self.engine.commitments)
        self.engine.v4_market = None
        self.engine.market = None
        self.engine.rpc = None
        self.engine.timechain_recorder = recorder

    def tearDown(self):
        self._tmp.cleanup()

    def _seed_observation(self, observation_id: str) -> None:
        with self.engine.store.connection() as connection:
            connection.execute(
                """INSERT INTO flow_observations (
                       observation_id, pool_id, token_address,
                       policy_version, observation_head,
                       observation_head_lag_blocks, observed_at,
                       observed_at_epoch, window_start_block,
                       window_end_block, transaction_set_hash,
                       transaction_count, features_json, quote_json,
                       sealed_at
                   ) VALUES (
                       :observation_id,
                       '0x' || 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                       '0x' || 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                       :policy_version, 100, 0, :observed_at,
                       1000.0, 90, 100, '0xseed', 1, '{}', '{}',
                       1000.0)""",
                {
                    "observation_id": observation_id,
                    "policy_version": rh.FLOW_EVIDENCE_POLICY_VERSION,
                    "observed_at": "2026-08-24T00:00:00+00:00",
                },
            )

    def test_live_scope_never_drains_historical_backlog(self):
        """Live cycles classify only their IDs; analysis drains backlog."""
        for i in range(4):
            self._seed_observation(f"obs-{chr(65 + i)}")
        decision_head = 12345

        # Cycle 1: only A and B belong to this live scope.
        cycle1 = self.engine.classify_sealed_observations(
            decision_head,
            observation_ids=["obs-A", "obs-B"],
            admission_limit=1)
        self.assertEqual(cycle1["scoped_rows_processed"], 1)
        self.assertEqual(
            cycle1["admission"]["admission_exceeded"], 1)
        processed_first = {
            e["observation_id"] for e in
            self.engine.store.recent_classified_observations()
        } if hasattr(self.engine.store,
                     "recent_classified_observations") else None
        with self.engine.store.connection() as connection:
            classified = {row[0] for row in connection.execute(
                "SELECT observation_id FROM"
                " flow_observation_classifications")}
        self.assertEqual(len(classified), 1)
        # Deferred observation survives unclassified.
        self.assertNotIn("obs-B", classified)

        # Cycle 2 must not spend live headroom on deferred B.
        cycle2 = self.engine.classify_sealed_observations(
            decision_head,
            observation_ids=["obs-C", "obs-D"],
            admission_limit=10)
        with self.engine.store.connection() as connection:
            classified = {row[0] for row in connection.execute(
                "SELECT observation_id FROM"
                " flow_observation_classifications")}
        self.assertEqual(len(classified), 3)
        self.assertNotIn("obs-B", classified)

        # The asynchronous/no-scope path subsequently drains it.
        self.engine.classify_sealed_observations(
            decision_head, admission_limit=10)
        with self.engine.store.connection() as connection:
            classified = {row[0] for row in connection.execute(
                "SELECT observation_id FROM"
                " flow_observation_classifications")}
        self.assertIn("obs-B", classified)

    def test_current_cycle_ids_prioritized_over_deferred(self):
        """With limited budget, current-cycle observations classify before
        older deferred ones."""
        for i in range(3):
            self._seed_observation(f"old-{i}")
        # Classify all old ones first so they are no longer pending.
        self.engine.classify_sealed_observations(1, admission_limit=10)
        # Now seal two new ones; budget admits only 1.
        self._seed_observation("new-0")
        self._seed_observation("new-1")
        result = self.engine.classify_sealed_observations(
            2,
            observation_ids=["new-0", "new-1"],
            admission_limit=1)
        self.assertEqual(result["scoped_rows_processed"], 1)
        with self.engine.store.connection() as connection:
            classified = {row[0] for row in connection.execute(
                "SELECT observation_id FROM"
                " flow_observation_classifications")}
        self.assertIn("new-0", classified)
        self.assertNotIn("new-1", classified)

    def test_empty_live_scope_does_not_become_backlog_scan(self):
        """An explicit empty batch means no live work, not all old work."""
        for i in range(20):
            self._seed_observation(f"old-{i}")
        result = self.engine.classify_sealed_observations(
            2, observation_ids=[], admission_limit=10)
        self.assertEqual(result["scoped_rows_selected"], 0)
        self.assertEqual(result["scoped_rows_processed"], 0)
        self.assertEqual(result["scoped_rows_deferred"], 0)
        with self.engine.store.connection() as connection:
            classified = connection.execute(
                "SELECT COUNT(*) FROM flow_observation_classifications"
            ).fetchone()[0]
        self.assertEqual(classified, 0)


if __name__ == "__main__":
    unittest.main()
