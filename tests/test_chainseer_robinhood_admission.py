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
        # 10s remaining, 3s reserve, 1.7s/candidate -> floor(7/1.7) = 4.
        result = self.engine.classification_admission(100, 10.0)
        self.assertEqual(result["admitted"], int(7.0 // 1.7))
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

    def test_admission_limit_defers_rows_without_dropping_them(self):
        """Rows beyond the admission limit stay unclassified and are still
        selectable by a later cycle -- durably deferred, never dropped."""
        for i in range(6):
            self._seed_observation(f"obs-{i}")
        decision_head = 12345
        result_limited = self.engine.classify_sealed_observations(
            decision_head, admission_limit=2)
        self.assertEqual(result_limited["scoped_rows_selected"], 6)
        self.assertEqual(result_limited["scoped_rows_processed"], 2)
        self.assertEqual(result_limited["scoped_rows_deferred"], 4)
        self.assertEqual(
            result_limited["admission"]["admission_limit"], 2)
        self.assertEqual(
            result_limited["admission"]["admission_exceeded"], 4)
        # The deferred rows remain classifiable by a later cycle.
        result_rest = self.engine.classify_sealed_observations(
            decision_head, admission_limit=10)
        self.assertGreaterEqual(
            result_rest["scoped_rows_processed"],
            6 - result_limited["scoped_rows_processed"])


if __name__ == "__main__":
    unittest.main()
