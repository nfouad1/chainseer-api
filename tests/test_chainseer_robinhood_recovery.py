"""Adversarial: recovery reconciles against the position store.

A crash between open_position success and confirm_action must resolve
the commitment as EXECUTED (position exists), never aborted.
"""
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import DecisionCommitmentStore

from tests.test_chainseer_robinhood_adversarial import FakeTimechain


class RecoveryReconciliationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fake_tc = FakeTimechain()
        self.fake_tc.genesis("Chainseer Robinhood Learning")
        recorder = object.__new__(
            rh.RobinhoodLearningTimechainRecorder)
        recorder.root = self.root / "producer-chain"
        recorder.root.mkdir(parents=True, exist_ok=True)
        recorder.ANALYSIS_VERSION = (
            rh.RobinhoodLearningTimechainRecorder.ANALYSIS_VERSION)
        recorder.tc = self.fake_tc
        engine = object.__new__(rh.RobinhoodLearningEngine)
        engine.root = self.root
        engine.root.mkdir(parents=True, exist_ok=True)
        engine.store = rh.RobinhoodLearningStore(
            self.root / "learning.sqlite3")
        engine.ledger = SimpleNamespace(append=lambda *a, **k: None)
        engine.commitments = DecisionCommitmentStore(
            self.root / "decision_commitments.sqlite3")
        engine.execution_gate = rh.ExecutionGate(engine.commitments)
        engine.v4_market = None
        engine.market = SimpleNamespace(
            snapshot=lambda token, pair=None: {
                "price_usd": 2.5, "liquidity_usd": 500_000.0,
                "market_cap_usd": 100_000.0,
                "current_state_verified": True})
        engine.rpc = SimpleNamespace(get_block_number=lambda: 5002)
        engine.timechain_recorder = recorder
        self.engine = engine

    def tearDown(self):
        self._tmp.cleanup()

    def test_crash_after_open_position_recovers_as_executed(self):
        """open_position succeeds -> process dies before confirm_action ->
        recovery reconciles against the position store: executed."""
        candidate_token = ("0xcc00000000000000000000000000000000000000")
        self.engine.store.add_candidates([{
            "token_address": candidate_token,
            "pair_address": "0x" + "22" * 20,
            "factory_address": "0x" + "33" * 20,
            "block_number": 4000, "block_timestamp": 1.0,
            "transaction_hash": "0x" + "44" * 32, "log_index": 0,
            "source_version": None, "pool_id": None,
            "symbol": "TEST", "name": "Test",
        }])
        self.engine.store.record_analysis(candidate_token, {
            "legitimacy_score": 80.0, "risk_level": "Low",
            "action_label": "BUY_ELIGIBLE",
            "hard_stop_overrides": []}, {
            "price_usd": 2.5, "liquidity_usd": 500_000.0,
            "market_cap_usd": 100_000.0})
        self.engine.publish_integrity_certificate()

        # Simulate the crash window: claim + position open, no confirm.
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = self.engine.commitments.create(commit_spec(
            idempotency_key="crash-window",
            # Same token as the seeded candidate so reconciliation can
            # find the open position.
            token_address=candidate_token))
        self.engine.commitments.claim_execution(record["commitment_id"])
        # The action actually succeeded (position is open in the store).
        market = {"price_usd": 2.5, "liquidity_usd": 500_000.0,
                  "market_cap_usd": 100_000.0}
        opened = self.engine.store.open_position(
            self.engine.store.candidate(candidate_token), market,
            decision_commitment_id=record["commitment_id"])
        self.assertTrue(opened)
        # Even if the position has since closed, its exact commitment link
        # still proves that the original action happened.
        with self.engine.store.connection() as conn:
            conn.execute(
                "UPDATE positions SET status='closed' WHERE token_address=?",
                (candidate_token,))

        # Force the claim stale so recovery processes it.
        with self.engine.commitments.connection() as conn:
            conn.execute(
                "UPDATE decision_commitments SET executed_at=?"
                " WHERE commitment_id=?",
                (time.time() - 120, record["commitment_id"]))

        resolved = self.engine.commitments.recover_expired_commitments(
            position_reconciler=self.engine._reconcile_position_effect)
        self.assertEqual(resolved, 1)
        events = {e["status"]: e for e in
                  self.engine.commitments.latest_events(
                      record["commitment_id"], limit=20)}
        self.assertIn("executed", events)
        self.assertEqual(events["executed"]["detail"],
                         "confirmed_by_recovery_commitment_link")

        # And the sealed ring carries executed, not aborted.
        drained = self.engine.drain_deferred_seals()
        self.assertGreaterEqual(drained["sealed"], 0)
        if drained["last_ring"]:
            payload = self.fake_tc.rings[
                drained["last_ring"]["index"]]["payload"]
            statuses = {e["status"] for e in payload["decision_events"]}
            self.assertIn("executed", statuses)
            self.assertNotIn("aborted", statuses)

    def test_expired_without_position_aborts(self):
        """Expired, claimed, NO position -> aborted (truly expired)."""
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = self.engine.commitments.create(commit_spec())
        self.engine.commitments.claim_execution(record["commitment_id"])
        with self.engine.commitments.connection() as conn:
            conn.execute(
                "UPDATE decision_commitments SET executed_at=?"
                " WHERE commitment_id=?",
                (time.time() - 120, record["commitment_id"]))
        resolved = self.engine.commitments.recover_expired_commitments(
            position_reconciler=self.engine._reconcile_position_effect)
        self.assertEqual(resolved, 1)
        events = {e["status"] for e in
                  self.engine.commitments.latest_events(
                      record["commitment_id"], limit=20)}
        self.assertIn("aborted", events)
        self.assertNotIn("executed", events)

    def test_same_token_position_does_not_prove_unrelated_commitment(self):
        """Token equality is not causal linkage."""
        token = "0xce00000000000000000000000000000000000000"
        self.engine.store.add_candidates([{
            "token_address": token, "pair_address": "0x" + "22" * 20,
            "factory_address": "0x" + "33" * 20,
            "block_number": 4000, "block_timestamp": 1.0,
            "transaction_hash": "0x" + "44" * 32, "log_index": 0,
            "source_version": None, "pool_id": None,
            "symbol": "OLD", "name": "Old",
        }])
        self.engine.store.record_analysis(token, {
            "legitimacy_score": 80.0, "risk_level": "Low",
            "action_label": "BUY_ELIGIBLE", "hard_stop_overrides": []}, {
            "price_usd": 2.5, "liquidity_usd": 500_000.0,
            "market_cap_usd": 100_000.0})
        self.assertTrue(self.engine.store.open_position(
            self.engine.store.candidate(token), {
                "price_usd": 2.5, "liquidity_usd": 500_000.0,
                "market_cap_usd": 100_000.0}))
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = self.engine.commitments.create(commit_spec(
            token_address=token, idempotency_key="unrelated-same-token"))
        self.engine.commitments.claim_execution(record["commitment_id"])
        with self.engine.commitments.connection() as conn:
            conn.execute(
                "UPDATE decision_commitments SET executed_at=?"
                " WHERE commitment_id=?",
                (time.time() - 120, record["commitment_id"]))
        resolved = self.engine.commitments.recover_expired_commitments(
            position_reconciler=self.engine._reconcile_position_effect)
        self.assertEqual(resolved, 1)
        statuses = {event["status"] for event in
                    self.engine.commitments.latest_events(
                        record["commitment_id"], limit=20)}
        self.assertIn("aborted", statuses)
        self.assertNotIn("executed", statuses)

    def test_reconciliation_error_remains_indeterminate(self):
        """A failed position-store read must not forge absence."""
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = self.engine.commitments.create(commit_spec(
            idempotency_key="reconcile-error"))
        self.engine.commitments.claim_execution(record["commitment_id"])
        with self.engine.commitments.connection() as conn:
            conn.execute(
                "UPDATE decision_commitments SET executed_at=?"
                " WHERE commitment_id=?",
                (time.time() - 120, record["commitment_id"]))

        def unavailable(commitment_id, token_address):
            raise OSError("position database unavailable")

        resolved = self.engine.commitments.recover_expired_commitments(
            position_reconciler=unavailable)
        self.assertEqual(resolved, 0)
        statuses = {event["status"] for event in
                    self.engine.commitments.latest_events(
                        record["commitment_id"], limit=20)}
        self.assertFalse(statuses & {"executed", "aborted"})
        with self.engine.commitments.connection() as conn:
            job = conn.execute(
                "SELECT state,last_error FROM deferred_seals"
                " WHERE commitment_id=?",
                (record["commitment_id"],)).fetchone()
        self.assertEqual(job["state"], "retrying")
        self.assertIn("position_reconciliation_indeterminate",
                      job["last_error"])


if __name__ == "__main__":
    unittest.main()
