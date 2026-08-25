"""Epoch/provenance estimator tests: stall separation, epoch reset,
stale queue expiry, three-way acceptance metrics."""
import json
import tempfile
import time
import unittest
from pathlib import Path

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import DecisionCommitmentStore

from tests.test_chainseer_robinhood_live_lane_reliability import (
    HEAD, POOL_ID, make_engine, seed_windows)


class StallSeparationTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = rh.RobinhoodLearningStore(self.root / "l.sqlite3")
        self.engine = make_engine(self.root, self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def test_stalls_counted_but_excluded_from_estimate(self):
        """A fixed-cost sample far above the window median is a STALL: it
        must raise the stall counter and NOT move the p95 estimate."""
        # Build a healthy baseline: 10 samples around 0.4s.
        for _ in range(10):
            self.engine._blend_seal_model(
                {"fixed_observation_cost_p95": 0.4})
        baseline = self.engine.seal_cost_model()[
            "fixed_observation_cost_p95"]
        self.assertLess(baseline, 1.0)
        stalls_before = rh.RobinhoodLearningStore.scheduler_state(
            self.store, rh.SEAL_COST_MODEL_STATE_KEY).get("stall_samples", 0)
        # A 60s selection pass (the observed outlier class).
        self.engine._blend_seal_model({"fixed_observation_cost_p95": 60.0})
        stored = rh.RobinhoodLearningStore.scheduler_state(
            self.store, rh.SEAL_COST_MODEL_STATE_KEY)
        self.assertEqual(
            stored.get("stall_samples"), stalls_before + 1,
            "the stall must be counted, not silently absorbed")
        after = self.engine.seal_cost_model()[
            "fixed_observation_cost_p95"]
        self.assertEqual(after, baseline,
                         "a stall must not move the cost estimate")
        # The raw record survives for research with status stalled.
        samples = stored.get("fixed_observation_samples") or []
        stalled_records = [s for s in samples
                           if s.get("status") == "stalled"]
        self.assertEqual(len(stalled_records), 1)
        self.assertEqual(stalled_records[0]["value"], 60.0)

    def test_epoch_reset_discards_poisoned_history(self):
        """Samples recorded under an older epoch never drive the current
        estimate; bumping SEAL_COST_MODEL_EPOCH starts clean."""
        self.engine._blend_seal_model({"per_window_cost_p95": [30.0] * 20})
        poisoned = self.engine.seal_cost_model()["per_window_cost_p95"]
        self.assertGreater(poisoned, 20.0)
        stored = self.store.scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY)
        stored["epoch"] = rh.SEAL_COST_MODEL_EPOCH - 1
        self.store.set_scheduler_state(
            rh.SEAL_COST_MODEL_STATE_KEY, stored)
        # Next recording resets to a fresh window; one healthy sample lands
        # on the cold-start default, not the poisoned p95.
        self.engine._blend_seal_model({"per_window_cost_p95": 0.5})
        fresh = self.engine.seal_cost_model()["per_window_cost_p95"]
        self.assertEqual(fresh, 0.5)


class StaleQueueExpiryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = rh.RobinhoodLearningStore(self.root / "l.sqlite3")

    def tearDown(self):
        self._tmp.cleanup()

    def _enqueue(self, pool_suffix: str, enqueued_at: float):
        with self.store.connection() as c:
            c.execute(
                """INSERT INTO flow_seal_queue (
                       pool_id, token_address, window_start_block,
                       window_end_block, enqueued_at, reason
                   ) VALUES (?, '0x' || '11', 100, 200, ?, 'live_lane_headroom')""",
                (POOL_ID[:-4] + pool_suffix, enqueued_at))

    def test_stale_entries_marked_expired_and_excluded(self):
        old = time.time() - rh.SEAL_QUEUE_STALE_SECONDS - 60
        self._enqueue("0001", old)
        self._enqueue("0002", time.time() - 10)
        expired = self.store.expire_stale_seal_queue()
        self.assertEqual(expired, 1)
        # Preserved for research, stamped completed.
        with self.store.connection() as c:
            row = dict(list(c.execute(
                "SELECT * FROM flow_seal_queue"
                " WHERE completed_at IS NOT NULL"))[0])
        self.assertIn("expired_stale", row["reason"])
        # Live admission no longer sees it.
        pending = self.store.pending_seal_windows(25)
        self.assertEqual([p["pool_id"][-4:] for p in pending], ["0002"])

    def test_expiry_runs_inside_seal_stage(self):
        """The seal stage expires stale entries before draining the queue."""
        old = time.time() - rh.SEAL_QUEUE_STALE_SECONDS - 60
        self._enqueue("0003", old)
        seed_windows(self.store, 1)
        engine = make_engine(self.root, self.store)
        result = engine.seal_near_head_observations(
            HEAD, time.time(), pool_ids=[POOL_ID],
            deadline=rh.CycleDeadline(20.0), limit=4, reserve_seconds=0.5)
        self.assertGreaterEqual(result.get("windows_expired_stale"), 1)


class ThreeWayMetricsTests(unittest.TestCase):
    def test_snapshot_reports_three_way_split(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            runs = [
                ("complete", {"duration_seconds": 8.0}),
                ("complete", {"duration_seconds": 9.0}),
                ("deferred", {"duration_seconds": 6.0}),
                ("deadline_exceeded", {"duration_seconds": 25.5,
                                       "failure_stage": "classification"}),
            ]
            for i, (status, summary) in enumerate(runs):
                store.begin_run(f"run-{i}", 25.0, lane="live")
                store.finish_run(f"run-{i}", status, summary=summary)
            snap = rh.live_lane_reliability_snapshot(root, store=store)
        self.assertEqual(snap["useful_completions"], 2)
        self.assertAlmostEqual(snap["useful_completion_rate"], 0.5)
        self.assertEqual(snap["controlled_deferrals"], 1)
        self.assertAlmostEqual(snap["controlled_deferral_rate"], 0.25)
        self.assertEqual(snap["uncontrolled_failures"], 1)
        self.assertAlmostEqual(snap["uncontrolled_failure_rate"], 0.25)


if __name__ == "__main__":
    unittest.main()
