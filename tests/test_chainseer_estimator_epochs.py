"""Epoch/provenance estimator tests: stall separation, epoch reset,
stale queue expiry, three-way acceptance metrics."""
import json
import os
import tempfile
import time
import unittest
from pathlib import Path

import chainseer_robinhood as rh
import run_chainseer_robinhood_learning as runner
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

    def test_reader_recomputes_counts_and_p95_from_provenance_records(self):
        self.engine._blend_seal_model({
            "per_window_cost_p95": [0.25, 0.5, 0.75]})
        model = self.engine.seal_cost_model()
        self.assertEqual(model["per_window_sample_count"], 3)
        self.assertEqual(model["per_window_cost_p95"], 0.75)
        self.assertEqual(model["epoch"], rh.SEAL_COST_MODEL_EPOCH)

    def test_censored_record_raises_effective_cost_without_becoming_success(self):
        self.engine._blend_seal_model({"per_window_cost_p95": [0.4] * 10})
        self.engine._blend_seal_model(
            {"per_window_cost_p95": 9.0}, censored=True)
        model = self.engine.seal_cost_model()
        self.assertEqual(model["per_window_sample_count"], 10)
        self.assertEqual(model["per_window_censored_count"], 1)
        self.assertEqual(model["per_window_cost_p95_successful_p95"], 0.4)
        self.assertGreaterEqual(model["per_window_cost_p95"], 9.0)

    def test_live_planning_quarantines_poisoned_censored_floor(self):
        for _ in range(10):
            self.engine._blend_seal_model({
                "fixed_observation_cost_p95": 5.797,
                "queue_settlement_p95": 0.141,
                "per_window_cost_p95": 0.812,
                "downstream_reserve_p95": 3.5,
            })
        self.engine._blend_seal_model(
            {"fixed_observation_cost_p95": 11_508.832}, censored=True)

        effective = self.engine.seal_cost_model()
        planning = self.engine.live_planning_seal_cost_model()
        self.assertGreater(effective["fixed_observation_cost_p95"], 11_000)
        self.assertAlmostEqual(
            planning["fixed_observation_cost_p95"], 5.797, places=3)
        self.assertTrue(planning["censored_guard_active"])
        self.assertIn(
            "fixed_observation_cost_p95",
            planning["quarantined_censored_components"],
        )
        self.assertLess(
            self.engine.ingestion_tail_reserve(),
            rh.LIVE_LANE_BUDGET_SECONDS,
        )

    def test_new_success_clears_censored_recovery_guard(self):
        self.engine._blend_seal_model({"per_window_cost_p95": 0.4})
        self.engine._blend_seal_model(
            {"per_window_cost_p95": 9.0}, censored=True)
        self.assertTrue(
            self.engine.live_planning_seal_cost_model()[
                "censored_guard_active"])
        time.sleep(0.002)
        self.engine._blend_seal_model({"per_window_cost_p95": 0.45})
        self.assertFalse(
            self.engine.live_planning_seal_cost_model()[
                "censored_guard_active"])

    def test_censored_guard_caps_admission_to_one_probe(self):
        for _ in range(10):
            self.engine._blend_seal_model({
                "fixed_observation_cost_p95": 0.4,
                "queue_settlement_p95": 0.1,
                "per_window_cost_p95": 0.4,
                "downstream_reserve_p95": 1.0,
            })
        self.engine._blend_seal_model(
            {"per_window_cost_p95": 9.0}, censored=True)
        seed_windows(self.store, 4)
        result = self.engine.seal_near_head_observations(
            HEAD, time.time(), pool_ids=[POOL_ID],
            deadline=rh.CycleDeadline(20.0), limit=4,
            reserve_seconds=0.5)
        admission = result["admission"]
        self.assertTrue(admission["censored_guard_active"])
        self.assertEqual(
            admission["censored_guard_action"],
            "cap_one_censored_recovery_probe",
        )
        self.assertEqual(result["windows_admitted"], 1)

    def test_cold_start_stall_cannot_become_the_epoch_baseline(self):
        self.engine._blend_seal_model({"fixed_observation_cost_p95": 60.0})
        model = self.engine.seal_cost_model()
        stored = self.store.scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY)
        self.assertEqual(model["fixed_sample_count"], 0)
        self.assertEqual(model["fixed_stall_count"], 1)
        self.assertEqual(
            stored["fixed_observation_samples"][0]["status"], "stalled")

    def test_queue_settlement_stall_is_preserved_but_cannot_starve_live(self):
        """A slow DB settlement is a stall, not a permanent tail reserve."""
        for _ in range(10):
            self.engine._blend_seal_model({"queue_settlement_p95": 0.2})
        baseline = self.engine.seal_cost_model()["queue_settlement_p95"]
        self.engine._blend_seal_model({"queue_settlement_p95": 12.7})
        model = self.engine.seal_cost_model()
        stored = self.store.scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY)

        self.assertEqual(model["queue_settlement_p95"], baseline)
        self.assertEqual(model["queue_settlement_stall_count"], 1)
        self.assertEqual(model["seal_stall_count"], 1)
        self.assertEqual(
            stored["queue_settlement_samples"][-1]["status"], "stalled")

    def test_queue_settlement_stalls_participate_in_tighten_only_guard(self):
        for _ in range(rh.SEAL_STALL_GUARD_MIN_SAMPLES):
            self.engine._blend_seal_model({"queue_settlement_p95": 0.2})
        self.engine._blend_seal_model({"queue_settlement_p95": 12.7})
        model = self.engine.seal_cost_model()
        self.assertTrue(model["stall_guard_active"])
        self.assertGreater(model["seal_stall_rate"], rh.SEAL_STALL_RATE_MAX)

    def test_excessive_stall_rate_activates_tighten_only_guard(self):
        for _ in range(rh.SEAL_STALL_GUARD_MIN_SAMPLES):
            self.engine._blend_seal_model(
                {"fixed_observation_cost_p95": 0.4})
        self.engine._blend_seal_model({"fixed_observation_cost_p95": 60.0})
        model = self.engine.seal_cost_model()
        self.assertTrue(model["stall_guard_active"])
        self.assertGreater(model["fixed_stall_rate"], rh.SEAL_STALL_RATE_MAX)

    def test_active_stall_guard_caps_admission_to_one_probe(self):
        for _ in range(rh.SEAL_STALL_GUARD_MIN_SAMPLES):
            self.engine._blend_seal_model(
                {"fixed_observation_cost_p95": 0.4})
        self.engine._blend_seal_model({"fixed_observation_cost_p95": 60.0})
        seed_windows(self.store, 4)
        result = self.engine.seal_near_head_observations(
            HEAD, time.time(), pool_ids=[POOL_ID],
            deadline=rh.CycleDeadline(20.0), limit=4,
            reserve_seconds=0.5)
        admission = result["admission"]
        self.assertTrue(admission["stall_guard_active"])
        self.assertEqual(admission["stall_guard_action"],
                         "cap_one_recovery_probe")
        self.assertEqual(result["windows_admitted"], 1)

    def test_supervisor_censor_writer_uses_same_record_schema(self):
        self.engine._blend_seal_model({"per_window_cost_p95": [0.4] * 10})
        run_id = "epoch-two-supervisor-kill"
        fake_pid = 987654
        self.store.begin_run(run_id, 25.0, lane="live")
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE runs SET pid=? WHERE run_id=?", (fake_pid, run_id))
            connection.execute(
                "UPDATE lane_state SET pid=? WHERE lane='live'",
                (fake_pid,))
        self.store.mark_lane_stage(
            "live", "fresh_quote_and_observation/window_quote_rpc",
            run_id=run_id, remaining=0.5)
        time.sleep(0.01)
        self.store.terminate_lane(
            "live", fake_pid, "supervisor_hard_deadline_exceeded")
        state = self.store.scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY)
        records = state["per_window_samples"]
        self.assertTrue(all(isinstance(record, dict) for record in records))
        self.assertEqual(records[-1]["status"], "censored")
        self.assertEqual(records[-1]["run_id"], run_id)
        self.assertGreaterEqual(
            self.engine.seal_cost_model()["per_window_cost_p95"], 0.4)

    def test_begin_run_clears_stale_stage_ownership(self):
        self.store.begin_run("old", 120.0, lane="backfill")
        self.store.mark_lane_stage(
            "backfill", "fresh_quote_and_observation/observation_selection",
            run_id="old", remaining=100.0,
            completed={"historical": 10.0}, detail={"old": True})
        self.store.finish_run("old", "complete")

        self.store.begin_run("new", 120.0, lane="backfill")
        with self.store.connection() as connection:
            row = connection.execute(
                "SELECT current_stage,stage_started_at,"
                " deadline_remaining_at_stage_start,"
                " completed_stage_seconds_json,stage_detail_json"
                " FROM lane_state WHERE lane='backfill'"
            ).fetchone()
        self.assertIsNone(row["current_stage"])
        self.assertIsNone(row["stage_started_at"])
        self.assertIsNone(row["deadline_remaining_at_stage_start"])
        self.assertEqual(json.loads(row["completed_stage_seconds_json"]), {})
        self.assertEqual(json.loads(row["stage_detail_json"]), {})

    def test_backfill_termination_cannot_contaminate_live_seal_model(self):
        run_id = "backfill-no-live-estimator-write"
        fake_pid = 876543
        self.store.begin_run(run_id, 120.0, lane="backfill")
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE runs SET pid=? WHERE run_id=?", (fake_pid, run_id))
            connection.execute(
                "UPDATE lane_state SET pid=? WHERE lane='backfill'",
                (fake_pid,))
        self.store.mark_lane_stage(
            "backfill", "fresh_quote_and_observation/observation_selection",
            run_id=run_id, remaining=1.0)
        self.store.terminate_lane("backfill", fake_pid, "deadline")
        self.assertEqual(
            self.store.scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY), {})

    def test_live_termination_clamps_impossible_censored_duration(self):
        run_id = "bounded-live-censor"
        fake_pid = 765432
        self.store.begin_run(run_id, 25.0, lane="live")
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE runs SET pid=? WHERE run_id=?", (fake_pid, run_id))
            connection.execute(
                "UPDATE lane_state SET pid=?,current_stage=?,"
                " stage_started_at=? WHERE lane='live'",
                (fake_pid,
                 "fresh_quote_and_observation/observation_selection",
                 time.time() - 11_508.832))
        self.store.terminate_lane("live", fake_pid, "deadline")
        state = self.store.scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY)
        expected_cap = 25.0 + rh.LANE_TERMINATION_GRACE_SECONDS
        self.assertEqual(state["last_censored_seconds"], expected_cap)
        self.assertGreater(state["last_censored_raw_seconds"], 11_000)
        self.assertTrue(state["last_censored_quarantined"])
        self.assertLessEqual(
            state["fixed_observation_samples"][-1]["value"], expected_cap)


class StaleQueueExpiryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = rh.RobinhoodLearningStore(self.root / "l.sqlite3")

    def tearDown(self):
        self._tmp.cleanup()

    def _enqueue(self, pool_suffix: str, enqueued_at: float,
                 window_end_block: int = 200):
        with self.store.connection() as c:
            c.execute(
                """INSERT INTO flow_seal_queue (
                       pool_id, token_address, window_start_block,
                       window_end_block, enqueued_at, reason
                   ) VALUES (?, '0x' || '11', 100, 200, ?, 'live_lane_headroom')""",
                (POOL_ID[:-4] + pool_suffix, enqueued_at))
            c.execute(
                "UPDATE flow_seal_queue SET window_end_block=? WHERE pool_id=?",
                (int(window_end_block), POOL_ID[:-4] + pool_suffix))

    def test_stale_entries_marked_expired_and_excluded(self):
        same_time = time.time() - 10
        self._enqueue("0001", same_time, HEAD - 121)
        self._enqueue("0002", same_time, HEAD - 120)
        expired = self.store.expire_stale_seal_queue(head_block=HEAD)
        self.assertEqual(expired, 1)
        # Preserved for audit in an explicit non-live state.
        with self.store.connection() as c:
            row = dict(list(c.execute(
                "SELECT * FROM flow_seal_queue"
                " WHERE queue_state='expired_stale'"))[0])
        self.assertIn("expired_stale", row["reason"])
        self.assertIsNone(row["completed_at"])
        # Live admission no longer sees it.
        pending = self.store.pending_seal_windows(25)
        self.assertEqual([p["pool_id"][-4:] for p in pending], ["0002"])
        retired = self.store.retire_expired_seal_queue(25)
        self.assertEqual(retired, 1)
        with self.store.connection() as c:
            terminal = dict(c.execute(
                "SELECT * FROM flow_seal_queue"
                " WHERE queue_state='expired_unsealed'").fetchone())
        self.assertEqual(terminal["pool_id"][-4:], "0001")
        self.assertIsNotNone(terminal["completed_at"])
        self.assertIn("retired_without_observation", terminal["reason"])

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
                ("complete", {
                    "duration_seconds": 8.0,
                    "observation_seal": {"sealed_this_cycle": 1},
                    "classification": {
                        "scoped_rows_selected": 1,
                        "scoped_rows_processed": 1,
                    },
                }),
                ("complete", {"duration_seconds": 9.0}),
                ("deferred", {"duration_seconds": 6.0}),
                ("deadline_exceeded", {"duration_seconds": 25.5,
                                       "failure_stage": "classification"}),
            ]
            for i, (status, summary) in enumerate(runs):
                store.begin_run(f"run-{i}", 25.0, lane="live")
                store.finish_run(f"run-{i}", status, summary=summary)
            snap = rh.live_lane_reliability_snapshot(root, store=store)
        self.assertEqual(snap["useful_completions"], 1)
        self.assertEqual(snap["decision_opportunities"], 2)
        self.assertAlmostEqual(snap["useful_completion_rate"], 0.5)
        self.assertAlmostEqual(snap["productive_cycle_rate"], 0.25)
        self.assertEqual(snap["idle_completions"], 1)
        self.assertEqual(snap["controlled_deferrals"], 1)
        self.assertAlmostEqual(snap["controlled_deferral_rate"], 0.25)
        self.assertEqual(snap["uncontrolled_failures"], 1)
        self.assertAlmostEqual(snap["uncontrolled_failure_rate"], 0.25)

    def test_running_attempt_is_not_a_terminal_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            store.begin_run("done", 25.0, lane="live")
            store.finish_run("done", "complete", summary={
                "duration_seconds": 1.0,
                "observation_seal": {"sealed_this_cycle": 1},
                "classification": {
                    "scoped_rows_selected": 1,
                    "scoped_rows_processed": 1,
                },
            })
            store.begin_run("still-running", 25.0, lane="live")
            snap = rh.live_lane_reliability_snapshot(root, store=store)
        self.assertEqual(snap["window_runs"], 1)
        self.assertEqual(snap["uncontrolled_failures"], 0)
        self.assertEqual(snap["useful_completion_rate"], 1.0)


class RevisionProvenanceTests(unittest.TestCase):
    def test_runner_reads_branch_revision_without_spawning_git(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            git_dir = root / ".git"
            (git_dir / "refs" / "heads").mkdir(parents=True)
            (git_dir / "HEAD").write_text(
                "ref: refs/heads/main\n", encoding="ascii")
            (git_dir / "refs" / "heads" / "main").write_text(
                "0123456789abcdef0123456789abcdef01234567\n",
                encoding="ascii")
            self.assertEqual(runner._workspace_revision(root), "0123456789ab")


class DashboardContractTests(unittest.TestCase):
    def test_new_readiness_criteria_have_explicit_renderers(self):
        html = Path("robinhood_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("key==='decision_usefulness'", html)
        self.assertIn("key==='seal_stall_guard'", html)


if __name__ == "__main__":
    unittest.main()
