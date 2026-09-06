import tempfile
import unittest
import sqlite3
import os
import time
from contextlib import contextmanager
from pathlib import Path
from unittest import mock
from tests.test_chainseer_robinhood import FakeRPC, FakeAnalyzer, FakeMarket
import chainseer_robinhood as rh
import run_chainseer_robinhood_learning as runner


class BackfillAdaptationTests(unittest.TestCase):
    def test_resident_backfill_window_is_bounded_and_schedulable(self):
        self.assertEqual(rh.BACKFILL_LANE_BUDGET_SECONDS, 240.0)
        self.assertGreaterEqual(
            runner.DEFAULT_SUPERVISOR_DURATION_SECONDS,
            4 * rh.BACKFILL_LANE_BUDGET_SECONDS)
        self.assertLess(
            runner.DEFAULT_SUPERVISOR_DURATION_SECONDS, 20 * 60.0)
        self.assertTrue(rh._lane_launch_fits(
            runner.DEFAULT_SUPERVISOR_DURATION_SECONDS - 6.0,
            rh.BACKFILL_LANE_BUDGET_SECONDS))
        policy = rh.operational_acceptance_policy(100)
        self.assertEqual(
            policy["backfill_lane_budget_seconds"],
            rh.BACKFILL_LANE_BUDGET_SECONDS)

    def exercise(self, windows=None):
        with tempfile.TemporaryDirectory() as root:
            engine = rh.RobinhoodLearningEngine(root, rpc=FakeRPC([], latest=100),
                analyzer=FakeAnalyzer(), market=FakeMarket())
            engine.store.set_scheduler_state(rh.BACKFILL_RPC_CHUNK_STATE_KEY, {
                "epoch": rh.BACKFILL_RPC_CHUNK_MODEL_EPOCH, "revision": rh.CODE_REVISION,
                "stable_chunk_blocks": 125, "next_chunk_blocks": 125, "success_streak": 4})
            calls = []
            if windows:
                sequence = iter(windows)
                engine.backfill_live_priority_window = lambda: {
                    "required": True, "admitted": True, "available_seconds": next(sequence)}
            def drain(deadline, *, block_limit):
                calls.append(block_limit)
                if len(calls) == 4:
                    return {"ranges_selected": 0}
                return {"ranges_selected": 1, "blocks_scanned": block_limit,
                        "from_block": 1, "to_block": block_limit}
            engine.drain_flow_backfill = drain
            result = engine.drain_flow_backfill_until_reserve(rh.CycleDeadline(10),
                        block_limit=1000, reserve_seconds=0.1)
            state = engine.backfill_rpc_chunk_plan(1000)
            return calls, result, state

    def test_probe_is_consumed_inside_cycle(self):
        calls, result, state = self.exercise()
        self.assertEqual(calls, [125, 250, 250, 250])
        self.assertEqual(result["blocks_scanned"], 625)
        self.assertEqual(state["stable_chunk_blocks"], 250)
        self.assertLessEqual(max(calls), rh.BACKFILL_BATCHED_LOGICAL_CHUNK_BLOCKS)

    def test_short_window_preserves_probe_until_safe_window(self):
        calls, result, state = self.exercise([20, 6, 20, 20])
        self.assertEqual(calls, [125, 125, 250, 250])
        self.assertEqual(state["stable_chunk_blocks"], 250)
        self.assertEqual(result["blocks_scanned"], 500)

    def test_all_short_windows_leave_probe_pending_for_next_worker(self):
        calls, result, state = self.exercise([6, 6, 6, 6])
        self.assertEqual(calls, [125]*4)
        self.assertEqual(state["chunk_blocks"], 250)
        self.assertTrue(state["probe_pending"])

    def test_cursor_commit_retries_transient_writer_lock(self):
        with tempfile.TemporaryDirectory() as root:
            store = rh.RobinhoodLearningStore(
                Path(root) / "learning.sqlite3")
            attempts = []

            class Result:
                def fetchall(self):
                    return [{"from_block": 1, "to_block": 10,
                             "cursor": 1}]

            class Connection:
                def set_progress_handler(self, *_args):
                    return None

                def execute(self, sql, _params=()):
                    return Result() if "SELECT" in sql else None

            @contextmanager
            def connection(**_kwargs):
                attempts.append(1)
                if len(attempts) == 1:
                    raise sqlite3.OperationalError("database is locked")
                yield Connection()

            store.connection = connection
            result = store.commit_backfill_scan_span(
                1, 10, deadline=rh.CycleDeadline(5.0))
            self.assertEqual(result["lock_retries"], 1)
            self.assertEqual(result["completed"], 1)
            self.assertEqual(len(attempts), 2)

    def test_prefetch_gate_runs_after_rpc_and_before_primary_apply(self):
        with tempfile.TemporaryDirectory() as root:
            order = []

            class RPC(FakeRPC):
                def get_logs(self, *args, **kwargs):
                    order.append("fetch")
                    return super().get_logs(*args, **kwargs)

            store = rh.RobinhoodLearningStore(
                Path(root) / "learning.sqlite3")
            store.known_v4_pool_ids = lambda: set()
            store.pending_v4_activations = lambda: []
            store.apply_v4_events = (
                lambda *_args, **_kwargs: order.append("apply"))

            def gate(_deadline):
                order.append("gate")
                return rh.CycleDeadline(2.0), {
                    "waited": True,
                    "wait_seconds": 0.1,
                    "window": {"required": True, "admitted": True},
                }

            observer = rh.RobinhoodV4Observer(
                RPC([], latest=100), store, Path(root) / "cursor.json",
                historical_only=True)
            _, coverage = observer.sync(
                block_limit=5, lookback=5, activation_limit=0,
                deadline=rh.CycleDeadline(5.0), before_apply=gate)

            self.assertEqual(order, ["fetch", "gate", "apply"])
            self.assertTrue(coverage["commit_gate"]["enabled"])
            self.assertIn("log_rpc", coverage["stage_timings_seconds"])
            self.assertIsNotNone(observer.last_persist_deadline)

    def test_zero_activation_limit_uses_count_without_loading_rows(self):
        with tempfile.TemporaryDirectory() as root:
            store = rh.RobinhoodLearningStore(
                Path(root) / "learning.sqlite3")
            store.known_v4_pool_ids = lambda: set()
            store.pending_v4_activation_count = lambda: 17
            store.pending_v4_activations = mock.Mock(
                side_effect=AssertionError("full activation book loaded"))
            store.apply_v4_events = lambda *_args, **_kwargs: None

            observer = rh.RobinhoodV4Observer(
                FakeRPC([], latest=100), store,
                Path(root) / "cursor.json", historical_only=True)
            candidates, coverage = observer.sync(
                block_limit=5, lookback=5, activation_limit=0,
                deadline=rh.CycleDeadline(5.0))

            self.assertEqual(candidates, [])
            self.assertEqual(coverage["activations_available"], 17)
            self.assertEqual(coverage["activations_deferred"], 17)
            store.pending_v4_activations.assert_not_called()

    def test_prefetch_cost_projection_excludes_internal_wait(self):
        with tempfile.TemporaryDirectory() as root:
            engine = rh.RobinhoodLearningEngine(
                root, rpc=FakeRPC([], latest=100),
                analyzer=FakeAnalyzer(), market=FakeMarket())
            engine.backfill_rpc_isolated = True
            engine.backfill_live_priority_window = lambda: {
                "required": True, "admitted": False,
                "available_seconds": 0.0, "reason": "live_lane_active",
            }
            calls = []

            def drain(_deadline, *, block_limit, **kwargs):
                calls.append((block_limit, kwargs))
                if len(calls) == 2:
                    return {"ranges_selected": 0}
                time.sleep(0.03)
                return {
                    "ranges_selected": 1, "blocks_scanned": block_limit,
                    "from_block": 1, "to_block": block_limit,
                    "prefetch_before_commit": True,
                    "stage_timings_seconds": {"commit_window_wait": 0.02},
                }

            engine.drain_flow_backfill = drain
            with mock.patch.dict(os.environ, {
                rh.BACKFILL_COOPERATIVE_PREEMPTION_ENV: "1",
            }):
                result = engine.drain_flow_backfill_until_reserve(
                    rh.CycleDeadline(5.0), block_limit=1_000,
                    reserve_seconds=0.1)

            self.assertEqual(len(calls), 2)
            self.assertNotEqual(
                result["stopped_reason"],
                "projected_chunk_cost_exceeds_headroom")
            self.assertLess(
                result["successful_work_seconds"],
                result["successful_elapsed_seconds"])

    def test_prefetched_chunk_advances_only_after_safe_commit_window(self):
        with tempfile.TemporaryDirectory() as root:
            engine = rh.RobinhoodLearningEngine(
                root, rpc=FakeRPC([], latest=100),
                analyzer=FakeAnalyzer(), market=FakeMarket())
            engine.store.enqueue_backfill(1, 10, "test")
            windows = iter([
                {"required": True, "admitted": False,
                 "available_seconds": 0.0, "reason": "live_lane_active"},
                {"required": True, "admitted": True,
                 "available_seconds": 2.0, "reason": "safe_background_window"},
            ])
            engine.backfill_live_priority_window = lambda: next(windows)
            result = engine.drain_flow_backfill(
                rh.CycleDeadline(5.0), block_limit=10,
                prefetch_before_commit=True)

            self.assertEqual(result["blocks_scanned"], 10)
            self.assertTrue(result["commit_gate"]["waited"])
            self.assertEqual(engine.store.pending_backfill(), [])

    def test_prefetch_deadline_never_advances_the_cursor_without_commit(self):
        with tempfile.TemporaryDirectory() as root:
            engine = rh.RobinhoodLearningEngine(
                root, rpc=FakeRPC([], latest=100),
                analyzer=FakeAnalyzer(), market=FakeMarket())
            engine.store.enqueue_backfill(1, 10, "test")
            engine.backfill_live_priority_window = lambda: {
                "required": True, "admitted": False,
                "available_seconds": 0.0, "reason": "live_lane_active",
            }
            with self.assertRaises(rh.CycleDeadlineExceeded):
                engine.drain_flow_backfill(
                    rh.CycleDeadline(0.2), block_limit=10,
                    prefetch_before_commit=True)

            pending = engine.store.pending_backfill()[0]
            self.assertIsNone(pending.get("next_block"))

    def test_coordinator_prefetches_only_with_isolated_supervised_rpc(self):
        with tempfile.TemporaryDirectory() as root:
            engine = rh.RobinhoodLearningEngine(
                root, rpc=FakeRPC([], latest=100),
                analyzer=FakeAnalyzer(), market=FakeMarket())
            engine.backfill_rpc_isolated = True
            engine.backfill_live_priority_window = lambda: {
                "required": True, "admitted": False,
                "available_seconds": 0.0, "reason": "live_lane_active",
            }
            calls = []

            def drain(_deadline, *, block_limit, **kwargs):
                calls.append((block_limit, kwargs))
                return {"ranges_selected": 0}

            engine.drain_flow_backfill = drain
            with mock.patch.dict(os.environ, {
                rh.BACKFILL_COOPERATIVE_PREEMPTION_ENV: "1",
            }):
                result = engine.drain_flow_backfill_until_reserve(
                    rh.CycleDeadline(5.0), block_limit=1_000,
                    reserve_seconds=0.1)

            self.assertEqual(len(calls), 1)
            self.assertTrue(calls[0][1]["prefetch_before_commit"])
            self.assertTrue(result["cooperative_preemption"]
                            ["prefetch_before_commit_enabled"])

    def test_prefetch_uses_proven_size_then_locally_retries_preemption(self):
        with tempfile.TemporaryDirectory() as root:
            engine = rh.RobinhoodLearningEngine(
                root, rpc=FakeRPC([], latest=100),
                analyzer=FakeAnalyzer(), market=FakeMarket())
            engine.backfill_rpc_isolated = True
            engine.store.set_scheduler_state(
                rh.BACKFILL_RPC_CHUNK_STATE_KEY, {
                    "epoch": rh.BACKFILL_RPC_CHUNK_MODEL_EPOCH,
                    "revision": rh.CODE_REVISION,
                    "stable_chunk_blocks": 250,
                    "next_chunk_blocks": 250,
                    "success_streak": 1,
                })
            engine.backfill_live_priority_window = lambda: {
                "required": True, "admitted": False,
                "available_seconds": 0.0, "reason": "live_lane_active",
            }
            calls = []

            def drain(_deadline, *, block_limit, **kwargs):
                calls.append((block_limit, kwargs))
                if len(calls) == 1:
                    raise rh.CycleDeadlineExceeded(
                        "near_head_ingest_commit")
                if len(calls) <= 4:
                    return {
                        "ranges_selected": 1,
                        "blocks_scanned": block_limit,
                        "from_block": 1,
                        "to_block": block_limit,
                        "prefetch_before_commit": True,
                        "stage_timings_seconds": {
                            "commit_window_wait": 0.0,
                        },
                    }
                return {"ranges_selected": 0}

            engine.drain_flow_backfill = drain
            with mock.patch.dict(os.environ, {
                rh.BACKFILL_COOPERATIVE_PREEMPTION_ENV: "1",
            }):
                result = engine.drain_flow_backfill_until_reserve(
                    rh.CycleDeadline(5.0), block_limit=1_000,
                    reserve_seconds=0.1)

            self.assertEqual(
                [row[0] for row in calls],
                [250, 125, 125, 125, 250])
            self.assertTrue(all(
                row[1].get("prefetch_before_commit") for row in calls))
            self.assertEqual(
                result["cooperative_preemption"]["density_fallbacks"], 1)
            self.assertEqual(result["blocks_scanned"], 375)
            self.assertEqual(
                result["cooperative_preemption"]
                ["fallback_successes_remaining"], 0)
            self.assertEqual(
                engine.backfill_rpc_chunk_plan(1_000)["stable_chunk_blocks"],
                250,
            )


if __name__ == "__main__":
    unittest.main()
