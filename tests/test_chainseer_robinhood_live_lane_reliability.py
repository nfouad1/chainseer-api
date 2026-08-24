"""Live-lane reliability tests.

Proves the seven requirement areas from the 2026-08-24 review:
non-overlapping stage timings, sub-stage attribution that survives a forced
termination, shared-deadline enforcement (stalled prefetch AND stalled
individual quotes interrupted and attributed), zero-headroom queues
everything, censored timeout samples never lower the estimator, two-cycle
deferred recovery with current-cycle priority, bounded backlog queries, and
no Timechain operation on the decision-critical path.
"""
from __future__ import annotations

import json
import multiprocessing
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import DecisionCommitmentStore


TOKEN = "0x" + "11" * 20
POOL_ID = "0x" + "ab" * 32
HEAD = 44_500_000


class _Market:
    """Records snapshots; optional per-call stall."""

    def __init__(self, stall=0.0):
        self.snapshots = 0
        self.stall = stall
        self.on_snapshot = None  # optional callable(candidate)

    def prime_window_quotes(self, windows, deadline=None):
        return {"batched": len(windows), "windows": len(windows)}

    def snapshot(self, candidate, quote_block=None, **kwargs):
        self.snapshots += 1
        if self.on_snapshot is not None:
            self.on_snapshot(candidate)
        if self.stall:
            time.sleep(self.stall)
        return {"execution_quote": {"verified": True},
                "current_state_verified": True}


class _Rpc:
    timeout = 5.0

    def get_block_number(self):
        return HEAD + 10


class _Ledger:
    def __init__(self):
        self.events = []

    def append(self, kind, payload):
        self.events.append((kind, payload))


class _SpyTimechain:
    """Any call here from the live lane is a violation."""

    def __init__(self):
        self.calls = []

    def __getattr__(self, name):
        def record(*args, **kwargs):
            self.calls.append(name)
        return record


def make_engine(root, store, market=None, timechain=None):
    engine = object.__new__(rh.RobinhoodLearningEngine)
    engine.root = Path(root)
    engine.store = store
    engine.v4_market = market if market is not None else _Market()
    engine.market = None
    engine.rpc = _Rpc()
    engine.ledger = _Ledger()
    engine.timechain_recorder = timechain
    engine.commitments = DecisionCommitmentStore(
        Path(root) / "decision_commitments.sqlite3")
    engine.cycle_run_uuid = "test-run-id"
    # Register the lane as running so mark_lane_stage's UPDATE (which only
    # touches rows with status='running') can persist sub-stage markers.
    with store.connection() as connection:
        connection.execute(
            """INSERT OR REPLACE INTO lane_state (
                   lane, run_id, pid, status, started_at
               ) VALUES ('live', 'test-run-id', 0, 'running', ?)""",
            (time.time(),))
    return engine


def seed_windows(store, count, head=HEAD):
    now = rh._utc_now()
    with store.connection() as connection:
        for i in range(count):
            connection.execute(
                """INSERT OR IGNORE INTO flow_signals (
                       source_version, pool_id, token_address,
                       window_blocks, window_start_block,
                       window_end_block, computed_at, swap_count,
                       buy_count, sell_count, unique_sender_hints,
                       unique_resolved_participants, identity_coverage,
                       buy_ratio, net_anchor_flow_fraction, price_multiple,
                       uncapped_shadow_score, shadow_score,
                       shadow_qualified, confidence,
                       qualification_gaps_json, limitations_json,
                       features_json
                   ) VALUES ('test', ?, ?, 1350, ?, ?, ?, 1, 1, 0, 1,
                             0, 1.0, 0.5, 0.0, 1.0, 0.0, 0.0, 0, 0.5,
                             '[]', '[]', '{}')""",
                (POOL_ID[:-4] + f"{i:04x}", TOKEN,
                 head - 1350 - i, head - i * 10, now))


def seed_observation(store, observation_id, sealed_at=1000.0):
    with store.connection() as connection:
        connection.execute(
            """INSERT INTO flow_observations (
                   observation_id, pool_id, token_address,
                   policy_version, observation_head,
                   observation_head_lag_blocks, observed_at,
                   observed_at_epoch, window_start_block,
                   window_end_block, transaction_set_hash,
                   transaction_count, features_json, quote_json,
                   sealed_at
               ) VALUES (?, '0x' || 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
                         '0x' || 'bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
                         ?, 100, 0, '2026-08-24T00:00:00+00:00',
                         ?, 90, 100, '0xseed', 1, '{}', '{}', ?)""",
            (observation_id, rh.FLOW_EVIDENCE_POLICY_VERSION,
             sealed_at, sealed_at))


def _hard_kill_observation_worker(root_text: str) -> None:
    """Child target for the real OS-termination durability proof."""
    root = Path(root_text)
    store = rh.RobinhoodLearningStore(root / "hard-kill.sqlite3")
    seed_windows(store, 3)
    engine = make_engine(root, store, _Market(stall=60.0))
    with rh.LearningRunLock(root / ".live_once.lock"):
        engine.seal_near_head_observations(
            HEAD, time.time(), pool_ids=[POOL_ID],
            deadline=rh.CycleDeadline(120.0), limit=3,
            reserve_seconds=0.5,
        )


def lane_state(store, lane="live"):
    with store.connection() as connection:
        row = connection.execute(
            "SELECT current_stage, stage_detail_json FROM lane_state"
            " WHERE lane=?", (lane,)).fetchone()
    if row is None or not row["current_stage"]:
        return None, {}
    try:
        return row["current_stage"], json.loads(row["stage_detail_json"] or "{}")
    except (TypeError, ValueError):
        return row["current_stage"], {}


class StageTimingBoundaryTests(unittest.TestCase):
    """Requirement 1: five disjoint clocks, and the deprecated total derived."""

    def test_stage_timings_are_disjoint_and_total_is_derived(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 2)
            engine = make_engine(root, store)
            engine.near_head_flow_pass = lambda **kwargs: {
                "scanned": True, "to_block": HEAD,
                "touched_pool_ids": [POOL_ID]}
            summary = engine.run_live_lane(budget_seconds=25.0)
            timings = summary["stage_timings_seconds"]
            five = [
                "head_ingestion_and_identity_seconds",
                "fresh_quote_and_observation_seconds",
                "decision_head_seconds", "classification_seconds",
                "ledger_append_seconds",
            ]
            for key in five:
                self.assertIn(key, timings)
                self.assertGreaterEqual(timings[key], 0.0)
            # Derived total equals the sum of the five -- it cannot
            # double-count stages the way the old measured clock did.
            self.assertTrue(timings["seal_and_fresh_quote_is_derived_total"])
            self.assertEqual(
                timings["seal_and_fresh_quote"],
                round(sum(timings[key] for key in five), 3))
            # Each stage's clock starts only when the previous ended, so the
            # sum of five covers the whole lane body: nothing else ran.
            self.assertLessEqual(
                timings["seal_and_fresh_quote"],
                summary["duration_seconds"] + 0.05)

    def test_absolute_deadline_charges_child_startup_to_same_budget(self):
        now = time.monotonic()
        deadline = rh.CycleDeadline(
            25.0, deadline_monotonic=now + 10.0)
        self.assertLessEqual(deadline.remaining(), 10.0)
        self.assertGreater(deadline.remaining(), 9.5)
        # Fifteen seconds were already consumed before the child constructed
        # its deadline; started reconstructs the supervisor launch point.
        self.assertAlmostEqual(now - deadline.started, 15.0, delta=0.1)
        source = Path(rh.__file__).read_text(
            encoding="utf-8", errors="replace")
        supervisor = source.split("def supervise_lanes", 1)[1].split(
            "def dashboard_operational_snapshot", 1)[0]
        self.assertIn("CHAINSEER_LANE_DEADLINE_MONOTONIC", supervisor)


class SubstageAttributionTests(unittest.TestCase):
    """Requirement 2 (+ part of 3): WHERE inside sealing are we?"""

    def test_stalled_prefetch_is_attributed_and_interrupted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 4)
            observed_stage = {}

            class StallingPrimerMarket(_Market):
                def prime_window_quotes(self, windows, deadline=None):
                    # Read the PERSISTED sub-stage from INSIDE the stall --
                    # exactly what a post-mortem after a hard kill sees.
                    observed_stage["during"] = lane_state(store)[0]
                    time.sleep(13.5)  # eats everything past prefetch
                    return {"batched": len(windows),
                            "windows": len(windows)}

                def snapshot(self, candidate, quote_block=None, **kw):
                    raise AssertionError(
                        "must not be reached after the stall")

            market = StallingPrimerMarket()
            engine = make_engine(root, store, market)
            result = engine.seal_near_head_observations(
                HEAD, time.time(), pool_ids=[POOL_ID],
                deadline=rh.CycleDeadline(14.0), limit=4,
                reserve_seconds=0.5)
            self.assertIn("quote_prefetch", observed_stage["during"])
            # Interrupted: nothing was quoted after the stall ate the budget,
            # and EVERY unprocessed window was durably queued.
            self.assertEqual(market.snapshots, 0)
            self.assertEqual(result["windows_queued"], 4)
            self.assertEqual(result["sealed_this_cycle"], 0)
            self.assertGreater(result["phase_seconds"]["prefetch"]["total"], 0)

    def test_stalled_individual_quote_is_attributed_and_interrupted(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 3)
            seen_during_quote = []

            market = _Market(stall=4.0)
            seen = []
            market.on_snapshot = lambda candidate: seen.append(
                lane_state(store))
            engine = make_engine(root, store, market)
            result = engine.seal_near_head_observations(
                HEAD, time.time(), pool_ids=[POOL_ID],
                deadline=rh.CycleDeadline(10.0), limit=3,
                reserve_seconds=0.5)
            # While the FIRST quote was in flight, the persisted sub-stage
            # named that exact window.
            first = seen[0]
            self.assertIn("window_quote_rpc", first[0])
            self.assertEqual(first[1].get("window_index"), 0)
            # Interrupted AFTER the stalled call returned: fewer windows
            # sealed than admitted, the rest queued, and the interruption
            # is attributed in the payload.
            self.assertLess(result["sealed_this_cycle"], 3)
            self.assertGreater(result["windows_queued"], 0)
            self.assertIn("interrupted_after_window", result["prefetch"])
            # The persisted sub-stage named the SEEDED pool of window 0.
            seeded_pool = POOL_ID[:-4] + "0000"
            self.assertEqual(first[1].get("pool_id"), seeded_pool)

    def test_forced_termination_leaves_queue_full_and_no_debris(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 3)

            class KilledMidQuote(_Market):
                def snapshot(self, candidate, quote_block=None, **kw):
                    raise rh.CycleDeadlineExceeded("hard_kill_simulation")

            engine = make_engine(root, store, KilledMidQuote())
            with rh.LearningRunLock(root / ".live_once.lock"):
                # The kill lands mid-quote; the finally-block must still
                # queue every unprocessed window and re-raise.
                with self.assertRaises(rh.CycleDeadlineExceeded):
                    engine.seal_near_head_observations(
                        HEAD, time.time(), pool_ids=[POOL_ID],
                        deadline=rh.CycleDeadline(8.0), limit=3,
                        reserve_seconds=0.5)
            # Lock released by the context manager.
            self.assertFalse((root / ".live_once.lock").exists())
            # No temporary debris left behind.
            leftovers = [p.name for p in root.iterdir()
                         if p.name.endswith((".tmp", ".temp"))
                         or p.name.startswith(("._", "~"))]
            self.assertEqual(leftovers, [])
            # Every unprocessed window sits in the DURABLE queue.
            self.assertEqual(
                store.seal_queue_backlog()["pending_windows"], 3)
            # The persisted stage names the sealing stage; the kill landed
            # during window_quote_rpc, and settlement legitimately runs
            # after (queue_settlement is the last marker), so attribute via
            # the failure detail recorded before the finally-block ran.
            stage, _detail = lane_state(store)
            self.assertIn("fresh_quote_and_observation", stage or "")

    def test_real_process_kill_preserves_prequeued_windows_and_releases_lock(self):
        """An OS kill bypasses finally; durability must already be committed."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            context = multiprocessing.get_context("spawn")
            process = context.Process(
                target=_hard_kill_observation_worker,
                args=(str(root),),
            )
            process.start()
            db_path = root / "hard-kill.sqlite3"
            deadline = time.monotonic() + 20.0
            pending = 0
            stage = None
            while time.monotonic() < deadline:
                if db_path.exists():
                    connection = None
                    try:
                        # Read directly while the child owns schema setup.
                        # Constructing a second Store here would run migrations
                        # concurrently and turn this kill test into a schema-
                        # initialization race unrelated to the behavior under
                        # test.
                        connection = sqlite3.connect(
                            str(db_path), timeout=0.1)
                        pending = connection.execute(
                            "SELECT COUNT(*) FROM flow_seal_queue"
                            " WHERE completed_at IS NULL"
                        ).fetchone()[0]
                        state = connection.execute(
                            "SELECT current_stage FROM lane_state"
                            " WHERE lane='live'"
                        ).fetchone()
                        stage = state[0] if state else None
                    except (sqlite3.Error, OSError):
                        pending = 0
                    finally:
                        if connection is not None:
                            connection.close()
                    if pending == 3 and stage and "window_quote_rpc" in stage:
                        break
                time.sleep(0.05)
            self.assertEqual(pending, 3)
            self.assertIn("window_quote_rpc", stage or "")

            process.kill()
            process.join(timeout=10.0)
            self.assertFalse(process.is_alive())

            # The selected targets were queued BEFORE the blocking quote, so
            # no child cleanup/finally block is needed to recover them.
            store = rh.RobinhoodLearningStore(db_path)
            self.assertEqual(
                store.seal_queue_backlog()["pending_windows"], 3)
            # The OS releases the advisory handle when it kills the process;
            # a new cycle can acquire the same lock immediately.
            with rh.LearningRunLock(root / ".live_once.lock"):
                pass
            leftovers = [
                path.name for path in root.iterdir()
                if path.name.endswith((".tmp", ".temp"))
                or path.name.startswith(("._", "~"))
            ]
            self.assertEqual(leftovers, [])

    def test_supervisor_kill_carries_detail_and_records_censored_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            run_id = "supervisor-kill-test"
            fake_pid = 987654
            store.begin_run(run_id, 25.0, lane="live")
            with store.connection() as connection:
                connection.execute(
                    "UPDATE runs SET pid=? WHERE run_id=?",
                    (fake_pid, run_id),
                )
                connection.execute(
                    "UPDATE lane_state SET pid=? WHERE lane='live'",
                    (fake_pid,),
                )
            store.mark_lane_stage(
                "live", "fresh_quote_and_observation/window_quote_rpc",
                run_id=run_id, remaining=3.0,
                detail={"window_index": 2, "pool_id": POOL_ID},
            )
            time.sleep(0.02)
            attempt_started_at = time.time() - 2.0
            store.terminate_lane(
                "live", fake_pid, "supervisor_hard_deadline_exceeded",
                attempt_started_at=attempt_started_at)
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT summary_json FROM runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()
            summary = json.loads(row[0])
            self.assertEqual(
                summary["failure_substage_detail"]["window_index"], 2)
            self.assertEqual(
                summary["failure_substage_detail"]["pool_id"], POOL_ID)
            self.assertGreaterEqual(summary["duration_seconds"], 2.0)
            model = store.scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY)
            self.assertEqual(model["censored_samples"], 1)
            self.assertEqual(model["last_censored_stage"],
                             "fresh_quote_and_observation/window_quote_rpc")
            self.assertGreaterEqual(len(model["per_window_samples"]), 1)


class AdmissionEstimatorTests(unittest.TestCase):
    """Requirements 4 (+ zero-headroom half of 3)."""

    def test_zero_usable_headroom_queues_everything(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 5)
            store.set_scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY, {
                "fixed_observation_cost_p95": 2.0,
                "per_window_cost_p95": 4.0,
                "downstream_reserve_p95": 6.0})
            market = _Market()
            result = make_engine(root, store, market).seal_near_head_observations(
                HEAD, time.time(), pool_ids=[POOL_ID],
                deadline=rh.CycleDeadline(7.0), limit=5,
                reserve_seconds=rh.LIVE_LANE_DECISION_RESERVE_SECONDS)
            # usable = 7 - 6 - 2 < 0 -> admit ZERO, queue ALL.
            self.assertEqual(market.snapshots, 0)
            self.assertEqual(result["windows_admitted"], 0)
            self.assertEqual(result["windows_queued"], 5)

    def test_positive_but_sub_window_headroom_admits_zero(self):
        """A durable queue is safer than knowingly consuming the tail reserve."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 3)
            store.set_scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY, {
                "fixed_observation_cost_p95": 1.0,
                "queue_settlement_p95": 1.0,
                "per_window_cost_p95": 4.0,
                "downstream_reserve_p95": 5.0,
            })
            # Roughly 0.5s is usable after fixed + downstream costs: positive,
            # but less than one predicted 4s window.
            result = make_engine(root, store).seal_near_head_observations(
                HEAD, time.time(), pool_ids=[POOL_ID],
                deadline=rh.CycleDeadline(6.55), limit=3,
                reserve_seconds=5.0,
            )
            self.assertEqual(result["windows_admitted"], 0)
            self.assertEqual(result["windows_queued"], 3)

    def test_cost_model_is_nearest_rank_p95_of_durable_samples(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            engine = make_engine(root, store)
            engine._blend_seal_model({
                "per_window_cost_p95": [float(i) for i in range(1, 21)]
            })
            model = engine.seal_cost_model()
            self.assertEqual(model["per_window_cost_p95"], 19.0)
            self.assertEqual(model["per_window_sample_count"], 20)

    def test_admission_follows_the_two_part_formula(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 8)
            store.set_scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY, {
                "fixed_observation_cost_p95": 1.0,
                "per_window_cost_p95": 1.0,
                "downstream_reserve_p95": 4.0})
            result = make_engine(root, store).seal_near_head_observations(
                HEAD, time.time(), pool_ids=[POOL_ID],
                deadline=rh.CycleDeadline(12.0), limit=8,
                reserve_seconds=rh.LIVE_LANE_DECISION_RESERVE_SECONDS)
            # usable = 11.97 - max(5,4) - 1 = 5.97 -> floor = 5 windows.
            self.assertEqual(result["windows_admitted"], 5)
            self.assertAlmostEqual(
                result["admission"]["usable_seconds"], 5.97, places=0)

    def test_timeout_samples_cannot_lower_the_estimator(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            engine = make_engine(root, store)
            # A real measurement first.
            engine.record_seal_cost({
                "selection": [0.1], "prefetch": [0.1], "queue_settle": [0.1],
                "quote_rpc": [0.5, 0.5], "database_commit": [0.1, 0.1]},
                2)
            raised = engine.seal_cost_estimate()
            # Now several TIMEOUT cycles report tiny elapsed times: a killed
            # attempt is a LOWER BOUND, so it must never pull the estimate
            # down toward its own (unobservably small) figure.
            for _ in range(6):
                engine.record_seal_cost_censored(0.001)
            self.assertGreaterEqual(engine.seal_cost_estimate(), raised)
            model = engine.seal_cost_model()
            self.assertGreaterEqual(model["censored_samples"], 6)

    def test_downstream_reserve_is_learned_from_completed_cycles(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            engine = make_engine(root, store)
            before = engine.downstream_reserve_estimate()
            measured = engine.record_downstream_reserve(before + 3.0)
            self.assertGreater(measured, before)
            self.assertEqual(
                make_engine(root, store).downstream_reserve_estimate(),
                measured,
                "the estimate must be durable across engine instances")


class DeferredRecoveryTests(unittest.TestCase):
    """Requirement 5 selection semantics + P0 never-dropped guarantee."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = rh.RobinhoodLearningStore(self.root / "l.sqlite3")
        self.engine = make_engine(self.root, self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def test_two_cycle_recovery_with_current_cycle_priority(self):
        for i in range(40):
            # A LARGE synthetic deferred backlog...
            seed_observation(self.store, f"old-{i:03d}",
                             sealed_at=1000.0 + i)
        # ...plus the three fresh observations across the two cycles.
        seed_observation(self.store, "fresh-0", sealed_at=2000.0)
        seed_observation(self.store, "fresh-1", sealed_at=2001.0)
        seed_observation(self.store, "fresh-2", sealed_at=2002.0)
        # Cycle 1 seals two fresh observations but admits only ONE
        # classification slot.
        cycle1 = self.engine.classify_sealed_observations(
            12345, observation_ids=["fresh-0", "fresh-1"],
            admission_limit=1)
        self.assertEqual(cycle1["scoped_rows_processed"], 1)
        with self.store.connection() as connection:
            classified = {row[0] for row in connection.execute(
                "SELECT observation_id FROM"
                " flow_observation_classifications")}
        self.assertIn("fresh-0", classified)
        self.assertNotIn("fresh-1", classified)
        # Cycle 2 passes ONLY its own fresh id with a generous limit: the
        # current-cycle id classifies first, THEN the OLDEST deferred rows
        # (fresh-1 from cycle 1 is now just a deferred row, older than it
        # only in sealed_at... but old-* rows are older still, so they go
        # first). The P0 guarantee: fresh-1 is eventually revisited.
        cycle2 = self.engine.classify_sealed_observations(
            12346, observation_ids=["fresh-2"], admission_limit=3)
        self.assertEqual(cycle2["scoped_rows_processed"], 3)
        with self.store.connection() as connection:
            classified = {row[0] for row in connection.execute(
                "SELECT observation_id FROM"
                " flow_observation_classifications")}
        self.assertIn("fresh-2", classified)
        self.assertIn("old-000", classified,
                      "deferred observations are eventually revisited")
        self.assertNotIn("old-039", classified,
                         "a bounded LIMIT fills from the oldest, not at random")
        # A third cycle with a generous limit catches everything left,
        # including cycle 1's deferred fresh-1: nothing is dropped.
        cycle3 = self.engine.classify_sealed_observations(
            12347, admission_limit=100)
        with self.store.connection() as connection:
            classified = {row[0] for row in connection.execute(
                "SELECT observation_id FROM"
                " flow_observation_classifications")}
        self.assertIn("fresh-1", classified)

    def test_large_backlog_is_queried_with_bounded_limit(self):
        for i in range(2000):
            seed_observation(self.store, f"backlog-{i:04d}",
                             sealed_at=1000.0 + i)
        seed_observation(self.store, "fresh-0", sealed_at=2000.0)
        result = self.engine.classify_sealed_observations(
            1, observation_ids=["fresh-0"], admission_limit=4)
        # Materialized rows never exceeded the slots: selected <= limit + 1
        # (the current-cycle id), even though 2001 unclassified rows exist.
        self.assertLessEqual(result["scoped_rows_selected"], 4 + 1)
        self.assertEqual(result["scoped_rows_processed"], 4)
        self.assertGreater(result["scoped_rows_deferred"], 1990)
        self.assertEqual(result["admission"]["admission_exceeded"],
                         2001 - 4)

    def test_no_timechain_operation_on_the_decision_critical_path(self):
        spy = _SpyTimechain()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            seed_windows(store, 2)
            seed_observation(store, "obs-0")
            engine = make_engine(root, store, _Market(), timechain=spy)
            engine.near_head_flow_pass = lambda **kwargs: {
                "scanned": True, "to_block": HEAD,
                "touched_pool_ids": [POOL_ID]}
            summary = engine.run_live_lane(budget_seconds=25.0)
            self.assertEqual(summary["status"], "complete")
            self.assertEqual(
                spy.calls, [],
                "a Timechain operation ran inside the live lane")
        # And structurally: the live-lane body never references the async
        # Timechain writers.
        source = Path(rh.__file__).read_text(
            encoding="utf-8", errors="replace")
        live = source.split("def run_live_lane", 1)[1].split(
            "def _analyze_candidates", 1)[0]
        for banned in ("drain_deferred_seals", "publish_integrity_certificate"):
            self.assertNotIn(banned, live)


class ReliabilityTelemetryTests(unittest.TestCase):
    """Requirement 6: dashboard telemetry aggregates durable state."""

    def test_telemetry_counts_timeouts_as_failures_and_reports_estimates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "l.sqlite3")
            runs = [
                ("complete", {"duration_seconds": 20.0}),
                ("deadline_exceeded", {"duration_seconds": 28.0,
                                       "failure_stage":
                                       "fresh_quote_and_observation/"
                                       "window_quote_rpc"}),
                ("deadline_exceeded", {"duration_seconds": 25.5,
                                       "failure_stage": "classification"}),
                ("failed", {"duration_seconds": 2.0}),
            ]
            for i, (status, summary) in enumerate(runs):
                store.begin_run(f"run-{i}", 25.0, lane="live")
                store.finish_run(f"run-{i}", status, summary=summary)
            store.set_scheduler_state(rh.SEAL_COST_MODEL_STATE_KEY, {
                "fixed_observation_cost_p95": 1.0,
                "per_window_cost_p95": 2.5,
                "downstream_reserve_p95": 5.0,
                "censored_samples": 3})
            telemetry = rh.live_lane_reliability_snapshot(root, store=store)
            # Completion rate INCLUDES timeouts as failures: 1 of 4.
            self.assertEqual(telemetry["window_runs"], 4)
            self.assertEqual(telemetry["completion_rate"], 0.25)
            self.assertEqual(telemetry["timeout_count_by_stage"].get(
                "fresh_quote_and_observation/window_quote_rpc"), 1)
            self.assertEqual(telemetry["timeout_count_by_stage"].get(
                "classification"), 1)
            # All-attempt p95 includes failed attempts' durations.
            self.assertAlmostEqual(telemetry["all_attempt_p95_seconds"], 28.0)
            self.assertEqual(
                telemetry["seal_cost_model"]["per_window_cost_p95"], 2.5)
            self.assertEqual(
                telemetry["seal_cost_model"]["censored_samples"], 3)


if __name__ == "__main__":
    unittest.main()
