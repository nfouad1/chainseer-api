import json
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

import chainseer_core
from chainseer_core import (
    atomic_json_write,
    read_json,
    schedule_with_live_state,
    scheduled_task_state,
)


class AtomicJsonWriteTests(unittest.TestCase):
    """Concurrent writers must never blend or lose each other's content.

    Pons writes admission_state.json from two processes -- the learn cycle and
    the quote guard. With a fixed "<name>.tmp" scratch file they shared one
    path, so the sequence "A writes scratch, B overwrites the same scratch, B
    replaces, A replaces" either raised FileNotFoundError in A (loud) or gave
    A's destination B's content (silent, and worse).
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_write_then_read_round_trips(self):
        target = self.root / "state.json"
        atomic_json_write(target, {"b": 2, "a": [1, 2, 3]})
        self.assertEqual(read_json(target), {"a": [1, 2, 3], "b": 2})

    def test_no_scratch_file_is_left_behind(self):
        target = self.root / "state.json"
        atomic_json_write(target, {"a": 1})
        self.assertEqual([p.name for p in self.root.iterdir()], ["state.json"])

    def test_a_failed_write_leaves_no_litter(self):
        target = self.root / "state.json"

        class Unserializable:
            pass

        with self.assertRaises(TypeError):
            atomic_json_write(target, {"bad": Unserializable()}, default=None)
        # Unique scratch names are never reused, so a failure that left one
        # behind would accumulate a new stray file on every retry.
        self.assertEqual(list(self.root.iterdir()), [])

    def test_scratch_path_is_unique_per_call(self):
        target = self.root / "state.json"
        seen = []
        real = Path.replace

        def capture(self, other):
            seen.append(self.name)
            return real(self, other)

        with unittest.mock.patch.object(Path, "replace", capture):
            atomic_json_write(target, {"a": 1})
            atomic_json_write(target, {"a": 2})
        self.assertEqual(len(seen), 2)
        self.assertNotEqual(seen[0], seen[1], "scratch name was reused")
        self.assertNotIn("state.json.tmp", seen)

    def test_interleaved_writers_never_blend_or_vanish(self):
        """The exact sequence that broke Pons, driven from many threads.

        Every writer must end with the destination holding exactly one
        writer's complete document -- never a mix, never a partial file, and
        never a FileNotFoundError from a scratch file another writer renamed.
        """
        target = self.root / "state.json"
        payloads = [{"writer": i, "body": [i] * 40} for i in range(12)]
        errors: list[BaseException] = []
        barrier = threading.Barrier(len(payloads))

        def write(payload):
            try:
                barrier.wait(timeout=10)
                for _ in range(15):
                    atomic_json_write(target, payload)
            except BaseException as exc:       # noqa: BLE001 - recorded below
                errors.append(exc)

        threads = [
            threading.Thread(target=write, args=(payload,))
            for payload in payloads
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [], f"concurrent writes raised: {errors[:3]}")
        final = json.loads(target.read_text(encoding="utf-8"))
        # Last writer wins -- that is expected. What must hold is that the
        # winner is ONE writer's intact document.
        self.assertIn(final, payloads)
        self.assertEqual(
            [p.name for p in self.root.iterdir()],
            ["state.json"],
            "scratch files were left behind",
        )


class ScheduledTaskStateTests(unittest.TestCase):
    """A dashboard must report the scheduler's real state, or admit it cannot.

    The adapters persist schedule.json, but only the manage_*.ps1 scripts write
    it. Enabling a task any other way leaves that file stale -- observed in
    production claiming enabled=false from a three-week-old timestamp while the
    task was Running, so the dashboard showed a paused learner that wasn't.
    """

    def setUp(self):
        chainseer_core._TASK_STATE_CACHE.clear()

    tearDown = setUp

    @staticmethod
    def _row(state):
        # schtasks CSV: TaskName, Next Run Time, Status
        return '"Task","N/A","' + state + '"' + chr(10)

    @staticmethod
    def _result(returncode=0, stdout="", stderr=""):
        class Result:
            pass

        value = Result()
        value.returncode = returncode
        value.stdout = stdout
        value.stderr = stderr
        return value

    def _as_windows(self, run_value):
        return (
            unittest.mock.patch.object(chainseer_core.os, "name", "nt"),
            unittest.mock.patch.object(
                chainseer_core.subprocess, "run", return_value=run_value
            ),
        )

    def test_running_task_reads_as_enabled(self):
        os_patch, run_patch = self._as_windows(self._result(stdout=self._row("Running")))
        with os_patch, run_patch:
            state = scheduled_task_state("Task")
        self.assertEqual(state["state"], "Running")
        self.assertTrue(state["enabled"])
        self.assertTrue(state["available"])

    def test_disabled_is_the_only_state_meaning_not_enabled(self):
        for raw, expected in (("Ready", True), ("Running", True), ("Disabled", False)):
            chainseer_core._TASK_STATE_CACHE.clear()
            os_patch, run_patch = self._as_windows(self._result(stdout=self._row(raw)))
            with os_patch, run_patch, self.subTest(state=raw):
                self.assertEqual(scheduled_task_state("Task")["enabled"], expected)

    def test_unknown_state_is_none_not_false(self):
        """Never claim the learner is stopped merely because we cannot see it."""
        os_patch, run_patch = self._as_windows(
            self._result(returncode=1, stderr="ERROR: cannot find the file")
        )
        with os_patch, run_patch:
            state = scheduled_task_state("Absent")
        self.assertIsNone(state["enabled"])
        self.assertFalse(state["available"])
        self.assertIn("cannot find", state["reason"])

    def test_non_windows_degrades_without_claiming_disabled(self):
        with unittest.mock.patch.object(chainseer_core.os, "name", "posix"):
            state = scheduled_task_state("Anything")
        self.assertIsNone(state["enabled"])
        self.assertFalse(state["available"])

    def test_result_is_cached_so_polling_does_not_spawn_a_process_each_time(self):
        calls = []

        def fake_run(*args, **kwargs):
            calls.append(args)
            return self._result(stdout=self._row("Ready"))

        with unittest.mock.patch.object(chainseer_core.os, "name", "nt"), \
                unittest.mock.patch.object(chainseer_core.subprocess, "run", fake_run):
            for _ in range(5):
                scheduled_task_state("Task")
        self.assertEqual(len(calls), 1, "cache did not hold")

    def test_live_state_overrides_the_declared_file_and_flags_drift(self):
        declared = {"task_name": "Task", "enabled": False, "interval_minutes": 10}
        os_patch, run_patch = self._as_windows(self._result(stdout=self._row("Running")))
        with os_patch, run_patch:
            merged = schedule_with_live_state(declared)
        self.assertTrue(merged["enabled"])              # reality wins
        self.assertFalse(merged["declared_enabled"])    # stale value preserved
        self.assertTrue(merged["schedule_drift"])       # and the gap is visible
        self.assertEqual(merged["interval_minutes"], 10)  # static config intact

    def test_no_drift_when_file_and_reality_agree(self):
        os_patch, run_patch = self._as_windows(self._result(stdout=self._row("Ready")))
        with os_patch, run_patch:
            merged = schedule_with_live_state({"task_name": "Task", "enabled": True})
        self.assertFalse(merged["schedule_drift"])

    def test_unavailable_scheduler_reports_enabled_none(self):
        with unittest.mock.patch.object(chainseer_core.os, "name", "posix"):
            merged = schedule_with_live_state({"task_name": "Task", "enabled": True})
        self.assertIsNone(merged["enabled"])
        self.assertIn("enabled_unavailable_reason", merged)

    def test_missing_task_name_leaves_the_declared_block_alone(self):
        merged = schedule_with_live_state({"enabled": True})
        self.assertTrue(merged["enabled"])
        self.assertEqual(merged["enabled_source"], "declared")


if __name__ == "__main__":
    unittest.main()
