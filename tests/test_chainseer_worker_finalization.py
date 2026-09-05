import inspect
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

import chainseer_robinhood as rh


class WorkerFinalizationTests(unittest.TestCase):
    def test_real_child_exit_zero_leaves_failed_not_successful_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/"learning.sqlite3"
            store=rh.RobinhoodLearningStore(path)
            script=(
                "import os; import chainseer_robinhood as rh; "
                f"s=rh.RobinhoodLearningStore({str(path)!r}); "
                "s.begin_run('child-exit',25,lane='live'); os._exit(0)")
            launched=time.time()
            # Use the real interpreter: Windows venv redirectors have a
            # different PID from the process which owns the database row.
            environment=os.environ.copy()
            environment["PYTHONPATH"]=os.pathsep.join(sys.path)
            process=subprocess.Popen(
                [getattr(sys,"_base_executable",sys.executable),"-c",script],
                env=environment,
                creationflags=getattr(subprocess,"CREATE_NO_WINDOW",0))
            try:
                code=process.wait(timeout=30)
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
            self.assertEqual(code,0)
            self.assertTrue(store.finalize_exited_lane(
                "live",process.pid,code,attempt_started_at=launched))
            self.assertEqual(store.lane_states()["live"]["status"],"failed")

    def test_exit_with_nonterminal_row_is_failure_even_with_zero_exit_code(self):
        for code in (0,1,-1):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                store = rh.RobinhoodLearningStore(Path(directory)/"learning.sqlite3")
                launched = time.time()-1
                store.begin_run("lost",25,lane="live")
                store.mark_lane_stage("live","test_stage",run_id="lost")
                self.assertTrue(store.finalize_exited_lane(
                    "live",os.getpid(),code,attempt_started_at=launched))
                state=store.lane_states()["live"]
                self.assertEqual(state["status"],"failed")
                payload=state["summary"]
                self.assertEqual(payload["returncode"],code)
                self.assertEqual(payload["failure_stage"],"test_stage")
                self.assertFalse(store.finalize_exited_lane(
                    "live",os.getpid(),code,attempt_started_at=launched))

    def test_completed_and_other_lane_are_untouched(self):
        with tempfile.TemporaryDirectory() as directory:
            store=rh.RobinhoodLearningStore(Path(directory)/"learning.sqlite3")
            launched=time.time()-1
            store.begin_run("done",25,lane="live")
            store.finish_run("done","complete",summary={"sentinel":True})
            store.begin_run("analysis",120,lane="analysis")
            self.assertFalse(store.finalize_exited_lane(
                "live",os.getpid(),1,attempt_started_at=launched))
            self.assertEqual(store.lane_states()["live"]["status"],"complete")
            self.assertEqual(store.lane_states()["analysis"]["status"],"running")

    def test_old_pid_reuse_and_newer_lane_pointer_are_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            store=rh.RobinhoodLearningStore(Path(directory)/"learning.sqlite3")
            store.begin_run("old",25,lane="live")
            self.assertFalse(store.finalize_exited_lane(
                "live",os.getpid(),1,attempt_started_at=time.time()+1))
            with store.connection() as c:
                c.execute("UPDATE runs SET pid=123 WHERE run_id='old'")
            store.begin_run("new",25,lane="live")
            self.assertTrue(store.finalize_exited_lane(
                "live",123,1,attempt_started_at=time.time()-10))
            self.assertEqual(store.lane_states()["live"]["run_id"],"new")
            self.assertEqual(store.lane_states()["live"]["status"],"running")

    def test_both_poll_and_shutdown_reconcile_observed_exits(self):
        source=inspect.getsource(rh.supervise_lanes)
        self.assertEqual(source.count("supervisor_store.finalize_exited_lane("),2)


if __name__ == "__main__":
    unittest.main()
