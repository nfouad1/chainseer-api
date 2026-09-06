import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import xml.etree.ElementTree as ET

from chainseer_learner_activity import classify_activity, learner_activity
import run_chainseer_robinhood_learning as runner_module


class ActivityTests(unittest.TestCase):
    def activity(self, **overrides):
        data = dict(
            schedule={"enabled": True, "interval_minutes": 5},
            scheduler={"status": "running", "heartbeat_at": 995, "started_at": 900},
            runner={}, live={"status": "complete", "timestamp": 980},
            evidence={"status": "complete", "timestamp": 970,
                      "shadow_path_marks": {"observed": 28, "stop_reason": "deadline_reserve"}},
            now=1000,
        )
        data.update(overrides)
        return classify_activity(**data)

    def test_current_activity_is_independent_of_cohort(self):
        self.assertEqual(self.activity()["status"], "active")
        self.assertEqual(self.activity()["evidence"]["status"], "collecting")
        self.assertTrue(self.activity()["historical_cohort_is_not_liveness"])

    def test_disabled_is_paused_even_with_recent_heartbeat(self):
        self.assertEqual(self.activity(schedule={"enabled": False})["status"], "paused")

    def test_recovery_priority_is_visible_without_hiding_evidence_age(self):
        state = self.activity(scheduler={"status": "running", "heartbeat_at": 995,
            "backfill_pressure": {"priority": True}}, evidence={"timestamp": 200})
        self.assertEqual(state["evidence"]["status"], "stale")
        self.assertEqual(state["evidence"]["scheduling_constraint"], "backfill_recovery_priority")

    def test_sleep_gap_is_not_active(self):
        self.assertEqual(self.activity(now=5000)["status"], "stale")
        self.assertEqual(self.activity(now=5000)["evidence"]["status"], "stale")

    def test_recovery_needs_completed_work(self):
        self.assertEqual(self.activity(live={}, scheduler={"status": "running", "heartbeat_at": 995,
                                                         "started_at": 990})["status"], "recovering")

    def test_idle_supervisor_does_not_imply_live_progress(self):
        self.assertEqual(self.activity(live={})["status"], "stalled")

    def test_latest_failed_attempt_overrides_previous_success(self):
        self.assertEqual(self.activity(scheduler={"status": "running", "heartbeat_at": 995,
            "lane_state": {"live": {"status": "failed", "heartbeat_at": 990}}})["status"], "degraded")

    def test_controlled_deferral_is_not_success_or_stall(self):
        state = self.activity(live={"timestamp": 980, "status": "deferred", "controlled_deferral": True})
        self.assertEqual(state["status"], "deferred")
        self.assertEqual(state["live_status"], "deferred")

    def test_between_windows_then_missing_launch(self):
        scheduler = {"status": "complete", "heartbeat_at": 990}
        self.assertEqual(self.activity(scheduler=scheduler)["status"], "waiting")
        self.assertEqual(self.activity(scheduler=scheduler, now=1400)["status"], "stale")

    def test_new_launcher_failure_overrides_old_heartbeat(self):
        self.assertEqual(self.activity(runner={"status": "failed", "heartbeat_at": 999})["status"], "error")

    def test_future_or_missing_telemetry_is_unknown(self):
        self.assertEqual(self.activity(scheduler={"heartbeat_at": 1001})["status"], "unknown")
        self.assertEqual(self.activity(scheduler={"heartbeat_at": "NaN"})["status"], "unknown")

    def test_no_work_requires_explicit_worker_check(self):
        evidence = {"status": "complete", "timestamp": 970, "shadow_path_marks": {
            "observed": 0, "selected": 0, "limit": 32, "stop_reason": "selected_batch_drained",
            "more_due_available": False}}
        self.assertEqual(self.activity(evidence=evidence)["evidence"]["status"], "no_eligible_work")
        del evidence["shadow_path_marks"]["selected"]
        self.assertEqual(self.activity(evidence=evidence)["evidence"]["status"], "deferred")

    def test_malformed_nested_telemetry_does_not_break_api(self):
        self.assertEqual(self.activity(scheduler={"lane_state": []},
            evidence={"timestamp": 970, "status": "complete", "shadow_path_marks": "bad"})
            ["evidence"]["status"], "deferred")

    def test_disabled_batch_does_not_claim_no_work(self):
        evidence = {"timestamp": 970, "status": "complete", "shadow_path_marks": {
            "observed": 0, "selected": 0, "limit": 0,
            "stop_reason": "selected_batch_drained", "more_due_available": False}}
        self.assertEqual(self.activity(evidence=evidence)["evidence"]["status"], "deferred")

    def test_fresh_live_cannot_hide_old_evidence(self):
        evidence = {"status": "complete", "timestamp": 200}
        state = self.activity(evidence=evidence)
        self.assertEqual(state["status"], "active")
        self.assertEqual(state["evidence"]["status"], "stale")

    def test_reads_are_read_only_and_age_again_on_every_call(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "schedule.json").write_text('broken')
            (root / "scheduler_status.json").write_text(json.dumps({"status": "complete", "heartbeat_at": 990}))
            before = {p.name: p.read_bytes() for p in root.iterdir()}
            self.assertEqual(learner_activity(root, now=1000)["status"], "stale")
            self.assertEqual({p.name: p.read_bytes() for p in root.iterdir()}, before)

    def test_initializing_runner_is_not_terminal(self):
        with tempfile.TemporaryDirectory() as tmp:
            runner_module._runner_status(Path(tmp), status="initializing", started=1000)
            data = json.loads((Path(tmp) / "runner_status.json").read_text())
            self.assertIsNone(data["completed_at"])

    def test_dashboard_handles_api_loss_and_renders_activity(self):
        html = Path("robinhood_dashboard.html").read_text(encoding="utf-8")
        self.assertIn("renderLearnerActivity(d.learner_activity)", html)
        self.assertIn("previous activity is not current proof", html)
        self.assertIn("Operational acceptance cohort", html)


@unittest.skipUnless(sys.platform == "win32", "Windows Task Scheduler XML")
class ResumeTriggerTests(unittest.TestCase):
    def test_transform_is_idempotent_and_preserves_existing_settings(self):
        namespace = "http://schemas.microsoft.com/windows/2004/02/mit/task"
        xml = f'''<Task xmlns="{namespace}"><Triggers><TimeTrigger><StartBoundary>2026-09-05T10:00:00</StartBoundary></TimeTrigger></Triggers><Principals><Principal id="Author"><UserId>test</UserId></Principal></Principals><Settings><MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy><Enabled>false</Enabled><WakeToRun>false</WakeToRun></Settings><Actions><Exec><Command>python.exe</Command></Exec></Actions></Task>'''
        helper = str(Path("chainseer_task_recovery.ps1").resolve()).replace("'", "''")
        command = f". '{helper}'; [xml]$taskInput=[Console]::In.ReadToEnd(); $once=Add-RobinhoodResumeTrigger $taskInput; Add-RobinhoodResumeTrigger ([xml]$once)"
        result = subprocess.run(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
                                input=xml, text=True, capture_output=True, timeout=20)
        self.assertEqual(result.returncode, 0, result.stderr)
        tree = ET.fromstring(result.stdout.strip())
        original = ET.fromstring(xml)
        ns = {"t": namespace}
        events = tree.findall("t:Triggers/t:EventTrigger", ns)
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].find("t:Delay", ns).text, "PT60S")
        self.assertIn("Power-Troubleshooter", events[0].find("t:Subscription", ns).text)
        for path in ("t:Settings", "t:Actions", "t:Principals", "t:Triggers/t:TimeTrigger"):
            self.assertEqual(ET.tostring(tree.find(path, ns)), ET.tostring(original.find(path, ns)))


if __name__ == "__main__":
    unittest.main()
