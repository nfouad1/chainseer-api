import inspect
import unittest
import chainseer_robinhood as rh


class EvidenceServiceTests(unittest.TestCase):
    def choose(self, **kwargs):
        return rh._select_background_candidate(
            [(name, {"next": index}) for index, name in enumerate(
                ("evidence", "backfill", "marks", "certificate", "memory"))],
            {"live": 10, "marks": 10}, backfill_pressure={"priority": True}, **kwargs)

    def test_overdue_evidence_gets_recovery_slot(self):
        self.assertEqual(self.choose(evidence_service_due=True), "evidence")

    def test_recovery_retains_priority_between_slots(self):
        self.assertEqual(self.choose(evidence_service_due=False), "backfill")

    def test_integrity_precedes_service(self):
        self.assertEqual(self.choose(evidence_service_due=True, certificate_urgent=True), "certificate")

    def test_memory_cannot_starve_overdue_evidence(self):
        self.assertEqual(self.choose(evidence_service_due=True, memory_urgent=True), "evidence")

    def test_frozen_cohort_priority_unchanged(self):
        self.assertEqual(self.choose(evidence_service_due=True, cohort_progress={
            "collecting": True, "backfill_deficit": 1}), "backfill")

    def test_position_mark_priority_unchanged(self):
        choice = rh._select_background_candidate(
            [("marks", {"next": 0}), ("evidence", {"next": 0})],
            {"live": 10, "marks": 0}, evidence_service_due=True)
        self.assertEqual(choice, "marks")

    def test_evidence_and_background_are_mutually_exclusive(self):
        for other in ("backfill", "analysis", "certificate", "memory"):
            for lane, active in (("evidence", other), (other, "evidence")):
                with self.subTest(lane=lane, active=active):
                    self.assertTrue(rh._low_priority_launch_blocked(
                        lane, {active: {}}, now=0, next_live=100))

    def test_evidence_does_not_block_live(self):
        self.assertFalse(rh._low_priority_launch_blocked("live", {"evidence": {}}, now=0, next_live=100))
        self.assertTrue(rh._low_priority_launch_blocked("evidence", {"live": {}}, now=0, next_live=100))
        self.assertTrue(rh._low_priority_launch_blocked("evidence", {}, now=0, next_live=1))

    def test_existing_worker_budget_and_window_fit_retained(self):
        self.assertEqual(rh.EVIDENCE_LANE_BUDGET_SECONDS, 90)
        self.assertFalse(rh._lane_launch_fits(89, rh.EVIDENCE_LANE_BUDGET_SECONDS))

    def test_attempt_budget_persisted_and_restored_not_completion_based(self):
        source = inspect.getsource(rh.supervise_lanes)
        self.assertIn('read_json(evidence_service_path', source)
        self.assertIn('atomic_json_write(evidence_service_path', source)
        self.assertIn('last_evidence_launch = wall_started', source)
        self.assertIn('get("started_at")', source)
        self.assertEqual(rh.EVIDENCE_RECOVERY_SERVICE_INTERVAL_SECONDS, 600)


if __name__ == "__main__":
    unittest.main()
