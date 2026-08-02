import tempfile
import unittest
from pathlib import Path

import chainseer_wallet_convergence as wc


class WalletConvergenceTrackerTests(unittest.TestCase):
    def _tracker(self, temp_dir, **kwargs):
        return wc.WalletConvergenceTracker(
            Path(temp_dir) / "wallet_convergence.json", **kwargs
        )

    def test_wallet_is_unrated_below_minimum_observations(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp, minimum_observations_for_accuracy=3)
            tracker.observe("wallet-a", "mint-1", evidence_state="complete_safe")
            tracker.observe("wallet-a", "mint-2", evidence_state="complete_safe")

            acc = tracker.wallet_accuracy("wallet-a")

            self.assertFalse(acc["rated"])
            self.assertEqual(acc["label"], "unrated")
            self.assertEqual(acc["observations"], 2)

    def test_wallet_becomes_historically_accurate_once_hit_rate_clears_floor(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(
                temp, minimum_observations_for_accuracy=3, minimum_accuracy_pct=50.0
            )
            tracker.observe("wallet-a", "mint-1", evidence_state="complete_safe")
            tracker.observe("wallet-a", "mint-2", evidence_state="complete_safe")
            tracker.observe("wallet-a", "mint-3", evidence_state="hard_stop_failed")

            acc = tracker.wallet_accuracy("wallet-a")

            self.assertTrue(acc["rated"])
            self.assertEqual(acc["observations"], 3)
            self.assertEqual(acc["hits"], 2)
            self.assertAlmostEqual(acc["hit_rate_pct"], 66.7, places=1)
            self.assertEqual(acc["label"], "historically_accurate")

    def test_wallet_with_low_hit_rate_is_historically_inaccurate(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(
                temp, minimum_observations_for_accuracy=3, minimum_accuracy_pct=50.0
            )
            tracker.observe("wallet-a", "mint-1", evidence_state="hard_stop_failed")
            tracker.observe("wallet-a", "mint-2", evidence_state="hard_stop_failed")
            tracker.observe("wallet-a", "mint-3", evidence_state="complete_safe")

            acc = tracker.wallet_accuracy("wallet-a")

            self.assertTrue(acc["rated"])
            self.assertEqual(acc["label"], "historically_inaccurate")

    def test_prospective_enrollment_records_unknown_outcome(self):
        """A wallet is enrolled the first time it's SEEN holding a token, before
        that token's outcome is known -- this is what keeps the dataset free of
        winner-only selection bias."""
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp)
            tracker.observe("wallet-a", "mint-1", evidence_state=None)

            acc = tracker.wallet_accuracy("wallet-a")
            self.assertEqual(acc["observations"], 0)
            positions = tracker.state["wallets"]["wallet-a"]["positions"]
            self.assertIsNone(positions["mint-1"]["evidence_state"])

    def test_regrade_updates_outcome_and_is_reflected_in_accuracy(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp, minimum_observations_for_accuracy=1)
            tracker.observe("wallet-a", "mint-1", evidence_state=None)
            self.assertEqual(tracker.wallet_accuracy("wallet-a")["observations"], 0)

            updated = tracker.regrade("mint-1", "complete_safe")

            self.assertEqual(updated, 1)
            acc = tracker.wallet_accuracy("wallet-a")
            self.assertEqual(acc["observations"], 1)
            self.assertEqual(acc["hits"], 1)

            # A no-op regrade to the same state must not report an update.
            self.assertEqual(tracker.regrade("mint-1", "complete_safe"), 0)

    def test_regrade_only_touches_wallets_holding_that_mint(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp)
            tracker.observe("wallet-a", "mint-1", evidence_state=None)
            tracker.observe("wallet-b", "mint-2", evidence_state=None)

            updated = tracker.regrade("mint-1", "hard_stop_failed")

            self.assertEqual(updated, 1)
            self.assertEqual(
                tracker.state["wallets"]["wallet-b"]["positions"]["mint-2"][
                    "evidence_state"
                ],
                None,
            )

    def test_convergence_for_requires_minimum_accurate_wallets(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(
                temp,
                minimum_observations_for_accuracy=1,
                convergence_minimum_accurate_wallets=2,
            )
            for wallet in ("wallet-a", "wallet-b", "wallet-c"):
                tracker.observe(wallet, f"prior-{wallet}", evidence_state=None)
                tracker.regrade(f"prior-{wallet}", "complete_safe")

            # Only one accurate wallet holds this candidate token.
            single = tracker.convergence_for([{"owner": "wallet-a"}])
            self.assertFalse(single["converged"])
            self.assertEqual(single["accurate_wallets_holding"], 1)

            # Two accurate wallets holding it clears the floor.
            double = tracker.convergence_for(
                [{"owner": "wallet-a"}, {"owner": "wallet-b"}, {"owner": "unrated"}]
            )
            self.assertTrue(double["converged"])
            self.assertEqual(double["accurate_wallets_holding"], 2)

    def test_convergence_for_ignores_holders_without_owner(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp)
            result = tracker.convergence_for([{}, {"owner": None}])
            self.assertEqual(result["accurate_wallets_holding"], 0)
            self.assertFalse(result["converged"])

    def test_snapshot_ranks_rated_wallets_by_hit_rate_then_observations(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp, minimum_observations_for_accuracy=1)
            tracker.observe("low", "mint-1", evidence_state=None)
            tracker.regrade("mint-1", "hard_stop_failed")
            tracker.observe("high", "mint-2", evidence_state=None)
            tracker.regrade("mint-2", "complete_safe")
            tracker.observe("unrated-wallet", "mint-3", evidence_state=None)

            snap = tracker.snapshot()

            self.assertEqual(snap["total_wallets_enrolled"], 3)
            self.assertEqual(snap["rated_wallets"], 2)
            self.assertEqual(snap["top_accurate_wallets"][0]["wallet"], "high")

    def test_state_persists_across_tracker_instances(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "wallet_convergence.json"
            first = wc.WalletConvergenceTracker(path, minimum_observations_for_accuracy=1)
            first.observe("wallet-a", "mint-1", evidence_state=None)
            first.regrade("mint-1", "complete_safe")

            second = wc.WalletConvergenceTracker(path, minimum_observations_for_accuracy=1)
            acc = second.wallet_accuracy("wallet-a")
            self.assertTrue(acc["rated"])
            self.assertEqual(acc["hits"], 1)


if __name__ == "__main__":
    unittest.main()
