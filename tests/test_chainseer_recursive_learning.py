from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

import chainseer_recursive_learning as recursive


class RecursiveLearningV2Tests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        connection = sqlite3.connect(self.root / "learning.sqlite3")
        connection.executescript("""
            CREATE TABLE flow_signal_events (
                event_id TEXT PRIMARY KEY, policy_version TEXT,
                cohort_id TEXT, source_version TEXT, pool_id TEXT,
                token_address TEXT, signal_role TEXT,
                matched_signal_event_id TEXT, window_start_block INTEGER,
                window_end_block INTEGER, head_block INTEGER,
                head_lag_blocks INTEGER, freshness TEXT,
                eligible_for_evaluation INTEGER, signaled_at REAL,
                snapshot_json TEXT, quote_status TEXT, quote_block INTEGER,
                quote_json TEXT, quote_verified INTEGER,
                quote_exitable INTEGER, quote_attempts INTEGER,
                quote_retry_after REAL, quote_failure_class TEXT,
                quote_last_error TEXT, created_at TEXT
            );
            CREATE TABLE flow_signal_outcomes (
                event_id TEXT, horizon_label TEXT, horizon_seconds INTEGER,
                target_at REAL, status TEXT, observed_at REAL,
                quote_block INTEGER, quote_json TEXT, quote_verified INTEGER,
                exit_valid INTEGER, net_return REAL,
                maximum_favorable_excursion REAL,
                maximum_adverse_excursion REAL, liquidity_usd REAL,
                error TEXT, PRIMARY KEY(event_id,horizon_label)
            );
            -- V1's broad corpus is deliberately present and must be ignored.
            CREATE TABLE flow_observations (
                observation_id TEXT PRIMARY KEY, pool_id TEXT,
                observed_at_epoch REAL, features_json TEXT
            );
        """)
        connection.execute(
            "INSERT INTO flow_observations VALUES (?,?,?,?)",
            ("wrong-corpus", "wrong-pool", 9_999.0, "{}"),
        )
        for index in range(1_000):
            signal = f"signal-{index:04d}"
            control = f"control-{index:04d}"
            snapshot = self._snapshot(index)
            quote = self._quote(index)
            self._insert_event(
                connection, signal, f"pool-{index:04d}", "qualified",
                None, 1_000.0 + index, snapshot, quote,
            )
            self._insert_event(
                connection, control, f"control-pool-{index:04d}",
                "matched_control", signal, 1_000.0 + index, snapshot, quote,
            )
            # Signal-time pressure is modestly predictive, while periodic
            # disappearance ensures the learner cannot ignore tail risk.
            signal_exit = index % 20 != 0
            signal_return = (
                0.20 if index % 10 >= 4 else -0.08
            ) if signal_exit else -1.0
            self._insert_outcome(
                connection, signal, signal_return, signal_exit)
            self._insert_outcome(connection, control, -0.10, True)
        connection.commit()
        connection.close()

    @staticmethod
    def _snapshot(index: int) -> str:
        identity = 1.0
        return json.dumps({
            "swap_count": 6 + index % 10,
            "buy_ratio": 0.55 + (index % 10) / 25,
            "identity_coverage": identity,
            "net_anchor_flow_fraction": 0.2 + (index % 8) / 10,
            "price_multiple": 0.98 + (index % 7) / 100,
            "uncapped_shadow_score": 70.0 + index % 25,
            "unique_resolved_participants": 4 + index % 8,
            "qualification_gaps": [],
            "policy": {"minimum_identity_coverage": 0.8},
            "features": {
                "identity_unverified": False,
                "maximum_participant_share": 0.15 + (index % 4) / 20,
                "velocity_quality": (index % 10) / 10,
                "participant_quality": 0.8,
                "pressure_quality": (index % 10) / 10,
            },
        })

    @staticmethod
    def _quote(index: int) -> str:
        return json.dumps({"market": {
            "estimated_liquidity_usd": 20_000.0 + index,
            "market_cap_usd": 400_000.0 + index * 10,
            "singleton_token_balance_fraction": 0.10,
            "execution_quote": {
                "round_trip_loss_fraction": 0.04,
                "buy_price_impact_fraction": 0.015,
                "pool_key": {"fee": 3_000},
            },
        }})

    @staticmethod
    def _insert_event(
        connection: sqlite3.Connection, event_id: str, pool_id: str,
        role: str, matched: str | None, signaled_at: float,
        snapshot: str, quote: str,
    ) -> None:
        connection.execute(
            """INSERT INTO flow_signal_events
               (event_id,policy_version,cohort_id,source_version,pool_id,
                token_address,signal_role,matched_signal_event_id,
                window_start_block,window_end_block,head_block,
                head_lag_blocks,freshness,eligible_for_evaluation,
                signaled_at,snapshot_json,quote_status,quote_block,quote_json,
                quote_verified,quote_exitable,quote_attempts,
                quote_retry_after,quote_failure_class,quote_last_error,created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                event_id, "flow-evidence-test", "cohort-test",
                "uniswap_v3", pool_id, "0x" + "1" * 40, role, matched,
                1, 2, 2, 0, "fresh", 1, signaled_at, snapshot, "verified",
                2, quote, 1, 1, 1, None, None, None,
                "2026-01-01T00:00:00+00:00",
            ),
        )

    @staticmethod
    def _insert_outcome(
        connection: sqlite3.Connection, event_id: str,
        net_return: float, exit_valid: bool,
    ) -> None:
        connection.execute(
            """INSERT INTO flow_signal_outcomes
               (event_id,horizon_label,horizon_seconds,target_at,status,
                observed_at,quote_block,quote_json,quote_verified,exit_valid,
                net_return,maximum_favorable_excursion,
                maximum_adverse_excursion,liquidity_usd,error)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                event_id, "15m", 900, 2_000.0, "observed", 2_001.0, 3,
                "{}", 1, int(exit_valid), net_return, None, None, 10_000.0,
                None,
            ),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def _run(self, permitted: bool = True) -> dict:
        return recursive.run_shadow_learning(
            self.root,
            source_policy_version="operational-test",
            source_revision="revision-test",
            source_digest="source-test",
            source_permissions={"uniswap_v3": permitted},
            operational_evidence={
                "stabilized": True, "cohort_id": "acceptance-test",
                "revision": "revision-test", "policy_hash": "policy-test",
                "criteria_passed": 13, "criteria_total": 13,
            },
        )

    def test_shadow_run_never_enables_or_changes_policy(self):
        report = self._run()
        governance = report["governance"]
        self.assertFalse(governance["promotion_enabled"])
        self.assertFalse(governance["promotion_eligible"])
        self.assertFalse(governance["active_policy_changed"])
        self.assertFalse(governance["single_trade_updates_allowed"])
        self.assertFalse(governance["live_execution_path_present"])
        self.assertEqual(report["source"]["source_digest"], "source-test")
        self.assertEqual(len(report["source"]["learner_digest"]), 64)

    def test_uses_signal_event_corpus_and_ignores_observation_rows(self):
        report = self._run()
        self.assertEqual(
            report["population_kind"], "prospective_flow_signal_events")
        self.assertEqual(
            report["data_quality"]["source_rows_with_complete_entry_features"],
            1_000,
        )
        self.assertGreaterEqual(
            report["data_quality"]["marketable_train_rows"],
            recursive.MINIMUM_TRAIN_ROWS,
        )
        self.assertGreaterEqual(
            report["data_quality"]["marketable_holdout_rows"],
            recursive.MINIMUM_HOLDOUT_ROWS,
        )

    def test_digest_is_idempotent_and_hash_chain_is_append_only(self):
        first = self._run()
        second = self._run()
        self.assertEqual(
            first["source"]["evidence_hash"],
            second["source"]["evidence_hash"],
        )
        ledger = sqlite3.connect(self.root / recursive.LEDGER_NAME)
        self.assertEqual(ledger.execute(
            "SELECT COUNT(*) FROM shadow_learning_runs").fetchone()[0], 1)
        with self.assertRaises(sqlite3.IntegrityError):
            ledger.execute(
                "UPDATE shadow_learning_runs SET status='tampered'")
        ledger.close()
        self.assertTrue(recursive.verify_shadow_ledger(self.root)["ok"])

    def test_source_permission_separates_marketability_from_executability(self):
        blocked = self._run(permitted=False)
        self.assertGreater(
            blocked["challenger"]["marketable_holdout_metrics"]["samples"], 0)
        self.assertEqual(
            blocked["challenger"][
                "capital_executable_holdout_metrics"]["samples"], 0)
        self.assertFalse(
            blocked["execution_boundary"]["all_selected_sources_admitted"])
        permitted = self._run(permitted=True)
        self.assertGreater(
            permitted["challenger"][
                "capital_executable_holdout_metrics"]["samples"], 0)

    def test_temporal_split_keeps_past_and_excludes_future_crossing_pool(self):
        rows = [
            {"event_id": f"e-{i}", "pool_id": f"pool-{i}",
             "signaled_at": float(i)}
            for i in range(10)
        ]
        rows[8]["pool_id"] = rows[0]["pool_id"]
        train, holdout, detail = recursive._temporal_split(rows)
        self.assertIn(rows[0], train)
        self.assertNotIn(rows[8], holdout)
        self.assertEqual(detail["crossing_pool_future_rows_excluded"], 1)
        self.assertFalse({r["pool_id"] for r in train} &
                         {r["pool_id"] for r in holdout})

    def test_threshold_selection_has_no_holdout_argument_or_dependency(self):
        rows, _ = recursive._load_rows(
            self.root / "learning.sqlite3", {"uniswap_v3": True})
        train, holdout, _ = recursive._temporal_split(
            recursive._deduplicate_windows(rows))
        train = [row for row in train if row["entry_marketable"]]
        model = recursive._fit(train)
        first = recursive._choose_threshold(train, model)
        for row in holdout:
            row["target_return"] = 3.0
            row["features"] = {name: -999.0 for name in recursive.FEATURES}
        second = recursive._choose_threshold(train, model)
        self.assertEqual(first, second)

    def test_malformed_quote_is_excluded_not_imputed(self):
        connection = sqlite3.connect(self.root / "learning.sqlite3")
        self._insert_event(
            connection, "signal-bad", "pool-bad", "qualified", None,
            3_000.0, self._snapshot(0), "{}",
        )
        self._insert_outcome(connection, "signal-bad", 2.0, True)
        connection.commit()
        connection.close()
        report = self._run()
        self.assertEqual(
            report["data_quality"]["excluded"][
                "incomplete_entry_snapshot_or_quote"], 1)
        self.assertEqual(
            report["data_quality"]["source_rows_with_complete_entry_features"],
            1_000,
        )

    def test_new_pending_cohort_never_falls_back_to_completed_old_cohort(self):
        connection = sqlite3.connect(self.root / "learning.sqlite3")
        self._insert_event(
            connection, "signal-new", "pool-new", "qualified", None,
            5_000.0, self._snapshot(0), self._quote(0),
        )
        connection.execute(
            "UPDATE flow_signal_events SET cohort_id='cohort-new' "
            "WHERE event_id='signal-new'"
        )
        connection.execute(
            """INSERT INTO flow_signal_outcomes
               (event_id,horizon_label,horizon_seconds,target_at,status,
                observed_at,quote_block,quote_json,quote_verified,exit_valid,
                net_return,maximum_favorable_excursion,
                maximum_adverse_excursion,liquidity_usd,error)
               VALUES ('signal-new','15m',900,5900,'pending',NULL,NULL,NULL,
                       0,0,NULL,NULL,NULL,NULL,NULL)"""
        )
        connection.commit()
        connection.close()
        rows, metadata = recursive._load_rows(
            self.root / "learning.sqlite3", {"uniswap_v3": True})
        self.assertEqual(rows, [])
        self.assertEqual(metadata["flow_cohort_id"], "cohort-new")
        self.assertEqual(metadata["pending_or_unpriceable_outcome"], 1)

    def test_latest_status_is_safe_and_supports_legacy_fallback(self):
        empty = self.root / "empty"
        latest = recursive.latest_shadow_learning(empty)
        self.assertEqual(latest["status"], "not_run")
        self.assertFalse(latest["promotion_enabled"])
        empty.mkdir()
        (empty / recursive.LEGACY_ARTIFACT_NAME).write_text(
            json.dumps({
                "policy_version": "recursive-learning-shadow-v1",
                "governance": {"status": "legacy", "promotion_enabled": True},
            }),
            encoding="utf-8",
        )
        legacy = recursive.latest_shadow_learning(empty)
        self.assertEqual(legacy["artifact_version"], "legacy_v1")
        self.assertFalse(legacy["promotion_enabled"])
        self.assertFalse(legacy["governance"]["promotion_enabled"])


if __name__ == "__main__":
    unittest.main()
