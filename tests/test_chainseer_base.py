import json
import sqlite3
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

import chainseer_base
from chainseer_outcome_ledger import verify_outcome_rings


class FakeBaseRPC:
    def __init__(self, code="0x60006000"):
        self.code = code
        self.context = None

    def get_block_number(self):
        return 123456

    def bind_context(self, context):
        self.context = context

    def get_code(self, address):
        if self.context:
            self.context.ledger.record(
                "rpc",
                {"method": "eth_getCode", "params": [address, hex(self.context.block_pin)]},
                {"result": self.code},
            )
        return self.code

    def virtuals_pair_snapshot(self, pair_address, token_address):
        if self.context:
            self.context.ledger.record(
                "rpc",
                {"method": "eth_call", "params": [pair_address, "getReserves"]},
                {"result": "synthetic_pair_snapshot"},
            )
        return {
            "pair_address": pair_address,
            "token_a": token_address,
            "token_b": chainseer_base.VIRTUALS_TOKEN_ADDRESS,
            "binding_verified": True,
            "token_reserve_raw": 1_000_000_000 * 10**18,
            "virtual_reserve_raw": 8_500 * 10**18,
            "token_reserve": 1_000_000_000.0,
            "virtual_reserve": 8_500.0,
            "price_virtual": 0.0000085,
            "block_pin": 123456,
            "method": "bonding_v5_pair_reserve_ratio",
        }


class FakeLearningObserver:
    def __init__(self, candidate):
        self.latest = candidate
        self.current = candidate

    def fetch_launches(self, limit=20, include_sentient=False):
        return [self.latest][:limit]

    def fetch_launch_by_id(self, launch_id):
        return self.current if self.current.launch_id == launch_id else None


class FakeLearningTrader:
    def __init__(self):
        self.policy = chainseer_base.PaperPolicy(amount_virtual=1)
        self.state = {"positions": {}, "realized_virtual": 0.0}

    @property
    def open_positions(self):
        return []

    def mark(self, _token_address, _price, _decision, **_kwargs):
        return []


class FakeLearningEngine:
    def __init__(self, root, candidate):
        self.root = Path(root)
        self.observer = FakeLearningObserver(candidate)
        self.trader = FakeLearningTrader()
        self.shadow_trader = FakeLearningTrader()
        self.shadow_ledger = chainseer_base.PaperTradeLedger(
            self.root / "shadow_events.jsonl"
        )
        self.timechain = None
        self.evaluate_calls = 0
        self.evaluated_ids = []
        self.paper_position = None
        self.shadow_position = None

    def evaluate_candidate(self, candidate, **_kwargs):
        self.evaluate_calls += 1
        self.evaluated_ids.append(candidate.launch_id)
        decision = chainseer_base.BaseRiskDecision(
            token_address=candidate.token_address,
            block_pin=123,
            score=80,
            risk_level="Medium",
            paper_entry_allowed=True,
            live_entry_allowed=False,
            hard_stops=[],
            warnings=[],
            green_flags=[],
            coverage={"rpc": True},
            canonicality={"pair_binding_verified": True},
            market={},
            security={},
            provenance={"fact_count": 1},
            timechain_ring=42,
        )
        return {
            "candidate": candidate.to_dict(),
            "decision": decision.to_dict(),
            "price_virtual": 0.0000085,
            "price_source": "onchain_bonding_reserve_spot",
            "paper_price_eligible": True,
            "paper_action": "waiting_for_policy_conditions:anti_sniper_wait",
            "paper_position": self.paper_position,
            "shadow_action": "waiting_for_policy_conditions:anti_sniper_wait",
            "shadow_position": self.shadow_position,
        }


def virtuals_item(**overrides):
    item = {
        "id": 119133,
        "name": "Safe Test by Virtuals",
        "symbol": "SAFE",
        "status": "UNDERGRAD",
        "chain": "BASE",
        "tokenAddress": None,
        "preToken": "0x" + "1" * 40,
        "lpAddress": None,
        "preTokenPair": "0x" + "2" * 40,
        "holderCount": 12,
        "mcapInVirtual": 9000,
        "totalSupply": 1_000_000_000,
        "createdAt": "2026-07-21T20:00:00.000Z",
        "description": "Synthetic test candidate",
    }
    item.update(overrides)
    return item


class BasePrototypeTests(unittest.TestCase):
    def test_learning_lock_prevents_overlapping_cycles(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / ".learn_once.lock"
            with chainseer_base.LearningRunLock(path):
                with self.assertRaises(chainseer_base.LearningRunLockedError):
                    with chainseer_base.LearningRunLock(path):
                        pass
            self.assertFalse(path.exists())

    def test_learning_store_refuses_silent_policy_change(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = chainseer_base.BaseLearningStore(Path(temp_dir) / "learning.sqlite3")
            store.bind_policy(chainseer_base.PaperPolicy(amount_virtual=1))
            store.bind_policy(chainseer_base.PaperPolicy(amount_virtual=1))
            with self.assertRaisesRegex(ValueError, "different paper policy"):
                store.bind_policy(chainseer_base.PaperPolicy(amount_virtual=2))

    def test_learning_store_migrates_shadow_event_counter(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "learning.sqlite3"
            connection = sqlite3.connect(path)
            try:
                connection.execute("""
                    CREATE TABLE learning_runs (
                        run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        started_at TEXT NOT NULL,
                        completed_at TEXT,
                        status TEXT NOT NULL,
                        discovered INTEGER NOT NULL DEFAULT 0,
                        new_projects INTEGER NOT NULL DEFAULT 0,
                        outcomes INTEGER NOT NULL DEFAULT 0,
                        migrations INTEGER NOT NULL DEFAULT 0,
                        paper_events INTEGER NOT NULL DEFAULT 0,
                        errors_json TEXT
                    )
                """)
                connection.commit()
            finally:
                connection.close()

            store = chainseer_base.BaseLearningStore(path)
            with store._connect() as connection:
                columns = {
                    row[1] for row in connection.execute(
                        "PRAGMA table_info(learning_runs)"
                    )
                }
            self.assertIn("shadow_events", columns)

            run_id = store.start_run()
            counters = {
                "discovered": 1,
                "new_projects": 1,
                "outcomes": 0,
                "migrations": 0,
                "paper_events": 0,
                "shadow_events": 1,
            }
            store.finish_run(run_id, "complete", counters, [])
            with store._connect() as connection:
                row = connection.execute(
                    "SELECT shadow_events FROM learning_runs WHERE run_id=?",
                    (run_id,),
                ).fetchone()
            self.assertEqual(row["shadow_events"], 1)

    def test_learning_timechain_seals_are_idempotent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            recorder = chainseer_base.BaseTimechainRecorder(temp_dir)
            candidate = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
            decision = chainseer_base.BaseRiskDecision(
                token_address=candidate.token_address,
                block_pin=123,
                score=80,
                risk_level="Medium",
                paper_entry_allowed=True,
                live_entry_allowed=False,
                hard_stops=[],
                warnings=["synthetic evidence fixture"],
                green_flags=["pair binding verified"],
                coverage={"rpc": True, "bonding_pair": True},
                canonicality={"pair_binding_verified": True},
                market={"bonding_spot": {"price_virtual": 0.0000085}},
                security={},
                provenance={"fact_count": 1, "facts": [{"fact_id": "F0000"}]},
            )
            analysis_key = "base:project:119133:prediction:v1"
            analysis_ring = recorder.seal_analysis(candidate, decision, analysis_key)
            self.assertEqual(
                recorder.seal_analysis(candidate, decision, analysis_key), analysis_ring
            )

            outcome = {
                "price_source": "onchain_bonding_reserve_spot",
                "price_return_pct": 2.5,
            }
            outcome_key = "base:project:119133:outcome:5m:v1"
            outcome_ring = recorder.seal_outcome(
                candidate.launch_id, candidate, decision, analysis_ring,
                "5m", outcome, outcome_key,
            )
            self.assertEqual(
                recorder.seal_outcome(
                    candidate.launch_id, candidate, decision, analysis_ring,
                    "5m", outcome, outcome_key,
                ),
                outcome_ring,
            )

            graduated = chainseer_base.VirtualsBaseObserver.normalize(
                virtuals_item(
                    status="AVAILABLE",
                    tokenAddress="0x" + "4" * 40,
                    lpAddress="0x" + "5" * 40,
                )
            )
            migration_key = "base:project:119133:migration:" + graduated.token_address.lower()
            migration_ring = recorder.seal_migration(
                candidate.launch_id, candidate, graduated, analysis_ring,
                decision, migration_key,
            )
            self.assertEqual(
                recorder.seal_migration(
                    candidate.launch_id, candidate, graduated, analysis_ring,
                    decision, migration_key,
                ),
                migration_ring,
            )
            self.assertLess(analysis_ring, outcome_ring)
            self.assertLess(outcome_ring, migration_ring)
            ledger_status = verify_outcome_rings(recorder.tc.load())
            self.assertTrue(ledger_status["ok"])
            self.assertEqual(ledger_status["checked"], 1)
            ok, _ = recorder.tc.verify()
            self.assertTrue(ok)

    def test_learn_once_deduplicates_and_tracks_graduation_outcome(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            prototype = chainseer_base.VirtualsBaseObserver.normalize(
                virtuals_item(launchInfo={"antiSniperTaxType": 1})
            )
            engine = FakeLearningEngine(temp_dir, prototype)
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("10m", 600),))
            first = loop.run_once(limit=1, now=1_000_000)
            self.assertEqual(first["cycle"]["new_projects"], 1)
            self.assertEqual(first["cycle"]["outcomes"], 0)

            graduated = chainseer_base.VirtualsBaseObserver.normalize(
                virtuals_item(
                    status="AVAILABLE",
                    tokenAddress="0x" + "4" * 40,
                    lpAddress="0x" + "5" * 40,
                )
            )
            engine.observer.current = graduated
            second = loop.run_once(limit=1, now=1_000_300)
            self.assertEqual(second["cycle"]["new_projects"], 0)
            self.assertEqual(second["cycle"]["migrations"], 1)
            self.assertEqual(second["cycle"]["refreshed"], 1)

            third = loop.run_once(limit=1, now=1_000_600)
            self.assertEqual(third["cycle"]["new_projects"], 0)
            self.assertEqual(third["cycle"]["migrations"], 0)
            self.assertEqual(third["cycle"]["outcomes"], 1)
            self.assertEqual(third["checkpoints_complete"], 1)
            self.assertEqual(third["migrations"], 1)

            with loop.store._connect() as connection:
                checkpoint = connection.execute(
                    "SELECT * FROM checkpoints WHERE project_id=? AND horizon='10m'",
                    (prototype.launch_id,),
                ).fetchone()
            self.assertEqual(checkpoint["status"], "complete")
            self.assertEqual(checkpoint["comparable_price"], 0)
            self.assertIsNone(checkpoint["price_return_pct"])
            ok, report = loop.store.verify()
            self.assertTrue(ok, report)

    def test_learn_once_counts_paper_buys_on_discovery_and_refresh(self):
        candidate = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
        paper_position = {
            "token_address": candidate.token_address,
            "status": "open",
            "cost_virtual": 1.0,
        }

        with self.subTest("new project entry"), tempfile.TemporaryDirectory() as temp_dir:
            engine = FakeLearningEngine(temp_dir, candidate)
            engine.paper_position = paper_position
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("1h", 3600),))

            summary = loop.run_once(limit=1, now=1_000_000)

            self.assertEqual(summary["cycle"]["new_projects"], 1)
            self.assertEqual(summary["cycle"]["paper_events"], 1)

        with self.subTest("delayed refresh entry"), tempfile.TemporaryDirectory() as temp_dir:
            engine = FakeLearningEngine(temp_dir, candidate)
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("1h", 3600),))
            first = loop.run_once(limit=1, now=1_000_000)
            self.assertEqual(first["cycle"]["paper_events"], 0)

            engine.paper_position = paper_position
            second = loop.run_once(limit=1, now=1_000_300)

            self.assertEqual(second["cycle"]["new_projects"], 0)
            self.assertEqual(second["cycle"]["paper_events"], 1)

    def test_learn_once_counts_shadow_buys_on_discovery_and_refresh(self):
        candidate = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
        shadow_position = {
            "token_address": candidate.token_address,
            "status": "open",
            "cost_virtual": 1.0,
            "simulation": "shadow",
        }

        with self.subTest("new project entry"), tempfile.TemporaryDirectory() as temp_dir:
            engine = FakeLearningEngine(temp_dir, candidate)
            engine.shadow_position = shadow_position
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("1h", 3600),))

            summary = loop.run_once(limit=1, now=1_000_000)

            self.assertEqual(summary["cycle"]["shadow_events"], 1)
            self.assertIn("shadow_performance", summary)
            self.assertTrue((Path(temp_dir) / "shadow_performance.json").exists())

        with self.subTest("delayed refresh entry"), tempfile.TemporaryDirectory() as temp_dir:
            engine = FakeLearningEngine(temp_dir, candidate)
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("1h", 3600),))
            first = loop.run_once(limit=1, now=1_000_000)
            self.assertEqual(first["cycle"]["shadow_events"], 0)

            engine.shadow_position = shadow_position
            second = loop.run_once(limit=1, now=1_000_300)

            self.assertEqual(second["cycle"]["shadow_events"], 1)

    def test_shadow_performance_summary_is_friction_aware_and_evidence_labeled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            now = time.time()
            created_at = datetime.fromtimestamp(
                now - 3600, tz=timezone.utc
            ).isoformat()
            policy = chainseer_base.PaperPolicy(
                amount_virtual=1,
                observation_seconds=0,
            )
            ledger = chainseer_base.PaperTradeLedger(root / "shadow_events.jsonl")
            trader = chainseer_base.BasePaperTrader(
                root / "shadow_state.json",
                ledger,
                policy,
                event_namespace="shadow",
                enforce_position_limit=False,
            )
            store = chainseer_base.BaseLearningStore(root / "learning.sqlite3")

            candidates = [
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(createdAt=created_at)
                ),
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(
                        id=119134,
                        createdAt=created_at,
                        preToken="0x" + "3" * 40,
                        preTokenPair="0x" + "4" * 40,
                    )
                ),
            ]

            def decision(candidate, verified):
                return chainseer_base.BaseRiskDecision(
                    token_address=candidate.token_address,
                    block_pin=123,
                    score=85 if verified else 75,
                    risk_level="Low" if verified else "Medium",
                    paper_entry_allowed=True,
                    live_entry_allowed=False,
                    hard_stops=[],
                    warnings=[],
                    green_flags=[],
                    coverage={"blockscout": True},
                    canonicality={"pair_binding_verified": True},
                    market={},
                    security={"source_verified": verified},
                    provenance={"fact_count": 1},
                    timechain_ring=42 if verified else 43,
                )

            decisions = [decision(candidates[0], True), decision(candidates[1], False)]
            for candidate, item_decision in zip(candidates, decisions):
                result = {
                    "decision": item_decision.to_dict(),
                    "price_virtual": 1.0,
                    "price_source": "onchain_bonding_reserve_spot",
                }
                self.assertTrue(
                    store.add_project(candidate, result, now, (("5m", 300),))
                )
                self.assertIsNotNone(
                    trader.enter(
                        candidate,
                        item_decision,
                        1.0,
                        now=now,
                        price_source="onchain_bonding_reserve_spot",
                    )
                )

            first_position = trader.state["positions"][
                candidates[0].token_address.lower()
            ]
            first_position.pop("last_mark_price_virtual")
            first_position.pop("last_mark_price_source")
            first_position.pop("last_mark_timestamp")
            trader._save()
            store.complete_checkpoint(
                candidates[0].launch_id,
                "5m",
                {
                    "observed_at": datetime.fromtimestamp(
                        now + 100, tz=timezone.utc
                    ).isoformat(),
                    "lifecycle": candidates[0].lifecycle,
                    "token_address": candidates[0].token_address,
                    "price_virtual": 1.5,
                    "price_source": "onchain_bonding_reserve_spot",
                    "price_return_pct": 50.0,
                    "comparable_price": True,
                    "risk_level": "Low",
                    "score": 85,
                    "hard_stops": [],
                    "warnings": [],
                },
                44,
            )
            exits = trader.mark(
                candidates[1].token_address,
                0.5,
                decisions[1],
                now=now + 120,
                price_source="onchain_bonding_reserve_spot",
            )
            self.assertEqual(exits[0]["payload"]["reason"], "stop_loss")

            report = chainseer_base.ShadowPerformanceReporter(
                trader, ledger, store
            ).build(now=now + 200)

            self.assertEqual(report["accounting"]["positions_opened"], 2)
            self.assertEqual(report["accounting"]["positions_open"], 1)
            self.assertEqual(report["accounting"]["positions_closed"], 1)
            self.assertEqual(report["accounting"]["closed_losses"], 1)
            self.assertEqual(report["accounting"]["closed_win_rate_pct"], 0.0)
            self.assertEqual(
                report["accounting"]["weighted_gross_price_return_pct_open"],
                50.0,
            )
            self.assertLess(
                report["friction_baselines"][
                    "prototype_flat_price_round_trip_return_pct"
                ],
                0,
            )
            self.assertEqual(report["exit_reasons"]["stop_loss"]["events"], 1)
            self.assertEqual(report["coverage"]["tracked_project_coverage_pct"], 100.0)
            self.assertAlmostEqual(
                report["coverage"]["median_open_mark_age_seconds"], 100.0, places=5
            )
            self.assertAlmostEqual(
                report["coverage"]["p95_open_mark_age_seconds"], 100.0, places=5
            )
            self.assertAlmostEqual(
                report["coverage"]["oldest_open_mark_age_seconds"], 100.0, places=5
            )
            self.assertEqual(
                report["performance_by_source_verification"]["verified"]["positions"],
                1,
            )
            self.assertEqual(
                report["performance_by_source_verification"]["unverified"]["positions"],
                1,
            )
            open_mark = next(
                row for row in report["position_marks"] if row["status"] == "open"
            )
            self.assertEqual(open_mark["mark_origin"], "learning_checkpoint")
            self.assertLess(
                open_mark["modeled_open_liquidation_virtual"],
                open_mark["remaining_quantity"] * open_mark["mark_price_virtual"],
            )
            self.assertTrue(report["data_quality"]["shadow_ledger_verified"])
            self.assertEqual(report["review_readiness"]["status"], "collecting")

    def test_learn_once_keeps_existing_projects_on_refresh_cadence(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            candidate = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
            engine = FakeLearningEngine(temp_dir, candidate)
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("1h", 3600),))

            first = loop.run_once(limit=1, now=1_000_000)
            early = loop.run_once(limit=1, now=1_000_299)
            due = loop.run_once(limit=1, now=1_000_300)

            self.assertEqual(first["cycle"]["new_projects"], 1)
            self.assertEqual(early["cycle"]["refreshed"], 0)
            self.assertEqual(due["cycle"]["refreshed"], 1)
            self.assertEqual(engine.evaluate_calls, 2)

    def test_learn_once_bounds_slow_refreshes_per_cycle(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            candidates = [
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(
                        id=119200 + index,
                        preToken="0x" + format(index + 1, "x") * 40,
                        preTokenPair="0x" + format(index + 4, "x") * 40,
                    )
                )
                for index in range(3)
            ]
            engine = FakeLearningEngine(temp_dir, candidates[0])
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("1h", 3600),))
            lookup = {candidate.launch_id: candidate for candidate in candidates}

            for candidate in candidates:
                result = engine.evaluate_candidate(candidate)
                self.assertTrue(
                    loop.store.add_project(
                        candidate, result, 1_000_000, (("1h", 3600),)
                    )
                )

            engine.evaluate_calls = 0
            engine.observer.fetch_launches = lambda **_kwargs: []
            engine.observer.fetch_launch_by_id = lookup.get

            summary = loop.run_once(
                limit=5, refresh_limit=2, now=1_000_300
            )

            self.assertEqual(summary["cycle"]["refreshed"], 2)
            self.assertEqual(engine.evaluate_calls, 2)
            self.assertEqual(len(loop.store.due_project_ids(1_000_300, 10)), 1)

    def test_due_project_ids_adds_deduped_priority_lane_without_starving_checkpoints(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_now = 1_000_000
            candidates = [
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(
                        id=119300 + index,
                        preToken="0x" + format(index + 1, "x") * 40,
                        preTokenPair="0x" + format(index + 6, "x") * 40,
                    )
                )
                for index in range(5)
            ]
            engine = FakeLearningEngine(temp_dir, candidates[0])
            loop = chainseer_base.BaseLearningLoop(engine)
            for index, candidate in enumerate(candidates):
                result = engine.evaluate_candidate(candidate)
                horizons = (("due", 300),) if index < 2 else (("later", 3600),)
                self.assertTrue(
                    loop.store.add_project(candidate, result, base_now, horizons)
                )

            planned = loop.store.due_project_ids(
                base_now + 300,
                2,
                priority_project_ids=[
                    candidates[1].launch_id,
                    candidates[2].launch_id,
                    candidates[3].launch_id,
                ],
                priority_target=3,
            )

            self.assertEqual(
                set(planned[:2]),
                {candidates[0].launch_id, candidates[1].launch_id},
            )
            self.assertEqual(len(planned), 4)
            self.assertEqual(len(planned), len(set(planned)))
            self.assertTrue({
                candidates[1].launch_id,
                candidates[2].launch_id,
                candidates[3].launch_id,
            }.issubset(planned))

    def test_shadow_refresh_quota_is_dynamic_and_safety_capped(self):
        self.assertEqual(
            chainseer_base.BaseLearningLoop._shadow_refresh_quota(46),
            (4, 4, 0),
        )
        self.assertEqual(
            chainseer_base.BaseLearningLoop._shadow_refresh_quota(75),
            (5, 4, 1),
        )
        self.assertEqual(
            chainseer_base.BaseLearningLoop._shadow_refresh_quota(46, 2),
            (4, 2, 2),
        )

    def test_shadow_refresh_order_uses_newest_checkpoint_for_legacy_positions(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_now = 1_000_000
            candidates = [
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item()
                ),
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(
                        id=119134,
                        preToken="0x" + "3" * 40,
                        preTokenPair="0x" + "4" * 40,
                    )
                ),
            ]
            engine = FakeLearningEngine(temp_dir, candidates[0])
            loop = chainseer_base.BaseLearningLoop(engine, horizons=(("mark", 1),))
            for index, candidate in enumerate(candidates):
                result = engine.evaluate_candidate(candidate)
                self.assertTrue(
                    loop.store.add_project(
                        candidate, result, base_now, (("mark", 1),)
                    )
                )
                engine.shadow_trader.state["positions"][
                    candidate.token_address.lower()
                ] = {
                    "launch_id": candidate.launch_id,
                    "token_address": candidate.token_address,
                    "status": "open",
                    "entry_timestamp": base_now - 500,
                }
                loop.store.complete_checkpoint(
                    candidate.launch_id,
                    "mark",
                    {
                        "observed_at": datetime.fromtimestamp(
                            base_now + 100 + index * 100, tz=timezone.utc
                        ).isoformat(),
                        "lifecycle": candidate.lifecycle,
                        "token_address": candidate.token_address,
                        "price_virtual": 1.0,
                        "price_source": "synthetic",
                        "price_return_pct": 0.0,
                        "comparable_price": True,
                        "risk_level": "Medium",
                        "score": 80,
                        "hard_stops": [],
                        "warnings": [],
                    },
                    None,
                )

            ordered = loop._open_shadow_project_ids_oldest_first()

            self.assertEqual(
                ordered,
                [candidates[0].launch_id, candidates[1].launch_id],
            )

    def test_learn_once_refreshes_oldest_shadow_marks_in_an_additive_lane(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_now = 1_000_000
            candidates = [
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(
                        id=119400 + index,
                        preToken="0x" + f"{index + 1:040x}",
                        preTokenPair="0x" + f"{index + 101:040x}",
                    )
                )
                for index in range(20)
            ]
            engine = FakeLearningEngine(temp_dir, candidates[0])
            loop = chainseer_base.BaseLearningLoop(
                engine, horizons=(("later", 3600),)
            )
            lookup = {candidate.launch_id: candidate for candidate in candidates}
            for index, candidate in enumerate(candidates):
                result = engine.evaluate_candidate(candidate)
                self.assertTrue(
                    loop.store.add_project(
                        candidate, result, base_now, (("later", 3600),)
                    )
                )
                engine.shadow_trader.state["positions"][
                    candidate.token_address.lower()
                ] = {
                    "launch_id": candidate.launch_id,
                    "token_address": candidate.token_address,
                    "symbol": candidate.symbol,
                    "status": "open",
                    "entry_timestamp": base_now - 10_000,
                    "last_mark_timestamp": base_now - (index + 1) * 100,
                }

            engine.evaluate_calls = 0
            engine.evaluated_ids = []
            engine.observer.fetch_launches = lambda **_kwargs: []
            engine.observer.fetch_launch_by_id = lookup.get

            summary = loop.run_once(
                limit=5,
                refresh_limit=1,
                now=base_now + 300,
            )

            self.assertEqual(engine.evaluated_ids[0], candidates[-1].launch_id)
            self.assertEqual(engine.evaluated_ids[1], candidates[0].launch_id)
            self.assertEqual(summary["cycle"]["shadow_refresh_required"], 2)
            self.assertEqual(summary["cycle"]["shadow_refresh_target"], 2)
            self.assertEqual(summary["cycle"]["refreshed"], 2)
            self.assertEqual(summary["cycle"]["shadow_refreshed"], 2)
            self.assertEqual(summary["cycle"]["shadow_refresh_selected"], 2)
            self.assertEqual(
                summary["cycle"]["shadow_refresh_capacity_shortfall"], 0
            )
            self.assertEqual(
                summary["cycle"]["shadow_refresh_completion_shortfall"], 0
            )
            self.assertEqual(summary["refresh_policy"]["shadow_selected"], 2)
            self.assertEqual(summary["refresh_policy"]["shadow_completed"], 2)
            self.assertEqual(summary["refresh_policy"]["regular_limit"], 1)

    def test_adaptive_headroom_adds_one_slot_for_a_projected_deficit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            base_now = 1_000_000
            candidates = [
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(
                        id=121000 + index,
                        preToken="0x" + f"{index + 1:040x}",
                        preTokenPair="0x" + f"{index + 1001:040x}",
                    )
                )
                for index in range(98)
            ]
            engine = FakeLearningEngine(root, candidates[0])
            loop = chainseer_base.BaseLearningLoop(
                engine, horizons=(("later", 3600),)
            )
            lookup = {candidate.launch_id: candidate for candidate in candidates}
            for index, candidate in enumerate(candidates):
                result = engine.evaluate_candidate(candidate)
                self.assertTrue(
                    loop.store.add_project(
                        candidate, result, base_now, (("later", 3600),)
                    )
                )
                if index > 0:
                    engine.shadow_trader.state["positions"][
                        candidate.token_address.lower()
                    ] = {
                        "launch_id": candidate.launch_id,
                        "token_address": candidate.token_address,
                        "symbol": candidate.symbol,
                        "status": "open",
                        "entry_timestamp": base_now - 10_000,
                        "last_mark_timestamp": base_now - index * 100,
                    }

            chainseer_base._atomic_json(
                root / "learning_summary.json",
                {"cycle": {"duration_seconds": 50.0, "errors": []}},
            )
            engine.evaluate_calls = 0
            engine.evaluated_ids = []
            engine.observer.fetch_launches = lambda **_kwargs: []
            engine.observer.fetch_launch_by_id = lookup.get

            summary = loop.run_once(
                limit=5,
                refresh_limit=3,
                now=base_now + 300,
            )

            cycle = summary["cycle"]
            self.assertEqual(cycle["shadow_refresh_required"], 7)
            self.assertEqual(cycle["shadow_refresh_baseline_selected"], 6)
            self.assertEqual(cycle["shadow_refresh_baseline_shortfall"], 1)
            self.assertTrue(cycle["adaptive_headroom_eligible"])
            self.assertTrue(cycle["adaptive_headroom_used"])
            self.assertEqual(cycle["adaptive_headroom_added_refreshes"], 1)
            self.assertEqual(cycle["shadow_refresh_target"], 5)
            self.assertEqual(cycle["refreshed"], 8)
            self.assertEqual(cycle["shadow_refresh_selected"], 7)
            self.assertEqual(cycle["shadow_refreshed"], 7)
            self.assertEqual(cycle["shadow_refresh_completion_shortfall"], 0)

    def test_adaptive_headroom_requires_a_clean_runtime_margin(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            candidate = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
            loop = chainseer_base.BaseLearningLoop(
                FakeLearningEngine(root, candidate)
            )

            self.assertEqual(
                loop._adaptive_headroom_safety()[:2],
                (False, "no_previous_cycle_runtime"),
            )
            chainseer_base._atomic_json(
                root / "learning_summary.json",
                {"cycle": {"duration_seconds": 76.0, "errors": []}},
            )
            self.assertEqual(
                loop._adaptive_headroom_safety()[:2],
                (False, "previous_cycle_runtime_guard"),
            )
            chainseer_base._atomic_json(
                root / "learning_summary.json",
                {"cycle": {"duration_seconds": 50.0, "errors": ["api"]}},
            )
            self.assertEqual(
                loop._adaptive_headroom_safety()[:2],
                (False, "previous_cycle_had_errors"),
            )
            chainseer_base._atomic_json(
                root / "learning_summary.json",
                {"cycle": {"duration_seconds": 50.0, "errors": []}},
            )
            self.assertEqual(
                loop._adaptive_headroom_safety(),
                (True, "previous_cycle_within_runtime_guard", 50.0),
            )

    def test_regular_lane_overlap_satisfies_shadow_freshness_capacity(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base_now = 1_000_000
            candidates = [
                chainseer_base.VirtualsBaseObserver.normalize(
                    virtuals_item(
                        id=120000 + index,
                        preToken="0x" + f"{index + 1:040x}",
                        preTokenPair="0x" + f"{index + 1001:040x}",
                    )
                )
                for index in range(97)
            ]
            engine = FakeLearningEngine(temp_dir, candidates[0])
            loop = chainseer_base.BaseLearningLoop(
                engine, horizons=(("later", 3600),)
            )
            lookup = {candidate.launch_id: candidate for candidate in candidates}
            for index, candidate in enumerate(candidates):
                result = engine.evaluate_candidate(candidate)
                self.assertTrue(
                    loop.store.add_project(
                        candidate, result, base_now, (("later", 3600),)
                    )
                )
                engine.shadow_trader.state["positions"][
                    candidate.token_address.lower()
                ] = {
                    "launch_id": candidate.launch_id,
                    "token_address": candidate.token_address,
                    "symbol": candidate.symbol,
                    "status": "open",
                    "entry_timestamp": base_now - 10_000,
                    "last_mark_timestamp": base_now - (index + 1) * 100,
                }

            engine.evaluate_calls = 0
            engine.evaluated_ids = []
            engine.observer.fetch_launches = lambda **_kwargs: []
            engine.observer.fetch_launch_by_id = lookup.get

            summary = loop.run_once(
                limit=5,
                refresh_limit=3,
                now=base_now + 300,
            )

            cycle = summary["cycle"]
            self.assertEqual(cycle["shadow_refresh_required"], 7)
            self.assertEqual(cycle["shadow_refresh_target"], 4)
            self.assertEqual(cycle["shadow_refresh_selected"], 7)
            self.assertEqual(cycle["shadow_refreshed"], 7)
            self.assertEqual(cycle["shadow_refresh_capacity_shortfall"], 0)
            self.assertEqual(cycle["shadow_refresh_completion_shortfall"], 0)
            self.assertEqual(cycle["shadow_refresh_priority_target_gap"], 3)
            self.assertEqual(summary["refresh_policy"]["shadow_capacity_shortfall"], 0)
            self.assertEqual(summary["refresh_policy"]["shadow_completion_shortfall"], 0)

    def test_rpc_reads_and_validates_bonding_v5_reserve_spot(self):
        rpc = chainseer_base.BaseRPC("https://example.invalid")
        token = "0x" + "1" * 40
        pair = "0x" + "2" * 40
        reserve_token = 1_000_000_000 * 10**18
        reserve_virtual = 8_500 * 10**18
        calls = {
            chainseer_base.PAIR_TOKEN_A: "0x" + token[2:].rjust(64, "0"),
            chainseer_base.PAIR_TOKEN_B: "0x" + chainseer_base.VIRTUALS_TOKEN_ADDRESS[2:].rjust(64, "0"),
            chainseer_base.PAIR_GET_RESERVES: (
                "0x" + f"{reserve_token:064x}" + f"{reserve_virtual:064x}"
            ),
        }
        rpc.call = lambda _address, data: calls[data]
        rpc.erc20_decimals = lambda _address: 18

        snapshot = rpc.virtuals_pair_snapshot(pair, token)

        self.assertTrue(snapshot["binding_verified"])
        self.assertEqual(snapshot["token_reserve"], 1_000_000_000.0)
        self.assertEqual(snapshot["virtual_reserve"], 8_500.0)
        self.assertEqual(snapshot["price_virtual"], 0.0000085)

        calls[chainseer_base.PAIR_TOKEN_B] = "0x" + ("3" * 40).rjust(64, "0")
        mismatched = rpc.virtuals_pair_snapshot(pair, token)
        self.assertFalse(mismatched["binding_verified"])
        self.assertIsNone(mismatched["price_virtual"])

    def test_observer_accepts_only_base_and_resolves_prototype_address(self):
        candidate = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
        self.assertIsNotNone(candidate)
        self.assertTrue(candidate.is_prototype)
        self.assertEqual(candidate.token_address, "0x" + "1" * 40)
        self.assertEqual(candidate.pair_address, "0x" + "2" * 40)
        self.assertEqual(candidate.implied_price_virtual, 0.000009)

        rejected = chainseer_base.VirtualsBaseObserver.normalize(
            virtuals_item(chain="SOLANA", preToken="not-an-evm-address")
        )
        self.assertIsNone(rejected)

    def test_observer_uses_official_base_filter(self):
        calls = []

        def fake_get(url, *, params=None, ledger=None):
            calls.append((url, params))
            return {"data": [virtuals_item()]}, None, False

        observer = chainseer_base.VirtualsBaseObserver(http_get=fake_get)
        candidates = observer.fetch_launches(limit=3)

        self.assertEqual(len(candidates), 1)
        self.assertEqual(calls[0][0], "https://api.virtuals.io/api/virtuals")
        self.assertEqual(calls[0][1]["filters[chain]"], "BASE")
        self.assertEqual(calls[0][1]["filters[status]"], "1")

    def test_candidate_public_view_excludes_raw_creator_metadata(self):
        candidate = chainseer_base.VirtualsBaseObserver.normalize(
            virtuals_item(creator={"email": "private@example.test", "wallet": "0x" + "3" * 40})
        )

        self.assertIn("creator", candidate.raw)
        self.assertNotIn("raw", candidate.to_dict())
        self.assertNotIn("private@example.test", json.dumps(candidate.to_dict()))

    def test_risk_analyzer_refuses_honeypot_and_live_execution(self):
        def fake_get(url, *, params=None, ledger=None):
            if "token_security" in url:
                payload = {
                    "result": {
                        ("0x" + "1" * 40): {
                            "is_honeypot": "1",
                            "cannot_buy": "0",
                            "cannot_sell_all": "1",
                            "buy_tax": "0.01",
                            "sell_tax": "0.25",
                        }
                    }
                }
            elif "/smart-contracts/" in url:
                payload = {"is_verified": True}
            elif "base.blockscout.com" in url:
                payload = {"is_scam": False, "is_contract": True}
            elif "dexscreener" in url:
                payload = {"pairs": []}
            else:
                payload = {}
            if ledger:
                ledger.record("http", {"url": url, "params": params or {}}, payload)
            return payload, None, False

        candidate = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
        analyzer = chainseer_base.BaseRiskAnalyzer(rpc=FakeBaseRPC(), http_get=fake_get)
        decision = analyzer.analyze(candidate)

        self.assertFalse(decision.paper_entry_allowed)
        self.assertFalse(decision.live_entry_allowed)
        self.assertEqual(decision.risk_level, "Critical")
        self.assertTrue(any("honeypot" in reason.lower() for reason in decision.hard_stops))
        self.assertGreater(decision.provenance["fact_count"], 0)
        self.assertTrue(decision.coverage["bonding_pair"])
        self.assertTrue(decision.canonicality["pair_binding_verified"])

    def test_paper_trader_enters_and_takes_profit_without_broadcast(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            ledger = chainseer_base.PaperTradeLedger(root / "events.jsonl")
            policy = chainseer_base.PaperPolicy(
                amount_virtual=10,
                observation_seconds=0,
                take_profit_tiers=((2.0, 0.5), (10.0, 0.5)),
            )
            trader = chainseer_base.BasePaperTrader(root / "state.json", ledger, policy)
            created_at = datetime.fromtimestamp(time.time() - 60, tz=timezone.utc).isoformat()
            candidate = chainseer_base.VirtualsBaseObserver.normalize(
                virtuals_item(createdAt=created_at)
            )
            decision = chainseer_base.BaseRiskDecision(
                token_address=candidate.token_address,
                block_pin=1,
                score=90,
                risk_level="Low",
                paper_entry_allowed=True,
                live_entry_allowed=False,
                hard_stops=[],
                warnings=[],
                green_flags=[],
                coverage={},
                canonicality={},
                market={},
                security={},
                provenance={"fact_count": 1},
            )

            position = trader.enter(candidate, decision, price_virtual=1.0)
            self.assertIsNotNone(position)
            events = trader.mark(candidate.token_address, price_virtual=2.2)
            self.assertEqual(len(events), 1)
            self.assertEqual(events[0]["payload"]["reason"], "take_profit_2x")
            self.assertGreater(trader.state["positions"][candidate.token_address.lower()]["remaining_quantity"], 0)
            ok, _ = ledger.verify()
            self.assertTrue(ok)
            with self.assertRaises(chainseer_base.LiveExecutionDisabledError):
                trader.broadcast_live_trade(candidate.token_address, 1)

    def test_shadow_trader_ignores_only_the_portfolio_capacity_gate(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            policy = chainseer_base.PaperPolicy(
                amount_virtual=1,
                maximum_positions=1,
                observation_seconds=0,
            )
            portfolio = chainseer_base.BasePaperTrader(
                root / "paper_state.json",
                chainseer_base.PaperTradeLedger(root / "paper_events.jsonl"),
                policy,
            )
            shadow_ledger = chainseer_base.PaperTradeLedger(
                root / "shadow_events.jsonl"
            )
            shadow = chainseer_base.BasePaperTrader(
                root / "shadow_state.json",
                shadow_ledger,
                policy,
                event_namespace="shadow",
                enforce_position_limit=False,
            )
            first = chainseer_base.VirtualsBaseObserver.normalize(virtuals_item())
            second = chainseer_base.VirtualsBaseObserver.normalize(
                virtuals_item(
                    id=119134,
                    preToken="0x" + "3" * 40,
                    preTokenPair="0x" + "4" * 40,
                )
            )

            def decision(candidate, allowed=True):
                return chainseer_base.BaseRiskDecision(
                    token_address=candidate.token_address,
                    block_pin=1,
                    score=90 if allowed else 20,
                    risk_level="Low" if allowed else "Critical",
                    paper_entry_allowed=allowed,
                    live_entry_allowed=False,
                    hard_stops=[] if allowed else ["synthetic hard stop"],
                    warnings=[],
                    green_flags=[],
                    coverage={},
                    canonicality={},
                    market={},
                    security={},
                    provenance={"fact_count": 1},
                )

            self.assertIsNotNone(portfolio.enter(first, decision(first), 1.0))
            self.assertIsNone(portfolio.enter(second, decision(second), 1.0))
            self.assertIsNotNone(shadow.enter(first, decision(first), 1.0))
            self.assertIsNotNone(shadow.enter(second, decision(second), 1.0))
            shadow_exits = shadow.mark(first.token_address, 2.2)
            self.assertEqual(len(shadow_exits), 1)
            self.assertEqual(shadow_exits[0]["event_type"], "shadow_sell")

            refused = chainseer_base.VirtualsBaseObserver.normalize(
                virtuals_item(
                    id=119135,
                    preToken="0x" + "5" * 40,
                    preTokenPair="0x" + "6" * 40,
                )
            )
            self.assertIsNone(shadow.enter(refused, decision(refused, False), 1.0))
            self.assertEqual(
                [event["event_type"] for event in shadow_ledger.load()],
                ["shadow_buy", "shadow_buy", "shadow_sell"],
            )
            ok, report = shadow_ledger.verify()
            self.assertTrue(ok, report)

    def test_paper_trader_waits_out_active_anti_sniper_window(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            trader = chainseer_base.BasePaperTrader(
                root / "state.json",
                chainseer_base.PaperTradeLedger(root / "events.jsonl"),
                chainseer_base.PaperPolicy(observation_seconds=0),
            )
            launched_at = datetime.fromtimestamp(time.time() - 60, tz=timezone.utc).isoformat()
            candidate = chainseer_base.VirtualsBaseObserver.normalize(
                virtuals_item(
                    launchedAt=launched_at,
                    launchInfo={"antiSniperTaxType": 1},
                )
            )
            decision = chainseer_base.BaseRiskDecision(
                token_address=candidate.token_address,
                block_pin=1,
                score=90,
                risk_level="Low",
                paper_entry_allowed=True,
                live_entry_allowed=False,
                hard_stops=[],
                warnings=[],
                green_flags=[],
                coverage={},
                canonicality={},
                market={},
                security={},
                provenance={"fact_count": 1},
            )

            self.assertIsNone(trader.enter(candidate, decision, price_virtual=1.0))
            after_wait = datetime.fromisoformat(launched_at).timestamp() + 98 * 60 + 1
            self.assertIsNotNone(
                trader.enter(candidate, decision, price_virtual=1.0, now=after_wait)
            )

    def test_paper_ledger_detects_tampering(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "events.jsonl"
            ledger = chainseer_base.PaperTradeLedger(path)
            ledger.append("paper_buy", {"amount": 1})
            ok, _ = ledger.verify()
            self.assertTrue(ok)

            record = json.loads(path.read_text(encoding="utf-8"))
            record["payload"]["amount"] = 2
            path.write_text(json.dumps(record) + "\n", encoding="utf-8")
            ok, reason = ledger.verify()
            self.assertFalse(ok)
            self.assertIn("event_hash", reason)


if __name__ == "__main__":
    unittest.main()
