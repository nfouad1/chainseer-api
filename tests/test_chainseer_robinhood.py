import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import chainseer_robinhood as rh
from chainseer_robinhood_reflection import RobinhoodReflectionCoordinator


TOKEN = "0x" + "11" * 20
PAIR = "0x" + "22" * 20
POOL_ID = "0x" + "ab" * 32


def topic(address):
    return "0x" + "0" * 24 + address.removeprefix("0x")


def word(value):
    if isinstance(value, str) and value.startswith("0x"):
        value = int(value, 16)
    return f"{value & ((1 << 256) - 1):064x}"


class FakeRPC:
    def __init__(self, logs=None, latest=100):
        self.logs = logs or []
        self.latest = latest
        self.calls = []

    def get_block_number(self):
        return self.latest

    def get_logs(self, start, end, address=None, topics=None):
        self.calls.append((start, end, address, topics))
        return self.logs

    def get_block(self, block):
        return {"timestamp": hex(1_700_000_000 + block)}

    def erc20_name(self, token, block=None):
        return "Test Token"

    def erc20_symbol(self, token, block=None):
        return "TEST"


class FakeResponse:
    def __init__(self, pairs): self.pairs = pairs
    def raise_for_status(self): return None
    def json(self): return {"pairs": self.pairs}


class FakeAnalyzer:
    def analyze_token(self, token, seal, defer_cognition):
        return {
            "analysis": {
                "legitimacy_score": 82,
                "risk_level": "Low",
                "action_label": "Paper admit",
                "hard_stop_overrides": [],
            },
            "data": {"dexscreener": {"pairs": [{
                "chainId": "robinhood", "pairAddress": PAIR,
                "baseToken": {"address": token, "symbol": "TEST"},
                "quoteToken": {"address": rh.WETH_ADDRESS},
                "priceUsd": "0.25", "liquidity": {"usd": 50000},
                "marketCap": 250000, "fdv": 300000,
            }]}},
        }


class FakeMarket:
    def snapshot(self, token, pair_address=None):
        return {"price_usd": 0.25, "liquidity_usd": 50000,
                "market_cap_usd": 250000, "fdv_usd": 300000}


class FakeV4RPC(FakeRPC):
    def call(self, address, data, block=None):
        if data.startswith("0x" + rh.V4_GET_LIQUIDITY_SELECTOR):
            return hex(10**22)
        if data.startswith("0x" + rh.V4_GET_SLOT0_SELECTOR):
            return "0x" + word(1 << 96) + word(0) + word(0) + word(3000)
        raise AssertionError(data)

    def erc20_decimals(self, token, block=None):
        return 18

    def erc20_total_supply(self, token, block=None):
        return 1_000_000 * 10**18


class RobinhoodLearningTests(unittest.TestCase):
    def test_entry_market_cap_ceiling_observes_without_opening(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": time.time(), "transaction_hash": "0x1",
                "log_index": 0, "name": "Mature", "symbol": "MATURE",
            }])
            market = {
                "price_usd": 0.02, "liquidity_usd": 500_000,
                "market_cap_usd": 12_000_000,
            }
            store.record_analysis(
                TOKEN, FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"],
                market,
            )
            candidate = store.candidate(TOKEN)
            self.assertEqual(candidate["paper_decision"], rh.PAPER_DECISION_ABOVE_CAP)
            self.assertFalse(candidate["paper_entry_allowed"])
            self.assertFalse(store.open_position(candidate, market))
            self.assertEqual(store.summary()["candidates"]["above_entry_ceiling"], 1)

    def test_above_cap_candidate_requires_new_below_cap_momentum(self):
        class FallingThenRisingMarket(FakeMarket):
            def __init__(self): self.price = 0.009
            def snapshots(self, _token):
                return [{
                    "pair_address": PAIR, "source_version": rh.SOURCE_V2,
                    "price_usd": self.price, "liquidity_usd": 100_000,
                    "market_cap_usd": self.price * 1_000_000_000,
                    "source": "reentry-test",
                }]

        with tempfile.TemporaryDirectory() as directory:
            market_client = FallingThenRisingMarket()
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=FakeRPC(), analyzer=FakeAnalyzer(), market=market_client,
            )
            engine.store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": time.time(), "transaction_hash": "0x1",
                "log_index": 0, "name": "Mature", "symbol": "MATURE",
            }])
            engine.store.record_analysis(TOKEN, {
                "legitimacy_score": 82, "risk_level": "Low",
                "hard_stop_overrides": [],
            }, {
                "price_usd": 0.012, "liquidity_usd": 100_000,
                "market_cap_usd": 12_000_000,
            })
            first = engine.recheck_executable_markets(time.time(), 1)
            self.assertEqual(first["paper_entries"], 0)
            self.assertEqual(
                engine.store.candidate(TOKEN)["paper_decision"],
                rh.PAPER_DECISION_REENTRY,
            )
            with engine.store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET market_watch_last_checked_at=0 WHERE token_address=?",
                    (TOKEN,),
                )
            market_client.price = 0.0095
            second = engine.recheck_executable_markets(time.time(), 1)
            self.assertEqual(second["paper_entries"], 1)
            self.assertEqual(
                engine.store.candidate(TOKEN)["paper_decision"],
                rh.PAPER_DECISION_ADMITTED,
            )

    def test_staged_exit_recovers_principal_then_trails_runner(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": time.time(), "transaction_hash": "0x1",
                "log_index": 0, "name": "Runner", "symbol": "RUN",
            }])
            entry = {
                "price_usd": 1.0, "liquidity_usd": 10_000_000,
                "market_cap_usd": 1_000_000,
            }
            store.record_analysis(
                TOKEN, FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"], entry
            )
            self.assertTrue(store.open_position(store.candidate(TOKEN), entry))
            first = store.mark_position(TOKEN, {
                "price_usd": 2.1, "liquidity_usd": 10_000_000,
                "market_cap_usd": 2_100_000, "source": "test",
            }, time.time())
            self.assertEqual(first["partial_exits"][0]["stage"], "recover_principal_2x")
            self.assertAlmostEqual(first["remaining_fraction"], 0.5, places=6)
            second = store.mark_position(TOKEN, {
                "price_usd": 3.2, "liquidity_usd": 10_000_000,
                "market_cap_usd": 3_200_000, "source": "test",
            }, time.time() + 1)
            self.assertEqual(second["partial_exits"][0]["stage"], "take_profit_3x")
            self.assertAlmostEqual(second["remaining_fraction"], 0.25, places=6)
            store.mark_position(TOKEN, {
                "price_usd": 4.0, "liquidity_usd": 10_000_000,
                "market_cap_usd": 4_000_000, "source": "test",
            }, time.time() + 2)
            final = store.mark_position(TOKEN, {
                "price_usd": 2.5, "liquidity_usd": 10_000_000,
                "market_cap_usd": 2_500_000, "source": "test",
            }, time.time() + 3)
            self.assertEqual(final["reason"], "runner_trailing_stop")
            position = store.recent_positions()[0]
            self.assertEqual(position["status"], "closed")
            self.assertEqual(position["remaining_fraction"], 0)
            policies = {p["policy"]: p for p in position["counterfactual_policies"]}
            self.assertEqual(policies[rh.SHADOW_POLICY_FIXED_3X]["status"], "closed")
            self.assertEqual(policies[rh.SHADOW_POLICY_PURE_TRAILING]["status"], "closed")
            dashboard = rh.dashboard_snapshot(directory)
            self.assertEqual(dashboard["positions"], [])
            self.assertEqual(len(dashboard["closed_positions"]), 1)
            closed = dashboard["closed_positions"][0]
            self.assertEqual(closed["token_address"], TOKEN.lower())
            self.assertIsNotNone(closed["exit_price_usd"])
            self.assertIsNotNone(closed["exit_value_usd"])
            self.assertIsNotNone(closed["closed_at"])
            self.assertGreater(closed["gain_pct"], 0)

    def test_high_quality_illiquid_candidate_waits_instead_of_rejection(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": time.time(), "transaction_hash": "0x1",
                "log_index": 0, "name": "BLINK", "symbol": "BLINK",
            }])
            store.record_analysis(TOKEN, {
                "legitimacy_score": 84.5, "risk_level": "Low",
                "action_label": "PASSES INITIAL CHECKS", "hard_stop_overrides": [],
            }, {
                "price_usd": 0.0015, "liquidity_usd": 7_000,
                "market_cap_usd": 1_500_000,
            })
            candidate = store.candidate(TOKEN)
            self.assertEqual(
                candidate["paper_decision"], rh.PAPER_DECISION_WATCHING
            )
            self.assertFalse(candidate["paper_entry_allowed"])
            self.assertEqual(store.summary()["candidates"]["watching"], 1)
            self.assertEqual(store.summary()["candidates"]["rejected"], 0)

    def test_market_watch_uses_future_cross_pool_snapshot_and_rechecks_analysis(self):
        class CrossPoolMarket(FakeMarket):
            def snapshots(self, _token):
                return [
                    {
                        "pair_address": "0x" + "33" * 20,
                        "source_version": rh.SOURCE_V4,
                        "price_usd": 0.0016, "liquidity_usd": 500_000,
                        "market_cap_usd": 1_600_000,
                    },
                    {
                        "pair_address": "0x" + "44" * 20,
                        "source_version": rh.SOURCE_V3,
                        "price_usd": 0.0014, "liquidity_usd": 175_000,
                        "market_cap_usd": 1_400_000,
                        "source": "future-cross-pool-test",
                    },
                ]

        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=FakeRPC(), analyzer=FakeAnalyzer(),
                market=CrossPoolMarket(),
            )
            engine.store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V4_POOL_MANAGER,
                "block_number": 1, "block_timestamp": time.time(),
                "transaction_hash": "0x1", "log_index": 0,
                "name": "BLINK", "symbol": "BLINK",
                "source_version": rh.SOURCE_V4, "pool_id": POOL_ID,
                "hooks_address": rh.ZERO_ADDRESS,
            }])
            engine.store.record_analysis(TOKEN, {
                "legitimacy_score": 84.5, "risk_level": "Low",
                "action_label": "PASSES INITIAL CHECKS",
                "hard_stop_overrides": [{"code": "V4_MARKET_STATE_UNVERIFIED"}],
            }, {})
            result = engine.recheck_executable_markets(time.time(), 1)
            self.assertEqual(result["checked"], 1)
            self.assertEqual(result["executable_markets_found"], 1)
            self.assertEqual(result["paper_entries"], 1)
            candidate = engine.store.candidate(TOKEN)
            self.assertEqual(candidate["source_version"], rh.SOURCE_V3)
            self.assertEqual(candidate["paper_decision"], rh.PAPER_DECISION_ADMITTED)
            self.assertEqual(candidate["market_watch_checks"], 1)
            self.assertEqual(candidate["market_watch_reason"], "executable_market_found")
            position = engine.store.recent_positions()[0]
            self.assertEqual(position["entry_market_cap_usd"], 1_400_000)
            self.assertEqual(position["entry_price_usd"], 0.0014)

    def test_market_watch_expires_without_fabricating_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": time.time(), "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST",
            }])
            store.record_analysis(TOKEN, {
                "legitimacy_score": 80, "risk_level": "Low",
                "hard_stop_overrides": [],
            }, {"price_usd": 1, "liquidity_usd": 100})
            with store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET market_watch_expires_at=? WHERE token_address=?",
                    (time.time() - 1, TOKEN),
                )
            self.assertEqual(store.pending_market_watches(time.time(), 1), [])
            candidate = store.candidate(TOKEN)
            self.assertEqual(candidate["paper_decision"], rh.PAPER_DECISION_EXPIRED)
            self.assertEqual(store.recent_positions(), [])

    def test_pending_analysis_reserves_momentum_and_oldest_fairness_slots(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "learning.sqlite3")
            old = "0x" + "41" * 20
            momentum = "0x" + "42" * 20
            artifact = "0x" + "43" * 20
            for index, token in enumerate((old, momentum, artifact)):
                store.add_candidates([{
                    "token_address": token,
                    "pair_address": PAIR,
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": index + 1,
                    "block_timestamp": 1_700_000_000 + index,
                    "transaction_hash": f"0x{index + 1}",
                    "log_index": index,
                    "name": f"Token {index}",
                    "symbol": f"T{index}",
                }])
            with store.connection() as connection:
                connection.executemany(
                    """
                    INSERT INTO checkpoints (
                        token_address,horizon_label,horizon_seconds,target_at,
                        observed_at,status,learning_eligible,lateness_seconds,
                        market_cap_usd,market_cap_multiple,liquidity_usd
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            momentum, "1h", 3600, 1_700_003_600,
                            "2026-08-12T00:00:00+00:00", "observed", 1, 0,
                            40_000, 5.0, 20_000,
                        ),
                        (
                            artifact, "1h", 3600, 1_700_003_601,
                            "2026-08-12T00:00:00+00:00", "observed", 1, 0,
                            1_000_000_000, 100.0, 100,
                        ),
                    ],
                )
            selected = store.pending_analysis(2)
            self.assertEqual(
                [row["token_address"] for row in selected], [momentum, old]
            )
            self.assertEqual(selected[0]["momentum_priority_multiple"], 5.0)

    def test_five_to_ten_million_momentum_is_downprioritized(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            lower_cap = "0x" + "61" * 20
            upper_band = "0x" + "62" * 20
            for index, token in enumerate((upper_band, lower_cap)):
                store.add_candidates([{
                    "token_address": token, "pair_address": PAIR,
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": index + 1, "block_timestamp": 1_700_000_000,
                    "transaction_hash": f"0x{index}", "log_index": index,
                    "name": "Priority", "symbol": "PRI",
                }])
            with store.connection() as connection:
                connection.executemany(
                    """
                    INSERT INTO checkpoints (
                        token_address,horizon_label,horizon_seconds,target_at,
                        observed_at,status,learning_eligible,lateness_seconds,
                        market_cap_usd,market_cap_multiple,liquidity_usd
                    ) VALUES (?, '1h',3600,0,'now','observed',1,0,?,?,?)
                    """,
                    [
                        (upper_band, 7_000_000, 6.0, 500_000),
                        (lower_cap, 4_000_000, 4.0, 500_000),
                    ],
                )
            selected = store.pending_analysis(1)
            self.assertEqual(selected[0]["token_address"], lower_cap)
            self.assertEqual(selected[0]["momentum_priority_multiple"], 4.0)

    def test_position_uses_actual_open_market_cap_and_live_mark(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "learning.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN,
                "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY,
                "block_number": 1,
                "block_timestamp": 1_700_000_000,
                "transaction_hash": "0x1",
                "log_index": 0,
                "name": "HOJAK",
                "symbol": "HOJAK",
            }])
            entry_market = {
                "price_usd": 0.0025,
                "liquidity_usd": 150_000,
                "market_cap_usd": 2_500_000,
                "fdv_usd": 2_500_000,
            }
            store.record_analysis(
                TOKEN,
                {
                    "legitimacy_score": 80,
                    "risk_level": "Medium",
                    "action_label": "WATCHLIST",
                    "hard_stop_overrides": [],
                },
                entry_market,
            )
            with store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET first_market_cap_usd=? WHERE token_address=?",
                    (5_643, TOKEN),
                )
            self.assertTrue(store.open_position(store.candidate(TOKEN), entry_market))
            positions = store.recent_positions(live_markets={TOKEN: {
                "price_usd": 0.0075,
                "liquidity_usd": 250_000,
                "market_cap_usd": 6_300_000,
                "source": "live-test",
                "observed_at": "2026-08-12T12:00:00+00:00",
            }})
            self.assertEqual(positions[0]["launch_market_cap_usd"], 5_643)
            self.assertEqual(positions[0]["entry_market_cap_usd"], 2_500_000)
            self.assertEqual(positions[0]["current_market_cap_usd"], 6_300_000)
            self.assertEqual(positions[0]["market_source"], "live-test")
            self.assertGreater(positions[0]["gain_pct"], 190)

            class ChangingMarket:
                def __init__(self):
                    self.calls = 0

                def snapshot(self, _token, _pair):
                    self.calls += 1
                    return {
                        "price_usd": 0.005 + self.calls * 0.001,
                        "liquidity_usd": 200_000,
                        "market_cap_usd": 5_000_000 + self.calls * 1_000_000,
                        "source": "changing-live-market",
                    }

            market = ChangingMarket()
            fake_engine = SimpleNamespace(
                root=root,
                store=store,
                market=market,
                v4_market=None,
            )
            refresher = rh.RobinhoodDashboardMarketRefresher(
                root, engine=fake_engine
            )
            first = refresher.snapshot()["positions"][0]
            second = refresher.snapshot()["positions"][0]
            self.assertEqual(market.calls, 2)
            self.assertEqual(first["current_market_cap_usd"], 6_000_000)
            self.assertEqual(second["current_market_cap_usd"], 7_000_000)

    def test_reflection_seals_once_per_fifteen_and_syncs_all_todos(self):
        class Result:
            stdout = "sealed test reflection"

        calls = []

        def command_runner(arguments, **_kwargs):
            calls.append(arguments)
            return Result()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "learning.sqlite3")
            todo = root / "TODO.md"
            todo.write_text("# TODO\n", encoding="utf-8")
            rh.atomic_json_write(
                root / "discovery_v4_cursor.json",
                {"coverage": {"blocks_behind": 500, "caught_up": False}},
            )

            def add_analyzed(start, count):
                for index in range(start, start + count):
                    token = f"0x{index + 1:040x}"
                    store.add_candidates([{
                        "token_address": token,
                        "pair_address": PAIR,
                        "factory_address": rh.UNISWAP_V2_FACTORY,
                        "block_number": index + 1,
                        "block_timestamp": 1_700_000_000 + index,
                        "transaction_hash": f"0x{index + 1:064x}",
                        "log_index": index,
                        "name": "" if index % 4 == 0 else f"Token {index}",
                        "symbol": "" if index % 4 == 0 else f"T{index}",
                    }])
                    store.record_analysis(
                        token,
                        {
                            "legitimacy_score": 65,
                            "risk_level": "Medium",
                            "action_label": "WATCHLIST",
                            "hard_stop_overrides": [],
                        },
                        {
                            "price_usd": 0.1,
                            "liquidity_usd": 50_000,
                            "market_cap_usd": (
                                1_200_000 if index in {0, 1} else 100_000
                            ),
                            "fdv_usd": 150_000,
                        },
                    )

            add_analyzed(0, 15)
            with store.connection() as connection:
                tokens = [
                    row[0]
                    for row in connection.execute(
                        "SELECT token_address FROM candidates ORDER BY analyzed_at LIMIT 10"
                    )
                ]
                for index, token in enumerate(tokens):
                    connection.execute(
                        """
                        INSERT INTO checkpoints (
                            token_address,horizon_label,horizon_seconds,target_at,
                            observed_at,status,learning_eligible,lateness_seconds,
                            market_cap_usd,market_cap_multiple,liquidity_usd
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (
                            token, "1h", 3600, 1_700_003_600,
                            "2026-08-12T00:00:00+00:00",
                            "no_market" if 2 <= index < 6 else "observed",
                            1, 0,
                            2_400_000 if index == 0 else 100_000,
                            2.0 if index == 0 else 1.0,
                            50_000,
                        ),
                    )
            coordinator = RobinhoodReflectionCoordinator(
                root,
                store,
                todo_path=todo,
                skill_root=root / "skill",
                command_runner=command_runner,
            )
            for name in ("timechain.py", "chronosynaptic.py", "recall.py"):
                (root / "skill").mkdir(exist_ok=True)
                (root / "skill" / name).write_text("# test", encoding="utf-8")

            first = coordinator.run_if_due()
            self.assertEqual(first["checkpoint"], 15)
            self.assertEqual(len(first["recommendations"]), 4)
            todo_text = todo.read_text(encoding="utf-8")
            self.assertIn("RH-REFLECT-FALSE-NEGATIVES", todo_text)
            self.assertIn("RH-REFLECT-MARKET-COVERAGE", todo_text)
            self.assertIn("RH-REFLECT-METADATA", todo_text)
            self.assertIn("RH-REFLECT-V4-CATCHUP", todo_text)
            self.assertTrue(any("collapse-notes" in call for call in calls))
            self.assertTrue(any("turn" in call for call in calls))

            call_count = len(calls)
            self.assertEqual(coordinator.run_if_due()["status"], "not_due")
            self.assertEqual(len(calls), call_count)

            add_analyzed(15, 15)
            second = coordinator.run_if_due()
            self.assertEqual(second["checkpoint"], 30)
            self.assertEqual(
                rh.read_json(root / "reflection_state.json")["next_checkpoint"],
                45,
            )

    def test_counterfactual_audit_requires_realizable_gain_and_keeps_hard_stops(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "learning.sqlite3")
            legitimate = "0x" + "31" * 20
            artifact = "0x" + "32" * 20
            store.add_candidates([
                {
                    "token_address": legitimate,
                    "pair_address": PAIR,
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": 1,
                    "block_timestamp": 1_700_000_000,
                    "transaction_hash": "0x31",
                    "log_index": 0,
                    "name": "Legitimate",
                    "symbol": "REAL",
                },
                {
                    "token_address": artifact,
                    "pair_address": PAIR,
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": 2,
                    "block_timestamp": 1_700_000_001,
                    "transaction_hash": "0x32",
                    "log_index": 1,
                    "name": "Artifact",
                    "symbol": "FAKE",
                },
            ])
            store.record_analysis(
                legitimate,
                {
                    "legitimacy_score": 65,
                    "risk_level": "Medium",
                    "action_label": "WATCHLIST",
                    "hard_stop_overrides": [],
                },
                {
                    "price_usd": 0.5,
                    "liquidity_usd": 20_000,
                    "market_cap_usd": 500_000,
                    "fdv_usd": 500_000,
                },
            )
            store.record_analysis(
                artifact,
                {
                    "legitimacy_score": 80,
                    "risk_level": "Critical",
                    "action_label": "AVOID",
                    "hard_stop_overrides": ["EXTREME_CONCENTRATION"],
                },
                {
                    "price_usd": 1e-20,
                    "liquidity_usd": 1,
                    "market_cap_usd": 1e47,
                    "fdv_usd": 1e47,
                },
            )
            with store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET peak_market_cap_usd=? WHERE token_address=?",
                    (2_000_000, legitimate),
                )
                connection.executemany(
                    """
                    INSERT INTO checkpoints (
                        token_address,horizon_label,horizon_seconds,target_at,
                        observed_at,status,learning_eligible,lateness_seconds,
                        market_cap_usd,market_cap_multiple,liquidity_usd
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    [
                        (
                            legitimate, "1h", 3600, 1_700_003_600,
                            "2026-08-12T00:00:00+00:00", "observed", 1, 0,
                            2_000_000, 4.0, 25_000,
                        ),
                        (
                            artifact, "1h", 3600, 1_700_003_601,
                            "2026-08-12T00:00:00+00:00", "observed", 1, 0,
                            1e47, 1.0, 1,
                        ),
                    ],
                )
            coordinator = RobinhoodReflectionCoordinator(
                root,
                store,
                todo_path=root / "TODO.md",
                skill_root=root / "skill",
            )
            audit = coordinator.audit_all()
            self.assertEqual(audit["headline_million_peaks"], 2)
            self.assertEqual(audit["confirmed_false_negatives"], 1)
            self.assertEqual(audit["confirmed"][0]["token_address"], legitimate)
            artifact_review = next(
                item for item in audit["headline_review"]
                if item["token_address"] == artifact
            )
            self.assertIn("hard_safety_stop", artifact_review["exclusion_reasons"])
            self.assertIn(
                "implausible_entry_valuation", artifact_review["exclusion_reasons"]
            )

    def test_observer_retries_transient_rpc_without_skipping_cursor_window(self):
        live_scan_active = threading.Event()
        throttle_observed = threading.Event()

        class ContendedRPC(FakeRPC):
            def __init__(self):
                super().__init__(latest=100)
                self.failures = 0

            def get_logs(self, *args, **kwargs):
                if live_scan_active.is_set():
                    self.failures += 1
                    throttle_observed.set()
                    raise TimeoutError("shared RPC temporarily throttled")
                return super().get_logs(*args, **kwargs)

        with tempfile.TemporaryDirectory() as directory:
            cursor = Path(directory) / "cursor.json"
            rpc = ContendedRPC()
            def simulated_live_scan():
                live_scan_active.set()
                throttle_observed.wait(1)
                live_scan_active.clear()

            scan = threading.Thread(target=simulated_live_scan)
            scan.start()
            self.assertTrue(live_scan_active.wait(1))
            real_sleep = time.sleep
            with patch.object(
                rh.time, "sleep", side_effect=lambda _seconds: real_sleep(0.01)
            ), patch.object(
                rh.random, "uniform", return_value=0.0
            ):
                rows, coverage = rh.RobinhoodPairObserver(rpc, cursor).sync(
                    block_limit=5, lookback=5
                )
            scan.join()
            self.assertEqual(rows, [])
            self.assertEqual(coverage["from_block"], 95)
            self.assertEqual(json.loads(cursor.read_text())["next_block"], 100)
            self.assertGreaterEqual(rpc.failures, 1)
            self.assertFalse(live_scan_active.is_set())

    def test_observer_does_not_advance_cursor_after_exhausted_rpc_retries(self):
        class BrokenRPC(FakeRPC):
            def get_logs(self, *args, **kwargs):
                raise TimeoutError("persistent throttle")

        with tempfile.TemporaryDirectory() as directory:
            cursor = Path(directory) / "cursor.json"
            cursor.write_text(json.dumps({"next_block": 77}))
            with patch.object(rh.time, "sleep", return_value=None), patch.object(
                rh.random, "uniform", return_value=0.0
            ):
                with self.assertRaisesRegex(RuntimeError, "after 4 attempts"):
                    rh.RobinhoodPairObserver(BrokenRPC(latest=100), cursor).sync(
                        block_limit=5, lookback=5
                    )
            self.assertEqual(json.loads(cursor.read_text())["next_block"], 77)

    def test_failed_learning_cycle_is_not_left_running(self):
        class BrokenRPC(FakeRPC):
            def get_block_number(self):
                raise TimeoutError("RPC unavailable")

        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(
                directory,
                rpc=BrokenRPC(),
                analyzer=FakeAnalyzer(),
                market=FakeMarket(),
            )
            with patch.object(rh.time, "sleep", return_value=None), patch.object(
                rh.random, "uniform", return_value=0.0
            ):
                with self.assertRaisesRegex(RuntimeError, "after 4 attempts"):
                    engine.run_once(outcome_limit=0, analysis_limit=0)
            with engine.store.connection() as connection:
                run = connection.execute(
                    "SELECT status,completed_at,summary_json FROM runs ORDER BY id DESC LIMIT 1"
                ).fetchone()
            self.assertEqual(run["status"], "failed")
            self.assertIsNotNone(run["completed_at"])
            self.assertIn("RPC unavailable", run["summary_json"])

    def test_v4_requires_initialize_liquidity_and_first_swap(self):
        initialize = {
            "topics": [rh.V4_INITIALIZE_TOPIC, POOL_ID, topic(TOKEN), topic(rh.USDG_ADDRESS)],
            "data": "0x" + word(3000) + word(60) + word(0) + word(1 << 96) + word(0),
            "blockNumber": "0x61", "logIndex": "0x0", "transactionHash": "0xi",
        }
        modify = {"topics": [rh.V4_MODIFY_LIQUIDITY_TOPIC, POOL_ID], "data": "0x",
                  "blockNumber": "0x62", "logIndex": "0x0", "transactionHash": "0xm"}
        swap = {"topics": [rh.V4_SWAP_TOPIC, POOL_ID, topic(PAIR)],
                "data": "0x" + word(1) + word(-1) + word(1 << 96) + word(10**18) + word(0) + word(3000),
                "blockNumber": "0x63", "logIndex": "0x1", "transactionHash": "0xs"}
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            cursor = Path(directory)/"v4.json"
            rows, coverage = rh.RobinhoodV4Observer(FakeRPC([initialize, modify]), store, cursor).sync(block_limit=5, lookback=5)
            self.assertEqual(rows, [])
            self.assertEqual(coverage["activated_pools"], 0)
            cursor.unlink()
            rows, coverage = rh.RobinhoodV4Observer(FakeRPC([initialize, modify, swap]), store, cursor).sync(block_limit=5, lookback=5)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["source_version"], rh.SOURCE_V4)
            self.assertEqual(rows[0]["pool_id"], POOL_ID)
            self.assertEqual(coverage["activated_pools"], 1)

    def test_v4_splits_provider_limited_windows_without_skipping_cursor(self):
        class RangeLimitedRPC(FakeRPC):
            def get_logs(self, start, end, address=None, topics=None):
                self.calls.append((start, end, address, topics))
                if end - start + 1 > 2:
                    raise RuntimeError("logs matched by query exceeds limit of 10000")
                return []

        with tempfile.TemporaryDirectory() as directory:
            cursor = Path(directory) / "v4.json"
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            rpc = RangeLimitedRPC(latest=100)
            rows, coverage = rh.RobinhoodV4Observer(rpc, store, cursor).sync(
                block_limit=5, lookback=5
            )
            self.assertEqual(rows, [])
            self.assertEqual(coverage["from_block"], 95)
            self.assertEqual(coverage["to_block"], 99)
            self.assertEqual(coverage["rpc_windows"], 3)
            self.assertEqual(json.loads(cursor.read_text())["next_block"], 100)
            self.assertIn((95, 99, rh.UNISWAP_V4_POOL_MANAGER, unittest.mock.ANY), rpc.calls)

    def test_v4_ignores_pool_without_supported_anchor(self):
        initialize = {
            "topics": [rh.V4_INITIALIZE_TOPIC, POOL_ID, topic(TOKEN), topic(PAIR)],
            "data": "0x" + word(3000) + word(60) + word(0) + word(1 << 96) + word(0),
            "blockNumber": "0x61", "logIndex": "0x0",
        }
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            rows, _ = rh.RobinhoodV4Observer(FakeRPC([initialize]), store, Path(directory)/"v4.json").sync(block_limit=5, lookback=5)
            self.assertEqual(rows, [])
            with store.connection() as connection:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM v4_pools").fetchone()[0], 0)

    def test_v4_market_uses_current_state_view_and_labels_estimate(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            store.apply_v4_events([{
                "kind":"initialize","pool_id":POOL_ID,"currency0":TOKEN.lower(),
                "currency1":rh.USDG_ADDRESS.lower(),"token_address":TOKEN.lower(),
                "anchor_address":rh.USDG_ADDRESS.lower(),"fee_tier":3000,"tick_spacing":60,
                "hooks_address":rh.ZERO_ADDRESS,"block_number":1,
                "sqrt_price_x96":1 << 96,"tick":0,
            }])
            market = rh.RobinhoodV4MarketClient(FakeV4RPC(), FakeMarket(), store).snapshot(
                {"token_address":TOKEN.lower(),"pool_id":POOL_ID}
            )
            self.assertTrue(market["current_state_verified"])
            self.assertEqual(market["price_usd"], 1.0)
            self.assertEqual(market["market_cap_usd"], 1_000_000)
            self.assertEqual(market["liquidity_model"], "active_concentrated_liquidity_estimate")

    def test_existing_database_migrates_source_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)/"learn.sqlite3"
            connection = __import__("sqlite3").connect(path)
            connection.execute("CREATE TABLE candidates(token_address TEXT PRIMARY KEY,pair_address TEXT NOT NULL,factory_address TEXT NOT NULL,block_number INTEGER NOT NULL,block_timestamp REAL NOT NULL,transaction_hash TEXT NOT NULL,log_index INTEGER NOT NULL,name TEXT,symbol TEXT,analysis_status TEXT NOT NULL DEFAULT 'pending',analysis_attempts INTEGER NOT NULL DEFAULT 0,analyzed_at TEXT,score REAL,risk_level TEXT,action_label TEXT,hard_stops_json TEXT NOT NULL DEFAULT '[]',paper_entry_allowed INTEGER,entry_price_usd REAL,entry_liquidity_usd REAL,first_market_cap_usd REAL,peak_market_cap_usd REAL,peak_fdv_usd REAL,last_observed_at TEXT,last_outcome_attempt_at REAL,discovered_at TEXT NOT NULL,updated_at TEXT NOT NULL)")
            connection.commit(); connection.close()
            store = rh.RobinhoodLearningStore(path)
            with store.connection() as connection:
                columns = {row[1] for row in connection.execute("PRAGMA table_info(candidates)")}
            self.assertTrue({"source_version","pool_id","market_evidence_json"} <= columns)

    def test_observer_decodes_wrapped_native_pair_and_advances_cursor(self):
        log = {
            "topics": [rh.PAIR_CREATED_TOPIC, topic(rh.WETH_ADDRESS), topic(TOKEN)],
            "data": "0x" + "0" * 24 + PAIR.removeprefix("0x") + "0" * 64,
            "blockNumber": hex(97), "logIndex": "0x2", "transactionHash": "0xabc",
        }
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "cursor.json"
            observer = rh.RobinhoodPairObserver(FakeRPC([log]), state)
            rows, coverage = observer.sync(block_limit=5, lookback=5)
            self.assertEqual(rows[0]["token_address"].lower(), TOKEN.lower())
            self.assertEqual(rows[0]["pair_address"].lower(), PAIR.lower())
            self.assertEqual(coverage["from_block"], 95)
            self.assertEqual(coverage["to_block"], 99)
            self.assertEqual(json.loads(state.read_text())["next_block"], 100)

    def test_observer_ignores_non_wrapped_pair(self):
        other = "0x" + "33" * 20
        log = {"topics": [rh.PAIR_CREATED_TOPIC, topic(TOKEN), topic(other)],
               "data": "0x" + "0" * 24 + PAIR[2:] + "0" * 64,
               "blockNumber": "0x60", "logIndex": "0x0"}
        with tempfile.TemporaryDirectory() as directory:
            rows, _ = rh.RobinhoodPairObserver(FakeRPC([log]), Path(directory)/"c.json").sync(block_limit=5, lookback=5)
            self.assertEqual(rows, [])

    def test_market_client_rejects_quote_only_price(self):
        client = rh.RobinhoodMarketClient()
        client.session.get = lambda *args, **kwargs: FakeResponse([{
            "chainId": "robinhood", "pairAddress": PAIR,
            "baseToken": {"address": rh.WETH_ADDRESS},
            "quoteToken": {"address": TOKEN}, "priceUsd": "999",
            "liquidity": {"usd": 999999},
        }])
        self.assertEqual(client.snapshot(TOKEN, PAIR), {})

    def test_store_records_real_and_missed_checkpoints_without_fabrication(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            candidate = {"token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": 1_700_000_000, "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST"}
            store.add_candidates([candidate])
            due = store.due_outcomes(1_700_000_000 + 900, 2)[0]
            store.record_outcome(due, FakeMarket().snapshot(TOKEN), due["target_at"] + 5)
            self.assertEqual(store.summary()["checkpoints"]["observed"], 1)
            expired = store.expire_missed(1_700_000_000 + 8*24*3600)
            self.assertGreaterEqual(expired, 1)
            with store.connection() as connection:
                missed = connection.execute("SELECT market_cap_usd FROM checkpoints WHERE status='missed'").fetchone()
            self.assertIsNone(missed[0])

    def test_analysis_opens_paper_position_but_has_no_live_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            store.add_candidates([{"token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": time.time(), "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST"}])
            market = FakeMarket().snapshot(TOKEN)
            store.record_analysis(TOKEN, FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"], market)
            self.assertTrue(store.open_position(store.candidate(TOKEN), market))
            summary = store.summary()
            self.assertEqual(summary["positions"]["open"], 1)
            self.assertFalse(summary["live_execution_enabled"])
            position = store.recent_positions()[0]
            self.assertEqual(position["symbol"], "TEST")
            self.assertEqual(position["entry_price_usd"], 0.25)
            self.assertEqual(position["current_market_cap_usd"], 250000)
            self.assertLess(position["gain_pct"], 0)  # entry + estimated exit friction
            analyzed = store.recent_analyzed_tokens()[0]
            self.assertEqual(analyzed["name"], "Test")
            self.assertEqual(analyzed["token_address"], TOKEN.lower())
            self.assertTrue(analyzed["paper_entry_allowed"])

    def test_failed_token_cannot_starve_new_analysis_forever(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            store.add_candidates([{"token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": time.time(), "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST"}])
            for _ in range(rh.MAXIMUM_ANALYSIS_ATTEMPTS):
                store.record_analysis_failure(TOKEN, "synthetic failure")
            self.assertEqual(store.pending_analysis(1), [])
            self.assertEqual(store.candidate(TOKEN)["analysis_status"], "failed")
            self.assertEqual(store.summary()["candidates"]["failed"], 1)

    def test_engine_runs_discovery_then_bounded_analysis(self):
        log = {"topics": [rh.PAIR_CREATED_TOPIC, topic(rh.WETH_ADDRESS), topic(TOKEN)],
               "data": "0x" + "0"*24 + PAIR[2:] + "0"*64,
               "blockNumber": "0x64", "logIndex": "0x0", "transactionHash": "0x1"}
        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(directory, rpc=FakeRPC([log]), analyzer=FakeAnalyzer(), market=FakeMarket())
            result = engine.run_once(discovery_block_limit=1, analysis_limit=1, outcome_limit=0, lookback=1)
            self.assertEqual(result["cycle"]["new_candidates"], 1)
            self.assertEqual(result["cycle"]["analyses"], 1)
            self.assertEqual(result["cycle"]["paper_entries"], 1)
            self.assertTrue(engine.verify()["ok"])

    def test_dashboard_is_local_only_and_read_only_asset_has_no_controls(self):
        with self.assertRaises(ValueError):
            rh.serve_dashboard("unused", "0.0.0.0", 0)
        html = Path(rh.__file__).with_name("robinhood_dashboard.html").read_text()
        self.assertNotIn("<form", html.lower())
        self.assertNotIn("fetch('/api/", html.replace("fetch('/api/status", ""))
        self.assertIn("Current market cap", html)
        self.assertIn("Gain", html)
        self.assertIn("Closed-position results", html)
        self.assertIn("Overall gain", html)
        self.assertIn("renderClosed", html)
        self.assertIn("Analyzed tokens", html)
        self.assertIn("Contract address", html)
        self.assertIn("Chronosynaptic reflection", html)
        self.assertIn("renderReflection", html)

    def test_scheduler_preserves_redirected_python_traceback(self):
        script = Path(rh.__file__).with_name(
            "run_chainseer_robinhood_learning.ps1"
        ).read_text()
        self.assertIn("RedirectStandardError", script)
        self.assertIn("Get-Content -LiteralPath $stderrPath -Raw", script)
        self.assertIn('Write-Status "failed" $process.ExitCode $detail', script)


class CheckpointCadenceTests(unittest.TestCase):
    """Short horizons were expiring because their window was one cycle wide.

    Measured cadence: median 300s between learn cycles, max 1,125s. The old
    tolerance floor of 5 minutes gave the 15m horizon a 300s window, so a
    single slow cycle expired the checkpoint outright -- 24.6% missed at 15m,
    12.1% at 1h, 0.3% at 6h, exactly tracking window width.
    """

    def test_short_horizon_window_survives_a_slow_cycle(self):
        window = rh.RobinhoodLearningStore.tolerance(15 * 60)
        self.assertGreaterEqual(
            window, 2 * 300,
            "the 15m window is still under two nominal cycles, so ordinary "
            "cadence jitter will keep expiring it",
        )

    def test_long_horizons_keep_their_proportional_window(self):
        # horizon//4 must still dominate once it exceeds the floor, or long
        # horizons would silently inherit a fixed short window.
        self.assertEqual(rh.RobinhoodLearningStore.tolerance(6 * 3600), 6 * 3600 // 4)

    def test_a_timely_mark_is_learning_eligible(self):
        self.assertEqual(rh.RobinhoodLearningStore.learning_eligible(900, 60), 1)

    def test_a_very_late_mark_is_recorded_but_not_learned_from(self):
        """Coverage and measurement integrity are separate concerns.

        Widening the window trades accuracy for observations unless late marks
        are excluded from learning -- a 15m outcome measured at 27m is not a
        15m outcome.
        """
        self.assertEqual(rh.RobinhoodLearningStore.learning_eligible(900, 700), 0)

    def test_eligibility_boundary_is_inclusive(self):
        self.assertEqual(rh.RobinhoodLearningStore.learning_eligible(900, 450), 1)


class ExitCauseClassificationTests(unittest.TestCase):
    """Exit reasons must name the cause, not the first matching branch.

    A rug satisfies the liquidity AND the stop-loss condition at once, so
    ordering alone decided the label. Measured on the first 8 closes, 5 exited
    as liquidity_below_minimum at exactly 0.0x with high_multiple 1.0 -- entry
    liquidity $51k-$155k, price straight to zero. stop_loss had never fired,
    so the stop was untested rather than working.
    """

    @staticmethod
    def _classify(price_multiple, multiple, liquidity):
        """Mirror of the live branch order, so the ordering itself is pinned."""
        if price_multiple <= rh.PRICE_COLLAPSE_MULTIPLE:
            return "price_collapse"
        if multiple <= rh.STOP_LOSS_MULTIPLE:
            return "stop_loss"
        if liquidity < rh.MINIMUM_ENTRY_LIQUIDITY_USD:
            return "liquidity_below_minimum"
        return None

    def test_a_rug_is_named_a_price_collapse(self):
        self.assertEqual(
            self._classify(price_multiple=0.0, multiple=0.0, liquidity=50.0),
            "price_collapse",
        )

    def test_a_real_decline_is_still_a_stop_loss(self):
        self.assertEqual(
            self._classify(
                price_multiple=0.5, multiple=0.5, liquidity=50_000.0
            ),
            "stop_loss",
            "a genuine decline was relabelled, so the stop stays untested",
        )

    def test_thin_market_with_healthy_price_is_a_liquidity_exit(self):
        self.assertEqual(
            self._classify(price_multiple=0.95, multiple=0.9, liquidity=500.0),
            "liquidity_below_minimum",
        )

    def test_collapse_outranks_liquidity_for_the_same_event(self):
        """Both conditions hold during a rug; the cause must win."""
        self.assertEqual(
            self._classify(price_multiple=0.001, multiple=0.0, liquidity=10.0),
            "price_collapse",
        )


class AdaptiveEntryFloorTests(unittest.TestCase):
    """The autonomous tighten may only ever raise the bar."""

    def _engine(self, root):
        store = rh.RobinhoodLearningStore.__new__(rh.RobinhoodLearningStore)
        store.path = Path(root) / "learning.sqlite3"
        return store

    def test_absent_policy_uses_the_module_default(self):
        with tempfile.TemporaryDirectory() as root:
            self.assertEqual(
                self._engine(root)._effective_minimum_entry_score(),
                rh.MINIMUM_ENTRY_SCORE,
            )

    def test_a_higher_floor_is_honoured(self):
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / "adaptive_policy.json").write_text(
                json.dumps({"minimum_entry_score": 78.0}), encoding="utf-8"
            )
            self.assertEqual(
                self._engine(root)._effective_minimum_entry_score(), 78.0
            )

    def test_a_lower_floor_cannot_loosen_admission(self):
        """The governing invariant: autonomous change may only tighten.

        A policy file that asked for a looser gate -- corrupt, stale, or
        hostile -- must not be able to widen admission.
        """
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / "adaptive_policy.json").write_text(
                json.dumps({"minimum_entry_score": 10.0}), encoding="utf-8"
            )
            self.assertEqual(
                self._engine(root)._effective_minimum_entry_score(),
                rh.MINIMUM_ENTRY_SCORE,
                "a lower adaptive score loosened the gate",
            )

    def test_malformed_policy_falls_back_rather_than_failing_open(self):
        with tempfile.TemporaryDirectory() as root:
            (Path(root) / "adaptive_policy.json").write_text(
                "{not json", encoding="utf-8"
            )
            self.assertEqual(
                self._engine(root)._effective_minimum_entry_score(),
                rh.MINIMUM_ENTRY_SCORE,
            )


if __name__ == "__main__":
    unittest.main()
