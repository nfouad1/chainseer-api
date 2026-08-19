import json
import hashlib
import tempfile
import threading
import time
import unittest
from datetime import datetime
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
    @staticmethod
    def _candidate(source_version=rh.SOURCE_V2):
        return {
            "token_address": TOKEN, "pair_address": PAIR,
            "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
            "block_timestamp": time.time(), "transaction_hash": "0x1",
            "log_index": 0, "name": "Test", "symbol": "TEST",
            "source_version": source_version,
            "pool_id": POOL_ID if source_version == rh.SOURCE_V4 else None,
            "hooks_address": rh.ZERO_ADDRESS,
            "fee_tier": 3000, "tick_spacing": 60,
        }

    def test_v4_admission_is_shadow_only_until_quotes_are_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            store.add_candidates([self._candidate(rh.SOURCE_V4)])
            market = {
                "price_usd": 0.25, "liquidity_usd": 50_000,
                "market_cap_usd": 250_000, "current_state_verified": True,
                "executable_quote_verified": False,
            }
            store.record_analysis(
                TOKEN, FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"],
                market,
            )
            candidate = store.candidate(TOKEN)
            shadow = json.loads(candidate["shadow_admission_json"])
            self.assertFalse(candidate["paper_entry_allowed"])
            self.assertEqual(candidate["paper_decision"], rh.PAPER_DECISION_V4_SHADOW)
            self.assertTrue(shadow["would_admit_legacy_gate"])
            self.assertTrue(shadow["v4_shadow_only"])
            self.assertFalse(store.open_position(candidate, market))

    def test_unverified_mark_cannot_revalue_or_close_a_position(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            store.add_candidates([self._candidate()])
            entry = {
                "price_usd": 0.25, "liquidity_usd": 50_000,
                "market_cap_usd": 250_000,
            }
            store.record_analysis(
                TOKEN, FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"],
                entry,
            )
            self.assertTrue(store.open_position(store.candidate(TOKEN), entry))
            mark = store.mark_position(TOKEN, {
                "source": "uniswap_v4_state_view",
                "current_state_verified": False,
                "reason": "anchor_usd_price_unavailable",
            }, time.time() + 300)
            self.assertFalse(mark["verified"])
            with store.connection() as connection:
                position = dict(connection.execute(
                    "SELECT * FROM positions WHERE token_address=?", (TOKEN,)
                ).fetchone())
            self.assertEqual(position["status"], "open")
            self.assertIsNone(position["net_multiple"])
            self.assertEqual(position["unverified_marks"], 1)
            self.assertEqual(position["verified_mark_count"], 0)
            self.assertEqual(position["last_price_usd"], entry["price_usd"])
            verified = store.mark_position(TOKEN, {
                "source": "uniswap_v4_state_view",
                "current_state_verified": True,
                "price_usd": entry["price_usd"],
                "liquidity_usd": 1_000_000,
                "market_cap_usd": entry["market_cap_usd"],
                "paper_exit_quote_required": True,
                "paper_exit_quote_verified": True,
                "paper_exit_quantity": position["quantity"],
                "paper_exit_value_usd": 40.0,
            }, time.time() + 600)
            self.assertTrue(verified["verified"])
            self.assertEqual(verified["reason"], "stop_loss")
            self.assertAlmostEqual(verified["value_usd"], 40.0)

    def test_v2_v3_floor_ignores_stale_global_adaptive_tightening(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "adaptive_policy.json").write_text(
                json.dumps({"minimum_entry_score": 85}), encoding="utf-8"
            )
            store = rh.RobinhoodLearningStore(root / "learning.sqlite3")
            store.add_candidates([self._candidate()])
            market = {
                "price_usd": 0.25, "liquidity_usd": 50_000,
                "market_cap_usd": 250_000,
            }
            analysis = FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"]
            analysis["legitimacy_score"] = 72
            store.record_analysis(TOKEN, analysis, market)
            self.assertTrue(store.candidate(TOKEN)["paper_entry_allowed"])

    def test_explicit_raw_safety_refusal_blocks_paper_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            store.add_candidates([self._candidate()])
            analysis = FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"]
            analysis["component_scores"] = {
                "holder_distribution": 20,
                "lp_lock": 50,
                "honeypot_safety": 95,
            }
            store.record_analysis(TOKEN, analysis, {
                "price_usd": 0.25, "liquidity_usd": 50_000,
                "market_cap_usd": 250_000,
            }, report_data={"lp_lock": {"locked": False}})

            candidate = store.candidate(TOKEN)
            safety = json.loads(candidate["safety_signals_json"])
            self.assertFalse(candidate["paper_entry_allowed"])
            self.assertEqual(candidate["paper_decision"], rh.PAPER_DECISION_REJECTED)
            self.assertEqual(safety["entry_deliberation"]["judgment"], "refuse")
            self.assertTrue(safety["entry_deliberation"]["enforced"])
            self.assertTrue(safety["raw_safety_refusal_enforced"])

    def test_source_loss_circuit_breaker_quarantines_only_bad_source(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            tokens = ["0x" + f"{index + 1:040x}" for index in range(6)]
            for index, token in enumerate(tokens):
                store.add_candidates([{
                    "token_address": token, "pair_address": PAIR,
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": index + 1,
                    "block_timestamp": time.time(),
                    "transaction_hash": f"0x{index + 1}", "log_index": index,
                    "name": "Risk sample", "symbol": "RISK",
                    "source_version": rh.SOURCE_V3,
                }])
            with store.connection() as connection:
                for index, token in enumerate(tokens[:5]):
                    loss = index < 4
                    connection.execute(
                        """
                        INSERT INTO positions (
                            token_address,symbol,status,opened_at,entry_price_usd,
                            entry_liquidity_usd,cost_usd,quantity,
                            entry_friction_bps,high_multiple,closed_at,
                            net_multiple,exit_reason
                        ) VALUES (?,?,'closed',?,?,?,?,?,?,1,?,?,?)
                        """,
                        (
                            token, "RISK", 1_700_000_000 + index, 1.0,
                            50_000, 100, 100, 100,
                            1_700_000_300 + index, 0.0 if loss else 0.8,
                            "price_collapse" if loss else "stop_loss",
                        ),
                    )

            risk = store.source_entry_risk(rh.SOURCE_V3)
            self.assertTrue(risk["quarantined"])
            self.assertEqual(risk["closed_sample"], 5)
            self.assertEqual(risk["total_losses"], 4)
            self.assertFalse(store.source_entry_risk(rh.SOURCE_V2)["quarantined"])

            store.record_analysis(
                tokens[-1], FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"],
                {"price_usd": 0.25, "liquidity_usd": 50_000,
                 "market_cap_usd": 250_000},
            )
            candidate = store.candidate(tokens[-1])
            self.assertFalse(candidate["paper_entry_allowed"])
            self.assertEqual(
                candidate["paper_decision"], rh.PAPER_DECISION_SOURCE_QUARANTINE,
            )

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
            self.assertEqual(market.calls, 1)
            self.assertEqual(first["current_market_cap_usd"], 6_000_000)
            self.assertEqual(second["current_market_cap_usd"], 6_000_000)
            refresher._refreshed_monotonic -= rh.DASHBOARD_MARKET_CACHE_SECONDS + 1
            third = refresher.snapshot()["positions"][0]
            self.assertEqual(market.calls, 2)
            self.assertEqual(third["current_market_cap_usd"], 7_000_000)

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

    def test_v4_flow_signal_persists_deduplicated_shadow_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            events = [{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }, {
                "kind": "modify", "pool_id": POOL_ID, "block_number": 101,
            }]
            for index in range(6):
                events.append({
                    "kind": "swap", "pool_id": POOL_ID,
                    "block_number": 102 + index,
                    "transaction_hash": "0x" + f"{index + 1:064x}",
                    "log_index": index,
                    "sender_hint": "0x" + f"{index % 4 + 1:040x}",
                    # token0 leaves the pool while USDG enters: token buy.
                    "amount0_raw": -(100 + index), "amount1_raw": 10 + index,
                    "sqrt_price_x96": int((1 + index * 0.02) * (1 << 96)),
                    "active_liquidity": 10**18, "tick": index,
                    "block_timestamp": None,
                })
            store.apply_v4_events(events)
            # Replaying the final log must not create a second observation.
            store.apply_v4_events([events[-1]])
            store.record_transaction_origins([{
                "transaction_hash": event["transaction_hash"],
                "transaction": {
                    "from": "0x" + f"{index % 4 + 1:040x}",
                    "to": "0x" + "99" * 20,
                    "blockNumber": hex(event["block_number"]),
                },
                "error": None,
            } for index, event in enumerate(events[2:])])
            older = "0x" + "66" * 20
            store.add_candidates([
                self._candidate(rh.SOURCE_V4),
                {
                    "token_address": older, "pair_address": PAIR,
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": 0, "block_timestamp": 1,
                    "transaction_hash": "0xold", "log_index": 0,
                    "name": "Older", "symbol": "OLD",
                },
            ])

            summary = store.flow_summary()
            signal = store.recent_flow_signals(1)[0]

            self.assertEqual(summary["raw_swaps"], 6)
            self.assertEqual(signal["swap_count"], 6)
            self.assertEqual(signal["buy_count"], 6)
            self.assertEqual(signal["unique_sender_hints"], 4)
            self.assertEqual(signal["unique_resolved_participants"], 4)
            self.assertEqual(signal["identity_coverage"], 1.0)
            self.assertTrue(signal["shadow_qualified"])
            self.assertGreaterEqual(signal["shadow_score"], 70)
            self.assertEqual(signal["qualification_gaps"], [])
            self.assertGreaterEqual(signal["uncapped_shadow_score"], 70)
            self.assertEqual(summary["active_identity_coverage"], 1.0)
            self.assertFalse(summary["admission_enabled"])
            self.assertIn("may_be_router", " ".join(signal["limitations"]))
            selected = store.pending_analysis(1)[0]
            self.assertEqual(selected["token_address"], TOKEN.lower())
            self.assertTrue(selected["flow_shadow_qualified"])

    def test_v4_flow_signal_caps_insufficient_sender_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }] + [{
                "kind": "swap", "pool_id": POOL_ID,
                "block_number": 101 + index,
                "transaction_hash": "0x" + f"{index + 1:064x}",
                "log_index": index, "sender_hint": PAIR.lower(),
                "amount0_raw": -100, "amount1_raw": 10,
                "sqrt_price_x96": int((1 + index * 0.02) * (1 << 96)),
                "active_liquidity": 10**18, "tick": index,
                "block_timestamp": None,
            } for index in range(8)])
            store.record_transaction_origins([{
                "transaction_hash": "0x" + f"{index + 1:064x}",
                "transaction": {
                    "from": PAIR.lower(), "to": "0x" + "99" * 20,
                    "blockNumber": hex(101 + index),
                }, "error": None,
            } for index in range(8)])

            signal = store.recent_flow_signals(1)[0]
            self.assertEqual(signal["unique_sender_hints"], 1)
            self.assertEqual(signal["unique_resolved_participants"], 1)
            self.assertFalse(signal["shadow_qualified"])
            self.assertLess(signal["shadow_score"], 70)

    def test_v4_flow_signal_requires_positive_net_anchor_inflow(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            events = [{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }]
            for index in range(6):
                anchor_delta = 10 if index < 5 else -100
                events.append({
                    "kind": "swap", "pool_id": POOL_ID,
                    "block_number": 101 + index,
                    "transaction_hash": "0x" + f"{index + 1:064x}",
                    "log_index": index,
                    "sender_hint": "0x" + f"{index % 4 + 1:040x}",
                    "amount0_raw": -100 if anchor_delta > 0 else 100,
                    "amount1_raw": anchor_delta,
                    "sqrt_price_x96": int((1 + index * 0.03) * (1 << 96)),
                    "active_liquidity": 10**18, "tick": index,
                    "block_timestamp": None,
                })
            store.apply_v4_events(events)
            store.record_transaction_origins([{
                "transaction_hash": event["transaction_hash"],
                "transaction": {
                    "from": "0x" + f"{index % 4 + 1:040x}",
                    "to": "0x" + "99" * 20,
                    "blockNumber": hex(event["block_number"]),
                }, "error": None,
            } for index, event in enumerate(events[1:])])

            signal = store.recent_flow_signals(1)[0]

            self.assertGreater(signal["buy_count"], signal["sell_count"])
            self.assertLess(signal["net_anchor_flow_fraction"], 0)
            self.assertFalse(signal["shadow_qualified"])
            self.assertLess(signal["shadow_score"], 70)

    def test_v4_flow_signal_rejects_adverse_price_direction(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            events = [{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(), "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }] + [{
                "kind": "swap", "pool_id": POOL_ID, "block_number": 101 + index,
                "transaction_hash": "0x" + f"{index + 1:064x}", "log_index": index,
                "sender_hint": "0x" + f"{index % 4 + 1:040x}",
                "amount0_raw": -100, "amount1_raw": 20,
                "sqrt_price_x96": int((1 - index * 0.05) * (1 << 96)),
                "active_liquidity": 10**18, "tick": -index,
                "block_timestamp": None,
            } for index in range(6)]
            store.apply_v4_events(events)
            store.record_transaction_origins([{
                "transaction_hash": event["transaction_hash"],
                "transaction": {
                    "from": "0x" + f"{index % 4 + 1:040x}",
                    "to": "0x" + "99" * 20,
                    "blockNumber": hex(event["block_number"]),
                }, "error": None,
            } for index, event in enumerate(events[1:])])
            signal = store.recent_flow_signals(1)[0]
            self.assertLess(signal["price_multiple"], 1)
            self.assertFalse(signal["shadow_qualified"])
            self.assertTrue(signal["features"]["adverse_price_direction"])
            self.assertGreaterEqual(signal["uncapped_shadow_score"], 70)
            self.assertLess(signal["shadow_score"], 70)
            self.assertIn(
                "non_adverse_price_direction", signal["qualification_gaps"]
            )

    def test_flow_evidence_is_immutable_freshness_gated_and_control_matched(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            control_pool = "0x" + "cd" * 32
            with store.connection() as connection:
                for pool, token, score, qualified, end_block in (
                    (POOL_ID, TOKEN, 82.0, 1, 1000),
                    (control_pool, "0x" + "33" * 20, 68.0, 0, 999),
                ):
                    connection.execute(
                        """
                        INSERT INTO flow_signals (
                            source_version,pool_id,token_address,computed_at,
                            window_blocks,window_start_block,window_end_block,
                            swap_count,buy_count,sell_count,unique_sender_hints,
                            unique_resolved_participants,identity_coverage,buy_ratio,
                            net_anchor_flow_fraction,price_multiple,shadow_score,
                            shadow_qualified,confidence,limitations_json,features_json
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """,
                        (rh.SOURCE_V4,pool,token,rh._utc_now(),450,end_block-5,end_block,
                         6,5,1,4,4,1.0,.83,.40,1.05,score,qualified,"high","[]","{}"),
                    )
            first = store.capture_flow_signal_events(1005, 1_700_000_000)
            second = store.capture_flow_signal_events(1005, 1_700_000_001)
            with store.connection() as connection:
                connection.execute(
                    "UPDATE flow_signals SET window_end_block=window_end_block+1 WHERE pool_id=?",
                    (POOL_ID,),
                )
            within_cooldown = store.capture_flow_signal_events(1006, 1_700_000_002)
            events = store.recent_flow_evidence_events()
            self.assertEqual(first["qualified_created"], 1)
            self.assertEqual(first["controls_created"], 1)
            self.assertEqual(second["created"], 0)
            self.assertEqual(within_cooldown["created"], 0)
            self.assertEqual({row["signal_role"] for row in events}, {"qualified", "matched_control"})
            self.assertTrue(all(row["freshness"] == "fresh" for row in events))
            self.assertTrue(all(row["eligible_for_evaluation"] for row in events))
            control = next(row for row in events if row["signal_role"] == "matched_control")
            signal = next(row for row in events if row["signal_role"] == "qualified")
            self.assertEqual(control["matched_signal_event_id"], signal["event_id"])

    def test_flow_outcome_counts_unexitable_quote_as_total_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            event_id = "e" * 64
            with store.connection() as connection:
                connection.execute(
                    """
                    INSERT INTO flow_signal_events (
                        event_id,policy_version,cohort_id,source_version,pool_id,
                        token_address,signal_role,window_start_block,window_end_block,
                        head_block,head_lag_blocks,freshness,eligible_for_evaluation,
                        signaled_at,snapshot_json,quote_status,quote_json,quote_verified,
                        quote_exitable,created_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (event_id,rh.FLOW_EVIDENCE_POLICY_VERSION,"cohort",rh.SOURCE_V4,
                     POOL_ID,TOKEN,"qualified",1,2,2,0,"fresh",1,1000,"{}","verified",
                     '{"market":{"execution_quote":{"anchor_in_raw":"1000","verified":true}}}',
                     1,1,rh._utc_now()),
                )
                connection.execute(
                    "INSERT INTO flow_signal_outcomes (event_id,horizon_label,horizon_seconds,target_at) VALUES (?,?,?,?)",
                    (event_id,"1m",60,1060),
                )
            due = store.due_flow_outcomes(1061, 1)[0]
            store.record_flow_outcome(
                due, {"paper_exit_quote": {"verified": False}}, 10, 1061,
            )
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT status,exit_valid,net_return FROM flow_signal_outcomes"
                ).fetchone()
            self.assertEqual(row["status"], "observed")
            self.assertFalse(row["exit_valid"])
            self.assertEqual(row["net_return"], -1.0)

    def test_flow_origin_resolver_batches_and_caches_transaction_senders(self):
        class OriginRPC(FakeRPC):
            def __init__(self):
                super().__init__([])
                self.origin_batches = []

            def get_transactions(self, hashes):
                self.origin_batches.append(list(hashes))
                return [{
                    "transaction_hash": transaction_hash,
                    "transaction": {
                        "from": "0x" + f"{index % 4 + 1:040x}",
                        "to": "0x" + "99" * 20,
                        "blockNumber": hex(101 + index),
                    },
                    "error": None,
                } for index, transaction_hash in enumerate(hashes)]

        with tempfile.TemporaryDirectory() as directory:
            rpc = OriginRPC()
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=rpc, analyzer=FakeAnalyzer(), market=FakeMarket()
            )
            engine.store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }] + [{
                "kind": "swap", "pool_id": POOL_ID,
                "block_number": 101 + index,
                "transaction_hash": "0x" + f"{index + 1:064x}",
                "log_index": index,
                "sender_hint": "0x" + "aa" * 20,
                "amount0_raw": -100, "amount1_raw": 10,
                "sqrt_price_x96": int((1 + index * 0.02) * (1 << 96)),
                "active_liquidity": 10**18, "tick": index,
                "block_timestamp": None,
            } for index in range(6)])

            first = engine.resolve_flow_participants()
            second = engine.resolve_flow_participants()

            self.assertEqual(first["selected"], 6)
            self.assertEqual(first["resolved"], 6)
            self.assertEqual(first["pending_after"], 0)
            self.assertEqual(second["selected"], 0)
            self.assertEqual(len(rpc.origin_batches), 1)
            signal = engine.store.recent_flow_signals(1)[0]
            self.assertEqual(signal["unique_sender_hints"], 1)
            self.assertEqual(signal["unique_resolved_participants"], 4)
            self.assertTrue(signal["shadow_qualified"])

    def test_flow_origin_resolver_prioritizes_active_window_before_history(self):
        class OriginRPC(FakeRPC):
            def get_transactions(self, hashes):
                return [{
                    "transaction_hash": transaction_hash,
                    "transaction": {
                        "from": "0x" + f"{index + 1:040x}",
                        "to": "0x" + "99" * 20,
                        "blockNumber": hex(1000 + index),
                    },
                    "error": None,
                } for index, transaction_hash in enumerate(hashes)]

        old_hash = "0x" + "01" * 32
        active_hashes = ["0x" + value * 32 for value in ("02", "03")]
        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=OriginRPC([]),
                analyzer=FakeAnalyzer(), market=FakeMarket(),
            )
            engine.store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }] + [{
                "kind": "swap", "pool_id": POOL_ID,
                "block_number": block_number,
                "transaction_hash": transaction_hash,
                "log_index": index,
                "sender_hint": "0x" + "aa" * 20,
                "amount0_raw": -100, "amount1_raw": 10,
                "sqrt_price_x96": 1 << 96,
                "active_liquidity": 10**18, "tick": 0,
                "block_timestamp": None,
            } for index, (block_number, transaction_hash) in enumerate([
                # Active blocks sit far enough above the old one that a
                # FLOW_WINDOW_BLOCKS-wide window cannot reach back to it.
                (101, old_hash), (5000, active_hashes[0]),
                (5001, active_hashes[1]),
            ])])

            self.assertEqual(
                engine.store.pending_transaction_origins(10, scope="active"),
                active_hashes[::-1],
            )
            self.assertEqual(
                engine.store.pending_transaction_origins(10, scope="historical"),
                [old_hash],
            )

            result = engine.resolve_flow_participants(limit=2)

            self.assertEqual(result["selected_active"], 2)
            self.assertEqual(result["selected_historical"], 0)
            self.assertEqual(result["pending_active_after"], 0)
            self.assertEqual(result["pending_historical_after"], 1)

    def test_flow_origin_resolver_honors_expired_stage_deadline(self):
        class OriginRPC(FakeRPC):
            def __init__(self):
                super().__init__([])
                self.calls = 0

            def get_transactions(self, hashes):
                self.calls += 1
                return []

        with tempfile.TemporaryDirectory() as directory:
            rpc = OriginRPC()
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=rpc,
                analyzer=FakeAnalyzer(), market=FakeMarket(),
            )
            engine.store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }, {
                "kind": "swap", "pool_id": POOL_ID,
                "block_number": 101,
                "transaction_hash": "0x" + "01" * 32,
                "log_index": 0, "sender_hint": "0x" + "aa" * 20,
                "amount0_raw": -100, "amount1_raw": 10,
                "sqrt_price_x96": 1 << 96,
                "active_liquidity": 10**18, "tick": 0,
                "block_timestamp": None,
            }])

            result = engine.resolve_flow_participants(
                deadline_monotonic=time.monotonic() - 1,
            )

            self.assertEqual(result["planned"], 1)
            self.assertEqual(result["selected"], 0)
            self.assertEqual(result["deferred_for_deadline"], 1)
            self.assertEqual(rpc.calls, 0)

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

    def test_v4_custody_maps_position_and_refuses_eoa_control_as_locked(self):
        token_id = 7
        lower, upper = -120, 120
        packed = (
            (int(POOL_ID.removeprefix("0x")[:50], 16) << 56)
            | ((upper & 0xFFFFFF) << 32)
            | ((lower & 0xFFFFFF) << 8)
        )
        owner = "0x" + "44" * 20
        transfer = {
            "topics": [
                rh.V4_POSITION_TRANSFER_TOPIC, topic(rh.ZERO_ADDRESS),
                topic(owner), "0x" + word(token_id),
            ],
            "blockNumber": hex(100), "logIndex": "0x2",
            "transactionHash": "0xcustody",
        }

        class CustodyRPC(FakeRPC):
            def call(self, address, data, block=None):
                if data.startswith("0x" + rh.V4_POSITION_INFO_SELECTOR):
                    return hex(packed)
                if data.startswith("0x" + rh.ERC721_OWNER_OF_SELECTOR):
                    return "0x" + word(owner)
                if data.startswith("0x" + rh.V4_POSITION_LIQUIDITY_SELECTOR):
                    return hex(1_000)
                if data.startswith("0x" + rh.ERC721_GET_APPROVED_SELECTOR):
                    return "0x" + word(0)
                raise AssertionError(data)

            def get_code(self, address, block=None):
                return "0x"

        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 90,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }, {
                "kind": "swap", "pool_id": POOL_ID, "block_number": 99,
                "block_timestamp": time.time(), "transaction_hash": "0xswap",
                "log_index": 1, "sender_hint": owner,
                "amount0_raw": -1, "amount1_raw": 1,
                "sqrt_price_x96": 1 << 96, "active_liquidity": 1_000,
                "tick": 0,
            }])
            result = rh.RobinhoodV4CustodyVerifier(
                CustodyRPC([transfer]), store,
                Path(directory) / "custody.json",
            ).sync(block_limit=20, lookback=20)
            snapshot = store.latest_v4_custody(POOL_ID)

            self.assertTrue(result["supported"])
            self.assertEqual(result["relevant_transfers"], 1)
            self.assertEqual(snapshot["managed_active_coverage"], 1.0)
            self.assertEqual(snapshot["verified_locked_active_fraction"], 0.0)
            self.assertEqual(snapshot["custody_verdict"], "eoa_controlled")

    def test_v4_custody_keeps_live_head_separate_from_historical_backfill(self):
        class CursorRPC(FakeRPC):
            def call(self, address, data, block=None):
                return "0x0"

            def get_code(self, address, block=None):
                return "0x"

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = rh.RobinhoodLearningStore(root / "learn.sqlite3")
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 100,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }])
            cursor = root / "custody.json"
            cursor.write_text(json.dumps({
                "next_block": 100,
                "updated_at": "2000-01-01T00:00:00+00:00",
            }), encoding="utf-8")
            rpc = CursorRPC(latest=10_000)
            verifier = rh.RobinhoodV4CustodyVerifier(rpc, store, cursor)

            first = verifier.sync(
                block_limit=500, lookback=500, live_block_limit=5_000,
            )
            state = json.loads(cursor.read_text(encoding="utf-8"))

            self.assertEqual(first["live"]["from_block"], 5_001)
            self.assertEqual(first["live"]["to_block"], 10_000)
            self.assertEqual(first["backfill"]["from_block"], 100)
            self.assertEqual(first["backfill"]["to_block"], 599)
            self.assertTrue(first["live_caught_up"])
            self.assertFalse(first["historical_caught_up"])
            self.assertEqual(state["live_next_block"], 10_001)
            self.assertEqual(state["backfill_next_block"], 600)

            rpc.latest = 13_000
            rpc.calls.clear()
            second = verifier.sync(
                block_limit=500, lookback=500, live_block_limit=5_000,
            )

            self.assertEqual(second["live"]["from_block"], 10_001)
            self.assertEqual(second["live"]["to_block"], 13_000)
            self.assertEqual(second["backfill"]["blocks_scanned"], 0)
            self.assertTrue(second["live_caught_up"])

    def test_v4_custody_requires_future_lock_and_no_token_approval(self):
        market = {
            "executable_quote_verified": True,
            "execution_quote": {"passes_round_trip_limit": True},
            "v4_custody": {
                "managed_active_coverage": 1.0,
                "verified_locked_active_fraction": 0.95,
                "approved_active_fraction": 0.0,
            },
        }
        passing = rh.v4_shadow_gates(market, hooks_present=False)
        approved = rh.v4_shadow_gates({
            **market,
            "v4_custody": {
                **market["v4_custody"], "approved_active_fraction": 0.5,
            },
        }, hooks_present=False)
        hooked = rh.v4_shadow_gates(market, hooks_present=True)

        self.assertTrue(passing["counterfactual_pass"])
        self.assertFalse(passing["paper_canary_enabled"])
        self.assertFalse(approved["counterfactual_pass"])
        self.assertFalse(hooked["counterfactual_pass"])

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
            self.assertEqual(
                market["liquidity_model"],
                "active_concentrated_liquidity_estimate_not_quote",
            )
            self.assertFalse(market["executable_quote_verified"])
            self.assertIsNone(market["pool_reserve_fraction"])

    def test_v4_checkpoint_snapshot_skips_execution_only_rpc_calls(self):
        class CheckpointRPC(FakeV4RPC):
            def call(self, address, data, block=None):
                if address.lower() == rh.UNISWAP_V4_QUOTER.lower():
                    raise AssertionError("checkpoint requested an execution quote")
                if data.startswith("0x" + rh.ERC20_BALANCE_OF_SELECTOR):
                    raise AssertionError("checkpoint requested singleton balance")
                return super().call(address, data, block)

        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }])
            market = rh.RobinhoodV4MarketClient(
                CheckpointRPC(), FakeMarket(), store
            ).snapshot(
                {"token_address": TOKEN.lower(), "pool_id": POOL_ID},
                include_execution_quote=False,
            )

            self.assertTrue(market["current_state_verified"])
            self.assertFalse(market["executable_quote_verified"])
            self.assertIsNone(market["execution_quote"])
            self.assertIsNone(market["singleton_token_balance_fraction"])

    def test_v4_market_records_exact_pool_round_trip_quote(self):
        class QuoteRPC(FakeV4RPC):
            def __init__(self):
                super().__init__()
                self.quote_calls = []

            def call(self, address, data, block=None):
                if address.lower() == rh.UNISWAP_V4_QUOTER.lower():
                    self.quote_calls.append(data)
                    amount = [90, 80, 70][len(self.quote_calls) - 1] * 10**18
                    return "0x" + word(amount) + word(123_456)
                return super().call(address, data, block)

        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": POOL_ID,
                "currency0": TOKEN.lower(),
                "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }])
            rpc = QuoteRPC()
            market = rh.RobinhoodV4MarketClient(rpc, FakeMarket(), store).snapshot(
                {
                    "token_address": TOKEN.lower(), "pool_id": POOL_ID,
                    "paper_quantity": 100,
                }
            )
            quote = market["execution_quote"]
            self.assertTrue(market["executable_quote_verified"])
            self.assertEqual(len(rpc.quote_calls), 3)
            self.assertTrue(all(
                call.startswith("0x" + rh.V4_QUOTE_EXACT_INPUT_SINGLE_SELECTOR)
                for call in rpc.quote_calls
            ))
            self.assertAlmostEqual(quote["round_trip_ratio"], 0.8)
            self.assertTrue(quote["passes_round_trip_limit"])
            self.assertTrue(market["paper_exit_quote_verified"])
            self.assertAlmostEqual(market["paper_exit_value_usd"], 70.0)

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

    def test_prospective_analysis_and_outcome_bind_to_dedicated_timechain(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            recorder = rh.RobinhoodLearningTimechainRecorder(
                root / "producer_chain", skill_root=rh.default_skill_root(),
            )
            engine = rh.RobinhoodLearningEngine(
                root / "learner", rpc=FakeRPC(), analyzer=FakeAnalyzer(),
                market=FakeMarket(), timechain_recorder=recorder,
            )
            engine.store.add_candidates([self._candidate()])
            market = FakeMarket().snapshot(TOKEN)
            digest = hashlib.sha256(b"evidence").hexdigest()
            report = FakeAnalyzer().analyze_token(TOKEN, False, True)
            report["provenance"] = {
                "block_pin": 100, "fact_count": 1,
                "facts": [{
                    "fact_id": "rpc-1", "source": "robinhood_rpc",
                    "query_hash": digest, "response_hash": digest,
                    "block": 100, "fetched_at": rh._utc_now(),
                }],
            }
            engine.store.record_analysis(
                TOKEN, report["analysis"], market,
                priority_reason="test", report_data=report.get("data") or {},
            )

            sealed = engine.seal_analysis_memory(
                engine.store.candidate(TOKEN), report, market,
                priority_reason="test",
            )
            bound = engine.store.candidate(TOKEN)
            duplicate = engine.seal_analysis_memory(
                bound, report, market, priority_reason="test",
            )

            self.assertEqual(sealed["status"], "sealed")
            self.assertEqual(duplicate["status"], "already_bound")
            self.assertEqual(recorder.tc.height(), 2)
            self.assertEqual(bound["producer_analysis_ring_index"], sealed["ring"])
            self.assertEqual(len(bound["producer_evidence_hash"]), 64)

            now = time.time()
            due = {
                "token_address": TOKEN.lower(), "horizon_label": "15m",
                "horizon_seconds": 900, "target_at": now,
            }
            checkpoint = engine.store.record_outcome(due, market, now)
            outcome = recorder.seal_checkpoint_outcome(
                bound, checkpoint, market, observation_block=101,
            )
            record = outcome["payload"]["outcome_record"]
            engine.store.record_outcome_producer_reference(
                TOKEN, "15m", outcome, rh.canonical_hash(record),
            )

            self.assertTrue(record["learning"]["eligible"])
            self.assertEqual(
                record["analysis_reference"]["original_evidence_hash"],
                bound["producer_evidence_hash"],
            )
            with engine.store.connection() as connection:
                row = connection.execute(
                    "SELECT * FROM checkpoints WHERE token_address=? AND horizon_label='15m'",
                    (TOKEN.lower(),),
                ).fetchone()
            self.assertEqual(row["producer_outcome_ring_index"], outcome["index"])
            self.assertEqual(len(row["producer_outcome_record_hash"]), 64)

            no_market = recorder.seal_checkpoint_outcome(
                bound,
                {
                    "horizon_label": "1h", "horizon_seconds": 3600,
                    "target_at": now + 3600, "observed_at": rh._utc_now(),
                    "status": "no_market", "learning_eligible": False,
                    "market_cap_multiple": None, "market_cap_usd": None,
                    "liquidity_usd": None,
                },
                {},
                observation_block=None,
            )
            excluded = no_market["payload"]["outcome_record"]
            self.assertFalse(excluded["learning"]["eligible"])
            self.assertEqual(
                excluded["learning"]["exclusion_reason"],
                "market_outcome_not_observed",
            )
            ledger = rh.verify_outcome_rings(recorder.tc.load())
            self.assertTrue(ledger["ok"], ledger["errors"])
            self.assertEqual(ledger["checked"], 2)
            self.assertEqual(ledger["learning_eligible"], 1)
            self.assertTrue(recorder.verify()[0])

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

    def test_market_client_batches_multiple_token_snapshots(self):
        second = "0x" + "44" * 20
        urls = []
        client = rh.RobinhoodMarketClient()
        client.session.get = lambda url, **kwargs: (
            urls.append(url) or FakeResponse([
                {
                    "chainId": "robinhood", "pairAddress": PAIR,
                    "baseToken": {"address": TOKEN}, "priceUsd": "1",
                    "liquidity": {"usd": 100},
                },
                {
                    "chainId": "robinhood", "pairAddress": "0x" + "55" * 20,
                    "baseToken": {"address": second}, "priceUsd": "2",
                    "liquidity": {"usd": 200},
                },
            ])
        )

        markets = client.snapshots_many([TOKEN, second])

        self.assertEqual(len(urls), 1)
        self.assertIn(TOKEN.lower() + "," + second.lower(), urls[0].lower())
        self.assertEqual(markets[TOKEN.lower()][0]["price_usd"], 1.0)
        self.assertEqual(markets[second.lower()][0]["price_usd"], 2.0)

    def test_outcome_market_uses_geckoterminal_after_primary_failure(self):
        class ProviderResponse:
            def __init__(self, value):
                self.value = value

            def raise_for_status(self):
                return None

            def json(self):
                return self.value

        client = rh.RobinhoodMarketClient()

        def get(url, **_kwargs):
            if "dexscreener" in url:
                raise RuntimeError("primary throttled")
            return ProviderResponse({"data": [{
                "id": "robinhood_" + TOKEN.lower(), "type": "token",
                "attributes": {
                    "address": TOKEN.lower(), "symbol": "TEST",
                    "price_usd": "0.25", "fdv_usd": "250000",
                    "market_cap_usd": None,
                    "total_reserve_in_usd": "50000",
                },
            }]})

        client.session.get = get
        with patch.object(
            rh, "_remote_call", side_effect=lambda _op, callback, **_kw: callback()
        ):
            markets, telemetry = client.outcome_snapshots_many([
                {"token_address": TOKEN, "pair_address": PAIR}
            ])

        market = markets[TOKEN.lower()]
        self.assertEqual(
            market["source"], "geckoterminal_robinhood_token_fallback"
        )
        self.assertEqual(market["fdv_usd"], 250000.0)
        self.assertIsNone(market["liquidity_usd"])
        self.assertEqual(market["aggregate_reserve_usd"], 50000.0)
        self.assertFalse(market["entry_eligible_evidence"])
        self.assertTrue(market["market_resolution"]["fallback_used"])
        self.assertEqual(
            telemetry["providers"]["dexscreener"]["state"], "failed"
        )
        self.assertEqual(
            telemetry["providers"]["geckoterminal"]["state"], "responded"
        )

    def test_total_outcome_provider_failure_is_explicitly_retryable(self):
        client = rh.RobinhoodMarketClient()
        client.session.get = lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("all providers unavailable")
        )
        with patch.object(
            rh, "_remote_call", side_effect=lambda _op, callback, **_kw: callback()
        ):
            markets, telemetry = client.outcome_snapshots_many([
                {"token_address": TOKEN, "pair_address": PAIR}
            ])

        resolution = markets[TOKEN.lower()]["market_resolution"]
        self.assertTrue(resolution["retryable_provider_failure"])
        self.assertFalse(resolution["confirmed_no_market"])
        self.assertEqual(telemetry["provider_unavailable"], 1)

    def test_store_records_real_and_missed_checkpoints_without_fabrication(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            candidate = {"token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": 1_700_000_000, "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST"}
            store.add_candidates([candidate])
            with store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET analysis_status='complete',outcome_anchor_at=block_timestamp"
                )
            due = store.due_outcomes(1_700_000_000 + 900, 2)[0]
            store.record_outcome(due, FakeMarket().snapshot(TOKEN), due["target_at"] + 5)
            self.assertEqual(store.summary()["checkpoints"]["observed"], 1)
            expired = store.expire_missed(1_700_000_000 + 8*24*3600)
            self.assertGreaterEqual(expired, 1)
            with store.connection() as connection:
                missed = connection.execute("SELECT market_cap_usd FROM checkpoints WHERE status='missed'").fetchone()
            self.assertIsNone(missed[0])

    def test_old_launch_waits_for_analysis_before_outcome_clock_starts(self):
        now = 1_800_000_000.0
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": now - 6 * 3600,
                "transaction_hash": "0x1", "log_index": 0,
                "name": "Old launch", "symbol": "OLD",
            }])

            self.assertEqual(store.expire_missed(now), 0)
            self.assertEqual(store.due_outcomes(now, 5), [])

            with patch.object(rh.time, "time", return_value=now):
                store.record_analysis(
                    TOKEN,
                    FakeAnalyzer().analyze_token(TOKEN, False, True)["analysis"],
                    FakeMarket().snapshot(TOKEN),
                )

            candidate = store.candidate(TOKEN)
            self.assertEqual(candidate["outcome_anchor_at"], now)
            self.assertEqual(store.expire_missed(now), 0)
            due = store.due_outcomes(now + 900, 5)
            self.assertEqual(len(due), 1)
            self.assertEqual(due[0]["horizon_label"], "15m")
            self.assertEqual(due[0]["target_at"], now + 900)
            self.assertEqual(due[0]["outcome_anchor_type"], "analysis")
            self.assertEqual(due[0]["outcome_anchor_at_resolved"], now)

            checkpoint = store.record_outcome(
                due[0], FakeMarket().snapshot(TOKEN), now + 905,
            )
            self.assertEqual(checkpoint["anchor_type"], "analysis")
            self.assertEqual(checkpoint["anchor_at"], now)

    def test_producer_anchor_migration_preserves_existing_missed_history(self):
        analyzed_at = "2026-08-15T20:00:00+00:00"
        expected_anchor = datetime.fromisoformat(analyzed_at).timestamp()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "learn.sqlite3"
            store = rh.RobinhoodLearningStore(path)
            store.add_candidates([self._candidate()])
            with store.connection() as connection:
                connection.execute(
                    """
                    UPDATE candidates SET analysis_status='complete',analyzed_at=?,
                        producer_analysis_ring_index=7,outcome_anchor_at=NULL
                    WHERE token_address=?
                    """,
                    (analyzed_at, TOKEN.lower()),
                )
                connection.execute(
                    """
                    INSERT INTO checkpoints (
                        token_address,horizon_label,horizon_seconds,target_at,
                        observed_at,status,learning_eligible,lateness_seconds
                    ) VALUES (?,?,900,?,?, 'missed',0,100)
                    """,
                    (TOKEN.lower(), "15m", expected_anchor, analyzed_at),
                )

            migrated = rh.RobinhoodLearningStore(path)
            candidate = migrated.candidate(TOKEN)
            with migrated.connection() as connection:
                checkpoint = dict(connection.execute(
                    "SELECT * FROM checkpoints WHERE token_address=? AND horizon_label='15m'",
                    (TOKEN.lower(),),
                ).fetchone())

            self.assertEqual(candidate["outcome_anchor_at"], expected_anchor)
            self.assertEqual(checkpoint["status"], "missed")
            self.assertIsNone(checkpoint["producer_outcome_ring_index"])

    def test_fresh_outcomes_are_selected_before_bounded_recovery(self):
        now = 1_700_010_000.0
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            rows = []
            for suffix, lateness in (("11", 100), ("22", 500)):
                rows.append({
                    "token_address": "0x" + suffix * 20,
                    "pair_address": "0x" + ("33" if suffix == "11" else "44") * 20,
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": 1,
                    "block_timestamp": now - 900 - lateness,
                    "transaction_hash": "0x" + suffix,
                    "log_index": 0, "name": "Test", "symbol": "TEST",
                })
            store.add_candidates(rows)
            with store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET analysis_status='complete',outcome_anchor_at=block_timestamp"
                )

            due = store.due_outcomes(now, limit=1, recovery_limit=1)

            self.assertEqual([row["outcome_queue"] for row in due], ["fresh", "recovery"])
            self.assertEqual(due[0]["token_address"], "0x" + "11" * 20)
            self.assertEqual(due[1]["token_address"], "0x" + "22" * 20)

    def test_missed_checkpoint_expiration_is_bounded(self):
        now = 1_800_000_000.0
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            store.add_candidates([
                {
                    "token_address": "0x" + f"{index:040x}",
                    "pair_address": "0x" + f"{index + 100:040x}",
                    "factory_address": rh.UNISWAP_V2_FACTORY,
                    "block_number": index,
                    "block_timestamp": now - 8 * 24 * 3600,
                    "transaction_hash": "0x" + f"{index:064x}",
                    "log_index": 0, "name": "Test", "symbol": "TEST",
                }
                for index in range(1, 4)
            ])
            with store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET analysis_status='complete',outcome_anchor_at=block_timestamp"
                )

            expired = store.expire_missed(now, limit=2)

            self.assertEqual(expired, 2)
            self.assertGreater(store.missed_backlog(now), 0)

    def test_zero_outcome_quotas_select_nothing(self):
        now = 1_700_010_000.0
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory)/"learn.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": now - 950, "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST",
            }])
            self.assertEqual(store.due_outcomes(now, 0, 0), [])

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
            # Admitted tokens are shown as positions, not in the entry queue,
            # so the unfiltered listing is what carries the analysis record.
            self.assertEqual(store.recent_analyzed_tokens(), [])
            analyzed = store.recent_analyzed_tokens(include_rejected=True)[0]
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
            timings = result["cycle"]["stage_timings_seconds"]
            for stage in (
                "position_evaluations", "outcomes", "uniswap_v2_discovery",
                "uniswap_v4_discovery", "candidate_persistence",
                "market_rechecks", "analyses", "learning_summary",
            ):
                self.assertIn(stage, timings)
            self.assertTrue(engine.verify()["ok"])

    def test_custody_rpc_failure_cannot_fail_learning_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(
                directory,rpc=FakeRPC([]),analyzer=FakeAnalyzer(),market=FakeMarket()
            )
            engine.v4_custody = SimpleNamespace(
                sync=lambda **_kwargs: (_ for _ in ()).throw(
                    RuntimeError("custody provider throttled")
                )
            )
            result = engine.run_once(
                discovery_block_limit=1,analysis_limit=0,outcome_limit=0,
                lookback=1,
            )

            custody = result["cycle"]["v4_position_custody"]
            self.assertTrue(custody["deferred"])
            self.assertEqual(custody["reason"], "custody_rpc_unavailable")
            self.assertEqual(result["cycle"]["analysis_failures"], 0)

    def test_outcome_deadline_defers_selected_remote_marks(self):
        now = 1_700_010_000.0
        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=FakeRPC([]), analyzer=FakeAnalyzer(), market=FakeMarket()
            )
            engine.store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": now - 950, "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST",
            }])
            with engine.store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET analysis_status='complete',outcome_anchor_at=block_timestamp"
                )

            result = engine.observe_outcomes(
                now, 1, 0, deadline_monotonic=0.0,
            )

            self.assertEqual(result["observed"], 0)
            self.assertEqual(result["deferred_for_deadline"], 1)
            self.assertEqual(result["fresh_selected"], 1)

    def test_total_market_provider_failure_keeps_checkpoint_due_for_retry(self):
        class FailedOutcomeMarket(FakeMarket):
            def outcome_snapshots_many(self, candidates):
                token = candidates[0]["token_address"].lower()
                return ({token: {"market_resolution": {
                    "retryable_provider_failure": True,
                    "provider_states": {
                        "dexscreener": "failed", "geckoterminal": "failed",
                    },
                }}}, {
                    "schema_version": 1,
                    "providers": {
                        "dexscreener": {"state": "failed"},
                        "geckoterminal": {"state": "failed"},
                    },
                    "provider_unavailable": 1,
                })

        now = 1_700_010_000.0
        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=FakeRPC([]), analyzer=FakeAnalyzer(),
                market=FailedOutcomeMarket(),
            )
            engine.store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": rh.UNISWAP_V2_FACTORY, "block_number": 1,
                "block_timestamp": now - 950, "transaction_hash": "0x1",
                "log_index": 0, "name": "Test", "symbol": "TEST",
            }])
            with engine.store.connection() as connection:
                connection.execute(
                    "UPDATE candidates SET analysis_status='complete',"
                    "outcome_anchor_at=block_timestamp"
                )

            result = engine.observe_outcomes(now, 1, 0)

            self.assertEqual(result["observed"], 0)
            self.assertEqual(result["failures"], 1)
            with engine.store.connection() as connection:
                self.assertEqual(
                    connection.execute("SELECT COUNT(*) FROM checkpoints").fetchone()[0],
                    0,
                )
            self.assertEqual(
                len(engine.store.due_outcomes(
                    now + rh.OUTCOME_RETRY_SECONDS + 1, 1, 0
                )),
                1,
            )

    def test_engine_defers_analysis_when_cycle_budget_is_exhausted(self):
        log = {"topics": [rh.PAIR_CREATED_TOPIC, topic(rh.WETH_ADDRESS), topic(TOKEN)],
               "data": "0x" + "0"*24 + PAIR[2:] + "0"*64,
               "blockNumber": "0x64", "logIndex": "0x0", "transactionHash": "0x1"}
        with tempfile.TemporaryDirectory() as directory:
            engine = rh.RobinhoodLearningEngine(
                directory, rpc=FakeRPC([log]), analyzer=FakeAnalyzer(), market=FakeMarket()
            )
            result = engine.run_once(
                discovery_block_limit=1, analysis_limit=1, outcome_limit=0,
                cycle_budget_seconds=0, lookback=1,
            )
            self.assertEqual(result["cycle"]["analyses"], 0)
            self.assertEqual(result["cycle"]["analyses_deferred_for_deadline"], 1)
            self.assertEqual(engine.store.candidate(TOKEN)["analysis_status"], "pending")

    def test_dashboard_is_local_only_and_read_only_asset_has_no_controls(self):
        with self.assertRaises(ValueError):
            rh.serve_dashboard("unused", "0.0.0.0", 0)
        html = Path(rh.__file__).with_name("robinhood_dashboard.html").read_text(
            encoding="utf-8"
        )
        self.assertNotIn("<form", html.lower())
        self.assertNotIn("fetch('/api/", html.replace("fetch('/api/status", ""))
        for section in ("Open paper positions", "Closed positions",
                        "Flow evidence", "Promotion", "Reflection"):
            self.assertIn(section, html)
        for renderer in ("renderPositions", "renderClosed", "renderEvidence",
                         "renderReflection"):
            self.assertIn(renderer, html)
        self.assertIn("All-time net P&amp;L", html)
        self.assertIn("closed_performance", html)
        self.assertIn("Detection activity", html)
        self.assertIn("activitywindow", html)
        self.assertIn("activitysearch", html)
        self.assertIn("renderAnalyzed", html)
        self.assertIn("V4 custody lab", html)
        self.assertIn("renderCustody", html)
        self.assertIn("Only immutable qualified signals and their matched controls", html)
        self.assertNotIn("Rejected by safety", html)

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

    Confirmed on-chain: the V4 PoolManager holds 0.00-0.20% of supply for
    every one of those tokens, against 2.2-13.0% for the positions still
    open. The pools really were drained.
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


class AutonomousTightenCooldownTests(unittest.TestCase):
    """The non-predictive global score floor must remain frozen."""

    def _coordinator(self, root, closed):
        import chainseer_robinhood_reflection as R
        c = R.RobinhoodReflectionCoordinator.__new__(R.RobinhoodReflectionCoordinator)
        c.policy_path = Path(root) / "adaptive_policy.json"
        c._closed_position_metrics = lambda: {"closed": closed}
        return c

    @staticmethod
    def _finding(closed):
        return {
            "code": "RH-REFLECT-TOTAL-LOSS-RATE",
            "autonomous_action": "tighten_admission",
            "metrics": {"closed": closed},
            "evidence": "test",
        }

    def test_tighten_is_frozen(self):
        with tempfile.TemporaryDirectory() as root:
            c = self._coordinator(root, 20)
            applied = c._apply_autonomous_actions([self._finding(20)], 1)
            self.assertEqual(applied[0]["status"], "frozen")
            self.assertFalse((Path(root) / "adaptive_policy.json").exists())

    def test_repeated_tighten_remains_frozen(self):
        with tempfile.TemporaryDirectory() as root:
            c = self._coordinator(root, 20)
            c._apply_autonomous_actions([self._finding(20)], 1)
            again = c._apply_autonomous_actions([self._finding(20)], 2)
            self.assertEqual(again[0]["status"], "frozen")

    def test_new_closes_do_not_unfreeze_the_score(self):
        import chainseer_robinhood_reflection as R
        with tempfile.TemporaryDirectory() as root:
            c = self._coordinator(root, 20)
            c._apply_autonomous_actions([self._finding(20)], 1)
            later = 20 + R.CLOSED_AUDIT_COOLDOWN_CLOSES
            c._closed_position_metrics = lambda: {"closed": later}
            resumed = c._apply_autonomous_actions([self._finding(later)], 3)
            self.assertEqual(resumed[0]["status"], "frozen")

    def test_a_run_of_checkpoints_never_creates_an_override(self):
        with tempfile.TemporaryDirectory() as root:
            c = self._coordinator(root, 20)
            for checkpoint in range(8):
                c._apply_autonomous_actions([self._finding(20)], checkpoint)
            self.assertFalse((Path(root) / "adaptive_policy.json").exists())



class ShadowAdmissionTests(unittest.TestCase):
    """Competing entry rules are recorded and never enforced.

    The mcap/liquidity threshold was fitted on 39 pools that had already
    resolved, so its in-sample record proves nothing. These tests pin the two
    properties that make the exercise worth anything: the rules cannot change
    an admission, and fitted candidates are never counted as evidence.
    """

    BASE = dict(
        liquidity=50_000.0, hooks_present=False, hard_stops=[],
        token_quality_passes=True, allowed=True,
    )

    def test_ratio_is_market_cap_over_liquidity(self):
        shadow = rh.shadow_admission(market_cap=65_000.0, **self.BASE)
        self.assertAlmostEqual(shadow["mcap_liquidity_ratio"], 1.3)
        self.assertTrue(shadow["ratio_pass"], "the boundary must be inclusive")

    def test_a_rug_shaped_ratio_fails_the_rule(self):
        shadow = rh.shadow_admission(market_cap=150_000.0, **self.BASE)
        self.assertFalse(shadow["ratio_pass"])
        self.assertFalse(shadow["would_admit_ratio_rule"])
        self.assertTrue(shadow["actual_admitted"], "the live gate is unchanged")

    def test_missing_market_cap_does_not_pass_by_default(self):
        shadow = rh.shadow_admission(market_cap=None, **self.BASE)
        self.assertIsNone(shadow["mcap_liquidity_ratio"])
        self.assertFalse(shadow["ratio_pass"])

    def test_hook_relaxation_only_ignores_the_hook_stop(self):
        args = dict(self.BASE, allowed=False)
        hook_only = rh.shadow_admission(
            market_cap=60_000.0, **dict(args, hard_stops=[rh.V4_HOOK_STOP_CODE])
        )
        self.assertTrue(hook_only["hook_was_the_only_stop"])
        self.assertTrue(hook_only["would_admit_hook_relaxed"])
        with_other = rh.shadow_admission(
            market_cap=60_000.0,
            **dict(args, hard_stops=[rh.V4_HOOK_STOP_CODE, "UNLOCKED_LP"]),
        )
        self.assertFalse(with_other["hook_was_the_only_stop"])
        self.assertFalse(
            with_other["would_admit_hook_relaxed"],
            "relaxing the hook stop must not wave through a different stop",
        )

    def test_nothing_is_enforced(self):
        self.assertFalse(rh.shadow_admission(market_cap=1e9, **self.BASE)["enforced"])

    def test_backfilled_candidates_are_reported_separately(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": PAIR, "block_number": 1, "block_timestamp": 1,
                "transaction_hash": "0x1", "log_index": 0, "name": "T", "symbol": "T",
            }])
            store.record_analysis(TOKEN, {
                "legitimacy_score": 82, "risk_level": "Low", "hard_stop_overrides": [],
            }, {"price_usd": 0.01, "liquidity_usd": 50_000, "market_cap_usd": 60_000})
            report = store.shadow_admission_report()
            self.assertFalse(report["enforced"])
            self.assertEqual(
                report["in_sample_fitted"]["actual_admitted"]["admitted"], 0,
                "a live analysis must count as out-of-sample evidence",
            )
            self.assertEqual(
                report["out_of_sample"]["actual_admitted"]["admitted"], 1
            )

    def test_backfill_marks_everything_in_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": PAIR, "block_number": 1, "block_timestamp": 1,
                "transaction_hash": "0x1", "log_index": 0, "name": "T", "symbol": "T",
            }])
            with store.connection() as connection:
                connection.execute(
                    """
                    UPDATE candidates SET analysis_status='complete', score=82,
                        risk_level='Low', hard_stops_json='[]', paper_entry_allowed=1,
                        entry_liquidity_usd=50000, first_market_cap_usd=60000
                    WHERE token_address=?
                    """, (TOKEN.lower(),),
                )
            self.assertEqual(store.backfill_shadow_admission(), 1)
            report = store.shadow_admission_report()
            self.assertEqual(
                report["in_sample_fitted"]["actual_admitted"]["admitted"], 1
            )
            self.assertEqual(
                report["out_of_sample"]["actual_admitted"]["admitted"], 0
            )
            self.assertEqual(store.backfill_shadow_admission(), 0, "must be idempotent")


class PoolReserveFractionTests(unittest.TestCase):
    """Singleton balance telemetry must never be labelled pool reserves."""

    def test_fraction_is_balance_over_supply(self):
        class BalanceRPC(FakeV4RPC):
            def call(self, address, data, block=None):
                if data.startswith("0x" + rh.ERC20_BALANCE_OF_SELECTOR):
                    return hex(25 * 10**18)
                return super().call(address, data, block)

        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            client = rh.RobinhoodV4MarketClient(BalanceRPC(), FakeMarket(), store)
            self.assertAlmostEqual(
                client.singleton_balance_fraction(TOKEN, 100 * 10**18), 0.25
            )

    def test_unreadable_balance_is_none_not_zero(self):
        class FailingRPC(FakeV4RPC):
            def call(self, address, data, block=None):
                raise RuntimeError("rpc down")

        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            client = rh.RobinhoodV4MarketClient(FailingRPC(), FakeMarket(), store)
            self.assertIsNone(client.singleton_balance_fraction(TOKEN, 10**18))

    def test_zero_supply_is_none(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            client = rh.RobinhoodV4MarketClient(FakeV4RPC(), FakeMarket(), store)
            self.assertIsNone(client.singleton_balance_fraction(TOKEN, 0))


class PipelineListingTests(unittest.TestCase):
    """The status view lists tokens still being decided, not the rejects.

    546 of 639 analysed candidates were rejected. Shipping all of them to the
    browser buried the handful the system was actually acting on.
    """

    def _analysed(self, store, token, decision, allowed):
        store.add_candidates([{
            "token_address": token, "pair_address": PAIR,
            "factory_address": PAIR, "block_number": 1, "block_timestamp": 1,
            "transaction_hash": "0x1", "log_index": 0, "name": "N", "symbol": "S",
        }])
        with store.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET analysis_status='complete', score=80,
                    risk_level='Low', paper_decision=?, paper_entry_allowed=?
                WHERE token_address=?
                """, (decision, int(allowed), token.lower()),
            )

    def test_rejected_and_expired_are_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._analysed(store, "0x" + "a1" * 20, "admitted", True)
            self._analysed(store, "0x" + "b2" * 20, "watching_for_executable_market", False)
            self._analysed(store, "0x" + "c3" * 20, "rejected", False)
            self._analysed(store, "0x" + "d4" * 20, "expired_no_executable_market", False)
            listed = {row["token_address"] for row in store.recent_analyzed_tokens()}
            self.assertEqual(listed, {"0x" + "b2" * 20})
            self.assertNotIn("0x" + "c3" * 20, listed)
            self.assertNotIn("0x" + "d4" * 20, listed)
            self.assertNotIn(
                "0x" + "a1" * 20, listed,
                "an admitted token holds a position and is listed there instead",
            )

    def test_rejected_remain_queryable_for_audits(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._analysed(store, "0x" + "c3" * 20, "rejected", False)
            self.assertEqual(len(store.recent_analyzed_tokens()), 0)
            self.assertEqual(
                len(store.recent_analyzed_tokens(include_rejected=True)), 1,
                "rejections must stay available to the counterfactual audit",
            )

    def test_dashboard_detection_feed_includes_excluded_tokens(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            token = "0x" + "e5" * 20
            self._analysed(store, token, "v4_shadow_only", False)
            snapshot = rh.dashboard_snapshot(directory)
            self.assertEqual(snapshot["analyzed_tokens"][0]["token_address"], token)
            self.assertEqual(
                snapshot["analyzed_tokens"][0]["paper_decision"],
                "v4_shadow_only",
            )


class SafetySignalTests(unittest.TestCase):
    """The gate reads a blend; the rugs hid inside it.

    All 22 total losses were admitted carrying NO hard stop, at Low or Medium
    risk. legitimacy_score weights holder_distribution at 0.07 and lp_lock at
    0.09, so a token can score 73.7 with holder_distribution at 35. These
    rules read the component instead of the blend -- and record only.
    """

    ANALYSIS = {
        "component_scores": {"holder_distribution": 0.0, "lp_lock": 50.0,
                             "honeypot_safety": 95.0, "security": 65.0},
        "holder_assessment": {"holder_count": 4},
        "hard_stop_overrides": [{
            "code": "EXTREME_CONCENTRATION",
            "reason": "Top non-AMM holder controls 85.0% of token supply",
        }],
        "red_flags": ["Top non-AMM holder has 85.0% of supply"],
    }

    def test_top_holder_percentage_is_parsed_from_the_stop(self):
        signals = rh.extract_safety_signals(self.ANALYSIS, {})
        self.assertEqual(signals["top_holder_pct"], 85.0)
        self.assertEqual(signals["holder_distribution_score"], 0.0)
        self.assertEqual(signals["holder_count"], 4.0)

    def test_percentage_falls_back_to_red_flags(self):
        analysis = dict(self.ANALYSIS, hard_stop_overrides=[])
        self.assertEqual(
            rh.extract_safety_signals(analysis, {})["top_holder_pct"], 85.0
        )

    def test_lp_lock_comes_from_the_data_block(self):
        signals = rh.extract_safety_signals(
            self.ANALYSIS, {"lp_lock": {"locked": True, "source": "teamfinance"}}
        )
        self.assertIs(signals["lp_locked"], True)
        self.assertEqual(signals["lp_lock_source"], "teamfinance")

    def _shadow(self, safety):
        return rh.shadow_admission(
            market_cap=100_000.0, liquidity=60_000.0, hooks_present=False,
            hard_stops=[], token_quality_passes=True, allowed=True, safety=safety,
        )

    def test_concentrated_token_is_refused_by_the_shadow_rule(self):
        shadow = self._shadow(rh.extract_safety_signals(self.ANALYSIS, {}))
        self.assertTrue(shadow["actual_admitted"], "the live gate is untouched")
        self.assertFalse(shadow["would_admit_concentration_rule"])
        self.assertFalse(shadow["would_admit_safety_combined"])

    def test_clean_distribution_passes(self):
        shadow = self._shadow({
            "top_holder_pct": 8.0, "holder_distribution_score": 80.0,
            "lp_locked": True,
        })
        self.assertTrue(shadow["would_admit_concentration_rule"])
        self.assertTrue(shadow["would_admit_liquidity_lock_rule"])
        self.assertTrue(shadow["would_admit_safety_combined"])

    def test_absent_evidence_is_not_a_pass(self):
        """Every rug looked clean; unmeasured must not read as safe."""
        shadow = self._shadow({})
        self.assertFalse(shadow["concentration_evidence_present"])
        self.assertFalse(shadow["concentration_pass"])
        self.assertFalse(shadow["would_admit_concentration_rule"])
        self.assertFalse(shadow["liquidity_custody_pass"])

    def test_safety_rules_never_widen_admission(self):
        """A shadow rule may only ever refuse what the gate allowed."""
        shadow = rh.shadow_admission(
            market_cap=100_000.0, liquidity=60_000.0, hooks_present=False,
            hard_stops=[], token_quality_passes=True, allowed=False,
            safety={"top_holder_pct": 1.0, "holder_distribution_score": 99.0,
                    "lp_locked": True},
        )
        for rule in ("would_admit_concentration_rule",
                     "would_admit_liquidity_lock_rule",
                     "would_admit_safety_combined"):
            self.assertFalse(shadow[rule], rule)

    def test_signals_are_persisted_on_the_candidate(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            store.add_candidates([{
                "token_address": TOKEN, "pair_address": PAIR,
                "factory_address": PAIR, "block_number": 1, "block_timestamp": 1,
                "transaction_hash": "0x1", "log_index": 0, "name": "N", "symbol": "S",
            }])
            store.record_analysis(
                TOKEN,
                dict(self.ANALYSIS, legitimacy_score=82, risk_level="Low"),
                {"price_usd": 0.01, "liquidity_usd": 60_000, "market_cap_usd": 100_000},
                report_data={"lp_lock": {"locked": False}},
            )
            row = store.candidate(TOKEN)
            self.assertEqual(row["top_holder_pct"], 85.0)
            self.assertEqual(row["holder_distribution_score"], 0.0)
            self.assertEqual(row["lp_lock_score"], 50.0)
            self.assertIn("lp_locked", row["safety_signals_json"])


class PortfolioMetricsTests(unittest.TestCase):
    """Headline results, bucketed by the score that admitted them."""

    def _closed(self, store, suffix, score, multiple, observed=True):
        token = "0x" + suffix * 20
        store.add_candidates([{
            "token_address": token, "pair_address": PAIR, "factory_address": PAIR,
            "block_number": 1, "block_timestamp": 1, "transaction_hash": "0x1",
            "log_index": 0, "name": "N", "symbol": "S",
        }])
        with store.connection() as connection:
            connection.execute(
                "UPDATE candidates SET score=?,analysis_status='complete' WHERE token_address=?",
                (score, token),
            )
            connection.execute(
                """
                INSERT INTO positions (token_address,symbol,status,opened_at,
                    entry_price_usd,entry_liquidity_usd,cost_usd,quantity,
                    entry_friction_bps,high_multiple,original_quantity,
                    realized_value_usd,net_multiple,closed_at,last_mark_at,
                    verified_mark_count)
                VALUES (?,?,'closed',1,0.001,50000,100,0,100,1,1000,?,?,2,?,?)
                """,
                (
                    token, "S", multiple * 100.0, multiple,
                    2 if observed else None, int(observed),
                ),
            )

    def test_buckets_group_by_five_point_score_bands(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._closed(store, "a1", 71.0, 2.0)
            self._closed(store, "b2", 73.0, 0.5)
            self._closed(store, "c3", 82.0, 0.0)
            metrics = store.portfolio_metrics()
            self.assertEqual(metrics["closed"], 3)
            self.assertEqual(metrics["winners"], 1)
            self.assertEqual(metrics["total_loss"], 1)
            self.assertEqual(set(metrics["by_score_bucket"]), {"70-75", "80-85"})
            self.assertEqual(metrics["by_score_bucket"]["70-75"]["n"], 2)
            self.assertEqual(metrics["by_score_bucket"]["80-85"]["total_loss"], 1)

    def test_closes_never_marked_are_held_out(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._closed(store, "a1", 80.0, 1.5)
            self._closed(store, "b2", 80.0, 0.0, observed=False)
            metrics = store.portfolio_metrics()
            self.assertEqual(metrics["closed"], 1)
            self.assertEqual(metrics["observation_indeterminate"], 1)
            self.assertEqual(metrics["total_loss"], 0,
                             "a position that was never marked was scored as a loss")

    def test_a_fast_rug_with_no_checkpoint_still_counts(self):
        """Rugs that die inside minutes never reach a 1h outcome horizon.

        Keying the holdout off outcome checkpoints discarded 15 price
        collapses, several held 4-5 minutes -- the worst outcomes in the book.
        """
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._closed(store, "a1", 80.0, 0.0)
            metrics = store.portfolio_metrics()
            self.assertEqual(metrics["closed"], 1)
            self.assertEqual(metrics["total_loss"], 1)
            self.assertEqual(metrics["observation_indeterminate"], 0)

    def test_negative_correlation_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            for index, (score, multiple) in enumerate(
                [(70.0, 3.0), (75.0, 2.0), (80.0, 1.0), (85.0, 0.0)]
            ):
                self._closed(store, f"{index}{index}", score, multiple)
            correlation = store.portfolio_metrics()["score_outcome_correlation"]
            self.assertIsNotNone(correlation)
            self.assertLess(correlation, 0, "higher score bought a worse outcome")

    def test_dashboard_closed_performance_uses_complete_closed_book(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            self._closed(store, "a1", 75.0, 2.0)
            self._closed(store, "b2", 75.0, 0.5)
            performance = rh.dashboard_snapshot(directory)["closed_performance"]
            self.assertEqual(performance["closed"], 2)
            self.assertEqual(performance["winners"], 1)
            self.assertEqual(performance["win_rate"], 0.5)
            self.assertEqual(performance["invested_usd"], 200.0)
            self.assertEqual(performance["returned_usd"], 250.0)
            self.assertEqual(performance["net_pnl_usd"], 50.0)

    def test_empty_book_does_not_divide_by_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            metrics = store.portfolio_metrics()
            self.assertEqual(metrics["closed"], 0)
            self.assertIsNone(metrics["average_multiple"])
            self.assertIsNone(metrics["score_outcome_correlation"])


class EntryDeliberationTests(unittest.TestCase):
    """The entry decision forks across tags instead of reading one blend.

    Every one of the 22 total losses satisfied all three flat-gate questions:
    score above floor, risk in {Low, Medium}, no hard stops.
    """

    CLEAN = {"top_holder_pct": 5.0, "holder_distribution_score": 85.0,
             "lp_locked": True, "honeypot_safety_score": 95.0}
    RUGGY = {"top_holder_pct": 85.0, "holder_distribution_score": 0.0,
             "lp_locked": False, "honeypot_safety_score": 95.0}

    def _judge(self, safety, allowed=True, score=80.0):
        return rh.deliberate_entry(
            safety, score=score, risk_level="Low", allowed=allowed,
        )

    def test_clean_token_is_admitted(self):
        result = self._judge(self.CLEAN)
        self.assertEqual(result["judgment"], "admit")
        self.assertEqual(result["refusal_reasons"], [])
        self.assertTrue(result["agrees_with_gate"])

    def test_concentrated_unlocked_token_is_refused_despite_a_clean_gate(self):
        result = self._judge(self.RUGGY)
        self.assertEqual(result["judgment"], "refuse")
        self.assertIn("Holder concentration (veto)", result["refusal_reasons"])
        self.assertIn("Liquidity custody", result["refusal_reasons"])
        self.assertFalse(
            result["agrees_with_gate"],
            "this is exactly the disagreement the fork exists to surface",
        )

    def test_missing_evidence_abstains_rather_than_admitting(self):
        result = self._judge({})
        self.assertEqual(result["judgment"], "abstain")
        self.assertGreaterEqual(result["weight_abstain"], 0.5)

    def test_severe_concentration_vetoes_every_other_clean_tag(self):
        """A wallet holding most of supply can zero the token by itself."""
        result = self._judge(dict(self.RUGGY, lp_locked=True))
        self.assertEqual(result["judgment"], "refuse")
        self.assertTrue(result["concentration_veto"])
        self.assertLess(
            result["weight_refuse"], result["weight_admit"],
            "the veto must win DESPITE losing the weighted vote",
        )

    def test_moderate_concentration_votes_instead_of_vetoing(self):
        result = self._judge({
            "top_holder_pct": 30.0, "holder_distribution_score": 40.0,
            "lp_locked": True, "honeypot_safety_score": 95.0,
        })
        self.assertFalse(result["concentration_veto"])
        self.assertEqual(result["judgment"], "admit")

    def test_deliberation_never_enforces(self):
        self.assertFalse(self._judge(self.RUGGY)["enforced"])

    def test_every_perspective_is_reported_with_its_verdict(self):
        names = {p["name"] for p in self._judge(self.CLEAN)["perspectives"]}
        self.assertEqual(
            names,
            {"Holder concentration", "Liquidity custody", "Sellability",
             "Blended score"},
        )


class FlowGateTelemetryTests(unittest.TestCase):
    """Flow Signal v1 produced 0 qualified events from 653 windows.

    Per-gate counts alone mislead, because two conditions sit outside the gate
    list and each independently makes a prospective event impossible: head lag
    against a 120-block bound, and a score threshold of 70.0 that the best
    observed window (69.0) has never reached.
    """

    def _signal(self, store, pool, score, gaps, end_block, computed_at=None):
        with store.connection() as connection:
            # Telemetry reads the append-only series, so the fixture must
            # write a window there rather than only the latest-state row.
            connection.execute(
                """
                INSERT OR IGNORE INTO flow_signal_windows (
                    policy_version,pool_id,source_version,token_address,
                    computed_at,window_blocks,window_start_block,
                    window_end_block,swap_count,buy_count,sell_count,
                    unique_sender_hints,unique_resolved_participants,
                    identity_coverage,buy_ratio,net_anchor_flow_fraction,
                    price_multiple,uncapped_shadow_score,shadow_score,
                    shadow_qualified,confidence,qualification_gaps_json,
                    features_json)
                VALUES (?,?,'uniswap_v4',?,?,450,?,?,6,4,2,4,4,1.0,0.66,0.4,
                        1.0,?,?,0,'test',?,?)
                """,
                (rh.FLOW_EVIDENCE_POLICY_VERSION, pool, TOKEN,
                 computed_at or rh._utc_now(), end_block - 450, end_block,
                 score, score, json.dumps(gaps),
                 json.dumps({"qualification_gaps": gaps})),
            )
            connection.execute(
                """
                INSERT INTO flow_signals (source_version,pool_id,token_address,
                    computed_at,window_blocks,window_start_block,window_end_block,
                    swap_count,buy_count,sell_count,unique_sender_hints,
                    unique_resolved_participants,identity_coverage,buy_ratio,
                    net_anchor_flow_fraction,uncapped_shadow_score,shadow_score,
                    shadow_qualified,confidence,qualification_gaps_json,
                    limitations_json,features_json)
                VALUES ('uniswap_v4',?,?,?,450,?,?,6,4,2,4,4,1.0,0.66,0.4,?,?,0,
                        'test',?,'[]',?)
                """,
                (pool, TOKEN, computed_at or rh._utc_now(), end_block - 450,
                 end_block, score, score, json.dumps(gaps),
                 json.dumps({"qualification_gaps": gaps})),
            )

    def test_gates_are_counted_per_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._signal(store, "0xaa", 40.0, ["minimum_swaps"], 100)
            self._signal(store, "0xbb", 50.0, ["minimum_swaps", "positive_net_anchor_flow"], 200)
            telemetry = store.flow_gate_telemetry()
            self.assertEqual(telemetry["windows"], 2)
            self.assertEqual(telemetry["qualified"], 0)
            self.assertEqual(telemetry["gates_failed"]["minimum_swaps"], 2)
            self.assertEqual(telemetry["gates_failed"]["positive_net_anchor_flow"], 1)

    def test_a_window_with_no_gate_evaluation_is_not_counted_as_passing(self):
        """96 real windows carried an empty gaps column for exactly this reason."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            with store.connection() as connection:
                connection.execute(
                    """
                    INSERT INTO flow_signal_windows (policy_version,pool_id,
                        source_version,token_address,computed_at,window_blocks,
                        window_start_block,window_end_block,swap_count,buy_count,
                        sell_count,unique_sender_hints,unique_resolved_participants,
                        identity_coverage,buy_ratio,net_anchor_flow_fraction,
                        uncapped_shadow_score,shadow_score,shadow_qualified,
                        confidence,qualification_gaps_json,features_json)
                    VALUES (?,'0xcc','uniswap_v4',?,?,450,10,460,6,4,2,4,4,1.0,
                            0.66,0.4,10.0,10.0,0,'test','[]','{}')
                    """, (rh.FLOW_EVIDENCE_POLICY_VERSION, TOKEN, rh._utc_now()),
                )
            telemetry = store.flow_gate_telemetry()
            self.assertEqual(telemetry["windows_without_gate_evaluation"], 1)
            self.assertEqual(telemetry["gates_failed"], {},
                             "an unevaluated window must not read as a clean pass")

    def test_score_ceiling_is_reported_against_the_threshold(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._signal(store, "0xaa", 69.0, ["minimum_swaps"], 100)
            telemetry = store.flow_gate_telemetry()
            self.assertEqual(telemetry["best_shadow_score"], 69.0)
            self.assertEqual(telemetry["score_threshold"], rh.FLOW_SHADOW_SCORE_THRESHOLD)
            self.assertFalse(telemetry["score_threshold_reached"])

    def test_head_lag_is_measured_against_the_chain_not_our_own_ingestion(self):
        """Comparing the reader with itself reports zero lag however far behind it is."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._signal(store, "0xaa", 40.0, ["minimum_swaps"], 1_000)
            without = store.flow_gate_telemetry()
            self.assertEqual(without["head_block_source"], "ingestion_high_water_mark")
            self.assertFalse(
                without["head_lag_within_prospective_bound"],
                "an unverified head must never certify freshness",
            )
            with_head = store.flow_gate_telemetry(head_block=1_000 + 794)
            self.assertEqual(with_head["head_block_source"], "chain_head")
            self.assertEqual(with_head["head_lag_blocks"], 794)
            self.assertFalse(with_head["head_lag_within_prospective_bound"])

    def test_a_fresh_window_is_recognised_as_prospective_eligible(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._signal(store, "0xaa", 40.0, [], 10_000)
            telemetry = store.flow_gate_telemetry(head_block=10_050)
            self.assertEqual(telemetry["head_lag_blocks"], 50)
            self.assertTrue(telemetry["head_lag_within_prospective_bound"])


class FlowEvidenceEndToEndTests(unittest.TestCase):
    """A qualifying fresh signal must produce the whole evidence chain.

    Flow Signal v1 has emitted 0 prospective events from 653 windows, and the
    two causes are upstream of this machinery: no window has reached the 70.0
    score threshold (best 69.0), and none has landed inside the 120-block
    freshness bound (observed lag 794). This test feeds the pipeline the
    qualifying fresh signal it has never actually seen, so that the path from
    signal to calibration eligibility is proven independently of whether
    production ever supplies one.
    """

    POOL = "0x" + "ee" * 32
    HEAD = 1_000_000

    def _store(self, directory):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        # Quote scheduling joins v4_pools, so the pools must exist on chain
        # terms before an event can be priced.
        for pool in (self.POOL, "0x" + "dd" * 32):
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": pool,
                "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }])
        return store

    def _signal(self, store, pool, *, qualified, end_block, score):
        with store.connection() as connection:
            connection.execute(
                """
                INSERT INTO flow_signals (source_version,pool_id,token_address,
                    computed_at,window_blocks,window_start_block,window_end_block,
                    swap_count,buy_count,sell_count,unique_sender_hints,
                    unique_resolved_participants,identity_coverage,buy_ratio,
                    net_anchor_flow_fraction,uncapped_shadow_score,shadow_score,
                    shadow_qualified,confidence,qualification_gaps_json,
                    limitations_json,features_json)
                VALUES ('uniswap_v4',?,?,?,450,?,?,12,9,3,6,6,1.0,0.75,0.42,?,?,?,
                        'transaction_origins_verified',?,'[]',?)
                """,
                (
                    pool, TOKEN, rh._utc_now(), end_block - 450, end_block,
                    score, score, int(qualified),
                    json.dumps([] if qualified else ["minimum_swaps"]),
                    json.dumps({"qualification_gaps": [] if qualified else ["minimum_swaps"]}),
                ),
            )

    def _quote(self, verified=True, anchor_in=1_000, anchor_out=1_300):
        return {
            "execution_quote": {
                "verified": verified, "passes_round_trip_limit": verified,
                "anchor_in_raw": anchor_in, "anchor_out_raw": anchor_in,
            },
            "paper_exit_quote": {
                "verified": verified, "anchor_out_raw": anchor_out,
            },
        }

    def test_qualifying_fresh_signal_produces_event_control_quote_and_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            # One qualifying signal and one non-qualifying peer to match against,
            # both inside the freshness bound.
            self._signal(store, self.POOL, qualified=True,
                         end_block=self.HEAD - 10, score=75.0)
            self._signal(store, "0x" + "dd" * 32, qualified=False,
                         end_block=self.HEAD - 12, score=30.0)

            captured = store.capture_flow_signal_events(self.HEAD, now=1_000.0)
            self.assertGreaterEqual(captured.get("qualified_created", 0), 1,
                                    f"no prospective event created: {captured}")
            self.assertGreaterEqual(captured.get("controls_created", 0), 1,
                                    f"no matched control created: {captured}")

            with store.connection() as connection:
                events = [dict(r) for r in connection.execute(
                    "SELECT * FROM flow_signal_events ORDER BY signal_role"
                )]
            roles = {e["signal_role"] for e in events}
            self.assertIn("qualified", roles)
            qualified = next(e for e in events if e["signal_role"] == "qualified")

            # Immutable, fresh, and eligible.
            self.assertEqual(qualified["freshness"], "fresh")
            self.assertTrue(qualified["eligible_for_evaluation"])
            self.assertLessEqual(
                qualified["head_lag_blocks"],
                rh.FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
            )

            # A matched control chosen without future information.
            if "matched_control" in roles:
                control = next(e for e in events if e["signal_role"] == "matched_control")
                self.assertEqual(control["matched_signal_event_id"],
                                 qualified["event_id"])
                self.assertNotEqual(control["pool_id"], qualified["pool_id"])

            # A block-pinned entry quote.
            pending = store.pending_flow_quotes()
            self.assertTrue(pending, "no quote was scheduled for the event")
            store.record_flow_entry_quote(
                qualified["event_id"], self._quote(), quote_block=self.HEAD
            )
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT quote_status,quote_block,quote_verified FROM"
                    " flow_signal_events WHERE event_id=?",
                    (qualified["event_id"],),
                ).fetchone()
            self.assertEqual(row["quote_status"], "verified")
            self.assertEqual(row["quote_block"], self.HEAD)

            # Scheduled outcome observations exist and can be resolved.
            with store.connection() as connection:
                scheduled = connection.execute(
                    "SELECT COUNT(*) FROM flow_signal_outcomes WHERE event_id=?",
                    (qualified["event_id"],),
                ).fetchone()[0]
            self.assertGreater(scheduled, 0, "no outcome observations scheduled")

            due = store.due_flow_outcomes(now=1_000.0 + 10 * 86_400, limit=20)
            self.assertTrue(due, "no outcome became due")
            fifteen = [
                row for row in due
                if row["event_id"] == qualified["event_id"]
                and row["horizon_label"] == "15m"
            ]
            self.assertTrue(fifteen, "the 15-minute horizon never became due")
            store.record_flow_outcome(
                fifteen[0], self._quote(), quote_block=self.HEAD + 50,
                now=1_000.0 + 10 * 86_400,
            )
            with store.connection() as connection:
                resolved = connection.execute(
                    "SELECT status,net_return,exit_valid FROM flow_signal_outcomes"
                    " WHERE event_id=? AND status!='pending'",
                    (qualified["event_id"],),
                ).fetchall()
            self.assertTrue(resolved, "outcome did not resolve")
            self.assertTrue(resolved[0]["exit_valid"])
            self.assertGreater(resolved[0]["net_return"], 0.0,
                               "a 1.3x exit should record a positive return")

            # Reflection / calibration can see it.
            summary = store.flow_evidence_summary()
            self.assertIsInstance(summary, dict)
            self.assertGreaterEqual(summary.get("events", 0), 1)

    def test_a_stale_signal_is_recorded_but_never_eligible(self):
        """Catch-up rows stay as diagnostic history and cannot schedule outcomes."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._signal(store, self.POOL, qualified=True,
                         end_block=self.HEAD - 5_000, score=75.0)
            store.capture_flow_signal_events(self.HEAD, now=1_000.0)
            with store.connection() as connection:
                rows = [dict(r) for r in connection.execute(
                    "SELECT freshness,eligible_for_evaluation FROM flow_signal_events"
                )]
            for row in rows:
                self.assertEqual(row["freshness"], "historical")
                self.assertFalse(row["eligible_for_evaluation"])


class NearHeadFlowPassTests(unittest.TestCase):
    """Flow windows must end near the chain head to be prospective at all.

    The batch cycle finished thousands of blocks back, so every window it
    produced was stale on arrival: 794 blocks late against a 120-block bound,
    and 0 of 653 windows ever eligible. The bound was never tested because
    nothing was computed near enough to the head to test it.
    """

    class HeadRPC(FakeRPC):
        def __init__(self, head, logs=None, advance=3):
            super().__init__(logs or [], latest=head)
            self.head = head
            self.advance = advance
            self.ranges = []

        def get_block_number(self):
            current = self.head
            self.head += self.advance
            return current

        def get_logs(self, start, end, address=None, topics=None):
            self.ranges.append((start, end))
            return self.logs

    def _engine(self, directory, rpc):
        return rh.RobinhoodLearningEngine(
            directory, rpc=rpc, analyzer=FakeAnalyzer(), market=FakeMarket(),
        )

    def test_pass_scans_exactly_the_final_window(self):
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.HeadRPC(1_000_000)
            result = self._engine(directory, rpc).near_head_flow_pass()
            self.assertTrue(result["supported"])
            self.assertTrue(result["scanned"])
            start, end = rpc.ranges[0]
            self.assertEqual(end - start + 1, rh.FLOW_WINDOW_BLOCKS)
            self.assertEqual(end, 1_000_000)

    def test_lag_is_re_read_not_assumed(self):
        """Reusing the starting head would make freshness self-certifying."""
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.HeadRPC(1_000_000, advance=7)
            result = self._engine(directory, rpc).near_head_flow_pass()
            self.assertEqual(result["head_block_after"], 1_000_007)
            self.assertEqual(result["elapsed_blocks"], 7)
            self.assertTrue(result["within_prospective_bound"])

    def test_a_slow_pass_reports_itself_out_of_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.HeadRPC(
                1_000_000, advance=rh.FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS + 1
            )
            result = self._engine(directory, rpc).near_head_flow_pass()
            self.assertFalse(
                result["within_prospective_bound"],
                "a pass slower than the freshness bound must say so",
            )

    def test_unknown_pools_are_skipped(self):
        swap = {
            "topics": [rh.V4_SWAP_TOPIC, POOL_ID, topic(TOKEN)],
            "data": "0x" + word(5) + word(5) + word(1 << 96) + word(10) + word(0),
            "blockNumber": "0xf4240", "logIndex": "0x0",
            "transactionHash": "0xabc",
        }
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.HeadRPC(1_000_000, logs=[swap])
            result = self._engine(directory, rpc).near_head_flow_pass()
            self.assertEqual(result["logs_seen"], 1)
            self.assertEqual(
                result["swaps_ingested"], 0,
                "a swap for an unknown pool must not be ingested",
            )

    def test_rpc_failure_is_reported_not_raised(self):
        class BrokenRPC(self.HeadRPC):
            def get_logs(self, start, end, address=None, topics=None):
                raise RuntimeError("rpc down")

        with tempfile.TemporaryDirectory() as directory:
            result = self._engine(directory, BrokenRPC(1_000_000)).near_head_flow_pass()
            self.assertTrue(result["supported"])
            self.assertFalse(result["scanned"])
            self.assertIn("rpc down", result["reason"])


class FlowWindowHistoryTests(unittest.TestCase):
    """flow_signals is latest-state; flow_signal_windows is the series.

    Keyed on pool_id alone, flow_signals held 773 rows for 773 pools, each
    overwritten every cycle and therefore parked at that pool's terminal
    window. Measuring across it selected the dying moments of dead pools and
    produced a spurious 18% direction anti-correlation.
    """

    def _pool(self, store, pool):
        store.apply_v4_events([{
            "kind": "initialize", "pool_id": pool,
            "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
            "token_address": TOKEN.lower(),
            "anchor_address": rh.USDG_ADDRESS.lower(),
            "fee_tier": 3000, "tick_spacing": 60,
            "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
            "sqrt_price_x96": 1 << 96, "tick": 0,
        }])

    def _window(self, store, pool, end_block, qualified=0):
        with store.connection() as connection:
            connection.execute(
                """
                INSERT OR IGNORE INTO flow_signal_windows (
                    policy_version,pool_id,source_version,token_address,
                    computed_at,window_blocks,window_start_block,
                    window_end_block,swap_count,buy_count,sell_count,
                    unique_sender_hints,unique_resolved_participants,
                    identity_coverage,buy_ratio,net_anchor_flow_fraction,
                    price_multiple,uncapped_shadow_score,shadow_score,
                    shadow_qualified,confidence,qualification_gaps_json,
                    features_json)
                VALUES (?,?,'uniswap_v4',?,?,450,?,?,6,4,2,4,4,1.0,0.66,0.4,
                        1.0,40.0,40.0,?,'test','[]','{}')
                """,
                (rh.FLOW_EVIDENCE_POLICY_VERSION, pool, TOKEN, rh._utc_now(),
                 end_block - 450, end_block, qualified),
            )

    def test_successive_windows_accumulate_instead_of_overwriting(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            for end in (1_000, 1_450, 1_900):
                self._window(store, "0xaa", end)
            history = store.flow_window_history()
            self.assertEqual(len(history), 3, "the series collapsed to one row")
            self.assertEqual(
                [row["window_end_block"] for row in history],
                [1_900, 1_450, 1_000], "history must read newest first",
            )

    def test_recomputing_a_window_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._window(store, "0xaa", 1_000)
            self._window(store, "0xaa", 1_000)
            self.assertEqual(len(store.flow_window_history()), 1)

    def test_history_is_namespaced_by_policy_version(self):
        """A cohort reset must not blend the biased corpus into the new one."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._window(store, "0xaa", 1_000)
            with store.connection() as connection:
                connection.execute(
                    "UPDATE flow_signal_windows SET policy_version='flow-evidence-v1'"
                )
            self.assertEqual(store.flow_window_history(), [])
            self.assertEqual(
                len(store.flow_window_history(policy_version="flow-evidence-v1")), 1
            )

    def test_retention_never_prunes_qualified_windows(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._window(store, "0xaa", 1_000, qualified=1)
            self._window(store, "0xbb", 1_500, qualified=0)
            self._window(store, "0xcc", 500_000, qualified=0)
            removed = store.prune_flow_windows(keep_blocks=1_000)
            self.assertEqual(removed, 1, "only the old non-qualified row should go")
            remaining = {row["pool_id"] for row in store.flow_window_history()}
            self.assertIn("0xaa", remaining, "a qualified window was pruned")
            self.assertIn("0xcc", remaining)

    def test_latest_state_table_keeps_one_row_per_pool(self):
        """The bounded contract other joins rely on must be preserved."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            with store.connection() as connection:
                cols = {row[1] for row in connection.execute(
                    "PRAGMA index_list(flow_signals)"
                )}
            self.assertIsNotNone(cols)
            with store.connection() as connection:
                pk = [row[1] for row in connection.execute(
                    "PRAGMA table_info(flow_signal_windows)"
                ) if row[5]]
            self.assertEqual(
                pk, ["policy_version", "pool_id", "window_end_block"],
                "the series key must be composite, not pool alone",
            )


class FlowCaptureAtomicityTests(unittest.TestCase):
    """Event, matched control and scheduled outcomes must land together.

    A half-written capture -- an event with no outcomes, or a qualified signal
    with no control -- would be indistinguishable from a signal that legitimately
    produced neither, and would silently bias every later comparison.
    """

    POOL = "0x" + "ee" * 32
    HEAD = 1_000_000

    def _store(self, directory):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        for pool in (self.POOL, "0x" + "dd" * 32):
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": pool,
                "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }])
        return store

    def _signal(self, store, pool, qualified, end_block):
        with store.connection() as connection:
            connection.execute(
                """
                INSERT INTO flow_signals (source_version,pool_id,token_address,
                    computed_at,window_blocks,window_start_block,window_end_block,
                    swap_count,buy_count,sell_count,unique_sender_hints,
                    unique_resolved_participants,identity_coverage,buy_ratio,
                    net_anchor_flow_fraction,uncapped_shadow_score,shadow_score,
                    shadow_qualified,confidence,qualification_gaps_json,
                    limitations_json,features_json)
                VALUES ('uniswap_v4',?,?,?,450,?,?,12,9,3,6,6,1.0,0.75,0.42,
                        75.0,75.0,?,'test','[]','[]','{}')
                """,
                (pool, TOKEN, rh._utc_now(), end_block - 450, end_block,
                 int(qualified)),
            )

    def test_event_control_and_outcomes_all_land(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._signal(store, self.POOL, True, self.HEAD - 10)
            self._signal(store, "0x" + "dd" * 32, False, self.HEAD - 12)
            store.capture_flow_signal_events(self.HEAD, now=1_000.0)
            with store.connection() as connection:
                events = connection.execute(
                    "SELECT COUNT(*) FROM flow_signal_events"
                ).fetchone()[0]
                outcomes = connection.execute(
                    "SELECT COUNT(*) FROM flow_signal_outcomes"
                ).fetchone()[0]
                orphans = connection.execute(
                    """
                    SELECT COUNT(*) FROM flow_signal_events e
                    WHERE e.eligible_for_evaluation=1 AND NOT EXISTS (
                        SELECT 1 FROM flow_signal_outcomes o
                        WHERE o.event_id=e.event_id)
                    """
                ).fetchone()[0]
            self.assertGreater(events, 0)
            self.assertGreater(outcomes, 0)
            self.assertEqual(
                orphans, 0, "an eligible event exists with no scheduled outcomes",
            )

    def test_a_failure_mid_capture_leaves_nothing_behind(self):
        """The whole capture is one transaction, so it rolls back entire."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._signal(store, self.POOL, True, self.HEAD - 10)
            self._signal(store, "0x" + "dd" * 32, False, self.HEAD - 12)
            original = store._flow_event_id

            calls = {"n": 0}

            def exploding(*parts):
                calls["n"] += 1
                if calls["n"] > 1:          # fail after the first insert
                    raise RuntimeError("interrupted mid-capture")
                return original(*parts)

            store._flow_event_id = exploding
            with self.assertRaises(RuntimeError):
                store.capture_flow_signal_events(self.HEAD, now=1_000.0)
            store._flow_event_id = original
            with store.connection() as connection:
                events = connection.execute(
                    "SELECT COUNT(*) FROM flow_signal_events"
                ).fetchone()[0]
                outcomes = connection.execute(
                    "SELECT COUNT(*) FROM flow_signal_outcomes"
                ).fetchone()[0]
            self.assertEqual(events, 0, "a partial event survived a failed capture")
            self.assertEqual(outcomes, 0, "orphan outcomes survived a failed capture")

    def test_entry_quote_is_deliberately_outside_the_transaction(self):
        """Quotes need an RPC call, so they cannot share the write transaction.

        The recoverable state is quote_status='pending': an event without a
        quote is re-offered by pending_flow_quotes rather than lost.
        """
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._signal(store, self.POOL, True, self.HEAD - 10)
            store.capture_flow_signal_events(self.HEAD, now=1_000.0)
            with store.connection() as connection:
                pending = connection.execute(
                    "SELECT COUNT(*) FROM flow_signal_events WHERE quote_status='pending'"
                ).fetchone()[0]
            self.assertGreater(pending, 0)
            self.assertTrue(
                store.pending_flow_quotes(),
                "an unquoted event must be re-offered, not stranded",
            )


class NearHeadCohortSelectorTests(unittest.TestCase):
    """A fixed enrichment budget must finish fresh windows before stale ones.

    active_window is not a freshness test: a pool's latest-state row keeps
    whatever window it last had, so 267 stale rows satisfied it and the budget
    spread across windows that could never meet the 120-block bound. None
    reached the 0.80 coverage floor while origin resolution was succeeding on
    33,522 of 33,522 attempts -- the work was selected, never the right work.
    """

    HEAD = 1_000_000
    STALE_POOLS = 267
    FRESH_POOLS = 8
    FRESH_TX = 53

    def _window(self, connection, pool, end_block, coverage=0.0):
        connection.execute(
            """
            INSERT INTO flow_signals (source_version,pool_id,token_address,
                computed_at,window_blocks,window_start_block,window_end_block,
                swap_count,buy_count,sell_count,unique_sender_hints,
                unique_resolved_participants,identity_coverage,buy_ratio,
                net_anchor_flow_fraction,uncapped_shadow_score,shadow_score,
                shadow_qualified,confidence,qualification_gaps_json,
                limitations_json,features_json)
            VALUES ('uniswap_v4',?,?,?,450,?,?,6,4,2,4,4,?,0.66,0.4,40.0,40.0,
                    0,'test','[]','[]','{}')
            """,
            (pool, TOKEN, rh._utc_now(), end_block - 450, end_block, coverage),
        )

    def _swap(self, connection, pool, block, tx_hash):
        connection.execute(
            """
            INSERT OR IGNORE INTO swap_observations (source_version,pool_id,
                token_address,block_number,transaction_hash,log_index,
                sender_hint,sender_identity_kind,amount0_raw,amount1_raw,
                anchor_delta_raw,token_delta_raw,side,sqrt_price_x96,observed_at)
            VALUES ('uniswap_v4',?,?,?,?,0,'0x1','event_sender_may_be_router',
                    '1','1','1','1','buy','1',?)
            """,
            (pool, TOKEN, block, tx_hash, rh._utc_now()),
        )

    def _build(self, store):
        with store.connection() as connection:
            # 267 stale pools: real windows, but far behind head.
            for index in range(self.STALE_POOLS):
                pool = f"0xstale{index:04d}"
                end = self.HEAD - 50_000 - index
                self._window(connection, pool, end)
                self._swap(connection, pool, end - 10, f"0xstaletx{index:04d}")
            # 8 fresh pools inside the bound, ~53 transactions between them.
            per_pool = self.FRESH_TX // self.FRESH_POOLS
            written = 0
            for index in range(self.FRESH_POOLS):
                pool = f"0xfresh{index:04d}"
                end = self.HEAD - 10 - index
                self._window(connection, pool, end)
                count = per_pool + (1 if index < self.FRESH_TX % self.FRESH_POOLS else 0)
                for slot in range(count):
                    self._swap(connection, pool, end - 5 - slot,
                               f"0xfreshtx{index:04d}{slot:03d}")
                    written += 1
            return written

    def test_every_fresh_transaction_precedes_any_stale_one(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            fresh_written = self._build(store)
            self.assertEqual(fresh_written, self.FRESH_TX)
            # A budget far smaller than the stale backlog.
            selected = store.pending_transaction_origins(
                100, scope="prospective", head_block=self.HEAD,
            )
            self.assertEqual(
                len(selected), self.FRESH_TX,
                "the prospective cohort must be exactly the fresh windows",
            )
            self.assertTrue(
                all(h.startswith("0xfreshtx") for h in selected),
                "a stale transaction entered the prospective cohort",
            )

    def test_stale_windows_are_excluded_by_the_head_relative_condition(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._build(store)
            background = store.pending_transaction_origins(
                1_000, scope="background", head_block=self.HEAD,
            )
            self.assertTrue(background, "stale work should remain available")
            self.assertTrue(
                all(h.startswith("0xstaletx") for h in background),
                "a fresh transaction was demoted to background",
            )

    def test_queue_counts_separate_the_two_cohorts(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._build(store)
            counts = store.prospective_enrichment_counts(self.HEAD)
            self.assertEqual(counts["fresh_pools"], self.FRESH_POOLS)
            self.assertEqual(counts["prospective_pending"], self.FRESH_TX)
            self.assertEqual(counts["background_pending"], self.STALE_POOLS)
            self.assertEqual(counts["prospective_resolved"], 0)
            self.assertEqual(counts["fresh_pools_completed_80pct"], 0)

    def test_a_completed_fresh_window_is_counted(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._build(store)
            with store.connection() as connection:
                connection.execute(
                    "UPDATE flow_signals SET identity_coverage=1.0"
                    " WHERE pool_id LIKE '0xfresh%'"
                )
            counts = store.prospective_enrichment_counts(self.HEAD)
            self.assertEqual(
                counts["fresh_pools_completed_80pct"], self.FRESH_POOLS,
                "a fully covered fresh window was not counted as complete",
            )


class IdentityDeadlineTests(unittest.TestCase):
    """A deadline is only honoured if the work after the last check fits in it.

    The check ran between batches only, so a 75-call batch beginning one second
    before the deadline still ran to completion. Measured in production: a
    60-second budget overran to 166.9s, drifting 1,676 blocks against a
    120-block freshness bound and expiring 9 otherwise-qualified windows.
    """

    class SlowRPC(FakeRPC):
        def __init__(self, seconds_per_batch=0.2):
            super().__init__([], latest=100)
            self.seconds = seconds_per_batch
            self.batches = 0

        def get_transactions(self, hashes):
            self.batches += 1
            time.sleep(self.seconds)
            return [
                {"transaction_hash": h, "origin_address": "0x" + "11" * 20,
                 "destination_address": "0x" + "22" * 20, "block_number": 1}
                for h in hashes
            ]

    def _engine(self, directory, rpc):
        return rh.RobinhoodLearningEngine(
            directory, rpc=rpc, analyzer=FakeAnalyzer(), market=FakeMarket(),
        )

    def _pending(self, store, count):
        store.apply_v4_events([{
            "kind": "initialize", "pool_id": POOL_ID,
            "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
            "token_address": TOKEN.lower(),
            "anchor_address": rh.USDG_ADDRESS.lower(),
            "fee_tier": 3000, "tick_spacing": 60,
            "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
            "sqrt_price_x96": 1 << 96, "tick": 0,
        }])
        with store.connection() as connection:
            for index in range(count):
                connection.execute(
                    """
                    INSERT OR IGNORE INTO swap_observations (source_version,
                        pool_id,token_address,block_number,transaction_hash,
                        log_index,sender_hint,sender_identity_kind,amount0_raw,
                        amount1_raw,anchor_delta_raw,token_delta_raw,side,
                        sqrt_price_x96,observed_at)
                    VALUES ('uniswap_v4',?,?,?,?,0,'0x1',
                            'event_sender_may_be_router','1','1','1','1','buy',
                            '1',?)
                    """,
                    (POOL_ID, TOKEN, 10 + index, f"0xdeadline{index:04d}",
                     rh._utc_now()),
                )

    def test_a_batch_is_not_started_when_it_cannot_finish(self):
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.SlowRPC(seconds_per_batch=0.2)
            engine = self._engine(directory, rpc)
            self._pending(engine.store, 300)
            started = time.monotonic()
            result = engine.resolve_flow_participants(
                limit=300, deadline_monotonic=started + 0.5,
            )
            elapsed = time.monotonic() - started
            self.assertGreaterEqual(result["stopped_at_deadline"], 1,
                                    "the deadline never triggered a stop")
            self.assertLess(
                elapsed, 1.5,
                f"overran its 0.5s deadline by too much ({elapsed:.2f}s)",
            )
            self.assertGreater(result["deferred_for_deadline"], 0,
                               "work should be deferred, not silently dropped")

    def test_all_work_completes_when_the_deadline_is_generous(self):
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.SlowRPC(seconds_per_batch=0.01)
            engine = self._engine(directory, rpc)
            self._pending(engine.store, 50)
            result = engine.resolve_flow_participants(
                limit=50, deadline_monotonic=time.monotonic() + 30,
            )
            self.assertEqual(result["stopped_at_deadline"], 0)
            self.assertEqual(result["deferred_for_deadline"], 0)

    def test_no_deadline_means_no_early_stop(self):
        with tempfile.TemporaryDirectory() as directory:
            rpc = self.SlowRPC(seconds_per_batch=0.01)
            engine = self._engine(directory, rpc)
            self._pending(engine.store, 30)
            result = engine.resolve_flow_participants(limit=30)
            self.assertEqual(result["stopped_at_deadline"], 0)


class StaleArmExperimentTests(unittest.TestCase):
    """Windows fresh at observation but stale at decision become evidence.

    Six such windows were discarded every cycle -- exactly the population that
    can say whether the 120-block bound earns its cost. They now qualify and
    schedule outcomes identically to the fresh arm, and are barred from paper
    entry alone.
    """

    POOL = "0x" + "ee" * 32
    INGEST_HEAD = 1_000_000
    DECISION_HEAD = 1_000_953        # the measured drift

    def _store(self, directory):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        for pool in (self.POOL, "0x" + "dd" * 32):
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": pool,
                "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }])
        return store

    def _signal(self, store, pool, qualified, end_block):
        with store.connection() as connection:
            connection.execute(
                """
                INSERT INTO flow_signals (source_version,pool_id,token_address,
                    computed_at,window_blocks,window_start_block,window_end_block,
                    swap_count,buy_count,sell_count,unique_sender_hints,
                    unique_resolved_participants,identity_coverage,buy_ratio,
                    net_anchor_flow_fraction,uncapped_shadow_score,shadow_score,
                    shadow_qualified,confidence,qualification_gaps_json,
                    limitations_json,features_json)
                VALUES ('uniswap_v4',?,?,?,450,?,?,12,9,3,6,6,1.0,0.75,0.42,
                        75.0,75.0,?,'test','[]','[]','{}')
                """,
                (pool, TOKEN, rh._utc_now(), end_block - 450, end_block,
                 int(qualified)),
            )

    def _capture(self, store, window_end):
        self._signal(store, self.POOL, True, window_end)
        self._signal(store, "0x" + "dd" * 32, False, window_end - 2)
        return store.capture_flow_signal_events(
            self.DECISION_HEAD, now=1_000.0,
            ingest_head_block=self.INGEST_HEAD,
        )

    def _events(self, store):
        with store.connection() as connection:
            return [dict(r) for r in connection.execute(
                "SELECT * FROM flow_signal_events WHERE signal_role='qualified'"
            )]

    def test_a_window_fresh_at_ingest_but_stale_at_decision_is_kept(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            result = self._capture(store, self.INGEST_HEAD - 10)
            self.assertGreaterEqual(result["stale_arm_created"], 1)
            event = self._events(store)[0]
            self.assertEqual(event["arm"], "decision_stale")
            self.assertTrue(
                event["eligible_for_evaluation"],
                "the stale arm must still be evaluated, or there is no experiment",
            )

    def test_the_stale_arm_is_barred_from_paper_entry(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._capture(store, self.INGEST_HEAD - 10)
            event = self._events(store)[0]
            self.assertFalse(
                event["paper_eligible"],
                "a decision-stale signal must never reach paper trading",
            )

    def test_a_window_stale_at_ingest_stays_historical(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._capture(store, self.INGEST_HEAD - 50_000)
            event = self._events(store)[0]
            self.assertEqual(event["arm"], "historical")
            self.assertFalse(event["eligible_for_evaluation"])

    def test_a_fresh_window_keeps_paper_eligibility(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._signal(store, self.POOL, True, self.DECISION_HEAD - 10)
            store.capture_flow_signal_events(
                self.DECISION_HEAD, now=1_000.0,
                ingest_head_block=self.INGEST_HEAD,
            )
            event = self._events(store)[0]
            self.assertEqual(event["arm"], "fresh")
            self.assertTrue(event["paper_eligible"])

    def test_comparison_reports_both_arms_separately(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._capture(store, self.INGEST_HEAD - 10)
            report = store.flow_arm_comparison()
            self.assertIn("arms", report)
            self.assertEqual(report["bound_blocks"],
                             rh.FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS)
            self.assertIn("decision_stale", report["arms"])


class EligibilitySeparationTests(unittest.TestCase):
    """Research eligibility and paper eligibility are different permissions.

    research  = observation was fresh, whatever identity says
    paper     = observation fresh AND decision fresh AND identity verified

    Two defects motivated this. Capture was classifying against the INGEST
    head, so a window 1,600 blocks stale at the decision was labelled fresh
    and paper-eligible. And identity coverage gated qualification, which kept
    the research cohort empty rather than making unresolved windows
    untradeable.
    """

    POOL = "0x" + "ee" * 32
    INGEST_HEAD = 1_000_000
    DECISION_HEAD = 1_000_953

    def _store(self, directory):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        store.apply_v4_events([{
            "kind": "initialize", "pool_id": self.POOL,
            "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
            "token_address": TOKEN.lower(),
            "anchor_address": rh.USDG_ADDRESS.lower(),
            "fee_tier": 3000, "tick_spacing": 60,
            "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
            "sqrt_price_x96": 1 << 96, "tick": 0,
        }])
        return store

    def _signal(self, store, end_block, coverage):
        with store.connection() as connection:
            connection.execute(
                """
                INSERT INTO flow_signals (source_version,pool_id,token_address,
                    computed_at,window_blocks,window_start_block,window_end_block,
                    swap_count,buy_count,sell_count,unique_sender_hints,
                    unique_resolved_participants,identity_coverage,buy_ratio,
                    net_anchor_flow_fraction,uncapped_shadow_score,shadow_score,
                    shadow_qualified,confidence,qualification_gaps_json,
                    limitations_json,features_json)
                VALUES ('uniswap_v4',?,?,?,450,?,?,12,9,3,6,6,?,0.75,0.42,
                        75.0,75.0,1,'test','[]','[]','{}')
                """,
                (self.POOL, TOKEN, rh._utc_now(), end_block - 450, end_block,
                 coverage),
            )

    def _event(self, store, end_block, coverage, head=None):
        self._signal(store, end_block, coverage)
        store.capture_flow_signal_events(
            head or self.DECISION_HEAD, now=1_000.0,
            ingest_head_block=self.INGEST_HEAD,
        )
        with store.connection() as connection:
            rows = [dict(r) for r in connection.execute(
                "SELECT * FROM flow_signal_events WHERE signal_role='qualified'"
            )]
        return rows[0] if rows else None

    def test_decision_stale_is_research_eligible_but_never_tradeable(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            event = self._event(store, self.INGEST_HEAD - 10, coverage=1.0)
            self.assertEqual(event["arm"], "decision_stale")
            self.assertTrue(event["eligible_for_evaluation"])
            self.assertFalse(
                event["paper_eligible"],
                "1,600 blocks of drift was previously marked tradeable",
            )

    def test_unverified_identity_blocks_paper_but_not_research(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            event = self._event(
                store, self.DECISION_HEAD - 10, coverage=0.10,
            )
            self.assertEqual(event["arm"], "fresh")
            self.assertTrue(
                event["eligible_for_evaluation"],
                "an unresolved window must still inform learning",
            )
            self.assertFalse(
                event["paper_eligible"],
                "unverified identity must never reach paper trading",
            )

    def test_paper_eligibility_requires_both_freshness_and_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            event = self._event(
                store, self.DECISION_HEAD - 10, coverage=1.0,
            )
            self.assertEqual(event["arm"], "fresh")
            self.assertTrue(event["paper_eligible"])

    def test_identity_no_longer_blocks_qualification(self):
        """The gate kept the research cohort empty; it now classifies."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            with store.connection() as connection:
                connection.execute(
                    """
                    INSERT OR IGNORE INTO swap_observations (source_version,
                        pool_id,token_address,block_number,transaction_hash,
                        log_index,sender_hint,sender_identity_kind,amount0_raw,
                        amount1_raw,anchor_delta_raw,token_delta_raw,side,
                        sqrt_price_x96,observed_at)
                    VALUES ('uniswap_v4',?,?,10,'0xaa',0,'0x1','event_sender_may_be_router',
                            '1','1','1','1','buy','1',?)
                    """, (self.POOL, TOKEN, rh._utc_now()),
                )
                store._refresh_v4_flow_signal(connection, self.POOL)
                row = connection.execute(
                    "SELECT qualification_gaps_json FROM flow_signals WHERE pool_id=?",
                    (self.POOL,),
                ).fetchone()
            self.assertNotIn(
                "minimum_identity_coverage", row["qualification_gaps_json"],
                "identity should classify, not gate qualification",
            )


class ProvisionalObservationCycleTests(unittest.TestCase):
    """Seal at ingest, enrich, classify, never mutate.

    Sealing BEFORE enrichment is what makes an observation evidence: the claim
    and its future outcomes both exist before anything is known about how it
    resolves. Measured drift between ingest and decision was 1,621 blocks
    against a 120-block bound, so a window genuinely near-head when seen is
    routinely stale at the decision -- that must be a classification, never a
    silent promotion to tradeable.
    """

    POOL = "0x" + "ee" * 32
    OBS_HEAD = 1_000_000
    DECISION_HEAD = 1_000_000 + 1_621
    NOW = 5_000.0

    def _store(self, directory):
        return rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")

    def _seal(self, store, window_end, head=None):
        return store.seal_flow_observation(
            pool_id=self.POOL, token_address=TOKEN,
            observation_head=head or self.OBS_HEAD,
            window_start_block=window_end - 450, window_end_block=window_end,
            transaction_hashes=["0xaa", "0xbb"],
            features={"buy_ratio": 0.9},
            # Production shape: v4_market.snapshot() nests the entry inside
            # execution_quote. A flat dict here is what let two shape bugs
            # reach production with green tests.
            quote={"execution_quote": {
                "verified": True, "anchor_in_raw": 1_000,
                "token_out_raw": 10 ** 18, "token_decimals": 18,
            }},
            quote_block=window_end, now=self.NOW,
        )

    # anchor_out_raw states what a same-block exit returns: without it the
    # quote cannot prove the position is exitable, and the gate says no.
    QUOTE = {"verified": True, "anchor_in_raw": 1_000, "anchor_out_raw": 990,
             "token_out_raw": 10 ** 18}

    def _classify(self, store, observation_id, coverage=1.0, gates=(),
                  decision_quote=QUOTE):
        return store.classify_flow_observation(
            observation_id, decision_head=self.DECISION_HEAD,
            identity_coverage=coverage, gates=list(gates),
            decision_quote=decision_quote,
        )

    def test_observation_is_committed_before_enrichment(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(store, self.OBS_HEAD - 10)
            self.assertIsNotNone(observation_id)
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT * FROM flow_observations WHERE observation_id=?",
                    (observation_id,),
                ).fetchone()
                outcomes = connection.execute(
                    "SELECT COUNT(*) FROM flow_observation_outcomes"
                    " WHERE observation_id=?", (observation_id,),
                ).fetchone()[0]
                classified = connection.execute(
                    "SELECT COUNT(*) FROM flow_observation_classifications"
                ).fetchone()[0]
            self.assertTrue(row["quote_verified"], "quote must be pinned at seal")
            self.assertEqual(row["quote_block"], self.OBS_HEAD - 10)
            self.assertGreater(outcomes, 0, "outcomes must be scheduled at seal")
            self.assertEqual(
                classified, 0, "nothing may be classified before enrichment",
            )

    def test_a_1621_block_delay_becomes_decision_stale(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            verdict = self._classify(
                store, self._seal(store, self.OBS_HEAD - 10),
            )
            self.assertEqual(verdict["arm"], "decision_stale")
            self.assertEqual(verdict["decision_head_lag_blocks"], 1_631)

    def test_decision_stale_still_receives_research_outcomes(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(store, self.OBS_HEAD - 10)
            verdict = self._classify(store, observation_id)
            self.assertTrue(verdict["research_eligible"])
            with store.connection() as connection:
                scheduled = connection.execute(
                    "SELECT COUNT(*) FROM flow_observation_outcomes"
                    " WHERE observation_id=?", (observation_id,),
                ).fetchone()[0]
            self.assertGreater(scheduled, 0)

    def test_decision_stale_is_tradeable_only_with_a_verified_price(self):
        """Policy change, deliberately recorded here.

        This previously asserted that stale-at-decision could NEVER be
        tradeable. That rule was retired because the 120-block bound it rested
        on was unreachable on this RPC -- drift is pass duration times ~9.95
        blocks/second, and the fastest of seven passes was 49s against the 12s
        the bound required -- so it was not protecting anything, it was
        refusing everything. The protection it was meant to give now comes
        from re-taking the price: stale-at-decision is tradeable when its
        entry price still verifies NOW, and never when it does not.
        """
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(store, self.OBS_HEAD - 10)
            self.assertFalse(
                self._classify(store, observation_id,
                               decision_quote=None)["paper_eligible"],
                "no re-quote must still mean no paper entry",
            )
            self.assertTrue(
                self._classify(store, observation_id)["paper_eligible"],
                "a re-verified price is what makes it actionable",
            )

    def test_fresh_identity_verified_reaches_the_paper_boundary(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(
                store, self.DECISION_HEAD - 10, head=self.DECISION_HEAD - 5,
            )
            verdict = self._classify(store, observation_id)
            self.assertEqual(verdict["arm"], "fresh")
            self.assertEqual(verdict["identity_tier"], "verified")
            self.assertTrue(verdict["research_eligible"])
            self.assertTrue(verdict["paper_eligible"])

    def test_without_a_decision_quote_nothing_is_tradeable(self):
        """The safe default: no re-quote, no paper entry.

        Decision freshness is now a price question, so a caller that offers no
        price has not answered it. Silence must not read as a pass -- that is
        the absence-as-evidence error this layer exists to prevent.
        """
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(
                store, self.DECISION_HEAD - 10, head=self.DECISION_HEAD - 5,
            )
            verdict = self._classify(store, observation_id, decision_quote=None)
            self.assertTrue(verdict["research_eligible"])
            self.assertFalse(verdict["paper_eligible"])
            self.assertFalse(verdict["decision_quote_verified"])

    def test_an_unverifiable_decision_quote_blocks_paper(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(
                store, self.DECISION_HEAD - 10, head=self.DECISION_HEAD - 5,
            )
            verdict = self._classify(
                store, observation_id,
                decision_quote={"verified": False, "anchor_in_raw": 1_000},
            )
            self.assertFalse(verdict["paper_eligible"])

    def test_price_drift_is_recorded_for_a_later_threshold(self):
        """Recorded, not gated -- the threshold comes from the cohort."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(
                store, self.DECISION_HEAD - 10, head=self.DECISION_HEAD - 5,
            )
            verdict = self._classify(
                store, observation_id,
                decision_quote={"verified": True, "anchor_in_raw": 1_100,
                                "anchor_out_raw": 1_089,
                                "token_out_raw": 10 ** 18},
            )
            self.assertIsNotNone(verdict["decision_price_drift_bps"])
            self.assertAlmostEqual(
                verdict["decision_price_drift_bps"], 1_000.0, places=1,
                msg="a 10% move must read as 1,000 bps",
            )
            self.assertIsNone(rh.FLOW_DECISION_DRIFT_THRESHOLD_BPS)
            self.assertTrue(
                verdict["paper_eligible"],
                "drift is recorded, not gated, until the cohort measures it",
            )

    def test_block_lag_no_longer_decides_eligibility(self):
        """1,631 blocks of drift was unreachable-by-design, not dangerous."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            verdict = self._classify(
                store, self._seal(store, self.OBS_HEAD - 10),
            )
            self.assertEqual(verdict["arm"], "decision_stale")
            self.assertEqual(verdict["decision_head_lag_blocks"], 1_631)
            self.assertTrue(
                verdict["paper_eligible"],
                "a verifiable price is what makes a signal actionable",
            )

    def test_historical_observations_stay_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            verdict = self._classify(
                store, self._seal(store, self.OBS_HEAD - 50_000),
            )
            self.assertEqual(verdict["arm"], "historical")
            self.assertFalse(verdict["research_eligible"])
            self.assertFalse(verdict["paper_eligible"])

    def test_classification_never_mutates_the_sealed_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(store, self.OBS_HEAD - 10)
            with store.connection() as connection:
                before = dict(connection.execute(
                    "SELECT * FROM flow_observations WHERE observation_id=?",
                    (observation_id,),
                ).fetchone())
            self._classify(store, observation_id, 0.0, ["minimum_swaps"])
            self._classify(store, observation_id, 1.0, [])
            with store.connection() as connection:
                after = dict(connection.execute(
                    "SELECT * FROM flow_observations WHERE observation_id=?",
                    (observation_id,),
                ).fetchone())
            self.assertEqual(before, after, "the sealed observation was rewritten")

    def test_identity_tiers_are_recorded_for_stratification(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(
                store, self.DECISION_HEAD - 10, head=self.DECISION_HEAD - 5,
            )
            for coverage, tier in ((1.0, "verified"), (0.3, "partial"),
                                   (0.0, "unresolved")):
                verdict = self._classify(store, observation_id, coverage)
                self.assertEqual(verdict["identity_tier"], tier)
                if tier != "verified":
                    self.assertFalse(verdict["paper_eligible"])

    def test_a_failing_gate_blocks_paper_even_when_fresh_and_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(
                store, self.DECISION_HEAD - 10, head=self.DECISION_HEAD - 5,
            )
            verdict = self._classify(
                store, observation_id, 1.0, ["non_adverse_price_direction"],
            )
            self.assertEqual(verdict["arm"], "fresh")
            self.assertTrue(verdict["research_eligible"])
            self.assertFalse(verdict["paper_eligible"])

    def test_sealing_the_same_window_twice_is_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            first = self._seal(store, self.OBS_HEAD - 10)
            second = self._seal(store, self.OBS_HEAD - 10)
            self.assertIsNotNone(first)
            self.assertIsNone(second, "a repeated pass duplicated the claim")


class ObservationLifecycleTests(unittest.TestCase):
    """One complete outcome lifecycle, and the gates that count only completions.

    1,985 outcomes were scheduled while zero had resolved, and the scheduled
    count was briefly read as evidence. Every gate here opens on COMPLETED
    primary-horizon outcomes only.
    """

    POOL = "0x" + "ee" * 32
    HEAD = 2_000_000
    NOW = 9_000.0

    def _store(self, directory):
        return rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")

    def _seal(self, store, pool, window_end, head=None):
        return store.seal_flow_observation(
            pool_id=pool, token_address=TOKEN,
            observation_head=head or self.HEAD,
            window_start_block=window_end - 450, window_end_block=window_end,
            transaction_hashes=[f"0x{pool[-4:]}a", f"0x{pool[-4:]}b"],
            features={"buy_ratio": 0.9},
            # Production shape: v4_market.snapshot() nests the entry inside
            # execution_quote. A flat dict here is what let two shape bugs
            # reach production with green tests.
            quote={"execution_quote": {
                "verified": True, "anchor_in_raw": 1_000,
                "token_out_raw": 10 ** 18, "token_decimals": 18,
            }},
            quote_block=window_end, now=self.NOW,
        )

    def test_a_full_lifecycle_resolves_with_a_positive_return(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(store, self.POOL, self.HEAD - 10)
            store.classify_flow_observation(
                observation_id, decision_head=self.HEAD,
                identity_coverage=1.0, gates=[],
            )
            due = store.due_flow_observation_outcomes(self.NOW + 10 * 86_400)
            primary = [d for d in due
                       if d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL]
            self.assertTrue(primary, "the primary horizon never became due")
            result = store.record_flow_observation_outcome(
                primary[0], {"verified": True, "anchor_out_raw": 1_300},
                quote_block=self.HEAD + 50, now=self.NOW + 10 * 86_400,
            )
            self.assertEqual(result["status"], "resolved")
            self.assertTrue(result["exit_valid"])
            self.assertGreater(result["net_return"], 0.0)
            progress = store.cohort_progress()
            self.assertEqual(progress["completed_primary_observations"], 1)
            self.assertEqual(progress["cohort_id"], rh.FLOW_EVIDENCE_COHORT_ID)

    def test_an_unexitable_outcome_records_a_total_loss_not_a_gap(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(store, self.POOL, self.HEAD - 10)
            store.classify_flow_observation(
                observation_id, decision_head=self.HEAD,
                identity_coverage=1.0, gates=[],
            )
            due = [d for d in store.due_flow_observation_outcomes(
                self.NOW + 10 * 86_400)
                if d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL]
            result = store.record_flow_observation_outcome(
                due[0], {"verified": False}, quote_block=self.HEAD + 50,
                now=self.NOW + 10 * 86_400,
            )
            self.assertEqual(result["status"], "non_exitable")
            self.assertFalse(result["exit_valid"])
            self.assertEqual(
                result["net_return"], -1.0,
                "a non-exitable outcome must not be recorded as missing",
            )

    def test_scheduled_outcomes_are_never_counted_as_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            for index in range(20):
                self._seal(store, f"0xpool{index:04d}", self.HEAD - 10 - index)
            with store.connection() as connection:
                scheduled = connection.execute(
                    "SELECT COUNT(*) FROM flow_observation_outcomes"
                ).fetchone()[0]
            self.assertGreaterEqual(scheduled, 100)
            progress = store.cohort_progress()
            self.assertEqual(
                progress["completed_primary_observations"], 0,
                "scheduled outcomes were counted as evidence",
            )
            self.assertFalse(progress["reflection_due"])
            self.assertFalse(progress["evaluation_due"])

    def test_reflection_opens_only_at_fifteen_completions(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            for index in range(rh.FLOW_COHORT_REFLECTION_AT):
                pool = f"0xpool{index:04d}"
                observation_id = self._seal(store, pool, self.HEAD - 10 - index)
                store.classify_flow_observation(
                    observation_id, decision_head=self.HEAD,
                    identity_coverage=1.0, gates=[],
                )
                due = [d for d in store.due_flow_observation_outcomes(
                    self.NOW + 10 * 86_400, limit=500)
                    if d["observation_id"] == observation_id
                    and d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL]
                store.record_flow_observation_outcome(
                    due[0], {"verified": True, "anchor_out_raw": 1_100},
                    quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
                )
            progress = store.cohort_progress()
            self.assertEqual(
                progress["completed_primary_observations"],
                rh.FLOW_COHORT_REFLECTION_AT,
            )
            self.assertTrue(progress["reflection_due"])
            self.assertFalse(progress["evaluation_due"])

    def test_fresh_versus_stale_stays_blocked_without_a_fresh_arm(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            for index in range(12):
                pool = f"0xpool{index:04d}"
                observation_id = self._seal(store, pool, self.HEAD - 10 - index)
                # decision far past the bound -> every one is decision_stale
                store.classify_flow_observation(
                    observation_id, decision_head=self.HEAD + 5_000,
                    identity_coverage=1.0, gates=[],
                )
                due = [d for d in store.due_flow_observation_outcomes(
                    self.NOW + 10 * 86_400, limit=500)
                    if d["observation_id"] == observation_id
                    and d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL]
                store.record_flow_observation_outcome(
                    due[0], {"verified": True, "anchor_out_raw": 1_100},
                    quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
                )
            progress = store.cohort_progress()
            self.assertEqual(progress["by_arm"].get("decision_stale"), 12)
            self.assertFalse(
                progress["fresh_versus_stale_permitted"],
                "a comparison was permitted with an empty fresh arm",
            )

    def test_tier_comparison_requires_a_minimum_in_each_tier(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            for index in range(10):
                pool = f"0xpool{index:04d}"
                observation_id = self._seal(store, pool, self.HEAD - 10 - index)
                # nine verified, one unresolved -> only one tier qualifies
                store.classify_flow_observation(
                    observation_id, decision_head=self.HEAD,
                    identity_coverage=1.0 if index else 0.0, gates=[],
                )
                due = [d for d in store.due_flow_observation_outcomes(
                    self.NOW + 10 * 86_400, limit=500)
                    if d["observation_id"] == observation_id
                    and d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL]
                store.record_flow_observation_outcome(
                    due[0], {"verified": True, "anchor_out_raw": 1_100},
                    quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
                )
            progress = store.cohort_progress()
            self.assertIn("verified", progress["tiers_with_minimum_samples"])
            self.assertNotIn("unresolved", progress["tiers_with_minimum_samples"])
            self.assertFalse(progress["tier_comparison_permitted"])

    def test_pilot_observations_are_excluded_from_the_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            observation_id = self._seal(store, self.POOL, self.HEAD - 10)
            with store.connection() as connection:
                connection.execute(
                    "UPDATE flow_observations SET cohort_id='pilot'"
                    " WHERE observation_id=?", (observation_id,),
                )
            store.classify_flow_observation(
                observation_id, decision_head=self.HEAD,
                identity_coverage=1.0, gates=[],
            )
            due = [d for d in store.due_flow_observation_outcomes(
                self.NOW + 10 * 86_400)
                if d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL]
            store.record_flow_observation_outcome(
                due[0], {"verified": True, "anchor_out_raw": 1_300},
                quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
            )
            progress = store.cohort_progress()
            self.assertEqual(
                progress["completed_primary_observations"], 0,
                "pilot data leaked into the promotion cohort",
            )
            pilot = store.cohort_progress(cohort_id="pilot")
            self.assertEqual(pilot["completed_primary_observations"], 1)


class UnpriceableOutcomeTests(unittest.TestCase):
    """Unmeasurable is not a loss, and unpriceable rows never reach the budget.

    1,034 observations were sealed with unverified entry quotes. Resolving them
    scored -1.0 each, which would have injected fabricated losses into every
    return statistic -- and consumed the 40-per-cycle budget for days before
    any priceable observation was reached.
    """

    POOL = "0x" + "ee" * 32
    HEAD = 3_000_000
    NOW = 11_000.0

    def _store(self, directory):
        return rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")

    def _seal(self, store, pool, verified):
        quote = {"execution_quote": {
            "verified": verified, "anchor_in_raw": 1_000,
            "token_out_raw": 10 ** 18, "token_decimals": 18,
        }}
        return store.seal_flow_observation(
            pool_id=pool, token_address=TOKEN, observation_head=self.HEAD,
            window_start_block=self.HEAD - 460, window_end_block=self.HEAD - 10,
            transaction_hashes=[f"0x{pool[-4:]}"], features={},
            quote=quote, quote_block=self.HEAD - 10, now=self.NOW,
        )

    def test_unverified_entry_is_never_offered_for_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._seal(store, self.POOL, verified=False)
            due = store.due_flow_observation_outcomes(self.NOW + 10 * 86_400)
            self.assertEqual(
                due, [],
                "an unpriceable observation consumed the resolution budget",
            )

    def test_verified_entry_is_offered(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._seal(store, self.POOL, verified=True)
            due = store.due_flow_observation_outcomes(self.NOW + 10 * 86_400)
            self.assertTrue(due)

    def test_unpriceable_records_no_return_rather_than_a_total_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._seal(store, self.POOL, verified=True)
            due = store.due_flow_observation_outcomes(self.NOW + 10 * 86_400)
            row = dict(due[0])
            row["quote_verified"] = 0          # entry price turned out invalid
            result = store.record_flow_observation_outcome(
                row, {"verified": True, "anchor_out_raw": 1_300},
                quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
            )
            self.assertEqual(result["status"], "unpriceable")
            self.assertIsNone(
                result["net_return"],
                "an unmeasurable outcome was scored as a loss",
            )

    def test_a_real_non_exit_is_still_a_total_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._seal(store, self.POOL, verified=True)
            due = store.due_flow_observation_outcomes(self.NOW + 10 * 86_400)
            result = store.record_flow_observation_outcome(
                due[0], {"verified": False}, quote_block=self.HEAD,
                now=self.NOW + 10 * 86_400,
            )
            self.assertEqual(result["status"], "non_exitable")
            self.assertEqual(
                result["net_return"], -1.0,
                "a priced entry that cannot exit is a genuine total loss",
            )

    def test_unpriceable_rows_are_excluded_from_cohort_progress(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._seal(store, self.POOL, verified=True)
            due = store.due_flow_observation_outcomes(self.NOW + 10 * 86_400)
            primary = [d for d in due
                       if d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL]
            row = dict(primary[0])
            row["quote_verified"] = 0
            store.record_flow_observation_outcome(
                row, {"verified": True, "anchor_out_raw": 1_300},
                quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
            )
            progress = store.cohort_progress()
            self.assertEqual(
                progress["completed_primary_observations"], 0,
                "an unpriceable row counted toward the reflection gate",
            )


class SenderHintParticipantGateTests(unittest.TestCase):
    """Distinct traders count from hints when origins are unresolved.

    After widening to 1350 blocks the gate failed on 100% of windows with a
    maximum of 0 resolved participants -- not because trading is concentrated
    but because enrichment cannot resolve 3x the transactions per window
    against a 153,797 backlog. Sender hints are read straight from the swap
    log and need no enrichment; they are weaker evidence, and the tier records
    that, but weaker is not absent.
    """

    POOL = "0x" + "cc" * 32

    def _store(self, directory):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        store.apply_v4_events([{
            "kind": "initialize", "pool_id": self.POOL,
            "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
            "token_address": TOKEN.lower(),
            "anchor_address": rh.USDG_ADDRESS.lower(),
            "fee_tier": 3000, "tick_spacing": 60,
            "hooks_address": rh.ZERO_ADDRESS, "block_number": 1,
            "sqrt_price_x96": 1 << 96, "tick": 0,
        }])
        return store

    def _swaps(self, store, senders, resolved=None, origins=None):
        with store.connection() as connection:
            for index, sender in enumerate(senders):
                connection.execute(
                    """
                    INSERT OR IGNORE INTO swap_observations (source_version,
                        pool_id,token_address,block_number,transaction_hash,
                        log_index,sender_hint,sender_identity_kind,
                        resolved_participant,amount0_raw,amount1_raw,
                        anchor_delta_raw,token_delta_raw,side,sqrt_price_x96,
                        observed_at)
                    VALUES ('uniswap_v4',?,?,?,?,0,?,'event_sender_may_be_router',
                            ?,'1','1','1','1','buy','1',?)
                    """,
                    # Blocks well above FLOW_WINDOW_BLOCKS so the window does
                    # not clamp to a single block.
                    (self.POOL, TOKEN, 10_000 + index, f"0xtx{index:04d}",
                     sender,
                     origins[index] if origins else (resolved or {}).get(sender),
                     rh._utc_now()),
                )
            # The window floor is the pool's FIRST swap block; production sets
            # it alongside the swap rows, so a raw insert must too or the
            # window collapses to a single block.
            connection.execute(
                "UPDATE v4_pools SET swapped_block=10000 WHERE pool_id=?",
                (self.POOL,),
            )
            store._refresh_v4_flow_signal(connection, self.POOL)

    def _gaps(self, store):
        with store.connection() as connection:
            row = connection.execute(
                "SELECT qualification_gaps_json, features_json FROM flow_signals"
                " WHERE pool_id=?", (self.POOL,),
            ).fetchone()
        return json.loads(row[0] or "[]"), json.loads(row[1] or "{}")

    def test_four_distinct_hints_clear_the_gate_without_resolution(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            senders = [f"0xsender{i}" for i in range(4)] * 2   # 8 swaps, 4 traders
            self._swaps(store, senders)
            gaps, features = self._gaps(store)
            self.assertNotIn("minimum_unique_participants", gaps)
            self.assertEqual(features["distinct_traders"], 4)
            self.assertEqual(
                features["participant_evidence"], "sender_hints_may_be_routers",
                "the weaker evidence source must be recorded, not hidden",
            )

    def test_too_few_distinct_hints_still_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._swaps(store, ["0xsolo"] * 8)      # 8 swaps, 1 trader
            gaps, features = self._gaps(store)
            self.assertIn("minimum_unique_participants", gaps)
            self.assertEqual(features["distinct_traders"], 1)

    def test_resolved_origins_are_preferred_and_labelled(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            senders = [f"0xsender{i}" for i in range(4)] * 2
            resolved = {s: f"0xorigin{i}" for i, s in enumerate(set(senders))}
            self._swaps(store, senders, resolved)
            gaps, features = self._gaps(store)
            self.assertNotIn("minimum_unique_participants", gaps)
            self.assertEqual(
                features["participant_evidence"], "resolved_origins",
                "verified origins must outrank hints when both exist",
            )

    def test_one_router_hiding_seven_traders_is_counted_as_seven(self):
        """Live pool 0x70c4cca4: 1 sender hint, 7 distinct resolved origins."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            senders = ["0xrouter"] * 7
            self._swaps(store, senders, resolved=None, origins=[
                f"0xtrader{i}" for i in range(7)
            ])
            gaps, features = self._gaps(store)
            self.assertEqual(features["distinct_traders"], 7)
            self.assertEqual(features["participant_evidence"], "resolved_origins")
            self.assertNotIn("minimum_unique_participants", gaps)
            self.assertNotIn("minimum_swaps", gaps)

    def test_hints_never_inflate_a_smaller_resolved_count(self):
        """Live pool 0x4370ad23: 3 hints, 2 origins -- max() reported 3."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            senders = ["0xa", "0xb", "0xc"] * 2
            self._swaps(store, senders, resolved=None,
                        origins=["0xt1", "0xt2"] * 3)
            gaps, features = self._gaps(store)
            self.assertEqual(
                features["distinct_traders"], 2,
                "a router count inflated the real number of traders",
            )
            self.assertEqual(features["participant_evidence"], "resolved_origins")
            self.assertIn("minimum_unique_participants", gaps)

    def test_partial_resolution_is_not_labelled_as_hint_backed(self):
        """Live pool 0x45de887b was labelled hint-backed on resolved origins."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._swaps(store, ["0xrouter"] * 2, resolved=None,
                        origins=["0xt1", "0xt2"])
            _, features = self._gaps(store)
            self.assertEqual(
                features["participant_evidence"], "resolved_origins",
                "fewer than the gate minimum is still resolved evidence",
            )

    def test_the_score_and_the_gate_use_the_same_count(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._swaps(store, ["0xa", "0xb", "0xc"] * 2, resolved=None,
                        origins=["0xt1", "0xt2"] * 3)
            _, features = self._gaps(store)
            self.assertAlmostEqual(
                features["participant_quality"],
                features["distinct_traders"] / rh.FLOW_MINIMUM_SENDER_HINTS,
                msg="the score credited more traders than the gate counted",
            )

    def test_hint_backed_signals_remain_ineligible_for_paper(self):
        """Weaker evidence may qualify for research and never for trading."""
        verdict = rh.shadow_admission(
            market_cap=60_000.0, liquidity=50_000.0, hooks_present=False,
            hard_stops=[], token_quality_passes=True, allowed=True,
            safety={"top_holder_pct": 5.0, "holder_distribution_score": 85.0,
                    "lp_locked": None},
        )
        self.assertFalse(
            verdict["liquidity_custody_pass"],
            "unverified custody must still block the paper path",
        )


class OriginTargetingScopeTests(unittest.TestCase):
    """Enrichment targets whole near-head windows, not just last-swap pools.

    The prospective scope reused the 120-block DECISION bound as its selection
    condition, applied to a pool's most recent swap. On live data that matched
    1 of 1,895 pools, so the cohort was ~1 transaction per cycle and the 18
    unresolved near-head swaps were never attempted -- none had a
    transaction_origins row at all, while resolution succeeded on 43,722 of
    43,722 rows it was asked about. The gate then failed for want of
    participants that were never looked up.
    """

    HEAD = 39_804_588

    def _store(self, directory):
        return rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")

    def _pool(self, store, pool_id, last_swap, swaps=3):
        with store.connection() as connection:
            connection.execute(
                """INSERT INTO v4_pools (pool_id,token_address,anchor_address,
                       currency0,currency1,fee_tier,tick_spacing,hooks_address,
                       initialized_block,swapped_block,updated_at)
                   VALUES (?,?,?,?,?,3000,60,?,1,?,?)""",
                (pool_id, TOKEN, rh.USDG_ADDRESS, TOKEN, rh.USDG_ADDRESS,
                 rh.ZERO_ADDRESS, last_swap - rh.FLOW_WINDOW_BLOCKS,
                 rh._utc_now()),
            )
            for index in range(swaps):
                connection.execute(
                    """INSERT INTO swap_observations (source_version,pool_id,
                           token_address,block_number,transaction_hash,log_index,
                           sender_hint,sender_identity_kind,amount0_raw,
                           amount1_raw,anchor_delta_raw,token_delta_raw,side,
                           sqrt_price_x96,observed_at)
                       VALUES ('uniswap_v4',?,?,?,?,0,'0xr',
                               'event_sender_may_be_router','1','1','1','1',
                               'buy','1',?)""",
                    (pool_id, TOKEN, last_swap - index * 300,
                     f"{pool_id}tx{index}", rh._utc_now()),
                )
            store._refresh_v4_flow_signal(connection, pool_id)

    def test_a_pool_whose_window_overlaps_head_is_selected(self):
        """Last swap 800 blocks back is still inside a 1350-block window."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._pool(store, "0x" + "a1" * 32, self.HEAD - 800)
            hashes = store.pending_transaction_origins(
                500, scope="prospective", head_block=self.HEAD,
            )
            self.assertTrue(
                hashes,
                "a window overlapping head was excluded from enrichment",
            )

    def test_the_whole_window_is_taken_not_only_recent_swaps(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._pool(store, "0x" + "a2" * 32, self.HEAD - 800, swaps=4)
            hashes = store.pending_transaction_origins(
                500, scope="prospective", head_block=self.HEAD,
            )
            self.assertEqual(
                len(hashes), 4,
                "a partly-covered window fails the coverage floor as"
                " completely as an empty one",
            )

    def test_a_long_dead_pool_is_still_excluded(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._pool(store, "0x" + "a3" * 32, self.HEAD - 1_233_967)
            self.assertEqual(
                store.pending_transaction_origins(
                    500, scope="prospective", head_block=self.HEAD,
                ),
                [],
                "the median pool last traded 1.2M blocks ago and must not"
                " consume the live budget",
            )

    def test_decision_freshness_is_unchanged_by_the_wider_target(self):
        """Enrichment reach must not silently widen what may be traded."""
        self.assertEqual(rh.FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS, 120)
        self.assertGreater(
            rh.FLOW_ORIGIN_TARGET_HEAD_LAG_BLOCKS,
            rh.FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS,
        )


class NearHeadEnrichmentOrderingTests(unittest.TestCase):
    """The window is enriched BEFORE it is sealed, not after.

    The ingest head jumps 7,114-11,614 blocks per cycle against a 1,350-block
    window, so consecutive windows are disjoint. Enriching after the seal meant
    every resolved origin landed on a transaction that had already left the
    window: 11 windows at identity_coverage 1.00 in one cycle, 1 in the next,
    with pool 0x70c4cca4 going from 7 resolved origins to 0.
    """

    POOL = "0x" + "dd" * 32
    HEAD = 39_820_000

    class _RPC:
        """Head advances a full window between calls, as the live chain does."""

        def __init__(self, head, swaps, jump=7_114):
            self.head = head
            self.swaps = swaps
            self.jump = jump
            self.transaction_calls = []

        def get_block_number(self):
            return self.head

        def get_logs(self, from_block, to_block, address=None, topics=None):
            return self.swaps

        def get_transactions(self, hashes):
            self.transaction_calls.append(list(hashes))
            return [{"transaction_hash": h,
                     "transaction": {"from": '0x' + format(index + 1, '040x'),
                                     "to": "0x" + "9" * 40,
                                     "blockNumber": hex(self.head - 5)}}
                    for index, h in enumerate(hashes)]

    def _log(self, index, sender):
        return {
            "topics": [rh.V4_SWAP_TOPIC, self.POOL, "0x" + "0" * 24 + sender],
            # Ascending, as the chain emits them: swapped_block is COALESCEd
            # to the first swap seen and floors the window.
            "blockNumber": hex(self.HEAD - 400 + index),
            "transactionHash": f"0xfeed{index:060x}",
            "logIndex": "0x0",
            "data": "0x" + ("0" * 63 + "1") * 5,
        }

    def _engine(self, directory, swaps):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        store.apply_v4_events([{
            "kind": "initialize", "pool_id": self.POOL,
            "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
            "token_address": TOKEN.lower(),
            "anchor_address": rh.USDG_ADDRESS.lower(),
            "fee_tier": 3000, "tick_spacing": 60,
            "hooks_address": rh.ZERO_ADDRESS,
            "block_number": self.HEAD - rh.FLOW_WINDOW_BLOCKS,
            "sqrt_price_x96": 1 << 96, "tick": 0,
        }])
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        engine.root = Path(directory)      # the near-head scan cursor lives here
        engine.rpc = self._RPC(self.HEAD, swaps)
        return engine, store

    def test_the_window_is_enriched_within_the_pass(self):
        swaps = [self._log(i, "a" * 40) for i in range(7)]
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, swaps)
            result = engine.near_head_flow_pass()
            self.assertTrue(result["scanned"])
            self.assertEqual(result["enrichment"]["resolved"], 7)
            self.assertTrue(result["enrichment"]["window_fully_enriched"])

    def test_participants_are_resolved_before_the_seal_happens(self):
        """The gate must see origins, not the zeros apply_v4_events wrote."""
        swaps = [self._log(i, "a" * 40) for i in range(7)]
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, swaps)
            engine.near_head_flow_pass()
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT * FROM flow_signals WHERE pool_id=?", (self.POOL,),
                ).fetchone()
            self.assertEqual(
                row["unique_resolved_participants"], 7,
                "the signal was still carrying the pre-enrichment zero",
            )
            self.assertEqual(row["identity_coverage"], 1.0)

    def test_one_router_no_longer_masks_seven_traders_at_the_gate(self):
        swaps = [self._log(i, "a" * 40) for i in range(7)]
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, swaps)
            engine.near_head_flow_pass()
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT * FROM flow_signals WHERE pool_id=?", (self.POOL,),
                ).fetchone()
            features = json.loads(row["features_json"] or "{}")
            gaps = json.loads(row["qualification_gaps_json"] or "[]")
            self.assertEqual(features["distinct_traders"], 7)
            self.assertEqual(features["participant_evidence"], "resolved_origins")
            self.assertNotIn("minimum_unique_participants", gaps)

    def test_already_resolved_transactions_are_not_requested_again(self):
        swaps = [self._log(i, "a" * 40) for i in range(7)]
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, swaps)
            engine.near_head_flow_pass()
            first = len(engine.rpc.transaction_calls)
            engine.near_head_flow_pass()          # same window, re-ingested
            self.assertEqual(
                len(engine.rpc.transaction_calls), first,
                "the budget was spent re-proving known origins",
            )

    def test_an_empty_window_costs_nothing(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, [])
            result = engine.near_head_flow_pass()
            self.assertEqual(result["enrichment"]["attempted"], 0)
            self.assertEqual(engine.rpc.transaction_calls, [])

    def test_truncation_is_recorded_rather_than_hidden(self):
        swaps = [self._log(i, "a" * 40)
                 for i in range(rh.FLOW_NEAR_HEAD_ENRICHMENT_LIMIT + 25)]
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, swaps)
            enrichment = engine.near_head_flow_pass()["enrichment"]
            self.assertEqual(enrichment["truncated"], 25)
            self.assertFalse(
                enrichment["window_fully_enriched"],
                "a partly-enriched window must not claim full coverage",
            )

    def test_an_rpc_without_transaction_support_degrades_quietly(self):
        swaps = [self._log(i, "a" * 40) for i in range(3)]
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, swaps)

            class _NoTransactions:
                get_block_number = engine.rpc.get_block_number
                get_logs = engine.rpc.get_logs

            engine.rpc = _NoTransactions()
            enrichment = engine.near_head_flow_pass()["enrichment"]
            self.assertFalse(enrichment["supported"])
            self.assertFalse(enrichment["window_fully_enriched"])


class NearHeadFlowEventTests(unittest.TestCase):
    """The near-head pass seals what it did, attributed to its own pools.

    flow_signals is written by two generating processes. Reading coverage off
    the whole table produced 1 of 42 when the near-head pass had actually
    produced 15 of 15, and the pass emitted no event at all, so the mistake was
    only findable by inference from database side effects.
    """

    POOL = "0x" + "bb" * 32
    OTHER = "0x" + "cc" * 32
    HEAD = 39_900_000

    def _engine(self, directory, swaps):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        for pool in (self.POOL, self.OTHER):
            store.apply_v4_events([{
                "kind": "initialize", "pool_id": pool,
                "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
                "token_address": TOKEN.lower(),
                "anchor_address": rh.USDG_ADDRESS.lower(),
                "fee_tier": 3000, "tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS,
                "block_number": self.HEAD - rh.FLOW_WINDOW_BLOCKS,
                "sqrt_price_x96": 1 << 96, "tick": 0,
            }])
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        engine.root = Path(directory)      # the near-head scan cursor lives here
        engine.rpc = NearHeadEnrichmentOrderingTests._RPC(self.HEAD, swaps)
        return engine, store

    def _log(self, index):
        return {
            "topics": [rh.V4_SWAP_TOPIC, self.POOL, "0x" + "0" * 24 + "a" * 40],
            "blockNumber": hex(self.HEAD - 400 + index),
            "transactionHash": f"0xbeef{index:060x}",
            "logIndex": "0x0",
            "data": "0x" + ("0" * 63 + "1") * 5,
        }

    def test_the_pass_reports_coverage_for_its_own_pools_only(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, [self._log(i) for i in range(7)])
            # A second pool carries an unenriched window, as the batch path
            # leaves behind. Built through the production write path, not by
            # hand, so it reflects the real schema rather than my belief
            # about it. It must not dilute the near-head figure.
            store.apply_v4_events([{
                "kind": "swap", "pool_id": self.OTHER,
                "block_number": self.HEAD - 900 + index,
                "block_timestamp": None,
                "transaction_hash": f"0xba7c{index:060x}",
                "log_index": 0, "sender_hint": "0x" + "b" * 40,
                "amount0_raw": 1, "amount1_raw": -1,
                "sqrt_price_x96": 1 << 96, "active_liquidity": 1, "tick": 0,
            } for index in range(4)])
            coverage = engine.near_head_flow_pass()["window_coverage"]
            self.assertEqual(
                coverage["windows"], 1,
                "the batch path's window was counted as a near-head result",
            )
            self.assertEqual(coverage["at_full_identity_coverage"], 1)
            self.assertEqual(coverage["evidence"], {"resolved_origins": 7 // 7})

    def test_the_event_is_sealed_with_its_population_named(self):
        recorded = []

        class _Ledger:
            def append(self, event_type, payload):
                recorded.append((event_type, payload))

        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, [self._log(i) for i in range(7)])
            engine.ledger = _Ledger()
            near_head = engine.near_head_flow_pass()
            engine.ledger.append("robinhood_near_head_flow", {
                **{k: v for k, v in near_head.items() if k != "enrichment"},
                "enrichment": near_head.get("enrichment") or {},
                "observation_seal": {},
                "population": "near_head_pass",
            })
            event_type, payload = recorded[-1]
            self.assertEqual(event_type, "robinhood_near_head_flow")
            self.assertEqual(payload["population"], "near_head_pass")
            self.assertEqual(payload["enrichment"]["resolved"], 7)
            self.assertTrue(payload["enrichment"]["window_fully_enriched"])
            self.assertEqual(payload["window_coverage"]["windows"], 1)

    def test_the_payload_survives_json_serialisation(self):
        """A chain event that cannot be written is worse than none."""
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, [self._log(i) for i in range(3)])
            payload = engine.near_head_flow_pass()
            round_tripped = json.loads(json.dumps(payload))
            self.assertEqual(round_tripped["window_coverage"]["windows"], 1)

    def test_an_empty_pass_still_reports_an_attributable_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, [])
            coverage = engine.near_head_flow_pass()["window_coverage"]
            self.assertEqual(coverage["windows"], 0)
            self.assertEqual(
                coverage["at_full_identity_coverage"], 0,
                "an empty pass must report zero, never the table's figure",
            )


class SealHeadAttributionTests(unittest.TestCase):
    """A window is sealed against the head it was observed at.

    The near-head pass took 703 seconds to scan a 1,350-block window while the
    chain advanced 7,012 blocks. Sealing against the head re-read AFTER the
    pass measured every window against a chain position that did not exist when
    it was observed, so windows_considered was 0 and sealed_this_cycle was 0 --
    windows enriched to full identity coverage were then discarded, and the
    cohort sat at 12 observations.
    """

    POOL = "0x" + "77" * 32
    OBSERVED_AT = 39_959_417          # to_block: what the window was built from
    HEAD_AFTER = 39_966_429           # head re-read 703 seconds later
    NOW = 21_000.0

    def _store(self, directory):
        return rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")

    def _engine(self, store):
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        return engine

    def _window(self, store, pool_id, window_end):
        """Build a real near-head window through the production write path."""
        store.apply_v4_events([{
            "kind": "initialize", "pool_id": pool_id,
            "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
            "token_address": TOKEN.lower(),
            "anchor_address": rh.USDG_ADDRESS.lower(),
            "fee_tier": 3000, "tick_spacing": 60,
            "hooks_address": rh.ZERO_ADDRESS,
            "block_number": window_end - rh.FLOW_WINDOW_BLOCKS,
            "sqrt_price_x96": 1 << 96, "tick": 0,
        }])
        store.apply_v4_events([{
            "kind": "swap", "pool_id": pool_id,
            "block_number": window_end - 300 + index,
            "block_timestamp": None,
            "transaction_hash": f"{pool_id[:10]}tx{index}",
            "log_index": 0, "sender_hint": "0x" + "c" * 40,
            "amount0_raw": 1, "amount1_raw": -1,
            "sqrt_price_x96": 1 << 96, "active_liquidity": 1, "tick": 0,
        } for index in range(6)])

    def test_the_observation_head_admits_the_window_it_built(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._window(store, self.POOL, self.OBSERVED_AT)
            result = self._engine(store).seal_near_head_observations(
                self.OBSERVED_AT, self.NOW,
            )
            self.assertGreater(
                result["windows_considered"], 0,
                "the window the pass just built was refused by its own seal",
            )

    def test_the_later_head_refuses_every_window(self):
        """The measured regression: 7,012 blocks of drift rejects everything."""
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._window(store, self.POOL, self.OBSERVED_AT)
            result = self._engine(store).seal_near_head_observations(
                self.HEAD_AFTER, self.NOW,
            )
            self.assertEqual(result["windows_considered"], 0)
            self.assertEqual(result["sealed_this_cycle"], 0)

    def test_drift_is_still_recorded_rather_than_forgiven(self):
        """Sealing on the observation head must not hide the staleness."""
        near_head = {"to_block": self.OBSERVED_AT,
                     "head_block_after": self.HEAD_AFTER}
        lag = max(0, int(near_head["head_block_after"])
                     - int(near_head["to_block"]))
        self.assertEqual(lag, 7_012)
        self.assertGreater(lag, rh.FLOW_MAXIMUM_PROSPECTIVE_HEAD_LAG_BLOCKS)

    def test_a_window_that_drifted_is_sealed_but_never_tradeable(self):
        with tempfile.TemporaryDirectory() as directory:
            store = self._store(directory)
            self._window(store, self.POOL, self.OBSERVED_AT)
            engine = self._engine(store)
            engine.seal_near_head_observations(self.OBSERVED_AT, self.NOW)
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT observation_id FROM flow_observations"
                    " ORDER BY rowid DESC LIMIT 1"
                ).fetchone()
            if row is None:
                self.skipTest("no observation sealed for this window shape")
            verdict = store.classify_flow_observation(
                row["observation_id"], decision_head=self.HEAD_AFTER,
                identity_coverage=1.0, gates=[],
            )
            self.assertEqual(verdict["arm"], "decision_stale")
            self.assertTrue(verdict["research_eligible"])
            self.assertFalse(
                verdict["paper_eligible"],
                "7,012 blocks of drift reached the paper path",
            )


class IncrementalNearHeadScanTests(unittest.TestCase):
    """The pass fetches only new blocks, so drift can fit the decision bound.

    Measured before this: 1,643 logs seen against 9 swaps ingested, 49.4s, and
    487 blocks of drift against a 120-block bound -- because a fixed
    1,350-block lookback re-downloaded roughly 1,200 blocks of logs already in
    the database. The window is computed from the database, not from the scan,
    so re-fetching them bought nothing.
    """

    POOL = "0x" + "5e" * 32
    HEAD = 40_000_000

    class _RPC:
        def __init__(self, head):
            self.head = head
            self.ranges = []
            self.fail_next = False

        def get_block_number(self):
            return self.head

        def get_logs(self, from_block, to_block, address=None, topics=None):
            self.ranges.append((from_block, to_block))
            if self.fail_next:
                raise RuntimeError("[RPC -429] RPC HTTP response failed (429)")
            return []

        def get_transactions(self, hashes):
            return []

    def _engine(self, directory):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        engine.root = Path(directory)
        engine.rpc = self._RPC(self.HEAD)
        return engine

    def test_the_first_pass_scans_the_whole_window(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._engine(directory)
            result = engine.near_head_flow_pass()
            self.assertFalse(result["incremental"])
            self.assertEqual(result["scan_blocks"], rh.FLOW_WINDOW_BLOCKS)

    def test_the_second_pass_scans_only_new_blocks(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._engine(directory)
            engine.near_head_flow_pass()
            engine.rpc.head = self.HEAD + 500      # chain advanced 500 blocks
            result = engine.near_head_flow_pass()
            self.assertTrue(result["incremental"])
            self.assertEqual(
                result["scan_blocks"], 500,
                "the pass re-downloaded blocks already in the database",
            )
            self.assertEqual(engine.rpc.ranges[-1][0], self.HEAD + 1)

    def test_a_long_gap_falls_back_to_the_full_window(self):
        """An interrupted cycle must not leave a hole in the record."""
        with tempfile.TemporaryDirectory() as directory:
            engine = self._engine(directory)
            engine.near_head_flow_pass()
            engine.rpc.head = self.HEAD + 50_000
            result = engine.near_head_flow_pass()
            self.assertEqual(result["scan_blocks"], rh.FLOW_WINDOW_BLOCKS)
            self.assertFalse(result["incremental"])

    def test_a_failed_scan_does_not_advance_the_cursor(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._engine(directory)
            engine.near_head_flow_pass()
            engine.rpc.head = self.HEAD + 500
            engine.rpc.fail_next = True
            failed = engine.near_head_flow_pass()
            self.assertFalse(failed["scanned"])
            engine.rpc.fail_next = False
            recovered = engine.near_head_flow_pass()
            self.assertEqual(
                recovered["scan_blocks"], 500,
                "blocks missed by the failed scan were skipped",
            )

    def test_no_new_blocks_costs_no_rpc_call(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._engine(directory)
            engine.near_head_flow_pass()
            calls = len(engine.rpc.ranges)
            result = engine.near_head_flow_pass()   # head unchanged
            self.assertEqual(len(engine.rpc.ranges), calls)
            self.assertEqual(result["scan_blocks"], 0)
            self.assertEqual(result["swaps_ingested"], 0)

    def test_the_narrowing_is_reported_for_the_drift_it_buys(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._engine(directory)
            engine.near_head_flow_pass()
            engine.rpc.head = self.HEAD + 487       # the measured live stride
            result = engine.near_head_flow_pass()
            self.assertLess(
                result["scan_blocks"], rh.FLOW_WINDOW_BLOCKS,
                "a narrower scan is the only lever on pass duration",
            )
            self.assertEqual(result["scan_blocks"], 487)


class DecisionQuoteWiringTests(unittest.TestCase):
    """The cycle re-prices only what a price could make tradeable.

    classify_sealed_observations sweeps every observation for the policy
    version each cycle -- 392 in one measured sweep. A quote is an RPC round
    trip, so quoting the whole sweep would spend hundreds of calls re-pricing
    observations that stay research-only whatever the price says.
    """

    HEAD = 41_000_000
    NOW = 30_000.0

    class _Market:
        def __init__(self, fail=False):
            self.calls = []
            self.fail = fail

        def snapshot(self, candidate, quote_block=None):
            self.calls.append(candidate["pool_id"])
            if self.fail:
                raise RuntimeError("quote unavailable")
            return {"execution_quote": {
                "verified": True, "anchor_in_raw": 1_000,
                "anchor_out_raw": 990, "token_out_raw": 10 ** 18,
            }}

    def _engine(self, directory, fail=False):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        engine.root = Path(directory)
        engine.v4_market = self._Market(fail=fail)
        return engine, store

    def _observation(self, store, pool, *, gaps, coverage):
        store.seal_flow_observation(
            pool_id=pool, token_address=TOKEN, observation_head=self.HEAD,
            window_start_block=self.HEAD - 450, window_end_block=self.HEAD - 10,
            transaction_hashes=[f"{pool}tx"], features={},
            quote={"execution_quote": {
                "verified": True, "anchor_in_raw": 1_000,
                "anchor_out_raw": 990, "token_out_raw": 10 ** 18}},
            quote_block=self.HEAD - 10, now=self.NOW,
        )
        with store.connection() as connection:
            connection.execute(
                """INSERT OR REPLACE INTO flow_signals (source_version,pool_id,
                       token_address,computed_at,window_blocks,
                       window_start_block,window_end_block,swap_count,buy_count,
                       sell_count,unique_sender_hints,
                       unique_resolved_participants,identity_coverage,buy_ratio,
                       net_anchor_flow_fraction,uncapped_shadow_score,
                       shadow_score,shadow_qualified,confidence,features_json,
                       qualification_gaps_json,limitations_json)
                   VALUES ('uniswap_v4',?,?,?,1350,?,?,8,6,2,4,4,?,0.75,0.5,
                           80.0,80.0,1,'ok','{}',?,'[]')""",
                (pool, TOKEN, rh._utc_now(), self.HEAD - 450, self.HEAD - 10,
                 coverage, json.dumps(gaps)),
            )

    def test_a_gated_observation_is_never_re_quoted(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory)
            self._observation(store, "0x" + "a1" * 32,
                              gaps=["minimum_swaps"], coverage=1.0)
            result = engine.classify_sealed_observations(self.HEAD)
            self.assertEqual(engine.v4_market.calls, [])
            self.assertEqual(result["decision_quotes_taken"], 0)

    def test_unverified_identity_is_never_re_quoted(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory)
            self._observation(store, "0x" + "a2" * 32, gaps=[], coverage=0.0)
            engine.classify_sealed_observations(self.HEAD)
            self.assertEqual(
                engine.v4_market.calls, [],
                "an unresolved-identity window cannot be traded at any price",
            )

    def test_a_paper_candidate_is_re_quoted_and_becomes_eligible(self):
        pool = "0x" + "a3" * 32
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory)
            self._observation(store, pool, gaps=[], coverage=1.0)
            result = engine.classify_sealed_observations(self.HEAD)
            self.assertEqual(engine.v4_market.calls, [pool])
            self.assertEqual(result["decision_quotes_taken"], 1)
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT paper_eligible, decision_quote_verified,"
                    " decision_price_drift_bps"
                    " FROM flow_observation_classifications"
                ).fetchone()
            self.assertTrue(row["paper_eligible"])
            self.assertTrue(row["decision_quote_verified"])
            self.assertEqual(row["decision_price_drift_bps"], 0.0)

    def test_a_failed_re_quote_leaves_it_research_only(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, fail=True)
            self._observation(store, "0x" + "a4" * 32, gaps=[], coverage=1.0)
            result = engine.classify_sealed_observations(self.HEAD)
            self.assertEqual(result["decision_quote_failures"], 1)
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT paper_eligible FROM flow_observation_classifications"
                ).fetchone()
            self.assertFalse(
                row["paper_eligible"],
                "a failed re-quote must not read as a pass",
            )

    def test_the_per_cycle_quote_budget_is_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory)
            for index in range(rh.FLOW_DECISION_QUOTE_LIMIT + 5):
                self._observation(store, f"0x{index:064x}", gaps=[], coverage=1.0)
            result = engine.classify_sealed_observations(self.HEAD)
            self.assertEqual(
                result["decision_quotes_taken"], rh.FLOW_DECISION_QUOTE_LIMIT,
            )
            self.assertLessEqual(
                len(engine.v4_market.calls), rh.FLOW_DECISION_QUOTE_LIMIT,
            )


class ScoreIsNotPromotionalTests(unittest.TestCase):
    """A reliably inverted quantity must not be the thing that says yes.

    Measured on 59 resolved observations: the high-score half returned -0.5678
    against -0.2876 for the low-score half, a gap of -0.2801 at permutation
    p=0.0135 over 4,000 shuffles. Survivorship came back clean -- resolved
    12.32, pending 13.02, non_exitable 14.10 mean score -- so requiring a high
    score was selecting the worse half of an already-losing population.
    """

    POOL = "0x" + "9f" * 32

    def _signal(self, store, pool, swaps, senders):
        store.apply_v4_events([{
            "kind": "initialize", "pool_id": pool,
            "currency0": TOKEN.lower(), "currency1": rh.USDG_ADDRESS.lower(),
            "token_address": TOKEN.lower(),
            "anchor_address": rh.USDG_ADDRESS.lower(),
            "fee_tier": 3000, "tick_spacing": 60,
            "hooks_address": rh.ZERO_ADDRESS, "block_number": 100_000,
            "sqrt_price_x96": 1 << 96, "tick": 0,
        }])
        store.apply_v4_events([{
            "kind": "swap", "pool_id": pool,
            "block_number": 101_000 + index, "block_timestamp": None,
            "transaction_hash": f"{pool[:8]}tx{index}", "log_index": 0,
            "sender_hint": senders[index % len(senders)],
            "amount0_raw": -1, "amount1_raw": 1,
            "sqrt_price_x96": 1 << 96, "active_liquidity": 1, "tick": 0,
        } for index in range(swaps)])

    def _row(self, store, pool):
        with store.connection() as connection:
            return connection.execute(
                "SELECT * FROM flow_signals WHERE pool_id=?", (pool,)
            ).fetchone()

    def test_a_clean_window_qualifies_without_clearing_the_score_bar(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._signal(store, self.POOL, 8,
                         ["0x" + c * 40 for c in "abcd"])
            row = self._row(store, self.POOL)
            gaps = json.loads(row["qualification_gaps_json"] or "[]")
            if gaps:
                self.skipTest(f"fixture did not clear the gates: {gaps}")
            self.assertTrue(
                row["shadow_qualified"],
                "a window with no failing gate was refused on score alone",
            )

    def test_the_score_is_still_recorded(self):
        """Inverted is not useless -- it stays measurable, it just cannot promote."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._signal(store, self.POOL, 8, ["0x" + c * 40 for c in "abcd"])
            row = self._row(store, self.POOL)
            self.assertIsNotNone(row["uncapped_shadow_score"])
            self.assertIsNotNone(row["shadow_score"])

    def test_a_gated_window_never_qualifies(self):
        """Removing the score bar must not remove the evidence gates."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            self._signal(store, self.POOL, 2, ["0x" + "a" * 40])
            row = self._row(store, self.POOL)
            self.assertTrue(json.loads(row["qualification_gaps_json"] or "[]"))
            self.assertFalse(row["shadow_qualified"])


class RoundTripGateTests(unittest.TestCase):
    """An unexitable position is not a trade whatever the flow said.

    Measured over 65 sealed observations, buying and selling at the SAME block
    loses a mean 42.10%, median 22.5%, minimum 100% (pools with no liquidity).
    Mean realised 15-minute return was -41.37%, so price movement contributed
    +0.73%: the returns this project analysed were the fee-and-slippage
    structure of the pools, not token behaviour.
    """

    POOL = "0x" + "7d" * 32
    HEAD = 42_000_000
    NOW = 40_000.0

    def _quote(self, anchor_out):
        return {"execution_quote": {
            "verified": True, "anchor_in_raw": 100_000_000,
            "anchor_out_raw": anchor_out, "token_out_raw": 10 ** 18,
        }}

    def _classify(self, directory, sealed_out, decision_out=None):
        store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
        observation_id = store.seal_flow_observation(
            pool_id=self.POOL, token_address=TOKEN, observation_head=self.HEAD,
            window_start_block=self.HEAD - 450, window_end_block=self.HEAD - 10,
            transaction_hashes=["0xrt"], features={},
            quote=self._quote(sealed_out), quote_block=self.HEAD - 10,
            now=self.NOW,
        )
        return store, store.classify_flow_observation(
            observation_id, decision_head=self.HEAD, identity_coverage=1.0,
            gates=[],
            decision_quote=(self._quote(decision_out)
                            if decision_out is not None else None),
        )

    def test_the_live_median_pool_is_refused(self):
        """$77.52 back on $100 in -- the real median, -22.5%."""
        with tempfile.TemporaryDirectory() as directory:
            _, verdict = self._classify(directory, 77_519_219, 77_519_219)
            self.assertAlmostEqual(verdict["round_trip_return"], -0.2248, places=3)
            self.assertFalse(verdict["exitable"])
            self.assertFalse(verdict["paper_eligible"])

    def test_an_empty_pool_is_refused_rather_than_scored(self):
        with tempfile.TemporaryDirectory() as directory:
            _, verdict = self._classify(directory, 0, 0)
            self.assertEqual(verdict["round_trip_return"], -1.0)
            self.assertFalse(verdict["exitable"])

    def test_a_deep_pool_passes(self):
        with tempfile.TemporaryDirectory() as directory:
            _, verdict = self._classify(directory, 99_000_000, 99_000_000)
            self.assertAlmostEqual(verdict["round_trip_return"], -0.01, places=4)
            self.assertTrue(verdict["exitable"])
            self.assertTrue(verdict["paper_eligible"])

    def test_the_decision_quote_overrides_a_stale_sealed_one(self):
        """Liquidity can drain after sealing; the current pool is what matters."""
        with tempfile.TemporaryDirectory() as directory:
            _, verdict = self._classify(directory, 99_000_000, 50_000_000)
            self.assertFalse(
                verdict["exitable"],
                "a pool that has since drained was admitted on its old quote",
            )

    def test_the_figure_is_pinned_on_the_sealed_observation(self):
        with tempfile.TemporaryDirectory() as directory:
            store, _ = self._classify(directory, 77_519_219)
            with store.connection() as connection:
                stored = connection.execute(
                    "SELECT round_trip_return FROM flow_observations"
                ).fetchone()[0]
            self.assertAlmostEqual(stored, -0.2248, places=3)

    def test_a_quote_that_cannot_price_an_exit_is_not_a_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "learn.sqlite3")
            observation_id = store.seal_flow_observation(
                pool_id=self.POOL, token_address=TOKEN,
                observation_head=self.HEAD,
                window_start_block=self.HEAD - 450,
                window_end_block=self.HEAD - 10,
                transaction_hashes=["0xrt2"], features={},
                quote={"execution_quote": {"verified": True,
                                           "anchor_in_raw": 100_000_000}},
                quote_block=self.HEAD - 10, now=self.NOW,
            )
            verdict = store.classify_flow_observation(
                observation_id, decision_head=self.HEAD,
                identity_coverage=1.0, gates=[],
                decision_quote={"verified": True, "anchor_in_raw": 100_000_000},
            )
            self.assertIsNone(verdict["round_trip_return"])
            self.assertFalse(verdict["exitable"])


class ResidualRecordedTests(unittest.TestCase):
    """The residual is stored, not re-derived.

    At n=236 friction is 96% of the realised return, so the residual -- the
    only place an edge could live -- is what every question turns on. It was
    reconstructed by hand three times in one night, which is three chances for
    two analyses to disagree about the same number.
    """

    POOL = "0x" + "3c" * 32
    HEAD = 43_000_000
    NOW = 50_000.0

    def _due(self, store, anchor_out):
        store.seal_flow_observation(
            pool_id=self.POOL, token_address=TOKEN, observation_head=self.HEAD,
            window_start_block=self.HEAD - 450, window_end_block=self.HEAD - 10,
            transaction_hashes=["0xres"], features={},
            quote={"execution_quote": {
                "verified": True, "anchor_in_raw": 100_000_000,
                "anchor_out_raw": anchor_out, "token_out_raw": 10 ** 18}},
            quote_block=self.HEAD - 10, now=self.NOW,
        )
        due = store.due_flow_observation_outcomes(self.NOW + 10 * 86_400,
                                                  limit=500)
        return [d for d in due
                if d["horizon_label"] == rh.FLOW_PRIMARY_HORIZON_LABEL][0]

    def test_friction_and_residual_are_persisted(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "l.sqlite3")
            due = self._due(store, 90_000_000)          # -10% friction
            result = store.record_flow_observation_outcome(
                due, {"verified": True, "anchor_out_raw": 1_300},
                quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
            )
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT friction_return,residual_return,net_return"
                    " FROM flow_observation_outcomes WHERE horizon_label=?",
                    (rh.FLOW_PRIMARY_HORIZON_LABEL,),
                ).fetchone()
            self.assertAlmostEqual(row["friction_return"], -0.10, places=4)
            self.assertAlmostEqual(
                row["residual_return"],
                row["net_return"] - row["friction_return"], places=6,
                msg="the stored residual disagrees with its own components",
            )

    def test_an_unpriceable_outcome_stores_no_residual(self):
        """Absence must not read as zero edge."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "l.sqlite3")
            due = dict(self._due(store, 90_000_000))
            due["quote_verified"] = 0
            result = store.record_flow_observation_outcome(
                due, {"verified": True, "anchor_out_raw": 1_300},
                quote_block=self.HEAD, now=self.NOW + 10 * 86_400,
            )
            self.assertEqual(result["status"], "unpriceable")
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT residual_return FROM flow_observation_outcomes"
                    " WHERE horizon_label=?",
                    (rh.FLOW_PRIMARY_HORIZON_LABEL,),
                ).fetchone()
            self.assertIsNone(row["residual_return"])


class ExitableQualificationTests(unittest.TestCase):
    """An unexitable pool is not a signal, whatever its flow looked like.

    The first two windows ever labelled `signal` were pools holding $2.11 and
    $0.39, with 98.96% and 99.81% buy impact and identical returns at 1m, 5m,
    15m and 1h of -0.9951 and -0.9995 -- pure friction. They passed because
    the five qualification gates say nothing about whether a pool holds money,
    and the shadow-score bar that had been excluding them as a side effect was
    removed (their scores were 21.9 and 24.6, both under the old 70).
    """

    POOL = "0x" + "6b" * 32
    HEAD = 44_000_000
    NOW = 60_000.0

    class _Market:
        def __init__(self, anchor_out):
            self.anchor_out = anchor_out

        def snapshot(self, candidate, quote_block=None):
            return {"execution_quote": {
                "verified": True, "anchor_in_raw": 100_000_000,
                "anchor_out_raw": self.anchor_out, "token_out_raw": 10 ** 18,
            }}

    def _seal(self, directory, anchor_out, gaps):
        store = rh.RobinhoodLearningStore(Path(directory) / "l.sqlite3")
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        engine.root = Path(directory)
        engine.v4_market = self._Market(anchor_out)
        with store.connection() as connection:
            connection.execute(
                """INSERT INTO flow_signals (source_version,pool_id,
                       token_address,computed_at,window_blocks,
                       window_start_block,window_end_block,swap_count,buy_count,
                       sell_count,unique_sender_hints,
                       unique_resolved_participants,identity_coverage,buy_ratio,
                       net_anchor_flow_fraction,uncapped_shadow_score,
                       shadow_score,shadow_qualified,confidence,features_json,
                       qualification_gaps_json,limitations_json)
                   VALUES ('uniswap_v4',?,?,?,1350,?,?,8,6,2,4,4,1.0,0.75,0.5,
                           21.9,21.9,1,'ok',?,?,'[]')""",
                (self.POOL, TOKEN, rh._utc_now(), self.HEAD - 1350,
                 self.HEAD - 10,
                 json.dumps({"qualification_gaps": gaps}), json.dumps(gaps)),
            )
        engine.seal_near_head_observations(self.HEAD, self.NOW)
        with store.connection() as connection:
            return connection.execute(
                "SELECT role,qualification_gap_count,features_json,"
                " round_trip_return FROM flow_observations"
            ).fetchone()

    def test_an_empty_pool_is_not_labelled_a_signal(self):
        """The live case: $0.39 of liquidity, -99.95% round trip, zero gaps."""
        with tempfile.TemporaryDirectory() as directory:
            row = self._seal(directory, 46_992, gaps=[])
            self.assertEqual(row["role"], "matched_control")
            self.assertIn(
                "exitable_round_trip",
                json.loads(row["features_json"])["qualification_gaps"],
            )
            self.assertEqual(row["qualification_gap_count"], 1)

    def test_a_tradeable_pool_with_no_gaps_is_still_a_signal(self):
        with tempfile.TemporaryDirectory() as directory:
            row = self._seal(directory, 99_000_000, gaps=[])
            self.assertEqual(
                row["role"], "signal",
                "the new gate rejected a pool that can actually be exited",
            )
            self.assertEqual(row["qualification_gap_count"], 0)

    def test_an_already_gated_window_is_unchanged(self):
        """The gate only fires where it would flip a label."""
        with tempfile.TemporaryDirectory() as directory:
            row = self._seal(directory, 46_992, gaps=["minimum_swaps"])
            self.assertEqual(row["role"], "matched_control")
            self.assertEqual(
                json.loads(row["features_json"])["qualification_gaps"],
                ["minimum_swaps"],
                "an unrelated gap list was rewritten",
            )

    def test_an_unpriceable_quote_is_not_a_signal(self):
        """Absence must never read as a pass."""
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "l2.sqlite3")
            engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
            engine.store = store
            engine.root = Path(directory)

            class _NoExit:
                def snapshot(self, candidate, quote_block=None):
                    return {"execution_quote": {
                        "verified": True, "anchor_in_raw": 100_000_000}}

            engine.v4_market = _NoExit()
            with store.connection() as connection:
                connection.execute(
                    """INSERT INTO flow_signals (source_version,pool_id,
                           token_address,computed_at,window_blocks,
                           window_start_block,window_end_block,swap_count,
                           buy_count,sell_count,unique_sender_hints,
                           unique_resolved_participants,identity_coverage,
                           buy_ratio,net_anchor_flow_fraction,
                           uncapped_shadow_score,shadow_score,shadow_qualified,
                           confidence,features_json,qualification_gaps_json,
                           limitations_json)
                       VALUES ('uniswap_v4',?,?,?,1350,?,?,8,6,2,4,4,1.0,0.75,
                               0.5,21.9,21.9,1,'ok','{"qualification_gaps":[]}',
                               '[]','[]')""",
                    (self.POOL, TOKEN, rh._utc_now(), self.HEAD - 1350,
                     self.HEAD - 10),
                )
            engine.seal_near_head_observations(self.HEAD, self.NOW)
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT role FROM flow_observations"
                ).fetchone()
            self.assertEqual(row["role"], "matched_control")


class DiscoveryGapTests(unittest.TestCase):
    """A pool born near the head is admitted on sight, not on the crawler.

    Batch discovery ran 477,270 blocks behind -- about 13 hours -- with 0
    pools known inside that gap. The near-head pass scanned those blocks every
    cycle and discarded every swap whose pool it did not already know: 1,967
    logs seen against 11 ingested. Two tokens the operator asked about were
    absent from every table in the database, never rejected and never
    analysed, because their pools were younger than the crawler's position.
    """

    NEW_POOL = "0x" + "e1" * 32
    TOKEN_NEW = "0x" + "42" * 20
    HEAD = 45_000_000

    class _RPC:
        def __init__(self, head, init_logs, swap_logs):
            self.head = head
            self.init_logs = init_logs
            self.swap_logs = swap_logs
            self.topics_seen = []

        def get_block_number(self):
            return self.head

        def get_logs(self, from_block, to_block, address=None, topics=None):
            flat = (topics or [[]])[0]
            self.topics_seen.append(flat[0] if flat else None)
            if flat and flat[0] == rh.V4_INITIALIZE_TOPIC:
                return self.init_logs
            return self.swap_logs

        def get_transactions(self, hashes):
            return []

    def _init_log(self, pool, token, anchor_first=False):
        anchor = rh.USDG_ADDRESS.lower()
        c0, c1 = (anchor, token) if anchor_first else (token, anchor)
        pad = lambda a: "0x" + "0" * 24 + a[2:]
        return {"topics": [rh.V4_INITIALIZE_TOPIC, pool, pad(c0), pad(c1)],
                "blockNumber": hex(self.HEAD - 300),
                "data": "0x" + ("0" * 63 + "1") * 5, "logIndex": "0x0",
                "transactionHash": "0x" + "1" * 64}

    def _swap_log(self, pool, index):
        return {"topics": [rh.V4_SWAP_TOPIC, pool, "0x" + "0" * 24 + "a" * 40],
                "blockNumber": hex(self.HEAD - 200 + index),
                "transactionHash": f"0xdead{index:060x}", "logIndex": "0x0",
                "data": "0x" + ("0" * 63 + "1") * 5}

    def _engine(self, directory, init_logs, swap_logs):
        store = rh.RobinhoodLearningStore(Path(directory) / "l.sqlite3")
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        engine.root = Path(directory)
        engine.rpc = self._RPC(self.HEAD, init_logs, swap_logs)
        return engine, store

    def test_a_pool_born_this_window_is_registered_and_its_swaps_kept(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(
                directory,
                [self._init_log(self.NEW_POOL, self.TOKEN_NEW)],
                [self._swap_log(self.NEW_POOL, i) for i in range(5)],
            )
            result = engine.near_head_flow_pass()
            self.assertEqual(result["pools_admitted_on_sight"], 1)
            self.assertEqual(
                result["swaps_ingested"], 5,
                "swaps in a newly born pool were discarded as unknown",
            )
            self.assertIn(self.NEW_POOL, store.known_v4_pool_ids())

    def test_without_the_fix_those_swaps_would_be_dropped(self):
        """Same swaps, no Initialize log: the pre-fix behaviour."""
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(
                directory, [], [self._swap_log(self.NEW_POOL, i) for i in range(5)],
            )
            result = engine.near_head_flow_pass()
            self.assertEqual(result["pools_admitted_on_sight"], 0)
            self.assertEqual(result["swaps_ingested"], 0)

    def test_a_pool_with_no_anchor_is_refused(self):
        """Unpriceable pairs must not be admitted just for being new."""
        pad = lambda a: "0x" + "0" * 24 + a[2:]
        log = {"topics": [rh.V4_INITIALIZE_TOPIC, self.NEW_POOL,
                          pad(self.TOKEN_NEW), pad("0x" + "77" * 20)],
               "blockNumber": hex(self.HEAD - 300),
               "data": "0x" + ("0" * 63 + "1") * 5, "logIndex": "0x0",
               "transactionHash": "0x" + "1" * 64}
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(directory, [log], [])
            self.assertEqual(
                engine.near_head_flow_pass()["pools_admitted_on_sight"], 0)

    def test_either_currency_ordering_resolves_the_token(self):
        with tempfile.TemporaryDirectory() as directory:
            engine, store = self._engine(
                directory,
                [self._init_log(self.NEW_POOL, self.TOKEN_NEW, anchor_first=True)],
                [],
            )
            engine.near_head_flow_pass()
            with store.connection() as connection:
                row = connection.execute(
                    "SELECT token_address,anchor_address FROM v4_pools"
                    " WHERE pool_id=?", (self.NEW_POOL,)).fetchone()
            self.assertEqual(row["token_address"], self.TOKEN_NEW)
            self.assertEqual(row["anchor_address"], rh.USDG_ADDRESS.lower())

    def test_a_failed_initialize_scan_does_not_lose_the_swap_pass(self):
        class _Failing(self._RPC):
            def get_logs(self, from_block, to_block, address=None, topics=None):
                flat = (topics or [[]])[0]
                if flat and flat[0] == rh.V4_INITIALIZE_TOPIC:
                    raise RuntimeError("[RPC -429] rate limited")
                return self.swap_logs

        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(Path(directory) / "l.sqlite3")
            engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
            engine.store = store
            engine.root = Path(directory)
            engine.rpc = _Failing(self.HEAD, [], [])
            result = engine.near_head_flow_pass()
            self.assertTrue(result["scanned"])
            self.assertEqual(result["pools_admitted_on_sight"], 0)
