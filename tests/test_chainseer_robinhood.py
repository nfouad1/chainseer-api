import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import chainseer_robinhood as rh


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
        self.assertIn("Analyzed tokens", html)
        self.assertIn("Contract address", html)

    def test_scheduler_preserves_redirected_python_traceback(self):
        script = Path(rh.__file__).with_name(
            "run_chainseer_robinhood_learning.ps1"
        ).read_text()
        self.assertIn("RedirectStandardError", script)
        self.assertIn("Get-Content -LiteralPath $stderrPath -Raw", script)
        self.assertIn('Write-Status "failed" $process.ExitCode $detail', script)


if __name__ == "__main__":
    unittest.main()
