import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import chainseer_robinhood as rh
import chainseer_shadow_paths as sp
import run_chainseer_robinhood_learning as runner


TOKEN = "0x" + "11" * 20
POOL = "0x" + "ab" * 32
CONTROL_POOL = "0x" + "cd" * 32


class ShadowPathPolicyTests(unittest.TestCase):
    def test_native_currency_is_shadow_only_and_never_calls_erc20_metadata(self):
        client = rh.RobinhoodV4MarketClient.__new__(rh.RobinhoodV4MarketClient)
        client.store = MagicMock()
        client.store.v4_pool.return_value = {
            "currency0": rh.ZERO_ADDRESS, "currency1": rh.USDG_ADDRESS,
            "token_address": rh.ZERO_ADDRESS,"anchor_address": rh.USDG_ADDRESS,
            "fee_tier": 3000,"tick_spacing": 60,"hooks_address": rh.ZERO_ADDRESS}
        client.store.latest_v4_custody.return_value = {}
        candidate = {"pool_id": POOL,"token_address": rh.ZERO_ADDRESS,
                     "shadow_entry_anchor_address": rh.USDG_ADDRESS,
                     "shadow_entry_anchor_in_raw": sp.ENTRY_ANCHOR_IN_RAW["stable"]}
        rejected = client.snapshot(candidate,quote_block=123)
        self.assertEqual(rejected["reason"],"invalid_token_address")
        candidate["shadow_path_quote"] = True
        candidate["paper_quantity"] = 1
        candidate["shadow_exit_token_in_raw"] = str(10**18)
        with (patch.object(client,"_cached_call",return_value="0x1000"),
              patch.object(client,"_decode_slot0",return_value=(2**96,0)),
              patch.object(client,"_cached_decimals",return_value=6) as decimals,
              patch.object(client,"_cached_total_supply") as supply,
              patch.object(client,"_quote_exact_input_single",
                           side_effect=[(10**18,100),(99*10**6,100),(98*10**6,100)]) as quote):
            result = client.snapshot(candidate,quote_block=123)
        decimals.assert_called_once_with(rh.USDG_ADDRESS,123)
        supply.assert_not_called()
        self.assertTrue(result["execution_quote"]["verified"])
        self.assertEqual(result["execution_quote"]["token_decimals"],18)
        self.assertEqual(result["paper_exit_quote"]["token_in_raw"],str(10**18))
        self.assertIsNone(result["market_cap_usd"])
        self.assertTrue(all(c.kwargs["block"] == 123 for c in quote.call_args_list))

    def test_shadow_currency_mismatch_is_rejected_before_rpc(self):
        client = rh.RobinhoodV4MarketClient.__new__(rh.RobinhoodV4MarketClient)
        client.store = MagicMock()
        client.store.v4_pool.return_value = {
            "currency0": TOKEN,"currency1": rh.USDG_ADDRESS,
            "token_address": TOKEN,"anchor_address": rh.USDG_ADDRESS}
        with patch.object(client,"_cached_call") as rpc:
            result = client.snapshot({
                "pool_id": POOL,"token_address": rh.ZERO_ADDRESS,
                "shadow_path_quote": True,
                "shadow_entry_anchor_address": rh.USDG_ADDRESS},quote_block=123)
        rpc.assert_not_called()
        self.assertEqual(result["reason"],"shadow_anchor_binding_mismatch")

    def test_fixed_anchor_probe_does_not_depend_on_later_usd_price(self):
        client = rh.RobinhoodV4MarketClient.__new__(rh.RobinhoodV4MarketClient)
        pool = {"anchor_address": rh.WETH_ADDRESS,
                "currency0": rh.WETH_ADDRESS, "currency1": TOKEN,
                "fee_tier": 3000,"tick_spacing": 60,
                "hooks_address": rh.ZERO_ADDRESS}
        amount = int(sp.ENTRY_ANCHOR_IN_RAW["wrapped_native"])
        for usd_price in (1_000,5_000):
            with patch.object(client,"_quote_exact_input_single",
                              side_effect=[(10**18,100),(amount*99//100,100)]) as quote:
                result = client.execution_quote(
                    pool,token=TOKEN,token_decimals=18,anchor_decimals=18,
                    anchor_usd=usd_price,token_usd=1,block=123,
                    anchor_in_raw=amount)
            self.assertEqual(quote.call_args_list[0].kwargs["exact_amount"],amount)
            self.assertEqual(result["anchor_in_raw"],str(amount))
        self.assertEqual(int(sp.ENTRY_ANCHOR_IN_RAW["stable"]),100*10**6)

    def test_schedule_is_exact_block_relative_and_policy_hashed(self):
        rows = sp.schedule_rows("p", 10_000, 1_000.0)
        self.assertEqual(rows[0]["label"], "entry")
        self.assertEqual(rows[0]["target_block"], 10_000)
        fifteen = next(row for row in rows if row["label"] == "15m")
        self.assertEqual(fifteen["target_block"], 19_000)
        self.assertEqual(fifteen["offset_blocks"], 9_000)
        self.assertTrue(all(row["policy_hash"] == sp.policy_hash() for row in rows))

    def test_sampling_is_deterministic(self):
        event = "e" * 64
        self.assertEqual(sp.sample_bucket(event), sp.sample_bucket(event))
        self.assertEqual(sp.selected(event), sp.selected(event))


class ShadowPathStoreTests(unittest.TestCase):
    def test_verified_retry_resolves_integrity_failure_without_erasing_attempt(self):
        with tempfile.TemporaryDirectory() as directory:
            store,_ = self._captured(directory)
            due = store.due_flow_shadow_path_marks(1_001,20_000,limit=1)[0]
            store.record_flow_shadow_path_attempt(
                due["path_id"],0,now=1_001,error="metadata unsupported",
                failure_class="evidence_integrity_failure")
            self.assertFalse(sp.verify_measurement_ledger(store.path)["ok"])
            store.record_flow_shadow_path_attempt(
                due["path_id"],0,now=1_001.5,error=TimeoutError("provider timeout"))
            self.assertFalse(sp.verify_measurement_ledger(store.path)["ok"])
            quote = {"current_state_verified": True,"quote_block":due["target_block"],
                     "execution_quote": {"verified": True,"passes_round_trip_limit": True,
                         "quote_block":due["target_block"],"round_trip_ratio":0.99,
                         "anchor_in_raw":due["entry_anchor_in_raw"],"token_out_raw":"1000"}}
            store.record_flow_shadow_path_measurement(
                due,quote,quote_block=due["target_block"],now=1_002)
            verified = sp.verify_measurement_ledger(store.path)
            self.assertTrue(verified["ok"],verified)
            summary = store.flow_shadow_path_summary()
            self.assertEqual(summary["integrity_failures"],0)
            self.assertEqual(summary["historical_integrity_failures"],1)
            self.assertEqual(summary["resolved_integrity_failures"],1)
            with store.connection() as connection:
                self.assertEqual(connection.execute(
                    "SELECT last_error FROM flow_shadow_path_attempts").fetchone()[0],
                    "metadata unsupported")

    def test_unverified_measurement_cannot_discharge_integrity_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            store,_ = self._captured(directory)
            due = store.due_flow_shadow_path_marks(1_001,20_000,limit=1)[0]
            store.record_flow_shadow_path_attempt(
                due["path_id"],0,now=1_001,error="binding mismatch",
                failure_class="evidence_integrity_failure")
            store.record_flow_shadow_path_measurement(
                due,{"quote_block":due["target_block"],"execution_quote":{"verified":False}},
                quote_block=due["target_block"],now=1_002)
            self.assertFalse(sp.verify_measurement_ledger(store.path)["ok"])

    @staticmethod
    def _signal(store, pool, *, qualified, end_block, score):
        with store.connection() as connection:
            connection.execute(
                """INSERT OR IGNORE INTO v4_pools (
                    pool_id,currency0,currency1,token_address,anchor_address,
                    fee_tier,tick_spacing,hooks_address,initialized_block,updated_at
                ) VALUES (?,?,?,?,?,3000,60,?,1,?)""",
                (pool,TOKEN,rh.WETH_ADDRESS,TOKEN,rh.WETH_ADDRESS,
                 rh.ZERO_ADDRESS,rh._utc_now()),
            )
            connection.execute(
                """
                INSERT INTO flow_signals (
                    source_version,pool_id,token_address,computed_at,
                    window_blocks,window_start_block,window_end_block,
                    swap_count,buy_count,sell_count,unique_sender_hints,
                    unique_resolved_participants,identity_coverage,buy_ratio,
                    net_anchor_flow_fraction,price_multiple,uncapped_shadow_score,
                    shadow_score,shadow_qualified,confidence,
                    qualification_gaps_json,limitations_json,features_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    rh.SOURCE_V4,pool,TOKEN,rh._utc_now(),1_350,
                    end_block - 10,end_block,8,7,1,5,5,1.0,0.875,0.5,1.1,
                    score,score,int(qualified),"high","[]","[]","{}",
                ),
            )

    def _captured(self, directory):
        store = rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
        end_block = 9_990
        while not sp.selected(store._flow_event_id(
                rh.FLOW_EVIDENCE_POLICY_VERSION,POOL,end_block,
                "qualified","")):
            end_block += 1
        self._signal(store,POOL,qualified=True,end_block=end_block,score=80.0)
        self._signal(
            store,CONTROL_POOL,qualified=False,end_block=end_block-2,score=60.0)
        result = store.capture_flow_signal_events(end_block+10,now=1_000.0)
        return store,result

    def test_capture_freezes_signal_and_control_paths_atomically(self):
        with tempfile.TemporaryDirectory() as directory:
            store,result = self._captured(directory)
            self.assertEqual(result["shadow_paths_created"],2)
            self.assertEqual(
                result["shadow_path_marks_scheduled"],2 * len(sp.SCHEDULE))
            with store.connection() as connection:
                paths = [dict(row) for row in connection.execute(
                    "SELECT * FROM flow_shadow_paths ORDER BY signal_role")]
                schedules = connection.execute(
                    "SELECT COUNT(*) FROM flow_shadow_path_schedule").fetchone()[0]
            self.assertEqual(len(paths),2)
            signal = next(row for row in paths if row["signal_role"] == "qualified")
            control = next(row for row in paths if row["signal_role"] == "matched_control")
            self.assertEqual(signal["matched_path_id"],control["path_id"])
            self.assertEqual(control["matched_path_id"],signal["path_id"])
            self.assertEqual(signal["sampled_by_event_id"],control["sampled_by_event_id"])
            self.assertEqual(schedules,2 * len(sp.SCHEDULE))

    def test_entry_and_exit_measurements_form_a_verified_hash_chain(self):
        with tempfile.TemporaryDirectory() as directory:
            store,_result = self._captured(directory)
            due = store.due_flow_shadow_path_marks(1_001.0,10_100,limit=1)[0]
            entry_market = {
                "current_state_verified": True,
                "price_usd": 1.0,"liquidity_usd": 50_000,
                "quote_block": due["target_block"],
                "execution_quote": {
                    "verified": True,"passes_round_trip_limit": True,
                    "anchor_in_raw": due["entry_anchor_in_raw"],
                    "anchor_out_raw": str(int(due["entry_anchor_in_raw"])*98//100),
                    "round_trip_ratio": 0.98,"token_out_raw": "1000",
                    "token_decimals": 18,
                    "quote_block": due["target_block"],
                },
            }
            entry = store.record_flow_shadow_path_measurement(
                due,entry_market,quote_block=due["target_block"],now=1_001.0)
            self.assertEqual(entry["status"],"observed")

            exit_due = next(
                row for row in store.due_flow_shadow_path_marks(
                    2_000.0,20_000,limit=10)
                if row["path_id"] == due["path_id"]
                and int(row["step_index"]) == 1)
            exit_market = {
                "current_state_verified": True,
                "price_usd": 1.5,"liquidity_usd": 45_000,
                "quote_block": exit_due["target_block"],
                "paper_exit_quote": {
                    "verified": True,"token_in_raw": "1000",
                    "anchor_out_raw": str(int(due["entry_anchor_in_raw"])*3//2)},
            }
            exit_result = store.record_flow_shadow_path_measurement(
                exit_due,exit_market,quote_block=exit_due["target_block"],
                now=2_000.0)
            self.assertEqual(exit_result["status"],"observed")
            self.assertGreater(exit_result["net_return"],0)
            verified = sp.verify_measurement_ledger(
                Path(directory) / "learning.sqlite3")
            self.assertTrue(verified["ok"],verified)
            self.assertEqual(verified["records"],2)

    def test_unmarketable_entry_terminally_accounts_for_whole_path(self):
        with tempfile.TemporaryDirectory() as directory:
            store,_result = self._captured(directory)
            due = store.due_flow_shadow_path_marks(1_001.0,10_100,limit=1)[0]
            result = store.record_flow_shadow_path_measurement(
                due,
                {"current_state_verified": True,
                 "quote_block": due["target_block"],"execution_quote": {
                    "verified": False,"reason": "no_route"}},
                quote_block=due["target_block"],now=1_001.0)
            self.assertEqual(result["status"],"entry_unmarketable")
            self.assertEqual(result["cascaded"],len(sp.SCHEDULE)-1)
            with store.connection() as connection:
                count = connection.execute(
                    "SELECT COUNT(*) FROM flow_shadow_path_measurements"
                    " WHERE path_id=?",(due["path_id"],)).fetchone()[0]
            self.assertEqual(count,len(sp.SCHEDULE))
            self.assertTrue(sp.verify_measurement_ledger(
                Path(directory) / "learning.sqlite3")["ok"])

    def test_schedule_and_measurements_are_sql_append_only(self):
        with tempfile.TemporaryDirectory() as directory:
            store,_result = self._captured(directory)
            with self.assertRaises(Exception):
                with store.connection() as connection:
                    connection.execute(
                        "UPDATE flow_shadow_path_schedule SET target_block=1")

    def test_existing_event_is_never_retroactively_enrolled(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(
                Path(directory) / "learning.sqlite3")
            end_block = 9_990
            while not sp.selected(store._flow_event_id(
                    rh.FLOW_EVIDENCE_POLICY_VERSION,POOL,end_block,
                    "qualified","")):
                end_block += 1
            self._signal(
                store,POOL,qualified=True,end_block=end_block,score=80.0)
            with patch.object(rh,"_flow_shadow_selected",return_value=False):
                first = store.capture_flow_signal_events(
                    end_block+10,now=1_000.0)
            second = store.capture_flow_signal_events(
                end_block+10,now=1_001.0)
            self.assertEqual(first["created"],1)
            self.assertEqual(first["shadow_paths_created"],0)
            self.assertEqual(second["created"],0)
            self.assertEqual(second["shadow_paths_created"],0)

    def test_observation_fresh_decision_stale_signal_gets_a_control(self):
        with tempfile.TemporaryDirectory() as directory:
            store = rh.RobinhoodLearningStore(
                Path(directory) / "learning.sqlite3")
            end_block = 9_990
            while not sp.selected(store._flow_event_id(
                    rh.FLOW_EVIDENCE_POLICY_VERSION,POOL,end_block,
                    "qualified","")):
                end_block += 1
            self._signal(
                store,POOL,qualified=True,end_block=end_block,score=80.0)
            self._signal(
                store,CONTROL_POOL,qualified=False,end_block=end_block-2,
                score=60.0)
            result = store.capture_flow_signal_events(
                end_block+1_000,now=1_000.0,
                ingest_head_block=end_block+10)
            self.assertEqual(result["qualified_created"],1)
            self.assertEqual(result["controls_created"],1)
            self.assertEqual(result["shadow_paths_created"],2)
            with store.connection() as connection:
                arms = {row[0] for row in connection.execute(
                    "SELECT arm FROM flow_shadow_paths")}
            self.assertEqual(arms,{"decision_stale"})


class ShadowPathEvidenceLaneTests(unittest.TestCase):
    def _backlogged_engine(self, directory):
        store,_ = ShadowPathStoreTests()._captured(directory)
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store = store
        engine.v4_market = MagicMock()

        def quote(candidate, *, include_execution_quote=True, quote_block=None):
            amount = candidate["shadow_entry_anchor_in_raw"]
            return {
                "current_state_verified": True,"quote_block":quote_block,
                "execution_quote": {
                    "verified":True,"passes_round_trip_limit":True,
                    "quote_block":quote_block,"round_trip_ratio":0.99,
                    "anchor_in_raw":amount,"token_out_raw":str(10**18),
                    "token_decimals":18},
                "paper_exit_quote": {
                    "verified":True,"token_in_raw":str(10**18),
                    "anchor_out_raw":str(int(amount)*99//100)},
            }
        engine.v4_market.snapshot.side_effect = quote
        engine.observe_flow_shadow_path_marks(1_001,10_000_000,limit=2)
        engine.v4_market.snapshot.reset_mock()
        return engine

    def test_backlog_drains_beyond_four_in_fifo_order_with_hard_row_cap(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._backlogged_engine(directory)
            result = engine.observe_flow_shadow_path_marks(2_000,10_000_000,limit=1000)
            self.assertEqual(result["observed"],32)
            self.assertEqual(result["limit"],32)
            self.assertTrue(result["batch_cap_reached"])
            self.assertTrue(result["more_due_available"])
            blocks=[c.kwargs["quote_block"]
                    for c in engine.v4_market.snapshot.call_args_list]
            self.assertEqual(blocks,sorted(blocks))
            self.assertTrue(sp.verify_measurement_ledger(engine.store.path)["ok"])
            tail=engine.observe_flow_shadow_path_marks(2_001,10_000_000)
            self.assertEqual(tail["observed"],2)
            self.assertFalse(tail["more_due_available"])

    def test_larger_batch_stops_before_spending_item_reserve(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._backlogged_engine(directory)
            deadline = MagicMock()
            deadline.remaining.side_effect = [20.0,8.0,7.9]
            result=engine.observe_flow_shadow_path_marks(
                2_000,10_000_000,deadline=deadline)
            self.assertEqual(result["observed"],2)
            self.assertEqual(result["deferred"],30)
            self.assertEqual(result["stop_reason"],"deadline_reserve")
            self.assertEqual(engine.v4_market.snapshot.call_count,2)
            self.assertFalse(result["batch_cap_reached"])

    def test_larger_batch_stops_on_provider_rate_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._backlogged_engine(directory)
            engine.v4_market.snapshot.side_effect = RuntimeError("RPC -429: too many requests")
            result=engine.observe_flow_shadow_path_marks(2_000,10_000_000)
            self.assertEqual(result["observed"],0)
            self.assertEqual(result["failures"],1)
            self.assertEqual(result["stop_reason"],"provider_rate_limited")
            self.assertEqual(engine.v4_market.snapshot.call_count,1)

    def test_larger_batch_yields_to_rpc_priority_without_retrying(self):
        with tempfile.TemporaryDirectory() as directory:
            engine = self._backlogged_engine(directory)
            engine.v4_market.snapshot.side_effect = rh.BackgroundRpcPriorityDeferred(
                "live reservation")
            result=engine.observe_flow_shadow_path_marks(2_000,10_000_000)
            self.assertEqual(result["observed"],0)
            self.assertEqual(result["failures"],0)
            self.assertEqual(result["deferred"],32)
            self.assertEqual(result["stop_reason"],"rpc_priority")
            self.assertEqual(engine.v4_market.snapshot.call_count,1)

    def test_zero_capacity_does_no_selection_or_rpc(self):
        engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
        engine.store,engine.v4_market=MagicMock(),MagicMock()
        result=engine.observe_flow_shadow_path_marks(2_000,10_000_000,limit=0)
        self.assertEqual(result["observed"],0)
        engine.store.due_flow_shadow_path_marks.assert_not_called()
        engine.v4_market.snapshot.assert_not_called()

    def test_operational_capacity_change_preserves_frozen_policy_hash(self):
        self.assertEqual(sp.policy_hash(),
            "6b61bb4f2f645a90343546c5e78a249237946d379cd79c19c3e950a52edcef21")

    def test_engine_quotes_the_frozen_target_block_not_current_head(self):
        with tempfile.TemporaryDirectory() as directory:
            store,result = ShadowPathStoreTests()._captured(directory)
            self.assertEqual(result["shadow_paths_created"],2)

            class Market:
                def __init__(self):
                    self.blocks = []

                def snapshot(self,candidate,*,include_execution_quote=True,
                             quote_block=None):
                    self.blocks.append(quote_block)
                    return {
                        "current_state_verified": True,
                        "quote_block": quote_block,
                        "price_usd": 1.0,"liquidity_usd": 50_000,
                        "execution_quote": {
                            "verified": True,
                            "passes_round_trip_limit": True,
                            "anchor_in_raw": candidate["shadow_entry_anchor_in_raw"],
                            "anchor_out_raw": str(int(candidate["shadow_entry_anchor_in_raw"])*99//100),
                            "round_trip_ratio": 0.99,
                            "token_out_raw": str(10**18),"token_decimals": 18,
                            "quote_block": quote_block,
                        },
                    }

            market = Market()
            engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
            engine.store,engine.v4_market = store,market
            summary = engine.observe_flow_shadow_path_marks(
                now=1_001.0,head_block=20_000,limit=1)
            self.assertEqual(summary["observed"],1)
            with store.connection() as connection:
                target = connection.execute(
                    "SELECT target_block FROM flow_shadow_path_schedule s"
                    " JOIN flow_shadow_path_measurements m USING(path_id,step_index)"
                    " ORDER BY m.sequence LIMIT 1").fetchone()[0]
            self.assertEqual(market.blocks,[target])
            self.assertNotEqual(target,20_000)

    def test_transport_failure_remains_retry_state_not_market_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            store,_result = ShadowPathStoreTests()._captured(directory)

            class Market:
                def snapshot(self,*_args,**_kwargs):
                    raise TimeoutError("provider timed out")

            engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
            engine.store,engine.v4_market = store,Market()
            summary = engine.observe_flow_shadow_path_marks(
                now=1_001.0,head_block=20_000,limit=1)
            self.assertEqual(summary["observed"],0)
            self.assertEqual(summary["failures"],1)
            self.assertEqual(summary["provider_unavailable"],1)
            with store.connection() as connection:
                measurements = connection.execute(
                    "SELECT COUNT(*) FROM flow_shadow_path_measurements"
                ).fetchone()[0]
                failure_class = connection.execute(
                    "SELECT failure_class FROM flow_shadow_path_attempts"
                ).fetchone()[0]
            self.assertEqual(measurements,0)
            self.assertEqual(failure_class,"provider_or_transport_failure")

    def test_wrong_block_attestation_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            store,_result = ShadowPathStoreTests()._captured(directory)

            class Market:
                def snapshot(self,candidate,*,include_execution_quote=True,
                             quote_block=None):
                    return {
                        "current_state_verified": True,
                        "quote_block": quote_block+1,
                        "execution_quote": {"verified": True,
                            "passes_round_trip_limit": True,
                            "quote_block": quote_block+1,
                            "token_out_raw": str(10**18),
                            "token_decimals": 18},
                    }

            engine = rh.RobinhoodLearningEngine.__new__(rh.RobinhoodLearningEngine)
            engine.store,engine.v4_market = store,Market()
            summary = engine.observe_flow_shadow_path_marks(
                now=1_001.0,head_block=20_000,limit=1)
            self.assertEqual(summary["observed"],0)
            self.assertEqual(summary["failures"],1)
            self.assertFalse(sp.verify_measurement_ledger(
                Path(directory) / "learning.sqlite3")["ok"])


class ShadowExitEvaluationTests(unittest.TestCase):
    def test_empty_prospective_corpus_reports_collecting_and_seals_once(self):
        with tempfile.TemporaryDirectory() as directory:
            rh.RobinhoodLearningStore(Path(directory) / "learning.sqlite3")
            first = sp.run_shadow_exit_evaluation(directory)
            second = sp.run_shadow_exit_evaluation(directory)
            self.assertEqual(first["status"],"collecting_training_paths")
            self.assertFalse(first["governance"]["promotion_enabled"])
            self.assertEqual(first["source"]["evidence_hash"],
                             second["source"]["evidence_hash"])
            ledger = sp.verify_evaluation_ledger(directory)
            self.assertTrue(ledger["ok"],ledger)
            self.assertEqual(ledger["records"],1)

    @staticmethod
    def _complete_path(identifier,signaled_at,pool):
        marks = {
            index: {
                "label": label,"status": "observed","exit_valid": True,
                "net_return": 0.1,"price_multiple": 1.1,
                "liquidity_usd": 50_000,
            }
            for index,(label,_seconds) in enumerate(sp.SCHEDULE)
        }
        return {
            "path_id": identifier,"pool_id": pool,"matched_path_id": None,
            "signal_role": "qualified","source_version": rh.SOURCE_V3,
            "signaled_at": signaled_at,"marks": marks,
        }

    def test_selection_is_sealed_before_forward_holdout_is_born(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_paths = [
                self._complete_path("train-a",1_000.0,"pool-a"),
                self._complete_path("train-b",1_001.0,"pool-b"),
                self._complete_path("already-known",1_002.0,"pool-c"),
            ]
            first_integrity = {
                "ok": True,"records": 54,"head": "1"*64,
                "status": "verified"}
            with (patch.object(sp,"MINIMUM_TRAIN_PATHS",2),
                  patch.object(sp,"MINIMUM_HOLDOUT_PATHS",1),
                  patch.object(sp,"_load_paths",return_value=first_paths),
                  patch.object(sp,"verify_measurement_ledger",
                               return_value=first_integrity)):
                first = sp.run_shadow_exit_evaluation(root)
            self.assertTrue(first["collection"]["selection_frozen"])
            self.assertEqual(first["collection"]["holdout_paths"],0)
            self.assertEqual(
                first["collection"]["pre_freeze_complete_paths_excluded"],1)
            frozen = first["selection"]["experiment"]

            future = self._complete_path(
                "future-holdout",float(frozen["frozen_at"])+1,"pool-d")
            second_integrity = {
                "ok": True,"records": 72,"head": "2"*64,
                "status": "verified"}
            with (patch.object(sp,"MINIMUM_TRAIN_PATHS",2),
                  patch.object(sp,"MINIMUM_HOLDOUT_PATHS",1),
                  patch.object(sp,"_load_paths",
                               return_value=[*first_paths,future]),
                  patch.object(sp,"verify_measurement_ledger",
                               return_value=second_integrity)):
                second = sp.run_shadow_exit_evaluation(root)
            self.assertEqual(second["collection"]["holdout_paths"],1)
            self.assertEqual(
                second["selection"]["selected_policy"],
                first["selection"]["selected_policy"])
            self.assertEqual(
                second["selection"]["experiment"]["record_hash"],
                frozen["record_hash"])

    def test_staged_proceeds_survive_a_later_nonexit(self):
        path = self._complete_path("signal",1_000,"pool")
        path["marks"][1]["net_return"] = 1.0
        path["marks"][2]["net_return"] = 2.0
        path["marks"][3].update(exit_valid=False,net_return=-1.0)
        result = sp._simulate(path,"staged_2x_3x_runner_7d")
        # 50% at 2x + 25% at 3x; the final 25% becomes unsellable.
        self.assertAlmostEqual(result["net_return"],0.75)
        self.assertTrue(result["non_exit"])

    def test_spot_price_jump_cannot_trigger_an_executable_take_profit(self):
        path = self._complete_path("signal",1_000,"pool")
        path["marks"][1]["price_multiple"] = 20.0
        result = sp._simulate(path,"fixed_3x_7d")
        self.assertEqual(result["exit_reason"],"maximum_hold")
        self.assertAlmostEqual(result["net_return"],0.1)


class ShadowReportRunnerTests(unittest.TestCase):
    def test_current_report_skips_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rh.RobinhoodLearningStore(root / "learning.sqlite3")
            sp.run_shadow_exit_evaluation(root)
            with patch.object(runner.subprocess,"run") as run:
                result = runner.refresh_shadow_exit_report(root)
            self.assertEqual(result["status"],"cadence_not_due")
            run.assert_not_called()

    def test_evaluation_timeout_does_not_fail_the_supervisor(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(runner.subprocess,"run",side_effect=
                              runner.subprocess.TimeoutExpired("offline",30)) as run:
                result = runner.refresh_shadow_exit_report(Path(directory))
            self.assertEqual(result["status"],"deferred")
            self.assertEqual(run.call_args.kwargs["timeout"],30)


if __name__ == "__main__":
    unittest.main()
