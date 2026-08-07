import json
import tempfile
import threading
import time
import unittest
from pathlib import Path

import chainseer_pons


TOKEN = "0x" + "1" * 40
DEPLOYER = "0x" + "2" * 40
POOL = "0x" + "3" * 40
TX_HASH = "0x" + "4" * 64
LAUNCH_BLOCK = chainseer_pons.PONS_FACTORIES[0].start_block + 100
LATEST_BLOCK = LAUNCH_BLOCK + 1_000


def candidate(**overrides):
    config = chainseer_pons.PONS_FACTORIES[0]
    value = dict(
        token_address=TOKEN,
        deployer=DEPLOYER,
        dex_factory=chainseer_pons.PONS_V3_FACTORY,
        pair_token=chainseer_pons.PONS_WETH,
        pool_address=POOL,
        factory_address=config.factory,
        factory_label=config.label,
        expected_locker=config.locker,
        launch_block=LAUNCH_BLOCK,
        transaction_hash=TX_HASH,
        log_index=7,
        dex_id=1,
        launch_config_id=2,
        position_id=1234,
        restrictions_end_block=LAUNCH_BLOCK + 2,
        initial_buy_amount_raw=10**15,
        name="Synthetic Pons",
        symbol="SPONS",
        decimals=18,
        total_supply_raw=chainseer_pons.PONS_FIXED_SUPPLY_RAW,
    )
    value.update(overrides)
    return chainseer_pons.PonsLaunchCandidate(**value)


def quote(token_in, token_out, amount_in, amount_out, block=200):
    return chainseer_pons.PonsQuote(
        token_in=token_in,
        token_out=token_out,
        amount_in_raw=amount_in,
        amount_out_raw=amount_out,
        sqrt_price_x96_after=2**96,
        initialized_ticks_crossed=0,
        gas_estimate=100_000,
        block_pin=block,
    )


class FakePonsRPC:
    def __init__(self, *, pool_address=POOL, locker=None, owner=None):
        config = chainseer_pons.PONS_FACTORIES[0]
        self.pool_address = pool_address
        self.locker = locker or config.locker
        self.owner = owner or self.locker
        self.context = None
        self.logs = []
        self.log_calls = []

    def bind_context(self, context):
        self.context = context

    def get_block_number(self):
        return LATEST_BLOCK

    def get_code(self, _address):
        return "0x60006000"

    def get_logs(self, start, end, address=None, topics=None):
        self.log_calls.append((start, end, address, topics))
        return [
            item for item in self.logs
            if start <= int(item["blockNumber"], 16) <= end
            and str(item.get("address", "")).lower() == str(address).lower()
        ]

    def erc20_name(self, _token):
        return "Synthetic Pons"

    def erc20_symbol(self, _token):
        return "SPONS"

    def erc20_decimals(self, _token):
        return 18

    def erc20_total_supply(self, _token):
        return chainseer_pons.PONS_FIXED_SUPPLY_RAW

    def get_launched_token(self, _factory, token):
        item = candidate()
        return {
            "token": token,
            "deployer": item.deployer,
            "paired_token": chainseer_pons.PONS_WETH,
            "position_manager": chainseer_pons.PONS_POSITION_MANAGER,
            "position_id": item.position_id,
            "dex_id": item.dex_id,
            "launch_config_id": item.launch_config_id,
            "restrictions_end_block": item.restrictions_end_block,
            "supply_raw": chainseer_pons.PONS_FIXED_SUPPLY_RAW,
            "is_token0": False,
            "pool_fee": chainseer_pons.PONS_POOL_FEE,
            "exists": True,
            "initial_buy_amount_raw": item.initial_buy_amount_raw,
        }

    def graduation_status(self, _factory, _token):
        return {
            "paired_principal_raw": 1 * 10**18,
            "threshold_raw": 42 * 10**17,
            "graduated": False,
            "progress": 1 / 4.2,
        }

    def token_liquidity_pool(self, _token):
        return self.pool_address

    def token_restriction_limits(self, _token):
        return {
            "max_wallet_amount_raw": 50_000_000 * 10**18,
            "max_tx_amount_raw": 55_000_000 * 10**18,
            "restriction_end_block": candidate().restrictions_end_block,
        }

    def v3_factory_pool(self, _token, _weth, _fee):
        return self.pool_address

    def pool_snapshot(self, _pool, _is_token0):
        return {
            "sqrt_price_x96": 2**96,
            "tick": 0,
            "observation_index": 0,
            "observation_cardinality": 2,
            "observation_cardinality_next": 2,
            "fee_protocol": 0,
            "unlocked": True,
            "token0": chainseer_pons.PONS_WETH,
            "token1": TOKEN,
            "fee": chainseer_pons.PONS_POOL_FEE,
            "liquidity": 10**18,
            "price_weth": 0.000001,
            "block_pin": 200,
        }

    def factory_locker(self, _factory):
        return self.locker

    def owner_of(self, _position_id):
        return self.owner

    def quote_exact_input_single(self, token_in, token_out, amount, _fee):
        if token_in.lower() == chainseer_pons.PONS_WETH.lower():
            return quote(token_in, token_out, amount, 10_000 * 10**18)
        return quote(token_in, token_out, amount, 97 * 10**14)


def fake_http(url, *, params=None, ledger=None):
    if "/holders" in url:
        payload = {
            "items": [
                {
                    "address": {"hash": POOL},
                    "value": str(800_000_000 * 10**18),
                },
                {
                    "address": {"hash": "0x" + "9" * 40},
                    "value": str(100_000_000 * 10**18),
                },
            ]
        }
    elif "/smart-contracts/" in url:
        payload = {
            "is_verified": True,
            "source_code": "contract Verified {}",
            "name": "Verified",
            "compiler_version": "0.8.30",
        }
    elif "dexscreener" in url:
        payload = {
            "pairs": [
                {
                    "pairAddress": POOL,
                    "priceUsd": "0.001",
                    "liquidity": {"usd": 50000},
                    "volume": {"h24": 10000},
                    "txns": {"h24": {"buys": 20, "sells": 10}},
                },
                {
                    "pairAddress": "0x" + "8" * 40,
                    "liquidity": {"usd": 999999999},
                },
            ]
        }
    else:
        payload = {}
    fact_id = (
        ledger.record("http", {"url": url, "params": params}, payload)
        if ledger else None
    )
    return payload, fact_id, False


class PonsAdapterTests(unittest.TestCase):
    def test_pons_rpc_retries_http_429_and_records_health(self):
        class Response:
            def __init__(self, status_code, payload, retry_after=None):
                self.status_code = status_code
                self.payload = payload
                self.headers = {}
                if retry_after is not None:
                    self.headers["Retry-After"] = retry_after

            def raise_for_status(self):
                if self.status_code >= 400:
                    error = RuntimeError(f"HTTP {self.status_code}")
                    error.response = self
                    raise error

            def json(self):
                return self.payload

        class Session:
            def __init__(self):
                self.responses = [
                    Response(429, {}, "0"),
                    Response(200, {
                        "jsonrpc": "2.0",
                        "id": 2,
                        "result": hex(LATEST_BLOCK),
                    }),
                ]

            def post(self, *_args, **_kwargs):
                return self.responses.pop(0)

        sleeps = []
        health = chainseer_pons.PonsRPCHealth()
        rpc = chainseer_pons.PonsRPC(
            health=health,
            maximum_attempts=2,
            base_backoff_seconds=0,
            sleeper=sleeps.append,
        )
        rpc._session = Session()
        self.assertEqual(rpc.get_block_number(), LATEST_BLOCK)
        self.assertEqual(health.total_attempts, 2)
        self.assertEqual(health.retries, 1)
        self.assertEqual(health.transient_failures, 1)
        self.assertEqual(health.successful_calls, 1)
        self.assertEqual(sleeps, [0.0])

    def test_required_rpc_failure_is_indeterminate_not_token_risk(self):
        class InfrastructureFailureRPC(FakePonsRPC):
            def get_code(self, _address):
                raise chainseer_pons.PonsInfrastructureError(
                    "rpc_http_429",
                    "rate limited",
                    retryable=True,
                    attempts=3,
                )

        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=InfrastructureFailureRPC(), http_get=fake_http
        )
        decision = analyzer.analyze(candidate())
        self.assertEqual(
            decision.analysis_status, "infrastructure_indeterminate"
        )
        self.assertEqual(decision.risk_level, "Indeterminate")
        self.assertFalse(decision.paper_entry_allowed)
        self.assertGreater(len(decision.infrastructure_errors), 0)
        self.assertFalse(
            any("Could not verify" in item for item in decision.hard_stops)
        )

    def test_indeterminate_observations_do_not_satisfy_admission(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        indeterminate = analyzer.analyze(candidate())
        indeterminate.analysis_status = "infrastructure_indeterminate"
        indeterminate.infrastructure_errors = ["rpc_http_429"]
        indeterminate.paper_entry_allowed = False
        indeterminate.risk_level = "Indeterminate"
        first = analyzer.analyze(candidate())
        first.block_pin += 1
        second = analyzer.analyze(candidate())
        second.block_pin += 2
        with tempfile.TemporaryDirectory() as temp_dir:
            quarantine = chainseer_pons.PonsAdmissionQuarantine(
                Path(temp_dir) / "admission.json"
            )
            initial = quarantine.record(
                candidate(), indeterminate, now=1000
            )
            after_one = quarantine.record(candidate(), first, now=1301)
            admitted = quarantine.record(candidate(), second, now=1602)
        self.assertEqual(initial["complete_observation_count"], 0)
        self.assertFalse(after_one["allowed"])
        self.assertEqual(after_one["complete_observation_count"], 1)
        self.assertTrue(admitted["allowed"])
        self.assertEqual(admitted["complete_observation_count"], 2)
        self.assertEqual(admitted["indeterminate_observation_count"], 1)

    def test_scheduler_prioritizes_clean_candidate_over_unsafe_cooldown(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        clean_candidate = candidate(token_address="0x" + "a" * 40)
        unsafe_candidate = candidate(token_address="0x" + "b" * 40)
        clean = analyzer.analyze(candidate()).to_dict()
        clean["token_address"] = clean_candidate.token_address
        unsafe = analyzer.analyze(candidate()).to_dict()
        unsafe["token_address"] = unsafe_candidate.token_address
        unsafe["analysis_status"] = "complete_unsafe"
        unsafe["paper_entry_allowed"] = False
        unsafe["risk_level"] = "Critical"
        unsafe["hard_stops"] = ["synthetic mutable risk"]
        unsafe["market"]["executable_quote"][
            "round_trip_loss_pct"
        ] = 40.0
        with tempfile.TemporaryDirectory() as temp_dir:
            quarantine = chainseer_pons.PonsAdmissionQuarantine(
                Path(temp_dir) / "admission.json"
            )
            quarantine._record_mapping(
                clean_candidate.to_dict(), clean, now=1000
            )
            quarantine._record_mapping(
                unsafe_candidate.to_dict(), unsafe, now=1000
            )
            plan = quarantine.refresh_plan(limit=1, now=2000)
        self.assertEqual(len(plan), 1)
        self.assertEqual(
            plan[0]["token_address"].lower(),
            clean_candidate.token_address.lower(),
        )
        self.assertEqual(plan[0]["scheduler"]["lane"], "promising")

    def test_schema_v1_429_migrates_without_rewriting_as_token_risk(self):
        token = TOKEN.lower()
        legacy = {
            "schema_version": 1,
            "protocol": "pons",
            "chain_id": chainseer_pons.PONS_CHAIN_ID,
            "policy": chainseer_pons.asdict(
                chainseer_pons.PonsAdmissionPolicy()
            ),
            "policy_sha256": chainseer_pons.hashlib.sha256(
                chainseer_pons._canonical_json(
                    chainseer_pons.asdict(
                        chainseer_pons.PonsAdmissionPolicy()
                    )
                ).encode("utf-8")
            ).hexdigest(),
            "candidates": {
                token: {
                    "token_address": TOKEN,
                    "symbol": "SPONS",
                    "first_seen_timestamp": 1000,
                    "last_seen_timestamp": 1000,
                    "status": "pending",
                    "observations": [{
                        "observed_timestamp": 1000,
                        "block_pin": 123,
                        "paper_entry_allowed": False,
                        "hard_stops": [
                            "Could not verify pool bytecode: "
                            "429 Client Error: Too Many Requests"
                        ],
                    }],
                }
            },
            "paper_only": True,
            "live_execution_enabled": False,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "admission.json"
            path.write_text(json.dumps(legacy), encoding="utf-8")
            quarantine = chainseer_pons.PonsAdmissionQuarantine(path)
            observation = quarantine.state["candidates"][token][
                "observations"
            ][0]
            ok, _ = quarantine.verify()
        self.assertTrue(ok)
        self.assertEqual(
            observation["analysis_status"],
            "infrastructure_indeterminate",
        )
        self.assertEqual(observation["hard_stops"], [])
        self.assertGreater(len(observation["infrastructure_errors"]), 0)

    def test_token_launched_log_decodes_all_canonical_fields(self):
        item = candidate()
        words = [
            chainseer_pons._address_word(item.pair_token),
            chainseer_pons._address_word(item.pool_address),
            chainseer_pons._uint_word(item.dex_id),
            chainseer_pons._uint_word(item.launch_config_id),
            chainseer_pons._uint_word(item.position_id),
            chainseer_pons._uint_word(item.restrictions_end_block),
            chainseer_pons._uint_word(item.initial_buy_amount_raw),
        ]
        raw = {
            "topics": [
                chainseer_pons.TOKEN_LAUNCHED_TOPIC,
                "0x" + chainseer_pons._address_word(item.token_address),
                "0x" + chainseer_pons._address_word(item.deployer),
                "0x" + chainseer_pons._address_word(item.dex_factory),
            ],
            "data": "0x" + "".join(words),
            "blockNumber": hex(item.launch_block),
            "transactionHash": item.transaction_hash,
            "logIndex": hex(item.log_index),
        }
        decoded = chainseer_pons.PonsObserver.decode_launch_log(
            raw, chainseer_pons.PONS_FACTORIES[0]
        )
        self.assertEqual(decoded.token_address.lower(), TOKEN.lower())
        self.assertEqual(decoded.pool_address.lower(), POOL.lower())
        self.assertEqual(decoded.position_id, item.position_id)
        self.assertEqual(
            decoded.restrictions_end_block, item.restrictions_end_block
        )

    def test_bounded_indexer_persists_cursor_and_catalog(self):
        item = candidate(launch_block=LATEST_BLOCK - 5)
        raw = {
            "topics": [
                chainseer_pons.TOKEN_LAUNCHED_TOPIC,
                "0x" + chainseer_pons._address_word(item.token_address),
                "0x" + chainseer_pons._address_word(item.deployer),
                "0x" + chainseer_pons._address_word(item.dex_factory),
            ],
            "data": "0x" + "".join([
                chainseer_pons._address_word(item.pair_token),
                chainseer_pons._address_word(item.pool_address),
                chainseer_pons._uint_word(item.dex_id),
                chainseer_pons._uint_word(item.launch_config_id),
                chainseer_pons._uint_word(item.position_id),
                chainseer_pons._uint_word(item.restrictions_end_block),
                chainseer_pons._uint_word(item.initial_buy_amount_raw),
            ]),
            "blockNumber": hex(item.launch_block),
            "address": chainseer_pons.PONS_FACTORIES[0].factory,
            "transactionHash": item.transaction_hash,
            "logIndex": hex(item.log_index),
        }
        rpc = FakePonsRPC()
        rpc.logs = [raw]
        with tempfile.TemporaryDirectory() as temp_dir:
            observer = chainseer_pons.PonsObserver(
                rpc, temp_dir, block_chunk=100, initial_lookback=100
            )
            launches = observer.sync(max_chunks=2)
            self.assertEqual(len(launches), 1)
            self.assertTrue(observer.cursor_path.is_file())
            self.assertTrue(observer.catalog_path.is_file())
            cursor = json.loads(observer.cursor_path.read_text("utf-8"))
            self.assertIn(
                chainseer_pons.PONS_FACTORIES[0].factory.lower(), cursor
            )
            self.assertTrue(
                all(end - start <= 99 for start, end, _, _ in rpc.log_calls)
            )

    def test_canonical_analysis_is_paper_eligible(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            analyzer = chainseer_pons.PonsRiskAnalyzer(
                evidence_root=temp_dir,
                rpc=FakePonsRPC(),
                http_get=fake_http,
            )
            decision = analyzer.analyze(candidate())
        self.assertTrue(decision.paper_entry_allowed)
        self.assertFalse(decision.live_entry_allowed)
        self.assertEqual(decision.hard_stops, [])
        self.assertTrue(
            decision.canonicality["locker_checks"][
                "position_nft_owned_by_locker"
            ]
        )
        self.assertAlmostEqual(
            decision.market["executable_quote"]["round_trip_loss_pct"],
            3.0,
        )
        self.assertEqual(
            decision.market["canonical_pool_market"]["liquidity_usd"],
            50000,
        )
        pipeline = decision.security["http_evidence_pipeline"]
        self.assertGreaterEqual(pipeline["workers"], 1)
        self.assertFalse(pipeline["candidate_parallelism"])

    def test_http_evidence_reads_are_parallel_but_provenance_order_is_stable(self):
        active = 0
        maximum_active = 0
        lock = threading.Lock()

        def slow_http(url, *, params=None, ledger=None):
            nonlocal active, maximum_active
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                time.sleep(0.03)
                return fake_http(url, params=params, ledger=ledger)
            finally:
                with lock:
                    active -= 1

        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(),
            http_get=slow_http,
            http_workers=5,
        )
        decision = analyzer.analyze(candidate())
        urls = [
            fact["query"]["url"]
            for fact in decision.provenance["facts"]
            if fact["source"] == "http"
        ]
        config = chainseer_pons.PONS_FACTORIES[0]
        self.assertGreaterEqual(maximum_active, 2)
        self.assertEqual(urls, [
            (
                f"{chainseer_pons.PONS_BLOCKSCOUT_API}/smart-contracts/"
                f"{TOKEN}"
            ),
            (
                f"{chainseer_pons.PONS_BLOCKSCOUT_API}/smart-contracts/"
                f"{config.factory}"
            ),
            (
                f"{chainseer_pons.PONS_BLOCKSCOUT_API}/smart-contracts/"
                f"{config.locker}"
            ),
            (
                f"{chainseer_pons.PONS_BLOCKSCOUT_API}/tokens/"
                f"{TOKEN}/holders"
            ),
            (
                f"{chainseer_pons.PONS_DEXSCREENER_API}/tokens/{TOKEN}"
            ),
        ])

    def test_verified_source_cache_survives_analyzer_restart_with_provenance(self):
        calls = []

        def counting_http(url, *, params=None, ledger=None):
            calls.append(url)
            return fake_http(url, params=params, ledger=ledger)

        with tempfile.TemporaryDirectory() as temp_dir:
            evidence_root = Path(temp_dir) / "analysis_evidence"
            cache_path = Path(temp_dir) / "cache" / "verified.json"
            first = chainseer_pons.PonsRiskAnalyzer(
                evidence_root=evidence_root,
                rpc=FakePonsRPC(),
                http_get=counting_http,
                source_cache_path=cache_path,
                source_cache_ttl_seconds=3600,
                http_workers=1,
            )
            first_ledger = chainseer_pons.ProvenanceLedger(
                evidence_root / "first"
            )
            first_ledger.block_pin = LATEST_BLOCK
            first_status = first._source_status(TOKEN, first_ledger)

            second = chainseer_pons.PonsRiskAnalyzer(
                evidence_root=evidence_root,
                rpc=FakePonsRPC(),
                http_get=counting_http,
                source_cache_path=cache_path,
                source_cache_ttl_seconds=3600,
                http_workers=1,
            )
            second_ledger = chainseer_pons.ProvenanceLedger(
                evidence_root / "second"
            )
            second_ledger.block_pin = LATEST_BLOCK
            second_status = second._source_status(TOKEN, second_ledger)
            persisted = json.loads(cache_path.read_text(encoding="utf-8"))
            entry = next(iter(persisted["entries"].values()))

        self.assertTrue(first_status["is_verified"])
        self.assertTrue(second_status["is_verified"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(persisted["schema_version"], 2)
        self.assertNotIn("payload", entry)
        self.assertIn("evidence_path", entry)
        cached_fact = second_ledger.to_dict()["facts"][0]
        self.assertTrue(cached_fact["cache_hit"])
        self.assertEqual(
            cached_fact["query"]["cache_layer"],
            "persistent_verified_source",
        )

    def test_unverified_source_is_never_persisted(self):
        calls = []

        def unverified_http(url, *, params=None, ledger=None):
            calls.append(url)
            payload = {"is_verified": False, "source_code": None}
            fact_id = (
                ledger.record("http", {"url": url}, payload)
                if ledger else None
            )
            return payload, fact_id, False

        with tempfile.TemporaryDirectory() as temp_dir:
            cache_path = Path(temp_dir) / "verified.json"
            analyzer = chainseer_pons.PonsRiskAnalyzer(
                rpc=FakePonsRPC(),
                http_get=unverified_http,
                source_cache_path=cache_path,
                http_workers=1,
            )
            for index in range(2):
                ledger = chainseer_pons.ProvenanceLedger()
                ledger.block_pin = LATEST_BLOCK + index
                self.assertFalse(
                    analyzer._source_status(TOKEN, ledger)["is_verified"]
                )
        self.assertEqual(len(calls), 2)

    def test_pool_mismatch_is_a_hard_stop(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(pool_address="0x" + "8" * 40),
            http_get=fake_http,
        )
        decision = analyzer.analyze(candidate())
        self.assertFalse(decision.paper_entry_allowed)
        self.assertTrue(
            any("event_pool_matches" in item for item in decision.hard_stops)
        )

    def test_position_nft_owner_mismatch_is_a_hard_stop(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(owner="0x" + "7" * 40),
            http_get=fake_http,
        )
        decision = analyzer.analyze(candidate())
        self.assertFalse(decision.paper_entry_allowed)
        self.assertTrue(
            any(
                "position_nft_owned_by_locker" in item
                for item in decision.hard_stops
            )
        )

    def test_paper_entry_uses_quoter_output_and_never_live(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        decision = analyzer.analyze(candidate())
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = chainseer_pons.PaperTradeLedger(
                Path(temp_dir) / "events.jsonl"
            )
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "state.json", ledger
            )
            position = trader.enter(candidate(), decision, now=1000)
            self.assertIsNotNone(position)
            self.assertLess(
                position["initial_quantity_raw"], 10_000 * 10**18
            )
            self.assertEqual(position["simulation"], "paper")
            ok, _ = ledger.verify()
            self.assertTrue(ok)
            with self.assertRaises(
                chainseer_pons.LiveExecutionDisabledError
            ):
                trader.broadcast_live_trade()

    def test_launch_protection_blocks_entry(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        decision = analyzer.analyze(candidate())
        decision.block_pin = candidate().launch_block + 1
        with tempfile.TemporaryDirectory() as temp_dir:
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "state.json",
                chainseer_pons.PaperTradeLedger(
                    Path(temp_dir) / "events.jsonl"
                ),
            )
            blockers = trader.entry_blockers(candidate(), decision)
            self.assertTrue(
                any(item.startswith("launch_protection_wait") for item in blockers)
            )
            self.assertIsNone(trader.enter(candidate(), decision))

    def test_risk_signal_closes_shadow_position_with_executable_quote(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        decision = analyzer.analyze(candidate())
        with tempfile.TemporaryDirectory() as temp_dir:
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "shadow.json",
                chainseer_pons.PaperTradeLedger(
                    Path(temp_dir) / "events.jsonl"
                ),
                event_namespace="shadow",
                enforce_position_limit=False,
            )
            position = trader.enter(candidate(), decision, now=1000)
            decision.hard_stops.append("synthetic post-entry risk")
            decision.risk_level = "Critical"
            events = trader.mark(
                TOKEN,
                decision,
                lambda amount: quote(TOKEN, chainseer_pons.PONS_WETH, amount, 8 * 10**15),
                now=1100,
            )
            self.assertEqual(len(events), 1)
            self.assertEqual(position["status"], "closed")
            self.assertEqual(position["close_reason"], "risk_signal")
            self.assertEqual(events[0]["event_type"], "pons_shadow_sell")

    def test_indeterminate_mark_never_closes_position_or_requests_quote(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        entry = analyzer.analyze(candidate())
        indeterminate = analyzer.analyze(candidate())
        indeterminate.analysis_status = "infrastructure_indeterminate"
        indeterminate.infrastructure_errors = ["rpc_http_429"]
        indeterminate.paper_entry_allowed = False
        indeterminate.risk_level = "Indeterminate"
        with tempfile.TemporaryDirectory() as temp_dir:
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "shadow.json",
                chainseer_pons.PaperTradeLedger(
                    Path(temp_dir) / "events.jsonl"
                ),
                event_namespace="shadow",
                enforce_position_limit=False,
            )
            position = trader.enter(candidate(), entry, now=1000)
            events = trader.mark(
                TOKEN,
                indeterminate,
                lambda _amount: self.fail(
                    "indeterminate analysis must not request an exit quote"
                ),
                now=1100,
            )
        self.assertEqual(events, [])
        self.assertEqual(position["status"], "open")
        self.assertEqual(
            position["last_infrastructure_errors"], ["rpc_http_429"]
        )

    def test_fast_quote_guard_enforces_stop_without_inventing_risk_signal(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        entry = analyzer.analyze(candidate())
        with tempfile.TemporaryDirectory() as temp_dir:
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "shadow.json",
                chainseer_pons.PaperTradeLedger(
                    Path(temp_dir) / "events.jsonl"
                ),
                event_namespace="shadow",
                enforce_position_limit=False,
            )
            position = trader.enter(candidate(), entry, now=1000)
            events = trader.mark_quote_guard(
                TOKEN,
                lambda amount: quote(
                    TOKEN,
                    chainseer_pons.PONS_WETH,
                    amount,
                    5 * 10**15,
                    block=LATEST_BLOCK,
                ),
                block_pin=LATEST_BLOCK,
                now=1060,
            )
        self.assertEqual(len(events), 1)
        self.assertEqual(position["status"], "closed")
        self.assertEqual(position["close_reason"], "stop_loss")
        self.assertNotIn("last_mark_hard_stops", position)
        self.assertEqual(position["last_mark_block"], LATEST_BLOCK)
        self.assertLess(position["realized_pnl_eth"], 0)
        self.assertLess(
            events[0]["payload"]["realized_pnl_eth"], 0
        )

    def test_failed_guard_quote_does_not_fake_mark_freshness(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        entry = analyzer.analyze(candidate())
        with tempfile.TemporaryDirectory() as temp_dir:
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "shadow.json",
                chainseer_pons.PaperTradeLedger(
                    Path(temp_dir) / "events.jsonl"
                ),
                event_namespace="shadow",
                enforce_position_limit=False,
            )
            position = trader.enter(candidate(), entry, now=1000)

            def fail_quote(_amount):
                raise RuntimeError("synthetic quoter failure")

            events = trader.mark_quote_guard(
                TOKEN,
                fail_quote,
                block_pin=LATEST_BLOCK,
                now=1060,
            )
        self.assertEqual(events, [])
        self.assertEqual(position["status"], "open")
        self.assertEqual(position["last_mark_timestamp"], 1000)
        self.assertEqual(position["last_quote_attempt_timestamp"], 1060)
        self.assertEqual(position["last_quote_attempt_block"], LATEST_BLOCK)
        self.assertIn("synthetic quoter failure", position["last_quote_error"])

    def test_policy_changes_require_a_new_root(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            state = Path(temp_dir) / "state.json"
            ledger = chainseer_pons.PaperTradeLedger(
                Path(temp_dir) / "events.jsonl"
            )
            trader = chainseer_pons.PonsPaperTrader(state, ledger)
            trader._save()
            with self.assertRaisesRegex(ValueError, "different paper policy"):
                chainseer_pons.PonsPaperTrader(
                    state,
                    ledger,
                    chainseer_pons.PonsPaperPolicy(amount_eth=0.02),
                )

    def test_managed_portfolio_requires_promotable_active_policy(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = chainseer_pons.PaperTradeLedger(
                Path(temp_dir) / "paper_events.jsonl"
            )
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "paper_state.json", ledger
            )
            controller = chainseer_pons.PonsManagedPortfolioController(
                Path(temp_dir) / "managed_portfolio.json"
            )
            missing = controller.evaluate(
                trader,
                ledger,
                {},
                prospective_cost_eth=0.01002,
                now=1000,
            )
            self.assertFalse(missing["allowed"])
            self.assertIn(
                "managed_policy_evidence_missing", missing["blockers"]
            )
            blocked = controller.evaluate(
                trader,
                ledger,
                {
                    "cohorts": {
                        "stability_v1": {
                            "promotion_blockers": [
                                "validation_return_non_positive"
                            ]
                        }
                    }
                },
                prospective_cost_eth=0.01002,
                now=1000,
            )
        self.assertFalse(blocked["allowed"])
        self.assertIn(
            "managed_active_policy_not_promotable",
            blocked["blockers"],
        )

    def test_managed_portfolio_enforces_exposure_and_position_limits(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = chainseer_pons.PaperTradeLedger(
                Path(temp_dir) / "paper_events.jsonl"
            )
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "paper_state.json", ledger
            )
            controller = chainseer_pons.PonsManagedPortfolioController(
                Path(temp_dir) / "managed_portfolio.json"
            )
            policy_learning = {
                "cohorts": {
                    "stability_v1": {"promotion_blockers": []}
                }
            }
            ready = controller.evaluate(
                trader,
                ledger,
                policy_learning,
                prospective_cost_eth=0.01002,
                now=1000,
            )
            self.assertTrue(ready["allowed"])
            for index in range(3):
                token = "0x" + f"{index + 5:x}" * 40
                trader.state["positions"][token] = {
                    "token_address": token,
                    "status": "open",
                    "initial_quantity_raw": 100,
                    "remaining_quantity_raw": 100,
                    "cost_basis_eth": 0.01,
                    "realized_pnl_eth": 0.0,
                    "last_modeled_exit_eth": 0.01,
                }
            blocked = controller.evaluate(
                trader,
                ledger,
                policy_learning,
                prospective_cost_eth=0.01002,
                now=1000,
            )
        self.assertIn("managed_position_limit", blocked["blockers"])
        self.assertIn("managed_exposure_limit", blocked["blockers"])

    def test_managed_portfolio_daily_loss_triggers_cooldown(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = chainseer_pons.PaperTradeLedger(
                Path(temp_dir) / "paper_events.jsonl"
            )
            ledger.append(
                "pons_paper_sell",
                {
                    "token_address": TOKEN,
                    "realized_pnl_eth": -0.011,
                    "paper_only": True,
                    "live_execution_enabled": False,
                },
            )
            trader = chainseer_pons.PonsPaperTrader(
                Path(temp_dir) / "paper_state.json", ledger
            )
            controller = chainseer_pons.PonsManagedPortfolioController(
                Path(temp_dir) / "managed_portfolio.json"
            )
            now = time.time()
            result = controller.evaluate(
                trader,
                ledger,
                {
                    "cohorts": {
                        "stability_v1": {"promotion_blockers": []}
                    }
                },
                now=now,
            )
            ok, report = controller.verify()
        self.assertFalse(result["allowed"])
        self.assertIn(
            "managed_daily_loss_breaker", result["blockers"]
        )
        self.assertIn(
            "managed_circuit_breaker_cooldown", result["blockers"]
        )
        self.assertGreater(result["cooldown_until"], now)
        self.assertTrue(ok, report)

    def test_performance_distribution_exposes_single_winner_concentration(self):
        outcomes = [{
            "symbol": "OUTLIER",
            "cost_eth": 0.01,
            "value_eth": 0.44,
            "multiple": 44.0,
        }]
        outcomes.extend({
            "symbol": f"LOSS{index}",
            "cost_eth": 0.01,
            "value_eth": 0.001,
            "multiple": 0.1,
        } for index in range(9))
        metrics = chainseer_pons._performance_distribution(outcomes)
        self.assertGreater(metrics["modeled_return_pct"], 0)
        self.assertAlmostEqual(metrics["return_without_best_pct"], -90.0)
        self.assertAlmostEqual(metrics["median_multiple"], 0.1)
        self.assertEqual(metrics["profitable"], 1)
        self.assertAlmostEqual(
            metrics["best_positive_profit_share_pct"], 100.0
        )
        self.assertEqual(metrics["best_position_symbol"], "OUTLIER")
        self.assertTrue(metrics["concentration_warning"])

    def test_policy_promotion_requires_profitable_breadth_and_robust_return(self):
        overall = {"closed": 30}
        concentrated = {
            "closed": 10,
            "profitable": 1,
            "modeled_return_pct": 100.0,
            "return_without_best_pct": -50.0,
            "best_positive_profit_share_pct": 100.0,
        }
        blockers = (
            chainseer_pons.PonsCounterfactualPolicyLearner
            ._promotion_blockers(overall, concentrated, -30.0)
        )
        self.assertIn("validation_profitable_breadth", blockers)
        self.assertIn(
            "validation_return_without_best_non_positive", blockers
        )
        self.assertIn("validation_profit_concentration", blockers)

        broad = {
            **concentrated,
            "profitable": 3,
            "return_without_best_pct": 20.0,
            "best_positive_profit_share_pct": 50.0,
        }
        self.assertEqual(
            chainseer_pons.PonsCounterfactualPolicyLearner
            ._promotion_blockers(overall, broad, -30.0),
            [],
        )

    def test_dashboard_snapshot_uses_real_state_and_excludes_unpriced_return(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = chainseer_pons.PonsPrototypeEngine(
                root=temp_dir,
                rpc=FakePonsRPC(),
                http_get=fake_http,
                record_timechain=False,
            )
            engine.shadow_trader.state["positions"] = {
                TOKEN.lower(): {
                    "token_address": TOKEN,
                    "symbol": "SPONS",
                    "status": "open",
                    "entry_timestamp": time.time() - 600,
                    "last_mark_timestamp": time.time() - 120,
                    "cost_basis_eth": 0.01,
                    "realized_eth": 0.0,
                    "last_modeled_exit_eth": 0.012,
                    "last_total_multiple": 1.2,
                    "analysis_score": 90.0,
                    "analysis_risk_level": "Low",
                },
                ("0x" + "9" * 40): {
                    "token_address": "0x" + "9" * 40,
                    "symbol": "UNMARKED",
                    "status": "open",
                    "entry_timestamp": time.time() - 60,
                    "last_mark_timestamp": time.time() - 60,
                    "cost_basis_eth": 0.02,
                    "realized_eth": 0.0,
                    "analysis_score": 80.0,
                    "analysis_risk_level": "Medium",
                },
            }
            engine.shadow_trader._save()
            (Path(temp_dir) / "schedule.json").write_text(
                json.dumps({"installed": True, "enabled": False}),
                encoding="utf-8",
            )
            (Path(temp_dir) / "scheduler_status.json").write_text(
                json.dumps({"status": "running"}),
                encoding="utf-8",
            )
            (Path(temp_dir) / "guard_schedule.json").write_text(
                json.dumps({"installed": True, "enabled": False}),
                encoding="utf-8",
            )
            (Path(temp_dir) / "guard_status.json").write_text(
                json.dumps({"status": "running"}),
                encoding="utf-8",
            )
            snapshot = chainseer_pons._dashboard_snapshot(engine)
        self.assertTrue(snapshot["paper_only"])
        self.assertFalse(snapshot["live_execution_enabled"])
        self.assertEqual(snapshot["shadow"]["open"], 2)
        self.assertEqual(snapshot["shadow"]["priced_positions"], 1)
        self.assertEqual(snapshot["shadow"]["unpriced_positions"], 1)
        self.assertAlmostEqual(
            snapshot["shadow"]["modeled_return_pct"], 20.0
        )
        self.assertIsNone(
            snapshot["shadow"]["return_without_best_pct"]
        )
        self.assertAlmostEqual(
            snapshot["shadow"]["median_multiple"], 1.2
        )
        self.assertEqual(snapshot["shadow"]["profitable_positions"], 1)
        self.assertTrue(snapshot["shadow"]["concentration_warning"])
        self.assertIn("admission", snapshot)
        self.assertIn("policy_learning", snapshot)
        self.assertIn("managed_portfolio", snapshot)
        self.assertIn("rpc_health", snapshot)
        self.assertEqual(
            snapshot["analysis_pipeline"]["entry_authority"],
            "pons_canonical_risk_v1",
        )
        self.assertFalse(
            snapshot["analysis_pipeline"]["full_chainseer_analysis_run"]
        )
        self.assertTrue(snapshot["integrity"]["ok"])
        self.assertEqual(snapshot["scheduler"]["status"], "disabled")
        self.assertEqual(snapshot["scheduler"]["stale_status"], "running")
        self.assertEqual(snapshot["guard_scheduler"]["status"], "disabled")
        self.assertEqual(
            snapshot["guard_scheduler"]["stale_status"], "running"
        )
        self.assertEqual(
            snapshot["integrity"]["managed_portfolio"],
            "verified managed paper portfolio state",
        )

    def test_dashboard_asset_is_read_only_and_has_no_example_data(self):
        html = (
            Path(chainseer_pons.__file__).with_name("pons_dashboard.html")
            .read_text("utf-8")
        )
        self.assertIn('fetch("/api/status"', html)
        self.assertIn("LIVE LOCAL DATA", html)
        self.assertIn("NO SIGNING", html)
        self.assertIn("Admission quarantine", html)
        self.assertIn("Counterfactual policy lab", html)
        self.assertIn("renderAdmission", html)
        self.assertIn("renderManaged", html)
        self.assertIn("Managed paper portfolio", html)
        self.assertIn("managed-drawdown", html)
        self.assertIn("admission-cooldown", html)
        self.assertIn("gate-rpc-retries", html)
        self.assertIn("robust-ex-best", html)
        self.assertIn("robust-concentration", html)
        self.assertIn("OUTLIER WARNING", html)
        self.assertNotIn("mockData", html)
        self.assertNotIn("example report", html.lower())

    def test_timechain_recorder_exposes_trade_event_sealing(self):
        self.assertTrue(
            callable(
                getattr(
                    chainseer_pons.PonsTimechainRecorder,
                    "seal_trade_event",
                    None,
                )
            )
        )

    def test_json_reader_accepts_windows_powershell_utf8_bom(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "status.json"
            path.write_text('{"status":"complete"}', encoding="utf-8-sig")
            self.assertEqual(
                chainseer_pons._read_json(path, {}),
                {"status": "complete"},
            )

    def test_admission_quarantine_requires_separated_clean_observations(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        first = analyzer.analyze(candidate())
        with tempfile.TemporaryDirectory() as temp_dir:
            quarantine = chainseer_pons.PonsAdmissionQuarantine(
                Path(temp_dir) / "admission.json"
            )
            initial = quarantine.record(candidate(), first, now=1000)
            self.assertFalse(initial["allowed"])
            self.assertIn(
                "admission_min_observations_1_of_2",
                initial["blockers"],
            )
            second = analyzer.analyze(candidate())
            second.block_pin += 1
            admitted = quarantine.record(candidate(), second, now=1301)
            self.assertTrue(admitted["allowed"])
            self.assertEqual(admitted["observation_count"], 2)

    def test_admission_quarantine_blocks_quote_deterioration(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        first = analyzer.analyze(candidate())
        second = analyzer.analyze(candidate())
        second.block_pin += 1
        second.market["executable_quote"]["round_trip_loss_pct"] = 12.0
        with tempfile.TemporaryDirectory() as temp_dir:
            quarantine = chainseer_pons.PonsAdmissionQuarantine(
                Path(temp_dir) / "admission.json"
            )
            quarantine.record(candidate(), first, now=1000)
            result = quarantine.record(candidate(), second, now=1301)
        self.assertFalse(result["allowed"])
        self.assertIn("admission_round_trip_limit", result["blockers"])
        self.assertIn(
            "admission_round_trip_deterioration", result["blockers"]
        )

    def test_counterfactual_learner_never_auto_adopts_small_sample(self):
        analyzer = chainseer_pons.PonsRiskAnalyzer(
            rpc=FakePonsRPC(), http_get=fake_http
        )
        first = analyzer.analyze(candidate())
        second = analyzer.analyze(candidate())
        second.block_pin += 1
        failed = analyzer.analyze(candidate())
        failed.block_pin += 2
        for decision in (first, second, failed):
            decision.market["canonical_pool_market"]["liquidity_usd"] = 2500
        failed.paper_entry_allowed = False
        failed.risk_level = "Critical"
        failed.hard_stops.append("synthetic liquidity collapse")
        failed.market["executable_quote"]["entry"]["amount_out_raw"] //= 10
        failed.market["executable_quote"]["immediate_exit"][
            "amount_out_raw"
        ] //= 10
        with tempfile.TemporaryDirectory() as temp_dir:
            quarantine = chainseer_pons.PonsAdmissionQuarantine(
                Path(temp_dir) / "admission.json"
            )
            quarantine.record(candidate(), first, now=1000)
            quarantine.record(candidate(), second, now=1301)
            quarantine.record(candidate(), failed, now=1602)
            learner = chainseer_pons.PonsCounterfactualPolicyLearner(
                Path(temp_dir) / "policy.json",
                chainseer_pons.PonsPaperPolicy(),
                chainseer_pons.PonsAdmissionPolicy(),
            )
            snapshot = learner.evaluate(quarantine)
        recommendation = snapshot["recommendation"]
        self.assertEqual(
            recommendation["status"],
            "insufficient_out_of_sample_evidence",
        )
        self.assertFalse(recommendation["auto_adopted"])
        self.assertTrue(recommendation["requires_human_approval"])
        self.assertGreaterEqual(
            snapshot["cohorts"]["stability_liquidity_3000_v1"][
                "avoided_control_losses"
            ],
            1,
        )

    def test_pipeline_names_pons_analyzer_as_entry_authority(self):
        pipeline = chainseer_pons.PonsPrototypeEngine.analysis_pipeline()
        self.assertEqual(
            pipeline["entry_authority"], "pons_canonical_risk_v1"
        )
        self.assertFalse(pipeline["full_chainseer_analysis_run"])
        self.assertFalse(pipeline["live_execution_enabled"])


class PonsSecurityOutcomeCollectorTests(unittest.TestCase):
    """The collector is only worth anything if a learn cycle actually runs it."""

    def _engine(self, temp_dir):
        return chainseer_pons.PonsPrototypeEngine(
            root=temp_dir,
            rpc=FakePonsRPC(),
            http_get=fake_http,
            record_timechain=False,
        )

    @staticmethod
    def _candidate():
        from datetime import datetime, timedelta, timezone
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)

        def obs(seconds, **kw):
            item = {
                "observed_at": (start + timedelta(seconds=seconds)).isoformat(),
                "analysis_ring": 11,
                "hard_stops": [],
                "liquidity_usd": 100_000.0,
                "risk_level": "Low",
                "score": 80.0,
            }
            item.update(kw)
            return item

        # Spaced for the shortest SERVICEABLE horizon (6h). The old 360s
        # spacing targeted the 5m horizon, which no longer exists.
        return {
            "observations": [
                obs(0),
                obs(6 * 3600 + 60,
                    hard_stops=["owner_can_mint"], liquidity_usd=1_000.0),
            ]
        }

    def test_collector_seals_due_outcomes_and_records_completion(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = self._engine(temp_dir)
            sealed = []

            class Recorder:
                def seal_security_outcome(self, ring, outcomes, **kw):
                    sealed.append((ring, outcomes, kw))
                    return 500 + len(sealed)

            engine.timechain = Recorder()
            engine.admission.state["candidates"] = {"t": self._candidate()}
            result = engine.collect_security_outcomes()

            self.assertEqual(result["sealed"], 1)
            self.assertEqual(result["errors"], [])
            ring, outcomes, kw = sealed[0]
            self.assertEqual(ring, 11)
            self.assertEqual(kw["horizon"], "6h")
            self.assertIn("owner_can_mint", outcomes["new_hard_stop_codes"])
            # Completion is persisted, so the next cycle does not re-seal it.
            self.assertTrue(engine.admission.state["completed_outcomes"])
            self.assertEqual(engine.collect_security_outcomes()["sealed"], 0)

    def test_collector_without_a_timechain_is_a_no_op(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = self._engine(temp_dir)
            engine.admission.state["candidates"] = {"t": self._candidate()}
            self.assertEqual(engine.collect_security_outcomes()["sealed"], 0)

    def test_one_failing_token_never_aborts_the_cycle(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = self._engine(temp_dir)
            calls = []

            class Recorder:
                def seal_security_outcome(self, ring, outcomes, **kw):
                    calls.append(ring)
                    if len(calls) == 1:
                        raise RuntimeError("chain busy")
                    return 501

            engine.timechain = Recorder()
            engine.admission.state["candidates"] = {
                "a": self._candidate(),
                "b": self._candidate(),
            }
            result = engine.collect_security_outcomes()
            self.assertEqual(result["sealed"], 1)
            self.assertEqual(len(result["errors"]), 1)
            self.assertIn("chain busy", result["errors"][0])

    def test_collector_does_not_clobber_admission_writes_from_the_same_cycle(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = self._engine(temp_dir)

            class Recorder:
                def seal_security_outcome(self, ring, outcomes, **kw):
                    return 502

            engine.timechain = Recorder()
            engine.admission.state["candidates"] = {"t": self._candidate()}
            engine.admission.state["candidates"]["t"]["marker"] = "set-earlier"
            engine.collect_security_outcomes()
            reloaded = engine.admission._load()
            self.assertEqual(
                reloaded["candidates"]["t"].get("marker"), "set-earlier"
            )

    def test_learn_once_reports_security_outcomes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = self._engine(temp_dir)
            sentinel = {"due": 3, "sealed": 2, "skipped_no_ring": 0, "errors": []}
            engine.collect_security_outcomes = lambda **kw: sentinel
            engine.observe = lambda *a, **kw: {"analyzed": 0}
            engine.mark_positions = lambda *a, **kw: {
                "marked": 0,
                "events": [],
                "errors": [],
                "indeterminate": [],
            }
            engine._evaluate_policy_learning = lambda: {"recommendation": None}
            engine.managed_portfolio.evaluate = lambda *a, **kw: {}
            summary = engine.learn_once(limit=0, mark_limit=0)
            self.assertEqual(summary["security_outcomes"], sentinel)


if __name__ == "__main__":
    unittest.main()


class PonsAnalysisEvidenceBindingTests(unittest.TestCase):
    """A Pons analysis must seal its evidence identity INTO the ring.

    An outcome can only ever bind to an analysis whose evidence manifest was
    sealed at analysis time -- a hash attached afterwards proves nothing about
    what was actually observed. Without this, every Pons analysis is
    permanently analysis_evidence_incomplete: unrecallable, and unable to carry
    a canonical outcome. Unbound rings cannot be retrofitted, so this has to be
    sealed from the moment an analysis is written.
    """

    @staticmethod
    def _provenance(block_pin=4242):
        return {
            "block_pin": block_pin,
            "anchor_type": "block_pin",
            "fact_count": 2,
            "facts": [
                {
                    "fact_id": "F0000", "source": "rpc",
                    "query_hash": "a" * 64, "response_hash": "b" * 64,
                    "block": block_pin,
                },
                {
                    "fact_id": "F0001", "source": "http",
                    "query_hash": "c" * 64, "response_hash": "d" * 64,
                    "block": block_pin,
                },
            ],
        }

    def test_seal_analysis_binds_evidence_in_the_ring(self):
        import inspect

        source = inspect.getsource(
            chainseer_pons.PonsTimechainRecorder.seal_analysis
            if hasattr(chainseer_pons, "PonsTimechainRecorder")
            else chainseer_pons.PonsTimechain.seal_analysis
        )
        self.assertIn("analysis_evidence_binding", source)
        # The subject/network must travel with it, or a reader cannot say
        # WHICH token on WHICH system the evidence belongs to.
        self.assertIn('"network": "pons"', source)
        self.assertIn("token_address", source)

    def test_binding_produces_a_complete_verifiable_manifest(self):
        from chainseer_outcome_ledger import analysis_evidence_binding

        binding = analysis_evidence_binding(
            self._provenance(), anchor_type="block_pin", anchor_value=4242
        )
        self.assertTrue(binding["evidence_manifest"]["complete_fact_hashes"])
        self.assertEqual(binding["anchor_type"], "block_pin")
        self.assertEqual(binding["anchor_value"], 4242)
        self.assertRegex(binding["evidence_hash"], r"^[a-f0-9]{64}$")

    def test_incomplete_provenance_is_reported_not_silently_accepted(self):
        """Missing fact hashes must mark the manifest incomplete rather than
        producing a confident-looking hash over nothing."""
        from chainseer_outcome_ledger import analysis_evidence_binding

        thin = {
            "block_pin": 7, "anchor_type": "block_pin", "fact_count": 1,
            "facts": [{"fact_id": "F0000", "source": "rpc", "block": 7}],
        }
        binding = analysis_evidence_binding(
            thin, anchor_type="block_pin", anchor_value=7
        )
        self.assertFalse(binding["evidence_manifest"]["complete_fact_hashes"])


class PonsSecurityOutcomeTests(unittest.TestCase):
    """Track A: security-horizon outcomes for EVERY analysis.

    92% of Pons analyses never trade, so trade-shaped outcomes would leave the
    admission gate -- Pons's defining feature -- permanently unvalidated. A rug
    in a token Pons REFUSED is the strongest evidence the gate worked, and only
    a security outcome can record it.
    """

    BASE = "2026-08-01T00:00:00+00:00"

    @staticmethod
    def _obs(offset_seconds, **kw):
        from datetime import datetime, timedelta, timezone
        start = datetime(2026, 8, 1, tzinfo=timezone.utc)
        item = {
            "observed_at": (start + timedelta(seconds=offset_seconds)).isoformat(),
            "analysis_ring": 11,
            "hard_stops": [],
            "liquidity_usd": 100_000.0,
            "risk_level": "Low",
            "score": 80.0,
        }
        item.update(kw)
        return item

    # ---- horizon selection ----

    def test_observation_must_actually_reach_the_horizon(self):
        obs = [self._obs(0), self._obs(60)]          # only 1 min elapsed
        self.assertEqual(chainseer_pons.pons_due_outcomes(obs, set()), [])

    def test_first_observation_at_or_after_each_horizon_is_selected(self):
        obs = [self._obs(0), self._obs(6 * 3600 + 60), self._obs(25 * 3600)]
        due = chainseer_pons.pons_due_outcomes(obs, set())
        self.assertEqual([d["horizon"] for d in due], ["6h", "24h"])
        self.assertEqual(due[0]["current"]["observed_at"], obs[1]["observed_at"])

    def test_observation_far_past_the_horizon_is_refused(self):
        """A 6h horizon measured 12 days late would be excluded by the outcome
        ledger anyway -- emitting it manufactures an untrainable record."""
        obs = [self._obs(0), self._obs(12 * 86400)]
        due = chainseer_pons.pons_due_outcomes(obs, set())
        self.assertNotIn("6h", [d["horizon"] for d in due])

    def test_completed_horizons_are_not_reissued(self):
        obs = [self._obs(0), self._obs(6 * 3600 + 60)]
        due = chainseer_pons.pons_due_outcomes(obs, {"11:6h"})
        self.assertEqual(due, [])

    def test_single_observation_yields_nothing(self):
        self.assertEqual(chainseer_pons.pons_due_outcomes([self._obs(0)], set()), [])

    def test_lock_wait_clears_the_longest_observed_guard_pass(self):
        """The guard and the learn cycle share .learn_once.lock.

        If the wait budget is shorter than a guard pass, the learn cycle gives
        up while the guard is still legitimately working and exits 1. That is
        not a deadlock or a stale lock -- it is the budget being wrong. At 90s
        against observed guard runs of 145-232s, learn failed every time it
        started during a guard pass.
        """
        self.assertGreater(
            chainseer_pons.PONS_RUN_LOCK_WAIT_SECONDS,
            chainseer_pons.PONS_OBSERVED_GUARD_SECONDS,
            "learn would abandon a run the guard is still doing",
        )

    def test_lock_wait_stays_below_the_stale_threshold(self):
        # Waiting longer than the staleness window would let a caller queue
        # behind a lock the system has already decided is abandoned.
        self.assertLess(
            chainseer_pons.PONS_RUN_LOCK_WAIT_SECONDS,
            chainseer_pons.PONS_RUN_LOCK_STALE_SECONDS,
        )

    def test_horizons_stop_at_the_maximum_hold(self):
        names = [h for h, _ in chainseer_pons.PONS_OUTCOME_HORIZONS]
        self.assertEqual(names, ["6h", "24h", "72h"])
        self.assertNotIn("30d", names)   # would outlive every position
        self.assertNotIn("7d", names)

    def test_no_declared_horizon_is_wildly_beyond_the_refresh_budget(self):
        """A horizon the schedule can never revisit in time is a promise to
        seal records the lateness gate must always exclude.

        Required refresh rate per horizon, at ~950 tracked candidates on an
        ~11-minute cycle: 72h needs ~2/cycle, 24h ~6, 6h ~23, 1h ~139, 5m ~522.
        The configured budget is PONS_ADMISSION_REFRESH_LIMIT.

        The allowance is 4x that budget rather than 1x, because the scheduler
        does not round-robin -- it prioritises by freshness shortfall, so a
        prioritised subset is revisited far faster than the mean. 6h sits at
        ~3.9x and is therefore partially measurable. 1h (23x) and 5m (87x) are
        not measurable under any prioritisation, which is why they were
        removed; this guard is what stops them being reinstated.
        """
        tracked, cycle_minutes, headroom = 950, 11, 4
        budget = chainseer_pons.PONS_ADMISSION_REFRESH_LIMIT
        for name, seconds in chainseer_pons.PONS_OUTCOME_HORIZONS:
            tolerance = max(15 * 60, 0.25 * seconds)
            window_hours = (seconds + tolerance) / 3600
            required = (tracked / window_hours) * (cycle_minutes / 60)
            with self.subTest(horizon=name):
                self.assertLessEqual(
                    required,
                    budget * headroom,
                    f"{name} needs ~{required:.0f} refreshes/cycle against a "
                    f"budget of {budget}; it cannot be graded",
                )

    def test_removed_horizons_would_fail_that_guard(self):
        # Pins WHY 5m and 1h are gone, so the reason survives the next edit.
        tracked, cycle_minutes = 950, 11
        budget = chainseer_pons.PONS_ADMISSION_REFRESH_LIMIT
        for name, seconds in (("5m", 300), ("1h", 3600)):
            tolerance = max(15 * 60, 0.25 * seconds)
            required = (tracked / ((seconds + tolerance) / 3600)) * (cycle_minutes / 60)
            with self.subTest(horizon=name):
                self.assertGreater(required, budget * 4)

    # ---- outcome content ----

    def test_new_hard_stops_and_liquidity_loss_are_recorded(self):
        base = self._obs(0)
        now = self._obs(3700, hard_stops=["HONEYPOT"], liquidity_usd=25_000.0)
        out = chainseer_pons.pons_security_outcomes(base, now, horizon_seconds=3600)
        self.assertEqual(out["new_hard_stop_codes"], ["HONEYPOT"])
        self.assertTrue(out["honeypot_observed"])
        self.assertAlmostEqual(out["liquidity_removed_pct"], 75.0, places=2)
        self.assertEqual(out["horizon_seconds"], 3600)

    def test_unmeasured_fields_are_omitted_not_reported_as_zero(self):
        base = self._obs(0, liquidity_usd=None)
        now = self._obs(3700, liquidity_usd=None)
        out = chainseer_pons.pons_security_outcomes(base, now, horizon_seconds=3600)
        self.assertNotIn("liquidity_removed_pct", out)
        self.assertNotIn("new_hard_stop_codes", out)

    def test_a_clean_token_produces_no_false_danger_signal(self):
        out = chainseer_pons.pons_security_outcomes(
            self._obs(0), self._obs(3700), horizon_seconds=3600
        )
        for danger in ("rug_pull", "honeypot_observed", "owner_privilege_used"):
            self.assertNotIn(danger, out)
        self.assertAlmostEqual(out["liquidity_removed_pct"], 0.0, places=6)

    def test_unparsable_timestamp_is_treated_as_absent(self):
        obs = [self._obs(0), self._obs(3700, observed_at="not-a-time")]
        self.assertEqual(chainseer_pons.pons_due_outcomes(obs, set()), [])
