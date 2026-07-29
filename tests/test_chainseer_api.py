import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from chainseer_api import (
    AnalysisService,
    AnalyzeRequest,
    Settings,
    SingleProcessLease,
    SlidingWindowRateLimiter,
    WatchRequest,
    _server_port,
    build_public_report,
    deterministic_benchmark_split,
)
from chainseer_benchmark import load_jsonl
from chainseer_entity_graph import build_robinhood_entity_graph


TOKEN = "0x" + "a" * 40
SOLANA_MINT = "So11111111111111111111111111111111111111112"


def sample_internal_report():
    return {
        "token_address": TOKEN,
        "token_name": "Example",
        "token_symbol": "EX",
        "chain_id": 4663,
        "timestamp": "2026-07-23T12:00:00+00:00",
        "explorer_url": f"https://example.invalid/token/{TOKEN}",
        "analysis_ring": 42,
        "analysis_ring_hash": "f" * 64,
        "cognitive_ring": 43,
        "cognitive_ring_hash": "e" * 64,
        "cognition": {
            "status": "complete",
            "senses": [{"id": 1, "name": "Grounding Stabilizer"}],
            "modalities": [{"id": 2, "name": "Richness Scoring"}],
        },
        "poq_scores": {
            "coherence": 230,
            "relevance": 240,
            "novelty": 210,
            "consistency": 230,
            "depth": 220,
            "covenant": 240,
        },
        "poq_verdict": {"decision": "SEAL"},
        "analysis": {
            "action_label": "WATCHLIST",
            "risk_level": "Medium",
            "model_risk_level": "Medium",
            "legitimacy_score": 74.5,
            "confidence_grade": "MODERATE",
            "confidence": "6/8 sources",
            "recommendation": "Keep on a watchlist.",
            "hard_stop_overrides": [],
            "holder_assessment": {
                "holder_count": 1_250,
                "source": "Blockscout",
                "largest_non_amm_holder_pct": 8.2,
                "concentration_source": "Blockscout holders / pinned total supply",
            },
            "component_scores": {
                "security": 90,
                "liquidity": 70,
                "legitimacy": 74.5,
            },
            "red_flags": [],
            "yellow_flags": ["Young token"],
            "green_flags": ["Verified source"],
            "uncertain_components": {"lp_lock": "Lock data unavailable"},
            "extended_evidence": {
                "social_attention": {
                    "status": "observed",
                    "trust": "low",
                    "bounded_score": 60,
                    "channels": [{"type": "twitter", "url": "https://example.test"}],
                    "dexscreener_boosts": 2,
                    "can_trigger_hard_stop": False,
                    "caveat": "Manipulable context.",
                },
                "cross_chain": {
                    "status": "provider_attested",
                    "foreign_markets": [{"chain": "base", "pairs": 1}],
                    "flow_records": [{"source_tx_hash": "0x" + "a" * 64}],
                    "verified_flow_count": 1,
                    "can_trigger_hard_stop": False,
                    "caveat": "Provider-attested.",
                },
                "mev_exposure": {
                    "status": "pre_trade_quote_required",
                    "risk_level": "Indeterminate",
                    "warnings": ["Quote required."],
                    "scoring_scope": "execution_risk_only",
                },
            },
        },
        "data": {
            "basic_info": {"name": "Example", "symbol": "EX"},
            "dex_pairs": {
                "primary_price_usd": 0.1,
                "market_cap": 1_000_000,
                "total_liquidity_usd": 100_000,
                "total_volume_24h": 25_000,
                "token_age_label": "2 days",
                "primary_amm_version": "v4",
            },
            "lp_lock": {
                "state": "custody_unverified",
                "amm_version": "v4",
                "method": "V4 position custody is not verified",
                "locked": False,
                "withdrawal_verified": False,
                "hard_stop_eligible": False,
            },
            "blockscout_holders": {
                "holders": [{"address": TOKEN, "balance_parsed": 10}],
                "adj_top_1_pct": 8.2,
                "adj_top_10_pct": 24.5,
                "concentration_basis": "total_supply",
            },
            "entity_graph": build_robinhood_entity_graph(
                TOKEN,
                {
                    "deployer": {
                        "creator_address": "0x" + "b" * 40,
                        "creation_tx_hash": "0x" + "c" * 64,
                    }
                },
                block_pin=12345,
            ),
        },
        "provenance": {
            "block_pin": 12345,
            "fact_count": 1,
            "facts": [
                {
                    "fact_id": "F0000",
                    "source": "rpc",
                    "query": {"method": "eth_call"},
                    "query_hash": "1" * 64,
                    "response_hash": "2" * 64,
                    "block": 12345,
                    "fetched_at": "2026-07-23T12:00:00+00:00",
                    "cache_hit": False,
                }
            ],
        },
        "infrastructure_indeterminate": [],
    }


class FakeAgent:
    def __init__(self):
        self.calls = 0

    def analyze_token(self, address, full_report=False):
        self.calls += 1
        report = sample_internal_report()
        report["token_address"] = address
        return report


class FakeSolanaAgent:
    def __init__(self):
        self.calls = 0

    def analyze_token(self, address):
        self.calls += 1
        report = sample_internal_report()
        report["token_address"] = address
        report["token_name"] = "Wrapped SOL"
        report["token_symbol"] = "SOL"
        report["chain_name"] = "Solana"
        report["chain_id"] = "mainnet-beta"
        report["provenance"]["anchor_type"] = "confirmed_slot_anchor"
        report["provenance"]["anchor_caveat"] = "Confirmed slot anchor."
        return report


class FakeBaseAgent(FakeAgent):
    network_key = "base"

    def analyze_token(self, address, full_report=False):
        report = super().analyze_token(address, full_report=full_report)
        report["chain_id"] = 8453
        report["chain"] = "base"
        report["chain_name"] = "Base"
        report["data"]["entity_graph"] = build_robinhood_entity_graph(
            address,
            {},
            block_pin=12345,
            network="base",
        )
        return report


class AnalyzeRequestTests(unittest.TestCase):
    def test_accepts_valid_address(self):
        request = AnalyzeRequest(address=TOKEN)
        self.assertEqual(request.address, TOKEN)

    def test_rejects_invalid_address(self):
        with self.assertRaises(ValidationError):
            AnalyzeRequest(address="not-an-address")

    def test_accepts_valid_solana_mint(self):
        request = AnalyzeRequest(
            network="solana",
            address=SOLANA_MINT,
        )
        self.assertEqual(request.network, "solana")
        self.assertEqual(request.address, SOLANA_MINT)

    def test_accepts_valid_base_contract(self):
        address = "0x" + "A" * 40
        request = AnalyzeRequest(network="base", address=address)
        self.assertEqual(request.network, "base")
        self.assertEqual(request.address, address)

    def test_rejects_invalid_solana_mint(self):
        with self.assertRaises(ValidationError):
            AnalyzeRequest(network="solana", address="2" * 32)

    def test_watch_request_accepts_solana_mint(self):
        request = WatchRequest(
            network="solana",
            address=SOLANA_MINT,
        )
        self.assertEqual(request.network, "solana")
        self.assertEqual(request.address, SOLANA_MINT)

    def test_network_address_types_do_not_cross(self):
        with self.assertRaises(ValidationError):
            AnalyzeRequest(network="robinhood", address=SOLANA_MINT)
        with self.assertRaises(ValidationError):
            AnalyzeRequest(network="solana", address=TOKEN)


class PublicReportTests(unittest.TestCase):
    def test_public_schema_omits_raw_queries(self):
        public = build_public_report(sample_internal_report())
        self.assertEqual(public["schema_version"], "1.2")
        self.assertEqual(public["timechain"]["ring"], 42)
        self.assertEqual(public["timechain"]["cognitive_ring"], 43)
        self.assertEqual(public["timechain"]["cognition"]["status"], "complete")
        self.assertEqual(public["evidence"]["facts"][0]["id"], "F0000")
        self.assertNotIn("query", public["evidence"]["facts"][0])
        self.assertEqual(len(public["evidence"]["ledger_hash"]), 64)
        self.assertEqual(
            public["liquidity_custody"]["state"],
            "custody_unverified",
        )
        self.assertEqual(public["liquidity_custody"]["amm_version"], "v4")
        self.assertFalse(
            public["liquidity_custody"]["withdrawal_verified"]
        )
        self.assertEqual(
            public["extended_evidence"]["social_attention"]["trust"], "low"
        )
        self.assertEqual(
            public["extended_evidence"]["mev_exposure"]["scoring_scope"],
            "execution_risk_only",
        )
        self.assertNotIn(
            "flow_records", public["extended_evidence"]["cross_chain"]
        )
        self.assertEqual(
            public["evidence"]["infrastructure_indeterminate"],
            [],
        )
        self.assertEqual(public["market"]["market_cap_usd"], 1_000_000)
        self.assertEqual(
            public["market"]["market_cap_kind"],
            "reported_market_cap",
        )
        self.assertEqual(public["holders"]["count"], 1_250)
        self.assertEqual(public["holders"]["count_source"], "Blockscout")
        self.assertEqual(public["holders"]["largest_holder_pct"], 8.2)
        self.assertEqual(public["holders"]["top10_holder_pct"], 24.5)
        self.assertEqual(
            public["entity_graph"]["network"], "robinhood"
        )
        self.assertEqual(len(public["entity_graph"]["graph_hash"]), 64)
        self.assertEqual(
            public["entity_graph"]["summary"]["scoring_scope"],
            "evidence_only",
        )

    def test_public_schema_exposes_solana_slot_boundary(self):
        report = sample_internal_report()
        report["chain_name"] = "Solana"
        report["chain_id"] = "mainnet-beta"
        report["provenance"]["anchor_type"] = "confirmed_slot_anchor"
        report["provenance"]["anchor_caveat"] = "Confirmed slot anchor."
        report["analysis"]["holder_assessment"] = None
        report["data"].pop("blockscout_holders")
        report["data"]["basic_info"]["jupiter_holder_count"] = 420
        report["data"]["holder_concentration"] = {
            "largest_accounts": [{"token_account": SOLANA_MINT}],
            "top1_total_supply_pct": 9.5,
            "top10_total_supply_pct": 31.0,
            "method": "getTokenLargestAccounts_plus_owner_resolution",
            "pool_and_program_vaults_excluded": False,
            "caveat": "Largest accounts may include program vaults.",
        }
        public = build_public_report(report)
        self.assertEqual(public["token"]["chain"], "Solana")
        self.assertEqual(public["token"]["chain_id"], "mainnet-beta")
        self.assertEqual(
            public["evidence"]["anchor_type"],
            "confirmed_slot_anchor",
        )
        self.assertEqual(
            public["evidence"]["anchor_caveat"],
            "Confirmed slot anchor.",
        )
        self.assertEqual(public["holders"]["count"], 420)
        self.assertEqual(public["holders"]["count_source"], "Jupiter")
        self.assertEqual(public["holders"]["sample_size"], 1)
        self.assertFalse(
            public["holders"]["pool_and_program_vaults_excluded"]
        )

    def test_public_schema_does_not_mislabel_holder_sample_as_count(self):
        report = sample_internal_report()
        report["chain_name"] = "Solana"
        report["analysis"]["holder_assessment"] = None
        report["data"].pop("blockscout_holders")
        report["data"]["holder_concentration"] = {
            "largest_accounts": [
                {"token_account": f"account-{index}"}
                for index in range(20)
            ],
            "top1_total_supply_pct": 12.0,
            "method": "getTokenLargestAccounts_plus_owner_resolution",
        }
        public = build_public_report(report)
        self.assertIsNone(public["holders"]["count"])
        self.assertEqual(public["holders"]["count_status"], "unavailable")
        self.assertEqual(public["holders"]["sample_size"], 20)
        self.assertIn("exact holder count was unavailable", public["holders"]["caveat"])


class RateLimiterTests(unittest.TestCase):
    def test_sliding_window(self):
        limiter = SlidingWindowRateLimiter(limit=2, window_seconds=60)
        self.assertTrue(limiter.allow("client", now=10))
        self.assertTrue(limiter.allow("client", now=11))
        self.assertFalse(limiter.allow("client", now=12))
        self.assertTrue(limiter.allow("client", now=71))


class SettingsTests(unittest.TestCase):
    def test_server_port_uses_platform_port(self):
        with patch.dict("os.environ", {"PORT": "10000"}, clear=True):
            self.assertEqual(_server_port(), 10000)

    def test_explicit_chainseer_port_overrides_platform_port(self):
        with patch.dict(
            "os.environ",
            {"PORT": "10000", "CHAINSEER_API_PORT": "8000"},
            clear=True,
        ):
            self.assertEqual(_server_port(), 8000)

    def test_production_rejects_placeholder_token(self):
        with patch.dict(
            "os.environ",
            {"CHAINSEER_ALLOWED_HOSTS": "api.usechainseer.com"},
            clear=False,
        ):
            settings = Settings(
                environment="production",
                api_token="replace-with-at-least-32-random-characters",
                rpc_url="https://rpc.mainnet.chain.robinhood.com",
                chain_root=str(Path.cwd().resolve() / "chainseer_chain"),
                allowed_origins=("https://usechainseer.com",),
                allowed_hosts=("api.usechainseer.com",),
            )
            with self.assertRaises(RuntimeError):
                settings.validate()

    def test_production_configuration_accepts_strict_values(self):
        with patch.dict(
            "os.environ",
            {"CHAINSEER_ALLOWED_HOSTS": "api.usechainseer.com"},
            clear=False,
        ):
            settings = Settings(
                environment="production",
                api_token="a-secure-production-token-with-40-characters",
                rpc_url="https://rpc.mainnet.chain.robinhood.com",
                chain_root=str(Path.cwd().resolve() / "chainseer_chain"),
                allowed_origins=("https://usechainseer.com",),
                allowed_hosts=("api.usechainseer.com",),
            )
            settings.validate()

    def test_production_benchmark_capture_requires_versioned_absolute_storage(self):
        with patch.dict(
            "os.environ",
            {"CHAINSEER_ALLOWED_HOSTS": "api.usechainseer.com"},
            clear=False,
        ):
            settings = Settings(
                environment="production",
                api_token="a-secure-production-token-with-40-characters",
                rpc_url="https://rpc.mainnet.chain.robinhood.com",
                chain_root=str(Path.cwd().resolve() / "chainseer_chain"),
                allowed_origins=("https://usechainseer.com",),
                allowed_hosts=("api.usechainseer.com",),
                benchmark_capture_enabled=True,
                benchmark_root=str(Path.cwd().resolve() / "benchmark_data"),
                benchmark_analyzer_version="local-unversioned",
            )
            with self.assertRaises(RuntimeError):
                settings.validate()

    def test_benchmark_split_is_stable_per_token(self):
        first = deterministic_benchmark_split("robinhood", TOKEN)
        second = deterministic_benchmark_split(
            "robinhood",
            TOKEN.upper(),
        )
        self.assertEqual(first, second)
        self.assertIn(first, {"train", "validation", "test"})

    def test_render_commit_is_default_benchmark_analyzer_version(self):
        with patch.dict(
            "os.environ",
            {"RENDER_GIT_COMMIT": "abc123def456"},
            clear=True,
        ):
            self.assertEqual(
                Settings().benchmark_analyzer_version,
                "abc123def456",
            )


class ServiceTests(unittest.TestCase):
    def settings(
        self,
        root,
        *,
        benchmark_root=None,
        benchmark_capture_enabled=False,
    ):
        return Settings(
            environment="test",
            api_token="",
            chain_root=root,
            queue_size=4,
            result_ttl_seconds=3600,
            cache_ttl_seconds=300,
            rate_limit_per_minute=6,
            shutdown_grace_seconds=10,
            benchmark_capture_enabled=benchmark_capture_enabled,
            benchmark_root=(
                benchmark_root
                or str(Path(root).resolve().parent / "benchmark_data")
            ),
            benchmark_analyzer_version="test-commit",
        )

    def test_worker_returns_and_caches_structured_result(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            fake = FakeAgent()
            service._agent = fake
            service.start()
            try:
                accepted = service.submit(TOKEN)
                deadline = time.time() + 3
                job = service.get(accepted.job_id)
                while job and job.status not in {"succeeded", "failed"}:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                    job = service.get(accepted.job_id)
                self.assertIsNotNone(job)
                self.assertEqual(job.status, "succeeded")
                self.assertEqual(job.result["token"]["address"], TOKEN)
                self.assertEqual(fake.calls, 1)

                cached = service.submit(TOKEN)
                cached_job = service.get(cached.job_id)
                self.assertTrue(cached.cached)
                self.assertEqual(cached_job.status, "succeeded")
                self.assertEqual(fake.calls, 1)
            finally:
                service.stop()

    def test_single_process_lease(self):
        with tempfile.TemporaryDirectory() as root:
            first = SingleProcessLease(root)
            second = SingleProcessLease(root)
            first.acquire()
            try:
                with self.assertRaises(RuntimeError):
                    second.acquire()
            finally:
                first.release()
            second.acquire()
            second.release()
            third = SingleProcessLease(root)
            third.acquire()
            third.release()

    def test_authenticated_watch_state_is_managed_by_service(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            service._agent = FakeAgent()
            service.start()
            try:
                subscription = service.watch_subscribe(TOKEN)
                self.assertEqual(subscription["token_address"], TOKEN)
                solana_subscription = service.watch_subscribe(
                    SOLANA_MINT,
                    "solana",
                )
                self.assertEqual(
                    solana_subscription["token_address"],
                    SOLANA_MINT,
                )
                status = service.watch_status()
                self.assertFalse(status["enabled"])
                self.assertEqual(len(status["subscriptions"]), 2)
                self.assertEqual(
                    status["subscription_counts"],
                    {"robinhood": 1, "base": 0, "solana": 1},
                )
                self.assertTrue(service.watch_unsubscribe(TOKEN))
                self.assertTrue(
                    service.watch_unsubscribe(SOLANA_MINT, "solana")
                )
                self.assertEqual(
                    service.watch_status()["subscriptions"], []
                )
            finally:
                service.stop()

    def test_worker_routes_solana_and_keeps_network_cache_separate(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            evm = FakeAgent()
            solana = FakeSolanaAgent()
            service._agent = evm
            service._solana_agent = solana
            service.start()
            try:
                accepted = service.submit(SOLANA_MINT, "solana")
                deadline = time.time() + 3
                job = service.get(accepted.job_id)
                while job and job.status not in {"succeeded", "failed"}:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                    job = service.get(accepted.job_id)
                self.assertIsNotNone(job)
                self.assertEqual(job.status, "succeeded")
                self.assertEqual(job.network, "solana")
                self.assertEqual(job.result["token"]["chain"], "Solana")
                self.assertEqual(solana.calls, 1)
                self.assertEqual(evm.calls, 0)

                cached = service.submit(SOLANA_MINT, "solana")
                self.assertTrue(cached.cached)
                self.assertEqual(solana.calls, 1)
                self.assertIn(f"solana:{SOLANA_MINT}", service.cache)
                self.assertNotIn(
                    f"robinhood:{SOLANA_MINT}",
                    service.cache,
                )
            finally:
                service.stop()

    def test_worker_routes_base_and_keeps_network_cache_separate(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            robinhood = FakeAgent()
            base = FakeBaseAgent()
            service._agent = robinhood
            service._base_agent = base
            service.start()
            try:
                accepted = service.submit(TOKEN.upper(), "base")
                deadline = time.time() + 3
                job = service.get(accepted.job_id)
                while job and job.status not in {"succeeded", "failed"}:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                    job = service.get(accepted.job_id)
                self.assertIsNotNone(job)
                self.assertEqual(job.status, "succeeded")
                self.assertEqual(job.result["token"]["chain"], "Base")
                self.assertEqual(job.result["token"]["chain_id"], 8453)
                self.assertEqual(
                    job.result["entity_graph"]["network"], "base"
                )
                self.assertEqual(base.calls, 1)
                self.assertEqual(robinhood.calls, 0)

                cached = service.submit(TOKEN, "base")
                self.assertTrue(cached.cached)
                self.assertEqual(base.calls, 1)
                self.assertIn(f"base:{TOKEN}", service.cache)
                self.assertNotIn(f"robinhood:{TOKEN}", service.cache)
            finally:
                service.stop()

    def test_fresh_analysis_is_captured_once_and_cache_hit_is_not(self):
        with tempfile.TemporaryDirectory() as root:
            chain_root = str(Path(root) / "chain")
            benchmark_root = str(Path(root) / "benchmark")
            service = AnalysisService(
                self.settings(
                    chain_root,
                    benchmark_root=benchmark_root,
                    benchmark_capture_enabled=True,
                )
            )
            fake = FakeAgent()
            service._agent = fake
            service.start()
            try:
                accepted = service.submit(TOKEN)
                deadline = time.time() + 3
                job = service.get(accepted.job_id)
                while job and job.status not in {"succeeded", "failed"}:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                    job = service.get(accepted.job_id)
                self.assertEqual(job.status, "succeeded")
                self.assertEqual(
                    job.benchmark_capture["status"],
                    "captured",
                )
                self.assertEqual(
                    job.benchmark_capture["analyzer_version"],
                    "test-commit",
                )
                observations = load_jsonl(
                    Path(benchmark_root) / "observations-v1.jsonl"
                )
                self.assertEqual(len(observations), 1)
                self.assertEqual(
                    service.benchmark_status()["observations"],
                    1,
                )
                self.assertNotIn(
                    TOKEN,
                    str(service.benchmark_status()),
                )

                cached = service.submit(TOKEN)
                cached_job = service.get(cached.job_id)
                self.assertTrue(cached.cached)
                self.assertEqual(
                    cached_job.benchmark_capture["status"],
                    "cache_hit_not_recaptured",
                )
                self.assertEqual(
                    len(
                        load_jsonl(
                            Path(benchmark_root)
                            / "observations-v1.jsonl"
                        )
                    ),
                    1,
                )
            finally:
                service.stop()

    def test_capture_storage_failure_does_not_discard_analysis(self):
        with tempfile.TemporaryDirectory() as root:
            chain_root = str(Path(root) / "chain")
            unavailable = Path(root) / "not-a-directory"
            unavailable.write_text("occupied", encoding="utf-8")
            service = AnalysisService(
                self.settings(
                    chain_root,
                    benchmark_root=str(unavailable),
                    benchmark_capture_enabled=True,
                )
            )
            service._agent = FakeAgent()
            service.start()
            try:
                accepted = service.submit(TOKEN)
                deadline = time.time() + 3
                job = service.get(accepted.job_id)
                while job and job.status not in {"succeeded", "failed"}:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                    job = service.get(accepted.job_id)
                self.assertEqual(job.status, "succeeded")
                self.assertIsNotNone(job.result)
                self.assertEqual(
                    job.benchmark_capture["status"],
                    "failed",
                )
                self.assertEqual(
                    service.benchmark_status()["state"],
                    "degraded",
                )
            finally:
                service.stop()


if __name__ == "__main__":
    unittest.main()
