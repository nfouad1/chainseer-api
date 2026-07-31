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
    _server_port,
    build_public_report,
)


TOKEN = "0x" + "a" * 40


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
    }


class FakeAgent:
    def __init__(self):
        self.calls = 0

    def analyze_token(self, address, full_report=False):
        self.calls += 1
        report = sample_internal_report()
        report["token_address"] = address
        return report


class AnalyzeRequestTests(unittest.TestCase):
    def test_accepts_valid_address(self):
        request = AnalyzeRequest(address=TOKEN)
        self.assertEqual(request.address, TOKEN)

    def test_rejects_invalid_address(self):
        with self.assertRaises(ValidationError):
            AnalyzeRequest(address="not-an-address")


class PublicReportTests(unittest.TestCase):
    def test_public_schema_omits_raw_queries(self):
        public = build_public_report(sample_internal_report())
        self.assertEqual(public["schema_version"], "1.0")
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


class ServiceTests(unittest.TestCase):
    def settings(self, root):
        return Settings(
            environment="test",
            api_token="",
            chain_root=root,
            queue_size=4,
            result_ttl_seconds=3600,
            cache_ttl_seconds=300,
            rate_limit_per_minute=6,
            shutdown_grace_seconds=10,
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
                status = service.watch_status()
                self.assertFalse(status["enabled"])
                self.assertEqual(len(status["subscriptions"]), 1)
                self.assertTrue(service.watch_unsubscribe(TOKEN))
                self.assertEqual(
                    service.watch_status()["subscriptions"], []
                )
            finally:
                service.stop()


if __name__ == "__main__":
    unittest.main()
