import json
import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pydantic import ValidationError

from chainseer_api import (
    AnalysisService,
    AnalyzeRequest,
    DeferredSealJob,
    Job,
    PreparedWatcherCommit,
    MemoryQueryRequest,
    Settings,
    SingleProcessLease,
    SlidingWindowRateLimiter,
    WatcherBusyError,
    WatchRequest,
    _cypher_tempre_runtime_status,
    _env_float,
    _server_port,
    build_public_report,
    deterministic_benchmark_split,
)
from chainseer_deferred import DeferredQueueItem
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

    def analyze_token(
        self, address, full_report=False, progress_callback=None
    ):
        self.calls += 1
        if progress_callback:
            progress_callback(
                "collecting_external_evidence", 15, "Collecting evidence"
            )
            progress_callback("sealing_timechain", 90, "Sealing Timechain")
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

    def test_memory_query_is_subject_scoped_and_topic_bounded(self):
        request = MemoryQueryRequest(
            network="base",
            address=TOKEN,
            topics=["latest_assessment", "outcomes", "outcomes"],
            limit=10,
        )
        self.assertEqual(request.network, "base")
        self.assertEqual(request.topics, ["latest_assessment", "outcomes"])
        with self.assertRaises(ValidationError):
            MemoryQueryRequest(
                network="base",
                address=TOKEN,
                topics=["raw_rings"],
            )


class PublicReportTests(unittest.TestCase):
    def test_public_schema_omits_raw_queries(self):
        public = build_public_report(sample_internal_report())
        self.assertEqual(public["schema_version"], "1.3")
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

    def test_public_schema_exposes_bounded_temporal_history(self):
        report = sample_internal_report()
        report["temporal_entity_graph"] = {
            "available": True,
            "schema_version": "1.0",
            "projection_hash": "d" * 64,
            "source_chain": {"head_index": 42, "head_hash": "f" * 64},
            "analysis_count": 2,
            "risk_evolution": {"total_score_delta": -5},
            "risk_timeline": [
                {"analysis_ring": {"index": index}, "score": 80 - index}
                for index in range(25)
            ],
            "relationship_events": [
                {"event": "reaffirmed", "relationship_id": str(index)}
                for index in range(45)
            ],
            "shared_entities": [
                {"identity_id": str(index)} for index in range(25)
            ],
        }
        temporal = build_public_report(report)["entity_graph"]["temporal"]
        self.assertTrue(temporal["available"])
        self.assertEqual(temporal["analysis_count"], 2)
        self.assertEqual(len(temporal["risk_timeline"]), 20)
        self.assertEqual(len(temporal["relationship_events"]), 40)
        self.assertEqual(len(temporal["shared_entities"]), 20)

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

    def test_global_limit_bounds_throughput_across_rotated_identities(self):
        # request_identity() cannot verify the caller-supplied identity
        # header, so a caller rotating a fresh fake identity every request
        # would otherwise never hit the per-identity limit at all. The
        # global cap must still bind regardless of how many distinct
        # identities are claimed.
        limiter = SlidingWindowRateLimiter(
            limit=100, window_seconds=60, global_limit=3
        )
        self.assertTrue(limiter.allow("identity-a", now=1))
        self.assertTrue(limiter.allow("identity-b", now=1))
        self.assertTrue(limiter.allow("identity-c", now=1))
        self.assertFalse(limiter.allow("identity-d", now=1))
        # A brand-new identity is still refused while the global window is
        # full, even though it has never been seen before.
        self.assertFalse(limiter.allow("identity-e", now=1))
        self.assertTrue(limiter.allow("identity-f", now=62))

    def test_tracked_identities_are_bounded_and_evicted_lru(self):
        limiter = SlidingWindowRateLimiter(limit=100, window_seconds=60)
        limiter.MAX_TRACKED_IDENTITIES = 3
        limiter.allow("a", now=1)
        limiter.allow("b", now=1)
        limiter.allow("c", now=1)
        self.assertEqual(len(limiter._events), 3)
        # A fourth distinct identity must not grow the tracked set past the
        # cap -- the least-recently-used one ("a") is evicted instead.
        limiter.allow("d", now=1)
        self.assertEqual(len(limiter._events), 3)
        self.assertNotIn("a", limiter._events)
        self.assertIn("d", limiter._events)


class SettingsTests(unittest.TestCase):
    def test_server_port_uses_platform_port(self):
        with patch.dict("os.environ", {"PORT": "10000"}, clear=True):
            self.assertEqual(_server_port(), 10000)

    def test_cypher_tempre_runtime_attests_exact_version(self):
        with tempfile.TemporaryDirectory() as temp:
            skill = Path(temp)
            (skill / "VERSION").write_text(
                "3.30.08-tca.1\n", encoding="utf-8"
            )
            with patch.dict(
                "os.environ",
                {
                    "CHAINSEER_SKILL_DIR": str(skill),
                    "CHAINSEER_CYPHER_TEMPRE_VERSION": "3.30.08-tca.1",
                    "CHAINSEER_CYPHER_TEMPRE_COMMIT": "abc123",
                },
                clear=False,
            ):
                status = _cypher_tempre_runtime_status()
        self.assertEqual(status["status"], "verified")
        self.assertEqual(status["version"], "3.30.08-tca.1")
        self.assertEqual(status["commit"], "abc123")

    def test_ci_and_docker_pin_the_same_cypher_tempre_runtime(self):
        root = Path(__file__).resolve().parents[1]
        docker = (root / "Dockerfile.api").read_text(encoding="utf-8")
        workflow = (root / ".github/workflows/test.yml").read_text(
            encoding="utf-8"
        )
        commit = "963ee364718e369718a2d8b15754305ae28806c5"
        version = "3.30.08-tca.1"
        self.assertIn(commit, docker)
        self.assertIn(commit, workflow)
        self.assertIn(version, docker)
        self.assertIn(version, workflow)
        self.assertIn("FrostedFlaming0/cypher-tempre-tcaFork", docker)
        self.assertIn("FrostedFlaming0/cypher-tempre-tcaFork", workflow)
        self.assertNotIn("patch_timechain_seal_timestamp", docker)

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

    def test_memory_backup_root_cannot_be_inside_timechain(self):
        chain_root = Path.cwd().resolve() / "chainseer_chain"
        settings = Settings(
            chain_root=str(chain_root),
            memory_backup_root=str(chain_root / "backups"),
        )
        with self.assertRaises(RuntimeError):
            settings.validate()


class TrustedHostCheckTests(unittest.TestCase):
    """Health-check paths must stay reachable for infrastructure probes (Fly,
    Render, etc.) that don't send an externally-valid Host header, while
    every other route keeps enforcing CHAINSEER_ALLOWED_HOSTS. Uses the
    TestClient without the `with` context manager so ASGI lifespan (which
    would start SERVICE and attempt real RPC calls) never runs -- only
    middleware behavior is under test here."""

    @classmethod
    def setUpClass(cls):
        from fastapi.testclient import TestClient

        import chainseer_api

        cls.client = TestClient(chainseer_api.app)
        # Not in the default allowed_hosts (localhost,127.0.0.1,testserver).
        cls.bad_host = "evil.example.invalid"

    def test_health_live_ignores_untrusted_host_header(self):
        response = self.client.get(
            "/health/live", headers={"host": self.bad_host}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    def test_health_ready_reaches_handler_despite_untrusted_host_header(self):
        response = self.client.get(
            "/health/ready", headers={"host": self.bad_host}
        )
        # SERVICE was never started (lifespan didn't run), so this isn't
        # necessarily 200 -- the point is it must not be rejected at 400 by
        # host validation before reaching the handler.
        self.assertNotEqual(response.status_code, 400)

    def test_health_ready_exposes_runtime_attestation(self):
        import chainseer_api

        health = {
            "watcher_last_error": None,
            "benchmark_capture": {"enabled": False},
            "timechain_integrity": {"status": "verified"},
            "cypher_tempre_runtime": {
                "status": "verified",
                "version": "3.30.08-tca.1",
                "expected_version": "3.30.08-tca.1",
                "commit": "abc123",
            },
            "maintenance_queue_depth": 0,
            "faculty_pack": {"status": "verified"},
            "memory": {"warning": False},
        }
        service = SimpleNamespace(
            ready=True,
            work=SimpleNamespace(qsize=lambda: 0),
            health_status=lambda: health,
        )
        with patch.object(chainseer_api, "SERVICE", service):
            response = self.client.get("/health/ready")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["cypher_tempre_runtime"],
            health["cypher_tempre_runtime"],
        )

    def test_other_routes_still_reject_untrusted_host_header(self):
        response = self.client.get("/", headers={"host": self.bad_host})
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json(), {"detail": "Invalid host header"})

    def test_other_routes_accept_trusted_host_header(self):
        response = self.client.get("/", headers={"host": "localhost"})
        # Still a 404 (no route registered at "/"), but crucially not a 400
        # -- proves the host check itself passed for a trusted host.
        self.assertEqual(response.status_code, 404)


class ServiceTests(unittest.TestCase):
    def wait_for_benchmark(self, service, job_id, timeout=3):
        deadline = time.time() + timeout
        job = service.get(job_id)
        while (
            job
            and (job.benchmark_capture or {}).get("status") == "queued"
        ):
            self.assertLess(time.time(), deadline)
            time.sleep(0.01)
            job = service.get(job_id)
        return job

    def settings(
        self,
        root,
        *,
        benchmark_root=None,
        benchmark_capture_enabled=False,
        watcher_enabled=False,
        watcher_interval_seconds=60,
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
            watcher_enabled=watcher_enabled,
            watcher_interval_seconds=watcher_interval_seconds,
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
                self.assertEqual(job.stage, "complete")
                self.assertEqual(job.progress_percent, 100)
                self.assertEqual(job.stage_detail, "Sealed analysis ready")
                self.assertEqual(job.public()["progress_percent"], 100)
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

    def test_health_status_does_not_wait_for_watcher_or_benchmark_locks(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            service._watcher_status = {
                "enabled": True,
                "last_cycle": {"robinhood": None, "base": None, "solana": None},
                "last_error": {"base": {"message": "temporary"}},
            }
            service._watch_lock.acquire()
            service._benchmark._lock.acquire()
            try:
                health = service.health_status()
            finally:
                service._benchmark._lock.release()
                service._watch_lock.release()
            self.assertEqual(
                health["watcher_last_error"],
                {"base": {"message": "temporary"}},
            )
            self.assertFalse(health["benchmark_capture"]["enabled"])

    def test_base_ring_import_uses_append_time_and_keeps_source_time(self):
        class FakeTimechain:
            def __init__(self):
                self.sealed = []

            def iter_rings(self):
                return iter(())

            def seal(self, ring_type, payload):
                self.sealed.append((ring_type, payload))
                return {"index": len(self.sealed)}

        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            tc = FakeTimechain()
            service._agent = SimpleNamespace(tc=tc)
            source_time = "2026-08-10T06:00:00+00:00"
            result = service.import_base_analysis_rings(
                [
                    {
                        "timestamp": source_time,
                        "payload": {"idempotency_key": "sample-1"},
                    }
                ]
            )
        self.assertEqual(result, [{"status": "sealed", "index": 1}])
        self.assertEqual(tc.sealed[0][0], "base_launch_analysis")
        self.assertEqual(tc.sealed[0][1]["timestamp"], source_time)
        self.assertEqual(tc.sealed[0][1]["source_timestamp"], source_time)

    def test_watcher_cycle_defers_all_networks_for_queued_analysis(self):
        class CountingWatcher:
            def __init__(self):
                self.calls = 0

            def run_once(self):
                self.calls += 1
                return {"calls": self.calls}

        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(
                self.settings(root, watcher_enabled=True)
            )
            watchers = [CountingWatcher() for _ in range(3)]
            (
                service._watcher,
                service._base_watcher,
                service._solana_watcher,
            ) = watchers
            service.work.put_nowait("queued-analysis")
            service._run_watcher_cycle()
            service.work.get_nowait()
        self.assertEqual([watcher.calls for watcher in watchers], [0, 0, 0])
        self.assertEqual(
            set(service._watcher_status["last_deferred"]),
            {"robinhood", "base", "solana"},
        )

    def test_full_audit_yields_at_ring_boundary_for_user_analysis(self):
        class Blockspace:
            @staticmethod
            def has(_blob_hash):
                return True

            @staticmethod
            def verify_blob(_blob_hash):
                return True

        class AuditTimechain:
            def __init__(self, root, rings):
                self.rings_path = Path(root) / "rings.jsonl"
                self.rings_path.write_text(
                    "".join(json.dumps(ring) + "\n" for ring in rings),
                    encoding="utf-8",
                )
                self.blockspace = Blockspace()
                self._tail = rings[-1]

            def tail_rings(self, _count):
                return [self._tail]

        class CognitiveLoop:
            def __init__(self):
                self.registry_checks = 0

            def verify_registry(self):
                self.registry_checks += 1

        with tempfile.TemporaryDirectory() as root:
            rings = []
            previous = "0" * 64
            for index in range(5):
                ring_hash = f"{index + 1:064x}"
                rings.append(
                    {
                        "index": index,
                        "prev_hash": previous,
                        "ring_hash": ring_hash,
                        "difficulty": 0,
                        "blockspace_refs": [],
                    }
                )
                previous = ring_hash
            service = AnalysisService(self.settings(root))
            cognitive_loop = CognitiveLoop()
            priority_injected = False

            def compute_ring_hash(ring):
                nonlocal priority_injected
                if not priority_injected:
                    priority_injected = True
                    service.work.put_nowait("priority-analysis")
                return ring["ring_hash"]

            # Production Chainseer exposes the hash module through its
            # cognitive loop rather than directly on the analyzer.
            cognitive_loop.timechain_module = SimpleNamespace(
                compute_ring_hash=compute_ring_hash
            )
            service._agent = SimpleNamespace(
                tc=AuditTimechain(root, rings),
                cognitive_loop=cognitive_loop,
            )
            service._watch_lock.acquire()
            try:
                completed = service._run_full_audit(batch_rings=5)
            finally:
                service._watch_lock.release()

            self.assertFalse(completed)
            self.assertEqual(service._full_audit_cursor.verified_rings, 1)
            self.assertEqual(
                service._integrity_status["full_audit_progress"]
                ["verified_rings"],
                1,
            )
            self.assertTrue(service._timechain_lock.acquire(blocking=False))
            service._timechain_lock.release()

            self.assertEqual(service.work.get_nowait(), "priority-analysis")
            while not service._run_full_audit(batch_rings=2):
                pass

            self.assertIsNone(service._full_audit_cursor)
            self.assertEqual(service._integrity_status["status"], "verified")
            self.assertEqual(cognitive_loop.registry_checks, 1)

            persisted = service._agent.tc.rings_path.read_text(
                encoding="utf-8"
            ).splitlines()
            damaged = [json.loads(line) for line in persisted]
            damaged[2]["prev_hash"] = "f" * 64
            service._agent.tc.rings_path.write_text(
                "".join(json.dumps(ring) + "\n" for ring in damaged),
                encoding="utf-8",
            )
            while not service._run_full_audit(batch_rings=2):
                pass

            self.assertEqual(service._integrity_status["status"], "failed")
            self.assertIn(
                "prev_hash broken",
                service._integrity_status["last_error"],
            )
            self.assertEqual(cognitive_loop.registry_checks, 1)

    def test_watcher_releases_lane_between_networks_for_new_analysis(self):
        class FirstWatcher:
            def __init__(self, service):
                self.service = service
                self.calls = 0

            def run_once(self, should_yield=None):
                self.calls += 1
                self.service.work.put_nowait("new-analysis")
                return {"calls": self.calls}

        class CountingWatcher:
            def __init__(self):
                self.calls = 0

            def run_once(self):
                self.calls += 1
                return {"calls": self.calls}

        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(
                self.settings(root, watcher_enabled=True)
            )
            first = FirstWatcher(service)
            second = CountingWatcher()
            third = CountingWatcher()
            service._watcher = first
            service._base_watcher = second
            service._solana_watcher = third
            service._run_watcher_cycle()
            service.work.get_nowait()
        self.assertEqual(first.calls, 1)
        self.assertEqual(second.calls, 0)
        self.assertEqual(third.calls, 0)
        self.assertEqual(
            service._watcher_status["last_deferred"]["base"]["reason"],
            "analysis_priority",
        )

    def test_api_watcher_cycle_disables_inline_calibration(self):
        class CalibrationAwareWatcher:
            def __init__(self):
                self.include_calibration = None

            def run_once(
                self, should_yield=None, *, include_calibration=True
            ):
                self.include_calibration = include_calibration
                return {"calibration": {"status": "deferred"}}

        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(
                self.settings(root, watcher_enabled=True)
            )
            watchers = [CalibrationAwareWatcher() for _ in range(3)]
            (
                service._watcher,
                service._base_watcher,
                service._solana_watcher,
            ) = watchers
            service._run_watcher_cycle()
        self.assertEqual(
            [watcher.include_calibration for watcher in watchers],
            [False, False, False],
        )

    def test_watcher_mutation_lane_disables_and_restores_autoindex(self):
        class LaneAwareWatcher:
            def __init__(self):
                self.autoindex_inside = None

            def run_once(
                self,
                should_yield=None,
                *,
                include_calibration=True,
                timechain_lane=None,
            ):
                with timechain_lane():
                    self.autoindex_inside = os.environ.get("CT_AUTOINDEX")
                return {"ok": True}

        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(
                self.settings(root, watcher_enabled=True)
            )
            watchers = [LaneAwareWatcher() for _ in range(3)]
            (
                service._watcher,
                service._base_watcher,
                service._solana_watcher,
            ) = watchers
            with patch.dict(os.environ, {"CT_AUTOINDEX": "1"}):
                service._run_watcher_cycle()
                self.assertEqual(os.environ["CT_AUTOINDEX"], "1")

        self.assertEqual(
            [watcher.autoindex_inside for watcher in watchers],
            ["0", "0", "0"],
        )

    def test_watcher_scheduler_uses_a_dedicated_thread(self):
        class IdleWatcher:
            def run_once(self, should_yield=None):
                return {"idle": True}

        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(
                self.settings(
                    root,
                    watcher_enabled=True,
                    watcher_interval_seconds=3600,
                )
            )
            service._agent = FakeAgent()
            service._watcher = IdleWatcher()
            service._base_watcher = IdleWatcher()
            service._solana_watcher = IdleWatcher()
            service.start()
            try:
                self.assertTrue(service._worker.is_alive())
                self.assertTrue(service._watcher_worker.is_alive())
                self.assertNotEqual(service._worker, service._watcher_worker)
                self.assertEqual(
                    service._watcher_worker.name,
                    "chainseer-watcher-worker",
                )
            finally:
                self.assertTrue(service.stop())

    def test_watch_reads_do_not_wait_for_long_running_cycle_lock(self):
        subscriber = "a" * 64
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            service._agent = FakeAgent()
            service.start()
            try:
                service.watch_subscribe(TOKEN, "robinhood", subscriber)
                service._watch_lock.acquire()
                started = time.monotonic()
                try:
                    status = service.watch_status(subscriber)
                    alerts = service.watch_alerts(
                        TOKEN, "robinhood", subscriber
                    )
                finally:
                    service._watch_lock.release()
                self.assertLess(time.monotonic() - started, 1.0)
                self.assertEqual(len(status["subscriptions"]), 1)
                self.assertEqual(alerts, [])
            finally:
                service.stop()

    def test_slow_benchmark_capture_does_not_delay_sealed_report(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(
                self.settings(root, benchmark_capture_enabled=True)
            )
            service._agent = FakeAgent()
            capture_started = threading.Event()
            release_capture = threading.Event()

            def slow_capture(job, public_report):
                capture_started.set()
                release_capture.wait(timeout=2)
                return {"status": "captured", "case_id": "test-case"}

            service._benchmark.capture = slow_capture
            service.start()
            try:
                accepted = service.submit(TOKEN)
                deadline = time.time() + 2
                job = service.get(accepted.job_id)
                while job and job.status not in {"succeeded", "failed"}:
                    self.assertLess(time.time(), deadline)
                    time.sleep(0.01)
                    job = service.get(accepted.job_id)
                self.assertTrue(capture_started.wait(timeout=1))
                self.assertEqual(job.status, "succeeded")
                self.assertIsNotNone(job.result)
                self.assertEqual(job.benchmark_capture, {"status": "queued"})
                release_capture.set()
                job = self.wait_for_benchmark(service, accepted.job_id)
                self.assertEqual(job.benchmark_capture["status"], "captured")
            finally:
                release_capture.set()
                service.stop()

    def test_watch_mutation_fails_fast_while_cycle_owns_lock(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            service._agent = FakeAgent()
            service.start()
            try:
                service._watch_lock.acquire()
                started = time.monotonic()
                try:
                    with self.assertRaises(WatcherBusyError):
                        service.watch_subscribe(TOKEN)
                finally:
                    service._watch_lock.release()
                self.assertLess(time.monotonic() - started, 1.0)
            finally:
                service.stop()

    def test_memory_facade_delegates_without_exposing_execution(self):
        class FakeMemory:
            def query(self, network, address, *, topics, limit):
                return {
                    "subject": {"network": network, "address": address},
                    "topics": topics,
                    "limit": limit,
                    "execution": False,
                }

            def status(self):
                return {"status": "healthy"}

            def citation(self, ring):
                return {"ring": ring, "payload": None}

        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(self.settings(root))
            service._memory = FakeMemory()
            result = service.memory_query(
                "robinhood",
                TOKEN,
                topics=["latest_assessment"],
                limit=1,
            )
            self.assertFalse(result["execution"])
            self.assertEqual(service.memory_status()["status"], "healthy")
            self.assertEqual(service.memory_citation(7)["ring"], 7)

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
                job = self.wait_for_benchmark(service, accepted.job_id)
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
                # The aggregate summary reload is throttled (see
                # BENCHMARK_SUMMARY_REFRESH_MIN_INTERVAL_SECONDS) and won't
                # reflect this capture yet on its own -- durability on disk
                # (asserted above) is the ground truth. force=True proves the
                # summary is eventually correct once actually recomputed.
                service._benchmark._refresh_summary(force=True)
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
                # A cache hit now returns the original completed job
                # (job_id included) rather than minting a duplicate Job
                # entry per repeat lookup, so its benchmark_capture reflects
                # that job's real, one-time capture rather than a synthetic
                # per-hit status. The invariant that matters -- no second
                # observation gets written -- is the assertion below.
                self.assertEqual(cached.job_id, accepted.job_id)
                self.assertEqual(
                    cached_job.benchmark_capture["status"],
                    "captured",
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

    def test_benchmark_summary_refresh_is_throttled_across_captures(self):
        # append_observation()/case_bank_status() both reload and re-validate
        # the entire historical observation ledger from disk -- doing that
        # unconditionally after every single capture makes each analysis
        # progressively more expensive as the ledger grows. This proves the
        # throttle actually skips the recompute on a rapid second capture,
        # and that force=True still reaches ground truth on demand.
        second_token = "0x" + "b" * 40
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
            service._agent = FakeAgent()
            service.start()
            try:
                for token in (TOKEN, second_token):
                    accepted = service.submit(token)
                    deadline = time.time() + 3
                    job = service.get(accepted.job_id)
                    while job and job.status not in {"succeeded", "failed"}:
                        self.assertLess(time.time(), deadline)
                        time.sleep(0.01)
                        job = service.get(accepted.job_id)
                    job = self.wait_for_benchmark(
                        service, accepted.job_id
                    )
                    self.assertEqual(job.status, "succeeded")

                # Both captures landed on disk regardless of the throttle.
                self.assertEqual(
                    len(
                        load_jsonl(
                            Path(benchmark_root) / "observations-v1.jsonl"
                        )
                    ),
                    2,
                )
                # Neither capture's summary refresh fired within the
                # throttle window (the first was skipped because
                # _initialize()'s startup refresh had just run; the second
                # was skipped because the first capture attempted -- and
                # skipped -- its own refresh moments earlier).
                self.assertEqual(
                    service.benchmark_status()["observations"],
                    0,
                )
                service._benchmark._refresh_summary(force=True)
                self.assertEqual(
                    service.benchmark_status()["observations"],
                    2,
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
                job = self.wait_for_benchmark(service, accepted.job_id)
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


class EnvFloatTests(unittest.TestCase):
    """_env_float parses and bounds-checks; nothing else covered it."""

    def test_returns_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CS_PROBE_FLOAT", None)
            self.assertEqual(_env_float("CS_PROBE_FLOAT", 2.5, 0.0, 10.0), 2.5)

    def test_parses_configured_value(self):
        with patch.dict(os.environ, {"CS_PROBE_FLOAT": "7.25"}, clear=False):
            self.assertEqual(_env_float("CS_PROBE_FLOAT", 2.5, 0.0, 10.0), 7.25)

    def test_rejects_non_numeric(self):
        with patch.dict(os.environ, {"CS_PROBE_FLOAT": "soon"}, clear=False):
            with self.assertRaises(RuntimeError) as ctx:
                _env_float("CS_PROBE_FLOAT", 2.5, 0.0, 10.0)
            self.assertIn("must be a number", str(ctx.exception))

    def test_rejects_out_of_range_both_ends(self):
        for raw in ("-0.5", "11"):
            with patch.dict(os.environ, {"CS_PROBE_FLOAT": raw}, clear=False):
                with self.assertRaises(RuntimeError) as ctx:
                    _env_float("CS_PROBE_FLOAT", 2.5, 0.0, 10.0)
                self.assertIn("must be between", str(ctx.exception))

    def test_boundaries_are_inclusive(self):
        # A timeout configured at exactly its documented limit must be
        # accepted; an off-by-one here silently refuses a legal setting.
        for raw, expected in (("0.0", 0.0), ("10.0", 10.0)):
            with patch.dict(os.environ, {"CS_PROBE_FLOAT": raw}, clear=False):
                self.assertEqual(
                    _env_float("CS_PROBE_FLOAT", 2.5, 0.0, 10.0), expected
                )


class _FakeTimechain:
    """Minimal tail_rings source for CAS tests."""

    def __init__(self, rings=None):
        self._rings = list(rings or [])

    def tail_rings(self, k):
        return self._rings[-k:]


def _ring(ring_type, *, network=None, token=None, block=None, key=None):
    payload = {}
    if network is not None:
        payload["network"] = network
    if token is not None:
        payload["token_address"] = token
    if block is not None:
        payload["block_pin"] = block
    if key is not None:
        payload["idempotency_key"] = key
    return {"ring_type": ring_type, "payload": payload}


def _job(**overrides):
    base = dict(
        network="robinhood",
        token_address="0xabc",
        pinned_snapshot={"analysis_ring": 12, "analysis_ring_hash": "aa" * 32},
        block_or_slot=100,
        evidence_hash="e" * 64,
        report_hash="r" * 64,
        analyzer_version="test",
        idempotency_key="robinhood:0xabc:100",
        prepared_head=11,
        enqueued_at=1.0,
    )
    base.update(overrides)
    return DeferredSealJob(**base)


class DeferredSealCoalescingTests(unittest.TestCase):
    """Draining must keep exactly one job per subject -- the newest.

    Coalescing is what stops rapid watcher rescans queueing redundant seals.
    If it kept the OLDEST, a burst would seal a stale snapshot and discard the
    fresh one, which looks identical from outside: one ring per token either
    way.
    """

    def _service(self, root):
        return AnalysisService(
            Settings(
                environment="test",
                api_token="",
                chain_root=root,
                queue_size=4,
                result_ttl_seconds=3600,
                cache_ttl_seconds=300,
                rate_limit_per_minute=6,
                shutdown_grace_seconds=10,
                watcher_enabled=False,
            )
        )

    def test_keeps_only_the_newest_job_per_subject(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            executed = []
            service._execute_deferred_seal = executed.append
            old = _job(block_or_slot=100, enqueued_at=1.0)
            new = _job(block_or_slot=200, enqueued_at=2.0)
            service._deferred_seal_work.put(old)
            service._deferred_seal_work.put(new)

            service._drain_deferred_seals()

            self.assertEqual(len(executed), 1, "coalescing kept both jobs")
            self.assertEqual(
                executed[0].block_or_slot, 200,
                "kept the stale snapshot and discarded the fresh one",
            )

    def test_distinct_subjects_are_not_coalesced(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            executed = []
            service._execute_deferred_seal = executed.append
            service._deferred_seal_work.put(_job(token_address="0xaaa"))
            service._deferred_seal_work.put(_job(token_address="0xbbb"))
            service._deferred_seal_work.put(
                _job(network="solana", token_address="0xaaa")
            )

            service._drain_deferred_seals()

            self.assertEqual(len(executed), 3, "distinct subjects collapsed")

    def test_empty_queue_is_a_noop(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            called = []
            service._execute_deferred_seal = called.append
            service._drain_deferred_seals()
            self.assertEqual(called, [])

    def test_one_failing_job_does_not_abort_the_batch(self):
        """A single bad subject must not strand every other pending seal."""
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            seen = []

            def flaky(job):
                seen.append(job.token_address)
                if job.token_address == "0xbad":
                    raise RuntimeError("seal exploded")

            service._execute_deferred_seal = flaky
            service._deferred_seal_work.put(_job(token_address="0xbad"))
            service._deferred_seal_work.put(_job(token_address="0xgood"))

            service._drain_deferred_seals()

            self.assertIn("0xgood", seen, "a failing job aborted the batch")


class CasValidateDeferredSealTests(unittest.TestCase):
    """The CAS verdict decides whether a deferred seal counts.

    Each branch has a distinct consequence and none were exercised: an
    idempotent miss double-seals, a missed supersede commits a stale
    snapshot over a newer one, and an over-eager supersede silently drops
    fresh analyses.
    """

    def _service(self, root):
        return AnalysisService(
            Settings(
                environment="test",
                api_token="",
                chain_root=root,
                queue_size=4,
                result_ttl_seconds=3600,
                cache_ttl_seconds=300,
                rate_limit_per_minute=6,
                shutdown_grace_seconds=10,
                watcher_enabled=False,
            )
        )

    def test_matching_idempotency_key_is_idempotent(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            tc = _FakeTimechain([
                _ring("token_analysis", network="robinhood",
                      token="0xabc", block=100, key="robinhood:0xabc:100")
            ])
            self.assertEqual(
                service._cas_validate_deferred_seal(_job(), tc), "idempotent"
            )

    def test_newer_sealed_block_supersedes(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            tc = _FakeTimechain([
                _ring("token_analysis", network="robinhood",
                      token="0xabc", block=250)
            ])
            self.assertEqual(
                service._cas_validate_deferred_seal(
                    _job(block_or_slot=100), tc
                ),
                "superseded",
            )

    def test_equal_block_supersedes(self):
        """>= not >: re-sealing the same block adds nothing."""
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            tc = _FakeTimechain([
                _ring("token_analysis", network="robinhood",
                      token="0xabc", block=100)
            ])
            self.assertEqual(
                service._cas_validate_deferred_seal(
                    _job(block_or_slot=100), tc
                ),
                "superseded",
            )

    def test_other_tokens_advancing_does_not_supersede(self):
        """Head movement on unrelated subjects must not invalidate a job.

        This is the whole point of a subject-scoped check rather than a
        head-index one: on a busy chain any other token's seal would
        otherwise cancel a pending analysis.
        """
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            tc = _FakeTimechain([
                _ring("token_analysis", network="robinhood",
                      token="0xOTHER", block=9999),
                _ring("token_analysis", network="solana",
                      token="0xabc", block=9999),
            ])
            self.assertEqual(
                service._cas_validate_deferred_seal(_job(), tc), "committed"
            )

    def test_missing_analysis_ring_is_discarded(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            tc = _FakeTimechain([])
            self.assertEqual(
                service._cas_validate_deferred_seal(
                    _job(pinned_snapshot={"analysis_ring": None}), tc
                ),
                "discarded",
            )

    def test_empty_chain_commits(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            self.assertEqual(
                service._cas_validate_deferred_seal(_job(), _FakeTimechain([])),
                "committed",
            )

    def test_unparseable_sealed_block_does_not_supersede(self):
        """A corrupt block_pin must not silently cancel a valid seal."""
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            tc = _FakeTimechain([
                _ring("token_analysis", network="robinhood",
                      token="0xabc", block="not-a-number")
            ])
            self.assertEqual(
                service._cas_validate_deferred_seal(_job(), tc), "committed"
            )


class PreparedWatcherCommitTests(unittest.TestCase):
    def test_unrelated_head_advancement_allows_exactly_one_minimal_append(self):
        with tempfile.TemporaryDirectory() as root:
            service = AnalysisService(Settings(
                environment="test", api_token="", chain_root=root,
                queue_size=4, result_ttl_seconds=3600,
                cache_ttl_seconds=300, rate_limit_per_minute=6,
                shutdown_grace_seconds=10, watcher_enabled=False,
            ))

            class TC:
                def __init__(self):
                    self.seals = []

                def tail_rings(self, _limit):
                    return [{
                        "ring_type": "token_analysis",
                        "payload": {
                            "network": "base",
                            "token_address": "0xother",
                        },
                    }]

                def seal(self, ring_type, payload, poq=None):
                    self.seals.append((ring_type, payload, poq))
                    return {"index": 77}

            tc = TC()
            loop = SimpleNamespace(
                verify_incremental=lambda: (True, []),
                establish_trust=lambda: None,
            )
            service._agent = SimpleNamespace(tc=tc, cognitive_loop=loop)
            service._current_commit_dependencies = lambda: ("policy", "registry")
            generation = service._deferred_queue.enqueue(
                kind="watcher_commit", subject_key=f"base:{TOKEN}",
                priority=20,
                payload={"anchor_value": 100, "observed_at_epoch": 1.0},
                now=1.0,
            )
            claimed = service._deferred_queue.claim(now=2.0)
            self.assertEqual(claimed.generation, generation)
            prepared = PreparedWatcherCommit(
                queue_item=claimed,
                payload={
                    "network": "base", "token_address": TOKEN,
                    "anchor_value": 100, "observed_at_epoch": 1.0,
                    "idempotency_key": f"base:{TOKEN}:100",
                },
                poq_scores={"coherence": 230},
                policy_hash="policy", registry_hash="registry",
            )

            result = service._commit_prepared_watcher(prepared)

            self.assertEqual(result, "committed:77")
            self.assertEqual(len(tc.seals), 1)
            self.assertEqual(tc.seals[0][0], "watcher_analysis")


class HybridCognitiveCompletionTests(unittest.TestCase):
    def _service(self, root):
        return AnalysisService(Settings(
            environment="test", api_token="", chain_root=root,
            queue_size=4, result_ttl_seconds=3600,
            cache_ttl_seconds=300, rate_limit_per_minute=6,
            shutdown_grace_seconds=10, watcher_enabled=False,
        ))

    def _install_agent(self, service, *, analysis_hash="analysis-hash"):
        class TC:
            def __init__(self):
                self.rings = [{
                    "index": 7,
                    "ring_type": "token_analysis",
                    "ring_hash": analysis_hash,
                    "payload": {"token_address": TOKEN},
                }]

            def tail_rings(self, limit):
                return self.rings[-limit:]

            def iter_rings(self):
                yield from self.rings

        tc = TC()

        class Loop:
            def verify_incremental(self):
                return True, []

            def finalize_deferred(self, report, ring):
                cognition = report["cognition"]
                cognition["status"] = "complete"
                cognition["analysis_ring"] = ring["index"]
                tc.rings.append({
                    "index": 8,
                    "ring_type": "cognitive_completion",
                    "ring_hash": "completion-hash",
                    "payload": {
                        "analysis_ring": ring["index"],
                        "analysis_ring_hash": ring["ring_hash"],
                        "cognitive_loop": cognition,
                    },
                })

            def establish_trust(self):
                return None

        service._agent = SimpleNamespace(tc=tc, cognitive_loop=Loop())
        return tc

    def _enqueue(self, service):
        job = Job(id="job-1", address=TOKEN, status="succeeded")
        job.result = {"timechain": {"ring": 7, "ring_hash": "analysis-hash"}}
        service.jobs[job.id] = job
        report = {
            "token_address": TOKEN,
            "chain_id": 4663,
            "analysis_ring": 7,
            "analysis_ring_hash": "analysis-hash",
            "cognition": {"status": "pending", "growth": []},
            "_cognitive_input": "trusted structured facts",
        }
        service._enqueue_cognitive_completion(job, report)
        return job

    def test_result_is_published_before_idle_completion_and_then_updated(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            tc = self._install_agent(service)
            job = self._enqueue(service)
            self.assertEqual(job.cognition_status, "queued")
            self.assertEqual(len(tc.rings), 1)

            service._analysis_active.set()
            service._drain_durable_commits()
            self.assertEqual(len(tc.rings), 1)
            self.assertEqual(job.cognition_status, "queued")

            service._analysis_active.clear()
            service._drain_durable_commits()
            self.assertEqual(len(tc.rings), 2)
            self.assertEqual(job.cognition_status, "complete")
            self.assertEqual(job.cognition_progress_percent, 100)
            self.assertEqual(job.result["timechain"]["cognitive_ring"], 8)
            self.assertEqual(
                service._deferred_queue.counts()["done"], 1
            )

    def test_analysis_hash_collision_discards_completion(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            self._install_agent(service, analysis_hash="different-hash")
            job = self._enqueue(service)

            service._drain_durable_commits()

            self.assertEqual(job.cognition_status, "failed")
            self.assertEqual(
                service._deferred_queue.counts()["discarded"], 1
            )


class TrackedTimechainLockTests(unittest.TestCase):
    """The lock's timeout path carries the diagnostics it promises.

    A deadlock here stalls sealing estate-wide, and the error message is the
    only evidence of who held it -- so the message content is the feature.
    """

    def _service(self, root, timeout=0.05):
        settings = Settings(
            environment="test",
            api_token="",
            chain_root=root,
            queue_size=4,
            result_ttl_seconds=3600,
            cache_ttl_seconds=300,
            rate_limit_per_minute=6,
            shutdown_grace_seconds=10,
            watcher_enabled=False,
        )
        object.__setattr__(
            settings, "timechain_lock_timeout_seconds", timeout
        ) if hasattr(settings, "timechain_lock_timeout_seconds") else None
        return AnalysisService(settings)

    def test_timeout_names_the_holder_and_reason(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            service.settings = service.settings.model_copy(
                update={"timechain_lock_timeout_seconds": 0.05}
            ) if hasattr(service.settings, "model_copy") else service.settings
            holding = threading.Event()
            release = threading.Event()

            def holder():
                with service._tracked_timechain_lock("holder-work"):
                    holding.set()
                    release.wait(2)

            t = threading.Thread(target=holder, daemon=True)
            t.start()
            self.assertTrue(holding.wait(2), "holder never acquired")
            try:
                with self.assertRaises(TimeoutError) as ctx:
                    with service._tracked_timechain_lock("second-work"):
                        pass
                message = str(ctx.exception)
                self.assertIn("second-work", message)
                self.assertIn("holder-work", message,
                              "timeout does not say who held the lock")
            finally:
                release.set()
                t.join(2)

    def test_lock_is_released_when_the_body_raises(self):
        """A leaked lock would wedge every later seal, not just this one."""
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            with self.assertRaises(ValueError):
                with service._tracked_timechain_lock("boom"):
                    raise ValueError("body failed")
            acquired = service._timechain_lock.acquire(timeout=1)
            self.assertTrue(acquired, "lock leaked after an exception")
            if acquired:
                service._timechain_lock.release()
            self.assertIsNone(
                service._timechain_owner,
                "owner tracking not cleared after an exception",
            )

    def test_worker_survives_timeout_and_retries_same_active_job(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            service._agent = FakeAgent()
            attempts = {"count": 0}

            class LockAttempt:
                def __enter__(self):
                    attempts["count"] += 1
                    if attempts["count"] == 1:
                        raise TimeoutError("synthetic contention")

                def __exit__(self, *_args):
                    return False

            service._tracked_timechain_lock = lambda _reason: LockAttempt()
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
                self.assertEqual(job.lock_retry_count, 1)
                self.assertTrue(service._worker.is_alive())
                self.assertNotIn(f"robinhood:{TOKEN.lower()}", service.active_by_address)
            finally:
                service.stop()

    def test_persisted_result_is_returned_while_repeat_scan_refreshes(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            service._deferred_queue.put_public_result(
                "base",
                TOKEN.lower(),
                {"timechain": {"ring": 44}, "evidence": {"block_pin": 123}},
                now=time.time() - 30,
            )

            accepted = service.submit(TOKEN, "base")

            self.assertTrue(accepted.refreshing)
            self.assertFalse(accepted.cached)
            self.assertEqual(accepted.previous_result["timechain"]["ring"], 44)
            self.assertGreaterEqual(accepted.previous_result_age_seconds, 29)
            self.assertEqual(service.get(accepted.job_id).status, "queued")

    def test_force_refresh_bypasses_hot_cache_and_reuses_active_job(self):
        with tempfile.TemporaryDirectory() as root:
            service = self._service(root)
            old = Job(id="a" * 32, address=TOKEN, status="succeeded")
            old.result = {"timechain": {"ring": 9}}
            service.jobs[old.id] = old
            service.cache[f"robinhood:{TOKEN.lower()}"] = (
                time.time() + 300,
                old.id,
            )
            service._deferred_queue.put_public_result(
                "robinhood", TOKEN.lower(), old.result
            )

            forced = service.submit(TOKEN, force_refresh=True)
            duplicate = service.submit(TOKEN, force_refresh=True)

            self.assertNotEqual(forced.job_id, old.id)
            self.assertTrue(forced.refreshing)
            self.assertEqual(duplicate.job_id, forced.job_id)
            self.assertEqual(duplicate.previous_result["timechain"]["ring"], 9)


if __name__ == "__main__":
    unittest.main()
