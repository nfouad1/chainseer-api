import json
import inspect
import os
import sys
import tempfile
import types
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

# The desktop test runtime does not bundle requests. Provide only the import
# surface needed by the mocked tests; production still requires real requests.
# Try the real module first: a "requests" not in sys.modules check alone would
# install this stub whenever this file happens to import before anything else
# does, permanently shadowing the real library in sys.modules for every other
# test file in the same pytest session (breaking anything that needs
# requests.post or requests.exceptions.HTTPError, neither of which this stub
# defines).
try:
    import requests as _real_requests  # noqa: F401
except ImportError:
    requests_stub = types.ModuleType("requests")
    requests_stub.Session = lambda: types.SimpleNamespace(headers={})
    requests_stub.get = lambda *args, **kwargs: None
    requests_stub.exceptions = types.SimpleNamespace(
        ConnectionError=ConnectionError,
        Timeout=TimeoutError,
    )
    sys.modules["requests"] = requests_stub

import chainseer


class FakeResponse:
    def __init__(self, data):
        self._data = data

    def raise_for_status(self):
        return None

    def json(self):
        return self._data


class FakeSession:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def post(self, url, json=None, timeout=None):
        self.calls.append((url, json, timeout))
        return FakeResponse(self.data)


class FakeTimechain:
    def __init__(self, rings):
        self._rings = rings

    def load(self):
        return self._rings


class FakePoQ:
    def __init__(self):
        self.kwargs = None

    def gate_and_seal(self, tc, candidate, **kwargs):
        self.kwargs = kwargs
        return {"decision": "SEAL"}, {"index": 10, "ring_hash": "abc"}


class FailOnLPTokenRPC:
    def erc20_total_supply(self, _pair):
        raise AssertionError(
            "position-based pools must not be probed as ERC-20 LP tokens"
        )


class FakeV2LiquidityRPC:
    def erc20_total_supply(self, _pair):
        return 1_000

    def erc20_balance_of(self, _pair, _holder):
        return 0

    def call(self, _address, _data):
        raise chainseer.RPCError("selector unavailable", -1)


class OwnershipCheckFallbackTests(unittest.TestCase):
    """_analyze_contract() must not conflate an RPC transport failure
    (timeout, connection drop, non-2xx) with a genuine on-chain revert --
    only the latter is real evidence "no owner() function exists"."""

    @staticmethod
    def _agent(rpc):
        # _analyze_contract only touches self.rpc, so a bare namespace
        # standing in for `self` is enough to call it unbound.
        return types.SimpleNamespace(rpc=rpc)

    def test_transport_failure_is_reported_as_unknown_not_renounced(self):
        class TimingOutRPC:
            def get_code(self, _token):
                return "0x6080"

            def erc20_owner(self, _token):
                raise chainseer.RPCError(
                    "RPC request timed out after 30s", -2
                )

        result = chainseer.Chainseer._analyze_contract(
            self._agent(TimingOutRPC()), "0x" + "1" * 40
        )
        self.assertTrue(result["owner_check_failed"])
        self.assertFalse(result["ownership_renounced"])
        self.assertIsNone(result["owner"])

    def test_http_failure_is_also_reported_as_unknown(self):
        class RateLimitedRPC:
            def get_code(self, _token):
                return "0x6080"

            def erc20_owner(self, _token):
                raise chainseer.RPCError(
                    "RPC HTTP response failed (429)", -429
                )

        result = chainseer.Chainseer._analyze_contract(
            self._agent(RateLimitedRPC()), "0x" + "1" * 40
        )
        self.assertTrue(result["owner_check_failed"])
        self.assertFalse(result["ownership_renounced"])

    def test_genuine_revert_is_still_treated_as_renounced(self):
        class RevertingRPC:
            def get_code(self, _token):
                return "0x6080"

            def erc20_owner(self, _token):
                raise chainseer.RPCError("execution reverted", -32000)

        result = chainseer.Chainseer._analyze_contract(
            self._agent(RevertingRPC()), "0x" + "1" * 40
        )
        self.assertFalse(result["owner_check_failed"])
        self.assertTrue(result["ownership_renounced"])
        self.assertEqual(result["owner"], "0x" + "0" * 40)


class ChainseerInfrastructureTests(unittest.TestCase):
    def test_version_dead_code_and_verification_scope_are_honest(self):
        source = Path(chainseer.__file__).read_text(encoding="utf-8")
        http_source = inspect.getsource(chainseer._http_get_json)
        provenance_doc = " ".join(
            inspect.getdoc(chainseer.ProvenanceLedger).split()
        )

        self.assertEqual(chainseer.CHAINSEER_VERSION, "7.1")
        self.assertNotIn('CHAINSEER_VERSION = "7.0"', source)
        self.assertEqual(source.count('"7.1"'), 1)
        self.assertNotIn("return int(val)", http_source)
        self.assertNotIn("independently verifiable end-to-end", source)
        self.assertNotIn("byte-for-byte identical provenance", source)
        self.assertIn("tamper-evident and internally consistent", source)
        self.assertIn(
            "does not re-execute external RPC or HTTP queries",
            provenance_doc,
        )

    def test_cognitive_input_receives_only_bounded_entity_graph_fields(self):
        safe = json.loads(
            chainseer.ChainseerCognitiveLoop._safe_input(
                {
                    "token_address": "0x" + "1" * 40,
                    "chain_id": 4663,
                    "analysis": {"risk_level": "High"},
                    "provenance": {"block_pin": 100, "fact_count": 2},
                    "data": {
                        "entity_graph": {
                            "graph_hash": "a" * 64,
                            "summary": {
                                "insider_risk_level": "High",
                                "coverage": "measured",
                            },
                            "signals": [
                                {
                                    "code": "direct_liquidity_control",
                                    "reason": "provider text must stay excluded",
                                }
                            ],
                            "nodes": [
                                {
                                    "address": "0x" + "b" * 40,
                                    "label": "provider-controlled label",
                                }
                            ],
                        }
                    },
                }
            )
        )
        self.assertEqual(safe["entity_graph_hash"], "a" * 64)
        self.assertEqual(safe["insider_risk_level"], "High")
        self.assertEqual(
            safe["entity_signal_codes"], ["direct_liquidity_control"]
        )
        self.assertNotIn("provider text", str(safe))
        self.assertNotIn("provider-controlled label", str(safe))

    def test_cognitive_input_exposes_bounded_production_risk_states(self):
        safe = json.loads(
            chainseer.ChainseerCognitiveLoop._safe_input(
                {
                    "token_address": "0x" + "1" * 40,
                    "chain_id": 8453,
                    "analysis": {
                        "risk_level": "High",
                        "holder_assessment": {
                            "holder_count": 420,
                            "largest_non_amm_holder_pct": 12.5,
                            "concentration_source": "total_supply",
                        },
                        "uncertain_components": {
                            "honeypot_safety": "provider unavailable"
                        },
                    },
                    "data": {
                        "basic_info": {},
                        "goplus_security": {
                            "is_proxy": "1",
                            "cannot_sell_all": "0",
                            "is_mintable": "1",
                        },
                        "source_code": {
                            "proxy_type": "eip1967",
                            "implementation_verified": False,
                        },
                        "lp_lock": {"state": "custody_unverified"},
                    },
                }
            )
        )
        self.assertEqual(safe["holder_count"], 420)
        self.assertEqual(safe["largest_non_amm_holder_pct"], 12.5)
        self.assertEqual(safe["liquidity_custody_state"], "custody_unverified")
        self.assertTrue(safe["mint_authority_active"])
        self.assertTrue(safe["is_proxy"])
        self.assertFalse(safe["implementation_verified"])
        self.assertFalse(safe["cannot_sell_all"])

    @staticmethod
    def investor_fixture():
        return {
            "goplus_security": {},
            "dexscreener": {"pairs": [{}]},
            "basic_info": {
                "name": "VANTIS by Virtuals", "symbol": "VANTIS",
                "total_supply": 1_000_000_000,
            },
            "contract_audit": {
                "ownership_renounced": False, "has_mint_function": False,
                "dangerous_functions": [],
            },
            "dex_pairs": {
                "total_liquidity_usd": 143_129.72,
                "primary_liquidity_usd": 143_129.72,
                "total_volume_24h": 118_189.64,
                "primary_price_usd": 0.0008903,
                "market_cap": 890_353,
                "token_age_days": 2,
                "token_age_label": "2 days",
                "pair_count": 1,
                "primary_amm_version": "v2",
                "txns_24h": {"buys": 120, "sells": 97},
                "on_chain_reserves": None,
            },
            "transfer_activity": {},
            "lp_lock": {
                "state": "creator_withdrawable",
                "locked": False,
                "withdrawal_verified": True,
                "hard_stop_eligible": True,
                "withdrawable_pct": 82.0,
                "amm_version": "v2",
                "method": (
                    "Token creator directly controls 82.0% of V2 LP supply"
                ),
            },
            "tax_estimate": {"available": False},
            "deployer": {},
            "blockscout_holders": {
                "blockscout_available": True,
                "adj_top_1_pct": 82.5,
                "concentration_complete": True,
                "holder_count": 20,
            },
            "wash_trading": {"available": True, "wash_score": 0, "wash_risk": "Low"},
            "blockscout_address": {},
            "blockscout_token": {"holders_count": 20},
            "source_code": {"available": False, "is_verified": False},
            "trend": {"available": False},
        }

    def test_agent_initializes_with_real_timechain_modules(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(chainseer.RobinhoodRPC, "get_block_number", return_value=100):
                agent = chainseer.Chainseer(chain_root=temp_dir)
            ok, _ = agent.tc.verify()
            self.assertTrue(ok)
            self.assertTrue(callable(agent.poq_module.gate_and_seal))

    def test_reviewed_faculty_pack_import_is_verified_and_idempotent(self):
        pack_path = (
            Path(chainseer.__file__).resolve().parent
            / "faculties"
            / "chainseer-production-v1.json"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                patch.dict(
                    os.environ,
                    {"CHAINSEER_FACULTY_PACK_PATH": str(pack_path)},
                ),
                patch.object(
                    chainseer.RobinhoodRPC,
                    "get_block_number",
                    return_value=100,
                ),
            ):
                first = chainseer.Chainseer(chain_root=temp_dir)
                second = chainseer.Chainseer(chain_root=temp_dir)

            self.assertEqual(first.faculty_pack_status["status"], "installed")
            self.assertEqual(second.faculty_pack_status["status"], "verified")
            self.assertEqual(first.faculty_pack_status["faculty_count"], 12)
            rings = second.tc.load()
            imports = [r for r in rings if r["ring_type"] == "faculty-import"]
            self.assertEqual(len(imports), 1)
            grown = json.loads(
                (Path(temp_dir) / "registry" / "grown.json").read_text(
                    encoding="utf-8"
                )
            )
            names = {
                item["name"]
                for key in ("senses", "modalities")
                for item in grown.get(key, [])
            }
            self.assertIn("Liquidity-Custody State Sensing", names)
            self.assertIn("Token-vs-Infrastructure Evidence Separation", names)
            labels = second.cognitive_loop.recall.label(
                json.dumps(
                    {
                        "liquidity_custody_state": "custody_unverified",
                        "largest_non_amm_holder_pct": 42.0,
                        "concentration_basis": "total_supply",
                        "is_proxy": True,
                        "implementation_verified": False,
                        "cannot_sell_all": False,
                        "uncertain_components": ["honeypot_safety"],
                    },
                    sort_keys=True,
                )
            )
            active = {
                item["name"]
                for key in ("senses", "modalities")
                for item in labels.get(key, [])
            }
            self.assertTrue(active & names)
            ok, _ = second.tc.verify()
            self.assertTrue(ok)

    def test_faculty_pack_refuses_to_mutate_registry_on_corrupt_chain(self):
        pack_path = (
            Path(chainseer.__file__).resolve().parent
            / "faculties"
            / "chainseer-production-v1.json"
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            with (
                patch.dict(
                    os.environ,
                    {"CHAINSEER_FACULTY_PACK_PATH": ""},
                ),
                patch.object(
                    chainseer.RobinhoodRPC,
                    "get_block_number",
                    return_value=100,
                ),
            ):
                chainseer.Chainseer(chain_root=temp_dir)

            registry_dir = Path(temp_dir) / "registry"
            registry_before = {
                path.name: path.read_bytes()
                for path in registry_dir.glob("*.json")
            }
            rings_path = Path(temp_dir) / "chain" / "rings.jsonl"
            with rings_path.open("ab") as handle:
                handle.write(b"\x00")

            with (
                patch.dict(
                    os.environ,
                    {"CHAINSEER_FACULTY_PACK_PATH": str(pack_path)},
                ),
                patch.object(
                    chainseer.RobinhoodRPC,
                    "get_block_number",
                    return_value=100,
                ),
                self.assertRaisesRegex(
                    RuntimeError,
                    "before faculty initialization",
                ),
            ):
                chainseer.Chainseer(chain_root=temp_dir)

            registry_after = {
                path.name: path.read_bytes()
                for path in registry_dir.glob("*.json")
            }
            self.assertEqual(registry_after, registry_before)
            self.assertNotIn("grown.json", registry_after)

    def test_report_seals_through_poq_with_provenance(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(chainseer.RobinhoodRPC, "get_block_number", return_value=100):
                agent = chainseer.Chainseer(chain_root=temp_dir)
            report = {
                "token_address": "0x" + "3" * 40,
                "token_name": "Test",
                "timestamp": "2026-07-21T00:00:00+00:00",
                "analysis": {
                    "legitimacy_score": 55,
                    "risk_level": "High",
                    "recommendation": "Treat as high risk.",
                    "green_flags": [],
                    "red_flags": ["Synthetic test risk"],
                    "component_scores": {"security": 40},
                    "confidence": "test",
                    "uncertain_components": {"market": "synthetic fixture"},
                },
                "provenance": {"block_pin": 100, "fact_count": 1, "facts": []},
                "claim_evidence": {"contract": ["F0000"]},
                "poq_scores": {
                    "coherence": 230, "relevance": 240, "novelty": 210,
                    "consistency": 230, "depth": 220, "covenant": 245,
                },
            }

            agent._seal_report(report)

            rings = agent.tc.load()
            ring = next(r for r in rings if r["ring_type"] == "token_analysis")
            self.assertEqual(ring["ring_type"], "token_analysis")
            self.assertEqual(ring["payload"]["block_pin"], 100)
            self.assertEqual(ring["payload"]["claim_evidence"]["contract"], ["F0000"])
            self.assertEqual(
                ring["payload"]["cognitive_loop"]["input_policy"],
                "trusted_structured_fields_only",
            )
            self.assertTrue(
                ring["payload"]["cognitive_loop"]["senses"]
                or ring["payload"]["cognitive_loop"]["modalities"]
            )
            self.assertEqual(report["analysis_ring"], ring["index"])
            self.assertEqual(report["cognition"]["status"], "complete")
            completion = next(
                r for r in rings if r["ring_type"] == "cognitive_completion"
            )
            self.assertEqual(
                completion["payload"]["analysis_ring"], report["analysis_ring"]
            )
            self.assertEqual(report["cognitive_ring"], completion["index"])

    def test_partial_faculty_registry_fails_closed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            registry = Path(temp_dir) / "registry"
            registry.mkdir()
            (registry / "senses.json").write_text(
                json.dumps({"registry": "senses", "senses": []}),
                encoding="utf-8",
            )
            with patch.object(chainseer.RobinhoodRPC, "get_block_number", return_value=100):
                with self.assertRaisesRegex(RuntimeError, "incomplete"):
                    chainseer.Chainseer(chain_root=temp_dir)

    def test_rpc_uses_block_pin_and_request_cache(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = chainseer.ProvenanceLedger(Path(temp_dir) / "evidence")
            ledger.block_pin = 12345
            context = chainseer.ScanContext(chainseer.CHAIN_ID, 12345, ledger)
            rpc = chainseer.RobinhoodRPC("https://rpc.invalid")
            rpc._session = FakeSession({"jsonrpc": "2.0", "id": 1, "result": "0x6000"})
            rpc.bind_context(context)

            self.assertEqual(rpc.get_code("0x" + "1" * 40), "0x6000")
            self.assertEqual(rpc.get_code("0x" + "1" * 40), "0x6000")

            self.assertEqual(len(rpc._session.calls), 1)
            payload = rpc._session.calls[0][1]
            self.assertEqual(payload["params"][1], hex(12345))
            self.assertEqual(ledger.to_dict()["fact_count"], 1)
            self.assertTrue(ledger.verify())

    def test_provenance_ledger_dedups_concurrent_identical_writes(self):
        """Phase 1's fetches now run on a thread pool and all write through
        the same ledger. Many threads recording the exact same fact must
        collapse to exactly one entry, not one per thread."""
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = chainseer.ProvenanceLedger(Path(temp_dir) / "evidence")
            ledger.block_pin = 100

            def _record():
                return ledger.record(
                    "http", {"url": "https://same.invalid"}, {"v": 1}
                )

            with ThreadPoolExecutor(max_workers=16) as pool:
                fact_ids = list(pool.map(lambda _: _record(), range(64)))

            self.assertEqual(len(set(fact_ids)), 1)
            self.assertEqual(ledger.to_dict()["fact_count"], 1)
            self.assertTrue(ledger.verify())

    def test_provenance_ledger_assigns_unique_ids_under_concurrency(self):
        """Distinct facts recorded concurrently must never collide on
        fact_id -- the citation system depends on fact_id uniqueness."""
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger = chainseer.ProvenanceLedger(Path(temp_dir) / "evidence")
            ledger.block_pin = 100

            def _record(index):
                return ledger.record(
                    "http", {"url": f"https://distinct-{index}.invalid"},
                    {"v": index},
                )

            with ThreadPoolExecutor(max_workers=16) as pool:
                fact_ids = list(pool.map(_record, range(64)))

            self.assertEqual(len(fact_ids), 64)
            self.assertEqual(len(set(fact_ids)), 64)
            self.assertEqual(ledger.to_dict()["fact_count"], 64)
            self.assertTrue(ledger.verify())

    def test_http_cache_preserves_provenance_for_each_scan(self):
        url = "https://example.invalid/cache-test"
        chainseer._HTTP_CACHE = chainseer.TTLCache(ttl_seconds=60, maxsize=4)
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_a = chainseer.ProvenanceLedger(Path(temp_dir) / "a")
            ledger_b = chainseer.ProvenanceLedger(Path(temp_dir) / "b")
            with patch("chainseer.requests.get", return_value=FakeResponse({"value": 7})) as get:
                first, _, first_hit = chainseer._http_get_json(url, ledger=ledger_a)
                second, _, second_hit = chainseer._http_get_json(url, ledger=ledger_b)

            self.assertEqual(first, second)
            self.assertFalse(first_hit)
            self.assertTrue(second_hit)
            self.assertEqual(get.call_count, 1)
            self.assertTrue(ledger_a.verify())
            self.assertTrue(ledger_b.verify())
            self.assertTrue(ledger_b.to_dict()["facts"][0]["cache_hit"])

    def test_isolated_provenance_absorbs_in_declared_order(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            evidence = Path(temp_dir) / "evidence"
            main = chainseer.ProvenanceLedger(evidence)
            first = chainseer.ProvenanceLedger(evidence)
            second = chainseer.ProvenanceLedger(evidence)
            for ledger in (main, first, second):
                ledger.block_pin = 12345
            second.record("http", {"url": "https://second.invalid"}, {"v": 2})
            first.record("http", {"url": "https://first.invalid"}, {"v": 1})

            main.absorb(first)
            main.absorb(second)

            facts = main.to_dict()["facts"]
            self.assertEqual(
                [fact["query"]["url"] for fact in facts],
                ["https://first.invalid", "https://second.invalid"],
            )
            self.assertEqual(
                [fact["fact_id"] for fact in facts],
                ["F0000", "F0001"],
            )
            self.assertTrue(main.verify())

    def test_structured_analysis_validation(self):
        value = {
            "risk_level": "High",
            "legitimacy_score": 42.5,
            "recommendation": "Avoid until uncertainty is resolved.",
            "evidence_fact_ids": ["F0000", "F0012"],
        }
        self.assertIs(chainseer.validate_structured_analysis(value), value)
        with self.assertRaises(ValueError):
            chainseer.validate_structured_analysis({**value, "legitimacy_score": 101})

    def test_hard_stops_override_weighted_model(self):
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        analysis = agent._analyze(self.investor_fixture())
        codes = {item["code"] for item in analysis["hard_stop_overrides"]}

        self.assertEqual(analysis["risk_level"], "Critical")
        self.assertEqual(analysis["action_label"], "AVOID")
        self.assertIn("UNLOCKED_LP", codes)
        self.assertIn("EXTREME_CONCENTRATION", codes)
        self.assertEqual(analysis["confidence_grade"], "LIMITED")

    def test_primary_market_selection_is_not_biased_toward_v2(self):
        v2 = {
            "pairAddress": "0x" + "1" * 40,
            "labels": ["v2"],
            "liquidity": {"usd": 100},
            "quoteToken": {"address": chainseer.WETH_ADDRESS},
        }
        v4 = {
            "pairAddress": "0x" + "2" * 64,
            "labels": ["v4"],
            "liquidity": {"usd": 50_000},
            "quoteToken": {"address": chainseer.WETH_ADDRESS},
        }

        self.assertIs(
            chainseer._pick_primary_pair({"pairs": [v2, v4]}),
            v4,
        )
        self.assertEqual(chainseer._amm_version(v4), "v4")

    def test_dex_analysis_ignores_pairs_from_other_chains(self):
        robinhood_pair = {
            "chainId": "robinhood",
            "baseToken": {"address": "0x" + "a" * 40},
            "pairAddress": "0x" + "2" * 64,
            "labels": ["v4"],
            "liquidity": {"usd": 10_000},
            "priceUsd": "0.25",
        }
        foreign_pair = {
            "chainId": "base",
            "baseToken": {"address": "0x" + "a" * 40},
            "pairAddress": "0x" + "1" * 40,
            "labels": ["v2"],
            "liquidity": {"usd": 1_000_000},
            "priceUsd": "99",
        }
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.rpc = FailOnLPTokenRPC()

        result = agent._analyze_dex_pairs(
            "0x" + "a" * 40,
            {
                "dexscreener": {
                    "pairs": [foreign_pair, robinhood_pair],
                },
                "goplus_security": {},
            },
        )

        self.assertEqual(
            result["primary_pair_address"],
            robinhood_pair["pairAddress"],
        )
        self.assertEqual(result["primary_price_usd"], 0.25)
        self.assertEqual(result["total_liquidity_usd"], 10_000)
        self.assertEqual(result["discarded_foreign_pair_count"], 1)

    def test_dex_analysis_ignores_pairs_where_target_is_quote_asset(self):
        token = "0x" + "a" * 40
        reversed_pair = {
            "chainId": "robinhood",
            "pairAddress": "0x" + "1" * 40,
            "baseToken": {"address": "0x" + "b" * 40},
            "quoteToken": {"address": token},
            "labels": ["v2"],
            "liquidity": {"usd": 1_000_000},
            "priceUsd": "99",
        }
        correctly_oriented = {
            "chainId": "robinhood",
            "pairAddress": "0x" + "2" * 64,
            "baseToken": {"address": token},
            "quoteToken": {"address": chainseer.WETH_ADDRESS},
            "labels": ["v4"],
            "liquidity": {"usd": 10_000},
            "priceUsd": "0.25",
        }
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.rpc = FailOnLPTokenRPC()
        result = agent._analyze_dex_pairs(
            token,
            {
                "dexscreener": {
                    "pairs": [reversed_pair, correctly_oriented],
                },
                "goplus_security": {},
            },
        )
        self.assertEqual(
            result["primary_pair_address"],
            correctly_oriented["pairAddress"],
        )
        self.assertEqual(result["primary_price_usd"], 0.25)
        self.assertEqual(
            result["discarded_quote_or_unbound_pair_count"], 1
        )

    def test_goplus_unavailable_tax_fallback_actually_computes_a_result(self):
        # _estimate_tax_from_reserves() reads dex_data["primary_pair_raw"],
        # which _analyze_dex_pairs() computes internally but never used to
        # store on the returned dict -- the GoPlus-unavailable fallback
        # always silently returned available=False. This proves the field
        # is now wired through and the fallback actually estimates a tax.
        token = "0x" + "a" * 40
        pair = {
            "chainId": "robinhood",
            "pairAddress": "0x" + "2" * 64,
            "baseToken": {"address": token},
            "quoteToken": {"address": chainseer.WETH_ADDRESS},
            "labels": ["v2"],
            "liquidity": {"usd": 10_000, "quote": 10, "base": 1_000_000},
            "priceUsd": "0.001",
            "priceNative": "0.0000105",
        }
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.rpc = FailOnLPTokenRPC()
        dex_pairs = agent._analyze_dex_pairs(
            token,
            {"dexscreener": {"pairs": [pair]}, "goplus_security": {}},
        )
        self.assertEqual(dex_pairs["primary_pair_raw"], pair)

        tax = agent._estimate_tax_from_reserves(
            token,
            {
                **dex_pairs,
                "on_chain_reserves": {
                    "reserve_eth": 10,
                    "reserve_tokens_raw": 1_000_000,
                },
            },
        )
        self.assertTrue(tax["available"])
        self.assertIsNotNone(tax["buy_tax_estimate"])
        self.assertGreater(tax["buy_tax_estimate"], 0)

    def test_holder_analysis_excludes_only_verified_amm_and_uses_supply(self):
        pool = "0x" + "a" * 40
        eoa = "0x" + "b" * 40
        eip7702 = "0x" + "c" * 40
        holders = [
            {
                "address": pool,
                "is_contract": True,
                "balance_raw": "330",
                "address_info": {},
            },
            {
                "address": eoa,
                "is_contract": False,
                "balance_raw": "40",
                "address_info": {},
            },
            {
                "address": eip7702,
                "is_contract": True,
                "balance_raw": "20",
                "address_info": {"proxy_type": "eip7702"},
            },
        ]
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.ledger = None
        agent.rpc = types.SimpleNamespace(
            erc20_balance_of=lambda _token, address, block=None: {
                pool: 330,
                eoa: 40,
                eip7702: 20,
            }[address]
        )

        with patch(
            "chainseer._fetch_blockscout_holders",
            return_value=holders,
        ):
            result = agent._analyze_holders_blockscout(
                "0x" + "d" * 40,
                # The EOA is deliberately included in the market candidate
                # list; dual-source classification must keep it counted.
                verified_amm_addresses=[pool, eoa],
                total_supply_raw=1000,
            )

        self.assertEqual(result["pair_contracts_excluded"], [pool])
        self.assertIn(eip7702, result["unclassified_contract_holders"])
        self.assertEqual(result["eip7702_count"], 1)
        self.assertEqual(result["concentration_basis"], "total_supply")
        self.assertTrue(result["concentration_complete"])
        self.assertEqual(result["top_1_pct"], 33.0)
        self.assertEqual(result["adj_top_1_pct"], 4.0)

    def test_unverified_contract_holder_is_not_excluded(self):
        contract_holder = "0x" + "a" * 40
        holders = [
            {
                "address": contract_holder,
                "is_contract": True,
                "balance_raw": "600",
                "address_info": {},
            },
            {
                "address": "0x" + "b" * 40,
                "is_contract": False,
                "balance_raw": "100",
                "address_info": {},
            },
        ]
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.ledger = None
        agent.rpc = types.SimpleNamespace(
            erc20_balance_of=lambda _token, address, block=None: {
                contract_holder: 600,
                "0x" + "b" * 40: 100,
            }[address]
        )

        with patch(
            "chainseer._fetch_blockscout_holders",
            return_value=holders,
        ):
            result = agent._analyze_holders_blockscout(
                "0x" + "d" * 40,
                verified_amm_addresses=[],
                total_supply_raw=1000,
            )

        self.assertEqual(result["pair_contracts_excluded"], [])
        self.assertIn(
            contract_holder, result["unclassified_contract_holders"]
        )
        self.assertEqual(result["adj_top_1_pct"], 60.0)

    def test_stale_indexer_balance_is_replaced_by_pinned_rpc_balance(self):
        stale_holder = "0x" + "a" * 40
        current_holder = "0x" + "b" * 40
        holders = [
            {
                "address": stale_holder,
                "is_contract": True,
                "balance_raw": "875",
                "address_info": {},
            },
            {
                "address": current_holder,
                "is_contract": False,
                "balance_raw": "80",
                "address_info": {},
            },
        ]
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.ledger = None
        agent.rpc = types.SimpleNamespace(
            erc20_balance_of=lambda _token, address, block=None: {
                stale_holder: 1,
                current_holder: 80,
            }[address]
        )

        with patch(
            "chainseer._fetch_blockscout_holders",
            return_value=holders,
        ):
            result = agent._analyze_holders_blockscout(
                "0x" + "d" * 40,
                verified_amm_addresses=[],
                total_supply_raw=1000,
            )

        self.assertTrue(result["concentration_complete"])
        self.assertEqual(result["indexed_balance_mismatch_count"], 1)
        self.assertEqual(result["adj_top_1_pct"], 8.0)
        self.assertEqual(result["holders"][0]["address"], current_holder)

    def test_unverified_indexer_concentration_cannot_hard_stop(self):
        data = self.investor_fixture()
        data["blockscout_holders"]["concentration_complete"] = False
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)

        analysis = agent._analyze(data)
        codes = {item["code"] for item in analysis["hard_stop_overrides"]}

        self.assertNotIn("EXTREME_CONCENTRATION", codes)

    def test_holder_base_score_is_age_aware_and_sybil_capped(self):
        young = chainseer._holder_base_score(669, 9)
        mature = chainseer._holder_base_score(669, 100)
        unknown = chainseer._holder_base_score(669, None)
        tiny = chainseer._holder_base_score(9, 0.1)

        self.assertEqual(young["cohort"], "7-14 days")
        self.assertEqual(young["target_holders"], 750)
        self.assertGreater(young["score"], 75)
        self.assertGreater(young["score"], mature["score"])
        self.assertEqual(unknown["target_holders"], 5000)
        self.assertEqual(unknown["score"], mature["score"])
        self.assertLessEqual(tiny["score"], 25)

    def test_position_based_pools_are_custody_unverified_not_unlocked(self):
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.rpc = FailOnLPTokenRPC()

        fixtures = [
            {
                "primary_pair_address": "0x" + "a" * 40,
                "primary_amm_version": "v3",
                "primary_liquidity_usd": 20_000,
            },
            {
                "primary_pair_address": "0x" + "b" * 64,
                "primary_amm_version": "v4",
                "primary_liquidity_usd": 20_000,
            },
        ]
        for dex_data in fixtures:
            with self.subTest(version=dex_data["primary_amm_version"]):
                custody = agent._verify_lp_lock(
                    "0x" + "c" * 40, dex_data
                )
                self.assertEqual(custody["state"], "custody_unverified")
                self.assertFalse(custody["hard_stop_eligible"])
                self.assertIn("position", custody["method"].lower())

    def test_unverified_v4_custody_does_not_trigger_unlocked_lp_stop(self):
        data = self.investor_fixture()
        data["blockscout_holders"]["adj_top_1_pct"] = 10
        data["dex_pairs"]["primary_amm_version"] = "v4"
        data["lp_lock"] = {
            "state": "custody_unverified",
            "locked": False,
            "withdrawal_verified": False,
            "hard_stop_eligible": False,
            "amm_version": "v4",
            "method": "V4 position owner is not verified",
        }

        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        analysis = agent._analyze(data)
        codes = {
            item["code"] for item in analysis["hard_stop_overrides"]
        }

        self.assertNotIn("UNLOCKED_LP", codes)
        self.assertEqual(analysis["component_scores"]["lp_lock"], 50)
        self.assertIn("lp_lock", analysis["uncertain_components"])
        self.assertFalse(
            any(
                "lp not locked" in flag.lower()
                for flag in analysis["red_flags"]
            )
        )

    def test_verified_v2_creator_control_is_creator_withdrawable(self):
        creator = "0x" + "d" * 40
        holder = {
            "address": creator,
            "is_contract": False,
            "balance_raw": "800",
        }
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.rpc = FakeV2LiquidityRPC()
        agent.ledger = None
        dex_data = {
            "primary_pair_address": "0x" + "e" * 40,
            "primary_amm_version": "v2",
            "primary_liquidity_usd": 25_000,
        }

        with patch(
            "chainseer._fetch_blockscout_holders",
            return_value=[holder],
        ):
            custody = agent._verify_lp_lock(
                "0x" + "f" * 40,
                dex_data,
                creator_address=creator,
            )

        self.assertEqual(custody["state"], "creator_withdrawable")
        self.assertTrue(custody["withdrawal_verified"])
        self.assertTrue(custody["hard_stop_eligible"])
        self.assertEqual(custody["withdrawable_pct"], 80.0)

    def test_source_verification_conflict_never_emits_verified_green_flag(self):
        data = self.investor_fixture()
        data["goplus_security"] = {"is_open_source": "1"}
        data["source_code"] = {
            "available": True,
            "is_verified": False,
        }

        agent = object.__new__(chainseer.Chainseer)
        analysis = agent._analyze(data)

        self.assertFalse(
            any(
                "verified contract" in flag.lower()
                or "verified on blockscout" in flag.lower()
                for flag in analysis["green_flags"]
            )
        )
        self.assertTrue(
            any(
                "goplus reports open source" in flag.lower()
                for flag in analysis["yellow_flags"]
            )
        )
        self.assertIn(
            "disagree",
            analysis["uncertain_components"]["source_verification"].lower(),
        )

    def test_summary_and_full_report_are_investor_first(self):
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        data = self.investor_fixture()
        analysis = agent._analyze(data)
        report = {
            "token_address": "0x" + "4" * 40,
            "explorer_url": "https://example.invalid/token",
            "data": data,
            "analysis": analysis,
            "provenance": {"block_pin": 12345678, "fact_count": 34},
        }

        summary = agent._format_summary(report)
        self.assertIn("ACTION: AVOID", summary)
        self.assertIn("Model score:", summary)
        self.assertIn("Risk override:", summary)
        self.assertIn("Top non-AMM holder controls 82.5%", summary)
        self.assertIn("LIQUIDITY CUSTODY", summary)
        self.assertIn("CREATOR WITHDRAWABLE", summary)
        self.assertLess(summary.index("ACTION: AVOID"), summary.index("MARKET"))

        output = StringIO()
        with redirect_stdout(output):
            agent._print_report(report)
        full = output.getvalue()
        self.assertIn("EXECUTIVE DECISION", full)
        self.assertIn("HARD-STOP RISKS", full)
        self.assertIn("TECHNICAL SCORECARD", full)
        self.assertIn("Liquidity Custody", full)
        self.assertLess(full.index("EXECUTIVE DECISION"), full.index("TECHNICAL SCORECARD"))

    def test_reflection_is_append_only_and_separates_market_outcome(self):
        original = {
            "index": 7,
            "ring_type": "token_analysis",
            "ring_hash": "original-hash",
            "payload": {"token_address": "0x" + "2" * 40, "risk_level": "Low"},
        }
        agent = chainseer.Chainseer.__new__(chainseer.Chainseer)
        agent.tc = FakeTimechain([original])
        agent.poq_module = FakePoQ()

        result = agent.reflect_on_analysis(
            7,
            {"rug_pull": True, "price_return_pct": -90},
            evidence_fact_ids=["F0001"],
        )

        payload = agent.poq_module.kwargs["extra_payload"]
        self.assertTrue(result["calibration"]["dangerous_false_negative"])
        self.assertEqual(payload["security_outcomes"], {"rug_pull": True})
        self.assertEqual(payload["market_outcomes"], {"price_return_pct": -90})
        self.assertEqual(payload["analysis_ring_hash"], "original-hash")


class AlertWiringTests(unittest.TestCase):
    """analyze_token() must forward its final decision to the shared
    chainseer_alerts hook -- this is the single call site that covers both
    Robinhood Chain and Base (BasePublicAnalyzer inherits analyze_token
    unchanged)."""

    @staticmethod
    def _run_analyze_token(stack, agent, token, basic_info, analysis):
        """Patches every Phase 1-8 data-gathering/scoring call in
        analyze_token() so the pipeline can run end-to-end against a real
        agent without any network access, isolating the assertion to the
        Phase 8 alert_on_decision call site."""
        stack.enter_context(
            patch.object(
                chainseer.RobinhoodRPC, "get_code", return_value="0x" + "60" * 40
            )
        )
        stack.enter_context(patch("chainseer._fetch_goplus_security", return_value={}))
        stack.enter_context(
            patch("chainseer._fetch_dexscreener_token", return_value={"pairs": []})
        )
        stack.enter_context(
            patch("chainseer._fetch_blockscout_address", return_value={})
        )
        stack.enter_context(patch("chainseer._fetch_blockscout_token", return_value={}))
        stack.enter_context(
            patch(
                "chainseer._fetch_blockscout_source",
                return_value={"available": False, "is_verified": False},
            )
        )
        for method, value in (
            ("_fetch_basic_info", basic_info),
            ("_analyze_contract", {}),
            ("_analyze_dex_pairs", {}),
            ("_verify_lp_lock", {}),
            ("_estimate_tax_from_reserves", {"available": False}),
            ("_check_transfer_activity", {}),
            ("_analyze_deployer_and_creation", {}),
            ("_analyze_holders_blockscout", {}),
            ("_detect_wash_trading", {"available": False}),
            ("_build_token_trend", {"available": False}),
            ("_analyze", analysis),
            (
                "_self_evaluate",
                {
                    "coherence": 230, "relevance": 240, "novelty": 210,
                    "consistency": 230, "depth": 220, "covenant": 245,
                },
            ),
            # The cognitive-completion/immune-guard pipeline inside
            # _seal_report is exercised by test_report_seals_through_poq_
            # with_provenance; it is out of scope here (this test isolates
            # the alert_on_decision call site, which fires before sealing).
            ("_seal_report", None),
            ("_print_summary", None),
        ):
            stack.enter_context(
                patch.object(chainseer.Chainseer, method, return_value=value)
            )
        stack.enter_context(
            patch("chainseer_controls.build_extended_evidence", return_value={})
        )
        stack.enter_context(
            patch("chainseer.build_robinhood_entity_graph", return_value={})
        )
        stack.enter_context(
            patch("chainseer.verify_entity_graph", return_value=(True, ""))
        )
        mocked_alert = stack.enter_context(patch("chainseer.alert_on_decision"))
        report = agent.analyze_token(token)
        return report, mocked_alert

    def test_analyze_token_forwards_hard_stops_to_alert_hook(self):
        analysis = {
            "legitimacy_score": 12.0,
            "risk_level": "Critical",
            "action_label": "AVOID",
            "hard_stop_overrides": [
                {"code": "UNLOCKED_LP"},
                {"code": "EXTREME_CONCENTRATION"},
            ],
            "recommendation": "Avoid until uncertainty is resolved.",
            "green_flags": [],
            "red_flags": ["Synthetic test risk"],
            "component_scores": {"security": 10},
            "confidence": "test",
            "confidence_grade": "LIMITED",
            "uncertain_components": {},
        }
        basic_info = {
            "name": "Test Token",
            "symbol": "TST",
            "total_supply_raw": 1,
            "total_supply": 1,
        }
        token = "0x" + "4" * 40

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(
                chainseer.RobinhoodRPC, "get_block_number", return_value=100
            ):
                agent = chainseer.Chainseer(chain_root=temp_dir)

            with ExitStack() as stack:
                report, mocked_alert = self._run_analyze_token(
                    stack, agent, token, basic_info, analysis
                )

            mocked_alert.assert_called_once()
            kwargs = mocked_alert.call_args.kwargs
            self.assertEqual(kwargs["chain"], agent.network_key)
            self.assertEqual(kwargs["token_address"], token)
            self.assertEqual(kwargs["symbol"], "TST")
            self.assertEqual(kwargs["risk_level"], "Critical")
            self.assertEqual(kwargs["score"], 12.0)
            self.assertEqual(
                set(kwargs["hard_stops"]),
                {"UNLOCKED_LP", "EXTREME_CONCENTRATION"},
            )
            self.assertEqual(report["analysis"]["risk_level"], "Critical")

    def test_analyze_token_reports_no_hard_stops_when_clean(self):
        analysis = {
            "legitimacy_score": 91.0,
            "risk_level": "Low",
            "action_label": "PASS",
            "hard_stop_overrides": [],
            "recommendation": "Looks clean.",
            "green_flags": ["Liquidity locked"],
            "red_flags": [],
            "component_scores": {"security": 90},
            "confidence": "test",
            "confidence_grade": "HIGH",
            "uncertain_components": {},
        }
        basic_info = {
            "name": "Clean Token",
            "symbol": "CLN",
            "total_supply_raw": 1,
            "total_supply": 1,
        }
        token = "0x" + "5" * 40

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(
                chainseer.RobinhoodRPC, "get_block_number", return_value=100
            ):
                agent = chainseer.Chainseer(chain_root=temp_dir)

            with ExitStack() as stack:
                _, mocked_alert = self._run_analyze_token(
                    stack, agent, token, basic_info, analysis
                )

            mocked_alert.assert_called_once()
            self.assertEqual(mocked_alert.call_args.kwargs["hard_stops"], [])
            self.assertEqual(mocked_alert.call_args.kwargs["risk_level"], "Low")


if __name__ == "__main__":
    unittest.main()
