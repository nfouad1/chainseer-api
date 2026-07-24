import json
import sys
import tempfile
import types
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

# The desktop test runtime does not bundle requests. Provide only the import
# surface needed by the mocked tests; production still requires real requests.
if "requests" not in sys.modules:
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


class ChainseerInfrastructureTests(unittest.TestCase):
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
            "pairAddress": "0x" + "2" * 64,
            "labels": ["v4"],
            "liquidity": {"usd": 10_000},
            "priceUsd": "0.25",
        }
        foreign_pair = {
            "chainId": "base",
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


if __name__ == "__main__":
    unittest.main()
