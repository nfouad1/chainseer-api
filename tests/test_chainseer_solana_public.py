import time
import unittest

from chainseer_solana_public import (
    SolanaMintError,
    SolanaPublicAnalyzer,
    TOKEN_PROGRAM_ID,
    validate_solana_mint,
)


MINT = "So11111111111111111111111111111111111111112"


class FakeRPC:
    def __init__(self, *, mint_authority=None):
        self.mint_authority = mint_authority

    def get_slot(self):
        return 321

    def get_account_info(self, address, *, encoding="jsonParsed"):
        return {
            "context": {"slot": 322},
            "value": {
                "owner": TOKEN_PROGRAM_ID,
                "data": {
                    "parsed": {
                        "type": "mint",
                        "info": {
                            "decimals": 9,
                            "supply": "1000000000000000",
                            "mintAuthority": self.mint_authority,
                            "freezeAuthority": None,
                        },
                    }
                },
            },
        }

    def get_token_supply(self, mint):
        return {
            "context": {"slot": 322},
            "value": {
                "amount": "1000000000000000",
                "decimals": 9,
            },
        }

    def get_token_largest_accounts(self, mint):
        return {
            "context": {"slot": 323},
            "value": [
                {"address": "3" * 44, "amount": "100000000000000"},
                {"address": "4" * 44, "amount": "50000000000000"},
            ],
        }

    def get_multiple_accounts(self, addresses, *, encoding="jsonParsed"):
        return {
            "context": {"slot": 323},
            "value": [
                {
                    "data": {
                        "parsed": {
                            # A valid-looking base58 owner per account, not
                            # the entity graph's real owner-resolution logic
                            # under test here -- just needs to satisfy
                            # SOLANA_ADDRESS_RE so the graph node is built.
                            "info": {"owner": str(index + 1) * 44}
                        }
                    }
                }
                for index, _ in enumerate(addresses)
            ],
        }


class FakeDexScreener:
    def token_pairs(self, mint):
        return [
            {
                "chainId": "solana",
                "dexId": "orca",
                "pairAddress": "pair-1",
                "url": "https://dexscreener.com/solana/pair-1",
                "baseToken": {
                    "address": mint,
                    "name": "Example",
                    "symbol": "EX",
                },
                "quoteToken": {
                    "address": "quote",
                    "name": "USD Coin",
                    "symbol": "USDC",
                },
                "priceUsd": "0.25",
                "marketCap": 2500000,
                "liquidity": {"usd": 150000},
                "volume": {"h24": 65000},
                "pairCreatedAt": int((time.time() - 10 * 86400) * 1000),
                "txns": {"h24": {"buys": 120, "sells": 100}},
                "priceChange": {"h24": 3.5},
            }
        ]


class FakeJupiter:
    def token_info(self, mint):
        return {
            "id": mint,
            "name": "Example",
            "symbol": "EX",
            "isVerified": True,
            "holderCount": 5000,
        }

    def roundtrip(self, mint, input_lamports):
        return {
            "buy": {
                "price_impact_pct": 0.4,
                "router": "jupiter",
            },
            "sell": {"router": "jupiter"},
            "roundtrip_retention_pct": 96.5,
        }


class UnavailableJupiter:
    def token_info(self, mint):
        from chainseer_solana_public import InfrastructureIndeterminateError

        raise InfrastructureIndeterminateError("Jupiter metadata unavailable")

    def roundtrip(self, mint, input_lamports):
        from chainseer_solana_public import InfrastructureIndeterminateError

        raise InfrastructureIndeterminateError("Jupiter route unavailable")


class FakeCognitiveLoop:
    def prepare(self, report):
        report["cognition"] = {"status": "prepared"}
        return report["cognition"]

    def finalize(self, report, ring):
        report["cognition"]["status"] = "complete"
        report["cognitive_ring"] = ring["index"] + 1
        report["cognitive_ring_hash"] = "c" * 64
        return report["cognition"]


class FakePoQ:
    def gate_and_seal(self, tc, candidate, **kwargs):
        self.candidate = candidate
        self.kwargs = kwargs
        return (
            {"decision": "SEAL", "scores": kwargs["external_scores"]},
            {"index": 77, "ring_hash": "a" * 64},
        )


class FakeTimechainAgent:
    def __init__(self):
        self.tc = object()
        self.cognitive_loop = FakeCognitiveLoop()
        self.poq_module = FakePoQ()


class SolanaMintValidationTests(unittest.TestCase):
    def test_accepts_canonical_32_byte_pubkey(self):
        self.assertEqual(validate_solana_mint(MINT), MINT)

    def test_rejects_wrong_decoded_length(self):
        with self.assertRaises(SolanaMintError):
            validate_solana_mint("2" * 32)

    def test_rejects_non_base58(self):
        with self.assertRaises(SolanaMintError):
            validate_solana_mint("0" * 44)


class SolanaPublicAnalyzerTests(unittest.TestCase):
    def analyzer(self, *, mint_authority=None):
        return SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=FakeRPC(mint_authority=mint_authority),
            dexscreener=FakeDexScreener(),
            jupiter=FakeJupiter(),
        )

    def test_builds_generic_spl_report_without_false_lp_claim(self):
        report = self.analyzer().analyze_token(MINT)
        self.assertEqual(report["chain_name"], "Solana")
        self.assertEqual(report["chain_id"], "mainnet-beta")
        self.assertEqual(
            report["provenance"]["anchor_type"],
            "confirmed_slot_anchor",
        )
        self.assertEqual(report["provenance"]["block_pin"], 321)
        self.assertEqual(report["analysis"]["risk_level"], "Medium")
        self.assertEqual(report["analysis"]["action_label"], "WATCHLIST")
        self.assertEqual(report["analysis"]["hard_stop_overrides"], [])
        self.assertEqual(
            report["data"]["lp_lock"]["state"],
            "custody_unverified",
        )
        self.assertFalse(
            report["data"]["holder_concentration"][
                "pool_and_program_vaults_excluded"
            ]
        )
        graph = report["data"]["entity_graph"]
        self.assertEqual(graph["network"], "solana")
        self.assertEqual(graph["anchor"]["value"], 321)
        self.assertEqual(
            graph["summary"]["scoring_scope"], "evidence_only"
        )
        self.assertFalse(
            graph["summary"]["changes_legitimacy_score"]
        )
        self.assertTrue(
            any(
                edge["relationship"] == "controls_token_account"
                for edge in graph["edges"]
            )
        )
        self.assertIn(
            "creator_risk",
            report["analysis"]["uncertain_components"],
        )
        self.assertEqual(
            set(report["analysis"]["component_scores"]),
            {
                "security",
                "honeypot_safety",
                "liquidity",
                "lp_lock",
                "holder_distribution",
                "volume",
                "maturity",
                "creator_risk",
                "wash_trading",
                "deployer",
                "sentiment",
                "trend",
            },
        )
        self.assertNotIn("analysis_ring", report)

    def test_active_mint_authority_is_a_hard_stop(self):
        report = self.analyzer(
            mint_authority="authority-address"
        ).analyze_token(MINT)
        codes = {
            item["code"]
            for item in report["analysis"]["hard_stop_overrides"]
        }
        self.assertIn("mint_authority_active", codes)
        self.assertEqual(report["analysis"]["risk_level"], "High")
        self.assertEqual(report["analysis"]["action_label"], "AVOID")
        self.assertLessEqual(
            report["analysis"]["legitimacy_score"],
            35,
        )

    def test_infrastructure_failure_lowers_confidence_not_token_score(self):
        analyzer = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=FakeRPC(),
            dexscreener=FakeDexScreener(),
            jupiter=UnavailableJupiter(),
        )
        report = analyzer.analyze_token(MINT)
        self.assertEqual(report["analysis"]["risk_level"], "Medium")
        self.assertEqual(report["analysis"]["hard_stop_overrides"], [])
        self.assertEqual(
            report["analysis"]["component_scores"]["honeypot_safety"],
            50.0,
        )
        self.assertNotIn(
            "Jupiter route unavailable",
            report["analysis"]["yellow_flags"],
        )
        self.assertIn(
            "infrastructure",
            report["analysis"]["uncertain_components"],
        )
        self.assertEqual(len(report["infrastructure_indeterminate"]), 2)

    def test_shared_cognitive_loop_and_poq_seal_the_report(self):
        agent = FakeTimechainAgent()
        analyzer = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=FakeRPC(),
            dexscreener=FakeDexScreener(),
            jupiter=FakeJupiter(),
            timechain_agent=agent,
        )
        report = analyzer.analyze_token(MINT)
        self.assertEqual(report["analysis_ring"], 77)
        self.assertEqual(report["analysis_ring_hash"], "a" * 64)
        self.assertEqual(report["cognition"]["status"], "complete")
        self.assertEqual(report["cognitive_ring"], 78)
        self.assertEqual(
            agent.poq_module.kwargs["ring_type"],
            "solana_token_analysis",
        )
        self.assertFalse(
            agent.poq_module.kwargs["extra_payload"][
                "live_execution_enabled"
            ]
        )


if __name__ == "__main__":
    unittest.main()
