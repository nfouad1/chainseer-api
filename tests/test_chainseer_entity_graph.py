import copy
import unittest

from chainseer_entity_graph import (
    build_robinhood_entity_graph,
    build_solana_entity_graph,
    verify_entity_graph,
)


TOKEN = "0x" + "1" * 40
INSIDER = "0x" + "a" * 40
PAIR = "0x" + "b" * 40
IMPLEMENTATION = "0x" + "c" * 40


class RobinhoodEntityGraphTests(unittest.TestCase):
    @staticmethod
    def data():
        return {
            "goplus_security": {
                "creator_address": INSIDER,
                "owner_address": INSIDER,
            },
            "blockscout_address": {"creator_address": INSIDER},
            "deployer": {
                "creator_address": INSIDER,
                "creation_tx_hash": "0x" + "d" * 64,
                "is_serial_deployer": True,
                "total_deployer_creations": 8,
            },
            "dex_pairs": {
                "primary_pair_address": PAIR,
                "primary_amm_version": "v2",
                "primary_liquidity_usd": 150_000,
            },
            "lp_lock": {
                "state": "creator_withdrawable",
                "withdrawal_controller": INSIDER,
                "withdrawal_verified": True,
                "withdrawable_pct": 60,
            },
            "source_code": {
                "fact_id": "F0002",
                "implementations": [
                    {
                        "address_hash": IMPLEMENTATION,
                        "name": "<script>provider-controlled</script>",
                        "is_verified": True,
                    }
                ],
            },
            "blockscout_holders": {
                "concentration_denominator_raw": 1_000,
                "total_supply_raw": 1_000,
                "verified_amm_addresses": [PAIR],
                "holders": [
                    {
                        "address": PAIR,
                        "is_contract": True,
                        "balance_raw": "400",
                    },
                    {
                        "address": INSIDER,
                        "is_contract": False,
                        "balance_raw": "300",
                    },
                ],
            },
        }

    def test_builds_direct_control_and_concentration_evidence(self):
        graph = build_robinhood_entity_graph(
            TOKEN,
            self.data(),
            block_pin=123,
            claim_evidence={
                "external_apis": ["F0000"],
                "dex_pairs": ["F0003"],
                "lp_lock": ["F0004"],
                "activity_and_holders": ["F0006"],
            },
        )
        ok, reason = verify_entity_graph(graph)
        self.assertTrue(ok, reason)
        self.assertEqual(graph["anchor"]["value"], 123)
        self.assertEqual(
            graph["summary"]["insider_risk_level"], "Critical"
        )
        self.assertFalse(
            graph["summary"]["changes_legitimacy_score"]
        )
        signal_codes = {item["code"] for item in graph["signals"]}
        self.assertIn("direct_liquidity_control", signal_codes)
        self.assertIn("privileged_supply_concentration", signal_codes)
        self.assertIn("privileged_role_overlap", signal_codes)
        self.assertIn("serial_deployer", signal_codes)
        insider = next(
            node
            for node in graph["nodes"]
            if node["address"] == INSIDER
        )
        self.assertEqual(
            set(insider["roles"]),
            {
                "contract_owner",
                "deployer",
                "liquidity_controller",
                "reported_creator",
                "top_holder",
            },
        )
        relationships = {item["relationship"] for item in graph["edges"]}
        self.assertIn("controls_liquidity", relationships)
        self.assertIn("holds", relationships)
        self.assertIn("implementation_for", relationships)
        self.assertNotIn("provider-controlled", str(graph))

    def test_graph_is_deterministic_and_tamper_evident(self):
        first = build_robinhood_entity_graph(TOKEN, self.data(), block_pin=123)
        second = build_robinhood_entity_graph(TOKEN, self.data(), block_pin=123)
        self.assertEqual(first, second)
        changed = copy.deepcopy(first)
        changed["summary"]["signal_count"] += 1
        self.assertEqual(
            verify_entity_graph(changed),
            (False, "graph_hash_mismatch"),
        )

    def test_creator_provider_disagreement_stays_explicit(self):
        data = self.data()
        data["goplus_security"]["creator_address"] = "0x" + "e" * 40
        graph = build_robinhood_entity_graph(TOKEN, data, block_pin=123)
        signal_codes = {item["code"] for item in graph["signals"]}
        self.assertIn("creator_source_disagreement", signal_codes)
        deployer_edges = [
            edge
            for edge in graph["edges"]
            if edge["relationship"] == "deployed"
        ]
        self.assertEqual(len(deployer_edges), 2)


class SolanaEntityGraphTests(unittest.TestCase):
    def test_resolves_authority_control_without_inventing_vault_identity(self):
        mint = "So11111111111111111111111111111111111111112"
        authority = "authority-address"
        graph = build_solana_entity_graph(
            mint,
            {
                "basic_info": {
                    "mint_authority": authority,
                    "freeze_authority": authority,
                },
                "dex_pairs": {
                    "primary_pair": "pair-address",
                    "primary_amm_version": "orca",
                    "total_liquidity_usd": 50_000,
                },
                "holder_concentration": {
                    "largest_accounts": [
                        {
                            "token_account": "token-account-1",
                            "owner": authority,
                            "amount_raw": 250,
                            "pct_total_supply": 25,
                        }
                    ]
                },
            },
            slot_anchor=456,
            facts=[
                {"fact_id": f"solana-{index:02d}"}
                for index in range(1, 6)
            ],
        )
        ok, reason = verify_entity_graph(graph)
        self.assertTrue(ok, reason)
        signal_codes = {item["code"] for item in graph["signals"]}
        self.assertIn("active_mint_authority", signal_codes)
        self.assertIn("active_freeze_authority", signal_codes)
        self.assertIn(
            "authority_controls_large_token_account", signal_codes
        )
        self.assertIn("privileged_role_overlap", signal_codes)
        account_edge = next(
            edge
            for edge in graph["edges"]
            if edge["relationship"] == "holds"
        )
        self.assertFalse(
            account_edge["attributes"]["vault_identity_resolved"]
        )
        self.assertTrue(
            any(
                "AMM or program vaults" in limitation
                for limitation in graph["limitations"]
            )
        )

    def test_generic_holders_do_not_become_insiders(self):
        graph = build_solana_entity_graph(
            "So11111111111111111111111111111111111111112",
            {
                "basic_info": {
                    "mint_authority": None,
                    "freeze_authority": None,
                },
                "holder_concentration": {
                    "largest_accounts": [
                        {
                            "token_account": "token-account-1",
                            "owner": "ordinary-owner",
                            "amount_raw": 900,
                            "pct_total_supply": 90,
                        }
                    ]
                },
            },
            slot_anchor=456,
        )
        self.assertEqual(
            graph["summary"]["insider_risk_level"], "Unknown"
        )
        self.assertNotIn(
            "privileged_supply_concentration",
            {item["code"] for item in graph["signals"]},
        )


if __name__ == "__main__":
    unittest.main()
