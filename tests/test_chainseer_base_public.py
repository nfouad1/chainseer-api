import unittest

from chainseer import BASE_NETWORK, ROBINHOOD_NETWORK
from chainseer_base_public import BasePublicAnalyzer
from chainseer_entity_graph import (
    build_robinhood_entity_graph,
    verify_entity_graph,
)


TOKEN = "0x" + "a" * 40


class BasePublicAnalyzerTests(unittest.TestCase):
    def test_base_profile_is_isolated_from_robinhood(self):
        self.assertEqual(BASE_NETWORK.key, "base")
        self.assertEqual(BASE_NETWORK.chain_id, 8453)
        self.assertEqual(BASE_NETWORK.dexscreener_chain_id, "base")
        self.assertEqual(
            BASE_NETWORK.wrapped_native.lower(),
            "0x4200000000000000000000000000000000000006",
        )
        self.assertNotEqual(BASE_NETWORK.rpc_url, ROBINHOOD_NETWORK.rpc_url)
        self.assertNotEqual(
            BASE_NETWORK.blockscout_base,
            ROBINHOOD_NETWORK.blockscout_base,
        )
        self.assertTrue(issubclass(BasePublicAnalyzer, object))

    def test_base_entity_graph_is_valid_and_network_bound(self):
        graph = build_robinhood_entity_graph(
            TOKEN,
            {},
            block_pin=123,
            network="base",
        )
        ok, reason = verify_entity_graph(graph)
        self.assertTrue(ok, reason)
        self.assertEqual(graph["network"], "base")
        self.assertEqual(graph["anchor"]["value"], 123)

    def test_evm_entity_graph_rejects_unknown_network(self):
        with self.assertRaises(ValueError):
            build_robinhood_entity_graph(
                TOKEN,
                {},
                block_pin=123,
                network="unknown",
            )


if __name__ == "__main__":
    unittest.main()

