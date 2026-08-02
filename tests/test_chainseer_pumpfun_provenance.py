import base64
import struct
import unittest

import chainseer_pumpfun_provenance as provenance


def b58_bytes(seed: int) -> bytes:
    return bytes([seed]) * 32


def borsh_string(value: str) -> bytes:
    raw = value.encode()
    return struct.pack("<I", len(raw)) + raw


def encode_create_event(
    *, mint: bytes, creator: bytes, block_time: int, symbol: str = "TEST"
) -> str:
    raw = (
        provenance.PUMP_CREATE_EVENT_DISCRIMINATOR
        + borsh_string("Test Token")
        + borsh_string(symbol)
        + borsh_string("https://example.invalid/meta.json")
        + mint
        + b58_bytes(2)  # bonding_curve
        + b58_bytes(3)  # user
        + creator
        + struct.pack("<q", block_time)
        + struct.pack("<Q", 1_073_000_000_000_000)
        + struct.pack("<Q", 30_000_000_000)
        + struct.pack("<Q", 793_100_000_000_000)
        + struct.pack("<Q", 1_000_000_000_000_000)
        + b58_bytes(5)  # token_program
        + b"\0\0"
    )
    return base64.b64encode(raw).decode()


class FakeRPC:
    """Minimal signature/transaction store, address-agnostic like the real
    getSignaturesForAddress semantics this module is written against."""

    def __init__(self):
        self.signatures: list[dict] = []
        self.transactions: dict[str, dict] = {}

    def get_signatures(self, _address, *, limit, before=None):
        rows = self.signatures
        if before:
            index = next(
                (i for i, row in enumerate(rows) if row.get("signature") == before),
                None,
            )
            rows = rows[index + 1:] if index is not None else []
        return rows[:limit]

    def get_transaction(self, signature):
        return self.transactions.get(signature)


def pump_transaction(logs: list[str]) -> dict:
    return {
        "transaction": {
            "message": {"accountKeys": [{"pubkey": provenance.PUMP_PROGRAM_ID}]}
        },
        "meta": {"err": None, "logMessages": logs},
    }


class DecodeCreateEventTests(unittest.TestCase):
    def test_round_trips_mint_and_creator(self):
        mint = provenance._b58encode(b58_bytes(50))
        creator = provenance._b58encode(b58_bytes(99))
        payload = encode_create_event(
            mint=b58_bytes(50), creator=b58_bytes(99), block_time=1_700_000_000
        )
        event = provenance.decode_create_event(
            payload, signature="sig", slot=1, block_time=1_700_000_000
        )
        self.assertIsNotNone(event)
        self.assertEqual(event.mint, mint)
        self.assertEqual(event.creator, creator)
        self.assertEqual(event.symbol, "TEST")

    def test_ignores_non_create_program_data(self):
        encoded = base64.b64encode(b"not-a-create-event").decode()
        self.assertIsNone(
            provenance.decode_create_event(
                encoded, signature="sig", slot=1, block_time=1
            )
        )


class ResolveGenesisCreatorTests(unittest.TestCase):
    def test_gives_up_past_max_pages_without_ever_decoding(self):
        rpc = FakeRPC()
        # 20 full pages of 5 -- never reaches a partial (final) page within
        # max_pages=3, so this must fail closed with None rather than guess.
        rpc.signatures = [
            {"signature": f"sig-{i}", "slot": i, "blockTime": 2_000_000_000 - i}
            for i in range(20)
        ]
        found = provenance.resolve_genesis_creator(
            rpc.get_signatures,
            rpc.get_transaction,
            provenance._b58encode(b58_bytes(1)),
            max_pages=3,
            page_size=5,
            decode_last=10,
        )
        self.assertIsNone(found)

    def test_finds_genesis_among_oldest_signatures_of_final_page(self):
        mint = provenance._b58encode(b58_bytes(50))
        creator = provenance._b58encode(b58_bytes(99))
        rpc = FakeRPC()
        rpc.signatures = [
            {
                "signature": "recent",
                "slot": 2,
                "blockTime": 2_000_000_000,
                "err": None,
            },
            {
                "signature": "genesis",
                "slot": 1,
                "blockTime": 1_999_999_000,
                "err": None,
            },
        ]
        rpc.transactions["recent"] = pump_transaction([])
        rpc.transactions["genesis"] = pump_transaction(
            [
                "Program data: "
                + encode_create_event(
                    mint=b58_bytes(50),
                    creator=b58_bytes(99),
                    block_time=1_999_999_000,
                )
            ]
        )

        found = provenance.resolve_genesis_creator(
            rpc.get_signatures, rpc.get_transaction, mint, page_size=5
        )
        self.assertIsNotNone(found)
        self.assertEqual(found.creator, creator)
        self.assertEqual(found.mint, mint)


class CreatorDeploymentHistoryTests(unittest.TestCase):
    def test_paginates_past_unrelated_activity_to_find_prior_deployments(self):
        creator = provenance._b58encode(b58_bytes(4))
        now = 2_000_000_000.0
        rpc = FakeRPC()
        noise = [
            {
                "signature": f"noise-{i}",
                "slot": 10_000 + i,
                "blockTime": int(now - 60 * i),
                "err": "some-other-instruction-failed",
            }
            for i in range(30)
        ]
        farm_rows = []
        for i in range(5):
            sig = f"farm-{i}"
            block_time = int(now - 3600 - 60 * i)
            farm_rows.append(
                {
                    "signature": sig,
                    "slot": 5_000 + i,
                    "blockTime": block_time,
                    "err": None,
                }
            )
            rpc.transactions[sig] = pump_transaction(
                [
                    "Program data: "
                    + encode_create_event(
                        mint=b58_bytes(10 + i),
                        creator=b58_bytes(4),
                        block_time=block_time,
                        symbol=f"FARM{i}",
                    )
                ]
            )
        rpc.signatures = noise + farm_rows

        history = provenance.creator_deployment_history(
            rpc.get_signatures, rpc.get_transaction, creator, now=now
        )
        self.assertTrue(history["scanned"])
        self.assertEqual(history["prior_deployments_in_window"], 5)
        self.assertGreaterEqual(history["pages_scanned"], 2)

    def test_excludes_the_mint_being_analyzed(self):
        creator = provenance._b58encode(b58_bytes(4))
        target_mint = provenance._b58encode(b58_bytes(50))
        now = 2_000_000_000.0
        rpc = FakeRPC()
        rpc.signatures = [
            {"signature": "genesis", "slot": 1, "blockTime": int(now - 60), "err": None}
        ]
        rpc.transactions["genesis"] = pump_transaction(
            [
                "Program data: "
                + encode_create_event(
                    mint=b58_bytes(50), creator=b58_bytes(4), block_time=int(now - 60)
                )
            ]
        )
        history = provenance.creator_deployment_history(
            rpc.get_signatures,
            rpc.get_transaction,
            creator,
            exclude_mint=target_mint,
            now=now,
        )
        self.assertEqual(history["prior_deployments_in_window"], 0)


if __name__ == "__main__":
    unittest.main()
