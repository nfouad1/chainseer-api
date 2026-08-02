import unittest

import chainseer_meteora_provenance as provenance

_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def b58_bytes(seed: int) -> bytes:
    return bytes([seed]) * 32


def b58_encode(raw: bytes) -> str:
    number = int.from_bytes(raw, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = _BASE58_ALPHABET[remainder] + encoded
    leading = len(raw) - len(raw.lstrip(b"\0"))
    return "1" * leading + (encoded or ("" if leading else "1"))


def pubkey(seed: int) -> str:
    return b58_encode(bytes([seed]) * 32)


def dbc_instruction(
    *,
    variant: str = "spl_token",
    config=None,
    pool_authority=None,
    creator=None,
    base_mint=None,
    quote_mint=None,
    pool=None,
    base_vault=None,
    quote_vault=None,
    extra_accounts: int = 6,
    program_id: str | None = None,
) -> dict:
    """A "partially decoded" jsonParsed instruction for a DBC pool-creation
    call, matching what Solana RPC returns for a program it has no built-in
    parser for: `accounts` is already a list of resolved pubkey strings
    (not accountKeys indices), and `data` is base58."""
    discriminator = next(
        raw for raw, name in provenance._DBC_POOL_CREATION_DISCRIMINATORS.items()
        if name == variant
    )
    accounts = [
        config or pubkey(1),
        pool_authority or pubkey(2),
        creator or pubkey(3),
        base_mint or pubkey(4),
        quote_mint or pubkey(5),
        pool or pubkey(6),
        base_vault or pubkey(7),
        quote_vault or pubkey(8),
    ] + [pubkey(50 + i) for i in range(extra_accounts)]
    data = b58_encode(discriminator + b"\x00" * 8)
    return {
        "programId": program_id or provenance.DBC_PROGRAM_ID,
        "accounts": accounts,
        "data": data,
    }


def dbc_transaction(
    instructions: list[dict], *, inner: list[dict] | None = None, err=None
) -> dict:
    return {
        "transaction": {"message": {"instructions": instructions}},
        "meta": {
            "err": err,
            "innerInstructions": (
                [{"index": 0, "instructions": inner}] if inner else []
            ),
        },
    }


class FakeRPC:
    """Minimal signature/transaction store, mirroring the fixture used for
    chainseer_pumpfun_provenance's own tests."""

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


class DecodePoolCreationInstructionTests(unittest.TestCase):
    def test_decodes_spl_token_variant(self):
        creator = pubkey(10)
        base_mint = pubkey(20)
        pool = pubkey(30)
        instruction = dbc_instruction(
            variant="spl_token", creator=creator, base_mint=base_mint, pool=pool
        )
        decoded = provenance._decode_pool_creation_instruction(instruction)
        self.assertEqual(decoded, (creator, base_mint, pool, "spl_token"))

    def test_decodes_token2022_variant(self):
        creator = pubkey(11)
        base_mint = pubkey(21)
        instruction = dbc_instruction(
            variant="token2022", creator=creator, base_mint=base_mint
        )
        decoded = provenance._decode_pool_creation_instruction(instruction)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded[3], "token2022")

    def test_decodes_token2022_transfer_hook_variant(self):
        instruction = dbc_instruction(variant="token2022_transfer_hook")
        decoded = provenance._decode_pool_creation_instruction(instruction)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded[3], "token2022_transfer_hook")

    def test_ignores_instructions_from_other_programs(self):
        instruction = dbc_instruction(program_id=pubkey(99))
        self.assertIsNone(
            provenance._decode_pool_creation_instruction(instruction)
        )

    def test_ignores_unrelated_dbc_instruction(self):
        """A DBC instruction that isn't one of the pool-creation variants
        (e.g. a swap) must not be misread as a launch."""
        instruction = dbc_instruction(variant="spl_token")
        instruction["data"] = b58_encode(b"\xff" * 8 + b"\x00" * 8)
        self.assertIsNone(
            provenance._decode_pool_creation_instruction(instruction)
        )

    def test_ignores_instruction_with_too_few_accounts(self):
        instruction = dbc_instruction(variant="spl_token", extra_accounts=0)
        instruction["accounts"] = instruction["accounts"][:4]
        self.assertIsNone(
            provenance._decode_pool_creation_instruction(instruction)
        )


class DecodeTransactionTests(unittest.TestCase):
    def test_decodes_top_level_instruction(self):
        creator = pubkey(10)
        base_mint = pubkey(20)
        row = {"signature": "sig1", "slot": 5, "blockTime": 1_700_000_000}
        tx = dbc_transaction(
            [dbc_instruction(creator=creator, base_mint=base_mint)]
        )
        creations = provenance.decode_transaction(row, tx)
        self.assertEqual(len(creations), 1)
        self.assertEqual(creations[0].creator, creator)
        self.assertEqual(creations[0].mint, base_mint)
        self.assertEqual(creations[0].signature, "sig1")

    def test_decodes_cpi_wrapped_inner_instruction(self):
        """Some launchpad front-ends invoke DBC via CPI rather than
        directly -- the pool-creation call then only appears in
        meta.innerInstructions, not the top-level instruction list."""
        creator = pubkey(15)
        base_mint = pubkey(25)
        row = {"signature": "sig2", "slot": 6, "blockTime": 1_700_000_100}
        tx = dbc_transaction(
            [{"programId": pubkey(200), "accounts": [], "data": "1111"}],
            inner=[dbc_instruction(creator=creator, base_mint=base_mint)],
        )
        creations = provenance.decode_transaction(row, tx)
        self.assertEqual(len(creations), 1)
        self.assertEqual(creations[0].creator, creator)
        self.assertEqual(creations[0].mint, base_mint)

    def test_returns_empty_for_failed_transaction(self):
        row = {"signature": "sig3", "slot": 7, "blockTime": 1_700_000_200}
        tx = dbc_transaction([dbc_instruction()], err="some-error")
        self.assertEqual(provenance.decode_transaction(row, tx), [])

    def test_returns_empty_for_missing_transaction(self):
        row = {"signature": "sig4", "slot": 8, "blockTime": 1_700_000_300}
        self.assertEqual(provenance.decode_transaction(row, None), [])


class ResolveGenesisCreatorTests(unittest.TestCase):
    def test_gives_up_past_max_pages_without_ever_decoding(self):
        rpc = FakeRPC()
        rpc.signatures = [
            {"signature": f"sig-{i}", "slot": i, "blockTime": 2_000_000_000 - i}
            for i in range(20)
        ]
        found = provenance.resolve_genesis_creator(
            rpc.get_signatures,
            rpc.get_transaction,
            pubkey(1),
            max_pages=3,
            page_size=5,
            decode_last=10,
        )
        self.assertIsNone(found)

    def test_finds_genesis_among_oldest_signatures_of_final_page(self):
        mint = pubkey(50)
        creator = pubkey(99)
        rpc = FakeRPC()
        rpc.signatures = [
            {"signature": "recent", "slot": 2, "blockTime": 2_000_000_000, "err": None},
            {"signature": "genesis", "slot": 1, "blockTime": 1_999_999_000, "err": None},
        ]
        rpc.transactions["recent"] = dbc_transaction([])
        rpc.transactions["genesis"] = dbc_transaction(
            [dbc_instruction(creator=creator, base_mint=mint)]
        )

        found = provenance.resolve_genesis_creator(
            rpc.get_signatures, rpc.get_transaction, mint, page_size=5
        )
        self.assertIsNotNone(found)
        self.assertEqual(found.creator, creator)
        self.assertEqual(found.mint, mint)


class CreatorDeploymentHistoryTests(unittest.TestCase):
    def test_paginates_past_unrelated_activity_to_find_prior_deployments(self):
        creator = pubkey(4)
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
                {"signature": sig, "slot": 5_000 + i, "blockTime": block_time, "err": None}
            )
            rpc.transactions[sig] = dbc_transaction(
                [dbc_instruction(creator=creator, base_mint=pubkey(10 + i))]
            )
        rpc.signatures = noise + farm_rows

        history = provenance.creator_deployment_history(
            rpc.get_signatures, rpc.get_transaction, creator, now=now
        )
        self.assertTrue(history["scanned"])
        self.assertEqual(history["prior_deployments_in_window"], 5)
        self.assertGreaterEqual(history["pages_scanned"], 2)

    def test_excludes_the_mint_being_analyzed(self):
        creator = pubkey(4)
        target_mint = pubkey(50)
        now = 2_000_000_000.0
        rpc = FakeRPC()
        rpc.signatures = [
            {"signature": "genesis", "slot": 1, "blockTime": int(now - 60), "err": None}
        ]
        rpc.transactions["genesis"] = dbc_transaction(
            [dbc_instruction(creator=creator, base_mint=target_mint)]
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
