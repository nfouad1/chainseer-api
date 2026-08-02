import base64
import struct
import time
import unittest
from unittest.mock import patch

import chainseer_solana
from tests.test_chainseer_solana import FakeRPC, FakeJupiter
from chainseer_meteora_provenance import (
    DAMM_V2_PROGRAM_ID,
    DBC_PROGRAM_ID,
    DLMM_PROGRAM_ID,
    MeteoraPoolCreation,
    _DBC_POOL_CREATION_DISCRIMINATORS,
)


def b58_bytes(seed: int) -> bytes:
    return bytes([seed]) * 32


def pubkey(seed: int) -> str:
    return chainseer_solana._b58encode(b58_bytes(seed))


def dbc_pool_payload(
    *,
    owner=None,
    creator=None,
    base_mint=None,
    base_vault=None,
    quote_vault=None,
    base_reserve=0,
    quote_reserve=0,
    is_migrated=False,
    discriminator=None,
    total_size=416,
) -> dict:
    """A synthetic VirtualPool account payload matching PoolState's real
    byte layout (verified against MeteoraAg/dynamic-bonding-curve's own
    const_assert_eq! macros -- see DBC_POOL_STATE_OFFSETS). Always built at
    full size (416 bytes) then truncated to `total_size` at the end, so a
    smaller total_size simulates truncation without corrupting the fields
    that do fit."""
    body = bytearray(416)

    def put_pubkey(offset: int, raw: bytes) -> None:
        body[offset:offset + 32] = raw

    def put_u64(offset: int, value: int) -> None:
        body[offset:offset + 8] = struct.pack("<Q", value)

    offsets = chainseer_solana.DBC_POOL_STATE_OFFSETS
    put_pubkey(offsets["config"], b58_bytes(90))
    put_pubkey(offsets["creator"], creator or b58_bytes(1))
    put_pubkey(offsets["base_mint"], base_mint or b58_bytes(2))
    put_pubkey(offsets["base_vault"], base_vault or b58_bytes(3))
    put_pubkey(offsets["quote_vault"], quote_vault or b58_bytes(4))
    put_u64(offsets["base_reserve"], base_reserve)
    put_u64(offsets["quote_reserve"], quote_reserve)
    body[offsets["is_migrated"]] = 1 if is_migrated else 0
    raw = (discriminator or chainseer_solana.DBC_VIRTUAL_POOL_DISCRIMINATOR) + bytes(body[:total_size])
    return {
        "value": {
            "owner": owner or DBC_PROGRAM_ID,
            "data": [base64.b64encode(raw).decode(), "base64"],
        }
    }


def dbc_instruction(
    *,
    variant="spl_token",
    creator=None,
    base_mint=None,
    pool=None,
    program_id=None,
) -> dict:
    discriminator = next(
        raw for raw, name in _DBC_POOL_CREATION_DISCRIMINATORS.items()
        if name == variant
    )
    accounts = [
        pubkey(10),  # config
        pubkey(11),  # pool_authority
        creator or pubkey(1),
        base_mint or pubkey(2),
        pubkey(12),  # quote_mint
        pool or pubkey(3),
        pubkey(13),  # base_vault
        pubkey(14),  # quote_vault
    ]
    return {
        "programId": program_id or DBC_PROGRAM_ID,
        "accounts": accounts,
        "data": chainseer_solana._b58encode(discriminator + b"\x00" * 8),
    }


def dbc_transaction(instructions: list, *, err=None) -> dict:
    return {
        "transaction": {"message": {"instructions": instructions}},
        "meta": {"err": err, "innerInstructions": []},
    }


def meteora_pool_creation(
    *, signature="sig1", slot=1, block_time=1_700_000_000,
    mint=None, creator=None, pool=None, variant="spl_token",
) -> MeteoraPoolCreation:
    return MeteoraPoolCreation(
        signature=signature,
        slot=slot,
        block_time=block_time,
        mint=mint or pubkey(2),
        creator=creator or pubkey(1),
        pool=pool or pubkey(3),
        variant=variant,
    )


class MeteoraCandidateFactoryTests(unittest.TestCase):
    def test_maps_spl_token_variant(self):
        creation = meteora_pool_creation(variant="spl_token")
        candidate = chainseer_solana.SolanaLaunchCandidate.from_meteora_pool_creation(
            creation
        )
        self.assertEqual(candidate.launch_ecosystem, "meteora_dbc")
        self.assertEqual(candidate.mint, creation.mint)
        self.assertEqual(candidate.creator, creation.creator)
        self.assertEqual(candidate.bonding_curve, creation.pool)
        self.assertEqual(candidate.token_program, chainseer_solana.TOKEN_PROGRAM_ID)

    def test_maps_token2022_variant(self):
        creation = meteora_pool_creation(variant="token2022")
        candidate = chainseer_solana.SolanaLaunchCandidate.from_meteora_pool_creation(
            creation
        )
        self.assertEqual(candidate.token_program, chainseer_solana.TOKEN_2022_PROGRAM_ID)

    def test_round_trips_through_to_dict_and_from_dict(self):
        creation = meteora_pool_creation()
        candidate = chainseer_solana.SolanaLaunchCandidate.from_meteora_pool_creation(
            creation
        )
        restored = chainseer_solana.SolanaLaunchCandidate.from_dict(candidate.to_dict())
        self.assertEqual(restored, candidate)


class DecodeDbcPoolStateTests(unittest.TestCase):
    def test_decodes_valid_pool_state(self):
        payload = dbc_pool_payload(
            creator=b58_bytes(5),
            base_mint=b58_bytes(6),
            base_vault=b58_bytes(7),
            base_reserve=999_000_000,
            quote_reserve=42,
            is_migrated=True,
        )
        decoded = chainseer_solana.SolanaRiskAnalyzer._decode_dbc_pool_state(payload)
        self.assertEqual(decoded["owner"], DBC_PROGRAM_ID)
        self.assertEqual(decoded["creator"], chainseer_solana._b58encode(b58_bytes(5)))
        self.assertEqual(decoded["base_mint"], chainseer_solana._b58encode(b58_bytes(6)))
        self.assertEqual(decoded["base_vault"], chainseer_solana._b58encode(b58_bytes(7)))
        self.assertEqual(decoded["base_reserve"], 999_000_000)
        self.assertEqual(decoded["quote_reserve"], 42)
        self.assertTrue(decoded["is_migrated"])

    def test_not_migrated_flag_decodes_false(self):
        payload = dbc_pool_payload(is_migrated=False)
        decoded = chainseer_solana.SolanaRiskAnalyzer._decode_dbc_pool_state(payload)
        self.assertFalse(decoded["is_migrated"])

    def test_rejects_wrong_discriminator(self):
        payload = dbc_pool_payload(discriminator=b"\x00" * 8)
        with self.assertRaises(chainseer_solana.InfrastructureIndeterminateError):
            chainseer_solana.SolanaRiskAnalyzer._decode_dbc_pool_state(payload)

    def test_rejects_truncated_payload(self):
        payload = dbc_pool_payload(total_size=100)
        with self.assertRaises(chainseer_solana.InfrastructureIndeterminateError):
            chainseer_solana.SolanaRiskAnalyzer._decode_dbc_pool_state(payload)

    def test_rejects_missing_account_data(self):
        with self.assertRaises(chainseer_solana.InfrastructureIndeterminateError):
            chainseer_solana.SolanaRiskAnalyzer._decode_dbc_pool_state({"value": {}})


class MeteoraObserverTests(unittest.TestCase):
    class FakeRPC:
        def __init__(self):
            self.signatures: list[dict] = []
            self.transactions: dict[str, dict] = {}

        def get_signatures(self, _address, *, limit, until=None, before=None):
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

    def _observer(self, temp_dir, rpc):
        ledger = chainseer_solana.HashLedger(temp_dir / "observation_events.jsonl")
        return chainseer_solana.MeteoraObserver(rpc, temp_dir, ledger)

    def test_sync_discovers_and_catalogs_new_pool_creation(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rpc = self.FakeRPC()
            mint = pubkey(20)
            creator = pubkey(21)
            rpc.signatures = [
                {"signature": "sig-a", "slot": 5, "blockTime": 1_700_000_000, "err": None}
            ]
            rpc.transactions["sig-a"] = dbc_transaction(
                [dbc_instruction(creator=creator, base_mint=mint)]
            )
            observer = self._observer(root, rpc)

            with patch("chainseer_solana.time.time", return_value=1_700_000_120):
                discovered = observer.sync()

            self.assertEqual(len(discovered), 1)
            self.assertEqual(discovered[0].mint, mint)
            self.assertEqual(discovered[0].creator, creator)
            self.assertEqual(discovered[0].launch_ecosystem, "meteora_dbc")

            cached = observer.by_mint(mint)
            self.assertIsNotNone(cached)
            self.assertEqual(cached.mint, mint)

            recent = observer.recent(10)
            self.assertEqual([c.mint for c in recent], [mint])

    def test_sync_ignores_failed_transactions(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rpc = self.FakeRPC()
            rpc.signatures = [
                {"signature": "sig-fail", "slot": 5, "blockTime": 1_700_000_000, "err": "x"}
            ]
            observer = self._observer(root, rpc)
            discovered = observer.sync()
            self.assertEqual(discovered, [])

    def test_resolve_candidate_falls_back_to_mint_history_on_catalog_miss(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            rpc = self.FakeRPC()
            mint = pubkey(30)
            creator = pubkey(31)
            rpc.signatures = [
                {"signature": "genesis", "slot": 1, "blockTime": 1_700_000_000, "err": None}
            ]
            rpc.transactions["genesis"] = dbc_transaction(
                [dbc_instruction(creator=creator, base_mint=mint)]
            )
            observer = self._observer(root, rpc)

            with patch("chainseer_solana.time.time", return_value=1_700_000_120):
                found = observer.resolve_candidate(mint, page_size=5)

                self.assertIsNotNone(found)
                self.assertEqual(found.mint, mint)
                self.assertEqual(found.creator, creator)
                # Second call should hit the now-populated catalog, not rescan.
                rpc.signatures = []
                cached = observer.resolve_candidate(mint, page_size=5)
            self.assertIsNotNone(cached)


class MeteoraCanonicalPoolEvidenceTests(unittest.TestCase):
    class StubRPC:
        def __init__(self, *, pool_owner=None):
            self.pool_owner = pool_owner

        def get_account_info(self, address, *, encoding="jsonParsed"):
            return {"value": {"owner": self.pool_owner}}

    class StubDexScreener:
        def __init__(self, pairs):
            self.pairs = pairs

        def token_pairs(self, mint):
            return self.pairs

    def _pair(self, *, mint, pool_address, labels, liquidity_usd=10_000):
        return {
            "chainId": "solana",
            "dexId": "meteora",
            "pairAddress": pool_address,
            "labels": labels,
            "baseToken": {"address": mint},
            "quoteToken": {"address": chainseer_solana.WRAPPED_SOL_MINT},
            "liquidity": {"usd": liquidity_usd},
        }

    def test_resolves_damm_v2_pool_with_owner_verified(self):
        mint = pubkey(40)
        pool_address = pubkey(41)
        rpc = self.StubRPC(pool_owner=DAMM_V2_PROGRAM_ID)
        dexscreener = self.StubDexScreener(
            [self._pair(mint=mint, pool_address=pool_address, labels=["DYN2"])]
        )
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            rpc, chainseer_solana.JupiterClient(None), dexscreener=dexscreener
        )
        evidence = analyzer._meteora_canonical_pool_evidence(mint)
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence["pool"], pool_address)
        self.assertEqual(evidence["amm_version"], "meteora_damm_v2")

    def test_resolves_dlmm_pool_with_owner_verified(self):
        mint = pubkey(42)
        pool_address = pubkey(43)
        rpc = self.StubRPC(pool_owner=DLMM_PROGRAM_ID)
        dexscreener = self.StubDexScreener(
            [self._pair(mint=mint, pool_address=pool_address, labels=["DLMM"])]
        )
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            rpc, chainseer_solana.JupiterClient(None), dexscreener=dexscreener
        )
        evidence = analyzer._meteora_canonical_pool_evidence(mint)
        self.assertIsNotNone(evidence)
        self.assertEqual(evidence["amm_version"], "meteora_dlmm")

    def test_returns_none_when_pool_owner_does_not_match_claimed_program(self):
        """A pool address DexScreener labels DYN2 but that isn't actually
        owned by the DAMM v2 program on-chain must not be trusted -- same
        verification level as PumpSwap's own canonical-pool check."""
        mint = pubkey(44)
        pool_address = pubkey(45)
        rpc = self.StubRPC(pool_owner=chainseer_solana.TOKEN_PROGRAM_ID)
        dexscreener = self.StubDexScreener(
            [self._pair(mint=mint, pool_address=pool_address, labels=["DYN2"])]
        )
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            rpc, chainseer_solana.JupiterClient(None), dexscreener=dexscreener
        )
        evidence = analyzer._meteora_canonical_pool_evidence(mint)
        self.assertIsNone(evidence)

    def test_returns_none_for_unrecognized_pool_label(self):
        mint = pubkey(46)
        rpc = self.StubRPC(pool_owner=DAMM_V2_PROGRAM_ID)
        dexscreener = self.StubDexScreener(
            [self._pair(mint=mint, pool_address=pubkey(47), labels=["DYN"])]
        )
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            rpc, chainseer_solana.JupiterClient(None), dexscreener=dexscreener
        )
        self.assertIsNone(analyzer._meteora_canonical_pool_evidence(mint))

    def test_returns_none_when_no_pairs_at_all(self):
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            self.StubRPC(), chainseer_solana.JupiterClient(None),
            dexscreener=self.StubDexScreener([]),
        )
        self.assertIsNone(analyzer._meteora_canonical_pool_evidence(pubkey(48)))


class AnalyzeMeteoraEcosystemTests(unittest.TestCase):
    """End-to-end analyze() coverage for meteora_dbc candidates, mirroring
    the existing Pump.fun coverage but exercising the ecosystem branch."""

    class FakeJupiter:
        def token_info(self, mint):
            return {"id": mint, "symbol": "MTX"}

        def quote(self, input_mint, output_mint, amount):
            return chainseer_solana.JupiterQuote(
                input_mint, output_mint, amount, int(amount * 0.9),
                "metis", 1.0, 10, "request", "quote_only", {},
            )

        def roundtrip(self, mint, amount):
            return {
                "buy": {"out_amount": amount * 2, "price_impact_pct": 1.0, "router": "metis"},
                "sell": {"router": "metis"},
                "roundtrip_retention_pct": 96.0,
            }

    class FakeDexScreener:
        def __init__(self, pairs=None):
            self.pairs = pairs or []

        def token_pairs(self, mint):
            return self.pairs

    def _mint_info_response(self, mint):
        return {
            "value": {
                "owner": chainseer_solana.TOKEN_PROGRAM_ID,
                "data": {
                    "parsed": {
                        "type": "mint",
                        "info": {
                            "decimals": 6,
                            "supply": "1000000000000000",
                            "mintAuthority": None,
                            "freezeAuthority": None,
                            "extensions": [],
                        },
                    }
                },
            }
        }

    def test_pre_migration_candidate_stays_graduation_pending(self):
        mint = pubkey(60)
        creator = pubkey(61)
        pool = pubkey(62)
        candidate = chainseer_solana.SolanaLaunchCandidate.from_meteora_pool_creation(
            meteora_pool_creation(mint=mint, creator=creator, pool=pool)
        )

        class RPC:
            def get_account_info(self, address, *, encoding="jsonParsed"):
                if address == pool:
                    return dbc_pool_payload(
                        creator=b58_bytes(61),
                        base_mint=b58_bytes(60),
                        is_migrated=False,
                    )
                return self._mint_response()

            def _mint_response(self):
                return {
                    "value": {
                        "owner": chainseer_solana.TOKEN_PROGRAM_ID,
                        "data": {
                            "parsed": {
                                "type": "mint",
                                "info": {
                                    "decimals": 6,
                                    "supply": "1000000000000000",
                                    "mintAuthority": None,
                                    "freezeAuthority": None,
                                    "extensions": [],
                                },
                            }
                        },
                    }
                }

            def get_token_supply(self, _mint):
                return {"value": {"amount": "1000000000000000", "decimals": 6}}

            def get_signatures(self, _address, *, limit, until=None, before=None):
                return []

            def get_token_largest_accounts(self, _mint):
                return {"value": []}

            def get_multiple_accounts(self, addresses, *, encoding="jsonParsed"):
                return {"value": [{} for _ in addresses]}

        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            RPC(), self.FakeJupiter(), dexscreener=self.FakeDexScreener()
        )
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate)

        self.assertFalse(decision.graduation["completion_verified_on_chain"])
        self.assertFalse(decision.shadow_entry_allowed)
        self.assertIn(
            decision.admission_state,
            {"graduation_pending", "market_age_pending"},
        )

    def test_migrated_candidate_resolves_canonical_pool_and_can_enter(self):
        mint = pubkey(70)
        creator = pubkey(71)
        pool = pubkey(72)
        damm_pool_address = pubkey(73)
        candidate = chainseer_solana.SolanaLaunchCandidate.from_meteora_pool_creation(
            meteora_pool_creation(mint=mint, creator=creator, pool=pool)
        )

        pair = {
            "chainId": "solana",
            "dexId": "meteora",
            "pairAddress": damm_pool_address,
            "labels": ["DYN2"],
            "baseToken": {"address": mint},
            "quoteToken": {"address": chainseer_solana.WRAPPED_SOL_MINT},
            "liquidity": {"usd": 50_000},
            "priceUsd": "0.01",
            "marketCap": 500_000,
            "pairCreatedAt": int((time.time() - 3600) * 1000),
            "txns": {"h24": {"buys": 50, "sells": 40}},
            "volume": {"h24": 20_000},
        }

        class RPC:
            def get_account_info(self, address, *, encoding="jsonParsed"):
                if address == pool:
                    return dbc_pool_payload(
                        creator=b58_bytes(71),
                        base_mint=b58_bytes(70),
                        is_migrated=True,
                        base_reserve=0,
                    )
                if address == damm_pool_address:
                    return {"value": {"owner": DAMM_V2_PROGRAM_ID}}
                return {
                    "value": {
                        "owner": chainseer_solana.TOKEN_PROGRAM_ID,
                        "data": {
                            "parsed": {
                                "type": "mint",
                                "info": {
                                    "decimals": 6,
                                    "supply": "1000000000000000",
                                    "mintAuthority": None,
                                    "freezeAuthority": None,
                                    "extensions": [],
                                },
                            }
                        },
                    }
                }

            def get_token_supply(self, _mint):
                return {"value": {"amount": "1000000000000000", "decimals": 6}}

            def get_signatures(self, _address, *, limit, until=None, before=None):
                return []

            def get_token_largest_accounts(self, _mint):
                return {
                    "value": [
                        {"address": "holder-1", "amount": "10000000000000"},
                        {"address": "holder-2", "amount": "5000000000000"},
                    ]
                }

            def get_multiple_accounts(self, addresses, *, encoding="jsonParsed"):
                return {
                    "value": [
                        {"data": {"parsed": {"info": {"owner": f"owner-{i}"}}}}
                        for i in range(len(addresses))
                    ]
                }

        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            RPC(), self.FakeJupiter(), dexscreener=self.FakeDexScreener([pair])
        )
        with patch("chainseer_solana.time.time", return_value=1_700_003_700):
            decision = analyzer.analyze(candidate)

        self.assertTrue(decision.graduation["completion_verified_on_chain"])
        self.assertTrue(decision.graduation["canonical_pool_verified_on_chain"])
        self.assertEqual(
            decision.graduation["canonical_pool"]["amm_version"], "meteora_damm_v2"
        )
        self.assertTrue(decision.graduation["secondary_market_observed"])
        self.assertNotIn("bonding_curve_owner_mismatch", decision.hard_stops)

    def test_bonding_curve_owner_mismatch_is_a_hard_stop(self):
        mint = pubkey(80)
        creator = pubkey(81)
        pool = pubkey(82)
        candidate = chainseer_solana.SolanaLaunchCandidate.from_meteora_pool_creation(
            meteora_pool_creation(mint=mint, creator=creator, pool=pool)
        )

        class RPC:
            def get_account_info(self, address, *, encoding="jsonParsed"):
                if address == pool:
                    return dbc_pool_payload(owner=chainseer_solana.TOKEN_PROGRAM_ID)
                return {
                    "value": {
                        "owner": chainseer_solana.TOKEN_PROGRAM_ID,
                        "data": {
                            "parsed": {
                                "type": "mint",
                                "info": {
                                    "decimals": 6,
                                    "supply": "1000000000000000",
                                    "mintAuthority": None,
                                    "freezeAuthority": None,
                                    "extensions": [],
                                },
                            }
                        },
                    }
                }

            def get_token_supply(self, _mint):
                return {"value": {"amount": "1000000000000000", "decimals": 6}}

            def get_signatures(self, _address, *, limit, until=None, before=None):
                return []

            def get_token_largest_accounts(self, _mint):
                return {"value": []}

            def get_multiple_accounts(self, addresses, *, encoding="jsonParsed"):
                return {"value": [{} for _ in addresses]}

        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            RPC(), self.FakeJupiter(), dexscreener=self.FakeDexScreener()
        )
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate)

        self.assertIn("bonding_curve_owner_mismatch", decision.hard_stops)


class LearnOnceDualObserverTests(unittest.TestCase):
    def test_observe_syncs_both_observers(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)

            class RPC:
                def get_signatures(self, _address, *, limit, until=None, before=None):
                    return []

                def get_account_info(self, address, *, encoding="jsonParsed"):
                    return {"value": {}}

                def get_token_supply(self, _mint):
                    return {"value": {"amount": "0", "decimals": 6}}

                def get_token_largest_accounts(self, _mint):
                    return {"value": []}

                def get_multiple_accounts(self, addresses, *, encoding="jsonParsed"):
                    return {"value": [{} for _ in addresses]}

                def health(self):
                    return {"attempts": 0, "successes": 0, "failures": 0}

            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root, rpc=RPC(),
                jupiter=chainseer_solana.JupiterClient(None),
                record_timechain=False,
            )
            with patch.object(
                engine.observer, "sync", wraps=engine.observer.sync
            ) as pump_sync, patch.object(
                engine.meteora_observer, "sync", wraps=engine.meteora_observer.sync
            ) as meteora_sync:
                engine.observe(limit=1)

            pump_sync.assert_called_once()
            meteora_sync.assert_called_once()

    @staticmethod
    def _build_engine(root):
        class RPC:
            def get_signatures(self, _address, *, limit, until=None, before=None):
                return []

            def get_account_info(self, address, *, encoding="jsonParsed"):
                return {"value": {}}

            def get_token_supply(self, _mint):
                return {"value": {"amount": "0", "decimals": 6}}

            def get_token_largest_accounts(self, _mint):
                return {"value": []}

            def get_multiple_accounts(self, addresses, *, encoding="jsonParsed"):
                return {"value": [{} for _ in addresses]}

            def health(self):
                return {"attempts": 0, "successes": 0, "failures": 0}

        return chainseer_solana.SolanaPrototypeEngine(
            root=root, rpc=RPC(),
            jupiter=chainseer_solana.JupiterClient(None),
            record_timechain=False,
        )

    def test_observe_survives_a_failing_meteora_sync(self):
        """chronosynaptic ring 230: sync() is not internally fault-tolerant,
        so a transient RPC failure on ONE observer must not abort the
        other observer's discovery or crash observe() entirely."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._build_engine(Path(temp))
            with patch.object(
                engine.meteora_observer,
                "sync",
                side_effect=chainseer_solana.InfrastructureIndeterminateError(
                    "rpc timeout"
                ),
            ), patch.object(
                engine.observer, "sync", wraps=engine.observer.sync
            ) as pump_sync:
                results = engine.observe(limit=1)  # must not raise

            pump_sync.assert_called_once()
            self.assertEqual(results, [])

    def test_observe_survives_a_failing_pumpfun_sync(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._build_engine(Path(temp))
            with patch.object(
                engine.observer,
                "sync",
                side_effect=chainseer_solana.InfrastructureIndeterminateError(
                    "rpc timeout"
                ),
            ), patch.object(
                engine.meteora_observer, "sync", wraps=engine.meteora_observer.sync
            ) as meteora_sync:
                results = engine.observe(limit=1)  # must not raise

            meteora_sync.assert_called_once()
            self.assertEqual(results, [])

    def test_learn_once_completes_and_reports_sync_error_when_meteora_sync_fails(self):
        """The rest of the cycle (recovery, graduation probing, promotion,
        reflection) must still run even when one observer's sync fails --
        before this fix, an unwrapped exception here aborted everything
        downstream, not just that observer's discovery."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._build_engine(Path(temp))
            with patch.object(
                engine.meteora_observer,
                "sync",
                side_effect=chainseer_solana.InfrastructureIndeterminateError(
                    "meteora rpc unavailable"
                ),
            ):
                summary = engine.learn_once()  # must not raise

            self.assertEqual(summary["cycle"]["new_launches_meteora_dbc"], 0)
            self.assertIsNotNone(summary["cycle"]["sync_errors"]["meteora_dbc"])
            self.assertIsNone(summary["cycle"]["sync_errors"]["pump_fun"])
            # Downstream bookkeeping still ran.
            self.assertIn("promotion", summary)
            self.assertIn("recovery", summary)

    def test_learn_once_reports_ecosystem_as_both_venues(self):
        """The summary's ecosystem label previously stayed hardcoded to
        pump_fun even after Meteora discovery was added -- a stale label
        the S43-honesty principle (ring 132) exists to prevent."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._build_engine(Path(temp))
            summary = engine.learn_once()
            self.assertEqual(summary["ecosystem"], "pump_fun+meteora_dbc")


class ReflectionIntervalScalesWithObserversTests(unittest.TestCase):
    """Chronosynaptic ring 230, perspective 2: REFLECTION_ANALYSIS_INTERVAL
    (200) was calibrated when Pump.fun was the only discovery venue. With
    Meteora also running every cycle, total analysis throughput roughly
    doubled without the checkpoint interval changing to match, so
    checkpoints started firing in about half the elapsed wall-clock time.
    _reflection_interval() scales the base interval by how many observers
    are actually active."""

    @staticmethod
    def _engine(root):
        return chainseer_solana.SolanaPrototypeEngine(
            root=root,
            rpc=FakeRPC(),
            jupiter=FakeJupiter(),
            record_timechain=False,
        )

    def test_two_observers_double_the_base_interval(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            self.assertEqual(len(engine._observers()), 2)
            self.assertEqual(
                engine._reflection_interval(),
                chainseer_solana.REFLECTION_ANALYSIS_INTERVAL * 2,
            )

    def test_reflection_status_reports_the_scaled_interval(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            state = engine.reflection_status()
            self.assertEqual(
                state["analysis_interval"],
                chainseer_solana.REFLECTION_ANALYSIS_INTERVAL * 2,
            )
            self.assertEqual(
                state["next_analysis_checkpoint"],
                chainseer_solana.REFLECTION_ANALYSIS_INTERVAL * 2,
            )

    def test_falls_back_to_base_interval_with_a_single_observer(self):
        """Scaling is observer-count-driven, not hardcoded to 2 -- confirms
        it would correctly shrink back to the original cadence if Meteora
        discovery were ever disabled/removed for an engine instance."""
        import tempfile
        from pathlib import Path
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            with patch.object(engine, "_observers", return_value=[engine.observer]):
                self.assertEqual(
                    engine._reflection_interval(),
                    chainseer_solana.REFLECTION_ANALYSIS_INTERVAL,
                )

    @staticmethod
    def _establish_zero_baseline(engine):
        """Initialize reflection_state.json via the real production
        initialization path (reflection_status()'s first-call branch),
        with analysis_events pinned at 0 so next_analysis_checkpoint ends
        up exactly at the (scaled) interval -- not offset by whatever the
        engine's own analysis history happens to already contain."""
        with patch.object(engine, "_analysis_event_count", return_value=0):
            engine.reflection_status()

    def test_checkpoint_does_not_fire_before_the_scaled_threshold(self):
        """The old (unscaled) threshold was 200 -- confirms reaching
        exactly 200 analyses does NOT fire a checkpoint anymore now that
        two observers are active; the scaled threshold is 400."""
        import tempfile
        from pathlib import Path
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            self._establish_zero_baseline(engine)
            with patch.object(
                engine,
                "_analysis_event_count",
                return_value=chainseer_solana.REFLECTION_ANALYSIS_INTERVAL,
            ), patch("chainseer_solana._send_telegram_notification"):
                result = engine._maybe_request_reflection()
            self.assertEqual(result["status"], "armed")

    def test_checkpoint_fires_at_the_scaled_threshold(self):
        import tempfile
        from pathlib import Path
        from unittest.mock import patch

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            self._establish_zero_baseline(engine)
            scaled = chainseer_solana.REFLECTION_ANALYSIS_INTERVAL * 2
            with patch.object(
                engine, "_analysis_event_count", return_value=scaled
            ), patch("chainseer_solana._send_telegram_notification"):
                result = engine._maybe_request_reflection()
            self.assertEqual(result["status"], "pending")
            self.assertEqual(
                result["pending_checkpoint"]["reason"], "analysis_interval"
            )


class RecoveryQueueEcosystemFairnessTests(unittest.TestCase):
    """Chronosynaptic ring 230, perspective 3: confirmed against real
    recovery_queue.json data before building this -- 10 Meteora items were
    stuck at attempts=0 (some for 3+ hours) while Pump.fun items reached
    up to 167 attempts, because the single global sort prioritizes
    graduation progress_pct first and Meteora's DBC curve has no
    comparable signal (see analyze()'s ecosystem branch), so every
    Meteora row structurally sorts below nearly any Pump.fun row."""

    @staticmethod
    def _engine(root):
        return chainseer_solana.SolanaPrototypeEngine(
            root=root, rpc=FakeRPC(), jupiter=FakeJupiter(),
            record_timechain=False,
        )

    @staticmethod
    def _row(mint, *, ecosystem, attempts=0, progress=None, admission="graduation_pending"):
        return {
            "mint": mint,
            "candidate": {"launch_ecosystem": ecosystem},
            "status": "pending",
            "attempts": attempts,
            "first_queued_at": "2026-08-02T00:00:00+00:00",
            "next_attempt_at": "2026-08-01T00:00:00+00:00",  # already due
            "last_admission_state": admission,
            "last_graduation_progress_pct": progress,
        }

    def _seed_queue(self, engine, rows: dict):
        chainseer_solana._atomic_json(
            engine.recovery_queue_path,
            {"schema_version": 1, "items": rows, "updated_at": None},
        )

    def test_starved_ecosystem_gets_a_slot_even_when_outranked_on_progress(self):
        """Reproduces the real starvation directly: many high-progress
        Pump.fun items plus one 0-attempts Meteora item with no progress
        signal -- the Meteora item must still be selected, not starved."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            rows = {
                f"pump-{i}": self._row(
                    f"pump-{i}", ecosystem="pump_fun", attempts=100 + i, progress=2.0 + i
                )
                for i in range(5)
            }
            rows["meteora-1"] = self._row(
                "meteora-1", ecosystem="meteora_dbc", attempts=0, progress=None
            )
            self._seed_queue(engine, rows)

            selected = engine._select_due_recovery_rows(limit=3)

            selected_mints = {row["mint"] for row in selected}
            self.assertIn(
                "meteora-1", selected_mints,
                "Meteora's only due item was starved out of the recovery budget",
            )

    def test_round_robin_gives_each_ecosystem_representation(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            rows = {}
            for i in range(4):
                rows[f"pump-{i}"] = self._row(
                    f"pump-{i}", ecosystem="pump_fun", attempts=i, progress=float(i)
                )
                rows[f"meteora-{i}"] = self._row(
                    f"meteora-{i}", ecosystem="meteora_dbc", attempts=i
                )
            self._seed_queue(engine, rows)

            selected = engine._select_due_recovery_rows(limit=4)

            ecosystems = [
                row["candidate"]["launch_ecosystem"] for row in selected
            ]
            self.assertEqual(len(selected), 4)
            self.assertEqual(ecosystems.count("pump_fun"), 2)
            self.assertEqual(ecosystems.count("meteora_dbc"), 2)

    def test_within_ecosystem_priority_order_is_unchanged(self):
        """The round-robin only changes CROSS-ecosystem fairness -- within
        one ecosystem's own rows, the original priority order (admission-
        schema migration first, then progress descending, then fewest
        attempts, then oldest-queued) must still hold."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            rows = {
                "low-progress": self._row(
                    "low-progress", ecosystem="pump_fun", attempts=0, progress=10.0
                ),
                "high-progress": self._row(
                    "high-progress", ecosystem="pump_fun", attempts=0, progress=95.0
                ),
            }
            self._seed_queue(engine, rows)

            selected = engine._select_due_recovery_rows(limit=1)

            self.assertEqual(selected[0]["mint"], "high-progress")

    def test_ecosystem_with_no_due_items_does_not_withhold_slots(self):
        """When only one ecosystem has due work, it should get the full
        budget rather than artificially reserving unused slots."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            rows = {
                f"pump-{i}": self._row(f"pump-{i}", ecosystem="pump_fun", attempts=i)
                for i in range(3)
            }
            self._seed_queue(engine, rows)

            selected = engine._select_due_recovery_rows(limit=3)

            self.assertEqual(len(selected), 3)

    def test_admission_schema_migration_rows_still_take_priority_across_both_ecosystems(self):
        """Legacy migration rows (last_admission_state missing) must still
        be cleared first regardless of which ecosystem they belong to."""
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as temp:
            engine = self._engine(Path(temp))
            rows = {
                "meteora-legacy": self._row(
                    "meteora-legacy", ecosystem="meteora_dbc", admission=None
                ),
                "pump-fresh": self._row(
                    "pump-fresh", ecosystem="pump_fun", progress=99.0
                ),
            }
            self._seed_queue(engine, rows)

            selected = engine._select_due_recovery_rows(limit=1)

            self.assertEqual(selected[0]["mint"], "meteora-legacy")


if __name__ == "__main__":
    unittest.main()
