import base64
import struct
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from chainseer_pumpfun_provenance import PUMP_AMM_PROGRAM_ID, PUMP_PROGRAM_ID, _b58encode
from chainseer_meteora_provenance import (
    DAMM_V2_PROGRAM_ID,
    DBC_PROGRAM_ID,
    DLMM_PROGRAM_ID,
    _DBC_POOL_CREATION_DISCRIMINATORS,
)
from chainseer_solana_public import (
    SolanaMintError,
    SolanaPublicAnalyzer,
    TOKEN_PROGRAM_ID,
    validate_solana_mint,
)
from chainseer_wallet_convergence import WalletConvergenceTracker


MINT = "So11111111111111111111111111111111111111112"


def b58_bytes(seed: int) -> bytes:
    return bytes([seed]) * 32


def borsh_string(value: str) -> bytes:
    raw = value.encode()
    return struct.pack("<I", len(raw)) + raw


def create_event_payload(
    *, mint: bytes, creator: bytes, symbol: str = "FARM", block_time: int
) -> str:
    """Encodes a Pump.fun CreateEvent -- the inverse of
    chainseer_pumpfun_provenance.decode_create_event(), for building
    FakeRPC transaction fixtures. Only the fields provenance scanning
    actually reads are meaningful; the rest are structurally valid
    placeholders."""
    from chainseer_pumpfun_provenance import PUMP_CREATE_EVENT_DISCRIMINATOR

    raw = (
        PUMP_CREATE_EVENT_DISCRIMINATOR
        + borsh_string("Farm Token")
        + borsh_string(symbol)
        + borsh_string("https://example.invalid/meta.json")
        + mint
        + b58_bytes(200)  # bonding_curve, unused by provenance scanning
        + b58_bytes(201)  # user, unused by provenance scanning
        + creator
        + struct.pack("<q", block_time)
        + struct.pack("<Q", 1_073_000_000_000_000)
        + struct.pack("<Q", 30_000_000_000)
        + struct.pack("<Q", 793_100_000_000_000)
        + struct.pack("<Q", 1_000_000_000_000_000)
        + b58_bytes(202)  # token_program, unused by provenance scanning
        + b"\0\0"
    )
    return base64.b64encode(raw).decode()


def signature_row(signature: str, *, slot: int, block_time: int, err=None) -> dict:
    return {"signature": signature, "slot": slot, "blockTime": block_time, "err": err}


def create_transaction(*, program_data_b64: str) -> dict:
    return {
        "transaction": {
            "message": {"accountKeys": [{"pubkey": PUMP_PROGRAM_ID}]}
        },
        "meta": {
            "err": None,
            "logMessages": ["Program data: " + program_data_b64],
        },
    }


def dbc_instruction(
    *, variant="spl_token", creator=None, base_mint=None, pool=None, program_id=None
) -> dict:
    """A "partially decoded" jsonParsed instruction for a Meteora DBC pool-
    creation call -- accounts is already a list of resolved pubkey strings
    (Solana RPC's jsonParsed behavior for a program it has no built-in
    parser for), matching what chainseer_meteora_provenance expects."""
    discriminator = next(
        raw for raw, name in _DBC_POOL_CREATION_DISCRIMINATORS.items()
        if name == variant
    )
    accounts = [
        _b58encode(bytes([1]) * 32),  # config
        _b58encode(bytes([2]) * 32),  # pool_authority
        creator or _b58encode(bytes([3]) * 32),
        base_mint or _b58encode(bytes([4]) * 32),
        _b58encode(bytes([5]) * 32),  # quote_mint
        pool or _b58encode(bytes([6]) * 32),
        _b58encode(bytes([7]) * 32),  # base_vault
        _b58encode(bytes([8]) * 32),  # quote_vault
    ]
    return {
        "programId": program_id or DBC_PROGRAM_ID,
        "accounts": accounts,
        "data": _b58encode(discriminator + b"\x00" * 8),
    }


def dbc_transaction(instructions: list[dict], *, err=None) -> dict:
    return {
        "transaction": {"message": {"instructions": instructions}},
        "meta": {"err": err, "innerInstructions": []},
    }


class FakeRPC:
    def __init__(self, *, mint_authority=None, pool_owner=None):
        self.mint_authority = mint_authority
        # Empty by default -- resolve_genesis_creator() then correctly
        # falls back to "no verified Pump.fun provenance" for every test
        # that doesn't explicitly populate these, preserving the
        # pre-existing unresolved/unknown expectations.
        self.signatures: list[dict] = []
        self.transactions: dict[str, dict] = {}
        self.creator_token_accounts: list[dict] = []
        # Only used when get_account_info is called with encoding="base64"
        # (the lp_lock pool-custody check) -- the mint-info lookup always
        # uses jsonParsed, so the two never collide on the same fixture.
        self.pool_owner = pool_owner

    def get_slot(self):
        return 321

    def get_account_info(self, address, *, encoding="jsonParsed"):
        if encoding == "base64":
            return {"context": {"slot": 322}, "value": {"owner": self.pool_owner}}
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

    def get_signatures_for_address(self, address, *, limit=25, before=None):
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

    def get_token_accounts_by_owner(self, owner, mint):
        return {"value": self.creator_token_accounts}


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


class ConfigurableDexScreener:
    def __init__(
        self,
        *,
        dex_id="pumpswap",
        pair_address="pool-1",
        buys=120,
        sells=100,
        volume_h24=65_000.0,
        liquidity_usd=150_000.0,
        labels=None,
    ):
        self.dex_id = dex_id
        self.pair_address = pair_address
        self.buys = buys
        self.sells = sells
        self.volume_h24 = volume_h24
        self.liquidity_usd = liquidity_usd
        self.labels = labels or []

    def token_pairs(self, mint):
        return [
            {
                "chainId": "solana",
                "dexId": self.dex_id,
                "labels": self.labels,
                "pairAddress": self.pair_address,
                "url": "https://dexscreener.com/solana/" + self.pair_address,
                "baseToken": {"address": mint, "name": "Example", "symbol": "EX"},
                "quoteToken": {
                    "address": "quote",
                    "name": "USD Coin",
                    "symbol": "USDC",
                },
                "priceUsd": "0.25",
                "marketCap": 2_500_000,
                "liquidity": {"usd": self.liquidity_usd},
                "volume": {"h24": self.volume_h24},
                "pairCreatedAt": int((time.time() - 10 * 86400) * 1000),
                "txns": {"h24": {"buys": self.buys, "sells": self.sells}},
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


class ProvenanceScoringTests(unittest.TestCase):
    """Solana's public analyzer used to hardcode creator_risk/deployer/
    lp_lock/wash_trading at a flat 50.0 -- these exercise the real checks
    that replaced those stubs, matching chainseer.py's EVM engine having
    genuine implementations for the same four dimensions."""

    def test_clean_creator_scores_well_and_clears_unknowns(self):
        now = int(time.time())
        mint_addr = _b58encode(b58_bytes(50))
        creator_bytes = b58_bytes(99)
        creator_addr = _b58encode(creator_bytes)
        genesis_block_time = now - 3600

        rpc = FakeRPC()
        rpc.signatures = [
            signature_row("genesis", slot=1, block_time=genesis_block_time)
        ]
        rpc.transactions["genesis"] = create_transaction(
            program_data_b64=create_event_payload(
                mint=b58_bytes(50),
                creator=creator_bytes,
                symbol="TARGET",
                block_time=genesis_block_time,
            )
        )
        rpc.creator_token_accounts = [
            {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {"tokenAmount": {"amount": "1000"}}
                        }
                    }
                }
            }
        ]

        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=FakeDexScreener(),
            jupiter=FakeJupiter(),
        ).analyze_token(mint_addr)

        scores = report["analysis"]["component_scores"]
        self.assertEqual(scores["deployer"], 85.0)
        self.assertEqual(scores["creator_risk"], 80.0)
        self.assertNotIn("deployer", report["analysis"]["uncertain_components"])
        self.assertNotIn(
            "creator_risk", report["analysis"]["uncertain_components"]
        )
        self.assertEqual(
            report["data"]["deployer"]["creator"], creator_addr
        )
        self.assertTrue(report["coverage"]["creator_attribution"])

    def test_deployer_hard_stops_on_industrialized_cadence(self):
        now = int(time.time())
        mint_bytes = b58_bytes(50)
        mint_addr = _b58encode(mint_bytes)
        creator_bytes = b58_bytes(99)

        rows = []
        rpc = FakeRPC()
        for i in range(11):
            sig = f"farm-{i}"
            block_time = now - 60 * (i + 1)
            rows.append(signature_row(sig, slot=1000 + i, block_time=block_time))
            rpc.transactions[sig] = create_transaction(
                program_data_b64=create_event_payload(
                    mint=b58_bytes(60 + i),
                    creator=creator_bytes,
                    symbol=f"FARM{i}",
                    block_time=block_time,
                )
            )
        genesis_block_time = now - 3600
        rows.append(signature_row("genesis", slot=1, block_time=genesis_block_time))
        rpc.transactions["genesis"] = create_transaction(
            program_data_b64=create_event_payload(
                mint=mint_bytes,
                creator=creator_bytes,
                symbol="TARGET",
                block_time=genesis_block_time,
            )
        )
        rpc.signatures = rows

        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=FakeDexScreener(),
            jupiter=FakeJupiter(),
        ).analyze_token(mint_addr)

        self.assertEqual(report["analysis"]["component_scores"]["deployer"], 10.0)
        red_flags = report["analysis"]["red_flags"]
        self.assertTrue(
            any("industrialized" in flag.lower() for flag in red_flags), red_flags
        )
        self.assertEqual(report["analysis"]["risk_level"], "High")

    def test_creator_risk_flags_high_supply_concentration(self):
        now = int(time.time())
        mint_bytes = b58_bytes(50)
        mint_addr = _b58encode(mint_bytes)
        creator_bytes = b58_bytes(99)
        genesis_block_time = now - 3600

        rpc = FakeRPC()
        rpc.signatures = [
            signature_row("genesis", slot=1, block_time=genesis_block_time)
        ]
        rpc.transactions["genesis"] = create_transaction(
            program_data_b64=create_event_payload(
                mint=mint_bytes,
                creator=creator_bytes,
                symbol="TARGET",
                block_time=genesis_block_time,
            )
        )
        # Mint fixture's supply is 1_000_000_000_000_000 raw -- 6% of it.
        rpc.creator_token_accounts = [
            {
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "tokenAmount": {"amount": "60000000000000"}
                            }
                        }
                    }
                }
            }
        ]

        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=FakeDexScreener(),
            jupiter=FakeJupiter(),
        ).analyze_token(mint_addr)

        self.assertEqual(
            report["analysis"]["component_scores"]["creator_risk"], 20.0
        )
        self.assertTrue(
            any(
                "holds" in flag and "%" in flag
                for flag in report["analysis"]["yellow_flags"]
            )
        )

    def test_lp_lock_verifies_program_owned_pumpswap_pool(self):
        rpc = FakeRPC(pool_owner=PUMP_AMM_PROGRAM_ID)
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=ConfigurableDexScreener(pair_address="pool-good"),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(
            report["data"]["lp_lock"]["state"], "protocol_secured"
        )
        self.assertEqual(report["analysis"]["component_scores"]["lp_lock"], 95.0)
        self.assertTrue(report["coverage"]["liquidity_custody"])
        self.assertTrue(
            any("protocol secured" in flag for flag in report["analysis"]["green_flags"])
        )

    def test_lp_lock_flags_unexpected_pool_owner(self):
        rpc = FakeRPC(pool_owner=TOKEN_PROGRAM_ID)
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=ConfigurableDexScreener(pair_address="pool-bad"),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(report["analysis"]["component_scores"]["lp_lock"], 15.0)
        red_flags = report["analysis"]["red_flags"]
        self.assertTrue(
            any("not owned by the canonical" in flag for flag in red_flags),
            red_flags,
        )

    def test_wash_trading_flags_tiny_balanced_trades(self):
        rpc = FakeRPC()
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=ConfigurableDexScreener(
                buys=100, sells=100, volume_h24=50.0
            ),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(
            report["analysis"]["component_scores"]["wash_trading"], 20.0
        )
        self.assertTrue(
            any(
                "wash trading" in flag.lower()
                for flag in report["analysis"]["yellow_flags"]
            )
        )

    def test_wash_trading_healthy_pattern_scores_well(self):
        rpc = FakeRPC()
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=FakeDexScreener(),  # buys=120 sells=100 volume=65000
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(
            report["analysis"]["component_scores"]["wash_trading"], 70.0
        )
        self.assertNotIn(
            "wash_trading", report["analysis"]["uncertain_components"]
        )


class MeteoraProvenanceWiringTests(unittest.TestCase):
    """Meteora Dynamic Bonding Curve is tried as a fallback when Pump.fun
    provenance doesn't resolve, and DAMM v2 / DLMM pools get the same
    on-chain custody verification PumpSwap pools already had."""

    def test_falls_back_to_meteora_dbc_when_no_pump_fun_provenance(self):
        now = int(time.time())
        mint_addr = _b58encode(bytes([50]) * 32)
        creator_addr = _b58encode(bytes([99]) * 32)

        rpc = FakeRPC()
        rpc.signatures = [
            signature_row("genesis", slot=1, block_time=now - 3600)
        ]
        rpc.transactions["genesis"] = dbc_transaction(
            [dbc_instruction(creator=creator_addr, base_mint=mint_addr)]
        )
        rpc.creator_token_accounts = [
            {
                "account": {
                    "data": {
                        "parsed": {"info": {"tokenAmount": {"amount": "1000"}}}
                    }
                }
            }
        ]

        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=FakeDexScreener(),
            jupiter=FakeJupiter(),
        ).analyze_token(mint_addr)

        self.assertEqual(report["data"]["deployer"]["launch_venue"], "meteora_dbc")
        self.assertEqual(report["data"]["deployer"]["creator"], creator_addr)
        self.assertEqual(report["data"]["creator"]["launch_venue"], "meteora_dbc")
        self.assertTrue(report["coverage"]["creator_attribution"])
        self.assertNotIn("deployer", report["analysis"]["uncertain_components"])

    def test_lp_lock_verifies_program_owned_damm_v2_pool(self):
        rpc = FakeRPC(pool_owner=DAMM_V2_PROGRAM_ID)
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=ConfigurableDexScreener(
                dex_id="meteora", labels=["DYN2"], pair_address="pool-good"
            ),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(report["data"]["lp_lock"]["state"], "protocol_secured")
        self.assertEqual(report["data"]["lp_lock"]["amm_version"], "meteora_damm_v2")
        self.assertEqual(report["analysis"]["component_scores"]["lp_lock"], 95.0)
        self.assertTrue(report["coverage"]["liquidity_custody"])

    def test_lp_lock_verifies_program_owned_dlmm_pool(self):
        rpc = FakeRPC(pool_owner=DLMM_PROGRAM_ID)
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=ConfigurableDexScreener(
                dex_id="meteora", labels=["DLMM"], pair_address="pool-good"
            ),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(report["data"]["lp_lock"]["state"], "protocol_secured")
        self.assertEqual(report["data"]["lp_lock"]["amm_version"], "meteora_dlmm")
        self.assertEqual(report["analysis"]["component_scores"]["lp_lock"], 95.0)

    def test_lp_lock_flags_unexpected_owner_for_meteora_pool(self):
        rpc = FakeRPC(pool_owner=TOKEN_PROGRAM_ID)
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=ConfigurableDexScreener(
                dex_id="meteora", labels=["DYN2"], pair_address="pool-bad"
            ),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(report["data"]["lp_lock"]["state"], "custody_unexpected_owner")
        self.assertEqual(report["analysis"]["component_scores"]["lp_lock"], 15.0)
        red_flags = report["analysis"]["red_flags"]
        self.assertTrue(
            any("not owned by the canonical" in flag for flag in red_flags),
            red_flags,
        )

    def test_lp_lock_stays_unverified_for_unrecognized_meteora_pool_type(self):
        """An older/unlabeled Meteora pool variant this module has no
        verified program ID for must stay honestly unresolved rather than
        risk a false custody claim."""
        rpc = FakeRPC(pool_owner=DAMM_V2_PROGRAM_ID)
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=rpc,
            dexscreener=ConfigurableDexScreener(
                dex_id="meteora", labels=["DYN"], pair_address="pool-legacy"
            ),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertEqual(report["data"]["lp_lock"]["state"], "custody_unverified")
        self.assertFalse(report["coverage"]["liquidity_custody"])


class AlertWiringTests(unittest.TestCase):
    """analyze_token() must push a best-effort alert on a hard-stop
    decision, and must never let that alert call affect the report even
    if alerting itself is misconfigured -- alerting is fire-and-forget."""

    def test_hard_stop_triggers_an_alert(self):
        with patch(
            "chainseer_solana_public.alert_on_decision"
        ) as mocked:
            mocked.return_value = {"sent": False, "reason": "no_webhook_configured"}
            report = SolanaPublicAnalyzer(
                "https://example.invalid",
                rpc=FakeRPC(mint_authority="9" * 44),
                dexscreener=FakeDexScreener(),
                jupiter=FakeJupiter(),
            ).analyze_token(MINT)

        mocked.assert_called_once()
        kwargs = mocked.call_args.kwargs
        self.assertEqual(kwargs["chain"], "solana")
        self.assertEqual(kwargs["token_address"], MINT)
        self.assertIn("mint_authority_active", kwargs["hard_stops"])
        self.assertEqual(report["analysis"]["risk_level"], "High")

    def test_clean_token_still_calls_alert_hook_with_no_hard_stops(self):
        # alert_on_decision itself no-ops on empty hard_stops with no
        # threshold configured -- this just proves the call site always
        # reports the real (empty) list rather than skipping the call.
        with patch(
            "chainseer_solana_public.alert_on_decision"
        ) as mocked:
            SolanaPublicAnalyzer(
                "https://example.invalid",
                rpc=FakeRPC(),
                dexscreener=FakeDexScreener(),
                jupiter=FakeJupiter(),
            ).analyze_token(MINT)

        mocked.assert_called_once()
        self.assertEqual(mocked.call_args.kwargs["hard_stops"], [])


class WalletConvergenceWiringTests(unittest.TestCase):
    """The public on-demand analyzer is stateless per query, but when given
    a WalletConvergenceTracker it builds up a real cross-query wallet
    registry over time -- same signal as the autotrader, opt-in via the
    convergence_tracker constructor param, and always additive-only."""

    def _tracker(self, temp_dir):
        return WalletConvergenceTracker(Path(temp_dir) / "wallet_convergence.json")

    def test_without_a_tracker_convergence_is_reported_as_unavailable(self):
        report = SolanaPublicAnalyzer(
            "https://example.invalid",
            rpc=FakeRPC(),
            dexscreener=FakeDexScreener(),
            jupiter=FakeJupiter(),
        ).analyze_token(MINT)

        self.assertFalse(report["coverage"]["wallet_convergence"])
        self.assertFalse(report["data"]["wallet_convergence"]["converged"])
        self.assertIn("reason", report["data"]["wallet_convergence"])

    def test_first_query_enrolls_holders_prospectively_with_unknown_outcome(self):
        """Enrollment (observe) must happen with evidence_state=None -- the
        outcome is only known once regrade() runs with this query's verdict,
        later in the same call. Both effects land in the same call, so the
        pre-regrade state is only observable by spying on the observe() call
        itself, not by inspecting final persisted state."""
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp)
            original_observe = tracker.observe
            calls = []

            def spy_observe(wallet, mint, *, evidence_state, **kwargs):
                calls.append((wallet, mint, evidence_state))
                return original_observe(
                    wallet, mint, evidence_state=evidence_state, **kwargs
                )

            with patch.object(tracker, "observe", side_effect=spy_observe):
                SolanaPublicAnalyzer(
                    "https://example.invalid",
                    rpc=FakeRPC(),
                    dexscreener=FakeDexScreener(),
                    jupiter=FakeJupiter(),
                    convergence_tracker=tracker,
                ).analyze_token(MINT)

            # FakeRPC's get_multiple_accounts derives owners "1"*44 and
            # "2"*44 for the two largest-account rows.
            self.assertEqual(
                {(wallet, mint) for wallet, mint, _ in calls},
                {("1" * 44, MINT), ("2" * 44, MINT)},
            )
            self.assertTrue(all(state is None for _, _, state in calls))

            # regrade() then updates the SAME positions once this query's
            # own verdict is known.
            position = tracker.state["wallets"]["1" * 44]["positions"][MINT]
            self.assertEqual(position["evidence_state"], "complete_safe")

    def test_query_regrades_its_own_mint_with_this_verdicts_outcome(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp)
            report = SolanaPublicAnalyzer(
                "https://example.invalid",
                rpc=FakeRPC(),
                dexscreener=FakeDexScreener(),
                jupiter=FakeJupiter(),
                convergence_tracker=tracker,
            ).analyze_token(MINT)

            self.assertEqual(report["analysis"]["hard_stop_overrides"], [])
            position = tracker.state["wallets"]["1" * 44]["positions"][MINT]
            self.assertEqual(position["evidence_state"], "complete_safe")

    def test_hard_stopped_query_regrades_as_hard_stop_failed(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp)
            SolanaPublicAnalyzer(
                "https://example.invalid",
                rpc=FakeRPC(mint_authority="9" * 44),
                dexscreener=FakeDexScreener(),
                jupiter=FakeJupiter(),
                convergence_tracker=tracker,
            ).analyze_token(MINT)

            position = tracker.state["wallets"]["1" * 44]["positions"][MINT]
            self.assertEqual(position["evidence_state"], "hard_stop_failed")

    def test_converged_wallets_add_a_warning_never_a_hard_stop(self):
        with tempfile.TemporaryDirectory() as temp:
            tracker = self._tracker(temp)
            # Build up accuracy for the two wallets FakeRPC will surface as
            # holders of MINT, using unrelated prior mints so the floor is
            # cleared before the actual query under test.
            for wallet in ("1" * 44, "2" * 44):
                for i in range(3):
                    prior_mint = f"prior-{wallet}-{i}"
                    tracker.observe(wallet, prior_mint, evidence_state=None)
                    tracker.regrade(prior_mint, "complete_safe")

            report = SolanaPublicAnalyzer(
                "https://example.invalid",
                rpc=FakeRPC(),
                dexscreener=FakeDexScreener(),
                jupiter=FakeJupiter(),
                convergence_tracker=tracker,
            ).analyze_token(MINT)

            self.assertTrue(report["coverage"]["wallet_convergence"])
            self.assertTrue(report["data"]["wallet_convergence"]["converged"])
            self.assertEqual(report["analysis"]["hard_stop_overrides"], [])
            self.assertTrue(
                any(
                    flag.startswith("wallet_convergence_")
                    for flag in report["analysis"]["yellow_flags"]
                )
            )


if __name__ == "__main__":
    unittest.main()
