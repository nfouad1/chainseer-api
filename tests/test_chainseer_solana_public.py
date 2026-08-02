import base64
import struct
import time
import unittest

from chainseer_pumpfun_provenance import PUMP_AMM_PROGRAM_ID, PUMP_PROGRAM_ID, _b58encode
from chainseer_solana_public import (
    SolanaMintError,
    SolanaPublicAnalyzer,
    TOKEN_PROGRAM_ID,
    validate_solana_mint,
)


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
    ):
        self.dex_id = dex_id
        self.pair_address = pair_address
        self.buys = buys
        self.sells = sells
        self.volume_h24 = volume_h24
        self.liquidity_usd = liquidity_usd

    def token_pairs(self, mint):
        return [
            {
                "chainId": "solana",
                "dexId": self.dex_id,
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


if __name__ == "__main__":
    unittest.main()
