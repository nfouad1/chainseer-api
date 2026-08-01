import base64
import json
import struct
import tempfile
import time
import unittest
from os import environ
from pathlib import Path
from unittest.mock import patch

import chainseer_solana


def b58_bytes(seed: int) -> bytes:
    return bytes([seed]) * 32


def b58_decode(value: str) -> bytes:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = 0
    for character in value:
        number = number * 58 + alphabet.index(character)
    raw = number.to_bytes((number.bit_length() + 7) // 8, "big")
    return b"\0" * (len(value) - len(value.lstrip("1"))) + raw


def borsh_string(value: str) -> bytes:
    raw = value.encode()
    return struct.pack("<I", len(raw)) + raw


def create_event_payload() -> str:
    raw = (
        chainseer_solana.PUMP_CREATE_EVENT_DISCRIMINATOR
        + borsh_string("Safe Token")
        + borsh_string("SAFE")
        + borsh_string("https://example.invalid/meta.json")
        + b58_bytes(1)
        + b58_bytes(2)
        + b58_bytes(3)
        + b58_bytes(4)
        + struct.pack("<q", 1_700_000_000)
        + struct.pack("<Q", 1_073_000_000_000_000)
        + struct.pack("<Q", 30_000_000_000)
        + struct.pack("<Q", 793_100_000_000_000)
        + struct.pack("<Q", 1_000_000_000_000_000)
        + b58_bytes(5)
        + b"\0\0"
    )
    return base64.b64encode(raw).decode()


def farm_create_event_payload(*, mint_seed: int, creator: bytes, block_time: int) -> str:
    """A CreateEvent payload for a different mint/creator, used to simulate a
    creator's prior deployment history during a _creator_history scan."""
    raw = (
        chainseer_solana.PUMP_CREATE_EVENT_DISCRIMINATOR
        + borsh_string("Farm Token")
        + borsh_string("FARM")
        + borsh_string("https://example.invalid/farm.json")
        + b58_bytes(mint_seed)
        + b58_bytes(mint_seed + 100)
        + b58_bytes(mint_seed + 200)
        + creator
        + struct.pack("<q", block_time)
        + struct.pack("<Q", 1_073_000_000_000_000)
        + struct.pack("<Q", 30_000_000_000)
        + struct.pack("<Q", 793_100_000_000_000)
        + struct.pack("<Q", 1_000_000_000_000_000)
        + b58_bytes(5)
        + b"\0\0"
    )
    return base64.b64encode(raw).decode()


def candidate(**changes):
    value = {
        "signature": "sig1",
        "slot": 123,
        "block_time": 1_700_000_000,
        "name": "Safe Token",
        "symbol": "SAFE",
        "uri": "https://example.invalid/meta.json",
        "mint": chainseer_solana._b58encode(b58_bytes(1)),
        "bonding_curve": chainseer_solana._b58encode(b58_bytes(2)),
        "user": chainseer_solana._b58encode(b58_bytes(3)),
        "creator": chainseer_solana._b58encode(b58_bytes(4)),
        "token_program": chainseer_solana.TOKEN_PROGRAM_ID,
        "virtual_token_reserves": 1_073_000_000_000_000,
        "virtual_quote_reserves": 30_000_000_000,
        "real_token_reserves": 793_100_000_000_000,
        "token_total_supply": 1_000_000_000_000_000,
    }
    value.update(changes)
    return chainseer_solana.SolanaLaunchCandidate(**value)


def curve_payload(
    item=None, *, complete=False, real_token_reserves=None
) -> dict:
    item = item or candidate()
    real_token_reserves = (
        0 if complete and real_token_reserves is None
        else (
            item.real_token_reserves
            if real_token_reserves is None
            else real_token_reserves
        )
    )
    raw = (
        chainseer_solana.PUMP_BONDING_CURVE_DISCRIMINATOR
        + struct.pack("<Q", item.virtual_token_reserves)
        + struct.pack("<Q", item.virtual_quote_reserves)
        + struct.pack("<Q", real_token_reserves)
        + struct.pack("<Q", 1_000_000)
        + struct.pack("<Q", item.token_total_supply)
        + bytes([int(complete)])
        + b58_bytes(4)
        + b"\0\0"
        + b58_bytes(0)
    )
    return {
        "value": {
            "owner": chainseer_solana.PUMP_PROGRAM_ID,
            "data": [base64.b64encode(raw).decode(), "base64"],
        }
    }


def canonical_pool_payload(item=None) -> dict:
    item = item or candidate()
    raw = (
        chainseer_solana.PUMP_AMM_POOL_DISCRIMINATOR
        + b"\xfe"
        + struct.pack("<H", 0)
        + b58_bytes(7)
        + b58_bytes(1)
        + b58_decode(chainseer_solana.WRAPPED_SOL_MINT)
        + b58_bytes(8)
        + b58_bytes(9)
        + b58_bytes(10)
        + struct.pack("<Q", 1_000_000)
    )
    return {
        "pubkey": chainseer_solana._b58encode(b58_bytes(6)),
        "account": {
            "owner": chainseer_solana.PUMP_AMM_PROGRAM_ID,
            "data": [base64.b64encode(raw).decode(), "base64"],
        },
    }


class FakeRPC:
    def __init__(
        self,
        *,
        mint_authority=None,
        freeze_authority=None,
        extensions=None,
        curve_complete=False,
        canonical_pool=True,
    ):
        self.mint_authority = mint_authority
        self.freeze_authority = freeze_authority
        self.extensions = extensions or []
        self.curve_complete = curve_complete
        self.canonical_pool = canonical_pool
        self.signatures = []
        self.transactions = {}
        self.failing_transaction_signatures: set[str] = set()

    def get_signatures(
        self, _address, *, limit, until=None, before=None
    ):
        rows = self.signatures
        if before:
            index = next(
                (i for i, row in enumerate(rows) if row.get("signature") == before),
                None,
            )
            rows = rows[index + 1:] if index is not None else []
        return rows[:limit]

    def get_transaction(self, signature):
        if signature in self.failing_transaction_signatures:
            raise chainseer_solana.InfrastructureIndeterminateError(
                f"transaction {signature} temporarily unavailable"
            )
        return self.transactions.get(signature)

    def get_account_info(self, address, *, encoding="jsonParsed"):
        item = candidate()
        if address == item.bonding_curve:
            return curve_payload(item, complete=self.curve_complete)
        return {
            "value": {
                "owner": item.token_program,
                "data": {
                    "parsed": {
                        "type": "mint",
                        "info": {
                            "decimals": 6,
                            "supply": str(item.token_total_supply),
                            "mintAuthority": self.mint_authority,
                            "freezeAuthority": self.freeze_authority,
                            "extensions": self.extensions,
                        },
                    }
                },
            }
        }

    def get_token_supply(self, _mint):
        return {"value": {"amount": "1000000000000000", "decimals": 6}}

    def get_token_largest_accounts(self, _mint):
        return {
            "value": [
                {"address": "curve-ata", "amount": "790000000000000"},
                {"address": "holder-1", "amount": "20000000000000"},
                {"address": "holder-2", "amount": "10000000000000"},
            ]
        }

    def get_program_accounts(
        self, _program_id, *, filters=None, encoding="base64"
    ):
        if self.curve_complete and self.canonical_pool:
            return [canonical_pool_payload()]
        return []

    def get_token_accounts_by_mint(self, mint, _token_program):
        return [
            {
                "pubkey": "curve-ata",
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "mint": mint,
                                "owner": candidate().bonding_curve,
                                "tokenAmount": {
                                    "amount": "790000000000000"
                                },
                            }
                        }
                    }
                },
            },
            {
                "pubkey": "holder-1",
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "mint": mint,
                                "owner": "holder-a",
                                "tokenAmount": {
                                    "amount": "20000000000000"
                                },
                            }
                        }
                    }
                },
            },
            {
                "pubkey": "holder-2",
                "account": {
                    "data": {
                        "parsed": {
                            "info": {
                                "mint": mint,
                                "owner": "holder-b",
                                "tokenAmount": {
                                    "amount": "10000000000000"
                                },
                            }
                        }
                    }
                },
            },
        ]

    def get_multiple_accounts(self, addresses, *, encoding="jsonParsed"):
        if encoding == "base64":
            return {
                "value": [
                    curve_payload(
                        complete=self.curve_complete
                    )["value"]
                    for _address in addresses
                ]
            }
        return {
            "value": [
                {"data": {"parsed": {"info": {"owner": candidate().bonding_curve}}}},
                {"data": {"parsed": {"info": {"owner": "holder-a"}}}},
                {"data": {"parsed": {"info": {"owner": "holder-b"}}}},
            ]
        }

    def health(self):
        return {"attempts": 1, "successes": 1, "failures": 0}


class FakeJupiter:
    def __init__(self, retention=90.0, assembled=True, fail=False):
        self.retention = retention
        self.assembled = assembled
        self.fail = fail

    @staticmethod
    def _quote(input_mint, output_mint, amount, out_amount):
        return chainseer_solana.JupiterQuote(
            input_mint,
            output_mint,
            amount,
            out_amount,
            "metis",
            1.0,
            10,
            "request",
            "quote_only",
            {},
        )

    def token_info(self, mint):
        if self.fail:
            raise chainseer_solana.InfrastructureIndeterminateError("jupiter down")
        return {"id": mint, "liquidity": 10_000, "organicScore": 50}

    def roundtrip(self, mint, input_lamports):
        if self.fail:
            raise chainseer_solana.InfrastructureIndeterminateError("jupiter down")
        token_amount = 1_000_000
        return {
            "buy": self._quote(
                chainseer_solana.WRAPPED_SOL_MINT, mint, input_lamports, token_amount
            ).to_dict(),
            "sell": self._quote(
                mint,
                chainseer_solana.WRAPPED_SOL_MINT,
                token_amount,
                int(input_lamports * self.retention / 100),
            ).to_dict(),
            "roundtrip_retention_pct": self.retention,
            "unsigned_buy_assembled": self.assembled,
            "assembled_buy": {} if self.assembled else None,
        }

    def quote(self, input_mint, output_mint, amount, *, assemble=False):
        if self.fail:
            raise chainseer_solana.InfrastructureIndeterminateError("jupiter down")
        return self._quote(input_mint, output_mint, amount, amount)


class FakeDexScreener:
    def __init__(self, *, observed=True, fail=False):
        self.observed = observed
        self.fail = fail
        self.calls = 0

    def token_pairs(self, mint):
        self.calls += 1
        if self.fail:
            raise chainseer_solana.InfrastructureIndeterminateError(
                "dexscreener down"
            )
        if not self.observed:
            return []
        return [
            {
                "chainId": "solana",
                "dexId": "pumpswap",
                "pairAddress": chainseer_solana._b58encode(b58_bytes(6)),
                "url": "https://dexscreener.com/solana/pair",
                "baseToken": {
                    "address": mint,
                    "name": "Safe Token",
                    "symbol": "SAFE",
                },
                "quoteToken": {
                    "address": chainseer_solana.WRAPPED_SOL_MINT,
                    "name": "Wrapped SOL",
                    "symbol": "SOL",
                },
                "priceUsd": "0.01",
                "liquidity": {"usd": 25_000},
                "marketCap": 1_000_000,
                "fdv": 1_000_000,
                "pairCreatedAt": 1_700_000_000_000,
                "txns": {"m5": {"buys": 10, "sells": 5}},
                "volume": {"m5": 1_000},
            }
        ]

    def health(self):
        return {
            "attempts": self.calls,
            "successes": self.calls if not self.fail else 0,
            "failures": self.calls if self.fail else 0,
            "cache_hits": 0,
        }


class SolanaPrototypeTests(unittest.TestCase):
    def test_atomic_json_retries_transient_windows_replace_lock(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "state.json"
            original_replace = Path.replace
            calls = 0

            def transient_lock(source, destination):
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise PermissionError("transient reader lock")
                return original_replace(source, destination)

            with patch.object(
                Path, "replace", autospec=True, side_effect=transient_lock
            ), patch("chainseer_solana.time.sleep") as sleep:
                chainseer_solana._atomic_json(path, {"ok": True})

            self.assertEqual(
                json.loads(path.read_text(encoding="utf-8")),
                {"ok": True},
            )
            self.assertEqual(calls, 2)
            sleep.assert_called_once_with(0.05)

    def test_environment_setting_uses_user_scope_when_process_is_stale(self):
        with patch.dict(
            environ, {"CHAINSEER_SOLANA_RPC_URL": ""}, clear=False
        ), patch(
            "chainseer_solana._windows_user_environment",
            return_value="https://mainnet.helius.invalid/?api-key=secret",
        ):
            value = chainseer_solana._environment_setting(
                "CHAINSEER_SOLANA_RPC_URL"
            )
        self.assertIn("mainnet.helius.invalid", value)

    def test_configured_rpc_policy_refuses_silent_public_fallback(self):
        secret = "never-store-this-key"
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc_url=(
                    "https://mainnet.helius.invalid/"
                    f"?api-key={secret}"
                ),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )
            policy = json.loads(
                (root / "rpc_policy.json").read_text(encoding="utf-8")
            )
            self.assertTrue(policy["require_configured_rpc"])
            self.assertEqual(policy["configured_provider"], "helius")
            self.assertNotIn(secret, json.dumps(policy))

            with self.assertRaises(
                chainseer_solana.ConfiguredSolanaRpcRequiredError
            ):
                chainseer_solana.SolanaPrototypeEngine(
                    root=root,
                    rpc_url=chainseer_solana.PUBLIC_SOLANA_RPC_URL,
                    jupiter=FakeJupiter(),
                    record_timechain=False,
                )

            overridden = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc_url=chainseer_solana.PUBLIC_SOLANA_RPC_URL,
                jupiter=FakeJupiter(),
                record_timechain=False,
                allow_public_rpc=True,
            )
            self.assertEqual(
                overridden.rpc.url,
                chainseer_solana.PUBLIC_SOLANA_RPC_URL,
            )

    def test_decodes_official_pump_create_event_prefix(self):
        decoded = chainseer_solana.PumpFunObserver.decode_create_event(
            create_event_payload(), signature="sig", slot=9, block_time=8
        )
        self.assertEqual(decoded.symbol, "SAFE")
        self.assertEqual(decoded.mint, chainseer_solana._b58encode(b58_bytes(1)))
        self.assertEqual(decoded.creator, chainseer_solana._b58encode(b58_bytes(4)))

    def test_ignores_non_create_program_data(self):
        encoded = base64.b64encode(b"not-create").decode()
        self.assertIsNone(
            chainseer_solana.PumpFunObserver.decode_create_event(
                encoded, signature="sig", slot=1, block_time=1
            )
        )

    def test_observer_persists_only_confirmed_pump_event(self):
        rpc = FakeRPC()
        rpc.signatures = [{"signature": "sig1", "slot": 123, "blockTime": 1_700_000_000}]
        rpc.transactions["sig1"] = {
            "blockTime": 1_700_000_000,
            "transaction": {
                "message": {
                    "accountKeys": [
                        {"pubkey": chainseer_solana.PUMP_PROGRAM_ID}
                    ]
                }
            },
            "meta": {"err": None, "logMessages": ["Program data: " + create_event_payload()]},
        }
        with tempfile.TemporaryDirectory() as temp:
            ledger = chainseer_solana.HashLedger(Path(temp) / "events.jsonl")
            observer = chainseer_solana.PumpFunObserver(rpc, temp, ledger)
            with patch("chainseer_solana.time.time", return_value=1_700_000_120):
                found = observer.sync(signature_limit=10)
            self.assertEqual(len(found), 1)
            self.assertEqual(observer.recent(1)[0].symbol, "SAFE")
            self.assertTrue(ledger.verify()[0])

    def test_resolve_candidate_finds_genesis_transaction_via_bounded_paging(self):
        mint_seed = 42
        mint = chainseer_solana._b58encode(b58_bytes(mint_seed))
        rpc = FakeRPC()
        signatures = []
        for i in range(11):
            sig = f"trade-{i}"
            block_time = 2_000_000_000 - i
            signatures.append(
                {"signature": sig, "slot": 100 + i, "blockTime": block_time, "err": None}
            )
            rpc.transactions[sig] = {
                "blockTime": block_time,
                "transaction": {
                    "message": {
                        "accountKeys": [{"pubkey": chainseer_solana.PUMP_PROGRAM_ID}]
                    }
                },
                "meta": {"err": None, "logMessages": []},
            }
        genesis_block_time = 2_000_000_000 - 11
        signatures.append(
            {
                "signature": "genesis",
                "slot": 1,
                "blockTime": genesis_block_time,
                "err": None,
            }
        )
        rpc.transactions["genesis"] = {
            "blockTime": genesis_block_time,
            "transaction": {
                "message": {
                    "accountKeys": [{"pubkey": chainseer_solana.PUMP_PROGRAM_ID}]
                }
            },
            "meta": {
                "err": None,
                "logMessages": [
                    "Program data: "
                    + farm_create_event_payload(
                        mint_seed=mint_seed,
                        creator=b58_bytes(4),
                        block_time=genesis_block_time,
                    )
                ],
            },
        }
        rpc.signatures = signatures

        with tempfile.TemporaryDirectory() as temp:
            ledger = chainseer_solana.HashLedger(Path(temp) / "events.jsonl")
            observer = chainseer_solana.PumpFunObserver(rpc, temp, ledger)
            found = observer.resolve_candidate(
                mint, max_pages=5, page_size=5, decode_last=10
            )
            self.assertIsNotNone(found)
            self.assertEqual(found.mint, mint)

            # A second lookup must be served from the catalog it just wrote,
            # not by re-scanning -- clearing rpc.signatures proves that.
            rpc.signatures = []
            cached = observer.resolve_candidate(
                mint, max_pages=5, page_size=5, decode_last=10
            )
            self.assertIsNotNone(cached)
            self.assertEqual(cached.mint, mint)

    def test_resolve_candidate_gives_up_past_max_pages(self):
        mint = chainseer_solana._b58encode(b58_bytes(43))
        rpc = FakeRPC()
        rpc.signatures = [
            {"signature": f"sig-{i}", "slot": i, "blockTime": 2_000_000_000 - i, "err": None}
            for i in range(20)
        ]
        with tempfile.TemporaryDirectory() as temp:
            ledger = chainseer_solana.HashLedger(Path(temp) / "events.jsonl")
            observer = chainseer_solana.PumpFunObserver(rpc, temp, ledger)
            found = observer.resolve_candidate(
                mint, max_pages=3, page_size=5, decode_last=10
            )
            self.assertIsNone(found)

    def test_pregraduation_safe_evidence_remains_observation_only(self):
        analyzer = chainseer_solana.SolanaRiskAnalyzer(FakeRPC(), FakeJupiter())
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate())
        self.assertEqual(decision.evidence_state, "complete_safe")
        self.assertFalse(decision.shadow_entry_allowed)
        self.assertEqual(decision.cohort, "launch_observation")
        self.assertEqual(decision.admission_state, "graduation_pending")
        self.assertFalse(decision.hard_stops)

    def test_recovery_reopens_resolved_legacy_admission_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            # A recent block_time (rather than the shared fixture's fixed
            # 2023 epoch) so this item survives analysis_index.json's
            # age-based retention pruning regardless of wall-clock time.
            item = candidate(block_time=int(time.time()) - 3600)
            legacy_decision = {
                "evidence_state": "complete_unsafe",
                "hard_stops": ["mint_authority_active"],
                "concentration": {},
                "infrastructure_errors": [],
            }
            (root / "analysis_index.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "tokens": {
                            item.mint: {
                                "candidate": item.to_dict(),
                                "decision": legacy_decision,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            (root / "recovery_queue.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "items": {
                            item.mint: {
                                "mint": item.mint,
                                "candidate": item.to_dict(),
                                "status": "resolved",
                                "attempts": 1,
                                "next_attempt_at": (
                                    "2026-07-28T18:00:00+00:00"
                                ),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )

            self.assertEqual(engine._seed_recovery_queue(), 1)
            queue = json.loads(
                (root / "recovery_queue.json").read_text(encoding="utf-8")
            )
            migrated = queue["items"][item.mint]
            self.assertEqual(migrated["status"], "pending")
            self.assertIsNone(migrated["last_admission_state"])
            self.assertNotEqual(
                migrated["next_attempt_at"],
                "2026-07-28T18:00:00+00:00",
            )
            recovered = engine._recover_indeterminate(
                limit=1, shadow_enter=False
            )
            self.assertEqual(len(recovered), 1)
            latest = json.loads(
                (root / "analysis_index.json").read_text(encoding="utf-8")
            )
            normalized = latest["tokens"][item.mint]["decision"]
            self.assertEqual(normalized["cohort"], "launch_observation")
            self.assertEqual(
                normalized["admission_state"], "graduation_pending"
            )

    def test_creator_history_scan_pages_past_unrelated_wallet_activity(self):
        """A single 30-signature page can be entirely unrelated wallet noise,
        hiding a genuine token-farm creator's prior deployments. Paging
        backwards until the lookback window is covered must recover them."""
        now = 2_000_000_000.0
        creator = candidate().creator
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
        for i in range(12):
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
            rpc.transactions[sig] = {
                "blockTime": block_time,
                "transaction": {
                    "message": {
                        "accountKeys": [{"pubkey": chainseer_solana.PUMP_PROGRAM_ID}]
                    }
                },
                "meta": {
                    "err": None,
                    "logMessages": [
                        "Program data: "
                        + farm_create_event_payload(
                            mint_seed=10 + i,
                            creator=b58_bytes(4),
                            block_time=block_time,
                        )
                    ],
                },
            }
        # Newest-first, matching real getSignaturesForAddress ordering: the
        # unrelated noise occupies all of page one, the farm deployments only
        # surface on page two.
        rpc.signatures = noise + farm_rows

        analyzer = chainseer_solana.SolanaRiskAnalyzer(rpc, FakeJupiter())
        with patch("chainseer_solana.time.time", return_value=now):
            history = analyzer._creator_history(candidate())
            decision = analyzer.analyze(candidate())

        self.assertGreaterEqual(history["pages_scanned"], 2)
        self.assertEqual(history["prior_deployments_in_window"], 12)
        self.assertTrue(
            any(
                h.startswith("creator_industrialized_deployment_")
                for h in decision.hard_stops
            ),
            decision.hard_stops,
        )

    def test_creator_history_scan_degraded_when_transaction_lookup_fails(self):
        now = 2_000_000_000.0
        rpc = FakeRPC()
        rows = []
        for i in range(3):
            sig = f"sig-{i}"
            block_time = int(now - 60 * i)
            rows.append(
                {"signature": sig, "slot": i, "blockTime": block_time, "err": None}
            )
            rpc.transactions[sig] = {
                "blockTime": block_time,
                "transaction": {
                    "message": {
                        "accountKeys": [{"pubkey": chainseer_solana.PUMP_PROGRAM_ID}]
                    }
                },
                "meta": {"err": None, "logMessages": []},
            }
        rpc.signatures = rows
        rpc.failing_transaction_signatures = {"sig-1"}

        analyzer = chainseer_solana.SolanaRiskAnalyzer(rpc, FakeJupiter())
        with patch("chainseer_solana.time.time", return_value=now):
            result = analyzer._creator_history(candidate())
        self.assertTrue(result["scanned"])
        self.assertTrue(result["scan_degraded"])
        self.assertEqual(result["transactions_failed"], 1)

        with patch("chainseer_solana.time.time", return_value=now):
            decision = analyzer.analyze(candidate())
        self.assertTrue(
            any(
                w.startswith("creator_history_scan_degraded_")
                for w in decision.warnings
            ),
            decision.warnings,
        )

    def test_creator_history_cache_is_bounded_with_lru_eviction(self):
        analyzer = chainseer_solana.SolanaRiskAnalyzer(FakeRPC(), FakeJupiter())
        analyzer._creator_history_cache_max = 3
        for i in range(5):
            analyzer._cache_creator_history(
                f"creator-{i}", time.monotonic(), {"scanned": False}
            )
        self.assertEqual(len(analyzer._creator_history_cache), 3)
        self.assertNotIn("creator-0", analyzer._creator_history_cache)
        self.assertNotIn("creator-1", analyzer._creator_history_cache)
        self.assertIn("creator-4", analyzer._creator_history_cache)

    def test_catalog_prunes_entries_older_than_retention_window(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            stale = candidate(
                mint=chainseer_solana._b58encode(b58_bytes(9)),
                block_time=1,
            ).to_dict()
            (root / "catalog.json").write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "ecosystem": "pump_fun",
                        "tokens": {stale["mint"]: stale},
                    }
                ),
                encoding="utf-8",
            )
            ledger = chainseer_solana.HashLedger(root / "events.jsonl")
            observer = chainseer_solana.PumpFunObserver(FakeRPC(), root, ledger)
            with patch("chainseer_solana.time.time", return_value=2_000_000_000.0):
                observer.sync(signature_limit=10)
            catalog = json.loads(
                (root / "catalog.json").read_text(encoding="utf-8")
            )
            self.assertNotIn(stale["mint"], catalog["tokens"])

    def test_recovery_queue_prunes_old_resolved_items_but_keeps_pending(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )
            queue = engine._load_recovery_queue()
            queue["items"]["stale-mint"] = {
                "mint": "stale-mint",
                "status": "resolved",
                "resolved_at": "2000-01-01T00:00:00+00:00",
            }
            queue["items"]["fresh-mint"] = {
                "mint": "fresh-mint",
                "status": "pending",
            }
            engine._save_recovery_queue(queue)
            saved = json.loads(
                (root / "recovery_queue.json").read_text(encoding="utf-8")
            )
            self.assertNotIn("stale-mint", saved["items"])
            self.assertIn("fresh-mint", saved["items"])

    def test_canonical_graduated_market_allows_shadow_entry(self):
        dex = FakeDexScreener()
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            FakeRPC(curve_complete=True),
            FakeJupiter(),
            dexscreener=dex,
        )
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate())
        self.assertEqual(decision.evidence_state, "complete_safe")
        self.assertTrue(decision.shadow_entry_allowed)
        self.assertEqual(decision.cohort, "graduated_market")
        self.assertEqual(decision.admission_state, "graduated_market_ready")
        self.assertTrue(
            decision.graduation["canonical_pool_verified_on_chain"]
        )
        self.assertTrue(decision.graduation["secondary_market_observed"])
        self.assertEqual(dex.calls, 1)

    def test_graduation_probe_backlinks_only_persisted_pump_launches(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            item = candidate()
            (root / "analysis_index.json").write_text(
                json.dumps(
                    {
                        "tokens": {
                            item.mint: {
                                "candidate": item.to_dict(),
                                "decision": {
                                    "evidence_state": "complete_safe",
                                    "admission_state": "graduation_pending",
                                    "cohort": "launch_observation",
                                },
                            }
                        }
                    }
                ),
                encoding="utf-8",
            )
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(curve_complete=True),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )
            engine._seed_recovery_queue()
            selected, stats = engine._probe_graduation_candidates(
                limit=1
            )
            self.assertEqual([value.mint for value in selected], [item.mint])
            self.assertEqual(stats["scanned"], 1)
            self.assertEqual(stats["completed"], 1)
            queue = json.loads(
                (root / "recovery_queue.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                queue["items"][item.mint][
                    "last_graduation_progress_pct"
                ],
                100.0,
            )

    def test_recovery_prioritizes_nearest_graduation_after_migrations(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = candidate(
                mint=chainseer_solana._b58encode(b58_bytes(11)),
                bonding_curve=chainseer_solana._b58encode(b58_bytes(12)),
                symbol="NEAR",
            )
            second = candidate(
                mint=chainseer_solana._b58encode(b58_bytes(13)),
                bonding_curve=chainseer_solana._b58encode(b58_bytes(14)),
                symbol="FAR",
            )
            now = "2026-01-01T00:00:00+00:00"
            (root / "recovery_queue.json").write_text(
                json.dumps(
                    {
                        "items": {
                            first.mint: {
                                "mint": first.mint,
                                "candidate": first.to_dict(),
                                "status": "pending",
                                "attempts": 1,
                                "first_queued_at": now,
                                "next_attempt_at": now,
                                "last_admission_state": "graduation_pending",
                                "last_graduation_progress_pct": 98.5,
                            },
                            second.mint: {
                                "mint": second.mint,
                                "candidate": second.to_dict(),
                                "status": "pending",
                                "attempts": 0,
                                "first_queued_at": now,
                                "next_attempt_at": now,
                                "last_admission_state": "graduation_pending",
                                "last_graduation_progress_pct": 12.0,
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )
            seen = []

            def evaluate(value, **_kwargs):
                seen.append(value.mint)
                return {"candidate": value.to_dict()}

            with patch.object(engine, "evaluate_candidate", side_effect=evaluate):
                engine._recover_indeterminate(
                    limit=1, shadow_enter=False
                )
            self.assertEqual(seen, [first.mint])

    def test_reflection_checkpoint_fail_closes_until_acknowledged(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )
            state = engine.reflection_status()
            state["next_analysis_checkpoint"] = 1
            chainseer_solana._atomic_json(
                engine.reflection_state_path, state
            )
            engine.observation_ledger.append(
                "solana_risk_analysis", {"mint": candidate().mint}
            )
            pending = engine._maybe_request_reflection()
            self.assertEqual(pending["status"], "pending")
            self.assertTrue(pending["pause_requested"])
            with self.assertRaises(
                chainseer_solana.ReflectionCheckpointPending
            ):
                engine.assert_learning_allowed()
            acknowledged = engine.acknowledge_reflection(
                "no_change", "Evidence did not justify a code change."
            )
            self.assertEqual(acknowledged["status"], "armed")
            self.assertFalse(acknowledged["pause_requested"])
            engine.assert_learning_allowed()
            self.assertTrue(engine.reflection_ledger.verify()[0])

    def test_curve_completion_without_canonical_pool_stays_pending(self):
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            FakeRPC(curve_complete=True, canonical_pool=False),
            FakeJupiter(),
            dexscreener=FakeDexScreener(),
        )
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate())
        self.assertEqual(decision.evidence_state, "complete_safe")
        self.assertFalse(decision.shadow_entry_allowed)
        self.assertEqual(
            decision.admission_state, "canonical_migration_pending"
        )

    def test_active_mint_authority_is_token_hard_stop(self):
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            FakeRPC(mint_authority="authority"), FakeJupiter()
        )
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate())
        self.assertIn("mint_authority_active", decision.hard_stops)
        self.assertEqual(decision.evidence_state, "complete_unsafe")

    def test_token_2022_transfer_hook_is_hard_stop(self):
        item = candidate(token_program=chainseer_solana.TOKEN_2022_PROGRAM_ID)
        rpc = FakeRPC(extensions=[{"extension": "transferHook"}])
        original = rpc.get_account_info

        def account(address, *, encoding="jsonParsed"):
            result = original(address, encoding=encoding)
            if address == item.mint:
                result["value"]["owner"] = chainseer_solana.TOKEN_2022_PROGRAM_ID
            return result

        rpc.get_account_info = account
        analyzer = chainseer_solana.SolanaRiskAnalyzer(rpc, FakeJupiter())
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(item)
        self.assertTrue(
            any(stop.startswith("risky_token_2022_extensions") for stop in decision.hard_stops)
        )

    def test_infrastructure_failure_is_not_mislabeled_token_unsafe(self):
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            FakeRPC(curve_complete=True),
            FakeJupiter(fail=True),
            dexscreener=FakeDexScreener(),
        )
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate())
        self.assertEqual(decision.evidence_state, "infrastructure_indeterminate")
        self.assertFalse(decision.shadow_entry_allowed)
        self.assertTrue(decision.infrastructure_errors)

    def test_low_roundtrip_retention_blocks_entry(self):
        analyzer = chainseer_solana.SolanaRiskAnalyzer(
            FakeRPC(curve_complete=True),
            FakeJupiter(retention=50),
            dexscreener=FakeDexScreener(),
        )
        with patch("chainseer_solana.time.time", return_value=1_700_000_120):
            decision = analyzer.analyze(candidate())
        self.assertIn("jupiter_roundtrip_retention_low", decision.hard_stops)

    def test_shadow_position_closes_at_stop_loss(self):
        with tempfile.TemporaryDirectory() as temp:
            ledger = chainseer_solana.HashLedger(Path(temp) / "shadow.jsonl")
            trader = chainseer_solana.SolanaShadowTrader(
                Path(temp) / "state.json", ledger
            )
            analyzer = chainseer_solana.SolanaRiskAnalyzer(
                FakeRPC(curve_complete=True),
                FakeJupiter(),
                dexscreener=FakeDexScreener(),
            )
            with patch("chainseer_solana.time.time", return_value=1_700_000_120):
                decision = analyzer.analyze(candidate())
            position = trader.enter(candidate(), decision)
            quote = FakeJupiter._quote(
                candidate().mint,
                chainseer_solana.WRAPPED_SOL_MINT,
                position["token_amount_raw"],
                int(position["entry_cost_lamports"] * 0.5),
            )
            trader.mark(candidate().mint, quote, now=1_700_000_180)
            self.assertEqual(
                trader.state["positions"][candidate().mint]["exit_reason"], "stop_loss"
            )
            self.assertTrue(ledger.verify()[0])

    def test_live_broadcast_is_absent(self):
        with tempfile.TemporaryDirectory() as temp:
            trader = chainseer_solana.SolanaShadowTrader(
                Path(temp) / "state.json",
                chainseer_solana.HashLedger(Path(temp) / "events.jsonl"),
            )
            with self.assertRaises(chainseer_solana.LiveExecutionDisabledError):
                trader.broadcast_live_trade()

    def test_promotion_requires_200_observations_and_50_closed(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            observation = chainseer_solana.HashLedger(root / "observations.jsonl")
            shadow = chainseer_solana.HashLedger(root / "shadow.jsonl")
            trader = chainseer_solana.SolanaShadowTrader(root / "state.json", shadow)
            evaluator = chainseer_solana.SolanaPromotionEvaluator(
                root / "analysis.json", trader, observation, shadow
            )
            report = evaluator.evaluate()
            self.assertEqual(report["status"], "NOT_PROMOTABLE")
            self.assertIn("minimum_observations_not_met", report["blockers"])
            self.assertIn("minimum_closed_positions_not_met", report["blockers"])
            self.assertIsNone(
                report["metrics"]["two_way_quote_coverage_pct"]
            )
            self.assertIsNone(
                report["metrics"][
                    "unsigned_transaction_assembly_coverage_pct"
                ]
            )
            self.assertIsNone(
                report["metrics"][
                    "infrastructure_indeterminate_rate_pct"
                ]
            )
            self.assertNotIn(
                "two_way_quote_coverage_not_met", report["blockers"]
            )
            self.assertNotIn(
                "unsigned_transaction_assembly_coverage_not_met",
                report["blockers"],
            )
            self.assertNotIn(
                "infrastructure_indeterminate_rate_too_high",
                report["blockers"],
            )
            self.assertFalse(report["live_execution_enabled"])

    def test_promotion_rates_use_only_the_graduated_cohort_denominator(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            observation = chainseer_solana.HashLedger(
                root / "observations.jsonl"
            )
            shadow = chainseer_solana.HashLedger(root / "shadow.jsonl")
            trader = chainseer_solana.SolanaShadowTrader(
                root / "state.json", shadow
            )
            analyses = {
                "launch-only": {
                    "decision": {
                        "evidence_state": "infrastructure_indeterminate",
                        "cohort": "launch_observation",
                        "coverage": {},
                    }
                },
                "graduated-good": {
                    "decision": {
                        "evidence_state": "complete_safe",
                        "cohort": "graduated_market",
                        "coverage": {
                            "jupiter_two_way_quote": True,
                            "unsigned_buy_assembly": True,
                        },
                    }
                },
                "graduated-infra": {
                    "decision": {
                        "evidence_state": "infrastructure_indeterminate",
                        "cohort": "graduated_market",
                        "coverage": {
                            "jupiter_two_way_quote": False,
                            "unsigned_buy_assembly": False,
                        },
                    }
                },
            }
            (root / "analysis.json").write_text(
                json.dumps({"tokens": analyses}), encoding="utf-8"
            )
            report = chainseer_solana.SolanaPromotionEvaluator(
                root / "analysis.json", trader, observation, shadow
            ).evaluate()
            self.assertEqual(report["metrics"]["observations"], 2)
            self.assertEqual(
                report["metrics"]["two_way_quote_coverage_pct"], 50.0
            )
            self.assertEqual(
                report["metrics"][
                    "unsigned_transaction_assembly_coverage_pct"
                ],
                50.0,
            )
            self.assertEqual(
                report["metrics"][
                    "infrastructure_indeterminate_rate_pct"
                ],
                50.0,
            )
            self.assertIn(
                "infrastructure_indeterminate_rate_too_high",
                report["blockers"],
            )

    def test_promotion_passes_only_to_manual_review_and_never_enables_live(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            observation = chainseer_solana.HashLedger(root / "observations.jsonl")
            shadow = chainseer_solana.HashLedger(root / "shadow.jsonl")
            trader = chainseer_solana.SolanaShadowTrader(root / "state.json", shadow)
            analyses = {
                f"mint-{index}": {
                    "decision": {
                        "evidence_state": "complete_safe",
                        "cohort": "graduated_market",
                        "coverage": {
                            "jupiter_two_way_quote": True,
                            "unsigned_buy_assembly": True,
                        },
                    }
                }
                for index in range(200)
            }
            (root / "analysis.json").write_text(
                json.dumps({"tokens": analyses}), encoding="utf-8"
            )
            trader.state["positions"] = {
                f"mint-{index}": {
                    "status": "closed",
                    "closed_at": f"2026-01-01T00:{index:02d}:00+00:00",
                    "entry_cost_lamports": 100,
                    "net_pnl_lamports": 10,
                }
                for index in range(50)
            }
            evaluator = chainseer_solana.SolanaPromotionEvaluator(
                root / "analysis.json", trader, observation, shadow
            )
            report = evaluator.evaluate()
            self.assertEqual(report["status"], "PROMOTABLE_FOR_REVIEW")
            self.assertEqual(report["blockers"], [])
            self.assertFalse(report["automatic_live_enable"])
            self.assertFalse(report["live_execution_enabled"])

    def test_hash_ledger_detects_tampering(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "events.jsonl"
            ledger = chainseer_solana.HashLedger(path)
            ledger.append("test", {"value": 1})
            row = json.loads(path.read_text())
            row["payload"]["value"] = 2
            path.write_text(json.dumps(row) + "\n")
            self.assertFalse(ledger.verify()[0])

    def test_rpc_retries_429_and_honors_retry_after(self):
        class Response:
            def __init__(self, status, payload, headers=None):
                self.status_code = status
                self.payload = payload
                self.headers = headers or {}

            def raise_for_status(self):
                if self.status_code >= 400:
                    error = ConnectionError("rate limited")
                    error.response = self
                    raise error

            def json(self):
                return self.payload

        class Session:
            def __init__(self):
                self.values = [
                    Response(429, {}, {"Retry-After": "0"}),
                    Response(200, {"jsonrpc": "2.0", "result": 123}),
                ]

            def post(self, *_args, **_kwargs):
                return self.values.pop(0)

        rpc = chainseer_solana.SolanaRPC(
            "https://rpc.test", session=Session(), minimum_request_interval=0
        )
        with patch("chainseer_solana.time.sleep"):
            self.assertEqual(rpc._call("getSlot"), 123)
        self.assertEqual(rpc.attempts, 2)
        self.assertEqual(rpc.successes, 1)

    def test_rpc_fails_over_and_never_exposes_endpoint_credentials(self):
        class Response:
            def __init__(self, status, payload):
                self.status_code = status
                self.payload = payload
                self.headers = {"Retry-After": "0"}

            def raise_for_status(self):
                if self.status_code >= 400:
                    error = ConnectionError("rate limited")
                    error.response = self
                    raise error

            def json(self):
                return self.payload

        class Session:
            def __init__(self):
                self.responses = [
                    Response(429, {}),
                    Response(200, {"jsonrpc": "2.0", "result": 456}),
                ]

            def post(self, *_args, **_kwargs):
                return self.responses.pop(0)

        secret = "never-persist-this-key"
        rpc = chainseer_solana.SolanaRPC(
            f"https://primary.helius.invalid/rpc?api-key={secret}",
            urls=[
                "https://fallback.chainstack.invalid/"
                f"private/{secret}"
            ],
            session=Session(),
            minimum_request_interval=0,
            max_retries=0,
            circuit_failure_threshold=1,
            jitter_fn=lambda _maximum: 0,
        )
        self.assertEqual(rpc._call("getSlot"), 456)
        health = rpc.health()
        encoded = json.dumps(health)
        self.assertNotIn(secret, encoded)
        self.assertEqual(health["endpoint_count"], 2)
        self.assertEqual(health["endpoint_switches"], 1)
        self.assertEqual(health["methods"]["getSlot"]["attempts"], 2)
        self.assertEqual(health["methods"]["getSlot"]["retries"], 1)
        self.assertEqual(
            health["rpc_url"], "https://primary.helius.invalid"
        )

    def test_rpc_health_merge_is_cumulative_per_method(self):
        current_one = {
            "rpc_url": "https://mainnet.helius.invalid",
            "provider": "helius",
            "endpoint_count": 1,
            "active_endpoint": "endpoint_1",
            "attempts": 3,
            "successes": 2,
            "failures": 1,
            "retries": 1,
            "endpoint_switches": 0,
            "methods": {
                "getSlot": {
                    "attempts": 3,
                    "successes": 2,
                    "failures": 1,
                    "retries": 1,
                    "status_codes": {"200": 2, "429": 1},
                }
            },
            "endpoints": [],
        }
        first = chainseer_solana._merge_rpc_health(
            {}, chainseer_solana._rpc_health_delta(current_one, {}), current_one
        )
        current_two = {
            **current_one,
            "attempts": 5,
            "successes": 4,
            "failures": 1,
            "retries": 1,
            "methods": {
                "getSlot": {
                    "attempts": 5,
                    "successes": 4,
                    "failures": 1,
                    "retries": 1,
                    "status_codes": {"200": 4, "429": 1},
                }
            },
        }
        second = chainseer_solana._merge_rpc_health(
            first,
            chainseer_solana._rpc_health_delta(current_two, current_one),
            current_two,
        )
        self.assertEqual(second["attempts"], 5)
        self.assertEqual(second["successes"], 4)
        self.assertEqual(second["methods"]["getSlot"]["attempts"], 5)
        self.assertEqual(
            second["methods"]["getSlot"]["status_codes"]["429"], 1
        )

    def test_rpc_health_keeps_provider_history_separate_by_endpoint_identity(self):
        public = {
            "rpc_url": chainseer_solana.PUBLIC_SOLANA_RPC_URL,
            "provider": "solana_public",
            "endpoint_count": 1,
            "active_endpoint": "endpoint_1",
            "attempts": 10,
            "successes": 8,
            "failures": 2,
            "retries": 0,
            "endpoint_switches": 0,
            "methods": {},
            "endpoints": [
                {
                    "label": "endpoint_1",
                    "rpc_url": chainseer_solana.PUBLIC_SOLANA_RPC_URL,
                    "provider": "solana_public",
                    "attempts": 10,
                    "successes": 8,
                    "failures": 2,
                }
            ],
        }
        first = chainseer_solana._merge_rpc_health(
            {}, chainseer_solana._rpc_health_delta(public, {}), public
        )
        helius = {
            **public,
            "rpc_url": "https://mainnet.helius.invalid",
            "provider": "helius",
            "attempts": 3,
            "successes": 3,
            "failures": 0,
            "endpoints": [
                {
                    "label": "endpoint_1",
                    "rpc_url": "https://mainnet.helius.invalid",
                    "provider": "helius",
                    "attempts": 3,
                    "successes": 3,
                    "failures": 0,
                }
            ],
        }
        second = chainseer_solana._merge_rpc_health(
            first, chainseer_solana._rpc_health_delta(helius, {}), helius
        )
        self.assertEqual(len(second["endpoints"]), 2)
        self.assertEqual(second["provider"], "mixed_history")
        self.assertEqual(second["current_segment"]["provider"], "helius")
        self.assertEqual(second["current_segment"]["attempts"], 3)
        self.assertEqual(second["current_segment"]["successes"], 3)

    def test_rpc_health_v2_archives_unverifiable_legacy_attribution(self):
        legacy = {
            "schema_version": 1,
            "rpc_url": "https://mainnet.helius.invalid",
            "provider": "helius",
            "attempts": 100,
            "successes": 90,
            "failures": 10,
            "retries": 2,
            "endpoint_switches": 0,
            "methods": {"getSlot": {"attempts": 100}},
        }
        current = {
            "rpc_url": "https://mainnet.helius.invalid",
            "provider": "helius",
            "endpoint_count": 1,
            "active_endpoint": "endpoint_1",
            "attempts": 2,
            "successes": 2,
            "failures": 0,
            "retries": 0,
            "endpoint_switches": 0,
            "methods": {},
            "endpoints": [
                {
                    "label": "endpoint_1",
                    "rpc_url": "https://mainnet.helius.invalid",
                    "provider": "helius",
                    "attempts": 2,
                    "successes": 2,
                    "failures": 0,
                }
            ],
        }
        migrated = chainseer_solana._merge_rpc_health(
            legacy,
            chainseer_solana._rpc_health_delta(current, {}),
            current,
        )
        self.assertEqual(migrated["telemetry_schema_version"], 2)
        self.assertEqual(migrated["attempts"], 2)
        self.assertEqual(migrated["current_segment"]["attempts"], 2)
        self.assertEqual(
            migrated["legacy_aggregate"]["attempts"], 100
        )
        self.assertEqual(
            migrated["legacy_aggregate"]["attribution"],
            "legacy_endpoint_identity_unverifiable",
        )

    def test_indeterminate_recovery_is_restart_safe_and_append_only(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            jupiter = FakeJupiter(fail=True)
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(curve_complete=True),
                jupiter=jupiter,
                dexscreener=FakeDexScreener(),
                record_timechain=False,
            )
            with patch(
                "chainseer_solana.time.time", return_value=1_700_000_120
            ):
                first = engine.evaluate_candidate(
                    candidate(), shadow_enter=True
                )
            self.assertEqual(
                first["decision"]["evidence_state"],
                "infrastructure_indeterminate",
            )
            self.assertEqual(
                engine._recovery_summary()["pending"], 1
            )
            self.assertEqual(len(engine.trader.open_positions()), 0)

            restarted = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(curve_complete=True),
                jupiter=FakeJupiter(fail=False),
                dexscreener=FakeDexScreener(),
                record_timechain=False,
            )
            with patch(
                "chainseer_solana.time.time", return_value=1_700_000_180
            ):
                recovered = restarted._recover_indeterminate(
                    limit=1, shadow_enter=False
                )
            self.assertEqual(len(recovered), 1)
            self.assertEqual(
                recovered[0]["decision"]["evidence_state"],
                "complete_safe",
            )
            latest = json.loads(
                (root / "analysis_index.json").read_text(encoding="utf-8")
            )
            self.assertEqual(
                latest["tokens"][candidate().mint]["decision"][
                    "evidence_state"
                ],
                "complete_safe",
            )
            events = restarted.observation_ledger.load()
            self.assertEqual(
                [row["event_type"] for row in events],
                ["solana_risk_analysis", "solana_risk_reanalysis"],
            )
            self.assertEqual(
                events[-1]["payload"]["previous_evidence_state"],
                "infrastructure_indeterminate",
            )
            self.assertEqual(
                restarted._recovery_summary()["resolved"], 1
            )
            self.assertEqual(len(restarted.trader.open_positions()), 0)

    def test_jupiter_keyless_access_is_paced_and_retries_429(self):
        class Response:
            def __init__(self, status, payload, headers=None):
                self.status_code = status
                self.payload = payload
                self.headers = headers or {}

            def raise_for_status(self):
                if self.status_code >= 400:
                    error = ConnectionError("rate limited")
                    error.response = self
                    raise error

            def json(self):
                return self.payload

        class Session:
            def __init__(self):
                self.values = [
                    Response(429, {}, {"Retry-After": "0"}),
                    Response(200, {"ok": True}),
                ]
                self.headers = []

            def get(self, *_args, **kwargs):
                self.headers.append(kwargs["headers"])
                return self.values.pop(0)

        session = Session()
        with patch.dict(environ, {"JUPITER_API_KEY": ""}, clear=False):
            client = chainseer_solana.JupiterClient(
                session=session,
                minimum_request_interval=0,
                max_retries=1,
            )
        with patch("chainseer_solana.time.sleep"):
            self.assertEqual(client._get("/test", {}), {"ok": True})
        self.assertEqual(session.headers, [{}, {}])
        self.assertEqual(client.health()["access_mode"], "keyless")
        self.assertEqual(client.retries, 1)
        self.assertEqual(client.successes, 1)
        self.assertEqual(client.failures, 0)

    def test_json_reader_accepts_windows_powershell_utf8_bom(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "schedule.json"
            path.write_text('{"enabled":true}', encoding="utf-8-sig")
            self.assertEqual(
                chainseer_solana._read_json(path, {}),
                {"enabled": True},
            )

    def test_dashboard_snapshot_uses_real_state_and_redacts_credentials(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=root,
                rpc=FakeRPC(),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )
            engine.rpc.url = (
                "https://current.helius.invalid/rpc"
                "?api-key=do-not-expose-current-key"
            )
            item = candidate()
            engine.trader.state["positions"][item.mint] = {
                "mint": item.mint,
                "symbol": item.symbol,
                "status": "open",
                "opened_at": "2026-07-26T08:00:00+00:00",
                "entry_cost_lamports": 10_000_000,
                "token_amount_raw": 1_000_000,
                "entry_score": 90.0,
                "entry_evidence_state": "complete_safe",
                "marks": [
                    {
                        "timestamp": "2026-07-26T08:05:00+00:00",
                        "proceeds_lamports": 12_000_000,
                        "multiple": 1.2,
                    }
                ],
                "paper_only": True,
                "live_execution_enabled": False,
            }
            engine.trader._save()
            analyses = {
                "safe": {
                    "candidate": {"symbol": "SAFE", "name": "Safe"},
                    "decision": {
                        "evidence_state": "complete_safe",
                        "coverage": {
                            "jupiter_two_way_quote": True,
                            "unsigned_buy_assembly": True,
                        },
                        "score": 90.0,
                        "risk_level": "Low",
                        "shadow_entry_allowed": True,
                        "hard_stops": [],
                        "warnings": [],
                        "infrastructure_errors": [],
                        "execution_evidence": {
                            "roundtrip_retention_pct": 98.0
                        },
                    },
                    "updated_at": "2026-07-26T08:06:00+00:00",
                },
                "unsafe": {
                    "candidate": {"symbol": "RISK", "name": "Risk"},
                    "decision": {
                        "evidence_state": "complete_unsafe",
                        "coverage": {},
                        "score": 30.0,
                        "risk_level": "High",
                        "shadow_entry_allowed": False,
                        "hard_stops": ["mint_authority_active"],
                    },
                    "updated_at": "2026-07-26T08:07:00+00:00",
                },
                "indeterminate": {
                    "candidate": {"symbol": "WAIT", "name": "Wait"},
                    "decision": {
                        "evidence_state": "infrastructure_indeterminate",
                        "coverage": {},
                        "score": 70.0,
                        "risk_level": "Indeterminate",
                        "shadow_entry_allowed": False,
                        "infrastructure_errors": [
                            "RPC unavailable at "
                            "https://mainnet.example.invalid/rpc"
                            "?api-key=do-not-expose-this-rpc-key"
                        ],
                    },
                    "updated_at": "2026-07-26T08:08:00+00:00",
                },
            }
            (root / "analysis_index.json").write_text(
                json.dumps({"tokens": analyses}), encoding="utf-8"
            )
            secret = "do-not-expose-this-rpc-key"
            (root / "rpc_health.json").write_text(
                json.dumps(
                    {
                        "rpc_url": (
                            "https://mainnet.example.invalid/rpc"
                            f"?api-key={secret}"
                        ),
                        "attempts": 10,
                        "successes": 9,
                        "failures": 1,
                    }
                ),
                encoding="utf-8",
            )
            (root / "schedule.json").write_text(
                json.dumps(
                    {
                        "installed": True,
                        "enabled": True,
                        "interval_minutes": 10,
                    }
                ),
                encoding="utf-8",
            )
            (root / "scheduler_status.json").write_text(
                json.dumps({"status": "complete"}),
                encoding="utf-8",
            )
            snapshot = chainseer_solana._solana_dashboard_snapshot(engine)

        self.assertTrue(snapshot["paper_only"])
        self.assertFalse(snapshot["live_execution_enabled"])
        self.assertEqual(snapshot["analysis_count"], 3)
        self.assertEqual(snapshot["evidence_states"]["complete_safe"], 1)
        self.assertEqual(snapshot["evidence_states"]["complete_unsafe"], 1)
        self.assertEqual(
            snapshot["evidence_states"]["infrastructure_indeterminate"], 1
        )
        self.assertEqual(snapshot["shadow"]["open"], 1)
        self.assertAlmostEqual(snapshot["shadow"]["modeled_return_pct"], 20.0)
        self.assertEqual(
            snapshot["rpc_health"]["rpc_url"],
            "https://mainnet.example.invalid",
        )
        self.assertEqual(
            snapshot["configuration"]["rpc_endpoint"],
            "https://current.helius.invalid",
        )
        self.assertEqual(
            snapshot["configuration"]["rpc_last_observed_endpoint"],
            "https://mainnet.example.invalid",
        )
        self.assertNotIn(secret, json.dumps(snapshot))
        self.assertNotIn("do-not-expose-current-key", json.dumps(snapshot))
        self.assertFalse(snapshot["configuration"]["credentials_exposed"])
        self.assertTrue(snapshot["schedule"]["enabled"])
        self.assertEqual(snapshot["scheduler"]["status"], "complete")
        self.assertTrue(snapshot["integrity"]["ok"])

    def test_dashboard_asset_is_read_only_and_has_no_mock_data(self):
        html = (
            Path(chainseer_solana.__file__)
            .with_name("solana_dashboard.html")
            .read_text(encoding="utf-8")
        )
        self.assertIn('fetch("/api/status"', html)
        self.assertIn("LIVE LOCAL DATA", html)
        self.assertIn("Infrastructure indeterminate", html)
        self.assertIn("NO SIGNING", html)
        self.assertIn("NO PRIVATE KEYS", html)
        self.assertNotIn("mockData", html)

    def test_dashboard_asset_reader_does_not_cache_process_lifetime_html(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "dashboard.html"
            path.write_bytes(b"version-one")
            self.assertEqual(
                chainseer_solana._read_dashboard_asset(path),
                b"version-one",
            )
            path.write_bytes(b"version-two")
            self.assertEqual(
                chainseer_solana._read_dashboard_asset(path),
                b"version-two",
            )

    def test_scheduler_scripts_preserve_paper_only_boundary(self):
        root = Path(chainseer_solana.__file__).parent
        runner = (root / "run_chainseer_solana_learning.ps1").read_text(
            encoding="utf-8"
        )
        manager = (
            root / "manage_chainseer_solana_learning_task.ps1"
        ).read_text(encoding="utf-8")
        self.assertIn("Local\\ChainseerSolanaLearnOnce", runner)
        self.assertIn("live_execution_enabled = $false", runner)
        self.assertIn("--graduation-limit $GraduationLimit", runner)
        self.assertIn("sealed_reflection_checkpoint_pending", runner)
        self.assertIn("controller_status.json", runner)
        self.assertIn("-MultipleInstances IgnoreNew", manager)
        self.assertIn('"stop"', manager)
        self.assertIn('"reschedule"', manager)
        self.assertIn("[ValidateRange(1, 1440)]", manager)
        self.assertIn(
            'GetEnvironmentVariable(\n            "CHAINSEER_SOLANA_RPC_URL",',
            manager,
        )
        self.assertIn("Get-ChainseerSolanaIntervalMinutes", manager)
        self.assertIn("live_execution_enabled = $false", manager)
        self.assertNotIn("private_key", (runner + manager).lower())

    def test_dashboard_refuses_non_loopback_bind(self):
        with tempfile.TemporaryDirectory() as temp:
            engine = chainseer_solana.SolanaPrototypeEngine(
                root=temp,
                rpc=FakeRPC(),
                jupiter=FakeJupiter(),
                record_timechain=False,
            )
            with self.assertRaisesRegex(ValueError, "local-only"):
                chainseer_solana.serve_solana_dashboard(
                    engine, host="0.0.0.0", port=8767
                )


if __name__ == "__main__":
    unittest.main()
