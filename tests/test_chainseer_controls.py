import json
import tempfile
import time
import unittest
from pathlib import Path

import chainseer_controls as controls
from chainseer_outcome_ledger import (
    analysis_evidence_binding,
    build_outcome_record,
)


TOKEN = "0x" + "1" * 40
PAIR = "0x" + "2" * 40
RECIPIENT = "0x" + "3" * 40
SOLANA_MINT = "So11111111111111111111111111111111111111112"


class FakeTimechain:
    def __init__(self, rings=None):
        self.rings = list(rings or [])

    def load(self):
        return list(self.rings)

    def verify(self):
        return True, ["ok"]


class FakePoQ:
    def gate_and_seal(self, tc, _candidate, **kwargs):
        ring = {
            "index": len(tc.rings),
            "ring_hash": f"ring-{len(tc.rings)}",
            "ring_type": kwargs["ring_type"],
            "payload": kwargs.get("extra_payload") or {},
        }
        tc.rings.append(ring)
        return {"decision": "SEAL"}, ring


class FakeRPC:
    def __init__(self):
        self.head = 102
        self.owner = "0x" + "4" * 40
        self.context = None
        self.ledger = None
        self.logs = []

    def unbind_context(self):
        self.context = None
        self.ledger = None

    def get_block_number(self):
        return self.head

    def get_block(self, block):
        return {"number": hex(block), "hash": f"hash-{block}"}

    def get_code(self, _address, block=None):
        return "0x6000" + hex(block or 0)[2:]

    def erc20_owner(self, _token, block=None):
        return self.owner

    def erc20_total_supply(self, _token, block=None):
        return 1_000_000

    def get_logs(self, *_args, **kwargs):
        return list(self.logs) if kwargs.get("address") == TOKEN else []


class FakeAgent:
    def __init__(self):
        self.rpc = FakeRPC()
        self.tc = FakeTimechain(
            [
                {
                    "index": 0,
                    "ring_hash": "genesis",
                    "ring_type": "genesis",
                    "payload": {},
                }
            ]
        )
        self.poq_module = FakePoQ()
        self.chain_id = 4663
        self.chain_root = "."
        self.scan_count = 0
        self.sell_tax = "0"

    def analyze_token(self, token, full_report=False, block_pin=None):
        self.scan_count += 1
        score = 90 if self.scan_count == 1 else 82
        ring = {
            "index": len(self.tc.rings),
            "ring_hash": f"analysis-{self.scan_count}",
            "ring_type": "token_analysis",
            "payload": {
                "token_address": token,
                "risk_level": "Low",
                "legitimacy_score": score,
                "analysis_version": "7.1",
            },
        }
        self.tc.rings.append(ring)
        return {
            "token_address": token,
            "timestamp": controls.utc_now_iso(),
            "analysis_ring": ring["index"],
            "analysis_ring_hash": ring["ring_hash"],
            "provenance": {"block_pin": block_pin, "fact_count": 8},
            "analysis": {
                "risk_level": "Low",
                "legitimacy_score": score,
                "confidence_grade": "HIGH",
                "hard_stop_overrides": [],
                "holder_assessment": {"holder_count": 100 + self.scan_count},
                "extended_evidence": {},
            },
            "data": {
                "contract_audit": {
                    "owner": self.rpc.owner,
                    "bytecode_hash": "code-hash",
                },
                "dex_pairs": {
                    "primary_pair_address": PAIR,
                    "primary_price_usd": 1.0,
                    "primary_liquidity_usd": 100_000,
                },
                "lp_lock": {"state": "protocol_secured"},
                "goplus_security": {
                    "buy_tax": "0",
                    "sell_tax": self.sell_tax,
                },
            },
        }

    def reflect_on_analysis(
        self, analysis_ring, outcomes, evidence_fact_ids=None, observed_at=None,
        outcome_provenance=None,
    ):
        adverse = (
            outcomes.get("liquidity_removed_pct", 0) >= 50
            or outcomes.get("honeypot_observed", False)
        )
        ring = {
            "index": len(self.tc.rings),
            "ring_hash": f"outcome-{len(self.tc.rings)}",
            "ring_type": "analysis_outcome",
            "payload": {
                "analysis_ring": analysis_ring,
                "calibration": {
                    "original_risk_level": "Low",
                    "adverse_security_event": adverse,
                },
            },
        }
        self.tc.rings.append(ring)
        return {
            "ring": ring,
            "verdict": {"decision": "SEAL"},
            "calibration": ring["payload"]["calibration"],
        }


class FakeSolanaRPC:
    def __init__(self):
        self.slot = 100
        self.mint_authority = None
        self.signatures = []
        self.fail_largest_accounts = False

    def get_slot(self):
        return self.slot

    def get_account_info(self, _mint, encoding="jsonParsed"):
        self.asserted_encoding = encoding
        return {
            "context": {"slot": self.slot},
            "value": {
                "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "data": {
                    "parsed": {
                        "type": "mint",
                        "info": {
                            "decimals": 6,
                            "supply": "1000000",
                            "mintAuthority": self.mint_authority,
                            "freezeAuthority": None,
                        },
                    }
                },
            },
        }

    def get_token_supply(self, _mint):
        return {
            "context": {"slot": self.slot},
            "value": {"amount": "1000000", "decimals": 6},
        }

    def get_token_largest_accounts(self, _mint):
        if self.fail_largest_accounts:
            raise RuntimeError(
                "synthetic provider rate limit at "
                "https://rpc.example/?api-key=secret"
            )
        return {
            "context": {"slot": self.slot},
            "value": [
                {"address": "holder-a", "amount": "300000"},
                {"address": "holder-b", "amount": "200000"},
            ],
        }

    def get_signatures_for_address(self, _mint, limit=25):
        return self.signatures[:limit]


class FakeDexScreener:
    def __init__(self):
        self.price = "1.0"
        self.liquidity = 100_000

    def token_pairs(self, mint):
        return [
            {
                "chainId": "solana",
                "pairAddress": "pair-a",
                "dexId": "raydium",
                "baseToken": {"address": mint},
                "quoteToken": {"address": "quote"},
                "priceUsd": self.price,
                "liquidity": {"usd": self.liquidity},
                "marketCap": 1_000_000,
                "fdv": 1_000_000,
            }
        ]


class FakeSolanaAnalyzer:
    def __init__(self, timechain_agent):
        self.rpc = FakeSolanaRPC()
        self.dexscreener = FakeDexScreener()
        self.timechain_agent = timechain_agent
        self.scan_count = 0

    @staticmethod
    def _extension_names(_info):
        return []

    @staticmethod
    def _market_pair(_mint, pairs):
        return pairs[0] if pairs else None

    def analyze_token(self, mint):
        self.scan_count += 1
        ring = {
            "index": len(self.timechain_agent.tc.rings),
            "ring_hash": f"solana-analysis-{self.scan_count}",
            "ring_type": "solana_token_analysis",
            "payload": {},
        }
        self.timechain_agent.tc.rings.append(ring)
        return {
            "token_address": mint,
            "timestamp": controls.utc_now_iso(),
            "analysis_ring": ring["index"],
            "analysis_ring_hash": ring["ring_hash"],
            "provenance": {"block_pin": self.rpc.slot},
            "analysis": {
                "risk_level": (
                    "High" if self.rpc.mint_authority else "Low"
                ),
                "legitimacy_score": (
                    35 if self.rpc.mint_authority else 85
                ),
                "hard_stop_overrides": (
                    [{"code": "mint_authority_active"}]
                    if self.rpc.mint_authority
                    else []
                ),
            },
            "data": {
                "basic_info": {
                    "owner_program": (
                        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                    ),
                    "mint_authority": self.rpc.mint_authority,
                    "freeze_authority": None,
                    "supply_raw": 1_000_000,
                    "extensions": [],
                    "jupiter_holder_count": 2,
                },
                "holder_concentration": {
                    "top1_total_supply_pct": 30,
                    "top10_total_supply_pct": 50,
                },
                "dex_pairs": {
                    "primary_pair": "pair-a",
                    "primary_amm_version": "raydium",
                    "primary_price_usd": float(self.dexscreener.price),
                    "total_liquidity_usd": self.dexscreener.liquidity,
                    "market_cap": 1_000_000,
                },
                "execution_evidence": {
                    "roundtrip_retention_pct": 98,
                },
            },
        }


def safe_quote(block=100, price_impact=50, slippage=100):
    return {
        "observed_block": block,
        "pair_address": PAIR,
        "amount_in": "1000",
        "amount_out": "900",
        "min_amount_out": "850",
        "price_impact_bps": price_impact,
        "slippage_bps": slippage,
        "route": [PAIR],
        "source": "synthetic-test-quote",
    }


class ExtendedEvidenceTests(unittest.TestCase):
    def test_social_is_bounded_and_cross_chain_is_not_overstated(self):
        data = {
            "dexscreener": {
                "pairs": [
                    {
                        "chainId": "robinhood",
                        "liquidity": {"usd": 1000},
                        "boosts": {"active": 99},
                        "info": {
                            "socials": [
                                {"type": "twitter", "url": "https://example.test"}
                            ],
                            "websites": [{"url": "https://example.test"}],
                        },
                    },
                    {
                        "chainId": "base",
                        "liquidity": {"usd": 5000},
                        "volume": {"h24": 2500},
                    },
                ]
            },
            "cross_chain_flow_records": [
                {
                    "source_chain": "base",
                    "destination_chain": "robinhood",
                    "source_tx_hash": "0x" + "a" * 64,
                    "provider": "test-provider",
                    "confidence": 0.9,
                }
            ],
        }
        result = controls.build_extended_evidence(data)
        self.assertEqual(result["social_attention"]["bounded_score"], 62)
        self.assertLessEqual(result["social_attention"]["bounded_score"], 70)
        self.assertFalse(result["social_attention"]["can_trigger_hard_stop"])
        self.assertEqual(result["cross_chain"]["status"], "provider_attested")
        self.assertEqual(result["cross_chain"]["verified_flow_count"], 1)
        self.assertIn("not proof", result["cross_chain"]["caveat"])

    def test_mev_quote_gate_rejects_stale_or_extreme_quote(self):
        result = controls.MEVExposureAssessor.assess(
            safe_quote(block=90, price_impact=1500),
            validated_block=100,
        )
        self.assertEqual(result["risk_level"], "Critical")
        self.assertIn("QUOTE_STALE_OR_FUTURE", result["hard_stops"])
        self.assertIn("PRICE_IMPACT_EXTREME", result["hard_stops"])

    def test_event_topic_hashes_are_canonical_width(self):
        for topic in (
            controls.TRANSFER_TOPIC,
            controls.OWNERSHIP_TRANSFERRED_TOPIC,
            controls.UPGRADED_TOPIC,
            controls.ADMIN_CHANGED_TOPIC,
        ):
            self.assertRegex(topic, r"^0x[a-f0-9]{64}$")


class WatcherAndOutcomeTests(unittest.TestCase):
    def test_single_writer_lease_refuses_second_owner(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            first = controls.SingleWriterLease(temp_dir)
            second = controls.SingleWriterLease(temp_dir)
            first.acquire()
            try:
                with self.assertRaises(RuntimeError):
                    second.acquire()
            finally:
                first.release()
            # A pre-existing metadata file exercises the Windows path that
            # failed in a real second CLI invocation.
            second.acquire()
            second.release()
            third = controls.SingleWriterLease(temp_dir)
            third.acquire()
            third.release()

    def test_watch_store_subscribe_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = controls.WatchStore(temp_dir)
            first = store.subscribe(TOKEN)
            second = store.subscribe(TOKEN.upper().replace("0X", "0x"))
            self.assertEqual(first["created_at"], second["created_at"])
            self.assertEqual(len(store.load()["subscriptions"]), 1)

    def test_base_watch_store_is_network_and_file_isolated(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            robinhood = controls.WatchStore(temp_dir)
            base = controls.WatchStore(temp_dir, "base")
            value = base.subscribe(TOKEN)
            self.assertEqual(value["network"], "base")
            self.assertEqual(robinhood.load()["subscriptions"], {})
            self.assertIn(TOKEN.lower(), base.load()["subscriptions"])
            self.assertNotEqual(robinhood.state_path, base.state_path)
            self.assertNotEqual(robinhood.alert_path, base.alert_path)

    def test_watch_store_isolates_subscribers_and_returns_public_critical_feed(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = controls.WatchStore(temp_dir)
            subscriber_a = "a" * 64
            subscriber_b = "b" * 64
            store.subscribe(TOKEN, subscriber_a)
            store.subscribe(TOKEN, subscriber_b)
            self.assertTrue(store.is_subscribed(TOKEN, subscriber_a))
            self.assertTrue(store.unsubscribe(TOKEN, subscriber_a))
            self.assertFalse(store.is_subscribed(TOKEN, subscriber_a))
            self.assertTrue(store.is_subscribed(TOKEN, subscriber_b))

            alert = {
                "schema_version": controls.CONTROL_SCHEMA_VERSION,
                "network": "robinhood",
                "token_address": TOKEN,
                "block": 100,
                "observed_at": "2026-07-29T20:00:00+00:00",
                "critical_events": [
                    controls._critical_event(
                        "sellability",
                        "sell_restriction_detected",
                        "A sell restriction was detected.",
                    )
                ],
                "analysis_ring": 12,
                "analysis_ring_hash": "analysis-hash",
                "timechain": {"ring": 13, "ring_hash": "watch-hash"},
            }
            alert["alert_hash"] = controls.canonical_hash(alert)
            store.append_alert(alert)
            feed = store.read_alerts(TOKEN)
            self.assertEqual(len(feed), 1)
            self.assertEqual(feed[0]["categories"], ["sellability"])
            self.assertNotIn("events", feed[0])
            self.assertEqual(
                store.read_alerts(
                    TOKEN,
                    after="2026-07-29T20:00:00+00:00",
                ),
                [],
            )

    def test_outcomes_keep_market_and_security_dimensions_separate(self):
        baseline = {
            "price_usd": 1.0,
            "liquidity_usd": 100_000,
            "hard_stop_codes": [],
            "owner": "0x" + "4" * 40,
            "buy_tax": "0",
            "sell_tax": "0",
        }
        current = {
            "price_usd": 0.2,
            "liquidity_usd": 40_000,
            "hard_stop_codes": [],
            "owner": baseline["owner"],
            "buy_tax": "0",
            "sell_tax": "0",
        }
        result = controls.OutcomeCollector.outcomes(
            baseline, current, horizon_seconds=3600
        )
        self.assertAlmostEqual(result["price_return_pct"], -80.0)
        self.assertAlmostEqual(result["liquidity_removed_pct"], 60.0)
        self.assertNotIn("rug_pull", result)

    def test_watcher_rescans_on_fingerprint_change_and_seals_alert(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            agent = FakeAgent()
            agent.chain_root = temp_dir
            watcher = controls.ChainseerWatcher(
                agent,
                control_root=temp_dir,
                config=controls.WatchConfig(
                    confirmations=2,
                    holder_rescan_blocks=12,
                    max_rescan_blocks=120,
                    score_alert_delta=5,
                ),
            )
            watcher.store.subscribe(TOKEN)
            first = watcher.run_once()
            self.assertEqual(first["rescans"], 1)
            self.assertEqual(first["alerts"], 0)

            agent.rpc.head = 103
            agent.rpc.owner = "0x" + "5" * 40
            second = watcher.run_once()
            self.assertEqual(second["rescans"], 1)
            self.assertEqual(second["alerts"], 1)
            alerts = watcher.store.alert_path.read_text(
                encoding="utf-8"
            ).splitlines()
            alert = json.loads(alerts[-1])
            self.assertEqual(alert["type"], "state_change")
            self.assertEqual(
                alert["critical_events"][0]["category"],
                "authority",
            )
            self.assertIsNotNone(alert["timechain"]["ring"])

    def test_watcher_seals_reorg_alert(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            agent = FakeAgent()
            agent.chain_root = temp_dir
            watcher = controls.ChainseerWatcher(
                agent,
                control_root=temp_dir,
                config=controls.WatchConfig(
                    confirmations=2,
                    holder_rescan_blocks=12,
                    max_rescan_blocks=120,
                    score_alert_delta=5,
                ),
            )
            watcher.store.subscribe(TOKEN)
            first = watcher.run_once()
            self.assertEqual(first["alerts"], 0)

            # Simulate a confirmed-block hash change without advancing the
            # head, so run_once takes the reorg branch rather than the
            # regular rescan branch.
            state = watcher.store.load()
            key = next(iter(state["subscriptions"]))
            state["subscriptions"][key]["last_processed_hash"] = "stale-hash"
            watcher.store.save(state)

            second = watcher.run_once()
            self.assertEqual(second["alerts"], 1)
            alerts = watcher.store.alert_path.read_text(
                encoding="utf-8"
            ).splitlines()
            alert = json.loads(alerts[-1])
            self.assertEqual(alert["type"], "reorg")
            self.assertIsNotNone(alert["timechain"])
            self.assertIsNotNone(alert["timechain"]["ring"])

    def test_custom_contract_activity_detects_sell_tax_increase(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            agent = FakeAgent()
            agent.chain_root = temp_dir
            watcher = controls.ChainseerWatcher(
                agent,
                control_root=temp_dir,
            )
            watcher.store.subscribe(TOKEN)
            watcher.run_once()

            agent.rpc.head = 103
            agent.sell_tax = "0.20"
            agent.rpc.logs = [
                {
                    "topics": ["0x" + "9" * 64],
                    "blockNumber": "0x65",
                    "transactionHash": "0x" + "8" * 64,
                    "logIndex": "0x0",
                }
            ]
            result = watcher.run_once()
            self.assertEqual(result["rescans"], 1)
            feed = watcher.store.read_alerts(TOKEN)
            self.assertEqual(feed[-1]["categories"], ["sellability"])
            self.assertIn("20.0%", feed[-1]["message"])

    def test_solana_watcher_rescans_on_confirmed_authority_change(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            timechain_agent = FakeAgent()
            analyzer = FakeSolanaAnalyzer(timechain_agent)
            watcher = controls.SolanaEventWatcher(
                analyzer,
                timechain_agent=timechain_agent,
                control_root=temp_dir,
                config=controls.SolanaWatchConfig(
                    minimum_rescan_slots=8,
                    max_reconcile_slots=900,
                ),
            )
            watcher.store.subscribe(SOLANA_MINT)
            first = watcher.run_once()
            self.assertEqual(first["rescans"], 1)
            self.assertEqual(first["alerts"], 0)

            analyzer.rpc.slot = 101
            analyzer.rpc.mint_authority = "authority-a"
            analyzer.rpc.signatures = [
                {
                    "signature": "signature-a",
                    "slot": 101,
                    "confirmationStatus": "confirmed",
                    "err": None,
                    "blockTime": 1_700_000_000,
                }
            ]
            second = watcher.run_once()
            self.assertEqual(second["material_events"], 1)
            self.assertEqual(second["rescans"], 1)
            self.assertEqual(second["alerts"], 1)
            alert = json.loads(
                watcher.store.alert_path.read_text(
                    encoding="utf-8"
                ).splitlines()[-1]
            )
            self.assertEqual(alert["network"], "solana")
            self.assertIn("mint_authority_changed", alert["reason"])
            self.assertEqual(
                alert["critical_events"][0]["category"],
                "authority",
            )
            self.assertEqual(
                alert["timechain"]["decision"],
                "SEAL",
            )
            self.assertEqual(
                timechain_agent.tc.rings[-1]["ring_type"],
                "solana_watch_transition",
            )

    def test_solana_watcher_debounces_small_holder_rotation(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            timechain_agent = FakeAgent()
            analyzer = FakeSolanaAnalyzer(timechain_agent)
            watcher = controls.SolanaEventWatcher(
                analyzer,
                timechain_agent=timechain_agent,
                control_root=temp_dir,
            )
            watcher.store.subscribe(SOLANA_MINT)
            watcher.run_once()

            analyzer.rpc.slot = 101
            analyzer.rpc.signatures = [
                {
                    "signature": "signature-b",
                    "slot": 101,
                    "confirmationStatus": "confirmed",
                    "err": None,
                }
            ]
            second = watcher.run_once()
            self.assertEqual(second["rescans"], 0)
            self.assertEqual(second["alerts"], 0)
            self.assertGreaterEqual(second["events_observed"], 1)

    def test_solana_watcher_separates_optional_rpc_failure_from_token_error(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            timechain_agent = FakeAgent()
            analyzer = FakeSolanaAnalyzer(timechain_agent)
            analyzer.rpc.fail_largest_accounts = True
            watcher = controls.SolanaEventWatcher(
                analyzer,
                timechain_agent=timechain_agent,
                control_root=temp_dir,
            )
            watcher.store.subscribe(SOLANA_MINT)
            result = watcher.run_once()
            self.assertEqual(result["errors"], [])
            self.assertEqual(result["rescans"], 1)
            self.assertEqual(
                result["infrastructure_indeterminate"][0]["source"],
                "solana_rpc.getTokenLargestAccounts",
            )
            diagnostic = result["infrastructure_indeterminate"][0]["message"]
            self.assertNotIn("https://", diagnostic)
            self.assertNotIn("secret", diagnostic)
            subscription = watcher.store.load()["subscriptions"][
                SOLANA_MINT
            ]
            self.assertIsNone(
                subscription["quick_snapshot"][
                    "top10_total_supply_pct"
                ]
            )

    def test_solana_liquidity_removal_is_a_critical_event(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            timechain_agent = FakeAgent()
            analyzer = FakeSolanaAnalyzer(timechain_agent)
            watcher = controls.SolanaEventWatcher(
                analyzer,
                timechain_agent=timechain_agent,
                control_root=temp_dir,
            )
            watcher.store.subscribe(SOLANA_MINT)
            watcher.run_once()
            analyzer.rpc.slot = 101
            analyzer.dexscreener.liquidity = 60_000
            result = watcher.run_once()
            self.assertEqual(result["alerts"], 1)
            alert = json.loads(
                watcher.store.alert_path.read_text(
                    encoding="utf-8"
                ).splitlines()[-1]
            )
            self.assertIn("liquidity_removed", alert["reason"])
            self.assertEqual(
                alert["critical_events"][0]["category"],
                "liquidity",
            )


class CalibrationTests(unittest.TestCase):
    @staticmethod
    def outcome_rings(index, adverse):
        analysis_index = index + 100
        provenance = {
            "block_pin": 1_000 + index,
            "fact_count": 1,
            "facts": [{
                "fact_id": "F0000",
                "source": "rpc",
                "query_hash": f"{index + 1:064x}",
                "response_hash": f"{index + 101:064x}",
                "block": 1_000 + index,
            }],
        }
        binding = analysis_evidence_binding(
            provenance,
            anchor_type="block_pin",
            anchor_value=1_000 + index,
        )
        analysis = {
            "index": analysis_index,
            "ring_type": "token_analysis",
            "ring_hash": f"{index + 201:064x}",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "payload": {
                "network": "robinhood",
                "token_address": "0x" + f"{index + 1:040x}",
                "risk_level": "Low",
                "legitimacy_score": 80,
                "provenance": provenance,
                **binding,
            },
        }
        calibration = {
            "original_risk_level": "Low",
            "adverse_security_event": adverse,
        }
        values = {
            "horizon_seconds": 3600,
            "rug_pull": adverse,
            "price_return_pct": -10 if adverse else 5,
        }
        outcome_record = build_outcome_record(
            analysis,
            values,
            # Must actually match the declared 1h horizon. This fixture used
            # to observe 24h after the analysis while still labelling itself
            # horizon_seconds=3600 -- exactly the mislabelling the outcome
            # timeliness gate now rejects, so it would no longer be
            # learning-eligible and calibration would see no usable data.
            observed_at="2026-01-01T01:00:00+00:00",
            outcome_provenance={**provenance, "block_pin": 2_000 + index},
            evidence_fact_ids=["F0000"],
            calibration=calibration,
        )
        outcome = {
            "index": index,
            "ring_type": "analysis_outcome",
            "payload": {
                "analysis_ring": analysis_index,
                "calibration": calibration,
                "other_outcomes": {"horizon_seconds": 3600},
                "market_outcomes": {"price_return_pct": -10 if adverse else 5},
                "outcome_record": outcome_record,
            },
        }
        return [analysis, outcome]

    def test_calibration_proposes_tighten_only_after_enough_outcomes(self):
        rings = [
            ring
            for index in range(20)
            for ring in self.outcome_rings(index, adverse=index < 4)
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = controls.CalibrationEngine(temp_dir)
            proposal = engine.propose(rings)
            self.assertEqual(proposal["status"], "proposed")
            self.assertEqual(proposal["direction"], "tighten_only")
            self.assertGreater(
                proposal["proposed_policy"]["min_trade_score"],
                proposal["current_policy"]["min_trade_score"],
            )
            self.assertEqual(
                proposal["proposed_policy"]["allowed_risk_levels"], ["Low"]
            )

    def test_calibration_does_not_change_policy_with_too_little_data(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            engine = controls.CalibrationEngine(temp_dir)
            proposal = engine.propose(self.outcome_rings(1, True))
            self.assertEqual(proposal["status"], "insufficient_data")
            self.assertFalse(engine.policy_path.exists())

    def test_legacy_unbound_outcome_is_excluded_from_calibration(self):
        legacy = {
            "index": 1,
            "ring_type": "analysis_outcome",
            "payload": {
                "analysis_ring": 100,
                "calibration": {
                    "original_risk_level": "Low",
                    "adverse_security_event": True,
                },
            },
        }
        metrics = controls.CalibrationEngine.summarize([legacy])
        self.assertEqual(metrics["sample_size"], 0)
        self.assertEqual(metrics["legacy_unbound_outcomes_excluded"], 1)

    def _hand_edited_proposal(self, **overrides):
        # adopt() takes an arbitrary proposal file path, not something bound
        # to propose()'s own output, so a real attack/misconfiguration looks
        # like this: a "proposed" status with a hand-edited proposed_policy
        # rather than anything propose() itself would ever emit.
        current = controls.CalibrationPolicy()
        from dataclasses import asdict

        proposed = asdict(current)
        proposed.update(overrides)
        return {
            "status": "proposed",
            "current_policy": asdict(current),
            "proposed_policy": proposed,
            "metrics": {"sample_size": current.min_outcomes},
        }

    def test_adopt_rejects_loosened_false_negative_rate(self):
        engine = controls.CalibrationEngine(tempfile.mkdtemp())
        proposal = self._hand_edited_proposal(max_false_negative_rate=0.5)
        with self.assertRaisesRegex(ValueError, "max_false_negative_rate"):
            engine.adopt(proposal, agent=None)

    def test_adopt_rejects_lowered_min_outcomes(self):
        engine = controls.CalibrationEngine(tempfile.mkdtemp())
        proposal = self._hand_edited_proposal(min_outcomes=1)
        with self.assertRaisesRegex(ValueError, "min_outcomes"):
            engine.adopt(proposal, agent=None)

    def test_adopt_rejects_widened_permit_block_drift(self):
        engine = controls.CalibrationEngine(tempfile.mkdtemp())
        proposal = self._hand_edited_proposal(max_permit_block_drift=50)
        with self.assertRaisesRegex(ValueError, "max_permit_block_drift"):
            engine.adopt(proposal, agent=None)

    def test_adopt_rejects_widened_quote_age(self):
        engine = controls.CalibrationEngine(tempfile.mkdtemp())
        proposal = self._hand_edited_proposal(max_quote_age_blocks=50)
        with self.assertRaisesRegex(ValueError, "max_quote_age_blocks"):
            engine.adopt(proposal, agent=None)

    def test_adopt_rejects_lengthened_permit_ttl(self):
        engine = controls.CalibrationEngine(tempfile.mkdtemp())
        proposal = self._hand_edited_proposal(permit_ttl_seconds=300)
        with self.assertRaisesRegex(ValueError, "permit_ttl_seconds"):
            engine.adopt(proposal, agent=None)


class TradePermitTests(unittest.TestCase):
    def test_permit_is_fresh_bound_and_non_signing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            agent = FakeAgent()
            agent.chain_root = temp_dir
            guard = controls.TradePermitGuard(
                agent,
                control_root=temp_dir,
                policy=controls.CalibrationPolicy(
                    max_permit_block_drift=2,
                    permit_ttl_seconds=90,
                ),
            )
            issued = 1_800_000_000
            permit = guard.authorize(
                TOKEN,
                amount_in="1000",
                recipient=RECIPIENT,
                quote=safe_quote(block=100),
                confirmations=2,
                private_routing=True,
                now=issued,
            )
            self.assertFalse(permit["signing_capability"])
            self.assertFalse(permit["broadcast_capability"])
            self.assertEqual(permit["analysis_block"], 100)
            verification = guard.verify(
                permit, current_block=101, now=issued + 10, consume=True
            )
            self.assertTrue(verification["valid"], verification["reasons"])
            self.assertIsNotNone(verification["consumed"])
            replay = guard.verify(
                permit, current_block=101, now=issued + 11
            )
            self.assertFalse(replay["valid"])
            self.assertIn("permit already consumed", replay["reasons"])

    def test_permit_refuses_analysis_hard_stop(self):
        agent = FakeAgent()
        original = agent.analyze_token

        def unsafe(*args, **kwargs):
            report = original(*args, **kwargs)
            report["analysis"]["hard_stop_overrides"] = [
                {"code": "HONEYPOT", "severity": "Critical"}
            ]
            return report

        agent.analyze_token = unsafe
        guard = controls.TradePermitGuard(
            agent, policy=controls.CalibrationPolicy()
        )
        with self.assertRaises(PermissionError):
            guard.authorize(
                TOKEN,
                amount_in="1000",
                recipient=RECIPIENT,
                quote=safe_quote(block=100),
            )


if __name__ == "__main__":
    unittest.main()
