import json
import tempfile
import time
import unittest
from pathlib import Path

import chainseer_controls as controls


TOKEN = "0x" + "1" * 40
PAIR = "0x" + "2" * 40
RECIPIENT = "0x" + "3" * 40


class FakeTimechain:
    def __init__(self, rings=None):
        self.rings = list(rings or [])

    def load(self):
        return list(self.rings)


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

    def get_logs(self, *_args, **_kwargs):
        return []


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
                "goplus_security": {"buy_tax": "0", "sell_tax": "0"},
            },
        }

    def reflect_on_analysis(
        self, analysis_ring, outcomes, evidence_fact_ids=None, observed_at=None
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
            self.assertIsNotNone(alert["timechain"]["ring"])


class CalibrationTests(unittest.TestCase):
    @staticmethod
    def outcome_ring(index, adverse):
        return {
            "index": index,
            "ring_type": "analysis_outcome",
            "payload": {
                "analysis_ring": index + 100,
                "calibration": {
                    "original_risk_level": "Low",
                    "adverse_security_event": adverse,
                },
                "other_outcomes": {"horizon_seconds": 3600},
                "market_outcomes": {"price_return_pct": -10 if adverse else 5},
            },
        }

    def test_calibration_proposes_tighten_only_after_enough_outcomes(self):
        rings = [
            self.outcome_ring(index, adverse=index < 4)
            for index in range(20)
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
            proposal = engine.propose([self.outcome_ring(1, True)])
            self.assertEqual(proposal["status"], "insufficient_data")
            self.assertFalse(engine.policy_path.exists())


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
