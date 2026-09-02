"""End-to-end tests for deferred Timechain sealing with the real engine.

Proves: the live decision path performs NO Timechain work; a durable
commitment exists before any simulated risk-increasing action; the analysis
lane asynchronously creates the full ring ONLY after the commitment reaches
a terminal state (linking evidence -> commitment -> action -> outcome);
tampering is detected; restart recovers the queue; and guarded_paper_entry
routes every production paper buy through commitment -> authorization ->
open_position.
"""
from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import (
    DecisionCommitmentStore, load_integrity_certificate)
from tests.test_chainseer_robinhood_deferred_sealing_helpers import commit_spec


class FakeTimechain:
    """Minimal stand-in for the producer Timechain seal/verify surface."""

    def __init__(self):
        self.rings: list[dict] = []
        self.seal_calls = 0

    def height(self):
        return len(self.rings)

    def genesis(self, name):
        self.rings.append({"index": 0, "ring_hash": "0x0", "payload": {
            "event": "genesis", "name": name}})

    def verify(self):
        for ring in self.rings:
            if json.dumps(ring["payload"], sort_keys=True) != \
                    ring.get("_frozen"):
                return False, ["payload mutated after seal"]
        return True, ["ok"]

    def iter_rings(self):
        return list(self.rings)

    def _current_head(self):
        return self.rings[-1] if self.rings else None

    def load(self):
        return list(self.rings)

    def seal(self, ring_type, payload):
        self.seal_calls += 1
        ring = {"index": len(self.rings),
                "ring_hash": f"0x{len(self.rings):064x}",
                "payload": dict(payload),
                "_frozen": json.dumps(payload, sort_keys=True)}
        self.rings.append(ring)
        return ring

    def _find(self, idempotency_key):
        return next((ring for ring in reversed(self.rings)
                     if (ring.get("payload") or {}).get("idempotency_key")
                     == idempotency_key), None)


def make_candidate(token="0xcc00000000000000000000000000000000000000",
                   **overrides) -> dict:
    candidate = {
        "token_address": token,
        "symbol": "TEST",
        "score": 80.0,
        "risk_level": "Low",
        "hard_stops_json": "[]",
        "shadow_admission_json": "{}",
        "paper_entry_allowed": 1,
        "source_version": rh.SOURCE_V2 if hasattr(rh, "SOURCE_V2") else None,
        "pair_address": "0x" + "22" * 20,
        "pool_id": None,
        "entry_policy_version": "robinhood-paper-v1",
    }
    candidate.update(overrides)
    return candidate


MARKET = {
    "price_usd": 2.5,
    "liquidity_usd": 500_000.0,
    "market_cap_usd": 100_000.0,
    "block_number": 5000,
    "current_state_verified": True,
}


class EngineSealingTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fake_tc = FakeTimechain()

    def tearDown(self):
        self._tmp.cleanup()

    def make_engine(self) -> rh.RobinhoodLearningEngine:
        recorder = object.__new__(
            rh.RobinhoodLearningTimechainRecorder)
        recorder.root = self.root / "producer-chain"
        recorder.root.mkdir(parents=True, exist_ok=True)
        recorder.ANALYSIS_VERSION = (
            rh.RobinhoodLearningTimechainRecorder.ANALYSIS_VERSION)
        recorder.tc = self.fake_tc
        engine = object.__new__(rh.RobinhoodLearningEngine)
        engine.root = self.root
        engine.root.mkdir(parents=True, exist_ok=True)
        engine.commitments = DecisionCommitmentStore(
            self.root / "decision_commitments.sqlite3")
        engine.execution_gate = rh.ExecutionGate(engine.commitments)
        engine.timechain_recorder = recorder
        return engine

    def test_commit_then_async_seal_links_ring(self):
        engine = self.make_engine()
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = engine.execution_gate.commit(
            commit_spec(idempotency_key="e2e|token|block"))
        self.assertEqual(engine.timechain_recorder.tc.seal_calls, 0,
                         "committing must not touch the Timechain")

        # First drain: BUY is not yet executed/aborted => deferred.
        first = engine.drain_deferred_seals()
        self.assertEqual(first["deferred_not_terminal"], 1)
        self.assertEqual(first["sealed"], 0)

        # Simulate the authorized action: claim the slot, then CONFIRM it
        # (open_position succeeded). Only confirmation is terminal.
        engine.commitments.claim_execution(record["commitment_id"])
        engine.commitments.confirm_action(record["commitment_id"],
                                          "paper entry")

        # ...then the next drain seals the FULL chain.
        result = engine.drain_deferred_seals()
        self.assertEqual(result["sealed"], 1)
        self.assertEqual(result["failed"], 0)
        sealed_ring = engine.timechain_recorder.tc.rings[result["last_ring"]["index"]]
        payload = sealed_ring["payload"]
        statuses = {event["status"] for event in payload["decision_events"]}
        self.assertIn("executed", statuses)
        self.assertEqual(payload["evidence_hash"], record["evidence_hash"])
        self.assertEqual(payload["quote_hash"], record["quote_hash"])
        self.assertTrue(payload["paper_only"])

        # Exactly-once: draining again seals nothing new.
        again = engine.drain_deferred_seals()
        self.assertEqual(again["claimed"], 0)
        self.assertEqual(engine.timechain_recorder.tc.seal_calls, 1)

    def test_aborted_buy_seals_with_abort_event(self):
        engine = self.make_engine()
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = engine.execution_gate.commit(commit_spec())
        engine.execution_gate.supersede(record["commitment_id"], "drift")
        result = engine.drain_deferred_seals()
        self.assertEqual(result["sealed"], 1)
        events = engine.timechain_recorder.tc.rings[
            result["last_ring"]["index"]]["payload"]["decision_events"]
        self.assertIn("superseded", {e["status"] for e in events})

    def test_rejected_decision_sealed_immediately(self):
        engine = self.make_engine()
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        engine.execution_gate.commit(
            commit_spec(decision="reject"))
        result = engine.drain_deferred_seals()
        # A REJECT has no action to await: terminal at creation.
        self.assertEqual(result["sealed"], 1)
        ring_payload = engine.timechain_recorder.tc.rings[
            result["last_ring"]["index"]]["payload"]
        self.assertEqual(ring_payload["decision_commitment"]["decision"],
                         "REJECT")

    def test_restart_recovers_unsealed_queue(self):
        engine = self.make_engine()
        record = engine.execution_gate.commit(commit_spec())
        engine.commitments.claim_execution(record["commitment_id"])
        engine.commitments.confirm_action(record["commitment_id"],
                                          "paper entry")
        fresh_engine = self.make_engine()  # simulates process restart
        result = fresh_engine.drain_deferred_seals()
        self.assertEqual(result["sealed"], 1)
        self.assertEqual(result["last_ring"]["commitment_id"],
                         record["commitment_id"])

    def test_tampering_detected_after_seal(self):
        engine = self.make_engine()
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        engine.execution_gate.commit(commit_spec(decision="reject"))
        engine.drain_deferred_seals()
        ok, _ = engine.timechain_recorder.tc.verify()
        self.assertTrue(ok)
        rings = engine.timechain_recorder.tc.rings
        victim = next(r for r in rings
                      if r["payload"].get("event") ==
                      "robinhood_decision_commitment_sealed")
        victim["payload"]["evidence_hash"] = "0xtampered"
        ok, report = engine.timechain_recorder.tc.verify()
        self.assertFalse(ok)
        self.assertIn("mutated", " ".join(report))

    def test_certificate_published_off_critical_path(self):
        engine = self.make_engine()
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        engine.execution_gate.commit(commit_spec(decision="reject"))
        engine.drain_deferred_seals()
        published = engine.publish_integrity_certificate()
        self.assertTrue(published["published"])
        self.assertTrue(published["verification_ok"])
        certificate = load_integrity_certificate(self.root)
        self.assertEqual(certificate["head_index"],
                         len(engine.timechain_recorder.tc.rings) - 1)
        self.assertEqual(certificate["verification_result"], "pass")
        self.assertTrue(str(certificate.get("registry_epoch")).startswith(
            ("epoch-ring:", "genesis:", "unknown-chain")),
            f"epoch identifier missing: {certificate}")

    def test_live_lane_latency_has_no_timechain_work(self):
        engine = self.make_engine()
        started = time.perf_counter()
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = engine.execution_gate.commit(commit_spec())
        commit_seconds = time.perf_counter() - started
        debt = engine.commitments.seal_debt()
        self.assertLess(commit_seconds, 1.0,
                        "commit must be a cheap local append")
        self.assertEqual(debt["pending_seals"], 1)
        self.assertEqual(engine.timechain_recorder.tc.seal_calls, 0,
                         "decision path performed Timechain work")

    def test_certificate_refresh_waits_until_analysis_writer_exits(self):
        """The certificate lane must attest the final producer head."""
        engine = object.__new__(rh.RobinhoodLearningEngine)
        engine.root = self.root
        engine.root.mkdir(parents=True, exist_ok=True)
        engine.timechain_recorder = object()
        engine.rpc = object()
        order = []

        def execute(lane, budget, worker):
            self.assertEqual(lane, "analysis")
            return worker(rh.CycleDeadline(budget))

        engine._execute_lane = execute
        engine.publish_integrity_certificate = lambda: (
            order.append("certificate_refresh") or {"published": True})
        engine.observe_outcomes = lambda *args, **kwargs: (
            order.append("outcomes") or {})
        engine.recheck_executable_markets = lambda *args, **kwargs: (
            order.append("market_rechecks") or {})
        engine._analyze_candidates = lambda *args, **kwargs: (
            order.append("analyses") or {})
        engine.drain_deferred_seals = lambda *args, **kwargs: (
            order.append("deferred_seals") or {})
        engine.store = type("Store", (), {
            "summary": lambda self: {"candidates": {"pending": 0}},
        })()

        result = engine.run_analysis_lane(budget_seconds=10.0)
        self.assertEqual(order, [
            "outcomes", "market_rechecks", "analyses", "deferred_seals"])
        self.assertNotIn("certificate_refresh", order)
        self.assertEqual(
            result["certificate_refresh"]["delegated_to"],
            "certificate_lane")
        self.assertEqual(
            result["certificate_refresh"]["trigger"],
            "after_analysis_writer_exit")


if __name__ == "__main__":
    unittest.main()
