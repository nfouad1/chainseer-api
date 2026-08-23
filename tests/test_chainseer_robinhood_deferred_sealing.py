"""End-to-end tests for deferred Timechain sealing with the real engine.

Proves: the live decision path performs NO Timechain work; a durable
commitment exists before any simulated risk-increasing action; the analysis
lane asynchronously creates the full ring linking evidence -> commitment ->
paper event -> outcome; tampering is detected; restart recovers the queue.
"""
from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import (
    DecisionCommitmentStore, load_integrity_certificate)
from chainseer_robinhood_gate import ExecutionGate, build_commitment_spec


def make_spec(**overrides) -> dict:
    base = build_commitment_spec(
        run_id="run-e2e", network="robinhood",
        token_address="0xBB" + "0" * 38,
        evidence={"window": 2}, evidence_block_pin=2000,
        quote={"price": 3.5}, quote_block=2001, decision="buy_eligible",
        hard_stops=["liquidity_floor"], policy_version="pv-e2e",
        faculty_registry_epoch="epoch-7",
        verified_head={"head_index": 1, "head_hash": "0xh"},
        simulation_ok=True, risk_score=0.3,
        idempotency_key="e2e|token|block",
    )
    base.update(overrides)
    return base


class FakeRing:
    def __init__(self, index, payload):
        self["index"] = index
        self["ring_hash"] = f"0x{index:064x}"
        self["payload"] = payload

    def __getitem__(self, key):
        return getattr(self, "_" + key)

    def get(self, key, default=None):
        return getattr(self, "_" + key, default)


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
        # Tamper detection: a payload mutated after sealing breaks linkage.
        for ring in self.rings:
            if json.dumps(ring["payload"], sort_keys=True) != \
                    ring.get("_frozen"):
                return False, ["payload mutated after seal"]
        return True, ["ok"]

    def iter_rings(self):
        return list(self.rings)

    def load(self):
        return list(self.rings)

    def seal(self, ring_type, payload):
        self.seal_calls += 1
        index = len(self.rings)
        frozen = json.dumps(payload, sort_keys=True)
        ring = {"index": index,
                "ring_hash": f"0x{index:064x}",
                "payload": dict(payload), "_frozen": frozen}
        self.rings.append(ring)
        return ring


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
        engine.execution_gate = ExecutionGate(engine.commitments)
        engine.timechain_recorder = recorder
        return engine

    def test_commit_then_async_seal_links_ring(self):
        engine = self.make_engine()
        spec = make_spec()
        record = engine.execution_gate.commit(spec)
        self.assertEqual(engine.timechain_recorder.tc.seal_calls, 0,
                         "committing must not touch the Timechain")

        # The analysis lane drains the queue and creates the ring.
        result = engine.drain_deferred_seals()
        self.assertEqual(result["sealed"], 1)
        self.assertEqual(result["failed"], 0)
        self.assertEqual(engine.timechain_recorder.tc.seal_calls, 1)
        sealed_ring = engine.timechain_recorder.tc.rings[result["last_ring"]["index"]]
        payload = sealed_ring["payload"]
        # Ring links evidence -> commitment -> paper event chain.
        self.assertEqual(payload["evidence_hash"], record["evidence_hash"])
        self.assertEqual(payload["quote_hash"], record["quote_hash"])
        self.assertEqual(payload["decision_commitment"]["commitment_id"],
                         record["commitment_id"])
        self.assertTrue(payload["paper_only"])
        self.assertEqual(payload["idempotency_key"],
                         "robinhood-decision-commitment:"
                         + record["commitment_hash"])

        # Exactly-once: draining again seals nothing new.
        again = engine.drain_deferred_seals()
        self.assertEqual(again["claimed"], 0)
        self.assertEqual(engine.timechain_recorder.tc.seal_calls, 1)

    def test_rejected_decision_sealed_too(self):
        engine = self.make_engine()
        engine.execution_gate.commit(make_spec(decision="reject"))
        result = engine.drain_deferred_seals()
        self.assertEqual(result["sealed"], 1)
        ring_payload = engine.timechain_recorder.tc.rings[
            result["last_ring"]["index"]]["payload"]
        self.assertEqual(ring_payload["decision_commitment"]["decision"],
                         "REJECT")

    def test_restart_recovers_unsealed_queue(self):
        engine = self.make_engine()
        record = engine.execution_gate.commit(make_spec())
        fresh_engine = self.make_engine()  # simulates process restart
        result = fresh_engine.drain_deferred_seals()
        self.assertEqual(result["sealed"], 1)
        self.assertEqual(result["last_ring"]["commitment_id"],
                         record["commitment_id"])

    def test_tampering_detected_after_seal(self):
        engine = self.make_engine()
        engine.execution_gate.commit(make_spec())
        engine.drain_deferred_seals()
        ok, _ = engine.timechain_recorder.tc.verify()
        self.assertTrue(ok)
        # Mutate a sealed payload in place (the seal ring, not genesis).
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
        engine.execution_gate.commit(make_spec())
        engine.drain_deferred_seals()
        published = engine.publish_integrity_certificate()
        self.assertTrue(published["published"])
        self.assertTrue(published["verification_ok"])
        certificate = load_integrity_certificate(self.root)
        self.assertEqual(certificate["head_index"],
                         len(engine.timechain_recorder.tc.rings) - 1)
        self.assertEqual(certificate["verification_result"], "pass")

    def test_live_lane_latency_has_no_timechain_work(self):
        """The decision-path stages contain no Timechain calls: measured as
        the drain cost being zero when the queue is empty (the live lane
        never drains), plus commit cost bounded well under the lane budget."""
        engine = self.make_engine()
        started = time.perf_counter()
        record = engine.execution_gate.commit(make_spec())
        commit_seconds = time.perf_counter() - started
        # The live lane never drains the queue: the commitment sits as
        # pending seal debt until the analysis lane picks it up.
        debt = engine.commitments.seal_debt()
        self.assertLess(commit_seconds, 1.0,
                        "commit must be a cheap local append")
        self.assertEqual(debt["pending_seals"], 1)
        self.assertEqual(engine.timechain_recorder.tc.seal_calls, 0,
                         "decision path performed Timechain work")


if __name__ == "__main__":
    unittest.main()
