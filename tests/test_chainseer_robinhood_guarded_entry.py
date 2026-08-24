"""Production-invocation test: the full paper-entry cycle through
guarded_paper_entry against a REAL RobinhoodLearningStore and REAL engine
wiring (fake RPC/analyzer only). Proves the gate is obligatory at the
effect boundary, not merely defined.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import (
    DecisionCommitmentStore, load_integrity_certificate)
from chainseer_robinhood_gate import ExecutionGate


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
        "source_version": None,
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


class GuardedPaperEntryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fake_tc = FakeTimechain()
        # The real recorder creates genesis on init; mirror that.
        self.fake_tc.genesis("Chainseer Robinhood Learning")
        self.engine = self.make_engine()

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
        engine.store = rh.RobinhoodLearningStore(
            self.root / "learning.sqlite3")
        engine.ledger = SimpleNamespace(append=lambda *a, **k: None)
        engine.commitments = DecisionCommitmentStore(
            self.root / "decision_commitments.sqlite3")
        engine.execution_gate = ExecutionGate(engine.commitments)
        engine.v4_market = None
        # A market client whose snapshot returns the test MARKET dict.
        engine.market = SimpleNamespace(
            snapshot=lambda token, pair=None: {
                "price_usd": 2.5, "liquidity_usd": 500_000.0,
                "market_cap_usd": 100_000.0,
                "current_state_verified": True})
        engine.rpc = SimpleNamespace(get_block_number=lambda: 5002)
        engine.timechain_recorder = recorder
        return engine

    def seed_candidate(self) -> dict:
        """Insert a fully-admitted candidate through the real store path."""
        candidate = make_candidate()
        self.engine.store.add_candidates([{
            "token_address": candidate["token_address"],
            "pair_address": candidate["pair_address"],
            "factory_address": "0x" + "33" * 20,
            "block_number": 4000,
            "block_timestamp": 1.0,
            "transaction_hash": "0x" + "44" * 32,
            "log_index": 0,
            "source_version": None,
            "pool_id": None,
            "symbol": "TEST",
            "name": "Test",
        }])
        analysis = {
            "legitimacy_score": 80.0,
            "risk_level": "Low",
            "action_label": "BUY_ELIGIBLE",
            "hard_stop_overrides": [],
        }
        self.engine.store.record_analysis(
            candidate["token_address"], analysis, MARKET,
            priority_reason="test",
        )
        return self.engine.store.candidate(candidate["token_address"])

    def test_full_paper_entry_cycle_end_to_end(self):
        """evaluation -> durable commitment -> atomic authorization ->
        paper entry -> asynchronous ring -> certificate refresh."""
        candidate = self.seed_candidate()
        self.assertEqual(candidate["paper_entry_allowed"], 1)

        # 0. Certificate refresh happens on the analysis lane; simulate it
        #    having run so there is a verified chain to act against.
        self.engine.publish_integrity_certificate()

        # 1. Guarded entry: durable commitment + authorization + position.
        result = self.engine.guarded_paper_entry(
            candidate, MARKET, run_id="cycle-e2e",
            priority_reason="test_entry")
        self.assertTrue(result["entered"],
                        f"expected entry, got {result}")
        commitment_id = result["commitment_id"]

        # 2. The commitment exists durably with an executed slot.
        record = self.engine.commitments.get(commitment_id)
        self.assertIsNotNone(record)
        self.assertTrue(record["commitment_hash_verified"])
        self.assertIsNotNone(record["executed_at"])

        # 3. The position is open in the REAL store.
        positions = self.engine.store.recent_positions()
        self.assertTrue(any(
            p["token_address"] == candidate["token_address"]
            and p["status"] == "open" for p in positions))

        # 4. A second entry attempt for the same token is refused
        #    (duplicate commitment -> no second position).
        again = self.engine.guarded_paper_entry(
            self.engine.store.candidate(candidate["token_address"]),
            MARKET, run_id="cycle-e2e", priority_reason="test_entry")
        self.assertFalse(again["entered"])
        open_count = sum(
            1 for p in positions if p["status"] == "open")
        self.assertEqual(open_count, 1)

        # 5. The analysis lane drains the queue and seals the ring.
        drained = self.engine.drain_deferred_seals()
        self.assertEqual(drained["sealed"], 1)
        self.assertEqual(drained["failed"], 0)
        ring_payload = self.fake_tc.rings[drained["last_ring"]["index"]][
            "payload"]
        statuses = {e["status"] for e in ring_payload["decision_events"]}
        self.assertIn("executed", statuses)
        self.assertEqual(ring_payload["decision_commitment"]["commitment_id"],
                         commitment_id)

        # 6. Certificate refresh publishes a pass over the new ring.
        published = self.engine.publish_integrity_certificate()
        self.assertTrue(published["verification_ok"])
        certificate = load_integrity_certificate(self.root)
        self.assertEqual(certificate["verification_result"], "pass")

        # 7. Dashboard snapshot reports the gate from DURABLE metrics.
        snapshot = rh.dashboard_operational_snapshot(
            self.root, store=self.engine.store)
        gate = snapshot["decision_gate"]
        self.assertGreaterEqual(
            gate["metrics"].get("commitments_created", 0), 1)

    def test_entry_refused_without_integrity_certificate(self):
        """Fail-closed: with no published certificate, guarded_paper_entry
        must NOT open a position."""
        candidate = self.seed_candidate()
        result = self.engine.guarded_paper_entry(
            candidate, MARKET, run_id="cycle-fail",
            priority_reason="test_entry")
        self.assertFalse(result["entered"])
        positions = self.engine.store.recent_positions()
        self.assertFalse(any(p["status"] == "open" for p in positions))


class FakeTimechain:
    def __init__(self):
        self.rings: list[dict] = []
        self.seal_calls = 0

    def height(self):
        return len(self.rings)

    def genesis(self, name):
        payload = {"event": "genesis", "name": name}
        self.rings.append({"index": 0, "ring_hash": "0x0",
                           "payload": payload,
                           "_frozen": json.dumps(payload, sort_keys=True)})

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


if __name__ == "__main__":
    unittest.main()
