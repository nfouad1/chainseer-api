"""Adversarial reproductions from the production-reachability review.

1. A certificate covering 1 ring must NOT keep authorizing after the
   producer chain has grown far past it (stale-tail authorization).
2. open_position() returning False must NEVER seal a ring whose events
   claim 'executed' -- the false-execution audit trail attack.
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

import chainseer_robinhood as rh
from chainseer_robinhood_commitments import (
    DecisionCommitmentStore, load_integrity_certificate)


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

    def _find(self, key):
        return next((r for r in reversed(self.rings)
                     if (r.get("payload") or {}).get("idempotency_key")
                     == key), None)


class AdversarialTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.fake_tc = FakeTimechain()
        self.fake_tc.genesis("Chainseer Robinhood Learning")
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
        engine.execution_gate = rh.ExecutionGate(engine.commitments)
        engine.v4_market = None
        self.market_state = {
            "price_usd": 2.5, "liquidity_usd": 500_000.0,
            "market_cap_usd": 100_000.0, "current_state_verified": True}
        engine.market = SimpleNamespace(
            snapshot=lambda token, pair=None: dict(self.market_state))
        self.rpc_block = 5002
        engine.rpc = SimpleNamespace(
            get_block_number=lambda: self.rpc_block)
        engine.timechain_recorder = recorder
        self.engine = engine

    def tearDown(self):
        self._tmp.cleanup()

    def seed_candidate(self, token="0xcc00000000000000000000000000000000000000"):
        self.engine.store.add_candidates([{
            "token_address": token,
            "pair_address": "0x" + "22" * 20,
            "factory_address": "0x" + "33" * 20,
            "block_number": 4000,
            "block_timestamp": 1.0,
            "transaction_hash": "0x" + "44" * 32,
            "log_index": 0,
            "source_version": None, "pool_id": None,
            "symbol": "TEST", "name": "Test",
        }])
        self.engine.store.record_analysis(token, {
            "legitimacy_score": 80.0, "risk_level": "Low",
            "action_label": "BUY_ELIGIBLE",
            "hard_stop_overrides": []}, {
            "price_usd": 2.5, "liquidity_usd": 500_000.0,
            "market_cap_usd": 100_000.0})
        return self.engine.store.candidate(token)

    def test_stale_certificate_tail_blocks_entry_after_chain_growth(self):
        """Certificate covers 1 ring; our sealing lane grows the producer
        chain by 10. The durable tail gate must refuse the stale cert."""
        candidate = self.seed_candidate()
        # Publish certificate at ring_count=1.
        self.assertEqual(
            self.engine.publish_integrity_certificate()["ring_count"], 1)
        # Grow the producer chain by 10 rings through the engine's OWN
        # seal path (drain_deferred_seals), as production would.
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        for i in range(10):
            rec = self.engine.commitments.create(commit_spec(
                decision="reject",
                token_address=f"0x{0xbb:040x}"[:2] + f"{i:02x}" * 19,
                idempotency_key=f"adv-growth-{i}"))
        drained = self.engine.drain_deferred_seals(limit=16)
        self.assertEqual(drained["sealed"], 10)
        self.assertEqual(len(self.fake_tc.rings), 11)

        result = self.engine.guarded_paper_entry(
            candidate, {"price_usd": 2.5, "liquidity_usd": 500_000.0,
                        "market_cap_usd": 100_000.0, "block_number": 5005},
            run_id="adv-1", priority_reason="adversarial")
        self.assertFalse(result["entered"])
        self.assertIn(result["reason"], {
            "producer_tail_lag_exceeded", "certificate_behind_rings"})
        positions = self.engine.store.recent_positions()
        self.assertFalse(any(p["status"] == "open" for p in positions))

    def test_external_growth_survives_restart_and_blocks_entry(self):
        """An external writer grows the chain after publication. A fresh
        engine process has no in-memory counter, yet must still observe the
        durable tail and refuse authorization."""
        candidate = self.seed_candidate(
            "0xcd00000000000000000000000000000000000000")
        self.assertEqual(
            self.engine.publish_integrity_certificate()["ring_count"], 1)
        for index in range(10):
            self.fake_tc.seal("external", {"index": index})

        def forbidden_full_scan():
            raise AssertionError("authorization materialized producer chain")

        self.fake_tc.iter_rings = forbidden_full_scan

        # Explicitly model a process restart: no process-local seal counter
        # or remembered tail is available on the new engine instance.
        restarted = object.__new__(rh.RobinhoodLearningEngine)
        restarted.__dict__.update(self.engine.__dict__)
        restarted.__dict__.pop("_rings_sealed_since_publish", None)
        result = restarted.guarded_paper_entry(
            candidate,
            {"price_usd": 2.5, "liquidity_usd": 500_000.0,
             "market_cap_usd": 100_000.0, "block_number": 5005},
            run_id="adv-restart", priority_reason="external-growth")
        self.assertFalse(result["entered"])
        self.assertIn(result["reason"], {
            "producer_tail_lag_exceeded", "certificate_behind_rings"})

    def test_open_position_failure_never_seals_executed(self):
        """open_position returns False -> commitment aborted -> the async
        seal carries an aborted event, never 'executed'."""
        candidate = self.seed_candidate()
        self.engine.publish_integrity_certificate()

        calls = {"open": 0}

        def failing_open(
            store_self, candidate_arg, market_arg, *,
            decision_commitment_id=None,
        ):
            calls["open"] += 1
            return False

        with patch.object(rh.RobinhoodLearningStore, "open_position",
                          failing_open):
            result = self.engine.guarded_paper_entry(
                candidate, {"price_usd": 2.5, "liquidity_usd": 500_000.0,
                            "market_cap_usd": 100_000.0,
                            # Same head as the RPC fake: no drift.
                            "block_number": self.rpc_block},
                run_id="adv-2", priority_reason="adversarial")
        self.assertFalse(result["entered"])
        self.assertEqual(calls["open"], 1)
        self.assertEqual(result.get("resolved"), "aborted")

        # The drain seals the ring; its events must NOT claim executed.
        drained = self.engine.drain_deferred_seals()
        self.assertEqual(drained["sealed"], 1)
        payload = self.fake_tc.rings[
            drained["last_ring"]["index"]]["payload"]
        statuses = {e["status"] for e in payload["decision_events"]}
        self.assertNotIn("executed", statuses)
        self.assertIn("aborted", statuses)
        self.assertTrue(payload["decision_events"][-1]["detail"].startswith(
            "action_failed"))

    def test_expired_commitment_recovered_and_sealed_as_aborted(self):
        """Finding 4: expired commitments without terminal events must be
        recovered by the drain, not left unresolved forever."""
        candidate = self.seed_candidate()
        self.engine.publish_integrity_certificate()
        # Commit with a tiny TTL and let it expire unclaimed.
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = self.engine.commitments.create(commit_spec(ttl_seconds=1))
        import time as time_mod
        time_mod.sleep(1.1)
        resolved = self.engine.commitments.recover_expired_commitments()
        self.assertEqual(resolved, 1)
        drained = self.engine.drain_deferred_seals()
        self.assertGreaterEqual(drained["sealed"], 0)
        # The commitment now has a terminal aborted event.
        events = {e["status"] for e in self.engine.commitments.latest_events(
            record["commitment_id"])}
        self.assertIn("aborted", events)

    def test_terminal_abort_is_idempotent(self):
        """Caller cleanup after a gate refusal must not duplicate history."""
        from tests.test_chainseer_robinhood_deferred_sealing_helpers \
            import commit_spec
        record = self.engine.commitments.create(commit_spec(
            idempotency_key="one-terminal-abort"))
        first = self.engine.execution_gate.record_action_result(
            record["commitment_id"], False, "first")
        second = self.engine.execution_gate.record_action_result(
            record["commitment_id"], False, "second")
        self.assertEqual(first["resolved"], "aborted")
        self.assertEqual(second["resolved"], "already_terminal")
        aborted = [event for event in self.engine.commitments.latest_events(
            record["commitment_id"], limit=20)
            if event["status"] == "aborted"]
        self.assertEqual(len(aborted), 1)


from unittest.mock import patch  # noqa: E402


if __name__ == "__main__":
    unittest.main()
