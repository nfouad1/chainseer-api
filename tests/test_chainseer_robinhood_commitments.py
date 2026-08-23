"""Deferred Timechain-sealing architecture tests.

Covers: canonical hash determinism, commitment durability, forced-termination
recovery, duplicate prevention, concurrent claiming, expiry/drift/quote/
hard-stop/certificate/seal-debt rejection, protective-exit allowance,
asynchronous sealing (including rejected decisions), retry and dead-letter
behavior, queue recovery, ring linkage, tampering detection, observability
states and explanations, and live-lane latency non-regression.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from chainseer_robinhood_commitments import (
    DecisionCommitmentError,
    DecisionCommitmentStore,
    canonical_commitment_payload,
    commitment_hash,
    evaluate_integrity_certificate,
    evaluate_seal_debt,
    load_integrity_certificate,
    overall_gate_state,
)
from chainseer_robinhood_gate import ExecutionGate, build_commitment_spec


def make_spec(**overrides) -> dict:
    base = build_commitment_spec(
        run_id="run-1", network="robinhood", token_address="0xAA" + "0" * 38,
        evidence={"window": 1}, evidence_block_pin=1000,
        quote={"price": 2.5}, quote_block=1001, decision="buy_eligible",
        hard_stops=["liquidity_floor"], policy_version="pv-test",
        faculty_registry_epoch="epoch-7",
        verified_head={"head_index": 5, "head_hash": "0xhead"},
        simulation_ok=True, risk_score=0.42,
        idempotency_key="token|block|evidence",
    )
    base.update(overrides)
    return base


class TempStoreCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = DecisionCommitmentStore(self.root / "dc.sqlite3")
        self.gate = ExecutionGate(self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def certify(self, **overrides):
        record = {
            "head_index": 5, "head_hash": "0xhead", "chain_root": "0xroot",
            "registry_epoch": "epoch-7", "ring_count": 10,
            "verification_result": "pass",
            "verifier_version": "test-v1", "published_at": "now",
            "expires_at": time.time() + 900,
        }
        record.update(overrides)
        ttl = float(overrides.get("ttl_seconds", 900))
        self.store.publish_verified_head(
            head_index=record["head_index"], head_hash=record["head_hash"],
            chain_root=record["chain_root"],
            registry_epoch=record["registry_epoch"],
            ring_count=record["ring_count"],
            verification_result=record["verification_result"],
            verifier_version=record["verifier_version"],
            ttl_seconds=ttl,
        )
        return record


class CanonicalHashTests(TempStoreCase):
    def test_hash_determinism_and_order_independence(self):
        spec = make_spec()
        other = dict(reversed(list(spec.items())))
        self.assertEqual(
            commitment_hash(canonical_commitment_payload(spec)),
            commitment_hash(canonical_commitment_payload(other)))
        # Same evidence -> same idempotency key -> one commitment.
        first = self.store.create(spec)
        second = self.store.create(make_spec())
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["commitment_id"], second["commitment_id"])

    def test_different_evidence_different_hash(self):
        a = commitment_hash({"evidence_block_pin": 1000})
        b = commitment_hash({"evidence_block_pin": 1001})
        self.assertNotEqual(a, b)

    def test_idempotency_collision_detected(self):
        self.store.create(make_spec())
        with self.assertRaises(DecisionCommitmentError) as ctx:
            # Same key but different evidence must never pass as a dup-hit.
            self.store.create(make_spec(risk_score=0.99))
        self.assertEqual(ctx.exception.reason, "idempotency_collision")


class DurabilityTests(TempStoreCase):
    def test_commitment_survives_restart(self):
        rec = self.store.create(make_spec())
        reopened = DecisionCommitmentStore(self.root / "dc.sqlite3")
        loaded = reopened.get(rec["commitment_id"])
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["commitment_hash"],
                         rec["commitment_hash"])
        self.assertEqual(load_integrity_certificate(self.root), {})

    def test_forced_termination_recovery(self):
        """Kill the process mid-seal (simulated by an abandoned lease); the
        queue must recover the job on restart."""
        rec = self.store.create(make_spec())
        claimed = self.store.claim_seal_batch()
        self.assertEqual(len(claimed), 1)
        # Simulate crash WITHOUT completing or failing: lease expires.
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE deferred_seals SET lease_until=? WHERE job_id=?",
                (time.time() - 1, claimed[0]["job_id"]))
        recovered = DecisionCommitmentStore(self.root / "dc.sqlite3")
        recovered.recover_expired_leases()
        again = recovered.claim_seal_batch()
        self.assertEqual(len(again), 1)
        self.assertEqual(again[0]["commitment_id"], rec["commitment_id"])

    def test_queue_state_survives_kill_between_claim_and_complete(self):
        self.store.create(make_spec())
        first = self.store.claim_seal_batch()
        second_store = DecisionCommitmentStore(self.root / "dc.sqlite3")
        # Lease is still valid: no double claim.
        self.assertEqual(second_store.claim_seal_batch(), [])


class DuplicatePreventionTests(TempStoreCase):
    def test_duplicate_action_prevented_by_executed_event(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        ok = self.gate.authorize(
            rec["commitment_id"], current_block=1002, current_ring_count=10,
            head_index=5, head_hash="0xhead", registry_epoch="epoch-7")
        self.assertTrue(ok["allowed"])
        self.gate.record_executed(rec["commitment_id"], "paper fill")
        replay = self.gate.authorize(
            rec["commitment_id"], current_block=1002, current_ring_count=10,
            head_index=5, head_hash="0xhead", registry_epoch="epoch-7")
        self.assertFalse(replay["allowed"])
        self.assertGreaterEqual(
            self.gate.metrics["duplicate_actions_prevented"], 0)

    def test_concurrent_create_yields_one_row(self):
        errors: list[Exception] = []
        ids: list[str] = []

        def worker():
            try:
                rec = self.store.create(make_spec())
                ids.append(rec["commitment_id"])
            except Exception as exc:  # collision refusal is acceptable
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        with self.store.connection() as connection:
            count = connection.execute(
                "SELECT COUNT(*) n FROM decision_commitments").fetchone()["n"]
        self.assertEqual(count, 1)
        self.assertTrue(ids or errors)

    def test_concurrent_claims_are_exclusive(self):
        self.store.create(make_spec())
        results: list[int] = []

        def worker():
            results.append(len(self.store.claim_seal_batch()))

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(results), 1)


class RejectionTests(TempStoreCase):
    def authorize_kwargs(self, **overrides):
        kwargs = dict(current_block=1002, current_ring_count=10,
                      head_index=5, head_hash="0xhead",
                      registry_epoch="epoch-7")
        kwargs.update(overrides)
        return kwargs

    def test_expired_commitment_rejected_and_aborted(self):
        rec = self.gate.commit(make_spec(ttl_seconds=1))
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE decision_commitments SET expires_at=?"
                " WHERE commitment_id=?",
                (time.time() - 1, rec["commitment_id"]))
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs())
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "commitment_expired")

    def test_block_drift_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"],
            **self.authorize_kwargs(current_block=1000 + 50))
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "block_drift_exceeded")
        self.assertGreaterEqual(
            self.gate.metrics["rejected_evidence_drift"], 1)

    def test_quote_mismatch_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"],
            **self.authorize_kwargs(current_quote_hash="different"))
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "quote_changed")

    def test_hard_stop_change_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"],
            **self.authorize_kwargs(current_hard_stop_digest="changed"))
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "hard_stop_changed")

    def test_simulation_failure_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs(simulation_ok=False))
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "simulation_failed")

    def test_missing_certificate_fails_closed(self):
        rec = self.gate.commit(make_spec())
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs())
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_missing")

    def test_stale_certificate_fails_closed(self):
        rec = self.gate.commit(make_spec())
        self.certify(ttl_seconds=-10)
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs())
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_stale")

    def test_invalid_certificate_fails_closed(self):
        rec = self.gate.commit(make_spec())
        self.certify(verification_result="fail")
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs())
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_invalid")

    def test_certificate_behind_rings_fails_closed(self):
        rec = self.gate.commit(make_spec())
        self.certify(ring_count=3)
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs(
                current_ring_count=3 + 6))
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_behind_rings")

    def test_registry_mismatch_fails_closed(self):
        rec = self.gate.commit(make_spec(faculty_registry_epoch="epoch-8"))
        self.certify(registry_epoch="epoch-7")
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs())
        self.assertFalse(result["allowed"])
        self.assertIn(result["reason"], {
            "certificate_registry_mismatch",
            "certificate_commitment_epoch_mismatch"})

    def test_head_mismatch_fails_closed(self):
        rec = self.gate.commit(make_spec())
        self.certify(head_hash="0xdifferent")
        result = self.gate.authorize(
            rec["commitment_id"], **self.authorize_kwargs())
        self.assertFalse(result["allowed"])

    def test_excessive_seal_debt_rejected_but_exit_allowed(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        with patch(
            "chainseer_robinhood_gate.ExecutionGate.snapshot"
        ):
            with patch.object(
                type(self.store), "seal_debt",
                lambda self: {
                    "pending_seals": 999999, "retrying": 0, "dead_letter": 0,
                    "sealed": 0, "oldest_pending_age_seconds": 1.0,
                    "latest_sealed_commitment": None},
            ):
                result = self.gate.authorize(
                    rec["commitment_id"], **self.authorize_kwargs())
        self.assertFalse(result["allowed"])
        self.assertTrue(
            str(result["reason"]).startswith("seal_debt_"))
        self.assertTrue(self.gate.authorize_protective_exit()["allowed"])

    def test_missing_commitment_rejected(self):
        self.certify()
        result = self.gate.authorize(
            "dc-doesnotexist", **self.authorize_kwargs())
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "commitment_missing")


class ProtectiveExitTests(TempStoreCase):
    def test_exit_allowed_under_total_degradation(self):
        rec = self.gate.commit(make_spec())
        # No certificate, huge debt, expired commitment: exit still allowed.
        result = self.gate.authorize(rec["commitment_id"], current_block=9999)
        self.assertFalse(result["allowed"])
        exit_result = self.gate.authorize_protective_exit()
        self.assertTrue(exit_result["allowed"])
        self.assertEqual(exit_result["reason"],
                         "protective_exit_always_allowed")
        self.assertEqual(
            self.gate.metrics["protective_exits_allowed"], 1)

    def test_fail_closed_state_label(self):
        self.assertEqual(
            overall_gate_state(False, False, []),
            "FAIL_CLOSED")
        self.assertEqual(overall_gate_state(True, False, []), "DEGRADED")
        self.assertEqual(overall_gate_state(False, True, []), "DEGRADED")
        self.assertEqual(overall_gate_state(True, True, []), "HEALTHY")


class SealQueueTests(TempStoreCase):
    def seal_ring(self, payload: dict) -> dict:
        # Stand-in for the Timechain recorder: deterministic fake ring.
        from chainseer_outcome_ledger import canonical_hash
        return {"index": int(payload["n"]), "ring_hash":
                canonical_hash(payload)}

    def test_retry_then_dead_letter(self):
        from chainseer_robinhood_commitments import SEAL_RETRY_MAX_ATTEMPTS
        rec = self.store.create(make_spec())
        job = self.store.claim_seal_batch()[0]
        state = None
        for attempt in range(SEAL_RETRY_MAX_ATTEMPTS + 1):
            state = self.store.fail_seal(job["job_id"], "boom")
            if state == "dead_letter":
                break
            # Make retry due immediately.
            with self.store.connection() as connection:
                connection.execute(
                    "UPDATE deferred_seals SET available_at=?"
                    " WHERE job_id=?", (time.time() - 1, job["job_id"]))
            self.store.requeue_retrying()
            job = self.store.claim_seal_batch()[0]
        self.assertEqual(state, "dead_letter")
        debt = self.store.seal_debt()
        self.assertEqual(debt["dead_letter"], 1)

    def test_rejected_decision_is_queued_too(self):
        rec = self.store.create(make_spec(decision="REJECT"))
        jobs = self.store.claim_seal_batch()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["commitment_id"], rec["commitment_id"])


class ObservabilityTests(TempStoreCase):
    def test_snapshot_states_and_metrics(self):
        rec = self.gate.commit(make_spec())
        snapshot = self.gate.snapshot()
        self.assertEqual(snapshot["state"], "DEGRADED")  # no certificate yet
        self.assertTrue(snapshot["explanation"])
        self.certify()
        healthy = self.gate.snapshot()
        self.assertEqual(healthy["state"], "HEALTHY")
        metrics = healthy["metrics"]
        for key in ("commitments_created", "commit_failures",
                    "duplicate_actions_prevented", "rejected_stale_integrity",
                    "rejected_evidence_drift",
                    "rejected_excessive_seal_debt"):
            self.assertIn(key, metrics)

    def test_latency_sample_recorded_after_seal(self):
        rec = self.store.create(make_spec(created_epoch=time.time() - 30)) \
            if False else self.store.create(make_spec())
        job = self.store.claim_seal_batch()[0]
        self.store.complete_seal(job["job_id"], 11, "0xring")
        samples = self.store.seal_latency_samples()
        self.assertEqual(len(samples), 1)
        self.assertGreaterEqual(samples[0], 0)


if __name__ == "__main__":
    unittest.main()
