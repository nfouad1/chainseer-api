"""Pre-effect decision commitments, asynchronous Timechain sealing, and
fail-closed execution gates for the Robinhood paper learner.

Covers every P0 finding from the production-reachability review:
- authorization fails closed when ANY revalidation input is omitted
- mutated persisted commitments (decision flip, extended expiry) are refused
- duplicate-action prevention is atomic under concurrency
- hard-stop digests are canonical (order/representation invariant)
"""
from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from chainseer_robinhood_commitments import (
    DecisionCommitmentError,
    DecisionCommitmentStore,
    canonical_hard_stop_digest,
    canonical_commitment_payload,
    commitment_hash,
    complete_commitment_hash,
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


AUTH_KWARGS = dict(
    current_block=1002,
    current_quote_hash=commitment_hash({"price": 2.5}),
    current_hard_stop_digest=canonical_hard_stop_digest(["liquidity_floor"]),
    current_ring_count=10,
    head_index=6,
    head_hash="0xhead",
    registry_epoch="epoch-7",
    simulation_ok=True,
)


class TempStoreCase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.store = DecisionCommitmentStore(self.root / "dc.sqlite3")
        self.gate = ExecutionGate(self.store)

    def tearDown(self):
        self._tmp.cleanup()

    def certify(self, **overrides):
        ttl = float(overrides.pop("ttl_seconds", 900))
        record = {
            "head_index": 5, "head_hash": "0xhead", "chain_root": "0xroot",
            "registry_epoch": "epoch-7", "ring_count": 10,
            "verification_result": "pass",
            "verifier_version": "test-v1", "published_at": "now",
            "expires_at": time.time() + 900,
        }
        record.update(overrides)
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
    def test_fingerprint_stable_across_retries(self):
        """Same evidence, different wall clock => same fingerprint."""
        first = self.store.create(make_spec())
        time.sleep(0.01)
        second = self.store.create(make_spec())
        self.assertTrue(second["duplicate"])
        self.assertEqual(first["commitment_id"], second["commitment_id"])
        self.assertEqual(first["idempotency_fingerprint"],
                         second["idempotency_fingerprint"])

    def test_complete_hash_covers_expiry_and_timestamps(self):
        rec = self.store.create(make_spec())
        loaded = self.store.get(rec["commitment_id"])
        self.assertTrue(loaded["commitment_hash_verified"])
        # Recompute over the full row (minus the hash and the mutable
        # executed_at) reproduces it.
        from chainseer_robinhood_commitments import _persisted_shape
        payload = _persisted_shape({
            k: v for k, v in loaded.items()
            if k not in ("commitment_hash", "executed_at",
                         "commitment_hash_verified", "tampered")})
        self.assertEqual(loaded["commitment_hash"],
                         complete_commitment_hash(payload))
        # The fingerprint alone would NOT change if expiry changed; the
        # complete hash must.
        mutated = dict(payload)
        mutated["expires_at"] = payload["expires_at"] + 999_999
        self.assertNotEqual(loaded["commitment_hash"],
                            complete_commitment_hash(mutated))

    def test_different_evidence_different_hash(self):
        a = commitment_hash({"evidence_block_pin": 1000})
        b = commitment_hash({"evidence_block_pin": 1001})
        self.assertNotEqual(a, b)

    def test_idempotency_collision_detected(self):
        self.store.create(make_spec())
        with self.assertRaises(DecisionCommitmentError) as ctx:
            self.store.create(make_spec(risk_score=0.99))
        self.assertEqual(ctx.exception.reason, "idempotency_collision")

    def test_hard_stop_digest_canonical(self):
        """Semantically identical stop sets produce identical digests:
        different orderings AND dict-vs-code representations."""
        d1 = canonical_hard_stop_digest([
            {"code": "V4_HOOK_UNAUDITED"}, {"code": "LIQUIDITY_FLOOR"}])
        d2 = canonical_hard_stop_digest([
            {"code": "LIQUIDITY_FLOOR"}, {"code": "V4_HOOK_UNAUDITED"}])
        d3 = canonical_hard_stop_digest(
            ["LIQUIDITY_FLOOR", "V4_HOOK_UNAUDITED"])
        self.assertEqual(d1, d2)
        self.assertEqual(d1, d3)


class DurabilityAndTamperTests(TempStoreCase):
    def test_commitment_survives_restart(self):
        rec = self.store.create(make_spec())
        reopened = DecisionCommitmentStore(self.root / "dc.sqlite3")
        loaded = reopened.get(rec["commitment_id"])
        self.assertIsNotNone(loaded)
        self.assertTrue(loaded["commitment_hash_verified"])
        self.assertEqual(load_integrity_certificate(self.root), {})

    def test_mutated_decision_detected_and_refused(self):
        """The reviewer's exact attack: flip REJECT to BUY_ELIGIBLE in the
        persisted row. Authorization must refuse."""
        rec = self.store.create(make_spec(decision="reject"))
        self.certify()
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE decision_commitments SET decision='BUY_ELIGIBLE'"
                " WHERE commitment_id=?", (rec["commitment_id"],))
        result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "commitment_tampered")

    def test_extended_expiry_detected(self):
        """Extending expires_at in the row must break the complete hash."""
        rec = self.store.create(make_spec())
        self.certify()
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE decision_commitments SET expires_at=?"
                " WHERE commitment_id=?",
                (time.time() + 999_999, rec["commitment_id"]))
        result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "commitment_tampered")

    def test_forced_termination_recovery(self):
        rec = self.store.create(make_spec())
        claimed = self.store.claim_seal_batch()
        self.assertEqual(len(claimed), 1)
        with self.store.connection() as connection:
            connection.execute(
                "UPDATE deferred_seals SET lease_until=? WHERE job_id=?",
                (time.time() - 1, claimed[0]["job_id"]))
        recovered = DecisionCommitmentStore(self.root / "dc.sqlite3")
        recovered.recover_expired_leases()
        again = recovered.claim_seal_batch()
        self.assertEqual(len(again), 1)
        self.assertEqual(again[0]["commitment_id"], rec["commitment_id"])


class FailClosedOmittedInputsTests(TempStoreCase):
    """The reviewer's negative case: omitting revalidation inputs must
    NEVER authorize."""

    def setUp(self):
        super().setUp()
        self.rec = self.gate.commit(
            make_spec(simulation_ok=False, idempotency_key="sim-false-key"))
        self.certify()

    def test_omitted_everything_refused(self):
        result = self.gate.authorize(self.rec["commitment_id"],
                                     current_block=1002)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "revalidation_inputs_missing")

    def test_missing_quote_hash_refused(self):
        kwargs = dict(AUTH_KWARGS); del kwargs["current_quote_hash"]
        kwargs["simulation_ok"] = True
        result = self.gate.authorize(self.rec["commitment_id"], **kwargs)
        self.assertFalse(result["allowed"])

    def test_missing_hard_stop_digest_refused(self):
        kwargs = dict(AUTH_KWARGS); del kwargs["current_hard_stop_digest"]
        result = self.gate.authorize(self.rec["commitment_id"], **kwargs)
        self.assertFalse(result["allowed"])

    def test_missing_head_info_refused(self):
        kwargs = dict(AUTH_KWARGS); kwargs.pop("head_index"); kwargs.pop("head_hash")
        result = self.gate.authorize(self.rec["commitment_id"], **kwargs)
        self.assertFalse(result["allowed"])

    def test_missing_ring_count_refused(self):
        kwargs = dict(AUTH_KWARGS); kwargs.pop("current_ring_count")
        result = self.gate.authorize(self.rec["commitment_id"], **kwargs)
        self.assertFalse(result["allowed"])

    def test_missing_simulation_ok_refused_even_if_stored_true(self):
        rec = self.gate.commit(
            make_spec(idempotency_key="sim-true-key"))  # stored sim ok
        kwargs = dict(AUTH_KWARGS); kwargs.pop("simulation_ok")
        result = self.gate.authorize(rec["commitment_id"], **kwargs)
        self.assertFalse(result["allowed"])

    def test_stored_simulation_false_refused_even_if_live_true(self):
        result = self.gate.authorize(self.rec["commitment_id"],
                                     **AUTH_KWARGS)  # live sim True
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "stored_simulation_failed")


class RejectionTests(TempStoreCase):
    def test_expired_commitment_rejected(self):
        """Expiry passes NATURALLY (ttl=1s, sleep past it) -- no row
        mutation, so the tamper check stays clean and we test pure expiry
        logic."""
        rec = self.gate.commit(make_spec(ttl_seconds=1))
        time.sleep(1.1)
        self.certify()
        result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "commitment_expired")

    def test_block_drift_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"],
            **{**AUTH_KWARGS, "current_block": 1000 + 50})
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "block_drift_exceeded")

    def test_quote_mismatch_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"],
            **{**AUTH_KWARGS, "current_quote_hash": "different"})
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "quote_changed")

    def test_hard_stop_change_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"],
            **{**AUTH_KWARGS, "current_hard_stop_digest": "changed"})
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "hard_stop_changed")

    def test_live_simulation_failure_rejected(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        result = self.gate.authorize(
            rec["commitment_id"], **{**AUTH_KWARGS, "simulation_ok": False})
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "simulation_failed")

    def test_missing_certificate_fails_closed(self):
        rec = self.gate.commit(make_spec())
        result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_missing")

    def test_stale_certificate_fails_closed(self):
        rec = self.gate.commit(make_spec())
        self.certify(ttl_seconds=-10)
        result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_stale")

    def test_invalid_certificate_fails_closed(self):
        rec = self.gate.commit(make_spec())
        self.certify(verification_result="fail")
        result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_invalid")

    def test_certificate_behind_rings_fails_closed(self):
        rec = self.gate.commit(make_spec())
        self.certify(ring_count=3)
        result = self.gate.authorize(
            rec["commitment_id"],
            **{**AUTH_KWARGS, "current_ring_count": 3 + 6})
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_behind_rings")

    def test_registry_mismatch_fails_closed(self):
        rec = self.gate.commit(
            make_spec(faculty_registry_epoch="epoch-8"))
        self.certify(registry_epoch="epoch-7")
        result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertIn(result["reason"], {
            "certificate_registry_mismatch",
            "certificate_commitment_epoch_mismatch"})

    def test_head_mismatch_fails_closed(self):
        rec = self.gate.commit(make_spec())
        # Certificate published for head 0xdifferent; observed head 0xhead.
        self.certify(head_hash="0xdifferent")
        result = self.gate.authorize(
            rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "certificate_head_mismatch")

    def test_excessive_seal_debt_rejected_but_exit_allowed(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        with patch.object(
            type(self.store), "seal_debt",
            lambda self: {
                "pending_seals": 999999, "retrying": 0, "dead_letter": 0,
                "sealed": 0, "oldest_pending_age_seconds": 1.0,
                "latest_sealed_commitment": None},
        ):
            result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertTrue(str(result["reason"]).startswith("seal_debt_"))
        self.assertTrue(self.gate.authorize_protective_exit()["allowed"])

    def test_missing_commitment_rejected(self):
        self.certify()
        result = self.gate.authorize("dc-doesnotexist", **AUTH_KWARGS)
        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "commitment_missing")


class AtomicDuplicatePreventionTests(TempStoreCase):
    def test_concurrent_authorization_allows_exactly_one(self):
        """The reviewer's finding: two concurrent authorizations both got
        allowed=True. With the atomic claim exactly one must win."""
        rec = self.gate.commit(make_spec())
        self.certify()
        allowed: list[bool] = []

        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            result = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
            allowed.append(bool(result["allowed"]))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(sum(allowed), 1)

    def test_replay_after_execute_refused(self):
        rec = self.gate.commit(make_spec())
        self.certify()
        first = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertTrue(first["allowed"])
        second = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(second["allowed"])
        # Either reason is safe: the atomic claim blocks the replay (the
        # first authorization claimed the slot), or the executed event
        # recorded by confirm_action does.
        self.assertIn(second["reason"],
                      {"already_claimed", "commitment_not_active"})

    def test_duplicate_create_is_not_a_second_slot(self):
        first = self.gate.commit(make_spec())
        dup = self.gate.commit(make_spec())
        self.assertTrue(dup["duplicate"])
        self.certify()
        ok = self.gate.authorize(first["commitment_id"], **AUTH_KWARGS)
        self.assertTrue(ok["allowed"])
        again = self.gate.authorize(dup["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(again["allowed"])


class ProtectiveExitTests(TempStoreCase):
    def test_exit_allowed_under_total_degradation(self):
        rec = self.gate.commit(make_spec())
        # No certificate yet: the buy is refused...
        refused = self.gate.authorize(rec["commitment_id"], **AUTH_KWARGS)
        self.assertFalse(refused["allowed"])
        # ...and even WITHOUT any integrity, the protective exit is allowed.
        exit_result = self.gate.authorize_protective_exit()
        self.assertTrue(exit_result["allowed"])
        self.assertEqual(self.gate.metrics()["protective_exits_allowed"], 1)

    def test_fail_closed_state_label(self):
        self.assertEqual(overall_gate_state(False, False, []), "FAIL_CLOSED")
        self.assertEqual(overall_gate_state(True, False, []), "DEGRADED")
        self.assertEqual(overall_gate_state(False, True, []), "DEGRADED")
        self.assertEqual(overall_gate_state(True, True, []), "HEALTHY")


class SealQueueTests(TempStoreCase):
    def test_retry_then_dead_letter(self):
        from chainseer_robinhood_commitments import SEAL_RETRY_MAX_ATTEMPTS
        rec = self.store.create(make_spec())
        job = self.store.claim_seal_batch()[0]
        state = None
        for _ in range(SEAL_RETRY_MAX_ATTEMPTS + 1):
            state = self.store.fail_seal(job["job_id"], "boom")
            if state == "dead_letter":
                break
            with self.store.connection() as connection:
                connection.execute(
                    "UPDATE deferred_seals SET available_at=?"
                    " WHERE job_id=?", (time.time() - 1, job["job_id"]))
            self.store.requeue_retrying()
            job = self.store.claim_seal_batch()[0]
        self.assertEqual(state, "dead_letter")
        self.assertEqual(self.store.seal_debt()["dead_letter"], 1)

    def test_rejected_decision_queued_too(self):
        rec = self.store.create(make_spec(decision="REJECT"))
        jobs = self.store.claim_seal_batch()
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["commitment_id"], rec["commitment_id"])


class ObservabilityTests(TempStoreCase):
    def test_metrics_are_durable_across_gate_instances(self):
        """The reviewer's finding: dashboard built a new gate instance and
        metrics reset to zero. Metrics now live in the store."""
        rec = self.gate.commit(make_spec())
        fresh_gate = ExecutionGate(
            DecisionCommitmentStore(self.root / "dc.sqlite3"))
        self.assertEqual(fresh_gate.metrics()["commitments_created"], 1)
        self.assertEqual(self.gate.metrics()["commitments_created"], 1)

    def test_snapshot_states_and_metrics(self):
        self.gate.commit(make_spec())
        snapshot = self.gate.snapshot()
        self.assertEqual(snapshot["state"], "DEGRADED")
        self.assertTrue(snapshot["explanation"])
        self.certify()
        healthy = self.gate.snapshot()
        self.assertEqual(healthy["state"], "HEALTHY")
        metrics = healthy["metrics"]
        self.assertGreaterEqual(metrics.get("commitments_created", 0), 1)
        # Counters that have not fired yet are simply absent from the
        # durable store; the dashboard contract is "present once non-zero".

    def test_latency_sample_recorded_after_seal(self):
        self.store.create(make_spec())
        job = self.store.claim_seal_batch()[0]
        self.store.complete_seal(job["job_id"], 11, "0xring")
        samples = self.store.seal_latency_samples()
        self.assertEqual(len(samples), 1)
        self.assertGreaterEqual(samples[0], 0)


if __name__ == "__main__":
    unittest.main()
