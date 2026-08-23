"""Pre-effect decision commitments, asynchronous Timechain sealing, and
fail-closed execution gates for the Robinhood paper learner.

The decision path never performs full-chain verification and never creates
Timechain rings: it appends a minimal durable commitment (with a canonical
evidence hash, block pin, quote pin, policy version, registry epoch and
expiry), revalidates immediately before any risk-increasing action, and
queues the full Timechain ring for the analysis lane -- the single
authoritative writer. Full-chain verification runs asynchronously and
publishes a cached integrity certificate the fast path consumes.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from chainseer_core import safe_float, safe_int
from chainseer_outcome_ledger import canonical_hash

from chainseer_robinhood_commitments import (
    CERTIFICATE_FILE_NAME,
    DEFAULT_MAX_BLOCK_DRIFT_BLOCKS,
    DecisionCommitmentError,
    DecisionCommitmentStore,
    evaluate_integrity_certificate,
    evaluate_seal_debt,
    load_integrity_certificate,
    overall_gate_state,
)


class ExecutionGate:
    """The revalidation boundary immediately before a risk-increasing action.

    Every check fails closed for buys/increased exposure. Protective sells
    and exposure-reducing emergency exits bypass this gate entirely: a
    degraded chain must never trap the learner in a position.
    """

    def __init__(self, store: DecisionCommitmentStore,
                 *, max_block_drift: int = DEFAULT_MAX_BLOCK_DRIFT_BLOCKS):
        self.store = store
        self.max_block_drift = int(max_block_drift)
        self.metrics = {
            "commitments_created": 0,
            "commit_failures": 0,
            "duplicate_actions_prevented": 0,
            "rejected_stale_integrity": 0,
            "rejected_evidence_drift": 0,
            "rejected_excessive_seal_debt": 0,
            "rejected_expired": 0,
            "rejected_other": 0,
            "protective_exits_allowed": 0,
        }

    # -- creation ------------------------------------------------------ #
    def commit(self, spec: dict) -> dict:
        """Append the durable commitment. Raises on failure: the caller must
        not act. Rejected decisions are committed too."""
        try:
            record = self.store.create(spec)
        except DecisionCommitmentError:
            self.metrics["commit_failures"] += 1
            raise
        self.metrics["commitments_created"] += 1
        if record.get("duplicate"):
            self.metrics["duplicate_actions_prevented"] += 1
        return record

    # -- revalidation --------------------------------------------------- #
    def authorize(self, commitment_id: str, *, current_block: int,
                  current_quote_hash: str | None = None,
                  current_hard_stop_digest: str | None = None,
                  simulation_ok: bool = True,
                  current_ring_count: int | None = None,
                  head_index: int | None = None,
                  head_hash: str | None = None,
                  registry_epoch: str | None = None) -> dict:
        record = self.store.get(commitment_id)
        if record is None:
            return self._refuse("commitment_missing")
        if record.get("decision") != "BUY_ELIGIBLE":
            return self._refuse("not_a_buy_decision")
        events = {e["status"] for e in
                  self.store.latest_events(commitment_id, limit=20)}
        if events & {"aborted", "superseded", "executed"}:
            return self._refuse("commitment_not_active", sorted(events))
        now = time.time()
        if now > safe_float(record.get("expires_at"), 0.0):
            self.store.record_event(
                commitment_id, "aborted", "expired at authorization")
            return self._refuse("commitment_expired")
        drift = int(current_block) - int(record["evidence_block_pin"])
        if drift < 0 or drift > self.max_block_drift:
            self.store.record_event(
                commitment_id, "aborted", f"block drift {drift}")
            return self._refuse("block_drift_exceeded", drift)
        if current_quote_hash is not None and \
                current_quote_hash != record["quote_hash"]:
            self.store.record_event(
                commitment_id, "aborted", "quote changed")
            return self._refuse("quote_changed")
        if current_hard_stop_digest is not None and \
                current_hard_stop_digest != record["hard_stop_digest"]:
            self.store.record_event(
                commitment_id, "aborted", "hard-stop state changed")
            return self._refuse("hard_stop_changed")
        if not simulation_ok:
            self.store.record_event(
                commitment_id, "aborted", "simulation failed")
            return self._refuse("simulation_failed")
        certificate = load_integrity_certificate(self.store.path.parent)
        integrity_ok, integrity_reason, _ = evaluate_integrity_certificate(
            certificate, now=now, current_ring_count=current_ring_count)
        # The cached certificate must also agree with the head the caller
        # sees and with the registry epoch the commitment was minted under.
        if integrity_ok:
            if head_index is not None and safe_int(
                    certificate.get("head_index"), None) is not None and \
                    safe_int(certificate["head_index"], 0) > int(head_index):
                integrity_ok, integrity_reason = False, \
                    "certificate_head_ahead_of_observed_head"
            if head_hash is not None and \
                    certificate.get("head_hash") not in (None, head_hash):
                integrity_ok, integrity_reason = False, \
                    "certificate_head_mismatch"
            if registry_epoch is not None and \
                    certificate.get("registry_epoch") not in \
                    (None, registry_epoch):
                integrity_ok, integrity_reason = False, \
                    "certificate_registry_mismatch"
        # The certificate must also describe the SAME registry epoch the
        # commitment was minted under, otherwise policy could have changed
        # between commitment and execution.
        if integrity_ok and record.get("faculty_registry_epoch") and \
                str(certificate.get("registry_epoch") or "") not in (
                    "", str(record["faculty_registry_epoch"])):
            integrity_ok, integrity_reason = False, \
                "certificate_commitment_epoch_mismatch"
        if not integrity_ok:
            self.metrics["rejected_stale_integrity"] += 1
            self.store.record_event(
                commitment_id, "aborted", integrity_reason)
            return self._refuse(integrity_reason)
        debt = self.store.seal_debt()
        debt_ok, debt_reason = evaluate_seal_debt(debt)
        if not debt_ok:
            self.metrics["rejected_excessive_seal_debt"] += 1
            self.store.record_event(commitment_id, "aborted", debt_reason)
            return self._refuse(debt_reason, debt)
        return {
            "allowed": True, "reason": "authorized",
            "commitment": record, "seal_debt": debt,
            "integrity": certificate,
        }

    def _refuse(self, reason: str, detail=None) -> dict:
        if reason == "commitment_expired":
            self.metrics["rejected_expired"] += 1
        elif reason in ("quote_changed", "block_drift_exceeded",
                        "hard_stop_changed"):
            self.metrics["rejected_evidence_drift"] += 1
        elif reason not in ("commitment_missing", "not_a_buy_decision",
                            "simulation_failed", "commitment_not_active"):
            self.metrics["rejected_other"] += 1
        return {"allowed": False, "reason": reason, "detail": detail}

    # -- protective exits ----------------------------------------------- #
    def authorize_protective_exit(self) -> dict:
        """Never blocks. Sealing debt, stale certificates and even a
        corrupt store must not prevent exposure-reducing actions."""
        self.metrics["protective_exits_allowed"] += 1
        return {"allowed": True, "reason": "protective_exit_always_allowed"}

    # -- post-action ----------------------------------------------------- #
    def record_executed(self, commitment_id: str, detail: str) -> None:
        self.store.record_event(commitment_id, "executed", detail)

    def supersede(self, commitment_id: str, detail: str) -> None:
        """Never mutates the commitment; appends a superseding event."""
        self.store.record_event(commitment_id, "superseded", detail)

    # -- observability --------------------------------------------------- #
    def snapshot(self) -> dict:
        debt = self.store.seal_debt()
        certificate = load_integrity_certificate(self.store.path.parent)
        integrity_ok, integrity_reason, integrity_detail = \
            evaluate_integrity_certificate(certificate)
        debt_ok, debt_reason = evaluate_seal_debt(debt)
        explanation: list[str] = []
        if not integrity_ok:
            explanation.append(
                f"integrity: {integrity_reason} "
                f"(certificate age limit and ring lag are policy-fixed)")
        if not debt_ok:
            explanation.append(f"seal debt: {debt_reason}")
        samples = self.store.seal_latency_samples()
        return {
            "state": overall_gate_state(integrity_ok, debt_ok, explanation),
            "explanation": explanation,
            "integrity": {
                "ok": integrity_ok, "reason": integrity_reason,
                "head_index": certificate.get("head_index"),
                "head_hash": certificate.get("head_hash"),
                "ring_count": certificate.get("ring_count"),
                "published_at": certificate.get("published_at"),
                "expires_at": certificate.get("expires_at"),
                "detail": integrity_detail,
            },
            "seal_debt": {
                **debt,
                "ok": debt_ok, "reason": debt_reason,
                "throughput_samples": len(samples),
                "median_latency_seconds": (
                    round(sorted(samples)[len(samples) // 2], 2)
                    if samples else None),
            },
            "metrics": dict(self.metrics),
        }


def build_commitment_spec(
    *, run_id: str, network: str, token_address: str, evidence: dict,
    evidence_block_pin: int, quote: dict, quote_block: int, decision: str,
    hard_stops: list, policy_version: str, faculty_registry_epoch: str,
    verified_head: dict | None, simulation_ok: bool, risk_score=None,
    ttl_seconds: float | None = None, idempotency_key: str,
) -> dict:
    """Canonical commitment payload. Hashes are deterministic: identical
    evidence always produces the identical idempotency key and hash."""
    return {
        "run_id": run_id,
        "network": network,
        "token_address": str(token_address).lower(),
        "evidence_hash": canonical_hash(evidence),
        "evidence_block_pin": int(evidence_block_pin),
        "quote_hash": canonical_hash(quote),
        "quote_block": int(quote_block),
        "decision": str(decision).upper(),
        "risk_score": risk_score,
        "hard_stop_digest": canonical_hash(sorted(map(str, hard_stops))),
        "policy_version": policy_version,
        "faculty_registry_epoch": faculty_registry_epoch,
        "previous_verified_head_index": safe_int(
            (verified_head or {}).get("head_index"), None),
        "previous_verified_head_hash": (verified_head or {}).get("head_hash"),
        "simulation_ok": bool(simulation_ok),
        "ttl_seconds": ttl_seconds,
        "idempotency_key": idempotency_key,
    }
