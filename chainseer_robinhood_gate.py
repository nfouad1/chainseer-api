"""Pre-effect decision commitments and the fail-closed execution gate.

Security invariants:
- Every security-critical revalidation input is REQUIRED. A missing quote
  hash, hard-stop digest, head, registry epoch, ring count or simulation
  result refuses authorization -- omission never means pass.
- The stored commitment's complete hash (covering timestamps, expiry and
  every authorization field) is verified on load; a mutated row fails.
- Authorization and the executed-slot claim are ONE atomic step: exactly
  one caller ever receives permission per commitment.
- Protective exits bypass this gate entirely: degraded integrity blocks
  buys, never exposure-reducing actions.

Terminology: a token is never "risk-free". It is "buy-eligible under the
current evidence and policy".
"""
from __future__ import annotations

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
    """The revalidation boundary immediately before a risk-increasing action."""

    def __init__(self, store: DecisionCommitmentStore,
                 *, max_block_drift: int = DEFAULT_MAX_BLOCK_DRIFT_BLOCKS):
        self.store = store
        self.max_block_drift = int(max_block_drift)

    # -- durable metrics ------------------------------------------------ #
    def _bump(self, metric: str) -> None:
        try:
            self.store.increment_metric(metric)
        except Exception:
            # Metrics are observability; never let them gate behavior.
            pass

    def metrics(self) -> dict[str, int]:
        return self.store.read_all_metrics()

    @property
    def metrics_dict(self) -> dict[str, int]:
        """Compatibility shim for earlier process-local access."""
        return self.metrics()

    # -- creation -------------------------------------------------------- #
    def commit(self, spec: dict) -> dict:
        """Append the durable commitment. Raises on failure: the caller must
        not act. Rejected decisions are committed too."""
        try:
            record = self.store.create(spec)
        except DecisionCommitmentError:
            self._bump("commit_failures")
            raise
        if not record.get("duplicate"):
            self._bump("commitments_created")
        else:
            self._bump("duplicate_actions_prevented")
        return record

    # -- revalidation -----------------------------------------------------#
    def authorize(
        self, commitment_id: str, *, current_block: int,
        current_quote_hash: str | None = None,
        current_hard_stop_digest: str | None = None,
        current_ring_count: int | None = None, head_index: int | None = None,
        head_hash: str | None = None, registry_epoch: str | None = None,
        simulation_ok: bool | None = None,
        integrity_enforced: bool = True,
    ) -> dict:
        """Revalidate EVERY input and atomically claim the action slot.

        All arguments except ``simulation_ok`` are REQUIRED keyword
        arguments: passing nothing for one is a rejection, not a pass.
        ``simulation_ok`` re-checks the live pre-trade simulation result;
        the commitment's stored result must ALSO be true.

        ``integrity_enforced`` is False ONLY in standalone paper-learning
        mode (no producer Timechain exists at all); every other caller
        gets the mandatory fail-closed certificate checks.
        """
        record = self.store.get(commitment_id)
        if record is None:
            return self._refuse("commitment_missing")
        if record.get("tampered"):
            self._bump("rejected_tampered_commitment")
            self.store.record_event(
                commitment_id, "aborted", "commitment hash mismatch")
            return self._refuse("commitment_tampered")
        if record.get("decision") != "BUY_ELIGIBLE":
            return self._refuse("not_a_buy_decision")

        # Required revalidation inputs. Missing => refuse (fail closed).
        required_inputs = {
            "current_quote_hash": current_quote_hash,
            "current_hard_stop_digest": current_hard_stop_digest,
            "head_index": head_index,
            "head_hash": head_hash,
            "registry_epoch": registry_epoch,
        }
        if integrity_enforced:
            required_inputs["current_ring_count"] = current_ring_count
        elif current_ring_count is not None:
            # Ring count is meaningless without a chain; drop it from the
            # authorized payload so it cannot be mistaken for verified.
            pass
        missing = [name for name, value in required_inputs.items()
                   if value is None or value == ""]
        if simulation_ok is None:
            missing.append("simulation_ok")
        if missing:
            return self._refuse("revalidation_inputs_missing", missing)

        now = time.time()
        events = {e["status"] for e in
                  self.store.latest_events(commitment_id, limit=20)}
        if events & {"aborted", "superseded", "executed"}:
            return self._refuse(
                "commitment_not_active", sorted(events))
        if now > safe_float(record.get("expires_at"), 0.0):
            self.store.record_event(
                commitment_id, "aborted", "expired at authorization")
            return self._refuse("commitment_expired")
        drift = int(current_block) - int(record["evidence_block_pin"])
        if drift < 0 or drift > self.max_block_drift:
            self.store.record_event(
                commitment_id, "aborted", f"block drift {drift}")
            return self._refuse("block_drift_exceeded", drift)
        if current_quote_hash != record["quote_hash"]:
            self.store.record_event(commitment_id, "aborted", "quote changed")
            return self._refuse("quote_changed")
        if current_hard_stop_digest != record["hard_stop_digest"]:
            self.store.record_event(
                commitment_id, "aborted", "hard-stop state changed")
            return self._refuse("hard_stop_changed")
        if not bool(record.get("simulation_ok")):
            self.store.record_event(
                commitment_id, "aborted", "stored simulation not ok")
            return self._refuse("stored_simulation_failed")
        if not simulation_ok:
            self.store.record_event(
                commitment_id, "aborted", "live simulation failed")
            return self._refuse("simulation_failed")

        certificate = None
        if integrity_enforced:
            certificate = load_integrity_certificate(self.store.path.parent)
            integrity_ok, integrity_reason, _ = (
                evaluate_integrity_certificate(
                    certificate, now=now,
                    current_ring_count=int(current_ring_count)))
            # The cached certificate must agree with the observed head and
            # with BOTH the caller's epoch and the epoch minted into the
            # commitment.
            if integrity_ok:
                cert_head = safe_int(certificate.get("head_index"), None)
                if cert_head is not None and cert_head > int(head_index):
                    integrity_ok, integrity_reason = False, \
                        "certificate_head_ahead_of_observed_head"
                elif certificate.get("head_hash") != head_hash:
                    integrity_ok, integrity_reason = False, \
                        "certificate_head_mismatch"
                elif certificate.get("registry_epoch") != registry_epoch:
                    integrity_ok, integrity_reason = False, \
                        "certificate_registry_mismatch"
                elif str(certificate.get("registry_epoch") or "") != \
                        str(record.get("faculty_registry_epoch") or ""):
                    integrity_ok, integrity_reason = False, \
                        "certificate_commitment_epoch_mismatch"
            if not integrity_ok:
                self._bump("rejected_stale_integrity")
                self.store.record_event(commitment_id, "aborted",
                                        integrity_reason)
                return self._refuse(integrity_reason)

        debt = self.store.seal_debt()
        debt_ok, debt_reason = evaluate_seal_debt(debt)
        if not debt_ok:
            self._bump("rejected_excessive_seal_debt")
            self.store.record_event(commitment_id, "aborted", debt_reason)
            return self._refuse(debt_reason, debt)

        # ATOMIC permission + duplicate-action prevention: the single
        # action slot is claimed inside this call. Two concurrent
        # authorizations cannot both succeed. The claim is PENDING --
        # the caller MUST confirm via claim_action()/confirm_action()
        # after the action actually succeeds, or abort on failure.
        claim = self.store.claim_execution(commitment_id)
        if not claim["claimed"]:
            self._bump("duplicate_actions_prevented")
            return self._refuse(claim["reason"])

        return {
            "allowed": True, "reason": "authorized",
            "commitment": record, "seal_debt": debt,
            "integrity": certificate,
        }

    def _refuse(self, reason: str, detail=None) -> dict:
        if reason in ("quote_changed", "block_drift_exceeded",
                      "hard_stop_changed", "commitment_expired"):
            self._bump("rejected_evidence_drift")
        elif reason == "already_claimed":
            pass  # counted by the caller as duplicate prevention
        elif reason == "commitment_tampered":
            pass  # counted above
        else:
            self._bump("rejected_other")
        return {"allowed": False, "reason": reason, "detail": detail}

    # -- post-action ---------------------------------------------------------#
    def record_action_result(self, commitment_id: str,
                             succeeded: bool, detail: str) -> dict:
        """Resolve the claimed slot based on the ACTUAL action result.

        - Success -> confirmed executed (the only path that seals an
          'executed' ring event).
        - Failure -> aborted with the reason; the commitment can never
          authorize again and its seal job carries the refusal context.
        Never records a false execution.
        """
        if succeeded:
            self.store.confirm_action(commitment_id, detail)
            return {"resolved": "executed"}
        self.store.abort_commitment(commitment_id, f"action_failed:{detail}")
        self._bump("actions_failed")
        return {"resolved": "aborted"}

    def record_executed(self, commitment_id: str, detail: str) -> dict:
        """Compatibility wrapper: confirm a successful action."""
        return self.record_action_result(commitment_id, True, detail)

    def supersede(self, commitment_id: str, detail: str) -> None:
        """Never mutates the commitment; appends a superseding event."""
        self.store.record_event(commitment_id, "superseded", detail)

    # -- protective exits --------------------------------------------------#
    def authorize_protective_exit(self) -> dict:
        """Never blocks. Seal debt, stale certificates and even a corrupt
        store must not prevent exposure-reducing actions."""
        self._bump("protective_exits_allowed")
        return {"allowed": True, "reason": "protective_exit_always_allowed"}

    # -- observability -------------------------------------------------------#
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
                "(certificate age limit and ring lag are policy-fixed)")
        if not debt_ok:
            explanation.append(f"seal debt: {debt_reason}")
        samples = self.store.seal_latency_samples()
        metrics = self.metrics()
        metrics.setdefault("commitments_created", 0)
        return {
            "state": overall_gate_state(integrity_ok, debt_ok, explanation),
            "explanation": explanation,
            "integrity": {
                "ok": integrity_ok, "reason": integrity_reason,
                "head_index": certificate.get("head_index"),
                "head_hash": certificate.get("head_hash"),
                "ring_count": certificate.get("ring_count"),
                "published_at": certificate.get("published_at"),
                "age_seconds": round(
                    time.time()
                    - safe_float(certificate.get("published_epoch"),
                                 time.time()), 1),
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
            "metrics": metrics,
        }


def build_commitment_spec(
    *, run_id: str, network: str, token_address: str, evidence: dict,
    evidence_block_pin: int, quote: dict, quote_block: int, decision: str,
    hard_stops: list, policy_version: str, faculty_registry_epoch: str,
    verified_head: dict | None, simulation_ok: bool, risk_score=None,
    ttl_seconds: float | None = None, idempotency_key: str,
) -> dict:
    """Canonical commitment payload. Hashes are deterministic: identical
    evidence always produces the identical fingerprint and key."""
    from chainseer_robinhood_commitments import \
        canonical_hard_stop_digest
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
        # Canonical serialization, NOT str(dict): semantically identical
        # stop sets must produce identical digests regardless of ordering
        # or formatting.
        "hard_stop_digest": canonical_hard_stop_digest(hard_stops),
        "policy_version": policy_version,
        "faculty_registry_epoch": faculty_registry_epoch,
        "previous_verified_head_index": safe_int(
            (verified_head or {}).get("head_index"), None),
        "previous_verified_head_hash": (verified_head or {}).get("head_hash"),
        "simulation_ok": bool(simulation_ok),
        "ttl_seconds": ttl_seconds,
        "idempotency_key": idempotency_key,
    }
