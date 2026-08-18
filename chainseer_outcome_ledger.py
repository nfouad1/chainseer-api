"""Canonical, provenance-bound outcome records for Chainseer.

The Timechain remains the canonical append-only ledger.  This module defines
the schema carried by outcome rings and verifies that an outcome points to the
exact analysis ring, the evidence manifest sealed by that analysis, and its
block/slot pin.  Query stores and dashboards may project these records, but
they must always be rebuildable from verified Timechain rings.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime
from typing import Any, Iterable


OUTCOME_LEDGER_SCHEMA_VERSION = "1.0"
# An outcome is only meaningful as evidence for the horizon it CLAIMS to
# measure. A checkpoint labelled "1h" but observed twelve days late records
# twelve days of movement under a one-hour label, and nothing downstream can
# tell it apart from an on-time record. Measured on the live Base ledger:
# 264 of 785 completed checkpoints (34%) were observed more than a day late
# after a stalled learner was restarted and its backlog drained.
#
# Tolerance is proportional with an absolute floor, so a short horizon is
# judged strictly while a long one is not failed for ordinary scheduling
# jitter: 1h -> 15m, 6h -> 90m, 24h -> 6h, 7d -> 42h.
OUTCOME_LATENESS_FLOOR_SECONDS = 15 * 60
OUTCOME_LATENESS_FRACTION = 0.25
EVIDENCE_MANIFEST_SCHEMA_VERSION = "1.0"
HASH_RE = re.compile(r"^[a-f0-9]{64}$")

#: Every ring type that carries an outcome_record. Producers each named their
#: own -- base_learning_outcome, pons_security_outcome,
#: robinhood_learning_outcome -- while consumers were written against the
#: generic "analysis_outcome" that nothing emits. CalibrationEngine therefore
#: reported sample_size 0 on all three chains while 2,450 verified records sat
#: in them. Shared here so a new producer joins by adding one name in one
#: place rather than by silently going unread.
OUTCOME_RING_TYPES = {
    "analysis_outcome",
    "base_learning_outcome",
    "pons_security_outcome",
    "robinhood_learning_outcome",
    "robinhood_learning_outcome_correction",
    "solana_learning_outcome",
}

SUPPORTED_ANALYSIS_RING_TYPES = {
    "token_analysis",
    "solana_token_analysis",
    "base_launch_analysis",
    "pons_launch_analysis",
    "solana_launch_analysis",
}

# Analyses whose provenance hangs off the decision rather than the payload
# root. These adapters seal the candidate and the decision side by side, so the
# provenance travels with the decision that used it.
_DECISION_PROVENANCE_RING_TYPES = frozenset(
    {"base_launch_analysis", "pons_launch_analysis"}
)

SECURITY_OUTCOME_KEYS = {
    "rug_pull",
    "liquidity_removed_pct",
    "honeypot_observed",
    "exploit",
    "owner_privilege_used",
    "tax_changed",
    "contract_upgraded",
}
MARKET_OUTCOME_KEYS = {
    "price_return_pct",
    "max_drawdown_pct",
    "volatility_pct",
}
INFRASTRUCTURE_OUTCOME_KEYS = {
    "infrastructure_indeterminate",
    "rpc_unavailable",
    "indexer_unavailable",
    "quote_unavailable",
    "data_stale",
}

# These exclusions are deliberately conservative: they can only remove a
# record from training, never promote one. Keeping the vocabulary bounded
# prevents an arbitrary producer-supplied string from silently becoming a new
# policy rule in the canonical ledger.
LEARNING_EXCLUSION_REASONS = frozenset({
    "market_outcome_not_observed",
    "checkpoint_outside_learner_tolerance",
})


class OutcomeLedgerError(ValueError):
    """An analysis/outcome provenance invariant was violated."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
        default=str,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _parse_iso(value: Any, field: str) -> datetime:
    text = str(value or "").strip()
    if not text:
        raise OutcomeLedgerError(f"{field} is required")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OutcomeLedgerError(f"{field} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise OutcomeLedgerError(f"{field} must include a timezone")
    return parsed


def _pin(anchor_type: Any, anchor_value: Any) -> dict[str, Any]:
    source_type = str(anchor_type or "block_pin").strip().lower()
    pin_type = "slot_pin" if "slot" in source_type else "block_pin"
    if isinstance(anchor_value, bool):
        raise OutcomeLedgerError("analysis pin must be an integer")
    try:
        value = int(anchor_value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise OutcomeLedgerError("analysis pin must be an integer") from exc
    if value < 0:
        raise OutcomeLedgerError("analysis pin cannot be negative")
    return {
        "type": pin_type,
        "value": value,
        "source_type": source_type,
    }


def build_evidence_manifest(
    provenance: dict[str, Any] | None,
    *,
    anchor_type: str | None = None,
    anchor_value: int | None = None,
) -> dict[str, Any]:
    """Build the stable evidence identity sealed by an analysis.

    Raw responses stay in their content-addressed evidence files.  The
    manifest binds their ordered query/response hashes, fact identifiers, and
    original pin without copying secrets or endpoint parameters into a second
    store.
    """

    provenance = provenance if isinstance(provenance, dict) else {}
    resolved_anchor_type = anchor_type or provenance.get("anchor_type")
    resolved_anchor_value = (
        anchor_value
        if anchor_value is not None
        else provenance.get("block_pin", provenance.get("slot_pin"))
    )
    pin = _pin(resolved_anchor_type, resolved_anchor_value)
    facts: list[dict[str, Any]] = []
    for position, fact in enumerate(provenance.get("facts") or []):
        if not isinstance(fact, dict):
            raise OutcomeLedgerError("provenance facts must be mappings")
        fact_pin = fact.get("block", fact.get("slot"))
        facts.append(
            {
                "position": position,
                "fact_id": fact.get("fact_id", fact.get("id")),
                "source": fact.get("source"),
                "query_hash": fact.get("query_hash"),
                "response_hash": fact.get("response_hash"),
                "pin": fact_pin,
                "observed_at": fact.get("fetched_at", fact.get("timestamp")),
                "cache_hit": bool(fact.get("cache_hit")),
            }
        )
    complete_hashes = bool(facts) and all(
        HASH_RE.fullmatch(str(fact.get(key) or "").lower())
        for fact in facts
        for key in ("query_hash", "response_hash")
    )
    return {
        "schema_version": EVIDENCE_MANIFEST_SCHEMA_VERSION,
        "pin": pin,
        "declared_fact_count": int(provenance.get("fact_count") or len(facts)),
        "fact_count": len(facts),
        "complete_fact_hashes": complete_hashes,
        "facts": facts,
    }


def analysis_evidence_binding(
    provenance: dict[str, Any] | None,
    *,
    anchor_type: str | None = None,
    anchor_value: int | None = None,
) -> dict[str, Any]:
    manifest = build_evidence_manifest(
        provenance,
        anchor_type=anchor_type,
        anchor_value=anchor_value,
    )
    return {
        "evidence_manifest": manifest,
        "evidence_hash": canonical_hash(manifest),
        "evidence_hash_algorithm": "sha256-canonical-json",
        "anchor_type": manifest["pin"]["type"],
        "anchor_value": manifest["pin"]["value"],
    }


def _ring_payload_provenance(ring_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    if ring_type in _DECISION_PROVENANCE_RING_TYPES:
        return ((payload.get("decision") or {}).get("provenance") or {})
    return payload.get("provenance") or {}


def _first_address(*values: Any) -> str | None:
    """First value that is actually an address string.

    Several adapters nest a metadata object under the same key that elsewhere
    holds the address (Solana's decision["mint"] is the mint ACCOUNT). A bare
    `a or b` chain accepts that dict because it is truthy, so the subject
    becomes an unusable blob rather than an address.
    """
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _ring_network_subject(
    ring_type: str, payload: dict[str, Any]
) -> tuple[str, str | None]:
    if ring_type == "solana_token_analysis":
        return "solana", payload.get("mint")
    if ring_type == "base_launch_analysis":
        candidate = payload.get("candidate") or {}
        return "base", candidate.get("token_address")
    if ring_type == "solana_launch_analysis":
        # Same "solana" namespace as solana_token_analysis on purpose: a mint
        # analysed by both the public product and the autotrader is ONE
        # subject, and splitting it would hide exactly the cross-system
        # agreement the Memory Core exists to surface.
        #
        # decision["mint"] is the mint ACCOUNT (authorities, decimals, supply),
        # not the address. It is a dict, so a plain `or` chain would take it as
        # the subject and key every ring by a metadata blob instead of a mint.
        decision = payload.get("decision") or {}
        candidate = payload.get("candidate") or {}
        return "solana", _first_address(
            decision.get("mint_address"), candidate.get("mint"), decision.get("mint")
        )
    if ring_type == "pons_launch_analysis":
        # Pons runs ON Robinhood Chain (chain_id 4663), so its subjects share
        # the "robinhood" namespace with general token_analysis rings rather
        # than forming a separate one. The token is the same token.
        decision = payload.get("decision") or {}
        candidate = payload.get("candidate") or {}
        return "robinhood", (
            decision.get("token_address") or candidate.get("token_address")
        )
    chain_id = payload.get("chain_id")
    network = str(payload.get("network") or "").strip().lower()
    if not network:
        network = "base" if chain_id == 8453 else "robinhood"
    return network, payload.get("token_address")


def analysis_reference_from_ring(ring: dict[str, Any]) -> dict[str, Any]:
    """Create and verify the immutable provenance pointer for one analysis."""

    if not isinstance(ring, dict):
        raise OutcomeLedgerError("analysis ring must be a mapping")
    ring_type = str(ring.get("ring_type") or "")
    if ring_type not in SUPPORTED_ANALYSIS_RING_TYPES:
        raise OutcomeLedgerError(
            "analysis ring type must be one of "
            + ", ".join(sorted(SUPPORTED_ANALYSIS_RING_TYPES))
        )
    ring_hash = str(ring.get("ring_hash") or "").lower()
    if not HASH_RE.fullmatch(ring_hash):
        raise OutcomeLedgerError("analysis ring hash must be a SHA-256 digest")
    index = ring.get("index")
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise OutcomeLedgerError("analysis ring index must be a non-negative integer")
    payload = ring.get("payload") or {}
    if not isinstance(payload, dict):
        raise OutcomeLedgerError("analysis ring payload must be a mapping")

    sealed_manifest = payload.get("evidence_manifest")
    sealed_hash = str(payload.get("evidence_hash") or "").lower()
    if sealed_manifest is not None or sealed_hash:
        if not isinstance(sealed_manifest, dict):
            raise OutcomeLedgerError("sealed evidence manifest must be a mapping")
        expected_hash = canonical_hash(sealed_manifest)
        if not HASH_RE.fullmatch(sealed_hash) or sealed_hash != expected_hash:
            raise OutcomeLedgerError("analysis evidence hash does not match its manifest")
        manifest = sealed_manifest
        binding_state = "sealed_at_analysis"
    else:
        provenance = _ring_payload_provenance(ring_type, payload)
        anchor_type = payload.get("anchor_type")
        anchor_value = payload.get("anchor_value")
        if ring_type == "solana_token_analysis":
            anchor_type = anchor_type or "slot_pin"
            anchor_value = (
                anchor_value
                if anchor_value is not None
                else payload.get("slot_anchor")
            )
        elif ring_type == "solana_launch_analysis":
            # Solana has no block numbers; the slot the candidate was observed
            # at is the only honest pin for these legacy rings.
            anchor_type = anchor_type or "slot_pin"
            if anchor_value is None:
                anchor_value = (payload.get("candidate") or {}).get("slot")
        else:
            if anchor_value is None:
                anchor_value = payload.get("block_pin")
            if anchor_value is None:
                # Pons seals its pin inside the decision, not at the root.
                anchor_value = (payload.get("decision") or {}).get("block_pin")
        binding = analysis_evidence_binding(
            provenance,
            anchor_type=anchor_type,
            anchor_value=anchor_value,
        )
        manifest = binding["evidence_manifest"]
        sealed_hash = binding["evidence_hash"]
        binding_state = "derived_from_legacy_analysis_ring"

    pin = manifest.get("pin") or {}
    normalized_pin = _pin(pin.get("type"), pin.get("value"))
    network, subject = _ring_network_subject(ring_type, payload)
    reference = {
        "ring": index,
        "ring_hash": ring_hash,
        "ring_type": ring_type,
        "ring_timestamp": ring.get("timestamp"),
        "network": network,
        "subject": subject,
        "analysis_version": payload.get("analysis_version")
        or ((payload.get("decision") or {}).get("analysis_version")),
        "original_evidence_hash": sealed_hash,
        "evidence_hash_algorithm": "sha256-canonical-json",
        "evidence_manifest_schema_version": manifest.get("schema_version"),
        "evidence_fact_count": manifest.get("fact_count", 0),
        "evidence_complete": bool(manifest.get("complete_fact_hashes")),
        "anchor_type": normalized_pin["type"],
        "anchor_value": normalized_pin["value"],
        "binding_state": binding_state,
    }
    reference["reference_hash"] = canonical_hash(reference)
    return reference


def partition_outcomes(
    outcomes: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    if not isinstance(outcomes, dict) or not outcomes:
        raise OutcomeLedgerError("outcomes must be a non-empty mapping")
    security = {key: outcomes[key] for key in SECURITY_OUTCOME_KEYS if key in outcomes}
    market = {key: outcomes[key] for key in MARKET_OUTCOME_KEYS if key in outcomes}
    infrastructure = {
        key: outcomes[key]
        for key in INFRASTRUCTURE_OUTCOME_KEYS
        if key in outcomes
    }
    classified = SECURITY_OUTCOME_KEYS | MARKET_OUTCOME_KEYS | INFRASTRUCTURE_OUTCOME_KEYS
    other = {key: value for key, value in outcomes.items() if key not in classified}
    return security, market, infrastructure, other


def _outcome_timing(
    outcomes: dict[str, Any],
    analysis_timestamp: Any,
    observed: datetime,
) -> dict[str, Any]:
    """Describe how faithfully an outcome measures the horizon it claims.

    ``within_tolerance`` is None when the outcome declares no nominal
    horizon -- lateness is then unknowable rather than acceptable, so it is
    reported honestly instead of silently passing as on time.
    """
    horizon = outcomes.get("horizon_seconds") if isinstance(outcomes, dict) else None
    result: dict[str, Any] = {
        "nominal_horizon_seconds": None,
        "elapsed_seconds": None,
        "lateness_seconds": None,
        "tolerance_seconds": None,
        "within_tolerance": None,
    }
    if analysis_timestamp:
        try:
            elapsed = (
                observed - _parse_iso(analysis_timestamp, "analysis ring timestamp")
            ).total_seconds()
            result["elapsed_seconds"] = round(elapsed, 3)
        except OutcomeLedgerError:
            return result
    if isinstance(horizon, bool) or not isinstance(horizon, (int, float)):
        return result
    if horizon <= 0 or result["elapsed_seconds"] is None:
        return result
    tolerance = max(
        OUTCOME_LATENESS_FLOOR_SECONDS, OUTCOME_LATENESS_FRACTION * float(horizon)
    )
    lateness = result["elapsed_seconds"] - float(horizon)
    result["nominal_horizon_seconds"] = float(horizon)
    result["lateness_seconds"] = round(lateness, 3)
    result["tolerance_seconds"] = round(tolerance, 3)
    result["within_tolerance"] = lateness <= tolerance
    return result


def build_outcome_record(
    analysis_ring: dict[str, Any],
    outcomes: dict[str, Any],
    *,
    observed_at: str,
    outcome_provenance: dict[str, Any] | None = None,
    evidence_fact_ids: Iterable[str] = (),
    calibration: dict[str, Any] | None = None,
    learning_exclusion_reason: str | None = None,
) -> dict[str, Any]:
    analysis_ref = analysis_reference_from_ring(analysis_ring)
    observed = _parse_iso(observed_at, "observed_at")
    analysis_timestamp = analysis_ref.get("ring_timestamp")
    if analysis_timestamp and observed < _parse_iso(
        analysis_timestamp, "analysis ring timestamp"
    ):
        raise OutcomeLedgerError("outcome cannot predate its analysis ring")
    security, market, infrastructure, other = partition_outcomes(outcomes)

    outcome_manifest = None
    outcome_evidence_hash = None
    if isinstance(outcome_provenance, dict) and (
        outcome_provenance.get("block_pin") is not None
        or outcome_provenance.get("slot_pin") is not None
    ):
        outcome_manifest = build_evidence_manifest(
            outcome_provenance,
            anchor_type=(
                outcome_provenance.get("anchor_type")
                or analysis_ref["anchor_type"]
            ),
        )
        outcome_evidence_hash = canonical_hash(outcome_manifest)
    fact_ids = sorted({str(value) for value in evidence_fact_ids if str(value)})
    outcome_evidence_complete = bool(
        outcome_manifest and outcome_manifest.get("complete_fact_hashes")
    )
    analysis_evidence_complete = bool(analysis_ref.get("evidence_complete"))
    timing = _outcome_timing(outcomes, analysis_timestamp, observed)
    # An outcome observed far past the horizon it claims to measure is not
    # evidence about that horizon. Excluding it from learning keeps it in the
    # ledger -- still sealed, still auditable -- without letting it train
    # anything under a label it does not honour.
    timely = timing["within_tolerance"] is not False
    learning_eligible = (
        analysis_evidence_complete and outcome_evidence_complete and timely
    )
    if not analysis_evidence_complete:
        learning_reason = "analysis_evidence_incomplete"
    elif not outcome_evidence_complete:
        learning_reason = "outcome_evidence_incomplete"
    elif not timely:
        learning_reason = "outcome_observed_too_late_for_its_horizon"
    else:
        learning_reason = "analysis_and_outcome_evidence_hashes_complete"
    if learning_exclusion_reason is not None:
        if learning_exclusion_reason not in LEARNING_EXCLUSION_REASONS:
            raise OutcomeLedgerError("unsupported learning exclusion reason")
        baseline_eligible = learning_eligible
        learning_eligible = False
        learning_reason = learning_exclusion_reason
    else:
        baseline_eligible = learning_eligible
    learning = {
        "eligible": learning_eligible,
        "reason": learning_reason,
    }
    if learning_exclusion_reason is not None:
        learning.update({
            "baseline_eligible": baseline_eligible,
            "exclusion_reason": learning_exclusion_reason,
        })
    record = {
        "record_type": "chainseer_analysis_outcome",
        "schema_version": OUTCOME_LEDGER_SCHEMA_VERSION,
        "analysis_reference": analysis_ref,
        "observed_at": observed_at,
        "security_outcomes": security,
        "market_outcomes": market,
        "infrastructure_outcomes": infrastructure,
        "other_outcomes": other,
        "outcome_evidence": {
            "fact_ids": fact_ids,
            "manifest": outcome_manifest,
            "evidence_hash": outcome_evidence_hash,
            "evidence_hash_algorithm": (
                "sha256-canonical-json" if outcome_evidence_hash else None
            ),
            "complete": outcome_evidence_complete,
        },
        "learning": learning,
        "timing": timing,
        "calibration": dict(calibration or {}),
    }
    identity = {
        "analysis_ring_hash": analysis_ref["ring_hash"],
        "observed_at": observed_at,
        "outcome_hash": canonical_hash(
            {
                "security": security,
                "market": market,
                "infrastructure": infrastructure,
                "other": other,
            }
        ),
    }
    record["outcome_id"] = f"outcome-{canonical_hash(identity)[:24]}"
    record["record_hash"] = canonical_hash(record)
    return record


def verify_outcome_record(
    record: dict[str, Any],
    analysis_ring: dict[str, Any] | None = None,
) -> tuple[bool, str]:
    try:
        if record.get("record_type") != "chainseer_analysis_outcome":
            raise OutcomeLedgerError("unsupported outcome record type")
        if record.get("schema_version") != OUTCOME_LEDGER_SCHEMA_VERSION:
            raise OutcomeLedgerError("unsupported outcome schema version")
        supplied_hash = str(record.get("record_hash") or "").lower()
        if not HASH_RE.fullmatch(supplied_hash):
            raise OutcomeLedgerError("outcome record hash must be a SHA-256 digest")
        unhashed = {key: value for key, value in record.items() if key != "record_hash"}
        if canonical_hash(unhashed) != supplied_hash:
            raise OutcomeLedgerError("outcome record hash mismatch")
        reference = record.get("analysis_reference") or {}
        reference_hash = str(reference.get("reference_hash") or "").lower()
        bare_reference = {
            key: value for key, value in reference.items() if key != "reference_hash"
        }
        if not HASH_RE.fullmatch(reference_hash) or canonical_hash(bare_reference) != reference_hash:
            raise OutcomeLedgerError("analysis reference hash mismatch")
        if not HASH_RE.fullmatch(str(reference.get("ring_hash") or "").lower()):
            raise OutcomeLedgerError("analysis reference ring hash is invalid")
        if not HASH_RE.fullmatch(
            str(reference.get("original_evidence_hash") or "").lower()
        ):
            raise OutcomeLedgerError("original evidence hash is invalid")
        _pin(reference.get("anchor_type"), reference.get("anchor_value"))
        _parse_iso(record.get("observed_at"), "observed_at")
        outcome_evidence = record.get("outcome_evidence") or {}
        outcome_manifest = outcome_evidence.get("manifest")
        outcome_hash = outcome_evidence.get("evidence_hash")
        if outcome_manifest is not None:
            expected_outcome_hash = canonical_hash(outcome_manifest)
            if (
                not HASH_RE.fullmatch(str(outcome_hash or "").lower())
                or str(outcome_hash).lower() != expected_outcome_hash
            ):
                raise OutcomeLedgerError("outcome evidence hash mismatch")
        elif outcome_hash is not None:
            raise OutcomeLedgerError(
                "outcome evidence hash cannot exist without its manifest"
            )
        # Timeliness is part of eligibility, so the verifier must apply the
        # same rule the builder did -- otherwise a correctly-excluded late
        # outcome would be rejected here as "inconsistent". A record with no
        # timing block predates the gate and is judged on evidence alone.
        timing = record.get("timing")
        timely = True
        if isinstance(timing, dict):
            timely = timing.get("within_tolerance") is not False
        baseline_eligibility = (
            bool(reference.get("evidence_complete"))
            and bool(outcome_manifest and outcome_manifest.get("complete_fact_hashes"))
            and timely
        )
        learning = record.get("learning") or {}
        exclusion_reason = learning.get("exclusion_reason")
        expected_eligibility = baseline_eligibility
        if exclusion_reason is not None:
            if exclusion_reason not in LEARNING_EXCLUSION_REASONS:
                raise OutcomeLedgerError("unsupported learning exclusion reason")
            if learning.get("reason") != exclusion_reason:
                raise OutcomeLedgerError("learning exclusion reason is inconsistent")
            if bool(learning.get("baseline_eligible")) != baseline_eligibility:
                raise OutcomeLedgerError("baseline learning eligibility is inconsistent")
            expected_eligibility = False
        if bool(learning.get("eligible")) != expected_eligibility:
            raise OutcomeLedgerError("outcome learning eligibility is inconsistent")
        if analysis_ring is not None:
            expected = analysis_reference_from_ring(analysis_ring)
            if canonical_hash(reference) != canonical_hash(expected):
                raise OutcomeLedgerError(
                    "outcome does not match the referenced analysis ring"
                )
        return True, "verified"
    except (OutcomeLedgerError, TypeError, ValueError) as exc:
        return False, str(exc)


def build_outcome_correction(
    original_outcome_ring: dict[str, Any],
    analysis_ring: dict[str, Any],
    *,
    learning_exclusion_reason: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build an append-only correction for the historical post-hash bug.

    Only the learning block and its dependent record hash may change. The
    correction metadata binds both versions and the immutable source ring, so
    a verifier can select the corrected canonical view without erasing the bad
    historical record.
    """
    if learning_exclusion_reason not in LEARNING_EXCLUSION_REASONS:
        raise OutcomeLedgerError("unsupported learning exclusion reason")
    payload = original_outcome_ring.get("payload") or {}
    original = payload.get("outcome_record")
    if not isinstance(original, dict):
        raise OutcomeLedgerError("superseded ring has no outcome record")
    original_ok, original_reason = verify_outcome_record(original, analysis_ring)
    if original_ok or original_reason != "outcome record hash mismatch":
        raise OutcomeLedgerError(
            "only outcome record hash mismatches can be corrected"
        )
    corrected = deepcopy(original)
    reference = corrected.get("analysis_reference") or {}
    expected_reference = analysis_reference_from_ring(analysis_ring)
    if canonical_hash(reference) != canonical_hash(expected_reference):
        raise OutcomeLedgerError("correction analysis reference mismatch")
    outcome_evidence = corrected.get("outcome_evidence") or {}
    manifest = outcome_evidence.get("manifest")
    timely = True
    timing = corrected.get("timing")
    if isinstance(timing, dict):
        timely = timing.get("within_tolerance") is not False
    baseline_eligible = (
        bool(reference.get("evidence_complete"))
        and bool(manifest and manifest.get("complete_fact_hashes"))
        and timely
    )
    corrected["learning"] = {
        "eligible": False,
        "reason": learning_exclusion_reason,
        "baseline_eligible": baseline_eligible,
        "exclusion_reason": learning_exclusion_reason,
    }
    corrected.pop("record_hash", None)
    corrected["record_hash"] = canonical_hash(corrected)
    corrected_ok, corrected_reason = verify_outcome_record(corrected, analysis_ring)
    if not corrected_ok:
        raise OutcomeLedgerError(
            f"corrected outcome record is invalid: {corrected_reason}"
        )
    correction = {
        "schema_version": "1.0",
        "correction_type": "outcome_learning_integrity_supersession",
        "supersedes_ring": original_outcome_ring.get("index"),
        "supersedes_ring_hash": original_outcome_ring.get("ring_hash"),
        "original_record_hash": original.get("record_hash"),
        "corrected_record_hash": corrected["record_hash"],
        "learning_exclusion_reason": learning_exclusion_reason,
    }
    correction["correction_hash"] = canonical_hash(correction)
    return corrected, correction


def _valid_outcome_corrections(
    ring_list: list[dict[str, Any]],
) -> tuple[dict[int, dict[str, Any]], list[dict[str, Any]]]:
    """Return valid one-to-one supersessions and correction errors."""
    by_index = {ring.get("index"): ring for ring in ring_list}
    candidates: dict[int, list[dict[str, Any]]] = {}
    errors: list[dict[str, Any]] = []
    for ring in ring_list:
        payload = ring.get("payload") or {}
        correction = payload.get("outcome_correction")
        if not isinstance(correction, dict):
            continue
        target_index = correction.get("supersedes_ring")
        candidates.setdefault(target_index, []).append(ring)
    valid: dict[int, dict[str, Any]] = {}
    for target_index, correction_rings in candidates.items():
        if len(correction_rings) != 1:
            for ring in correction_rings:
                errors.append({
                    "ring": ring.get("index"),
                    "reason": "multiple outcome corrections target one ring",
                })
            continue
        ring = correction_rings[0]
        payload = ring.get("payload") or {}
        correction = payload.get("outcome_correction") or {}
        try:
            if correction.get("schema_version") != "1.0":
                raise OutcomeLedgerError("unsupported outcome correction schema")
            if correction.get("correction_type") != (
                "outcome_learning_integrity_supersession"
            ):
                raise OutcomeLedgerError("unsupported outcome correction type")
            target = by_index.get(target_index)
            if target is None:
                raise OutcomeLedgerError("superseded outcome ring is missing")
            if (target.get("payload") or {}).get("outcome_correction") is not None:
                raise OutcomeLedgerError("correction chains are not supported")
            if target.get("ring_hash") != correction.get("supersedes_ring_hash"):
                raise OutcomeLedgerError("superseded outcome ring hash mismatch")
            bare_correction = {
                key: value for key, value in correction.items()
                if key != "correction_hash"
            }
            if canonical_hash(bare_correction) != correction.get("correction_hash"):
                raise OutcomeLedgerError("outcome correction hash mismatch")
            original = (target.get("payload") or {}).get("outcome_record")
            corrected = payload.get("outcome_record")
            if not isinstance(original, dict) or not isinstance(corrected, dict):
                raise OutcomeLedgerError("outcome correction record is missing")
            if original.get("record_hash") != correction.get("original_record_hash"):
                raise OutcomeLedgerError("original outcome record hash mismatch")
            if corrected.get("record_hash") != correction.get("corrected_record_hash"):
                raise OutcomeLedgerError("corrected outcome record hash mismatch")
            if (corrected.get("learning") or {}).get("exclusion_reason") != (
                correction.get("learning_exclusion_reason")
            ):
                raise OutcomeLedgerError("correction learning exclusion mismatch")
            # The repair is deliberately narrow. Replacing these two dependent
            # fields in the original must yield the exact corrected record.
            expected = deepcopy(original)
            expected["learning"] = deepcopy(corrected.get("learning"))
            expected["record_hash"] = corrected.get("record_hash")
            if canonical_hash(expected) != canonical_hash(corrected):
                raise OutcomeLedgerError(
                    "outcome correction changed fields outside learning integrity"
                )
            reference = corrected.get("analysis_reference") or {}
            analysis = by_index.get(reference.get("ring"))
            ok, reason = verify_outcome_record(corrected, analysis)
            if not ok:
                raise OutcomeLedgerError(f"corrected outcome is invalid: {reason}")
            original_ok, original_reason = verify_outcome_record(original, analysis)
            if original_ok or original_reason != "outcome record hash mismatch":
                raise OutcomeLedgerError("superseded outcome is not the known hash defect")
            valid[int(target_index)] = ring
        except (OutcomeLedgerError, TypeError, ValueError) as exc:
            errors.append({"ring": ring.get("index"), "reason": str(exc)})
    return valid, errors


def canonical_outcome_rings(
    rings: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Project immutable rings into the valid canonical supersession view."""
    ring_list = list(rings)
    valid, _errors = _valid_outcome_corrections(ring_list)
    correction_indices = {
        ring.get("index") for ring in ring_list
        if isinstance((ring.get("payload") or {}).get("outcome_correction"), dict)
    }
    valid_correction_indices = {
        ring.get("index") for ring in valid.values()
    }
    return [
        ring for ring in ring_list
        if ring.get("index") not in valid
        and (
            ring.get("index") not in correction_indices
            or ring.get("index") in valid_correction_indices
        )
    ]


def _record_is_timely(record: dict[str, Any]) -> bool:
    """Was this outcome observed close enough to the horizon it claims?

    Uses the record's stored timing block when present; otherwise derives it
    from fields every record already carries (analysis ring timestamp,
    observed_at, and the declared horizon), so records sealed before the
    gate existed are still judged honestly rather than counted as clean.
    """
    timing = record.get("timing")
    if isinstance(timing, dict) and timing.get("within_tolerance") is not None:
        return bool(timing["within_tolerance"])
    reference = record.get("analysis_reference") or {}
    horizon = (record.get("other_outcomes") or {}).get("horizon_seconds")
    derived = _outcome_timing(
        {"horizon_seconds": horizon},
        reference.get("ring_timestamp"),
        _parse_iso(record.get("observed_at"), "observed_at"),
    )
    return derived["within_tolerance"] is not False


def verify_outcome_rings(
    rings: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    ring_list = list(rings)
    valid_corrections, correction_errors = _valid_outcome_corrections(ring_list)
    correction_indices = {
        ring.get("index") for ring in ring_list
        if isinstance((ring.get("payload") or {}).get("outcome_correction"), dict)
    }
    valid_correction_indices = {
        ring.get("index") for ring in valid_corrections.values()
    }
    canonical_rings = [
        ring for ring in ring_list
        if ring.get("index") not in valid_corrections
        and (
            ring.get("index") not in correction_indices
            or ring.get("index") in valid_correction_indices
        )
    ]
    by_index = {ring.get("index"): ring for ring in ring_list}
    checked = 0
    eligible = 0
    legacy_unbound = 0
    stale_horizon = 0
    # Newest outcome ring of ANY kind -- canonical or legacy. A producer that
    # has stopped emitting outcomes is otherwise indistinguishable from one
    # that is simply quiet, which is how a 12-day learning stall once went
    # unnoticed while every integrity check kept reporting healthy.
    latest_outcome_at: str | None = None
    latest_outcome_ring: int | None = None
    errors: list[dict[str, Any]] = list(correction_errors)
    for ring in canonical_rings:
        payload = ring.get("payload") or {}
        record = payload.get("outcome_record")
        is_outcome_ring = ring.get("ring_type") in OUTCOME_RING_TYPES
        if is_outcome_ring or record is not None:
            timestamp = ring.get("timestamp")
            if timestamp:
                latest_outcome_at = str(timestamp)
                latest_outcome_ring = ring.get("index")
        if record is None:
            if is_outcome_ring:
                legacy_unbound += 1
            continue
        checked += 1
        reference = record.get("analysis_reference") or {}
        analysis = by_index.get(reference.get("ring"))
        ok, reason = verify_outcome_record(record, analysis)
        if not ok:
            errors.append({"ring": ring.get("index"), "reason": reason})
        elif (record.get("learning") or {}).get("eligible"):
            # Records sealed before the timeliness gate carry no timing
            # block, but their lateness is still derivable from data they
            # already hold. Recompute it here so the corpus-quality figure
            # is honest, WITHOUT failing verification on them -- they were
            # built correctly under the rule of their day, and their rings
            # are immutable.
            if _record_is_timely(record):
                eligible += 1
            else:
                stale_horizon += 1
    return {
        "ok": not errors,
        "schema_version": OUTCOME_LEDGER_SCHEMA_VERSION,
        "checked": checked,
        "learning_eligible": eligible,
        "legacy_unbound": legacy_unbound,
        "superseded_records": len(valid_corrections),
        "stale_horizon_excluded": stale_horizon,
        "latest_outcome_at": latest_outcome_at,
        "latest_outcome_ring": latest_outcome_ring,
        "errors": errors,
    }
