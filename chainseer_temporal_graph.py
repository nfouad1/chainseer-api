"""Timechain-derived temporal entity and risk projection for Chainseer.

The Timechain is the source of truth.  This module turns verified analysis
rings into a fast longitudinal read model without making the projection
authoritative.  Deleting the JSON projection and rebuilding it from the rings
must always reproduce the same hash.

Relationship absence is deliberately conservative: a previously observed
relationship becomes ``disappeared`` only when the current graph explicitly
declares complete coverage for that relationship type.  Otherwise it becomes
``unconfirmed`` and the last positive observation remains available.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any, Iterable

from chainseer_entity_graph import verify_entity_graph


TEMPORAL_GRAPH_SCHEMA_VERSION = "1.0"
TEMPORAL_GRAPH_FILENAME = "temporal_entity_graph-v1.json"
ANALYSIS_RING_TYPES = {
    "token_analysis",
    "solana_token_analysis",
    "base_launch_analysis",
}
HASH_LENGTH = 64


class TemporalGraphError(ValueError):
    """A temporal projection or analysis-ring invariant was violated."""


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


def _hash_ok(value: Any) -> bool:
    text = str(value or "").lower()
    return len(text) == HASH_LENGTH and all(ch in "0123456789abcdef" for ch in text)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _normalize_subject(network: str, subject: Any) -> str:
    value = str(subject or "").strip()
    if not value:
        raise TemporalGraphError("analysis subject is required")
    return value if network == "solana" else value.lower()


def _subject_key(network: str, subject: str) -> str:
    return f"{network}:{subject}"


def _analysis_fields(ring: dict[str, Any]) -> dict[str, Any] | None:
    ring_type = str(ring.get("ring_type") or "")
    if ring_type not in ANALYSIS_RING_TYPES:
        return None
    payload = ring.get("payload") or {}
    if ring_type == "solana_token_analysis":
        network = "solana"
        subject = payload.get("mint")
        analysis = payload.get("analysis") or {}
        score = analysis.get("legitimacy_score")
        hard_stops = analysis.get("hard_stop_codes") or []
        component_scores = analysis.get("component_scores") or {}
    elif ring_type == "base_launch_analysis":
        network = "base"
        subject = (payload.get("candidate") or {}).get("token_address")
        analysis = payload.get("decision") or {}
        score = analysis.get("score")
        hard_stops = analysis.get("hard_stops") or []
        component_scores = analysis.get("component_scores") or {}
    else:
        network = str(payload.get("network") or "").strip().lower()
        if not network:
            network = "base" if payload.get("chain_id") == 8453 else "robinhood"
        subject = payload.get("token_address")
        analysis = payload
        score = payload.get("legitimacy_score")
        hard_stops = payload.get("hard_stop_overrides") or []
        component_scores = payload.get("component_scores") or {}

    subject = _normalize_subject(network, subject)
    manifest = payload.get("evidence_manifest") or {}
    pin = manifest.get("pin") or {}
    anchor_type = str(
        pin.get("type") or payload.get("anchor_type") or (
            "slot_pin" if network == "solana" else "block_pin"
        )
    )
    anchor_value = pin.get("value", payload.get("anchor_value"))
    if anchor_value is None:
        anchor_value = payload.get("slot_anchor", payload.get("block_pin"))
    try:
        anchor_value = int(anchor_value) if anchor_value is not None else None
    except (TypeError, ValueError, OverflowError):
        anchor_value = None

    ring_hash = str(ring.get("ring_hash") or "").lower()
    if not _hash_ok(ring_hash):
        raise TemporalGraphError("analysis ring hash is invalid")
    ring_index = ring.get("index")
    if isinstance(ring_index, bool) or not isinstance(ring_index, int):
        raise TemporalGraphError("analysis ring index is invalid")

    evidence_state = str(payload.get("evidence_state") or "").strip()
    if not evidence_state:
        evidence_state = "unknown_legacy"
    evidence_hash = str(payload.get("evidence_hash") or "").lower() or None
    if evidence_hash is not None and not _hash_ok(evidence_hash):
        raise TemporalGraphError("analysis evidence hash is invalid")

    hard_stop_codes: list[str] = []
    for item in hard_stops:
        code = item.get("code") if isinstance(item, dict) else item
        code = str(code or "").strip()
        if code and code not in hard_stop_codes:
            hard_stop_codes.append(code)

    return {
        "network": network,
        "subject": subject,
        "ring": {
            "index": ring_index,
            "hash": ring_hash,
            "type": ring_type,
            "timestamp": str(ring.get("timestamp") or ""),
        },
        "observed_at": str(
            payload.get("timestamp")
            or analysis.get("analyzed_at")
            or ring.get("timestamp")
            or ""
        ),
        "pin": {"type": anchor_type, "value": anchor_value},
        "evidence_hash": evidence_hash,
        "evidence_state": evidence_state,
        "score": _number(score),
        "risk_level": analysis.get("risk_level"),
        "action": analysis.get("action_label"),
        "confidence": analysis.get("confidence_grade", analysis.get("confidence")),
        "component_scores": {
            str(key): numeric
            for key, value in component_scores.items()
            if (numeric := _number(value)) is not None
        },
        "hard_stop_codes": sorted(hard_stop_codes),
        "graph": payload.get("entity_graph_snapshot") or payload.get("entity_graph"),
    }


def _observation_ref(fields: dict[str, Any]) -> dict[str, Any]:
    return {
        "analysis_ring": copy.deepcopy(fields["ring"]),
        "observed_at": fields["observed_at"],
        "pin": copy.deepcopy(fields["pin"]),
        "evidence_hash": fields["evidence_hash"],
    }


def _relationship_key(network: str, edge: dict[str, Any]) -> str:
    identity = [
        network,
        edge.get("relationship"),
        edge.get("source"),
        edge.get("target"),
    ]
    return f"relationship-{canonical_hash(identity)[:24]}"


def _relationship_semantics(edge: dict[str, Any]) -> dict[str, Any]:
    """Fields whose changes mean the relationship itself was re-described.

    Evidence fact identifiers are intentionally excluded because each scan
    creates fresh fact IDs even when the relationship did not change.
    """

    return {
        "evidence_status": edge.get("evidence_status"),
        "confidence": edge.get("confidence"),
        "attributes": edge.get("attributes") or {},
    }


def _blank_projection() -> dict[str, Any]:
    return {
        "schema_version": TEMPORAL_GRAPH_SCHEMA_VERSION,
        "source_chain": {
            "head_index": None,
            "head_hash": None,
            "ring_count": 0,
            "analysis_ring_count": 0,
            "legacy_graph_ring_count": 0,
        },
        "identities": {},
        "entities": {},
        "relationships": {},
        "subjects": {},
        "limitations": [
            "Exact-address equality is the only cross-analysis identity join; no beneficial ownership or heuristic wallet clustering is inferred.",
            "Summary-only historical graph rings cannot reconstruct relationship lifecycles and are labeled legacy_graph_summary_only.",
            "A missing relationship is called disappeared only after complete comparable coverage for its relationship type.",
        ],
    }


def _ensure_subject(
    projection: dict[str, Any], fields: dict[str, Any]
) -> dict[str, Any]:
    key = _subject_key(fields["network"], fields["subject"])
    subject = projection["subjects"].get(key)
    if subject is None:
        ref = _observation_ref(fields)
        subject = {
            "network": fields["network"],
            "subject": fields["subject"],
            "first_observed": ref,
            "last_observed": ref,
            "analysis_count": 0,
            "risk_timeline": [],
            "risk_evolution": {},
            "entity_ids": [],
            "relationship_ids": [],
            "relationship_events": [],
            "latest_graph": None,
            "legacy_graph_observations": 0,
        }
        projection["subjects"][key] = subject
    return subject


def _risk_point(fields: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    point = {
        **_observation_ref(fields),
        "score": fields["score"],
        "risk_level": fields["risk_level"],
        "action": fields["action"],
        "confidence": fields["confidence"],
        "component_scores": fields["component_scores"],
        "hard_stop_codes": fields["hard_stop_codes"],
        "evidence_state": fields["evidence_state"],
        "score_usable": bool(
            fields["score"] is not None
            and fields["evidence_state"] == "token_evidence"
        ),
        "delta_from_previous": None,
        "component_deltas": {},
        "risk_level_changed": False,
        "hard_stops_added": [],
        "hard_stops_removed": [],
    }
    if previous is None:
        return point
    if fields["score"] is not None and previous.get("score") is not None:
        point["delta_from_previous"] = round(
            fields["score"] - float(previous["score"]), 6
        )
    for name in sorted(set(fields["component_scores"]).intersection(
        previous.get("component_scores") or {}
    )):
        point["component_deltas"][name] = round(
            fields["component_scores"][name]
            - float(previous["component_scores"][name]),
            6,
        )
    point["risk_level_changed"] = fields["risk_level"] != previous.get("risk_level")
    old_stops = set(previous.get("hard_stop_codes") or [])
    new_stops = set(fields["hard_stop_codes"])
    point["hard_stops_added"] = sorted(new_stops - old_stops)
    point["hard_stops_removed"] = sorted(old_stops - new_stops)
    return point


def _risk_evolution(timeline: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [item for item in timeline if item.get("score_usable")]
    observed = [item for item in timeline if item.get("score") is not None]
    scores = [float(item["score"]) for item in usable]
    result = {
        "observation_count": len(timeline),
        "scored_observation_count": len(observed),
        "usable_score_count": len(usable),
        "infrastructure_indeterminate_count": sum(
            item.get("evidence_state") == "infrastructure_indeterminate"
            for item in timeline
        ),
        "legacy_evidence_state_count": sum(
            item.get("evidence_state") == "unknown_legacy"
            for item in timeline
        ),
        "first_score": scores[0] if scores else None,
        "current_score": scores[-1] if scores else None,
        "minimum_score": min(scores) if scores else None,
        "maximum_score": max(scores) if scores else None,
        "total_score_delta": round(scores[-1] - scores[0], 6) if len(scores) >= 2 else None,
        "risk_level_change_count": sum(
            bool(item.get("risk_level_changed")) for item in timeline[1:]
        ),
        "hard_stop_addition_count": sum(
            len(item.get("hard_stops_added") or []) for item in timeline[1:]
        ),
        "hard_stop_removal_count": sum(
            len(item.get("hard_stops_removed") or []) for item in timeline[1:]
        ),
        "calibration_scope": "token_evidence_only",
    }
    return result


def _event(
    event_type: str,
    relationship_id: str,
    fields: dict[str, Any],
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "event": event_type,
        "relationship_id": relationship_id,
        **_observation_ref(fields),
        "details": details or {},
    }


def _apply_graph(
    projection: dict[str, Any],
    subject: dict[str, Any],
    fields: dict[str, Any],
) -> None:
    graph = fields.get("graph")
    if not isinstance(graph, dict) or not graph.get("nodes"):
        subject["legacy_graph_observations"] += 1
        projection["source_chain"]["legacy_graph_ring_count"] += 1
        return
    ok, reason = verify_entity_graph(graph)
    if not ok:
        # Historical summary-only entity_graph payloads have a graph hash but
        # intentionally lack nodes/edges.  They are legacy, not corrupt.
        if not graph.get("nodes") and graph.get("graph_hash"):
            subject["legacy_graph_observations"] += 1
            projection["source_chain"]["legacy_graph_ring_count"] += 1
            return
        raise TemporalGraphError(f"analysis ring contains invalid entity graph: {reason}")

    graph_network = str(graph.get("network") or fields["network"]).lower()
    if graph_network != fields["network"]:
        raise TemporalGraphError("entity graph network does not match analysis ring")
    graph_anchor = graph.get("anchor") or {}
    if (
        fields["pin"]["value"] is not None
        and graph_anchor.get("value") is not None
        and int(graph_anchor["value"]) != fields["pin"]["value"]
    ):
        raise TemporalGraphError("entity graph anchor does not match analysis pin")

    ref = _observation_ref(fields)
    current_entity_ids: set[str] = set()
    for node in graph.get("nodes") or []:
        node_id = str(node.get("id") or "")
        if not node_id:
            raise TemporalGraphError("entity graph node is missing an id")
        current_entity_ids.add(node_id)
        existing = projection["entities"].get(node_id)
        if existing is None:
            existing = {
                "id": node_id,
                "network": fields["network"],
                "type": node.get("type"),
                "address": node.get("address"),
                "label": node.get("label"),
                "roles": [],
                "role_events": [],
                "subjects": [],
                "first_seen": ref,
                "last_seen": ref,
                "observation_count": 0,
            }
            projection["entities"][node_id] = existing
        elif (
            existing.get("network") != fields["network"]
            or existing.get("address") != node.get("address")
            or existing.get("type") != node.get("type")
        ):
            raise TemporalGraphError("deterministic entity id collision")
        added_roles = sorted(set(node.get("roles") or []) - set(existing["roles"]))
        if added_roles:
            existing["role_events"].append({
                "event": "roles_added",
                "roles": added_roles,
                **ref,
            })
            existing["roles"] = sorted(set(existing["roles"]).union(added_roles))
        key = _subject_key(fields["network"], fields["subject"])
        if key not in existing["subjects"]:
            existing["subjects"].append(key)
            existing["subjects"].sort()
        existing["last_seen"] = ref
        existing["observation_count"] += 1

        # Entity node types remain precise (market, contract, address), while
        # this exact-address identity layer connects the same public address
        # across type changes and different analyzed tokens.  It is equality,
        # not a heuristic claim about beneficial ownership.
        address = str(node.get("address") or "")
        identity_id = f"identity-{canonical_hash([fields['network'], address])[:24]}"
        identity = projection["identities"].get(identity_id)
        if identity is None:
            identity = {
                "id": identity_id,
                "network": fields["network"],
                "address": address,
                "entity_ids": [],
                "entity_types": [],
                "roles": [],
                "subjects": [],
                "first_seen": ref,
                "last_seen": ref,
                "observation_count": 0,
            }
            projection["identities"][identity_id] = identity
        identity["entity_ids"] = sorted(
            set(identity["entity_ids"]).union([node_id])
        )
        identity["entity_types"] = sorted(
            set(identity["entity_types"]).union([str(node.get("type") or "")])
        )
        identity["roles"] = sorted(
            set(identity["roles"]).union(node.get("roles") or [])
        )
        if key not in identity["subjects"]:
            identity["subjects"].append(key)
            identity["subjects"].sort()
        identity["last_seen"] = ref
        identity["observation_count"] += 1
        existing["identity_id"] = identity_id

    subject["entity_ids"] = sorted(
        set(subject["entity_ids"]).union(current_entity_ids)
    )

    current_relationships: dict[str, dict[str, Any]] = {}
    for edge in graph.get("edges") or []:
        relationship_id = _relationship_key(fields["network"], edge)
        current_relationships[relationship_id] = edge
        semantics = _relationship_semantics(edge)
        semantic_hash = canonical_hash(semantics)
        existing = projection["relationships"].get(relationship_id)
        if existing is None:
            existing = {
                "id": relationship_id,
                "network": fields["network"],
                "source": edge.get("source"),
                "target": edge.get("target"),
                "relationship": edge.get("relationship"),
                "state": "active",
                "semantics": semantics,
                "semantic_hash": semantic_hash,
                "evidence_refs": edge.get("evidence_refs") or [],
                "subjects": [],
                "first_seen": ref,
                "last_seen": ref,
                "last_changed": ref,
                "observation_count": 1,
                "revision_count": 0,
            }
            projection["relationships"][relationship_id] = existing
            subject["relationship_events"].append(
                _event("appeared", relationship_id, fields, details={"semantics": semantics})
            )
        else:
            previous_state = existing.get("state")
            previous_hash = existing.get("semantic_hash")
            existing["observation_count"] += 1
            existing["last_seen"] = ref
            existing["evidence_refs"] = edge.get("evidence_refs") or []
            if previous_hash != semantic_hash:
                changed = sorted(
                    key
                    for key in set(existing.get("semantics") or {}).union(semantics)
                    if (existing.get("semantics") or {}).get(key) != semantics.get(key)
                )
                existing["revision_count"] += 1
                existing["last_changed"] = ref
                subject["relationship_events"].append(
                    _event(
                        "changed",
                        relationship_id,
                        fields,
                        details={
                            "changed_fields": changed,
                            "previous_semantic_hash": previous_hash,
                            "semantic_hash": semantic_hash,
                        },
                    )
                )
            elif previous_state in {"disappeared", "unconfirmed"}:
                subject["relationship_events"].append(
                    _event("reappeared", relationship_id, fields, details={"previous_state": previous_state})
                )
            else:
                subject["relationship_events"].append(
                    _event("reaffirmed", relationship_id, fields)
                )
            existing["semantics"] = semantics
            existing["semantic_hash"] = semantic_hash
            existing["state"] = "active"
        key = _subject_key(fields["network"], fields["subject"])
        if key not in existing["subjects"]:
            existing["subjects"].append(key)
            existing["subjects"].sort()

    previously_known = set(subject["relationship_ids"])
    current_ids = set(current_relationships)
    coverage = graph.get("relationship_coverage") or {}
    for relationship_id in sorted(previously_known - current_ids):
        existing = projection["relationships"].get(relationship_id)
        if not existing:
            continue
        relationship_type = str(existing.get("relationship") or "")
        coverage_state = str(coverage.get(relationship_type) or "unavailable")
        previous_state = existing.get("state")
        if coverage_state == "complete":
            next_state = "disappeared"
            event_type = "disappeared"
        elif coverage_state == "historical_static":
            # A creation relationship remains historically true even when a
            # later provider response omits it.
            continue
        else:
            next_state = "unconfirmed"
            event_type = "not_observed"
        if previous_state == next_state:
            continue
        existing["state"] = next_state
        subject["relationship_events"].append(
            _event(
                event_type,
                relationship_id,
                fields,
                details={"coverage": coverage_state, "previous_state": previous_state},
            )
        )

    subject["relationship_ids"] = sorted(previously_known.union(current_ids))
    subject["latest_graph"] = {
        "graph_hash": graph.get("graph_hash"),
        "schema_version": graph.get("schema_version"),
        "anchor": graph.get("anchor") or {},
        "relationship_coverage": coverage,
        "entity_count": len(current_entity_ids),
        "relationship_count": len(current_ids),
        **ref,
    }


def _apply_analysis_ring(
    projection: dict[str, Any], ring: dict[str, Any]
) -> None:
    try:
        fields = _analysis_fields(ring)
    except TemporalGraphError:
        # A recognized ring type with content too malformed to project (e.g.
        # missing subject) is excluded the same way an unrecognized ring type
        # already is -- tamper-evident on the chain, just not promoted into
        # the projection. One bad ring must not abort the fold over every
        # other ring, which is what building a whole status/recall response
        # from this projection depends on.
        return
    if fields is None:
        return
    subject = _ensure_subject(projection, fields)
    previous = subject["risk_timeline"][-1] if subject["risk_timeline"] else None
    subject["risk_timeline"].append(_risk_point(fields, previous))
    subject["risk_evolution"] = _risk_evolution(subject["risk_timeline"])
    subject["analysis_count"] += 1
    subject["last_observed"] = _observation_ref(fields)
    _apply_graph(projection, subject, fields)
    projection["source_chain"]["analysis_ring_count"] += 1


def _finalize_projection(
    projection: dict[str, Any], rings: list[dict[str, Any]]
) -> dict[str, Any]:
    # The projection consumes analysis events only. Outcome, faculty, alert,
    # and maintenance rings may advance the Timechain without making this read
    # model stale, so the committed source cursor is the latest analysis ring.
    analysis_rings = [
        ring for ring in rings if ring.get("ring_type") in ANALYSIS_RING_TYPES
    ]
    if analysis_rings:
        head = analysis_rings[-1]
        projection["source_chain"].update({
            "head_index": head.get("index"),
            "head_hash": head.get("ring_hash"),
            "ring_count": len(analysis_rings),
        })
    else:
        projection["source_chain"].update({
            "head_index": None,
            "head_hash": None,
            "ring_count": 0,
        })
    return _seal_projection(projection)


def _seal_projection(projection: dict[str, Any]) -> dict[str, Any]:
    """Recompute the deterministic summary and hash for a projection.

    Keeping this independent from a complete ring list lets the online path
    append one already-verified analysis ring in O(1).  Full rebuilds remain
    available for startup recovery and periodic integrity audits.
    """

    projection["summary"] = {
        "subject_count": len(projection["subjects"]),
        "entity_count": len(projection["entities"]),
        "identity_count": len(projection["identities"]),
        "relationship_count": len(projection["relationships"]),
        "active_relationship_count": sum(
            item.get("state") == "active"
            for item in projection["relationships"].values()
        ),
        "disappeared_relationship_count": sum(
            item.get("state") == "disappeared"
            for item in projection["relationships"].values()
        ),
        "unconfirmed_relationship_count": sum(
            item.get("state") == "unconfirmed"
            for item in projection["relationships"].values()
        ),
        "cross_subject_entity_count": sum(
            len(item.get("subjects") or []) > 1
            for item in projection["entities"].values()
        ),
        "cross_subject_identity_count": sum(
            len(item.get("subjects") or []) > 1
            for item in projection["identities"].values()
        ),
    }
    unsigned = {key: value for key, value in projection.items() if key != "projection_hash"}
    projection["projection_hash"] = canonical_hash(unsigned)
    return projection


def build_temporal_projection(
    rings: Iterable[dict[str, Any]],
) -> dict[str, Any]:
    """Build the complete deterministic projection from Timechain rings."""

    ordered = list(rings)
    projection = _blank_projection()
    for ring in ordered:
        _apply_analysis_ring(projection, ring)
    return _finalize_projection(projection, ordered)


def verify_temporal_projection(
    projection: dict[str, Any],
    rings: Iterable[dict[str, Any]] | None = None,
) -> tuple[bool, str]:
    if not isinstance(projection, dict):
        return False, "projection_not_object"
    if projection.get("schema_version") != TEMPORAL_GRAPH_SCHEMA_VERSION:
        return False, "unsupported_schema"
    expected = str(projection.get("projection_hash") or "").lower()
    if not _hash_ok(expected):
        return False, "projection_hash_missing"
    unsigned = {key: value for key, value in projection.items() if key != "projection_hash"}
    if canonical_hash(unsigned) != expected:
        return False, "projection_hash_mismatch"
    if rings is not None:
        ordered = list(rings)
        rebuilt = build_temporal_projection(ordered)
        if rebuilt.get("projection_hash") != expected:
            return False, "projection_drift"
    return True, "verified"


def subject_temporal_view(
    projection: dict[str, Any],
    network: str,
    subject: str,
    *,
    score_limit: int = 20,
    event_limit: int = 40,
    shared_entity_limit: int = 20,
) -> dict[str, Any]:
    normalized = _normalize_subject(network, subject)
    key = _subject_key(network, normalized)
    item = (projection.get("subjects") or {}).get(key)
    if item is None:
        return {
            "available": False,
            "schema_version": TEMPORAL_GRAPH_SCHEMA_VERSION,
            "network": network,
            "subject": normalized,
            "reason": "no_temporal_observations",
        }
    shared_entities = []
    subject_identity_ids = {
        ((projection.get("entities") or {}).get(entity_id) or {}).get("identity_id")
        for entity_id in item.get("entity_ids") or []
    }
    for identity_id in sorted(value for value in subject_identity_ids if value):
        identity = (projection.get("identities") or {}).get(identity_id) or {}
        subjects = identity.get("subjects") or []
        if len(subjects) <= 1:
            continue
        shared_entities.append({
            "identity_id": identity_id,
            "entity_ids": identity.get("entity_ids") or [],
            "types": identity.get("entity_types") or [],
            "address": identity.get("address"),
            "roles": identity.get("roles") or [],
            "subject_count": len(subjects),
            "other_subjects": [value for value in subjects if value != key][:10],
            "first_seen": identity.get("first_seen"),
            "last_seen": identity.get("last_seen"),
        })
    relationships = [
        (projection.get("relationships") or {}).get(identifier) or {}
        for identifier in item.get("relationship_ids") or []
    ]
    relationship_summary = {
        "known": len(relationships),
        "active": sum(value.get("state") == "active" for value in relationships),
        "disappeared": sum(value.get("state") == "disappeared" for value in relationships),
        "unconfirmed": sum(value.get("state") == "unconfirmed" for value in relationships),
        "appeared_events": sum(value.get("event") == "appeared" for value in item.get("relationship_events") or []),
        "changed_events": sum(value.get("event") == "changed" for value in item.get("relationship_events") or []),
        "disappeared_events": sum(value.get("event") == "disappeared" for value in item.get("relationship_events") or []),
        "not_observed_events": sum(value.get("event") == "not_observed" for value in item.get("relationship_events") or []),
    }
    return {
        "available": True,
        "schema_version": TEMPORAL_GRAPH_SCHEMA_VERSION,
        "network": network,
        "subject": normalized,
        "projection_hash": projection.get("projection_hash"),
        "source_chain": projection.get("source_chain") or {},
        "first_observed": item.get("first_observed"),
        "last_observed": item.get("last_observed"),
        "analysis_count": item.get("analysis_count", 0),
        "risk_evolution": item.get("risk_evolution") or {},
        "risk_timeline": (item.get("risk_timeline") or [])[-max(1, score_limit):],
        "relationship_summary": relationship_summary,
        "relationship_events": (item.get("relationship_events") or [])[-max(1, event_limit):],
        "shared_entities": shared_entities[:max(1, shared_entity_limit)],
        "latest_graph": item.get("latest_graph"),
        "legacy_graph_observations": item.get("legacy_graph_observations", 0),
        "limitations": projection.get("limitations") or [],
    }


class TemporalGraphStore:
    """Atomic local cache of the Timechain-derived temporal projection."""

    def __init__(self, chain_root: str | Path):
        self.chain_root = Path(chain_root)
        self.path = self.chain_root / TEMPORAL_GRAPH_FILENAME

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            return None
        ok, _ = verify_temporal_projection(value)
        return value if ok else None

    def write(self, projection: dict[str, Any]) -> None:
        ok, reason = verify_temporal_projection(projection)
        if not ok:
            raise TemporalGraphError(f"refusing invalid temporal projection: {reason}")
        self.chain_root.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + f".{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(projection, indent=2, sort_keys=True, ensure_ascii=True),
            encoding="utf-8",
        )
        os.replace(temporary, self.path)

    def rebuild(self, rings: Iterable[dict[str, Any]]) -> dict[str, Any]:
        ordered = list(rings)
        projection = build_temporal_projection(ordered)
        self.write(projection)
        return projection

    def refresh(self, rings: Iterable[dict[str, Any]]) -> dict[str, Any]:
        """Append newly sealed analysis rings or rebuild on any cursor doubt."""

        ordered = list(rings)
        existing = self.load()
        if existing is None:
            return self.rebuild(ordered)
        source = existing.get("source_chain") or {}
        cursor_index = source.get("head_index")
        cursor_hash = source.get("head_hash")
        analysis_rings = [
            ring
            for ring in ordered
            if ring.get("ring_type") in ANALYSIS_RING_TYPES
        ]
        if not analysis_rings:
            if cursor_index is None and int(source.get("analysis_ring_count") or 0) == 0:
                return existing
            return self.rebuild(ordered)
        if isinstance(cursor_index, bool) or not isinstance(cursor_index, int):
            return self.rebuild(ordered)
        cursor = next(
            (ring for ring in analysis_rings if ring.get("index") == cursor_index),
            None,
        )
        prior_count = sum(
            int(ring.get("index", -1)) <= cursor_index for ring in analysis_rings
        )
        if (
            cursor is None
            or cursor.get("ring_hash") != cursor_hash
            or prior_count != int(source.get("analysis_ring_count") or 0)
        ):
            return self.rebuild(ordered)
        new_rings = [
            ring
            for ring in analysis_rings
            if int(ring.get("index", -1)) > cursor_index
        ]
        if not new_rings:
            return existing
        projection = copy.deepcopy(existing)
        projection.pop("projection_hash", None)
        projection.pop("summary", None)
        for ring in new_rings:
            _apply_analysis_ring(projection, ring)
        projection = _finalize_projection(projection, ordered)
        self.write(projection)
        return projection

    def append_analysis_ring(self, ring: dict[str, Any]) -> dict[str, Any]:
        """Append one trusted analysis ring without replaying the Timechain.

        The caller must first verify the ring against the trusted Timechain
        head.  This projection is rebuildable and non-authoritative, so any
        missing/stale cursor is surfaced rather than repaired on the request
        path; a maintenance worker can perform the full rebuild later.
        """

        fields = _analysis_fields(ring)
        if fields is None:
            raise TemporalGraphError("ring is not an analysis ring")
        existing = self.load()
        if existing is None:
            raise TemporalGraphError("projection_missing_or_invalid")
        source = existing.get("source_chain") or {}
        cursor_index = source.get("head_index")
        cursor_hash = source.get("head_hash")
        ring_index = int(ring["index"])
        ring_hash = str(ring["ring_hash"])
        if cursor_index == ring_index:
            if cursor_hash != ring_hash:
                raise TemporalGraphError("projection_cursor_hash_mismatch")
            return existing
        if cursor_index is not None and (
            isinstance(cursor_index, bool)
            or not isinstance(cursor_index, int)
            or cursor_index >= ring_index
        ):
            raise TemporalGraphError("projection_cursor_out_of_order")

        projection = copy.deepcopy(existing)
        projection.pop("projection_hash", None)
        projection.pop("summary", None)
        _apply_analysis_ring(projection, ring)
        projection["source_chain"].update({
            "head_index": ring_index,
            "head_hash": ring_hash,
            "ring_count": int(source.get("ring_count") or 0) + 1,
        })
        projection = _seal_projection(projection)
        self.write(projection)
        return projection

    def verify(self, rings: Iterable[dict[str, Any]]) -> tuple[bool, str]:
        projection = self.load()
        if projection is None:
            return False, "projection_missing_or_invalid"
        return verify_temporal_projection(projection, rings)


def refresh_temporal_projection(
    timechain: Any,
    chain_root: str | Path,
    *,
    network: str,
    subject: str,
) -> dict[str, Any]:
    """Verify the Timechain, rebuild its projection, and return one subject view."""

    ok, report = timechain.verify()
    if not ok:
        raise TemporalGraphError(
            f"Timechain failed before temporal projection refresh: {report}"
        )
    rings = timechain.load()
    store = TemporalGraphStore(chain_root)
    projection = store.refresh(rings)
    return subject_temporal_view(projection, network, subject)


def append_temporal_projection(
    chain_root: str | Path,
    ring: dict[str, Any],
    *,
    network: str,
    subject: str,
) -> dict[str, Any]:
    """Update the derived temporal graph from one integrity-checked ring."""

    store = TemporalGraphStore(chain_root)
    projection = store.append_analysis_ring(ring)
    return subject_temporal_view(projection, network, subject)
