"""Governed faculty and learned-pattern lifecycle for Chainseer.

The Timechain records what happened; this module controls what learned behavior
is allowed to become active.  Its central invariant is deliberately simple:

    autonomous changes may only preserve or tighten risk controls.

A change that might raise a legitimacy score, lower a risk classification,
remove a hard stop, broaden admission, or loosen a numeric threshold is never
auto-activated.  It requires an explicit, proposal-bound human override and a
new Cypher Tempre registry epoch.  Signing, broadcasting, or enabling live
execution is outside this override mechanism and is always refused.

The governance projection lives inside ``registry/grown.json`` under a
top-level ``chainseer_governance`` key.  Cypher Tempre already hashes that file
into every registry epoch, so governance state is inside the existing integrity
perimeter instead of becoming an unauthenticated sidecar.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


GOVERNANCE_SCHEMA_VERSION = "1.0"
TIGHTEN_ONLY_POLICY_VERSION = "1.0"
OVERRIDE_CONFIRMATION = "I AUTHORIZE THIS RISK-RELAXING CHANGE"
GOVERNANCE_KEY = "chainseer_governance"

_SAFE_SCORE_EFFECTS = {"none", "same_or_lower_legitimacy", "same_or_higher_risk"}
_RELAXING_SCORE_EFFECTS = {"may_raise_legitimacy", "may_lower_risk"}
_SAFE_HARD_STOP_EFFECTS = {"none", "add_only"}
_RELAXING_HARD_STOP_EFFECTS = {"may_remove_or_suppress"}
_SAFE_ADMISSION_EFFECTS = {"none", "narrow_only"}
_RELAXING_ADMISSION_EFFECTS = {"may_broaden"}
_SAFE_EXECUTION_EFFECTS = {"none", "narrow_only"}
_FORBIDDEN_EXECUTION_EFFECTS = {"may_enable_or_broaden", "may_sign", "may_broadcast"}


class GovernanceError(ValueError):
    """A faculty or learned policy failed the production governance gate."""


def canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )


def canonical_hash(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _finite_number(value: Any) -> float:
    if isinstance(value, bool):
        raise GovernanceError("threshold values must be finite numbers")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise GovernanceError("threshold values must be finite numbers") from exc
    if not math.isfinite(number):
        raise GovernanceError("threshold values must be finite numbers")
    return number


@dataclass(frozen=True)
class EffectAssessment:
    classification: str
    automatic_activation_allowed: bool
    human_override_required: bool
    non_overridable: bool
    tightening_changes: tuple[str, ...]
    relaxing_changes: tuple[str, ...]
    unknown_changes: tuple[str, ...]
    manifest_hash: str
    policy_version: str = TIGHTEN_ONLY_POLICY_VERSION

    def to_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["tightening_changes"] = list(self.tightening_changes)
        value["relaxing_changes"] = list(self.relaxing_changes)
        value["unknown_changes"] = list(self.unknown_changes)
        return value


def cognitive_only_effect_manifest() -> dict[str, Any]:
    """The only effect contract autonomous Cambium faculties may receive.

    This is an architectural capability boundary, not a judgment about the
    faculty's prose: the cognition loop receives a bounded JSON string after
    deterministic scoring and has no score, hard-stop, admission, or execution
    mutation hook.
    """
    return {
        "schema_version": GOVERNANCE_SCHEMA_VERSION,
        "declared_effect": "observability_only",
        "authority": "cognitive_advisory_only",
        "score_effect": "none",
        "hard_stop_effect": "none",
        "admission_effect": "none",
        "execution_effect": "none",
        "threshold_changes": [],
    }


def assess_effect_manifest(manifest: dict[str, Any]) -> EffectAssessment:
    """Recompute an effect classification instead of trusting its label."""
    if not isinstance(manifest, dict):
        raise GovernanceError("a structured governance effect manifest is required")
    if str(manifest.get("schema_version") or "") != GOVERNANCE_SCHEMA_VERSION:
        raise GovernanceError("unsupported or missing governance schema_version")

    tightening: list[str] = []
    relaxing: list[str] = []
    unknown: list[str] = []
    non_overridable = False

    authority = str(manifest.get("authority") or "unknown")
    if authority == "cognitive_advisory_only":
        pass
    elif authority == "risk_policy":
        tightening.append("authority:risk_policy")
    else:
        unknown.append(f"authority:{authority}")

    score_effect = str(manifest.get("score_effect") or "unknown")
    if score_effect in _SAFE_SCORE_EFFECTS:
        if score_effect != "none":
            tightening.append(f"score_effect:{score_effect}")
    elif score_effect in _RELAXING_SCORE_EFFECTS:
        relaxing.append(f"score_effect:{score_effect}")
    else:
        unknown.append(f"score_effect:{score_effect}")

    hard_stop_effect = str(manifest.get("hard_stop_effect") or "unknown")
    if hard_stop_effect in _SAFE_HARD_STOP_EFFECTS:
        if hard_stop_effect != "none":
            tightening.append(f"hard_stop_effect:{hard_stop_effect}")
    elif hard_stop_effect in _RELAXING_HARD_STOP_EFFECTS:
        relaxing.append(f"hard_stop_effect:{hard_stop_effect}")
    else:
        unknown.append(f"hard_stop_effect:{hard_stop_effect}")

    admission_effect = str(manifest.get("admission_effect") or "unknown")
    if admission_effect in _SAFE_ADMISSION_EFFECTS:
        if admission_effect != "none":
            tightening.append(f"admission_effect:{admission_effect}")
    elif admission_effect in _RELAXING_ADMISSION_EFFECTS:
        relaxing.append(f"admission_effect:{admission_effect}")
    else:
        unknown.append(f"admission_effect:{admission_effect}")

    execution_effect = str(manifest.get("execution_effect") or "unknown")
    if execution_effect in _SAFE_EXECUTION_EFFECTS:
        if execution_effect != "none":
            tightening.append(f"execution_effect:{execution_effect}")
    elif execution_effect in _FORBIDDEN_EXECUTION_EFFECTS:
        relaxing.append(f"execution_effect:{execution_effect}")
        non_overridable = True
    else:
        unknown.append(f"execution_effect:{execution_effect}")

    changes = manifest.get("threshold_changes")
    if not isinstance(changes, list):
        raise GovernanceError("threshold_changes must be a list")
    for index, change in enumerate(changes):
        if not isinstance(change, dict):
            raise GovernanceError(f"threshold_changes[{index}] must be an object")
        control = str(change.get("control") or f"index_{index}")
        direction = str(change.get("tightening_direction") or "")
        if direction in {"increase", "decrease"}:
            current = _finite_number(change.get("current"))
            proposed = _finite_number(change.get("proposed"))
            delta = proposed - current
            is_tighter = delta >= 0 if direction == "increase" else delta <= 0
            if delta == 0:
                continue
            target = tightening if is_tighter else relaxing
            target.append(
                f"threshold:{control}:{current:g}->{proposed:g}:tighten_by_{direction}"
            )
        elif direction == "subset":
            current = set(change.get("current") or [])
            proposed = set(change.get("proposed") or [])
            if proposed == current:
                continue
            target = tightening if proposed.issubset(current) else relaxing
            target.append(f"set:{control}:subset")
        else:
            unknown.append(f"threshold:{control}:unknown_direction")

    if non_overridable:
        classification = "forbidden_execution_expansion"
    elif relaxing or unknown:
        classification = "human_override_required"
    elif tightening:
        classification = "tighten_only"
    else:
        classification = "observability_only"

    declared = str(manifest.get("declared_effect") or "")
    expected_declared = (
        "potentially_relaxing"
        if relaxing or unknown or non_overridable
        else classification
    )
    if declared != expected_declared:
        unknown.append(
            f"declared_effect_mismatch:{declared or 'missing'}!={expected_declared}"
        )
        if not non_overridable:
            classification = "human_override_required"

    override_required = bool(relaxing or unknown) and not non_overridable
    return EffectAssessment(
        classification=classification,
        automatic_activation_allowed=(
            not non_overridable and not relaxing and not unknown
        ),
        human_override_required=override_required,
        non_overridable=non_overridable,
        tightening_changes=tuple(tightening),
        relaxing_changes=tuple(relaxing),
        unknown_changes=tuple(unknown),
        manifest_hash=canonical_hash(manifest),
    )


def calibration_effect_manifest(
    current: dict[str, Any], proposed: dict[str, Any]
) -> dict[str, Any]:
    """Map the complete CalibrationPolicy surface to explicit directions."""
    changes = []
    for control, direction in (
        ("min_trade_score", "increase"),
        ("max_false_negative_rate", "decrease"),
        ("min_outcomes", "increase"),
        ("max_permit_block_drift", "decrease"),
        ("max_quote_age_blocks", "decrease"),
        ("permit_ttl_seconds", "decrease"),
    ):
        changes.append(
            {
                "control": control,
                "current": current.get(control),
                "proposed": proposed.get(control),
                "tightening_direction": direction,
            }
        )
    changes.append(
        {
            "control": "allowed_risk_levels",
            "current": list(current.get("allowed_risk_levels") or []),
            "proposed": list(proposed.get("allowed_risk_levels") or []),
            "tightening_direction": "subset",
        }
    )
    manifest = {
        "schema_version": GOVERNANCE_SCHEMA_VERSION,
        "declared_effect": "tighten_only",
        "authority": "risk_policy",
        "score_effect": "none",
        "hard_stop_effect": "none",
        "admission_effect": "none",
        "execution_effect": "none",
        "threshold_changes": changes,
    }
    preliminary = assess_effect_manifest(manifest)
    if preliminary.relaxing_changes or preliminary.unknown_changes:
        manifest["declared_effect"] = "potentially_relaxing"
    return manifest


def assess_calibration_change(
    current: dict[str, Any], proposed: dict[str, Any]
) -> EffectAssessment:
    return assess_effect_manifest(calibration_effect_manifest(current, proposed))


def validate_faculty_pack_governance(pack: dict[str, Any]) -> dict[str, Any]:
    """Require every faculty in a curated pack to have a safe effect contract."""
    decisions = []
    for definition in pack.get("faculties") or []:
        assessment = assess_effect_manifest(definition.get("governance"))
        if not assessment.automatic_activation_allowed:
            raise GovernanceError(
                f"faculty {definition.get('name')!r} is not tighten-only/advisory: "
                f"{assessment.classification}"
            )
        if str((definition.get("governance") or {}).get("authority")) != (
            "cognitive_advisory_only"
        ):
            raise GovernanceError(
                f"faculty {definition.get('name')!r} exceeds cognitive-only authority"
            )
        decisions.append(
            {
                "kind": definition.get("kind"),
                "name": definition.get("name"),
                "assessment": assessment.to_dict(),
            }
        )
    if not decisions:
        raise GovernanceError("faculty pack contains no governed faculties")
    return {
        "policy_version": TIGHTEN_ONLY_POLICY_VERSION,
        "automatic_activation_allowed": True,
        "faculties": decisions,
    }


def _faculty_fingerprint(kind: str, definition: dict[str, Any]) -> str:
    return canonical_hash(
        {
            "kind": kind,
            "name": definition.get("name"),
            "function": definition.get("function"),
            "effect": definition.get("effect"),
            "seed_terms": definition.get("seed_terms") or [],
        }
    )


def _read_grown(root: Path) -> dict[str, Any]:
    path = root / "registry" / "grown.json"
    if not path.is_file():
        return {"registry": "grown", "senses": [], "modalities": []}
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise GovernanceError("registry/grown.json is not an object")
    return value


def _atomic_write_grown(root: Path, value: dict[str, Any]) -> None:
    path = root / "registry" / "grown.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(
        json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False),
        encoding="utf-8",
    )
    os.replace(temp, path)


def _governance_state(grown: dict[str, Any]) -> dict[str, Any]:
    state = grown.setdefault(
        GOVERNANCE_KEY,
        {
            "schema_version": GOVERNANCE_SCHEMA_VERSION,
            "policy_version": TIGHTEN_ONLY_POLICY_VERSION,
            "faculties": {},
            "patterns": {},
        },
    )
    if str(state.get("schema_version")) != GOVERNANCE_SCHEMA_VERSION:
        raise GovernanceError("unsupported Chainseer governance registry schema")
    state.setdefault("faculties", {})
    state.setdefault("patterns", {})
    return state


def seal_registry_mutation(epochs_module, root, reason: str, write):
    """Authorize a registry mutation from a verified baseline, then commit it.

    Cypher Tempre >=3.30 refuses to seal an epoch over a registry that already
    differs from the last one, because a post-write snapshot cannot distinguish
    a legitimate change from injected content -- it lets a tamper alarm
    self-clear. The supported pattern is a preflight ticket from
    ``begin_mutation()`` that validates the live registry BEFORE the write and
    binds the later commit to that exact epoch, which also catches a registry
    that changed underneath us mid-mutation.

    ``write`` is a zero-argument callable performing the actual registry write.
    Falls back to the pre-3.30 post-write seal when the installed skill has no
    ``begin_mutation``, so the repo still runs against an older bundle.
    """
    begin = getattr(epochs_module, "begin_mutation", None)
    ticket = begin(root) if callable(begin) else None
    write()
    if ticket is not None:
        return epochs_module.seal_epoch(
            root, reason=reason, expected_previous=ticket
        )
    return epochs_module.seal_epoch(root, reason=reason)


def register_faculty_governance(
    root: str | Path,
    definitions: list[dict[str, Any]],
    *,
    source: str,
    default_manifest: dict[str, Any] | None = None,
) -> bool:
    """Place faculty effect contracts in the epoch-covered grown registry.

    The caller owns the surrounding registry transaction and must seal/check an
    epoch after this returns ``True``.  This lets a curated-pack import and its
    governance metadata share one epoch rather than creating a mismatched
    intermediate state.
    """
    root = Path(root)
    grown = _read_grown(root)
    state = _governance_state(grown)
    records = state["faculties"]
    live_definitions = {}
    for registry_key, registry_kind in (
        ("senses", "sense"), ("modalities", "modality")
    ):
        for item in grown.get(registry_key) or []:
            live_definitions[(registry_kind, item.get("name"))] = item
    changed = False
    for definition in definitions:
        kind = str(definition.get("kind") or "")
        name = str(definition.get("name") or "")
        if kind not in {"sense", "modality"} or not name:
            raise GovernanceError("cannot govern an invalid faculty definition")
        manifest = definition.get("governance") or default_manifest
        assessment = assess_effect_manifest(manifest)
        if not assessment.automatic_activation_allowed:
            raise GovernanceError(
                f"faculty {name!r} requires a human override and cannot auto-install"
            )
        if str((manifest or {}).get("authority")) != "cognitive_advisory_only":
            raise GovernanceError(f"faculty {name!r} exceeds cognitive-only authority")
        key = f"{kind}:{name}"
        live_definition = live_definitions.get((kind, name), definition)
        record = {
            "kind": kind,
            "name": name,
            "source": source,
            "authority": "cognitive_advisory_only",
            "effect_manifest": manifest,
            "effect_assessment": assessment.to_dict(),
            "faculty_fingerprint": _faculty_fingerprint(kind, live_definition),
            "active": True,
        }
        if records.get(key) != record:
            records[key] = record
            changed = True
    if changed:
        state["updated_at"] = utc_now_iso()
        _atomic_write_grown(root, grown)
    return changed


def migrate_cognitive_faculty_governance(root: str | Path) -> dict[str, Any]:
    """Bind legacy grown faculties to the existing cognitive-only boundary."""
    root = Path(root)
    grown = _read_grown(root)
    state = _governance_state(grown)
    existing = state.get("faculties") or {}
    definitions = []
    for key, kind in (("senses", "sense"), ("modalities", "modality")):
        for item in grown.get(key) or []:
            if f"{kind}:{item.get('name')}" not in existing:
                definitions.append({**item, "kind": kind})
    changed = register_faculty_governance(
        root,
        definitions,
        source="legacy_cognitive_registry_migration",
        default_manifest=cognitive_only_effect_manifest(),
    ) if definitions else False
    return {
        "changed": changed,
        "faculty_count": len(definitions),
        "already_governed": len(existing),
    }


def verify_governance_registry(root: str | Path) -> tuple[bool, list[str]]:
    root = Path(root)
    try:
        grown = _read_grown(root)
        state = _governance_state(grown)
    except (OSError, json.JSONDecodeError, GovernanceError) as exc:
        return False, [str(exc)]
    errors = []
    records = state.get("faculties") or {}
    count = 0
    for key, kind in (("senses", "sense"), ("modalities", "modality")):
        for item in grown.get(key) or []:
            count += 1
            identity = f"{kind}:{item.get('name')}"
            record = records.get(identity)
            if not record:
                errors.append(f"faculty lacks governance record: {identity}")
                continue
            try:
                assessment = assess_effect_manifest(record.get("effect_manifest"))
            except GovernanceError as exc:
                errors.append(f"{identity}: {exc}")
                continue
            if not assessment.automatic_activation_allowed:
                errors.append(f"{identity}: active faculty is not cognitive/tighten-only")
            expected = _faculty_fingerprint(kind, item)
            if record.get("faculty_fingerprint") != expected:
                errors.append(f"{identity}: definition changed after governance review")

    for pattern_hash, record in (state.get("patterns") or {}).items():
        if pattern_hash != record.get("proposal_hash"):
            errors.append(f"pattern key/hash mismatch: {pattern_hash}")
            continue
        if record.get("state") != "active":
            continue
        try:
            assessment = assess_effect_manifest(record.get("effect_manifest"))
        except GovernanceError as exc:
            errors.append(f"pattern {pattern_hash}: {exc}")
            continue
        if assessment.non_overridable:
            errors.append(f"pattern {pattern_hash}: forbidden execution expansion")
        elif not assessment.automatic_activation_allowed:
            receipt = record.get("human_override") or {}
            if not _valid_override_dict(receipt, pattern_hash):
                errors.append(f"pattern {pattern_hash}: relaxing activation lacks override")
        if not isinstance(record.get("registry_epoch"), int):
            errors.append(f"pattern {pattern_hash}: active state lacks registry epoch")
        validation = record.get("validation") or {}
        if not validation.get("benchmark_hash") or not validation.get(
            "outcome_record_hashes"
        ):
            errors.append(f"pattern {pattern_hash}: active state lacks held-out validation")
    if errors:
        return False, errors
    return True, [
        f"governance registry verified: {count} faculties, "
        f"{len(state.get('patterns') or {})} learned patterns"
    ]


@dataclass(frozen=True)
class HumanOverride:
    approval_id: str
    approved_by: str
    approved_at: str
    reason: str
    proposal_hash: str
    confirmation: str

    def validate(self) -> None:
        if not self.approval_id.strip() or len(self.approval_id) > 160:
            raise GovernanceError("human override requires a bounded approval_id")
        identity = self.approved_by.strip()
        if not identity or identity.lower() in {
            "agent", "chainseer", "system", "automation", "bot"
        }:
            raise GovernanceError("human override requires an explicit human identity")
        if len(self.reason.strip()) < 12 or len(self.reason) > 1000:
            raise GovernanceError("human override requires a substantive bounded reason")
        try:
            datetime.fromisoformat(self.approved_at.replace("Z", "+00:00"))
        except (AttributeError, ValueError) as exc:
            raise GovernanceError("human override approved_at must be ISO-8601") from exc
        if not self.proposal_hash or len(self.proposal_hash) != 64:
            raise GovernanceError("human override must bind the exact proposal hash")
        if self.confirmation != OVERRIDE_CONFIRMATION:
            raise GovernanceError("human override confirmation phrase is not exact")


def _valid_override_dict(value: dict[str, Any], proposal_hash: str) -> bool:
    try:
        receipt = HumanOverride(**{
            key: value.get(key)
            for key in (
                "approval_id", "approved_by", "approved_at", "reason",
                "proposal_hash", "confirmation",
            )
        })
        receipt.validate()
        return receipt.proposal_hash == proposal_hash
    except (TypeError, GovernanceError):
        return False


class GovernedPatternRegistry:
    """Candidate -> shadow -> validated -> active pattern lifecycle."""

    def __init__(self, root: str | Path, tc: Any, epochs_module: Any):
        self.root = Path(root)
        self.tc = tc
        self.epochs = epochs_module

    def _assert_integrity(self) -> None:
        ok, report = self.tc.verify()
        if not ok:
            raise GovernanceError("Timechain verification failed: " + "; ".join(report))
        ok, report = self.epochs.check_epoch(self.root)
        if not ok:
            raise GovernanceError("registry epoch verification failed: " + "; ".join(report))

    def _load(self) -> tuple[dict[str, Any], dict[str, Any]]:
        grown = _read_grown(self.root)
        return grown, _governance_state(grown)

    def _persist_epoch(
        self,
        grown: dict[str, Any],
        *,
        reason: str,
        expected_epoch: int,
    ) -> dict[str, Any]:
        # Authorize the mutation from a VERIFIED baseline before writing, then
        # bind the commit to that exact ticket. Sealing after the write and
        # snapshotting whatever is on disk cannot tell a legitimate change from
        # injected content -- it makes a tamper alarm self-clear. The ticket
        # also detects a registry that changed underneath us between authorize
        # and commit. Falls back to the older post-write seal when running
        # against a skill build that predates begin_mutation().
        begin = getattr(self.epochs, "begin_mutation", None)
        ticket = begin(self.root) if callable(begin) else None
        _atomic_write_grown(self.root, grown)
        if ticket is not None:
            epoch = self.epochs.seal_epoch(
                self.root, reason=reason, expected_previous=ticket
            )
        else:
            epoch = self.epochs.seal_epoch(self.root, reason=reason)
        if not epoch or epoch.get("index") != expected_epoch:
            raise GovernanceError("governance mutation did not create the expected registry epoch")
        ok, report = self.epochs.check_epoch(self.root)
        if not ok:
            raise GovernanceError("new registry epoch failed verification: " + "; ".join(report))
        return epoch

    def propose(self, pattern: dict[str, Any]) -> dict[str, Any]:
        """Register inert candidate data; never execute a proposed rule."""
        self._assert_integrity()
        if not isinstance(pattern, dict) or not str(pattern.get("name") or "").strip():
            raise GovernanceError("pattern proposal requires a name")
        assessment = assess_effect_manifest(pattern.get("effect_manifest"))
        proposal_body = {
            "schema_version": GOVERNANCE_SCHEMA_VERSION,
            "name": str(pattern.get("name"))[:160],
            "version": str(pattern.get("version") or "0.1.0")[:40],
            "description": str(pattern.get("description") or "")[:1000],
            "rule": pattern.get("rule") or {},
            "effect_manifest": pattern.get("effect_manifest"),
            "evidence": pattern.get("evidence") or {},
        }
        proposal_hash = canonical_hash(proposal_body)
        grown, state = self._load()
        existing = state["patterns"].get(proposal_hash)
        if existing:
            return existing
        expected_epoch = int(self.tc.height())
        record = {
            **proposal_body,
            "proposal_hash": proposal_hash,
            "state": "candidate",
            "effect_assessment": assessment.to_dict(),
            "activation_policy": (
                "forbidden"
                if assessment.non_overridable
                else "human_override_required"
                if assessment.human_override_required
                else "tighten_only"
            ),
            "proposed_at": utc_now_iso(),
            "registry_epoch": expected_epoch,
        }
        state["patterns"][proposal_hash] = record
        state["updated_at"] = utc_now_iso()
        epoch = self._persist_epoch(
            grown,
            reason=f"Chainseer learned-pattern candidate {proposal_hash[:12]}",
            expected_epoch=expected_epoch,
        )
        ring = self.tc.seal(
            "governance_pattern_proposal",
            {
                "summary": f"Registered inert Chainseer pattern candidate {proposal_hash[:12]}",
                "proposal_hash": proposal_hash,
                "state": "candidate",
                "effect_assessment": assessment.to_dict(),
                "registry_epoch": epoch["index"],
                "executable": False,
            },
            poq={
                "coherence": 245, "relevance": 250, "novelty": 230,
                "consistency": 250, "depth": 245, "covenant": 255,
            },
        )
        return {**record, "governance_ring": ring["index"]}

    def transition(
        self,
        proposal_hash: str,
        target_state: str,
        evidence: dict[str, Any] | None = None,
        *,
        human_override: HumanOverride | None = None,
    ) -> dict[str, Any]:
        self._assert_integrity()
        if target_state not in {"shadow", "validated", "active", "rejected", "retired"}:
            raise GovernanceError("unsupported pattern lifecycle state")
        grown, state = self._load()
        record = state["patterns"].get(proposal_hash)
        if not record:
            raise GovernanceError("unknown pattern proposal hash")
        current = record.get("state")
        allowed = {
            "candidate": {"shadow", "rejected"},
            "shadow": {"validated", "rejected"},
            "validated": {"active", "rejected"},
            "active": {"retired"},
            "rejected": set(),
            "retired": set(),
        }
        if target_state not in allowed.get(current, set()):
            raise GovernanceError(f"invalid lifecycle transition {current!r} -> {target_state!r}")

        assessment = assess_effect_manifest(record.get("effect_manifest"))
        evidence = dict(evidence or {})
        if target_state == "validated":
            if not evidence.get("benchmark_hash"):
                raise GovernanceError("validation requires a held-out benchmark_hash")
            hashes = evidence.get("outcome_record_hashes")
            if not isinstance(hashes, list) or not hashes:
                raise GovernanceError("validation requires provenance-bound outcome_record_hashes")
            if int(evidence.get("sample_size") or 0) <= 0:
                raise GovernanceError("validation requires a positive sample_size")
            record["validation"] = {
                "benchmark_hash": str(evidence["benchmark_hash"]),
                "outcome_record_hashes": [str(value) for value in hashes],
                "sample_size": int(evidence["sample_size"]),
                "dangerous_false_negative_delta": evidence.get(
                    "dangerous_false_negative_delta"
                ),
                "false_positive_delta": evidence.get("false_positive_delta"),
                "validated_at": utc_now_iso(),
            }

        override_dict = None
        if target_state == "active":
            if assessment.non_overridable:
                raise GovernanceError("live execution expansion is non-overridable")
            if assessment.human_override_required:
                if human_override is None:
                    raise GovernanceError(
                        "risk-relaxing pattern activation requires explicit human override"
                    )
                human_override.validate()
                if human_override.proposal_hash != proposal_hash:
                    raise GovernanceError("human override does not bind this proposal")
                override_dict = asdict(human_override)
                record["human_override"] = override_dict
            elif human_override is not None:
                raise GovernanceError("human override supplied for a tighten-only pattern")

        expected_epoch = int(self.tc.height())
        record["state"] = target_state
        record["registry_epoch"] = expected_epoch
        record["updated_at"] = utc_now_iso()
        record.setdefault("history", []).append(
            {
                "from": current,
                "to": target_state,
                "at": record["updated_at"],
                "evidence_hash": canonical_hash(evidence) if evidence else None,
            }
        )
        state["updated_at"] = utc_now_iso()
        epoch = self._persist_epoch(
            grown,
            reason=(
                f"Chainseer learned-pattern {target_state} {proposal_hash[:12]}"
            ),
            expected_epoch=expected_epoch,
        )
        payload = {
            "summary": f"Chainseer pattern {proposal_hash[:12]} moved {current} -> {target_state}",
            "proposal_hash": proposal_hash,
            "from_state": current,
            "to_state": target_state,
            "effect_assessment": assessment.to_dict(),
            "evidence_hash": canonical_hash(evidence) if evidence else None,
            "registry_epoch": epoch["index"],
            "human_override": override_dict,
            "automatic_activation": target_state == "active" and override_dict is None,
        }
        ring = self.tc.seal(
            "governance_pattern_transition",
            payload,
            poq={
                "coherence": 248, "relevance": 252, "novelty": 235,
                "consistency": 252, "depth": 248, "covenant": 255,
            },
        )
        return {**record, "governance_ring": ring["index"]}

    def status(self) -> dict[str, Any]:
        grown, state = self._load()
        ok, report = verify_governance_registry(self.root)
        states: dict[str, int] = {}
        for record in state.get("patterns", {}).values():
            key = str(record.get("state") or "unknown")
            states[key] = states.get(key, 0) + 1
        return {
            "ok": ok,
            "report": report,
            "policy_version": state.get("policy_version"),
            "faculty_count": len(state.get("faculties") or {}),
            "pattern_count": len(state.get("patterns") or {}),
            "pattern_states": states,
            "tighten_only": True,
            "override_confirmation": OVERRIDE_CONFIRMATION,
            "live_execution_override_allowed": False,
            "grown_registry_hash": canonical_hash(grown),
        }

    def migrate_cognitive_faculties(self) -> dict[str, Any]:
        """One-time, integrity-checked migration for pre-governance registries."""
        self._assert_integrity()
        expected_epoch = int(self.tc.height())
        result: dict[str, Any] = {}

        def _migrate():
            result.update(migrate_cognitive_faculty_governance(self.root))

        epoch = None
        # The ticket must be taken before the migration writes, so authorize
        # and commit as one unit rather than snapshotting after the fact.
        begin = getattr(self.epochs, "begin_mutation", None)
        ticket = begin(self.root) if callable(begin) else None
        _migrate()
        if result.get("changed"):
            reason = (
                "Chainseer tighten-only governance migration for "
                f"{result.get('faculty_count')} cognitive faculties"
            )
            epoch = (
                self.epochs.seal_epoch(
                    self.root, reason=reason, expected_previous=ticket
                )
                if ticket is not None
                else self.epochs.seal_epoch(self.root, reason=reason)
            )
            if not epoch or epoch.get("index") != expected_epoch:
                raise GovernanceError(
                    "faculty governance migration did not create the expected epoch"
                )
            ring = self.tc.seal(
                "governance_faculty_migration",
                {
                    "summary": (
                        "Bound legacy Chainseer faculties to the "
                        "cognitive-advisory tighten-only authority boundary"
                    ),
                    "faculty_count": result.get("faculty_count"),
                    "registry_epoch": epoch["index"],
                    "authority": "cognitive_advisory_only",
                    "score_mutation": False,
                    "hard_stop_mutation": False,
                    "admission_mutation": False,
                    "execution_mutation": False,
                },
                poq={
                    "coherence": 250, "relevance": 252, "novelty": 238,
                    "consistency": 252, "depth": 248, "covenant": 255,
                },
            )
            result["governance_ring"] = ring["index"]
            result["registry_epoch"] = epoch["index"]
        ok, report = verify_governance_registry(self.root)
        if not ok:
            raise GovernanceError(
                "faculty governance migration failed verification: "
                + "; ".join(report)
            )
        chain_ok, chain_report = self.tc.verify()
        if not chain_ok:
            raise GovernanceError(
                "Timechain failed after faculty governance migration: "
                + "; ".join(chain_report)
            )
        return {**result, "ok": True, "report": report}


def _load_module(path: Path, name: str):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
    spec = importlib.util.spec_from_file_location(name, path / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _skill_dir() -> Path:
    configured = os.environ.get("CHAINSEER_SKILL_DIR", "").strip()
    candidates = [
        Path(configured).expanduser() if configured else None,
        Path.home() / ".zcode" / "skills" / "cypher-tempre-self-model",
        Path.home() / ".claude" / "skills" / "cypher-tempre-self-model",
        Path.home() / ".codex" / "skills" / "cypher-tempre-self-model",
    ]
    for candidate in candidates:
        if candidate and (candidate / "timechain.py").is_file():
            return candidate
    raise RuntimeError("Cypher Tempre Timechain skill was not found")


def _registry_from_args(args) -> GovernedPatternRegistry:
    skill = _skill_dir()
    tc_module = _load_module(skill, "timechain")
    epochs_module = _load_module(skill, "epochs")
    root = Path(args.root)
    return GovernedPatternRegistry(root, tc_module.Timechain(root), epochs_module)


def _json_file(path: str) -> dict[str, Any]:
    file_path = Path(path)
    if file_path.stat().st_size > 1_000_000:
        raise GovernanceError("governance input exceeds 1 MB")
    value = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise GovernanceError("governance input must be a JSON object")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=str(Path(__file__).parent / "chainseer_chain"))
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    commands.add_parser("migrate-cognitive-faculties")
    propose = commands.add_parser("propose")
    propose.add_argument("manifest")
    transition = commands.add_parser("transition")
    transition.add_argument("proposal_hash")
    transition.add_argument("state", choices=("shadow", "validated", "active", "rejected", "retired"))
    transition.add_argument("--evidence")
    transition.add_argument("--approval-id")
    transition.add_argument("--approved-by")
    transition.add_argument("--approved-at")
    transition.add_argument("--reason")
    transition.add_argument("--confirm")
    args = parser.parse_args()
    registry = _registry_from_args(args)
    if args.command == "status":
        result = registry.status()
    elif args.command == "migrate-cognitive-faculties":
        result = registry.migrate_cognitive_faculties()
    elif args.command == "propose":
        result = registry.propose(_json_file(args.manifest))
    else:
        evidence = _json_file(args.evidence) if args.evidence else {}
        override = None
        supplied = [
            args.approval_id, args.approved_by, args.approved_at,
            args.reason, args.confirm,
        ]
        if any(value is not None for value in supplied):
            if not all(value is not None for value in supplied):
                raise GovernanceError("all human override fields must be supplied together")
            override = HumanOverride(
                approval_id=args.approval_id,
                approved_by=args.approved_by,
                approved_at=args.approved_at,
                reason=args.reason,
                proposal_hash=args.proposal_hash,
                confirmation=args.confirm,
            )
        result = registry.transition(
            args.proposal_hash,
            args.state,
            evidence,
            human_override=override,
        )
    print(json.dumps(result, indent=2, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
