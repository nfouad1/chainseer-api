"""Verifiable recall and recovery for the Chainseer Timechain Memory Core.

The Timechain and its epoch-covered registry are authoritative.  This module
only exposes subject-scoped, citation-complete claims and rebuildable operator
views.  It never returns raw ring payloads, provider requests, credentials, or
an execution capability.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from chainseer_governance import verify_governance_registry
from chainseer_outcome_ledger import (
    SUPPORTED_ANALYSIS_RING_TYPES,
    analysis_reference_from_ring,
    verify_outcome_record,
    verify_outcome_rings,
)
from chainseer_temporal_graph import (
    TemporalGraphStore,
    build_temporal_projection,
    subject_temporal_view,
    verify_temporal_projection,
)


MEMORY_SCHEMA_VERSION = "1.0"
BACKUP_SCHEMA_VERSION = "1.0"
RECOVERY_STATUS_FILENAME = "memory_recovery_status-v1.json"
# A learning producer that has silently stopped must not keep reporting
# healthy. Distinguish "never produced an outcome yet" (a legitimately empty
# corpus) from "produced outcomes, then went quiet" (a stalled producer) --
# only the latter is a fault. Generous enough that ordinary gaps between
# due checkpoints never trip it.
STALE_OUTCOME_SECONDS = 48 * 3600
ALLOWED_TOPICS = {
    "latest_assessment",
    "risk_history",
    "entity_history",
    "outcomes",
}
SUPPORTED_NETWORKS = {"robinhood", "base", "solana"}
HASH_RE = re.compile(r"^[a-f0-9]{64}$")


class MemoryCoreError(ValueError):
    """A recall, citation, backup, or recovery invariant was violated."""


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_subject(network: str, subject: str) -> str:
    network = str(network or "").strip().lower()
    if network not in SUPPORTED_NETWORKS:
        raise MemoryCoreError("unsupported memory network")
    subject = str(subject or "").strip()
    if not subject:
        raise MemoryCoreError("memory subject is required")
    return subject if network == "solana" else subject.lower()


def _safe_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _analysis_value(ring: dict[str, Any]) -> dict[str, Any]:
    payload = ring.get("payload") or {}
    ring_type = ring.get("ring_type")
    if ring_type == "solana_token_analysis":
        analysis = payload.get("analysis") or {}
        score = analysis.get("legitimacy_score")
        hard_stops = analysis.get("hard_stop_codes") or []
    elif ring_type == "base_launch_analysis":
        analysis = payload.get("decision") or {}
        score = analysis.get("score")
        hard_stops = analysis.get("hard_stops") or []
    else:
        analysis = payload
        score = payload.get("legitimacy_score")
        hard_stops = payload.get("hard_stop_overrides") or []
    codes = []
    for item in hard_stops:
        code = item.get("code") if isinstance(item, dict) else item
        if code is not None and str(code) not in codes:
            codes.append(str(code))
    return {
        "risk_level": analysis.get("risk_level"),
        "legitimacy_score": score,
        "action": analysis.get("action_label"),
        "confidence": analysis.get(
            "confidence_grade", analysis.get("confidence")
        ),
        "hard_stop_codes": sorted(codes),
        "component_scores": analysis.get("component_scores") or {},
        "evidence_state": payload.get("evidence_state") or "unknown_legacy",
    }


def _citation(
    *,
    ring: dict[str, Any],
    evidence_hash: str,
    evidence_kind: str,
    anchor_type: str | None,
    anchor_value: int | None,
    claim_path: str,
    binding_state: str,
    reference_hash: str | None = None,
) -> dict[str, Any]:
    citation = {
        "ring": int(ring["index"]),
        "ring_hash": str(ring["ring_hash"]).lower(),
        "ring_type": str(ring.get("ring_type") or ""),
        "ring_timestamp": ring.get("timestamp"),
        "evidence_hash": evidence_hash,
        "evidence_kind": evidence_kind,
        "anchor": {"type": anchor_type, "value": anchor_value},
        "claim_path": claim_path,
        "binding_state": binding_state,
        "reference_hash": reference_hash,
    }
    citation["citation_hash"] = canonical_hash(citation)
    return citation


def _claim(
    category: str,
    statement: str,
    value: dict[str, Any],
    observed_at: str | None,
    citations: list[dict[str, Any]],
) -> dict[str, Any]:
    if not citations:
        raise MemoryCoreError("uncited claims are forbidden")
    item = {
        "category": category,
        "statement": statement,
        "value": value,
        "observed_at": observed_at,
        "citations": citations,
    }
    item["claim_id"] = f"claim-{canonical_hash(item)[:24]}"
    item["claim_hash"] = canonical_hash(item)
    return item


class MemoryRecallEngine:
    """Return only verified, subject-scoped claims with exact citations."""

    def __init__(self, tc: Any, chain_root: str | Path):
        self.tc = tc
        self.chain_root = Path(chain_root)
        self._lock = threading.RLock()
        self._verified_signature: tuple[int, int] | None = None

    def _signature(self) -> tuple[int, int]:
        path = Path(self.tc.rings_path)
        stat = path.stat()
        return stat.st_size, stat.st_mtime_ns

    def _verified_rings(self) -> list[dict[str, Any]]:
        with self._lock:
            signature = self._signature()
            if signature != self._verified_signature:
                ok, report = self.tc.verify()
                if not ok:
                    raise MemoryCoreError(
                        "Timechain verification failed: " + "; ".join(report)
                    )
                self._verified_signature = signature
            return list(self.tc.iter_rings())

    @staticmethod
    def _analysis_reference(
        ring: dict[str, Any], exclusions: list[dict[str, Any]]
    ) -> dict[str, Any] | None:
        try:
            reference = analysis_reference_from_ring(ring)
        except (TypeError, ValueError) as exc:
            exclusions.append(
                {
                    "ring": ring.get("index"),
                    "reason": "invalid_analysis_reference",
                    "detail": str(exc),
                }
            )
            return None
        if reference.get("binding_state") != "sealed_at_analysis":
            exclusions.append(
                {
                    "ring": ring.get("index"),
                    "reason": "legacy_analysis_not_evidence_bound_at_seal",
                }
            )
            return None
        if not reference.get("evidence_complete"):
            exclusions.append(
                {
                    "ring": ring.get("index"),
                    "reason": "analysis_evidence_incomplete",
                }
            )
            return None
        return reference

    @staticmethod
    def _analysis_citation(
        ring: dict[str, Any], reference: dict[str, Any], claim_path: str
    ) -> dict[str, Any]:
        return _citation(
            ring=ring,
            evidence_hash=reference["original_evidence_hash"],
            evidence_kind="analysis_evidence_manifest",
            anchor_type=reference.get("anchor_type"),
            anchor_value=_safe_int(reference.get("anchor_value")),
            claim_path=claim_path,
            binding_state=reference["binding_state"],
            reference_hash=reference.get("reference_hash"),
        )

    def query(
        self,
        network: str,
        subject: str,
        *,
        topics: Iterable[str] | None = None,
        limit: int = 20,
    ) -> dict[str, Any]:
        network = str(network or "").strip().lower()
        subject = _normalize_subject(network, subject)
        requested = list(topics or sorted(ALLOWED_TOPICS))
        unknown = sorted(set(requested) - ALLOWED_TOPICS)
        if unknown:
            raise MemoryCoreError(
                "unsupported memory topics: " + ", ".join(unknown)
            )
        topics_set = set(requested)
        if not 1 <= int(limit) <= 100:
            raise MemoryCoreError("memory query limit must be between 1 and 100")

        rings = self._verified_rings()
        by_index = {ring.get("index"): ring for ring in rings}
        exclusions: list[dict[str, Any]] = []
        analyses: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for ring in rings:
            if ring.get("ring_type") not in SUPPORTED_ANALYSIS_RING_TYPES:
                continue
            reference = self._analysis_reference(ring, exclusions)
            if reference is None:
                continue
            ref_subject = _normalize_subject(
                str(reference.get("network") or ""),
                str(reference.get("subject") or ""),
            )
            if reference.get("network") == network and ref_subject == subject:
                analyses.append((ring, reference))
        analyses.sort(key=lambda pair: int(pair[0]["index"]))

        claims: list[dict[str, Any]] = []
        selected = analyses[-int(limit):]
        if "latest_assessment" in topics_set and analyses:
            ring, reference = analyses[-1]
            value = _analysis_value(ring)
            claims.append(
                _claim(
                    "latest_assessment",
                    f"Latest verified assessment: {value.get('risk_level') or 'Unknown'} risk.",
                    value,
                    str((ring.get("payload") or {}).get("timestamp") or ring.get("timestamp") or ""),
                    [self._analysis_citation(ring, reference, "analysis.assessment")],
                )
            )
        if "risk_history" in topics_set:
            for ring, reference in selected:
                value = _analysis_value(ring)
                claims.append(
                    _claim(
                        "risk_history",
                        f"Risk was {value.get('risk_level') or 'Unknown'} at the cited chain anchor.",
                        value,
                        str((ring.get("payload") or {}).get("timestamp") or ring.get("timestamp") or ""),
                        [self._analysis_citation(ring, reference, "analysis.risk_history")],
                    )
                )

        if "entity_history" in topics_set and analyses:
            projection = build_temporal_projection(rings)
            view = subject_temporal_view(
                projection,
                network,
                subject,
                score_limit=int(limit),
                event_limit=int(limit),
            )
            for event in (view.get("relationship_events") or [])[-int(limit):]:
                ring_ref = event.get("analysis_ring") or {}
                ring = by_index.get(ring_ref.get("index"))
                if ring is None:
                    exclusions.append(
                        {"reason": "relationship_event_ring_missing", "ring": ring_ref.get("index")}
                    )
                    continue
                reference = self._analysis_reference(ring, exclusions)
                if reference is None:
                    continue
                value = {
                    key: event.get(key)
                    for key in (
                        "event", "relationship_id", "relationship", "source",
                        "target", "state", "evidence_status", "confidence",
                    )
                    if event.get(key) is not None
                }
                claims.append(
                    _claim(
                        "entity_history",
                        f"Entity relationship event: {event.get('event') or 'observed'}.",
                        value,
                        event.get("observed_at"),
                        [self._analysis_citation(ring, reference, "entity_graph.relationship_event")],
                    )
                )

        if "outcomes" in topics_set:
            for ring in rings:
                record = (ring.get("payload") or {}).get("outcome_record")
                if not isinstance(record, dict):
                    continue
                reference = record.get("analysis_reference") or {}
                if reference.get("network") != network:
                    continue
                ref_subject = _normalize_subject(
                    network, str(reference.get("subject") or "")
                )
                if ref_subject != subject:
                    continue
                analysis_ring = by_index.get(reference.get("ring"))
                ok, reason = verify_outcome_record(record, analysis_ring)
                if not ok:
                    exclusions.append(
                        {"ring": ring.get("index"), "reason": "invalid_outcome", "detail": reason}
                    )
                    continue
                if not (record.get("learning") or {}).get("eligible"):
                    exclusions.append(
                        {"ring": ring.get("index"), "reason": "outcome_evidence_incomplete"}
                    )
                    continue
                outcome_evidence = record.get("outcome_evidence") or {}
                manifest = outcome_evidence.get("manifest") or {}
                pin = manifest.get("pin") or {}
                analysis_reference = analysis_reference_from_ring(analysis_ring)
                citations = [
                    _citation(
                        ring=ring,
                        evidence_hash=outcome_evidence["evidence_hash"],
                        evidence_kind="outcome_evidence_manifest",
                        anchor_type=pin.get("type"),
                        anchor_value=_safe_int(pin.get("value")),
                        claim_path="outcome.observation",
                        binding_state="sealed_outcome_record",
                        reference_hash=record.get("record_hash"),
                    ),
                    self._analysis_citation(
                        analysis_ring,
                        analysis_reference,
                        "outcome.original_analysis",
                    ),
                ]
                value = {
                    "security": record.get("security_outcomes") or {},
                    "market": record.get("market_outcomes") or {},
                    "infrastructure": record.get("infrastructure_outcomes") or {},
                    "other": record.get("other_outcomes") or {},
                    "outcome_id": record.get("outcome_id"),
                }
                claims.append(
                    _claim(
                        "outcomes",
                        "Verified outcome linked to the exact original analysis and evidence hash.",
                        value,
                        record.get("observed_at"),
                        citations,
                    )
                )

        claims.sort(key=lambda item: (str(item.get("observed_at") or ""), item["claim_id"]))
        if len(claims) > int(limit) * max(1, len(topics_set)):
            claims = claims[-int(limit) * max(1, len(topics_set)):]
        citation_count = sum(len(item["citations"]) for item in claims)
        response = {
            "schema_version": MEMORY_SCHEMA_VERSION,
            "subject": {"network": network, "address": subject},
            "topics": sorted(topics_set),
            "claims": claims,
            "exclusions": exclusions[-100:],
            "integrity": {
                "timechain_verified": True,
                "citation_coverage_pct": 100.0,
                "claim_count": len(claims),
                "citation_count": citation_count,
                "uncited_claim_count": 0,
                "policy": "no factual claim without an exact verified citation",
            },
            "source_chain": {
                "head_index": rings[-1].get("index") if rings else None,
                "head_hash": rings[-1].get("ring_hash") if rings else None,
                "ring_count": len(rings),
            },
            "limitations": [
                "Legacy analyses without an evidence manifest sealed in the original ring are excluded.",
                "Results describe cited historical anchors, not current chain state unless the latest cited scan is current.",
            ],
        }
        response["result_hash"] = canonical_hash(response)
        ok, reason = self.verify_result(response, rings=rings)
        if not ok:
            raise MemoryCoreError(f"recall result failed self-verification: {reason}")
        return response

    def verify_result(
        self,
        result: dict[str, Any],
        *,
        rings: Iterable[dict[str, Any]] | None = None,
    ) -> tuple[bool, str]:
        try:
            supplied = str(result.get("result_hash") or "").lower()
            unsigned = {key: value for key, value in result.items() if key != "result_hash"}
            if not HASH_RE.fullmatch(supplied) or canonical_hash(unsigned) != supplied:
                raise MemoryCoreError("result hash mismatch")
            ring_list = list(rings) if rings is not None else self._verified_rings()
            by_index = {ring.get("index"): ring for ring in ring_list}
            for claim in result.get("claims") or []:
                claim_hash = str(claim.get("claim_hash") or "").lower()
                bare_claim = {key: value for key, value in claim.items() if key != "claim_hash"}
                if not HASH_RE.fullmatch(claim_hash) or canonical_hash(bare_claim) != claim_hash:
                    raise MemoryCoreError("claim hash mismatch")
                citations = claim.get("citations") or []
                if not citations:
                    raise MemoryCoreError("uncited claim")
                for citation in citations:
                    citation_hash = str(citation.get("citation_hash") or "").lower()
                    bare = {key: value for key, value in citation.items() if key != "citation_hash"}
                    if not HASH_RE.fullmatch(citation_hash) or canonical_hash(bare) != citation_hash:
                        raise MemoryCoreError("citation hash mismatch")
                    ring = by_index.get(citation.get("ring"))
                    if ring is None or ring.get("ring_hash") != citation.get("ring_hash"):
                        raise MemoryCoreError("citation ring mismatch")
                    evidence_kind = citation.get("evidence_kind")
                    if evidence_kind == "analysis_evidence_manifest":
                        ref = analysis_reference_from_ring(ring)
                        if (
                            ref.get("binding_state") != "sealed_at_analysis"
                            or ref.get("original_evidence_hash") != citation.get("evidence_hash")
                        ):
                            raise MemoryCoreError("analysis evidence citation mismatch")
                    elif evidence_kind == "outcome_evidence_manifest":
                        record = (ring.get("payload") or {}).get("outcome_record") or {}
                        if (record.get("outcome_evidence") or {}).get("evidence_hash") != citation.get("evidence_hash"):
                            raise MemoryCoreError("outcome evidence citation mismatch")
                    else:
                        raise MemoryCoreError("unsupported citation evidence kind")
            return True, "verified"
        except (MemoryCoreError, TypeError, ValueError) as exc:
            return False, str(exc)

    def citation_proof(self, ring_index: int) -> dict[str, Any]:
        rings = self._verified_rings()
        ring = next((item for item in rings if item.get("index") == ring_index), None)
        if ring is None:
            raise KeyError("citation ring not found")
        if ring.get("ring_type") in SUPPORTED_ANALYSIS_RING_TYPES:
            reference = analysis_reference_from_ring(ring)
            proof = {
                "schema_version": MEMORY_SCHEMA_VERSION,
                "kind": "analysis",
                "ring": reference["ring"],
                "ring_hash": reference["ring_hash"],
                "ring_type": reference["ring_type"],
                "ring_timestamp": reference["ring_timestamp"],
                "network": reference["network"],
                "subject": reference["subject"],
                "evidence_hash": reference["original_evidence_hash"],
                "evidence_complete": reference["evidence_complete"],
                "binding_state": reference["binding_state"],
                "anchor": {
                    "type": reference["anchor_type"],
                    "value": reference["anchor_value"],
                },
                "reference_hash": reference["reference_hash"],
            }
        else:
            record = (ring.get("payload") or {}).get("outcome_record")
            if not isinstance(record, dict):
                raise KeyError("ring is not a public memory citation")
            analysis_ring = next(
                (
                    item
                    for item in rings
                    if item.get("index")
                    == (record.get("analysis_reference") or {}).get("ring")
                ),
                None,
            )
            ok, reason = verify_outcome_record(record, analysis_ring)
            if not ok:
                raise MemoryCoreError(f"outcome citation is invalid: {reason}")
            proof = {
                "schema_version": MEMORY_SCHEMA_VERSION,
                "kind": "outcome",
                "ring": ring["index"],
                "ring_hash": ring["ring_hash"],
                "ring_type": ring["ring_type"],
                "ring_timestamp": ring.get("timestamp"),
                "outcome_id": record.get("outcome_id"),
                "record_hash": record.get("record_hash"),
                "outcome_evidence_hash": (record.get("outcome_evidence") or {}).get("evidence_hash"),
                "analysis_reference": record.get("analysis_reference"),
                "learning_eligible": (record.get("learning") or {}).get("eligible"),
            }
        proof["proof_hash"] = canonical_hash(proof)
        return proof


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _load_module(skill_dir: Path, name: str):
    if str(skill_dir) not in sys.path:
        sys.path.insert(0, str(skill_dir))
    spec = importlib.util.spec_from_file_location(name, skill_dir / f"{name}.py")
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {name} from {skill_dir}")
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


class MemoryRecoveryManager:
    """Create and exercise verified snapshots without touching the live root."""

    def __init__(
        self,
        chain_root: str | Path,
        backup_root: str | Path,
        *,
        tc: Any | None = None,
        timechain_module: Any | None = None,
        epochs_module: Any | None = None,
    ):
        self.chain_root = Path(chain_root).resolve()
        self.backup_root = Path(backup_root).resolve()
        if self.backup_root == self.chain_root or self.chain_root in self.backup_root.parents:
            raise MemoryCoreError("backup root must be outside the live Timechain root")
        if timechain_module is None or epochs_module is None:
            skill = _skill_dir()
            timechain_module = timechain_module or _load_module(skill, "timechain")
            epochs_module = epochs_module or _load_module(skill, "epochs")
        self.timechain_module = timechain_module
        self.epochs_module = epochs_module
        self.tc = tc or timechain_module.Timechain(self.chain_root)

    @staticmethod
    def _copy_authoritative(source: Path, destination: Path) -> None:
        for relative in (Path("chain"), Path("registry")):
            src = source / relative
            if src.is_dir():
                shutil.copytree(src, destination / relative)

    @staticmethod
    def _manifest_files(root: Path) -> list[dict[str, Any]]:
        files = []
        for directory in (root / "chain", root / "registry"):
            if not directory.is_dir():
                continue
            for path in sorted(value for value in directory.rglob("*") if value.is_file()):
                files.append(
                    {
                        "path": path.relative_to(root).as_posix(),
                        "size": path.stat().st_size,
                        "sha256": _file_hash(path),
                    }
                )
        return files

    def create_backup(self) -> dict[str, Any]:
        self.backup_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=".chainseer-backup-", dir=self.backup_root))
        try:
            self._copy_authoritative(self.chain_root, staging)
            snapshot_tc = self.timechain_module.Timechain(staging)
            chain_ok, chain_report = snapshot_tc.verify()
            epoch_ok, epoch_report = self.epochs_module.check_epoch(staging)
            if not chain_ok or not epoch_ok:
                raise MemoryCoreError(
                    "snapshot integrity failed: "
                    + "; ".join(chain_report + epoch_report)
                )
            rings = list(snapshot_tc.iter_rings())
            manifest = {
                "schema_version": BACKUP_SCHEMA_VERSION,
                "created_at": _utc_now(),
                "source_chain": {
                    "head_index": rings[-1].get("index") if rings else None,
                    "head_hash": rings[-1].get("ring_hash") if rings else None,
                    "ring_count": len(rings),
                },
                "authoritative_paths": ["chain/", "registry/"],
                "files": self._manifest_files(staging),
            }
            manifest["manifest_hash"] = canonical_hash(manifest)
            (staging / "backup-manifest.json").write_text(
                json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
            )
            final = self.backup_root / (
                f"backup-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"
                f"{manifest['source_chain']['head_index']}-{manifest['manifest_hash'][:10]}"
            )
            if final.exists():
                raise MemoryCoreError("backup destination already exists")
            os.replace(staging, final)
            staging = None
            return {"path": str(final), **manifest}
        finally:
            if staging is not None and staging.exists():
                shutil.rmtree(staging)

    def verify_backup(self, backup: str | Path) -> tuple[bool, dict[str, Any]]:
        root = Path(backup).resolve()
        manifest_path = root / "backup-manifest.json"
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            supplied = str(manifest.get("manifest_hash") or "").lower()
            unsigned = {key: value for key, value in manifest.items() if key != "manifest_hash"}
            if not HASH_RE.fullmatch(supplied) or canonical_hash(unsigned) != supplied:
                raise MemoryCoreError("backup manifest hash mismatch")
            expected_paths = set()
            for item in manifest.get("files") or []:
                relative = Path(str(item.get("path") or ""))
                if relative.is_absolute() or ".." in relative.parts:
                    raise MemoryCoreError("unsafe backup manifest path")
                path = root / relative
                expected_paths.add(relative.as_posix())
                if (
                    not path.is_file()
                    or path.stat().st_size != item.get("size")
                    or _file_hash(path) != item.get("sha256")
                ):
                    raise MemoryCoreError(f"backup file mismatch: {relative.as_posix()}")
            actual_paths = {
                path.relative_to(root).as_posix()
                for directory in (root / "chain", root / "registry")
                if directory.is_dir()
                for path in directory.rglob("*")
                if path.is_file()
            }
            if actual_paths != expected_paths:
                raise MemoryCoreError("backup contains unmanifested or missing authoritative files")
            tc = self.timechain_module.Timechain(root)
            chain_ok, chain_report = tc.verify()
            epoch_ok, epoch_report = self.epochs_module.check_epoch(root)
            if not chain_ok or not epoch_ok:
                raise MemoryCoreError("; ".join(chain_report + epoch_report))
            return True, {
                "manifest_hash": supplied,
                "source_chain": manifest.get("source_chain"),
                "file_count": len(expected_paths),
                "chain_report": chain_report,
                "epoch_report": epoch_report,
            }
        except (OSError, json.JSONDecodeError, MemoryCoreError, TypeError, ValueError) as exc:
            return False, {"error": str(exc)}

    def restore(self, backup: str | Path, destination: str | Path) -> dict[str, Any]:
        backup = Path(backup).resolve()
        destination = Path(destination).resolve()
        if destination == self.chain_root or self.chain_root in destination.parents:
            raise MemoryCoreError("recovery restore must never target the live Timechain root")
        if destination.exists() and any(destination.iterdir()):
            raise MemoryCoreError("restore destination must be new or empty")
        ok, report = self.verify_backup(backup)
        if not ok:
            raise MemoryCoreError(f"refusing invalid backup: {report.get('error')}")
        destination.mkdir(parents=True, exist_ok=True)
        self._copy_authoritative(backup, destination)
        restored_tc = self.timechain_module.Timechain(destination)
        chain_ok, chain_report = restored_tc.verify()
        epoch_ok, epoch_report = self.epochs_module.check_epoch(destination)
        governance_ok, governance_report = verify_governance_registry(destination)
        if not chain_ok or not epoch_ok or not governance_ok:
            raise MemoryCoreError(
                "restored state failed verification: "
                + "; ".join(chain_report + epoch_report + governance_report)
            )
        return {
            "destination": str(destination),
            "manifest_hash": report["manifest_hash"],
            "chain_verified": True,
            "epoch_verified": True,
            "governance_verified": True,
        }

    def drill(self, *, retain: bool = False) -> dict[str, Any]:
        started = time.perf_counter()
        backup = self.create_backup()
        drill_parent = Path(tempfile.mkdtemp(prefix="chainseer-recovery-drill-"))
        restored = drill_parent / "restored"
        try:
            restore = self.restore(backup["path"], restored)
            restored_tc = self.timechain_module.Timechain(restored)
            rings = list(restored_tc.iter_rings())
            outcome = verify_outcome_rings(rings)
            projection = build_temporal_projection(rings)
            projection_ok, projection_reason = verify_temporal_projection(
                projection, rings
            )
            live_rings = list(self.tc.iter_rings())
            snapshot_head = backup["source_chain"].get("head_index")
            live_snapshot = [
                ring
                for ring in live_rings
                if int(ring.get("index", -1)) <= int(snapshot_head)
            ]
            live_projection = build_temporal_projection(live_snapshot)
            deterministic = (
                projection.get("projection_hash")
                == live_projection.get("projection_hash")
            )
            sample = next(
                (
                    analysis_reference_from_ring(ring)
                    for ring in reversed(rings)
                    if ring.get("ring_type") in SUPPORTED_ANALYSIS_RING_TYPES
                    and (ring.get("payload") or {}).get("evidence_manifest")
                ),
                None,
            )
            recall = None
            if sample:
                recall = MemoryRecallEngine(restored_tc, restored).query(
                    sample["network"],
                    sample["subject"],
                    topics=["latest_assessment"],
                    limit=1,
                )
            success = bool(
                outcome.get("ok")
                and projection_ok
                and deterministic
                and restore.get("chain_verified")
                and (
                    recall is None
                    or (recall.get("integrity") or {}).get("citation_coverage_pct")
                    == 100.0
                )
            )
            duration = round(time.perf_counter() - started, 3)
            result = {
                "schema_version": MEMORY_SCHEMA_VERSION,
                "status": "passed" if success else "failed",
                "completed_at": _utc_now(),
                "backup": {
                    "path": backup["path"],
                    "manifest_hash": backup["manifest_hash"],
                    "file_count": len(backup["files"]),
                },
                "source_chain": backup["source_chain"],
                "recovery": {
                    "rpo_rings": 0,
                    "rto_seconds": duration,
                    "restored_chain_verified": bool(restore.get("chain_verified")),
                    "registry_epoch_verified": bool(restore.get("epoch_verified")),
                    "governance_verified": bool(restore.get("governance_verified")),
                    "outcome_ledger_verified": bool(outcome.get("ok")),
                    "outcome_records": outcome.get("checked", 0),
                    "temporal_projection_verified": projection_ok,
                    "temporal_projection_reason": projection_reason,
                    "temporal_projection_hash": projection.get("projection_hash"),
                    "deterministic_rebuild": deterministic,
                    "recall_probe": {
                        "available": recall is not None,
                        "claim_count": len((recall or {}).get("claims") or []),
                        "citation_coverage_pct": ((recall or {}).get("integrity") or {}).get("citation_coverage_pct"),
                    },
                },
                "live_root_modified": False,
            }
            result["drill_hash"] = canonical_hash(result)
            self._write_status(result)
            if not success:
                raise MemoryCoreError("recovery drill did not satisfy every integrity gate")
            return result
        finally:
            if not retain and drill_parent.exists():
                shutil.rmtree(drill_parent)

    def _write_status(self, value: dict[str, Any]) -> None:
        path = self.chain_root / RECOVERY_STATUS_FILENAME
        temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True), encoding="utf-8"
        )
        os.replace(temporary, path)


class MemoryCore:
    """Application façade for recall, proofs, status, and recovery drills."""

    def __init__(
        self,
        tc: Any,
        chain_root: str | Path,
        *,
        backup_root: str | Path | None = None,
        epochs_module: Any | None = None,
        timechain_module: Any | None = None,
    ):
        self.tc = tc
        self.chain_root = Path(chain_root)
        self.recall = MemoryRecallEngine(tc, chain_root)
        self.backup_root = Path(
            backup_root or self.chain_root.parent / "chainseer_backups"
        )
        self._epochs_module = epochs_module
        self._timechain_module = timechain_module
        self._status_cache: tuple[float, tuple[int, int], dict[str, Any]] | None = None
        self._lock = threading.RLock()

    def query(self, *args, **kwargs) -> dict[str, Any]:
        return self.recall.query(*args, **kwargs)

    def citation(self, ring_index: int) -> dict[str, Any]:
        return self.recall.citation_proof(ring_index)

    def status(self, *, cache_seconds: int = 30) -> dict[str, Any]:
        with self._lock:
            signature = self.recall._signature()
            now = time.monotonic()
            if (
                self._status_cache
                and self._status_cache[1] == signature
                and now - self._status_cache[0] <= cache_seconds
            ):
                return json.loads(json.dumps(self._status_cache[2]))
            rings = self.recall._verified_rings()
            chain_ok, chain_report = self.tc.verify()
            if self._epochs_module is None:
                self._epochs_module = _load_module(_skill_dir(), "epochs")
            epoch_ok, epoch_report = self._epochs_module.check_epoch(self.chain_root)
            outcomes = verify_outcome_rings(rings)
            rebuilt = build_temporal_projection(rings)
            projection_path = self.chain_root / "temporal_entity_graph-v1.json"
            projection_present = projection_path.is_file()
            projection_ok = False
            projection_reason = "projection_missing"
            if projection_present:
                try:
                    persisted = json.loads(projection_path.read_text(encoding="utf-8"))
                    # rings=None skips verify_temporal_projection's own internal
                    # rebuild-and-compare -- `rebuilt` above already is that rebuild,
                    # so drift is checked against it directly instead of paying for
                    # a second full build_temporal_projection pass over every ring.
                    projection_ok, projection_reason = verify_temporal_projection(
                        persisted, rings=None
                    )
                    if projection_ok and rebuilt.get("projection_hash") != persisted.get(
                        "projection_hash"
                    ):
                        projection_ok, projection_reason = False, "projection_drift"
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    projection_reason = "projection_unreadable"
            governance_ok, governance_report = verify_governance_registry(
                self.chain_root
            )
            grown = {}
            try:
                grown = json.loads(
                    (self.chain_root / "registry" / "grown.json").read_text(
                        encoding="utf-8"
                    )
                )
            except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                pass
            governance = grown.get("chainseer_governance") or {}
            patterns: dict[str, int] = {}
            for record in (governance.get("patterns") or {}).values():
                state = str(record.get("state") or "unknown")
                patterns[state] = patterns.get(state, 0) + 1
            recovery = None
            status_path = self.chain_root / RECOVERY_STATUS_FILENAME
            if status_path.is_file():
                try:
                    recovery = json.loads(status_path.read_text(encoding="utf-8"))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    recovery = {"status": "unreadable"}
            current_head = rings[-1].get("index") if rings else None
            drill_head = (
                ((recovery or {}).get("source_chain") or {}).get("head_index")
                if isinstance(recovery, dict)
                else None
            )
            rings_behind = (
                max(0, int(current_head) - int(drill_head))
                if current_head is not None and drill_head is not None
                else None
            )
            recovery_freshness = {
                "current_head_index": current_head,
                "drill_head_index": drill_head,
                "rings_behind": rings_behind,
                "current_head_match": rings_behind == 0 if rings_behind is not None else False,
            }
            recall_candidates = []
            analysis_ring_count = 0
            for ring in rings:
                if ring.get("ring_type") not in SUPPORTED_ANALYSIS_RING_TYPES:
                    continue
                analysis_ring_count += 1
                try:
                    reference = analysis_reference_from_ring(ring)
                except (TypeError, ValueError):
                    continue
                if (
                    reference.get("binding_state") == "sealed_at_analysis"
                    and reference.get("evidence_complete")
                ):
                    recall_candidates.append(reference)
            if recall_candidates:
                probe_reference = recall_candidates[-1]
                probe = self.recall.query(
                    probe_reference["network"],
                    probe_reference["subject"],
                    topics=["latest_assessment"],
                    limit=1,
                )
                recall_probe = {
                    "status": "verified",
                    "available": True,
                    "claim_count": len(probe.get("claims") or []),
                    "citation_coverage_pct": (probe.get("integrity") or {}).get(
                        "citation_coverage_pct"
                    ),
                    "result_hash": probe.get("result_hash"),
                }
            else:
                recall_probe = {
                    "status": "ready_no_evidence_bound_analyses",
                    "available": False,
                    "claim_count": 0,
                    "citation_coverage_pct": None,
                    "result_hash": None,
                }
            source = rebuilt.get("source_chain") or {}
            learning_freshness = self._outcome_freshness(outcomes)
            response = {
                "schema_version": MEMORY_SCHEMA_VERSION,
                "status": (
                    "healthy"
                    if all((
                        chain_ok, epoch_ok, outcomes.get("ok"),
                        projection_ok, governance_ok,
                    )) and learning_freshness["state"] != "stalled"
                    else "degraded"
                ),
                "checked_at": _utc_now(),
                "pillars": {
                    "timechain_ledger": {
                        "ok": chain_ok and epoch_ok,
                        "ring_count": len(rings),
                        "head_index": rings[-1].get("index") if rings else None,
                        "head_hash": rings[-1].get("ring_hash") if rings else None,
                        "chain_report": chain_report[-1:] if chain_report else [],
                        "epoch_report": epoch_report,
                    },
                    "entity_knowledge_graph": {
                        "ok": projection_ok,
                        "projection_present": projection_present,
                        "projection_reason": projection_reason,
                        "projection_hash": rebuilt.get("projection_hash"),
                        "subject_count": (rebuilt.get("summary") or {}).get("subject_count", 0),
                        "analysis_ring_count": source.get("analysis_ring_count", 0),
                        "projection_head_index": source.get("head_index"),
                    },
                    "pattern_faculty_store": {
                        "ok": governance_ok,
                        "tighten_only": True,
                        "faculty_count": len(governance.get("faculties") or {}),
                        "pattern_count": len(governance.get("patterns") or {}),
                        "pattern_states": patterns,
                        "report": governance_report,
                    },
                    "outcome_ledger": {
                        "ok": outcomes.get("ok"),
                        "canonical_records": outcomes.get("checked", 0),
                        "learning_eligible": outcomes.get("learning_eligible", 0),
                        "legacy_unbound": outcomes.get("legacy_unbound", 0),
                        "invalid_records": len(outcomes.get("errors") or []),
                        "freshness": learning_freshness,
                    },
                    "query_recall_engine": {
                        "ok": chain_ok,
                        "subject_scoped": True,
                        "citation_policy": "100% exact ring/evidence citations",
                        "legacy_unbound_claims_excluded": True,
                        "analysis_ring_count": analysis_ring_count,
                        "evidence_bound_analysis_count": len(recall_candidates),
                        "recall_probe": recall_probe,
                        "raw_ring_payloads_public": False,
                    },
                },
                "recovery": {
                    "backup_root_configured": bool(self.backup_root),
                    "last_drill": recovery,
                    "last_drill_freshness": recovery_freshness,
                    "policy": "restore only to new/empty isolated roots; never overwrite live Timechain",
                },
                "execution": {
                    "signing": False,
                    "broadcast": False,
                    "live_capital": False,
                },
            }
            response["status_hash"] = canonical_hash(response)
            self._status_cache = (now, signature, response)
            return json.loads(json.dumps(response))

    @staticmethod
    def _outcome_freshness(
        outcomes: dict[str, Any], *, now: float | None = None
    ) -> dict[str, Any]:
        """Report whether outcome production has silently stopped.

        Three distinct states, deliberately not collapsed into one boolean:
        ``no_outcomes_yet`` (an empty corpus, which is not a fault),
        ``current`` (producing), and ``stalled`` (produced before, then went
        quiet past the threshold). Only ``stalled`` is a fault -- a system
        that has never recorded an outcome is empty, not broken, and saying
        otherwise would fail a fresh install for the wrong reason.
        """
        latest = outcomes.get("latest_outcome_at")
        result: dict[str, Any] = {
            "latest_outcome_at": latest,
            "latest_outcome_ring": outcomes.get("latest_outcome_ring"),
            "age_seconds": None,
            "stale_after_seconds": STALE_OUTCOME_SECONDS,
            "state": "no_outcomes_yet",
        }
        if not latest:
            return result
        reference = (
            datetime.fromtimestamp(now, tz=timezone.utc)
            if now is not None
            else datetime.now(timezone.utc)
        )
        try:
            observed = datetime.fromisoformat(str(latest))
        except ValueError:
            result["state"] = "unparsable_timestamp"
            return result
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=timezone.utc)
        age = max(0.0, (reference - observed).total_seconds())
        result["age_seconds"] = round(age, 3)
        result["state"] = (
            "stalled" if age > STALE_OUTCOME_SECONDS else "current"
        )
        return result

    def recovery_manager(self) -> MemoryRecoveryManager:
        return MemoryRecoveryManager(
            self.chain_root,
            self.backup_root,
            tc=self.tc,
            timechain_module=self._timechain_module,
            epochs_module=self._epochs_module,
        )


def _runtime(root: Path, backup_root: Path | None = None) -> MemoryCore:
    skill = _skill_dir()
    timechain_module = _load_module(skill, "timechain")
    epochs_module = _load_module(skill, "epochs")
    tc = timechain_module.Timechain(root)
    return MemoryCore(
        tc,
        root,
        backup_root=backup_root,
        timechain_module=timechain_module,
        epochs_module=epochs_module,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--root", default=str(Path(__file__).resolve().parent / "chainseer_chain")
    )
    parser.add_argument("--backup-root")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("status")
    query = commands.add_parser("query")
    query.add_argument("network", choices=sorted(SUPPORTED_NETWORKS))
    query.add_argument("subject")
    query.add_argument("--topic", action="append", choices=sorted(ALLOWED_TOPICS))
    query.add_argument("--limit", type=int, default=20)
    citation = commands.add_parser("citation")
    citation.add_argument("ring", type=int)
    commands.add_parser("backup")
    verify = commands.add_parser("verify-backup")
    verify.add_argument("path")
    restore = commands.add_parser("restore")
    restore.add_argument("path")
    restore.add_argument("destination")
    drill = commands.add_parser("drill")
    drill.add_argument("--retain", action="store_true")
    args = parser.parse_args()
    core = _runtime(
        Path(args.root), Path(args.backup_root) if args.backup_root else None
    )
    if args.command == "status":
        result = core.status(cache_seconds=0)
    elif args.command == "query":
        result = core.query(
            args.network, args.subject, topics=args.topic, limit=args.limit
        )
    elif args.command == "citation":
        result = core.citation(args.ring)
    else:
        manager = core.recovery_manager()
        if args.command == "backup":
            result = manager.create_backup()
        elif args.command == "verify-backup":
            ok, result = manager.verify_backup(args.path)
            result = {"ok": ok, **result}
        elif args.command == "restore":
            result = manager.restore(args.path, args.destination)
        else:
            result = manager.drill(retain=args.retain)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
