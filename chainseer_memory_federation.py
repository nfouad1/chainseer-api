"""Verified, append-only federation of independent Chainseer Timechains.

Producer Timechains remain authoritative.  The federation stores only
sanitized, provenance-complete projections plus exact source Ring hashes.  It
never copies raw provider responses, signs transactions, or treats its derived
index as a source of truth.
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from chainseer_memory import _analysis_value, _load_module, _skill_dir
from chainseer_outcome_ledger import (
    HASH_RE,
    SUPPORTED_ANALYSIS_RING_TYPES,
    analysis_reference_from_ring,
    canonical_hash,
    canonical_outcome_rings,
    verify_outcome_record,
)


FEDERATION_SCHEMA_VERSION = "1.0"
FEDERATION_RING_TYPE = "federated_memory_ingest"
DEFAULT_BATCH_SIZE = 250
SOURCE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")


class MemoryFederationError(ValueError):
    """A source, projection, citation, or extension invariant failed."""


@dataclass(frozen=True)
class MemorySource:
    source_id: str
    chain_root: Path
    tc: Any


def _without_hash(value: dict[str, Any], field: str) -> dict[str, Any]:
    return {key: item for key, item in value.items() if key != field}


def _normalize_subject(network: str, subject: Any) -> str:
    value = str(subject or "").strip()
    if not value:
        raise MemoryFederationError("federated memory subject is required")
    return value if str(network).lower() == "solana" else value.lower()


class FederatedMemoryCore:
    """Ingest and recall verified projections from independent producers."""

    def __init__(
        self,
        federation_tc: Any,
        federation_root: str | Path,
        sources: Iterable[MemorySource],
    ):
        self.tc = federation_tc
        self.root = Path(federation_root)
        self.sources: dict[str, MemorySource] = {}
        for source in sources:
            source_id = str(source.source_id or "").strip().lower()
            if not SOURCE_ID_RE.fullmatch(source_id):
                raise MemoryFederationError("invalid federation source id")
            if source_id in self.sources:
                raise MemoryFederationError("duplicate federation source id")
            if Path(source.chain_root).resolve() == self.root.resolve():
                raise MemoryFederationError(
                    "federation root cannot also be a producer root")
            self.sources[source_id] = MemorySource(
                source_id, Path(source.chain_root), source.tc)

    def initialize(self) -> dict[str, Any]:
        rings = list(self.tc.iter_rings())
        if rings:
            return rings[0]
        return self.tc.genesis(name="Chainseer Federated Verified Memory Core")

    @staticmethod
    def _verify_timechain(tc: Any, label: str) -> list[dict[str, Any]]:
        ok, report = tc.verify()
        if not ok:
            raise MemoryFederationError(
                f"{label} Timechain verification failed: "
                + "; ".join(str(item) for item in report)
            )
        return list(tc.iter_rings())

    def _federation_entries(
        self,
    ) -> tuple[list[dict[str, Any]], list[tuple[dict, dict]]]:
        rings = self._verify_timechain(self.tc, "federation")
        entries: list[tuple[dict, dict]] = []
        for ring in rings:
            if ring.get("ring_type") != FEDERATION_RING_TYPE:
                continue
            payload = ring.get("payload") or {}
            if payload.get("schema_version") != FEDERATION_SCHEMA_VERSION:
                raise MemoryFederationError("unsupported federation Ring schema")
            records = payload.get("records")
            snapshot = payload.get("source_snapshot")
            if not isinstance(records, list) or not isinstance(snapshot, dict):
                raise MemoryFederationError("invalid federation batch payload")
            expected = canonical_hash({
                "source_snapshot": snapshot,
                "records": records,
            })
            if expected != payload.get("batch_hash"):
                raise MemoryFederationError("federation batch hash mismatch")
            for record in records:
                if not isinstance(record, dict):
                    raise MemoryFederationError("federation record is not a mapping")
                record_hash = str(record.get("record_hash") or "").lower()
                if (
                    not HASH_RE.fullmatch(record_hash)
                    or canonical_hash(_without_hash(record, "record_hash"))
                        != record_hash
                ):
                    raise MemoryFederationError("federation record hash mismatch")
                entries.append((ring, record))
        return rings, entries

    @staticmethod
    def _source_snapshot(
        source_id: str, rings: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if not rings:
            raise MemoryFederationError("producer Timechain is empty")
        return {
            "source_id": source_id,
            "genesis_hash": str(rings[0].get("ring_hash") or ""),
            "head_index": int(rings[-1].get("index") or 0),
            "head_hash": str(rings[-1].get("ring_hash") or ""),
            "ring_count": len(rings),
        }

    @staticmethod
    def _assert_source_extension(
        source_id: str,
        rings: list[dict[str, Any]],
        entries: list[tuple[dict, dict]],
    ) -> None:
        snapshots = [
            (ring.get("payload") or {}).get("source_snapshot") or {}
            for ring, _record in entries
            if ((_record.get("source") or {}).get("source_id") == source_id)
        ]
        if not snapshots:
            return
        previous = snapshots[-1]
        index = int(previous.get("head_index", -1))
        if index < 0 or index >= len(rings):
            raise MemoryFederationError("producer history was truncated")
        if str(rings[index].get("ring_hash") or "") != str(
            previous.get("head_hash") or ""
        ):
            raise MemoryFederationError("producer history fork detected")
        if str(rings[0].get("ring_hash") or "") != str(
            previous.get("genesis_hash") or ""
        ):
            raise MemoryFederationError("producer genesis changed")

    @staticmethod
    def _analysis_record(
        source_id: str, ring: dict[str, Any],
    ) -> dict[str, Any] | None:
        reference = analysis_reference_from_ring(ring)
        if (
            reference.get("binding_state") != "sealed_at_analysis"
            or not reference.get("evidence_complete")
        ):
            return None
        network = str(reference.get("network") or "").lower()
        subject = _normalize_subject(network, reference.get("subject"))
        bare = {
            "schema_version": FEDERATION_SCHEMA_VERSION,
            "kind": "analysis",
            "network": network,
            "subject": subject,
            "observed_at": reference.get("ring_timestamp"),
            "source": {
                "source_id": source_id,
                "ring": reference.get("ring"),
                "ring_hash": reference.get("ring_hash"),
                "ring_type": reference.get("ring_type"),
            },
            "evidence": {
                "analysis_evidence_hash":
                    reference.get("original_evidence_hash"),
                "anchor_type": reference.get("anchor_type"),
                "anchor_value": reference.get("anchor_value"),
                "reference_hash": reference.get("reference_hash"),
            },
            "value": _analysis_value(ring),
            "training_eligible": True,
        }
        bare["record_id"] = f"fmem-{canonical_hash(bare)[:24]}"
        bare["record_hash"] = canonical_hash(bare)
        return bare

    @staticmethod
    def _outcome_record(
        source_id: str,
        ring: dict[str, Any],
        by_index: dict[int, dict[str, Any]],
    ) -> dict[str, Any] | None:
        outcome = (ring.get("payload") or {}).get("outcome_record")
        if not isinstance(outcome, dict):
            return None
        reference = outcome.get("analysis_reference") or {}
        analysis = by_index.get(reference.get("ring"))
        ok, reason = verify_outcome_record(outcome, analysis)
        if not ok:
            raise MemoryFederationError(f"invalid producer outcome: {reason}")
        analysis_ref = analysis_reference_from_ring(analysis)
        if (
            analysis_ref.get("binding_state") != "sealed_at_analysis"
            or not analysis_ref.get("evidence_complete")
        ):
            return None
        evidence = outcome.get("outcome_evidence") or {}
        manifest = evidence.get("manifest") or {}
        # A small historical cohort intentionally records "outcome not
        # observed" with no evidence manifest. The canonical ledger verifier
        # accepts those records only as learning-ineligible bookkeeping. They
        # are not factual outcome evidence and must not be laundered into the
        # federation or turn a valid producer history into an ingest failure.
        if not evidence.get("complete") or not manifest:
            if (outcome.get("learning") or {}).get("eligible"):
                raise MemoryFederationError(
                    "learning-eligible outcome has incomplete evidence")
            return None
        pin = manifest.get("pin") or {}
        evidence_hash = str(evidence.get("evidence_hash") or "").lower()
        if not HASH_RE.fullmatch(evidence_hash):
            raise MemoryFederationError("outcome evidence hash is incomplete")
        network = str(reference.get("network") or "").lower()
        subject = _normalize_subject(network, reference.get("subject"))
        bare = {
            "schema_version": FEDERATION_SCHEMA_VERSION,
            "kind": "outcome",
            "network": network,
            "subject": subject,
            "observed_at": outcome.get("observed_at"),
            "source": {
                "source_id": source_id,
                "ring": ring.get("index"),
                "ring_hash": ring.get("ring_hash"),
                "ring_type": ring.get("ring_type"),
                "analysis_ring": reference.get("ring"),
                "analysis_ring_hash": reference.get("ring_hash"),
            },
            "evidence": {
                "analysis_evidence_hash":
                    reference.get("original_evidence_hash"),
                "outcome_evidence_hash": evidence_hash,
                "anchor_type": pin.get("type"),
                "anchor_value": pin.get("value"),
                "outcome_record_hash": outcome.get("record_hash"),
            },
            "value": {
                "security": outcome.get("security_outcomes") or {},
                "market": outcome.get("market_outcomes") or {},
                "infrastructure":
                    outcome.get("infrastructure_outcomes") or {},
                "other": outcome.get("other_outcomes") or {},
                "outcome_id": outcome.get("outcome_id"),
            },
            "training_eligible": bool(
                (outcome.get("learning") or {}).get("eligible")),
            "training_exclusion_reason": (
                outcome.get("learning") or {}).get("reason"),
        }
        bare["record_id"] = f"fmem-{canonical_hash(bare)[:24]}"
        bare["record_hash"] = canonical_hash(bare)
        return bare

    def ingest(
        self,
        source_id: str,
        *,
        batch_size: int = DEFAULT_BATCH_SIZE,
        deadline_monotonic: float | None = None,
    ) -> dict:
        source_id = str(source_id or "").strip().lower()
        source = self.sources.get(source_id)
        if source is None:
            raise MemoryFederationError("unknown federation source")
        self.initialize()
        source_rings = self._verify_timechain(source.tc, source_id)
        federation_rings, existing_entries = self._federation_entries()
        self._assert_source_extension(source_id, source_rings, existing_entries)
        existing = {
            ((record.get("source") or {}).get("source_id"),
             (record.get("source") or {}).get("ring_hash"))
            for _ring, record in existing_entries
        }
        by_index = {ring.get("index"): ring for ring in source_rings}
        records: list[dict[str, Any]] = []
        exclusions = {
            "analysis_not_evidence_bound_at_seal": 0,
            "outcome_evidence_incomplete": 0,
        }
        for ring in canonical_outcome_rings(source_rings):
            projected = None
            if ring.get("ring_type") in SUPPORTED_ANALYSIS_RING_TYPES:
                projected = self._analysis_record(source_id, ring)
                if projected is None:
                    exclusions["analysis_not_evidence_bound_at_seal"] += 1
            elif isinstance(
                (ring.get("payload") or {}).get("outcome_record"), dict
            ):
                projected = self._outcome_record(source_id, ring, by_index)
                if projected is None:
                    exclusions["outcome_evidence_incomplete"] += 1
            if projected is None:
                continue
            key = (source_id, (projected.get("source") or {}).get("ring_hash"))
            if key not in existing:
                records.append(projected)
                existing.add(key)
        size = max(1, min(1_000, int(batch_size)))
        snapshot = self._source_snapshot(source_id, source_rings)
        sealed: list[dict[str, Any]] = []
        deferred = 0
        for offset in range(0, len(records), size):
            if (
                deadline_monotonic is not None
                and time.monotonic() >= float(deadline_monotonic)
            ):
                deferred = len(records) - offset
                break
            batch = records[offset:offset + size]
            payload = {
                "schema_version": FEDERATION_SCHEMA_VERSION,
                "source_snapshot": snapshot,
                "records": batch,
            }
            payload["batch_hash"] = canonical_hash({
                "source_snapshot": snapshot,
                "records": batch,
            })
            ring = self.tc.seal(FEDERATION_RING_TYPE, payload)
            sealed.append({
                "ring": ring.get("index"),
                "ring_hash": ring.get("ring_hash"),
                "records": len(batch),
            })
        counts: dict[str, int] = {}
        for record in records:
            counts[record["kind"]] = counts.get(record["kind"], 0) + 1
        return {
            "schema_version": FEDERATION_SCHEMA_VERSION,
            "source_id": source_id,
            "source_snapshot": snapshot,
            "records_ingested": len(records),
            "records_sealed": sum(item["records"] for item in sealed),
            "records_deferred": deferred,
            "records_by_kind": counts,
            "records_excluded": exclusions,
            "batches_sealed": len(sealed),
            "sealed_batches": sealed,
            "idempotent_noop": not records,
            "complete": deferred == 0,
            "federation_head_before": (
                federation_rings[-1].get("index")
                if federation_rings else None),
            "execution": {"signing": False, "broadcast": False},
        }

    def query(
        self, network: str, subject: str, *, limit: int = 20,
    ) -> dict[str, Any]:
        network = str(network or "").strip().lower()
        subject = _normalize_subject(network, subject)
        if not 1 <= int(limit) <= 100:
            raise MemoryFederationError("query limit must be between 1 and 100")
        federation_rings, entries = self._federation_entries()
        selected = [
            (ring, record) for ring, record in entries
            if record.get("network") == network
            and record.get("subject") == subject
        ][-int(limit):]
        verified_sources: dict[str, list[dict[str, Any]]] = {}
        claims: list[dict[str, Any]] = []
        for ingest_ring, record in selected:
            source_ref = record.get("source") or {}
            source_id = source_ref.get("source_id")
            source = self.sources.get(source_id)
            if source is None:
                raise MemoryFederationError("query source is not configured")
            rings = verified_sources.setdefault(
                source_id, self._verify_timechain(source.tc, source_id))
            index = int(source_ref.get("ring"))
            if index >= len(rings) or rings[index].get("ring_hash") != (
                source_ref.get("ring_hash")
            ):
                raise MemoryFederationError("source citation no longer verifies")
            citation = {
                "federation_ring": ingest_ring.get("index"),
                "federation_ring_hash": ingest_ring.get("ring_hash"),
                "source_id": source_id,
                "source_ring": index,
                "source_ring_hash": source_ref.get("ring_hash"),
                "source_ring_type": source_ref.get("ring_type"),
                "evidence": record.get("evidence"),
                "record_hash": record.get("record_hash"),
            }
            citation["citation_hash"] = canonical_hash(citation)
            claim = {
                "category": record.get("kind"),
                "observed_at": record.get("observed_at"),
                "value": record.get("value"),
                "training_eligible": record.get("training_eligible"),
                "citations": [citation],
            }
            claim["claim_id"] = f"fclaim-{canonical_hash(claim)[:24]}"
            claim["claim_hash"] = canonical_hash(claim)
            claims.append(claim)
        result = {
            "schema_version": FEDERATION_SCHEMA_VERSION,
            "subject": {"network": network, "address": subject},
            "claims": claims,
            "integrity": {
                "federation_timechain_verified": True,
                "source_timechains_verified": sorted(verified_sources),
                "citation_coverage_pct": 100.0,
                "uncited_claim_count": 0,
            },
            "federation_head": {
                "index": federation_rings[-1].get("index")
                    if federation_rings else None,
                "ring_hash": federation_rings[-1].get("ring_hash")
                    if federation_rings else None,
            },
            "execution": {"signing": False, "broadcast": False},
        }
        result["result_hash"] = canonical_hash(result)
        return result

    def status(self) -> dict[str, Any]:
        rings, entries = self._federation_entries()
        counts: dict[str, int] = {}
        sources: dict[str, int] = {}
        for _ring, record in entries:
            kind = str(record.get("kind") or "unknown")
            source_id = str((record.get("source") or {}).get("source_id"))
            counts[kind] = counts.get(kind, 0) + 1
            sources[source_id] = sources.get(source_id, 0) + 1
        return {
            "schema_version": FEDERATION_SCHEMA_VERSION,
            "status": "healthy",
            "federation_ring_count": len(rings),
            "federated_record_count": len(entries),
            "records_by_kind": counts,
            "records_by_source": sources,
            "head_index": rings[-1].get("index") if rings else None,
            "head_hash": rings[-1].get("ring_hash") if rings else None,
            "source_authority": "independent_verified_timechains",
            "execution": {
                "signing": False,
                "broadcast": False,
                "live_capital": False,
            },
        }


def _runtime(
    federation_root: Path, source_id: str, source_root: Path,
) -> FederatedMemoryCore:
    timechain = _load_module(_skill_dir(), "timechain")
    federation_tc = timechain.Timechain(federation_root)
    source_tc = timechain.Timechain(source_root)
    return FederatedMemoryCore(
        federation_tc,
        federation_root,
        [MemorySource(source_id, source_root, source_tc)],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default="chainseer_memory_federation")
    parser.add_argument("--source-id", default="robinhood-learning")
    parser.add_argument("--source-root", default="robinhood_learning_chain")
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest")
    ingest.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    query = commands.add_parser("query")
    query.add_argument("network")
    query.add_argument("subject")
    query.add_argument("--limit", type=int, default=20)
    commands.add_parser("status")
    args = parser.parse_args()
    core = _runtime(Path(args.root), args.source_id, Path(args.source_root))
    if args.command == "ingest":
        result = core.ingest(args.source_id, batch_size=args.batch_size)
    elif args.command == "query":
        result = core.query(args.network, args.subject, limit=args.limit)
    else:
        result = core.status()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
