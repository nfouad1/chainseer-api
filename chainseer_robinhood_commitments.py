"""Deferred Timechain-sealing architecture for the Robinhood paper learner.

Removes full Timechain sealing and full-chain verification from the live
decision/execution critical path while preserving provable pre-action
authorization. The boundary follows Timechain rings 4179/4180: a minimal,
durable decision commitment is written BEFORE any risk-increasing action;
full Timechain rings are created later by the analysis lane -- the single
authoritative writer -- and full-chain verification publishes a cached
integrity certificate that the fast path consumes.

Terminology: a token is never "risk-free". It is "buy-eligible under the
current evidence and policy".
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from chainseer_core import atomic_json_write, read_json, safe_float, safe_int

try:  # reuse the canonical serialization the outcome ledger already uses
    from chainseer_outcome_ledger import canonical_json
except ImportError:  # pragma: no cover - fallback keeps this module standalone
    def canonical_json(value) -> str:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=True,
            allow_nan=False, default=str,
        )


COMMITMENT_SCHEMA_VERSION = 1

#: A commitment authorizes action only inside this window.
DEFAULT_COMMITMENT_TTL_SECONDS = 120.0
#: Maximum permitted drift between the pinned evidence block/slot and the
#: head observed at execution time. Beyond this the evidence is stale.
DEFAULT_MAX_BLOCK_DRIFT_BLOCKS = 10
#: Cached integrity certificate bounds. Fail closed outside them.
INTEGRITY_CERTIFICATE_MAX_AGE_SECONDS = 15 * 60.0
INTEGRITY_CERTIFICATE_MAX_RING_LAG = 5
#: Conservative seal-debt limits. Exceeding them blocks NEW exposure; it must
#: never block protective exits. These thresholds are configuration, never
#: something the system loosens automatically.
SEAL_DEBT_MAX_PENDING = 5_000
SEAL_DEBT_MAX_OLDEST_PENDING_AGE_SECONDS = 6 * 60 * 60.0
SEAL_DEBT_MAX_DEAD_LETTER = 50
SEAL_DEBT_MAX_RETRYING = 500
SEAL_RETRY_MAX_ATTEMPTS = 8
SEAL_RETRY_BASE_BACKOFF_SECONDS = 30.0
SEAL_LEASE_SECONDS = 120.0
CERTIFICATE_FILE_NAME = "integrity_certificate.json"


class DecisionCommitmentError(RuntimeError):
    """A pre-effect commitment gate refused the action. Fail closed."""

    def __init__(self, reason: str, detail: str | None = None):
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


def canonical_commitment_payload(record: dict) -> dict:
    """Fields covered by the STABLE idempotency fingerprint.

    Volatile envelope fields (creation wall clock, expiry) are excluded:
    two commits of the SAME evidence under one idempotency key must carry
    the same fingerprint so a retry-after-crash deduplicates.
    """
    return {
        key: value for key, value in record.items()
        if key not in ("commitment_hash", "idempotency_fingerprint",
                       "created_at", "expires_at")
    }


def commitment_hash(record: dict) -> str:
    """Canonical hash over the given payload mapping."""
    return hashlib.sha256(
        canonical_json(record).encode("utf-8")
    ).hexdigest()


def _persisted_shape(record: dict) -> dict:
    """Normalize a commitment to its SQLite round-trip representation so
    create-time and load-time hashing see identical bytes."""
    return {
        key: (int(value) if isinstance(value, bool) else value)
        for key, value in record.items()
    }


def canonical_hard_stop_digest(hard_stops) -> str:
    """Digest of a hard-stop list that is invariant to ordering and to
    semantically identical representations (dicts vs their code strings)."""
    normalized = sorted(
        (item.get("code") or item.get("reason")) if isinstance(item, dict)
        else str(item)
        for item in (hard_stops or [])
    )
    return commitment_hash(normalized)


class RevalidationSnapshot:
    """Fresh evidence acquired AT the execution boundary.

    Every field is gathered now, from the current state -- never carried
    forward from the analysis that produced the commitment. If any field
    cannot be acquired the snapshot is invalid and authorization must
    refuse (fail closed).
    """

    def __init__(self, *, block: int, quote_hash: str,
                 hard_stop_digest: str, simulation_ok: bool,
                 producer_tail: dict, source: str):
        self.block = int(block)
        self.quote_hash = str(quote_hash)
        self.hard_stop_digest = str(hard_stop_digest)
        self.simulation_ok = bool(simulation_ok)
        self.producer_tail = producer_tail or {}
        self.source = source

    @property
    def valid(self) -> bool:
        return bool(self.block > 0 and self.quote_hash
                    and self.hard_stop_digest)

    def as_authorize_kwargs(self) -> dict:
        return {
            "current_block": self.block,
            "current_quote_hash": self.quote_hash,
            "current_hard_stop_digest": self.hard_stop_digest,
            "simulation_ok": self.simulation_ok,
        }


def _quote_projection(fresh_quote: dict, fields: tuple) -> dict:
    """Canonical quote projection: float-normalized so int/float type
    drift between data sources can never masquerade as a price change."""
    projected = {}
    for key in fields:
        value = fresh_quote.get(key)
        if value is None:
            continue
        projected[key] = float(value) if isinstance(value, (int, float)) \
            and not isinstance(value, bool) else value
    return projected


def acquire_revalidation_snapshot(
    *, rpc, market_client, candidate: dict, token_address: str,
    hard_stops: list, run_pre_trade_simulation, producer_chain_rings,
    quote_fields: tuple = ("price_usd", "liquidity_usd",
                           "market_cap_usd"),
    max_ring_lag: int = INTEGRITY_CERTIFICATE_MAX_RING_LAG,
    certificate_ring_count: int | None = None,
) -> RevalidationSnapshot:
    """Acquire FRESH revalidation evidence at the action boundary.

    - ``rpc.get_block_number()``: the real current head, not a stale pin.
    - ``market_client.snapshot(...)``: a fresh quote taken NOW, hashed
      over exactly ``quote_fields`` -- the same fields the commitment's
      original quote covered.
    - ``hard_stops`` recomputed from the candidate's CURRENT stored state.
    - ``run_pre_trade_simulation()`` callback result -- never assumed True.
    - Producer Timechain tail so certificate lag is measured against
      reality; if verification has fallen too far behind, the caller sees
      it and fails closed.

    Any failure produces an INVALID snapshot with ``valid=False``.
    """
    try:
        block = int(rpc.get_block_number())
    except Exception:
        return RevalidationSnapshot(
            block=0, quote_hash="", hard_stop_digest="",
            simulation_ok=False,
            producer_tail={}, source="rpc_error")
    try:
        fresh_quote = market_client.snapshot(
            token_address, candidate.get("pair_address"))
        projected = _quote_projection(fresh_quote, quote_fields)
        quote_hash = commitment_hash(projected)
    except Exception:
        return RevalidationSnapshot(
            block=block, quote_hash="", hard_stop_digest="",
            simulation_ok=False,
            producer_tail={}, source="quote_error")
    digest = canonical_hard_stop_digest(hard_stops)
    try:
        simulation_ok = bool(run_pre_trade_simulation(fresh_quote))
    except Exception:
        simulation_ok = False
    tail = {"ring_count": len(producer_chain_rings or [])}
    if certificate_ring_count is not None:
        tail["certificate_lag"] = max(
            0, tail["ring_count"] - int(certificate_ring_count))
        tail["lag_excessive"] = (
            tail["certificate_lag"] > max_ring_lag)
    return RevalidationSnapshot(
        block=block, quote_hash=quote_hash,
        hard_stop_digest=digest, simulation_ok=simulation_ok,
        producer_tail=tail, source="acquired")


def idempotency_fingerprint(record: dict) -> str:
    """Stable across retries of the same decision."""
    return commitment_hash(canonical_commitment_payload(record))


def complete_commitment_hash(record: dict) -> str:
    """Covers EVERY stored field including timestamps and expiry.

    Recomputed and verified whenever a commitment is loaded; any mutation
    of the persisted row -- a flipped decision, an extended expiry --
    breaks this hash and the commitment fails authorization.
    """
    payload = {
        key: value for key, value in record.items()
        if key != "commitment_hash"
    }
    return hashlib.sha256(
        canonical_json(payload).encode("utf-8")
    ).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class DecisionCommitmentStore:
    """Append-only, crash-safe store of pre-effect decision commitments.

    The commitment itself is immutable after creation; every later state
    change lands in ``commitment_events`` as a separate append-only row. The
    seal queue lives beside it so queue state survives forced termination
    (WAL mode + durable commits), and claiming uses short leases so a killed
    worker's jobs are recoverable on restart.
    """

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextmanager
    def connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connection() as connection:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS decision_commitments (
                    commitment_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    run_id TEXT NOT NULL,
                    network TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    evidence_hash TEXT NOT NULL,
                    evidence_block_pin INTEGER NOT NULL,
                    quote_hash TEXT NOT NULL,
                    quote_block INTEGER NOT NULL,
                    decision TEXT NOT NULL,
                    risk_score REAL,
                    hard_stop_digest TEXT NOT NULL,
                    policy_version TEXT NOT NULL,
                    faculty_registry_epoch TEXT NOT NULL,
                    previous_verified_head_index INTEGER,
                    previous_verified_head_hash TEXT,
                    simulation_ok INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    expires_at REAL NOT NULL,
                    idempotency_key TEXT NOT NULL UNIQUE,
                    commitment_hash TEXT NOT NULL,
                    idempotency_fingerprint TEXT,
                    executed_at REAL,
                    created_epoch REAL NOT NULL
                );
                """
            )
            self._migrate(connection)

    def _migrate(self, connection: sqlite3.Connection) -> None:
        """Backward-compatible column additions; never rewrites history."""
        columns = {
            row["name"] for row in connection.execute(
                "PRAGMA table_info(decision_commitments)")
        }
        if "idempotency_fingerprint" not in columns:
            connection.execute(
                "ALTER TABLE decision_commitments"
                " ADD COLUMN idempotency_fingerprint TEXT")
        if "executed_at" not in columns:
            connection.execute(
                "ALTER TABLE decision_commitments"
                " ADD COLUMN executed_at REAL")
        # Durable gate metrics: process-local counters would reset every
        # time the dashboard or a new worker built its own gate instance.
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS gate_metrics (
                metric TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        connection.executescript(
            """
                CREATE TABLE IF NOT EXISTS commitment_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    commitment_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    detail TEXT,
                    at_epoch REAL NOT NULL,
                    at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS commitment_events_by_commitment
                    ON commitment_events(commitment_id, event_id);
                CREATE TABLE IF NOT EXISTS verified_heads (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    head_index INTEGER NOT NULL,
                    head_hash TEXT NOT NULL,
                    chain_root TEXT NOT NULL,
                    registry_epoch TEXT NOT NULL,
                    ring_count INTEGER NOT NULL,
                    verification_result TEXT NOT NULL,
                    verifier_version TEXT NOT NULL,
                    published_at TEXT NOT NULL,
                    expires_at REAL NOT NULL
                );
                """
            )
        self._ensure_queue_table(connection)

    # ------------------------------------------------------------------ #
    # Commitment creation                                                 #
    # ------------------------------------------------------------------ #
    def create(self, spec: dict) -> dict:
        """Durably append one commitment. Idempotent per idempotency_key.

        Returns ``(record, created)`` semantics as a dict with
        ``duplicate`` set when an identical commitment already existed.
        Raises :class:`DecisionCommitmentError` on any failure -- callers
        must treat a failed write as "do not act".
        """
        required = (
            "run_id", "network", "token_address", "evidence_hash",
            "evidence_block_pin", "quote_hash", "quote_block", "decision",
            "hard_stop_digest", "policy_version", "faculty_registry_epoch",
            "idempotency_key",
        )
        missing = [key for key in required if not spec.get(key)]
        if missing:
            raise DecisionCommitmentError(
                "commitment_spec_incomplete",
                f"missing fields: {','.join(missing)}",
            )
        decision = str(spec["decision"]).upper()
        if decision not in {"BUY_ELIGIBLE", "REJECT"}:
            raise DecisionCommitmentError(
                "invalid_decision", f"decision={spec['decision']!r}")
        now_epoch = time.time()
        ttl = safe_float(
            spec.get("ttl_seconds"), DEFAULT_COMMITMENT_TTL_SECONDS)
        record = {
            "schema_version": COMMITMENT_SCHEMA_VERSION,
            "run_id": str(spec["run_id"]),
            "network": str(spec["network"]),
            "token_address": str(spec["token_address"]).lower(),
            "evidence_hash": str(spec["evidence_hash"]),
            "evidence_block_pin": int(spec["evidence_block_pin"]),
            "quote_hash": str(spec["quote_hash"]),
            "quote_block": int(spec["quote_block"]),
            "decision": decision,
            "risk_score": safe_float(spec.get("risk_score"), None),
            "hard_stop_digest": str(spec["hard_stop_digest"]),
            "policy_version": str(spec["policy_version"]),
            "faculty_registry_epoch": str(spec["faculty_registry_epoch"]),
            "previous_verified_head_index": safe_int(
                spec.get("previous_verified_head_index"), None),
            "previous_verified_head_hash": spec.get(
                "previous_verified_head_hash") or None,
            "simulation_ok": bool(spec.get("simulation_ok")),
            "created_at": _utc_now(),
            "expires_at": now_epoch + max(1.0, ttl),
            "idempotency_key": str(spec["idempotency_key"]),
        }
        record["idempotency_fingerprint"] = idempotency_fingerprint(record)
        # commitment_id derives from the STABLE fingerprint (not the
        # complete hash), so the complete hash can cover the id itself.
        commitment_id = "dc-" + hashlib.sha256(
            (record["idempotency_key"]
             + record["idempotency_fingerprint"]).encode()
        ).hexdigest()[:24]
        # The complete hash is computed over the FULL persisted shape:
        # commitment_id and created_epoch included -- so it can be
        # recomputed from any loaded row and catch any mutation (decision
        # flip, expiry extension). executed_at is EXCLUDED: the atomic
        # claim legitimately writes it after creation. Booleans are
        # normalized to ints to match their SQLite round-trip form.
        full = {
            **record,
            "commitment_id": commitment_id,
            "created_epoch": now_epoch,
        }
        full.pop("executed_at", None)
        record["commitment_hash"] = complete_commitment_hash(
            _persisted_shape(full))
        try:
            with self.connection() as connection:
                existing = connection.execute(
                    "SELECT * FROM decision_commitments"
                    " WHERE idempotency_key=?",
                    (record["idempotency_key"],),
                ).fetchone()
                if existing is not None:
                    prior = dict(existing)
                    # Compare the STABLE fingerprint, not the complete
                    # hash: the envelope (created_at/expires_at) differs on
                    # a legitimate retry, the evidence must not.
                    if prior.get("idempotency_fingerprint") != record[
                            "idempotency_fingerprint"]:
                        raise DecisionCommitmentError(
                            "idempotency_collision",
                            "same key committed with different evidence",
                        )
                    prior["duplicate"] = True
                    return prior
                connection.execute(
                    """INSERT INTO decision_commitments (
                       commitment_id,schema_version,run_id,network,
                       token_address,evidence_hash,evidence_block_pin,
                       quote_hash,quote_block,decision,risk_score,
                       hard_stop_digest,policy_version,faculty_registry_epoch,
                       previous_verified_head_index,
                       previous_verified_head_hash,simulation_ok,created_at,
                       expires_at,idempotency_key,commitment_hash,
                       idempotency_fingerprint,created_epoch)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        commitment_id, record["schema_version"],
                        record["run_id"], record["network"],
                        record["token_address"], record["evidence_hash"],
                        record["evidence_block_pin"], record["quote_hash"],
                        record["quote_block"], record["decision"],
                        record["risk_score"], record["hard_stop_digest"],
                        record["policy_version"],
                        record["faculty_registry_epoch"],
                        record["previous_verified_head_index"],
                        record["previous_verified_head_hash"],
                        int(record["simulation_ok"]), record["created_at"],
                        record["expires_at"], record["idempotency_key"],
                        record["commitment_hash"],
                        record["idempotency_fingerprint"], now_epoch,
                    ),
                )
                connection.execute(
                    "INSERT INTO commitment_events"
                    " (commitment_id,status,detail,at_epoch,at)"
                    " VALUES (?,'created',NULL,?,?)",
                    (commitment_id, now_epoch, record["created_at"]),
                )
                # Rejected decisions are queued for sealing too: false
                # negatives stay learnable outcomes.
                connection.execute(
                    "INSERT OR IGNORE INTO deferred_seals"
                    " (job_id,commitment_id,state,attempts,"
                    " available_at,lease_until,last_error,created_at,"
                    " updated_at)"
                    " VALUES (?,?,'pending',0,?,NULL,NULL,?,?)",
                    ("ds-" + commitment_id[3:], commitment_id,
                     now_epoch, record["created_at"], record["created_at"]),
                )
        except DecisionCommitmentError:
            raise
        except Exception as exc:
            raise DecisionCommitmentError(
                "commitment_write_failed", str(exc)) from exc
        record["duplicate"] = False
        record["commitment_id"] = commitment_id
        return record

    def get(self, commitment_id: str) -> dict | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM decision_commitments WHERE commitment_id=?",
                (commitment_id,),
            ).fetchone()
        if row is None:
            return None
        record = dict(row)
        for derived in ("commitment_hash_verified", "tampered"):
            record.pop(derived, None)
        # executed_at is written by the atomic claim AFTER creation and is
        # excluded from the hashed shape; drop it before recomputing.
        hash_input = {k: v for k, v in record.items()
                      if k != "executed_at"}
        # Tamper detection: recompute the complete hash over the FULL
        # persisted row and compare. Any mutation -- decision flip, expiry
        # extension -- breaks this.
        expected = complete_commitment_hash(_persisted_shape(hash_input))
        if record.get("commitment_hash") != expected:
            return {
                **record,
                "commitment_hash_verified": False,
                "tampered": True,
            }
        record["commitment_hash_verified"] = True
        return record

    def claim_execution(self, commitment_id: str) -> dict:
        """Atomically claim the action slot (state: claimed).

        Exactly one concurrent caller can move a commitment from
        un-claimed to claimed; everyone else gets refused. The claim is
        PENDING until the caller confirms with :meth:`confirm_action` --
        an open_position failure must leave the commitment abortable or
        retryable, never falsely executed.
        """
        now = time.time()
        with self.connection() as connection:
            changed = connection.execute(
                """UPDATE decision_commitments SET executed_at=?
                   WHERE commitment_id=? AND decision='BUY_ELIGIBLE'
                     AND executed_at IS NULL""",
                (now, commitment_id),
            ).rowcount
            if not changed:
                row = connection.execute(
                    "SELECT decision, executed_at FROM decision_commitments"
                    " WHERE commitment_id=?", (commitment_id,),
                ).fetchone()
                if row is None:
                    return {"claimed": False, "reason": "commitment_missing"}
                if row["decision"] != "BUY_ELIGIBLE":
                    return {"claimed": False, "reason": "not_a_buy_decision"}
                if row["executed_at"] is not None:
                    return {"claimed": False, "reason": "already_claimed"}
                return {"claimed": False, "reason": "claim_refused"}
            connection.execute(
                "INSERT INTO commitment_events"
                " (commitment_id,status,detail,at_epoch,at)"
                " VALUES (?,'action_claimed',NULL,?,?)",
                (commitment_id, now, _utc_now()),
            )
        return {"claimed": True, "reason": "action_slot_claimed"}

    def confirm_action(self, commitment_id: str,
                       detail: str | None = None) -> None:
        """Record that the claimed action actually SUCCEEDED.

        Only after this does the commitment count as executed for
        sealing/dedup purposes. A claim without confirmation must be
        resolved by :meth:`abort_commitment` (failure) so no ring ever
        seals a false 'executed'.
        """
        self.record_event(commitment_id, "executed", detail)

    def abort_commitment(self, commitment_id: str,
                         reason: str) -> None:
        """Resolve a claim (or an unclaimed commitment) as aborted.

        Aborted commitments are terminal: they seal with their refusal
        context and can never authorize again.
        """
        self.record_event(commitment_id, "aborted", reason)

    def recover_expired_commitments(self, *, now: float | None = None,
                                    grace_seconds: float = 60.0) -> int:
        """Lifecycle recovery: expire stale commitments and resolve their
        seal jobs. Runs at the start of every analysis-lane drain.

        - Unclaimed + past expiry -> aborted(expired); seal job released.
        - Claimed but never confirmed within the grace window (a worker
          died between claim and confirm) -> aborted(claim_expired).
        Returns the number of commitments resolved.
        """
        now = time.time() if now is None else float(now)
        resolved = 0
        with self.connection() as connection:
            rows = connection.execute(
                """SELECT commitment_id, decision, executed_at, expires_at
                   FROM decision_commitments
                   WHERE commitment_id NOT IN (
                       SELECT DISTINCT commitment_id FROM commitment_events
                       WHERE status IN ('executed','aborted',
                                        'superseded','executed_outcome'))"""
            ).fetchall()
            for row in rows:
                expired = now > safe_float(row["expires_at"], 0.0)
                claimed = row["executed_at"] is not None
                claim_stale = claimed and (
                    now - float(row["executed_at"]) > grace_seconds)
                if not expired and not claim_stale:
                    continue
                if claimed and not claim_stale:
                    continue  # actively claimed inside its grace window
                reason = ("claim_expired" if claim_stale
                          else "expired_without_action")
                connection.execute(
                    "INSERT INTO commitment_events"
                    " (commitment_id,status,detail,at_epoch,at)"
                    " VALUES (?,?,?,?,?)",
                    (row["commitment_id"], "aborted", reason,
                     now, _utc_now()),
                )
                # Release the seal job immediately: aborted commitments
                # are terminal and seal on the next drain.
                connection.execute(
                    """UPDATE deferred_seals SET state='pending',
                       available_at=?, lease_until=NULL, last_error=?,
                       updated_at=? WHERE commitment_id=? AND state IN
                       ('claiming','retrying')""",
                    (now, reason, _utc_now(), row["commitment_id"]),
                )
                resolved += 1
        return resolved

    def get_by_idempotency(self, key: str) -> dict | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM decision_commitments WHERE idempotency_key=?",
                (key,),
            ).fetchone()
        return dict(row) if row else None

    def record_event(self, commitment_id: str, status: str,
                     detail: str | None = None) -> None:
        with self.connection() as connection:
            connection.execute(
                "INSERT INTO commitment_events"
                " (commitment_id,status,detail,at_epoch,at)"
                " VALUES (?,?,?,?,?)",
                (commitment_id, status, detail, time.time(), _utc_now()),
            )

    def latest_events(self, commitment_id: str,
                      limit: int = 10) -> list[dict]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT * FROM commitment_events WHERE commitment_id=?"
                " ORDER BY event_id DESC LIMIT ?",
                (commitment_id, int(limit)),
            ).fetchall()
        return [dict(row) for row in reversed(rows)]

    def count_created(self) -> int:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) n FROM decision_commitments").fetchone()
        return int(row["n"])

    def increment_metric(self, metric: str, amount: int = 1) -> None:
        """Durable counter: survives process and dashboard restarts."""
        with self.connection() as connection:
            self._migrate(connection)
            connection.execute(
                """INSERT INTO gate_metrics (metric,value) VALUES (?,?)
                   ON CONFLICT(metric) DO UPDATE
                   SET value=value+excluded.value""",
                (metric, int(amount)),
            )

    def read_metric(self, metric: str) -> int:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT value FROM gate_metrics WHERE metric=?",
                (metric,),
            ).fetchone()
        return int(row["value"]) if row else 0

    def read_all_metrics(self) -> dict[str, int]:
        with self.connection() as connection:
            rows = connection.execute(
                "SELECT metric,value FROM gate_metrics").fetchall()
        return {row["metric"]: int(row["value"]) for row in rows}

    # ------------------------------------------------------------------ #
    # Verified-head cache (published by asynchronous verification)         #
    # ------------------------------------------------------------------ #
    def publish_verified_head(self, *, head_index: int, head_hash: str,
                              chain_root: str, registry_epoch: str,
                              ring_count: int, verification_result: str,
                              verifier_version: str,
                              ttl_seconds: float = (
                                  INTEGRITY_CERTIFICATE_MAX_AGE_SECONDS),
                              ) -> dict:
        record = {
            "head_index": int(head_index),
            "head_hash": str(head_hash),
            "chain_root": str(chain_root),
            "registry_epoch": str(registry_epoch),
            "ring_count": int(ring_count),
            "verification_result": str(verification_result),
            "verifier_version": str(verifier_version),
            "published_at": _utc_now(),
            "published_epoch": time.time(),
            "expires_at": time.time() + float(ttl_seconds),
        }
        atomic_json_write(
            self.path.parent / CERTIFICATE_FILE_NAME, record)
        with self.connection() as connection:
            connection.execute(
                """INSERT INTO verified_heads
                   (id,head_index,head_hash,chain_root,registry_epoch,
                    ring_count,verification_result,verifier_version,
                    published_at,expires_at)
                   VALUES (1,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(id) DO UPDATE SET
                     head_index=excluded.head_index,
                     head_hash=excluded.head_hash,
                     chain_root=excluded.chain_root,
                     registry_epoch=excluded.registry_epoch,
                     ring_count=excluded.ring_count,
                     verification_result=excluded.verification_result,
                     verifier_version=excluded.verifier_version,
                     published_at=excluded.published_at,
                     expires_at=excluded.expires_at""",
                (record["head_index"], record["head_hash"],
                 record["chain_root"], record["registry_epoch"],
                 record["ring_count"], record["verification_result"],
                 record["verifier_version"], record["published_at"],
                 record["expires_at"]),
            )
        return record

    def verified_head(self) -> dict | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM verified_heads WHERE id=1").fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------ #
    # Asynchronous seal queue                                             #
    # ------------------------------------------------------------------ #
    def _ensure_queue_table(self, connection: sqlite3.Connection) -> None:
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS deferred_seals (
                job_id TEXT PRIMARY KEY,
                commitment_id TEXT NOT NULL UNIQUE,
                state TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                available_at REAL NOT NULL,
                lease_until REAL,
                last_error TEXT,
                sealed_ring_index INTEGER,
                sealed_ring_hash TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS deferred_seals_ready
                ON deferred_seals(state, available_at);
            """
        )

    def claim_seal_batch(self, limit: int = 4) -> list[dict]:
        """Lease up to ``limit`` due jobs to this worker. Crash-safe: a
        claimed job whose worker dies becomes claimable again once its
        lease expires."""
        now = time.time()
        with self.connection() as connection:
            self._ensure_queue_table(connection)
            claimed = []
            for _ in range(max(1, int(limit))):
                # The guarded UPDATE is the claim: only ONE concurrent
                # worker's UPDATE matches a given pending row; everyone
                # else's rowcount is 0 and they move on.
                row = connection.execute(
                    """SELECT * FROM deferred_seals
                       WHERE (state='pending' AND available_at<=?)
                          OR (state='claiming' AND lease_until<?)
                       ORDER BY available_at LIMIT 1""",
                    (now, now),
                ).fetchone()
                if row is None:
                    break
                changed = connection.execute(
                    """UPDATE deferred_seals SET state='claiming',
                       lease_until=?, attempts=attempts+1, updated_at=?
                       WHERE job_id=? AND (
                         (state='pending' AND available_at<=?) OR
                         (state='claiming' AND lease_until<?))""",
                    (now + SEAL_LEASE_SECONDS, _utc_now(), row["job_id"],
                     now, now),
                ).rowcount
                if changed:
                    claimed.append(dict(row))
        return claimed

    def complete_seal(self, job_id: str, ring_index: int,
                      ring_hash: str) -> None:
        with self.connection() as connection:
            self._ensure_queue_table(connection)
            connection.execute(
                """UPDATE deferred_seals SET state='sealed',
                   sealed_ring_index=?, sealed_ring_hash=?, lease_until=NULL,
                   last_error=NULL, updated_at=? WHERE job_id=?""",
                (int(ring_index), str(ring_hash), _utc_now(), job_id),
            )

    def fail_seal(self, job_id: str, error: str,
                  *, attempts: int | None = None) -> str:
        """Record a failed attempt: retry with bounded backoff, or move to
        the dead-letter state once retries are exhausted. Returns the new
        state."""
        now = time.time()
        with self.connection() as connection:
            self._ensure_queue_table(connection)
            row = connection.execute(
                "SELECT attempts FROM deferred_seals WHERE job_id=?",
                (job_id,),
            ).fetchone()
            used = int(row["attempts"]) if row else 1
            if attempts is not None:
                # Explicit attempt override (e.g. releasing a job without
                # burning a retry for "awaiting terminal state").
                connection.execute(
                    """UPDATE deferred_seals SET state='retrying',
                       attempts=?, last_error=?, lease_until=NULL,
                       available_at=?, updated_at=? WHERE job_id=?""",
                    (int(attempts), error[:500],
                     now if attempts == 0 else now + 60.0,
                     _utc_now(), job_id),
                )
                return "retrying"
            if used >= SEAL_RETRY_MAX_ATTEMPTS:
                new_state = "dead_letter"
                connection.execute(
                    """UPDATE deferred_seals SET state='dead_letter',
                       last_error=?, lease_until=NULL, updated_at=?
                       WHERE job_id=?""",
                    (error[:500], _utc_now(), job_id),
                )
            else:
                new_state = "retrying"
                backoff = min(
                    SEAL_RETRY_BASE_BACKOFF_SECONDS * (2 ** max(0, used - 1)),
                    3600.0,
                )
                connection.execute(
                    """UPDATE deferred_seals SET state='retrying',
                       last_error=?, available_at=?, lease_until=NULL,
                       updated_at=? WHERE job_id=?""",
                    (error[:500], now + backoff, _utc_now(), job_id),
                )
        return new_state

    def requeue_retrying(self) -> int:
        """Move due retrying jobs back to pending so they can be claimed."""
        now = time.time()
        with self.connection() as connection:
            self._ensure_queue_table(connection)
            changed = connection.execute(
                """UPDATE deferred_seals SET state='pending', updated_at=?
                   WHERE state='retrying' AND available_at<=?""",
                (_utc_now(), now),
            ).rowcount
        return int(changed)

    def recover_expired_leases(self) -> int:
        """Restart recovery: reclaim jobs whose worker died mid-seal."""
        now = time.time()
        with self.connection() as connection:
            self._ensure_queue_table(connection)
            changed = connection.execute(
                """UPDATE deferred_seals SET state='pending', updated_at=?
                   WHERE state='claiming' AND lease_until<?""",
                (_utc_now(), now),
            ).rowcount
        return int(changed)

    def seal_debt(self) -> dict:
        with self.connection() as connection:
            self._ensure_queue_table(connection)
            counts = {
                row["state"]: int(row["n"])
                for row in connection.execute(
                    "SELECT state, COUNT(*) n FROM deferred_seals"
                    " GROUP BY state")
            }
            oldest = connection.execute(
                """SELECT MIN(created_epoch) oldest
                   FROM deferred_seals JOIN decision_commitments
                   USING(commitment_id)
                   WHERE deferred_seals.state IN
                         ('pending','claiming','retrying')"""
            ).fetchone()
            latest = connection.execute(
                """SELECT commitment_id, sealed_ring_index, sealed_ring_hash,
                          updated_at
                   FROM deferred_seals WHERE state='sealed'
                   ORDER BY updated_at DESC LIMIT 1"""
            ).fetchone()
        oldest_epoch = None
        if oldest is not None:
            oldest_epoch = safe_float(oldest["oldest"], None)
        pending = counts.get("pending", 0) + counts.get("claiming", 0) \
            + counts.get("retrying", 0)
        return {
            "pending_seals": pending,
            "retrying": counts.get("retrying", 0),
            "dead_letter": counts.get("dead_letter", 0),
            "sealed": counts.get("sealed", 0),
            "oldest_pending_age_seconds": (
                round(time.time() - oldest_epoch, 1)
                if oldest_epoch is not None else None),
            "latest_sealed_commitment": (
                dict(latest) if latest else None),
        }

    def seal_latency_samples(self) -> list[float]:
        with self.connection() as connection:
            self._ensure_queue_table(connection)
            rows = connection.execute(
                """SELECT c.created_epoch, s.updated_at
                   FROM deferred_seals s
                   JOIN decision_commitments c USING(commitment_id)
                   WHERE s.state='sealed' ORDER BY s.updated_at DESC
                   LIMIT 200"""
            ).fetchall()
        samples = []
        for row in rows:
            created = safe_float(row["created_epoch"], None)
            try:
                finished = datetime.fromisoformat(
                    row["updated_at"]).timestamp()
            except (TypeError, ValueError):
                continue
            if created is not None and finished >= created:
                samples.append(finished - created)
        return samples


# ---------------------------------------------------------------------- #
# Cached integrity certificate                                            #
# ---------------------------------------------------------------------- #
def evaluate_integrity_certificate(
    certificate: dict | None, *, now: float | None = None,
    current_ring_count: int | None = None,
) -> tuple[bool, str, dict]:
    """Fail-closed evaluation of the cached integrity certificate."""
    now = time.time() if now is None else float(now)
    detail: dict = {}
    if not certificate:
        return False, "certificate_missing", detail
    result = str(certificate.get("verification_result") or "").lower()
    if result not in {"pass", "ok", "valid", "true"}:
        return False, "certificate_invalid", detail
    expires_at = safe_float(certificate.get("expires_at"), None)
    if expires_at is None or now > expires_at:
        return False, "certificate_stale", detail
    if current_ring_count is not None:
        ring_count = safe_int(certificate.get("ring_count"), None)
        if ring_count is not None and (
                current_ring_count - ring_count
                > INTEGRITY_CERTIFICATE_MAX_RING_LAG):
            return False, "certificate_behind_rings", detail
    detail["age_seconds"] = round(
        now - safe_float(certificate.get("published_epoch"), now), 1)
    return True, "certificate_valid", detail


def load_integrity_certificate(root: str | Path) -> dict | None:
    return read_json(Path(root) / CERTIFICATE_FILE_NAME, {}) or {}


# ---------------------------------------------------------------------- #
# Seal-debt policy                                                        #
# ---------------------------------------------------------------------- #
def evaluate_seal_debt(debt: dict) -> tuple[bool, str]:
    """Conservative gates. Exceeding any limit blocks new exposure only."""
    if safe_int(debt.get("pending_seals"), 0) > SEAL_DEBT_MAX_PENDING:
        return False, "seal_debt_pending_exceeded"
    if safe_int(debt.get("retrying"), 0) > SEAL_DEBT_MAX_RETRYING:
        return False, "seal_debt_retrying_exceeded"
    if safe_int(debt.get("dead_letter"), 0) > SEAL_DEBT_MAX_DEAD_LETTER:
        return False, "seal_debt_dead_letter_exceeded"
    age = safe_float(debt.get("oldest_pending_age_seconds"), 0.0) or 0.0
    if age > SEAL_DEBT_MAX_OLDEST_PENDING_AGE_SECONDS:
        return False, "seal_debt_oldest_age_exceeded"
    return True, "seal_debt_within_bounds"


def overall_gate_state(integrity_ok: bool, debt_ok: bool,
                       explanation: list[str]) -> str:
    if integrity_ok and debt_ok:
        return "HEALTHY"
    if integrity_ok or debt_ok:
        explanation.append(
            "DEGRADED: one safety input is out of bounds; "
            "new exposure is blocked until it recovers")
        return "DEGRADED"
    explanation.append(
        "FAIL_CLOSED: integrity and seal-debt inputs are both unhealthy")
    return "FAIL_CLOSED"
