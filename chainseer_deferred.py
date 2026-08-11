"""Durable, coalescing work queue for deferred Chainseer commits.

The queue is deliberately independent from the Timechain.  It persists immutable
observation/preparation envelopes, but it can never append a ring.  The API's
single writer boundary owns the final validate-and-append operation.
"""

from __future__ import annotations

import json
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


TERMINAL_STATES = {"done", "idempotent", "superseded", "discarded", "dead_letter"}
ACTIVE_STATES = {"pending", "preparing", "ready", "committing", "retry"}


def _canonical_json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
        default=str,
    )


@dataclass(frozen=True)
class DeferredQueueItem:
    id: int
    kind: str
    subject_key: str
    generation: int
    priority: int
    state: str
    payload: dict[str, Any]
    attempts: int
    created_at: float
    updated_at: float


class DurableDeferredQueue:
    """SQLite-backed work queue with subject coalescing and crash leases."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=5.0,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=5000")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        return connection

    @contextmanager
    def _connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self._connection() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS deferred_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    subject_key TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    priority INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    available_at REAL NOT NULL,
                    lease_until REAL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    UNIQUE(kind, subject_key)
                )
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS deferred_jobs_ready
                ON deferred_jobs(state, available_at, priority, updated_at)
                """
            )

    @staticmethod
    def _freshness(payload: dict[str, Any]) -> tuple[int, float]:
        anchor = payload.get("anchor_value")
        try:
            anchor_value = int(anchor)
        except (TypeError, ValueError, OverflowError):
            anchor_value = -1
        observed = payload.get("observed_at_epoch", payload.get("enqueued_at", 0))
        try:
            observed_value = float(observed)
        except (TypeError, ValueError, OverflowError):
            observed_value = 0.0
        return anchor_value, observed_value

    def enqueue(
        self,
        *,
        kind: str,
        subject_key: str,
        payload: dict[str, Any],
        priority: int,
        now: float | None = None,
    ) -> int:
        """Insert or replace a subject's work with its freshest observation.

        Returns the generation that represents the currently stored work.  An
        older delivery never replaces a newer anchor already in the queue.
        """

        stamp = time.time() if now is None else float(now)
        encoded = _canonical_json(payload)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM deferred_jobs WHERE kind=? AND subject_key=?",
                (kind, subject_key),
            ).fetchone()
            if existing is None:
                generation = 1
                connection.execute(
                    """
                    INSERT INTO deferred_jobs(
                        kind, subject_key, generation, priority, state,
                        payload_json, available_at, lease_until, attempts,
                        last_error, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'pending', ?, ?, NULL, 0, NULL, ?, ?)
                    """,
                    (
                        kind,
                        subject_key,
                        generation,
                        int(priority),
                        encoded,
                        stamp,
                        stamp,
                        stamp,
                    ),
                )
                connection.execute("COMMIT")
                return generation

            existing_payload = json.loads(existing["payload_json"])
            if self._freshness(payload) < self._freshness(existing_payload):
                connection.execute("COMMIT")
                return int(existing["generation"])
            if (
                self._freshness(payload) == self._freshness(existing_payload)
                and payload.get("report_hash") == existing_payload.get("report_hash")
            ):
                connection.execute("COMMIT")
                return int(existing["generation"])

            generation = int(existing["generation"]) + 1
            connection.execute(
                """
                UPDATE deferred_jobs
                SET generation=?, priority=?, state='pending', payload_json=?,
                    available_at=?, lease_until=NULL, attempts=0,
                    last_error=NULL, updated_at=?
                WHERE id=?
                """,
                (
                    generation,
                    int(priority),
                    encoded,
                    stamp,
                    stamp,
                    int(existing["id"]),
                ),
            )
            connection.execute("COMMIT")
            return generation

    def claim(
        self,
        *,
        lease_seconds: float = 30.0,
        now: float | None = None,
        kinds: tuple[str, ...] | None = None,
    ) -> DeferredQueueItem | None:
        stamp = time.time() if now is None else float(now)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(
                """
                UPDATE deferred_jobs
                SET state='retry', lease_until=NULL, available_at=?, updated_at=?
                WHERE state IN ('preparing', 'ready', 'committing')
                  AND lease_until IS NOT NULL AND lease_until <= ?
                """,
                (stamp, stamp, stamp),
            )
            params: list[Any] = [stamp]
            kind_clause = ""
            if kinds:
                placeholders = ",".join("?" for _ in kinds)
                kind_clause = f" AND kind IN ({placeholders})"
                params.extend(kinds)
            row = connection.execute(
                """
                SELECT * FROM deferred_jobs
                WHERE state IN ('pending', 'retry') AND available_at <= ?
                """
                + kind_clause
                + " ORDER BY priority ASC, updated_at ASC LIMIT 1",
                tuple(params),
            ).fetchone()
            if row is None:
                connection.execute("COMMIT")
                return None
            connection.execute(
                """
                UPDATE deferred_jobs
                SET state='preparing', lease_until=?, attempts=attempts+1,
                    updated_at=?
                WHERE id=? AND generation=?
                """,
                (
                    stamp + max(1.0, float(lease_seconds)),
                    stamp,
                    int(row["id"]),
                    int(row["generation"]),
                ),
            )
            connection.execute("COMMIT")
            return DeferredQueueItem(
                id=int(row["id"]),
                kind=str(row["kind"]),
                subject_key=str(row["subject_key"]),
                generation=int(row["generation"]),
                priority=int(row["priority"]),
                state="preparing",
                payload=json.loads(row["payload_json"]),
                attempts=int(row["attempts"]) + 1,
                created_at=float(row["created_at"]),
                updated_at=stamp,
            )

    def transition(
        self,
        item: DeferredQueueItem,
        state: str,
        *,
        lease_seconds: float = 30.0,
        now: float | None = None,
    ) -> bool:
        if state not in ACTIVE_STATES | TERMINAL_STATES:
            raise ValueError(f"unsupported deferred queue state: {state}")
        stamp = time.time() if now is None else float(now)
        lease = (
            stamp + max(1.0, float(lease_seconds))
            if state in {"preparing", "ready", "committing"}
            else None
        )
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE deferred_jobs SET state=?, lease_until=?, updated_at=?
                WHERE id=? AND generation=?
                """,
                (state, lease, stamp, item.id, item.generation),
            )
            return cursor.rowcount == 1

    def is_current(self, item: DeferredQueueItem) -> bool:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT generation FROM deferred_jobs WHERE id=?",
                (item.id,),
            ).fetchone()
        return row is not None and int(row["generation"]) == item.generation

    def retry(
        self,
        item: DeferredQueueItem,
        error: str,
        *,
        delay_seconds: float = 0.1,
        max_attempts: int = 5,
        now: float | None = None,
    ) -> bool:
        stamp = time.time() if now is None else float(now)
        state = "dead_letter" if item.attempts >= max_attempts else "retry"
        available = stamp if state == "dead_letter" else stamp + max(0.0, delay_seconds)
        with self._connection() as connection:
            cursor = connection.execute(
                """
                UPDATE deferred_jobs
                SET state=?, available_at=?, lease_until=NULL,
                    last_error=?, updated_at=?
                WHERE id=? AND generation=?
                """,
                (
                    state,
                    available,
                    str(error)[:1000],
                    stamp,
                    item.id,
                    item.generation,
                ),
            )
            return cursor.rowcount == 1

    def counts(self) -> dict[str, int]:
        with self._connection() as connection:
            rows = connection.execute(
                "SELECT state, COUNT(*) AS count FROM deferred_jobs GROUP BY state"
            ).fetchall()
        return {str(row["state"]): int(row["count"]) for row in rows}

    def get(self, kind: str, subject_key: str) -> DeferredQueueItem | None:
        with self._connection() as connection:
            row = connection.execute(
                "SELECT * FROM deferred_jobs WHERE kind=? AND subject_key=?",
                (kind, subject_key),
            ).fetchone()
        if row is None:
            return None
        return DeferredQueueItem(
            id=int(row["id"]),
            kind=str(row["kind"]),
            subject_key=str(row["subject_key"]),
            generation=int(row["generation"]),
            priority=int(row["priority"]),
            state=str(row["state"]),
            payload=json.loads(row["payload_json"]),
            attempts=int(row["attempts"]),
            created_at=float(row["created_at"]),
            updated_at=float(row["updated_at"]),
        )
