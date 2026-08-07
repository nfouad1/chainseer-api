"""chainseer_core.py — shared low-level helpers for the Base, Solana, and Pons
adapters (chainseer_base.py, chainseer_solana.py, chainseer_pons.py).

Extracted 2026-07-31 from three copies that had independently drifted:
solana had picked up a Windows atomic-replace retry loop and an
isfinite/OverflowError-safe float coercion that base and pons lacked; pons and
solana had both independently added BOM-tolerant reads that base lacked.
Pre-consolidation originals are preserved as *.pre-consolidation.bak next to
each adapter file.

IMPORTANT — `canonical_json` is used to compute the SHA-256 hashes that seal
each adapter's append-only event ledger (see `record["event_hash"] =
hashlib.sha256(canonical_json(record).encode()).hexdigest()` call sites). Its
exact byte output for a given adapter must not change, or that adapter's
historical records would fail re-verification. Base and Pons originally called
`json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)`
(ensure_ascii defaults to True) — that is this module's default. Solana
originally called `json.dumps(value, sort_keys=True, separators=(",", ":"),
ensure_ascii=False)` with no `default` — reproduce that with
`canonical_json(value, ensure_ascii=False, default=None)`. Do not change these
defaults without re-verifying each adapter's ledger against its stored hashes.

The other helpers here (`atomic_json_write`, `read_json`, `safe_float`,
`safe_int`, `utc_now`) are plain I/O and numeric-coercion utilities with no
hash-verification contract, so they are merged to the best-of-breed behavior
across the three original copies (BOM-tolerant reads, Windows-safe atomic
replace, isfinite-checked float coercion).
"""

from __future__ import annotations

import json
import math
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value, *, ensure_ascii: bool = True, default=str) -> str:
    """Deterministic JSON used as the pre-image for ledger event hashes.

    See the module docstring: each adapter must bind this with the flags that
    match its original behavior so historical hashes keep verifying.
    """
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii, default=default
    )


def safe_float(value, default: float = 0.0) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) else default


def safe_int(value, default: int = 0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def atomic_json_write(path: Path, value, *, ensure_ascii: bool = False, default=str) -> None:
    """Write JSON via temp file + atomic replace.

    Retries the final replace with exponential backoff on PermissionError,
    since a dashboard or tailing script on Windows can briefly hold the
    destination handle open; the reader releases it immediately, so a short
    bounded retry preserves atomicity without weakening the single-writer
    contract these adapters rely on.

    The scratch file is unique per call. A fixed "<name>.tmp" is shared by
    every process writing the same path, so two writers interleave as:
    A writes scratch, B overwrites the SAME scratch, B replaces, A replaces.
    The loud outcome is A failing with FileNotFoundError because its scratch
    is gone. The quiet one is worse -- A's destination silently receives B's
    content, with no exception raised at all. Pons hit both: its learn cycle
    and its quote guard each write admission_state.json.

    A unique scratch name does not make concurrent writes safe -- last writer
    still wins, and callers that must not clobber each other need a lock. It
    makes each write self-consistent, so a file can never contain a blend of
    two writers or lose one entirely to the other's rename.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f"{path.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"
    )
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True, ensure_ascii=ensure_ascii, default=default),
            encoding="utf-8",
        )
        for attempt in range(6):
            try:
                temporary.replace(path)
                return
            except PermissionError:
                if attempt == 5:
                    raise
                time.sleep(0.05 * (2**attempt))
    finally:
        # A unique name means a failed write leaves litter no later run would
        # reuse, so it has to be cleaned up here. Succeeded replaces have
        # already moved the file, making this a no-op.
        try:
            temporary.unlink()
        except OSError:
            pass


def read_json(path: Path, default=None):
    """Read JSON, tolerant of a UTF-8 BOM (PowerShell's `-Encoding utf8`
    writes one) and of the file not existing or being unreadable/corrupt.
    """
    path = Path(path)
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        return default
