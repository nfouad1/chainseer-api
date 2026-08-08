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
import subprocess
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



# --------------------------------------------------------------------------- #
# Live scheduler state
# --------------------------------------------------------------------------- #

_TASK_STATE_CACHE: dict[str, tuple[float, dict]] = {}
_TASK_STATE_TTL_SECONDS = 10.0


def scheduled_task_state(
    task_name: str, *, ttl_seconds: float = _TASK_STATE_TTL_SECONDS
) -> dict:
    """Ask the OS what a scheduled task is actually doing.

    The adapters persist a schedule.json saying whether their task is enabled,
    but only the manage_*.ps1 scripts update it. Enabling a task any other way
    -- Enable-ScheduledTask, the Task Scheduler UI -- leaves that file behind,
    and a dashboard reading it then reports "paused" while the learner runs.
    Observed in production: schedule.json still claimed enabled=false from a
    timestamp three weeks stale while the task was Running.

    Returns enabled=None, not False, when the state cannot be determined. A
    dashboard that cannot see the scheduler must say so rather than assert the
    learner is stopped -- the whole point is to stop reporting confident
    falsehoods about it.
    """
    now = time.time()
    cached = _TASK_STATE_CACHE.get(task_name)
    if cached and (now - cached[0]) < max(0.0, ttl_seconds):
        return dict(cached[1])

    result = {
        "task_name": task_name,
        "state": None,
        "enabled": None,
        "available": False,
        "source": "unavailable",
        "reason": None,
    }
    if os.name != "nt":
        result["reason"] = "scheduled tasks are Windows-only"
    else:
        try:
            completed = subprocess.run(
                ["schtasks", "/query", "/tn", task_name, "/fo", "csv", "/nh"],
                capture_output=True,
                text=True,
                errors="replace",
                timeout=8,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            result["reason"] = f"{type(exc).__name__}: {exc}"[:160]
        else:
            if completed.returncode != 0:
                result["reason"] = (
                    (completed.stderr or "schtasks query failed").strip()[:160]
                )
            else:
                row = (completed.stdout or "").strip().splitlines()
                fields = (
                    [item.strip().strip('"') for item in row[0].split('","')]
                    if row
                    else []
                )
                state = fields[2].strip() if len(fields) > 2 else ""
                if state:
                    result.update({
                        "state": state,
                        # "Disabled" is the only state that means not running on
                        # schedule; Ready and Running are both live.
                        "enabled": state.lower() != "disabled",
                        "available": True,
                        "source": "schtasks",
                    })
                else:
                    result["reason"] = "schtasks returned no state column"

    _TASK_STATE_CACHE[task_name] = (now, dict(result))
    return result


def schedule_with_live_state(declared: dict, task_name: str | None = None) -> dict:
    """Merge a persisted schedule block with the scheduler's real state.

    The declared file still supplies static configuration -- interval, runner
    path, task name. Only `enabled` is replaced, and the file's own value is
    preserved as declared_enabled so a drift between the two stays visible
    rather than being silently overwritten.
    """
    merged = dict(declared or {})
    name = task_name or merged.get("task_name")
    if not name:
        merged["enabled_source"] = "declared"
        return merged
    live = scheduled_task_state(str(name))
    merged["declared_enabled"] = merged.get("enabled")
    merged["live_state"] = live["state"]
    merged["enabled_source"] = live["source"]
    if live["available"]:
        merged["enabled"] = live["enabled"]
    else:
        merged["enabled"] = None
        merged["enabled_unavailable_reason"] = live["reason"]
    merged["schedule_drift"] = (
        live["available"]
        and merged["declared_enabled"] is not None
        and bool(merged["declared_enabled"]) != bool(live["enabled"])
    )
    return merged

def read_json(path: Path, default=None):
    """Read JSON, tolerant of a UTF-8 BOM (PowerShell's `-Encoding utf8`
    writes one) and of the file not existing or being unreadable/corrupt.
    """
    path = Path(path)
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        return default
