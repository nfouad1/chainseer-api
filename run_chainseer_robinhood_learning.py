"""Scheduled Robinhood learner supervisor with native process-tree ownership.

This launcher intentionally uses only the standard library until the local
virtual-environment site-packages directory has been added to ``sys.path``.
Task Scheduler invokes the real CPython interpreter directly, so neither the
uv virtual-environment launcher nor PowerShell can become an orphan boundary.
"""

from __future__ import annotations

import argparse
import ctypes
from ctypes import wintypes
from datetime import datetime, timezone
import faulthandler
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback


WORKSPACE = Path(__file__).resolve().parent
VENV_SITE_PACKAGES = WORKSPACE / ".venv" / "Lib" / "site-packages"
if str(VENV_SITE_PACKAGES) not in sys.path:
    sys.path.insert(0, str(VENV_SITE_PACKAGES))


JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x00002000
JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS = 9
_JOB_HANDLE: int | None = None
_JOB_MODE = "uninitialized"


def _workspace_revision(root: Path = WORKSPACE) -> str:
    """Read the current Git commit directly, including worktree gitdirs.

    The runner stamps this into the environment before importing the learner,
    so every estimator record identifies the code revision that produced it.
    No subprocess or network dependency is needed during supervised startup.
    """
    git_dir = root / ".git"
    try:
        if git_dir.is_file():
            marker = git_dir.read_text(encoding="utf-8").strip()
            if not marker.lower().startswith("gitdir:"):
                return "unknown"
            git_dir = (root / marker.split(":", 1)[1].strip()).resolve()
        head = (git_dir / "HEAD").read_text(encoding="ascii").strip()
        if not head.startswith("ref:"):
            return head[:12] if head else "unknown"
        ref = head.split(":", 1)[1].strip()
        ref_path = git_dir / ref
        if ref_path.exists():
            value = ref_path.read_text(encoding="ascii").strip()
            return value[:12] if value else "unknown"
        packed = git_dir / "packed-refs"
        if packed.exists():
            for line in packed.read_text(encoding="ascii").splitlines():
                if not line or line.startswith(("#", "^")):
                    continue
                value, name = line.split(" ", 1)
                if name.strip() == ref:
                    return value[:12]
    except (OSError, ValueError):
        return "unknown"
    return "unknown"


class _BasicLimitInformation(ctypes.Structure):
    _fields_ = [
        ("PerProcessUserTimeLimit", ctypes.c_int64),
        ("PerJobUserTimeLimit", ctypes.c_int64),
        ("LimitFlags", wintypes.DWORD),
        ("MinimumWorkingSetSize", ctypes.c_size_t),
        ("MaximumWorkingSetSize", ctypes.c_size_t),
        ("ActiveProcessLimit", wintypes.DWORD),
        ("Affinity", ctypes.c_size_t),
        ("PriorityClass", wintypes.DWORD),
        ("SchedulingClass", wintypes.DWORD),
    ]


class _IoCounters(ctypes.Structure):
    _fields_ = [
        ("ReadOperationCount", ctypes.c_uint64),
        ("WriteOperationCount", ctypes.c_uint64),
        ("OtherOperationCount", ctypes.c_uint64),
        ("ReadTransferCount", ctypes.c_uint64),
        ("WriteTransferCount", ctypes.c_uint64),
        ("OtherTransferCount", ctypes.c_uint64),
    ]


class _ExtendedLimitInformation(ctypes.Structure):
    _fields_ = [
        ("BasicLimitInformation", _BasicLimitInformation),
        ("IoInfo", _IoCounters),
        ("ProcessMemoryLimit", ctypes.c_size_t),
        ("JobMemoryLimit", ctypes.c_size_t),
        ("PeakProcessMemoryUsed", ctypes.c_size_t),
        ("PeakJobMemoryUsed", ctypes.c_size_t),
    ]


def own_process_tree() -> int:
    """Put this process and every descendant in a kill-on-close Job Object."""
    if sys.platform != "win32":
        raise RuntimeError("the scheduled Robinhood learner requires Windows")
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel32.CreateJobObjectW.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
    kernel32.CreateJobObjectW.restype = wintypes.HANDLE
    kernel32.SetInformationJobObject.argtypes = [
        wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD,
    ]
    kernel32.SetInformationJobObject.restype = wintypes.BOOL
    kernel32.AssignProcessToJobObject.argtypes = [wintypes.HANDLE, wintypes.HANDLE]
    kernel32.AssignProcessToJobObject.restype = wintypes.BOOL
    kernel32.IsProcessInJob.argtypes = [
        wintypes.HANDLE, wintypes.HANDLE, ctypes.POINTER(wintypes.BOOL),
    ]
    kernel32.IsProcessInJob.restype = wintypes.BOOL
    kernel32.GetCurrentProcess.restype = wintypes.HANDLE

    current_process = kernel32.GetCurrentProcess()
    already_owned = wintypes.BOOL(False)
    if not kernel32.IsProcessInJob(current_process, None, ctypes.byref(already_owned)):
        raise ctypes.WinError(ctypes.get_last_error())
    global _JOB_HANDLE, _JOB_MODE
    if already_owned.value:
        # Task Scheduler already owns the direct interpreter. Children inherit
        # that job, and Stop-ScheduledTask terminates the complete tree. A
        # second nested job was observed to suspend module initialization.
        _JOB_HANDLE = 0
        _JOB_MODE = "inherited_scheduler_job"
        return _JOB_HANDLE

    handle = kernel32.CreateJobObjectW(None, None)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    limits = _ExtendedLimitInformation()  # ctypes zero-initializes every field.
    limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
    if not kernel32.SetInformationJobObject(
        handle,
        JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS,
        ctypes.byref(limits),
        ctypes.sizeof(limits),
    ):
        raise ctypes.WinError(ctypes.get_last_error())
    if not kernel32.AssignProcessToJobObject(handle, current_process):
        raise ctypes.WinError(ctypes.get_last_error())
    _JOB_HANDLE = int(handle)
    _JOB_MODE = "created_kill_on_close_job"
    return _JOB_HANDLE


def _atomic_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True), encoding="utf-8"
    )
    temporary.replace(path)


def _runner_status(root: Path, *, status: str, started: float,
                   error: str | None = None, result: dict | None = None) -> None:
    terminal = status != "running"
    _atomic_json(root / "runner_status.json", {
        "schema_version": 2,
        "mode": "native_python_job_supervisor",
        "status": status,
        "pid": int(__import__("os").getpid()),
        "started_at": datetime.fromtimestamp(started, timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat() if terminal else None,
        "heartbeat_at": time.time(),
        "duration_seconds": round(time.time() - started, 3),
        "last_error": error,
        "result": result or {},
        "paper_only": True,
        "live_execution_enabled": False,
        "process_tree_owned": True,
        "process_tree_ownership": _JOB_MODE,
    })


def _probe() -> int:
    """Test hook: prove a child dies when this process is force-terminated."""
    own_process_tree()
    child = subprocess.Popen([
        sys.executable, "-c", "import time; time.sleep(300)",
    ])
    print(json.dumps({"parent_pid": __import__("os").getpid(),
                      "child_pid": child.pid}), flush=True)
    time.sleep(300)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=285.0)
    # The chain has recently grown faster than 500 blocks per five-minute
    # backfill cadence, so 500 guaranteed backlog growth even with perfect
    # runs. One thousand provides measured convergence headroom while the
    # lane's own deadline/admission controller remains authoritative.
    parser.add_argument("--discovery-block-limit", type=int, default=1000)
    parser.add_argument("--analysis-limit", type=int, default=8)
    parser.add_argument("--outcome-limit", type=int, default=12)
    parser.add_argument("--outcome-recovery-limit", type=int, default=4)
    parser.add_argument("--market-recheck-limit", type=int, default=4)
    parser.add_argument("--job-object-probe", action="store_true")
    args = parser.parse_args()
    if args.job_object_probe:
        return _probe()

    root = WORKSPACE / "robinhood_learning"
    started = time.time()
    try:
        own_process_tree()
        _runner_status(root, status="initializing", started=started)
        # Import only after ownership and status are authoritative. This module
        # is intentionally large; a scheduled startup must never disappear
        # into an unobservable import before the Job Object exists.
        os.environ.setdefault(
            "CHAINSEER_CODE_REVISION", _workspace_revision(WORKSPACE))
        startup_trace = root / "runner_startup_trace.log"
        with startup_trace.open("ab", buffering=0) as trace_stream:
            faulthandler.dump_traceback_later(
                10.0, repeat=True, file=trace_stream,
            )
            try:
                import chainseer_robinhood as robinhood
            finally:
                faulthandler.cancel_dump_traceback_later()
        _runner_status(root, status="running", started=started)
        result = robinhood.supervise_lanes(
            root,
            chain_root="robinhood_learning_chain",
            skill_root=str(robinhood.default_skill_root()),
            duration_seconds=max(5.0, float(args.duration_seconds)),
            discovery_block_limit=max(1, int(args.discovery_block_limit)),
            analysis_limit=max(0, int(args.analysis_limit)),
            outcome_limit=max(0, int(args.outcome_limit)),
            outcome_recovery_limit=max(0, int(args.outcome_recovery_limit)),
            market_recheck_limit=max(0, int(args.market_recheck_limit)),
        )
        _runner_status(
            root, status="complete", started=started, result=result,
        )
        return 0
    except BaseException as exc:
        _runner_status(
            root, status="failed", started=started,
            error="".join(traceback.format_exception(exc))[-4_000:],
        )
        raise


if __name__ == "__main__":
    raise SystemExit(main())
