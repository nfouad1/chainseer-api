"""Read-only, independently aged local learner telemetry (no DB or RPC work)."""
from __future__ import annotations

from datetime import datetime
import json
import math
from pathlib import Path
import time


def _read(path: Path) -> dict:
    try:
        # Request-path telemetry must stay bounded even if a file is damaged.
        with path.open(encoding="utf-8-sig") as stream:
            raw = stream.read(1_048_577)
        if len(raw) > 1_048_576:
            return {}
        value = json.loads(raw)
        return value if isinstance(value, dict) else {}
    except (OSError, ValueError):
        return {}


def _age(value, now: float) -> float | None:
    try:
        stamp = (float(value) if isinstance(value, (int, float)) else
                 datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
        age = now - stamp
        # Future or malformed telemetry is not fresh evidence.
        return round(age, 1) if math.isfinite(age) and stamp > 0 and age >= 0 else None
    except (ValueError, TypeError, OverflowError, OSError):
        return None


def classify_activity(schedule: dict, scheduler: dict, runner: dict,
                      live: dict, evidence: dict, *, now: float) -> dict:
    """Liveness is recent activity, not a passed historical acceptance cohort."""
    try:
        interval = float(schedule.get("interval_minutes", 5)) * 60
        if not math.isfinite(interval) or not 60 <= interval <= 86400:
            interval = 300
    except (TypeError, ValueError):
        interval = 300
    heartbeat_age = _age(scheduler.get("heartbeat_at"), now)
    runner_age = _age(runner.get("heartbeat_at"), now)
    live_age = _age(live.get("timestamp"), now)
    evidence_age = _age(evidence.get("timestamp"), now)
    enabled = schedule.get("enabled")
    recent = lambda age, limit: age is not None and age <= limit
    scheduler_fresh = (scheduler.get("status") == "running"
                       and recent(heartbeat_age, 60))
    newer_runner = (runner_age is not None and
                    (heartbeat_age is None or runner_age < heartbeat_age))
    lane_state = scheduler.get("lane_state")
    latest_live = lane_state.get("live") if isinstance(lane_state, dict) else {}
    latest_live = latest_live if isinstance(latest_live, dict) else {}
    failed_age = _age(latest_live.get("heartbeat_at"), now)
    live_failed = (latest_live.get("status") in {"failed", "deadline_exceeded"}
                   and failed_age is not None
                   and (live_age is None or failed_age <= live_age))
    if enabled is False:
        status, reason = "paused", "Learning is disabled in the local schedule."
    elif newer_runner and runner.get("status") == "failed":
        status, reason = "error", "Latest scheduled launcher failed."
    elif scheduler_fresh and live_failed:
        status, reason = "degraded", "Supervisor is active; latest live attempt failed."
    elif (scheduler_fresh and recent(live_age, 90)
          and live.get("status") == "deferred" and live.get("controlled_deferral") is True):
        status, reason = "deferred", "Supervisor is active; latest live attempt yielded within its safety budget."
    elif scheduler_fresh and recent(live_age, 90) and live.get("status") == "complete":
        status, reason = "active", "Recent supervisor heartbeat and completed live work."
    elif ((newer_runner and runner.get("status") in {"initializing", "running"}
           and recent(runner_age, 60)) or
          (scheduler_fresh and recent(_age(scheduler.get("started_at"), now), 90))):
        status, reason = "recovering", "Launcher started; waiting for recent completed live work."
    elif (enabled is True and scheduler.get("status") == "complete"
          and recent(heartbeat_age, interval + 60)):
        status, reason = "waiting", "Supervisor window ended; waiting for the next scheduled run."
    elif scheduler_fresh:
        status, reason = "stalled", "Supervisor heartbeat is fresh but live progress is overdue."
    elif heartbeat_age is not None or runner_age is not None:
        status, reason = "stale", "No recent supervisor activity; check sleep, network and task status."
    else:
        status, reason = "unknown", "Current learner telemetry is unavailable."

    marks = evidence.get("shadow_path_marks")
    marks = marks if isinstance(marks, dict) else {}
    observed = marks.get("observed")
    observed = (observed if isinstance(observed, (int, float))
                and math.isfinite(observed) and observed >= 0 else None)
    evidence_limit = 2 * interval + 90  # Background lanes share bounded windows.
    pressure = scheduler.get("backfill_pressure")
    recovery_priority = (scheduler_fresh and isinstance(pressure, dict)
                         and pressure.get("priority") is True)
    service = scheduler.get("evidence_service")
    service = service if isinstance(service, dict) else {}
    if status == "paused":
        evidence_status = "paused"
    elif evidence_age is None:
        evidence_status = "unknown"
    elif evidence_age > evidence_limit:
        evidence_status = "stale"
    elif evidence.get("status") != "complete":
        evidence_status = "error"
    elif observed is not None and observed > 0:
        evidence_status = "collecting"
    elif (marks.get("stop_reason") == "selected_batch_drained"
          and marks.get("selected") == 0
          and isinstance(marks.get("limit"), (int, float))
          and marks["limit"] > 0
          and marks.get("more_due_available") is False):
        evidence_status = "no_eligible_work"
    else:
        evidence_status = "deferred"
    return {
        "status": status, "reason": reason, "checked_at": now,
        "schedule_enabled": enabled, "interval_seconds": interval,
        "scheduler_age_seconds": heartbeat_age, "live_age_seconds": live_age,
        "live_status": live.get("status"),
        "evidence": {
            "status": evidence_status, "age_seconds": evidence_age,
            "stale_after_seconds": evidence_limit,
            "observed_last_batch": observed,
            "stop_reason": marks.get("stop_reason"),
            "more_due_available": marks.get("more_due_available"),
            "scheduling_constraint": "backfill_recovery_priority" if recovery_priority else None,
            "fair_service_due": service.get("due"),
            "fair_service_interval_seconds": service.get("interval_seconds"),
        },
        "historical_cohort_is_not_liveness": True,
        "paper_only": True, "live_execution_enabled": False,
    }


def learner_activity(root: str | Path, *, now: float | None = None) -> dict:
    root = Path(root)
    return classify_activity(
        *[_read(root / name) for name in (
            "schedule.json", "scheduler_status.json", "runner_status.json",
            "live_lane_summary.json", "evidence_lane_summary.json")],
        now=time.time() if now is None else now,
    )
