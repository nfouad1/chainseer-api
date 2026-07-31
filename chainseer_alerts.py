"""chainseer_alerts.py — outbound push notifications for hard-stop and
high-conviction analysis events.

Today, insight only exists if someone is looking at a dashboard. This module
lets any adapter (base/solana/pons) or the API push a bounded, best-effort
notification to a webhook (Discord, Slack-compatible, Telegram, or a plain
JSON endpoint) when something worth a human's attention happens, without
requiring the caller to poll.

Design constraints, matching the rest of the project's safety posture:

- Opt-in only. If CHAINSEER_ALERT_WEBHOOK_URL is not set, every call here is a
  silent no-op — no accidental outbound network calls in test/dev environments.
- Fail open. A broken webhook (DNS failure, 500, timeout) must never raise
  into the caller and must never block or slow down the analysis pipeline
  beyond a small bounded timeout. Alerting is a side channel, not a dependency.
- Credential-safe. The configured webhook URL is never included in logs,
  exceptions, or the returned result -- only a boolean success flag and a
  redacted host component, consistent with how RPC endpoints are handled
  elsewhere in this codebase (see SOLANA_PROTOTYPE.md "credential-safe
  endpoint origins").
- Rate-limited per (chain, token, event_type). A flapping token should not be
  able to spam the channel; a cooldown window collapses repeats.
- No new hard dependency: uses `requests`, already a project dependency
  (requirements-api.txt) and already the HTTP client used by chainseer.py's
  RobinhoodRPC.
"""

from __future__ import annotations

import os
import threading
import time
from typing import Any
from urllib.parse import urlparse

import requests

from chainseer_core import utc_now

DEFAULT_TIMEOUT_SECONDS = 5
DEFAULT_COOLDOWN_SECONDS = 900  # 15 minutes per (chain, token, event_type)

_cooldown_lock = threading.Lock()
_last_sent: dict[tuple[str, str, str], float] = {}


def _redacted_host(url: str) -> str:
    """Return only the scheme+host of a webhook URL, never the path/query
    (which usually carries the secret token for Discord/Slack-style webhooks)."""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.hostname or 'unknown'}"
    except Exception:
        return "unknown"


def _cooldown_key(chain: str, token_address: str, event_type: str) -> tuple[str, str, str]:
    return (chain.lower(), (token_address or "").lower(), event_type)


def _in_cooldown(chain: str, token_address: str, event_type: str, cooldown_seconds: float) -> bool:
    if cooldown_seconds <= 0:
        return False
    key = _cooldown_key(chain, token_address, event_type)
    now = time.monotonic()
    with _cooldown_lock:
        last = _last_sent.get(key)
        if last is not None and (now - last) < cooldown_seconds:
            return True
        _last_sent[key] = now
        return False


def _format_payload(fmt: str, event: dict[str, Any]) -> dict[str, Any]:
    """Shape the outbound JSON body for the configured webhook flavor.
    `generic` sends the raw event; `discord`/`slack` wrap it as chat-message
    content so it renders directly in those clients without a bot/app setup.
    """
    summary = event.get("summary") or "Chainseer alert"
    lines = [f"**{summary}**"]
    if event.get("chain"):
        lines.append(f"chain: {event['chain']}")
    if event.get("token_address"):
        lines.append(f"token: {event['token_address']}")
    if event.get("risk_level"):
        lines.append(f"risk_level: {event['risk_level']}")
    if event.get("score") is not None:
        lines.append(f"score: {event['score']}")
    if event.get("hard_stops"):
        lines.append("hard_stops: " + ", ".join(event["hard_stops"]))
    text = "\n".join(lines)

    if fmt in ("discord", "slack"):
        return {"content": text}
    if fmt == "telegram":
        return {"text": text, "parse_mode": "Markdown"}
    return dict(event)


def send_alert(
    event: dict[str, Any],
    *,
    chain: str,
    token_address: str = "",
    event_type: str = "hard_stop",
    webhook_url: str | None = None,
    webhook_format: str | None = None,
    cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Best-effort push of `event` to the configured webhook.

    Always returns a result dict and never raises. Callers should treat
    alerting as fire-and-forget: check `result["sent"]` only if they care.
    """
    webhook_url = webhook_url or os.environ.get("CHAINSEER_ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return {"sent": False, "reason": "no_webhook_configured"}

    if _in_cooldown(chain, token_address, event_type, cooldown_seconds):
        return {"sent": False, "reason": "cooldown", "host": _redacted_host(webhook_url)}

    fmt = (webhook_format or os.environ.get("CHAINSEER_ALERT_WEBHOOK_FORMAT", "generic")).strip().lower()
    body = _format_payload(fmt, {**event, "chain": chain, "token_address": token_address,
                                  "event_type": event_type, "sealed_at": utc_now()})

    try:
        response = requests.post(webhook_url, json=body, timeout=timeout)
        ok = 200 <= response.status_code < 300
        return {
            "sent": ok,
            "status_code": response.status_code,
            "host": _redacted_host(webhook_url),
        }
    except requests.exceptions.RequestException as exc:
        # Fail open: alerting must never break the analysis pipeline.
        return {
            "sent": False,
            "reason": "request_error",
            "error_type": type(exc).__name__,
            "host": _redacted_host(webhook_url),
        }


def alert_on_decision(
    *,
    chain: str,
    token_address: str,
    symbol: str,
    risk_level: str,
    score: float,
    hard_stops: list[str] | None = None,
    high_conviction_threshold: float | None = None,
    webhook_url: str | None = None,
    webhook_format: str | None = None,
    cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
) -> dict[str, Any]:
    """Convenience wrapper an adapter calls after producing a risk decision.

    Fires when either:
      - a hard stop is present (event_type="hard_stop"), or
      - `high_conviction_threshold` is set and `score` meets/exceeds it
        (event_type="high_conviction").
    No-ops (returns sent=False, reason=no_trigger) otherwise, and no-ops
    entirely if no webhook is configured, matching every other opt-in
    behavior in this project.
    """
    hard_stops = hard_stops or []
    if hard_stops:
        event_type = "hard_stop"
        summary = f"{symbol or token_address}: hard stop ({risk_level})"
    elif high_conviction_threshold is not None and score >= high_conviction_threshold:
        event_type = "high_conviction"
        summary = f"{symbol or token_address}: high-conviction score {score:.1f}"
    else:
        return {"sent": False, "reason": "no_trigger"}

    event = {
        "summary": summary,
        "symbol": symbol,
        "risk_level": risk_level,
        "score": score,
        "hard_stops": hard_stops,
    }
    return send_alert(
        event,
        chain=chain,
        token_address=token_address,
        event_type=event_type,
        webhook_url=webhook_url,
        webhook_format=webhook_format,
        cooldown_seconds=cooldown_seconds,
    )
