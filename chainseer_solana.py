"""Solana-native Chainseer observer and risk-managed paper/shadow trader.

The first supported launch ecosystem is Pump.fun. Launches are admitted only
when they are decoded from the Pump program's on-chain CreateEvent. Jupiter
Swap v2 supplies two-way route evidence. This module deliberately has no
private-key loading, signing, transaction submission, or broadcast path.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import random
import re
import struct
import threading
import time
from collections import OrderedDict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests

from chainseer import (
    _get_skill_dir,
    _load_skill_module,
    _load_timechain_module,
    ensure_utf8_runtime,
)

REQUEST_EXCEPTION = getattr(
    getattr(requests, "exceptions", None), "RequestException", OSError
)

PUBLIC_SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"


def _windows_user_environment(name: str) -> str | None:
    """Read one user-scoped Windows setting without logging its value."""
    if os.name != "nt":
        return None
    try:
        import winreg

        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, _kind = winreg.QueryValueEx(key, name)
    except (ImportError, OSError):
        return None
    text = str(value or "").strip()
    return text or None


def _environment_setting(name: str) -> str | None:
    """Prefer the process environment, then the persistent user environment."""
    value = str(os.environ.get(name) or "").strip()
    return value or _windows_user_environment(name)


SOLANA_RPC_URL = (
    _environment_setting("CHAINSEER_SOLANA_RPC_URL")
    or PUBLIC_SOLANA_RPC_URL
)
JUPITER_API_URL = "https://api.jup.ag"
DEXSCREENER_API_URL = "https://api.dexscreener.com"
PUMP_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
PUMP_AMM_PROGRAM_ID = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
PUMP_PUBLIC_DOCS_COMMIT = "9c82f61cb711b044a17f770ab8ce9f9bdf78f333"
TOKEN_PROGRAM_ID = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM_ID = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
WRAPPED_SOL_MINT = "So11111111111111111111111111111111111111112"
SOLANA_USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
PUMP_CREATE_EVENT_DISCRIMINATOR = bytes([27, 114, 169, 77, 222, 235, 99, 118])
PUMP_BONDING_CURVE_DISCRIMINATOR = bytes([23, 183, 248, 55, 96, 216, 172, 96])
PUMP_AMM_POOL_DISCRIMINATOR = bytes([241, 154, 109, 4, 17, 177, 109, 188])
SCHEMA_VERSION = 1
RPC_HEALTH_SCHEMA_VERSION = 2
REFLECTION_ANALYSIS_INTERVAL = 200
REFLECTION_MIN_SECONDS = 6 * 60 * 60

RISKY_TOKEN_2022_EXTENSIONS = {
    "confidentialtransfermint",
    "defaultaccountstate",
    "permanentdelegate",
    "pausableconfig",
    "nontransferable",
    "transferfeeconfig",
    "transferhook",
}


class LiveExecutionDisabledError(RuntimeError):
    """Raised whenever a caller attempts to cross the paper-only boundary."""


class InfrastructureIndeterminateError(RuntimeError):
    """A required external observation could not be completed."""


class ConfiguredSolanaRpcRequiredError(RuntimeError):
    """A previously configured private RPC must not silently become public."""


class ReflectionCheckpointPending(RuntimeError):
    """The learner is paused until a sealed reflection is acknowledged."""


import functools

from chainseer_core import (
    utc_now as _utc_now,
    canonical_json as _canonical_json_impl,
    safe_float as _safe_float,
    safe_int as _safe_int,
    atomic_json_write as _atomic_json,
    read_json as _read_json,
)

# This adapter's original _canonical_json used ensure_ascii=False with no
# `default=str` fallback (stricter: raises on a non-JSON-serializable value).
# Bind chainseer_core.canonical_json to reproduce that exact behavior so
# historical event_hash values in solana_chain/ keep re-verifying.
_canonical_json = functools.partial(_canonical_json_impl, ensure_ascii=False, default=None)

# catalog.json and analysis_index.json accumulate one entry per token ever
# observed/analyzed and are fully read+rewritten every learn cycle -- on an
# active launchpad this grows without bound for as long as the process runs
# (the same failure shape as the OOM-causing watcher churn already fixed
# elsewhere). Prune anything older than this on every write. Well beyond
# SolanaShadowPolicy.maximum_hold_seconds (6h default) so open positions and
# their candidates are never pruned out from under an in-flight mark/exit.
CATALOG_RETENTION_SECONDS = _safe_int(
    _environment_setting("CHAINSEER_SOLANA_CATALOG_RETENTION_DAYS"), 30
) * 86_400


def _timestamp(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _public_rpc_endpoint(value: str | None) -> str:
    """Return only a credential-safe RPC origin."""
    try:
        parsed = urlparse(value or "")
    except ValueError:
        return "configured" if value else "not configured"
    if not parsed.scheme or not parsed.hostname:
        return "configured" if value else "not configured"
    port = f":{parsed.port}" if parsed.port else ""
    return f"{parsed.scheme}://{parsed.hostname}{port}"


def _rpc_provider(value: str | None) -> str:
    try:
        host = (urlparse(value or "").hostname or "").lower()
    except ValueError:
        return "custom"
    for name in ("helius", "alchemy", "quicknode", "chainstack"):
        if name in host:
            return name
    if host == "api.mainnet-beta.solana.com":
        return "solana_public"
    return "custom"


def _rpc_endpoint_id(value: str | None) -> str:
    """Stable, credential-safe identity for endpoint telemetry."""
    public = _public_rpc_endpoint(value)
    return hashlib.sha256(public.encode("utf-8")).hexdigest()[:16]


def _redact_sensitive_text(value) -> str:
    """Remove endpoint paths, query strings, and common secret parameters."""
    text = re.sub(
        r"https?://[^\s\"'<>]+",
        lambda match: _public_rpc_endpoint(match.group(0)),
        str(value or ""),
    )
    return re.sub(
        r"(?i)\b(api[-_]?key|token|secret|password)=([^&\s]+)",
        r"\1=REDACTED",
        text,
    )


def _b58encode(raw: bytes) -> str:
    alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
    number = int.from_bytes(raw, "big")
    encoded = ""
    while number:
        number, remainder = divmod(number, 58)
        encoded = alphabet[remainder] + encoded
    leading = len(raw) - len(raw.lstrip(b"\0"))
    return "1" * leading + (encoded or ("" if leading else "1"))


class _BorshReader:
    def __init__(self, raw: bytes):
        self.raw = raw
        self.offset = 0

    def take(self, size: int) -> bytes:
        end = self.offset + size
        if end > len(self.raw):
            raise ValueError("truncated Borsh payload")
        value = self.raw[self.offset:end]
        self.offset = end
        return value

    def u8(self) -> int:
        return self.take(1)[0]

    def u16(self) -> int:
        return struct.unpack("<H", self.take(2))[0]

    def u32(self) -> int:
        return struct.unpack("<I", self.take(4))[0]

    def u64(self) -> int:
        return struct.unpack("<Q", self.take(8))[0]

    def i64(self) -> int:
        return struct.unpack("<q", self.take(8))[0]

    def string(self, maximum: int = 4096) -> str:
        size = self.u32()
        if size > maximum:
            raise ValueError(f"Borsh string exceeds {maximum} bytes")
        return self.take(size).decode("utf-8", errors="replace")

    def pubkey(self) -> str:
        return _b58encode(self.take(32))


@dataclass(frozen=True)
class SolanaLaunchCandidate:
    signature: str
    slot: int
    block_time: int
    name: str
    symbol: str
    uri: str
    mint: str
    bonding_curve: str
    user: str
    creator: str
    token_program: str
    virtual_token_reserves: int
    virtual_quote_reserves: int
    real_token_reserves: int
    token_total_supply: int
    is_mayhem_mode: bool = False
    is_cashback_enabled: bool = False
    launch_ecosystem: str = "pump_fun"

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: dict) -> "SolanaLaunchCandidate":
        names = cls.__dataclass_fields__
        return cls(**{key: value[key] for key in names if key in value})


@dataclass
class JupiterQuote:
    input_mint: str
    output_mint: str
    in_amount: int
    out_amount: int
    router: str | None
    price_impact_pct: float | None
    fee_bps: int | None
    request_id: str | None
    transaction_state: str
    raw: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        value = asdict(self)
        value["raw"] = {
            key: self.raw.get(key)
            for key in (
                "inAmount",
                "outAmount",
                "router",
                "mode",
                "feeBps",
                "priceImpactPct",
                "requestId",
                "errorCode",
                "errorMessage",
                "lastValidBlockHeight",
                "expireAt",
            )
            if key in self.raw
        }
        return value


@dataclass
class SolanaRiskDecision:
    score: float
    risk_level: str
    evidence_state: str
    shadow_entry_allowed: bool
    hard_stops: list[str]
    warnings: list[str]
    infrastructure_errors: list[str]
    coverage: dict
    origin: dict
    mint: dict
    bonding_curve: dict
    concentration: dict
    creator_evidence: dict
    convergence_evidence: dict
    market: dict
    execution_evidence: dict
    graduation: dict = field(default_factory=dict)
    cohort: str = "launch_observation"
    admission_state: str = "graduation_pending"
    analyzed_at: str = field(default_factory=_utc_now)
    timechain_ring: int | None = None

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class SolanaRiskPolicy:
    amount_sol: float = 0.01
    minimum_age_seconds: int = 60
    maximum_top1_circulating_pct: float = 20.0
    maximum_top10_circulating_pct: float = 65.0
    minimum_roundtrip_retention_pct: float = 72.0
    maximum_buy_price_impact_pct: float = 12.0
    minimum_graduated_market_age_seconds: int = 60
    # Minimum fraction of total supply that must be outside the bonding curve
    # before holder concentration is treated as a meaningful risk signal. On a
    # brand-new Pump.fun launch the curve holds ~99.7% of supply, so the small
    # circulating sliver is dominated by whichever wallet bought first -- making
    # any holder % misleading until supply has actually distributed. Below this
    # floor the concentration hard-stops are suppressed and the token is marked
    # pre_distribution (unjudgeable on this axis yet, NOT safe). Calibrated to
    # the ring-94 dataset: median circulating was 0.32% of supply, and 5.0%
    # correctly captures the bulk of false positives while letting genuinely
    # distributed tokens be judged normally.
    minimum_circulating_pct_of_supply: float = 5.0
    # Creator-history cadence thresholds. These measure the deployer's recent
    # Pump.fun launch BEHAVIOUR (an objective risk-correlated fact), not a
    # verdict on any prior token. Industrialized deployment cadence (many
    # tokens in a short window) is empirically token-farm behaviour. Thresholds
    # set high enough that only genuine farms trigger (legitimate launchpad
    # services rarely exceed these), with a warning tier below the hard-stop.
    # Page size for each getSignaturesForAddress call against the creator's
    # wallet. A single page is not enough recall on its own -- see
    # creator_history_max_pages below.
    creator_history_signature_limit: int = 30
    # getSignaturesForAddress returns EVERY signature for the creator wallet,
    # not just Pump.fun interactions -- a deployer who also trades/swaps on
    # the same wallet burns through a single page with unrelated activity,
    # silently hiding genuine prior launches from the scan. Paging backwards
    # (like PumpFunObserver.sync) until the lookback window is covered fixes
    # the recall gap; these two caps bound the worst-case RPC cost of doing
    # so (a wallet with heavy unrelated activity within the window).
    creator_history_max_pages: int = 6
    creator_history_max_transactions_scanned: int = 180
    creator_history_window_hours: float = 72.0
    creator_history_warning_count: int = 5
    creator_history_hard_stop_count: int = 10
    creator_history_hard_stop_window_hours: float = 24.0

    def __post_init__(self):
        if self.amount_sol <= 0:
            raise ValueError("amount_sol must be positive")
        if not 0 < self.minimum_roundtrip_retention_pct <= 100:
            raise ValueError("minimum_roundtrip_retention_pct must be in (0, 100]")
        if not 0 < self.minimum_circulating_pct_of_supply <= 100:
            raise ValueError("minimum_circulating_pct_of_supply must be in (0, 100]")
        if self.minimum_graduated_market_age_seconds < 0:
            raise ValueError("minimum_graduated_market_age_seconds must be non-negative")
        if self.creator_history_signature_limit <= 0:
            raise ValueError("creator_history_signature_limit must be positive")
        if self.creator_history_max_pages <= 0:
            raise ValueError("creator_history_max_pages must be positive")
        if self.creator_history_max_transactions_scanned <= 0:
            raise ValueError("creator_history_max_transactions_scanned must be positive")
        if self.creator_history_window_hours <= 0:
            raise ValueError("creator_history_window_hours must be positive")
        if not 0 < self.creator_history_warning_count < self.creator_history_hard_stop_count:
            raise ValueError(
                "creator_history_warning_count must be in "
                "(0, creator_history_hard_stop_count)"
            )


@dataclass(frozen=True)
class SolanaShadowPolicy:
    amount_sol: float = 0.01
    maximum_positions: int = 25
    stop_loss_multiple: float = 0.65
    take_profit_multiple: float = 3.0
    maximum_hold_seconds: int = 6 * 60 * 60

    def __post_init__(self):
        if self.amount_sol <= 0 or self.maximum_positions < 1:
            raise ValueError("invalid shadow policy")
        if not 0 < self.stop_loss_multiple < 1:
            raise ValueError("stop_loss_multiple must be below one")
        if self.take_profit_multiple <= 1:
            raise ValueError("take_profit_multiple must exceed one")


@dataclass(frozen=True)
class SolanaPromotionPolicy:
    minimum_observations: int = 200
    minimum_closed_positions: int = 50
    minimum_profitable_positions: int = 5
    minimum_winner_rate_pct: float = 15.0
    maximum_drawdown_pct: float = 35.0
    minimum_quote_coverage_pct: float = 90.0
    minimum_unsigned_assembly_coverage_pct: float = 90.0
    maximum_indeterminate_rate_pct: float = 10.0


class HashLedger:
    """Append-only, hash-linked JSONL evidence ledger."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._tail_hash: str | None = None
        self._length: int | None = None

    def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        rows = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                rows.append(json.loads(line))
        return rows

    def _tail_state(self) -> tuple[str, int]:
        if self._tail_hash is None or self._length is None:
            previous_hash = "0" * 64
            length = 0
            if self.path.exists():
                for line in self.path.read_text(encoding="utf-8").splitlines():
                    if line.strip():
                        previous_hash = json.loads(line)["event_hash"]
                        length += 1
            self._tail_hash = previous_hash
            self._length = length
        return self._tail_hash, self._length

    def append(self, event_type: str, payload: dict) -> dict:
        previous_hash, length = self._tail_state()
        event = {
            "index": length,
            "timestamp": _utc_now(),
            "event_type": event_type,
            "payload": payload,
            "previous_hash": previous_hash,
        }
        event["event_hash"] = hashlib.sha256(
            _canonical_json(event).encode("utf-8")
        ).hexdigest()
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(_canonical_json(event) + "\n")
        self._tail_hash = event["event_hash"]
        self._length = length + 1
        return event

    def verify(self) -> tuple[bool, str]:
        previous = "0" * 64
        try:
            rows = self.load()
        except (OSError, ValueError) as exc:
            return False, f"ledger unreadable: {exc}"
        for index, event in enumerate(rows):
            if event.get("index") != index:
                return False, f"index mismatch at {index}"
            if event.get("previous_hash") != previous:
                return False, f"previous hash mismatch at {index}"
            clone = dict(event)
            actual = clone.pop("event_hash", None)
            expected = hashlib.sha256(
                _canonical_json(clone).encode("utf-8")
            ).hexdigest()
            if actual != expected:
                return False, f"event hash mismatch at {index}"
            previous = actual
        return True, f"verified {len(rows)} events"


class SolanaRPC:
    def __init__(
        self,
        url: str = SOLANA_RPC_URL,
        *,
        urls: Iterable[str] | None = None,
        timeout: float = 15.0,
        session: requests.Session | None = None,
        minimum_request_interval: float | None = None,
        max_retries: int = 5,
        circuit_failure_threshold: int = 3,
        circuit_cooldown_seconds: float = 30.0,
        sleep_fn=None,
        monotonic_fn=None,
        jitter_fn=None,
    ):
        configured = [url]
        if urls:
            configured.extend(urls)
        fallback_text = (
            _environment_setting("CHAINSEER_SOLANA_RPC_FALLBACK_URLS") or ""
        )
        configured.extend(
            item.strip()
            for item in fallback_text.replace("\n", ";").split(";")
            if item.strip()
        )
        unique = []
        for item in configured:
            if item and item not in unique:
                unique.append(item)
        self.urls = unique or [PUBLIC_SOLANA_RPC_URL]
        self.url = self.urls[0]
        self.timeout = timeout
        self.session = session or requests.Session()
        self.max_retries = max(0, int(max_retries))
        self.circuit_failure_threshold = max(
            1, int(circuit_failure_threshold)
        )
        self.circuit_cooldown_seconds = max(
            0.0, float(circuit_cooldown_seconds)
        )
        self._sleep = sleep_fn or time.sleep
        self._monotonic = monotonic_fn or time.monotonic
        self._jitter = jitter_fn or (
            lambda maximum: random.uniform(0.0, maximum)
        )
        self._endpoint_states = []
        for index, endpoint in enumerate(self.urls):
            public = _public_rpc_endpoint(endpoint)
            interval = (
                0.25
                if minimum_request_interval is None
                and public == "https://api.mainnet-beta.solana.com"
                else max(0.0, minimum_request_interval or 0.0)
            )
            self._endpoint_states.append(
                {
                    "url": endpoint,
                    "public": public,
                    "provider": _rpc_provider(endpoint),
                    "label": f"endpoint_{index + 1}",
                    "minimum_request_interval": interval,
                    "last_request_at": 0.0,
                    "consecutive_failures": 0,
                    "circuit_open_until": 0.0,
                    "attempts": 0,
                    "successes": 0,
                    "failures": 0,
                }
            )
        self._active_endpoint_index = 0
        self.attempts = 0
        self.successes = 0
        self.failures = 0
        self.retries = 0
        self.endpoint_switches = 0
        self.method_stats: dict[str, dict] = {}

    def _method_stat(self, method: str) -> dict:
        return self.method_stats.setdefault(
            method,
            {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "retries": 0,
                "status_codes": {},
            },
        )

    @staticmethod
    def _retry_after(response) -> float:
        value = getattr(response, "headers", {}).get("Retry-After")
        return max(0.0, _safe_float(value, 0.0))

    def _available_endpoint_indexes(self) -> list[int]:
        now = self._monotonic()
        count = len(self._endpoint_states)
        ordered = [
            (self._active_endpoint_index + offset) % count
            for offset in range(count)
        ]
        available = [
            index
            for index in ordered
            if self._endpoint_states[index]["circuit_open_until"] <= now
        ]
        if available:
            return available
        soonest = min(
            range(count),
            key=lambda index: self._endpoint_states[index][
                "circuit_open_until"
            ],
        )
        return [soonest]

    def _record_attempt_failure(
        self, endpoint: dict, method_stat: dict, status
    ) -> None:
        endpoint["failures"] += 1
        endpoint["consecutive_failures"] += 1
        method_stat["failures"] += 1
        if status is not None:
            key = str(status)
            method_stat["status_codes"][key] = (
                _safe_int(method_stat["status_codes"].get(key)) + 1
            )
        if endpoint["consecutive_failures"] >= self.circuit_failure_threshold:
            endpoint["circuit_open_until"] = (
                self._monotonic() + self.circuit_cooldown_seconds
            )

    def _call(self, method: str, params: list | None = None):
        method_stat = self._method_stat(method)
        last_status = None
        last_provider = "unknown"
        request_number = 0
        for retry_round in range(self.max_retries + 1):
            retry_after = 0.0
            round_retryable = False
            for endpoint_index in self._available_endpoint_indexes():
                endpoint = self._endpoint_states[endpoint_index]
                if request_number:
                    self.retries += 1
                    method_stat["retries"] += 1
                if endpoint_index != self._active_endpoint_index:
                    self.endpoint_switches += 1
                circuit_delay = (
                    endpoint["circuit_open_until"] - self._monotonic()
                )
                if circuit_delay > 0:
                    self._sleep(circuit_delay)
                delay = endpoint["minimum_request_interval"] - (
                    self._monotonic() - endpoint["last_request_at"]
                )
                if delay > 0:
                    self._sleep(delay)
                self.attempts += 1
                request_number += 1
                endpoint["attempts"] += 1
                method_stat["attempts"] += 1
                last_provider = endpoint["provider"]
                response = None
                try:
                    endpoint["last_request_at"] = self._monotonic()
                    response = self.session.post(
                        endpoint["url"],
                        json={
                            "jsonrpc": "2.0",
                            "id": self.attempts,
                            "method": method,
                            "params": params or [],
                        },
                        timeout=self.timeout,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if payload.get("error"):
                        error = payload.get("error") or {}
                        last_status = error.get("code")
                        self._record_attempt_failure(
                            endpoint, method_stat, last_status
                        )
                        round_retryable = round_retryable or last_status in {
                            -32004,
                            -32005,
                            -32007,
                            -32009,
                            -32603,
                        }
                        continue
                    endpoint["successes"] += 1
                    endpoint["consecutive_failures"] = 0
                    endpoint["circuit_open_until"] = 0.0
                    method_stat["successes"] += 1
                    method_stat["status_codes"]["200"] = (
                        _safe_int(method_stat["status_codes"].get("200")) + 1
                    )
                    self._active_endpoint_index = endpoint_index
                    self.successes += 1
                    return payload.get("result")
                except (REQUEST_EXCEPTION, OSError, ValueError) as exc:
                    response = getattr(exc, "response", None) or response
                    last_status = getattr(response, "status_code", None)
                    retry_after = max(
                        retry_after, self._retry_after(response)
                    )
                    round_retryable = (
                        round_retryable
                        or last_status is None
                        or last_status in {408, 425, 429, 500, 502, 503, 504}
                    )
                    self._record_attempt_failure(
                        endpoint, method_stat, last_status
                    )
            if retry_round < self.max_retries and round_retryable:
                backoff = min(8.0, 0.5 * (2**retry_round))
                self._sleep(
                    max(retry_after, backoff + self._jitter(backoff * 0.2))
                )
            else:
                break
        self.failures += 1
        status_text = (
            f"status {last_status}" if last_status is not None else "no status"
        )
        raise InfrastructureIndeterminateError(
            f"Solana RPC {method} unavailable after {request_number} "
            f"attempts ({status_text}, provider {last_provider})"
        )

    def get_signatures(
        self, address: str, *, limit: int, until: str | None = None,
        before: str | None = None,
    ) -> list[dict]:
        options = {"limit": max(1, min(int(limit), 1000)), "commitment": "confirmed"}
        if until:
            options["until"] = until
        if before:
            options["before"] = before
        return self._call("getSignaturesForAddress", [address, options]) or []

    def get_transaction(self, signature: str) -> dict | None:
        result = self._call(
            "getTransaction",
            [
                signature,
                {
                    "encoding": "jsonParsed",
                    "commitment": "confirmed",
                    "maxSupportedTransactionVersion": 0,
                },
            ],
        )
        if result is None:
            raise InfrastructureIndeterminateError(
                f"confirmed transaction {signature} is temporarily unavailable"
            )
        return result

    def get_account_info(self, address: str, *, encoding: str = "jsonParsed"):
        return self._call(
            "getAccountInfo", [address, {"encoding": encoding, "commitment": "confirmed"}]
        )

    def get_multiple_accounts(
        self, addresses: list[str], *, encoding: str = "jsonParsed"
    ):
        if not addresses:
            return {"value": []}
        return self._call(
            "getMultipleAccounts",
            [addresses, {"encoding": encoding, "commitment": "confirmed"}],
        )

    def get_token_supply(self, mint: str):
        return self._call("getTokenSupply", [mint, {"commitment": "confirmed"}])

    def get_token_largest_accounts(self, mint: str):
        return self._call(
            "getTokenLargestAccounts", [mint, {"commitment": "confirmed"}]
        )

    def get_token_accounts_by_mint(
        self, mint: str, token_program: str
    ) -> list[dict]:
        return self.get_program_accounts(
            token_program,
            filters=[{"memcmp": {"offset": 0, "bytes": mint}}],
            encoding="jsonParsed",
        )

    def get_program_accounts(
        self,
        program_id: str,
        *,
        filters: list[dict] | None = None,
        encoding: str = "base64",
    ) -> list[dict]:
        return self._call(
            "getProgramAccounts",
            [
                program_id,
                {
                    "encoding": encoding,
                    "commitment": "confirmed",
                    "filters": filters or [],
                },
            ],
        ) or []

    def health(self) -> dict:
        return {
            "rpc_url": _public_rpc_endpoint(self.url),
            "provider": _rpc_provider(self.url),
            "endpoint_count": len(self._endpoint_states),
            "active_endpoint": self._endpoint_states[
                self._active_endpoint_index
            ]["label"],
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "retries": self.retries,
            "endpoint_switches": self.endpoint_switches,
            "methods": json.loads(_canonical_json(self.method_stats)),
            "endpoints": [
                {
                    "label": endpoint["label"],
                    "endpoint_id": _rpc_endpoint_id(endpoint["public"]),
                    "rpc_url": endpoint["public"],
                    "provider": endpoint["provider"],
                    "attempts": endpoint["attempts"],
                    "successes": endpoint["successes"],
                    "failures": endpoint["failures"],
                    "consecutive_failures": endpoint[
                        "consecutive_failures"
                    ],
                    "circuit_open": endpoint["circuit_open_until"]
                    > self._monotonic(),
                }
                for endpoint in self._endpoint_states
            ],
            "updated_at": _utc_now(),
        }


_RPC_COUNTER_KEYS = (
    "attempts",
    "successes",
    "failures",
    "retries",
    "endpoint_switches",
)


def _rpc_health_delta(current: dict, previous: dict) -> dict:
    delta = {
        key: max(
            0,
            _safe_int(current.get(key)) - _safe_int(previous.get(key)),
        )
        for key in _RPC_COUNTER_KEYS
    }
    delta["methods"] = {}
    for method, values in (current.get("methods") or {}).items():
        old = (previous.get("methods") or {}).get(method, {})
        row = {
            key: max(
                0,
                _safe_int(values.get(key)) - _safe_int(old.get(key)),
            )
            for key in ("attempts", "successes", "failures", "retries")
        }
        row["status_codes"] = {
            status: max(
                0,
                _safe_int(count)
                - _safe_int((old.get("status_codes") or {}).get(status)),
            )
            for status, count in (values.get("status_codes") or {}).items()
        }
        delta["methods"][method] = row
    delta["endpoints"] = {}
    previous_endpoints = {
        row.get("endpoint_id") or _rpc_endpoint_id(row.get("rpc_url")): row
        for row in (previous.get("endpoints") or [])
    }
    for values in current.get("endpoints") or []:
        label = values.get("label")
        if not label:
            continue
        endpoint_id = values.get("endpoint_id") or _rpc_endpoint_id(
            values.get("rpc_url")
        )
        old = previous_endpoints.get(endpoint_id, {})
        row = {
            "label": label,
            "endpoint_id": endpoint_id,
        }
        row.update({
            key: max(
                0,
                _safe_int(values.get(key)) - _safe_int(old.get(key)),
            )
            for key in ("attempts", "successes", "failures")
        })
        delta["endpoints"][endpoint_id] = row
    return delta


def _merge_rpc_health(existing: dict, delta: dict, current: dict) -> dict:
    legacy_aggregate = existing.get("legacy_aggregate")
    if existing and _safe_int(existing.get("telemetry_schema_version")) < 2:
        legacy_aggregate = {
            "attribution": "legacy_endpoint_identity_unverifiable",
            "rpc_url": _public_rpc_endpoint(existing.get("rpc_url")),
            "provider": existing.get("provider") or "unknown",
            **{
                key: _safe_int(existing.get(key))
                for key in _RPC_COUNTER_KEYS
            },
            "methods": existing.get("methods") or {},
            "endpoints": existing.get("endpoints") or [],
            "archived_at": _utc_now(),
        }
        existing = {}
    merged = {
        "schema_version": SCHEMA_VERSION,
        "telemetry_schema_version": RPC_HEALTH_SCHEMA_VERSION,
        "rpc_url": _public_rpc_endpoint(
            current.get("rpc_url") or existing.get("rpc_url")
        ),
        "provider": current.get("provider")
        or existing.get("provider")
        or "unknown",
        "endpoint_count": _safe_int(
            current.get("endpoint_count"),
            _safe_int(existing.get("endpoint_count"), 1),
        ),
        "active_endpoint": current.get("active_endpoint")
        or existing.get("active_endpoint"),
        "updated_at": _utc_now(),
    }
    for key in _RPC_COUNTER_KEYS:
        merged[key] = _safe_int(existing.get(key)) + _safe_int(
            delta.get(key)
        )
    methods = json.loads(_canonical_json(existing.get("methods") or {}))
    for method, values in (delta.get("methods") or {}).items():
        row = methods.setdefault(
            method,
            {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "retries": 0,
                "status_codes": {},
            },
        )
        for key in ("attempts", "successes", "failures", "retries"):
            row[key] = _safe_int(row.get(key)) + _safe_int(values.get(key))
        for status, count in (values.get("status_codes") or {}).items():
            row.setdefault("status_codes", {})[status] = _safe_int(
                row["status_codes"].get(status)
            ) + _safe_int(count)
    merged["methods"] = methods
    old_endpoints = {
        row.get("endpoint_id") or _rpc_endpoint_id(row.get("rpc_url")): row
        for row in (existing.get("endpoints") or [])
    }
    current_endpoints = {
        row.get("endpoint_id") or _rpc_endpoint_id(row.get("rpc_url")): row
        for row in (current.get("endpoints") or [])
    }
    for endpoint_id, values in (delta.get("endpoints") or {}).items():
        live = current_endpoints.get(endpoint_id) or {}
        label = live.get("label") or values.get("label") or "endpoint"
        old = old_endpoints.setdefault(
            endpoint_id,
            {
                "label": label,
                "endpoint_id": endpoint_id,
                "rpc_url": _public_rpc_endpoint(
                    live.get("rpc_url")
                ),
                "provider": live.get("provider", "unknown"),
                "attempts": 0,
                "successes": 0,
                "failures": 0,
            },
        )
        for key in ("attempts", "successes", "failures"):
            old[key] = _safe_int(old.get(key)) + _safe_int(values.get(key))
        old["label"] = label
        old["endpoint_id"] = endpoint_id
        old["rpc_url"] = _public_rpc_endpoint(
            live.get("rpc_url") or old.get("rpc_url")
        )
        old["provider"] = live.get("provider") or old.get("provider")
        old["consecutive_failures"] = _safe_int(
            live.get("consecutive_failures")
        )
        old["circuit_open"] = bool(live.get("circuit_open"))
    merged["endpoints"] = sorted(
        old_endpoints.values(),
        key=lambda row: (
            row.get("provider") or "",
            row.get("rpc_url") or "",
        ),
    )
    active_ids = set(current_endpoints)
    active_rows = [
        row for endpoint_id, row in old_endpoints.items()
        if endpoint_id in active_ids
    ]
    merged["current_segment"] = {
        "endpoint_ids": sorted(active_ids),
        "rpc_url": _public_rpc_endpoint(current.get("rpc_url")),
        "provider": current.get("provider") or "unknown",
        "attempts": sum(_safe_int(row.get("attempts")) for row in active_rows),
        "successes": sum(_safe_int(row.get("successes")) for row in active_rows),
        "failures": sum(_safe_int(row.get("failures")) for row in active_rows),
    }
    if len({row.get("provider") for row in old_endpoints.values()}) > 1:
        merged["provider"] = "mixed_history"
    if legacy_aggregate:
        merged["legacy_aggregate"] = legacy_aggregate
    merged["last_cycle"] = json.loads(_canonical_json(delta))
    return merged


class PumpFunObserver:
    def __init__(self, rpc: SolanaRPC, root: str | Path, ledger: HashLedger):
        self.rpc = rpc
        self.root = Path(root)
        self.catalog_path = self.root / "catalog.json"
        self.cursor_path = self.root / "observer_cursor.json"
        self.ledger = ledger

    @staticmethod
    def decode_create_event(
        encoded: str, *, signature: str, slot: int, block_time: int
    ) -> SolanaLaunchCandidate | None:
        try:
            raw = base64.b64decode(encoded, validate=True)
            if not raw.startswith(PUMP_CREATE_EVENT_DISCRIMINATOR):
                return None
            reader = _BorshReader(raw[len(PUMP_CREATE_EVENT_DISCRIMINATOR):])
            name = reader.string(512)
            symbol = reader.string(128)
            uri = reader.string(4096)
            mint = reader.pubkey()
            bonding_curve = reader.pubkey()
            user = reader.pubkey()
            creator = reader.pubkey()
            event_time = reader.i64()
            virtual_token = reader.u64()
            virtual_quote = reader.u64()
            real_token = reader.u64()
            total_supply = reader.u64()
            token_program = reader.pubkey()
            mayhem = bool(reader.u8()) if reader.offset < len(reader.raw) else False
            cashback = bool(reader.u8()) if reader.offset < len(reader.raw) else False
            return SolanaLaunchCandidate(
                signature=signature,
                slot=slot,
                block_time=event_time or block_time,
                name=name,
                symbol=symbol,
                uri=uri,
                mint=mint,
                bonding_curve=bonding_curve,
                user=user,
                creator=creator,
                token_program=token_program,
                virtual_token_reserves=virtual_token,
                virtual_quote_reserves=virtual_quote,
                real_token_reserves=real_token,
                token_total_supply=total_supply,
                is_mayhem_mode=mayhem,
                is_cashback_enabled=cashback,
            )
        except (ValueError, struct.error):
            return None

    @classmethod
    def decode_transaction(
        cls, signature_row: dict, transaction: dict | None
    ) -> list[SolanaLaunchCandidate]:
        if not transaction or (transaction.get("meta") or {}).get("err") is not None:
            return []
        message = ((transaction.get("transaction") or {}).get("message") or {})
        keys = message.get("accountKeys") or []
        addresses = {
            item.get("pubkey") if isinstance(item, dict) else item for item in keys
        }
        if PUMP_PROGRAM_ID not in addresses:
            return []
        output = []
        for log in (transaction.get("meta") or {}).get("logMessages") or []:
            if not isinstance(log, str) or not log.startswith("Program data: "):
                continue
            candidate = cls.decode_create_event(
                log.split("Program data: ", 1)[1].strip(),
                signature=signature_row["signature"],
                slot=_safe_int(signature_row.get("slot")),
                block_time=_safe_int(
                    transaction.get("blockTime") or signature_row.get("blockTime")
                ),
            )
            if candidate:
                output.append(candidate)
        return output

    def sync(
        self,
        *,
        signature_limit: int = 100,
        slot_span: int | None = None,
        max_pages: int = 10,
    ) -> list[SolanaLaunchCandidate]:
        """Sweep recent Pump program activity for new CreateEvents.

        Pump.fun throughput is high enough that a single ``getSignatures`` call
        can return hundreds of transactions all from ONE slot, most of them
        failed trades rather than token creations. When ``slot_span`` is set,
        this method pages backwards through signatures (via the ``before``
        cursor) until it has either collected ``signature_limit`` candidates,
        reached a signature at or before the cursor's last slot, swept the
        requested slot span, or hit the ``max_pages`` cost ceiling.

        Without ``slot_span`` the behaviour matches the original single-batch
        sweep, preserving backward compatibility.
        """
        cursor = _read_json(self.cursor_path, {})
        cursor_signature = cursor.get("newest_signature")
        cursor_slot = _safe_int(cursor.get("newest_slot"))
        page_size = max(1, min(int(signature_limit), 1000))
        max_pages = max(1, int(max_pages))

        catalog = _read_json(
            self.catalog_path,
            {"schema_version": SCHEMA_VERSION, "ecosystem": "pump_fun", "tokens": {}},
        )
        discovered: list[SolanaLaunchCandidate] = []
        all_signatures: list[dict] = []
        newest_signature = cursor_signature
        newest_slot = cursor_slot
        before_signature: str | None = None
        slot_floor = (
            max(0, cursor_slot - int(slot_span))
            if slot_span is not None and cursor_slot
            else None
        )

        for page_index in range(max_pages):
            batch = self.rpc.get_signatures(
                PUMP_PROGRAM_ID,
                limit=page_size,
                until=cursor_signature,
                before=before_signature,
            )
            if not batch:
                break
            all_signatures.extend(batch)
            # Track the freshest signature seen across all pages for cursor advance.
            if page_index == 0:
                newest_signature = batch[0].get("signature", cursor_signature)
                newest_slot = _safe_int(batch[0].get("slot"), cursor_slot)
            oldest_in_batch = batch[-1]
            oldest_slot = _safe_int(oldest_in_batch.get("slot"))

            # Stop conditions evaluated AFTER absorbing the batch.
            reached_cursor = cursor_slot and oldest_slot is not None and oldest_slot <= cursor_slot
            reached_floor = slot_floor is not None and oldest_slot is not None and oldest_slot <= slot_floor
            if reached_cursor or reached_floor:
                break
            # Advance the paging cursor to the oldest signature of this batch.
            before_signature = oldest_in_batch.get("signature")
            if not before_signature:
                break

        # Decode in chronological order (oldest first) so the catalog reflects
        # the order events actually occurred on-chain. Pump.fun emits many more
        # trade transactions than creates, and most trades fail (err set) -- a
        # CreateEvent only lives in a SUCCESSFUL tx. Skip failed rows at the
        # signature level (their err is already visible in the signature row)
        # so we don't burn a getTransaction call on each one. This typically
        # cuts the per-sweep getTransaction workload by ~90%.
        decoded = 0
        for row in reversed(all_signatures):
            if row.get("err"):
                continue
            transaction = self.rpc.get_transaction(row["signature"])
            for candidate in self.decode_transaction(row, transaction):
                catalog["tokens"][candidate.mint] = candidate.to_dict()
                discovered.append(candidate)
                self.ledger.append(
                    "pump_create_event",
                    {
                        "candidate": candidate.to_dict(),
                        "source": "solana_rpc_confirmed_transaction_log",
                    },
                )
            decoded += 1
        if all_signatures:
            cursor = {
                "newest_signature": newest_signature,
                "newest_slot": newest_slot,
                "updated_at": _utc_now(),
            }
        retention_cutoff = time.time() - CATALOG_RETENTION_SECONDS
        catalog["tokens"] = {
            mint: value
            for mint, value in catalog["tokens"].items()
            if _safe_int(value.get("block_time")) >= retention_cutoff
        }
        catalog["updated_at"] = _utc_now()
        _atomic_json(self.catalog_path, catalog)
        _atomic_json(self.cursor_path, cursor)
        return discovered

    def recent(self, limit: int = 10) -> list[SolanaLaunchCandidate]:
        tokens = _read_json(self.catalog_path, {}).get("tokens", {})
        values = [SolanaLaunchCandidate.from_dict(value) for value in tokens.values()]
        return sorted(values, key=lambda item: (item.slot, item.signature), reverse=True)[
            : max(0, limit)
        ]

    def by_mint(self, mint: str) -> SolanaLaunchCandidate | None:
        value = _read_json(self.catalog_path, {}).get("tokens", {}).get(mint)
        return SolanaLaunchCandidate.from_dict(value) if value else None


class JupiterClient:
    def __init__(
        self,
        api_key: str | None = None,
        *,
        paper_taker: str | None = None,
        timeout: float = 15.0,
        session: requests.Session | None = None,
        minimum_request_interval: float | None = None,
        max_retries: int = 3,
    ):
        self.api_key = api_key or os.environ.get("JUPITER_API_KEY")
        self.paper_taker = paper_taker or os.environ.get("SOLANA_PAPER_TAKER")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.minimum_request_interval = (
            (1.05 if self.api_key else 2.05)
            if minimum_request_interval is None
            else max(0.0, minimum_request_interval)
        )
        self.max_retries = max(0, int(max_retries))
        self._last_request_at = 0.0
        self.attempts = 0
        self.successes = 0
        self.failures = 0
        self.retries = 0

    def _get(self, path: str, params: dict):
        headers = {"x-api-key": self.api_key} if self.api_key else {}
        last_error = None
        for attempt in range(self.max_retries + 1):
            delay = self.minimum_request_interval - (
                time.monotonic() - self._last_request_at
            )
            if delay > 0:
                time.sleep(delay)
            self.attempts += 1
            try:
                self._last_request_at = time.monotonic()
                response = self.session.get(
                    JUPITER_API_URL + path,
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )
                response.raise_for_status()
                payload = response.json()
                self.successes += 1
                return payload
            except (REQUEST_EXCEPTION, OSError, ValueError) as exc:
                last_error = exc
                status = getattr(
                    getattr(exc, "response", None), "status_code", None
                )
                if (
                    attempt >= self.max_retries
                    or status not in {429, 500, 502, 503, 504}
                ):
                    break
                self.retries += 1
                retry_after = getattr(
                    getattr(exc, "response", None), "headers", {}
                ).get("Retry-After")
                wait = _safe_float(retry_after, 0.0)
                time.sleep(max(wait, min(8.0, 0.5 * (2**attempt))))
        self.failures += 1
        raise InfrastructureIndeterminateError(
            f"Jupiter {path} unavailable: {last_error}"
        ) from last_error

    def health(self) -> dict:
        return {
            "access_mode": "api_key" if self.api_key else "keyless",
            "minimum_request_interval_seconds": self.minimum_request_interval,
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "retries": self.retries,
            "updated_at": _utc_now(),
        }

    @staticmethod
    def _quote_from_payload(
        payload: dict, input_mint: str, output_mint: str, amount: int
    ) -> JupiterQuote:
        out_amount = _safe_int(payload.get("outAmount"), -1)
        if out_amount <= 0 or payload.get("errorCode") is not None:
            raise InfrastructureIndeterminateError(
                f"Jupiter route unavailable: {payload.get('errorCode')} "
                f"{payload.get('errorMessage') or ''}".strip()
            )
        transaction = payload.get("transaction")
        if transaction is None:
            state = "quote_only"
        elif transaction == "":
            state = "unbuildable"
        else:
            state = "assembled_unsigned"
        impact = payload.get("priceImpactPct")
        return JupiterQuote(
            input_mint=input_mint,
            output_mint=output_mint,
            in_amount=_safe_int(payload.get("inAmount"), amount),
            out_amount=out_amount,
            router=payload.get("router"),
            price_impact_pct=_safe_float(impact, None) if impact is not None else None,
            fee_bps=_safe_int(payload.get("feeBps"), None),
            request_id=payload.get("requestId"),
            transaction_state=state,
            raw=payload,
        )

    def quote(
        self,
        input_mint: str,
        output_mint: str,
        amount: int,
        *,
        assemble: bool = False,
    ) -> JupiterQuote:
        params = {
            "inputMint": input_mint,
            "outputMint": output_mint,
            "amount": str(int(amount)),
        }
        if assemble and self.paper_taker:
            params["taker"] = self.paper_taker
        payload = self._get("/swap/v2/order", params)
        return self._quote_from_payload(payload, input_mint, output_mint, amount)

    def roundtrip(self, mint: str, input_lamports: int) -> dict:
        buy = self.quote(WRAPPED_SOL_MINT, mint, input_lamports)
        sell = self.quote(mint, WRAPPED_SOL_MINT, buy.out_amount)
        assembled = None
        if self.paper_taker:
            try:
                assembled = self.quote(
                    WRAPPED_SOL_MINT, mint, input_lamports, assemble=True
                )
            except InfrastructureIndeterminateError:
                assembled = None
        return {
            "buy": buy.to_dict(),
            "sell": sell.to_dict(),
            "roundtrip_retention_pct": 100.0 * sell.out_amount / input_lamports,
            "unsigned_buy_assembled": bool(
                assembled and assembled.transaction_state == "assembled_unsigned"
            ),
            "assembled_buy": assembled.to_dict() if assembled else None,
        }

    def token_info(self, mint: str) -> dict | None:
        values = self._get("/tokens/v2/search", {"query": mint})
        if not isinstance(values, list):
            return None
        return next((item for item in values if item.get("id") == mint), None)


class DexScreenerClient:
    """Credential-free secondary-market observation with a bounded TTL cache."""

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        session: requests.Session | None = None,
        ttl_seconds: float = 30.0,
    ):
        self.timeout = timeout
        self.session = session or requests.Session()
        self.ttl_seconds = max(0.0, float(ttl_seconds))
        self._cache: dict[str, tuple[float, list[dict]]] = {}
        self.attempts = 0
        self.successes = 0
        self.failures = 0
        self.cache_hits = 0

    def token_pairs(self, mint: str) -> list[dict]:
        cached = self._cache.get(mint)
        if cached and time.monotonic() - cached[0] <= self.ttl_seconds:
            self.cache_hits += 1
            return json.loads(_canonical_json(cached[1]))
        self.attempts += 1
        try:
            response = self.session.get(
                f"{DEXSCREENER_API_URL}/token-pairs/v1/solana/{mint}",
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, list):
                raise ValueError("token-pairs response is not a list")
        except (REQUEST_EXCEPTION, OSError, ValueError) as exc:
            self.failures += 1
            raise InfrastructureIndeterminateError(
                f"DexScreener token-pairs unavailable: {exc}"
            ) from exc
        self.successes += 1
        self._cache[mint] = (time.monotonic(), payload)
        return json.loads(_canonical_json(payload))

    def health(self) -> dict:
        return {
            "attempts": self.attempts,
            "successes": self.successes,
            "failures": self.failures,
            "cache_hits": self.cache_hits,
            "ttl_seconds": self.ttl_seconds,
            "updated_at": _utc_now(),
        }


class WalletConvergenceTracker:
    """Prospective wallet-accumulation registry and convergence detector.

    Survivorship-bias-safe by construction (ring 100 design): wallets are
    enrolled PROSPECTIVELY the first time they are observed as a holder of ANY
    analyzed token, and EVERY token they are later observed holding is recorded
    -- including the ones that go on to fail hard-stops. A wallet is only
    labelled 'historically accurate' (never 'smart') once enough of its
    observed positions have a known outcome AND its forward hit-rate clears a
    floor. The convergence signal is strictly additive context: it can NEVER
    override a hard-stop.

    'Hit' definition: an observed position whose token later reached
    evidence_state complete_safe (passed every hard-stop). This is an
    ANALYSIS-OUTCOME oracle, not a price oracle -- it measures 'did the gate
    clear this token', which is the signal the tracker exists to learn. Price
    outcomes would introduce the very survivorship bias this design avoids.
    """

    def __init__(
        self,
        state_path: str | Path,
        *,
        minimum_observations_for_accuracy: int = 3,
        minimum_accuracy_pct: float = 50.0,
        convergence_minimum_accurate_wallets: int = 2,
    ):
        self.state_path = Path(state_path)
        self.minimum_observations_for_accuracy = max(
            1, int(minimum_observations_for_accuracy)
        )
        self.minimum_accuracy_pct = max(
            0.0, min(100.0, float(minimum_accuracy_pct))
        )
        self.convergence_minimum_accurate_wallets = max(
            1, int(convergence_minimum_accurate_wallets)
        )
        self.state = self._load()

    def _load(self) -> dict:
        state = _read_json(
            self.state_path,
            {
                "schema_version": SCHEMA_VERSION,
                "wallets": {},
                "updated_at": None,
            },
        )
        if not isinstance(state.get("wallets"), dict):
            state["wallets"] = {}
        return state

    def _save(self) -> None:
        self.state["updated_at"] = _utc_now()
        _atomic_json(self.state_path, self.state)

    def observe(
        self,
        wallet: str,
        mint: str,
        *,
        evidence_state: str | None,
        observed_at: str | None = None,
    ) -> None:
        """Record that ``wallet`` was observed holding ``mint``.

        Prospective enrollment: the wallet enters the registry on first sight
        regardless of whether the token later passes or fails -- this is what
        keeps the dataset free of winner-only selection bias.
        """
        if not wallet or not mint:
            return
        wallets = self.state["wallets"]
        entry = wallets.get(wallet)
        if entry is None:
            entry = {
                "first_observed_at": observed_at or _utc_now(),
                "positions": {},
            }
            wallets[wallet] = entry
        entry["positions"][mint] = {
            "evidence_state": evidence_state,
            "observed_at": observed_at or _utc_now(),
        }
        self._save()

    def regrade(self, mint: str, evidence_state: str) -> int:
        """Update the recorded outcome for ``mint`` across all holding wallets.

        Called when an analysis re-classifies a token; lets the hit-rate track
        the latest verdict rather than the first observation. Returns the count
        of positions updated.
        """
        updated = 0
        for entry in self.state["wallets"].values():
            pos = entry.get("positions", {}).get(mint)
            if pos and pos.get("evidence_state") != evidence_state:
                pos["evidence_state"] = evidence_state
                pos["regraded_at"] = _utc_now()
                updated += 1
        if updated:
            self._save()
        return updated

    def wallet_accuracy(self, wallet: str) -> dict:
        """Compute a wallet's forward hit-rate from observed positions.

        A 'hit' is a position whose token reached complete_safe. Wallets with
        fewer than the minimum observation count are 'unrated' -- they have not
        yet accumulated enough prospective data to judge.
        """
        entry = self.state["wallets"].get(wallet) or {}
        positions = entry.get("positions") or {}
        known = [
            p for p in positions.values()
            if p.get("evidence_state") is not None
        ]
        total = len(known)
        if total < self.minimum_observations_for_accuracy:
            return {
                "wallet": wallet,
                "rated": False,
                "observations": total,
                "hits": sum(1 for p in known if p["evidence_state"] == "complete_safe"),
                "hit_rate_pct": None,
                "label": "unrated",
            }
        hits = sum(1 for p in known if p["evidence_state"] == "complete_safe")
        hit_rate = 100.0 * hits / total
        accurate = hit_rate >= self.minimum_accuracy_pct
        return {
            "wallet": wallet,
            "rated": True,
            "observations": total,
            "hits": hits,
            "hit_rate_pct": round(hit_rate, 1),
            "label": "historically_accurate" if accurate else "historically_inaccurate",
        }

    def convergence_for(
        self, candidate_holders: list[dict]
    ) -> dict:
        """How many historically-accurate wallets also hold this token.

        ``candidate_holders`` is the token's largest_non_curve_accounts (each
        with an ``owner`` field). The result is strictly additive context --
        the caller must NEVER use it to override a hard-stop.
        """
        accurate_wallets = []
        for holder in candidate_holders:
            owner = holder.get("owner")
            if not owner:
                continue
            acc = self.wallet_accuracy(owner)
            if acc["rated"] and acc["label"] == "historically_accurate":
                accurate_wallets.append(acc)
        converged = len(accurate_wallets) >= self.convergence_minimum_accurate_wallets
        return {
            "accurate_wallets_holding": len(accurate_wallets),
            "converged": converged,
            "wallets": [
                {
                    "wallet": w["wallet"],
                    "observations": w["observations"],
                    "hit_rate_pct": w["hit_rate_pct"],
                }
                for w in accurate_wallets
            ],
            "minimum_for_convergence": self.convergence_minimum_accurate_wallets,
            "caveat": (
                "Convergence is an additive correlation observation, not a "
                "safety signal. It must never override a hard-stop. Wallet "
                "accuracy is measured against analysis outcomes, not price."
            ),
        }

    def snapshot(self, limit: int = 20) -> dict:
        """Top historically-accurate wallets for dashboard / observability."""
        rated = []
        for wallet in self.state["wallets"]:
            acc = self.wallet_accuracy(wallet)
            if acc["rated"]:
                rated.append(acc)
        rated.sort(
            key=lambda w: (w["hit_rate_pct"] or 0, w["observations"]),
            reverse=True,
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "total_wallets_enrolled": len(self.state["wallets"]),
            "rated_wallets": len(rated),
            "top_accurate_wallets": rated[:limit],
            "updated_at": self.state.get("updated_at"),
        }


class SolanaRiskAnalyzer:
    def __init__(
        self,
        rpc: SolanaRPC,
        jupiter: JupiterClient,
        policy: SolanaRiskPolicy | None = None,
        dexscreener: DexScreenerClient | None = None,
        convergence: WalletConvergenceTracker | None = None,
    ):
        self.rpc = rpc
        self.jupiter = jupiter
        self.policy = policy or SolanaRiskPolicy()
        self.dexscreener = dexscreener or DexScreenerClient()
        self.convergence = convergence
        # Per-creator history cache: many tokens share a creator, and the scan
        # is ~N getTransaction calls, so caching amortizes the cost. Keyed by
        # creator pubkey; entries are (fetched_at_monotonic, result). A
        # long-running process observes an unbounded number of distinct
        # creator wallets over time, so this is size-capped with LRU eviction
        # (same shape as the rate-limiter identity leak fixed in
        # chainseer_api.py) rather than left to grow for the life of the
        # process.
        self._creator_history_cache: "OrderedDict[str, tuple[float, dict]]" = (
            OrderedDict()
        )
        self._creator_history_ttl = 300.0  # 5 min
        self._creator_history_cache_max = 5_000

    def _cache_creator_history(self, creator: str, now: float, value: dict) -> None:
        self._creator_history_cache[creator] = (now, value)
        self._creator_history_cache.move_to_end(creator)
        while len(self._creator_history_cache) > self._creator_history_cache_max:
            self._creator_history_cache.popitem(last=False)

    @staticmethod
    def _decode_curve(account: dict | None) -> dict:
        value = (account or {}).get("value") or {}
        owner = value.get("owner")
        data = value.get("data")
        if not isinstance(data, list) or not data:
            raise InfrastructureIndeterminateError("bonding curve data is unavailable")
        try:
            raw = base64.b64decode(data[0], validate=True)
        except (ValueError, TypeError) as exc:
            raise InfrastructureIndeterminateError("invalid bonding curve encoding") from exc
        if not raw.startswith(PUMP_BONDING_CURVE_DISCRIMINATOR):
            raise InfrastructureIndeterminateError("unexpected bonding curve discriminator")
        reader = _BorshReader(raw[len(PUMP_BONDING_CURVE_DISCRIMINATOR):])
        result = {
            "owner": owner,
            "virtual_token_reserves": reader.u64(),
            "virtual_quote_reserves": reader.u64(),
            "real_token_reserves": reader.u64(),
            "real_quote_reserves": reader.u64(),
            "token_total_supply": reader.u64(),
            "complete": bool(reader.u8()),
            "creator": reader.pubkey(),
        }
        if reader.offset < len(reader.raw):
            result["is_mayhem_mode"] = bool(reader.u8())
        if reader.offset < len(reader.raw):
            result["is_cashback_coin"] = bool(reader.u8())
        if reader.offset + 32 <= len(reader.raw):
            result["quote_mint"] = reader.pubkey()
        return result

    @staticmethod
    def _decode_pump_amm_pool(row: dict) -> dict:
        account = row.get("account") or {}
        data = account.get("data")
        if not isinstance(data, list) or not data:
            raise InfrastructureIndeterminateError("PumpSwap pool data is unavailable")
        try:
            raw = base64.b64decode(data[0], validate=True)
        except (ValueError, TypeError) as exc:
            raise InfrastructureIndeterminateError(
                "invalid PumpSwap pool encoding"
            ) from exc
        if not raw.startswith(PUMP_AMM_POOL_DISCRIMINATOR):
            raise InfrastructureIndeterminateError(
                "unexpected PumpSwap pool discriminator"
            )
        reader = _BorshReader(raw[len(PUMP_AMM_POOL_DISCRIMINATOR):])
        try:
            result = {
                "pool": row.get("pubkey"),
                "owner": account.get("owner"),
                "pool_bump": reader.u8(),
                "index": reader.u16(),
                "creator": reader.pubkey(),
                "base_mint": reader.pubkey(),
                "quote_mint": reader.pubkey(),
                "lp_mint": reader.pubkey(),
                "pool_base_token_account": reader.pubkey(),
                "pool_quote_token_account": reader.pubkey(),
                "lp_supply": reader.u64(),
            }
        except ValueError as exc:
            raise InfrastructureIndeterminateError(
                "truncated PumpSwap pool account"
            ) from exc
        return result

    def _canonical_pool_evidence(self, mint: str) -> dict | None:
        rows = self.rpc.get_program_accounts(
            PUMP_AMM_PROGRAM_ID,
            filters=[
                {
                    "memcmp": {
                        "offset": 0,
                        "bytes": _b58encode(PUMP_AMM_POOL_DISCRIMINATOR),
                    }
                },
                {"memcmp": {"offset": 9, "bytes": _b58encode(b"\0\0")}},
                {"memcmp": {"offset": 43, "bytes": mint}},
            ],
            encoding="base64",
        )
        matches = []
        for row in rows:
            pool = self._decode_pump_amm_pool(row)
            if (
                pool.get("owner") == PUMP_AMM_PROGRAM_ID
                and pool.get("index") == 0
                and pool.get("base_mint") == mint
                and pool.get("quote_mint")
                in {WRAPPED_SOL_MINT, SOLANA_USDC_MINT}
            ):
                matches.append(pool)
        if not matches:
            return None
        return sorted(matches, key=lambda item: item.get("pool") or "")[0]

    def _dexscreener_market(
        self, mint: str, canonical_pool: str
    ) -> dict | None:
        pairs = self.dexscreener.token_pairs(mint)
        matches = []
        for pair in pairs:
            if str(pair.get("chainId") or "").lower() != "solana":
                continue
            if str(pair.get("dexId") or "").lower() != "pumpswap":
                continue
            if pair.get("pairAddress") != canonical_pool:
                continue
            base = pair.get("baseToken") or {}
            quote = pair.get("quoteToken") or {}
            if base.get("address") != mint:
                continue
            if quote.get("address") not in {WRAPPED_SOL_MINT, SOLANA_USDC_MINT}:
                continue
            matches.append(pair)
        if not matches:
            return None
        pair = max(
            matches,
            key=lambda item: _safe_float(
                (item.get("liquidity") or {}).get("usd")
            ),
        )
        created_ms = _safe_int(pair.get("pairCreatedAt"), 0)
        age_seconds = (
            max(0, int(time.time() - created_ms / 1000))
            if created_ms > 0
            else None
        )
        return {
            "source": "dexscreener_token_pairs",
            "chain_id": pair.get("chainId"),
            "dex_id": pair.get("dexId"),
            "pair_address": pair.get("pairAddress"),
            "url": pair.get("url"),
            "base_token": {
                key: base.get(key) for key in ("address", "name", "symbol")
            },
            "quote_token": {
                key: quote.get(key) for key in ("address", "name", "symbol")
            },
            "price_usd": pair.get("priceUsd"),
            "liquidity_usd": (pair.get("liquidity") or {}).get("usd"),
            "market_cap": pair.get("marketCap"),
            "fdv": pair.get("fdv"),
            "pair_created_at_ms": created_ms or None,
            "market_age_seconds": age_seconds,
            "txns": pair.get("txns") or {},
            "volume": pair.get("volume") or {},
        }

    @staticmethod
    def _extension_names(info: dict) -> list[str]:
        values = info.get("extensions") or []
        names = []
        for value in values:
            if isinstance(value, str):
                names.append(value)
            elif isinstance(value, dict):
                name = value.get("extension") or value.get("type")
                if name:
                    names.append(str(name))
        return sorted(set(names))

    def _mint_evidence(self, candidate: SolanaLaunchCandidate) -> dict:
        account = self.rpc.get_account_info(candidate.mint, encoding="jsonParsed")
        value = (account or {}).get("value") or {}
        parsed = ((value.get("data") or {}).get("parsed") or {})
        if parsed.get("type") != "mint":
            raise InfrastructureIndeterminateError("mint account is not jsonParsed as mint")
        info = parsed.get("info") or {}
        supply = self.rpc.get_token_supply(candidate.mint) or {}
        supply_value = supply.get("value") or {}
        return {
            "owner_program": value.get("owner"),
            "event_token_program": candidate.token_program,
            "decimals": _safe_int(info.get("decimals")),
            "supply_raw": _safe_int(supply_value.get("amount"), _safe_int(info.get("supply"))),
            "mint_authority": info.get("mintAuthority"),
            "freeze_authority": info.get("freezeAuthority"),
            "extensions": self._extension_names(info),
        }

    def _creator_history(self, candidate: SolanaLaunchCandidate) -> dict:
        """Scan the creator's recent Pump.fun deployment behaviour.

        This is a DESCRIPTIVE behavioural measurement, not a verdict on any
        prior token: we count how many tokens the creator deployed in a recent
        window and how rapidly. Industrialized deployment cadence (many tokens
        in hours) is empirically token-farm behaviour and an objective
        risk-correlated signal -- it does NOT require labelling any prior token
        a rug or a winner.

        Reuses PumpFunObserver.decode_transaction to extract CreateEvents from
        the creator's transaction history. Results are cached per creator for a
        short TTL because many tokens share a creator and the scan issues one
        getTransaction per recent signature.
        """
        creator = candidate.creator
        result = {
            "creator": creator,
            "scanned": False,
            "scan_degraded": False,
            "transactions_failed": 0,
            "pages_scanned": 0,
            "signatures_scanned": 0,
            "prior_deployments_in_window": 0,
            "prior_deployments_total_observed": 0,
            "window_hours": self.policy.creator_history_window_hours,
            "deployments_per_hour": 0.0,
            "median_minutes_between_deployments": None,
            "prior_symbols_sample": [],
            "oldest_observed_slot": None,
            "cache_hit": False,
        }
        if not creator or creator == "0x" + "0" * 40 or len(creator) < 32:
            return result

        # Per-creator cache (many tokens share a creator).
        cached = self._creator_history_cache.get(creator)
        now = time.monotonic()
        if cached and now - cached[0] <= self._creator_history_ttl:
            self._creator_history_cache.move_to_end(creator)
            cached_result = cached[1]
            # The cached result is creator-scoped, so it's valid for this token.
            return {**cached_result, "cache_hit": True}

        window_seconds = self.policy.creator_history_window_hours * 3600.0
        now_epoch = time.time()
        cutoff_epoch = now_epoch - window_seconds

        # getSignaturesForAddress returns EVERY signature for this wallet, not
        # just Pump.fun interactions -- a creator who also trades/swaps on the
        # same wallet can fill a single page with unrelated activity and hide
        # genuine prior launches from a one-shot scan. Page backwards (like
        # PumpFunObserver.sync) until the lookback window is covered, bounded
        # by max_pages / max_transactions_scanned as an RPC cost ceiling.
        signatures: list[dict] = []
        before_signature: str | None = None
        pages_scanned = 0
        try:
            for _ in range(self.policy.creator_history_max_pages):
                batch = self.rpc.get_signatures(
                    creator,
                    limit=self.policy.creator_history_signature_limit,
                    before=before_signature,
                )
                pages_scanned += 1
                if not batch:
                    break
                signatures.extend(batch)
                oldest_row = batch[-1]
                before_signature = oldest_row.get("signature")
                if (
                    not before_signature
                    or len(signatures)
                    >= self.policy.creator_history_max_transactions_scanned
                ):
                    break
                oldest_time = _safe_int(oldest_row.get("blockTime"), None)
                if oldest_time is not None and oldest_time < cutoff_epoch:
                    break
        except InfrastructureIndeterminateError:
            return result

        result["pages_scanned"] = pages_scanned
        if not signatures:
            self._cache_creator_history(creator, now, dict(result))
            return result

        signatures = signatures[
            : self.policy.creator_history_max_transactions_scanned
        ]
        result["signatures_scanned"] = len(signatures)

        # Decode each transaction and collect CreateEvents attributed to this
        # creator. Non-create activity (buys/sells) yields no candidates.
        prior_deployments: list[SolanaLaunchCandidate] = []
        oldest_slot = None
        transactions_failed = 0
        for row in signatures:
            slot = _safe_int(row.get("slot"))
            if oldest_slot is None or (slot and slot < oldest_slot):
                oldest_slot = slot
            if row.get("err"):
                continue
            try:
                transaction = self.rpc.get_transaction(row["signature"])
            except InfrastructureIndeterminateError:
                transactions_failed += 1
                continue
            for decoded in PumpFunObserver.decode_transaction(row, transaction):
                # Only count deployments attributed to THIS creator (a wallet
                # could appear in other creators' txs as a buyer/signer).
                if decoded.creator == creator and decoded.mint != candidate.mint:
                    prior_deployments.append(decoded)

        result["scanned"] = True
        # A partial scan (some getTransaction lookups failed) must not look
        # identical to a clean one -- surfaced so callers/dashboards can flag
        # that the deployment count is a floor, not a confirmed total.
        result["scan_degraded"] = transactions_failed > 0
        result["transactions_failed"] = transactions_failed
        result["prior_deployments_total_observed"] = len(prior_deployments)
        result["oldest_observed_slot"] = oldest_slot

        # Cadence within the lookback window (recent-first signatures).
        recent = [
            d for d in prior_deployments
            if (now_epoch - d.block_time) <= window_seconds
        ]
        result["prior_deployments_in_window"] = len(recent)
        if window_seconds > 0:
            result["deployments_per_hour"] = round(
                len(recent) / (window_seconds / 3600.0), 3
            )

        # Median minutes between deployments (velocity signal).
        if len(recent) >= 2:
            times = sorted(d.block_time for d in recent)
            gaps = [
                (times[i + 1] - times[i]) / 60.0
                for i in range(len(times) - 1)
            ]
            gaps.sort()
            result["median_minutes_between_deployments"] = round(
                gaps[len(gaps) // 2], 1
            )

        result["prior_symbols_sample"] = [
            {"symbol": d.symbol, "mint": d.mint, "block_time": d.block_time}
            for d in sorted(recent, key=lambda d: d.block_time, reverse=True)[:10]
        ]

        cache_value = {k: v for k, v in result.items() if k != "cache_hit"}
        self._cache_creator_history(creator, now, cache_value)
        return result

    def _concentration(
        self, candidate: SolanaLaunchCandidate, supply_raw: int
    ) -> dict:
        if candidate.token_program == TOKEN_2022_PROGRAM_ID:
            rows = self.rpc.get_token_accounts_by_mint(
                candidate.mint, candidate.token_program
            )
            holders = []
            excluded_curve_raw = 0
            for row in rows:
                parsed = (
                    (((row.get("account") or {}).get("data") or {}).get(
                        "parsed"
                    ) or {})
                )
                info = parsed.get("info") or {}
                if info.get("mint") != candidate.mint:
                    continue
                owner = info.get("owner")
                raw = _safe_int(
                    (info.get("tokenAmount") or {}).get("amount")
                )
                record = {
                    "token_account": row.get("pubkey"),
                    "owner": owner,
                    "amount_raw": raw,
                }
                if owner == candidate.bonding_curve:
                    record["excluded_reason"] = "pump_bonding_curve_inventory"
                    excluded_curve_raw += raw
                else:
                    holders.append(record)
            method = "getProgramAccounts_token2022_with_curve_owner_exclusion"
        else:
            result = self.rpc.get_token_largest_accounts(candidate.mint) or {}
            largest_rows = (result.get("value") or [])[:20]
            addresses = [
                row.get("address")
                for row in largest_rows
                if row.get("address")
            ]
            accounts = self.rpc.get_multiple_accounts(addresses) or {}
            account_values = accounts.get("value") or []
            holders = []
            excluded_curve_raw = 0
            for row, account in zip(largest_rows, account_values):
                raw = _safe_int(row.get("amount"))
                parsed = (
                    (((account or {}).get("data") or {}).get("parsed") or {})
                )
                info = parsed.get("info") or {}
                owner = info.get("owner")
                record = {
                    "token_account": row.get("address"),
                    "owner": owner,
                    "amount_raw": raw,
                }
                if owner == candidate.bonding_curve:
                    record["excluded_reason"] = "pump_bonding_curve_inventory"
                    excluded_curve_raw += raw
                else:
                    holders.append(record)
            method = "getTokenLargestAccounts_with_curve_owner_exclusion"
        circulating = max(0, supply_raw - excluded_curve_raw)
        amounts = sorted((row["amount_raw"] for row in holders), reverse=True)
        top1 = 100.0 * (amounts[0] if amounts else 0) / circulating if circulating else None
        top10 = 100.0 * sum(amounts[:10]) / circulating if circulating else None
        circulating_pct_of_supply = (
            100.0 * circulating / supply_raw if supply_raw > 0 else None
        )
        return {
            "supply_raw": supply_raw,
            "excluded_bonding_curve_raw": excluded_curve_raw,
            "circulating_raw": circulating,
            "circulating_pct_of_supply": circulating_pct_of_supply,
            "top1_circulating_pct": top1,
            "top10_circulating_pct": top10,
            "largest_non_curve_accounts": holders[:10],
            "method": method,
        }

    def analyze(self, candidate: SolanaLaunchCandidate) -> SolanaRiskDecision:
        hard_stops: list[str] = []
        warnings: list[str] = []
        infrastructure_errors: list[str] = []
        coverage = {
            "pump_create_event": True,
            "mint_state": False,
            "bonding_curve_state": False,
            "curve_completion": False,
            "canonical_pumpswap_pool": False,
            "dexscreener_canonical_pair": False,
            "holder_concentration": False,
            "creator_history": False,
            "wallet_convergence": False,
            "jupiter_token_info": False,
            "jupiter_two_way_quote": False,
            "unsigned_buy_assembly": False,
        }
        origin = {
            "ecosystem": candidate.launch_ecosystem,
            "program_id": PUMP_PROGRAM_ID,
            "signature": candidate.signature,
            "slot": candidate.slot,
            "source": "confirmed_program_create_event",
        }
        mint_evidence: dict = {}
        curve_evidence: dict = {}
        graduation: dict = {
            "curve_completed": False,
            "real_token_reserves_zero": False,
            "completion_verified_on_chain": False,
            "canonical_pool_verified_on_chain": False,
            "canonical_pool": None,
            "secondary_market_observed": False,
            "secondary_market_source": None,
            "initial_real_token_reserves": candidate.real_token_reserves,
            "current_real_token_reserves": None,
            "progress_pct": None,
        }
        concentration: dict = {}
        convergence_evidence: dict = {}
        creator_evidence: dict = {}
        market: dict = {}
        execution: dict = {}

        age_seconds = max(0, int(time.time()) - candidate.block_time)
        origin["age_seconds"] = age_seconds
        if age_seconds < self.policy.minimum_age_seconds:
            warnings.append("anti_sniper_wait_active")

        try:
            mint_evidence = self._mint_evidence(candidate)
            coverage["mint_state"] = True
            if mint_evidence.get("owner_program") != candidate.token_program:
                hard_stops.append("mint_owner_program_mismatch")
            if mint_evidence.get("owner_program") not in {
                TOKEN_PROGRAM_ID,
                TOKEN_2022_PROGRAM_ID,
            }:
                hard_stops.append("unsupported_token_program")
            if mint_evidence.get("mint_authority"):
                hard_stops.append("mint_authority_active")
            if mint_evidence.get("freeze_authority"):
                hard_stops.append("freeze_authority_active")
            risky = sorted(
                name
                for name in mint_evidence.get("extensions", [])
                if name.replace("_", "").lower() in RISKY_TOKEN_2022_EXTENSIONS
            )
            if risky:
                hard_stops.append("risky_token_2022_extensions:" + ",".join(risky))
        except InfrastructureIndeterminateError as exc:
            infrastructure_errors.append(str(exc))

        # Creator deployment history -- runs REGARDLESS of distribution_stage,
        # because creator risk is independent of the token's current holders.
        # A serial/industrialized deployer's brand-new token is the highest-
        # risk case, and this is the one signal that can hard-stop a
        # pre_distribution token. Measures objective cadence, not a verdict.
        try:
            creator_evidence = self._creator_history(candidate)
            coverage["creator_history"] = bool(creator_evidence.get("scanned"))
            if creator_evidence.get("scanned"):
                in_window = _safe_int(
                    creator_evidence.get("prior_deployments_in_window")
                )
                # Extreme tier (industrialized) -> hard-stop. Counted in the
                # tighter 24h window to catch active farming.
                hard_stop_window_seconds = (
                    self.policy.creator_history_hard_stop_window_hours * 3600.0
                )
                now_epoch = time.time()
                in_hard_stop_window = _safe_int(sum(
                    1 for d in creator_evidence.get("prior_symbols_sample", [])
                    if (now_epoch - _safe_int(d.get("block_time")))
                    <= hard_stop_window_seconds
                )) if creator_evidence.get("prior_symbols_sample") else in_window
                if in_hard_stop_window >= self.policy.creator_history_hard_stop_count:
                    hard_stops.append(
                        "creator_industrialized_deployment_"
                        f"{in_hard_stop_window}_in_"
                        f"{int(self.policy.creator_history_hard_stop_window_hours)}h"
                    )
                elif in_window >= self.policy.creator_history_warning_count:
                    warnings.append(
                        "creator_multiple_recent_deployments_"
                        f"{in_window}_in_"
                        f"{int(self.policy.creator_history_window_hours)}h"
                    )
                # A degraded scan (some getTransaction lookups failed) can
                # only under-count deployment cadence, never over-count -- a
                # clean "no farming detected" result is not trustworthy when
                # the scan itself was incomplete, so this stays visible even
                # when no hard-stop/warning fired above.
                if creator_evidence.get("scan_degraded"):
                    warnings.append(
                        "creator_history_scan_degraded_"
                        f"{_safe_int(creator_evidence.get('transactions_failed'))}"
                        "_lookups_failed"
                    )
        except InfrastructureIndeterminateError as exc:
            infrastructure_errors.append(str(exc))

        try:
            curve_evidence = self._decode_curve(
                self.rpc.get_account_info(candidate.bonding_curve, encoding="base64")
            )
            coverage["bonding_curve_state"] = True
            if curve_evidence.get("owner") != PUMP_PROGRAM_ID:
                hard_stops.append("bonding_curve_owner_mismatch")
            if curve_evidence.get("creator") != candidate.creator:
                hard_stops.append("bonding_curve_creator_mismatch")
            graduation["curve_completed"] = bool(
                curve_evidence.get("complete")
            )
            graduation["real_token_reserves_zero"] = (
                _safe_int(curve_evidence.get("real_token_reserves"), -1) == 0
            )
            current_real_reserves = _safe_int(
                curve_evidence.get("real_token_reserves"), -1
            )
            graduation["current_real_token_reserves"] = (
                current_real_reserves
                if current_real_reserves >= 0
                else None
            )
            if candidate.real_token_reserves > 0 and current_real_reserves >= 0:
                graduation["progress_pct"] = round(
                    100.0
                    * max(
                        0.0,
                        min(
                            1.0,
                            1.0
                            - current_real_reserves
                            / candidate.real_token_reserves,
                        ),
                    ),
                    4,
                )
            graduation["completion_verified_on_chain"] = bool(
                graduation["curve_completed"]
                and graduation["real_token_reserves_zero"]
            )
            coverage["curve_completion"] = graduation[
                "completion_verified_on_chain"
            ]
            if graduation["curve_completed"] != graduation[
                "real_token_reserves_zero"
            ]:
                hard_stops.append("bonding_curve_completion_state_inconsistent")
        except InfrastructureIndeterminateError as exc:
            infrastructure_errors.append(str(exc))

        if graduation["completion_verified_on_chain"]:
            try:
                canonical_pool = self._canonical_pool_evidence(candidate.mint)
                if canonical_pool:
                    graduation["canonical_pool_verified_on_chain"] = True
                    graduation["canonical_pool"] = canonical_pool
                    coverage["canonical_pumpswap_pool"] = True
                else:
                    warnings.append("canonical_pumpswap_migration_pending")
            except InfrastructureIndeterminateError as exc:
                infrastructure_errors.append(str(exc))

        if graduation["canonical_pool_verified_on_chain"]:
            try:
                dex_market = self._dexscreener_market(
                    candidate.mint,
                    graduation["canonical_pool"]["pool"],
                )
                if dex_market:
                    graduation["secondary_market_observed"] = True
                    graduation["secondary_market_source"] = (
                        "dexscreener_canonical_pair"
                    )
                    coverage["dexscreener_canonical_pair"] = True
                    market["dexscreener"] = dex_market
                    market_age = dex_market.get("market_age_seconds")
                    if market_age is None:
                        warnings.append("graduated_market_age_unresolved")
                    elif (
                        market_age
                        < self.policy.minimum_graduated_market_age_seconds
                    ):
                        warnings.append("graduated_market_anti_sniper_wait_active")
                else:
                    warnings.append("dexscreener_canonical_pair_not_yet_indexed")
            except InfrastructureIndeterminateError as exc:
                infrastructure_errors.append(str(exc))

        if coverage["mint_state"]:
            try:
                concentration = self._concentration(
                    candidate, _safe_int(mint_evidence.get("supply_raw"))
                )
                coverage["holder_concentration"] = True
                top1 = concentration.get("top1_circulating_pct")
                top10 = concentration.get("top10_circulating_pct")
                circulating_pct = _safe_float(
                    concentration.get("circulating_pct_of_supply")
                )
                if top1 is None or top10 is None:
                    hard_stops.append("circulating_concentration_unresolved")
                    concentration["distribution_stage"] = "unresolved"
                elif (
                    circulating_pct is not None
                    and circulating_pct < self.policy.minimum_circulating_pct_of_supply
                ):
                    # Most supply is still in the bonding curve: holder % of the
                    # tiny circulating sliver is not yet a meaningful signal.
                    # Suppress the concentration hard-stops and surface the
                    # state honestly -- this is NOT safe, it is unjudgeable yet.
                    concentration["distribution_stage"] = "pre_distribution"
                    warnings.append("distribution_too_early_to_judge")
                else:
                    concentration["distribution_stage"] = "measurable"
                    if top1 > self.policy.maximum_top1_circulating_pct:
                        hard_stops.append("top1_circulating_concentration_high")
                    if top10 > self.policy.maximum_top10_circulating_pct:
                        hard_stops.append("top10_circulating_concentration_high")
            except InfrastructureIndeterminateError as exc:
                infrastructure_errors.append(str(exc))

        # Wallet convergence -- strictly ADDITIVE context, never a hard-stop
        # override (ring 100 S84). Observed holders are enrolled prospectively
        # (regardless of this token's outcome) so the registry avoids
        # survivorship bias; convergence counts how many historically-accurate
        # wallets (by analysis-outcome hit-rate) also hold this token. It is
        # surfaced as a warning when it fires, and as context otherwise.
        if self.convergence is not None and coverage["holder_concentration"]:
            holders = concentration.get("largest_non_curve_accounts") or []
            # Prospective enrollment of every observed holder.
            for holder in holders:
                owner = holder.get("owner")
                if owner:
                    self.convergence.observe(
                        owner,
                        candidate.mint,
                        evidence_state=None,  # outcome filled by regrade below
                    )
            convergence_evidence = self.convergence.convergence_for(holders)
            coverage["wallet_convergence"] = bool(convergence_evidence.get("converged"))
            if convergence_evidence.get("converged"):
                # NOTE: a warning, not a hard-stop. Convergence is a correlation
                # observation that may merit attention but must never greenlight
                # a token the hard-stops reject.
                warnings.append(
                    "wallet_convergence_"
                    f"{convergence_evidence['accurate_wallets_holding']}_accurate_holders"
                )

        if graduation["secondary_market_observed"]:
            try:
                token_info = self.jupiter.token_info(candidate.mint)
                if token_info:
                    market["jupiter_token_info"] = {
                        key: token_info.get(key)
                        for key in (
                            "id",
                            "name",
                            "symbol",
                            "decimals",
                            "holderCount",
                            "liquidity",
                            "organicScore",
                            "organicScoreLabel",
                            "isVerified",
                            "usdPrice",
                            "mcap",
                            "firstPool",
                        )
                    }
                    coverage["jupiter_token_info"] = True
                else:
                    warnings.append("jupiter_token_metadata_not_yet_indexed")
            except InfrastructureIndeterminateError as exc:
                infrastructure_errors.append(str(exc))

            try:
                input_lamports = int(self.policy.amount_sol * 1_000_000_000)
                execution = self.jupiter.roundtrip(candidate.mint, input_lamports)
                coverage["jupiter_two_way_quote"] = True
                coverage["unsigned_buy_assembly"] = bool(
                    execution.get("unsigned_buy_assembled")
                )
                retention = _safe_float(execution.get("roundtrip_retention_pct"))
                impact = _safe_float(
                    ((execution.get("buy") or {}).get("price_impact_pct")), None
                )
                if retention < self.policy.minimum_roundtrip_retention_pct:
                    hard_stops.append("jupiter_roundtrip_retention_low")
                if (
                    impact is not None
                    and impact > self.policy.maximum_buy_price_impact_pct
                ):
                    hard_stops.append("jupiter_buy_price_impact_high")
                if not coverage["unsigned_buy_assembly"]:
                    warnings.append("unsigned_buy_transaction_not_assembled")
            except InfrastructureIndeterminateError as exc:
                infrastructure_errors.append(str(exc))

        observation_required = (
            coverage["pump_create_event"]
            and coverage["mint_state"]
            and coverage["bonding_curve_state"]
            and coverage["holder_concentration"]
        )
        graduated_market_required = (
            graduation["canonical_pool_verified_on_chain"]
            and graduation["secondary_market_observed"]
            and coverage["jupiter_two_way_quote"]
        )
        if infrastructure_errors or not observation_required:
            evidence_state = "infrastructure_indeterminate"
        elif hard_stops:
            evidence_state = "complete_unsafe"
        else:
            evidence_state = "complete_safe"
        distribution_stage = concentration.get("distribution_stage")
        pre_distribution = distribution_stage == "pre_distribution"
        # Honest labeling (ring 131/132): a token whose holder concentration
        # cannot yet be judged (pre_distribution) is NOT 'complete_safe' --
        # 'complete' implies every axis was measurable. Surface it as a distinct
        # 'distribution_pending' state so complete_safe honestly means 'all axes
        # judged AND clear'. Admission is unaffected (the admission_state path
        # already blocked pre_distribution); this is purely about the label and
        # the score not inverting (least-vetted ranking highest).
        if (
            evidence_state == "complete_safe"
            and pre_distribution
        ):
            evidence_state = "distribution_pending"
        cohort = (
            "graduated_market"
            if graduation["canonical_pool_verified_on_chain"]
            else "launch_observation"
        )
        dex_market = market.get("dexscreener") or {}
        market_age = dex_market.get("market_age_seconds")
        market_age_ready = (
            market_age is not None
            and market_age >= self.policy.minimum_graduated_market_age_seconds
        )
        if evidence_state == "infrastructure_indeterminate":
            admission_state = "infrastructure_indeterminate"
        elif hard_stops:
            admission_state = (
                "graduated_market_unsafe"
                if cohort == "graduated_market"
                else "launch_observation_unsafe"
            )
        elif not graduation["completion_verified_on_chain"]:
            admission_state = "graduation_pending"
        elif not graduation["canonical_pool_verified_on_chain"]:
            admission_state = "canonical_migration_pending"
        elif not graduation["secondary_market_observed"]:
            admission_state = "market_indexing_pending"
        elif not graduated_market_required:
            admission_state = "execution_evidence_pending"
        elif pre_distribution:
            admission_state = "distribution_pending"
        elif not market_age_ready:
            admission_state = "market_age_pending"
        else:
            admission_state = "graduated_market_ready"
        allowed = (
            evidence_state == "complete_safe"
            and admission_state == "graduated_market_ready"
            and age_seconds >= self.policy.minimum_age_seconds
        )
        score = max(
            0.0,
            min(
                100.0,
                100.0
                - 22.0 * len(hard_stops)
                - 5.0 * len(warnings)
                - 12.0 * len(infrastructure_errors),
            ),
        )
        # An unjudgeable token (distribution_pending) must never outrank a
        # measured one. Cap below the score any measured-clean token would get
        # (>=95), in distinct 'unjudgeable territory', while still subtracting
        # for hard_stops/warnings within the cap to preserve relative ordering.
        if evidence_state == "distribution_pending":
            score = min(score, 50.0)
        risk_level = (
            "Indeterminate"
            if evidence_state == "infrastructure_indeterminate"
            else ("Pending" if evidence_state == "distribution_pending"
                  else ("High" if hard_stops else ("Medium" if warnings else "Low")))
        )
        return SolanaRiskDecision(
            score=round(score, 1),
            risk_level=risk_level,
            evidence_state=evidence_state,
            shadow_entry_allowed=allowed,
            hard_stops=hard_stops,
            warnings=warnings,
            infrastructure_errors=infrastructure_errors,
            coverage=coverage,
            origin=origin,
            mint=mint_evidence,
            bonding_curve=curve_evidence,
            concentration=concentration,
            creator_evidence=creator_evidence,
            convergence_evidence=convergence_evidence,
            market=market,
            execution_evidence=execution,
            graduation=graduation,
            cohort=cohort,
            admission_state=admission_state,
        )


class SolanaShadowTrader:
    def __init__(
        self,
        state_path: str | Path,
        ledger: HashLedger,
        policy: SolanaShadowPolicy | None = None,
    ):
        self.state_path = Path(state_path)
        self.ledger = ledger
        self.policy = policy or SolanaShadowPolicy()
        self.policy_hash = hashlib.sha256(
            _canonical_json(asdict(self.policy)).encode("utf-8")
        ).hexdigest()
        self.state = self._load()

    def _load(self) -> dict:
        state = _read_json(
            self.state_path,
            {
                "schema_version": SCHEMA_VERSION,
                "paper_only": True,
                "live_execution_enabled": False,
                "policy": asdict(self.policy),
                "policy_hash": self.policy_hash,
                "positions": {},
            },
        )
        if state.get("policy_hash") != self.policy_hash:
            raise ValueError("existing Solana shadow state uses a different policy")
        return state

    def _save(self) -> None:
        self.state["updated_at"] = _utc_now()
        _atomic_json(self.state_path, self.state)

    def open_positions(self) -> list[dict]:
        return [
            value
            for value in self.state["positions"].values()
            if value.get("status") == "open"
        ]

    def enter(
        self, candidate: SolanaLaunchCandidate, decision: SolanaRiskDecision
    ) -> dict | None:
        if not decision.shadow_entry_allowed:
            return None
        existing = self.state["positions"].get(candidate.mint)
        if existing:
            return None
        if len(self.open_positions()) >= self.policy.maximum_positions:
            return None
        buy = (decision.execution_evidence or {}).get("buy") or {}
        token_raw = _safe_int(buy.get("out_amount"))
        if token_raw <= 0:
            return None
        cost = int(self.policy.amount_sol * 1_000_000_000)
        position = {
            "mint": candidate.mint,
            "symbol": candidate.symbol,
            "candidate": candidate.to_dict(),
            "status": "open",
            "opened_at": _utc_now(),
            "entry_cost_lamports": cost,
            "token_amount_raw": token_raw,
            "entry_score": decision.score,
            "entry_evidence_state": decision.evidence_state,
            "entry_quote": buy,
            "marks": [],
            "paper_only": True,
            "live_execution_enabled": False,
        }
        self.state["positions"][candidate.mint] = position
        self.ledger.append("solana_shadow_buy", {"position": position})
        self._save()
        return position

    def mark(
        self,
        mint: str,
        sell_quote: JupiterQuote,
        *,
        risk_signal: str | None = None,
        now: float | None = None,
    ) -> list[dict]:
        position = self.state["positions"].get(mint)
        if not position or position.get("status") != "open":
            return []
        current = now if now is not None else time.time()
        opened = _timestamp(position.get("opened_at")) or current
        proceeds = sell_quote.out_amount
        multiple = proceeds / max(1, position["entry_cost_lamports"])
        age = max(0.0, current - opened)
        mark = {
            "timestamp": datetime.fromtimestamp(current, timezone.utc).isoformat(),
            "proceeds_lamports": proceeds,
            "multiple": multiple,
            "age_seconds": age,
            "quote": sell_quote.to_dict(),
        }
        position["marks"].append(mark)
        reason = None
        if risk_signal:
            reason = "risk_signal:" + risk_signal
        elif multiple <= self.policy.stop_loss_multiple:
            reason = "stop_loss"
        elif multiple >= self.policy.take_profit_multiple:
            reason = "take_profit"
        elif age >= self.policy.maximum_hold_seconds:
            reason = "maximum_hold"
        events = []
        if reason:
            position.update(
                {
                    "status": "closed",
                    "closed_at": mark["timestamp"],
                    "exit_reason": reason,
                    "exit_proceeds_lamports": proceeds,
                    "net_pnl_lamports": proceeds - position["entry_cost_lamports"],
                    "net_multiple": multiple,
                }
            )
            events.append(
                self.ledger.append(
                    "solana_shadow_sell",
                    {
                        "mint": mint,
                        "reason": reason,
                        "proceeds_lamports": proceeds,
                        "net_pnl_lamports": position["net_pnl_lamports"],
                        "net_multiple": multiple,
                        "paper_only": True,
                    },
                )
            )
        else:
            self.ledger.append("solana_shadow_mark", {"mint": mint, "mark": mark})
        self._save()
        return events

    def broadcast_live_trade(self, *_args, **_kwargs):
        raise LiveExecutionDisabledError(
            "Solana live signing and broadcast are intentionally absent"
        )


class SolanaPromotionEvaluator:
    def __init__(
        self,
        analysis_index_path: str | Path,
        trader: SolanaShadowTrader,
        observation_ledger: HashLedger,
        shadow_ledger: HashLedger,
        policy: SolanaPromotionPolicy | None = None,
    ):
        self.analysis_index_path = Path(analysis_index_path)
        self.trader = trader
        self.observation_ledger = observation_ledger
        self.shadow_ledger = shadow_ledger
        self.policy = policy or SolanaPromotionPolicy()

    @staticmethod
    def _drawdown_pct(closed: list[dict]) -> float:
        equity = 0.0
        peak = 0.0
        maximum = 0.0
        basis = max(1.0, sum(_safe_float(row.get("entry_cost_lamports")) for row in closed))
        for row in sorted(closed, key=lambda item: item.get("closed_at") or ""):
            equity += _safe_float(row.get("net_pnl_lamports"))
            peak = max(peak, equity)
            maximum = max(maximum, peak - equity)
        return 100.0 * maximum / basis

    def evaluate(self) -> dict:
        analyses = _read_json(self.analysis_index_path, {}).get("tokens", {})
        all_observations = list(analyses.values())
        observations = [
            row
            for row in all_observations
            if (row.get("decision") or {}).get("cohort")
            == "graduated_market"
        ]
        closed = [
            row
            for row in self.trader.state["positions"].values()
            if row.get("status") == "closed"
        ]
        profits = [_safe_float(row.get("net_pnl_lamports")) for row in closed]
        costs = [_safe_float(row.get("entry_cost_lamports")) for row in closed]
        total_cost = sum(costs)
        net_return = 100.0 * sum(profits) / total_cost if total_cost else None
        if len(closed) > 1:
            best = max(range(len(profits)), key=profits.__getitem__)
            reduced_cost = total_cost - costs[best]
            without_best = (
                100.0 * (sum(profits) - profits[best]) / reduced_cost
                if reduced_cost
                else None
            )
        else:
            without_best = None
        profitable = sum(value > 0 for value in profits)
        winner_rate = 100.0 * profitable / len(closed) if closed else None
        quote_complete = sum(
            bool((row.get("decision") or {}).get("coverage", {}).get("jupiter_two_way_quote"))
            for row in observations
        )
        assembled = sum(
            bool((row.get("decision") or {}).get("coverage", {}).get("unsigned_buy_assembly"))
            for row in observations
        )
        indeterminate = sum(
            (row.get("decision") or {}).get("evidence_state")
            == "infrastructure_indeterminate"
            for row in observations
        )
        count = len(observations)
        # A zero-sized graduated cohort has no rate denominator. Reporting
        # 0% quote/assembly coverage or 100% infrastructure failures would
        # turn "not observed yet" into a false measured result. Keep these
        # rates explicitly unavailable until the cohort exists; the separate
        # minimum-observations gate remains fail-closed.
        quote_coverage = (
            100.0 * quote_complete / count if count else None
        )
        assembly_coverage = (
            100.0 * assembled / count if count else None
        )
        indeterminate_rate = (
            100.0 * indeterminate / count if count else None
        )
        observation_ok, observation_report = self.observation_ledger.verify()
        shadow_ok, shadow_report = self.shadow_ledger.verify()
        drawdown = self._drawdown_pct(closed)
        blockers = []
        if count < self.policy.minimum_observations:
            blockers.append("minimum_observations_not_met")
        if len(closed) < self.policy.minimum_closed_positions:
            blockers.append("minimum_closed_positions_not_met")
        if net_return is None or net_return <= 0:
            blockers.append("net_return_not_positive")
        if without_best is None or without_best <= 0:
            blockers.append("return_without_best_not_positive")
        if profitable < self.policy.minimum_profitable_positions:
            blockers.append("profitable_breadth_not_met")
        if winner_rate is None or winner_rate < self.policy.minimum_winner_rate_pct:
            blockers.append("winner_rate_not_met")
        if drawdown > self.policy.maximum_drawdown_pct:
            blockers.append("maximum_drawdown_exceeded")
        if (
            quote_coverage is not None
            and quote_coverage < self.policy.minimum_quote_coverage_pct
        ):
            blockers.append("two_way_quote_coverage_not_met")
        if (
            assembly_coverage is not None
            and assembly_coverage
            < self.policy.minimum_unsigned_assembly_coverage_pct
        ):
            blockers.append("unsigned_transaction_assembly_coverage_not_met")
        if (
            indeterminate_rate is not None
            and indeterminate_rate
            > self.policy.maximum_indeterminate_rate_pct
        ):
            blockers.append("infrastructure_indeterminate_rate_too_high")
        if not observation_ok or not shadow_ok:
            blockers.append("ledger_integrity_failed")
        return {
            "schema_version": SCHEMA_VERSION,
            "evaluated_at": _utc_now(),
            "status": "PROMOTABLE_FOR_REVIEW" if not blockers else "NOT_PROMOTABLE",
            "automatic_live_enable": False,
            "paper_only": True,
            "live_execution_enabled": False,
            "policy": asdict(self.policy),
            "metrics": {
                "observations": count,
                "all_launch_observations": len(all_observations),
                "graduated_market_observations": count,
                "closed_positions": len(closed),
                "profitable_positions": profitable,
                "winner_rate_pct": winner_rate,
                "net_return_pct": net_return,
                "return_without_best_pct": without_best,
                "maximum_drawdown_pct": drawdown,
                "two_way_quote_coverage_pct": quote_coverage,
                "unsigned_transaction_assembly_coverage_pct": assembly_coverage,
                "infrastructure_indeterminate_rate_pct": indeterminate_rate,
            },
            "integrity": {
                "observation_ledger": observation_report,
                "shadow_ledger": shadow_report,
            },
            "blockers": blockers,
        }


class SolanaTimechainRecorder:
    def __init__(self, chain_root: str | Path):
        skill_dir = _get_skill_dir()
        tc_module = _load_timechain_module(skill_dir)
        self.poq_module = _load_skill_module(skill_dir, "poq")
        self.tc = tc_module.Timechain(root=Path(chain_root))
        if self.tc.height() == 0:
            self.tc.genesis(name="Chainseer Solana")
        ok, report = self.tc.verify()
        if not ok:
            raise RuntimeError(f"Solana Timechain verification failed: {report}")

    def _find(self, key: str) -> dict | None:
        return next(
            (
                ring
                for ring in self.tc.iter_rings()
                if (ring.get("payload") or {}).get("idempotency_key") == key
            ),
            None,
        )

    def seal_analysis(
        self, candidate: SolanaLaunchCandidate, decision: SolanaRiskDecision
    ) -> int:
        key = f"solana-analysis:{candidate.signature}:{candidate.mint}"
        key += ":" + hashlib.sha256(
            (decision.analyzed_at + _canonical_json(decision.coverage)).encode("utf-8")
        ).hexdigest()[:16]
        summary = (
            f"Pump.fun launch {candidate.symbol} ({candidate.mint}) assessed as "
            f"{decision.risk_level} with score {decision.score}/100; "
            f"shadow entry {'allowed' if decision.shadow_entry_allowed else 'refused'}."
        )
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            summary,
            context=_canonical_json(
                {"candidate": candidate.to_dict(), "decision": decision.to_dict()}
            ),
            ring_type="solana_launch_analysis",
            external_scores={
                "coherence": 245,
                "relevance": 250,
                "novelty": 235,
                "consistency": 245
                if decision.evidence_state != "infrastructure_indeterminate"
                else 205,
                "depth": min(
                    250, 175 + 10 * sum(bool(value) for value in decision.coverage.values())
                ),
                "covenant": 255,
            },
            frame="assertion",
            evidence_texts=[
                _canonical_json(decision.origin),
                _canonical_json(decision.coverage),
                _canonical_json(decision.execution_evidence),
            ],
            extra_payload={
                "idempotency_key": key,
                "ecosystem": "pump_fun",
                "pump_public_docs_commit": PUMP_PUBLIC_DOCS_COMMIT,
                "candidate": candidate.to_dict(),
                "decision": decision.to_dict(),
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(f"PoQ refused Solana seal: {verdict.get('decision')}")
        decision.timechain_ring = ring["index"]
        return ring["index"]

    def seal_trade_event(self, event: dict) -> int:
        key = f"solana-trade:{event.get('event_hash')}"
        existing = self._find(key)
        if existing:
            return existing["index"]
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            f"Solana shadow event {event.get('event_type')} recorded at ledger index "
            f"{event.get('index')}; live execution remained disabled.",
            context=_canonical_json(event),
            ring_type="solana_shadow_event",
            external_scores={
                "coherence": 245,
                "relevance": 250,
                "novelty": 225,
                "consistency": 250,
                "depth": 235,
                "covenant": 255,
            },
            frame="assertion",
            evidence_texts=[_canonical_json(event)],
            extra_payload={
                "idempotency_key": key,
                "ledger_event": event,
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(f"PoQ refused Solana trade seal: {verdict.get('decision')}")
        return ring["index"]

    def seal_reflection_checkpoint(self, checkpoint: dict) -> int:
        key = f"solana-reflection:{checkpoint.get('checkpoint_id')}"
        existing = self._find(key)
        if existing:
            return existing["index"]
        reason = checkpoint.get("reason") or "analysis_interval"
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            "Solana recursive-learning checkpoint "
            f"{checkpoint.get('checkpoint_id')} requested after "
            f"{checkpoint.get('analysis_events')} committed analyses "
            f"({reason}); the paper-only learner paused before any code change.",
            context=_canonical_json(checkpoint),
            ring_type="solana_reflection_checkpoint",
            external_scores={
                "coherence": 250,
                "relevance": 250,
                "novelty": 235,
                "consistency": 250,
                "depth": 245,
                "covenant": 255,
            },
            frame="assertion",
            evidence_texts=[_canonical_json(checkpoint)],
            extra_payload={
                "idempotency_key": key,
                "checkpoint": checkpoint,
                "paper_only": True,
                "live_execution_enabled": False,
                "code_change_authorized": False,
            },
        )
        if ring is None:
            raise RuntimeError(
                f"PoQ refused reflection checkpoint: {verdict.get('decision')}"
            )
        return ring["index"]

    def seal_reflection_outcome(self, outcome: dict) -> int:
        key = (
            f"solana-reflection-outcome:{outcome.get('checkpoint_id')}:"
            f"{outcome.get('outcome')}"
        )
        existing = self._find(key)
        if existing:
            return existing["index"]
        verdict, ring = self.poq_module.gate_and_seal(
            self.tc,
            "Solana recursive-learning checkpoint "
            f"{outcome.get('checkpoint_id')} was reviewed with outcome "
            f"{outcome.get('outcome')}; regression and smoke evidence remain "
            "an external prerequisite before the scheduler resumes.",
            context=_canonical_json(outcome),
            ring_type="solana_reflection_outcome",
            external_scores={
                "coherence": 245,
                "relevance": 250,
                "novelty": 230,
                "consistency": 245,
                "depth": 240,
                "covenant": 255,
            },
            frame="assertion",
            evidence_texts=[_canonical_json(outcome)],
            extra_payload={
                "idempotency_key": key,
                "outcome": outcome,
                "paper_only": True,
                "live_execution_enabled": False,
            },
        )
        if ring is None:
            raise RuntimeError(
                f"PoQ refused reflection outcome: {verdict.get('decision')}"
            )
        return ring["index"]


class SolanaPrototypeEngine:
    def __init__(
        self,
        root: str | Path = "solana_learning",
        *,
        rpc_url: str = SOLANA_RPC_URL,
        jupiter_api_key: str | None = None,
        paper_taker: str | None = None,
        record_timechain: bool = True,
        chain_root: str | Path = "solana_chain",
        risk_policy: SolanaRiskPolicy | None = None,
        shadow_policy: SolanaShadowPolicy | None = None,
        promotion_policy: SolanaPromotionPolicy | None = None,
        rpc: SolanaRPC | None = None,
        jupiter: JupiterClient | None = None,
        dexscreener: DexScreenerClient | None = None,
        allow_public_rpc: bool = False,
    ):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.rpc_policy_path = self.root / "rpc_policy.json"
        self.observation_ledger = HashLedger(self.root / "observation_events.jsonl")
        self.shadow_ledger = HashLedger(self.root / "shadow_events.jsonl")
        if rpc is None:
            rpc_provider = _rpc_provider(rpc_url)
            rpc_policy = _read_json(self.rpc_policy_path, {})
            if (
                rpc_policy.get("require_configured_rpc") is True
                and rpc_provider == "solana_public"
                and not allow_public_rpc
            ):
                raise ConfiguredSolanaRpcRequiredError(
                    "This Solana learning root previously used a configured "
                    "RPC. Refusing silent fallback to the public Solana "
                    "endpoint. Restore CHAINSEER_SOLANA_RPC_URL or pass "
                    "--allow-public-rpc for an explicit temporary override."
                )
            self.rpc = SolanaRPC(rpc_url)
            if rpc_provider != "solana_public":
                _atomic_json(
                    self.rpc_policy_path,
                    {
                        "schema_version": SCHEMA_VERSION,
                        "require_configured_rpc": True,
                        "configured_provider": rpc_provider,
                        "configured_origin": _public_rpc_endpoint(rpc_url),
                        "updated_at": _utc_now(),
                        "credentials_persisted": False,
                    },
                )
        else:
            # Injected transports are used by tests and controlled adapters;
            # they own their own policy and never cause a credential marker.
            self.rpc = rpc
        self.jupiter = jupiter or JupiterClient(
            jupiter_api_key, paper_taker=paper_taker
        )
        self.dexscreener = dexscreener or DexScreenerClient()
        self.observer = PumpFunObserver(
            self.rpc, self.root, self.observation_ledger
        )
        self.convergence_tracker = WalletConvergenceTracker(
            self.root / "wallet_convergence.json"
        )
        self.analyzer = SolanaRiskAnalyzer(
            self.rpc,
            self.jupiter,
            risk_policy,
            self.dexscreener,
            self.convergence_tracker,
        )
        self.trader = SolanaShadowTrader(
            self.root / "shadow_state.json", self.shadow_ledger, shadow_policy
        )
        self.analysis_index_path = self.root / "analysis_index.json"
        self.recovery_queue_path = self.root / "recovery_queue.json"
        self.rpc_health_path = self.root / "rpc_health.json"
        self.reflection_state_path = self.root / "reflection_state.json"
        self.reflection_ledger = HashLedger(
            self.root / "reflection_checkpoints.jsonl"
        )
        self._rpc_health_checkpoint: dict = {}
        self.promotion = SolanaPromotionEvaluator(
            self.analysis_index_path,
            self.trader,
            self.observation_ledger,
            self.shadow_ledger,
            promotion_policy,
        )
        self.timechain = (
            SolanaTimechainRecorder(chain_root) if record_timechain else None
        )

    def _jupiter_health(self) -> dict:
        health = getattr(self.jupiter, "health", None)
        if callable(health):
            return health()
        return {
            "access_mode": (
                "api_key"
                if bool(getattr(self.jupiter, "api_key", None))
                else "test_or_injected"
            ),
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "retries": 0,
            "updated_at": _utc_now(),
        }

    def _dexscreener_health(self) -> dict:
        health = getattr(self.dexscreener, "health", None)
        if callable(health):
            return health()
        return {
            "attempts": 0,
            "successes": 0,
            "failures": 0,
            "cache_hits": 0,
            "updated_at": _utc_now(),
        }

    def _persist_rpc_health(self) -> dict:
        current = self.rpc.health()
        delta = _rpc_health_delta(current, self._rpc_health_checkpoint)
        existing = _read_json(self.rpc_health_path, {})
        merged = _merge_rpc_health(existing, delta, current)
        self._rpc_health_checkpoint = json.loads(_canonical_json(current))
        _atomic_json(self.rpc_health_path, merged)
        return merged

    def _analysis_event_count(self) -> int:
        return sum(
            row.get("event_type")
            in {"solana_risk_analysis", "solana_risk_reanalysis"}
            for row in self.observation_ledger.load()
        )

    def reflection_status(self) -> dict:
        state = _read_json(self.reflection_state_path, {})
        analysis_events = self._analysis_event_count()
        if not state:
            state = {
                "schema_version": SCHEMA_VERSION,
                "status": "armed",
                "analysis_interval": REFLECTION_ANALYSIS_INTERVAL,
                "minimum_seconds_between_reflections": REFLECTION_MIN_SECONDS,
                "baseline_analysis_events": analysis_events,
                "last_reflected_analysis_events": analysis_events,
                "next_analysis_checkpoint": (
                    analysis_events + REFLECTION_ANALYSIS_INTERVAL
                ),
                "first_graduated_market_seen": False,
                "pause_requested": False,
                "paper_only": True,
                "live_execution_enabled": False,
                "updated_at": _utc_now(),
            }
            _atomic_json(self.reflection_state_path, state)
        state["analysis_events"] = analysis_events
        state["analyses_until_checkpoint"] = max(
            0,
            _safe_int(
                state.get("next_analysis_checkpoint"),
                analysis_events + REFLECTION_ANALYSIS_INTERVAL,
            )
            - analysis_events,
        )
        return state

    def assert_learning_allowed(self) -> None:
        state = self.reflection_status()
        if state.get("pause_requested") or state.get("status") == "pending":
            raise ReflectionCheckpointPending(
                "A sealed recursive-learning reflection checkpoint is pending. "
                "No further analysis will run until it is reviewed and acknowledged."
            )

    def _maybe_request_reflection(self) -> dict:
        state = self.reflection_status()
        if state.get("pause_requested") or state.get("status") == "pending":
            return state
        analyses = _read_json(
            self.analysis_index_path, {}
        ).get("tokens", {})
        graduated = sorted(
            mint
            for mint, row in analyses.items()
            if (row.get("decision") or {}).get("cohort")
            == "graduated_market"
            and (
                (row.get("decision") or {}).get("graduation") or {}
            ).get("canonical_pool_verified_on_chain")
        )
        first_graduated = bool(graduated) and not bool(
            state.get("first_graduated_market_seen")
        )
        analysis_events = self._analysis_event_count()
        interval_due = analysis_events >= _safe_int(
            state.get("next_analysis_checkpoint"),
            analysis_events + REFLECTION_ANALYSIS_INTERVAL,
        )
        if not first_graduated and not interval_due:
            state["analysis_events"] = analysis_events
            state["analyses_until_checkpoint"] = max(
                0,
                _safe_int(state.get("next_analysis_checkpoint"))
                - analysis_events,
            )
            state["updated_at"] = _utc_now()
            _atomic_json(self.reflection_state_path, state)
            return state
        checkpoint_id = hashlib.sha256(
            (
                f"{analysis_events}:{graduated[0] if graduated else ''}:"
                f"{state.get('last_checkpoint_id') or ''}"
            ).encode("utf-8")
        ).hexdigest()[:16]
        checkpoint = {
            "checkpoint_id": checkpoint_id,
            "requested_at": _utc_now(),
            "reason": (
                "first_canonical_graduated_market"
                if first_graduated
                else "analysis_interval"
            ),
            "analysis_events": analysis_events,
            "analysis_interval": REFLECTION_ANALYSIS_INTERVAL,
            "graduated_market_count": len(graduated),
            "first_graduated_mint": graduated[0] if graduated else None,
            "observation_ledger_head": (
                self.observation_ledger.load()[-1].get("event_hash")
                if self.observation_ledger.load()
                else None
            ),
            "paper_only": True,
            "live_execution_enabled": False,
            "automatic_code_change": False,
        }
        event = self.reflection_ledger.append(
            "solana_reflection_requested", checkpoint
        )
        checkpoint["reflection_ledger_event_hash"] = event["event_hash"]
        if self.timechain:
            checkpoint["solana_timechain_ring"] = (
                self.timechain.seal_reflection_checkpoint(checkpoint)
            )
        state.update(
            {
                "status": "pending",
                "pause_requested": True,
                "pending_checkpoint": checkpoint,
                "last_checkpoint_id": checkpoint_id,
                "analysis_events": analysis_events,
                "analyses_until_checkpoint": 0,
                "updated_at": _utc_now(),
            }
        )
        _atomic_json(self.reflection_state_path, state)
        return state

    def acknowledge_reflection(self, outcome: str, summary: str) -> dict:
        if outcome not in {"applied", "no_change"}:
            raise ValueError("reflection outcome must be applied or no_change")
        state = self.reflection_status()
        pending = state.get("pending_checkpoint") or {}
        if state.get("status") != "pending" or not pending:
            raise ValueError("no pending reflection checkpoint exists")
        analysis_events = self._analysis_event_count()
        outcome_record = {
            "checkpoint_id": pending.get("checkpoint_id"),
            "outcome": outcome,
            "summary": _redact_sensitive_text(summary)[:1000],
            "acknowledged_at": _utc_now(),
            "analysis_events": analysis_events,
            "paper_only": True,
            "live_execution_enabled": False,
        }
        event = self.reflection_ledger.append(
            "solana_reflection_acknowledged", outcome_record
        )
        if self.timechain:
            outcome_record["solana_timechain_ring"] = (
                self.timechain.seal_reflection_outcome(outcome_record)
            )
        state.update(
            {
                "status": "armed",
                "pause_requested": False,
                "pending_checkpoint": None,
                "last_reflection_outcome": outcome,
                "last_reflection_summary": _redact_sensitive_text(summary)[
                    :1000
                ],
                "last_reflection_at": _utc_now(),
                "last_reflection_ledger_event_hash": event["event_hash"],
                "last_reflected_analysis_events": analysis_events,
                "next_analysis_checkpoint": (
                    analysis_events + REFLECTION_ANALYSIS_INTERVAL
                ),
                "first_graduated_market_seen": bool(
                    state.get("first_graduated_market_seen")
                    or pending.get("first_graduated_mint")
                ),
                "analysis_events": analysis_events,
                "analyses_until_checkpoint": REFLECTION_ANALYSIS_INTERVAL,
                "updated_at": _utc_now(),
            }
        )
        _atomic_json(self.reflection_state_path, state)
        return state

    def _load_recovery_queue(self) -> dict:
        value = _read_json(
            self.recovery_queue_path,
            {
                "schema_version": SCHEMA_VERSION,
                "items": {},
                "updated_at": None,
            },
        )
        if not isinstance(value.get("items"), dict):
            value["items"] = {}
        return value

    def _save_recovery_queue(self, queue: dict) -> None:
        # Resolved items are kept forever otherwise (only ever marked
        # "resolved", never removed) -- the same unbounded-growth shape as
        # catalog.json/analysis_index.json. Pending items are never pruned
        # here regardless of age: an old still-pending item is exactly the
        # thing this queue exists to keep retrying.
        retention_cutoff = time.time() - CATALOG_RETENTION_SECONDS
        queue["items"] = {
            mint: item
            for mint, item in queue["items"].items()
            if item.get("status") != "resolved"
            or (_timestamp(item.get("resolved_at")) or 0) >= retention_cutoff
        }
        queue["schema_version"] = SCHEMA_VERSION
        queue["updated_at"] = _utc_now()
        _atomic_json(self.recovery_queue_path, queue)

    @staticmethod
    def _needs_admission_schema_migration(decision: dict) -> bool:
        """Return true when a persisted decision predates cohort admission."""
        return not decision.get("cohort") or not decision.get(
            "admission_state"
        )

    @staticmethod
    def _is_recoverable(decision: dict | SolanaRiskDecision) -> bool:
        """A decision is recoverable (worth re-analyzing later) when it is not
        yet judgeable, or when a launch is still progressing toward the separately
        evidenced graduated-market cohort."""
        evidence_state = (
            decision.evidence_state if isinstance(decision, SolanaRiskDecision)
            else decision.get("evidence_state")
        )
        if evidence_state == "infrastructure_indeterminate":
            return True
        concentration = (
            decision.concentration if isinstance(decision, SolanaRiskDecision)
            else decision.get("concentration")
        ) or {}
        if concentration.get("distribution_stage") == "pre_distribution":
            return True
        admission_state = (
            decision.admission_state
            if isinstance(decision, SolanaRiskDecision)
            else decision.get("admission_state")
        )
        if admission_state is None and not isinstance(
            decision, SolanaRiskDecision
        ):
            # Compatible migration path for decisions written before the
            # graduated-market cohort existed. Revisit safe rows and rows whose
            # only blockers can genuinely change as a market matures.
            if evidence_state == "complete_safe":
                return True
            transient_prefixes = (
                "top1_circulating_concentration_high",
                "top10_circulating_concentration_high",
                "jupiter_roundtrip_retention_low",
                "jupiter_buy_price_impact_high",
            )
            hard_stops = decision.get("hard_stops") or []
            if hard_stops and all(
                str(stop).startswith(transient_prefixes)
                for stop in hard_stops
            ):
                return True
        return admission_state in {
            "graduation_pending",
            "canonical_migration_pending",
            "market_indexing_pending",
            "execution_evidence_pending",
            "distribution_pending",
            "market_age_pending",
        }

    def _seed_recovery_queue(self) -> int:
        queue = self._load_recovery_queue()
        index = _read_json(self.analysis_index_path, {}).get("tokens", {})
        added = 0
        for mint, row in index.items():
            decision = row.get("decision") or {}
            candidate_value = row.get("candidate") or {}
            needs_migration = self._needs_admission_schema_migration(
                decision
            )
            if (
                not (needs_migration or self._is_recoverable(decision))
                or not candidate_value
            ):
                continue
            now = _utc_now()
            concentration = decision.get("concentration") or {}
            existing = queue["items"].get(mint)
            if existing:
                if not needs_migration:
                    continue
                # A legacy item may have been marked resolved by the old
                # recovery policy even though its persisted decision still
                # lacks the cohort/admission fields. Re-open it until the
                # analysis index itself contains the migrated decision.
                existing["candidate"] = candidate_value
                existing["status"] = "pending"
                existing["updated_at"] = now
                existing["next_attempt_at"] = now
                existing["last_evidence_state"] = decision.get(
                    "evidence_state"
                )
                existing["last_admission_state"] = decision.get(
                    "admission_state"
                )
                existing["last_distribution_stage"] = concentration.get(
                    "distribution_stage"
                )
                existing["last_graduation_progress_pct"] = _safe_float(
                    (decision.get("graduation") or {}).get("progress_pct"),
                    None,
                )
                existing["last_errors"] = [
                    _redact_sensitive_text(value)
                    for value in decision.get("infrastructure_errors") or []
                ]
                added += 1
                continue
            queue["items"][mint] = {
                "mint": mint,
                "candidate": candidate_value,
                "status": "pending",
                "attempts": 0,
                "first_queued_at": now,
                "updated_at": now,
                "next_attempt_at": now,
                "last_evidence_state": decision.get("evidence_state"),
                "last_admission_state": decision.get("admission_state"),
                "last_distribution_stage": concentration.get("distribution_stage"),
                "last_graduation_progress_pct": _safe_float(
                    (decision.get("graduation") or {}).get("progress_pct"),
                    None,
                ),
                "last_errors": [
                    _redact_sensitive_text(value)
                    for value in decision.get("infrastructure_errors") or []
                ],
            }
            added += 1
        if added:
            self._save_recovery_queue(queue)
        return added

    def _update_recovery_queue(
        self,
        candidate: SolanaLaunchCandidate,
        decision: SolanaRiskDecision,
        *,
        recovery: bool,
    ) -> None:
        queue = self._load_recovery_queue()
        item = queue["items"].get(candidate.mint)
        if self._is_recoverable(decision):
            now = _utc_now()
            item = item or {
                "mint": candidate.mint,
                "candidate": candidate.to_dict(),
                "status": "pending",
                "attempts": 0,
                "first_queued_at": now,
            }
            item["candidate"] = candidate.to_dict()
            item["status"] = "pending"
            if recovery:
                item["attempts"] = _safe_int(item.get("attempts")) + 1
            progress = _safe_float(
                (decision.graduation or {}).get("progress_pct"), None
            )
            if not recovery:
                delay = 0
            elif progress is not None and progress >= 99:
                delay = 60
            elif progress is not None and progress >= 90:
                delay = 2 * 60
            elif progress is not None and progress >= 75:
                delay = 5 * 60
            elif progress is not None and progress >= 50:
                delay = 15 * 60
            else:
                delay = min(
                    6 * 3600,
                    300 * (2 ** max(0, item["attempts"] - 1)),
                )
            item["next_attempt_at"] = datetime.fromtimestamp(
                time.time() + delay, timezone.utc
            ).isoformat()
            item["updated_at"] = now
            item["last_evidence_state"] = decision.evidence_state
            item["last_admission_state"] = decision.admission_state
            item["last_distribution_stage"] = (
                decision.concentration or {}
            ).get("distribution_stage")
            item["last_graduation_progress_pct"] = progress
            item["last_errors"] = [
                _redact_sensitive_text(value)
                for value in decision.infrastructure_errors
            ]
            item["last_timechain_ring"] = decision.timechain_ring
            queue["items"][candidate.mint] = item
            self._save_recovery_queue(queue)
            return
        if item and item.get("status") != "resolved":
            item["status"] = "resolved"
            item["resolved_at"] = _utc_now()
            item["updated_at"] = item["resolved_at"]
            item["last_evidence_state"] = decision.evidence_state
            item["last_errors"] = []
            item["last_timechain_ring"] = decision.timechain_ring
            self._save_recovery_queue(queue)

    def _recovery_summary(self) -> dict:
        values = list(self._load_recovery_queue()["items"].values())
        return {
            "total": len(values),
            "pending": sum(row.get("status") == "pending" for row in values),
            "resolved": sum(row.get("status") == "resolved" for row in values),
            "due": sum(
                row.get("status") == "pending"
                and (_timestamp(row.get("next_attempt_at")) or 0)
                <= time.time()
                for row in values
            ),
        }

    def _probe_graduation_candidates(
        self, *, limit: int, exclude_mints: set[str] | None = None
    ) -> tuple[list[SolanaLaunchCandidate], dict]:
        """Batch-probe confirmed Pump launches for curve completion.

        This is a discovery lane, not an admission shortcut. Candidates remain
        bound to their confirmed CreateEvent, and full analysis still requires
        the completed curve, canonical index-0 PumpSwap pool, DexScreener pair,
        and Jupiter execution evidence before shadow admission.
        """
        stats = {
            "scanned": 0,
            "completed": 0,
            "selected": 0,
            "rpc_batches": 0,
            "errors": 0,
        }
        if limit <= 0:
            return [], stats
        exclude = exclude_mints or set()
        analyses = _read_json(
            self.analysis_index_path, {}
        ).get("tokens", {})
        candidates: list[SolanaLaunchCandidate] = []
        for mint, row in analyses.items():
            if mint in exclude:
                continue
            decision = row.get("decision") or {}
            if decision.get("admission_state") not in {
                "graduation_pending",
                "canonical_migration_pending",
                "market_indexing_pending",
                "execution_evidence_pending",
                "distribution_pending",
                "market_age_pending",
            }:
                continue
            try:
                candidates.append(
                    SolanaLaunchCandidate.from_dict(row["candidate"])
                )
            except (KeyError, TypeError):
                continue
        if not candidates:
            return [], stats

        queue = self._load_recovery_queue()
        probed: list[tuple[bool, float, SolanaLaunchCandidate]] = []
        for offset in range(0, len(candidates), 100):
            batch = candidates[offset : offset + 100]
            try:
                response = self.rpc.get_multiple_accounts(
                    [item.bonding_curve for item in batch],
                    encoding="base64",
                )
                stats["rpc_batches"] += 1
            except InfrastructureIndeterminateError:
                stats["errors"] += 1
                continue
            values = (response or {}).get("value") or []
            for candidate, account in zip(batch, values):
                stats["scanned"] += 1
                try:
                    curve = self.analyzer._decode_curve({"value": account})
                except InfrastructureIndeterminateError:
                    stats["errors"] += 1
                    continue
                current = _safe_int(
                    curve.get("real_token_reserves"), -1
                )
                progress = (
                    100.0
                    * max(
                        0.0,
                        min(
                            1.0,
                            1.0
                            - current / candidate.real_token_reserves,
                        ),
                    )
                    if candidate.real_token_reserves > 0 and current >= 0
                    else 0.0
                )
                completed = bool(curve.get("complete")) and current == 0
                if completed:
                    stats["completed"] += 1
                item = queue["items"].get(candidate.mint)
                if item:
                    item["last_graduation_progress_pct"] = round(
                        progress, 4
                    )
                    item["last_curve_probe_at"] = _utc_now()
                    item["last_curve_completed"] = completed
                probed.append((completed, progress, candidate))
        if probed:
            self._save_recovery_queue(queue)
        selected = [
            candidate
            for _completed, _progress, candidate in sorted(
                probed,
                key=lambda item: (
                    not item[0],
                    -item[1],
                    -item[2].slot,
                ),
            )[:limit]
        ]
        stats["selected"] = len(selected)
        return selected, stats

    def _recover_indeterminate(
        self, *, limit: int, shadow_enter: bool
    ) -> list[dict]:
        if limit <= 0:
            return []
        queue = self._load_recovery_queue()
        now = time.time()
        due = sorted(
            (
                row
                for row in queue["items"].values()
                if row.get("status") == "pending"
                and (_timestamp(row.get("next_attempt_at")) or 0) <= now
            ),
            key=lambda row: (
                # Complete the admission-schema migration before ordinary
                # cadence work so dashboards never mix legacy and v2 states
                # longer than the bounded recovery capacity requires.
                0 if not row.get("last_admission_state") else 1,
                -_safe_float(
                    row.get("last_graduation_progress_pct"), -1.0
                ),
                _safe_int(row.get("attempts")),
                row.get("first_queued_at") or "",
            ),
        )[:limit]
        results = []
        for row in due:
            try:
                candidate = SolanaLaunchCandidate(**row["candidate"])
            except (KeyError, TypeError):
                latest = self._load_recovery_queue()
                invalid = latest["items"].get(row.get("mint")) or row
                invalid["status"] = "invalid"
                invalid["updated_at"] = _utc_now()
                invalid["last_errors"] = [
                    "stored candidate schema is invalid"
                ]
                latest["items"][invalid.get("mint", "invalid")] = invalid
                self._save_recovery_queue(latest)
                continue
            results.append(
                self.evaluate_candidate(
                    candidate,
                    shadow_enter=shadow_enter,
                    recovery=True,
                )
            )
        return results

    def _record_analysis(
        self,
        candidate: SolanaLaunchCandidate,
        decision: SolanaRiskDecision,
        *,
        recovery: bool,
    ) -> None:
        index = _read_json(
            self.analysis_index_path,
            {"schema_version": SCHEMA_VERSION, "tokens": {}},
        )
        previous = index["tokens"].get(candidate.mint) or {}
        previous_state = (previous.get("decision") or {}).get(
            "evidence_state"
        )
        index["tokens"][candidate.mint] = {
            "candidate": candidate.to_dict(),
            "decision": decision.to_dict(),
            "updated_at": _utc_now(),
        }
        # Same unbounded-growth shape as catalog.json: every analyzed mint
        # would otherwise stay forever. A recoverable/pending token is not
        # lost by this -- _seed_recovery_queue() runs every learn cycle and
        # copies any still-pending mint's candidate into recovery_queue.json
        # (which is not pruned by age) well before it ages out here.
        retention_cutoff = time.time() - CATALOG_RETENTION_SECONDS
        index["tokens"] = {
            mint: row
            for mint, row in index["tokens"].items()
            if _safe_int((row.get("candidate") or {}).get("block_time"))
            >= retention_cutoff
        }
        index["updated_at"] = _utc_now()
        _atomic_json(self.analysis_index_path, index)
        self.observation_ledger.append(
            "solana_risk_reanalysis" if recovery else "solana_risk_analysis",
            {
                "mint": candidate.mint,
                "signature": candidate.signature,
                "previous_evidence_state": previous_state,
                "decision": decision.to_dict(),
                "recovery": recovery,
            },
        )
        self._update_recovery_queue(
            candidate, decision, recovery=recovery
        )
        # Propagate the finalized analysis outcome into the wallet-convergence
        # registry so per-wallet hit-rates track the latest verdict (not the
        # first observation). This is what makes the registry outcome-aware
        # without re-deriving it each time.
        if decision.evidence_state:
            self.convergence_tracker.regrade(
                candidate.mint, decision.evidence_state
            )

    def evaluate_candidate(
        self,
        candidate: SolanaLaunchCandidate,
        *,
        shadow_enter: bool = False,
        recovery: bool = False,
    ) -> dict:
        decision = self.analyzer.analyze(candidate)
        if self.timechain:
            self.timechain.seal_analysis(candidate, decision)
        self._record_analysis(candidate, decision, recovery=recovery)
        before = len(self.shadow_ledger.load())
        position = self.trader.enter(candidate, decision) if shadow_enter else None
        if self.timechain and len(self.shadow_ledger.load()) > before:
            self.timechain.seal_trade_event(self.shadow_ledger.load()[-1])
        action = (
            "shadow_position_opened"
            if position
            else (
                "risk_gate_refused"
                if not decision.shadow_entry_allowed
                else ("observation_only" if not shadow_enter else "portfolio_gate_wait")
            )
        )
        return {
            "candidate": candidate.to_dict(),
            "decision": decision.to_dict(),
            "shadow_action": action,
            "shadow_position": position,
            "recovery": recovery,
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def observe(
        self, *, limit: int = 10, signature_limit: int = 100,
        slot_span: int | None = None, max_pages: int = 10,
    ) -> list[dict]:
        discovered = self.observer.sync(
            signature_limit=signature_limit, slot_span=slot_span, max_pages=max_pages
        )
        candidates = discovered[-limit:] if discovered else self.observer.recent(limit)
        results = [self.evaluate_candidate(candidate) for candidate in candidates]
        _atomic_json(self.root / "last_observe.json", results)
        self._persist_rpc_health()
        _atomic_json(
            self.root / "jupiter_health.json", self._jupiter_health()
        )
        _atomic_json(
            self.root / "dexscreener_health.json",
            self._dexscreener_health(),
        )
        return results

    def _mark_open_positions(self, decisions: dict[str, SolanaRiskDecision]) -> int:
        events = 0
        for position in list(self.trader.open_positions()):
            mint = position["mint"]
            risk = decisions.get(mint)
            signal = None
            if risk and risk.evidence_state == "complete_unsafe":
                signal = ",".join(risk.hard_stops) or "risk_deterioration"
            try:
                quote = self.jupiter.quote(
                    mint, WRAPPED_SOL_MINT, _safe_int(position["token_amount_raw"])
                )
            except InfrastructureIndeterminateError:
                continue
            before = len(self.shadow_ledger.load())
            self.trader.mark(mint, quote, risk_signal=signal)
            after_rows = self.shadow_ledger.load()
            if len(after_rows) > before:
                events += len(after_rows) - before
                if self.timechain:
                    for event in after_rows[before:]:
                        self.timechain.seal_trade_event(event)
        return events

    def learn_once(
        self,
        *,
        limit: int = 10,
        signature_limit: int = 100,
        recovery_limit: int = 3,
        graduation_limit: int = 3,
        slot_span: int | None = None,
        max_pages: int = 10,
    ) -> dict:
        self.assert_learning_allowed()
        recovery_seeded = self._seed_recovery_queue()
        recovered = self._recover_indeterminate(
            limit=max(0, recovery_limit), shadow_enter=True
        )
        discovered = self.observer.sync(
            signature_limit=signature_limit, slot_span=slot_span, max_pages=max_pages
        )
        recovered_mints = {
            result["candidate"]["mint"] for result in recovered
        }
        graduation_candidates, graduation_probe = (
            self._probe_graduation_candidates(
                limit=max(0, graduation_limit),
                exclude_mints=recovered_mints,
            )
        )
        candidate_map = {
            candidate.mint: candidate
            for candidate in graduation_candidates
        }
        candidate_map.update({
            candidate.mint: candidate
            for candidate in discovered[-limit:]
            if candidate.mint not in recovered_mints
        })
        for position in self.trader.open_positions():
            candidate = self.observer.by_mint(position["mint"])
            if candidate and candidate.mint not in recovered_mints:
                candidate_map[candidate.mint] = candidate
        results = list(recovered)
        decisions = {
            result["candidate"]["mint"]: SolanaRiskDecision(
                **result["decision"]
            )
            for result in recovered
        }
        for candidate in candidate_map.values():
            result = self.evaluate_candidate(candidate, shadow_enter=True)
            results.append(result)
            decisions[candidate.mint] = SolanaRiskDecision(
                **result["decision"]
            )
        shadow_events = self._mark_open_positions(decisions)
        promotion = self.promotion.evaluate()
        _atomic_json(self.root / "promotion_status.json", promotion)
        calibration = self.concentration_calibration()
        _atomic_json(self.root / "concentration_calibration.json", calibration)
        rpc_health = self._persist_rpc_health()
        recovery_summary = self._recovery_summary()
        admission_states = {
            name: sum(
                result["decision"].get("admission_state") == name
                for result in results
            )
            for name in (
                "graduation_pending",
                "canonical_migration_pending",
                "market_indexing_pending",
                "execution_evidence_pending",
                "distribution_pending",
                "market_age_pending",
                "graduated_market_ready",
                "graduated_market_unsafe",
                "launch_observation_unsafe",
                "infrastructure_indeterminate",
            )
        }
        summary = {
            "schema_version": SCHEMA_VERSION,
            "timestamp": _utc_now(),
            "ecosystem": "pump_fun",
            "cycle": {
                "new_launches": len(discovered),
                "analyzed": len(results),
                "new_analyzed": len(results) - len(recovered),
                "recovery_analyzed": len(recovered),
                "recovery_resolved": sum(
                    not self._is_recoverable(result["decision"])
                    for result in recovered
                ),
                "recovery_seeded": recovery_seeded,
                "shadow_events": shadow_events,
                "shadow_open": len(self.trader.open_positions()),
                "shadow_closed": sum(
                    row.get("status") == "closed"
                    for row in self.trader.state["positions"].values()
                ),
                "curve_completed": sum(
                    bool(
                        (result["decision"].get("graduation") or {}).get(
                            "completion_verified_on_chain"
                        )
                    )
                    for result in results
                ),
                "canonical_pools": sum(
                    bool(
                        (result["decision"].get("graduation") or {}).get(
                            "canonical_pool_verified_on_chain"
                        )
                    )
                    for result in results
                ),
                "graduated_markets": sum(
                    result["decision"].get("cohort")
                    == "graduated_market"
                    and bool(
                        (result["decision"].get("graduation") or {}).get(
                            "secondary_market_observed"
                        )
                    )
                    for result in results
                ),
                "graduation_probe": graduation_probe,
            },
            "evidence_states": {
                name: sum(
                    result["decision"]["evidence_state"] == name for result in results
                )
                for name in (
                    "complete_safe",
                    "complete_unsafe",
                    "distribution_pending",
                    "infrastructure_indeterminate",
                )
            },
            "admission_states": admission_states,
            "promotion": promotion,
            "concentration_calibration": calibration,
            "wallet_convergence": self.convergence_tracker.snapshot(),
            "recovery": recovery_summary,
            "rpc_health": rpc_health,
            "jupiter_health": self._jupiter_health(),
            "dexscreener_health": self._dexscreener_health(),
            "paper_only": True,
            "live_execution_enabled": False,
        }
        _atomic_json(self.root / "learning_summary.json", summary)
        _atomic_json(
            self.root / "jupiter_health.json", self._jupiter_health()
        )
        _atomic_json(
            self.root / "dexscreener_health.json",
            self._dexscreener_health(),
        )
        reflection = self._maybe_request_reflection()
        summary["reflection"] = reflection
        _atomic_json(self.root / "learning_summary.json", summary)
        return summary

    def verify(self) -> dict:
        observation_ok, observation_report = self.observation_ledger.verify()
        shadow_ok, shadow_report = self.shadow_ledger.verify()
        reflection_ok, reflection_report = self.reflection_ledger.verify()
        chain_ok = True
        chain_report = "timechain disabled"
        if self.timechain:
            chain_ok, chain_report = self.timechain.tc.verify()
        state = self.trader.state
        state_ok = (
            state.get("paper_only") is True
            and state.get("live_execution_enabled") is False
            and state.get("policy_hash") == self.trader.policy_hash
        )
        return {
            "ok": (
                observation_ok
                and shadow_ok
                and reflection_ok
                and chain_ok
                and state_ok
            ),
            "observation_ledger": observation_report,
            "shadow_ledger": shadow_report,
            "reflection_ledger": reflection_report,
            "timechain": chain_report,
            "state_policy_bound": state_ok,
            "paper_only": True,
            "live_execution_enabled": False,
        }

    # ── Concentration calibration ──────────────────────────────────────────
    #
    # This is a DESCRIPTIVE report over observed analyses, not a validator.
    # It characterizes the empirical holder-concentration distribution and
    # shows how the admission rate moves as the policy threshold varies —
    # so a human can see whether the current gate is plausibly in range for
    # this ecosystem. It does NOT validate the threshold (no realized
    # outcomes to correlate against yet) and it does NOT auto-tune policy.
    _CALIBRATION_TOP1_GRID = (10, 20, 30, 40, 50, 60, 80)
    _CALIBRATION_TOP10_GRID = (40, 50, 65, 80, 90)
    _CALIBRATION_RETENTION_GRID = (60, 72, 80, 90, 95)

    def concentration_calibration(self) -> dict:
        analyses = _read_json(self.analysis_index_path, {}).get("tokens", {})
        all_rows = list(analyses.values())
        rows = [
            row
            for row in all_rows
            if (row.get("decision") or {}).get("cohort")
            == "graduated_market"
        ]
        active_policy = asdict(self.analyzer.policy)

        def _pct(value):
            value = _safe_float(value, None)
            return value if value is not None and math.isfinite(value) else None

        top1_values = []
        top10_values = []
        retention_values = []
        unresolved = 0
        distribution_stage_counts = {"pre_distribution": 0, "measurable": 0, "unresolved": 0}
        cross = []  # (top1, top10, retention) for tokens with all three
        for row in rows:
            decision = row.get("decision") or {}
            concentration = decision.get("concentration") or {}
            top1 = _pct(concentration.get("top1_circulating_pct"))
            top10 = _pct(concentration.get("top10_circulating_pct"))
            execution = decision.get("execution_evidence") or {}
            retention = _pct(execution.get("roundtrip_retention_pct"))
            stage = concentration.get("distribution_stage")
            if stage in distribution_stage_counts:
                distribution_stage_counts[stage] += 1
            if top1 is None or top10 is None:
                unresolved += 1
            else:
                top1_values.append(top1)
                top10_values.append(top10)
            if retention is not None:
                retention_values.append(retention)
            if top1 is not None and retention is not None:
                cross.append((top1, top10, retention))

        def _bucket(values, edges):
            counts = {f"<= {edge}%": 0 for edge in edges}
            counts[f"> {edges[-1]}%"] = 0
            for value in values:
                placed = False
                for edge in edges:
                    if value <= edge:
                        counts[f"<= {edge}%"] += 1
                        placed = True
                        break
                if not placed:
                    counts[f"> {edges[-1]}%"] += 1
            return counts

        def _admit_rate(values, threshold, higher_is_worse):
            if not values:
                return None
            admitted = sum(
                1 for value in values
                if (value <= threshold if higher_is_worse else value >= threshold)
            )
            return round(100.0 * admitted / len(values), 1)

        top1_grid = [
            {"threshold_pct": t, "admit_rate_pct": _admit_rate(top1_values, t, True)}
            for t in self._CALIBRATION_TOP1_GRID
        ]
        top10_grid = [
            {"threshold_pct": t, "admit_rate_pct": _admit_rate(top10_values, t, True)}
            for t in self._CALIBRATION_TOP10_GRID
        ]
        retention_grid = [
            {"threshold_pct": t, "admit_rate_pct": _admit_rate(retention_values, t, False)}
            for t in self._CALIBRATION_RETENTION_GRID
        ]

        # Joint gate: would a token pass BOTH concentration AND retention?
        joint_admit = 0
        for top1, _top10, retention in cross:
            if (top1 <= active_policy["maximum_top1_circulating_pct"]
                    and retention >= active_policy["minimum_roundtrip_retention_pct"]):
                joint_admit += 1
        joint_rate = round(100.0 * joint_admit / len(cross), 1) if cross else None

        # Hard-stop frequency — shows which gates actually fire.
        hard_stop_counts = {}
        for row in rows:
            for stop in (row.get("decision") or {}).get("hard_stops") or []:
                hard_stop_counts[stop] = hard_stop_counts.get(stop, 0) + 1
        hard_stop_frequency = sorted(
            ({"hard_stop": k, "count": v} for k, v in hard_stop_counts.items()),
            key=lambda item: item["count"],
            reverse=True,
        )

        return {
            "schema_version": SCHEMA_VERSION,
            "generated_at": _utc_now(),
            "ecosystem": "pump_fun",
            "paper_only": True,
            "live_execution_enabled": False,
            "status": "DESCRIPTIVE_NOT_VALIDATING",
            "observations": len(rows),
            "all_launch_observations": len(all_rows),
            "cohort": "graduated_market",
            "concentration_resolved": len(top1_values),
            "concentration_unresolved": unresolved,
            "distribution_stage_counts": distribution_stage_counts,
            "active_policy": active_policy,
            "distribution": {
                "top1_circulating_pct": {
                    "buckets": _bucket(top1_values, self._CALIBRATION_TOP1_GRID),
                    "min": round(min(top1_values), 2) if top1_values else None,
                    "median": round(sorted(top1_values)[len(top1_values) // 2], 2) if top1_values else None,
                    "max": round(max(top1_values), 2) if top1_values else None,
                },
                "top10_circulating_pct": {
                    "buckets": _bucket(top10_values, self._CALIBRATION_TOP10_GRID),
                    "min": round(min(top10_values), 2) if top10_values else None,
                    "median": round(sorted(top10_values)[len(top10_values) // 2], 2) if top10_values else None,
                    "max": round(max(top10_values), 2) if top10_values else None,
                },
                "roundtrip_retention_pct": {
                    "buckets": _bucket(retention_values, self._CALIBRATION_RETENTION_GRID),
                    "min": round(min(retention_values), 2) if retention_values else None,
                    "median": round(sorted(retention_values)[len(retention_values) // 2], 2) if retention_values else None,
                    "max": round(max(retention_values), 2) if retention_values else None,
                },
            },
            "threshold_sensitivity": {
                "top1_circulating_pct": top1_grid,
                "top10_circulating_pct": top10_grid,
                "roundtrip_retention_pct": retention_grid,
            },
            "joint_gate_at_active_policy": {
                "observations_with_all_axes": len(cross),
                "admitted": joint_admit,
                "admit_rate_pct": joint_rate,
            },
            "hard_stop_frequency": hard_stop_frequency,
            "caveats": [
                "This report characterizes the empirical distribution; it does "
                "NOT validate the threshold. Threshold validation requires "
                "realized shadow-trade outcomes, which are not yet collected "
                "because no token has passed the gate.",
                "The policy is NOT auto-tuned by this report. Any threshold "
                "change is a human decision that must be made explicitly.",
                "Concentration is measured against circulating supply (bonding-"
                "curve inventory excluded); unresolved rows lack a supply "
                "denominator and are counted separately, not imputed.",
            ],
        }


def _dashboard_report_text(value) -> str:
    if isinstance(value, (list, tuple)):
        return "; ".join(str(item) for item in value)
    return str(value or "")


def _dashboard_public_endpoint(value: str | None) -> str:
    """Return a credential-safe endpoint origin for observability."""
    return _public_rpc_endpoint(value)


def _dashboard_redact_text(value) -> str:
    """Remove URL paths and query credentials from dashboard-facing errors."""
    return _redact_sensitive_text(value)


def _solana_dashboard_cohort(trader: SolanaShadowTrader) -> dict:
    now = time.time()
    positions = []
    priced_cost = 0.0
    modeled_value = 0.0
    total_cost = 0.0
    realized_pnl = 0.0
    open_mark_ages = []
    for value in trader.state.get("positions", {}).values():
        status = value.get("status") or "unknown"
        cost = _safe_float(value.get("entry_cost_lamports"))
        total_cost += cost
        marks = value.get("marks") or []
        latest_mark = marks[-1] if marks else {}
        if status == "closed":
            proceeds = _safe_float(value.get("exit_proceeds_lamports"), None)
            multiple = _safe_float(value.get("net_multiple"), None)
            realized_pnl += _safe_float(value.get("net_pnl_lamports"))
            marked_at = value.get("closed_at")
        else:
            proceeds = _safe_float(latest_mark.get("proceeds_lamports"), None)
            multiple = _safe_float(latest_mark.get("multiple"), None)
            marked_at = latest_mark.get("timestamp")
        mark_timestamp = _timestamp(marked_at)
        mark_age = (
            max(0.0, now - mark_timestamp)
            if mark_timestamp is not None and status == "open"
            else None
        )
        if mark_age is not None:
            open_mark_ages.append(mark_age)
        if proceeds is not None:
            priced_cost += cost
            modeled_value += proceeds
        positions.append(
            {
                "mint": value.get("mint"),
                "symbol": value.get("symbol") or "???",
                "status": status,
                "entry_score": value.get("entry_score"),
                "entry_evidence_state": value.get("entry_evidence_state"),
                "opened_at": value.get("opened_at"),
                "marked_at": marked_at,
                "mark_age_seconds": mark_age,
                "entry_cost_sol": cost / 1_000_000_000,
                "modeled_value_sol": (
                    proceeds / 1_000_000_000 if proceeds is not None else None
                ),
                "multiple": multiple,
                "exit_reason": value.get("exit_reason"),
            }
        )
    positions.sort(
        key=lambda item: (
            item["status"] != "open",
            -(float(_timestamp(item["marked_at"]) or 0)),
        )
    )
    priced = [item for item in positions if item["multiple"] is not None]
    profits = [
        (
            (item["modeled_value_sol"] or 0.0)
            - item["entry_cost_sol"]
        )
        for item in priced
    ]
    modeled_return = (
        100.0 * (modeled_value - priced_cost) / priced_cost
        if priced_cost
        else None
    )
    return_without_best = None
    if len(priced) > 1:
        best = max(range(len(profits)), key=profits.__getitem__)
        reduced_cost = priced_cost - (
            priced[best]["entry_cost_sol"] * 1_000_000_000
        )
        if reduced_cost:
            return_without_best = (
                100.0
                * (sum(profits) * 1_000_000_000 - profits[best] * 1_000_000_000)
                / reduced_cost
            )
    return {
        "opened": len(positions),
        "open": sum(item["status"] == "open" for item in positions),
        "closed": sum(item["status"] == "closed" for item in positions),
        "priced": len(priced),
        "unpriced": len(positions) - len(priced),
        "total_cost_sol": total_cost / 1_000_000_000,
        "priced_cost_sol": priced_cost / 1_000_000_000,
        "modeled_value_sol": modeled_value / 1_000_000_000,
        "modeled_return_pct": modeled_return,
        "return_without_best_pct": return_without_best,
        "winner_rate_pct": (
            100.0 * sum(value > 0 for value in profits) / len(profits)
            if profits
            else None
        ),
        "realized_pnl_sol": realized_pnl / 1_000_000_000,
        "stale_open_marks": sum(age > 15 * 60 for age in open_mark_ages),
        "oldest_mark_age_seconds": max(open_mark_ages) if open_mark_ages else None,
        "positions": positions,
    }


def _solana_dashboard_snapshot(
    engine: SolanaPrototypeEngine,
    learning_loop: LearningLoopController | None = None,
) -> dict:
    """Build a read-only snapshot from atomically persisted learner state."""
    engine.trader.state = engine.trader._load()
    verification = engine.verify()
    observation_events = engine.observation_ledger.load()
    shadow_events = engine.shadow_ledger.load()
    catalog = _read_json(engine.root / "catalog.json", {})
    analysis_index = _read_json(engine.analysis_index_path, {})
    analyses = analysis_index.get("tokens", {})
    learning = _read_json(engine.root / "learning_summary.json", {})
    promotion = _read_json(
        engine.root / "promotion_status.json",
        engine.promotion.evaluate(),
    )
    calibration = _read_json(
        engine.root / "concentration_calibration.json",
        engine.concentration_calibration(),
    )
    schedule = _read_json(engine.root / "schedule.json", {})
    scheduler = _read_json(engine.root / "scheduler_status.json", {})
    controller = _read_json(engine.root / "controller_status.json", {})
    reflection = engine.reflection_status()
    if schedule.get("installed") and schedule.get("enabled") is False:
        scheduler = {
            **scheduler,
            "stale_status": scheduler.get("status"),
            "status": "disabled",
        }
    rpc_health = _read_json(engine.root / "rpc_health.json", {})
    recovery = engine._recovery_summary()
    jupiter_health = _read_json(
        engine.root / "jupiter_health.json",
        engine._jupiter_health(),
    )
    dexscreener_health = _read_json(
        engine.root / "dexscreener_health.json",
        engine._dexscreener_health(),
    )
    current_segment = rpc_health.get("current_segment") or rpc_health
    rpc_attempts = _safe_int(current_segment.get("attempts"))
    rpc_successes = _safe_int(current_segment.get("successes"))
    rpc_failures = _safe_int(current_segment.get("failures"))
    configured_rpc_url = _dashboard_public_endpoint(
        getattr(engine.rpc, "url", None)
    )
    observed_rpc_url = _dashboard_public_endpoint(
        rpc_health.get("rpc_url")
    )
    rpc_health = {
        **rpc_health,
        # The headline counters are the currently configured endpoint segment.
        # Full cross-provider history remains available in `endpoints`.
        "rpc_url": _dashboard_public_endpoint(
            current_segment.get("rpc_url") or observed_rpc_url
        ),
        "configured_rpc_url": configured_rpc_url,
        "configured_provider": _rpc_provider(
            getattr(engine.rpc, "url", None)
        ),
        "configured_rpc_required": bool(
            _read_json(engine.rpc_policy_path, {}).get(
                "require_configured_rpc"
            )
        ),
        "success_rate_pct": (
            100.0 * rpc_successes / rpc_attempts if rpc_attempts else None
        ),
        "attempts": rpc_attempts,
        "successes": rpc_successes,
        "failures": rpc_failures,
    }

    evidence_states = {
        name: sum(
            (row.get("decision") or {}).get("evidence_state") == name
            for row in analyses.values()
        )
        for name in (
            "complete_safe",
            "complete_unsafe",
            "distribution_pending",
            "infrastructure_indeterminate",
        )
    }
    admission_states: dict[str, int] = {
        name: 0
        for name in (
            "graduation_pending",
            "canonical_migration_pending",
            "market_indexing_pending",
            "execution_evidence_pending",
            "distribution_pending",
            "market_age_pending",
            "graduated_market_ready",
            "graduated_market_unsafe",
            "launch_observation_unsafe",
            "infrastructure_indeterminate",
        )
    }
    for row in analyses.values():
        state = (row.get("decision") or {}).get(
            "admission_state", "graduation_pending"
        )
        admission_states[state] = _safe_int(admission_states.get(state)) + 1
    graduated_market_count = sum(
        (row.get("decision") or {}).get("cohort") == "graduated_market"
        for row in analyses.values()
    )
    quote_coverage = sum(
        bool(
            (row.get("decision") or {})
            .get("coverage", {})
            .get("jupiter_two_way_quote")
        )
        for row in analyses.values()
    )
    assembly_coverage = sum(
        bool(
            (row.get("decision") or {})
            .get("coverage", {})
            .get("unsigned_buy_assembly")
        )
        for row in analyses.values()
    )
    decisions = []
    for mint, row in analyses.items():
        candidate = row.get("candidate") or {}
        decision = row.get("decision") or {}
        coverage = decision.get("coverage") or {}
        execution = decision.get("execution_evidence") or {}
        decisions.append(
            {
                "mint": mint,
                "symbol": candidate.get("symbol") or "???",
                "name": candidate.get("name"),
                "updated_at": row.get("updated_at"),
                "score": decision.get("score"),
                "risk_level": decision.get("risk_level"),
                "evidence_state": decision.get("evidence_state"),
                "cohort": decision.get("cohort") or "launch_observation",
                "admission_state": decision.get("admission_state")
                or "graduation_pending",
                "shadow_entry_allowed": bool(
                    decision.get("shadow_entry_allowed")
                ),
                "hard_stops": decision.get("hard_stops") or [],
                "warnings": decision.get("warnings") or [],
                "infrastructure_errors": [
                    _dashboard_redact_text(value)
                    for value in (
                        decision.get("infrastructure_errors") or []
                    )
                ],
                "coverage_complete": sum(bool(value) for value in coverage.values()),
                "coverage_total": len(coverage),
                "roundtrip_retention_pct": execution.get(
                    "roundtrip_retention_pct"
                ),
                "distribution_stage": (decision.get("concentration") or {}).get(
                    "distribution_stage"
                ),
                "circulating_pct_of_supply": (decision.get("concentration") or {}).get(
                    "circulating_pct_of_supply"
                ),
                "convergent_wallets": (decision.get("convergence_evidence") or {}).get(
                    "accurate_wallets_holding"
                ),
                "timechain_ring": decision.get("timechain_ring"),
                "graduation": decision.get("graduation") or {},
            }
        )
    decisions.sort(key=lambda item: item.get("updated_at") or "", reverse=True)

    symbol_by_mint = {
        mint: (row.get("candidate") or {}).get("symbol") or "???"
        for mint, row in analyses.items()
    }
    recent_events = []
    for event in sorted(
        observation_events + shadow_events,
        key=lambda item: item.get("timestamp") or "",
        reverse=True,
    )[:24]:
        payload = event.get("payload") or {}
        candidate = payload.get("candidate") or {}
        position = payload.get("position") or {}
        mint = (
            payload.get("mint")
            or candidate.get("mint")
            or position.get("mint")
        )
        recent_events.append(
            {
                "index": event.get("index"),
                "event_type": event.get("event_type"),
                "timestamp": event.get("timestamp"),
                "event_hash": event.get("event_hash"),
                "mint": mint,
                "symbol": (
                    candidate.get("symbol")
                    or position.get("symbol")
                    or symbol_by_mint.get(mint)
                ),
                "reason": payload.get("reason"),
                "source": (
                    "shadow"
                    if str(event.get("event_type") or "").startswith(
                        "solana_shadow"
                    )
                    else "observation"
                ),
            }
        )

    timechain = {
        "enabled": engine.timechain is not None,
        "ok": False,
        "report": _dashboard_report_text(verification.get("timechain")),
        "height": 0,
        "head": None,
    }
    if engine.timechain:
        chain_ok, chain_report = engine.timechain.tc.verify()
        rings = list(engine.timechain.tc.iter_rings())
        head = rings[-1] if rings else None
        timechain.update(
            {
                "ok": chain_ok,
                "report": _dashboard_report_text(chain_report),
                "height": len(rings),
                "head": (
                    {
                        "index": head.get("index"),
                        "ring_type": head.get("ring_type"),
                        "timestamp": head.get("timestamp"),
                        "ring_hash": head.get("ring_hash"),
                        "summary": str(
                            (head.get("payload") or {}).get("summary") or ""
                        ),
                    }
                    if head
                    else None
                ),
            }
        )

    cycle_timestamp = learning.get("timestamp")
    cycle_epoch = _timestamp(cycle_timestamp)
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": _utc_now(),
        "ecosystem": "pump_fun",
        "network": "solana_mainnet",
        "paper_only": True,
        "live_execution_enabled": False,
        "learning_loop": (
            learning_loop.status() if learning_loop is not None else None
        ),
        "catalog_size": len(catalog.get("tokens", {})),
        "analysis_count": len(analyses),
        "evidence_states": evidence_states,
        "admission_states": admission_states,
        "graduated_market_count": graduated_market_count,
        "evidence_coverage": {
            "two_way_quote_count": quote_coverage,
            "two_way_quote_pct": (
                100.0 * quote_coverage / graduated_market_count
                if graduated_market_count
                else 0.0
            ),
            "unsigned_assembly_count": assembly_coverage,
            "unsigned_assembly_pct": (
                100.0 * assembly_coverage / graduated_market_count
                if graduated_market_count
                else 0.0
            ),
        },
        "cycle": {
            **(learning.get("cycle") or {}),
            "timestamp": cycle_timestamp,
            "age_seconds": (
                max(0.0, time.time() - cycle_epoch)
                if cycle_epoch is not None
                else None
            ),
        },
        "configuration": {
            "jupiter_api_key_configured": bool(
                getattr(engine.jupiter, "api_key", None)
            ) or jupiter_health.get("access_mode") == "api_key",
            "paper_taker_configured": bool(
                getattr(engine.jupiter, "paper_taker", None)
            ),
            "rpc_endpoint": configured_rpc_url,
            "rpc_provider": rpc_health.get("configured_provider"),
            "rpc_last_observed_endpoint": observed_rpc_url,
            "configured_rpc_required": rpc_health.get(
                "configured_rpc_required"
            ),
            "credentials_exposed": False,
        },
        "rpc_health": rpc_health,
        "recovery": recovery,
        "jupiter_health": jupiter_health,
        "dexscreener_health": dexscreener_health,
        "schedule": schedule,
        "scheduler": scheduler,
        "controller": controller,
        "reflection": reflection,
        "shadow": _solana_dashboard_cohort(engine.trader),
        "promotion": promotion,
        "concentration_calibration": calibration,
        "learning": learning,
        "decisions": decisions[:50],
        "events": {
            "observation": len(observation_events),
            "shadow": len(shadow_events),
            "recent": recent_events,
        },
        "integrity": {
            "ok": verification["ok"],
            "observation_ledger": verification["observation_ledger"],
            "shadow_ledger": verification["shadow_ledger"],
            "reflection_ledger": verification["reflection_ledger"],
            "state_policy_bound": verification["state_policy_bound"],
            "timechain": timechain,
        },
    }


def _read_dashboard_asset(path: Path) -> bytes:
    """Read the current local dashboard asset without process-lifetime caching."""
    return path.read_bytes()


class LearningLoopController:
    """Thread-safe manager for a background learn_once loop.

    Driven from the dashboard's Start/Stop controls. The loop runs learn_once
    cycles with the proven-safe defaults on a daemon thread, so it dies with
    the dashboard process (no orphaned learning). Stop is responsive: the
    cooldown between cycles uses an Event wait, so a stop request takes effect
    at the next cycle boundary (worst case ~one cycle duration). On a cycle
    error the loop logs and continues (matching the recovery-queue resilience);
    only an explicit stop or duration expiry ends it. The loop only calls
    learn_once, which never signs -- the paper-only / PoQ-sealed boundary is
    preserved.
    """

    _COOLDOWN_SECONDS = 60.0

    def __init__(self, engine: "SolanaPrototypeEngine"):
        self._engine = engine
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._status = {
            "running": False,
            "started_at": None,
            "duration_seconds": None,
            "cycles": 0,
            "last_cycle_at": None,
            "last_error": None,
            "paused_reason": None,
        }

    def start(self, duration_seconds: float | None) -> dict:
        with self._lock:
            if self._status["running"]:
                # Idempotent: a restart while running is a no-op so we never
                # spawn a second loop on top of the first. Build the snapshot
                # under the lock we hold; do NOT call self.status() (it
                # re-acquires this non-reentrant lock and self-deadlocks).
                return self._snapshot_from(dict(self._status))
            self._stop_event.clear()
            now = _utc_now()
            self._status = {
                "running": True,
                "started_at": now,
                "duration_seconds": duration_seconds,
                "cycles": 0,
                "last_cycle_at": None,
                "last_error": None,
                "paused_reason": None,
            }
            self._thread = threading.Thread(
                target=self._run,
                name="chainseer-solana-learn-loop",
                daemon=True,
            )
            self._thread.start()
            # Build the snapshot under the lock we already hold -- do NOT call
            # self.status() here, it re-acquires this (non-reentrant) lock and
            # would self-deadlock.
            return self._snapshot_from(dict(self._status))

    @staticmethod
    def _snapshot_from(base: dict) -> dict:
        started_epoch = _timestamp(base.get("started_at"))
        duration = base.get("duration_seconds")
        now = time.time()
        elapsed = (now - started_epoch) if started_epoch else None
        remaining = None
        if elapsed is not None and duration is not None:
            remaining = max(0.0, duration - elapsed)
        return {
            **base,
            "elapsed_seconds": (
                round(elapsed, 1) if elapsed is not None else None
            ),
            "remaining_seconds": (
                round(remaining, 1) if remaining is not None else None
            ),
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def stop(self) -> dict:
        with self._lock:
            was_running = self._status["running"]
        if was_running:
            self._stop_event.set()
            thread = self._thread
            if thread is not None and thread.is_alive():
                # Don't block indefinitely; a cycle in flight resolves at its
                # own boundary. The thread is a daemon, so it is safe to leave.
                thread.join(timeout=5.0)
        with self._lock:
            self._status["running"] = False
            self._thread = None
            return self._snapshot_from(dict(self._status))

    def _run(self) -> None:
        duration = self._status.get("duration_seconds")
        started_epoch = _timestamp(self._status.get("started_at")) or time.time()
        while not self._stop_event.is_set():
            if duration is not None:
                elapsed = time.time() - started_epoch
                if elapsed >= duration:
                    break
            try:
                self._engine.learn_once(
                    limit=4,
                    signature_limit=80,
                    recovery_limit=2,
                    slot_span=10,
                    max_pages=4,
                )
                with self._lock:
                    self._status["cycles"] = _safe_int(
                        self._status.get("cycles")
                    ) + 1
                    self._status["last_cycle_at"] = _utc_now()
                    self._status["last_error"] = None
            except (ReflectionCheckpointPending, LiveExecutionDisabledError) as exc:
                # Intentional control-flow signals, NOT transient errors. The
                # learner is paused for a sealed checkpoint review (or blocked
                # from live execution). Retrying every cooldown is futile -- the
                # pause persists until a human acknowledges the checkpoint.
                # Stop the loop cleanly and record an informative status so the
                # UI can explain WHY the loop stopped rather than showing a
                # scary error.
                with self._lock:
                    self._status["last_error"] = None
                    self._status["paused_reason"] = _redact_sensitive_text(
                        f"{type(exc).__name__}: {exc}"
                    )
                break
            except Exception as exc:
                # Continue on cycle error: log redacted message and proceed to
                # the next cycle after cooldown (resilient to transient outages).
                with self._lock:
                    self._status["last_error"] = _redact_sensitive_text(
                        f"{type(exc).__name__}: {exc}"
                    )
            # Interruptible cooldown: stop() wakes this immediately.
            self._stop_event.wait(self._COOLDOWN_SECONDS)
        with self._lock:
            self._status["running"] = False

    def status(self) -> dict:
        with self._lock:
            base = dict(self._status)
        return self._snapshot_from(base)


def serve_solana_dashboard(
    engine: SolanaPrototypeEngine,
    *,
    host: str = "127.0.0.1",
    port: int = 8767,
) -> None:
    if host not in {"127.0.0.1", "localhost", "::1"}:
        raise ValueError(
            "The Solana dashboard is local-only; bind to 127.0.0.1 or localhost"
        )
    dashboard_path = Path(__file__).with_name("solana_dashboard.html")
    if not dashboard_path.is_file():
        raise FileNotFoundError(
            f"Solana dashboard was not found: {dashboard_path}"
        )
    learning_loop = LearningLoopController(engine)

    class DashboardHandler(BaseHTTPRequestHandler):
        def _send(self, status: int, content_type: str, body: bytes) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("Referrer-Policy", "no-referrer")
            self.send_header(
                "Content-Security-Policy",
                "default-src 'self'; style-src 'self' 'unsafe-inline'; "
                "script-src 'self' 'unsafe-inline'; connect-src 'self'; "
                "img-src 'self' data:; frame-ancestors 'none'",
            )
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            route = urlparse(self.path).path
            if route == "/":
                try:
                    # Read for every request. Dashboard development is local
                    # and Cache-Control is no-store, so a browser refresh must
                    # reflect the current asset without restarting Python.
                    dashboard_html = _read_dashboard_asset(dashboard_path)
                    self._send(
                        200, "text/html; charset=utf-8", dashboard_html
                    )
                except OSError as exc:
                    self._send(
                        500,
                        "text/plain; charset=utf-8",
                        (
                            "Dashboard asset unavailable: "
                            f"{type(exc).__name__}"
                        ).encode("utf-8"),
                    )
                return
            if route == "/api/status":
                try:
                    payload = _canonical_json(
                        _solana_dashboard_snapshot(engine, learning_loop)
                    ).encode("utf-8")
                    self._send(
                        200, "application/json; charset=utf-8", payload
                    )
                except Exception as exc:
                    payload = _canonical_json(
                        {"error": str(exc), "generated_at": _utc_now()}
                    ).encode("utf-8")
                    self._send(
                        500, "application/json; charset=utf-8", payload
                    )
                return
            if route == "/health":
                self._send(
                    200,
                    "application/json; charset=utf-8",
                    b'{"status":"ok","read_only":true,"live_execution_enabled":false}',
                )
                return
            self._send(404, "text/plain; charset=utf-8", b"Not found")

        def _read_json_body(self) -> dict | None:
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                return None
            if length <= 0 or length > 4096:
                return {}
            raw = self.rfile.read(length)
            try:
                value = json.loads(raw.decode("utf-8"))
                return value if isinstance(value, dict) else {}
            except (ValueError, UnicodeDecodeError):
                return None

        def do_POST(self):
            route = urlparse(self.path).path
            if route == "/api/learn/start":
                body = self._read_json_body()
                if body is None:
                    self._send(
                        400, "application/json; charset=utf-8",
                        b'{"error":"malformed json body"}',
                    )
                    return
                minutes = body.get("duration_minutes")
                if minutes is None or minutes == "":
                    duration_seconds = None
                else:
                    try:
                        minutes = float(minutes)
                    except (TypeError, ValueError):
                        self._send(
                            400, "application/json; charset=utf-8",
                            b'{"error":"duration_minutes must be a number"}',
                        )
                        return
                    if minutes < 1:
                        self._send(
                            400, "application/json; charset=utf-8",
                            b'{"error":"duration_minutes must be >= 1"}',
                        )
                        return
                    duration_seconds = minutes * 60.0
                status = learning_loop.start(duration_seconds)
                payload = _canonical_json(status).encode("utf-8")
                self._send(200, "application/json; charset=utf-8", payload)
                return
            if route == "/api/learn/stop":
                status = learning_loop.stop()
                payload = _canonical_json(status).encode("utf-8")
                self._send(200, "application/json; charset=utf-8", payload)
                return
            self._send(404, "text/plain; charset=utf-8", b"Not found")

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer((host, int(port)), DashboardHandler)
    print(f"Solana learn-once dashboard: http://{host}:{port}")
    print("Read-only local view. Press Ctrl+C to stop.")
    try:
        server.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def _print_results(results: Iterable[dict]) -> None:
    for result in results:
        candidate = result["candidate"]
        decision = result["decision"]
        print(
            f"{candidate['symbol'][:12]:12} {decision['risk_level']:13} "
            f"score={decision['score']:5.1f} "
            f"entry={'YES' if decision['shadow_entry_allowed'] else 'NO ':3} "
            f"evidence={decision['evidence_state']} "
            f"admission={decision.get('admission_state', 'unknown')}"
        )
        print(f"  SHADOW ACTION: {result['shadow_action']}")
        if decision["hard_stops"]:
            print("  HARD STOPS: " + ", ".join(decision["hard_stops"]))
        if decision["infrastructure_errors"]:
            print(
                "  INFRASTRUCTURE: "
                + "; ".join(
                    _redact_sensitive_text(value)
                    for value in decision["infrastructure_errors"]
                )
            )


def main() -> None:
    ensure_utf8_runtime()
    parser = argparse.ArgumentParser(
        description="Chainseer Solana Pump.fun paper/shadow prototype"
    )
    parser.add_argument("--root", default="solana_learning")
    parser.add_argument("--rpc-url", default=SOLANA_RPC_URL)
    parser.add_argument("--chain-root", default="solana_chain")
    parser.add_argument("--jupiter-api-key", default=None)
    parser.add_argument("--paper-taker", default=None)
    parser.add_argument("--no-timechain", action="store_true")
    parser.add_argument(
        "--allow-public-rpc",
        action="store_true",
        help="Explicitly override a persisted configured-RPC requirement. "
        "Never implied or enabled by the scheduler.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("observe", "learn-once"):
        command = subparsers.add_parser(name)
        command.add_argument("--limit", type=int, default=10)
        command.add_argument("--signature-limit", type=int, default=100)
        command.add_argument(
            "--slot-span", type=int, default=None,
            help="Sweep backwards through this many slots of Pump program "
                 "activity to catch CreateEvents that a single signature batch "
                 "misses (Pump.fun emits many txs per slot). Bounds cost via "
                 "--max-pages.",
        )
        command.add_argument(
            "--max-pages", type=int, default=10,
            help="Maximum getSignatures pages the slot sweep will issue.",
        )
        if name == "learn-once":
            command.add_argument("--recovery-limit", type=int, default=3)
            command.add_argument(
                "--graduation-limit",
                type=int,
                default=3,
                help="Maximum confirmed Pump launches selected by the bounded "
                "curve-completion discovery lane for full analysis.",
            )
    dashboard = subparsers.add_parser("dashboard")
    dashboard.add_argument("--host", default="127.0.0.1")
    dashboard.add_argument("--port", type=int, default=8767)
    subparsers.add_parser("status")
    subparsers.add_parser("promotion")
    subparsers.add_parser("calibration")
    subparsers.add_parser("reflection-status")
    reflection_ack = subparsers.add_parser("reflection-ack")
    reflection_ack.add_argument(
        "--outcome", choices=("applied", "no_change"), required=True
    )
    reflection_ack.add_argument("--summary", required=True)
    subparsers.add_parser("verify")
    args = parser.parse_args()
    engine = SolanaPrototypeEngine(
        args.root,
        rpc_url=args.rpc_url,
        jupiter_api_key=args.jupiter_api_key,
        paper_taker=args.paper_taker,
        record_timechain=not args.no_timechain,
        chain_root=args.chain_root,
        allow_public_rpc=args.allow_public_rpc,
    )
    if args.command == "observe":
        _print_results(
            engine.observe(
                limit=args.limit, signature_limit=args.signature_limit,
                slot_span=args.slot_span, max_pages=args.max_pages,
            )
        )
    elif args.command == "learn-once":
        print(
            json.dumps(
                engine.learn_once(
                    limit=args.limit,
                    signature_limit=args.signature_limit,
                    recovery_limit=args.recovery_limit,
                    graduation_limit=args.graduation_limit,
                    slot_span=args.slot_span,
                    max_pages=args.max_pages,
                ),
                indent=2,
            )
        )
    elif args.command == "promotion":
        promotion = engine.promotion.evaluate()
        _atomic_json(engine.root / "promotion_status.json", promotion)
        print(json.dumps(promotion, indent=2))
    elif args.command == "calibration":
        calibration = engine.concentration_calibration()
        _atomic_json(engine.root / "concentration_calibration.json", calibration)
        print(json.dumps(calibration, indent=2))
    elif args.command == "status":
        print(
            json.dumps(
                _read_json(
                    engine.root / "learning_summary.json",
                    {
                        "status": "no learning cycle recorded",
                        "promotion": engine.promotion.evaluate(),
                    },
                ),
                indent=2,
            )
        )
    elif args.command == "reflection-status":
        print(json.dumps(engine.reflection_status(), indent=2))
    elif args.command == "reflection-ack":
        print(
            json.dumps(
                engine.acknowledge_reflection(
                    args.outcome, args.summary
                ),
                indent=2,
            )
        )
    elif args.command == "dashboard":
        serve_solana_dashboard(engine, host=args.host, port=args.port)
    elif args.command == "verify":
        report = engine.verify()
        print(json.dumps(report, indent=2))
        raise SystemExit(0 if report["ok"] else 1)


if __name__ == "__main__":
    main()
