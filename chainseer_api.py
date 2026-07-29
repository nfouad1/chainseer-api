"""Production HTTP boundary for the Robinhood Chain Chainseer engine.

The API deliberately runs one analysis worker. Chainseer owns mutable,
request-scoped scan state and appends to a single Timechain, so concurrent
analysis in one process would be unsafe. Web requests enqueue jobs and poll
for a structured public report.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
import queue
import re
import secrets
import socket
import threading
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, Field, field_validator, model_validator
from starlette.responses import JSONResponse

from chainseer import Chainseer
from chainseer_benchmark import (
    append_observation,
    build_observation_from_report,
    case_bank_status,
    load_jsonl,
)
from chainseer_controls import ChainseerWatcher, WatchConfig
from chainseer_solana_public import (
    SolanaMintError,
    SolanaPublicAnalyzer,
    validate_solana_mint,
)

LOGGER = logging.getLogger("chainseer.api")
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
SUPPORTED_NETWORKS = {"robinhood", "solana"}


def deterministic_benchmark_split(network: str, address: str) -> str:
    """Keep every observation for one token in the same leakage-safe split."""
    normalized = address.lower() if network == "robinhood" else address
    digest = hashlib.sha256(
        f"chainseer-benchmark-v1:{network}:{normalized}".encode("utf-8")
    ).digest()
    bucket = int.from_bytes(digest[:4], "big") % 100
    if bucket < 60:
        return "train"
    if bucket < 80:
        return "validation"
    return "test"


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}")
    return value


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{name} must be a boolean")


def _server_port() -> int:
    """Honor a platform-assigned port while preserving the local default."""
    if os.environ.get("CHAINSEER_API_PORT", "").strip():
        return _env_int("CHAINSEER_API_PORT", 8000, 1, 65535)
    return _env_int("PORT", 8000, 1, 65535)


@dataclass(frozen=True)
class Settings:
    environment: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_ENVIRONMENT", "development"
        ).lower()
    )
    api_token: str = field(
        default_factory=lambda: os.environ.get("CHAINSEER_API_TOKEN", "")
    )
    rpc_url: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_RPC_URL",
            "https://rpc.mainnet.chain.robinhood.com",
        )
    )
    chain_root: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_CHAIN_ROOT",
            str(Path(__file__).resolve().parent / "chainseer_chain"),
        )
    )
    allowed_origins: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            origin.strip()
            for origin in os.environ.get(
                "CHAINSEER_ALLOWED_ORIGINS",
                "http://localhost:3000,http://127.0.0.1:3000",
            ).split(",")
            if origin.strip()
        )
    )
    allowed_hosts: tuple[str, ...] = field(
        default_factory=lambda: tuple(
            host.strip()
            for host in os.environ.get(
                "CHAINSEER_ALLOWED_HOSTS",
                "localhost,127.0.0.1,testserver",
            ).split(",")
            if host.strip()
        )
    )
    max_request_bytes: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_MAX_REQUEST_BYTES", 2048, 256, 16384
        )
    )
    queue_size: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_QUEUE_SIZE", 20, 1, 500
        )
    )
    result_ttl_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_RESULT_TTL_SECONDS", 3600, 60, 86400
        )
    )
    cache_ttl_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_SCAN_CACHE_TTL_SECONDS", 300, 0, 3600
        )
    )
    rate_limit_per_minute: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_RATE_LIMIT_PER_MINUTE", 6, 1, 120
        )
    )
    shutdown_grace_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_SHUTDOWN_GRACE_SECONDS", 180, 10, 900
        )
    )
    solana_rpc_url: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_SOLANA_RPC_URL",
            "https://api.mainnet-beta.solana.com",
        )
    )
    jupiter_api_key: str = field(
        default_factory=lambda: os.environ.get("JUPITER_API_KEY", "")
    )
    watcher_enabled: bool = field(
        default_factory=lambda: _env_bool(
            "CHAINSEER_WATCHER_ENABLED", False
        )
    )
    watcher_interval_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_WATCHER_INTERVAL_SECONDS", 15, 3, 3600
        )
    )
    watcher_confirmations: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_WATCHER_CONFIRMATIONS", 2, 0, 100
        )
    )
    benchmark_capture_enabled: bool = field(
        default_factory=lambda: _env_bool(
            "CHAINSEER_BENCHMARK_CAPTURE_ENABLED", False
        )
    )
    benchmark_root: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_BENCHMARK_ROOT",
            str(Path(__file__).resolve().parent / "benchmark_data"),
        )
    )
    benchmark_analyzer_version: str = field(
        default_factory=lambda: (
            os.environ.get("CHAINSEER_BENCHMARK_ANALYZER_VERSION", "").strip()
            or os.environ.get("RENDER_GIT_COMMIT", "").strip()
            or "local-unversioned"
        )
    )
    benchmark_robinhood_cohort: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_BENCHMARK_ROBINHOOD_COHORT",
            "robinhood_public_analysis",
        ).strip()
    )
    benchmark_solana_cohort: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_BENCHMARK_SOLANA_COHORT",
            "solana_public_analysis",
        ).strip()
    )

    def validate(self) -> None:
        if self.environment not in {"development", "test", "production"}:
            raise RuntimeError(
                "CHAINSEER_ENVIRONMENT must be development, test, or production"
            )
        rpc = urlparse(self.rpc_url)
        if rpc.scheme not in {"http", "https"} or not rpc.hostname:
            raise RuntimeError("CHAINSEER_RPC_URL must be an HTTP(S) URL")
        solana_rpc = urlparse(self.solana_rpc_url)
        if (
            solana_rpc.scheme not in {"http", "https"}
            or not solana_rpc.hostname
        ):
            raise RuntimeError(
                "CHAINSEER_SOLANA_RPC_URL must be an HTTP(S) URL"
            )
        for origin in self.allowed_origins:
            parsed = urlparse(origin)
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.path not in {"", "/"}
                or parsed.params
                or parsed.query
                or parsed.fragment
            ):
                raise RuntimeError(
                    "CHAINSEER_ALLOWED_ORIGINS must contain origins only"
                )
        if self.environment == "production":
            if len(self.api_token) < 32 or "replace" in self.api_token.lower():
                raise RuntimeError(
                    "CHAINSEER_API_TOKEN must contain at least 32 characters "
                    "in production"
                )
            if rpc.scheme != "https":
                raise RuntimeError(
                    "CHAINSEER_RPC_URL must use HTTPS in production"
                )
            if solana_rpc.scheme != "https":
                raise RuntimeError(
                    "CHAINSEER_SOLANA_RPC_URL must use HTTPS in production"
                )
            if not Path(self.chain_root).is_absolute():
                raise RuntimeError(
                    "CHAINSEER_CHAIN_ROOT must be absolute in production"
                )
            if not self.allowed_origins:
                raise RuntimeError(
                    "CHAINSEER_ALLOWED_ORIGINS is required in production"
                )
            if any(origin == "*" for origin in self.allowed_origins):
                raise RuntimeError(
                    "Wildcard CORS origins are forbidden in production"
                )
            if not os.environ.get("CHAINSEER_ALLOWED_HOSTS", "").strip():
                raise RuntimeError(
                    "CHAINSEER_ALLOWED_HOSTS is required in production"
                )
            if not self.allowed_hosts or "*" in self.allowed_hosts:
                raise RuntimeError(
                    "Wildcard or empty trusted hosts are forbidden in production"
                )
            if self.benchmark_capture_enabled:
                if not Path(self.benchmark_root).is_absolute():
                    raise RuntimeError(
                        "CHAINSEER_BENCHMARK_ROOT must be absolute in production"
                    )
                if self.benchmark_analyzer_version == "local-unversioned":
                    raise RuntimeError(
                        "Benchmark capture requires RENDER_GIT_COMMIT or "
                        "CHAINSEER_BENCHMARK_ANALYZER_VERSION in production"
                    )
        if self.benchmark_capture_enabled:
            if not self.benchmark_robinhood_cohort:
                raise RuntimeError(
                    "CHAINSEER_BENCHMARK_ROBINHOOD_COHORT is required"
                )
            if not self.benchmark_solana_cohort:
                raise RuntimeError(
                    "CHAINSEER_BENCHMARK_SOLANA_COHORT is required"
                )
            benchmark_path = Path(self.benchmark_root).resolve()
            chain_path = Path(self.chain_root).resolve()
            try:
                benchmark_path.relative_to(chain_path)
            except ValueError:
                pass
            else:
                raise RuntimeError(
                    "CHAINSEER_BENCHMARK_ROOT must not be inside "
                    "CHAINSEER_CHAIN_ROOT"
                )


class AnalyzeRequest(BaseModel):
    network: str = Field(default="robinhood", min_length=6, max_length=16)
    address: str = Field(min_length=32, max_length=44)

    @field_validator("network")
    @classmethod
    def validate_network(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in SUPPORTED_NETWORKS:
            raise ValueError("unsupported analysis network")
        return normalized

    @model_validator(mode="after")
    def validate_address_for_network(self):
        self.address = self.address.strip()
        if self.network == "robinhood":
            if not ADDRESS_RE.fullmatch(self.address):
                raise ValueError("invalid EVM contract address")
        else:
            validate_solana_mint(self.address)
        return self


class WatchRequest(BaseModel):
    address: str = Field(min_length=42, max_length=42)

    @field_validator("address")
    @classmethod
    def validate_address(cls, value: str) -> str:
        value = value.strip()
        if not ADDRESS_RE.fullmatch(value):
            raise ValueError("invalid EVM contract address")
        return value


class JobAccepted(BaseModel):
    job_id: str
    status: str
    cached: bool = False


@dataclass
class Job:
    id: str
    address: str
    network: str = "robinhood"
    status: str = "queued"
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    result: dict[str, Any] | None = None
    benchmark_capture: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None

    def public(self) -> dict[str, Any]:
        return {
            "job_id": self.id,
            "address": self.address,
            "network": self.network,
            "status": self.status,
            "created_at": _iso(self.created_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "result": self.result,
            "benchmark_capture": self.benchmark_capture,
            "error": (
                {
                    "code": self.error_code,
                    "message": self.error_message,
                }
                if self.error_code
                else None
            ),
        }


def _iso(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


class BenchmarkCaptureRecorder:
    """Append fresh analysis predictions to the durable benchmark ledger."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.enabled = settings.benchmark_capture_enabled
        self.root = Path(settings.benchmark_root)
        self.observations_path = self.root / "observations-v1.jsonl"
        self.outcomes_path = self.root / "outcomes-v1.jsonl"
        self._lock = threading.Lock()
        self._summary: dict[str, Any] = {
            "enabled": self.enabled,
            "state": "disabled" if not self.enabled else "initializing",
            "last_capture_at": None,
            "last_error": None,
        }
        if self.enabled:
            self._initialize()

    def _initialize(self) -> None:
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            self._refresh_summary()
        except Exception as exc:
            LOGGER.exception("Benchmark capture storage initialization failed")
            self._summary = {
                "enabled": True,
                "state": "degraded",
                "last_capture_at": None,
                "last_error": {
                    "at": datetime.now(timezone.utc).isoformat(),
                    "code": "benchmark_storage_unavailable",
                    "error_type": type(exc).__name__,
                },
            }

    def _refresh_summary(self) -> None:
        ledger = case_bank_status(
            load_jsonl(self.observations_path),
            load_jsonl(self.outcomes_path),
        )
        self._summary = {
            "enabled": True,
            "state": "ready",
            "last_capture_at": self._summary.get("last_capture_at"),
            "last_error": None,
            **ledger,
        }

    def capture(
        self,
        job: Job,
        public_report: dict[str, Any],
        *,
        captured_at: str | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"status": "disabled"}
        cohort = (
            self.settings.benchmark_solana_cohort
            if job.network == "solana"
            else self.settings.benchmark_robinhood_cohort
        )
        split = deterministic_benchmark_split(job.network, job.address)
        latency_ms = max(
            0.0,
            (time.time() - (job.started_at or time.time())) * 1000,
        )
        try:
            with self._lock:
                observation = build_observation_from_report(
                    public_report,
                    cohort=cohort,
                    split=split,
                    analyzer="chainseer",
                    analyzer_version=(
                        self.settings.benchmark_analyzer_version
                    ),
                    latency_ms=latency_ms,
                    captured_at=captured_at,
                )
                append_observation(
                    self.observations_path,
                    observation,
                )
                captured_timestamp = datetime.now(timezone.utc).isoformat()
                self._summary["last_capture_at"] = captured_timestamp
                self._refresh_summary()
            return {
                "status": "captured",
                "case_id": observation["case_id"],
                "observation_hash": observation["observation_hash"],
                "split": observation["split"],
                "cohort": observation["cohort"],
                "analyzer_version": observation["analyzer_version"],
            }
        except Exception as exc:
            # Benchmark telemetry is intentionally non-critical: an unforeseen
            # recorder defect must never turn a valid analysis into a failed job.
            LOGGER.exception(
                "Benchmark observation capture failed",
                extra={"job_id": job.id, "network": job.network},
            )
            with self._lock:
                self._summary = {
                    **self._summary,
                    "enabled": True,
                    "state": "degraded",
                    "last_error": {
                        "at": datetime.now(timezone.utc).isoformat(),
                        "code": "benchmark_capture_failed",
                        "error_type": type(exc).__name__,
                    },
                }
            return {
                "status": "failed",
                "error": "benchmark_capture_failed",
            }

    def status(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._summary))


class SlidingWindowRateLimiter:
    def __init__(self, limit: int, window_seconds: int = 60):
        self.limit = limit
        self.window_seconds = window_seconds
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, identity: str, now: float | None = None) -> bool:
        now = now if now is not None else time.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            events = self._events[identity]
            while events and events[0] <= cutoff:
                events.popleft()
            if len(events) >= self.limit:
                return False
            events.append(now)
            return True


class AnalysisService:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.jobs: dict[str, Job] = {}
        self.active_by_address: dict[str, str] = {}
        self.cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self.work: queue.Queue[str | None] = queue.Queue(
            maxsize=settings.queue_size
        )
        self._lock = threading.RLock()
        self._worker: threading.Thread | None = None
        self._agent: Chainseer | None = None
        self._solana_agent: SolanaPublicAnalyzer | None = None
        self._stopping = threading.Event()
        self._ready = threading.Event()
        self._watch_lock = threading.Lock()
        self._watcher: ChainseerWatcher | None = None
        self._watcher_status: dict[str, Any] = {
            "enabled": settings.watcher_enabled,
            "last_cycle": None,
            "last_error": None,
        }
        self._benchmark = BenchmarkCaptureRecorder(settings)

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stopping.clear()
        if self._agent is None:
            self._agent = Chainseer(
                rpc_url=self.settings.rpc_url,
                chain_root=self.settings.chain_root,
            )
        if self._solana_agent is None:
            self._solana_agent = SolanaPublicAnalyzer(
                self.settings.solana_rpc_url,
                timechain_agent=self._agent,
                jupiter_api_key=self.settings.jupiter_api_key or None,
            )
        if self._watcher is None:
            self._watcher = ChainseerWatcher(
                self._agent,
                control_root=self.settings.chain_root,
                config=WatchConfig(
                    poll_seconds=self.settings.watcher_interval_seconds,
                    confirmations=self.settings.watcher_confirmations,
                ),
            )
        self._worker = threading.Thread(
            target=self._run,
            name="chainseer-analysis-worker",
            daemon=True,
        )
        self._worker.start()
        self._ready.set()

    def stop(self) -> bool:
        self._ready.clear()
        self._stopping.set()
        try:
            self.work.put_nowait(None)
        except queue.Full:
            pass
        if self._worker:
            self._worker.join(
                timeout=self.settings.shutdown_grace_seconds
            )
        return not bool(self._worker and self._worker.is_alive())

    @property
    def ready(self) -> bool:
        return bool(
            self._ready.is_set()
            and self._worker
            and self._worker.is_alive()
            and not self._stopping.is_set()
        )

    def submit(
        self, address: str, network: str = "robinhood"
    ) -> JobAccepted:
        normalized_address = (
            address.lower() if network == "robinhood" else address
        )
        normalized = f"{network}:{normalized_address}"
        now = time.time()
        with self._lock:
            self._prune(now)
            cached = self.cache.get(normalized)
            if cached and cached[0] > now:
                job = Job(
                    id=uuid.uuid4().hex,
                    address=address,
                    network=network,
                    status="succeeded",
                    started_at=now,
                    finished_at=now,
                    result=cached[1],
                    benchmark_capture={
                        "status": "cache_hit_not_recaptured"
                    },
                )
                self.jobs[job.id] = job
                return JobAccepted(
                    job_id=job.id, status=job.status, cached=True
                )

            active_id = self.active_by_address.get(normalized)
            if active_id:
                active = self.jobs.get(active_id)
                if active and active.status in {"queued", "running"}:
                    return JobAccepted(
                        job_id=active.id,
                        status=active.status,
                        cached=False,
                    )

            if self.work.full():
                raise QueueFullError

            job = Job(
                id=uuid.uuid4().hex,
                address=address,
                network=network,
            )
            self.jobs[job.id] = job
            self.active_by_address[normalized] = job.id
            self.work.put_nowait(job.id)
            return JobAccepted(job_id=job.id, status=job.status)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            self._prune(time.time())
            return self.jobs.get(job_id)

    def watch_status(self) -> dict[str, Any]:
        with self._watch_lock:
            state = (
                self._watcher.store.load()
                if self._watcher is not None
                else {"subscriptions": {}}
            )
            return {
                **self._watcher_status,
                "subscriptions": list(state.get("subscriptions", {}).values()),
            }

    def benchmark_status(self) -> dict[str, Any]:
        return self._benchmark.status()

    def watch_subscribe(self, address: str) -> dict[str, Any]:
        if self._watcher is None:
            raise RuntimeError("watcher is not initialized")
        with self._watch_lock:
            return self._watcher.store.subscribe(address)

    def watch_unsubscribe(self, address: str) -> bool:
        if self._watcher is None:
            raise RuntimeError("watcher is not initialized")
        with self._watch_lock:
            return self._watcher.store.unsubscribe(address)

    def _run_watcher_cycle(self) -> None:
        if not self.settings.watcher_enabled or self._watcher is None:
            return
        try:
            with self._watch_lock:
                summary = self._watcher.run_once()
            self._watcher_status = {
                "enabled": True,
                "last_cycle": summary,
                "last_error": None,
            }
        except Exception as exc:
            LOGGER.exception("Watcher cycle failed")
            self._watcher_status = {
                "enabled": True,
                "last_cycle": self._watcher_status.get("last_cycle"),
                "last_error": {
                    "at": datetime.now(timezone.utc).isoformat(),
                    "message": str(exc),
                },
            }

    def _prune(self, now: float) -> None:
        cutoff = now - self.settings.result_ttl_seconds
        expired_jobs = [
            job_id
            for job_id, job in self.jobs.items()
            if job.finished_at and job.finished_at < cutoff
        ]
        for job_id in expired_jobs:
            self.jobs.pop(job_id, None)
        expired_cache = [
            address
            for address, (expires_at, _) in self.cache.items()
            if expires_at <= now
        ]
        for address in expired_cache:
            self.cache.pop(address, None)

    def _run(self) -> None:
        next_watch = (
            time.monotonic() + self.settings.watcher_interval_seconds
        )
        while not self._stopping.is_set():
            timeout = None
            if self.settings.watcher_enabled:
                timeout = max(0.1, next_watch - time.monotonic())
            try:
                job_id = self.work.get(timeout=timeout)
            except queue.Empty:
                self._run_watcher_cycle()
                next_watch = (
                    time.monotonic()
                    + self.settings.watcher_interval_seconds
                )
                continue
            if job_id is None:
                return
            with self._lock:
                job = self.jobs.get(job_id)
                if not job:
                    continue
                job.status = "running"
                job.started_at = time.time()

            try:
                if self._agent is None:
                    raise RuntimeError("analysis worker has no Chainseer agent")
                if job.network == "solana":
                    if self._solana_agent is None:
                        raise RuntimeError(
                            "analysis worker has no Solana analyzer"
                        )
                    report = self._solana_agent.analyze_token(job.address)
                else:
                    report = self._agent.analyze_token(
                        job.address, full_report=False
                    )
                if report.get("error"):
                    raise PublicAnalysisError(
                        "analysis_rejected", str(report["error"])
                    )
                public_report = build_public_report(report)
                benchmark_capture = self._benchmark.capture(
                    job,
                    public_report,
                )
                with self._lock:
                    job.result = public_report
                    job.benchmark_capture = benchmark_capture
                    job.status = "succeeded"
                    if self.settings.cache_ttl_seconds:
                        cache_address = (
                            job.address.lower()
                            if job.network == "robinhood"
                            else job.address
                        )
                        self.cache[f"{job.network}:{cache_address}"] = (
                            time.time()
                            + self.settings.cache_ttl_seconds,
                            public_report,
                        )
            except PublicAnalysisError as exc:
                with self._lock:
                    job.status = "failed"
                    job.error_code = exc.code
                    job.error_message = exc.message
            except SolanaMintError as exc:
                with self._lock:
                    job.status = "failed"
                    job.error_code = exc.code
                    job.error_message = exc.message
            except Exception:
                LOGGER.exception(
                    "Analysis job failed",
                    extra={"job_id": job.id, "address": job.address},
                )
                with self._lock:
                    job.status = "failed"
                    job.error_code = "analysis_failed"
                    job.error_message = (
                        "The analysis could not be completed. "
                        "No result was published."
                    )
            finally:
                with self._lock:
                    job.finished_at = time.time()
                    self.active_by_address.pop(
                        (
                            f"{job.network}:"
                            + (
                                job.address.lower()
                                if job.network == "robinhood"
                                else job.address
                            )
                        ),
                        None,
                    )
                self.work.task_done()
            if (
                self.settings.watcher_enabled
                and time.monotonic() >= next_watch
            ):
                self._run_watcher_cycle()
                next_watch = (
                    time.monotonic()
                    + self.settings.watcher_interval_seconds
                )


class QueueFullError(Exception):
    pass


class PublicAnalysisError(Exception):
    def __init__(self, code: str, message: str):
        self.code = code
        self.message = message
        super().__init__(message)


class SingleProcessLease:
    """OS-backed exclusive lease preventing two Timechain writers."""

    def __init__(self, chain_root: str):
        self.path = Path(chain_root) / ".chainseer-api.lock"
        self.handle = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        self.handle = self.path.open("r+b")
        try:
            self.handle.seek(0, os.SEEK_END)
            if self.handle.tell() == 0:
                self.handle.write(b" ")
                self.handle.flush()
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(
                    self.handle.fileno(), msvcrt.LK_NBLCK, 1
                )
            else:
                import fcntl

                fcntl.flock(
                    self.handle.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
        except (OSError, BlockingIOError) as exc:
            self.handle.close()
            self.handle = None
            raise RuntimeError(
                "Another Chainseer API process already owns this "
                "Timechain root"
            ) from exc

        # Preserve the locked sentinel byte. On Windows, truncating byte zero
        # invalidates the byte-range lock and makes a later unlock fail.
        self.handle.seek(1)
        self.handle.truncate()
        self.handle.write(
            (
                f"pid={os.getpid()} host={socket.gethostname()} "
                f"acquired={datetime.now(timezone.utc).isoformat()}\n"
            ).encode("utf-8")
        )
        self.handle.flush()

    def release(self) -> None:
        if not self.handle:
            return
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                msvcrt.locking(
                    self.handle.fileno(), msvcrt.LK_UNLCK, 1
                )
            else:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def build_public_report(report: dict[str, Any]) -> dict[str, Any]:
    """Reduce the internal report to a stable, explicitly public schema."""
    analysis = report.get("analysis") or {}
    data = report.get("data") or {}
    basic = data.get("basic_info") or {}
    dex = data.get("dex_pairs") or {}
    holder_assessment = analysis.get("holder_assessment") or {}
    holder_evidence = (
        data.get("holder_concentration")
        or data.get("blockscout_holders")
        or {}
    )
    liquidity_custody = data.get("lp_lock") or {}
    extended = analysis.get("extended_evidence") or {}
    social_attention = extended.get("social_attention") or {}
    cross_chain = extended.get("cross_chain") or {}
    mev_exposure = extended.get("mev_exposure") or {}
    provenance = report.get("provenance") or {}
    evidence_facts = provenance.get("facts") or []
    public_facts = [
        {
            "id": fact.get("fact_id"),
            "source": fact.get("source"),
            "query_hash": fact.get("query_hash"),
            "response_hash": fact.get("response_hash"),
            "block": fact.get("block"),
            "timestamp": fact.get("fetched_at"),
            "cache_hit": bool(fact.get("cache_hit")),
        }
        for fact in evidence_facts[:50]
        if isinstance(fact, dict)
    ]
    ledger_hash = hashlib.sha256(
        json.dumps(
            [
                {
                    "id": fact.get("id"),
                    "query_hash": fact.get("query_hash"),
                    "response_hash": fact.get("response_hash"),
                }
                for fact in public_facts
            ],
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()

    raw_holder_count = holder_assessment.get("holder_count")
    holder_count_source = holder_assessment.get("source")
    if raw_holder_count in (None, "", 0, "0"):
        raw_holder_count = basic.get("jupiter_holder_count")
        if raw_holder_count not in (None, "", 0, "0"):
            holder_count_source = "Jupiter"
    try:
        holder_count = int(raw_holder_count)
    except (TypeError, ValueError, OverflowError):
        holder_count = None
    if holder_count is not None and holder_count <= 0:
        holder_count = None

    largest_accounts = holder_evidence.get("largest_accounts") or []
    top_holders = holder_evidence.get("holders") or []
    holder_sample_size = len(largest_accounts or top_holders)
    largest_holder_pct = holder_assessment.get(
        "largest_non_amm_holder_pct"
    )
    if largest_holder_pct is None:
        largest_holder_pct = holder_evidence.get("adj_top_1_pct")
    if largest_holder_pct is None:
        largest_holder_pct = holder_evidence.get(
            "top1_total_supply_pct"
        )
    top10_holder_pct = holder_evidence.get("adj_top_10_pct")
    if top10_holder_pct is None:
        top10_holder_pct = holder_evidence.get(
            "top10_total_supply_pct"
        )
    is_solana = (
        str(report.get("chain_name") or "").strip().lower() == "solana"
    )
    if holder_count is not None:
        holder_caveat = (
            f"Holder count was reported by {holder_count_source or 'an upstream provider'} "
            "at analysis time."
        )
    elif holder_sample_size:
        holder_caveat = (
            f"An exact holder count was unavailable. Chainseer observed the "
            f"{holder_sample_size} largest "
            f"{'token accounts' if is_solana else 'holder records'} only."
        )
    else:
        holder_caveat = "Holder-count evidence was unavailable."
    if holder_evidence.get("caveat"):
        holder_caveat = (
            f"{holder_caveat} {holder_evidence['caveat']}"
        )

    return {
        "schema_version": "1.1",
        "token": {
            "address": report.get("token_address"),
            "name": report.get("token_name") or basic.get("name"),
            "symbol": report.get("token_symbol") or basic.get("symbol"),
            "chain": report.get("chain_name") or "Robinhood Chain",
            "chain_id": report.get("chain_id"),
            "explorer_url": report.get("explorer_url"),
        },
        "decision": {
            "action": analysis.get("action_label"),
            "risk_level": analysis.get("risk_level"),
            "model_risk_level": analysis.get("model_risk_level"),
            "score": analysis.get("legitimacy_score"),
            "confidence": analysis.get("confidence_grade"),
            "confidence_detail": analysis.get("confidence"),
            "recommendation": analysis.get("recommendation"),
            "hard_stops": analysis.get("hard_stop_overrides") or [],
        },
        "factors": analysis.get("component_scores") or {},
        "flags": {
            "red": analysis.get("red_flags") or [],
            "yellow": analysis.get("yellow_flags") or [],
            "green": analysis.get("green_flags") or [],
            "unknown": analysis.get("uncertain_components") or {},
        },
        "market": {
            "price_usd": dex.get("primary_price_usd"),
            "market_cap_usd": dex.get("market_cap"),
            "market_cap_kind": dex.get(
                "market_cap_kind",
                (
                    "reported_market_cap"
                    if dex.get("market_cap") not in (None, "", 0, "0")
                    else "unavailable"
                ),
            ),
            "market_cap_source": dex.get("market_cap_source"),
            "fdv_usd": dex.get("fdv"),
            "liquidity_usd": dex.get("total_liquidity_usd"),
            "volume_24h_usd": dex.get("total_volume_24h"),
            "age": dex.get("token_age_label"),
        },
        "holders": {
            "count": holder_count,
            "count_status": (
                "reported" if holder_count is not None else "unavailable"
            ),
            "count_source": holder_count_source,
            "sample_size": holder_sample_size,
            "sample_kind": (
                "largest_token_accounts"
                if is_solana and holder_sample_size
                else "top_holder_records" if holder_sample_size else None
            ),
            "largest_holder_pct": largest_holder_pct,
            "top10_holder_pct": top10_holder_pct,
            "concentration_basis": (
                holder_assessment.get("concentration_source")
                or holder_evidence.get("concentration_basis")
                or holder_evidence.get("method")
            ),
            "pool_and_program_vaults_excluded": holder_evidence.get(
                "pool_and_program_vaults_excluded"
            ),
            "caveat": holder_caveat,
        },
        "liquidity_custody": {
            "state": liquidity_custody.get(
                "state", "custody_unverified"
            ),
            "amm_version": liquidity_custody.get(
                "amm_version", dex.get("primary_amm_version", "unknown")
            ),
            "method": liquidity_custody.get("method"),
            "locked": bool(liquidity_custody.get("locked")),
            "withdrawal_verified": bool(
                liquidity_custody.get("withdrawal_verified")
            ),
            "withdrawable_pct": liquidity_custody.get("withdrawable_pct"),
        },
        "extended_evidence": {
            "social_attention": {
                "status": social_attention.get("status"),
                "trust": social_attention.get("trust"),
                "bounded_score": social_attention.get("bounded_score"),
                "channels": social_attention.get("channels") or [],
                "dexscreener_boosts": social_attention.get(
                    "dexscreener_boosts", 0
                ),
                "can_trigger_hard_stop": False,
                "caveat": social_attention.get("caveat"),
            },
            "cross_chain": {
                "status": cross_chain.get("status"),
                "foreign_markets": cross_chain.get("foreign_markets") or [],
                "verified_flow_count": cross_chain.get(
                    "verified_flow_count", 0
                ),
                "can_trigger_hard_stop": False,
                "caveat": cross_chain.get("caveat"),
            },
            "mev_exposure": {
                "status": mev_exposure.get("status"),
                "risk_level": mev_exposure.get("risk_level"),
                "warnings": mev_exposure.get("warnings") or [],
                "scoring_scope": mev_exposure.get("scoring_scope"),
            },
        },
        "evidence": {
            "fact_count": provenance.get("fact_count", 0),
            "block_pin": provenance.get("block_pin"),
            "anchor_type": provenance.get("anchor_type", "block_pin"),
            "anchor_caveat": provenance.get("anchor_caveat"),
            "infrastructure_indeterminate": (
                report.get("infrastructure_indeterminate") or []
            ),
            "ledger_hash": ledger_hash,
            "facts": public_facts,
        },
        "timechain": {
            "ring": report.get("analysis_ring"),
            "ring_hash": report.get("analysis_ring_hash"),
            "decision": (report.get("poq_verdict") or {}).get("decision"),
            "scores": report.get("poq_scores") or {},
            "cognition": report.get("cognition") or {},
            "cognitive_ring": report.get("cognitive_ring"),
            "cognitive_ring_hash": report.get("cognitive_ring_hash"),
        },
        "analyzed_at": report.get("timestamp"),
        "disclaimer": (
            "Informational risk analysis only. This is not financial advice "
            "or proof that a token is safe."
        ),
    }


SETTINGS = Settings()
SETTINGS.validate()
SERVICE = AnalysisService(SETTINGS)
LIMITER = SlidingWindowRateLimiter(SETTINGS.rate_limit_per_minute)
LEASE = SingleProcessLease(SETTINGS.chain_root)


def require_api_token(
    authorization: str | None = Header(default=None),
) -> None:
    if not SETTINGS.api_token and SETTINGS.environment != "production":
        return
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="missing API credentials",
        )
    supplied = authorization[7:]
    if not hmac.compare_digest(supplied, SETTINGS.api_token):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid API credentials",
        )


def request_identity(request: Request) -> str:
    # The authenticated website proxy sends a one-way HMAC identity so the
    # service can rate-limit end users without receiving their raw IP address.
    proxied = request.headers.get("x-chainseer-client", "")
    if re.fullmatch(r"[a-f0-9]{64}", proxied):
        return proxied
    host = request.client.host if request.client else "unknown"
    return hashlib.sha256(host.encode("utf-8")).hexdigest()


@asynccontextmanager
async def lifespan(_: FastAPI):
    LEASE.acquire()
    try:
        SERVICE.start()
        yield
    finally:
        stopped = SERVICE.stop()
        if stopped:
            LEASE.release()
        else:
            LOGGER.error(
                "Analysis worker exceeded shutdown grace; retaining the "
                "Timechain lease until process exit"
            )


app = FastAPI(
    title="Chainseer Analysis API",
    version="1.0.0",
    docs_url=None if SETTINGS.environment == "production" else "/docs",
    redoc_url=None,
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=list(SETTINGS.allowed_origins),
    allow_credentials=False,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
)
app.add_middleware(
    TrustedHostMiddleware,
    allowed_hosts=list(SETTINGS.allowed_hosts),
)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or secrets.token_hex(12)

    def finalize(response):
        response.headers["X-Request-ID"] = request_id[:128]
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    content_length = request.headers.get("content-length")
    if content_length:
        try:
            length = int(content_length)
            if length < 0 or length > SETTINGS.max_request_bytes:
                return finalize(
                    JSONResponse(
                        {"detail": "request body is too large"},
                        status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    )
                )
        except ValueError:
            return finalize(
                JSONResponse(
                    {"detail": "invalid content length"},
                    status_code=status.HTTP_400_BAD_REQUEST,
                )
            )
    elif request.method in {"POST", "PUT", "PATCH"}:
        body = await request.body()
        if len(body) > SETTINGS.max_request_bytes:
            return finalize(
                JSONResponse(
                    {"detail": "request body is too large"},
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                )
            )
    response = await call_next(request)
    return finalize(response)


@app.get("/health/live")
def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
def ready() -> dict[str, Any]:
    if not SERVICE.ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="analysis worker is not ready",
        )
    return {
        "status": "ready",
        "queue_depth": SERVICE.work.qsize(),
        "environment": SETTINGS.environment,
        "watcher_enabled": SETTINGS.watcher_enabled,
        "watcher_last_error": SERVICE.watch_status().get("last_error"),
        "networks": ["robinhood", "solana"],
        "solana_rpc_configured": bool(SETTINGS.solana_rpc_url),
        "benchmark_capture": SERVICE.benchmark_status(),
    }


@app.post(
    "/v1/analyses",
    response_model=JobAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_api_token)],
)
def create_analysis(payload: AnalyzeRequest, request: Request) -> JobAccepted:
    if not LIMITER.allow(request_identity(request)):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="analysis rate limit exceeded",
            headers={"Retry-After": "60"},
        )
    try:
        return SERVICE.submit(payload.address, payload.network)
    except QueueFullError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="analysis queue is full; try again shortly",
            headers={"Retry-After": "30"},
        ) from exc


@app.get(
    "/v1/analyses/{job_id}",
    dependencies=[Depends(require_api_token)],
)
def get_analysis(job_id: str) -> dict[str, Any]:
    if not re.fullmatch(r"[a-f0-9]{32}", job_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="analysis job not found",
        )
    job = SERVICE.get(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="analysis job not found",
        )
    return job.public()


@app.get(
    "/v1/watch",
    dependencies=[Depends(require_api_token)],
)
def get_watch_status() -> dict[str, Any]:
    return SERVICE.watch_status()


@app.post(
    "/v1/watch",
    dependencies=[Depends(require_api_token)],
)
def create_watch(payload: WatchRequest) -> dict[str, Any]:
    return {
        "enabled": SETTINGS.watcher_enabled,
        "subscription": SERVICE.watch_subscribe(payload.address),
    }


@app.delete(
    "/v1/watch/{address}",
    dependencies=[Depends(require_api_token)],
)
def delete_watch(address: str) -> dict[str, Any]:
    if not ADDRESS_RE.fullmatch(address):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="watch subscription not found",
        )
    return {
        "removed": SERVICE.watch_unsubscribe(address),
        "address": address,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "chainseer_api:app",
        host=os.environ.get("CHAINSEER_API_HOST", "127.0.0.1"),
        port=_server_port(),
        workers=1,
        reload=False,
    )
