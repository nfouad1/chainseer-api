"""Production HTTP boundary for the Robinhood Chain Chainseer engine.

The API deliberately runs one analysis worker. Chainseer owns mutable,
request-scoped scan state and appends to a single Timechain, so concurrent
analysis in one process would be unsafe. A separate scheduler thread triggers
watcher work only while the analysis lane is idle; the Timechain lock still
serializes cognitive writes, but a three-network watcher sweep can no longer
occupy the queue worker itself. Web requests enqueue jobs and poll for a
structured public report.
"""

from __future__ import annotations

import hashlib
import hmac
import inspect
import json
import logging
import os
import queue
import re
import statistics
try:
    import resource  # POSIX only -- unavailable on Windows dev/test hosts
except ImportError:
    resource = None
import secrets
import socket
import threading
import time
import uuid
from collections import OrderedDict, deque
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator, model_validator
from starlette.responses import JSONResponse, Response

from chainseer import Chainseer, RobinhoodRPC
from chainseer_base_public import BasePublicAnalyzer
from chainseer_benchmark import (
    append_observation,
    build_observation_from_report,
    case_bank_status,
    load_jsonl,
)
from chainseer_controls import (
    ChainseerWatcher,
    SolanaEventWatcher,
    SolanaWatchConfig,
    WatchConfig,
    credential_safe_error,
)
from chainseer_deferred import DeferredQueueItem, DurableDeferredQueue
from chainseer_job_store import SharedJobStore, create_shared_job_store
from chainseer_memory import MemoryCore, MemoryCoreError
from chainseer_outcome_ledger import analysis_evidence_binding
from chainseer_solana_public import (
    SolanaMintError,
    SolanaPublicAnalyzer,
    validate_solana_mint,
)
from chainseer_temporal_graph import (
    append_temporal_projection,
    refresh_temporal_projection,
)
from chainseer_wallet_convergence import WalletConvergenceTracker

LOGGER = logging.getLogger("chainseer.api")
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
SUPPORTED_NETWORKS = {"robinhood", "base", "solana"}
EVM_NETWORKS = {"robinhood", "base"}
WATCH_MUTATION_LOCK_TIMEOUT_SECONDS = 0.25


def deterministic_benchmark_split(network: str, address: str) -> str:
    """Keep every observation for one token in the same leakage-safe split."""
    normalized = address.lower() if network in EVM_NETWORKS else address
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
    raise RuntimeError(f"{name} must be a boolean (0/1/true/false)")


def _env_float(
    name: str, default: float, minimum: float, maximum: float
) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be a number") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}")
    return value


def _cypher_tempre_runtime_status() -> dict[str, Any]:
    """Return a bounded, public-safe attestation of the loaded skill runtime."""

    configured = os.environ.get("CHAINSEER_SKILL_DIR", "").strip()
    skill_dir = Path(configured).expanduser() if configured else None
    version = None
    if skill_dir is not None:
        try:
            version = (skill_dir / "VERSION").read_text(
                encoding="utf-8"
            ).strip()
        except OSError:
            version = None
    expected = os.environ.get(
        "CHAINSEER_CYPHER_TEMPRE_VERSION", ""
    ).strip() or None
    commit = os.environ.get(
        "CHAINSEER_CYPHER_TEMPRE_COMMIT", ""
    ).strip() or None
    attested = bool(version and expected and version == expected)
    return {
        "status": "verified" if attested else "unattested",
        "version": version,
        "expected_version": expected,
        "commit": commit,
    }


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
    paper_telemetry_token: str = field(
        default_factory=lambda: os.environ.get("CHAINSEER_PAPER_TELEMETRY_TOKEN", "")
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
            # A completed job only needs to outlive a client's poll loop, not
            # sit resident for an hour. On a memory-constrained instance with
            # continuous watcher-driven analyses, an hour of full result
            # retention was a major contributor to sustained RSS growth.
            "CHAINSEER_RESULT_TTL_SECONDS", 600, 60, 86400
        )
    )
    cache_ttl_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_SCAN_CACHE_TTL_SECONDS", 300, 0, 3600
        )
    )
    process_role: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_PROCESS_ROLE", "combined"
        ).strip().lower()
    )
    writer_internal_url: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_WRITER_INTERNAL_URL",
            "http://writer.process.chainseer-api.internal:8000",
        ).strip().rstrip("/")
    )
    writer_proxy_timeout_seconds: float = field(
        default_factory=lambda: _env_float(
            "CHAINSEER_WRITER_PROXY_TIMEOUT_SECONDS", 30.0, 1.0, 300.0
        )
    )
    stale_result_ttl_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_STALE_RESULT_TTL_SECONDS", 86400, 300, 604800
        )
    )
    rate_limit_per_minute: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_RATE_LIMIT_PER_MINUTE", 6, 1, 120
        )
    )
    global_rate_limit_per_minute: int = field(
        # Bounds total request throughput across every claimed identity
        # combined, since the per-identity limit alone is only as strong as
        # request_identity()'s unverifiable header (see
        # SlidingWindowRateLimiter's docstring).
        default_factory=lambda: _env_int(
            "CHAINSEER_GLOBAL_RATE_LIMIT_PER_MINUTE", 60, 1, 6000
        )
    )
    shutdown_grace_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_SHUTDOWN_GRACE_SECONDS", 180, 10, 900
        )
    )
    base_rpc_url: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_BASE_RPC_URL",
            "https://mainnet.base.org",
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
            # Each idle tick still allocates RPC/HTTP call structures for
            # every subscription across all three networks even when no
            # rescan is due; at 15s this churn runs continuously. 60s cuts
            # that allocation/deallocation volume roughly 4x with no material
            # loss of monitoring freshness.
            "CHAINSEER_WATCHER_INTERVAL_SECONDS", 60, 3, 3600
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
    memory_backup_root: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_MEMORY_BACKUP_ROOT",
            str(
                Path(
                    os.environ.get(
                        "CHAINSEER_CHAIN_ROOT",
                        str(Path(__file__).resolve().parent / "chainseer_chain"),
                    )
                ).parent
                / "chainseer_backups"
            ),
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
    benchmark_base_cohort: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_BENCHMARK_BASE_COHORT",
            "base_public_analysis",
        ).strip()
    )
    full_audit_interval_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_FULL_AUDIT_INTERVAL_SECONDS",
            900,
            60,
            86400,
        )
    )
    timechain_lock_timeout_seconds: float = field(
        default_factory=lambda: _env_float(
            "CHAINSEER_TIMECHAIN_LOCK_TIMEOUT_SECONDS",
            5.0,
            1.0,
            30.0,
        )
    )
    memory_warning_mb: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_MEMORY_WARNING_MB",
            1536,
            256,
            16384,
        )
    )
    temporal_projection_enabled: bool = field(
        default_factory=lambda: _env_bool(
            "CHAINSEER_TEMPORAL_PROJECTION_ENABLED", True
        )
    )
    cognitive_completion_enabled: bool = field(
        default_factory=lambda: _env_bool(
            "CHAINSEER_COGNITIVE_COMPLETION_ENABLED", True
        )
    )
    shared_store_url: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_SHARED_STORE_URL", ""
        ).strip()
    )
    shared_store_prefix: str = field(
        default_factory=lambda: os.environ.get(
            "CHAINSEER_SHARED_STORE_PREFIX", "chainseer"
        ).strip()
    )
    shared_store_timeout_seconds: float = field(
        default_factory=lambda: _env_float(
            "CHAINSEER_SHARED_STORE_TIMEOUT_SECONDS",
            2.0,
            0.1,
            10.0,
        )
    )
    shared_work_queue_enabled: bool = field(
        default_factory=lambda: _env_bool(
            "CHAINSEER_SHARED_WORK_QUEUE_ENABLED", False
        )
    )
    writer_lease_ttl_seconds: int = field(
        default_factory=lambda: _env_int(
            "CHAINSEER_WRITER_LEASE_TTL_SECONDS", 30, 10, 300
        )
    )

    def validate(self) -> None:
        if self.process_role not in {"combined", "gateway", "writer"}:
            raise RuntimeError(
                "CHAINSEER_PROCESS_ROLE must be combined, gateway, or writer"
            )
        if self.environment not in {"development", "test", "production"}:
            raise RuntimeError(
                "CHAINSEER_ENVIRONMENT must be development, test, or production"
            )
        rpc = urlparse(self.rpc_url)
        if rpc.scheme not in {"http", "https"} or not rpc.hostname:
            raise RuntimeError("CHAINSEER_RPC_URL must be an HTTP(S) URL")
        base_rpc = urlparse(self.base_rpc_url)
        if base_rpc.scheme not in {"http", "https"} or not base_rpc.hostname:
            raise RuntimeError(
                "CHAINSEER_BASE_RPC_URL must be an HTTP(S) URL"
            )
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
            if base_rpc.scheme != "https":
                raise RuntimeError(
                    "CHAINSEER_BASE_RPC_URL must use HTTPS in production"
                )
            if solana_rpc.scheme != "https":
                raise RuntimeError(
                    "CHAINSEER_SOLANA_RPC_URL must use HTTPS in production"
                )
            if not Path(self.chain_root).is_absolute():
                raise RuntimeError(
                    "CHAINSEER_CHAIN_ROOT must be absolute in production"
                )
            if not Path(self.memory_backup_root).is_absolute():
                raise RuntimeError(
                    "CHAINSEER_MEMORY_BACKUP_ROOT must be absolute in production"
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
            if not self.benchmark_base_cohort:
                raise RuntimeError(
                    "CHAINSEER_BENCHMARK_BASE_COHORT is required"
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
        if self.shared_work_queue_enabled and not self.shared_store_url:
            raise RuntimeError(
                "CHAINSEER_SHARED_WORK_QUEUE_ENABLED requires "
                "CHAINSEER_SHARED_STORE_URL"
            )
        if self.process_role in {"gateway", "writer"}:
            if not self.shared_work_queue_enabled or not self.shared_store_url:
                raise RuntimeError(
                    "split gateway/writer roles require the shared Redis work queue"
                )
        if self.process_role == "gateway":
            writer_url = urlparse(self.writer_internal_url)
            if (
                writer_url.scheme not in {"http", "https"}
                or not writer_url.hostname
            ):
                raise RuntimeError(
                    "CHAINSEER_WRITER_INTERNAL_URL must be an HTTP(S) URL"
                )
        backup_path = Path(self.memory_backup_root).resolve()
        chain_path = Path(self.chain_root).resolve()
        if backup_path == chain_path or chain_path in backup_path.parents:
            raise RuntimeError(
                "CHAINSEER_MEMORY_BACKUP_ROOT must be outside CHAINSEER_CHAIN_ROOT"
            )


class AnalyzeRequest(BaseModel):
    network: str = Field(default="robinhood", min_length=4, max_length=16)
    address: str = Field(min_length=32, max_length=44)
    force_refresh: bool = False

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
        if self.network in EVM_NETWORKS:
            if not ADDRESS_RE.fullmatch(self.address):
                raise ValueError("invalid EVM contract address")
        else:
            validate_solana_mint(self.address)
        return self


class WatchRequest(BaseModel):
    network: str = Field(default="robinhood", min_length=4, max_length=16)
    address: str = Field(min_length=32, max_length=44)

    @field_validator("network")
    @classmethod
    def validate_network(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in SUPPORTED_NETWORKS:
            raise ValueError("unsupported watch network")
        return normalized

    @model_validator(mode="after")
    def validate_address_for_network(self):
        self.address = self.address.strip()
        if self.network in EVM_NETWORKS:
            if not ADDRESS_RE.fullmatch(self.address):
                raise ValueError("invalid EVM contract address")
        else:
            validate_solana_mint(self.address)
        return self


class MemoryQueryRequest(AnalyzeRequest):
    topics: list[str] = Field(
        default_factory=lambda: [
            "latest_assessment",
            "risk_history",
            "entity_history",
            "outcomes",
        ],
        min_length=1,
        max_length=4,
    )
    limit: int = Field(default=20, ge=1, le=100)

    @field_validator("topics")
    @classmethod
    def validate_topics(cls, value: list[str]) -> list[str]:
        allowed = {
            "latest_assessment",
            "risk_history",
            "entity_history",
            "outcomes",
        }
        normalized = []
        for item in value:
            topic = str(item or "").strip().lower()
            if topic not in allowed:
                raise ValueError("unsupported memory topic")
            if topic not in normalized:
                normalized.append(topic)
        if not normalized:
            raise ValueError("at least one memory topic is required")
        return normalized


class PaperTelemetryRequest(BaseModel):
    """Bounded read-only snapshot supplied by the paper learner."""

    payload: dict[str, Any]

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        positions = value.get("positions", [])
        if not isinstance(positions, list) or len(positions) > 250:
            raise ValueError("paper telemetry positions must contain at most 250 rows")
        if value.get("paper_only") is not True:
            raise ValueError("paper telemetry must explicitly declare paper_only")
        return value


class JobAccepted(BaseModel):
    job_id: str
    status: str
    cached: bool = False
    refreshing: bool = False
    previous_result: dict[str, Any] | None = None
    previous_result_age_seconds: float | None = None


class RingImportItem(BaseModel):
    timestamp: str
    payload: dict[str, Any]

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: str) -> str:
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("invalid ring timestamp") from exc
        return value


class RingImportRequest(BaseModel):
    rings: list[RingImportItem] = Field(min_length=1, max_length=50)


@dataclass
class Job:
    id: str
    address: str
    network: str = "robinhood"
    status: str = "queued"
    stage: str = "queued"
    stage_detail: str = "Waiting for the analysis worker"
    progress_percent: int = 0
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    analysis_latency_ms: float | None = None
    result: dict[str, Any] | None = None
    benchmark_capture: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None
    lock_retry_count: int = 0
    cognition_status: str = "not_started"
    cognition_stage_detail: str = "Cognitive completion starts after analysis"
    cognition_progress_percent: int = 0
    cognition_updated_at: float | None = None
    stage_started_at: float | None = None
    stage_timings_ms: dict[str, float] = field(default_factory=dict)

    def public(self) -> dict[str, Any]:
        return {
            "job_id": self.id,
            "address": self.address,
            "network": self.network,
            "status": self.status,
            "stage": self.stage,
            "stage_detail": self.stage_detail,
            "progress_percent": self.progress_percent,
            "created_at": _iso(self.created_at),
            "updated_at": _iso(self.updated_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "timing": {
                "queue_delay_ms": (
                    round(max(0.0, (self.started_at - self.created_at) * 1000), 1)
                    if self.started_at is not None
                    else None
                ),
                "analysis_latency_ms": (
                    round(self.analysis_latency_ms, 1)
                    if self.analysis_latency_ms is not None
                    else None
                ),
                "stages_ms": dict(self.stage_timings_ms),
            },
            "result": self.result,
            "benchmark_capture": self.benchmark_capture,
            "cognitive_completion": {
                "status": self.cognition_status,
                "stage_detail": self.cognition_stage_detail,
                "progress_percent": self.cognition_progress_percent,
                "updated_at": _iso(self.cognition_updated_at),
            },
            "error": (
                {
                    "code": self.error_code,
                    "message": self.error_message,
                }
                if self.error_code
                else None
            ),
        }

    @classmethod
    def from_public(cls, value: dict[str, Any]) -> "Job":
        """Rehydrate only worker-owned fields from a shared queue snapshot."""
        job_id = str(value.get("job_id") or "")
        address = str(value.get("address") or "")
        network = str(value.get("network") or "")
        if not re.fullmatch(r"[a-f0-9]{32}", job_id):
            raise ValueError("shared scan job id is invalid")
        if network not in SUPPORTED_NETWORKS:
            raise ValueError("shared scan network is invalid")
        if network in EVM_NETWORKS:
            if not ADDRESS_RE.fullmatch(address):
                raise ValueError("shared EVM scan address is invalid")
        else:
            validate_solana_mint(address)
        timing = value.get("timing") or {}
        cognition = value.get("cognitive_completion") or {}
        error = value.get("error") or {}
        return cls(
            id=job_id,
            address=address,
            network=network,
            status=str(value.get("status") or "queued"),
            stage=str(value.get("stage") or "queued"),
            stage_detail=str(
                value.get("stage_detail") or "Waiting for the analysis worker"
            ),
            progress_percent=int(value.get("progress_percent") or 0),
            created_at=_epoch_from_iso(value.get("created_at")) or time.time(),
            updated_at=_epoch_from_iso(value.get("updated_at")) or time.time(),
            started_at=_epoch_from_iso(value.get("started_at")),
            finished_at=_epoch_from_iso(value.get("finished_at")),
            analysis_latency_ms=(
                float(timing["analysis_latency_ms"])
                if timing.get("analysis_latency_ms") is not None
                else None
            ),
            result=(
                value.get("result")
                if isinstance(value.get("result"), dict)
                else None
            ),
            benchmark_capture=(
                value.get("benchmark_capture")
                if isinstance(value.get("benchmark_capture"), dict)
                else None
            ),
            error_code=str(error.get("code")) if error.get("code") else None,
            error_message=(
                str(error.get("message")) if error.get("message") else None
            ),
            cognition_status=str(cognition.get("status") or "not_started"),
            cognition_stage_detail=str(
                cognition.get("stage_detail")
                or "Cognitive completion starts after analysis"
            ),
            cognition_progress_percent=int(
                cognition.get("progress_percent") or 0
            ),
            cognition_updated_at=_epoch_from_iso(cognition.get("updated_at")),
        )


def _iso(value: float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, timezone.utc).isoformat()


def _epoch_from_iso(value: Any) -> float | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(
            str(value).replace("Z", "+00:00")
        ).timestamp()
    except (TypeError, ValueError):
        return None


#: case_bank_status()/append_observation() both reload and fully
#: re-validate the entire append-only observation/outcome ledgers from disk
#: -- O(total historical observations), growing without bound as the ledger
#: accumulates over the service's lifetime. status()/health_status() already
#: document this summary as an "immutable-enough" (eventually consistent)
#: snapshot, so the expensive recompute is throttled to at most once per
#: this interval rather than run unconditionally after every single
#: capture() -- otherwise every analysis gets progressively more expensive
#: as the ledger grows, compounding the allocation-churn pressure that also
#: drove the OOM investigation.
BENCHMARK_SUMMARY_REFRESH_MIN_INTERVAL_SECONDS = 30


class BenchmarkCaptureRecorder:
    """Append fresh analysis predictions to the durable benchmark ledger."""

    def __init__(
        self,
        settings: Settings,
        *,
        enabled: bool | None = None,
    ):
        self.settings = settings
        self.enabled = (
            settings.benchmark_capture_enabled if enabled is None else enabled
        )
        self.root = Path(settings.benchmark_root)
        self.observations_path = self.root / "observations-v1.jsonl"
        self.outcomes_path = self.root / "outcomes-v1.jsonl"
        self._lock = threading.Lock()
        self._last_summary_refresh_at = 0.0
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
            self._refresh_summary(force=True)
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

    def _refresh_summary(self, *, force: bool = False) -> None:
        now = time.monotonic()
        if (
            not force
            and now - self._last_summary_refresh_at
            < BENCHMARK_SUMMARY_REFRESH_MIN_INTERVAL_SECONDS
        ):
            return
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
        self._last_summary_refresh_at = now

    def capture(
        self,
        job: Job,
        public_report: dict[str, Any],
        *,
        captured_at: str | None = None,
    ) -> dict[str, Any]:
        if not self.enabled:
            return {"status": "disabled"}
        cohort = {
            "robinhood": self.settings.benchmark_robinhood_cohort,
            "base": self.settings.benchmark_base_cohort,
            "solana": self.settings.benchmark_solana_cohort,
        }[job.network]
        split = deterministic_benchmark_split(job.network, job.address)
        latency_ms = (
            max(0.0, float(job.analysis_latency_ms))
            if job.analysis_latency_ms is not None
            else max(
                0.0,
                (time.time() - (job.started_at or time.time())) * 1000,
            )
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

    def health_status(self) -> dict[str, Any]:
        """Return the latest immutable-enough snapshot without blocking probes."""
        return dict(self._summary)


class SlidingWindowRateLimiter:
    #: request_identity() cannot cryptographically verify the caller-supplied
    #: X-Chainseer-Client header (it only checks the format matches a
    #: 64-hex digest -- see request_identity()'s docstring), so a caller who
    #: already holds the bearer token could rotate fake identities forever.
    #: Without a bound, every distinct identity -- spoofed or a real
    #: long-lived device -- permanently grows self._events, since nothing
    #: ever sweeps a key whose window has fully expired if that identity is
    #: never seen again. Evict least-recently-used identities past this cap
    #: instead of growing without bound.
    MAX_TRACKED_IDENTITIES = 10_000

    def __init__(
        self,
        limit: int,
        window_seconds: int = 60,
        *,
        global_limit: int | None = None,
        shared_store: SharedJobStore | None = None,
    ):
        self.limit = limit
        self.window_seconds = window_seconds
        # Defense-in-depth against the same unverifiable-identity gap: even
        # if every request claims a different identity, total throughput
        # across all of them combined is still bounded. None disables it.
        self.global_limit = global_limit
        self.shared_store = shared_store
        self._events: "OrderedDict[str, deque[float]]" = OrderedDict()
        self._global_events: deque[float] = deque()
        self._lock = threading.Lock()

    def allow(self, identity: str, now: float | None = None) -> bool:
        if self.shared_store is not None:
            wall_now = now if now is not None else time.time()
            try:
                return self.shared_store.allow_request(
                    identity,
                    identity_limit=self.limit,
                    global_limit=self.global_limit,
                    window_seconds=self.window_seconds,
                    now_ms=int(wall_now * 1000),
                    request_id=f"{int(wall_now * 1_000_000)}:{uuid.uuid4().hex}",
                )
            except Exception as exc:
                raise SharedStoreUnavailableError(
                    "shared rate limiter is temporarily unavailable"
                ) from exc
        now = now if now is not None else time.monotonic()
        cutoff = now - self.window_seconds
        with self._lock:
            if self.global_limit is not None:
                global_events = self._global_events
                while global_events and global_events[0] <= cutoff:
                    global_events.popleft()
                if len(global_events) >= self.global_limit:
                    return False

            events = self._events.get(identity)
            if events is None:
                events = deque()
            else:
                self._events.move_to_end(identity)
                while events and events[0] <= cutoff:
                    events.popleft()

            if len(events) >= self.limit:
                if events:
                    self._events[identity] = events
                else:
                    self._events.pop(identity, None)
                return False

            events.append(now)
            self._events[identity] = events
            while len(self._events) > self.MAX_TRACKED_IDENTITIES:
                self._events.popitem(last=False)
            if self.global_limit is not None:
                self._global_events.append(now)
            return True


@dataclass
class MaintenanceTask:
    job_id: str
    network: str
    address: str
    report: dict[str, Any]
    public_report: dict[str, Any]
    benchmark_done: bool = False
    projection_done: bool = False


@dataclass
class FullAuditCursor:
    """Bounded, resumable state for a deep append-only ledger walk."""

    target_index: int
    target_hash: str
    started_monotonic: float
    started_at: str
    offset: int = 0
    next_index: int = 0
    previous_hash: str = "0" * 64
    verified_rings: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class PreparedWatcherCommit:
    """Immutable watcher cognition prepared without owning the writer lane."""

    queue_item: DeferredQueueItem
    payload: dict[str, Any]
    poq_scores: dict[str, Any]
    policy_hash: str
    registry_hash: str


@dataclass
class DeferredSealJob:
    """Deprecated compatibility envelope; production uses DurableDeferredQueue."""

    network: str
    token_address: str
    pinned_snapshot: dict[str, Any]
    block_or_slot: int | None
    evidence_hash: str
    report_hash: str
    analyzer_version: str
    idempotency_key: str
    prepared_head: int
    enqueued_at: float


class DistributedWriterLease:
    """Renewable Redis lease for the sole authoritative Timechain writer."""

    def __init__(
        self,
        store: SharedJobStore | None,
        *,
        owner_id: str,
        ttl_seconds: int,
    ):
        self.store = store
        self.owner_id = owner_id
        self.ttl_seconds = max(10, int(ttl_seconds))
        self._stopping = threading.Event()
        self._healthy = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_confirmed_monotonic = 0.0

    @property
    def enabled(self) -> bool:
        return self.store is not None

    @property
    def healthy(self) -> bool:
        return not self.enabled or self._healthy.is_set()

    def acquire(self) -> None:
        if self.store is None:
            self._healthy.set()
            return
        if not self.store.claim_writer(self.owner_id, self.ttl_seconds):
            raise RuntimeError(
                "another process owns the authoritative Timechain writer lease"
            )
        self._stopping.clear()
        self._healthy.set()
        self._last_confirmed_monotonic = time.monotonic()
        self._thread = threading.Thread(
            target=self._renew_loop,
            name="chainseer-writer-lease",
            daemon=True,
        )
        self._thread.start()

    def _renew_loop(self) -> None:
        interval = max(1.0, self.ttl_seconds / 3.0)
        wait_seconds = interval
        while not self._stopping.wait(wait_seconds):
            try:
                renewed = bool(
                    self.store
                    and self.store.renew_writer(
                        self.owner_id, self.ttl_seconds
                    )
                )
            except Exception:
                LOGGER.exception("Timechain writer lease renewal failed")
                renewed = False
            if renewed:
                self._last_confirmed_monotonic = time.monotonic()
                wait_seconds = interval
                continue
            # A single network hiccup must not unnecessarily take production
            # offline while the Redis lease is still valid. Retry quickly, but
            # fence this process well before another owner could claim the TTL.
            elapsed = time.monotonic() - self._last_confirmed_monotonic
            if elapsed >= self.ttl_seconds * 0.6:
                self._healthy.clear()
                LOGGER.critical(
                    "Timechain writer lease was lost; all new appends are fenced"
                )
                return
            wait_seconds = 1.0

    def release(self) -> None:
        self._stopping.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        if self.store is not None and self._healthy.is_set():
            try:
                self.store.release_writer(self.owner_id)
            except Exception:
                LOGGER.exception("Could not release Timechain writer lease")
        self._healthy.clear()


class AnalysisService:
    def __init__(
        self,
        settings: Settings,
        *,
        shared_job_store: SharedJobStore | None = None,
        writer_lease: DistributedWriterLease | None = None,
    ):
        self.settings = settings
        self._gateway_only = settings.process_role == "gateway"
        self._shared_job_store = shared_job_store
        self._writer_lease = writer_lease
        self.jobs: dict[str, Job] = {}
        self.active_by_address: dict[str, str] = {}
        self.cache: dict[str, tuple[float, str]] = {}
        self.work: queue.Queue[str | None] = queue.Queue(
            maxsize=settings.queue_size
        )
        self._lock = threading.RLock()
        self._worker: threading.Thread | None = None
        self._watcher_worker: threading.Thread | None = None
        self._maintenance_worker: threading.Thread | None = None
        self._maintenance_work: queue.Queue[MaintenanceTask | None] = (
            queue.Queue(maxsize=max(4, settings.queue_size * 2))
        )
        # Lock-owner tracking for timeout diagnostics.
        self._timechain_owner: threading.Thread | None = None
        self._timechain_owner_since: float = 0.0
        self._timechain_owner_reason: str = ""
        self._timechain_owner_depth: int = 0
        self._timechain_owner_guard = threading.Lock()
        self._deferred_queue = (
            None
            if self._gateway_only
            else DurableDeferredQueue(
                Path(settings.chain_root) / "deferred_commits.sqlite3"
            )
        )
        # Compatibility-only seam for pre-durable-queue callers/tests.  No
        # production path enqueues here.
        self._deferred_seal_work: queue.Queue[DeferredSealJob | None] = (
            queue.Queue(maxsize=64)
        )
        self._analysis_active = threading.Event()
        self._timechain_lock = threading.RLock()
        self._agent: Chainseer | None = None
        self._base_agent: BasePublicAnalyzer | None = None
        self._solana_agent: SolanaPublicAnalyzer | None = None
        self._watch_analysis_agent: Chainseer | None = None
        self._base_watch_analysis_agent: BasePublicAnalyzer | None = None
        self._solana_watch_analysis_agent: SolanaPublicAnalyzer | None = None
        self._memory: MemoryCore | None = None
        self._memory_status_snapshot: tuple[float, dict[str, Any]] | None = None
        self._memory_status_snapshot_lock = threading.Lock()
        self._memory_status_refresh_lock = threading.Lock()
        self._memory_status_worker: threading.Thread | None = None
        self._stopping = threading.Event()
        self._ready = threading.Event()
        self._integrity_status: dict[str, Any] = {
            "status": "initializing",
            "last_full_audit_at": None,
            "last_full_audit_duration_seconds": None,
            "last_error": None,
            "full_audit_progress": None,
        }
        self._full_audit_cursor: FullAuditCursor | None = None
        self._watch_lock = threading.Lock()
        self._watcher: ChainseerWatcher | None = None
        self._base_watcher: ChainseerWatcher | None = None
        self._solana_watcher: SolanaEventWatcher | None = None
        self._watcher_status: dict[str, Any] = {
            "enabled": settings.watcher_enabled,
            "last_cycle": {
                "robinhood": None,
                "base": None,
                "solana": None,
            },
            "last_error": None,
            "last_deferred": None,
        }
        self._benchmark = BenchmarkCaptureRecorder(
            settings,
            enabled=False if self._gateway_only else None,
        )
        self._base_analysis_idempotency_keys: set[str] | None = None
        self._cypher_tempre_runtime = (
            {
                "status": "delegated",
                "reason": "authoritative_writer_process",
            }
            if self._gateway_only
            else _cypher_tempre_runtime_status()
        )
        self._last_memory_rss_mb: float | None = None
        self._last_memory_peak_mb: float | None = None
        self._memory_warning_active = False
        self._process_started_at = time.time()
        self._process_instance_id = uuid.uuid4().hex[:16]
        self._last_cpu_sample: tuple[float, float] | None = None
        self._last_cpu_percent: float | None = None
        self._cgroup_cpu: dict[str, int] = {}
        self._recent_analysis_latencies_ms: deque[float] = deque(maxlen=100)
        self._latest_analysis_summary: dict[str, Any] | None = None
        self._maintenance_telemetry = {
            "full_audit_deferred_analysis": 0,
            "full_audit_deferred_memory": 0,
        }
        self._last_full_audit_deferred_reason: str | None = None

    @staticmethod
    def _install_durable_timechain_append(agent: Any) -> bool:
        """Replace the runtime's buffered append with write-through + fsync.

        The pinned Cypher Tempre runtime closes its text handle after writing,
        which flushes Python buffers but does not force the newest filesystem
        extent to stable storage. A VM restart can therefore leave a valid
        file length followed by NULs. The API owns the single writer, so this
        instance-level adapter safely strengthens every ring append without
        changing the vendored, commit-attested runtime.
        """
        tc = getattr(agent, "tc", None)
        if tc is None or getattr(tc, "_chainseer_durable_append", False):
            return False
        if not callable(getattr(tc, "_append", None)):
            return False
        rings_path = Path(tc.rings_path)
        auto_attest = getattr(tc, "_auto_attest", None)

        def durable_append(ring: dict[str, Any]) -> None:
            payload = (
                json.dumps(ring, ensure_ascii=False) + "\n"
            ).encode("utf-8")
            flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            descriptor = os.open(str(rings_path), flags, 0o600)
            try:
                view = memoryview(payload)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise OSError("Timechain append made no progress")
                    view = view[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            if callable(auto_attest):
                auto_attest(ring)

        tc._append = durable_append
        tc._chainseer_durable_append = True
        return True

    def start(self) -> None:
        if self._gateway_only:
            if not self._shared_work_queue():
                raise RuntimeError(
                    "stateless gateway requires the shared Redis work queue"
                )
            if not self._shared_job_store or not self._shared_job_store.ping():
                raise RuntimeError("stateless gateway cannot reach Redis")
            self._stopping.clear()
            self._integrity_status = {
                "status": "delegated",
                "writer_role": "writer",
                "last_full_audit_at": None,
                "last_full_audit_duration_seconds": None,
                "last_error": None,
                "full_audit_progress": None,
            }
            self._ready.set()
            return
        if self._worker and self._worker.is_alive():
            return
        if self._writer_lease is not None and not self._writer_lease.healthy:
            raise RuntimeError("authoritative Timechain writer lease is unavailable")
        self._stopping.clear()
        if (
            self.settings.environment == "production"
            and self._cypher_tempre_runtime.get("status") != "verified"
        ):
            raise RuntimeError(
                "Cypher Tempre production runtime is not version-attested"
            )
        if self._agent is None:
            self._agent = Chainseer(
                rpc_url=self.settings.rpc_url,
                chain_root=self.settings.chain_root,
            )
        self._install_durable_timechain_append(self._agent)
        if self._memory is None and hasattr(self._agent, "tc"):
            self._memory = MemoryCore(
                self._agent.tc,
                self.settings.chain_root,
                backup_root=self.settings.memory_backup_root,
            )
        if self._solana_agent is None:
            self._solana_agent = SolanaPublicAnalyzer(
                self.settings.solana_rpc_url,
                timechain_agent=self._agent,
                jupiter_api_key=self.settings.jupiter_api_key or None,
                convergence_tracker=WalletConvergenceTracker(
                    Path(self.settings.chain_root)
                    / "solana_public_wallet_convergence.json"
                ),
            )
        if self._base_agent is None and isinstance(self._agent, Chainseer):
            self._base_agent = BasePublicAnalyzer(
                self.settings.base_rpc_url,
                timechain_agent=self._agent,
            )
        # Watcher analyzers duplicate sizable RPC and Timechain-facing state.
        # Keep the lightweight watcher stores for the authenticated watch API,
        # but only allocate dedicated observer analyzers when their background
        # polling worker is enabled.
        if self.settings.watcher_enabled:
            if (
                self._watch_analysis_agent is None
                and isinstance(self._agent, Chainseer)
            ):
                self._watch_analysis_agent = Chainseer(
                    rpc_url=self.settings.rpc_url,
                    timechain_agent=self._agent,
                )
            if (
                self._base_watch_analysis_agent is None
                and isinstance(self._agent, Chainseer)
            ):
                self._base_watch_analysis_agent = BasePublicAnalyzer(
                    self.settings.base_rpc_url,
                    timechain_agent=self._agent,
                )
            if (
                self._solana_watch_analysis_agent is None
                and isinstance(self._agent, Chainseer)
            ):
                self._solana_watch_analysis_agent = SolanaPublicAnalyzer(
                    self.settings.solana_rpc_url,
                    timechain_agent=self._agent,
                    jupiter_api_key=self.settings.jupiter_api_key or None,
                )
        if self._watcher is None:
            self._watcher = ChainseerWatcher(
                self._agent,
                observer_rpc=RobinhoodRPC(self.settings.rpc_url),
                analysis_agent=(self._watch_analysis_agent or self._agent),
                control_root=self.settings.chain_root,
                config=WatchConfig(
                    poll_seconds=self.settings.watcher_interval_seconds,
                    confirmations=self.settings.watcher_confirmations,
                ),
            )
        if self._solana_watcher is None:
            self._solana_watcher = SolanaEventWatcher(
                (self._solana_watch_analysis_agent or self._solana_agent),
                timechain_agent=self._agent,
                observer_analyzer=(
                    self._solana_watch_analysis_agent or self._solana_agent
                ),
                control_root=self.settings.chain_root,
                config=SolanaWatchConfig(
                    poll_seconds=self.settings.watcher_interval_seconds,
                ),
            )
        if self._base_watcher is None and self._base_agent is not None:
            self._base_watcher = ChainseerWatcher(
                self._base_agent,
                observer_rpc=RobinhoodRPC(self.settings.base_rpc_url),
                analysis_agent=(
                    self._base_watch_analysis_agent or self._base_agent
                ),
                control_root=self.settings.chain_root,
                config=WatchConfig(
                    poll_seconds=self.settings.watcher_interval_seconds,
                    confirmations=self.settings.watcher_confirmations,
                ),
                network="base",
            )
        if self.settings.shared_work_queue_enabled:
            if self._shared_job_store is None:
                raise RuntimeError("shared scan work queue is unavailable")
            recovered = self._shared_job_store.recover_claimed_scans()
            if recovered:
                LOGGER.warning(
                    "Recovered %d crash-stranded scan job(s)", recovered
                )
        self._worker = threading.Thread(
            target=self._run,
            name="chainseer-analysis-worker",
            daemon=True,
        )
        self._maintenance_worker = threading.Thread(
            target=self._run_maintenance,
            name="chainseer-maintenance-worker",
            daemon=True,
        )
        self._watcher_worker = (
            threading.Thread(
                target=self._run_watchers,
                name="chainseer-watcher-worker",
                daemon=True,
            )
            if self.settings.watcher_enabled
            else None
        )
        self._worker.start()
        self._maintenance_worker.start()
        if self._watcher_worker is not None:
            self._watcher_worker.start()
        if (
            self.settings.environment == "production"
            and self._memory is not None
        ):
            self._start_memory_status_refresh()
        self._integrity_status = {
            "status": "verified",
            "last_full_audit_at": datetime.now(timezone.utc).isoformat(),
            "last_full_audit_duration_seconds": None,
            "last_error": None,
            "full_audit_progress": None,
        }
        self._ready.set()

    def stop(self) -> bool:
        self._ready.clear()
        self._stopping.set()
        if self._gateway_only:
            return True
        try:
            self.work.put_nowait(None)
        except queue.Full:
            pass
        try:
            self._maintenance_work.put_nowait(None)
        except queue.Full:
            pass
        if self._worker:
            self._worker.join(
                timeout=self.settings.shutdown_grace_seconds
            )
        if self._maintenance_worker:
            self._maintenance_worker.join(
                timeout=self.settings.shutdown_grace_seconds
            )
        if self._watcher_worker:
            self._watcher_worker.join(
                timeout=self.settings.shutdown_grace_seconds
            )
        return not bool(
            (self._worker and self._worker.is_alive())
            or (
                self._maintenance_worker
                and self._maintenance_worker.is_alive()
            )
            or (
                self._watcher_worker
                and self._watcher_worker.is_alive()
            )
        )

    @property
    def ready(self) -> bool:
        if self._gateway_only:
            if not self._ready.is_set() or self._stopping.is_set():
                return False
            try:
                return bool(
                    self._shared_job_store
                    and self._shared_job_store.ping()
                )
            except Exception:
                return False
        return bool(
            self._ready.is_set()
            and self._worker
            and self._worker.is_alive()
            and self._maintenance_worker
            and self._maintenance_worker.is_alive()
            and (
                not self.settings.watcher_enabled
                or bool(
                    self._watcher_worker
                    and self._watcher_worker.is_alive()
                )
            )
            and self._integrity_status.get("status") != "failed"
            and (
                self._writer_lease is None or self._writer_lease.healthy
            )
            and not self._stopping.is_set()
        )

    def _shared_work_queue(self) -> bool:
        return bool(
            self.settings.shared_work_queue_enabled
            and self._shared_job_store is not None
        )

    def _scan_queue_depth(self) -> int:
        if not self._shared_work_queue():
            return self.work.qsize()
        try:
            return self._shared_job_store.scan_queue_depth()
        except Exception:
            LOGGER.exception("Could not read shared scan queue depth")
            return -1

    def _enqueue_scan_job(self, job_id: str) -> bool:
        if not self._shared_work_queue():
            try:
                self.work.put_nowait(job_id)
                return True
            except queue.Full:
                return False
        return self._shared_job_store.enqueue_scan(
            job_id, self.settings.queue_size
        )

    def _requeue_scan_job(self, job_id: str) -> bool:
        if not self._shared_work_queue():
            return self._enqueue_scan_job(job_id)
        return self._shared_job_store.requeue_scan(
            job_id, self.settings.queue_size
        )

    def _claim_scan_job(self) -> tuple[str | None, bool]:
        """Return (job id, requires shared acknowledgement)."""
        if not self._shared_work_queue():
            return self.work.get(), False
        return self._shared_job_store.claim_scan(timeout_seconds=1), True

    def _acknowledge_scan_job(self, job_id: str, shared: bool) -> None:
        if shared:
            self._shared_job_store.acknowledge_scan(job_id)
        else:
            self.work.task_done()

    def _publish_shared_job(self, job: Job) -> None:
        """Publish a detached job snapshot without making Redis authoritative
        for the currently executing worker.

        A transient store failure must not discard an analysis that has already
        started. New cross-replica submissions use strict shared-store calls,
        while progress publication is best-effort and observable in logs.
        """
        if self._shared_job_store is None:
            return
        with self._lock:
            snapshot = json.loads(json.dumps(job.public(), default=str))
        try:
            self._shared_job_store.put_job(
                job.id,
                snapshot,
                max(900, self.settings.result_ttl_seconds),
            )
        except Exception:
            LOGGER.exception(
                "Could not publish shared job state",
                extra={"job_id": job.id},
            )

    def get_public(self, job_id: str) -> dict[str, Any] | None:
        """Return a local job or its cross-replica shared snapshot."""
        # Gateways never execute jobs, so any local copy can only be the
        # submission-time ``queued`` snapshot.  Reading it before Redis lets
        # that stale copy mask the writer's later running/completed state and
        # makes polling nondeterministic across gateway replicas.
        if not self._gateway_only:
            local = self.get(job_id)
            if local is not None:
                return local.public()
        if self._shared_job_store is None:
            return None
        try:
            return self._shared_job_store.get_job(job_id)
        except Exception as exc:
            LOGGER.exception(
                "Could not read shared job state",
                extra={"job_id": job_id},
            )
            raise SharedStoreUnavailableError(
                "shared scan state is temporarily unavailable"
            ) from exc

    def submit(
        self,
        address: str,
        network: str = "robinhood",
        *,
        force_refresh: bool = False,
    ) -> JobAccepted:
        if self._integrity_status.get("status") == "failed":
            raise IntegrityUnavailableError(
                "Timechain integrity audit failed; analysis is paused"
            )
        normalized_address = (
            address.lower() if network in EVM_NETWORKS else address
        )
        normalized = f"{network}:{normalized_address}"
        now = time.time()
        if not force_refresh and self._shared_job_store is not None:
            try:
                shared_cached_id = self._shared_job_store.get_cache(normalized)
                shared_cached = (
                    self._shared_job_store.get_job(shared_cached_id)
                    if shared_cached_id
                    else None
                )
            except Exception as exc:
                raise SharedStoreUnavailableError(
                    "shared scan state is temporarily unavailable"
                ) from exc
            if shared_cached and shared_cached.get("status") == "succeeded":
                return JobAccepted(
                    job_id=str(shared_cached_id),
                    status="succeeded",
                    cached=True,
                )
        # Keep the common hot-repeat path entirely in memory. Durable storage
        # is only consulted for a stale/forced refresh or after a restart.
        if not force_refresh and not self._gateway_only:
            with self._lock:
                self._prune(now)
                cached = self.cache.get(normalized)
                if cached and cached[0] > now:
                    cached_job = self.jobs.get(cached[1])
                    if cached_job is not None:
                        return JobAccepted(
                            job_id=cached_job.id,
                            status=cached_job.status,
                            cached=True,
                        )
                    self.cache.pop(normalized, None)
        previous = None
        if self._shared_job_store is not None:
            try:
                shared_previous = self._shared_job_store.get_latest_result(
                    normalized
                )
            except Exception as exc:
                raise SharedStoreUnavailableError(
                    "shared scan state is temporarily unavailable"
                ) from exc
            if shared_previous is not None:
                stored_at = float(shared_previous.get("stored_at") or 0)
                age_seconds = max(0.0, now - stored_at)
                if age_seconds <= self.settings.stale_result_ttl_seconds:
                    previous = {
                        "result": shared_previous.get("result"),
                        "age_seconds": age_seconds,
                    }
        if previous is None and self._deferred_queue is not None:
            previous = self._deferred_queue.get_public_result(
                network,
                normalized_address,
                max_age_seconds=self.settings.stale_result_ttl_seconds,
                now=now,
            )

        def accepted_with_previous(job: Job) -> JobAccepted:
            return JobAccepted(
                job_id=job.id,
                status=job.status,
                cached=False,
                refreshing=True,
                previous_result=(previous or {}).get("result"),
                previous_result_age_seconds=(previous or {}).get("age_seconds"),
            )

        with self._lock:
            self._prune(now)
            cached = self.cache.get(normalized)
            if (
                not self._gateway_only
                and not force_refresh
                and cached
                and cached[0] > now
            ):
                cached_job = self.jobs.get(cached[1])
                if cached_job is not None:
                    # Serve the existing completed job rather than minting a
                    # new Job entry per cache hit: cache_ttl_seconds is always
                    # <= result_ttl_seconds by convention, so the job this
                    # entry points to is still present whenever the cache
                    # entry itself hasn't expired. Repeat lookups of a hot
                    # address used to grow self.jobs by one entry each time.
                    return JobAccepted(
                        job_id=cached_job.id,
                        status=cached_job.status,
                        cached=True,
                    )
                # The pointer outlived its job (e.g. mismatched TTL
                # configuration) -- fall through and treat this as a miss.
                self.cache.pop(normalized, None)

            active_id = (
                None
                if self._gateway_only
                else self.active_by_address.get(normalized)
            )
            if active_id:
                active = self.jobs.get(active_id)
                if active and active.status in {
                    "queued", "running", "waiting_for_timechain"
                }:
                    return accepted_with_previous(active)

            if not self._shared_work_queue() and self.work.full():
                raise QueueFullError

            proposed_job_id = uuid.uuid4().hex
            if self._shared_job_store is not None:
                try:
                    lease_owner = self._shared_job_store.claim_active(
                        normalized,
                        proposed_job_id,
                        max(900, self.settings.result_ttl_seconds),
                    )
                    if lease_owner != proposed_job_id:
                        shared_active = self._shared_job_store.get_job(
                            lease_owner
                        )
                        return JobAccepted(
                            job_id=lease_owner,
                            status=str(
                                (shared_active or {}).get("status") or "queued"
                            ),
                            cached=False,
                            refreshing=previous is not None,
                            previous_result=(previous or {}).get("result"),
                            previous_result_age_seconds=(
                                (previous or {}).get("age_seconds")
                            ),
                        )
                except Exception as exc:
                    raise SharedStoreUnavailableError(
                        "shared scan state is temporarily unavailable"
                    ) from exc
            job = Job(
                id=proposed_job_id,
                address=address,
                network=network,
            )
            if self._shared_job_store is not None:
                try:
                    self._shared_job_store.put_job(
                        job.id,
                        job.public(),
                        max(900, self.settings.result_ttl_seconds),
                    )
                except Exception as exc:
                    try:
                        self._shared_job_store.release_active(
                            normalized, job.id
                        )
                    except Exception:
                        LOGGER.exception(
                            "Could not roll back shared active-scan lease",
                            extra={"job_id": job.id},
                        )
                    raise SharedStoreUnavailableError(
                        "shared scan state is temporarily unavailable"
                    ) from exc
            # Only the writer/combined role owns mutable in-process job state.
            # A gateway relies on the Redis active lease for coalescing and on
            # the shared job snapshot for polling.
            if not self._gateway_only:
                self.jobs[job.id] = job
                self.active_by_address[normalized] = job.id
            try:
                enqueued = self._enqueue_scan_job(job.id)
            except Exception as exc:
                self.jobs.pop(job.id, None)
                self.active_by_address.pop(normalized, None)
                if self._shared_job_store is not None:
                    try:
                        self._shared_job_store.release_active(
                            normalized, job.id
                        )
                    except Exception:
                        LOGGER.exception(
                            "Could not roll back shared scan lease after "
                            "queue failure",
                            extra={"job_id": job.id},
                        )
                raise SharedStoreUnavailableError(
                    "shared scan queue is temporarily unavailable"
                ) from exc
            if not enqueued:
                self.jobs.pop(job.id, None)
                self.active_by_address.pop(normalized, None)
                if self._shared_job_store is not None:
                    try:
                        self._shared_job_store.release_active(
                            normalized, job.id
                        )
                    except Exception:
                        LOGGER.exception(
                            "Could not roll back shared scan lease after "
                            "queue rejection",
                            extra={"job_id": job.id},
                        )
                raise QueueFullError
            accepted = (
                accepted_with_previous(job)
                if previous is not None
                else JobAccepted(job_id=job.id, status=job.status)
            )
        self._publish_shared_job(job)
        return accepted

    def _persist_public_result(self, job: Job) -> None:
        """Store a detached public snapshot without extending job lifetime."""
        with self._lock:
            if job.result is None:
                return
            subject = (
                job.address.lower() if job.network in EVM_NETWORKS else job.address
            )
            network = job.network
            snapshot = json.loads(json.dumps(job.result, default=str))
        try:
            self._deferred_queue.put_public_result(network, subject, snapshot)
        except Exception:
            LOGGER.exception(
                "Could not persist latest public result",
                extra={"job_id": job.id, "network": network},
            )
        if self._shared_job_store is not None:
            try:
                self._shared_job_store.put_latest_result(
                    f"{network}:{subject}",
                    {"result": snapshot, "stored_at": time.time()},
                    self.settings.stale_result_ttl_seconds,
                )
                if self.settings.cache_ttl_seconds:
                    self._shared_job_store.put_cache(
                        f"{network}:{subject}",
                        job.id,
                        self.settings.cache_ttl_seconds,
                    )
            except Exception:
                LOGGER.exception(
                    "Could not publish shared scan cache",
                    extra={"job_id": job.id, "network": network},
                )

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            self._prune(time.time())
            return self.jobs.get(job_id)

    def watch_status(
        self,
        subscriber: str | None = None,
    ) -> dict[str, Any]:
        # Watch stores use atomic file replacement, so a read observes either
        # the previous complete cycle or the next complete cycle. It does not
        # need the cycle/mutation lock. Holding request threads on that lock
        # while run_once() performs minutes of RPC work can exhaust FastAPI's
        # shared thread pool and make analyses and health checks unreachable.
        robinhood_state = (
            self._watcher.store.load()
            if self._watcher is not None
            else {"subscriptions": {}}
        )
        base_state = (
            self._base_watcher.store.load()
            if self._base_watcher is not None
            else {"subscriptions": {}}
        )
        solana_state = (
            self._solana_watcher.store.load()
            if self._solana_watcher is not None
            else {"subscriptions": {}}
        )
        subscriptions = []
        for value in robinhood_state.get("subscriptions", {}).values():
            if subscriber and subscriber not in (
                value.get("subscribers") or []
            ):
                continue
            subscriptions.append(
                self._watcher.store.public_subscription(value)
            )
        for value in solana_state.get("subscriptions", {}).values():
            if subscriber and subscriber not in (
                value.get("subscribers") or []
            ):
                continue
            subscriptions.append(
                self._solana_watcher.store.public_subscription(value)
            )
        for value in base_state.get("subscriptions", {}).values():
            if subscriber and subscriber not in (
                value.get("subscribers") or []
            ):
                continue
            subscriptions.append(
                self._base_watcher.store.public_subscription(value)
            )
        network_counts = {
            network: sum(
                item.get("network") == network
                for item in subscriptions
            )
            for network in ("robinhood", "base", "solana")
        }
        return {
            **self._watcher_status,
            "subscriptions": subscriptions,
            "subscription_counts": network_counts,
        }

    def benchmark_status(self) -> dict[str, Any]:
        return self._benchmark.status()

    def memory_query(
        self,
        network: str,
        address: str,
        *,
        topics: list[str],
        limit: int,
    ) -> dict[str, Any]:
        if self._memory is None:
            raise RuntimeError("Timechain Memory Core is not initialized")
        return self._memory.query(
            network, address, topics=topics, limit=limit
        )

    def memory_status(self) -> dict[str, Any]:
        if self._memory is None:
            raise RuntimeError("Timechain Memory Core is not initialized")
        if self.settings.environment != "production":
            return self._memory.status()
        with self._memory_status_snapshot_lock:
            snapshot = self._memory_status_snapshot
        if snapshot is None:
            self._start_memory_status_refresh()
            raise RuntimeError("Timechain Memory Core status is warming")
        captured_at, value = snapshot
        age_seconds = max(0.0, time.time() - captured_at)
        refreshing = bool(
            self._memory_status_worker
            and self._memory_status_worker.is_alive()
        )
        if age_seconds >= 300 and not refreshing:
            self._start_memory_status_refresh()
            refreshing = True
        result = json.loads(json.dumps(value))
        result["delivery"] = {
            "mode": "background_verified_snapshot",
            "snapshot_age_seconds": round(age_seconds, 1),
            "refreshing": refreshing,
            "may_trail_new_rings": True,
        }
        return result

    def _start_memory_status_refresh(self) -> None:
        """Start at most one expensive Memory status rebuild off-request."""
        with self._memory_status_refresh_lock:
            if (
                self._memory_status_worker
                and self._memory_status_worker.is_alive()
            ):
                return
            self._memory_status_worker = threading.Thread(
                target=self._refresh_memory_status_snapshot,
                name="chainseer-memory-status-refresh",
                daemon=True,
            )
            self._memory_status_worker.start()

    def _refresh_memory_status_snapshot(self) -> None:
        memory = self._memory
        if memory is None or self._stopping.is_set():
            return
        try:
            value = memory.status(cache_seconds=0)
        except Exception:
            LOGGER.exception("Background Memory Core status refresh failed")
            return
        with self._memory_status_snapshot_lock:
            self._memory_status_snapshot = (
                time.time(),
                json.loads(json.dumps(value)),
            )

    def memory_citation(self, ring_index: int) -> dict[str, Any]:
        if self._memory is None:
            raise RuntimeError("Timechain Memory Core is not initialized")
        return self._memory.citation(ring_index)

    def import_base_analysis_rings(
        self, items: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Backfill/forward-sync base_launch_analysis rings sealed elsewhere
        (locally-run chainseer_base.py) onto this process's Timechain.

        The imported observation time is retained inside the payload. The new
        ring keeps its real append time: backdating a ledger ring makes chain
        chronology describe the source event rather than the import that
        actually happened, and previously required a private Timechain patch
        that made production differ from CI.

        Runs under self._timechain_lock so the idempotency-key scan and the
        seal it guards stay atomic against any other seal this process makes
        concurrently; Timechain.seal()'s own file lock already keeps the
        chain itself consistent, this lock just keeps the dedup check honest.

        Suppresses Timechain's per-seal hippocampus reindex (CT_AUTOINDEX)
        for the duration of the batch -- its own code comment names bulk
        imports as exactly the case to disable it for, since a full-text
        reindex on every large historical payload is what actually times
        out a batch import. The next organic seal (the watcher runs every
        60s in production) brings the index back to head incrementally via
        its own staleness check, so nothing needs to be reindexed here.

        The idempotency-key set is scanned from the full chain once per
        process lifetime and cached on self, not rescanned every call --
        at production scale (thousands of unrecognized-type rings this
        endpoint must still stream past to find its own) a fresh full scan
        per request was itself slow enough to time out a small batch.
        """
        if self._agent is None or not hasattr(self._agent, "tc"):
            raise RuntimeError("Timechain is not initialized")
        tc = self._agent.tc
        results: list[dict[str, Any]] = []
        previous_autoindex = os.environ.get("CT_AUTOINDEX")
        with self._tracked_timechain_lock("base_analysis_import"):
            os.environ["CT_AUTOINDEX"] = "0"
            try:
                if self._base_analysis_idempotency_keys is None:
                    self._base_analysis_idempotency_keys = {
                        (ring.get("payload") or {}).get("idempotency_key")
                        for ring in tc.iter_rings()
                        if ring.get("ring_type") == "base_launch_analysis"
                        and (ring.get("payload") or {}).get("idempotency_key")
                    }
                seen_keys = self._base_analysis_idempotency_keys
                for item in items:
                    payload = dict(item["payload"])
                    payload.setdefault("timestamp", item["timestamp"])
                    payload.setdefault("source_timestamp", item["timestamp"])
                    key = payload.get("idempotency_key")
                    if key and key in seen_keys:
                        results.append({"status": "duplicate", "idempotency_key": key})
                        continue
                    try:
                        ring = tc.seal("base_launch_analysis", payload)
                    except Exception as exc:  # noqa: BLE001 - report per-item, never abort the batch
                        results.append({"status": "error", "detail": str(exc)})
                        continue
                    if key:
                        seen_keys.add(key)
                    results.append({"status": "sealed", "index": ring["index"]})
            finally:
                if previous_autoindex is None:
                    os.environ.pop("CT_AUTOINDEX", None)
                else:
                    os.environ["CT_AUTOINDEX"] = previous_autoindex
        return results

    def health_status(self) -> dict[str, Any]:
        """Return cached worker health without loading watcher state from disk."""
        watcher_status = self._watcher_status
        owner, owner_since, owner_reason = self._timechain_owner_snapshot()
        with self._lock:
            running = next(
                (
                    job
                    for job in self.jobs.values()
                    if job.status in {"running", "waiting_for_timechain"}
                ),
                None,
            )
            latencies = list(self._recent_analysis_latencies_ms)
        now = time.time()
        return {
            "watcher_last_error": watcher_status.get("last_error"),
            "watcher_last_deferred": watcher_status.get("last_deferred"),
            "benchmark_capture": self._benchmark.health_status(),
            "timechain_integrity": dict(self._integrity_status),
            "cypher_tempre_runtime": dict(self._cypher_tempre_runtime),
            "maintenance_queue_depth": self._maintenance_work.qsize(),
            "shared_job_store": {
                "enabled": self._shared_job_store is not None,
                "backend": (
                    self._shared_job_store.backend
                    if self._shared_job_store is not None
                    else "process_local"
                ),
                "work_queue": (
                    "shared_reliable"
                    if self._shared_work_queue()
                    else "process_local"
                ),
                "queue_depth": self._scan_queue_depth(),
            },
            "timechain_writer": {
                "distributed_lease": bool(
                    self._writer_lease and self._writer_lease.enabled
                ),
                "lease_healthy": bool(
                    self._writer_lease is None
                    or self._writer_lease.healthy
                ),
            },
            "deferred_commits": (
                self._deferred_queue.counts()
                if self._deferred_queue is not None
                else {"status": "delegated"}
            ),
            "faculty_pack": (
                dict(getattr(self._agent, "faculty_pack_status", {}) or {})
                if self._agent is not None
                else {"status": "not_initialized"}
            ),
            "memory": {
                "rss_mb": self._last_memory_rss_mb,
                "peak_rss_mb": self._last_memory_peak_mb,
                "warning_threshold_mb": self.settings.memory_warning_mb,
                "warning": self._memory_warning_active,
            },
            "runtime": {
                "process_role": self.settings.process_role,
                "process_instance_id": self._process_instance_id,
                "started_at": _iso(self._process_started_at),
                "uptime_seconds": round(
                    max(0.0, now - self._process_started_at), 1
                ),
                "cpu_percent": self._last_cpu_percent,
                "cgroup_cpu": dict(self._cgroup_cpu),
                "workers": {
                    "analysis": bool(self._worker and self._worker.is_alive()),
                    "maintenance": bool(
                        self._maintenance_worker
                        and self._maintenance_worker.is_alive()
                    ),
                    "watcher": bool(
                        self._watcher_worker
                        and self._watcher_worker.is_alive()
                    ),
                },
                "active_analysis": (
                    {
                        "network": running.network,
                        "stage": running.stage,
                        "age_seconds": round(
                            max(0.0, now - (running.started_at or now)), 1
                        ),
                    }
                    if running is not None
                    else None
                ),
                "latest_analysis": dict(self._latest_analysis_summary or {}),
                "analysis_latency_ms": {
                    "sample_size": len(latencies),
                    "p50": (
                        round(statistics.median(latencies), 1)
                        if latencies
                        else None
                    ),
                    "p95": (
                        round(
                            sorted(latencies)[
                                max(0, int(len(latencies) * 0.95) - 1)
                            ],
                            1,
                        )
                        if latencies
                        else None
                    ),
                },
                "timechain_lane": {
                    "owner": owner.name if owner else None,
                    "reason": owner_reason or None,
                    "held_seconds": (
                        round(
                            max(0.0, time.monotonic() - owner_since), 1
                        )
                        if owner is not None and owner_since > 0
                        else 0.0
                    ),
                },
            },
            "maintenance_telemetry": dict(self._maintenance_telemetry),
        }

    @contextmanager
    def _watch_mutation(self):
        acquired = self._watch_lock.acquire(
            timeout=WATCH_MUTATION_LOCK_TIMEOUT_SECONDS
        )
        if not acquired:
            raise WatcherBusyError
        try:
            yield
        finally:
            self._watch_lock.release()

    def watch_subscribe(
        self,
        address: str,
        network: str = "robinhood",
        subscriber: str | None = None,
    ) -> dict[str, Any]:
        with self._watch_mutation():
            if network == "solana":
                if self._solana_watcher is None:
                    raise RuntimeError("Solana watcher is not initialized")
                value = self._solana_watcher.store.subscribe(
                    address, subscriber
                )
                return self._solana_watcher.store.public_subscription(value)
            if network == "base":
                if self._base_watcher is None:
                    raise RuntimeError("Base watcher is not initialized")
                value = self._base_watcher.store.subscribe(
                    address, subscriber
                )
                return self._base_watcher.store.public_subscription(value)
            if self._watcher is None:
                raise RuntimeError("watcher is not initialized")
            value = self._watcher.store.subscribe(address, subscriber)
            return self._watcher.store.public_subscription(value)

    def watch_unsubscribe(
        self,
        address: str,
        network: str = "robinhood",
        subscriber: str | None = None,
    ) -> bool:
        with self._watch_mutation():
            if network == "solana":
                if self._solana_watcher is None:
                    raise RuntimeError("Solana watcher is not initialized")
                return self._solana_watcher.store.unsubscribe(
                    address, subscriber
                )
            if network == "base":
                if self._base_watcher is None:
                    raise RuntimeError("Base watcher is not initialized")
                return self._base_watcher.store.unsubscribe(
                    address, subscriber
                )
            if self._watcher is None:
                raise RuntimeError("watcher is not initialized")
            return self._watcher.store.unsubscribe(address, subscriber)

    def watch_alerts(
        self,
        address: str,
        network: str,
        subscriber: str,
        *,
        after: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        # Atomic state/append-only alert files make these reads safe without
        # waiting for the long-running watcher cycle lock.
        watcher = {
            "robinhood": self._watcher,
            "base": self._base_watcher,
            "solana": self._solana_watcher,
        }.get(network)
        if watcher is None:
            raise RuntimeError(f"{network} watcher is not initialized")
        if not watcher.store.is_subscribed(address, subscriber):
            raise KeyError("watch subscription not found")
        return watcher.store.read_alerts(
            address,
            after=after,
            limit=limit,
            critical_only=True,
        )

    def _run_watcher_cycle(self) -> None:
        if not self.settings.watcher_enabled:
            return
        previous = self._watcher_status.get("last_cycle") or {}
        summaries: dict[str, Any] = {
            network: previous.get(network)
            for network in ("robinhood", "base", "solana")
        }
        errors: dict[str, Any] = {}
        deferred: dict[str, Any] = {}
        for network, watcher in (
            ("robinhood", self._watcher),
            ("base", self._base_watcher),
            ("solana", self._solana_watcher),
        ):
            observed_at = datetime.now(timezone.utc).isoformat()
            if watcher is None:
                errors[network] = {
                    "at": observed_at,
                    "message": f"{network} watcher is not initialized",
                }
                continue
            if self._analysis_active.is_set() or not self.work.empty():
                deferred[network] = {
                    "at": observed_at,
                    "reason": "analysis_priority",
                }
                continue
            method = watcher.run_once
            try:
                supports_scoped_lane = (
                    "timechain_lane" in inspect.signature(method).parameters
                )
            except (TypeError, ValueError):
                supports_scoped_lane = False
            acquired_timechain = False
            if not supports_scoped_lane:
                acquired_timechain = self._acquire_timechain(
                    f"legacy_{network}_watcher_cycle", blocking=False
                )
                if not acquired_timechain:
                    deferred[network] = {
                        "at": observed_at,
                        "reason": "timechain_busy",
                    }
                    continue
            acquired_watch = False
            try:
                acquired_watch = self._watch_lock.acquire(blocking=False)
                if not acquired_watch:
                    deferred[network] = {
                        "at": observed_at,
                        "reason": "watch_state_busy",
                    }
                    continue
                kwargs: dict[str, Any] = {}
                try:
                    if "should_yield" in inspect.signature(method).parameters:
                        kwargs["should_yield"] = lambda: (
                            self._analysis_active.is_set()
                            or not self.work.empty()
                        )
                    if (
                        "include_calibration"
                        in inspect.signature(method).parameters
                    ):
                        # Calibration scans the complete Timechain and is not
                        # latency-sensitive.  It stays available through the
                        # explicit calibration workflow; a watcher sweep must
                        # never do it while holding the user-analysis lane.
                        kwargs["include_calibration"] = False
                    if (
                        "timechain_lane"
                        in inspect.signature(method).parameters
                    ):
                        # Read-only watcher RPC preflight runs outside this
                        # lane. Only analyzer/sealing mutations serialize with
                        # user work, and those sections cooperatively yield.
                        kwargs["timechain_lane"] = (
                            self._watcher_timechain_lane
                        )
                except (TypeError, ValueError):
                    pass
                summaries[network] = method(**kwargs)
            except Exception as exc:
                LOGGER.exception("%s watcher cycle failed", network)
                errors[network] = {
                    "at": observed_at,
                    "message": credential_safe_error(exc),
                }
            finally:
                if acquired_watch:
                    self._watch_lock.release()
                if acquired_timechain:
                    self._release_timechain()
        # Persist only envelopes produced by this cycle.  Never retain the full
        # snapshots in public watcher status or replay a previous cycle's work.
        for network in ("robinhood", "base", "solana"):
            net_summary = summaries.get(network)
            if not isinstance(net_summary, dict):
                continue
            pending_seals = list(net_summary.pop("pending_seals", []) or [])
            for env in pending_seals:
                try:
                    token = str(env["token_address"])
                    anchor = env.get("block_or_slot")
                    payload = {
                        **env,
                        "anchor_kind": (
                            "confirmed_slot"
                            if network == "solana"
                            else "confirmed_block"
                        ),
                        "anchor_value": anchor,
                        "observed_at_epoch": env.get(
                            "enqueued_at", time.time()
                        ),
                    }
                    self._deferred_queue.enqueue(
                        kind="watcher_commit",
                        subject_key=f"{network}:{token}",
                        payload=payload,
                        priority=20,
                    )
                except Exception:
                    LOGGER.exception(
                        "failed to persist deferred watcher commit for %s",
                        env.get("token_address"),
                    )
        self._watcher_status = {
            "enabled": True,
            "last_cycle": summaries,
            "last_error": errors or None,
            "last_deferred": deferred or None,
        }

    def _acquire_timechain(
        self,
        reason: str,
        *,
        blocking: bool = True,
        timeout: float | None = None,
    ) -> bool:
        """Acquire the writer lock and atomically publish its real owner."""
        if self._writer_lease is not None and not self._writer_lease.healthy:
            return False
        if not blocking:
            acquired = self._timechain_lock.acquire(blocking=False)
        elif timeout is None:
            acquired = self._timechain_lock.acquire()
        else:
            acquired = self._timechain_lock.acquire(timeout=timeout)
        if not acquired:
            return False
        if self._writer_lease is not None and not self._writer_lease.healthy:
            self._timechain_lock.release()
            return False
        me = threading.current_thread()
        with self._timechain_owner_guard:
            if self._timechain_owner is me:
                self._timechain_owner_depth += 1
            else:
                self._timechain_owner = me
                self._timechain_owner_since = time.monotonic()
                self._timechain_owner_reason = reason
                self._timechain_owner_depth = 1
        return True

    def _release_timechain(self) -> None:
        """Release one writer-lock level without erasing a newer owner."""
        me = threading.current_thread()
        self._timechain_lock.release()
        with self._timechain_owner_guard:
            if self._timechain_owner is not me:
                return
            self._timechain_owner_depth -= 1
            if self._timechain_owner_depth <= 0:
                self._timechain_owner = None
                self._timechain_owner_since = 0.0
                self._timechain_owner_reason = ""
                self._timechain_owner_depth = 0

    def _timechain_owner_snapshot(
        self,
    ) -> tuple[threading.Thread | None, float, str]:
        with self._timechain_owner_guard:
            return (
                self._timechain_owner,
                self._timechain_owner_since,
                self._timechain_owner_reason,
            )

    @contextmanager
    def _try_timechain_lane(
        self,
        reason: str,
        *,
        blocking: bool = False,
        timeout: float | None = None,
    ):
        """Yield whether a consistently tracked writer-lane acquire succeeded."""
        acquired = self._acquire_timechain(
            reason, blocking=blocking, timeout=timeout
        )
        try:
            yield acquired
        finally:
            if acquired:
                self._release_timechain()

    @contextmanager
    def _tracked_timechain_lock(self, reason: str):
        """Acquire ``_timechain_lock`` with a configurable timeout.

        Sets lock-owner tracking fields for diagnostics on timeout.
        Raises ``TimeoutError`` with full context if the lock cannot be
        acquired within ``timechain_lock_timeout_seconds``.
        """
        timeout = self.settings.timechain_lock_timeout_seconds
        acquired = self._acquire_timechain(reason, timeout=timeout)
        if not acquired:
            owner, since, owner_reason = self._timechain_owner_snapshot()
            held = (
                f"{max(0.0, time.monotonic() - since):.1f}s"
                if owner is not None and since > 0
                else "unknown"
            )
            raise TimeoutError(
                f"Timed out waiting for _timechain_lock ({timeout:.1f}s) "
                f"for '{reason}'. "
                f"Owner: {owner.name if owner else 'None'} "
                f"reason='{owner_reason}' "
                f"held={held}"
            )
        try:
            yield
        finally:
            self._release_timechain()

    @contextmanager
    def _watcher_timechain_lane(self):
        """Serialize watcher writes without doing index maintenance inline."""
        timeout = self.settings.timechain_lock_timeout_seconds
        acquired = self._acquire_timechain(
            "watcher_timechain_lane", timeout=timeout
        )
        if not acquired:
            raise TimeoutError(
                f"Watcher timechain_lane timed out ({timeout:.1f}s); "
                f"analysis worker likely holding lock"
            )
        previous = os.environ.get("CT_AUTOINDEX")
        os.environ["CT_AUTOINDEX"] = "0"
        try:
            yield
        finally:
            if previous is None:
                os.environ.pop("CT_AUTOINDEX", None)
            else:
                os.environ["CT_AUTOINDEX"] = previous
            self._release_timechain()

    def _run_watchers(self) -> None:
        interval = max(0.1, float(self.settings.watcher_interval_seconds))
        while not self._stopping.wait(interval):
            self._run_watcher_cycle()

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

    def _set_job_progress(
        self,
        job_id: str,
        stage: str,
        percent: int,
        detail: str,
    ) -> None:
        with self._lock:
            job = self.jobs.get(job_id)
            if job is None or job.status in {"succeeded", "failed"}:
                return
            now = time.time()
            next_stage = str(stage)[:80]
            if job.stage_started_at is not None and job.stage != next_stage:
                elapsed_ms = max(0.0, (now - job.stage_started_at) * 1000)
                job.stage_timings_ms[job.stage] = round(
                    job.stage_timings_ms.get(job.stage, 0.0) + elapsed_ms,
                    1,
                )
                job.stage_started_at = now
            elif job.stage_started_at is None:
                job.stage_started_at = now
            job.stage = next_stage
            job.stage_detail = str(detail)[:240]
            job.progress_percent = max(0, min(100, int(percent)))
            job.updated_at = now
        self._publish_shared_job(job)

    @staticmethod
    def _read_current_rss_mb() -> float | None:
        if resource is None:
            return None
        # Prefer current Linux RSS so pressure can clear after objects are
        # released; ru_maxrss remains useful as the fallback/high-water mark.
        rss_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
        try:
            with Path("/proc/self/status").open(encoding="utf-8") as handle:
                for line in handle:
                    if line.startswith("VmRSS:"):
                        rss_mb = float(line.split()[1]) / 1024
                        break
        except (OSError, ValueError, IndexError):
            pass
        return round(rss_mb, 1)

    def _sample_runtime_telemetry(self) -> None:
        now = time.monotonic()
        process_cpu = time.process_time()
        previous = self._last_cpu_sample
        if previous is not None and now > previous[0]:
            self._last_cpu_percent = round(
                max(
                    0.0,
                    (process_cpu - previous[1])
                    / (now - previous[0])
                    * 100,
                ),
                1,
            )
        self._last_cpu_sample = (now, process_cpu)
        try:
            values: dict[str, int] = {}
            for line in Path("/sys/fs/cgroup/cpu.stat").read_text(
                encoding="utf-8"
            ).splitlines():
                key, value = line.split(maxsplit=1)
                values[key] = int(value)
            self._cgroup_cpu = {
                key: values[key]
                for key in (
                    "usage_usec",
                    "nr_periods",
                    "nr_throttled",
                    "throttled_usec",
                )
                if key in values
            }
        except (OSError, ValueError):
            pass

    def _enqueue_maintenance(self, job: Job, report: dict[str, Any]) -> None:
        task = MaintenanceTask(
            job_id=job.id,
            network=job.network,
            address=job.address,
            report=report,
            public_report=job.result or {},
        )
        try:
            self._maintenance_work.put_nowait(task)
        except queue.Full:
            with self._lock:
                job.benchmark_capture = {
                    "status": "deferred_queue_full"
                }

    def _set_cognitive_progress(
        self,
        job_id: str,
        status: str,
        percent: int,
        detail: str,
    ) -> None:
        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return
            job.cognition_status = str(status)[:40]
            job.cognition_stage_detail = str(detail)[:240]
            job.cognition_progress_percent = max(0, min(100, int(percent)))
            job.cognition_updated_at = time.time()
            job.updated_at = job.cognition_updated_at
        self._publish_shared_job(job)

    def _enqueue_cognitive_completion(
        self,
        job: Job,
        report: dict[str, Any],
    ) -> None:
        """Persist the bounded post-publication cognitive commit."""
        if not self.settings.cognitive_completion_enabled:
            job.cognition_status = "isolated"
            job.cognition_stage_detail = (
                "Deep learning is delegated to the isolated learner; "
                "the risk result is already sealed"
            )
            job.cognition_progress_percent = 100
            job.cognition_updated_at = time.time()
            return
        ring_index = report.get("analysis_ring")
        ring_hash = report.get("analysis_ring_hash")
        cognition = report.get("cognition") or {}
        if ring_index is None or not ring_hash or cognition.get("status") != "pending":
            job.cognition_status = "complete"
            job.cognition_stage_detail = "Cognitive audit completed inline"
            job.cognition_progress_percent = 100
            job.cognition_updated_at = time.time()
            return
        completion_report = {
            "token_address": report.get("token_address"),
            "chain_id": report.get("chain_id"),
            "cognition": json.loads(json.dumps(cognition, default=str)),
            "_cognitive_input": str(report.get("_cognitive_input") or ""),
        }
        canonical = json.dumps(
            completion_report, sort_keys=True, separators=(",", ":")
        )
        self._deferred_queue.enqueue(
            kind="cognitive_completion",
            subject_key=f"{job.network}:{job.address}:{ring_index}",
            priority=5,
            payload={
                "job_id": job.id,
                "network": job.network,
                "token_address": job.address,
                "analysis_ring": int(ring_index),
                "analysis_ring_hash": str(ring_hash),
                "anchor_value": int(ring_index),
                "observed_at_epoch": time.time(),
                "report_hash": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
                "completion_report": completion_report,
            },
        )
        job.cognition_status = "queued"
        job.cognition_stage_detail = "Risk result ready; cognitive audit queued"
        job.cognition_progress_percent = 0
        job.cognition_updated_at = time.time()

    def _check_memory_usage(self) -> None:
        """Log an edge-triggered warning when RSS crosses the configured
        threshold. A ring-import batch or a /v1/memory/status rebuild has
        already OOM-killed this machine once (materializing the whole chain
        into memory) -- this is the early signal so that's caught before
        the kernel does it for us, not silent until the crash.
        """
        self._sample_runtime_telemetry()
        rss_mb = self._read_current_rss_mb()
        if rss_mb is None:
            return
        self._last_memory_rss_mb = rss_mb
        self._last_memory_peak_mb = max(
            rss_mb, self._last_memory_peak_mb or 0.0
        )
        over_threshold = rss_mb >= self.settings.memory_warning_mb
        if over_threshold and not self._memory_warning_active:
            self._memory_warning_active = True
            LOGGER.error(
                "Memory usage warning: RSS %.1fMB >= threshold %dMB "
                "(prior OOM kill threshold is the machine's total memory)",
                rss_mb,
                self.settings.memory_warning_mb,
            )
        elif (
            self._memory_warning_active
            and rss_mb < self.settings.memory_warning_mb * 0.9
        ):
            self._memory_warning_active = False
            LOGGER.info(
                "Memory pressure cleared: RSS %.1fMB < recovery threshold %.1fMB",
                rss_mb,
                self.settings.memory_warning_mb * 0.9,
            )

    def _run_full_audit(self, *, batch_rings: int = 8) -> bool:
        """Advance a deep audit by a bounded number of physical rings.

        The target head is pinned once. Each batch owns the writer lock only
        while validating a few immutable JSONL records, then releases it so a
        queued analysis can run. Appends between batches are safe because the
        Timechain is append-only; they belong to the next audit/incremental
        verification span.

        Returns True only when the pinned audit has completed (successfully or
        with a fail-closed integrity result), False when it yielded/deferred.
        """
        if self._agent is None or not hasattr(self._agent, "tc"):
            self._integrity_status = {
                "status": "unsupported",
                "last_full_audit_at": None,
                "last_full_audit_duration_seconds": None,
                "last_error": None,
            }
            return True
        if self._analysis_active.is_set() or not self.work.empty():
            if self._last_full_audit_deferred_reason != "analysis":
                self._maintenance_telemetry[
                    "full_audit_deferred_analysis"
                ] += 1
                self._last_full_audit_deferred_reason = "analysis"
            return False
        if (
            self._last_memory_rss_mb is not None
            and self._last_memory_rss_mb >= self.settings.memory_warning_mb
        ):
            if self._last_full_audit_deferred_reason != "memory":
                self._maintenance_telemetry[
                    "full_audit_deferred_memory"
                ] += 1
                self._last_full_audit_deferred_reason = "memory"
            self._integrity_status = {
                **self._integrity_status,
                "full_audit_deferred_reason": "memory_pressure",
            }
            # Treat this interval as deliberately deferred so maintenance
            # does not spin every 100ms while the process is under pressure.
            return True
        if not self._acquire_timechain("full_timechain_audit", blocking=False):
            return False
        try:
            # Close the race between the priority check and lock acquisition.
            if self._analysis_active.is_set() or not self.work.empty():
                return False
            self._last_full_audit_deferred_reason = None
            self._integrity_status.pop("full_audit_deferred_reason", None)
            tc = self._agent.tc
            cursor = self._full_audit_cursor
            if cursor is None:
                tail = tc.tail_rings(1)
                if not tail:
                    self._integrity_status = {
                        "status": "verified",
                        "last_full_audit_at": datetime.now(
                            timezone.utc
                        ).isoformat(),
                        "last_full_audit_duration_seconds": 0.0,
                        "last_error": None,
                        "full_audit_progress": None,
                    }
                    return True
                target = tail[-1]
                cursor = FullAuditCursor(
                    target_index=int(target["index"]),
                    target_hash=str(target["ring_hash"]),
                    started_monotonic=time.monotonic(),
                    started_at=datetime.now(timezone.utc).isoformat(),
                )
                self._full_audit_cursor = cursor

            processed = 0
            with tc.rings_path.open("rb") as handle:
                handle.seek(cursor.offset)
                while (
                    processed < max(1, int(batch_rings))
                    and cursor.next_index <= cursor.target_index
                ):
                    raw = handle.readline()
                    if not raw:
                        cursor.errors.append(
                            f"ring {cursor.next_index}: pinned audit target "
                            "disappeared -> TAMPERED"
                        )
                        cursor.next_index = cursor.target_index + 1
                        break
                    cursor.offset = handle.tell()
                    line = raw.strip()
                    if not line:
                        continue
                    index = cursor.next_index
                    try:
                        ring = json.loads(line.decode("utf-8"))
                    except Exception as exc:
                        cursor.errors.append(
                            f"ring {index}: unreadable/torn line -> "
                            f"TAMPERED ({exc})"
                        )
                        cursor.next_index += 1
                        processed += 1
                        continue
                    if ring.get("index") != index:
                        cursor.errors.append(
                            f"ring {index}: index mismatch "
                            f"(got {ring.get('index')})"
                        )
                    if ring.get("prev_hash") != cursor.previous_hash:
                        cursor.errors.append(
                            f"ring {index}: prev_hash broken "
                            f"(expected {cursor.previous_hash[:12]}..)"
                        )
                    timechain_module = getattr(
                        self._agent, "timechain_module", None
                    ) or getattr(
                        getattr(self._agent, "cognitive_loop", None),
                        "timechain_module",
                        None,
                    )
                    if timechain_module is None:
                        raise RuntimeError(
                            "Timechain hash implementation is unavailable"
                        )
                    recomputed = timechain_module.compute_ring_hash(ring)
                    if recomputed != ring.get("ring_hash"):
                        cursor.errors.append(
                            f"ring {index}: ring_hash mismatch -> TAMPERED"
                        )
                    difficulty = ring.get("difficulty", 0)
                    if difficulty and not str(
                        ring.get("ring_hash", "")
                    ).startswith("0" * difficulty):
                        cursor.errors.append(
                            f"ring {index}: does not meet stated difficulty "
                            f"{difficulty}"
                        )
                    for ref in ring.get("blockspace_refs", []):
                        blob_hash = ref.get("hash")
                        if not tc.blockspace.has(blob_hash):
                            cursor.errors.append(
                                f"ring {index}: blockspace blob "
                                f"{str(blob_hash)[:12]}.. missing"
                            )
                        elif not tc.blockspace.verify_blob(blob_hash):
                            cursor.errors.append(
                                f"ring {index}: blockspace blob "
                                f"{str(blob_hash)[:12]}.. corrupted"
                            )
                    cursor.previous_hash = str(ring.get("ring_hash"))
                    cursor.next_index += 1
                    cursor.verified_rings += 1
                    processed += 1
                    if self._analysis_active.is_set() or not self.work.empty():
                        break

            self._integrity_status = {
                **self._integrity_status,
                "status": "auditing",
                "last_error": None,
                "full_audit_progress": {
                    "verified_rings": cursor.verified_rings,
                    "target_rings": cursor.target_index + 1,
                    "started_at": cursor.started_at,
                },
            }
            if cursor.next_index <= cursor.target_index:
                return False
            if cursor.previous_hash != cursor.target_hash:
                cursor.errors.append(
                    "pinned audit head hash changed -> TAMPERED"
                )
            if cursor.errors:
                self._integrity_status = {
                    "status": "failed",
                    "last_full_audit_at": datetime.now(timezone.utc).isoformat(),
                    "last_full_audit_duration_seconds": round(
                        time.monotonic() - cursor.started_monotonic, 3
                    ),
                    "last_error": "; ".join(cursor.errors)[:500],
                    "full_audit_progress": None,
                }
                self._full_audit_cursor = None
                return True
            self._agent.cognitive_loop.verify_registry()
            self._integrity_status = {
                "status": "verified",
                "last_full_audit_at": datetime.now(timezone.utc).isoformat(),
                "last_full_audit_duration_seconds": round(
                    time.monotonic() - cursor.started_monotonic, 3
                ),
                "last_error": None,
                "full_audit_progress": None,
            }
            self._full_audit_cursor = None
            return True
        except Exception as exc:
            LOGGER.exception("Background Timechain audit failed")
            started = (
                self._full_audit_cursor.started_monotonic
                if self._full_audit_cursor is not None
                else time.monotonic()
            )
            self._integrity_status = {
                "status": "failed",
                "last_full_audit_at": datetime.now(timezone.utc).isoformat(),
                "last_full_audit_duration_seconds": round(
                    time.monotonic() - started, 3
                ),
                "last_error": credential_safe_error(exc),
                "full_audit_progress": None,
            }
            self._full_audit_cursor = None
            return True
        finally:
            self._release_timechain()

    def _drain_deferred_seals(self) -> None:
        """Drain and coalesce pending deferred-seal jobs, then execute.

        Coalescing keeps only the latest envelope per ``(network, token)``
        so that rapid watcher rescans for the same subject don't queue
        redundant seals.  The maintenance worker calls this once per loop
        iteration, so the effective batch window is the maintenance poll
        interval (~0.1–1 s).
        """
        # 1. Drain all pending jobs into a local list.
        pending: list[DeferredSealJob] = []
        while True:
            try:
                job = self._deferred_seal_work.get_nowait()
            except queue.Empty:
                break
            if job is not None:
                pending.append(job)
            self._deferred_seal_work.task_done()

        if not pending:
            return

        # 2. Coalesce: keep only the latest job per (network, token).
        latest: dict[tuple[str, str], DeferredSealJob] = {}
        for job in pending:
            key = (job.network, job.token_address)
            existing = latest.get(key)
            if existing is None or job.enqueued_at > existing.enqueued_at:
                latest[key] = job

        # 3. Execute each coalesced job.
        for job in latest.values():
            try:
                self._execute_deferred_seal(job)
            except Exception:
                LOGGER.exception(
                    "deferred seal failed for %s:%s",
                    job.network,
                    job.token_address,
                    extra={"idempotency_key": job.idempotency_key},
                )

    def _find_latest_token_ring(
        self,
        tc: Any,
        network: str,
        token_address: str,
        limit: int = 500,
    ) -> dict | None:
        """Scan tail rings for the most recent ring matching *network+token*.

        This is a read-only tail scan — safe to call without holding the
        writer lock, and fast (bounded by *limit* lines from the JSONL).
        """
        for ring in reversed(tc.tail_rings(limit)):
            payload = ring.get("payload") or {}
            rtype = ring.get("ring_type", "")
            # Match EVM and Solana analysis ring types
            if rtype in ("token_analysis", "solana_token_analysis"):
                p_net = payload.get("network")
                p_tok = (
                    payload.get("token_address")
                    or payload.get("mint")
                )
                if p_net == network and p_tok == token_address:
                    return ring
        return None

    def _cas_validate_deferred_seal(
        self,
        job: DeferredSealJob,
        tc: Any,
    ) -> str:
        """Atomic CAS validation inside the writer lock.

        Returns one of: ``"committed"``, ``"idempotent"``, ``"superseded"``,
        or ``"discarded"``.

        Protocol (all steps inside the writer lock):
        1. Idempotency — if a ring with matching idempotency_key already
           exists in the tail → ``"idempotent"`` (already committed).
        2. Subject-scoped freshness — find the latest ring for the same
           (network, token).  If its sealed block/slot is ≥ this job's
           pinned block/slot → ``"superseded"``.  Unrelated head advancement
           (different tokens) does NOT invalidate.
        3. Hash verification — confirm the report now bears a valid
           ``analysis_ring`` and ``analysis_ring_hash`` (set by Phase A).
        4. Success → ``"committed"``.
        """
        # 1. Idempotency check (tail-only scan)
        for ring in reversed(tc.tail_rings(200)):
            payload = ring.get("payload") or {}
            if payload.get("idempotency_key") == job.idempotency_key:
                return "idempotent"

        # 2. Subject-scoped freshness check
        latest = self._find_latest_token_ring(
            tc, job.network, job.token_address
        )
        if latest is not None:
            lp = latest.get("payload") or {}
            sealed_block = lp.get("block_pin")
            if sealed_block is not None and job.block_or_slot is not None:
                try:
                    if int(sealed_block) >= int(job.block_or_slot):
                        return "superseded"
                except (TypeError, ValueError):
                    pass

        # 3. Hash verification — Phase A should have populated these
        snapshot = job.pinned_snapshot
        if snapshot.get("analysis_ring") is None:
            return "discarded"

        # 4. Committed
        return "committed"

    def _execute_deferred_seal(self, job: DeferredSealJob) -> None:
        """Persist a legacy envelope for the durable prepare/commit worker."""
        # Compatibility adapter only.  The former implementation called
        # _seal_report() here, which appended before validation.  Persist the
        # envelope instead; the durable prepare/commit worker owns all writes.
        self._deferred_queue.enqueue(
            kind="watcher_commit",
            subject_key=f"{job.network}:{job.token_address}",
            priority=20,
            payload={
                "network": job.network,
                "token_address": job.token_address,
                "pinned_snapshot": job.pinned_snapshot,
                "block_or_slot": job.block_or_slot,
                "anchor_kind": (
                    "confirmed_slot"
                    if job.network == "solana"
                    else "confirmed_block"
                ),
                "anchor_value": job.block_or_slot,
                "evidence_hash": job.evidence_hash,
                "report_hash": job.report_hash,
                "analyzer_version": job.analyzer_version,
                "idempotency_key": job.idempotency_key,
                "enqueued_at": job.enqueued_at,
                "observed_at_epoch": job.enqueued_at,
            },
        )
        return

    @staticmethod
    def _stable_hash(value: Any) -> str:
        encoded = json.dumps(
            value, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, default=str,
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    def _current_commit_dependencies(self) -> tuple[str, str]:
        if self._agent is None:
            raise RuntimeError("canonical Timechain agent is unavailable")
        loop = self._agent.cognitive_loop
        registry = loop.epochs_module.registry_hashes(loop.root)
        policy = self._agent.poq_module.PoQGate().t
        return self._stable_hash(policy), self._stable_hash(registry)

    def _prepare_watcher_commit(
        self, item: DeferredQueueItem
    ) -> PreparedWatcherCommit:
        """Run bounded cognition and PoQ scoring without appending a ring."""
        if self._agent is None:
            raise RuntimeError("canonical Timechain agent is unavailable")
        envelope = item.payload
        report = json.loads(json.dumps(envelope["pinned_snapshot"], default=str))
        if self._stable_hash(report) != envelope.get("report_hash"):
            raise ValueError("watcher report hash mismatch")
        facts = (report.get("provenance") or {}).get("facts") or []
        if self._stable_hash(facts) != envelope.get("evidence_hash"):
            raise ValueError("watcher evidence hash mismatch")
        cognition = self._agent.cognitive_loop.prepare(report)
        analysis = report.get("analysis") or {}
        network = str(envelope["network"])
        token = str(envelope["token_address"])
        anchor_kind = str(envelope["anchor_kind"])
        anchor_value = envelope.get("anchor_value")
        evidence_binding = analysis_evidence_binding(
            report.get("provenance") or {},
            anchor_type=anchor_kind,
            anchor_value=anchor_value,
        )
        candidate = (
            f"Watcher observation for {network} subject {token} at "
            f"{anchor_kind} {anchor_value}: risk "
            f"{analysis.get('risk_level', 'Unknown')}, legitimacy "
            f"{analysis.get('legitimacy_score', 'unknown')}."
        )
        context = json.dumps(
            {
                "network": network, "token_address": token,
                "anchor_kind": anchor_kind, "anchor_value": anchor_value,
                "analysis": analysis,
                "provenance_hash": envelope["evidence_hash"],
            }, sort_keys=True, default=str,
        )
        window = self._agent.poq_module.relevance_window(self._agent.tc)
        verdict = self._agent.poq_module.PoQGate().evaluate(
            candidate, window, context=context,
            external_scores=report.get("poq_scores") or {},
            span_guard=True, evidence_texts=[context], frame="assertion",
        )
        if verdict.get("decision") != "SEAL":
            raise ValueError(
                "watcher PoQ preparation refused: "
                + str(verdict.get("decision"))
            )
        policy_hash, registry_hash = self._current_commit_dependencies()
        payload = {
            "summary": candidate,
            "network": network,
            "token_address": token,
            "anchor_kind": anchor_kind,
            "anchor_value": anchor_value,
            "observed_at_epoch": envelope.get("observed_at_epoch"),
            "risk_level": analysis.get("risk_level"),
            "legitimacy_score": analysis.get("legitimacy_score"),
            "action_label": analysis.get("action_label"),
            **evidence_binding,
            "source_fact_hash": envelope["evidence_hash"],
            "report_hash": envelope["report_hash"],
            "analyzer_version": envelope.get("analyzer_version"),
            "idempotency_key": envelope["idempotency_key"],
            "policy_hash": policy_hash,
            "registry_hash": registry_hash,
            "cognition": {
                key: value for key, value in cognition.items()
                if key != "growth"
            },
            "poq_verdict": {
                "decision": verdict["decision"],
                "cited_rings": verdict.get("cited_rings") or [],
            },
        }
        return PreparedWatcherCommit(
            item, payload, dict(verdict.get("scores") or {}),
            policy_hash, registry_hash,
        )

    def _commit_prepared_watcher(self, prepared: PreparedWatcherCommit) -> str:
        """Validate and append one minimal ring under the writer lock."""
        item = prepared.queue_item
        if self._analysis_active.is_set() or not self.work.empty():
            return "retry"
        if not self._acquire_timechain("watcher_commit", blocking=False):
            return "retry"
        try:
            if self._analysis_active.is_set() or not self.work.empty():
                return "retry"
            if not self._deferred_queue.is_current(item):
                return "superseded"
            if self._agent is None:
                return "retry"
            ok, verification = self._agent.cognitive_loop.verify_incremental()
            if not ok:
                raise RuntimeError(
                    "incremental Timechain verification failed: "
                    + "; ".join(verification)
                )
            policy_hash, registry_hash = self._current_commit_dependencies()
            if (policy_hash, registry_hash) != (
                prepared.policy_hash, prepared.registry_hash
            ):
                return "retry"
            payload = prepared.payload
            key = payload["idempotency_key"]
            subject = item.subject_key
            fresh = (
                int(payload.get("anchor_value") or -1),
                float(payload.get("observed_at_epoch") or 0),
            )
            for ring in reversed(self._agent.tc.tail_rings(512)):
                existing = ring.get("payload") or {}
                if existing.get("idempotency_key") == key:
                    return "idempotent"
                existing_subject = (
                    f"{existing.get('network')}:"
                    f"{existing.get('token_address')}"
                )
                if existing_subject == subject:
                    previous = (
                        int(existing.get("anchor_value") or -1),
                        float(existing.get("observed_at_epoch") or 0),
                    )
                    if previous >= fresh:
                        return "superseded"
                    break
            previous_autoindex = os.environ.get("CT_AUTOINDEX")
            os.environ["CT_AUTOINDEX"] = "0"
            try:
                ring_type = (
                    "solana_token_analysis"
                    if payload.get("network") == "solana"
                    else "token_analysis"
                )
                if ring_type == "solana_token_analysis":
                    payload.setdefault("mint", payload.get("token_address"))
                    payload.setdefault("slot_anchor", payload.get("anchor_value"))
                    payload.setdefault(
                        "analysis",
                        {
                            "risk_level": payload.get("risk_level"),
                            "legitimacy_score": payload.get("legitimacy_score"),
                            "action_label": payload.get("action_label"),
                        },
                    )
                ring = self._agent.tc.seal(
                    ring_type, payload, poq=prepared.poq_scores
                )
            finally:
                if previous_autoindex is None:
                    os.environ.pop("CT_AUTOINDEX", None)
                else:
                    os.environ["CT_AUTOINDEX"] = previous_autoindex
            self._agent.cognitive_loop.establish_trust()
            LOGGER.debug(
                "committed watcher observation %s at ring %s",
                key, ring.get("index"),
            )
            return f"committed:{ring.get('index')}"
        finally:
            self._release_timechain()

    def _drain_durable_commits(self) -> None:
        """Prepare one durable item and commit only while the user lane is idle."""
        if self._analysis_active.is_set() or not self.work.empty():
            return
        item = self._deferred_queue.claim(
            kinds=(
                "cognitive_completion",
                "watcher_commit",
                "watcher_outcome",
            ),
            lease_seconds=30.0,
        )
        if item is None:
            return
        try:
            if item.kind == "cognitive_completion":
                if not self.settings.cognitive_completion_enabled:
                    self._deferred_queue.transition(item, "discarded")
                    self._set_cognitive_progress(
                        str(item.payload.get("job_id") or ""),
                        "isolated",
                        100,
                        "Deep learning is delegated to the isolated learner",
                    )
                    return
                outcome = self._execute_cognitive_completion(item)
                if outcome in {"done", "idempotent"}:
                    self._deferred_queue.transition(item, outcome)
                else:
                    self._deferred_queue.retry(
                        item, outcome, delay_seconds=0.1
                    )
                    self._set_cognitive_progress(
                        str(item.payload.get("job_id") or ""),
                        "failed" if item.attempts >= 5 else "retrying",
                        0 if item.attempts >= 5 else 15,
                        (
                            "Cognitive completion needs operator attention"
                            if item.attempts >= 5
                            else "Cognitive completion yielded to newer analysis work"
                        ),
                    )
                return
            if item.kind == "watcher_outcome":
                outcome = self._execute_deferred_outcome(item)
                if outcome == "done":
                    self._deferred_queue.transition(item, "done")
                else:
                    self._deferred_queue.retry(
                        item, outcome, delay_seconds=0.25
                    )
                return
            # A reclaimed lease may represent a crash after append but before
            # SQLite acknowledgement.  Pay for a full read only on that rare
            # recovery path so idempotency remains correct beyond any bounded
            # tail window, without taxing ordinary commits or lock hold time.
            if item.attempts > 1 and self._agent is not None:
                key = item.payload.get("idempotency_key")
                if any(
                    (ring.get("payload") or {}).get("idempotency_key") == key
                    for ring in self._agent.tc.load()
                ):
                    self._deferred_queue.transition(item, "idempotent")
                    return
            prepared = self._prepare_watcher_commit(item)
            if not self._deferred_queue.transition(item, "ready"):
                return
            outcome = self._commit_prepared_watcher(prepared)
            if outcome.startswith("committed:"):
                ring_index = int(outcome.split(":", 1)[1])
                self._deferred_queue.transition(item, "done")
                envelope = item.payload
                self._deferred_queue.enqueue(
                    kind="watcher_outcome",
                    subject_key=item.subject_key,
                    priority=30,
                    payload={
                        "network": envelope["network"],
                        "token_address": envelope["token_address"],
                        "anchor_value": envelope.get("anchor_value"),
                        "observed_at_epoch": envelope.get("observed_at_epoch"),
                        "analysis_ring": ring_index,
                        "pinned_snapshot": envelope["pinned_snapshot"],
                    },
                )
            elif outcome in {"idempotent", "superseded"}:
                self._deferred_queue.transition(item, outcome)
            else:
                self._deferred_queue.retry(
                    item, "user analysis has priority", delay_seconds=0.1
                )
        except (ValueError, KeyError) as exc:
            self._deferred_queue.transition(item, "discarded")
            if item.kind == "cognitive_completion":
                self._set_cognitive_progress(
                    str(item.payload.get("job_id") or ""),
                    "failed",
                    0,
                    "Cognitive completion failed its integrity check",
                )
            LOGGER.warning("discarded deferred commit: %s", exc)
        except Exception as exc:
            self._deferred_queue.retry(item, str(exc), delay_seconds=0.25)
            if item.kind == "cognitive_completion":
                self._set_cognitive_progress(
                    str(item.payload.get("job_id") or ""),
                    "retrying",
                    15,
                    "Cognitive completion will retry while the lane is idle",
                )
            LOGGER.exception("deferred commit failed")

    def _execute_cognitive_completion(
        self,
        item: DeferredQueueItem,
    ) -> str:
        """Append one bounded completion linked to an immutable analysis ring."""
        if self._agent is None or not hasattr(self._agent, "tc"):
            return "timechain unavailable"
        if self._analysis_active.is_set() or not self.work.empty():
            return "user analysis has priority"
        payload = item.payload
        expected_index = int(payload["analysis_ring"])
        expected_hash = str(payload["analysis_ring_hash"])
        tail = self._agent.tc.tail_rings(512)

        def completion_for(rings: list[dict[str, Any]]) -> dict[str, Any] | None:
            return next(
                (
                    ring for ring in reversed(rings)
                    if ring.get("ring_type") == "cognitive_completion"
                    and (ring.get("payload") or {}).get("analysis_ring")
                    == expected_index
                    and (ring.get("payload") or {}).get("analysis_ring_hash")
                    == expected_hash
                ),
                None,
            )

        existing = completion_for(tail)
        analysis_ring = next(
            (ring for ring in tail if ring.get("index") == expected_index),
            None,
        )
        if (existing is None or analysis_ring is None) and item.attempts > 1:
            # Crash recovery is uncommon. Stream the full ledger outside the
            # writer lane only when the bounded tail cannot prove state.
            for ring in self._agent.tc.iter_rings():
                if analysis_ring is None and ring.get("index") == expected_index:
                    analysis_ring = ring
                ring_payload = ring.get("payload") or {}
                if (
                    existing is None
                    and ring.get("ring_type") == "cognitive_completion"
                    and ring_payload.get("analysis_ring") == expected_index
                    and ring_payload.get("analysis_ring_hash") == expected_hash
                ):
                    existing = ring
        if existing is not None:
            self._publish_cognitive_completion(payload, existing)
            return "idempotent"
        if analysis_ring is None:
            return "analysis ring not visible in bounded tail"
        if analysis_ring.get("ring_hash") != expected_hash:
            raise ValueError("analysis ring hash collision")
        if not self._deferred_queue.transition(item, "ready"):
            return "queue generation changed"
        if not self._acquire_timechain("cognitive_completion", blocking=False):
            return "timechain busy"
        try:
            if self._analysis_active.is_set() or not self.work.empty():
                return "user analysis has priority"
            if not self._deferred_queue.is_current(item):
                return "queue generation changed"
            latest_match = next(
                (
                    ring for ring in self._agent.tc.tail_rings(512)
                    if ring.get("index") == expected_index
                ),
                None,
            )
            if latest_match is None or latest_match.get("ring_hash") != expected_hash:
                raise ValueError("analysis ring changed before cognitive append")
            ok, verification = self._agent.cognitive_loop.verify_incremental()
            if not ok:
                raise RuntimeError(
                    "incremental Timechain verification failed: "
                    + "; ".join(verification)
                )
            self._set_cognitive_progress(
                str(payload.get("job_id") or ""),
                "running",
                60,
                "Verifying and sealing the cognitive audit",
            )
            completion_report = json.loads(
                json.dumps(payload["completion_report"])
            )
            self._agent.cognitive_loop.finalize_deferred(
                completion_report, latest_match
            )
            completed = completion_for(self._agent.tc.tail_rings(8))
            if completed is None:
                raise RuntimeError("cognitive completion ring was not appended")
            self._agent.cognitive_loop.establish_trust()
            self._publish_cognitive_completion(payload, completed)
            return "done"
        finally:
            self._release_timechain()

    def _publish_cognitive_completion(
        self,
        payload: dict[str, Any],
        completion_ring: dict[str, Any],
    ) -> None:
        job_id = str(payload.get("job_id") or "")
        cognition = (completion_ring.get("payload") or {}).get(
            "cognitive_loop"
        ) or {}
        with self._lock:
            job = self.jobs.get(job_id)
            if job is None:
                return
            if job.result is not None:
                timechain = job.result.setdefault("timechain", {})
                timechain["cognition"] = cognition
                timechain["cognitive_ring"] = completion_ring.get("index")
                timechain["cognitive_ring_hash"] = completion_ring.get("ring_hash")
            job.cognition_status = "complete"
            job.cognition_stage_detail = "Cognitive audit sealed"
            job.cognition_progress_percent = 100
            job.cognition_updated_at = time.time()
            job.updated_at = job.cognition_updated_at
        self._persist_public_result(job)

    def _execute_deferred_outcome(self, item: DeferredQueueItem) -> str:
        """Prepare watcher cognition off-lane; append only its bounded result."""
        payload = item.payload
        network = str(payload["network"])
        # Solana did not previously have an outcome collector.  Its committed
        # watcher ring is still durable; no synthetic calibration is invented.
        if network == "solana":
            return "done"
        watcher = (
            self._base_watcher if network == "base" else self._watcher
        )
        if watcher is None or self._agent is None:
            return "watcher unavailable"
        if self._analysis_active.is_set() or not self.work.empty():
            return "user analysis has priority"
        if not self._watch_lock.acquire(blocking=False):
            return "watch state busy"
        try:
            state = watcher.store.load()
            token = str(payload["token_address"])
            subscriptions = state.get("subscriptions") or {}
            subscription = (
                subscriptions.get(token.lower()) or subscriptions.get(token)
            )
            if subscription is None:
                return "done"
            report = json.loads(
                json.dumps(payload["pinned_snapshot"], default=str)
            )
            report["analysis_ring"] = int(payload["analysis_ring"])
            baselines = subscription.setdefault("analyses", [])
            if baselines:
                baselines[-1]["analysis_ring"] = int(payload["analysis_ring"])
            # Binding the observational baseline to its committed ring is
            # watcher state only; it must not occupy the Timechain writer lane.
            watcher.store.save(state)
            subscription_snapshot = json.loads(json.dumps(subscription))
        finally:
            self._watch_lock.release()

        if self._analysis_active.is_set() or not self.work.empty():
            return "user analysis has priority"
        prepared = watcher.outcomes.prepare(
            self._agent,
            subscription_snapshot,
            report,
            now=float(payload.get("observed_at_epoch") or time.time()),
            horizons=watcher.config.outcome_horizons_seconds,
            limit=1,
        )
        if not prepared:
            return "done"

        # A concurrent user seal invalidates the PoQ window/head anchor.  Do
        # not append stale cognition; a retry will re-prepare from the new head.
        reflection = prepared[0]["reflection"]
        if not reflection.get("head_stable_during_prepare"):
            return "timechain advanced during outcome preparation"
        if self._analysis_active.is_set() or not self.work.empty():
            return "user analysis has priority"
        if not self._acquire_timechain("watcher_outcome_append", blocking=False):
            return "timechain busy"
        acquired_watch = False

        try:
            if self._analysis_active.is_set() or not self.work.empty():
                return "user analysis has priority"
            acquired_watch = self._watch_lock.acquire(blocking=False)
            if not acquired_watch:
                return "watch state busy"
            if not self._deferred_queue.is_current(item):
                return "queue generation changed"
            state = watcher.store.load()
            subscriptions = state.get("subscriptions") or {}
            current_subscription = (
                subscriptions.get(token.lower()) or subscriptions.get(token)
            )
            if current_subscription is None:
                return "done"
            if prepared[0]["key"] in set(
                current_subscription.get("completed_outcomes") or []
            ):
                return "done"
            emitted = watcher.outcomes.commit_prepared(
                self._agent, current_subscription, prepared[0]
            )
            watcher.store.save(state)
            LOGGER.debug(
                "processed deferred outcome %s at ring %s",
                emitted["key"], emitted["outcome_ring"],
            )
            return "done"
        except RuntimeError as exc:
            if "Timechain" in str(exc) and "advanced" in str(exc):
                return "timechain advanced before outcome append"
            raise
        finally:
            if acquired_watch:
                self._watch_lock.release()
            self._release_timechain()

    def _run_maintenance(self) -> None:
        next_audit = (
            time.monotonic() + self.settings.full_audit_interval_seconds
        )
        while not self._stopping.is_set():
            timeout = max(
                0.1,
                min(1.0, next_audit - time.monotonic()),
            )
            task: MaintenanceTask | None
            try:
                task = self._maintenance_work.get(timeout=timeout)
            except queue.Empty:
                task = None
            self._check_memory_usage()
            if task is not None:
                if not task.benchmark_done:
                    with self._lock:
                        benchmark_job = self.jobs.get(task.job_id)
                    capture = self._benchmark.capture(
                        benchmark_job
                        or Job(
                            id=task.job_id,
                            address=task.address,
                            network=task.network,
                        ),
                        task.public_report,
                    )
                    task.benchmark_done = True
                    with self._lock:
                        job = self.jobs.get(task.job_id)
                        if job is not None:
                            job.benchmark_capture = capture
                            job.updated_at = time.time()
                # The bounded cognitive append has higher user-visible value
                # than a potentially large temporal read-model update. Give
                # it the first idle writer-lane opportunity.
                self._drain_durable_commits()
                temporal = task.report.get("temporal_entity_graph") or {}
                needs_rebuild = (
                    self.settings.temporal_projection_enabled
                    and not temporal.get("available", False)
                )
                if not self.settings.temporal_projection_enabled:
                    task.projection_done = True
                if needs_rebuild and self._analysis_active.is_set():
                    try:
                        self._maintenance_work.put_nowait(task)
                    except queue.Full:
                        pass
                    self._stopping.wait(0.1)
                elif (
                    needs_rebuild
                    and self._agent is not None
                    and hasattr(self._agent, "tc")
                    and hasattr(self._agent, "cognitive_loop")
                ):
                    try:
                        analysis_ring = task.report.get("_analysis_ring_record")
                        if analysis_ring is not None:
                            task.report["temporal_entity_graph"] = (
                                append_temporal_projection(
                                    self.settings.chain_root,
                                    analysis_ring,
                                    network=task.network,
                                    subject=task.address,
                                )
                            )
                        else:
                            # Compatibility fallback for imported/legacy
                            # reports. It is intentionally outside the writer
                            # lane, so read-model work cannot block a scan.
                            task.report["temporal_entity_graph"] = (
                                refresh_temporal_projection(
                                    self._agent.tc,
                                    self.settings.chain_root,
                                    network=task.network,
                                    subject=task.address,
                                )
                            )
                        task.projection_done = True
                        refreshed = build_public_report(task.report)
                        with self._lock:
                            job = self.jobs.get(task.job_id)
                            if job is not None:
                                # Preserve a completion that may have landed
                                # while the projection was being prepared.
                                prior_timechain = (
                                    (job.result or {}).get("timechain") or {}
                                )
                                if prior_timechain.get("cognitive_ring"):
                                    refreshed["timechain"] = prior_timechain
                                job.result = refreshed
                                job.updated_at = time.time()
                        if job is not None:
                            self._persist_public_result(job)
                    except Exception:
                        LOGGER.exception(
                            "Deferred temporal projection refresh failed",
                            extra={"job_id": task.job_id},
                        )
                self._maintenance_work.task_done()
            self._drain_durable_commits()
            if time.monotonic() >= next_audit:
                completed = self._run_full_audit()
                next_audit = (
                    time.monotonic()
                    + (
                        self.settings.full_audit_interval_seconds
                        if completed
                        else 0.1
                    )
                )

    @staticmethod
    def _restore_sealed_report(
        report: dict[str, Any], ring: dict[str, Any]
    ) -> None:
        """Reconstruct request output after append-before-ack recovery."""
        payload = ring.get("payload") or {}
        report["analysis_ring"] = ring.get("index")
        report["analysis_ring_hash"] = ring.get("ring_hash")
        report["analysis_evidence_hash"] = payload.get("evidence_hash")
        report["poq_verdict"] = payload.get("poq_verdict") or {
            "decision": "SEAL",
            "cited_rings": [],
        }
        report["_analysis_ring_record"] = ring
        cognition = payload.get("cognitive_loop") or report.get("cognition") or {}
        cognition["status"] = "pending"
        cognition["analysis_ring"] = ring.get("index")
        cognition["growth_status"] = "excluded_from_online_completion"
        report["cognition"] = cognition
        report["cognitive_completion"] = {
            "status": "queued",
            "analysis_ring": ring.get("index"),
        }
        report["temporal_entity_graph"] = {
            "available": False,
            "status": "queued",
            "reason": "temporal_projection_pending",
        }

    def _seal_user_report_once(
        self,
        sealing_agent: Any,
        report: dict[str, Any],
        job_id: str,
        *,
        exhaustive_recovery: bool = False,
    ) -> None:
        """Append once even when a reliable queue redelivers after a crash."""
        if self._agent is None:
            raise RuntimeError("Timechain writer is unavailable")
        key = f"public_analysis:{job_id}"
        report["_idempotency_key"] = key
        tc = getattr(self._agent, "tc", None)
        if tc is None:
            # Lightweight test/adaptor agents may expose only _seal_report.
            # Production Chainseer agents always expose their Timechain.
            sealing_agent._seal_report(report, defer_cognition=True)
            return
        rings = tc.tail_rings(512)
        existing = next(
            (
                ring
                for ring in reversed(rings)
                if (ring.get("payload") or {}).get("idempotency_key") == key
            ),
            None,
        )
        if existing is None and exhaustive_recovery:
            existing = next(
                (
                    ring
                    for ring in tc.iter_rings()
                    if (ring.get("payload") or {}).get("idempotency_key")
                    == key
                ),
                None,
            )
        if existing is not None:
            payload = existing.get("payload") or {}
            expected_address = (
                report.get("token_address") or report.get("mint")
            )
            stored_address = payload.get("token_address") or payload.get("mint")
            if str(stored_address) != str(expected_address):
                raise RuntimeError("analysis idempotency key subject collision")
            self._restore_sealed_report(report, existing)
            return
        sealing_agent._seal_report(report, defer_cognition=True)

    def _run(self) -> None:
        while not self._stopping.is_set():
            try:
                job_id, claimed_shared = self._claim_scan_job()
            except Exception:
                LOGGER.exception("Could not claim scan work")
                self._stopping.wait(0.5)
                continue
            if job_id is None and claimed_shared:
                continue
            if job_id is None:
                return
            with self._lock:
                job = self.jobs.get(job_id)
                recovered_execution = False
                if not job:
                    shared_snapshot = None
                    if claimed_shared and self._shared_job_store is not None:
                        try:
                            shared_snapshot = self._shared_job_store.get_job(
                                job_id
                            )
                        except Exception:
                            LOGGER.exception(
                                "Could not load claimed shared scan",
                                extra={"job_id": job_id},
                            )
                    if shared_snapshot is not None:
                        try:
                            job = Job.from_public(shared_snapshot)
                        except (TypeError, ValueError):
                            LOGGER.exception(
                                "Discarding invalid shared scan snapshot",
                                extra={"job_id": job_id},
                            )
                    if job is None or job.status not in {
                        "queued", "running", "waiting_for_timechain"
                    }:
                        try:
                            self._acknowledge_scan_job(
                                job_id, claimed_shared
                            )
                        except Exception:
                            LOGGER.exception(
                                "Could not acknowledge unusable shared scan",
                                extra={"job_id": job_id},
                            )
                        continue
                    recovered_execution = job.status in {
                        "running", "waiting_for_timechain"
                    }
                    self.jobs[job.id] = job
                    subject = (
                        job.address.lower()
                        if job.network in EVM_NETWORKS
                        else job.address
                    )
                    self.active_by_address[f"{job.network}:{subject}"] = job.id
                recovered_execution = recovered_execution or job.status in {
                    "running", "waiting_for_timechain"
                }
                job.status = "running"
                job.stage = "initializing"
                job.stage_detail = "Preparing a block-pinned analysis"
                job.progress_percent = 2
                job.started_at = time.time()
                job.stage_started_at = job.started_at
                job.updated_at = job.started_at
            self._publish_shared_job(job)

            self._analysis_active.set()
            retrying = False
            try:
                if self._integrity_status.get("status") == "failed":
                    raise PublicAnalysisError(
                        "timechain_integrity_failed",
                        "Timechain integrity audit failed; analysis is paused",
                    )
                if self._agent is None:
                    raise RuntimeError("analysis worker has no Chainseer agent")

                def progress(stage: str, percent: int, detail: str) -> None:
                    self._set_job_progress(
                        job.id, stage, percent, detail
                    )

                seal_deferred = False
                sealing_agent: Any = None
                if job.network == "solana":
                    if self._solana_agent is None:
                        raise RuntimeError(
                            "analysis worker has no Solana analyzer"
                        )
                    progress(
                        "collecting_external_evidence",
                        18,
                        "Reading mint controls, markets, holders, and routes",
                    )
                    method = self._solana_agent.analyze_token
                    kwargs: dict[str, Any] = {}
                    try:
                        parameters = inspect.signature(method).parameters
                        if "progress_callback" in parameters:
                            kwargs["progress_callback"] = progress
                        if "defer_cognition" in parameters:
                            kwargs["defer_cognition"] = True
                        if "seal" in parameters:
                            kwargs["seal"] = False
                            seal_deferred = True
                            sealing_agent = self._solana_agent
                    except (TypeError, ValueError):
                        pass
                    if seal_deferred:
                        report = method(job.address, **kwargs)
                    else:
                        with self._tracked_timechain_lock("user_analysis"):
                            report = method(job.address, **kwargs)
                elif job.network == "base":
                    if self._base_agent is None:
                        raise RuntimeError(
                            "analysis worker has no Base analyzer"
                        )
                    method = self._base_agent.analyze_token
                    kwargs: dict[str, Any] = {"full_report": False}
                    try:
                        parameters = inspect.signature(method).parameters
                        if "progress_callback" in parameters:
                            kwargs["progress_callback"] = progress
                        if "defer_cognition" in parameters:
                            kwargs["defer_cognition"] = True
                        if "seal" in parameters:
                            kwargs["seal"] = False
                            seal_deferred = True
                            sealing_agent = self._base_agent
                    except (TypeError, ValueError):
                        pass
                    if seal_deferred:
                        report = method(job.address, **kwargs)
                    else:
                        with self._tracked_timechain_lock("user_analysis"):
                            report = method(job.address, **kwargs)
                else:
                    method = self._agent.analyze_token
                    kwargs = {"full_report": False}
                    try:
                        parameters = inspect.signature(method).parameters
                        if "progress_callback" in parameters:
                            kwargs["progress_callback"] = progress
                        if "defer_cognition" in parameters:
                            kwargs["defer_cognition"] = True
                        if "seal" in parameters:
                            kwargs["seal"] = False
                            seal_deferred = True
                            sealing_agent = self._agent
                    except (TypeError, ValueError):
                        pass
                    if seal_deferred:
                        report = method(job.address, **kwargs)
                    else:
                        with self._tracked_timechain_lock("user_analysis"):
                            report = method(job.address, **kwargs)
                if report.get("error"):
                    raise PublicAnalysisError(
                        "analysis_rejected", str(report["error"])
                    )
                if seal_deferred:
                    progress(
                        "sealing_timechain",
                        90,
                        "Verifying and appending the immutable analysis",
                    )
                    with self._tracked_timechain_lock("user_analysis_append"):
                        self._seal_user_report_once(
                            sealing_agent,
                            report,
                            job.id,
                            exhaustive_recovery=recovered_execution,
                        )
                progress(
                    "publishing",
                    98,
                    "Preparing the privacy-safe sealed report",
                )
                try:
                    self._enqueue_cognitive_completion(job, report)
                except Exception:
                    LOGGER.exception(
                        "Could not persist cognitive completion",
                        extra={"job_id": job.id},
                    )
                    job.cognition_status = "failed"
                    job.cognition_stage_detail = (
                        "Risk result is sealed; cognitive completion could not be queued"
                    )
                    job.cognition_progress_percent = 0
                    job.cognition_updated_at = time.time()
                public_report = build_public_report(report)
                with self._lock:
                    job.result = public_report
                    job.benchmark_capture = (
                        {"status": "queued"}
                        if self._benchmark.enabled
                        else {"status": "disabled"}
                    )
                    job.status = "succeeded"
                    completed_at = time.time()
                    if job.stage_started_at is not None:
                        elapsed_ms = max(
                            0.0, (completed_at - job.stage_started_at) * 1000
                        )
                        job.stage_timings_ms[job.stage] = round(
                            job.stage_timings_ms.get(job.stage, 0.0)
                            + elapsed_ms,
                            1,
                        )
                    job.stage = "complete"
                    job.stage_detail = (
                        "Sealed analysis ready; cognitive audit continues in background"
                        if job.cognition_status == "queued"
                        else "Sealed analysis ready"
                    )
                    job.progress_percent = 100
                    job.stage_started_at = completed_at
                    job.updated_at = completed_at
                    job.analysis_latency_ms = max(
                        0.0,
                        (job.updated_at - (job.started_at or job.updated_at))
                        * 1000,
                    )
                    self._recent_analysis_latencies_ms.append(
                        job.analysis_latency_ms
                    )
                    self._latest_analysis_summary = {
                        "network": job.network,
                        "finished_at": _iso(job.updated_at),
                        "latency_ms": round(job.analysis_latency_ms, 1),
                        "queue_delay_ms": round(
                            max(
                                0.0,
                                (
                                    (job.started_at or job.created_at)
                                    - job.created_at
                                )
                                * 1000,
                            ),
                            1,
                        ),
                    }
                    if self.settings.cache_ttl_seconds:
                        cache_address = (
                            job.address.lower()
                            if job.network in EVM_NETWORKS
                            else job.address
                        )
                        self.cache[f"{job.network}:{cache_address}"] = (
                            time.time()
                            + self.settings.cache_ttl_seconds,
                            job.id,
                        )
                self._enqueue_maintenance(job, report)
            except PublicAnalysisError as exc:
                with self._lock:
                    job.status = "failed"
                    job.stage = "failed"
                    job.stage_detail = exc.message
                    job.updated_at = time.time()
                    job.error_code = exc.code
                    job.error_message = exc.message
            except SolanaMintError as exc:
                with self._lock:
                    job.status = "failed"
                    job.stage = "failed"
                    job.stage_detail = exc.message
                    job.updated_at = time.time()
                    job.error_code = exc.code
                    job.error_message = exc.message
            except TimeoutError as exc:
                with self._lock:
                    job.lock_retry_count += 1
                    retries = job.lock_retry_count
                LOGGER.warning(
                    "Timechain lock timeout for job %s (attempt %d/3): %s",
                    job.id,
                    retries,
                    exc,
                )
                if retries < 3:
                    with self._lock:
                        job.status = "waiting_for_timechain"
                        job.stage = "waiting_for_timechain"
                        job.stage_detail = (
                            f"Waiting for timechain lock (attempt {retries}/3)"
                        )
                        job.updated_at = time.time()
                    # Re-enqueue for retry.
                    try:
                        retrying = self._requeue_scan_job(job.id)
                    except Exception:
                        LOGGER.exception(
                            "Could not requeue scan after Timechain timeout",
                            extra={"job_id": job.id},
                        )
                        retrying = False
                    if not retrying:
                        with self._lock:
                            job.status = "failed"
                            job.stage = "failed"
                            job.stage_detail = (
                                "Queue full after lock timeout retries"
                            )
                            job.updated_at = time.time()
                else:
                    with self._lock:
                        job.status = "failed"
                        job.stage = "failed"
                        job.stage_detail = (
                            f"Timechain lock unavailable after {retries} attempts"
                        )
                        job.updated_at = time.time()
                        job.error_code = "timechain_lock_timeout"
                        job.error_message = str(exc)
            except Exception:
                LOGGER.exception(
                    "Analysis job failed",
                    extra={"job_id": job.id, "address": job.address},
                )
                with self._lock:
                    job.status = "failed"
                    job.stage = "failed"
                    job.stage_detail = (
                        "Analysis stopped before a report was published"
                    )
                    job.updated_at = time.time()
                    job.error_code = "analysis_failed"
                    job.error_message = (
                        "The analysis could not be completed. "
                        "No result was published."
                    )
            finally:
                self._analysis_active.clear()
                with self._lock:
                    if not retrying:
                        job.finished_at = time.time()
                        job.updated_at = job.finished_at
                        self.active_by_address.pop(
                            (
                                f"{job.network}:"
                                + (
                                    job.address.lower()
                                    if job.network in EVM_NETWORKS
                                    else job.address
                                )
                            ),
                            None,
                        )
                if not retrying and job.status == "succeeded":
                    self._persist_public_result(job)
                self._publish_shared_job(job)
                if not retrying and self._shared_job_store is not None:
                    subject = (
                        f"{job.network}:"
                        + (
                            job.address.lower()
                            if job.network in EVM_NETWORKS
                            else job.address
                        )
                    )
                    try:
                        self._shared_job_store.release_active(
                            subject, job.id
                        )
                    except Exception:
                        LOGGER.exception(
                            "Could not release shared active-scan lease",
                            extra={"job_id": job.id},
                        )
                if not retrying or not claimed_shared:
                    try:
                        self._acknowledge_scan_job(job.id, claimed_shared)
                    except Exception:
                        LOGGER.exception(
                            "Could not acknowledge completed scan work",
                            extra={"job_id": job.id},
                        )


class QueueFullError(Exception):
    pass


class SharedStoreUnavailableError(Exception):
    pass


class IntegrityUnavailableError(Exception):
    pass


class WatcherBusyError(Exception):
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
    entity_graph = data.get("entity_graph") or {}
    temporal_graph = report.get("temporal_entity_graph") or {}
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
        "schema_version": "1.3",
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
            "balance_verification": {
                "complete": holder_evidence.get(
                    "balance_verification_complete"
                ),
                "source": holder_evidence.get("holder_balance_source"),
                "verified": holder_evidence.get(
                    "rpc_balance_verified_count"
                ),
                "failures": holder_evidence.get(
                    "rpc_balance_failure_count"
                ),
                "indexer_mismatches": holder_evidence.get(
                    "indexed_balance_mismatch_count"
                ),
            },
            "caveat": holder_caveat,
        },
        "entity_graph": {
            "schema_version": entity_graph.get("schema_version"),
            "network": entity_graph.get("network"),
            "root_entity_id": entity_graph.get("root_entity_id"),
            "anchor": entity_graph.get("anchor") or {},
            "summary": entity_graph.get("summary") or {},
            "nodes": (entity_graph.get("nodes") or [])[:50],
            "edges": (entity_graph.get("edges") or [])[:100],
            "signals": (entity_graph.get("signals") or [])[:30],
            "limitations": entity_graph.get("limitations") or [],
            "graph_hash": entity_graph.get("graph_hash"),
            "temporal": {
                "available": bool(temporal_graph.get("available")),
                "schema_version": temporal_graph.get("schema_version"),
                "projection_hash": temporal_graph.get("projection_hash"),
                "source_chain": temporal_graph.get("source_chain") or {},
                "first_observed": temporal_graph.get("first_observed"),
                "last_observed": temporal_graph.get("last_observed"),
                "analysis_count": temporal_graph.get("analysis_count", 0),
                "risk_evolution": temporal_graph.get("risk_evolution") or {},
                "risk_timeline": (temporal_graph.get("risk_timeline") or [])[-20:],
                "relationship_summary": temporal_graph.get("relationship_summary") or {},
                "relationship_events": (temporal_graph.get("relationship_events") or [])[-40:],
                "shared_entities": (temporal_graph.get("shared_entities") or [])[:20],
                "legacy_graph_observations": temporal_graph.get(
                    "legacy_graph_observations", 0
                ),
                "limitations": temporal_graph.get("limitations") or [],
                "reason": temporal_graph.get("reason"),
            },
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
            "analysis_evidence_hash": report.get("analysis_evidence_hash"),
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
SHARED_JOB_STORE = create_shared_job_store(
    SETTINGS.shared_store_url,
    prefix=SETTINGS.shared_store_prefix,
    socket_timeout_seconds=SETTINGS.shared_store_timeout_seconds,
)
AUTHORITATIVE_PROCESS = SETTINGS.process_role != "gateway"
WRITER_LEASE = DistributedWriterLease(
    SHARED_JOB_STORE if AUTHORITATIVE_PROCESS else None,
    owner_id=(
        f"fly-machine:{os.environ['FLY_MACHINE_ID']}"
        if os.environ.get("FLY_MACHINE_ID")
        else f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex}"
    ),
    ttl_seconds=SETTINGS.writer_lease_ttl_seconds,
)
SERVICE = AnalysisService(
    SETTINGS,
    shared_job_store=SHARED_JOB_STORE,
    writer_lease=WRITER_LEASE,
)
LIMITER = SlidingWindowRateLimiter(
    SETTINGS.rate_limit_per_minute,
    global_limit=SETTINGS.global_rate_limit_per_minute,
    shared_store=SHARED_JOB_STORE,
)
LEASE = SingleProcessLease(SETTINGS.chain_root)


async def require_api_token(
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
    if not AUTHORITATIVE_PROCESS:
        SERVICE.start()
        try:
            yield
        finally:
            SERVICE.stop()
        return
    LEASE.acquire()
    writer_lease_acquired = False
    try:
        WRITER_LEASE.acquire()
        writer_lease_acquired = True
        SERVICE.start()
        yield
    finally:
        stopped = SERVICE.stop()
        if stopped:
            if writer_lease_acquired:
                WRITER_LEASE.release()
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

_AUTHORITATIVE_ROUTE_PREFIXES = (
    "/v1/memory",
    "/v1/watch",
    "/v1/admin",
)


@app.middleware("http")
async def gateway_authoritative_proxy(request: Request, call_next):
    """Forward disk-backed APIs while keeping gateway Machines stateless.

    Analysis submission and polling stay on the horizontally scalable Redis
    gateway. Memory, watch, and import endpoints retain their existing public
    URLs but execute only inside the private writer process that owns /data.
    """
    if not (
        SETTINGS.process_role == "gateway"
        and request.url.path.startswith(_AUTHORITATIVE_ROUTE_PREFIXES)
    ):
        return await call_next(request)
    target = f"{SETTINGS.writer_internal_url}{request.url.path}"
    if request.url.query:
        target = f"{target}?{request.url.query}"
    forwarded_headers = {
        name: value
        for name, value in request.headers.items()
        if name.lower()
        in {
            "authorization",
            "content-type",
            "x-chainseer-client",
            "x-request-id",
        }
    }
    try:
        async with httpx.AsyncClient(
            timeout=SETTINGS.writer_proxy_timeout_seconds,
            trust_env=False,
        ) as client:
            upstream = await client.request(
                request.method,
                target,
                headers=forwarded_headers,
                content=await request.body(),
            )
    except httpx.HTTPError:
        LOGGER.exception("Authoritative writer proxy request failed")
        return JSONResponse(
            {"detail": "authoritative service is temporarily unavailable"},
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            headers={"Retry-After": "5"},
        )
    response_headers = {}
    for name in ("content-type", "retry-after"):
        value = upstream.headers.get(name)
        if value:
            response_headers[name] = value
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        headers=response_headers,
    )


async def require_paper_telemetry_token(
    authorization: str | None = Header(default=None),
) -> None:
    if not SETTINGS.paper_telemetry_token:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="paper telemetry publisher is not configured")
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="missing paper telemetry credentials")
    if not hmac.compare_digest(authorization[7:], SETTINGS.paper_telemetry_token):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="invalid paper telemetry credentials")
# Health-check paths are exempt from Host validation below. Infrastructure
# health probes (Fly.io's proxy, Render's, etc.) routinely hit the app over
# an internal network path without setting a Host header that matches any
# externally-valid hostname -- that's normal for a trusted internal check,
# not a spoofing attempt. The endpoints themselves return no sensitive data,
# so exempting only these two paths doesn't weaken Host validation for
# anything that actually needs it.
_HOST_CHECK_EXEMPT_PATHS = {"/health/live", "/health/ready"}
ADMIN_IMPORT_MAX_REQUEST_BYTES = 3_000_000
PAPER_TELEMETRY_MAX_REQUEST_BYTES = 256_000


def _host_header_allowed(host_header: str, patterns: tuple[str, ...]) -> bool:
    host = host_header.split(":")[0].lower()
    for pattern in patterns:
        pattern = pattern.lower()
        if pattern.startswith("*."):
            suffix = pattern[1:]
            if host == pattern[2:] or host.endswith(suffix):
                return True
        elif host == pattern:
            return True
    return False


@app.middleware("http")
async def trusted_host_check(request: Request, call_next):
    if request.url.path not in _HOST_CHECK_EXEMPT_PATHS:
        if not _host_header_allowed(
            request.headers.get("host", ""), SETTINGS.allowed_hosts
        ):
            return JSONResponse(
                {"detail": "Invalid host header"},
                status_code=status.HTTP_400_BAD_REQUEST,
            )
    return await call_next(request)


def _max_request_bytes_for(path: str) -> int:
    # The ring-import batch endpoint carries full historical analysis
    # payloads (10-25KB each, up to 50 per batch) -- far past the
    # anti-abuse-sized default meant for /v1/analyses-style requests. It's
    # auth-gated the same as every other admin surface, so a higher,
    # still-bounded cap here doesn't loosen the guard on public routes.
    if path == "/v1/admin/rings/import":
        return ADMIN_IMPORT_MAX_REQUEST_BYTES
    if path == "/v1/paper/telemetry":
        return PAPER_TELEMETRY_MAX_REQUEST_BYTES
    return SETTINGS.max_request_bytes


@app.middleware("http")
async def security_headers(request: Request, call_next):
    request_id = request.headers.get("x-request-id") or secrets.token_hex(12)

    def finalize(response):
        response.headers["X-Request-ID"] = request_id[:128]
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    limit = _max_request_bytes_for(request.url.path)
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            length = int(content_length)
            if length < 0 or length > limit:
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
        if len(body) > limit:
            return finalize(
                JSONResponse(
                    {"detail": "request body is too large"},
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                )
            )
    response = await call_next(request)
    return finalize(response)


@app.get("/health/live")
async def live() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/ready")
async def ready() -> dict[str, Any]:
    if not SERVICE.ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="analysis worker is not ready",
        )
    health = SERVICE.health_status()
    return {
        "status": "ready",
        "queue_depth": (
            (health.get("shared_job_store") or {}).get("queue_depth")
            if (health.get("shared_job_store") or {}).get("queue_depth")
            is not None
            else SERVICE.work.qsize()
        ),
        "environment": SETTINGS.environment,
        "watcher_enabled": SETTINGS.watcher_enabled,
        "cognitive_completion_enabled": (
            SETTINGS.cognitive_completion_enabled
        ),
        "watcher_last_error": health["watcher_last_error"],
        "networks": ["robinhood", "base", "solana"],
        "base_rpc_configured": bool(SETTINGS.base_rpc_url),
        "solana_rpc_configured": bool(SETTINGS.solana_rpc_url),
        "benchmark_capture": health["benchmark_capture"],
        "timechain_integrity": health["timechain_integrity"],
        "cypher_tempre_runtime": health["cypher_tempre_runtime"],
        "maintenance_queue_depth": health["maintenance_queue_depth"],
        "shared_job_store": health.get(
            "shared_job_store",
            {"enabled": False, "backend": "process_local"},
        ),
        "timechain_writer": health.get("timechain_writer", {}),
        "maintenance_telemetry": health["maintenance_telemetry"],
        "faculty_pack": health["faculty_pack"],
        "memory": health["memory"],
        "runtime": health.get("runtime", {}),
    }


def _paper_telemetry_path() -> Path:
    return Path(SETTINGS.chain_root).resolve().parent / "paper_telemetry.json"


@app.get("/v1/paper/status", dependencies=[Depends(require_api_token)])
def get_paper_telemetry() -> dict[str, Any]:
    path = _paper_telemetry_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="paper telemetry is not connected") from exc
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="paper telemetry is unavailable") from exc
    if payload.get("paper_only") is not True:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="paper telemetry failed its safety boundary")
    payload["telemetry_age_seconds"] = round(max(0.0, time.time() - float(payload.get("published_at", 0))), 1)
    return payload


@app.post("/v1/paper/telemetry", dependencies=[Depends(require_paper_telemetry_token)])
def publish_paper_telemetry(payload: PaperTelemetryRequest) -> dict[str, Any]:
    # No analysis, Timechain, or trader state is modified: this is a compact
    # read model written atomically by the separate learner-side publisher.
    snapshot = dict(payload.payload)
    snapshot["published_at"] = time.time()
    encoded = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    if len(encoded.encode("utf-8")) > 256_000:
        raise HTTPException(status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, detail="paper telemetry is too large")
    path = _paper_telemetry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".next")
    temporary.write_text(encoded, encoding="utf-8")
    os.replace(temporary, path)
    return {"accepted": True, "positions": len(snapshot.get("positions", [])), "paper_only": True}


@app.post(
    "/v1/analyses",
    response_model=JobAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_api_token)],
)
def create_analysis(
    payload: AnalyzeRequest, request: Request
) -> JobAccepted:
    try:
        admitted = LIMITER.allow(request_identity(request))
    except SharedStoreUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
            headers={"Retry-After": "5"},
        ) from exc
    if not admitted:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="analysis rate limit exceeded",
            headers={"Retry-After": "60"},
        )
    try:
        return SERVICE.submit(
            payload.address,
            payload.network,
            force_refresh=payload.force_refresh,
        )
    except IntegrityUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
            headers={"Retry-After": "60"},
        ) from exc
    except QueueFullError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="analysis queue is full; try again shortly",
            headers={"Retry-After": "30"},
        ) from exc
    except SharedStoreUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
            headers={"Retry-After": "5"},
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
    try:
        job = SERVICE.get_public(job_id)
    except SharedStoreUnavailableError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
            headers={"Retry-After": "5"},
        ) from exc
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="analysis job not found",
        )
    return job


@app.post(
    "/v1/memory/query",
    dependencies=[Depends(require_api_token)],
)
def query_memory(
    payload: MemoryQueryRequest,
    request: Request,
) -> dict[str, Any]:
    if not LIMITER.allow(request_identity(request)):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="memory query rate limit exceeded",
            headers={"Retry-After": "60"},
        )
    try:
        return SERVICE.memory_query(
            payload.network,
            payload.address,
            topics=payload.topics,
            limit=payload.limit,
        )
    except MemoryCoreError as exc:
        LOGGER.error("Memory query integrity gate refused output: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="verified memory recall is temporarily unavailable",
        ) from exc


@app.get(
    "/v1/memory/status",
    dependencies=[Depends(require_api_token)],
)
def get_memory_status() -> dict[str, Any]:
    try:
        return SERVICE.memory_status()
    except (MemoryCoreError, RuntimeError) as exc:
        LOGGER.error("Memory Core status unavailable: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Timechain Memory Core status is unavailable",
        ) from exc


@app.get(
    "/v1/memory/citations/{ring_index}",
    dependencies=[Depends(require_api_token)],
)
def get_memory_citation(ring_index: int) -> dict[str, Any]:
    if ring_index < 0:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="memory citation not found",
        )
    try:
        return SERVICE.memory_citation(ring_index)
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="memory citation not found",
        ) from exc
    except MemoryCoreError as exc:
        LOGGER.error("Memory citation integrity gate refused output: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="verified memory citation is temporarily unavailable",
        ) from exc


@app.post(
    "/v1/admin/rings/import",
    dependencies=[Depends(require_api_token)],
)
def import_rings(payload: RingImportRequest) -> dict[str, Any]:
    try:
        results = SERVICE.import_base_analysis_rings(
            [item.model_dump() for item in payload.rings]
        )
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=str(exc),
        ) from exc
    return {"results": results}


@app.get(
    "/v1/watch",
    dependencies=[Depends(require_api_token)],
)
def get_watch_status(request: Request) -> dict[str, Any]:
    return SERVICE.watch_status(request_identity(request))


@app.post(
    "/v1/watch",
    dependencies=[Depends(require_api_token)],
)
def create_watch(payload: WatchRequest, request: Request) -> dict[str, Any]:
    identity = request_identity(request)
    try:
        subscription = SERVICE.watch_subscribe(
            payload.address, payload.network, identity
        )
    except WatcherBusyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="watcher state is busy; retry shortly",
            headers={"Retry-After": "5"},
        ) from exc
    return {
        "enabled": SETTINGS.watcher_enabled,
        "subscription": subscription,
    }


@app.get(
    "/v1/watch/alerts",
    dependencies=[Depends(require_api_token)],
)
def get_watch_alerts(
    request: Request,
    network: str,
    address: str,
    after: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    normalized_network = network.strip().lower()
    if normalized_network not in SUPPORTED_NETWORKS:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="unsupported watch network",
        )
    try:
        WatchRequest(network=normalized_network, address=address)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="invalid watch address",
        ) from exc
    if after:
        try:
            datetime.fromisoformat(after.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="invalid alert cursor",
            ) from exc
    if not 1 <= limit <= 100:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="alert limit must be between 1 and 100",
        )
    try:
        alerts = SERVICE.watch_alerts(
            address,
            normalized_network,
            request_identity(request),
            after=after,
            limit=limit,
        )
    except KeyError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="watch subscription not found",
        ) from exc
    cursor = alerts[-1]["observed_at"] if alerts else after
    return {
        "network": normalized_network,
        "address": address,
        "alerts": alerts,
        "cursor": cursor,
    }


@app.delete(
    "/v1/watch/{address}",
    dependencies=[Depends(require_api_token)],
)
def delete_watch(
    address: str,
    request: Request,
    network: str | None = None,
) -> dict[str, Any]:
    network = (
        network.strip().lower()
        if network
        else ("solana" if not ADDRESS_RE.fullmatch(address) else "robinhood")
    )
    if network not in SUPPORTED_NETWORKS:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="watch subscription not found",
        )
    if network == "solana":
        try:
            validate_solana_mint(address)
        except ValueError as exc:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="watch subscription not found",
            ) from exc
    elif not ADDRESS_RE.fullmatch(address):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="watch subscription not found",
        )
    try:
        removed = SERVICE.watch_unsubscribe(
            address, network, request_identity(request)
        )
    except WatcherBusyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="watcher state is busy; retry shortly",
            headers={"Retry-After": "5"},
        ) from exc
    return {
        "removed": removed,
        "address": address,
        "network": network,
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
