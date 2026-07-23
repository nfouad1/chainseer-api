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
from pydantic import BaseModel, Field, field_validator
from starlette.responses import JSONResponse

from chainseer import Chainseer

LOGGER = logging.getLogger("chainseer.api")
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise RuntimeError(f"{name} must be between {minimum} and {maximum}")
    return value


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

    def validate(self) -> None:
        if self.environment not in {"development", "test", "production"}:
            raise RuntimeError(
                "CHAINSEER_ENVIRONMENT must be development, test, or production"
            )
        rpc = urlparse(self.rpc_url)
        if rpc.scheme not in {"http", "https"} or not rpc.hostname:
            raise RuntimeError("CHAINSEER_RPC_URL must be an HTTP(S) URL")
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


class AnalyzeRequest(BaseModel):
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
    status: str = "queued"
    created_at: float = field(default_factory=time.time)
    started_at: float | None = None
    finished_at: float | None = None
    result: dict[str, Any] | None = None
    error_code: str | None = None
    error_message: str | None = None

    def public(self) -> dict[str, Any]:
        return {
            "job_id": self.id,
            "address": self.address,
            "status": self.status,
            "created_at": _iso(self.created_at),
            "started_at": _iso(self.started_at),
            "finished_at": _iso(self.finished_at),
            "result": self.result,
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
        self._stopping = threading.Event()
        self._ready = threading.Event()

    def start(self) -> None:
        if self._worker and self._worker.is_alive():
            return
        self._stopping.clear()
        if self._agent is None:
            self._agent = Chainseer(
                rpc_url=self.settings.rpc_url,
                chain_root=self.settings.chain_root,
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

    def submit(self, address: str) -> JobAccepted:
        normalized = address.lower()
        now = time.time()
        with self._lock:
            self._prune(now)
            cached = self.cache.get(normalized)
            if cached and cached[0] > now:
                job = Job(
                    id=uuid.uuid4().hex,
                    address=address,
                    status="succeeded",
                    started_at=now,
                    finished_at=now,
                    result=cached[1],
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

            job = Job(id=uuid.uuid4().hex, address=address)
            self.jobs[job.id] = job
            self.active_by_address[normalized] = job.id
            self.work.put_nowait(job.id)
            return JobAccepted(job_id=job.id, status=job.status)

    def get(self, job_id: str) -> Job | None:
        with self._lock:
            self._prune(time.time())
            return self.jobs.get(job_id)

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
        while not self._stopping.is_set():
            job_id = self.work.get()
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
                report = self._agent.analyze_token(
                    job.address, full_report=False
                )
                if report.get("error"):
                    raise PublicAnalysisError(
                        "analysis_rejected", str(report["error"])
                    )
                public_report = build_public_report(report)
                with self._lock:
                    job.result = public_report
                    job.status = "succeeded"
                    if self.settings.cache_ttl_seconds:
                        self.cache[job.address.lower()] = (
                            time.time()
                            + self.settings.cache_ttl_seconds,
                            public_report,
                        )
            except PublicAnalysisError as exc:
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
                        job.address.lower(), None
                    )
                self.work.task_done()


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
        self.handle = self.path.open("a+", encoding="utf-8")
        try:
            if os.name == "nt":
                import msvcrt

                self.handle.seek(0)
                if self.handle.read(1) == "":
                    self.handle.write(" ")
                    self.handle.flush()
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

        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(
            f"pid={os.getpid()} host={socket.gethostname()} "
            f"acquired={datetime.now(timezone.utc).isoformat()}\n"
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

    return {
        "schema_version": "1.0",
        "token": {
            "address": report.get("token_address"),
            "name": report.get("token_name") or basic.get("name"),
            "symbol": report.get("token_symbol") or basic.get("symbol"),
            "chain": "Robinhood Chain",
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
            "liquidity_usd": dex.get("total_liquidity_usd"),
            "volume_24h_usd": dex.get("total_volume_24h"),
            "age": dex.get("token_age_label"),
        },
        "evidence": {
            "fact_count": provenance.get("fact_count", 0),
            "block_pin": provenance.get("block_pin"),
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
    allow_methods=["GET", "POST"],
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
        return SERVICE.submit(payload.address)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "chainseer_api:app",
        host=os.environ.get("CHAINSEER_API_HOST", "127.0.0.1"),
        port=_server_port(),
        workers=1,
        reload=False,
    )
