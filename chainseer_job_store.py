"""Optional shared job state for horizontally scaled Chainseer API replicas.

The scanner remains process-local for now. This store makes accepted jobs,
progress, completed results, hot-cache pointers, and active-scan leases visible
to every web replica. No network connection is created unless an explicit
Redis/Valkey URL is configured.
"""

from __future__ import annotations

import json
from typing import Any, Protocol


class SharedJobStore(Protocol):
    backend: str

    def put_job(
        self, job_id: str, value: dict[str, Any], ttl_seconds: int
    ) -> None: ...

    def get_job(self, job_id: str) -> dict[str, Any] | None: ...

    def put_cache(
        self, subject_key: str, job_id: str, ttl_seconds: int
    ) -> None: ...

    def get_cache(self, subject_key: str) -> str | None: ...

    def put_latest_result(
        self, subject_key: str, value: dict[str, Any], ttl_seconds: int
    ) -> None: ...

    def get_latest_result(
        self, subject_key: str
    ) -> dict[str, Any] | None: ...

    def claim_active(
        self, subject_key: str, job_id: str, ttl_seconds: int
    ) -> str: ...

    def release_active(self, subject_key: str, job_id: str) -> bool: ...

    def ping(self) -> bool: ...


class RedisJobStore:
    """Redis/Valkey-backed shared job state with atomic scan coalescing."""

    backend = "redis"
    _COMPARE_DELETE = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
            return redis.call('del', KEYS[1])
        end
        return 0
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        prefix: str = "chainseer",
        socket_timeout_seconds: float = 2.0,
        client: Any | None = None,
    ):
        normalized_prefix = str(prefix).strip().strip(":")
        if not normalized_prefix or len(normalized_prefix) > 80:
            raise ValueError("shared job-store prefix must contain 1-80 characters")
        self.prefix = normalized_prefix
        if client is not None:
            self._client = client
            return
        if not url:
            raise ValueError("a Redis/Valkey URL is required")
        try:
            import redis
        except ImportError as exc:  # pragma: no cover - deployment packaging guard
            raise RuntimeError(
                "redis package is required when CHAINSEER_SHARED_STORE_URL is set"
            ) from exc
        self._client = redis.Redis.from_url(
            url,
            decode_responses=True,
            socket_connect_timeout=socket_timeout_seconds,
            socket_timeout=socket_timeout_seconds,
            health_check_interval=30,
        )

    def _key(self, kind: str, identifier: str) -> str:
        return f"{self.prefix}:{kind}:{identifier}"

    def put_job(
        self, job_id: str, value: dict[str, Any], ttl_seconds: int
    ) -> None:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
        self._client.set(
            self._key("job", job_id),
            encoded,
            ex=max(1, int(ttl_seconds)),
        )

    def get_job(self, job_id: str) -> dict[str, Any] | None:
        raw = self._client.get(self._key("job", job_id))
        if not raw:
            return None
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    def put_cache(
        self, subject_key: str, job_id: str, ttl_seconds: int
    ) -> None:
        self._client.set(
            self._key("cache", subject_key),
            job_id,
            ex=max(1, int(ttl_seconds)),
        )

    def get_cache(self, subject_key: str) -> str | None:
        value = self._client.get(self._key("cache", subject_key))
        return str(value) if value else None

    def put_latest_result(
        self, subject_key: str, value: dict[str, Any], ttl_seconds: int
    ) -> None:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            default=str,
        )
        self._client.set(
            self._key("latest", subject_key),
            encoded,
            ex=max(1, int(ttl_seconds)),
        )

    def get_latest_result(
        self, subject_key: str
    ) -> dict[str, Any] | None:
        raw = self._client.get(self._key("latest", subject_key))
        if not raw:
            return None
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return None
        return value if isinstance(value, dict) else None

    def claim_active(
        self, subject_key: str, job_id: str, ttl_seconds: int
    ) -> str:
        key = self._key("active", subject_key)
        claimed = self._client.set(
            key,
            job_id,
            ex=max(1, int(ttl_seconds)),
            nx=True,
        )
        if claimed:
            return job_id
        existing = self._client.get(key)
        if existing:
            return str(existing)
        # The prior lease expired between SET NX and GET. Retry once so the
        # caller either owns the lease or receives its current owner.
        claimed = self._client.set(
            key,
            job_id,
            ex=max(1, int(ttl_seconds)),
            nx=True,
        )
        if claimed:
            return job_id
        existing = self._client.get(key)
        if not existing:
            raise RuntimeError("active-scan lease changed during acquisition")
        return str(existing)

    def release_active(self, subject_key: str, job_id: str) -> bool:
        deleted = self._client.eval(
            self._COMPARE_DELETE,
            1,
            self._key("active", subject_key),
            job_id,
        )
        return bool(deleted)

    def ping(self) -> bool:
        return bool(self._client.ping())


def create_shared_job_store(
    url: str,
    *,
    prefix: str = "chainseer",
    socket_timeout_seconds: float = 2.0,
) -> SharedJobStore | None:
    """Create and verify the opt-in shared store, or return local-only mode."""
    if not str(url or "").strip():
        return None
    store = RedisJobStore(
        str(url).strip(),
        prefix=prefix,
        socket_timeout_seconds=socket_timeout_seconds,
    )
    if not store.ping():
        raise RuntimeError("configured shared job store did not answer PING")
    return store
