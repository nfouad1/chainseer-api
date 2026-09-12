import asyncio
import tempfile
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from starlette.requests import Request

from chainseer_api import (
    AnalysisService,
    DistributedWriterLease,
    Job,
    Settings,
    SlidingWindowRateLimiter,
    gateway_authoritative_proxy,
)
from chainseer_job_store import RedisJobStore


TOKEN = "0x" + "12" * 20


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
        self.sorted_sets = {}
        self.lock = threading.Lock()

    def set(self, key, value, ex=None, nx=False):
        with self.lock:
            if nx and key in self.values:
                return False
            self.values[key] = value
            return True

    def get(self, key):
        with self.lock:
            return self.values.get(key)

    def eval(self, script, key_count, *args):
        with self.lock:
            keys = args[:key_count]
            argv = args[key_count:]
            if "zremrangebyscore" in script:
                identity_key, global_key = keys
                now_ms, window_ms, identity_limit, global_limit, member = argv
                cutoff = int(now_ms) - int(window_ms)
                for key in keys:
                    entries = self.sorted_sets.setdefault(key, {})
                    self.sorted_sets[key] = {
                        item: score
                        for item, score in entries.items()
                        if score > cutoff
                    }
                identity_entries = self.sorted_sets[identity_key]
                global_entries = self.sorted_sets[global_key]
                if len(identity_entries) >= int(identity_limit):
                    return 0
                if (
                    int(global_limit) > 0
                    and len(global_entries) >= int(global_limit)
                ):
                    return 0
                identity_entries[member] = int(now_ms)
                if int(global_limit) > 0:
                    global_entries[member] = int(now_ms)
                return 1
            if "lpos" in script and "lpush" in script:
                pending, processing = keys
                job_id, maximum_depth = argv
                pending_items = self.lists.setdefault(pending, [])
                processing_items = self.lists.setdefault(processing, [])
                if "lrem" in script:
                    processing_items[:] = [
                        item for item in processing_items if item != job_id
                    ]
                if job_id in pending_items or job_id in processing_items:
                    return 1
                if len(pending_items) + len(processing_items) >= int(maximum_depth):
                    return 0
                pending_items.insert(0, job_id)
                return 1
            key = keys[0]
            expected = argv[0]
            if self.values.get(key) != expected:
                return 0
            if "expire" in script:
                return 1
            del self.values[key]
            return 1

    def brpoplpush(self, source, destination, timeout=1):
        del timeout
        with self.lock:
            source_items = self.lists.setdefault(source, [])
            if not source_items:
                return None
            value = source_items.pop()
            self.lists.setdefault(destination, []).insert(0, value)
            return value

    def rpoplpush(self, source, destination):
        return self.brpoplpush(source, destination)

    def rpop(self, key):
        with self.lock:
            items = self.lists.setdefault(key, [])
            return items.pop() if items else None

    def rpush(self, key, value):
        with self.lock:
            items = self.lists.setdefault(key, [])
            items.append(value)
            return len(items)

    def lrem(self, key, count, value):
        with self.lock:
            items = self.lists.setdefault(key, [])
            removed = 0
            result = []
            for item in items:
                if item == value and (count == 0 or removed < count):
                    removed += 1
                else:
                    result.append(item)
            self.lists[key] = result
            return removed

    def llen(self, key):
        with self.lock:
            return len(self.lists.get(key, []))

    def ping(self):
        return True


def settings(root):
    return Settings(
        environment="test",
        api_token="",
        chain_root=str(root),
        queue_size=4,
        result_ttl_seconds=3600,
        cache_ttl_seconds=300,
        rate_limit_per_minute=6,
        shutdown_grace_seconds=10,
        watcher_enabled=False,
    )


def test_redis_store_round_trips_jobs_cache_and_owned_lease():
    store = RedisJobStore(client=FakeRedis(), prefix="test")

    store.put_job("job-1", {"status": "queued"}, 60)
    store.put_cache("robinhood:token", "job-1", 60)
    store.put_latest_result(
        "robinhood:token",
        {"result": {"score": 88}, "stored_at": 10.0},
        60,
    )

    assert store.get_job("job-1") == {"status": "queued"}
    assert store.get_cache("robinhood:token") == "job-1"
    assert store.get_latest_result("robinhood:token") == {
        "result": {"score": 88},
        "stored_at": 10.0,
    }
    assert store.claim_active("robinhood:token", "job-1", 60) == "job-1"
    assert store.claim_active("robinhood:token", "job-2", 60) == "job-1"
    assert not store.release_active("robinhood:token", "job-2")
    assert store.release_active("robinhood:token", "job-1")


def test_reliable_scan_queue_recovers_claimed_work_and_preserves_fifo():
    store = RedisJobStore(client=FakeRedis(), prefix="test")

    assert store.enqueue_scan("job-1", 2)
    assert store.enqueue_scan("job-1", 2)
    assert store.enqueue_scan("job-2", 2)
    assert not store.enqueue_scan("job-3", 2)
    assert store.scan_queue_depth() == 2
    assert store.claim_scan() == "job-1"
    assert store.recover_claimed_scans() == 1
    assert store.claim_scan() == "job-1"
    assert store.acknowledge_scan("job-1")
    assert store.claim_scan() == "job-2"
    assert store.acknowledge_scan("job-2")
    assert store.scan_queue_depth() == 0


def test_writer_lease_is_single_owner_and_compare_guarded():
    store = RedisJobStore(client=FakeRedis(), prefix="test")

    assert store.claim_writer("writer-1", 30)
    assert store.claim_writer("writer-1", 30)
    assert not store.claim_writer("writer-2", 30)
    assert not store.renew_writer("writer-2", 30)
    assert not store.release_writer("writer-2")
    assert store.release_writer("writer-1")
    assert store.claim_writer("writer-2", 30)


def test_two_api_replicas_share_polling_and_coalesce_active_scan():
    store = RedisJobStore(client=FakeRedis(), prefix="test")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        first = AnalysisService(
            settings(root / "replica-1"), shared_job_store=store
        )
        second = AnalysisService(
            settings(root / "replica-2"), shared_job_store=store
        )

        accepted = first.submit(TOKEN)
        remote_snapshot = second.get_public(accepted.job_id)
        duplicate = second.submit(TOKEN)

        assert remote_snapshot is not None
        assert remote_snapshot["status"] == "queued"
        assert duplicate.job_id == accepted.job_id
        assert duplicate.status == "queued"
        assert second.work.empty()


def test_completed_shared_job_is_a_cross_replica_cache_hit():
    store = RedisJobStore(client=FakeRedis(), prefix="test")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        first = AnalysisService(
            settings(root / "replica-1"), shared_job_store=store
        )
        second = AnalysisService(
            settings(root / "replica-2"), shared_job_store=store
        )
        accepted = first.submit(TOKEN)
        job = first.get(accepted.job_id)
        assert job is not None
        job.status = "succeeded"
        job.result = {"decision": {"score": 88}}
        first._publish_shared_job(job)
        first._persist_public_result(job)

        cached = second.submit(TOKEN)

        assert cached.cached
        assert cached.status == "succeeded"
        assert cached.job_id == job.id


def test_shared_work_queue_detaches_submission_from_writer_process():
    store = RedisJobStore(client=FakeRedis(), prefix="test")
    with tempfile.TemporaryDirectory() as directory:
        configured = replace(
            settings(Path(directory)),
            shared_store_url="redis://test",
            shared_work_queue_enabled=True,
        )
        gateway = AnalysisService(configured, shared_job_store=store)

        accepted = gateway.submit(TOKEN)

        assert gateway.work.empty()
        assert store.scan_queue_depth() == 1
        claimed = store.claim_scan()
        assert claimed == accepted.job_id
        snapshot = store.get_job(claimed)
        recovered = Job.from_public(snapshot)
        assert recovered.id == accepted.job_id
        assert recovered.address == TOKEN
        assert recovered.network == "robinhood"
        assert store.acknowledge_scan(claimed)


def test_gateway_start_is_redis_only_and_never_touches_chain_root():
    store = RedisJobStore(client=FakeRedis(), prefix="test")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory) / "must-not-exist"
        configured = replace(
            settings(root),
            process_role="gateway",
            shared_store_url="redis://test",
            shared_work_queue_enabled=True,
            benchmark_capture_enabled=True,
        )
        gateway = AnalysisService(configured, shared_job_store=store)

        gateway.start()
        try:
            assert gateway.ready
            assert gateway._agent is None
            assert gateway._deferred_queue is None
            assert (
                gateway.health_status()["runtime"]["process_role"]
                == "gateway"
            )
            assert not root.exists()
        finally:
            assert gateway.stop()


def test_gateway_polling_never_masks_writer_state_with_local_queued_job():
    store = RedisJobStore(client=FakeRedis(), prefix="test")
    with tempfile.TemporaryDirectory() as directory:
        configured = replace(
            settings(Path(directory)),
            process_role="gateway",
            shared_store_url="redis://test",
            shared_work_queue_enabled=True,
        )
        gateway = AnalysisService(configured, shared_job_store=store)

        accepted = gateway.submit(TOKEN, force_refresh=True)
        assert gateway.get(accepted.job_id) is None
        completed = store.get_job(accepted.job_id)
        completed["status"] = "succeeded"
        completed["result"] = {"decision": {"score": 88}}
        store.put_job(accepted.job_id, completed, 3600)

        assert gateway.get_public(accepted.job_id)["status"] == "succeeded"


def test_split_roles_fail_closed_without_shared_queue():
    with tempfile.TemporaryDirectory() as directory:
        for role in ("gateway", "writer"):
            configured = replace(
                settings(Path(directory) / role),
                process_role=role,
            )
            try:
                configured.validate()
            except RuntimeError as exc:
                assert "shared Redis work queue" in str(exc)
            else:  # pragma: no cover - explicit fail-closed assertion
                raise AssertionError(f"{role} accepted process-local state")


def test_rate_limits_are_atomic_across_gateway_replicas():
    store = RedisJobStore(client=FakeRedis(), prefix="test")
    first = SlidingWindowRateLimiter(
        2, global_limit=3, shared_store=store
    )
    second = SlidingWindowRateLimiter(
        2, global_limit=3, shared_store=store
    )

    assert first.allow("identity-a", now=100.0)
    assert second.allow("identity-a", now=100.1)
    assert not first.allow("identity-a", now=100.2)
    assert second.allow("identity-b", now=100.3)
    assert not first.allow("identity-c", now=100.4)
    assert first.allow("identity-c", now=161.0)


def test_fly_process_groups_isolate_gateway_from_timechain_volume():
    root = Path(__file__).resolve().parents[1]
    config = (root / "fly.toml").read_text(encoding="utf-8")

    assert "CHAINSEER_PROCESS_ROLE=gateway" in config
    assert "CHAINSEER_PROCESS_ROLE=writer" in config
    assert 'processes = ["gateway"]' in config
    assert 'processes = ["writer"]' in config
    mount = config.split("[[mounts]]", 1)[1].split("[http_service]", 1)[0]
    assert 'processes = ["writer"]' in mount
    service = config.split("[http_service]", 1)[1].split("[[vm]]", 1)[0]
    assert 'processes = ["gateway"]' in service


def test_gateway_proxies_disk_backed_routes_to_private_writer():
    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured["client"] = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def request(self, method, url, **kwargs):
            captured.update(method=method, url=url, request=kwargs)
            return httpx.Response(
                200,
                json={"state": "ready"},
                headers={"content-type": "application/json"},
            )

    messages = [{"type": "http.request", "body": b"", "more_body": False}]

    async def receive():
        return messages.pop(0)

    async def call_next(_request):  # pragma: no cover - must be bypassed
        raise AssertionError("gateway executed the disk-backed route locally")

    request = Request(
        {
            "type": "http",
            "method": "GET",
            "scheme": "https",
            "path": "/v1/memory/status",
            "raw_path": b"/v1/memory/status",
            "query_string": b"detail=1",
            "headers": [(b"authorization", b"Bearer test-token")],
            "client": ("127.0.0.1", 1234),
            "server": ("testserver", 443),
        },
        receive=receive,
    )
    configured = replace(
        settings(Path("gateway-proxy-test")),
        process_role="gateway",
        writer_internal_url="http://writer.internal:8000",
    )
    with patch("chainseer_api.SETTINGS", configured), patch(
        "chainseer_api.httpx.AsyncClient", FakeClient
    ):
        response = asyncio.run(
            gateway_authoritative_proxy(request, call_next)
        )

    assert response.status_code == 200
    assert response.body == b'{"state":"ready"}'
    assert captured["method"] == "GET"
    assert captured["url"] == (
        "http://writer.internal:8000/v1/memory/status?detail=1"
    )
    assert captured["request"]["headers"]["authorization"] == (
        "Bearer test-token"
    )


def test_distributed_writer_lease_refuses_second_process():
    store = RedisJobStore(client=FakeRedis(), prefix="test")
    first = DistributedWriterLease(
        store, owner_id="writer-1", ttl_seconds=30
    )
    second = DistributedWriterLease(
        store, owner_id="writer-2", ttl_seconds=30
    )

    first.acquire()
    try:
        try:
            second.acquire()
        except RuntimeError as exc:
            assert "another process" in str(exc)
        else:  # pragma: no cover - explicit assertion for lease safety
            raise AssertionError("second Timechain writer lease was accepted")
    finally:
        first.release()


def test_distributed_writer_lease_recovers_after_transient_loss():
    class RecoveryStore:
        def __init__(self):
            self.claims = 0
            self.renewals = 0

        def claim_writer(self, _owner, _ttl):
            self.claims += 1
            return True

        def renew_writer(self, _owner, _ttl):
            self.renewals += 1
            return False

        def release_writer(self, _owner):
            return True

    class BoundedWait:
        def __init__(self):
            self.calls = 0

        def wait(self, _seconds):
            self.calls += 1
            return self.calls > 2

    store = RecoveryStore()
    lease = DistributedWriterLease(
        store, owner_id="writer-1", ttl_seconds=10
    )
    lease._healthy.set()
    lease._last_confirmed_monotonic = time.monotonic() - 10
    lease._stopping = BoundedWait()

    lease._renew_loop()

    assert lease.healthy
    assert lease.recovery_count == 1
    assert store.renewals == 1
    assert store.claims == 1


def test_redelivered_scan_restores_existing_ring_instead_of_appending():
    existing = {
        "index": 42,
        "ring_hash": "a" * 64,
        "payload": {
            "idempotency_key": "public_analysis:" + ("1" * 32),
            "token_address": TOKEN,
            "evidence_hash": "b" * 64,
            "poq_verdict": {"decision": "SEAL", "cited_rings": []},
            "cognitive_loop": {"status": "prepared"},
        },
    }
    tc = SimpleNamespace(
        tail_rings=lambda _limit: [existing],
        iter_rings=lambda: iter([existing]),
    )
    sealing_agent = SimpleNamespace(
        calls=0,
        _seal_report=lambda *_args, **_kwargs: setattr(
            sealing_agent, "calls", sealing_agent.calls + 1
        ),
    )
    with tempfile.TemporaryDirectory() as directory:
        service = AnalysisService(settings(Path(directory)))
        service._agent = SimpleNamespace(tc=tc)
        report = {"token_address": TOKEN}

        service._seal_user_report_once(
            sealing_agent,
            report,
            "1" * 32,
            exhaustive_recovery=True,
        )

    assert sealing_agent.calls == 0
    assert report["analysis_ring"] == 42
    assert report["analysis_ring_hash"] == "a" * 64
    assert report["cognitive_completion"]["status"] == "queued"


def test_api_writer_replaces_buffered_append_with_fsynced_append():
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "rings.jsonl"
        attested = []
        tc = SimpleNamespace(
            rings_path=path,
            _append=lambda _ring: None,
            _auto_attest=attested.append,
        )
        agent = SimpleNamespace(tc=tc)
        ring = {"index": 1, "ring_type": "test", "payload": {"ok": True}}

        assert AnalysisService._install_durable_timechain_append(agent)
        assert not AnalysisService._install_durable_timechain_append(agent)
        with patch("chainseer_api.os.fsync") as fsync:
            tc._append(ring)

        fsync.assert_called_once()
        assert path.read_text(encoding="utf-8") == (
            '{"index": 1, "ring_type": "test", "payload": {"ok": true}}\n'
        )
        assert attested == [ring]
