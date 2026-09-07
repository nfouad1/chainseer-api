import tempfile
import threading
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from chainseer_api import (
    AnalysisService,
    DistributedWriterLease,
    Job,
    Settings,
)
from chainseer_job_store import RedisJobStore


TOKEN = "0x" + "12" * 20


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}
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
