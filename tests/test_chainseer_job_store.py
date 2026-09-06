import tempfile
import threading
from pathlib import Path

from chainseer_api import AnalysisService, Settings
from chainseer_job_store import RedisJobStore


TOKEN = "0x" + "12" * 20


class FakeRedis:
    def __init__(self):
        self.values = {}
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

    def eval(self, script, key_count, key, expected):
        assert key_count == 1
        with self.lock:
            if self.values.get(key) != expected:
                return 0
            del self.values[key]
            return 1

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
