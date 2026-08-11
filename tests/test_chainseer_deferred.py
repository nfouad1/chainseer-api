import tempfile
import unittest
from pathlib import Path

from chainseer_deferred import DurableDeferredQueue


def payload(anchor, observed=1.0, report_hash="hash"):
    return {
        "anchor_value": anchor,
        "observed_at_epoch": observed,
        "report_hash": report_hash,
    }


class DurableDeferredQueueTests(unittest.TestCase):
    def test_coalesces_to_freshest_subject_generation(self):
        with tempfile.TemporaryDirectory() as root:
            queue = DurableDeferredQueue(Path(root) / "queue.sqlite3")
            self.assertEqual(
                queue.enqueue(
                    kind="watcher_commit", subject_key="base:0x1",
                    payload=payload(10), priority=20, now=1.0,
                ),
                1,
            )
            self.assertEqual(
                queue.enqueue(
                    kind="watcher_commit", subject_key="base:0x1",
                    payload=payload(11), priority=20, now=1.1,
                ),
                2,
            )
            item = queue.claim(now=2.0)
            self.assertEqual(item.payload["anchor_value"], 11)

    def test_stale_delivery_never_replaces_newer_work(self):
        with tempfile.TemporaryDirectory() as root:
            queue = DurableDeferredQueue(Path(root) / "queue.sqlite3")
            queue.enqueue(
                kind="watcher_commit", subject_key="solana:mint",
                payload=payload(500), priority=20, now=1.0,
            )
            generation = queue.enqueue(
                kind="watcher_commit", subject_key="solana:mint",
                payload=payload(499, 99.0), priority=20, now=2.0,
            )
            self.assertEqual(generation, 1)
            self.assertEqual(
                queue.get("watcher_commit", "solana:mint").payload["anchor_value"],
                500,
            )

    def test_survives_process_reopen(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / "queue.sqlite3"
            DurableDeferredQueue(path).enqueue(
                kind="watcher_commit", subject_key="robinhood:0x2",
                payload=payload(12), priority=20, now=1.0,
            )
            reopened = DurableDeferredQueue(path)
            self.assertEqual(reopened.claim(now=2.0).payload["anchor_value"], 12)

    def test_expired_lease_is_recovered(self):
        with tempfile.TemporaryDirectory() as root:
            queue = DurableDeferredQueue(Path(root) / "queue.sqlite3")
            queue.enqueue(
                kind="watcher_commit", subject_key="base:0x3",
                payload=payload(13), priority=20, now=1.0,
            )
            first = queue.claim(now=2.0, lease_seconds=1.0)
            self.assertIsNotNone(first)
            self.assertIsNone(queue.claim(now=2.5))
            recovered = queue.claim(now=3.1)
            self.assertEqual(recovered.generation, first.generation)
            self.assertEqual(recovered.attempts, 2)

    def test_old_generation_cannot_ack_replacement(self):
        with tempfile.TemporaryDirectory() as root:
            queue = DurableDeferredQueue(Path(root) / "queue.sqlite3")
            queue.enqueue(
                kind="watcher_commit", subject_key="base:0x4",
                payload=payload(14), priority=20, now=1.0,
            )
            old = queue.claim(now=2.0)
            queue.enqueue(
                kind="watcher_commit", subject_key="base:0x4",
                payload=payload(15), priority=20, now=3.0,
            )
            self.assertFalse(queue.transition(old, "done"))
            self.assertEqual(
                queue.get("watcher_commit", "base:0x4").state,
                "pending",
            )


if __name__ == "__main__":
    unittest.main()
