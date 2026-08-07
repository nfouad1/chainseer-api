import json
import tempfile
import threading
import unittest
import unittest.mock
from pathlib import Path

from chainseer_core import atomic_json_write, read_json


class AtomicJsonWriteTests(unittest.TestCase):
    """Concurrent writers must never blend or lose each other's content.

    Pons writes admission_state.json from two processes -- the learn cycle and
    the quote guard. With a fixed "<name>.tmp" scratch file they shared one
    path, so the sequence "A writes scratch, B overwrites the same scratch, B
    replaces, A replaces" either raised FileNotFoundError in A (loud) or gave
    A's destination B's content (silent, and worse).
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_write_then_read_round_trips(self):
        target = self.root / "state.json"
        atomic_json_write(target, {"b": 2, "a": [1, 2, 3]})
        self.assertEqual(read_json(target), {"a": [1, 2, 3], "b": 2})

    def test_no_scratch_file_is_left_behind(self):
        target = self.root / "state.json"
        atomic_json_write(target, {"a": 1})
        self.assertEqual([p.name for p in self.root.iterdir()], ["state.json"])

    def test_a_failed_write_leaves_no_litter(self):
        target = self.root / "state.json"

        class Unserializable:
            pass

        with self.assertRaises(TypeError):
            atomic_json_write(target, {"bad": Unserializable()}, default=None)
        # Unique scratch names are never reused, so a failure that left one
        # behind would accumulate a new stray file on every retry.
        self.assertEqual(list(self.root.iterdir()), [])

    def test_scratch_path_is_unique_per_call(self):
        target = self.root / "state.json"
        seen = []
        real = Path.replace

        def capture(self, other):
            seen.append(self.name)
            return real(self, other)

        with unittest.mock.patch.object(Path, "replace", capture):
            atomic_json_write(target, {"a": 1})
            atomic_json_write(target, {"a": 2})
        self.assertEqual(len(seen), 2)
        self.assertNotEqual(seen[0], seen[1], "scratch name was reused")
        self.assertNotIn("state.json.tmp", seen)

    def test_interleaved_writers_never_blend_or_vanish(self):
        """The exact sequence that broke Pons, driven from many threads.

        Every writer must end with the destination holding exactly one
        writer's complete document -- never a mix, never a partial file, and
        never a FileNotFoundError from a scratch file another writer renamed.
        """
        target = self.root / "state.json"
        payloads = [{"writer": i, "body": [i] * 40} for i in range(12)]
        errors: list[BaseException] = []
        barrier = threading.Barrier(len(payloads))

        def write(payload):
            try:
                barrier.wait(timeout=10)
                for _ in range(15):
                    atomic_json_write(target, payload)
            except BaseException as exc:       # noqa: BLE001 - recorded below
                errors.append(exc)

        threads = [
            threading.Thread(target=write, args=(payload,))
            for payload in payloads
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        self.assertEqual(errors, [], f"concurrent writes raised: {errors[:3]}")
        final = json.loads(target.read_text(encoding="utf-8"))
        # Last writer wins -- that is expected. What must hold is that the
        # winner is ONE writer's intact document.
        self.assertIn(final, payloads)
        self.assertEqual(
            [p.name for p in self.root.iterdir()],
            ["state.json"],
            "scratch files were left behind",
        )


if __name__ == "__main__":
    unittest.main()
