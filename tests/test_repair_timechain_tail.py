import importlib.util
import json
import tempfile
import unittest
from pathlib import Path


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "scripts"
    / "repair_timechain_tail.py"
)
SPEC = importlib.util.spec_from_file_location("repair_timechain_tail", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


class RepairTimechainTailTests(unittest.TestCase):
    def test_accepts_only_nul_suffix_after_complete_ring(self):
        ring = {
            "index": 9,
            "ring_type": "experience",
            "prev_hash": "a" * 64,
            "ring_hash": "b" * 64,
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "rings.jsonl"
            valid = (json.dumps(ring) + "\n").encode("utf-8")
            path.write_bytes(valid + (b"\x00" * 29))
            candidate, suffix_bytes, last_ring = MODULE.inspect_tail(path)
            self.assertEqual(candidate, valid)
            self.assertEqual(suffix_bytes, 29)
            self.assertEqual(last_ring["index"], 9)

    def test_refuses_truncated_json_before_nul_suffix(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "rings.jsonl"
            path.write_bytes(b'{"index":9' + (b"\x00" * 4))
            with self.assertRaisesRegex(RuntimeError, "newline terminated"):
                MODULE.inspect_tail(path)


if __name__ == "__main__":
    unittest.main()
