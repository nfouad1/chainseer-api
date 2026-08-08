"""Incremental verification must re-anchor when it falls behind, not lock down.

The ledger has no exclusive process lease: the bot, the dashboards and three
per-chain learners all seal into it. A long-lived process therefore drifts past
``maximum_new_rings`` just by staying up. Treating that drift as tampering once
locked a provably intact 2,551-ring chain and stopped all sealing estate-wide,
so these tests pin the recovery rather than the symptom.

They run against a REAL Timechain in a temp dir -- real sealing, real hashing,
real ``verify()`` -- because the whole question is whether the fallback trusts
the right authority, and a stubbed ``verify()`` would be the test asserting its
own premise.
"""
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path

import chainseer

SKILL_DIR = str(Path.home() / ".zcode" / "skills" / "cypher-tempre-self-model")


def _timechain_module():
    if SKILL_DIR not in sys.path:
        sys.path.insert(0, SKILL_DIR)
    import timechain

    return timechain


class SpanOverflowReanchorTest(unittest.TestCase):
    """A wide span on an intact chain is drift, not damage."""

    def setUp(self):
        self.timechain_module = _timechain_module()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.tc = self.timechain_module.Timechain(self.root)
        # attach_registries=False: the faculty registries belong to a full
        # cognitive loop, and this test deliberately builds none.
        self.tc.genesis("test-chain", attach_registries=False)

        # A bare instance with a real Timechain attached. verify_incremental
        # touches only these four attributes; building the full loop would
        # bootstrap the faculty registry, epochs and immune system, none of
        # which participate in the decision under test.
        self.loop = chainseer.ChainseerCognitiveLoop.__new__(
            chainseer.ChainseerCognitiveLoop
        )
        self.loop._integrity_lock = threading.RLock()
        self.loop._trusted_head = None
        self.loop.timechain_module = self.timechain_module
        self.loop.recall = type("R", (), {"tc": self.tc})()

    def _seal(self, count):
        for i in range(count):
            self.tc.seal("test_ring", {"n": i})

    def _anchor_here(self):
        tail = self.tc.tail_rings(1)
        self.loop._trusted_head = (
            int(tail[-1]["index"]),
            str(tail[-1]["ring_hash"]),
        )

    def test_span_over_bound_reanchors_on_intact_chain(self):
        """The regression: drift past the bound must recover, not fail."""
        self._seal(3)
        self._anchor_here()
        anchored_at = self.loop._trusted_head[0]
        self._seal(20)                      # 20 new rings against a bound of 5

        ok, report = self.loop.verify_incremental(maximum_new_rings=5)

        self.assertTrue(ok, f"intact chain rejected as tampered: {report}")
        head = int(self.tc.tail_rings(1)[-1]["index"])
        self.assertEqual(
            self.loop._trusted_head[0], head,
            "anchor must advance to the verified head, or the very next call "
            "overflows again and the process is stuck in a recovery loop",
        )
        self.assertGreater(head, anchored_at)
        self.assertTrue(
            any("re-established" in line for line in report),
            f"recovery should say so in the report: {report}",
        )

    def test_span_over_bound_still_fails_on_damaged_chain(self):
        """The fallback must not become a way to launder a broken chain."""
        self._seal(3)
        self._anchor_here()
        self._seal(20)

        # Corrupt a sealed ring's payload, leaving its stored hash stale.
        # Done by parsing rather than string replacement: the obvious
        # `.replace('"n": 7', ...)` silently matches nothing (genesis occupies
        # index 0, so the payload numbering is offset), which makes the tamper
        # a no-op and the whole test vacuous.
        rings_path = self.root / "chain" / "rings.jsonl"
        lines = rings_path.read_text(encoding="utf-8").splitlines()
        target = json.loads(lines[10])
        original_hash = target["ring_hash"]
        target["payload"]["n"] = 9999
        lines[10] = json.dumps(target)
        rings_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        reread = json.loads(
            rings_path.read_text(encoding="utf-8").splitlines()[10]
        )
        self.assertEqual(reread["payload"]["n"], 9999, "tamper did not apply")
        self.assertEqual(
            reread["ring_hash"], original_hash,
            "the stored hash must be left stale -- that is the tamper",
        )

        ok, report = self.loop.verify_incremental(maximum_new_rings=5)

        self.assertFalse(
            ok, "a tampered chain must still be rejected after the fallback"
        )

    def test_span_within_bound_is_unchanged(self):
        """The ordinary incremental path keeps working."""
        self._seal(3)
        self._anchor_here()
        self._seal(2)

        ok, report = self.loop.verify_incremental(maximum_new_rings=5)

        self.assertTrue(ok, report)
        self.assertTrue(
            any("incrementally verified" in line for line in report),
            f"small spans should use the incremental path, not the full "
            f"walk: {report}",
        )

    def test_regressed_head_still_rejected(self):
        """Re-anchoring must not paper over a head that moved backwards."""
        self._seal(10)
        self._anchor_here()
        # Anchor ahead of the real head, as if the chain had been truncated.
        self.loop._trusted_head = (
            self.loop._trusted_head[0] + 50,
            self.loop._trusted_head[1],
        )

        ok, report = self.loop.verify_incremental(maximum_new_rings=5)

        self.assertFalse(ok, f"a regressed head must be rejected: {report}")


if __name__ == "__main__":
    unittest.main()
