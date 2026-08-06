"""Apply the optional-timestamp addition to the vendored, pinned timechain.py.

Dockerfile.api downloads a pinned upstream commit of the Cypher Tempre
Timechain rather than this machine's locally-evolving skill copy, so the
timestamp-override change made to the local skill (for replaying historical
Base-chain analyses with their real timestamps) has to be re-applied to
whatever timechain.py the pinned commit ships. This does an exact,
count-checked string replacement so a future commit bump that changes this
code breaks the build loudly instead of silently no-op'ing.
"""

from __future__ import annotations

import sys
from pathlib import Path

SEAL_SIGNATURE_OLD = (
    "    def seal(self, ring_type: str, payload: dict, files=None,\n"
    "             poq=None, difficulty: int = 0) -> dict:\n"
)
SEAL_SIGNATURE_NEW = (
    "    def seal(self, ring_type: str, payload: dict, files=None,\n"
    "             poq=None, difficulty: int = 0, timestamp: str | None = None) -> dict:\n"
)

RING_TIMESTAMP_OLD = (
    '            "timestamp": now_iso(),\n'
    '            "prev_hash": prev["ring_hash"],\n'
)
RING_TIMESTAMP_NEW = (
    '            "timestamp": timestamp if timestamp is not None else now_iso(),\n'
    '            "prev_hash": prev["ring_hash"],\n'
)


def apply_patch(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    for old, _new in ((SEAL_SIGNATURE_OLD, SEAL_SIGNATURE_NEW), (RING_TIMESTAMP_OLD, RING_TIMESTAMP_NEW)):
        count = text.count(old)
        if count != 1:
            raise SystemExit(
                f"expected exactly 1 match for a timechain.py seal() patch anchor, found {count}. "
                "The pinned Cypher Tempre commit's seal() no longer matches this patch -- "
                "update scripts/patch_timechain_seal_timestamp.py."
            )
    text = text.replace(SEAL_SIGNATURE_OLD, SEAL_SIGNATURE_NEW, 1)
    text = text.replace(RING_TIMESTAMP_OLD, RING_TIMESTAMP_NEW, 1)
    path.write_text(text, encoding="utf-8")


if __name__ == "__main__":
    apply_patch(Path(sys.argv[1]))
