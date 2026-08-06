"""Read-only export of base_launch_analysis rings from a local Timechain.

Streams chain_root/chain/rings.jsonl without taking any lock -- safe to run
while chainseer_base.py's learn-once/guard-once loops are actively writing
to the same root. Output feeds chainseer_memory_import_base.py's backfill
into the production Memory Core.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from chainseer import _get_skill_dir, _load_timechain_module, ensure_utf8_runtime

RING_TYPE = "base_launch_analysis"


def export_rings(chain_root: str | Path, skill_dir: str | None = None) -> list[dict]:
    skill_dir = skill_dir or _get_skill_dir()
    tc_module = _load_timechain_module(skill_dir)
    tc = tc_module.Timechain(root=Path(chain_root))
    exported = []
    for ring in tc.iter_rings():
        if ring.get("ring_type") != RING_TYPE:
            continue
        exported.append(
            {
                "index": ring["index"],
                "timestamp": ring["timestamp"],
                "payload": ring["payload"],
            }
        )
    return exported


def main() -> None:
    ensure_utf8_runtime()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chain-root", default="chainseer_chain")
    parser.add_argument("--out", default="base_launch_analysis_export.json")
    parser.add_argument("--skill-dir", default=None)
    args = parser.parse_args()

    exported = export_rings(args.chain_root, args.skill_dir)
    out_path = Path(args.out)
    out_path.write_text(json.dumps(exported, indent=2, sort_keys=True), encoding="utf-8")
    print(f"exported {len(exported)} {RING_TYPE} rings from {args.chain_root} -> {out_path}")


if __name__ == "__main__":
    main()
