"""Repair only a NUL-padded Timechain JSONL suffix, with strict guards.

This utility intentionally refuses truncated JSON, invalid UTF-8, embedded
NULs, hash mismatches, or any corruption other than one or more NUL bytes
appended after a complete newline-terminated JSON ring.  Dry-run is the
default.  The caller must opt into ``--apply`` and provide a backup path.
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path


def inspect_tail(path: Path) -> tuple[bytes, int, dict]:
    raw = path.read_bytes()
    candidate = raw.rstrip(b"\x00")
    suffix_bytes = len(raw) - len(candidate)
    if suffix_bytes <= 0:
        raise RuntimeError("No trailing NUL suffix was found; refusing repair")
    if not candidate.endswith(b"\n"):
        raise RuntimeError(
            "The bytes before the NUL suffix are not newline terminated; "
            "refusing repair"
        )
    records = candidate[:-1].rsplit(b"\n", 1)
    last_line = records[-1]
    if not last_line:
        raise RuntimeError("No complete JSON record precedes the NUL suffix")
    try:
        last_ring = json.loads(last_line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "The record before the NUL suffix is not valid UTF-8 JSON; "
            "refusing repair"
        ) from exc
    required = {"index", "ring_type", "ring_hash", "prev_hash"}
    if not required.issubset(last_ring):
        raise RuntimeError("The last JSON object is not a complete Timechain ring")
    return candidate, suffix_bytes, last_ring


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup", type=Path)
    args = parser.parse_args()

    rings_path = args.root / "chain" / "rings.jsonl"
    if not rings_path.is_file():
        raise SystemExit(f"Timechain rings file not found: {rings_path}")
    candidate, suffix_bytes, last_ring = inspect_tail(rings_path)
    print(
        f"trailing_nul_bytes={suffix_bytes} "
        f"last_ring={last_ring['index']} "
        f"ring_hash={last_ring['ring_hash']} "
        f"candidate_size={len(candidate)}"
    )
    if not args.apply:
        print("dry_run=true; no bytes changed")
        return
    if args.backup is None:
        raise SystemExit("--backup is required with --apply")
    if args.backup.exists():
        raise SystemExit(f"Backup path already exists: {args.backup}")
    shutil.copy2(rings_path, args.backup)
    with rings_path.open("r+b") as handle:
        handle.truncate(len(candidate))
        handle.flush()
    repaired, remaining, repaired_ring = inspect_tail(args.backup)
    if len(repaired) != len(candidate) or remaining != suffix_bytes:
        raise RuntimeError("Backup verification failed after repair")
    if rings_path.read_bytes() != candidate:
        raise RuntimeError("Repaired file does not match the guarded candidate")
    print(
        f"applied=true backup={args.backup} "
        f"last_ring={repaired_ring['index']} new_size={rings_path.stat().st_size}"
    )


if __name__ == "__main__":
    main()
