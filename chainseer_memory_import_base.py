"""Drive the production POST /v1/admin/rings/import backfill.

Reads a chainseer_memory_export_base.py export and posts it in chunks to
the deployed chainseer-api, so a large local export never exceeds the
endpoint's request-size cap in one call. Requires CHAINSEER_API_TOKEN in
the environment (the same production token used for /v1/memory/*).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_URL = "https://chainseer-api.fly.dev/v1/admin/rings/import"
DEFAULT_CHUNK_SIZE = 20  # ~20 rings * ~25KB worst case stays well under the 3MB cap


def post_chunk(url: str, token: str, chunk: list[dict]) -> dict:
    body = json.dumps({"rings": chunk}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=240) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export", default="base_launch_analysis_export.json")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE)
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="only import the first N rings (for a canary run)",
    )
    args = parser.parse_args()

    token = os.environ.get("CHAINSEER_API_TOKEN", "").strip()
    if not token:
        print("CHAINSEER_API_TOKEN is not set", file=sys.stderr)
        raise SystemExit(1)

    rings = json.load(open(args.export, encoding="utf-8"))
    if args.limit is not None:
        rings = rings[: args.limit]
    items = [{"timestamp": r["timestamp"], "payload": r["payload"]} for r in rings]

    totals = {"sealed": 0, "duplicate": 0, "error": 0}
    for start in range(0, len(items), args.chunk_size):
        chunk = items[start : start + args.chunk_size]
        result = post_chunk(args.url, token, chunk)
        for entry in result["results"]:
            totals[entry["status"]] = totals.get(entry["status"], 0) + 1
            if entry["status"] == "error":
                print(f"  error: {entry.get('detail')}", file=sys.stderr)
        print(
            f"chunk {start}-{start + len(chunk)}: "
            f"sealed={sum(1 for e in result['results'] if e['status'] == 'sealed')} "
            f"duplicate={sum(1 for e in result['results'] if e['status'] == 'duplicate')} "
            f"error={sum(1 for e in result['results'] if e['status'] == 'error')}"
        )

    print(f"TOTAL: {totals}")


if __name__ == "__main__":
    main()
