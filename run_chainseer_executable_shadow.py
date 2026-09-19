"""Run the isolated V2 executable-shadow evidence sidecar.

It consumes only the already-running fresh-discovery ledger.  It does not
start discovery itself, so it cannot race the active V4 sampler, and it has no
paper or live execution capability.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from chainseer import ROBINHOOD_NETWORK, RobinhoodRPC
from chainseer_executable_shadow_v2 import V2ExecutableShadowV2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="robinhood_learning")
    parser.add_argument("--continuous", action="store_true")
    # The fixed freshness interval is 120 blocks (~12 seconds), so a slower
    # default would make the lane structurally unable to observe first liquidity.
    parser.add_argument("--cadence-seconds", type=float, default=5.0)
    parser.add_argument("--limit", type=int, default=12)
    options = parser.parse_args()
    root = Path(options.root)
    worker = V2ExecutableShadowV2(root, rpc=RobinhoodRPC(ROBINHOOD_NETWORK.rpc_url, timeout=8))
    while True:
        print(worker.run_once(entry_limit=options.limit, outcome_limit=options.limit), flush=True)
        if not options.continuous:
            return 0
        time.sleep(max(1.0, options.cadence_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
