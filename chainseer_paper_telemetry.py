"""Publish a bounded, read-only paper-book view without touching the learner."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import time
from pathlib import Path

import requests


def _number(value):
    return float(value) if value is not None else None


def snapshot(root: Path) -> dict:
    database = root / "learning.sqlite3"
    connection = sqlite3.connect(
        f"file:{database.as_posix()}?mode=ro", uri=True, timeout=5,
    )
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """SELECT p.token_address,p.symbol,p.entry_price_usd,p.last_price_usd,
                      p.entry_market_cap_usd,p.last_market_cap_usd,p.net_multiple,
                      p.cost_usd,p.last_verified_mark_at,p.verified_mark_count,
                      c.name FROM positions p LEFT JOIN candidates c USING(token_address)
                 WHERE p.status='open' ORDER BY p.opened_at DESC LIMIT 250"""
        ).fetchall()
        closed = connection.execute(
            """SELECT COUNT(*) closed,SUM(net_multiple>1) winners,
                      SUM(exit_value_usd-cost_usd) net_pnl_usd FROM positions
                 WHERE status='closed'"""
        ).fetchone()
    finally:
        connection.close()
    positions = []
    for row in rows:
        multiple = _number(row["net_multiple"])
        positions.append({
            "token_address": row["token_address"], "symbol": row["symbol"],
            "name": row["name"], "entry_price_usd": _number(row["entry_price_usd"]),
            "current_price_usd": _number(row["last_price_usd"]),
            "entry_market_cap_usd": _number(row["entry_market_cap_usd"]),
            "current_market_cap_usd": _number(row["last_market_cap_usd"]),
            "gain_pct": None if multiple is None else (multiple - 1) * 100,
            "market_observation_verified": bool(row["verified_mark_count"]),
            "market_observed_at": _number(row["last_verified_mark_at"]),
        })
    return {
        "schema_version": 1, "paper_only": True, "positions": positions,
        "closed_performance": {key: _number(closed[key]) for key in closed.keys()},
        "wallet_cohort": {},
    }


def publish(root: Path, url: str, token: str) -> None:
    response = requests.post(
        url, json={"payload": snapshot(root)}, timeout=(3, 10),
        headers={"Authorization": f"Bearer {token}", "X-Request-ID": os.urandom(12).hex()},
    )
    response.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="robinhood_learning")
    parser.add_argument("--interval-seconds", type=float, default=30.0)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    url = os.environ.get("CHAINSEER_PAPER_TELEMETRY_URL", "").strip()
    token = os.environ.get("CHAINSEER_PAPER_TELEMETRY_TOKEN", "").strip()
    if not url or not token:
        raise SystemExit("Set CHAINSEER_PAPER_TELEMETRY_URL and CHAINSEER_PAPER_TELEMETRY_TOKEN.")
    root = Path(args.root)
    while True:
        try:
            publish(root, url, token)
        except (OSError, requests.RequestException, sqlite3.Error) as error:
            print(f"paper telemetry publish deferred: {type(error).__name__}", flush=True)
        if args.once:
            return 0
        time.sleep(max(10.0, args.interval_seconds))


if __name__ == "__main__":
    raise SystemExit(main())
