"""Robinhood Chain launch learning and counterfactual paper trading.

Paper-only by construction: this module contains no private-key, signing,
approval, transaction-submission, or broadcast path. Discovery is isolated
from expensive analysis so new-pair intake remains bounded and observable.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import random
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import requests

from chainseer import (
    ADDRESS_RE,
    ROBINHOOD_NETWORK,
    UNISWAP_V2_FACTORY,
    UNISWAP_V4_POOL_MANAGER,
    WETH_ADDRESS,
    Chainseer,
    RobinhoodRPC,
    ensure_utf8_runtime,
)
from chainseer_base import LearningRunLock
from chainseer_core import atomic_json_write, read_json, safe_float, safe_int


PAIR_CREATED_TOPIC = (
    "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
)
USDG_ADDRESS = "0x5fc5360D0400a0Fd4f2af552ADD042D716F1d168"
UNISWAP_V4_STATE_VIEW = "0xf3334192d15450cdd385c8b70e03f9a6bd9e673b"
V4_INITIALIZE_TOPIC = "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438"
V4_MODIFY_LIQUIDITY_TOPIC = "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec"
V4_SWAP_TOPIC = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
V4_GET_LIQUIDITY_SELECTOR = "fa6793d5"
V4_GET_SLOT0_SELECTOR = "c815641c"
SOURCE_V2 = "uniswap_v2"
SOURCE_V4 = "uniswap_v4"
HORIZONS = (
    ("15m", 15 * 60),
    ("1h", 60 * 60),
    ("6h", 6 * 60 * 60),
    ("24h", 24 * 60 * 60),
    ("7d", 7 * 24 * 60 * 60),
)
DEFAULT_ROOT = "robinhood_learning"
DEFAULT_CHAIN_ROOT = "robinhood_learning_chain"
DEFAULT_DASHBOARD_PORT = 8769
DEFAULT_DISCOVERY_LOOKBACK_BLOCKS = 5_000
DEFAULT_DISCOVERY_BLOCK_LIMIT = 5_000
DEFAULT_ANALYSIS_LIMIT = 1
DEFAULT_OUTCOME_LIMIT = 12
MAXIMUM_ANALYSIS_ATTEMPTS = 3
OUTCOME_RETRY_SECONDS = 60
MINIMUM_ENTRY_SCORE = 70.0
MINIMUM_ENTRY_LIQUIDITY_USD = 10_000.0
PAPER_COST_USD = 100.0
MAXIMUM_POSITIONS = 50
STOP_LOSS_MULTIPLE = 0.65
TAKE_PROFIT_MULTIPLE = 3.0
MAXIMUM_HOLD_SECONDS = 7 * 24 * 60 * 60
ZERO_ADDRESS = "0x" + "0" * 40
REMOTE_RETRY_ATTEMPTS = 4
REMOTE_RETRY_BASE_SECONDS = 0.25
REMOTE_RETRY_MAX_SECONDS = 2.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _topic_address(value: str) -> str:
    text = str(value or "").removeprefix("0x")
    if len(text) != 64:
        return ZERO_ADDRESS
    return "0x" + text[-40:]


def _data_address(value: str, word: int = 0) -> str:
    text = str(value or "").removeprefix("0x")
    start = word * 64
    if len(text) < start + 64:
        return ZERO_ADDRESS
    return "0x" + text[start + 24:start + 64]


def _data_word(value: str, word: int = 0) -> int:
    text = str(value or "").removeprefix("0x")
    start = word * 64
    return int(text[start:start + 64], 16) if len(text) >= start + 64 else 0


def _signed_word(value: str, word: int, bits: int) -> int:
    raw = _data_word(value, word) & ((1 << bits) - 1)
    return raw - (1 << bits) if raw & (1 << (bits - 1)) else raw


def _timestamp(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _remote_call(operation: str, callback, *, attempts: int = REMOTE_RETRY_ATTEMPTS):
    """Retry a bounded remote read without changing any durable cursor state."""
    last_error: Exception | None = None
    for attempt in range(max(1, attempts)):
        try:
            return callback()
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= max(1, attempts):
                break
            ceiling = min(
                REMOTE_RETRY_MAX_SECONDS,
                REMOTE_RETRY_BASE_SECONDS * (2**attempt),
            )
            time.sleep(ceiling + random.uniform(0.0, ceiling * 0.2))
    raise RuntimeError(
        f"{operation} failed after {max(1, attempts)} attempts: {last_error}"
    ) from last_error


class HashEventLedger:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> list[dict]:
        if not self.path.exists():
            return []
        with self.path.open("r", encoding="utf-8") as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def append(self, event_type: str, payload: dict) -> dict:
        rows = self.load()
        event = {
            "index": len(rows),
            "event_type": event_type,
            "timestamp": _utc_now(),
            "previous_hash": rows[-1]["event_hash"] if rows else "0" * 64,
            "payload": payload,
        }
        event["event_hash"] = hashlib.sha256(
            _canonical(event).encode("utf-8")
        ).hexdigest()
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event, sort_keys=True) + "\n")
        return event

    def verify(self) -> tuple[bool, str]:
        previous = "0" * 64
        rows = self.load()
        for index, event in enumerate(rows):
            if event.get("index") != index:
                return False, f"index mismatch at {index}"
            if event.get("previous_hash") != previous:
                return False, f"previous hash mismatch at {index}"
            expected = hashlib.sha256(
                _canonical({k: v for k, v in event.items() if k != "event_hash"}).encode("utf-8")
            ).hexdigest()
            if event.get("event_hash") != expected:
                return False, f"event hash mismatch at {index}"
            previous = expected
        return True, f"verified {len(rows)} Robinhood learning events"


class RobinhoodLearningStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=10000")
        return connection

    @contextmanager
    def connection(self):
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def _initialize(self) -> None:
        with self.connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS candidates (
                    token_address TEXT PRIMARY KEY,
                    pair_address TEXT NOT NULL,
                    factory_address TEXT NOT NULL,
                    block_number INTEGER NOT NULL,
                    block_timestamp REAL NOT NULL,
                    transaction_hash TEXT NOT NULL,
                    log_index INTEGER NOT NULL,
                    name TEXT,
                    symbol TEXT,
                    analysis_status TEXT NOT NULL DEFAULT 'pending',
                    analysis_attempts INTEGER NOT NULL DEFAULT 0,
                    analyzed_at TEXT,
                    score REAL,
                    risk_level TEXT,
                    action_label TEXT,
                    hard_stops_json TEXT NOT NULL DEFAULT '[]',
                    paper_entry_allowed INTEGER,
                    entry_price_usd REAL,
                    entry_liquidity_usd REAL,
                    first_market_cap_usd REAL,
                    peak_market_cap_usd REAL,
                    peak_fdv_usd REAL,
                    last_observed_at TEXT,
                    last_outcome_attempt_at REAL,
                    discovered_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS checkpoints (
                    token_address TEXT NOT NULL,
                    horizon_label TEXT NOT NULL,
                    horizon_seconds INTEGER NOT NULL,
                    target_at REAL NOT NULL,
                    observed_at TEXT NOT NULL,
                    status TEXT NOT NULL,
                    learning_eligible INTEGER NOT NULL,
                    lateness_seconds REAL NOT NULL,
                    price_usd REAL,
                    liquidity_usd REAL,
                    market_cap_usd REAL,
                    fdv_usd REAL,
                    market_cap_multiple REAL,
                    maximum_favorable_excursion_pct REAL,
                    PRIMARY KEY (token_address, horizon_label)
                );
                CREATE TABLE IF NOT EXISTS positions (
                    token_address TEXT PRIMARY KEY,
                    symbol TEXT,
                    status TEXT NOT NULL,
                    opened_at REAL NOT NULL,
                    entry_price_usd REAL NOT NULL,
                    entry_liquidity_usd REAL NOT NULL,
                    cost_usd REAL NOT NULL,
                    quantity REAL NOT NULL,
                    entry_friction_bps REAL NOT NULL,
                    high_multiple REAL NOT NULL DEFAULT 1,
                    last_price_usd REAL,
                    last_liquidity_usd REAL,
                    last_mark_at REAL,
                    exit_price_usd REAL,
                    exit_value_usd REAL,
                    exit_reason TEXT,
                    closed_at REAL,
                    net_multiple REAL
                );
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    summary_json TEXT
                );
                CREATE TABLE IF NOT EXISTS v4_pools (
                    pool_id TEXT PRIMARY KEY,
                    currency0 TEXT NOT NULL,
                    currency1 TEXT NOT NULL,
                    token_address TEXT NOT NULL,
                    anchor_address TEXT NOT NULL,
                    fee_tier INTEGER NOT NULL,
                    tick_spacing INTEGER NOT NULL,
                    hooks_address TEXT NOT NULL,
                    initialized_block INTEGER NOT NULL,
                    modified_block INTEGER,
                    swapped_block INTEGER,
                    swap_timestamp REAL,
                    transaction_hash TEXT,
                    log_index INTEGER,
                    sqrt_price_x96 TEXT,
                    active_liquidity TEXT,
                    last_tick INTEGER,
                    promoted INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_candidates_analysis
                    ON candidates(analysis_status, block_number);
                CREATE INDEX IF NOT EXISTS idx_checkpoints_status
                    ON checkpoints(status, horizon_label);
                CREATE INDEX IF NOT EXISTS idx_positions_status
                    ON positions(status, opened_at);
                CREATE INDEX IF NOT EXISTS idx_v4_activation
                    ON v4_pools(promoted, modified_block, swapped_block);
                """
            )
            columns = {row[1] for row in connection.execute("PRAGMA table_info(candidates)")}
            migrations = {
                "source_version": "TEXT NOT NULL DEFAULT 'uniswap_v2'",
                "pool_id": "TEXT",
                "hooks_address": "TEXT",
                "fee_tier": "INTEGER",
                "tick_spacing": "INTEGER",
                "market_evidence_json": "TEXT NOT NULL DEFAULT '{}'",
            }
            for name, declaration in migrations.items():
                if name not in columns:
                    connection.execute(f"ALTER TABLE candidates ADD COLUMN {name} {declaration}")

    def add_candidates(self, candidates: list[dict]) -> int:
        if not candidates:
            return 0
        with self.connection() as connection:
            before = connection.total_changes
            connection.executemany(
                """
                INSERT OR IGNORE INTO candidates (
                    token_address, pair_address, factory_address, block_number,
                    block_timestamp, transaction_hash, log_index, name, symbol,
                    source_version, pool_id, hooks_address, fee_tier, tick_spacing,
                    discovered_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [(
                    row["token_address"].lower(), row["pair_address"].lower(),
                    row["factory_address"].lower(), row["block_number"],
                    row["block_timestamp"], row["transaction_hash"], row["log_index"],
                    row.get("name") or "", row.get("symbol") or "",
                    row.get("source_version") or SOURCE_V2, row.get("pool_id"),
                    row.get("hooks_address"), row.get("fee_tier"), row.get("tick_spacing"),
                    _utc_now(), _utc_now(),
                ) for row in candidates],
            )
            return connection.total_changes - before

    def apply_v4_events(self, events: list[dict]) -> None:
        """Persist V4 lifecycle evidence without creating analysis work yet."""
        with self.connection() as connection:
            for event in events:
                kind = event["kind"]
                if kind == "initialize":
                    connection.execute(
                        """
                        INSERT INTO v4_pools (
                            pool_id,currency0,currency1,token_address,anchor_address,
                            fee_tier,tick_spacing,hooks_address,initialized_block,
                            sqrt_price_x96,last_tick,updated_at
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(pool_id) DO UPDATE SET
                            sqrt_price_x96=excluded.sqrt_price_x96,
                            last_tick=excluded.last_tick,updated_at=excluded.updated_at
                        """,
                        (event["pool_id"],event["currency0"],event["currency1"],
                         event["token_address"],event["anchor_address"],event["fee_tier"],
                         event["tick_spacing"],event["hooks_address"],event["block_number"],
                         str(event["sqrt_price_x96"]),event["tick"],_utc_now()),
                    )
                elif kind == "modify":
                    connection.execute(
                        "UPDATE v4_pools SET modified_block=COALESCE(modified_block,?),updated_at=? WHERE pool_id=?",
                        (event["block_number"],_utc_now(),event["pool_id"]),
                    )
                elif kind == "swap":
                    connection.execute(
                        """
                        UPDATE v4_pools SET swapped_block=COALESCE(swapped_block,?),
                            swap_timestamp=COALESCE(swap_timestamp,?),transaction_hash=?,log_index=?,
                            sqrt_price_x96=?,active_liquidity=?,last_tick=?,updated_at=?
                        WHERE pool_id=?
                        """,
                        (event["block_number"],event["block_timestamp"],event["transaction_hash"],
                         event["log_index"],str(event["sqrt_price_x96"]),
                         str(event["active_liquidity"]),event["tick"],_utc_now(),event["pool_id"]),
                    )

    def pending_v4_activations(self) -> list[dict]:
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """SELECT * FROM v4_pools WHERE promoted=0 AND modified_block IS NOT NULL
                   AND swapped_block IS NOT NULL ORDER BY swapped_block,pool_id"""
            )]

    def known_v4_pool_ids(self) -> set[str]:
        with self.connection() as connection:
            return {row[0] for row in connection.execute("SELECT pool_id FROM v4_pools")}

    def mark_v4_promoted(self, pool_ids: list[str]) -> None:
        if not pool_ids:
            return
        with self.connection() as connection:
            connection.executemany(
                "UPDATE v4_pools SET promoted=1,updated_at=? WHERE pool_id=?",
                [(_utc_now(), pool_id) for pool_id in pool_ids],
            )

    def v4_pool(self, pool_id: str) -> dict | None:
        with self.connection() as connection:
            row = connection.execute("SELECT * FROM v4_pools WHERE pool_id=?", (pool_id.lower(),)).fetchone()
            return dict(row) if row else None

    def pending_analysis(self, limit: int) -> list[dict]:
        with self.connection() as connection:
            return [dict(row) for row in connection.execute(
                """
                SELECT * FROM candidates
                WHERE analysis_status IN ('pending','retry')
                  AND analysis_attempts < ?
                ORDER BY CASE analysis_status WHEN 'pending' THEN 0 ELSE 1 END,
                         block_number, log_index LIMIT ?
                """, (MAXIMUM_ANALYSIS_ATTEMPTS, max(0, limit))
            )]

    def record_analysis(self, token: str, analysis: dict, market: dict) -> None:
        hard_stops = [
            (item.get("code") or item.get("reason")) if isinstance(item, dict) else str(item)
            for item in analysis.get("hard_stop_overrides") or []
        ]
        score = safe_float(analysis.get("legitimacy_score"), 0.0)
        risk = str(analysis.get("risk_level") or "Unknown")
        liquidity = safe_float(market.get("liquidity_usd"), 0.0)
        price = safe_float(market.get("price_usd"), 0.0)
        market_cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        fdv = safe_float(market.get("fdv_usd"), 0.0) or None
        allowed = bool(
            score >= MINIMUM_ENTRY_SCORE
            and risk in {"Low", "Medium"}
            and not hard_stops
            and liquidity >= MINIMUM_ENTRY_LIQUIDITY_USD
            and price > 0
        )
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET analysis_status='complete',
                    analysis_attempts=analysis_attempts+1, analyzed_at=?, score=?,
                    risk_level=?, action_label=?, hard_stops_json=?,
                    paper_entry_allowed=?, entry_price_usd=?,
                    entry_liquidity_usd=?, first_market_cap_usd=COALESCE(first_market_cap_usd,?),
                    peak_market_cap_usd=MAX(COALESCE(peak_market_cap_usd,0),COALESCE(?,0)),
                    peak_fdv_usd=MAX(COALESCE(peak_fdv_usd,0),COALESCE(?,0)), updated_at=?
                    ,market_evidence_json=?
                WHERE token_address=?
                """,
                (
                    _utc_now(), score, risk, analysis.get("action_label"),
                    _canonical(hard_stops), int(allowed), price or None,
                    liquidity or None, market_cap, market_cap, fdv, _utc_now(),
                    _canonical(market), token.lower(),
                ),
            )

    def record_analysis_failure(self, token: str, reason: str) -> None:
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET
                    analysis_status=CASE
                        WHEN analysis_attempts+1 >= ? THEN 'failed'
                        ELSE 'retry'
                    END,
                    analysis_attempts=analysis_attempts+1, updated_at=?
                WHERE token_address=?
                """, (MAXIMUM_ANALYSIS_ATTEMPTS, _utc_now(), token.lower())
            )

    def candidate(self, token: str) -> dict | None:
        with self.connection() as connection:
            row = connection.execute(
                "SELECT * FROM candidates WHERE token_address=?", (token.lower(),)
            ).fetchone()
            return dict(row) if row else None

    @staticmethod
    def tolerance(horizon: int) -> float:
        return float(max(5 * 60, horizon // 4))

    def expire_missed(self, now: float) -> int:
        expired = 0
        with self.connection() as connection:
            for label, horizon in HORIZONS:
                cursor = connection.execute(
                    """
                    INSERT OR IGNORE INTO checkpoints (
                        token_address,horizon_label,horizon_seconds,target_at,
                        observed_at,status,learning_eligible,lateness_seconds
                    )
                    SELECT token_address,?,?,block_timestamp+?,?,'missed',0,
                           ?-(block_timestamp+?) FROM candidates
                    WHERE block_timestamp+?+? < ?
                    """,
                    (
                        label, horizon, horizon, _utc_now(), now, horizon,
                        horizon, self.tolerance(horizon), now,
                    ),
                )
                expired += cursor.rowcount
        return expired

    def due_outcomes(self, now: float, limit: int) -> list[dict]:
        values = []
        with self.connection() as connection:
            for label, horizon in HORIZONS:
                rows = connection.execute(
                    """
                    SELECT c.*, ? horizon_label, ? horizon_seconds,
                           c.block_timestamp+? target_at
                    FROM candidates c LEFT JOIN checkpoints p
                      ON p.token_address=c.token_address AND p.horizon_label=?
                    WHERE p.token_address IS NULL
                      AND c.block_timestamp+? <= ?
                      AND c.block_timestamp+?+? >= ?
                      AND (c.last_outcome_attempt_at IS NULL OR c.last_outcome_attempt_at<=?)
                    ORDER BY target_at, c.token_address LIMIT ?
                    """,
                    (
                        label, horizon, horizon, label, horizon, now, horizon,
                        self.tolerance(horizon), now, now-OUTCOME_RETRY_SECONDS, limit,
                    ),
                )
                values.extend(dict(row) for row in rows)
        values.sort(key=lambda row: (row["target_at"], row["token_address"]))
        selected, seen = [], set()
        for row in values:
            if row["token_address"] in seen:
                continue
            selected.append(row)
            seen.add(row["token_address"])
            if len(selected) >= limit:
                break
        return selected

    def record_outcome(self, due: dict, market: dict, now: float) -> None:
        token = due["token_address"]
        cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        fdv = safe_float(market.get("fdv_usd"), 0.0) or None
        status = "observed" if cap or fdv else "no_market"
        with self.connection() as connection:
            current = connection.execute(
                "SELECT * FROM candidates WHERE token_address=?", (token,)
            ).fetchone()
            first = current["first_market_cap_usd"] or cap
            peak = max(filter(None, [current["peak_market_cap_usd"], cap]), default=None)
            peak_fdv = max(filter(None, [current["peak_fdv_usd"], fdv]), default=None)
            multiple = cap / first if cap and first else None
            mfe = (peak / first - 1) * 100 if peak and first else None
            connection.execute(
                """
                INSERT INTO checkpoints (
                    token_address,horizon_label,horizon_seconds,target_at,
                    observed_at,status,learning_eligible,lateness_seconds,
                    price_usd,liquidity_usd,market_cap_usd,fdv_usd,
                    market_cap_multiple,maximum_favorable_excursion_pct
                ) VALUES (?,?,?,?,?,?,1,?,?,?,?,?,?,?)
                """,
                (
                    token,due["horizon_label"],due["horizon_seconds"],due["target_at"],
                    _utc_now(),status,max(0,now-due["target_at"]),
                    market.get("price_usd"),market.get("liquidity_usd"),cap,fdv,
                    multiple,mfe,
                ),
            )
            connection.execute(
                """
                UPDATE candidates SET first_market_cap_usd=?,peak_market_cap_usd=?,
                    peak_fdv_usd=?,last_observed_at=?,last_outcome_attempt_at=?,updated_at=?
                WHERE token_address=?
                """, (first,peak,peak_fdv,_utc_now(),now,_utc_now(),token)
            )

    def outcome_failure(self, token: str, now: float) -> None:
        with self.connection() as connection:
            connection.execute(
                "UPDATE candidates SET last_outcome_attempt_at=?,updated_at=? WHERE token_address=?",
                (now,_utc_now(),token.lower()),
            )

    def open_position(self, candidate: dict, market: dict) -> bool:
        if not candidate.get("paper_entry_allowed"):
            return False
        price = safe_float(market.get("price_usd"), 0.0)
        liquidity = safe_float(market.get("liquidity_usd"), 0.0)
        if price <= 0 or liquidity < MINIMUM_ENTRY_LIQUIDITY_USD:
            return False
        with self.connection() as connection:
            if connection.execute("SELECT 1 FROM positions WHERE token_address=?", (candidate["token_address"],)).fetchone():
                return False
            open_count = connection.execute("SELECT COUNT(*) FROM positions WHERE status='open'").fetchone()[0]
            if open_count >= MAXIMUM_POSITIONS:
                return False
            friction_bps = min(5_000.0, 100.0 + PAPER_COST_USD / max(1.0, 2*liquidity) * 10_000)
            retained = 1-friction_bps/10_000
            quantity = PAPER_COST_USD*retained/price
            connection.execute(
                """
                INSERT INTO positions (
                    token_address,symbol,status,opened_at,entry_price_usd,
                    entry_liquidity_usd,cost_usd,quantity,entry_friction_bps,
                    high_multiple,last_price_usd,last_liquidity_usd,last_mark_at
                ) VALUES (?,?,'open',?,?,?,?,?,?,1,?,?,?)
                """,
                (
                    candidate["token_address"],candidate.get("symbol") or "",time.time(),
                    price,liquidity,PAPER_COST_USD,quantity,friction_bps,
                    price,liquidity,time.time(),
                ),
            )
            return True

    def mark_position(self, token: str, market: dict, now: float) -> dict | None:
        price = safe_float(market.get("price_usd"), 0.0)
        liquidity = safe_float(market.get("liquidity_usd"), 0.0)
        if price <= 0:
            return None
        with self.connection() as connection:
            position = connection.execute(
                "SELECT * FROM positions WHERE token_address=? AND status='open'", (token.lower(),)
            ).fetchone()
            if not position:
                return None
            exit_impact = min(5_000.0, 100.0 + PAPER_COST_USD/max(1.0,2*liquidity)*10_000)
            value = position["quantity"]*price*(1-exit_impact/10_000)
            multiple = value/position["cost_usd"]
            high = max(position["high_multiple"],multiple)
            age = now-position["opened_at"]
            reason = None
            if liquidity <= 0:
                reason = "liquidity_unavailable"
            elif multiple <= STOP_LOSS_MULTIPLE:
                reason = "stop_loss"
            elif multiple >= TAKE_PROFIT_MULTIPLE:
                reason = "take_profit"
            elif age >= MAXIMUM_HOLD_SECONDS:
                reason = "maximum_hold"
            if reason:
                connection.execute(
                    """
                    UPDATE positions SET status='closed',high_multiple=?,last_price_usd=?,
                        last_liquidity_usd=?,last_mark_at=?,exit_price_usd=?,exit_value_usd=?,
                        exit_reason=?,closed_at=?,net_multiple=? WHERE token_address=?
                    """, (high,price,liquidity,now,price,value,reason,now,multiple,token.lower())
                )
            else:
                connection.execute(
                    """
                    UPDATE positions SET high_multiple=?,last_price_usd=?,
                        last_liquidity_usd=?,last_mark_at=? WHERE token_address=?
                    """, (high,price,liquidity,now,token.lower())
                )
            return {"token_address":token.lower(),"multiple":multiple,"reason":reason,"value_usd":value}

    def summary(self) -> dict:
        with self.connection() as connection:
            candidate = connection.execute(
                """
                SELECT COUNT(*) total,
                  SUM(analysis_status='complete') analyzed,
                  SUM(analysis_status IN ('pending','retry')) pending,
                  SUM(analysis_status='failed') failed,
                  SUM(paper_entry_allowed=1) admitted,
                  SUM(paper_entry_allowed=0) rejected,
                  SUM(peak_market_cap_usd>=1000000) million_peak
                FROM candidates
                """
            ).fetchone()
            statuses = {row[0]:row[1] for row in connection.execute(
                "SELECT status,COUNT(*) FROM checkpoints GROUP BY status"
            )}
            positions = connection.execute(
                """
                SELECT COUNT(*) opened,SUM(status='open') open,SUM(status='closed') closed,
                  SUM(CASE WHEN status='closed' AND net_multiple>1 THEN 1 ELSE 0 END) winners,
                  AVG(CASE WHEN status='closed' THEN net_multiple END) average_multiple
                FROM positions
                """
            ).fetchone()
            sources = {row[0]: row[1] for row in connection.execute(
                "SELECT source_version,COUNT(*) FROM candidates GROUP BY source_version"
            )}
        return {
            "candidates": {key:(candidate[key] or 0) for key in candidate.keys()},
            "checkpoints": statuses,
            "positions": {key:(positions[key] or 0) for key in positions.keys()},
            "sources": sources,
            "paper_only": True,
            "live_execution_enabled": False,
        }

    def recent_positions(self, limit: int = 50) -> list[dict]:
        """Return browser-safe paper marks, newest first.

        Open gains use the same estimated exit friction as mark_position, so the
        dashboard cannot display an optimistic friction-free percentage.
        """
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT p.*, c.name, c.pair_address, c.source_version,c.first_market_cap_usd,
                       cp.market_cap_usd current_market_cap_usd,
                       cp.observed_at market_observed_at
                FROM positions p
                JOIN candidates c ON c.token_address=p.token_address
                LEFT JOIN checkpoints cp ON cp.rowid=(
                    SELECT latest.rowid FROM checkpoints latest
                    WHERE latest.token_address=p.token_address
                      AND latest.status='observed'
                    ORDER BY latest.target_at DESC LIMIT 1
                )
                ORDER BY p.opened_at DESC LIMIT ?
                """,
                (max(0, limit),),
            ).fetchall()
        positions = []
        for raw in rows:
            row = dict(raw)
            current_price = safe_float(row.get("last_price_usd"), 0.0)
            current_liquidity = safe_float(row.get("last_liquidity_usd"), 0.0)
            if row.get("status") == "closed" and row.get("net_multiple") is not None:
                multiple = safe_float(row.get("net_multiple"), 0.0)
            elif current_price > 0:
                exit_friction_bps = min(
                    5_000.0,
                    100.0 + PAPER_COST_USD / max(1.0, 2 * current_liquidity) * 10_000,
                )
                marked_value = (
                    safe_float(row.get("quantity"), 0.0)
                    * current_price
                    * (1 - exit_friction_bps / 10_000)
                )
                multiple = marked_value / max(0.01, safe_float(row.get("cost_usd"), PAPER_COST_USD))
            else:
                multiple = None
            positions.append({
                "token_address": row["token_address"],
                "pair_address": row.get("pair_address"),
                "source_version": row.get("source_version"),
                "name": row.get("name") or "",
                "symbol": row.get("symbol") or "",
                "status": row.get("status"),
                "opened_at": row.get("opened_at"),
                "entry_price_usd": row.get("entry_price_usd"),
                "current_price_usd": row.get("last_price_usd"),
                "entry_market_cap_usd": row.get("first_market_cap_usd"),
                "current_market_cap_usd": (
                    row.get("current_market_cap_usd") or row.get("first_market_cap_usd")
                ),
                "gain_pct": (multiple - 1) * 100 if multiple is not None else None,
                "multiple": multiple,
                "high_multiple": row.get("high_multiple"),
                "last_mark_at": row.get("last_mark_at"),
                "market_observed_at": row.get("market_observed_at"),
                "exit_reason": row.get("exit_reason"),
                "paper_only": True,
            })
        return positions

    def recent_analyzed_tokens(self, limit: int = 100) -> list[dict]:
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT token_address,pair_address,name,symbol,analyzed_at,score,
                       risk_level,action_label,paper_entry_allowed,source_version,pool_id
                FROM candidates
                WHERE analysis_status='complete'
                ORDER BY analyzed_at DESC,block_number DESC LIMIT ?
                """,
                (max(0, limit),),
            ).fetchall()
        return [
            {
                "token_address": row["token_address"],
                "pair_address": row["pair_address"],
                "name": row["name"] or "",
                "symbol": row["symbol"] or "",
                "analyzed_at": row["analyzed_at"],
                "score": row["score"],
                "risk_level": row["risk_level"],
                "action_label": row["action_label"],
                "paper_entry_allowed": bool(row["paper_entry_allowed"]),
                "source_version": row["source_version"],
                "pool_id": row["pool_id"],
            }
            for row in rows
        ]


class RobinhoodPairObserver:
    def __init__(self, rpc: RobinhoodRPC, state_path: str | Path):
        self.rpc = rpc
        self.state_path = Path(state_path)

    def _state(self) -> dict:
        return read_json(self.state_path, {}) or {}

    def sync(self, *, block_limit: int, lookback: int) -> tuple[list[dict], dict]:
        latest = _remote_call("Robinhood latest block", self.rpc.get_block_number)
        state = self._state()
        start = safe_int(state.get("next_block"), max(0,latest-lookback))
        start = min(start, latest)
        end = min(latest,start+max(1,block_limit)-1)
        logs = _remote_call(
            f"Robinhood V2 logs {start}-{end}",
            lambda: self.rpc.get_logs(
                start, end, address=UNISWAP_V2_FACTORY,
                topics=[PAIR_CREATED_TOPIC],
            ),
        )
        block_times = {}
        candidates = []
        wrapped = WETH_ADDRESS.lower()
        for log in logs or []:
            topics = log.get("topics") or []
            if len(topics)<3:
                continue
            token0,token1 = _topic_address(topics[1]),_topic_address(topics[2])
            if token0.lower()==wrapped:
                token=token1
            elif token1.lower()==wrapped:
                token=token0
            else:
                continue
            pair=_data_address(log.get("data"),0)
            if token==ZERO_ADDRESS or pair==ZERO_ADDRESS:
                continue
            block_number=int(str(log.get("blockNumber") or "0x0"),16)
            if block_number not in block_times:
                block = _remote_call(
                    f"Robinhood block {block_number}",
                    lambda block_number=block_number: self.rpc.get_block(block_number),
                )
                block_times[block_number]=int(str(block.get("timestamp") or "0x0"),16)
            try:
                name=self.rpc.erc20_name(token,block=block_number)
                symbol=self.rpc.erc20_symbol(token,block=block_number)
            except Exception:
                name=symbol=""
            candidates.append({
                "token_address":token,
                "pair_address":pair,
                "factory_address":UNISWAP_V2_FACTORY,
                "block_number":block_number,
                "block_timestamp":block_times[block_number],
                "transaction_hash":log.get("transactionHash") or "",
                "log_index":int(str(log.get("logIndex") or "0x0"),16),
                "name":name,"symbol":symbol,
            })
        coverage={
            "from_block":start,"to_block":end,"latest_block":latest,
            "blocks_scanned":end-start+1,"logs_seen":len(logs or []),
            "wrapped_native_pairs_found":len(candidates),
            "caught_up":end>=latest,"blocks_behind":max(0,latest-end),
            "scope":"uniswap_v2_wrapped_native_pairs_only",
            "measured_at":_utc_now(),
        }
        atomic_json_write(self.state_path,{"next_block":end+1,"coverage":coverage,"updated_at":_utc_now()})
        return candidates,coverage


class RobinhoodV4Observer:
    """Observe V4 pools cheaply and activate only after liquidity and a swap."""
    def __init__(self, rpc: RobinhoodRPC, store: RobinhoodLearningStore, state_path: str | Path):
        self.rpc = rpc
        self.store = store
        self.state_path = Path(state_path)

    def _state(self) -> dict:
        return read_json(self.state_path, {}) or {}

    def sync(self, *, block_limit: int, lookback: int) -> tuple[list[dict], dict]:
        latest = _remote_call("Robinhood latest block", self.rpc.get_block_number)
        state = self._state()
        start = safe_int(state.get("next_block"), max(0, latest - lookback))
        start = min(start, latest)
        end = min(latest, start + max(1, block_limit) - 1)
        logs = _remote_call(
            f"Robinhood V4 logs {start}-{end}",
            lambda: self.rpc.get_logs(
                start, end, address=UNISWAP_V4_POOL_MANAGER,
                topics=[[V4_INITIALIZE_TOPIC, V4_MODIFY_LIQUIDITY_TOPIC, V4_SWAP_TOPIC]],
            ),
        )
        anchors = {WETH_ADDRESS.lower(), USDG_ADDRESS.lower()}
        known_pool_ids = self.store.known_v4_pool_ids()
        events = []
        counts = {"initialize": 0, "modify": 0, "swap": 0}
        ordered = sorted(logs or [], key=lambda row: (
            int(str(row.get("blockNumber") or "0x0"), 16),
            int(str(row.get("logIndex") or "0x0"), 16),
        ))
        for log in ordered:
            topics = log.get("topics") or []
            if len(topics) < 2:
                continue
            event_topic = str(topics[0]).lower()
            pool_id = str(topics[1]).lower()
            block_number = int(str(log.get("blockNumber") or "0x0"), 16)
            if event_topic == V4_INITIALIZE_TOPIC:
                if len(topics) < 4:
                    continue
                currency0 = _topic_address(topics[2]).lower()
                currency1 = _topic_address(topics[3]).lower()
                if (currency0 in anchors) == (currency1 in anchors):
                    continue
                token = currency1 if currency0 in anchors else currency0
                anchor = currency0 if currency0 in anchors else currency1
                events.append({
                    "kind": "initialize", "pool_id": pool_id,
                    "currency0": currency0, "currency1": currency1,
                    "token_address": token, "anchor_address": anchor,
                    "fee_tier": _data_word(log.get("data"), 0) & ((1 << 24) - 1),
                    "tick_spacing": _signed_word(log.get("data"), 1, 24),
                    "hooks_address": _data_address(log.get("data"), 2).lower(),
                    "sqrt_price_x96": _data_word(log.get("data"), 3),
                    "tick": _signed_word(log.get("data"), 4, 24),
                    "block_number": block_number,
                })
                known_pool_ids.add(pool_id)
                counts["initialize"] += 1
            elif event_topic == V4_MODIFY_LIQUIDITY_TOPIC:
                if pool_id not in known_pool_ids:
                    continue
                events.append({"kind": "modify", "pool_id": pool_id, "block_number": block_number})
                counts["modify"] += 1
            elif event_topic == V4_SWAP_TOPIC:
                if pool_id not in known_pool_ids:
                    continue
                events.append({
                    "kind": "swap", "pool_id": pool_id, "block_number": block_number,
                    "block_timestamp": None,
                    "transaction_hash": log.get("transactionHash") or "",
                    "log_index": int(str(log.get("logIndex") or "0x0"), 16),
                    "sqrt_price_x96": _data_word(log.get("data"), 2),
                    "active_liquidity": _data_word(log.get("data"), 3),
                    "tick": _signed_word(log.get("data"), 4, 24),
                })
                counts["swap"] += 1
        self.store.apply_v4_events(events)
        activations = self.store.pending_v4_activations()
        candidates = []
        activation_block_times: dict[int, int] = {}
        for pool in activations:
            token = pool["token_address"]
            activation_block = pool["swapped_block"]
            if activation_block not in activation_block_times:
                block = _remote_call(
                    f"Robinhood block {activation_block}",
                    lambda activation_block=activation_block: self.rpc.get_block(
                        activation_block
                    ),
                )
                activation_block_times[activation_block] = int(str(block.get("timestamp") or "0x0"), 16)
            try:
                name = self.rpc.erc20_name(token, block=pool["swapped_block"])
                symbol = self.rpc.erc20_symbol(token, block=pool["swapped_block"])
            except Exception:
                name = symbol = ""
            candidates.append({
                "token_address": token, "pair_address": UNISWAP_V4_POOL_MANAGER,
                "factory_address": UNISWAP_V4_POOL_MANAGER,
                "block_number": activation_block, "block_timestamp": activation_block_times[activation_block],
                "transaction_hash": pool["transaction_hash"] or "", "log_index": pool["log_index"] or 0,
                "name": name, "symbol": symbol, "source_version": SOURCE_V4,
                "pool_id": pool["pool_id"], "hooks_address": pool["hooks_address"],
                "fee_tier": pool["fee_tier"], "tick_spacing": pool["tick_spacing"],
            })
        coverage = {
            "from_block": start, "to_block": end, "latest_block": latest,
            "blocks_scanned": end-start+1, "logs_seen": len(logs or []),
            "initialize_events": counts["initialize"], "liquidity_events": counts["modify"],
            "swap_events": counts["swap"], "activated_pools": len(candidates),
            "caught_up": end >= latest, "blocks_behind": max(0, latest-end),
            "scope": "uniswap_v4_weth_or_usdg_first_swap_activation", "measured_at": _utc_now(),
        }
        atomic_json_write(self.state_path, {"next_block": end+1, "coverage": coverage, "updated_at": _utc_now()})
        return candidates, coverage


class RobinhoodMarketClient:
    def __init__(self, timeout: float = 10.0):
        self.timeout=timeout
        self.session=requests.Session()

    def snapshot(self, token: str, pair_address: str | None = None) -> dict:
        response = _remote_call(
            "DexScreener token market",
            lambda: self.session.get(
                f"https://api.dexscreener.com/latest/dex/tokens/{token}",
                timeout=self.timeout,
            ),
        )
        response.raise_for_status()
        pairs=response.json().get("pairs") or []
        matches=[]
        for pair in pairs:
            if str(pair.get("chainId") or "").lower()!="robinhood":
                continue
            base=(pair.get("baseToken") or {}).get("address","").lower()
            # DexScreener priceUsd is the base token's price. A quote-only
            # match would silently mark the wrapped-native asset as this token.
            if token.lower() != base:
                continue
            if pair_address and str(pair.get("pairAddress") or "").lower()==pair_address.lower():
                matches.append((1,pair))
            else:
                matches.append((0,pair))
        if not matches:
            return {}
        pair=max(matches,key=lambda item:(item[0],safe_float((item[1].get("liquidity") or {}).get("usd"),0.0)))[1]
        return {
            "pair_address":pair.get("pairAddress"),"dex_id":pair.get("dexId"),
            "price_usd":safe_float(pair.get("priceUsd"),0.0) or None,
            "liquidity_usd":safe_float((pair.get("liquidity") or {}).get("usd"),0.0) or None,
            "market_cap_usd":safe_float(pair.get("marketCap"),0.0) or None,
            "fdv_usd":safe_float(pair.get("fdv"),0.0) or None,
            "pair_symbol":(pair.get("baseToken") or {}).get("symbol"),
            "source":"dexscreener_exact_chain_token_pair",
        }

    def wrapped_native_usd(self) -> tuple[float, str]:
        """Resolve ETH/USD independently of Robinhood pair indexing."""
        try:
            market = self.snapshot(WETH_ADDRESS)
            price = safe_float(market.get("price_usd"), 0.0)
            if price > 0:
                return price, str(market.get("source") or "dexscreener")
        except Exception:
            pass
        response = _remote_call(
            "Coinbase ETH/USD spot",
            lambda: self.session.get(
                "https://api.coinbase.com/v2/prices/ETH-USD/spot",
                timeout=self.timeout,
            ),
        )
        response.raise_for_status()
        price = safe_float(((response.json().get("data") or {}).get("amount")), 0.0)
        return price, "coinbase_eth_usd_spot" if price > 0 else "unavailable"


class RobinhoodV4MarketClient:
    """Read current V4 pool state and derive explicitly-labelled market evidence."""
    def __init__(self, rpc: RobinhoodRPC, anchors: RobinhoodMarketClient, store: RobinhoodLearningStore):
        self.rpc = rpc
        self.anchors = anchors
        self.store = store

    @staticmethod
    def _decode_slot0(raw: str) -> tuple[int, int]:
        text = str(raw or "").removeprefix("0x")
        if len(text) < 128:
            return 0, 0
        sqrt_price = int(text[:64], 16) & ((1 << 160) - 1)
        tick_raw = int(text[64:128], 16) & ((1 << 24) - 1)
        tick = tick_raw - (1 << 24) if tick_raw & (1 << 23) else tick_raw
        return sqrt_price, tick

    def snapshot(self, candidate: dict) -> dict:
        pool_id = str(candidate.get("pool_id") or "")
        if len(pool_id.removeprefix("0x")) != 64:
            return {}
        argument = pool_id.removeprefix("0x")
        liquidity_raw = self.rpc.call(UNISWAP_V4_STATE_VIEW, "0x"+V4_GET_LIQUIDITY_SELECTOR+argument)
        slot0_raw = self.rpc.call(UNISWAP_V4_STATE_VIEW, "0x"+V4_GET_SLOT0_SELECTOR+argument)
        active_liquidity = int(liquidity_raw or "0x0", 16)
        sqrt_price, tick = self._decode_slot0(slot0_raw)
        if active_liquidity <= 0 or sqrt_price <= 0:
            return {"source": "uniswap_v4_state_view", "current_state_verified": False}
        pool = self.store.v4_pool(pool_id)
        if not pool:
            return {}
        token = candidate["token_address"]
        anchor = pool["anchor_address"]
        token_decimals = self.rpc.erc20_decimals(token)
        anchor_decimals = self.rpc.erc20_decimals(anchor)
        total_supply = self.rpc.erc20_total_supply(token)
        if str(anchor).lower() == USDG_ADDRESS.lower():
            anchor_usd, anchor_source = 1.0, "usdg_par_assumption"
        else:
            anchor_usd, anchor_source = self.anchors.wrapped_native_usd()
        if anchor_usd <= 0:
            return {"source": "uniswap_v4_state_view", "current_state_verified": False,
                    "reason": "anchor_usd_price_unavailable"}
        raw_ratio = (sqrt_price / (1 << 96)) ** 2
        human_ratio = raw_ratio * (10 ** (token_decimals-anchor_decimals) if pool["currency0"].lower()==token.lower() else 10 ** (anchor_decimals-token_decimals))
        if pool["currency0"].lower() == token.lower():
            token_usd = anchor_usd * human_ratio
        else:
            token_usd = anchor_usd / human_ratio if human_ratio else 0.0
        token_scale = 10 ** token_decimals
        anchor_scale = 10 ** anchor_decimals
        liquidity_usd = 2 * active_liquidity * (token_usd * anchor_usd) ** 0.5 / (token_scale * anchor_scale) ** 0.5
        supply = total_supply / token_scale
        return {
            "pool_id": pool_id, "source": "uniswap_v4_state_view",
            "price_usd": token_usd or None, "liquidity_usd": liquidity_usd or None,
            "market_cap_usd": token_usd*supply if token_usd and supply else None,
            "fdv_usd": token_usd*supply if token_usd and supply else None,
            "active_liquidity_raw": str(active_liquidity), "sqrt_price_x96": str(sqrt_price),
            "tick": tick, "anchor_price_source": anchor_source,
            "current_state_verified": True, "liquidity_model": "active_concentrated_liquidity_estimate",
        }


def _market_from_report(report: dict, token: str, pair_address: str) -> dict:
    pairs=((report.get("data") or {}).get("dexscreener") or {}).get("pairs") or []
    matches=[]
    for pair in pairs:
        if str(pair.get("chainId") or "").lower()!="robinhood":
            continue
        base=str((pair.get("baseToken") or {}).get("address") or "").lower()
        if token.lower() != base:
            continue
        exact=str(pair.get("pairAddress") or "").lower()==pair_address.lower()
        matches.append((int(exact),pair))
    if not matches:
        return {}
    pair=max(matches,key=lambda item:(item[0],safe_float((item[1].get("liquidity") or {}).get("usd"),0.0)))[1]
    return {
        "pair_address":pair.get("pairAddress"),"dex_id":pair.get("dexId"),
        "price_usd":safe_float(pair.get("priceUsd"),0.0) or None,
        "liquidity_usd":safe_float((pair.get("liquidity") or {}).get("usd"),0.0) or None,
        "market_cap_usd":safe_float(pair.get("marketCap"),0.0) or None,
        "fdv_usd":safe_float(pair.get("fdv"),0.0) or None,
    }


class RobinhoodLearningEngine:
    def __init__(self, root: str | Path = DEFAULT_ROOT, *, rpc=None, analyzer=None, market=None):
        self.root=Path(root)
        self.root.mkdir(parents=True,exist_ok=True)
        self.rpc=rpc or RobinhoodRPC(ROBINHOOD_NETWORK.rpc_url)
        self.store=RobinhoodLearningStore(self.root/"learning.sqlite3")
        self.ledger=HashEventLedger(self.root/"events.jsonl")
        self.observer=RobinhoodPairObserver(self.rpc,self.root/"discovery_cursor.json")
        self.v4_observer=RobinhoodV4Observer(self.rpc,self.store,self.root/"discovery_v4_cursor.json")
        self.market=market or RobinhoodMarketClient()
        self.v4_market=RobinhoodV4MarketClient(self.rpc,self.market,self.store)
        self.analyzer=analyzer

    def _analyzer(self):
        if self.analyzer is None:
            self.analyzer=Chainseer(
                rpc_url=ROBINHOOD_NETWORK.rpc_url,
                chain_root=str(Path(DEFAULT_CHAIN_ROOT)),
                network=ROBINHOOD_NETWORK,
            )
        return self.analyzer

    def observe_outcomes(self, now: float, limit: int) -> dict:
        expired=self.store.expire_missed(now)
        observed=no_market=failures=marks=0
        for due in self.store.due_outcomes(now,limit):
            try:
                market=(self.v4_market.snapshot(due) if due.get("source_version")==SOURCE_V4
                        else self.market.snapshot(due["token_address"],due["pair_address"]))
            except Exception:
                failures+=1
                self.store.outcome_failure(due["token_address"],now)
                continue
            self.store.record_outcome(due,market,now)
            mark=self.store.mark_position(due["token_address"],market,now)
            if mark:
                marks+=1
                self.ledger.append("robinhood_paper_mark",mark)
            observed+=1
            no_market+=not bool(market)
        return {"observed":observed,"no_market":no_market,"failures":failures,"expired_missed":expired,"position_marks":marks,"limit":limit}

    def run_once(self, *, discovery_block_limit=DEFAULT_DISCOVERY_BLOCK_LIMIT,
                 analysis_limit=DEFAULT_ANALYSIS_LIMIT,outcome_limit=DEFAULT_OUTCOME_LIMIT,
                 lookback=DEFAULT_DISCOVERY_LOOKBACK_BLOCKS,now:float|None=None) -> dict:
        started = time.monotonic()
        now = time.time() if now is None else now
        with LearningRunLock(self.root / ".learn_once.lock"):
            with self.store.connection() as connection:
                run_id = connection.execute(
                    "INSERT INTO runs(started_at,status) VALUES (?,'running')",
                    (_utc_now(),),
                ).lastrowid
            try:
                outcomes = self.observe_outcomes(now, outcome_limit)
                discovered, coverage = self.observer.sync(
                    block_limit=discovery_block_limit, lookback=lookback
                )
                v4_discovered, v4_coverage = self.v4_observer.sync(
                    block_limit=discovery_block_limit, lookback=lookback
                )
                new_candidates = self.store.add_candidates(discovered + v4_discovered)
                self.store.mark_v4_promoted(
                    [row["pool_id"] for row in v4_discovered]
                )
                analyses = analysis_failures = entries = 0
                for candidate in self.store.pending_analysis(analysis_limit):
                    try:
                        report = self._analyzer().analyze_token(
                            candidate["token_address"],
                            seal=False,
                            defer_cognition=True,
                        )
                        if report.get("error"):
                            raise RuntimeError(report["error"])
                        analysis = report.get("analysis") or {}
                        if candidate.get("source_version") == SOURCE_V4:
                            market = self.v4_market.snapshot(candidate)
                            stops = list(analysis.get("hard_stop_overrides") or [])
                            if str(candidate.get("hooks_address") or ZERO_ADDRESS).lower() != ZERO_ADDRESS:
                                stops.append({
                                    "code": "V4_HOOK_UNAUDITED", "severity": "High",
                                    "reason": "The V4 pool uses an unaudited hook", "action": "AVOID",
                                })
                            if not market.get("current_state_verified"):
                                stops.append({
                                    "code": "V4_MARKET_STATE_UNVERIFIED", "severity": "High",
                                    "reason": "Current V4 liquidity and price could not be verified", "action": "AVOID",
                                })
                            analysis = dict(analysis)
                            analysis["hard_stop_overrides"] = stops
                            analysis["v4_market_evidence"] = {
                                "pool_id": candidate.get("pool_id"),
                                "fee_tier": candidate.get("fee_tier"),
                                "tick_spacing": candidate.get("tick_spacing"),
                                "hooks_address": candidate.get("hooks_address"),
                                "activation": "initialize_then_modify_liquidity_then_first_swap",
                            }
                        else:
                            market = _market_from_report(
                                report,
                                candidate["token_address"],
                                candidate["pair_address"],
                            )
                        self.store.record_analysis(
                            candidate["token_address"], analysis, market
                        )
                        latest = self.store.candidate(candidate["token_address"])
                        if self.store.open_position(latest, market):
                            entries += 1
                            self.ledger.append("robinhood_paper_buy", {
                                "token_address": candidate["token_address"],
                                "symbol": latest.get("symbol"),
                                "score": latest.get("score"),
                                "price_usd": market.get("price_usd"),
                                "liquidity_usd": market.get("liquidity_usd"),
                                "paper_only": True,
                            })
                        analyses += 1
                    except Exception as exc:
                        analysis_failures += 1
                        self.store.record_analysis_failure(
                            candidate["token_address"], str(exc)
                        )
                summary = {
                    "schema_version": 2, "timestamp": _utc_now(),
                    "network": "robinhood", "chain_id": ROBINHOOD_NETWORK.chain_id,
                    "cycle": {
                        "new_candidates": new_candidates,
                        "pair_events_seen": len(discovered),
                        "v4_activations_seen": len(v4_discovered),
                        "analyses": analyses,
                        "analysis_failures": analysis_failures,
                        "paper_entries": entries,
                        "outcomes": outcomes,
                        "duration_seconds": round(time.monotonic() - started, 3),
                    },
                    "discovery_coverage": coverage,
                    "discovery_coverage_by_source": {
                        "uniswap_v2": coverage, "uniswap_v4": v4_coverage,
                    },
                    "learning": self.store.summary(),
                    "paper_only": True, "live_execution_enabled": False,
                }
                atomic_json_write(self.root / "learning_summary.json", summary)
                with self.store.connection() as connection:
                    connection.execute(
                        "UPDATE runs SET completed_at=?,status='complete',summary_json=? WHERE id=?",
                        (_utc_now(), _canonical(summary), run_id),
                    )
                return summary
            except Exception as exc:
                failure = {
                    "schema_version": 1, "timestamp": _utc_now(),
                    "status": "failed", "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                with self.store.connection() as connection:
                    connection.execute(
                        "UPDATE runs SET completed_at=?,status='failed',summary_json=? WHERE id=?",
                        (_utc_now(), _canonical(failure), run_id),
                    )
                raise

    def verify(self) -> dict:
        ledger_ok,ledger_report=self.ledger.verify()
        with self.store.connection() as connection:
            sqlite_ok=connection.execute("PRAGMA integrity_check").fetchone()[0]=="ok"
        return {"ok":ledger_ok and sqlite_ok,"ledger":ledger_report,"sqlite_integrity":sqlite_ok,"paper_only":True}


def dashboard_snapshot(root: str | Path) -> dict:
    root=Path(root)
    store=RobinhoodLearningStore(root/"learning.sqlite3")
    summary=read_json(root/"learning_summary.json",{}) or {}
    cursor=read_json(root/"discovery_cursor.json",{}) or {}
    v4_cursor=read_json(root/"discovery_v4_cursor.json",{}) or {}
    return {
        "timestamp":_utc_now(),"network":"robinhood","chain_id":ROBINHOOD_NETWORK.chain_id,
        "learning":store.summary(),"positions":store.recent_positions(),
        "analyzed_tokens":store.recent_analyzed_tokens(),
        "last_cycle":summary.get("cycle") or {},
        "discovery_coverage":cursor.get("coverage") or summary.get("discovery_coverage") or {},
        "discovery_coverage_by_source":{
            "uniswap_v2":cursor.get("coverage") or {},
            "uniswap_v4":v4_cursor.get("coverage") or {},
        },
        "paper_only":True,"live_execution_enabled":False,
    }


def serve_dashboard(root: str | Path, host: str, port: int) -> None:
    if host not in {"127.0.0.1","localhost"}:
        raise ValueError("Robinhood dashboard is local-only")
    html_path=Path(__file__).with_name("robinhood_dashboard.html")
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in {"/","/index.html"}:
                content=html_path.read_bytes(); content_type="text/html; charset=utf-8"
            elif self.path=="/api/status":
                content=json.dumps(dashboard_snapshot(root)).encode(); content_type="application/json"
            else:
                self.send_error(404); return
            self.send_response(200); self.send_header("Content-Type",content_type)
            self.send_header("Cache-Control","no-store"); self.send_header("Content-Length",str(len(content)))
            self.end_headers(); self.wfile.write(content)
        def log_message(self, *_args):
            return
    server=ThreadingHTTPServer((host,port),Handler)
    print(f"Robinhood learning dashboard: http://{host}:{port}")
    print("READ-ONLY: no write endpoints exist. Press Ctrl+C to stop.")
    try: server.serve_forever(poll_interval=.5)
    except KeyboardInterrupt: pass
    finally: server.server_close()


def main() -> None:
    ensure_utf8_runtime()
    parser=argparse.ArgumentParser(description="Robinhood Chain paper learning")
    parser.add_argument("command",choices=("learn-once","status","dashboard","verify"))
    parser.add_argument("--root",default=DEFAULT_ROOT)
    parser.add_argument("--host",default="127.0.0.1")
    parser.add_argument("--port",type=int,default=DEFAULT_DASHBOARD_PORT)
    parser.add_argument("--discovery-block-limit",type=int,default=DEFAULT_DISCOVERY_BLOCK_LIMIT)
    parser.add_argument("--analysis-limit",type=int,default=DEFAULT_ANALYSIS_LIMIT)
    parser.add_argument("--outcome-limit",type=int,default=DEFAULT_OUTCOME_LIMIT)
    parser.add_argument("--lookback",type=int,default=DEFAULT_DISCOVERY_LOOKBACK_BLOCKS)
    args=parser.parse_args()
    if args.command=="dashboard":
        serve_dashboard(args.root,args.host,args.port); return
    if args.command=="status":
        print(json.dumps(dashboard_snapshot(args.root),indent=2)); return
    engine=RobinhoodLearningEngine(args.root)
    if args.command=="verify":
        result=engine.verify(); print(json.dumps(result,indent=2)); raise SystemExit(0 if result["ok"] else 1)
    summary=engine.run_once(
        discovery_block_limit=max(1,args.discovery_block_limit),
        analysis_limit=max(0,args.analysis_limit),outcome_limit=max(0,args.outcome_limit),
        lookback=max(1,args.lookback),
    )
    print(json.dumps(summary,indent=2))


if __name__=="__main__":
    main()
