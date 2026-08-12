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
import threading
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
from chainseer_robinhood_reflection import (
    RobinhoodReflectionCoordinator,
    default_skill_root,
)


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
SOURCE_V3 = "uniswap_v3"
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
DEFAULT_MARKET_RECHECK_LIMIT = 4
MAXIMUM_ANALYSIS_ATTEMPTS = 3
OUTCOME_RETRY_SECONDS = 60

# Two nominal learn cycles. See OutcomeStore.tolerance for why one was not
# enough: a 15m checkpoint had a 300s window against a 300s median cadence.
CHECKPOINT_TOLERANCE_MINIMUM_SECONDS = 10 * 60
# A mark landing later than this fraction of its horizon is recorded but not
# learned from -- a 15m outcome observed at 27m is measuring something else.
CHECKPOINT_LEARNING_LATENESS_FRACTION = 0.5
MINIMUM_ENTRY_SCORE = 70.0
MINIMUM_ENTRY_LIQUIDITY_USD = 10_000.0
ENTRY_PRIORITY_PENALTY_MARKET_CAP_USD = 5_000_000.0
MAXIMUM_ENTRY_MARKET_CAP_USD = 10_000_000.0
REENTRY_MOMENTUM_MULTIPLE = 1.05
MOMENTUM_PRIORITY_MULTIPLE = 2.0
MOMENTUM_PRIORITY_MINIMUM_HORIZON_SECONDS = 60 * 60
MOMENTUM_PRIORITY_MAXIMUM_MARKET_CAP_USD = 100_000_000_000.0
MOMENTUM_PRIORITY_MAXIMUM_CAP_TO_LIQUIDITY = 1_000.0
EXECUTABLE_MARKET_RECHECK_SECONDS = 15 * 60
EXECUTABLE_MARKET_WATCH_SECONDS = 7 * 24 * 60 * 60
EXECUTABLE_MARKET_MAXIMUM_POOLS_PER_TOKEN = 8
TEMPORARY_EXECUTION_STOPS = {"V4_MARKET_STATE_UNVERIFIED"}
PAPER_DECISION_ADMITTED = "admitted"
PAPER_DECISION_REJECTED = "rejected"
PAPER_DECISION_WATCHING = "watching_for_executable_market"
PAPER_DECISION_EXPIRED = "expired_no_executable_market"
PAPER_DECISION_ABOVE_CAP = "observing_above_entry_ceiling"
PAPER_DECISION_REENTRY = "waiting_for_reentry_momentum"
# A position whose price has fallen to this fraction of entry is not a market
# to exit, it is a token that stopped existing. Held separately from
# STOP_LOSS_MULTIPLE so the two causes stay distinguishable in every downstream
# audit: a stop loss is a decline the exit rules are meant to catch, a price
# collapse is a rug the entry rules should have refused.
PRICE_COLLAPSE_MULTIPLE = 0.02

PAPER_COST_USD = 100.0
MAXIMUM_POSITIONS = 50
STOP_LOSS_MULTIPLE = 0.65
STAGE_ONE_MULTIPLE = 2.0
STAGE_TWO_MULTIPLE = 3.0
STAGE_ONE_ORIGINAL_FRACTION = 0.50
STAGE_TWO_ORIGINAL_FRACTION = 0.25
RUNNER_TRAILING_DRAWDOWN = 0.35
STAGNATION_MINIMUM_MULTIPLE = 1.25
STAGNATION_EXIT_SECONDS = 24 * 60 * 60
MAXIMUM_HOLD_SECONDS = 7 * 24 * 60 * 60
SHADOW_POLICY_FIXED_3X = "fixed_3x"
SHADOW_POLICY_PURE_TRAILING = "pure_trailing"
PURE_TRAILING_ACTIVATION_MULTIPLE = 1.25
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
            # Retrying an oversized eth_getLogs window cannot change the
            # provider's deterministic result. Callers that support adaptive
            # windowing can split immediately after this error is wrapped.
            if "exceeds limit of 10000" in str(exc).lower():
                break
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
                CREATE TABLE IF NOT EXISTS position_policy_states (
                    token_address TEXT NOT NULL,
                    policy TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    high_multiple REAL NOT NULL DEFAULT 1,
                    exit_price_usd REAL,
                    exit_value_usd REAL,
                    exit_reason TEXT,
                    closed_at REAL,
                    net_multiple REAL,
                    last_mark_at REAL,
                    PRIMARY KEY (token_address, policy)
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
                CREATE INDEX IF NOT EXISTS idx_position_policy_status
                    ON position_policy_states(status, policy);
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
                "analysis_priority_reason": "TEXT",
                "analysis_queue_age_seconds": "REAL",
                "paper_decision": "TEXT",
                "market_watch_started_at": "REAL",
                "market_watch_last_checked_at": "REAL",
                "market_watch_checks": "INTEGER NOT NULL DEFAULT 0",
                "market_watch_expires_at": "REAL",
                "market_watch_reason": "TEXT",
                "market_watch_reference_price_usd": "REAL",
                "market_watch_reference_market_cap_usd": "REAL",
                "market_watch_reference_at": "REAL",
            }
            for name, declaration in migrations.items():
                if name not in columns:
                    connection.execute(f"ALTER TABLE candidates ADD COLUMN {name} {declaration}")
            position_columns = {
                row[1] for row in connection.execute("PRAGMA table_info(positions)")
            }
            position_migrations = {
                "entry_market_cap_usd": "REAL",
                "last_market_cap_usd": "REAL",
                "last_market_observed_at": "TEXT",
                "original_quantity": "REAL",
                "realized_value_usd": "REAL NOT NULL DEFAULT 0",
                "stage_one_sold_at": "REAL",
                "stage_two_sold_at": "REAL",
                "runner_high_multiple": "REAL",
                "entry_policy_version": "TEXT",
                "grandfathered_above_cap": "INTEGER NOT NULL DEFAULT 0",
            }
            for name, declaration in position_migrations.items():
                if name not in position_columns:
                    connection.execute(
                        f"ALTER TABLE positions ADD COLUMN {name} {declaration}"
                    )
            connection.execute(
                """
                UPDATE positions SET
                    original_quantity=COALESCE(original_quantity,quantity),
                    realized_value_usd=COALESCE(realized_value_usd,0),
                    runner_high_multiple=COALESCE(runner_high_multiple,high_multiple,1),
                    entry_policy_version=COALESCE(entry_policy_version,'legacy_grandfathered'),
                    grandfathered_above_cap=CASE
                        WHEN entry_market_cap_usd>? THEN 1
                        ELSE COALESCE(grandfathered_above_cap,0)
                    END
                """,
                (MAXIMUM_ENTRY_MARKET_CAP_USD,),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO position_policy_states (
                    token_address,policy,status,high_multiple,last_mark_at
                ) SELECT token_address,?,status,high_multiple,last_mark_at FROM positions
                """,
                (SHADOW_POLICY_FIXED_3X,),
            )
            connection.execute(
                """
                INSERT OR IGNORE INTO position_policy_states (
                    token_address,policy,status,high_multiple,last_mark_at
                ) SELECT token_address,?,status,high_multiple,last_mark_at FROM positions
                """,
                (SHADOW_POLICY_PURE_TRAILING,),
            )
            connection.execute(
                """
                UPDATE candidates SET paper_decision=CASE
                    WHEN paper_entry_allowed=1 THEN ?
                    WHEN paper_entry_allowed=0 THEN ?
                    ELSE paper_decision
                END
                WHERE paper_decision IS NULL
                """,
                (PAPER_DECISION_ADMITTED, PAPER_DECISION_REJECTED),
            )
            # Earlier versions treated a temporarily unverifiable V4 market as
            # a terminal rejection. Preserve the analysis result but migrate
            # otherwise-qualified, hook-free candidates into a bounded market
            # watch. Any eventual entry is priced at its future executable
            # snapshot; this never creates a retroactive paper fill.
            now = time.time()
            legacy_watch_rows = connection.execute(
                """
                SELECT token_address,hard_stops_json FROM candidates
                WHERE analysis_status='complete' AND paper_entry_allowed=0
                  AND paper_decision=? AND score>=?
                  AND risk_level IN ('Low','Medium')
                  AND source_version=? AND LOWER(COALESCE(hooks_address,?))=?
                """,
                (
                    PAPER_DECISION_REJECTED, MINIMUM_ENTRY_SCORE, SOURCE_V4,
                    ZERO_ADDRESS, ZERO_ADDRESS,
                ),
            ).fetchall()
            for row in legacy_watch_rows:
                try:
                    stops = set(json.loads(row["hard_stops_json"] or "[]"))
                except (TypeError, ValueError):
                    stops = set()
                if stops and stops <= TEMPORARY_EXECUTION_STOPS:
                    connection.execute(
                        """
                        UPDATE candidates SET paper_decision=?,
                            market_watch_started_at=COALESCE(market_watch_started_at,?),
                            market_watch_expires_at=COALESCE(market_watch_expires_at,?),
                            market_watch_reason=? WHERE token_address=?
                        """,
                        (
                            PAPER_DECISION_WATCHING, now,
                            now + EXECUTABLE_MARKET_WATCH_SECONDS,
                            "market_state_unverified", row["token_address"],
                        ),
                    )
            # Positions created before entry-market-cap persistence can be
            # reconstructed from the exact analysis-time market evidence used
            # to open them. Never substitute the earlier launch checkpoint.
            legacy = connection.execute(
                """
                SELECT p.token_address,c.market_evidence_json,p.opened_at
                FROM positions p JOIN candidates c USING(token_address)
                WHERE p.entry_market_cap_usd IS NULL
                """
            ).fetchall()
            for row in legacy:
                try:
                    evidence = json.loads(row["market_evidence_json"] or "{}")
                except (TypeError, ValueError):
                    evidence = {}
                market_cap = safe_float(evidence.get("market_cap_usd"), 0.0) or None
                connection.execute(
                    """
                    UPDATE positions SET entry_market_cap_usd=?,
                        last_market_cap_usd=COALESCE(last_market_cap_usd,?),
                        last_market_observed_at=COALESCE(last_market_observed_at,?)
                    WHERE token_address=?
                    """,
                    (market_cap, market_cap, _utc_now(), row["token_address"]),
                )

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
        limit = max(0, limit)
        if not limit:
            return []
        with self.connection() as connection:
            rows = [dict(row) for row in connection.execute(
                """
                SELECT c.*,
                    MAX(CASE
                        WHEN cp.status='observed'
                         AND cp.learning_eligible=1
                         AND cp.horizon_seconds>=?
                         AND cp.market_cap_multiple>=?
                         AND cp.liquidity_usd>=?
                         AND cp.market_cap_usd>0
                         AND cp.market_cap_usd<=?
                         AND cp.market_cap_usd/cp.liquidity_usd<=?
                        THEN cp.market_cap_multiple * CASE
                            WHEN cp.market_cap_usd>=? THEN 0.5 ELSE 1.0 END
                    END) momentum_priority_multiple
                FROM candidates c
                LEFT JOIN checkpoints cp ON cp.token_address=c.token_address
                WHERE c.analysis_status IN ('pending','retry')
                  AND c.analysis_attempts < ?
                GROUP BY c.token_address
                """,
                (
                    MOMENTUM_PRIORITY_MINIMUM_HORIZON_SECONDS,
                    MOMENTUM_PRIORITY_MULTIPLE,
                    MINIMUM_ENTRY_LIQUIDITY_USD,
                    MAXIMUM_ENTRY_MARKET_CAP_USD,
                    MOMENTUM_PRIORITY_MAXIMUM_CAP_TO_LIQUIDITY,
                    ENTRY_PRIORITY_PENALTY_MARKET_CAP_USD,
                    MAXIMUM_ANALYSIS_ATTEMPTS,
                ),
            )]
        oldest = sorted(
            rows,
            key=lambda row: (
                0 if row["analysis_status"] == "pending" else 1,
                row["block_number"],
                row["log_index"],
            ),
        )
        momentum = sorted(
            (row for row in rows if row.get("momentum_priority_multiple") is not None),
            key=lambda row: (
                0 if row["analysis_status"] == "pending" else 1,
                -safe_float(row.get("momentum_priority_multiple"), 0.0),
                row["block_number"],
            ),
        )
        selected = []
        if momentum:
            selected.append(momentum[0])
        for row in oldest:
            if len(selected) >= limit:
                break
            if any(item["token_address"] == row["token_address"] for item in selected):
                continue
            selected.append(row)
        return selected

    def record_analysis(
        self,
        token: str,
        analysis: dict,
        market: dict,
        *,
        priority_reason: str | None = None,
        queue_age_seconds: float | None = None,
    ) -> None:
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
        # The floor may have been raised by the closed-position audit. Read it
        # per call rather than caching: the audit runs in the same process and
        # a stale in-memory copy would silently ignore a tightening it just
        # applied. max() is the guarantee -- the adaptive value can only ever
        # raise the module constant, never lower it, so a corrupt or hostile
        # policy file cannot loosen admission.
        minimum_entry_score = self._effective_minimum_entry_score()
        token_quality_passes = bool(
            score >= minimum_entry_score and risk in {"Low", "Medium"}
        )
        permanent_stops = set(hard_stops) - TEMPORARY_EXECUTION_STOPS
        above_cap = bool(
            market_cap is not None
            and market_cap > MAXIMUM_ENTRY_MARKET_CAP_USD
        )
        allowed = bool(
            token_quality_passes
            and not hard_stops
            and liquidity >= MINIMUM_ENTRY_LIQUIDITY_USD
            and price > 0
            and market_cap is not None
            and not above_cap
        )
        watching = bool(
            token_quality_passes and not permanent_stops and not allowed
            and (
                not price or liquidity < MINIMUM_ENTRY_LIQUIDITY_USD
                or market_cap is None
            )
        )
        observing_above_cap = bool(
            token_quality_passes and not permanent_stops and above_cap
            and liquidity >= MINIMUM_ENTRY_LIQUIDITY_USD and price > 0
        )
        watched = watching or observing_above_cap
        decision = (
            PAPER_DECISION_ADMITTED if allowed else
            PAPER_DECISION_ABOVE_CAP if observing_above_cap else
            PAPER_DECISION_WATCHING if watching else PAPER_DECISION_REJECTED
        )
        now = time.time()
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
                    ,market_evidence_json=?,analysis_priority_reason=?,
                    analysis_queue_age_seconds=?,paper_decision=?,
                    market_watch_started_at=CASE WHEN ? THEN COALESCE(market_watch_started_at,?) ELSE market_watch_started_at END,
                    market_watch_last_checked_at=market_watch_last_checked_at,
                    market_watch_checks=market_watch_checks,
                    market_watch_expires_at=CASE WHEN ? THEN COALESCE(market_watch_expires_at,?) ELSE market_watch_expires_at END,
                    market_watch_reason=CASE WHEN ? THEN ? ELSE market_watch_reason END,
                    market_watch_reference_price_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_price_usd END,
                    market_watch_reference_market_cap_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_market_cap_usd END,
                    market_watch_reference_at=CASE WHEN ? THEN ? ELSE market_watch_reference_at END
                WHERE token_address=?
                """,
                (
                    _utc_now(), score, risk, analysis.get("action_label"),
                    _canonical(hard_stops), int(allowed), price or None,
                    liquidity or None, market_cap, market_cap, fdv, _utc_now(),
                    _canonical(market), priority_reason, queue_age_seconds, decision,
                    int(watched), now, int(watched),
                    now + EXECUTABLE_MARKET_WATCH_SECONDS, int(watched),
                    (
                        "above_entry_market_cap_ceiling" if observing_above_cap
                        else "market_unverified" if not price
                        else "market_cap_unavailable" if market_cap is None
                        else "liquidity_below_minimum"
                    ),
                    int(observing_above_cap), price or None,
                    int(observing_above_cap), market_cap,
                    int(observing_above_cap), now,
                    token.lower(),
                ),
            )

    def pending_market_watches(self, now: float, limit: int) -> list[dict]:
        if limit <= 0:
            return []
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET paper_decision=?,market_watch_reason='watch_expired',
                    updated_at=? WHERE paper_decision IN (?,?,?) AND market_watch_expires_at<=?
                """,
                (
                    PAPER_DECISION_EXPIRED, _utc_now(), PAPER_DECISION_WATCHING,
                    PAPER_DECISION_ABOVE_CAP, PAPER_DECISION_REENTRY, now,
                ),
            )
            return [dict(row) for row in connection.execute(
                """
                SELECT * FROM candidates WHERE paper_decision IN (?,?,?)
                  AND market_watch_expires_at>?
                  AND (market_watch_last_checked_at IS NULL OR market_watch_last_checked_at<=?)
                ORDER BY COALESCE(market_watch_last_checked_at,0),market_watch_started_at,token_address
                LIMIT ?
                """,
                (
                    PAPER_DECISION_WATCHING, PAPER_DECISION_ABOVE_CAP,
                    PAPER_DECISION_REENTRY, now,
                    now - EXECUTABLE_MARKET_RECHECK_SECONDS, limit,
                ),
            )]

    def record_market_watch_check(
        self, token: str, market: dict | None = None, *, reason: str | None = None,
        update_reference: bool = False, decision: str | None = None,
    ) -> None:
        evidence = _canonical(market or {})
        reason = reason or (
            "executable_market_found" if market else "no_executable_market"
        )
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET market_watch_last_checked_at=?,
                    market_watch_checks=market_watch_checks+1,market_watch_reason=?,
                    market_evidence_json=CASE WHEN ? THEN ? ELSE market_evidence_json END,
                    market_watch_reference_price_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_price_usd END,
                    market_watch_reference_market_cap_usd=CASE WHEN ? THEN ? ELSE market_watch_reference_market_cap_usd END,
                    market_watch_reference_at=CASE WHEN ? THEN ? ELSE market_watch_reference_at END,
                    paper_decision=COALESCE(?,paper_decision),
                    updated_at=? WHERE token_address=?
                """,
                (
                    time.time(), reason, int(bool(market)), evidence,
                    int(update_reference), safe_float((market or {}).get("price_usd"), 0.0) or None,
                    int(update_reference), safe_float((market or {}).get("market_cap_usd"), 0.0) or None,
                    int(update_reference), time.time(), decision, _utc_now(), token.lower(),
                ),
            )

    def select_execution_pool(self, token: str, market: dict) -> None:
        source_version = str(market.get("source_version") or SOURCE_V2)
        pair_address = str(market.get("pair_address") or "").lower()
        pool_id = pair_address if source_version == SOURCE_V4 else None
        hooks_address = ZERO_ADDRESS
        fee_tier = tick_spacing = None
        if pool_id:
            pool = self.v4_pool(pool_id)
            if pool:
                hooks_address = pool.get("hooks_address") or ZERO_ADDRESS
                fee_tier = pool.get("fee_tier")
                tick_spacing = pool.get("tick_spacing")
        with self.connection() as connection:
            connection.execute(
                """
                UPDATE candidates SET pair_address=?,source_version=?,pool_id=?,
                    hooks_address=?,fee_tier=?,tick_spacing=?,updated_at=?
                WHERE token_address=?
                """,
                (
                    pair_address, source_version, pool_id, hooks_address,
                    fee_tier, tick_spacing, _utc_now(), token.lower(),
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
        """How late a mark may land and still be recorded rather than expired.

        The old floor was 5 minutes, which for the 15m horizon is a window
        exactly ONE learn cycle wide (measured cadence: median 300s, max
        1,125s). A single slow cycle therefore expired the checkpoint outright,
        and the miss rate tracked the window width exactly: 24.6% at 15m, 12.1%
        at 1h, 0.3% at 6h. RH-REFLECT-MARKET-COVERAGE has reported this as a
        31.7% observation-failure ratio since checkpoint 15.

        The floor is now two nominal cycles, so ordinary jitter no longer
        destroys an observation. Widening a tolerance normally trades accuracy
        for coverage -- a mark arriving late measures a later moment than the
        horizon names -- so it is paired with the learning-eligibility cutoff
        below: late marks are RECORDED but excluded from learning. Coverage and
        measurement integrity are separate concerns and are now separately
        controlled.
        """
        return float(max(CHECKPOINT_TOLERANCE_MINIMUM_SECONDS, horizon // 4))

    @staticmethod
    def learning_eligible(horizon: int, lateness: float) -> int:
        """Whether a recorded mark is timely enough to learn from.

        Previously hardcoded to 1 on every observation, so a mark that landed
        near the edge of its window fed calibration identically to one on time.
        A 15m outcome measured at 27m is not a 15m outcome.
        """
        if horizon <= 0:
            return 1
        return int(
            max(0.0, float(lateness))
            <= horizon * CHECKPOINT_LEARNING_LATENESS_FRACTION
        )

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
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    token,due["horizon_label"],due["horizon_seconds"],due["target_at"],
                    _utc_now(),status,
                    self.learning_eligible(
                        due["horizon_seconds"], max(0, now-due["target_at"])
                    ),
                    max(0,now-due["target_at"]),
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
        market_cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        if (
            price <= 0 or liquidity < MINIMUM_ENTRY_LIQUIDITY_USD
            or market_cap is None or market_cap > MAXIMUM_ENTRY_MARKET_CAP_USD
        ):
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
                    entry_market_cap_usd,high_multiple,last_price_usd,
                    last_liquidity_usd,last_market_cap_usd,last_market_observed_at,
                    last_mark_at,original_quantity,realized_value_usd,
                    runner_high_multiple,entry_policy_version,grandfathered_above_cap
                ) VALUES (?,?,'open',?,?,?,?,?,?,?,1,?,?,?,?,?,?,0,1,?,0)
                """,
                (
                    candidate["token_address"],candidate.get("symbol") or "",time.time(),
                    price,liquidity,PAPER_COST_USD,quantity,friction_bps,
                    market_cap,price,liquidity,market_cap,_utc_now(),time.time(),
                    quantity,"staged_v1",
                ),
            )
            connection.executemany(
                """
                INSERT INTO position_policy_states (
                    token_address,policy,status,high_multiple,last_mark_at
                ) VALUES (?,?,'open',1,?)
                """,
                [
                    (candidate["token_address"], SHADOW_POLICY_FIXED_3X, time.time()),
                    (candidate["token_address"], SHADOW_POLICY_PURE_TRAILING, time.time()),
                ],
            )
            return True

    def _effective_minimum_entry_score(self) -> float:
        """Module constant, or a higher floor set by the closed-position audit.

        Clamped with max() so this can only tighten. A missing, unreadable or
        malformed file falls back to the constant rather than failing open.
        """
        try:
            # self.path is the sqlite file; the policy sits beside it in
            # the learning root.
            state = read_json(
                self.path.parent / "adaptive_policy.json", {}
            ) or {}
            adaptive = safe_float(state.get("minimum_entry_score"), 0.0)
        except Exception:
            return MINIMUM_ENTRY_SCORE
        return max(MINIMUM_ENTRY_SCORE, adaptive)

    @staticmethod
    def _liquidation_value(quantity: float, price: float, liquidity: float) -> float:
        gross = max(0.0, quantity) * max(0.0, price)
        impact_bps = min(
            5_000.0,
            100.0 + gross / max(1.0, 2 * liquidity) * 10_000,
        )
        return gross * (1 - impact_bps / 10_000)

    def _mark_shadow_policies(
        self, connection, position, price: float, liquidity: float, now: float,
    ) -> None:
        full_value = self._liquidation_value(
            safe_float(position["original_quantity"], position["quantity"]),
            price, liquidity,
        )
        multiple = full_value / max(0.01, position["cost_usd"])
        age = now - position["opened_at"]
        for state in connection.execute(
            "SELECT * FROM position_policy_states WHERE token_address=? AND status='open'",
            (position["token_address"],),
        ).fetchall():
            high = max(safe_float(state["high_multiple"], 1.0), multiple)
            reason = None
            # Same cause-before-ordering classification as the live path; the
            # shadow policies exist to be compared against it, so they have to
            # label an exit the same way or the comparison measures taxonomy
            # rather than policy.
            price_multiple = price / max(1e-30, position["entry_price_usd"])
            if price_multiple <= PRICE_COLLAPSE_MULTIPLE:
                reason = "price_collapse"
            elif multiple <= STOP_LOSS_MULTIPLE:
                reason = "stop_loss"
            elif liquidity < MINIMUM_ENTRY_LIQUIDITY_USD:
                reason = "liquidity_below_minimum"
            elif age >= MAXIMUM_HOLD_SECONDS:
                reason = "maximum_hold"
            elif state["policy"] == SHADOW_POLICY_FIXED_3X and multiple >= 3.0:
                reason = "fixed_take_profit_3x"
            elif (
                state["policy"] == SHADOW_POLICY_PURE_TRAILING
                and high >= PURE_TRAILING_ACTIVATION_MULTIPLE
                and multiple <= high * (1 - RUNNER_TRAILING_DRAWDOWN)
            ):
                reason = "pure_trailing_stop"
            if reason:
                connection.execute(
                    """
                    UPDATE position_policy_states SET status='closed',high_multiple=?,
                        exit_price_usd=?,exit_value_usd=?,exit_reason=?,closed_at=?,
                        net_multiple=?,last_mark_at=? WHERE token_address=? AND policy=?
                    """,
                    (
                        high, price, full_value, reason, now, multiple, now,
                        position["token_address"], state["policy"],
                    ),
                )
            else:
                connection.execute(
                    """
                    UPDATE position_policy_states SET high_multiple=?,last_mark_at=?
                    WHERE token_address=? AND policy=?
                    """,
                    (high, now, position["token_address"], state["policy"]),
                )

    def mark_position(self, token: str, market: dict, now: float) -> dict | None:
        price = safe_float(market.get("price_usd"), 0.0)
        liquidity = safe_float(market.get("liquidity_usd"), 0.0)
        market_cap = safe_float(market.get("market_cap_usd"), 0.0) or None
        if price <= 0 and not market.get("source"):
            return None
        with self.connection() as connection:
            position = connection.execute(
                "SELECT * FROM positions WHERE token_address=? AND status='open'", (token.lower(),)
            ).fetchone()
            if not position:
                return None
            self._mark_shadow_policies(connection, position, price, liquidity, now)
            original = safe_float(position["original_quantity"], position["quantity"])
            remaining = safe_float(position["quantity"], 0.0)
            realized = safe_float(position["realized_value_usd"], 0.0)
            marked_remaining = self._liquidation_value(remaining, price, liquidity)
            value = realized + marked_remaining
            multiple = value / max(0.01, position["cost_usd"])
            high = max(safe_float(position["high_multiple"], 1.0), multiple)
            age = now-position["opened_at"]
            price_multiple = price / max(1e-30, position["entry_price_usd"])
            reason = None
            partial_exits = []
            # Classify by CAUSE, not by which branch happens to be first.
            #
            # A rug satisfies both the liquidity and the stop-loss condition at
            # once, so ordering alone decided the label: liquidity_below_minimum
            # was checked first and absorbed every one. Measured on the first 8
            # closes, 5 exited with that label at exactly 0.0x and
            # high_multiple 1.0 -- entry liquidity $51k-$155k, price straight to
            # zero, never a cent above entry. Those are rugs, not thin markets,
            # and stop_loss had never once fired, so the stop was untested
            # rather than working.
            #
            # price_multiple is the discriminator: it is the token's own price
            # against entry, independent of pool depth and of the size-based
            # slippage in _liquidation_value (capped at 50%, so slippage alone
            # can never reach zero). A collapsed price with drained liquidity is
            # a rug; healthy price with thin liquidity is an exit-liquidity
            # problem; a real decline is a stop loss.
            if price_multiple <= PRICE_COLLAPSE_MULTIPLE:
                reason = "price_collapse"
            elif multiple <= STOP_LOSS_MULTIPLE:
                reason = "stop_loss"
            elif liquidity < MINIMUM_ENTRY_LIQUIDITY_USD:
                reason = "liquidity_below_minimum"
            elif age >= STAGNATION_EXIT_SECONDS and high < STAGNATION_MINIMUM_MULTIPLE:
                reason = "stagnation_24h"
            elif age >= MAXIMUM_HOLD_SECONDS:
                reason = "maximum_hold"
            if not reason and position["stage_one_sold_at"] is None and price_multiple >= STAGE_ONE_MULTIPLE:
                sold = min(remaining, original * STAGE_ONE_ORIGINAL_FRACTION)
                proceeds = self._liquidation_value(sold, price, liquidity)
                remaining -= sold; realized += proceeds
                partial_exits.append({"stage":"recover_principal_2x","quantity":sold,"value_usd":proceeds})
            if not reason and position["stage_two_sold_at"] is None and price_multiple >= STAGE_TWO_MULTIPLE:
                sold = min(remaining, original * STAGE_TWO_ORIGINAL_FRACTION)
                proceeds = self._liquidation_value(sold, price, liquidity)
                remaining -= sold; realized += proceeds
                partial_exits.append({"stage":"take_profit_3x","quantity":sold,"value_usd":proceeds})
            runner_high = max(
                safe_float(position["runner_high_multiple"], 1.0), price_multiple
            )
            stage_two_at = position["stage_two_sold_at"] or (
                now if any(item["stage"] == "take_profit_3x" for item in partial_exits) else None
            )
            if (
                not reason and stage_two_at is not None
                and price_multiple <= runner_high * (1 - RUNNER_TRAILING_DRAWDOWN)
            ):
                reason = "runner_trailing_stop"
            if reason:
                realized += self._liquidation_value(remaining, price, liquidity)
                remaining = 0.0
                value = realized
                multiple = value / max(0.01, position["cost_usd"])
                connection.execute(
                    """
                    UPDATE positions SET status='closed',high_multiple=?,last_price_usd=?,
                        last_liquidity_usd=?,last_market_cap_usd=?,
                        last_market_observed_at=?,last_mark_at=?,exit_price_usd=?,exit_value_usd=?,
                        exit_reason=?,closed_at=?,net_multiple=?,quantity=?,realized_value_usd=?,
                        stage_one_sold_at=COALESCE(stage_one_sold_at,?),
                        stage_two_sold_at=COALESCE(stage_two_sold_at,?),runner_high_multiple=?
                    WHERE token_address=?
                    """, (high,price,liquidity,market_cap,_utc_now(),now,price,value,
                            reason,now,multiple,remaining,realized,
                            now if any(item["stage"] == "recover_principal_2x" for item in partial_exits) else None,
                            now if any(item["stage"] == "take_profit_3x" for item in partial_exits) else None,
                            runner_high,token.lower())
                )
            else:
                value = realized + self._liquidation_value(remaining, price, liquidity)
                multiple = value / max(0.01, position["cost_usd"])
                connection.execute(
                    """
                    UPDATE positions SET high_multiple=?,last_price_usd=?,
                        last_liquidity_usd=?,last_market_cap_usd=?,
                        last_market_observed_at=?,last_mark_at=?,quantity=?,realized_value_usd=?,
                        stage_one_sold_at=COALESCE(stage_one_sold_at,?),
                        stage_two_sold_at=COALESCE(stage_two_sold_at,?),runner_high_multiple=?
                    WHERE token_address=?
                    """, (
                        high,price,liquidity,market_cap,_utc_now(),now,remaining,realized,
                        now if any(item["stage"] == "recover_principal_2x" for item in partial_exits) else None,
                        now if any(item["stage"] == "take_profit_3x" for item in partial_exits) else None,
                        runner_high,token.lower(),
                    ))
            return {
                "token_address":token.lower(),"multiple":multiple,"reason":reason,
                "value_usd":value,"partial_exits":partial_exits,
                "remaining_fraction":remaining/max(original,1e-30),
            }

    def summary(self) -> dict:
        with self.connection() as connection:
            candidate = connection.execute(
                """
                SELECT COUNT(*) total,
                  SUM(analysis_status='complete') analyzed,
                  SUM(analysis_status IN ('pending','retry')) pending,
                  SUM(analysis_status='failed') failed,
                  SUM(paper_entry_allowed=1) admitted,
                  SUM(paper_decision='rejected') rejected,
                  SUM(paper_decision='watching_for_executable_market') watching,
                  SUM(paper_decision='observing_above_entry_ceiling') above_entry_ceiling,
                  SUM(paper_decision='waiting_for_reentry_momentum') waiting_reentry_momentum,
                  SUM(paper_decision='expired_no_executable_market') watch_expired,
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
                  AVG(CASE WHEN status='closed' THEN net_multiple END) average_multiple,
                  SUM(stage_one_sold_at IS NOT NULL) stage_one_exits,
                  SUM(stage_two_sold_at IS NOT NULL) stage_two_exits,
                  SUM(grandfathered_above_cap=1) grandfathered_above_cap
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

    def recent_positions(
        self,
        limit: int = 50,
        live_markets: dict[str, dict] | None = None,
    ) -> list[dict]:
        """Return browser-safe paper marks, newest first.

        Open gains use the same estimated exit friction as mark_position, so the
        dashboard cannot display an optimistic friction-free percentage.
        """
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT p.*, c.name, c.pair_address, c.source_version,
                       c.first_market_cap_usd launch_market_cap_usd
                FROM positions p
                JOIN candidates c ON c.token_address=p.token_address
                ORDER BY p.opened_at DESC LIMIT ?
                """,
                (max(0, limit),),
            ).fetchall()
            policy_rows = connection.execute(
                "SELECT * FROM position_policy_states"
            ).fetchall()
        policies_by_token: dict[str, list[dict]] = {}
        for policy in policy_rows:
            policies_by_token.setdefault(policy["token_address"], []).append(dict(policy))
        positions = []
        live_markets = live_markets or {}
        for raw in rows:
            row = dict(raw)
            live = (
                live_markets.get(row["token_address"], {})
                if row.get("status") == "open"
                else {}
            )
            current_price = safe_float(
                live.get("price_usd") or row.get("last_price_usd"), 0.0
            )
            current_liquidity = safe_float(
                live.get("liquidity_usd") or row.get("last_liquidity_usd"), 0.0
            )
            current_market_cap = (
                safe_float(live.get("market_cap_usd"), 0.0)
                or row.get("last_market_cap_usd")
            )
            if row.get("status") == "closed" and row.get("net_multiple") is not None:
                multiple = safe_float(row.get("net_multiple"), 0.0)
            elif current_price > 0:
                marked_value = safe_float(row.get("realized_value_usd"), 0.0) + self._liquidation_value(
                    safe_float(row.get("quantity"), 0.0), current_price,
                    current_liquidity,
                )
                multiple = marked_value / max(0.01, safe_float(row.get("cost_usd"), PAPER_COST_USD))
            else:
                multiple = None
            policy_results = []
            for policy in policies_by_token.get(row["token_address"], []):
                policy_multiple = policy.get("net_multiple")
                if policy_multiple is None and current_price > 0:
                    policy_multiple = self._liquidation_value(
                        safe_float(row.get("original_quantity"), row.get("quantity")),
                        current_price, current_liquidity,
                    ) / max(0.01, safe_float(row.get("cost_usd"), PAPER_COST_USD))
                policy_results.append({
                    "policy": policy["policy"], "status": policy["status"],
                    "multiple": policy_multiple,
                    "gain_pct": (policy_multiple - 1) * 100 if policy_multiple is not None else None,
                    "high_multiple": policy.get("high_multiple"),
                    "exit_reason": policy.get("exit_reason"),
                })
            original_quantity = safe_float(
                row.get("original_quantity"), row.get("quantity")
            )
            positions.append({
                "token_address": row["token_address"],
                "pair_address": row.get("pair_address"),
                "source_version": row.get("source_version"),
                "name": row.get("name") or "",
                "symbol": row.get("symbol") or "",
                "status": row.get("status"),
                "opened_at": row.get("opened_at"),
                "entry_price_usd": row.get("entry_price_usd"),
                "exit_price_usd": row.get("exit_price_usd"),
                "exit_value_usd": row.get("exit_value_usd"),
                "closed_at": row.get("closed_at"),
                "current_price_usd": current_price or None,
                "launch_market_cap_usd": row.get("launch_market_cap_usd"),
                "entry_market_cap_usd": row.get("entry_market_cap_usd"),
                "current_market_cap_usd": current_market_cap,
                "gain_pct": (multiple - 1) * 100 if multiple is not None else None,
                "multiple": multiple,
                "high_multiple": row.get("high_multiple"),
                "remaining_fraction": safe_float(row.get("quantity"), 0.0) / max(original_quantity, 1e-30),
                "realized_value_usd": row.get("realized_value_usd"),
                "stage_one_sold": row.get("stage_one_sold_at") is not None,
                "stage_two_sold": row.get("stage_two_sold_at") is not None,
                "entry_policy_version": row.get("entry_policy_version"),
                "grandfathered_above_cap": bool(row.get("grandfathered_above_cap")),
                "counterfactual_policies": policy_results,
                "last_mark_at": row.get("last_mark_at"),
                "market_observed_at": (
                    live.get("observed_at") or row.get("last_market_observed_at")
                ),
                "market_source": live.get("source") or "stored_paper_mark",
                "exit_reason": row.get("exit_reason"),
                "paper_only": True,
            })
        return positions

    def recent_analyzed_tokens(self, limit: int = 100) -> list[dict]:
        with self.connection() as connection:
            rows = connection.execute(
                """
                SELECT token_address,pair_address,name,symbol,analyzed_at,score,
                       risk_level,action_label,paper_entry_allowed,source_version,pool_id,
                       paper_decision,hard_stops_json,market_watch_checks,
                       market_watch_last_checked_at,market_watch_expires_at,
                       market_watch_reason
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
                "paper_decision": row["paper_decision"] or (
                    PAPER_DECISION_ADMITTED if row["paper_entry_allowed"] else PAPER_DECISION_REJECTED
                ),
                "hard_stops": json.loads(row["hard_stops_json"] or "[]"),
                "market_watch_checks": row["market_watch_checks"] or 0,
                "market_watch_last_checked_at": row["market_watch_last_checked_at"],
                "market_watch_expires_at": row["market_watch_expires_at"],
                "market_watch_reason": row["market_watch_reason"],
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

    def _adaptive_logs(self, start: int, end: int) -> tuple[list[dict], int]:
        """Split deterministic provider-limit failures without moving the cursor."""
        pending = [(start, end)]
        logs: list[dict] = []
        successful_windows = 0
        while pending:
            window_start, window_end = pending.pop()
            try:
                window_logs = _remote_call(
                    f"Robinhood V4 logs {window_start}-{window_end}",
                    lambda window_start=window_start, window_end=window_end: self.rpc.get_logs(
                        window_start,
                        window_end,
                        address=UNISWAP_V4_POOL_MANAGER,
                        topics=[[
                            V4_INITIALIZE_TOPIC,
                            V4_MODIFY_LIQUIDITY_TOPIC,
                            V4_SWAP_TOPIC,
                        ]],
                    ),
                )
            except RuntimeError as exc:
                if (
                    "exceeds limit of 10000" not in str(exc).lower()
                    or window_start >= window_end
                ):
                    raise
                midpoint = (window_start + window_end) // 2
                # LIFO order keeps requests and accumulated events chronological.
                pending.append((midpoint + 1, window_end))
                pending.append((window_start, midpoint))
                continue
            logs.extend(window_logs or [])
            successful_windows += 1
        return logs, successful_windows

    def sync(self, *, block_limit: int, lookback: int) -> tuple[list[dict], dict]:
        latest = _remote_call("Robinhood latest block", self.rpc.get_block_number)
        state = self._state()
        start = safe_int(state.get("next_block"), max(0, latest - lookback))
        start = min(start, latest)
        end = min(latest, start + max(1, block_limit) - 1)
        logs, rpc_windows = self._adaptive_logs(start, end)
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
            "rpc_windows": rpc_windows,
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

    def snapshots(self, token: str) -> list[dict]:
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
            labels = [str(value).lower() for value in pair.get("labels") or []]
            version = (
                SOURCE_V4 if "v4" in labels else
                SOURCE_V3 if "v3" in labels else SOURCE_V2
            )
            matches.append({
                "pair_address":pair.get("pairAddress"),"dex_id":pair.get("dexId"),
                "price_usd":safe_float(pair.get("priceUsd"),0.0) or None,
                "liquidity_usd":safe_float((pair.get("liquidity") or {}).get("usd"),0.0) or None,
                "market_cap_usd":safe_float(pair.get("marketCap"),0.0) or None,
                "fdv_usd":safe_float(pair.get("fdv"),0.0) or None,
                "pair_symbol":(pair.get("baseToken") or {}).get("symbol"),
                "source":"dexscreener_exact_chain_token_pair",
                "source_version": version,
                "labels": labels,
            })
        return matches

    def snapshot(self, token: str, pair_address: str | None = None) -> dict:
        matches = self.snapshots(token)
        if not matches:
            return {}
        exact = [
            market for market in matches
            if pair_address and str(market.get("pair_address") or "").lower() == pair_address.lower()
        ]
        choices = exact or matches
        return max(
            choices,
            key=lambda market: safe_float(market.get("liquidity_usd"), 0.0),
        )

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

    def _best_executable_market(self, candidate: dict) -> dict:
        """Find the most liquid safely identifiable venue for a watched token."""
        choices = []
        for market in self.market.snapshots(candidate["token_address"]):
            version = market.get("source_version")
            if version == SOURCE_V4:
                pool = self.store.v4_pool(str(market.get("pair_address") or ""))
                # Unknown V4 pools cannot be assumed hook-free.
                if not pool or str(pool.get("hooks_address") or ZERO_ADDRESS).lower() != ZERO_ADDRESS:
                    continue
            if (
                safe_float(market.get("price_usd"), 0.0) > 0
                and safe_float(market.get("liquidity_usd"), 0.0)
                    >= MINIMUM_ENTRY_LIQUIDITY_USD
            ):
                choices.append(market)
        if not choices:
            return {}
        return max(
            choices,
            key=lambda market: safe_float(market.get("liquidity_usd"), 0.0),
        )

    def recheck_executable_markets(self, now: float, limit: int) -> dict:
        checked = executable = entries = failures = 0
        for candidate in self.store.pending_market_watches(now, limit):
            checked += 1
            try:
                market = self._best_executable_market(candidate)
                if not market:
                    self.store.record_market_watch_check(candidate["token_address"])
                    continue
                market_cap = safe_float(market.get("market_cap_usd"), 0.0)
                price = safe_float(market.get("price_usd"), 0.0)
                if market_cap <= 0 or market_cap > MAXIMUM_ENTRY_MARKET_CAP_USD:
                    self.store.record_market_watch_check(
                        candidate["token_address"], market,
                        reason="above_entry_market_cap_ceiling",
                        update_reference=True, decision=PAPER_DECISION_ABOVE_CAP,
                    )
                    continue
                if candidate.get("paper_decision") in {
                    PAPER_DECISION_ABOVE_CAP, PAPER_DECISION_REENTRY,
                }:
                    reference_price = safe_float(
                        candidate.get("market_watch_reference_price_usd"), 0.0
                    )
                    reference_cap = safe_float(
                        candidate.get("market_watch_reference_market_cap_usd"), 0.0
                    )
                    if (
                        reference_cap > MAXIMUM_ENTRY_MARKET_CAP_USD
                        or reference_price <= 0
                        or price < reference_price * REENTRY_MOMENTUM_MULTIPLE
                    ):
                        self.store.record_market_watch_check(
                            candidate["token_address"], market,
                            reason="waiting_for_reentry_momentum",
                            update_reference=True, decision=PAPER_DECISION_REENTRY,
                        )
                        continue
                self.store.record_market_watch_check(
                    candidate["token_address"], market,
                    reason="executable_market_found",
                )
                executable += 1
                self.store.select_execution_pool(candidate["token_address"], market)
                current = self.store.candidate(candidate["token_address"])
                report = self._analyzer().analyze_token(
                    current["token_address"], seal=False, defer_cognition=True,
                )
                if report.get("error"):
                    raise RuntimeError(report["error"])
                analysis = dict(report.get("analysis") or {})
                stops = list(analysis.get("hard_stop_overrides") or [])
                if current.get("source_version") == SOURCE_V4:
                    pool = self.store.v4_pool(current.get("pool_id") or "")
                    if not pool or str(pool.get("hooks_address") or ZERO_ADDRESS).lower() != ZERO_ADDRESS:
                        stops.append({
                            "code": "V4_HOOK_UNAUDITED", "severity": "High",
                            "reason": "The selected V4 pool hook could not be verified as absent",
                            "action": "AVOID",
                        })
                analysis["hard_stop_overrides"] = stops
                self.store.record_analysis(
                    current["token_address"], analysis, market,
                    priority_reason="executable_market_recheck",
                    queue_age_seconds=max(
                        0.0, now - safe_float(current.get("market_watch_started_at"), now)
                    ),
                )
                latest = self.store.candidate(current["token_address"])
                if self.store.open_position(latest, market):
                    entries += 1
                    self.ledger.append("robinhood_paper_buy", {
                        "token_address": current["token_address"],
                        "symbol": latest.get("symbol"),
                        "score": latest.get("score"),
                        "price_usd": market.get("price_usd"),
                        "liquidity_usd": market.get("liquidity_usd"),
                        "market_cap_usd": market.get("market_cap_usd"),
                        "entry_trigger": "future_executable_market_recheck",
                        "paper_only": True,
                    })
            except Exception:
                failures += 1
        return {
            "checked": checked, "executable_markets_found": executable,
            "paper_entries": entries, "failures": failures, "limit": limit,
        }

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
            observed+=1
            no_market+=not bool(market)
        return {"observed":observed,"no_market":no_market,"failures":failures,"expired_missed":expired,"position_marks":marks,"limit":limit}

    def evaluate_open_positions(self, now: float) -> dict:
        """Evaluate every open paper position independently of outcome horizons."""
        with self.store.connection() as connection:
            candidates = [dict(row) for row in connection.execute(
                """
                SELECT c.* FROM candidates c JOIN positions p USING(token_address)
                WHERE p.status='open' ORDER BY p.opened_at
                """
            )]
        checked = marked = closed = partial_exits = failures = 0
        for candidate in candidates:
            checked += 1
            try:
                market = (
                    self.v4_market.snapshot(candidate)
                    if candidate.get("source_version") == SOURCE_V4
                    else self.market.snapshot(
                        candidate["token_address"], candidate["pair_address"]
                    )
                )
                mark = self.store.mark_position(
                    candidate["token_address"], market, now
                )
                if not mark:
                    continue
                marked += 1
                closed += bool(mark.get("reason"))
                partial_exits += len(mark.get("partial_exits") or [])
                self.ledger.append("robinhood_paper_mark", mark)
            except Exception:
                failures += 1
        return {
            "checked": checked, "marked": marked, "closed": closed,
            "partial_exits": partial_exits, "failures": failures,
            "cadence": "every_learning_cycle",
        }

    def run_once(self, *, discovery_block_limit=DEFAULT_DISCOVERY_BLOCK_LIMIT,
                 analysis_limit=DEFAULT_ANALYSIS_LIMIT,outcome_limit=DEFAULT_OUTCOME_LIMIT,
                 market_recheck_limit=DEFAULT_MARKET_RECHECK_LIMIT,
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
                position_evaluations = self.evaluate_open_positions(now)
                market_rechecks = self.recheck_executable_markets(
                    now, market_recheck_limit
                )
                analyses = analysis_failures = entries = momentum_analyses = 0
                queue_ages = []
                for candidate in self.store.pending_analysis(analysis_limit):
                    try:
                        priority_reason = (
                            "liquid_momentum"
                            if candidate.get("momentum_priority_multiple") is not None
                            else "oldest_fairness"
                        )
                        discovered_at = _timestamp(candidate.get("discovered_at"))
                        queue_age = (
                            max(0.0, time.time() - discovered_at)
                            if discovered_at is not None else None
                        )
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
                            candidate["token_address"], analysis, market,
                            priority_reason=priority_reason,
                            queue_age_seconds=queue_age,
                        )
                        momentum_analyses += priority_reason == "liquid_momentum"
                        if queue_age is not None:
                            queue_ages.append(queue_age)
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
                        "momentum_analyses": momentum_analyses,
                        "maximum_analysis_queue_age_seconds": (
                            round(max(queue_ages), 3) if queue_ages else None
                        ),
                        "analysis_failures": analysis_failures,
                        "paper_entries": entries + market_rechecks["paper_entries"],
                        "market_rechecks": market_rechecks,
                        "position_evaluations": position_evaluations,
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


def dashboard_snapshot(
    root: str | Path,
    *,
    live_position_markets: dict[str, dict] | None = None,
    market_refresh_errors: dict[str, str] | None = None,
) -> dict:
    root=Path(root)
    store=RobinhoodLearningStore(root/"learning.sqlite3")
    summary=read_json(root/"learning_summary.json",{}) or {}
    cursor=read_json(root/"discovery_cursor.json",{}) or {}
    v4_cursor=read_json(root/"discovery_v4_cursor.json",{}) or {}
    reflection_state=read_json(root/"reflection_state.json",{}) or {}
    reflection_result=read_json(
        reflection_state.get("latest_result") or root/"reflection_missing.json", {}
    ) or {}
    counterfactual_audit=read_json(root/"counterfactual_audit.json",{}) or {}
    positions = store.recent_positions(live_markets=live_position_markets)
    return {
        "timestamp":_utc_now(),"network":"robinhood","chain_id":ROBINHOOD_NETWORK.chain_id,
        "learning":store.summary(),
        "positions":[position for position in positions if position.get("status") == "open"],
        "closed_positions":[
            position for position in positions if position.get("status") == "closed"
        ],
        "analyzed_tokens":store.recent_analyzed_tokens(),
        "last_cycle":summary.get("cycle") or {},
        "discovery_coverage":cursor.get("coverage") or summary.get("discovery_coverage") or {},
        "discovery_coverage_by_source":{
            "uniswap_v2":cursor.get("coverage") or {},
            "uniswap_v4":v4_cursor.get("coverage") or {},
        },
        "paper_only":True,"live_execution_enabled":False,
        "market_refresh_errors": market_refresh_errors or {},
        "reflection": {
            "last_checkpoint": reflection_state.get("last_checkpoint", 0),
            "next_checkpoint": reflection_state.get("next_checkpoint", 15),
            "winner": reflection_result.get("winner"),
            "recommendations": reflection_result.get("recommendations") or [],
            "counterfactual_audit": counterfactual_audit,
        },
    }


class RobinhoodDashboardMarketRefresher:
    """Fetch exact-pool marks for every open position on each dashboard update."""

    def __init__(self, root: str | Path, *, engine=None):
        self.engine = engine or RobinhoodLearningEngine(root)
        self.lock = threading.Lock()

    def snapshot(self) -> dict:
        markets: dict[str, dict] = {}
        errors: dict[str, str] = {}
        with self.lock:
            with self.engine.store.connection() as connection:
                candidates = [dict(row) for row in connection.execute(
                    """
                    SELECT c.* FROM candidates c JOIN positions p USING(token_address)
                    WHERE p.status='open' ORDER BY p.opened_at
                    """
                )]
            for candidate in candidates:
                token = candidate["token_address"]
                try:
                    market = (
                        self.engine.v4_market.snapshot(candidate)
                        if candidate.get("source_version") == SOURCE_V4
                        else self.engine.market.snapshot(token, candidate["pair_address"])
                    )
                    if market:
                        markets[token] = {**market, "observed_at": _utc_now()}
                except Exception as exc:
                    errors[token] = str(exc)
        return dashboard_snapshot(
            self.engine.root,
            live_position_markets=markets,
            market_refresh_errors=errors,
        )


def serve_dashboard(root: str | Path, host: str, port: int) -> None:
    if host not in {"127.0.0.1","localhost"}:
        raise ValueError("Robinhood dashboard is local-only")
    html_path=Path(__file__).with_name("robinhood_dashboard.html")
    refresher=RobinhoodDashboardMarketRefresher(root)
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path in {"/","/index.html"}:
                content=html_path.read_bytes(); content_type="text/html; charset=utf-8"
            elif self.path=="/api/status":
                content=json.dumps(refresher.snapshot()).encode(); content_type="application/json"
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
    parser.add_argument(
        "command",
        choices=("learn-once","status","dashboard","verify","reflect","audit"),
    )
    parser.add_argument("--root",default=DEFAULT_ROOT)
    parser.add_argument("--host",default="127.0.0.1")
    parser.add_argument("--port",type=int,default=DEFAULT_DASHBOARD_PORT)
    parser.add_argument("--discovery-block-limit",type=int,default=DEFAULT_DISCOVERY_BLOCK_LIMIT)
    parser.add_argument("--analysis-limit",type=int,default=DEFAULT_ANALYSIS_LIMIT)
    parser.add_argument("--outcome-limit",type=int,default=DEFAULT_OUTCOME_LIMIT)
    parser.add_argument(
        "--market-recheck-limit", type=int,
        default=DEFAULT_MARKET_RECHECK_LIMIT,
    )
    parser.add_argument("--lookback",type=int,default=DEFAULT_DISCOVERY_LOOKBACK_BLOCKS)
    parser.add_argument(
        "--todo", default=str(Path(__file__).with_name("TODO.md"))
    )
    parser.add_argument("--skill-root",default=str(default_skill_root()))
    args=parser.parse_args()
    if args.command=="dashboard":
        serve_dashboard(args.root,args.host,args.port); return
    if args.command=="status":
        print(json.dumps(dashboard_snapshot(args.root),indent=2)); return
    engine=RobinhoodLearningEngine(args.root)
    if args.command=="verify":
        result=engine.verify(); print(json.dumps(result,indent=2)); raise SystemExit(0 if result["ok"] else 1)
    reflection = RobinhoodReflectionCoordinator(
        args.root,
        engine.store,
        todo_path=args.todo,
        skill_root=args.skill_root,
    )
    if args.command=="reflect":
        print(json.dumps(reflection.run_if_due(),indent=2)); return
    if args.command=="audit":
        print(json.dumps(reflection.audit_all(),indent=2)); return
    summary=engine.run_once(
        discovery_block_limit=max(1,args.discovery_block_limit),
        analysis_limit=max(0,args.analysis_limit),outcome_limit=max(0,args.outcome_limit),
        market_recheck_limit=max(0,args.market_recheck_limit),
        lookback=max(1,args.lookback),
    )
    try:
        summary["reflection"] = reflection.run_if_due()
    except Exception as exc:
        # Reflection is retried from the same durable checkpoint next cycle. A
        # sealing/tooling failure must not turn completed paper learning into a
        # failed analysis cycle.
        summary["reflection"] = {
            "status": "retry_pending",
            "error": str(exc),
        }
    print(json.dumps(summary,indent=2))


if __name__=="__main__":
    main()
