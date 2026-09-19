"""Prospective, executable-shadow evidence for Robinhood V2 launches.

This is deliberately a sidecar, not an extension of the frozen V4 Flow
cohort.  It observes only envelopes discovered after its own arm time and
never submits a transaction, opens a paper position, or modifies an existing
experiment.  Each accepted observation and later mark is hash chained.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from chainseer import WETH_ADDRESS, RobinhoodRPC
from chainseer_core import atomic_json_write
from chainseer_fresh_discovery import LEDGER_NAME as DISCOVERY_LEDGER_NAME


POLICY_VERSION = "v2-executable-shadow-v1"
LEDGER_NAME = "v2_executable_shadow.sqlite3"
STATUS_NAME = "v2_executable_shadow_status.json"
GENESIS_HASH = "0" * 64
SOURCE_VERSION = "uniswap_v2"
MAXIMUM_OBSERVATION_LAG_BLOCKS = 120
BLOCKS_PER_SECOND = 10
FINALITY_BLOCKS = 20
FRICTION_BPS = 100
ENTRY_ANCHOR_RAW = {"wrapped_native": 30_000_000_000_000_000,
                    "stable": 100_000_000}
SCHEDULE = (("entry", 0), ("15m", 15 * 60), ("1h", 60 * 60),
            ("6h", 6 * 60 * 60), ("24h", 24 * 60 * 60))

GET_RESERVES_SELECTOR = "0902f1ac"
TOKEN0_SELECTOR = "0dfe1681"
TOKEN1_SELECTOR = "d21220a7"
USDG_ADDRESS = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def _address(raw: str) -> str:
    return "0x" + str(raw or "").removeprefix("0x")[-40:].lower()


def _reserve_words(raw: str) -> tuple[int, int]:
    data = str(raw or "").removeprefix("0x")
    if len(data) < 128:
        raise ValueError("truncated getReserves response")
    return int(data[:64], 16), int(data[64:128], 16)


def _constant_product_out(amount_in: int, reserve_in: int, reserve_out: int) -> int:
    if amount_in <= 0 or reserve_in <= 0 or reserve_out <= 0:
        return 0
    # Uniswap V2's 0.30% swap fee.  Integer arithmetic deliberately rounds
    # down just as the pair contract does.
    amount_with_fee = amount_in * 997
    return amount_with_fee * reserve_out // (reserve_in * 1000 + amount_with_fee)


class ExecutableShadowStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=0.1)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout=100")
        return connection

    def _initialize(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS observations (
                    observation_id TEXT PRIMARY KEY, policy_version TEXT NOT NULL,
                    envelope_id TEXT NOT NULL UNIQUE, source_version TEXT NOT NULL,
                    pool_address TEXT NOT NULL, token_address TEXT NOT NULL,
                    anchor_address TEXT NOT NULL, anchor_kind TEXT NOT NULL,
                    entry_anchor_raw TEXT NOT NULL, observed_block INTEGER NOT NULL,
                    observed_head INTEGER NOT NULL, created_at REAL NOT NULL,
                    previous_hash TEXT NOT NULL, record_hash TEXT NOT NULL UNIQUE
                );
                CREATE TABLE IF NOT EXISTS marks (
                    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
                    observation_id TEXT NOT NULL, label TEXT NOT NULL,
                    target_block INTEGER NOT NULL, target_at REAL NOT NULL,
                    status TEXT NOT NULL, observed_at REAL NOT NULL,
                    quote_json TEXT, net_return REAL, exit_valid INTEGER NOT NULL,
                    previous_hash TEXT NOT NULL, record_hash TEXT NOT NULL UNIQUE,
                    UNIQUE(observation_id,label),
                    FOREIGN KEY(observation_id) REFERENCES observations(observation_id)
                );
                CREATE INDEX IF NOT EXISTS executable_shadow_marks_due
                    ON marks(target_block,observation_id,label);
                CREATE TRIGGER IF NOT EXISTS executable_shadow_observations_no_update
                BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT,'append-only'); END;
                CREATE TRIGGER IF NOT EXISTS executable_shadow_observations_no_delete
                BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT,'append-only'); END;
                CREATE TRIGGER IF NOT EXISTS executable_shadow_marks_no_update
                BEFORE UPDATE ON marks BEGIN SELECT RAISE(ABORT,'append-only'); END;
                CREATE TRIGGER IF NOT EXISTS executable_shadow_marks_no_delete
                BEFORE DELETE ON marks BEGIN SELECT RAISE(ABORT,'append-only'); END;
            """)

    def arm(self, now: float | None = None) -> float:
        armed_at = time.time() if now is None else float(now)
        with self._connect() as db:
            row = db.execute("SELECT value FROM state WHERE key='armed_at'").fetchone()
            if row:
                return float(row[0])
            db.execute("INSERT INTO state VALUES ('armed_at',?)", (str(armed_at),))
        return armed_at

    def armed_at(self) -> float | None:
        with self._connect() as db:
            row = db.execute("SELECT value FROM state WHERE key='armed_at'").fetchone()
        return float(row[0]) if row else None

    def append_observation(self, envelope: dict) -> bool:
        source = str(envelope["source_version"])
        if source != SOURCE_VERSION:
            return False
        currency0, currency1 = (str(envelope.get(key) or "").lower()
                               for key in ("currency0", "currency1"))
        anchors = {WETH_ADDRESS.lower(): "wrapped_native", USDG_ADDRESS: "stable"}
        anchor = currency0 if currency0 in anchors else currency1 if currency1 in anchors else ""
        token = currency1 if anchor == currency0 else currency0
        if not anchor or not token or token != str(envelope["token_address"]).lower():
            return False
        identity = {"policy_version": POLICY_VERSION, "envelope_id": envelope["envelope_id"]}
        observation_id = digest(identity)
        with self._connect() as db:
            previous_row = db.execute("SELECT record_hash FROM observations ORDER BY rowid DESC LIMIT 1").fetchone()
            previous = str(previous_row[0]) if previous_row else GENESIS_HASH
            payload = {**identity, "source_version": source,
                       "pool_address": str(envelope["pool_address"]).lower(),
                       "token_address": token, "anchor_address": anchor,
                       "anchor_kind": anchors[anchor],
                       "entry_anchor_raw": str(ENTRY_ANCHOR_RAW[anchors[anchor]]),
                       "observed_block": int(envelope["block_number"]),
                       "observed_head": int(envelope["observed_head"]),
                       "created_at": float(envelope["created_at"]), "previous_hash": previous}
            inserted = db.execute("""
                INSERT OR IGNORE INTO observations VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (observation_id, POLICY_VERSION, envelope["envelope_id"], source,
                  payload["pool_address"], token, anchor, payload["anchor_kind"],
                  payload["entry_anchor_raw"], payload["observed_block"],
                  payload["observed_head"], payload["created_at"], previous,
                  digest(payload))).rowcount
            if not inserted:
                return False
            for label, seconds in SCHEDULE:
                target_block = payload["observed_block"] + seconds * BLOCKS_PER_SECOND
                target_at = payload["created_at"] + seconds
                schedule_hash = digest({"observation_id": observation_id, "label": label,
                                        "target_block": target_block, "target_at": target_at,
                                        "policy_version": POLICY_VERSION})
                db.execute("""INSERT INTO marks(observation_id,label,target_block,target_at,status,observed_at,quote_json,net_return,exit_valid,previous_hash,record_hash)
                    VALUES (?,?,?,?, 'pending',0,NULL,NULL,0,'',?)""",
                    (observation_id, label, target_block, target_at, schedule_hash))
        return True

    def due_marks(self, head_block: int, limit: int = 12) -> list[dict]:
        finalized = int(head_block) - FINALITY_BLOCKS
        with self._connect() as db:
            return [dict(row) for row in db.execute("""
                SELECT m.*,o.pool_address,o.token_address,o.anchor_address,o.anchor_kind,o.entry_anchor_raw,o.observed_block
                FROM marks m JOIN observations o USING(observation_id)
                LEFT JOIN marks resolved ON resolved.observation_id=m.observation_id
                  AND resolved.label=m.label || ':resolved'
                LEFT JOIN marks entry ON entry.observation_id=m.observation_id
                  AND entry.label='entry:resolved'
                WHERE m.status='pending' AND resolved.sequence IS NULL AND m.target_block<=?
                  AND (m.label='entry' OR entry.status='observed')
                ORDER BY m.target_block,m.observation_id,m.label LIMIT ?
            """, (finalized, max(0, int(limit))))]

    def append_mark(self, due: dict, *, quote: dict, now: float | None = None) -> bool:
        observed_at = time.time() if now is None else float(now)
        status = "observed" if quote.get("verified") else "unmarketable"
        with self._connect() as db:
            previous_row = db.execute("SELECT record_hash FROM marks WHERE record_hash<>'' ORDER BY sequence DESC LIMIT 1").fetchone()
            previous = str(previous_row[0]) if previous_row else GENESIS_HASH
            payload = {"observation_id": due["observation_id"], "label": due["label"],
                       "target_block": int(due["target_block"]), "status": status,
                       "observed_at": observed_at, "quote": quote,
                       "net_return": quote.get("net_return"),
                       "exit_valid": int(bool(quote.get("exitable"))),
                       "previous_hash": previous}
            # Pending rows reserve target membership.  Updating them would
            # violate append-only semantics, so append a resolution record and
            # retain the reservation as the immutable precommitment.
            db.execute("""INSERT INTO marks(observation_id,label,target_block,target_at,status,observed_at,quote_json,net_return,exit_valid,previous_hash,record_hash)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (
                due["observation_id"], due["label"] + ":resolved", int(due["target_block"]),
                float(due["target_at"]), status, observed_at, canonical(quote),
                quote.get("net_return"), int(bool(quote.get("exitable"))), previous, digest(payload)))
            return True

    def snapshot(self) -> dict:
        with self._connect() as db:
            observations = int(db.execute("SELECT COUNT(*) FROM observations").fetchone()[0])
            pending = int(db.execute("""SELECT COUNT(*) FROM marks m WHERE m.status='pending'
                AND NOT EXISTS (SELECT 1 FROM marks r WHERE r.observation_id=m.observation_id
                  AND r.label=m.label || ':resolved')""").fetchone()[0])
            resolved = int(db.execute("SELECT COUNT(*) FROM marks WHERE status IN ('observed','unmarketable')").fetchone()[0])
            executable = int(db.execute("SELECT COUNT(*) FROM marks WHERE status='observed' AND exit_valid=1").fetchone()[0])
        return {"policy_version": POLICY_VERSION, "observations": observations,
                "pending_marks": pending, "resolved_marks": resolved,
                "exitable_marks": executable, "shadow_only": True,
                "paper_execution_enabled": False, "live_execution_enabled": False}


class V2ExecutableShadow:
    def __init__(self, root: str | Path, *, rpc: RobinhoodRPC | None = None):
        self.root = Path(root)
        self.store = ExecutableShadowStore(self.root / LEDGER_NAME)
        self.rpc = rpc or RobinhoodRPC()

    def _quote(self, due: dict) -> dict:
        block = int(due["target_block"])
        pool = str(due["pool_address"])
        try:
            token0 = _address(self.rpc.call(pool, "0x" + TOKEN0_SELECTOR, block=block))
            token1 = _address(self.rpc.call(pool, "0x" + TOKEN1_SELECTOR, block=block))
            reserve0, reserve1 = _reserve_words(self.rpc.call(pool, "0x" + GET_RESERVES_SELECTOR, block=block))
            anchor, token = str(due["anchor_address"]), str(due["token_address"])
            if {token0, token1} != {anchor, token}:
                return {"verified": False, "exitable": False, "quote_block": block,
                        "reason": "pair_identity_mismatch"}
            reserve_anchor, reserve_token = (reserve0, reserve1) if token0 == anchor else (reserve1, reserve0)
            if str(due["label"]) == "entry":
                token_out = _constant_product_out(int(due["entry_anchor_raw"]), reserve_anchor, reserve_token)
                anchor_out = _constant_product_out(token_out, reserve_token, reserve_anchor)
                ratio = anchor_out / max(1, int(due["entry_anchor_raw"]))
                return {"verified": token_out > 0 and anchor_out > 0, "exitable": ratio >= .90,
                        "quote_block": block, "token_out_raw": str(token_out),
                        "anchor_out_raw": str(anchor_out), "round_trip_ratio": ratio,
                        "net_return": ratio * (1 - FRICTION_BPS / 10_000) - 1}
            # Later marks need the fixed entry inventory from the sealed entry mark.
            entry = self._entry_quote(str(due["observation_id"]))
            token_in = int((entry or {}).get("token_out_raw") or 0)
            if token_in <= 0:
                return {"verified": False, "exitable": False, "quote_block": block,
                        "reason": "entry_inventory_unavailable"}
            anchor_out = _constant_product_out(token_in, reserve_token, reserve_anchor)
            ratio = anchor_out / max(1, int(due["entry_anchor_raw"]))
            return {"verified": anchor_out > 0, "exitable": anchor_out > 0,
                    "quote_block": block, "token_in_raw": str(token_in),
                    "anchor_out_raw": str(anchor_out), "net_return": ratio * (1 - FRICTION_BPS / 10_000) - 1}
        except Exception as exc:
            return {"verified": False, "exitable": False, "quote_block": block,
                    "reason": "rpc_or_archive_failure", "error": str(exc)[:240]}

    def _entry_quote(self, observation_id: str) -> dict | None:
        with self.store._connect() as db:
            row = db.execute("SELECT quote_json FROM marks WHERE observation_id=? AND label='entry:resolved'", (observation_id,)).fetchone()
        try:
            return json.loads(row[0]) if row and row[0] else None
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    def run_once(self, *, head_block: int | None = None, limit: int = 12) -> dict:
        now = time.time()
        armed_at = self.store.arm(now)
        enrolled = 0
        discovery = self.root / DISCOVERY_LEDGER_NAME
        if discovery.exists():
            with sqlite3.connect(f"file:{discovery.resolve().as_posix()}?mode=ro", uri=True) as db:
                db.row_factory = sqlite3.Row
                envelopes = db.execute("""SELECT * FROM launch_envelopes
                    WHERE source_version=? AND created_at>=? AND observed_head-block_number<=?
                    ORDER BY created_at,envelope_id LIMIT 50""",
                    (SOURCE_VERSION, armed_at, MAXIMUM_OBSERVATION_LAG_BLOCKS)).fetchall()
                enrolled = sum(self.store.append_observation(dict(row)) for row in envelopes)
        head = int(head_block if head_block is not None else self.rpc.get_block_number())
        due = self.store.due_marks(head, limit)
        observed = 0
        for row in due:
            self.store.append_mark(row, quote=self._quote(row), now=now)
            observed += 1
        status = {**self.store.snapshot(), "armed_at": armed_at, "head_block": head,
                  "enrolled": enrolled, "marks_attempted": observed}
        atomic_json_write(self.root / STATUS_NAME, status)
        return status
