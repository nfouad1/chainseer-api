"""V2 prospective executable-shadow lane with first-liquidity entry timing.

V1 bound entry to ``PairCreated`` and correctly showed that this is often
before liquidity exists.  This new policy keeps the 120-block observation
bound, but binds the entry to the first V2 ``Sync`` after pair creation.  It is
an independent, append-only experiment; no paper or live execution exists.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from pathlib import Path
from typing import Any

from chainseer import WETH_ADDRESS, RobinhoodRPC, UNISWAP_V2_FACTORY
from chainseer_core import atomic_json_write

POLICY_VERSION = "v2-executable-shadow-v2-first-liquidity"
LEDGER_NAME = "v2_executable_shadow_v2.sqlite3"
STATUS_NAME = "v2_executable_shadow_v2_status.json"
GENESIS_HASH = "0" * 64
SOURCE_VERSION = "uniswap_v2"
MAXIMUM_OBSERVATION_LAG_BLOCKS = 120
BLOCKS_PER_SECOND, FINALITY_BLOCKS, FRICTION_BPS = 10, 20, 100
ENTRY_ANCHOR_RAW = {"wrapped_native": 30_000_000_000_000_000, "stable": 100_000_000}
SCHEDULE = (("entry", 0), ("15m", 900), ("1h", 3600), ("6h", 21600), ("24h", 86400))
SYNC_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"
PAIR_CREATED_TOPIC = "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9"
GET_RESERVES_SELECTOR, TOKEN0_SELECTOR, TOKEN1_SELECTOR = "0902f1ac", "0dfe1681", "d21220a7"
USDG_ADDRESS = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"


def canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: Any) -> str:
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _address(raw: str) -> str:
    return "0x" + str(raw or "").removeprefix("0x")[-40:].lower()


def _integer(raw: object) -> int:
    return int(str(raw or "0x0"), 16)


def _topic_address(raw: str) -> str:
    return "0x" + str(raw or "")[-40:].lower()


def _data_address(raw: str) -> str:
    data = str(raw or "").removeprefix("0x")
    return "0x" + data[24:64].lower() if len(data) >= 64 else ""


def _reserves(raw: str) -> tuple[int, int]:
    value = str(raw or "").removeprefix("0x")
    if len(value) < 128:
        raise ValueError("truncated getReserves response")
    return int(value[:64], 16), int(value[64:128], 16)


def _out(amount_in: int, reserve_in: int, reserve_out: int) -> int:
    if min(amount_in, reserve_in, reserve_out) <= 0:
        return 0
    amount_with_fee = amount_in * 997
    return amount_with_fee * reserve_out // (reserve_in * 1000 + amount_with_fee)


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path); self.path.parent.mkdir(parents=True, exist_ok=True); self._init()

    def _connect(self):
        db = sqlite3.connect(self.path, timeout=.1); db.row_factory = sqlite3.Row; db.execute("PRAGMA busy_timeout=100")
        return db

    def _init(self):
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
            CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS observations(
              observation_id TEXT PRIMARY KEY, policy_version TEXT NOT NULL,envelope_id TEXT NOT NULL UNIQUE,
              pool_address TEXT NOT NULL,token_address TEXT NOT NULL,anchor_address TEXT NOT NULL,anchor_kind TEXT NOT NULL,
              anchor_in_raw TEXT NOT NULL,created_block INTEGER NOT NULL,created_log_index INTEGER NOT NULL,
              observed_head INTEGER NOT NULL,created_at REAL NOT NULL,previous_hash TEXT NOT NULL,record_hash TEXT NOT NULL UNIQUE);
            CREATE TABLE IF NOT EXISTS entry_selections(
              selection_id TEXT PRIMARY KEY,observation_id TEXT NOT NULL UNIQUE,status TEXT NOT NULL,entry_block INTEGER,
              sync_transaction_hash TEXT, sync_log_index INTEGER, quote_json TEXT, reason TEXT,observed_at REAL NOT NULL,
              previous_hash TEXT NOT NULL,record_hash TEXT NOT NULL UNIQUE, FOREIGN KEY(observation_id) REFERENCES observations(observation_id));
            CREATE TABLE IF NOT EXISTS schedules(
              observation_id TEXT NOT NULL,label TEXT NOT NULL,target_block INTEGER NOT NULL,target_at REAL NOT NULL,
              schedule_hash TEXT NOT NULL UNIQUE, PRIMARY KEY(observation_id,label));
            CREATE TABLE IF NOT EXISTS marks(
              sequence INTEGER PRIMARY KEY AUTOINCREMENT,observation_id TEXT NOT NULL,label TEXT NOT NULL,status TEXT NOT NULL,
              target_block INTEGER NOT NULL,observed_at REAL NOT NULL,quote_json TEXT,net_return REAL,exit_valid INTEGER NOT NULL,
              previous_hash TEXT NOT NULL,record_hash TEXT NOT NULL UNIQUE,UNIQUE(observation_id,label),
              FOREIGN KEY(observation_id) REFERENCES observations(observation_id));
            CREATE INDEX IF NOT EXISTS v2_shadow_v2_entries ON entry_selections(status,observation_id);
            CREATE INDEX IF NOT EXISTS v2_shadow_v2_due ON schedules(target_block,observation_id,label);
            CREATE TRIGGER IF NOT EXISTS v2_shadow_v2_observations_no_update BEFORE UPDATE ON observations BEGIN SELECT RAISE(ABORT,'append-only'); END;
            CREATE TRIGGER IF NOT EXISTS v2_shadow_v2_entries_no_update BEFORE UPDATE ON entry_selections BEGIN SELECT RAISE(ABORT,'append-only'); END;
            CREATE TRIGGER IF NOT EXISTS v2_shadow_v2_schedules_no_update BEFORE UPDATE ON schedules BEGIN SELECT RAISE(ABORT,'append-only'); END;
            CREATE TRIGGER IF NOT EXISTS v2_shadow_v2_marks_no_update BEFORE UPDATE ON marks BEGIN SELECT RAISE(ABORT,'append-only'); END;
            """)

    def arm(self, now: float) -> float:
        with self._connect() as db:
            row=db.execute("SELECT value FROM state WHERE key='armed_at'").fetchone()
            if row: return float(row[0])
            db.execute("INSERT INTO state VALUES('armed_at',?)",(str(now),))
        return now

    def state(self, key: str) -> str | None:
        with self._connect() as db:
            row = db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return str(row[0]) if row else None

    def set_state(self, key: str, value: object) -> None:
        with self._connect() as db:
            db.execute("INSERT INTO state(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, str(value)))

    def append_observation(self, envelope: dict) -> bool:
        if str(envelope.get("source_version")) != SOURCE_VERSION: return False
        c0,c1=(str(envelope.get(key) or "").lower() for key in ("currency0","currency1"))
        kinds={WETH_ADDRESS.lower():"wrapped_native",USDG_ADDRESS:"stable"}
        anchor=c0 if c0 in kinds else c1 if c1 in kinds else ""; token=c1 if anchor==c0 else c0
        if not anchor or token != str(envelope.get("token_address") or "").lower(): return False
        oid=digest({"policy":POLICY_VERSION,"envelope":envelope["envelope_id"]})
        with self._connect() as db:
            prior=db.execute("SELECT record_hash FROM observations ORDER BY rowid DESC LIMIT 1").fetchone(); prev=str(prior[0]) if prior else GENESIS_HASH
            data={"observation_id":oid,"policy_version":POLICY_VERSION,"envelope_id":envelope["envelope_id"],"pool_address":str(envelope["pool_address"]).lower(),"token_address":token,"anchor_address":anchor,"anchor_kind":kinds[anchor],"anchor_in_raw":str(ENTRY_ANCHOR_RAW[kinds[anchor]]),"created_block":int(envelope["block_number"]),"created_log_index":int(envelope["log_index"]),"observed_head":int(envelope["observed_head"]),"created_at":float(envelope["created_at"]),"previous_hash":prev}
            return bool(db.execute("INSERT OR IGNORE INTO observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",tuple(data.values())+(digest(data),)).rowcount)

    def pending_entries(self, head: int, limit: int) -> list[dict]:
        with self._connect() as db:
            return [dict(r) for r in db.execute("""SELECT o.* FROM observations o LEFT JOIN entry_selections e USING(observation_id)
                WHERE e.selection_id IS NULL ORDER BY o.created_block,o.observation_id LIMIT ?""",(max(0,int(limit)),))]

    def append_selection(self, observation: dict, *, status: str, now: float, entry_block: int|None=None, log: dict|None=None, quote: dict|None=None, reason: str|None=None) -> bool:
        sid=digest({"policy":POLICY_VERSION,"observation":observation["observation_id"]})
        with self._connect() as db:
            prior=db.execute("SELECT record_hash FROM entry_selections ORDER BY rowid DESC LIMIT 1").fetchone(); prev=str(prior[0]) if prior else GENESIS_HASH
            payload={"selection_id":sid,"observation_id":observation["observation_id"],"status":status,"entry_block":entry_block,"sync_transaction_hash":str((log or {}).get("transactionHash") or ""),"sync_log_index":_integer((log or {}).get("logIndex")) if log else None,"quote":quote,"reason":reason,"observed_at":now,"previous_hash":prev}
            inserted=db.execute("INSERT OR IGNORE INTO entry_selections VALUES(?,?,?,?,?,?,?,?,?,?,?)",(sid,observation["observation_id"],status,entry_block,payload["sync_transaction_hash"] or None,payload["sync_log_index"],canonical(quote) if quote else None,reason,now,prev,digest(payload))).rowcount
            if not inserted or status != "selected" or not quote: return bool(inserted)
            for label,seconds in SCHEDULE:
                block=int(entry_block)+seconds*BLOCKS_PER_SECOND
                sh=digest({"policy":POLICY_VERSION,"observation":observation["observation_id"],"label":label,"target_block":block})
                db.execute("INSERT INTO schedules VALUES(?,?,?,?,?)",(observation["observation_id"],label,block,now+seconds,sh))
            self._append_mark(db,observation["observation_id"],"entry",int(entry_block),now,quote)
            return True

    def _append_mark(self, db, oid: str, label: str, target_block: int, now: float, quote: dict):
        prior=db.execute("SELECT record_hash FROM marks ORDER BY sequence DESC LIMIT 1").fetchone(); prev=str(prior[0]) if prior else GENESIS_HASH
        status="observed" if quote.get("verified") else "unmarketable"; payload={"observation_id":oid,"label":label,"target_block":target_block,"status":status,"observed_at":now,"quote":quote,"net_return":quote.get("net_return"),"exit_valid":int(bool(quote.get("exitable"))),"previous_hash":prev}
        db.execute("INSERT INTO marks(observation_id,label,status,target_block,observed_at,quote_json,net_return,exit_valid,previous_hash,record_hash) VALUES(?,?,?,?,?,?,?,?,?,?)",(oid,label,status,target_block,now,canonical(quote),quote.get("net_return"),int(bool(quote.get("exitable"))),prev,digest(payload)))

    def due_outcomes(self, head: int, limit: int) -> list[dict]:
        with self._connect() as db:
            return [dict(r) for r in db.execute("""SELECT s.*,o.pool_address,o.token_address,o.anchor_address,o.anchor_in_raw,e.quote_json entry_quote_json
              FROM schedules s JOIN observations o USING(observation_id) JOIN entry_selections e USING(observation_id)
              JOIN marks m ON m.observation_id=s.observation_id AND m.label='entry' AND m.status='observed'
              LEFT JOIN marks done ON done.observation_id=s.observation_id AND done.label=s.label
              WHERE s.label!='entry' AND done.sequence IS NULL AND s.target_block<=? ORDER BY s.target_block LIMIT ?""",(int(head)-FINALITY_BLOCKS,max(0,int(limit))))]

    def append_outcome(self,due:dict,quote:dict,now:float):
        with self._connect() as db: self._append_mark(db,due["observation_id"],due["label"],int(due["target_block"]),now,quote)

    def snapshot(self) -> dict:
        with self._connect() as db:
            count=lambda sql:int(db.execute(sql).fetchone()[0])
            return {"policy_version":POLICY_VERSION,"observations":count("SELECT COUNT(*) FROM observations"),"selected_entries":count("SELECT COUNT(*) FROM entry_selections WHERE status='selected'"),"rejected_entries":count("SELECT COUNT(*) FROM entry_selections WHERE status='rejected'"),"expired_without_liquidity":count("SELECT COUNT(*) FROM entry_selections WHERE status='expired'"),"resolved_marks":count("SELECT COUNT(*) FROM marks"),"exitable_marks":count("SELECT COUNT(*) FROM marks WHERE status='observed' AND exit_valid=1"),"shadow_only":True,"paper_execution_enabled":False,"live_execution_enabled":False}


class V2ExecutableShadowV2:
    def __init__(self, root: str|Path, *, rpc:RobinhoodRPC|None=None): self.root=Path(root); self.store=Store(self.root/LEDGER_NAME); self.rpc=rpc or RobinhoodRPC()

    def _quote(self,row:dict,block:int,token_in:int|None=None)->dict:
        pool=str(row["pool_address"])
        try:
            t0=_address(self.rpc.call(pool,"0x"+TOKEN0_SELECTOR,block=block)); t1=_address(self.rpc.call(pool,"0x"+TOKEN1_SELECTOR,block=block)); r0,r1=_reserves(self.rpc.call(pool,"0x"+GET_RESERVES_SELECTOR,block=block)); anchor=str(row["anchor_address"]); token=str(row["token_address"])
            if {t0,t1}!={anchor,token}: return {"verified":False,"exitable":False,"quote_block":block,"reason":"pair_identity_mismatch"}
            ra,rt=(r0,r1) if t0==anchor else (r1,r0); amount=int(row["anchor_in_raw"])
            if token_in is None:
                token_out=_out(amount,ra,rt); anchor_out=_out(token_out,rt,ra); ratio=anchor_out/max(1,amount)
                return {"verified":token_out>0 and anchor_out>0,"exitable":ratio>=.90,"quote_block":block,"token_out_raw":str(token_out),"anchor_out_raw":str(anchor_out),"round_trip_ratio":ratio,"net_return":ratio*(1-FRICTION_BPS/10_000)-1}
            anchor_out=_out(token_in,rt,ra); ratio=anchor_out/max(1,amount)
            return {"verified":anchor_out>0,"exitable":anchor_out>0,"quote_block":block,"token_in_raw":str(token_in),"anchor_out_raw":str(anchor_out),"net_return":ratio*(1-FRICTION_BPS/10_000)-1}
        except Exception as exc: return {"verified":False,"exitable":False,"quote_block":block,"reason":"rpc_or_archive_failure","error":str(exc)[:240]}

    def _first_sync(self,row:dict,head:int)->dict|None:
        end=min(int(head),int(row["created_block"])+MAXIMUM_OBSERVATION_LAG_BLOCKS)
        logs=self.rpc.get_logs(int(row["created_block"]),end,address=row["pool_address"],topics=[SYNC_TOPIC])
        valid=[x for x in logs if (_integer(x.get("blockNumber"))>int(row["created_block"]) or _integer(x.get("logIndex"))>int(row["created_log_index"]))]
        return min(valid,key=lambda x:(_integer(x.get("blockNumber")),_integer(x.get("logIndex")))) if valid else None

    def _capture_new_launches(self, head: int, now: float) -> int:
        """Follow the V2 factory directly; never wait for the V4 radar.

        The first invocation arms at the present head. Later invocations read
        only a short head-adjacent range.  Any gap is intentionally discarded:
        this is prospective evidence, never historical recovery.
        """
        next_block = self.store.state("v2_factory_next_block")
        if next_block is None:
            self.store.set_state("v2_factory_next_block", head + 1)
            return 0
        start = max(int(next_block), int(head) - 30)
        if start > head:
            return 0
        logs = self.rpc.get_logs(start, int(head), address=UNISWAP_V2_FACTORY,
                                 topics=[PAIR_CREATED_TOPIC])
        self.store.set_state("v2_factory_next_block", head + 1)
        added = 0
        anchors = {WETH_ADDRESS.lower(), USDG_ADDRESS}
        for log in logs:
            topics = log.get("topics") or []
            if len(topics) < 3:
                continue
            token0, token1 = _topic_address(topics[1]), _topic_address(topics[2])
            if (token0 in anchors) == (token1 in anchors):
                continue
            token = token1 if token0 in anchors else token0
            pool = _data_address(log.get("data"))
            if not pool or pool == "0x" + "0" * 40:
                continue
            event = {
                "envelope_id": digest({"source": SOURCE_VERSION,
                    "transaction_hash": log.get("transactionHash"),
                    "log_index": _integer(log.get("logIndex"))}),
                "source_version": SOURCE_VERSION, "currency0": token0,
                "currency1": token1, "pool_address": pool,
                "token_address": token, "block_number": _integer(log.get("blockNumber")),
                "log_index": _integer(log.get("logIndex")),
                "observed_head": int(head), "created_at": now,
            }
            added += self.store.append_observation(event)
        return added

    def run_once(self,*,head_block:int|None=None,entry_limit:int=8,outcome_limit:int=8)->dict:
        now=time.time(); armed=self.store.arm(now)
        head=int(head_block if head_block is not None else self.rpc.get_block_number())
        enrolled=self._capture_new_launches(head,now); selected=rejected=expired=0
        for row in self.store.pending_entries(head,entry_limit):
            log=self._first_sync(row,head)
            if log:
                block=_integer(log["blockNumber"]); quote=self._quote(row,block)
                status="selected" if quote.get("verified") and quote.get("exitable") else "rejected"; self.store.append_selection(row,status=status,now=now,entry_block=block,log=log,quote=quote,reason=None if status=="selected" else str(quote.get("reason") or "entry_unmarketable")); selected+=status=="selected"; rejected+=status=="rejected"
            elif head>int(row["created_block"])+MAXIMUM_OBSERVATION_LAG_BLOCKS:
                self.store.append_selection(row,status="expired",now=now,reason="no_sync_within_freshness_window"); expired+=1
        outcomes=0
        for due in self.store.due_outcomes(head,outcome_limit):
            entry=json.loads(due["entry_quote_json"]); q=self._quote(due,int(due["target_block"]),int(entry.get("token_out_raw") or 0)); self.store.append_outcome(due,q,now); outcomes+=1
        status={**self.store.snapshot(),"armed_at":armed,"head_block":head,"enrolled":enrolled,"entries_selected":selected,"entries_rejected":rejected,"entries_expired":expired,"outcomes_resolved":outcomes}; atomic_json_write(self.root/STATUS_NAME,status); return status
