"""Append-only, shadow-only quality gate for existing fresh V4 pullback candidates."""
from __future__ import annotations
import json,sqlite3,time
from pathlib import Path
from chainseer_core import atomic_json_write

POLICY='v4-quality-arm-v1'; LEDGER='v4_quality_arm.sqlite3'; STATUS='v4_quality_arm_status.json'
MIN_TX=2; HORIZONS=(('15m',900),('1h',3600),('6h',21600),('24h',86400))

class V4QualityArm:
 def __init__(self,root): self.root=Path(root);self.db=self.root/LEDGER;self._init()
 def _c(self):
  c=sqlite3.connect(self.db,timeout=.1);c.row_factory=sqlite3.Row;return c
 def _init(self):
  with self._c() as c:c.executescript("""CREATE TABLE IF NOT EXISTS state(key TEXT PRIMARY KEY,value TEXT NOT NULL);CREATE TABLE IF NOT EXISTS decisions(candidate_id TEXT PRIMARY KEY,policy_version TEXT NOT NULL,status TEXT NOT NULL,decision_block INTEGER NOT NULL,reason TEXT NOT NULL,evidence_json TEXT NOT NULL,created_at REAL NOT NULL);CREATE TABLE IF NOT EXISTS schedules(candidate_id TEXT,label TEXT,target_at REAL NOT NULL,status TEXT NOT NULL DEFAULT 'pending',PRIMARY KEY(candidate_id,label));""")
 def arm(self,now):
  with self._c() as c:
   r=c.execute("SELECT value FROM state WHERE key='armed_at'").fetchone()
   if r:return float(r[0])
   c.execute("INSERT INTO state VALUES('armed_at',?)",(str(now),));return now
 def run(self):
  now=time.time(); armed=self.arm(now); tape=self.root/'pullback_tape_v4.sqlite3'; learn=self.root/'learning.sqlite3'; added=0
  if tape.exists() and learn.exists():
   with sqlite3.connect(tape) as t,sqlite3.connect(learn) as l:
    t.row_factory=l.row_factory=sqlite3.Row
    rows=t.execute("""SELECT c.*,s.pool_id,s.token_address,s.currency0,s.currency1 FROM candidates c JOIN sessions s USING(session_id) WHERE c.observed_at>=?""",(armed,)).fetchall()
    with self._c() as q:
     known={x[0] for x in q.execute('SELECT candidate_id FROM decisions')}
     for r in rows:
      if r['candidate_id'] in known:continue
      quote=json.loads(r['quote_json'] or '{}'); pool=l.execute('SELECT hooks_address FROM v4_pools WHERE pool_id=?',(r['pool_id'],)).fetchone(); custody=l.execute('SELECT custody_verdict FROM v4_custody_snapshots WHERE pool_id=? ORDER BY observed_block DESC LIMIT 1',(r['pool_id'],)).fetchone()
      hooks=str(pool['hooks_address'] if pool else '').lower(); cv=str(custody['custody_verdict'] if custody else 'unverified')
      ok=bool(quote.get('verified') and quote.get('exitable')) and hooks in ('','0x0000000000000000000000000000000000000000') and cv not in ('unverified','unsafe','')
      reason='accepted_shadow' if ok else 'quote_or_custody_gate'; evidence={'quote':quote,'hooks_address':hooks,'custody_verdict':cv,'sample_count':r['sample_count']}
      q.execute('INSERT INTO decisions VALUES(?,?,?,?,?,?,?)',(r['candidate_id'],POLICY,'selected' if ok else 'rejected',r['observed_block'],reason,json.dumps(evidence,sort_keys=True),now));
      if ok:
       for label,seconds in HORIZONS:q.execute('INSERT INTO schedules VALUES(?,?,?,?)',(r['candidate_id'],label,now+seconds,'pending'))
      added+=1
  with self._c() as c:
   summary={x[0]:x[1] for x in c.execute('SELECT status,count(*) FROM decisions GROUP BY status')}; pending=c.execute("SELECT count(*) FROM schedules WHERE status='pending'").fetchone()[0]
  out={'policy_version':POLICY,'armed_at':armed,'decisions_added':added,'decisions':summary,'pending_outcomes':pending,'shadow_only':True,'paper_execution_enabled':False,'live_execution_enabled':False};atomic_json_write(self.root/STATUS,out);return out
