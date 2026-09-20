"""Forward-only V2 quality-entry shadow arm; never executes trades."""
from __future__ import annotations
import json, sqlite3, time
from pathlib import Path
from chainseer_executable_shadow_v2 import V2ExecutableShadowV2, digest, canonical, TOKEN0_SELECTOR, _address
from chainseer_core import atomic_json_write

POLICY="v2-quality-arm-v1"; LEDGER="v2_quality_arm.sqlite3"; STATUS="v2_quality_arm_status.json"
SWAP="0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
MIN_RATIO=.98; MIN_SWAPS=3; MIN_TX=2; WINDOW=120; BPS=10; HORIZONS=(("15m",900),("1h",3600),("6h",21600),("24h",86400))

def _i(v): return int(str(v or "0x0"),16)
def _words(data):
    s=str(data or "").removeprefix("0x"); return [int(s[i:i+64],16) for i in range(0,min(len(s),256),64)]

class QualityArm:
 def __init__(self,root,rpc):
  self.root=Path(root); self.rpc=rpc; self.base=self.root/"v2_executable_shadow_v2.sqlite3"; self.db=self.root/LEDGER; self._init(); self.quoter=V2ExecutableShadowV2(self.root,rpc=rpc)
 def _c(self):
  c=sqlite3.connect(self.db,timeout=.1); c.row_factory=sqlite3.Row; return c
 def _init(self):
  with self._c() as c:c.executescript("""CREATE TABLE IF NOT EXISTS decisions(id TEXT PRIMARY KEY,observation_id TEXT UNIQUE,status TEXT NOT NULL,decision_block INTEGER,quote_json TEXT,flow_json TEXT,created_at REAL NOT NULL);CREATE TABLE IF NOT EXISTS marks(observation_id TEXT,label TEXT,target_block INTEGER,status TEXT,quote_json TEXT,net_return REAL,PRIMARY KEY(observation_id,label));""")
 def _candidates(self):
  if not self.base.exists(): return []
  with sqlite3.connect(self.base) as c:
   c.row_factory=sqlite3.Row
   return [dict(r) for r in c.execute("SELECT o.*,e.entry_block,e.quote_json FROM observations o JOIN entry_selections e USING(observation_id) WHERE e.status='selected'").fetchall()]
 def run(self,head):
  now=time.time(); decided=outcomes=0
  with self._c() as q:
   known={r[0] for r in q.execute("SELECT observation_id FROM decisions")}
  for row in self._candidates():
   if row['observation_id'] in known: continue
   entry=json.loads(row['quote_json']); start=int(row['entry_block']); end=min(head,start+WINDOW)
   logs=self.rpc.get_logs(start,end,address=row['pool_address'],topics=[SWAP])
   logs=[x for x in logs if _i(x.get('blockNumber'))>start or _i(x.get('logIndex'))>0]
   if len(logs)<MIN_SWAPS and head<=start+WINDOW: continue
   if len(logs)<MIN_SWAPS:
    self._decide(row,'rejected',None,None,{'reason':'insufficient_swaps'}); decided+=1; continue
   logs=sorted(logs,key=lambda x:(_i(x.get('blockNumber')),_i(x.get('logIndex')))); tx={x.get('transactionHash') for x in logs}
   try: anchor0=_address(self.rpc.call(row['pool_address'],'0x'+TOKEN0_SELECTOR,block=start))==str(row['anchor_address']).lower()
   except Exception: continue
   flow=0
   for log in logs:
    w=_words(log.get('data')); flow+=(w[0]-w[2]) if anchor0 else (w[1]-w[3])
   block=_i(logs[-1]['blockNumber']); quote=self.quoter._quote(row,block)
   ok=len(tx)>=MIN_TX and flow>0 and quote.get('round_trip_ratio',0)>=MIN_RATIO and quote.get('verified')
   self._decide(row,'selected' if ok else 'rejected',block,quote,{'swaps':len(logs),'transactions':len(tx),'net_anchor_flow':str(flow)}); decided+=1
  status={'policy_version':POLICY,'head_block':head,'decisions_added':decided,'outcomes_resolved':outcomes,'shadow_only':True,'paper_execution_enabled':False,'live_execution_enabled':False}
  atomic_json_write(self.root/STATUS,status); return status
 def _decide(self,row,status,block,quote,flow):
  with self._c() as c:c.execute("INSERT OR IGNORE INTO decisions VALUES(?,?,?,?,?,?,?)",(digest({'p':POLICY,'o':row['observation_id']}),row['observation_id'],status,block,canonical(quote) if quote else None,canonical(flow),time.time()))
