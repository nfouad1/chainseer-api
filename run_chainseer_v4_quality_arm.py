from __future__ import annotations
import argparse,time
from chainseer_v4_quality_arm import V4QualityArm
p=argparse.ArgumentParser();p.add_argument('--root',default='robinhood_learning');p.add_argument('--continuous',action='store_true');a=p.parse_args();w=V4QualityArm(a.root)
while True:
 try: print(w.run(),flush=True)
 except Exception as e: print({'status':'deferred','reason':type(e).__name__},flush=True);time.sleep(30);continue
 if not a.continuous:break
 time.sleep(15)
