from __future__ import annotations
import argparse,time
from chainseer import ROBINHOOD_NETWORK,RobinhoodRPC
from chainseer_v2_quality_arm import QualityArm
p=argparse.ArgumentParser();p.add_argument('--root',default='robinhood_learning');p.add_argument('--continuous',action='store_true');a=p.parse_args(); w=QualityArm(a.root,RobinhoodRPC(ROBINHOOD_NETWORK.rpc_url,timeout=8))
while True:
 try:
  print(w.run(w.rpc.get_block_number()),flush=True)
 except Exception as error:
  # Provider throttling is operational, never a market rejection.  Keep the
  # process alive and yield capacity to the other evidence lanes.
  print({'status':'deferred','reason':type(error).__name__,'detail':str(error)[:180]},flush=True)
  time.sleep(30)
  continue
 if not a.continuous: break
 time.sleep(5)
