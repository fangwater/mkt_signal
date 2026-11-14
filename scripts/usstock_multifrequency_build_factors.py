"""Build each frozen security only after all of its aggregated inputs are ready."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import time


def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('--run',type=Path,required=True)
    ap.add_argument('--root',type=Path,required=True);ap.add_argument('--workers',type=int,default=8)
    ap.add_argument('--execute',action='store_true');a=ap.parse_args()
    byric={}
    for j in json.loads((a.run/'jobs.json').read_text()):
        if j['hfq']:byric.setdefault((j['venue'],j['ric']),[]).append(j['day'])
    pending=[(freq,venue,ric,sorted(days)) for (venue,ric),days in sorted(byric.items()) for freq in ['1h','1day']]
    if not a.execute:print(json.dumps(dict(tasks=len(pending),root=str(a.root))));return
    logs=a.run/'factor_logs';logs.mkdir(exist_ok=True);active={};receipts=[];deadline=time.monotonic()+14400
    def command(task):
        freq,venue,ric,days=task;inp=a.root/f'baseline_data_{freq}_hfq_raw';out=a.root/f'baseline_factor_{freq}_hfq_raw'
        return [str(a.run/'bin/usstock_features_1min_hfq'),'--input-root',str(inp),'--output-root',str(out),
                '--raw','--bar-frequency',freq,'--bar-metadata-root',str(inp/'_bars'),'--calendar',str(a.run/'calendar.csv'),
                '--rics',ric,'--start','2021-07-01','--end','2025-06-30','--workers','1']
    def ready(task):
        freq,venue,ric,days=task;inp=a.root/f'baseline_data_{freq}_hfq_raw'
        if not (inp/f'_bars/{venue}/{ric}/{days[-1]}.json.zst').exists():return False
        return all((inp/f'{venue}/{ric}/{d}.parquet').exists() and (inp/f'_bars/{venue}/{ric}/{d}.json.zst').exists() for d in days)
    def run(task):
        cmd=command(task);began=time.monotonic();log=logs/f'{task[0]}_{task[2]}.log'
        with log.open('w') as f:p=subprocess.run(cmd,env={**os.environ,'POLARS_MAX_THREADS':'1'},stdout=f,stderr=subprocess.STDOUT)
        assert p.returncode==0,(task,log.read_text())
        return dict(frequency=task[0],venue=task[1],ric=task[2],files=len(task[3]),command=cmd,log=str(log),seconds=time.monotonic()-began)
    with ThreadPoolExecutor(a.workers) as pool:
        while pending or active:
            assert time.monotonic()<deadline,'factor producer timeout'
            for f in list(active):
                if f.done():
                    receipts.append(f.result());del active[f]
                    print(json.dumps(dict(completed=len(receipts),total=154,ric=receipts[-1]['ric'],frequency=receipts[-1]['frequency'])),flush=True)
                    (a.run/'factor_build_progress.json').write_text(json.dumps(receipts,indent=2))
            for task in list(pending):
                if len(active)>=a.workers:break
                if ready(task):active[pool.submit(run,task)]=task;pending.remove(task)
            if pending or active:time.sleep(2)
    (a.run/'factor_build.json').write_text(json.dumps(dict(status='passed',tasks=receipts),indent=2))
    print('factor builds complete',flush=True)


if __name__=='__main__':main()
