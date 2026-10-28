"""Freeze the audited 26-name XNAS scope against a supplied session calendar.

Source gzip files remain on the mounted NFS. The inventory and calendar are
explicit inputs; missing dates after the security's audited start are errors.
"""
import argparse
import csv
import gzip
import hashlib
import json
from pathlib import Path

STARTS={name:'2021-01-01' for name in
        'AAPL AMD AMZN AVGO COST INTC LITE LRCX META MU NFLX NVDA PANW PLTR QCOM QQQ SMH SQQQ TER TSLA'.split()}
STARTS.update(COIN='2021-04-14',SOFI='2021-06-01',HOOD='2021-07-29',
              CRDO='2022-01-27',NBIS='2024-10-21',SNDK='2025-02-24')


def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--inventory',type=Path,required=True)
    p.add_argument('--calendar',type=Path,required=True)
    p.add_argument('--nfs-root',type=Path,required=True)
    p.add_argument('--end',default='2026-08-27')
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    with open(a.calendar) as f:sessions=list(csv.DictReader(f))
    dates=[s['session_date'] for s in sessions]
    if dates!=sorted(set(dates)):raise ValueError('calendar dates are not strictly ordered')
    files={}
    with gzip.open(a.inventory,'rt') as f:
        for r in csv.DictReader(f):
            if r['group']=='XNAS.ITCH' and r['symbol'] in STARTS:
                key=r['symbol'],r['day']
                if key in files:raise ValueError('duplicate inventory identity')
                files[key]=r
    jobs=[];census=[];total_bytes=0
    for symbol,start in sorted(STARTS.items()):
        days=[];source_bytes=0
        for s in sessions:
            day=s['session_date']
            if not start<=day<=a.end:continue
            r=files.get((symbol,day))
            if r is None:raise ValueError(f'missing {symbol} {day}')
            path=a.nfs_root/r['path'];st=path.stat()
            if st.st_size!=int(r['bytes']) or st.st_mtime_ns!=int(r['mtime_ns']):
                raise ValueError(f'inventory changed: {path}')
            source_symbol='FB' if symbol=='META' and day<'2022-06-09' else symbol
            days.append(dict(day=day,source=str(path),symbol=source_symbol,
                             open_ts=int(s['open_ts']),close_ts=int(s['close_ts'])))
            source_bytes+=st.st_size
        if not days:raise ValueError('empty scope')
        jobs.append(dict(ric=symbol+'.O',days=days));total_bytes+=source_bytes
        census.append(dict(ric=symbol+'.O',first=days[0]['day'],last=days[-1]['day'],
                           days=len(days),source_bytes=source_bytes,
                           rows=sum((d['close_ts']-d['open_ts'])//60 for d in days)))
    a.output.parent.mkdir(parents=True,exist_ok=True)
    a.output.write_text(json.dumps(jobs,indent=2)+'\n')
    manifest=dict(inventory=str(a.inventory),inventory_sha256=sha(a.inventory),
                  calendar=str(a.calendar),calendar_sha256=sha(a.calendar),
                  jobs_sha256=sha(a.output),source_bytes=total_bytes,
                  files=sum(c['days'] for c in census),rows=sum(c['rows'] for c in census),census=census)
    a.output.with_suffix('.manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    print(json.dumps(manifest,indent=2))


if __name__=='__main__':main()
