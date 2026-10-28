"""Real-source prefix, missing-session and minute-compatibility acceptance."""
import argparse
import csv
import json
import os
from pathlib import Path
import subprocess

import numpy as np
import pyarrow.parquet as pq

from usstock_aggregate_baseline import aggregate_table
from usstock_multifrequency_validate import eq, factor_job, load, sha


def compare(a,b):
    x,y=pq.read_table(a),pq.read_table(b)
    assert x.schema.equals(y.schema,check_metadata=False)
    for k in x.column_names:eq(x[k].to_numpy(),y[k].to_numpy(),(a,b,k))


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--run',type=Path,required=True);a=ap.parse_args()
    r=a.run;env={**os.environ,'POLARS_MAX_THREADS':'1'};receipts=[]
    calendar={row['session_date'].replace('-',''):i for i,row in enumerate(csv.DictReader((r/'calendar.csv').open()))}
    def run(cmd):
        p=subprocess.run(list(map(str,cmd)),env=env,capture_output=True,text=True)
        assert p.returncode==0,(cmd,p.stderr)
        receipts.append(dict(command=list(map(str,cmd)),stdout=p.stdout,stderr=p.stderr))
    for freq in ['1h','1day']:
        src=r/f'factor_pilot/baseline_data_{freq}_hfq_raw';days=sorted(p.stem for p in (src/'ARCA/SPY.P').glob('*.parquet'))
        for case,selected in [('prefix',days[:60]),('missing_session',days[:50]+days[51:])]:
            root=r/'acceptance'/case;inp=root/src.name;out=root/f'baseline_factor_{freq}_hfq_raw'
            for day in selected:
                for rel in [Path(f'ARCA/SPY.P/{day}.parquet'),Path(f'_bars/ARCA/SPY.P/{day}.json.zst')]:
                    dst=inp/rel;dst.parent.mkdir(parents=True,exist_ok=True);dst.symlink_to(src/rel)
            run([r/'bin/usstock_features_1min_hfq','--input-root',inp,'--output-root',out,'--raw','--bar-frequency',freq,
                 '--bar-metadata-root',inp/'_bars','--calendar',r/'calendar.csv','--rics','SPY.P','--start','2023-11-01','--end','2024-06-30','--workers','1'])
            result=factor_job((root,freq,'ARCA','SPY.P',selected,calendar))
            if case=='prefix':
                for day in selected:
                    p=out/f'ARCA/SPY.P/{day}.parquet';full=r/f'factor_pilot_new/baseline_factor_{freq}_hfq_raw/ARCA/SPY.P/{day}.parquet'
                    compare(p,full);assert sha(p)==sha(full)
            else:
                first=pq.read_table(out/f'ARCA/SPY.P/{days[51]}.parquet')
                assert all(np.isnan(first[k][0].as_py()) for k in first.column_names[4:])
            receipts.append(dict(case=case,frequency=freq,files=len(selected),rows=sum(x['rows'] for x in result['records']),status='passed'))
    # Freeze a partial real minute day. Only bars already closed in both inputs compare.
    job=next(j for j in json.loads((r/'factor_pilot_jobs.json').read_text()) if j['day']=='20231101')
    table=pq.read_table(job['input']);partial=table.slice(0,337);base=r/'acceptance/minute_prefix';base.mkdir()
    path=base/'20231101.parquet';pq.write_table(partial,path,compression='zstd');st=path.stat()
    j={**job,'input':str(path),'source_bytes':st.st_size,'source_mtime_ns':st.st_mtime_ns}
    manifest=base/'jobs.json';manifest.write_text(json.dumps([j]));out=base/'out'
    run([r/'bin/usstock_aggregate_baseline','--jobs',manifest,'--out-root',out,'--workers','1','--execute'])
    full,fullmeta=aggregate_table(table,'1h','20231101');prefix,pm=aggregate_table(partial,'1h','20231101')
    sealed=[i for i,b in enumerate(pm['bars']) if b['end_ts']==fullmeta['bars'][i]['end_ts']]
    actual=pq.read_table(out/'baseline_data_1h_hfq_raw/ARCA/SPY.P/20231101.parquet')
    for k in table.column_names:
        eq(prefix[k].to_numpy(),actual[k].to_numpy(),('partial-day Rust/Python',k))
        eq(full[k].to_numpy()[sealed],actual[k].to_numpy()[sealed],('sealed bar prefix',k))
    receipts.append(dict(case='partial_minute_day',source_minutes=len(partial),sealed_bars=len(sealed),status='passed'))
    # The default minute invocation retains the previously published behavior.
    data=Path('/mnt/hdd-raid5-72t/liang_torch/usstock_data');out=r/'acceptance/minute_regression/baseline_factor_1min_hfq_raw'
    run([r/'bin/usstock_features_1min_hfq','--input-root',data/'baseline_data_1min_hfq_raw','--output-root',out,
         '--raw','--rics','SPY.P','--start','2021-07-01','--end','2021-07-06','--workers','1'])
    files=list(out.glob('*/*/*.parquet'))
    for p in files:compare(p,data/'baseline_factor_1min_hfq_raw'/p.relative_to(out))
    receipts.append(dict(case='existing_minute_regression',files=len(files),status='passed'))
    (r/'acceptance.json').write_text(json.dumps(dict(status='passed',checks=receipts,script_sha256=sha(Path(__file__))),indent=2))
    print(json.dumps(dict(status='passed',checks=len(receipts))))


if __name__=='__main__':main()
