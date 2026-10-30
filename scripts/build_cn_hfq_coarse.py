#!/usr/bin/env python3
"""Build 1h/1day CN HFQ baselines only; continuous factors use the Rust driver."""
from __future__ import annotations

import argparse
from concurrent.futures import ProcessPoolExecutor
import hashlib
import json
from pathlib import Path
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from build_cn_hfq_5min import write_verified
from cn_hfq_coarse_reference import aggregate_coarse
from rebuild_cn_hfq_ylabels import atomic_json, signature


CONTRACT = dict(
    frequencies=['1h','1day'], source='existing 1min additive HFQ',
    hour_alignment='first minute of each contiguous source segment; retain short tail',
    daily_alignment='entire source official TradDay; include all observed source sessions',
    timestamp='first contributing minute, UTC Unix seconds; usable after end_ts',
    factor_state='continuous observed bars within product; do not reset on sessions, dates or absent days',
    factor_windows='unchanged original bar counts; original missing-field semantics; shift(1)',
    labels='not generated', book='all five levels and mid_price from same last source minute',
)


def build_product(job):
    pa.set_cpu_count(1);pa.set_io_thread_count(1)
    start=time.perf_counter();root=Path(job['stage_root'])
    records=[];total=0;previous_end=None
    for name in job['days']:
        path=Path(job['in_root'])/job['exchange']/job['product']/(name+'.parquet')
        before=signature(path);payload=path.read_bytes()
        table=pq.read_table(pa.BufferReader(payload));frame=table.to_pandas()
        if not (frame.contract_id==job['product']).all():
            raise ValueError(f'wrong product in {path}')
        if previous_end is not None and previous_end>int(frame.ts.iloc[0]):
            raise ValueError(f'overlapping input trading days: {path}')
        previous_end=int(frame.ts.iloc[-1])+60
        record=dict(day=name,source_rows=len(frame),source_sha256=hashlib.sha256(payload).hexdigest(),
                    source_signature=before,outputs={})
        for frequency,minutes in [('1h',60),('1day',1440)]:
            bars,meta=aggregate_coarse(frame,minutes,int(name))
            bid=bars.bid0p.to_numpy();ask=bars.ask0p.to_numpy()
            if not (np.isfinite(bid)&np.isfinite(ask)&(ask>=bid)).all():
                raise ValueError(f'invalid top of book in {frequency}: {path}')
            target=root/f'baseline_data_{frequency}_hfq'/job['exchange']/job['product']/path.name
            schema=table.schema.with_metadata({b'cn_hfq_coarse_contract':json.dumps(CONTRACT,sort_keys=True).encode()})
            out=pa.Table.from_pandas(bars,schema=schema,preserve_index=False)
            result=write_verified(out,target)
            metadata_path=root/f'baseline_data_{frequency}_hfq'/'_segments'/job['exchange']/job['product']/(name+'.json')
            atomic_json(metadata_path,meta)
            result.update(path=str(target.relative_to(root)),metadata_sha256=hashlib.sha256(metadata_path.read_bytes()).hexdigest(),
                          short_tails=sum(c<minutes for c in meta['source_minutes']) if minutes==60 else 0)
            record['outputs'][frequency]=result
        if signature(path)!=before:raise ValueError(f'source changed while reading: {path}')
        records.append(record);total+=len(frame)
    report=dict(status='passed',exchange=job['exchange'],product=job['product'],files=len(records),
                source_rows=total,records=records,seconds=time.perf_counter()-start)
    atomic_json(root/'_audit'/f"{job['exchange']}_{job['product']}.json",report)
    return {k:v for k,v in report.items() if k!='records'}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--universe-json',type=Path,required=True)
    parser.add_argument('--in-root',type=Path,required=True)
    parser.add_argument('--stage-root',type=Path,required=True)
    parser.add_argument('--start',required=True);parser.add_argument('--end',required=True)
    parser.add_argument('--workers',type=int,default=16)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args()
    if args.workers<1 or args.start>args.end:parser.error('invalid workers or date range')
    u=json.loads(args.universe_json.read_text());jobs=[]
    for exchange,products in u['included_products_by_exchange'].items():
        for product in products:
            days=sorted(p.stem for p in (args.in_root/exchange/product).glob('*.parquet')
                        if args.start.replace('-','')<=p.stem<=args.end.replace('-',''))
            if not days:raise ValueError(f'no inputs: {exchange}/{product}')
            jobs.append(dict(exchange=exchange,product=product,days=days,
                             in_root=str(args.in_root),stage_root=str(args.stage_root)))
    report=dict(status='planned',contract=CONTRACT,jobs=jobs,products=len(jobs),
                files_per_frequency=sum(len(j['days']) for j in jobs),start=args.start,end=args.end)
    print(json.dumps({k:v for k,v in report.items() if k!='jobs'},ensure_ascii=False),flush=True)
    if not args.execute:return
    if args.stage_root.exists() and any(args.stage_root.iterdir()):
        raise FileExistsError('use a new empty staging directory')
    args.stage_root.mkdir(parents=True,exist_ok=True)
    atomic_json(args.stage_root/'_audit/plan.json',report)
    start=time.perf_counter();results=[]
    with ProcessPoolExecutor(args.workers) as pool:
        for result in pool.map(build_product,jobs):
            results.append(result);print(json.dumps(result),flush=True)
    report.update(status='passed',results=results,seconds=time.perf_counter()-start)
    atomic_json(args.stage_root/'_audit/build.json',report)
    print(json.dumps(dict(status=report['status'],files_per_frequency=report['files_per_frequency'],seconds=report['seconds'])),flush=True)


if __name__=='__main__':main()
