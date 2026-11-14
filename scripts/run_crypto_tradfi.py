#!/usr/bin/env python3
"""Run, independently verify, and publish the requested TradFi parquet datasets."""
import argparse
import concurrent.futures
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp=path.with_suffix('.json.tmp');tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');tmp.replace(path)


def independent_four_formulas(combined):
    """Previous completed bars, with explicit continuity and full-book masks."""
    length=len(combined)
    expected={name:np.full(length,np.nan) for name in ['factor_005','factor_007','factor_013','factor_081']}
    connected=combined[:-1,1]==combined[1:,0]
    complete=combined[:,2]==20
    if length>=2:
        ready=connected & complete[:-1]
        expected['factor_007'][1:]=np.where(ready,combined[:-1,4],np.nan)
        expected['factor_013'][1:]=np.where(ready,combined[:-1,5],np.nan)
    if length>=3:
        ready=connected[:-1] & connected[1:] & complete[:-2] & complete[1:-1]
        expected['factor_005'][2:]=np.where(ready,combined[1:-1,3]-combined[:-2,3],np.nan)
    if length>=4:
        ready=(connected[:-2] & connected[1:-1] & connected[2:]
               & complete[:-3] & complete[1:-2] & complete[2:-1])
        mean=(combined[:-3,6]+combined[1:-2,6]+combined[2:-1,6])/3
        expected['factor_081'][3:]=np.where(ready,mean,np.nan)
    return expected


def single_thread_arrow():
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)


def verify_symbol(job):
    stage,sym=job;stats={};valid_counts={}
    for n in [1,5,15]:
        source=stage/f'baseline_data_{n}min/binance-futures/{sym}'
        dest=stage/f'baseline_factor_{n}min/binance-futures/{sym}'
        baseline_files=sorted(source.glob('*.parquet'));factor_files=sorted(dest.glob('*.parquet'))
        if [p.name for p in baseline_files]!=[p.name for p in factor_files]:raise ValueError(f'{sym} {n}min file keys mismatch')
        rows=0;history=np.empty((0,7));finite=None;names=None
        for bp,fp in zip(baseline_files,factor_files):
            b=pq.read_table(bp);f=pq.read_table(fp)
            if b.num_columns!=121 or f.num_columns!=634:raise ValueError(f'schema width {bp} {b.num_columns}/{f.num_columns}')
            if b.num_rows!=f.num_rows or not b['ts'].equals(f['ts']) or not b['symbol'].equals(f['symbol']):raise ValueError(f'primary key mismatch {fp}')
            if set(b['symbol'].to_pylist())!={sym}:raise ValueError(f'wrong symbol {bp}')
            names=f.column_names[2:]
            matrix=np.column_stack([f[c].to_numpy(zero_copy_only=False) for c in names])
            if np.isinf(matrix).any():raise ValueError(f'infinite factor {fp}')
            counts=np.isfinite(matrix).sum(axis=0);finite=counts if finite is None else finite+counts
            a=lambda name:b[name].to_numpy(zero_copy_only=False)
            bid20=sum(a(f'bid{i}v') for i in range(20));ask20=sum(a(f'ask{i}v') for i in range(20))
            bid5=sum(a(f'bid{i}v') for i in range(5))
            current=np.column_stack([a('ts'),a('end_ts'),a('book_depth'),a('mid_price'),bid20-ask20,bid5/bid20,a('bid9v')+a('ask9v')])
            combined=np.concatenate([history,current]);offset=len(history)
            expected=independent_four_formulas(combined)
            for name,wanted in expected.items():
                actual=f[name].to_numpy(zero_copy_only=False)
                if not np.allclose(actual,wanted[offset:],rtol=1e-9,atol=1e-8,equal_nan=True):
                    mismatch=~np.isclose(actual,wanted[offset:],rtol=1e-9,atol=1e-8,equal_nan=True)
                    i=int(np.flatnonzero(mismatch)[0]);raise ValueError(f'{fp} {name} row {i}: Rust={actual[i]}, Python={wanted[offset+i]}')
            history=combined[-3:];rows+=b.num_rows
        stats[str(n)]={'files':len(baseline_files),'rows':rows,'factor_columns':len(names or []),'all_null_columns':[] if finite is None else [name for name,c in zip(names,finite) if c==0]}
        valid_counts[str(n)]={} if finite is None else dict(zip(names,map(int,finite)))
    # Recheck every source identity/stat and every original output hash from the writer receipt.
    inputs=0;outputs=0
    for ap in sorted((stage/f'_audit/days/{sym}').glob('*.json')):
        audit=json.loads(ap.read_text())
        for name in ['trade_source','book_source']:
            source=audit[name];stat=Path(source['path']).stat()
            if stat.st_size!=source['bytes'] or str(stat.st_mtime_ns)!=source['mtime_ns']:raise ValueError(f'input changed {source["path"]}')
            inputs+=1
        for rec in audit['outputs']:
            p=Path(rec['path'])
            if hashlib.sha256(p.read_bytes()).hexdigest()!=rec['sha256']:raise ValueError(f'baseline changed {p}')
            outputs+=1
    marker=json.loads((stage/f'_audit/factors_{sym}.json').read_text())
    for freq in marker['frequencies']:
        if freq['finite_counts']!=valid_counts[str(freq['bar_minutes'])]:raise ValueError(f'factor count mismatch {sym}')
        for rec in freq['files']:
            if hashlib.sha256(Path(rec['path']).read_bytes()).hexdigest()!=rec['sha256']:raise ValueError(f'factor hash mismatch {rec["path"]}')
            outputs+=1
    result={'symbol':sym,'frequencies':stats,'source_stat_checks':inputs,'output_hash_checks':outputs,'finite_counts':valid_counts}
    atomic_json(stage/f'_audit/verified_{sym}.json',result)
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root',type=Path,required=True);p.add_argument('--manifest',type=Path,required=True)
    p.add_argument('--binary',type=Path,required=True);p.add_argument('--workers',type=int,default=24)
    p.add_argument('--factor-processes',type=int,default=None,
                   help='Isolate each symbol in a single-worker factor process; limit concurrent processes.')
    p.add_argument('--factors-only',action='store_true',help='Reuse existing baseline data; compute factors, verify and publish.')
    p.add_argument('--wait-for-baseline-seconds',type=int,default=0,
                   help='Allow a concurrent baseline producer to finish each symbol before factors start.')
    p.add_argument('--execute',action='store_true');p.add_argument('--verify-only',action='store_true')
    a=p.parse_args();request=json.loads(a.manifest.read_text());symbols=request['symbols']
    stage=a.root/'_build_20261026';receipt={'request':request,'stage_root':str(stage),'status':'planned'}
    print(json.dumps({'symbols':len(symbols),'end_date':request['end_date'],'stage':str(stage),'execute':a.execute}),flush=True)
    if not a.execute:return
    stage.mkdir(exist_ok=True);logs=stage/'_logs';logs.mkdir(exist_ok=True)
    env=dict(os.environ,POLARS_MAX_THREADS='1',OMP_NUM_THREADS='1',OPENBLAS_NUM_THREADS='1')
    receipt['binary_sha256']=hashlib.sha256(a.binary.read_bytes()).hexdigest();receipt['started_utc']=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime())
    for phase in ([] if a.verify_only else ['factors'] if a.factors_only else ['baseline','factors']):
        receipt['status']=phase;atomic_json(a.root/'_provenance/run_20261026.json',receipt)
        started=time.monotonic()
        with (logs/f'{phase}.log').open('a') as log:
            print('START',phase,'log',str(logs/f'{phase}.log'),flush=True)
            cmd=[str(a.binary),'--manifest',str(a.manifest),'--out-root',str(stage),'--phase',phase]
            if phase=='factors' and a.factor_processes:
                def run_factor_symbol(sym):
                    symbol_log=logs/'factors_by_symbol'/f'{sym}.log'
                    symbol_log.parent.mkdir(exist_ok=True)
                    began=time.monotonic()
                    deadline=began+a.wait_for_baseline_seconds
                    baseline_marker=stage/'_audit'/f'baseline_{sym}.json'
                    while not baseline_marker.exists():
                        if time.monotonic()>=deadline:
                            raise TimeoutError(f'baseline symbol not complete: {sym}')
                        time.sleep(2)
                    with symbol_log.open('a') as out:
                        subprocess.run(cmd+['--symbols',sym,'--workers','1','--execute'],
                                       stdout=out,stderr=subprocess.STDOUT,env=env,check=True)
                    return sym,time.monotonic()-began
                with concurrent.futures.ThreadPoolExecutor(max_workers=a.factor_processes) as pool:
                    build_symbols=sorted(symbols,key=lambda sym:not (stage/'_audit'/f'baseline_{sym}.json').exists())
                    futures=[pool.submit(run_factor_symbol,sym) for sym in build_symbols]
                    for completed,future in enumerate(concurrent.futures.as_completed(futures),1):
                        sym,elapsed=future.result()
                        line=f'FACTORS_DONE {sym} symbols={completed}/{len(symbols)} elapsed={elapsed:.1f}'
                        print(line,file=log,flush=True);print(line,flush=True)
            else:
                subprocess.run(cmd+['--workers',str(a.workers),'--execute'],stdout=log,stderr=subprocess.STDOUT,env=env,check=True)
        receipt[phase+'_seconds']=time.monotonic()-started;print('DONE',phase,receipt[phase+'_seconds'],flush=True)
    receipt['status']='verification';atomic_json(a.root/'_provenance/run_20261026.json',receipt)
    pa.set_cpu_count(1);pa.set_io_thread_count(1);results=[];started=time.monotonic()
    with concurrent.futures.ProcessPoolExecutor(max_workers=min(a.workers,12),initializer=single_thread_arrow) as pool:
        for result in pool.map(verify_symbol,[(stage,sym) for sym in symbols]):
            results.append(result);print('VERIFIED',result['symbol'],len(results),'/',len(symbols),flush=True)
    receipt['verification_seconds']=time.monotonic()-started
    totals={}
    for n in [1,5,15]:
        all_counts={}
        for r in results:
            for name,c in r['finite_counts'][str(n)].items():all_counts[name]=all_counts.get(name,0)+c
        totals[str(n)]={'symbols':len(results),'files_per_dataset':sum(r['frequencies'][str(n)]['files'] for r in results),'rows_per_dataset':sum(r['frequencies'][str(n)]['rows'] for r in results),'finite_counts':all_counts,'all_null_columns':[name for name,c in all_counts.items() if c==0]}
    receipt['totals']=totals
    receipt['source_stat_checks']=sum(r['source_stat_checks'] for r in results)
    receipt['output_hash_checks']=sum(r['output_hash_checks'] for r in results)
    # Every target is checked before the first rename; the build root is retained as provenance.
    targets=[f'{kind}_{n}min' for n in [1,5,15] for kind in ['baseline_data','baseline_factor']]
    for name in targets:
        if (a.root/name).exists():raise FileExistsError(a.root/name)
    for name in targets:(stage/name).rename(a.root/name)
    # Read first/last published primary keys after the atomic directory renames.
    samples=0
    for sym in symbols:
        for n in [1,5,15]:
            for kind in ['baseline_data','baseline_factor']:
                files=sorted((a.root/f'{kind}_{n}min/binance-futures/{sym}').glob('*.parquet'))
                for f in {files[0],files[-1]}:
                    pq.read_table(f,columns=['symbol','ts']);samples+=1
    receipt.update(status='published',published_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),published_key_readback_samples=samples,published_datasets=targets)
    atomic_json(a.root/'_provenance/delivery_20261026.json',receipt)
    atomic_json(a.root/'_provenance/run_20261026.json',receipt)
    print('PUBLISHED',json.dumps({n:{k:v for k,v in values.items() if k!='finite_counts'} for n,values in totals.items()},ensure_ascii=False),flush=True)


if __name__=='__main__':main()
