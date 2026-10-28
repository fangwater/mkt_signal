"""Independent complete readback of US RTH coarse bars and shifted factors."""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
import csv
import hashlib
import json
import multiprocessing as mp
from pathlib import Path
import time

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd


def sha(p):
    h = hashlib.sha256()
    with Path(p).open('rb') as f:
        for b in iter(lambda: f.read(1 << 20), b''):
            h.update(b)
    return h.hexdigest()


def load(p):
    with Path(p).open('rb') as f, zstd.ZstdDecompressor().stream_reader(f) as d:
        return json.load(d)


def dump(p, obj):
    Path(p).write_bytes(zstd.ZstdCompressor(level=3).compress(json.dumps(obj).encode()))


def eq(x, y, label):
    x, y = np.asarray(x), np.asarray(y)
    if x.dtype.kind == 'f':
        assert np.array_equal(np.isfinite(x), np.isfinite(y)), (label, 'missing mask')
        np.testing.assert_allclose(x, y, rtol=2e-12, atol=1e-9, equal_nan=True, err_msg=str(label))
    else:
        assert np.array_equal(x, y), label


SUM = set('volume amount count buy_volume buy_amount buy_count sell_volume sell_amount sell_count unknown_volume unknown_amount unknown_count off_exchange_volume off_exchange_amount off_exchange_count large_order medium_order small_order large_buy large_sell medium_buy medium_sell small_buy small_sell n_seconds'.split())


def data_job(item):
    root, j = item
    source = Path(j['input']); st = source.stat()
    assert (st.st_size, st.st_mtime_ns) == (j['source_bytes'], j['source_mtime_ns']), source
    t = pq.read_table(source); v = {k: t[k].to_numpy() for k in t.column_names}; n = len(t)
    ts = v['ts']; assert n and np.all(np.diff(ts) > 0) and np.all(ts % 60 == 0)
    assert np.all((ts >= j['open_ts']) & (ts < j['close_ts'])) and np.all(v['n_seconds'] == 60)
    assert np.all(v['ric'] == j['ric']) and np.all(v['venue'] == j['venue'])
    segment = np.r_[True, np.diff(ts) != 60]
    origin = np.maximum.accumulate(np.where(segment, np.arange(n), 0))
    records = []
    for freq in ['1h', '1day']:
        start = np.array([0]) if freq == '1day' else np.flatnonzero(segment | ((np.arange(n)-origin) % 60 == 0))
        end = np.r_[start[1:], n]; last = end-1; length = end-start
        family = f'baseline_data_{freq}' + ('_hfq' if j['hfq'] else '') + '_raw'
        rel = Path(j['venue'])/j['ric']/f"{j['day']}.parquet"; p = root/family/rel
        mpath = root/family/'_bars'/rel.with_suffix('.json.zst')
        deadline=time.monotonic()+3600
        while not p.exists() or not mpath.exists():
            assert time.monotonic()<deadline,('producer timeout',p)
            time.sleep(.2)
        o = pq.read_table(p); assert o.schema.equals(t.schema, check_metadata=False) and len(o) == len(start), p
        a = {k: o[k].to_numpy() for k in o.column_names}
        m = load(mpath)
        assert m['frequency'] == freq and m['session_date'] == j['day']
        metadata = dict(ts=ts[start], end_ts=ts[last]+60, last_minute_ts=ts[last], source_minutes=length,
                        segment_start=np.ones(len(start), bool) if freq == '1day' else segment[start],
                        source_depth_ts_utc_ns=v.get('source_depth_ts_utc_ns',np.full(n,np.nan))[last],
                        source_quality_flags_any=np.bitwise_or.reduceat(v.get('source_quality_flags',np.zeros(n,dtype=np.int8)), start))
        assert len(m['bars']) == len(start)
        for k, expected in metadata.items():
            got = [b[k] for b in m['bars']]
            if k == 'source_depth_ts_utc_ns':
                got = np.asarray([np.nan if x is None else x for x in got])
            eq(expected, got, (p, k, 'metadata'))
        e = {k: v[k][last].copy() for k in v}; e['ts'] = ts[start]
        for k in SUM & v.keys():
            e[k] = np.add.reduceat(v[k], start)
            eq(e[k], a[k], (p, k))
        active = v['count'] > 0; anytrade = np.logical_or.reduceat(active, start)
        first = np.minimum.reduceat(np.where(active, np.arange(n), n), start)
        final = np.maximum.reduceat(np.where(active, np.arange(n), -1), start)
        for k, indices in [('open', first), ('close', final)]:
            e[k][anytrade] = v[k][indices[anytrade]]
        for k, mask, reducer, sentinel in [
            ('high', active, np.maximum, -np.inf), ('low', active, np.minimum, np.inf),
            ('buy_high', v['buy_count'] > 0, np.maximum, -np.inf),
            ('sell_low', v['sell_count'] > 0, np.minimum, np.inf)]:
            if k in v:
                has = np.logical_or.reduceat(mask, start)
                values = reducer.reduceat(np.where(mask, v[k], sentinel), start)
                e[k][has] = values[has]
        for k, weight in [('vwap','volume'),('buy_vwap','buy_volume'),('sell_vwap','sell_volume')]:
            assert np.isfinite(v[weight]).all() and np.all(v[weight] >= 0)
            contribution = np.zeros(n); np.multiply(v[k], v[weight], out=contribution, where=v[weight] > 0)
            e[k] = np.divide(np.add.reduceat(contribution,start), a[weight], out=e[k], where=a[weight] > 0)
        e['twap'] = np.add.reduceat(v['twap'],start)/length
        e['avg_amount'] = np.divide(a['amount'], a['count'], out=np.zeros(len(o)), where=a['count'] > 0)
        for k,b,s in [('net_buy_amount','buy_amount','sell_amount'),('net_buy_volume','buy_volume','sell_volume'),
                      ('net_buy_large','large_buy','large_sell'),('net_buy_medium','medium_buy','medium_sell'),('net_buy_small','small_buy','small_sell')]:
            e[k] = a[b]-a[s]
        den = a['buy_amount']+a['sell_amount']
        e['net_buy_pct'] = np.divide(a['buy_amount']-a['sell_amount'], den, out=np.zeros(len(o)), where=den > 0)
        for k in v:
            eq(e[k], a[k], (p,k))
            if pa.types.is_floating(o.schema.field(k).type):
                # Baseline unknown values are physical Arrow nulls, not NaN payloads.
                assert o[k].null_count == int(np.sum(~np.isfinite(a[k]))), (p,k,'null encoding')
        records.append(dict(family=family,ric=j['ric'],path=str(p.relative_to(root)),rows=len(o),
                            short_bars=int(np.sum(length < 60)) if freq == '1h' else 0,
                            sha256=sha(p),metadata_sha256=sha(mpath)))
    st = source.stat(); assert (st.st_size,st.st_mtime_ns)==(j['source_bytes'],j['source_mtime_ns'])
    return dict(input=j['input'],input_sha256=sha(source),input_rows=n,outputs=records)


def factor_job(item):
    root, freq, venue, ric, days, calendar = item
    family = f'baseline_factor_{freq}_hfq_raw'; input_family = f'baseline_data_{freq}_hfq_raw'
    last_output=root/family/venue/ric/f'{days[-1]}.parquet';deadline=time.monotonic()+7200
    while not last_output.exists():
        assert time.monotonic()<deadline,('factor producer timeout',last_output)
        time.sleep(.5)
    columns = None; matrices=[]; closes=[]; highs=[]; lows=[]; starts=[]; records=[]; previous=None
    for day in days:
        rel=Path(venue)/ric/f'{day}.parquet'; p=root/family/rel; t=pq.read_table(p)
        h=pq.read_table(root/input_family/rel); m=load(root/input_family/'_bars'/rel.with_suffix('.json.zst'))
        assert t.num_columns==636 and len(t)==len(h),p
        for k in ['venue','ric','ts']:assert t[k].equals(h[k]),(p,k)
        assert t['source_depth_ts_utc_ns'].null_count==len(t),p
        names=t.column_names[4:]
        if columns is None:columns=names
        assert columns==names and all(t[k].type==pa.float64() and t[k].null_count==0 for k in names),p
        matrix=np.column_stack([t[k].to_numpy() for k in names]);assert not np.isinf(matrix).any(),p
        reset=np.asarray([b['segment_start'] for b in m['bars']]);reset[0]=previous is None or calendar[day]!=previous+1
        assert np.isnan(matrix[reset]).all(),(p,'reset rows')
        previous=calendar[day];starts.extend(reset);closes.extend(h['close'].to_numpy())
        highs.extend(h['high'].to_numpy());lows.extend(h['low'].to_numpy());matrices.append(matrix)
        records.append(dict(family=family,ric=ric,path=str(p.relative_to(root)),rows=len(t),sha256=sha(p)))
    matrix=np.concatenate(matrices);close=np.asarray(closes);reset=np.asarray(starts)
    with np.errstate(divide='ignore',invalid='ignore'):
        returns=np.r_[np.nan,np.log(close[1:]/close[:-1])]
    valid=np.r_[False,(close[1:]>0)&(close[:-1]>0)&np.isfinite(close[1:])&np.isfinite(close[:-1])]
    returns[reset|~valid]=np.nan
    for name,fun in [('lseg_features_baseline_159',np.sin),('lseg_features_baseline_160',np.cos)]:
        expected=np.r_[np.nan,fun(returns[:-1])];expected[reset]=np.nan
        eq(expected,matrix[:,columns.index(name)],(family,ric,name))
    for name in ['lseg_features_factor_009','lseg_features_factor_013','lseg_features_factor_014']:
        assert np.isnan(matrix[:,columns.index(name)]).all(),(family,ric,name,'unavailable depth')
    # Independent fixed-window means verify bar units and overnight continuity.
    boundaries=np.r_[np.flatnonzero(reset),len(close)]
    for name,window,arrays in [('lseg_features_baseline_097',45,[close]),
                               ('lseg_features_baseline_106',20,[close,np.asarray(highs),-np.asarray(lows)])]:
        raw=np.full(len(close),np.nan)
        for beg,end in zip(boundaries[:-1],boundaries[1:]):
            for i in range(beg+window-1,end):
                parts=[x[i-window+1:i+1] for x in arrays]
                if all(np.isfinite(x).all() for x in parts):raw[i]=sum(float(np.mean(x)) for x in parts)
        expected=np.r_[np.nan,raw[:-1]];expected[reset]=np.nan
        eq(expected,matrix[:,columns.index(name)],(family,ric,name))
    return dict(family=family,ric=ric,columns=columns,finite=np.isfinite(matrix).sum(axis=0).tolist(),records=records)


def init():
    pa.set_cpu_count(1);pa.set_io_thread_count(1)


def main():
    ap=argparse.ArgumentParser(description=__doc__);ap.add_argument('mode',choices=['data','factor'])
    ap.add_argument('--run',type=Path,required=True);ap.add_argument('--root',type=Path,required=True)
    ap.add_argument('--workers',type=int,default=16);args=ap.parse_args();began=time.monotonic()
    jobs=json.loads((args.run/'jobs.json').read_text());checks=[];totals={};availability=[]
    if args.mode=='data':
        tasks=[(args.root,j) for j in jobs];fn=data_job
    else:
        calendar={r['session_date'].replace('-',''):i for i,r in enumerate(csv.DictReader((args.run/'calendar.csv').open()))}
        byric={}
        for j in jobs:
            if j['hfq']:byric.setdefault((j['venue'],j['ric']),[]).append(j['day'])
        tasks=[(args.root,f,v,r,sorted(days),calendar) for f in ['1h','1day'] for (v,r),days in sorted(byric.items())];fn=factor_job
    with ProcessPoolExecutor(args.workers,mp_context=mp.get_context('spawn'),initializer=init) as pool:
        for i,result in enumerate(pool.map(fn,tasks,chunksize=4 if args.mode=='data' else 1)):
            checks.append(result)
            outputs=result['outputs'] if args.mode=='data' else result['records']
            for r in outputs:
                counter=totals.setdefault(r['family'],Counter());counter['files']+=1;counter['rows']+=r['rows'];counter['short_bars']+=r.get('short_bars',0)
            if args.mode=='factor':
                for name,count in zip(result['columns'],result['finite']):
                    availability.append([result['family'],result['ric'],name,count,sum(x['rows'] for x in outputs)])
            if (i+1)%(1000 if args.mode=='data' else 10)==0:print(json.dumps(dict(mode=args.mode,completed=i+1,total=len(tasks))),flush=True)
    expected={}
    for j in jobs:
        if args.mode=='factor' and not j['hfq']:continue
        for freq in ['1h','1day']:
            family=f'baseline_factor_{freq}_hfq_raw' if args.mode=='factor' else f'baseline_data_{freq}'+('_hfq' if j['hfq'] else '')+'_raw'
            expected.setdefault(family,set()).add(f"{j['venue']}/{j['ric']}/{j['day']}.parquet")
    for family,files in expected.items():
        actual={str(p.relative_to(args.root/family)) for p in (args.root/family).glob('*/*/*.parquet')}
        assert actual==files,(family,'file set mismatch')
    if args.mode=='data':
        audit=args.root/'_audit/data_jobs.json.zst';deadline=time.monotonic()+3600
        while not audit.exists():
            assert time.monotonic()<deadline,'producer audit timeout'
            time.sleep(.2)
        producer={r['job']['input']:r for r in load(audit)}
        assert len(producer)==len(jobs)
        for r in checks:
            p=producer[r['input']];assert r['input_sha256']==p['input_sha256'] and r['input_rows']==p['input_rows']
            for actual,stored in zip(r['outputs'],p['outputs']):
                assert actual['sha256']==stored['sha256'] and actual['metadata_sha256']==stored['metadata_sha256']
    else:
        with (args.run/'factor_availability.csv').open('w') as f:
            w=csv.writer(f);w.writerow(['family','ric','factor','finite_rows','rows']);w.writerows(availability)
    dump(args.run/f'{args.mode}_checks.json.zst',checks)
    result=dict(status='passed',mode=args.mode,seconds=time.monotonic()-began,totals=totals,script_sha256=sha(Path(__file__)))
    (args.run/f'{args.mode}_validation.json').write_text(json.dumps(result,indent=2));print(json.dumps(result),flush=True)


if __name__=='__main__':main()
