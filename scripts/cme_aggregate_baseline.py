"""Independent correctness baseline for CME bars aggregated from published 1min."""
from __future__ import annotations

import argparse
import csv
import io
import itertools
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import zstandard as zstd

FREQUENCIES = {'5min': 5, '15min': 15, '1h': 60, '1day': 1440}
BOOK = ['mid_price'] + [f'{s}{i}{f}' for s in ('bid', 'ask') for i in range(10) for f in ('p', 'v')]
SUM = ['volume','amount','count','buy_count','sell_count','buy_amount','sell_amount',
       'buy_volume','sell_volume','large_order','medium_order','small_order',
       'large_buy','large_sell','medium_buy','medium_sell','small_buy','small_sell',
       'special_count','special_volume','implied_count','implied_volume','implied_amount']
WEIGHTED = [('vwap','volume'),('buy_vwap','buy_volume'),('sell_vwap','sell_volume'),('implied_vwap','implied_volume')]
DERIVED = ['avg_amount','net_buy_amount','net_buy_volume','net_buy_pct','net_buy_large','net_buy_medium','net_buy_small']
PRICE = ['open','high','low','close','twap','implied_twap'] + [p for p,_ in WEIGHTED]


def read_depth_sources(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {}
    with path.open('rb') as stream, zstd.ZstdDecompressor().stream_reader(stream) as decoded:
        output = {}
        for r in csv.DictReader(io.TextIOWrapper(decoded)):
            key = (r['ric'], int(r['ts']))
            source_ns = int(r['source_ts_utc_ns'])
            if key in output or not key[1]*10**9 <= source_ns < (key[1]+60)*10**9:
                raise ValueError('duplicate or out-of-minute source')
            output[key] = source_ns
        return output


def aggregate_table(table: pa.Table, frequency: str, trading_day: int, depth_sources: dict | None = None):
    """All sums use chronological IEEE-f64 addition, never compensated sums."""
    minutes = FREQUENCIES[frequency]
    depth_sources = {} if depth_sources is None else depth_sources
    extra = ['segment_id','offset_e9'] if 'segment_id' in table.column_names else []
    expected = set(['contract_id','ric','ts']+BOOK+SUM+DERIVED+PRICE+extra)
    if set(table.column_names) != expected or not len(table):
        raise ValueError('expected nonempty native 84/86-column CME baseline')
    values = {name:table[name].to_numpy(zero_copy_only=False) for name in table.column_names}
    ts = values['ts']
    if np.any(ts % 60):
        raise ValueError('source timestamps must be UTC minute starts')
    identities = ['contract_id','ric']+extra
    def identity(i):
        return tuple(values[name][i] for name in identities)
    order = sorted(range(len(table)), key=lambda i:(identity(i),int(ts[i])))
    groups=[]
    for _,it in itertools.groupby(order,key=identity):
        rows=list(it)
        if any(values[n][rows[0]] is None for n in identities):
            raise ValueError('null source identity')
        times=ts[rows]
        if np.any(np.diff(times)<=0):
            raise ValueError('duplicate or decreasing contract minute')
        if frequency=='1day':
            groups.append((rows,True))
            continue
        start=0
        for end in range(1,len(rows)+1):
            if end==len(rows) or times[end]-times[end-1]!=60:
                for i in range(start,end,minutes):groups.append((rows[i:min(i+minutes,end)],i==start))
                start=end

    def finite(value):
        return None if value is None or not np.isfinite(value) else float(value)
    def strict_sum(name,rows):
        x=values[name][rows].astype(float)
        return finite(np.add.accumulate(x)[-1])
    def subtract(a,b):
        return None if a is None or b is None else a-b
    result=[]
    for rows,is_start in groups:
        first,last=rows[0],rows[-1]
        r={name:values[name][last] for name in table.column_names}
        for name in SUM:r[name]=strict_sum(name,rows)
        r['ts']=int(ts[first])
        count=r['count']
        if count is None:raise ValueError('unknown input trade count')
        r['avg_amount']=(None if r['amount'] is None else r['amount']/count) if count>0 else 0.0
        for target,buy,sell in [('net_buy_amount','buy_amount','sell_amount'),('net_buy_volume','buy_volume','sell_volume'),
                                ('net_buy_large','large_buy','large_sell'),('net_buy_medium','medium_buy','medium_sell'),('net_buy_small','small_buy','small_sell')]:
            r[target]=subtract(r[buy],r[sell])
        directed=None if r['buy_amount'] is None or r['sell_amount'] is None else r['buy_amount']+r['sell_amount']
        r['net_buy_pct']=None if directed is None else (r['net_buy_amount']/directed if directed>0 else 0.0)
        active=[i for i in rows if values['count'][i]>0]
        if active:
            r['open']=finite(values['open'][active[0]])
            r['close']=finite(values['close'][active[-1]])
            for name,op in [('high',np.max),('low',np.min)]:r[name]=finite(op(values[name][active].astype(float)))
        for name,weight in WEIGHTED:
            w=values[weight][rows].astype(float);p=values[name][rows].astype(float)
            if np.any(~np.isfinite(w)) or np.any(w<0):raise ValueError('unknown or negative volume')
            total=r[weight]
            if total>0:
                contribution=np.zeros(len(rows))
                np.multiply(p,w,out=contribution,where=w!=0)
                numerator=finite(np.add.accumulate(contribution)[-1])
                r[name]=None if numerator is None else numerator/total
        for name in ['twap','implied_twap']:
            total=strict_sum(name,rows);r[name]=None if total is None else total/len(rows)
        for name in PRICE+BOOK:
            r[name]=finite(r[name])
        source_ns=depth_sources.get((str(r['ric']),int(ts[last])))
        has_book=any(r[n] is not None for n in BOOK if n!='mid_price')
        if has_book != (source_ns is not None):raise ValueError('book/source provenance mismatch')
        if source_ns is not None and not int(ts[last])*10**9<=source_ns<(int(ts[last])+60)*10**9:
            raise ValueError('book source outside last minute')
        meta=dict(ts=int(ts[first]),end_ts=int(ts[last])+60,last_minute_ts=int(ts[last]),source_minutes=len(rows),
                  segment_start=is_start,source_depth_ts_utc_ns=source_ns,contract_id=str(r['contract_id']),ric=str(r['ric']))
        result.append((r,meta))
    result.sort(key=lambda pair:(pair[0]['ts'],str(pair[0]['contract_id']),str(pair[0]['ric'])))
    output=pa.Table.from_arrays([pa.array([r[name] for r,_ in result],type=table.schema.field(name).type,from_pandas=True)
                                for name in table.column_names],schema=table.schema)
    metadata=dict(frequency=frequency,bar_minutes=minutes,trading_day=trading_day,
                  **{name:[m[name] for _,m in result] for name in result[0][1]})
    return output,metadata


def main():
    ap=argparse.ArgumentParser();ap.add_argument('--input',type=Path,required=True);ap.add_argument('--output',type=Path,required=True)
    ap.add_argument('--metadata',type=Path,required=True);ap.add_argument('--frequency',choices=list(FREQUENCIES),required=True)
    ap.add_argument('--depth-audit',type=Path);ap.add_argument('--execute',action='store_true');a=ap.parse_args()
    if not a.execute:
        print(json.dumps(dict(input=str(a.input),output=str(a.output),frequency=a.frequency)));return
    assert not a.output.exists() and not a.metadata.exists()
    table,meta=aggregate_table(pq.read_table(a.input),a.frequency,int(a.input.stem),read_depth_sources(a.depth_audit))
    a.output.parent.mkdir(parents=True,exist_ok=True);a.metadata.parent.mkdir(parents=True,exist_ok=True)
    pq.write_table(table,a.output,compression='zstd')
    a.metadata.write_bytes(zstd.ZstdCompressor(level=1).compress(json.dumps(meta,separators=(',',':')).encode()))


if __name__=='__main__':main()
