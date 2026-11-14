#!/usr/bin/env python3
"""Small-input correctness reference for receive-time TradFi minute bars."""
import csv
import gzip
import math
from itertools import groupby

FIELDS = ('open high low close volume amount avg_amount count buy_count sell_count '
          'buy_amount sell_amount buy_volume sell_volume large_order medium_order small_order '
          'large_buy large_sell medium_buy medium_sell small_buy small_sell vwap buy_vwap '
          'sell_vwap net_buy_amount net_buy_volume net_buy_pct net_buy_large net_buy_medium '
          'net_buy_small').split()
SUM_FIELDS = FIELDS[4:6] + FIELDS[7:23] + FIELDS[29:32]


def records(path, symbol):
    with gzip.open(path, 'rt', newline='') as f:
        for r in csv.DictReader(f):
            if r['exchange'] != 'binance-futures' or r['symbol'] != symbol:
                raise ValueError('source identity mismatch')
            yield r


def day(trades_path, book_path, symbol, start, end, thresholds=None):
    """Independent Python replay; callers use small fixtures or bounded sources."""
    rows = {}
    for ts in range(start, end, 60):
        r = dict(symbol=symbol, ts=ts, **{f: 0.0 for f in FIELDS})
        for f in FIELDS[:4] + FIELDS[23:26]:
            r[f] = None
        r.update(twap=None, mid_price=None, book_depth=0, source_quality_flags=1,
                 source_depth_ts_utc_us=None, source_minutes=1, end_ts=ts+60)
        for side in ('bid', 'ask'):
            for i in range(20):
                for pv in ('p', 'v'):
                    r[f'{side}{i}{pv}'] = None
        rows[ts] = r
    ts_groups = {}
    for r in records(trades_path, symbol):
        recv = int(r['local_timestamp'])
        if not start*10**6 <= recv < end*10**6:
            raise ValueError('source receive timestamp outside file day')
        p, v = float(r['price']), float(r['amount'])
        if not all(math.isfinite(x) and x>0 for x in (p, v, p*v)):
            raise ValueError('invalid trade')
        ts_groups.setdefault(recv//60000000*60, []).append((recv, p, v, r['side']))
    last = None
    prior_vwap = [None]*3
    for ts, row in rows.items():
        trades = sorted(ts_groups.get(ts, []), key=lambda t:t[0])
        cursor, weighted, seeded = ts*10**6, 0.0, last is not None
        if trades:
            row.update(open=trades[0][1], high=max(t[1] for t in trades),
                       low=min(t[1] for t in trades), close=trades[-1][1])
        else:
            for f in FIELDS[:4]:row[f] = last
        for recv, p, v, side in trades:
            if last is not None:weighted += last*(recv-cursor)
            cursor,last=recv,p
            row['volume']+=v;row['amount']+=p*v;row['count']+=1
            if side not in ('buy','sell'):raise ValueError('side')
            row[side+'_volume']+=v;row[side+'_amount']+=p*v;row[side+'_count']+=1
            if thresholds is not None:
                size='large' if p*v>=thresholds[1] else 'medium' if p*v>=thresholds[0] else 'small'
                row[size+'_order']+=p*v;row[size+'_'+side]+=p*v
        if last is not None:weighted+=last*((ts+60)*10**6-cursor)
        row['twap']=weighted/60000000 if seeded else None
        for i,prefix in enumerate(('', 'buy_', 'sell_')):
            if row[prefix+'volume']>0:
                prior_vwap[i]=row[prefix+'amount']/row[prefix+'volume']
            row[['vwap','buy_vwap','sell_vwap'][i]]=prior_vwap[i] if prior_vwap[i] is not None else last
        row['avg_amount']=row['amount']/row['count'] if row['count'] else 0.0
        row['net_buy_amount']=row['buy_amount']-row['sell_amount']
        row['net_buy_volume']=row['buy_volume']-row['sell_volume']
        row['net_buy_pct']=row['net_buy_amount']/row['amount'] if row['amount'] else 0.0
        for size in ('large','medium','small'):row['net_buy_'+size]=row[size+'_buy']-row[size+'_sell']

    book = {'bid':{}, 'ask':{}}
    next_minute=start;depth_recv=None;initialized=False
    def emit(until):
        nonlocal next_minute
        while next_minute<min(until,end):
            r=rows[next_minute]
            bids=sorted(book['bid'].items(),reverse=True)[:20]
            asks=sorted(book['ask'].items())[:20]
            valid=initialized and bids and asks and bids[0][0]<asks[0][0]
            r['source_depth_ts_utc_us']=depth_recv
            if valid:
                r['book_depth']=min(len(bids),len(asks));r['source_quality_flags']=0 if r['book_depth']==20 else 2
                r['mid_price']=(bids[0][0]+asks[0][0])/2
                for side,levels in [('bid',bids),('ask',asks)]:
                    for i,(p,v) in enumerate(levels):r[f'{side}{i}p']=p;r[f'{side}{i}v']=v
            next_minute+=60
    key=lambda r:(r['timestamp'],r['local_timestamp'],r['is_snapshot'])
    previous=None
    for (_,recv_text,snapshot),group in groupby(records(book_path,symbol),key=key):
        recv=int(recv_text)
        if previous is not None and recv<previous:raise ValueError('nonmonotone receive timestamp')
        previous=recv
        emit(recv//60000000*60)
        if snapshot=='true':book={'bid':{},'ask':{}};initialized=True
        for r in group:
            side='bid' if r['side']=='bid' else 'ask' if r['side']=='ask' else None
            if side is None:raise ValueError('invalid book side')
            p,v=float(r['price']),float(r['amount'])
            if v:book[side][p]=v
            else:book[side].pop(p,None)
        depth_recv=recv
    emit(end)
    return list(rows.values())


def aggregate(rows, minutes):
    output=[]
    for _,it in groupby(rows,key=lambda r:r['ts']//(minutes*60)):
        group=list(it);last=group[-1];r=last.copy()
        r.update(ts=group[0]['ts'],source_minutes=len(group),end_ts=last['ts']+60)
        for f in SUM_FIELDS:r[f]=sum(t[f] for t in group)
        traded=[t for t in group if t['count']>0]
        if traded:
            r.update(open=traded[0]['open'],close=traded[-1]['close'],
                     high=max(t['high'] for t in traded),low=min(t['low'] for t in traded))
        else:
            for f in FIELDS[:4]:r[f]=last['close']
        for prefix,name in [('', 'vwap'),('buy_','buy_vwap'),('sell_','sell_vwap')]:
            if r[prefix+'volume']>0:r[name]=r[prefix+'amount']/r[prefix+'volume']
        r['avg_amount']=r['amount']/r['count'] if r['count'] else 0.0
        r['net_buy_amount']=r['buy_amount']-r['sell_amount']
        r['net_buy_volume']=r['buy_volume']-r['sell_volume']
        r['net_buy_pct']=r['net_buy_amount']/r['amount'] if r['amount'] else 0.0
        r['twap']=sum(t['twap'] for t in group)/len(group) if all(t['twap'] is not None for t in group) else None
        output.append(r)
    return output
