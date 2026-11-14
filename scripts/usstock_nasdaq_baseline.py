"""Python correctness reference: XNAS.ITCH MBO -> RTH minute depth/trades.

Ordinary availability is ts_event + ts_in_delta; complete event boundaries and
source order take precedence over exchange timestamps. No cross-minute book
fill. T is counted once; F and C are never counted as additional executions.
"""
import argparse
import csv
import gzip
import json
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

NS = 10**9
HEADER = 'ts_event,rtype,publisher_id,instrument_id,action,side,price,size,channel_id,order_id,flags,ts_in_delta,sequence,symbol'.split(',')


def timestamp(text):
    whole, frac = text[:-6].split('.') if '.' in text else (text[:-6], '')
    if not text.endswith('+00:00') or len(frac)>9:
        raise ValueError('timestamp is not UTC nanoseconds')
    return int(datetime.fromisoformat(whole).replace(tzinfo=timezone.utc).timestamp())*NS+int(frac.ljust(9,'0') or '0')


def extract(source, symbol, open_ts, close_ts):
    orders, levels = {}, {'B': Counter(), 'A': Counter()}
    output = {s:dict(ts=s,source_quality_flags=0,trade_time_uncertain=False,source_depth_ts_utc_ns=None,source_row=None,book_depth=0,
                    bid=[],ask=[],trades=[]) for s in range(open_ts,close_ts,60)}
    actions=Counter(); flags_seen=Counter(); pending=[]
    available=None; current_minute=None; last_row=None; instrument=None
    invalid_since=None;invalid_intervals=[]
    group_start=0;group_bad_time=False;deferred_bad_time=None;bounded_bad_timestamp_intervals=[]

    def adjust(order,sign):
        side,price,size=order
        levels[side][price]+=sign*size
        if levels[side][price]<0:raise ValueError('negative level')
        if levels[side][price]==0:del levels[side][price]

    def apply(r):
        action=r['action'];k=(int(r['channel_id']),int(r['order_id']))
        if action=='R':
            orders.clear();levels['B'].clear();levels['A'].clear()
        elif action in ('A','M'):
            if (action=='A')==(k in orders):raise ValueError('invalid A/M identity')
            if k in orders:adjust(orders.pop(k),-1)
            p=Decimal(r['price'])*NS;size=int(r['size'])
            if p!=int(p) or p<=0 or size<=0 or r['side'] not in levels:
                raise ValueError('invalid order price/size/side')
            orders[k]=(r['side'],int(p),size);adjust(orders[k],1)
        elif action=='C':
            if k not in orders:raise ValueError('cancel unknown order')
            old=orders.pop(k);adjust(old,-1);remaining=old[2]-int(r['size'])
            if remaining<0 or r['side']!=old[0]:raise ValueError('invalid cancel')
            if remaining:orders[k]=(old[0],old[1],remaining);adjust(orders[k],1)
        elif action not in ('T','F','N'):raise ValueError('unknown action')

    def flush():
        if current_minute not in output:return
        if invalid_since is not None:return
        r=output[current_minute]
        bids=sorted(levels['B'],reverse=True)[:10];asks=sorted(levels['A'])[:10]
        r['source_row']=last_row
        if not bids or not asks or bids[0]>asks[0]:return
        r.update(source_depth_ts_utc_ns=available,book_depth=min(len(bids),len(asks)),
                 bid=[[p,levels['B'][p]] for p in bids],
                 ask=[[p,levels['A'][p]] for p in asks])

    with gzip.open(source,'rt',newline='') as f:
        reader=csv.DictReader(f)
        if reader.fieldnames!=HEADER:raise ValueError('MBO header mismatch')
        for row_no,r in enumerate(reader,1):
            if r['rtype']!='160' or r['publisher_id']!='2' or r['symbol']!=symbol:
                raise ValueError('MBO source identity mismatch')
            if instrument is None:instrument=r['instrument_id']
            if instrument!=r['instrument_id']:raise ValueError('multiple instruments')
            flags=int(r['flags']);flags_seen[str(flags)]+=1;actions[r['action']]+=1
            if flags&8 and row_no==1 and r['action']=='R' and not flags&(32|64|16|4):
                apply(r);continue
            if flags&(32|64|16):raise ValueError('unsupported time/book quality flags')
            recv=0 if flags&8 else timestamp(r['ts_event'])+int(r['ts_in_delta'])
            pending.append((r,recv,row_no))
            group_bad_time|=bool(flags&8)
            if not flags&128:continue
            if group_bad_time:
                has_trade=any(e['action']=='T' and Decimal(e['price'] or '0')>0 and int(e['size'])>0 for e,_,_ in pending[group_start:])
                deferred_bad_time=(available or 0,has_trade or (deferred_bad_time[1] if deferred_bad_time else False))
                group_start=len(pending);group_bad_time=False
                continue
            next_available=max([x[1] for x in pending]+([available] if available else []))
            if deferred_bad_time is not None:
                left,has_trade=deferred_bad_time
                if left<=0 or left//(60*NS)!=next_available//(60*NS):
                    raise ValueError('bad receive timestamp lacks trusted bounds within one minute')
                bounded_bad_timestamp_intervals.append((left,next_available,has_trade));deferred_bad_time=None
            minute=next_available//(60*NS)*60
            if current_minute is not None and minute!=current_minute:flush()
            has_gap=any(int(event['flags'])&4 for event,_,_ in pending)
            if has_gap and invalid_since is None:
                invalid_since=next_available
                orders.clear();levels['B'].clear();levels['A'].clear()
            for event,_,source_row in pending:
                if not has_gap and event['action']=='R' and invalid_since is not None:
                    invalid_intervals.append((invalid_since,next_available));invalid_since=None
                if invalid_since is None:apply(event)
                if event['action']=='T' and open_ts*NS<=next_available<close_ts*NS:
                    if event['side'] not in 'BAN':raise ValueError('unknown trade side')
                    price=Decimal(event['price'] or '0')*NS;size=int(event['size'])
                    if price>0 and size>0:
                        if price!=int(price):raise ValueError('noninteger nanodollar')
                        output[minute]['trades'].append([next_available,int(price),size,event['side'],source_row])
            pending.clear();available=next_available;current_minute=minute;last_row=row_no
            group_start=0
        ignored_post_session_incomplete_rows=len(pending)
        if deferred_bad_time is not None or any(recv<close_ts*NS for _,recv,_ in pending):
            raise ValueError('file ended inside an atomic event within/before the session')
    flush()
    if invalid_since is not None:
        invalid_intervals.append((invalid_since,max(close_ts*NS,available+1)))
    for minute in output.values():
        start=minute['ts']*NS
        if any(a<start+60*NS and b>start for a,b in invalid_intervals):
            minute.update(source_quality_flags=minute['source_quality_flags']|4,source_depth_ts_utc_ns=None,book_depth=0,bid=[],ask=[])
        for left,right,has_trade in bounded_bad_timestamp_intervals:
            if left<start+60*NS and right>=start:
                minute.update(source_quality_flags=minute['source_quality_flags']|8,
                              trade_time_uncertain=minute['trade_time_uncertain'] or has_trade,
                              source_depth_ts_utc_ns=None,book_depth=0,bid=[],ask=[])
    return dict(rows=row_no,actions=dict(actions),flags=dict(flags_seen),instrument_id=instrument,
                bad_book_intervals_ns=[list(x) for x in invalid_intervals],
                bounded_bad_timestamp_intervals_ns=[list(x) for x in bounded_bad_timestamp_intervals],
                ignored_post_session_incomplete_rows=ignored_post_session_incomplete_rows,minutes=list(output.values()))


def aggregate(ric, extracted, thresholds=None):
    """Independent, deliberately simple minute baseline reference.

    Notionals and thresholds are exact Decimal dollar amounts. Thresholds are
    previous natural month linear p50/p90, never the current day/month.
    """
    result=[];last_price=None;prior={}
    for m in extracted['minutes']:
        row=dict(ric=ric,venue='NASDAQ',ts=m['ts'],book_depth=m['book_depth'],source_quality_flags=m['source_quality_flags'],
                 source_depth_ts_utc_ns=m['source_depth_ts_utc_ns'],n_seconds=60)
        trades=[(t[0],Decimal(t[1])/NS,t[2],t[3]) for t in m['trades']]
        groups={'':trades,'buy_':[t for t in trades if t[3]=='B'],
                'sell_':[t for t in trades if t[3]=='A'],
                'unknown_':[t for t in trades if t[3]=='N'],'off_exchange_':[]}
        for prefix,ts in groups.items():
            row[prefix+'volume']=sum(t[2] for t in ts)
            row[prefix+'amount']=sum((t[1]*t[2] for t in ts),Decimal(0))
            row[prefix+'count']=len(ts)
        prices=[t[1] for t in trades]
        row.update(open=prices[0] if prices else None,close=prices[-1] if prices else None,
                   high=max(prices,default=None),low=min(prices,default=None),
                   buy_high=max((t[1] for t in groups['buy_']),default=None),
                   sell_low=min((t[1] for t in groups['sell_']),default=None))
        cursor=m['ts']*NS;weighted=Decimal(0)
        for t in trades:
            if last_price is not None:weighted+=last_price*(t[0]-cursor)
            cursor,last_price=t[0],t[1]
        if last_price is not None:weighted+=last_price*((m['ts']+60)*NS-cursor)
        row['twap']=weighted/(60*NS) if last_price is not None and not m['trade_time_uncertain'] else None
        for prefix,name in [('', 'vwap'),('buy_','buy_vwap'),('sell_','sell_vwap')]:
            if row[prefix+'volume']:
                prior[name]=row[prefix+'amount']/row[prefix+'volume']
            row[name]=prior.get(name,last_price)
        row['avg_amount']=row['amount']/row['count'] if row['count'] else Decimal(0)
        row['net_buy_amount']=row['buy_amount']-row['sell_amount']
        row['net_buy_volume']=row['buy_volume']-row['sell_volume']
        directional=row['buy_amount']+row['sell_amount']
        row['net_buy_pct']=row['net_buy_amount']/directional if directional else Decimal(0)
        for size in ['large','medium','small']:
            selected=[] if thresholds is None else [t for t in trades if (
                'large' if t[1]*t[2]>=thresholds[1] else
                'medium' if t[1]*t[2]>=thresholds[0] else 'small')==size]
            row[size+'_order']=sum((t[1]*t[2] for t in selected),Decimal(0))
            for side,name in [('B','buy'),('A','sell')]:
                row[size+'_'+name]=sum((t[1]*t[2] for t in selected if t[3]==side),Decimal(0))
            row['net_buy_'+size]=row[size+'_buy']-row[size+'_sell']
        for side in ['bid','ask']:
            for level in range(10):
                book=m[side]
                row[f'l2_{side}{level}p']=Decimal(book[level][0])/NS if level<len(book) else None
                row[f'l2_{side}{level}v']=book[level][1] if level<len(book) else None
            for pv in ['p','v']:row[f'{side}0{pv}']=row[f'l2_{side}0{pv}']
        row['midp']=row['mid_price']=(row['bid0p']+row['ask0p'])/2 if row['bid0p'] is not None and row['ask0p'] is not None else None
        result.append({k:float(v) if isinstance(v,Decimal) else v for k,v in row.items()})
    return result


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--symbol',required=True)
    p.add_argument('--open-ts',type=int,required=True)
    p.add_argument('--close-ts',type=int,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    result=extract(a.source,a.symbol,a.open_ts,a.close_ts)
    a.output.write_text(json.dumps(result,separators=(',',':'))+'\n')
    print(json.dumps({k:v for k,v in result.items() if k!='minutes'}))


if __name__=='__main__':main()
