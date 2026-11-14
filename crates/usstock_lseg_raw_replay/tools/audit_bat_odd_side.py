"""Quote/midpoint/tick accuracy for frozen BAT trades missing ORDER_SIDE."""

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from reference import open_text, iter_messages
from quote_reference import _timestamp_ns, _e9
from audit_single_side import history, previous, classify
from audit_nbbo_fallback import tick, fallback, stats


def force(side, method, tick_side, previous_side):
    if side!='N':
        return side,method
    if tick_side!='N':
        return tick_side,'forced_tick_rule'
    if previous_side in ('B','S'):
        return previous_side,'forced_previous_side'
    return 'B','forced_default_buy'


def extract(path,ric,end):
    quotes={'bid':[],'ask':[]}
    trades=[]
    fields_seen=Counter()
    reached=False
    with open_text(path) as f:
        for m in iter_messages(f):
            if m.ric!=ric:
                continue
            if m.date_time>=end:
                reached=True
                break
            if m.message_class!='UPDATE':
                continue
            fields={x.name:x.value.strip() for x in m.fields}
            src=_timestamp_ns(m.date_time)
            if m.update_type=='QUOTE':
                for side,px,sz,tm in [('bid','BID','BIDSIZE','BID_TIM_NS'),('ask','ASK','ASKSIZE','ASK_TIM_NS')]:
                    if px not in fields and sz not in fields:
                        continue
                    if px not in fields or sz not in fields:
                        raise ValueError('incomplete quote side')
                    field=tm if fields.get(tm) else 'QUOTIM_NS'
                    if not fields.get(field):
                        raise ValueError('missing quote clock')
                    qt=_timestamp_ns(m.date_time[:10]+'T'+fields[field]+'Z')
                    fields_seen[field]+=1
                    quotes[side].append((src,m.source_row,(_e9(fields[px]),int(fields[sz] or 0),'BAT',src,qt)))
            elif m.update_type=='TRADE':
                normal='TRDPRC_1' in fields
                px,sz=('TRDPRC_1','TRDVOL_1') if normal else ('IRGPRC','IRGVOL')
                tm=next(n for n in ['TRDTIM_MS','IRGTIM_MS','TIMACT_MS','SALTIM_MS'] if fields.get(n))
                fields_seen[tm]+=1
                event=_timestamp_ns(m.date_time[:10]+'T00:00:00Z')+int(fields[tm])*1000000
                trades.append(dict(row=m.source_row,source_ns=src,event_ns=event,price=_e9(fields[px]),volume=int(fields[sz])))
    if not reached:
        raise ValueError('incomplete window')
    return quotes,trades,dict(fields_seen)


def run(root,audit_root,output):
    output.mkdir(parents=True,exist_ok=False)
    all_rows=[]
    provenance=[]
    for name,shard,end in [('arkg',161,'2022-02-24T17:31:00Z'),('arkk',162,'2021-11-01T18:43:00Z')]:
        ric=name.upper()+'.BAT'
        pairs_path=audit_root/(name+'_order_side_sequence_20260908')/'matches.csv'
        with pairs_path.open() as f:
            target={int(r['row']):r for r in csv.DictReader(f) if r['tolerance_ms']=='sequence' and not r['order_side']}
        path=root/f'merged-Data-part-000000-shard-{shard:06}.csv.zst'
        quotes,trades,clock_fields=extract(path,ric,end)
        provenance.append(dict(ric=ric,source=str(path),pairs=str(pairs_path),pairs_sha256=hashlib.sha256(pairs_path.read_bytes()).hexdigest(),clock_fields=clock_fields,first_trade=trades[0]['source_ns']))
        for mode in ['event_ms','source']:
            hist=history(quotes,mode)
            clock='event_ns' if mode=='event_ms' else 'source_ns'
            ticks=sorted((dict(t,event_ns=t[clock]) for t in trades if t['price']>0 and t['volume']>0),key=lambda t:(t['event_ns'],t['row']))
            times=[t['event_ns'] for t in ticks]
            evidence=[]
            for t in sorted(trades,key=lambda t:(t['source_ns'],t['row'])):
                time=t[clock]
                bid,ask=[previous(hist[s],time,t['source_ns'],t['row']) for s in ['bid','ask']]
                old,reason=classify(t['price'],'BAT',bid,ask,'nbbo')
                ts,anchor=tick(t['price'],time,t['source_ns'],t['row'],ticks,times)
                side,method=fallback(t['price'],bid,ask,old,ts)
                method={'nbbo_touch':'venue_bbo_touch','nbbo_midpoint':'venue_bbo_midpoint'}.get(method,method)
                prev=next((s for event,s in reversed(evidence) if event<time),None)
                final,final_method=force(side,method,ts,prev)
                if side!='N':
                    evidence.append((time,side))
                elif ts!='N':
                    evidence.append((time,ts))
                if t['row'] not in target:
                    continue
                r=target[t['row']]
                if r['match_status']!='unique' or int(r['price'])!=t['price'] or int(r['size'])!=t['volume']:
                    raise ValueError('frozen pair mismatch')
                all_rows.append(dict(ric=ric,clock=mode,source_row=t['row'],source_ns=t['source_ns'],event_ns=t['event_ns'],price_e9=t['price'],volume=t['volume'],truth=r['truth'],touch=old,touch_reason=reason,estimate=side,method=method,final=final,final_method=final_method,tick_anchor=anchor,bid_e9=bid[0] if bid else '',ask_e9=ask[0] if ask else '',bid_source_ns=bid[3] if bid else '',ask_source_ns=ask[3] if ask else '',bid_event_ns=bid[4] if bid else '',ask_event_ns=ask[4] if ask else ''))
    if len(all_rows)!=208:
        raise ValueError('expected 104 missing-side trades on each clock')
    summary=dict(created_utc=datetime.now(timezone.utc).isoformat(),provenance=provenance,
                 policy='Frozen missing ORDER_SIDE sample. Same BAT RIC per-side quotes; strict earlier quote/event timestamps plus source availability. Trade clock TIMACT_MS/SALTIM_MS vs BID/ASK_TIM_NS. No Databento labels used in prediction. Native ORDER_SIDE not used as anchor because message gating remains unresolved. Warmup from available same-RIC shard prefix only, not full session. Positive-size trade ticks; no condition/correction netting. No age cutoff. Source-clock sensitivity separate. Native N excluded from accuracy.',results={})
    for mode in ['event_ms','source']:
        rows=[r for r in all_rows if r['clock']==mode]
        known=[r for r in rows if r['truth']!='N']
        if len(known)!=85:
            raise ValueError('expected 85 known-side trades')
        summary['results'][mode]=dict(total=len(rows),db_unknown=len(rows)-len(known),
             metrics={col:stats(known,col) for col in ['touch','estimate','final']},
             layers={m:stats([r for r in known if r['final_method']==m],'final') for m in sorted({r['final_method'] for r in known})},
             per_ric={ric:stats([r for r in known if r['ric']==ric],'final') for ric in ['ARKG.BAT','ARKK.BAT']},
             unknown_truth_predictions=dict(Counter(r['final_method'] for r in rows if r['truth']=='N')))
    with (output/'classified.csv').open('x',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(all_rows[0]));w.writeheader();w.writerows(all_rows)
    with (output/'summary.json').open('x') as f:
        json.dump(summary,f,indent=2)
    print(json.dumps(summary['results'],indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--staging-root',required=True,type=Path)
    p.add_argument('--audit-root',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    a=p.parse_args()
    run(a.staging_root,a.audit_root,a.output)
