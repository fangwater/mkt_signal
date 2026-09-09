"""Test ORDER_SIDE mappings without using side to choose cross-feed matches."""

import argparse
from collections import Counter
import csv
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from reference import open_text, iter_messages
from quote_reference import _e9, _timestamp_ns
from validate_databento_side import match


def sequence_match(left,right):
    # Sequence is only used within one symbol, venue and bounded feed session.
    lkeys=Counter((r['sequence'],r['price'],r['size']) for r in left if r['sequence'])
    index={}
    rkeys=Counter((r['sequence'],r['price'],r['size']) for r in right)
    for j,r in enumerate(right):
        index[(r['sequence'],r['price'],r['size'])]=j
    result=[]
    for l in left:
        key=(l['sequence'],l['price'],l['size'])
        count=rkeys[key]
        if lkeys[key]==count==1 and abs(l['ms']-right[index[key]]['ms'])<=1:
            result.append((index[key],'unique',count))
        else:
            result.append((None,'unmatched' if not count else 'ambiguous',count))
    return result


def run(root, ric, shard, start, db_csv, output):
    output.mkdir(parents=True,exist_ok=False)
    end=(datetime.fromisoformat(start.replace('Z','+00:00'))+timedelta(minutes=10)).isoformat().replace('+00:00','Z')
    path=root/f'merged-Data-part-000000-shard-{shard:06}.csv.zst'
    left=[]
    reached=False
    with open_text(path) as f:
        for m in iter_messages(f):
            if m.ric!=ric:
                continue
            if m.date_time>=end:
                reached=True
                break
            if m.date_time<start or m.message_class!='UPDATE' or m.update_type!='TRADE':
                continue
            fields={x.name:x for x in m.fields}
            def value(name):
                return fields[name].value.strip() if name in fields else ''
            normal='TRDPRC_1' in fields
            price=_e9(value('TRDPRC_1' if normal else 'IRGPRC'))
            size=int(value('TRDVOL_1' if normal else 'IRGVOL'))
            # Retain each clock independently; do not assume equal semantics.
            times={name:value(name) for name in ['TRDTIM_MS','IRGTIM_MS','TIMACT_MS','SALTIM_MS'] if value(name)}
            clock=next((n for n in ['TRDTIM_MS','IRGTIM_MS','TIMACT_MS','SALTIM_MS'] if n in times),None)
            if clock is None:
                raise ValueError('no event time evidence')
            ms=_timestamp_ns(m.date_time[:10]+'T00:00:00Z')//1000000+int(times[clock])
            left.append(dict(row=m.source_row,source_time=m.date_time,ms=ms,price=price,size=size,
                             order_side=value('ORDER_SIDE'),order_id=value('ORDER_ID'),
                             sequence=value('SEQNUM'),trade_id=value('TRADE_ID' if normal else 'IRG_TRDID'),
                             clock=clock,times=json.dumps(times,sort_keys=True),field_names=','.join(sorted(fields))))
    if not reached:
        raise ValueError('incomplete source window')
    with db_csv.open() as f:
        right=list(csv.DictReader(f))
    for r in right:
        r.update(ms=int(r['ts_event'])//1000000,price=int(r['price_e9']),size=int(r['size']))
    summary=dict(ric=ric,start=start,end=end,source=str(path),db_csv=str(db_csv),
                 db_csv_sha256=hashlib.sha256(db_csv.read_bytes()).hexdigest(),
                 lseg_count=len(left),databento_count=len(right),
                 raw_side=dict(Counter(r['order_side'] or 'absent' for r in left)),
                 db_side=dict(Counter(r['side'] for r in right)),
                 time_fields=dict(Counter(r['clock'] for r in left)),results={})
    rows=[]
    for tolerance in [0,1,'sequence']:
        pairs=sequence_match(left,right) if tolerance=='sequence' else match(left,right,tolerance)
        counts=Counter()
        matrix=Counter()
        for l,(j,status,candidates) in zip(left,pairs):
            counts[status]+=1
            d=right[j] if j is not None else None
            truth={'A':'S','B':'B','N':'N'}[d['side']] if d else ''
            raw=l['order_side']
            if d:
                matrix[(raw or 'absent')+'->'+truth]+=1
                if raw in ('1','2'):
                    counts['matched_with_order_side']+=1
                    if truth=='N':
                        counts['order_side_db_unknown']+=1
                    else:
                        counts['order_side_db_known']+=1
                        direct={'1':'B','2':'S'}[raw]
                        counts['direct_correct' if direct==truth else 'reverse_correct']+=1
                else:
                    counts['missing_order_side_db_'+truth]+=1
            rows.append(dict(tolerance_ms=tolerance,**l,match_status=status,candidates=candidates,
                             db_row=d['row'] if d else '',db_sequence=d['sequence'] if d else '',
                             delta_ns=int(d['ts_event'])-l['ms']*1000000 if d else '',truth=truth))
        summary['results'][str(tolerance)]=dict(counts=counts,matrix=matrix)
    with (output/'matches.csv').open('x',newline='') as f:
        w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    with (output/'summary.json').open('x') as f:
        json.dump(summary,f,indent=2)
    print(json.dumps(summary,indent=2))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--staging-root',required=True,type=Path)
    p.add_argument('--ric',required=True)
    p.add_argument('--shard',required=True,type=int)
    p.add_argument('--start',required=True)
    p.add_argument('--db-csv',required=True,type=Path)
    p.add_argument('--output',required=True,type=Path)
    a=p.parse_args()
    run(a.staging_root,a.ric,a.shard,a.start,a.db_csv,a.output)
