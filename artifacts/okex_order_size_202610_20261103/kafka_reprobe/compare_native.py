"""Compare captured Kafka payloads with simultaneous official OKX trades."""
from pathlib import Path
import json,math,collections,datetime
import period_pb2
p=Path(__file__).resolve().parent
report=json.loads((p/'jp_live/result.json').read_text())
native=json.loads((p/'native_trades.json').read_text())
ct={x['instId']:float(x['ctVal']) for x in json.loads((p.parent/'okex_instruments.json').read_text())['data'] if x.get('ctVal')}
rows=[]
seen=set()
duplicated=[]
for m in report['messages']:
 identity=(m['ts'],m['sha256'])
 if identity in seen:
  duplicated.append({'offset':m['offset'],'ts':m['ts'],'sha256':m['sha256']})
  continue
 seen.add(identity)
 period=period_pb2.PeriodMessage.FromString((p/'jp_live'/(m['file']+'.pb')).read_bytes())
 for s in period.symbol_infos:
  for t in s.trades:
   rows.append({'symbol':s.symbol,'timestamp_ms':t.timestamp//1000 if t.timestamp>10**14 else t.timestamp,'side':t.side,'price':t.price,'amount':t.amount,'offset':m['offset']})
matched=[]
ambiguous=[]
missed=[]
for n in native['trades']:
 sym=n['instId'].replace('-USDT-SWAP','USDT')
 ts=int(n['ts'])
 side={'buy':'B','sell':'S'}[n['side']]
 candidates=[x for x in rows if x['symbol']==sym and x['side']==side and abs(x['timestamp_ms']-ts)<=2 and math.isclose(x['price'],float(n['px']),rel_tol=1e-12,abs_tol=1e-12)]
 if not candidates:
  missed.append(n)
  continue
 unique={(x['timestamp_ms'],x['amount']):x for x in candidates}
 if len(unique)>1:
  exact=[x for x in unique.values() if x['timestamp_ms']==ts]
  if len(exact)==1: candidates=exact
  else:
   ambiguous.append({'native':n,'candidates':list(unique.values())})
   continue
 x=candidates[0]
 matched.append({'native':n,'kafka':x,'amount_ratio':x['amount']/float(n['sz']),'ctVal':ct[n['instId']]})
assert matched,'No live trades matched'
assert all(math.isclose(x['amount_ratio'],1.0,rel_tol=1e-12) for x in matched),'Kafka quantity differs from official raw sz'
result={'verified_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'source':'Kafka topic okex-swap, partition 0 (not IPC)','match_method':'symbol and side equal; price equal within floating tolerance; timestamps within 2 ms; ambiguous candidates excluded','quantity_unit':'native_contracts','native_trades':len(native['trades']),'matched_unique_trades':len(matched),'matched_by_symbol':dict(collections.Counter(x['kafka']['symbol'] for x in matched)),'ambiguous':len(ambiguous),'unmatched':len(missed),'kafka_records':len(report['messages']),'unique_payloads':len(seen),'duplicate_payload_records':len(duplicated),'all_matched_amounts_equal_official_sz':True,'matches':matched}
(p/'kafka_native_comparison.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps({k:v for k,v in result.items() if k!='matches'},indent=2))
print('Examples',json.dumps([x for x in matched if x['kafka']['symbol']=='DOGEUSDT'][:3],indent=2))
