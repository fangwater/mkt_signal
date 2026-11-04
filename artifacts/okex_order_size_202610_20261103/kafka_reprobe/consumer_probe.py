"""Read-only Kafka probe: explicit assignment, no consumer group or offset commits."""
import argparse,datetime,hashlib,json,logging,socket,time,zlib,traceback
from pathlib import Path
import kafka
from kafka import KafkaConsumer,TopicPartition

parser=argparse.ArgumentParser()
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--api-version',default='auto')
parser.add_argument('--seconds',type=float,default=20)
parser.add_argument('--samples',type=int,default=5)
args=parser.parse_args()
args.output.mkdir(parents=True,exist_ok=True)
logging.basicConfig(filename=args.output/'client.log',level=logging.DEBUG,format='%(asctime)s %(name)s %(levelname)s %(message)s')
original=socket.getaddrinfo
rewrites=[]
def resolve(host,port,*a,**kw):
    if host=='10.140.140.55':
        rewrites.append({'host':host,'to_host':'156.231.137.189','port':port})
        host='156.231.137.189'
    return original(host,port,*a,**kw)
socket.getaddrinfo=resolve
report={'observed_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'library':'kafka-python '+kafka.__version__,'requested_api_version':args.api_version,'read_only':True,'group_id':None,'enable_auto_commit':False,'rewrites':rewrites,'messages':[]}
t=time.monotonic()
consumer=None
try:
    av=None if args.api_version=='auto' else tuple(map(int,args.api_version.split('.')))
    consumer=KafkaConsumer(bootstrap_servers=['156.231.137.189:1501'],group_id=None,enable_auto_commit=False,allow_auto_create_topics=False,api_version=av,bootstrap_timeout_ms=5000,request_timeout_ms=12000,session_timeout_ms=10000,fetch_max_bytes=10_485_760,max_partition_fetch_bytes=10_485_760,security_protocol='PLAINTEXT',client_id='okex_readonly_probe')
    report['negotiated_api_version']=consumer.config['api_version']
    parts=consumer.partitions_for_topic('okex-swap')
    report['partitions']=sorted(parts) if parts else []
    if not parts: raise RuntimeError('No okex-swap partitions')
    tps=[TopicPartition('okex-swap',p) for p in sorted(parts)]
    consumer.assign(tps)
    lows=consumer.beginning_offsets(tps,timeout_ms=10000)
    highs=consumer.end_offsets(tps,timeout_ms=10000)
    report['watermarks']=[{'topic':tp.topic,'partition':tp.partition,'low':lows[tp],'high':highs[tp]} for tp in tps]
    for tp in tps: consumer.seek(tp,max(lows[tp],highs[tp]-3))
    start=time.monotonic()
    while time.monotonic()-start<args.seconds and len(report['messages'])<args.samples:
        rows=consumer.poll(timeout_ms=1000,max_records=args.samples-len(report['messages']))
        for tp,records in rows.items():
            for row in records:
                name=f'{row.topic}_{row.partition}_{row.offset}.value'
                (args.output/name).write_bytes(row.value)
                info={'topic':row.topic,'partition':row.partition,'offset':row.offset,'timestamp':row.timestamp,'bytes':len(row.value),'sha256':hashlib.sha256(row.value).hexdigest(),'file':name}
                try:
                    pb=zlib.decompress(row.value)
                    (args.output/(name+'.pb')).write_bytes(pb)
                    info['zlib_decoded_bytes']=len(pb)
                    import period_pb2
                    period=period_pb2.PeriodMessage.FromString(pb)
                    info.update({'period':period.period,'ts':period.ts,'post_ts':period.post_ts,'poster_id':period.poster_id,'symbols':len(period.symbol_infos),'trades':sum(len(s.trades) for s in period.symbol_infos),'incs':sum(len(s.incs) for s in period.symbol_infos)})
                    info['selected_symbols']={s.symbol:{'trade_count':len(s.trades),'inc_count':len(s.incs),'trades':[{'timestamp':x.timestamp,'side':x.side,'price':x.price,'amount':x.amount} for x in s.trades[:10]]} for s in period.symbol_infos if s.symbol in {'TRBUSDT','DOTUSDT','ADAUSDT','NEARUSDT','ATOMUSDT','DOGEUSDT','ETCUSDT','CRVUSDT'}}
                except Exception as exc:
                    info['decode_error']=f'{type(exc).__name__}: {exc}'
                report['messages'].append(info)
    report['success']=bool(report['messages'])
except Exception as exc:
    report['success']=False
    report['error']=f'{type(exc).__name__}: {exc}'
    report['traceback']=traceback.format_exc()
finally:
    if consumer is not None: consumer.close(autocommit=False)
    report['elapsed_seconds']=time.monotonic()-t
    (args.output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({k:v for k,v in report.items() if k not in {'messages','traceback'}},indent=2))
    for info in report['messages']:
        print(json.dumps({k:v for k,v in info.items() if k!='selected_symbols'}))
