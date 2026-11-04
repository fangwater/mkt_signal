"""Bounded public OKX trade capture for comparison with Kafka messages."""
import json,time,datetime,argparse
from pathlib import Path
import websocket
parser=argparse.ArgumentParser()
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--seconds',type=float,default=30)
args=parser.parse_args()
result={'started_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'endpoint':'wss://ws.okx.com:8443/ws/v5/public','trades':[]}
ws=None
try:
 ws=websocket.create_connection(result['endpoint'],timeout=8)
 ws.send(json.dumps({'op':'subscribe','args':[{'channel':'trades','instId':f'{s}-USDT-SWAP'} for s in ['TRB','DOT','ADA','NEAR','ATOM','DOGE','ETC','CRV']]}))
 ws.settimeout(2)
 t=time.monotonic()
 while time.monotonic()-t<args.seconds:
  try: msg=ws.recv()
  except websocket.WebSocketTimeoutException: continue
  if not msg: break
  if msg=='pong': continue
  obj=json.loads(msg)
  result['trades'].extend(obj.get('data',[]))
except Exception as exc: result['error']=f'{type(exc).__name__}: {exc}'
finally:
 if ws is not None: ws.close()
 result['finished_at_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 args.output.write_text(json.dumps(result,indent=2)+'\n')
 print(json.dumps({'captured':len(result['trades']),'error':result.get('error'),'output':str(args.output)}))
