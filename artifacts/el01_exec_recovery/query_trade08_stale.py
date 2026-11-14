import sys,json,os,pathlib
sys.path.insert(0,'/home/el01/binance_exec_trade08/scripts')
import binance_cancel_all_std_um_ws_orders as api
from binance_local_ip import resolve_local_address
key,secret=api.load_credentials()
local,source=resolve_local_address(explicit_local_address=None,trade_engine_config=None,env_dir='/home/el01/binance_exec_trade08')
for row in json.load(open('/home/el01/.el01-native-spread-staging/trade08_stale_orders.json')):
 status,body,headers=api.signed_rest_request(base_url=api.DEFAULT_REST_BASE_URL,path='/fapi/v1/order',params={'symbol':row['symbol'],'origClientOrderId':row['client_order_id'],'recvWindow':'10000'},api_key=key,api_secret=secret,method='GET',timeout=10,local_address=local)
 try:d=json.loads(body)
 except Exception:d={}
 print(json.dumps({**row,'http_status':status,'exchange_order_id':d.get('orderId'),'exchange_status':d.get('status'),'error_code':d.get('code'),'executed_qty':d.get('executedQty')}),flush=True)
