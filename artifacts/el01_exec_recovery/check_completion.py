import json,urllib.request,tomllib,time,collections,concurrent.futures
cfg=tomllib.load(open('/home/el01/crypto_cta_manager/config/cta-manager.toml','rb'))
def check(s):
 if not s.get('enabled',True):return
 j=json.load(urllib.request.urlopen(s['exec_viz_url'].rstrip('/')+'/snapshot',timeout=10))
 x=next(e['entry'] for e in j['entries'] if e.get('type')=='exec_pre_trade_state');rows=x['rows']
 target=[r for r in rows if r.get('target_qty') is not None]
 incomplete=[r for r in target if not r.get('execution_complete',False)]
 print(json.dumps({'env':s['id'],'checked_ms':int(time.time()*1000),'ready':x['position_ready'],'snapshot_age_ms':int(time.time()*1000)-x['ts_ms'],'rows':len(rows),'targets':len(target),'complete':sum(r.get('execution_complete',False) for r in target),'reasons':dict(collections.Counter(str(r.get('completion_reason')) for r in target)),'live_rows':sum(abs(r.get('live_order_qty',0))>1e-9 for r in rows),'pending_rows':sum(abs(r.get('pending_qty',0))>1e-9 for r in rows),'max_complete_residual_usdt':max([abs(r.get('delta_usdt') or 0) for r in target if r.get('execution_complete')]+[0]),'incomplete':incomplete}),flush=True)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:list(ex.map(check,cfg['sources']))
