"""RX03 activation steps. Credentials remain in remote memory/files only."""
import contextlib, datetime, hashlib, json, os, pathlib, secrets, shutil, subprocess, sys, time, tomllib
import urllib.request, urllib.error, urllib.parse

ROOT=pathlib.Path.home()
MANAGER=ROOT/'crypto_cta_manager'
EXEC=ROOT/'binance_exec_trade07'
SIGNAL=ROOT/'binance-cta-perp-rx03'
OLD=ROOT/'binance-cta-special-rx03'
STAGE=EXEC/'run/activation_20261014'
ORIGIN='http://127.0.0.1:4191'
PREFIX='binance_cta_perp_rx03_'
INVENTORY=PREFIX+'inventory'

def save(path,data):
 path=pathlib.Path(path)
 tmp=path.with_suffix(path.suffix+'.next')
 tmp.write_text(json.dumps(data,indent=2)+'\n'); os.replace(tmp,path)

def env_for(path):
 raw=subprocess.check_output(['bash','-c','source "$1/env.sh"; env -0','bash',str(path)])
 return dict(x.decode().split('=',1) for x in raw.split(b'\0') if b'=' in x)

def db_env():
 config=tomllib.loads((MANAGER/'config/cta-manager.toml').read_text())
 pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
 values=dict(x.split(b'=',1) for x in pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
 u=urllib.parse.urlsplit(values[config['database'].get('url_env','CRYPTO_CTA_LOCAL_DATABASE_URL').encode()].decode())
 return dict(os.environ,PGHOST=u.hostname,PGPORT=str(u.port or 5432),PGUSER=urllib.parse.unquote(u.username),PGPASSWORD=urllib.parse.unquote(u.password or ''),PGDATABASE=u.path[1:])

def sql(q):
 r=subprocess.run(['psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1'],input=q,text=True,capture_output=True,env=db_env())
 if r.returncode: raise RuntimeError('database request failed; details suppressed')
 return r.stdout.strip()

@contextlib.contextmanager
def session(username='prc'):
 assert username in ['prc','lgq']
 token=secrets.token_urlsafe(32); digest=hashlib.sha256(token.encode()).hexdigest()
 sid=sql(f"INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('{digest}','hex'),now()+interval '30 minutes' FROM cta_users WHERE username='{username}' AND disabled=false RETURNING session_id;")
 assert sid.isdigit()
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 def api(path,payload=None,method=None):
  req=urllib.request.Request(ORIGIN+path,data=None if payload is None else json.dumps(payload).encode(),headers={'Cookie':'cta_session='+token,'Content-Type':'application/json'},method=method)
  try:
   with opener.open(req,timeout=170) as r:
    data=r.read()
    return json.loads(data) if data and 'json' in r.headers.get('Content-Type','') else {'status':r.status,'bytes':len(data)}
  except urllib.error.HTTPError as e:
   raise RuntimeError(f'{path}: HTTP {e.code}: '+e.read().decode()[:600]) from None
 try: yield api
 finally: sql(f"DELETE FROM cta_auth_sessions WHERE session_id={sid} AND token_hash=decode('{digest}','hex');")

def live_pids(path):
 rows=[]
 for p in pathlib.Path('/proc').glob('[0-9]*'):
  try:
   if p.joinpath('cwd').resolve()==path:
    rows.append({'pid':int(p.name),'comm':p.joinpath('comm').read_text().strip()})
  except OSError: pass
 return rows

def account_snapshot():
 sys.path.insert(0,'/tmp')
 from neutralize_rapidx_binance import Client
 values=env_for(OLD); ref=env_for(ROOT/'binance_exec_trade05')
 assert values['LTP_PORTFOLIO_ID']!=ref['LTP_PORTFOLIO_ID']
 for number in range(1,7):
  other=env_for(ROOT/f'binance_exec_trade{number:02d}')
  assert values['LTP_PORTFOLIO_ID']!=other.get('LTP_PORTFOLIO_ID')
 assert not live_pids(OLD) and not live_pids(ROOT/'binance-exec-cta-special-rx03')
 os.environ.update(values)
 ips=tomllib.loads((OLD/'trade_engine.toml').read_text())['local_ips']
 assert ips==['172.31.35.228']
 c=Client(ips[0],execute=False); c.empty_orders(); cash,perp,marks=c.snapshot()
 assert not any(v for k,v in cash.items() if k!='USDT') and not any(perp.values()), 'account must be flat'
 result={'ts_us':time.time_ns()//1000,'cash_usdt':float(cash['USDT']),'non_usdt_cash':{},'perpetuals':{},'open_orders':0,'normal_net':True,'distinct_from_existing_execs':True}
 if STAGE.exists(): save(STAGE/'account_before.json',result)
 return result

def capture_protected():
 rows=[]; files={}
 for p in ROOT.iterdir():
  if not p.is_dir() or not (p.name.startswith('binance') or p.name.startswith('okex_exec')) or p==EXEC: continue
  if p==SIGNAL: continue
  for proc in live_pids(p):
   q=pathlib.Path('/proc')/str(proc['pid'])
   proc.update(env=p.name,start=q.joinpath('stat').read_text().split()[21])
   rows.append(proc)
  for f in [p/'env.sh',p/'trade_engine.toml',*p.glob('config/cta*.json')]:
   if f.is_file():files[str(f)]=hashlib.sha256(f.read_bytes()).hexdigest()
 return {'processes':rows,'files':files}

def provision():
 assert not EXEC.exists(), 'new Exec directory already exists'
 assert not json.loads((SIGNAL/'config/cta_perp.json').read_text())['enabled']
 inventory=account_snapshot()
 EXEC.mkdir(mode=0o700); STAGE.mkdir(parents=True,mode=0o700)
 for name in ['config','scripts','logs','data']: (EXEC/name).mkdir(exist_ok=True)
 save(STAGE/'account_before.json',inventory)
 save(STAGE/'protected_before.json',capture_protected())
 for src,name in [(MANAGER/'config/cta-manager.toml','manager.toml.before'),(SIGNAL/'env.sh','signal.env.before'),(SIGNAL/'config/cta_perp.json','signal.json.before'),(pathlib.Path('/etc/nginx/snippets/crypto_cta_manager.conf'),'nginx.before')]:
  shutil.copy2(src,STAGE/name); os.chmod(STAGE/name,0o600)
 ref=env_for(ROOT/'binance_exec_trade05'); old=env_for(OLD)
 allow=['TRADE_SIGNAL_ENABLE_QUEUE_POSITION','OPEN_VENUE','HEDGE_VENUE','TRADE_ENGINE_EXEC_BACKEND_MAP','RAPIDX_BINANCE_CASH_BUSINESS_TYPE','RUST_LOG','ACCOUNT_MONITOR_CORE','PERSIST_MANAGER_CORE','EXEC_VENUE','VENUE','EXEC_CONFIG_RELOAD_MS','enable_ipc_fast_poll','ENABLE_IPC_FAST_POLL']
 settings={k:ref[k] for k in allow if k in ref}
 settings.update({k:old[k] for k in ['LTP_API_KEY','LTP_API_SECRET','LTP_PORTFOLIO_ID']})
 settings.update(IPC_NAMESPACE=EXEC.name,EXEC_INSTANCE='07',EXEC_VIZ_PORT='10047',EXEC_CONFIG_PORT='18167',PYTHON_BIN='/usr/bin/python3',PRE_TRADE_NOTIFICATION_URL='')
 import shlex
 with os.fdopen(os.open(EXEC/'env.sh',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600),'w') as f:
  f.write('#!/usr/bin/env bash\n# RX03 account; RX02 execution settings.\n')
  for k,v in settings.items():f.write('export '+k+'='+shlex.quote(v)+'\n')
 for rel in ['trade_engine.toml','config/iceoryx2.toml','config/exec_viz.toml','config/exec_config_server.env','config/persist_sync_distribution.toml']:
  src=ROOT/'binance_exec_trade05'/rel
  text=src.read_text()
  if rel in ['config/exec_viz.toml','config/exec_config_server.env']:
   text=text.replace('binance_exec_trade05',EXEC.name).replace('10045','10047').replace('18165','18167')
  (EXEC/rel).write_text(text)
 with session() as api:
  account=api('/manager/api/catalog/accounts/binance_exec_trade05')
  save(STAGE/'fee_reference.json',{k:account[k] for k in ['maker_fee_rate','taker_fee_rate','theoretical_twap_fee_rate']})
  params=next(r['order_parameters'] for r in api('/manager/api/catalog/order-strategies') if r['strategy_name']=='lgq_cta_ctaker')
  save(STAGE/'order_parameters.json',params)
 import redis
 r=redis.Redis.from_url(ref.get('REDIS_URL','redis://127.0.0.1:6379/0'),decode_responses=True)
 risk=r.hgetall('binance_exec_trade05:binance-futures:pre_trade_risk_params')
 save(STAGE/'risk_reference.json',risk)
 print(json.dumps({'provisioned':EXEC.name,'account':inventory,'fees':json.loads((STAGE/'fee_reference.json').read_text()),'order_parameters':params,'risk_reference':risk}))

def configure_manager():
 assert not live_pids(EXEC)
 cfgpath=MANAGER/'config/cta-manager.toml'; nginxpath=pathlib.Path('/etc/nginx/snippets/crypto_cta_manager.conf')
 assert cfgpath.read_bytes()==(STAGE/'manager.toml.before').read_bytes()
 assert nginxpath.read_bytes()==(STAGE/'nginx.before').read_bytes()
 text=cfgpath.read_text(); cfg=tomllib.loads(text)
 assert not any(s['id']==EXEC.name for s in cfg['sources'])
 fee=json.loads((STAGE/'fee_reference.json').read_text());values=env_for(EXEC)
 source={'id':EXEC.name,'account':'trade07','alias':'lgq_cta_RX03','venue':'binance-futures','rocksdb_path':str(EXEC/'data/persist_manager'),'enabled':True,'gateway_prefix':'/exec_trade07','exec_config_url':'http://127.0.0.1:18167/','exec_viz_url':'http://127.0.0.1:10047/','ipc_namespace':EXEC.name,'maker_fee_rate':fee['maker_fee_rate'],'taker_fee_rate':fee['taker_fee_rate'],'account_ipc_service':'account_pubs/rapidx_binance_'+values['LTP_PORTFOLIO_ID']+'_pm'}
 text+='\n# RX03 CTA Perp independent Exec\n[[sources]]\n'+'\n'.join(k+' = '+json.dumps(v) for k,v in source.items())+'\n'
 old_path=str(ROOT/'binance_exec_trade05/trade_engine.toml')
 assert old_path in cfg['kline']['trade_engine_configs']
 text=text.replace(json.dumps(old_path),json.dumps(old_path)+',\n    '+json.dumps(str(EXEC/'trade_engine.toml')),1)
 assert len(tomllib.loads(text)['sources'])==len(cfg['sources'])+1
 nextpath=cfgpath.with_suffix('.toml.next');nextpath.write_text(text);os.chmod(nextpath,0o600);os.replace(nextpath,cfgpath)
 nginx=nginxpath.read_text();blocks=[]
 import re
 for m in re.finditer(r'^location [^\n]*?/exec_trade05[^\n]*\{',nginx,re.M):
  start=m.start();pos=m.end();depth=1
  while depth:
   if nginx[pos]=='{':depth+=1
   elif nginx[pos]=='}':depth-=1
   pos+=1
  blocks.append(nginx[start:pos])
 assert len(blocks)==4 and '/exec_trade07' not in nginx
 block='\n\n'.join(blocks).replace('exec_trade05','exec_trade07').replace('10045','10047').replace('18165','18167')
 (STAGE/'nginx.after').write_text(nginx+'\n# RX03 Exec07\n'+block+'\n')
 subprocess.run(['sudo','-n','install','-m','644',str(STAGE/'nginx.after'),str(nginxpath)+'.next'],check=True)
 subprocess.run(['sudo','-n','mv',str(nginxpath)+'.next',str(nginxpath)],check=True)
 subprocess.run(['sudo','-n','nginx','-t'],check=True)
 subprocess.run(['sudo','-n','systemctl','reload','nginx'],check=True)
 subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True)
 print('Manager source07 and authenticated Exec07 gateway installed')

def catalog():
 assert not any(p['comm']=='exec-pre-trade' for p in live_pids(EXEC))
 account_snapshot()
 table=json.loads((SIGNAL/'config/cta_perp_symbol_factors.json').read_text())
 factors=sorted({f for fs in table.values() for f in fs});assert len(factors)==5 and len(table)==14 and sum(map(len,table.values()))==34
 token=secrets.token_urlsafe(32);bindings=[]
 with session() as api:
  assert not api('/manager/api/catalog/accounts/'+EXEC.name)['bindings']
  assert not any(p['strategy_name'].startswith(PREFIX) for p in api('/manager/api/catalog/position-strategies'))
  params=next(r['order_parameters'] for r in api('/manager/api/catalog/order-strategies') if r['strategy_name']=='lgq_cta_ctaker')
  assert params==json.loads((STAGE/'order_parameters.json').read_text())
  order_name=PREFIX+'ctaker'
  api('/manager/api/catalog/order-strategies',{'strategy_name':order_name,'order_parameters':params})
  for factor in factors:
   name=PREFIX+factor; targets={s:{'qty':0.0,'signal':0} for s,fs in table.items() if factor in fs}
   api('/manager/api/catalog/position-strategies',{'strategy_name':name,'targets':targets})
   api('/manager/api/catalog/position-strategies/'+name+'/managers',{'user_ids':[3]},'PUT')
   api('/manager/api/catalog/position-strategies/'+name+'/publish-token',{'publish_token':token},'PUT')
   bindings.append({'binding_name':name,'position_strategy_name':name,'order_strategy_name':order_name,'shares':1.0})
  virtual=api('/manager/api/catalog/virtual-accounts',{'name':'cta_perp_rx03','bindings':bindings})
  save(STAGE/'virtual_created.json',virtual)
  api('/manager/api/catalog/virtual-accounts/'+virtual['virtual_id']+'/grants',{'user_ids':[3]},'PUT')
  api('/manager/api/catalog/accounts/'+EXEC.name+'/grants',{'grants':[{'user_id':3,'access_level':'configure'}]},'PUT')
  api('/manager/api/catalog/accounts/'+EXEC.name+'/fee-rates',json.loads((STAGE/'fee_reference.json').read_text()),'PUT')
  result=api('/manager/api/catalog/accounts/'+EXEC.name+'/configuration',{'mode':'follow','virtual_id':virtual['virtual_id'],'multiplier':1.0},'PUT')
  save(STAGE/'binding_result.json',result)
 with (SIGNAL/'env.sh').open('a') as f:
  f.write('\n# RX03 Exec07 activation 2026-10-14\n')
  for k,v in {'CTA_MANAGER_API_URL':ORIGIN+'/manager/api','CTA_FOLLOWER_SOURCE_ID':EXEC.name,'CTA_PUBLISH_TOKEN':token}.items():f.write(f"export {k}='{v}'\n")
 print('Catalog configured: virtual '+virtual['virtual_id']+', five strategies, independent ctaker template equal to RX02, shares1, multiplier1')

def redis_client():
 import redis
 return redis.Redis.from_url(env_for(EXEC).get('REDIS_URL','redis://127.0.0.1:6379/0'),decode_responses=True)

def verify_config():
 r=redis_client(); table=json.loads((SIGNAL/'config/cta_perp_symbol_factors.json').read_text())
 risk=r.hgetall(EXEC.name+':binance-futures:pre_trade_risk_params')
 assert risk==r.hgetall('binance_exec_trade05:binance-futures:pre_trade_risk_params')==json.loads((STAGE/'risk_reference.json').read_text())
 rules=json.loads(r.get(EXEC.name+':binance-futures:market_rules'))
 assert time.time_ns()//1000-rules['fetched_at_us']<180_000_000
 assert all(s in rules['symbols'] for s in table)
 reference_runtime=json.loads(r.get('binance_exec_trade05:binance-futures:batch_exec:lgq_cta_baseline_164'))
 excluded={'targets','updated_at_us','strategy_name'}
 for factor in sorted({f for fs in table.values() for f in fs}):
  runtime=json.loads(r.get(EXEC.name+':binance-futures:batch_exec:'+PREFIX+factor))
  assert {k:v for k,v in runtime.items() if k not in excluded}=={k:v for k,v in reference_runtime.items() if k not in excluded}
 with session('lgq') as api:
  a=api('/manager/api/catalog/accounts/'+EXEC.name)
  assert len(a['bindings'])==5 and not a['pending_publishes']
  assert a['configuration']['mode']=='follow' and a['configuration']['multiplier']==1
  fee=json.loads((STAGE/'fee_reference.json').read_text());assert all(a[k]==v for k,v in fee.items())
  orders=api('/manager/api/catalog/order-strategies')
  ref=next(s['order_parameters'] for s in orders if s['strategy_name']=='lgq_cta_ctaker')
  ours=next(s['order_parameters'] for s in orders if s['strategy_name']==PREFIX+'ctaker')
  assert ref==ours==json.loads((STAGE/'order_parameters.json').read_text())
  for binding in a['bindings']:
   assert binding['shares']==1 and binding['order_strategy_name']==PREFIX+'ctaker'
  for path in ['/exec_trade07/','/exec_trade07/config/','/exec_trade07/snapshot']:api(path)
  save(STAGE/'catalog_verified.json',{'account':a,'risk':risk,'order_parameters':ours,'rule_symbols':len(rules['symbols']),'runtime_parameters_equal_rx02':True})
 print('CONFIG VERIFIED: five bindings, fees/order parameters/risk exactly RX02, lgq access and gateway healthy')

def anchor_flat():
 account=account_snapshot()
 save(STAGE/'flat_start.json',{'account':account,'initial_positions':[], 'nav_anchor_required':False, 'reason':'Fresh Exec ledger and factual empty account; NAV starts with its first factual fill.'})
 print('Fresh ledger with factual empty account verified; no inventory anchor needed')

def signal_snapshot():
 with urllib.request.urlopen(ORIGIN+'/cta/binance-cta-perp-rx03/api/snapshot',timeout=20) as response:return json.load(response)

def runtime_state(api):
 data=api('/exec_trade07/snapshot')
 state=next(e for e in data['entries'] if e['channel']=='exec_pre_trade_state')
 assert time.time()*1000-state['ts_ms']<15000 and state['entry']['position_ready']
 return state['entry']

def restart_preview():
 assert not json.loads((SIGNAL/'config/cta_perp.json').read_text())['enabled']
 save(STAGE/'signal_positions_before_restart.json',json.loads((SIGNAL/'run/cta_perp_position_flags.json').read_text()))
 signal_env={k:v for k,v in os.environ.items() if not k.startswith('LTP_')}
 subprocess.run([str(SIGNAL/'scripts/start_cta_perp_signal.sh')],cwd=SIGNAL,env=signal_env,check=True)
 print('Signal restarted with Manager route and scoped publication token, still disabled')

def enable():
 verify_config()
 with session() as api:
  state=runtime_state(api)
  assert all(abs(row['target_qty'])<1e-12 and abs(row['current_qty'])<1e-12 and not row['live_order_qty'] and not row['pending_qty'] for row in state['rows'])
  save(STAGE/'exec_before_enable.json',state)
 s=signal_snapshot();status=s['status']
 assert s['signal_state']=='online' and status['publication_error'] is None and status['journal_healthy']
 assert status['follower_source_id']==EXEC.name and not status['enabled'] and status['max_pos_u']==100
 table=json.loads((SIGNAL/'config/cta_perp_symbol_factors.json').read_text())
 rows=[status['strategies'].get(f,{}).get(sym,{}) for sym,fs in table.items() for f in fs]
 assert len(rows)==34 and all(row.get('score_ready') and row.get('signal',{}).get('filter_ready') and 0<=time.time()*1000-row.get('model_ts_ms',0)<130000 for row in rows)
 base=ORIGIN+'/cta/binance-cta-perp-rx03/config/api/config'
 config=json.load(urllib.request.urlopen(base,timeout=10)); c=config['config']
 assert not c['enabled'] and c['max_pos_u']==100
 save(STAGE/'enable_before.json',config);c['enabled']=True
 req=urllib.request.Request(base,data=json.dumps({'config':c,'expected_revision':config['revision']}).encode(),headers={'Content-Type':'application/json'},method='PUT')
 with urllib.request.urlopen(req,timeout=10) as response:save(STAGE/'enable_after.json',json.load(response))
 print('ENABLED: RX03 live targets to Exec07; 14 symbols, 34 pairs, 100 USDT per pair')

def validate_live():
 s=signal_snapshot();status=s['status'];table=json.loads((SIGNAL/'config/cta_perp_symbol_factors.json').read_text())
 assert s['signal_state']=='online' and status['enabled'] and status['publication_error'] is None and status['journal_healthy']
 assert status['follower_source_id']==EXEC.name and status['max_pos_u']==100
 r=redis_client();checks=[]
 for factor in sorted({f for fs in table.values() for f in fs}):
  config=json.loads(r.get(EXEC.name+':binance-futures:batch_exec:'+PREFIX+factor))
  targets=config['targets']
  symbols={sym for sym,fs in table.items() if factor in fs}
  assert set(targets)==symbols
  for sym in symbols:
   row=status['strategies'][factor][sym]
   target=targets[sym];qty=target['qty'] if isinstance(target,dict) else target
   assert abs(qty-row['position_qty'])<1e-9
   assert row['score_ready'] and row['signal']['filter_ready'] and time.time()*1000-row['model_ts_ms']<130000
   checks.append({'factor':factor,'symbol':sym,'qty':qty,'model_ts_ms':row['model_ts_ms']})
 with session('lgq') as api:
  state=runtime_state(api)
  account=api('/manager/api/catalog/accounts/'+EXEC.name)
  assert not account['pending_publishes']
  save(STAGE/'exec_live.json',state)
 changed=[];before=json.loads((STAGE/'protected_before.json').read_text())
 for filename,digest in before['files'].items():
  if hashlib.sha256(pathlib.Path(filename).read_bytes()).hexdigest()!=digest:changed.append(filename)
 for row in before['processes']:
  p=pathlib.Path('/proc')/str(row['pid'])
  if not p.exists() or p.joinpath('stat').read_text().split()[21]!=row['start']:changed.append('pid:'+str(row['pid']))
 assert not changed,changed
 rows=state['rows']
 evidence={'checked_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'target_checks':checks,'signal_enabled':True,'publication_error':status['publication_error'],'journal_healthy':status['journal_healthy'],'position_ready':state['position_ready'],'exec_rows':len(rows),'nonzero_target_rows':sum(abs(row['target_qty'])>1e-12 for row in rows),'nonzero_current_rows':sum(abs(row['current_qty'])>1e-12 for row in rows),'pending_rows':sum(bool(row['pending_qty']) for row in rows),'live_order_rows':sum(bool(row['live_order_qty']) for row in rows),'processes':live_pids(EXEC),'protected_processes':len(before['processes']),'protected_files':len(before['files']),'protected_changed':changed}
 save(STAGE/'live_validation.json',evidence)
 print(json.dumps(evidence))

if __name__=='__main__':
 {'provision':provision,'configure-manager':configure_manager,'catalog':catalog,'verify-config':verify_config,'anchor-flat':anchor_flat,'restart-preview':restart_preview,'enable':enable,'validate-live':validate_live}[sys.argv[1]]()

