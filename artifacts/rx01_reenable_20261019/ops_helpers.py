"""RX01 activation steps. Credentials remain in remote memory/files only."""
import contextlib, datetime, hashlib, json, os, pathlib, secrets, shutil, subprocess, sys, time, tomllib
import urllib.request, urllib.error, urllib.parse

ROOT=pathlib.Path.home()
MANAGER=ROOT/'crypto_cta_manager'
EXEC=ROOT/'binance_exec_trade06'
SIGNAL=ROOT/'binance-cta-spread-rx01'
OLD=ROOT/'binance-cta-rx01'
STAGE=EXEC/'run/activation_20261013'
ORIGIN='http://127.0.0.1:4191'
PREFIX='binance_cta_spread_rx01_'
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

def snapshot():
 sys.path.insert(0,str(OLD/'scripts'))
 from neutralize_rapidx_binance import Client
 values=env_for(EXEC); old=env_for(OLD); rx02=env_for(ROOT/'binance_exec_trade05')
 assert all(values[k]==old[k] for k in ['LTP_API_KEY','LTP_API_SECRET','LTP_PORTFOLIO_ID'])
 assert values['LTP_PORTFOLIO_ID']!=rx02['LTP_PORTFOLIO_ID']
 os.environ.update(values)
 ips=tomllib.loads((EXEC/'trade_engine.toml').read_text())['local_ips']
 assert ips==['172.31.35.228']
 c=Client(ips[0],execute=False)
 c.empty_orders()
 cash,perp,marks=c.snapshot()
 rows=[]; targets={}
 for coin in sorted(set(cash)|set(perp)):
  if coin=='USDT': continue
  a,b=cash.get(coin,0),perp.get(coin,0)
  if not a and not b: continue
  price=marks[coin]; assert price>0
  rows.append({'coin':coin,'cash_qty':float(a),'perp_qty':float(b),'mark':float(price),'net_usdt':float((a+b)*price)})
  if a: targets[coin]={'qty':float(a),'signal':0}
  if b: targets[coin+'USDT']={'qty':float(b),'signal':0}
 d={'ts_us':time.time_ns()//1000,'rows':rows,'targets':targets,'open_orders':0,'credentials_verified':True}
 save(STAGE/'inventory.json',d)
 print(json.dumps(d))

def prepare():
 assert not STAGE.exists(), 'staging already exists'
 STAGE.mkdir(mode=0o700)
 assert not live_pids(EXEC)
 for src,name in [(MANAGER/'config/cta-manager.toml','manager.toml.before'),(SIGNAL/'env.sh','signal.env.before'),(EXEC/'env.sh','exec.env.before'),(SIGNAL/'config/cta_spread.json','signal.json.before'),(SIGNAL/'run/cta_spread_position_flags.json','positions.before'),(pathlib.Path('/etc/nginx/snippets/crypto_cta_manager.conf'),'nginx.before')]:
  shutil.copy2(src,STAGE/name)
  os.chmod(STAGE/name,0o600)
 snapshot()
 with session() as api:
  fee=api('/manager/api/catalog/accounts/binance_exec_trade05')
  save(STAGE/'fee_reference.json',{k:fee[k] for k in ['maker_fee_rate','taker_fee_rate','theoretical_twap_fee_rate']})
  orders=api('/manager/api/catalog/order-strategies')
  save(STAGE/'order_templates.json',orders)
  print('order_template',json.dumps([r for r in orders if r['strategy_name']=='lgq_cta_ctaker']))
 print('PREPARED',str(STAGE))

def configure():
 assert not live_pids(EXEC)
 assert (STAGE/'manager.toml.before').read_bytes()==(MANAGER/'config/cta-manager.toml').read_bytes(), 'Manager config changed since backup'
 assert (STAGE/'nginx.before').read_bytes()==pathlib.Path('/etc/nginx/snippets/crypto_cta_manager.conf').read_bytes(), 'Nginx config changed since backup'
 for p,settings in [(EXEC,{'RAPIDX_BINANCE_CASH_BUSINESS_TYPE':'MARGIN'}),(SIGNAL,{
  'CTA_MANAGER_API_URL':ORIGIN+'/manager/api','CTA_VIRTUAL_ACCOUNT_NAME':'cta_spread_rx01',
  'CTA_FOLLOWER_SOURCE_ID':'binance_exec_trade06','CTA_POSITION_STRATEGY_PREFIX':PREFIX})]:
  with (p/'env.sh').open('a') as f:
   f.write('\n# RX01 Exec06 activation 2026-10-13\n')
   for k,v in settings.items(): f.write(f"export {k}='{v}'\n")
 cfg=json.loads((SIGNAL/'config/cta_spread.json').read_text())
 assert not cfg['enabled'] and cfg['max_pos_u']==100
 cfg['execution']={'factor_exit_quantile_long':0.7,'factor_exit_quantile_short':0.3}
 save(SIGNAL/'config/cta_spread.json',cfg)
 values=env_for(EXEC)
 fee=json.loads((STAGE/'fee_reference.json').read_text())
 source={'id':'binance_exec_trade06','account':'trade06','alias':'lgq_cta_RX01',
  'venue':'binance-futures','rocksdb_path':str(EXEC/'data/persist_manager'),
  'enabled':True,'gateway_prefix':'/exec_trade06','exec_config_url':'http://127.0.0.1:18166/',
  'exec_viz_url':'http://127.0.0.1:10046/','maker_fee_rate':fee['maker_fee_rate'],
  'taker_fee_rate':fee['taker_fee_rate'],'ipc_namespace':'binance_exec_trade06',
  'account_ipc_service':'account_pubs/rapidx_binance_'+values['LTP_PORTFOLIO_ID']+'_pm'}
 text=(STAGE/'manager.toml.before').read_text()
 assert not any(s['id']==source['id'] for s in tomllib.loads(text)['sources'])
 text+='\n# RX01 mixed cash/perpetual Exec\n[[sources]]\n'+'\n'.join(k+' = '+json.dumps(v) for k,v in source.items())+'\n'
 old_config=str(ROOT/'binance_exec_trade05/trade_engine.toml')
 if old_config in tomllib.loads(text)['kline']['trade_engine_configs']:
  text=text.replace(json.dumps(old_config),json.dumps(old_config)+',\n    '+json.dumps(str(EXEC/'trade_engine.toml')),1)
 assert len(tomllib.loads(text)['sources'])==6
 next_path=STAGE/'manager.toml.after'; next_path.write_text(text); os.chmod(next_path,0o600)
 shutil.copy2(next_path,MANAGER/'config/cta-manager.toml.next'); os.replace(MANAGER/'config/cta-manager.toml.next',MANAGER/'config/cta-manager.toml')
 nginx=(STAGE/'nginx.before').read_text()
 start=nginx.index('location /exec_trade05/config/')
 block=nginx[start:]
 assert block.count('location ')==4 and '/exec_trade06/' not in nginx
 block=block.replace('exec_trade05','exec_trade06').replace('18165','18166').replace('10045','10046')
 (STAGE/'nginx.after').write_text(nginx+'\n# RX01 Exec06\n'+block)
 subprocess.run(['sudo','-n','install','-m','644',str(STAGE/'nginx.after'),'/etc/nginx/snippets/crypto_cta_manager.conf.next'],check=True)
 subprocess.run(['sudo','-n','mv','/etc/nginx/snippets/crypto_cta_manager.conf.next','/etc/nginx/snippets/crypto_cta_manager.conf'],check=True)
 subprocess.run(['sudo','-n','nginx','-t'],check=True)
 subprocess.run(['sudo','-n','systemctl','reload','nginx'],check=True)
 subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True)
 print('CONFIGURED: RX01 exits 70/30; Exec06 MARGIN; Manager source and authenticated gateway installed')

def catalog():
 assert not any(x['comm']=='exec-pre-trade' for x in live_pids(EXEC))
 snapshot()
 inv=json.loads((STAGE/'inventory.json').read_text())
 table=json.loads((SIGNAL/'config/cta_spread_symbol_factors.json').read_text())
 factors=sorted({f for rows in table.values() for f in rows})
 assert len(table)==16 and len(factors)==5 and sum(map(len,table.values()))==46
 token=secrets.token_urlsafe(32)
 bindings=[]
 with session() as api:
  assert not api('/manager/api/catalog/accounts/binance_exec_trade06')['bindings']
  existing=api('/manager/api/catalog/position-strategies')
  assert not any(p['strategy_name'].startswith(PREFIX) for p in existing), 'strategy names already exist'
  params=next(p['order_parameters'] for p in api('/manager/api/catalog/order-strategies') if p['strategy_name']=='lgq_cta_ctaker')
  order_name=PREFIX+'ctaker'
  api('/manager/api/catalog/order-strategies',{'strategy_name':order_name,'order_parameters':params})
  for factor in factors+['inventory']:
   name=PREFIX+factor
   if factor=='inventory': targets=inv['targets']
   else:
    symbols=[s for s,fs in table.items() if factor in fs]
    targets={key:{'qty':0.0,'signal':0} for s in symbols for key in [s,s.removesuffix('USDT')]}
   api('/manager/api/catalog/position-strategies',{'strategy_name':name,'targets':targets})
   api('/manager/api/catalog/position-strategies/'+name+'/managers',{'user_ids':[3]},'PUT')
   api('/manager/api/catalog/position-strategies/'+name+'/publish-token',{'publish_token':token},'PUT')
   bindings.append({'binding_name':name,'position_strategy_name':name,'order_strategy_name':order_name,'shares':1.0})
  virtual=api('/manager/api/catalog/virtual-accounts',{'name':'cta_spread_rx01','bindings':bindings})
  save(STAGE/'virtual_created.json',virtual)
  virtual_id=virtual['virtual_id']
  api('/manager/api/catalog/virtual-accounts/'+virtual_id+'/grants',{'user_ids':[3]},'PUT')
  api('/manager/api/catalog/accounts/binance_exec_trade06/grants',{'grants':[{'user_id':3,'access_level':'configure'}]},'PUT')
  api('/manager/api/catalog/accounts/binance_exec_trade06/fee-rates',json.loads((STAGE/'fee_reference.json').read_text()),'PUT')
  result=api('/manager/api/catalog/accounts/binance_exec_trade06/configuration',{'mode':'follow','virtual_id':virtual_id,'multiplier':1.0},'PUT')
  save(STAGE/'binding_result.json',result)
 with (SIGNAL/'env.sh').open('a') as f: f.write("export CTA_PUBLISH_TOKEN='"+token+"'\n")
 print('CATALOG: six strategies, one virtual, one Exec follower; lgq configure granted')

def anchor():
 inv=json.loads((STAGE/'inventory.json').read_text())
 assert time.time_ns()//1000-inv['ts_us']<900_000_000
 assert not any(x['comm']=='exec-pre-trade' for x in live_pids(EXEC))
 pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
 values=dict(x.decode().split('=',1) for x in pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
 args=['--config',str(MANAGER/'config/cta-manager.toml'),'--source','binance_exec_trade06','--snapshot-ts-us',str(inv['ts_us']),
       '--note','RX01 takeover: signed RapidX balance minus debt and perpetual quantity; contemporaneous index marks; historical fills remain in old environment']
 for binary,strategy in [('nav_snapshot',False),('nav_strategy_snapshot',True)]:
  positions=[]
  for row in inv['rows']:
   for symbol,venue,qty in [(row['coin'],0,row['cash_qty']),(row['coin']+'USDT',1,row['perp_qty'])]:
    if qty:
     value=f"{symbol}:{venue}:{qty}:{row['mark']}"
     positions+=['--position',(INVENTORY+':'+value) if strategy else value]
  if strategy:
   subprocess.run([str(MANAGER/'bin'/binary),*args,*positions,'--dry-run'],env=values,stdout=subprocess.DEVNULL,check=True)
  result=subprocess.run([str(MANAGER/'bin'/binary),*args,*positions],env=values,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
  if result.returncode: raise RuntimeError('snapshot writer failed: '+result.stderr[-800:])
  save(STAGE/(binary+'.json'),json.loads(result.stdout))
 print('ANCHORS: immutable cash/perp account and inventory-strategy opening snapshots stored')

def finish_catalog():
 assert not any(x['comm']=='exec-pre-trade' for x in live_pids(EXEC))
 virtual=json.loads((STAGE/'virtual_created.json').read_text())
 token=secrets.token_urlsafe(32)
 token_path=STAGE/'publish_token'
 fd=os.open(token_path,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'w') as f: f.write(token)
 with session() as api:
  account=api('/manager/api/catalog/accounts/binance_exec_trade06')
  fee=json.loads((STAGE/'fee_reference.json').read_text())
  assert all(account[k]==v for k,v in fee.items()), 'fee update not persisted'
  assert account['configuration']['mode']=='independent' and not account['bindings']
  for binding in virtual['bindings']:
   api('/manager/api/catalog/position-strategies/'+binding['position_strategy_name']+'/publish-token',{'publish_token':token},'PUT')
  with (SIGNAL/'env.sh').open('a') as f: f.write("export CTA_PUBLISH_TOKEN='"+token+"'\n")
  result=api('/manager/api/catalog/accounts/binance_exec_trade06/configuration',{'mode':'follow','virtual_id':virtual['virtual_id'],'multiplier':1.0},'PUT')
  save(STAGE/'binding_result.json',result)
 print('CATALOG_FINISHED')

def verify_config():
 import redis
 values=env_for(EXEC)
 client=redis.Redis.from_url(values.get('REDIS_URL','redis://127.0.0.1:6379/0'),decode_responses=True)
 keys=sorted(client.scan_iter(match='binance_exec_trade06:*'))
 print('redis_keys',json.dumps(keys))
 for key in keys:
  if 'market_rules' in key:
   data=json.loads(client.get(key)); print('rules',key,'keys',list(data),'symbols',len(data.get('symbols',data.get('rules',{}))))
 with session('lgq') as api:
  account=api('/manager/api/catalog/accounts/binance_exec_trade06')
  virtual=[v for v in api('/manager/api/catalog/virtual-accounts') if v['name']=='cta_spread_rx01']
  access=[v for v in api('/manager/api/catalog/position-strategies-access') if v['strategy_name'].startswith(PREFIX)]
  evidence={'account':account,'virtual':virtual,'access':access}
  save(STAGE/'lgq_config_validation.json',evidence)
  print('lgq_account',json.dumps(account))
  print('lgq_virtual',json.dumps(virtual))
  assert len(account['bindings'])==6 and not account['pending_publishes']
  assert len(virtual)==1 and virtual[0]['can_configure']
  assert len(access)==6
  for path in ['/exec_trade06/','/exec_trade06/config/','/exec_trade06/snapshot']:
   result=api(path); print('gateway',path,'ok')
 print('CONFIG_VERIFIED')

def restart_preview():
 assert not json.loads((SIGNAL/'config/cta_spread.json').read_text())['enabled']
 subprocess.run([str(SIGNAL/'scripts/stop_cta_spread_signal.sh')],cwd=SIGNAL,check=True)
 assert not any(p['comm']=='cta_spread_sign' for p in live_pids(SIGNAL))
 state=json.loads((SIGNAL/'run/cta_spread_position_flags.json').read_text())
 assert all(not row.get(k,0) for symbols in state.values() for row in symbols.values() for k in ['qty','cash_qty','perpetual_qty'])
 save(STAGE/'final_zero_preview_positions.json',state)
 save(SIGNAL/'run/cta_spread_position_flags.json',{})
 subprocess.run([str(SIGNAL/'scripts/start_cta_spread_signal.sh')],cwd=SIGNAL,check=True)
 print('PREVIEW_RESTARTED: disabled, current factors only, Manager route and scoped token loaded')

def runtime():
 d=json.load(urllib.request.urlopen('http://127.0.0.1:10046/snapshot',timeout=10))
 save(STAGE/'exec_snapshot.json',d)
 state=next(e for e in d['entries'] if e['channel']=='exec_pre_trade_state')
 assert time.time()*1000-state['ts_ms']<15000
 assert state['entry']['position_ready']
 rows=state['entry']['rows']; inv=json.loads((STAGE/'inventory.json').read_text())
 own=[r for r in rows if r['strategy_name']==INVENTORY]
 assert {r['symbol'] for r in own}==set(inv['targets'])
 summary=[]
 for row in own:
  target=inv['targets'][row['symbol']]['qty']
  assert abs(row['target_qty']-target)<1e-8
  assert abs(row['delta_usdt'])<1.0
  assert row['live_order_qty']==0 and row['pending_qty']==0
  summary.append({k:row[k] for k in ['symbol','target_qty','current_qty','account_position_qty','delta_usdt','completion_reason']})
 unexpected=[r for r in rows if r['strategy_name']!=INVENTORY and (abs(r['current_usdt'])>0.01 or abs(r['target_usdt'])>0.01)]
 assert not unexpected, 'non-inventory exposure before activation'
 print('inventory_runtime',json.dumps(summary))
 print('EXEC_READY',len(rows),'rows; inventory accounted for; no pending or live orders')

def enable():
 runtime()
 base=ORIGIN+'/cta/binance-cta-spread-rx01'
 s=json.load(urllib.request.urlopen(base+'/api/snapshot'))
 assert s['signal_state']=='online' and s['status']['publication_error'] is None
 assert s['status']['follower_source_id']=='binance_exec_trade06'
 rows=[s['status']['strategies'].get(f,{}).get(sym,{}) for sym,fs in s['status']['symbol_factors'].items() for f in fs]
 assert len(rows)==46 and all(r.get('score_ready') and r.get('signal',{}).get('filter_ready') and 0<=time.time()*1000-r.get('model_ts_ms',0)<130000 for r in rows)
 config=json.load(urllib.request.urlopen(base+'/config/api/config'))
 c=config['config']; assert not c['enabled'] and c['max_pos_u']==100
 assert c['entry']=={'factor_long_quantile':0.9,'factor_short_quantile':0.1,'spread_lower_quantile':0.3,'spread_upper_quantile':0.7}
 assert c['execution']=={'factor_exit_quantile_long':0.7,'factor_exit_quantile_short':0.3}
 save(STAGE/'enable_before.json',config)
 c['enabled']=True
 req=urllib.request.Request(base+'/config/api/config',data=json.dumps({'config':c,'expected_revision':config['revision']}).encode(),headers={'Content-Type':'application/json'},method='PUT')
 with urllib.request.urlopen(req,timeout=10) as r: save(STAGE/'enable_after.json',json.load(r))
 print('SIGNAL_ENABLED: RX01 16 symbols / 46 combinations, 100 USDT each; real orders may now be submitted')

def validate_live():
 import redis
 signal=json.load(urllib.request.urlopen(ORIGIN+'/cta/binance-cta-spread-rx01/api/snapshot',timeout=10))
 status=signal['status']
 assert signal['signal_state']=='online' and status['enabled'] and status['publication_error'] is None
 assert status['journal_healthy']
 table=status['symbol_factors']; factors=sorted({f for fs in table.values() for f in fs})
 assert len(factors)==5 and sum(map(len,table.values()))==46
 r=redis.Redis.from_url(env_for(EXEC).get('REDIS_URL','redis://127.0.0.1:6379/0'),decode_responses=True)
 checks=[]
 for f in factors:
  for venue,field in [('binance-margin','cash_qty'),('binance-futures','perpetual_qty')]:
   target=json.loads(r.get(f'binance_exec_trade06:{venue}:batch_exec:{PREFIX}{f}'))['targets']
   for sym,fs in table.items():
    if f not in fs: continue
    key=sym.removesuffix('USDT') if venue=='binance-margin' else sym
    row=status['strategies'][f][sym]
    assert abs(target[key]['qty']-row[field])<1e-8, (f,key,'signal/Exec target mismatch')
    assert row['score_ready'] and row['signal']['filter_ready']
    assert time.time()*1000-row['model_ts_ms']<130000
   checks.append({'factor':f,'venue':venue,'targets':len(target)})
 d=json.load(urllib.request.urlopen('http://127.0.0.1:10046/snapshot',timeout=10))
 e=next(e for e in d['entries'] if e['channel']=='exec_pre_trade_state')
 assert e['entry']['position_ready'] and time.time()*1000-e['ts_ms']<15000
 rows=e['entry']['rows']
 with session('lgq') as api:
  account=api('/manager/api/catalog/accounts/binance_exec_trade06')
  assert len(account['bindings'])==6 and not account['pending_publishes']
  virtual=next(v for v in api('/manager/api/catalog/virtual-accounts') if v['virtual_id']=='virtual03')
  assert virtual['can_configure'] and len(virtual['followers'])==1
  access=api('/manager/api/catalog/accounts/binance_exec_trade06/grants')
  assert any(g['username']=='lgq' and g['access_level']=='configure' for g in access)
 rx02=json.load(urllib.request.urlopen(ORIGIN+'/cta/binance-cta-perp-rx02/api/snapshot',timeout=10))
 assert rx02['signal_state']=='online' and rx02['status']['enabled'] and rx02['status']['publication_error'] is None
 evidence={'validated_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'signal':signal,'exec_snapshot':d,'account':account,
  'virtual':virtual,'grants':access,'target_checks':checks,'rx02_enabled':rx02['status']['enabled'],'rx02_config':rx02['config'],
  'processes':{p.name:live_pids(p) for p in [EXEC,SIGNAL,ROOT/'binance_exec_trade05',ROOT/'binance-cta-perp-rx02']}}
 for ps in evidence['processes'].values():
  for p in ps:
   proc=pathlib.Path('/proc')/str(p['pid'])
   p['cpu_mask']=next(line.split(':')[1].strip() for line in proc.joinpath('status').read_text().splitlines() if line.startswith('Cpus_allowed_list:'))
 save(STAGE/'live_validation.json',evidence)
 save(EXEC/'run/rx01_execution_plan.json',{'state':'activated','signal_env':SIGNAL.name,'exec_env':EXEC.name,'virtual_id':'virtual03','virtual_name':'cta_spread_rx01','max_pos_u':100,'factor_count':5,'pair_count':46,'inventory_strategy':INVENTORY,'evidence':str(STAGE/'live_validation.json'),'updated_at':evidence['validated_at']})
 hashes={name:hashlib.sha256((EXEC/name).read_bytes()).hexdigest() for name in ['exec-pre-trade','trade_engine','rapidx_account_monitor','persist_manager','viz_server','trade_signal']}
 save(EXEC/'release.json',{'branch':'arbmm','base_commit':'ba3da22d','exec_pre_trade_commit':'c484f85d','trade_engine_commit':'480bc990','built_on':'local_active_worktree','state':'activated','binaries':hashes,'validated_at':evidence['validated_at']})
 (EXEC/'release.sha256').write_text(''.join(h+'  '+name+'\n' for name,h in hashes.items()))
 print(json.dumps({'enabled':True,'ready_pairs':46,'markets':['cash','perpetual'],'virtual_id':'virtual03','follower':'binance_exec_trade06','lgq_configure':True,
  'pending_publishes':len(account['pending_publishes']),'nonzero_factor_targets':[{'factor':f,'symbol':sym,'cash_qty':row['cash_qty'],'perpetual_qty':row['perpetual_qty']} for f,ss in status['strategies'].items() for sym,row in ss.items() if row.get('cash_qty',0) or row.get('perpetual_qty',0)],
  'live_order_rows':sum(bool(r['live_order_qty']) for r in rows),'pending_rows':sum(bool(r['pending_qty']) for r in rows),'processes':evidence['processes']}))

if __name__=='__main__':
 mode=sys.argv[1]
 if mode=='prepare': prepare()
 elif mode=='snapshot': snapshot()
 elif mode=='configure': configure()
 elif mode=='catalog': catalog()
 elif mode=='anchor': anchor()
 elif mode=='finish-catalog': finish_catalog()
 elif mode=='verify-config': verify_config()
 elif mode=='restart-preview': restart_preview()
 elif mode=='runtime': runtime()
 elif mode=='enable': enable()
 elif mode=='validate-live': validate_live()
