#!/usr/bin/env python3
"""Restore one public account with two execution Viz panes, without trading."""
import argparse,ast,copy,hashlib,json,os,re,shutil,subprocess,tarfile,time,tomllib,urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('phase',choices=['prepare','activate','verify']);p.add_argument('--stage',required=True);a=p.parse_args()
manager=Path.home()/'crypto_cta_manager';stage=Path(a.stage).resolve();assert stage.parent==manager/'deploy-staging';audit=manager/'maintenance-backups'/stage.name;manifest=json.loads((stage/'manifest.json').read_text());cfg=manager/'config/cta-manager.toml';account_id='cta_cross_spread_rx01';account_alias='okex_binace_cross_1min';members=['okex_exec_trade01','binance_exec_spread01'];aliases=['okex_exec_cross01','binance_exec_cross01']
h=Path.home()/'binance_exec_trade06/run/rx01_activation_ops.py';tree=ast.parse(h.read_text());ns={'MANAGER':manager,'ORIGIN':'http://127.0.0.1:4191'};nodes=[n for n in tree.body if isinstance(n,(ast.Import,ast.ImportFrom)) or isinstance(n,ast.FunctionDef) and n.name in ['db_env','sql','session']];exec(compile(ast.Module(body=nodes,type_ignores=[]),str(h),'exec'),ns);sql=ns['sql'];session=ns['session']
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,s):
 with Path(p).open('x') as f:f.write(s);f.flush();os.fsync(f.fileno())
 Path(p).chmod(0o600)
def receipt(name,v):write(audit/name,json.dumps(v,indent=2)+'\n')
def procs():
 names={'trade_engine','exec-pre-trade','pre_trade','trade_signal','account_monitor','binance_account','okex_account_mo','persist_manager','viz_server','cta-factor-spre','cta_signal'};out={}
 for proc in Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:
   name=(proc/'comm').read_text().strip()
   if name in names:out[proc.name]={'comm':name,'cwd':os.readlink(proc/'cwd'),'start_ticks':(proc/'stat').read_text().split(') ',1)[1].split()[19]}
  except OSError:pass
 return out
def invariants():
 return {t:sql(f"SELECT count(*) FROM {t} WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01');") for t in ['cta_position_snapshots','cta_strategy_position_snapshots','cta_strategy_allocation_snapshots','cta_account_strategy_bindings']}
def check_delta():
 old=tomllib.loads((audit/'cta-manager.toml.before').read_text());new=tomllib.loads((audit/'cta-manager.toml.candidate').read_text())
 assert account_id not in old.get('accounts',{})
 assert new['accounts'].pop(account_id)=={'name':'cross01','source_ids':members,'created_at_us':1793625556000000}
 if not old.get('accounts'):old.pop('accounts',None)
 if not new.get('accounts'):new.pop('accounts',None)
 assert old==new,'unexpected config changes'
def check_alias():
 assert sql("SELECT display_alias FROM cta_managed_account_aliases WHERE account_id='cta_cross_spread_rx01';")==account_alias
 assert sql("SELECT count(*) FROM cta_order_sources WHERE source_id='cta_cross_spread_rx01';")=='0'
def check_public_account(d):
 selected=[x for x in d['accounts'] if x['source_id']==account_id]
 assert len(selected)==1 and selected[0]['account']==account_alias and selected[0]['display_alias']==account_alias
 assert [x['source_id'] for x in selected[0]['execution_sources']]==members
 assert [x['account'] for x in selected[0]['execution_sources']]==aliases
 assert not any(x['source_id'] in members for x in d['accounts'])
 return selected[0]
def install_config(source):
 tmp=cfg.with_name('cta-manager.toml.cross01-next');assert not tmp.exists();shutil.copy2(source,tmp);tmp.chmod(0o600);assert sha(source)==sha(tmp);os.replace(tmp,cfg)
if a.phase=='prepare':
 assert not audit.exists();audit.mkdir(mode=0o700);assert sha(manager/'bin/cta_web')==manifest['api_sha256'];assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256']
 raw=cfg.read_text();write(audit/'cta-manager.toml.before',raw);write(audit/'webroot.before',os.readlink(manager/'webroot'))
 check_alias()
 assert account_id not in tomllib.loads(raw).get('accounts',{})
 candidate=raw.rstrip()+'\n\n[accounts.cta_cross_spread_rx01]\nname = "cross01"\nsource_ids = ["okex_exec_trade01", "binance_exec_spread01"]\ncreated_at_us = 1793625556000000\n'
 write(audit/'cta-manager.toml.candidate',candidate);check_delta()
 assert all(v=='0' for v in invariants().values()),'new account has unexpected anchors or bindings'
 receipt('processes.before.json',procs());receipt('invariants.before.json',invariants())
 settings=tomllib.loads(raw);paths=[]
 for source in settings['sources']:
  if source.get('env_path'):paths.extend([Path(source['env_path']),Path(source['env_path']).parent/'trade_engine.toml'])
 receipt('protected-files.json',{str(p):sha(p) for p in set(paths) if p.is_file()})
 write(audit/'aliases.before.json',sql("SELECT jsonb_object_agg(source_id,display_alias) FROM cta_order_sources;")+'\n')
 pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip();env=dict(os.environ);env.update(x.decode().split('=',1) for x in Path('/proc/'+pid+'/environ').read_bytes().split(b'\0') if b'=' in x)
 checked=subprocess.run([str(manager/'bin/cta_web'),'--config',str(audit/'cta-manager.toml.candidate'),'--check-allocations'],env=env,text=True,capture_output=True);write(audit/'allocation-preflight.log',checked.stdout+checked.stderr);assert checked.returncode==0,'allocation validation failed'
 print('Prepared one public account with two execution sources; existing aliases, source identities and execution files preserved.',flush=True)
elif a.phase=='activate':
 check_delta();assert sha(cfg)==sha(audit/'cta-manager.toml.before');assert sha(manager/'bin/cta_web')==manifest['api_sha256'];assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256']
 release=manager/'web-releases'/stage.name;assert not release.exists();release.mkdir()
 with tarfile.open(stage/'frontend.tar.gz') as tar:
  for x in tar.getmembers():assert x.isfile() or x.isdir();assert '..' not in Path(x.name).parts and not Path(x.name).is_absolute()
  tar.extractall(release,filter='data')
 assert (release/'manager/index.html').is_file(),'frontend must contain manager/index.html'
 try:
  install_config(audit/'cta-manager.toml.candidate');nxt=manager/'webroot.cross01-next';nxt.symlink_to(release.relative_to(manager));os.replace(nxt,manager/'webroot')
  subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True);print('Only Manager restarted; waiting for historical cache initialization.',flush=True)
  opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
  for attempt in range(300):
   try:
    with opener.open('http://127.0.0.1:4191/manager/accounts/',timeout=5) as r:assert r.status==200
    with opener.open('http://127.0.0.1:4191/manager/api/auth/status',timeout=5) as r:assert r.status==200
    with session('lgq') as api:
     d=api('/manager/api/dashboard');check_public_account(d);assert api('/manager/api/health')['status']=='ok'
    break
   except Exception:
    if attempt==299:raise
    time.sleep(2)
  print('Manager healthy; one public account exposes two execution Viz panes.',flush=True)
 except Exception:
  if sha(cfg)==sha(audit/'cta-manager.toml.candidate'):install_config(audit/'cta-manager.toml.before')
  nxt=manager/'webroot.cross01-rollback';nxt.symlink_to((audit/'webroot.before').read_text());os.replace(nxt,manager/'webroot');subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True)
  raise RuntimeError('Activation failed; prior Manager configuration/frontend restored') from None
elif a.phase=='verify':
 check_delta();check_alias()
 assert sha(cfg)==sha(audit/'cta-manager.toml.candidate') and sha(manager/'bin/cta_web')==manifest['api_sha256']
 assert invariants()==json.loads((audit/'invariants.before.json').read_text())
 assert sql("SELECT jsonb_object_agg(source_id,display_alias) FROM cta_order_sources;")==(audit/'aliases.before.json').read_text().strip()
 assert procs()==json.loads((audit/'processes.before.json').read_text())
 for path,digest in json.loads((audit/'protected-files.json').read_text()).items():assert sha(path)==digest,path
 with session('lgq') as api:
  d=api('/manager/api/dashboard');account=check_public_account(d)
  report=next(x for x in d['report']['sources'] if x['source_id']==account_id)
  assert not any(x['source_id'] in members for x in d['report']['sources'])
  assert report['order_event_count']==0 and report['initial_position_count']==0
  assert report['nav_change_after_fee_quote']==0
  for id in members:
   funding=api('/manager/api/catalog/accounts/'+id+'/funding');assert not funding['enabled'] and funding['summary']['record_count']==0
  for path in ['/account-overview','/nav/exchange']:
   rows=api('/manager/api'+path)['accounts'];selected=[r for r in rows if r['source_id']==account_id]
   assert len(selected)==1 and selected[0]['account']==account_alias
   assert not any(r['source_id'] in members for r in rows)
  end=int(time.time()*1000)-60000
  t=api('/manager/api/timeline?sourceIds='+account_id+'&startMs='+str(end-86400000)+'&endMs='+str(end)+'&includeTheoretical=false')
  assert set(t['report']['selected_source_ids'])==set(members)
  assert t['report']['summary']['nav_change_after_fee_quote']==0 and t['estimated_funding']['income_quote']==0
  for prefix in ['/exec_okex01/','/exec_binance_spread01/']:assert api(prefix)['status']==200
 with session('prc') as api:
  d=api('/manager/api/dashboard');assert not any(x['source_id'] in members+[account_id] for x in d['accounts'])
  for prefix in ['/exec_okex01/','/exec_binance_spread01/']:
   try:api(prefix)
   except RuntimeError as e:assert 'HTTP 403' in str(e)
   else:raise AssertionError('hidden Viz allowed')
 pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
 mask=next(l.split(':',1)[1].strip() for l in Path('/proc/'+pid+'/status').read_text().splitlines() if l.startswith('Cpus_allowed_list:'));assert mask=='24'
 result={'public_account_id':account_id,'public_alias':account_alias,'public_accounts':1,'execution_sources':dict(zip(members,aliases)),'pair_viz':'/manager/exec/?group=cross01','combined_pnl':0,'funding':'estimated','orders':0,'initial_positions':0,'protected_processes_unchanged':len(procs()),'execution_configs_unchanged':True,'aliases_unchanged':True,'manager_pid':int(pid),'manager_cpu':mask,'trading_started':False}
 receipt('verification.json',result);shutil.copy2(stage/'manifest.json',audit/'manifest.json');print(json.dumps(result),flush=True)
