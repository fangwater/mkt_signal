#!/usr/bin/env python3
"""Publish one unified execution dashboard; frontend only, with no process restart."""
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
def pid():return subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
def check_unchanged():
 assert sha(cfg)==sha(audit/'cta-manager.toml.before')
 assert sha(manager/'bin/cta_web')==manifest['api_sha256']
 assert pid()==(audit/'manager-pid.before').read_text().strip()
 assert procs()==json.loads((audit/'processes.before.json').read_text())
 assert invariants()==json.loads((audit/'invariants.before.json').read_text())
 for path,digest in json.loads((audit/'protected-files.json').read_text()).items():assert sha(path)==digest,path
 check_alias()
def static_check():
 opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 for path,digest in manifest['frontend_files'].items():
  assert sha(manager/'webroot'/path)==digest
  with opener.open('http://127.0.0.1:4191/'+path,timeout=15) as response:
   assert response.status==200 and hashlib.sha256(response.read()).hexdigest()==digest,path
if a.phase=='prepare':
 assert not audit.exists();audit.mkdir(mode=0o700)
 assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256'] and sha(manager/'bin/cta_web')==manifest['api_sha256']
 with tarfile.open(stage/'frontend.tar.gz') as tar:
  assert 'manager/index.html' in tar.getnames()
  assert set(tar.getnames())==set(manifest['frontend_files'])
  for item in tar.getmembers():assert item.isfile() and '..' not in Path(item.name).parts and not Path(item.name).is_absolute()
 shutil.copy2(cfg,audit/'cta-manager.toml.before');write(audit/'webroot.before',os.readlink(manager/'webroot'));write(audit/'manager-pid.before',pid()+'\n')
 receipt('processes.before.json',procs());receipt('invariants.before.json',invariants())
 paths=[]
 for source in tomllib.loads(cfg.read_text())['sources']:
  if source.get('env_path'):paths.extend([Path(source['env_path']),Path(source['env_path']).parent/'trade_engine.toml'])
 receipt('protected-files.json',{str(p):sha(p) for p in set(paths) if p.is_file()})
 with session('lgq') as api:check_public_account(api('/manager/api/dashboard'))
 check_unchanged();print('Prepared frontend-only publication; single account and execution invariants checked.',flush=True)
elif a.phase=='activate':
 check_unchanged();assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256']
 release=manager/'web-releases'/stage.name;assert not release.exists();release.mkdir(mode=0o755)
 with tarfile.open(stage/'frontend.tar.gz') as tar:
  for item in tar.getmembers():assert item.isfile() and item.name in manifest['frontend_files']
  tar.extractall(release,filter='data')
 assert (release/'manager/index.html').is_file()
 for path,digest in manifest['frontend_files'].items():assert sha(release/path)==digest
 nxt=manager/'webroot.unified-exec-next';nxt.symlink_to(release.relative_to(manager));os.replace(nxt,manager/'webroot')
 try:static_check();check_unchanged()
 except Exception:
  nxt=manager/'webroot.unified-exec-rollback';nxt.symlink_to((audit/'webroot.before').read_text());os.replace(nxt,manager/'webroot');raise
 print('Unified execution frontend activated; no process restarted.',flush=True)
elif a.phase=='verify':
 static_check();check_unchanged()
 with session('lgq') as api:
  d=api('/manager/api/dashboard');check_public_account(d)
  report=next(r for r in d['report']['sources'] if r['source_id']==account_id)
  assert report['order_event_count']==0 and report['initial_position_count']==0
  for prefix in ['/exec_okex01/','/exec_binance_spread01/']:assert api(prefix+'snapshot')['entries']==[]
  for source in members:
   f=api('/manager/api/catalog/accounts/'+source+'/funding');assert not f['enabled'] and f['summary']['record_count']==0
  assert api('/manager/api/health')['status']=='ok'
 with session('prc') as api:
  d=api('/manager/api/dashboard');assert not any(x['source_id'] in members+[account_id] for x in d['accounts'])
  for prefix in ['/exec_okex01/','/exec_binance_spread01/']:
   try:api(prefix+'snapshot')
   except RuntimeError as e:assert 'HTTP 403' in str(e)
   else:raise AssertionError('hidden execution snapshot allowed')
 result={'public_account_id':account_id,'public_alias':account_alias,'public_accounts':1,'execution_sources':members,'manager_pid':int(pid()),'processes_restarted':False,'protected_processes_unchanged':len(procs()),'api_and_config_unchanged':True,'execution_configs_unchanged':True,'snapshots_and_bindings_unchanged':True,'actual_exec_snapshots_empty':True,'orders':0,'initial_positions':0,'funding':'estimated','trading_started':False}
 receipt('verification.json',result);shutil.copy2(stage/'manifest.json',audit/'manifest.json');print(json.dumps(result),flush=True)
