#!/usr/bin/env python3
"""Account display metadata/UI only. Keep both Exec environments unchanged."""
import argparse,ast,hashlib,json,os,shutil,subprocess,tarfile,time,tomllib,urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('phase',choices=['prepare','activate','verify']);p.add_argument('--stage',required=True);a=p.parse_args()
manager=Path.home()/'crypto_cta_manager';stage=Path(a.stage).resolve();assert stage.parent==manager/'deploy-staging'
audit=manager/'maintenance-backups'/stage.name;manifest=json.loads((stage/'manifest.json').read_text());account='cta_cross_spread_rx01';alias='okex_binace_cross_1min';members=['okex_exec_trade01','binance_exec_spread01'];cfg=manager/'config/cta-manager.toml'
h=Path.home()/'binance_exec_trade06/run/rx01_activation_ops.py';t=ast.parse(h.read_text());ns={'MANAGER':manager,'ORIGIN':'http://127.0.0.1:4191'};nodes=[n for n in t.body if isinstance(n,(ast.Import,ast.ImportFrom)) or isinstance(n,ast.FunctionDef) and n.name in ['db_env','sql','session']];exec(compile(ast.Module(body=nodes,type_ignores=[]),str(h),'exec'),ns);sql=ns['sql'];session=ns['session']

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def write(p,text):
 with Path(p).open('x') as f:f.write(text);f.flush();os.fsync(f.fileno())
 Path(p).chmod(0o600)
def receipt(name,value):write(audit/name,json.dumps(value,indent=2,ensure_ascii=False)+'\n')
def install(src,dst):
 dst=Path(dst);tmp=dst.with_name(dst.name+'.alias-next');assert not tmp.exists();shutil.copyfile(src,tmp);tmp.chmod(0o755);assert sha(src)==sha(tmp);os.replace(tmp,dst)
def procs():
 names={'trade_engine','exec-pre-trade','pre_trade','trade_signal','account_monitor','binance_account','okex_account_mo','persist_manager','viz_server','cta-factor-spre','cta_signal'};out={}
 for proc in Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:
   name=(proc/'comm').read_text().strip()
   if name in names:out[proc.name]={'comm':name,'cwd':os.readlink(proc/'cwd'),'start_ticks':(proc/'stat').read_text().split(') ',1)[1].split()[19]}
  except OSError:pass
 return out
def native_aliases():return sql("SELECT coalesce(jsonb_object_agg(source_id, display_alias),'{}'::jsonb) FROM cta_order_sources;")
def invariants():
 return {table:sql(f"SELECT count(*) FROM {table} WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01');") for table in ['cta_position_snapshots','cta_strategy_position_snapshots','cta_strategy_allocation_snapshots','cta_account_strategy_bindings']}
def verify_metadata():
 assert sha(cfg)==sha(audit/'cta-manager.toml.before')
 assert native_aliases()==(audit/'native-aliases.before.json').read_text().strip()
 assert invariants()==json.loads((audit/'invariants.before.json').read_text())
 assert sql("SELECT count(*) FROM cta_order_sources WHERE source_id='cta_cross_spread_rx01';")=='0'
 assert sql("SELECT display_alias FROM cta_managed_account_aliases WHERE account_id='cta_cross_spread_rx01';")==alias
 assert procs()==json.loads((audit/'processes.before.json').read_text())
 for p,digest in json.loads((audit/'protected-files.json').read_text()).items():assert sha(p)==digest,p

if a.phase=='prepare':
 assert not audit.exists();audit.mkdir(mode=0o700)
 assert sha(stage/'cta_web')==manifest['cta_web_sha256'];assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256']
 shutil.copy2(manager/'bin/cta_web',audit/'cta_web.before');shutil.copy2(cfg,audit/'cta-manager.toml.before');write(audit/'webroot.before',os.readlink(manager/'webroot'))
 receipt('processes.before.json',procs());receipt('invariants.before.json',invariants());write(audit/'native-aliases.before.json',native_aliases()+'\n')
 settings=tomllib.loads(cfg.read_text());paths=[]
 for s in settings['sources']:
  if s.get('env_path'):paths.extend([Path(s['env_path']),Path(s['env_path']).parent/'trade_engine.toml'])
 paths.extend(Path.home()/env/file for env in ['okex_mm_alpha','binance_exec_trade08','okex-binance-cta-spread-rx01'] for file in ['env.sh','trade_engine.toml'])
 receipt('protected-files.json',{str(p):sha(p) for p in set(paths) if p.is_file()})
 exists=sql("SELECT to_regclass('public.cta_managed_account_aliases') IS NOT NULL;")=='t'
 write(audit/'managed-aliases.before.json',sql("SELECT coalesce(jsonb_object_agg(account_id,display_alias),'{}'::jsonb) FROM cta_managed_account_aliases;")+'\n' if exists else '{}\n')
 pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip();env=dict(os.environ);env.update(x.decode().split('=',1) for x in Path('/proc/'+pid+'/environ').read_bytes().split(b'\0') if b'=' in x)
 checked=subprocess.run([str(stage/'cta_web'),'--config',str(cfg),'--check-allocations'],env=env,text=True,capture_output=True);write(audit/'allocation-preflight.log',checked.stdout+checked.stderr);assert checked.returncode==0,'allocation preflight failed'
 # Explicit, additive metadata maintenance, with all preexisting aliases backed up.
 maintenance=(stage/'apply_account_alias_schema.sql').read_text();assert maintenance.count('COMMIT;')==1
 maintenance=maintenance.replace('COMMIT;',"INSERT INTO cta_managed_account_aliases(account_id,display_alias) VALUES ('cta_cross_spread_rx01','okex_binace_cross_1min') ON CONFLICT(account_id) DO UPDATE SET display_alias=EXCLUDED.display_alias,updated_at=now();\nCOMMIT;")
 sql(maintenance);verify_metadata();print('Alias metadata saved; source IDs, original aliases, immutable anchors and execution configuration unchanged.',flush=True)
elif a.phase=='activate':
 verify_metadata();assert sha(stage/'cta_web')==manifest['cta_web_sha256'];assert sha(stage/'frontend.tar.gz')==manifest['frontend_sha256']
 release=manager/'web-releases'/stage.name;assert not release.exists();release.mkdir()
 with tarfile.open(stage/'frontend.tar.gz') as tar:
  for item in tar.getmembers():assert item.isfile() or item.isdir();assert '..' not in Path(item.name).parts and not Path(item.name).is_absolute()
  tar.extractall(release,filter='data')
 try:
  install(stage/'cta_web',manager/'bin/cta_web');nxt=manager/'webroot.alias-next';nxt.symlink_to(release.relative_to(manager));os.replace(nxt,manager/'webroot')
  subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True);print('Only Manager restarted; allowing its historical cold scan to complete.',flush=True)
  opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
  for attempt in range(300):
   try:
    with opener.open('http://127.0.0.1:4191/manager/api/auth/status',timeout=5) as r:assert r.status==200
    with session('lgq') as api:
     d=api('/manager/api/dashboard');group=next(x for x in d['accounts'] if x['source_id']==account);assert group['display_alias']==alias and group['account']==alias and group['can_edit_alias'];assert api('/manager/api/health')['status']=='ok'
    break
   except Exception:
    if attempt==299:raise
    time.sleep(2)
  print('Manager healthy; public account alias is '+alias,flush=True)
 except Exception:
  install(audit/'cta_web.before',manager/'bin/cta_web');nxt=manager/'webroot.alias-rollback';nxt.symlink_to((audit/'webroot.before').read_text());os.replace(nxt,manager/'webroot');subprocess.run(['systemctl','--user','restart','crypto-cta-manager-web.service'],check=True)
  raise RuntimeError('Manager activation failed; prior binary and webroot restored') from None
elif a.phase=='verify':
 verify_metadata()
 with session('lgq') as api:
  dash=api('/manager/api/dashboard');group=[x for x in dash['accounts'] if x['source_id']==account];assert len(group)==1 and group[0]['display_alias']==alias and group[0]['account']==alias and group[0]['can_edit_alias']
  assert not any(x['source_id'] in members for x in dash['accounts'])
  assert next(x for x in dash['report']['sources'] if x['source_id']==account)['account']==alias
  for path in ['/account-overview','/nav/exchange']:
   value=api('/manager/api'+path);rows=[r for r in value['accounts'] if r['source_id']==account];assert len(rows)==1 and rows[0]['account']==alias
  # Exercise the normal alias endpoint with the operator-requested value only.
  saved=api(f'/manager/api/catalog/accounts/{account}/alias',{'alias':alias},'PUT');assert saved=={'source_id':account,'account':alias,'display_alias':alias}
  assert next(x for x in api('/manager/api/dashboard')['accounts'] if x['source_id']==account)['display_alias']==alias
 with session('prc') as api:
  try:api(f'/manager/api/catalog/accounts/{account}/alias',{'alias':alias},'PUT')
  except RuntimeError as e:assert 'HTTP 403' in str(e)
  else:raise AssertionError('hidden account alias was writable')
 verify_metadata()
 pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
 result={'account_id':account,'display_alias':alias,'public_accounts':1,'execution_sources':members,'protected_processes_unchanged':len(procs()),'execution_configs_unchanged':True,'snapshots_and_bindings_unchanged':True,'native_aliases_unchanged':True,'alias_endpoint_verified':True,'prc_alias_write_forbidden':True,'manager_pid':int(pid),'trading_started':False}
 receipt('verification.json',result);shutil.copy2(stage/'manifest.json',audit/'manifest.json');print(json.dumps(result),flush=True)
