"""Read-only deployment evidence; never serialize environment secrets."""
import hashlib,json,os,pathlib,subprocess,tomllib,urllib.parse,urllib.request
root=pathlib.Path.home()
manager=root/'crypto_cta_manager'
def sql(query):
 config=tomllib.loads((manager/'config/cta-manager.toml').read_text())
 pid=subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
 raw=pathlib.Path('/proc',pid,'environ').read_bytes()
 values=dict(x.split(b'=',1) for x in raw.split(b'\0') if b'=' in x)
 u=urllib.parse.urlsplit(values[config['database'].get('url_env','CRYPTO_CTA_LOCAL_DATABASE_URL').encode()].decode())
 env=dict(os.environ,PGHOST=u.hostname,PGPORT=str(u.port or 5432),PGUSER=urllib.parse.unquote(u.username),PGPASSWORD=urllib.parse.unquote(u.password or ''),PGDATABASE=u.path[1:])
 r=subprocess.run(['psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1'],input=query,text=True,capture_output=True,env=env)
 if r.returncode:raise RuntimeError('read-only SQL failed; details suppressed')
 return json.loads(r.stdout)
result={'host':os.uname().nodename,'environments':{},'protected':[]}
for name in ['binance-cta-spread-rx01','binance-cta-perp-rx02','binance-cta-perp-rx03']:
 p=root/name; assert p.is_dir()
 d={}
 for f in ['config/cta_perp.json','config/cta_perp_symbol_factors.json','run/cta_perp_status.json','run/cta_perp_position_flags.json']:
  q=p/f
  if q.exists():d[f]=json.loads(q.read_text())
 if (p/'config/cta_perp_factor_model_1m_pub.toml').exists():d['factor_config']=(p/'config/cta_perp_factor_model_1m_pub.toml').read_text()
 d['hashes']={str(q.relative_to(p)):hashlib.sha256(q.read_bytes()).hexdigest() for q in [p/'env.sh',*p.glob('config/cta*')] if q.is_file()}
 result['environments'][name]=d
for p in pathlib.Path('/proc').iterdir():
 if not p.name.isdigit():continue
 try:
  cwd=os.readlink(p/'cwd');exe=os.readlink(p/'exe')
  if cwd.startswith(str(root)) and not any(n in cwd for n in ['binance-cta-perp-rx02','binance-cta-perp-rx03']):
   result['protected'].append({'pid':int(p.name),'start':(p/'stat').read_text().split()[21],'cwd':cwd,'exe':exe})
 except OSError:pass
result['bindings']=sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT * FROM cta_account_strategy_bindings WHERE source_id IN ('binance_exec_trade05','binance_exec_trade07') ORDER BY source_id,binding_name) t;")
result['virtuals']=sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT * FROM cta_virtual_account_bindings WHERE virtual_id IN ('virtual02','virtual04') ORDER BY virtual_id,binding_name) t;")
result['targets']=sql("SELECT coalesce(json_agg(t),'[]') FROM (SELECT strategy_name,targets FROM cta_position_strategies WHERE strategy_name LIKE 'lgq_cta_%' OR strategy_name LIKE 'binance_cta_perp_rx03_%' ORDER BY strategy_name) t;")
print(json.dumps(result,indent=2))
