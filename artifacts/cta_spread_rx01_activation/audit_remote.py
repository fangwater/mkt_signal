import pathlib, subprocess, os, json, tomllib, urllib.parse

root = pathlib.Path.home()
cfg = tomllib.loads((root/'crypto_cta_manager/config/cta-manager.toml').read_text())
pid = subprocess.check_output(['systemctl','--user','show','crypto-cta-manager-web.service','-p','MainPID','--value'],text=True).strip()
runtime = dict(x.split(b'=',1) for x in pathlib.Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
u = urllib.parse.urlsplit(runtime[cfg['database'].get('url_env','CRYPTO_CTA_LOCAL_DATABASE_URL').encode()].decode())
env = dict(os.environ, PGHOST=u.hostname, PGPORT=str(u.port or 5432), PGUSER=urllib.parse.unquote(u.username), PGPASSWORD=urllib.parse.unquote(u.password or ''), PGDATABASE=u.path[1:])
for q in [
 'SELECT source_id,maker_fee_rate,taker_fee_rate,theoretical_twap_fee_rate FROM cta_order_sources ORDER BY source_id',
 'SELECT user_id,username,role,disabled FROM cta_users',
 'SELECT virtual_id,name FROM cta_virtual_accounts',
 'SELECT strategy_name,algorithm,single_order_usdt,target_tolerance_usdt FROM cta_order_strategies',
]:
 p = subprocess.run(['psql','-X','-q','-A','-t','-v','ON_ERROR_STOP=1'],input=q+';',text=True,capture_output=True,env=env)
 assert p.returncode == 0
 print(p.stdout)
for name in ['binance-cta-rx01','binance_exec_trade06','binance_exec_trade05','binance-cta-spread-rx01']:
 p=root/name
 raw=subprocess.check_output(['bash','-c','source "$1/env.sh"; env -0','bash',str(p)])
 values=dict(x.decode().split('=',1) for x in raw.split(b'\0') if b'=' in x)
 print(name,'runtime_settings',json.dumps({k:v for k,v in values.items() if k in ['IPC_NAMESPACE','TRADE_ENGINE_EXEC_BACKEND_MAP','RAPIDX_BINANCE_CASH_BUSINESS_TYPE','CTA_MANAGER_API_URL','CTA_POSITION_STRATEGY_PREFIX','CTA_VIRTUAL_ACCOUNT_NAME','CTA_FOLLOWER_SOURCE_ID'] or k.endswith('_CORE')}))
 print('snapshot_helpers',[f.name for f in (p/'scripts').glob('*rapidx*')])
