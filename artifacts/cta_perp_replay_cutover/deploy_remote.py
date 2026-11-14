"""RX02/RX03 cutover. Dry-run by default; operations use env-local wrappers."""
import argparse,hashlib,json,os,pathlib,shutil,subprocess,time
ROOT=pathlib.Path.home()
STAGE=pathlib.Path(__file__).resolve().parent
TARGETS=['binance-cta-perp-rx02','binance-cta-perp-rx03']
def atomic(src,dst):
 tmp=dst.with_name(dst.name+'.cutover-next');shutil.copy2(src,tmp)
 assert hashlib.sha256(tmp.read_bytes()).digest()==hashlib.sha256(src.read_bytes()).digest()
 os.replace(tmp,dst)
def shell(env,script):
 subprocess.run(['bash',str(env/'scripts'/script)],cwd=env,check=True)
def assert_stopped(env):
 for proc in pathlib.Path('/proc').iterdir():
  if not proc.name.isdigit():continue
  try:
   if proc.joinpath('cwd').resolve()==env and proc.joinpath('exe').resolve().name=='cta_perp_signal':raise RuntimeError('signal still running')
  except OSError:pass
def verify_artifacts():
 manifest=json.loads((STAGE/'manifest.json').read_text())
 for name,digest in manifest['files'].items():assert hashlib.sha256((STAGE/name).read_bytes()).hexdigest()==digest, name
 assert os.uname().nodename.startswith('ip-172-31-35-228')
 assert (ROOT/'binance-cta-spread-rx01').is_dir()
 return manifest
def install():
 before=json.loads((STAGE/'before.json').read_text())
 for name in TARGETS:
  env=ROOT/name
  old=before['environments'][name]
  for relative,digest in old['hashes'].items():
   assert hashlib.sha256((env/relative).read_bytes()).hexdigest()==digest, 'operator configuration changed: '+name+'/'+relative
 # Stop both signal processes before changing any factor/config state.
 for name in TARGETS:shell(ROOT/name,'stop_cta_perp_signal.sh')
 for name in TARGETS:
  env=ROOT/name;assert_stopped(env)
  for script in ['stop_cta_perp_factor_model_1m_pub.sh','stop_cta_perp_config_server.sh','stop_cta_perp_dashboard.sh']:shell(env,script)
  backup=env/'run'/'cutover_before_20261015';backup.mkdir(mode=0o700,exist_ok=False)
  for name2 in ['cta_perp_signal','cta_perp_factor_model_1m_pub','config','scripts','web','run/cta_perp_position_flags.json','run/cta_perp_status.json']:
   src=env/name2;dst=backup/name2;dst.parent.mkdir(exist_ok=True,parents=True)
   if src.is_dir():shutil.copytree(src,dst)
   elif src.exists():shutil.copy2(src,dst)
  for name2 in ['cta_perp_signal','cta_perp_factor_model_1m_pub']:
   atomic(STAGE/'bin'/name2,env/name2);os.chmod(env/name2,0o755)
  for directory in ['scripts','web']:
   for src in (STAGE/directory).rglob('*'):
    if src.is_file():
     dst=env/src.relative_to(STAGE);dst.parent.mkdir(exist_ok=True,parents=True);atomic(src,dst)
  tag=name[-4:]
  for name2 in ['cta_perp.json','cta_perp_factor_model_1m_pub.toml','cta_perp_symbol_factors.json']:
   atomic(STAGE/tag/name2,env/'config'/name2)
  subprocess.run(['bash','-c','source ./env.sh; python3 scripts/cta_perp_factor_config.py --env-name "$1" --config config/cta_perp_symbol_factors.json --execute > run/cutover_factor_table.json','bash',name],cwd=env,check=True)
  for script in ['start_cta_perp_config_server.sh','start_cta_perp_dashboard.sh','start_cta_perp_factor_model_1m_pub.sh']:shell(env,script)
  print(name,'installed; factor recovery started; signal remains stopped',flush=True)
def recover():
 for name in TARGETS:
  env=ROOT/name;assert_stopped(env)
  progress=json.loads((env/'data/cta_perp/factor_history/recovery.json').read_text())
  assert progress['generation']
  path=env/'run/cta_perp_recovery_request.json'
  assert not path.exists()
  path.write_text(json.dumps({'history_path':'data/cta_perp/factor_history','generation':progress['generation']})+'\n')
  shell(env,'start_cta_perp_signal.sh')
  print(name,'silent position recovery started',flush=True)
def main():
 p=argparse.ArgumentParser();p.add_argument('action',choices=['install','recover','verify']);p.add_argument('--execute',action='store_true');args=p.parse_args()
 m=verify_artifacts();print('verified artifact set',m['source_head'])
 if not args.execute:print('DRY RUN',args.action,'environments',TARGETS,'venue Binance Futures; RX02 v1 / RX03 v0');return
 if args.action=='install':install()
 elif args.action=='recover':recover()
if __name__=='__main__':main()
