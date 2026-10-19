"""Install already verified artifacts into the stopped RX01 Spread environment."""
import hashlib,json,os,shutil,sys
from pathlib import Path

root=Path.home()/'binance-cta-spread-rx01'
stage=Path(sys.argv[1]).resolve()
assert root.is_dir() and stage.parent.parent==root/'run'
sys.path.insert(0,str(stage/'scripts'))
from cta_spread_config_server import normalize_config
from cta_spread_factor_config import normalize_table,validate_available,available_universe,read_factor_table,save_factor_table
import redis
for proc in Path('/proc').glob('[0-9]*'):
    try:
        if os.readlink(proc/'cwd') in {str(root),str(Path.home()/'binance_exec_trade06')}:
            comm=(proc/'comm').read_text().strip()
            if comm in {'trade_engine','exec-pre-trade','cta_spread_sign','cta_spread_fact','viz_server','persist_manager'} or 'account_mon' in comm:
                raise RuntimeError('target process is running: '+proc.name+'/'+comm)
    except (FileNotFoundError,ProcessLookupError,PermissionError): pass
manifest=json.loads((stage/'manifest.json').read_text())
for name,expected in manifest.items():
    if hashlib.sha256((stage/name).read_bytes()).hexdigest()!=expected: raise RuntimeError('checksum mismatch: '+name)
config=normalize_config(json.loads((stage/'config/cta_spread.json').read_text()))
assert not config['enabled'] and config['max_pos_u']==100
current=json.loads((root/'config/cta_spread.json').read_text())
assert current['max_pos_u']==config['max_pos_u']
table=normalize_table(json.loads((stage/'config/cta_spread_symbol_factors.json').read_text()))
validate_available(table,available_universe(stage/'config/cta_spread_factor_model_1m_pub.toml'))
assert len(table)==50 and sum(len(row) for row in table.values())==873
client=redis.Redis(host=os.getenv('REDIS_HOST','127.0.0.1'),port=int(os.getenv('REDIS_PORT','6379')),db=int(os.getenv('REDIS_DB','0')),username=os.getenv('REDIS_USERNAME'),password=os.getenv('REDIS_PASSWORD'),socket_connect_timeout=3,socket_timeout=3)
previous=read_factor_table(client,root.name)
backup=stage.parent/'backup'; backup.mkdir(exist_ok=False)
(backup/'symbol_factors_redis.json').write_text(json.dumps(previous,indent=2))
state=root/'run/cta_spread_position_flags.json'
state_hash=hashlib.sha256(state.read_bytes()).hexdigest() if state.exists() else None
for name in list(manifest)+['run/cta_spread_position_flags.json','run/cta_spread_status.json']:
    path=root/name
    if path.exists():
        target=backup/name; target.parent.mkdir(parents=True,exist_ok=True); shutil.copy2(path,target)
for name in manifest:
    source=stage/name; destination=root/name
    destination.parent.mkdir(parents=True,exist_ok=True)
    temp=destination.with_name('.'+destination.name+'.corr50-upload')
    shutil.copyfile(source,temp)
    os.chmod(temp,0o755 if name.startswith('scripts/') or '/' not in name else 0o644)
    with temp.open('rb') as handle: os.fsync(handle.fileno())
    os.replace(temp,destination)
    fd=os.open(destination.parent,os.O_DIRECTORY); os.fsync(fd); os.close(fd)
saved=save_factor_table(client,root.name,table,previous['revision'],available_universe(root/'config/cta_spread_factor_model_1m_pub.toml'))
assert saved['symbol_factors']==table
assert (hashlib.sha256(state.read_bytes()).hexdigest() if state.exists() else None)==state_hash
receipt={'files_verified':len(manifest),'symbols':len(table),'strategies':len({f for row in table.values() for f in row}),'combinations':sum(len(row) for row in table.values()),'enabled':config['enabled'],'max_pos_u':config['max_pos_u'],'backup':str(backup),'state_preserved':True}
(stage.parent/'receipt.json').write_text(json.dumps(receipt,indent=2))
print(json.dumps(receipt))
