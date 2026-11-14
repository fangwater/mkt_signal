"""JP symbol-group cutover audit helper. Private captures stay on the host."""
import argparse
import collections
import copy
import fcntl
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.request

H = Path.home()
M = H / 'crypto_cta_manager'
PREP = H / 'cta-prep-binance-futures'
SPREAD = H / 'cta-factor-spread'
SOURCE = 'binance_exec_rx03'
OLD = {f'cta-prep02-bn-{i}-pos' for i in ('1min', '5min', '15min')}
PERIODS = ('1min', '5min', '15min')
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session, sql, env_for, db_env
from manager_entries import protected_processes
from clear_positions import exchange_snapshot
for site in (H / '.venvs/default/lib').glob('python*/site-packages'):
    sys.path.insert(0, str(site))
import redis


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.next')
    with temporary.open('w') as stream:
        os.fchmod(stream.fileno(), 0o600)
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
    os.replace(temporary, path)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def local_api(path, body=None):
    request = urllib.request.Request('http://127.0.0.1:19220' + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=30) as response:
        return json.load(response)


def client():
    return redis.Redis.from_url(tomllib.loads((M / 'config/cta-manager.toml').read_text())['redis']['url'])


def source_config():
    return next(s for s in tomllib.loads((M / 'config/cta-manager.toml').read_text())['sources'] if s['id'] == SOURCE)


def manager_env():
    config = tomllib.loads((M/'config/cta-manager.toml').read_text())
    name = config.get('database',{}).get('url_env','DATABASE_URL')
    for process in Path('/proc').glob('[0-9]*'):
        try:
            if (process/'comm').read_text().strip()!='cta_web' or (process/'cwd').resolve()!=M:
                continue
            values = dict(item.split(b'=',1) for item in (process/'environ').read_bytes().split(b'\0') if b'=' in item)
            require(name.encode() in values,'Manager database environment variable missing')
            result=dict(os.environ);result[name]=values[name.encode()].decode()
            return result
        except (FileNotFoundError,PermissionError):
            continue
    raise RuntimeError('running Manager environment unavailable')


def snapshot():
    url = source_config()['exec_viz_url'].rstrip('/') + '/snapshot'
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(url, timeout=20) as response:
        value = json.load(response)
    entry = next(e['entry'] for e in value['entries'] if e['channel'] == 'exec_pre_trade_state')
    return value, entry


def ledgers(r):
    result = {}
    for family in ['batch_exec', 'chase_exec']:
        key = f'{SOURCE}:binance-futures:{family}_state:position_allocations'
        raw = r.get(key)
        result[family] = json.loads(raw) if raw is not None else None
    return result


def lifecycle(root, name, audit):
    arguments = [str(root / 'scripts' / name)]
    if root in [PREP, SPREAD]:
        arguments.append('--execute')
    result = subprocess.run(arguments, cwd=root, capture_output=True, timeout=60)
    (audit / (root.name + '.' + name + '.log')).write_bytes(result.stdout + result.stderr)
    require(result.returncode == 0, f'environment-local lifecycle failed: {root.name}/{name}')
    print(json.dumps({'env': root.name, 'script': name, 'ok': True}), flush=True)


def state():
    return read(PREP / 'data/cta_prep_binance_futures/state.json')


def run_private(command, audit, name, env=None):
    result = subprocess.run(command, capture_output=True, env=env, timeout=180)
    (audit / (name + '.stdout')).write_bytes(result.stdout)
    (audit / (name + '.stderr')).write_bytes(result.stderr)
    return result


def prepare(audit):
    require(not (audit / 'before.json').exists(), 'audit already prepared')
    before = {'config_sha256': {}, 'bindings': [], 'processes': protected_processes()}
    paths = [PREP/'env.sh', SPREAD/'env.sh', H/SOURCE/'env.sh', H/SOURCE/'trade_engine.toml',
             PREP/'config/cta_prep_binance_futures.toml', SPREAD/'config/cta_factor_spread.toml']
    paths += list((PREP/'config/cta_prep_binance_futures').glob('*.json'))
    paths += list((SPREAD/'config/cta_factor_spread').glob('*.json'))
    for path in paths:
        before['config_sha256'][str(path)] = sha(path)
        target = audit/'original-files'/path.relative_to(H)
        target.parent.mkdir(parents=True, exist_ok=True); shutil.copy2(path, target)
    with session('lgq') as api:
        account = api('/manager/api/catalog/accounts/' + SOURCE)
        require(account['configuration']['mode'] == 'independent', 'review virtual-account ownership first')
        require({r['position_strategy_name'] for r in account['bindings']} == OLD, 'original account bindings changed')
        before['bindings'] = account['bindings']
        before['catalog'] = api('/manager/api/catalog/position-strategies')
    save(audit/'before.json', before)
    result = run_private(['pg_dump', '-Fc', '-f', str(audit/'manager.before.dump')], audit, 'pg_dump', db_env(True))
    require(result.returncode == 0, 'Manager backup failed; see private audit')
    for worker, directory in [(PREP, 'cta_prep_binance_futures'), (SPREAD, 'cta_factor_spread')]:
        staged = audit/'stage'/worker.name/'config'
        (staged/directory).mkdir(parents=True, exist_ok=True)
        shutil.copy2(worker/'config'/(directory+'.toml'), staged/(directory+'.toml'))
        for path in (worker/'config'/directory).glob('*.json'):
            if path.stem.startswith('prep02_'):
                continue
            shutil.copy2(path, staged/directory/path.name)
    template = audit/'templates/config/cta_prep_binance_futures'
    for path in template.glob('*_prep02_*.json'):
        c = read(path)
        original = read(PREP/'config/cta_prep_binance_futures'/f"prep02_binance-futures_{c['interval']}.json")
        c['signal'] = copy.deepcopy(original['signal'])
        c['signal']['enabled'] = False
        members = read(template/'symbol_groups.json')[c['symbol_group']]
        c['signal']['symbol_overrides'] = {s:v for s,v in c['signal'].get('symbol_overrides',{}).items() if s in members}
        save(audit/'stage/cta-prep-binance-futures/config/cta_prep_binance_futures'/path.name, c)
    for root, directory in [('cta-prep-binance-futures','cta_prep_binance_futures'), ('cta-factor-spread','cta_factor_spread')]:
        shutil.copy2(audit/'templates/config'/directory/'symbol_groups.json', audit/'stage'/root/'config'/directory/'symbol_groups.json')
        result = run_private([str(audit/'bin'/root), '--config', str(audit/'stage'/root/'config'/(directory+'.toml')), '--check'], audit, root+'.check')
        require(result.returncode == 0, 'staged worker plan failed validation')
    print(json.dumps({'prepared': True, 'source': SOURCE, 'old_strategies': 3, 'new_strategies': 9, 'operator_parameters_preserved': True}), flush=True)


def preview(audit):
    value, entry = snapshot(); save(audit/'preview-snapshot.json', value)
    result = run_private([str(audit/'bin/nav_allocation_reconcile'), '--config', str(M/'config/cta-manager.toml'), '--source', SOURCE,
        '--snapshot-json', str(audit/'preview-snapshot.json'), '--audit-json', str(audit/'preview-manager-audit.json'),
        '--manager-db', str(M/'db'), '--manager-secondary', str(audit/'preview-manager-secondary')], audit, 'preview-nav', manager_env())
    require((audit/'preview-manager-audit.json').exists(), 'Manager historical replay failed before the audit; see private logs')
    audit_value = read(audit/'preview-manager-audit.json')
    expected = {(s,m,v):q for s,m,v,q in audit_value['strategies']}
    differences = []
    for family, ledger in ledgers(client()).items():
        for name,row in (ledger or {}).get('positions',{}).items():
            if name not in OLD: continue
            for symbol,qty in row.items():
                actual = expected.get((name,symbol,1),0)
                if abs(actual-qty)>1e-8: differences.append({'strategy':name,'symbol':symbol,'ledger':qty,'manager':actual})
    print(json.dumps({'manager_replay_available': True, 'differences': differences, 'candidate_prepare_exit':result.returncode}), flush=True)


def freeze(audit):
    require((audit/'before.json').exists(), 'prepare first')
    for period in PERIODS:
        catalog = local_api('/api/config')
        c = catalog['plan']['combinations'][f'prep02_binance-futures_{period}']
        c['signal']['enabled'] = False
        local_api('/api/combinations', {'combination':c,'expected_revision':catalog['revision']})
    save(audit/'frozen.json', {'at_ms':time.time_ns()//1000000})
    print(json.dumps({'prep02_publishing_paused':True,'prep01_unchanged':True}), flush=True)


def capture(audit):
    require((audit/'frozen.json').exists(), 'freeze first')
    require(all(not t['enabled'] for t in state()['position_tables']['tables'] if t['strategy_group']=='prep02'), 'runtime has not acknowledged pause')
    r = client(); first = ledgers(r); start_ms = time.time_ns()//1000000
    value, entry = snapshot()
    require(entry['position_ready'], 'Exec positions not ready')
    require(all(row['active_batches']==0 and row['live_order_qty']==0 for row in entry['rows']), 'Exec still has active orders; wait before capture')
    # Wait only for a fresh observation; do not trade to force a flat account.
    deadline=time.monotonic()+12
    while entry['ts_ms'] < start_ms:
        require(time.monotonic()<deadline,'Exec observation did not refresh')
        time.sleep(.25);value,entry=snapshot()
    second = ledgers(r); require(first==second,'Exec ledger moved during capture; retry')
    save(audit/'capture-ledgers.json', first); save(audit/'snapshot.json',value)
    save(audit/'capture-times.json',{'start_ms':start_ms,'snapshot_ms':entry['ts_ms'],'end_ms':time.time_ns()//1000000})
    lifecycle(H/SOURCE,'stop_exec_trade_signal.sh',audit)
    lifecycle(H/SOURCE,'stop_exec_pre_trade.sh',audit)
    require(ledgers(r)==second,'Exec ledger moved before stop; recapture final state')
    save(audit/'capture-ledgers-after.json',ledgers(r))
    positions=PREP/'data/cta_prep_binance_futures/positions'
    for name in OLD:
        shutil.copy2(positions/(name+'.json'),audit/(name+'.json'))
    print(json.dumps({'captured':True,'cutover_ts_us':entry['ts_ms']*1000,'exec_paused':True}),flush=True)


def clone_access(mapping):
    statements=['BEGIN;']
    columns=json.loads(sql("SELECT jsonb_agg(column_name) FROM information_schema.columns WHERE table_schema='public' AND table_name='cta_position_strategies'"))
    copied=[c for c in ['created_by_user_id','publish_token_hash','open_visibility'] if c in columns]
    for old,names in mapping.items():
        for name in names:
            if copied:
                statements.append('UPDATE cta_position_strategies n SET '+','.join(c+'=o.'+c for c in copied)+" FROM cta_position_strategies o WHERE n.strategy_name='"+name+"' AND o.strategy_name='"+old+"';")
    for table in ['cta_position_strategy_grants','cta_position_strategy_managers','cta_position_strategy_viewers']:
        cols=json.loads(sql("SELECT coalesce(jsonb_agg(column_name ORDER BY ordinal_position),'[]'::jsonb) FROM information_schema.columns WHERE table_schema='public' AND table_name='"+table+"' AND column_name<>'id' AND is_identity='NO' AND coalesce(column_default,'') NOT LIKE 'nextval(%'"))
        if 'strategy_name' not in cols:continue
        for old,names in mapping.items():
            for name in names:
                values=["'"+name+"'" if c=='strategy_name' else 'o.'+c for c in cols]
                statements.append('INSERT INTO '+table+' ('+','.join(cols)+') SELECT '+','.join(values)+' FROM '+table+" o WHERE o.strategy_name='"+old+"' ON CONFLICT DO NOTHING;")
    statements.append('COMMIT;')
    sql('\n'.join(statements),read_only=False)


def install(audit):
    require((audit/'nav-verification.json').exists(),'validate historical PnL first')
    require(not (audit/'installed.json').exists(),'already installed; use verification')
    summary=read(audit/'bundle/migration.json');request=read(audit/'request.json')
    verification=read(audit/'nav-verification.json')
    require(verification['before']['account']==verification['after']['account'],'account quantity verification failed')
    require(all(w['account_unchanged'] for w in verification['windows']),'account PnL verification failed')
    require(ledgers(client())==request['sources'][0]['ledgers'],'captured Exec allocations changed')
    for name,digest in summary['input_position_sha256'].items():
        path=PREP/'data/cta_prep_binance_futures/positions'/(name+'.json')
        require(sha(path)==digest or read(path)==read(audit/'bundle/signal_positions'/(name+'.json')),'signal position capture changed')
    before=read(audit/'before.json')
    for path,digest in before['config_sha256'].items():
        if path.endswith('/env.sh') or path.endswith('.toml'):
            require(sha(path)==digest,'operator environment or main configuration changed')
    manifest=read(audit/'release_manifest.json')
    for name,digest in manifest['sha256'].items():
        require(sha(audit/'bin'/name)==digest,'staged binary checksum changed')
    # All target changes occur while this account's Exec is stopped.
    with session('lgq') as api:
        current={p['strategy_name']:p for p in api('/manager/api/catalog/position-strategies')}
        new_names={n for names in summary['old_to_new'].values() for n in names}
        applied_before=(audit/'allocation-applied.json').exists()
        if not applied_before:require(not new_names & current.keys(),'new strategy identity already exists')
        expected_targets={r['strategy_name']:r['targets'] for r in read(audit/'bundle/manager-targets.json')}
        for name in new_names & current.keys():require(current[name]['targets']==expected_targets[name],'prepared new strategy targets changed')
        account=api('/manager/api/catalog/accounts/'+SOURCE)
        if not applied_before:require(account['bindings']==before['bindings'],'original bindings changed')
        else:
            templates={b['position_strategy_name']:b for b in before['bindings']}
            for binding in account['bindings']:
                name=binding['position_strategy_name']
                require(name in OLD|new_names,'unexpected binding during cutover resume')
                old=name if name in OLD else next(o for o,names in summary['old_to_new'].items() if name in names)
                original=templates[old]
                require(binding['order_strategy_name']==original['order_strategy_name'] and binding['shares'] in ([0,original['shares']] if name in OLD else [original['shares']]),'binding changed during resume')
        require(not account['pending_publishes'],'account still has pending publications')
    with (audit/'cutover.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        lifecycle(PREP,'stop_cta_factor_config.sh',audit)
        lifecycle(PREP,'stop_cta_prep_binance_futures.sh',audit)
        lifecycle(SPREAD,'stop_cta_factor_spread.sh',audit)
        position_lock=(PREP/'data/cta_prep_binance_futures/.worker.lock').open('a')
        fcntl.flock(position_lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        try:
            if not applied_before:
                applied=run_private([str(audit/'bin/nav_allocation_reconcile'),'--config',str(M/'config/cta-manager.toml'),'--source',SOURCE,
                    '--candidate',str(audit/'bundle/sources'/SOURCE/'manager-allocation.json'),'--apply','--note','Prep02 symbol-group cutover; preserve account FIFO and pre-cutover history'],audit,'apply-nav',manager_env())
                require(applied.returncode==0,'Manager allocation insertion failed; execution remains stopped')
                save(audit/'allocation-applied.json',read(audit/'apply-nav.stdout'))
            else:
                saved=json.loads(sql("SELECT positions FROM cta_strategy_allocation_snapshots WHERE source_id='"+SOURCE+"' AND snapshot_ts_us="+str(summary['cutover_ts_us'])))
                require(saved==read(audit/'bundle/sources'/SOURCE/'manager-allocation.json')['positions'],'immutable allocation record differs')
            targets=read(audit/'bundle/manager-targets.json')
            with session('lgq') as api:
                for target in targets:
                    if target['strategy_name'] in new_names:
                        if target['strategy_name'] not in current:api('/manager/api/catalog/position-strategies',target)
                clone_access(summary['old_to_new'])
                by_name={b['position_strategy_name']:b for b in before['bindings']}
                for old,names in summary['old_to_new'].items():
                    original=by_name[old]
                    for name in names:
                        if any(b['position_strategy_name']==name for b in account['bindings']):continue
                        api('/manager/api/catalog/accounts/'+SOURCE+'/bindings',{
                            'binding_name':name,'position_strategy_name':name,
                            'order_strategy_name':original['order_strategy_name'],'shares':original['shares']})
                for b in before['bindings']:
                    existing=next((r for r in account['bindings'] if r['binding_name']==b['binding_name']),None)
                    if existing:
                        if existing['shares']!=0:
                            api('/manager/api/catalog/accounts/'+SOURCE+'/bindings',{'binding_name':b['binding_name'],'position_strategy_name':b['position_strategy_name'],'order_strategy_name':b['order_strategy_name'],'shares':0})
                        api('/manager/api/catalog/accounts/'+SOURCE+'/bindings/'+b['binding_name'],None,'DELETE')
                for target in targets:
                    if target['strategy_name'] in OLD:
                        if target['strategy_name'] in current:api('/manager/api/catalog/position-strategies',target)
            # Keep raw history and immutable allocation records; retire only catalog definitions.
            with session('prc') as admin:
                for name in OLD:
                    if name in current:admin('/manager/api/catalog/position-strategies/'+name,None,'DELETE')
            save(audit/'bindings-installed.json',{'new_names':sorted(new_names),'old_removed':sorted(OLD)})
            r=client();keys={family:f'{SOURCE}:binance-futures:{family}_state:position_allocations' for family in ['batch_exec','chase_exec']}
            with r.pipeline() as pipe:
                pipe.watch(*keys.values())
                for family,key in keys.items():
                    raw=pipe.get(key);value=json.loads(raw) if raw is not None else None
                    planned=read(audit/'bundle/sources'/SOURCE/(family+'-allocations.json'))
                    require(value==request['sources'][0]['ledgers'][family] or value==planned,'Exec allocations changed before compare-and-swap')
                pipe.multi()
                for family,key in keys.items():
                    value=read(audit/'bundle/sources'/SOURCE/(family+'-allocations.json'))
                    if value is not None:pipe.set(key,json.dumps(value,separators=(',',':')))
                pipe.execute()
            save(audit/'exec-allocations-installed.json',ledgers(r))
            for path in (audit/'bundle/signal_positions').glob('*.json'):
                save(PREP/'data/cta_prep_binance_futures/positions'/path.name,read(path))
            for worker,directory in [(PREP,'cta_prep_binance_futures'),(SPREAD,'cta_factor_spread')]:
                destination=worker/'config'/directory
                for path in (audit/'stage'/worker.name/'config'/directory).glob('*.json'):
                    value=read(path)
                    if value.get('strategy_group')=='prep02':
                        original=read(audit/'original-files'/PREP.name/'config/cta_prep_binance_futures'/f"prep02_binance-futures_{value['interval']}.json")
                        value['signal']['enabled']=original['signal']['enabled']
                    save(destination/path.name,value)
                if worker==PREP:
                    for period in PERIODS:(destination/f'prep02_binance-futures_{period}.json').unlink()
            binaries={PREP:['cta-prep-binance-futures','cta-factor-config','cta-symbol-group-migrate'],SPREAD:['cta-factor-spread']}
            for root,names in binaries.items():
                for name in names:
                    target=root/name;backup=audit/'original-binaries'/root.name/name
                    if target.exists() and not backup.exists():backup.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(target,backup)
                    temporary=target.with_name(target.name+'.publish-next')
                    shutil.copy2(audit/'bin'/name,temporary);temporary.chmod(0o755);os.replace(temporary,target)
                    require(sha(target)==manifest['sha256'][name],'installed binary checksum differs')
            delist=H/'delist_risk_server'
            lifecycle(delist,'stop_delist_risk_server.sh',audit)
            backup=audit/'original-binaries/delist_risk_server';backup.mkdir(parents=True,exist_ok=True)
            shutil.copy2(delist/'delist_risk_server',backup/'delist_risk_server')
            temporary=delist/'delist_risk_server.publish-next';shutil.copy2(audit/'bin/delist_risk_server',temporary);temporary.chmod(0o755);os.replace(temporary,delist/'delist_risk_server')
            lifecycle(delist,'start_delist_risk_server.sh',audit)
            save(audit/'installed.json',{'at_ms':time.time_ns()//1000000,'runtime_commit':manifest['commit'],'new_strategies':sorted(new_names),'cutover_ts_us':summary['cutover_ts_us']})
        finally:
            position_lock.close()
    print(json.dumps({'installed':True,'new_strategy_count':9,'exec_still_paused':True}),flush=True)


def activate(audit):
    require((audit/'installed.json').exists(),'install first')
    current=exchange_snapshot(SOURCE);save(audit/'exchange.resume-check.json',current)
    require(current['open_orders']==current['algo_orders']==0,'account has orders before resuming')
    actual={}
    for row in current['nonzero_positions']:
        symbol=row['symbol'];match=re.fullmatch(r'BINANCE_PERP_([A-Z0-9]+)_USDT',symbol)
        symbol=match[1]+'USDT' if match else symbol
        require(re.fullmatch(r'[A-Z0-9]+USDT',symbol) and symbol not in actual,'invalid or duplicate exchange symbol')
        actual[symbol]=float(row['qty'])
    before={s:q for s,v,q in read(audit/'manager-audit.json')['account'] if v==1}
    require(all(abs(before.get(s,0)-actual.get(s,0))<1e-8 for s in before.keys()|actual.keys()),'exchange quantity changed before resuming')
    def running(root,binary):
        for p in Path('/proc').glob('[0-9]*'):
            try:
                if (p/'cwd').resolve()==root and Path((p/'cmdline').read_bytes().split(b'\0')[0].decode()).name==binary:return True
            except (OSError,UnicodeError):pass
        return False
    for root,binary,script in [(PREP,'cta-factor-config','start_cta_factor_config.sh'),(PREP,'cta-prep-binance-futures','start_cta_prep_binance_futures.sh'),
            (SPREAD,'cta-factor-spread','start_cta_factor_spread.sh'),(H/SOURCE,'exec-pre-trade','start_exec_pre_trade.sh'),(H/SOURCE,'trade_signal','start_exec_trade_signal.sh')]:
        if not running(root,binary):lifecycle(root,script,audit)
    save(audit/'activated.json',{'at_ms':time.time_ns()//1000000})
    print(json.dumps({'activated':True,'prep02_strategies':9}),flush=True)


def publish_exec(audit):
    require((audit/'installed.json').exists(),'install first')
    lifecycle(H/SOURCE,'stop_exec_trade_signal.sh',audit)
    lifecycle(H/SOURCE,'stop_exec_pre_trade.sh',audit)
    names=set(read(audit/'installed.json')['new_strategies'])
    results=[]
    with session('lgq') as api:
        account=api('/manager/api/catalog/accounts/'+SOURCE)
        require({b['position_strategy_name'] for b in account['bindings']}==names,'unexpected execution bindings')
        for binding in account['bindings']:
            results.append(api('/manager/api/catalog/accounts/'+SOURCE+'/bindings/'+binding['binding_name']+'/publish',{},'POST'))
    save(audit/'execution-publications.json',results)
    origin=source_config()['exec_config_url'].rstrip('/')
    opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for name in OLD:
        request=urllib.request.Request(origin+'/api/strategy?name='+name,method='DELETE')
        with opener.open(request,timeout=30) as response:response.read()
    r=client();raw=r.get(SOURCE+':binance-futures:batch_exec:strategy_names')
    require(set(json.loads(raw))==names,'physical execution strategy index differs')
    for name in names:
        value=json.loads(r.get(SOURCE+':binance-futures:batch_exec:'+name))
        require(value['algorithm']=='grid','execution algorithm changed')
    save(audit/'execution-published.json',{'new_strategies':sorted(names),'old_removed':sorted(OLD)})
    print(json.dumps({'execution_configs_published':9,'old_execution_configs_retired':3}),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase',choices=['prepare','preview','freeze','capture','install','publish_exec','activate'])
    parser.add_argument('--audit',type=Path,required=True)
    parser.add_argument('--execute',action='store_true')
    args=parser.parse_args();audit=args.audit
    os.umask(0o077)
    require(audit.parent==M/'maintenance-backups' and audit.name.startswith('cta_symbol_groups_'),'unexpected audit root')
    require(audit.is_dir(),'create private staging root first')
    if args.phase in ['freeze','capture','install','publish_exec','activate']: require(args.execute,'live phase requires --execute')
    globals()[args.phase](audit)


if __name__=='__main__':main()
