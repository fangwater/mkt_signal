"""Read-only live checks; saves a sanitized receipt under the private audit root."""
from pathlib import Path
import collections
import json
import sys
import time

AUDIT = Path('/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
sys.path.insert(0, str(AUDIT))
import ops

now = time.time_ns() // 1000000
installed = ops.read(AUDIT / 'installed.json')
names = set(installed['new_strategies'])
assert len(names) == 9
receipt = {'checked_at_ms': now, 'cutover_ts_us': installed['cutover_ts_us'], 'source': ops.SOURCE}
workers = {}
for root, directory, count in [(ops.PREP, 'cta_prep_binance_futures', 408), (ops.SPREAD, 'cta_factor_spread', 50)]:
    state = ops.read(root / 'data' / directory / 'state.json')
    assert 0 <= now - state['updated_ms'] < 15000, 'stale worker'
    assert not state['warming'] and state['generation'], 'worker still replaying'
    assert state['error'] is None and state['positions_error'] is None
    plan = state['plan']; latest = state['latest'] or {}
    assert len(plan['calculations']) == count
    missing = sorted(set(plan['calculations']) - set(latest))
    assert all('icxusdt' in key for key in missing), 'unexpected missing observation'
    freshness = {}
    readiness = collections.Counter()
    for key, point in latest.items():
        interval = plan['calculations'][key]['id']['interval']
        limit = {'1min': 180000, '5min': 660000, '15min': 1860000}[interval]
        age = now - point['ts_ms']
        assert -10000 < age < limit, 'stale factor: ' + key
        freshness[interval] = max(freshness.get(interval, 0), age)
        fields = point['fields']
        ready = bool(fields) and all(field['ready'] for field in fields.values())
        readiness[interval + (':ready' if ready else ':unready')] += 1
    tables = state['position_tables']
    assert tables['publication_error'] is None
    for publication in tables['manager_publications']:
        assert publication['error'] is None and not publication['pending_strategies']
        assert publication['last_success_ts_us'] >= installed['cutover_ts_us']
    if root == ops.PREP:
        active = [t for t in tables['tables'] if not t['retired']]
        assert len(active) == 12 and {t['strategy_name'] for t in active if t['strategy_group'] == 'prep02'} == names
        assert all(t['enabled'] and t['max_pos_u'] == 100 for t in active if t['strategy_group'] == 'prep02')
        empty = next(t for t in active if t['strategy_name'] == 'cta-top-20-prep02-bn-5min-pos')
        assert not empty['targets']
        assert all(not t['enabled'] and t['retired'] for t in tables['tables'] if t['strategy_name'] in ops.OLD)
    workers[root.name] = {'generation': state['generation'], 'warming': False, 'calculation_count': count,
        'latest_count': len(latest), 'missing': missing, 'readiness': dict(readiness),
        'max_point_age_ms': freshness, 'errors': [], 'publications_acknowledged': True}
receipt['workers'] = workers
r = ops.client(); prefix = ops.SOURCE + ':binance-futures:batch_exec:'
assert set(json.loads(r.get(prefix + 'strategy_names'))) == names
for name in names:
    config = json.loads(r.get(prefix + name)); assert config['algorithm'] == 'grid'
assert ops.OLD <= set(json.loads(r.get(prefix + 'removed_strategy_names')))
for name in ops.OLD:
    config = json.loads(r.get(prefix + name))
    assert all(target['qty'] in [0, 'clear'] for target in config['targets'].values())
_, execution = ops.snapshot()
assert execution['position_ready'] and 0 <= now - execution['ts_ms'] < 15000
assert all(row['strategy_name'] not in ops.OLD for row in execution['rows'])
with ops.session('lgq') as api:
    account = api('/manager/api/catalog/accounts/' + ops.SOURCE)
    assert {b['position_strategy_name'] for b in account['bindings']} == names
    assert all(b['shares'] == 1 and b['order_strategy_name'] == 'lgq_grid_prep' for b in account['bindings'])
    assert not account['pending_publishes']
    catalog = {p['strategy_name']:p for p in api('/manager/api/catalog/position-strategies')}
    assert names <= catalog.keys() and not ops.OLD & catalog.keys()
receipt['execution'] = {'position_ready': True, 'strategy_count': 9, 'old_active_strategies': 0,
    'algorithm': 'grid', 'template': 'lgq_grid_prep', 'shares': 1, 'pending_publishes': 0}
verification = ops.read(AUDIT / 'nav-verification.json')
assert verification['before']['account'] == verification['after']['account']
assert all(w['account_unchanged'] for w in verification['windows'])
receipt['migration'] = {'account_quantity_conserved': True, 'historical_pnl_rewritten': False,
    'pnl_windows_verified': [w['window'] for w in verification['windows']], 'cutover_ownership_applied': True}
before = ops.read(AUDIT / 'before.json')
unchanged = []
for path, digest in before['config_sha256'].items():
    if path.endswith('/env.sh') or path.endswith('.toml'):
        assert ops.sha(Path(path)) == digest, 'operator environment or main config changed'
        unchanged.append(Path(path).parent.name + '/' + Path(path).name)
receipt['env_and_main_configs_unchanged'] = unchanged
manifest = ops.read(AUDIT / 'release_manifest.current.json')
for root, binaries in [(ops.PREP, ['cta-prep-binance-futures', 'cta-factor-config', 'cta-symbol-group-migrate']),
    (ops.SPREAD, ['cta-factor-spread']), (ops.H/'delist_risk_server', ['delist_risk_server'])]:
    for binary in binaries:
        assert ops.sha(root/binary) == manifest['sha256'][binary], 'installed artifact differs'
receipt['installed_sha256_verified'] = True
processes=[]
for p in Path('/proc').glob('[0-9]*'):
    try:
        cwd=(p/'cwd').resolve()
        if cwd not in [ops.PREP, ops.SPREAD, ops.H/ops.SOURCE, ops.H/'delist_risk_server', ops.M]:continue
        comm=(p/'comm').read_text().strip()
        cpus=next(line.split(':',1)[1].strip() for line in (p/'status').read_text().splitlines() if line.startswith('Cpus_allowed_list:'))
        processes.append({'pid':int(p.name),'env':cwd.name,'comm':comm,'cpus':cpus})
    except (OSError,StopIteration):pass
receipt['processes']=processes
for process in processes:
    if process['comm'] in ['cta-prep-binanc', 'cta-factor-spre']:
        statuses = list((Path('/proc')/str(process['pid'])/'task').glob('*/status'))
        assert statuses
        assert all(next(line.split(':',1)[1].strip() for line in status.read_text().splitlines() if line.startswith('Cpus_allowed_list:')) == process['cpus'] for status in statuses)
receipt['all_factor_threads_cpu_verified'] = True
baseline = before['processes']
current = ops.protected_processes()
unchanged_ids = ['3936910','3684036','3685214','3685348']
assert all(baseline[pid] == current.get(pid) for pid in unchanged_ids), 'protected Manager/RX03 process changed'
receipt['protected_rx03_process_identities_unchanged'] = [int(pid) for pid in unchanged_ids]
assert {2097112, 3685475} <= {p['pid'] for p in processes}
receipt['manager_and_rx03_config_pids_verified'] = [2097112, 3685475]
receipt['concurrent_bitget_process_changes'] = sum(row != current.get(pid) and Path(row['cwd']).name in ['bitget_fr_arb01','bitget_fr_arb02','bitget_fr_arb03'] for pid,row in baseline.items())
for env,comm in [(ops.PREP.name,'cta-prep-binanc'),(ops.SPREAD.name,'cta-factor-spre'),(ops.SOURCE,'exec-pre-trade'),(ops.SOURCE,'trade_signal'),('delist_risk_server','delist_risk_ser')]:
    assert len([p for p in processes if p['env']==env and p['comm']==comm])==1, 'missing or duplicate process'
assert next(p for p in processes if p['comm']=='cta-prep-binanc')['cpus']=='17'
assert next(p for p in processes if p['comm']=='cta-factor-spre')['cpus']=='18'
ops.save(AUDIT/'verified-public.json',receipt)
print(json.dumps(receipt,ensure_ascii=False,indent=2))
