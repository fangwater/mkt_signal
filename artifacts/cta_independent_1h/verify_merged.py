"""Observe the final merged deployment and save a credential-free receipt."""
import collections
import hashlib
import json
import os
from pathlib import Path
import sys
import time
import tomllib
import urllib.request

sys.path.insert(0, '/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
import ops

AUDIT = Path('/home/ubuntu/cta-hourly-history/merged-deployment')
MAIN = Path('/home/ubuntu/cta-prep-binance-futures')


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    state = ops.state()
    before = ops.read(AUDIT / 'before.json')
    activated = ops.read(AUDIT / 'activated.json')
    destinations = activated['destinations']
    expected_generation = '6297f72e-fe66-4265-a8b0-c92249807d8b'
    ops.require(state['generation'] == expected_generation and not state['warming'], 'generation changed or warming')
    ops.require(not any(state[k] for k in ['error', 'positions_error', 'pending_plan']), 'worker has errors or pending plan')
    ops.require(time.time_ns() // 1000000 - state['updated_ms'] < 15000, 'worker status stale')
    tables = state['position_tables']
    ops.require(not tables['publication_error'], 'position publication failed')
    ops.require(all(not p['error'] and not p['pending_strategies'] for p in tables['manager_publications']), 'Manager publish pending')
    combos = state['plan']['combinations']
    hourly = {k: c for k, c in combos.items() if c['interval'] == '1h'}
    ops.require(len(combos) == 13 and len(hourly) == 4, 'combination scope changed')
    ops.require(sum(len(c['factor_rules']) for c in hourly.values()) == 326, 'rule scope changed')
    for c in hourly.values():
        ops.require(c['signal']['enabled'] and c['signal']['max_pos_u'] == (50 if c['strategy_group'] == 'prep01' else 100), 'hourly activation differs')
    for p, digest in before['main_json'].items():
        ops.require(sha(Path(p)) == digest, 'existing minute config changed: ' + p)
    ready = collections.defaultdict(lambda: {'total': 0, 'ready': 0, 'unready': []})
    for name, calc in state['plan']['calculations'].items():
        counts = ready[calc['id']['interval']]
        counts['total'] += 1
        point = state['latest'].get(name)
        if point and all(f['ready'] for f in point['fields'].values()):
            counts['ready'] += 1
        else:
            counts['unready'].append(name)
    ops.require(ready['1h']['total'] == 265 and ready['1h']['unready'] == ['binance-futures-dashusdt-1h-depth_imb_bps1'], 'hourly readiness changed')
    processes = []
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            comm = (proc / 'comm').read_text().strip()
            if comm not in ['cta-prep-binanc', 'cta-factor-conf'] or (proc / 'cwd').resolve() != MAIN:
                continue
            processes.append({'pid': int(proc.name), 'comm': comm, 'cpus': sorted(os.sched_getaffinity(int(proc.name)))})
        except (OSError, ProcessLookupError):
            pass
    workers = [p for p in processes if p['comm'] == 'cta-prep-binanc']
    ops.require(len(workers) == 1 and workers[0]['pid'] == 2168358 and workers[0]['cpus'] == [17], 'main worker process or CPU changed')
    ops.require(not Path('/home/ubuntu/cta-prep-binance-futures-1h').exists(), 'temporary hourly environment still active')
    config = tomllib.loads((ops.M / 'config/cta-manager.toml').read_text())
    redis = ops.client()
    accounts = {}
    snapshots = {}
    with ops.session('lgq') as api:
        for source in sorted(set(destinations.values())):
            account = api('/manager/api/catalog/accounts/' + source)
            bindings = account['bindings']
            kept = [b for b in bindings if b['position_strategy_name'] not in destinations]
            ops.require(kept == before['accounts'][source]['bindings'], 'original bindings changed: ' + source)
            added = [b for b in bindings if b['position_strategy_name'] in destinations]
            ops.require(len(bindings) == (4 if source.endswith('rx01') else 9), 'binding count differs')
            ops.require(all(destinations[b['position_strategy_name']] == source and b['shares'] == 1 and b['order_strategy_name'] == 'lgq_grid_prep' for b in added), 'hourly binding differs')
            ops.require(not account['pending_publishes'], 'account publication pending')
            source_config = next(s for s in config['sources'] if s['id'] == source)
            url = source_config['exec_viz_url'].rstrip('/') + '/snapshot'
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(url, timeout=20) as response:
                snapshot = json.load(response)
            execution = next(e['entry'] for e in snapshot['entries'] if e['channel'] == 'exec_pre_trade_state')
            ops.require(execution['position_ready'] and 0 <= time.time_ns() // 1000000 - execution['ts_ms'] < 20000, 'Exec unavailable')
            snapshots[source] = execution
            installed = []
            for strategy in [n for n, s in destinations.items() if s == source]:
                prefix = source + ':binance-futures:batch_exec:'
                index = json.loads(redis.get(prefix + 'strategy_names') or '[]')
                raw = redis.get(prefix + strategy)
                stored = json.loads(raw) if raw else None
                ops.require(strategy in index and stored and stored['algorithm'] == 'grid', 'hourly Exec configuration missing')
                installed.append({'strategy': strategy, 'algorithm': stored['algorithm'], 'target_symbols': len(stored['targets']), 'indexed': True})
            accounts[source] = {'bindings': bindings, 'hourly_exec_configs': installed, 'exec_ready': True, 'pending_publishes': 0}
    retired = {f'cta-{p}-prep02-bn-1min-pos' for p in ['top-5', 'top-20', 'last-20']}
    rows = [r for r in snapshots['binance_exec_rx03']['rows'] if r['strategy_name'] in retired]
    ops.require(all(r['execution_complete'] and not r['active_batches'] and all(abs(r[k]) < 1e-10 for k in ['target_qty', 'pending_qty', 'live_order_qty']) for r in rows), 'RX03 minute retirement not complete')
    residuals = [{k: r[k] for k in ['strategy_name', 'symbol', 'current_qty', 'current_usdt', 'completion_reason']} for r in rows if abs(r['current_qty']) > 1e-10]
    ops.require(all(r['completion_reason'] == 'exchange_minimum' for r in residuals), 'unexplained retired residual')
    ops.require(not any(c['strategy_group'] == 'prep02' and c['interval'] == '1min' for c in combos.values()), 'RX03 minute configuration reintroduced')
    ops.require(not any(b['position_strategy_name'] in retired for b in accounts['binance_exec_rx03']['bindings']), 'RX03 minute binding reintroduced')
    backups = list((AUDIT / 'original/cta-prep-binance-futures/data/cta_prep_binance_futures/positions').glob('*.json'))
    ops.require(backups and all((MAIN / 'data/cta_prep_binance_futures/positions' / p.name).exists() for p in backups), 'previous ledger file missing')
    receipt = {
        'verified_at_ms': time.time_ns() // 1000000, 'env': str(MAIN), 'worker_generation': state['generation'],
        'warming': False, 'processes': processes, 'config_server_port': 19220, 'separate_hourly_process': False,
        'additional_dedicated_cpu': False, 'calculations': len(state['plan']['calculations']), 'readiness': ready,
        'hourly_rules': 326, 'hourly_tables_enabled': 4, 'amounts': activated['amounts'],
        'next_hourly_decision_utc': '2026-11-09T15:00:00Z', 'historical_entries_forced': False,
        'publication_error': None, 'manager_pending': 0, 'accounts': accounts,
        'existing_minute_config_hashes_unchanged': True, 'existing_bindings_unchanged': True,
        'backed_up_ledger_files_retained': len(backups), 'rx03_1min': {'configurations': 0, 'bindings': 0,
            'active_batches': 0, 'nonzero_targets': 0, 'pending_or_live_orders': 0,
            'residual_rows': len(residuals), 'residual_abs_usdt': sum(abs(r['current_usdt']) for r in residuals), 'residuals': residuals},
        'audit': str(AUDIT), 'worker_sha256': sha(MAIN / 'cta-prep-binance-futures'),
        'checks': {'cta_library': 69, 'cta_config': 4, 'hourly_lifecycle': 1, 'cargo_fmt': True},
    }
    ops.save(AUDIT / 'verified.json', receipt)
    print(json.dumps({'verified': True, 'hourly_enabled': 4, 'readiness': {k: {'ready': v['ready'], 'total': v['total']} for k, v in ready.items()}, 'residual_rows': len(residuals), 'residual_abs_usdt': receipt['rx03_1min']['residual_abs_usdt']}))


if __name__ == '__main__':
    main()
