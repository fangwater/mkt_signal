"""Activate the existing JP OKX/Binance spread with its deployed APIs."""
import copy
import json
import os
from pathlib import Path
import sys
import time
import urllib.request

H = Path.home()
M = H / 'crypto_cta_manager'
ROOT = H / 'cta-factor-spread'
SOURCES = ('okex_exec_trade01', 'binance_exec_spread01')
STRATEGY = 'cta-spread01-okx-bn-1min-pos'
COMBO = 'okex-futures_binance-futures_1min'
AUDIT = M / 'maintenance-backups/cta_spread_live_20261103T090000Z'
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session, sql


def require(value, message):
    if not value:
        raise RuntimeError(message)


def api(path, payload=None):
    req = urllib.request.Request('http://127.0.0.1:19220' + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(req, timeout=30) as response:
        return json.load(response)


def catalog():
    return api('/api/config?worker=cta-factor-spread')


def bindings():
    return json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM cta_account_strategy_bindings t WHERE source_id IN ('okex_exec_trade01','binance_exec_spread01')"))


def state():
    return json.loads((ROOT / 'data/cta_factor_spread/state.json').read_text())


def save(name, value):
    path = AUDIT / name
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    path.chmod(0o600)


def processes():
    found = {}
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            cwd = (proc / 'cwd').resolve()
            if cwd in [H / x for x in SOURCES]:
                name = (proc / 'exe').resolve().name
                if name in ('trade_engine', 'exec-pre-trade', 'trade_signal'):
                    require((cwd.name, name) not in found, 'duplicate process')
                    found[(cwd.name, name)] = int(proc.name)
        except OSError:
            continue
    return found


def prepare():
    AUDIT.mkdir(mode=0o700, exist_ok=False)
    current = catalog()
    require(current['unique_factor_count'] == 50, 'unexpected factor plan')
    combo = current['plan']['combinations'][COMBO]
    require(not combo['signal']['enabled'], 'already enabled')
    require(combo['signal']['max_pos_u'] == 100, 'unexpected sizing')
    require(len(combo['factor_rules']) == 34, 'unexpected rules')
    b = bindings()
    require(len(b) == 2 and {x['source_id'] for x in b} == set(SOURCES), 'binding scope differs')
    require(all(x['shares'] == 0 and x['position_strategy_name'] == STRATEGY and x['binding_name'] == STRATEGY and x['order_strategy_name'] == 'lgq_grid_spread' for x in b), 'unexpected bindings')
    rows = json.loads(sql("SELECT coalesce(jsonb_agg(targets), '[]'::jsonb) FROM cta_position_strategies WHERE strategy_name='cta-spread01-okx-bn-1min-pos'"))
    require(len(rows) == 1 and len(rows[0]) == 16 and all(x['qty'] == 0 for x in rows[0].values()), 'existing manager targets are not zero')
    require(len(processes()) == 6, 'execution processes missing')
    candidate = copy.deepcopy(combo)
    candidate['signal']['enabled'] = True
    preview = api('/api/preview', {'worker': current['worker'], 'combination': candidate, 'expected_revision': current['revision']})
    require(preview['unique_factor_count'] == 50, 'enable changes factor plan')
    save('before.json', {'catalog': current, 'bindings': b, 'processes': {f'{a}/{b}': pid for (a,b),pid in processes().items()}, 'at_ms': time.time_ns() // 1000000})
    print('Prepared: two zero-share bindings, zero targets, six execution processes and enable preview verified.', flush=True)


def enable():
    old = json.loads((AUDIT / 'before.json').read_text())
    current = catalog()
    require(current['revision'] == old['catalog']['revision'], 'configuration changed since preflight')
    def binding_values(rows):
        keys = ('source_id', 'binding_name', 'position_strategy_name', 'order_strategy_name', 'shares')
        return sorted(tuple(row[k] for k in keys) for row in rows)
    require(binding_values(bindings()) == binding_values(old['bindings']), 'bindings changed since preflight')
    require(len(processes()) == 6, 'execution processes missing')
    s = state()
    require(not s['warming'] and s['generation'] and not s['error'] and not s['positions_error'], 'factor replay not healthy')
    require(not s['position_tables']['publication_error'], 'publication error')
    spreads = [x for k, x in s['latest'].items() if k.endswith('-spread')]
    require(len(spreads) == 8, 'missing spread outputs')
    fresh_ready = [x for x in spreads if x['fields']['value']['ready'] and
                   0 <= time.time_ns() // 1000000 - x['ts_ms'] < 180000]
    require(fresh_ready, 'no fresh and ready spread outputs')
    print(f'Fresh ready spread outputs: {len(fresh_ready)}/8', flush=True)
    with session('lgq') as manager:
        for source in SOURCES:
            b = next(x for x in old['bindings'] if x['source_id'] == source)
            payload = {k: b[k] for k in ('binding_name', 'position_strategy_name', 'order_strategy_name')}
            payload['shares'] = 1.0
            manager('/manager/api/catalog/accounts/' + source + '/bindings', payload, 'POST')
            print('shares=1: ' + source, flush=True)
    b = bindings()
    require(len(b) == 2 and all(x['shares'] == 1 for x in b), 'paired binding verification failed')
    candidate = copy.deepcopy(current['plan']['combinations'][COMBO])
    candidate['signal']['enabled'] = True
    result = api('/api/combinations', {'worker': current['worker'], 'combination': candidate, 'expected_revision': current['revision']})
    require(result['plan']['combinations'][COMBO]['signal']['enabled'], 'enable did not persist')
    save('enabled.json', {'bindings': b, 'revision': result['revision'], 'at_ms': time.time_ns() // 1000000})
    print('Paired bindings and signal publication enabled.', flush=True)


def verify():
    current = catalog()
    b = bindings()
    s = state()
    require(current['plan']['combinations'][COMBO]['signal']['enabled'], 'configuration not enabled')
    require(len(b) == 2 and all(x['shares'] == 1 for x in b), 'bindings not paired')
    require(len(processes()) == 6, 'execution process missing')
    require(not s['warming'] and not s['error'] and not s['positions_error'], 'runtime error')
    table = s['position_tables']['tables'][0]
    require(table['enabled'], 'runtime has not loaded enabled config')
    require(not s['position_tables']['publication_error'], 'publication failed')
    pubs = s['position_tables']['manager_publications']
    require(all(p['last_success_ts_us'] and not p['error'] for p in pubs), 'manager publication not yet successful')
    latest = s['latest']
    ready = sum(all(f['ready'] for f in x['fields'].values()) for x in latest.values())
    spreads = [x for k, x in latest.items() if k.endswith('-spread')]
    result = {'bindings_shares': 1, 'enabled': True, 'warming': s['warming'], 'generation': s['generation'], 'factors_ready': ready, 'factors_total': len(latest), 'spreads_ready': sum(x['fields']['value']['ready'] for x in spreads), 'spread_total': len(spreads), 'publications': pubs, 'processes': {f'{a}/{b}': pid for (a,b),pid in processes().items()}, 'checked_ms': time.time_ns() // 1000000}
    save('verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    os.umask(0o077)
    require(sys.argv[1] in ('prepare', 'enable', 'verify'), 'unknown phase')
    if sys.argv[1] == 'enable':
        require(sys.argv[2:] == ['--execute'], 'enable requires --execute')
    globals()[sys.argv[1]]()
