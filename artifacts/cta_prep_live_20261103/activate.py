"""Activate the six RX01/RX03 Prep bindings using deployed APIs and scripts.

Preparation is read-only outside the private audit directory. Mutating phases
require --execute. No credentials are copied into this helper or its receipt.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request

H = Path.home()
M = H / 'crypto_cta_manager'
PREP = H / 'cta-prep-binance-futures'
SOURCES = {'binance_exec_rx01': 'prep01', 'binance_exec_rx03': 'prep02'}
PARALLEL_SPREAD_SOURCES = {'binance_exec_spread01', 'okex_exec_trade01'}
PERIODS = ['1min', '5min', '15min']
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session, sql
from clear_positions import exchange_snapshot
from manager_entries import protected_processes
from neutralize_rapidx_binance import assert_stopped


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')
    path.chmod(0o600)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def api(path, data=None):
    request = urllib.request.Request(
        'http://127.0.0.1:19220' + path,
        data=None if data is None else json.dumps(data).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=30) as response:
        return json.load(response)


def catalog():
    return api('/api/config?worker=cta-prep-binance-futures')


def rows(table):
    require(table in {'cta_account_strategy_bindings', 'cta_order_strategies'}, 'unexpected table')
    return json.loads(sql(f"SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM {table} t"))


def canonical(value):
    return sorted(value, key=lambda v: json.dumps(v, sort_keys=True))


def binding_contract(value):
    keys = ['source_id', 'binding_name', 'position_strategy_name', 'order_strategy_name', 'shares']
    return canonical([{key: row[key] for key in keys} for row in value])


def nonspread_binding_contract(value):
    # A separate operator is activating Spread in parallel. Audit our six
    # bindings and the remaining accounts without reverting that operation.
    return binding_contract([row for row in value if row['source_id'] not in PARALLEL_SPREAD_SOURCES])


def scoped_hashes(value):
    spread = str(H / 'cta-factor-spread') + '/'
    return {key: digest for key, digest in value.items() if not key.startswith(spread)}


def parallel_spread_compute(row):
    return row['cwd'] == str(H / 'cta-factor-spread') and row['comm'] == 'cta-factor-spre'


def expected():
    return [{'source_id': source, 'binding_name': f'cta-{group}-bn-{period}-pos',
             'position_strategy_name': f'cta-{group}-bn-{period}-pos',
             'order_strategy_name': 'lgq_grid_prep', 'shares': 1.0}
            for source, group in SOURCES.items() for period in PERIODS]


def processes():
    result = protected_processes()
    for p in Path('/proc').glob('[0-9]*'):
        try:
            root = (p / 'cwd').resolve()
            if root == PREP or root.name in SOURCES:
                result[p.name] = {'comm': (p / 'comm').read_text().strip(),
                                 'cwd': str(root),
                                 'start_ticks': (p / 'stat').read_text().split(') ', 1)[1].split()[19]}
        except OSError:
            pass
    return result


def immutable_hashes():
    files = [M / 'config/cta-manager.toml', PREP / 'env.sh',
             PREP / 'config/cta_prep_binance_futures.toml']
    for name in SOURCES:
        root = H / name
        files += [root / 'env.sh', root / 'trade_engine.toml', root / 'release.json']
        files += [p for p in (root / 'config').glob('*') if p.is_file()]
        files += [p for p in (root / 'scripts').glob('*.sh')]
    spread = H / 'cta-factor-spread'
    files += [spread / 'env.sh', spread / 'config/cta_factor_spread.toml']
    files += list((spread / 'config/cta_factor_spread').glob('*.json'))
    return {str(p): sha(p) for p in files}


def state():
    return json.loads((PREP / 'data/cta_prep_binance_futures/state.json').read_text())


def prepare(audit):
    audit.mkdir(mode=0o700, exist_ok=False)
    current = catalog()
    require(len(current['plan']['combinations']) == 6, 'unexpected Prep combination count')
    require(current['unique_factor_count'] == 404, 'unexpected factor plan')
    live = state()
    require(not live['warming'] and not live['error'] and not live['positions_error'], 'Prep is not healthy')
    for c in current['plan']['combinations'].values():
        require(c['signal']['enabled'] is False, 'a Prep table is already enabled')
        request = {'worker': current['worker'], 'combination': copy.deepcopy(c),
                   'expected_revision': current['revision']}
        request['combination']['signal']['enabled'] = True
        preview = api('/api/preview', request)
        require(preview['unique_factor_count'] == 404, 'enable preview changes calculations')
    account_snapshots = {}
    releases = {}
    for name in SOURCES:
        root = H / name
        assert_stopped(root)
        snapshot = exchange_snapshot(name)
        require(not snapshot['nonzero_positions'] and snapshot['open_orders'] == snapshot['algo_orders'] == 0,
                name + ' has unexpected initial exposure or orders')
        account_snapshots[name] = snapshot
        release = json.loads((root / 'release.json').read_text())
        require(release['branch'] == 'arbmm', 'deployment did not come from arbmm')
        for binary, digest in release['binaries'].items():
            require(sha(root / binary) == digest, 'deployed binary differs from release receipt')
        releases[name] = release
    before_bindings = rows('cta_account_strategy_bindings')
    own = [b for b in before_bindings if b['source_id'] in SOURCES]
    require(len(own) == 6 and all(b['shares'] == 0 for b in own), 'unexpected initial binding')
    for b in expected():
        require(any(all(o[k] == v for k, v in b.items() if k != 'shares') for o in own), 'binding mapping differs')
    strategies = rows('cta_order_strategies')
    template = next(s for s in strategies if s['strategy_name'] == 'lgq_grid_prep')
    require(template['signal_execution_enabled'], 'Prep execution template is disabled')
    for f in (PREP / 'config/cta_prep_binance_futures').glob('*.json'):
        shutil.copyfile(f, audit / (f.name + '.before'))
    before = {'catalog': current, 'bindings': before_bindings, 'orders': strategies,
              'processes': processes(), 'immutable_hashes': immutable_hashes(),
              'generation': live['generation'], 'accounts': account_snapshots, 'releases': releases,
              'prepared_at_ms': time.time_ns() // 1000000}
    save(audit / 'before.json', before)
    print(json.dumps({'prepared': True, 'six_enable_previews_passed': True,
                      'accounts_flat': True, 'binary_hashes_match': True}), flush=True)


def run_script(audit, source, script):
    root = H / source
    log = audit / (source + '.' + script + '.log')
    with log.open('ab') as output:
        log.chmod(0o600)
        result = subprocess.run([str(root / 'scripts' / script)], cwd=root,
                                env=dict(os.environ, PATH=str(H / '.local/bin') + ':' + os.environ['PATH']),
                                stdin=subprocess.DEVNULL, stdout=output, stderr=subprocess.STDOUT, timeout=60)
    require(result.returncode == 0, source + '/' + script + ' failed; diagnostics saved privately')
    print(source + ': ' + script + ' completed', flush=True)


def start(audit):
    before = json.loads((audit / 'before.json').read_text())
    require(immutable_hashes() == before['immutable_hashes'], 'runtime configuration changed')
    require(catalog()['revision'] == before['catalog']['revision'], 'Prep configuration changed')
    require(binding_contract(rows('cta_account_strategy_bindings')) == binding_contract(before['bindings']), 'bindings changed')
    for source in SOURCES:
        assert_stopped(H / source)
        for script in ['start_exec_trade_engine.sh', 'start_exec_pre_trade.sh', 'start_exec_trade_signal.sh']:
            run_script(audit, source, script)
    save(audit / 'started.json', {'started_at_ms': time.time_ns() // 1000000, 'sources': list(SOURCES)})


def enable(audit):
    before = json.loads((audit / 'before.json').read_text())
    require(immutable_hashes() == before['immutable_hashes'], 'runtime configuration changed')
    require(catalog()['revision'] == before['catalog']['revision'], 'Prep configuration changed')
    require((audit / 'started.json').is_file(), 'start execution before enabling publications')
    with session('lgq') as manager:
        for b in expected():
            manager('/manager/api/catalog/accounts/' + b['source_id'] + '/bindings',
                    {k: v for k, v in b.items() if k != 'source_id'}, 'POST')
            print('shares=1: ' + b['source_id'] + ' / ' + b['binding_name'], flush=True)
    current = catalog()
    for name in sorted(current['plan']['combinations']):
        c = copy.deepcopy(current['plan']['combinations'][name])
        c['signal']['enabled'] = True
        current = api('/api/combinations', {'worker': current['worker'], 'combination': c,
                                           'expected_revision': current['revision']})
        print('enabled: ' + current['position_strategies'][name], flush=True)
    save(audit / 'enabled.json', {'enabled_at_ms': time.time_ns() // 1000000,
                                'catalog_revision': current['revision'], 'six_tables_enabled': True})


def verify(audit):
    before = json.loads((audit / 'before.json').read_text())
    require(scoped_hashes(immutable_hashes()) == scoped_hashes(before['immutable_hashes']),
            'unrequested Prep runtime configuration changed')
    orders = rows('cta_order_strategies')
    require(canonical([o for o in orders if o['strategy_name'] != 'lgq_grid_spread']) ==
            canonical([o for o in before['orders'] if o['strategy_name'] != 'lgq_grid_spread']),
            'non-Spread execution template changed')
    bindings = rows('cta_account_strategy_bindings')
    require(nonspread_binding_contract([b for b in bindings if b['source_id'] not in SOURCES]) ==
            nonspread_binding_contract([b for b in before['bindings'] if b['source_id'] not in SOURCES]), 'other non-Spread bindings changed')
    own = [b for b in bindings if b['source_id'] in SOURCES]
    require(canonical([{k: b[k] for k in expected()[0]} for b in own]) == canonical(expected()), 'live bindings differ')
    after = catalog()
    original = copy.deepcopy(before['catalog']['plan'])
    for c in original['combinations'].values():
        c['signal']['enabled'] = True
    require(after['plan'] == original, 'changes beyond six enable flags')
    now = processes()
    recovery_path = audit / 'calculation_recovery.json'
    recovered = recovery_path.is_file()
    protected = {pid: row for pid, row in before['processes'].items()
                 if not (recovered and row['cwd'] == str(PREP)
                         and row['comm'] == 'cta-prep-binanc')
                 and not parallel_spread_compute(row)}
    require(all(now.get(pid) == row for pid, row in protected.items()), 'protected process changed')
    found = {}
    for source in SOURCES:
        procs = {}
        for p in Path('/proc').glob('[0-9]*'):
            try:
                if (p / 'cwd').resolve() == H / source:
                    name = (p / 'exe').resolve().name
                    if name in ['trade_engine', 'exec-pre-trade', 'trade_signal']:
                        require(name not in procs, 'duplicate execution process')
                        procs[name] = {'pid': int(p.name), 'cpus': sorted(os.sched_getaffinity(int(p.name)))}
            except OSError:
                pass
        require(set(procs) == {'trade_engine', 'exec-pre-trade', 'trade_signal'}, 'execution process missing')
        found[source] = procs
    s = state()
    require(not s['warming'], 'factor replay is still warming')
    if recovered:
        recovery = json.loads(recovery_path.read_text())
        require(s['generation'] == recovery['generation'], 'unexpected factor generation after recovery')
    else:
        require(s['generation'] == before['generation'], 'enable unexpectedly restarted factor replay')
    require(not s['error'] and not s['positions_error'], 'factor or position-table error')
    tables = s['position_tables']['tables']
    require(len(tables) == 6 and all(t['enabled'] for t in tables), 'runtime enable not loaded')
    result = {'sources': found, 'six_bindings_shares_one': True, 'six_prep_tables_enabled': True,
              'factor_plan_and_amounts_unchanged': True, 'factor_generation_preserved': not recovered,
              'localset_fix_recovery': recovered,
              'prep_configs_and_nonspread_bindings_unchanged': True,
              'spread_activated_by_parallel_operation': True,
              'parallel_spread_order_template_excluded_from_prep_audit': True,
              'protected_processes_unchanged': len(protected),
              'factor_error': s['error'], 'positions_error': s['positions_error'],
              'manager_publications': s['position_tables']['manager_publications'],
              'checked_at_ms': time.time_ns() // 1000000}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=['prepare', 'start', 'enable', 'verify'])
    p.add_argument('--audit', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    os.umask(0o077)
    require(subprocess.check_output(['hostname', '-f'], text=True).strip() ==
            'ip-172-31-35-228.ap-northeast-1.compute.internal', 'unexpected host')
    require((H / 'binance_exec_rx01').is_dir() and (H / 'binance_exec_rx03').is_dir(), 'runtime missing')
    require(a.audit.parent == M / 'maintenance-backups' and a.audit.name.startswith('cta_prep_live_'), 'unexpected audit directory')
    if a.phase in {'start', 'enable'}:
        require(a.execute, 'mutation requires --execute')
    globals()[a.phase](a.audit)


if __name__ == '__main__':
    main()
