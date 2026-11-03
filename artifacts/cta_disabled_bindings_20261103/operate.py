"""Restore RX03 monitoring and save the requested CTA bindings with zero shares.

Run prepare before apply --execute. Only Manager is restarted; exchange order
APIs and Exec lifecycle scripts are never invoked. Private backups stay on JP.
"""
import argparse
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import time
import tomllib
from urllib.parse import urlsplit

H = Path.home()
M = H / 'crypto_cta_manager'
CONFIG = M / 'config/cta-manager.toml'
SERVICE = 'crypto-cta-manager-web.service'
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session, sql
from manager_entries import protected_processes
from neutralize_rapidx_binance import assert_stopped

PLAN = [
    {'source_id': source, 'binding_name': name, 'position_strategy_name': name,
     'order_strategy_name': 'lgq_grid_prep', 'shares': 0.0}
    for source, group in [('binance_exec_trade06', 'prep01'), ('binance_exec_trade07', 'prep02')]
    for period in ['1min', '5min', '15min']
    for name in [f'cta-{group}-bn-{period}-pos']
] + [
    {'source_id': source, 'binding_name': 'cta-spread01-okx-bn-1min-pos',
     'position_strategy_name': 'cta-spread01-okx-bn-1min-pos',
     'order_strategy_name': 'lgq_grid_spread', 'shares': 0.0}
    for source in ['okex_exec_trade01', 'binance_exec_spread01']
]
SOURCES = {r['source_id'] for r in PLAN}
OWN = {r['position_strategy_name'] for r in PLAN}


def require(value, message):
    if not value:
        raise RuntimeError(message)


def save(path, data):
    raw = data if isinstance(data, bytes) else (json.dumps(data, indent=2) + '\n').encode()
    with path.with_suffix(path.suffix + '.next').open('wb') as f:
        os.fchmod(f.fileno(), 0o600)
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    os.replace(f.name, path)


def rows(table):
    require(table in {'cta_account_strategy_bindings', 'cta_account_follows',
                      'cta_follow_publish_queue', 'cta_order_strategies'}, 'unexpected table')
    return json.loads(sql(f"SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM {table} t"))


def canonical(value):
    return sorted(value, key=lambda r: json.dumps(r, sort_keys=True))


def processes():
    result = protected_processes()
    for p in Path('/proc').glob('[0-9]*'):
        try:
            if (p / 'cwd').resolve() == H / 'cta-prep-binance-futures':
                result[p.name] = {'comm': (p / 'comm').read_text().strip(),
                                 'cwd': str((p / 'cwd').resolve()),
                                 'start_ticks': (p / 'stat').read_text().split(') ', 1)[1].split()[19]}
        except OSError:
            pass
    return result


def safety():
    hashes = {}
    for source in SOURCES:
        assert_stopped(H / source)
        for p in [H / source / 'env.sh', *(H / source / 'config').glob('*')]:
            if p.is_file():
                hashes[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
    for name, sub in [('cta-prep-binance-futures', 'cta_prep_binance_futures'),
                      ('cta-factor-spread', 'cta_factor_spread')]:
        for p in [H / name / 'env.sh', H / name / 'config' / (sub + '.toml'),
                  *(H / name / 'config' / sub).glob('*.json')]:
            hashes[str(p)] = hashlib.sha256(p.read_bytes()).hexdigest()
        for p in (H / name / 'config' / sub).glob('*.json'):
            require(json.loads(p.read_text())['signal']['enabled'] is False, 'publishing is enabled')
    c = tomllib.loads(CONFIG.read_text())
    bnb = Path(c['kline']['rocksdb_path']).parent / 'config/bnb-auto.json'
    if bnb.exists():
        accounts = json.loads(bnb.read_text()).get('accounts', {})
        a = accounts.get('binance_exec_trade07', {})
        require(not a.get('settings', {}).get('enabled', False) and not a.get('pending'),
                'RX03 BNB automation requires review before restoring its source')
    require(json.loads(sql("SELECT to_jsonb(funding_enabled) FROM cta_order_sources WHERE source_id='binance_exec_trade07'")) is False,
            'RX03 funding collection must remain disabled')
    require(not rows('cta_account_follows') and not rows('cta_follow_publish_queue'),
            'unexpected follows or pending publications')
    return hashes


def candidate(raw):
    text = raw.decode()
    before = tomllib.loads(text)
    expected = copy.deepcopy(before)
    source = 'binance_exec_trade07'
    require(not any(s['id'] == source for s in before['sources']), 'RX03 already configured')
    backup = (M / 'maintenance-backups/rx03_rx04_manager_entries_20261103T022116Z/cta-manager.toml.before').read_text()
    headers = list(re.finditer(r'(?m)^\[.*\][ \t]*$', backup))
    blocks = []
    for i, header in enumerate(headers):
        if header.group() == '[[sources]]':
            end = headers[i + 1].start() if i + 1 < len(headers) else len(backup)
            block = backup[header.start():end]
            if tomllib.loads(block)['sources'][0]['id'] == source:
                blocks.append(block)
    require(len(blocks) == 1, 'original RX03 source block unavailable')
    restored = tomllib.loads(blocks[0])['sources'][0]
    expected['sources'].append(restored)
    expected['dashboard']['source_viewers'][source] = ['lgq']
    text += '\n' + blocks[0] + '\n'
    text, count = re.subn(r'(?m)^\[dashboard.source_viewers\]\s*$',
                         '[dashboard.source_viewers]\n' + source + ' = ["lgq"]', text)
    require(count == 1 and tomllib.loads(text) == expected, 'unexpected config change')
    return text.encode()


def install(raw):
    staged = CONFIG.with_suffix('.cta-bindings-next')
    require(not staged.exists(), 'staged config exists')
    with staged.open('xb') as f:
        os.fchmod(f.fileno(), stat.S_IMODE(CONFIG.stat().st_mode))
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    os.replace(staged, CONFIG)


def restart():
    subprocess.run(['systemctl', '--user', 'restart', SERVICE], check=True,
                   timeout=45, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def redis_read(*args):
    c = tomllib.loads(CONFIG.read_text())
    u = urlsplit(c['redis']['url'])
    require(u.hostname in {'127.0.0.1', 'localhost'}, 'unexpected Redis host')
    env = dict(os.environ)
    if u.password:
        from urllib.parse import unquote
        env['REDISCLI_AUTH'] = unquote(u.password)
    cmd = ['redis-cli', '-h', u.hostname, '-p', str(u.port or 6379), '-n', u.path.strip('/') or '0', '--raw']
    return subprocess.check_output(cmd + list(args), env=env, stderr=subprocess.DEVNULL, text=True).strip()


def risk_hashes():
    return {source: hashlib.sha256(redis_read('HGETALL', f'{source}:{venue}:pre_trade_risk_params').encode()).hexdigest()
            for source, venue in [(s, 'okex-futures' if s == 'okex_exec_trade01' else 'binance-futures') for s in SOURCES]}


def verify(audit):
    before = json.loads((audit / 'private-before.json').read_text())
    require(CONFIG.read_bytes() == (audit / 'cta-manager.toml.candidate').read_bytes(), 'live Manager config changed')
    require(safety() == before['runtime_hashes'], 'runtime configs changed')
    now = processes()
    require(all(now.get(pid) == row for pid, row in before['processes'].items()), 'protected process changed')
    bindings = rows('cta_account_strategy_bindings')
    require(canonical([b for b in bindings if b['source_id'] not in SOURCES]) == canonical(before['bindings']),
            'other account bindings changed')
    actual = [{k: b[k] for k in PLAN[0]} for b in bindings if b['source_id'] in SOURCES]
    require(canonical(actual) == canonical(PLAN), 'disabled binding plan differs')
    require(canonical(rows('cta_order_strategies')) == canonical(before['order_strategies']), 'execution templates changed')
    current_risk = risk_hashes()
    require(all(current_risk.get(k) == v for k, v in before['risk_hashes'].items()), 'risk settings changed')
    runtime = []
    c = tomllib.loads(CONFIG.read_text())
    source_map = {s['id']: s for s in c['sources']}
    for source in SOURCES:
        venue = source_map[source]['venue']
        names = json.loads(redis_read('GET', f'{source}:{venue}:batch_exec:strategy_names'))
        expected = {p['binding_name'] for p in PLAN if p['source_id'] == source}
        require(set(names) == expected, 'unexpected runtime strategy index')
        for name in names:
            payload = json.loads(redis_read('GET', f'{source}:{venue}:batch_exec:{name}'))
            require(payload['targets'] and all(v['qty'] == 0 for v in payload['targets'].values()), 'runtime target is nonzero')
            require(all(':' not in symbol for symbol in payload['targets']), 'cross venue target not projected')
            runtime.append({'source_id': source, 'strategy_name': name, 'targets': len(payload['targets']), 'nonzero_targets': 0})
    with session('lgq') as api:
        for source in SOURCES:
            studio = api('/manager/api/catalog/accounts/' + source)
            require(not studio['pending_publishes'] and all(b['shares'] == 0 for b in studio['bindings']), 'API binding enabled or pending')
        spread = api('/manager/api/catalog/accounts/cta_cross_spread_rx01')
        require(len(spread['execution_sources']) == 2 and all(len(s['bindings']) == 1 for s in spread['execution_sources']),
                'logical Spread account bindings missing')
        catalog = {p['strategy_name']: p for p in api('/manager/api/catalog/position-strategies') if p['strategy_name'] in OWN}
        require(catalog == before['catalog'], 'own position definitions changed')
    result = {'bindings': PLAN, 'unique_position_strategies': len(OWN), 'physical_bindings': len(PLAN),
              'all_shares_zero': True, 'all_seven_publishing_disabled': True,
              'four_exec_submitters_stopped': True, 'runtime': runtime,
              'other_account_bindings_unchanged': len(before['bindings']), 'execution_templates_unchanged': True,
              'runtime_configs_unchanged': True, 'protected_processes_unchanged': len(before['processes']),
              'rx03_monitoring_source_restored': True, 'rx03_funding_collection_disabled': True,
              'exchange_order_api_calls': 0, 'checked_at_ms': time.time_ns() // 1000000}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=['prepare', 'apply', 'verify'])
    p.add_argument('--audit', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    os.umask(0o077)
    audit = a.audit
    require(audit.parent == M / 'maintenance-backups' and audit.name.startswith('cta_disabled_bindings_'), 'unexpected audit path')
    if a.phase == 'verify':
        verify(audit)
        return
    if a.phase == 'prepare':
        audit.mkdir(exist_ok=False)
        raw = CONFIG.read_bytes()
        save(audit / 'cta-manager.toml.before', raw)
        save(audit / 'cta-manager.toml.candidate', candidate(raw))
        with session('lgq') as api:
            catalog = {r['strategy_name']: r for r in api('/manager/api/catalog/position-strategies') if r['strategy_name'] in OWN}
        require(set(catalog) == OWN and all(all(v['qty'] == 0 for v in p['targets'].values()) for p in catalog.values()), 'new strategy targets not zero')
        bindings = rows('cta_account_strategy_bindings')
        require(not any(b['source_id'] in SOURCES for b in bindings), 'destination already has bindings')
        pid = subprocess.check_output(['systemctl', '--user', 'show', SERVICE, '-p', 'MainPID', '--value'], text=True).strip()
        env = dict(os.environ)
        env.update(row.decode().split('=', 1) for row in Path('/proc', pid, 'environ').read_bytes().split(b'\0') if b'=' in row)
        checked = subprocess.run([str(M / 'bin/cta_web'), '--config', str(audit / 'cta-manager.toml.candidate'), '--check-allocations'],
                                 env=env, capture_output=True, timeout=45)
        require(checked.returncode == 0, 'config validation failed; credential-bearing diagnostics suppressed')
        save(audit / 'private-before.json', {'bindings': bindings, 'catalog': catalog,
             'order_strategies': rows('cta_order_strategies'), 'runtime_hashes': safety(),
             'processes': processes(), 'risk_hashes': risk_hashes()})
        print(json.dumps({'prepared': True, 'bindings': PLAN, 'config_and_allocations_valid': True}), flush=True)
        return
    require(a.execute, 'apply requires --execute')
    with (audit / 'apply.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = json.loads((audit / 'private-before.json').read_text())
        require(safety() == before['runtime_hashes'], 'runtime config changed since prepare')
        require(canonical(rows('cta_account_strategy_bindings')) == canonical(before['bindings']), 'bindings changed since prepare')
        raw = (audit / 'cta-manager.toml.before').read_bytes()
        desired = (audit / 'cta-manager.toml.candidate').read_bytes()
        require(CONFIG.read_bytes() == raw and candidate(raw) == desired, 'Manager config changed since prepare')
        try:
            install(desired)
            restart()
            deadline = time.monotonic() + 300
            progress = time.monotonic() + 20
            while True:
                try:
                    with session('lgq') as api:
                        api('/manager/api/catalog/accounts/binance_exec_trade07')
                    break
                except Exception:
                    require(time.monotonic() < deadline, 'Manager warmup timed out')
                    if time.monotonic() >= progress:
                        print('Waiting for Manager history cache and RX03 configuration API.', flush=True)
                        progress = time.monotonic() + 20
                    time.sleep(1)
        except Exception:
            require(CONFIG.read_bytes() == desired, 'concurrent Manager edit; automatic rollback halted')
            install(raw)
            restart()
            raise RuntimeError('Manager activation failed; original configuration restored') from None
        print('Manager healthy; saving zero-share bindings.', flush=True)
        receipts = []
        with session('lgq') as api:
            for row in PLAN:
                safety()
                body = {k: v for k, v in row.items() if k != 'source_id'}
                api('/manager/api/catalog/accounts/' + row['source_id'] + '/bindings', body, 'POST')
                receipts.append(row)
                save(audit / 'binding-receipts.json', receipts)
                print('Saved stopped binding: ' + row['source_id'] + ' / ' + row['binding_name'], flush=True)
        verify(audit)


if __name__ == '__main__':
    main()
