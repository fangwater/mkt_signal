"""Prepare and switch RX01/RX03 to new, empty Exec identities; never start trading.

The retired runtimes are moved intact into the private audit directory. Historical
Manager snapshots keep their original IDs and are never imported into new sources.
"""
import argparse
import base64
import copy
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import tarfile
import time
import tomllib

H = Path.home()
M = H / 'crypto_cta_manager'
C = M / 'config/cta-manager.toml'
N = Path('/etc/nginx/snippets/crypto_cta_manager.conf')
SERVICE = 'crypto-cta-manager-web.service'
PAIRS = [('binance_exec_trade06', 'binance_exec_rx01', 'prep01'),
         ('binance_exec_trade07', 'binance_exec_rx03', 'prep02')]
OLD = {old for old, _, _ in PAIRS}
NEW = {new for _, new, _ in PAIRS}
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import env_for, session, sql
from clear_positions import exchange_snapshot
from manager_entries import protected_processes
from neutralize_rapidx_binance import assert_stopped

try:
    import redis
except ImportError:
    for site in (H / '.venvs/default/lib').glob('python*/site-packages'):
        sys.path.insert(0, str(site))
    import redis


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = value if isinstance(value, bytes) else (json.dumps(value, indent=2) + '\n').encode()
    temp = path.with_name(path.name + '.next')
    with temp.open('wb') as f:
        os.fchmod(f.fileno(), 0o600)
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)


def rows(table):
    require(table in {'cta_account_strategy_bindings', 'cta_account_follows',
                      'cta_follow_publish_queue', 'cta_order_strategies'}, 'unexpected table')
    return json.loads(sql(f"SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM {table} t"))


def canonical(value):
    return sorted(value, key=lambda r: json.dumps(r, sort_keys=True))


def client():
    return redis.Redis.from_url(tomllib.loads(C.read_text())['redis']['url'])


def guarded_processes():
    result = {pid: row for pid, row in protected_processes().items()
              if Path(row['cwd']).name not in OLD | NEW}
    for p in Path('/proc').glob('[0-9]*'):
        try:
            if (p / 'cwd').resolve() == H / 'cta-prep-binance-futures':
                result[p.name] = {'comm': (p / 'comm').read_text().strip(),
                                 'cwd': str((p / 'cwd').resolve()),
                                 'start_ticks': (p / 'stat').read_text().split(') ', 1)[1].split()[19]}
        except OSError:
            pass
    return result


def factor_hashes():
    out = {}
    for name, sub in [('cta-prep-binance-futures', 'cta_prep_binance_futures'),
                      ('cta-factor-spread', 'cta_factor_spread')]:
        root = H / name
        for p in [root / 'env.sh', root / 'config' / (sub + '.toml'),
                  *(root / 'config' / sub).glob('*.json')]:
            out[str(p)] = sha(p)
        for p in (root / 'config' / sub).glob('*.json'):
            require(json.loads(p.read_text())['signal']['enabled'] is False, 'publishing enabled')
    return out


def run_script(root, name):
    e = dict(os.environ, PATH=str(H / '.local/bin') + ':' + os.environ['PATH'])
    p = subprocess.run([str(root / 'scripts' / name)], cwd=root, env=e,
                       capture_output=True, timeout=60)
    require(p.returncode == 0, f'{root.name}/{name} failed; private diagnostics suppressed')
    print(root.name + ': ' + name + ' completed', flush=True)


def flat(source):
    assert_stopped(H / source)
    s = exchange_snapshot(source)
    require(not s['nonzero_positions'] and s['open_orders'] == s['algo_orders'] == 0,
            source + ' has perpetual exposure or orders')
    return s


def expected_binding(new, group, period):
    name = f'cta-{group}-bn-{period}-pos'
    return {'source_id': new, 'binding_name': name, 'position_strategy_name': name,
            'order_strategy_name': 'lgq_grid_prep', 'shares': 0.0}


def replacement(raw):
    text = raw.decode()
    before = tomllib.loads(text)
    expected = copy.deepcopy(before)
    for old, new, _ in PAIRS:
        require(sum(s['id'] == old for s in before['sources']) == 1, 'old source missing')
        require(not any(s['id'] == new for s in before['sources']), 'new source already exists')
        text = text.replace(old, new)
    def replace(v):
        if isinstance(v, dict):
            return {replace(k): replace(x) for k, x in v.items()}
        if isinstance(v, list):
            return [replace(x) for x in v]
        if isinstance(v, str):
            for old, new, _ in PAIRS:
                v = v.replace(old, new)
        return v
    expected = replace(expected)
    require(tomllib.loads(text) == expected, 'unexpected Manager changes')
    return text.encode()


def install_config(raw):
    staged = C.with_suffix('.fresh-next')
    require(not staged.exists(), 'staged Manager config exists')
    save(staged, raw)
    os.replace(staged, C)


def restart_manager():
    subprocess.run(['systemctl', '--user', 'restart', SERVICE], check=True, timeout=45,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline, progress = time.monotonic() + 300, time.monotonic() + 20
    while True:
        try:
            migrate_grants()
            with session('lgq') as api:
                for new in NEW:
                    api('/manager/api/catalog/accounts/' + new)
            return
        except Exception:
            require(time.monotonic() < deadline, 'Manager warmup timed out; observers only, trading remains stopped')
            if time.monotonic() >= progress:
                print('Waiting for Manager cache and new account APIs.', flush=True)
                progress = time.monotonic() + 20
            time.sleep(1)


def migrate_grants():
    """Carry the existing account access levels to the new source identities."""
    return sql("""
        INSERT INTO cta_user_source_permissions (user_id,source_id,access_level)
        SELECT p.user_id,m.new_id,p.access_level
        FROM cta_user_source_permissions p
        JOIN (VALUES ('binance_exec_trade06','binance_exec_rx01'),
                     ('binance_exec_trade07','binance_exec_rx03')) m(old_id,new_id)
          ON p.source_id=m.old_id
        JOIN cta_order_sources s ON s.source_id=m.new_id
        ON CONFLICT (user_id,source_id) DO NOTHING;
    """, False)


def prepare(stage, audit):
    require(not audit.exists(), 'audit already exists')
    audit.mkdir(mode=0o700)
    r = client()
    snapshots = {old: flat(old) for old in OLD}
    require(not rows('cta_account_follows') and not rows('cta_follow_publish_queue'), 'unexpected follows/pending tasks')
    bindings = rows('cta_account_strategy_bindings')
    for old, new, group in PAIRS:
        require(not (H / new).exists() and not list(r.scan_iter(new + ':*')), 'new identity not empty')
        require({b['binding_name'] for b in bindings if b['source_id'] == old} ==
                {f'cta-{group}-bn-{p}-pos' for p in ['1min', '5min', '15min']} and
                all(b['shares'] == 0 for b in bindings if b['source_id'] == old), 'unexpected old bindings')
    require(N.read_text().count('X-CTA-Source-ID binance_exec_trade06;') == 1 and
            N.read_text().count('X-CTA-Source-ID binance_exec_trade07;') == 1, 'unexpected gateway mapping')
    save(audit / 'cta-manager.toml.before', C.read_bytes())
    save(audit / 'cta-manager.toml.candidate', replacement(C.read_bytes()))
    save(audit / 'nginx.before', N.read_bytes())
    nginx = N.read_bytes()
    for old, new, _ in PAIRS:
        nginx = nginx.replace(('X-CTA-Source-ID ' + old + ';').encode(), ('X-CTA-Source-ID ' + new + ';').encode())
    save(audit / 'nginx.candidate', nginx)
    source_ids = "'binance_exec_rx01','binance_exec_rx03'"
    require(sql(f'SELECT count(*) FROM cta_order_sources WHERE source_id IN ({source_ids})') == '0', 'new Manager source identity already used')
    before = {'bindings': bindings, 'orders': rows('cta_order_strategies'),
              'processes': guarded_processes(), 'factor_hashes': factor_hashes(), 'exchange': snapshots,
              'old_env_hashes': {old: sha(H / old / 'env.sh') for old in OLD}}
    save(audit / 'private-before.json', before)
    pid = subprocess.check_output(['systemctl', '--user', 'show', SERVICE, '-p', 'MainPID', '--value'], text=True).strip()
    env = dict(os.environ)
    env.update(v.decode().split('=', 1) for v in Path('/proc', pid, 'environ').read_bytes().split(b'\0') if b'=' in v)
    checked = subprocess.run([str(M / 'bin/cta_web'), '--config', str(audit / 'cta-manager.toml.candidate'), '--check-allocations'],
                             env=env, capture_output=True, timeout=45)
    require(checked.returncode == 0, 'Manager candidate invalid; private diagnostics suppressed')
    manifest = json.loads((stage / 'manifest.json').read_text())
    for name, value in manifest['binaries'].items():
        require(sha(stage / 'bin' / name) == value, 'published binary hash mismatch')
    save(audit / 'release.json', manifest)
    print(json.dumps({'prepared': True, 'accounts': PAIRS, 'six_bindings_stopped': True,
                      'exchange_perpetual_positions_and_orders_zero': True, 'candidate_valid': True}), flush=True)


def apply(stage, audit):
    before = json.loads((audit / 'private-before.json').read_text())
    require(C.read_bytes() == (audit / 'cta-manager.toml.before').read_bytes() and
            N.read_bytes() == (audit / 'nginx.before').read_bytes(), 'concurrent config edit')
    require(factor_hashes() == before['factor_hashes'] and
            canonical(rows('cta_account_strategy_bindings')) == canonical(before['bindings']), 'configuration changed')
    for old in OLD:
        require(sha(H / old / 'env.sh') == before['old_env_hashes'][old], 'account credentials/config changed')
        flat(old)
    r = client()
    manifest = json.loads((stage / 'manifest.json').read_text())
    with session('lgq') as api:
        for b in before['bindings']:
            if b['source_id'] in OLD:
                api('/manager/api/catalog/accounts/' + b['source_id'] + '/bindings/' + b['binding_name'], None, 'DELETE')
    for old, new, _ in PAIRS:
        root, fresh = H / old, H / new
        for script in ['stop_exec_config_server.sh', 'stop_exec_viz_server.sh',
                       'stop_account_monitor.sh', 'stop_exec_persist_manager.sh']:
            run_script(root, script)
        for proc in Path('/proc').glob('[0-9]*'):
            try:
                require((proc / 'cwd').resolve() != root, 'old runtime process still running')
            except OSError:
                pass
        flat(old)
        keys = sorted(r.scan_iter(old + ':*'))
        save(audit / (old + '.redis.json'), [{'key': k.decode(), 'dump': base64.b64encode(r.dump(k)).decode(),
                                            'pttl': r.pttl(k)} for k in keys])
        fresh.mkdir(mode=0o700)
        with tarfile.open(stage / 'runtime.tar.gz') as tar:
            require(all(not Path(m.name).is_absolute() and '..' not in Path(m.name).parts and
                        (m.isfile() or m.isdir()) for m in tar.getmembers()), 'invalid runtime payload')
            tar.extractall(fresh, filter='data')
        for name, expected in manifest['binaries'].items():
            temporary = fresh / (name + '.next')
            shutil.copyfile(stage / 'bin' / name, temporary)
            temporary.chmod(0o755)
            require(sha(temporary) == expected, 'installed binary hash mismatch')
            os.replace(temporary, fresh / name)
        save(fresh / 'env.sh', root.joinpath('env.sh').read_bytes() +
             ('\n# Fresh CTA Exec identity; credentials and source IP remain account-scoped.\n'
              f'export IPC_NAMESPACE={shlex.quote(new)}\nexport EXEC_INSTANCE={shlex.quote(new[-2:])}\n').encode())
        save(fresh / 'trade_engine.toml', root.joinpath('trade_engine.toml').read_bytes())
        for name in ['exec_viz.toml', 'exec_config_server.env']:
            save(fresh / 'config' / name, root.joinpath('config', name).read_bytes().replace(old.encode(), new.encode()))
        (fresh / 'data/persist_manager').mkdir(parents=True)
        e = env_for(fresh)
        require(e['IPC_NAMESPACE'] == new and e['LTP_PORTFOLIO_ID'] == env_for(root)['LTP_PORTFOLIO_ID'], 'account identity mismatch')
        for venue in ['binance-futures', 'binance-margin']:
            for suffix in ['pre_trade_risk_params', 'exec:max_pos_u']:
                key = f'{old}:{venue}:{suffix}'
                value = r.dump(key)
                if value is not None:
                    r.restore(f'{new}:{venue}:{suffix}', 0, value)
        save(fresh / 'release.json', manifest)
        archive = audit / 'retired-runtimes' / old
        archive.parent.mkdir(exist_ok=True)
        inode = root.stat().st_ino
        os.rename(root, archive)
        require(archive.stat().st_ino == inode, 'runtime archive identity changed')
        print(old + ' archived intact; ' + new + ' has an empty data directory.', flush=True)
        for script in ['start_exec_persist_manager.sh', 'start_account_monitor.sh',
                       'start_exec_viz_server.sh', 'start_exec_config_server.sh']:
            run_script(fresh, script)
        assert_stopped(fresh)
    require(C.read_bytes() == (audit / 'cta-manager.toml.before').read_bytes() and
            N.read_bytes() == (audit / 'nginx.before').read_bytes(), 'concurrent Manager/gateway edit; activation halted')
    install_config((audit / 'cta-manager.toml.candidate').read_bytes())
    subprocess.run(['sudo', '-n', 'install', '-m', '0644', str(audit / 'nginx.candidate'), str(N)], check=True, capture_output=True)
    checked = subprocess.run(['sudo', '-n', 'nginx', '-t'], capture_output=True)
    require(checked.returncode == 0, 'Nginx validation failed')
    subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True, capture_output=True)
    restart_manager()
    with session('lgq') as api:
        for _, new, group in PAIRS:
            for period in ['1min', '5min', '15min']:
                b = expected_binding(new, group, period)
                api('/manager/api/catalog/accounts/' + new + '/bindings',
                    {k: v for k, v in b.items() if k != 'source_id'}, 'POST')
                print('Saved stopped binding: ' + new + ' / ' + b['binding_name'], flush=True)
    sql("UPDATE cta_order_sources SET enabled=false,updated_at=now() WHERE source_id IN ('binance_exec_trade06','binance_exec_trade07');", False)
    for old in OLD:
        keys = list(r.scan_iter(old + ':*'))
        if keys:
            r.delete(*keys)
    verify(stage, audit)


def verify(stage, audit):
    before = json.loads((audit / 'private-before.json').read_text())
    require(factor_hashes() == before['factor_hashes'], 'factor settings changed')
    now = guarded_processes()
    require(all(now.get(pid) == row for pid, row in before['processes'].items()), 'unrelated protected process changed')
    bindings = rows('cta_account_strategy_bindings')
    require(not any(b['source_id'] in OLD for b in bindings), 'old binding remains')
    require(canonical([b for b in bindings if b['source_id'] not in NEW]) ==
            canonical([b for b in before['bindings'] if b['source_id'] not in OLD]), 'other bindings changed')
    require(canonical(rows('cta_order_strategies')) == canonical(before['orders']), 'execution template changed')
    require(C.read_bytes() == (audit / 'cta-manager.toml.candidate').read_bytes() and
            N.read_bytes() == (audit / 'nginx.candidate').read_bytes(), 'configuration differs')
    results = []
    r = client()
    manifest = json.loads((audit / 'release.json').read_text())
    with session('lgq') as api:
        dashboard = api('/manager/api/dashboard')
        ids = {a['source_id'] for a in dashboard['accounts']}
        require(NEW <= ids and not OLD & ids, 'account list differs')
        for old, new, group in PAIRS:
            require(not (H / old).exists() and (audit / 'retired-runtimes' / old / 'data/persist_manager').is_dir(), 'archive missing')
            require(not list(r.scan_iter(old + ':*')), 'retired Redis keys remain')
            flat(new)
            require(all(sha(H / new / name) == value for name, value in manifest['binaries'].items()), 'live binary differs')
            archived = audit / 'retired-runtimes' / old
            require((H / new / 'trade_engine.toml').read_bytes() == (archived / 'trade_engine.toml').read_bytes(), 'source IP config changed')
            fresh_env, old_env = env_for(H / new), env_for(archived)
            require(all(fresh_env[k] == old_env[k] for k in ['LTP_API_KEY', 'LTP_API_SECRET', 'LTP_PORTFOLIO_ID']), 'credentials changed')
            for saved in json.loads((audit / (old + '.redis.json')).read_text()):
                if saved['key'].endswith((':pre_trade_risk_params', ':exec:max_pos_u')):
                    require(r.dump(saved['key'].replace(old, new, 1)) == base64.b64decode(saved['dump']), 'risk settings changed')
            financial = list((H / new / 'data/rapidx_account').glob('*/latest_financial.json'))
            require(len(financial) == 1 and json.loads(financial[0].read_text())['recovery_ready'] is True,
                    'new account monitor has not completed current snapshots')
            studio = api('/manager/api/catalog/accounts/' + new)
            expected = [expected_binding(new, group, p) for p in ['1min', '5min', '15min']]
            require(canonical([{k: b[k] for k in expected[0]} for b in studio['bindings']]) == canonical(expected), 'new bindings differ')
            for b in expected:
                value = json.loads(r.get(f'{new}:binance-futures:batch_exec:' + b['binding_name']))
                require(all(t['qty'] == 0 for t in value['targets'].values()), 'nonzero runtime target')
            for table in ['cta_uniform_order_events', 'cta_position_snapshots', 'cta_strategy_position_snapshots']:
                require(sql(f"SELECT count(*) FROM {table} WHERE source_id='{new}'") == '0', 'new source has historical orders/snapshots')
            for p in (H / new / 'data/rapidx_account').glob('*/*.jsonl'):
                for line in p.read_text().splitlines():
                    record = json.loads(line)
                    require('history_end_ms' not in record, 'execution history checkpoint reappeared')
                    require('execution' not in record, 'unexpected execution while trading stopped')
            results.append({'retired_source': old, 'new_source': new, 'group': group,
                            'bindings': 3, 'shares': 0, 'perpetual_positions': 0,
                            'open_orders': 0, 'historical_snapshots_imported': 0})
    result = {'accounts': results, 'automatic_execution_history_backfill_removed': True,
              'new_account_orders_and_strategy_targets_zero': True,
              'old_runtime_data_archived': True, 'all_seven_publishing_disabled': True,
              'trading_submitters_stopped': True, 'factor_settings_unchanged': True,
              'other_bindings_unchanged': len(before['bindings']) - 6,
              'protected_processes_unchanged': len(before['processes']),
              'exchange_order_api_calls': 0, 'checked_at_ms': time.time_ns() // 1000000}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=['prepare', 'apply', 'verify'])
    p.add_argument('--stage', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    os.umask(0o077)
    stage = a.stage.resolve()
    require(stage.parent == M / 'deploy-staging' and stage.name.startswith('cta_prep_fresh_'), 'unexpected staging path')
    audit = M / 'maintenance-backups' / stage.name
    if a.phase == 'prepare':
        prepare(stage, audit)
    elif a.phase == 'verify':
        verify(stage, audit)
    else:
        require(a.execute, 'apply requires --execute')
        with (audit / 'apply.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            apply(stage, audit)


if __name__ == '__main__':
    main()
