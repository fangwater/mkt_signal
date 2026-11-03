"""Publish CTA strategy groups. Starts only Prep and Config; every table stays disabled."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time

H = Path.home()
M = H / 'crypto_cta_manager'
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import env_for, session, sql
from manager_entries import protected_processes
from clear_positions import exchange_snapshot
import neutralize_rapidx_binance as n

WORKERS = {'cta-prep-binance-futures': 'cta_prep_binance_futures', 'cta-factor-spread': 'cta_factor_spread'}
OLD = ['cta-prep-bn-1min-pos', 'cta-prep-bn-5min-pos', 'cta-prep-bn-15min-pos', 'cta-spread-okx-bn-1min-pos']
EXPECTED = {f'cta-{group}-bn-{period}-pos' for group in ['prep01', 'prep02'] for period in ['1min', '5min', '15min']} | {'cta-spread01-okx-bn-1min-pos'}
EXECS = ['binance_exec_trade06', 'okex_exec_trade01', 'binance_exec_spread01']


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.next')
    with tmp.open('w') as f:
        os.fchmod(f.fileno(), 0o600)
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def owned_processes():
    found = []
    for path in Path('/proc').glob('[0-9]*'):
        try:
            cwd = str((path / 'cwd').resolve())
            if cwd not in {str(H / worker) for worker in WORKERS}:
                continue
            binary = Path((path / 'cmdline').read_bytes().split(b'\0')[0].decode()).name
            status = (path / 'status').read_text()
            if '\nState:\tZ' in status:
                continue
            cpus = next(line.split(':', 1)[1].strip() for line in status.splitlines() if line.startswith('Cpus_allowed_list:'))
            found.append({'pid': int(path.name), 'binary': binary, 'cwd': cwd, 'cpus': cpus})
        except (OSError, UnicodeError, IndexError):
            pass
    return found


def protected():
    return {pid: row for pid, row in protected_processes().items() if row['cwd'] not in {str(H / w) for w in WORKERS}}


def require_execution_stopped():
    for env in EXECS:
        n.assert_stopped(H / env)
    snap = exchange_snapshot(EXECS[0])
    n.require(not snap['nonzero_positions'] and snap['open_orders'] == snap['algo_orders'] == 0, 'RX01 perpetual account is not flat')
    names = ','.join("'" + v + "'" for v in sorted(EXPECTED | set(OLD)))
    n.require(sql(f'SELECT count(*) FROM cta_account_strategy_bindings WHERE position_strategy_name IN ({names})') == '0', 'CTA strategy bindings exist')
    n.require(sql("SELECT count(*) FROM cta_account_strategy_bindings WHERE source_id='binance_exec_trade06'") == '0', 'RX01 bindings exist')


def documents(stage):
    result = []
    for worker, directory in WORKERS.items():
        for path in sorted((stage / worker / 'config' / directory).glob('*.json')):
            doc = json.loads(path.read_text())
            n.require(doc['signal']['enabled'] is False, 'every position table must remain disabled')
            group = doc['strategy_group']
            name = 'cta-' + group + ('-okx-bn-' if worker == 'cta-factor-spread' else '-bn-') + doc['interval'] + '-pos'
            symbols = {r['标的'] for r in doc['factor_rules']}
            targets = {symbol: {'qty': 0, 'signal': 0} for symbol in symbols}
            if worker == 'cta-factor-spread':
                targets = {venue + ':' + symbol: val for symbol, val in targets.items() for venue in ['okex-futures', 'binance-futures']}
            result.append({'worker': worker, 'strategy_name': name, 'targets': targets, 'document': doc, 'path': str(path.relative_to(stage))})
    n.require({r['strategy_name'] for r in result} == EXPECTED and len(result) == 7, 'expected seven independent tables')
    return result


def lifecycle(worker, script, audit):
    root = H / worker
    r = subprocess.run([str(root / 'scripts' / script), '--execute'], cwd=root, capture_output=True, timeout=55)
    (audit / (script + '.log')).write_bytes(r.stdout + r.stderr)
    n.require(r.returncode == 0, 'env-local lifecycle script failed; see private audit')
    print(json.dumps({'worker': worker, 'script': script, 'completed': True}), flush=True)


def verify(audit, manifest):
    require_execution_stopped()
    before = json.loads((audit / 'protected.before.json').read_text())
    now = protected()
    n.require(all(now.get(pid) == row for pid, row in before.items()), 'protected process changed')
    for relative, expected in manifest['files'].items():
        n.require(sha(H / relative) == expected, 'installed artifact hash changed')
    with session('lgq') as api:
        profiles = {}
        for worker in WORKERS:
            data = api('/cta/cta-prep-binance-futures/config/api/config?worker=' + worker)
            combos = data['plan']['combinations']
            n.require(all(not c['signal']['enabled'] for c in combos.values()), 'position publishing was enabled')
            profiles[worker] = {name: {'strategy_group': c['strategy_group'], 'rules': len(c['factor_rules']), 'enabled': c['signal']['enabled'], 'max_pos_u': c['signal']['max_pos_u']} for name, c in combos.items()}
            n.require(data['unique_factor_count'] == (408 if worker.startswith('cta-prep') else 50), 'unexpected calculation plan')
        catalog = {r['strategy_name']: r for r in api('/manager/api/catalog/position-strategies')}
        n.require(EXPECTED <= catalog.keys() and not set(OLD) & catalog.keys(), 'Manager strategy identity mismatch')
        n.require(all(all(target['qty'] == 0 for target in catalog[name]['targets'].values()) for name in EXPECTED), 'Manager has nonzero initial targets')
    processes = owned_processes()
    n.require(len([p for p in processes if p['binary'] == 'cta-prep-binance-futures' and p['cpus'] == '17']) == 1, 'Prep worker is not on CPU17')
    n.require(len([p for p in processes if p['binary'] == 'cta-factor-config']) == 1, 'Config service is not running')
    n.require(not any(p['binary'] == 'cta-factor-spread' for p in processes), 'Spread worker must stay stopped')
    state_path = H / 'cta-prep-binance-futures/data/cta_prep_binance_futures/state.json'
    state = json.loads(state_path.read_text())
    n.require(not state['error'] and not state['positions_error'], 'Prep runtime reports an error')
    result = {'commit': manifest['commit'], 'profiles': profiles, 'processes': processes, 'protected_processes_unchanged': len(before), 'rx01_perpetual_positions': 0, 'rx01_orders': 0, 'bindings': 0, 'all_publishing_disabled': True, 'spread_worker_started': False, 'warming': state['warming'], 'warm_records': state['warm_records'], 'checked_at_ms': time.time_ns() // 1000000}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=['prepare', 'install', 'activate', 'verify'])
    p.add_argument('--audit', type=Path, required=True)
    p.add_argument('--payload-sha256')
    p.add_argument('--execute', action='store_true')
    a = p.parse_args();audit = a.audit
    os.umask(0o077)
    n.require(audit.parent == M / 'maintenance-backups' and audit.name.startswith('cta_prep02_deploy_'), 'unexpected audit path')
    if a.phase == 'prepare':
        require_execution_stopped()
        n.require(sha(audit / 'payload.tar') == a.payload_sha256, 'payload checksum mismatch')
        stage = audit / 'stage';stage.mkdir()
        with tarfile.open(audit / 'payload.tar') as archive:
            n.require(all(not Path(v.name).is_absolute() and '..' not in Path(v.name).parts and (v.isfile() or v.isdir()) for v in archive.getmembers()), 'unsafe payload')
            archive.extractall(stage, filter='data')
        manifest = json.loads((stage / 'manifest.json').read_text())
        for path, expected in manifest['files'].items():
            n.require(sha(stage / path) == expected, 'artifact checksum mismatch')
        docs = documents(stage)
        before = {}
        for worker, directory in WORKERS.items():
            root = H / worker
            for doc in [r for r in docs if r['worker'] == worker]:
                target = H / doc['path']
                if target.exists():
                    current = json.loads(target.read_text());expected = dict(doc['document']);expected.pop('strategy_group')
                    n.require(current == expected, 'operator changed existing frequency configuration')
            for path in [root / 'env.sh', root / 'config' / (directory + '.toml'), *[H / r for r in manifest['files'] if r.startswith(worker + '/')]]:
                before[str(path)] = sha(path) if path.exists() else None
            for path in (root / 'data' / directory / 'positions').glob('*.json'):
                row = json.loads(path.read_text())
                n.require(row['strategy_name'] in OLD and not row['ever_enabled'] and not row['combination']['signal']['enabled'] and not row['ledger']['rows'] and not row['clock'] and not row['observations'], 'legacy ledger is not unused and empty')
                before[str(path)] = sha(path)
            state = root / 'data' / directory / 'state.json'
            if state.exists():before[str(state)] = sha(state)
        save(audit / 'files.before.json', before)
        save(audit / 'protected.before.json', protected())
        with session('lgq') as api:
            catalog = {r['strategy_name']: r for r in api('/manager/api/catalog/position-strategies')}
            n.require(not EXPECTED & catalog.keys(), 'new strategy identities already exist')
            n.require(set(OLD) <= catalog.keys(), 'legacy strategy identity missing')
            n.require(all(all(t['qty'] == 0 for t in catalog[name]['targets'].values()) for name in OLD), 'legacy strategy targets are nonzero')
            save(audit / 'legacy-strategies.before.json', {name: catalog[name] for name in OLD})
        n.require(not any(p['binary'] in WORKERS for p in owned_processes()), 'calculation worker is already running')
        print(json.dumps({'prepared': True, 'tables': len(docs), 'all_disabled': True}), flush=True)
        return
    manifest = json.loads((audit / 'stage/manifest.json').read_text())
    if a.phase == 'verify':verify(audit, manifest);return
    if a.phase == 'activate':
        n.require(a.execute, 'activation requires --execute')
        with (audit / 'install.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            n.require(not (audit / 'installed.json').exists(), 'already activated; use verify')
            require_execution_stopped()
            n.require(not owned_processes(), 'owned process is already running')
            for path, expected in manifest['files'].items():
                n.require(sha(H / path) == expected, 'partially installed artifact changed')
            before = json.loads((audit / 'files.before.json').read_text())
            for path, expected in before.items():
                if path.endswith('/env.sh') or path.endswith('.toml'):
                    n.require(sha(path) == expected, 'protected runtime configuration changed')
            docs = documents(audit / 'stage')
            legacy = json.loads((audit / 'legacy-strategies.before.json').read_text())
            with session('lgq') as api, session('prc') as admin:
                current = {r['strategy_name']: r for r in api('/manager/api/catalog/position-strategies')}
                for doc in docs:
                    n.require(doc['strategy_name'] in current and current[doc['strategy_name']]['targets'] == doc['targets'], 'new strategy registration differs')
                for name in OLD:
                    if name in current:
                        n.require(current[name] == legacy[name], 'legacy strategy changed during activation')
                        admin('/manager/api/catalog/position-strategies/' + name, None, 'DELETE')
            save(audit / 'installed.json', {'commit': manifest['commit'], 'tables': sorted(EXPECTED), 'all_disabled': True, 'at_ms': time.time_ns() // 1000000})
            lifecycle('cta-prep-binance-futures', 'start_cta_factor_config.sh', audit)
            lifecycle('cta-prep-binance-futures', 'start_cta_prep_binance_futures.sh', audit)
        for _ in range(30):
            if (H / 'cta-prep-binance-futures/data/cta_prep_binance_futures/state.json').exists():break
            time.sleep(1)
        verify(audit, manifest)
        return
    n.require(a.execute, 'install requires --execute')
    with (audit / 'install.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        n.require(not (audit / 'installed.json').exists(), 'already installed; use verify')
        require_execution_stopped()
        before = json.loads((audit / 'files.before.json').read_text())
        for path, expected in before.items():
            n.require((sha(path) if Path(path).exists() else None) == expected, 'file changed after preparation; do not overwrite')
        lifecycle('cta-prep-binance-futures', 'stop_cta_factor_config.sh', audit)
        for _ in range(50):
            if not owned_processes():break
            time.sleep(0.1)
        n.require(not owned_processes(), 'owned process remains after Config stop')
        for path, expected in before.items():
            if expected is not None and not path.endswith('/env.sh'):
                target = audit / 'before-files' / str(Path(path).relative_to(H));target.parent.mkdir(parents=True, exist_ok=True);shutil.copy2(path, target)
        for worker, directory in WORKERS.items():
            for path in (H / worker / 'data' / directory / 'positions').glob('*.json'):
                n.require(sha(path) == before[str(path)], 'legacy ledger changed')
                path.unlink()
            state = H / worker / 'data' / directory / 'state.json'
            state.unlink(missing_ok=True)
        for relative, expected in manifest['files'].items():
            source = audit / 'stage' / relative;target = H / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            temporary = target.with_name(target.name + '.cta-next');shutil.copyfile(source, temporary)
            temporary.chmod(0o755 if '/config/' not in relative else 0o644)
            n.require(sha(temporary) == expected, 'staged file checksum mismatch')
            os.replace(temporary, target)
        for worker, directory in WORKERS.items():
            r = subprocess.run([str(H / worker / worker), '--config', str(H / worker / 'config' / (directory + '.toml')), '--check'], capture_output=True, timeout=30)
            n.require(r.returncode == 0, 'installed worker check failed')
            (audit / (worker + '.checked.json')).write_bytes(r.stdout)
        docs = documents(audit / 'stage')
        with session('lgq') as api, session('prc') as admin:
            for doc in docs:
                result = api('/manager/api/catalog/position-strategies', {'strategy_name': doc['strategy_name'], 'targets': doc['targets']}, 'POST')
                n.require(not result['publishes'], 'unexpected execution publication')
                token = env_for(H / doc['worker'])['CTA_PUBLISH_TOKEN']
                admin('/manager/api/catalog/position-strategies/' + doc['strategy_name'] + '/publish-token', {'publish_token': token}, 'PUT')
            require_execution_stopped()
            for name in OLD:
                admin('/manager/api/catalog/position-strategies/' + name, None, 'DELETE')
        save(audit / 'installed.json', {'commit': manifest['commit'], 'tables': sorted(EXPECTED), 'all_disabled': True, 'at_ms': time.time_ns() // 1000000})
        lifecycle('cta-prep-binance-futures', 'start_cta_factor_config.sh', audit)
        lifecycle('cta-prep-binance-futures', 'start_cta_prep_binance_futures.sh', audit)
    for _ in range(30):
        if (H / 'cta-prep-binance-futures/data/cta_prep_binance_futures/state.json').exists():break
        time.sleep(1)
    verify(audit, manifest)


if __name__ == '__main__':main()
