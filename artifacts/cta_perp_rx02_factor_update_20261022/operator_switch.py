#!/usr/bin/env python3
"""Operator-run RX02 cutover. Default: read-only preflight; --execute trades live."""
from __future__ import annotations

import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import tomllib

LIVE = Path('/home/ubuntu/binance-cta-perp-rx02')
STAGE = Path('/home/ubuntu/cta-audits/rx02-factor-replay-20261022T0540Z')
PAYLOAD = {
    'cta_perp_factor_model_1m_pub': ('cta_perp_factor_model_1m_pub', 'target/release/cta_perp_factor_model_1m_pub'),
    'config/cta_perp_factor_model_1m_pub.toml': ('cta_perp_factor_model_1m_pub.toml', 'config/cta_perp_rx02/cta_perp_factor_model_1m_pub.toml'),
    'config/cta_perp_symbol_factors.json': ('cta_perp_symbol_factors.json', 'config/cta_perp_rx02/cta_perp_symbol_factors.json'),
    'scripts/cta_perp_factor_config.py': ('payload/scripts/cta_perp_factor_config.py', 'scripts/cta_perp_factor_config.py'),
    'scripts/factor_validation_allowlist.txt': ('payload/scripts/factor_validation_allowlist.txt', 'scripts/factor_validation_allowlist.txt'),
    'scripts/start_cta_perp_factor_model_1m_pub.sh': ('payload/scripts/start_cta_perp_factor_model_1m_pub.sh', 'scripts/start_cta_perp_factor_model_1m_pub.sh'),
}
WRAPPERS = ['stop_cta_perp_signal.sh', 'stop_cta_perp_factor_model_1m_pub.sh',
            'stop_cta_perp_config_server.sh', 'start_cta_perp_config_server.sh',
            'start_cta_perp_factor_model_1m_pub.sh', 'start_cta_perp_signal.sh']


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read(path):
    return json.loads(path.read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def processes(fragment):
    found = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            if (proc / 'cwd').resolve() != LIVE:
                continue
            args = (proc / 'cmdline').read_bytes().split(b'\0')
            if any(Path(arg.decode()).name == fragment for arg in args[:2] if arg):
                found.append(proc)
        except (OSError, UnicodeError):
            pass
    return found


def run(args, capture=False):
    # Existing env-local wrappers own supervisor behavior and credentials.
    command = ['bash', '-c', 'set -e; source ./env.sh; unset PMDAEMON_NAME; exec "$@"',
               'rx02-operator', *args]
    result = subprocess.run(command, cwd=LIVE, capture_output=True, text=True)
    require(result.returncode == 0, 'Command failed: ' + args[0] + '; inspect host logs')
    return result.stdout if capture else None


def redis_table():
    output = run(['python3', 'scripts/cta_perp_factor_config.py',
                  '--env-name', LIVE.name, '--check'], capture=True)
    return json.loads(output)['symbol_factors']


def summary(table):
    return {'symbols': len(table), 'factors': len({f for row in table.values() for f in row}),
            'pairs': sum(len(row) for row in table.values())}


def fresh_replay(status, old_generation, symbols, now_ms):
    latest = status.get('latest', {})
    return (status.get('generation') not in (None, old_generation)
            and status.get('caught_up') is True
            and 0 <= now_ms - status.get('updated_ms', 0) <= 60_000
            and all(0 <= now_ms - latest.get(s, 0) <= 120_000 for s in symbols))


def preflight():
    require(LIVE.resolve() == LIVE and STAGE.resolve() == STAGE, 'Unexpected path/symlink')
    manifest = read(STAGE / 'build_manifest.json')
    require(manifest['branch'] == 'arbmm', 'Expected production branch artifact')
    for destination, (source, key) in PAYLOAD.items():
        require(sha(STAGE / source) == manifest['sha256'][key], 'Payload changed: ' + source)
        require((LIVE / destination).resolve() == LIVE / destination, 'Unexpected destination symlink')
    before = read(STAGE / 'result.json')['before']['hashes']
    for name, digest in before.items():
        require(sha(LIVE / name) == digest, 'Live file changed since audit: ' + name)
    for name in WRAPPERS:
        require((LIVE / 'scripts' / name).is_file(), 'Missing wrapper: ' + name)
    config = read(LIVE / 'config/cta_perp.json')
    require(config['enabled'] and config['position_algorithm'] == 'v1'
            and config['max_pos_u'] == 50, 'Live strategy settings changed')
    status = read(LIVE / 'run/cta_perp_status.json')
    require(time.time() * 1e6 - status['updated_ts_us'] < 30e6, 'Signal status stale')
    require(not status['recovering'] and status['journal_healthy'], 'Signal not healthy')
    require(not any(status.get(k) for k in ['configuration_error', 'publication_error', 'recovery_error']),
            'Signal has an error')
    old_table = read(LIVE / 'config/cta_perp_symbol_factors.json')
    require(status['symbol_factors'] == old_table == redis_table(), 'Live table mismatch')
    new_table = read(STAGE / 'cta_perp_symbol_factors.json')
    require(summary(new_table) == {'symbols': 23, 'factors': 22, 'pairs': 69}, 'Candidate scope changed')
    require(not any('baseline_047' in row for row in new_table.values()), 'Size factor present')
    universe = tomllib.loads((STAGE / 'cta_perp_factor_model_1m_pub.toml').read_text())['universe']
    require(set(universe['symbols']) == set(new_table), 'Publisher symbol mismatch')
    require(set(universe['factors']) == {f for row in new_table.values() for f in row}, 'Publisher factor mismatch')
    for binary in ['cta_perp_signal', 'cta_perp_factor_model_1m_pub', 'cta_perp_config_server.py']:
        require(len(processes(binary)) == 1, 'Expected one live ' + binary)
    proc = processes('cta_perp_factor_model_1m_pub')[0]
    runtime = dict(part.split(b'=', 1) for part in (proc / 'environ').read_bytes().split(b'\0') if b'=' in part)
    require(runtime.get(b'CTA_PERP_FACTOR_MODEL_1M_CORE') == b'18', 'Publisher CPU setting changed')
    require(not runtime.get(b'CTA_PERP_FACTOR_HISTORY_PATH'), 'Custom history needs review')
    require(runtime.get(b'CTA_PERP_RUN_FACTOR_PUBLISHER', b'1') == b'1', 'Shared publisher ownership changed')
    print(json.dumps({'read_only_preflight': 'passed', 'current': summary(old_table),
                      'candidate': summary(new_table), 'wait_for_all_factors': False}), flush=True)
    return new_table, old_table, manifest


def stop(wrapper, binary):
    run(['scripts/' + wrapper])
    until = time.monotonic() + 30
    while processes(binary) and time.monotonic() < until:
        time.sleep(0.5)
    require(not processes(binary), 'Process did not stop: ' + binary)


def started(binary):
    until = time.monotonic() + 15
    while time.monotonic() < until:
        found = processes(binary)
        if len(found) == 1:
            return found[0]
        require(len(found) < 2, 'Duplicate process: ' + binary)
        time.sleep(0.5)
    raise RuntimeError('Process did not start: ' + binary)


def execute(new_table, old_table, manifest):
    # Never reset flags/journals or automatically roll back after target changes.
    import fcntl
    lock = (LIVE / 'run/rx02-factor-cutover.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    backup = LIVE / 'run' / ('rx02_factor_before_' + stamp)
    backup.mkdir(mode=0o700)
    preserved = {n: sha(LIVE / n) for n in ['env.sh', 'cta_perp_signal', 'config/cta_perp.json']}
    for name in sorted(set(PAYLOAD) | set(preserved)):
        destination = backup / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(LIVE / name, destination)
    (backup / 'redis_table.json').write_text(json.dumps(old_table))
    print('Backup: ' + str(backup), flush=True)
    stop('stop_cta_perp_signal.sh', 'cta_perp_signal')
    stop('stop_cta_perp_factor_model_1m_pub.sh', 'cta_perp_factor_model_1m_pub')
    stop('stop_cta_perp_config_server.sh', 'cta_perp_config_server.py')
    require(redis_table() == old_table, 'Concurrent factor edit; stopped before installation')
    require(all(sha(LIVE / n) == digest for n, digest in preserved.items()), 'Concurrent preserved setting edit')
    for name in ['run/cta_perp_position_flags.json', 'run/cta_perp_status.json']:
        shutil.copy2(LIVE / name, backup / Path(name).name)
    shutil.copytree(LIVE / 'data/cta_perp/signal_events', backup / 'signal_events')
    history = LIVE / 'data/cta_perp/factor_history'
    old_generation = read(history / 'recovery.json')['generation']
    os.rename(history, backup / 'factor_history')
    for destination, (source, key) in PAYLOAD.items():
        require(sha(STAGE / source) == manifest['sha256'][key], 'Payload changed during switch')
        target = LIVE / destination
        temp = target.with_name(target.name + '.rx02-next')
        require(not temp.exists(), 'Stale staging file: ' + str(temp))
        shutil.copy2(STAGE / source, temp)
        if destination == 'cta_perp_factor_model_1m_pub' or destination.endswith('.sh'):
            temp.chmod(0o755)
        os.replace(temp, target)
    run(['python3', 'scripts/cta_perp_factor_config.py', '--env-name', LIVE.name,
         '--config', 'config/cta_perp_symbol_factors.json', '--execute'])
    require(redis_table() == new_table, 'New Redis table mismatch')
    run(['scripts/start_cta_perp_factor_model_1m_pub.sh'])
    started('cta_perp_factor_model_1m_pub')
    print('Waiting for this publisher generation to catch up with Kafka; signal remains stopped.', flush=True)
    until = time.monotonic() + 3700
    while time.monotonic() < until:
        require(len(processes('cta_perp_factor_model_1m_pub')) == 1, 'Publisher stopped; inspect host logs')
        status = read(history / 'recovery.json') if (history / 'recovery.json').exists() else {}
        if fresh_replay(status, old_generation, new_table, int(time.time() * 1000)):
            break
        print(json.dumps({'caught_up': status.get('caught_up', False),
                          'covered_symbols': len(status.get('latest', {}))}), flush=True)
        time.sleep(10)
    else:
        raise RuntimeError('Kafka catch-up timed out; signal remains stopped, no rollback performed')
    proc = processes('cta_perp_factor_model_1m_pub')[0]
    require(sha(proc / 'exe') == manifest['sha256']['target/release/cta_perp_factor_model_1m_pub'],
            'Running publisher hash mismatch')
    require(os.sched_getaffinity(int(proc.name)) == {18}, 'Running publisher CPU mismatch')
    require(all(sha(LIVE / n) == digest for n, digest in preserved.items()), 'Preserved setting changed')
    require(redis_table() == new_table, 'Concurrent factor edit during catch-up')
    run(['scripts/start_cta_perp_config_server.sh'])
    started('cta_perp_config_server.py')
    started_us = int(time.time() * 1e6)
    run(['scripts/start_cta_perp_signal.sh'])
    started('cta_perp_signal')
    until = time.monotonic() + 300
    while time.monotonic() < until:
        status = read(LIVE / 'run/cta_perp_status.json')
        publications = status.get('manager_publications', [])
        if (status.get('updated_ts_us', 0) >= started_us and status['symbol_factors'] == new_table
                and not status['recovering'] and status['journal_healthy']
                and not any(status.get(k) for k in ['configuration_error', 'publication_error', 'recovery_error'])
                and len(publications) == 2
                and all(not p['error'] and not p['pending_strategies']
                        and (p.get('last_success_ts_us') or 0) >= started_us for p in publications)):
            result = {'cutover_complete': True, 'candidate': summary(new_table),
                      'both_managers_received': True, 'fills_verified': False,
                      'backup': str(backup), 'completed_at_us': int(time.time() * 1e6)}
            (backup / 'result.json').write_text(json.dumps(result, indent=2))
            print(json.dumps(result), flush=True)
            print('Target delivery verified. Inspect Exec target/order/fill records separately; this is not proof of a fill.')
            return
        time.sleep(5)
    raise RuntimeError('Signal delivery not confirmed; inspect live state, no automatic rollback performed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true', help='OPERATOR ONLY: switch live strategy and trigger orders')
    args = parser.parse_args()
    candidate, previous, release = preflight()
    if args.execute:
        execute(candidate, previous, release)
