"""Publish the factor-only fix, preserving disabled signals and all Exec processes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import tomllib

H = Path.home()
M = H / 'crypto_cta_manager'
PREP = H / 'cta-prep-binance-futures'
SPREAD = H / 'cta-factor-spread'
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import sql
from manager_entries import protected_processes
from neutralize_rapidx_binance import assert_stopped


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')
    path.chmod(0o600)


def hashes():
    result = {}
    for root, sub in [(PREP, 'cta_prep_binance_futures'), (SPREAD, 'cta_factor_spread')]:
        for path in [root / 'env.sh', root / 'config' / (sub + '.toml'),
                     *(root / 'config' / sub).glob('*.json')]:
            result[str(path)] = sha(path)
        for path in (root / 'config' / sub).glob('*.json'):
            require(json.loads(path.read_text())['signal']['enabled'] is False, 'publishing enabled')
    return result


def processes():
    out = protected_processes()
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            cwd = (proc / 'cwd').resolve()
            exe = (proc / 'exe').resolve().name.removesuffix(' (deleted)')
            if cwd == PREP and exe == PREP.name:
                out.pop(proc.name, None)
            elif cwd == SPREAD:
                require(exe != SPREAD.name, 'Spread calculation unexpectedly running')
        except OSError:
            pass
    return out


def disabled():
    for name in ['binance_exec_rx01', 'binance_exec_rx03', 'okex_exec_trade01', 'binance_exec_spread01']:
        assert_stopped(H / name)
    rows = json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(b)), '[]'::jsonb) FROM cta_account_strategy_bindings b"))
    own = [b for b in rows if b['position_strategy_name'].startswith('cta-')]
    require(len(own) == 8 and all(b['shares'] == 0 for b in own), 'CTA binding is active or missing')
    return rows


def state():
    c = tomllib.loads((PREP / 'config/cta_prep_binance_futures.toml').read_text())
    return json.loads(((PREP / 'config' / c['history_dir']).resolve() / 'state.json').read_text())


def lifecycle(script):
    result = subprocess.run([str(PREP / 'scripts' / script), '--execute'], cwd=PREP,
                            env=dict(os.environ, PATH=str(H / '.local/bin') + ':' + os.environ['PATH']),
                            capture_output=True, timeout=60)
    require(result.returncode == 0, 'environment-local lifecycle failed; private output suppressed')
    print(script + ' completed', flush=True)


def verify(audit, manifest):
    before = json.loads((audit / 'before.json').read_text())
    require(hashes() == before['config_hashes'], 'configuration changed during publish')
    require(disabled() == before['bindings'], 'Manager execution bindings changed')
    current = processes()
    require(all(current.get(pid) == value for pid, value in before['processes'].items()), 'protected process changed')
    for root in [PREP, SPREAD]:
        require(sha(root / root.name) == manifest[root.name], 'live binary mismatch')
    s = state()
    require(s['generation'] != before['generation'] and not s['warming'] and not s['error'], 'new generation not healthy')
    require(not s.get('positions_error'), 'position table error')
    require(len(s['plan']['calculations']) == len(s['latest']) == 404, 'missing factor outputs')
    require(not any('icxusdt' in name for name in s['plan']['calculations']), 'ICX remains')
    nq = [p for name, p in s['latest'].items() if name.endswith('-nq')]
    require(nq and all(all(v['ready'] for v in p['fields'].values()) for p in nq), 'NQ remains unready')
    require(all(t['enabled'] is False for t in s['position_tables']['tables']), 'runtime position table enabled')
    ready = sum(all(v['ready'] for v in p['fields'].values()) for p in s['latest'].values())
    result = {'generation': s['generation'], 'warming': False, 'kafka_error': s['error'],
              'factor_outputs': len(s['latest']), 'ready_factors': ready, 'nq_factors': len(nq),
              'nq_ready': len(nq), 'icx_removed': True, 'all_seven_publishing_disabled': True,
              'all_eight_bindings_stopped': True, 'exec_submitters_stopped': True,
              'configurations_and_bindings_unchanged': True, 'protected_processes_unchanged': len(before['processes']),
              'binary_sha256': manifest, 'checked_at_ms': time.time_ns() // 1000000}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('phase', choices=['prepare', 'apply', 'watch', 'verify'])
    p.add_argument('--stage', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    a = p.parse_args()
    os.umask(0o077)
    stage = a.stage.resolve()
    require(stage.parent == M / 'deploy-staging' and stage.name.startswith('cta_factor_health_'), 'unexpected stage')
    audit = M / 'maintenance-backups' / stage.name
    if a.phase == 'prepare':
        audit.mkdir(mode=0o700, exist_ok=False)
        manifest = json.loads((stage / 'manifest.json').read_text())
        for name, expected in manifest.items():
            require(sha(stage / name) == expected, 'uploaded artifact checksum mismatch')
        save(audit / 'manifest.json', manifest)
        save(audit / 'before.json', {'config_hashes': hashes(), 'bindings': disabled(),
                                    'processes': processes(), 'generation': state()['generation']})
        for root in [PREP, SPREAD]:
            shutil.copyfile(root / root.name, audit / (root.name + '.before'))
        print(json.dumps({'prepared': True, 'publishing_disabled': True, 'artifacts_verified': True}), flush=True)
        return
    manifest = json.loads((audit / 'manifest.json').read_text())
    if a.phase == 'apply':
        require(a.execute, 'apply requires --execute')
        before = json.loads((audit / 'before.json').read_text())
        require(hashes() == before['config_hashes'] and disabled() == before['bindings'], 'configuration changed')
        lifecycle('stop_cta_prep_binance_futures.sh')
        for root in [PREP, SPREAD]:
            temp = root / (root.name + '.next')
            shutil.copyfile(stage / root.name, temp)
            temp.chmod(0o755)
            require(sha(temp) == manifest[root.name], 'installed binary checksum mismatch')
            os.replace(temp, root / root.name)
        lifecycle('start_cta_prep_binance_futures.sh')
        hashes()
        disabled()
        print('Prep calculation restarted; Spread stays stopped and all trading stays disabled.', flush=True)
        return
    if a.phase == 'watch':
        end = time.monotonic() + 5500
        while time.monotonic() < end:
            s = state()
            print(json.dumps({'warming': s['warming'], 'warm_records': s.get('warm_records'),
                              'generation': s.get('generation'), 'error': s.get('error')}), flush=True)
            if s['generation'] and s['generation'] != json.loads((audit / 'before.json').read_text())['generation'] and not s['warming']:
                verify(audit, manifest)
                return
            time.sleep(20)
        raise RuntimeError('replay did not finish within the configured catchup window')
    verify(audit, manifest)


if __name__ == '__main__':
    main()
