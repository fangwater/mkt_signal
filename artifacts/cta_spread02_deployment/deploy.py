#!/usr/bin/env python3
"""Install reviewed Spread02 files. Dry-run unless --execute is supplied.

Run on jp-meta-elvpn from the uploaded staging directory. Builds happen locally.
The existing Spread01 configurations, environment and persisted positions stay
in place; only the shared factor worker and configuration service are restarted.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def processes():
    found = {}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            exe = str((proc / 'exe').resolve(strict=True))
            if not exe.startswith(str(Path.home()) + '/'):
                continue
            fields = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
            status = (proc / 'status').read_text().splitlines()
            cpu = next(line.split(':', 1)[1].strip() for line in status
                       if line.startswith('Cpus_allowed_list:'))
            found[proc.name] = {'exe': exe, 'starttime': fields[19], 'cpus': cpu}
        except (OSError, StopIteration):
            pass
    return found


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    stage = Path(__file__).resolve().parent
    root = Path.home() / 'cta-factor-spread'
    config_root = Path.home() / 'cta-prep-binance-futures'
    assert root.is_dir() and config_root.is_dir()
    host = subprocess.check_output(['hostname', '-f'], text=True).strip()
    assert host.startswith('ip-172-31-35-228.'), host
    manifest = json.loads((stage / 'upload_manifest.json').read_text())
    for filename, expected in manifest.items():
        assert digest(stage / filename) == expected, filename
    candidate = stage / 'candidate'
    candidate.mkdir(exist_ok=True)
    existing = sorted((root / 'config/cta_factor_spread').glob('*.json'))
    assert len(existing) == 5, 'unexpected live catalog; recheck before installing'
    for path in existing:
        shutil.copy2(path, candidate / path.name)
    incoming = sorted((stage / 'combinations').glob('*.json'))
    assert len(incoming) == 4
    expected_counts = {'1min': 48, '5min': 79, '15min': 64, '1h': 128}
    for path in incoming:
        document = json.loads(path.read_text())
        assert document['strategy_group'] == 'spread02'
        assert len(document['factor_rules']) == expected_counts[document['interval']]
        assert document['signal']['enabled'] is False
        assert document['signal']['max_pos_u'] == 100
        assert all(row['venue'] == 'okex-futures' for row in document['factor_rules'])
        assert not (root / 'config/cta_factor_spread' / path.name).exists()
        shutil.copy2(path, candidate / path.name)
    config = (root / 'config/cta_factor_spread.toml').read_text()
    config = config.replace('combinations_dir = "cta_factor_spread"',
                            'combinations_dir = ' + json.dumps(str(candidate)))
    (stage / 'candidate.toml').write_text(config)
    plan = json.loads(subprocess.check_output([
        str(stage / 'cta-factor-spread'), '--config', str(stage / 'candidate.toml'), '--check'
    ], text=True))
    assert len(plan['plan']['combinations']) == 8
    assert len(plan['plan']['calculations']) == 532
    save(stage / 'candidate_plan.json', plan)
    for path in existing:
        if path.name != 'symbol_groups.json':
            assert plan['plan']['combinations'][path.stem] == json.loads(path.read_text())
    print(json.dumps({'execute': args.execute, 'host': host, 'env': str(root),
                      'markets': ['okex-futures', 'binance-futures'],
                      'scope': '18 Spread02 symbols, 319 rules; four tables disabled',
                      'shared_calculations': 532,
                      'restart': ['cta-factor-spread', 'cta-prep-binance-futures-config']},
                     ensure_ascii=False), flush=True)
    if not args.execute:
        return
    before = stage / 'before'
    before.mkdir(mode=0o700)
    protected = processes()
    expected_restarts = [pid for pid, item in protected.items()
                         if item['exe'] in [str(root / 'cta-factor-spread'),
                                            str(config_root / 'cta-factor-config')]]
    assert len(expected_restarts) == 2
    save(before / 'processes.json', protected)
    original_hashes = {str(path.relative_to(root)): digest(path) for path in existing}
    original_hashes['env.sh'] = digest(root / 'env.sh')
    original_hashes['config/cta_factor_spread.toml'] = digest(root / 'config/cta_factor_spread.toml')
    save(before / 'configuration_hashes.json', original_hashes)
    shutil.copy2(root / 'cta-factor-spread', before / 'cta-factor-spread')
    shutil.copy2(config_root / 'cta-factor-config', before / 'cta-factor-config')
    shutil.copytree(root / 'config', before / 'config')
    shutil.copy2(root / 'env.sh', before / 'env.sh')
    os.chmod(before / 'env.sh', 0o600)
    shutil.copy2(root / 'data/cta_factor_spread/state.json', before / 'state.json')
    subprocess.run([str(config_root / 'scripts/stop_cta_factor_config.sh'), '--execute'],
                   cwd=config_root, check=True)
    subprocess.run([str(root / 'scripts/stop_cta_factor_spread.sh'), '--execute'],
                   cwd=root, check=True)
    after_stop = processes()
    assert all(pid not in after_stop for pid in expected_restarts), 'old processes still alive'
    position_root = root / 'data/cta_factor_spread/positions'
    shutil.copytree(position_root, before / 'positions')
    positions = {path.name: json.loads(path.read_text())['ledger']
                 for path in position_root.glob('*.json')}
    save(before / 'position_ledgers.json', positions)
    for destination, source in [(root / 'cta-factor-spread', stage / 'cta-factor-spread'),
                                (config_root / 'cta-factor-config', stage / 'cta-factor-config')]:
        temporary = destination.with_name('.' + destination.name + '.spread02-next')
        shutil.copyfile(source, temporary)
        os.chmod(temporary, 0o755)
        os.replace(temporary, destination)
        assert digest(destination) == digest(source)
    for path in incoming:
        destination = root / 'config/cta_factor_spread' / path.name
        temporary = destination.with_suffix('.next')
        shutil.copy2(path, temporary)
        os.replace(temporary, destination)
    subprocess.run([str(root / 'cta-factor-spread'), '--config',
                    'config/cta_factor_spread.toml', '--check'], cwd=root, check=True,
                   stdout=(stage / 'installed_plan.json').open('w'))
    assert all(digest(root / path) == value for path, value in original_hashes.items())
    assert all(json.loads((position_root / name).read_text())['ledger'] == ledger
               for name, ledger in positions.items())
    subprocess.run([str(config_root / 'scripts/start_cta_factor_config.sh'), '--execute'],
                   cwd=config_root, check=True)
    subprocess.run([str(root / 'scripts/start_cta_factor_spread.sh'), '--execute'],
                   cwd=root, check=True)
    time.sleep(3)
    current = processes()
    changed = {pid: item for pid, item in protected.items()
               if pid not in expected_restarts and current.get(pid) != item}
    save(stage / 'protected_process_changes.json', changed)
    assert not changed, 'an unrelated application process changed; inspect receipt'
    for exe in [root / 'cta-factor-spread', config_root / 'cta-factor-config']:
        matches = [(pid, item) for pid, item in current.items() if item['exe'] == str(exe)]
        assert len(matches) == 1, (str(exe), matches)
        if exe == root / 'cta-factor-spread':
            assert matches[0][1]['cpus'] == '18'
    save(stage / 'installed.json', {'installed_at_ms': int(time.time()*1000),
                                   'original_configuration_hashes': original_hashes,
                                   'preserved_ledgers': list(positions),
                                   'protected_processes': len(protected)-2,
                                   'protected_process_changes': changed,
                                   'new_processes': {pid: item for pid, item in current.items()
                                                     if pid not in protected}})
    print('INSTALLED: Spread02 disabled; preserved Spread01 ledger; shared replay started', flush=True)


if __name__ == '__main__':
    main()
