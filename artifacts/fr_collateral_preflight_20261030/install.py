#!/usr/bin/env python3
"""Install only reviewed collateral guards; never invoke a process supervisor."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path(__file__).resolve().parent
EXPECTED_WRAPPERS = {
    'start_trade_signal.sh': {
        '684d944b90ec3d73d0e9c5abee13ed07db6439a2b1623345e6d0435ef81513cb': 'current',
        '7a654750943b463d9e7118cd3a098320f0a90709db0bab435a978f4e1ea424fe': 'legacy',
        '2c3965e4e17ef1a4b6fd6a77233b1ccdae71bc9ad33cc0ab3e0561a015e34bae': 'legacy_arb01',
    },
    'start_fr_pre_trade.sh': {
        '3a6e9b2696a905f431490474e5e1ff1876148bfd0172eefb18a8c782170b8062': 'current',
        'b0461373b96b2051777a797db63070185b58fec429160d4445ed04a2c455971d': 'legacy',
    },
}
TARGETS = {
    'sg': ['bybit_fr_arb01'],
    'jp-meta-elvpn': ['binance_fr_arb01', 'binance_fr_arb02', 'binance_fr_arb03', 'binance_fr_arb04',
                      'bitget_fr_arb01', 'bitget_fr_arb02', 'bitget_fr_arb03',
                      'gate_fr_arb01', 'gate_fr_arb02', 'gate_fr_arb03', 'gate_fr_arb04', 'gate_fr_arb05'],
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def processes(target):
    found = {}
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            if (proc/'cwd').resolve() != target:
                continue
            comm = (proc/'comm').read_text().strip()
            if comm in {'trade_signal', 'pre_trade', 'trade_engine', 'account_monitor'}:
                found[proc.name] = {'comm': comm, 'start': (proc/'stat').read_text().split()[21]}
        except OSError:
            pass
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', choices=TARGETS)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    expected_hostname = 'ip-172-31-7-123' if args.host == 'sg' else 'ip-172-31-35-228'
    assert os.uname().nodename.startswith(expected_hostname), 'unexpected production host'
    manifest = json.loads((ROOT/'manifest.json').read_text())
    for name, expected in manifest.items():
        data = (ROOT/'payload'/name).read_bytes()
        assert sha(data) == expected, f'payload checksum mismatch: {name}'
        if name.endswith('.py'):
            compile(data, name, 'exec')
        elif name.endswith('.sh'):
            subprocess.run(['bash', '-n'], input=data, check=True)

    plans = []
    for env_name in TARGETS[args.host]:
        target = Path('/home/ubuntu')/env_name
        assert target.is_dir() and target.resolve() == target
        before = processes(target)
        expected_count = {'binance_fr_arb01': 0, 'gate_fr_arb04': 1, 'gate_fr_arb05': 0}.get(env_name, 4)
        assert len(before) == expected_count, f'unexpected process state: {env_name}'
        files = []
        for name in ['scripts/lib/exchange_signing.py', 'scripts/lib/intra_symbols.py',
                     'scripts/check_fr_collateral.py']:
            dst = target/name
            old = sha(dst.read_bytes()) if dst.exists() else None
            assert old in {None, manifest[name]}, f'existing helper differs: {env_name}/{name}'
            files.append((name, name, old))
        for name, versions in EXPECTED_WRAPPERS.items():
            old = sha((target/'scripts'/name).read_bytes())
            assert old in versions, f'operator startup changes detected: {env_name}/{name}'
            files.append((f'scripts/{name}', f'{versions[old]}/{name}', old))
        plans.append((target, files, before))
    if not args.execute:
        for target, files, before in plans:
            print(json.dumps({'env': target.name, 'files': len(files), 'processes': before, 'dry_run': True}))
        return

    records = []
    for target, files, before in plans:
        run = target/'run'/'fr_collateral_preflight_20261030'
        run.mkdir(parents=True, exist_ok=True)
        record = {'env': target.name, 'at_ms': int(time.time()*1000), 'before_processes': before, 'files': []}
        for destination, payload, expected_old in files:
            dst = target/destination
            actual_old = sha(dst.read_bytes()) if dst.exists() else None
            assert actual_old == expected_old, f'concurrent file change: {dst}'
            if dst.exists():
                backup = run/'backup'/destination
                backup.parent.mkdir(parents=True, exist_ok=True)
                assert not backup.exists(), f'backup already exists: {backup}'
                shutil.copy2(dst, backup)
            data = (ROOT/'payload'/payload).read_bytes()
            dst.parent.mkdir(parents=True, exist_ok=True)
            tmp = dst.with_name(dst.name + f'.collateral.{os.getpid()}.tmp')
            with tmp.open('xb') as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            tmp.chmod(0o755 if destination.endswith('.sh') or destination.endswith('check_fr_collateral.py') else 0o644)
            os.replace(tmp, dst)
            assert sha(dst.read_bytes()) == manifest[payload]
            record['files'].append({'path': destination, 'old': expected_old, 'new': manifest[payload]})
        record['after_processes'] = processes(target)
        assert record['after_processes'] == before, 'live process changed during script installation'
        (run/'installation.json').write_text(json.dumps(record, indent=2)+'\n')
        records.append(record)
        print(json.dumps({'env': target.name, 'installed': True, 'processes_unchanged': True}), flush=True)
    (ROOT/f'installed_{args.host}.json').write_text(json.dumps(records, indent=2)+'\n')


if __name__ == '__main__':
    main()
