#!/usr/bin/env python3
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent
TARGETS = {'sg': ['bybit_fr_arb01'], 'jp-meta-elvpn': [
    'binance_fr_arb01', 'binance_fr_arb02', 'binance_fr_arb03', 'binance_fr_arb04',
    'bitget_fr_arb01', 'bitget_fr_arb02', 'bitget_fr_arb03',
    'gate_fr_arb01', 'gate_fr_arb02', 'gate_fr_arb03', 'gate_fr_arb04', 'gate_fr_arb05']}
OLD_HASHES = {
    'check_fr_collateral.py': {'87188c7ba694fbd5a627b891e785cbac8f682110ba880b0c669f66d8c733d60f'},
    'fr_config_server.py': {'c8dbb66bde5d3ef41b15feeff59fe3aba458b80d3419e0ecb9195291cd3c2d47',
                            '1c3ab2a794df0d857f0e7213281f82b4378de1f86f101c499eb9bd7d395308c6'},
    'sync_fr_symbol_lists.py': {'d73a96c5d3031b759d7a19f3fae47e3468a1209cb919091a79192ec58cd9ca41',
                               '9a59e4b4a434fec13af2f53b2d23c5b84c8ef7831254354a6870d756f7e72d7a'},
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def processes(target, config=False):
    found = {}
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            if (proc/'cwd').resolve() != target:
                continue
            comm = (proc/'comm').read_text().strip()
            if config:
                if not comm.startswith('python'):
                    continue
                args = (proc/'cmdline').read_bytes().split(b'\0')
                if not any(a.endswith(b'/fr_config_server.py') for a in args):
                    continue
                found[proc.name] = int(args[args.index(b'--port')+1])
            elif comm in {'trade_signal', 'pre_trade', 'trade_engine', 'account_monitor'}:
                found[proc.name] = {'comm': comm, 'start': (proc/'stat').read_text().split()[21]}
        except (OSError, ValueError):
            pass
    return found


def api(port, payload=None):
    req = urllib.request.Request(f'http://127.0.0.1:{port}/api/symbol-lists',
                                 data=json.dumps(payload).encode() if payload is not None else None,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as exc:
        return exc.code, json.load(exc)


def run_wrapper(target, name):
    result = subprocess.run(['bash', str(target/'scripts'/name)], cwd=target,
                            capture_output=True, timeout=45)
    assert result.returncode == 0, f'env-local wrapper failed: {target.name}/{name}'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('host', choices=TARGETS)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    expected_host = 'ip-172-31-7-123' if args.host == 'sg' else 'ip-172-31-35-228'
    assert os.uname().nodename.startswith(expected_host)
    manifest = json.loads((ROOT/'manifest.json').read_text())
    for name, expected in manifest.items():
        data = (ROOT/'payload'/name).read_bytes()
        assert sha(data) == expected
        compile(data, name, 'exec')
    plans = []
    for name in TARGETS[args.host]:
        target = Path('/home/ubuntu')/name
        assert target.resolve() == target and target.is_dir()
        originals = {}
        for file in manifest:
            data = (target/'scripts'/file).read_bytes()
            originals[file] = sha(data)
            assert originals[file] in OLD_HASHES[file], f'operator file changes: {name}/{file}'
        trading = processes(target)
        configs = processes(target, config=True)
        expected_count = {'binance_fr_arb01': 0, 'gate_fr_arb04': 1, 'gate_fr_arb05': 0}.get(name, 4)
        assert len(trading) == expected_count and len(configs) == (1 if expected_count == 4 else 0)
        plans.append((target, originals, trading, configs))
    if not args.execute:
        for target, originals, trading, configs in plans:
            print(json.dumps({'env': target.name, 'dry_run': True, 'config_restart': bool(configs)}))
        return

    records = []
    for target, originals, trading, configs in plans:
        record = {'env': target.name, 'before_trading': trading, 'before_config': configs,
                  'files': {}, 'at_ms': int(time.time()*1000)}
        baseline = None
        port = next(iter(configs.values())) if configs else None
        if port:
            status, baseline = api(port)
            assert status == 200
        backup = target/'run/fr_collateral_hot_update_20261030/backup'
        for file, expected in manifest.items():
            dst = target/'scripts'/file
            assert sha(dst.read_bytes()) == originals[file]
            old = backup/file
            old.parent.mkdir(parents=True, exist_ok=True)
            assert not old.exists()
            shutil.copy2(dst, old)
            tmp = dst.with_name(dst.name+f'.collateral.{os.getpid()}.tmp')
            with tmp.open('xb') as stream:
                stream.write((ROOT/'payload'/file).read_bytes())
                stream.flush()
                os.fsync(stream.fileno())
            tmp.chmod(0o755)
            os.replace(tmp, dst)
            assert sha(dst.read_bytes()) == expected
            record['files'][file] = {'old': originals[file], 'new': expected}
        if configs:
            run_wrapper(target, 'stop_fr_config_server.sh')
            for _ in range(30):
                if not processes(target, config=True):
                    break
                time.sleep(0.2)
            assert not processes(target, config=True), 'config server stop did not complete'
            run_wrapper(target, 'start_fr_config_server.sh')
            for _ in range(40):
                try:
                    if api(port)[0] == 200:
                        break
                except OSError:
                    pass
                time.sleep(0.25)
            assert api(port)[0] == 200
            wrong_exchange = 'gate' if target.name.startswith('bybit_') else 'bybit'
            status, result = api(port, {'exchange': wrong_exchange, 'fwd_trade_symbols': ['BTCUSDT']})
            assert status == 503 and '名单未更新' in result.get('error', ''), 'guard endpoint is not active'
            status, after = api(port)
            assert status == 200 and after == baseline, 'symbol lists changed during endpoint probe'
            record['guard_rejected_mismatched_account'] = True
        record['after_trading'] = processes(target)
        record['after_config'] = processes(target, config=True)
        assert record['after_trading'] == trading, 'trading process changed'
        if configs:
            assert len(record['after_config']) == 1
        out = target/'run/fr_collateral_hot_update_20261030/installation.json'
        out.write_text(json.dumps(record, indent=2)+'\n')
        records.append(record)
        (ROOT/f'installed_{args.host}.json').write_text(json.dumps(records, indent=2)+'\n')
        print(json.dumps({'env': target.name, 'installed': True, 'config_restarted': bool(configs),
                          'trading_processes_unchanged': True}), flush=True)


if __name__ == '__main__':
    main()
