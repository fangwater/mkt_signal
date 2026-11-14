#!/usr/bin/env python3
"""Publish only the shared editor's Manager authentication gateway."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import urllib.error
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument('phase', choices=['session', 'dry-run', 'activate', 'verify'])
parser.add_argument('--username', choices=['lgq', 'prc'], default='lgq')
args = parser.parse_args()
root = Path.home() / 'crypto_cta_manager'
stage = Path(__file__).resolve().parent
assert stage.parent == root / 'deploy-staging'

if args.phase == 'session':
    sys.path.insert(0, str(root / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
    from manager_ops import session
    with session(args.username) as api:
        token = next(cell.cell_contents for cell in api.__closure__ if isinstance(cell.cell_contents, str))
        print(token, flush=True)  # Consumed in browser-process memory only.
        sys.stdin.readline()
    sys.exit(0)

manifest = json.loads((stage / 'manifest.json').read_text())
target = Path('/etc/nginx/snippets/cta_factor_config.conf')
candidate = stage / 'cta_factor_config.conf'
audit = root / 'maintenance-backups' / stage.name

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def protected():
    paths = [root / 'bin/cta_web', root / 'config/cta-manager.toml']
    for env in ('cta-factor-spread', 'cta-prep-binance-futures'):
        paths.extend(p for p in (Path.home() / env / 'config').rglob('*') if p.is_file())
    for env in ('okex_exec_trade01', 'binance_exec_spread01'):
        paths.extend(p for p in (Path.home() / env / 'config').rglob('*') if p.is_file())
    return {str(path): sha(path) for path in sorted(set(paths))}

def processes():
    result = {}
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            name = (proc / 'comm').read_text().strip()
            if name.startswith(('cta', 'exec-pre-trade', 'trade_engine', 'trade_signal', 'account_monitor', 'binance_account', 'okex_account', 'persist_manager', 'viz_server', 'delist_risk')):
                result[proc.name] = [name, os.readlink(proc / 'cwd'), (proc / 'stat').read_text().split(') ', 1)[1].split()[19], sorted(os.sched_getaffinity(int(proc.name)))]
        except (OSError, ProcessLookupError):
            pass
    return result

def before_state():
    return {'processes': processes(), 'protected': protected(), 'nginx_master_pid': Path('/run/nginx.pid').read_text().strip()}

def status(path):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open('http://127.0.0.1:4191' + path, timeout=15) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code

def verify():
    before = json.loads((audit / 'before.json').read_text())
    assert sha(target) == manifest['candidate_sha256']
    assert before_state() == before, 'Protected runtime identity/configuration changed'
    assert status('/cta/cta-prep-binance-futures/config/?worker=cta-factor-spread') == 401
    return {'release': stage.name, 'gateway_sha256': sha(target), 'protected_processes': len(before['processes']), 'protected_files': len(before['protected']), 'processes_and_affinities_unchanged': True, 'configuration_and_binaries_unchanged': True, 'nginx_master_pid': before['nginx_master_pid'], 'anonymous_status': 401}

assert sha(candidate) == manifest['candidate_sha256']
if args.phase in ('dry-run', 'activate'):
    assert sha(target) == manifest['previous_sha256'], 'Live gateway changed; inspect before publishing'
    validation = stage / 'nginx-validation.conf'
    validation.write_text('worker_processes 1;\npid ' + str(stage / 'validation.pid') + ';\nerror_log stderr;\nevents { worker_connections 32; }\nhttp { access_log off; server { listen 127.0.0.1:19489; include ' + str(candidate) + '; } }\n')
    subprocess.run(['sudo', '-n', 'nginx', '-t', '-p', str(stage), '-c', str(validation)], check=True)
    if args.phase == 'dry-run':
        print(json.dumps({'execute': False, 'candidate_hash_verified': True, 'previous_hash_verified': True, 'nginx_syntax': 'passed'}))
        sys.exit(0)
    audit.mkdir(mode=0o700)
    (audit / 'before.json').write_text(json.dumps(before_state(), indent=2) + '\n')
    shutil.copy2(target, audit / 'cta_factor_config.conf.before')
    shutil.copy2(candidate, audit / 'cta_factor_config.conf.after')
    shutil.copy2(stage / 'manifest.json', audit / 'manifest.json')
    shutil.copy2(__file__, audit / 'gateway.py')
    nxt = str(target) + '.' + stage.name + '.next'
    subprocess.run(['sudo', '-n', 'install', '-m', '0644', str(candidate), nxt], check=True)
    assert sha(target) == manifest['previous_sha256']
    subprocess.run(['sudo', '-n', 'mv', nxt, str(target)], check=True)
    try:
        subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True)
        subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
        result = verify()
    except Exception:
        subprocess.run(['sudo', '-n', 'install', '-m', '0644', str(audit / 'cta_factor_config.conf.before'), str(target)], check=True)
        subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True)
        subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
        raise
else:
    result = verify()
(audit / 'verification.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
