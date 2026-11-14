#!/usr/bin/env python3
"""Publish only Delist CTA navigation and its Nginx gate; dry-run by default."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

stage = Path(__file__).resolve().parent
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--execute', action='store_true')
args = parser.parse_args()
sys.path.insert(0, str(Path.home() / 'cta-factor-spread/run/spread02_20261112T055433Z'))
import deploy
manifest = json.loads((stage / 'manifest.json').read_text())
targets = {'web': Path.home() / 'delist_risk_server/web/delist_risk/index.html',
           'nginx': Path('/etc/nginx/snippets/cta_factor_config.conf')}
candidates = {'web': stage / 'index.html', 'nginx': stage / 'cta_factor_config.conf'}
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
for kind, path in targets.items():
    assert sha(path) == manifest['before'][kind], 'live file changed: '+str(path)
    assert sha(candidates[kind]) == manifest['after'][kind]
validation = stage / 'validation.conf'
validation.write_text(f'worker_processes 1; pid {stage}/validation.pid; error_log stderr;\n'
                      f'events {{ worker_connections 32; }}\nhttp {{ access_log off; '
                      f'server {{ listen 127.0.0.1:19489; include {candidates["nginx"]}; }} }}\n')
subprocess.run(['sudo', '-n', 'nginx', '-t', '-p', str(stage), '-c', str(validation)], check=True)
print(json.dumps({'execute': args.execute, 'scope': 'Delist CTA links and login gateway',
                  'files': [str(p) for p in targets.values()], 'nginx_syntax': 'passed'}), flush=True)
if not args.execute:
    sys.exit(0)
before = stage / 'before'
before.mkdir(mode=0o700)
processes = deploy.processes()
master = Path('/run/nginx.pid').read_text().strip()
protected = {}
for env in ['cta-factor-spread', 'cta-prep-binance-futures']:
    for path in (Path.home() / env / 'config').rglob('*'):
        if path.is_file():
            protected[str(path)] = sha(path)
for kind, path in targets.items():
    shutil.copy2(path, before / (kind+'.before'))
(before / 'processes.json').write_text(json.dumps(processes, indent=2)+'\n')
(before / 'configuration_hashes.json').write_text(json.dumps(protected, indent=2)+'\n')
for kind, path in targets.items():
    temporary = str(path)+'.spread02-next'
    subprocess.run(['sudo', '-n', 'install', '-m', '0644', str(candidates[kind]), temporary], check=True)
    subprocess.run(['sudo', '-n', 'mv', temporary, str(path)], check=True)
try:
    subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True)
    subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
except Exception:
    for kind, path in targets.items():
        subprocess.run(['sudo', '-n', 'install', '-m', '0644', str(before/(kind+'.before')), str(path)], check=True)
    subprocess.run(['sudo', '-n', 'nginx', '-t'], check=True)
    subprocess.run(['sudo', '-n', 'systemctl', 'reload', 'nginx'], check=True)
    raise
assert Path('/run/nginx.pid').read_text().strip() == master
assert all(sha(Path(path)) == expected for path, expected in protected.items())
current = deploy.processes()
changed = {pid: item for pid, item in processes.items() if current.get(pid) != item}
assert not changed, changed
assert all(sha(path) == manifest['after'][kind] for kind, path in targets.items())
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
statuses = {}
for path in ['/delist/cta/config/?worker=cta-factor-spread',
             '/delist/cta/config/api/config?worker=cta-factor-spread',
             '/cta/cta-prep-binance-futures/config/?worker=cta-factor-spread',
             '/delist-auth/cta-factor-config']:
    try:
        response = opener.open('http://127.0.0.1:4191'+path, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    statuses[path] = response.status
assert list(statuses.values()) == [401, 401, 401, 404], statuses
with opener.open('http://127.0.0.1:19220/api/config?worker=cta-factor-spread') as response:
    catalog = json.load(response)
spread02 = [c for c in catalog['plan']['combinations'].values() if c['strategy_group'] == 'spread02']
assert len(spread02) == 4 and all(not c['signal']['enabled'] for c in spread02)
report = {'verified_at_ms': int(time.time()*1000), 'nginx_master_pid': master,
          'protected_processes': len(processes), 'changed_processes': changed,
          'protected_configs': len(protected), 'configurations_unchanged': True,
          'file_hashes': manifest['after'], 'anonymous_statuses': statuses,
          'spread02_enabled': False, 'rollback_directory': str(before)}
(stage / 'verified.json').write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps(report))
