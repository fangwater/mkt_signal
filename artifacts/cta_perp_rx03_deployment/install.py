from pathlib import Path
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tarfile
import time

home = Path.home()
target = home / 'binance-cta-perp-rx03'
stage = home / '.cta_perp_rx03_stage'
archive = Path('/tmp/cta_perp_rx03_payload.tar.gz')
assert (home / 'spread_pbs/binance-futures').is_dir()
assert (home / 'binance-cta-perp-rx02').is_dir()
assert not target.exists(), 'refuse to overwrite an existing environment'
assert not stage.exists(), 'refuse to overwrite staging'
assert hashlib.sha256(archive.read_bytes()).hexdigest() == sys.argv[1]
for port in (10197, 19187):
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', port))
stage.mkdir(mode=0o700)
with tarfile.open(archive) as bundle:
    bundle.extractall(stage, filter='data')
subprocess.run(['sha256sum', '-c', 'SHA256SUMS'], cwd=stage, check=True, stdout=subprocess.DEVNULL)
cfg = json.loads((stage / 'config/cta_perp.json').read_text())
assert cfg['enabled'] is False and cfg['max_pos_u'] == 100
table = json.loads((stage / 'config/cta_perp_symbol_factors.json').read_text())
assert len(table) == 14 and sum(map(len, table.values())) == 34
for directory in ('run', 'data', 'logs'):
    (stage / directory).mkdir(exist_ok=True)
stage.rename(target)
subprocess.run(['python3', 'scripts/cta_perp_factor_config.py', '--env-name', target.name,
                '--config', 'config/cta_perp_symbol_factors.json'], cwd=target, check=True,
               stdout=subprocess.DEVNULL)
import redis
client = redis.Redis(socket_connect_timeout=3, socket_timeout=3)
assert client.get(target.name + ':cta_symbol_factors') is None, 'existing Redis key requires review'
subprocess.run(['python3', 'scripts/cta_perp_factor_config.py', '--env-name', target.name,
                '--config', 'config/cta_perp_symbol_factors.json', '--execute'], cwd=target,
               check=True, stdout=subprocess.DEVNULL)
subprocess.run(['./scripts/start_cta_perp.sh'], cwd=target, check=True)
subprocess.run(['./scripts/start_cta_perp.sh', '--execute'], cwd=target, check=True)
print(json.dumps({'installed': str(target), 'started_at': time.time(), 'enabled': False,
                  'publisher_scope': 'binance_cta_perp_rx03', 'ports': [10197, 19187]}))
