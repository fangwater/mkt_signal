"""Publish verified local builds and reset only the stopped Spread shadow ledger."""
import datetime
import hashlib
import json
import os
import shutil
import subprocess
import sys
import urllib.request
from pathlib import Path

root = Path.home() / 'binance-cta-spread-rx01'
release = root / 'run/corr50_warmup_20261019T064500Z'
stage = release / 'stage'
assert Path.cwd() == root
for proc in Path('/proc').glob('[0-9]*'):
    try:
        cwd = os.readlink(proc / 'cwd')
        comm = (proc / 'comm').read_text().strip()
        if cwd == str(root):
            assert comm not in {'cta_spread_fact', 'cta_spread_sign'}, (proc.name, comm)
        assert cwd != str(Path.home() / 'binance_exec_trade06') or comm in {'bash', 'python3'}, (proc.name, comm)
    except (OSError, ProcessLookupError):
        pass
config_path = root / 'config/cta_spread.json'
config = json.loads(config_path.read_text())
assert config['enabled'] is False and config['max_pos_u'] == 100
table = json.loads((root / 'config/cta_spread_symbol_factors.json').read_text())
live = json.load(urllib.request.urlopen('http://127.0.0.1:19174/api/symbol-factors', timeout=5))
assert live['symbol_factors'] == table
assert len(table) == 50 and sum(map(len, table.values())) == 873
state = root / 'run/cta_spread_position_flags.json'
assert hashlib.sha256(state.read_bytes()).hexdigest() == '4ca0335f89617c592fcffee66ead4f8a0b5a20aca784f8d510876a1643cb11db'
old = json.loads(state.read_text())
manifest = json.loads((stage / 'manifest.json').read_text())
for name, expected in manifest.items():
    assert hashlib.sha256((stage / name).read_bytes()).hexdigest() == expected, name
    if name.endswith('.sh'):
        subprocess.run(['bash', '-n', str(stage / name)], check=True)
backup = release / 'backup'
backup.mkdir(exist_ok=False)
for name in list(manifest) + ['run/cta_spread_position_flags.json', 'run/cta_spread_status.json', 'config/cta_spread.json', 'config/cta_spread_symbol_factors.json']:
    source = root / name
    if source.exists():
        target = backup / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
for name in manifest:
    target = root / name
    temporary = target.with_name('.' + target.name + '.warmup-upload')
    shutil.copyfile(stage / name, temporary)
    temporary.chmod(0o755)
    with temporary.open('rb') as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == manifest[name]
positions = {}
for symbol, row in table.items():
    for factor, sign in row.items():
        if sign:
            positions.setdefault(factor, {})[symbol] = dict(flag=0, qty=0.0, cash_qty=0.0, perpetual_qty=0.0, entry_notional=0.0)
assert len(positions) == 57 and sum(map(len, positions.values())) == 873
temporary = state.with_suffix('.json.warmup-reset')
with temporary.open('w') as handle:
    json.dump(positions, handle, separators=(',', ':'))
    handle.write('\n')
    handle.flush()
    os.fsync(handle.fileno())
os.replace(temporary, state)
for directory in [root, root / 'scripts', state.parent]:
    fd = os.open(directory, os.O_DIRECTORY)
    os.fsync(fd)
    os.close(fd)
receipt = dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), files_verified=len(manifest),
               previous_strategies=len(old), previous_combinations=sum(map(len, old.values())),
               previous_active=sum(p['flag'] != 0 for row in old.values() for p in row.values()),
               strategies=len(positions), combinations=sum(map(len, positions.values())), enabled=False,
               backup=str(backup), state_sha256=hashlib.sha256(state.read_bytes()).hexdigest())
(release / 'receipt.json').write_text(json.dumps(receipt, indent=2))
print(json.dumps(receipt))
