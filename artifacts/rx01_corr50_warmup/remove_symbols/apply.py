"""Remove two RX01 bindings; default is a read-only plan, never submit orders."""
import argparse
import datetime
import hashlib
import json
import os
import shutil
import tomllib
import urllib.request
from pathlib import Path

args = argparse.ArgumentParser()
args.add_argument('--execute', action='store_true')
execute = args.parse_args().execute
root = Path.home() / 'binance-cta-spread-rx01'
release = root / 'run/corr50_remove_symbols_20261019T065917Z'
removed = {'ICXUSDT', 'STORJUSDT'}
manifest = json.loads((release / 'stage/manifest.json').read_text())
before_hashes = {
    'config/cta_spread_symbol_factors.json': '5617cc0838443c7d52c4c6cef3d28440823abd01e44f4c1be1a98b8995189e18',
    'config/cta_spread_factor_model_1m_pub.toml': '3aa497a225422b2457c03e48374d85c64909f870858da667a841f62e016e3b1d',
    'config/cta_spread_deployment.json': '5ae8c39b61dfdafb8726884454c8de891b6dd26897f72e1184ae1db073ad302e',
}
for name, digest in manifest.items():
    assert hashlib.sha256((release / 'stage' / name).read_bytes()).hexdigest() == digest, name
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == before_hashes[name], name
config_bytes = (root / 'config/cta_spread.json').read_bytes()
config = json.loads(config_bytes)
assert config['enabled'] is False and config['max_pos_u'] == 100
old = json.loads((root / 'config/cta_spread_symbol_factors.json').read_text())
table = json.loads((release / 'stage/config/cta_spread_symbol_factors.json').read_text())
assert table == {symbol: row for symbol, row in old.items() if symbol not in removed}
assert len(table) == 48 and sum(map(len, table.values())) == 794
assert {factor for row in table.values() for factor in row} == {factor for row in old.values() for factor in row}
old_universe = tomllib.loads((root / 'config/cta_spread_factor_model_1m_pub.toml').read_text())
new_universe = tomllib.loads((release / 'stage/config/cta_spread_factor_model_1m_pub.toml').read_text())
old_universe['universe']['symbols'] = [symbol for symbol in old_universe['universe']['symbols'] if symbol not in removed]
assert old_universe == new_universe
state = root / 'run/cta_spread_position_flags.json'
positions = json.loads(state.read_text())
assert all(all(value == 0 for value in position.values()) for row in positions.values() for position in row.values())
current = json.load(urllib.request.urlopen('http://127.0.0.1:19174/api/symbol-factors', timeout=5))
assert current['symbol_factors'] == old
warmup_path = root / 'data/cta_spread/factor_history/warmup.json'
generation = json.loads(warmup_path.read_text())['generation']
plan = dict(removed=sorted(removed), removed_combinations=sum(len(old[symbol]) for symbol in removed),
            symbols=48, strategies=57, combinations=794, enabled=False, factor_generation=generation,
            publisher_restart=False, execute=execute)
print(json.dumps(plan, ensure_ascii=False))
if not execute:
    raise SystemExit(0)
for proc in Path('/proc').glob('[0-9]*'):
    try:
        cwd = os.readlink(proc / 'cwd')
        comm = (proc / 'comm').read_text().strip()
        assert not (cwd == str(root) and comm == 'cta_spread_sign'), (proc.name, comm)
        assert cwd != str(Path.home() / 'binance_exec_trade06') or comm in {'python3', 'bash'}, (proc.name, comm)
    except OSError:
        pass
backup = release / 'backup'
backup.mkdir(exist_ok=False)
for name in list(manifest) + ['run/cta_spread_position_flags.json', 'run/cta_spread_status.json']:
    target = backup / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root / name, target)
(backup / 'redis_table.json').write_text(json.dumps(current, indent=2))
for name in manifest:
    target = root / name
    temporary = target.with_name('.' + target.name + '.remove-symbols')
    shutil.copy2(release / 'stage' / name, temporary)
    with temporary.open('rb') as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, target)
request = urllib.request.Request('http://127.0.0.1:19174/api/symbol-factors', method='POST',
                                 data=json.dumps(dict(symbol_factors=table, expected_revision=current['revision'])).encode(),
                                 headers={'Content-Type': 'application/json'})
saved = json.load(urllib.request.urlopen(request, timeout=5))
assert saved['symbol_factors'] == table
positions = {factor: {symbol: position for symbol, position in row.items() if symbol not in removed}
             for factor, row in positions.items()}
assert len(positions) == 57 and sum(map(len, positions.values())) == 794
temporary = state.with_suffix('.json.remove-symbols')
with temporary.open('w') as handle:
    json.dump(positions, handle, separators=(',', ':'))
    handle.write('\n')
    handle.flush()
    os.fsync(handle.fileno())
os.replace(temporary, state)
for directory in [root / 'config', root / 'run']:
    fd = os.open(directory, os.O_DIRECTORY)
    os.fsync(fd)
    os.close(fd)
assert config_bytes == (root / 'config/cta_spread.json').read_bytes()
assert json.loads(warmup_path.read_text())['generation'] == generation
assert all(hashlib.sha256((root / name).read_bytes()).hexdigest() == digest for name, digest in manifest.items())
receipt = dict(plan, observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
               revision=saved['revision'], backup=str(backup), files_verified=len(manifest))
(release / 'receipt.json').write_text(json.dumps(receipt, indent=2))
print(json.dumps(receipt, ensure_ascii=False))
