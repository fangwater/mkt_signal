"""Install Spread defaults only; retain runtime config, ledger and Kafka generation."""
import argparse
import datetime
import hashlib
import json
import os
import shutil
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--execute', action='store_true')
execute = parser.parse_args().execute
root = Path.home() / 'binance-cta-spread-rx01'
release = root / 'run/spread_exit_defaults_20261019T072100Z'
stage = release / 'stage'
manifest = json.loads((stage / 'manifest.json').read_text())
for name, expected in manifest.items():
    assert hashlib.sha256((stage / name).read_bytes()).hexdigest() == expected, name
config_path = root / 'config/cta_spread.json'
config_bytes = config_path.read_bytes()
config = json.loads(config_bytes)
assert not config['enabled'] and config['max_pos_u'] == 100
assert config['execution'] == dict(factor_exit_quantile_long=0.7, factor_exit_quantile_short=0.3)
positions_path = root / 'run/cta_spread_position_flags.json'
positions_bytes = positions_path.read_bytes()
positions = json.loads(positions_bytes)
assert sum(map(len, positions.values())) == 794
assert all(all(value == 0 for value in position.values()) for row in positions.values() for position in row.values())
warmup_path = root / 'data/cta_spread/factor_history/warmup.json'
warmup = json.loads(warmup_path.read_text())
assert warmup['caught_up'] and warmup['generation'] == '9389c457-cff7-4fce-a392-90f8f9ea438d'
perp = {name: hashlib.sha256((Path.home() / name / 'config/cta_perp.json').read_bytes()).hexdigest()
        for name in ['binance-cta-perp-rx02', 'binance-cta-perp-rx03']}
plan = dict(execute=execute, files_verified=len(manifest), config_preserved=True, state_preserved=True,
            factor_generation=warmup['generation'], publisher_restart=False, perp_config_sha256=perp)
print(json.dumps(plan))
if not execute:
    raise SystemExit(0)
for proc in Path('/proc').glob('[0-9]*'):
    try:
        cwd = os.readlink(proc / 'cwd')
        if cwd != str(root):
            continue
        comm = (proc / 'comm').read_text().strip()
        cmd = (proc / 'cmdline').read_bytes().decode(errors='replace')
        assert comm != 'cta_spread_sign', proc.name
        assert not (comm.startswith('python') and 'cta_spread_config_server.py' in cmd), proc.name
    except OSError:
        pass
backup = release / 'backup'
backup.mkdir(exist_ok=False)
for name in list(manifest) + ['config/cta_spread.json', 'run/cta_spread_position_flags.json']:
    target = backup / name
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(root / name, target)
for name in manifest:
    target = root / name
    temporary = target.with_name('.' + target.name + '.exit-default-upload')
    shutil.copy2(stage / name, temporary)
    with temporary.open('rb') as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, target)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == manifest[name], name
assert config_bytes == config_path.read_bytes()
assert positions_bytes == positions_path.read_bytes()
assert json.loads(warmup_path.read_text())['generation'] == warmup['generation']
assert all(hashlib.sha256((Path.home() / name / 'config/cta_perp.json').read_bytes()).hexdigest() == digest
           for name, digest in perp.items())
receipt = dict(plan, observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), backup=str(backup),
               runtime_config_sha256=hashlib.sha256(config_bytes).hexdigest(),
               state_sha256=hashlib.sha256(positions_bytes).hexdigest())
(release / 'receipt.json').write_text(json.dumps(receipt, indent=2))
print(json.dumps(receipt))
