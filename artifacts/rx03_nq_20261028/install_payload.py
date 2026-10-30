import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import tomllib

root = Path('/home/ubuntu/binance-cta-perp-rx03')
backup = root / 'run/nq_backup_20261028T094509Z'
staging = backup / 'staging'
manifest = json.loads((staging / 'manifest.json').read_text())
for relative, expected in manifest.items():
    assert hashlib.sha256((staging / relative).read_bytes()).hexdigest() == expected, relative
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():
        continue
    try:
        cwd = str((proc / 'cwd').resolve())
        comm = (proc / 'comm').read_text().strip()
        argv = (proc / 'cmdline').read_bytes().split(b'\0')
        assert not (cwd == str(root) and comm in ('cta_perp_signal', 'cta_perp_factor')), comm
        assert not (cwd == str(root) and any(a.endswith(b'/cta_perp_config_server.py') for a in argv)), 'config server running'
        assert not (cwd == '/home/ubuntu/binance_exec_trade07' and comm == 'exec-pre-trade'), comm
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        pass

def atomic(path, payload, mode):
    fd, tmp = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as output:
            os.fchmod(output.fileno(), mode)
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)

for relative in manifest:
    original = root / relative
    saved = backup / 'previous_artifacts' / relative
    saved.parent.mkdir(parents=True, exist_ok=True)
    assert not saved.exists(), 'refuse overwrite backup'
    shutil.copy2(original, saved)
for relative in manifest:
    path = root / relative
    atomic(path, (staging / relative).read_bytes(), path.stat().st_mode & 0o777)

publisher = root / 'config/cta_perp_factor_model_1m_pub.toml'
old = tomllib.loads(publisher.read_text())
universe = json.loads((staging / 'publisher_universe.json').read_text())
replacement = '[universe]\nsymbols = ' + json.dumps(universe['symbols']) + '\nfactors = ' + json.dumps(universe['factors']) + '\n\n'
text, count = re.subn(r'(?ms)^\[universe\]\s*.*?(?=^\[|\Z)', replacement, publisher.read_text())
assert count == 1
new = tomllib.loads(text)
assert {k:v for k,v in old.items() if k != 'universe'} == {k:v for k,v in new.items() if k != 'universe'}
atomic(publisher, text.encode(), publisher.stat().st_mode & 0o777)

sys.path.insert(0, str(root / 'scripts'))
from cta_perp_config_server import ConfigStore, normalize_config
store = ConfigStore(root / 'config/cta_perp.json')
current, revision = store.load()
assert current['enabled'] is False
assert current['max_pos_u'] == 300
assert current['bar_interval'] == '1m' and current['position_algorithm'] == 'v0'
assert not current['factor_rules']
rules = json.loads((staging / 'operator_factor_rules.json').read_text())
assert len(rules) == 45
config, final_revision = store.save({**current, 'factor_rules': rules}, revision)
assert {k:v for k,v in current.items() if k != 'factor_rules'} == {k:v for k,v in config.items() if k != 'factor_rules'}
for relative, expected in manifest.items():
    assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected
report = {'manifest': manifest, 'config_revision': final_revision, 'rules': len(config['factor_rules']), 'symbols': len(universe['symbols']), 'factors': len(universe['factors']), 'enabled': config['enabled'], 'max_pos_u': config['max_pos_u']}
(backup / 'installation.json').write_text(json.dumps(report, indent=2))
print(json.dumps(report))
