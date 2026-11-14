#!/usr/bin/env python3
"""Install configuration-only skeletons; leave all live settings/processes alone."""
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import tarfile
import tomllib

home = Path.home()
stage = home / '.cta_cross_spread_prepare_20261102'
manager = home / 'crypto_cta_manager'
live_config = manager / 'config/cta-manager.toml'
audit = manager / 'maintenance-backups/cta_cross_spread_prepare_20261102'
targets = ['okex_exec_trade01', 'okex-binance-cta-spread-rx01']
protected = [live_config, manager / 'bin/cta_web'] + [home / env / name for env in ['okex_mm_alpha', 'binance_exec_trade08'] for name in ['env.sh', 'trade_engine.toml']]


def hashes():
    return {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in protected}


def manager_pids():
    found = []
    for p in Path('/proc').iterdir():
        try:
            if p.name.isdigit() and (p / 'comm').read_text().strip() == 'cta_web':
                found.append(int(p.name))
        except OSError:
            pass
    return sorted(found)


def write_new(path, text, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
    with os.fdopen(fd, 'w') as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())


assert all(not (home / name).exists() for name in targets), 'target already exists; inspect instead of replacing it'
assert not audit.exists(), 'preparation audit already exists'
raw = live_config.read_text()
config = tomllib.loads(raw)
assert next(s for s in config['sources'] if s['id'] == 'binance_exec_trade08')['enabled'] is False
assert not any(s['id'] == 'okex_exec_trade01' for s in config['sources'])
before, pids_before = hashes(), manager_pids()
audit.mkdir(mode=0o700)
write_new(audit / 'cta-manager.toml.before', raw)
with tarfile.open(stage / 'config_payload.tar.gz') as tar:
    for member in tar.getmembers():
        parts = Path(member.name).parts
        assert parts and parts[0] in targets and not Path(member.name).is_absolute() and '..' not in parts
        assert member.isfile() or member.isdir()
    tar.extractall(home, filter='data')

# Copy only the three requested credentials in memory, never through output or
# process arguments. Do not source the MM namespace or its obsolete CPU pins.
envraw = subprocess.run(['bash', '-c', 'set -a; source "$1" >/dev/null 2>&1; env -0', 'bash', str(home / 'okex_mm_alpha/env.sh')], capture_output=True, check=True).stdout
env = dict(x.decode().split('=', 1) for x in envraw.split(b'\0') if b'=' in x)
keys = ['OKX_API_KEY', 'OKX_API_SECRET', 'OKX_PASSPHRASE']
assert all(env.get(k) for k in keys)
settings = {
    'IPC_NAMESPACE': 'okex_exec_trade01', 'EXEC_VENUE': 'okex-futures',
    'VENUE': 'okex-futures', 'EXEC_INSTANCE': '01',
    'TRADE_ENGINE_EXEC_BACKEND_MAP': 'okex=native',
    'EXEC_VIZ_PORT': '10049', 'EXEC_CONFIG_PORT': '18169',
    'EXEC_CONFIG_RELOAD_MS': '30000', 'ACCOUNT_MONITOR_CORE': '21',
    'EXEC_PRE_TRADE_CORE': '22', 'PRE_TRADE_CORE': '22', 'TRADE_ENGINE_CORE': '23',
    'TRADE_SIGNAL_ENABLE_QUEUE_POSITION': '0', 'enable_ipc_fast_poll': '0', 'ENABLE_IPC_FAST_POLL': '0',
}
settings.update({k: env[k] for k in keys})
write_new(home / targets[0] / 'env.sh', '#!/usr/bin/env bash\n' + ''.join(f'export {k}={shlex.quote(v)}\n' for k, v in settings.items()))

fragment_text = (stage / 'cta-cross-spread.toml').read_text()
fragment = tomllib.loads(fragment_text)
source = fragment['sources'][0]
assert source['enabled'] is False and source['monitor_enabled'] is False
source_text = fragment_text[fragment_text.index('[[sources]]'):]
candidate = raw.rstrip() + '\n\n' + source_text
pattern = r'(?m)^\[dashboard\.source_viewers\][ \t]*$'
assert len(re.findall(pattern, candidate)) == 1
candidate = re.sub(pattern, '[dashboard.source_viewers]\nokex_exec_trade01 = ["lgq"]', candidate)
expected = copy.deepcopy(config)
expected['dashboard']['source_viewers']['okex_exec_trade01'] = ['lgq']
expected['sources'].append(source)
# Add the new trading config to each existing fixed-egress exclusion list.
for section in ['kline', 'funding']:
    existing = expected.get(section, {}).get('trade_engine_configs')
    if existing is not None:
        new_path = str(home / 'okex_exec_trade01/trade_engine.toml')
        if new_path not in existing:
            existing.append(new_path)
        section_pattern = rf'(?ms)(^\[{section}\][ \t]*\n)(.*?)(?=^\[|\Z)'
        match = re.search(section_pattern, candidate)
        assert match
        body = match.group(2)
        rewritten, count = re.subn(r'(?ms)^trade_engine_configs\s*=\s*\[.*?\]', 'trade_engine_configs = ' + json.dumps(existing), body, count=1)
        assert count == 1
        candidate = candidate[:match.start(2)] + rewritten + candidate[match.end(2):]
assert tomllib.loads(candidate) == expected, 'unexpected candidate change'
write_new(audit / 'cta-manager.toml.candidate', candidate)
write_new(audit / 'cta-cross-spread.toml', fragment_text)
assert before == hashes(), 'protected live file changed during configuration preparation'
assert pids_before == manager_pids(), 'Manager process changed during preparation'
receipt = {
    'configuration_only': True, 'manager_live_config_changed': False,
    'manager_restarted': False, 'manager_pids': pids_before,
    'protected_files_unchanged': before,
    'created_envs': [str(home / n) for n in targets],
    'manager_candidate': str(audit / 'cta-manager.toml.candidate'),
    'binance_source_enabled': False, 'okex_source_enabled': False,
    'credential_source': 'okex_mm_alpha/env.sh (three OKX variables only)',
    'pending': ['live Manager release with cross-spread support', 'resolve IOTAUSDT -0.1 residual', 'reviewed symbol/factor list', 'build/publish Exec and signal binaries', 'account rules/risk configuration and paired strategy bindings', 'authenticated gateway route for /exec_okex01'],
}
write_new(audit / 'receipt.json', json.dumps(receipt, indent=2) + '\n')
print(json.dumps(receipt, indent=2))
