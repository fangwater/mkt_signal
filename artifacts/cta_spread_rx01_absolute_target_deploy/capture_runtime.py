"""Read-only deployment evidence; never output environment or config contents."""

import hashlib
import json
import os
import pathlib
import time
import urllib.request

HOME_ROOT = pathlib.Path('/home/ubuntu')
SIGNAL_ROOT = HOME_ROOT / 'binance-cta-spread-rx01'
roots = [*HOME_ROOT.glob('binance_exec_trade*')]
roots += [SIGNAL_ROOT, HOME_ROOT / 'binance-cta-perp-rx02', HOME_ROOT / 'crypto_cta_manager']
root_names = {str(root) for root in roots if root.is_dir()}
processes = []
for entry in pathlib.Path('/proc').iterdir():
    if not entry.name.isdigit():
        continue
    try:
        cwd = os.readlink(entry / 'cwd')
        if cwd in root_names:
            processes.append({
                'pid': int(entry.name), 'cwd': cwd,
                'exe': os.readlink(entry / 'exe'),
                'affinity': sorted(os.sched_getaffinity(int(entry.name))),
            })
    except (OSError, PermissionError):
        continue

paths = [SIGNAL_ROOT / 'cta_spread_signal']
for root in roots:
    paths.extend([root / 'env.sh', root / 'trade_engine.toml'])
paths += [SIGNAL_ROOT / 'config/cta_spread.json']
hashes = {
    str(path): hashlib.sha256(path.read_bytes()).hexdigest()
    for path in paths if path.is_file()
}
positions = json.loads((SIGNAL_ROOT / 'run/cta_spread_position_flags.json').read_text())
position_hash = hashlib.sha256(json.dumps(positions, sort_keys=True).encode()).hexdigest()
status = json.loads((SIGNAL_ROOT / 'run/cta_spread_status.json').read_text())
with urllib.request.urlopen('http://127.0.0.1:19174/api/config', timeout=10) as response:
    config = json.load(response)
with urllib.request.urlopen('http://127.0.0.1:18166/api/strategies', timeout=10) as response:
    exec_strategies = json.load(response)
print(json.dumps({
    'captured_at_us': time.time_ns() // 1000,
    'processes': sorted(processes, key=lambda process: process['pid']),
    'hashes': hashes,
    'position_state_hash': position_hash,
    'config_enabled': config['config']['enabled'],
    'status': {key: status.get(key) for key in [
        'enabled', 'journal_healthy', 'publication_error', 'max_pos_u',
        'updated_ts_us', 'scheduled_entries',
    ]},
    'observation_count': sum(len(rows) for rows in status['strategies'].values()),
    'ready_observation_count': sum(
        bool(row.get('score_ready')) for rows in status['strategies'].values()
        for row in rows.values()
    ),
    'latest_model_ts_ms': max(
        (row.get('model_ts_ms', 0) for rows in status['strategies'].values()
         for row in rows.values()), default=0,
    ),
    'exec_strategy_catalog_hash': hashlib.sha256(
        json.dumps(exec_strategies, sort_keys=True).encode()
    ).hexdigest(),
}, indent=2))
