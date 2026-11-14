"""Store a new factual RX01 account and inventory allocation anchor."""
import importlib.util
import json
import os
import subprocess
import time
from decimal import Decimal
from pathlib import Path

home = Path.home()
root = home / 'binance-cta-spread-rx01'
exe = home / 'binance_exec_trade06'
manager = home / 'crypto_cta_manager'
stage = root / 'run/reenable_20261019'
spec = importlib.util.spec_from_file_location('ops_helpers', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
assert not any(p['comm'] in {'exec-pre-trade', 'trade_engine', 'trade_signal'}
               for p in ops.live_pids(exe))
assert (stage / 'catalog_prepared.json').exists()
assert not (stage / 'snapshots_stored.json').exists()
plan = json.loads((stage / 'plan.json').read_text())
assert time.time_ns() // 1000 - plan['prepared_ts_us'] < 900_000_000
pid = subprocess.check_output(['systemctl', '--user', 'show', 'crypto-cta-manager-web.service',
                               '-p', 'MainPID', '--value'], text=True).strip()
values = dict(x.decode().split('=', 1) for x in Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
base = ['--config', str(manager / 'config/cta-manager.toml'), '--source', exe.name,
        '--snapshot-ts-us', str(plan['prepared_ts_us']), '--note',
        'RX01 corr50 flat restart: preserve factual cash minus debt and USDT perpetual holdings '
        'under the inventory strategy; 57 factor strategies start flat; no historical order rewrite']
account = []
strategy = []
for row in plan['inventory_snapshot']:
    for symbol, venue, qty in [(row['coin'], 0, row['cash_qty']),
                               (row['coin'] + 'USDT', 1, row['perpetual_qty'])]:
        if Decimal(qty):
            value = f"{symbol}:{venue}:{qty}:{row['mark']}"
            account += ['--position', value]
            strategy += ['--position', ops.PREFIX + 'inventory:' + value]
result = {}
checked = subprocess.run([str(manager / 'bin/nav_strategy_snapshot'), *base, *strategy, '--dry-run'],
                         env=values, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
assert checked.returncode == 0, 'strategy baseline validation failed; details retained on host'
for binary, positions in [('nav_snapshot', account), ('nav_strategy_snapshot', strategy)]:
    completed = subprocess.run([str(manager / 'bin' / binary), *base, *positions], env=values,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if completed.returncode:
        path = stage / (binary + '.error.log')
        path.write_text(completed.stderr)
        os.chmod(path, 0o600)
        raise RuntimeError(binary + ' failed; details retained on host')
    result[binary] = json.loads(completed.stdout)
path = stage / 'snapshots_stored.json'
path.write_text(json.dumps(result, indent=2) + '\n')
os.chmod(path, 0o600)
print(json.dumps(dict(snapshot_ts_us=plan['prepared_ts_us'], account_positions=len(account) // 2,
    inventory_positions=len(strategy) // 2, factor_initial_positions=0,
    historical_orders_changed=False)))
