"""Create a new immutable factual anchor after RX01 liquidation has settled."""
import datetime
import importlib.util
import json
import math
import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path
import redis

h = Path.home()
root = h / 'binance-cta-spread-rx01'
exe = h / 'binance_exec_trade06'
manager = h / 'crypto_cta_manager'
stage = root / 'run/reenable_20261019'
spec = importlib.util.spec_from_file_location('ops', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
assert not (stage / 'post_flat_snapshots.json').exists(), 'post-flat anchor already exists'
assert not any(p['comm'] in {'exec-pre-trade', 'trade_signal'} for p in ops.live_pids(exe))
cfg = json.loads((root / 'config/cta_spread.json').read_text())
assert not cfg['enabled']
flags = json.loads((root / 'run/cta_spread_position_flags.json').read_text())
assert sum(map(len, flags.values())) == 794
assert all(not any(p.values()) for row in flags.values() for p in row.values())
values = ops.env_for(exe)
os.environ.update(values)
sys.path.insert(0, str(h / 'binance-cta-rx01/scripts'))
from neutralize_rapidx_binance import Client
c = Client('172.31.35.228')
c.empty_orders()
cash1, perp1, marks1 = c.snapshot()
cash, perp, marks = c.snapshot()
assert cash == cash1 and perp == perp1, 'holdings are not settled'
assert not any(perp.values()), 'perpetual positions remain'
assets = c.pages('/api/v1/trading/portfolio/assets', {'exchangeType': 'BINANCE'})
for row in assets:
    c.identity(row)
    assert Decimal(row['debt']) == 0 and Decimal(row['frozen']) == 0, 'debt or frozen funds remain'
gross = sum(abs(v) * marks[k] for k, v in cash.items() if k != 'USDT')
assert gross < 4, 'remaining cash exceeds expected exchange dust'
ts = time.time_ns() // 1000
backup = stage / ('post_flat_backup_' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
backup.mkdir(mode=0o700)
with (backup / 'manager.pgdump').open('wb') as f:
    os.chmod(f.name, 0o600)
    result = subprocess.run(['pg_dump', '--format=custom', '--no-owner', '--no-acl'],
                            env=ops.db_env(), stdout=f, stderr=subprocess.PIPE)
    assert result.returncode == 0, 'database backup failed'
r = redis.Redis(host=values.get('REDIS_HOST', '127.0.0.1'),
    port=int(values.get('REDIS_PORT', 6379)), db=int(values.get('REDIS_DB', 0)),
    username=values.get('REDIS_USERNAME'), password=values.get('REDIS_PASSWORD'),
    socket_connect_timeout=3, socket_timeout=3, decode_responses=True)
keys = [f'{exe.name}:{venue}:batch_exec_state:position_allocations'
        for venue in ['binance-margin', 'binance-futures']]
before = {k: r.get(k) for k in keys}
(backup / 'ledgers.json').write_text(json.dumps(before, indent=2))
with ops.session() as api:
    strategies = [x for x in api('/manager/api/catalog/position-strategies')
                  if x['strategy_name'].startswith(ops.PREFIX)]
    assert len(strategies) == 59
    inv = next(x for x in strategies if x['strategy_name'] == ops.INVENTORY)
    assert all(not v['qty'] for row in strategies for v in row['targets'].values())
    targets = {k: dict(qty=0.0, signal=0) for k in inv['targets']}
    for coin, qty in cash.items():
        if coin != 'USDT' and qty:
            targets[coin] = dict(qty=float(qty), signal=0)
    result = api('/manager/api/catalog/position-strategies',
                 dict(strategy_name=ops.INVENTORY, targets=targets))
    assert not [x for x in result.get('publishes', []) if x.get('error')]
    account = api('/manager/api/catalog/accounts/' + exe.name)
    assert not account['pending_publishes']
    for venue in ['binance-margin', 'binance-futures']:
        actual = json.loads(r.get(f'{exe.name}:{venue}:batch_exec:{ops.INVENTORY}'))['targets']
        expected = {k: v for k, v in targets.items() if k.endswith('USDT') == (venue == 'binance-futures')}
        assert set(actual) == set(expected)
        assert all(math.isclose(actual[k]['qty'], v['qty'], rel_tol=1e-14, abs_tol=1e-12)
                   for k, v in expected.items())
updates = {}
for key in keys:
    ledger = json.loads(before[key])
    assert ledger['version'] == 1 and not ledger.get('inverse_notionals')
    ledger['positions'] = {name: {symbol: 0.0 for symbol in row}
                           for name, row in ledger['positions'].items()}
    ledger['positions'][ops.INVENTORY] = {coin + 'USDT':
        float(qty) if ':binance-margin:' in key else 0.0
        for coin, qty in cash.items() if coin != 'USDT' and qty}
    ledger['updated_at_us'] = ts
    updates[key] = json.dumps(ledger, separators=(',', ':'))
with r.pipeline(transaction=True) as p:
    p.watch(*keys)
    assert all(p.get(k) == v for k, v in before.items()), 'stopped ledger changed'
    p.multi()
    for key, value in updates.items():
        p.set(key, value)
    p.execute()
pid = subprocess.check_output(['systemctl', '--user', 'show', 'crypto-cta-manager-web.service',
                              '-p', 'MainPID', '--value'], text=True).strip()
manager_env = dict(x.decode().split('=', 1) for x in Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in x)
base = ['--config', str(manager / 'config/cta-manager.toml'), '--source', exe.name,
        '--snapshot-ts-us', str(ts), '--note',
        'RX01 post taker liquidation: all USDT perpetuals and margin debt zero; '
        'factual below-minimum cash dust belongs to inventory; 57 factor strategies flat; historical orders retained']
account_args, strategy_args = [], []
positions = []
for coin, qty in sorted(cash.items()):
    if coin == 'USDT' or not qty:
        continue
    value = f'{coin}:0:{qty}:{marks[coin]}'
    account_args += ['--position', value]
    strategy_args += ['--position', ops.INVENTORY + ':' + value]
    positions.append(dict(coin=coin, qty=str(qty), mark=str(marks[coin])))
result = {}
check = subprocess.run([str(manager / 'bin/nav_strategy_snapshot'), *base, *strategy_args, '--dry-run'],
                       env=manager_env, capture_output=True, text=True)
assert check.returncode == 0, 'strategy snapshot validation failed'
for binary, positions_args in [('nav_snapshot', account_args), ('nav_strategy_snapshot', strategy_args)]:
    completed = subprocess.run([str(manager / 'bin' / binary), *base, *positions_args],
                               env=manager_env, capture_output=True, text=True)
    if completed.returncode:
        (backup / (binary + '.error.log')).write_text(completed.stderr)
        raise RuntimeError(binary + ' failed; inspect host-private log')
    result[binary] = json.loads(completed.stdout)
assert c.snapshot()[0] == cash
receipt = dict(snapshot_ts_us=ts, observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    backup=str(backup), snapshots=result, remaining_cash=positions, gross_cash_dust_usdt=str(gross),
    perpetual_positions=0, debts=0, open_orders=0, factor_positions=0, historical_orders_changed=False,
    usdt_balance=str(cash['USDT']))
(stage / 'post_flat_snapshots.json').write_text(json.dumps(receipt, indent=2))
print(json.dumps({k: v for k, v in receipt.items() if k not in ['snapshots', 'remaining_cash']}))
