"""Plan or prepare the RX01 flat restart; no exchange mutations in this phase."""
import argparse
import datetime
import importlib.util
import json
import math
import os
import shutil
import subprocess
import sys
import time
import tomllib
from decimal import Decimal
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--execute', action='store_true')
args = parser.parse_args()
home = Path.home()
root = home / 'binance-cta-spread-rx01'
exe = home / 'binance_exec_trade06'
stage = root / 'run/reenable_20261019'
spec = importlib.util.spec_from_file_location('ops_helpers', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)

def save(name, value):
    path = stage / name
    temp = path.with_suffix(path.suffix + '.next')
    fd = os.open(temp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(value, f, indent=2, allow_nan=False)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    os.replace(temp, path)

def stopped():
    assert not [p for p in ops.live_pids(exe) if p['comm'] in {
        'trade_engine', 'exec-pre-trade', 'trade_signal'}], 'execution must stay stopped'

stopped()
config = json.loads((root / 'config/cta_spread.json').read_text())
assert config['enabled'] is False and config['max_pos_u'] == 10.0
status = json.loads((root / 'run/cta_spread_status.json').read_text())
warmup = json.loads((root / 'data/cta_spread/factor_history/warmup.json').read_text())
assert warmup['caught_up'] and time.time_ns() // 1_000_000 - warmup['updated_ms'] < 10000
assert not any(warmup[k] for k in ['decode_errors', 'evaluation_errors', 'publish_failed'])
assert status['journal_healthy'] and status['publication_error'] is None
positions = json.loads((root / 'run/cta_spread_position_flags.json').read_text())
assert sum(map(len, positions.values())) == 794
assert all(not any(p.values()) for row in positions.values() for p in row.values())
table = json.loads((root / 'config/cta_spread_symbol_factors.json').read_text())
factors = sorted({factor for row in table.values() for factor in row})
assert len(table) == 48 and len(factors) == 57
values = ops.env_for(exe)
signal_values = ops.env_for(root)
assert signal_values['CTA_MANAGER_API_URL'] == 'http://127.0.0.1:4191/manager/api'
assert not signal_values.get('CTA_MANAGER_API_URLS')
assert signal_values['CTA_FOLLOWER_SOURCE_ID'] == exe.name
assert signal_values['CTA_POSITION_STRATEGY_PREFIX'] == ops.PREFIX
token = signal_values['CTA_PUBLISH_TOKEN']
assert token
os.environ.update(values)
sys.path.insert(0, str(home / 'binance-cta-rx01/scripts'))
from neutralize_rapidx_binance import Client
local_ips = tomllib.loads((exe / 'trade_engine.toml').read_text())['local_ips']
assert local_ips == ['172.31.35.228']
client = Client(local_ips[0], execute=False)
client.empty_orders()
cash1, perp1, marks1 = client.snapshot()
cash, perp, marks = client.snapshot()
assert perp1 == perp, 'futures positions changed between observations'
assert all(abs(cash1.get(c, Decimal(0)) - cash.get(c, Decimal(0))) * marks.get(c, Decimal(0))
    < Decimal('0.1') for c in set(cash1) | set(cash)), 'cash positions changed materially'
client.empty_orders()
ts_us = time.time_ns() // 1000
rows = [dict(coin=c, cash_qty=str(cash.get(c, 0)), perpetual_qty=str(perp.get(c, 0)),
    mark=str(marks[c])) for c in sorted(set(cash) | set(perp))
    if c != 'USDT' and (cash.get(c, 0) or perp.get(c, 0))]
inventory_targets = {}
for row in rows:
    for symbol, qty in [(row['coin'], row['cash_qty']),
                        (row['coin'] + 'USDT', row['perpetual_qty'])]:
        if Decimal(qty):
            inventory_targets[symbol] = dict(qty=float(qty), signal=0)

import redis
r = redis.Redis(host=values.get('REDIS_HOST', '127.0.0.1'),
    port=int(values.get('REDIS_PORT', 6379)), db=int(values.get('REDIS_DB', 0)),
    username=values.get('REDIS_USERNAME'), password=values.get('REDIS_PASSWORD'),
    socket_connect_timeout=3, socket_timeout=3, decode_responses=True)
for venue in ['binance-margin', 'binance-futures']:
    rules = json.loads(r.get(f'{exe.name}:{venue}:market_rules'))
    assert time.time_ns() // 1000 - rules['fetched_at_us'] < 180_000_000
    for symbol in set(table) | {row['coin'] + 'USDT' for row in rows}:
        assert rules['symbols'].get(symbol, {}).get('status') == 'TRADING', (venue, symbol)

with ops.session() as api:
    account = api('/manager/api/catalog/accounts/' + exe.name)
    assert account['configuration'] == dict(mode='follow', virtual_id='virtual03', multiplier=1.0)
    virtual = next(v for v in api('/manager/api/catalog/virtual-accounts') if v['virtual_id'] == 'virtual03')
    assert [f['source_id'] for f in virtual['followers']] == [exe.name]
    assert any(x['username'] == 'lgq' for x in virtual['managers'])
    existing = {s['strategy_name']: s for s in api('/manager/api/catalog/position-strategies')
                if s['strategy_name'].startswith(ops.PREFIX)}
    wanted = {ops.PREFIX + f for f in factors}
    inventory = ops.PREFIX + 'inventory'
    retired = sorted(set(existing) - wanted - {inventory})
    assert retired == [ops.PREFIX + 'factor_052']
    targets = {}
    for factor in factors:
        name = ops.PREFIX + factor
        symbols = {key for symbol, fs in table.items() if factor in fs
                   for key in [symbol, symbol.removesuffix('USDT')]}
        symbols.update(existing.get(name, {}).get('targets', {}))
        targets[name] = {symbol: dict(qty=0.0, signal=0) for symbol in sorted(symbols)}
    for name in retired:
        targets[name] = {symbol: dict(qty=0.0, signal=0) for symbol in existing[name]['targets']}
    targets[inventory] = inventory_targets
    bindings = [dict(binding_name=name, position_strategy_name=name,
        order_strategy_name=ops.PREFIX + 'ctaker', shares=0.0 if name in retired else 1.0)
        for name in sorted(targets)]
    plan = dict(prepared_ts_us=ts_us, config=config, inventory_snapshot=rows,
                strategies=len(factors), combinations=794, new_strategies=len(wanted - set(existing)),
                retired=retired, virtual_id='virtual03', follower=exe.name,
                targets=targets, bindings=bindings, dry_run=not args.execute)
    save('plan.json', plan)
    if not args.execute:
        print(json.dumps({k: v for k, v in plan.items() if k not in ['targets', 'bindings', 'inventory_snapshot']}))
        sys.exit(0)
    stopped()
    assert not (stage / 'catalog_prepared.json').exists(), 'preparation already completed'
    backup = stage / ('backup_' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    backup.mkdir(mode=0o700)
    for path in [root / 'config/cta_spread.json', root / 'run/cta_spread_position_flags.json',
                 root / 'release.json', exe / 'release.json']:
        if path.exists():
            dst = backup / (('exec_' if path.parent == exe else '') + path.name)
            shutil.copy2(path, dst)
            os.chmod(dst, 0o600)
    with (backup / 'manager.pgdump').open('wb') as f:
        os.chmod(f.name, 0o600)
        p = subprocess.run(['pg_dump', '--format=custom', '--no-owner', '--no-acl'],
                            env=ops.db_env(), stdout=f, stderr=subprocess.PIPE)
        assert p.returncode == 0, 'database backup failed; no catalog change made'
    save('catalog_before.json', dict(account=account, virtual=virtual, positions=existing))
    ledgers_before = {}
    for venue in ['binance-margin', 'binance-futures']:
        key = f'{exe.name}:{venue}:batch_exec_state:position_allocations'
        ledgers_before[key] = r.get(key)
    save('ledgers_before.json', ledgers_before)
    for name, target in sorted(targets.items()):
        response = api('/manager/api/catalog/position-strategies',
                       dict(strategy_name=name, targets=target))
        assert not [p for p in response.get('publishes', []) if p.get('error')], name
        if name in wanted - set(existing):
            api('/manager/api/catalog/position-strategies/' + name + '/managers',
                {'user_ids': [3]}, 'PUT')
            api('/manager/api/catalog/position-strategies/' + name + '/publish-token',
                {'publish_token': token}, 'PUT')
    api('/manager/api/catalog/virtual-accounts/virtual03',
        dict(name=virtual['name'], bindings=bindings), 'PUT')
    for attempt in range(40):
        current = api('/manager/api/catalog/accounts/' + exe.name)
        if not current['pending_publishes']:
            break
        time.sleep(0.5)
    assert not current['pending_publishes'], 'follow delivery is still pending; execution remains stopped'
    for venue in ['binance-margin', 'binance-futures']:
        names = json.loads(r.get(f'{exe.name}:{venue}:batch_exec:strategy_names'))
        assert set(names) == set(targets), 'runtime strategy index mismatch'
        for name, target in targets.items():
            expected = {symbol: value for symbol, value in target.items()
                        if (symbol.endswith('USDT')) == (venue == 'binance-futures')}
            actual = json.loads(r.get(f'{exe.name}:{venue}:batch_exec:{name}'))['targets']
            assert actual.keys() == expected.keys(), (venue, name, 'runtime symbol mismatch')
            assert all(actual[s]['signal'] == value['signal'] and
                       math.isclose(actual[s]['qty'], value['qty'], rel_tol=1e-14, abs_tol=1e-12)
                       for s, value in expected.items()), (venue, name, 'runtime target mismatch')
    stopped()
    ledger_updates = {}
    for venue in ['binance-margin', 'binance-futures']:
        key = f'{exe.name}:{venue}:batch_exec_state:position_allocations'
        ledger = json.loads(ledgers_before[key])
        assert ledger['version'] == 1 and not ledger.get('inverse_notionals')
        ledger['positions'] = {name: {symbol: 0.0 for symbol in row}
                                for name, row in ledger['positions'].items()}
        for factor in factors:
            name = ops.PREFIX + factor
            for symbol, fs in table.items():
                if factor in fs:
                    ledger['positions'].setdefault(name, {})[symbol] = 0.0
        ledger['positions'][inventory] = {row['coin'] + 'USDT':
            float(row['cash_qty'] if venue == 'binance-margin' else row['perpetual_qty']) for row in rows}
        ledger['updated_at_us'] = ts_us
        ledger_updates[key] = json.dumps(ledger, separators=(',', ':'), allow_nan=False)
    with r.pipeline(transaction=True) as pipe:
        pipe.watch(*ledger_updates)
        assert all(pipe.get(k) == v for k, v in ledgers_before.items()), 'ledger changed during preparation'
        pipe.multi()
        for k, v in ledger_updates.items():
            pipe.set(k, v)
        pipe.execute()
    save('ledgers_after.json', {k: json.loads(v) for k, v in ledger_updates.items()})
    save('catalog_prepared.json', dict(backup=str(backup), prepared_ts_us=ts_us,
        strategies=57, active_bindings=58, retired_bindings=retired, targets_verified=True,
        execution_stopped=True, exchange_orders_submitted=0, signal_config_preserved=config))
    print(json.dumps(json.loads((stage / 'catalog_prepared.json').read_text())))
