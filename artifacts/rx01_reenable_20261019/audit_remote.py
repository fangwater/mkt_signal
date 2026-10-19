"""Observe RX01 before resuming; credentials stay exclusively on the host."""
import datetime
import hashlib
import importlib.util
import json
import os
import sys
import time
import tomllib
from pathlib import Path

home = Path.home()
root = home / 'binance-cta-spread-rx01'
exe = home / 'binance_exec_trade06'
stage = root / 'run/reenable_20261019'
spec = importlib.util.spec_from_file_location('ops_helpers', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)

result = dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
result['config'] = json.loads((root / 'config/cta_spread.json').read_text())
result['status'] = json.loads((root / 'run/cta_spread_status.json').read_text())
result['positions'] = json.loads((root / 'run/cta_spread_position_flags.json').read_text())
result['warmup'] = json.loads((root / 'data/cta_spread/factor_history/warmup.json').read_text())
result['table'] = json.loads((root / 'config/cta_spread_symbol_factors.json').read_text())
result['processes'] = []
for proc in Path('/proc').glob('[0-9]*'):
    try:
        cwd = os.readlink(proc / 'cwd')
        if cwd.startswith(str(home) + '/'):
            fields = (proc / 'stat').read_text().split(') ', 1)[1].split()
            result['processes'].append(dict(pid=int(proc.name), cwd=cwd,
                comm=(proc / 'comm').read_text().strip(), start_ticks=fields[19],
                affinity=sorted(os.sched_getaffinity(int(proc.name)))))
    except OSError:
        pass
paths = [root / 'env.sh', exe / 'env.sh', exe / 'trade_engine.toml',
         exe / 'config/trade_engine.toml', root / 'cta_spread_signal',
         root / 'cta_spread_factor_model_1m_pub']
for p in home.glob('binance-cta-*'):
    if p != root:
        paths.extend([p / 'env.sh', p / 'config/cta_perp.json', p / 'config/cta_spread.json'])
result['file_hashes'] = {str(p): hashlib.sha256(p.read_bytes()).hexdigest()
                         for p in paths if p.is_file()}
with ops.session() as api:
    result['account'] = api('/manager/api/catalog/accounts/binance_exec_trade06')
    result['virtuals'] = api('/manager/api/catalog/virtual-accounts')
    strategies = api('/manager/api/catalog/position-strategies')
    result['position_strategies'] = [{k: v for k, v in s.items()
        if 'token' not in k.lower()} for s in strategies
        if s.get('strategy_name', s.get('position_strategy_name', '')).startswith(ops.PREFIX)]
    result['order_strategies'] = api('/manager/api/catalog/order-strategies')
values = ops.env_for(exe)
os.environ.update(values)
sys.path.insert(0, str(home / 'binance-cta-rx01/scripts'))
from neutralize_rapidx_binance import Client
ip = tomllib.loads((exe / 'trade_engine.toml').read_text())['local_ips']
assert ip == ['172.31.35.228']
client = Client(ip[0], execute=False)
orders = client.pages('/api/v1/trading/orders', {'exchange': 'BINANCE'})
algos = client.pages('/api/v1/algo/openOrders', {'exchange': 'BINANCE'})
for row in orders + algos:
    client.identity(row)
cash, perp, marks = client.snapshot()
result['exchange'] = dict(snapshot_ts_us=time.time_ns() // 1000,
    open_orders=len(orders), algo_orders=len(algos),
    rows=[dict(coin=c, cash_qty=str(cash.get(c, 0)), perpetual_qty=str(perp.get(c, 0)),
        mark=str(marks.get(c, 0)), net_usdt=str((cash.get(c, 0) + perp.get(c, 0)) * marks.get(c, 0)))
        for c in sorted(set(cash) | set(perp)) if c != 'USDT' and (cash.get(c, 0) or perp.get(c, 0))])
import redis
r = redis.Redis(host=values.get('REDIS_HOST', '127.0.0.1'),
    port=int(values.get('REDIS_PORT', 6379)), db=int(values.get('REDIS_DB', 0)),
    username=values.get('REDIS_USERNAME'), password=values.get('REDIS_PASSWORD'),
    socket_connect_timeout=3, socket_timeout=3, decode_responses=True)
result['redis'] = {}
for key in r.scan_iter(match='binance_exec_trade06:*', count=100):
    kind = r.type(key)
    if kind == 'string':
        raw = r.get(key)
        try:
            value = json.loads(raw)
        except (ValueError, TypeError):
            value = raw
        result['redis'][key] = value
    elif kind == 'hash' and 'risk' in key:
        result['redis'][key] = r.hgetall(key)
print(json.dumps(result, indent=2))
