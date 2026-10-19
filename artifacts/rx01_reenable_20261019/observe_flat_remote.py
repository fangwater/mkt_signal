"""Read-only factual RX01 liquidation progress, no credentials in output."""
import importlib.util
import json
import os
import sys
import time
import urllib.request
from pathlib import Path

h = Path.home()
root = h / 'binance-cta-spread-rx01'
exe = h / 'binance_exec_trade06'
stage = root / 'run/reenable_20261019'
spec = importlib.util.spec_from_file_location('ops', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
os.environ.update(ops.env_for(exe))
sys.path.insert(0, str(h / 'binance-cta-rx01/scripts'))
from neutralize_rapidx_binance import Client
c = Client('172.31.35.228')
cash, perp, marks = c.snapshot()
assets = c.pages('/api/v1/trading/portfolio/assets', {'exchangeType': 'BINANCE'})
orders = c.pages('/api/v1/trading/orders', {'exchange': 'BINANCE'})
algos = c.pages('/api/v1/algo/openOrders', {'exchange': 'BINANCE'})
for row in assets + orders + algos:
    c.identity(row)
u = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with u.open('http://127.0.0.1:10046/snapshot', timeout=10) as response:
    snap = json.load(response)
states = [x['entry'] for x in snap['entries'] if x['channel'] == 'exec_pre_trade_state']
value = dict(ts_us=time.time_ns() // 1000, cash={k: str(v) for k, v in cash.items() if v},
             perp={k: str(v) for k, v in perp.items() if v},
             gross_cash_usdt=str(sum(abs(v) * marks[k] for k, v in cash.items() if k != 'USDT')),
             assets=[{k: r.get(k) for k in ['coin', 'balance', 'available', 'frozen', 'debt', 'indexPrice']}
                     for r in assets if r['coin'] != 'USDT' and (float(r['balance']) or float(r['debt']))],
             open_orders=len(orders), algo_orders=len(algos), states=states)
(stage / 'flat_progress.json').write_text(json.dumps(value, indent=2))
print(json.dumps(value))
