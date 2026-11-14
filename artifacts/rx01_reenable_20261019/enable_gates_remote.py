"""Validate the post-flat anchor and fresh live readiness before enabling RX01."""
import importlib.util
import json
import time
import urllib.request
from pathlib import Path

h = Path.home()
root = h / 'binance-cta-spread-rx01'
exe = h / 'binance_exec_trade06'
stage = root / 'run/reenable_20261019'
sp = importlib.util.spec_from_file_location('ops', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(sp)
sp.loader.exec_module(ops)
u = urllib.request.build_opener(urllib.request.ProxyHandler({}))
with u.open('http://127.0.0.1:10046/snapshot', timeout=10) as response:
    v = json.load(response)
state = next(x['entry'] for x in v['entries'] if x['channel'] == 'exec_pre_trade_state')
assert state['position_ready'] and time.time() * 1000 - state['ts_ms'] < 3000
assert all(not x['pending_qty'] and not x['live_order_qty'] and not x['active_batches'] for x in state['rows'])
assert all(abs(x['delta_usdt']) < 0.001 for x in state['rows']), 'ledger and targets mismatch'
assert all(x['strategy_name'] == ops.INVENTORY for x in state['rows'] if abs(x['current_qty']) > 1e-12)
with ops.session() as api:
    account = api('/manager/api/catalog/accounts/' + exe.name)
    assert not account['pending_publishes']
    strategies = [x for x in api('/manager/api/catalog/position-strategies')
                  if x['strategy_name'].startswith(ops.PREFIX)]
    assert len(strategies) == 59
    assert all(not z['qty'] for x in strategies if x['strategy_name'] != ops.INVENTORY
               for z in x['targets'].values())
receipt = json.loads((stage / 'post_flat_snapshots.json').read_text())
ts = receipt['snapshot_ts_us']
counts = ops.sql(f"SELECT (SELECT count(*) FROM cta_position_snapshot_entries WHERE source_id='binance_exec_trade06' AND snapshot_ts_us={ts}), (SELECT count(*) FROM cta_strategy_position_snapshot_entries WHERE source_id='binance_exec_trade06' AND snapshot_ts_us={ts});")
assert counts == '20|20', counts
w = json.loads((root / 'data/cta_spread/factor_history/warmup.json').read_text())
assert w['caught_up'] and time.time() * 1000 - w['updated_ms'] < 5000
assert not any(w[k] for k in ['decode_errors', 'evaluation_errors', 'publish_failed'])
print(json.dumps(dict(gates_passed=True, exec_rows=len(state['rows']),
    exec_age_ms=time.time() * 1000 - state['ts_ms'], snapshot_counts=counts, snapshot_ts_us=ts)))
(stage / 'pre_enable_verified.json').write_text(json.dumps(dict(ts_us=time.time_ns() // 1000,
    exec_state=state, snapshot_counts=counts)))
