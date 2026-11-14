"""One-shot documented RapidX close for remaining RX01 Binance perpetual dust."""
import importlib.util
import http.client
import json
import os
import sys
import time
from pathlib import Path

h = Path.home()
stage = h / 'binance-cta-spread-rx01/run/reenable_20261019'
exe = h / 'binance_exec_trade06'
spec = importlib.util.spec_from_file_location('ops', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
assert not any(p['comm'] == 'exec-pre-trade' for p in ops.live_pids(exe))
os.environ.update(ops.env_for(exe))
sys.path.insert(0, str(h / 'binance-cta-rx01/scripts'))
from neutralize_rapidx_binance import Client, signed_headers
c = Client('172.31.35.228')
c.empty_orders()
cash, perp, marks = c.snapshot()
remaining = {k: v for k, v in perp.items() if v}
assert set(remaining) <= {'XRP', 'SOL', 'LTC'}
assert all(abs(v) * marks[k] < 4 for k, v in remaining.items())
assert remaining, 'already flat; no mutation needed'
params = dict(symList=','.join('BINANCE_PERP_' + k + '_USDT' for k in sorted(remaining)),
              positionSide='NONE', closeAllPos='false', exchangeType='BINANCE')
intent = stage / 'perp_dust_close_intent.json'
fd = os.open(intent, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'w') as f:
    json.dump(dict(ts_us=time.time_ns() // 1000, params=params,
                   positions={k: str(v) for k, v in remaining.items()}), f)
    f.flush()
    os.fsync(f.fileno())
conn = http.client.HTTPSConnection('api.liquiditytech.com', timeout=10,
                                  source_address=('172.31.35.228', 0))
try:
    conn.request('DELETE', '/api/v1/trading/positions', body=json.dumps(params).encode(),
                 headers=signed_headers(c.key, c.secret, params))
    response = conn.getresponse()
    result = dict(http_status=response.status, response=json.loads(response.read()))
finally:
    conn.close()
(stage / 'perp_dust_close_result.json').write_text(json.dumps(result, indent=2))
print(json.dumps(result))
assert result['http_status'] in {200, 202}
assert result['response'].get('code') == 200000
