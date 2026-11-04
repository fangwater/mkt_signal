#!/usr/bin/env python3
"""Read-only Gate ALPINE ADL attribution evidence; run remotely over stdin."""
import hashlib
import hmac
import http.client
import json
import os
import sys
import time
import tomllib
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.dont_write_bytecode = True
ROOT = Path.home() / 'gate_fr_arb01'
assert ROOT.is_dir()
engines = []
for p in Path('/proc').iterdir():
    if not p.name.isdigit():
        continue
    try:
        if os.readlink(p / 'cwd') == str(ROOT) and (p / 'comm').read_text().strip() == 'trade_engine':
            engines.append(p)
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        pass
assert len(engines) == 1
env = dict(x.split('=', 1) for x in (engines[0] / 'environ').read_bytes().decode().split('\0') if '=' in x)
key, secret = env['GATE_API_KEY'], env['GATE_API_SECRET']
config = next(p for p in [ROOT / 'trade_engine.toml', ROOT / 'trade engine.toml'] if p.is_file())
source = tomllib.loads(config.read_text())['local_ips'][0]


def get(path, params=None, private=True):
    assert path in {
        '/futures/usdt/auto_deleverages', '/futures/usdt/my_trades',
        '/futures/usdt/my_trades_timerange', '/futures/usdt/account_book',
        '/futures/usdt/position_close', '/futures/usdt/positions/ALPINE_USDT',
        '/futures/usdt/contracts/ALPINE_USDT', '/futures/usdt/tickers',
        '/unified/accounts', '/spot/my_trades', '/spot/tickers',
        '/futures/usdt/orders', '/futures/usdt/candlesticks',
        '/spot/candlesticks', '/futures/usdt/funding_rate',
        '/spot/trades', '/futures/usdt/trades',
    }
    query = urllib.parse.urlencode(sorted((params or {}).items()))
    full = '/api/v4' + path
    headers = {'Accept': 'application/json', 'X-Gate-Size-Decimal': '1'}
    if private:
        ts = str(int(time.time()))
        payload = '\n'.join(['GET', full, query, hashlib.sha512(b'').hexdigest(), ts])
        headers.update(KEY=key, Timestamp=ts, SIGN=hmac.new(secret.encode(), payload.encode(), hashlib.sha512).hexdigest())
    conn = http.client.HTTPSConnection('api.gateio.ws', timeout=20, source_address=(source, 0))
    try:
        conn.request('GET', full + ('?' + query if query else ''), headers=headers)
        response = conn.getresponse()
        data = json.loads(response.read())
        result = {'path': path, 'params': params, 'status': response.status, 'data': data,
                  'utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
        print(json.dumps(result), flush=True)
        return result
    finally:
        conn.close()


if __name__ == '__main__':
    jobs = [
        ('/futures/usdt/auto_deleverages', {'contract': 'ALPINE_USDT', 'limit': 100}),
        ('/futures/usdt/my_trades', {'contract': 'ALPINE_USDT', 'limit': 1000}),
        ('/futures/usdt/position_close', {'contract': 'ALPINE_USDT', 'limit': 100}),
        ('/futures/usdt/positions/ALPINE_USDT', {}),
        ('/unified/accounts', {'currency': 'ALPINE'}),
        ('/spot/my_trades', {'currency_pair': 'ALPINE_USDT', 'limit': 1000, 'account': 'unified'}),
        ('/futures/usdt/tickers', {'contract': 'ALPINE_USDT'}, False),
        ('/spot/tickers', {'currency_pair': 'ALPINE_USDT'}, False),
    ]
    if '--followup' in sys.argv:
        jobs = [
            ('/futures/usdt/account_book', {'contract': 'ALPINE_USDT', 'from': 1793355399, 'to': int(time.time()), 'limit': 1000}),
            ('/futures/usdt/my_trades_timerange', {'contract': 'ALPINE_USDT', 'from': 1793355000, 'to': int(time.time()), 'limit': 1000}),
            ('/futures/usdt/funding_rate', {'contract': 'ALPINE_USDT', 'from': 1793355399, 'to': int(time.time()), 'limit': 1000}, False),
            ('/futures/usdt/candlesticks', {'contract': 'ALPINE_USDT', 'from': 1793355300, 'to': 1793355480, 'interval': '10s'}, False),
            ('/spot/candlesticks', {'currency_pair': 'ALPINE_USDT', 'from': 1793355300, 'to': 1793355480, 'interval': '10s'}, False),
        ]
    if '--valuation' in sys.argv:
        jobs = [
            ('/spot/candlesticks', {'currency_pair': 'ALPINE_USDT', 'from': 1793355120, 'to': 1793355720, 'interval': '1m'}, False),
            ('/futures/usdt/candlesticks', {'contract': 'ALPINE_USDT', 'from': 1793355120, 'to': 1793355720, 'interval': '1m'}, False),
            ('/futures/usdt/candlesticks', {'contract': 'mark_ALPINE_USDT', 'from': 1793355120, 'to': 1793355720, 'interval': '1m'}, False),
            ('/futures/usdt/positions/ALPINE_USDT', {}),
            ('/unified/accounts', {'currency': 'ALPINE'}),
            ('/futures/usdt/tickers', {'contract': 'ALPINE_USDT'}, False),
            ('/spot/tickers', {'currency_pair': 'ALPINE_USDT'}, False),
        ]
    if '--event-prices' in sys.argv:
        jobs = [
            ('/spot/trades', {'currency_pair': 'ALPINE_USDT', 'from': 1793355370, 'to': 1793355410, 'limit': 1000}, False),
            ('/futures/usdt/trades', {'contract': 'ALPINE_USDT', 'from': 1793355370, 'to': 1793355410, 'limit': 1000}, False),
        ]
    with ThreadPoolExecutor(max_workers=3) as pool:
        for future in [pool.submit(get, *job) for job in jobs]:
            future.result()
