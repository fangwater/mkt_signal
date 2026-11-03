"""Read-only OKX USDT swap unit verification on the JP Manager host.

Uses Manager's existing public-REST source IP and its Redis rule snapshot.
Queries public instrument specifications only; no order or account API calls.
"""
from decimal import Decimal
import hashlib
import http.client
import json
from pathlib import Path
import time
import tomllib

import redis

root = Path.home() / 'crypto_cta_manager'
config = tomllib.loads((root / 'config/cta-manager.toml').read_text())
settings = config.get('redis', {})
client = redis.Redis(decode_responses=True, **{
    key: settings[key] for key in ('host', 'port', 'db', 'username', 'password') if key in settings
})
cache_key = 'okex_exec_trade01:okex-futures:market_rules'
snapshot = json.loads(client.get(cache_key))
assert snapshot['venue'] == 'okex-futures' and snapshot['execution_backend'] == 'native'
assert 0 <= time.time_ns() // 1000 - snapshot['fetched_at_us'] < 180_000_000
source_ip = config['kline']['local_ip']
connection = http.client.HTTPSConnection('www.okx.com', timeout=15, source_address=(source_ip, 0))
try:
    connection.request('GET', '/api/v5/public/instruments?instType=SWAP', headers={'Accept': 'application/json'})
    response = connection.getresponse()
    assert response.status == 200, f'public instruments HTTP {response.status}'
    official = json.loads(response.read())
finally:
    connection.close()
assert official['code'] == '0'
rows = {row['instId'].replace('-SWAP', '').replace('-', ''): row for row in official['data']
        if row.get('ctType') == 'linear' and row.get('settleCcy') == 'USDT'}
checks = []
for symbol in ('BTCUSDT', 'ETHUSDT', 'TRBUSDT', 'DOTUSDT', 'ADAUSDT', 'NEARUSDT',
               'ATOMUSDT', 'DOGEUSDT', 'ETCUSDT', 'CRVUSDT'):
    rule, instrument = snapshot['symbols'][symbol], rows[symbol]
    multiplier = Decimal(instrument['ctVal']) * Decimal(instrument['ctMult'] or '1')
    assert multiplier == Decimal(rule['contract_multiplier']) > 0
    assert Decimal(instrument['lotSz']) == Decimal(rule['qty_step']) > 0
    assert Decimal(instrument['minSz']) == Decimal(rule['min_qty']) > 0
    assert instrument['ctValCcy'] == rule['base_asset']
    checks.append({'symbol': symbol, 'base_per_contract': str(multiplier),
                   'contract_step': instrument['lotSz'], 'minimum_contract_qty': instrument['minSz'],
                   'base_step': str(multiplier * Decimal(instrument['lotSz'])), 'matches_exchange': True})
env = Path.home() / 'okex_exec_trade01'
hashes = {name: hashlib.sha256((env / name).read_bytes()).hexdigest()
          for name in ('exec-pre-trade', 'trade_engine', 'account_monitor')}
print(json.dumps({'checked_at_ms': time.time_ns() // 1_000_000, 'source': env.name,
                  'cache_key': cache_key, 'cache_fetched_at_us': snapshot['fetched_at_us'],
                  'checks': checks, 'installed_sha256': hashes, 'orders_submitted': 0}, indent=2))
