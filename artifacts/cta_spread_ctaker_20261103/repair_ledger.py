"""Repair the proven erroneous TRB allocation while both Execs are stopped."""
import base64
import datetime as dt
from decimal import Decimal
import hashlib
import hmac
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tomllib

H = Path.home()
ROOT = H / 'okex_exec_trade01'
AUDIT = H / 'crypto_cta_manager/maintenance-backups/cta_spread_ctaker_20261103'
KEY = 'okex_exec_trade01:okex-futures:batch_exec_state:position_allocations'
NAME = 'cta-spread01-okx-bn-1min-pos'
for site in (H / '.venvs/default/lib').glob('python*/site-packages'):
    sys.path.insert(0, str(site))
import redis


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def save(name, value):
    path = AUDIT / name
    path.write_text(json.dumps(value, indent=2) + '\n')
    path.chmod(0o600)


def stopped():
    for p in Path('/proc').glob('[0-9]*'):
        try:
            require(not ((p / 'cwd').resolve().name in ('okex_exec_trade01', 'binance_exec_spread01') and
                         (p / 'exe').resolve().name == 'exec-pre-trade'), 'pre-trade must be stopped')
        except OSError:
            pass


def snapshot():
    raw = subprocess.check_output(['bash', '-c', 'set -a; source "$1/env.sh" >/dev/null 2>&1; env -0', 'bash', str(ROOT)])
    env = dict(x.decode().split('=', 1) for x in raw.split(b'\0') if b'=' in x)
    ip = tomllib.loads((ROOT / 'trade_engine.toml').read_text())['local_ips'][0]
    def get(path, private=True):
        headers = {'Accept': 'application/json'}
        if private:
            timestamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            signature = base64.b64encode(hmac.new(env['OKX_API_SECRET'].encode(), (timestamp + 'GET' + path).encode(), hashlib.sha256).digest()).decode()
            headers.update({'OK-ACCESS-KEY': env['OKX_API_KEY'], 'OK-ACCESS-SIGN': signature,
                            'OK-ACCESS-TIMESTAMP': timestamp, 'OK-ACCESS-PASSPHRASE': env['OKX_PASSPHRASE']})
        conn = http.client.HTTPSConnection('www.okx.com', timeout=20, source_address=(ip, 0))
        try:
            conn.request('GET', path, headers=headers)
            response = conn.getresponse()
            result = json.loads(response.read())
            require(response.status == 200 and result.get('code') == '0', 'OKX snapshot failed')
            return result['data']
        finally:
            conn.close()
    positions = get('/api/v5/account/positions?instType=SWAP')
    live = {x['instId']: Decimal(x['pos']) for x in positions if Decimal(x.get('pos') or '0') != 0}
    require(live == {'TRB-USDT-SWAP': Decimal('-62'), 'DOT-USDT-SWAP': Decimal('107')}, 'exchange positions changed')
    require(not get('/api/v5/trade/orders-pending'), 'open orders exist')
    for kind in ('conditional', 'oco', 'trigger', 'move_order_stop'):
        require(not get('/api/v5/trade/orders-algo-pending?ordType=' + kind), 'open algo orders exist')
    instruments = {}
    for symbol in live:
        rows = get('/api/v5/public/instruments?instType=SWAP&instId=' + symbol, False)
        require(len(rows) == 1 and rows[0]['state'] == 'live', 'instrument unavailable')
        r = rows[0]
        instruments[symbol] = Decimal(r['ctVal']) * Decimal(r['ctMult'] or '1')
    require(instruments == {'TRB-USDT-SWAP': Decimal('0.1'), 'DOT-USDT-SWAP': Decimal('1')}, 'instrument multiplier changed')
    return {'positions_contracts': {k: str(v) for k, v in live.items()},
            'multipliers': {k: str(v) for k, v in instruments.items()},
            'base_quantities': {k: str(v * instruments[k]) for k, v in live.items()},
            'open_orders': 0, 'at_ms': time.time_ns() // 1000000}


def main():
    os.umask(0o077)
    require(sys.argv[1:] == ['--execute'], 'explicit --execute required')
    require(AUDIT.is_dir(), 'audit directory missing')
    stopped()
    snap = snapshot()
    client = redis.Redis.from_url(tomllib.loads((H / 'crypto_cta_manager/config/cta-manager.toml').read_text())['redis']['url'])
    old_bytes = client.get(KEY)
    require(old_bytes, 'position ledger missing')
    old = json.loads(old_bytes)
    positions = old['positions']
    require(set(positions) == {'SYSTEM_POSITION_CLOSE', NAME}, 'unexpected strategy ledger')
    require(positions['SYSTEM_POSITION_CLOSE'] == {'TRBUSDT': -55.75}, 'close ledger differs')
    require(positions[NAME]['TRBUSDT'] == -6.25 and positions[NAME]['DOTUSDT'] == 107.0, 'strategy ledger differs')
    require(not old.get('inverse_notionals'), 'unexpected inverse position ledger')
    updated = json.loads(old_bytes)
    updated['positions'][NAME]['TRBUSDT'] = float(Decimal(snap['base_quantities']['TRB-USDT-SWAP']))
    del updated['positions']['SYSTEM_POSITION_CLOSE']['TRBUSDT']
    if not updated['positions']['SYSTEM_POSITION_CLOSE']:
        del updated['positions']['SYSTEM_POSITION_CLOSE']
    updated['updated_at_us'] = time.time_ns() // 1000
    save('ledger-before.json', old)
    save('ledger-exchange-snapshot.json', snap)
    save('ledger-after-planned.json', updated)
    stopped()
    script = "if redis.call('get',KEYS[1]) ~= ARGV[1] then return 0 end redis.call('set',KEYS[1],ARGV[2]); return 1"
    require(client.eval(script, 1, KEY, old_bytes, json.dumps(updated, separators=(',', ':'))) == 1, 'ledger changed during repair')
    require(json.loads(client.get(KEY)) == updated, 'ledger readback mismatch')
    save('ledger-repaired.json', {'positions': updated['positions'], 'at_ms': time.time_ns() // 1000000})
    print('Removed erroneous TRB system allocation; strategy ledger now matches signed OKX positions.', flush=True)


if __name__ == '__main__':
    main()
