#!/usr/bin/env python3
"""Run on JP with python3; authenticated GET only, fixed trading source IPs."""
import base64
import datetime as dt
from decimal import Decimal
import hashlib
import hmac
import http.client
import json
from pathlib import Path
import subprocess
import time
import tomllib
from urllib.parse import urlencode


def credentials(root):
    result = subprocess.run(
        ['bash', '-c', 'set -a; source "$1" >/dev/null 2>&1; env -0', 'bash', str(root / 'env.sh')],
        capture_output=True, check=True,
    )
    return dict(x.decode().split('=', 1) for x in result.stdout.split(b'\0') if b'=' in x)


def source_ip(root):
    ip = tomllib.loads((root / 'trade_engine.toml').read_text())['local_ips'][0]
    if ip in ('0.0.0.0', '::', ''):
        raise RuntimeError('explicit trading IP required')
    return ip


def get(host, path, headers, ip):
    conn = http.client.HTTPSConnection(host, timeout=20, source_address=(ip, 0))
    try:
        conn.request('GET', path, headers={'User-Agent': 'curl/8.5.0', 'Accept': 'application/json', **headers})
        response = conn.getresponse()
        data = json.loads(response.read())
        if response.status != 200:
            return {'error': True, 'http_status': response.status, 'code': data.get('code'), 'message': data.get('msg')}
        return data
    except Exception as exc:
        return {'error': True, 'exception_type': type(exc).__name__}
    finally:
        conn.close()


def nonzero(value):
    return Decimal(str(value or '0')) != 0


def okx():
    root = Path.home() / 'okex_mm_alpha'
    env, ip = credentials(root), source_ip(root)

    def request(path):
        timestamp = dt.datetime.now(dt.timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        signature = base64.b64encode(hmac.new(env['OKX_API_SECRET'].encode(), (timestamp + 'GET' + path).encode(), hashlib.sha256).digest()).decode()
        result = get('www.okx.com', path, {
            'OK-ACCESS-KEY': env['OKX_API_KEY'], 'OK-ACCESS-SIGN': signature,
            'OK-ACCESS-TIMESTAMP': timestamp, 'OK-ACCESS-PASSPHRASE': env['OKX_PASSPHRASE'],
        }, ip)
        if result.get('error') or result.get('code') != '0':
            return {'error': True, 'code': result.get('code'), 'message': result.get('msg', result.get('message'))}
        return result['data']

    positions = request('/api/v5/account/positions')
    orders = request('/api/v5/trade/orders-pending')
    balance = request('/api/v5/account/balance')
    config = request('/api/v5/account/config')
    fees = request('/api/v5/account/trade-fee?instType=SWAP&instFamily=BTC-USDT')
    algos = {kind: request('/api/v5/trade/orders-algo-pending?ordType=' + kind) for kind in ['conditional', 'oco', 'trigger', 'move_order_stop']}
    result = {'env': root.name, 'source_ip': ip, 'positions': positions, 'open_orders': orders, 'algo_orders': algos}
    result['config'] = [{k: r.get(k) for k in ['acctLv', 'posMode', 'autoLoan', 'perm']} for r in config] if isinstance(config, list) else config
    result['swap_fees'] = [{k: r.get(k) for k in ['instType', 'makerU', 'takerU', 'maker', 'taker', 'level']} for r in fees] if isinstance(fees, list) else fees
    if isinstance(positions, list):
        result['nonzero_positions'] = [{k: r.get(k) for k in ['instId', 'instType', 'pos', 'posSide', 'mgnMode', 'notionalUsd']} for r in positions if nonzero(r.get('pos'))]
    if isinstance(balance, list):
        result['balances'] = [{k: r.get(k) for k in ['ccy', 'cashBal', 'eq', 'liab', 'interest', 'upl']} for account in balance for r in account.get('details', []) if any(nonzero(r.get(k)) for k in ['cashBal', 'eq', 'liab', 'interest'])]
    else:
        result['balances'] = balance
    result['complete'] = all(isinstance(v, list) for v in [positions, orders, balance, config, *algos.values()])
    result['flat'] = result['complete'] and not result['nonzero_positions']
    result['orders_clear'] = result['complete'] and not orders and not any(algos.values())
    return result


def binance():
    root = Path.home() / 'binance_exec_trade08'
    env, ip = credentials(root), source_ip(root)
    mode = env.get('BINANCE_ACCOUNT_MODE', '').upper()
    if mode != 'STANDARD':
        raise RuntimeError('expected native STANDARD account; do not guess API scope')

    def request(host, path):
        query = urlencode({'timestamp': int(time.time() * 1000), 'recvWindow': 10000})
        signature = hmac.new(env['BINANCE_API_SECRET'].encode(), query.encode(), hashlib.sha256).hexdigest()
        return get(host, path + '?' + query + '&signature=' + signature, {'X-MBX-APIKEY': env['BINANCE_API_KEY']}, ip)

    positions = request('fapi.binance.com', '/fapi/v2/positionRisk')
    orders = request('fapi.binance.com', '/fapi/v1/openOrders')
    algos = request('fapi.binance.com', '/fapi/v1/openAlgoOrders')
    account = request('fapi.binance.com', '/fapi/v3/account')
    spot = request('api.binance.com', '/api/v3/account')
    spot_orders = request('api.binance.com', '/api/v3/openOrders')
    exchange_info = get('fapi.binance.com', '/fapi/v1/exchangeInfo', {}, ip)
    result = {'env': root.name, 'label': 'lgq_ltp_81857', 'source_ip': ip, 'account_mode': mode, 'open_orders': orders, 'algo_orders': algos, 'spot_open_orders': spot_orders}
    if isinstance(positions, list):
        result['nonzero_positions'] = [{k: r.get(k) for k in ['symbol', 'positionAmt', 'positionSide', 'entryPrice', 'markPrice', 'notional', 'unRealizedProfit', 'marginType', 'leverage']} for r in positions if nonzero(r.get('positionAmt'))]
    else:
        result['positions_error'] = positions
    result['futures_account'] = {k: account.get(k) for k in ['canTrade', 'multiAssetsMargin', 'totalWalletBalance', 'totalMarginBalance', 'totalPositionInitialMargin', 'totalOpenOrderInitialMargin', 'availableBalance']}
    result['spot_balances'] = [r for r in spot.get('balances', []) if nonzero(r.get('free')) or nonzero(r.get('locked'))] if 'balances' in spot else spot
    result['iota_rules'] = [{k: r.get(k) for k in ['symbol', 'status', 'filters']} for r in exchange_info.get('symbols', []) if r['symbol'] == 'IOTAUSDT']
    result['complete'] = all(isinstance(v, list) for v in [positions, orders, algos, spot_orders]) and 'totalWalletBalance' in account and 'balances' in spot
    result['flat'] = result['complete'] and not result['nonzero_positions']
    result['orders_clear'] = result['complete'] and not orders and not algos and not spot_orders
    return result


print(json.dumps({'checked_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'observation_only': True, 'okx': okx(), 'binance': binance()}, indent=2))
