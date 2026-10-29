#!/usr/bin/env python3
"""Read-only RapidX cash, debt and perpetual retirement inventory.

Run in the stopped account environment with its env.sh sourced. No requests that
change orders, loans, transfers or account configuration are made by this tool.
"""
import argparse
import json
import os
import time
from decimal import Decimal
from pathlib import Path

import neutralize_rapidx_binance as n
from binance_local_ip import resolve_local_address

ZERO = Decimal(0)


def build_plan(assets, positions, rules):
    result = []
    for item in assets:
        coin = item['coin']
        balance = n.dec(item['balance'])
        debt = n.dec(item['debt'])
        available = n.dec(item['available'])
        price = n.dec(item['indexPrice'])
        n.require(balance >= ZERO and debt >= ZERO and available >= ZERO,
                  'negative asset balance, debt or available')
        if coin == 'USDT' or (not balance and not debt):
            continue
        sym = f'BINANCE_MARGIN_{coin}_USDT'
        rule = rules.get(sym)
        row = {'market': 'margin', 'coin': coin, 'symbol': sym,
               'balance': n.fmt(balance), 'debt': n.fmt(debt),
               'available': n.fmt(available), 'net': n.fmt(balance - debt),
               'mark': n.fmt(price), 'reason': 'ready'}
        if not rule or rule.get('state') != 'live':
            row['reason'] = 'missing_or_inactive_market'
        elif price <= ZERO:
            row['reason'] = 'missing_mark'
        else:
            step = n.dec(rule['lotSize'])
            minimum = n.dec(rule['minSize'])
            min_notional = n.dec(rule['minNotional'])
            n.require(step > ZERO and minimum > ZERO and min_notional >= ZERO, 'invalid rule')
            row.update(lot_size=n.fmt(step), min_size=n.fmt(minimum),
                       min_notional=n.fmt(min_notional))
            net = balance - debt
            row['repay_available'] = n.fmt(min(available, debt))
            row['action'] = 'repay_then_reassess' if min(available, debt) else ('sell' if net > ZERO else 'buy' if net < ZERO else 'reassess')
            qty = n.quantize(abs(net), step)
            row['estimated_quantity'] = n.fmt(qty)
            if net and (qty < minimum or qty * price < min_notional):
                row['reason'] = 'below_minimum_order'
        result.append(row)
    for coin, qty in positions.items():
        qty = n.dec(qty)
        if not qty:
            continue
        sym = f'BINANCE_PERP_{coin}_USDT'
        rule = rules.get(sym)
        row = {'market': 'perpetual', 'coin': coin, 'symbol': sym,
               'position': n.fmt(qty), 'action': 'sell' if qty > ZERO else 'buy',
               'reason': 'ready'}
        if not rule or rule.get('state') != 'live':
            row['reason'] = 'missing_or_inactive_market'
        else:
            step, minimum = n.dec(rule['lotSize']), n.dec(rule['minSize'])
            n.require(step > ZERO and minimum > ZERO, 'invalid rule')
            if n.quantize(abs(qty), step) != abs(qty) or abs(qty) < minimum:
                row['reason'] = 'invalid_or_below_minimum_quantity'
        result.append(row)
    return sorted(result, key=lambda x: (x['market'], x['coin']))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-dir', required=True)
    parser.add_argument('--output', required=True, help='new private JSON file')
    args = parser.parse_args()
    env = Path(args.env_dir).resolve()
    n.assert_stopped(env)
    ip, _ = resolve_local_address(env_dir=str(env))
    n.require(bool(ip), 'explicit trading IP required')
    client = n.Client(ip, execute=False)
    orders = client.pages('/api/v1/trading/orders', {'exchange': 'BINANCE'})
    algos = client.pages('/api/v1/algo/openOrders', {'exchange': 'BINANCE'})
    for row in orders + algos:
        client.identity(row)
    assets = client.pages('/api/v1/trading/portfolio/assets', {'exchangeType': 'BINANCE'})
    for row in assets:
        client.identity(row)
    cash, positions, marks = client.snapshot()
    rules = client.request('GET', '/api/v1/trading/sym/info')
    plan = build_plan(assets, positions, rules)
    result = {'created_at_ms': time.time_ns() // 1_000_000,
              'source': env.name, 'local_ip': ip, 'open_orders': len(orders),
              'algo_orders': len(algos), 'rows': plan}
    out = Path(args.output)
    fd = os.open(out, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(result, f, indent=2, allow_nan=False)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    print(json.dumps({'source': env.name, 'cash_markets': sum(x['market'] == 'margin' for x in plan),
                      'perpetual_markets': sum(x['market'] == 'perpetual' for x in plan),
                      'blocked': sum(x['reason'] != 'ready' for x in plan),
                      'open_orders': len(orders), 'algo_orders': len(algos), 'output': str(out)}))


if __name__ == '__main__':
    main()
