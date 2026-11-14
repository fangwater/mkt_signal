#!/usr/bin/env python3
"""Close a stopped RapidX Binance portfolio, journaling each market order.

Default is read-only. Market orders require --execute; every intent is fsynced
before submission. An unresolved order remains pending and blocks new orders.
This tool does not transfer funds or alter Manager or Exec databases.
"""
import argparse
import fcntl
import json
import os
import time
import uuid
from decimal import Decimal, ROUND_CEILING
from pathlib import Path

import neutralize_rapidx_binance as n
from binance_local_ip import resolve_local_address

D = Decimal


def snapshot_assets(client):
    rows = client.pages('/api/v1/trading/portfolio/assets', {'exchangeType': 'BINANCE'})
    result = {}
    for row in rows:
        client.identity(row)
        coin = row['coin']
        n.require(coin not in result, 'duplicate asset')
        for key in ('balance', 'debt', 'available', 'indexPrice'):
            n.dec(row[key])
        result[coin] = row
    return result


def market_request(coin, market, qty, mark, rule, available, clear_dust=False):
    """Return an actionable MARKET request, or None for below-minimum exposure."""
    qty, mark, available = map(n.dec, (qty, mark, available))
    n.require(qty != 0 and mark > 0, 'zero exposure or missing mark')
    n.require(rule and rule.get('state') == 'live', 'market unavailable')
    lot, minimum, notional = (n.dec(rule[k]) for k in ('lotSize', 'minSize', 'minNotional'))
    n.require(lot > 0 and minimum > 0 and notional >= 0, 'invalid market rule')
    req = {'sym': f'BINANCE_{market}_{coin}_USDT', 'orderType': 'MARKET',
           'side': 'SELL' if qty > 0 else 'BUY',
           'clientOrderId': 'rx01ret' + uuid.uuid4().hex[:24]}
    if market == 'PERP':
        n.require(n.quantize(abs(qty), lot) == abs(qty), 'invalid perpetual lot')
        if abs(qty) < minimum:
            return None
        req.update(orderQty=n.fmt(abs(qty)), reduceOnly='TRUE', positionSide='NONE')
    elif market == 'MARGIN' and qty > 0:
        amount = n.quantize(min(qty, available), lot)
        if amount < minimum or amount * mark < notional * D('1.01'):
            if not clear_dust or amount == 0:
                return None
            req['side'] = 'BUY'
            req['quoteOrderQty'] = n.fmt((notional * D('2.1')).quantize(D('0.01'), rounding=ROUND_CEILING))
        else:
            req['orderQty'] = n.fmt(amount)
    elif market == 'MARGIN':
        amount = n.quantize(abs(qty), lot)
        if amount < minimum or amount * mark < notional * D('1.01'):
            if not clear_dust:
                return None
            quote = ((abs(qty) * mark + notional * D('2.1')) * D('1.01')).quantize(D('0.01'), rounding=ROUND_CEILING)
            req['quoteOrderQty'] = n.fmt(quote)
            return req
        # Quote-sized cash buys may leave a residual; each pass is re-snapshotted.
        quote = (abs(qty) * mark * D('1.01')).quantize(D('0.01'), rounding=ROUND_CEILING)
        req['quoteOrderQty'] = n.fmt(quote)
    else:
        raise n.Halt('invalid market')
    return req


def settle(client, journal, timeout=45):
    pending = journal.data['pending']
    n.require(pending is not None, 'no pending order')
    req = pending['request']
    deadline = time.monotonic() + timeout
    row = None
    while time.monotonic() < deadline:
        try:
            row = client.request('GET', n.ORDER, {'clientOrderId': req['clientOrderId']})
            client.identity(row)
            n.require(all(row[k] == req[k] for k in ('clientOrderId', 'sym', 'side')),
                      'order identity mismatch')
            n.require(row['orderState'] in n.TERMINAL | n.ACTIVE, 'unknown order state')
            if row['orderState'] in n.TERMINAL:
                break
        except (OSError, ValueError, KeyError, n.Halt):
            pass
        time.sleep(1)
    n.require(row is not None and row['orderState'] in n.TERMINAL,
              'unresolved order; retain journal and recover before doing anything else')
    filled = n.dec(row['executedQty'])
    n.require(filled >= 0, 'invalid fill')
    coin = req['sym'].split('_')[2]
    market = req['sym'].split('_')[1]
    before = n.dec(pending['before']['perp' if market == 'PERP' else 'cash'])
    delta = filled if req['side'] == 'BUY' else -filled
    if market == 'MARGIN' and row.get('feeCoin') == coin:
        delta -= n.dec(row['fee'])
    if market == 'MARGIN' and row.get('rebateCoin') == coin:
        delta += n.dec(row['rebate'])
    expected = before + delta
    deadline = time.monotonic() + timeout
    while True:
        cash, perp, _ = client.snapshot()
        actual = (perp if market == 'PERP' else cash).get(coin, n.ZERO)
        tolerance = max(D('0.00000001'), abs(before) * D('0.0000001'))
        if abs(actual - expected) <= tolerance:
            break
        n.require(time.monotonic() < deadline,
                  'terminal order position did not reconcile; retain pending journal')
        time.sleep(1)
    journal.data['orders'].append({'request': req, 'response': row,
                                   'reconciled_quantity': n.fmt(actual),
                                   'settled_ms': time.time_ns() // 1_000_000})
    journal.data['pending'] = None
    journal.save()
    n.require(row['orderState'] == 'FILLED' and n.dec(row['executedQty']) > 0,
              'order not fully filled; inspect journal and account before continuing')
    return row


def submit(client, journal, req, before):
    n.require(journal.data['pending'] is None, 'pending order requires recovery')
    journal.data['pending'] = {'request': req, 'before': before,
                               'created_ms': time.time_ns() // 1_000_000}
    journal.save()
    try:
        client.request('POST', n.ORDER, req)
    except (OSError, ValueError, KeyError, n.Halt):
        # Even a failed HTTP response can hide a successful exchange submission.
        pass
    return settle(client, journal)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-dir', required=True)
    parser.add_argument('--stage-dir', required=True)
    parser.add_argument('--coin', required=True, help='one asset per invocation')
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--clear-dust', action='store_true', help='buy enough to cover small debt or make a small balance sellable')
    parser.add_argument('--recover-only', action='store_true')
    args = parser.parse_args()
    env, stage = Path(args.env_dir).resolve(), Path(args.stage_dir).resolve()
    n.require(env.name == 'binance_exec_trade06' and args.coin.isalnum() and args.coin.isupper(),
              'unexpected environment or symbol')
    n.assert_stopped(env)
    ip, _ = resolve_local_address(env_dir=str(env))
    n.require(ip == '172.31.35.228', 'unexpected trading IP')
    client = n.Client(ip, execute=args.execute)
    stage.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (stage / 'flatten.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        journal = n.Journal(stage / 'flatten_journal.json', client.portfolio)
        if journal.data['pending']:
            n.require(args.execute and args.recover_only, 'pending order requires --execute --recover-only')
            settle(client, journal)
        if args.recover_only:
            return
        client.empty_orders()
        assets = snapshot_assets(client)
        cash, perp, marks = client.snapshot()
        rules = client.request('GET', '/api/v1/trading/sym/info')
        row = assets.get(args.coin)
        spot = cash.get(args.coin, n.ZERO)
        position = perp.get(args.coin, n.ZERO)
        mark = marks.get(args.coin, n.ZERO)
        actions = []
        for market, qty in [('MARGIN', spot), ('PERP', position)]:
            if not qty:
                continue
            available = n.dec(row['available']) if row else n.ZERO
            request = market_request(args.coin, market, qty, mark,
                                     rules.get(f'BINANCE_{market}_{args.coin}_USDT'), available, args.clear_dust)
            if request:
                actions.append((market, request))
        print(json.dumps({'coin': args.coin, 'cash': n.fmt(spot), 'perp': n.fmt(position),
                          'execute': args.execute, 'orders': [r for _, r in actions]}), flush=True)
        if not args.execute:
            return
        for market, req in actions:
            n.assert_stopped(env)
            client.empty_orders()
            cash_now, perp_now, _ = client.snapshot()
            current = cash_now.get(args.coin, n.ZERO) if market == 'MARGIN' else perp_now.get(args.coin, n.ZERO)
            expected = spot if market == 'MARGIN' else position
            n.require(current == expected, 'position changed since plan; stopped')
            result = submit(client, journal, req, {'cash': n.fmt(cash_now.get(args.coin, n.ZERO)),
                                                   'perp': n.fmt(perp_now.get(args.coin, n.ZERO))})
            print(json.dumps({'symbol': req['sym'], 'state': result['orderState'],
                              'filled': result['executedQty'], 'order_id': result['orderId']}), flush=True)
            time.sleep(2)
        client.empty_orders()
        cash_after, perp_after, marks_after = client.snapshot()
        print(json.dumps({'coin': args.coin, 'cash_after': n.fmt(cash_after.get(args.coin, n.ZERO)),
                          'perp_after': n.fmt(perp_after.get(args.coin, n.ZERO))}), flush=True)


if __name__ == '__main__':
    main()
