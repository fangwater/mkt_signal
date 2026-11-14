"""Close only RX03/RX04 residual USDT perpetuals, with a durable order journal.

Default is a signed read-only dry run. All trading submitters must be stopped.
Reuses the existing RapidX MARKET submit/reconcile functions; never trades cash.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import time
import uuid

import neutralize_rapidx_binance as n
from rapidx_retirement_execute import market_request, submit, settle
from clear_positions import SCOPES, save
from manager_ops import env_for
from binance_local_ip import resolve_local_address


def make_client(root, execute):
    values = env_for(root)
    keys = ('LTP_API_KEY', 'LTP_API_SECRET', 'LTP_PORTFOLIO_ID', 'LTP_REST_URL')
    before = {key: os.environ.get(key) for key in keys}
    try:
        for key in keys:
            if key in values:
                os.environ[key] = values[key]
            else:
                os.environ.pop(key, None)
        ip, _ = resolve_local_address(env_dir=str(root))
        n.require(bool(ip), 'explicit source IP required')
        return n.Client(ip, execute=execute)
    finally:
        for key, value in before.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit', required=True, type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--recover-only', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    n.require(args.audit.is_dir() and args.audit.name.startswith('rx03_rx04_taker_clear_'),
              'unexpected audit directory')
    allowed = json.loads((args.audit / 'clear_plan.json').read_text())
    n.require([r['source'] for r in allowed] == [s[1] for s in SCOPES], 'scope changed')
    with (args.audit / 'close_tails.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for signal, source, *_ in SCOPES:
            root = Path.home() / source
            n.assert_stopped(root)
            n.require(json.loads((Path.home() / signal / 'config/cta_perp.json').read_text())['enabled'] is False,
                      'signal enabled')
            client = make_client(root, args.execute)
            journal = n.Journal(args.audit / (source + '.tail_journal.json'), client.portfolio)
            if journal.data['pending']:
                n.require(args.execute and args.recover_only, 'pending order requires recovery only')
                settle(client, journal)
            if args.recover_only:
                continue
            client.empty_orders()
            cash, perp, marks = client.snapshot()
            rules = client.request('GET', '/api/v1/trading/sym/info')
            actions = []
            for coin, qty in sorted(perp.items()):
                if not qty:
                    continue
                request = market_request(coin, 'PERP', qty, marks[coin],
                                         rules.get(f'BINANCE_PERP_{coin}_USDT'), n.ZERO)
                n.require(request is not None, f'cannot close exact lot: {source} {coin}')
                request['clientOrderId'] = 'rx0304close' + uuid.uuid4().hex[:20]
                actions.append({'coin': coin, 'qty': n.fmt(qty), 'request': request})
            save(args.audit / (source + '.tail_plan.json'), actions)
            print(json.dumps({'source': source, 'execute': args.execute, 'orders': actions}), flush=True)
            if not args.execute:
                continue
            for action in actions:
                n.assert_stopped(root)
                client.empty_orders()
                cash_now, perp_now, _ = client.snapshot()
                coin = action['coin']
                n.require(perp_now.get(coin, n.ZERO) == n.dec(action['qty']), 'position changed since plan')
                result = submit(client, journal, action['request'],
                                {'cash': n.fmt(cash_now.get(coin, n.ZERO)), 'perp': action['qty']})
                n.require(n.dec(result['executedQty']) == abs(n.dec(action['qty'])), 'incomplete close')
                print(json.dumps({'source': source, 'symbol': action['request']['sym'],
                                  'filled': result['executedQty'], 'state': result['orderState']}), flush=True)
            client.empty_orders()
            _, after, _ = client.snapshot()
            n.require(not any(after.values()), 'nonzero perpetual position remains')
            print(json.dumps({'source': source, 'positions': 0, 'orders': 0,
                              'checked_at_ms': time.time_ns() // 1_000_000}), flush=True)


if __name__ == '__main__':
    main()
