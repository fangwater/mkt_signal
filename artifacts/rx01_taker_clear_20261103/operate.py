"""Scoped RX01/15min taker flatten and removal of all old execution bindings.

Use beside existing Manager/RapidX operational helpers on JP. Mutations require
--execute; existing order identities are preserved for the normal Exec clear.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

from manager_ops import env_for, session, sql
from clear_positions import exchange_snapshot, save
from close_tails import make_client
from rapidx_retirement_execute import market_request, submit, settle
from remove_bindings import config_api
import neutralize_rapidx_binance as n

SOURCE = 'binance_exec_trade06'
SIGNAL = 'binance-cta-perp-rx01'
PREFIX = 'binance_cta_perp_rx01_'


def flat_snapshot(audit):
    snapshot = exchange_snapshot(SOURCE)
    save(audit / 'exchange_verified.json', snapshot)
    return snapshot


def require_flat(audit):
    row = flat_snapshot(audit)
    n.require(not row['nonzero_positions'] and row['open_orders'] == row['algo_orders'] == 0,
              'futures positions or orders remain')
    return row


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['clear', 'tails', 'remove', 'verify'])
    parser.add_argument('--audit', required=True, type=Path)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--recover-only', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    audit = args.audit
    n.require(audit.parent == Path.home() / 'crypto_cta_manager/maintenance-backups'
              and audit.name.startswith('rx01_taker_clear_'), 'unexpected audit path')
    plan = json.loads((audit / 'clear_plan.json').read_text())
    n.require(plan['source'] == SOURCE and plan['signal_env'] == SIGNAL and plan['virtual_id'] == 'virtual06',
              'unexpected execution scope')
    n.require(json.loads((Path.home() / SIGNAL / 'config/cta_perp.json').read_text())['enabled'] is False,
              'old signal enabled')
    if args.phase == 'clear':
        current = json.loads(sql("SELECT jsonb_agg(to_jsonb(b)) FROM cta_account_strategy_bindings b WHERE source_id='binance_exec_trade06'"))
        n.require(current == plan['bindings'], 'account bindings changed')
        print(json.dumps({'phase': 'clear', 'execute': args.execute, 'strategies': len(plan['payloads'])}), flush=True)
        if not args.execute:
            return
        values = env_for(Path.home() / SIGNAL)
        url = values.get('CTA_MANAGER_API_URLS', values.get('CTA_MANAGER_API_URL', ''))
        n.require(urllib.parse.urlsplit(url).hostname == '127.0.0.1' and ',' not in url, 'unexpected Manager URL')
        token = json.loads(values.get('CTA_PUBLISH_TOKENS', '{}')).get(url, values.get('CTA_PUBLISH_TOKEN'))
        n.require(bool(token), 'publish credential absent')
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        receipts = []
        for payload in plan['payloads']:
            n.require(payload['strategy_name'].startswith(PREFIX), 'strategy out of scope')
            req = urllib.request.Request(url.rstrip('/') + '/catalog/position-strategies',
                                         data=json.dumps(payload).encode(), method='POST',
                                         headers={'Content-Type': 'application/json', 'X-CTA-Publish-Token': token})
            with opener.open(req, timeout=30) as response:
                data = json.load(response)
            receipts.append({'strategy_name': payload['strategy_name'], 'response': data,
                             'at_ms': time.time_ns() // 1_000_000})
            save(audit / 'clear_receipts.json', receipts)
            n.require(data['strategy_name'] == payload['strategy_name'] and len(data['publishes']) == 1
                      and data['publishes'][0]['source_id'] == SOURCE
                      and not data['publishes'][0].get('error'), 'clear publication failed')
        print(json.dumps({'taker_clear_strategies_published': len(receipts)}), flush=True)
        return
    if args.phase == 'tails':
        before = flat_snapshot(audit)
        n.require(before['open_orders'] == before['algo_orders'] == 0, 'Exec clear has live orders; wait')
        if args.execute:
            for script in ['stop_exec_pre_trade.sh', 'stop_exec_trade_engine.sh']:
                result = subprocess.run([str(Path.home() / SOURCE / 'scripts' / script)],
                                        cwd=Path.home() / SOURCE, capture_output=True, timeout=60)
                n.require(result.returncode == 0, 'env-local stop failed; sensitive stderr suppressed')
                print('Stopped ' + SOURCE + '/' + script, flush=True)
        else:
            print(json.dumps({'execute': False, 'remaining_perpetual_positions': before['nonzero_positions']}), flush=True)
            return
        n.assert_stopped(Path.home() / SOURCE)
        with (audit / 'tail.lock').open('a') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            client = make_client(Path.home() / SOURCE, True)
            journal = n.Journal(audit / 'tail_journal.json', client.portfolio)
            if journal.data['pending']:
                n.require(args.recover_only, 'pending order requires --recover-only')
                settle(client, journal)
            if args.recover_only:
                return
            client.empty_orders()
            _, positions, marks = client.snapshot()
            rules = client.request('GET', '/api/v1/trading/sym/info')
            for coin, qty in sorted(positions.items()):
                if not qty:
                    continue
                n.assert_stopped(Path.home() / SOURCE)
                client.empty_orders()
                cash_now, positions_now, _ = client.snapshot()
                n.require(positions_now.get(coin, n.ZERO) == qty, 'position changed since plan')
                req = market_request(coin, 'PERP', qty, marks[coin], rules.get(f'BINANCE_PERP_{coin}_USDT'), n.ZERO)
                n.require(req is not None, 'residual below lot minimum requires inspection')
                req['clientOrderId'] = 'rx01close' + uuid.uuid4().hex[:22]
                result = submit(client, journal, req, {'cash': n.fmt(cash_now.get(coin, n.ZERO)), 'perp': n.fmt(qty)})
                n.require(n.dec(result['executedQty']) == abs(qty), 'incomplete residual close')
                print(json.dumps({'symbol': req['sym'], 'filled': result['executedQty'], 'state': result['orderState']}), flush=True)
        row = require_flat(audit)
        print(json.dumps(row), flush=True)
        return
    n.assert_stopped(Path.home() / SOURCE)
    require_flat(audit)
    if args.phase == 'remove':
        with session('lgq') as api:
            path = '/manager/api/catalog/accounts/' + SOURCE
            studio = api(path)
            n.require({b['binding_name'] for b in studio['bindings']} <= {b['binding_name'] for b in plan['bindings']},
                      'unexpected binding')
            virtuals = api('/manager/api/catalog/virtual-accounts')
            virtual = next((v for v in virtuals if v['virtual_id'] == 'virtual06'), None)
            if virtual:
                save(audit / 'virtual06.before.json', virtual)
                n.require(all(f['source_id'] == SOURCE for f in virtual['followers']), 'virtual account has other followers')
            print(json.dumps({'phase': 'remove', 'execute': args.execute, 'bindings': len(studio['bindings'])}), flush=True)
            if not args.execute:
                return
            receipts = []
            def record(action, result):
                receipts.append({'action': action, 'result': result})
                save(audit / 'remove_receipts.json', receipts)
            studio = api(path + '/configuration', {'mode': 'independent'}, 'PUT')
            record('remove_follow', studio)
            for binding in studio['bindings']:
                name = urllib.parse.quote(binding['binding_name'], safe='')
                record('shares_zero:' + name, api(path + '/bindings/' + name + '/shares', {'shares': 0}, 'PUT'))
                record('delete_binding:' + name, api(path + '/bindings/' + name, None, 'DELETE'))
            index = config_api(plan['exec_config_url'], '/api/strategies?execution_family=batch_exec')
            save(audit / 'exec_index.before_remove.json', index)
            for name in index['strategies']:
                query = urllib.parse.urlencode({'name': name, 'execution_family': 'batch_exec'})
                record('remove_exec:' + name, config_api(plan['exec_config_url'], '/api/strategy?' + query, 'DELETE'))
            if virtual:
                record('delete_virtual06', api('/manager/api/catalog/virtual-accounts/virtual06', None, 'DELETE'))
    with session('lgq') as api:
        studio = api('/manager/api/catalog/accounts/' + SOURCE)
        n.require(studio['configuration']['mode'] == 'independent' and not studio['bindings']
                  and not studio['pending_publishes'], 'Manager binding remains')
        virtuals = api('/manager/api/catalog/virtual-accounts')
        n.require(not any(v['virtual_id'] == 'virtual06' for v in virtuals), 'old virtual definition remains')
    index = config_api(plan['exec_config_url'], '/api/strategies?execution_family=batch_exec')
    n.require(not index['strategies'], 'active Exec strategy remains')
    row = require_flat(audit)
    result = {'source': SOURCE, 'old_bindings_removed': len(plan['bindings']), 'follows': 0,
              'active_exec_strategies': 0, 'perpetual_positions': 0, 'open_orders': 0, 'algo_orders': 0,
              'signal_disabled': True, 'submitters_stopped': True, 'checked_at_ms': row['checked_at_ms']}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
