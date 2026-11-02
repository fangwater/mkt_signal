"""Remove only the stopped, flat RX03/RX04 follows and Exec strategy indices.

Default performs signed exchange checks and prints the scoped removal plan.
Existing position strategy definitions, execution configs and history survive.
"""
import argparse
import json
import os
from pathlib import Path
import time
import urllib.parse
import urllib.request

import neutralize_rapidx_binance as n
from clear_positions import SCOPES, exchange_snapshot, inventory, save
from manager_ops import session


def config_api(base, path, method='GET'):
    url = urllib.parse.urlsplit(base)
    n.require(url.scheme == 'http' and url.hostname == '127.0.0.1', 'unexpected Exec endpoint')
    request = urllib.request.Request(base.rstrip('/') + path, method=method)
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(request, timeout=15) as response:
        result = json.load(response)
    n.require(result.get('ok') is True, 'Exec config request failed')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    n.require(args.audit.is_dir() and args.audit.name.startswith('rx03_rx04_taker_clear_'),
              'unexpected audit directory')
    plan = json.loads((args.audit / 'clear_plan.json').read_text())
    n.require([r['source'] for r in plan] == [s[1] for s in SCOPES], 'scope changed')
    current = inventory()
    n.require(not current['other_bindings'] and not current['pending'], 'shared binding or pending publish')
    for item in plan:
        source, prefix = item['source'], item['prefix']
        n.assert_stopped(Path.home() / source)
        n.require(json.loads((Path.home() / item['signal_env'] / 'config/cta_perp.json').read_text())['enabled'] is False,
                  'signal enabled')
        snap = exchange_snapshot(source)
        n.require(not snap['nonzero_positions'] and not snap['open_orders'] and not snap['algo_orders'],
                  f'{source}: exchange is not flat')
        journal = json.loads((args.audit / (source + '.tail_journal.json')).read_text())
        n.require(journal['pending'] is None, 'unresolved market order')
        expected = {b['binding_name']: b for b in item['bindings']}
        actual = [b for b in current['bindings'] or [] if b['source_id'] == source]
        for binding in actual:
            name = binding['binding_name']
            n.require(name in expected and name.startswith(prefix), 'unexpected live binding')
            n.require(all(binding[k] == expected[name][k] for k in
                          ('source_id', 'position_strategy_name', 'order_strategy_name')), 'binding changed')
            n.require(binding['shares'] in (0, expected[name]['shares']), 'shares changed')
        follows = [f for f in current['follows'] or [] if f['source_id'] == source]
        n.require(not follows or (len(follows) == 1 and follows[0]['virtual_id'] == item['virtual_id']
                                 and follows[0]['multiplier'] == 1), 'follow changed')
        index = config_api(item['exec_config_url'], '/api/strategies?execution_family=batch_exec')
        n.require(all(name.startswith(prefix) for name in index['strategies']), 'unrelated active Exec strategy')
        print(json.dumps({'source': source, 'execute': args.execute, 'manager_bindings': len(actual),
                          'follows': len(follows), 'exec_active_strategies': len(index['strategies']),
                          'positions': 0, 'open_orders': 0, 'algo_orders': 0}), flush=True)
    if not args.execute:
        return
    receipts_path = args.audit / 'remove_receipts.json'
    receipts = json.loads(receipts_path.read_text()) if receipts_path.exists() else []

    def record(action, source, result):
        receipts.append({'action': action, 'source': source, 'at_ms': time.time_ns() // 1_000_000,
                         'result': result})
        save(receipts_path, receipts)

    with session('lgq') as api:
        for item in plan:
            source = item['source']
            n.assert_stopped(Path.home() / source)
            studio_path = '/manager/api/catalog/accounts/' + source
            studio = api(studio_path + '/configuration', {'mode': 'independent'}, 'PUT')
            n.require(studio['configuration']['mode'] == 'independent', 'follow removal failed')
            record('remove_follow', source, studio)
            for binding in list(studio['bindings']):
                name = binding['binding_name']
                n.require(name in {b['binding_name'] for b in item['bindings']}, 'binding scope changed')
                path = studio_path + '/bindings/' + urllib.parse.quote(name, safe='')
                result = api(path + '/shares', {'shares': 0}, 'PUT')
                record('zero_shares:' + name, source, result)
                result = api(path, None, 'DELETE')
                record('delete_binding:' + name, source, result)
            index = config_api(item['exec_config_url'], '/api/strategies?execution_family=batch_exec')
            n.require(all(name.startswith(item['prefix']) for name in index['strategies']),
                      'Exec index scope changed')
            for name in index['strategies']:
                query = urllib.parse.urlencode({'name': name, 'execution_family': 'batch_exec'})
                result = config_api(item['exec_config_url'], '/api/strategy?' + query, 'DELETE')
                record('remove_exec_index:' + name, source, result)
            studio = api(studio_path)
            index = config_api(item['exec_config_url'], '/api/strategies?execution_family=batch_exec')
            n.require(studio['configuration']['mode'] == 'independent' and not studio['bindings']
                      and not studio['pending_publishes'] and not index['strategies'], 'incomplete removal')
            snap = exchange_snapshot(source)
            n.require(not snap['nonzero_positions'] and not snap['open_orders'] and not snap['algo_orders'],
                      'exchange became non-flat')
            result = {'source': source, 'account': studio, 'exec_index': index, 'exchange': snap}
            save(args.audit / (source + '.removed_verified.json'), result)
            print(json.dumps({'source': source, 'manager_bindings': 0, 'follows': 0,
                              'exec_active_strategies': 0, 'positions': 0, 'open_orders': 0, 'algo_orders': 0}),
                  flush=True)


if __name__ == '__main__':
    main()
