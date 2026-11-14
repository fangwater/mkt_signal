"""Remove inactive ICX from live CTA catalogs using the existing revision-checked API."""
import argparse
import copy
import json
from pathlib import Path
import sys
import time

PREVIOUS = Path('/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
sys.path.insert(0, str(PREVIOUS))
import ops


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    snapshots, changes = {}, []
    exchange = ops.exchange_snapshot(ops.SOURCE)
    ops.require(not any('ICX' in row['symbol'] for row in exchange['nonzero_positions']), 'ICX still has exchange exposure')
    ops.require(exchange['open_orders'] == exchange['algo_orders'] == 0, 'inspect live orders before removing ICX')
    _, execution = ops.snapshot()
    ops.require(execution['position_ready'], 'Exec position state is not ready')
    ops.require(not any(row['symbol'] == 'ICXUSDT' and any(abs(float(row.get(k, 0))) > 1e-12 for k in ['current_qty','pending_qty','live_order_qty']) for row in execution['rows']), 'ICX still has Exec exposure')
    for worker in ['cta-prep-binance-futures', 'cta-factor-spread']:
        catalog = ops.local_api('/api/config?worker=' + worker)
        snapshots[worker] = catalog
        for doc in catalog['plan']['combinations'].values():
            rules = [rule for rule in doc['factor_rules'] if rule['标的'] == 'ICXUSDT']
            if not rules:
                continue
            ops.require(worker == 'cta-prep-binance-futures' and doc['strategy_group'] == 'prep02' and doc['symbol_group'] == 'last-20', 'unexpected ICX scope')
            candidate = copy.deepcopy(doc)
            candidate['factor_rules'] = [rule for rule in doc['factor_rules'] if rule['标的'] != 'ICXUSDT']
            ops.local_api('/api/preview', {'worker': worker, 'expected_revision': catalog['revision'], 'combination': candidate})
            changes.append({'worker': worker, 'before': doc, 'after': candidate, 'removed_rules': len(rules)})
    ops.require(sum(change['removed_rules'] for change in changes) == 3, 'unexpected ICX rule count')
    summary = {'source': ops.SOURCE, 'ICX_position': 0, 'exchange_open_orders': 0, 'removed_rules': 3,
        'before_factor_count': snapshots['cta-prep-binance-futures']['unique_factor_count'], 'after_factor_count': 404,
        'strategy_count': 9, 'execute': args.execute}
    if not args.execute:
        print(json.dumps(summary));return
    audit = ops.M / 'maintenance-backups' / ('cta_icx_cleanup_' + time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()))
    audit.mkdir(mode=0o700)
    ops.save(audit/'catalogs-before.json', snapshots)
    ops.save(audit/'exchange-before.json', exchange)
    ops.save(audit/'execution-before.json', execution)
    expected = {worker: value['revision'] for worker, value in snapshots.items()}
    for change in changes:
        worker = change['worker']
        current = ops.local_api('/api/config?worker=' + worker)
        ops.require(current['revision'] == expected[worker], 'catalog changed during cleanup')
        result = ops.local_api('/api/combinations', {'worker': worker, 'expected_revision': expected[worker], 'combination': change['after']})
        expected[worker] = result['revision']
    for worker in snapshots:
        current = ops.local_api('/api/config?worker=' + worker)
        ops.require(current['revision'] == expected[worker], 'catalog changed during cleanup')
        groups = copy.deepcopy(current['plan']['symbol_groups'])
        for members in groups.values():
            if 'ICXUSDT' in members:members.remove('ICXUSDT')
        request = {'worker': worker, 'expected_revision': current['revision'], 'symbol_groups': groups}
        ops.local_api('/api/symbol-groups/preview', request)
        result = ops.local_api('/api/symbol-groups', request)
        expected[worker] = result['revision']
    for worker in snapshots:
        current = ops.local_api('/api/config?worker=' + worker)
        assert current['revision'] == expected[worker]
        for name, doc in current['plan']['combinations'].items():
            before = snapshots[worker]['plan']['combinations'][name]
            check = copy.deepcopy(before)
            check['factor_rules'] = [rule for rule in before['factor_rules'] if rule['标的'] != 'ICXUSDT']
            assert doc == check, 'unrelated strategy setting changed'
        assert not any('ICXUSDT' in members for members in current['plan']['symbol_groups'].values())
        ops.save(audit/(worker+'-after.json'), current)
    summary['audit'] = str(audit)
    ops.save(audit/'summary.json', summary)
    print(json.dumps(summary))


if __name__ == '__main__':main()
