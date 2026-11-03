"""Remove ICX's already-zero Manager targets from the two affected Prep02 strategies."""
import argparse
import copy
import json
import os
from pathlib import Path
import sys
import subprocess

M = Path.home() / 'crypto_cta_manager'
sys.path.insert(0, str(M / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session

NAMES = {'cta-prep02-bn-1min-pos', 'cta-prep02-bn-15min-pos'}
SOURCE = 'binance_exec_rx03'


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--audit', type=Path, required=True)
    p.add_argument('--verify-only', action='store_true')
    a = p.parse_args()
    os.umask(0o077)
    with session('lgq') as api:
        original = (json.loads((a.audit / 'before.json').read_text()) if a.verify_only else
                    {s['strategy_name']: s for s in api('/manager/api/catalog/position-strategies')})
        studio = api('/manager/api/catalog/accounts/' + SOURCE)
        assert all(b['shares'] == 0 for b in studio['bindings'])
        assert {s for s, v in original.items() if 'ICXUSDT' in v['targets']} == NAMES
        for name in NAMES:
            assert original[name]['targets']['ICXUSDT']['qty'] == 0
        print(json.dumps({'execute': a.execute, 'strategies': sorted(NAMES), 'qty_to_remove': 0}), flush=True)
        if not a.execute and not a.verify_only:
            return
        if not a.verify_only:
            a.audit.mkdir(mode=0o700, parents=True, exist_ok=False)
            (a.audit / 'before.json').write_text(json.dumps({name: original[name] for name in NAMES}, indent=2) + '\n')
        for name in ([] if a.verify_only else sorted(NAMES)):
            current = {s['strategy_name']: s for s in api('/manager/api/catalog/position-strategies')}
            assert current[name] == original[name], 'concurrent position edit'
            body = {k: copy.deepcopy(original[name][k]) for k in ['strategy_name', 'targets', 'symbol_order_strategy_overrides']}
            del body['targets']['ICXUSDT']
            body['symbol_order_strategy_overrides'].pop('ICXUSDT', None)
            response = api('/manager/api/catalog/position-strategies', body, 'POST')
            assert not response.get('publishes'), 'stopped binding received automatic targets'
            api('/manager/api/catalog/accounts/' + SOURCE + '/bindings/' + name + '/publish', {}, 'POST')
        current = {s['strategy_name']: s for s in api('/manager/api/catalog/position-strategies')}
        # Other owners' machine publishers may legitimately update their targets
        # while these two disabled strategies are edited.
        for name in NAMES:
            expected = copy.deepcopy(original[name]);expected['targets'].pop('ICXUSDT')
            expected['symbol_order_strategy_overrides'].pop('ICXUSDT', None)
            expected['updated_at_us'] = current[name]['updated_at_us']
            assert current[name] == expected, 'unexpected position change'
            runtime = json.loads(subprocess.check_output(
                ['redis-cli', '--raw', 'GET', SOURCE + ':binance-futures:batch_exec:' + name], text=True))
            assert 'ICXUSDT' not in runtime['targets']
            assert all(v['qty'] == 0 for v in runtime['targets'].values())
        assert all(b['shares'] == 0 for b in api('/manager/api/catalog/accounts/' + SOURCE)['bindings'])
        result = {'removed_symbol': 'ICXUSDT', 'strategies': sorted(NAMES), 'zero_targets_removed': 2,
                  'unrelated_strategy_writes': 0, 'runtime_targets_pruned_and_zero': True,
                  'all_bindings_still_stopped': True}
        (a.audit / 'verified.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
