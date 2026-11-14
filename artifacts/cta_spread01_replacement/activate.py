#!/usr/bin/env python3
"""Enable the reviewed Spread01 replacement; dry-run unless --execute is given."""
import argparse
import copy
import json
from pathlib import Path
import time
import urllib.request


def api(path, payload=None):
    request = urllib.request.Request(
        'http://127.0.0.1:19220/' + path,
        data=None if payload is None else json.dumps(payload).encode(),
        headers={'Content-Type': 'application/json', 'Origin': 'http://127.0.0.1:19220'},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    home = Path.home()
    root = home / 'cta-factor-spread'
    audit = Path((home / 'cta-audits/spread01_reset_current.txt').read_text().strip())
    state = json.loads((root / 'data/cta_factor_spread/state.json').read_text())
    assert state['generation'] and not state['warming'], 'wait for Kafka replay'
    assert state['error'] is None and state['positions_error'] is None
    assert 0 <= time.time() * 1000 - state['updated_ms'] < 15000, 'worker status is stale'
    seed = state['hourly_history_warmup']['active']
    assert not seed['missing_symbols'] and len(seed['ready_formulas']) == 88
    assert all(seed['ready_formulas'].values())
    hourly = [point for name, point in state['latest'].items() if '-1h-' in name]
    ready = sum(all(field['ready'] for field in point['fields'].values()) for point in hourly)
    assert len(hourly) == ready == 109, ('hourly calculations not ready', ready, len(hourly))
    minute_ts = min(point['ts_ms'] for name, point in state['latest'].items() if '-1min-' in name)
    assert time.time() * 1000 - minute_ts < 125000, 'Kafka has not caught up to live bars'
    catalog = api('api/config?worker=cta-factor-spread')
    combinations = catalog['plan']['combinations']
    expected = {'1min': 28, '5min': 56, '15min': 18, '1h': 107}
    assert len(combinations) == 4 and catalog['unique_factor_count'] == 215
    for combination in combinations.values():
        assert combination['strategy_group'] == 'spread01' and not combination.get('symbol_group')
        assert len(combination['factor_rules']) == expected[combination['interval']]
        assert combination['signal']['enabled'] is False
        assert combination['signal']['max_pos_u'] == 100
    print(json.dumps({'execute': args.execute, 'worker': str(root), 'venues': ['okex-futures', 'binance-futures'],
                      'scope': '12 reviewed symbols; four unpooled frequency strategies',
                      'generation': state['generation'], 'hourly_ready': ready, 'rules': expected}), flush=True)
    if not args.execute:
        return
    (audit / 'activation_config_before.json').write_text(json.dumps(catalog, indent=2))
    results = []
    for interval in expected:
        name = 'okex-futures_binance-futures_' + interval
        combination = copy.deepcopy(catalog['plan']['combinations'][name])
        combination['signal']['enabled'] = True
        payload = {'worker': 'cta-factor-spread', 'combination': combination,
                   'expected_revision': catalog['revision']}
        preview = api('api/preview', payload)
        assert not preview['excluded_symbols'], preview['excluded_symbols']
        catalog = api('api/combinations', payload)
        assert not catalog['excluded_symbols'], catalog['excluded_symbols']
        results.append({'interval': interval, 'rules': len(combination['factor_rules']), 'enabled': True})
        print('ENABLED', interval, flush=True)
        (audit / 'activation_results.json').write_text(json.dumps(results, indent=2))
    (audit / 'activation_config_after.json').write_text(json.dumps(catalog, indent=2))


if __name__ == '__main__':
    main()
