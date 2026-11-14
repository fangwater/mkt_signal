"""Remove only ICX rules through the live factor Config API; dry-run by default."""
import argparse
import copy
import json
import os
from pathlib import Path
import urllib.request

ORIGIN = 'http://127.0.0.1:19220'
WORKER = 'cta-prep-binance-futures'
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def api(path, body=None):
    request = urllib.request.Request(ORIGIN + path, data=None if body is None else json.dumps(body).encode(),
                                     headers={'Content-Type': 'application/json'})
    with OPENER.open(request, timeout=30) as response:
        return json.load(response)


def catalog():
    return api('/api/config?worker=' + WORKER)


def without_icx(combination):
    value = copy.deepcopy(combination)
    value['factor_rules'] = [r for r in value['factor_rules'] if r['标的'].upper() not in {'ICX', 'ICXUSDT'}]
    return value


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--audit', type=Path, required=True)
    args = p.parse_args()
    os.umask(0o077)
    before = catalog()
    originals = before['plan']['combinations']
    changes = {name: without_icx(c) for name, c in originals.items() if without_icx(c) != c}
    assert set(changes) == {'prep02_binance-futures_1min', 'prep02_binance-futures_15min'}
    assert sum(len(originals[n]['factor_rules']) - len(c['factor_rules']) for n, c in changes.items()) == 3
    assert all(c['signal']['enabled'] is False for c in originals.values())
    for c in changes.values():
        api('/api/preview', {'worker': WORKER, 'expected_revision': before['revision'], 'combination': c})
    print(json.dumps({'execute': args.execute, 'changed': sorted(changes), 'removed_rules': 3}), flush=True)
    if not args.execute:
        return
    args.audit.mkdir(mode=0o700, parents=True, exist_ok=False)
    (args.audit / 'catalog.before.json').write_text(json.dumps(before, indent=2) + '\n')
    previous = before
    for name, combination in changes.items():
        current = catalog()
        assert current['revision'] == previous['revision'], 'concurrent config edit'
        previous = api('/api/combinations', {'worker': WORKER, 'expected_revision': current['revision'], 'combination': combination})
    after = catalog()
    expected = copy.deepcopy(originals)
    expected.update(changes)
    assert after['plan']['combinations'] == expected, 'unexpected config change'
    assert not any('icxusdt' in name for name in after['plan']['calculations'])
    result = {'removed_symbol': 'ICXUSDT', 'removed_rules': 3,
              'removed_calculations': len(before['plan']['calculations']) - len(after['plan']['calculations']),
              'remaining_calculations': len(after['plan']['calculations']), 'all_publishing_disabled': True,
              'other_configuration_unchanged': True, 'revision': after['revision']}
    (args.audit / 'verified.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
