"""Scoped RX03/RX04 stop and taker clear. Default only prepares/observes.

Uses the existing Manager protocol and RapidX read-only queries. Credentials stay
in remote process memory; target JSON and receipts remain in the private audit.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request

from manager_ops import env_for, sql
import neutralize_rapidx_binance as n
from binance_local_ip import resolve_local_address

SCOPES = [
    ('binance-cta-perp-rx03', 'binance_exec_trade07', 'virtual04', 'binance_cta_perp_rx03_', 28),
    ('binance-cta-perp-rx04', 'binance_exec_rx04_5m05', 'virtual05', 'binance_cta_perp_rx04_', 35),
]


def save(path, value):
    temp = path.with_suffix(path.suffix + '.next')
    with temp.open('w') as f:
        json.dump(value, f, indent=2, allow_nan=False)
        f.write('\n'); f.flush(); os.fsync(f.fileno())
    temp.chmod(0o600); os.replace(temp, path)


def inventory():
    ids = "'binance_exec_trade07','binance_exec_rx04_5m05'"
    return json.loads(sql(f"""SELECT jsonb_build_object(
      'bindings', (SELECT jsonb_agg(to_jsonb(b)) FROM cta_account_strategy_bindings b WHERE source_id IN ({ids})),
      'follows', (SELECT jsonb_agg(to_jsonb(f)) FROM cta_account_follows f WHERE source_id IN ({ids})),
      'pending', coalesce((SELECT jsonb_agg(to_jsonb(q)) FROM cta_follow_publish_queue q WHERE source_id IN ({ids})),'[]'::jsonb),
      'positions', (SELECT jsonb_agg(jsonb_build_object('strategy_name',p.strategy_name,'targets',p.targets)) FROM cta_position_strategies p WHERE strategy_name IN (SELECT position_strategy_name FROM cta_account_strategy_bindings WHERE source_id IN ({ids}))),
      'other_bindings', coalesce((SELECT jsonb_agg(to_jsonb(b)) FROM cta_account_strategy_bindings b WHERE source_id NOT IN ({ids}) AND position_strategy_name IN (SELECT position_strategy_name FROM cta_account_strategy_bindings WHERE source_id IN ({ids}))),'[]'::jsonb))"""))


def exchange_snapshot(source):
    root = Path.home() / source
    values = env_for(root)
    keys = ['LTP_API_KEY', 'LTP_API_SECRET', 'LTP_PORTFOLIO_ID', 'LTP_REST_URL']
    old = {key: os.environ.get(key) for key in keys}
    try:
        for key in keys:
            if key in values: os.environ[key] = values[key]
            else: os.environ.pop(key, None)
        ip, _ = resolve_local_address(env_dir=str(root))
        assert ip, 'missing explicit source IP'
        client = n.Client(ip, execute=False)
    finally:
        for key, value in old.items():
            if value is None: os.environ.pop(key, None)
            else: os.environ[key] = value
    rows = client.request('GET', '/api/v1/trading/position', {'exchange': 'BINANCE'})
    positions = []
    for row in rows:
        client.identity(row)
        assert row['sym'].startswith('BINANCE_PERP_') and row['sym'].endswith('_USDT')
        if n.dec(row['positionQty']):
            positions.append({'symbol': row['sym'], 'qty': row['positionQty']})
    orders = client.pages('/api/v1/trading/orders', {'exchange': 'BINANCE'})
    algos = client.pages('/api/v1/algo/openOrders', {'exchange': 'BINANCE'})
    for row in orders + algos: client.identity(row)
    return {'source': source, 'nonzero_positions': positions, 'open_orders': len(orders), 'algo_orders': len(algos), 'checked_at_ms': time.time_ns() // 1_000_000}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['prepare', 'clear', 'verify'])
    p.add_argument('--audit', type=Path, required=True)
    p.add_argument('--execute', action='store_true')
    args = p.parse_args(); os.umask(0o077)
    assert args.audit.is_dir() and args.audit.name.startswith('rx03_rx04_taker_clear_')
    for name, *_ in SCOPES:
        assert json.loads((Path.home() / name / 'config/cta_perp.json').read_text())['enabled'] is False
    if args.action == 'prepare':
        before = inventory()
        assert not before['other_bindings'] and not before['pending']
        positions = {r['strategy_name']: r['targets'] for r in before['positions']}
        plan = []
        sources = tomllib.loads((Path.home() / 'crypto_cta_manager/config/cta-manager.toml').read_text())['sources']
        for name, source, virtual, prefix, count in SCOPES:
            bindings = [b for b in before['bindings'] if b['source_id'] == source]
            follows = [f for f in before['follows'] if f['source_id'] == source]
            assert len(bindings) == count and len(follows) == 1 and follows[0]['virtual_id'] == virtual
            assert all(b['position_strategy_name'].startswith(prefix) and b['binding_name'] == b['position_strategy_name'] for b in bindings)
            state = json.loads((args.audit / name / 'cta_perp_position_flags.json').read_text())
            payloads = []
            for b in bindings:
                strategy = b['position_strategy_name']
                symbols = set(positions[strategy]) | set(state.get(strategy[len(prefix):], {}))
                assert symbols, f'empty target scope: {strategy}'
                payloads.append({'strategy_name': strategy, 'targets': {s: {'qty': 'clear', 'signal': 1} for s in sorted(symbols)}})
            config = next(s for s in sources if s['id'] == source)
            snap = exchange_snapshot(source)
            plan.append({'signal_env': name, 'source': source, 'virtual_id': virtual, 'prefix': prefix, 'bindings': bindings, 'payloads': payloads, 'exec_config_url': config['exec_config_url'], 'exec_viz_url': config['exec_viz_url']})
            save(args.audit / (source + '.exchange_before.json'), snap)
            print(json.dumps({'source': source, 'strategies': len(payloads), 'target_rows': sum(len(x['targets']) for x in payloads), 'position_count': len(snap['nonzero_positions']), 'open_orders': snap['open_orders'], 'algo_orders': snap['algo_orders']}), flush=True)
        assert not (args.audit / 'clear_plan.json').exists()
        save(args.audit / 'manager_before.json', before)
        save(args.audit / 'clear_plan.json', plan)
    elif args.action == 'verify':
        rows = [exchange_snapshot(source) for _, source, *_ in SCOPES]
        save(args.audit / 'exchange_verified.json', rows)
        for row in rows: print(json.dumps(row), flush=True)
    else:
        plan = json.loads((args.audit / 'clear_plan.json').read_text())
        current = inventory()
        before = json.loads((args.audit / 'manager_before.json').read_text())
        assert current['bindings'] == before['bindings'] and current['follows'] == before['follows'] and not current['other_bindings']
        print(json.dumps({'action': 'clear', 'execute': args.execute, 'sources': [r['source'] for r in plan], 'strategies': sum(len(r['payloads']) for r in plan)}), flush=True)
        if not args.execute: return
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        receipts = []
        for item in plan:
            values = env_for(Path.home() / item['signal_env'])
            urls = values.get('CTA_MANAGER_API_URLS', values.get('CTA_MANAGER_API_URL', '')).split(',')
            assert len(urls) == 1 and urllib.parse.urlsplit(urls[0]).hostname in {'127.0.0.1', 'localhost'}
            overrides = json.loads(values.get('CTA_PUBLISH_TOKENS', '{}'))
            token = overrides.get(urls[0], values.get('CTA_PUBLISH_TOKEN'))
            assert token, 'missing publish credential'
            for payload in item['payloads']:
                request = urllib.request.Request('http://127.0.0.1:18201/api/catalog/position-strategies', data=json.dumps(payload).encode(), headers={'X-CTA-Publish-Token': token, 'Content-Type': 'application/json'}, method='POST')
                try:
                    with opener.open(request, timeout=30) as response:
                        data = json.load(response)
                except urllib.error.HTTPError as exc:
                    raise RuntimeError(f"{payload['strategy_name']}: Manager HTTP {exc.code}") from None
                receipts.append({'source': item['source'], 'strategy_name': payload['strategy_name'], 'published_at_ms': time.time_ns() // 1_000_000, 'response': data})
                save(args.audit / 'clear_receipts.json', receipts)
                assert data['strategy_name'] == payload['strategy_name']
                assert len(data['publishes']) == 1 and data['publishes'][0]['source_id'] == item['source']
                assert all(not r.get('error') for r in data['publishes'])
            print(json.dumps({'source': item['source'], 'taker_clear_strategies_published': len(item['payloads'])}), flush=True)


if __name__ == '__main__':
    main()
