"""Read-only reconciliation of the six Prep strategies and their Exec targets."""
import json
from pathlib import Path
import sys
import time
import tomllib

import activate as a
from clear_positions import exchange_snapshot

try:
    import redis
except ImportError:
    for site in (a.H / '.venvs/default/lib').glob('python*/site-packages'):
        sys.path.insert(0, str(site))
    import redis


def quantity(value):
    return 0.0 if value == 'clear' else float(value)


def main():
    audit = Path(sys.argv[1])
    a.require(audit.parent == a.M / 'maintenance-backups' and audit.name.startswith('cta_prep_live_'),
              'unexpected audit directory')
    r = redis.Redis.from_url(tomllib.loads((a.M / 'config/cta-manager.toml').read_text())['redis']['url'])
    with a.session('lgq') as manager:
        catalog = {p['strategy_name']: p for p in manager('/manager/api/catalog/position-strategies')}
        studios = {source: manager('/manager/api/catalog/accounts/' + source) for source in a.SOURCES}
        dashboard = manager('/manager/api/dashboard')
    manager_orders = [{key: row[key] for key in ['source_id', 'initial_position_count', 'order_event_count',
                                               'fill_count', 'first_fill_ts_us', 'last_fill_ts_us']}
                      for row in dashboard['report']['sources'] if row['source_id'] in a.SOURCES]
    tables = []
    for b in a.expected():
        name = b['binding_name']
        source = b['source_id']
        own = studios[source]
        a.require(not own['pending_publishes'], 'Manager account still has pending publications')
        actual = next(x for x in own['bindings'] if x['binding_name'] == name)
        a.require(actual['shares'] == 1, 'binding share differs')
        batch = json.loads(r.get(f'{source}:binance-futures:batch_exec:{name}'))
        targets = catalog[name]['targets']
        a.require(batch['algorithm'] == 'grid', 'execution algorithm differs')
        match = set(targets) == set(batch['targets']) and all(
            str(targets[symbol]['qty']) == str(batch['targets'][symbol]['qty'])
            or quantity(targets[symbol]['qty']) == quantity(batch['targets'][symbol]['qty'])
            for symbol in targets)
        tables.append({'source_id': source, 'strategy_name': name, 'shares': 1,
                       'target_count': len(targets),
                       'nonzero_targets': sum(quantity(v['qty']) != 0 for v in targets.values()),
                       'manager_targets_match_exec': match,
                       'manager_updated_at_us': catalog[name].get('updated_at_us'),
                       'exec_updated_at_us': batch.get('updated_at_us')})
    activity = json.loads(a.sql("""
        SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM (
          SELECT source_id, status, count(*) AS events,
                 count(DISTINCT client_order_id) AS unique_orders
          FROM cta_uniform_order_events
          WHERE source_id IN ('binance_exec_rx01','binance_exec_rx03')
          GROUP BY source_id,status ORDER BY source_id,status
        ) t
    """))
    origins = json.loads(a.sql("""
        SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM (
          SELECT source_id, from_key_text, count(*) AS events
          FROM cta_uniform_order_events
          WHERE source_id IN ('binance_exec_rx01','binance_exec_rx03')
          GROUP BY source_id,from_key_text ORDER BY source_id,from_key_text
        ) t
    """))
    accounts = [exchange_snapshot(source) for source in a.SOURCES]
    s = a.state()
    a.require(not s['warming'] and not s['error'] and not s['positions_error'], 'calculation is not live')
    a.require(all(t['enabled'] for t in s['position_tables']['tables']), 'runtime table disabled')
    result = {'checked_at_ms': time.time_ns() // 1000000, 'generation': s['generation'],
              'tables': tables, 'manager_order_reports': manager_orders,
              'legacy_postgres_order_activity': activity, 'legacy_postgres_order_attribution': origins,
              'exchange_accounts': accounts,
              'manager_publications': s['position_tables']['manager_publications'],
              'publication_error': s['position_tables']['publication_error'],
              'factor_outputs': len(s['latest']),
              'ready_factors': sum(all(f['ready'] for f in p['fields'].values()) for p in s['latest'].values())}
    a.save(audit / 'trading_verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
