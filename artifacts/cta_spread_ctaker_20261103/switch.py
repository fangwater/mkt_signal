"""Switch the existing two-market Spread execution template to ctaker."""
import copy
import json
import os
from pathlib import Path
import sys
import time
import tomllib

HOME = Path.home()
MANAGER = HOME / 'crypto_cta_manager'
AUDIT = MANAGER / 'maintenance-backups/cta_spread_ctaker_20261103'
NAME = 'cta-spread01-okx-bn-1min-pos'
TEMPLATE = 'lgq_grid_spread'
SOURCES = (('okex_exec_trade01', 'okex-futures'), ('binance_exec_spread01', 'binance-futures'))
sys.path.insert(0, str(MANAGER / 'maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session, sql
for site in (HOME / '.venvs/default/lib').glob('python*/site-packages'):
    sys.path.insert(0, str(site))
import redis


def require(ok, message):
    if not ok:
        raise RuntimeError(message)


def save(name, value):
    path = AUDIT / name
    path.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    path.chmod(0o600)


def get_bindings():
    return json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(b)), '[]'::jsonb) FROM cta_account_strategy_bindings b WHERE order_strategy_name = 'lgq_grid_spread'"))


def get_template(api):
    return next(x for x in api('/manager/api/catalog/order-strategies') if x['strategy_name'] == TEMPLATE)


def get_exec():
    client = redis.Redis.from_url(tomllib.loads((MANAGER / 'config/cta-manager.toml').read_text())['redis']['url'])
    return {source: json.loads(client.get(f'{source}:{venue}:batch_exec:{NAME}')) for source, venue in SOURCES}


def prepare():
    AUDIT.mkdir(mode=0o700, exist_ok=False)
    bindings = get_bindings()
    require(len(bindings) == 2 and {b['source_id'] for b in bindings} == {s for s, _ in SOURCES}, 'template is used outside scope')
    require(all(b['binding_name'] == NAME and b['position_strategy_name'] == NAME and b['shares'] == 1 for b in bindings), 'binding scope changed')
    overrides = json.loads(sql("SELECT coalesce(jsonb_agg(strategy_name), '[]'::jsonb) FROM cta_position_strategies WHERE symbol_order_strategy_overrides::text LIKE '%lgq_grid_spread%'"))
    require(not overrides, 'template is used in position overrides')
    own_overrides = json.loads(sql("SELECT symbol_order_strategy_overrides FROM cta_position_strategies WHERE strategy_name='cta-spread01-okx-bn-1min-pos'"))
    require(not own_overrides, 'spread has per-symbol order template overrides')
    with session('lgq') as api:
        template = get_template(api)
    require(template['can_configure'] and template['order_parameters']['algorithm'] == 'grid', 'unexpected template')
    executions = get_exec()
    require(all(e['algorithm'] == 'grid' and len(e['targets']) == 8 and not e.get('symbol_overrides') for e in executions.values()), 'unexpected live execution config')
    save('before.json', {'template': template, 'bindings': bindings, 'exec': executions, 'at_ms': time.time_ns() // 1000000})
    print('Preflight: two Spread bindings, no symbol overrides, eight symbols per side; current algorithm grid.', flush=True)


def apply():
    before = json.loads((AUDIT / 'before.json').read_text())
    current = get_bindings()
    require(current == before['bindings'], 'bindings changed since preflight')
    with session('lgq') as api:
        template = get_template(api)
        require(template['order_parameters'] == before['template']['order_parameters'], 'template changed since preflight')
        params = copy.deepcopy(template['order_parameters'])
        params['algorithm'] = 'ctaker'
        result = api('/manager/api/catalog/order-strategies', {'strategy_name': TEMPLATE, 'order_parameters': params}, 'POST')
        require(result['order_parameters'] == params, 'saved template differs')
        save('template-saved.json', {'template': result, 'at_ms': time.time_ns() // 1000000})
        print('Template saved as ctaker.', flush=True)
        for source, _ in SOURCES:
            published = api(f'/manager/api/catalog/accounts/{source}/bindings/{NAME}/publish', {}, 'POST')
            save(source + '.published.json', published)
            print('Published ctaker: ' + source, flush=True)


def verify():
    before = json.loads((AUDIT / 'before.json').read_text())
    with session('lgq') as api:
        template = get_template(api)
        studios = {s: api('/manager/api/catalog/accounts/' + s) for s, _ in SOURCES}
    expected = copy.deepcopy(before['template']['order_parameters'])
    expected['algorithm'] = 'ctaker'
    require(template['order_parameters'] == expected, 'template parameters changed beyond algorithm')
    require(get_bindings() == before['bindings'], 'binding shares or mappings changed')
    executions = get_exec()
    for source, payload in executions.items():
        require(payload['algorithm'] == 'ctaker' and len(payload['targets']) == 8 and not payload.get('symbol_overrides'), source + ': ctaker not applied to all symbols')
        require(not studios[source]['pending_publishes'], source + ': pending publishes')
    result = {'template': TEMPLATE, 'algorithm': 'ctaker', 'per_market': {source: {'symbol_count': len(value['targets']), 'nonzero_targets': sum(float(v['qty']) != 0 for v in value['targets'].values()), 'updated_at_us': value['updated_at_us']} for source, value in executions.items()}, 'binding_shares': 1, 'other_parameters_unchanged': True, 'verified_at_ms': time.time_ns() // 1000000}
    save('verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    os.umask(0o077)
    require(sys.argv[1] in ('prepare', 'apply', 'verify'), 'unknown phase')
    if sys.argv[1] == 'apply':
        require(sys.argv[2:] == ['--execute'], 'apply requires --execute')
    globals()[sys.argv[1]]()
