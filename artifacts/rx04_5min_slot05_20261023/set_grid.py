"""Switch only RX04 bindings to lgq_grid_prep; keep all execution gates off."""
import argparse
import json
import time

import ops_remote as op


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    op.stopped()
    with op.session() as api:
        virtual = next(row for row in api('/manager/api/catalog/virtual-accounts') if row['virtual_id'] == 'virtual05')
        account = api('/manager/api/catalog/accounts/' + op.EXEC.name)
        template = next(row for row in api('/manager/api/catalog/order-strategies') if row['strategy_name'] == 'lgq_grid_prep')
        protected_rx02 = api('/manager/api/catalog/accounts/binance_exec_trade08')
        assert virtual['name'] == op.ALIAS and len(virtual['bindings']) == 35
        assert all(row['order_strategy_name'] == op.PREFIX + 'ctaker' for row in virtual['bindings'])
        expected_configuration = {'mode': 'follow', 'follows': [{'virtual_id': 'virtual05', 'multiplier': 0.0}]}
        assert account['configuration'] == expected_configuration
        assert len(account['bindings']) == 35 and all(row['shares'] == 0 for row in account['bindings'])
        assert template['order_parameters']['algorithm'] == 'grid'
        payload = {'name': virtual['name'], 'bindings': [dict(row, order_strategy_name='lgq_grid_prep') for row in virtual['bindings']]}
        before = {'virtual': virtual, 'account': account, 'grid_template': template, 'rx02_account': protected_rx02, 'processes': op.processes()}
        if not args.execute:
            print(json.dumps({'execute': False, 'source': op.EXEC.name, 'virtual_id': 'virtual05', 'changed_bindings': 35, 'order_strategy': 'lgq_grid_prep', 'multiplier': 0, 'signal_enabled': False, 'grid': template['order_parameters']['grid']['futures']}))
            return
        op.save(op.STAGE / 'grid_before.json', before)
        api('/manager/api/catalog/virtual-accounts/virtual05', payload, 'PUT')
        deadline = time.monotonic() + 45
        while True:
            actual = api('/manager/api/catalog/accounts/' + op.EXEC.name)
            if not actual['pending_publishes']:
                break
            assert time.monotonic() < deadline, 'disabled grid publication is still pending'
            time.sleep(1)
        assert actual['configuration'] == expected_configuration
        assert len(actual['bindings']) == 35
        assert all(row['shares'] == 0 and row['order_strategy_name'] == 'lgq_grid_prep' for row in actual['bindings'])
        updated = next(row for row in api('/manager/api/catalog/virtual-accounts') if row['virtual_id'] == 'virtual05')
        assert updated['bindings'] == payload['bindings']
        assert next(row for row in api('/manager/api/catalog/order-strategies') if row['strategy_name'] == 'lgq_grid_prep') == template
        rx02 = api('/manager/api/catalog/accounts/binance_exec_trade08')
        assert rx02['configuration'] == protected_rx02['configuration'] and rx02['bindings'] == protected_rx02['bindings']
        import redis
        client = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        prefix = op.EXEC.name + ':binance-futures:batch_exec:'
        names = json.loads(client.get(prefix + 'strategy_names'))
        assert len(names) == 35
        runtime = {name: json.loads(client.get(prefix + name)) for name in names}
        for row in runtime.values():
            assert row['algorithm'] == 'grid'
            assert row['grid'] == template['order_parameters']['grid']
            assert row['single_order_usdt'] == template['order_parameters']['single_order_usdt']
            assert row['target_tolerance_usdt'] == template['order_parameters']['target_tolerance_usdt']
            assert all(target['qty'] in (0, 'clear') for target in row['targets'].values())
        op.stopped()
        assert op.processes() == before['processes']
        result = {'verified_at_us': time.time_ns() // 1000, 'virtual': updated, 'account': actual, 'order_strategy': template, 'runtime': runtime, 'template_unchanged': True, 'rx02_unchanged': True, 'processes_unchanged': True, 'signal_enabled': False, 'exec_processes': []}
        op.save(op.STAGE / 'grid_verified.json', result)
        print(json.dumps({'changed_bindings': 35, 'order_strategy': 'lgq_grid_prep', 'runtime_algorithms': ['grid'], 'multiplier': 0, 'signal_enabled': False, 'exec_processes_started': False, 'template_unchanged': True}))


if __name__ == '__main__':
    main()
