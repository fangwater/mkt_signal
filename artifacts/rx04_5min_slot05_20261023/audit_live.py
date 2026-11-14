"""Read-only RX04 live acceptance report; never print credential material."""
import collections
import json
import os
from pathlib import Path
import subprocess
import time
import tomllib

import activate as act
import ops_remote as op
from neutralize_rapidx_binance import Client


def audit():
    status = act.signal_status()
    act.check_signal(status, True)
    with op.session('lgq') as api:
        state = act.exec_state(api)
        account = api('/manager/api/catalog/accounts/' + op.EXEC.name)
    rows = state['rows']
    os.environ.update(op.env_for(op.EXEC))
    client = Client('172.31.35.228', execute=False)
    cash, perp, marks = client.snapshot()
    orders = client.pages('/api/v1/trading/orders', {'exchange': 'BINANCE'})
    algos = client.pages('/api/v1/algo/openOrders', {'exchange': 'BINANCE'})
    for order in orders + algos:
        client.identity(order)
    processes = []
    manager_pid = int(subprocess.check_output(['systemctl', '--user', 'show', 'crypto-cta-manager-web.service', '-p', 'MainPID', '--value'], text=True))
    for row in op.processes() + [{'pid': manager_pid, 'cwd': str(op.MANAGER), 'comm': 'cta_web'}]:
        if row['cwd'] not in (str(op.EXEC), str(op.SIGNAL), str(op.MANAGER)):
            continue
        masks = set()
        for task in Path('/proc', str(row['pid']), 'task').iterdir():
            masks.add(next(line.split(':', 1)[1].strip() for line in (task / 'status').read_text().splitlines() if line.startswith('Cpus_allowed_list:')))
        processes.append(dict(row, thread_cpu_masks=sorted(masks)))
    config = tomllib.loads((op.MANAGER / 'config/cta-manager.toml').read_text())
    source = next(row for row in config['sources'] if row['id'] == op.EXEC.name)
    assert source['enabled'] and source['monitor_enabled']
    summary = {
        'at_us': time.time_ns() // 1000,
        'enabled': status['enabled'], 'multiplier': account['configuration']['follows'][0]['multiplier'],
        'max_pos_u': status['max_pos_u'], 'manager_monitor_enabled': source['monitor_enabled'],
        'position_ready': state['position_ready'], 'exec_rows': len(rows),
        'rows_with_fills': sum(bool(row['current_qty']) for row in rows),
        'execution_complete_rows': sum(row['execution_complete'] for row in rows),
        'incomplete_rows': [row for row in rows if not row['execution_complete']],
        'completion_reasons': dict(collections.Counter(row['completion_reason'] for row in rows)),
        'max_abs_remaining_usdt': max((abs(row['delta_usdt']) for row in rows), default=0),
        'open_orders': len(orders), 'algo_orders': len(algos),
        'exchange_nonzero_positions': {coin: str(qty) for coin, qty in perp.items() if qty},
        'cash_usdt': str(cash.get('USDT', 0)),
        'pending_publishes': account['pending_publishes'], 'publication_error': status['publication_error'],
        'journal_healthy': status['journal_healthy'], 'processes': processes,
    }
    result = dict(summary, exec_state=state)
    op.save(act.STAGE / ('execution_' + str(summary['at_us']) + '.json'), result)
    print(json.dumps(summary))


if __name__ == '__main__':
    audit()
