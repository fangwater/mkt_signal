"""Audited RX04 activation: recover 100-USDT positions, then enable multiplier one."""
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import tomllib
import urllib.request

import ops_remote as op

STAGE = op.STAGE / 'activate_100'
CONFIG_URL = op.ORIGIN + '/cta/binance-cta-perp-rx04/config/api/config'
HISTORY = op.SIGNAL / 'data/cta_perp_5m/factor_history'


def get_json(url):
    with urllib.request.urlopen(url, timeout=20) as response:
        return json.load(response)


def signal_status():
    return json.loads((op.SIGNAL / 'run/cta_perp_status.json').read_text())


def check_signal(status, enabled):
    assert status['enabled'] is enabled and status['bar_interval'] == '5m'
    assert status['max_pos_u'] == 100 and status['position_algorithm'] == 'v0'
    assert status['journal_healthy'] and not status['recovering']
    assert not any(status.get(key) for key in ['configuration_error', 'publication_error', 'recovery_error'])
    assert status['follower_source_id'] == op.EXEC.name
    rows = [(factor, symbol, row) for factor, symbols in status['strategies'].items() for symbol, row in symbols.items()]
    assert len(rows) == 48 and len(status['strategies']) == 35
    now = time.time_ns() // 1_000_000
    assert now - status['updated_ts_us'] // 1000 < 10000
    assert all(row.get('score_ready') and row.get('signal', {}).get('filter_ready') and 0 <= now - row['model_ts_ms'] < 360000 for _, _, row in rows)
    return rows


def preflight():
    op.stopped()
    assert not STAGE.exists()
    manifest = json.loads((op.EXEC / 'release.json').read_text())
    for row in manifest['files']:
        assert hashlib.sha256((op.EXEC / row['path']).read_bytes()).hexdigest() == row['sha256']
    config = get_json(CONFIG_URL)
    assert config['config']['max_pos_u'] == 100 and config['config']['enabled'] is False
    status = signal_status()
    rows = check_signal(status, False)
    history = json.loads((HISTORY / 'recovery.json').read_text())
    assert history['caught_up'] and history['bar_interval'] == '5m'
    assert time.time_ns() // 1_000_000 - history['updated_ms'] < 5000
    from neutralize_rapidx_binance import Client
    os.environ.update(op.env_for(op.EXEC))
    client = Client('172.31.35.228', execute=False)
    client.empty_orders()
    cash, perp, _ = client.snapshot()
    assert not any(perp.values()) and not any(value for key, value in cash.items() if key != 'USDT')
    with op.session() as api:
        account = api('/manager/api/catalog/accounts/' + op.EXEC.name)
        assert account['configuration'] == {'mode': 'follow', 'follows': [{'virtual_id': 'virtual05', 'multiplier': 0.0}]}
        assert len(account['bindings']) == 35 and not account['pending_publishes']
        assert all(row['shares'] == 0 and row['order_strategy_name'] == 'lgq_grid_prep' for row in account['bindings'])
        grid = next(row for row in api('/manager/api/catalog/order-strategies') if row['strategy_name'] == 'lgq_grid_prep')
        rx02 = api('/manager/api/catalog/accounts/binance_exec_trade08')
    STAGE.mkdir(mode=0o700)
    for source, name in [(op.SIGNAL / 'config/cta_perp.json', 'signal_config.before.json'), (op.SIGNAL / 'run/cta_perp_position_flags.json', 'position_flags.before.json'), (op.MANAGER / 'config/cta-manager.toml', 'manager.before.toml')]:
        shutil.copy2(source, STAGE / name)
        os.chmod(STAGE / name, 0o600)
    op.save(STAGE / 'preflight.json', {'at_us': time.time_ns() // 1000, 'config': config, 'status': status, 'factor_history': history, 'account': account, 'grid_template': grid, 'rx02_account': rx02, 'protected': op.capture_protected(), 'account_before': {'cash_usdt': str(cash['USDT']), 'nonzero_positions': 0, 'open_orders': 0}, 'release_commit': manifest['commit']})
    print(json.dumps({'ready_pairs': len(rows), 'cash_usdt': str(cash['USDT']), 'empty_positions_and_orders': True, 'max_pos_u': 100, 'previous_observation_positions': sum(bool(row['position_qty']) for _, _, row in rows)}))


def recover():
    op.stopped()
    before = json.loads((STAGE / 'preflight.json').read_text())
    assert get_json(CONFIG_URL) == before['config']
    assert not (op.SIGNAL / 'run/cta_perp_recovery_request.json').exists()
    history = json.loads((HISTORY / 'recovery.json').read_text())
    assert history['generation'] == before['factor_history']['generation']
    subprocess.run([str(op.SIGNAL / 'scripts/stop_cta_perp_5m_signal.sh')], cwd=op.SIGNAL, check=True)
    op.save(op.SIGNAL / 'run/cta_perp_recovery_request.json', {'history_path': str(HISTORY), 'generation': history['generation']})
    subprocess.run([str(op.SIGNAL / 'scripts/start_cta_perp_5m_signal.sh')], cwd=op.SIGNAL, check=True)
    print('RX04 observation signal restarted for silent position recovery; publisher unchanged')


def recovered():
    status = signal_status()
    rows = check_signal(status, False)
    assert not (op.SIGNAL / 'run/cta_perp_recovery_request.json').exists()
    complete = json.loads((op.SIGNAL / 'run/cta_perp_recovery_completed.json').read_text())
    before = json.loads((STAGE / 'preflight.json').read_text())
    assert complete['generation'] == before['factor_history']['generation']
    current_history = json.loads((HISTORY / 'recovery.json').read_text())
    assert current_history['generation'] == complete['generation']
    for _, _, row in rows:
        if row['position_qty']:
            assert row['entry_notional'] == 100
        assert math.isfinite(row['position_qty'])
    result = {'at_us': time.time_ns() // 1000, 'max_pos_u': 100, 'ready_pairs': len(rows), 'nonzero_positions': sum(bool(row['position_qty']) for _, _, row in rows), 'all_entry_notionals_100': True, 'publisher_generation_unchanged': True, 'status': status, 'positions': json.loads((op.SIGNAL / 'run/cta_perp_position_flags.json').read_text())}
    op.save(STAGE / 'recovered.json', result)
    print(json.dumps({key: value for key, value in result.items() if key not in ['status', 'positions']}))


def monitor():
    path = op.MANAGER / 'config/cta-manager.toml'
    assert path.read_bytes() == (STAGE / 'manager.before.toml').read_bytes()
    before = tomllib.loads(path.read_text())
    text = path.read_text()
    blocks = re.split(r'(?=^\[\[sources\]\]\s*$)', text, flags=re.M)
    count = 0
    for index, block in enumerate(blocks):
        if re.search(r'^id\s*=\s*"' + re.escape(op.EXEC.name) + r'"\s*$', block, re.M):
            assert re.search(r'^monitor_enabled\s*=\s*false\s*$', block, re.M)
            blocks[index] = re.sub(r'^monitor_enabled\s*=\s*false\s*$', 'monitor_enabled = true', block, flags=re.M)
            count += 1
    assert count == 1
    text = ''.join(blocks)
    parsed = tomllib.loads(text)
    expected = json.loads(json.dumps(before))
    next(row for row in expected['sources'] if row['id'] == op.EXEC.name)['monitor_enabled'] = True
    assert parsed == expected
    candidate = path.with_suffix('.toml.next')
    candidate.write_text(text)
    os.chmod(candidate, 0o600)
    os.replace(candidate, path)
    subprocess.run(['systemctl', '--user', 'restart', 'crypto-cta-manager-web.service'], check=True)
    print('Manager account observation enabled for RX04; trading targets remain zero')


def exec_state(api):
    snapshot = api('/exec_trade05/snapshot')
    entry = next(row for row in snapshot['entries'] if row['channel'] == 'exec_pre_trade_state')
    assert time.time_ns() // 1_000_000 - entry['ts_ms'] < 15000
    assert entry['entry']['position_ready']
    return entry['entry']


def enable():
    check_signal(signal_status(), False)
    before = json.loads((STAGE / 'preflight.json').read_text())
    config = get_json(CONFIG_URL)
    assert config == before['config'], 'operator changed strategy config since preflight'
    with op.session() as api:
        state = exec_state(api)
        assert all(not row['current_qty'] and not row['target_qty'] and not row['pending_qty'] and not row['live_order_qty'] for row in state['rows'])
        op.save(STAGE / 'exec_before_enable.json', state)
        account = api('/manager/api/catalog/accounts/' + op.EXEC.name)
        assert account['configuration'] == before['account']['configuration']
        assert next(row for row in api('/manager/api/catalog/order-strategies') if row['strategy_name'] == 'lgq_grid_prep') == before['grid_template']
        result = api('/manager/api/catalog/accounts/' + op.EXEC.name + '/configuration', {'mode': 'follow', 'follows': [{'virtual_id': 'virtual05', 'multiplier': 1.0}]}, 'PUT')
        op.save(STAGE / 'multiplier_one.json', result)
        deadline = time.monotonic() + 45
        while True:
            account = api('/manager/api/catalog/accounts/' + op.EXEC.name)
            if not account['pending_publishes']:
                break
            assert time.monotonic() < deadline
            time.sleep(1)
        assert len(account['bindings']) == 35 and all(row['shares'] == 1 and row['order_strategy_name'] == 'lgq_grid_prep' for row in account['bindings'])
    status = signal_status()
    rows = check_signal(status, False)
    assert all(row['entry_notional'] == 100 for _, _, row in rows if row['position_qty'])
    config['config']['enabled'] = True
    request = urllib.request.Request(CONFIG_URL, data=json.dumps({'config': config['config'], 'expected_revision': config['revision']}).encode(), headers={'Content-Type': 'application/json'}, method='PUT')
    with urllib.request.urlopen(request, timeout=20) as response:
        op.save(STAGE / 'enabled.json', json.load(response))
    print('RX04 enabled: multiplier one, 100-USDT recovered positions, lgq_grid_prep')


def validate():
    status = signal_status()
    rows = check_signal(status, True)
    with op.session('lgq') as api:
        account = api('/manager/api/catalog/accounts/' + op.EXEC.name)
        assert account['configuration'] == {'mode': 'follow', 'follows': [{'virtual_id': 'virtual05', 'multiplier': 1.0}]}
        assert not account['pending_publishes']
        assert all(row['shares'] == 1 and row['order_strategy_name'] == 'lgq_grid_prep' for row in account['bindings'])
        state = exec_state(api)
        rx02 = api('/manager/api/catalog/accounts/binance_exec_trade08')
    import redis
    client = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    targets = {}
    for factor, symbol, row in rows:
        if factor not in targets:
            runtime = json.loads(client.get(op.EXEC.name + ':binance-futures:batch_exec:' + op.PREFIX + factor))
            assert runtime['algorithm'] == 'grid'
            targets[factor] = runtime['targets']
        quantity = targets[factor][symbol]['qty']
        assert abs((0.0 if quantity == 'clear' else float(quantity)) - row['position_qty']) < 1e-9
    before = json.loads((STAGE / 'preflight.json').read_text())
    assert rx02['configuration'] == before['rx02_account']['configuration'] and rx02['bindings'] == before['rx02_account']['bindings']
    protected = before['protected']
    for path, digest in protected['files'].items():
        if path == str(op.SIGNAL / 'config/cta_perp.json'):
            continue
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest
    for process in protected['processes']:
        if process['cwd'] == str(op.SIGNAL) and process['comm'] == 'cta_perp_5m_sig':
            continue
        assert Path('/proc', str(process['pid']), 'stat').read_text().split()[21] == process['start']
    processes = [row for row in op.processes() if row['cwd'] == str(op.EXEC)]
    assert len(processes) == 6 and not any(row['comm'] == 'trade_signal' for row in processes)
    result = {'at_us': time.time_ns() // 1000, 'enabled': True, 'multiplier': 1, 'max_pos_u': 100, 'ready_pairs': len(rows), 'nonzero_signal_targets': sum(bool(row['position_qty']) for _, _, row in rows), 'exec_processes': processes, 'position_ready': state['position_ready'], 'exec_rows': state['rows'], 'publication_error': status['publication_error'], 'manager_publications': status['manager_publications'], 'pending_publishes': account['pending_publishes'], 'protected_processes_and_configs_unchanged': True, 'rx02_unchanged': True}
    op.save(STAGE / 'live_verified.json', result)
    print(json.dumps({key: value for key, value in result.items() if key != 'exec_rows'}))


if __name__ == '__main__':
    {'preflight': preflight, 'recover': recover, 'recovered': recovered, 'monitor': monitor, 'enable': enable, 'validate': validate}[sys.argv[1]]()
