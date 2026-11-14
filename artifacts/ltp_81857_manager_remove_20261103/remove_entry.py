"""Remove only the retired lgq_ltp_81857 Manager source. Never trade.

Run next to the existing manager_entries/manager_ops operational helpers on JP.
Default prepare is read-only; applying the backed-up config requires --execute.
The new cross-spread account and both execution legs must remain unchanged.
"""
import argparse
import copy
import json
import os
from pathlib import Path
import re
import subprocess
import time
import tomllib

from manager_entries import MANAGER, CONFIG, SERVICE, protected_processes, write_private, install, restart
from manager_ops import session, sql
from clear_positions import save
import neutralize_rapidx_binance as n

SOURCE = 'binance_exec_trade08'
GROUP = 'cta_cross_spread_rx01'
LEGS = ['okex_exec_trade01', 'binance_exec_spread01']


def candidate(raw):
    before = tomllib.loads(raw.decode())
    source = next(s for s in before['sources'] if s['id'] == SOURCE)
    n.require(source['alias'] == 'lgq_ltp_81857' and source['enabled'] is False,
              'unexpected source identity or active source')
    expected = copy.deepcopy(before)
    expected['sources'] = [s for s in expected['sources'] if s['id'] != SOURCE]
    expected['dashboard']['source_viewers'].pop(SOURCE)
    text = raw.decode()
    headers = list(re.finditer(r'(?m)^\[.*\][ \t]*$', text))
    removed = 0
    for i, header in enumerate(headers):
        if header.group() != '[[sources]]':
            continue
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        block = tomllib.loads(text[header.start():end])['sources'][0]
        if block.get('id') == SOURCE:
            n.require(block == source, 'nested source configuration requires review')
            text = text[:header.start()] + text[end:]
            removed += 1
            break
    text, count = re.subn(r'(?m)^' + SOURCE + r'[ \t]*=[^\n]*\n', '', text)
    n.require(removed == count == 1 and tomllib.loads(text) == expected, 'unrelated configuration edit')
    n.require(expected['accounts'][GROUP]['source_ids'] == LEGS, 'cross-spread source mapping changed')
    return text.encode()


def stopped_and_unbound():
    n.assert_stopped(Path.home() / SOURCE)
    for table in ['cta_account_strategy_bindings', 'cta_account_follows', 'cta_follow_publish_queue']:
        n.require(sql(f"SELECT count(*) FROM {table} WHERE source_id='{SOURCE}'") == '0',
                  'legacy source has execution bindings or pending publishes')


def history():
    result = {}
    for table in ['cta_order_sources', 'cta_source_symbols', 'cta_position_snapshots',
                  'cta_strategy_position_snapshots', 'cta_strategy_allocation_snapshots',
                  'cta_account_equity_daily']:
        row = "to_jsonb(t)-'updated_at'" if table == 'cta_order_sources' else 'to_jsonb(t)'
        result[table] = json.loads(sql(f"SELECT jsonb_build_object('count',count(*),'digest',md5(coalesce(string_agg(({row})::text,'' ORDER BY ({row})::text),''))) FROM {table} t WHERE source_id='{SOURCE}'"))
    n.require(result['cta_order_sources']['count'] == 1, 'legacy historical source missing')
    return result


def group_identity(dashboard):
    group = next(r for r in dashboard['accounts'] if r['source_id'] == GROUP)
    return {'account': group['account'], 'enabled': group['enabled'],
            'execution_sources': [r['source_id'] for r in group['execution_sources']]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['prepare', 'apply', 'verify'])
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    audit = args.audit
    os.umask(0o077)
    n.require(audit.parent == MANAGER / 'maintenance-backups'
              and audit.name.startswith('ltp_81857_manager_remove_'), 'unexpected audit path')
    stopped_and_unbound()
    if args.phase == 'prepare':
        n.require(not audit.exists(), 'audit already exists')
        audit.mkdir(mode=0o700)
        raw = CONFIG.read_bytes()
        write_private(audit / 'cta-manager.toml.before', raw)
        write_private(audit / 'cta-manager.toml.candidate', candidate(raw))
        save(audit / 'history.before.json', history())
        save(audit / 'processes.before.json', protected_processes())
        with session('lgq') as api:
            dashboard = api('/manager/api/dashboard')
            n.require(any(r['source_id'] == SOURCE and not r['enabled'] for r in dashboard['accounts']),
                      'expected disabled legacy entry')
            save(audit / 'dashboard.before.json', dashboard)
            save(audit / 'virtuals.before.json', [r['virtual_id'] for r in api('/manager/api/catalog/virtual-accounts')])
        pid = subprocess.check_output(['systemctl', '--user', 'show', SERVICE, '-p', 'MainPID', '--value'], text=True).strip()
        env = dict(os.environ)
        env.update(r.decode().split('=', 1) for r in Path('/proc', pid, 'environ').read_bytes().split(b'\0') if b'=' in r)
        checked = subprocess.run([str(MANAGER / 'bin/cta_web'), '--config',
                                  str(audit / 'cta-manager.toml.candidate'), '--check-allocations'],
                                 env=env, capture_output=True, timeout=45)
        n.require(checked.returncode == 0, 'config/allocation validation failed; sensitive details suppressed')
        print(json.dumps({'prepared': True, 'remove_source': SOURCE, 'keep_group': GROUP,
                          'keep_legs': LEGS, 'orders_submitted': 0}), flush=True)
        return
    desired = (audit / 'cta-manager.toml.candidate').read_bytes()
    if args.phase == 'apply':
        n.require(args.execute, 'apply requires --execute')
        original = (audit / 'cta-manager.toml.before').read_bytes()
        n.require(CONFIG.read_bytes() == original and candidate(original) == desired, 'live config changed')
        n.require(protected_processes() == json.loads((audit / 'processes.before.json').read_text()),
                  'protected processes changed before apply')
        try:
            install(desired)
            restart()
            deadline = time.monotonic() + 300
            next_progress = time.monotonic() + 20
            while True:
                try:
                    with session('lgq') as api:
                        n.require(api('/manager/api/health')['status'] == 'ok', 'Manager not ready')
                        dashboard = api('/manager/api/dashboard')
                        n.require(not any(r['source_id'] == SOURCE for r in dashboard['accounts']),
                                  'legacy entry still visible')
                    break
                except Exception:
                    n.require(time.monotonic() < deadline, 'Manager health timeout')
                    if time.monotonic() >= next_progress:
                        print('Waiting for Manager history cache and authenticated health.', flush=True)
                        next_progress = time.monotonic() + 20
                    time.sleep(1)
        except Exception:
            n.require(CONFIG.read_bytes() == desired, 'concurrent config edit; rollback halted')
            install(original)
            restart()
            raise RuntimeError('Activation failed; original configuration restored') from None
    n.require(CONFIG.read_bytes() == desired, 'unexpected active config')
    before_dashboard = json.loads((audit / 'dashboard.before.json').read_text())
    with session('lgq') as api:
        dashboard = api('/manager/api/dashboard')
        overview = api('/manager/api/account-overview')
        virtuals = [r['virtual_id'] for r in api('/manager/api/catalog/virtual-accounts')]
    n.require(not any(r['source_id'] == SOURCE for r in dashboard['accounts'] + overview['accounts']),
              'legacy account entry remains')
    n.require({r['source_id'] for r in dashboard['accounts']} ==
              {r['source_id'] for r in before_dashboard['accounts']} - {SOURCE}, 'unrelated account entry changed')
    n.require(group_identity(dashboard) == group_identity(before_dashboard), 'cross-spread identity changed')
    n.require(virtuals == json.loads((audit / 'virtuals.before.json').read_text()), 'virtual definitions changed')
    n.require(history() == json.loads((audit / 'history.before.json').read_text()), 'legacy history changed')
    processes = protected_processes()
    n.require(processes == json.loads((audit / 'processes.before.json').read_text()), 'protected process changed')
    stopped_and_unbound()
    pid = subprocess.check_output(['systemctl', '--user', 'show', SERVICE, '-p', 'MainPID', '--value'], text=True).strip()
    result = {'removed_source': SOURCE, 'removed_account': 'lgq_ltp_81857', 'manager_entry_count': 0,
              'preserved_group': group_identity(dashboard), 'history_preserved': True,
              'virtual_definitions_preserved': True, 'protected_processes_unchanged': len(processes),
              'orders_submitted': 0, 'manager_pid': int(pid), 'checked_at_ms': time.time_ns() // 1_000_000}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
