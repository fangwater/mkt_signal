"""Retire the remaining RX03/RX04 Manager entries without deleting history.

Run prepare first. Apply requires --execute, retains private backups and only
restarts Manager's systemd service. It never submits exchange orders.
Place beside the existing scoped clear_positions/manager_ops helpers remotely.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import time
import tomllib

from clear_positions import SCOPES, exchange_snapshot, inventory, save
from manager_ops import session, sql
import neutralize_rapidx_binance as n

MANAGER = Path.home() / 'crypto_cta_manager'
CONFIG = MANAGER / 'config/cta-manager.toml'
SERVICE = 'crypto-cta-manager-web.service'
SOURCES = [s[1] for s in SCOPES]
VIRTUALS = [s[2] for s in SCOPES]


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def write_private(path, raw):
    with path.open('xb') as f:
        path.chmod(0o600)
        f.write(raw)
        f.flush()
        os.fsync(f.fileno())


def candidate(raw):
    text = raw.decode()
    before = tomllib.loads(text)
    expected = copy.deepcopy(before)
    expected['sources'] = [s for s in expected['sources'] if s['id'] not in SOURCES]
    n.require(len(before['sources']) - len(expected['sources']) == 2, 'unexpected source scope')
    headers = list(re.finditer(r'(?m)^\[.*\][ \t]*$', text))
    removals = []
    for i, header in enumerate(headers):
        if header.group() != '[[sources]]':
            continue
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        block = tomllib.loads(text[header.start():end])['sources'][0]
        if block.get('id') in SOURCES:
            n.require(block == next(s for s in before['sources'] if s['id'] == block['id']),
                      'nested source block requires explicit review')
            removals.append((header.start(), end))
    n.require(len(removals) == 2, 'expected two source blocks')
    for start, end in reversed(removals):
        text = text[:start] + text[end:]
    for source in SOURCES:
        expected['dashboard']['source_viewers'].pop(source)
        pattern = r'(?m)^' + re.escape(source) + r'[ \t]*=[^\n]*\n'
        text, count = re.subn(pattern, '', text)
        n.require(count == 1, 'expected one viewer cap per retired source')
    exceptions = expected['funding']['rapidx_trading_ip_sources']
    expected['funding']['rapidx_trading_ip_sources'] = [s for s in exceptions if s not in SOURCES]
    pattern = r'(?m)^rapidx_trading_ip_sources[ \t]*=[ \t]*\[[^\]]*\]'
    replacement = 'rapidx_trading_ip_sources = ' + json.dumps(expected['funding']['rapidx_trading_ip_sources'])
    text, count = re.subn(pattern, replacement, text)
    n.require(count == 1 and tomllib.loads(text) == expected, 'unexpected configuration changes')
    return text.encode()


def protected_processes():
    names = {'trade_engine', 'exec-pre-trade', 'pre_trade', 'trade_signal', 'persist_manager',
             'viz_server', 'rapidx_account_', 'binance_account', 'okex_account_mo',
             'gate_account_mo', 'bybit_account_m', 'bitget_account_', 'spread_pbs', 'depth_pub'}
    result = {}
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            comm = (proc / 'comm').read_text().strip()
            if comm in names or comm.startswith(('cta_perp_', 'cta-factor-', 'cta_signal')):
                result[proc.name] = {'comm': comm, 'cwd': os.readlink(proc / 'cwd'),
                                     'start_ticks': (proc / 'stat').read_text().split(') ', 1)[1].split()[19]}
        except OSError:
            pass
    return result


def history():
    ids = "'binance_exec_trade07','binance_exec_rx04_5m05'"
    result = {}
    for table in ['cta_order_sources', 'cta_position_snapshots',
                  'cta_strategy_position_snapshots', 'cta_strategy_allocation_snapshots']:
        result[table] = json.loads(sql(f"SELECT jsonb_build_object('count',count(*),'digest',md5(coalesce(string_agg(to_jsonb(t)::text,'' ORDER BY to_jsonb(t)::text),''))) FROM {table} t WHERE source_id IN ({ids})"))
    return result


def flat():
    state = inventory()
    n.require(not state['bindings'] and not state['follows'] and not state['pending'],
              'execution bindings or pending publishes remain')
    for signal, source, *_ in SCOPES:
        n.assert_stopped(Path.home() / source)
        n.require(json.loads((Path.home() / signal / 'config/cta_perp.json').read_text())['enabled'] is False,
                  'signal enabled')
        snap = exchange_snapshot(source)
        n.require(not snap['nonzero_positions'] and snap['open_orders'] == snap['algo_orders'] == 0,
                  'exchange not flat')
    follows = json.loads(sql("SELECT coalesce(jsonb_agg(to_jsonb(f)), '[]'::jsonb) FROM cta_account_follows f WHERE virtual_id IN ('virtual04','virtual05')"))
    n.require(not follows, 'virtual account still followed')


def install(raw):
    path = CONFIG.with_suffix('.retire-next')
    n.require(not path.exists(), 'staged config already exists')
    write_private(path, raw)
    path.chmod(stat.S_IMODE(CONFIG.stat().st_mode))
    os.replace(path, CONFIG)


def restart():
    subprocess.run(['systemctl', '--user', 'restart', SERVICE], check=True, timeout=45,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('phase', choices=['prepare', 'apply', 'verify'])
    parser.add_argument('--audit', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    audit = args.audit
    os.umask(0o077)
    n.require(audit.parent == MANAGER / 'maintenance-backups'
              and audit.name.startswith('rx03_rx04_manager_entries_'), 'unexpected audit path')
    if args.phase == 'prepare':
        n.require(not audit.exists(), 'audit already exists')
        audit.mkdir(mode=0o700)
        flat()
        raw = CONFIG.read_bytes()
        write_private(audit / 'cta-manager.toml.before', raw)
        write_private(audit / 'cta-manager.toml.candidate', candidate(raw))
        save(audit / 'processes.before.json', protected_processes())
        save(audit / 'history.before.json', history())
        with session('lgq') as api:
            rows = api('/manager/api/catalog/virtual-accounts')
            targets = [r for r in rows if r['virtual_id'] in VIRTUALS]
            n.require(len(targets) == 2 and all(r['can_configure'] and not r['followers'] for r in targets),
                      'unexpected virtual ownership or followers')
            save(audit / 'virtuals.before.json', targets)
            save(audit / 'dashboard.before.json', api('/manager/api/dashboard'))
        for table in ['cta_virtual_accounts', 'cta_virtual_account_bindings', 'cta_virtual_account_managers']:
            rows = json.loads(sql(f"SELECT coalesce(jsonb_agg(to_jsonb(t)), '[]'::jsonb) FROM {table} t WHERE virtual_id IN ('virtual04','virtual05')"))
            save(audit / (table + '.before.json'), rows)
        pid = subprocess.check_output(['systemctl', '--user', 'show', SERVICE, '-p', 'MainPID', '--value'], text=True).strip()
        env = dict(os.environ)
        env.update(r.decode().split('=', 1) for r in Path('/proc', pid, 'environ').read_bytes().split(b'\0') if b'=' in r)
        checked = subprocess.run([str(MANAGER / 'bin/cta_web'), '--config',
                                  str(audit / 'cta-manager.toml.candidate'), '--check-allocations'],
                                 env=env, capture_output=True, timeout=45)
        n.require(checked.returncode == 0, 'candidate validation failed; credential-bearing diagnostics suppressed')
        print(json.dumps({'prepared': True, 'remove_sources': SOURCES, 'remove_virtuals': VIRTUALS,
                          'configuration_and_immutable_allocations_valid': True}), flush=True)
        return
    if args.phase == 'apply':
        n.require(args.execute, 'apply requires --execute')
        flat()
        original = (audit / 'cta-manager.toml.before').read_bytes()
        desired = (audit / 'cta-manager.toml.candidate').read_bytes()
        n.require(CONFIG.read_bytes() == original and candidate(original) == desired, 'configuration changed')
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
                        health = api('/manager/api/health')
                        dash = api('/manager/api/dashboard')
                        n.require(health['status'] == 'ok', 'Manager not ready')
                        n.require(not any(r['source_id'] in SOURCES for r in dash['accounts']), 'source still visible')
                    break
                except Exception:
                    n.require(time.monotonic() < deadline, 'Manager health check timed out')
                    if time.monotonic() >= next_progress:
                        print('Waiting for Manager history cache and authenticated HTTP health.', flush=True)
                        next_progress = time.monotonic() + 20
                    time.sleep(1)
        except Exception:
            n.require(CONFIG.read_bytes() == desired, 'concurrent config change; rollback halted')
            install(original)
            restart()
            raise RuntimeError('Manager activation failed; original configuration restored') from None
        print('Manager source entries removed; service healthy.', flush=True)
        with session('lgq') as api:
            for virtual in VIRTUALS:
                result = api('/manager/api/catalog/virtual-accounts/' + virtual, None, 'DELETE')
                save(audit / (virtual + '.delete-receipt.json'), result)
                n.require(result['status'] == 204, 'virtual removal failed')
    n.require(CONFIG.read_bytes() == (audit / 'cta-manager.toml.candidate').read_bytes(), 'unexpected live config')
    with session('lgq') as api:
        dashboard = api('/manager/api/dashboard')
        overview = api('/manager/api/account-overview')
        virtuals = api('/manager/api/catalog/virtual-accounts')
        n.require(not any(r['source_id'] in SOURCES for r in dashboard['accounts']), 'dashboard entry remains')
        n.require(not any(r['source_id'] in SOURCES for r in overview['accounts']), 'overview entry remains')
        n.require(not any(r['virtual_id'] in VIRTUALS for r in virtuals), 'virtual entry remains')
    before_history = json.loads((audit / 'history.before.json').read_text())
    after_history = history()
    # register_sources refreshes updated_at when the original config is loaded
    # during rollback. The source registry is mutable metadata, not a snapshot.
    for table in before_history:
        if table != 'cta_order_sources':
            n.require(after_history[table] == before_history[table], 'historical snapshots changed')
    n.require(after_history['cta_order_sources']['count'] == before_history['cta_order_sources']['count'] == 2,
              'historical source registry row removed')
    registry = json.loads(sql("SELECT jsonb_agg(jsonb_build_object('source_id',source_id,'venue',venue_label,'rocksdb_path',rocksdb_path)) FROM cta_order_sources WHERE source_id IN ('binance_exec_trade07','binance_exec_rx04_5m05')"))
    original_sources = {s['id']: s for s in tomllib.loads((audit / 'cta-manager.toml.before').read_text())['sources']}
    for row in registry:
        expected = original_sources[row['source_id']]
        n.require(row['venue'] == expected['venue'] and row['rocksdb_path'] == expected['rocksdb_path'],
                  'historical source identity changed')
    save(audit / 'history.after.json', after_history)
    processes = protected_processes()
    n.require(processes == json.loads((audit / 'processes.before.json').read_text()), 'protected process changed')
    flat()
    result = {'removed_sources': SOURCES, 'removed_virtuals': VIRTUALS,
              'dashboard_and_overview_entries': 0, 'virtual_entries': 0,
              'history_preserved': True, 'historical_source_rows_preserved': len(registry),
              'protected_processes_unchanged': len(processes),
              'positions': 0, 'open_orders': 0, 'algo_orders': 0,
              'checked_at_ms': time.time_ns() // 1_000_000, 'config_sha256': digest(CONFIG.read_bytes())}
    save(audit / 'verified.json', result)
    print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
