import collections
import datetime
import hashlib
import json
import pathlib
import socket
import sys
import time
import urllib.request

sys.path.insert(0, '/tmp')
from cta_perp_rx03_activation import EXEC, SIGNAL, STAGE, ROOT, ORIGIN, env_for, live_pids, session, signal_snapshot, sql, save

result = {'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'processes': {}}
for env in [EXEC, SIGNAL]:
    processes = live_pids(env)
    assert len(processes) == (6 if env == EXEC else 4), processes
    for row in processes:
        proc = pathlib.Path('/proc') / str(row['pid'])
        masks = sorted({next(line.split(':', 1)[1].strip() for line in task.joinpath('status').read_text().splitlines() if line.startswith('Cpus_allowed_list:')) for task in proc.joinpath('task').iterdir()})
        row['thread_cpu_masks'] = masks
        assert masks == (['19'] if row['comm'] == 'cta_perp_factor' else ['0-5,48-95'])
        if row['comm'] == 'cta_perp_signal':
            keys = [x.split(b'=', 1)[0] for x in proc.joinpath('environ').read_bytes().split(b'\0') if b'=' in x]
            assert b'CTA_PUBLISH_TOKEN' in keys and not any(k.startswith(b'LTP_') for k in keys)
    result['processes'][env.name] = processes
assert {r['pid'] for r in result['processes'][SIGNAL.name] if r['comm'] != 'cta_perp_signal'} == {2038356, 2049101, 2054156}

manifest = json.loads((STAGE / 'release.json').read_text())
assert all(hashlib.sha256((EXEC / name).read_bytes()).hexdigest() == digest for name, digest in manifest['files'].items())
result['release_hashes_verified'] = len(manifest['files'])
result['http'] = {}
with session('lgq') as api:
    for path in ['/exec_trade07/', '/exec_trade07/config/', '/exec_trade07/snapshot']:
        api(path)
        result['http'][path] = 200
    ours = api('/manager/api/catalog/accounts/binance_exec_trade07')
    ref = api('/manager/api/catalog/accounts/binance_exec_trade05')
    fee_keys = ['maker_fee_rate', 'taker_fee_rate', 'theoretical_twap_fee_rate']
    assert all(ours[k] == ref[k] for k in fee_keys)
    result['fees'] = {k: ours[k] for k in fee_keys}
    result['configuration'] = ours['configuration']
    assert len(ours['bindings']) == 5 and not ours['pending_publishes']
    virtual = next(a for a in api('/manager/api/catalog/virtual-accounts') if a['virtual_id'] == 'virtual04')
    assert virtual['can_configure'] and len(virtual['followers']) == 1
    result['lgq_access_verified'] = True

for path in ['/cta/binance-cta-perp-rx03/', '/cta/binance-cta-perp-rx03/config/', '/cta/binance-cta-perp-rx03/signals/']:
    with urllib.request.urlopen(ORIGIN + path, timeout=20) as response:
        result['http'][path] = response.status
        assert response.status == 200
s = signal_snapshot()
assert s['healthy'] and s['status']['enabled'] and s['status']['publication_error'] is None
result['signal_healthy'] = True
with urllib.request.urlopen(ORIGIN + '/cta/binance-cta-perp-rx03/api/signal-panel', timeout=30) as response:
    panel = json.load(response)
assert panel['trading_enabled'] and panel['simulation']['ready'] and panel['simulation']['error'] is None
result['signal_panel_trading_enabled'] = True

counts = collections.Counter()
published = set()
for path in (SIGNAL / 'data/cta_perp/signal_events').glob('*.jsonl'):
    for line in path.read_text().splitlines():
        row = json.loads(line)
        counts[row['state']] += 1
        if row['state'] == 'published':
            assert row['publication']['error'] is None
            published.add((row['snapshot']['strategy_name'], row['symbol']))
assert len(published) == 34
result['journal_states'] = dict(counts)
result['published_pairs'] = len(published)

errors = []
for path in (ROOT / '.pmdaemon/logs').glob('*trade07*-error.log'):
    for line in path.read_text(errors='replace').splitlines():
        if ' ERROR ' in line or 'panicked at' in line or 'login failed' in line.lower():
            errors.append({'file': path.name, 'message': line[:600]})
values = env_for(EXEC)
for row in errors:
    for key, value in values.items():
        if value and any(word in key for word in ['KEY', 'SECRET', 'PORTFOLIO', 'TOKEN']):
            row['message'] = row['message'].replace(value, '<redacted>')
result['exec_errors'] = errors
assert not errors, errors
result['temporary_auth_sessions'] = int(sql("SELECT count(*) FROM cta_auth_sessions WHERE created_at > '2026-10-14 07:40:00+00' AND expires_at-created_at < interval '31 minutes';"))
save(STAGE / 'health.json', result)
print(json.dumps(result))
