"""Start only RX01 environment wrappers and enable publication after checks."""
import argparse
import importlib.util
import json
import os
import subprocess
import time
import urllib.request
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('phase', choices=['observers', 'executor', 'enable'])
args = parser.parse_args()
root = Path.home() / 'binance-cta-spread-rx01'
exe = Path.home() / 'binance_exec_trade06'
stage = root / 'run/reenable_20261019'
spec = importlib.util.spec_from_file_location('ops_helpers', stage / 'ops_helpers.py')
ops = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ops)
opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def get(url):
    with opener.open(url, timeout=15) as response:
        return json.load(response)

def save(name, value):
    path = stage / name
    path.write_text(json.dumps(value, indent=2) + '\n')
    os.chmod(path, 0o600)

def run(base, script, comm=None):
    before = ops.live_pids(base)
    if comm:
        assert not any(p['comm'] == comm for p in before), 'component already running'
    log = stage / (args.phase + '_' + script + '.log')
    with log.open('wb') as output:
        os.chmod(log, 0o600)
        value = subprocess.run([str(base / 'scripts' / script)], cwd=base,
            env=dict(os.environ, PATH=str(Path.home() / '.local/bin') + ':' + os.environ['PATH']),
            stdout=output, stderr=subprocess.STDOUT, timeout=180)
    assert value.returncode == 0, script + ' failed; inspect host-local private log'
    print(json.dumps(dict(started=script, processes=ops.live_pids(base))), flush=True)

assert (stage / 'snapshots_stored.json').exists()
plan = json.loads((stage / 'plan.json').read_text())
cfg_url = 'http://127.0.0.1:19174/api/config'
config = get(cfg_url)
assert config['config'] == plan['config'], 'signal configuration changed since preparation'
assert config['config']['enabled'] is False
if args.phase == 'observers':
    assert not any(p['comm'] in {'trade_engine', 'exec-pre-trade', 'trade_signal'}
                   for p in ops.live_pids(exe))
    for script, comm in [('start_exec_persist_manager.sh', 'persist_manager'),
                         ('start_account_monitor.sh', 'rapidx_account_'),
                         ('start_exec_viz_server.sh', 'viz_server'),
                         ('start_exec_config_server.sh', None)]:
        run(exe, script, comm)
    get('http://127.0.0.1:10046/snapshot')
    catalog = get('http://127.0.0.1:18166/api/strategies')
    run(root, 'start_cta_spread_dashboard.sh')
    get('http://127.0.0.1:10186/api/snapshot')
    save('observers_started.json', dict(started_at_us=time.time_ns() // 1000,
        processes=ops.live_pids(exe), config_api_healthy=True, viz_api_healthy=True))
elif args.phase == 'executor':
    assert (stage / 'observers_started.json').exists()
    assert not any(p['comm'] == 'trade_signal' for p in ops.live_pids(exe))
    run(exe, 'start_exec_trade_engine.sh', 'trade_engine')
    run(exe, 'start_exec_pre_trade.sh', 'exec-pre-trade')
    save('executor_started.json', dict(started_at_us=time.time_ns() // 1000,
                                      processes=ops.live_pids(exe)))
elif args.phase == 'enable':
    assert (stage / 'executor_started.json').exists()
    processes = ops.live_pids(exe)
    assert all(any(p['comm'] == comm for p in processes)
               for comm in ['persist_manager', 'trade_engine', 'rapidx_account_', 'exec-pre-trade', 'viz_server'])
    snap = get('http://127.0.0.1:10046/snapshot')
    state = next(e for e in snap['entries'] if e['channel'] == 'exec_pre_trade_state')
    assert state['entry']['position_ready'], 'account position snapshot is not ready'
    status = json.loads((root / 'run/cta_spread_status.json').read_text())
    assert status['journal_healthy'] and not status['publication_error']
    before = get(cfg_url)
    assert before == config
    desired = dict(before['config'], enabled=True)
    request = urllib.request.Request(cfg_url, data=json.dumps(dict(config=desired,
        expected_revision=before['revision'])).encode(),
        headers={'Content-Type': 'application/json'}, method='PUT')
    with opener.open(request, timeout=15) as response:
        enabled = json.load(response)
    save('enabled.json', dict(enabled_at_us=time.time_ns() // 1000, config=enabled))
    print(json.dumps(dict(publication_enabled=True, max_pos_u=desired['max_pos_u'])))
