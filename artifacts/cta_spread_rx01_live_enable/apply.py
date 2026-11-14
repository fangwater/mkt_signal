"""Apply the reviewed 100-USDT absolute state and enable live publication."""
import argparse
import datetime
import hashlib
import json
import math
import os
import pathlib
import shutil
import subprocess
import sys
import time
import urllib.request

args = argparse.ArgumentParser()
args.add_argument('--execute', action='store_true')
execute = args.parse_args().execute
root = pathlib.Path('/home/ubuntu/binance-cta-spread-rx01')
plan_path = root / '.enable_plan_20261014.upload.json'
plan = json.loads(plan_path.read_text())
state_path = root / 'run/cta_spread_position_flags.json'
status_path = root / 'run/cta_spread_status.json'
config_url = 'http://127.0.0.1:19174/api/config'


def get_config():
    with urllib.request.urlopen(config_url, timeout=10) as response:
        return json.load(response)


def state_matches(actual, expected):
    assert actual.keys() == expected.keys()
    for factor, rows in expected.items():
        assert actual[factor].keys() == rows.keys()
        for symbol, fields in rows.items():
            assert actual[factor][symbol].keys() == fields.keys()
            for key, value in fields.items():
                other = actual[factor][symbol][key]
                assert (value == other or
                        (key in {'qty', 'cash_qty', 'perpetual_qty'} and
                         abs(value - other) <= 2 * max(math.ulp(value), math.ulp(other))))


def pids():
    result = []
    for entry in pathlib.Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if (os.readlink(entry / 'cwd') == str(root) and
                    os.readlink(entry / 'exe').removesuffix(' (deleted)') == str(root / 'cta_spread_signal')):
                result.append(int(entry.name))
        except OSError:
            continue
    return result


config = get_config()
assert config['revision'] == plan['config_revision']
assert config['config']['enabled'] is False and config['config']['max_pos_u'] == 100
assert hashlib.sha256((root / 'cta_spread_signal').read_bytes()).hexdigest() == '4069055ff3a20582f727d8cecdc6a14af7d619af7ca12ed625bad765add330b4'
assert pids() == [46021]
state_matches(json.loads(state_path.read_text()), plan['before'])
if not execute:
    print(json.dumps({'dry_run': True, 'summary': plan['summary']}))
    sys.exit(0)

backup = root / 'run' / ('live_enable_' + datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
backup.mkdir(mode=0o700)
for path in [state_path, root / 'config/cta_spread.json', root / 'release.json']:
    shutil.copy2(path, backup / (path.name + '.before'))
shutil.copy2(plan_path, backup / 'plan.json')
print('Backed up signal state; stopping only Spread signal.', file=sys.stderr, flush=True)
with (backup / 'signal_restart.log').open('wb') as output:
    os.chmod(backup / 'signal_restart.log', 0o600)
    stopped = subprocess.run([str(root / 'scripts/stop_cta_spread_signal.sh')], cwd=root,
                             stdout=output, stderr=subprocess.STDOUT, timeout=180)
assert stopped.returncode == 0 and not pids(), 'signal did not stop'
assert get_config()['revision'] == plan['config_revision']
state_matches(json.loads(state_path.read_text()), plan['before'])
tmp = state_path.with_name('.cta_spread_position_flags.live-enable.tmp')
with tmp.open('w') as output:
    json.dump(plan['after'], output, indent=2, allow_nan=False)
    output.flush()
    os.fsync(output.fileno())
os.replace(tmp, state_path)
directory_fd = os.open(state_path.parent, os.O_DIRECTORY)
os.fsync(directory_fd)
os.close(directory_fd)
print('Loaded bounded absolute targets; starting Spread signal while disabled.', file=sys.stderr, flush=True)
started_at_us = time.time_ns() // 1000
with (backup / 'signal_restart.log').open('ab') as output:
    started = subprocess.run([str(root / 'scripts/start_cta_spread_signal.sh')], cwd=root,
                             stdout=output, stderr=subprocess.STDOUT, timeout=180)
assert started.returncode == 0
ready = False
for _ in range(90):
    status = json.loads(status_path.read_text())
    rows = [row for values in status['strategies'].values() for row in values.values()]
    if (len(pids()) == 1 and status['updated_ts_us'] > started_at_us and
            status['enabled'] is False and status['journal_healthy'] is True and
            len(rows) == 46 and all(row.get('score_ready') for row in rows)):
        ready = True
        break
    time.sleep(1)
assert ready, 'signal observations not ready; publication remains disabled'
state_matches(json.loads(state_path.read_text()), plan['after'])
config = get_config()
assert config['revision'] == plan['config_revision'] and config['config']['enabled'] is False
journal_path = root / 'data/cta_spread/signal_events' / (datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d') + '.jsonl')
journal_offset = journal_path.stat().st_size
enabled_at_us = time.time_ns() // 1000
config['config']['enabled'] = True
request = urllib.request.Request(config_url, data=json.dumps({
    'config': config['config'], 'expected_revision': config['revision'],
}).encode(), headers={'Content-Type': 'application/json'}, method='PUT')
with urllib.request.urlopen(request, timeout=10) as response:
    enabled_config = json.load(response)
print('Live publication enabled; checking all five strategy publications.', file=sys.stderr, flush=True)
published = set()
for _ in range(60):
    with journal_path.open() as journal:
        journal.seek(journal_offset)
        for line in journal:
            event = json.loads(line)
            if event['state'] == 'published' and event['event_ts_us'] >= enabled_at_us:
                published.add(event['snapshot']['strategy_name'])
    status = json.loads(status_path.read_text())
    if (set(plan['after']).issubset(published) and status['enabled'] is True and
            status['publication_error'] is None and status['journal_healthy'] is True):
        break
    time.sleep(1)
else:
    raise RuntimeError('live publication enabled but full delivery not yet verified')
release_path = root / 'release.json'
release = json.loads(release_path.read_text())
release.update(publication_enabled=True,
               publication_enabled_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
               absolute_state_rebuild_backup=str(backup))
tmp = root / '.release.json.live-enable.tmp'
tmp.write_text(json.dumps(release, indent=2) + '\n')
os.replace(tmp, release_path)
result = {'enabled_at_us': enabled_at_us, 'new_signal_pid': pids()[0],
          'backup': str(backup), 'publication_enabled': True,
          'published_strategies': sorted(published), 'summary': plan['summary']}
(backup / 'enabled.json').write_text(json.dumps(result, indent=2) + '\n')
plan_path.unlink()
print(json.dumps(result))
