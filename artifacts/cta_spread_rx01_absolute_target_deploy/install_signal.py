"""Install the verified signal artifact and restart only its env-local wrapper."""

import datetime
import hashlib
import json
import math
import os
import pathlib
import shutil
import subprocess
import time
import urllib.request

root = pathlib.Path('/home/ubuntu/binance-cta-spread-rx01')
release_id = '20261014T035311Z'
upload = root / f'.cta_spread_signal.{release_id}.upload'
manifest_upload = root / f'.cta_spread_signal.{release_id}.manifest.upload'
manifest = json.loads(manifest_upload.read_text())
binary = root / 'cta_spread_signal'
config = root / 'config/cta_spread.json'
positions = root / 'run/cta_spread_position_flags.json'
status_path = root / 'run/cta_spread_status.json'


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def assert_disabled():
    assert json.loads(config.read_text())['enabled'] is False, 'disk config enabled'
    with urllib.request.urlopen('http://127.0.0.1:19174/api/config', timeout=10) as response:
        assert json.load(response)['config']['enabled'] is False, 'API config enabled'


def signal_pids():
    result = []
    for entry in pathlib.Path('/proc').iterdir():
        if not entry.name.isdigit():
            continue
        try:
            if (os.readlink(entry / 'cwd') == str(root)
                    and os.readlink(entry / 'exe').removesuffix(' (deleted)') == str(binary)):
                result.append(int(entry.name))
        except OSError:
            continue
    return result


assert_disabled()
assert manifest['release_id'] == release_id
backup = root / 'run' / f'absolute_target_{release_id}'
already_running = False
if sha256(binary) == manifest['binary_sha256']:
    # Resume after a wrapper timeout without overwriting the original backup.
    assert sha256(backup / 'cta_spread_signal.before') == manifest['previous_binary_sha256']
    assert json.loads((backup / 'manifest.json').read_text()) == manifest
    current_pids = signal_pids()
    if len(current_pids) == 1 and current_pids[0] != manifest['old_signal_pid']:
        assert sha256(pathlib.Path(f'/proc/{current_pids[0]}/exe')) == manifest['binary_sha256']
        already_running = True
    else:
        assert current_pids in ([], [manifest['old_signal_pid']]), 'unexpected signal process'
else:
    assert sha256(upload) == manifest['binary_sha256'], 'uploaded binary checksum mismatch'
    assert sha256(binary) == manifest['previous_binary_sha256'], 'live binary changed'
    assert signal_pids() == [manifest['old_signal_pid']], 'live signal PID changed'
    backup.mkdir(mode=0o700)
    for source in [binary, config, positions, root / 'release.json']:
        if source.exists():
            shutil.copy2(source, backup / (source.name + '.before'))
    shutil.copy2(manifest_upload, backup / 'manifest.json')
    os.chmod(upload, 0o755)
    assert_disabled()
    os.replace(upload, binary)
restart_at_us = time.time_ns() // 1000
if already_running:
    restart_at_us -= 10_000_000
else:
    with (backup / 'restart.log').open('ab') as output:
        os.chmod(backup / 'restart.log', 0o600)
        restart = subprocess.run(
            [str(root / 'scripts/start_cta_spread_signal.sh')], cwd=root,
            stdout=output, stderr=subprocess.STDOUT, timeout=180,
        )
    assert restart.returncode == 0, f'env-local restart failed; see {backup}/restart.log'
healthy = False
for _ in range(40):
    pids = signal_pids()
    status = json.loads(status_path.read_text())
    if (len(pids) == 1 and pids[0] != manifest['old_signal_pid']
            and status['updated_ts_us'] > restart_at_us
            and status['enabled'] is False and status['journal_healthy'] is True):
        healthy = True
        break
    time.sleep(0.5)
assert healthy, f'new signal not healthy; old binary retained at {backup}'
assert_disabled()
old_positions = json.loads((backup / (positions.name + '.before')).read_text())
new_positions = json.loads(positions.read_text())
assert old_positions.keys() == new_positions.keys()
rounding_fields = 0
max_rounding_delta = 0.0
for factor, rows in old_positions.items():
    assert rows.keys() == new_positions[factor].keys()
    for symbol, old_row in rows.items():
        new_row = new_positions[factor][symbol]
        assert old_row.keys() == new_row.keys()
        for key, old in old_row.items():
            new = new_row[key]
            if old == new:
                continue
            # Rust JSON decoding/encoding may round existing f64 targets by one
            # ULP. Reject structural changes, flag changes, or any larger drift.
            assert key in {'cash_qty', 'perpetual_qty', 'qty'}
            assert abs(new - old) <= max(math.ulp(old), math.ulp(new))
            rounding_fields += 1
            max_rounding_delta = max(max_rounding_delta, abs(new - old))
release_path = root / 'release.json'
release = json.loads(release_path.read_text()) if release_path.exists() else {}
release.update({
    'signal_commit': manifest['base_commit'],
    'signal_patch_sha256': manifest['source_patch_sha256'],
    'signal_binary_sha256': manifest['binary_sha256'],
    'signal_release_id': release_id,
    'signal_deployed_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
    'publication_enabled': False,
})
release_tmp = root / '.release.json.signal-install'
release_tmp.write_text(json.dumps(release, indent=2) + '\n')
os.replace(release_tmp, release_path)
manifest_upload.unlink()
result = {
    'release_id': release_id, 'new_pid': pids[0],
    'binary_sha256': sha256(binary), 'publication_enabled': False,
    'backup': str(backup), 'status_updated_ts_us': status['updated_ts_us'],
    'position_targets_unchanged': True,
    'position_state_rounding_fields': rounding_fields,
    'max_position_rounding_delta': max_rounding_delta,
}
(backup / 'installed.json').write_text(json.dumps(result, indent=2) + '\n')
print(json.dumps(result))
