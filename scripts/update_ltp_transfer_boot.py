#!/usr/bin/env python3
"""Update only this environment's PM2 reboot entry; preserve every other entry."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stopped', action='store_true')
    args = parser.parse_args()
    base = Path(__file__).resolve().parents[1]
    dump = Path.home()/'.pm2/dump.pm2'
    original = dump.read_bytes() if dump.exists() else b'[]'
    entries = json.loads(original)
    others = [p for p in entries if p.get('name') != 'ltp-transfer-server']
    if not args.stopped:
        live = json.loads(subprocess.check_output(['pm2', 'jlist'], text=True))
        candidates = [p['pm2_env'] for p in live if p.get('name') == 'ltp-transfer-server']
        assert len(candidates) == 1 and candidates[0]['namespace'] == 'ltp_transfer_server'
        current = candidates[0]
        for environment in [current, current.get('env', {})]:
            assert not any(environment.get(k) for k in ['LTP_TRANSFER_API_KEY', 'LTP_TRANSFER_API_SECRET'])
        others.append(current)
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S.%fZ')
    backup = base/'run'/f'boot-registry-{stamp}'
    backup.mkdir(parents=True, mode=0o700)
    saved = backup/'dump.pm2'
    saved.write_bytes(original)
    saved.chmod(0o600)
    dump.parent.mkdir(exist_ok=True, mode=0o700)
    descriptor, temporary = tempfile.mkstemp(prefix='.ltp-dump-', dir=dump.parent)
    try:
        with os.fdopen(descriptor, 'w') as file:
            json.dump(others, file, indent=2)
            file.flush()
            os.fsync(file.fileno())
        assert (dump.read_bytes() if dump.exists() else b'[]') == original, 'boot registry changed concurrently'
        os.replace(temporary, dump)
        directory = os.open(dump.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if Path(temporary).exists():
            Path(temporary).unlink()
    assert [p for p in json.loads(dump.read_bytes()) if p.get('name') != 'ltp-transfer-server'] == [p for p in entries if p.get('name') != 'ltp-transfer-server']
    print('[INFO] updated only the LTP transfer reboot entry')


if __name__ == '__main__':
    main()
