"""One-time Binance duplicate-key retirement; default is read-only."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path

import redis

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--execute', action='store_true')
parser.add_argument('--backup-dir', type=Path, required=True)
args = parser.parse_args()
client = redis.Redis(decode_responses=True, socket_timeout=5)
venues = ['binance-futures', 'binance-margin']
retire = [venue + ':amount-thresholds-1m' + suffix for venue in venues for suffix in ['', ':meta']]
keep = [venue + ':' + suffix for venue in venues for suffix in
        ['amount-thresholds', 'amount-thresholds:meta', 'factor-plan', 'factor-plan-1m']]
keys = keep + retire
with client.pipeline() as pipe:
    pipe.watch(*keys)
    before = {key: pipe.hgetall(key) for key in keys}
    for venue in venues:
        shared = before[venue + ':amount-thresholds']
        assert len(shared) == 43
        for symbol, value in before[venue + ':amount-thresholds-1m'].items():
            assert json.loads(value) == json.loads(shared[symbol]), (venue, symbol)
    report = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'execute': args.execute,
              'retire': {key: len(before[key]) for key in retire},
              'keep': {key: len(before[key]) for key in keep}}
    if args.execute:
        args.backup_dir.mkdir(parents=True, exist_ok=False)
        for name in ['tlen_config_server.py', 'publish_binance_order_size_thresholds.py']:
            original = Path('/home/ubuntu/tlen_config_shared/scripts') / name
            (args.backup_dir / name).write_bytes(original.read_bytes())
        with (args.backup_dir / 'before.json').open('x') as handle:
            os.chmod(handle.name, 0o600)
            json.dump({'report': report, 'hashes': before}, handle, indent=2)
            handle.flush()
            os.fsync(handle.fileno())
        pipe.multi()
        pipe.delete(*retire)
        pipe.execute()
        assert all(not client.exists(key) for key in retire)
        assert all(client.hgetall(key) == before[key] for key in keep)
        report['verified'] = True
        (args.backup_dir / 'applied.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2))
