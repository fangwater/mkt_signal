import datetime
import hashlib
import json
import os
from pathlib import Path
import time
import urllib.request

import redis

root = Path('/home/ubuntu/tlen_config_shared/run/order_size_202609_20261023')
backup = json.loads((root / 'before.json').read_text())
client = redis.Redis(decode_responses=True, socket_timeout=5)
result = {'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
          'applied_at': backup['created_at'], 'keys': {}, 'http': {},
          'other_threshold_hashes': {}, 'rx_tables': {}, 'rx_status': {},
          'processes': {}, 'trade_flow_reload': {}}
for key, expected in backup['after'].items():
    actual = client.hgetall(key)
    result['keys'][key] = {'count': len(actual), 'matches': actual == expected}
for venue in ['binance-futures', 'binance-margin']:
    for family in ['amount_thresholds', 'amount_thresholds_1m', 'factor_plan', 'factor_plan_1m']:
        url = 'http://127.0.0.1:6322/api/thresholds?venue=' + venue + '&config_type=' + family
        with urllib.request.urlopen(url, timeout=5) as response:
            body = json.load(response)
            item = {'status': response.status, 'count': body['count']}
            if family.startswith('amount_thresholds'):
                expected = {s: json.loads(v) for s, v in backup['after'][body['redis_key']].items()}
                item['matches'] = body['thresholds'] == expected
            result['http'][venue + '/' + family] = item
for key in sorted(client.scan_iter(match='*amount*threshold*')):
    if key not in backup['after']:
        result['other_threshold_hashes'][key] = hashlib.sha256(json.dumps(client.hgetall(key), sort_keys=True).encode()).hexdigest()
for env in ['binance-cta-spread-rx01', 'binance-cta-perp-rx02', 'binance-cta-perp-rx03', 'binance-cta-perp-rx04']:
    result['rx_tables'][env] = client.get(env + ':cta_symbol_factors')
    file = next((Path('/home/ubuntu') / env / 'run').glob('*status.json'))
    state = json.loads(file.read_text())
    result['rx_status'][env] = {'age_seconds': time.time() - file.stat().st_mtime,
                               'table_matches': state['symbol_factors'] == json.loads(result['rx_tables'][env])}
for proc in Path('/proc').glob('[0-9]*'):
    try:
        cwd = str((proc / 'cwd').resolve())
        if cwd.startswith('/home/ubuntu/') and cwd != '/home/ubuntu':
            stat = (proc / 'stat').read_text().rsplit(')', 1)[1].split()
            result['processes'][proc.name] = {'comm': (proc / 'comm').read_text().strip(), 'cwd': cwd, 'start_ticks': stat[19]}
        if cwd in ['/home/ubuntu/trade_flow_feature/binance-futures', '/home/ubuntu/trade_flow_feature/binance-margin']:
            log = Path(os.readlink(proc / 'fd/2'))
            with log.open('rb') as handle:
                handle.seek(max(0, log.stat().st_size - 500000))
                lines = handle.read().decode(errors='replace').splitlines()
            rows = [s for s in lines if 'symbols reloaded:' in s]
            result['trade_flow_reload'][Path(cwd).name] = {'pid': int(proc.name), 'latest_reload': rows[-1:]}
    except OSError:
        pass
print(json.dumps(result, indent=2))
