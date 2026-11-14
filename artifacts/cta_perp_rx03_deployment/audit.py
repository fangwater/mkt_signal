from pathlib import Path
import datetime
import hashlib
import json
import os
import time
import urllib.request

home = Path.home()
env = home / 'binance-cta-perp-rx03'
now_ms = int(time.time() * 1000)
out = {'checked_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'processes': []}
for proc in Path('/proc').glob('[0-9]*'):
    try:
        if (proc / 'cwd').resolve() != env:
            continue
        masks = sorted({next(line.split(':', 1)[1].strip() for line in (task / 'status').read_text().splitlines()
                            if line.startswith('Cpus_allowed_list:')) for task in (proc / 'task').iterdir()})
        out['processes'].append({'pid': int(proc.name), 'comm': (proc / 'comm').read_text().strip(),
                                 'thread_cpu_masks': masks})
    except (OSError, StopIteration):
        pass
table = json.loads((env / 'config/cta_perp_symbol_factors.json').read_text())
status_path = env / 'run/cta_perp_status.json'
status = json.loads(status_path.read_text()) if status_path.exists() else {}
out['status'] = {k: status.get(k) for k in ('enabled', 'max_pos_u', 'publication_error', 'journal_healthy')}
out['status']['table_matches'] = status.get('symbol_factors') == table
observations = [status.get('strategies', {}).get(factor, {}).get(symbol, {})
                for symbol, row in table.items() for factor in row]
out['live_ready_pairs'] = sum(bool(row.get('score_ready')) and now_ms - row.get('model_ts_ms', 0) <= 120000
                             for row in observations)
out['live_min_model_ms'] = min((row.get('model_ts_ms', 0) for row in observations), default=0)
out['live_max_model_ms'] = max((row.get('model_ts_ms', 0) for row in observations), default=0)
latest = {}
for path in sorted((env / 'data/cta_perp/factor_history').glob('*.jsonl'))[-2:]:
    with path.open('rb') as handle:
        size = path.stat().st_size
        handle.seek(max(0, size - 2 * 1024 * 1024))
        if size > 2 * 1024 * 1024:
            handle.readline()
        for line in handle:
            try:
                row = json.loads(line)
                if row['model_ts_ms'] >= latest.get(row['symbol'], {}).get('model_ts_ms', 0):
                    latest[row['symbol']] = row
            except (json.JSONDecodeError, KeyError):
                pass
out['history_symbols'] = len(latest)
out['history_min_model_ms'] = min((r['model_ts_ms'] for r in latest.values()), default=0)
out['history_max_model_ms'] = max((r['model_ts_ms'] for r in latest.values()), default=0)
out['history_ready_pairs'] = sum(bool(latest.get(symbol, {}).get('inputs', {}).get(factor, {}).get('ready'))
                                for symbol, row in table.items() for factor in row)
out['history_ready_nq'] = sum(bool(row.get('nq', {}).get('ready')) for row in latest.values())
out['http'] = {}
for path, url in {
    'dashboard': 'http://127.0.0.1:10197/',
    'config': 'http://127.0.0.1:19187/',
    'signals': 'http://127.0.0.1:10197/signals/',
    'gateway': 'http://127.0.0.1:4191/cta/binance-cta-perp-rx03/',
}.items():
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            out['http'][path] = response.status
    except Exception as error:
        out['http'][path] = type(error).__name__
before_path = Path('/tmp/cta_perp_rx03_before.json')
if before_path.exists():
    before = json.loads(before_path.read_text())
    changed_files = []
    changed_processes = []
    for filename, expected in before['files'].items():
        path = Path(filename)
        if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            changed_files.append(filename)
    for expected in before['processes']:
        proc = Path('/proc') / str(expected['pid'])
        try:
            mask = next(line.split(':', 1)[1].strip() for line in (proc / 'status').read_text().splitlines()
                        if line.startswith('Cpus_allowed_list:'))
            if (proc / 'cwd').resolve().name != expected['env'] or (proc / 'stat').read_text().split()[21] != expected['start'] or mask != expected['affinity']:
                changed_processes.append(expected)
        except OSError:
            changed_processes.append(expected)
    out['protected'] = {'processes': len(before['processes']), 'configs': len(before['files']),
                         'changed_processes': changed_processes, 'changed_files': changed_files}
print(json.dumps(out))
