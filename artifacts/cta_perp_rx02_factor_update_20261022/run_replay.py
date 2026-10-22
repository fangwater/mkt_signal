#!/usr/bin/env python3
"""Isolated Kafka audit: no trading credentials, signal process or IPC output."""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

ROOT = Path(__file__).resolve().parent
LIVE = Path('/home/ubuntu/binance-cta-perp-rx02')


def snapshot():
    files = ['cta_perp_factor_model_1m_pub', 'cta_perp_signal',
             'config/cta_perp.json', 'config/cta_perp_symbol_factors.json',
             'config/cta_perp_factor_model_1m_pub.toml']
    hashes = {name: hashlib.sha256((LIVE / name).read_bytes()).hexdigest() for name in files}
    processes = {}
    for directory in Path('/proc').iterdir():
        if not directory.name.isdigit():
            continue
        try:
            args = (directory / 'cmdline').read_bytes().split(b'\0')
            if not any(arg.startswith(str(LIVE).encode() + b'/') for arg in args[:2]):
                continue
            stat = (directory / 'stat').read_text().rsplit(')', 1)[1].split()
            processes[directory.name] = {'start_ticks': stat[19]}
        except (OSError, ValueError, IndexError):
            continue
    return {'hashes': hashes, 'processes': processes}


def inspect_history():
    table = json.loads((ROOT / 'cta_perp_symbol_factors.json').read_text())
    first, latest, counts, first_ready = {}, {}, {}, {}
    for path in sorted((ROOT / 'history').glob('*.jsonl')):
        for line in path.open():
            row = json.loads(line)
            symbol, ts = row['symbol'], row['model_ts_ms']
            first[symbol] = min(first.get(symbol, ts), ts)
            latest[symbol] = row
            counts[symbol] = counts.get(symbol, 0) + 1
            for factor in table.get(symbol, {}):
                if row['inputs'][factor]['ready']:
                    first_ready.setdefault(symbol + '/' + factor, ts)
    symbols = {}
    missing = []
    for symbol, factors in table.items():
        row = latest.get(symbol)
        if row is None:
            missing.extend(symbol + '/' + f for f in factors)
            continue
        pending = [factor for factor in factors if not row['inputs'][factor]['ready']]
        missing.extend(symbol + '/' + f for f in pending)
        symbols[symbol] = {
            'first_ts_ms': first[symbol], 'latest_ts_ms': row['model_ts_ms'],
            'rows': counts[symbol], 'pending_factors': pending,
            'nq_ready': row['nq']['ready'],
        }
    return {'symbols': symbols, 'first_ready': first_ready,
            'ready_pairs': sum(map(len, table.values())) - len(missing),
            'total_pairs': sum(map(len, table.values())), 'pending_pairs': missing}


def main():
    os.chdir(ROOT)
    if (ROOT / 'started.json').exists():
        raise SystemExit('audit already started; use a fresh directory')
    before = snapshot()
    started = time.time()
    (ROOT / 'started.json').write_text(json.dumps({'started_at_ms': int(started * 1000), 'before': before}, indent=2))
    # The public Kafka consumer needs no host credentials. Keep the inherited
    # environment out, and always force replay-only regardless of caller flags.
    env = {'PATH': '/usr/local/bin:/usr/bin:/bin', 'RUST_LOG': 'info',
           'IPC_NAMESPACE': 'rx02_factor_replay_audit',
           'CTA_PERP_MODEL_SCOPE': 'rx02_factor_replay_audit'}
    command = [str(ROOT / 'cta_perp_factor_model_1m_pub'), '--config',
               str(ROOT / 'cta_perp_factor_model_1m_pub.toml'), '--venue',
               'binance-futures', '--history-path', str(ROOT / 'history'),
               '--replay-only', '--core', '18']
    os.nice(19)
    with (ROOT / 'replay.log').open('w') as log:
        try:
            result = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=3700)
            code = result.returncode
        except subprocess.TimeoutExpired:
            code = 124
    after = snapshot()
    report = {'started_at_ms': int(started * 1000), 'completed_at_ms': int(time.time() * 1000),
              'exit_code': code, 'replay_only': True, 'live_unchanged': before == after,
              'before': before, 'after': after, 'history': inspect_history()}
    (ROOT / 'result.json').write_text(json.dumps(report, indent=2) + '\n')
    raise SystemExit(code)


if __name__ == '__main__':
    main()
