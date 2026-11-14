#!/usr/bin/env python3
"""Read account-monitor equity facts, without modifying runtime or database."""
import json
import re
import time
from pathlib import Path

path = Path.home() / '.pmdaemon/logs/fr_am_gt_arb01-error.log'
start_ms = 1793355399705
targets = {
    'one_minute_before_adl': start_ms - 60000,
    'immediately_before_adl': start_ms,
    'one_minute_after_adl': start_ms + 60000,
    'one_hour_after_adl': start_ms + 3600000,
    'one_day_after_adl': start_ms + 86400000,
    'two_days_after_adl': start_ms + 2 * 86400000,
    'three_days_after_adl': start_ms + 3 * 86400000,
    'before_rehedge': 1793713512000,
    'latest': int(time.time() * 1000),
}
nearest = {}
event_rows = {}
low = high = None
count = 0
sample = None
with path.open('rb') as stream:
    for line in stream:
        if b'Gate AccountRisk:' not in line:
            continue
        match = re.search(rb'\bts=(\d+).*?actual_eq_usd=([-+\d.]+)', line)
        if not match:
            continue
        ts, equity = int(match[1]), float(match[2])
        row = {'ts_ms': ts, 'equity_usd': equity}
        count += 1
        sample = line.decode(errors='replace').strip()
        for label, target in targets.items():
            if ts <= target and (label not in nearest or ts > nearest[label]['ts_ms']):
                nearest[label] = row
        if start_ms - 30000 <= ts <= start_ms + 90000:
            event_rows[(ts, equity)] = row
        if ts >= start_ms:
            if low is None or equity < low['equity_usd']:
                low = row
            if high is None or equity > high['equity_usd']:
                high = row
print(json.dumps({'path': str(path), 'risk_line_count': count, 'sample': sample, 'targets': nearest,
                  'event_rows': sorted(event_rows.values(), key=lambda x: x['ts_ms']),
                  'since_adl_min': low, 'since_adl_max': high}), flush=True)
