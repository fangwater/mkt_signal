"""Reconstruct the corrected absolute targets from factual signal entries."""
import copy
import hashlib
import json
import math
import pathlib
import time
import urllib.request

root = pathlib.Path('/home/ubuntu/binance-cta-spread-rx01')
with urllib.request.urlopen('http://127.0.0.1:19174/api/config', timeout=10) as response:
    config = json.load(response)
assert config['config']['enabled'] is False
assert config['config']['max_pos_u'] == 100
state = json.loads((root / 'run/cta_spread_position_flags.json').read_text())
status = json.loads((root / 'run/cta_spread_status.json').read_text())
assert status['enabled'] is False and status['journal_healthy'] is True
assert time.time_ns() // 1000 - status['updated_ts_us'] < 10_000_000
entries = {}
for path in sorted((root / 'data/cta_spread/signal_events').glob('*.jsonl')):
    with path.open() as lines:
        for line in lines:
            event = json.loads(line)
            if event['state'] != 'observed' or event['reason'] not in {'full_position_entry', 'factor_reversal'}:
                continue
            snapshot = event['snapshot']
            if snapshot.get('enabled') is not True:
                continue
            key = (snapshot['strategy_name'], event['symbol'])
            if event['event_ts_us'] > entries.get(key, {}).get('event_ts_us', -1):
                entries[key] = event
new_state = copy.deepcopy(state)
rows = []
for factor, symbols in state.items():
    for symbol, old in symbols.items():
        assert factor in status['symbol_factors'][symbol]
        mark = status['strategies'][factor][symbol]['mark_price']
        assert mark and math.isfinite(mark) and mark > 0
        entry = entries.get((factor, symbol))
        if entry is None:
            assert all(old[key] == 0 for key in old), (factor, symbol, 'missing historical entry')
            qty = 0.0
        else:
            snapshot = entry['snapshot']
            assert snapshot['max_pos_u'] == config['config']['max_pos_u']
            qty = snapshot['position_qty']
            assert math.isfinite(qty) and qty * snapshot['position_flag'] > 0
        new = new_state[factor][symbol]
        if old['flag'] == 0:
            new.update(qty=0.0, cash_qty=qty, perpetual_qty=-qty, entry_notional=0.0)
        else:
            assert math.isclose(old['qty'], qty, rel_tol=1e-14)
            assert old['entry_notional'] == 100
            new.update(cash_qty=old['qty'], perpetual_qty=0.0)
        assert math.isclose(new['qty'], new['cash_qty'] + new['perpetual_qty'], abs_tol=1e-12)
        rows.append({
            'factor': factor, 'symbol': symbol, 'before': old, 'after': new,
            'last_entry_ts_us': entry['event_ts_us'] if entry else None,
            'last_entry_qty': qty, 'mark': mark,
            'old_gross_usdt': (abs(old['cash_qty']) + abs(old['perpetual_qty'])) * mark,
            'new_gross_usdt': (abs(new['cash_qty']) + abs(new['perpetual_qty'])) * mark,
        })
assert len(rows) == 46
new_gross = sum(row['new_gross_usdt'] for row in rows)
assert new_gross < 10_000
print(json.dumps({
    'prepared_at_us': time.time_ns() // 1000, 'config_revision': config['revision'],
    'state_sha256': hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest(),
    'before': state, 'after': new_state, 'rows': rows,
    'summary': {'combinations': len(rows), 'entries_found': sum(row['last_entry_ts_us'] is not None for row in rows),
                'old_gross_usdt': sum(row['old_gross_usdt'] for row in rows), 'new_gross_usdt': new_gross},
}, indent=2))
