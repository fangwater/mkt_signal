#!/usr/bin/env python3
"""Read replay history and count readiness only for the requested RX02 pairs."""
import argparse
from collections import deque
import json
import math
from pathlib import Path


def analyze(root, table):
    counts, first, latest, windows, first_score, first_ready = {}, {}, {}, {}, {}, {}
    for path in sorted(root.glob('*.jsonl')):
        with path.open() as stream:
            for line in stream:
                row = json.loads(line)
                symbol, ts = row['symbol'], row['model_ts_ms']
                if symbol not in table:
                    continue
                first.setdefault(symbol, ts)
                counts[symbol] = counts.get(symbol, 0) + 1
                latest[symbol] = row
                for factor in table[symbol]:
                    key = symbol + '/' + factor
                    value = row['inputs'][factor]
                    score = value.get('score')
                    valid = isinstance(score, (int, float)) and math.isfinite(score)
                    windows.setdefault(key, deque(maxlen=1440)).append(valid)
                    if valid:
                        first_score.setdefault(key, ts)
                    if value['ready']:
                        first_ready.setdefault(key, ts)
    pending = []
    ready_symbols = []
    for symbol, factors in table.items():
        row = latest.get(symbol)
        all_ready = row is not None
        for factor in factors:
            key = symbol + '/' + factor
            value = row['inputs'][factor] if row else {}
            if not value.get('ready', False):
                all_ready = False
                pending.append({'symbol': symbol, 'factor': factor,
                                'first_score_ts_ms': first_score.get(key),
                                'last_score': value.get('score'),
                                'finite_scores_in_latest_1440_rows': sum(windows.get(key, [])),
                                'minimum_additional_valid_samples': 1440 - sum(windows.get(key, []))})
        if all_ready:
            ready_symbols.append(symbol)
    total = sum(map(len, table.values()))
    return {'ready_pairs': total - len(pending), 'total_pairs': total,
            'ready_symbols': ready_symbols, 'pending_pairs': pending,
            'first_ready_ts_ms': first_ready,
            'symbols': {symbol: {'rows': counts[symbol], 'first_ts_ms': first[symbol],
                                'latest_ts_ms': row['model_ts_ms'], 'nq_ready': row['nq']['ready']}
                        for symbol, row in latest.items()},
            'remaining_sample_note': 'Lower bound from valid scores in the rolling window; gaps or later invalid scores can extend warm-up.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('history', type=Path)
    parser.add_argument('table', type=Path)
    args = parser.parse_args()
    print(json.dumps(analyze(args.history, json.loads(args.table.read_text())), indent=2))
