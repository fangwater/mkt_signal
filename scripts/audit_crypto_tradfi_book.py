#!/usr/bin/env python3
"""Independently replay a raw TradFi book day and explain invalid minute ends."""
import argparse
import csv
import gzip
import hashlib
import itertools
import json
from collections import Counter
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, required=True)
    p.add_argument('--symbol', required=True)
    p.add_argument('--day', required=True, help='YYYYMMDD')
    p.add_argument('--execute', action='store_true')
    args = p.parse_args()
    audit_path = args.root / '_build_20261026/_audit/days' / args.symbol / (args.day + '.json')
    receipt = json.loads(audit_path.read_text())
    base = args.root / 'baseline_data_1min'
    if not base.exists():
        base = args.root / '_build_20261026/baseline_data_1min'
    table = pq.read_table(base / 'binance-futures' / args.symbol / (args.day + '.parquet'))
    actual = {row['ts']: row for row in table.to_pylist()}
    start, end = min(actual), max(actual) + 60
    cursor = start
    book = {'bid': {}, 'ask': {}}
    initialized = False
    last_receive = None
    reasons = Counter()
    intervals = []
    snapshots = []
    checked = 0

    def emit(until):
        nonlocal cursor, checked
        while cursor < min(until, end):
            bids = sorted(book['bid'].items(), reverse=True)[:20]
            asks = sorted(book['ask'].items())[:20]
            bid = bids[0][0] if bids else None
            ask = asks[0][0] if asks else None
            reason = ('uninitialized' if not initialized else 'empty_side' if not bids or not asks
                      else 'crossed_or_locked' if bid >= ask else 'partial' if min(len(bids), len(asks)) < 20 else 'depth20')
            valid = reason in ('depth20', 'partial')
            row = actual[cursor]
            assert row['book_depth'] == (min(len(bids), len(asks)) if valid else 0), cursor
            assert row['source_quality_flags'] == (0 if reason == 'depth20' else 2 if reason == 'partial' else 1), cursor
            assert row['source_depth_ts_utc_us'] == last_receive, cursor
            expected, observed = [], []
            for side, levels in [('bid', bids), ('ask', asks)]:
                for i in range(20):
                    for j, suffix in enumerate(['p', 'v']):
                        expected.append(levels[i][j] if valid and i < len(levels) else np.nan)
                        value = row[f'{side}{i}{suffix}']
                        observed.append(np.nan if value is None else value)
            expected.append((bid + ask) / 2 if valid else np.nan)
            observed.append(np.nan if row['mid_price'] is None else row['mid_price'])
            np.testing.assert_allclose(observed, expected, rtol=0, atol=0, equal_nan=True)
            reasons[reason] += 1
            if not intervals or intervals[-1]['reason'] != reason:
                intervals.append(dict(start_ts=cursor, end_ts=cursor+60, reason=reason,
                                      first_best_bid=bid, first_best_ask=ask))
            else:
                intervals[-1]['end_ts'] = cursor+60
            checked += 1
            cursor += 60

    source = Path(receipt['book_source']['path'])
    with gzip.open(source, 'rt', newline='') as stream:
        rows = csv.DictReader(stream)
        for (_, receive, snapshot), group in itertools.groupby(rows, key=lambda r: (r['timestamp'], r['local_timestamp'], r['is_snapshot'])):
            receive = int(receive)
            emit(receive // 60_000_000 * 60)
            if snapshot == 'true':
                book = {'bid': {}, 'ask': {}}
                initialized = True
                snapshots.append(dict(receive_us=receive, rows=0, positive_bid_rows=0, positive_ask_rows=0))
            for row in group:
                assert row['symbol'] == args.symbol and row['exchange'] == 'binance-futures'
                side = book[row['side']]
                price, amount = float(row['price']), float(row['amount'])
                if snapshot == 'true':
                    snapshots[-1]['rows'] += 1
                    snapshots[-1]['positive_' + row['side'] + '_rows'] += amount > 0
                if amount:
                    side[price] = amount
                else:
                    side.pop(price, None)
            last_receive = receive
    emit(end)
    result = dict(symbol=args.symbol, day=args.day, source=receipt['book_source'],
                  source_sha256_matches=hashlib.sha256(source.read_bytes()).hexdigest() == receipt['book_source']['sha256'],
                  checked_minutes=checked, exact_book_fields_match=True,
                  reason_counts=dict(reasons), intervals=intervals, snapshot_events=snapshots)
    assert result['source_sha256_matches']
    print(json.dumps(result, indent=2))
    if args.execute:
        target = args.root / '_provenance/book_diagnostics' / (args.symbol + '_' + args.day + '.json')
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(result, indent=2) + '\n')


if __name__ == '__main__':
    main()
