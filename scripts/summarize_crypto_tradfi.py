#!/usr/bin/env python3
"""Summarize published TradFi source quality and per-factor availability."""
import argparse
import csv
import datetime
import hashlib
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    root = args.root
    provenance = root / '_provenance'
    delivery = json.loads((provenance / 'delivery_20261026.json').read_text())
    assert delivery['status'] == 'published'
    symbols = delivery['request']['symbols']
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    audit = root / '_build_20261026/_audit'
    quality_rows = []
    totals = {}
    for symbol in symbols:
        marker = json.loads((audit / f'baseline_{symbol}.json').read_text())
        files = sorted((audit / 'days' / symbol).glob('*.json'))
        assert len(files) == marker['days'], symbol
        record = dict(symbol=symbol, days=len(files), first_day=None, last_day=None,
                      minute_rows=0, depth20_minutes=0, partial_depth_minutes=0,
                      invalid_depth_minutes=0, book_rows=0, snapshots=0, trades=0,
                      trades_different_exchange_minute=0, threshold_unavailable_days=0,
                      book_source_bytes=0, trade_source_bytes=0)
        missing_days = []
        for path in files:
            day = json.loads(path.read_text())
            assert day['symbol'] == symbol, path
            record['first_day'] = record['first_day'] or day['day']
            record['last_day'] = day['day']
            # Raw receipts count all 1,440 UTC minutes, including time before
            # listing on a first day. Availability concerns emitted bars only.
            baseline = root / 'baseline_data_1min/binance-futures' / symbol / (path.stem + '.parquet')
            depth = pq.read_table(baseline, columns=['book_depth'])['book_depth'].to_numpy(zero_copy_only=False)
            assert len(depth) == day['outputs'][0]['rows'], path
            record['minute_rows'] += len(depth)
            day_quality = dict(depth20_minutes=int(np.sum(depth == 20)),
                               partial_depth_minutes=int(np.sum((depth > 0) & (depth < 20))),
                               invalid_depth_minutes=int(np.sum(~(depth > 0))))
            assert day_quality['depth20_minutes'] == day['book']['depth20_minutes'], path
            assert day_quality['partial_depth_minutes'] == day['book']['partial_depth_minutes'], path
            assert day_quality['invalid_depth_minutes'] == day['book']['invalid_depth_minutes'] - (1440-len(depth)), path
            for name, count in day_quality.items():
                record[name] += count
            record['snapshots'] += day['book']['snapshots']
            record['book_rows'] += day['book']['rows']
            record['trades'] += day['trades']['rows']
            record['trades_different_exchange_minute'] += day['trades']['exchange_minute_differs']
            record['threshold_unavailable_days'] += day['trades']['threshold'] is None
            for name in ['book', 'trade']:
                record[name + '_source_bytes'] += day[name + '_source']['bytes']
            if day_quality['partial_depth_minutes'] or day_quality['invalid_depth_minutes']:
                missing_days.append(day_quality | {'day': day['day']})
        assert record['minute_rows'] == marker['minute_rows'], symbol
        assert record['minute_rows'] == sum(record[k] for k in
                                            ['depth20_minutes', 'partial_depth_minutes', 'invalid_depth_minutes']), symbol
        record['depth20_fraction'] = record['depth20_minutes'] / record['minute_rows']
        quality_rows.append(record)
        for name, value in record.items():
            if isinstance(value, int):
                totals[name] = totals.get(name, 0) + value
        if args.execute:
            target = provenance / 'book_quality_days' / f'{symbol}.json'
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(json.dumps({'symbol': symbol, 'days_with_missing_depth': missing_days}, indent=2) + '\n')
    totals['depth20_fraction'] = totals['depth20_minutes'] / totals['minute_rows']
    assert totals['days'] == delivery['totals']['1']['files_per_dataset']
    assert totals['minute_rows'] == delivery['totals']['1']['rows_per_dataset']
    factor_rows = []
    symbol_factor_rows = []
    for symbol in symbols:
        verification = json.loads((audit / f'verified_{symbol}.json').read_text())
        for minutes in [1, 5, 15]:
            stats = verification['frequencies'][str(minutes)]
            empty = stats['all_null_columns']
            symbol_factor_rows.append(dict(symbol=symbol, bar_minutes=minutes, rows=stats['rows'],
                                           columns_with_finite_values=632-len(empty),
                                           all_null_count=len(empty), all_null_columns=';'.join(empty)))
    for minutes in [1, 5, 15]:
        rows = delivery['totals'][str(minutes)]['rows_per_dataset']
        counts = delivery['totals'][str(minutes)]['finite_counts']
        assert len(counts) == 632
        for name, count in counts.items():
            factor_rows.append(dict(bar_minutes=minutes, factor=name, rows=rows,
                                    finite_rows=count, finite_fraction=count / rows))
    report = {'status': 'published', 'created_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'symbols': len(symbols), 'source_quality': totals,
              'factor_all_null_columns': {n: v['all_null_columns'] for n, v in delivery['totals'].items()},
              'delivery_sha256': hashlib.sha256((provenance / 'delivery_20261026.json').read_bytes()).hexdigest(),
              'definitions': {'depth20_fraction': 'Minutes with valid uncrossed bid and ask books, at least 20 levels per side / all emitted minutes.',
                              'partial_depth_minutes': 'Valid two-sided book with fewer than 20 levels on one or both sides.',
                              'invalid_depth_minutes': 'No valid initialized two-sided uncrossed book at minute end.',
                              'threshold_unavailable_days': 'Previous UTC natural month has no known trade samples; size buckets remain 0.',
                              'factor_availability': 'Finite values after shift(1), including defined zeros; original warm-up and missing-value rules retained.'}}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if args.execute:
        for filename, records in [('source_quality_by_symbol.csv', quality_rows),
                                  ('factor_availability.csv', factor_rows),
                                  ('factor_availability_by_symbol.csv', symbol_factor_rows)]:
            with (provenance / filename).open('w', newline='') as handle:
                writer = csv.DictWriter(handle, fieldnames=list(records[0]))
                writer.writeheader()
                writer.writerows(records)
        (provenance / 'quality_summary_20261026.json').write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()
