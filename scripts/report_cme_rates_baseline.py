#!/usr/bin/env python3
"""Summarise export day audits and Parquet footers, without scanning source DBs."""
import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq


def report(root):
    contracts_path = root / '_audit/contracts.json'
    contracts = json.loads(contracts_path.read_text()) if contracts_path.exists() else []
    rics = defaultdict(set)
    for row in contracts:
        rics[row['product']].add(row['ric'])
    products = {}
    for product in sorted(rics):
        audits = [json.loads(p.read_text()) for p in sorted((root / '_audit' / product).glob('*.json'))]
        audits = [a for a in audits if a['published']]
        files = sorted((root / 'CBOT' / product).glob('*.parquet'))
        footer_rows = 0
        stored_bytes = 0
        for path in files:
            footer = pq.read_metadata(path)
            assert footer.num_columns == 84, (path, footer.num_columns)
            footer_rows += footer.num_rows
            stored_bytes += path.stat().st_size
        audited_rows = sum(a['output_rows'] for a in audits)
        assert footer_rows == audited_rows, (product, footer_rows, audited_rows)
        assert len(files) == len(audits), (product, len(files), len(audits))
        fields = ('trade_rows', 'special_rows', 'invalid_special_rows', 'grid_minutes',
                  'exact_ll2_minutes', 'raw_bidirectional_minutes', 'raw_full_depth_minutes',
                  'raw_last_invalid_minutes', 'raw_no_snapshot_minutes', 'outside_session_trades')
        item = dict(contracts=len(rics[product]), rics=sorted(rics[product]),
                    files=len(files), rows=footer_rows, bytes=stored_bytes,
                    first_day=min((a['trading_day'] for a in audits), default=None),
                    last_day=max((a['trading_day'] for a in audits), default=None))
        item.update({name: sum(a.get(name, 0) for a in audits) for name in fields})
        item['months'] = {}
        for month in sorted({a['trading_day'][:7] for a in audits}):
            days = [a for a in audits if a['trading_day'].startswith(month)]
            item['months'][month] = dict(files=len(days), rows=sum(a['output_rows'] for a in days))
        item['size_threshold_months'] = sorted(p.stem for p in (root / '_audit/size_buckets' / product).glob('*.json'))
        item['raw_bidirectional_pct'] = (100 * item['raw_bidirectional_minutes'] / footer_rows) if footer_rows else None
        item['raw_full_depth_pct'] = (100 * item['raw_full_depth_minutes'] / footer_rows) if footer_rows else None
        item['rics_without_raw_on_some_day'] = sorted(set(r for a in audits for r in a['rics_without_ll2']))
        products[product] = item
    sums = ('contracts', 'files', 'rows', 'bytes', 'trade_rows', 'special_rows',
            'exact_ll2_minutes', 'raw_bidirectional_minutes', 'raw_full_depth_minutes',
            'raw_last_invalid_minutes', 'raw_no_snapshot_minutes', 'outside_session_trades')
    return dict(measured_at=datetime.now(timezone.utc).isoformat(), output_root=str(root),
                scope='2026 Jan-May; TU/FV/TY/TN/US/AUL/FF; 1min contract baseline',
                source_verification='source full scans skipped; day audits and Parquet footers only',
                columns=84, products=products,
                total={name: sum(p[name] for p in products.values()) for name in sums})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = report(args.root)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result['total']))
