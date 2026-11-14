"""Reject an entire selected donor if known price ranks are inconsistent.

This correctness-baseline stage follows cme_patch_ll2_1min. It does not sort,
renumber, interpolate, or search another instant after rejecting a donor.
"""
import argparse
import csv
import hashlib
import io
import json
import shutil
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import zstandard as zstd

PRICES = [f'{side}{i}p' for side in ['bid', 'ask'] for i in range(10)]
BOOK = ['mid_price'] + [f'{s}{i}{f}' for i in range(10) for s in ['bid', 'ask'] for f in ['p', 'v']]


def unordered_known_prices(bids, asks):
    """Check every known rank, including ranks separated by missing slots."""
    bids, asks = np.asarray(bids), np.asarray(asks)
    if bids.ndim != 2 or bids.shape != asks.shape:
        raise ValueError('bid/ask matrices must have the same two-dimensional shape')
    bad = np.zeros(len(bids), dtype=bool)
    for prices, is_bid in [(bids, True), (asks, False)]:
        previous = np.full(len(prices), np.inf if is_bid else -np.inf)
        for price in prices.T:
            known = np.isfinite(price)
            bad |= known & (price > previous if is_bid else price < previous)
            previous = np.where(known, np.minimum(previous, price) if is_bid else np.maximum(previous, price), previous)
    return bad


def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 << 20), b''):
            h.update(block)
    return h.hexdigest()


def atomic_json(path, value):
    partial = path.with_name(path.name + '.tmp')
    partial.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')
    partial.replace(path)


def clear_book(table, mask):
    mask = pa.array(mask, type=pa.bool_())
    for name in BOOK:
        index = table.schema.get_field_index(name)
        table = table.set_column(index, table.schema.field(index), pc.if_else(mask, pa.scalar(None, table[name].type), table[name]))
    return table


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--baseline-root', type=Path, required=True)
    ap.add_argument('--hfq-root', type=Path, required=True)
    ap.add_argument('--backup-root', type=Path, required=True)
    ap.add_argument('--report', type=Path, required=True)
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--apply', action='store_true')
    args = ap.parse_args()
    assert not args.report.exists() and not args.backup_root.exists()
    assert args.baseline_root.resolve() != args.hfq_root.resolve()
    assert not args.backup_root.resolve().is_relative_to(args.baseline_root.resolve())
    assert not args.backup_root.resolve().is_relative_to(args.hfq_root.resolve())
    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    paths = sorted(args.baseline_root.glob('*/*/*.parquet'))
    assert paths

    def backup(path, relative):
        dest = args.backup_root / relative
        dest.parent.mkdir(parents=True, exist_ok=True)
        assert not dest.exists()
        shutil.copy2(path, dest)
        assert digest(path) == digest(dest)

    def replace_table(path, table):
        partial = path.with_suffix('.quality.parquet')
        assert not partial.exists()
        pq.write_table(table, partial, compression='zstd')
        partial.replace(path)

    def review(path):
        relative = path.relative_to(args.baseline_root)
        prices = pq.read_table(path, columns=PRICES, use_threads=False)
        matrices = [np.column_stack([prices[f'{s}{i}p'].to_numpy() for i in range(10)]) for s in ['bid', 'ask']]
        bad = unordered_known_prices(*matrices)
        if not bad.any():
            return None
        table = pq.read_table(path, use_threads=False)
        keys = list(zip(table['ric'].to_pylist(), map(int, table['ts'].to_numpy())))
        rejected = {key for key, flag in zip(keys, bad) if flag}
        assert len(rejected) == int(bad.sum())
        csv_path = args.baseline_root / '_audit/ll2' / relative.with_suffix('.csv.zst')
        json_path = csv_path.with_suffix('').with_suffix('.json')
        with csv_path.open('rb') as stream, zstd.ZstdDecompressor().stream_reader(stream) as decoded:
            reader = csv.DictReader(io.TextIOWrapper(decoded))
            fields, records = reader.fieldnames, list(reader)
        removed = [r for r in records if (r['ric'], int(r['ts'])) in rejected]
        assert len(removed) == len(rejected)
        kept = [r for r in records if (r['ric'], int(r['ts'])) not in rejected]
        audit = json.loads(json_path.read_text())
        assert audit['filled_rows'] == len(records)
        cleared_cells = sum(int(np.count_nonzero(np.isfinite(table[name].to_numpy()[bad]))) for name in BOOK)
        audit.update(filled_rows=len(kept), filled_cells=audit['filled_cells']-cleared_cells,
                     source_rows=dict(Counter(r['source_index'] for r in kept)),
                     quality_rejected_rows=len(rejected), quality_rule='reject_entire_donor_on_unordered_known_price_ranks')
        result = dict(path=str(relative), baseline_before_sha256=digest(path), rejected_rows=len(rejected),
                      cleared_cells=cleared_cells, donors=removed,
                      rejected_book=table.filter(pa.array(bad)).select(['ric','ts']+BOOK).to_pylist())
        hfq_path = args.hfq_root / relative
        hfq = pq.read_table(hfq_path, use_threads=False) if hfq_path.exists() else None
        hfq_bad = None if hfq is None else np.array([(r, int(t)) in rejected for r, t in zip(hfq['ric'].to_pylist(), hfq['ts'].to_numpy())])
        result['hfq_rejected_rows'] = 0 if hfq_bad is None else int(hfq_bad.sum())
        if args.apply:
            backup(path, Path('baseline')/relative)
            backup(csv_path, Path('donors')/relative.with_suffix('.csv.zst'))
            backup(json_path, Path('donors')/relative.with_suffix('.json'))
            replace_table(path, clear_book(table, bad))
            output = io.StringIO(); writer = csv.DictWriter(output, fieldnames=fields)
            writer.writeheader(); writer.writerows(kept)
            partial = csv_path.with_name(csv_path.name+'.tmp')
            partial.write_bytes(zstd.ZstdCompressor(level=1).compress(output.getvalue().encode()))
            partial.replace(csv_path)
            atomic_json(json_path, audit)
            if hfq_bad is not None and hfq_bad.any():
                backup(hfq_path, Path('hfq')/relative)
                result['hfq_before_sha256'] = digest(hfq_path)
                replace_table(hfq_path, clear_book(hfq, hfq_bad))
                result['hfq_after_sha256'] = digest(hfq_path)
            result['baseline_after_sha256'] = digest(path)
        return result

    with ThreadPoolExecutor(args.workers) as pool:
        changes = [r for r in pool.map(review, paths) if r is not None]
    atomic_json(args.report, dict(status='applied_quality_veto' if args.apply else 'dry_run', files_scanned=len(paths),
        changed_files=len(changes), rejected_rows=sum(r['rejected_rows'] for r in changes),
        hfq_rejected_rows=sum(r['hfq_rejected_rows'] for r in changes), changes=changes, script_sha256=digest(Path(__file__))))
    print(json.dumps(dict(files_scanned=len(paths), changed_files=len(changes), rejected_rows=sum(r['rejected_rows'] for r in changes))))


if __name__ == '__main__':
    main()
