#!/usr/bin/env python3
"""Reproduce the independent Raw baseline fixture and optional CPU benchmark."""
import argparse
import json
import time
from pathlib import Path

from cme_raw_ll2_reference import extract, fill_baseline_records, minute_snapshots

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'crates/cme_tas_replay/tests/fixtures'


def cases():
    output = []
    for name in ('small', 'refresh', 'partial_refresh', 'status'):
        snapshots = extract([FIXTURES / f'raw_ll2_{name}.csv'], 15)['snapshots']
        rows = {}
        for snapshot in minute_snapshots(snapshots):
            ts = int.from_bytes(bytes.fromhex(snapshot['key_hex'])[16:], 'big') // 10**9
            for minute in (ts, ts + 60):
                row = {f'{side}{i}{field}': None for side in ('bid', 'ask')
                       for i in range(10) for field in ('p', 'v')}
                row.update(ric=snapshot['ric'], ts=minute, mid_price=None, volume=17)
                rows[(row['ric'], minute)] = row
        rows = list(rows.values())
        expected = fill_baseline_records(rows, snapshots)
        assert len(expected) == len(rows) and all(r['volume'] == 17 for r in expected)
        output.append(dict(name=name, snapshots=snapshots, rows=rows, expected=expected))
    return output


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--iterations', type=int, default=2000)
    args = parser.parse_args()
    fixed = cases()
    path = FIXTURES / 'raw_ll2_baseline_overlay.expected.json'
    if args.write:
        path.write_text(json.dumps(fixed, indent=2) + '\n')
    else:
        assert fixed == json.loads(path.read_text())
    started = time.perf_counter()
    for _ in range(args.iterations):
        for case in fixed:
            result = fill_baseline_records(case['rows'], case['snapshots'])
    print(json.dumps(dict(implementation='independent_python_reference',
                         scope='in-memory raw minute selection and baseline overlay; excludes DB IO',
                         iterations=args.iterations, cases=len(fixed),
                         elapsed_seconds=time.perf_counter()-started)))
