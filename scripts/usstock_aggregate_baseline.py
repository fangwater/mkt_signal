"""Correctness baseline for US-stock RTH 1h and session-day bars."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

FREQUENCIES = {'1h': 60, '1day': 1440}
SUM = {
    'volume', 'amount', 'count', 'buy_volume', 'buy_amount', 'buy_count',
    'sell_volume', 'sell_amount', 'sell_count', 'unknown_volume', 'unknown_amount',
    'unknown_count', 'off_exchange_volume', 'off_exchange_amount', 'off_exchange_count',
    'large_order', 'medium_order', 'small_order', 'large_buy', 'large_sell',
    'medium_buy', 'medium_sell', 'small_buy', 'small_sell', 'n_seconds',
}


def aggregate_table(table: pa.Table, frequency: str, session_date: str):
    if frequency not in FREQUENCIES or not table.num_rows:
        raise ValueError('expected nonempty US-stock minute table and 1h/1day')
    names = set(table.column_names)
    required = {'ric', 'venue', 'ts', 'count', 'volume', 'amount', 'open', 'high', 'low', 'close', 'n_seconds'}
    if not required <= names:
        raise ValueError('missing minute fields')
    a = {k: table[k].to_pylist() for k in table.column_names}
    ts = a['ts']; n = len(ts)
    if any(t is None or t % 60 for t in ts) or any(x >= y for x, y in zip(ts, ts[1:])):
        raise ValueError('minute keys must increase strictly')
    if any(x != 60 for x in a['n_seconds']):
        raise ValueError('source must contain 60-second minute bars')
    if any(len(set(a[k])) != 1 or a[k][0] is None for k in ['ric', 'venue']):
        raise ValueError('multiple or null securities')
    # Session-day membership is checked by the frozen calendar before execution.
    groups = []
    if frequency == '1day':
        groups = [(0, n, True)]
    else:
        start = 0
        for end in range(1, n + 1):
            if end == n or ts[end] - ts[end - 1] != 60:
                groups.extend((i, min(i + 60, end), i == start) for i in range(start, end, 60))
                start = end

    def finite(v):
        return v is not None and np.isfinite(v)

    def total(k, rows):
        vals = [a[k][i] for i in rows]
        if not all(finite(v) for v in vals):
            return None
        if pa.types.is_integer(table.schema.field(k).type):
            return sum(vals)
        value = 0.0
        for v in vals:
            value += v
        return value if finite(value) else None

    def sub(x, y):
        return None if x is None or y is None else x - y

    output = []; bars = []
    for start, end, segment_start in groups:
        rows = range(start, end); last = end - 1
        r = {k: a[k][last] for k in table.column_names}
        r['ts'] = ts[start]
        for k in SUM & names:
            r[k] = total(k, rows)
        active = [i for i in rows if a['count'][i] is not None and a['count'][i] > 0]
        if active:
            r['open'], r['close'] = a['open'][active[0]], a['close'][active[-1]]
            for k, op in [('high', max), ('low', min)]:
                v = [a[k][i] for i in active]
                r[k] = op(v) if all(finite(x) for x in v) else None
        for k, count, op in [('buy_high', 'buy_count', max), ('sell_low', 'sell_count', min)]:
            if k in names:
                v = [a[k][i] for i in rows if a[count][i] > 0]
                if v:
                    r[k] = op(v) if all(finite(x) for x in v) else None
        for k, weight in [('vwap', 'volume'), ('buy_vwap', 'buy_volume'), ('sell_vwap', 'sell_volume')]:
            if k not in names:
                continue
            if any(not finite(a[weight][i]) or a[weight][i] < 0 for i in rows):
                raise ValueError('unknown or negative trade volume')
            if r[weight] > 0:
                value = 0.0
                for i in rows:
                    if a[weight][i] > 0:
                        if not finite(a[k][i]):
                            value = float('nan'); break
                        value += a[k][i] * a[weight][i]
                r[k] = value / r[weight] if finite(value) else None
        if 'twap' in names:
            value = total('twap', rows)
            r['twap'] = None if value is None else value / len(rows)
        if 'avg_amount' in names:
            r['avg_amount'] = (None if r['amount'] is None else r['amount'] / r['count']) if r['count'] else 0.0
        for k, b, s in [('net_buy_amount', 'buy_amount', 'sell_amount'), ('net_buy_volume', 'buy_volume', 'sell_volume'),
                        ('net_buy_large', 'large_buy', 'large_sell'), ('net_buy_medium', 'medium_buy', 'medium_sell'),
                        ('net_buy_small', 'small_buy', 'small_sell')]:
            if k in names:
                r[k] = sub(r[b], r[s])
        if 'net_buy_pct' in names:
            b, s = r['buy_amount'], r['sell_amount']
            r['net_buy_pct'] = None if b is None or s is None else ((b-s)/(b+s) if b+s > 0 else 0.0)
        for k in names:
            if pa.types.is_floating(table.schema.field(k).type) and not finite(r[k]):
                r[k] = None
        # All book columns, depth, source timestamp and source-quality bitmask
        # come atomically from the final source minute, including its nulls.
        flags = 0
        for i in rows:
            flags |= a.get('source_quality_flags', [0]*n)[i]
        bars.append(dict(ts=ts[start], end_ts=ts[last]+60, last_minute_ts=ts[last],
                         source_minutes=end-start, segment_start=segment_start,
                         source_depth_ts_utc_ns=r.get('source_depth_ts_utc_ns'),
                         source_quality_flags_any=flags))
        output.append(r)
    result = pa.Table.from_arrays([
        pa.array([r[k] for r in output], type=table.schema.field(k).type)
        for k in table.column_names], schema=table.schema)
    return result, dict(frequency=frequency, session_date=session_date, bars=bars)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', type=Path, required=True); p.add_argument('--output', type=Path, required=True)
    p.add_argument('--metadata', type=Path, required=True); p.add_argument('--frequency', choices=FREQUENCIES, required=True)
    p.add_argument('--execute', action='store_true'); a = p.parse_args()
    if not a.execute:
        print(json.dumps(dict(input=str(a.input), output=str(a.output), frequency=a.frequency))); return
    if a.output.exists() or a.metadata.exists():
        raise FileExistsError('output already exists')
    result, meta = aggregate_table(pq.read_table(a.input), a.frequency, a.input.stem)
    a.output.parent.mkdir(parents=True, exist_ok=True); a.metadata.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(result, a.output, compression='zstd'); a.metadata.write_text(json.dumps(meta)+'\n')


if __name__ == '__main__':
    main()
