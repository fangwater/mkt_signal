#!/usr/bin/env python3
"""Small-input Python correctness baseline for tardis_order_size (not a batch runner)."""

import argparse
import csv
import datetime as dt
import gzip
import json
import math
from pathlib import Path
import struct
import time

CAPACITY = 512
SEED = 0x243F6A8885A308D3
MASK = (1 << 64) - 1
HOUR_US = 3_600_000_000
HEADER = ["exchange", "symbol", "timestamp", "local_timestamp", "id", "side", "price", "amount"]


def random_step(state):
    state = (state + 0x9E3779B97F4A7C15) & MASK
    z = ((state ^ (state >> 30)) * 0xBF58476D1CE4E5B9) & MASK
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
    return state, z ^ (z >> 31)


class Sketch:
    def __init__(self, streaming=True):
        self.state = random_step(SEED)[1] if streaming else SEED
        self.levels = [[]]
        self.count = 0

    def compact(self, level):
        values = self.levels[level]
        if len(values) <= CAPACITY:
            return
        values.sort()
        even_length = len(values) & ~1
        carry = values[even_length:]
        self.state, random = random_step(self.state)
        promoted = values[random & 1:even_length:2]
        self.levels[level] = carry
        if len(self.levels) == level + 1:
            self.levels.append([])
        self.levels[level + 1].extend(promoted)
        self.compact(level + 1)

    def insert(self, value):
        self.count += 1
        self.levels[0].append(value)
        self.compact(0)

    @classmethod
    def merge(cls, sketches):
        merged = cls(streaming=False)
        for sketch in sketches:
            merged.count += sketch.count
            while len(merged.levels) < len(sketch.levels):
                merged.levels.append([])
            for level, values in enumerate(sketch.levels):
                merged.levels[level].extend(values)
        level = 0
        while level < len(merged.levels):
            merged.compact(level)
            level += 1
        return merged

    def quantile(self, q):
        weighted = sorted((value, 1 << level) for level, values in enumerate(self.levels) for value in values)
        if not weighted:
            return None
        rank = float32(q) * (sum(weight for _, weight in weighted) - 1)
        lower, upper = math.floor(rank), math.ceil(rank)
        seen, low = 0, None
        for value, weight in weighted:
            seen += weight
            if low is None and seen > lower:
                low = value
            if seen > upper:
                return low + (value - low) * (rank - lower)
        raise AssertionError("rank exceeds retained weight")


def float32(value):
    return struct.unpack("f", struct.pack("f", value))[0]


def month_bounds(month):
    start = dt.datetime.strptime(month + "-01", "%Y-%m-%d").replace(tzinfo=dt.timezone.utc)
    end = (start + dt.timedelta(days=32)).replace(day=1)
    return int(start.timestamp()) * 1_000_000, int(end.timestamp()) * 1_000_000


def reference(paths, exchange, symbol, month):
    """Keep exact samples only for validation/benchmark inputs that fit in RAM."""
    started = time.perf_counter()
    start, end = month_bounds(month)
    stats = dict(source_rows=0, sample_count=0, invalid_values=0, outside_month=0, late_trades=0)
    hourly, current, values = [], None, []
    first = last = None
    for path in paths:
        with gzip.open(path, "rt", newline="") as stream:
            reader = csv.DictReader(stream)
            if reader.fieldnames != HEADER:
                raise ValueError(f"unexpected header: {path}")
            for row in reader:
                stats["source_rows"] += 1
                if None in row or None in row.values():
                    raise ValueError("wrong column count")
                if row["exchange"] != exchange or row["symbol"] != symbol:
                    raise ValueError("exchange/symbol mismatch")
                timestamp = int(row["timestamp"])
                if timestamp <= 0 or int(row["id"]) <= 0 or row["side"].lower() not in ("buy", "sell"):
                    raise ValueError("invalid timestamp/id/side")
                price, amount = float(row["price"]), float(row["amount"])
                notional = price * amount
                if any(not math.isfinite(v) or v <= 0 for v in (price, amount, notional)):
                    stats["invalid_values"] += 1
                    continue
                if not start <= timestamp < end:
                    stats["outside_month"] += 1
                    continue
                hour = timestamp // HOUR_US
                if current is not None and hour < current:
                    stats["late_trades"] += 1
                    continue
                if hour != current:
                    current = hour
                    hourly.append(Sketch())
                hourly[-1].insert(notional)
                values.append(notional)
                first = timestamp if first is None else min(first, timestamp)
                last = timestamp if last is None else max(last, timestamp)
    merged = Sketch.merge(hourly)
    stats.update(sample_count=len(values), nonempty_hours=len(hourly), first_timestamp_us=first, last_timestamp_us=last)
    values.sort()

    def exact(q):
        if not values:
            return None
        rank = float32(q) * (len(values) - 1)
        lower, upper = math.floor(rank), math.ceil(rank)
        return values[lower] + (values[upper] - values[lower]) * (rank - lower)

    return dict(**stats, medium_notional_threshold=merged.quantile(0.5),
                large_notional_threshold=merged.quantile(0.9), exact_p50=exact(0.5), exact_p90=exact(0.9),
                sketch=dict(sample_count=merged.count, level_capacity=CAPACITY, levels=merged.levels),
                elapsed_seconds=time.perf_counter() - started)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, nargs="+", required=True)
    parser.add_argument("--exchange", choices=["binance", "binance-futures"], required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--month", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = reference(args.input, args.exchange, args.symbol, args.month)
    args.output.write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")


if __name__ == "__main__":
    main()
