#!/usr/bin/env python3
"""Python correctness baseline for causal, additive CME minute HFQ parquet."""
import argparse
from pathlib import Path

from cme_causal_roll_tables import EXCHANGES, load_daily

PRICE_COLUMNS = ("open", "high", "low", "close", "vwap", "buy_vwap", "sell_vwap", "twap",
                 "mid_price", "implied_vwap", "implied_twap") + tuple(
                     f"{side}{level}p" for side in ("bid", "ask") for level in range(10))


def apply_hfq(table, metadata):
    import math
    import pyarrow as pa
    import pyarrow.compute as pc
    table = table.filter(pc.equal(table["ric"], metadata["ric"]))
    if not len(table):
        raise ValueError("dominant RIC absent from source baseline")
    if any(v != metadata["contract_id"] for v in table["contract_id"].to_pylist()):
        raise ValueError("source contract_id mismatch")
    timestamps = table["ts"].to_pylist()
    if any(t is None or t * 10**9 < metadata["effective_ns"] for t in timestamps):
        raise ValueError("row precedes decision effectiveness")
    if any(b <= a for a, b in zip(timestamps, timestamps[1:])):
        raise ValueError("non-increasing timestamps")
    table = table.set_column(table.schema.get_field_index("contract_id"), "contract_id",
                             pa.array([metadata["product"]] * len(table), type=table["contract_id"].type))
    gap = metadata["offset_e9"] / 10**9
    for name in PRICE_COLUMNS:
        column = table[name]
        if column.type != pa.float64():
            raise ValueError(f"price must be float64: {name}")
        table = table.set_column(table.schema.get_field_index(name), name, pc.add(column, gap))
    for bid, ask in zip(table["bid0p"].to_pylist(), table["ask0p"].to_pylist()):
        if bid is None and ask is None:
            continue
        if bid is None or ask is None or not math.isfinite(bid) or not math.isfinite(ask) or bid > ask:
            raise ValueError("invalid adjusted L1")
    for name in ("segment_id", "offset_e9"):
        if name in table.schema.names:
            raise ValueError(f"source baseline already has causal field {name}")
        table = table.append_column(name, pa.array([metadata[name]] * len(table), type=pa.int64()))
    return table


def main():
    import pyarrow.parquet as pq
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True)
    parser.add_argument("--roll-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--products", required=True)
    args = parser.parse_args()
    if args.input_root.resolve() == args.output_root.resolve() or args.start >= args.end:
        raise ValueError("unsafe output or invalid range")
    jobs = []
    products = args.products.split(",")
    if len(set(products)) != len(products):
        raise ValueError("duplicate products")
    for product in products:
        relative = Path(EXCHANGES[product]) / product
        for row in load_daily(args.roll_root / relative / "dominant.csv"):
            if not args.start <= row["trading_day"] < args.end:
                continue
            name = row["trading_day"].replace("-", "") + ".parquet"
            source, output = args.input_root / relative / name, args.output_root / relative / name
            if output.exists():
                raise FileExistsError(output)
            if not row["ric"]:
                continue
            if not source.is_file():
                raise FileNotFoundError(source)
            jobs.append((source, output, row))
    if not jobs:
        raise ValueError("no selected days")
    for source, output, row in jobs:
        result = apply_hfq(pq.read_table(source), row)
        output.parent.mkdir(parents=True, exist_ok=True)
        partial = output.with_suffix(".parquet.partial")
        with partial.open("xb") as f:
            pq.write_table(result, f, compression="zstd")
        partial.rename(output)


if __name__ == "__main__":
    main()
