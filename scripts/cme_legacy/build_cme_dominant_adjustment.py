#!/usr/bin/env python3
"""Build narrow settlement-adjusted volume rolls for six liquid CME roots.

The input contract data stays untouched. This produces a separate research
layer for ES, NQ, RTY, YM, GC, and CL only.
"""

from __future__ import annotations

import argparse
import csv
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from .cme_dominant_adjustment import (
    AdjustmentRecord,
    ContractSnapshot,
    DominantRecord,
    SettlementEvent,
    compute_adjustments,
    select_dominants,
)


DEFAULT_BASELINE_ROOT = Path("/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min")
DEFAULT_OUTPUT_ROOT = Path("/mnt/hdd-raid5-72t/liang_torch/lseg_data/cme_volume_roll_1d")
PRODUCTS: dict[str, tuple[str, str]] = {
    "ES": ("CME", "ES"),
    "NQ": ("CME", "NQ"),
    "RTY": ("CME", "RTY"),
    "YM": ("CBOT", "YM"),
    "GC": ("COMEX", "GC"),
    "CL": ("NYMEX", "CL"),
}


def _parse_day(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value!r}") from exc


def _parse_products(value: str) -> list[str]:
    products = sorted({part.strip().upper() for part in value.split(",") if part.strip()})
    if not products:
        raise argparse.ArgumentTypeError("products cannot be empty")
    unknown = sorted(set(products) - PRODUCTS.keys())
    if unknown:
        raise argparse.ArgumentTypeError(f"unsupported liquid roots: {unknown}")
    return products


def _file_day(path: Path) -> date:
    try:
        return datetime.strptime(path.stem, "%Y%m%d").date()
    except ValueError as exc:
        raise ValueError(f"baseline file does not use YYYYMMDD.parquet: {path}") from exc


def discover_baseline_files(
    *, baseline_root: Path, exchange: str, product: str, start: date, end: date
) -> dict[date, Path]:
    product_dir = baseline_root / exchange / product
    if not product_dir.is_dir():
        raise FileNotFoundError(f"missing TAS baseline directory: {product_dir}")
    files: dict[date, Path] = {}
    for path in sorted(product_dir.glob("*.parquet")):
        trading_day = _file_day(path)
        if start <= trading_day <= end:
            if trading_day in files:
                raise ValueError(f"duplicate TAS baseline file for {exchange}:{product} {trading_day}")
            files[trading_day] = path
    if not files:
        raise ValueError(f"no TAS baseline files for {exchange}:{product} in {start}..{end}")
    return files


def read_daily_volume_snapshots(
    path: Path, *, exchange: str, product: str
) -> list[ContractSnapshot]:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required; run with `uv run --with pyarrow`") from exc

    table = pq.read_table(path, columns=["contract_id", "ric", "volume"])
    groups = table.group_by(["contract_id", "ric"]).aggregate([("volume", "sum")])
    expected = f"{exchange}:{product}:"
    snapshots: list[ContractSnapshot] = []
    for row in groups.to_pylist():
        contract_id = row["contract_id"]
        ric = row["ric"]
        volume = row["volume_sum"]
        if not isinstance(contract_id, str) or not contract_id.startswith(expected):
            raise ValueError(f"unexpected contract_id in {path}: {contract_id!r}")
        if not isinstance(ric, str) or not ric:
            raise ValueError(f"missing RIC for {contract_id} in {path}")
        snapshots.append(
            ContractSnapshot(
                contract_id=contract_id,
                ric=ric,
                volume=float(volume) if volume is not None else 0.0,
            )
        )
    return snapshots


def read_product_snapshots(
    files: dict[date, Path], *, exchange: str, product: str
) -> dict[date, list[ContractSnapshot]]:
    return {
        trading_day: read_daily_volume_snapshots(path, exchange=exchange, product=product)
        for trading_day, path in sorted(files.items())
    }


def read_reference_closes(
    files: dict[date, Path], dominants: list[DominantRecord]
) -> dict[tuple[str, date], float]:
    try:
        import pyarrow.compute as pc
        import pyarrow.parquet as pq
    except ImportError as exc:
        raise RuntimeError("pyarrow is required; run with `uv run --with pyarrow`") from exc

    wanted: dict[date, set[str]] = {}
    for previous, current in zip(dominants, dominants[1:]):
        if (
            previous.ric is not None
            and current.ric is not None
            and previous.ric != current.ric
        ):
            wanted.setdefault(previous.trading_day, set()).update(
                (previous.ric, current.ric)
            )

    closes: dict[tuple[str, date], float] = {}
    for reference_day, rics in wanted.items():
        path = files.get(reference_day)
        if path is None:
            continue
        table = pq.read_table(path, columns=["ric", "ts", "close"])
        for ric in sorted(rics):
            rows = table.filter(
                pc.and_(pc.equal(table["ric"], ric), pc.is_valid(table["close"]))
            ).sort_by([("ts", "ascending")])
            if rows.num_rows:
                closes[(ric, reference_day)] = float(rows["close"][-1].as_py())
    return closes


def load_settlements(
    path: Path, products: Iterable[str], start: date, end: date
) -> dict[tuple[str, date], list[SettlementEvent]]:
    wanted = set(products)
    settlements: dict[tuple[str, date], list[SettlementEvent]] = {}
    with path.open(newline="", encoding="ascii") as handle:
        reader = csv.DictReader(handle)
        expected = {
            "product",
            "ric",
            "source_date",
            "published_ts_utc_ns",
            "price_e9",
            "part",
            "seq",
        }
        if reader.fieldnames is None or set(reader.fieldnames) != expected:
            raise ValueError(f"unexpected settlement CSV header in {path}: {reader.fieldnames}")
        for row in reader:
            product = row["product"]
            source_day = date.fromisoformat(row["source_date"])
            if product not in wanted or source_day < start or source_day > end:
                continue
            settlements.setdefault((row["ric"], source_day), []).append(
                SettlementEvent(
                    price=int(row["price_e9"]) / 1_000_000_000.0,
                    published_ts_ns=int(row["published_ts_utc_ns"]),
                    part=int(row["part"]),
                    seq=int(row["seq"]),
                )
            )
    return settlements


def _csv_value(value: object) -> object:
    return value.isoformat() if isinstance(value, date) else "" if value is None else value


def write_csv(path: Path, rows: Iterable[DominantRecord | AdjustmentRecord], *, overwrite: bool) -> None:
    rows = list(rows)
    if not rows:
        raise ValueError(f"refuse to write empty result: {path}")
    if path.exists() and not overwrite:
        raise FileExistsError(f"output exists: {path}; pass --overwrite")
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.partial")
    if partial.exists():
        partial.unlink()
    with partial.open("w", newline="", encoding="ascii") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(asdict(rows[0]).keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(value) for key, value in asdict(row).items()})
        handle.flush()
    partial.replace(path)


def build_product(
    *,
    baseline_root: Path,
    settlement_events: dict[tuple[str, date], list[SettlementEvent]],
    output_root: Path,
    exchange: str,
    product: str,
    start: date,
    end: date,
    warmup_start: date | None,
    overwrite: bool,
) -> tuple[str, int, int, int]:
    selection_start = warmup_start or start
    files = discover_baseline_files(
        baseline_root=baseline_root,
        exchange=exchange,
        product=product,
        start=selection_start,
        end=end,
    )
    calendar = sorted(files)
    snapshots = read_product_snapshots(files, exchange=exchange, product=product)
    dominants = select_dominants(
        calendar=calendar,
        snapshots_by_day=snapshots,
        exchange=exchange,
        product=product,
    )
    close_prices = read_reference_closes(files, dominants)
    adjustments = compute_adjustments(
        dominants=dominants,
        settlements=settlement_events,
        close_prices=close_prices,
    )
    dominants = [row for row in dominants if row.trading_day >= start]
    adjustments = [row for row in adjustments if row.effective_trading_day >= start]
    target = output_root / exchange / product
    write_csv(target / "dominant.csv", dominants, overwrite=overwrite)
    if adjustments:
        write_csv(target / "adjustment.csv", adjustments, overwrite=overwrite)
    return product, len(dominants), len(adjustments), sum(row.skipped for row in adjustments)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-root", type=Path, default=DEFAULT_BASELINE_ROOT)
    parser.add_argument("--settlements", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start", type=_parse_day, required=True)
    parser.add_argument("--end", type=_parse_day, required=True)
    parser.add_argument(
        "--warmup-start",
        type=_parse_day,
        help="optional earlier day used to seed causal state; rows before --start are not written",
    )
    parser.add_argument("--products", type=_parse_products, default=list(PRODUCTS))
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.end < args.start:
        raise ValueError(f"end {args.end} precedes start {args.start}")
    if args.warmup_start is not None and args.warmup_start >= args.start:
        raise ValueError(
            f"warmup-start {args.warmup_start} must precede start {args.start}"
        )
    if args.workers < 1:
        raise ValueError("workers must be positive")
    settlement_start = args.warmup_start or args.start
    settlements = load_settlements(args.settlements, args.products, settlement_start, args.end)
    jobs = []
    with ThreadPoolExecutor(max_workers=min(args.workers, len(args.products))) as pool:
        for product in args.products:
            exchange, root = PRODUCTS[product]
            jobs.append(
                pool.submit(
                    build_product,
                    baseline_root=args.baseline_root,
                    settlement_events=settlements,
                    output_root=args.output_root,
                    exchange=exchange,
                    product=root,
                    start=args.start,
                    end=args.end,
                    warmup_start=args.warmup_start,
                    overwrite=args.overwrite,
                )
            )
        for job in jobs:
            product, days, rolls, skipped = job.result()
            print(f"cme_volume_roll product={product} days={days} rolls={rolls} skipped={skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
