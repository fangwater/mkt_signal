#!/usr/bin/env python3
from __future__ import annotations

import argparse
import binascii
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq


EXPECTED_SCHEMAS = {
    "order_updates_unmatched.parquet": [
        "key",
        "ts_us",
        "event_time",
        "symbol",
        "order_id",
        "client_order_id",
        "client_order_id_str",
        "side",
        "order_type",
        "time_in_force",
        "price",
        "quantity",
        "cumulative_filled_quantity",
        "status",
        "raw_status",
        "execution_type",
        "raw_execution_type",
        "trading_venue",
    ],
    "trade_updates_unmatched.parquet": [
        "key",
        "ts_us",
        "event_time",
        "trade_time",
        "symbol",
        "order_id",
        "client_order_id",
        "side",
        "price",
        "is_maker",
        "trading_venue",
        "cumulative_filled_quantity",
        "order_status",
    ],
    "uniform_orders.parquet": [
        "key",
        "ts_us",
        "recv_ts_us",
        "symbol",
        "create_ts",
        "update_ts",
        "signal_ts",
        "client_order_id",
        "trading_venue",
        "order_type",
        "side",
        "price",
        "price_offset",
        "amount_init",
        "amount_update",
        "status",
        "from_key",
        "from_key_hex",
    ],
}


@dataclass
class CheckReport:
    failures: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def fail(self, msg: str) -> None:
        self.failures.append(msg)

    def warn(self, msg: str) -> None:
        self.warnings.append(msg)

    def note(self, msg: str) -> None:
        self.notes.append(msg)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate order_export parquet output under one YYYYMMDD directory."
    )
    parser.add_argument(
        "export_dir",
        type=Path,
        help="Export directory containing order_updates_unmatched.parquet, trade_updates_unmatched.parquet, uniform_orders.parquet",
    )
    parser.add_argument(
        "--date",
        help="Expected UTC date in YYYYMMDD or YYYY-MM-DD. Defaults to inferring from export_dir name.",
    )
    return parser.parse_args()


def parse_date_arg(raw: str) -> datetime:
    if len(raw) == 8 and raw.isdigit():
        dt = datetime.strptime(raw, "%Y%m%d")
    else:
        dt = datetime.strptime(raw, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)


def infer_utc_day_bounds(export_dir: Path, date_arg: str | None) -> tuple[int, int] | None:
    if date_arg:
        day = parse_date_arg(date_arg)
    else:
        try:
            day = parse_date_arg(export_dir.name)
        except ValueError:
            return None
    start = int(day.timestamp() * 1_000_000)
    end = int((day + timedelta(days=1)).timestamp() * 1_000_000) - 1
    return start, end


def micros_to_utc(ts_us: int | None) -> str:
    if ts_us is None:
        return "n/a"
    return datetime.fromtimestamp(ts_us / 1_000_000, tz=timezone.utc).isoformat()


def load_parquet(path: Path) -> tuple[dict[str, list], list[str]]:
    table = pq.read_table(path)
    return table.to_pydict(), table.column_names


def is_sorted_non_decreasing(values: Iterable[int]) -> bool:
    prev = None
    for value in values:
        if prev is not None and value < prev:
            return False
        prev = value
    return True


def check_key_matches_ts(keys: list, ts_values: list, report: CheckReport, label: str) -> None:
    bad = 0
    for key, ts_us in zip(keys, ts_values):
        try:
            parsed = int(key)
        except (TypeError, ValueError):
            bad += 1
            continue
        if parsed != ts_us:
            bad += 1
    if bad:
        report.fail(f"{label}: {bad} rows have key that does not match ts_us")


def check_ts_range(
    values: list,
    start_us: int | None,
    end_us: int | None,
    report: CheckReport,
    label: str,
    hard_fail: bool = True,
) -> None:
    if start_us is None or end_us is None or not values:
        return
    bad = [v for v in values if v is not None and (v < start_us or v > end_us)]
    if not bad:
        return
    msg = (
        f"{label}: {len(bad)} rows outside UTC day window, "
        f"first_bad={bad[0]} ({micros_to_utc(bad[0])})"
    )
    if hard_fail:
        report.fail(msg)
    else:
        report.warn(msg)


def check_non_negative(values: list, report: CheckReport, label: str) -> None:
    bad = sum(1 for v in values if v is not None and v < 0)
    if bad:
        report.fail(f"{label}: {bad} negative values")


def check_positive(values: list, report: CheckReport, label: str) -> None:
    bad = sum(1 for v in values if v is None or v <= 0)
    if bad:
        report.fail(f"{label}: {bad} non-positive values")


def check_enum_presence(values: list, report: CheckReport, label: str) -> None:
    bad = sum(1 for v in values if not v)
    if bad:
        report.fail(f"{label}: {bad} empty values")


def counter_preview(values: list, limit: int = 8) -> str:
    items = Counter(v for v in values if v is not None).most_common(limit)
    return ", ".join(f"{k}={v}" for k, v in items) if items else "n/a"


def validate_order_updates(data: dict[str, list], bounds: tuple[int, int] | None, report: CheckReport) -> None:
    keys = data["key"]
    ts_us = data["ts_us"]
    event_time = data["event_time"]
    quantity = data["quantity"]
    cumulative = data["cumulative_filled_quantity"]

    check_key_matches_ts(keys, ts_us, report, "order_updates")
    if not is_sorted_non_decreasing(ts_us):
        report.warn("order_updates: ts_us is not sorted ascending")
    if bounds:
        check_ts_range(ts_us, bounds[0], bounds[1], report, "order_updates.ts_us")
        check_ts_range(event_time, bounds[0], bounds[1], report, "order_updates.event_time", hard_fail=False)
    check_positive(data["price"], report, "order_updates.price")
    check_positive(quantity, report, "order_updates.quantity")
    check_non_negative(cumulative, report, "order_updates.cumulative_filled_quantity")
    overfilled = sum(
        1
        for qty, cum in zip(quantity, cumulative)
        if qty is not None and cum is not None and cum > qty + 1e-9
    )
    if overfilled:
        report.warn(f"order_updates: {overfilled} rows have cumulative_filled_quantity > quantity")
    for column in ["symbol", "side", "order_type", "time_in_force", "status", "execution_type", "trading_venue"]:
        check_enum_presence(data[column], report, f"order_updates.{column}")

    report.note(
        "order_updates: "
        f"rows={len(keys)}, symbols={len(set(data['symbol']))}, "
        f"status_top=[{counter_preview(data['status'])}], "
        f"execution_type_top=[{counter_preview(data['execution_type'])}]"
    )


def validate_trade_updates(data: dict[str, list], bounds: tuple[int, int] | None, report: CheckReport) -> None:
    keys = data["key"]
    ts_us = data["ts_us"]
    event_time = data["event_time"]
    trade_time = data["trade_time"]

    check_key_matches_ts(keys, ts_us, report, "trade_updates")
    if not is_sorted_non_decreasing(ts_us):
        report.warn("trade_updates: ts_us is not sorted ascending")
    if bounds:
        check_ts_range(ts_us, bounds[0], bounds[1], report, "trade_updates.ts_us")
        check_ts_range(event_time, bounds[0], bounds[1], report, "trade_updates.event_time", hard_fail=False)
        check_ts_range(trade_time, bounds[0], bounds[1], report, "trade_updates.trade_time", hard_fail=False)
    check_positive(data["price"], report, "trade_updates.price")
    check_non_negative(data["cumulative_filled_quantity"], report, "trade_updates.cumulative_filled_quantity")
    backwards = sum(
        1
        for trade_ts, event_ts, recv_ts in zip(trade_time, event_time, ts_us)
        if (trade_ts is not None and event_ts is not None and trade_ts > event_ts)
        or (event_ts is not None and recv_ts is not None and event_ts > recv_ts)
    )
    if backwards:
        report.warn(f"trade_updates: {backwards} rows have trade_time/event_time/ts_us ordering anomalies")
    for column in ["symbol", "side", "trading_venue", "order_status"]:
        check_enum_presence(data[column], report, f"trade_updates.{column}")

    report.note(
        "trade_updates: "
        f"rows={len(keys)}, symbols={len(set(data['symbol']))}, "
        f"status_top=[{counter_preview(data['order_status'])}], "
        f"is_maker_top=[{counter_preview(data['is_maker'])}]"
    )


def validate_uniform_orders(data: dict[str, list], bounds: tuple[int, int] | None, report: CheckReport) -> None:
    keys = data["key"]
    ts_us = data["ts_us"]
    recv_ts_us = data["recv_ts_us"]
    create_ts = data["create_ts"]
    update_ts = data["update_ts"]
    signal_ts = data["signal_ts"]
    amount_init = data["amount_init"]
    amount_update = data["amount_update"]

    check_key_matches_ts(keys, ts_us, report, "uniform_orders")
    if not is_sorted_non_decreasing(ts_us):
        report.warn("uniform_orders: ts_us is not sorted ascending")
    if bounds:
        check_ts_range(ts_us, bounds[0], bounds[1], report, "uniform_orders.ts_us")
        check_ts_range(recv_ts_us, bounds[0], bounds[1], report, "uniform_orders.recv_ts_us")
        check_ts_range(update_ts, bounds[0], bounds[1], report, "uniform_orders.update_ts", hard_fail=False)
        check_ts_range(create_ts, bounds[0], bounds[1], report, "uniform_orders.create_ts", hard_fail=False)
        check_ts_range(signal_ts, bounds[0], bounds[1], report, "uniform_orders.signal_ts", hard_fail=False)
    check_positive(data["price"], report, "uniform_orders.price")
    check_non_negative(amount_init, report, "uniform_orders.amount_init")
    check_non_negative(amount_update, report, "uniform_orders.amount_update")
    over_init = sum(
        1
        for init, update in zip(amount_init, amount_update)
        if init is not None and update is not None and update > init + 1e-9
    )
    if over_init:
        report.warn(f"uniform_orders: {over_init} rows have amount_update > amount_init")
    bad_order = sum(
        1
        for sig, create, update, recv in zip(signal_ts, create_ts, update_ts, recv_ts_us)
        if (sig is not None and create is not None and sig > create)
        or (create is not None and update is not None and create > update)
        or (update is not None and recv is not None and update > recv)
    )
    if bad_order:
        report.warn(f"uniform_orders: {bad_order} rows have signal/create/update/recv timestamp ordering anomalies")

    bad_hex = 0
    mismatch_hex = 0
    for raw, raw_hex in zip(data["from_key"], data["from_key_hex"]):
        if raw_hex is None:
            continue
        try:
            decoded = binascii.unhexlify(raw_hex).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError):
            bad_hex += 1
            continue
        if raw is not None and decoded != raw:
            mismatch_hex += 1
    if bad_hex:
        report.fail(f"uniform_orders.from_key_hex: {bad_hex} rows are not valid utf-8 hex payloads")
    if mismatch_hex:
        report.fail(f"uniform_orders.from_key_hex: {mismatch_hex} rows do not match from_key")

    for column in ["symbol", "trading_venue", "order_type", "side", "status", "from_key"]:
        check_enum_presence(data[column], report, f"uniform_orders.{column}")

    report.note(
        "uniform_orders: "
        f"rows={len(keys)}, symbols={len(set(data['symbol']))}, "
        f"status_top=[{counter_preview(data['status'])}], "
        f"side_top=[{counter_preview(data['side'])}]"
    )


def validate_cross_table(
    uniform_orders: dict[str, list],
    order_updates: dict[str, list],
    trade_updates: dict[str, list],
    report: CheckReport,
) -> None:
    uniform_ids = {v for v in uniform_orders["client_order_id"] if v not in (None, 0)}
    order_ids = {v for v in order_updates["client_order_id"] if v not in (None, 0)}
    trade_ids = {v for v in trade_updates["client_order_id"] if v not in (None, 0)}

    if uniform_ids:
        order_overlap = len(order_ids & uniform_ids) / max(len(order_ids), 1)
        trade_overlap = len(trade_ids & uniform_ids) / max(len(trade_ids), 1)
        report.note(
            "cross_table: "
            f"order_client_id_overlap={order_overlap:.2%}, "
            f"trade_client_id_overlap={trade_overlap:.2%}"
        )
        if order_ids and order_overlap < 0.2:
            report.warn("cross_table: low overlap between order_updates and uniform_orders client_order_id")
        if trade_ids and trade_overlap < 0.2:
            report.warn("cross_table: low overlap between trade_updates and uniform_orders client_order_id")


def validate_schema(columns: list[str], expected: list[str], report: CheckReport, label: str) -> None:
    if columns != expected:
        report.fail(f"{label}: unexpected schema columns: got={columns}")


def main() -> int:
    args = parse_args()
    export_dir = args.export_dir.expanduser().resolve()
    report = CheckReport()

    if not export_dir.is_dir():
        print(f"export_dir not found: {export_dir}", file=sys.stderr)
        return 2

    bounds = infer_utc_day_bounds(export_dir, args.date)
    if bounds:
        report.note(
            f"expected_utc_window: {bounds[0]}..{bounds[1]} "
            f"({micros_to_utc(bounds[0])} .. {micros_to_utc(bounds[1])})"
        )
    else:
        report.warn("could not infer UTC day window from directory name; timestamp range checks skipped")

    loaded: dict[str, dict[str, list]] = {}
    for filename, expected_columns in EXPECTED_SCHEMAS.items():
        path = export_dir / filename
        if not path.is_file():
            report.fail(f"missing file: {path}")
            continue
        data, columns = load_parquet(path)
        validate_schema(columns, expected_columns, report, filename)
        loaded[filename] = data

    if report.failures:
        print_report(report)
        return 1

    validate_order_updates(loaded["order_updates_unmatched.parquet"], bounds, report)
    validate_trade_updates(loaded["trade_updates_unmatched.parquet"], bounds, report)
    validate_uniform_orders(loaded["uniform_orders.parquet"], bounds, report)
    validate_cross_table(
        loaded["uniform_orders.parquet"],
        loaded["order_updates_unmatched.parquet"],
        loaded["trade_updates_unmatched.parquet"],
        report,
    )

    print_report(report)
    return 1 if report.failures else 0


def print_report(report: CheckReport) -> None:
    print("== Order Export Check ==")
    if report.failures:
        print("FAIL")
        for item in report.failures:
            print(f"  [FAIL] {item}")
    else:
        print("PASS")
    for item in report.warnings:
        print(f"  [WARN] {item}")
    for item in report.notes:
        print(f"  [INFO] {item}")


if __name__ == "__main__":
    raise SystemExit(main())
