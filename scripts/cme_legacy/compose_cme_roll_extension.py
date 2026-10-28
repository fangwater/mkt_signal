"""Compose a validated pre-history extension with an existing CME roll root.

The extension keeps the published continuation from the cutover date onward,
while recalculating the backward cumulative adjustment used by the earlier
HFQ and factor replays.
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import shutil
import tempfile
from datetime import date
from pathlib import Path


PRODUCTS = {
    "ES": ("CME", "ES"),
    "NQ": ("CME", "NQ"),
    "RTY": ("CME", "RTY"),
    "YM": ("CBOT", "YM"),
    "GC": ("COMEX", "GC"),
    "CL": ("NYMEX", "CL"),
}


def parse_day(value: str) -> date:
    return date.fromisoformat(value)


def parse_products(value: str) -> list[str]:
    products = [item.strip().upper() for item in value.split(",") if item.strip()]
    invalid = sorted(set(products) - PRODUCTS.keys())
    if invalid or not products:
        raise ValueError(f"unsupported products: {invalid or value!r}")
    if len(set(products)) != len(products):
        raise ValueError("duplicate products")
    return products


def read_csv(path: Path, key: str) -> tuple[list[str], list[dict[str, str]]]:
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None or key not in reader.fieldnames:
            raise ValueError(f"{path} has no {key} column")
        rows = list(reader)
        if not rows:
            raise ValueError(f"{path} is empty")
        if any(not row[key] for row in rows):
            raise ValueError(f"{path} has an empty {key}")
        if rows != sorted(rows, key=lambda row: row[key]):
            raise ValueError(f"{path} is not sorted by {key}")
        if len({row[key] for row in rows}) != len(rows):
            raise ValueError(f"{path} has duplicate {key} values")
        return list(reader.fieldnames), rows


def split_rows(
    rows: list[dict[str, str]], key: str, cutover: str, before: bool
) -> list[dict[str, str]]:
    selected = [row for row in rows if (row[key] < cutover) == before]
    if len(selected) != len(rows):
        side = "before" if before else "on or after"
        raise ValueError(f"expected every row to be {side} cutover {cutover}")
    return selected


def recompute_cumulative(rows: list[dict[str, str]]) -> None:
    cumulative = 0.0
    for row in reversed(rows):
        if row["skipped"].strip().lower() not in {"false", "0", "f", ""}:
            raise ValueError(
                f"cannot compose skipped adjustment on {row['effective_trading_day']}"
            )
        value = row["adjustment_value"].strip()
        if value:
            parsed = float(value)
            if not math.isfinite(parsed):
                raise ValueError(
                    f"non-finite adjustment on {row['effective_trading_day']}"
                )
            cumulative += parsed
        row["cumulative_factor"] = repr(cumulative)
        row["cumulative_complete"] = "True"


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def compose_product(
    extension_root: Path,
    legacy_root: Path,
    output_root: Path,
    exchange: str,
    product: str,
    cutover: str,
) -> tuple[int, int, str, str]:
    relative = Path(exchange) / product
    dominant_header, extension_dominants = read_csv(
        extension_root / relative / "dominant.csv", "trading_day"
    )
    legacy_dominant_header, legacy_dominants = read_csv(
        legacy_root / relative / "dominant.csv", "trading_day"
    )
    if dominant_header != legacy_dominant_header:
        raise ValueError(f"dominant schema mismatch for {exchange}/{product}")
    extension_dominants = split_rows(
        extension_dominants, "trading_day", cutover, before=True
    )
    legacy_dominants = split_rows(
        legacy_dominants, "trading_day", cutover, before=False
    )
    extension_last = extension_dominants[-1]
    legacy_first = legacy_dominants[0]
    for field in ("contract_id", "ric"):
        if extension_last[field] != legacy_first[field]:
            raise ValueError(
                f"{exchange}/{product} boundary {field} mismatch: "
                f"{extension_last[field]!r} != {legacy_first[field]!r}"
            )

    adjustment_header, extension_adjustments = read_csv(
        extension_root / relative / "adjustment.csv", "effective_trading_day"
    )
    legacy_adjustment_header, legacy_adjustments = read_csv(
        legacy_root / relative / "adjustment.csv", "effective_trading_day"
    )
    if adjustment_header != legacy_adjustment_header:
        raise ValueError(f"adjustment schema mismatch for {exchange}/{product}")
    adjustments = split_rows(
        extension_adjustments, "effective_trading_day", cutover, before=True
    ) + split_rows(
        legacy_adjustments, "effective_trading_day", cutover, before=False
    )
    if adjustments != sorted(adjustments, key=lambda row: row["effective_trading_day"]):
        raise ValueError(f"overlapping adjustment dates for {exchange}/{product}")
    recompute_cumulative(adjustments)

    write_csv(
        output_root / relative / "dominant.csv",
        dominant_header,
        extension_dominants + legacy_dominants,
    )
    write_csv(output_root / relative / "adjustment.csv", adjustment_header, adjustments)
    return (
        len(extension_dominants),
        len(legacy_dominants),
        extension_last["ric"],
        legacy_first["ric"],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--extension-root", type=Path, required=True)
    parser.add_argument("--legacy-root", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--cutover", type=parse_day, required=True)
    parser.add_argument("--products", type=parse_products, default=list(PRODUCTS))
    parser.add_argument("--settlements", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.output_root.exists():
        raise ValueError(f"output root already exists: {args.output_root}")
    if args.output_root == args.extension_root or args.output_root == args.legacy_root:
        raise ValueError("output root must be independent")
    if args.settlements is not None and not args.settlements.is_file():
        raise ValueError(f"settlements file does not exist: {args.settlements}")

    args.output_root.parent.mkdir(parents=True, exist_ok=True)
    temporary_root = Path(
        tempfile.mkdtemp(prefix=f".{args.output_root.name}.partial.", dir=args.output_root.parent)
    )
    try:
        cutover = args.cutover.isoformat()
        audit_rows = []
        for product in args.products:
            exchange, root = PRODUCTS[product]
            extension_days, legacy_days, extension_ric, legacy_ric = compose_product(
                args.extension_root,
                args.legacy_root,
                temporary_root,
                exchange,
                root,
                cutover,
            )
            audit_rows.append(
                {
                    "product": product,
                    "extension_days": extension_days,
                    "legacy_days": legacy_days,
                    "extension_last_ric": extension_ric,
                    "legacy_first_ric": legacy_ric,
                }
            )
        if args.settlements is not None:
            os.link(args.settlements, temporary_root / args.settlements.name)
        write_csv(
            temporary_root / "_audit" / "composition.csv",
            list(audit_rows[0]),
            audit_rows,
        )
        os.replace(temporary_root, args.output_root)
    except BaseException:
        shutil.rmtree(temporary_root, ignore_errors=True)
        raise
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
