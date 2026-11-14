#!/usr/bin/env python3
"""Audit normalised Quote Buyer ID / Seller ID presence patterns."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import Counter
from pathlib import Path


SAMPLE_FIELDS = (
    "#RIC",
    "Date-Time",
    "Exch Time",
    "Buyer ID",
    "Bid Price",
    "Bid Size",
    "No. Buyers",
    "Seller ID",
    "Ask Price",
    "Ask Size",
    "No. Sellers",
    "Seq. No.",
    "Qualifiers",
)


def classify(row: dict[str, str]) -> str:
    bid_venue = row["Buyer ID"].strip()
    ask_venue = row["Seller ID"].strip()
    has_bid_venue = bool(bid_venue)
    has_ask_venue = bool(ask_venue)
    if has_bid_venue and has_ask_venue:
        return "both_same" if bid_venue == ask_venue else "both_different"
    if has_bid_venue:
        return "bid_only"
    if has_ask_venue:
        return "ask_only"
    return "both_empty"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    counts: Counter[str] = Counter()
    examples: dict[str, dict[str, str]] = {}
    quote_rows = 0
    with gzip.open(args.input, "rt", encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        missing = set(SAMPLE_FIELDS) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"input missing fields: {sorted(missing)}")
        for row in reader:
            if row["Type"] != "Quote":
                continue
            quote_rows += 1
            kind = classify(row)
            counts[kind] += 1
            examples.setdefault(
                kind,
                {
                    **{field: row[field] for field in SAMPLE_FIELDS},
                    "Buyer ID normalized": row["Buyer ID"].strip(),
                    "Seller ID normalized": row["Seller ID"].strip(),
                },
            )

    result = {
        "input": str(args.input),
        "quote_rows": quote_rows,
        "counts": dict(counts),
        "examples": examples,
    }
    args.output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
