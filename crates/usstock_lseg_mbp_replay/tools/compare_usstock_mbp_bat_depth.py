#!/usr/bin/env python3
"""Compare same-RIC .BAT normalised L1 with MBP price maps by exchange time."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path


def parse_utc_ns(value: str) -> int:
    main, fraction = value[:-1].split(".")
    seconds = int(
        datetime.strptime(main, "%Y-%m-%dT%H:%M:%S")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    return seconds * 1_000_000_000 + int(fraction[:9].ljust(9, "0"))


def effective_ts(day: str, clock: str) -> str:
    return f"{day}T{clock}Z"


def decode_key(key: str) -> tuple[str, str] | None:
    if len(key) < 2 or key[-1] not in ("A", "B"):
        return None
    try:
        Decimal(key[:-1])
    except Exception:
        return None
    return ("ask" if key[-1] == "A" else "bid", key[:-1])


def apply(book: dict[str, tuple[str, str]], event: dict) -> None:
    if event["kind"] == "reset":
        book.clear()
        return
    decoded = decode_key(event["key"])
    if decoded is None:
        return
    if event["action"] == "DELETE":
        book.pop(event["key"], None)
    elif event["action"] in ("ADD", "UPDATE"):
        book[event["key"]] = decoded


def sorted_levels(book: dict[str, tuple[str, str]]) -> tuple[list[tuple[Decimal, str]], list[tuple[Decimal, str]]]:
    bids = sorted(
        ((Decimal(price), price) for side, price in book.values() if side == "bid"),
        reverse=True,
    )
    asks = sorted((Decimal(price), price) for side, price in book.values() if side == "ask")
    return bids, asks


def load_quotes(path: Path, rics: set[str], start: str, end: str) -> dict[str, list[dict]]:
    quotes: dict[str, list[dict]] = defaultdict(list)
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            ric = row["#RIC"]
            if ric not in rics or row["Type"] != "Quote" or not row["Exch Time"]:
                continue
            timestamp = effective_ts(row["Date-Time"][:10], row["Exch Time"])
            if not (start <= timestamp < end):
                continue
            try:
                bid = Decimal(row["Bid Price"])
                ask = Decimal(row["Ask Price"])
            except Exception:
                continue
            if bid <= 0 or ask <= 0:
                continue
            quotes[ric].append(
                {
                    "event_ns": parse_utc_ns(timestamp),
                    "event_ts": timestamp,
                    "outer_ts": row["Date-Time"],
                    "bid": bid,
                    "ask": ask,
                    "buyer_id": row["Buyer ID"].strip(),
                    "seller_id": row["Seller ID"].strip(),
                }
            )
    for rows in quotes.values():
        rows.sort(key=lambda row: (row["event_ns"], row["outer_ts"]))
    return quotes


def actions_for_messages(messages: list[dict]) -> list[dict]:
    events: list[dict] = []
    for message in messages:
        actions: list[dict] = []
        for entry in message["entries"]:
            day = entry["date"] or message["outer"][:10]
            clock = entry["time"] or message["timact"] or message["outer"][11:]
            timestamp = effective_ts(day, clock)
            actions.append(
                {
                    "kind": "action",
                    "event_ns": parse_utc_ns(timestamp),
                    "event_ts": timestamp,
                    "outer_order": message["outer_order"],
                    "entry_order": entry["entry_order"],
                    "action": entry["action"],
                    "key": entry["key"],
                }
            )
        if message["msg_class"] == "REFRESH" and actions:
            reset = min(actions, key=lambda action: action["event_ns"])
            events.append(
                {
                    "kind": "reset",
                    "event_ns": reset["event_ns"],
                    "event_ts": reset["event_ts"],
                    "outer_order": message["outer_order"],
                    "entry_order": -1,
                }
            )
        events.extend(actions)
    return sorted(events, key=lambda event: (event["event_ns"], event["outer_order"], event["entry_order"]))


def analyze(ric: str, messages: list[dict], quotes: list[dict], start: str, end: str) -> dict:
    events = actions_for_messages(messages)
    book: dict[str, tuple[str, str]] = {}
    event_index = quote_index = 0
    quote: dict | None = None
    stats: Counter[str] = Counter()
    bid_depth: list[int] = []
    ask_depth: list[int] = []
    start_second = parse_utc_ns(start) // 1_000_000_000
    end_second = parse_utc_ns(end) // 1_000_000_000

    for second in range(start_second, end_second):
        cutoff = (second + 1) * 1_000_000_000
        while event_index < len(events) and events[event_index]["event_ns"] < cutoff:
            apply(book, events[event_index])
            event_index += 1
        while quote_index < len(quotes) and quotes[quote_index]["event_ns"] < cutoff:
            quote = quotes[quote_index]
            quote_index += 1
        if quote is None:
            continue
        bids, asks = sorted_levels(book)
        if not bids or not asks:
            continue
        stats["comparable_seconds"] += 1
        bid_depth.append(len(bids))
        ask_depth.append(len(asks))
        bid_prices = {price for _, price in bids}
        ask_prices = {price for _, price in asks}
        bid = str(quote["bid"])
        ask = str(quote["ask"])
        bid_class = "top" if bid == bids[0][1] else "deep" if bid in bid_prices else "absent"
        ask_class = "top" if ask == asks[0][1] else "deep" if ask in ask_prices else "absent"
        stats[f"bid_{bid_class}"] += 1
        stats[f"ask_{ask_class}"] += 1
        stats[f"pair_{bid_class}_{ask_class}"] += 1
        stats["extra_levels"] += len(bids) + len(asks) - 2

    count = stats["comparable_seconds"]
    if not count:
        return {"ric": ric, "quote_rows": len(quotes), "comparable_seconds": 0}
    return {
        "ric": ric,
        "quote_rows": len(quotes),
        "comparable_seconds": count,
        "bid": {key: stats[f"bid_{key}"] for key in ("top", "deep", "absent")},
        "ask": {key: stats[f"ask_{key}"] for key in ("top", "deep", "absent")},
        "both_top": stats["pair_top_top"],
        "both_top_pct": round(100 * stats["pair_top_top"] / count, 3),
        "mbp_depth": {
            "bid_min": min(bid_depth),
            "bid_max": max(bid_depth),
            "bid_mean": round(sum(bid_depth) / count, 3),
            "ask_min": min(ask_depth),
            "ask_max": max(ask_depth),
            "ask_mean": round(sum(ask_depth) / count, 3),
            "extra_levels_mean": round(stats["extra_levels"] / count, 3),
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mbp", required=True, type=Path)
    parser.add_argument("--normalised", required=True, type=Path)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--ric", required=True, nargs="+")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rics = set(args.ric)
    quotes = load_quotes(args.normalised, rics, args.start, args.end)
    messages: dict[str, list[dict]] = defaultdict(list)
    current: dict | None = None
    active_ric: str | None = None
    outer_order = 0
    results: dict[str, dict] = {}

    def flush() -> None:
        nonlocal current, outer_order
        if current is not None and current["entries"]:
            current["outer_order"] = outer_order
            outer_order += 1
            messages[current["ric"]].append(current)
        current = None

    def finish_ric(ric: str) -> None:
        if ric not in results:
            results[ric] = analyze(ric, messages.pop(ric, []), quotes.get(ric, []), args.start, args.end)

    with gzip.open(args.mbp, "rt", encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh)
        next(reader)
        for row in reader:
            row += [""] * max(0, 14 - len(row))
            if row[0] and row[4] == "Raw":
                flush()
                row_ric = row[0]
                if active_ric is not None and row_ric != active_ric:
                    finish_ric(active_ric)
                    active_ric = None
                if row_ric not in rics:
                    continue
                if row[2] >= args.end:
                    continue
                active_ric = row_ric
                current = {
                    "ric": row_ric,
                    "outer": row[2],
                    "msg_class": row[5],
                    "timact": "",
                    "entries": [],
                    "active_entry": None,
                }
                continue
            if current is None:
                continue
            if row[4] == "MapEntry":
                entry = {"action": row[6], "key": row[12], "date": "", "time": "", "entry_order": len(current["entries"])}
                current["entries"].append(entry)
                current["active_entry"] = entry
            elif row[4] == "FID":
                if row[7] == "TIMACT_NS":
                    current["timact"] = row[8]
                if current["active_entry"] is not None:
                    if row[7] == "LV_DATE":
                        current["active_entry"]["date"] = row[8]
                    elif row[7] == "LV_TIM_NS":
                        current["active_entry"]["time"] = row[8]
        flush()
    if active_ric is not None:
        finish_ric(active_ric)
    for ric in rics:
        finish_ric(ric)

    output = {
        "start": args.start,
        "end": args.end,
        "clock_alignment": "normalised Exch Time vs MBP per-level LV_TIM_NS; deletes use TIMACT_NS",
        "results": [results[ric] for ric in sorted(rics)],
    }
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
