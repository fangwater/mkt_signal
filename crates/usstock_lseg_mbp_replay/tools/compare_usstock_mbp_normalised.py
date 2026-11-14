#!/usr/bin/env python3
"""Compare one LSEG MBP venue book with normalised TAS L1 quotes.

The two source products have different message boundaries, so this tool compares
the MBP best bid/ask state causally as of each normalised Quote timestamp.  It
does not attempt a row-for-row comparison.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import gzip
import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator


RAW_TYPE_COL = 4
RAW_MSG_CLASS_COL = 5
RAW_ACTION_COL = 6
RAW_KEY_COL = 12
RAW_SEQUENCE_COL = 12


@dataclass(frozen=True)
class BboEvent:
    ts: str
    ts_ns: int
    source_seq: str
    bid: str | None
    ask: str | None


def parse_utc_ns(value: str) -> int:
    """Parse a UTC ISO-8601 timestamp with up to nine fractional digits."""
    if not value.endswith("Z"):
        raise ValueError(f"expected UTC Z timestamp, got {value!r}")
    main, dot, fraction = value[:-1].partition(".")
    dt = datetime.strptime(main, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
    seconds = calendar.timegm(dt.utctimetuple())
    nanoseconds = int((fraction[:9]).ljust(9, "0")) if dot else 0
    return seconds * 1_000_000_000 + nanoseconds


def normalize_decimal(value: str) -> Decimal | None:
    if value == "":
        return None
    try:
        return Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"invalid decimal {value!r}") from exc


def decode_price_key(key: str) -> tuple[str, str] | None:
    """Decode the price + B/A keys observed in this MBP sample."""
    if len(key) < 2:
        return None
    suffix = key[-1]
    if suffix not in ("B", "A"):
        return None
    price = normalize_decimal(key[:-1])
    if price is None:
        return None
    return ("bid" if suffix == "B" else "ask", key[:-1])


def best_prices(book: dict[str, tuple[str, str]]) -> tuple[str | None, str | None]:
    bids: list[tuple[Decimal, str]] = []
    asks: list[tuple[Decimal, str]] = []
    for side, price_text in book.values():
        price = normalize_decimal(price_text)
        if price is None:
            continue
        if side == "bid":
            bids.append((price, price_text))
        else:
            asks.append((price, price_text))
    bid = max(bids)[1] if bids else None
    ask = min(asks)[1] if asks else None
    return bid, ask


def write_json_line(fh, value: dict) -> None:
    fh.write(json.dumps(value, separators=(",", ":"), ensure_ascii=False))
    fh.write("\n")


def extract_mbp_bbo(
    source: Path,
    ric: str,
    start_ns: int,
    end_ns: int,
    output: Path,
) -> tuple[list[BboEvent], dict]:
    """Replay price keys for one RIC and write BBO-changing events.

    The package is RIC-grouped in the examined extraction.  After the desired
    RIC ends the reader stops, and that source ordering is recorded in output.
    A Raw REFRESH resets state only if it carries one or more MapEntry rows.
    """
    book: dict[str, tuple[str, str]] = {}
    events: list[BboEvent] = []
    last_before_start: BboEvent | None = None
    last_top: tuple[str | None, str | None] | None = None
    current: dict | None = None
    seen_target = False
    stopped_on_ric = ""
    stats = Counter()

    def flush_current() -> None:
        nonlocal current, last_before_start, last_top
        if current is None:
            return
        entries: list[tuple[str, str]] = current["entries"]
        stats["raw_messages"] += 1
        if current["msg_class"] == "REFRESH":
            stats["refresh_messages"] += 1
            if entries:
                book.clear()
                stats["book_images"] += 1
            else:
                stats["refresh_without_map_entries"] += 1
        if not entries:
            current = None
            return

        for action, key in entries:
            decoded = decode_price_key(key)
            if decoded is None:
                stats["unsupported_keys"] += 1
                continue
            side, price = decoded
            if action == "DELETE":
                if key in book:
                    del book[key]
                else:
                    stats["delete_without_prior_key"] += 1
            elif action in ("ADD", "UPDATE"):
                if action == "UPDATE" and key not in book:
                    stats["update_without_prior_key"] += 1
                book[key] = (side, price)
            else:
                stats["unsupported_actions"] += 1
                continue
            stats[f"map_{action.lower()}"] += 1

        top = best_prices(book)
        if top != last_top:
            event = BboEvent(
                ts=current["ts"],
                ts_ns=current["ts_ns"],
                source_seq=current["source_seq"],
                bid=top[0],
                ask=top[1],
            )
            if event.ts_ns < start_ns:
                last_before_start = event
            elif event.ts_ns < end_ns:
                events.append(event)
            last_top = top
            stats["bbo_changes"] += 1
        current = None

    with gzip.open(source, "rt", encoding="utf-8", newline="") as raw_fh, output.open(
        "w", encoding="utf-8"
    ) as out_fh:
        reader = csv.reader(raw_fh)
        header = next(reader, None)
        if header is None or header[:5] != [
            "#RIC",
            "Domain",
            "Date-Time",
            "GMT Offset",
            "Type",
        ]:
            raise ValueError(f"unexpected MBP header in {source}")

        for row in reader:
            row.extend([""] * max(0, 14 - len(row)))
            if row[0] and row[RAW_TYPE_COL] == "Raw":
                flush_current()
                row_ric = row[0]
                if row_ric == ric:
                    seen_target = True
                    ts_ns = parse_utc_ns(row[2])
                    if ts_ns >= end_ns:
                        break
                    current = {
                        "ts": row[2],
                        "ts_ns": ts_ns,
                        "msg_class": row[RAW_MSG_CLASS_COL],
                        "source_seq": row[RAW_SEQUENCE_COL],
                        "entries": [],
                    }
                elif seen_target:
                    stopped_on_ric = row_ric
                    break
                continue
            if current is not None and row[RAW_TYPE_COL] == "MapEntry":
                current["entries"].append((row[RAW_ACTION_COL], row[RAW_KEY_COL]))
        flush_current()

        if last_before_start is not None:
            events.insert(0, last_before_start)
        for event in events:
            write_json_line(
                out_fh,
                {
                    "ts": event.ts,
                    "source_seq": event.source_seq,
                    "bid": event.bid,
                    "ask": event.ask,
                },
            )

    stats["bbo_events_written"] = len(events)
    stats["stopped_on_ric"] = stopped_on_ric
    stats["target_seen"] = seen_target
    stats["final_live_levels"] = len(book)
    return events, dict(stats)


def quote_view(row: list[str], indexes: dict[str, int]) -> dict:
    def value(name: str) -> str:
        return row[indexes[name]] if indexes[name] < len(row) else ""

    return {
        "ts": value("Date-Time"),
        "type": value("Type"),
        "bid": {
            "venue": value("Buyer ID"),
            "price": value("Bid Price"),
            "size": value("Bid Size"),
        },
        "ask": {
            "venue": value("Seller ID"),
            "price": value("Ask Price"),
            "size": value("Ask Size"),
        },
        "seq_no": value("Seq. No."),
        "exch_time": value("Exch Time"),
        "qualifiers": value("Qualifiers"),
    }


def compare_normalised_quotes(
    source: Path,
    ric: str,
    start_ns: int,
    end_ns: int,
    venue: str,
    venue_mode: str,
    bbo_events: list[BboEvent],
    output: Path,
) -> dict:
    """Extract normalised Quote rows and compare same-venue L1 sides as-of MBP."""
    stats = Counter()
    venue_pairs: Counter[tuple[str, str]] = Counter()
    mismatch_examples: list[dict] = []
    match_examples: list[dict] = []
    bbo_index = 0
    current_bbo: BboEvent | None = None
    seen_target = False
    stopped_on_ric = ""

    with gzip.open(source, "rt", encoding="utf-8", newline="") as raw_fh, output.open(
        "w", encoding="utf-8"
    ) as out_fh:
        reader = csv.reader(raw_fh)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"empty normalised source {source}")
        required = {
            "#RIC",
            "Date-Time",
            "Type",
            "Buyer ID",
            "Bid Price",
            "Bid Size",
            "Seller ID",
            "Ask Price",
            "Ask Size",
            "Seq. No.",
            "Exch Time",
            "Qualifiers",
        }
        missing = required - set(header)
        if missing:
            raise ValueError(f"normalised header missing {sorted(missing)}")
        indexes = {name: header.index(name) for name in required}
        ric_index = indexes["#RIC"]
        ts_index = indexes["Date-Time"]

        # The package is RIC-grouped in this extraction.  csv.reader is
        # required here because quoted source fields can contain newlines.
        for row in reader:
            if ric_index >= len(row):
                raise ValueError("normalised row missing #RIC")
            row_ric = row[ric_index]
            if row_ric != ric:
                if seen_target:
                    stopped_on_ric = row_ric
                    break
                continue
            seen_target = True
            if ts_index >= len(row):
                raise ValueError(f"normalised row missing Date-Time for {ric}")
            ts = row[ts_index]
            ts_ns = parse_utc_ns(ts)
            if ts_ns >= end_ns:
                break
            if ts_ns < start_ns:
                continue
            if row[indexes["Type"]] != "Quote":
                continue

            quote = quote_view(row, indexes)
            write_json_line(out_fh, quote)
            stats["quotes"] += 1
            bid_venue = quote["bid"]["venue"]
            ask_venue = quote["ask"]["venue"]
            venue_pairs[(bid_venue, ask_venue)] += 1

            while bbo_index < len(bbo_events) and bbo_events[bbo_index].ts_ns <= ts_ns:
                current_bbo = bbo_events[bbo_index]
                bbo_index += 1
            if current_bbo is None:
                stats["no_prior_mbp_bbo"] += 1
                continue

            if venue_mode == "explicit":
                bid_is_venue = bid_venue == venue
                ask_is_venue = ask_venue == venue
            elif venue_mode == "implicit_ric":
                # This is valid only for a RIC whose venue was established
                # externally (for example ARKG.BAT -> BZX in the MBP Report).
                bid_is_venue = bid_venue == ""
                ask_is_venue = ask_venue == ""
                if bid_is_venue and ask_is_venue:
                    stats["implicit_ric_venue_quotes"] += 1
            else:
                raise ValueError(f"unsupported venue mode {venue_mode!r}")
            if bid_is_venue:
                stats["venue_bid_quotes"] += 1
            if ask_is_venue:
                stats["venue_ask_quotes"] += 1
            if not (bid_is_venue and ask_is_venue):
                continue

            stats["same_venue_quotes"] += 1
            norm_bid = normalize_decimal(quote["bid"]["price"])
            norm_ask = normalize_decimal(quote["ask"]["price"])
            mbp_bid = normalize_decimal(current_bbo.bid or "")
            mbp_ask = normalize_decimal(current_bbo.ask or "")
            if norm_bid is None or norm_ask is None or mbp_bid is None or mbp_ask is None:
                stats["incomplete_bbo"] += 1
                continue

            stats["comparable_quotes"] += 1
            bid_match = norm_bid == mbp_bid
            ask_match = norm_ask == mbp_ask
            if bid_match:
                stats["bid_match"] += 1
            if ask_match:
                stats["ask_match"] += 1
            detail = {
                "quote_ts": quote["ts"],
                "quote_seq_no": quote["seq_no"],
                "mbp_asof_ts": current_bbo.ts,
                "mbp_source_seq": current_bbo.source_seq,
                "asof_lag_ns": ts_ns - current_bbo.ts_ns,
                "normalised_bid": quote["bid"]["price"],
                "mbp_bid": current_bbo.bid,
                "normalised_ask": quote["ask"]["price"],
                "mbp_ask": current_bbo.ask,
                "bid_match": bid_match,
                "ask_match": ask_match,
            }
            if bid_match and ask_match:
                stats["both_match"] += 1
                if len(match_examples) < 10:
                    match_examples.append(detail)
            else:
                stats["any_side_mismatch"] += 1
                if len(mismatch_examples) < 10:
                    mismatch_examples.append(detail)

    stats["target_seen"] = seen_target
    stats["stopped_on_ric"] = stopped_on_ric
    stats["venue_pairs"] = [
        {"bid_venue": bid, "ask_venue": ask, "count": count}
        for (bid, ask), count in venue_pairs.most_common()
    ]
    stats["match_examples"] = match_examples
    stats["mismatch_examples"] = mismatch_examples
    return dict(stats)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mbp", required=True, type=Path, help="MBP merged-Data.csv.gz")
    parser.add_argument(
        "--normalised", required=True, type=Path, help="normalised TAS merged-Data.csv.gz"
    )
    parser.add_argument("--ric", required=True, help="same RIC in both source files")
    parser.add_argument(
        "--venue",
        required=True,
        help="logical venue being compared; in explicit mode it is Buyer/Seller ID",
    )
    parser.add_argument(
        "--venue-mode",
        choices=("explicit", "implicit_ric"),
        default="explicit",
        help="use explicit Buyer/Seller IDs, or blank sides on a venue-specific RIC",
    )
    parser.add_argument("--start", required=True, help="UTC ISO-8601 inclusive timestamp")
    parser.add_argument("--end", required=True, help="UTC ISO-8601 exclusive timestamp")
    parser.add_argument("--out-dir", required=True, type=Path, help="derived output directory")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    start_ns = parse_utc_ns(args.start)
    end_ns = parse_utc_ns(args.end)
    if end_ns <= start_ns:
        raise ValueError("--end must be after --start")
    args.out_dir.mkdir(parents=True, exist_ok=True)

    mbp_events, mbp_stats = extract_mbp_bbo(
        source=args.mbp,
        ric=args.ric,
        start_ns=start_ns,
        end_ns=end_ns,
        output=args.out_dir / "mbp_bbo.jsonl",
    )
    normalised_stats = compare_normalised_quotes(
        source=args.normalised,
        ric=args.ric,
        start_ns=start_ns,
        end_ns=end_ns,
        venue=args.venue,
        venue_mode=args.venue_mode,
        bbo_events=mbp_events,
        output=args.out_dir / "normalised_quotes.jsonl",
    )
    report = {
        "ric": args.ric,
        "normalised_venue": args.venue,
        "normalised_venue_mode": args.venue_mode,
        "start": args.start,
        "end": args.end,
        "mbp": mbp_stats,
        "normalised": normalised_stats,
        "source": {"mbp": str(args.mbp), "normalised": str(args.normalised)},
    }
    (args.out_dir / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
