"""Bounded, read-only RAW trade direction evidence census."""

import argparse
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reference import iter_messages, open_text
from event_reference import classify_trade_direction


SAMPLES = [
    ("AAPL.O", 0, 0, "2021-07-01T13:30:00"),
    ("GOOG.O", 0, 255, "2021-07-14T16:27:00"),
    ("QQQ.O", 1, 224, "2021-07-02T15:40:00"),
    ("ABBV.N", 0, 79, "2021-07-22T14:48:00"),
    ("SPY.P", 2, 194, "2021-07-06T14:44:00"),
    ("ARKG.BAT", 0, 161, "2022-02-24T17:21:00"),
    ("ARKK.BAT", 0, 162, "2021-11-01T18:33:00"),
]


def audit(root, ric, part, shard, start):
    end = (datetime.fromisoformat(start) + timedelta(minutes=10)).isoformat()
    path = root / f"merged-Data-part-{part:06}-shard-{shard:06}.csv.zst"
    summary = defaultdict(Counter)
    venues = defaultdict(Counter)
    sides = Counter()
    reached_end = False
    last_seen = None
    with open_text(path) as stream:
        for message in iter_messages(stream):
            if message.ric != ric:
                continue
            last_seen = message.date_time
            if message.date_time >= end:
                reached_end = True
                break
            if message.date_time < start or message.update_type != "TRADE":
                continue
            fields = {field.name: field for field in message.fields}
            normal = "TRDPRC_1" in fields
            price_name, size_name, venue_name = (
                ("TRDPRC_1", "TRDVOL_1", "TRADE_EXID") if normal
                else ("IRGPRC", "IRGVOL", "IRG_EXID")
            )
            if price_name not in fields or size_name not in fields:
                raise ValueError(f"incomplete trade at {message.date_time}")
            size = int(fields[size_name].value)
            venue_field = fields.get(venue_name)
            venue = venue_field.enum_value.strip() if venue_field else ""
            venue_source = "explicit_exid" if venue_field else "missing_exid"
            if venue_field is None and ric.endswith(".BAT"):
                venue, venue_source = "BAT", "venue_specific_ric"
            order_side = fields.get("ORDER_SIDE")
            raw_side = order_side.value.strip() if order_side else ""
            side = int(raw_side) if raw_side else 65535
            _, _, venue_class = classify_trade_direction(venue, side)
            evidence = "order_side_present" if side in (1, 2) else "direction_missing"
            category = {0: "unknown_venue", 1: "exchange", 2: "reporting_facility"}[venue_class]
            key = f"{category}:{evidence}"
            summary[key]["count"] += 1
            summary[key]["volume"] += size
            venues[f"{venue or 'UNKNOWN'}:{venue_source}"]["count"] += 1
            venues[f"{venue or 'UNKNOWN'}:{venue_source}"][evidence] += 1
            if order_side:
                sides[f"{raw_side}:{order_side.enum_value}"] += 1
    if not reached_end:
        raise ValueError(f"incomplete sample {ric}: stopped at {last_seen}, expected {end}")
    return dict(ric=ric, start_utc=start+'Z', end_utc=end+'Z', source=str(path),
                summary=dict(summary), venues=dict(venues), order_side_values=dict(sides))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--staging-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise FileExistsError(args.output)
    results = []
    for sample in SAMPLES:
        result = audit(args.staging_root, *sample)
        results.append(result)
        print(json.dumps(result), flush=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as stream:
        json.dump(dict(created_utc=datetime.now(timezone.utc).isoformat(),
                       definition="ORDER_SIDE present is evidence, not verified aggressor side; source trade updates without correction netting",
                       samples=results), stream, indent=2)


if __name__ == "__main__":
    main()
