"""Correctness oracle for unfiltered TAS daily inventory (not publishable volume).

Trade-day labels here use the regular Chicago 17:00 boundary only. Official
holiday/session filtering and historical availability certification are separate.
No missing contract/day is manufactured as a zero-volume day.
"""
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

NS = 10**9
DAY = 86400 * NS
CHICAGO = ZoneInfo("America/Chicago")


def event_time(source_ns, exchange_hms_ns):
    if exchange_hms_ns is None:
        return source_ns
    if not 0 <= exchange_hms_ns < DAY:
        raise ValueError("exchange time outside UTC day")
    value = source_ns // DAY * DAY + exchange_hms_ns
    if value > source_ns + DAY // 2:
        value -= DAY
    elif source_ns > value + DAY // 2:
        value += DAY
    return value


def aggregate(events):
    output = {}
    for r in events:
        event = event_time(r["published_ns"], r["exchange_hms_ns"])
        local = datetime.fromtimestamp(event // NS, timezone.utc).astimezone(CHICAGO)
        label = (local.date() + timedelta(days=local.hour >= 17)).isoformat()
        key = r["ric"], label
        a = output.setdefault(key, dict(ric=r["ric"], trading_day=label, volume=0,
                                       trades=0, invalid_price=0, max_published_ns=0,
                                       min_event_ns=event, max_event_ns=event,
                                       exchange_time_fallback=0, late_after_1655=0))
        a["max_published_ns"] = max(a["max_published_ns"], r["published_ns"])
        a["min_event_ns"] = min(a["min_event_ns"], event)
        a["max_event_ns"] = max(a["max_event_ns"], event)
        a["exchange_time_fallback"] += r["exchange_hms_ns"] is None
        cutoff = int(datetime.fromisoformat(label + "T16:55:00").replace(tzinfo=CHICAGO).timestamp()) * NS
        a["late_after_1655"] += r["published_ns"] >= cutoff
        if r["price_e9"] is None or r["price_e9"] <= 0:
            a["invalid_price"] += 1
            continue
        if type(r["volume"]) is not int or r["volume"] <= 0:
            raise ValueError("printable trade requires positive integer volume")
        a["volume"] += r["volume"]
        a["trades"] += 1
    return [output[k] for k in sorted(output)]
