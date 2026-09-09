"""Python correctness baseline for venue-partitioned 1-second RAW quotes."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from reference import open_text, iter_messages


MISSING_PRICE = -(1 << 63)
MISSING_SIZE = (1 << 32) - 1
MISSING_CODE = (1 << 16) - 1
QUOTE_RIPPLE_FIELDS = {
    (23, "BID_1"),
    (24, "BID_2"),
    (26, "ASK_1"),
    (27, "ASK_2"),
}
QUOTE_FIELDS = {
    (22, "BID"),
    (25, "ASK"),
    (30, "BIDSIZE"),
    (31, "ASKSIZE"),
    (11683, "BIDFINMMID"),
    (11684, "ASKFINMMID"),
    (3298, "BIDXID"),
    (3297, "ASKXID"),
    (6579, "BID_COND_N"),
    (6580, "ASK_COND_N"),
    (293, "BID_MMID1"),
    (296, "ASK_MMID1"),
    (1000, "GV1_TEXT"),
    (8937, "LIMIT_INDQ"),
    (3887, "SEQNUM_QT"),
    (118, "PRC_QL_CD"),
    (3264, "PRC_QL3"),
    (8406, "QTE_ORIGIN"),
    (1041, "GV1_FLAG"),
    (8935, "RETAIL_INT"),
    (1501, "STOCK_TYPE"),
    (6513, "SETL_TYPE"),
    (6516, "BOOK_STATE"),
    (12783, "NBBO_IND"),
    (3855, "QUOTIM_MS"),
    (1025, "QUOTIM"),
    (14238, "ORDRECV_MS"),
    (14246, "ORDREC2_MS"),
    (14263, "ASK_TIM_NS"),
    (14264, "BID_TIM_NS"),
    (14265, "QUOTIM_NS"),
    (3386, "QUOTE_DATE"),
}


@dataclass(frozen=True)
class QuoteCandidate:
    ric: str
    bucket_ns: int
    source_ts_ns: int
    source_order: int
    bid: int
    bid_size: int
    ask: int
    ask_size: int
    bid_venue: str
    ask_venue: str
    quality_code: int


@dataclass(frozen=True)
class VenueQuote:
    bucket_ns: int
    source_ts_ns: int
    source_order: int
    bid: int
    bid_size: int
    ask: int
    ask_size: int


def _e9(raw: str) -> int:
    if not raw:
        return MISSING_PRICE
    negative = raw.startswith("-")
    digits = raw[1:] if negative else raw
    integer, _, fraction = digits.partition(".")
    if not integer.isdigit() or (fraction and not fraction.isdigit()) or len(fraction) > 9:
        raise ValueError(f"invalid e9 price: {raw!r}")
    scaled = int(integer) * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")
    return -scaled if negative else scaled


def _size(raw: str) -> int:
    return MISSING_SIZE if not raw else int(raw)


def _timestamp_ns(raw: str) -> int:
    if not raw.endswith("Z"):
        raise ValueError(f"expected UTC Date-Time ending in Z: {raw!r}")
    body = raw[:-1]
    whole, dot, fraction = body.partition(".")
    if dot and (not fraction.isdigit() or len(fraction) > 9):
        raise ValueError(f"invalid Date-Time fraction: {raw!r}")
    parsed = datetime.fromisoformat(whole).replace(tzinfo=timezone.utc)
    return int(parsed.timestamp()) * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")


def is_quote_ripple(message: object) -> bool:
    if message.message_class != "UPDATE" or message.update_type != "QUOTE":
        return False
    identities = [(field.fid, field.name) for field in message.fields]
    if not identities or any(identity not in QUOTE_RIPPLE_FIELDS for identity in identities):
        return False
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate Quote ripple FID")
    for field in message.fields:
        if field.enum_value.strip():
            raise ValueError(f"Quote ripple field has enum value: {field.name}")
        _e9(field.value)
    return True


def _quote_millis(raw: str) -> int:
    hour, minute, second = raw.split(":")
    whole_seconds, dot, fraction = second.partition(".")
    if not (hour.isdigit() and minute.isdigit() and whole_seconds.isdigit()):
        raise ValueError(f"invalid QUOTIM: {raw!r}")
    if dot and (not fraction.isdigit() or len(fraction) > 9):
        raise ValueError(f"invalid QUOTIM fraction: {raw!r}")
    total = int(hour) * 3_600_000 + int(minute) * 60_000 + int(whole_seconds) * 1_000
    total += int(fraction.ljust(3, "0")[:3] or "0")
    if total >= 86_400_000:
        raise ValueError(f"QUOTIM is outside one day: {raw!r}")
    return total


def _side_updated(fields: dict[str, object], price: str, size: str) -> bool:
    present = (price in fields, size in fields)
    if present == (True, True):
        return True
    if present == (False, False):
        return False
    side = "bid" if price == "BID" else "ask"
    raise ValueError(f"incomplete {side} Quote side update")


def compress_quotes(path: Path, *, part: int, shard: int) -> dict[tuple[str, int], QuoteCandidate]:
    winners: dict[tuple[str, int], QuoteCandidate] = {}
    with open_text(path) as stream:
        for message in iter_messages(stream):
            if message.message_class != "UPDATE" or message.update_type != "QUOTE":
                continue
            if is_quote_ripple(message):
                continue
            identities = [(field.fid, field.name) for field in message.fields]
            if len(set(identities)) != len(identities):
                raise ValueError(f"duplicate Quote FID at {message.ric} {message.date_time}")
            unknown = set(identities) - QUOTE_FIELDS
            if unknown:
                raise ValueError(
                    f"unsupported Quote FID at {message.ric} {message.date_time}: {sorted(unknown)}"
                )
            fields = {field.name: field for field in message.fields}
            bid_updated = _side_updated(fields, "BID", "BIDSIZE")
            ask_updated = _side_updated(fields, "ASK", "ASKSIZE")
            quality_updated = "PRC_QL_CD" in fields or "PRC_QL3" in fields
            if not bid_updated and not ask_updated and not quality_updated:
                raise ValueError(f"Quote {message.ric} {message.date_time} has neither a side nor quality update")
            source_ts_ns = _timestamp_ns(message.date_time)
            date = fields.get("QUOTE_DATE")
            quote_date = date.value if date and date.value else message.date_time[:10]
            quote_ms = fields.get("QUOTIM_MS")
            if quote_ms and quote_ms.value:
                millis = int(quote_ms.value)
                if millis >= 86_400_000:
                    raise ValueError(f"QUOTIM_MS is outside one day: {millis}")
                midnight = datetime.fromisoformat(quote_date).replace(tzinfo=timezone.utc)
                bucket_ns = int(midnight.timestamp()) * 1_000_000_000 + millis // 1000 * 1_000_000_000
            else:
                quote_time = fields.get("QUOTIM")
                if quote_time and quote_time.value:
                    millis = _quote_millis(quote_time.value)
                    midnight = datetime.fromisoformat(quote_date).replace(tzinfo=timezone.utc)
                    bucket_ns = int(midnight.timestamp()) * 1_000_000_000 + millis // 1000 * 1_000_000_000
                else:
                    bucket_ns = source_ts_ns // 1_000_000_000 * 1_000_000_000
            left = fields["PRC_QL_CD"].value if "PRC_QL_CD" in fields else ""
            right = fields["PRC_QL3"].value if "PRC_QL3" in fields else ""
            if left and right and left != right:
                raise ValueError("quote quality fields disagree")
            order = part << 48 | shard << 32 | message.source_row
            candidate = QuoteCandidate(
                ric=message.ric,
                bucket_ns=bucket_ns,
                source_ts_ns=source_ts_ns,
                source_order=order,
                bid=_e9(fields["BID"].value) if bid_updated else MISSING_PRICE,
                bid_size=_size(fields["BIDSIZE"].value) if bid_updated else MISSING_SIZE,
                ask=_e9(fields["ASK"].value) if ask_updated else MISSING_PRICE,
                ask_size=_size(fields["ASKSIZE"].value) if ask_updated else MISSING_SIZE,
                bid_venue=fields.get("BIDXID").enum_value.strip()
                if bid_updated and fields.get("BIDXID")
                else "",
                ask_venue=fields.get("ASKXID").enum_value.strip()
                if ask_updated and fields.get("ASKXID")
                else "",
                quality_code=int(left or right) if left or right else MISSING_CODE,
            )
            key = (candidate.ric, candidate.bucket_ns)
            prior = winners.get(key)
            if prior is None:
                winners[key] = candidate
            else:
                winners[key] = replace(
                    prior,
                    source_ts_ns=candidate.source_ts_ns,
                    source_order=candidate.source_order,
                    bid=candidate.bid if bid_updated else prior.bid,
                    bid_size=candidate.bid_size if bid_updated else prior.bid_size,
                    ask=candidate.ask if ask_updated else prior.ask,
                    ask_size=candidate.ask_size if ask_updated else prior.ask_size,
                    bid_venue=candidate.bid_venue if bid_updated else prior.bid_venue,
                    ask_venue=candidate.ask_venue if ask_updated else prior.ask_venue,
                    quality_code=candidate.quality_code if quality_updated else prior.quality_code,
                )
    return winners


def route_quotes(
    winners: dict[tuple[str, int], QuoteCandidate],
) -> tuple[dict[str, list[VenueQuote]], dict[str, list[tuple[int, bool, bool, int]]]]:
    venues: dict[str, list[VenueQuote]] = {}
    states: dict[str, list[tuple[int, bool, bool, int]]] = {}
    last_quality: dict[str, int] = {}
    for (ric, bucket), row in sorted(winners.items()):
        bid_clear = (row.bid, row.bid_size) in {
            (MISSING_PRICE, MISSING_SIZE),
            (0, 0),
        }
        ask_clear = (row.ask, row.ask_size) in {
            (MISSING_PRICE, MISSING_SIZE),
            (0, 0),
        }
        def routed(bid: bool, ask: bool) -> VenueQuote:
            return VenueQuote(
                bucket_ns=bucket,
                source_ts_ns=row.source_ts_ns,
                source_order=row.source_order,
                bid=row.bid if bid else MISSING_PRICE,
                bid_size=row.bid_size if bid else MISSING_SIZE,
                ask=row.ask if ask else MISSING_PRICE,
                ask_size=row.ask_size if ask else MISSING_SIZE,
            )

        if (
            not bid_clear
            and not ask_clear
            and row.bid_venue
            and row.bid_venue == row.ask_venue
        ):
            venues.setdefault(f"v:{ric}:{row.bid_venue}", []).append(routed(True, True))
        else:
            if not bid_clear and row.bid_venue:
                venues.setdefault(f"v:{ric}:{row.bid_venue}", []).append(routed(True, False))
            if not ask_clear and row.ask_venue:
                venues.setdefault(f"v:{ric}:{row.ask_venue}", []).append(routed(False, True))
        previous = last_quality.get(ric, MISSING_CODE)
        if bid_clear or ask_clear or row.quality_code != previous:
            states.setdefault(f"i:{ric}", []).append(
                (bucket, bid_clear, ask_clear, row.quality_code)
            )
        last_quality[ric] = row.quality_code
    return venues, states
