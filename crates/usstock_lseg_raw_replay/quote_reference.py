"""Python correctness baseline for venue-partitioned 1-second RAW quotes."""

from __future__ import annotations

from dataclasses import dataclass
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


def compress_quotes(path: Path, *, part: int, shard: int) -> dict[tuple[str, int], QuoteCandidate]:
    winners: dict[tuple[str, int], QuoteCandidate] = {}
    with open_text(path) as stream:
        for message in iter_messages(stream):
            if message.message_class != "UPDATE" or message.update_type != "QUOTE":
                continue
            if is_quote_ripple(message):
                continue
            fields = {field.name: field for field in message.fields}
            source_ts_ns = _timestamp_ns(message.date_time)
            date = fields.get("QUOTE_DATE")
            quote_date = date.value if date and date.value else message.date_time[:10]
            quote_ms = int(fields["QUOTIM_MS"].value)
            midnight = datetime.fromisoformat(quote_date).replace(tzinfo=timezone.utc)
            bucket_ns = int(midnight.timestamp()) * 1_000_000_000 + quote_ms // 1000 * 1_000_000_000
            left = fields["PRC_QL_CD"].value
            right = fields["PRC_QL3"].value
            if left and right and left != right:
                raise ValueError("quote quality fields disagree")
            order = part << 48 | shard << 32 | message.source_row
            candidate = QuoteCandidate(
                ric=message.ric,
                bucket_ns=bucket_ns,
                source_ts_ns=source_ts_ns,
                source_order=order,
                bid=_e9(fields["BID"].value),
                bid_size=_size(fields["BIDSIZE"].value),
                ask=_e9(fields["ASK"].value),
                ask_size=_size(fields["ASKSIZE"].value),
                bid_venue=fields["BIDXID"].enum_value.strip(),
                ask_venue=fields["ASKXID"].enum_value.strip(),
                quality_code=int(left or right) if left or right else MISSING_CODE,
            )
            key = (candidate.ric, candidate.bucket_ns)
            prior = winners.get(key)
            if prior is None or (candidate.source_ts_ns, candidate.source_order) >= (
                prior.source_ts_ns,
                prior.source_order,
            ):
                winners[key] = candidate
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

        if not bid_clear and not ask_clear and row.bid_venue == row.ask_venue:
            venues.setdefault(f"v:{ric}:{row.bid_venue}", []).append(routed(True, True))
        else:
            if not bid_clear:
                venues.setdefault(f"v:{ric}:{row.bid_venue}", []).append(routed(True, False))
            if not ask_clear:
                venues.setdefault(f"v:{ric}:{row.ask_venue}", []).append(routed(False, True))
        previous = last_quality.get(ric, MISSING_CODE)
        if bid_clear or ask_clear or row.quality_code != previous:
            states.setdefault(f"i:{ric}", []).append(
                (bucket, bid_clear, ask_clear, row.quality_code)
            )
        last_quality[ric] = row.quality_code
    return venues, states
