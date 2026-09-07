"""Python correctness baseline for fixed LSEG RAW event values."""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import Iterable, Mapping

from reference import Field, Message


TRADE = struct.Struct("<QQIIqQQQ4sHH")
CORRECTION = struct.Struct("<QQIIqQQQ4sHHi4x")
SLOT_VALUE_LEN = 24
SLOT_ENUM_LEN = 8
SLOT_LEN = SLOT_VALUE_LEN + SLOT_ENUM_LEN
SLOT_HEADER = struct.Struct("<QQ")
RANGE_FIELDS = {
    (90, "YRHIGH"),
    (91, "YRLOW"),
    (110, "YCHIGH_IND"),
    (111, "YCLOW_IND"),
    (350, "YRHIGHDAT"),
    (351, "YRLOWDAT"),
    (1075, "YRHI_IND"),
    (1076, "YRLO_IND"),
    (3265, "52WK_HIGH"),
    (3266, "52WK_LOW"),
    (3448, "52W_HDAT"),
    (3449, "52W_HIND"),
    (3450, "52W_LDAT"),
    (3451, "52W_LIND"),
}


def is_empty_closing_run(message: Message) -> bool:
    return (
        message.message_class == "UPDATE"
        and message.update_type == "CLOSING_RUN"
        and bool(message.fields)
        and all(not field.value and not field.enum_value for field in message.fields)
    )


def is_range_update_only(message: Message) -> bool:
    return (
        message.message_class == "UPDATE"
        and message.update_type == "UNSPECIFIED"
        and bool(message.fields)
        and all(
            (field.fid, field.name) in RANGE_FIELDS
            for field in message.fields
        )
    )


@dataclass(frozen=True)
class TradeValue:
    source_ts_utc_ns: int
    source_order: int
    event_ms: int
    exchange_id: int
    price: int
    size: int
    trade_id: int
    sequence: int
    condition: bytes
    flags: int
    quality_code: int

    def encode(self) -> bytes:
        return TRADE.pack(
            self.source_ts_utc_ns, self.source_order, self.event_ms,
            self.exchange_id, self.price, self.size, self.trade_id,
            self.sequence, self.condition, self.flags, self.quality_code,
        )


@dataclass(frozen=True)
class CorrectionValue:
    source_ts_utc_ns: int
    source_order: int
    event_ms: int
    exchange_id: int
    price: int
    size: int
    trade_id: int
    sequence: int
    condition: bytes
    condition_code: int
    flags: int
    trade_date_days: int

    def encode(self) -> bytes:
        return CORRECTION.pack(
            self.source_ts_utc_ns, self.source_order, self.event_ms,
            self.exchange_id, self.price, self.size, self.trade_id,
            self.sequence, self.condition, self.condition_code, self.flags,
            self.trade_date_days,
        )


def _ascii_slot(value: str, width: int, name: str) -> bytes:
    encoded = value.encode("ascii")
    if b"\0" in encoded or len(encoded) > width:
        raise ValueError(f"{name} exceeds its {width}-byte fixed slot")
    return encoded.ljust(width, b"\0")


def encode_exact_slots(
    source_ts_utc_ns: int,
    source_order: int,
    fields: Mapping[str, Field],
    layout_fields: Iterable[str],
) -> bytes:
    output = bytearray(SLOT_HEADER.pack(source_ts_utc_ns, source_order))
    for name in layout_fields:
        field = fields.get(name)
        if field is None:
            output.extend(b"\xff" * SLOT_LEN)
        else:
            output.extend(_ascii_slot(field.value, SLOT_VALUE_LEN, name))
            output.extend(_ascii_slot(field.enum_value, SLOT_ENUM_LEN, name))
    return bytes(output)
