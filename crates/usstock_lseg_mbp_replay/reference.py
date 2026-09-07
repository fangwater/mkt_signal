#!/usr/bin/env python3
"""Correctness reference for the compact LSEG US-stock MBP event codec."""

from __future__ import annotations

import argparse
import csv
import json
import struct
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterator


HEADER = [
    "#RIC", "Domain", "Date-Time", "GMT Offset", "Type",
    "MsgClass/FID number", "UpdateType/Action", "FID Name", "FID Value",
    "FID Enum String", "PE Code", "Template Number",
    "Key/Msg Sequence Number", "Number of FIDs",
]
SUMMARY_FIDS = [
    (1, "PROD_PERM"), (3, "DSPLY_NAME"), (15, "CURRENCY"),
    (17, "ACTIV_DATE"), (53, "TRD_UNITS"), (78, "OFFCL_CODE"),
    (198, "LOT_SIZE_A"), (259, "RECORDTYPE"), (1709, "RDN_EXCHD2"),
    (3183, "LIST_MKT"), (3422, "PROV_SYMB"), (3423, "PR_RNK_RUL"),
    (3425, "OR_RNK_RUL"), (3694, "MNEMONIC"), (3984, "TRD_TYPE"),
    (4148, "TIMACT_MS"), (5357, "CONTEXT_ID"), (6401, "DDS_DSO_ID"),
    (6480, "SPS_SP_RIC"), (6516, "BOOK_STATE"), (6519, "MKT_OR_RUL"),
    (6614, "TRD_STATUS"), (6618, "HALT_DATE"), (6619, "HALT_TIME"),
    (14269, "TIMACT_NS"), (14319, "HALT_TM_NS"),
]
SUMMARY_INDEX = {fid: index for index, (fid, _) in enumerate(SUMMARY_FIDS)}
SUMMARY_NAMES = dict(SUMMARY_FIDS)
ENTRY_FIDS = {
    3427: "ORDER_PRC", 3428: "ORDER_SIDE", 3430: "NO_ORD",
    4356: "ACC_SIZE", 6527: "LV_TIM_MS", 6528: "LV_TIM_MSP",
    6529: "LV_DATE", 14268: "LV_TIM_NS",
}
MESSAGE_CLASS = {"REFRESH": 1, "UPDATE": 2, "STATUS": 3}
ACTION = {"ADD": 1, "UPDATE": 2, "DELETE": 3}
MAGIC = b"MB\x02"
KIND_LOGICAL_MESSAGE = 1
VALUE_HEADER_LEN = 52


@dataclass(frozen=True)
class SummaryField:
    fid: int
    value: str | None
    enum_value: str | None


@dataclass(frozen=True)
class LevelDelta:
    action: str
    side: int
    price_e9: int
    no_ord: int | None = None
    acc_size: int | None = None
    level_time_ms: int | None = None
    level_time_msp: int | None = None
    level_date: int | None = None
    level_time_ns: int | None = None


@dataclass(frozen=True)
class Message:
    ric: str
    ts_utc_ns: int
    source_row: int
    source_sequence: int | None
    gmt_offset_minutes: int
    message_class: str
    update_type: str
    pe_code: int
    template_number: int | None
    summary: tuple[SummaryField, ...] | None
    entries: tuple[LevelDelta, ...]

    @property
    def is_book_image(self) -> bool:
        return self.message_class == "REFRESH" and bool(self.entries)


def _timestamp_ns(raw: str) -> int:
    if not raw.endswith("Z"):
        raise ValueError(f"timestamp is not UTC: {raw!r}")
    body = raw[:-1]
    whole, _, fraction = body.partition(".")
    if len(fraction) > 9 or (fraction and not fraction.isdigit()):
        raise ValueError(f"invalid timestamp: {raw!r}")
    seconds = int(datetime.strptime(whole, "%Y-%m-%dT%H:%M:%S")
                  .replace(tzinfo=timezone.utc).timestamp())
    return seconds * 1_000_000_000 + int(fraction.ljust(9, "0") or "0")


def _hms_ns(raw: str) -> int:
    whole, _, fraction = raw.partition(".")
    parts = whole.split(":")
    if len(parts) != 3 or len(fraction) > 9:
        raise ValueError(f"invalid time: {raw!r}")
    hour, minute, second = map(int, parts)
    if minute >= 60 or second >= 60 or (fraction and not fraction.isdigit()):
        raise ValueError(f"invalid time: {raw!r}")
    return (hour * 3600 + minute * 60 + second) * 1_000_000_000 + int(
        fraction.ljust(9, "0") or "0"
    )


def _decimal_e9(raw: str) -> int:
    scaled = Decimal(raw) * 1_000_000_000
    integral = scaled.to_integral_exact()
    if scaled != integral or not -(1 << 63) <= integral < (1 << 63):
        raise ValueError(f"invalid e9 decimal: {raw!r}")
    return int(integral)


def _date_yyyymmdd(raw: str) -> int:
    parsed = datetime.strptime(raw, "%Y-%m-%d")
    return parsed.year * 10_000 + parsed.month * 100 + parsed.day


def _parse_fid(row: list[str]) -> tuple[int, str, str, str]:
    fid = int(row[5])
    return fid, row[7], row[8], row[9]


def _parse_summary(rows: list[list[str]]) -> tuple[SummaryField, ...]:
    fields: dict[int, SummaryField] = {}
    for row in rows:
        fid, name, value, enum_value = _parse_fid(row)
        if SUMMARY_NAMES.get(fid) != name or fid in fields:
            raise ValueError(f"unknown/mismatched/duplicate Summary FID {fid}:{name}")
        fields[fid] = SummaryField(fid, value or None, enum_value or None)
    return tuple(fields[fid] for fid, _ in SUMMARY_FIDS if fid in fields)


def _parse_entry(action: str, key: str, rows: list[list[str]]) -> LevelDelta:
    if action not in ACTION or len(key) < 2 or key[-1] not in "BA":
        raise ValueError(f"invalid MapEntry {action!r}/{key!r}")
    side = 1 if key[-1] == "B" else 2
    key_price = _decimal_e9(key[:-1])
    if action == "DELETE":
        if rows:
            raise ValueError("DELETE has child FIDs")
        return LevelDelta(action, side, key_price)
    if len(rows) not in (7, 8):
        raise ValueError("ADD/UPDATE must have seven or eight child FIDs")
    fields: dict[int, tuple[str, str]] = {}
    for row in rows:
        fid, name, value, enum_value = _parse_fid(row)
        if ENTRY_FIDS.get(fid) != name or fid in fields or not value:
            raise ValueError(f"invalid MapEntry FID {fid}:{name}")
        fields[fid] = (value, enum_value)
    if set(fields) != set(list(ENTRY_FIDS)[:7]) | ({14268} if len(rows) == 8 else set()):
        raise ValueError("MapEntry FID set mismatch")
    price = _decimal_e9(fields[3427][0])
    value_side = int(fields[3428][0])
    if price != key_price or value_side != side or fields[3428][1] != ("BID" if side == 1 else "ASK"):
        raise ValueError("MapEntry key/price/side mismatch")
    if any(enum for fid, (_, enum) in fields.items() if fid != 3428):
        raise ValueError("unexpected MapEntry enum")
    return LevelDelta(
        action=action, side=side, price_e9=price,
        no_ord=int(fields[3430][0]), acc_size=int(fields[4356][0]),
        level_time_ms=int(fields[6527][0]), level_time_msp=int(fields[6528][0]),
        level_date=_date_yyyymmdd(fields[6529][0]),
        level_time_ns=_hms_ns(fields[14268][0]) if 14268 in fields else None,
    )


def iter_messages(path: Path) -> Iterator[Message]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        if next(reader) != HEADER:
            raise ValueError("unexpected MBP header")
        outer: list[str] | None = None
        outer_row = 0
        summary: list[list[str]] | None = None
        summary_declared: int | None = None
        entries: list[tuple[str, str, int, list[list[str]]]] = []
        current_entry: list[list[str]] | None = None

        def finish() -> Message | None:
            nonlocal outer, summary, summary_declared, entries, current_entry
            if outer is None:
                return None
            if summary is not None and summary_declared != len(summary):
                raise ValueError("Summary declared count mismatch")
            parsed_entries = []
            for action, key, declared, rows in entries:
                if declared != len(rows):
                    raise ValueError("MapEntry declared count mismatch")
                parsed_entries.append(_parse_entry(action, key, rows))
            if outer[1] != "Market By Price" or outer[4] != "Raw" or int(outer[13]) != 0:
                raise ValueError("invalid outer row")
            source_sequence = int(outer[12]) if outer[12] else None
            parsed_summary = _parse_summary(summary) if summary is not None else None
            if outer[5] == "STATUS":
                if source_sequence is not None or parsed_summary is not None or parsed_entries:
                    raise ValueError("STATUS is not a standalone empty message")
            elif outer[5] not in MESSAGE_CLASS or source_sequence is None:
                raise ValueError("invalid REFRESH/UPDATE source sequence")
            result = Message(
                ric=outer[0], ts_utc_ns=_timestamp_ns(outer[2]), source_row=outer_row,
                source_sequence=source_sequence,
                gmt_offset_minutes=int(Decimal(outer[3]) * 60),
                message_class=outer[5], update_type=outer[6], pe_code=int(outer[10]),
                template_number=int(outer[11]) if outer[11] else None,
                summary=parsed_summary,
                entries=tuple(parsed_entries),
            )
            outer = None
            summary = None
            summary_declared = None
            entries = []
            current_entry = None
            return result

        for source_row, raw in enumerate(reader, 1):
            row = raw + [""] * (len(HEADER) - len(raw))
            if row[0]:
                message = finish()
                if message is not None:
                    yield message
                outer, outer_row = row, source_row
            elif outer is None:
                raise ValueError(f"orphan child row {source_row}")
            elif row[4] == "Summary":
                if summary is not None:
                    raise ValueError("repeated Summary")
                summary, summary_declared = [], int(row[13])
                current_entry = None
            elif row[4] == "MapEntry":
                current_entry = []
                entries.append((row[6], row[12], int(row[13]), current_entry))
            elif row[4] == "FID":
                if current_entry is not None:
                    # MapEntry starts a scope that lasts until the next MapEntry/outer row.
                    current_entry.append(row)
                elif summary is not None:
                    summary.append(row)
                else:
                    raise ValueError("FID outside Summary/MapEntry")
            else:
                raise ValueError(f"unsupported child type {row[4]!r}")
        message = finish()
        if message is not None:
            yield message


def encode_key(message: Message) -> bytes:
    return struct.pack(">QQ", message.ts_utc_ns, message.source_row)


def _short_ascii(raw: str) -> bytes:
    encoded = raw.encode("ascii")
    if len(encoded) > 255:
        raise ValueError("short ASCII field is too long")
    return bytes([len(encoded)]) + encoded


def encode_message(message: Message) -> bytes:
    fields = message.summary or ()
    present = sum(1 << SUMMARY_INDEX[field.fid] for field in fields)
    empty = sum(1 << SUMMARY_INDEX[field.fid] for field in fields if field.value is None)
    enums = sum(1 << SUMMARY_INDEX[field.fid] for field in fields if field.enum_value is not None)
    flags = (1 if message.summary is not None else 0) | (2 if message.is_book_image else 0)
    update = message.update_type.encode("ascii")
    template = message.template_number if message.template_number is not None else 0xFFFF
    output = bytearray(struct.pack(
        "<3sBBBhQQQHHHBBIII", MAGIC, KIND_LOGICAL_MESSAGE,
        MESSAGE_CLASS[message.message_class], flags, message.gmt_offset_minutes,
        message.ts_utc_ns,
        message.source_sequence if message.source_sequence is not None else (1 << 64) - 1,
        message.source_row,
        message.pe_code, template, len(message.entries), len(update), 0,
        present, empty, enums,
    ))
    assert len(output) == VALUE_HEADER_LEN
    output.extend(update)
    for field in fields:
        if field.value is None:
            continue
        if field.fid == 4148:
            output.extend(struct.pack("<I", int(field.value)))
        elif field.fid == 14269:
            output.extend(struct.pack("<Q", _hms_ns(field.value)))
        else:
            output.extend(_short_ascii(field.value))
    for field in fields:
        if field.enum_value is not None:
            output.extend(_short_ascii(field.enum_value))
    for entry in message.entries:
        has_fields = entry.action != "DELETE"
        entry_flags = (1 if has_fields else 0) | (2 if entry.level_time_ns is not None else 0)
        output.extend(struct.pack("<BBHq", ACTION[entry.action], entry.side, entry_flags, entry.price_e9))
        if has_fields:
            assert None not in (
                entry.no_ord, entry.acc_size, entry.level_time_ms,
                entry.level_time_msp, entry.level_date,
            )
            output.extend(struct.pack(
                "<IIIIi", entry.no_ord, entry.acc_size, entry.level_time_ms,
                entry.level_time_msp, entry.level_date,
            ))
            if entry.level_time_ns is not None:
                output.extend(struct.pack("<Q", entry.level_time_ns))
    return bytes(output)


def replay(path: Path, max_messages: int | None = None) -> dict:
    books: dict[str, set[tuple[int, int]]] = defaultdict(set)
    counts: Counter[str] = Counter()
    by_ric: Counter[str] = Counter()
    for message in iter_messages(path):
        counts["messages"] += 1
        by_ric[message.ric] += 1
        counts[f"msg_class:{message.message_class}"] += 1
        if message.message_class == "REFRESH" and message.entries:
            books[message.ric].clear()
            counts["book_images"] += 1
        elif message.message_class == "REFRESH":
            counts["refresh_without_entries"] += 1
        for entry in message.entries:
            key = (entry.side, entry.price_e9)
            if entry.action == "ADD":
                counts["add_existing"] += key in books[message.ric]
                books[message.ric].add(key)
            elif entry.action == "UPDATE":
                counts["update_missing"] += key not in books[message.ric]
                books[message.ric].add(key)
            else:
                counts["delete_missing"] += key not in books[message.ric]
                books[message.ric].discard(key)
            counts[f"action:{entry.action}"] += 1
            counts["map_entries"] += 1
        if max_messages is not None and counts["messages"] >= max_messages:
            break
    return {
        "counts": dict(sorted(counts.items())),
        "messages_by_ric": dict(sorted(by_ric.items())),
        "final_depth_by_ric": {ric: len(book) for ric, book in sorted(books.items())},
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--max-messages", type=int)
    args = parser.parse_args()
    print(json.dumps(replay(args.source, args.max_messages), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
