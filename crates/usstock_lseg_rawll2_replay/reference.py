"""Python correctness baseline for source-exact US-stock rawLL2 messages."""

from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

HEADER = [
    "#RIC", "Domain", "Date-Time", "GMT Offset", "Type",
    "MsgClass/FID number", "UpdateType/Action", "FID Name", "FID Value",
    "FID Enum String", "PE Code", "Template Number", "Key/Msg Sequence Number",
    "Number of FIDs",
]


@dataclass(frozen=True)
class Field:
    fid: int
    name: str
    value: str
    enum_value: str


@dataclass(frozen=True)
class Message:
    source_row: int
    ric: str
    date_time: str
    gmt_offset: str
    message_class: str
    update_type: str
    source_sequence: str
    fields: tuple[Field, ...]


def iter_messages(path: Path) -> Iterator[Message]:
    with path.open(newline="") as handle:
        rows = csv.reader(handle)
        if next(rows) != HEADER:
            raise ValueError("unexpected rawLL2 header")
        outer: list[str] | None = None
        outer_row = 0
        fields: list[Field] = []
        declared = 0
        for source_row, row in enumerate(rows, start=2):
            if row and row[0]:
                if outer is not None:
                    if len(fields) != declared:
                        raise ValueError(f"row {outer_row}: declared {declared} FIDs, got {len(fields)}")
                    yield Message(outer_row, outer[0], outer[2], outer[3], outer[5], outer[6], outer[12], tuple(fields))
                if len(row) != 14 or row[1] != "Market Price" or row[4] != "Legacy Level 2":
                    raise ValueError(f"invalid outer row {source_row}")
                outer, outer_row, fields, declared = row, source_row, [], int(row[13])
            else:
                if outer is None or len(row) < 10 or row[4] != "FID":
                    raise ValueError(f"invalid FID row {source_row}")
                fields.append(Field(int(row[5]), row[7], row[8], row[9]))
        if outer is not None:
            if len(fields) != declared:
                raise ValueError(f"row {outer_row}: declared {declared} FIDs, got {len(fields)}")
            yield Message(outer_row, outer[0], outer[2], outer[3], outer[5], outer[6], outer[12], tuple(fields))


def census(path: Path) -> dict[str, object]:
    messages = list(iter_messages(path))
    return {
        "messages": len(messages),
        "by_ric": dict(Counter(message.ric for message in messages)),
        "fids": dict(Counter(f"{field.fid}:{field.name}" for message in messages for field in message.fields)),
    }
