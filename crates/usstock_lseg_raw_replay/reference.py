#!/usr/bin/env python3
"""Correctness parser and audit baseline for LSEG US-stock RAW logical messages."""

from __future__ import annotations

import argparse
import csv
import io
import json
import subprocess
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, TextIO


HEADER = [
    "#RIC", "Domain", "Date-Time", "GMT Offset", "Type",
    "MsgClass/FID number", "UpdateType/Action", "FID Name", "FID Value",
    "FID Enum String", "PE Code", "Template Number",
    "Key/Msg Sequence Number", "Number of FIDs",
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
    domain: str
    date_time: str
    gmt_offset: str
    message_class: str
    update_type: str
    pe_code: str
    template_number: str
    source_sequence: str
    fields: tuple[Field, ...]


@contextmanager
def open_text(path: Path) -> Iterator[TextIO]:
    if path.suffix == ".zst":
        process = subprocess.Popen(
            ["zstd", "-qdc", str(path)], stdout=subprocess.PIPE,
        )
        assert process.stdout is not None
        stream = io.TextIOWrapper(process.stdout, encoding="utf-8", newline="")
        try:
            yield stream
        finally:
            stream.close()
            return_code = process.wait()
            if return_code not in (0, -13, 141):
                raise RuntimeError(f"zstd failed for {path}, status={return_code}")
    else:
        with path.open(encoding="utf-8", newline="") as stream:
            yield stream


def iter_messages(stream: TextIO) -> Iterator[Message]:
    rows = csv.reader(stream)
    if next(rows) != HEADER:
        raise ValueError("unexpected RAW header")
    outer: list[str] | None = None
    outer_source_row = 0
    fields: list[Field] = []
    declared = 0

    def finish() -> Message | None:
        nonlocal outer, outer_source_row, fields, declared
        if outer is None:
            return None
        if len(fields) != declared:
            raise ValueError(
                f"{outer[0]} {outer[2]} declares {declared} FIDs, got {len(fields)}"
            )
        message = Message(
            source_row=outer_source_row, ric=outer[0], domain=outer[1], date_time=outer[2],
            gmt_offset=outer[3], message_class=outer[5], update_type=outer[6],
            pe_code=outer[10], template_number=outer[11],
            source_sequence=outer[12], fields=tuple(fields),
        )
        outer, fields, declared = None, [], 0
        return message

    for source_row, raw in enumerate(rows, 2):
        row = raw + [""] * (len(HEADER) - len(raw))
        if row[0]:
            message = finish()
            if message is not None:
                yield message
            if row[1] != "Market Price" or row[4] != "Raw":
                raise ValueError(f"invalid outer row {source_row}: {row!r}")
            outer = row
            outer_source_row = source_row
            declared = int(row[13])
        else:
            if outer is None or row[4] != "FID":
                raise ValueError(f"invalid child row {source_row}: {row!r}")
            fields.append(Field(int(row[5]), row[7], row[8], row[9]))
    message = finish()
    if message is not None:
        yield message


def audit(path: Path, max_messages: int | None = None) -> dict:
    rics: Counter[str] = Counter()
    domains: Counter[str] = Counter()
    classes: Counter[str] = Counter()
    update_types: Counter[str] = Counter()
    templates: Counter[str] = Counter()
    signatures: Counter[str] = Counter()
    combinations: Counter[str] = Counter()
    field_counts: Counter[int] = Counter()
    field_names: dict[int, Counter[str]] = {}
    field_empty_values: Counter[int] = Counter()
    field_enum_values: Counter[int] = Counter()
    examples: dict[str, dict] = {}
    messages = 0
    physical_rows = 0
    with open_text(path) as stream:
        for message in iter_messages(stream):
            messages += 1
            physical_rows += 1 + len(message.fields)
            rics[message.ric] += 1
            domains[message.domain] += 1
            classes[message.message_class] += 1
            update_types[message.update_type or "<EMPTY>"] += 1
            templates[message.template_number or "<EMPTY>"] += 1
            signature = ",".join(str(field.fid) for field in message.fields)
            signatures[signature] += 1
            combination = "|".join((
                message.message_class,
                message.update_type or "<EMPTY>",
                message.template_number or "<EMPTY>",
                signature,
            ))
            combinations[combination] += 1
            if combination not in examples:
                examples[combination] = {
                    "ric": message.ric,
                    "date_time": message.date_time,
                    "gmt_offset": message.gmt_offset,
                    "message_class": message.message_class,
                    "update_type": message.update_type,
                    "pe_code": message.pe_code,
                    "template_number": message.template_number,
                    "source_sequence": message.source_sequence,
                    "fields": [
                        {
                            "fid": field.fid,
                            "name": field.name,
                            "value": field.value,
                            "enum_value": field.enum_value,
                        }
                        for field in message.fields
                    ],
                }
            for field in message.fields:
                field_counts[field.fid] += 1
                field_names.setdefault(field.fid, Counter())[field.name] += 1
                field_empty_values[field.fid] += not bool(field.value)
                field_enum_values[field.fid] += bool(field.enum_value)
            if max_messages is not None and messages >= max_messages:
                break
    return {
        "source": str(path),
        "logical_messages": messages,
        "physical_rows": physical_rows,
        "rics": dict(rics.most_common()),
        "domains": dict(domains.most_common()),
        "message_classes": dict(classes.most_common()),
        "update_types": dict(update_types.most_common()),
        "templates": dict(templates.most_common()),
        "distinct_fids": len(field_names),
        "fids": {
            str(fid): {
                "names": dict(field_names[fid].most_common()),
                "present": field_counts[fid],
                "empty_value": field_empty_values[fid],
                "nonempty_enum": field_enum_values[fid],
            }
            for fid in sorted(field_names)
        },
        "distinct_ordered_fid_signatures": len(signatures),
        "top_ordered_fid_signatures": dict(signatures.most_common(100)),
        "distinct_message_combinations": len(combinations),
        "message_combinations": dict(combinations.most_common()),
        "first_example_by_combination": examples,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--max-messages", type=int)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = audit(args.source, args.max_messages)
    rendered = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
