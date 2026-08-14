#!/usr/bin/env python3
"""Collect instrument metadata on a remote egress host and load PostgreSQL."""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Sequence

import instrument_snapshot as snapshot


REMOTE_TEMP_RE = re.compile(r"^/tmp/mkt-instrument-(?:collect|load)\.[A-Za-z0-9]+$")
HOST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@-]*$")
PG_URL_QUERY_ENV = {
    "application_name": "PGAPPNAME",
    "connect_timeout": "PGCONNECT_TIMEOUT",
    "sslmode": "PGSSLMODE",
    "sslrootcert": "PGSSLROOTCERT",
}


def add_query_param(url: str, name: str, value: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [(key, item) for key, item in query if key != name]
    query.append((name, value))
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query), parsed.fragment)
    )


def fetch_page(
    source: snapshot.SourceSpec,
    market_types: tuple[str, ...],
    *,
    page: int,
    url: str,
    timeout_sec: float,
    retries: int,
) -> snapshot.RawResponse:
    headers = {"Accept": "application/json", "User-Agent": snapshot.USER_AGENT}
    headers.update(dict(source.headers))
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        fetched_at = snapshot.iso_utc(snapshot.utc_now())
        request = urllib.request.Request(url, method="GET", headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=timeout_sec) as response:
                body = response.read()
                status = int(response.status)
                response_headers = {key: value for key, value in response.headers.items()}
                final_url = response.geturl()
            if status < 200 or status >= 300:
                raise snapshot.SnapshotError(f"{source.source_id}: HTTP {status} from {final_url}")
            if not body:
                raise snapshot.SnapshotError(f"{source.source_id}: empty HTTP response from {final_url}")
            try:
                payload = json.loads(body.decode("utf-8"), parse_float=Decimal, parse_int=Decimal)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise snapshot.SnapshotError(
                    f"{source.source_id}: invalid UTF-8 JSON from {final_url}"
                ) from exc
            return snapshot.RawResponse(
                source=source,
                page=page,
                market_types=market_types,
                request_url=final_url,
                request_headers=headers,
                response_headers=response_headers,
                http_status=status,
                fetched_at=fetched_at,
                body=body,
                payload=payload,
            )
        except urllib.error.HTTPError as exc:
            error_body = exc.read(512).decode("utf-8", errors="replace")
            last_error = snapshot.SnapshotError(
                f"{source.source_id}: HTTP {exc.code} from {url}; body={error_body!r}"
            )
            if exc.code < 500 and exc.code != 429:
                break
        except (urllib.error.URLError, TimeoutError, OSError, snapshot.SnapshotError) as exc:
            last_error = exc
        if attempt < retries:
            time.sleep(min(2 ** (attempt - 1), 4))
    raise snapshot.SnapshotError(
        f"{source.source_id}: request failed after {retries} attempt(s): {last_error}"
    )


def fetch_source(
    source: snapshot.SourceSpec,
    market_types: tuple[str, ...],
    *,
    timeout_sec: float,
    retries: int,
) -> list[snapshot.RawResponse]:
    responses: list[snapshot.RawResponse] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    for page in range(1, 21):
        url = source.url
        if cursor:
            url = add_query_param(url, "cursor", cursor)
        response = fetch_page(
            source,
            market_types,
            page=page,
            url=url,
            timeout_sec=timeout_sec,
            retries=retries,
        )
        responses.append(response)
        if not source.paginated:
            break
        result = response.payload.get("result") if isinstance(response.payload, dict) else None
        next_cursor = result.get("nextPageCursor") if isinstance(result, dict) else None
        cursor_text = str(next_cursor).strip() if next_cursor is not None else ""
        if not cursor_text:
            break
        if cursor_text in seen_cursors:
            raise snapshot.SnapshotError(
                f"{source.source_id}: pagination cursor did not advance: {cursor_text}"
            )
        seen_cursors.add(cursor_text)
        cursor = cursor_text
    else:
        raise snapshot.SnapshotError(f"{source.source_id}: pagination exceeded 20 pages")
    return responses


def selected_sources(
    exchanges: Sequence[str], market_types: Sequence[str]
) -> list[tuple[snapshot.SourceSpec, tuple[str, ...]]]:
    exchange_set = set(exchanges)
    market_set = set(market_types)
    selected: list[tuple[snapshot.SourceSpec, tuple[str, ...]]] = []
    for source in snapshot.SOURCES:
        targets = tuple(market for market in source.market_types if market in market_set)
        if source.exchange in exchange_set and targets:
            selected.append((source, targets))
    if not selected:
        raise snapshot.SnapshotError("no REST sources selected")
    return selected


def validate_names(
    values: Sequence[str], allowed: Sequence[str], label: str
) -> tuple[str, ...]:
    result: list[str] = []
    if not values:
        values = [",".join(allowed)]
    for raw in values:
        for item in raw.split(","):
            value = item.strip().lower()
            if not value:
                continue
            if value not in allowed:
                raise snapshot.SnapshotError(
                    f"unknown {label} {value!r}; expected one of {', '.join(allowed)}"
                )
            if value not in result:
                result.append(value)
    if not result:
        raise snapshot.SnapshotError(f"at least one {label} is required")
    return tuple(result)

def scope_summaries(
    rules: Sequence[dict[str, Any]],
    effective_from: str,
    exchanges: Sequence[str],
    market_types: Sequence[str],
) -> list[dict[str, Any]]:
    counts: dict[tuple[str, str], int] = {
        (exchange, market_type): 0
        for exchange in exchanges
        for market_type in market_types
    }
    for rule in rules:
        key = (rule["exchange"], rule["market_type"])
        counts[key] = counts.get(key, 0) + 1
    return [
        {
            "exchange": exchange,
            "market_type": market_type,
            "effective_from": effective_from,
            "instrument_count": count,
        }
        for (exchange, market_type), count in sorted(counts.items())
    ]


def write_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as handle:
        handle.write(data)


def write_text(path: Path, data: str) -> None:
    write_bytes(path, data.encode("utf-8"))


def build_snapshot_tree(
    root: Path,
    *,
    snapshot_id: str,
    captured_at: str,
    completed_at: str,
    effective_from: str,
    exchanges: tuple[str, ...],
    market_types: tuple[str, ...],
    responses: Sequence[snapshot.RawResponse],
    rules: list[dict[str, Any]],
    script_sha256: str,
) -> dict[str, Any]:
    raw_metadata: list[dict[str, Any]] = []
    for response in responses:
        filename = f"raw/{response.source.source_id}/page-{response.page:03d}.json"
        response.filename = filename
        write_bytes(root / filename, response.body)
        raw_metadata.append(
            {
                "source_id": response.source.source_id,
                "page": response.page,
                "exchange": response.source.exchange,
                "market_types": list(response.market_types),
                "request_url": response.request_url,
                "request_headers": response.request_headers,
                "response_headers": response.response_headers,
                "http_status": response.http_status,
                "fetched_at": response.fetched_at,
                "response_sha256": response.sha256,
                "body_file": filename,
            }
        )

    normalized_path = root / "normalized_instruments.jsonl"
    normalized_text = "".join(snapshot.compact_json(rule) + "\n" for rule in rules)
    write_text(normalized_path, normalized_text)
    scopes = scope_summaries(rules, effective_from, exchanges, market_types)
    manifest = {
        "schema_version": snapshot.SCHEMA_VERSION,
        "snapshot_id": snapshot_id,
        "captured_at": captured_at,
        "completed_at": completed_at,
        "effective_from": effective_from,
        "collector_host": snapshot.socket.gethostname(),
        "collector_script_sha256": script_sha256,
        "exchanges": list(exchanges),
        "market_types": list(market_types),
        "raw_response_count": len(responses),
        "instrument_count": len(rules),
        "scopes": scopes,
        "raw_responses": raw_metadata,
        "normalized_file": normalized_path.name,
        "normalized_sha256": hashlib.sha256(normalized_text.encode("utf-8")).hexdigest(),
        "instruments": rules,
    }
    manifest_path = root / "manifest.json"
    write_text(
        manifest_path,
        json.dumps(manifest, ensure_ascii=True, sort_keys=True, indent=2) + "\n",
    )
    checksummed = [manifest_path, normalized_path]
    checksummed.extend(root / item["body_file"] for item in raw_metadata)
    checksum_lines = [
        f"{snapshot.sha256_file(path)}  {path.relative_to(root).as_posix()}"
        for path in sorted(checksummed)
    ]
    write_text(root / "SHA256SUMS", "\n".join(checksum_lines) + "\n")
    return manifest


def create_archive(source_root: Path, archive: Path) -> None:
    archive.parent.mkdir(parents=True, exist_ok=True)
    temporary = archive.with_name(archive.name + ".part")
    try:
        with tarfile.open(temporary, "w:gz", format=tarfile.PAX_FORMAT) as output:
            output.add(source_root, arcname=source_root.name, recursive=True)
        os.replace(temporary, archive)
    finally:
        temporary.unlink(missing_ok=True)


def collect_snapshot(
    archive: Path,
    *,
    exchanges: tuple[str, ...],
    market_types: tuple[str, ...],
    effective_from: str | None,
    timeout_sec: float,
    retries: int,
) -> dict[str, Any]:
    if timeout_sec <= 0:
        raise snapshot.SnapshotError("timeout must be positive")
    if retries <= 0:
        raise snapshot.SnapshotError("retries must be positive")
    captured_dt = snapshot.utc_now()
    captured_at = snapshot.iso_utc(captured_dt)
    effective_at = (
        snapshot.parse_utc(effective_from, "effective_from")
        if effective_from
        else captured_at
    )
    snapshot_id = str(uuid.uuid4())
    script_sha256 = snapshot.sha256_file(Path(snapshot.__file__).resolve())
    responses: list[snapshot.RawResponse] = []
    rules: list[dict[str, Any]] = []
    for source, targets in selected_sources(exchanges, market_types):
        pages = fetch_source(source, targets, timeout_sec=timeout_sec, retries=retries)
        responses.extend(pages)
        rules.extend(snapshot.NORMALIZERS[source.parser](source, pages, targets))
    rules = snapshot.finalize_rules(
        rules,
        snapshot_id=snapshot_id,
        captured_at=captured_at,
        effective_from=effective_at,
    )
    completed_at = snapshot.iso_utc(snapshot.utc_now())
    snapshot_name = f"{captured_dt.strftime('%Y%m%dT%H%M%S%fZ')}_{snapshot_id}"
    with tempfile.TemporaryDirectory(prefix="mkt-instrument-collect-") as temporary:
        root = Path(temporary) / snapshot_name
        root.mkdir()
        manifest = build_snapshot_tree(
            root,
            snapshot_id=snapshot_id,
            captured_at=captured_at,
            completed_at=completed_at,
            effective_from=effective_at,
            exchanges=exchanges,
            market_types=market_types,
            responses=responses,
            rules=rules,
            script_sha256=script_sha256,
        )
        create_archive(root, archive)
    return {
        "snapshot_id": snapshot_id,
        "snapshot_name": snapshot_name,
        "captured_at": captured_at,
        "effective_from": effective_at,
        "archive": str(archive),
        "archive_sha256": snapshot.sha256_file(archive),
        "raw_response_count": manifest["raw_response_count"],
        "instrument_count": manifest["instrument_count"],
        "scopes": manifest["scopes"],
    }


def parse_checksum_file(contents: str) -> dict[str, str]:
    checksums: dict[str, str] = {}
    for line_number, line in enumerate(contents.splitlines(), 1):
        if not line:
            continue
        try:
            digest, name = line.split("  ", 1)
        except ValueError as exc:
            raise snapshot.SnapshotError(
                f"malformed SHA256SUMS line {line_number}"
            ) from exc
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise snapshot.SnapshotError(f"invalid SHA-256 on line {line_number}")
        checksums[name] = digest
    return checksums


def safe_archive_members(
    archive: tarfile.TarFile,
) -> tuple[str, list[tarfile.TarInfo]]:
    members = archive.getmembers()
    if not members:
        raise snapshot.SnapshotError("snapshot archive is empty")
    roots: set[str] = set()
    for member in members:
        path = PurePosixPath(member.name)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise snapshot.SnapshotError(f"unsafe archive member: {member.name!r}")
        if member.issym() or member.islnk() or member.isdev():
            raise snapshot.SnapshotError(
                f"unsupported archive member type: {member.name!r}"
            )
        roots.add(path.parts[0])
    if len(roots) != 1:
        raise snapshot.SnapshotError(
            f"snapshot archive must have one root directory, found {sorted(roots)}"
        )
    return next(iter(roots)), members


def extract_snapshot_archive(
    archive_path: Path, destination: Path
) -> tuple[Path, dict[str, Any]]:
    destination.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive_path, "r:gz") as archive:
        root_name, members = safe_archive_members(archive)
        root = destination / root_name
        if root.exists():
            raise snapshot.SnapshotError(f"snapshot destination already exists: {root}")
        for member in members:
            output = destination.joinpath(*PurePosixPath(member.name).parts)
            if member.isdir():
                output.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise snapshot.SnapshotError(
                    f"unsupported archive member: {member.name!r}"
                )
            output.parent.mkdir(parents=True, exist_ok=True)
            source = archive.extractfile(member)
            if source is None:
                raise snapshot.SnapshotError(
                    f"failed to read archive member: {member.name!r}"
                )
            with source, output.open("wb") as target:
                shutil.copyfileobj(source, target)

    manifest_path = root / "manifest.json"
    checksum_path = root / "SHA256SUMS"
    if not manifest_path.is_file() or not checksum_path.is_file():
        raise snapshot.SnapshotError(
            "snapshot archive is missing manifest.json or SHA256SUMS"
        )
    checksums = parse_checksum_file(checksum_path.read_text(encoding="utf-8"))
    for relative_name, expected in checksums.items():
        path = root / relative_name
        if not path.is_file():
            raise snapshot.SnapshotError(
                f"checksummed snapshot file is missing: {relative_name}"
            )
        actual = snapshot.sha256_file(path)
        if actual != expected:
            raise snapshot.SnapshotError(
                f"snapshot checksum mismatch for {relative_name}: {actual} != {expected}"
            )
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise snapshot.SnapshotError("manifest.json is invalid") from exc
    validate_extracted_snapshot(root, manifest)
    return root, manifest


def validate_extracted_snapshot(root: Path, manifest: dict[str, Any]) -> None:
    if manifest.get("schema_version") != snapshot.SCHEMA_VERSION:
        raise snapshot.SnapshotError(
            f"unsupported manifest schema_version={manifest.get('schema_version')}; "
            f"expected {snapshot.SCHEMA_VERSION}"
        )
    try:
        uuid.UUID(str(manifest["snapshot_id"]))
    except (KeyError, ValueError) as exc:
        raise snapshot.SnapshotError(
            "manifest snapshot_id is missing or invalid"
        ) from exc
    for field in ("captured_at", "completed_at", "effective_from"):
        snapshot.parse_utc(str(manifest.get(field, "")), field)
    normalized_path = root / str(manifest.get("normalized_file", ""))
    if not normalized_path.is_file():
        raise snapshot.SnapshotError("manifest normalized_file is missing")
    if snapshot.sha256_file(normalized_path) != manifest.get("normalized_sha256"):
        raise snapshot.SnapshotError(
            "normalized instrument checksum does not match manifest"
        )
    rules: list[dict[str, Any]] = []
    with normalized_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            try:
                rules.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise snapshot.SnapshotError(
                    f"invalid normalized JSONL line {line_number}"
                ) from exc
    if len(rules) != manifest.get("instrument_count"):
        raise snapshot.SnapshotError(
            "normalized instrument count does not match manifest"
        )
    if rules != manifest.get("instruments"):
        raise snapshot.SnapshotError(
            "manifest instruments differ from normalized JSONL"
        )
    raw = manifest.get("raw_responses")
    if not isinstance(raw, list) or len(raw) != manifest.get("raw_response_count"):
        raise snapshot.SnapshotError("raw response count does not match manifest")
    for response in raw:
        body_path = root / str(response.get("body_file", ""))
        if (
            not body_path.is_file()
            or snapshot.sha256_file(body_path) != response.get("response_sha256")
        ):
            raise snapshot.SnapshotError(
                f"raw response checksum mismatch: {response.get('body_file')}"
            )


def read_env_file(path: Path) -> dict[str, str]:
    path = path.expanduser()
    try:
        mode = path.stat().st_mode & 0o777
    except OSError as exc:
        raise snapshot.SnapshotError(f"cannot stat database env file {path}: {exc}") from exc
    if mode & 0o077:
        raise snapshot.SnapshotError(
            f"database env file must not be group/world accessible: {path} mode={mode:03o}"
        )
    result: dict[str, str] = {}
    for line_number, raw_line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, separator, raw_value = line.partition("=")
        if not separator or not key.strip():
            raise snapshot.SnapshotError(
                f"malformed database env file line {line_number}"
            )
        try:
            parsed = shlex.split(raw_value, comments=True, posix=True)
        except ValueError as exc:
            raise snapshot.SnapshotError(
                f"malformed database env value on line {line_number}"
            ) from exc
        if len(parsed) != 1:
            raise snapshot.SnapshotError(
                f"database env value on line {line_number} must be one shell word"
            )
        result[key.strip()] = parsed[0]
    return result


def postgres_process_env(database_url: str) -> dict[str, str]:
    parsed = urllib.parse.urlsplit(database_url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise snapshot.SnapshotError("database URL must use postgres:// or postgresql://")
    if not parsed.hostname or not parsed.username or not parsed.path.strip("/"):
        raise snapshot.SnapshotError(
            "database URL must include host, username, and database name"
        )
    environment = os.environ.copy()
    for name in (
        "PGHOST",
        "PGPORT",
        "PGUSER",
        "PGPASSWORD",
        "PGDATABASE",
        "PGSERVICE",
        "PGSERVICEFILE",
        "PGPASSFILE",
        "PGSSLMODE",
        "PGSSLROOTCERT",
    ):
        environment.pop(name, None)
    environment.update(
        PGHOST=parsed.hostname,
        PGPORT=str(parsed.port or 5432),
        PGUSER=urllib.parse.unquote(parsed.username),
        PGDATABASE=urllib.parse.unquote(parsed.path.lstrip("/")),
        PGCONNECT_TIMEOUT="10",
        PGAPPNAME="mkt_signal_instrument_snapshot",
    )
    if parsed.password is not None:
        environment["PGPASSWORD"] = urllib.parse.unquote(parsed.password)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    unsupported = sorted(set(query) - set(PG_URL_QUERY_ENV))
    if unsupported:
        raise snapshot.SnapshotError(
            f"unsupported database URL query parameters: {', '.join(unsupported)}"
        )
    for key, values in query.items():
        if values:
            environment[PG_URL_QUERY_ENV[key]] = values[-1]
    return environment


def stage_payloads(root: Path, manifest: dict[str, Any]) -> Iterable[dict[str, Any]]:
    yield {"kind": "run", "data": manifest}
    for scope in manifest["scopes"]:
        yield {
            "kind": "scope",
            "data": {"snapshot_id": manifest["snapshot_id"], **scope},
        }
    for response in manifest["raw_responses"]:
        body = (root / response["body_file"]).read_text(encoding="utf-8")
        yield {
            "kind": "raw",
            "data": {
                "snapshot_id": manifest["snapshot_id"],
                **response,
                "body_text": body,
            },
        }
    for rule in manifest["instruments"]:
        yield {"kind": "rule", "data": rule}


LOAD_SQL_PREFIX = r"""\set ON_ERROR_STOP on
BEGIN;
CREATE TEMP TABLE instrument_snapshot_stage (payload jsonb NOT NULL);
COPY instrument_snapshot_stage(payload) FROM STDIN WITH (FORMAT csv);
"""


LOAD_SQL_SUFFIX = r"""\.

INSERT INTO market_metadata.instrument_snapshot_runs (
    snapshot_id, schema_version, captured_at, completed_at, effective_from,
    collector_host, collector_script_sha256, exchanges, market_types,
    raw_response_count, instrument_count, manifest
)
SELECT
    (data->>'snapshot_id')::uuid,
    (data->>'schema_version')::integer,
    (data->>'captured_at')::timestamptz,
    (data->>'completed_at')::timestamptz,
    (data->>'effective_from')::timestamptz,
    data->>'collector_host',
    data->>'collector_script_sha256',
    ARRAY(SELECT jsonb_array_elements_text(data->'exchanges')),
    ARRAY(SELECT jsonb_array_elements_text(data->'market_types')),
    (data->>'raw_response_count')::integer,
    (data->>'instrument_count')::integer,
    data
FROM (
    SELECT payload->'data' AS data
    FROM instrument_snapshot_stage
    WHERE payload->>'kind' = 'run'
) AS staged
ON CONFLICT (snapshot_id) DO NOTHING;

INSERT INTO market_metadata.instrument_snapshot_scopes (
    snapshot_id, exchange, market_type, effective_from, instrument_count
)
SELECT
    (data->>'snapshot_id')::uuid,
    data->>'exchange',
    data->>'market_type',
    (data->>'effective_from')::timestamptz,
    (data->>'instrument_count')::integer
FROM (
    SELECT payload->'data' AS data
    FROM instrument_snapshot_stage
    WHERE payload->>'kind' = 'scope'
) AS staged
ON CONFLICT (snapshot_id, exchange, market_type) DO NOTHING;

INSERT INTO market_metadata.instrument_raw_responses (
    snapshot_id, source_id, page, exchange, market_types, request_url,
    request_headers, response_headers, http_status, fetched_at,
    response_sha256, response_body_text, response_body
)
SELECT
    (data->>'snapshot_id')::uuid,
    data->>'source_id',
    (data->>'page')::integer,
    data->>'exchange',
    ARRAY(SELECT jsonb_array_elements_text(data->'market_types')),
    data->>'request_url',
    data->'request_headers',
    data->'response_headers',
    (data->>'http_status')::integer,
    (data->>'fetched_at')::timestamptz,
    data->>'response_sha256',
    data->>'body_text',
    (data->>'body_text')::jsonb
FROM (
    SELECT payload->'data' AS data
    FROM instrument_snapshot_stage
    WHERE payload->>'kind' = 'raw'
) AS staged
ON CONFLICT (snapshot_id, source_id, page) DO NOTHING;

INSERT INTO market_metadata.instrument_rules (
    snapshot_id, captured_at, effective_from, exchange, market_type,
    instrument_id, symbol, base_asset, quote_asset, contract_type, status,
    price_tick_raw, price_tick, price_tick_integer, price_tick_scale, price_tick_source,
    qty_step_raw, qty_step, qty_step_integer, qty_step_scale, qty_step_source,
    min_qty_raw, min_qty, max_qty_raw, max_qty,
    market_qty_step_raw, market_qty_step,
    market_min_qty_raw, market_min_qty, market_max_qty_raw, market_max_qty,
    min_notional_raw, min_notional, max_notional_raw, max_notional,
    market_min_notional_raw, market_min_notional,
    market_max_notional_raw, market_max_notional,
    contract_multiplier_raw, contract_multiplier, contract_multiplier_components,
    source_id, source_page, rule_sha256, raw_instrument
)
SELECT
    (data->>'snapshot_id')::uuid,
    (data->>'captured_at')::timestamptz,
    (data->>'effective_from')::timestamptz,
    data->>'exchange', data->>'market_type', data->>'instrument_id', data->>'symbol',
    NULLIF(data->>'base_asset', ''), NULLIF(data->>'quote_asset', ''),
    NULLIF(data->>'contract_type', ''), NULLIF(data->>'status', ''),
    NULLIF(data->>'price_tick_raw', ''), NULLIF(data->>'price_tick', '')::numeric,
    NULLIF(data->>'price_tick_integer', '')::numeric,
    NULLIF(data->>'price_tick_scale', '')::smallint,
    NULLIF(data->>'price_tick_source', ''),
    NULLIF(data->>'qty_step_raw', ''), NULLIF(data->>'qty_step', '')::numeric,
    NULLIF(data->>'qty_step_integer', '')::numeric,
    NULLIF(data->>'qty_step_scale', '')::smallint,
    NULLIF(data->>'qty_step_source', ''),
    NULLIF(data->>'min_qty_raw', ''), NULLIF(data->>'min_qty', '')::numeric,
    NULLIF(data->>'max_qty_raw', ''), NULLIF(data->>'max_qty', '')::numeric,
    NULLIF(data->>'market_qty_step_raw', ''), NULLIF(data->>'market_qty_step', '')::numeric,
    NULLIF(data->>'market_min_qty_raw', ''), NULLIF(data->>'market_min_qty', '')::numeric,
    NULLIF(data->>'market_max_qty_raw', ''), NULLIF(data->>'market_max_qty', '')::numeric,
    NULLIF(data->>'min_notional_raw', ''), NULLIF(data->>'min_notional', '')::numeric,
    NULLIF(data->>'max_notional_raw', ''), NULLIF(data->>'max_notional', '')::numeric,
    NULLIF(data->>'market_min_notional_raw', ''),
    NULLIF(data->>'market_min_notional', '')::numeric,
    NULLIF(data->>'market_max_notional_raw', ''),
    NULLIF(data->>'market_max_notional', '')::numeric,
    NULLIF(data->>'contract_multiplier_raw', ''),
    NULLIF(data->>'contract_multiplier', '')::numeric,
    COALESCE(data->'contract_multiplier_components', '{}'::jsonb),
    data->>'source_id', (data->>'source_page')::integer,
    data->>'rule_sha256', data->'raw_instrument'
FROM (
    SELECT payload->'data' AS data
    FROM instrument_snapshot_stage
    WHERE payload->>'kind' = 'rule'
) AS staged
ON CONFLICT (snapshot_id, exchange, market_type, instrument_id) DO NOTHING;

WITH ordered AS (
    SELECT
        snapshot_id, exchange, market_type,
        lead(effective_from) OVER (
            PARTITION BY exchange, market_type ORDER BY effective_from, snapshot_id
        ) AS next_effective_from
    FROM market_metadata.instrument_snapshot_scopes
)
UPDATE market_metadata.instrument_snapshot_scopes AS target
SET effective_to = ordered.next_effective_from
FROM ordered
WHERE target.snapshot_id = ordered.snapshot_id
  AND target.exchange = ordered.exchange
  AND target.market_type = ordered.market_type
  AND target.effective_to IS DISTINCT FROM ordered.next_effective_from;

DO $validation$
DECLARE
    staged_manifest jsonb;
    staged_snapshot_id uuid;
    expected_raw integer;
    expected_rules integer;
    expected_scopes integer;
BEGIN
    SELECT payload->'data' INTO STRICT staged_manifest
    FROM instrument_snapshot_stage WHERE payload->>'kind' = 'run';
    staged_snapshot_id := (staged_manifest->>'snapshot_id')::uuid;
    expected_raw := (staged_manifest->>'raw_response_count')::integer;
    expected_rules := (staged_manifest->>'instrument_count')::integer;
    expected_scopes := jsonb_array_length(staged_manifest->'scopes');

    IF (SELECT manifest FROM market_metadata.instrument_snapshot_runs
        WHERE snapshot_id = staged_snapshot_id) IS DISTINCT FROM staged_manifest THEN
        RAISE EXCEPTION 'stored manifest differs for snapshot %', staged_snapshot_id;
    END IF;
    IF (SELECT count(*) FROM market_metadata.instrument_raw_responses
        WHERE snapshot_id = staged_snapshot_id) <> expected_raw THEN
        RAISE EXCEPTION 'raw response count differs for snapshot %', staged_snapshot_id;
    END IF;
    IF (SELECT count(*) FROM market_metadata.instrument_rules
        WHERE snapshot_id = staged_snapshot_id) <> expected_rules THEN
        RAISE EXCEPTION 'instrument count differs for snapshot %', staged_snapshot_id;
    END IF;
    IF (SELECT count(*) FROM market_metadata.instrument_snapshot_scopes
        WHERE snapshot_id = staged_snapshot_id) <> expected_scopes THEN
        RAISE EXCEPTION 'scope count differs for snapshot %', staged_snapshot_id;
    END IF;
END
$validation$;

COMMIT;

SELECT json_build_object(
    'snapshot_id', run.snapshot_id,
    'raw_response_count', (
        SELECT count(*) FROM market_metadata.instrument_raw_responses raw
        WHERE raw.snapshot_id = run.snapshot_id
    ),
    'instrument_count', (
        SELECT count(*) FROM market_metadata.instrument_rules rules
        WHERE rules.snapshot_id = run.snapshot_id
    ),
    'scope_count', (
        SELECT count(*) FROM market_metadata.instrument_snapshot_scopes scopes
        WHERE scopes.snapshot_id = run.snapshot_id
    )
)::text
FROM market_metadata.instrument_snapshot_runs run
WHERE run.snapshot_id = (
    SELECT (payload->'data'->>'snapshot_id')::uuid
    FROM instrument_snapshot_stage WHERE payload->>'kind' = 'run'
);
"""


def psql_csv_script(root: Path, manifest: dict[str, Any]) -> str:
    output = io.StringIO()
    output.write(LOAD_SQL_PREFIX)
    writer = csv.writer(output, lineterminator="\n")
    for payload in stage_payloads(root, manifest):
        writer.writerow([snapshot.compact_json(payload)])
    output.write(LOAD_SQL_SUFFIX)
    return output.getvalue()


def run_psql(
    arguments: Sequence[str],
    *,
    environment: dict[str, str],
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            ["psql", "-X", *arguments],
            env=environment,
            input=input_text,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
    except FileNotFoundError as exc:
        raise snapshot.SnapshotError("psql is not installed on the database host") from exc
    if result.returncode != 0:
        error = result.stderr.strip() or result.stdout.strip()
        raise snapshot.SnapshotError(f"psql failed: {error}")
    return result


def load_snapshot(
    archive: Path,
    *,
    schema_sql: Path,
    database_env: Path,
    database_url_env: str,
) -> dict[str, Any]:
    if not archive.is_file():
        raise snapshot.SnapshotError(f"snapshot archive is missing: {archive}")
    if not schema_sql.is_file():
        raise snapshot.SnapshotError(f"schema SQL is missing: {schema_sql}")
    values = read_env_file(database_env)
    database_url = values.get(database_url_env)
    if not database_url:
        raise snapshot.SnapshotError(
            f"database env file does not define {database_url_env}"
        )
    environment = postgres_process_env(database_url)
    with tempfile.TemporaryDirectory(prefix="mkt-instrument-load-") as temporary:
        root, manifest = extract_snapshot_archive(archive, Path(temporary))
        run_psql(
            ["--quiet", "--set", "ON_ERROR_STOP=1", "--file", str(schema_sql)],
            environment=environment,
        )
        result = run_psql(
            ["--quiet", "--tuples-only", "--no-align"],
            environment=environment,
            input_text=psql_csv_script(root, manifest),
        )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise snapshot.SnapshotError(
            f"unexpected psql verification output ({len(lines)} lines)"
        )
    try:
        loaded = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise snapshot.SnapshotError("psql verification output is not JSON") from exc
    if loaded.get("snapshot_id") != manifest["snapshot_id"]:
        raise snapshot.SnapshotError("PostgreSQL verification returned another snapshot_id")
    return loaded


def validate_host(host: str) -> None:
    if not HOST_RE.fullmatch(host):
        raise snapshot.SnapshotError(f"unsafe SSH host alias: {host!r}")


def run_command(
    command: Sequence[str],
    *,
    capture_output: bool = True,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            text=True,
            stdout=subprocess.PIPE if capture_output else None,
            stderr=subprocess.PIPE if capture_output else None,
            check=False,
        )
    except FileNotFoundError as exc:
        raise snapshot.SnapshotError(f"required command is not installed: {command[0]}") from exc
    if check and result.returncode != 0:
        detail = ""
        if capture_output:
            detail = (result.stderr or result.stdout or "").strip()
        raise snapshot.SnapshotError(
            f"command failed with status {result.returncode}: {command[0]}: {detail}"
        )
    return result


def ssh_command(host: str, remote_arguments: Sequence[str]) -> list[str]:
    validate_host(host)
    return ["ssh", *snapshot.SSH_OPTIONS, host, shlex.join(remote_arguments)]


def remote_temp_dir(host: str, kind: str) -> str:
    if kind not in {"collect", "load"}:
        raise snapshot.SnapshotError(f"invalid remote temporary directory kind: {kind}")
    result = run_command(
        ssh_command(host, ["mktemp", "-d", f"/tmp/mkt-instrument-{kind}.XXXXXX"])
    )
    path = result.stdout.strip()
    if not REMOTE_TEMP_RE.fullmatch(path):
        raise snapshot.SnapshotError(f"remote mktemp returned unsafe path: {path!r}")
    return path


def cleanup_remote(host: str, path: str) -> None:
    if not REMOTE_TEMP_RE.fullmatch(path):
        raise snapshot.SnapshotError(f"refusing to clean unsafe remote path: {path!r}")
    run_command(
        ssh_command(host, ["rm", "-rf", "--", path]),
        capture_output=True,
        check=False,
    )


def remote_spec(host: str, path: str) -> str:
    validate_host(host)
    if not path.startswith("/tmp/mkt-instrument-") or any(
        char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789/._-"
        for char in path
    ):
        raise snapshot.SnapshotError(f"unsafe remote transfer path: {path!r}")
    return f"{host}:{path}"


def scp_to(host: str, local: Path, remote: str) -> None:
    run_command(
        ["scp", *snapshot.SSH_OPTIONS, str(local), remote_spec(host, remote)],
        capture_output=True,
    )


def scp_from(host: str, remote: str, local: Path) -> None:
    run_command(
        ["scp", *snapshot.SSH_OPTIONS, remote_spec(host, remote), str(local)],
        capture_output=True,
    )


def parse_single_json_output(result: subprocess.CompletedProcess[str], label: str) -> dict[str, Any]:
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if len(lines) != 1:
        raise snapshot.SnapshotError(f"{label} returned {len(lines)} non-empty output lines")
    try:
        value = json.loads(lines[0])
    except json.JSONDecodeError as exc:
        raise snapshot.SnapshotError(f"{label} did not return JSON") from exc
    if not isinstance(value, dict):
        raise snapshot.SnapshotError(f"{label} returned non-object JSON")
    return value


def copy_runtime_files(host: str, remote_dir: str, *, include_schema: bool) -> dict[str, str]:
    module_path = Path(snapshot.__file__).resolve()
    cli_path = Path(__file__).resolve()
    paths = {
        "module": f"{remote_dir}/{module_path.name}",
        "cli": f"{remote_dir}/{cli_path.name}",
    }
    scp_to(host, module_path, paths["module"])
    scp_to(host, cli_path, paths["cli"])
    if include_schema:
        schema_path = snapshot.DEFAULT_SCHEMA_SQL.resolve()
        if not schema_path.is_file():
            raise snapshot.SnapshotError(f"schema SQL is missing: {schema_path}")
        paths["schema"] = f"{remote_dir}/{schema_path.name}"
        scp_to(host, schema_path, paths["schema"])
    return paths


def orchestrate(args: argparse.Namespace) -> dict[str, Any]:
    exchanges = validate_names(args.exchanges, snapshot.VALID_EXCHANGES, "exchange")
    market_types = validate_names(
        args.market_types, snapshot.VALID_MARKET_TYPES, "market type"
    )
    sources = selected_sources(exchanges, market_types)
    plan = {
        "execute": bool(args.execute),
        "collector_host": args.collector_host,
        "database_host": None if args.skip_database else args.database_host,
        "database_schema": None if args.skip_database else "market_metadata",
        "output_root": str(args.output_root.resolve()),
        "exchanges": list(exchanges),
        "market_types": list(market_types),
        "effective_from": args.effective_from or "capture_time",
        "sources": [
            {
                "source_id": source.source_id,
                "exchange": source.exchange,
                "market_types": list(targets),
                "url": source.url,
            }
            for source, targets in sources
        ],
    }
    if not args.execute:
        return {"dry_run": plan}

    validate_host(args.collector_host)
    if not args.skip_database:
        validate_host(args.database_host)
    collector_temp: str | None = None
    database_temp: str | None = None
    with tempfile.TemporaryDirectory(prefix="mkt-instrument-sync-") as temporary:
        local_archive = Path(temporary) / "instrument_snapshot.tar.gz"
        try:
            print(f"collecting public instrument metadata on {args.collector_host}", file=sys.stderr)
            collector_temp = remote_temp_dir(args.collector_host, "collect")
            remote_files = copy_runtime_files(
                args.collector_host, collector_temp, include_schema=False
            )
            remote_archive = f"{collector_temp}/instrument_snapshot.tar.gz"
            collect_command = [
                "python3",
                remote_files["cli"],
                "collect",
                "--execute",
                "--archive",
                remote_archive,
                "--exchanges",
                ",".join(exchanges),
                "--market-types",
                ",".join(market_types),
                "--timeout-sec",
                str(args.timeout_sec),
                "--retries",
                str(args.retries),
            ]
            if args.effective_from:
                collect_command.extend(["--effective-from", args.effective_from])
            collected_result = run_command(
                ssh_command(args.collector_host, collect_command)
            )
            collected = parse_single_json_output(collected_result, "remote collector")
            scp_from(args.collector_host, remote_archive, local_archive)
            actual_archive_sha256 = snapshot.sha256_file(local_archive)
            if actual_archive_sha256 != collected.get("archive_sha256"):
                raise snapshot.SnapshotError(
                    "archive SHA-256 changed between collector and local host"
                )

            output_root = args.output_root.resolve()
            local_root, manifest = extract_snapshot_archive(local_archive, output_root)
            if manifest["snapshot_id"] != collected.get("snapshot_id"):
                raise snapshot.SnapshotError(
                    "downloaded manifest snapshot_id differs from collector"
                )
            archive_copy = local_root / "snapshot.tar.gz"
            shutil.copy2(local_archive, archive_copy)
            write_text(
                local_root / "snapshot.tar.gz.sha256",
                f"{actual_archive_sha256}  snapshot.tar.gz\n",
            )
            print(
                f"validated local snapshot {manifest['snapshot_id']} at {local_root}",
                file=sys.stderr,
            )

            loaded: dict[str, Any] | None = None
            if not args.skip_database:
                print(
                    f"loading snapshot {manifest['snapshot_id']} into PostgreSQL on {args.database_host}",
                    file=sys.stderr,
                )
                database_temp = remote_temp_dir(args.database_host, "load")
                database_files = copy_runtime_files(
                    args.database_host, database_temp, include_schema=True
                )
                database_archive = f"{database_temp}/instrument_snapshot.tar.gz"
                scp_to(args.database_host, local_archive, database_archive)
                load_command = [
                    "python3",
                    database_files["cli"],
                    "load",
                    "--execute",
                    "--archive",
                    database_archive,
                    "--schema-sql",
                    database_files["schema"],
                    "--database-env",
                    args.database_env,
                    "--database-url-env",
                    args.database_url_env,
                ]
                loaded_result = run_command(
                    ssh_command(args.database_host, load_command)
                )
                loaded = parse_single_json_output(loaded_result, "remote PostgreSQL loader")
                if loaded.get("snapshot_id") != manifest["snapshot_id"]:
                    raise snapshot.SnapshotError(
                        "PostgreSQL loader returned another snapshot_id"
                    )
            return {
                "snapshot_id": manifest["snapshot_id"],
                "captured_at": manifest["captured_at"],
                "effective_from": manifest["effective_from"],
                "local_snapshot": str(local_root),
                "archive_sha256": actual_archive_sha256,
                "raw_response_count": manifest["raw_response_count"],
                "instrument_count": manifest["instrument_count"],
                "scopes": manifest["scopes"],
                "postgresql": loaded,
            }
        finally:
            if collector_temp and not args.keep_remote_temp:
                cleanup_remote(args.collector_host, collector_temp)
            if database_temp and not args.keep_remote_temp:
                cleanup_remote(args.database_host, database_temp)


def add_selection_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--exchanges",
        action="append",
        default=[],
        help=f"comma-separated exchanges (default: {','.join(snapshot.VALID_EXCHANGES)})",
    )
    parser.add_argument(
        "--market-types",
        action="append",
        default=[],
        help=f"comma-separated market types (default: {','.join(snapshot.VALID_MARKET_TYPES)})",
    )
    parser.add_argument(
        "--effective-from",
        help="explicit ISO-8601 effectivity; default is capture time",
    )
    parser.add_argument(
        "--timeout-sec", type=float, default=snapshot.DEFAULT_TIMEOUT_SEC
    )
    parser.add_argument("--retries", type=int, default=snapshot.DEFAULT_RETRIES)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    sync_parser = subparsers.add_parser(
        "sync", help="collect remotely, retain locally, and load PostgreSQL"
    )
    add_selection_arguments(sync_parser)
    sync_parser.add_argument(
        "--collector-host", default=snapshot.DEFAULT_COLLECTOR_HOST
    )
    sync_parser.add_argument(
        "--database-host", default=snapshot.DEFAULT_DATABASE_HOST
    )
    sync_parser.add_argument("--database-env", default=snapshot.DEFAULT_DATABASE_ENV)
    sync_parser.add_argument(
        "--database-url-env", default=snapshot.DEFAULT_DATABASE_URL_ENV
    )
    sync_parser.add_argument(
        "--output-root", type=Path, default=snapshot.DEFAULT_OUTPUT_ROOT
    )
    sync_parser.add_argument(
        "--skip-database", action="store_true", help="collect and retain locally only"
    )
    sync_parser.add_argument(
        "--keep-remote-temp", action="store_true", help="retain remote staging directories"
    )
    sync_parser.add_argument(
        "--execute",
        action="store_true",
        help="perform REST, SSH transfers, and PostgreSQL writes",
    )

    collect_parser = subparsers.add_parser(
        "collect", help="internal collector mode used on the egress host"
    )
    add_selection_arguments(collect_parser)
    collect_parser.add_argument("--archive", type=Path, required=True)
    collect_parser.add_argument("--execute", action="store_true")

    load_parser = subparsers.add_parser(
        "load", help="internal PostgreSQL loader mode used on the database host"
    )
    load_parser.add_argument("--archive", type=Path, required=True)
    load_parser.add_argument(
        "--schema-sql", type=Path, default=snapshot.DEFAULT_SCHEMA_SQL
    )
    load_parser.add_argument(
        "--database-env", type=Path, default=Path(snapshot.DEFAULT_DATABASE_ENV)
    )
    load_parser.add_argument(
        "--database-url-env", default=snapshot.DEFAULT_DATABASE_URL_ENV
    )
    load_parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "sync":
            result = orchestrate(args)
        elif args.command == "collect":
            exchanges = validate_names(
                args.exchanges, snapshot.VALID_EXCHANGES, "exchange"
            )
            market_types = validate_names(
                args.market_types, snapshot.VALID_MARKET_TYPES, "market type"
            )
            if not args.execute:
                result = {
                    "dry_run": {
                        "archive": str(args.archive),
                        "exchanges": list(exchanges),
                        "market_types": list(market_types),
                    }
                }
            else:
                result = collect_snapshot(
                    args.archive,
                    exchanges=exchanges,
                    market_types=market_types,
                    effective_from=args.effective_from,
                    timeout_sec=args.timeout_sec,
                    retries=args.retries,
                )
        elif args.command == "load":
            if not args.execute:
                result = {
                    "dry_run": {
                        "archive": str(args.archive),
                        "schema_sql": str(args.schema_sql),
                        "database_env": str(args.database_env),
                        "database_url_env": args.database_url_env,
                    }
                }
            else:
                result = load_snapshot(
                    args.archive,
                    schema_sql=args.schema_sql,
                    database_env=args.database_env,
                    database_url_env=args.database_url_env,
                )
        else:
            parser.error(f"unsupported command: {args.command}")
            return 2
    except (snapshot.SnapshotError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
