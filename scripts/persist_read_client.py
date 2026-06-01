#!/usr/bin/env python3
"""Small Python client for persist_read_server.

HTTP uses the Python standard library. pyarrow/pandas are imported only by
methods that decode into Arrow or DataFrame objects.
"""
from __future__ import annotations

import argparse
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone, tzinfo
from io import BytesIO
from pathlib import Path
from typing import Any, Iterator, Literal, Mapping, Sequence
from zoneinfo import ZoneInfo

TableName = Literal["uniform_orders", "order_updates_unmatched", "trade_updates_unmatched"]
OutputFormat = Literal["arrow_ipc", "parquet"]
TimeValue = int | str | datetime
DEFAULT_TZ = "Asia/Shanghai"


class PersistReadError(RuntimeError):
    """Raised when persist_read_server returns an error or invalid response."""

    def __init__(
        self,
        message: str,
        *,
        status: int | None = None,
        error: str | None = None,
        url: str | None = None,
        body: bytes | None = None,
        retry_after: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.error = error
        self.url = url
        self.body = body
        self.retry_after = retry_after


class PersistReadRateLimited(PersistReadError):
    """Raised for HTTP 429 responses."""


@dataclass(frozen=True)
class SchemaInfo:
    table: str
    columns: tuple[str, ...]
    formats: tuple[str, ...]
    source_id: str | None = None


@dataclass(frozen=True)
class ReadWindow:
    start_us: int
    end_us: int


class PersistReadClient:
    """Client for persist_read_server.

    A single read_* request maps to one /v1/read HTTP response. Server-side
    batch_rows only controls RocksDB scan/decode batching; it is not HTTP
    streaming. Use read_*_range(..., window_sec=...) to split large analysis
    ranges into multiple requests.
    """

    def __init__(self, base_url: str, *, timeout_sec: float = 120.0, tz: str | tzinfo = DEFAULT_TZ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = float(timeout_sec)
        self.tz = resolve_timezone(tz)
        if not self.base_url:
            raise ValueError("base_url must not be empty")

    def health(self) -> Mapping[str, Any]:
        return self._get_json("/healthz", {})

    def schema(self, table: str) -> SchemaInfo:
        raw = self._get_json("/v1/schema", {"table": table})
        return SchemaInfo(
            table=str(raw["table"]),
            columns=tuple(str(v) for v in raw.get("columns", ())),
            formats=tuple(str(v) for v in raw.get("formats", ())),
            source_id=raw.get("source_id"),
        )

    def read_bytes(
        self,
        table: str,
        start: TimeValue | None = None,
        end: TimeValue | None = None,
        *,
        start_us: int | None = None,
        end_us: int | None = None,
        columns: Sequence[str] | str | None = None,
        format: OutputFormat = "arrow_ipc",
        timeout_sec: float | None = None,
        tz: str | tzinfo | None = None,
    ) -> bytes:
        params = self._read_params(
            table,
            start,
            end,
            start_us=start_us,
            end_us=end_us,
            columns=columns,
            format=format,
            tz=tz,
        )
        return self._get_bytes("/v1/read", params, timeout_sec=timeout_sec)

    def read_arrow(
        self,
        table: str,
        start: TimeValue | None = None,
        end: TimeValue | None = None,
        *,
        start_us: int | None = None,
        end_us: int | None = None,
        columns: Sequence[str] | str | None = None,
        timeout_sec: float | None = None,
        tz: str | tzinfo | None = None,
    ):
        pa, ipc = _require_pyarrow_ipc()
        data = self.read_bytes(
            table,
            start,
            end,
            start_us=start_us,
            end_us=end_us,
            columns=columns,
            format="arrow_ipc",
            timeout_sec=timeout_sec,
            tz=tz,
        )
        return ipc.open_stream(pa.BufferReader(data)).read_all()

    def read_pandas(
        self,
        table: str,
        start: TimeValue | None = None,
        end: TimeValue | None = None,
        *,
        start_us: int | None = None,
        end_us: int | None = None,
        columns: Sequence[str] | str | None = None,
        format: OutputFormat = "arrow_ipc",
        timeout_sec: float | None = None,
        tz: str | tzinfo | None = None,
        to_pandas_kwargs: Mapping[str, Any] | None = None,
    ):
        if format == "arrow_ipc":
            arrow_table = self.read_arrow(
                table,
                start,
                end,
                start_us=start_us,
                end_us=end_us,
                columns=columns,
                timeout_sec=timeout_sec,
                tz=tz,
            )
            return arrow_table.to_pandas(**dict(to_pandas_kwargs or {}))

        if format == "parquet":
            pd = _require_pandas()
            data = self.read_bytes(
                table,
                start,
                end,
                start_us=start_us,
                end_us=end_us,
                columns=columns,
                format="parquet",
                timeout_sec=timeout_sec,
                tz=tz,
            )
            return pd.read_parquet(BytesIO(data))

        raise ValueError(f"unsupported format: {format}")

    def read_arrow_range(
        self,
        table: str,
        start: TimeValue,
        end: TimeValue,
        *,
        columns: Sequence[str] | str | None = None,
        window_sec: int = 3600,
        timeout_sec: float | None = None,
        tz: str | tzinfo | None = None,
    ):
        """Read a long range by splitting it into window_sec HTTP requests."""
        pa, _ipc = _require_pyarrow_ipc()
        tables = []
        for window in iter_windows(start, end, window_sec=window_sec, tz=self._call_tz(tz)):
            tables.append(
                self.read_arrow(
                    table,
                    start_us=window.start_us,
                    end_us=window.end_us,
                    columns=columns,
                    timeout_sec=timeout_sec,
                )
            )
        try:
            return pa.concat_tables(tables, promote_options="default")
        except TypeError:
            return pa.concat_tables(tables, promote=True)

    def read_pandas_range(
        self,
        table: str,
        start: TimeValue,
        end: TimeValue,
        *,
        columns: Sequence[str] | str | None = None,
        window_sec: int = 3600,
        timeout_sec: float | None = None,
        tz: str | tzinfo | None = None,
        to_pandas_kwargs: Mapping[str, Any] | None = None,
    ):
        """Read a long range by splitting requests, then convert the concatenated Arrow table to pandas."""
        table_obj = self.read_arrow_range(
            table,
            start,
            end,
            columns=columns,
            window_sec=window_sec,
            timeout_sec=timeout_sec,
            tz=tz,
        )
        return table_obj.to_pandas(**dict(to_pandas_kwargs or {}))

    def _read_params(
        self,
        table: str,
        start: TimeValue | None,
        end: TimeValue | None,
        *,
        start_us: int | None,
        end_us: int | None,
        columns: Sequence[str] | str | None,
        format: str,
        tz: str | tzinfo | None,
    ) -> dict[str, str]:
        resolved_start, resolved_end = self._resolve_window(start, end, start_us=start_us, end_us=end_us, tz=tz)
        if resolved_end <= resolved_start:
            raise ValueError(f"end must be greater than start, got {resolved_start}..{resolved_end}")
        params = {
            "table": table,
            "start_us": str(resolved_start),
            "end_us": str(resolved_end),
            "format": format,
        }
        columns_csv = normalize_columns(columns)
        if columns_csv:
            params["columns"] = columns_csv
        return params

    def _resolve_window(
        self,
        start: TimeValue | None,
        end: TimeValue | None,
        *,
        start_us: int | None,
        end_us: int | None,
        tz: str | tzinfo | None,
    ) -> tuple[int, int]:
        if start_us is not None or end_us is not None:
            if start is not None or end is not None:
                raise ValueError("use either start/end or start_us/end_us, not both")
            if start_us is None or end_us is None:
                raise ValueError("start_us and end_us must be provided together")
            return int(start_us), int(end_us)

        if start is None or end is None:
            raise ValueError("start and end are required")
        call_tz = self._call_tz(tz)
        return to_unix_us(start, tz=call_tz), to_unix_us(end, tz=call_tz)

    def _call_tz(self, tz: str | tzinfo | None) -> tzinfo:
        return self.tz if tz is None else resolve_timezone(tz)

    def _get_json(self, path: str, params: Mapping[str, str]) -> Mapping[str, Any]:
        body = self._get_bytes(path, params)
        try:
            value = json.loads(body.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - keep SDK error surface simple.
            raise PersistReadError(f"invalid JSON response from {path}: {exc}", body=body) from exc
        if not isinstance(value, dict):
            raise PersistReadError(f"expected JSON object from {path}, got {type(value).__name__}")
        return value

    def _get_bytes(
        self,
        path: str,
        params: Mapping[str, str],
        *,
        timeout_sec: float | None = None,
    ) -> bytes:
        url = build_url(self.base_url, path, params)
        req = urllib.request.Request(url, method="GET")
        try:
            timeout = self.timeout_sec if timeout_sec is None else timeout_sec
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as exc:
            body = exc.read()
            raise _http_error(exc, body, url) from exc
        except urllib.error.URLError as exc:
            raise PersistReadError(f"request failed: {exc}", url=url) from exc
        except TimeoutError as exc:
            raise PersistReadError(f"request timed out: {url}", url=url) from exc


def build_url(base_url: str, path: str, params: Mapping[str, str]) -> str:
    query = urllib.parse.urlencode(params)
    return f"{base_url.rstrip('/')}{path}?{query}" if query else f"{base_url.rstrip('/')}{path}"


def normalize_columns(columns: Sequence[str] | str | None) -> str | None:
    if columns is None:
        return None
    if isinstance(columns, str):
        out = ",".join(part.strip() for part in columns.split(",") if part.strip())
    else:
        out = ",".join(str(part).strip() for part in columns if str(part).strip())
    return out or None


def resolve_timezone(value: str | tzinfo) -> tzinfo:
    if isinstance(value, tzinfo):
        return value
    raw = value.strip()
    upper = raw.upper()
    lower = raw.lower()
    if upper in {"UTC", "Z"}:
        return timezone.utc
    if lower in {"sh", "shanghai", "asia/shanghai", "cn", "china"}:
        try:
            return ZoneInfo("Asia/Shanghai")
        except Exception:
            return timezone(timedelta(hours=8), "Asia/Shanghai")
    return ZoneInfo(raw)


def to_unix_us(value: TimeValue, *, tz: str | tzinfo = DEFAULT_TZ) -> int:
    default_tz = resolve_timezone(tz)
    if isinstance(value, bool):
        raise TypeError("boolean timestamps are not supported")
    if isinstance(value, int):
        return value
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        dt = parse_datetime(value)
    else:
        raise TypeError(f"unsupported timestamp type: {type(value).__name__}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=default_tz)
    return int(dt.timestamp() * 1_000_000)


def parse_datetime(value: str) -> datetime:
    raw = value.strip()
    if not raw:
        raise ValueError("time string must not be empty")
    if raw.isdigit():
        return datetime.fromtimestamp(int(raw) / 1_000_000, tz=timezone.utc)
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(
            "time must be int unix-us, datetime, or string like '2026-05-31 09:30:00'"
        ) from exc


def iter_windows(start: TimeValue, end: TimeValue, *, window_sec: int, tz: str | tzinfo = DEFAULT_TZ) -> Iterator[ReadWindow]:
    start_us = to_unix_us(start, tz=tz)
    end_us = to_unix_us(end, tz=tz)
    if end_us <= start_us:
        raise ValueError(f"end must be greater than start, got {start_us}..{end_us}")
    step = int(window_sec) * 1_000_000
    if step <= 0:
        raise ValueError("window_sec must be positive")
    cur = start_us
    while cur < end_us:
        nxt = min(cur + step, end_us)
        yield ReadWindow(cur, nxt)
        cur = nxt


def _http_error(exc: urllib.error.HTTPError, body: bytes, url: str) -> PersistReadError:
    error = None
    message = body.decode("utf-8", errors="replace")
    try:
        payload = json.loads(message)
        if isinstance(payload, dict):
            error = str(payload.get("error") or "") or None
            message = str(payload.get("message") or message)
    except json.JSONDecodeError:
        pass

    retry_after = exc.headers.get("Retry-After") if exc.headers else None
    cls = PersistReadRateLimited if exc.code == 429 else PersistReadError
    return cls(
        f"HTTP {exc.code}: {message}",
        status=exc.code,
        error=error,
        url=url,
        body=body,
        retry_after=retry_after,
    )


def _require_pyarrow_ipc():
    try:
        import pyarrow as pa
        import pyarrow.ipc as ipc
    except ImportError as exc:
        raise RuntimeError("missing dependency: pip install pyarrow") from exc
    return pa, ipc


def _require_pandas():
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("missing dependency: pip install pandas pyarrow") from exc
    return pd


def _parse_columns(raw: str | None) -> list[str] | None:
    if raw is None or not raw.strip():
        return None
    return [part.strip() for part in raw.split(",") if part.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Pull data from persist_read_server")
    parser.add_argument("--base-url", default="http://127.0.0.1:8822")
    parser.add_argument("--timeout-sec", type=float, default=120.0)
    parser.add_argument("--tz", default=DEFAULT_TZ, help="timezone for naive time strings, e.g. Asia/Shanghai or UTC")

    sub = parser.add_subparsers(dest="command", required=True)

    schema_cmd = sub.add_parser("schema", help="show schema for a table")
    schema_cmd.add_argument("--table", required=True)

    read_cmd = sub.add_parser("read", help="pull one window and write bytes to a file")
    read_cmd.add_argument("--table", required=True)
    read_cmd.add_argument("--start", help="start time, e.g. '2026-05-31 09:30:00'")
    read_cmd.add_argument("--end", help="end time, e.g. '2026-05-31 10:30:00'")
    read_cmd.add_argument("--start-us", type=int, help="legacy unix microsecond start")
    read_cmd.add_argument("--end-us", type=int, help="legacy unix microsecond end")
    read_cmd.add_argument("--columns", help="comma-separated columns; omitted means all columns")
    read_cmd.add_argument("--format", choices=("arrow_ipc", "parquet"), default="arrow_ipc")
    read_cmd.add_argument("--out", type=Path, required=True)

    args = parser.parse_args(argv)
    client = PersistReadClient(args.base_url, timeout_sec=args.timeout_sec, tz=args.tz)

    if args.command == "schema":
        schema = client.schema(args.table)
        print(json.dumps(schema.__dict__, ensure_ascii=False, indent=2))
        return 0

    if args.command == "read":
        data = client.read_bytes(
            args.table,
            args.start,
            args.end,
            start_us=args.start_us,
            end_us=args.end_us,
            columns=_parse_columns(args.columns),
            format=args.format,
        )
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_bytes(data)
        print(f"wrote {len(data)} bytes to {args.out}")
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
