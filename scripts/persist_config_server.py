#!/usr/bin/env python3
"""Small HTTP UI for editing persist.toml and viewing sync source heartbeats."""

from __future__ import annotations

import argparse
import html
import json
import os
import shutil
import tempfile
import time
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python < 3.11 fallback when tomli is installed.
    import tomli as tomllib  # type: ignore[no-redef]

DEFAULT_CONFIG = "config/persist.toml"
DEFAULT_STATUS = "data/persist_sync_status.json"
DEFAULT_BIND = "127.0.0.1"
DEFAULT_PORT = 8830
ALLOWED_TABLES = ["uniform_orders", "order_updates_unmatched", "trade_updates_unmatched"]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def now_us() -> int:
    return int(time.time() * 1_000_000)


def expand_path(path: str | Path) -> Path:
    return Path(path).expanduser()


def read_toml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return default_config()
    with path.open("rb") as fp:
        return tomllib.load(fp)


def read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            fp.write(text)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)


def backup_file(path: Path) -> str | None:
    if not path.exists():
        return None
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_suffix(path.suffix + f".{stamp}.bak")
    shutil.copy2(path, backup)
    return str(backup)


def default_config() -> dict[str, Any]:
    return {
        "center_db": "data/persist_sync_center",
        "batch_records": 1000,
        "batch_bytes": 4194304,
        "reconnect_delay_ms": 1000,
        "status_path": DEFAULT_STATUS,
        "sync_writes": False,
        "repair_enabled": True,
        "repair_interval_secs": 1800,
        "repair_lookback_hours": 24,
        "repair_bucket_us": 60000000,
        "sources": [{"id": "default", "url": "http://127.0.0.1:50051"}],
        "read_server": {
            "enabled": True,
            "bind": "0.0.0.0:8822",
            "secondary_dir": "data/persist_read_server_secondary",
            "source_id": "default",
            "max_concurrent": 8,
            "batch_rows": 50000,
            "max_window_sec": 3600,
            "max_result_rows": 5000000,
            "request_timeout_sec": 120,
            "default_format": "arrow_ipc",
            "enable_parquet": True,
            "allowed_tables": ALLOWED_TABLES[:],
        },
    }


def normalize_config(raw: dict[str, Any]) -> dict[str, Any]:
    cfg = default_config()
    cfg.update({k: v for k, v in raw.items() if k not in {"sources", "read_server"}})
    sources = raw.get("sources", cfg["sources"])
    if not isinstance(sources, list):
        sources = []
    cfg["sources"] = [
        {"id": str(src.get("id", "")).strip(), "url": str(src.get("url", "")).strip()}
        for src in sources
        if isinstance(src, dict)
    ]
    read = cfg["read_server"].copy()
    if isinstance(raw.get("read_server"), dict):
        read.update(raw["read_server"])
    cfg["read_server"] = read
    return cfg


def parse_bool(value: Any, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def parse_int(value: Any, *, default: int, minimum: int | None = None) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = default
    if minimum is not None:
        parsed = max(minimum, parsed)
    return parsed


def form_to_config(form: dict[str, list[str]]) -> dict[str, Any]:
    current = default_config()
    cfg: dict[str, Any] = {
        "center_db": first(form, "center_db", current["center_db"]),
        "batch_records": parse_int(first(form, "batch_records", current["batch_records"]), default=current["batch_records"], minimum=1),
        "batch_bytes": parse_int(first(form, "batch_bytes", current["batch_bytes"]), default=current["batch_bytes"], minimum=1),
        "reconnect_delay_ms": parse_int(first(form, "reconnect_delay_ms", current["reconnect_delay_ms"]), default=current["reconnect_delay_ms"], minimum=1),
        "status_path": first(form, "status_path", current["status_path"]),
        "sync_writes": parse_bool(first(form, "sync_writes", None)),
        "repair_enabled": parse_bool(first(form, "repair_enabled", None)),
        "repair_interval_secs": parse_int(first(form, "repair_interval_secs", current["repair_interval_secs"]), default=current["repair_interval_secs"], minimum=1),
        "repair_lookback_hours": parse_int(first(form, "repair_lookback_hours", current["repair_lookback_hours"]), default=current["repair_lookback_hours"], minimum=1),
        "repair_bucket_us": parse_int(first(form, "repair_bucket_us", current["repair_bucket_us"]), default=current["repair_bucket_us"], minimum=1),
    }

    source_ids = form.get("source_id", [])
    source_urls = form.get("source_url", [])
    sources = []
    seen = set()
    for source_id, url in zip(source_ids, source_urls):
        source_id = source_id.strip()
        url = url.strip()
        if not source_id and not url:
            continue
        if not source_id:
            raise ValueError("source id cannot be empty")
        if source_id in seen:
            raise ValueError(f"duplicate source id: {source_id}")
        if not url:
            raise ValueError(f"source url cannot be empty: {source_id}")
        seen.add(source_id)
        sources.append({"id": source_id, "url": url})
    if not sources:
        raise ValueError("at least one source is required")
    cfg["sources"] = sources

    allowed_tables = [v.strip() for v in form.get("allowed_tables", []) if v.strip()]
    cfg["read_server"] = {
        "enabled": parse_bool(first(form, "read_enabled", None)),
        "bind": first(form, "read_bind", current["read_server"]["bind"]),
        "secondary_dir": first(form, "read_secondary_dir", current["read_server"]["secondary_dir"]),
        "source_id": first(form, "read_source_id", sources[0]["id"]),
        "max_concurrent": parse_int(first(form, "read_max_concurrent", 8), default=8, minimum=1),
        "batch_rows": parse_int(first(form, "read_batch_rows", 50000), default=50000, minimum=1),
        "max_window_sec": parse_int(first(form, "read_max_window_sec", 3600), default=3600, minimum=1),
        "max_result_rows": parse_int(first(form, "read_max_result_rows", 5000000), default=5000000, minimum=1),
        "request_timeout_sec": parse_int(first(form, "read_request_timeout_sec", 120), default=120, minimum=1),
        "default_format": first(form, "read_default_format", "arrow_ipc"),
        "enable_parquet": parse_bool(first(form, "read_enable_parquet", None)),
        "allowed_tables": allowed_tables or ALLOWED_TABLES[:],
    }
    read_primary = first(form, "read_primary_dir", "").strip()
    if read_primary:
        cfg["read_server"]["primary_dir"] = read_primary
    return cfg


def first(form: dict[str, list[str]], key: str, default: Any) -> Any:
    values = form.get(key)
    if not values:
        return default
    return values[0]


def toml_quote(value: str) -> str:
    return json.dumps(value)


def toml_bool(value: bool) -> str:
    return "true" if value else "false"


def render_toml(cfg: dict[str, Any]) -> str:
    cfg = normalize_config(cfg)
    read = cfg["read_server"]
    lines = [
        "# Single persist config for the central recorder, read server, and config UI.",
        "# persist_sync_collector consumes the top-level fields; persist_read_server consumes [read_server].",
        "",
        f"center_db = {toml_quote(str(cfg['center_db']))}",
        f"batch_records = {int(cfg['batch_records'])}",
        f"batch_bytes = {int(cfg['batch_bytes'])}",
        f"reconnect_delay_ms = {int(cfg['reconnect_delay_ms'])}",
        f"status_path = {toml_quote(str(cfg.get('status_path') or DEFAULT_STATUS))}",
        f"sync_writes = {toml_bool(parse_bool(cfg.get('sync_writes')))}",
        "",
        f"repair_enabled = {toml_bool(parse_bool(cfg.get('repair_enabled'), default=True))}",
        f"repair_interval_secs = {int(cfg['repair_interval_secs'])}",
        f"repair_lookback_hours = {int(cfg['repair_lookback_hours'])}",
        f"repair_bucket_us = {int(cfg['repair_bucket_us'])}",
        "",
        "# Add one source per remote persist manager.",
        "# If there are multiple sources, read_server.source_id must be set explicitly.",
    ]
    for source in cfg["sources"]:
        lines.extend([
            "[[sources]]",
            f"id = {toml_quote(str(source['id']))}",
            f"url = {toml_quote(str(source['url']))}",
            "",
        ])
    lines.extend([
        "[read_server]",
        f"enabled = {toml_bool(parse_bool(read.get('enabled'), default=True))}",
        f"bind = {toml_quote(str(read.get('bind') or '0.0.0.0:8822'))}",
        "",
        "# Defaults to top-level center_db when omitted.",
    ])
    if read.get("primary_dir"):
        lines.append(f"primary_dir = {toml_quote(str(read['primary_dir']))}")
    else:
        lines.append(f"# primary_dir = {toml_quote(str(cfg['center_db']))}")
    lines.extend([
        f"secondary_dir = {toml_quote(str(read.get('secondary_dir') or 'data/persist_read_server_secondary'))}",
        "",
        "# Optional for a single [[sources]] entry; required when multiple sources exist.",
        f"source_id = {toml_quote(str(read.get('source_id') or cfg['sources'][0]['id']))}",
        "",
        f"max_concurrent = {int(read.get('max_concurrent') or 8)}",
        "",
        "# Server-side RocksDB scan/decode batch size only.",
        "# This does not split the HTTP response; each /v1/read request still returns one full Arrow/Parquet body.",
        f"batch_rows = {int(read.get('batch_rows') or 50000)}",
        "",
        "# Hard per-request time-window limit. For larger analysis ranges, use the Python SDK",
        "# read_*_range(..., window_sec=...) helpers to split requests by time.",
        f"max_window_sec = {int(read.get('max_window_sec') or 3600)}",
        f"max_result_rows = {int(read.get('max_result_rows') or 5000000)}",
        f"request_timeout_sec = {int(read.get('request_timeout_sec') or 120)}",
        "",
        f"default_format = {toml_quote(str(read.get('default_format') or 'arrow_ipc'))}",
        f"enable_parquet = {toml_bool(parse_bool(read.get('enable_parquet'), default=True))}",
        "",
        "allowed_tables = [",
    ])
    for table in read.get("allowed_tables") or ALLOWED_TABLES:
        lines.append(f"  {toml_quote(str(table))},")
    lines.extend([
        "]",
        "",
    ])
    return "\n".join(lines)


def read_status(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as fp:
            status = json.load(fp)
    except FileNotFoundError:
        return {"updated_at_us": None, "sources": {}, "recent_events": [], "missing": True}
    except json.JSONDecodeError as exc:
        return {"updated_at_us": None, "sources": {}, "recent_events": [], "error": str(exc)}
    status["missing"] = False
    return status


def us_to_iso(value: Any) -> str:
    if not value:
        return "-"
    try:
        return datetime.fromtimestamp(int(value) / 1_000_000, timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OSError):
        return "-"


def age_text(value: Any) -> str:
    if not value:
        return "-"
    age = max(0.0, time.time() - int(value) / 1_000_000)
    if age < 1:
        return f"{age * 1000:.0f} ms"
    if age < 60:
        return f"{age:.1f} s"
    if age < 3600:
        return f"{age / 60:.1f} min"
    return f"{age / 3600:.1f} h"


def status_class(source: dict[str, Any]) -> str:
    if source.get("status") == "error":
        return "bad"
    last_response = source.get("last_response_us")
    if not last_response:
        return "warn"
    age = time.time() - int(last_response) / 1_000_000
    if age > 120:
        return "warn"
    return "ok"


class PersistConfigServer(ThreadingHTTPServer):
    config_path: Path
    status_path: Path


class Handler(BaseHTTPRequestHandler):
    server: PersistConfigServer

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self.send_html(render_page(self.server.config_path, self.server.status_path))
        elif parsed.path == "/api/config":
            cfg = normalize_config(read_toml(self.server.config_path))
            self.send_json({"path": str(self.server.config_path), "config": cfg, "raw": read_text(self.server.config_path)})
        elif parsed.path == "/api/status":
            self.send_json({"path": str(self.server.status_path), "status": read_status(self.server.status_path), "now_us": now_us()})
        elif parsed.path == "/raw/config":
            self.send_text(read_text(self.server.config_path), content_type="text/plain; charset=utf-8")
        else:
            self.send_error(HTTPStatus.NOT_FOUND, "not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/config":
            self.send_error(HTTPStatus.NOT_FOUND, "not found")
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode("utf-8")
        content_type = self.headers.get("Content-Type", "")
        try:
            if "application/json" in content_type:
                cfg = json.loads(body)
            else:
                cfg = form_to_config(parse_qs(body, keep_blank_values=True))
            text = render_toml(cfg)
            parsed_cfg = tomllib.loads(text)
            backup = backup_file(self.server.config_path)
            atomic_write_text(self.server.config_path, text)
            self.send_json({"ok": True, "backup": backup, "config": normalize_config(parsed_cfg)})
        except Exception as exc:  # noqa: BLE001 - return validation errors to the UI.
            self.send_json({"ok": False, "error": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.log_date_time_string()} {self.client_address[0]} {fmt % args}")

    def send_json(self, payload: dict[str, Any], status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_text(self, text: str, *, content_type: str) -> None:
        body = text.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_html(self, text: str) -> None:
        self.send_text(text, content_type="text/html; charset=utf-8")


def render_page(config_path: Path, status_path: Path) -> str:
    cfg = normalize_config(read_toml(config_path))
    status = read_status(status_path)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Persist Config</title>
<style>{CSS}</style>
</head>
<body>
<header>
  <div>
    <h1>Persist Config</h1>
    <p>{h(config_path)} · status {h(status_path)}</p>
  </div>
  <div class="actions">
    <a class="button" href="/raw/config">Raw TOML</a>
    <button type="button" onclick="refreshStatus()">Refresh</button>
  </div>
</header>
<main>
  <section class="band">
    <div class="section-head">
      <h2>Source Heartbeats</h2>
      <span id="status-updated">updated {h(us_to_iso(status.get('updated_at_us')))}</span>
    </div>
    <div id="heartbeat-table">{render_heartbeat_table(status)}</div>
  </section>
  <section class="band">
    <div class="section-head">
      <h2>Config Editor</h2>
      <span id="save-state"></span>
    </div>
    {render_config_form(cfg)}
  </section>
  <section class="band raw">
    <div class="section-head"><h2>Recent Events</h2></div>
    <div id="events-table">{render_events_table(status)}</div>
  </section>
</main>
<script>{JS}</script>
</body>
</html>"""


def render_heartbeat_table(status: dict[str, Any]) -> str:
    sources = status.get("sources") or {}
    if not sources:
        msg = "status file not found" if status.get("missing") else "no source status yet"
        return f"<p class='empty'>{h(msg)}</p>"
    rows = []
    for source_id, source in sorted(sources.items()):
        cls = status_class(source)
        rows.append(
            "<tr>"
            f"<td><span class='dot {cls}'></span>{h(source_id)}</td>"
            f"<td>{h(source.get('endpoint', ''))}</td>"
            f"<td><span class='pill {cls}'>{h(source.get('status', '-'))}</span></td>"
            f"<td>{h(source.get('last_method') or '-')}</td>"
            f"<td>{h(age_text(source.get('last_response_us')))}</td>"
            f"<td>{h(source.get('last_seq', 0))}</td>"
            f"<td>{h(source.get('last_records', 0))}</td>"
            f"<td>{h(source.get('total_records', 0))}</td>"
            f"<td>{h(source.get('total_errors', 0))}</td>"
            f"<td class='err'>{h(source.get('last_error') or '')}</td>"
            "</tr>"
        )
    return """
<table>
<thead><tr><th>Source</th><th>Endpoint</th><th>Status</th><th>Method</th><th>Last Response</th><th>Seq</th><th>Last Records</th><th>Total Records</th><th>Errors</th><th>Last Error</th></tr></thead>
<tbody>""" + "".join(rows) + "</tbody></table>"


def render_events_table(status: dict[str, Any]) -> str:
    events = list(status.get("recent_events") or [])[-30:]
    if not events:
        return "<p class='empty'>no events yet</p>"
    rows = []
    for event in reversed(events):
        cls = "bad" if event.get("status") == "error" else ("req" if event.get("direction") == "request" else "ok")
        rows.append(
            "<tr>"
            f"<td>{h(us_to_iso(event.get('ts_us')))}</td>"
            f"<td>{h(event.get('source_id', ''))}</td>"
            f"<td>{h(event.get('direction', ''))}</td>"
            f"<td>{h(event.get('method', ''))}</td>"
            f"<td><span class='pill {cls}'>{h(event.get('status', ''))}</span></td>"
            f"<td>{h(event.get('first_seq', 0))}</td>"
            f"<td>{h(event.get('last_seq', 0))}</td>"
            f"<td>{h(event.get('records', 0))}</td>"
            f"<td class='err'>{h(event.get('error') or '')}</td>"
            "</tr>"
        )
    return """
<table>
<thead><tr><th>Time</th><th>Source</th><th>Dir</th><th>Method</th><th>Status</th><th>First Seq</th><th>Last Seq</th><th>Records</th><th>Error</th></tr></thead>
<tbody>""" + "".join(rows) + "</tbody></table>"


def render_config_form(cfg: dict[str, Any]) -> str:
    read = cfg["read_server"]
    source_rows = []
    for source in cfg["sources"]:
        source_rows.append(source_row(source.get("id", ""), source.get("url", "")))
    tables = read.get("allowed_tables") or ALLOWED_TABLES
    table_checks = "".join(
        f"<label class='check'><input type='checkbox' name='allowed_tables' value='{h(table)}' {'checked' if table in tables else ''}> {h(table)}</label>"
        for table in ALLOWED_TABLES
    )
    return f"""
<form id="config-form" method="post" action="/api/config">
  <div class="grid two">
    {field('center_db', 'Center DB', cfg.get('center_db'))}
    {field('status_path', 'Status JSON', cfg.get('status_path') or DEFAULT_STATUS)}
    {field('batch_records', 'Batch Records', cfg.get('batch_records'), 'number')}
    {field('batch_bytes', 'Batch Bytes', cfg.get('batch_bytes'), 'number')}
    {field('reconnect_delay_ms', 'Reconnect Delay Ms', cfg.get('reconnect_delay_ms'), 'number')}
    <label class="toggle"><input type="checkbox" name="sync_writes" {'checked' if cfg.get('sync_writes') else ''}> Sync writes</label>
  </div>
  <h3>Sources</h3>
  <div id="source-list">{''.join(source_rows)}</div>
  <button type="button" class="secondary" onclick="addSource()">Add source</button>
  <h3>Repair</h3>
  <div class="grid four">
    <label class="toggle"><input type="checkbox" name="repair_enabled" {'checked' if cfg.get('repair_enabled') else ''}> Enabled</label>
    {field('repair_interval_secs', 'Interval Sec', cfg.get('repair_interval_secs'), 'number')}
    {field('repair_lookback_hours', 'Lookback Hours', cfg.get('repair_lookback_hours'), 'number')}
    {field('repair_bucket_us', 'Bucket Us', cfg.get('repair_bucket_us'), 'number')}
  </div>
  <h3>Read Server</h3>
  <div class="grid three">
    <label class="toggle"><input type="checkbox" name="read_enabled" {'checked' if read.get('enabled') else ''}> Enabled</label>
    {field('read_bind', 'Bind', read.get('bind'))}
    {field('read_source_id', 'Source ID', read.get('source_id'))}
    {field('read_primary_dir', 'Primary Dir', read.get('primary_dir') or '')}
    {field('read_secondary_dir', 'Secondary Dir', read.get('secondary_dir'))}
    {select_field('read_default_format', 'Default Format', read.get('default_format') or 'arrow_ipc', ['arrow_ipc', 'parquet'])}
    {field('read_max_concurrent', 'Max Concurrent', read.get('max_concurrent'), 'number')}
    {field('read_batch_rows', 'Batch Rows', read.get('batch_rows'), 'number')}
    {field('read_max_window_sec', 'Max Window Sec', read.get('max_window_sec'), 'number')}
    {field('read_max_result_rows', 'Max Result Rows', read.get('max_result_rows'), 'number')}
    {field('read_request_timeout_sec', 'Timeout Sec', read.get('request_timeout_sec'), 'number')}
    <label class="toggle"><input type="checkbox" name="read_enable_parquet" {'checked' if read.get('enable_parquet') else ''}> Parquet</label>
  </div>
  <div class="checks">{table_checks}</div>
  <div class="form-actions"><button type="submit">Save config</button><button type="button" class="secondary" onclick="location.reload()">Reset</button></div>
</form>"""


def field(name: str, label: str, value: Any, typ: str = "text") -> str:
    return f"<label><span>{h(label)}</span><input type='{typ}' name='{h(name)}' value='{h(value if value is not None else '')}'></label>"


def select_field(name: str, label: str, value: str, options: list[str]) -> str:
    opts = "".join(f"<option value='{h(opt)}' {'selected' if opt == value else ''}>{h(opt)}</option>" for opt in options)
    return f"<label><span>{h(label)}</span><select name='{h(name)}'>{opts}</select></label>"


def source_row(source_id: str = "", url: str = "") -> str:
    return f"""<div class="source-row">
  <input name="source_id" placeholder="source id" value="{h(source_id)}">
  <input name="source_url" placeholder="http://127.0.0.1:50051" value="{h(url)}">
  <button type="button" class="icon" onclick="this.parentElement.remove()">Remove</button>
</div>"""


def h(value: Any) -> str:
    return html.escape(str(value), quote=True)


CSS = r"""
:root { color-scheme: light; --bg:#f6f7f9; --fg:#17202a; --muted:#667085; --line:#d8dee6; --panel:#ffffff; --ok:#16803c; --warn:#a15c00; --bad:#b42318; --req:#375dfb; }
* { box-sizing: border-box; }
body { margin:0; font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif; background:var(--bg); color:var(--fg); }
header { display:flex; justify-content:space-between; align-items:flex-end; gap:24px; padding:22px 28px; border-bottom:1px solid var(--line); background:#fff; }
h1 { margin:0; font-size:24px; }
p { margin:4px 0 0; color:var(--muted); }
main { padding:20px 28px 36px; display:grid; gap:18px; }
.band { background:var(--panel); border:1px solid var(--line); border-radius:8px; padding:18px; overflow:auto; }
.section-head { display:flex; justify-content:space-between; align-items:center; gap:16px; margin-bottom:14px; }
h2 { margin:0; font-size:18px; } h3 { margin:20px 0 10px; font-size:15px; }
button,.button { appearance:none; border:1px solid #1f2937; background:#1f2937; color:#fff; padding:8px 12px; border-radius:6px; text-decoration:none; cursor:pointer; font:inherit; }
button.secondary,.button.secondary { background:#fff; color:#1f2937; border-color:var(--line); }
button.icon { background:#fff; color:#1f2937; border-color:var(--line); padding:7px 10px; }
.actions,.form-actions { display:flex; gap:10px; align-items:center; }
table { width:100%; border-collapse:collapse; min-width:980px; }
th,td { text-align:left; padding:9px 10px; border-bottom:1px solid var(--line); vertical-align:top; white-space:nowrap; }
th { font-size:12px; color:var(--muted); font-weight:600; background:#fafbfc; }
.err { white-space:normal; max-width:420px; color:#7a271a; }
.dot { width:9px; height:9px; display:inline-block; border-radius:50%; margin-right:8px; background:var(--muted); }
.dot.ok,.pill.ok { background:var(--ok); } .dot.warn,.pill.warn { background:var(--warn); } .dot.bad,.pill.bad { background:var(--bad); } .pill.req { background:var(--req); }
.pill { display:inline-block; color:#fff; border-radius:999px; padding:2px 8px; font-size:12px; background:var(--muted); }
.grid { display:grid; gap:12px; } .grid.two { grid-template-columns:repeat(2,minmax(220px,1fr)); } .grid.three { grid-template-columns:repeat(3,minmax(180px,1fr)); } .grid.four { grid-template-columns:repeat(4,minmax(150px,1fr)); }
label span { display:block; color:var(--muted); font-size:12px; margin-bottom:4px; }
input,select { width:100%; border:1px solid var(--line); border-radius:6px; padding:8px 9px; font:inherit; background:#fff; color:var(--fg); }
.toggle { display:flex; align-items:center; gap:8px; min-height:38px; padding-top:18px; } .toggle input,.check input { width:auto; }
.source-row { display:grid; grid-template-columns:minmax(140px,220px) minmax(300px,1fr) auto; gap:10px; margin-bottom:8px; }
.checks { display:flex; flex-wrap:wrap; gap:12px; margin:14px 0; } .check { display:flex; align-items:center; gap:6px; }
.empty { padding:12px; border:1px dashed var(--line); border-radius:6px; color:var(--muted); }
@media (max-width: 900px) { header { align-items:flex-start; flex-direction:column; } .grid.two,.grid.three,.grid.four,.source-row { grid-template-columns:1fr; } table { min-width:760px; } }
"""


JS = r"""
async function refreshStatus() {
  const resp = await fetch('/api/status');
  const data = await resp.json();
  document.getElementById('heartbeat-table').innerHTML = renderHeartbeat(data.status || {});
  document.getElementById('events-table').innerHTML = renderEvents(data.status || {});
  document.getElementById('status-updated').textContent = 'updated ' + fmtTime((data.status || {}).updated_at_us);
}
function fmtTime(us) {
  if (!us) return '-';
  return new Date(Number(us) / 1000).toLocaleString();
}
function ageText(us) {
  if (!us) return '-';
  const age = Math.max(0, Date.now() - Number(us) / 1000);
  if (age < 1000) return Math.round(age) + ' ms';
  if (age < 60000) return (age / 1000).toFixed(1) + ' s';
  if (age < 3600000) return (age / 60000).toFixed(1) + ' min';
  return (age / 3600000).toFixed(1) + ' h';
}
function esc(v) { return String(v ?? '').replace(/[&<>'"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;',"'":'&#39;','"':'&quot;'}[c])); }
function cls(source) {
  if (source.status === 'error') return 'bad';
  if (!source.last_response_us) return 'warn';
  return (Date.now() - Number(source.last_response_us) / 1000) > 120000 ? 'warn' : 'ok';
}
function renderHeartbeat(status) {
  const sources = status.sources || {};
  const ids = Object.keys(sources).sort();
  if (!ids.length) return `<p class="empty">${status.missing ? 'status file not found' : 'no source status yet'}</p>`;
  return `<table><thead><tr><th>Source</th><th>Endpoint</th><th>Status</th><th>Method</th><th>Last Response</th><th>Seq</th><th>Last Records</th><th>Total Records</th><th>Errors</th><th>Last Error</th></tr></thead><tbody>` + ids.map(id => {
    const s = sources[id] || {}; const c = cls(s);
    return `<tr><td><span class="dot ${c}"></span>${esc(id)}</td><td>${esc(s.endpoint)}</td><td><span class="pill ${c}">${esc(s.status || '-')}</span></td><td>${esc(s.last_method || '-')}</td><td>${esc(ageText(s.last_response_us))}</td><td>${esc(s.last_seq || 0)}</td><td>${esc(s.last_records || 0)}</td><td>${esc(s.total_records || 0)}</td><td>${esc(s.total_errors || 0)}</td><td class="err">${esc(s.last_error || '')}</td></tr>`;
  }).join('') + '</tbody></table>';
}
function renderEvents(status) {
  const events = (status.recent_events || []).slice(-30).reverse();
  if (!events.length) return '<p class="empty">no events yet</p>';
  return `<table><thead><tr><th>Time</th><th>Source</th><th>Dir</th><th>Method</th><th>Status</th><th>First Seq</th><th>Last Seq</th><th>Records</th><th>Error</th></tr></thead><tbody>` + events.map(e => {
    const c = e.status === 'error' ? 'bad' : (e.direction === 'request' ? 'req' : 'ok');
    return `<tr><td>${esc(fmtTime(e.ts_us))}</td><td>${esc(e.source_id)}</td><td>${esc(e.direction)}</td><td>${esc(e.method)}</td><td><span class="pill ${c}">${esc(e.status)}</span></td><td>${esc(e.first_seq || 0)}</td><td>${esc(e.last_seq || 0)}</td><td>${esc(e.records || 0)}</td><td class="err">${esc(e.error || '')}</td></tr>`;
  }).join('') + '</tbody></table>';
}
function addSource() {
  const row = document.createElement('div');
  row.className = 'source-row';
  row.innerHTML = '<input name="source_id" placeholder="source id"><input name="source_url" placeholder="http://127.0.0.1:50051"><button type="button" class="icon" onclick="this.parentElement.remove()">Remove</button>';
  document.getElementById('source-list').appendChild(row);
}
document.getElementById('config-form').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const state = document.getElementById('save-state');
  state.textContent = 'saving...';
  const resp = await fetch('/api/config', { method: 'POST', body: new URLSearchParams(new FormData(ev.target)) });
  const data = await resp.json();
  state.textContent = data.ok ? 'saved' : ('error: ' + data.error);
  if (data.ok) setTimeout(() => location.reload(), 500);
});
setInterval(refreshStatus, 3000);
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Manage persist.toml and view gRPC sync heartbeats.")
    parser.add_argument("--config", default=str(repo_root() / DEFAULT_CONFIG), help="persist.toml path")
    parser.add_argument("--status", default=None, help="status json path; defaults to status_path in config")
    parser.add_argument("--bind", default=DEFAULT_BIND)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    args = parser.parse_args()

    config_path = expand_path(args.config)
    if not config_path.is_absolute():
        config_path = (Path.cwd() / config_path).resolve()
    cfg = normalize_config(read_toml(config_path))
    status_path = expand_path(args.status or str(cfg.get("status_path") or DEFAULT_STATUS))
    if not status_path.is_absolute():
        status_path = (Path.cwd() / status_path).resolve()

    server = PersistConfigServer((args.bind, args.port), Handler)
    server.config_path = config_path
    server.status_path = status_path
    print(f"persist config server listening at http://{args.bind}:{args.port}")
    print(f"config={config_path}")
    print(f"status={status_path}")
    server.serve_forever()


if __name__ == "__main__":
    main()
