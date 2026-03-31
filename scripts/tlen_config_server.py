#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TLEN 配置服务器（tlen_config_server）。

用途：
  - 以 HTTP + 简单页面的方式管理 MM tlen threshold。
  - 支持 open/hedge 双 venue 组合。
  - 当前默认支持：
      - binance-margin
      - binance-futures
      - okex-margin
      - okex-futures

Redis key 约定：
  <hedge_venue_prefix>_<open_venue_prefix>:mm:tlen_threshold

示例：
  python scripts/tlen_config_server.py
  python scripts/tlen_config_server.py --port 18161 --redis-db 15
"""

from __future__ import annotations

import argparse
import json
import math
import os
import socket
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Iterable, List, Optional, Tuple

SERVICE_NAME = "tlen_config_server"
DEFAULT_PORT = 18161
DEFAULT_OPEN_VENUE = "binance-futures"
DEFAULT_HEDGE_VENUE = "binance-futures"
SUPPORTED_VENUES = [
    "binance-margin",
    "binance-futures",
    "okex-margin",
    "okex-futures",
]
QUOTE_ASSETS = ("USDT", "USDC", "BUSD", "FDUSD", "USD")


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def now_ts_ms() -> int:
    return int(time.time() * 1000)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def json_dumps(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=False).encode("utf-8")


def venue_prefix(venue: str) -> str:
    return venue.replace("-", "_")


def normalize_exchange(exchange: str) -> str:
    value = (exchange or "").strip().lower()
    if value == "okx":
        value = "okex"
    return value


def normalize_venue(value: str) -> Optional[str]:
    raw = (value or "").strip().lower().replace("_", "-")
    if not raw:
        return None
    parts = raw.split("-", 1)
    if len(parts) != 2:
        return None
    exchange, market = parts
    normalized = f"{normalize_exchange(exchange)}-{market}"
    if normalized not in SUPPORTED_VENUES:
        return None
    return normalized


def split_assets(symbol: str) -> Tuple[str, str]:
    text = "".join(ch for ch in (symbol or "").upper() if ch.isalnum())
    for quote in QUOTE_ASSETS:
        if text.endswith(quote) and len(text) > len(quote):
            return text[: -len(quote)], quote
    return text, "USDT"


def normalize_symbol_for_open_venue(symbol: str, open_venue: str) -> str:
    venue = normalize_venue(open_venue)
    if venue is None:
        raise ValueError(f"unsupported open_venue: {open_venue}")

    text = (symbol or "").strip().upper().replace("_", "-").replace("/", "-")
    if not text:
        raise ValueError("symbol is empty")

    if venue == "okex-margin":
        if text.endswith("-SWAP"):
            text = text[: -len("-SWAP")]
        if "-" in text:
            return text
        base, quote = split_assets(text)
        return f"{base}-{quote}"

    if venue == "okex-futures":
        if text.endswith("-SWAP"):
            return text
        if "-" in text:
            return f"{text}-SWAP"
        base, quote = split_assets(text)
        return f"{base}-{quote}-SWAP"

    # Binance margin/futures use compact symbols.
    return text.replace("-", "").replace("SWAP", "")


def parse_threshold_value(raw_value: object) -> float:
    try:
        value = float(raw_value)
    except Exception as exc:
        raise ValueError(f"invalid threshold value: {raw_value!r}") from exc
    if not math.isfinite(value) or value < 0.0:
        raise ValueError(f"threshold must be finite and >= 0: {raw_value!r}")
    return value


def redis_key(open_venue: str, hedge_venue: str) -> str:
    open_norm = normalize_venue(open_venue)
    hedge_norm = normalize_venue(hedge_venue)
    if open_norm is None:
        raise ValueError(f"unsupported open_venue: {open_venue}")
    if hedge_norm is None:
        raise ValueError(f"unsupported hedge_venue: {hedge_venue}")
    return f"{venue_prefix(hedge_norm)}_{venue_prefix(open_norm)}:mm:tlen_threshold"


def meta_key(key: str) -> str:
    return f"{key}:meta"


def parse_symbol_filters(params: Dict[str, List[str]]) -> List[str]:
    raw_values = params.get("symbol", []) + params.get("symbols", [])
    results: List[str] = []
    for raw in raw_values:
        for part in raw.split(","):
            value = part.strip()
            if value:
                results.append(value)
    return results


def decode_hash(data: Dict[object, object]) -> Dict[str, str]:
    decoded: Dict[str, str] = {}
    for raw_key, raw_value in data.items():
        key = raw_key.decode("utf-8", "ignore") if isinstance(raw_key, bytes) else str(raw_key)
        value = (
            raw_value.decode("utf-8", "ignore") if isinstance(raw_value, bytes) else str(raw_value)
        )
        decoded[key] = value
    return decoded


def build_rows_payload(
    data: Dict[str, str], open_venue: str, symbol_filters: Iterable[str]
) -> Tuple[List[Dict[str, object]], int]:
    wanted = {
        normalize_symbol_for_open_venue(symbol, open_venue)
        for symbol in symbol_filters
        if str(symbol).strip()
    }
    rows: List[Dict[str, object]] = []
    bad_fields = 0
    for symbol in sorted(data.keys()):
        if wanted and symbol not in wanted:
            continue
        raw_value = data[symbol]
        try:
            value = parse_threshold_value(raw_value)
        except ValueError:
            bad_fields += 1
            rows.append(
                {
                    "symbol": symbol,
                    "tlen_threshold": raw_value,
                    "valid": False,
                }
            )
            continue
        rows.append(
            {
                "symbol": symbol,
                "tlen_threshold": value,
                "valid": True,
            }
        )
    return rows, bad_fields


def coerce_rows(payload: dict, open_venue: str) -> List[Tuple[str, float]]:
    rows_raw = payload.get("rows")
    thresholds_raw = payload.get("thresholds")

    rows: List[Tuple[str, float]] = []
    if isinstance(thresholds_raw, dict):
        for raw_symbol, raw_value in thresholds_raw.items():
            symbol = normalize_symbol_for_open_venue(str(raw_symbol), open_venue)
            value = parse_threshold_value(raw_value)
            rows.append((symbol, value))
    elif isinstance(rows_raw, list):
        for item in rows_raw:
            if not isinstance(item, dict):
                raise ValueError("rows item must be an object")
            symbol = normalize_symbol_for_open_venue(str(item.get("symbol", "")), open_venue)
            value = parse_threshold_value(item.get("tlen_threshold"))
            rows.append((symbol, value))
    else:
        raise ValueError("payload must contain either object field 'thresholds' or array field 'rows'")

    deduped: Dict[str, float] = {}
    for symbol, value in rows:
        deduped[symbol] = value
    return sorted(deduped.items())


@dataclass
class ServerConfig:
    host: str
    port: int
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    default_open_venue: str
    default_hedge_venue: str


class TlenConfigStore:
    def __init__(self, config: ServerConfig):
        redis_mod = try_import_redis()
        if redis_mod is None:
            raise RuntimeError("redis package is not installed; run 'pip install redis'")
        self._redis = redis_mod.Redis(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            password=config.redis_password,
            decode_responses=True,
        )

    def fetch(
        self,
        open_venue: str,
        hedge_venue: str,
        symbol_filters: Iterable[str],
    ) -> Dict[str, object]:
        key = redis_key(open_venue, hedge_venue)
        rows, bad_fields = build_rows_payload(
            decode_hash(self._redis.hgetall(key)),
            open_venue,
            symbol_filters,
        )
        meta = decode_hash(self._redis.hgetall(meta_key(key)))
        return {
            "redis_key": key,
            "meta_key": meta_key(key),
            "open_venue": open_venue,
            "hedge_venue": hedge_venue,
            "count": len(rows),
            "bad_fields": bad_fields,
            "rows": rows,
            "meta": meta,
        }

    def replace(
        self,
        open_venue: str,
        hedge_venue: str,
        rows: List[Tuple[str, float]],
        allow_empty: bool,
    ) -> Dict[str, object]:
        if not rows and not allow_empty:
            raise ValueError("replace with empty rows requires allow_empty=true")
        key = redis_key(open_venue, hedge_venue)
        self._redis.delete(key)
        if rows:
            mapping = {symbol: f"{value:.8f}" for symbol, value in rows}
            self._redis.hset(key, mapping=mapping)
        self._write_meta(key, open_venue, hedge_venue, len(rows), "replace")
        return self.fetch(open_venue, hedge_venue, [])

    def upsert(
        self,
        open_venue: str,
        hedge_venue: str,
        rows: List[Tuple[str, float]],
    ) -> Dict[str, object]:
        if not rows:
            raise ValueError("upsert rows cannot be empty")
        key = redis_key(open_venue, hedge_venue)
        mapping = {symbol: f"{value:.8f}" for symbol, value in rows}
        self._redis.hset(key, mapping=mapping)
        row_count = self._redis.hlen(key)
        self._write_meta(key, open_venue, hedge_venue, int(row_count), "upsert")
        return self.fetch(open_venue, hedge_venue, [])

    def delete_symbols(
        self,
        open_venue: str,
        hedge_venue: str,
        symbols: Iterable[str],
    ) -> Dict[str, object]:
        key = redis_key(open_venue, hedge_venue)
        normalized = [
            normalize_symbol_for_open_venue(symbol, open_venue)
            for symbol in symbols
            if str(symbol).strip()
        ]
        if not normalized:
            raise ValueError("delete requires at least one symbol")
        self._redis.hdel(key, *normalized)
        row_count = self._redis.hlen(key)
        self._write_meta(key, open_venue, hedge_venue, int(row_count), "delete")
        return self.fetch(open_venue, hedge_venue, [])

    def _write_meta(
        self,
        key: str,
        open_venue: str,
        hedge_venue: str,
        row_count: int,
        action: str,
    ) -> None:
        payload = {
            "service": SERVICE_NAME,
            "updated_at_iso": now_iso(),
            "updated_at_ts_ms": str(now_ts_ms()),
            "hostname": socket.gethostname(),
            "pid": str(os.getpid()),
            "open_venue": open_venue,
            "hedge_venue": hedge_venue,
            "row_count": str(row_count),
            "last_action": action,
        }
        self._redis.hset(meta_key(key), mapping=payload)


def page_html(config: ServerConfig) -> str:
    venue_options = json.dumps(SUPPORTED_VENUES, ensure_ascii=False)
    default_open = json.dumps(config.default_open_venue, ensure_ascii=False)
    default_hedge = json.dumps(config.default_hedge_venue, ensure_ascii=False)
    service_name = json.dumps(SERVICE_NAME, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{SERVICE_NAME}</title>
  <style>
    :root {{
      --bg: #0f172a;
      --panel: #111827;
      --panel-2: #172033;
      --line: #2b3648;
      --text: #e5edf7;
      --muted: #95a4b8;
      --accent: #26c281;
      --danger: #ff6b6b;
      --warn: #f5b642;
      --input: #0b1220;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace;
      background:
        radial-gradient(circle at top left, rgba(38, 194, 129, 0.14), transparent 24%),
        linear-gradient(180deg, #0b1220 0%, #0f172a 100%);
      color: var(--text);
    }}
    .wrap {{
      width: min(1180px, calc(100vw - 32px));
      margin: 20px auto 40px;
    }}
    .card {{
      background: rgba(17, 24, 39, 0.96);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.28);
      margin-bottom: 16px;
    }}
    h1 {{
      font-size: 20px;
      margin: 0 0 8px;
    }}
    p, label, th, td, button, input, select, textarea {{
      font-size: 13px;
    }}
    .muted {{
      color: var(--muted);
    }}
    .toolbar {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
      align-items: end;
    }}
    .field {{
      display: flex;
      flex-direction: column;
      gap: 6px;
    }}
    input, select, textarea {{
      width: 100%;
      background: var(--input);
      color: var(--text);
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 12px;
      outline: none;
    }}
    textarea {{
      min-height: 150px;
      resize: vertical;
      line-height: 1.5;
    }}
    .actions {{
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 12px;
    }}
    button {{
      cursor: pointer;
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px 14px;
      background: var(--panel-2);
      color: var(--text);
    }}
    button.primary {{
      background: rgba(38, 194, 129, 0.14);
      border-color: rgba(38, 194, 129, 0.5);
      color: #bbf7d0;
    }}
    button.danger {{
      background: rgba(255, 107, 107, 0.12);
      border-color: rgba(255, 107, 107, 0.35);
      color: #fecaca;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 12px;
      background: rgba(11, 18, 32, 0.8);
      border-radius: 12px;
      overflow: hidden;
    }}
    th, td {{
      border-bottom: 1px solid var(--line);
      padding: 10px;
      text-align: left;
      vertical-align: middle;
    }}
    tr:last-child td {{
      border-bottom: none;
    }}
    td input {{
      padding: 8px 10px;
      min-width: 0;
    }}
    .meta {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 16px;
    }}
    .status {{
      margin-top: 10px;
      white-space: pre-wrap;
      color: var(--muted);
    }}
    .ok {{
      color: var(--accent);
    }}
    .warn {{
      color: var(--warn);
    }}
    .err {{
      color: var(--danger);
    }}
    @media (max-width: 860px) {{
      .toolbar, .meta {{
        grid-template-columns: 1fr;
      }}
      .wrap {{
        width: calc(100vw - 20px);
      }}
      table {{
        display: block;
        overflow-x: auto;
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>{SERVICE_NAME}</h1>
      <div class="muted">共享 TLEN threshold 管理服务。Redis key 按 <code>&lt;hedge&gt;_&lt;open&gt;:mm:tlen_threshold</code> 写入。</div>
    </div>

    <div class="card">
      <div class="toolbar">
        <div class="field">
          <label for="openVenue">open_venue</label>
          <select id="openVenue"></select>
        </div>
        <div class="field">
          <label for="hedgeVenue">hedge_venue</label>
          <select id="hedgeVenue"></select>
        </div>
        <div class="field">
          <label for="symbolFilter">symbol filter</label>
          <input id="symbolFilter" placeholder="BTCUSDT, ETHUSDT" />
        </div>
        <div class="field">
          <label>redis key</label>
          <input id="redisKey" readonly />
        </div>
      </div>
      <div class="actions">
        <button class="primary" id="loadBtn">Load</button>
        <button id="addRowBtn">Add Row</button>
        <button id="exportBtn">Export JSON</button>
        <button id="importBtn">Import JSON</button>
        <button class="primary" id="saveBtn">Save Replace</button>
      </div>
      <div id="status" class="status"></div>
    </div>

    <div class="card">
      <div class="muted">批量 JSON 格式：<code>{{"BTCUSDT": 123.45}}</code>。OKEX 建议直接填目标 venue 格式，例如 <code>BTC-USDT</code> 或 <code>BTC-USDT-SWAP</code>。</div>
      <textarea id="bulkJson" spellcheck="false"></textarea>
    </div>

    <div class="card">
      <div class="meta">
        <div>count: <span id="rowCount">0</span></div>
        <div>bad_fields: <span id="badFields">0</span></div>
        <div>meta_key: <span id="metaKey" class="muted">-</span></div>
        <div>updated_at: <span id="updatedAt" class="muted">-</span></div>
      </div>
      <table>
        <thead>
          <tr>
            <th style="width: 46%">symbol</th>
            <th style="width: 34%">tlen_threshold</th>
            <th style="width: 20%">action</th>
          </tr>
        </thead>
        <tbody id="tableBody"></tbody>
      </table>
    </div>
  </div>

  <script>
    const SERVICE_NAME = {service_name};
    const SUPPORTED_VENUES = {venue_options};
    const DEFAULT_OPEN_VENUE = {default_open};
    const DEFAULT_HEDGE_VENUE = {default_hedge};

    const openVenueEl = document.getElementById('openVenue');
    const hedgeVenueEl = document.getElementById('hedgeVenue');
    const symbolFilterEl = document.getElementById('symbolFilter');
    const redisKeyEl = document.getElementById('redisKey');
    const bulkJsonEl = document.getElementById('bulkJson');
    const tableBodyEl = document.getElementById('tableBody');
    const statusEl = document.getElementById('status');
    const rowCountEl = document.getElementById('rowCount');
    const badFieldsEl = document.getElementById('badFields');
    const metaKeyEl = document.getElementById('metaKey');
    const updatedAtEl = document.getElementById('updatedAt');

    function fillVenueSelect(selectEl, value) {{
      selectEl.innerHTML = '';
      for (const venue of SUPPORTED_VENUES) {{
        const option = document.createElement('option');
        option.value = venue;
        option.textContent = venue;
        if (venue === value) option.selected = true;
        selectEl.appendChild(option);
      }}
    }}

    function setStatus(message, kind = '') {{
      statusEl.className = `status ${{kind}}`;
      statusEl.textContent = message;
    }}

    function makeRow(symbol = '', threshold = '') {{
      const tr = document.createElement('tr');
      tr.innerHTML = `
        <td><input data-role="symbol" value="${{escapeHtml(String(symbol))}}" placeholder="BTCUSDT / BTC-USDT / BTC-USDT-SWAP" /></td>
        <td><input data-role="threshold" value="${{escapeHtml(String(threshold))}}" placeholder="123.45" /></td>
        <td><button type="button" data-role="remove" class="danger">Remove</button></td>
      `;
      tr.querySelector('[data-role="remove"]').addEventListener('click', () => {{
        tr.remove();
        updateBulkPreview();
      }});
      tr.querySelector('[data-role="symbol"]').addEventListener('input', updateBulkPreview);
      tr.querySelector('[data-role="threshold"]').addEventListener('input', updateBulkPreview);
      return tr;
    }}

    function escapeHtml(text) {{
      return text
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }}

    function readTableRows() {{
      return [...tableBodyEl.querySelectorAll('tr')].map((tr) => {{
        const symbol = tr.querySelector('[data-role="symbol"]').value.trim();
        const threshold = tr.querySelector('[data-role="threshold"]').value.trim();
        return {{ symbol, tlen_threshold: threshold }};
      }}).filter((row) => row.symbol || row.tlen_threshold);
    }}

    function renderRows(rows) {{
      tableBodyEl.innerHTML = '';
      if (!rows.length) {{
        tableBodyEl.appendChild(makeRow());
      }} else {{
        for (const row of rows) {{
          tableBodyEl.appendChild(makeRow(row.symbol || '', row.tlen_threshold ?? ''));
        }}
      }}
      updateBulkPreview();
    }}

    function updateBulkPreview() {{
      const payload = {{}};
      for (const row of readTableRows()) {{
        if (!row.symbol) continue;
        payload[row.symbol] = row.tlen_threshold === '' ? '' : Number(row.tlen_threshold);
      }}
      bulkJsonEl.value = JSON.stringify(payload, null, 2);
      rowCountEl.textContent = String(readTableRows().length);
    }}

    function parseBulkJson() {{
      const raw = bulkJsonEl.value.trim();
      if (!raw) return [];
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {{
        throw new Error('bulk JSON 必须是 object，格式如 {{"BTCUSDT": 123.45}}');
      }}
      return Object.entries(parsed).map(([symbol, tlen_threshold]) => ({{ symbol, tlen_threshold }}));
    }}

    async function apiFetch(path, options = undefined) {{
      const resp = await fetch(path, options);
      const text = await resp.text();
      let payload = null;
      try {{
        payload = text ? JSON.parse(text) : null;
      }} catch (_) {{
        payload = {{ raw: text }};
      }}
      if (!resp.ok) {{
        const msg = payload && payload.error ? payload.error : text || `HTTP ${{resp.status}}`;
        throw new Error(msg);
      }}
      return payload;
    }}

    function currentQuery() {{
      const params = new URLSearchParams();
      params.set('open_venue', openVenueEl.value);
      params.set('hedge_venue', hedgeVenueEl.value);
      if (symbolFilterEl.value.trim()) {{
        params.set('symbols', symbolFilterEl.value.trim());
      }}
      return params;
    }}

    async function loadRows() {{
      const params = currentQuery();
      setStatus('loading...', 'warn');
      const payload = await apiFetch(`/api/thresholds?${{params.toString()}}`);
      redisKeyEl.value = payload.redis_key || '';
      metaKeyEl.textContent = payload.meta_key || '-';
      updatedAtEl.textContent = payload.meta?.updated_at_iso || '-';
      rowCountEl.textContent = String(payload.count || 0);
      badFieldsEl.textContent = String(payload.bad_fields || 0);
      renderRows(payload.rows || []);
      setStatus(`loaded ${{payload.count || 0}} rows`, 'ok');
    }}

    async function saveReplace() {{
      const rows = readTableRows();
      if (!rows.length && !window.confirm('当前 table 为空，这会清空该 venue pair 的 tlen 配置。继续吗？')) {{
        return;
      }}
      const body = {{
        open_venue: openVenueEl.value,
        hedge_venue: hedgeVenueEl.value,
        allow_empty: rows.length === 0,
        rows,
      }};
      setStatus('saving...', 'warn');
      const payload = await apiFetch('/api/thresholds/replace', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body),
      }});
      redisKeyEl.value = payload.redis_key || '';
      metaKeyEl.textContent = payload.meta_key || '-';
      updatedAtEl.textContent = payload.meta?.updated_at_iso || '-';
      rowCountEl.textContent = String(payload.count || 0);
      badFieldsEl.textContent = String(payload.bad_fields || 0);
      renderRows(payload.rows || []);
      setStatus(`saved ${{payload.count || 0}} rows`, 'ok');
    }}

    document.getElementById('loadBtn').addEventListener('click', () => {{
      loadRows().catch((err) => setStatus(String(err), 'err'));
    }});
    document.getElementById('saveBtn').addEventListener('click', () => {{
      saveReplace().catch((err) => setStatus(String(err), 'err'));
    }});
    document.getElementById('addRowBtn').addEventListener('click', () => {{
      tableBodyEl.appendChild(makeRow());
      updateBulkPreview();
    }});
    document.getElementById('exportBtn').addEventListener('click', updateBulkPreview);
    document.getElementById('importBtn').addEventListener('click', () => {{
      try {{
        const rows = parseBulkJson();
        renderRows(rows);
        setStatus(`imported ${{rows.length}} rows into editor`, 'ok');
      }} catch (err) {{
        setStatus(String(err), 'err');
      }}
    }});

    fillVenueSelect(openVenueEl, DEFAULT_OPEN_VENUE);
    fillVenueSelect(hedgeVenueEl, DEFAULT_HEDGE_VENUE);
    renderRows([]);
    loadRows().catch((err) => setStatus(String(err), 'err'));
  </script>
</body>
</html>
"""


class TlenConfigRequestHandler(BaseHTTPRequestHandler):
    config: ServerConfig = None  # type: ignore
    store: TlenConfigStore = None  # type: ignore

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        try:
            if parsed.path in ("", "/"):
                self._write_html(page_html(self.config))
                return
            if parsed.path == "/healthz":
                self._write_json(
                    {
                        "status": "ok",
                        "service": SERVICE_NAME,
                        "port": self.config.port,
                        "redis": f"{self.config.redis_host}:{self.config.redis_port}/{self.config.redis_db}",
                    }
                )
                return
            if parsed.path == "/api/venues":
                self._write_json(
                    {
                        "service": SERVICE_NAME,
                        "venues": SUPPORTED_VENUES,
                        "default_open_venue": self.config.default_open_venue,
                        "default_hedge_venue": self.config.default_hedge_venue,
                    }
                )
                return
            if parsed.path == "/api/thresholds":
                open_venue, hedge_venue = self._venues_from_params(params)
                payload = self.store.fetch(
                    open_venue,
                    hedge_venue,
                    parse_symbol_filters(params),
                )
                self._write_json(payload)
                return
        except Exception as exc:  # noqa: BLE001
            self._write_json({"error": str(exc)}, status=400)
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlparse(self.path)
        try:
            payload = self._read_json_body()
            open_venue = normalize_venue(str(payload.get("open_venue", self.config.default_open_venue)))
            hedge_venue = normalize_venue(str(payload.get("hedge_venue", self.config.default_hedge_venue)))
            if open_venue is None:
                raise ValueError("unsupported open_venue")
            if hedge_venue is None:
                raise ValueError("unsupported hedge_venue")

            if parsed.path == "/api/thresholds/replace":
                rows = coerce_rows(payload, open_venue)
                allow_empty = bool(payload.get("allow_empty"))
                self._write_json(self.store.replace(open_venue, hedge_venue, rows, allow_empty))
                return

            if parsed.path == "/api/thresholds/upsert":
                rows = coerce_rows(payload, open_venue)
                self._write_json(self.store.upsert(open_venue, hedge_venue, rows))
                return

            if parsed.path == "/api/thresholds/delete":
                symbols = payload.get("symbols")
                if not isinstance(symbols, list):
                    raise ValueError("symbols must be an array")
                self._write_json(self.store.delete_symbols(open_venue, hedge_venue, symbols))
                return
        except Exception as exc:  # noqa: BLE001
            self._write_json({"error": str(exc)}, status=400)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length", "0") or "0")
        body = self.rfile.read(length) if length > 0 else b"{}"
        if not body:
            return {}
        payload = json.loads(body.decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("JSON body must be an object")
        return payload

    def _venues_from_params(self, params: Dict[str, List[str]]) -> Tuple[str, str]:
        open_venue = normalize_venue(
            (params.get("open_venue") or [self.config.default_open_venue])[0]
        )
        hedge_venue = normalize_venue(
            (params.get("hedge_venue") or [self.config.default_hedge_venue])[0]
        )
        if open_venue is None:
            raise ValueError("unsupported open_venue")
        if hedge_venue is None:
            raise ValueError("unsupported hedge_venue")
        return open_venue, hedge_venue

    def _write_html(self, html: str, status: int = 200) -> None:
        body = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_json(self, payload: object, status: int = 200) -> None:
        body = json_dumps(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def parse_args() -> ServerConfig:
    parser = argparse.ArgumentParser(description="TLEN config server")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("PORT", str(DEFAULT_PORT))),
    )
    parser.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    parser.add_argument(
        "--redis-port",
        type=int,
        default=int(os.environ.get("REDIS_PORT", "6379")),
    )
    parser.add_argument(
        "--redis-db",
        type=int,
        default=int(os.environ.get("REDIS_DB", "0")),
    )
    parser.add_argument("--redis-password", default=os.environ.get("REDIS_PASSWORD"))
    parser.add_argument(
        "--default-open-venue",
        default=os.environ.get("DEFAULT_OPEN_VENUE", DEFAULT_OPEN_VENUE),
    )
    parser.add_argument(
        "--default-hedge-venue",
        default=os.environ.get("DEFAULT_HEDGE_VENUE", DEFAULT_HEDGE_VENUE),
    )
    args = parser.parse_args()

    default_open_venue = normalize_venue(args.default_open_venue)
    default_hedge_venue = normalize_venue(args.default_hedge_venue)
    if default_open_venue is None:
        raise ValueError(f"unsupported default open venue: {args.default_open_venue}")
    if default_hedge_venue is None:
        raise ValueError(f"unsupported default hedge venue: {args.default_hedge_venue}")

    return ServerConfig(
        host=args.host,
        port=args.port,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        default_open_venue=default_open_venue,
        default_hedge_venue=default_hedge_venue,
    )


def main() -> int:
    config = parse_args()
    store = TlenConfigStore(config)

    TlenConfigRequestHandler.config = config
    TlenConfigRequestHandler.store = store
    httpd = ThreadingHTTPServer((config.host, config.port), TlenConfigRequestHandler)

    print(
        f"[INFO] {SERVICE_NAME} listening on http://{config.host}:{config.port} "
        f"(redis={config.redis_host}:{config.redis_port}/{config.redis_db}, "
        f"default_open={config.default_open_venue}, default_hedge={config.default_hedge_venue})"
    )

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[INFO] received KeyboardInterrupt, exiting")
    finally:
        httpd.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
