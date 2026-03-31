#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一阈值配置服务器（沿用 tlen_config_server 名称）。

用途：
  - 以 HTTP + 简单页面的方式管理共享阈值配置。
  - 当前支持：
      - tlen_threshold
      - amount_thresholds
  - 配置按 venue 维度存取。

Redis key 约定：
  - TLEN: <venue_prefix>:tlen_threshold
  - Amount thresholds: <venue>:amount-thresholds

示例：
  python scripts/tlen_config_server.py
  python scripts/tlen_config_server.py --port 6322 --redis-db 15
"""

from __future__ import annotations

import argparse
import json
import math
import os
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, List, Optional, Tuple

SERVICE_NAME = "tlen_config_server"
DEFAULT_PORT = 6322
DEFAULT_VENUE = "binance-futures"
DEFAULT_CONFIG_TYPE = "tlen"
SUPPORTED_CONFIG_TYPES = [
    "tlen",
    "amount_thresholds",
]
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


def normalize_symbol_for_venue(symbol: str, venue: str) -> str:
    venue = normalize_venue(venue)
    if venue is None:
        raise ValueError(f"unsupported venue: {venue}")

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


def normalize_config_type(value: object) -> Optional[str]:
    raw = str(value or "").strip().lower().replace("-", "_")
    if raw in ("tlen", "tlen_threshold", "tlen_thresholds"):
        return "tlen"
    if raw in ("amount", "amount_threshold", "amount_thresholds"):
        return "amount_thresholds"
    return None


def redis_key(venue: str, config_type: str) -> str:
    venue_norm = normalize_venue(venue)
    if venue_norm is None:
        raise ValueError(f"unsupported venue: {venue}")
    normalized_type = normalize_config_type(config_type)
    if normalized_type == "tlen":
        return f"{venue_prefix(venue_norm)}:tlen_threshold"
    if normalized_type == "amount_thresholds":
        return f"{venue_norm}:amount-thresholds"
    raise ValueError(f"unsupported config_type: {config_type}")


def decode_hash(data: Dict[object, object]) -> Dict[str, str]:
    decoded: Dict[str, str] = {}
    for raw_key, raw_value in data.items():
        key = raw_key.decode("utf-8", "ignore") if isinstance(raw_key, bytes) else str(raw_key)
        value = (
            raw_value.decode("utf-8", "ignore") if isinstance(raw_value, bytes) else str(raw_value)
        )
        decoded[key] = value
    return decoded


def parse_amount_threshold_value(raw_value: object) -> Dict[str, float]:
    if isinstance(raw_value, str):
        try:
            payload = json.loads(raw_value)
        except Exception as exc:
            raise ValueError(f"invalid amount threshold json: {raw_value!r}") from exc
    elif isinstance(raw_value, dict):
        payload = raw_value
    else:
        raise ValueError(f"invalid amount threshold payload: {raw_value!r}")

    if not isinstance(payload, dict):
        raise ValueError("amount threshold payload must be an object")

    medium = parse_threshold_value(payload.get("medium_notional_threshold"))
    large = parse_threshold_value(payload.get("large_notional_threshold"))
    if medium > large:
        raise ValueError(
            f"medium_notional_threshold must be <= large_notional_threshold: {medium} > {large}"
        )
    return {
        "medium_notional_threshold": medium,
        "large_notional_threshold": large,
    }


def parse_thresholds_map(data: Dict[str, str], venue: str, config_type: str) -> Dict[str, object]:
    thresholds: Dict[str, object] = {}
    for raw_symbol in sorted(data.keys()):
        symbol = normalize_symbol_for_venue(raw_symbol, venue)
        raw_value = data[raw_symbol]
        if config_type == "tlen":
            thresholds[symbol] = parse_threshold_value(raw_value)
            continue
        if config_type == "amount_thresholds":
            thresholds[symbol] = parse_amount_threshold_value(raw_value)
            continue
        raise ValueError(f"unsupported config_type: {config_type}")
    return thresholds


def coerce_thresholds(payload: dict, venue: str, config_type: str) -> Dict[str, str]:
    thresholds_raw = payload.get("thresholds", payload)
    if not isinstance(thresholds_raw, dict):
        raise ValueError("payload must be an object or contain object field 'thresholds'")

    encoded: Dict[str, str] = {}
    for raw_symbol, raw_value in thresholds_raw.items():
        symbol = normalize_symbol_for_venue(str(raw_symbol), venue)
        if config_type == "tlen":
            encoded[symbol] = f"{parse_threshold_value(raw_value):.8f}"
            continue
        if config_type == "amount_thresholds":
            value = parse_amount_threshold_value(raw_value)
            encoded[symbol] = json.dumps(value, ensure_ascii=False, sort_keys=True)
            continue
        raise ValueError(f"unsupported config_type: {config_type}")
    return encoded


@dataclass
class ServerConfig:
    host: str
    port: int
    redis_host: str
    redis_port: int
    redis_db: int
    redis_password: Optional[str]
    default_venue: str
    default_config_type: str


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
        venue: str,
        config_type: str,
    ) -> Dict[str, object]:
        key = redis_key(venue, config_type)
        thresholds = parse_thresholds_map(decode_hash(self._redis.hgetall(key)), venue, config_type)
        return {
            "redis_key": key,
            "venue": venue,
            "config_type": config_type,
            "count": len(thresholds),
            "thresholds": thresholds,
        }

    def replace(
        self,
        venue: str,
        config_type: str,
        thresholds: Dict[str, str],
    ) -> Dict[str, object]:
        key = redis_key(venue, config_type)
        self._redis.delete(key)
        self._redis.delete(f"{key}:meta")
        if thresholds:
            self._redis.hset(key, mapping=thresholds)
        return self.fetch(venue, config_type)


def page_html(config: ServerConfig) -> str:
    venue_options = json.dumps(SUPPORTED_VENUES, ensure_ascii=False)
    default_venue = json.dumps(config.default_venue, ensure_ascii=False)
    config_type_options = json.dumps(SUPPORTED_CONFIG_TYPES, ensure_ascii=False)
    default_config_type = json.dumps(config.default_config_type, ensure_ascii=False)
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
      grid-template-columns: repeat(3, minmax(0, 1fr));
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
    .meta {{
      display: flex;
      gap: 16px;
      flex-wrap: wrap;
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
      .toolbar {{
        grid-template-columns: 1fr;
      }}
      .wrap {{
        width: calc(100vw - 20px);
      }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <div class="card">
      <h1>{SERVICE_NAME}</h1>
      <div class="muted">共享阈值管理服务。当前支持 <code>tlen_threshold</code> 与 <code>amount_thresholds</code>。</div>
    </div>

    <div class="card">
      <div class="toolbar">
        <div class="field">
          <label for="configType">config type</label>
          <select id="configType"></select>
        </div>
        <div class="field">
          <label for="venue">venue</label>
          <select id="venue"></select>
        </div>
        <div class="field">
          <label>redis key</label>
          <input id="redisKey" readonly />
        </div>
      </div>
      <div class="actions">
        <button class="primary" id="loadBtn">Load</button>
        <button class="primary" id="saveBtn">Save</button>
      </div>
      <div id="status" class="status"></div>
    </div>

    <div class="card">
      <div class="muted">批量 JSON：
        <code>{{"BTCUSDT": 123.45}}</code>
        或
        <code>{{"BTCUSDT": {{"medium_notional_threshold": 10000, "large_notional_threshold": 50000}}}}</code>。
        OKEX 建议直接填目标 venue 格式，例如 <code>BTC-USDT</code> 或 <code>BTC-USDT-SWAP</code>。
      </div>
      <textarea id="bulkJson" spellcheck="false"></textarea>
    </div>

    <div class="card">
      <div class="meta">
        <div>count: <span id="rowCount">0</span></div>
      </div>
    </div>
  </div>

  <script>
    const SERVICE_NAME = {service_name};
    const SUPPORTED_VENUES = {venue_options};
    const DEFAULT_VENUE = {default_venue};
    const SUPPORTED_CONFIG_TYPES = {config_type_options};
    const DEFAULT_CONFIG_TYPE = {default_config_type};

    const configTypeEl = document.getElementById('configType');
    const venueEl = document.getElementById('venue');
    const redisKeyEl = document.getElementById('redisKey');
    const bulkJsonEl = document.getElementById('bulkJson');
    const statusEl = document.getElementById('status');
    const rowCountEl = document.getElementById('rowCount');

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

    function fillConfigTypeSelect(selectEl, value) {{
      selectEl.innerHTML = '';
      for (const configType of SUPPORTED_CONFIG_TYPES) {{
        const option = document.createElement('option');
        option.value = configType;
        option.textContent = configType;
        if (configType === value) option.selected = true;
        selectEl.appendChild(option);
      }}
    }}

    function setStatus(message, kind = '') {{
      statusEl.className = `status ${{kind}}`;
      statusEl.textContent = message;
    }}

    function currentConfigType() {{
      return configTypeEl.value || DEFAULT_CONFIG_TYPE;
    }}

    function parseBulkJson() {{
      const raw = bulkJsonEl.value.trim();
      if (!raw) return {{}};
      const parsed = JSON.parse(raw);
      if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {{
        throw new Error('bulk JSON 必须是 object，格式如 {{"BTCUSDT": 123.45}}');
      }}
      return parsed;
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
      params.set('config_type', currentConfigType());
      params.set('venue', venueEl.value);
      return params;
    }}

    async function loadConfig() {{
      const params = currentQuery();
      setStatus('loading...', 'warn');
      const payload = await apiFetch(`/api/thresholds?${{params.toString()}}`);
      redisKeyEl.value = payload.redis_key || '';
      rowCountEl.textContent = String(payload.count || 0);
      bulkJsonEl.value = JSON.stringify(payload.thresholds || {{}}, null, 2);
      setStatus(`loaded ${{payload.count || 0}} rows`, 'ok');
    }}

    async function saveReplace() {{
      const thresholds = parseBulkJson();
      const count = Object.keys(thresholds).length;
      if (!count && !window.confirm('当前 JSON 为空，这会清空该配置。继续吗？')) {{
        return;
      }}
      const body = {{
        venue: venueEl.value,
        config_type: currentConfigType(),
        thresholds,
      }};
      setStatus('saving...', 'warn');
      const payload = await apiFetch('/api/thresholds/replace', {{
        method: 'POST',
        headers: {{ 'Content-Type': 'application/json' }},
        body: JSON.stringify(body),
      }});
      redisKeyEl.value = payload.redis_key || '';
      rowCountEl.textContent = String(payload.count || 0);
      bulkJsonEl.value = JSON.stringify(payload.thresholds || {{}}, null, 2);
      setStatus(`saved ${{payload.count || 0}} rows`, 'ok');
    }}

    configTypeEl.addEventListener('change', () => {{
      loadConfig().catch((err) => setStatus(String(err), 'err'));
    }});
    document.getElementById('loadBtn').addEventListener('click', () => {{
      loadConfig().catch((err) => setStatus(String(err), 'err'));
    }});
    document.getElementById('saveBtn').addEventListener('click', () => {{
      saveReplace().catch((err) => setStatus(String(err), 'err'));
    }});

    fillConfigTypeSelect(configTypeEl, DEFAULT_CONFIG_TYPE);
    fillVenueSelect(venueEl, DEFAULT_VENUE);
    bulkJsonEl.value = '{{}}';
    loadConfig().catch((err) => setStatus(String(err), 'err'));
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
                        "config_types": SUPPORTED_CONFIG_TYPES,
                        "default_config_type": self.config.default_config_type,
                        "default_venue": self.config.default_venue,
                    }
                )
                return
            if parsed.path == "/api/thresholds":
                venue = self._venue_from_params(params)
                config_type = self._config_type_from_params(params)
                payload = self.store.fetch(venue, config_type)
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
            venue_raw = payload.get("venue", payload.get("open_venue", self.config.default_venue))
            venue = normalize_venue(str(venue_raw))
            if venue is None:
                raise ValueError("unsupported venue")
            config_type = normalize_config_type(
                payload.get("config_type", self.config.default_config_type)
            )
            if config_type is None:
                raise ValueError("unsupported config_type")

            if parsed.path == "/api/thresholds/replace":
                thresholds = coerce_thresholds(payload, venue, config_type)
                self._write_json(self.store.replace(venue, config_type, thresholds))
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

    def _venue_from_params(self, params: Dict[str, List[str]]) -> str:
        venue = normalize_venue(
            (params.get("venue") or params.get("open_venue") or [self.config.default_venue])[0]
        )
        if venue is None:
            raise ValueError("unsupported venue")
        return venue

    def _config_type_from_params(self, params: Dict[str, List[str]]) -> str:
        config_type = normalize_config_type(
            (params.get("config_type") or [self.config.default_config_type])[0]
        )
        if config_type is None:
            raise ValueError("unsupported config_type")
        return config_type

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
        "--default-venue",
        default=os.environ.get("DEFAULT_VENUE", os.environ.get("DEFAULT_OPEN_VENUE", DEFAULT_VENUE)),
    )
    parser.add_argument(
        "--default-open-venue",
        default=None,
    )
    parser.add_argument(
        "--default-config-type",
        default=os.environ.get("DEFAULT_CONFIG_TYPE", DEFAULT_CONFIG_TYPE),
    )
    args = parser.parse_args()

    default_venue = normalize_venue(args.default_open_venue or args.default_venue)
    if default_venue is None:
        raise ValueError(f"unsupported default venue: {args.default_open_venue or args.default_venue}")
    default_config_type = normalize_config_type(args.default_config_type)
    if default_config_type is None:
        raise ValueError(f"unsupported default config type: {args.default_config_type}")

    return ServerConfig(
        host=args.host,
        port=args.port,
        redis_host=args.redis_host,
        redis_port=args.redis_port,
        redis_db=args.redis_db,
        redis_password=args.redis_password,
        default_venue=default_venue,
        default_config_type=default_config_type,
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
        f"default_venue={config.default_venue}, default_config_type={config.default_config_type})"
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
