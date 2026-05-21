#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
轮询 Okx 公共借贷利率接口、缓存最近 N 期结果，并通过 HTTP 输出缓存。

用法示例:
    python scripts/okx_rate_cache_server.py --port 8901
    curl http://127.0.0.1:8901/rates
"""

from __future__ import annotations

import argparse
import json
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Deque, Dict, List, Optional

from json.encoder import INFINITY, encode_basestring, encode_basestring_ascii, _make_iterencode


class PlainFloatJSONEncoder(json.JSONEncoder):
    """JSON encoder that keeps float output in plain decimal form."""

    def __init__(self, *args, float_precision: int = 12, **kwargs) -> None:
        self.float_precision = float_precision
        super().__init__(*args, **kwargs)

    def iterencode(self, o, _one_shot=False):  # noqa: D401 (doc inherited)
        if self.check_circular:
            markers = {}
        else:
            markers = None
        if self.ensure_ascii:
            _encoder = encode_basestring_ascii
        else:
            _encoder = encode_basestring

        def floatstr(value, allow_nan=self.allow_nan):
            if value != value:
                text = "NaN"
            elif value == INFINITY:
                text = "Infinity"
            elif value == -INFINITY:
                text = "-Infinity"
            else:
                text = format(value, f".{self.float_precision}f")
                text = text.rstrip("0").rstrip(".")
                if text in {"", "-0"}:
                    text = "0"
                return text

            if not allow_nan:
                raise ValueError(f"Out of range float values are not JSON compliant: {value!r}")

            return text

        _iterencode = _make_iterencode(
            markers,
            self.default,
            _encoder,
            self.indent,
            floatstr,
            self.key_separator,
            self.item_separator,
            self.sort_keys,
            self.skipkeys,
            _one_shot,
        )
        return _iterencode(o, 0)

API = "https://openapi.okx.com/api/v5/public/interest-rate-loan-quota"


def fetch_data(ccy: Optional[str]) -> dict:
    params = {}
    if ccy:
        params["ccy"] = ccy
    url = API
    if params:
        url = f"{API}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, method="GET")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", "replace"))


def extract_rates(payload: dict) -> Dict[str, float]:
    data = payload.get("data") or []
    if not data:
        return {}
    rows: List[dict] = data[0].get("basic", [])
    return {row["ccy"].upper(): float(row["rate"]) for row in rows if "ccy" in row and "rate" in row}


class RateCache:
    """线程安全的缓存结构，保存最近 N 次抓取结果。"""

    def __init__(self, max_entries: int):
        self._entries: Deque[dict] = deque(maxlen=max_entries)
        self._lock = threading.Lock()

    def append(self, entry: dict) -> None:
        with self._lock:
            self._entries.append(entry)

    def dump(self) -> List[dict]:
        with self._lock:
            return list(self._entries)


class RateRequestHandler(BaseHTTPRequestHandler):
    """简单的 HTTP 处理器，只暴露 /rates JSON。"""

    cache: RateCache = None  # type: ignore

    def do_GET(self) -> None:  # noqa: N802 (http server naming)
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path.rstrip("/") in ("", "/"):
            self._write_json({"status": "ok", "path": "/rates"})
            return
        if parsed.path.startswith("/rates"):
            entries = self.cache.dump()
            params = urllib.parse.parse_qs(parsed.query)
            symbol_param = params.get("symbol") or params.get("ccy")
            if symbol_param:
                sym = symbol_param[0].upper()
                filtered = [
                    {"tp": entry["tp"], "rate": entry["rates"].get(sym)}
                    for entry in entries
                    if sym in entry["rates"]
                ]
                self._write_json({"count": len(filtered), "symbol": sym, "entries": filtered})
            else:
                self._write_json({"count": len(entries), "entries": entries})
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        # 安静一点，避免刷屏
        pass

    def _write_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False, cls=PlainFloatJSONEncoder).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def start_http_server(host: str, port: int, cache: RateCache) -> ThreadingHTTPServer:
    RateRequestHandler.cache = cache
    httpd = ThreadingHTTPServer((host, port), RateRequestHandler)

    def serve() -> None:
        httpd.serve_forever()

    thread = threading.Thread(target=serve, daemon=True, name="http-server")
    thread.start()
    print(f"🌐 HTTP server started on http://{host}:{port}")
    return httpd


def fetch_loop(interval_secs: int, cache: RateCache, ccy: Optional[str]) -> None:
    while True:
        started = time.time()
        try:
            payload = fetch_data(ccy)
            if payload.get("code") != "0":
                raise RuntimeError(payload.get("msg") or "non-zero code")
            rates = extract_rates(payload)
            if rates:
                entry = {
                    "tp": int(time.time()),
                    "rates": rates,
                }
                cache.append(entry)
                print(f"✅ fetched {len(rates)} rates @ {entry['tp']}")
            else:
                print("⚠️ empty rate list from Okx")
        except Exception as exc:  # noqa: BLE001
            print(f"❌ fetch failed: {exc}")
        elapsed = time.time() - started
        sleep_secs = max(interval_secs - elapsed, 0)
        time.sleep(sleep_secs)


def main() -> int:
    parser = argparse.ArgumentParser(description="Okx loan rate cache service")
    parser.add_argument("--interval-secs", type=int, default=3600, help="Fetch interval in seconds")
    parser.add_argument("--max-entries", type=int, default=24, help="Max cached points")
    parser.add_argument("--ccy", help="Optional currency filter (defaults to Okx server default)")
    parser.add_argument("--host", default="127.0.0.1", help="HTTP bind host")
    parser.add_argument("--port", type=int, default=8000, help="HTTP bind port")
    args = parser.parse_args()

    cache = RateCache(args.max_entries)
    start_http_server(args.host, args.port, cache)

    # 立即抓一次，避免冷启动为空
    threading.Thread(target=fetch_loop, args=(args.interval_secs, cache, args.ccy), daemon=True, name="fetch-loop").start()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        print("Bye")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
