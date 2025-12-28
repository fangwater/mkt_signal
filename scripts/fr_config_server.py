#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funding Rate 配置服务器（fr_config_server）
-------------------------------------------
一个轻量级的 HTTP 服务，提供页面/接口用于查看和编辑 Funding Rate 相关的 symbol 列表，
直接读写 Redis 中的以下 3 个 key（JSON 数组，String 类型）：
  - fr_dump_symbols:{key_suffix}      平仓列表
  - fr_fwd_trade_symbols:{key_suffix} 正套建仓列表
  - fr_bwd_trade_symbols:{key_suffix} 反套建仓列表

其中 key_suffix 为 "<open_venue>_<hedge_venue>"（例如 gate-margin_gate-futures）。

启动示例：
  python scripts/fr_config_server.py --port 8000 --default-exchange okex
  REDIS_URL=redis://:pwd@127.0.0.1:6379/0 python scripts/fr_config_server.py
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]

EXCHANGE_DEFAULTS = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}

INDEX_HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Funding Rate Symbol 列表管理</title>
  <style>
    :root { color-scheme: light dark; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; margin: 0; padding: 0; background: #0b1221; color: #e4ecff; }
    header { padding: 16px 20px; background: #101a33; border-bottom: 1px solid #1f2a44; }
    h1 { margin: 0; font-size: 20px; }
    main { padding: 20px; }
    .panel { background: #121c36; border: 1px solid #223259; border-radius: 10px; padding: 16px; box-shadow: 0 10px 30px rgba(0,0,0,0.35); }
    .toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; margin-bottom: 16px; }
    select, button { padding: 8px 12px; border-radius: 6px; border: 1px solid #2f4066; background: #19274a; color: #e4ecff; }
    button { cursor: pointer; border-color: #3b82f6; background: linear-gradient(90deg, #2563eb, #38bdf8); color: #fff; font-weight: 600; }
    button.secondary { background: #19274a; color: #cbd5f5; border-color: #32476d; }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }
    .card { background: #0f172a; border: 1px solid #1f2a44; border-radius: 10px; padding: 12px; display: flex; flex-direction: column; gap: 8px; }
    .card h3 { margin: 0; font-size: 16px; display: flex; justify-content: space-between; align-items: center; }
    textarea { width: 100%; height: 220px; resize: vertical; border-radius: 8px; padding: 10px; border: 1px solid #283a5f; background: #0b1221; color: #e4ecff; font-family: Menlo, Consolas, monospace; line-height: 1.4; }
    .hint { color: #94a3b8; font-size: 12px; }
    .status { margin-top: 10px; font-size: 14px; }
    .status.ok { color: #34d399; }
    .status.err { color: #f87171; }
    footer { margin-top: 12px; font-size: 12px; color: #94a3b8; text-align: right; }
  </style>
</head>
<body>
  <header>
    <h1>Funding Rate Symbol 列表管理</h1>
  </header>
  <main>
    <div class="panel">
      <div class="toolbar">
        <label for="exchange">Exchange:</label>
        <select id="exchange"></select>
        <button id="btn-load" class="secondary">读取</button>
        <button id="btn-save">保存到 Redis</button>
        <span id="status" class="status"></span>
      </div>

      <div class="grid">
        <div class="card">
          <h3>平仓列表 <span class="hint">fr_dump_symbols</span></h3>
          <textarea id="dump-list" placeholder="每行一个 symbol，例如&#10;BTCUSDT&#10;ETHUSDT"></textarea>
          <div class="hint">未填写时会使用正套/反套并集作为平仓列表</div>
        </div>
        <div class="card">
          <h3>正套建仓列表 <span class="hint">fr_fwd_trade_symbols</span></h3>
          <textarea id="fwd-list" placeholder="每行一个 symbol，例如&#10;LTCUSDT"></textarea>
        </div>
        <div class="card">
          <h3>反套建仓列表 <span class="hint">fr_bwd_trade_symbols</span></h3>
          <textarea id="bwd-list" placeholder="每行一个 symbol，例如&#10;XMRUSDT"></textarea>
        </div>
      </div>

      <footer>默认 exchange: <span id="default-exchange"></span> · key_suffix: <span id="key-suffix">-</span> · 数据来源/落地：Redis</footer>
    </div>
  </main>
  <script>
    const EXCHANGES = __EXCHANGES__;
    const DEFAULT_EXCHANGE = "__DEFAULT_EXCHANGE__";

    const exchangeSelect = document.getElementById('exchange');
    const statusEl = document.getElementById('status');
    const dumpEl = document.getElementById('dump-list');
    const fwdEl = document.getElementById('fwd-list');
    const bwdEl = document.getElementById('bwd-list');
    const defaultExEl = document.getElementById('default-exchange');
    defaultExEl.textContent = DEFAULT_EXCHANGE || '-';

    function setStatus(msg, ok = true) {
      statusEl.textContent = msg;
      statusEl.className = 'status ' + (ok ? 'ok' : 'err');
    }

    function fillExchanges() {
      exchangeSelect.innerHTML = '';
      EXCHANGES.forEach(ex => {
        const opt = document.createElement('option');
        opt.value = ex;
        opt.textContent = ex;
        if (ex === DEFAULT_EXCHANGE) opt.selected = true;
        exchangeSelect.appendChild(opt);
      });
    }

    function toList(text) {
      return text
        .split(/[^A-Za-z0-9]+/)
        .map(s => s.trim().toUpperCase())
        .filter(Boolean);
    }

    function fromList(arr) {
      return (arr || []).join("\\n");
    }

    async function loadLists() {
      const ex = exchangeSelect.value;
      setStatus('读取中...', true);
      try {
        const resp = await fetch(`/api/symbol-lists?exchange=${encodeURIComponent(ex)}`);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        dumpEl.value = fromList(data.dump_symbols || []);
        fwdEl.value = fromList(data.fwd_trade_symbols || []);
        bwdEl.value = fromList(data.bwd_trade_symbols || []);
        const keySuffixEl = document.getElementById('key-suffix');
        if (keySuffixEl) keySuffixEl.textContent = data.key_suffix || '-';
        setStatus('读取完成', true);
      } catch (err) {
        console.error(err);
        setStatus(`读取失败: ${err}`, false);
      }
    }

    async function saveLists() {
      const ex = exchangeSelect.value;
      const payload = {
        exchange: ex,
        dump_symbols: toList(dumpEl.value),
        fwd_trade_symbols: toList(fwdEl.value),
        bwd_trade_symbols: toList(bwdEl.value),
      };
      setStatus('保存中...', true);
      try {
        const resp = await fetch('/api/symbol-lists', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        if (!resp.ok) {
          const txt = await resp.text();
          throw new Error(`HTTP ${resp.status} ${txt}`);
        }
        const data = await resp.json();
        setStatus(`保存成功：dump=${data.dump_count}, fwd=${data.fwd_count}, bwd=${data.bwd_count}`, true);
      } catch (err) {
        console.error(err);
        setStatus(`保存失败: ${err}`, false);
      }
    }

    document.getElementById('btn-load').addEventListener('click', loadLists);
    document.getElementById('btn-save').addEventListener('click', saveLists);
    fillExchanges();
    loadLists();
  </script>
</body>
</html>
"""


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Funding Rate 配置服务器 (fr_config_server)")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    parser.add_argument(
        "--default-exchange",
        default=os.environ.get("DEFAULT_EXCHANGE", "okex"),
        help="默认选中的交易所（也用于接口缺省值）",
    )
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    parser.add_argument("--redis-host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--redis-port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    parser.add_argument("--redis-db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    parser.add_argument("--redis-password", default=os.environ.get("REDIS_PASSWORD"))
    return parser.parse_args()


def normalize_symbol_list(value: Any) -> List[str]:
    """将字符串/数组标准化为大写且去重的列表"""
    items: List[str] = []
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, str):
        raw_items = re.split(r"[^\w]+", value)
    else:
        raw_items = []

    seen = set()
    for item in raw_items:
        if not isinstance(item, str):
            continue
        sym = item.strip().upper()
        if not sym:
            continue
        if sym not in seen:
            seen.add(sym)
            items.append(sym)
    return items


def read_symbol_list(rds, key: str) -> List[str]:
    raw = rds.get(key)
    if not raw:
        return []
    try:
        decoded = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        data = json.loads(decoded)
        if isinstance(data, list):
            return normalize_symbol_list(data)
    except Exception:
        pass
    return []


def make_key_suffix(open_venue: str, hedge_venue: str) -> str:
    return f"{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"


def resolve_key_suffix(exchange: str, key_suffix: Optional[str]) -> Optional[str]:
    if key_suffix:
        return str(key_suffix).strip().lower()
    venues = EXCHANGE_DEFAULTS.get(exchange)
    if not venues:
        return None
    return make_key_suffix(venues[0], venues[1])


def render_index_html(default_exchange: str) -> str:
    html = INDEX_HTML_TEMPLATE.replace("__EXCHANGES__", json.dumps(SUPPORTED_EXCHANGES))
    html = html.replace("__DEFAULT_EXCHANGE__", default_exchange)
    return html


@dataclass
class ServerContext:
    redis_client: Any
    default_exchange: str


class SymbolConfigServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, context: ServerContext):
        super().__init__(server_address, RequestHandlerClass)
        self.context = context


class RequestHandler(BaseHTTPRequestHandler):
    server: SymbolConfigServer  # type hint for self.server.context

    def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_html(self, body: str) -> None:
        data = body.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_error(self, status: int, message: str) -> None:
        self._send_json(status, {"error": message})

    def log_message(self, fmt: str, *args: Any) -> None:
        # 复用 stdout，避免写入 stderr。
        sys.stdout.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), fmt % args))

    def do_OPTIONS(self) -> None:
        # 简单处理预检请求
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            html = render_index_html(self.server.context.default_exchange)
            self._send_html(html)
            return

        if parsed.path == "/api/symbol-lists":
            params = parse_qs(parsed.query)
            exchange = (params.get("exchange") or [self.server.context.default_exchange])[0].lower()
            if exchange not in SUPPORTED_EXCHANGES:
                self._send_error(400, f"unsupported exchange: {exchange}")
                return
            key_suffix = resolve_key_suffix(exchange, (params.get("key_suffix") or [None])[0])
            if not key_suffix:
                self._send_error(400, f"invalid key_suffix for exchange: {exchange}")
                return
            rds = self.server.context.redis_client
            data = {
                "exchange": exchange,
                "key_suffix": key_suffix,
                "dump_symbols": read_symbol_list(rds, f"fr_dump_symbols:{key_suffix}"),
                "fwd_trade_symbols": read_symbol_list(rds, f"fr_fwd_trade_symbols:{key_suffix}"),
                "bwd_trade_symbols": read_symbol_list(rds, f"fr_bwd_trade_symbols:{key_suffix}"),
            }
            self._send_json(200, data)
            return

        self._send_error(404, "not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path != "/api/symbol-lists":
            self._send_error(404, "not found")
            return

        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        payload_raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(payload_raw.decode("utf-8") or "{}")
        except Exception:
            self._send_error(400, "invalid json")
            return

        exchange = str(payload.get("exchange") or self.server.context.default_exchange).lower()
        if exchange not in SUPPORTED_EXCHANGES:
            self._send_error(400, f"unsupported exchange: {exchange}")
            return
        key_suffix = resolve_key_suffix(exchange, payload.get("key_suffix"))
        if not key_suffix:
            self._send_error(400, f"invalid key_suffix for exchange: {exchange}")
            return

        dump_symbols = normalize_symbol_list(payload.get("dump_symbols") or [])
        fwd_symbols = normalize_symbol_list(payload.get("fwd_trade_symbols") or [])
        bwd_symbols = normalize_symbol_list(payload.get("bwd_trade_symbols") or [])

        # 若未提供平仓/建仓列表，则默认使用正套+反套的并集
        if not dump_symbols:
            dump_symbols = list(dict.fromkeys(fwd_symbols + bwd_symbols))

        rds = self.server.context.redis_client
        try:
            rds.set(f"fr_dump_symbols:{key_suffix}", json.dumps(dump_symbols, ensure_ascii=False))
            rds.set(f"fr_fwd_trade_symbols:{key_suffix}", json.dumps(fwd_symbols, ensure_ascii=False))
            rds.set(f"fr_bwd_trade_symbols:{key_suffix}", json.dumps(bwd_symbols, ensure_ascii=False))
        except Exception as exc:
            self._send_error(500, f"redis write failed: {exc}")
            return

        self._send_json(
            200,
            {
                "exchange": exchange,
                "key_suffix": key_suffix,
                "dump_count": len(dump_symbols),
                "fwd_count": len(fwd_symbols),
                "bwd_count": len(bwd_symbols),
            },
        )


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请执行: pip install redis", file=sys.stderr)
        return 2

    default_exchange = args.default_exchange.lower()
    if default_exchange not in SUPPORTED_EXCHANGES:
        print(f"❌ 默认交易所 {default_exchange} 不在支持列表 {SUPPORTED_EXCHANGES}", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.redis_host,
        port=args.redis_port,
        db=args.redis_db,
        password=args.redis_password,
    )

    context = ServerContext(redis_client=rds, default_exchange=default_exchange)
    server = SymbolConfigServer((args.host, args.port), RequestHandler, context)
    print(
        f"🚀 fr_config_server started on http://{args.host}:{args.port} "
        f"(default_exchange={default_exchange})"
    )
    print("按 Ctrl+C 退出")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n收到中断信号，正在退出...")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
