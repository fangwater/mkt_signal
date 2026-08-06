#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BatchExec Redis configuration server."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

STRATEGY_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SYMBOL_RE = re.compile(r"^[A-Z0-9]+$")
CONFIG_FIELDS = {
    "single_order_usdt",
    "orders_per_batch",
    "maker_price_anchor",
    "tick_spacing",
    "batch_interval_ms",
    "maker_timeout_ms",
    "max_maker_requotes",
    "target_tolerance_usdt",
    "targets",
}
DEFAULT_CONFIG: Dict[str, Any] = {
    "single_order_usdt": 100.0,
    "orders_per_batch": 3,
    "maker_price_anchor": "own_best",
    "tick_spacing": 1,
    "batch_interval_ms": 500,
    "maker_timeout_ms": 1000,
    "max_maker_requotes": 2,
    "target_tolerance_usdt": 10.0,
    "targets": {},
}
CLIENT_SCRIPT_PATH = Path(__file__).resolve().with_name("exec_config_client.py")
CLIENT_SCRIPT_ROUTE = "/exec_config_client.py"


def validate_strategy_name(raw: Any) -> str:
    name = str(raw or "").strip()
    if not STRATEGY_NAME_RE.fullmatch(name):
        raise ValueError("strategy_name must match [A-Za-z0-9][A-Za-z0-9._-]*")
    if name == "strategy_names":
        raise ValueError("strategy_name is reserved: strategy_names")
    return name


def normalize_symbol(raw: Any) -> str:
    symbol = str(raw or "").strip().upper()
    if not SYMBOL_RE.fullmatch(symbol):
        raise ValueError(f"invalid symbol: {raw}")
    return symbol


def finite_float(raw: Any, field: str, *, positive: bool = False) -> float:
    try:
        value = float(raw)
    except Exception as exc:
        raise ValueError(f"{field} must be a number") from exc
    if not math.isfinite(value):
        raise ValueError(f"{field} must be finite")
    if positive and value <= 0:
        raise ValueError(f"{field} must be > 0")
    return value


def integer(raw: Any, field: str, *, positive: bool = False) -> int:
    if isinstance(raw, bool):
        raise ValueError(f"{field} must be an integer")
    try:
        value = int(raw)
    except Exception as exc:
        raise ValueError(f"{field} must be an integer") from exc
    if str(raw).strip() not in {str(value), f"{value}.0"} and not isinstance(raw, int):
        raise ValueError(f"{field} must be an integer")
    if value < 0 or (positive and value == 0):
        op = "> 0" if positive else ">= 0"
        raise ValueError(f"{field} must be {op}")
    if value > 4_294_967_295:
        raise ValueError(f"{field} exceeds uint32")
    return value


def normalize_exec_config(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("config must be an object")
    unknown = sorted(set(raw) - CONFIG_FIELDS)
    missing = sorted(CONFIG_FIELDS - set(raw))
    if unknown:
        raise ValueError(f"unknown fields: {', '.join(unknown)}")
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")

    anchor = str(raw["maker_price_anchor"]).strip()
    if anchor not in {"own_best", "opposite_best_plus_one_tick"}:
        raise ValueError("invalid maker_price_anchor")

    targets_raw = raw["targets"]
    if not isinstance(targets_raw, dict):
        raise ValueError("targets must be an object")
    targets: Dict[str, float] = {}
    for raw_symbol, raw_qty in targets_raw.items():
        symbol = normalize_symbol(raw_symbol)
        if symbol in targets:
            raise ValueError(f"duplicate symbol: {symbol}")
        targets[symbol] = finite_float(raw_qty, f"targets.{symbol}")

    tolerance = finite_float(raw["target_tolerance_usdt"], "target_tolerance_usdt")
    if tolerance < 0:
        raise ValueError("target_tolerance_usdt must be >= 0")

    return {
        "single_order_usdt": finite_float(
            raw["single_order_usdt"], "single_order_usdt", positive=True
        ),
        "orders_per_batch": integer(
            raw["orders_per_batch"], "orders_per_batch", positive=True
        ),
        "maker_price_anchor": anchor,
        "tick_spacing": integer(raw["tick_spacing"], "tick_spacing"),
        "batch_interval_ms": integer(raw["batch_interval_ms"], "batch_interval_ms"),
        "maker_timeout_ms": integer(
            raw["maker_timeout_ms"], "maker_timeout_ms", positive=True
        ),
        "max_maker_requotes": integer(
            raw["max_maker_requotes"], "max_maker_requotes"
        ),
        "target_tolerance_usdt": tolerance,
        "targets": dict(sorted(targets.items())),
    }


class ExecConfigStore:
    def __init__(
        self,
        redis_url: str,
        env_name: str,
        venue: str,
    ) -> None:
        try:
            import redis  # type: ignore
        except ImportError as exc:
            raise RuntimeError("redis package is required: pip install redis") from exc
        self.client = redis.Redis.from_url(redis_url, decode_responses=True)
        self.env_name = str(env_name).strip()
        self.venue = str(venue).strip()
        if not self.env_name or not self.venue:
            raise ValueError("env_name and venue are required")
        if self.venue not in ("binance-futures", "okex-futures"):
            raise ValueError("venue must be binance-futures or okex-futures")
        self.prefix = f"{self.env_name}:{self.venue}:batch_exec:"
        self.index_key = f"{self.prefix}strategy_names"
        self._save_lock = threading.Lock()

    def key(self, strategy_name: str) -> str:
        return f"{self.prefix}{validate_strategy_name(strategy_name)}"

    def list_strategy_names(self) -> List[str]:
        raw = self.client.get(self.index_key)
        if raw is None:
            return []
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"strategy index is not valid JSON: {exc}") from exc
        if not isinstance(decoded, list):
            raise ValueError("strategy index must be a JSON array")
        names = [validate_strategy_name(name) for name in decoded]
        if len(names) != len(set(names)):
            raise ValueError("strategy index contains duplicate names")
        return sorted(names)

    def load(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        raw = self.client.get(self.key(strategy_name))
        if raw is None:
            return None
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Redis value is not valid JSON: {exc}") from exc
        return normalize_exec_config(decoded)

    def save(self, strategy_name: str, config: Any) -> Dict[str, Any]:
        name = validate_strategy_name(strategy_name)
        normalized = normalize_exec_config(config)
        with self._save_lock:
            strategy_names = self.list_strategy_names()
            self.client.set(
                self.key(name),
                json.dumps(normalized, ensure_ascii=False, separators=(",", ":")),
            )
            if name not in strategy_names:
                strategy_names.append(name)
                self.client.set(
                    self.index_key,
                    json.dumps(sorted(strategy_names), ensure_ascii=False, separators=(",", ":")),
                )
        return normalized


INDEX_HTML = r"""<!doctype html>
<html lang="zh-CN">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Exec Config</title>
    <style>
      :root {
        color-scheme: dark;
        font-family: "Segoe UI", "PingFang SC", sans-serif;
        --bg: #0b0f14; --surface: #121820; --surface2: #18212b; --line: #2c3744;
        --text: #edf2f7; --muted: #91a0b2; --cyan: #38bdf8; --green: #4ade80;
        --red: #fb7185; --amber: #fbbf24;
      }
      * { box-sizing: border-box; }
      body { margin: 0; min-height: 100vh; background: var(--bg); color: var(--text); letter-spacing: 0; }
      header {
        padding: 12px 18px; min-height: 62px; display: flex; align-items: center; gap: 12px;
        flex-wrap: wrap; border-bottom: 1px solid var(--line); background: #0e141b;
        position: sticky; top: 0; z-index: 5;
      }
      h1 { margin: 0 12px 0 0; font-size: 18px; }
      select, input, button, a.command {
        height: 34px; border: 1px solid var(--line); border-radius: 6px; background: var(--surface);
        color: var(--text); padding: 0 10px; font: inherit; font-size: 13px;
      }
      select { min-width: 220px; }
      input { width: 100%; }
      button, a.command { cursor: pointer; }
      button.primary { background: #087ea4; border-color: #0ea5c9; }
      button.danger { color: var(--red); }
      button:disabled { opacity: .5; cursor: wait; }
      a.command { display: inline-flex; align-items: center; text-decoration: none; }
      .new-name { width: 180px; }
      .key { color: var(--muted); font: 12px ui-monospace, monospace; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
      .status { margin-left: auto; font-size: 12px; color: var(--muted); }
      .status.ok { color: var(--green); } .status.err { color: var(--red); } .status.warn { color: var(--amber); }
      main { max-width: 1160px; margin: 0 auto; padding: 18px; display: grid; gap: 22px; }
      section { border-top: 1px solid var(--line); padding-top: 14px; }
      .section-head { display: flex; align-items: center; gap: 10px; margin-bottom: 12px; }
      .section-head h2 { margin: 0; font-size: 15px; }
      .section-head .actions { margin-left: auto; display: flex; gap: 8px; }
      .param-grid { display: grid; grid-template-columns: repeat(4, minmax(170px, 1fr)); gap: 12px; }
      .field { min-width: 0; }
      .field label { display: block; margin-bottom: 6px; color: var(--muted); font-size: 11px; }
      .field select { width: 100%; min-width: 0; }
      .targets { border: 1px solid var(--line); border-radius: 8px; overflow: auto; background: var(--surface); }
      table { width: 100%; border-collapse: collapse; }
      th, td { height: 42px; border-bottom: 1px solid var(--line); padding: 6px 10px; text-align: left; }
      th { color: var(--muted); font-size: 11px; background: var(--surface2); }
      td:last-child, th:last-child { width: 54px; text-align: center; }
      tbody tr:last-child td { border-bottom: 0; }
      td input { background: var(--bg); }
      .empty { padding: 36px; text-align: center; color: var(--muted); }
      .footer-actions { display: flex; justify-content: flex-end; gap: 8px; padding-top: 4px; }
      @media (max-width: 860px) { .param-grid { grid-template-columns: repeat(2, 1fr); } .status { width: 100%; margin-left: 0; } }
      @media (max-width: 520px) { header, main { padding-left: 10px; padding-right: 10px; } .param-grid { grid-template-columns: 1fr; } select { min-width: 150px; flex: 1; } }
    </style>
  </head>
  <body>
    <header>
      <h1>Exec Config</h1>
      <select id="strategy"><option value="">Select strategy</option></select>
      <input id="new-name" class="new-name" placeholder="New strategy name" />
      <button id="add-strategy" type="button">Add</button>
      <button id="reload" type="button">Reload</button>
      <a id="dashboard" class="command" href="../">Dashboard</a>
      <span id="status" class="status">Loading</span>
    </header>
    <main>
      <div id="redis-key" class="key">-</div>
      <section>
        <div class="section-head"><h2>Order Parameters</h2></div>
        <div class="param-grid">
          <div class="field"><label>Single Order USDT</label><input id="single_order_usdt" inputmode="decimal" /></div>
          <div class="field"><label>Orders Per Batch</label><input id="orders_per_batch" inputmode="numeric" /></div>
          <div class="field"><label>Maker Price Anchor</label><select id="maker_price_anchor"><option value="own_best">Own Best</option><option value="opposite_best_plus_one_tick">Opposite Best + 1 Tick</option></select></div>
          <div class="field"><label>Tick Spacing</label><input id="tick_spacing" inputmode="numeric" /></div>
          <div class="field"><label>Batch Interval ms</label><input id="batch_interval_ms" inputmode="numeric" /></div>
          <div class="field"><label>Maker Timeout ms</label><input id="maker_timeout_ms" inputmode="numeric" /></div>
          <div class="field"><label>Max Maker Requotes</label><input id="max_maker_requotes" inputmode="numeric" /></div>
          <div class="field"><label>Target Tolerance USDT</label><input id="target_tolerance_usdt" inputmode="decimal" /></div>
        </div>
      </section>
      <section>
        <div class="section-head">
          <h2>Target Positions</h2>
          <div class="actions"><button id="add-target" type="button">Add Symbol</button></div>
        </div>
        <div class="targets">
          <table><thead><tr><th>Symbol</th><th>Target Qty</th><th></th></tr></thead><tbody id="target-rows"></tbody></table>
          <div id="target-empty" class="empty">No target positions</div>
        </div>
      </section>
      <div class="footer-actions">
        <button id="discard" type="button">Discard</button>
        <button id="save" class="primary" type="button">Save Strategy</button>
      </div>
    </main>
    <script>
      (() => {
        const DEFAULTS = __DEFAULTS__;
        const fields = ["single_order_usdt", "orders_per_batch", "maker_price_anchor", "tick_spacing", "batch_interval_ms", "maker_timeout_ms", "max_maker_requotes", "target_tolerance_usdt"];
        const state = { bootstrap: null, names: [], name: "", config: null, dirty: false };
        const el = (id) => document.getElementById(id);
        function api(path) { return new URL(`api/${path}`, location.href).toString(); }
        function setStatus(text, level = "") { el("status").textContent = text; el("status").className = `status ${level}`.trim(); }
        async function request(path, options = {}) {
          const response = await fetch(api(path), { headers: {"Content-Type": "application/json"}, ...options });
          const body = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
          return body;
        }
        function markDirty() { state.dirty = true; setStatus("Unsaved changes", "warn"); }
        function strategyFromQuery() { return new URLSearchParams(location.search).get("strategy") || ""; }
        function updateQuery() { const url = new URL(location.href); state.name ? url.searchParams.set("strategy", state.name) : url.searchParams.delete("strategy"); history.replaceState(null, "", url); }
        function renderNames() {
          const select = el("strategy");
          select.innerHTML = '<option value="">Select strategy</option>' + state.names.map((name) => `<option value="${name}">${name}</option>`).join("");
          select.value = state.name;
        }
        function renderTargets(targets) {
          const tbody = el("target-rows");
          tbody.innerHTML = "";
          Object.entries(targets || {}).sort(([a], [b]) => a.localeCompare(b)).forEach(([symbol, qty]) => addTargetRow(symbol, qty, false));
          el("target-empty").style.display = tbody.children.length ? "none" : "block";
        }
        function addTargetRow(symbol = "", qty = "0", dirty = true) {
          const tr = document.createElement("tr");
          tr.innerHTML = `<td><input class="target-symbol" value="${String(symbol).replace(/"/g, "&quot;")}" placeholder="BTCUSDT" /></td><td><input class="target-qty" value="${qty}" inputmode="decimal" /></td><td><button class="danger remove-target" type="button" aria-label="Remove">X</button></td>`;
          tr.querySelectorAll("input").forEach((input) => input.addEventListener("input", markDirty));
          tr.querySelector(".remove-target").onclick = () => { tr.remove(); el("target-empty").style.display = el("target-rows").children.length ? "none" : "block"; markDirty(); };
          el("target-rows").appendChild(tr);
          el("target-empty").style.display = "none";
          if (dirty) markDirty();
        }
        function renderConfig(config) {
          state.config = structuredClone(config);
          fields.forEach((name) => { el(name).value = config[name]; });
          renderTargets(config.targets);
          el("redis-key").textContent = state.bootstrap ? `${state.bootstrap.key_prefix}${state.name}` : "-";
          state.dirty = false;
        }
        function collectConfig() {
          const targets = {};
          el("target-rows").querySelectorAll("tr").forEach((row) => {
            const symbol = row.querySelector(".target-symbol").value.trim().toUpperCase();
            if (!symbol) throw new Error("Target symbol is required");
            if (Object.prototype.hasOwnProperty.call(targets, symbol)) throw new Error(`Duplicate symbol: ${symbol}`);
            targets[symbol] = Number(row.querySelector(".target-qty").value);
          });
          return {
            single_order_usdt: Number(el("single_order_usdt").value),
            orders_per_batch: Number(el("orders_per_batch").value),
            maker_price_anchor: el("maker_price_anchor").value,
            tick_spacing: Number(el("tick_spacing").value),
            batch_interval_ms: Number(el("batch_interval_ms").value),
            maker_timeout_ms: Number(el("maker_timeout_ms").value),
            max_maker_requotes: Number(el("max_maker_requotes").value),
            target_tolerance_usdt: Number(el("target_tolerance_usdt").value),
            targets,
          };
        }
        async function loadNames(preferred = "") {
          const payload = await request("strategies");
          state.names = payload.strategies || [];
          if (preferred && state.names.includes(preferred)) state.name = preferred;
          else if (!state.names.includes(state.name)) state.name = state.names[0] || "";
          renderNames();
          if (state.name) await loadStrategy(state.name); else { renderConfig(DEFAULTS); setStatus("Add a strategy", "warn"); }
        }
        async function loadStrategy(name) {
          if (!name) return;
          setStatus("Loading");
          try {
            const payload = await request(`strategy?name=${encodeURIComponent(name)}`);
            state.name = name;
            renderConfig(payload.config || DEFAULTS);
            renderNames(); updateQuery(); setStatus(payload.exists ? "Loaded" : "New strategy", payload.exists ? "ok" : "warn");
          } catch (error) { setStatus(error.message, "err"); }
        }
        async function save() {
          if (!state.name) { setStatus("Select a strategy", "err"); return; }
          el("save").disabled = true;
          try {
            const payload = await request("strategy", { method: "POST", body: JSON.stringify({ strategy_name: state.name, config: collectConfig() }) });
            renderConfig(payload.config); if (!state.names.includes(state.name)) state.names.push(state.name); state.names.sort(); renderNames();
            setStatus("Saved", "ok");
          } catch (error) { setStatus(error.message, "err"); }
          finally { el("save").disabled = false; }
        }
        async function boot() {
          try {
            state.bootstrap = await request("bootstrap");
            el("dashboard").href = new URL(state.bootstrap.dashboard_url || "../", location.href);
            await loadNames(strategyFromQuery());
          } catch (error) { setStatus(error.message, "err"); }
        }
        fields.forEach((name) => el(name).addEventListener("input", markDirty));
        el("strategy").onchange = (event) => { if (state.dirty && !confirm("Discard unsaved changes?")) { event.target.value = state.name; return; } loadStrategy(event.target.value); };
        el("add-strategy").onclick = () => {
          const name = el("new-name").value.trim();
          if (!name) return setStatus("Enter strategy name", "err");
          if (!/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(name)) return setStatus("Invalid strategy name", "err");
          state.name = name; if (!state.names.includes(name)) state.names.push(name); state.names.sort(); renderNames(); renderConfig(DEFAULTS); updateQuery(); el("new-name").value = ""; markDirty();
        };
        el("reload").onclick = () => loadNames(state.name);
        el("add-target").onclick = () => addTargetRow();
        el("discard").onclick = () => state.name ? loadStrategy(state.name) : renderConfig(DEFAULTS);
        el("save").onclick = save;
        window.addEventListener("beforeunload", (event) => { if (state.dirty) { event.preventDefault(); event.returnValue = ""; } });
        boot();
      })();
    </script>
  </body>
</html>"""


def make_handler(store: ExecConfigStore, dashboard_url: str):
    html = INDEX_HTML.replace(
        "__DEFAULTS__", json.dumps(DEFAULT_CONFIG, ensure_ascii=False)
    ).encode("utf-8")

    class Handler(BaseHTTPRequestHandler):
        server_version = "ExecConfigServer/1.0"

        def send_json(self, status: int, payload: Dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def send_error_json(self, status: int, exc: Exception) -> None:
            self.send_json(status, {"ok": False, "error": str(exc)})

        def read_json(self) -> Dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except ValueError as exc:
                raise ValueError("invalid Content-Length") from exc
            if length <= 0 or length > 1_000_000:
                raise ValueError("invalid request body length")
            decoded = json.loads(self.rfile.read(length))
            if not isinstance(decoded, dict):
                raise ValueError("request body must be an object")
            return decoded

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            try:
                if parsed.path == CLIENT_SCRIPT_ROUTE:
                    try:
                        script = CLIENT_SCRIPT_PATH.read_bytes()
                    except OSError as exc:
                        self.send_error_json(
                            404, ValueError(f"client script is unavailable: {exc}")
                        )
                        return
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header(
                        "Content-Disposition",
                        'attachment; filename="exec_config_client.py"',
                    )
                    self.send_header("Content-Length", str(len(script)))
                    self.send_header("Cache-Control", "no-store")
                    self.send_header("X-Content-Type-Options", "nosniff")
                    self.end_headers()
                    self.wfile.write(script)
                    return
                if parsed.path in {"/", "/index.html"}:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html; charset=utf-8")
                    self.send_header("Content-Length", str(len(html)))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(html)
                    return
                if parsed.path == "/healthz":
                    store.client.ping()
                    self.send_json(200, {"ok": True})
                    return
                if parsed.path == "/api/bootstrap":
                    self.send_json(
                        200,
                        {
                            "ok": True,
                            "env_name": store.env_name,
                            "venue": store.venue,
                            "key_prefix": store.prefix,
                            "dashboard_url": dashboard_url,
                            "defaults": DEFAULT_CONFIG,
                        },
                    )
                    return
                if parsed.path == "/api/strategies":
                    self.send_json(200, {"ok": True, "strategies": store.list_strategy_names()})
                    return
                if parsed.path == "/api/strategy":
                    query = parse_qs(parsed.query)
                    name = validate_strategy_name((query.get("name") or [""])[0])
                    config = store.load(name)
                    self.send_json(
                        200,
                        {
                            "ok": True,
                            "strategy_name": name,
                            "key": store.key(name),
                            "exists": config is not None,
                            "config": config or DEFAULT_CONFIG,
                        },
                    )
                    return
                self.send_error_json(404, ValueError("not found"))
            except ValueError as exc:
                self.send_error_json(400, exc)
            except Exception as exc:
                self.send_error_json(500, exc)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            try:
                if parsed.path != "/api/strategy":
                    response = {"ok": False, "error": "not found"}
                    self.log_update_response(404, response)
                    self.send_json(404, response)
                    return
                payload = self.read_json()
                name = validate_strategy_name(payload.get("strategy_name"))
                config = store.save(name, payload.get("config"))
                response = {
                    "ok": True,
                    "strategy_name": name,
                    "key": store.key(name),
                    "config": config,
                }
                self.log_update_response(200, response)
                self.send_json(200, response)
            except (ValueError, json.JSONDecodeError) as exc:
                response = {"ok": False, "error": str(exc)}
                self.log_update_response(400, response)
                self.send_json(400, response)
            except Exception as exc:
                response = {"ok": False, "error": str(exc)}
                self.log_update_response(500, response)
                self.send_json(500, response)

        def log_update_response(self, status: int, payload: Dict[str, Any]) -> None:
            encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
            print(
                f"[exec-config] update status={status} response={encoded}",
                flush=True,
            )

        def log_message(self, fmt: str, *args: Any) -> None:
            print(f"[exec-config] {self.address_string()} {fmt % args}", flush=True)

    return Handler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="BatchExec Redis config server")
    parser.add_argument("--bind", default=os.environ.get("BIND", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "18161")))
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0"))
    parser.add_argument("--env-name", default=os.environ.get("ENV_NAME", ""))
    parser.add_argument("--venue", default=os.environ.get("VENUE", ""))
    parser.add_argument("--dashboard-url", default=os.environ.get("DASHBOARD_URL", "../"))
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    store = ExecConfigStore(args.redis_url, args.env_name, args.venue)
    server = ThreadingHTTPServer(
        (args.bind, args.port), make_handler(store, args.dashboard_url)
    )
    print(
        f"[exec-config] listening http://{args.bind}:{args.port} "
        f"prefix={store.prefix}"
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
