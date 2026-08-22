#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""BatchExec Redis configuration server."""

from __future__ import annotations

import argparse
import hmac
import json
import math
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

STRATEGY_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
SYMBOL_RE = re.compile(r"^[A-Z0-9]+$")
ORDER_PARAMETER_FIELDS = (
    "single_order_usdt",
    "orders_per_batch",
    "max_batch",
    "maker_price_anchor",
    "tick_spacing",
    "batch_interval_ms",
    "maker_timeout_ms",
    "max_maker_requotes",
    "target_tolerance_usdt",
)
CONFIG_FIELDS = set(ORDER_PARAMETER_FIELDS) | {"targets"}
OPTIONAL_CONFIG_FIELDS = {"updated_at_us"}
ALLOWED_TARGET_SIGNALS = (-2, -1, 0, 1, 2)
DEFAULT_CONFIG: Dict[str, Any] = {
    "single_order_usdt": 100.0,
    "orders_per_batch": 3,
    "max_batch": 20,
    "maker_price_anchor": "own_best",
    "tick_spacing": 1,
    "batch_interval_ms": 500,
    "maker_timeout_ms": 1000,
    "max_maker_requotes": 2,
    "target_tolerance_usdt": 10.0,
    "targets": {},
}
POSITION_CLOSE_STRATEGY_NAME = "SYSTEM_POSITION_CLOSE"
ORDER_PARAMETER_TOKEN_ENV = "CRYPTO_CTA_MANAGER_WRITE_TOKEN"


class ConfigVersionConflict(ValueError):
    """Raised when an editor tries to overwrite a newer Redis value."""


def validate_strategy_name(raw: Any) -> str:
    name = str(raw or "").strip()
    if not STRATEGY_NAME_RE.fullmatch(name):
        raise ValueError("strategy_name must match [A-Za-z0-9][A-Za-z0-9._-]*")
    if name in {
        "strategy_names",
        "removed_strategy_names",
        POSITION_CLOSE_STRATEGY_NAME,
    }:
        raise ValueError(f"strategy_name is reserved: {name}")
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


def signed_integer(raw: Any, field: str) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError(f"{field} must be an integer")
    return raw


def normalize_target_signal(raw: Any, field: str) -> int:
    if raw is None:
        return 0
    value = signed_integer(raw, field)
    if value not in ALLOWED_TARGET_SIGNALS:
        allowed = ", ".join(str(item) for item in ALLOWED_TARGET_SIGNALS)
        raise ValueError(f"{field} must be one of {allowed}")
    return value


def normalize_target_position(raw: Any, field: str) -> Dict[str, Any]:
    if isinstance(raw, bool) or isinstance(raw, (int, float, str)):
        return {"qty": finite_float(raw, f"{field}.qty"), "signal": 0}
    if not isinstance(raw, dict):
        raise ValueError(f"{field} must be a number or an object with qty")
    unknown = sorted(set(raw) - {"qty", "signal"})
    if unknown:
        raise ValueError(f"unknown {field} fields: {', '.join(unknown)}")
    if "qty" not in raw:
        raise ValueError(f"{field}.qty is required")
    return {
        "qty": finite_float(raw["qty"], f"{field}.qty"),
        "signal": normalize_target_signal(raw.get("signal"), f"{field}.signal"),
    }


def normalize_targets(raw: Any) -> Dict[str, Dict[str, Any]]:
    if not isinstance(raw, dict):
        raise ValueError("targets must be an object")
    targets: Dict[str, Dict[str, Any]] = {}
    for raw_symbol, raw_target in raw.items():
        symbol = normalize_symbol(raw_symbol)
        if symbol in targets:
            raise ValueError(f"duplicate symbol: {symbol}")
        targets[symbol] = normalize_target_position(raw_target, f"targets.{symbol}")
    return dict(sorted(targets.items()))


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
    raw = {**raw}
    raw.setdefault("max_batch", DEFAULT_CONFIG["max_batch"])
    unknown = sorted(set(raw) - CONFIG_FIELDS - OPTIONAL_CONFIG_FIELDS)
    missing = sorted(CONFIG_FIELDS - set(raw))
    if unknown:
        raise ValueError(f"unknown fields: {', '.join(unknown)}")
    if missing:
        raise ValueError(f"missing fields: {', '.join(missing)}")

    anchor = str(raw["maker_price_anchor"]).strip()
    if anchor not in {"own_best", "opposite_best_plus_one_tick"}:
        raise ValueError("invalid maker_price_anchor")

    targets = normalize_targets(raw["targets"])

    tolerance = finite_float(raw["target_tolerance_usdt"], "target_tolerance_usdt")
    if tolerance < 0:
        raise ValueError("target_tolerance_usdt must be >= 0")

    normalized = {
        "single_order_usdt": finite_float(
            raw["single_order_usdt"], "single_order_usdt", positive=True
        ),
        "orders_per_batch": integer(
            raw["orders_per_batch"], "orders_per_batch", positive=True
        ),
        "max_batch": integer(raw["max_batch"], "max_batch", positive=True),
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
        "targets": targets,
    }
    updated_at_us = raw.get("updated_at_us")
    if updated_at_us is not None:
        if isinstance(updated_at_us, bool) or not isinstance(updated_at_us, int):
            raise ValueError("updated_at_us must be an integer")
        if updated_at_us <= 0 or updated_at_us > 9_223_372_036_854_775_807:
            raise ValueError("updated_at_us must be a positive int64")
        normalized["updated_at_us"] = updated_at_us
    return normalized


def normalize_order_parameters(raw: Any) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("order_parameters must be an object")
    allowed = set(ORDER_PARAMETER_FIELDS)
    unknown = sorted(set(raw) - allowed)
    missing = sorted(allowed - set(raw))
    if unknown:
        raise ValueError(f"unknown order parameter fields: {', '.join(unknown)}")
    if missing:
        raise ValueError(f"missing order parameter fields: {', '.join(missing)}")

    normalized = normalize_exec_config({**raw, "targets": {}})
    return {field: normalized[field] for field in ORDER_PARAMETER_FIELDS}


def normalize_expected_updated_at_us(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise ValueError("expected_updated_at_us must be an integer or null")
    if raw <= 0 or raw > 9_223_372_036_854_775_807:
        raise ValueError("expected_updated_at_us must be a positive int64")
    return raw


def decode_strategy_names(raw: Any, label: str) -> List[str]:
    if raw is None:
        return []
    try:
        decoded = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is not valid JSON: {exc}") from exc
    if not isinstance(decoded, list):
        raise ValueError(f"{label} must be a JSON array")
    names = [validate_strategy_name(name) for name in decoded]
    if len(names) != len(set(names)):
        raise ValueError(f"{label} contains duplicate names")
    return sorted(names)


def decode_stored_exec_config(raw: Any) -> Optional[Dict[str, Any]]:
    if raw is None:
        return None
    try:
        decoded = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Redis value is not valid JSON: {exc}") from exc
    return normalize_exec_config(decoded)


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
        self._watch_error = redis.exceptions.WatchError
        self.env_name = str(env_name).strip()
        self.venue = str(venue).strip()
        if not self.env_name or not self.venue:
            raise ValueError("env_name and venue are required")
        if self.venue not in ("binance-futures", "okex-futures"):
            raise ValueError("venue must be binance-futures or okex-futures")
        self.prefix = f"{self.env_name}:{self.venue}:batch_exec:"
        self.index_key = f"{self.prefix}strategy_names"
        self.removed_index_key = f"{self.prefix}removed_strategy_names"
        self._save_lock = threading.Lock()

    def key(self, strategy_name: str) -> str:
        return f"{self.prefix}{validate_strategy_name(strategy_name)}"

    def list_strategy_names(self) -> List[str]:
        return decode_strategy_names(self.client.get(self.index_key), "strategy index")

    def load(self, strategy_name: str) -> Optional[Dict[str, Any]]:
        return decode_stored_exec_config(self.client.get(self.key(strategy_name)))

    def list_removed_strategy_names(self) -> List[str]:
        return decode_strategy_names(
            self.client.get(self.removed_index_key), "removed strategy index"
        )

    def save(self, strategy_name: str, config: Any) -> Dict[str, Any]:
        name = validate_strategy_name(strategy_name)
        normalized = normalize_exec_config(config)
        with self._save_lock:
            if name in self.list_removed_strategy_names():
                raise ValueError(f"strategy removal already requested: {name}")
            strategy_names = self.list_strategy_names()
            current = self.load(name) if name in strategy_names else None
            if current is not None:
                # Existing strategy publishers own targets; the Config page owns order params.
                for field in ORDER_PARAMETER_FIELDS:
                    normalized[field] = current[field]
            # Receipt time is authoritative even when a publisher repeats an unchanged target map.
            next_version = time.time_ns() // 1_000
            if current is not None and current.get("updated_at_us") is not None:
                next_version = max(next_version, current["updated_at_us"] + 1)
            normalized["updated_at_us"] = next_version
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

    def save_order_parameters(
        self,
        strategy_name: str,
        order_parameters: Any,
        expected_updated_at_us: Any,
    ) -> Dict[str, Any]:
        name = validate_strategy_name(strategy_name)
        normalized_parameters = normalize_order_parameters(order_parameters)
        expected_version = normalize_expected_updated_at_us(expected_updated_at_us)
        config_key = self.key(name)
        with self._save_lock:
            try:
                with self.client.pipeline() as pipeline:
                    pipeline.watch(
                        self.index_key,
                        self.removed_index_key,
                        config_key,
                    )
                    strategy_names = decode_strategy_names(
                        pipeline.get(self.index_key), "strategy index"
                    )
                    removed_names = decode_strategy_names(
                        pipeline.get(self.removed_index_key),
                        "removed strategy index",
                    )
                    current = decode_stored_exec_config(pipeline.get(config_key))

                    if name in removed_names:
                        raise ValueError(f"strategy removal already requested: {name}")
                    if name not in strategy_names:
                        raise ValueError(f"strategy is not active: {name}")
                    if current is None:
                        raise ValueError(f"strategy config is missing: {name}")
                    current_version = current.get("updated_at_us")
                    if current_version != expected_version:
                        raise ConfigVersionConflict(
                            "strategy config changed after it was loaded; reload before saving"
                        )

                    updated = dict(current)
                    updated.update(normalized_parameters)
                    next_version = time.time_ns() // 1_000
                    if current_version is not None:
                        next_version = max(next_version, current_version + 1)
                    updated["updated_at_us"] = next_version
                    pipeline.multi()
                    pipeline.set(
                        config_key,
                        json.dumps(
                            updated,
                            ensure_ascii=False,
                            separators=(",", ":"),
                        ),
                    )
                    pipeline.execute()
            except self._watch_error as exc:
                raise ConfigVersionConflict(
                    "strategy config changed while it was being saved; reload before saving"
                ) from exc
        return updated

    def remove(self, strategy_name: str) -> bool:
        name = validate_strategy_name(strategy_name)
        with self._save_lock:
            strategy_names = self.list_strategy_names()
            removed_names = self.list_removed_strategy_names()
            if (
                name not in strategy_names
                and name not in removed_names
                and self.client.get(self.key(name)) is None
            ):
                return False
            if name not in removed_names:
                removed_names.append(name)
                self.client.set(
                    self.removed_index_key,
                    json.dumps(
                        sorted(removed_names),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                )
            if name in strategy_names:
                strategy_names.remove(name)
                self.client.set(
                    self.index_key,
                    json.dumps(strategy_names, ensure_ascii=False, separators=(",", ":")),
                )
        return True


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
      input:disabled, select:disabled { color: var(--text); opacity: 1; cursor: default; }
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
      .readonly-state { color: var(--muted); font-size: 11px; }
      .param-grid { display: grid; grid-template-columns: repeat(4, minmax(170px, 1fr)); gap: 12px; }
      .field { min-width: 0; }
      .field label { display: block; margin-bottom: 6px; color: var(--muted); font-size: 11px; }
      .field select { width: 100%; min-width: 0; }
      .targets { border: 1px solid var(--line); border-radius: 8px; overflow: auto; background: var(--surface); }
      table { width: 100%; border-collapse: collapse; }
      th, td { height: 42px; border-bottom: 1px solid var(--line); padding: 6px 10px; text-align: left; }
      th { color: var(--muted); font-size: 11px; background: var(--surface2); }
      td:last-child, th:last-child { text-align: right; }
      tbody tr:last-child td { border-bottom: 0; }
      td input { background: var(--bg); }
      .empty { padding: 36px; text-align: center; color: var(--muted); }
      .footer-actions { display: flex; justify-content: flex-end; gap: 8px; padding-top: 4px; }
      @media (max-width: 860px) { .param-grid { grid-template-columns: repeat(2, 1fr); } .status { width: 100%; margin-left: 0; } }
      @media (max-width: 520px) {
        header, main { padding-left: 10px; padding-right: 10px; }
        .param-grid { grid-template-columns: 1fr; }
        select { min-width: 150px; flex: 1; }
      }
    </style>
  </head>
  <body>
    <header>
      <h1>Exec Config</h1>
      <select id="strategy"><option value="">Select strategy</option></select>
      <button id="reload" type="button">Reload</button>
      <a id="dashboard" class="command" href="../">Dashboard</a>
      <span id="status" class="status">Loading</span>
    </header>
    <main>
      <div id="redis-key" class="key">-</div>
      <section>
        <div class="section-head"><h2>Order Parameters</h2></div>
        <div class="param-grid">
          <div class="field"><label>Single Order USDT</label><input id="single_order_usdt" inputmode="decimal" disabled /></div>
          <div class="field"><label>Orders Per Batch</label><input id="orders_per_batch" inputmode="numeric" disabled /></div>
          <div class="field"><label>Max Batch</label><input id="max_batch" inputmode="numeric" disabled /></div>
          <div class="field"><label>Maker Price Anchor</label><select id="maker_price_anchor" disabled><option value="own_best">Own Best</option><option value="opposite_best_plus_one_tick">Opposite Best + 1 Tick</option></select></div>
          <div class="field"><label>Tick Spacing</label><input id="tick_spacing" inputmode="numeric" disabled /></div>
          <div class="field"><label>Batch Interval ms</label><input id="batch_interval_ms" inputmode="numeric" disabled /></div>
          <div class="field"><label>Maker Timeout ms</label><input id="maker_timeout_ms" inputmode="numeric" disabled /></div>
          <div class="field"><label>Max Maker Requotes</label><input id="max_maker_requotes" inputmode="numeric" disabled /></div>
          <div class="field"><label>Target Tolerance USDT</label><input id="target_tolerance_usdt" inputmode="decimal" disabled /></div>
        </div>
      </section>
      <section>
        <div class="section-head">
          <h2>Target Positions</h2>
          <span class="readonly-state">Read only</span>
        </div>
        <div class="targets">
          <table><thead><tr><th>Symbol</th><th>Target Qty</th><th>Signal</th></tr></thead><tbody id="target-rows"></tbody></table>
          <div id="target-empty" class="empty">No non-zero target positions</div>
        </div>
      </section>
    </main>
    <script>
      (() => {
        const DEFAULTS = __DEFAULTS__;
        const fields = ["single_order_usdt", "orders_per_batch", "max_batch", "maker_price_anchor", "tick_spacing", "batch_interval_ms", "maker_timeout_ms", "max_maker_requotes", "target_tolerance_usdt"];
        const state = { bootstrap: null, names: [], name: "", config: null };
        const el = (id) => document.getElementById(id);
        function api(path) { return new URL(`api/${path}`, location.href).toString(); }
        function setStatus(text, level = "") { el("status").textContent = text; el("status").className = `status ${level}`.trim(); }
        async function request(path, options = {}) {
          const response = await fetch(api(path), { headers: {"Content-Type": "application/json"}, ...options });
          const body = await response.json().catch(() => ({}));
          if (!response.ok) throw new Error(body.error || `HTTP ${response.status}`);
          return body;
        }
        function strategyFromQuery() { return new URLSearchParams(location.search).get("strategy") || ""; }
        function updateQuery() { const url = new URL(location.href); state.name ? url.searchParams.set("strategy", state.name) : url.searchParams.delete("strategy"); history.replaceState(null, "", url); }
        function renderNames() {
          const select = el("strategy");
          select.innerHTML = '<option value="">Select strategy</option>' + state.names.map((name) => `<option value="${name}">${name}</option>`).join("");
          select.value = state.name;
        }
        function targetQty(raw) { return raw && typeof raw === "object" ? Number(raw.qty) : Number(raw); }
        function targetSignal(raw) { return raw && typeof raw === "object" && raw.signal != null ? Number(raw.signal) : 0; }
        function renderTargets(targets) {
          const tbody = el("target-rows");
          tbody.innerHTML = "";
          Object.entries(targets || {})
            .filter(([, raw]) => targetQty(raw) !== 0)
            .sort(([a], [b]) => a.localeCompare(b))
            .forEach(([symbol, raw]) => {
              const tr = document.createElement("tr");
              const qty = targetQty(raw);
              const signal = targetSignal(raw);
              tr.innerHTML = `<td>${String(symbol).replace(/&/g, "&amp;").replace(/</g, "&lt;")}</td><td>${qty}</td><td>${signal}</td>`;
              tbody.appendChild(tr);
            });
          el("target-empty").style.display = tbody.children.length ? "none" : "block";
        }
        function renderConfig(config) {
          state.config = structuredClone(config);
          fields.forEach((name) => { el(name).value = config[name]; el(name).disabled = true; });
          renderTargets(config.targets);
          el("redis-key").textContent = state.bootstrap ? `${state.bootstrap.key_prefix}${state.name}` : "-";
        }
        async function loadNames(preferred = "") {
          const payload = await request("strategies");
          state.names = payload.strategies || [];
          if (preferred && state.names.includes(preferred)) state.name = preferred;
          else if (!state.names.includes(state.name)) state.name = state.names[0] || "";
          renderNames();
          if (state.name) await loadStrategy(state.name); else { renderConfig(DEFAULTS); setStatus("No strategies", "warn"); }
        }
        async function loadStrategy(name) {
          if (!name) return;
          setStatus("Loading");
          try {
            const payload = await request(`strategy?name=${encodeURIComponent(name)}`);
            state.name = name;
            renderConfig(payload.config || DEFAULTS);
            renderNames(); updateQuery(); setStatus(payload.exists ? "Loaded" : "Strategy not found", payload.exists ? "ok" : "warn");
          } catch (error) { setStatus(error.message, "err"); }
        }
        async function boot() {
          try {
            state.bootstrap = await request("bootstrap");
            el("dashboard").href = new URL(state.bootstrap.dashboard_url || "../", location.href);
            await loadNames(strategyFromQuery());
          } catch (error) { setStatus(error.message, "err"); }
        }
        el("strategy").onchange = (event) => loadStrategy(event.target.value);
        el("reload").onclick = () => loadNames(state.name);
        boot();
      })();
    </script>
  </body>
</html>"""


def make_handler(
    store: ExecConfigStore,
    dashboard_url: str,
    order_parameter_token: Optional[str] = None,
):
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
                    self.send_json(
                        200,
                        {
                            "ok": True,
                            "strategies": store.list_strategy_names(),
                            "removed": store.list_removed_strategy_names(),
                        },
                    )
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
                if parsed.path not in {"/api/strategy", "/api/order-parameters"}:
                    response = {"ok": False, "error": "not found"}
                    self.log_update_response(404, response)
                    self.send_json(404, response)
                    return
                if parsed.path == "/api/order-parameters":
                    if not order_parameter_token:
                        response = {
                            "ok": False,
                            "error": "order parameter writes are not configured",
                        }
                        self.log_update_response(503, response)
                        self.send_json(503, response)
                        return
                    authorization = self.headers.get("Authorization", "")
                    expected = f"Bearer {order_parameter_token}"
                    if not hmac.compare_digest(authorization, expected):
                        response = {
                            "ok": False,
                            "error": "write authorization is required",
                        }
                        self.log_update_response(401, response)
                        self.send_json(401, response)
                        return
                payload = self.read_json()
                if parsed.path == "/api/order-parameters":
                    required = {
                        "strategy_name",
                        "expected_updated_at_us",
                        "order_parameters",
                    }
                    unknown = sorted(set(payload) - required)
                    missing = sorted(required - set(payload))
                    if unknown:
                        raise ValueError(
                            f"unknown request fields: {', '.join(unknown)}"
                        )
                    if missing:
                        raise ValueError(
                            f"missing request fields: {', '.join(missing)}"
                        )
                    name = validate_strategy_name(payload["strategy_name"])
                    config = store.save_order_parameters(
                        name,
                        payload["order_parameters"],
                        payload["expected_updated_at_us"],
                    )
                    response = {
                        "ok": True,
                        "strategy_name": name,
                        "key": store.key(name),
                        "order_parameters": {
                            field: config[field] for field in ORDER_PARAMETER_FIELDS
                        },
                        "updated_at_us": config["updated_at_us"],
                    }
                    self.log_update_response(200, response)
                    self.send_json(200, response)
                    return
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
            except ConfigVersionConflict as exc:
                response = {"ok": False, "error": str(exc)}
                self.log_update_response(409, response)
                self.send_json(409, response)
            except (ValueError, json.JSONDecodeError) as exc:
                response = {"ok": False, "error": str(exc)}
                self.log_update_response(400, response)
                self.send_json(400, response)
            except Exception as exc:
                response = {"ok": False, "error": str(exc)}
                self.log_update_response(500, response)
                self.send_json(500, response)

        def do_DELETE(self) -> None:
            parsed = urlparse(self.path)
            try:
                if parsed.path != "/api/strategy":
                    response = {"ok": False, "error": "not found"}
                    self.log_update_response(404, response)
                    self.send_json(404, response)
                    return
                query = parse_qs(parsed.query)
                name = validate_strategy_name((query.get("name") or [""])[0])
                if not store.remove(name):
                    response = {"ok": False, "error": "strategy is unknown"}
                    self.log_update_response(404, response)
                    self.send_json(404, response)
                    return
                response = {
                    "ok": True,
                    "strategy_name": name,
                    "state": "removal_requested",
                }
                self.log_update_response(202, response)
                self.send_json(202, response)
            except ValueError as exc:
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
    parser.add_argument(
        "--order-parameter-token-file",
        default=os.environ.get("ORDER_PARAMETER_TOKEN_FILE", ""),
    )
    return parser.parse_args()


def load_order_parameter_token(raw_path: str) -> Optional[str]:
    path_text = str(raw_path or "").strip()
    if not path_text:
        return None
    path = Path(path_text)
    mode = path.stat().st_mode & 0o777
    if mode & 0o077:
        raise ValueError("order parameter token file must not be accessible by group or other")
    contents = path.read_text(encoding="utf-8")
    assignments = []
    for raw_line in contents.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").lstrip()
        key, separator, value = line.partition("=")
        if separator and key.strip() == ORDER_PARAMETER_TOKEN_ENV:
            assignments.append(value.strip())
    if len(assignments) > 1:
        raise ValueError(f"duplicate {ORDER_PARAMETER_TOKEN_ENV} assignments")
    token = assignments[0] if assignments else contents.strip()
    if len(token) >= 2 and token[0] == token[-1] and token[0] in {'"', "'"}:
        token = token[1:-1]
    if len(token) < 32 or any(character.isspace() for character in token):
        raise ValueError(
            "order parameter token must contain at least 32 non-whitespace characters"
        )
    return token


def main() -> int:
    args = parse_args()
    store = ExecConfigStore(args.redis_url, args.env_name, args.venue)
    order_parameter_token = load_order_parameter_token(
        args.order_parameter_token_file
    )
    server = ThreadingHTTPServer(
        (args.bind, args.port),
        make_handler(store, args.dashboard_url, order_parameter_token),
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
