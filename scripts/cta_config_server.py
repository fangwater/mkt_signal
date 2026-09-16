#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""CTA 专用 config server（与 intra_config_server 独立）。

只暴露 cta env 真正消费的配置：
  - CTA Rules          -> {env}:cta_rules                        (JSON 数组，trade_signal 热加载)
  - Symbol Lists       -> {env}:cta_trade_symbols:{exchange}     (单一交易宇宙，无正反概念)
                         {env}:cta_dump_symbols:{exchange}       (平仓/禁用列表)
                         + 镜像 {env}:intra_bwd_trade_symbols:{exchange} (pre_trade 借贷白名单)
  - Strategy Params    -> cta_strategy_params_{open}_{hedge}     (hash)
  - Risk Params        -> {env}:{open}:{hedge}:pre_trade_risk_params (hash, pre_trade 读取)

schema 常量与通用 helper 从 intra_config_server import（单一来源），
Redis 校验复用 sync_cta_rules.validate_rules（与 Rust RawCtaRule 同规则）。

运行：在 env 目录（<exchange>-cta-<tag>）下启动，例如
  cd ~/binance-cta-rx01 && python3 scripts/cta_config_server.py --port 19174
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

import intra_config_server as base  # noqa: E402  (schema 常量与通用 helper 的单一来源)
import cta_rules_panel  # noqa: E402

SUPPORTED_EXCHANGES = base.SUPPORTED_EXCHANGES

_CSS_MATCH = re.search(
    r"<style>(.*?)</style>", base.INDEX_HTML_TEMPLATE, re.DOTALL
)
_PAGE_CSS = _CSS_MATCH.group(1) if _CSS_MATCH else ""

INDEX_HTML_TEMPLATE = (
    """<!DOCTYPE html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <title>CTA 配置中心</title>
  <style>"""
    + _PAGE_CSS
    + """</style>
</head>
<body>
  <header>
    <h1>CTA 配置中心</h1>
    <div class="toolbar">
      <div class="field">
        <label for="env-name">Env</label>
        <input id="env-name" readonly />
      </div>
      <div class="field">
        <label for="open-venue">Open Venue</label>
        <input id="open-venue" readonly />
      </div>
      <div class="field">
        <label for="hedge-venue">Hedge Venue</label>
        <input id="hedge-venue" readonly />
      </div>
      <div class="field">
        <label>Symbol Key Suffix</label>
        <div class="inline"><span id="key-suffix" class="badge">-</span></div>
      </div>
      <button id="reload-all" class="ghost" title="批量读取">读取全部</button>
    </div>
  </header>

  <main>
    <div class="subnav">
      <a href="#cta-rules">CTA Rules</a>
      <a href="#symbol-lists">Symbol Lists</a>
      <a href="#strategy-params">Strategy Params</a>
      <a href="#risk-params">Risk Params</a>
    </div>

    <section id="symbol-lists" class="panel">
      <div class="section-header">
        <h2>Symbol Lists</h2>
        <div class="actions">
          <button id="sym-load" class="secondary">读取</button>
          <button id="sym-save">保存</button>
        </div>
      </div>
      <div class="grid-2">
        <div>
          <h3>交易列表 <span class="hint">cta_trade_symbols</span></h3>
          <textarea id="sym-trade" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
        <div>
          <h3>平仓列表 <span class="hint">cta_dump_symbols</span></h3>
          <textarea id="sym-dump" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
      </div>
      <div class="hint" style="margin-top:8px;">
        cta 只有单一交易宇宙（无正反/vol gate 概念）。保存时同时把交易列表镜像到
        <code>{env}:intra_bwd_trade_symbols:{exchange}</code> 作为 pre_trade 现货借贷白名单。
      </div>
      <div id="sym-status" class="status"></div>
    </section>

    <section id="strategy-params" class="panel">
      <div class="section-header">
        <h2>Strategy Params</h2>
        <div class="actions">
          <button id="strategy-load" class="secondary">读取</button>
          <button id="strategy-save">保存</button>
          <button id="strategy-default" class="ghost">默认</button>
        </div>
      </div>
      <div class="hint">
        hash key: <code>cta_strategy_params_{open_venue}_{hedge_venue}</code>，trade_signal 60s 热加载。
        仅共享执行管道参数（对冲腿/订单TTL/冷却/撤单链路）；每笔网格参数（档位、单笔名义、TP、trailing）在
        <a href="#cta-rules">CTA Rules</a> 内按规则配置。
      </div>
      <div id="strategy-table" class="kv-table"></div>
      <div id="strategy-status" class="status"></div>
    </section>

    <section id="risk-params" class="panel">
      <div class="section-header">
        <h2>Risk Params (pre_trade)</h2>
        <div class="actions">
          <button id="risk-load" class="secondary">读取</button>
          <button id="risk-save">保存</button>
          <button id="risk-default" class="ghost">默认</button>
        </div>
      </div>
      <div class="hint">
        hash key: <code>{env}:{open}:{hedge}:pre_trade_risk_params</code>，pre_trade 60s 热加载。
      </div>
      <div id="risk-table" class="kv-table"></div>
      <div id="risk-status" class="status"></div>
    </section>
__CTA_RULES_PANEL_HTML__
  </main>

  <script>
    const BOOTSTRAP = __BOOTSTRAP__;

    const envNameInput = document.getElementById('env-name');
    const openVenueInput = document.getElementById('open-venue');
    const hedgeVenueInput = document.getElementById('hedge-venue');
    const keySuffixEl = document.getElementById('key-suffix');

    function setStatus(id, msg, ok = true) {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = msg;
      el.className = 'status ' + (ok ? 'ok' : 'err');
    }

    function applyFixedContext() {
      envNameInput.value = BOOTSTRAP.env_name || '';
      openVenueInput.value = BOOTSTRAP.default_open_venue || '';
      hedgeVenueInput.value = BOOTSTRAP.default_hedge_venue || '';
      keySuffixEl.textContent = BOOTSTRAP.key_suffix || '-';
    }

    function toList(text) {
      return (text || '')
        .split(/[\\s,]+/)
        .map(s => s.trim().toUpperCase())
        .filter(Boolean);
    }

    function fromList(arr) {
      return (arr || []).join("\\n");
    }

    function apiUrl(path) {
      const base = window.location.pathname.endsWith('/') ? window.location.pathname : window.location.pathname + '/';
      const clean = path.replace(/^\\//, '');
      return `${base}api/${clean}`;
    }

    function isBooleanParamValue(value) {
      const normalized = String(value ?? '').trim().toLowerCase();
      return ['true', 'false', '1', '0', 'yes', 'no', 'on', 'off', ''].includes(normalized);
    }

    async function fetchJson(url, opts = {}) {
      const resp = await fetch(url, opts);
      if (!resp.ok) {
        const text = await resp.text();
        throw new Error(`HTTP ${resp.status} ${text}`);
      }
      return await resp.json();
    }

    function buildParamRows(containerId, defaults, comments, order, values = {}) {
      const container = document.getElementById(containerId);
      container.innerHTML = '';
      const ordered = [...order];
      Object.keys(defaults).forEach(key => {
        if (!ordered.includes(key)) ordered.push(key);
      });
      Object.keys(values).forEach(key => {
        if (!ordered.includes(key)) ordered.push(key);
      });
      const boolKeys = new Set(containerId === 'strategy-table' ? (BOOTSTRAP.param_schema?.strategy_bool_params || []) : []);
      ordered.forEach(key => {
        const row = document.createElement('div');
        row.className = 'kv-row';
        const keyCell = document.createElement('div');
        keyCell.className = 'kv-key';
        keyCell.textContent = key;
        const inputCell = document.createElement('div');
        inputCell.className = 'kv-input';
        const rawValue = values[key] ?? defaults[key] ?? '';
        let input;
        if (boolKeys.has(key) && isBooleanParamValue(rawValue)) {
          input = document.createElement('select');
          [['false', 'false'], ['true', 'true']].forEach(([value, label]) => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = label;
            input.appendChild(option);
          });
          const normalized = String(rawValue ?? '').trim().toLowerCase();
          input.value = ['true', '1', 'yes', 'on'].includes(normalized) ? 'true' : 'false';
        } else {
          input = document.createElement('input');
          input.className = 'mono';
          input.value = rawValue;
        }
        input.dataset.key = key;
        inputCell.appendChild(input);
        const descCell = document.createElement('div');
        descCell.className = 'kv-desc';
        descCell.textContent = comments[key] || '';
        container.appendChild(keyCell);
        container.appendChild(inputCell);
        container.appendChild(descCell);
      });
    }

    function collectParamValues(containerId) {
      const container = document.getElementById(containerId);
      const values = {};
      container.querySelectorAll('input[data-key], select[data-key]').forEach(input => {
        values[input.dataset.key] = input.value.trim();
      });
      return values;
    }

    async function loadSymbolLists() {
      setStatus('sym-status', '读取中...');
      try {
        const data = await fetchJson(apiUrl('symbol-lists'));
        document.getElementById('sym-trade').value = fromList(data.trade_symbols || []);
        document.getElementById('sym-dump').value = fromList(data.dump_symbols || []);
        setStatus('sym-status', '读取完成');
      } catch (err) {
        setStatus('sym-status', `读取失败: ${err}`, false);
      }
    }

    async function saveSymbolLists() {
      setStatus('sym-status', '保存中...');
      try {
        const payload = {
          trade_symbols: toList(document.getElementById('sym-trade').value),
          dump_symbols: toList(document.getElementById('sym-dump').value),
        };
        await fetchJson(apiUrl('symbol-lists'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus('sym-status', '保存成功');
      } catch (err) {
        setStatus('sym-status', `保存失败: ${err}`, false);
      }
    }

    async function loadParamPanel(name) {
      setStatus(`${name}-status`, '读取中...');
      try {
        const data = await fetchJson(apiUrl(`${name}-params`));
        buildParamRows(`${name}-table`, BOOTSTRAP.defaults[`${name}_params`] || {}, BOOTSTRAP.comments[`${name}_params`] || {}, BOOTSTRAP.order[name] || [], data.values || {});
        const extra = data.stale_count ? `，忽略 ${data.stale_count} 个未知字段` : '';
        setStatus(`${name}-status`, `读取完成 (${data.count || 0} 字段${extra})`);
      } catch (err) {
        setStatus(`${name}-status`, `读取失败: ${err}`, false);
      }
    }

    async function saveParamPanel(name) {
      setStatus(`${name}-status`, '保存中...');
      try {
        const payload = { values: collectParamValues(`${name}-table`) };
        const data = await fetchJson(apiUrl(`${name}-params`), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus(`${name}-status`, `保存成功 (${data.count ?? data.written ?? ''} 字段)`);
      } catch (err) {
        setStatus(`${name}-status`, `保存失败: ${err}`, false);
      }
    }

    function applyParamDefaults(name) {
      buildParamRows(`${name}-table`, BOOTSTRAP.defaults[`${name}_params`] || {}, BOOTSTRAP.comments[`${name}_params`] || {}, BOOTSTRAP.order[name] || [], {});
      setStatus(`${name}-status`, '已载入默认值（未保存）');
    }

    const loadStrategyParams = () => loadParamPanel('strategy');
    const saveStrategyParams = () => saveParamPanel('strategy');
    const applyStrategyDefaults = () => applyParamDefaults('strategy');
    const loadRiskParams = () => loadParamPanel('risk');
    const saveRiskParams = () => saveParamPanel('risk');
    const applyRiskDefaults = () => applyParamDefaults('risk');

    async function reloadAll() {
      await loadSymbolLists();
      await loadStrategyParams();
      await loadRiskParams();
    }

    applyFixedContext();
    document.getElementById('sym-load').addEventListener('click', loadSymbolLists);
    document.getElementById('sym-save').addEventListener('click', saveSymbolLists);
    document.getElementById('strategy-load').addEventListener('click', loadStrategyParams);
    document.getElementById('strategy-save').addEventListener('click', saveStrategyParams);
    document.getElementById('strategy-default').addEventListener('click', applyStrategyDefaults);
    document.getElementById('risk-load').addEventListener('click', loadRiskParams);
    document.getElementById('risk-save').addEventListener('click', saveRiskParams);
    document.getElementById('risk-default').addEventListener('click', applyRiskDefaults);
    document.getElementById('reload-all').addEventListener('click', reloadAll);
__CTA_RULES_PANEL_JS__
    bindCtaRulesPanel();
    reloadAll();
  </script>
</body>
</html>
"""
)


def current_env_name() -> str:
    env = base.infer_dir_prefix_from_cwd() or ""
    if not env:
        raise ValueError("env_name unavailable (no cwd prefix)")
    return env


def strategy_params_key(open_venue: str, hedge_venue: str) -> str:
    return f"cta_strategy_params_{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"


def symbol_list_key(env_name: str, name: str, suffix: str, namespace: str = "cta") -> str:
    return base.intra_symbol_list_key(env_name, name, suffix, namespace)


# cta 是网格报单模型：每笔执行参数（网格档 open_offsets、单笔名义、TP、
# trailing、冷却、持仓上限）都在 cta_rules 规则内。strategy_params hash 只承载
# 共享执行链路里 mode-agnostic 的少量参数：
#   signal_cooldown    —— 信号冷却/扫档节拍（main.rs 决策循环直接消费）
#   open_order_timeout —— 开仓单 TTL 兜底（打进 ArbOpen ctx）
#   hedge_timeout      —— 对冲腿成交时限（打进 ArbOpen ctx / hedge 查询 exp_time）
#   enable_tlen_cancel / tlen_cancel_freq_ms —— 通用挂单撤单链路
# intra 的 inventory-hedge 定价（hedge_vol_multiplier/hedge_offset_ratio/
# hedge_price_offset_limit_*/max_hedge_price_pct_change）只服务 return-score
# 驱动的库存再平衡对冲，cta 的 per-lot entry 锚定 TP 对冲不消费；
# hedge_aggressive_seq_threshold 全库无读取点（死配置）；vol gate / taker
# decision model / model 角色订阅在 cta 路径均被 build_cta_shell 显式禁用。
_CTA_STRATEGY_KEYS: Tuple[str, ...] = (
    "signal_cooldown",
    "open_order_timeout",
    "hedge_timeout",
    "enable_tlen_cancel",
    "tlen_cancel_freq_ms",
)


def _cta_strategy_schema() -> Tuple[Dict[str, Any], Dict[str, str], List[str]]:
    allowed = set(_CTA_STRATEGY_KEYS)
    defaults = {
        k: v for k, v in base.DEFAULT_STRATEGY_PARAMS.items() if k in allowed
    }
    comments = {
        k: v for k, v in base.STRATEGY_PARAM_COMMENTS.items() if k in allowed
    }
    order = [
        k
        for k in base.STRATEGY_PARAM_ORDER
        if k in allowed
    ]
    return defaults, comments, order


def render_index_html(
    default_open_venue: Optional[str],
    default_hedge_venue: Optional[str],
) -> str:
    key_suffix = ""
    if default_open_venue and default_hedge_venue:
        try:
            key_suffix = base.make_key_suffix(default_open_venue, default_hedge_venue)
        except Exception:
            key_suffix = ""
    strategy_defaults, strategy_comments, strategy_order = _cta_strategy_schema()
    bootstrap = {
        "env_name": base.infer_dir_prefix_from_cwd() or "",
        "default_open_venue": default_open_venue or "",
        "default_hedge_venue": default_hedge_venue or "",
        "key_suffix": key_suffix,
        "features": {"cta_rules": True},
        "param_schema": {
            "strategy_bool_params": base.STRATEGY_BOOL_PARAM_KEYS,
        },
        "defaults": {
            "strategy_params": strategy_defaults,
            "risk_params": dict(base.DEFAULT_RISK_PARAMS),
        },
        "comments": {
            "strategy_params": strategy_comments,
            "risk_params": dict(base.RISK_PARAM_COMMENTS),
        },
        "order": {
            "strategy": strategy_order,
            "risk": base.RISK_PARAM_ORDER,
        },
    }
    html = INDEX_HTML_TEMPLATE.replace(
        "__BOOTSTRAP__", json.dumps(bootstrap, ensure_ascii=False)
    )
    html = html.replace(
        "__CTA_RULES_PANEL_HTML__", cta_rules_panel.render_cta_rules_panel_html()
    )
    html = html.replace(
        "__CTA_RULES_PANEL_JS__", cta_rules_panel.render_cta_rules_panel_js()
    )
    return html


@dataclass
class ServerContext:
    redis_client: Any
    default_open_venue: Optional[str]
    default_hedge_venue: Optional[str]


class CtaConfigServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, context: ServerContext):
        super().__init__(server_address, RequestHandlerClass)
        self.context = context


class RequestHandler(BaseHTTPRequestHandler):
    server: CtaConfigServer

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
        sys.stdout.write(
            "%s - - [%s] %s\n"
            % (self.address_string(), self.log_date_time_string(), fmt % args)
        )

    def do_OPTIONS(self) -> None:
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def _fixed_context(self) -> Tuple[str, str]:
        open_venue = self.server.context.default_open_venue
        hedge_venue = self.server.context.default_hedge_venue
        if not open_venue or not hedge_venue:
            raise ValueError("cta_config_server missing fixed open/hedge venue")
        return open_venue, hedge_venue

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            self._send_html(
                render_index_html(
                    self.server.context.default_open_venue,
                    self.server.context.default_hedge_venue,
                )
            )
            return

        if parsed.path == "/api/symbol-lists":
            try:
                open_venue, hedge_venue = self._fixed_context()
                key_suffix = base.make_key_suffix(open_venue, hedge_venue)
                env_name = current_env_name()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            rds = self.server.context.redis_client
            self._send_json(
                200,
                {
                    "env_name": env_name,
                    "key_suffix": key_suffix,
                    "trade_symbols": base.read_symbol_list(
                        rds, symbol_list_key(env_name, "trade_symbols", key_suffix)
                    ),
                    "dump_symbols": base.read_symbol_list(
                        rds, symbol_list_key(env_name, "dump_symbols", key_suffix)
                    ),
                },
            )
            return

        if parsed.path == "/api/strategy-params":
            try:
                open_venue, hedge_venue = self._fixed_context()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = strategy_params_key(open_venue, hedge_venue)
            raw_values = base.read_hash(self.server.context.redis_client, key)
            values, stale_values = base.filter_mapping_by_schema(
                raw_values,
                base.DEFAULT_STRATEGY_PARAMS,
                base.STRATEGY_PARAM_COMMENTS,
                base.STRATEGY_PARAM_ORDER,
            )
            self._send_json(
                200,
                {
                    "key": key,
                    "values": values,
                    "raw_count": len(raw_values),
                    "count": len(values),
                    "stale_count": len(stale_values),
                    "stale_values": stale_values,
                },
            )
            return

        if parsed.path == "/api/risk-params":
            try:
                open_venue, hedge_venue = self._fixed_context()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = base.build_risk_params_key(open_venue, hedge_venue)
            raw_values = base.read_hash(self.server.context.redis_client, key)
            values, stale_values = base.filter_mapping_by_schema(
                raw_values,
                base.DEFAULT_RISK_PARAMS,
                base.RISK_PARAM_COMMENTS,
                base.RISK_PARAM_ORDER,
            )
            if not values and not raw_values:
                self._send_error(404, f"risk params not found: {key}")
                return
            self._send_json(
                200,
                {
                    "key": key,
                    "values": values,
                    "raw_count": len(raw_values),
                    "count": len(values),
                    "stale_count": len(stale_values),
                    "stale_values": stale_values,
                },
            )
            return

        if parsed.path == "/api/cta-rules":
            try:
                env_name = current_env_name()
            except ValueError as exc:
                self._send_error(400, str(exc))
                return
            self._send_json(
                200,
                cta_rules_panel.read_cta_rules(
                    self.server.context.redis_client, env_name
                ),
            )
            return

        self._send_error(404, "not found")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
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
        if parsed.path.startswith("/api/"):
            print(
                "[request] POST {} len={} keys={}".format(
                    parsed.path, length, list(payload.keys())
                )
            )
            sys.stdout.flush()

        if parsed.path == "/api/symbol-lists":
            try:
                open_venue, hedge_venue = self._fixed_context()
                key_suffix = base.make_key_suffix(open_venue, hedge_venue)
                env_name = current_env_name()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            for rejected in ("fwd_trade_symbols", "bwd_trade_symbols", "vol_gate_symbols"):
                if payload.get(rejected):
                    self._send_error(
                        400,
                        f"cta env 不支持 {rejected}；使用 trade_symbols（单一交易宇宙）",
                    )
                    return
            trade_symbols = base.normalize_symbol_list_for_intra(
                payload.get("trade_symbols") or []
            )
            dump_symbols = base.normalize_symbol_list_for_intra(
                payload.get("dump_symbols") or []
            )
            rds = self.server.context.redis_client
            try:
                rds.set(
                    symbol_list_key(env_name, "trade_symbols", key_suffix),
                    json.dumps(trade_symbols, ensure_ascii=False),
                )
                rds.set(
                    symbol_list_key(env_name, "dump_symbols", key_suffix),
                    json.dumps(dump_symbols, ensure_ascii=False),
                )
                # pre_trade 的现货借贷白名单固定读 {env}:intra_bwd_trade_symbols:{suffix}
                rds.set(
                    symbol_list_key(env_name, "bwd_trade_symbols", key_suffix, "intra"),
                    json.dumps(trade_symbols, ensure_ascii=False),
                )
                # 清掉历史误写的 cta fwd/bwd/vol_gate key（loader 已不读）
                for stale in ("fwd_trade_symbols", "bwd_trade_symbols", "vol_gate_symbols"):
                    rds.delete(symbol_list_key(env_name, stale, key_suffix))
            except Exception as exc:
                self._send_error(500, f"redis write failed: {exc}")
                return
            print(
                "[symbol-lists][cta] env={} key_suffix={} trade={} dump={}".format(
                    env_name, key_suffix, len(trade_symbols), len(dump_symbols)
                )
            )
            sys.stdout.flush()
            self._send_json(
                200,
                {
                    "env_name": env_name,
                    "key_suffix": key_suffix,
                    "trade_count": len(trade_symbols),
                    "dump_count": len(dump_symbols),
                },
            )
            return

        if parsed.path == "/api/strategy-params":
            try:
                open_venue, hedge_venue = self._fixed_context()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            key = strategy_params_key(open_venue, hedge_venue)
            st_defaults, st_comments, st_order = _cta_strategy_schema()
            try:
                mapping = base.sanitize_mapping_by_schema(
                    values,
                    st_defaults,
                    st_comments,
                    st_order,
                    normalize_strategy=True,
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            result = base.replace_hash(self.server.context.redis_client, key, mapping)
            self._send_json(200, result)
            return

        if parsed.path == "/api/risk-params":
            try:
                open_venue, hedge_venue = self._fixed_context()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            exchange = base.exchange_from_venue(open_venue) or ""
            values = payload.get("values") or {}
            key = base.build_risk_params_key(open_venue, hedge_venue)
            try:
                mapping = base.sanitize_mapping_by_schema(
                    values,
                    base.DEFAULT_RISK_PARAMS,
                    base.RISK_PARAM_COMMENTS,
                    base.RISK_PARAM_ORDER,
                )
                mapping = base.normalize_unimmr_control_lines(mapping)
                mapping = base.normalize_intra_risk_limits(exchange, mapping)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            result = base.replace_hash(self.server.context.redis_client, key, mapping)
            print(
                f"[risk-params][POST] key={key} fields={len(mapping)} removed={result['removed_count']}"
            )
            sys.stdout.flush()
            self._send_json(200, result)
            return

        if parsed.path == "/api/cta-rules":
            try:
                env_name = current_env_name()
            except ValueError as exc:
                self._send_error(400, str(exc))
                return
            rules = payload.get("rules")
            if not isinstance(rules, list):
                self._send_error(400, "rules must be an array of rule objects")
                return
            dry_run = bool(payload.get("dry_run"))
            if dry_run:
                errors = cta_rules_panel.validate_cta_rules(rules)
                if errors:
                    self._send_json(200, {"ok": False, "errors": errors})
                    return
                self._send_json(
                    200,
                    {
                        "ok": True,
                        "key": cta_rules_panel.cta_rules_redis_key(env_name),
                        "count": len(rules),
                    },
                )
                return
            try:
                result = cta_rules_panel.write_cta_rules(
                    self.server.context.redis_client, env_name, rules
                )
            except ValueError as exc:
                self._send_error(400, str(exc))
                return
            except Exception as exc:
                self._send_error(500, f"redis write failed: {exc}")
                return
            print(
                "[cta-rules][POST] env={} key={} count={}".format(
                    env_name, result["key"], result["count"]
                )
            )
            sys.stdout.flush()
            self._send_json(200, result)
            return

        self._send_error(404, "not found")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="CTA config server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--default-exchange", default="")
    parser.add_argument("--default-open-venue", default="")
    parser.add_argument("--default-hedge-venue", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    redis = base.try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请执行: pip install redis", file=sys.stderr)
        return 2

    default_open_venue = args.default_open_venue
    default_hedge_venue = args.default_hedge_venue
    if (not default_open_venue or not default_hedge_venue) and (
        base.infer_default_venues_from_cwd()
    ):
        inferred_open, inferred_hedge = base.infer_default_venues_from_cwd() or ("", "")
        default_open_venue = default_open_venue or inferred_open
        default_hedge_venue = default_hedge_venue or inferred_hedge

    env_name = base.infer_dir_prefix_from_cwd() or ""
    if "-cta-" not in env_name:
        print(
            f"[WARN] 当前目录 '{env_name or os.getcwd()}' 不是 <exchange>-cta-<tag> 形式，"
            "cta_config_server 应按 env 目录启动",
            file=sys.stderr,
        )

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    context = ServerContext(
        redis_client=rds,
        default_open_venue=default_open_venue,
        default_hedge_venue=default_hedge_venue,
    )
    try:
        server = CtaConfigServer((args.host, args.port), RequestHandler, context)
    except OSError as exc:
        print(
            f"❌ 无法监听 {args.host}:{args.port}，端口可能被占用: {exc}",
            file=sys.stderr,
        )
        return 2
    print(
        f"🚀 cta_config_server started on http://{args.host}:{args.port} "
        f"(env={env_name or '-'}, open={default_open_venue or '-'}, hedge={default_hedge_venue or '-'})"
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
