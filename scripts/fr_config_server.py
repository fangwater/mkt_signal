#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Funding Rate 配置服务器（fr_config_server）
-------------------------------------------
提供页面/接口用于查看和编辑 FR 相关配置：
- symbol lists
- rolling metrics params
- funding rate thresholds
- strategy params
- risk params
- spread thresholds (可从 rolling_metrics 同步)
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

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]

EXCHANGE_DEFAULTS = {
    "binance": ("binance-margin", "binance-futures"),
    "okex": ("okex-margin", "okex-futures"),
    "bybit": ("bybit-margin", "bybit-futures"),
    "bitget": ("bitget-margin", "bitget-futures"),
    "gate": ("gate-margin", "gate-futures"),
}

ROLLING_METRICS_SCRIPT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "rolling_metrics",
)
if os.path.isdir(ROLLING_METRICS_SCRIPT_DIR):
    sys.path.insert(0, ROLLING_METRICS_SCRIPT_DIR)


def infer_dir_prefix_from_cwd() -> Optional[str]:
    name = os.path.basename(os.getcwd()).strip().lower()
    return name or None


def build_risk_params_key(open_venue: str, hedge_venue: str) -> str:
    dir_prefix = infer_dir_prefix_from_cwd()
    if dir_prefix:
        return f"{dir_prefix}:{open_venue}:{hedge_venue}:pre_trade_risk_params"
    return f"{open_venue}:{hedge_venue}:pre_trade_risk_params"

try:
    import sync_fr_risk_params as risk_defaults

    DEFAULT_RISK_PARAMS = dict(risk_defaults.RISK_PARAMS)
    RISK_PARAM_COMMENTS = dict(risk_defaults.PARAM_COMMENTS)
except Exception:
    DEFAULT_RISK_PARAMS = {}
    RISK_PARAM_COMMENTS = {}

try:
    import sync_fr_strategy_params as strategy_defaults

    DEFAULT_STRATEGY_PARAMS = dict(strategy_defaults.STRATEGY_PARAMS)
    STRATEGY_PARAM_COMMENTS = dict(strategy_defaults.PARAM_COMMENTS)
except Exception:
    DEFAULT_STRATEGY_PARAMS = {}
    STRATEGY_PARAM_COMMENTS = {}

try:
    import sync_funding_rate_thresholds as funding_defaults

    DEFAULT_FUNDING_THRESHOLDS = dict(funding_defaults.FUNDING_RATE_THRESHOLDS)
    FUNDING_THRESHOLD_COMMENTS = dict(funding_defaults.THRESHOLD_COMMENTS)
    FUNDING_THRESHOLD_ORDER = list(funding_defaults.THRESHOLD_ORDER)
except Exception:
    DEFAULT_FUNDING_THRESHOLDS = {}
    FUNDING_THRESHOLD_COMMENTS = {}
    FUNDING_THRESHOLD_ORDER = []

try:
    import sync_rolling_metrics_params as rolling_defaults

    DEFAULT_ROLLING_PARAMS = dict(rolling_defaults.DEFAULTS)
except Exception:
    DEFAULT_ROLLING_PARAMS = {
        "MAX_LENGTH": 150_000,
        "refresh_sec": 30,
        "reload_param_sec": 3,
        "factors": {},
    }

try:
    import sync_fr_symbol_lists as symbol_defaults

    SYMBOL_DEFAULTS_SRC = symbol_defaults
except Exception:
    SYMBOL_DEFAULTS_SRC = None

spread_sync = None
try:
    import sync_fr_spread_thresholds as spread_sync

    SPREAD_THRESHOLD_MAPPING = dict(spread_sync.SPREAD_THRESHOLD_MAPPING)
    SPREAD_THRESHOLD_ORDER = list(spread_sync.THRESHOLD_ORDER)
except Exception:
    SPREAD_THRESHOLD_MAPPING = {}
    SPREAD_THRESHOLD_ORDER = []

INDEX_HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>FR 配置中心</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #0b1221;
      --panel: #111a33;
      --panel-2: #0f172a;
      --border: #223259;
      --muted: #94a3b8;
      --text: #e4ecff;
      --accent: #38bdf8;
      --accent-2: #2563eb;
      --danger: #ef4444;
      --ok: #34d399;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      padding: 18px 20px;
      background: linear-gradient(180deg, #0f1830, #0b1221);
      border-bottom: 1px solid var(--border);
    }
    h1 { margin: 0 0 12px 0; font-size: 20px; }
    main { padding: 20px; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 12px;
      padding: 16px;
      box-shadow: 0 10px 28px rgba(0,0,0,0.35);
      margin-bottom: 18px;
    }
    .toolbar {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: center;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 180px;
    }
    label { font-size: 12px; color: var(--muted); }
    input, select, button, textarea {
      padding: 8px 10px;
      border-radius: 8px;
      border: 1px solid #2f4066;
      background: #0f1c3b;
      color: var(--text);
      font-family: inherit;
    }
    input.mono, textarea.mono { font-family: Menlo, Consolas, monospace; }
    textarea { width: 100%; min-height: 160px; resize: vertical; }
    button {
      cursor: pointer;
      border-color: var(--accent-2);
      background: linear-gradient(90deg, var(--accent-2), var(--accent));
      color: white;
      font-weight: 600;
    }
    button.secondary {
      background: #16213c;
      border-color: #31446d;
      color: #cbd5f5;
    }
    button.ghost {
      background: transparent;
      border-color: #2f4066;
      color: #cbd5f5;
    }
    button:disabled { opacity: 0.6; cursor: not-allowed; }
    .section-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      margin-bottom: 12px;
    }
    .section-header h2 { margin: 0; font-size: 16px; }
    .actions { display: flex; flex-wrap: wrap; gap: 8px; }
    .status { font-size: 12px; color: var(--muted); min-height: 16px; }
    .status.ok { color: var(--ok); }
    .status.err { color: var(--danger); }
    .hint { color: var(--muted); font-size: 12px; }
    .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; }
    .kv-table { display: grid; grid-template-columns: 220px 1fr 1.5fr; gap: 8px; }
    .kv-row { display: contents; }
    .kv-key {
      padding: 8px 10px;
      background: #0c142b;
      border: 1px solid #1b294c;
      border-radius: 6px;
      font-family: Menlo, Consolas, monospace;
      font-size: 12px;
    }
    .kv-input input { width: 100%; }
    .kv-desc { font-size: 12px; color: var(--muted); padding: 8px 4px; }
    .subnav { display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 16px; }
    .subnav a {
      color: #cbd5f5;
      text-decoration: none;
      padding: 6px 10px;
      border: 1px solid #26375b;
      border-radius: 999px;
      font-size: 12px;
    }
    .badge { font-size: 12px; color: var(--muted); }
    .inline { display: inline-flex; align-items: center; gap: 8px; }
    @media (max-width: 960px) {
      .kv-table { grid-template-columns: 1fr; }
      .kv-key { border-radius: 8px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>FR 配置中心</h1>
    <div class="toolbar">
      <div class="field">
        <label for="exchange">Exchange</label>
        <select id="exchange"></select>
      </div>
      <div class="field">
        <label for="open-venue">Open Venue</label>
        <input id="open-venue" />
      </div>
      <div class="field">
        <label for="hedge-venue">Hedge Venue</label>
        <input id="hedge-venue" />
      </div>
      <div class="field">
        <label>Key Suffix</label>
        <div class="inline"><span id="key-suffix" class="badge">-</span></div>
      </div>
      <button id="reload-all" class="ghost" title="批量读取">读取全部</button>
    </div>
  </header>

  <main>
    <div class="subnav">
      <a href="#symbol-lists">Symbol Lists</a>
      <a href="#strategy-params">Strategy Params</a>
      <a href="#risk-params">Risk Params</a>
      <a href="#funding-thresholds">Funding Thresholds</a>
      <a href="#rolling-params">Rolling Params</a>
      <a href="#spread-thresholds">Spread Thresholds</a>
    </div>

    <section id="symbol-lists" class="panel">
      <div class="section-header">
        <h2>Symbol Lists</h2>
        <div class="actions">
          <button id="sym-load" class="secondary">读取</button>
          <button id="sym-save">保存</button>
          <button id="sym-default" class="ghost">默认列表</button>
        </div>
      </div>
      <div class="grid-2">
        <div>
          <h3>平仓列表 <span class="hint">fr_dump_symbols</span></h3>
          <textarea id="sym-dump" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
        <div>
          <h3>正套建仓 <span class="hint">fr_fwd_trade_symbols</span></h3>
          <textarea id="sym-fwd" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
        <div>
          <h3>反套建仓 <span class="hint">fr_bwd_trade_symbols</span></h3>
          <textarea id="sym-bwd" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
        <div>
          <div class="hint">说明：支持逗号/空格/换行分隔；不拆分符号内的 '-' 或 '_'。</div>
        </div>
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
      <div id="strategy-table" class="kv-table"></div>
      <div id="strategy-status" class="status"></div>
    </section>

    <section id="risk-params" class="panel">
      <div class="section-header">
        <h2>Risk Params</h2>
        <div class="actions">
          <button id="risk-load" class="secondary">读取</button>
          <button id="risk-save">保存</button>
          <button id="risk-default" class="ghost">默认</button>
        </div>
      </div>
      <div id="risk-table" class="kv-table"></div>
      <div id="risk-status" class="status"></div>
    </section>

    <section id="funding-thresholds" class="panel">
      <div class="section-header">
        <h2>Funding Rate Thresholds</h2>
        <div class="actions">
          <button id="funding-load" class="secondary">读取</button>
          <button id="funding-save">保存</button>
          <button id="funding-default" class="ghost">默认</button>
        </div>
      </div>
      <div id="funding-table" class="kv-table"></div>
      <div id="funding-status" class="status"></div>
    </section>

    <section id="rolling-params" class="panel">
      <div class="section-header">
        <h2>Rolling Metrics Params</h2>
        <div class="actions">
          <button id="rolling-load" class="secondary">读取</button>
          <button id="rolling-save">保存</button>
          <button id="rolling-default" class="ghost">默认</button>
        </div>
      </div>
      <div class="grid-2">
        <div>
          <label for="rolling-max">MAX_LENGTH</label>
          <input id="rolling-max" class="mono" />
        </div>
        <div>
          <label for="rolling-refresh">refresh_sec</label>
          <input id="rolling-refresh" class="mono" />
        </div>
        <div>
          <label for="rolling-reload">reload_param_sec</label>
          <input id="rolling-reload" class="mono" />
        </div>
        <div>
          <label for="rolling-output">output_hash_key</label>
          <input id="rolling-output" class="mono" />
        </div>
      </div>
      <div style="margin-top: 12px;">
        <label for="rolling-factors">factors (JSON)</label>
        <textarea id="rolling-factors" class="mono"></textarea>
      </div>
      <div id="rolling-status" class="status"></div>
    </section>

    <section id="spread-thresholds" class="panel">
      <div class="section-header">
        <h2>Spread Threshold Mapping</h2>
        <div class="actions">
          <button id="spread-config-load" class="secondary">读取配置</button>
          <button id="spread-config-save">保存配置</button>
          <button id="spread-sync" class="ghost">同步阈值</button>
        </div>
      </div>
      <div class="toolbar" style="margin-bottom: 10px;">
        <div class="field">
          <label for="spread-symbol">Symbol (可选)</label>
          <input id="spread-symbol" placeholder="BTCUSDT" />
        </div>
        <div class="hint">格式: bidask_10 / askbid_90 / spread_15</div>
      </div>
      <div id="spread-table" class="kv-table"></div>
      <div id="spread-status" class="status"></div>
    </section>
  </main>

  <script>
    const BOOTSTRAP = __BOOTSTRAP__;

    const exchangeSelect = document.getElementById('exchange');
    const openVenueInput = document.getElementById('open-venue');
    const hedgeVenueInput = document.getElementById('hedge-venue');
    const keySuffixEl = document.getElementById('key-suffix');

    function setStatus(id, msg, ok = true) {
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = msg;
      el.className = 'status ' + (ok ? 'ok' : 'err');
    }

    function normalizeExchange(ex) {
      const raw = (ex || '').toLowerCase();
      return raw === 'okx' ? 'okex' : raw;
    }

    function fillExchanges() {
      exchangeSelect.innerHTML = '';
      BOOTSTRAP.exchanges.forEach(ex => {
        const opt = document.createElement('option');
        opt.value = ex;
        opt.textContent = ex;
        if (ex === BOOTSTRAP.default_exchange) opt.selected = true;
        exchangeSelect.appendChild(opt);
      });
    }

    function applyExchangeDefaults() {
      const ex = normalizeExchange(exchangeSelect.value);
      const pair = BOOTSTRAP.exchange_defaults[ex] || [];
      if (!openVenueInput.value) openVenueInput.value = pair[0] || '';
      if (!hedgeVenueInput.value) hedgeVenueInput.value = pair[1] || '';
      refreshKeySuffix();
    }

    function refreshKeySuffix() {
      const open = (openVenueInput.value || '').trim().toLowerCase();
      const hedge = (hedgeVenueInput.value || '').trim().toLowerCase();
      keySuffixEl.textContent = open && hedge ? `${open}_${hedge}` : '-';
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

    function queryParams(extra = {}) {
      const params = new URLSearchParams();
      params.set('exchange', normalizeExchange(exchangeSelect.value));
      if (openVenueInput.value) params.set('open_venue', openVenueInput.value.trim());
      if (hedgeVenueInput.value) params.set('hedge_venue', hedgeVenueInput.value.trim());
      Object.keys(extra).forEach(key => {
        if (extra[key]) params.set(key, extra[key]);
      });
      return params.toString();
    }

    function apiUrl(path) {
      const base = window.location.pathname.endsWith('/') ? window.location.pathname : window.location.pathname + '/';
      const clean = path.replace(/^\\//, '');
      return `${base}api/${clean}`;
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
      ordered.forEach(key => {
        const row = document.createElement('div');
        row.className = 'kv-row';

        const keyCell = document.createElement('div');
        keyCell.className = 'kv-key';
        keyCell.textContent = key;

        const inputCell = document.createElement('div');
        inputCell.className = 'kv-input';
        const input = document.createElement('input');
        input.dataset.key = key;
        input.className = 'mono';
        input.value = values[key] ?? defaults[key] ?? '';
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
      container.querySelectorAll('input[data-key]').forEach(input => {
        values[input.dataset.key] = input.value.trim();
      });
      return values;
    }

    async function loadSymbolLists() {
      setStatus('sym-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('symbol-lists')}?${queryParams()}`);
        document.getElementById('sym-dump').value = fromList(data.dump_symbols || []);
        document.getElementById('sym-fwd').value = fromList(data.fwd_trade_symbols || []);
        document.getElementById('sym-bwd').value = fromList(data.bwd_trade_symbols || []);
        setStatus('sym-status', '读取完成');
      } catch (err) {
        setStatus('sym-status', `读取失败: ${err}`, false);
      }
    }

    async function saveSymbolLists() {
      setStatus('sym-status', '保存中...');
      try {
        const payload = {
          exchange: normalizeExchange(exchangeSelect.value),
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          dump_symbols: toList(document.getElementById('sym-dump').value),
          fwd_trade_symbols: toList(document.getElementById('sym-fwd').value),
          bwd_trade_symbols: toList(document.getElementById('sym-bwd').value),
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

    function applySymbolDefaults() {
      const ex = normalizeExchange(exchangeSelect.value);
      const defaults = (BOOTSTRAP.defaults.symbol_lists || {})[ex] || {};
      document.getElementById('sym-dump').value = fromList(defaults.dump_symbols || []);
      document.getElementById('sym-fwd').value = fromList(defaults.fwd_trade_symbols || []);
      document.getElementById('sym-bwd').value = fromList(defaults.bwd_trade_symbols || []);
      setStatus('sym-status', '已载入默认列表');
    }

    async function loadRiskParams() {
      setStatus('risk-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('risk-params')}?${queryParams()}`);
        buildParamRows('risk-table', BOOTSTRAP.defaults.risk_params || {}, BOOTSTRAP.comments.risk_params || {}, BOOTSTRAP.order.risk || [], data.values || {});
        setStatus('risk-status', `读取完成: ${data.key || '-'}`);
      } catch (err) {
        setStatus('risk-status', `读取失败: ${err}`, false);
      }
    }

    async function saveRiskParams() {
      setStatus('risk-status', '保存中...');
      try {
        const payload = {
          exchange: normalizeExchange(exchangeSelect.value),
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          values: collectParamValues('risk-table'),
        };
        const data = await fetchJson(apiUrl('risk-params'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus('risk-status', `保存成功: ${data.key || '-'} (fields=${data.count || 0})`);
        await loadRiskParams();
      } catch (err) {
        setStatus('risk-status', `保存失败: ${err}`, false);
      }
    }

    function applyRiskDefaults() {
      buildParamRows('risk-table', BOOTSTRAP.defaults.risk_params || {}, BOOTSTRAP.comments.risk_params || {}, BOOTSTRAP.order.risk || [], {});
      setStatus('risk-status', '已载入默认参数');
    }

    async function loadStrategyParams() {
      setStatus('strategy-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('strategy-params')}?${queryParams()}`);
        buildParamRows('strategy-table', BOOTSTRAP.defaults.strategy_params || {}, BOOTSTRAP.comments.strategy_params || {}, BOOTSTRAP.order.strategy || [], data.values || {});
        setStatus('strategy-status', '读取完成');
      } catch (err) {
        setStatus('strategy-status', `读取失败: ${err}`, false);
      }
    }

    async function saveStrategyParams() {
      setStatus('strategy-status', '保存中...');
      try {
        const payload = {
          exchange: normalizeExchange(exchangeSelect.value),
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          values: collectParamValues('strategy-table'),
        };
        await fetchJson(apiUrl('strategy-params'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus('strategy-status', '保存成功');
      } catch (err) {
        setStatus('strategy-status', `保存失败: ${err}`, false);
      }
    }

    function applyStrategyDefaults() {
      buildParamRows('strategy-table', BOOTSTRAP.defaults.strategy_params || {}, BOOTSTRAP.comments.strategy_params || {}, BOOTSTRAP.order.strategy || [], {});
      setStatus('strategy-status', '已载入默认参数');
    }

    async function loadFundingThresholds() {
      setStatus('funding-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('funding-thresholds')}?${queryParams()}`);
        buildParamRows('funding-table', BOOTSTRAP.defaults.funding_thresholds || {}, BOOTSTRAP.comments.funding_thresholds || {}, BOOTSTRAP.order.funding_thresholds || [], data.values || {});
        setStatus('funding-status', '读取完成');
      } catch (err) {
        setStatus('funding-status', `读取失败: ${err}`, false);
      }
    }

    async function saveFundingThresholds() {
      setStatus('funding-status', '保存中...');
      try {
        const payload = {
          exchange: normalizeExchange(exchangeSelect.value),
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          values: collectParamValues('funding-table'),
        };
        await fetchJson(apiUrl('funding-thresholds'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus('funding-status', '保存成功');
      } catch (err) {
        setStatus('funding-status', `保存失败: ${err}`, false);
      }
    }

    function applyFundingDefaults() {
      buildParamRows('funding-table', BOOTSTRAP.defaults.funding_thresholds || {}, BOOTSTRAP.comments.funding_thresholds || {}, BOOTSTRAP.order.funding_thresholds || [], {});
      setStatus('funding-status', '已载入默认参数');
    }

    function applyRollingDefaults() {
      const open = openVenueInput.value.trim().toLowerCase();
      const hedge = hedgeVenueInput.value.trim().toLowerCase();
      const defaults = BOOTSTRAP.defaults.rolling_params || {};
      document.getElementById('rolling-max').value = defaults.MAX_LENGTH ?? '';
      document.getElementById('rolling-refresh').value = defaults.refresh_sec ?? '';
      document.getElementById('rolling-reload').value = defaults.reload_param_sec ?? '';
      document.getElementById('rolling-output').value = `rolling_metrics_thresholds_${open}_${hedge}`;
      document.getElementById('rolling-factors').value = JSON.stringify(defaults.factors || {}, null, 2);
      setStatus('rolling-status', '已载入默认参数');
    }

    async function loadRollingParams() {
      setStatus('rolling-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('rolling-params')}?${queryParams()}`);
        const values = data.values || {};
        document.getElementById('rolling-max').value = values.MAX_LENGTH ?? '';
        document.getElementById('rolling-refresh').value = values.refresh_sec ?? '';
        document.getElementById('rolling-reload').value = values.reload_param_sec ?? '';
        document.getElementById('rolling-output').value = values.output_hash_key ?? '';
        document.getElementById('rolling-factors').value = JSON.stringify(values.factors || {}, null, 2);
        setStatus('rolling-status', '读取完成');
      } catch (err) {
        setStatus('rolling-status', `读取失败: ${err}`, false);
      }
    }

    async function saveRollingParams() {
      setStatus('rolling-status', '保存中...');
      try {
        const factorsText = document.getElementById('rolling-factors').value || '{}';
        let factors;
        try {
          factors = JSON.parse(factorsText);
        } catch (err) {
          throw new Error(`factors JSON 无法解析: ${err}`);
        }
        const payload = {
          exchange: normalizeExchange(exchangeSelect.value),
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          values: {
            MAX_LENGTH: document.getElementById('rolling-max').value.trim(),
            refresh_sec: document.getElementById('rolling-refresh').value.trim(),
            reload_param_sec: document.getElementById('rolling-reload').value.trim(),
            output_hash_key: document.getElementById('rolling-output').value.trim(),
            factors: factors,
          }
        };
        await fetchJson(apiUrl('rolling-params'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus('rolling-status', '保存成功');
      } catch (err) {
        setStatus('rolling-status', `保存失败: ${err}`, false);
      }
    }

    async function loadSpreadMapping() {
      setStatus('spread-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('spread-thresholds')}?${queryParams()}`);
        buildParamRows('spread-table', BOOTSTRAP.defaults.spread_mapping || {}, {}, BOOTSTRAP.order.spread_mapping || [], data.values || {});
        setStatus('spread-status', '读取完成');
      } catch (err) {
        setStatus('spread-status', `读取失败: ${err}`, false);
      }
    }

    async function saveSpreadMapping() {
      setStatus('spread-status', '保存中...');
      try {
        const payload = {
          exchange: normalizeExchange(exchangeSelect.value),
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          values: collectParamValues('spread-table'),
        };
        const data = await fetchJson(apiUrl('spread-thresholds'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus('spread-status', `保存成功 (${data.count || 0} 字段)`);
      } catch (err) {
        setStatus('spread-status', `保存失败: ${err}`, false);
      }
    }

    async function syncSpreadThresholds() {
      setStatus('spread-status', '同步中...');
      try {
        const symbol = document.getElementById('spread-symbol').value.trim();
        const payload = {
          exchange: normalizeExchange(exchangeSelect.value),
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          symbol,
          mapping: collectParamValues('spread-table'),
        };
        const data = await fetchJson(apiUrl('spread-thresholds/sync'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        const changed = data.changed != null ? `, changed=${data.changed}` : '';
        setStatus('spread-status', `同步完成: ${data.written || 0} 字段${changed}`);
      } catch (err) {
        setStatus('spread-status', `同步失败: ${err}`, false);
      }
    }

    async function reloadAll() {
      await loadSymbolLists();
      await loadStrategyParams();
      await loadRiskParams();
      await loadFundingThresholds();
      await loadRollingParams();
      await loadSpreadMapping();
    }

    fillExchanges();
    applyExchangeDefaults();
    buildParamRows('risk-table', BOOTSTRAP.defaults.risk_params || {}, BOOTSTRAP.comments.risk_params || {}, BOOTSTRAP.order.risk || [], {});
    buildParamRows('strategy-table', BOOTSTRAP.defaults.strategy_params || {}, BOOTSTRAP.comments.strategy_params || {}, BOOTSTRAP.order.strategy || [], {});
    buildParamRows('funding-table', BOOTSTRAP.defaults.funding_thresholds || {}, BOOTSTRAP.comments.funding_thresholds || {}, BOOTSTRAP.order.funding_thresholds || [], {});
    buildParamRows('spread-table', BOOTSTRAP.defaults.spread_mapping || {}, {}, BOOTSTRAP.order.spread_mapping || [], {});

    exchangeSelect.addEventListener('change', () => {
      openVenueInput.value = '';
      hedgeVenueInput.value = '';
      applyExchangeDefaults();
      reloadAll();
    });
    openVenueInput.addEventListener('input', refreshKeySuffix);
    hedgeVenueInput.addEventListener('input', refreshKeySuffix);

    document.getElementById('sym-load').addEventListener('click', loadSymbolLists);
    document.getElementById('sym-save').addEventListener('click', saveSymbolLists);
    document.getElementById('sym-default').addEventListener('click', applySymbolDefaults);

    document.getElementById('risk-load').addEventListener('click', loadRiskParams);
    document.getElementById('risk-save').addEventListener('click', saveRiskParams);
    document.getElementById('risk-default').addEventListener('click', applyRiskDefaults);

    document.getElementById('strategy-load').addEventListener('click', loadStrategyParams);
    document.getElementById('strategy-save').addEventListener('click', saveStrategyParams);
    document.getElementById('strategy-default').addEventListener('click', applyStrategyDefaults);

    document.getElementById('funding-load').addEventListener('click', loadFundingThresholds);
    document.getElementById('funding-save').addEventListener('click', saveFundingThresholds);
    document.getElementById('funding-default').addEventListener('click', applyFundingDefaults);

    document.getElementById('rolling-load').addEventListener('click', loadRollingParams);
    document.getElementById('rolling-save').addEventListener('click', saveRollingParams);
    document.getElementById('rolling-default').addEventListener('click', applyRollingDefaults);

    document.getElementById('spread-config-load').addEventListener('click', loadSpreadMapping);
    document.getElementById('spread-config-save').addEventListener('click', saveSpreadMapping);
    document.getElementById('spread-sync').addEventListener('click', syncSpreadThresholds);

    document.getElementById('reload-all').addEventListener('click', reloadAll);

    reloadAll();
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
    return parser.parse_args()


def normalize_exchange(exchange: str) -> str:
    ex = exchange.strip().lower()
    return "okex" if ex == "okx" else ex


def normalize_symbol_list(value: Any) -> List[str]:
    items: List[str] = []
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, str):
        raw_items = re.split(r"[\s,]+", value)
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


def normalize_symbol_list_for_exchange(exchange: str, value: Any) -> List[str]:
    """Normalize symbol list with exchange-specific rules.

    For OKEx: uppercase, drop '-'/'_', and strip trailing 'SWAP' to align with rolling_metrics keys.
    Other exchanges keep the basic normalization only.
    """
    base = normalize_symbol_list(value)
    if normalize_exchange(exchange) != "okex":
        return base

    items: List[str] = []
    seen = set()
    for sym in base:
        norm = sym.replace("-", "").replace("_", "")
        if norm.endswith("SWAP"):
            norm = norm[:-4]
        if not norm:
            continue
        if norm not in seen:
            seen.add(norm)
            items.append(norm)
    return items


def summarize_symbol_payload(value: Any) -> Tuple[int, List[str]]:
    """Return (count, sample) for raw symbol payload."""
    raw_items: List[str] = []
    if isinstance(value, list):
        raw_items = [item for item in value if isinstance(item, str)]
    elif isinstance(value, str):
        raw_items = [s for s in re.split(r"[\s,]+", value) if s]

    cleaned: List[str] = []
    for item in raw_items:
        sym = item.strip()
        if sym:
            cleaned.append(sym)
    return len(cleaned), cleaned[:5]


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


def decode_redis_value(value: Any) -> str:
    return value.decode("utf-8", "ignore") if isinstance(value, (bytes, bytearray)) else str(value)


def read_hash(rds, key: str) -> Dict[str, str]:
    raw = rds.hgetall(key)
    return {decode_redis_value(k): decode_redis_value(v) for k, v in (raw or {}).items()}


def write_hash(rds, key: str, values: Dict[str, Any]) -> int:
    payload = {str(k): str(v) for k, v in values.items()}
    if not payload:
        return 0
    rds.hset(key, mapping=payload)
    return len(payload)


def make_key_suffix(open_venue: str, hedge_venue: str) -> str:
    return f"{open_venue.strip().lower()}_{hedge_venue.strip().lower()}"


def resolve_venues(
    exchange: str, open_venue: Optional[str], hedge_venue: Optional[str]
) -> Tuple[str, str, str, str]:
    ex = normalize_exchange(exchange)
    if ex not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {ex}")

    if open_venue or hedge_venue:
        if not open_venue or not hedge_venue:
            raise ValueError("open_venue/hedge_venue 需要成对提供")
        open_v = open_venue.strip().lower()
        hedge_v = hedge_venue.strip().lower()
    else:
        open_v, hedge_v = EXCHANGE_DEFAULTS[ex]

    key_suffix = make_key_suffix(open_v, hedge_v)
    return ex, open_v, hedge_v, key_suffix


def get_symbol_defaults() -> Dict[str, Dict[str, List[str]]]:
    defaults: Dict[str, Dict[str, List[str]]] = {}
    if not SYMBOL_DEFAULTS_SRC:
        return defaults
    for ex in SUPPORTED_EXCHANGES:
        fwd, bwd, _ = SYMBOL_DEFAULTS_SRC.resolve_symbol_lists(ex)
        dump_symbols = list(dict.fromkeys(fwd + bwd))
        defaults[ex] = {
            "dump_symbols": dump_symbols,
            "fwd_trade_symbols": fwd,
            "bwd_trade_symbols": bwd,
        }
    return defaults


def parse_rolling_params(values: Dict[str, str]) -> Dict[str, Any]:
    parsed: Dict[str, Any] = {}
    for key, value in values.items():
        if key == "factors":
            try:
                parsed[key] = json.loads(value)
            except Exception:
                parsed[key] = {}
            continue
        if key in ("MAX_LENGTH", "refresh_sec", "reload_param_sec"):
            try:
                parsed[key] = int(float(value))
            except Exception:
                parsed[key] = value
            continue
        parsed[key] = value
    return parsed


def normalize_rolling_factors_for_save(factors: Any) -> Dict[str, Any]:
    if not isinstance(factors, dict):
        return {}
    normalized = json.loads(json.dumps(factors, ensure_ascii=False))
    for factor_name in ("open_vol", "hedge_vol"):
        factor_cfg = normalized.get(factor_name)
        if not isinstance(factor_cfg, dict):
            continue
        quantiles = factor_cfg.get("quantiles")
        if not isinstance(quantiles, list):
            continue
        while len(quantiles) > 8:
            quantiles.pop(0)
    return normalized


def serialize_rolling_params(values: Dict[str, Any]) -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for key, value in values.items():
        if key == "factors":
            payload[key] = json.dumps(
                normalize_rolling_factors_for_save(value),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        else:
            payload[key] = str(value)
    return payload


def spread_mapping_key(open_venue: str, hedge_venue: str) -> str:
    return f"fr_spread_threshold_mapping_{open_venue}_{hedge_venue}"


def normalize_spread_mapping(values: Dict[str, Any]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for key, value in (values or {}).items():
        k = str(key).strip()
        v = str(value).strip()
        if not k or not v:
            continue
        mapping[k] = v
    return mapping


def read_spread_mapping(rds, open_venue: str, hedge_venue: str) -> Dict[str, str]:
    key = spread_mapping_key(open_venue, hedge_venue)
    values = read_hash(rds, key)
    if values:
        return normalize_spread_mapping(values)
    return normalize_spread_mapping(SPREAD_THRESHOLD_MAPPING)


def generate_threshold_fields(
    symbol_data: Dict[str, Dict],
    mapping: Dict[str, str],
) -> tuple[Dict[str, str], set[str]]:
    all_fields: Dict[str, str] = {}
    skipped: set[str] = set()
    for symbol, obj in symbol_data.items():
        symbol_fields: Dict[str, str] = {}
        for suffix, field_ref in mapping.items():
            value = spread_sync.extract_quantile_value(obj, field_ref)
            if value is None:
                skipped.add(symbol)
                symbol_fields = {}
                break
            field_key = f"{symbol}_{suffix}"
            symbol_fields[field_key] = f"{value:.8f}".rstrip("0").rstrip(".")
        if symbol_fields:
            all_fields.update(symbol_fields)
    return all_fields, skipped


def sync_spread_thresholds(
    rds,
    open_venue: str,
    hedge_venue: str,
    key_suffix: str,
    mapping: Optional[Dict[str, str]] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    if spread_sync is None:
        raise RuntimeError("sync_fr_spread_thresholds.py not available")

    dump_keys = [f"fr_dump_symbols:{key_suffix}"]
    fwd_keys = [f"fr_fwd_trade_symbols:{key_suffix}"]
    bwd_keys = [f"fr_bwd_trade_symbols:{key_suffix}"]
    rolling_key = f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"
    write_key = f"fr_spread_thresholds_{open_venue}_{hedge_venue}"

    mapping = normalize_spread_mapping(mapping or {})
    if not mapping:
        mapping = normalize_spread_mapping(SPREAD_THRESHOLD_MAPPING)
    if not mapping:
        raise RuntimeError("spread mapping is empty")

    target_symbols = spread_sync._get_target_symbols(rds, dump_keys, fwd_keys, bwd_keys, symbol)
    rolling_data = spread_sync.read_rolling_metrics(rds, rolling_key)
    symbol_data, missing_symbols = spread_sync._find_symbol_data(
        rolling_data, target_symbols
    )
    all_fields, skipped_symbols = generate_threshold_fields(symbol_data, mapping)

    changed = 0
    if all_fields:
        res = rds.hset(write_key, mapping=all_fields)
        changed = int(res) if res is not None else 0

    return {
        "written": len(all_fields),
        "changed": changed,
        "missing_symbols": sorted(list(missing_symbols)),
        "skipped_symbols": sorted(list(skipped_symbols)),
        "write_key": write_key,
        "rolling_key": rolling_key,
        "mapping_key": spread_mapping_key(open_venue, hedge_venue),
    }


def render_index_html(default_exchange: str) -> str:
    bootstrap = {
        "exchanges": SUPPORTED_EXCHANGES,
        "default_exchange": default_exchange,
        "exchange_defaults": EXCHANGE_DEFAULTS,
        "defaults": {
            "symbol_lists": get_symbol_defaults(),
            "risk_params": DEFAULT_RISK_PARAMS,
            "strategy_params": DEFAULT_STRATEGY_PARAMS,
            "funding_thresholds": DEFAULT_FUNDING_THRESHOLDS,
            "rolling_params": DEFAULT_ROLLING_PARAMS,
            "spread_mapping": SPREAD_THRESHOLD_MAPPING,
        },
        "comments": {
            "risk_params": RISK_PARAM_COMMENTS,
            "strategy_params": STRATEGY_PARAM_COMMENTS,
            "funding_thresholds": FUNDING_THRESHOLD_COMMENTS,
        },
        "order": {
            "risk": list(DEFAULT_RISK_PARAMS.keys()),
            "strategy": list(DEFAULT_STRATEGY_PARAMS.keys()),
            "funding_thresholds": FUNDING_THRESHOLD_ORDER,
            "spread_mapping": SPREAD_THRESHOLD_ORDER,
        },
    }

    html = INDEX_HTML_TEMPLATE.replace("__BOOTSTRAP__", json.dumps(bootstrap, ensure_ascii=False))
    return html


@dataclass
class ServerContext:
    redis_client: Any
    default_exchange: str


class FrConfigServer(ThreadingHTTPServer):
    def __init__(self, server_address, RequestHandlerClass, context: ServerContext):
        super().__init__(server_address, RequestHandlerClass)
        self.context = context


class RequestHandler(BaseHTTPRequestHandler):
    server: FrConfigServer

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

    def _resolve_request_context(self, params: Dict[str, List[str]]) -> Tuple[str, str, str, str]:
        exchange = normalize_exchange(
            (params.get("exchange") or [self.server.context.default_exchange])[0]
        )
        open_venue = (params.get("open_venue") or [None])[0]
        hedge_venue = (params.get("hedge_venue") or [None])[0]
        return resolve_venues(exchange, open_venue, hedge_venue)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            html = render_index_html(self.server.context.default_exchange)
            self._send_html(html)
            return

        params = parse_qs(parsed.query)

        if parsed.path == "/api/symbol-lists":
            try:
                exchange, open_venue, hedge_venue, key_suffix = self._resolve_request_context(
                    params
                )
            except Exception as exc:
                self._send_error(400, str(exc))
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

        if parsed.path == "/api/risk-params":
            try:
                exchange, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = build_risk_params_key(open_venue, hedge_venue)
            print(
                f"[risk-params][GET] exchange={exchange} open={open_venue} hedge={hedge_venue} key={key}"
            )
            sys.stdout.flush()
            values = read_hash(self.server.context.redis_client, key)
            if not values:
                self._send_error(404, f"risk params not found: {key}")
                return
            self._send_json(200, {"key": key, "values": values})
            return

        if parsed.path == "/api/strategy-params":
            try:
                _, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = f"fr_strategy_params_{open_venue}_{hedge_venue}"
            values = read_hash(self.server.context.redis_client, key)
            self._send_json(200, {"key": key, "values": values})
            return

        if parsed.path == "/api/funding-thresholds":
            try:
                _, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = f"funding_rate_thresholds_{open_venue}_{hedge_venue}"
            values = read_hash(self.server.context.redis_client, key)
            self._send_json(200, {"key": key, "values": values})
            return

        if parsed.path == "/api/rolling-params":
            try:
                _, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = f"rolling_metrics_params_{open_venue}_{hedge_venue}"
            values = read_hash(self.server.context.redis_client, key)
            parsed_values = parse_rolling_params(values)
            if "output_hash_key" not in parsed_values:
                parsed_values["output_hash_key"] = (
                    f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"
                )
            self._send_json(200, {"key": key, "values": parsed_values})
            return

        if parsed.path == "/api/spread-thresholds":
            try:
                _, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = spread_mapping_key(open_venue, hedge_venue)
            values = read_hash(self.server.context.redis_client, key)
            if not values:
                values = normalize_spread_mapping(SPREAD_THRESHOLD_MAPPING)
            self._send_json(200, {"key": key, "count": len(values), "values": values})
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
            exchange = normalize_exchange(
                str(payload.get("exchange") or self.server.context.default_exchange)
            )
            open_venue = payload.get("open_venue")
            hedge_venue = payload.get("hedge_venue")
            try:
                _, open_v, hedge_v, key_suffix = resolve_venues(
                    exchange, open_venue, hedge_venue
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            raw_dump = payload.get("dump_symbols") or []
            raw_fwd = payload.get("fwd_trade_symbols") or []
            raw_bwd = payload.get("bwd_trade_symbols") or []
            dump_symbols = normalize_symbol_list_for_exchange(
                exchange, raw_dump
            )
            fwd_symbols = normalize_symbol_list_for_exchange(
                exchange, raw_fwd
            )
            bwd_symbols = normalize_symbol_list_for_exchange(
                exchange, raw_bwd
            )
            raw_dump_len, raw_dump_sample = summarize_symbol_payload(raw_dump)
            raw_fwd_len, raw_fwd_sample = summarize_symbol_payload(raw_fwd)
            raw_bwd_len, raw_bwd_sample = summarize_symbol_payload(raw_bwd)
            print(
                "[symbol-lists] exchange={} open={} hedge={} key_suffix={}".format(
                    exchange, open_v, hedge_v, key_suffix
                )
            )
            print(
                "[symbol-lists] dump raw={} norm={} sample_raw={} sample_norm={}".format(
                    raw_dump_len, len(dump_symbols), raw_dump_sample, dump_symbols[:5]
                )
            )
            print(
                "[symbol-lists] fwd  raw={} norm={} sample_raw={} sample_norm={}".format(
                    raw_fwd_len, len(fwd_symbols), raw_fwd_sample, fwd_symbols[:5]
                )
            )
            print(
                "[symbol-lists] bwd  raw={} norm={} sample_raw={} sample_norm={}".format(
                    raw_bwd_len, len(bwd_symbols), raw_bwd_sample, bwd_symbols[:5]
                )
            )
            sys.stdout.flush()

            rds = self.server.context.redis_client
            try:
                rds.set(
                    f"fr_dump_symbols:{key_suffix}",
                    json.dumps(dump_symbols, ensure_ascii=False),
                )
                rds.set(
                    f"fr_fwd_trade_symbols:{key_suffix}",
                    json.dumps(fwd_symbols, ensure_ascii=False),
                )
                rds.set(
                    f"fr_bwd_trade_symbols:{key_suffix}",
                    json.dumps(bwd_symbols, ensure_ascii=False),
                )
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
            return

        if parsed.path == "/api/risk-params":
            exchange = normalize_exchange(
                str(payload.get("exchange") or self.server.context.default_exchange)
            )
            try:
                _, open_v, hedge_v, _ = resolve_venues(
                    exchange, payload.get("open_venue"), payload.get("hedge_venue")
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            key = build_risk_params_key(open_v, hedge_v)
            written = write_hash(self.server.context.redis_client, key, values)
            print(
                f"[risk-params][POST] exchange={exchange} open={open_v} hedge={hedge_v} "
                f"key={key} fields={len(values)}"
            )
            sys.stdout.flush()
            self._send_json(200, {"key": key, "count": written})
            return

        if parsed.path == "/api/strategy-params":
            exchange = normalize_exchange(
                str(payload.get("exchange") or self.server.context.default_exchange)
            )
            try:
                _, open_v, hedge_v, _ = resolve_venues(
                    exchange, payload.get("open_venue"), payload.get("hedge_venue")
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            key = f"fr_strategy_params_{open_v}_{hedge_v}"
            written = write_hash(self.server.context.redis_client, key, values)
            self._send_json(200, {"key": key, "count": written})
            return

        if parsed.path == "/api/funding-thresholds":
            exchange = normalize_exchange(
                str(payload.get("exchange") or self.server.context.default_exchange)
            )
            try:
                _, open_v, hedge_v, _ = resolve_venues(
                    exchange, payload.get("open_venue"), payload.get("hedge_venue")
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            key = f"funding_rate_thresholds_{open_v}_{hedge_v}"
            written = write_hash(self.server.context.redis_client, key, values)
            self._send_json(200, {"key": key, "count": written})
            return

        if parsed.path == "/api/rolling-params":
            exchange = normalize_exchange(
                str(payload.get("exchange") or self.server.context.default_exchange)
            )
            try:
                _, open_v, hedge_v, _ = resolve_venues(
                    exchange, payload.get("open_venue"), payload.get("hedge_venue")
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            key = f"rolling_metrics_params_{open_v}_{hedge_v}"
            serialized = serialize_rolling_params(values)
            written = write_hash(self.server.context.redis_client, key, serialized)
            self._send_json(200, {"key": key, "count": written})
            return

        if parsed.path == "/api/spread-thresholds":
            exchange = normalize_exchange(
                str(payload.get("exchange") or self.server.context.default_exchange)
            )
            try:
                _, open_v, hedge_v, _ = resolve_venues(
                    exchange, payload.get("open_venue"), payload.get("hedge_venue")
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            mapping = normalize_spread_mapping(values)
            if not mapping:
                self._send_error(400, "mapping is empty")
                return
            key = spread_mapping_key(open_v, hedge_v)
            written = write_hash(self.server.context.redis_client, key, mapping)
            self._send_json(200, {"key": key, "count": written})
            return

        if parsed.path == "/api/spread-thresholds/sync":
            exchange = normalize_exchange(
                str(payload.get("exchange") or self.server.context.default_exchange)
            )
            try:
                _, open_v, hedge_v, key_suffix = resolve_venues(
                    exchange, payload.get("open_venue"), payload.get("hedge_venue")
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            symbol = payload.get("symbol")
            mapping = payload.get("mapping") if isinstance(payload, dict) else None
            try:
                result = sync_spread_thresholds(
                    self.server.context.redis_client,
                    open_v,
                    hedge_v,
                    key_suffix,
                    mapping,
                    symbol,
                )
            except Exception as exc:
                self._send_error(500, f"sync failed: {exc}")
                return
            self._send_json(200, result)
            return

        self._send_error(404, "not found")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请执行: pip install redis", file=sys.stderr)
        return 2

    default_exchange = normalize_exchange(args.default_exchange)
    if default_exchange not in SUPPORTED_EXCHANGES:
        print(
            f"❌ 默认交易所 {default_exchange} 不在支持列表 {SUPPORTED_EXCHANGES}",
            file=sys.stderr,
        )
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    context = ServerContext(redis_client=rds, default_exchange=default_exchange)
    try:
        server = FrConfigServer((args.host, args.port), RequestHandler, context)
    except OSError as exc:
        print(
            f"❌ 无法监听 {args.host}:{args.port}，端口可能被占用: {exc}",
            file=sys.stderr,
        )
        return 2
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
