#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MM 配置服务器（mm_config_server）
--------------------------------
提供页面/接口用于查看和编辑 MM 相关配置：
- symbol list
- strategy params
- amount_u overrides
- max_pos_u overrides
- pre-trade risk params
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
MODEL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._-]*"
VENUE_PREFIX_PATTERN = re.compile(
    r"^([a-z0-9]+-(?:margin|futures|spot|swap|perpetual|perp))(?:$|[-_.])"
)
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
AMOUNT_U_EXAMPLE = {
    "BTCUSDT": 150.0,
    "ETHUSDT": 80.0,
}
MAX_POS_U_EXAMPLE = {
    "BTCUSDT": 200000.0,
    "ETHUSDT": 120000.0,
}
MODEL_SCORE_ROLLING_SCRIPT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "model_score_rolling",
)
if os.path.isdir(MODEL_SCORE_ROLLING_SCRIPT_DIR):
    sys.path.insert(0, MODEL_SCORE_ROLLING_SCRIPT_DIR)

try:
    import sync_mm_symbol_list as mm_symbol_defaults

    DEFAULT_SYMBOLS = list(getattr(mm_symbol_defaults, "SYMBOLS", DEFAULT_SYMBOLS))
except Exception:
    pass

try:
    import sync_mm_strategy_params as mm_strategy_defaults

    DEFAULT_STRATEGY_PARAMS = dict(mm_strategy_defaults.STRATEGY_PARAMS)
    STRATEGY_PARAM_COMMENTS = dict(mm_strategy_defaults.PARAM_COMMENTS)
    STRATEGY_PARAM_ORDER = list(mm_strategy_defaults.PARAM_PRINT_ORDER)
except Exception:
    DEFAULT_STRATEGY_PARAMS = {}
    STRATEGY_PARAM_COMMENTS = {}
    STRATEGY_PARAM_ORDER = []

try:
    import sync_mm_risk_params as mm_risk_defaults

    DEFAULT_RISK_PARAMS = dict(mm_risk_defaults.RISK_PARAMS)
    RISK_PARAM_COMMENTS = dict(mm_risk_defaults.PARAM_COMMENTS)
    RISK_PARAM_ORDER = list(DEFAULT_RISK_PARAMS.keys())
except Exception:
    DEFAULT_RISK_PARAMS = {}
    RISK_PARAM_COMMENTS = {}
    RISK_PARAM_ORDER = []

try:
    import sync_model_score_rolling_params as model_score_defaults

    DEFAULT_MODEL_SCORE_ROLLING_PARAMS = dict(model_score_defaults.DEFAULTS)
except Exception:
    DEFAULT_MODEL_SCORE_ROLLING_PARAMS = {
        "max_length": 150_000,
        "reload_param_sec": 60,
        "rolling_window": 17_800,
        "min_periods": 100,
        "quantiles": [0.9, 0.8, 0.2, 0.1],
    }

MODEL_SCORE_PARAM_COMMENTS = {
    "reload_param_sec": "配置热更新周期(秒)",
    "max_length": "环形缓冲最大长度",
    "rolling_window": "rolling 窗口长度",
    "min_periods": "最小样本数",
    "quantiles": "需要计算的分位点(JSON 数组，如 [0.9,0.8,0.2,0.1])",
}
MODEL_SCORE_PARAM_ORDER = [
    "reload_param_sec",
    "max_length",
    "rolling_window",
    "min_periods",
    "quantiles",
]
INDEX_HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>MM 配置中心</title>
  <style>
    :root {
      color-scheme: light dark;
      --bg: #08131e;
      --panel: #0f2030;
      --panel-2: #11283d;
      --border: #213a53;
      --muted: #8aa6be;
      --text: #edf6ff;
      --accent: #0ea5e9;
      --accent-2: #0284c7;
      --danger: #ef4444;
      --ok: #34d399;
      --warn: #f59e0b;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Space Grotesk", "Segoe UI", sans-serif;
      background:
        radial-gradient(circle at top left, rgba(14,165,233,0.16), transparent 28%),
        radial-gradient(circle at top right, rgba(16,185,129,0.10), transparent 24%),
        var(--bg);
      color: var(--text);
    }
    header {
      padding: 18px 20px;
      background: linear-gradient(180deg, rgba(9,20,32,0.98), rgba(8,19,30,0.92));
      border-bottom: 1px solid var(--border);
      position: sticky;
      top: 0;
      backdrop-filter: blur(8px);
      z-index: 5;
    }
    h1 { margin: 0 0 10px 0; font-size: 20px; }
    main { padding: 20px; max-width: 1280px; margin: 0 auto; }
    .panel {
      background: linear-gradient(180deg, rgba(18,40,61,0.96), rgba(15,32,48,0.96));
      border: 1px solid var(--border);
      border-radius: 14px;
      padding: 16px;
      box-shadow: 0 12px 30px rgba(0,0,0,0.28);
      margin-bottom: 18px;
    }
    .toolbar, .meta {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: end;
    }
    .meta {
      margin-top: 10px;
      align-items: center;
    }
    .field {
      display: flex;
      flex-direction: column;
      gap: 6px;
      min-width: 180px;
    }
    .field.grow { flex: 1; min-width: 260px; }
    label { font-size: 12px; color: var(--muted); }
    input, select, button, textarea {
      padding: 9px 10px;
      border-radius: 8px;
      border: 1px solid #33506b;
      background: #0a1a29;
      color: var(--text);
      font-family: inherit;
    }
    input[readonly] {
      opacity: 0.9;
      background: #0b1725;
    }
    textarea {
      width: 100%;
      min-height: 170px;
      resize: vertical;
      line-height: 1.45;
    }
    button {
      cursor: pointer;
      border-color: var(--accent-2);
      background: linear-gradient(90deg, var(--accent-2), var(--accent));
      color: white;
      font-weight: 600;
    }
    button.secondary {
      background: #13273a;
      border-color: #2e4961;
      color: #dbeafe;
    }
    button.ghost {
      background: transparent;
      border-color: #2e4961;
      color: #dbeafe;
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
    .status.warn { color: var(--warn); }
    .hint { color: var(--muted); font-size: 12px; line-height: 1.5; }
    .badge {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 5px 10px;
      border-radius: 999px;
      border: 1px solid #35536b;
      background: rgba(10,26,41,0.92);
      font-size: 12px;
      color: #d7e8f6;
    }
    .kv-table {
      display: grid;
      grid-template-columns: 280px 1fr 1.5fr;
      gap: 8px;
    }
    .kv-row { display: contents; }
    .kv-key {
      padding: 8px 10px;
      background: #0a1725;
      border: 1px solid #1d3247;
      border-radius: 6px;
      font-family: Menlo, Consolas, monospace;
      font-size: 12px;
      word-break: break-word;
    }
    .kv-input input { width: 100%; }
    .kv-input select { width: 100%; }
    .kv-desc {
      font-size: 12px;
      color: var(--muted);
      padding: 8px 4px;
      line-height: 1.45;
    }
    .mapping-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 10px;
      margin-top: 10px;
    }
    .mapping-item {
      border: 1px solid #23435b;
      border-radius: 10px;
      padding: 10px 12px;
      background: rgba(11,22,34,0.92);
    }
    .mapping-item strong {
      display: block;
      margin-bottom: 4px;
      font-size: 13px;
    }
    .mono { font-family: Menlo, Consolas, monospace; }
    @media (max-width: 960px) {
      .kv-table { grid-template-columns: 1fr; }
      .kv-key { border-radius: 8px; }
    }
  </style>
</head>
<body>
  <header>
    <h1>MM 配置中心</h1>
    <div class="toolbar">
      <div class="field">
        <label for="exchange">Exchange</label>
        <select id="exchange"></select>
      </div>
      <div class="field">
        <label for="venue">Venue</label>
        <input id="venue" readonly />
      </div>
      <div class="field grow">
        <label for="model-name">Model Name</label>
        <input id="model-name" list="recent-model-names" placeholder="binance_futures_direction_model" />
        <datalist id="recent-model-names"></datalist>
      </div>
      <div class="field grow">
        <label for="env-name">Env Name</label>
        <input id="env-name" readonly />
      </div>
      <button id="reload-all" class="ghost" title="批量读取">读取全部</button>
    </div>
    <div class="meta">
      <span class="badge mono" id="symbol-key">-</span>
      <span class="badge mono" id="strategy-key">-</span>
      <span class="badge mono" id="amount-u-key">-</span>
      <span class="badge mono" id="max-pos-u-key">-</span>
      <span class="badge mono" id="risk-key">-</span>
      <span class="badge mono" id="model-params-key">-</span>
    </div>
  </header>

  <main>
    <section class="panel">
      <div class="section-header">
        <h2>Symbol List</h2>
        <div class="actions">
          <button id="load-symbols" class="secondary">读取</button>
          <button id="save-symbols">保存</button>
        </div>
      </div>
      <div class="hint">一行一个 symbol，保存后会写入 Redis String `mm_trade_symbols:{venue}`。</div>
      <textarea id="symbols-text" class="mono" placeholder="BTCUSDT&#10;ETHUSDT"></textarea>
      <div id="symbols-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Strategy Params</h2>
        <div class="actions">
          <button id="load-strategy" class="secondary">读取</button>
          <button id="reset-strategy" class="ghost">恢复默认</button>
          <button id="save-strategy">保存</button>
        </div>
      </div>
      <div class="hint">
        `enable_return_score_cancel` 控制基于模型 score_quantile 的方向撤单；`return_score_buy_cancel_quantile` / `return_score_sell_cancel_quantile` 必须在 (0,99) 内，默认 90/10；`enable_tlen_cancel` 单独控制基于 tlen 的 trigger/query/cancel 链路；`tlen_cancel_freq_ms` 控制 MMCancelTrigger 的发送频率；`enable_return_score_adjust_hegde=false` 时，MM hedge offset 不再被 return score 调整；`enable_environment_model=false` 时 env/pnlu 仍会写入 from_key，但不阻拦开仓；`enable_volatility_limit` 控制是否启用波动率限制下单；`open_volatility_limit` 控制 trade signal / MM 决策侧内联波动率阈值采样使用的分位数；`enable_tradecount_limit` / `open_tradecount_limit` 与 vol 用法一致，但读取 trade_flow_feature 的 `count.rolling(30,min_periods=25).mean()` 做 gate，并且仅当 tradecount 大于阈值时允许 open；`enable_open_time_block` 启用后，在 UTC `open_block_utc_time_range` 内 trade signal 不发开仓单，时间格式必须为 `HH:MM-HH:MM`，允许跨天，但开始/结束不能相同；前端布尔项使用下拉框编辑。
      </div>
      <div class="kv-table" id="strategy-table"></div>
      <div id="strategy-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>MM Amount U Overrides</h2>
        <div class="actions">
          <button id="load-amount-u" class="secondary">读取</button>
          <button id="reset-amount-u" class="ghost">示例</button>
          <button id="save-amount-u">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构为 `{"SYMBOL": amount_u}`。保存时会写入 Redis String
        `<env_name>:<venue>:mm:amount_u`，用于覆盖 strategy params 里的 `default_order_amount`。
      </div>
      <div class="hint">示例：</div>
      <pre id="amount-u-example" class="mono"></pre>
      <textarea id="amount-u-text" class="mono" spellcheck="false"></textarea>
      <div id="amount-u-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>MM Max Pos U Overrides</h2>
        <div class="actions">
          <button id="load-max-pos-u" class="secondary">读取</button>
          <button id="reset-max-pos-u" class="ghost">示例</button>
          <button id="save-max-pos-u">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构为 `{"SYMBOL": max_pos_u}`。保存时会写入 Redis String
        `<env_name>:<venue>:mm:max_pos_u`，用于按 symbol 覆盖 risk params 里的默认 `max_pos_u`。
      </div>
      <div class="hint">示例：</div>
      <pre id="max-pos-u-example" class="mono"></pre>
      <textarea id="max-pos-u-text" class="mono" spellcheck="false"></textarea>
      <div id="max-pos-u-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Risk Params</h2>
        <div class="actions">
          <button id="load-risk" class="secondary">读取</button>
          <button id="reset-risk" class="ghost">恢复默认</button>
          <button id="save-risk">保存</button>
        </div>
      </div>
      <div class="hint">
        MM 风控 key 格式为 `<env_name>:<open_venue>:<hedge_venue>:pre_trade_risk_params`；
        当前 MM 默认 `open_venue=hedge_venue=venue`，所以中间的 venue 会重复一次。
        `max_pending_limit_orders` / `max_pending_limit_buy_orders` / `max_pending_limit_sell_orders`
        都只限制 open 单，`MMHedge` 不受这组限制且不计入统计。
        `open_order_rate_limit_per_min` / `open_order_rate_limit_10s` 是开仓侧独立额度；
        `hedge_order_rate_limit_per_min` / `hedge_order_rate_limit_10s` 是对冲侧独立额度；若对冲额度触发上限，会在当前这一轮 hedge 批次里临时借用 open 剩余额度，下一轮恢复正常。`0` 表示关闭。
      </div>
      <div class="kv-table" id="risk-table"></div>
      <div id="risk-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Model Pub Score Params</h2>
        <div class="actions">
          <button id="load-model-score" class="secondary">读取</button>
          <button id="reset-model-score" class="ghost">恢复默认</button>
          <button id="save-model-score">保存</button>
        </div>
      </div>
      <div class="hint">
        配置 Redis Hash `model_score_rolling_params_{model_name}`，用于控制 `model_pub` 后处理的 score quantile 计算。
      </div>
      <div class="hint">rolling 输出 key：<span id="model-source-key" class="mono">-</span></div>
      <div class="kv-table" id="model-score-table"></div>
      <div id="model-score-status" class="status"></div>
    </section>

  </main>

  <script>
    const BOOTSTRAP = __BOOTSTRAP__;
    const RECENT_MODELS_STORAGE_KEY = `mm_config_recent_models:${BOOTSTRAP.env_name}`;
    const state = {
      exchange: BOOTSTRAP.default_exchange,
      venue: BOOTSTRAP.default_venue,
      envName: BOOTSTRAP.env_name,
      modelName: BOOTSTRAP.default_model_name || '',
    };

    function apiUrl(path) {
      const pathname = window.location.pathname || '/';
      const withoutFile = pathname.endsWith('/index.html')
        ? pathname.slice(0, -'/index.html'.length)
        : pathname;
      const base = withoutFile.endsWith('/') ? withoutFile : withoutFile + '/';
      const clean = String(path || '').replace(/^\\//, '');
      return `${base}api/${clean}`;
    }

    function setStatus(id, message, level = '') {
      const el = document.getElementById(id);
      el.textContent = message || '';
      el.className = `status ${level}`.trim();
    }

    function formatError(err) {
      if (err && typeof err === 'object' && 'message' in err && err.message) {
        return String(err.message);
      }
      return String(err);
    }

    async function fetchJson(url, options = {}) {
      const resp = await fetch(url, {
        headers: {'Content-Type': 'application/json'},
        ...options,
      });
      const text = await resp.text();
      let data = {};
      if (text) {
        try {
          data = JSON.parse(text);
        } catch (err) {
          throw new Error(text);
        }
      }
      if (!resp.ok) {
        throw new Error(data.error || `HTTP ${resp.status}`);
      }
      return data;
    }

    function valueToInputText(value) {
      if (value === null || value === undefined) {
        return '';
      }
      if (typeof value === 'object') {
        return JSON.stringify(value);
      }
      return String(value);
    }

    function defaultVenue(exchange) {
      return `${exchange}-futures`;
    }

    function isBooleanParamValue(value) {
      const normalized = String(value ?? '').trim().toLowerCase();
      return ['true', 'false', '1', '0', 'yes', 'no', 'on', 'off', ''].includes(normalized);
    }

    function validateOpenBlockUtcTimeRangeText(raw) {
      const text = String(raw ?? '').trim();
      const matched = text.match(/^([01]\\d|2[0-3]):([0-5]\\d)-([01]\\d|2[0-3]):([0-5]\\d)$/);
      if (!matched) {
        throw new Error('open_block_utc_time_range 必须使用 UTC HH:MM-HH:MM 格式，例如 15:55-23:59');
      }
      const start = Number(matched[1]) * 60 + Number(matched[2]);
      const end = Number(matched[3]) * 60 + Number(matched[4]);
      if (end === start) {
        throw new Error('open_block_utc_time_range 开始/结束时间不能相同，避免全天/空窗口歧义');
      }
      return text;
    }

    function validateStrategyValues(values) {
      if (Object.prototype.hasOwnProperty.call(values, 'open_block_utc_time_range')) {
        values.open_block_utc_time_range = validateOpenBlockUtcTimeRangeText(
          values.open_block_utc_time_range
        );
      }
      ['return_score_buy_cancel_quantile', 'return_score_sell_cancel_quantile'].forEach((key) => {
        if (!Object.prototype.hasOwnProperty.call(values, key)) {
          return;
        }
        const value = Number(values[key]);
        if (!Number.isFinite(value) || value <= 0 || value >= 99) {
          throw new Error(`${key} 必须在 (0,99) 内`);
        }
        values[key] = String(value);
      });
      return values;
    }

    function currentModelName() {
      return (state.modelName || '').trim();
    }

    function loadRecentModels() {
      try {
        const raw = localStorage.getItem(RECENT_MODELS_STORAGE_KEY);
        const parsed = raw ? JSON.parse(raw) : [];
        return Array.isArray(parsed) ? parsed.filter(Boolean).map(String) : [];
      } catch (err) {
        return [];
      }
    }

    function persistModelName(modelName) {
      const trimmed = (modelName || '').trim();
      if (!trimmed) {
        return;
      }
      const recent = loadRecentModels().filter((item) => item !== trimmed);
      recent.unshift(trimmed);
      localStorage.setItem(RECENT_MODELS_STORAGE_KEY, JSON.stringify(recent.slice(0, 8)));
      renderRecentModels();
    }

    function renderRecentModels() {
      const datalist = document.getElementById('recent-model-names');
      datalist.innerHTML = '';
      loadRecentModels().forEach((modelName) => {
        const option = document.createElement('option');
        option.value = modelName;
        datalist.appendChild(option);
      });
    }

    function inferVenueFromModelName(modelName) {
      const trimmed = (modelName || '').trim().toLowerCase();
      const match = trimmed.match(/^([a-z0-9]+-(?:margin|futures|spot|swap|perpetual|perp))(?:$|[-_.])/);
      return match ? match[1] : '';
    }

    function defaultModelScoreParams(modelName) {
      return {
        reload_param_sec: BOOTSTRAP.defaults.model_score_rolling_params?.reload_param_sec ?? 60,
        max_length: BOOTSTRAP.defaults.model_score_rolling_params?.max_length ?? 150000,
        rolling_window: BOOTSTRAP.defaults.model_score_rolling_params?.rolling_window ?? 17800,
        min_periods: BOOTSTRAP.defaults.model_score_rolling_params?.min_periods ?? 100,
        quantiles: BOOTSTRAP.defaults.model_score_rolling_params?.quantiles ?? [0.9, 0.8, 0.2, 0.1],
      };
    }

    function requireModelName(statusId = 'model-score-status') {
      const modelName = currentModelName();
      if (!modelName) {
        setStatus(statusId, '请先输入 model_name', 'warn');
        throw new Error('model_name is required');
      }
      return modelName;
    }

    function updateDerivedKeys() {
      state.venue = defaultVenue(state.exchange);
      document.getElementById('venue').value = state.venue;
      document.getElementById('env-name').value = state.envName;
      document.getElementById('model-name').value = state.modelName;
      document.getElementById('symbol-key').textContent = `mm_trade_symbols:${state.venue}`;
      document.getElementById('strategy-key').textContent = `mm_strategy_params_${state.venue}`;
      document.getElementById('amount-u-key').textContent =
        `${state.envName}:${state.venue}:mm:amount_u`;
      document.getElementById('max-pos-u-key').textContent =
        `${state.envName}:${state.venue}:mm:max_pos_u`;
      document.getElementById('risk-key').textContent =
        `${state.envName}:${state.venue}:${state.venue}:pre_trade_risk_params`;
      const modelName = currentModelName();
      document.getElementById('model-params-key').textContent =
        modelName ? `model_score_rolling_params_${modelName}` : '-';
      document.getElementById('model-source-key').textContent =
        modelName ? `model_score_rolling_thresholds_${modelName}` : '-';
    }

    function buildParamRows(containerId, defaults, comments, order, values) {
      const container = document.getElementById(containerId);
      const keys = [];
      const seen = new Set();
      [...order, ...Object.keys(defaults || {}), ...Object.keys(comments || {})].forEach((key) => {
        if (!seen.has(key)) {
          seen.add(key);
          keys.push(key);
        }
      });

      container.innerHTML = '';
      keys.forEach((key) => {
        const row = document.createElement('div');
        row.className = 'kv-row';

        const keyEl = document.createElement('div');
        keyEl.className = 'kv-key';
        keyEl.textContent = key;

        const inputWrap = document.createElement('div');
        inputWrap.className = 'kv-input';
        const rawValue = values && values[key] !== undefined ? values[key] : (defaults[key] ?? '');
        const useBooleanSelect =
          containerId === 'strategy-table' &&
          ['enable_return_score_cancel', 'enable_tlen_cancel', 'enable_return_score_adjust_hegde', 'enable_environment_model', 'enable_volatility_limit', 'enable_tradecount_limit', 'enable_open_time_block'].includes(key) &&
          isBooleanParamValue(rawValue);
        let field;
        if (useBooleanSelect) {
          field = document.createElement('select');
          [
            ['false', 'false'],
            ['true', 'true'],
          ].forEach(([value, label]) => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = label;
            field.appendChild(option);
          });
          const normalized = String(rawValue ?? '').trim().toLowerCase();
          field.value = ['true', '1', 'yes', 'on'].includes(normalized) ? 'true' : 'false';
        } else {
          field = document.createElement('input');
          field.type = 'text';
          field.value = valueToInputText(rawValue);
          if (containerId === 'strategy-table' && key === 'open_block_utc_time_range') {
            field.placeholder = '15:55-23:59';
            field.pattern = '([01]\\\\d|2[0-3]):[0-5]\\\\d-([01]\\\\d|2[0-3]):[0-5]\\\\d';
            field.addEventListener('input', () => {
              try {
                validateOpenBlockUtcTimeRangeText(field.value);
                setStatus('strategy-status', '', '');
              } catch (err) {
                setStatus('strategy-status', formatError(err), 'err');
              }
            });
          }
        }
        field.dataset.key = key;
        inputWrap.appendChild(field);

        const descEl = document.createElement('div');
        descEl.className = 'kv-desc';
        descEl.textContent = comments[key] || '';

        container.appendChild(keyEl);
        container.appendChild(inputWrap);
        container.appendChild(descEl);
      });
    }

    function collectParamRows(containerId) {
      const out = {};
      document
        .querySelectorAll(`#${containerId} input[data-key], #${containerId} select[data-key], #${containerId} textarea[data-key]`)
        .forEach((input) => {
          out[input.dataset.key] = input.value.trim();
        });
      return out;
    }

    async function loadModelScoreParams() {
      const modelName = requireModelName('model-score-status');
      setStatus('model-score-status', '读取中...');
      try {
        const data = await fetchJson(
          `${apiUrl('model-score-rolling-params')}?exchange=${encodeURIComponent(state.exchange)}&model_name=${encodeURIComponent(modelName)}`
        );
        state.modelName = modelName;
        updateDerivedKeys();
        buildParamRows(
          'model-score-table',
          defaultModelScoreParams(modelName),
          BOOTSTRAP.comments.model_score_rolling_params || {},
          BOOTSTRAP.order.model_score_rolling_params || [],
          data.values || {}
        );
        persistModelName(modelName);
        const modelVenue = data.model_venue || inferVenueFromModelName(modelName);
        const suffix = modelVenue ? `，model venue=${modelVenue}` : '';
        const staleHint = data.stale_count ? `，隐藏旧字段 ${data.stale_count} 个` : '';
        setStatus('model-score-status', `已读取 ${data.count || 0} 个参数 ${data.key}${suffix}${staleHint}`, 'ok');
      } catch (err) {
        setStatus('model-score-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveModelScoreParams() {
      const modelName = requireModelName('model-score-status');
      setStatus('model-score-status', '保存中...');
      try {
        const values = collectParamRows('model-score-table');
        const data = await fetchJson(apiUrl('model-score-rolling-params'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, model_name: modelName, values}),
        });
        state.modelName = modelName;
        updateDerivedKeys();
        persistModelName(modelName);
        setStatus(
          'model-score-status',
          `已保存 ${data.count} 个字段，清理旧字段 ${data.removed_count || 0}`,
          'ok'
        );
      } catch (err) {
        setStatus('model-score-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    function resetModelScoreParams() {
      const modelName = currentModelName();
      buildParamRows(
        'model-score-table',
        defaultModelScoreParams(modelName),
        BOOTSTRAP.comments.model_score_rolling_params || {},
        BOOTSTRAP.order.model_score_rolling_params || [],
        {}
      );
      setStatus('model-score-status', '已恢复默认 rolling 参数，尚未写入 Redis', 'warn');
    }

    async function loadSymbols() {
      setStatus('symbols-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('symbol-list')}?exchange=${encodeURIComponent(state.exchange)}`);
        document.getElementById('symbols-text').value = (data.symbols || []).join('\\n');
        setStatus('symbols-status', `已读取 ${data.symbols.length} 个 symbol`, 'ok');
      } catch (err) {
        setStatus('symbols-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveSymbols() {
      setStatus('symbols-status', '保存中...');
      try {
        const symbols = document.getElementById('symbols-text').value;
        const data = await fetchJson(apiUrl('symbol-list'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, symbols}),
        });
        setStatus('symbols-status', `已保存 ${data.count} 个 symbol`, 'ok');
      } catch (err) {
        setStatus('symbols-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function loadStrategy() {
      setStatus('strategy-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('strategy-params')}?exchange=${encodeURIComponent(state.exchange)}`);
        buildParamRows(
          'strategy-table',
          BOOTSTRAP.defaults.strategy_params || {},
          BOOTSTRAP.comments.strategy_params || {},
          BOOTSTRAP.order.strategy_params || [],
          data.values || {}
        );
        const staleHint = data.stale_count ? `，隐藏旧字段 ${data.stale_count} 个` : '';
        setStatus('strategy-status', `已读取 ${data.count || 0} 个参数${staleHint}`, 'ok');
      } catch (err) {
        setStatus('strategy-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveStrategy() {
      setStatus('strategy-status', '保存中...');
      try {
        const values = validateStrategyValues(collectParamRows('strategy-table'));
        const data = await fetchJson(apiUrl('strategy-params'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, values}),
        });
        setStatus(
          'strategy-status',
          `已保存 ${data.count} 个参数，清理旧字段 ${data.removed_count || 0}`,
          'ok'
        );
      } catch (err) {
        setStatus('strategy-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    function resetStrategy() {
      buildParamRows(
        'strategy-table',
        BOOTSTRAP.defaults.strategy_params || {},
        BOOTSTRAP.comments.strategy_params || {},
        BOOTSTRAP.order.strategy_params || [],
        {}
      );
      setStatus('strategy-status', '已恢复默认值，尚未写入 Redis', 'warn');
    }

    async function loadAmountU() {
      setStatus('amount-u-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('amount-u')}?exchange=${encodeURIComponent(state.exchange)}`);
        document.getElementById('amount-u-text').value =
          JSON.stringify(data.values || {}, null, 2);
        setStatus(
          'amount-u-status',
          `已读取 ${data.count || 0} 个 symbol ${data.key}`,
          'ok'
        );
      } catch (err) {
        setStatus('amount-u-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveAmountU() {
      setStatus('amount-u-status', '保存中...');
      let values;
      try {
        values = JSON.parse(document.getElementById('amount-u-text').value || '{}');
      } catch (err) {
        setStatus('amount-u-status', `JSON 解析失败: ${err.message}`, 'err');
        return;
      }
      try {
        const data = await fetchJson(apiUrl('amount-u'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, values}),
        });
        document.getElementById('amount-u-text').value =
          JSON.stringify(data.values || {}, null, 2);
        setStatus(
          'amount-u-status',
          `已保存 ${data.count || 0} 个 symbol ${data.key}`,
          'ok'
        );
      } catch (err) {
        setStatus('amount-u-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    function resetAmountU() {
      document.getElementById('amount-u-text').value =
        JSON.stringify(BOOTSTRAP.amount_u_example || {}, null, 2);
      setStatus('amount-u-status', '已填入示例，尚未写入 Redis', 'warn');
    }

    async function loadMaxPosU() {
      setStatus('max-pos-u-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('max-pos-u')}?exchange=${encodeURIComponent(state.exchange)}`);
        document.getElementById('max-pos-u-text').value =
          JSON.stringify(data.values || {}, null, 2);
        setStatus(
          'max-pos-u-status',
          `已读取 ${data.count || 0} 个 symbol ${data.key}`,
          'ok'
        );
      } catch (err) {
        setStatus('max-pos-u-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveMaxPosU() {
      setStatus('max-pos-u-status', '保存中...');
      let values;
      try {
        values = JSON.parse(document.getElementById('max-pos-u-text').value || '{}');
      } catch (err) {
        setStatus('max-pos-u-status', `JSON 解析失败: ${err.message}`, 'err');
        return;
      }
      try {
        const data = await fetchJson(apiUrl('max-pos-u'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, values}),
        });
        document.getElementById('max-pos-u-text').value =
          JSON.stringify(data.values || {}, null, 2);
        setStatus(
          'max-pos-u-status',
          `已保存 ${data.count || 0} 个 symbol ${data.key}`,
          'ok'
        );
      } catch (err) {
        setStatus('max-pos-u-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    function resetMaxPosU() {
      document.getElementById('max-pos-u-text').value =
        JSON.stringify(BOOTSTRAP.max_pos_u_example || {}, null, 2);
      setStatus('max-pos-u-status', '已填入示例，尚未写入 Redis', 'warn');
    }

    async function loadRisk() {
      setStatus('risk-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('risk-params')}?exchange=${encodeURIComponent(state.exchange)}`);
        buildParamRows(
          'risk-table',
          BOOTSTRAP.defaults.risk_params || {},
          BOOTSTRAP.comments.risk_params || {},
          BOOTSTRAP.order.risk_params || [],
          data.values || {}
        );
        const staleHint = data.stale_count ? `，隐藏旧字段 ${data.stale_count} 个` : '';
        setStatus('risk-status', `已读取 ${data.count || 0} 个参数${staleHint}`, 'ok');
      } catch (err) {
        setStatus('risk-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveRisk() {
      setStatus('risk-status', '保存中...');
      try {
        const values = collectParamRows('risk-table');
        const data = await fetchJson(apiUrl('risk-params'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, values}),
        });
        setStatus(
          'risk-status',
          `已保存 ${data.count} 个参数，清理旧字段 ${data.removed_count || 0}`,
          'ok'
        );
      } catch (err) {
        setStatus('risk-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    function resetRisk() {
      buildParamRows(
        'risk-table',
        BOOTSTRAP.defaults.risk_params || {},
        BOOTSTRAP.comments.risk_params || {},
        BOOTSTRAP.order.risk_params || [],
        {}
      );
      setStatus('risk-status', '已恢复默认值，尚未写入 Redis', 'warn');
    }

    async function loadAll() {
      updateDerivedKeys();
      const tasks = [
        loadSymbols(),
        loadStrategy(),
        loadAmountU(),
        loadMaxPosU(),
        loadRisk(),
      ];
      if (currentModelName()) {
        tasks.push(loadModelScoreParams());
      } else {
        resetModelScoreParams();
      }
      await Promise.all(tasks);
    }

    function initExchangeSelector() {
      const select = document.getElementById('exchange');
      select.innerHTML = '';
      (BOOTSTRAP.supported_exchanges || []).forEach((exchange) => {
        const option = document.createElement('option');
        option.value = exchange;
        option.textContent = exchange;
        if (exchange === state.exchange) {
          option.selected = true;
        }
        select.appendChild(option);
      });
      select.addEventListener('change', async (event) => {
        state.exchange = event.target.value;
        try {
          await loadAll();
        } catch (err) {
          console.error(err);
        }
      });
    }

    function initModelNameInput() {
      const input = document.getElementById('model-name');
      const recent = loadRecentModels();
      if (!state.modelName && recent.length > 0) {
        state.modelName = recent[0];
      }
      input.value = state.modelName;
      renderRecentModels();
      input.addEventListener('change', () => {
        state.modelName = input.value.trim();
        updateDerivedKeys();
      });
      input.addEventListener('blur', () => {
        state.modelName = input.value.trim();
        updateDerivedKeys();
      });
      input.addEventListener('keydown', async (event) => {
        if (event.key !== 'Enter') {
          return;
        }
        state.modelName = input.value.trim();
        updateDerivedKeys();
        if (!state.modelName) {
          return;
        }
        try {
          await loadModelScoreParams();
        } catch (err) {
          console.error(err);
        }
      });
    }

    document.getElementById('reload-all').addEventListener('click', () => loadAll());
    document.getElementById('load-symbols').addEventListener('click', () => loadSymbols());
    document.getElementById('save-symbols').addEventListener('click', () => saveSymbols());
    document.getElementById('load-strategy').addEventListener('click', () => loadStrategy());
    document.getElementById('save-strategy').addEventListener('click', () => saveStrategy());
    document.getElementById('reset-strategy').addEventListener('click', () => resetStrategy());
    document.getElementById('load-amount-u').addEventListener('click', () => loadAmountU());
    document.getElementById('save-amount-u').addEventListener('click', () => saveAmountU());
    document.getElementById('reset-amount-u').addEventListener('click', () => resetAmountU());
    document.getElementById('load-max-pos-u').addEventListener('click', () => loadMaxPosU());
    document.getElementById('save-max-pos-u').addEventListener('click', () => saveMaxPosU());
    document.getElementById('reset-max-pos-u').addEventListener('click', () => resetMaxPosU());
    document.getElementById('load-risk').addEventListener('click', () => loadRisk());
    document.getElementById('save-risk').addEventListener('click', () => saveRisk());
    document.getElementById('reset-risk').addEventListener('click', () => resetRisk());
    document.getElementById('load-model-score').addEventListener('click', () => loadModelScoreParams());
    document.getElementById('save-model-score').addEventListener('click', () => saveModelScoreParams());
    document.getElementById('reset-model-score').addEventListener('click', () => resetModelScoreParams());

    initExchangeSelector();
    initModelNameInput();
    document.getElementById('amount-u-example').textContent =
      JSON.stringify(BOOTSTRAP.amount_u_example || {}, null, 2);
    document.getElementById('max-pos-u-example').textContent =
      JSON.stringify(BOOTSTRAP.max_pos_u_example || {}, null, 2);
    loadAll().catch((err) => {
      console.error(err);
      setStatus('symbols-status', err.message || String(err), 'err');
    });
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


def normalize_exchange(exchange: str) -> str:
    value = (exchange or "").strip().lower()
    if value == "okx":
        value = "okex"
    return value


def infer_exchange_from_name(name: str) -> Optional[str]:
    text = (name or "").strip().lower()
    if not text:
        return None

    mm_match = re.match(r"^([a-z0-9]+)[-_]mm(?:$|[_-])", text)
    if mm_match:
        return normalize_exchange(mm_match.group(1))

    for exchange in SUPPORTED_EXCHANGES + ["okx"]:
        if re.search(rf"(^|[^a-z0-9]){exchange}([^a-z0-9]|$)", text):
            return normalize_exchange(exchange)
    return None


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(os.path.basename(os.getcwd()))


def default_venue_for_exchange(exchange: str) -> str:
    return f"{normalize_exchange(exchange)}-futures"


def infer_env_name_from_cwd() -> str:
    name = os.path.basename(os.getcwd()).strip().lower()
    return name or "mm"


def infer_venue_from_model_name(model_name: str) -> str:
    normalized = (model_name or "").strip().lower()
    if normalized == "binance_futures_direction_model":
        return "binance-futures"
    matched = VENUE_PREFIX_PATTERN.match(normalized)
    if not matched:
        raise ValueError(
            "cannot infer venue from model_name prefix; expected prefix like "
            "'binance-futures-...' or special model name 'binance_futures_direction_model'"
        )
    return matched.group(1)


def validate_model_name(model_name: str) -> str:
    value = (model_name or "").strip()
    if not value:
        raise ValueError("model_name is required")
    if re.fullmatch(MODEL_RE, value) is None:
        raise ValueError(f"invalid model_name: {value}")
    return value


def make_symbol_key(venue: str) -> str:
    return f"mm_trade_symbols:{venue}"


def make_strategy_key(venue: str) -> str:
    return f"mm_strategy_params_{venue}"


def make_amount_u_key(env_name: str, venue: str) -> str:
    return f"{env_name}:{venue}:mm:amount_u"


def make_max_pos_u_key(env_name: str, venue: str) -> str:
    return f"{env_name}:{venue}:mm:max_pos_u"


def make_risk_key(env_name: str, venue: str) -> str:
    return f"{env_name}:{venue}:{venue}:pre_trade_risk_params"


def make_model_score_params_key(model_name: str) -> str:
    return f"model_score_rolling_params_{model_name}"


def make_model_score_threshold_source_key(model_name: str) -> str:
    return f"model_score_rolling_thresholds_{model_name}"


def decode_hash(data: Dict[object, object]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for raw_key, raw_value in data.items():
        key = raw_key.decode("utf-8", "ignore") if isinstance(raw_key, bytes) else str(raw_key)
        value = (
            raw_value.decode("utf-8", "ignore")
            if isinstance(raw_value, bytes)
            else str(raw_value)
        )
        out[key] = value
    return out


def parse_symbols(text_or_list: Any) -> List[str]:
    raw_items: List[str] = []
    if isinstance(text_or_list, list):
        raw_items = [str(item) for item in text_or_list]
    elif isinstance(text_or_list, str):
        raw_items = re.split(r"[\s,]+", text_or_list)
    elif text_or_list is None:
        raw_items = []
    else:
        raise ValueError("symbols must be a string or an array")

    seen = set()
    symbols: List[str] = []
    for item in raw_items:
        symbol = str(item).strip().upper()
        if not symbol:
            continue
        if symbol in seen:
            continue
        seen.add(symbol)
        symbols.append(symbol)
    return symbols


def normalize_amount_u_symbol(raw: Any) -> str:
    symbol = re.sub(r"[^A-Za-z0-9]", "", str(raw or "").strip()).upper()
    if not symbol:
        raise ValueError(f"invalid symbol: {raw!r}")
    return symbol


def normalize_amount_u_mapping(values: Any) -> Dict[str, float]:
    if values is None:
        return {}
    if not isinstance(values, dict):
        raise ValueError("amount_u values must be an object")

    normalized: Dict[str, float] = {}
    for raw_symbol, raw_value in values.items():
        symbol = normalize_amount_u_symbol(raw_symbol)
        try:
            amount_u = float(raw_value)
        except Exception as exc:
            raise ValueError(f"invalid amount_u for {symbol}: {raw_value}") from exc
        if not (amount_u > 0.0):
            raise ValueError(f"amount_u must be > 0 for {symbol}: {raw_value}")
        normalized[symbol] = amount_u
    return dict(sorted(normalized.items()))


def dumps_amount_u_mapping(values: Dict[str, float]) -> str:
    ordered = {symbol: float(f"{values[symbol]:.12g}") for symbol in sorted(values.keys())}
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def normalize_max_pos_u_mapping(values: Any) -> Dict[str, float]:
    if values is None:
        return {}
    if not isinstance(values, dict):
        raise ValueError("max_pos_u values must be an object")

    normalized: Dict[str, float] = {}
    for raw_symbol, raw_value in values.items():
        symbol = normalize_amount_u_symbol(raw_symbol)
        try:
            max_pos_u = float(raw_value)
        except Exception as exc:
            raise ValueError(f"invalid max_pos_u for {symbol}: {raw_value}") from exc
        if not (max_pos_u > 0.0):
            raise ValueError(f"max_pos_u must be > 0 for {symbol}: {raw_value}")
        normalized[symbol] = max_pos_u
    return dict(sorted(normalized.items()))


def dumps_max_pos_u_mapping(values: Dict[str, float]) -> str:
    ordered = {symbol: float(f"{values[symbol]:.12g}") for symbol in sorted(values.keys())}
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def sanitize_string_mapping(values: Any) -> Dict[str, str]:
    if not isinstance(values, dict):
        raise ValueError("values must be an object")
    out: Dict[str, str] = {}
    for key, value in values.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            continue
        if value is None:
            out[normalized_key] = ""
        else:
            out[normalized_key] = str(value).strip()
    return out


def ordered_schema_keys(
    defaults: Dict[str, Any],
    comments: Dict[str, str],
    order: List[str],
) -> List[str]:
    keys: List[str] = []
    seen = set()
    for key in [*order, *defaults.keys(), *comments.keys()]:
        if key in seen:
            continue
        seen.add(key)
        keys.append(key)
    return keys


def sanitize_mapping_by_schema(
    values: Any,
    defaults: Dict[str, Any],
    comments: Dict[str, str],
    order: List[str],
) -> Dict[str, str]:
    mapping = sanitize_string_mapping(values)
    allowed = set(ordered_schema_keys(defaults, comments, order))
    sanitized = {key: mapping[key] for key in mapping.keys() if key in allowed}
    return normalize_strategy_params_by_schema(sanitized)


def normalize_positive_int_text(raw: Any, field_name: str) -> str:
    text = str(raw).strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    try:
        value = int(text)
    except Exception as exc:
        raise ValueError(f"{field_name} must be a positive integer: {text}") from exc
    if value <= 0:
        raise ValueError(f"{field_name} must be > 0: {text}")
    return str(value)


def normalize_nonnegative_int_text(raw: Any, field_name: str) -> str:
    text = str(raw).strip()
    if not text:
        raise ValueError(f"{field_name} is required")
    try:
        value = int(text)
    except Exception as exc:
        raise ValueError(f"{field_name} must be a non-negative integer: {text}") from exc
    if value < 0:
        raise ValueError(f"{field_name} must be >= 0: {text}")
    return str(value)


def normalize_open_block_utc_time_range(raw: Any) -> str:
    text = str(raw or "").strip()
    matched = re.fullmatch(
        r"([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)",
        text,
    )
    if not matched:
        raise ValueError(
            "open_block_utc_time_range 必须使用 UTC HH:MM-HH:MM 格式，例如 15:55-23:59"
        )

    start_hour, start_min, end_hour, end_min = [int(part) for part in matched.groups()]
    start_total = start_hour * 60 + start_min
    end_total = end_hour * 60 + end_min
    if end_total == start_total:
        raise ValueError(
            "open_block_utc_time_range 开始/结束时间不能相同，避免全天/空窗口歧义"
        )
    return text


def normalize_strategy_params_by_schema(mapping: Dict[str, str]) -> Dict[str, str]:
    normalized = dict(mapping)
    if "tlen_cancel_freq_ms" in normalized:
        normalized["tlen_cancel_freq_ms"] = normalize_positive_int_text(
            normalized["tlen_cancel_freq_ms"], "tlen_cancel_freq_ms"
        )
    for key in ("order_interval_ms", "next_query_delay_ms"):
        if key in normalized:
            normalized[key] = normalize_positive_int_text(normalized[key], key)
    if "enable_clock_shift_ms" in normalized:
        normalized["enable_clock_shift_ms"] = normalize_nonnegative_int_text(
            normalized["enable_clock_shift_ms"], "enable_clock_shift_ms"
        )
        shift = int(normalized["enable_clock_shift_ms"])
        order_interval_ms = int(
            normalized.get("order_interval_ms", DEFAULT_STRATEGY_PARAMS.get("order_interval_ms", "5000"))
        )
        next_query_delay_ms = int(
            normalized.get("next_query_delay_ms", DEFAULT_STRATEGY_PARAMS.get("next_query_delay_ms", "30000"))
        )
        if shift and (shift >= order_interval_ms or shift >= next_query_delay_ms):
            raise ValueError(
                "enable_clock_shift_ms must be smaller than both order_interval_ms and next_query_delay_ms"
            )
    for key in ("return_score_buy_cancel_quantile", "return_score_sell_cancel_quantile"):
        if key in normalized:
            value = float(str(normalized[key]).strip())
            if not (0.0 < value < 99.0):
                raise ValueError(f"{key} must be within (0,99): {value}")
            normalized[key] = f"{value:g}"
    if "open_block_utc_time_range" in normalized:
        normalized["open_block_utc_time_range"] = normalize_open_block_utc_time_range(
            normalized["open_block_utc_time_range"]
        )
    return normalized


def serialize_model_score_params(values: Any) -> Dict[str, str]:
    if not isinstance(values, dict):
        raise ValueError("values must be an object")

    serialized: Dict[str, str] = {}
    for key, raw_value in values.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            continue
        if isinstance(raw_value, (list, dict)):
            serialized[normalized_key] = json.dumps(
                raw_value, ensure_ascii=False, separators=(",", ":")
            )
        elif raw_value is None:
            serialized[normalized_key] = ""
        else:
            serialized[normalized_key] = str(raw_value).strip()
    return serialized


def serialize_model_score_params_by_schema(values: Any, model_name: str) -> Dict[str, str]:
    defaults = build_default_model_score_params(model_name)
    allowed = set(ordered_schema_keys(defaults, MODEL_SCORE_PARAM_COMMENTS, MODEL_SCORE_PARAM_ORDER))
    serialized = serialize_model_score_params(values)
    return {key: serialized[key] for key in serialized.keys() if key in allowed}


def maybe_decode_json_string(raw: str) -> Any:
    text = (raw or "").strip()
    if not text:
        return ""
    if text[0] not in "[{\"-0123456789tfn":
        return raw
    try:
        return json.loads(text)
    except Exception:
        return raw


def deserialize_model_score_params(values: Dict[str, str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, raw in values.items():
        out[key] = maybe_decode_json_string(raw)
    return out


def build_default_model_score_params(model_name: str) -> Dict[str, Any]:
    validate_model_name(model_name)
    defaults = {}
    for key, value in DEFAULT_MODEL_SCORE_ROLLING_PARAMS.items():
        if isinstance(value, list):
            defaults[key] = list(value)
        elif isinstance(value, dict):
            defaults[key] = dict(value)
        else:
            defaults[key] = value
    return defaults


def filter_mapping_by_schema(
    raw_values: Dict[str, str],
    defaults: Dict[str, Any],
    comments: Dict[str, str],
    order: List[str],
) -> Tuple[Dict[str, str], Dict[str, str]]:
    allowed_keys = ordered_schema_keys(defaults, comments, order)
    seen = set(allowed_keys)
    filtered = {key: raw_values[key] for key in allowed_keys if key in raw_values}
    stale = {key: value for key, value in raw_values.items() if key not in seen}
    return filtered, stale


def build_bootstrap(default_exchange: str, env_name: str) -> Dict[str, Any]:
    exchange = normalize_exchange(default_exchange)
    venue = default_venue_for_exchange(exchange)
    return {
        "supported_exchanges": SUPPORTED_EXCHANGES,
        "default_exchange": exchange,
        "default_venue": venue,
        "env_name": env_name,
        "default_model_name": "",
        "defaults": {
            "symbols": DEFAULT_SYMBOLS,
            "strategy_params": DEFAULT_STRATEGY_PARAMS,
            "risk_params": DEFAULT_RISK_PARAMS,
            "model_score_rolling_params": DEFAULT_MODEL_SCORE_ROLLING_PARAMS,
        },
        "comments": {
            "strategy_params": STRATEGY_PARAM_COMMENTS,
            "risk_params": RISK_PARAM_COMMENTS,
            "model_score_rolling_params": MODEL_SCORE_PARAM_COMMENTS,
        },
        "order": {
            "strategy_params": STRATEGY_PARAM_ORDER,
            "risk_params": RISK_PARAM_ORDER,
            "model_score_rolling_params": MODEL_SCORE_PARAM_ORDER,
        },
        "amount_u_example": AMOUNT_U_EXAMPLE,
        "max_pos_u_example": MAX_POS_U_EXAMPLE,
        "keys": {
            "symbol": make_symbol_key(venue),
            "strategy": make_strategy_key(venue),
            "amount_u": make_amount_u_key(env_name, venue),
            "max_pos_u": make_max_pos_u_key(env_name, venue),
            "risk": make_risk_key(env_name, venue),
        },
    }


def resolve_exchange(query: Dict[str, List[str]], payload: Optional[Dict[str, Any]], fallback: str) -> str:
    payload = payload or {}
    raw = (
        payload.get("exchange")
        or payload.get("default_exchange")
        or first_query_value(query, "exchange")
        or fallback
    )
    exchange = normalize_exchange(str(raw))
    if exchange not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {exchange}")
    return exchange


def resolve_model_name(query: Dict[str, List[str]], payload: Optional[Dict[str, Any]]) -> str:
    payload = payload or {}
    raw = payload.get("model_name") or first_query_value(query, "model_name") or ""
    return validate_model_name(str(raw))


def first_query_value(query: Dict[str, List[str]], key: str) -> Optional[str]:
    values = query.get(key) or []
    return values[0] if values else None


def read_json_body(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length_text = handler.headers.get("Content-Length", "0").strip() or "0"
    try:
        length = int(length_text)
    except Exception as exc:
        raise ValueError(f"invalid Content-Length: {length_text}") from exc

    raw = handler.rfile.read(length) if length > 0 else b"{}"
    if not raw:
        return {}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise ValueError(f"invalid JSON body: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON body must be an object")
    return payload


@dataclass
class AppConfig:
    host: str
    port: int
    default_exchange: str
    env_name: str


class MMConfigStore:
    def __init__(self, config: AppConfig):
        self._config = config
        self._redis_mod = try_import_redis()

    def redis(self):
        if self._redis_mod is None:
            raise RuntimeError("redis package is not installed. Please run: pip install redis")
        return self._redis_mod.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    def _replace_hash(self, key: str, mapping: Dict[str, str]) -> Dict[str, Any]:
        redis = self.redis()
        existing = {
            item.decode("utf-8", "ignore") if isinstance(item, bytes) else str(item)
            for item in redis.hkeys(key)
        }
        stale_fields = sorted(existing - set(mapping.keys()))

        pipe = redis.pipeline()
        if mapping:
            pipe.hset(key, mapping=mapping)
        if stale_fields:
            pipe.hdel(key, *stale_fields)
        pipe.execute()

        return {
            "key": key,
            "count": len(mapping),
            "values": mapping,
            "removed_count": len(stale_fields),
            "removed_fields": stale_fields,
        }

    def _write_hash_replace(self, key: str, values: Any) -> Dict[str, Any]:
        return self._replace_hash(key, sanitize_string_mapping(values))

    def read_symbols(self, venue: str) -> Dict[str, Any]:
        key = make_symbol_key(venue)
        payload = self.redis().get(key)
        if payload is None:
            symbols = list(DEFAULT_SYMBOLS)
        else:
            try:
                decoded = payload.decode("utf-8", "ignore") if isinstance(payload, bytes) else str(payload)
                loaded = json.loads(decoded)
                symbols = parse_symbols(loaded)
            except Exception:
                symbols = []
        return {"key": key, "symbols": symbols}

    def write_symbols(self, venue: str, symbols_payload: Any) -> Dict[str, Any]:
        symbols = parse_symbols(symbols_payload)
        key = make_symbol_key(venue)
        self.redis().set(key, json.dumps(symbols))
        return {"key": key, "count": len(symbols), "symbols": symbols}

    def read_strategy_params(self, venue: str) -> Dict[str, Any]:
        key = make_strategy_key(venue)
        raw_values = decode_hash(self.redis().hgetall(key))
        values, stale_values = filter_mapping_by_schema(
            raw_values,
            DEFAULT_STRATEGY_PARAMS,
            STRATEGY_PARAM_COMMENTS,
            STRATEGY_PARAM_ORDER,
        )
        return {
            "key": key,
            "values": values,
            "raw_count": len(raw_values),
            "count": len(values),
            "stale_count": len(stale_values),
            "stale_values": stale_values,
        }

    def write_strategy_params(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_strategy_key(venue)
        mapping = sanitize_mapping_by_schema(
            values,
            DEFAULT_STRATEGY_PARAMS,
            STRATEGY_PARAM_COMMENTS,
            STRATEGY_PARAM_ORDER,
        )
        return self._replace_hash(key, mapping)

    def read_amount_u(self, venue: str) -> Dict[str, Any]:
        key = make_amount_u_key(self._config.env_name, venue)
        raw = self.redis().get(key)
        if raw is None:
            values: Dict[str, float] = {}
        else:
            text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
            try:
                decoded = json.loads(text)
            except Exception as exc:
                raise ValueError(f"invalid JSON in {key}: {exc}") from exc
            values = normalize_amount_u_mapping(decoded)
        return {
            "key": key,
            "values": values,
            "count": len(values),
        }

    def write_amount_u(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_amount_u_key(self._config.env_name, venue)
        normalized = normalize_amount_u_mapping(values)
        payload = dumps_amount_u_mapping(normalized)
        self.redis().set(key, payload)
        return {
            "key": key,
            "values": normalized,
            "count": len(normalized),
        }

    def read_max_pos_u(self, venue: str) -> Dict[str, Any]:
        key = make_max_pos_u_key(self._config.env_name, venue)
        raw = self.redis().get(key)
        if raw is None:
            values: Dict[str, float] = {}
        else:
            text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
            try:
                decoded = json.loads(text)
            except Exception as exc:
                raise ValueError(f"invalid JSON in {key}: {exc}") from exc
            values = normalize_max_pos_u_mapping(decoded)
        return {
            "key": key,
            "values": values,
            "count": len(values),
        }

    def write_max_pos_u(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_max_pos_u_key(self._config.env_name, venue)
        normalized = normalize_max_pos_u_mapping(values)
        payload = dumps_max_pos_u_mapping(normalized)
        self.redis().set(key, payload)
        return {
            "key": key,
            "values": normalized,
            "count": len(normalized),
        }

    def read_risk_params(self, venue: str) -> Dict[str, Any]:
        key = make_risk_key(self._config.env_name, venue)
        raw_values = decode_hash(self.redis().hgetall(key))
        values, stale_values = filter_mapping_by_schema(
            raw_values,
            DEFAULT_RISK_PARAMS,
            RISK_PARAM_COMMENTS,
            RISK_PARAM_ORDER,
        )
        return {
            "key": key,
            "values": values,
            "raw_count": len(raw_values),
            "count": len(values),
            "stale_count": len(stale_values),
            "stale_values": stale_values,
        }

    def write_risk_params(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_risk_key(self._config.env_name, venue)
        mapping = sanitize_mapping_by_schema(
            values,
            DEFAULT_RISK_PARAMS,
            RISK_PARAM_COMMENTS,
            RISK_PARAM_ORDER,
        )
        return self._replace_hash(key, mapping)

    def read_model_score_rolling_params(self, model_name: str) -> Dict[str, Any]:
        key = make_model_score_params_key(model_name)
        raw = decode_hash(self.redis().hgetall(key))
        if raw:
            filtered, stale_values = filter_mapping_by_schema(
                raw,
                build_default_model_score_params(model_name),
                MODEL_SCORE_PARAM_COMMENTS,
                MODEL_SCORE_PARAM_ORDER,
            )
            values = deserialize_model_score_params(filtered)
        else:
            values = build_default_model_score_params(model_name)
            stale_values = {}
        return {
            "key": key,
            "source_key": make_model_score_threshold_source_key(model_name),
            "model_name": model_name,
            "model_venue": infer_venue_from_model_name(model_name),
            "values": values,
            "raw_count": len(raw),
            "count": len(values),
            "stale_count": len(stale_values),
            "stale_values": stale_values,
        }

    def write_model_score_rolling_params(self, model_name: str, values: Any) -> Dict[str, Any]:
        key = make_model_score_params_key(model_name)
        mapping = serialize_model_score_params_by_schema(values, model_name)
        if not mapping:
            raise ValueError("model score rolling params cannot be empty")
        result = self._replace_hash(key, mapping)
        result.update(
            {
                "source_key": make_model_score_threshold_source_key(model_name),
                "model_name": model_name,
                "model_venue": infer_venue_from_model_name(model_name),
                "values": deserialize_model_score_params(mapping),
            }
        )
        return result


def build_index_html(config: AppConfig) -> bytes:
    bootstrap = build_bootstrap(config.default_exchange, config.env_name)
    html = INDEX_HTML_TEMPLATE.replace(
        "__BOOTSTRAP__",
        json.dumps(bootstrap, ensure_ascii=False),
    )
    return html.encode("utf-8")


def build_handler(config: AppConfig):
    store = MMConfigStore(config)
    index_html = build_index_html(config)

    class Handler(BaseHTTPRequestHandler):
        server_version = "MMConfigServer/1.0"

        def _send_json(self, status: int, payload: Dict[str, Any]) -> None:
            encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _send_html(self, payload: bytes) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def _handle_exception(self, exc: Exception, status: int = 400) -> None:
            self._send_json(status, {"ok": False, "error": str(exc)})

        def log_message(self, format_str: str, *args: Any) -> None:
            sys.stderr.write(
                "%s - - [%s] %s\n"
                % (self.address_string(), self.log_date_time_string(), format_str % args)
            )

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            try:
                if parsed.path in ("/", "/index.html"):
                    self._send_html(index_html)
                    return

                if parsed.path == "/healthz":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "default_exchange": config.default_exchange,
                            "env_name": config.env_name,
                        },
                    )
                    return

                if parsed.path == "/api/bootstrap":
                    self._send_json(
                        200,
                        {"ok": True, **build_bootstrap(config.default_exchange, config.env_name)},
                    )
                    return

                exchange = resolve_exchange(query, None, config.default_exchange)
                venue = default_venue_for_exchange(exchange)

                if parsed.path == "/api/symbol-list":
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **store.read_symbols(venue)})
                    return

                if parsed.path == "/api/strategy-params":
                    self._send_json(
                        200,
                        {"ok": True, "exchange": exchange, "venue": venue, **store.read_strategy_params(venue)},
                    )
                    return

                if parsed.path == "/api/amount-u":
                    self._send_json(
                        200,
                        {"ok": True, "exchange": exchange, "venue": venue, **store.read_amount_u(venue)},
                    )
                    return

                if parsed.path == "/api/max-pos-u":
                    self._send_json(
                        200,
                        {"ok": True, "exchange": exchange, "venue": venue, **store.read_max_pos_u(venue)},
                    )
                    return

                if parsed.path == "/api/risk-params":
                    self._send_json(
                        200,
                        {"ok": True, "exchange": exchange, "venue": venue, **store.read_risk_params(venue)},
                    )
                    return

                if parsed.path == "/api/model-score-rolling-params":
                    model_name = resolve_model_name(query, None)
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "exchange": exchange,
                            "venue": venue,
                            **store.read_model_score_rolling_params(model_name),
                        },
                    )
                    return

                self._send_json(404, {"ok": False, "error": f"not found: {parsed.path}"})
            except RuntimeError as exc:
                self._handle_exception(exc, status=500)
            except ValueError as exc:
                self._handle_exception(exc, status=400)
            except Exception as exc:
                self._handle_exception(exc, status=500)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            try:
                payload = read_json_body(self)
                exchange = resolve_exchange({}, payload, config.default_exchange)
                venue = default_venue_for_exchange(exchange)

                if parsed.path == "/api/symbol-list":
                    result = store.write_symbols(venue, payload.get("symbols"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/strategy-params":
                    result = store.write_strategy_params(venue, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/amount-u":
                    result = store.write_amount_u(venue, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/max-pos-u":
                    result = store.write_max_pos_u(venue, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/risk-params":
                    result = store.write_risk_params(venue, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/model-score-rolling-params":
                    model_name = resolve_model_name({}, payload)
                    result = store.write_model_score_rolling_params(model_name, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                self._send_json(404, {"ok": False, "error": f"not found: {parsed.path}"})
            except RuntimeError as exc:
                self._handle_exception(exc, status=500)
            except ValueError as exc:
                self._handle_exception(exc, status=400)
            except Exception as exc:
                self._handle_exception(exc, status=500)

    return Handler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MM config server")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "18131")))
    parser.add_argument(
        "--default-exchange",
        default=os.environ.get("DEFAULT_EXCHANGE") or infer_exchange_from_cwd() or "binance",
    )
    parser.add_argument(
        "--env-name",
        default=os.environ.get("ENV_NAME") or infer_env_name_from_cwd(),
    )
    args = parser.parse_args()

    args.default_exchange = normalize_exchange(args.default_exchange)
    if args.default_exchange not in SUPPORTED_EXCHANGES:
        parser.error(
            f"--default-exchange must be one of: {', '.join(SUPPORTED_EXCHANGES)}"
        )

    args.env_name = (args.env_name or "").strip().lower()
    if not args.env_name:
        parser.error("--env-name cannot be empty")

    return args


def main() -> int:
    args = parse_args()
    config = AppConfig(
        host=args.host,
        port=args.port,
        default_exchange=args.default_exchange,
        env_name=args.env_name,
    )
    handler_cls = build_handler(config)
    server = ThreadingHTTPServer((config.host, config.port), handler_cls)
    print(
        f"[INFO] mm_config_server listening on http://{config.host}:{config.port} "
        f"(exchange={config.default_exchange}, env={config.env_name})"
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
