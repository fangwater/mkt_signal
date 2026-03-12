#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MM 配置服务器（mm_config_server）
--------------------------------
提供页面/接口用于查看和编辑 MM 相关配置：
- symbol list
- strategy params
- pre-trade risk params
- return-model-score thresholds
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
MODEL_RE = r"[a-zA-Z0-9][a-zA-Z0-9._-]*"
VENUE_PREFIX_PATTERN = re.compile(
    r"^([a-z0-9]+-(?:margin|futures|spot|swap|perpetual|perp))(?:$|[-_.])"
)
DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]
RETURN_THRESHOLD_EXAMPLE = {
    "BTCUSDT": {
        "forward_open": "0.002",
        "forward_cancel": "0.0015",
        "backward_open": "-0.002",
        "backward_cancel": "-0.0015",
    }
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
    import print_mm_return_score_thresholds as mm_threshold_defaults

    RETURN_THRESHOLD_OPERATIONS = list(mm_threshold_defaults.OPERATIONS)
    RETURN_THRESHOLD_MAPPING = dict(mm_threshold_defaults.DEFAULT_RETURN_MODEL_SCORE_MAPPING)
except Exception:
    RETURN_THRESHOLD_OPERATIONS = [
        "forward_open",
        "forward_cancel",
        "backward_open",
        "backward_cancel",
    ]
    RETURN_THRESHOLD_MAPPING = {
        "forward_open": "score_90",
        "forward_cancel": "score_80",
        "backward_open": "score_10",
        "backward_cancel": "score_20",
    }

try:
    import sync_model_score_rolling_params as model_score_defaults

    DEFAULT_MODEL_SCORE_ROLLING_PARAMS = dict(model_score_defaults.DEFAULTS)
except Exception:
    DEFAULT_MODEL_SCORE_ROLLING_PARAMS = {
        "max_length": 150_000,
        "refresh_sec": 30,
        "reload_param_sec": 3,
        "rolling_window": 17_800,
        "min_periods": 100,
        "quantiles": [0.9, 0.8, 0.2, 0.1],
    }

MODEL_SCORE_PARAM_COMMENTS = {
    "input_services": "输入模型输出服务列表(JSON 数组)，通常为 [\"model_output/<model_name>\"]",
    "output_hash_key": "rolling 服务输出的 Redis Hash key",
    "refresh_sec": "rolling quantile 重算周期(秒)",
    "reload_param_sec": "配置热更新周期(秒)",
    "max_length": "环形缓冲最大长度",
    "rolling_window": "rolling 窗口长度",
    "min_periods": "最小样本数",
    "quantiles": "需要计算的分位点(JSON 数组，如 [0.9,0.8,0.2,0.1])",
}
MODEL_SCORE_PARAM_ORDER = [
    "input_services",
    "output_hash_key",
    "refresh_sec",
    "reload_param_sec",
    "max_length",
    "rolling_window",
    "min_periods",
    "quantiles",
]
RETURN_MAPPING_COMMENTS = {
    "forward_open": "开正向单时使用的 score 百分位引用",
    "forward_cancel": "撤正向单时使用的 score 百分位引用",
    "backward_open": "开反向单时使用的 score 百分位引用",
    "backward_cancel": "撤反向单时使用的 score 百分位引用",
}

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
    .grid-2 {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 14px;
    }
    .kv-table {
      display: grid;
      grid-template-columns: 220px 1fr 1.5fr;
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
        <input id="model-name" list="recent-model-names" placeholder="binance-futures-mm-xgb-test" />
        <datalist id="recent-model-names"></datalist>
      </div>
      <div class="field grow">
        <label for="env-name">Env Name</label>
        <input id="env-name" readonly />
      </div>
      <button id="reload-all" class="ghost" title="批量读取">读取全部</button>
    </div>
    <div class="meta">
      <span class="badge">symbol key: <span id="symbol-key" class="mono">-</span></span>
      <span class="badge">strategy key: <span id="strategy-key" class="mono">-</span></span>
      <span class="badge">risk key: <span id="risk-key" class="mono">-</span></span>
      <span class="badge">model params key: <span id="model-params-key" class="mono">-</span></span>
      <span class="badge">mapping key: <span id="mapping-key" class="mono">-</span></span>
      <span class="badge">threshold key: <span id="threshold-key" class="mono">-</span></span>
    </div>
  </header>

  <main>
    <section class="panel">
      <div class="section-header">
        <h2>环境信息</h2>
      </div>
      <div class="grid-2">
        <div>
          <div class="hint">当前 server 以部署目录为上下文，MM 风控 key 会带上目录名作为前缀。</div>
          <div class="hint">切换 exchange 会同步 venue，并刷新 symbol / strategy / risk / rolling / mapping / threshold 配置。</div>
          <div class="hint">`model_name` 需要手动输入；浏览器会缓存最近使用过的值，便于下次快速选择。</div>
        </div>
        <div>
          <div class="hint">return score 阈值 JSON 结构：</div>
          <pre id="threshold-example" class="mono"></pre>
        </div>
      </div>
    </section>

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
      <div class="kv-table" id="strategy-table"></div>
      <div id="strategy-status" class="status"></div>
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
      <div class="hint">MM 风控 key 使用 `<env_name>:<venue>:<venue>:pre_trade_risk_params`。</div>
      <div class="kv-table" id="risk-table"></div>
      <div id="risk-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Model Score Rolling Params</h2>
        <div class="actions">
          <button id="load-model-score" class="secondary">读取</button>
          <button id="reset-model-score" class="ghost">恢复默认</button>
          <button id="save-model-score">保存</button>
        </div>
      </div>
      <div class="hint">
        配置 Redis Hash `model_score_rolling_params_{model_name}`，用于决定 model score rolling 计算哪些 quantile。
      </div>
      <div class="hint">rolling 输出 key：<span id="model-source-key" class="mono">-</span></div>
      <div class="kv-table" id="model-score-table"></div>
      <div id="model-score-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Return Score Mapping</h2>
        <div class="actions">
          <button id="load-return-mapping" class="secondary">读取</button>
          <button id="reset-return-mapping" class="ghost">恢复默认</button>
          <button id="save-return-mapping">保存</button>
        </div>
      </div>
      <div class="hint">
        配置 `业务动作 -> score_xx` 的映射，写入 Redis Hash `return_model_score_mapping_{venue}`。
      </div>
      <div class="kv-table" id="return-mapping-table"></div>
      <div id="return-mapping-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Return Score Thresholds</h2>
        <div class="actions">
          <button id="load-thresholds" class="secondary">读取</button>
          <button id="sync-thresholds" class="secondary">按模型同步</button>
          <button id="reset-thresholds" class="ghost">示例</button>
          <button id="save-thresholds">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构为 `{"SYMBOL": {"forward_open": "...", ...}}`。保存时会重写 Redis Hash
        `return_model_score_thresholds_{venue}`，并清理旧字段。
      </div>
      <div class="mapping-grid" id="threshold-mapping"></div>
      <textarea id="thresholds-text" class="mono" spellcheck="false"></textarea>
      <div id="thresholds-status" class="status"></div>
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
      const effectiveModelName = (modelName || '').trim();
      return {
        input_services: effectiveModelName ? [`model_output/${effectiveModelName}`] : [],
        output_hash_key: effectiveModelName ? `model_score_rolling_thresholds_${effectiveModelName}` : '',
        refresh_sec: BOOTSTRAP.defaults.model_score_rolling_params?.refresh_sec ?? 30,
        reload_param_sec: BOOTSTRAP.defaults.model_score_rolling_params?.reload_param_sec ?? 3,
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
      document.getElementById('risk-key').textContent =
        `${state.envName}:${state.venue}:${state.venue}:pre_trade_risk_params`;
      document.getElementById('mapping-key').textContent =
        `return_model_score_mapping_${state.venue}`;
      document.getElementById('threshold-key').textContent =
        `return_model_score_thresholds_${state.venue}`;
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
      [...order, ...Object.keys(defaults), ...Object.keys(values || {})].forEach((key) => {
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
        const input = document.createElement('input');
        input.type = 'text';
        const rawValue = values && values[key] !== undefined ? values[key] : (defaults[key] ?? '');
        input.value = valueToInputText(rawValue);
        input.dataset.key = key;
        inputWrap.appendChild(input);

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
      document.querySelectorAll(`#${containerId} input[data-key]`).forEach((input) => {
        out[input.dataset.key] = input.value.trim();
      });
      return out;
    }

    function renderThresholdMapping(values = null) {
      const container = document.getElementById('threshold-mapping');
      container.innerHTML = '';
      const mapping = values || BOOTSTRAP.return_threshold_mapping || {};
      Object.entries(mapping).forEach(([operation, ref]) => {
        const item = document.createElement('div');
        item.className = 'mapping-item';
        item.innerHTML = `<strong>${operation}</strong><span class="mono">${ref}</span>`;
        container.appendChild(item);
      });
      document.getElementById('threshold-example').textContent =
        JSON.stringify(BOOTSTRAP.return_threshold_example || {}, null, 2);
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
        setStatus('model-score-status', `已读取 ${data.key}${suffix}`, 'ok');
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

    async function loadReturnMapping() {
      setStatus('return-mapping-status', '读取中...');
      try {
        const data = await fetchJson(
          `${apiUrl('return-score-mapping')}?exchange=${encodeURIComponent(state.exchange)}`
        );
        buildParamRows(
          'return-mapping-table',
          BOOTSTRAP.defaults.return_threshold_mapping || {},
          BOOTSTRAP.comments.return_threshold_mapping || {},
          BOOTSTRAP.order.return_threshold_operations || [],
          data.values || {}
        );
        renderThresholdMapping(data.values || {});
        setStatus('return-mapping-status', `已读取 ${data.key}`, 'ok');
      } catch (err) {
        setStatus('return-mapping-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveReturnMapping() {
      setStatus('return-mapping-status', '保存中...');
      try {
        const values = collectParamRows('return-mapping-table');
        const data = await fetchJson(apiUrl('return-score-mapping'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, values}),
        });
        renderThresholdMapping(values);
        setStatus(
          'return-mapping-status',
          `已保存 ${data.count} 个映射，清理旧字段 ${data.removed_count || 0}`,
          'ok'
        );
      } catch (err) {
        setStatus('return-mapping-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    function resetReturnMapping() {
      buildParamRows(
        'return-mapping-table',
        BOOTSTRAP.defaults.return_threshold_mapping || {},
        BOOTSTRAP.comments.return_threshold_mapping || {},
        BOOTSTRAP.order.return_threshold_operations || [],
        {}
      );
      renderThresholdMapping(BOOTSTRAP.defaults.return_threshold_mapping || {});
      setStatus('return-mapping-status', '已恢复默认映射，尚未写入 Redis', 'warn');
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
        setStatus('strategy-status', `已读取 ${Object.keys(data.values || {}).length} 个参数`, 'ok');
      } catch (err) {
        setStatus('strategy-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveStrategy() {
      setStatus('strategy-status', '保存中...');
      try {
        const values = collectParamRows('strategy-table');
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
        setStatus('risk-status', `已读取 ${Object.keys(data.values || {}).length} 个参数`, 'ok');
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

    async function loadThresholds() {
      setStatus('thresholds-status', '读取中...');
      try {
        const data = await fetchJson(`${apiUrl('return-score-thresholds')}?exchange=${encodeURIComponent(state.exchange)}`);
        document.getElementById('thresholds-text').value =
          JSON.stringify(data.values || {}, null, 2);
        setStatus(
          'thresholds-status',
          `已读取 ${data.symbol_count || 0} 个 symbol / ${data.field_count || 0} 个字段`,
          'ok'
        );
      } catch (err) {
        setStatus('thresholds-status', `读取失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function saveThresholds() {
      setStatus('thresholds-status', '保存中...');
      let values;
      try {
        values = JSON.parse(document.getElementById('thresholds-text').value || '{}');
      } catch (err) {
        setStatus('thresholds-status', `JSON 解析失败: ${err.message}`, 'err');
        return;
      }
      try {
        const data = await fetchJson(apiUrl('return-score-thresholds'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, values}),
        });
        setStatus(
          'thresholds-status',
          `已保存 ${data.symbol_count} 个 symbol / ${data.field_count} 个字段，清理旧字段 ${data.removed_count}`,
          'ok'
        );
      } catch (err) {
        setStatus('thresholds-status', `保存失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    function resetThresholds() {
      document.getElementById('thresholds-text').value =
        JSON.stringify(BOOTSTRAP.return_threshold_example || {}, null, 2);
      setStatus('thresholds-status', '已填入示例，尚未写入 Redis', 'warn');
    }

    async function syncThresholdsFromModel() {
      const modelName = requireModelName('thresholds-status');
      setStatus('thresholds-status', '同步中...');
      try {
        const mapping = collectParamRows('return-mapping-table');
        const data = await fetchJson(apiUrl('return-score-thresholds/sync'), {
          method: 'POST',
          body: JSON.stringify({exchange: state.exchange, model_name: modelName, mapping}),
        });
        persistModelName(modelName);
        await loadThresholds();
        setStatus(
          'thresholds-status',
          `同步完成: ready=${data.ready_count} synced=${data.synced_symbols} fields=${data.field_count} removed=${data.removed_count}`,
          'ok'
        );
      } catch (err) {
        setStatus('thresholds-status', `同步失败: ${formatError(err)}`, 'err');
        throw err;
      }
    }

    async function loadAll() {
      updateDerivedKeys();
      const tasks = [loadSymbols(), loadStrategy(), loadRisk(), loadReturnMapping(), loadThresholds()];
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
    document.getElementById('load-risk').addEventListener('click', () => loadRisk());
    document.getElementById('save-risk').addEventListener('click', () => saveRisk());
    document.getElementById('reset-risk').addEventListener('click', () => resetRisk());
    document.getElementById('load-model-score').addEventListener('click', () => loadModelScoreParams());
    document.getElementById('save-model-score').addEventListener('click', () => saveModelScoreParams());
    document.getElementById('reset-model-score').addEventListener('click', () => resetModelScoreParams());
    document.getElementById('load-return-mapping').addEventListener('click', () => loadReturnMapping());
    document.getElementById('save-return-mapping').addEventListener('click', () => saveReturnMapping());
    document.getElementById('reset-return-mapping').addEventListener('click', () => resetReturnMapping());
    document.getElementById('load-thresholds').addEventListener('click', () => loadThresholds());
    document.getElementById('sync-thresholds').addEventListener('click', () => syncThresholdsFromModel());
    document.getElementById('save-thresholds').addEventListener('click', () => saveThresholds());
    document.getElementById('reset-thresholds').addEventListener('click', () => resetThresholds());

    initExchangeSelector();
    initModelNameInput();
    renderThresholdMapping();
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
    matched = VENUE_PREFIX_PATTERN.match((model_name or "").strip().lower())
    if not matched:
        raise ValueError(
            "cannot infer venue from model_name prefix; expected prefix like "
            "'binance-futures-...' or 'okex-futures-...'"
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


def make_risk_key(env_name: str, venue: str) -> str:
    return f"{env_name}:{venue}:{venue}:pre_trade_risk_params"


def make_return_threshold_key(venue: str) -> str:
    return f"return_model_score_thresholds_{venue}"


def make_return_mapping_key(venue: str) -> str:
    return f"return_model_score_mapping_{venue}"


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


def sanitize_return_mapping(values: Any) -> Dict[str, str]:
    mapping = sanitize_string_mapping(values)
    out: Dict[str, str] = {}
    for operation in RETURN_THRESHOLD_OPERATIONS:
        score_ref = mapping.get(operation)
        if score_ref:
            out[operation] = score_ref
    return out


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
        if key in {"input_services", "quantiles"}:
            out[key] = maybe_decode_json_string(raw)
        else:
            out[key] = maybe_decode_json_string(raw)
    return out


def build_default_model_score_params(model_name: str) -> Dict[str, Any]:
    validated_model_name = validate_model_name(model_name)
    defaults = {
        "input_services": [f"model_output/{validated_model_name}"],
        "output_hash_key": make_model_score_threshold_source_key(validated_model_name),
    }
    for key, value in DEFAULT_MODEL_SCORE_ROLLING_PARAMS.items():
        if isinstance(value, list):
            defaults[key] = list(value)
        elif isinstance(value, dict):
            defaults[key] = dict(value)
        else:
            defaults[key] = value
    return defaults


def normalize_quantile(q_raw: object) -> Optional[int]:
    try:
        q = float(q_raw)
    except Exception:
        return None

    if q > 1.0:
        q = q / 100.0
    if q < 0.0 or q > 1.0:
        return None
    return int(round(q * 100.0))


def build_score_points(payload: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, float]]]:
    quantiles = payload.get("quantiles")
    thresholds = payload.get("thresholds")
    ready = bool(payload.get("ready", False))

    if not ready or not isinstance(quantiles, list) or not isinstance(thresholds, list):
        return None
    if len(quantiles) != len(thresholds) or not quantiles:
        return None

    symbol = str(payload.get("symbol", "")).strip().upper()
    if not symbol:
        return None

    points: Dict[str, float] = {}
    for q_raw, threshold_raw in zip(quantiles, thresholds):
        percentile = normalize_quantile(q_raw)
        if percentile is None:
            continue
        try:
            threshold = float(threshold_raw)
        except Exception:
            continue
        points[f"score_{percentile}"] = threshold

    if not points:
        return None
    return symbol, points


def format_threshold_value(value: float) -> str:
    return f"{value:.12g}"


def parse_return_threshold_fields(raw_fields: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    result: Dict[str, Dict[str, str]] = {}
    for field, value in raw_fields.items():
        matched = None
        for operation in sorted(RETURN_THRESHOLD_OPERATIONS, key=len, reverse=True):
            suffix = f"_{operation}"
            if field.endswith(suffix):
                symbol = field[: -len(suffix)].rstrip("_").upper()
                if symbol:
                    matched = (symbol, operation)
                break
        if not matched:
            continue
        symbol, operation = matched
        result.setdefault(symbol, {})[operation] = value
    return dict(sorted(result.items()))


def flatten_return_threshold_values(values: Any) -> Dict[str, str]:
    if not isinstance(values, dict):
        raise ValueError("threshold values must be an object")

    flat: Dict[str, str] = {}
    for raw_symbol, raw_ops in values.items():
        symbol = str(raw_symbol).strip().upper()
        if not symbol:
            continue
        if not isinstance(raw_ops, dict):
            raise ValueError(f"threshold row for {symbol} must be an object")
        for operation in RETURN_THRESHOLD_OPERATIONS:
            if operation not in raw_ops:
                continue
            value = raw_ops[operation]
            if value is None or str(value).strip() == "":
                continue
            flat[f"{symbol}_{operation}"] = str(value).strip()
    return flat


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
            "return_threshold_mapping": RETURN_THRESHOLD_MAPPING,
        },
        "comments": {
            "strategy_params": STRATEGY_PARAM_COMMENTS,
            "risk_params": RISK_PARAM_COMMENTS,
            "model_score_rolling_params": MODEL_SCORE_PARAM_COMMENTS,
            "return_threshold_mapping": RETURN_MAPPING_COMMENTS,
        },
        "order": {
            "strategy_params": STRATEGY_PARAM_ORDER,
            "risk_params": RISK_PARAM_ORDER,
            "model_score_rolling_params": MODEL_SCORE_PARAM_ORDER,
            "return_threshold_operations": RETURN_THRESHOLD_OPERATIONS,
        },
        "return_threshold_mapping": RETURN_THRESHOLD_MAPPING,
        "return_threshold_example": RETURN_THRESHOLD_EXAMPLE,
        "keys": {
            "symbol": make_symbol_key(venue),
            "strategy": make_strategy_key(venue),
            "risk": make_risk_key(env_name, venue),
            "return_mapping": make_return_mapping_key(venue),
            "return_threshold": make_return_threshold_key(venue),
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
        values = decode_hash(self.redis().hgetall(key))
        return {"key": key, "values": values}

    def write_strategy_params(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_strategy_key(venue)
        return self._write_hash_replace(key, values)

    def read_risk_params(self, venue: str) -> Dict[str, Any]:
        key = make_risk_key(self._config.env_name, venue)
        values = decode_hash(self.redis().hgetall(key))
        return {"key": key, "values": values}

    def write_risk_params(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_risk_key(self._config.env_name, venue)
        return self._write_hash_replace(key, values)

    def read_return_thresholds(self, venue: str) -> Dict[str, Any]:
        key = make_return_threshold_key(venue)
        raw = decode_hash(self.redis().hgetall(key))
        values = parse_return_threshold_fields(raw)
        return {
            "key": key,
            "values": values,
            "field_count": len(raw),
            "symbol_count": len(values),
        }

    def write_return_thresholds(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_return_threshold_key(venue)
        redis = self.redis()
        new_fields = flatten_return_threshold_values(values)
        existing = {
            item.decode("utf-8", "ignore") if isinstance(item, bytes) else str(item)
            for item in redis.hkeys(key)
        }
        stale_fields = sorted(existing - set(new_fields.keys()))

        pipe = redis.pipeline()
        if new_fields:
            pipe.hset(key, mapping=new_fields)
        if stale_fields:
            pipe.hdel(key, *stale_fields)
        pipe.execute()

        symbol_count = len({field.rsplit("_", 1)[0] for field in new_fields.keys()})
        return {
            "key": key,
            "field_count": len(new_fields),
            "symbol_count": symbol_count,
            "removed_count": len(stale_fields),
            "removed_fields": stale_fields,
        }

    def read_return_mapping(self, venue: str) -> Dict[str, Any]:
        key = make_return_mapping_key(venue)
        raw = decode_hash(self.redis().hgetall(key))
        values = dict(RETURN_THRESHOLD_MAPPING)
        values.update(raw)
        return {"key": key, "values": values}

    def write_return_mapping(self, venue: str, values: Any) -> Dict[str, Any]:
        key = make_return_mapping_key(venue)
        mapping = sanitize_return_mapping(values)
        if not mapping:
            raise ValueError("return score mapping cannot be empty")
        return self._replace_hash(key, mapping)

    def read_model_score_rolling_params(self, model_name: str) -> Dict[str, Any]:
        key = make_model_score_params_key(model_name)
        raw = decode_hash(self.redis().hgetall(key))
        if raw:
            values = deserialize_model_score_params(raw)
        else:
            values = build_default_model_score_params(model_name)
        return {
            "key": key,
            "source_key": make_model_score_threshold_source_key(model_name),
            "model_name": model_name,
            "model_venue": infer_venue_from_model_name(model_name),
            "values": values,
        }

    def write_model_score_rolling_params(self, model_name: str, values: Any) -> Dict[str, Any]:
        key = make_model_score_params_key(model_name)
        mapping = serialize_model_score_params(values)
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

    def sync_return_thresholds_from_model(
        self,
        venue: str,
        model_name: str,
        mapping_override: Optional[Any] = None,
    ) -> Dict[str, Any]:
        model_venue = infer_venue_from_model_name(model_name)
        if model_venue != venue:
            raise ValueError(
                f"model_name venue mismatch: model={model_venue}, selected={venue}"
            )

        source_key = make_model_score_threshold_source_key(model_name)
        target_key = make_return_threshold_key(venue)
        if mapping_override is None:
            mapping = self.read_return_mapping(venue)["values"]
        else:
            mapping = sanitize_return_mapping(mapping_override)
        if not mapping:
            raise ValueError("return score mapping cannot be empty")

        redis = self.redis()
        raw = decode_hash(redis.hgetall(source_key))
        output_fields: Dict[str, str] = {}
        total = 0
        ready_symbols = 0
        synced_symbols = 0

        for field_symbol, text in raw.items():
            total += 1
            try:
                payload = json.loads(text)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue

            parsed = build_score_points(payload)
            if parsed is None:
                continue

            ready_symbols += 1
            payload_symbol, points = parsed
            symbol = payload_symbol or field_symbol.upper()

            symbol_fields: Dict[str, str] = {}
            for operation in RETURN_THRESHOLD_OPERATIONS:
                score_ref = mapping.get(operation)
                if not score_ref:
                    symbol_fields = {}
                    break
                value = points.get(score_ref)
                if value is None:
                    symbol_fields = {}
                    break
                symbol_fields[f"{symbol}_{operation}"] = format_threshold_value(value)

            if not symbol_fields:
                continue

            output_fields.update(symbol_fields)
            synced_symbols += 1

        existing_fields = {
            item.decode("utf-8", "ignore") if isinstance(item, bytes) else str(item)
            for item in redis.hkeys(target_key)
        }
        stale_fields = sorted(existing_fields - set(output_fields.keys()))

        pipe = redis.pipeline()
        if output_fields:
            pipe.hset(target_key, mapping=output_fields)
        if stale_fields:
            pipe.hdel(target_key, *stale_fields)
        pipe.execute()

        return {
            "model_name": model_name,
            "model_venue": model_venue,
            "source_key": source_key,
            "target_key": target_key,
            "mapping": mapping,
            "total_count": total,
            "ready_count": ready_symbols,
            "synced_symbols": synced_symbols,
            "field_count": len(output_fields),
            "removed_count": len(stale_fields),
            "removed_fields": stale_fields,
        }


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

                if parsed.path == "/api/risk-params":
                    self._send_json(
                        200,
                        {"ok": True, "exchange": exchange, "venue": venue, **store.read_risk_params(venue)},
                    )
                    return

                if parsed.path == "/api/return-score-thresholds":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "exchange": exchange,
                            "venue": venue,
                            "mapping": RETURN_THRESHOLD_MAPPING,
                            **store.read_return_thresholds(venue),
                        },
                    )
                    return

                if parsed.path == "/api/return-score-mapping":
                    self._send_json(
                        200,
                        {
                            "ok": True,
                            "exchange": exchange,
                            "venue": venue,
                            **store.read_return_mapping(venue),
                        },
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

                if parsed.path == "/api/risk-params":
                    result = store.write_risk_params(venue, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/return-score-thresholds":
                    result = store.write_return_thresholds(venue, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/return-score-mapping":
                    result = store.write_return_mapping(venue, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/model-score-rolling-params":
                    model_name = resolve_model_name({}, payload)
                    result = store.write_model_score_rolling_params(model_name, payload.get("values"))
                    self._send_json(200, {"ok": True, "exchange": exchange, "venue": venue, **result})
                    return

                if parsed.path == "/api/return-score-thresholds/sync":
                    model_name = resolve_model_name({}, payload)
                    result = store.sync_return_thresholds_from_model(
                        venue,
                        model_name,
                        payload.get("mapping"),
                    )
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
