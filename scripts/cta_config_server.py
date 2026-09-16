#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""CTA 专用 config server（与 intra_config_server 独立）。

只暴露 cta env 真正消费的配置：
  - CTA 信号           -> {env}:cta_rules                        (单 JSON 对象：model/分位/方向)
  - Symbol Lists       -> {env}:cta_trade_symbols:{exchange}     (单一交易宇宙，无正反概念)
                         {env}:cta_dump_symbols:{exchange}       (平仓/禁用列表)
                         + 镜像 {env}:intra_bwd_trade_symbols:{exchange} (pre_trade 借贷白名单)
  - Strategy Params    -> cta_strategy_params_{open}_{hedge}     (hash：网格执行参数)
  - Spread Thresholds  -> cta_spread_thresholds_config_{o}_{h}   (JSON mapping：阈值字段→rolling 分位)
                         cta_spread_thresholds_{o}_{h}           (hash：同步物化的 per-symbol 阈值)
  - Risk Params        -> {env}:{open}:{hedge}:pre_trade_risk_params (hash, pre_trade 读取)

schema 常量与通用 helper 从 intra_config_server import（单一来源），
校验复用 sync_cta_rules（与 Rust cta_config.rs 同规则）。

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
import sync_cta_rules  # noqa: E402  (cta 信号/执行字段校验，与 Rust loader 同规则)

spread_sync = None  # noqa: E402
try:
    import sync_intra_spread_thresholds as spread_sync  # noqa: E402
except Exception:
    pass

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
      <a href="#cta-signal">CTA 信号</a>
      <a href="#symbol-lists">Symbol Lists</a>
      <a href="#spread-thresholds">Spread Thresholds</a>
      <a href="#strategy-params">Strategy Params</a>
      <a href="#risk-params">Risk Params</a>
    </div>

    <section id="cta-signal" class="panel">
      <div class="section-header">
        <h2>CTA 信号 <span class="badge" id="signal-key"></span></h2>
        <div class="actions">
          <button id="signal-load" class="secondary">读取</button>
          <button id="signal-save">保存</button>
          <button id="signal-default" class="ghost">默认</button>
        </div>
      </div>
      <div class="hint">
        key: <code>{env}:cta_rules</code>（单 JSON 对象，trade_signal 60s 热加载）。
        配置哪个 model、分位阈值、多空方向；网格执行参数（档位/单笔名义/TP/trailing）在
        <a href="#strategy-params">Strategy Params</a>。
      </div>
      <div id="signal-table" class="kv-table"></div>
      <div id="signal-status" class="status"></div>
    </section>

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
        全部为网格报单执行参数（open_offsets 档位、单笔名义、挂单TTL、持仓上限、TP/trailing、最长持仓）。
      </div>
      <div id="strategy-table" class="kv-table"></div>
      <div id="strategy-status" class="status"></div>
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
      <div class="hint">
        mapping 存 <code>cta_spread_thresholds_config_{open}_{hedge}</code>；
        「同步阈值」按 symbol 列表从 <code>rolling_metrics_thresholds_{open}_{hedge}</code>
        取分位值，物化到 <code>cta_spread_thresholds_{open}_{hedge}</code>。
        trade_signal 与 intra/cross 同一机制：读 mapping + rolling 发布值解析 per-symbol
        价差阈值（60s 热加载）。forward=开多（买现货卖期货），backward=开空。
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
      const panel = containerId.replace(/-table$/, '');
      const boolKeys = new Set((BOOTSTRAP.bool_params && BOOTSTRAP.bool_params[panel]) || []);
      const selectDefs = (BOOTSTRAP.selects && BOOTSTRAP.selects[panel]) || {};
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
        if (selectDefs[key]) {
          input = document.createElement('select');
          const options = [...selectDefs[key]];
          if (rawValue !== '' && !options.includes(String(rawValue))) options.unshift(String(rawValue));
          options.forEach(value => {
            const option = document.createElement('option');
            option.value = value;
            option.textContent = value;
            input.appendChild(option);
          });
          input.value = String(rawValue);
        } else if (boolKeys.has(key) && isBooleanParamValue(rawValue)) {
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
        // hash 未配置（404）时直接渲染默认值，便于一次性编辑后保存；
        // 其它错误（网络/500）同样落到默认表，避免面板不可用。
        buildParamRows(`${name}-table`, BOOTSTRAP.defaults[`${name}_params`] || {}, BOOTSTRAP.comments[`${name}_params`] || {}, BOOTSTRAP.order[name] || [], {});
        const notConfigured = String(err).includes('404');
        setStatus(
          `${name}-status`,
          notConfigured ? '尚未配置，已载入默认值（修改后点保存写入）' : `读取失败: ${err}（已载入默认值）`,
          false
        );
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

    // ---- CTA 信号（{env}:cta_rules 单对象）----
    function renderSignalRows(values) {
      buildParamRows(
        'signal-table',
        BOOTSTRAP.defaults.signal_params || {},
        BOOTSTRAP.comments.signal_params || {},
        BOOTSTRAP.order.signal || [],
        values || {}
      );
    }

    async function loadSignalConfig() {
      setStatus('signal-status', '读取中...');
      try {
        const data = await fetchJson(apiUrl('cta-rules'));
        document.getElementById('signal-key').textContent = data.key || '';
        renderSignalRows(data.values || {});
        setStatus('signal-status', '读取完成');
      } catch (err) {
        // key 未配置（404）或读失败都落到默认表，编辑后保存即可创建。
        renderSignalRows({});
        const notConfigured = String(err).includes('404');
        setStatus(
          'signal-status',
          notConfigured ? '尚未配置，已载入默认值（修改后点保存写入）' : `读取失败: ${err}（已载入默认值）`,
          false
        );
      }
    }

    async function saveSignalConfig() {
      setStatus('signal-status', '保存中...');
      try {
        const data = await fetchJson(apiUrl('cta-rules'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ values: collectParamValues('signal-table') }),
        });
        setStatus('signal-status', `保存成功 -> ${data.key}`);
      } catch (err) {
        setStatus('signal-status', `保存失败: ${err}`, false);
      }
    }

    const applySignalDefaults = () => {
      renderSignalRows({});
      setStatus('signal-status', '已载入默认值（未保存）');
    };

    // ---- Spread Threshold Mapping（cta_spread_thresholds_config_*）----
    async function loadSpreadMapping() {
      setStatus('spread-status', '读取中...');
      try {
        const data = await fetchJson(apiUrl('spread-thresholds'));
        buildParamRows('spread-table', BOOTSTRAP.defaults.spread_mapping || {}, BOOTSTRAP.comments.spread_mapping || {}, BOOTSTRAP.order.spread || [], data.values || {});
        setStatus('spread-status', '读取完成');
      } catch (err) {
        setStatus('spread-status', `读取失败: ${err}`, false);
      }
    }

    async function saveSpreadMapping() {
      setStatus('spread-status', '保存中...');
      try {
        const data = await fetchJson(apiUrl('spread-thresholds'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ values: collectParamValues('spread-table') }),
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
        const data = await fetchJson(apiUrl('spread-thresholds/sync'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ symbol, mapping: collectParamValues('spread-table') }),
        });
        const changed = data.changed != null ? `, changed=${data.changed}` : '';
        const warnings = data.warnings && data.warnings.length ? ` ⚠️ ${data.warnings.join('; ')}` : '';
        const missing = data.missing_symbols && data.missing_symbols.length ? ` missing=${data.missing_symbols.length}` : '';
        setStatus('spread-status', `同步完成: ${data.written || 0} 字段${changed}${missing}${warnings}`, !warnings);
      } catch (err) {
        setStatus('spread-status', `同步失败: ${err}`, false);
      }
    }

    async function reloadAll() {
      await loadSignalConfig();
      await loadSymbolLists();
      await loadStrategyParams();
      await loadRiskParams();
      await loadSpreadMapping();
    }

    applyFixedContext();
    document.getElementById('signal-load').addEventListener('click', loadSignalConfig);
    document.getElementById('signal-save').addEventListener('click', saveSignalConfig);
    document.getElementById('signal-default').addEventListener('click', applySignalDefaults);
    document.getElementById('sym-load').addEventListener('click', loadSymbolLists);
    document.getElementById('sym-save').addEventListener('click', saveSymbolLists);
    document.getElementById('strategy-load').addEventListener('click', loadStrategyParams);
    document.getElementById('strategy-save').addEventListener('click', saveStrategyParams);
    document.getElementById('strategy-default').addEventListener('click', applyStrategyDefaults);
    document.getElementById('risk-load').addEventListener('click', loadRiskParams);
    document.getElementById('risk-save').addEventListener('click', saveRiskParams);
    document.getElementById('risk-default').addEventListener('click', applyRiskDefaults);
    document.getElementById('spread-config-load').addEventListener('click', loadSpreadMapping);
    document.getElementById('spread-config-save').addEventListener('click', saveSpreadMapping);
    document.getElementById('spread-sync').addEventListener('click', syncSpreadThresholds);
    document.getElementById('reload-all').addEventListener('click', reloadAll);
    buildParamRows('spread-table', BOOTSTRAP.defaults.spread_mapping || {}, BOOTSTRAP.comments.spread_mapping || {}, BOOTSTRAP.order.spread || [], {});
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


# ---- Spread Thresholds（与 intra/cross 同一机制）----
# mapping 存 cta_spread_thresholds_config_{open}_{hedge}（JSON），
# trade_signal 的 reload_spread_thresholds_from_rolling 读它 + rolling_metrics
# 发布值解析 per-symbol 阈值 -> SpreadFactor。「同步阈值」同时把解析结果物化到
# cta_spread_thresholds_{open}_{hedge} hash。
# 字段名沿用共享 schema：forward=开多（买现货卖期货）/backward=开空，
# mm=maker 挂单阈值 / mt=taker 阈值（apply 需要 mm+mt 成对存在）。
# 默认值按 notebook 的 spread overlay 语义：开多要求 spread<q30、开空要求
# spread>q70、越过 q50 撤同向未成交。依赖 rolling_metrics 发布 50 分位
#（rolling_metrics_params_binance-margin_binance-futures 已给三个因子加 50）。
_CTA_SPREAD_DEFAULTS: Dict[str, str] = {
    "forward_open_mm": "spread_30",
    "forward_open_mt": "bidask_30",
    "forward_cancel_mm": "spread_50",
    "forward_cancel_mt": "bidask_50",
    "backward_open_mm": "spread_70",
    "backward_open_mt": "askbid_70",
    "backward_cancel_mm": "spread_50",
    "backward_cancel_mt": "askbid_50",
}
_CTA_SPREAD_ORDER: List[str] = list(_CTA_SPREAD_DEFAULTS.keys())
_CTA_SPREAD_COMMENTS: Dict[str, str] = {
    "forward_open_mm": "开多 maker 阈值：spread < 该分位值才挂",
    "forward_open_mt": "开多 taker 阈值（bidask 分位）",
    "forward_cancel_mm": "开多挂单撤离：spread > 该分位值",
    "forward_cancel_mt": "开多 taker 撤离（bidask 分位）",
    "backward_open_mm": "开空 maker 阈值：spread > 该分位值才挂",
    "backward_open_mt": "开空 taker 阈值（askbid 分位）",
    "backward_cancel_mm": "开空挂单撤离：spread < 该分位值",
    "backward_cancel_mt": "开空 taker 撤离（askbid 分位）",
}


def _cta_load_symbol_lists(
    rds, key_suffix: str, env_name: str, open_venue: str, hedge_venue: str
) -> List[str]:
    """cta 单一交易宇宙：trade_symbols ∪ dump_symbols（无正反列表）。"""
    symbols: set = set()
    for name in ("trade_symbols", "dump_symbols"):
        for sym in base.read_symbol_list(
            rds, symbol_list_key(env_name, name, key_suffix)
        ):
            s = str(sym).strip().upper()
            if s:
                symbols.add(s)
    return sorted(symbols)


def sync_spread_thresholds(
    rds,
    open_venue: str,
    hedge_venue: str,
    key_suffix: str,
    mapping: Optional[Dict[str, str]] = None,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    if spread_sync is None:
        raise RuntimeError("sync_intra_spread_thresholds.py not available")
    if not mapping:
        # 裸调 sync（不带 mapping）时优先用已持久化的 mapping，
        # 否则回落代码默认值——避免与已存 config 漂移。
        mapping = base.read_threshold_mapping(
            rds, "spread", open_venue, hedge_venue, _CTA_SPREAD_DEFAULTS
        )
    return base.sync_thresholds(
        rds,
        "spread",
        current_env_name(),
        open_venue,
        hedge_venue,
        key_suffix,
        mapping,
        _CTA_SPREAD_DEFAULTS,
        _cta_load_symbol_lists,
        spread_sync.read_rolling_metrics,
        spread_sync.normalize_for_rolling,
        spread_sync.extract_quantile_value,
        symbol,
    )


# cta 信号配置（{env}:cta_rules 单对象）的字段默认值/注释/顺序——
# 「CTA 信号」面板按此渲染扁平参数行。执行/网格字段不在这里，归 strategy hash。
_CTA_SIGNAL_DEFAULTS: Dict[str, Any] = {
    "model_service": "intra-binance-futures-1m-baseline_035",
    "enabled": True,
    "trade_sides": "both",
    "application": "each_bar",
    "long_quantile": 0.9,
    "short_quantile": 0.1,
    "cooldown_seconds": 0,
}
_CTA_SIGNAL_COMMENTS: Dict[str, str] = {
    "model_service": "因子信号流 service（model_output/<service>，必填），默认 intra-binance-futures-1m-baseline_035",
    "enabled": "false = 不产生任何信号（配置保留）",
    "trade_sides": "方向生效：both 多空都做 / long 只多 / short 只空",
    "application": "each_bar 每根 bar 评估 / on_change 仅方向翻转时",
    "long_quantile": "score 分位 > 此值做多",
    "short_quantile": "score 分位 < 此值做空（须 < long_quantile）",
    "cooldown_seconds": "同一 symbol 两次开仓最小间隔（秒），0=不限制",
}
_CTA_SIGNAL_ORDER: List[str] = list(_CTA_SIGNAL_DEFAULTS.keys())
_CTA_SIGNAL_SELECTS: Dict[str, List[str]] = {
    "trade_sides": ["both", "long", "short"],
    "application": ["each_bar", "on_change"],
}
_CTA_SIGNAL_BOOLS: List[str] = ["enabled"]

# strategy_params hash 只承载 cta 执行/网格参数（sync_cta_rules.EXEC_FIELD_TYPES
# 全集）——Rust CtaExecOverrides 加载时覆盖到规则上，与 cta_rules 对象同名字段兼容。
_CTA_EXEC_COMMENTS: Dict[str, str] = {
    "order_notional_usdt": "网格单档挂单名义（USDT）",
    "open_offsets": "网格档位价格偏移，JSON 数组或逗号分隔（0..0.01），档数=个数",
    "open_ttl_seconds": "开仓挂单 TTL（秒）",
    "max_position_notional_usdt": "单向名义上限（USDT），须 ≥ 档数×单档名义",
    "take_profit": "swap 腿 maker 止盈偏移（价格分数）；0=不挂止盈",
    "reward_risk_ratio": "止盈/止损比：stop_loss = take_profit / rr",
    "trailing_stop_enabled": "trailing stop 开关（true/false）",
    "trailing_stop_trigger_step": "trailing 触发步进（价格分数）",
    "trailing_stop_move_step": "trailing 移动步进（价格分数）",
    "max_holding_seconds": "最长持仓（秒），0=不限制",
}
# CTA strategy hash 只承载执行/网格参数（sync_cta_rules.EXEC_FIELD_TYPES）。
# intra 共享链路里被其它 arb 模式消费的旋钮对 cta 均为死参数，不暴露：
#   signal_cooldown      —— 只喂 FundingArb 的 cooldown sweep worker；
#                          cta 的同语义旋钮是信号对象里的 cooldown_seconds
#   open_order_timeout   —— open ctx 的 TTL 兜底；cta 网格单 TTL 用
#                          per-rule open_ttl_seconds
#   hedge_timeout        —— intra/xarb 对冲腿成交时限；cta 对冲是 entry 锚定
#                          per-lot maker TP，生命周期由 trailing/max_holding 管
#   enable_tlen_cancel / tlen_cancel_freq_ms —— tlen 衰减撤单；
#                          CtaShell 对 cancel trigger/candidate 显式 no-op
_CTA_STRATEGY_KEYS: Tuple[str, ...] = tuple(sync_cta_rules.EXEC_FIELD_TYPES.keys())


def _cta_strategy_schema() -> Tuple[Dict[str, Any], Dict[str, str], List[str]]:
    exec_defaults = {
        k: v for k, v in sync_cta_rules.EXEC_FIELD_DEFAULTS.items()
    }
    exec_defaults["open_offsets"] = json.dumps(
        sync_cta_rules.EXEC_FIELD_DEFAULTS["open_offsets"], separators=(",", ":")
    )
    exec_defaults["trailing_stop_enabled"] = "true"
    defaults = dict(exec_defaults)
    comments = dict(_CTA_EXEC_COMMENTS)
    order = list(_CTA_STRATEGY_KEYS)
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
        "defaults": {
            "signal_params": dict(_CTA_SIGNAL_DEFAULTS),
            "strategy_params": strategy_defaults,
            "risk_params": dict(base.DEFAULT_RISK_PARAMS),
            "spread_mapping": dict(_CTA_SPREAD_DEFAULTS),
        },
        "comments": {
            "signal_params": dict(_CTA_SIGNAL_COMMENTS),
            "strategy_params": strategy_comments,
            "risk_params": dict(base.RISK_PARAM_COMMENTS),
            "spread_mapping": dict(_CTA_SPREAD_COMMENTS),
        },
        "order": {
            "signal": _CTA_SIGNAL_ORDER,
            "strategy": strategy_order,
            "risk": base.RISK_PARAM_ORDER,
            "spread": _CTA_SPREAD_ORDER,
        },
        "selects": {"signal": _CTA_SIGNAL_SELECTS},
        "bool_params": {
            "signal": _CTA_SIGNAL_BOOLS,
            "strategy": ["trailing_stop_enabled"],
        },
    }
    return INDEX_HTML_TEMPLATE.replace(
        "__BOOTSTRAP__", json.dumps(bootstrap, ensure_ascii=False)
    )


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

        if parsed.path == "/api/spread-thresholds":
            try:
                open_venue, hedge_venue = self._fixed_context()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = base.threshold_mapping_key("spread", open_venue, hedge_venue)
            values = base.read_threshold_mapping(
                self.server.context.redis_client,
                "spread",
                open_venue,
                hedge_venue,
                _CTA_SPREAD_DEFAULTS,
            )
            if not values:
                values = base.normalize_threshold_mapping(_CTA_SPREAD_DEFAULTS)
            self._send_json(
                200, {"key": key, "count": len(values), "values": values}
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
            st_defaults, st_comments, st_order = _cta_strategy_schema()
            values, stale_values = base.filter_mapping_by_schema(
                raw_values, st_defaults, st_comments, st_order
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
            key = sync_cta_rules.cta_rules_key(env_name)
            raw = self.server.context.redis_client.get(key)
            if raw is None:
                self._send_error(404, f"cta signal config not found: {key}")
                return
            text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            try:
                doc = json.loads(text)
            except Exception as exc:
                self._send_error(500, f"{key} 不是合法 JSON: {exc}")
                return
            # 兼容旧数组格式：取第一条；空数组视为未配置。
            if isinstance(doc, list):
                doc = doc[0] if doc and isinstance(doc[0], dict) else None
            if not isinstance(doc, dict) or not doc:
                self._send_error(404, f"cta signal config empty: {key}")
                return
            values = {
                k: doc[k]
                for k in list(sync_cta_rules.SIGNAL_FIELD_TYPES) + ["rule_id"]
                if k in doc
            }
            self._send_json(200, {"key": key, "exists": True, "values": values})
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
            # 执行/网格字段与 Rust CtaRule::validate 同规则校验并规范化
            # （open_offsets 统一为 JSON 数组字符串）。
            exec_values = {
                k: mapping[k] for k in sync_cta_rules.EXEC_FIELD_TYPES if k in mapping
            }
            norm_exec, exec_errors = sync_cta_rules.parse_exec_params(exec_values)
            if exec_errors:
                self._send_error(400, "exec params 校验失败: " + "; ".join(exec_errors))
                return
            mapping.update(norm_exec)
            result = base.replace_hash(self.server.context.redis_client, key, mapping)
            self._send_json(200, result)
            return

        if parsed.path == "/api/spread-thresholds":
            try:
                open_venue, hedge_venue = self._fixed_context()
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            mapping = base.normalize_threshold_mapping(values)
            if not mapping:
                self._send_error(400, "mapping is empty")
                return
            key = base.threshold_mapping_key("spread", open_venue, hedge_venue)
            written = base.write_threshold_mapping(
                self.server.context.redis_client,
                "spread",
                open_venue,
                hedge_venue,
                mapping,
            )
            self._send_json(200, {"key": key, "count": written})
            return

        if parsed.path == "/api/spread-thresholds/sync":
            try:
                open_venue, hedge_venue = self._fixed_context()
                key_suffix = base.make_key_suffix(open_venue, hedge_venue)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            symbol = payload.get("symbol")
            mapping = payload.get("mapping") if isinstance(payload, dict) else None
            try:
                result = sync_spread_thresholds(
                    self.server.context.redis_client,
                    open_venue,
                    hedge_venue,
                    key_suffix,
                    mapping,
                    symbol,
                )
            except Exception as exc:
                self._send_error(500, f"sync failed: {exc}")
                return
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
            values = payload.get("values")
            if not isinstance(values, dict):
                self._send_error(400, "values must be an object of signal fields")
                return
            config, errors = sync_cta_rules.parse_signal_config(values)
            if errors:
                self._send_error(400, "cta signal 校验失败: " + "; ".join(errors))
                return
            key = sync_cta_rules.cta_rules_key(env_name)
            body = json.dumps(config, ensure_ascii=False, separators=(",", ":"))
            try:
                self.server.context.redis_client.set(key, body)
            except Exception as exc:
                self._send_error(500, f"redis write failed: {exc}")
                return
            print(f"[cta-rules][POST] env={env_name} key={key} fields={len(config)}")
            sys.stdout.flush()
            self._send_json(200, {"key": key, "count": len(config), "bytes": len(body)})
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
