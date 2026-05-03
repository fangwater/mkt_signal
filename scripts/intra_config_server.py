#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
INTRA 配置服务器（intra_config_server）
-------------------------------------
同所期现（intra-exchange spot/futures arb）的配置中心。

提供页面/接口查看和编辑：
- symbol lists
- rolling metrics params
- funding thresholds (premium_rate)
- strategy params
- risk params
- spread thresholds mapping (可从 rolling_metrics 同步)

部署目录约定：<exchange>-intra-<tag>
默认 venues：(<exchange>-margin, <exchange>-futures)
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
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INTRA_SCRIPT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "intra_scripts"))
INTRA_RE = re.compile(r"^([a-z0-9]+)[-_]intra([_-].*)?$")

# Per-symbol overrides 面板（amount_u / max_pos_u / hedge_offset_limits）：
# 三 arb config_server 共用 helper，这里只负责 import + 在 HTML / 路由里挂接。
sys.path.insert(0, SCRIPT_DIR)
import arb_per_symbol_overrides as ps_overrides  # noqa: E402

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
if os.path.isdir(INTRA_SCRIPT_DIR):
    sys.path.insert(0, INTRA_SCRIPT_DIR)


def infer_dir_prefix_from_cwd() -> Optional[str]:
    name = os.path.basename(os.getcwd()).strip().lower()
    return name or None


def normalize_exchange(exchange: str) -> str:
    ex = exchange.strip().lower()
    return "okex" if ex == "okx" else ex


def exchange_from_venue(venue: str) -> Optional[str]:
    raw = (venue or "").strip().lower()
    if not raw or "-" not in raw:
        return None
    exchange = normalize_exchange(raw.split("-", 1)[0])
    if exchange not in SUPPORTED_EXCHANGES:
        return None
    return exchange


def infer_exchange_from_name(name: str) -> Optional[str]:
    """同所期现：从 <exchange>-intra-<tag> 推单一 exchange。"""
    matched = INTRA_RE.match((name or "").strip().lower())
    if not matched:
        return None
    exchange = normalize_exchange(matched.group(1))
    if exchange not in SUPPORTED_EXCHANGES:
        return None
    return exchange


def infer_pair_from_name(name: str) -> Optional[Tuple[str, str]]:
    """兼容旧调用方：返回 (exchange, exchange)（同所，两个 exchange 相同）。"""
    exchange = infer_exchange_from_name(name)
    if not exchange:
        return None
    return exchange, exchange


def infer_default_venues_from_name(name: str) -> Optional[Tuple[str, str]]:
    exchange = infer_exchange_from_name(name)
    if not exchange:
        return None
    return f"{exchange}-margin", f"{exchange}-futures"


def infer_default_venues_from_cwd() -> Optional[Tuple[str, str]]:
    return infer_default_venues_from_name(os.path.basename(os.getcwd()))


def build_risk_params_key(open_venue: str, hedge_venue: str) -> str:
    dir_prefix = infer_dir_prefix_from_cwd()
    if dir_prefix:
        return f"{dir_prefix}:{open_venue}:{hedge_venue}:pre_trade_risk_params"
    return f"{open_venue}:{hedge_venue}:pre_trade_risk_params"


def venue_kind(venue: str) -> str:
    raw = (venue or "").strip().lower()
    if "-" not in raw:
        return ""
    return raw.split("-", 1)[1]


def attached_vol_source_key_for_venue(venue: str) -> str:
    exchange = normalize_exchange((venue or "").split("-", 1)[0])
    return f"rolling_metrics_params_{exchange}-margin_{exchange}-futures"


def attached_vol_factor_name_for_venue(venue: str) -> str:
    normalized = (venue or "").strip().lower()
    return "hedge_vol" if normalized.endswith("-futures") else "open_vol"


def normalize_percentile_text(raw: Any) -> Tuple[float, str]:
    text = str(raw).strip()
    if not text:
        raise ValueError("percentile is required")
    try:
        value = float(text)
    except Exception as exc:
        raise ValueError(f"invalid percentile: {text}") from exc
    if 0.0 <= value <= 1.0:
        value *= 100.0
    if not (0.0 <= value <= 100.0):
        raise ValueError(f"percentile out of range: {text}")
    rounded = round(value)
    if abs(value - rounded) < 1e-9:
        return float(rounded), str(int(rounded))
    return value, f"{value:.12g}"


def preview_open_volatility_source(rds, venue: str, percentile_raw: Any) -> Dict[str, Any]:
    percentile_value, percentile_text = normalize_percentile_text(percentile_raw)
    source_key = attached_vol_source_key_for_venue(venue)
    factor_name = attached_vol_factor_name_for_venue(venue)
    raw_values = read_hash(rds, source_key)
    if not raw_values:
        return {
            "venue": venue,
            "source_key": source_key,
            "factor": factor_name,
            "percentile": percentile_value,
            "percentile_text": percentile_text,
            "exists": False,
            "will_modify": True,
            "modification_detail": "source hash missing",
            "current_quantiles": [],
        }

    factors_raw = raw_values.get("factors", "").strip()
    if not factors_raw:
        return {
            "venue": venue,
            "source_key": source_key,
            "factor": factor_name,
            "percentile": percentile_value,
            "percentile_text": percentile_text,
            "exists": False,
            "will_modify": True,
            "modification_detail": "factors missing",
            "current_quantiles": [],
        }

    try:
        factors = json.loads(factors_raw)
    except Exception as exc:
        raise ValueError(f"invalid factors json in {source_key}: {exc}") from exc
    if not isinstance(factors, dict):
        raise ValueError(f"invalid factors object in {source_key}")

    factor_cfg = factors.get(factor_name)
    if not isinstance(factor_cfg, dict):
        return {
            "venue": venue,
            "source_key": source_key,
            "factor": factor_name,
            "percentile": percentile_value,
            "percentile_text": percentile_text,
            "exists": False,
            "will_modify": True,
            "modification_detail": f"{factor_name} missing",
            "current_quantiles": [],
        }

    quantiles_raw = factor_cfg.get("quantiles")
    quantiles_list = quantiles_raw if isinstance(quantiles_raw, list) else []
    normalized_quantiles: List[str] = []
    requested_exists = False
    for item in quantiles_list:
        try:
            value = float(item)
        except Exception:
            continue
        text = str(int(round(value))) if abs(value - round(value)) < 1e-9 else f"{value:.12g}"
        normalized_quantiles.append(text)
        if abs(value - percentile_value) < 1e-9:
            requested_exists = True

    will_trim = len(quantiles_list) > 8
    will_modify = (not requested_exists) or will_trim
    if not requested_exists:
        modification_detail = f"{factor_name}_{percentile_text} missing"
        if len(quantiles_list) >= 8:
            modification_detail += ", append then trim oldest"
    elif will_trim:
        modification_detail = "quantiles exceed limit, will trim oldest"
    else:
        modification_detail = ""

    return {
        "venue": venue,
        "source_key": source_key,
        "factor": factor_name,
        "percentile": percentile_value,
        "percentile_text": percentile_text,
        "exists": requested_exists,
        "will_modify": will_modify,
        "modification_detail": modification_detail,
        "current_quantiles": normalized_quantiles,
    }


def funding_thresholds_applicable(open_venue: Optional[str], hedge_venue: Optional[str]) -> bool:
    return True

try:
    import sync_intra_risk_params as risk_defaults

    DEFAULT_RISK_PARAMS = dict(risk_defaults.RISK_PARAMS)
    RISK_PARAM_COMMENTS = dict(risk_defaults.PARAM_COMMENTS)
    RISK_PARAM_ORDER = list(getattr(risk_defaults, "PARAM_PRINT_ORDER", DEFAULT_RISK_PARAMS.keys()))
except Exception:
    DEFAULT_RISK_PARAMS = {}
    RISK_PARAM_COMMENTS = {}
    RISK_PARAM_ORDER = []

try:
    import sync_intra_strategy_params as strategy_defaults

    DEFAULT_STRATEGY_PARAMS = dict(strategy_defaults.STRATEGY_PARAMS)
    STRATEGY_PARAM_COMMENTS = dict(strategy_defaults.PARAM_COMMENTS)
    STRATEGY_PARAM_ORDER = list(
        getattr(strategy_defaults, "PARAM_PRINT_ORDER", DEFAULT_STRATEGY_PARAMS.keys())
    )
except Exception:
    DEFAULT_STRATEGY_PARAMS = {}
    STRATEGY_PARAM_COMMENTS = {}
    STRATEGY_PARAM_ORDER = []

try:
    import sync_intra_funding_thresholds as funding_defaults

    FUNDING_THRESHOLD_ORDER = list(funding_defaults.THRESHOLD_ORDER)
except Exception:
    FUNDING_THRESHOLD_ORDER = []


# Funding 因子链(hardcoded)。未来扩到第 2、3 个因子时直接在 list 尾部追加。
# 因子名必须与 Rust 侧 `arb_open_filter::lookup_factor_realtime_value` 中登记的取数路径
# 一一对应,任一边漏改链上对应因子会被跳过 / 报 `miss_<factor>_value`。
INTRA_FACTOR_CHAIN: List[Dict[str, Any]] = [
    {"factor": "hedge_premium_rate", "enabled": True, "forward_open": 50, "backward_open": 50},
]


def percentile_text_from_value(value: float) -> str:
    rounded = round(value)
    if abs(value - rounded) < 1e-9:
        return str(int(rounded))
    return f"{value:.12g}"


def _funding_dashboard_keys() -> List[str]:
    """按因子链顺序展开 dashboard 平铺 key:`<factor>.enabled` / `.forward_open` / `.backward_open`。"""
    keys: List[str] = []
    for entry in INTRA_FACTOR_CHAIN:
        f = entry["factor"]
        keys.extend([f"{f}.enabled", f"{f}.forward_open", f"{f}.backward_open"])
    return keys


def default_funding_threshold_config(
    open_venue: Optional[str], hedge_venue: Optional[str]
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for entry in INTRA_FACTOR_CHAIN:
        f = entry["factor"]
        out[f"{f}.enabled"] = "true" if entry.get("enabled", True) else "false"
        out[f"{f}.forward_open"] = str(entry.get("forward_open", 50))
        out[f"{f}.backward_open"] = str(entry.get("backward_open", 50))
    return out


def default_funding_threshold_comments() -> Dict[str, str]:
    out: Dict[str, str] = {}
    for entry in INTRA_FACTOR_CHAIN:
        f = entry["factor"]
        out[f"{f}.enabled"] = f"[{f}] 是否启用 (链上 enabled=false 的因子在评估时跳过)"
        out[f"{f}.forward_open"] = f"[{f}] 正向开仓分位数 (0,99]"
        out[f"{f}.backward_open"] = f"[{f}] 反向开仓分位数 (0,99]"
    return out


def parse_bool_text(raw: Any, default: bool = True) -> bool:
    if raw is None:
        return default
    normalized = str(raw).strip().lower()
    if normalized in ("1", "true", "yes", "on"):
        return True
    if normalized in ("0", "false", "no", "off"):
        return False
    return default


def read_funding_threshold_config(
    rds, open_venue: str, hedge_venue: str
) -> Dict[str, str]:
    """从 Redis 读 factor_chain JSON,展平成 dashboard 用的 `<factor>.<key>` flat dict。"""
    defaults = default_funding_threshold_config(open_venue, hedge_venue)
    key = threshold_mapping_key("funding", open_venue, hedge_venue)
    raw = rds.get(key)
    if not raw:
        return defaults
    try:
        decoded = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        parsed = json.loads(decoded)
    except Exception:
        return defaults
    if not isinstance(parsed, dict):
        return defaults
    chain = parsed.get("factor_chain")
    if not isinstance(chain, list):
        return defaults
    out = dict(defaults)  # 用 default 填底,Redis 有的覆盖
    for entry in chain:
        if not isinstance(entry, dict):
            continue
        f = str(entry.get("factor") or "").strip()
        if not f:
            continue
        if "enabled" in entry:
            out[f"{f}.enabled"] = "true" if bool(entry["enabled"]) else "false"
        if "forward_open" in entry:
            out[f"{f}.forward_open"] = str(entry["forward_open"])
        if "backward_open" in entry:
            out[f"{f}.backward_open"] = str(entry["backward_open"])
    return out


def write_funding_threshold_config(
    rds,
    open_venue: str,
    hedge_venue: str,
    values: Dict[str, Any],
) -> Dict[str, Any]:
    """从 dashboard 平铺 input 反向组装 factor_chain JSON 写 Redis。
    链顺序 hardcoded 在 `INTRA_FACTOR_CHAIN`,Redis 里只覆盖 enabled/forward_open/backward_open。
    """
    values = values or {}
    chain_payload: List[Dict[str, Any]] = []
    flat_out: Dict[str, str] = {}
    for default_entry in INTRA_FACTOR_CHAIN:
        f = default_entry["factor"]
        enabled = parse_bool_text(
            values.get(f"{f}.enabled"), bool(default_entry.get("enabled", True))
        )
        forward_raw = values.get(f"{f}.forward_open", default_entry.get("forward_open", 50))
        backward_raw = values.get(f"{f}.backward_open", default_entry.get("backward_open", 50))
        forward_value, forward_text = normalize_percentile_text(forward_raw)
        backward_value, backward_text = normalize_percentile_text(backward_raw)
        if not (0.0 < forward_value <= 99.0):
            raise ValueError(f"{f}.forward_open must be in (0,99]")
        if not (0.0 < backward_value <= 99.0):
            raise ValueError(f"{f}.backward_open must be in (0,99]")
        chain_payload.append(
            {
                "factor": f,
                "enabled": enabled,
                "forward_open": forward_value,
                "backward_open": backward_value,
            }
        )
        flat_out[f"{f}.enabled"] = "true" if enabled else "false"
        flat_out[f"{f}.forward_open"] = forward_text
        flat_out[f"{f}.backward_open"] = backward_text

    key = threshold_mapping_key("funding", open_venue, hedge_venue)
    payload = {
        "factor_chain": chain_payload,
        "rolling_key": f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}",
    }
    rds.set(key, json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return {
        "key": key,
        "count": len(flat_out),
        "values": flat_out,
    }

try:
    import sync_rolling_metrics_params as rolling_defaults

    BASE_ROLLING_PARAMS = dict(rolling_defaults.DEFAULTS)
except Exception:
    BASE_ROLLING_PARAMS = {
        "MAX_LENGTH": 150_000,
        "refresh_sec": 30,
        "reload_param_sec": 3,
        "factors": {},
    }


def clone_json_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: clone_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [clone_json_value(v) for v in value]
    return value


def build_intra_rolling_defaults(
    base_defaults: Dict[str, Any], *mappings: Dict[str, str]
) -> Dict[str, Any]:
    defaults = clone_json_value(base_defaults or {})
    defaults.setdefault("MAX_LENGTH", 150_000)
    defaults.setdefault("refresh_sec", 30)
    defaults.setdefault("reload_param_sec", 3)

    base_factors = defaults.get("factors")
    if not isinstance(base_factors, dict):
        base_factors = {}

    factor_quantiles: Dict[str, set] = {}
    for mapping in mappings:
        for field_ref in (mapping or {}).values():
            if not isinstance(field_ref, str):
                continue
            matched = re.fullmatch(r"([a-z_]+)_(\d+(?:\.\d+)?)", field_ref.strip())
            if not matched:
                continue
            factor_name = matched.group(1)
            raw = float(matched.group(2))
            quantile = int(raw) if abs(raw - round(raw)) < 1e-9 else raw
            factor_quantiles.setdefault(factor_name, set()).add(quantile)

    factors: Dict[str, Dict[str, Any]] = {}
    for factor_name, quantiles in factor_quantiles.items():
        factor_cfg = clone_json_value(base_factors.get(factor_name) or {})
        factor_cfg.setdefault("resample_interval_ms", 1_000)
        if factor_name.endswith("_fr") or factor_name.endswith("_premium_rate"):
            factor_cfg.setdefault("rolling_window", 14_400)
            factor_cfg.setdefault("min_periods", 7_200)
        else:
            factor_cfg.setdefault("rolling_window", 100_000)
            factor_cfg.setdefault("min_periods", 1)
        if quantiles:
            factor_cfg["quantiles"] = sorted(quantiles)
        else:
            factor_cfg.setdefault("quantiles", [])
        factors[factor_name] = factor_cfg

    defaults["factors"] = factors
    return defaults

try:
    import sync_intra_symbol_lists as symbol_defaults

    SYMBOL_DEFAULTS_SRC = symbol_defaults
except Exception:
    SYMBOL_DEFAULTS_SRC = None

spread_sync = None
try:
    import sync_intra_spread_thresholds as spread_sync

    SPREAD_THRESHOLD_MAPPING = dict(spread_sync.SPREAD_THRESHOLD_MAPPING)
    SPREAD_THRESHOLD_ORDER = list(spread_sync.THRESHOLD_ORDER)
except Exception:
    SPREAD_THRESHOLD_MAPPING = {}
    SPREAD_THRESHOLD_ORDER = []

def build_runtime_rolling_defaults(
    open_venue: Optional[str], hedge_venue: Optional[str]
) -> Dict[str, Any]:
    # 把 hardcoded 因子链展开成 rolling_metrics_params 用的 dest_field → field_ref 形式,
    # 给 build_intra_rolling_defaults 注入,保证启动时 rolling pipeline 会算出每个因子
    # 默认分位数。disabled 因子也保留(以便用户后续打开时已经有对应数据)。
    funding_mapping: Dict[str, str] = {}
    for entry in INTRA_FACTOR_CHAIN:
        f = entry["factor"]
        funding_mapping[f"{f}.forward_open"] = (
            f"{f}_{percentile_text_from_value(float(entry.get('forward_open', 50)))}"
        )
        funding_mapping[f"{f}.backward_open"] = (
            f"{f}_{percentile_text_from_value(float(entry.get('backward_open', 50)))}"
        )
    open_vol_mapping: Dict[str, Any] = {}
    defaults = build_intra_rolling_defaults(
        BASE_ROLLING_PARAMS,
        SPREAD_THRESHOLD_MAPPING,
        funding_mapping,
        open_vol_mapping,
    )
    if (
        "rolling_defaults" in globals()
        and open_venue
        and hedge_venue
        and hasattr(rolling_defaults, "apply_pair_specific_defaults")
    ):
        try:
            rolling_defaults.apply_pair_specific_defaults(open_venue, hedge_venue, defaults)
        except Exception:
            pass
    return defaults

INDEX_HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>INTRA 配置中心</title>
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
    .kv-table { display: grid; grid-template-columns: 280px 1fr 1.5fr; gap: 8px; }
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
    <h1>INTRA 配置中心</h1>
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
      <a href="#symbol-lists">Symbol Lists</a>
      <a href="#strategy-params">Strategy Params</a>
      <a href="#risk-params">Risk Params</a>
      <a id="funding-nav-link" href="#funding-thresholds">Funding Thresholds</a>
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
          <h3>平仓列表 <span class="hint">intra_dump_symbols</span></h3>
          <textarea id="sym-dump" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
        <div>
          <h3>正套建仓 <span class="hint">intra_fwd_trade_symbols</span></h3>
          <textarea id="sym-fwd" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
        <div>
          <h3>反套建仓 <span class="hint">intra_bwd_trade_symbols</span></h3>
          <textarea id="sym-bwd" class="mono" placeholder="每行一个 symbol"></textarea>
        </div>
        <div>
      <div class="hint">说明：intra 统一按基础 symbol 管理；支持逗号/空格/换行分隔，保存时会做大写和基础归一化。</div>
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
      <div class="hint">
        `enable_tlen_cancel` 控制基于 tlen 的 open 撤单 trigger/query/cancel 链路；`tlen_cancel_freq_ms` 控制 trigger 频率(ms)；`spread_cancel_cooldown_ms` 控制 spread cancel 的去抖冷却(ms，默认 100，可设 0 关闭)。
      </div>
      <div id="strategy-table" class="kv-table"></div>
      <div id="strategy-vol-preview" class="status"></div>
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
        <h2>Funding Filter Config</h2>
        <div class="actions">
          <button id="funding-config-load" class="secondary">读取配置</button>
          <button id="funding-config-save">保存配置</button>
          <button id="funding-default" class="ghost">默认</button>
        </div>
      </div>
      <div class="hint" style="margin-bottom: 10px;">只配置 enable 与 forward/backward 两个分位数。固定使用 hedge_premium_rate；trade_signal 会自动读取 rolling quantiles，不需要手工同步阈值。</div>
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
      <div class="hint" style="margin-top: 8px;">可用 factor: bidask, askbid, spread, hedge_premium_rate, open_vol, hedge_vol。intra 的 funding filter 固定读取 hedge_premium_rate 的 quantiles，无需单独配置 mapping。</div>
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
__PER_SYMBOL_PANELS_HTML__
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

    function normalizeExchange(ex) {
      const raw = (ex || '').toLowerCase();
      return raw === 'okx' ? 'okex' : raw;
    }

    function applyFixedContext() {
      envNameInput.value = BOOTSTRAP.env_name || '';
      const defaultOpen = BOOTSTRAP.default_open_venue || '';
      const defaultHedge = BOOTSTRAP.default_hedge_venue || '';
      openVenueInput.value = defaultOpen;
      hedgeVenueInput.value = defaultHedge;
      refreshKeySuffix();
    }

    function venueExchange(venue) {
      const raw = (venue || '').trim().toLowerCase();
      if (!raw.includes('-')) return '';
      return normalizeExchange(raw.split('-', 1)[0]);
    }

    function refreshKeySuffix() {
      const open = (openVenueInput.value || '').trim().toLowerCase();
      const hedge = (hedgeVenueInput.value || '').trim().toLowerCase();
      const openEx = venueExchange(open);
      const hedgeEx = venueExchange(hedge);
      keySuffixEl.textContent = openEx && hedgeEx ? `${openEx}-${hedgeEx}` : '-';
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
      ordered.forEach(key => {
        const row = document.createElement('div');
        row.className = 'kv-row';

        const keyCell = document.createElement('div');
        keyCell.className = 'kv-key';
        keyCell.textContent = key;

        const inputCell = document.createElement('div');
        inputCell.className = 'kv-input';
        const rawValue = values[key] ?? defaults[key] ?? '';
        const useBooleanSelect =
          ((containerId === 'strategy-table' &&
            ['enable_tlen_cancel', 'enable_environment_model'].includes(key)) ||
           (containerId === 'funding-table' && ['enabled'].includes(key))) &&
          isBooleanParamValue(rawValue);
        let input;
        if (useBooleanSelect) {
          input = document.createElement('select');
          [
            ['false', 'false'],
            ['true', 'true'],
          ].forEach(([value, label]) => {
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

    function findStrategyInput(key) {
      return document.querySelector(
        `#strategy-table input[data-key="${key}"], #strategy-table select[data-key="${key}"]`
      );
    }

    async function refreshStrategyVolPreview() {
      setStatus('strategy-vol-preview', '', '');
    }

    function bindStrategyVolPreview() {
      setStatus('strategy-vol-preview', '', '');
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
      const ex = normalizeExchange(BOOTSTRAP.default_exchange || '');
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
        bindStrategyVolPreview();
        await refreshStrategyVolPreview();
        setStatus('strategy-status', '读取完成');
      } catch (err) {
        setStatus('strategy-status', `读取失败: ${err}`, false);
      }
    }

    async function saveStrategyParams() {
      setStatus('strategy-status', '保存中...');
      try {
        const payload = {
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
      bindStrategyVolPreview();
      refreshStrategyVolPreview().catch(console.error);
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
          open_venue: openVenueInput.value.trim(),
          hedge_venue: hedgeVenueInput.value.trim(),
          values: collectParamValues('funding-table'),
        };
        await fetchJson(apiUrl('funding-thresholds'), {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload),
        });
        setStatus('funding-status', '配置保存成功');
      } catch (err) {
        setStatus('funding-status', `保存失败: ${err}`, false);
      }
    }

    function applyFundingDefaults() {
      buildParamRows('funding-table', BOOTSTRAP.defaults.funding_thresholds || {}, BOOTSTRAP.comments.funding_thresholds || {}, BOOTSTRAP.order.funding_thresholds || [], {});
      setStatus('funding-status', '已载入默认配置');
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
        const warnings = data.warnings && data.warnings.length ? ` ⚠️ ${data.warnings.join('; ')}` : '';
        setStatus('spread-status', `同步完成: ${data.written || 0} 字段${changed}${warnings}`, !warnings);
      } catch (err) {
        setStatus('spread-status', `同步失败: ${err}`, false);
      }
    }

    async function reloadAll() {
      await loadSymbolLists();
      await loadStrategyParams();
      await loadRiskParams();
      if (BOOTSTRAP.features?.funding_thresholds !== false) {
        await loadFundingThresholds();
      }
      await loadRollingParams();
      await loadSpreadMapping();
    }

    applyFixedContext();

    function applyFeatureVisibility() {
      const fundingEnabled = BOOTSTRAP.features?.funding_thresholds !== false;
      const fundingPanel = document.getElementById('funding-thresholds');
      const fundingNav = document.getElementById('funding-nav-link');
      if (!fundingEnabled) {
        if (fundingPanel) fundingPanel.style.display = 'none';
        if (fundingNav) fundingNav.style.display = 'none';
      }
    }

    applyFeatureVisibility();
    buildParamRows('risk-table', BOOTSTRAP.defaults.risk_params || {}, BOOTSTRAP.comments.risk_params || {}, BOOTSTRAP.order.risk || [], {});
    buildParamRows('strategy-table', BOOTSTRAP.defaults.strategy_params || {}, BOOTSTRAP.comments.strategy_params || {}, BOOTSTRAP.order.strategy || [], {});
    if (BOOTSTRAP.features?.funding_thresholds !== false) {
      buildParamRows('funding-table', BOOTSTRAP.defaults.funding_thresholds || {}, BOOTSTRAP.comments.funding_thresholds || {}, BOOTSTRAP.order.funding_thresholds || [], {});
    }
    buildParamRows('spread-table', BOOTSTRAP.defaults.spread_mapping || {}, {}, BOOTSTRAP.order.spread_mapping || [], {});

    document.getElementById('sym-load').addEventListener('click', loadSymbolLists);
    document.getElementById('sym-save').addEventListener('click', saveSymbolLists);
    document.getElementById('sym-default').addEventListener('click', applySymbolDefaults);

    document.getElementById('risk-load').addEventListener('click', loadRiskParams);
    document.getElementById('risk-save').addEventListener('click', saveRiskParams);
    document.getElementById('risk-default').addEventListener('click', applyRiskDefaults);

    document.getElementById('strategy-load').addEventListener('click', loadStrategyParams);
    document.getElementById('strategy-save').addEventListener('click', saveStrategyParams);
    document.getElementById('strategy-default').addEventListener('click', applyStrategyDefaults);

    if (BOOTSTRAP.features?.funding_thresholds !== false) {
      document.getElementById('funding-config-load').addEventListener('click', loadFundingThresholds);
      document.getElementById('funding-config-save').addEventListener('click', saveFundingThresholds);
      document.getElementById('funding-default').addEventListener('click', applyFundingDefaults);
    }

    document.getElementById('rolling-load').addEventListener('click', loadRollingParams);
    document.getElementById('rolling-save').addEventListener('click', saveRollingParams);
    document.getElementById('rolling-default').addEventListener('click', applyRollingDefaults);

    document.getElementById('spread-config-load').addEventListener('click', loadSpreadMapping);
    document.getElementById('spread-config-save').addEventListener('click', saveSpreadMapping);
    document.getElementById('spread-sync').addEventListener('click', syncSpreadThresholds);

    document.getElementById('reload-all').addEventListener('click', reloadAll);

    // Per-symbol overrides panels：context 取自现有 venue input
    function _psGetContext() {
      return {
        exchange: (BOOTSTRAP.default_exchange || '').trim(),
        openVenue: (openVenueInput.value || '').trim(),
        hedgeVenue: (hedgeVenueInput.value || '').trim(),
      };
    }
__PER_SYMBOL_PANELS_JS__
    bindPerSymbolPanels();
    loadAmountU();
    loadMaxPosU();
    loadHedgeOffsetLimits();

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
    parser = argparse.ArgumentParser(description="INTRA 配置服务器 (intra_config_server)")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8000)))
    parser.add_argument(
        "--default-exchange",
        default=os.environ.get("DEFAULT_EXCHANGE", "okex"),
        help="默认选中的 open exchange（仅用于页面初始化）",
    )
    parser.add_argument(
        "--default-open-venue",
        default=os.environ.get("DEFAULT_OPEN_VENUE"),
        help="默认 open venue；未提供时尝试从目录名推断",
    )
    parser.add_argument(
        "--default-hedge-venue",
        default=os.environ.get("DEFAULT_HEDGE_VENUE"),
        help="默认 hedge venue；未提供时尝试从目录名推断",
    )
    return parser.parse_args()

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


def normalize_symbol_list_for_intra(value: Any) -> List[str]:
    base = normalize_symbol_list(value)

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


def replace_hash(rds, key: str, mapping: Dict[str, str]) -> Dict[str, Any]:
    existing = {
        decode_redis_value(item)
        for item in (rds.hkeys(key) or [])
    }
    stale_fields = sorted(existing - set(mapping.keys()))

    pipe = rds.pipeline()
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


def sanitize_string_mapping(values: Any) -> Dict[str, str]:
    if not isinstance(values, dict):
        raise ValueError("values must be object")

    mapping: Dict[str, str] = {}
    for raw_key, raw_value in values.items():
        key = str(raw_key).strip()
        if not key:
            continue
        if raw_value is None:
            mapping[key] = ""
        else:
            mapping[key] = str(raw_value).strip()
    return mapping


def ordered_schema_keys(
    defaults: Dict[str, Any], comments: Dict[str, str], order: List[str]
) -> List[str]:
    keys: List[str] = []
    seen = set()
    for key in [*order, *defaults.keys(), *comments.keys()]:
        if key in seen:
            continue
        seen.add(key)
        keys.append(key)
    return keys


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


def normalize_strategy_params_by_schema(mapping: Dict[str, str]) -> Dict[str, str]:
    normalized = dict(mapping)
    if "tlen_cancel_freq_ms" in normalized:
        normalized["tlen_cancel_freq_ms"] = normalize_positive_int_text(
            normalized["tlen_cancel_freq_ms"], "tlen_cancel_freq_ms"
        )
    if "spread_cancel_cooldown_ms" in normalized:
        normalized["spread_cancel_cooldown_ms"] = normalize_nonnegative_int_text(
            normalized["spread_cancel_cooldown_ms"], "spread_cancel_cooldown_ms"
        )
    return normalized


def sanitize_mapping_by_schema(
    values: Any,
    defaults: Dict[str, Any],
    comments: Dict[str, str],
    order: List[str],
    *,
    normalize_strategy: bool = False,
) -> Dict[str, str]:
    mapping = sanitize_string_mapping(values)
    allowed = set(ordered_schema_keys(defaults, comments, order))
    sanitized = {key: mapping[key] for key in mapping.keys() if key in allowed}
    if normalize_strategy:
        return normalize_strategy_params_by_schema(sanitized)
    return sanitized


def make_key_suffix(open_venue: str, hedge_venue: str) -> str:
    open_ex = exchange_from_venue(open_venue)
    hedge_ex = exchange_from_venue(hedge_venue)
    if not open_ex or not hedge_ex:
        raise ValueError(f"invalid intra venues: open={open_venue}, hedge={hedge_venue}")
    if open_ex != hedge_ex:
        raise ValueError(f"intra requires same exchange: open={open_venue}, hedge={hedge_venue}")
    return open_ex


def resolve_venues(
    exchange: str,
    open_venue: Optional[str],
    hedge_venue: Optional[str],
    default_open_venue: Optional[str],
    default_hedge_venue: Optional[str],
) -> Tuple[str, str, str, str]:
    ex = normalize_exchange(exchange)
    if ex not in SUPPORTED_EXCHANGES:
        raise ValueError(f"unsupported exchange: {ex}")

    if open_venue or hedge_venue:
        if not open_venue or not hedge_venue:
            raise ValueError("open_venue/hedge_venue 需要成对提供")
        open_v = open_venue.strip().lower()
        hedge_v = hedge_venue.strip().lower()
    elif default_open_venue and default_hedge_venue:
        open_v = default_open_venue.strip().lower()
        hedge_v = default_hedge_venue.strip().lower()
    else:
        raise ValueError("intra 需要 open_venue/hedge_venue（或通过默认配置提供）")

    key_suffix = make_key_suffix(open_v, hedge_v)
    resolved_ex = exchange_from_venue(open_v) or ex
    return resolved_ex, open_v, hedge_v, key_suffix


def get_symbol_defaults() -> Dict[str, Dict[str, List[str]]]:
    defaults: Dict[str, Dict[str, List[str]]] = {}
    dump_symbols = (
        list(getattr(SYMBOL_DEFAULTS_SRC, "DUMP_SYMBOLS", [])) if SYMBOL_DEFAULTS_SRC else []
    )
    fwd_symbols = (
        list(getattr(SYMBOL_DEFAULTS_SRC, "FWD_SYMBOLS", [])) if SYMBOL_DEFAULTS_SRC else []
    )
    bwd_symbols = (
        list(getattr(SYMBOL_DEFAULTS_SRC, "BWD_SYMBOLS", [])) if SYMBOL_DEFAULTS_SRC else []
    )
    for ex in SUPPORTED_EXCHANGES:
        defaults[ex] = {
            "dump_symbols": dump_symbols,
            "fwd_trade_symbols": fwd_symbols,
            "bwd_trade_symbols": bwd_symbols,
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
    return json.loads(json.dumps(factors, ensure_ascii=False))


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


def default_factor_config_for_name(factor_name: str) -> Dict[str, Any]:
    cfg: Dict[str, Any] = {"resample_interval_ms": 1_000}
    if factor_name.endswith("_fr") or factor_name.endswith("_premium_rate"):
        cfg["rolling_window"] = 14_400
        cfg["min_periods"] = 7_200
    else:
        cfg["rolling_window"] = 100_000
        cfg["min_periods"] = 1
    cfg["quantiles"] = []
    return cfg


def ensure_factor_quantile_config(
    factors: Dict[str, Any], factor_name: str, percentile_raw: Any
) -> bool:
    percentile_value, percentile_text = normalize_percentile_text(percentile_raw)
    quantile_value = (
        int(percentile_value) if abs(percentile_value - round(percentile_value)) < 1e-9 else percentile_value
    )
    factor_cfg = factors.get(factor_name)
    if not isinstance(factor_cfg, dict):
        factor_cfg = default_factor_config_for_name(factor_name)
        factors[factor_name] = factor_cfg
    quantiles = factor_cfg.get("quantiles")
    if not isinstance(quantiles, list):
        quantiles = []
        factor_cfg["quantiles"] = quantiles
    normalized_existing: List[str] = []
    for item in quantiles:
        try:
            value = float(item)
        except Exception:
            continue
        normalized_existing.append(percentile_text_from_value(value))
    if percentile_text in normalized_existing:
        return False
    quantiles.append(quantile_value)
    while len(quantiles) > 8:
        quantiles.pop(0)
    return True


def ensure_intra_runtime_quantiles(
    rds,
    open_venue: str,
    hedge_venue: str,
    funding_values: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    key = f"rolling_metrics_params_{open_venue}_{hedge_venue}"
    parsed = parse_rolling_params(read_hash(rds, key))
    factors = parsed.get("factors")
    if not isinstance(factors, dict):
        factors = {}
        parsed["factors"] = factors

    changed_factors: List[str] = []
    if funding_values is not None:
        # 遍历 hardcoded 因子链,只为 enabled=true 的因子在 rolling_metrics_params
        # 里登记 forward/backward 分位数,确保上游 rolling_metrics 会算出对应 quantile。
        for default_entry in INTRA_FACTOR_CHAIN:
            f = default_entry["factor"]
            if not parse_bool_text(funding_values.get(f"{f}.enabled"), True):
                continue
            for direction in ("forward_open", "backward_open"):
                raw = funding_values.get(f"{f}.{direction}")
                if raw in (None, ""):
                    continue
                if ensure_factor_quantile_config(factors, f, raw):
                    changed_factors.append(f"{f}_{direction}")

    if changed_factors:
        write_hash(rds, key, serialize_rolling_params(parsed))

    return {
        "key": key,
        "changed": bool(changed_factors),
        "changed_factors": changed_factors,
    }


def threshold_mapping_key(kind: str, open_venue: str, hedge_venue: str) -> str:
    return f"intra_{kind}_thresholds_config_{open_venue}_{hedge_venue}"


def normalize_threshold_mapping(values: Dict[str, Any]) -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    for key, value in (values or {}).items():
        k = str(key).strip()
        v = str(value).strip()
        if not k or not v:
            continue
        mapping[k] = v
    return mapping


def read_threshold_mapping(
    rds,
    kind: str,
    open_venue: str,
    hedge_venue: str,
    defaults: Dict[str, str],
) -> Dict[str, str]:
    key = threshold_mapping_key(kind, open_venue, hedge_venue)
    raw = rds.get(key)
    if raw:
        try:
            decoded = (
                raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            )
            parsed = json.loads(decoded)
            if isinstance(parsed, dict):
                values = parsed.get("mapping")
                if isinstance(values, dict):
                    return normalize_threshold_mapping(values)
        except Exception:
            pass
    return normalize_threshold_mapping(defaults)


def write_threshold_mapping(
    rds,
    kind: str,
    open_venue: str,
    hedge_venue: str,
    mapping: Dict[str, str],
) -> int:
    key = threshold_mapping_key(kind, open_venue, hedge_venue)
    existing: Dict[str, Any] = {}
    raw = rds.get(key)
    if raw:
        try:
            decoded = (
                raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
            )
            parsed = json.loads(decoded)
            if isinstance(parsed, dict):
                existing = parsed
        except Exception:
            existing = {}
    payload = {
        "schema_version": 1,
        "namespace": "intra",
        "kind": kind,
        "open_venue": open_venue,
        "hedge_venue": hedge_venue,
        "rolling_key": existing.get(
            "rolling_key", f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"
        ),
        "mapping": mapping,
        "threshold_order": list(mapping.keys()),
        "generated_at": existing.get("generated_at"),
    }
    rds.set(key, json.dumps(payload, ensure_ascii=False, sort_keys=True))
    return len(mapping)


def generate_threshold_fields(
    symbol_data: Dict[str, Dict],
    mapping: Dict[str, str],
    extractor,
) -> tuple[Dict[str, str], set[str]]:
    all_fields: Dict[str, str] = {}
    skipped: set[str] = set()
    for symbol, obj in symbol_data.items():
        symbol_fields: Dict[str, str] = {}
        for suffix, field_ref in mapping.items():
            value = extractor(obj, field_ref)
            if value is None:
                skipped.add(symbol)
                symbol_fields = {}
                break
            field_key = f"{symbol}_{suffix}"
            symbol_fields[field_key] = f"{value:.8f}".rstrip("0").rstrip(".")
        if symbol_fields:
            all_fields.update(symbol_fields)
    return all_fields, skipped


def sync_thresholds(
    rds,
    kind: str,
    open_venue: str,
    hedge_venue: str,
    key_suffix: str,
    mapping: Optional[Dict[str, str]],
    defaults: Dict[str, str],
    load_symbol_lists_fn,
    read_rolling_metrics_fn,
    normalize_symbol_fn,
    extract_quantile_fn,
    symbol: Optional[str] = None,
) -> Dict[str, Any]:
    rolling_key = f"rolling_metrics_thresholds_{open_venue}_{hedge_venue}"
    write_key = f"intra_{kind}_thresholds_{open_venue}_{hedge_venue}"

    mapping = normalize_threshold_mapping(mapping or {})
    if not mapping:
        mapping = normalize_threshold_mapping(defaults)
    if not mapping:
        raise RuntimeError(f"{kind} mapping is empty")

    target_symbols = load_symbol_lists_fn(rds, key_suffix)
    if symbol:
        target_symbols = [str(symbol).strip().upper()]
    if not target_symbols:
        raise RuntimeError(f"no symbols found for key_suffix={key_suffix}")
    rolling_data = read_rolling_metrics_fn(rds, rolling_key)
    if not rolling_data:
        raise RuntimeError(f"rolling metrics not found: {rolling_key}")

    symbol_data: Dict[str, Dict] = {}
    missing_symbols: set[str] = set()
    for target_symbol in target_symbols:
        rolling_key_symbol = normalize_symbol_fn(target_symbol)
        payload = rolling_data.get(rolling_key_symbol)
        if payload is None:
            missing_symbols.add(rolling_key_symbol)
            continue
        symbol_data[rolling_key_symbol] = payload
    mm_mapping = {k: v for k, v in mapping.items() if k.endswith("_mm")}
    mt_mapping = {k: v for k, v in mapping.items() if k.endswith("_mt")}
    other_mapping = {k: v for k, v in mapping.items() if not k.endswith(("_mm", "_mt"))}

    all_fields: Dict[str, str] = {}
    all_skipped: set[str] = set()
    sync_warnings: List[str] = []

    for role, role_mapping in [("mm", mm_mapping), ("mt", mt_mapping), ("other", other_mapping)]:
        if not role_mapping:
            continue
        role_fields, role_skipped = generate_threshold_fields(symbol_data, role_mapping, extract_quantile_fn)
        all_skipped.update(role_skipped)
        if role_fields:
            all_fields.update(role_fields)
        else:
            sync_warnings.append(f"{role} 阈值无可写入数据（rolling 数据不足，{len(role_skipped)} 个 symbols 缺少数据）")

    changed = 0
    if all_fields:
        res = rds.hset(write_key, mapping=all_fields)
        changed = int(res) if res is not None else 0
        write_threshold_mapping(rds, kind, open_venue, hedge_venue, mapping)

    return {
        "written": len(all_fields),
        "changed": changed,
        "missing_symbols": sorted(list(missing_symbols)),
        "skipped_symbols": sorted(list(all_skipped)),
        "warnings": sync_warnings,
        "write_key": write_key,
        "rolling_key": rolling_key,
        "mapping_key": threshold_mapping_key(kind, open_venue, hedge_venue),
    }


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
    return sync_thresholds(
        rds,
        "spread",
        open_venue,
        hedge_venue,
        key_suffix,
        mapping,
        SPREAD_THRESHOLD_MAPPING,
        spread_sync.load_symbol_lists,
        spread_sync.read_rolling_metrics,
        spread_sync.normalize_for_rolling,
        spread_sync.extract_quantile_value,
        symbol,
    )


def render_index_html(
    default_exchange: str,
    default_open_venue: Optional[str],
    default_hedge_venue: Optional[str],
) -> str:
    funding_defaults_mapping = default_funding_threshold_config(
        default_open_venue, default_hedge_venue
    )
    rolling_defaults_mapping = build_runtime_rolling_defaults(
        default_open_venue, default_hedge_venue
    )
    bootstrap = {
        "env_name": infer_dir_prefix_from_cwd() or "",
        "exchanges": SUPPORTED_EXCHANGES,
        "default_exchange": default_exchange,
        "default_open_venue": default_open_venue or "",
        "default_hedge_venue": default_hedge_venue or "",
        "features": {
            "funding_thresholds": funding_thresholds_applicable(
                default_open_venue, default_hedge_venue
            ),
        },
        "exchange_defaults": EXCHANGE_DEFAULTS,
        "defaults": {
            "symbol_lists": get_symbol_defaults(),
            "risk_params": DEFAULT_RISK_PARAMS,
            "strategy_params": DEFAULT_STRATEGY_PARAMS,
            "funding_thresholds": funding_defaults_mapping,
            "rolling_params": rolling_defaults_mapping,
            "spread_mapping": SPREAD_THRESHOLD_MAPPING,
        },
        "comments": {
            "risk_params": RISK_PARAM_COMMENTS,
            "strategy_params": STRATEGY_PARAM_COMMENTS,
            "funding_thresholds": default_funding_threshold_comments(),
        },
        "order": {
            "risk": RISK_PARAM_ORDER,
            "strategy": STRATEGY_PARAM_ORDER,
            "funding_thresholds": _funding_dashboard_keys(),
            "spread_mapping": SPREAD_THRESHOLD_ORDER,
        },
    }

    html = INDEX_HTML_TEMPLATE.replace("__BOOTSTRAP__", json.dumps(bootstrap, ensure_ascii=False))
    html = html.replace("__PER_SYMBOL_PANELS_HTML__", ps_overrides.render_per_symbol_panels_html())
    html = html.replace("__PER_SYMBOL_PANELS_JS__", ps_overrides.render_per_symbol_panels_js())
    return html


@dataclass
class ServerContext:
    redis_client: Any
    default_exchange: str
    default_open_venue: Optional[str]
    default_hedge_venue: Optional[str]


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

    def _fixed_context(self) -> Tuple[str, str, str, str]:
        open_venue = self.server.context.default_open_venue
        hedge_venue = self.server.context.default_hedge_venue
        if not open_venue or not hedge_venue:
            raise ValueError("intra_config_server missing fixed open/hedge venue")
        exchange = exchange_from_venue(open_venue) or self.server.context.default_exchange
        key_suffix = make_key_suffix(open_venue, hedge_venue)
        return exchange, open_venue, hedge_venue, key_suffix

    def _validate_requested_context(
        self,
        exchange: Optional[str],
        open_venue: Optional[str],
        hedge_venue: Optional[str],
    ) -> None:
        fixed_exchange, fixed_open, fixed_hedge, _ = self._fixed_context()
        if exchange and normalize_exchange(exchange) != fixed_exchange:
            raise ValueError(
                f"intra_config_server is bound to exchange={fixed_exchange}, got {exchange}"
            )
        if open_venue and open_venue.strip().lower() != fixed_open:
            raise ValueError(
                f"intra_config_server is bound to open_venue={fixed_open}, got {open_venue}"
            )
        if hedge_venue and hedge_venue.strip().lower() != fixed_hedge:
            raise ValueError(
                f"intra_config_server is bound to hedge_venue={fixed_hedge}, got {hedge_venue}"
            )

    def _resolve_request_context(self, params: Dict[str, List[str]]) -> Tuple[str, str, str, str]:
        exchange = (params.get("exchange") or [None])[0]
        open_venue = (params.get("open_venue") or [None])[0]
        hedge_venue = (params.get("hedge_venue") or [None])[0]
        self._validate_requested_context(exchange, open_venue, hedge_venue)
        return self._fixed_context()

    def _resolve_payload_context(self, payload: Dict[str, Any]) -> Tuple[str, str, str, str]:
        exchange = payload.get("exchange")
        open_venue = payload.get("open_venue")
        hedge_venue = payload.get("hedge_venue")
        self._validate_requested_context(
            str(exchange) if exchange is not None else None,
            str(open_venue) if open_venue is not None else None,
            str(hedge_venue) if hedge_venue is not None else None,
        )
        return self._fixed_context()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            html = render_index_html(
                self.server.context.default_exchange,
                self.server.context.default_open_venue,
                self.server.context.default_hedge_venue,
            )
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
                "dump_symbols": read_symbol_list(rds, f"intra_dump_symbols:{key_suffix}"),
                "fwd_trade_symbols": read_symbol_list(rds, f"intra_fwd_trade_symbols:{key_suffix}"),
                "bwd_trade_symbols": read_symbol_list(rds, f"intra_bwd_trade_symbols:{key_suffix}"),
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
            raw_values = read_hash(self.server.context.redis_client, key)
            values, stale_values = filter_mapping_by_schema(
                raw_values, DEFAULT_RISK_PARAMS, RISK_PARAM_COMMENTS, RISK_PARAM_ORDER
            )
            if not values:
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

        if parsed.path == "/api/strategy-params":
            try:
                _, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = f"intra_strategy_params_{open_venue}_{hedge_venue}"
            raw_values = read_hash(self.server.context.redis_client, key)
            values, stale_values = filter_mapping_by_schema(
                raw_values,
                DEFAULT_STRATEGY_PARAMS,
                STRATEGY_PARAM_COMMENTS,
                STRATEGY_PARAM_ORDER,
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

        if parsed.path == "/api/open-volatility-preview":
            try:
                _, open_venue, _, _ = self._resolve_request_context(params)
                percentile = (params.get("percentile") or [None])[0]
                if percentile is None:
                    self._send_error(400, "missing percentile")
                    return
                data = preview_open_volatility_source(
                    self.server.context.redis_client,
                    open_venue,
                    percentile,
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            self._send_json(200, data)
            return

        if parsed.path == "/api/funding-thresholds":
            try:
                _, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            key = threshold_mapping_key("funding", open_venue, hedge_venue)
            values = read_funding_threshold_config(
                self.server.context.redis_client,
                open_venue,
                hedge_venue,
            )
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
            key = threshold_mapping_key("spread", open_venue, hedge_venue)
            values = read_threshold_mapping(
                self.server.context.redis_client,
                "spread",
                open_venue,
                hedge_venue,
                SPREAD_THRESHOLD_MAPPING,
            )
            if not values:
                values = normalize_threshold_mapping(SPREAD_THRESHOLD_MAPPING)
            self._send_json(200, {"key": key, "count": len(values), "values": values})
            return

        if parsed.path in ("/api/amount-u", "/api/max-pos-u", "/api/hedge-offset-limits"):
            try:
                _, open_venue, hedge_venue, _ = self._resolve_request_context(params)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            env_name = infer_dir_prefix_from_cwd() or ""
            if not env_name:
                self._send_error(400, "env_name unavailable (no cwd prefix)")
                return
            rds = self.server.context.redis_client
            try:
                if parsed.path == "/api/amount-u":
                    data = ps_overrides.read_amount_u(rds, env_name, open_venue, hedge_venue)
                elif parsed.path == "/api/max-pos-u":
                    data = ps_overrides.read_max_pos_u(rds, env_name, open_venue, hedge_venue)
                else:
                    data = ps_overrides.read_hedge_offset_limits(
                        rds, env_name, open_venue, hedge_venue
                    )
            except ValueError as exc:
                self._send_error(400, str(exc))
                return
            self._send_json(200, data)
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
                exchange, open_v, hedge_v, key_suffix = self._resolve_payload_context(payload)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            raw_dump = payload.get("dump_symbols") or []
            raw_fwd = payload.get("fwd_trade_symbols") or []
            raw_bwd = payload.get("bwd_trade_symbols") or []
            dump_symbols = normalize_symbol_list_for_intra(raw_dump)
            fwd_symbols = normalize_symbol_list_for_intra(raw_fwd)
            bwd_symbols = normalize_symbol_list_for_intra(raw_bwd)
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
                    f"intra_dump_symbols:{key_suffix}",
                    json.dumps(dump_symbols, ensure_ascii=False),
                )
                rds.set(
                    f"intra_fwd_trade_symbols:{key_suffix}",
                    json.dumps(fwd_symbols, ensure_ascii=False),
                )
                rds.set(
                    f"intra_bwd_trade_symbols:{key_suffix}",
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
            try:
                exchange, open_v, hedge_v, _ = self._resolve_payload_context(payload)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            key = build_risk_params_key(open_v, hedge_v)
            try:
                mapping = sanitize_mapping_by_schema(
                    values, DEFAULT_RISK_PARAMS, RISK_PARAM_COMMENTS, RISK_PARAM_ORDER
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            result = replace_hash(self.server.context.redis_client, key, mapping)
            print(
                f"[risk-params][POST] exchange={exchange} open={open_v} hedge={hedge_v} "
                f"key={key} fields={len(mapping)} removed={result['removed_count']}"
            )
            sys.stdout.flush()
            self._send_json(200, result)
            return

        if parsed.path == "/api/strategy-params":
            try:
                _, open_v, hedge_v, _ = self._resolve_payload_context(payload)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            key = f"intra_strategy_params_{open_v}_{hedge_v}"
            try:
                mapping = sanitize_mapping_by_schema(
                    values,
                    DEFAULT_STRATEGY_PARAMS,
                    STRATEGY_PARAM_COMMENTS,
                    STRATEGY_PARAM_ORDER,
                    normalize_strategy=True,
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            result = replace_hash(self.server.context.redis_client, key, mapping)
            rolling_attach = ensure_intra_runtime_quantiles(
                self.server.context.redis_client,
                open_v,
                hedge_v,
            )
            result["rolling_attach"] = rolling_attach
            self._send_json(200, result)
            return

        if parsed.path == "/api/funding-thresholds":
            try:
                _, open_v, hedge_v, _ = self._resolve_payload_context(payload)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            try:
                result = write_funding_threshold_config(
                    self.server.context.redis_client, open_v, hedge_v, values
                )
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            result["rolling_attach"] = ensure_intra_runtime_quantiles(
                self.server.context.redis_client,
                open_v,
                hedge_v,
                funding_values=result.get("values"),
            )
            self._send_json(200, result)
            return

        if parsed.path == "/api/rolling-params":
            try:
                _, open_v, hedge_v, _ = self._resolve_payload_context(payload)
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
            try:
                _, open_v, hedge_v, _ = self._resolve_payload_context(payload)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            values = payload.get("values") or {}
            if not isinstance(values, dict):
                self._send_error(400, "values must be object")
                return
            mapping = normalize_threshold_mapping(values)
            if not mapping:
                self._send_error(400, "mapping is empty")
                return
            key = threshold_mapping_key("spread", open_v, hedge_v)
            written = write_threshold_mapping(
                self.server.context.redis_client, "spread", open_v, hedge_v, mapping
            )
            self._send_json(200, {"key": key, "count": written})
            return

        if parsed.path == "/api/funding-thresholds/sync":
            self._send_error(
                410,
                "funding threshold sync is deprecated; trade_signal reads rolling quantiles directly",
            )
            return

        if parsed.path == "/api/spread-thresholds/sync":
            try:
                _, open_v, hedge_v, key_suffix = self._resolve_payload_context(payload)
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

        if parsed.path in ("/api/amount-u", "/api/max-pos-u", "/api/hedge-offset-limits"):
            try:
                _, open_v, hedge_v, _ = self._resolve_payload_context(payload)
            except Exception as exc:
                self._send_error(400, str(exc))
                return
            env_name = infer_dir_prefix_from_cwd() or ""
            if not env_name:
                self._send_error(400, "env_name unavailable (no cwd prefix)")
                return
            rds = self.server.context.redis_client
            values = payload.get("values")
            try:
                if parsed.path == "/api/amount-u":
                    result = ps_overrides.write_amount_u(rds, env_name, open_v, hedge_v, values)
                elif parsed.path == "/api/max-pos-u":
                    result = ps_overrides.write_max_pos_u(rds, env_name, open_v, hedge_v, values)
                else:
                    result = ps_overrides.write_hedge_offset_limits(
                        rds, env_name, open_v, hedge_v, values
                    )
            except ValueError as exc:
                self._send_error(400, str(exc))
                return
            except Exception as exc:
                self._send_error(500, f"write failed: {exc}")
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

    default_open_venue = args.default_open_venue
    default_hedge_venue = args.default_hedge_venue
    if (not default_open_venue or not default_hedge_venue) and infer_default_venues_from_cwd():
        inferred_open, inferred_hedge = infer_default_venues_from_cwd() or ("", "")
        default_open_venue = default_open_venue or inferred_open
        default_hedge_venue = default_hedge_venue or inferred_hedge

    default_exchange = normalize_exchange(args.default_exchange)
    if default_open_venue:
        default_exchange = exchange_from_venue(default_open_venue) or default_exchange
    if default_exchange not in SUPPORTED_EXCHANGES:
        print(
            f"❌ 默认交易所 {default_exchange} 不在支持列表 {SUPPORTED_EXCHANGES}",
            file=sys.stderr,
        )
        return 2

    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    context = ServerContext(
        redis_client=rds,
        default_exchange=default_exchange,
        default_open_venue=default_open_venue,
        default_hedge_venue=default_hedge_venue,
    )
    try:
        server = FrConfigServer((args.host, args.port), RequestHandler, context)
    except OSError as exc:
        print(
            f"❌ 无法监听 {args.host}:{args.port}，端口可能被占用: {exc}",
            file=sys.stderr,
        )
        return 2
    print(
        f"🚀 intra_config_server started on http://{args.host}:{args.port} "
        f"(default_open={default_open_venue or '-'}, default_hedge={default_hedge_venue or '-'})"
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
