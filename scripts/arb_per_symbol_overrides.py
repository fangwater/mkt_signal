"""arb 三分支（intra/cross/fr）共用的 per-symbol 覆盖工具。

存储模型与 mm 对齐，但 redis key 用 (env, open, hedge) 三元组定位
（与 Rust 端 strategy_loader.rs / pre_trade/params_load.rs 已经在读的
键名严格一致）：

- amount_u                : `<env>:<open>:<hedge>:amount_u_overrides`
- max_pos_u               : `<env>:<open>:<hedge>:max_pos_u_overrides`
- taker_decsion_model      : `<env>:<open>:<hedge>:taker_decsion_model_overrides`
- hedge_price_offset_limits : 合并 STRING `<env>:<open>:<hedge>:hedge_price_offset_limits`
                              JSON {symbol: {hedge_price_offset_limit_lower,
                                             hedge_price_offset_limit_upper}}
- hedge_price_offset_limit_upper : 拆分 STRING（与合并键同步写入，作为 fallback）
- hedge_price_offset_limit_lower : 同上

写 hedge_offset_limits 合并键时同时刷新 upper/lower 两个拆分键，让 Rust
loader 在读到合并键不存在时仍能 fallback。读 hedge_offset_limits 时若
合并键缺失，则尝试用拆分键拼回来（与 mm_config_server 同语义）。
"""

from __future__ import annotations

import json
import math
import re
from typing import Any, Dict


def make_amount_u_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:amount_u_overrides"


def make_max_pos_u_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:max_pos_u_overrides"


def make_taker_decision_model_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:taker_decsion_model_overrides"


def make_hedge_offset_limits_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:hedge_price_offset_limits"


def make_hedge_offset_upper_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:hedge_price_offset_limit_upper"


def make_hedge_offset_lower_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:hedge_price_offset_limit_lower"


def make_open_offset_lower_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:open_offset_lower_overrides"


def normalize_amount_u_symbol(raw: Any) -> str:
    """与 mm_config_server.normalize_amount_u_symbol 等价：丢分隔符、转大写。"""
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
        if not (math.isfinite(amount_u) and amount_u > 0.0):
            raise ValueError(f"amount_u must be finite and > 0 for {symbol}: {raw_value}")
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
        if not (math.isfinite(max_pos_u) and max_pos_u > 0.0):
            raise ValueError(f"max_pos_u must be finite and > 0 for {symbol}: {raw_value}")
        normalized[symbol] = max_pos_u
    return dict(sorted(normalized.items()))


def dumps_max_pos_u_mapping(values: Dict[str, float]) -> str:
    ordered = {symbol: float(f"{values[symbol]:.12g}") for symbol in sorted(values.keys())}
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def normalize_open_offset_lower_mapping(values: Any) -> Dict[str, float]:
    if values is None:
        return {}
    if not isinstance(values, dict):
        raise ValueError("open_offset_lower values must be an object")
    normalized: Dict[str, float] = {}
    for raw_symbol, raw_value in values.items():
        symbol = normalize_amount_u_symbol(raw_symbol)
        try:
            value = float(raw_value)
        except Exception as exc:
            raise ValueError(f"invalid open_offset_lower for {symbol}: {raw_value}") from exc
        if not (math.isfinite(value) and value >= 0.0):
            raise ValueError(
                f"open_offset_lower must be finite and >= 0 for {symbol}: {raw_value}"
            )
        normalized[symbol] = value
    return dict(sorted(normalized.items()))


def dumps_open_offset_lower_mapping(values: Dict[str, float]) -> str:
    ordered = {symbol: float(f"{values[symbol]:.12g}") for symbol in sorted(values.keys())}
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def _coerce_positive_int(raw_value: Any, field_name: str, symbol: str) -> int:
    if isinstance(raw_value, bool):
        raise ValueError(f"{field_name} must be a positive integer for {symbol}: {raw_value}")
    try:
        if isinstance(raw_value, float) and not raw_value.is_integer():
            raise ValueError
        value = int(raw_value)
    except Exception as exc:
        raise ValueError(f"{field_name} must be a positive integer for {symbol}: {raw_value}") from exc
    if value <= 0:
        raise ValueError(f"{field_name} must be > 0 for {symbol}: {raw_value}")
    return value


def _coerce_percentile(raw_value: Any, field_name: str, symbol: str) -> float:
    try:
        value = float(raw_value)
    except Exception as exc:
        raise ValueError(f"{field_name} must be a percentile for {symbol}: {raw_value}") from exc
    if not (math.isfinite(value) and 0.0 <= value <= 100.0):
        raise ValueError(f"{field_name} must be in [0,100] for {symbol}: {raw_value}")
    return value


def normalize_taker_decision_model_mapping(values: Any) -> Dict[str, Dict[str, Any]]:
    if values is None:
        return {}
    if not isinstance(values, dict):
        raise ValueError("taker_decision_model values must be an object")
    normalized: Dict[str, Dict[str, Any]] = {}
    disallowed_rolling_fields = {"rolling_n", "rolling_window", "window"}
    allowed_fields = {
        "keep_long_percentile",
        "keep_long",
        "keep_short_percentile",
        "keep_short",
        "open_cancel_long_percentile",
        "open_cancel_long",
        "open_cancel_short_percentile",
        "open_cancel_short",
    }
    for raw_symbol, raw_cfg in values.items():
        symbol = normalize_amount_u_symbol(raw_symbol)
        if raw_cfg is None:
            raw_cfg = {}
        if not isinstance(raw_cfg, dict):
            raise ValueError(f"taker_decision_model config for {symbol} must be an object")
        rolling_fields = sorted(str(field) for field in raw_cfg.keys() if str(field) in disallowed_rolling_fields)
        if rolling_fields:
            raise ValueError(
                f"taker_decision_model config for {symbol} no longer accepts rolling fields: "
                f"{', '.join(rolling_fields)}; configure model_pub score_rolling instead"
            )
        unknown = sorted(str(field) for field in raw_cfg.keys() if str(field) not in allowed_fields)
        if unknown:
            raise ValueError(f"taker_decision_model config for {symbol} has unknown fields: {', '.join(unknown)}")
        cfg: Dict[str, Any] = {}

        keep_long_raw = raw_cfg.get("keep_long_percentile", raw_cfg.get("keep_long"))
        keep_short_raw = raw_cfg.get("keep_short_percentile", raw_cfg.get("keep_short"))
        open_cancel_long_raw = raw_cfg.get(
            "open_cancel_long_percentile", raw_cfg.get("open_cancel_long")
        )
        open_cancel_short_raw = raw_cfg.get(
            "open_cancel_short_percentile", raw_cfg.get("open_cancel_short")
        )
        if keep_long_raw is not None:
            cfg["keep_long_percentile"] = _coerce_percentile(
                keep_long_raw, "keep_long_percentile", symbol
            )
        if keep_short_raw is not None:
            cfg["keep_short_percentile"] = _coerce_percentile(
                keep_short_raw, "keep_short_percentile", symbol
            )
        if open_cancel_long_raw is not None:
            cfg["open_cancel_long_percentile"] = _coerce_percentile(
                open_cancel_long_raw, "open_cancel_long_percentile", symbol
            )
        if open_cancel_short_raw is not None:
            cfg["open_cancel_short_percentile"] = _coerce_percentile(
                open_cancel_short_raw, "open_cancel_short_percentile", symbol
            )
        if (
            "keep_long_percentile" in cfg
            and "keep_short_percentile" in cfg
            and cfg["keep_short_percentile"] > cfg["keep_long_percentile"]
        ):
            raise ValueError(
                f"keep_short_percentile must be <= keep_long_percentile for {symbol}: "
                f"short={cfg['keep_short_percentile']}, long={cfg['keep_long_percentile']}"
            )
        if (
            "open_cancel_long_percentile" in cfg
            and "open_cancel_short_percentile" in cfg
            and cfg["open_cancel_short_percentile"] > cfg["open_cancel_long_percentile"]
        ):
            raise ValueError(
                f"open_cancel_short_percentile must be <= open_cancel_long_percentile for {symbol}: "
                f"short={cfg['open_cancel_short_percentile']}, long={cfg['open_cancel_long_percentile']}"
            )
        normalized[symbol] = cfg
    return dict(sorted(normalized.items()))


def dumps_taker_decision_model_mapping(values: Dict[str, Dict[str, Any]]) -> str:
    ordered: Dict[str, Dict[str, Any]] = {}
    for symbol in sorted(values.keys()):
        raw_cfg = values[symbol]
        cfg: Dict[str, Any] = {}
        if "keep_long_percentile" in raw_cfg:
            cfg["keep_long_percentile"] = float(f"{float(raw_cfg['keep_long_percentile']):.12g}")
        if "keep_short_percentile" in raw_cfg:
            cfg["keep_short_percentile"] = float(f"{float(raw_cfg['keep_short_percentile']):.12g}")
        if "open_cancel_long_percentile" in raw_cfg:
            cfg["open_cancel_long_percentile"] = float(
                f"{float(raw_cfg['open_cancel_long_percentile']):.12g}"
            )
        if "open_cancel_short_percentile" in raw_cfg:
            cfg["open_cancel_short_percentile"] = float(
                f"{float(raw_cfg['open_cancel_short_percentile']):.12g}"
            )
        ordered[symbol] = cfg
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def normalize_hedge_offset_mapping(values: Any, field_name: str) -> Dict[str, float]:
    if values is None:
        return {}
    if not isinstance(values, dict):
        raise ValueError(f"{field_name} values must be an object")
    normalized: Dict[str, float] = {}
    for raw_symbol, raw_value in values.items():
        symbol = normalize_amount_u_symbol(raw_symbol)
        try:
            value = float(raw_value)
        except Exception as exc:
            raise ValueError(f"invalid {field_name} for {symbol}: {raw_value}") from exc
        if not (math.isfinite(value) and value > 0.0):
            raise ValueError(f"{field_name} must be finite and > 0 for {symbol}: {raw_value}")
        normalized[symbol] = value
    return dict(sorted(normalized.items()))


def dumps_hedge_offset_mapping(values: Dict[str, float]) -> str:
    ordered = {symbol: float(f"{values[symbol]:.12g}") for symbol in sorted(values.keys())}
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def normalize_hedge_offset_limits_mapping(values: Any) -> Dict[str, Dict[str, float]]:
    if values is None:
        return {}
    if not isinstance(values, dict):
        raise ValueError("hedge offset limit values must be an object")
    normalized: Dict[str, Dict[str, float]] = {}
    for raw_symbol, raw_limits in values.items():
        symbol = normalize_amount_u_symbol(raw_symbol)
        if not isinstance(raw_limits, dict):
            raise ValueError(f"hedge offset limits for {symbol} must be an object")
        missing = [
            field
            for field in (
                "hedge_price_offset_limit_lower",
                "hedge_price_offset_limit_upper",
            )
            if field not in raw_limits
        ]
        if missing:
            raise ValueError(
                f"hedge offset limits for {symbol} missing fields: {', '.join(missing)}"
            )
        try:
            lower = float(raw_limits["hedge_price_offset_limit_lower"])
            upper = float(raw_limits["hedge_price_offset_limit_upper"])
        except Exception as exc:
            raise ValueError(f"invalid hedge offset limits for {symbol}: {raw_limits}") from exc
        if not (math.isfinite(lower) and math.isfinite(upper)):
            raise ValueError(f"hedge offset limits must be finite for {symbol}: {raw_limits}")
        if not (lower > 0.0 and upper >= lower):
            raise ValueError(
                f"hedge offset limits must satisfy 0 < lower <= upper for {symbol}: "
                f"lower={lower}, upper={upper}"
            )
        normalized[symbol] = {
            "hedge_price_offset_limit_lower": lower,
            "hedge_price_offset_limit_upper": upper,
        }
    return dict(sorted(normalized.items()))


def dumps_hedge_offset_limits_mapping(values: Dict[str, Dict[str, float]]) -> str:
    ordered: Dict[str, Dict[str, float]] = {}
    for symbol in sorted(values.keys()):
        limits = values[symbol]
        ordered[symbol] = {
            "hedge_price_offset_limit_lower": float(
                f"{limits['hedge_price_offset_limit_lower']:.12g}"
            ),
            "hedge_price_offset_limit_upper": float(
                f"{limits['hedge_price_offset_limit_upper']:.12g}"
            ),
        }
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def combine_hedge_offset_maps(
    lower_values: Dict[str, float],
    upper_values: Dict[str, float],
) -> Dict[str, Dict[str, float]]:
    """把拆分版 upper/lower 拼回合并版（缺一个的 symbol 直接丢弃）。"""
    symbols = sorted(set(lower_values.keys()) | set(upper_values.keys()))
    combined: Dict[str, Dict[str, float]] = {}
    for symbol in symbols:
        if symbol not in lower_values or symbol not in upper_values:
            continue
        combined[symbol] = {
            "hedge_price_offset_limit_lower": lower_values[symbol],
            "hedge_price_offset_limit_upper": upper_values[symbol],
        }
    return normalize_hedge_offset_limits_mapping(combined)


def _decode_redis_str(raw: Any) -> str:
    if isinstance(raw, bytes):
        return raw.decode("utf-8", "ignore")
    return str(raw)


# ---- read / write helpers -------------------------------------------------


def read_amount_u(rds, env_name: str, open_venue: str, hedge_venue: str) -> Dict[str, Any]:
    key = make_amount_u_key(env_name, open_venue, hedge_venue)
    raw = rds.get(key)
    if raw is None:
        values: Dict[str, float] = {}
    else:
        try:
            decoded = json.loads(_decode_redis_str(raw))
        except Exception as exc:
            raise ValueError(f"invalid JSON in {key}: {exc}") from exc
        values = normalize_amount_u_mapping(decoded)
    return {"key": key, "values": values, "count": len(values)}


def write_amount_u(
    rds, env_name: str, open_venue: str, hedge_venue: str, values: Any
) -> Dict[str, Any]:
    key = make_amount_u_key(env_name, open_venue, hedge_venue)
    normalized = normalize_amount_u_mapping(values)
    rds.set(key, dumps_amount_u_mapping(normalized))
    return {"key": key, "values": normalized, "count": len(normalized)}


def read_max_pos_u(rds, env_name: str, open_venue: str, hedge_venue: str) -> Dict[str, Any]:
    key = make_max_pos_u_key(env_name, open_venue, hedge_venue)
    raw = rds.get(key)
    if raw is None:
        values: Dict[str, float] = {}
    else:
        try:
            decoded = json.loads(_decode_redis_str(raw))
        except Exception as exc:
            raise ValueError(f"invalid JSON in {key}: {exc}") from exc
        values = normalize_max_pos_u_mapping(decoded)
    return {"key": key, "values": values, "count": len(values)}


def write_max_pos_u(
    rds, env_name: str, open_venue: str, hedge_venue: str, values: Any
) -> Dict[str, Any]:
    key = make_max_pos_u_key(env_name, open_venue, hedge_venue)
    normalized = normalize_max_pos_u_mapping(values)
    rds.set(key, dumps_max_pos_u_mapping(normalized))
    return {"key": key, "values": normalized, "count": len(normalized)}


def read_taker_decision_model(
    rds, env_name: str, open_venue: str, hedge_venue: str
) -> Dict[str, Any]:
    key = make_taker_decision_model_key(env_name, open_venue, hedge_venue)
    raw = rds.get(key)
    if raw is None:
        values: Dict[str, Dict[str, Any]] = {}
    else:
        try:
            decoded = json.loads(_decode_redis_str(raw))
        except Exception as exc:
            raise ValueError(f"invalid JSON in {key}: {exc}") from exc
        values = normalize_taker_decision_model_mapping(decoded)
    return {"key": key, "values": values, "count": len(values)}


def write_taker_decision_model(
    rds, env_name: str, open_venue: str, hedge_venue: str, values: Any
) -> Dict[str, Any]:
    key = make_taker_decision_model_key(env_name, open_venue, hedge_venue)
    normalized = normalize_taker_decision_model_mapping(values)
    rds.set(key, dumps_taker_decision_model_mapping(normalized))
    return {"key": key, "values": normalized, "count": len(normalized)}


def read_open_offset_lower(
    rds, env_name: str, open_venue: str, hedge_venue: str
) -> Dict[str, Any]:
    key = make_open_offset_lower_key(env_name, open_venue, hedge_venue)
    raw = rds.get(key)
    if raw is None:
        values: Dict[str, float] = {}
    else:
        try:
            decoded = json.loads(_decode_redis_str(raw))
        except Exception as exc:
            raise ValueError(f"invalid JSON in {key}: {exc}") from exc
        values = normalize_open_offset_lower_mapping(decoded)
    return {"key": key, "values": values, "count": len(values)}


def write_open_offset_lower(
    rds, env_name: str, open_venue: str, hedge_venue: str, values: Any
) -> Dict[str, Any]:
    key = make_open_offset_lower_key(env_name, open_venue, hedge_venue)
    normalized = normalize_open_offset_lower_mapping(values)
    rds.set(key, dumps_open_offset_lower_mapping(normalized))
    return {"key": key, "values": normalized, "count": len(normalized)}


def _read_hedge_offset_split(
    rds, key: str, field_name: str
) -> Dict[str, float]:
    raw = rds.get(key)
    if raw is None:
        return {}
    try:
        decoded = json.loads(_decode_redis_str(raw))
    except Exception as exc:
        raise ValueError(f"invalid JSON in {key}: {exc}") from exc
    return normalize_hedge_offset_mapping(decoded, field_name)


def read_hedge_offset_limits(
    rds, env_name: str, open_venue: str, hedge_venue: str
) -> Dict[str, Any]:
    key = make_hedge_offset_limits_key(env_name, open_venue, hedge_venue)
    upper_key = make_hedge_offset_upper_key(env_name, open_venue, hedge_venue)
    lower_key = make_hedge_offset_lower_key(env_name, open_venue, hedge_venue)

    raw = rds.get(key)
    if raw is None:
        lower = _read_hedge_offset_split(rds, lower_key, "hedge_price_offset_limit_lower")
        upper = _read_hedge_offset_split(rds, upper_key, "hedge_price_offset_limit_upper")
        values = combine_hedge_offset_maps(lower, upper)
        source = "split"
    else:
        try:
            decoded = json.loads(_decode_redis_str(raw))
        except Exception as exc:
            raise ValueError(f"invalid JSON in {key}: {exc}") from exc
        values = normalize_hedge_offset_limits_mapping(decoded)
        source = "combined"
    return {
        "key": key,
        "values": values,
        "count": len(values),
        "source": source,
        "split_keys": {"upper": upper_key, "lower": lower_key},
    }


def write_hedge_offset_limits(
    rds, env_name: str, open_venue: str, hedge_venue: str, values: Any
) -> Dict[str, Any]:
    """合并键 + 拆分键同时写入，与 mm_config_server.write_hedge_offset_limits 同语义。"""
    key = make_hedge_offset_limits_key(env_name, open_venue, hedge_venue)
    upper_key = make_hedge_offset_upper_key(env_name, open_venue, hedge_venue)
    lower_key = make_hedge_offset_lower_key(env_name, open_venue, hedge_venue)
    normalized = normalize_hedge_offset_limits_mapping(values)
    payload = dumps_hedge_offset_limits_mapping(normalized)
    upper_values = {
        symbol: limits["hedge_price_offset_limit_upper"]
        for symbol, limits in normalized.items()
    }
    lower_values = {
        symbol: limits["hedge_price_offset_limit_lower"]
        for symbol, limits in normalized.items()
    }
    rds.set(key, payload)
    rds.set(upper_key, dumps_hedge_offset_mapping(upper_values))
    rds.set(lower_key, dumps_hedge_offset_mapping(lower_values))
    return {
        "key": key,
        "values": normalized,
        "count": len(normalized),
        "split_keys": {"upper": upper_key, "lower": lower_key},
    }


# ---- HTML / JS panel snippets ---------------------------------------------


def render_per_symbol_panels_html() -> str:
    """三个 textarea 直编面板，与 mm_config_server 保持一致的视觉与交互。

    依赖前端已有的 .panel / .section-header / .actions / .hint / .status / .mono 样式
    （三个 arb config_server 都已包含）。
    """
    return """
    <section class="panel">
      <div class="section-header">
        <h2>Per-Symbol Amount U Overrides</h2>
        <div class="actions">
          <button id="load-amount-u" class="secondary">读取</button>
          <button id="reset-amount-u" class="ghost">示例</button>
          <button id="save-amount-u">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构 <code>{"SYMBOL": amount_u}</code>。Redis String
        <code>&lt;env_name&gt;:&lt;open_venue&gt;:&lt;hedge_venue&gt;:amount_u_overrides</code>，
        命中即覆盖 strategy_params 的 <code>order_amount</code>。
      </div>
      <div class="hint">示例：</div>
      <pre id="amount-u-example" class="mono"></pre>
      <textarea id="amount-u-text" class="mono" spellcheck="false"></textarea>
      <div id="amount-u-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Per-Symbol Max Pos U Overrides</h2>
        <div class="actions">
          <button id="load-max-pos-u" class="secondary">读取</button>
          <button id="reset-max-pos-u" class="ghost">示例</button>
          <button id="save-max-pos-u">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构 <code>{"SYMBOL": max_pos_u}</code>。Redis String
        <code>&lt;env_name&gt;:&lt;open_venue&gt;:&lt;hedge_venue&gt;:max_pos_u_overrides</code>，
        命中即覆盖 risk_params 的 <code>max_pos_u</code>。
      </div>
      <div class="hint">示例：</div>
      <pre id="max-pos-u-example" class="mono"></pre>
      <textarea id="max-pos-u-text" class="mono" spellcheck="false"></textarea>
      <div id="max-pos-u-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Per-Symbol Hedge Price Offset Limit Overrides</h2>
        <div class="actions">
          <button id="load-hedge-offset-limits" class="secondary">读取</button>
          <button id="reset-hedge-offset-limits" class="ghost">示例</button>
          <button id="save-hedge-offset-limits">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构
        <code>{"SYMBOL":{"hedge_price_offset_limit_lower":0.0005,"hedge_price_offset_limit_upper":0.005}}</code>。
        保存时同时写入合并键
        <code>&lt;env&gt;:&lt;open&gt;:&lt;hedge&gt;:hedge_price_offset_limits</code>
        与拆分键
        <code>...:hedge_price_offset_limit_upper</code> /
        <code>...:hedge_price_offset_limit_lower</code>，命中即按 symbol
        覆盖对冲侧报价范围上下界。
      </div>
      <div class="hint">示例：</div>
      <pre id="hedge-offset-limits-example" class="mono"></pre>
      <textarea id="hedge-offset-limits-text" class="mono" spellcheck="false"></textarea>
      <div id="hedge-offset-limits-status" class="status"></div>
    </section>

    <section class="panel">
      <div class="section-header">
        <h2>Per-Symbol Open Offset Lower Overrides</h2>
        <div class="actions">
          <button id="load-open-offset-lower" class="secondary">读取</button>
          <button id="reset-open-offset-lower" class="ghost">示例</button>
          <button id="save-open-offset-lower">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构 <code>{"SYMBOL": open_offset_lower}</code>，单位价格分数（0.001=10bps）。
        Redis String <code>&lt;env_name&gt;:&lt;open_venue&gt;:&lt;hedge_venue&gt;:open_offset_lower_overrides</code>。
        Arb 开仓内层 offset 取 <code>max(vol_band_scale[0]*volatility, lower)</code>，
        默认 0（不 clamp，保持 vol_band_scale 行为）。
      </div>
      <div class="hint">示例：</div>
      <pre id="open-offset-lower-example" class="mono"></pre>
      <textarea id="open-offset-lower-text" class="mono" spellcheck="false"></textarea>
      <div id="open-offset-lower-status" class="status"></div>
    </section>
"""


# 示例数据（reset 按钮填回）
PANEL_EXAMPLES_JSON: str = json.dumps(
    {
        "amount_u": {"BTCUSDT": 200, "ETHUSDT": 100},
        "max_pos_u": {"BTCUSDT": 5000, "ETHUSDT": 2000},
        "hedge_offset_limits": {
            "BTCUSDT": {
                "hedge_price_offset_limit_lower": 0.0005,
                "hedge_price_offset_limit_upper": 0.005,
            },
            "ETHUSDT": {
                "hedge_price_offset_limit_lower": 0.0006,
                "hedge_price_offset_limit_upper": 0.006,
            },
        },
        "open_offset_lower": {"BTCUSDT": 0.0008, "ETHUSDT": 0.0010},
    },
    ensure_ascii=False,
    separators=(",", ":"),
)


def render_per_symbol_panels_js() -> str:
    """对应三个面板的前端逻辑。

    JS 完全自包含（前缀 _ps* 避免与调用方命名冲突）：自带 `_psSetStatus` /
    `_psFormatError` / `_psApiUrl` / `_psFetch`，不依赖 mm 与 intra/cross/fr 各自
    略不同的 setStatus 签名。

    依赖调用方在嵌入此 JS 之前定义一个全局 `_psGetContext()` 函数，返回
    `{{ exchange, openVenue, hedgeVenue }}`（任意字段可缺失）。三种 config_server
    DOM 风格不一致：mm 用 `state.exchange` + 单 venue，intra/cross/fr 用 input
    元素读双 venue，让调用方自己取最简单。
    """
    return f"""
    const PER_SYMBOL_PANEL_EXAMPLES = {PANEL_EXAMPLES_JSON};

    function _psSetStatus(id, msg, level) {{
      const el = document.getElementById(id);
      if (!el) return;
      el.textContent = msg || '';
      const lv = level || '';
      el.className = lv ? ('status ' + lv) : 'status';
    }}

    function _psFormatError(err) {{
      if (err && typeof err === 'object' && err.message) return String(err.message);
      return String(err);
    }}

    function _psApiUrl(path) {{
      const pathname = window.location.pathname || '/';
      const withoutFile = pathname.endsWith('/index.html')
        ? pathname.slice(0, -'/index.html'.length)
        : pathname;
      const base = withoutFile.endsWith('/') ? withoutFile : withoutFile + '/';
      const clean = String(path || '').replace(/^\\//, '');
      return base + 'api/' + clean;
    }}

    function _psContext() {{
      try {{
        if (typeof _psGetContext === 'function') return _psGetContext() || {{}};
      }} catch (e) {{}}
      return {{}};
    }}

    function _psQs() {{
      const ctx = _psContext();
      const parts = [];
      if (ctx.exchange) parts.push('exchange=' + encodeURIComponent(ctx.exchange));
      if (ctx.openVenue) parts.push('open_venue=' + encodeURIComponent(ctx.openVenue));
      if (ctx.hedgeVenue) parts.push('hedge_venue=' + encodeURIComponent(ctx.hedgeVenue));
      return parts.length ? '?' + parts.join('&') : '';
    }}

    function _psBody(values) {{
      const ctx = _psContext();
      return JSON.stringify({{
        exchange: ctx.exchange || '',
        open_venue: ctx.openVenue || '',
        hedge_venue: ctx.hedgeVenue || '',
        values: values,
      }});
    }}

    async function _psFetch(path, options) {{
      options = options || {{}};
      const isPost = !!options.method;
      const url = _psApiUrl(path) + (isPost ? '' : _psQs());
      const resp = await fetch(url, Object.assign(
        {{headers: {{'Content-Type': 'application/json'}}}}, options));
      const text = await resp.text();
      let data = {{}};
      if (text) {{
        try {{ data = JSON.parse(text); }} catch (e) {{ throw new Error(text); }}
      }}
      if (!resp.ok) throw new Error(data.error || data.message || ('HTTP ' + resp.status));
      return data;
    }}

    async function loadAmountU() {{
      _psSetStatus('amount-u-status', '读取中...');
      try {{
        const data = await _psFetch('amount-u');
        document.getElementById('amount-u-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        _psSetStatus('amount-u-status',
          '已读取 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      }} catch (err) {{
        _psSetStatus('amount-u-status', '读取失败: ' + _psFormatError(err), 'err');
      }}
    }}

    async function saveAmountU() {{
      _psSetStatus('amount-u-status', '保存中...');
      let values;
      try {{ values = JSON.parse(document.getElementById('amount-u-text').value || '{{}}'); }}
      catch (err) {{ _psSetStatus('amount-u-status', 'JSON 解析失败: ' + err.message, 'err'); return; }}
      try {{
        const data = await _psFetch('amount-u', {{method: 'POST', body: _psBody(values)}});
        document.getElementById('amount-u-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        _psSetStatus('amount-u-status',
          '已保存 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      }} catch (err) {{
        _psSetStatus('amount-u-status', '保存失败: ' + _psFormatError(err), 'err');
      }}
    }}

    function resetAmountU() {{
      document.getElementById('amount-u-text').value =
        JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.amount_u || {{}}, null, 2);
      _psSetStatus('amount-u-status', '已填入示例，尚未写入 Redis', 'warn');
    }}

    async function loadMaxPosU() {{
      _psSetStatus('max-pos-u-status', '读取中...');
      try {{
        const data = await _psFetch('max-pos-u');
        document.getElementById('max-pos-u-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        _psSetStatus('max-pos-u-status',
          '已读取 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      }} catch (err) {{
        _psSetStatus('max-pos-u-status', '读取失败: ' + _psFormatError(err), 'err');
      }}
    }}

    async function saveMaxPosU() {{
      _psSetStatus('max-pos-u-status', '保存中...');
      let values;
      try {{ values = JSON.parse(document.getElementById('max-pos-u-text').value || '{{}}'); }}
      catch (err) {{ _psSetStatus('max-pos-u-status', 'JSON 解析失败: ' + err.message, 'err'); return; }}
      try {{
        const data = await _psFetch('max-pos-u', {{method: 'POST', body: _psBody(values)}});
        document.getElementById('max-pos-u-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        _psSetStatus('max-pos-u-status',
          '已保存 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      }} catch (err) {{
        _psSetStatus('max-pos-u-status', '保存失败: ' + _psFormatError(err), 'err');
      }}
    }}

    function resetMaxPosU() {{
      document.getElementById('max-pos-u-text').value =
        JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.max_pos_u || {{}}, null, 2);
      _psSetStatus('max-pos-u-status', '已填入示例，尚未写入 Redis', 'warn');
    }}

    async function loadHedgeOffsetLimits() {{
      _psSetStatus('hedge-offset-limits-status', '读取中...');
      try {{
        const data = await _psFetch('hedge-offset-limits');
        document.getElementById('hedge-offset-limits-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        const src = data.source ? ' source=' + data.source : '';
        _psSetStatus('hedge-offset-limits-status',
          '已读取 ' + (data.count || 0) + ' 个 symbol ' + (data.key || '') + src, 'ok');
      }} catch (err) {{
        _psSetStatus('hedge-offset-limits-status', '读取失败: ' + _psFormatError(err), 'err');
      }}
    }}

    async function saveHedgeOffsetLimits() {{
      _psSetStatus('hedge-offset-limits-status', '保存中...');
      let values;
      try {{ values = JSON.parse(document.getElementById('hedge-offset-limits-text').value || '{{}}'); }}
      catch (err) {{ _psSetStatus('hedge-offset-limits-status', 'JSON 解析失败: ' + err.message, 'err'); return; }}
      try {{
        const data = await _psFetch('hedge-offset-limits', {{method: 'POST', body: _psBody(values)}});
        document.getElementById('hedge-offset-limits-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        _psSetStatus('hedge-offset-limits-status',
          '已保存 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      }} catch (err) {{
        _psSetStatus('hedge-offset-limits-status', '保存失败: ' + _psFormatError(err), 'err');
      }}
    }}

    function resetHedgeOffsetLimits() {{
      document.getElementById('hedge-offset-limits-text').value =
        JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.hedge_offset_limits || {{}}, null, 2);
      _psSetStatus('hedge-offset-limits-status', '已填入示例，尚未写入 Redis', 'warn');
    }}

    async function loadOpenOffsetLower() {{
      _psSetStatus('open-offset-lower-status', '读取中...');
      try {{
        const data = await _psFetch('open-offset-lower');
        document.getElementById('open-offset-lower-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        _psSetStatus('open-offset-lower-status',
          '已读取 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      }} catch (err) {{
        _psSetStatus('open-offset-lower-status', '读取失败: ' + _psFormatError(err), 'err');
      }}
    }}

    async function saveOpenOffsetLower() {{
      _psSetStatus('open-offset-lower-status', '保存中...');
      let values;
      try {{ values = JSON.parse(document.getElementById('open-offset-lower-text').value || '{{}}'); }}
      catch (err) {{ _psSetStatus('open-offset-lower-status', 'JSON 解析失败: ' + err.message, 'err'); return; }}
      try {{
        const data = await _psFetch('open-offset-lower', {{method: 'POST', body: _psBody(values)}});
        document.getElementById('open-offset-lower-text').value =
          JSON.stringify(data.values || {{}}, null, 2);
        _psSetStatus('open-offset-lower-status',
          '已保存 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      }} catch (err) {{
        _psSetStatus('open-offset-lower-status', '保存失败: ' + _psFormatError(err), 'err');
      }}
    }}

    function resetOpenOffsetLower() {{
      document.getElementById('open-offset-lower-text').value =
        JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.open_offset_lower || {{}}, null, 2);
      _psSetStatus('open-offset-lower-status', '已填入示例，尚未写入 Redis', 'warn');
    }}

    function bindPerSymbolPanels() {{
      const map = [
        ['load-amount-u', loadAmountU],
        ['save-amount-u', saveAmountU],
        ['reset-amount-u', resetAmountU],
        ['load-max-pos-u', loadMaxPosU],
        ['save-max-pos-u', saveMaxPosU],
        ['reset-max-pos-u', resetMaxPosU],
        ['load-hedge-offset-limits', loadHedgeOffsetLimits],
        ['save-hedge-offset-limits', saveHedgeOffsetLimits],
        ['reset-hedge-offset-limits', resetHedgeOffsetLimits],
        ['load-open-offset-lower', loadOpenOffsetLower],
        ['save-open-offset-lower', saveOpenOffsetLower],
        ['reset-open-offset-lower', resetOpenOffsetLower],
      ];
      for (const [id, fn] of map) {{
        const el = document.getElementById(id);
        if (el) el.addEventListener('click', fn);
      }}
      const exA = document.getElementById('amount-u-example');
      if (exA) exA.textContent = JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.amount_u || {{}}, null, 2);
      const exM = document.getElementById('max-pos-u-example');
      if (exM) exM.textContent = JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.max_pos_u || {{}}, null, 2);
      const exH = document.getElementById('hedge-offset-limits-example');
      if (exH) exH.textContent = JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.hedge_offset_limits || {{}}, null, 2);
      const exO = document.getElementById('open-offset-lower-example');
      if (exO) exO.textContent = JSON.stringify(PER_SYMBOL_PANEL_EXAMPLES.open_offset_lower || {{}}, null, 2);
    }}
"""


def render_taker_decision_model_panel_html() -> str:
    """intra 专用：per-symbol lazy taker model rolling/threshold overrides。"""
    return """
    <section class="panel">
      <div class="section-header">
        <h2>Per-Symbol Taker Decision Model Overrides</h2>
        <div class="actions">
          <button id="load-taker-decision-model" class="secondary">读取</button>
          <button id="reset-taker-decision-model" class="ghost">示例</button>
          <button id="save-taker-decision-model">保存</button>
        </div>
      </div>
      <div class="hint">
        JSON 结构
        <code>{"SYMBOL":{"keep_long_percentile":80,"keep_short_percentile":20,"open_cancel_long_percentile":80,"open_cancel_short_percentile":20}}</code>。
        Redis String <code>&lt;env_name&gt;:&lt;open_venue&gt;:&lt;hedge_venue&gt;:taker_decsion_model_overrides</code>。
        收到 model value msg 的 symbol 会使用 lazy taker decision model；这个 JSON 只覆盖对应 symbol 的阈值。
        <code>open_cancel_long_percentile</code> 表示模型分位 &lt; 阈值时撤掉开仓多单；
        <code>open_cancel_short_percentile</code> 表示模型分位 &gt; 阈值时撤掉开仓空单。
        rolling/score_quantile 由 model_pub score_rolling 维护。未覆盖字段使用 strategy params 里的全局默认值。未收到 model msg 的 symbol 继续走正常 MT。
      </div>
      <div class="hint">示例：</div>
      <pre id="taker-decision-model-example" class="mono"></pre>
      <textarea id="taker-decision-model-text" class="mono" spellcheck="false"></textarea>
      <div id="taker-decision-model-status" class="status"></div>
    </section>
"""


def render_taker_decision_model_panel_js() -> str:
    return r"""
    const TAKER_DECISION_MODEL_PANEL_EXAMPLE = {
      "BTCUSDT": {
        "keep_long_percentile": 80,
        "keep_short_percentile": 20,
        "open_cancel_long_percentile": 80,
        "open_cancel_short_percentile": 20
      },
      "ETHUSDT": {
        "keep_long_percentile": 85,
        "keep_short_percentile": 15,
        "open_cancel_long_percentile": 82,
        "open_cancel_short_percentile": 18
      }
    };

    async function loadTakerDecisionModel() {
      _psSetStatus('taker-decision-model-status', '读取中...');
      try {
        const data = await _psFetch('taker-decision-model');
        document.getElementById('taker-decision-model-text').value =
          JSON.stringify(data.values || {}, null, 2);
        _psSetStatus('taker-decision-model-status',
          '已读取 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      } catch (err) {
        _psSetStatus('taker-decision-model-status', '读取失败: ' + _psFormatError(err), 'err');
      }
    }

    async function saveTakerDecisionModel() {
      _psSetStatus('taker-decision-model-status', '保存中...');
      let values;
      try { values = JSON.parse(document.getElementById('taker-decision-model-text').value || '{}'); }
      catch (err) { _psSetStatus('taker-decision-model-status', 'JSON 解析失败: ' + err.message, 'err'); return; }
      try {
        const data = await _psFetch('taker-decision-model', {method: 'POST', body: _psBody(values)});
        document.getElementById('taker-decision-model-text').value =
          JSON.stringify(data.values || {}, null, 2);
        _psSetStatus('taker-decision-model-status',
          '已保存 ' + (data.count || 0) + ' 个 symbol ' + (data.key || ''), 'ok');
      } catch (err) {
        _psSetStatus('taker-decision-model-status', '保存失败: ' + _psFormatError(err), 'err');
      }
    }

    function resetTakerDecisionModel() {
      document.getElementById('taker-decision-model-text').value =
        JSON.stringify(TAKER_DECISION_MODEL_PANEL_EXAMPLE, null, 2);
      _psSetStatus('taker-decision-model-status', '已填入示例，尚未写入 Redis', 'warn');
    }

    function bindTakerDecisionModelPanel() {
      const map = [
        ['load-taker-decision-model', loadTakerDecisionModel],
        ['save-taker-decision-model', saveTakerDecisionModel],
        ['reset-taker-decision-model', resetTakerDecisionModel],
      ];
      for (const [id, fn] of map) {
        const el = document.getElementById(id);
        if (el) el.addEventListener('click', fn);
      }
      const ex = document.getElementById('taker-decision-model-example');
      if (ex) ex.textContent = JSON.stringify(TAKER_DECISION_MODEL_PANEL_EXAMPLE, null, 2);
    }

    const _psBaseBindPerSymbolPanelsForTakerDecisionModel = bindPerSymbolPanels;
    bindPerSymbolPanels = function() {
      _psBaseBindPerSymbolPanelsForTakerDecisionModel();
      bindTakerDecisionModelPanel();
    };
"""
