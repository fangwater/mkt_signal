#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 cta 信号配置同步到 Redis（env 作用域 STRING key，单 JSON 对象）。

写入的 Redis key：
  - {env}:cta_rules    - 例如 binance-cta-rx01:cta_rules

`{env}:cta_rules` 存放信号配置对象（model_service/分位/方向）；
执行/网格参数（open_offsets、单笔名义、因子退出、trailing 等）归
`{env}:cta_strategy_params:{open}:{hedge}` hash（本脚本提供 parse_exec_params
供 config server 校验写入）。

校验规则与 trade_signal `cta_config.rs` 的 `CtaRule::validate` 保持一致
（在写入前本地全量校验，避免把 loader 会拒绝的配置写进 Redis）。

env-name 推断：--env-name，或 CWD 目录名 <exchange>-cta-<tag>。

用法：
  scripts/sync_cta_rules.py --env-name binance-cta-rx01 --file signal.json
  scripts/sync_cta_rules.py --file signal.json --dry-run    # 只校验不写
  scripts/sync_cta_rules.py --clear                          # 写入 [] 清空信号
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

RULE_ID_MAX_LEN = 32
MAX_OPEN_LEVELS = 8
MAX_OPEN_OFFSET = 0.01
RULE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

TRADE_SIDES = {"long", "buy", "short", "sell", "both", "long_short", "long,short", "short,long"}
# 与 RawCtaRule 的 serde 字段一一对应（deny_unknown_fields）。
ALLOWED_FIELDS = {
    "rule_id",
    "model_service",
    "trade_sides",
    "nq_change_enabled",
    "cooldown_seconds",
    "application",
    "order_notional_usdt",
    "open_offsets",
    "open_ttl_seconds",
    "factor_exit_quantile_long",
    "factor_exit_quantile_short",
    "trailing_stop_enabled",
    "trailing_stop_trigger_step",
    "trailing_stop_move_step",
    "enabled",
}

# ---- 信号配置（{env}:cta_rules 单对象）与执行参数（strategy hash）的字段分区 ----
# 信号字段：config server 的「CTA 信号」面板编辑这些；rule_id 可选透传。
SIGNAL_FIELD_TYPES: Dict[str, str] = {
    "model_service": "str",
    "enabled": "bool",
    "trade_sides": "str",
    "application": "str",
    "nq_change_enabled": "bool",
    "cooldown_seconds": "int",
}

# 执行/网格字段：存于 env-scoped cta_strategy_params hash（String->String），
# Rust CtaExecOverrides 加载时覆盖到规则上。
EXEC_FIELD_TYPES: Dict[str, str] = {
    "order_notional_usdt": "float",
    "open_offsets": "offsets",
    "open_ttl_seconds": "int",
    "factor_exit_quantile_long": "float",
    "factor_exit_quantile_short": "float",
    "trailing_stop_enabled": "bool",
    "trailing_stop_trigger_step": "float",
    "trailing_stop_move_step": "float",
}
EXEC_FIELD_DEFAULTS: Dict[str, Any] = {
    "order_notional_usdt": 100.0,
    "open_offsets": [0.0, 0.0001, 0.0003, 0.0005],
    "open_ttl_seconds": 120,
    "factor_exit_quantile_long": 0.3,
    "factor_exit_quantile_short": 0.7,
    "trailing_stop_enabled": True,
    "trailing_stop_trigger_step": 0.01,
    "trailing_stop_move_step": 0.005,
}

_BOOL_TRUE = {"true", "1", "yes", "on"}
_BOOL_FALSE = {"false", "0", "no", "off"}

# serde 默认值，与 crates/trade_signal/src/cta_config.rs RawCtaRule 一一对应。
# config server 的 cta rules 面板用它做表单预填；validate_rule 里的内联默认值必须保持一致。
RULE_DEFAULTS: Dict[str, Any] = {
    "rule_id": "",
    "model_service": "intra-binance-futures-1m-baseline_035",
    "trade_sides": "both",
    "nq_change_enabled": True,
    "cooldown_seconds": 0,
    "application": "each_bar",
    "order_notional_usdt": 100.0,
    "open_offsets": [0.0, 0.0001, 0.0003, 0.0005],
    "open_ttl_seconds": 120,
    "factor_exit_quantile_long": 0.3,
    "factor_exit_quantile_short": 0.7,
    "trailing_stop_enabled": True,
    "trailing_stop_trigger_step": 0.01,
    "trailing_stop_move_step": 0.005,
    "enabled": True,
}


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def infer_cta_env_from_name(name: str) -> bool:
    n = (name or "").strip().lower()
    return re.match(r"^[a-z0-9]+[-_]cta[-_][a-z0-9]", n) is not None


def cta_rules_key(env_name: str) -> str:
    env = (env_name or "").strip().rstrip(":").lower()
    if not env:
        raise ValueError("env_name is required")
    return f"{env}:cta_rules"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync cta rules to Redis ({env}:cta_rules)")
    p.add_argument("--env-name", help="环境目录名（例如 binance-cta-rx01），缺省取 CWD basename")
    p.add_argument("--file", help="cta 信号配置 JSON 文件（单对象，兼容规则数组）")
    p.add_argument("--clear", action="store_true", help="写入空数组 []，清空信号配置")
    p.add_argument("--dry-run", action="store_true", help="只校验，不写入 Redis")
    args = p.parse_args()
    if args.clear and args.file:
        p.error("--clear 与 --file 互斥")
    if not args.clear and not args.file:
        p.error("需要 --file <rules.json> 或 --clear")
    return args


def _fail(rule_id: str, msg: str, errors: List[str]) -> None:
    errors.append(f"rule '{rule_id}': {msg}")


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v))


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def validate_rule(raw: Any, index: int, errors: List[str]) -> Optional[Dict[str, Any]]:
    if not isinstance(raw, dict):
        errors.append(f"rule[{index}]: must be an object")
        return None
    unknown = sorted(set(raw.keys()) - ALLOWED_FIELDS)
    rule_id = str(raw.get("rule_id", f"#{index}"))
    if unknown:
        _fail(rule_id, f"unknown fields rejected by loader (deny_unknown_fields): {', '.join(unknown)}", errors)

    rid = str(raw.get("rule_id") or "").strip()
    if rid and (len(rid) > RULE_ID_MAX_LEN or not RULE_ID_RE.match(rid)):
        _fail(rid, f"rule_id invalid: [a-zA-Z0-9_-]{{1,{RULE_ID_MAX_LEN}}}", errors)
    service = str(raw.get("model_service") or "").strip()
    if not service:
        _fail(rid, "model_service must be non-empty", errors)

    trade_sides = raw.get("trade_sides")
    if trade_sides is None:
        trade_sides = "both"
    normalized_trade_sides = str(trade_sides).strip().lower()
    if normalized_trade_sides not in TRADE_SIDES:
        _fail(rid, f"trade_sides must be long, short, or both, got '{trade_sides}'", errors)

    nq_enabled = raw.get("nq_change_enabled", True)
    if not isinstance(nq_enabled, bool):
        _fail(rid, f"nq_change_enabled must be bool, got {nq_enabled}", errors)

    cooldown = raw.get("cooldown_seconds", 0)
    if not _is_int(cooldown) or cooldown < 0:
        _fail(rid, f"cooldown_seconds must be non-negative, got {cooldown}", errors)

    app = str(raw.get("application", "each_bar")).strip().lower()
    if app not in {"each_bar", "on_change"}:
        _fail(rid, f"application must be each_bar or on_change, got '{app}'", errors)

    order_notional = raw.get("order_notional_usdt", 100.0)
    if not _is_num(order_notional) or float(order_notional) <= 0.0:
        _fail(rid, f"order_notional_usdt must be positive finite, got {order_notional}", errors)

    offsets = raw.get("open_offsets", [0.0, 0.0001, 0.0003, 0.0005])
    if not isinstance(offsets, list) or not 1 <= len(offsets) <= MAX_OPEN_LEVELS:
        _fail(rid, f"open_offsets len must be in [1, {MAX_OPEN_LEVELS}]", errors)
        offsets = []
    for off in offsets:
        if not _is_num(off) or not 0.0 <= float(off) <= MAX_OPEN_OFFSET:
            _fail(rid, f"open_offsets item must be finite in [0, {MAX_OPEN_OFFSET}], got {off}", errors)

    ttl = raw.get("open_ttl_seconds", 120)
    if not _is_int(ttl) or ttl <= 0:
        _fail(rid, f"open_ttl_seconds must be positive int, got {ttl}", errors)

    for field, default, lower, upper in (
        ("factor_exit_quantile_long", 0.3, 0.0, 0.9),
        ("factor_exit_quantile_short", 0.7, 0.1, 1.0),
    ):
        value = raw.get(field, default)
        valid = _is_num(value) and lower <= float(value) <= upper
        if field.endswith("long") and valid:
            valid = float(value) < upper
        if field.endswith("short") and valid:
            valid = float(value) > lower
        if not valid:
            _fail(rid, f"{field} must be within V007 exit band, got {value}", errors)

    trailing_enabled = raw.get("trailing_stop_enabled", True)
    if not isinstance(trailing_enabled, bool):
        _fail(rid, f"trailing_stop_enabled must be bool, got {trailing_enabled}", errors)
    for name in ("trailing_stop_trigger_step", "trailing_stop_move_step"):
        v = raw.get(name, 0.01 if name == "trailing_stop_trigger_step" else 0.005)
        if trailing_enabled is True and (not _is_num(v) or float(v) <= 0.0):
            _fail(rid, f"{name} must be positive finite when trailing_stop_enabled, got {v}", errors)
    trigger = raw.get("trailing_stop_trigger_step", 0.01)
    move = raw.get("trailing_stop_move_step", 0.005)
    if (
        trailing_enabled is True
        and _is_num(trigger)
        and _is_num(move)
        and float(move) >= float(trigger)
    ):
        _fail(
            rid,
            f"trailing_stop_move_step({move}) must be < trailing_stop_trigger_step({trigger})",
            errors,
        )


    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        _fail(rid, f"enabled must be bool, got {enabled}", errors)

    return raw


def validate_rules(raw: Any) -> List[str]:
    """校验 {env}:cta_rules 内容：单对象（当前格式）或规则数组（兼容）。"""
    errors: List[str] = []
    if isinstance(raw, dict):
        raw = [raw]
    if not isinstance(raw, list):
        return ["cta rules JSON must be an object or an array of rule objects"]
    if len(raw) > 1:
        errors.append(
            f"one CTA environment may contain at most one independent rule, got {len(raw)}"
        )
    seen = set()
    for index, item in enumerate(raw):
        validate_rule(item, index, errors)
        rid = str(item.get("rule_id") or "").strip() if isinstance(item, dict) else ""
        if rid:
            if rid in seen:
                errors.append(f"rule_id '{rid}' duplicated")
            seen.add(rid)
    return errors


def _coerce_bool(raw: Any) -> Optional[bool]:
    v = str(raw).strip().lower()
    if v in _BOOL_TRUE:
        return True
    if v in _BOOL_FALSE:
        return False
    return None


def parse_offsets_text(raw: Any) -> Optional[List[float]]:
    """open_offsets 接受 JSON 数组或逗号/空白分隔列表，返回 float 列表。"""
    if isinstance(raw, list):
        try:
            return [float(v) for v in raw]
        except (TypeError, ValueError):
            return None
    text = str(raw).strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return [float(v) for v in parsed]
    except (TypeError, ValueError, json.JSONDecodeError):
        pass
    try:
        return [float(p) for p in re.split(r"[\s,]+", text) if p]
    except ValueError:
        return None


def parse_signal_config(values: Any) -> Tuple[Optional[Dict[str, Any]], List[str]]:
    """把 config server 表单的字符串 values 转成 typed 信号配置对象
    （写入 {env}:cta_rules 的内容）。返回 (config, errors)。"""
    errors: List[str] = []
    if not isinstance(values, dict):
        return None, ["signal config must be an object of field values"]
    unknown = sorted(set(values) - set(SIGNAL_FIELD_TYPES) - {"rule_id"})
    if unknown:
        errors.append(
            "unknown signal fields: " + ", ".join(unknown) + "（执行参数走 strategy params）"
        )
    config: Dict[str, Any] = {}
    for name, kind in SIGNAL_FIELD_TYPES.items():
        if name not in values or values[name] is None:
            continue
        raw = values[name]
        if kind == "str":
            config[name] = str(raw).strip()
        elif kind == "bool":
            parsed = _coerce_bool(raw)
            if parsed is None:
                errors.append(f"{name} must be bool, got '{raw}'")
            else:
                config[name] = parsed
        elif kind == "int":
            try:
                config[name] = int(str(raw).strip())
            except (TypeError, ValueError):
                errors.append(f"{name} must be int, got '{raw}'")
        elif kind == "float":
            try:
                parsed = float(str(raw).strip())
            except (TypeError, ValueError):
                errors.append(f"{name} must be a number, got '{raw}'")
                continue
            if not math.isfinite(parsed):
                errors.append(f"{name} must be finite, got '{raw}'")
            else:
                config[name] = parsed
    rid = str(values.get("rule_id") or "").strip()
    if rid:
        if len(rid) > RULE_ID_MAX_LEN or not RULE_ID_RE.match(rid):
            errors.append(f"rule_id invalid: [a-zA-Z0-9_-]{{1,{RULE_ID_MAX_LEN}}}")
        else:
            config["rule_id"] = rid
    if errors:
        return None, errors
    # 复用全量校验（缺省执行字段走 serde 同默认值，不会误报）。
    errors = validate_rules(config)
    if errors:
        return None, errors
    return config, []


def parse_exec_params(values: Any) -> Tuple[Dict[str, str], List[str]]:
    """校验+规范化 strategy hash 里的 cta 执行/网格字段。

    返回 (normalized, errors)：normalized 只包含提交过的字段（String 值，
    open_offsets 统一为紧凑 JSON 数组字符串）；交叉校验按"提交值缺省走
    EXEC_FIELD_DEFAULTS"的有效值做（与 Rust CtaExecOverrides 兜底一致）。
    """
    errors: List[str] = []
    norm: Dict[str, str] = {}
    if not isinstance(values, dict):
        return {}, ["exec params must be an object"]
    unknown = sorted(set(values) - set(EXEC_FIELD_TYPES))
    if unknown:
        errors.append("unknown exec fields: " + ", ".join(unknown))
    typed: Dict[str, Any] = {}
    for name, kind in EXEC_FIELD_TYPES.items():
        if name not in values or values[name] is None:
            continue
        raw = values[name]
        if kind == "offsets":
            parsed = parse_offsets_text(raw)
            if parsed is None:
                errors.append(f"{name} must be a JSON array or CSV of numbers, got '{raw}'")
            else:
                typed[name] = parsed
        elif kind == "bool":
            parsed = _coerce_bool(raw)
            if parsed is None:
                errors.append(f"{name} must be bool, got '{raw}'")
            else:
                typed[name] = parsed
        elif kind == "int":
            try:
                typed[name] = int(str(raw).strip())
            except (TypeError, ValueError):
                errors.append(f"{name} must be int, got '{raw}'")
        else:
            try:
                parsed = float(str(raw).strip())
            except (TypeError, ValueError):
                errors.append(f"{name} must be a number, got '{raw}'")
                continue
            if not math.isfinite(parsed):
                errors.append(f"{name} must be finite, got '{raw}'")
            else:
                typed[name] = parsed

    # 与 Rust CtaRule::validate 对齐的交叉/边界校验（有效值 = 提交值或默认）。
    eff = {**EXEC_FIELD_DEFAULTS, **typed}
    if eff["order_notional_usdt"] <= 0:
        errors.append(f"order_notional_usdt must be positive, got {eff['order_notional_usdt']}")
    offsets = eff["open_offsets"]
    if not 1 <= len(offsets) <= MAX_OPEN_LEVELS:
        errors.append(f"open_offsets len must be in [1, {MAX_OPEN_LEVELS}], got {len(offsets)}")
    for off in offsets:
        if not 0.0 <= off <= MAX_OPEN_OFFSET:
            errors.append(f"open_offsets item must be in [0, {MAX_OPEN_OFFSET}], got {off}")
            break
    if eff["open_ttl_seconds"] <= 0:
        errors.append(f"open_ttl_seconds must be positive, got {eff['open_ttl_seconds']}")
    if not 0 <= eff["factor_exit_quantile_long"] < 0.9:
        errors.append("factor_exit_quantile_long must be in [0,0.9)")
    if not 0.1 < eff["factor_exit_quantile_short"] <= 1:
        errors.append("factor_exit_quantile_short must be in (0.1,1]")
    if eff["trailing_stop_enabled"]:
        for name in ("trailing_stop_trigger_step", "trailing_stop_move_step"):
            if eff[name] <= 0:
                errors.append(f"{name} must be positive when trailing_stop_enabled, got {eff[name]}")
        if eff["trailing_stop_move_step"] >= eff["trailing_stop_trigger_step"]:
            errors.append(
                "trailing_stop_move_step must be < trailing_stop_trigger_step, got "
                f"move={eff['trailing_stop_move_step']} trigger={eff['trailing_stop_trigger_step']}"
            )

    if not errors:
        for name, value in typed.items():
            if name == "open_offsets":
                norm[name] = json.dumps(value, separators=(",", ":"))
            elif isinstance(value, bool):
                norm[name] = "true" if value else "false"
            else:
                norm[name] = str(value)
    return norm, errors


def resolve_env_name(args: argparse.Namespace) -> str:
    env_name = (args.env_name or Path.cwd().name).strip().lower()
    if not infer_cta_env_from_name(env_name):
        print(
            f"⚠️  env-name '{env_name}' 不匹配 <exchange>-cta-<tag>，仍按该名写 key",
            file=sys.stderr,
        )
    return env_name


def main() -> int:
    args = parse_args()
    env_name = resolve_env_name(args)
    key = cta_rules_key(env_name)

    if args.clear:
        payload = "[]"
        rules: List[Any] = []
    else:
        path = Path(args.file)
        if not path.is_file():
            print(f"❌ rules 文件不存在: {path}", file=sys.stderr)
            return 2
        try:
            rules = json.loads(path.read_text())
        except Exception as exc:
            print(f"❌ rules 文件不是合法 JSON: {path}: {exc}", file=sys.stderr)
            return 2
        payload = json.dumps(rules, ensure_ascii=False, separators=(",", ":"))

    errors = validate_rules(rules)
    if errors:
        print(f"❌ cta rules 校验失败（{len(errors)} 个问题）:", file=sys.stderr)
        for err in errors:
            print(f"   - {err}", file=sys.stderr)
        return 2

    items = rules if isinstance(rules, list) else [rules]
    print(f"✅ 校验通过: {len(items)} 条配置 -> {key}")
    if args.dry_run:
        print("📄 dry-run，未写入 Redis")
        return 0

    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    rds.set(key, payload)
    print(f"✅ 已写入 '{key}'（{len(payload)} 字节，{len(items)} 条配置）")
    for item in items:
        rid = item.get("rule_id", "default") if isinstance(item, dict) else "?"
        svc = item.get("model_service", "?") if isinstance(item, dict) else "?"
        en = item.get("enabled", True) if isinstance(item, dict) else True
        print(f"   - {rid}: service={svc} enabled={en}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
