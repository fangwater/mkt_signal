#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
将 cta 规则集同步到 Redis（env 作用域 STRING key，JSON 数组）。

写入的 Redis key：
  - {env}:cta_rules    - 例如 binance-cta-rx01:cta_rules

校验规则与 trade_signal `cta_config.rs` 的 `CtaRule::validate` 保持一致
（在写入前本地全量校验，避免把 loader 会拒绝的配置写进 Redis）。

env-name 推断：--env-name，或 CWD 目录名 <exchange>-cta-<tag>。

用法：
  scripts/sync_cta_rules.py --env-name binance-cta-rx01 --file rules.json
  scripts/sync_cta_rules.py --file rules.json --dry-run     # 只校验不写
  scripts/sync_cta_rules.py --clear                          # 写入 [] 清空规则
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

RULE_ID_MAX_LEN = 32
MAX_OPEN_LEVELS = 8
MAX_OPEN_OFFSET = 0.01
RULE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

TRADE_SIDES = {"long", "buy", "short", "sell", "both", "long_short", "long,short", "short,long"}
APPLICATIONS = {"each_bar", "on_change"}

# 与 RawCtaRule 的 serde 字段一一对应（deny_unknown_fields）。
ALLOWED_FIELDS = {
    "rule_id",
    "model_service",
    "trade_sides",
    "long_quantile",
    "short_quantile",
    "spread_long_quantile",
    "spread_short_quantile",
    "spread_cancel_quantile",
    "rolling_window",
    "rolling_min_periods",
    "frequency_seconds",
    "cooldown_seconds",
    "signal_delay_seconds",
    "application",
    "order_notional_usdt",
    "open_offsets",
    "open_ttl_seconds",
    "max_position_notional_usdt",
    "take_profit",
    "reward_risk_ratio",
    "trailing_stop_enabled",
    "trailing_stop_trigger_step",
    "trailing_stop_move_step",
    "max_holding_seconds",
    "enabled",
}

QUANTILE_FIELDS = (
    "long_quantile",
    "short_quantile",
    "spread_long_quantile",
    "spread_short_quantile",
    "spread_cancel_quantile",
)

# serde 默认值，与 crates/trade_signal/src/cta_config.rs RawCtaRule 一一对应。
# config server 的 cta rules 面板用它做表单预填；validate_rule 里的内联默认值必须保持一致。
RULE_DEFAULTS: Dict[str, Any] = {
    "rule_id": "",
    "model_service": "",
    "trade_sides": "both",
    "long_quantile": 0.9,
    "short_quantile": 0.1,
    "spread_long_quantile": 0.7,
    "spread_short_quantile": 0.3,
    "spread_cancel_quantile": 0.5,
    "rolling_window": 2880,
    "rolling_min_periods": 1440,
    "frequency_seconds": 60,
    "cooldown_seconds": 0,
    "signal_delay_seconds": 1,
    "application": "each_bar",
    "order_notional_usdt": 100.0,
    "open_offsets": [0.0, 0.0001, 0.0003, 0.0005],
    "open_ttl_seconds": 120,
    "max_position_notional_usdt": 10000.0,
    "take_profit": 0.0,
    "reward_risk_ratio": 1.0,
    "trailing_stop_enabled": True,
    "trailing_stop_trigger_step": 0.001,
    "trailing_stop_move_step": 0.0005,
    "max_holding_seconds": 14400,
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
    p.add_argument("--file", help="cta rules JSON 文件（数组）")
    p.add_argument("--clear", action="store_true", help="写入空数组 []，清空全部规则")
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
    if not rid or len(rid) > RULE_ID_MAX_LEN or not RULE_ID_RE.match(rid):
        _fail(rid or f"#{index}", f"rule_id invalid: [a-zA-Z0-9_-]{{1,{RULE_ID_MAX_LEN}}}", errors)
    service = str(raw.get("model_service") or "").strip()
    if not service:
        _fail(rid, "model_service must be non-empty", errors)

    trade_sides = raw.get("trade_sides")
    if trade_sides is not None and str(trade_sides).strip().lower() not in TRADE_SIDES:
        _fail(rid, f"trade_sides must be long, short, or both, got '{trade_sides}'", errors)

    long_q = float(raw.get("long_quantile", 0.9))
    short_q = float(raw.get("short_quantile", 0.1))
    for name in QUANTILE_FIELDS:
        v = raw.get(name)
        if v is None:
            continue
        if not _is_num(v) or not 0.0 <= float(v) <= 1.0:
            _fail(rid, f"{name} must be finite in [0,1], got {v}", errors)
    if _is_num(raw.get("long_quantile", long_q)) and _is_num(raw.get("short_quantile", short_q)):
        if not short_q < long_q:
            _fail(rid, f"short_quantile({short_q}) must be < long_quantile({long_q})", errors)

    window = raw.get("rolling_window", 2880)
    min_periods = raw.get("rolling_min_periods", 1440)
    if not _is_int(window) or window <= 0:
        _fail(rid, f"rolling_window must be positive int, got {window}", errors)
    if not _is_int(min_periods) or not 1 <= min_periods <= (window if _is_int(window) and window > 0 else 0):
        _fail(rid, "rolling_min_periods must be in [1, rolling_window]", errors)

    freq = raw.get("frequency_seconds", 60)
    if not _is_int(freq) or freq <= 0:
        _fail(rid, f"frequency_seconds must be positive int, got {freq}", errors)
    for name in ("cooldown_seconds", "signal_delay_seconds"):
        v = raw.get(name, 0 if name == "cooldown_seconds" else 1)
        if not _is_int(v) or v < 0:
            _fail(rid, f"{name} must be a non-negative int, got {v}", errors)

    app = str(raw.get("application", "each_bar")).strip().lower()
    if app not in APPLICATIONS:
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

    max_pos = raw.get("max_position_notional_usdt", 10_000.0)
    if not _is_num(max_pos) or float(max_pos) <= 0.0:
        _fail(rid, f"max_position_notional_usdt must be positive finite, got {max_pos}", errors)
    grid_notional = float(order_notional) * len(offsets) if _is_num(order_notional) else 0.0
    if _is_num(max_pos) and offsets and float(max_pos) < grid_notional:
        _fail(
            rid,
            f"max_position_notional_usdt({max_pos}) must cover one complete grid ({grid_notional})",
            errors,
        )

    tp = raw.get("take_profit", 0.0)
    if not _is_num(tp) or float(tp) < 0.0:
        _fail(rid, f"take_profit must be finite and >= 0 (0 disables the maker tp hedge), got {tp}", errors)
    rr = raw.get("reward_risk_ratio", 1.0)
    if not _is_num(rr) or float(rr) <= 0.0:
        _fail(rid, f"reward_risk_ratio must be positive finite, got {rr}", errors)

    trailing_enabled = raw.get("trailing_stop_enabled", True)
    if not isinstance(trailing_enabled, bool):
        _fail(rid, f"trailing_stop_enabled must be bool, got {trailing_enabled}", errors)
    for name in ("trailing_stop_trigger_step", "trailing_stop_move_step"):
        v = raw.get(name, 0.001 if name == "trailing_stop_trigger_step" else 0.0005)
        if trailing_enabled is True and (not _is_num(v) or float(v) <= 0.0):
            _fail(rid, f"{name} must be positive finite when trailing_stop_enabled, got {v}", errors)

    max_hold = raw.get("max_holding_seconds", 14_400)
    if not _is_int(max_hold) or max_hold < 0:
        _fail(rid, f"max_holding_seconds must be a non-negative int (0 disables), got {max_hold}", errors)

    enabled = raw.get("enabled", True)
    if not isinstance(enabled, bool):
        _fail(rid, f"enabled must be bool, got {enabled}", errors)

    return raw


def validate_rules(raw: Any) -> List[str]:
    errors: List[str] = []
    if not isinstance(raw, list):
        return ["cta rules JSON must be an array of rule objects"]
    seen = set()
    for index, item in enumerate(raw):
        validate_rule(item, index, errors)
        rid = str(item.get("rule_id") or "").strip() if isinstance(item, dict) else ""
        if rid:
            if rid in seen:
                errors.append(f"rule_id '{rid}' duplicated")
            seen.add(rid)
    return errors


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

    print(f"✅ 校验通过: {len(rules)} 条规则 -> {key}")
    if args.dry_run:
        print("📄 dry-run，未写入 Redis")
        return 0

    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    rds.set(key, payload)
    print(f"✅ 已写入 '{key}'（{len(payload)} 字节，{len(rules)} 条规则）")
    for item in rules:
        rid = item.get("rule_id", "?") if isinstance(item, dict) else "?"
        svc = item.get("model_service", "?") if isinstance(item, dict) else "?"
        en = item.get("enabled", True) if isinstance(item, dict) else True
        print(f"   - {rid}: service={svc} enabled={en}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
