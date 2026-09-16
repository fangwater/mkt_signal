#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Redis 中的 cta 规则集（{env}:cta_rules，JSON 数组）。

env-name 推断：--env-name，或 CWD 目录名 <exchange>-cta-<tag>。

用法：
  scripts/print_cta_rules.py --env-name binance-cta-rx01
  scripts/print_cta_rules.py --json          # 原样输出 JSON
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def cta_rules_key(env_name: str) -> str:
    env = (env_name or "").strip().rstrip(":").lower()
    if not env:
        raise ValueError("env_name is required")
    return f"{env}:cta_rules"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Print cta rules from Redis ({env}:cta_rules)")
    p.add_argument("--env-name", help="环境目录名（例如 binance-cta-rx01），缺省取 CWD basename")
    p.add_argument("--json", action="store_true", help="原样输出 JSON")
    return p.parse_args()


def _fmt(v, default="-"):
    return default if v is None else v


def print_rule(index: int, rule: dict) -> None:
    rid = rule.get("rule_id", f"#{index}")
    enabled = rule.get("enabled", True)
    flag = "🟢" if enabled else "⚪"
    print(f"\n{flag} [{index}] rule_id={rid} enabled={enabled}")
    print(f"    model_service:            {_fmt(rule.get('model_service'))}")
    print(
        "    signal:                   "
        f"trade_sides={_fmt(rule.get('trade_sides'), 'both')} "
        f"long_q={_fmt(rule.get('long_quantile'), 0.9)} "
        f"short_q={_fmt(rule.get('short_quantile'), 0.1)} "
        f"app={_fmt(rule.get('application'), 'each_bar')} "
        f"cooldown={_fmt(rule.get('cooldown_seconds'), 0)}s "
        f"delay={_fmt(rule.get('signal_delay_seconds'), 1)}s"
    )
    print(
        "    spread overlay:           "
        f"long_q={_fmt(rule.get('spread_long_quantile'), 0.7)} "
        f"short_q={_fmt(rule.get('spread_short_quantile'), 0.3)} "
        f"cancel_q={_fmt(rule.get('spread_cancel_quantile'), 0.5)} "
        f"window={_fmt(rule.get('rolling_window'), 2880)} "
        f"min_periods={_fmt(rule.get('rolling_min_periods'), 1440)}"
    )
    print(
        "    open grid:                "
        f"offsets={_fmt(rule.get('open_offsets'), '[0.0, 0.0001, 0.0003, 0.0005]')} "
        f"notional={_fmt(rule.get('order_notional_usdt'), 100.0)}u "
        f"ttl={_fmt(rule.get('open_ttl_seconds'), 120)}s "
        f"max_pos={_fmt(rule.get('max_position_notional_usdt'), 10000.0)}u"
    )
    tp = rule.get("take_profit", 0.0)
    rr = rule.get("reward_risk_ratio", 1.0)
    try:
        sl = float(tp) / float(rr) if float(rr) > 0 else float("nan")
    except (TypeError, ValueError):
        sl = float("nan")
    print(
        "    exits:                    "
        f"tp={tp} rr={rr} -> stop_loss={sl:.6g} "
        f"trailing={'on' if rule.get('trailing_stop_enabled', True) else 'off'}"
        f"(trig={_fmt(rule.get('trailing_stop_trigger_step'), 0.001)}"
        f"/move={_fmt(rule.get('trailing_stop_move_step'), 0.0005)}) "
        f"max_holding={_fmt(rule.get('max_holding_seconds'), 14400)}s"
    )


def main() -> int:
    args = parse_args()
    env_name = (args.env_name or Path.cwd().name).strip().lower()
    key = cta_rules_key(env_name)

    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)

    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📦 env_name: {env_name}")
    print(f"🔑 key: {key}")

    data = rds.get(key)
    if not data:
        print(f"⚠️  '{key}' 不存在或为空（loader 会按空规则集运行，不产生信号）")
        return 0
    text = data.decode("utf-8", "ignore") if isinstance(data, bytes) else str(data)
    if args.json:
        print(text)
        return 0
    try:
        rules = json.loads(text)
    except Exception as exc:
        print(f"❌ '{key}' 不是合法 JSON: {exc}")
        print(f"   原始值: {text[:500]}")
        return 1
    if not isinstance(rules, list):
        print(f"❌ '{key}' 不是 JSON 数组: {text[:200]}")
        return 1

    print(f"\n📊 cta 规则集: {len(rules)} 条")
    print("=" * 80)
    for index, rule in enumerate(rules):
        if isinstance(rule, dict):
            print_rule(index, rule)
        else:
            print(f"\n⚠️ [{index}] 非对象条目: {rule!r}")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
