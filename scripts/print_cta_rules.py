#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
打印 Redis 中的 cta 信号配置（{env}:cta_rules，单 JSON 对象；兼容旧数组格式）。

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
    p = argparse.ArgumentParser(description="Print cta signal config from Redis ({env}:cta_rules)")
    p.add_argument("--env-name", help="环境目录名（例如 binance-cta-rx01），缺省取 CWD basename")
    p.add_argument("--open-venue", help="开仓 venue，缺省按 <exchange>-margin 推断")
    p.add_argument("--hedge-venue", help="对冲 venue，缺省按 <exchange>-futures 推断")
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
        f"nq={'on' if rule.get('nq_change_enabled', True) else 'off'} "
        f"app={_fmt(rule.get('application'), 'each_bar')} "
        f"cooldown={_fmt(rule.get('cooldown_seconds'), 0)}s"
    )
    print(
        "    open grid:                "
        f"offsets={_fmt(rule.get('open_offsets'), '[0.0, 0.0001, 0.0003, 0.0005]')} "
        f"notional={_fmt(rule.get('order_notional_usdt'), 100.0)}u "
        f"ttl={_fmt(rule.get('open_ttl_seconds'), 120)}s "
        f"max_pos={_fmt(rule.get('max_position_notional_usdt'), 10000.0)}u"
    )
    tp = rule.get("take_profit", 0.005)
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
        doc = json.loads(text)
    except Exception as exc:
        print(f"❌ '{key}' 不是合法 JSON: {exc}")
        print(f"   原始值: {text[:500]}")
        return 1
    if isinstance(doc, dict):
        rules = [doc]
    elif isinstance(doc, list):
        rules = doc
    else:
        print(f"❌ '{key}' 不是 JSON 对象/数组: {text[:200]}")
        return 1

    print(f"\n📊 cta 信号配置: {len(rules)} 条")
    print("=" * 80)
    for index, rule in enumerate(rules):
        if isinstance(rule, dict):
            print_rule(index, rule)
        else:
            print(f"\n⚠️ [{index}] 非对象条目: {rule!r}")

    # 执行/网格参数在 cta_strategy_params hash（加载时覆盖到规则上）。
    exchange = env_name.split("-")[0] if "-" in env_name else ""
    open_venue = (args.open_venue or f"{exchange}-margin").strip()
    hedge_venue = (args.hedge_venue or f"{exchange}-futures").strip()
    strat_key = f"{env_name}:cta_strategy_params:{open_venue}:{hedge_venue}"
    exec_fields = (
        "order_notional_usdt", "open_offsets", "open_ttl_seconds",
        "max_position_notional_usdt", "take_profit", "reward_risk_ratio",
        "trailing_stop_enabled", "trailing_stop_trigger_step",
        "trailing_stop_move_step", "max_holding_seconds",
    )
    try:
        strat = rds.hgetall(strat_key) or {}
    except Exception:
        strat = {}
    decoded = {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in strat.items()
    }
    exec_values = {k: decoded[k] for k in exec_fields if k in decoded}
    print("\n🛠  执行/网格参数 (hash: {})".format(strat_key))
    if exec_values:
        for k in exec_fields:
            if k in exec_values:
                print(f"    {k:32} = {exec_values[k]}")
    else:
        print("    （未配置 — 加载时走 serde 默认值）")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
