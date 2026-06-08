#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
同步 intra per-symbol taker decision model 覆盖到 Redis 并打印。

写入 Redis String(JSON):
  <env_name>:<open_venue>:<hedge_venue>:taker_decsion_model_overrides

JSON 格式:
  {
    "BTCUSDT": {
      "keep_long_percentile": 80,
      "keep_short_percentile": 20,
      "open_cancel_long_percentile": 80,
      "open_cancel_short_percentile": 20
    },
    "ETH-USDT": {
      "keep_long": 85,
      "keep_short": 15,
      "open_cancel_long": 82,
      "open_cancel_short": 18
    }
  }

说明:
  - 收到 model value msg 的 symbol 会使用 pre_trade lazy taker decision model。
  - 该 JSON 只覆盖对应 symbol 的阈值；未配置 symbol 使用 strategy params 全局默认值。
  - rolling 由 model_pub 维护，本脚本不再接受 rolling_n / rolling_window / window。
  - 未收到 model msg 的 symbol 继续走正常 MT。
  - 每个字段可省略；Rust 侧会使用 strategy params 里的全局默认值。
  - symbol 自动规范化为 alphanumeric uppercase。
  - keep 阈值必须满足 short <= long。
  - open cancel 阈值必须满足 short <= long，其中 long 表示“小于该阈值撤多单”，short 表示“大于该阈值撤空单”。

示例:
  cd ~/bybit-intra-arb01
  python intra_scripts/sync_intra_taker_decision_model.py --json '{"BTCUSDT":{"keep_long_percentile":80,"keep_short_percentile":20,"open_cancel_long_percentile":80,"open_cancel_short_percentile":20}}'
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, Optional

SUPPORTED_EXCHANGES = ["binance", "okex", "bybit", "bitget", "gate"]


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def normalize_exchange(ex: str) -> str:
    ex = (ex or "").strip().lower()
    if ex == "okx":
        ex = "okex"
    return ex


def infer_exchange_from_name(name: str) -> Optional[str]:
    n = (name or "").strip().lower()
    m = re.match(r"^([a-z0-9]+)[-_]intra([_-].*)?$", n)
    if not m:
        return None
    ex = normalize_exchange(m.group(1))
    if ex not in SUPPORTED_EXCHANGES:
        return None
    return ex


def infer_exchange_from_cwd() -> Optional[str]:
    return infer_exchange_from_name(Path.cwd().name)


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def normalize_symbol(raw: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]", "", (raw or "").strip()).upper()
    if not text:
        raise ValueError(f"invalid symbol: {raw!r}")
    return text


def coerce_percentile(raw_value: Any, field_name: str, symbol: str) -> float:
    try:
        value = float(raw_value)
    except Exception as exc:
        raise ValueError(f"{field_name} must be a percentile for {symbol}: {raw_value}") from exc
    if not (math.isfinite(value) and 0.0 <= value <= 100.0):
        raise ValueError(f"{field_name} must be in [0,100] for {symbol}: {raw_value}")
    return value


def normalize_mapping(raw_json: str) -> Dict[str, Dict[str, Any]]:
    try:
        payload = json.loads(raw_json)
    except Exception as exc:
        raise ValueError(f"invalid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("JSON must be an object: {symbol: config}")

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
    disallowed_rolling_fields = {"rolling_n", "rolling_window", "window"}
    normalized: Dict[str, Dict[str, Any]] = {}
    for raw_symbol, raw_cfg in payload.items():
        symbol = normalize_symbol(str(raw_symbol))
        if raw_cfg is None:
            raw_cfg = {}
        if not isinstance(raw_cfg, dict):
            raise ValueError(f"config for {symbol} must be an object")
        rolling_fields = sorted(str(field) for field in raw_cfg.keys() if str(field) in disallowed_rolling_fields)
        if rolling_fields:
            raise ValueError(
                f"config for {symbol} no longer accepts rolling fields: "
                f"{', '.join(rolling_fields)}; configure model_pub score rolling instead"
            )
        unknown = sorted(str(field) for field in raw_cfg.keys() if str(field) not in allowed_fields)
        if unknown:
            raise ValueError(f"config for {symbol} has unknown fields: {', '.join(unknown)}")
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
            cfg["keep_long_percentile"] = coerce_percentile(
                keep_long_raw, "keep_long_percentile", symbol
            )
        if keep_short_raw is not None:
            cfg["keep_short_percentile"] = coerce_percentile(
                keep_short_raw, "keep_short_percentile", symbol
            )
        if open_cancel_long_raw is not None:
            cfg["open_cancel_long_percentile"] = coerce_percentile(
                open_cancel_long_raw, "open_cancel_long_percentile", symbol
            )
        if open_cancel_short_raw is not None:
            cfg["open_cancel_short_percentile"] = coerce_percentile(
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
    return normalized


def dumps_mapping(mapping: Dict[str, Dict[str, Any]]) -> str:
    ordered: Dict[str, Dict[str, Any]] = {}
    for symbol in sorted(mapping.keys()):
        cfg = mapping[symbol]
        out: Dict[str, Any] = {}
        if "keep_long_percentile" in cfg:
            out["keep_long_percentile"] = float(f"{float(cfg['keep_long_percentile']):.12g}")
        if "keep_short_percentile" in cfg:
            out["keep_short_percentile"] = float(f"{float(cfg['keep_short_percentile']):.12g}")
        if "open_cancel_long_percentile" in cfg:
            out["open_cancel_long_percentile"] = float(
                f"{float(cfg['open_cancel_long_percentile']):.12g}"
            )
        if "open_cancel_short_percentile" in cfg:
            out["open_cancel_short_percentile"] = float(
                f"{float(cfg['open_cancel_short_percentile']):.12g}"
            )
        ordered[symbol] = out
    return json.dumps(ordered, ensure_ascii=False, separators=(",", ":"))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync intra taker decision model per-symbol overrides to Redis"
    )
    p.add_argument("--exchange", default=os.environ.get("EXCHANGE"))
    p.add_argument("--open-venue", default=os.environ.get("OPEN_VENUE"))
    p.add_argument("--hedge-venue", default=os.environ.get("HEDGE_VENUE"))
    p.add_argument("--env-name", help="环境目录名（默认取 CWD basename）")
    p.add_argument("--json", default="{}", help='JSON mapping, e.g. \'{"BTCUSDT":{"keep_long_percentile":80,"keep_short_percentile":20,"open_cancel_long_percentile":80,"open_cancel_short_percentile":20}}\'')
    args = p.parse_args()

    exchange = args.exchange
    if not exchange:
        exchange = infer_exchange_from_name(args.env_name) if args.env_name else infer_exchange_from_cwd()
        if exchange:
            print(f"[INFO] 未提供 --exchange，基于目录推断: exchange={exchange}", file=sys.stderr)

    if exchange:
        exchange = normalize_exchange(exchange)
        if exchange not in SUPPORTED_EXCHANGES:
            p.error(f"不支持的 exchange: {exchange}")
        if not args.open_venue:
            args.open_venue = f"{exchange}-margin"
        if not args.hedge_venue:
            args.hedge_venue = f"{exchange}-futures"

    if not args.open_venue or not args.hedge_venue:
        p.error("需要 --exchange 或同时提供 --open-venue/--hedge-venue")

    args.open_venue = args.open_venue.lower()
    args.hedge_venue = args.hedge_venue.lower()

    if not args.env_name:
        args.env_name = infer_env_name_from_cwd()
    if not args.env_name:
        p.error("无法推断 env_name，请通过 --env-name 显式提供或在合适的目录运行")
    args.env_name = args.env_name.strip().lower()
    return args


def make_key(env_name: str, open_venue: str, hedge_venue: str) -> str:
    return f"{env_name}:{open_venue}:{hedge_venue}:taker_decsion_model_overrides"


def print_value(rds, key: str) -> None:
    raw = rds.get(key)
    print("\n📊 intra taker decision model per-symbol 覆盖配置:")
    print("=" * 96)
    if raw is None:
        print(f"⚠️  STRING '{key}' 为空或不存在")
        return
    text = raw.decode("utf-8", "ignore") if isinstance(raw, bytes) else str(raw)
    try:
        mapping = normalize_mapping(text)
    except ValueError:
        print(text)
        return
    if not mapping:
        print("{}")
        return
    for symbol in sorted(mapping.keys()):
        cfg = mapping[symbol]
        print(
            f"  {symbol:24} "
            f"keep_long={cfg.get('keep_long_percentile', '-'):>6} "
            f"keep_short={cfg.get('keep_short_percentile', '-'):>6} "
            f"open_cancel_long={cfg.get('open_cancel_long_percentile', '-'):>6} "
            f"open_cancel_short={cfg.get('open_cancel_short_percentile', '-'):>6}"
        )


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    try:
        mapping = normalize_mapping(args.json)
    except ValueError as exc:
        print(f"❌ {exc}", file=sys.stderr)
        return 1

    key = make_key(args.env_name, args.open_venue, args.hedge_venue)
    payload = dumps_mapping(mapping)
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    rds.set(key, payload)

    print(f"🔄 同步 intra taker decision model per-symbol 覆盖配置: {key}")
    print(f"📁 env_name: {args.env_name}")
    print(f"🏷️ open: {args.open_venue}  hedge: {args.hedge_venue}")
    print(f"🧩 symbols: {len(mapping)}")
    print("📍 Redis: 127.0.0.1:6379/0")

    print_value(rds, key)
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
