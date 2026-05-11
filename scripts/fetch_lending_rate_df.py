#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 Redis 获取 FR symbol list，拉取每个 symbol 的借贷利率历史（1天），构造 DataFrame 并 stack 成一起。

参考 rate_fetcher.rs 的算法：
  - 从 Redis 读取 {env_name}:fr_fwd_trade_symbols:{key_suffix} 获取当前 symbol list
  - 对每个 symbol 提取 base asset（去掉 USDT 后缀）
  - 调用 Binance /sapi/v1/margin/interestRateHistory API 获取借贷利率历史
  - 构造 DataFrame，增加 symbol 列，然后 stack 成一起

示例：
  BINANCE_API_KEY=... BINANCE_API_SECRET=... python scripts/fetch_lending_rate_df.py
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional


def try_import_redis():
    try:
        import redis
        return redis
    except ImportError:
        return None


def try_import_pandas():
    try:
        import pandas as pd
        return pd
    except ImportError:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch lending rate history for FR symbols and build DataFrame",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    # Binance API 参数
    p.add_argument(
        "--base-url",
        default=os.environ.get("BINANCE_API_URL", "https://api.binance.com"),
        help="Binance API base URL",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=24,
        help="Number of records to fetch per asset (1 day = 24 hours)",
    )
    p.add_argument(
        "--output",
        "-o",
        help="Output CSV file path (optional)",
    )
    p.add_argument(
        "--redis-key",
        help="Redis key for symbol list；提供后会覆盖 env/key-suffix/list-name 自动拼 key",
    )
    p.add_argument(
        "--env-name",
        help="部署 env 名（例如 binance_fr_trade01）；未提供时使用当前目录名",
    )
    p.add_argument(
        "--key-suffix",
        default="binance-margin_binance-futures",
        help="FR symbol list key_suffix",
    )
    p.add_argument(
        "--list-name",
        default="fwd_trade_symbols",
        choices=["dump_symbols", "trade_symbols", "fwd_trade_symbols", "bwd_trade_symbols"],
        help="FR symbol list name",
    )
    return p.parse_args()


def infer_env_name_from_cwd() -> Optional[str]:
    name = Path.cwd().name.strip().lower()
    return name or None


def symbol_list_key(env_name: str, list_name: str, key_suffix: str) -> str:
    return f"{env_name}:fr_{list_name}:{key_suffix.strip().lower()}"


def warn_if_env_name_mismatched(env_name: str) -> None:
    if not re.match(r"^binance_fr_[a-z0-9][a-z0-9_-]*$", env_name):
        print(
            f"[WARN] env-name '{env_name}' 不符合 binance_fr_<suffix> 规范，仍然继续",
            file=sys.stderr,
        )


def now_ms() -> int:
    """Get current timestamp in milliseconds."""
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    """Generate HMAC SHA256 signature."""
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def request_binance_api(
    base_url: str,
    path: str,
    params: dict,
    api_key: str,
    api_secret: str,
    timeout: int = 10,
) -> tuple[int, str, dict]:
    """Make authenticated request to Binance API."""
    q = dict(params)
    q["timestamp"] = str(now_ms())

    items = sorted(q.items(), key=lambda kv: kv[0])
    query = urllib.parse.urlencode(items, safe="-_.~")
    signature = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={signature}"

    req = urllib.request.Request(
        url,
        method="GET",
        headers={"X-MBX-APIKEY": api_key}
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body, headers
    except Exception as exc:
        return 0, str(exc), {}


def get_symbols_from_redis(rds, key: str) -> list[str]:
    """从 Redis 读取 symbol list"""
    symbols_json = rds.get(key)
    if not symbols_json:
        return []

    symbols_str = symbols_json.decode('utf-8', 'ignore') if isinstance(symbols_json, bytes) else str(symbols_json)
    try:
        symbols = json.loads(symbols_str)
        if isinstance(symbols, list):
            return symbols
    except json.JSONDecodeError:
        pass
    return []


def fetch_lending_rate_history(
    base_url: str,
    asset: str,
    api_key: str,
    api_secret: str,
    limit: int = 24,
) -> Optional[list[dict]]:
    """获取单个 asset 的借贷利率历史"""
    params = {
        "asset": asset.upper(),
        "isIsolated": "FALSE",
        "size": str(min(limit, 100)),
    }

    status, body, _ = request_binance_api(
        base_url,
        "/sapi/v1/margin/interestRateHistory",
        params,
        api_key,
        api_secret,
    )

    if not (200 <= status < 300):
        return None

    try:
        data = json.loads(body)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        pass
    return None


def main() -> int:
    args = parse_args()

    # 检查依赖
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    pd = try_import_pandas()
    if pd is None:
        print("❌ pandas 包未安装，请使用 pip install pandas", file=sys.stderr)
        return 2

    # 检查 API 凭证
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()

    if not api_key or not api_secret:
        print("❌ 请设置环境变量 BINANCE_API_KEY 和 BINANCE_API_SECRET", file=sys.stderr)
        return 1

    # 连接 Redis
    rds = redis.Redis(host="127.0.0.1", port=6379, db=0, password=None)
    if args.redis_key:
        redis_key = args.redis_key
    else:
        env_name = (args.env_name or infer_env_name_from_cwd() or "").strip().lower()
        if not env_name:
            print(
                "❌ 需要 --env-name，或在 binance_fr_<suffix> 命名的目录下运行以自动推断",
                file=sys.stderr,
            )
            return 2
        if not args.env_name:
            print(f"[INFO] 未提供 env-name，基于目录推断: {env_name}", file=sys.stderr)
        warn_if_env_name_mismatched(env_name)
        redis_key = symbol_list_key(env_name, args.list_name, args.key_suffix)

    print("📍 Redis: 127.0.0.1:6379/0")
    print(f"📍 Redis Key: {redis_key}")

    # 获取 symbol list
    symbols = get_symbols_from_redis(rds, redis_key)
    if not symbols:
        print(f"⚠️  未找到 symbol list (key: {redis_key})")
        return 1

    print(f"📊 找到 {len(symbols)} 个 symbol")

    # 提取去重的 base assets
    base_assets = []
    symbol_to_asset = {}
    for s in symbols:
        if s.endswith("USDT"):
            asset = s[:-4]
            if asset not in base_assets:
                base_assets.append(asset)
            symbol_to_asset[s] = asset

    print(f"📊 去重后 {len(base_assets)} 个 base asset")
    print()

    # 拉取借贷利率历史
    all_records = []
    success_count = 0
    fail_count = 0

    for i, asset in enumerate(base_assets):
        print(f"  [{i+1}/{len(base_assets)}] 拉取 {asset}...", end=" ")

        records = fetch_lending_rate_history(
            args.base_url,
            asset,
            api_key,
            api_secret,
            limit=args.limit,
        )

        if records:
            for rec in records:
                rec["asset"] = asset
                rec["symbol"] = asset + "USDT"
            all_records.extend(records)
            print(f"✓ {len(records)} 条")
            success_count += 1
        else:
            print("✗ 失败")
            fail_count += 1

        # 请求间隔，避免限速
        time.sleep(0.1)

    print()
    print(f"📈 统计: 成功 {success_count}, 失败 {fail_count}")

    if not all_records:
        print("⚠️  没有获取到任何数据")
        return 1

    # 构造 DataFrame
    df = pd.DataFrame(all_records)

    # 转换时间戳
    if "timestamp" in df.columns:
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="ms")

    # 转换利率为数值
    if "dailyInterestRate" in df.columns:
        df["dailyInterestRate"] = pd.to_numeric(df["dailyInterestRate"], errors="coerce")

    # 重新排列列
    cols = ["symbol", "asset"]
    if "datetime" in df.columns:
        cols.append("datetime")
    if "timestamp" in df.columns:
        cols.append("timestamp")
    if "dailyInterestRate" in df.columns:
        cols.append("dailyInterestRate")
    # 添加其他列
    for c in df.columns:
        if c not in cols:
            cols.append(c)
    df = df[cols]

    # 按 symbol 和时间排序
    sort_cols = []
    if "symbol" in df.columns:
        sort_cols.append("symbol")
    if "timestamp" in df.columns:
        sort_cols.append("timestamp")
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=[True, False])

    print()
    print("=" * 80)
    print("📋 借贷利率 DataFrame:")
    print("=" * 80)
    print(f"Shape: {df.shape}")
    print()
    print(df.to_string(max_rows=50))

    # 打印每个 symbol 的均值统计
    if "dailyInterestRate" in df.columns:
        print()
        print("=" * 80)
        print("📊 每个 symbol 的日利率均值 (%):")
        print("=" * 80)
        stats = df.groupby("symbol")["dailyInterestRate"].agg(["mean", "std", "count"])
        stats["mean_pct"] = stats["mean"] * 100
        stats["std_pct"] = stats["std"] * 100
        print(stats[["mean_pct", "std_pct", "count"]].to_string())

    # 保存到文件
    if args.output:
        df.to_csv(args.output, index=False)
        print()
        print(f"💾 已保存到: {args.output}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
