#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
从 rolling_metrics_thresholds 读取百分位数据，生成价差阈值并同步到 Redis。

工作流程：
  1. 从 Redis 读取 fr_dump_symbols:binance_margin 和 fr_trade_symbols:binance_margin
  2. 合并两个列表得到要同步的 symbols（去重）
  3. 从 rolling_metrics_thresholds 读取这些 symbols 的百分位数据
  4. 根据 SPREAD_THRESHOLD_MAPPING 配置，提取对应的百分位值
  5. 生成价差阈值并写入 binance_spread_thresholds

读取 Redis:
  - String `fr_dump_symbols:binance_margin` - 平仓列表（JSON 数组）
  - String `fr_trade_symbols:binance_margin` - 建仓列表（JSON 数组）
  - Hash `rolling_metrics_thresholds` - rolling metrics 百分位数据

写入 Redis Hash:
  `binance_spread_thresholds` - 价差阈值（每个 symbol 12个字段）

配置说明：
  - SPREAD_THRESHOLD_MAPPING: 百分位映射配置
    格式: "binance_{factor}_{percentile}"
    示例: "binance_bidask_10" = bidask_sr 的第 10 百分位

输出格式: {symbol}_{direction}_{operation}_{mode}
  - direction: forward, backward
  - operation: open, cancel, close
  - mode: mm, mt

示例：
  python scripts/sync_spread_thresholds.py                     # 从 Redis 读取 symbols 并同步
  python scripts/sync_spread_thresholds.py --symbol BTCUSDT    # 只同步 BTCUSDT
  python scripts/sync_spread_thresholds.py --redis-url redis://:pwd@127.0.0.1:6379/0
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from typing import Dict, List, Optional, Set


def try_import_redis():
    try:
        import redis  # type: ignore
        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync spread thresholds from rolling metrics to Redis")
    p.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    p.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    p.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    p.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    p.add_argument("--symbol", help="只同步指定 symbol（如 BTCUSDT）")
    p.add_argument(
        "--rolling-key",
        default="rolling_metrics_thresholds",
        help="Rolling metrics Redis Hash key (default: rolling_metrics_thresholds)"
    )
    p.add_argument(
        "--write-key",
        default="binance_spread_thresholds",
        help="写入的 Redis Hash key (default: binance_spread_thresholds)"
    )
    p.add_argument(
        "--dump-key",
        default="fr_dump_symbols:binance_margin",
        help="平仓列表 Redis key (default: fr_dump_symbols:binance_margin)"
    )
    p.add_argument(
        "--trade-key",
        default="fr_trade_symbols:binance_margin",
        help="建仓列表 Redis key (default: fr_trade_symbols:binance_margin)"
    )
    return p.parse_args()


# ========== 价差阈值映射配置 ==========
#
# 将每个阈值字段映射到 rolling metrics 的百分位字段
# 格式: "binance_{factor}_{percentile}"
#   - factor: bidask, askbid, spread
#   - percentile: 5, 10, 15, 85, 90, 95
#
# 此配置对所有 symbol 通用，只是每个 symbol 计算出的具体值不同

SPREAD_THRESHOLD_MAPPING = {
    "forward_open_mm": "binance_spread_10", # spread < binance_spread_10
    "forward_open_mt": "binance_bidask_10",
    "forward_cancel_mm": "binance_spread_15", # spread > q15
    "forward_cancel_mt": "binance_bidask_15", 
    
    "backward_open_mm": "binance_spread_90", # spread > q90
    "backward_open_mt": "binance_askbid_5",  
    "backward_cancel_mm": "binance_spread_85", #spread < q85
    "backward_cancel_mt": "binance_askbid_5",
}

THRESHOLD_ORDER = list(SPREAD_THRESHOLD_MAPPING.keys())


def load_symbol_lists(rds, dump_key: str, trade_key: str) -> List[str]:
    """
    从 Redis 读取 dump 和 trade 列表，返回并集（去重且排序）

    参数：
      - dump_key: 平仓列表 Redis key
      - trade_key: 建仓列表 Redis key

    返回：
      合并后的 symbol 列表（大写、去重、排序）
    """
    symbols_set: Set[str] = set()

    # 读取平仓列表
    dump_data = rds.get(dump_key)
    if dump_data:
        dump_str = dump_data.decode('utf-8', 'ignore') if isinstance(dump_data, bytes) else str(dump_data)
        try:
            dump_list = json.loads(dump_str)
            if isinstance(dump_list, list):
                symbols_set.update(s.upper() for s in dump_list if s)
                print(f"📖 从 '{dump_key}' 读取 {len(dump_list)} 个 symbols")
        except Exception as e:
            print(f"⚠️  解析 '{dump_key}' 失败: {e}")

    # 读取建仓列表
    trade_data = rds.get(trade_key)
    if trade_data:
        trade_str = trade_data.decode('utf-8', 'ignore') if isinstance(trade_data, bytes) else str(trade_data)
        try:
            trade_list = json.loads(trade_str)
            if isinstance(trade_list, list):
                symbols_set.update(s.upper() for s in trade_list if s)
                print(f"📖 从 '{trade_key}' 读取 {len(trade_list)} 个 symbols")
        except Exception as e:
            print(f"⚠️  解析 '{trade_key}' 失败: {e}")

    result = sorted(symbols_set)
    print(f"✅ 合并后共 {len(result)} 个唯一 symbols")
    return result


def read_rolling_metrics(rds, key: str) -> Dict[str, Dict]:
    """读取 rolling metrics Hash 数据"""
    result: Dict[str, Dict] = {}
    data = rds.hgetall(key)
    for k, v in data.items():
        field = k.decode("utf-8", "ignore") if isinstance(k, bytes) else str(k)
        val = v.decode("utf-8", "ignore") if isinstance(v, bytes) else str(v)
        try:
            obj = json.loads(val)
            if isinstance(obj, dict):
                result[field] = obj
        except Exception:
            continue
    return result


def extract_quantile_value(obj: Dict, field_ref: str) -> Optional[float]:
    """
    从 rolling metrics 对象中提取百分位值

    field_ref 格式: "binance_{factor}_{percentile}"
    - factor: bidask, askbid, spread
    - percentile: 5, 10, 15, 85, 90, 95
    """
    # 解析字段引用
    parts = field_ref.split("_")
    if len(parts) < 3 or parts[0] != "binance":
        return None

    factor = parts[1]  # bidask, askbid, 或 spread
    percentile_str = parts[2]  # 5, 10, 15, 85, 90, 95

    try:
        percentile = float(percentile_str) / 100.0  # 转换为 0.05, 0.10 等
    except ValueError:
        return None

    # 确定 quantile key
    if factor == "bidask":
        quantile_key = "bidask_quantiles"
    elif factor == "askbid":
        quantile_key = "askbid_quantiles"
    elif factor == "spread":
        quantile_key = "spread_quantiles"
    else:
        return None

    # 提取对应的 quantile 值
    quantiles = obj.get(quantile_key)
    if not isinstance(quantiles, list):
        return None

    for item in quantiles:
        if not isinstance(item, dict):
            continue
        q = item.get("quantile")
        if q is None:
            continue
        try:
            q_val = float(q)
            if q_val > 1.0:
                q_val /= 100.0
            if abs(q_val - percentile) < 0.001:  # 允许微小误差
                threshold = item.get("threshold")
                if threshold is not None and not (isinstance(threshold, float) and math.isnan(threshold)):
                    return float(threshold)
        except (ValueError, TypeError):
            continue

    return None


def sync_thresholds(
    rds,
    rolling_key: str,
    write_key: str,
    dump_key: str,
    trade_key: str,
    filter_symbol: Optional[str] = None
) -> int:
    """
    从 rolling metrics 生成价差阈值并同步到 Redis

    返回: 写入的字段数量
    """
    # 确定要处理的 symbols
    if filter_symbol:
        target_symbols = [filter_symbol.upper()]
        print(f"🎯 目标 symbols: {filter_symbol.upper()} (单独指定)")
    else:
        # 从 Redis 读取 symbol 列表
        target_symbols = load_symbol_lists(rds, dump_key, trade_key)
        if not target_symbols:
            print("❌ 未找到任何 symbols，请检查 Redis 中的 dump/trade 列表")
            return 0
        print(f"🎯 目标 symbols: {', '.join(target_symbols)} (共 {len(target_symbols)} 个)")

    # 读取 rolling metrics
    rolling_data = read_rolling_metrics(rds, rolling_key)
    if not rolling_data:
        print(f"⚠️  未找到 rolling metrics 数据 (key: {rolling_key})")
        return 0

    # 提取目标 symbol 的数据
    symbol_data: Dict[str, Dict] = {}
    missing_symbols: Set[str] = set()

    for symbol in target_symbols:
        symbol_upper = symbol.upper()
        found = False

        # 在 rolling_data 中查找匹配的数据
        for field_key, obj in rolling_data.items():
            base_symbol = obj.get("base_symbol") or obj.get("symbol")
            if not base_symbol:
                continue

            if str(base_symbol).upper() == symbol_upper:
                symbol_data[symbol_upper] = obj
                found = True
                break

        if not found:
            missing_symbols.add(symbol_upper)

    if missing_symbols:
        print(f"⚠️  以下 symbols 未在 rolling_metrics 中找到: {', '.join(sorted(missing_symbols))}")

    if not symbol_data:
        print("❌ 没有任何 symbol 有可用的 rolling metrics 数据")
        return 0

    print(f"✅ 找到 {len(symbol_data)} 个 symbols 的 rolling metrics 数据")

    # 生成阈值字段
    all_fields = {}
    skipped_symbols: Set[str] = set()

    for symbol, obj in symbol_data.items():
        symbol_ok = True
        for suffix, field_ref in SPREAD_THRESHOLD_MAPPING.items():
            value = extract_quantile_value(obj, field_ref)
            if value is None:
                print(f"⚠️  {symbol}: 无法提取 {suffix} (来源: {field_ref})，跳过该 symbol")
                symbol_ok = False
                skipped_symbols.add(symbol)
                break

            field_key = f"{symbol}_{suffix}"
            # 格式化为字符串，保留足够精度
            all_fields[field_key] = f"{value:.8f}".rstrip("0").rstrip(".")

        if not symbol_ok:
            # 清理已添加的字段
            for suffix in SPREAD_THRESHOLD_MAPPING.keys():
                field_key = f"{symbol}_{suffix}"
                all_fields.pop(field_key, None)

    if not all_fields:
        print("❌ 所有 symbols 都无法提取完整的阈值数据")
        return 0

    # 写入 Redis
    rds.hset(write_key, mapping=all_fields)

    successful_symbols = len(symbol_data) - len(skipped_symbols)
    print(f"✅ 已写入 {len(all_fields)} 个价差阈值到 HASH '{write_key}'")
    print(f"   成功: {successful_symbols} 个 symbols")
    if skipped_symbols:
        print(f"   跳过: {len(skipped_symbols)} 个 symbols ({', '.join(sorted(skipped_symbols))})")

    return len(all_fields)


def print_three_line_table(headers: List[str], rows: List[List[str]]) -> None:
    """打印三线表格"""
    ncols = len(headers)
    widths = [0] * ncols
    for i, h in enumerate(headers):
        widths[i] = max(widths[i], len(h))
    for r in rows:
        for i, cell in enumerate(r):
            widths[i] = max(widths[i], len(cell))

    def fmt_row(values: List[str]) -> str:
        parts: List[str] = []
        for i, v in enumerate(values):
            parts.append(v.ljust(widths[i]))
        return "  ".join(parts)

    header_line = fmt_row(headers)
    top_rule = "=" * len(header_line)
    mid_rule = "-" * len(header_line)
    bot_rule = "=" * len(header_line)

    print(top_rule)
    print(header_line)
    print(mid_rule)
    for row in rows:
        print(fmt_row(row))
    print(bot_rule)


def print_thresholds(rds, write_key: str, filter_symbol: Optional[str] = None) -> None:
    """打印价差阈值配置"""
    print("\n📊 价差阈值配置 (对所有 symbol 通用):")
    print("-" * 80)

    # 打印配置映射表
    headers = ["operation", "percentile_reference"]
    rows: List[List[str]] = []

    for operation in THRESHOLD_ORDER:
        percentile_ref = SPREAD_THRESHOLD_MAPPING.get(operation, "-")
        rows.append([operation, percentile_ref])

    print_three_line_table(headers, rows)

    # 读取 Redis 数据并统计
    data = rds.hgetall(write_key)
    if not data:
        print("\n⚠️  Redis 中未找到阈值数据")
        return

    # 解码数据
    kv: Dict[str, str] = {}
    for k, v in data.items():
        kk = k.decode('utf-8', 'ignore') if isinstance(k, bytes) else str(k)
        vv = v.decode('utf-8', 'ignore') if isinstance(v, bytes) else str(v)
        kv[kk] = vv

    # 统计 symbol
    all_symbols = set()
    for field_key in kv.keys():
        parts = field_key.split("_")
        if len(parts) >= 4:
            symbol = "_".join(parts[:-3])
            all_symbols.add(symbol)

    print(f"\n📈 统计:")
    print(f"   - 已同步 symbols: {len(all_symbols)} 个")
    print(f"   - 阈值字段总数: {len(kv)} 个")
    if all_symbols:
        print(f"   - Symbols 列表: {', '.join(sorted(all_symbols))}")


def main() -> int:
    args = parse_args()
    redis = try_import_redis()
    if redis is None:
        print("❌ redis 包未安装，请使用 pip install redis", file=sys.stderr)
        return 2

    rds = redis.from_url(args.redis_url) if args.redis_url else redis.Redis(
        host=args.host, port=args.port, db=args.db, password=args.password
    )

    print("🔄 开始从 rolling metrics 同步价差阈值...")
    print(f"📍 Redis: {args.host}:{args.port}/{args.db}")
    print(f"📖 Rolling Metrics: {args.rolling_key}")
    print(f"📖 Dump List: {args.dump_key}")
    print(f"📖 Trade List: {args.trade_key}")
    print(f"📝 写入: {args.write_key}")
    print()

    # 同步阈值
    count = sync_thresholds(
        rds,
        args.rolling_key,
        args.write_key,
        args.dump_key,
        args.trade_key,
        args.symbol
    )
    if count == 0:
        return 1

    # 打印结果
    print_thresholds(rds, args.write_key, args.symbol)

    print("\n✅ 同步完成！")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
