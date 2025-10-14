#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
清理 pre_trade Redis 持久化数据。

默认会删除以下键（假设前缀为 pre_trade）：
  - pre_trade:orders
  - pre_trade:strategies
  - pre_trade:saved_at

示例：
  python scripts/cleanup_pre_trade_store.py --redis-url redis://:pwd@127.0.0.1:6379/0
  python scripts/cleanup_pre_trade_store.py --host 192.168.1.10 --port 6379 --prefix my_pretrade --dry-run
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import List, Tuple


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="清理 pre_trade Redis 持久化快照",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--redis-url", default=os.environ.get("REDIS_URL"))
    parser.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    parser.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    parser.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    parser.add_argument(
        "--prefix",
        default=os.environ.get("PRE_TRADE_STORE_PREFIX", "pre_trade"),
        help="pre_trade 持久化使用的键名前缀",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅展示将被删除的键，不真正执行删除",
    )
    parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="无需确认直接执行删除",
    )
    return parser.parse_args()


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        print("redis 包未安装，请先通过 `pip install redis` 安装。", file=sys.stderr)
        sys.exit(2)
    if args.redis_url:
        return redis.from_url(args.redis_url)
    return redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)


def target_keys(prefix: str) -> List[str]:
    return [f"{prefix}:orders", f"{prefix}:strategies", f"{prefix}:saved_at"]


def describe_existing(rds, keys: List[str]) -> List[Tuple[str, bool, int]]:
    results: List[Tuple[str, bool, int]] = []
    for key in keys:
        exists = bool(rds.exists(key))
        size = 0
        if exists:
            try:
                size = rds.strlen(key)
            except Exception:
                size = -1
        results.append((key, exists, size))
    return results


def confirm(prompt: str) -> bool:
    answer = input(f"{prompt} [y/N]: ").strip().lower()
    return answer in {"y", "yes"}


def main() -> int:
    args = parse_args()
    rds = connect_redis(args)

    keys = target_keys(args.prefix)
    info = describe_existing(rds, keys)

    if not any(exists for _, exists, _ in info):
        print(f"[INFO] 未发现前缀 {args.prefix} 下的持久化键，无需清理。")
        return 0

    print("[INFO] 即将清理以下键：")
    for key, exists, size in info:
        status = "存在" if exists else "不存在"
        size_str = f"{size} bytes" if size >= 0 else "-"
        print(f"  - {key}: {status} size={size_str}")

    if args.dry_run:
        print("[DRY-RUN] 预览模式，不执行删除。")
        return 0

    if not args.yes and not confirm("确认删除上述键吗？"):
        print("已取消。")
        return 0

    deleted = 0
    for key, exists, _ in info:
        if not exists:
            continue
        try:
            deleted += rds.delete(key)
        except Exception as exc:
            print(f"[WARN] 删除 {key} 失败: {exc}", file=sys.stderr)

    print(f"[DONE] 共删除 {deleted} 个键。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
