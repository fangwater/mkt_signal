#!/usr/bin/env python3
"""Batch-set Binance UM leverage to a fixed value for all symbols.

Usage:
  python scripts/set_um_leverage.py [--redis-url ...] [--leverage 4]

The script reads symbols from two Redis String keys (JSON arrays):
  - fr_dump_symbols:binance_margin (平仓列表)
  - fr_trade_symbols:binance_margin (建仓列表)

Merges them (union, deduplicated), and issues PAPI requests to set the same
leverage (default: 4) on Binance UM. API credentials come from
``BINANCE_API_KEY`` and ``BINANCE_API_SECRET`` environment variables.
"""

import argparse
import json
import os
import sys
import time
import hmac
import hashlib
import urllib.parse
import urllib.request
import urllib.error
from typing import List, Set


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Set Binance UM leverage for symbols from Redis")
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL"),
        help="Redis URL, e.g. redis://:pwd@host:6379/0 (overrides host/port/db)",
    )
    parser.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    parser.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    parser.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    parser.add_argument(
        "--dump-key",
        default="fr_dump_symbols:binance_margin",
        help="平仓列表 Redis key (default: fr_dump_symbols:binance_margin)",
    )
    parser.add_argument(
        "--trade-key",
        default="fr_trade_symbols:binance_margin",
        help="建仓列表 Redis key (default: fr_trade_symbols:binance_margin)",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com"),
        help="Binance PAPI base url (default: https://papi.binance.com)",
    )
    parser.add_argument(
        "--leverage",
        type=int,
        default=4,
        help="Target leverage to apply to every symbol (default: 4)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.12,
        help="Sleep seconds between requests to avoid rate limits (default: 0.12)",
    )
    return parser.parse_args()


def now_ms() -> int:
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def post_papi(base_url: str, path: str, params: dict, api_key: str, api_secret: str, timeout: int = 10):
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    items = sorted(q.items(), key=lambda kv: kv[0])
    query = urllib.parse.urlencode(items, safe='-_.~')
    sig = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={sig}"
    req = urllib.request.Request(url, method="POST", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        headers = dict(getattr(e, 'headers', {}).items()) if getattr(e, 'headers', None) else {}
        return e.code, body, headers
    except Exception as e:
        return 0, str(e), {}


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        print("ERROR: python-redis package is required (pip install redis)")
        sys.exit(1)
    if args.redis_url:
        return redis.from_url(args.redis_url)
    return redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)


def load_symbol_lists(rds, dump_key: str, trade_key: str) -> List[str]:
    """
    从 Redis 读取 dump 和 trade 列表，返回并集（去重且排序）
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


def main():
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: Please set BINANCE_API_KEY and BINANCE_API_SECRET in environment.")
        sys.exit(1)

    leverage = int(args.leverage)
    if leverage <= 0:
        print(f"ERROR: leverage must be positive, got {leverage}")
        sys.exit(1)

    rds = connect_redis(args)
    symbols = load_symbol_lists(rds, args.dump_key, args.trade_key)
    if not symbols:
        print("No symbols found in Redis.")
        return

    base_url = args.base_url.rstrip("/")

    print(
        f"\nSetting UM leverage={leverage} on {len(symbols)} symbols via {base_url} ..."
    )

    ok = 0
    for idx, symbol in enumerate(symbols, start=1):
        params = {"symbol": symbol, "leverage": str(leverage)}
        status, body, headers = post_papi(base_url, "/papi/v1/um/leverage", params, api_key, api_secret)
        used_w = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
        ord_cnt = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
        tag = "OK" if 200 <= status < 300 else "ERR"
        print(
            f"[{idx}/{len(symbols)}] {symbol} -> {leverage}: {tag} {status}; used_weight={used_w}, order_count={ord_cnt}"
        )
        if not (200 <= status < 300):
            print(f"  body: {body}")
        else:
            ok += 1
        # gentle pacing
        time.sleep(args.sleep)

    print(f"Done. success={ok}/{len(symbols)}")


if __name__ == "__main__":
    main()
