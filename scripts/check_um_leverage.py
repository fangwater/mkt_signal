#!/usr/bin/env python3
"""检查 Redis 中列出的 Binance UM 交易对当前杠杆倍数。

参考 ``set_um_leverage.py``，从 Redis HASH 读取 symbol 列表，
调用 ``GET /papi/v1/um/account``，输出每个交易对当前杠杆、保证金模式等信息。
"""

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Optional, Tuple


def normalize_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "yes", "y"}:
            return True
        if lowered in {"false", "0", "no", "n"}:
            return False
    return None


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="检查 Binance UM 杠杆：从 Redis HASH 获取 symbol 列表并打印当前杠杆"
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("REDIS_URL"),
        help="Redis URL，例如 redis://:pwd@host:6379/0（存在则覆盖 host/port/db）",
    )
    parser.add_argument("--host", default=os.environ.get("REDIS_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("REDIS_PORT", 6379)))
    parser.add_argument("--db", type=int, default=int(os.environ.get("REDIS_DB", 0)))
    parser.add_argument("--password", default=os.environ.get("REDIS_PASSWORD"))
    parser.add_argument(
        "--key",
        default="binance_arb_price_spread_threshold",
        help="Redis HASH key，字段名视为 symbol（默认：binance_arb_price_spread_threshold）",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com"),
        help="Binance PAPI 基础域名（默认：https://papi.binance.com）",
    )
    parser.add_argument(
        "--symbols",
        nargs="*",
        help="直接指定 symbol 列表（可与 Redis 同时使用；省略则仅使用 Redis）",
    )
    parser.add_argument(
        "--skip-redis",
        action="store_true",
        help="仅使用 --symbols 指定的 symbol，忽略 Redis HASH",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        default=None,
        help="自定义 recvWindow（毫秒）。不指定时使用 Binance 默认 5000。",
    )
    return parser.parse_args() 


def now_ms() -> int:
    return int(time.time() * 1000)


def sign(query: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def get_papi(
    base_url: str,
    path: str,
    params: Dict[str, Any],
    api_key: str,
    api_secret: str,
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    q = dict(params)
    q.setdefault("recvWindow", "5000")
    q["timestamp"] = str(now_ms())
    items = sorted((k, str(v)) for k, v in q.items())
    query = urllib.parse.urlencode(items, safe="-_.~")
    sig = sign(query, api_secret)
    url = f"{base_url}{path}?{query}&signature={sig}"
    req = urllib.request.Request(url, method="GET", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
            return status, body, headers
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        headers = dict(getattr(e, "headers", {}).items()) if getattr(e, "headers", None) else {}
        return e.code, body, headers
    except Exception as e:  # pragma: no cover - 网络异常直接返回错误
        return 0, str(e), {}


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        print("ERROR: 需要 redis 模块，请先执行 pip install redis", file=sys.stderr)
        sys.exit(1)
    if args.redis_url:
        return redis.from_url(args.redis_url)
    return redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)


def fetch_symbols(rds, key: str) -> List[str]:
    try:
        raw_keys: Iterable[bytes] = rds.hkeys(key)
    except Exception as exc:
        print(f"ERROR: 无法读取 Redis HASH '{key}': {exc}")
        sys.exit(1)

    symbols: List[str] = []
    for field in raw_keys:
        if isinstance(field, bytes):
            sym = field.decode("utf-8", "ignore")
        else:
            sym = str(field)
        sym = sym.strip().upper()
        if sym:
            symbols.append(sym)

    return sorted(set(symbols))


def fetch_um_positions(
    base_url: str, api_key: str, api_secret: str, recv_window: Optional[int]
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any], Dict[str, str]]:
    params: Dict[str, Any] = {}
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)

    status, body, headers = get_papi(base_url, "/papi/v1/um/account", params, api_key, api_secret)
    if not (200 <= status < 300):
        print(f"ERROR: GET /papi/v1/um/account 失败，status={status}")
        print(f"body: {body}")
        sys.exit(1)

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        print(f"ERROR: 无法解析响应 JSON: {exc}")
        print(f"body: {body}")
        sys.exit(1)

    positions = data.get("positions")
    if not isinstance(positions, list):
        print("ERROR: /papi/v1/um/account 响应缺少 positions 字段")
        sys.exit(1)

    mapping: Dict[str, Dict[str, Any]] = {}
    for entry in positions:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol", "")).strip().upper()
        if symbol:
            mapping[symbol] = entry

    return mapping, data, headers


def detect_account_margin_mode(account_data: Dict[str, Any]) -> Optional[str]:
    visited: set[int] = set()

    def _search(obj: Any, depth: int = 0) -> Optional[str]:
        if depth > 6:
            return None
        obj_id = id(obj)
        if obj_id in visited:
            return None
        visited.add(obj_id)

        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "accountType" and isinstance(value, str) and value.strip():
                    return value.strip().upper()
                if key in {"portfolioMargin", "isPortfolioMargin", "portfolioMarginAccount"}:
                    bool_val = normalize_bool(value)
                    if bool_val:
                        return "PORTFOLIO"
                res = _search(value, depth + 1)
                if res:
                    return res
        elif isinstance(obj, list):
            for item in obj:
                res = _search(item, depth + 1)
                if res:
                    return res
        return None

    return _search(account_data)


def normalize_margin_mode(entry: Dict[str, Any], account_mode: Optional[str]) -> str:
    margin_type = entry.get("marginType")
    if isinstance(margin_type, str) and margin_type.strip():
        return margin_type.strip().upper()
    isolated_flag = normalize_bool(entry.get("isolated"))
    if isolated_flag is not None:
        return "ISOLATED" if isolated_flag else "CROSSED"
    if account_mode:
        return account_mode
    return "UNKNOWN"


def normalize_number(value: Any) -> str:
    if value is None:
        return "0"
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else "0"
    return str(value)


def main():
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: 请在环境变量中设置 BINANCE_API_KEY 与 BINANCE_API_SECRET。")
        sys.exit(1)

    base_url = args.base_url.rstrip("/")

    cli_symbols = []
    if args.symbols:
        cli_symbols = [s.strip().upper() for s in args.symbols if s and s.strip()]

    symbols: List[str] = []
    redis_used = False
    if cli_symbols:
        symbols.extend(cli_symbols)

    if not args.skip_redis:
        # 需要 Redis symbol 列表（默认行为）
        rds = connect_redis(args)
        redis_used = True
        symbols.extend(fetch_symbols(rds, args.key))

    symbols = sorted(set(sym for sym in symbols if sym))

    positions_map, account_data, headers = fetch_um_positions(base_url, api_key, api_secret, args.recv_window)
    total_positions = len(positions_map)
    used_w = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    account_mode = detect_account_margin_mode(account_data)

    print(
        f"成功获取 UM 账户信息：positions={total_positions}，used_weight={used_w}（via {base_url}）"
    )
    if account_mode:
        print(f"账户 accountType={account_mode}")

    if not symbols:
        print("提示：未提供 symbol，默认输出账户中所有 position（按 symbol 排序）。")
        symbols = sorted(positions_map.keys())
    else:
        sources = []
        if cli_symbols:
            sources.append("命令行")
        if redis_used:
            sources.append("Redis")
        source_str = "、".join(sources) if sources else "未知"
        print(f"将输出 {len(symbols)} 个 symbol (来源: {source_str})")

    if not symbols:
        print("账户中没有任何 symbol 信息。")
        return

    for idx, symbol in enumerate(symbols, start=1):
        entry = positions_map.get(symbol)
        if entry is None:
            print(f"[{idx}/{len(symbols)}] {symbol}: 未在账户 positions 中找到")
            continue

        leverage = entry.get("leverage")
        leverage_str = str(leverage) if leverage is not None else "N/A"
        margin_mode = normalize_margin_mode(entry, account_mode)
        position_side = entry.get("positionSide") or entry.get("positionMode") or ""
        pos_amt = normalize_number(entry.get("positionAmt") or entry.get("positionAmount"))
        pos_margin = normalize_number(entry.get("positionInitialMargin") or entry.get("initialMargin"))
        open_margin = normalize_number(entry.get("openOrderInitialMargin"))
        maint_margin = normalize_number(entry.get("maintMargin") or entry.get("maintenanceMargin"))
        wallet = normalize_number(entry.get("isolatedWallet") or entry.get("marginBalance"))
        unrealized = normalize_number(entry.get("unrealizedProfit"))
        print(
            f"[{idx}/{len(symbols)}] {symbol}: leverage={leverage_str}, margin_mode={margin_mode}, "
            f"position_side={position_side}, position_amt={pos_amt}, "
            f"pos_margin={pos_margin}, open_margin={open_margin}, maint_margin={maint_margin}, "
            f"wallet={wallet}, unrealized={unrealized}"
        )


if __name__ == "__main__":
    main()
