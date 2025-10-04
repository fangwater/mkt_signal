#!/usr/bin/env python3
"""Set Binance UM dual-side (hedge) position mode for symbols defined in Redis.

Usage:
  python scripts/set_um_dual_side.py [--redis-url ...] [--key HASH_KEY]

The script reuses the symbol list from Redis (default hash:
``binance_arb_price_spread_threshold``) simply as the scope definition. For each
symbol it sends a PAPI request to enable dual-side position mode (hedge mode).
The endpoint is account-scoped, but repeating the request per symbol is safe and
helps with idempotency when running in parallel with other tools.

Environment variables:
  - ``BINANCE_API_KEY``
  - ``BINANCE_API_SECRET``

Requires the ``redis`` Python package.
"""

import argparse
import hashlib
import hmac
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Iterable, List


def try_import_redis():
    try:
        import redis  # type: ignore

        return redis
    except Exception:
        return None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enable Binance UM dual-side (hedge) position mode for symbols listed in a Redis hash"
    )
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
        "--key",
        default="binance_arb_price_spread_threshold",
        help="Redis HASH key containing symbol entries (default: binance_arb_price_spread_threshold)",
    )
    parser.add_argument(
        "--base-url",
        default=os.environ.get("BINANCE_PAPI_URL", "https://papi.binance.com"),
        help="Binance PAPI base url (default: https://papi.binance.com)",
    )
    parser.add_argument(
        "--sleep",
        type=float,
        default=0.12,
        help="Sleep seconds between requests to avoid rate limits (default: 0.12)",
    )
    parser.add_argument(
        "--disable",
        action="store_true",
        help="Disable dual-side (switch back to single-side). Default: enable dual-side.",
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
    query = urllib.parse.urlencode(items, safe="-_.~")
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
        headers = dict(getattr(e, "headers", {}).items()) if getattr(e, "headers", None) else {}
        return e.code, body, headers
    except Exception as e:  # pragma: no cover - network failures
        return 0, str(e), {}


def connect_redis(args: argparse.Namespace):
    redis = try_import_redis()
    if redis is None:
        print("ERROR: python-redis package is required (pip install redis)")
        sys.exit(1)
    if args.redis_url:
        return redis.from_url(args.redis_url)
    return redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password)


def fetch_symbols(rds, key: str) -> List[str]:
    try:
        raw_keys: Iterable[bytes] = rds.hkeys(key)
    except Exception as exc:
        print(f"ERROR: failed to read Redis hash '{key}': {exc}")
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

    unique = sorted(set(symbols))
    return unique


def main():
    args = parse_args()

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: Please set BINANCE_API_KEY and BINANCE_API_SECRET in environment.")
        sys.exit(1)

    rds = connect_redis(args)
    symbols = fetch_symbols(rds, args.key)
    if not symbols:
        print(f"No symbols found in Redis hash '{args.key}'.")
        return

    base_url = args.base_url.rstrip("/")
    dual_value = not args.disable
    dual_str = "true" if dual_value else "false"

    print(
        f"Setting dualSidePosition={dual_str} for {len(symbols)} symbols via {base_url} ..."
    )

    ok = 0
    for idx, symbol in enumerate(symbols, start=1):
        params = {"dualSidePosition": dual_str}
        status, body, headers = post_papi(
            base_url,
            "/papi/v1/positionSide/dual",
            params,
            api_key,
            api_secret,
        )
        used_w = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
        tag = "OK" if 200 <= status < 300 else "ERR"
        print(
            f"[{idx}/{len(symbols)}] {symbol} -> dualSidePosition={dual_str}: {tag} {status}; used_weight={used_w}"
        )
        if not (200 <= status < 300):
            print(f"  body: {body}")
        else:
            ok += 1
        time.sleep(args.sleep)

    print(f"Done. success={ok}/{len(symbols)} (note: endpoint is account-scoped, repeated calls are idempotent)")


if __name__ == "__main__":
    main()

