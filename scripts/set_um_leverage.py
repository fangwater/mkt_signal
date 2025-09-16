#!/usr/bin/env python3
"""
Simple script to batch-set Binance UM leverage via PAPI.

Reads config from JSON (default: config/binance_um_leverage.json):
{
  "base_url": "https://papi.binance.com",
  "default_leverage": 3,
  "symbols": ["BTCUSDT", {"symbol":"ETHUSDT","leverage":5}, ...]
}

API key/secret are read from environment:
  BINANCE_API_KEY, BINANCE_API_SECRET

Usage:
  python3 scripts/set_um_leverage.py [path/to/config.json]
"""

import json
import sys
import time
import hmac
import hashlib
import urllib.parse
import urllib.request
import urllib.error
import os


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


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_symbols(cfg: dict):
    syms = cfg.get("symbols", [])
    for item in syms:
        if isinstance(item, str):
            yield item, None
        elif isinstance(item, dict):
            sym = item.get("symbol")
            lev = item.get("leverage")
            if sym:
                yield sym, lev


def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else "config/binance_um_leverage.json"
    cfg = load_config(cfg_path)

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: Please set BINANCE_API_KEY and BINANCE_API_SECRET in environment.")
        sys.exit(1)

    base_url = cfg.get("base_url", "https://papi.binance.com")
    default_leverage = int(cfg.get("default_leverage", 3))

    symbols = list(iter_symbols(cfg))
    if not symbols:
        print("No symbols in config; nothing to do.")
        return

    print(f"Setting UM leverage on {len(symbols)} symbols (default={default_leverage}) via {base_url} ...")
    ok = 0
    for i, (symbol, lev_override) in enumerate(symbols, start=1):
        leverage = int(lev_override or default_leverage)
        params = {"symbol": symbol, "leverage": str(leverage)}
        status, body, headers = post_papi(base_url, "/papi/v1/um/leverage", params, api_key, api_secret)
        used_w = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
        ord_cnt = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
        tag = "OK" if 200 <= status < 300 else "ERR"
        print(f"[{i}/{len(symbols)}] {symbol} -> {leverage}: {tag} {status}; used_weight={used_w}, order_count={ord_cnt}")
        if not (200 <= status < 300):
            print(f"  body: {body}")
        else:
            ok += 1
        # gentle pacing
        time.sleep(0.12)

    print(f"Done. success={ok}/{len(symbols)}")


if __name__ == "__main__":
    main()

