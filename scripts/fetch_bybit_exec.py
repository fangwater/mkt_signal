#!/usr/bin/env python3
"""Pull Bybit /v5/execution/list fills for a symbol+window, both categories, to JSON.

Reads BYBIT_API_KEY / BYBIT_API_SECRET from env (source env.sh first).
Usage: fetch_bybit_exec.py --symbol ETHUSDT --start 2026-05-29T03:00:00Z --end 2026-05-29T04:00:00Z --out /tmp/exec.json
"""
from __future__ import annotations
import argparse, hashlib, hmac, json, os, sys, time
from datetime import datetime, timezone
import urllib.parse, urllib.request

HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV = "5000"


def sign(secret, ts, payload):
    raw = f"{ts}{os.environ['BYBIT_API_KEY']}{RECV}{payload}"
    return hmac.new(secret.encode(), raw.encode(), hashlib.sha256).hexdigest()


def get(path, params):
    key = os.environ["BYBIT_API_KEY"]
    sec = os.environ["BYBIT_API_SECRET"]
    query = urllib.parse.urlencode(params)
    ts = str(int(time.time() * 1000))
    headers = {
        "X-BAPI-API-KEY": key,
        "X-BAPI-SIGN": sign(sec, ts, query),
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": ts,
        "X-BAPI-RECV-WINDOW": RECV,
    }
    url = f"{HOST}{path}?{query}"
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read().decode())


def to_ms(s):
    return int(datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp() * 1000)


def fetch_category(category, symbol, start_ms, end_ms):
    rows, cursor = [], ""
    while True:
        params = {"category": category, "symbol": symbol,
                  "startTime": start_ms, "endTime": end_ms, "limit": 100}
        if cursor:
            params["cursor"] = cursor
        resp = get("/v5/execution/list", params)
        if resp.get("retCode") != 0:
            print(f"[{category}] API error: {resp.get('retCode')} {resp.get('retMsg')}", file=sys.stderr)
            sys.exit(2)
        res = resp.get("result", {})
        batch = res.get("list", []) or []
        for e in batch:
            e["_category"] = category
        rows.extend(batch)
        cursor = res.get("nextPageCursor") or ""
        print(f"[{category}] +{len(batch)} (total {len(rows)}) cursor={'yes' if cursor else 'end'}", file=sys.stderr)
        if not cursor or not batch:
            break
        time.sleep(0.1)
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--categories", default="spot,linear")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    start_ms, end_ms = to_ms(a.start), to_ms(a.end)
    print(f"window ms [{start_ms}, {end_ms}]  symbol={a.symbol}", file=sys.stderr)
    all_rows = []
    for cat in a.categories.split(","):
        all_rows.extend(fetch_category(cat.strip(), a.symbol, start_ms, end_ms))
    with open(a.out, "w") as f:
        json.dump(all_rows, f)
    print(f"wrote {len(all_rows)} executions -> {a.out}", file=sys.stderr)
    # quick stdout summary
    by_cat = {}
    for e in all_rows:
        by_cat.setdefault(e["_category"], 0)
        by_cat[e["_category"]] += 1
    print(json.dumps({"total": len(all_rows), "by_category": by_cat}))


if __name__ == "__main__":
    main()
