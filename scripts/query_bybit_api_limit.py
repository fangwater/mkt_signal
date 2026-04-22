#!/usr/bin/env python3
"""Query or set Bybit V5 API rate limits, and query API key information.

Docs:
  https://bybit-exchange.github.io/docs/zh-TW/v5/rate-limit/rules-for-pros/apilimit-query

Examples:
  python scripts/query_bybit_api_limit.py --uid 290118
  python scripts/query_bybit_api_limit.py --uid 290118 --pretty
  python scripts/query_bybit_api_limit.py --uid 290118 --biz-type DERIVATIVES --set-rate 1000
  python scripts/query_bybit_api_limit.py --uid 290118 --biz-type DERIVATIVES --set-rate 1000 --execute --pretty
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
import urllib.parse
from typing import Any, Dict, List

import requests

HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BYBIT_API_KEY", "").strip()
    api_secret = os.environ.get("BYBIT_API_SECRET", "").strip()
    missing = [
        name
        for name, value in (
            ("BYBIT_API_KEY", api_key),
            ("BYBIT_API_SECRET", api_secret),
        )
        if not value
    ]
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def sign(api_key: str, api_secret: str, timestamp_ms: str, payload: str) -> str:
    raw = f"{timestamp_ms}{api_key}{RECV_WINDOW_MS}{payload}"
    return hmac.new(api_secret.encode("utf-8"), raw.encode("utf-8"), hashlib.sha256).hexdigest()


def request(
    api_key: str,
    api_secret: str,
    method: str,
    path: str,
    *,
    query: str = "",
    body: str = "",
) -> Dict[str, Any]:
    timestamp_ms = str(int(time.time() * 1000))
    payload = body if method.upper() != "GET" else query
    signature = sign(api_key, api_secret, timestamp_ms, payload)
    url = f"{HOST}{path}"
    if query:
        url = f"{url}?{query}"
    headers = {
        "X-BAPI-API-KEY": api_key,
        "X-BAPI-SIGN": signature,
        "X-BAPI-SIGN-TYPE": "2",
        "X-BAPI-TIMESTAMP": timestamp_ms,
        "X-BAPI-RECV-WINDOW": RECV_WINDOW_MS,
        "Content-Type": "application/json",
    }
    resp = requests.request(method.upper(), url, headers=headers, data=body, timeout=10)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(f"Bybit {method} {path} returned non-JSON: {resp.status_code} {resp.text}")
    if resp.status_code >= 300 or data.get("retCode") not in (0, "0", None):
        raise RuntimeError(f"Bybit {method} {path} failed: http={resp.status_code} body={data}")
    return data


def normalize_uid(value: str) -> str:
    return re.sub(r"\s+", "", str(value or ""))


def parse_uids(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in args.uid:
        uid = normalize_uid(raw)
        if uid and uid not in seen:
            out.append(uid)
            seen.add(uid)
    for chunk in args.uids:
        for part in re.split(r"[\s,]+", chunk.strip()):
            uid = normalize_uid(part)
            if uid and uid not in seen:
                out.append(uid)
                seen.add(uid)
    if not out:
        raise SystemExit("missing uid input: use --uid or --uids")
    return out


def normalize_biz_type(value: str) -> str:
    return re.sub(r"\s+", "", str(value or "")).upper()


def parse_biz_types(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    seen = set()
    for raw in args.biz_type:
        for part in re.split(r"[\s,]+", raw.strip()):
            biz_type = normalize_biz_type(part)
            if biz_type and biz_type not in seen:
                out.append(biz_type)
                seen.add(biz_type)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query or set Bybit V5 API rate limits, and query API key information.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--uid", action="append", default=[], help="Single UID, can be repeated.")
    parser.add_argument(
        "--uids",
        action="append",
        default=[],
        help="Comma/space separated UID list, e.g. 290118,290119.",
    )
    parser.add_argument(
        "--biz-type",
        action="append",
        default=[],
        help="Bybit bizType for set API, can repeat or use comma-separated values, e.g. SPOT,DERIVATIVES.",
    )
    parser.add_argument("--set-rate", type=int, help="Target API rate limit per second.")
    parser.add_argument("--execute", action="store_true", help="Actually send POST /v5/apilimit/set.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print full JSON response.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()
    uids = parse_uids(args)
    biz_types = parse_biz_types(args)

    if args.set_rate is not None and not biz_types:
        raise SystemExit("--biz-type is required when using --set-rate")

    query = urllib.parse.urlencode({"uids": ",".join(uids)})
    api_limit_data = request(api_key, api_secret, "GET", "/v5/apilimit/query", query=query)
    api_key_info_data = request(api_key, api_secret, "GET", "/v5/user/query-api")
    combined = {
        "requestedUids": uids,
        "apiLimitQuery": api_limit_data,
        "apiKeyInfoQuery": api_key_info_data,
    }

    if args.set_rate is not None:
        payload = {
            "list": []
        }
        for biz_type in biz_types:
            payload["list"].append(
                {
                    "uids": ",".join(uids),
                    "bizType": biz_type,
                    "rate": int(args.set_rate),
                }
            )
        combined["apiLimitSetPlan"] = payload
        if args.execute:
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=True)
            combined["apiLimitSetResult"] = request(api_key, api_secret, "POST", "/v5/apilimit/set", body=body)
        else:
            combined["apiLimitSetDryRun"] = True

    if args.pretty:
        print(json.dumps(combined, ensure_ascii=False, indent=2, sort_keys=True))
        return 0

    print(json.dumps(combined, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
