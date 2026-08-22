#!/usr/bin/env python3
"""Convert Binance STANDARD spot dust assets to BNB.

Default is dry-run. Use --execute to call SAPI /sapi/v1/asset/dust.
"""

from __future__ import annotations

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
from typing import Iterable, List, Tuple


DEFAULT_BASE_URL = os.environ.get("BINANCE_SAPI_URL") or "https://api.binance.com"
DUST_PATH = "/sapi/v1/asset/dust"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Binance spot dust assets to BNB",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Binance SAPI base URL")
    parser.add_argument(
        "--asset",
        action="append",
        default=[],
        help="Asset to convert, repeatable: --asset WLD --asset XRP",
    )
    parser.add_argument(
        "--assets",
        help="Comma/space separated assets to convert: WLD,XRP",
    )
    parser.add_argument("--recv-window", type=int, default=5000, help="recvWindow in milliseconds")
    parser.add_argument("--execute", action="store_true", help="Actually submit dust conversion")
    return parser.parse_args()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: missing BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        raise SystemExit(1)
    return api_key, api_secret


def parse_assets(asset_args: Iterable[str], assets_arg: str | None) -> List[str]:
    raw: List[str] = []
    raw.extend(asset_args)
    if assets_arg:
        raw.extend(urllib.parse.unquote(assets_arg).replace(",", " ").split())
    out: List[str] = []
    seen: set[str] = set()
    for item in raw:
        asset = item.strip().upper()
        if not asset or asset == "BNB" or asset in seen:
            continue
        seen.add(asset)
        out.append(asset)
    if not out:
        print("ERROR: require --asset or --assets", file=sys.stderr)
        raise SystemExit(2)
    return out


def now_ms() -> int:
    return int(time.time() * 1000)


def sign_query(query: str, api_secret: str) -> str:
    return hmac.new(api_secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()


def submit_dust_convert(
    base_url: str,
    assets: List[str],
    api_key: str,
    api_secret: str,
    recv_window: int,
) -> Tuple[int, str]:
    params: List[Tuple[str, str]] = [("asset", asset) for asset in assets]
    params.append(("recvWindow", str(recv_window)))
    params.append(("timestamp", str(now_ms())))
    query = urllib.parse.urlencode(sorted(params, key=lambda kv: kv[0]), safe="-_.~")
    signature = sign_query(query, api_secret)
    url = f"{base_url.rstrip('/')}{DUST_PATH}?{query}&signature={signature}"
    req = urllib.request.Request(url, method="POST", headers={"X-MBX-APIKEY": api_key})
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            status = resp.getcode()
            body = resp.read().decode("utf-8", errors="replace")
            headers = dict(resp.headers.items())
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read().decode("utf-8", errors="replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
    except Exception as exc:
        status = 0
        body = str(exc)
        headers = {}
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    print(f"[dust] status={status} used_weight={weight}")
    print(body)
    return status, body


def main() -> None:
    args = parse_args()
    assets = parse_assets(args.asset, args.assets)
    print(f"[dust] assets={','.join(assets)} execute={args.execute}")
    if not args.execute:
        print("dry-run: add --execute to convert dust to BNB")
        return

    api_key, api_secret = load_credentials()
    status, body = submit_dust_convert(
        args.base_url,
        assets,
        api_key,
        api_secret,
        args.recv_window,
    )
    if not (200 <= status < 300):
        try:
            parsed = json.loads(body)
            if str(parsed.get("code", "")) == "32110":
                print("ERROR: dust conversion is rate limited; Binance allows one request per hour", file=sys.stderr)
            elif str(parsed.get("code", "")) == "-31002":
                print("ERROR: no eligible dust balance for requested asset(s)", file=sys.stderr)
        except json.JSONDecodeError:
            pass
        raise SystemExit(1)


if __name__ == "__main__":
    main()
