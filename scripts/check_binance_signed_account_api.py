#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import os
import sys
import time
from typing import Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter


class SourceAddressAdapter(HTTPAdapter):
    def __init__(self, source_address: str, **kwargs):
        self._source_address = (source_address, 0)
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        pool_kwargs["source_address"] = self._source_address
        return super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)

    def proxy_manager_for(self, proxy, **proxy_kwargs):
        proxy_kwargs["source_address"] = self._source_address
        return super().proxy_manager_for(proxy, **proxy_kwargs)


def build_signed_url(base_url: str, path: str, api_secret: str, recv_window: int) -> str:
    params = {
        "timestamp": str(int(time.time() * 1000)),
        "recvWindow": str(recv_window),
    }
    query = urlencode(sorted(params.items()))
    signature = hmac.new(
        api_secret.encode("utf-8"),
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"{base_url.rstrip('/')}{path}?{query}&signature={signature}"


def preview(value: str, limit: int) -> str:
    value = value.strip()
    if len(value) <= limit:
        return value
    return value[:limit] + "...(truncated)"


def run_once(
    base_url: str,
    path: str,
    api_key: str,
    api_secret: str,
    local_address: Optional[str],
    timeout: float,
    recv_window: int,
    body_chars: int,
) -> int:
    sess = requests.Session()
    if local_address:
        adapter = SourceAddressAdapter(local_address, max_retries=0)
        sess.mount("https://", adapter)
        sess.mount("http://", adapter)

    url = build_signed_url(base_url, path, api_secret, recv_window)
    print(f"base_url={base_url}")
    print(f"path={path}")
    print(f"local_address={local_address or '-'}")
    print(f"url={url}")
    sys.stdout.flush()

    try:
        resp = sess.get(
            url,
            headers={"X-MBX-APIKEY": api_key.strip()},
            timeout=timeout,
        )
    except Exception as exc:
        print(f"request_error={type(exc).__name__}: {exc}")
        return 2

    print(f"status={resp.status_code}")
    for header in [
        "x-mbx-used-weight",
        "x-mbx-used-weight-1m",
        "x-mbx-order-count-1m",
        "content-type",
    ]:
        if header in resp.headers:
            print(f"header[{header}]={resp.headers[header]}")

    body = resp.text
    print("body_preview=")
    print(preview(body, body_chars))
    return 0 if resp.ok else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check Binance signed account REST API reachability")
    parser.add_argument("--base-url", default="https://api.binance.com")
    parser.add_argument("--path", default="/api/v3/account")
    parser.add_argument("--local-address", default=None)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--recv-window", type=int, default=5000)
    parser.add_argument("--body-chars", type=int, default=1200)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("BINANCE_API_KEY / BINANCE_API_SECRET not set", file=sys.stderr)
        return 2

    return run_once(
        base_url=args.base_url,
        path=args.path,
        api_key=api_key,
        api_secret=api_secret,
        local_address=args.local_address,
        timeout=args.timeout,
        recv_window=args.recv_window,
        body_chars=args.body_chars,
    )


if __name__ == "__main__":
    raise SystemExit(main())
