#!/usr/bin/env python3
"""查询并切换 OKX 账户保证金模式（acctLv）。

acctLv 含义:
  1 = 简单模式
  2 = 单币种保证金
  3 = 多币种保证金（Multi-currency margin）← 策略最低要求
  4 = 组合保证金（Portfolio margin）

升级到 acctLv=3 可直接通过 API 切换（账户无持仓/挂单时）。
升级到 acctLv=4 需要先在 OKX 官网申请开通。

用法:
  # 仅查询当前 acctLv（dry-run）
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/okx_set_account_level.py

  # 切换到多币种保证金（acctLv=3）
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/okx_set_account_level.py --target 3 --execute

  # 通过 env.sh 注入凭证
  source ~/okex-intra-arb01/env.sh && \\
    python scripts/okx_set_account_level.py --target 3 --execute
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any, Dict, Optional, Tuple

DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com")

ACCT_LV_NAMES = {
    "1": "简单模式",
    "2": "单币种保证金",
    "3": "多币种保证金 (Multi-currency margin)",
    "4": "组合保证金 (Portfolio margin)",
}


def utc_timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime())


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def _do_request(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    body: Optional[Dict[str, Any]] = None,
    timeout: int = 10,
) -> Tuple[int, str]:
    method = method.upper()
    body_str = "" if body is None else json.dumps(body, separators=(",", ":"))
    timestamp = utc_timestamp()
    signature = sign(timestamp, method, path, body_str, api_secret)

    url = f"{base_url.rstrip('/')}{path}"
    data = body_str.encode() if body_str else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    api_secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in [
            ("OKX_API_KEY", api_key),
            ("OKX_API_SECRET", api_secret),
            ("OKX_PASSPHRASE", passphrase),
        ]
        if not value
    ]
    if missing:
        print(f"ERROR: 缺少环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def fetch_acct_lv(
    base_url: str, api_key: str, api_secret: str, passphrase: str, timeout: int
) -> str:
    status, body = _do_request(base_url, "GET", "/api/v5/account/config",
                                api_key, api_secret, passphrase, timeout=timeout)
    if status != 200:
        print(f"ERROR: GET /api/v5/account/config http_status={status} body={body[:256]}", file=sys.stderr)
        sys.exit(2)
    v = json.loads(body)
    code = v.get("code", "-1")
    if code != "0":
        print(f"ERROR: GET /api/v5/account/config code={code} msg={v.get('msg')}", file=sys.stderr)
        sys.exit(2)
    data = v.get("data", [{}])
    acct_lv = str(data[0].get("acctLv", "?"))
    return acct_lv


def set_acct_lv(
    base_url: str, api_key: str, api_secret: str, passphrase: str, target: int, timeout: int
) -> None:
    status, body = _do_request(
        base_url, "POST", "/api/v5/account/set-account-level",
        api_key, api_secret, passphrase,
        body={"acctLv": str(target)},
        timeout=timeout,
    )
    if status != 200:
        print(f"ERROR: POST /api/v5/account/set-account-level http_status={status} body={body[:256]}", file=sys.stderr)
        sys.exit(2)
    v = json.loads(body)
    code = v.get("code", "-1")
    if code != "0":
        print(f"ERROR: set-account-level 失败 code={code} msg={v.get('msg')}", file=sys.stderr)
        sys.exit(2)
    print(f"OK: 账户模式已切换至 acctLv={target} ({ACCT_LV_NAMES.get(str(target), '?')})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="查询/切换 OKX 账户保证金模式 (acctLv)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--target", type=int, choices=[3, 4], default=3,
        help="目标 acctLv: 3=多币种保证金, 4=组合保证金",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="真实切换；默认 dry-run 只查询",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="OKX REST base URL")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout 秒")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")

    acct_lv = fetch_acct_lv(base_url, api_key, api_secret, passphrase, args.timeout)
    lv_name = ACCT_LV_NAMES.get(acct_lv, "未知")
    print(f"当前 acctLv={acct_lv} ({lv_name})")

    if int(acct_lv) >= args.target:
        print(f"已满足要求（>= {args.target}），无需切换。")
        return

    print(f"需切换: acctLv {acct_lv} → {args.target} ({ACCT_LV_NAMES.get(str(args.target), '?')})")

    if not args.execute:
        print("Dry-run，仅查询。加 --execute 后真实切换。")
        return

    set_acct_lv(base_url, api_key, api_secret, passphrase, args.target, args.timeout)


if __name__ == "__main__":
    main()
