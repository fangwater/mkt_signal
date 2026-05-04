#!/usr/bin/env python3
"""查询并切换 Bitget UTA 账户等级（basic / advanced）。

Bitget UTA Advanced mode 是 bitget-margin 自动借币下单的前置条件。

用法:
  # 仅查询当前 accountLevel（dry-run）
  BITGET_API_KEY=... BITGET_API_SECRET=... BITGET_API_PASSPHRASE=... \\
    python scripts/bitget_set_account_level.py

  # 切换到 advanced
  BITGET_API_KEY=... BITGET_API_SECRET=... BITGET_API_PASSPHRASE=... \\
    python scripts/bitget_set_account_level.py --target advanced --execute

  # 通过 env.sh 注入凭证
  source ~/bitget-intra-arb01/env.sh && \\
    python /home/ubuntu/crypto_mkt/mkt_signal/scripts/bitget_set_account_level.py --execute

  # 母账户为子账户切换
  python scripts/bitget_set_account_level.py --target advanced --target-uid 123456789 --execute
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

DEFAULT_BASE_URL = os.environ.get("BITGET_API_BASE", "https://api.bitget.com")

ACCOUNT_SETTINGS_PATH = "/api/v3/account/settings"
ADJUST_ACCOUNT_MODE_PATH = "/api/v3/account/adjust-account-mode"

ACCOUNT_LEVEL_NAMES = {
    "basic": "基础模式 (Basic Mode)",
    "advanced": "进阶模式 (Advanced Mode)",
}


def now_ms() -> str:
    return str(int(time.time() * 1000))


def sign(timestamp_ms: str, method: str, signed_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp_ms}{method.upper()}{signed_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


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
    timestamp_ms = now_ms()
    signature = sign(timestamp_ms, method, path, body_str, api_secret)

    url = f"{base_url.rstrip('/')}{path}"
    data = body_str.encode("utf-8") if body_str else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("ACCESS-KEY", api_key)
    req.add_header("ACCESS-SIGN", signature)
    req.add_header("ACCESS-TIMESTAMP", timestamp_ms)
    req.add_header("ACCESS-PASSPHRASE", passphrase)
    req.add_header("Content-Type", "application/json")
    req.add_header("locale", "en-US")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8", errors="replace")


def load_credentials() -> Tuple[str, str, str]:
    api_key = os.environ.get("BITGET_API_KEY", "").strip()
    api_secret = os.environ.get("BITGET_API_SECRET", "").strip()
    passphrase = os.environ.get("BITGET_API_PASSPHRASE", "").strip()
    missing = [
        name
        for name, value in [
            ("BITGET_API_KEY", api_key),
            ("BITGET_API_SECRET", api_secret),
            ("BITGET_API_PASSPHRASE", passphrase),
        ]
        if not value
    ]
    if missing:
        print(f"ERROR: 缺少环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def parse_json_response(path: str, status: int, body: str) -> Dict[str, Any]:
    if status != 200:
        print(f"ERROR: {path} http_status={status} body={body[:512]}", file=sys.stderr)
        sys.exit(2)
    try:
        v = json.loads(body)
    except json.JSONDecodeError:
        print(f"ERROR: {path} 返回非 JSON: {body[:512]}", file=sys.stderr)
        sys.exit(2)
    code = str(v.get("code", "-1"))
    if code != "00000":
        print(f"ERROR: {path} code={code} msg={v.get('msg')}", file=sys.stderr)
        sys.exit(2)
    return v


def fetch_account_settings(
    base_url: str, api_key: str, api_secret: str, passphrase: str, timeout: int
) -> Dict[str, Any]:
    status, body = _do_request(
        base_url,
        "GET",
        ACCOUNT_SETTINGS_PATH,
        api_key,
        api_secret,
        passphrase,
        timeout=timeout,
    )
    v = parse_json_response(ACCOUNT_SETTINGS_PATH, status, body)
    data = v.get("data") or {}
    if not isinstance(data, dict):
        print(f"ERROR: {ACCOUNT_SETTINGS_PATH} data 不是对象: {data!r}", file=sys.stderr)
        sys.exit(2)
    return data


def set_account_level(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    target: str,
    target_uid: Optional[str],
    timeout: int,
) -> None:
    payload: Dict[str, Any] = {"mode": target}
    if target_uid:
        payload["targetUid"] = target_uid
    status, body = _do_request(
        base_url,
        "POST",
        ADJUST_ACCOUNT_MODE_PATH,
        api_key,
        api_secret,
        passphrase,
        body=payload,
        timeout=timeout,
    )
    parse_json_response(ADJUST_ACCOUNT_MODE_PATH, status, body)
    target_desc = ACCOUNT_LEVEL_NAMES.get(target, target)
    uid_desc = f" targetUid={target_uid}" if target_uid else ""
    print(f"OK: 已提交 Bitget accountLevel 切换为 {target} ({target_desc}){uid_desc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="查询/切换 Bitget UTA accountLevel (basic / advanced)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--target",
        choices=["advanced", "basic"],
        default="advanced",
        help="目标账户等级",
    )
    parser.add_argument(
        "--target-uid",
        default=None,
        help="目标子账户 UID；不传则切换当前 API key 所属账户",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="真实切换；默认 dry-run 只查询",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Bitget REST base URL")
    parser.add_argument("--timeout", type=int, default=10, help="HTTP timeout 秒")
    return parser.parse_args()


def print_settings(data: Dict[str, Any], label: str) -> None:
    uid = data.get("uid", "?")
    account_mode = data.get("accountMode", "?")
    account_level = data.get("accountLevel", "?")
    asset_mode = data.get("assetMode", "?")
    hold_mode = data.get("holdMode", "?")
    level_name = ACCOUNT_LEVEL_NAMES.get(str(account_level), "未知")
    print(
        f"{label}: uid={uid} accountMode={account_mode} "
        f"accountLevel={account_level} ({level_name}) "
        f"assetMode={asset_mode} holdMode={hold_mode}"
    )


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")

    before = fetch_account_settings(base_url, api_key, api_secret, passphrase, args.timeout)
    print_settings(before, "当前")

    current_level = str(before.get("accountLevel", "")).lower()
    if current_level == args.target and not args.target_uid:
        print(f"已满足要求（accountLevel={args.target}），无需切换。")
        return

    target_desc = ACCOUNT_LEVEL_NAMES.get(args.target, args.target)
    uid_desc = f" targetUid={args.target_uid}" if args.target_uid else ""
    print(f"需切换: accountLevel {current_level or '?'} -> {args.target} ({target_desc}){uid_desc}")

    if not args.execute:
        print("Dry-run，仅查询。加 --execute 后真实切换。")
        return

    set_account_level(
        base_url,
        api_key,
        api_secret,
        passphrase,
        args.target,
        args.target_uid,
        args.timeout,
    )

    after = fetch_account_settings(base_url, api_key, api_secret, passphrase, args.timeout)
    print_settings(after, "切换后")


if __name__ == "__main__":
    main()
