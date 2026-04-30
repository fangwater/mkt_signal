#!/usr/bin/env python3
"""配置 Bybit 账号以满足现货保证金 (BybitMargin) 下单的前置条件。

需要满足两点：
  1) 账号已升级到 UTA (unifiedMarginStatus >= 3)
  2) 现货保证金已开启 (spotMarginMode == "1")

任一未满足时 trade_engine 启动会被 src/trade_engine/bybit_precheck.rs 拦截。

使用方式：
  BYBIT_API_KEY=... BYBIT_API_SECRET=... python3 scripts/setup_bybit_spot_margin.py
  添加 --check-only 仅查询不修改
  添加 --yes 跳过交互确认 (不可回滚的 UTA 升级仍需用户清楚后果)

注意：
  - UTA 升级是单向的，一旦升级无法回退到 Classic。
  - Spot Margin 开关需要先在 Bybit 网页/APP 完成知识测验 (quiz)。
    若返回特定错误码，脚本会提示去网页操作。
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sys
import time
from typing import Any, Dict, Tuple

import requests

HOST = os.environ.get("BYBIT_API_BASE", "https://api.bybit.com").rstrip("/")
RECV_WINDOW_MS = "5000"

PATH_ACCOUNT_INFO = "/v5/account/info"
PATH_UPGRADE_TO_UTA = "/v5/account/upgrade-to-uta"
PATH_SPOT_MARGIN_STATE = "/v5/spot-margin-trade/state"
PATH_SPOT_MARGIN_SWITCH = "/v5/spot-margin-trade/switch-mode"

MIN_UNIFIED_MARGIN_STATUS = 3
UNIFIED_STATUS_LABEL = {
    1: "Classic",
    3: "UTA 1.0",
    4: "UTA 1.0 Pro",
    5: "UTA 2.0",
    6: "UTA 2.0 Pro",
}


def load_credentials() -> Tuple[str, str]:
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
        raise SystemExit(2)
    return api_key, api_secret


def sign(api_secret: str, api_key: str, timestamp_ms: str, payload: str) -> str:
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
) -> Tuple[int, Dict[str, Any]]:
    timestamp_ms = str(int(time.time() * 1000))
    payload_for_sign = body if method.upper() != "GET" else query
    signature = sign(api_secret, api_key, timestamp_ms, payload_for_sign)
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
    resp = requests.request(method.upper(), url, headers=headers, data=body or None, timeout=15)
    try:
        data = resp.json()
    except ValueError:
        raise RuntimeError(
            f"Bybit {method} {path} non-JSON response: http={resp.status_code} body={resp.text!r}"
        )
    return resp.status_code, data


def assert_ok(http_status: int, data: Dict[str, Any], path: str) -> Dict[str, Any]:
    ret_code = data.get("retCode")
    if http_status >= 300 or ret_code not in (0, "0"):
        raise RuntimeError(
            f"Bybit {path} failed: http={http_status} retCode={ret_code} retMsg={data.get('retMsg')!r}"
        )
    return data


def get_account_info(api_key: str, api_secret: str) -> Dict[str, Any]:
    status, data = request(api_key, api_secret, "GET", PATH_ACCOUNT_INFO)
    return assert_ok(status, data, PATH_ACCOUNT_INFO).get("result") or {}


def get_spot_margin_state(api_key: str, api_secret: str) -> Dict[str, Any]:
    status, data = request(api_key, api_secret, "GET", PATH_SPOT_MARGIN_STATE)
    return assert_ok(status, data, PATH_SPOT_MARGIN_STATE).get("result") or {}


def upgrade_to_uta(api_key: str, api_secret: str) -> Dict[str, Any]:
    body = "{}"
    status, data = request(api_key, api_secret, "POST", PATH_UPGRADE_TO_UTA, body=body)
    return assert_ok(status, data, PATH_UPGRADE_TO_UTA).get("result") or {}


def switch_spot_margin_on(api_key: str, api_secret: str) -> Dict[str, Any]:
    body = json.dumps({"spotMarginMode": "1"}, separators=(",", ":"))
    status, data = request(api_key, api_secret, "POST", PATH_SPOT_MARGIN_SWITCH, body=body)
    # spot margin 开关如果未做题，会返回特定 retCode；这里把原始 retCode/retMsg 透传给上层
    if status >= 300 or data.get("retCode") not in (0, "0"):
        raise RuntimeError(
            f"Bybit {PATH_SPOT_MARGIN_SWITCH} failed: "
            f"http={status} retCode={data.get('retCode')} retMsg={data.get('retMsg')!r}\n"
            f"  Hint: spot margin requires the on-web/app knowledge quiz to be completed first.\n"
            f"  Open Bybit web > Trade > Spot Margin and finish the quiz, then re-run."
        )
    return data.get("result") or {}


def confirm(prompt: str, assume_yes: bool) -> bool:
    if assume_yes:
        print(f"{prompt} [auto-yes]")
        return True
    try:
        ans = input(f"{prompt} [y/N] ").strip().lower()
    except EOFError:
        return False
    return ans in {"y", "yes"}


def describe_unified_status(status: int) -> str:
    return UNIFIED_STATUS_LABEL.get(status, f"unknown({status})")


def ensure_uta(api_key: str, api_secret: str, *, check_only: bool, assume_yes: bool) -> int:
    info = get_account_info(api_key, api_secret)
    status = int(info.get("unifiedMarginStatus") or 0)
    print(
        f"[1/2] account info: unifiedMarginStatus={status} ({describe_unified_status(status)}) "
        f"marginMode={info.get('marginMode')!r}"
    )
    if status >= MIN_UNIFIED_MARGIN_STATUS:
        print("      -> already on UTA, OK")
        return status

    if check_only:
        print("      -> CHECK ONLY: would call POST /v5/account/upgrade-to-uta")
        return status

    print(
        "      WARNING: upgrading to UTA is IRREVERSIBLE. After upgrade you cannot return to Classic.\n"
        "               Read https://bybit-exchange.github.io/docs/v5/account/upgrade-unified-account first."
    )
    if not confirm("      Proceed to upgrade this account to UTA?", assume_yes):
        print("      -> aborted by user", file=sys.stderr)
        raise SystemExit(3)

    print("      -> POST /v5/account/upgrade-to-uta ...")
    upgrade_to_uta(api_key, api_secret)
    # 文档明确升级期间 REST/WS 数据可能不准，等几秒再校验
    time.sleep(5)
    info = get_account_info(api_key, api_secret)
    status = int(info.get("unifiedMarginStatus") or 0)
    print(
        f"      -> post-upgrade unifiedMarginStatus={status} ({describe_unified_status(status)})"
    )
    if status < MIN_UNIFIED_MARGIN_STATUS:
        print(
            "      -> upgrade did not take effect yet; Bybit may still be processing. "
            "Wait a minute and re-run with --check-only.",
            file=sys.stderr,
        )
        raise SystemExit(4)
    return status


def ensure_spot_margin(api_key: str, api_secret: str, *, check_only: bool, assume_yes: bool) -> str:
    state = get_spot_margin_state(api_key, api_secret)
    mode = str(state.get("spotMarginMode") or "")
    leverage = state.get("spotLeverage")
    print(f"[2/2] spot margin state: spotMarginMode={mode!r} spotLeverage={leverage!r}")
    if mode == "1":
        print("      -> spot margin already ON, OK")
        return mode

    if check_only:
        print("      -> CHECK ONLY: would call POST /v5/spot-margin-trade/switch-mode {\"spotMarginMode\":\"1\"}")
        return mode

    if not confirm("      Enable spot margin trading on this account?", assume_yes):
        print("      -> aborted by user", file=sys.stderr)
        raise SystemExit(5)

    print("      -> POST /v5/spot-margin-trade/switch-mode ...")
    switch_spot_margin_on(api_key, api_secret)
    state = get_spot_margin_state(api_key, api_secret)
    mode = str(state.get("spotMarginMode") or "")
    print(f"      -> post-switch spotMarginMode={mode!r}")
    if mode != "1":
        print("      -> switch did not take effect; check Bybit account settings.", file=sys.stderr)
        raise SystemExit(6)
    return mode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--check-only", action="store_true", help="只查询当前状态，不做任何修改。")
    parser.add_argument("--yes", action="store_true", help="跳过交互确认 (UTA 升级仍会打印警告)。")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    api_key, api_secret = load_credentials()

    ensure_uta(api_key, api_secret, check_only=args.check_only, assume_yes=args.yes)
    ensure_spot_margin(api_key, api_secret, check_only=args.check_only, assume_yes=args.yes)

    if args.check_only:
        print("CHECK ONLY: see lines above for which steps would run.")
    else:
        print("ALL READY: account is on UTA and spot margin is ON.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
