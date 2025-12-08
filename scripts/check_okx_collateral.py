#!/usr/bin/env python3
"""查询和设置 OKX 质押币种配置

用法：
  # 查询所有质押币种配置
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/check_okx_collateral.py

  # 设置所有币种为可质押
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/check_okx_collateral.py --set-all --enable

  # 设置特定币种为可质押
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/check_okx_collateral.py --set-custom USDT,USDC,BTC --enable

  # 查询账户配置信息
  OKX_API_KEY=... OKX_API_SECRET=... OKX_PASSPHRASE=... \\
    python scripts/check_okx_collateral.py --account-config
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False
    print("警告: pandas 未安装，将使用纯文本输出", file=sys.stderr)

DEFAULT_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com")


def utc_timestamp() -> str:
    """OKX expects ISO8601 with milliseconds, suffixed by Z."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def sign(timestamp: str, method: str, request_path: str, body: str, secret: str) -> str:
    payload = f"{timestamp}{method.upper()}{request_path}{body}"
    digest = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def request_okx(
    base_url: str,
    method: str,
    path: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    params: Dict[str, Any] | None = None,
    body: Dict[str, Any] | None = None,
    timeout: int = 10,
) -> Tuple[int, str, Dict[str, str]]:
    method = method.upper()
    params = params or {}
    query = urllib.parse.urlencode(params) if params else ""
    request_path = path
    if query:
        request_path = f"{path}?{query}"

    body_str = "" if method == "GET" else (json.dumps(body, ensure_ascii=False, separators=(",", ":")) if body else "")
    timestamp = utc_timestamp()
    signature = sign(timestamp, method, request_path, body_str, api_secret)

    url = f"{base_url.rstrip('/')}{request_path}"
    data = None if method == "GET" else (body_str.encode("utf-8") if body_str else None)
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("OK-ACCESS-KEY", api_key)
    req.add_header("OK-ACCESS-SIGN", signature)
    req.add_header("OK-ACCESS-TIMESTAMP", timestamp)
    req.add_header("OK-ACCESS-PASSPHRASE", passphrase)
    req.add_header("Content-Type", "application/json")

    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            status = resp.getcode()
            body_text = resp.read().decode("utf-8", "replace")
            headers = dict(resp.headers.items())
            return status, body_text, headers
    except urllib.error.HTTPError as exc:
        body_text = exc.read().decode("utf-8", "replace")
        headers = dict(getattr(exc, "headers", {}).items()) if getattr(exc, "headers", None) else {}
        return exc.code, body_text, headers
    except Exception as exc:
        return 0, str(exc), {}


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
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret, passphrase


def query_collateral_assets(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    ccy: str | None = None,
) -> Dict[str, Any]:
    """查询质押币种配置"""
    params = {}
    if ccy:
        params["ccy"] = ccy

    status, body, _ = request_okx(
        base_url,
        "GET",
        "/api/v5/account/collateral-assets",
        api_key,
        api_secret,
        passphrase,
        params=params,
    )

    if status != 200:
        print(f"❌ 查询失败 status={status}", file=sys.stderr)
        print(f"响应内容: {body}", file=sys.stderr)
        sys.exit(1)

    return json.loads(body)


def set_collateral_assets(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
    type_: str,
    collateral_enabled: bool,
    ccy_list: List[str] | None = None,
) -> Dict[str, Any]:
    """设置质押币种"""
    payload: Dict[str, Any] = {
        "type": type_,
        "collateralEnabled": collateral_enabled,
    }
    if ccy_list and type_ == "custom":
        payload["ccyList"] = ccy_list

    status, body, _ = request_okx(
        base_url,
        "POST",
        "/api/v5/account/set-collateral-assets",
        api_key,
        api_secret,
        passphrase,
        body=payload,
    )

    if status != 200:
        print(f"❌ 设置失败 status={status}", file=sys.stderr)
        print(f"响应内容: {body}", file=sys.stderr)
        sys.exit(1)

    return json.loads(body)


def query_account_config(
    base_url: str,
    api_key: str,
    api_secret: str,
    passphrase: str,
) -> Dict[str, Any]:
    """查询账户配置"""
    status, body, _ = request_okx(
        base_url,
        "GET",
        "/api/v5/account/config",
        api_key,
        api_secret,
        passphrase,
    )

    if status != 200:
        print(f"❌ 查询账户配置失败 status={status}", file=sys.stderr)
        print(f"响应内容: {body}", file=sys.stderr)
        sys.exit(1)

    return json.loads(body)


def display_collateral_assets(data: Dict[str, Any]) -> None:
    """显示质押币种配置"""
    if data.get("code") != "0":
        print(f"❌ API 返回错误: code={data.get('code')}, msg={data.get('msg')}", file=sys.stderr)
        return

    assets = data.get("data", [])
    if not assets:
        print("⚠️  没有查询到质押币种配置")
        return

    if HAS_PANDAS:
        df = pd.DataFrame(assets)
        print("\n=== 质押币种配置 ===")
        print(df.to_string(index=False))
        print(f"\n总计: {len(df)} 个币种")

        enabled_count = df[df["collateralEnabled"] == True].shape[0]
        disabled_count = df[df["collateralEnabled"] == False].shape[0]
        print(f"已启用: {enabled_count} 个")
        print(f"未启用: {disabled_count} 个")

        if disabled_count > 0:
            print("\n⚠️  未启用质押的币种:")
            disabled_ccys = df[df["collateralEnabled"] == False]["ccy"].tolist()
            print(", ".join(disabled_ccys))
    else:
        print("\n=== 质押币种配置 ===")
        for asset in assets:
            status_icon = "✅" if asset.get("collateralEnabled") else "❌"
            print(f"{status_icon} {asset.get('ccy'):10s} collateralEnabled={asset.get('collateralEnabled')}")
        print(f"\n总计: {len(assets)} 个币种")


def display_account_config(data: Dict[str, Any]) -> None:
    """显示账户配置"""
    if data.get("code") != "0":
        print(f"❌ API 返回错误: code={data.get('code')}, msg={data.get('msg')}", file=sys.stderr)
        return

    config_list = data.get("data", [])
    if not config_list:
        print("⚠️  没有查询到账户配置")
        return

    config = config_list[0] if config_list else {}

    print("\n=== 账户配置信息 ===")
    print(f"账户等级 (acctLv):        {config.get('acctLv')}")
    print(f"持仓模式 (posMode):        {config.get('posMode')}")
    print(f"自动借币 (autoLoan):       {config.get('autoLoan')}")
    print(f"希腊字母 (greeksType):     {config.get('greeksType')}")
    print(f"账户级别 (level):          {config.get('level')}")
    print(f"临时等级 (levelTmp):       {config.get('levelTmp')}")
    print(f"")
    print("账户等级说明:")
    print("  1: 简单交易模式")
    print("  2: 单币种保证金模式")
    print("  3: 跨币种保证金模式")
    print("  4: 组合保证金模式")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="查询和设置 OKX 质押币种配置",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="OKX REST base URL",
    )
    parser.add_argument(
        "--ccy",
        help="查询特定币种，多个币种用逗号分隔，如: USDT,BTC,ETH",
    )
    parser.add_argument(
        "--account-config",
        action="store_true",
        help="查询账户配置信息（账户等级、持仓模式等）",
    )
    parser.add_argument(
        "--set-all",
        action="store_true",
        help="设置所有币种",
    )
    parser.add_argument(
        "--set-custom",
        metavar="CCY_LIST",
        help="设置自定义币种列表，用逗号分隔，如: USDT,BTC,ETH",
    )
    parser.add_argument(
        "--enable",
        action="store_true",
        help="启用质押（与 --set-all 或 --set-custom 配合使用）",
    )
    parser.add_argument(
        "--disable",
        action="store_true",
        help="禁用质押（与 --set-all 或 --set-custom 配合使用）",
    )
    # 过滤掉 Jupyter 的参数
    import sys
    filtered_args = [arg for arg in sys.argv[1:] if not arg.startswith('--f=') and not arg.startswith('-f=')]
    return parser.parse_args(filtered_args)


def main() -> None:
    args = parse_args()
    api_key, api_secret, passphrase = load_credentials()
    base_url = args.base_url.rstrip("/")

    print(f"🔗 Base URL: {base_url}")
    print(f"🔑 API Key: {api_key[:8]}...{api_key[-4:]}")

    # 查询账户配置
    if args.account_config:
        print("\n📊 查询账户配置...")
        config_data = query_account_config(base_url, api_key, api_secret, passphrase)
        display_account_config(config_data)
        print("")

    # 设置质押币种
    if args.set_all or args.set_custom:
        if not args.enable and not args.disable:
            print("❌ 请指定 --enable 或 --disable", file=sys.stderr)
            sys.exit(1)

        collateral_enabled = args.enable

        if args.set_all:
            print(f"\n⚙️  设置所有币种 collateralEnabled={collateral_enabled}...")
            result = set_collateral_assets(
                base_url,
                api_key,
                api_secret,
                passphrase,
                type_="all",
                collateral_enabled=collateral_enabled,
            )
            print(f"✅ 设置成功")
            print(json.dumps(result, ensure_ascii=False, indent=2))
        elif args.set_custom:
            ccy_list = [c.strip().upper() for c in args.set_custom.split(",")]
            print(f"\n⚙️  设置币种 {ccy_list} collateralEnabled={collateral_enabled}...")
            result = set_collateral_assets(
                base_url,
                api_key,
                api_secret,
                passphrase,
                type_="custom",
                collateral_enabled=collateral_enabled,
                ccy_list=ccy_list,
            )
            print(f"✅ 设置成功")
            print(json.dumps(result, ensure_ascii=False, indent=2))

    # 查询质押币种配置
    print("\n📊 查询质押币种配置...")
    data = query_collateral_assets(base_url, api_key, api_secret, passphrase, ccy=args.ccy)
    display_collateral_assets(data)


def check_collateral(ccy: str | None = None, account_config: bool = True) -> None:
    """便捷函数：在 Jupyter 中查询质押币种和账户配置

    Args:
        ccy: 可选，查询特定币种，如 "USDT,BTC,ETH"
        account_config: 是否显示账户配置
    """
    api_key, api_secret, passphrase = load_credentials()
    base_url = DEFAULT_BASE_URL.rstrip("/")

    print(f"🔗 Base URL: {base_url}")
    print(f"🔑 API Key: {api_key[:8]}...{api_key[-4:]}")

    if account_config:
        print("\n📊 查询账户配置...")
        config_data = query_account_config(base_url, api_key, api_secret, passphrase)
        display_account_config(config_data)
        print("")

    print("\n📊 查询质押币种配置...")
    data = query_collateral_assets(base_url, api_key, api_secret, passphrase, ccy=ccy)
    display_collateral_assets(data)


def enable_all_collateral() -> None:
    """便捷函数：在 Jupyter 中启用所有币种作为质押"""
    api_key, api_secret, passphrase = load_credentials()
    base_url = DEFAULT_BASE_URL.rstrip("/")

    print(f"⚙️  设置所有币种 collateralEnabled=True...")
    result = set_collateral_assets(
        base_url,
        api_key,
        api_secret,
        passphrase,
        type_="all",
        collateral_enabled=True,
    )
    print(f"✅ 设置成功")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    # 再次查询确认
    print("\n📊 验证设置结果...")
    data = query_collateral_assets(base_url, api_key, api_secret, passphrase)
    display_collateral_assets(data)


def enable_collateral(ccys: str) -> None:
    """便捷函数：在 Jupyter 中启用指定币种作为质押

    Args:
        ccys: 币种列表，用逗号分隔，如 "USDT,BTC,ETH"
    """
    api_key, api_secret, passphrase = load_credentials()
    base_url = DEFAULT_BASE_URL.rstrip("/")

    ccy_list = [c.strip().upper() for c in ccys.split(",")]
    print(f"⚙️  设置币种 {ccy_list} collateralEnabled=True...")
    result = set_collateral_assets(
        base_url,
        api_key,
        api_secret,
        passphrase,
        type_="custom",
        collateral_enabled=True,
        ccy_list=ccy_list,
    )
    print(f"✅ 设置成功")
    print(json.dumps(result, ensure_ascii=False, indent=2))

    # 再次查询确认
    print("\n📊 验证设置结果...")
    data = query_collateral_assets(base_url, api_key, api_secret, passphrase, ccy=ccys)
    display_collateral_assets(data)


if __name__ == "__main__":
    main()
