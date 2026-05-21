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

Jupyter 用法：
  from scripts.check_okx_collateral import check_collateral, enable_all_collateral
  check_collateral()          # 查询
  enable_all_collateral()     # 启用所有
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    print("警告: requests 未安装，将使用 urllib", file=sys.stderr)
    import urllib.request
    import urllib.error

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

BASE_URL = os.environ.get("OKX_BASE_URL", "https://openapi.okx.com")


def okx_sign(timestamp: str, method: str, path: str, body: str, secret: str) -> str:
    """生成 OKX API 签名"""
    payload = f"{timestamp}{method.upper()}{path}{body}"
    return base64.b64encode(
        hmac.new(secret.encode(), payload.encode(), hashlib.sha256).digest()
    ).decode()


def okx_request(method: str, path: str, api_key: str, secret: str, passphrase: str,
                params: Dict = None, body: Dict = None) -> Dict[str, Any]:
    """发送 OKX API 请求"""
    timestamp = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    # 处理查询参数
    query_str = ""
    if params:
        query_str = "?" + "&".join(f"{k}={v}" for k, v in params.items())
    request_path = path + query_str

    # 处理 body
    body_str = json.dumps(body, separators=(",", ":")) if body else ""
    signature = okx_sign(timestamp, method, request_path, body_str, secret)

    headers = {
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "Content-Type": "application/json"
    }

    url = f"{BASE_URL}{request_path}"

    if HAS_REQUESTS:
        # 使用 requests 库
        if method == "GET":
            resp = requests.get(url, headers=headers)
        else:
            resp = requests.post(url, headers=headers, data=body_str)

        if resp.status_code != 200:
            print(f"❌ 请求失败 status={resp.status_code}", file=sys.stderr)
            print(f"响应: {resp.text}", file=sys.stderr)
            sys.exit(1)

        return resp.json()
    else:
        # 使用 urllib（兼容）
        data = body_str.encode() if body_str else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)

        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            print(f"❌ 请求失败 status={e.code}", file=sys.stderr)
            print(f"响应: {e.read().decode()}", file=sys.stderr)
            sys.exit(1)


def load_credentials() -> tuple[str, str, str]:
    """加载 API 凭证"""
    # api_key = os.environ.get("OKX_API_KEY", "").strip()
    # secret = os.environ.get("OKX_API_SECRET", "").strip()
    # passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    api_key = 'c445393e-0013-4388-a8c4-26ad158b4b17'
    secret = 'D173555BA3419757E86B1D994BF34117'
    passphrase = 'Ok@lgq02'

    missing = [name for name, val in [
        ("OKX_API_KEY", api_key),
        ("OKX_API_SECRET", secret),
        ("OKX_PASSPHRASE", passphrase)
    ] if not val]

    if missing:
        print(f"请设置环境变量: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    return api_key, secret, passphrase


def query_collateral_assets(ccy: str = None) -> Dict[str, Any]:
    """查询质押币种配置"""
    api_key, secret, passphrase = load_credentials()
    params = {"ccy": ccy} if ccy else None
    return okx_request("GET", "/api/v5/account/collateral-assets",
                       api_key, secret, passphrase, params=params)


def set_collateral_assets(type_: str, enabled: bool, ccy_list: List[str] = None) -> Dict[str, Any]:
    """设置质押币种"""
    api_key, secret, passphrase = load_credentials()
    body = {"type": type_, "collateralEnabled": enabled}
    if ccy_list and type_ == "custom":
        body["ccyList"] = ccy_list

    return okx_request("POST", "/api/v5/account/set-collateral-assets",
                       api_key, secret, passphrase, body=body)


def query_account_config() -> Dict[str, Any]:
    """查询账户配置"""
    api_key, secret, passphrase = load_credentials()
    return okx_request("GET", "/api/v5/account/config", api_key, secret, passphrase)


def display_collateral_assets(data: Dict[str, Any]) -> None:
    """显示质押币种配置"""
    if data.get("code") != "0":
        print(f"❌ API 错误: {data.get('msg')}", file=sys.stderr)
        return

    assets = data.get("data", [])
    if not assets:
        print("⚠️  没有质押币种配置")
        return

    if HAS_PANDAS:
        df = pd.DataFrame(assets)
        print("\n=== 质押币种配置 ===")
        print(df.to_string(index=False))
        print(f"\n总计: {len(df)} 个币种")

        enabled = df[df["collateralEnabled"] == True].shape[0]
        disabled = df[df["collateralEnabled"] == False].shape[0]
        print(f"已启用: {enabled} 个 | 未启用: {disabled} 个")

        if disabled > 0:
            print("\n⚠️  未启用质押的币种:")
            print(", ".join(df[df["collateralEnabled"] == False]["ccy"].tolist()))
    else:
        print("\n=== 质押币种配置 ===")
        for a in assets:
            icon = "✅" if a.get("collateralEnabled") else "❌"
            print(f"{icon} {a.get('ccy'):8s} collateralEnabled={a.get('collateralEnabled')}")
        print(f"\n总计: {len(assets)} 个")


def display_account_config(data: Dict[str, Any]) -> None:
    """显示账户配置"""
    if data.get("code") != "0":
        print(f"❌ API 错误: {data.get('msg')}", file=sys.stderr)
        return

    config = data.get("data", [{}])[0]

    print("\n=== 账户配置 ===")
    print(f"账户等级 (acctLv):    {config.get('acctLv')}")
    print(f"持仓模式 (posMode):    {config.get('posMode')}")
    print(f"自动借币 (autoLoan):   {config.get('autoLoan')}")
    print(f"希腊字母 (greeksType): {config.get('greeksType')}")
    print("\n等级说明: 1=简单 2=单币种 3=跨币种 4=组合保证金")


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="查询和设置 OKX 质押币种配置",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ccy", help="查询特定币种，多个用逗号分隔")
    parser.add_argument("--account-config", action="store_true", help="查询账户配置")
    parser.add_argument("--set-all", action="store_true", help="设置所有币种")
    parser.add_argument("--set-custom", metavar="CCY_LIST", help="设置自定义币种，逗号分隔")
    parser.add_argument("--enable", action="store_true", help="启用质押")
    parser.add_argument("--disable", action="store_true", help="禁用质押")

    # 过滤 Jupyter 参数
    filtered = [a for a in sys.argv[1:] if not a.startswith('--f=') and not a.startswith('-f=')]
    return parser.parse_args(filtered)


def main() -> None:
    """主函数"""
    args = parse_args()
    api_key, _, _ = load_credentials()

    print(f"🔗 Base URL: {BASE_URL}")
    print(f"🔑 API Key: {api_key[:8]}...{api_key[-4:]}\n")

    # 查询账户配置
    if args.account_config:
        print("📊 查询账户配置...")
        display_account_config(query_account_config())

    # 设置质押币种
    if args.set_all or args.set_custom:
        if not args.enable and not args.disable:
            print("❌ 请指定 --enable 或 --disable", file=sys.stderr)
            sys.exit(1)

        enabled = args.enable

        if args.set_all:
            print(f"⚙️  设置所有币种 collateralEnabled={enabled}...")
            result = set_collateral_assets("all", enabled)
            print(f"✅ 设置成功")
            print(json.dumps(result, indent=2, ensure_ascii=False))

        elif args.set_custom:
            ccy_list = [c.strip().upper() for c in args.set_custom.split(",")]
            print(f"⚙️  设置币种 {ccy_list} collateralEnabled={enabled}...")
            result = set_collateral_assets("custom", enabled, ccy_list)
            print(f"✅ 设置成功")
            print(json.dumps(result, indent=2, ensure_ascii=False))

    # 查询质押币种
    print("\n📊 查询质押币种配置...")
    data = query_collateral_assets(args.ccy)
    display_collateral_assets(data)


# ==================== Jupyter 便捷函数 ====================

def check_collateral(ccy: str = None, account_config: bool = True) -> None:
    """便捷函数：查询质押币种和账户配置

    Args:
        ccy: 可选，查询特定币种，如 "USDT,BTC,ETH"
        account_config: 是否显示账户配置
    """
    api_key, _, _ = load_credentials()
    print(f"🔗 Base URL: {BASE_URL}")
    print(f"🔑 API Key: {api_key[:8]}...{api_key[-4:]}\n")

    if account_config:
        print("📊 查询账户配置...")
        display_account_config(query_account_config())

    print("\n📊 查询质押币种配置...")
    display_collateral_assets(query_collateral_assets(ccy))


def enable_all_collateral() -> None:
    """便捷函数：启用所有币种作为质押"""
    print("⚙️  设置所有币种 collateralEnabled=True...")
    result = set_collateral_assets("all", True)
    print("✅ 设置成功")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print("\n📊 验证设置结果...")
    display_collateral_assets(query_collateral_assets())


def enable_collateral(ccys: str) -> None:
    """便捷函数：启用指定币种作为质押

    Args:
        ccys: 币种列表，用逗号分隔，如 "USDT,BTC,ETH"
    """
    ccy_list = [c.strip().upper() for c in ccys.split(",")]
    print(f"⚙️  设置币种 {ccy_list} collateralEnabled=True...")
    result = set_collateral_assets("custom", True, ccy_list)
    print("✅ 设置成功")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    print("\n📊 验证设置结果...")
    display_collateral_assets(query_collateral_assets(ccys))


if __name__ == "__main__":
    main()
