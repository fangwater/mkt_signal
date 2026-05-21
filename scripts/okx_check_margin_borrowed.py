#!/usr/bin/env python3
"""查询 OKX 币币杠杆借币信息

用法：
  python scripts/okx_check_margin_borrowed.py
  python scripts/okx_check_margin_borrowed.py --verbose  # 显示完整的 API 响应

显示：
  1. 账户余额中的借币信息（/api/v5/account/balance）
  2. 币币杠杆持仓（/api/v5/account/positions with instType=MARGIN）
  3. SWAP 持仓（/api/v5/account/positions with instType=SWAP）- 可能包含借币
  4. 未还利息记录（/api/v5/account/interest-accrued）- 主要的借币记录来源
"""

import argparse
import base64
import hashlib
import hmac
import json
import os
import sys
from datetime import datetime, timezone
from typing import Any, Dict

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False
    import urllib.request
    import urllib.error

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
        if method == "GET":
            resp = requests.get(url, headers=headers)
        else:
            resp = requests.post(url, headers=headers, data=body_str)

        if resp.status_code != 200:
            print(f"❌ 请求失败 status={resp.status_code}", file=sys.stderr)
            print(f"响应: {resp.text}", file=sys.stderr)
            return {"code": "-1", "msg": f"HTTP {resp.status_code}"}

        return resp.json()
    else:
        data = body_str.encode() if body_str else None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)

        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            print(f"❌ 请求失败 status={e.code}", file=sys.stderr)
            print(f"响应: {e.read().decode()}", file=sys.stderr)
            return {"code": "-1", "msg": f"HTTP {e.code}"}


def load_credentials() -> tuple[str, str, str]:
    """加载 API 凭证"""
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    secret = os.environ.get("OKX_API_SECRET", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()

    # 如果环境变量未设置，使用硬编码的凭证（仅用于测试）
    if not api_key:
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


def query_balance(ccy: str = None) -> Dict[str, Any]:
    """查询账户余额"""
    api_key, secret, passphrase = load_credentials()
    params = {"ccy": ccy} if ccy else None
    return okx_request("GET", "/api/v5/account/balance",
                       api_key, secret, passphrase, params=params)


def query_positions(inst_type: str = "MARGIN") -> Dict[str, Any]:
    """查询持仓信息"""
    api_key, secret, passphrase = load_credentials()
    params = {"instType": inst_type}
    return okx_request("GET", "/api/v5/account/positions",
                       api_key, secret, passphrase, params=params)


def query_interest_accrued(inst_id: str = None, ccy: str = None,
                          mgnMode: str = None, after: str = None,
                          before: str = None, limit: str = "100") -> Dict[str, Any]:
    """查询未还利息记录"""
    api_key, secret, passphrase = load_credentials()
    params = {}
    if inst_id:
        params["instId"] = inst_id
    if ccy:
        params["ccy"] = ccy
    if mgnMode:
        params["mgnMode"] = mgnMode
    if after:
        params["after"] = after
    if before:
        params["before"] = before
    if limit:
        params["limit"] = limit

    return okx_request("GET", "/api/v5/account/interest-accrued",
                       api_key, secret, passphrase, params=params)


def display_balance_borrowed(data: Dict[str, Any], verbose: bool = False) -> None:
    """显示账户余额中的借币信息"""
    if data.get("code") != "0":
        print(f"❌ API 错误: {data.get('msg')}", file=sys.stderr)
        return

    account_data = data.get("data", [])
    if not account_data:
        print("⚠️  没有账户数据")
        return

    print("\n" + "=" * 80)
    print("📊 账户余额中的借币信息 (GET /api/v5/account/balance)")
    print("=" * 80)

    if verbose:
        print("\n完整响应:")
        print(json.dumps(data, indent=2, ensure_ascii=False))

    for account in account_data:
        details = account.get("details", [])
        has_borrowed = False

        # 先查找所有有借币的币种
        borrowed_details = []
        for detail in details:
            # OKX 使用 crossLiab, isoLiab, liab 来表示负债
            cross_liab = float(detail.get("crossLiab", "0") or "0")
            iso_liab = float(detail.get("isoLiab", "0") or "0")
            liab = float(detail.get("liab", "0") or "0")
            interest = float(detail.get("interest", "0") or "0")

            if cross_liab > 0 or iso_liab > 0 or liab > 0 or interest > 0:
                borrowed_details.append((detail, cross_liab, iso_liab, liab, interest))

        if borrowed_details:
            print(f"\n账户等级: {account.get('acctLv')} | 账户模式: {account.get('mgnMode', 'N/A')}")
            print(f"总权益: {account.get('totalEq')} USDT")

            for detail, cross_liab, iso_liab, liab, interest in borrowed_details:
                ccy = detail.get("ccy")
                avail_bal = detail.get("availBal", "0")
                cash_bal = detail.get("cashBal", "0")
                eq = detail.get("eq", "0")
                frozen_bal = detail.get("frozenBal", "0")
                spot_in_use = detail.get("spotInUseAmt", "0")
                borrow_froz = detail.get("borrowFroz", "0")

                print(f"\n  💰 币种: {ccy}")
                print(f"     ├─ 负债信息:")
                if cross_liab > 0:
                    print(f"     │  ├─ 全仓负债 (crossLiab):  {cross_liab:>20.10f}")
                if iso_liab > 0:
                    print(f"     │  ├─ 逐仓负债 (isoLiab):    {iso_liab:>20.10f}")
                if liab > 0:
                    print(f"     │  ├─ 总负债 (liab):         {liab:>20.10f}")
                if interest > 0:
                    print(f"     │  └─ 未还利息 (interest):   {interest:>20.10f}")

                print(f"     ├─ 余额信息:")
                print(f"     │  ├─ 现金余额 (cashBal):    {cash_bal:>20}")
                print(f"     │  ├─ 可用余额 (availBal):   {avail_bal:>20}")
                print(f"     │  ├─ 冻结余额 (frozenBal):  {frozen_bal:>20}")
                print(f"     │  └─ 总权益   (eq):         {eq:>20}")

                print(f"     └─ 其他信息:")
                print(f"        ├─ 现货占用 (spotInUseAmt): {spot_in_use}")
                print(f"        └─ 借币冻结 (borrowFroz):   {borrow_froz}")

        # 显示 SOL 的完整信息（即使没有借币）
        if verbose:
            for detail in details:
                if detail.get("ccy") in ["SOL", "USDT", "BTC", "ETH"]:
                    print(f"\n  [详细] {detail.get('ccy')}:")
                    print(json.dumps(detail, indent=4, ensure_ascii=False))

        if not borrowed_details:
            print("\n  ✅ 该账户在 balance API 中没有显示借币")
            print("     （但可能在 interest-accrued 中有记录）")


def display_positions(data: Dict[str, Any], inst_type: str = "MARGIN", verbose: bool = False) -> None:
    """显示持仓信息（重点关注 Margin）"""
    if data.get("code") != "0":
        print(f"❌ API 错误: {data.get('msg')}", file=sys.stderr)
        return

    positions = data.get("data", [])

    print("\n" + "=" * 80)
    print(f"📊 持仓信息 (GET /api/v5/account/positions?instType={inst_type})")
    print("=" * 80)

    if verbose:
        print("\n完整响应:")
        print(json.dumps(data, indent=2, ensure_ascii=False))

    if not positions:
        print(f"\n  ⚠️  没有 {inst_type} 类型的持仓")
        return

    for pos in positions:
        inst_id = pos.get("instId")
        inst_type_val = pos.get("instType")
        mgn_mode = pos.get("mgnMode")

        # 借币相关字段
        liab = float(pos.get("liab", "0") or "0")
        liab_ccy = pos.get("liabCcy", "")
        base_borrowed = pos.get("baseBorrowed", "")
        quote_borrowed = pos.get("quoteBorrowed", "")
        base_interest = pos.get("baseInterest", "")
        quote_interest = pos.get("quoteInterest", "")

        print(f"\n  📈 交易对: {inst_id} ({inst_type_val}, {mgn_mode})")
        print(f"     持仓数量 (pos):         {pos.get('pos', '0')}")
        print(f"     持仓方向 (posSide):     {pos.get('posSide', '')}")
        print(f"     持仓币种 (posCcy):      {pos.get('posCcy', '')}")

        if liab > 0:
            print(f"     💸 负债金额 (liab):     {liab:>20.8f} {liab_ccy}")

        if base_borrowed:
            print(f"     💸 base 借币:           {base_borrowed}")
        if quote_borrowed:
            print(f"     💸 quote 借币:          {quote_borrowed}")
        if base_interest:
            print(f"     💸 base 利息:           {base_interest}")
        if quote_interest:
            print(f"     💸 quote 利息:          {quote_interest}")

        print(f"     未实现盈亏 (upl):       {pos.get('upl', '0')}")
        print(f"     已实现盈亏:             {pos.get('realizedPnl', '0')}")

        # 如果有借币信息，显示更多细节
        if verbose and (liab > 0 or base_borrowed or quote_borrowed):
            print(f"\n     [详细信息]:")
            print(json.dumps(pos, indent=6, ensure_ascii=False))


def display_interest_accrued(data: Dict[str, Any], verbose: bool = False) -> None:
    """显示未还利息记录"""
    if data.get("code") != "0":
        print(f"❌ API 错误: {data.get('msg')}", file=sys.stderr)
        return

    records = data.get("data", [])

    print("\n" + "=" * 80)
    print("📊 未还利息记录 (GET /api/v5/account/interest-accrued)")
    print("=" * 80)

    if verbose:
        print("\n完整响应:")
        print(json.dumps(data, indent=2, ensure_ascii=False))

    if not records:
        print("\n  ✅ 没有未还利息记录")
        return

    total_interest = {}
    total_liab = {}

    # 按币种分组
    by_ccy = {}
    for record in records:
        ccy = record.get("ccy", "UNKNOWN")
        if ccy not in by_ccy:
            by_ccy[ccy] = []
        by_ccy[ccy].append(record)

    for ccy, ccy_records in sorted(by_ccy.items()):
        print(f"\n  💸 币种: {ccy}")

        latest_liab = 0
        total_ccy_interest = 0

        for record in ccy_records[-5:]:  # 只显示最近5条
            inst_id = record.get("instId", "")
            interest = float(record.get("interest", "0") or "0")
            interest_rate = float(record.get("interestRate", "0") or "0")
            liab = float(record.get("liab", "0") or "0")
            mgn_mode = record.get("mgnMode", "")
            ts = record.get("ts", "")

            # 转换时间戳
            if ts:
                dt = datetime.fromtimestamp(int(ts)/1000, tz=timezone.utc)
                ts_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            else:
                ts_str = ""

            print(f"     [{ts_str}] 负债: {liab:>15.8f} | 利息: {interest:>15.8f} | 利率: {interest_rate*100:.8f}%")

            latest_liab = liab
            total_ccy_interest += interest

        print(f"     ---")
        print(f"     当前负债: {latest_liab:>15.8f} {ccy}")
        print(f"     累计利息: {total_ccy_interest:>15.8f} {ccy} (最近{len(ccy_records)}条记录)")

        if ccy not in total_interest:
            total_interest[ccy] = 0
            total_liab[ccy] = 0
        total_interest[ccy] += total_ccy_interest
        total_liab[ccy] = latest_liab

    if total_interest:
        print("\n" + "-" * 80)
        print("📌 总计:")
        for ccy in sorted(total_interest.keys()):
            print(f"  {ccy:8s}: 负债 {total_liab[ccy]:>15.8f} | 累计利息 {total_interest[ccy]:>15.8f}")


def parse_args() -> argparse.Namespace:
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description="查询 OKX 币币杠杆借币信息",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="显示完整的 API 响应")
    return parser.parse_args()


def main() -> None:
    """主函数"""
    args = parse_args()
    api_key, _, _ = load_credentials()

    print(f"🔗 Base URL: {BASE_URL}")
    print(f"🔑 API Key: {api_key[:8]}...{api_key[-4:]}")

    # 1. 查询账户余额中的借币信息
    print("\n📡 正在查询账户余额...")
    balance_data = query_balance()
    display_balance_borrowed(balance_data, verbose=args.verbose)

    # 2. 查询币币杠杆持仓
    print("\n📡 正在查询币币杠杆持仓 (MARGIN)...")
    positions_data = query_positions(inst_type="MARGIN")
    display_positions(positions_data, inst_type="MARGIN", verbose=args.verbose)

    # 3. 查询 SWAP 持仓（可能包含借币信息）
    print("\n📡 正在查询 SWAP 持仓...")
    swap_positions_data = query_positions(inst_type="SWAP")
    display_positions(swap_positions_data, inst_type="SWAP", verbose=args.verbose)

    # 4. 查询未还利息记录
    print("\n📡 正在查询未还利息记录...")
    interest_data = query_interest_accrued()
    display_interest_accrued(interest_data, verbose=args.verbose)

    print("\n" + "=" * 80)
    print("✅ 查询完成")
    print("=" * 80)


if __name__ == "__main__":
    main()
