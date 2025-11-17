#!/usr/bin/env python3
"""Query Binance PM margin spot + UM 合约仓位，一键平仓/卖出。

默认仅打印计划，添加 --execute 后提交市价单：
- 优先调用 /papi/v1/margin/account 与 /papi/v1/um/account；若 404 会 fallback 到 /sapi/v1/margin/account 或 /fapi/v2/account（可用 --no-fallback 禁用）。
- margin netAsset > 0 走 SELL、< 0 走 BUY（忽略与 quote 相同的资产）。
- UM positionAmt > 0 走 SELL reduceOnly，< 0 走 BUY reduceOnly。

依赖环境变量 BINANCE_API_KEY / BINANCE_API_SECRET。
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from typing import Any, Dict, List, Optional, Tuple

from sell_margin_spot import request_papi

DEFAULT_BASE_URL = (
    os.environ.get("BINANCE_PAPI_URL")
    or os.environ.get("BINANCE_FAPI_URL")
    or "https://papi.binance.com"
)
# marginAccount：PAPI -> SAPI；UM account：PAPI -> FAPI v2
DEFAULT_MARGIN_FALLBACK = "https://api.binance.com"
DEFAULT_UM_FALLBACK = "https://fapi.binance.com"
DEFAULT_PAPI_MARGIN_ACCOUNT_PATH = "/papi/v1/margin/account"
DEFAULT_PAPI_UM_ACCOUNT_PATH = "/papi/v1/um/account"
DEFAULT_MARGIN_ACCOUNT_FALLBACK_PATH = "/sapi/v1/margin/account"
DEFAULT_UM_ACCOUNT_FALLBACK_PATH = "/fapi/v2/account"

# 下单路径（若落到 FAPI/SAPI 也会调整）
DEFAULT_MARGIN_ORDER_PATH = "/papi/v1/margin/order"
DEFAULT_UM_ORDER_PATH = "/papi/v1/um/order"
DEFAULT_MARGIN_ORDER_FALLBACK_PATH = "/sapi/v1/margin/order"
DEFAULT_UM_ORDER_FALLBACK_PATH = "/fapi/v1/order"

FALLBACK_STATUSES = {0, 404}

Endpoint = Tuple[str, str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="列出当前 margin spot 资产与 UM 仓位，并可一键平掉（默认 dry-run）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="默认请求域名（margin/UM 都会用，除非下方单独提供 base-url）",
    )
    parser.add_argument(
        "--margin-base-url",
        help="仅对 margin account 请求使用的 base URL，未提供时沿用 --base-url",
    )
    parser.add_argument(
        "--um-base-url",
        help="仅对 UM account 请求使用的 base URL，未提供时沿用 --base-url",
    )
    parser.add_argument(
        "--quote-asset",
        default="USDT",
        help="卖出现货时拼接的报价资产，例如 USDT -> BTCUSDT",
    )
    parser.add_argument(
        "--quantity-precision",
        type=int,
        default=6,
        help="数量精度，ROUND_DOWN 处理，避免因过多小数被拒单",
    )
    parser.add_argument(
        "--min-qty",
        type=Decimal,
        default=Decimal("0"),
        help="低于此数量的订单跳过",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        dest="recv_window",
        help="可选 recvWindow（毫秒）",
    )
    parser.add_argument(
        "--margin-account-path",
        help="自定义 margin account API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--um-account-path",
        help="自定义 UM account API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--margin-order-path",
        help="自定义 margin 下单 API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--um-order-path",
        help="自定义 UM 下单 API path（默认按 base-url 自动推断）",
    )
    parser.add_argument(
        "--skip-margin",
        action="store_true",
        help="仅处理 UM，不提交 margin spot 卖单",
    )
    parser.add_argument(
        "--skip-um",
        action="store_true",
        help="仅处理 margin spot，不提交 UM 平仓单",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="margin spot 下单使用逐仓（默认全仓）",
    )
    parser.add_argument(
        "--side-effect",
        dest="side_effect",
        choices=["AUTO_REPAY", "MARGIN_BUY", "NO_SIDE_EFFECT"],
        default="AUTO_REPAY",
        help="margin spot 下单 sideEffectType，默认 AUTO_REPAY",
    )
    parser.add_argument(
        "--um-qty-precision",
        type=int,
        default=4,
        help="UM 数量精度（ROUND_DOWN），如需更细粒度自行调整",
    )
    parser.add_argument(
        "--no-reduce-only",
        dest="reduce_only",
        action="store_false",
        help="UM 平仓不带 reduceOnly（默认开启 reduceOnly）",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="实际提交订单；默认仅打印计划",
    )
    parser.add_argument(
        "--no-fallback",
        action="store_true",
        help="禁用自动 fallback 到 SAPI/FAPI，当 PAPI 404 时直接报错",
    )
    return parser.parse_args()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("请设置环境变量 BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def format_qty(value: Decimal, precision: int) -> Decimal:
    if precision < 0:
        raise ValueError("precision 必须为非负整数")
    quant = Decimal(1).scaleb(-precision)
    try:
        return value.quantize(quant, rounding=ROUND_DOWN)
    except InvalidOperation:
        steps = (value / quant).to_integral_value(rounding=ROUND_DOWN)
        return steps * quant


def format_str(qty: Decimal) -> str:
    normalized = qty.normalize()
    if normalized == normalized.to_integral():
        normalized = normalized.quantize(Decimal("1"))
    return format(normalized, "f")


@dataclass
class MarginOrder:
    asset: str
    symbol: str
    side: str
    quantity: Decimal


@dataclass
class UmOrder:
    symbol: str
    side: str
    position_side: str
    quantity: Decimal


def fetch_margin_orders(
    base_url: str,
    account_path: str,
    api_key: str,
    api_secret: str,
    quote_asset: str,
    precision: int,
    min_qty: Decimal,
    recv_window: Optional[int],
) -> List[MarginOrder]:
    params: Dict[str, Any] = {}
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, _ = request_papi(base_url.rstrip("/"), account_path, params, api_key, api_secret, method="GET")
    if status != 200:
        raise SystemExit(f"获取 margin 账户失败 status={status} body={body}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"解析 margin 账户响应失败: {exc}") from exc

    assets = data.get("userAssets")
    if not isinstance(assets, list):
        raise SystemExit("margin 响应缺少 userAssets 字段")

    orders: List[MarginOrder] = []
    for entry in assets:
        if not isinstance(entry, dict):
            continue
        asset = str(entry.get("asset", "")).strip().upper()
        if not asset or asset == quote_asset.upper():
            continue
        net_raw = entry.get("netAsset")
        if net_raw is None:
            continue
        try:
            net_qty = Decimal(str(net_raw))
        except InvalidOperation:
            continue
        if net_qty == 0:
            continue
        side = "SELL" if net_qty > 0 else "BUY"
        qty = format_qty(abs(net_qty), precision)
        if qty <= 0 or qty < min_qty:
            continue
        symbol = f"{asset}{quote_asset.upper()}"
        orders.append(MarginOrder(asset=asset, symbol=symbol, side=side, quantity=qty))
    return orders


def fetch_um_orders(
    base_url: str,
    account_path: str,
    api_key: str,
    api_secret: str,
    precision: int,
    min_qty: Decimal,
    recv_window: Optional[int],
) -> List[UmOrder]:
    params: Dict[str, Any] = {}
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, _ = request_papi(base_url.rstrip("/"), account_path, params, api_key, api_secret, method="GET")
    if status != 200:
        raise SystemExit(f"获取 UM 仓位失败 status={status} body={body}")
    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"解析 UM 响应失败: {exc}") from exc

    positions = data.get("positions")
    if not isinstance(positions, list):
        raise SystemExit("UM 响应缺少 positions 字段")

    orders: List[UmOrder] = []
    for entry in positions:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        amt_raw = entry.get("positionAmt")
        if amt_raw is None:
            continue
        try:
            amt = Decimal(str(amt_raw))
        except InvalidOperation:
            continue
        if amt == 0:
            continue
        side = "SELL" if amt > 0 else "BUY"
        qty = format_qty(abs(amt), precision)
        if qty <= 0 or qty < min_qty:
            continue
        pos_side = str(entry.get("positionSide") or ("LONG" if amt > 0 else "SHORT")).upper()
        orders.append(UmOrder(symbol=symbol, side=side, position_side=pos_side, quantity=qty))
    return orders


def submit_margin_order(
    order: MarginOrder,
    base_url: str,
    order_path: str,
    api_key: str,
    api_secret: str,
    recv_window: Optional[int],
    isolated: bool,
    side_effect: Optional[str],
) -> int:
    params: Dict[str, Any] = {
        "symbol": order.symbol,
        "side": order.side,
        "type": "MARKET",
        "quantity": format_str(order.quantity),
    }
    if isolated:
        params["isIsolated"] = "TRUE"
    if side_effect:
        params["sideEffectType"] = side_effect
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, headers = request_papi(base_url.rstrip("/"), order_path, params, api_key, api_secret, method="POST")
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    order_count = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
    print(f"[margin {order.symbol}] status={status} used_weight={weight} order_count={order_count}")
    print(body)
    return status


def submit_um_order(
    order: UmOrder,
    base_url: str,
    order_path: str,
    api_key: str,
    api_secret: str,
    recv_window: Optional[int],
    reduce_only: bool,
) -> int:
    params: Dict[str, Any] = {
        "symbol": order.symbol,
        "side": order.side,
        "type": "MARKET",
        "quantity": format_str(order.quantity),
        "positionSide": order.position_side,
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, headers = request_papi(base_url.rstrip("/"), order_path, params, api_key, api_secret, method="POST")
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    order_count = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
    print(f"[UM {order.symbol}] status={status} used_weight={weight} order_count={order_count}")
    print(body)
    return status


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    base_url = args.base_url.rstrip("/")

    if args.skip_margin and args.skip_um:
        print("skip-margin 与 skip-um 同时开启，无事可做", file=sys.stderr)
        sys.exit(0)

    margin_orders: List[MarginOrder] = []
    um_orders: List[UmOrder] = []

    if not args.skip_margin:
        margin_orders = fetch_margin_orders(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            quote_asset=args.quote_asset,
            precision=args.quantity_precision,
            min_qty=args.min_qty,
            recv_window=args.recv_window,
        )
        if margin_orders:
            print("Margin spot 待执行：")
            for o in margin_orders:
                print(f"  {o.symbol}: side={o.side} qty={format_str(o.quantity)} (asset={o.asset})")
        else:
            print("Margin spot 无需下单")

    if not args.skip_um:
        um_orders = fetch_um_orders(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            precision=args.um_qty_precision,
            min_qty=args.min_qty,
            recv_window=args.recv_window,
        )
        if um_orders:
            print("UM 待平仓：")
            for o in um_orders:
                print(
                    f"  {o.symbol}: side={o.side} positionSide={o.position_side}"
                    f" qty={format_str(o.quantity)}"
                )
        else:
            print("UM 无需平仓")

    if not args.execute:
        print("dry-run：未提交任何订单，添加 --execute 执行")
        return

    failures = 0
    for order in margin_orders:
        status = submit_margin_order(
            order,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            isolated=args.isolated,
            side_effect=args.side_effect,
        )
        if not (200 <= status < 300):
            failures += 1
    for order in um_orders:
        status = submit_um_order(
            order,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            reduce_only=args.reduce_only,
        )
        if not (200 <= status < 300):
            failures += 1

    if failures:
        print(f"有 {failures} 笔订单失败", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
