#!/usr/bin/env python3
"""根据敞口表自动卖出多余 spot，复用 Binance PM 下单逻辑。"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import sell_margin_spot


@dataclass
class ExposureRow:
    asset: str
    spot_qty: Decimal
    um_net_qty: Decimal
    exposure_qty: Decimal


@dataclass
class SellOrder:
    asset: str
    symbol: str
    quantity: Decimal


def parse_symbol_decimal_map(items: Optional[List[str]]) -> Dict[str, Decimal]:
    mapping: Dict[str, Decimal] = {}
    if not items:
        return mapping
    for raw in items:
        if "=" not in raw:
            raise SystemExit(f"无效的参数格式: {raw}，需使用 SYMBOL=value")
        symbol, value = raw.split("=", 1)
        symbol = symbol.strip().upper()
        value = value.strip()
        try:
            mapping[symbol] = Decimal(value)
        except InvalidOperation as exc:
            raise SystemExit(f"解析 {raw} 失败: {exc}") from exc
    return mapping


def parse_decimal(raw: str) -> Optional[Decimal]:
    value = raw.strip().replace(",", "")
    if not value or value == "-":
        return None
    try:
        return Decimal(value)
    except InvalidOperation:
        return None


def parse_table(text: str) -> List[ExposureRow]:
    rows: List[ExposureRow] = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("+"):
            continue
        if "Asset" in line and "SpotQty" in line:
            continue
        if line.startswith("-"):
            continue
        if "TOTAL" in line.upper():
            continue
        parts = [col.strip() for col in line.split("|") if col.strip()]
        if len(parts) < 7:
            continue
        asset = parts[0].upper()
        if asset == "ASSET":
            continue
        spot_qty = parse_decimal(parts[1])
        um_net_qty = parse_decimal(parts[3])
        exposure_qty = parse_decimal(parts[5])
        if spot_qty is None or um_net_qty is None:
            continue
        if exposure_qty is None:
            exposure_qty = spot_qty + um_net_qty
        rows.append(
            ExposureRow(
                asset=asset,
                spot_qty=spot_qty,
                um_net_qty=um_net_qty,
                exposure_qty=exposure_qty,
            )
        )
    return rows


def determine_orders(
    exposures: Iterable[ExposureRow],
    quote_asset: str,
    min_qty: Decimal,
) -> List[SellOrder]:
    orders: List[SellOrder] = []
    for entry in exposures:
        qty = entry.exposure_qty
        if qty <= Decimal("0"):
            continue
        if qty < min_qty:
            continue
        symbol = f"{entry.asset}{quote_asset.upper()}"
        orders.append(
            SellOrder(
                asset=entry.asset,
                symbol=symbol,
                quantity=qty,
            )
        )
    return orders


def adjust_quantity(
    symbol: str,
    quantity: Decimal,
    precision: Optional[int],
    lot_steps: Dict[str, Decimal],
    min_qtys: Dict[str, Decimal],
) -> Decimal:
    symbol = symbol.upper()
    result = quantity
    step = lot_steps.get(symbol)
    if step is not None:
        try:
            result = result.quantize(step, rounding=ROUND_DOWN)
        except InvalidOperation:
            steps = (result / step).to_integral_value(rounding=ROUND_DOWN)
            result = steps * step
    elif precision is not None:
        if precision < 0:
            raise ValueError("precision 必须是非负整数")
        quant = Decimal(1).scaleb(-precision)
        result = result.quantize(quant, rounding=ROUND_DOWN)
    min_qty = min_qtys.get(symbol)
    if min_qty is not None and result < min_qty:
        return Decimal("0")
    if result < 0:
        return Decimal("0")
    return result.normalize()


def format_quantity(quantity: Decimal) -> str:
    q = quantity.normalize()
    if q == q.to_integral():
        q = q.quantize(Decimal("1"))
    return format(q, "f")


def submit_order(
    order: SellOrder,
    quantity_str: str,
    base_url: str,
    api_key: str,
    api_secret: str,
    recv_window: Optional[int],
    isolated: bool,
    side_effect: Optional[str],
) -> int:
    params = {
        "symbol": order.symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": quantity_str,
    }
    if isolated:
        params["isIsolated"] = "TRUE"
    if side_effect:
        params["sideEffectType"] = side_effect
    if recv_window is not None:
        params["recvWindow"] = str(recv_window)
    status, body, headers = sell_margin_spot.request_papi(
        base_url,
        "/papi/v1/margin/order",
        params,
        api_key,
        api_secret,
        method="POST",
    )
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    order_count = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
    print(
        f"[{order.symbol}] status={status} used_weight={weight} order_count={order_count}\n{body}"
    )
    return status


def read_source(args: argparse.Namespace) -> str:
    if args.source_file:
        return Path(args.source_file).read_text(encoding="utf-8")
    if sys.stdin.isatty():
        raise SystemExit("请通过管道或 --source-file 提供敞口表文本")
    return sys.stdin.read()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="读取 pre_trade 敞口表，卖出多余 spot，保持 qty 敞口为 0",
    )
    parser.add_argument(
        "--source-file",
        type=str,
        help="包含敞口表的文本文件；若未提供则从 STDIN 读取",
    )
    parser.add_argument(
        "--base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="Binance PM REST 地址，默认 https://papi.binance.com",
    )
    parser.add_argument(
        "--quote-asset",
        default="USDT",
        help="symbol 后缀，默认 USDT",
    )
    parser.add_argument(
        "--min-qty",
        type=Decimal,
        default=Decimal("0"),
        help="最小下单数量，低于此值的敞口忽略",
    )
    parser.add_argument(
        "--quantity-precision",
        type=int,
        default=2,
        help="数量精度，使用 ROUND_DOWN 处理，默认 2 位小数",
    )
    parser.add_argument(
        "--lot-step",
        action="append",
        default=[],
        help="指定每个 symbol 的数量步长，例如 AIUSDT=1 或 TWTUSDT=0.1，可重复使用",
    )
    parser.add_argument(
        "--min-order-qty",
        action="append",
        default=[],
        help="指定最小下单量，例如 AIUSDT=1，可重复使用",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="实际提交订单；默认只打印计划",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="是否使用逐仓 (默认全仓)",
    )
    parser.add_argument(
        "--side-effect",
        dest="side_effect",
        choices=["AUTO_REPAY", "MARGIN_BUY", "NO_SIDE_EFFECT"],
        default=None,
        help="sideEffectType，可选 AUTO_REPAY 等",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        dest="recv_window",
        help="recvWindow 参数 (毫秒)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    text = read_source(args)
    exposures = parse_table(text)
    if not exposures:
        print("未解析到有效敞口行", file=sys.stderr)
        sys.exit(1)

    orders = determine_orders(exposures, args.quote_asset, args.min_qty)
    if not orders:
        print("没有需要卖出的敞口，退出")
        return

    lot_steps = parse_symbol_decimal_map(args.lot_step)
    min_qtys = parse_symbol_decimal_map(args.min_order_qty)

    prepared: List[Tuple[SellOrder, Decimal, str]] = []
    print("待平仓订单：")
    for order in orders:
        adjusted_qty = adjust_quantity(
            order.symbol,
            order.quantity,
            args.quantity_precision,
            lot_steps,
            min_qtys,
        )
        qty_str = format_quantity(adjusted_qty)
        print(
            "  asset="
            f"{order.asset} symbol={order.symbol} quantity={qty_str} "
            f"(原始 {order.quantity})"
        )
        if adjusted_qty > 0:
            prepared.append((order, adjusted_qty, qty_str))
        else:
            print("    -> 调整后数量小于最小下单量，跳过该资产")

    if not prepared:
        print("没有满足条件的订单，退出")
        return

    if not args.execute:
        print("dry-run: 未提交任何订单，添加 --execute 执行")
        return

    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("请在环境变量中设置 BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)

    base_url = args.base_url.rstrip("/")
    failures = 0
    for order, _, quantity_str in prepared:
        status = submit_order(
            order,
            quantity_str=quantity_str,
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            isolated=args.isolated,
            side_effect=args.side_effect,
        )
        if not (200 <= status < 300):
            failures += 1
    if failures:
        print(f"有 {failures} 笔订单失败", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
