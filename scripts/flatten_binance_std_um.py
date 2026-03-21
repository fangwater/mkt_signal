#!/usr/bin/env python3
"""Flatten all Binance standard UM futures positions with taker market orders.

默认仅打印计划，添加 --execute 后才会实际下单。
依赖环境变量 BINANCE_API_KEY / BINANCE_API_SECRET。
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Iterable, List, Optional

from sell_margin_spot import request_papi

DEFAULT_BASE_URL = os.environ.get("BINANCE_FAPI_URL") or "https://fapi.binance.com"
ACCOUNT_PATH = "/fapi/v2/account"
ORDER_PATH = "/fapi/v1/order"


@dataclass
class UmPosition:
    symbol: str
    position_side: str
    quantity: Decimal

    @property
    def close_side(self) -> str:
        return "SELL" if self.quantity > 0 else "BUY"

    @property
    def close_quantity(self) -> Decimal:
        return abs(self.quantity)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="按 Binance 标准 UM 合约当前持仓，使用 taker 市价单全部平仓（默认 dry-run）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Binance FAPI base URL",
    )
    parser.add_argument(
        "--recv-window",
        type=int,
        dest="recv_window",
        default=5000,
        help="recvWindow in milliseconds",
    )
    parser.add_argument(
        "--symbols",
        help="仅处理指定 symbol，逗号或空格分隔，例如 BTCUSDT,ETHUSDT",
    )
    parser.add_argument(
        "--min-qty",
        default="0",
        help="小于该数量的仓位跳过",
    )
    parser.add_argument(
        "--reduce-only-mode",
        choices=["auto", "always", "never"],
        default="auto",
        help="auto: BOTH 仓位带 reduceOnly，LONG/SHORT 自动不带；always: 全部带；never: 全部不带",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="实际提交订单；默认仅打印计划",
    )
    return parser.parse_args()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("ERROR: 请先设置 BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def parse_decimal(value: Any, *, field: str) -> Decimal:
    try:
        return Decimal(str(value))
    except InvalidOperation as exc:
        raise SystemExit(f"无法解析 {field}: {value!r}") from exc


def parse_symbol_filter(raw: Optional[str]) -> Optional[set[str]]:
    if not raw:
        return None
    items = [item.strip().upper() for item in re.split(r"[,\s]+", raw) if item.strip()]
    return set(items) if items else None


def format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        normalized = normalized.quantize(Decimal("1"))
    return format(normalized, "f")


def fetch_positions(
    base_url: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
) -> List[UmPosition]:
    params: Dict[str, Any] = {"recvWindow": str(recv_window)}
    status, body, _headers = request_papi(
        base_url.rstrip("/"),
        ACCOUNT_PATH,
        params,
        api_key,
        api_secret,
        method="GET",
    )
    if status != 200:
        raise SystemExit(f"获取 UM 账户失败 status={status} body={body}")

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise SystemExit(f"解析 UM 账户响应失败: {exc}") from exc

    raw_positions = data.get("positions")
    if not isinstance(raw_positions, list):
        raise SystemExit("UM 响应缺少 positions 字段")

    results: List[UmPosition] = []
    for entry in raw_positions:
        if not isinstance(entry, dict):
            continue
        symbol = str(entry.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        amt_raw = entry.get("positionAmt")
        if amt_raw is None:
            continue
        qty = parse_decimal(amt_raw, field=f"{symbol}.positionAmt")
        if qty == 0:
            continue
        position_side = str(entry.get("positionSide") or "BOTH").strip().upper() or "BOTH"
        results.append(UmPosition(symbol=symbol, position_side=position_side, quantity=qty))
    return results


def should_send_reduce_only(position_side: str, mode: str) -> bool:
    if mode == "always":
        return True
    if mode == "never":
        return False
    return position_side == "BOTH"


def filter_positions(
    positions: Iterable[UmPosition],
    symbol_filter: Optional[set[str]],
    min_qty: Decimal,
) -> List[UmPosition]:
    selected: List[UmPosition] = []
    for pos in positions:
        if symbol_filter is not None and pos.symbol not in symbol_filter:
            continue
        if pos.close_quantity < min_qty:
            continue
        selected.append(pos)
    return selected


def submit_order(
    base_url: str,
    api_key: str,
    api_secret: str,
    recv_window: int,
    position: UmPosition,
    reduce_only_mode: str,
) -> int:
    params: Dict[str, Any] = {
        "symbol": position.symbol,
        "side": position.close_side,
        "type": "MARKET",
        "quantity": format_decimal(position.close_quantity),
        "recvWindow": str(recv_window),
    }
    if position.position_side:
        params["positionSide"] = position.position_side
    if should_send_reduce_only(position.position_side, reduce_only_mode):
        params["reduceOnly"] = "true"

    status, body, headers = request_papi(
        base_url.rstrip("/"),
        ORDER_PATH,
        params,
        api_key,
        api_secret,
        method="POST",
    )
    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    order_count = headers.get("x-mbx-order-count-1m") or headers.get("x-mbx-order-count")
    print(
        f"[{position.symbol}] status={status} used_weight={weight} "
        f"order_count={order_count} side={position.close_side} "
        f"qty={format_decimal(position.close_quantity)} positionSide={position.position_side}"
    )
    try:
        print(json.dumps(json.loads(body), ensure_ascii=False, indent=2, sort_keys=True))
    except json.JSONDecodeError:
        print(body)
    return status


def main() -> None:
    args = parse_args()
    api_key, api_secret = load_credentials()
    min_qty = parse_decimal(args.min_qty, field="min_qty")
    symbol_filter = parse_symbol_filter(args.symbols)
    base_url = args.base_url.rstrip("/")

    positions = fetch_positions(base_url, api_key, api_secret, args.recv_window)
    positions = filter_positions(positions, symbol_filter, min_qty)

    if symbol_filter:
        print(f"symbols filter: {', '.join(sorted(symbol_filter))}")
    if not positions:
        print("未找到需要平仓的持仓")
        return

    print(f"base_url={base_url}")
    print(f"reduce_only_mode={args.reduce_only_mode}")
    print(f"positions_to_close={len(positions)}")
    for pos in positions:
        reduce_only = should_send_reduce_only(pos.position_side, args.reduce_only_mode)
        print(
            f"- {pos.symbol}: pos={format_decimal(pos.quantity)} "
            f"-> {pos.close_side} {format_decimal(pos.close_quantity)} "
            f"positionSide={pos.position_side} reduceOnly={str(reduce_only).lower()}"
        )

    if not args.execute:
        print("dry-run：未提交任何订单，添加 --execute 执行")
        return

    failures = 0
    for pos in positions:
        status = submit_order(
            base_url=base_url,
            api_key=api_key,
            api_secret=api_secret,
            recv_window=args.recv_window,
            position=pos,
            reduce_only_mode=args.reduce_only_mode,
        )
        if not (200 <= status < 300):
            failures += 1

    if failures:
        print(f"有 {failures} 笔平仓单失败", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
