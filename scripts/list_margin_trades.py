#!/usr/bin/env python3
"""List Binance PM margin spot trade fills for a given symbol."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
from typing import Any, Dict, List

from sell_margin_spot import request_papi  # reuse signing helper


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Query Binance Portfolio Margin trade fills for a symbol",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--symbol", required=True, help="交易对，例如 TWTUSDT")
    parser.add_argument(
        "--base-url",
        default=(
            os.environ.get("BINANCE_PAPI_URL")
            or os.environ.get("BINANCE_FAPI_URL")
            or "https://papi.binance.com"
        ),
        help="REST 基础地址",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="返回条数上限 (1-1000)",
    )
    parser.add_argument(
        "--order-id",
        type=int,
        help="按订单 ID 过滤",
    )
    parser.add_argument(
        "--from-id",
        type=int,
        help="从指定成交 ID 开始翻页",
    )
    parser.add_argument(
        "--start-time",
        type=int,
        help="开始时间 (毫秒时间戳)",
    )
    parser.add_argument(
        "--end-time",
        type=int,
        help="结束时间 (毫秒时间戳)",
    )
    parser.add_argument(
        "--isolated",
        action="store_true",
        help="查询逐仓成交 (默认跨仓)",
    )
    parser.add_argument(
        "--raw",
        action="store_true",
        help="直接输出原始 JSON",
    )
    parser.add_argument(
        "--maker-only",
        action="store_true",
        help="仅展示挂单成交 (isMaker=true)",
    )
    parser.add_argument(
        "--taker-only",
        action="store_true",
        help="仅展示吃单成交 (isMaker=false)",
    )
    return parser.parse_args()


def load_credentials() -> tuple[str, str]:
    api_key = os.environ.get("BINANCE_API_KEY", "").strip()
    api_secret = os.environ.get("BINANCE_API_SECRET", "").strip()
    if not api_key or not api_secret:
        print("请先设置环境变量 BINANCE_API_KEY / BINANCE_API_SECRET", file=sys.stderr)
        sys.exit(1)
    return api_key, api_secret


def ms_to_iso(ts: int) -> str:
    if ts <= 0:
        return "-"
    return dt.datetime.fromtimestamp(ts / 1000, dt.timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def render_table(symbol: str, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        print(f"{symbol}: 未查询到成交记录")
        return

    headers = [
        "tradeId",
        "orderId",
        "clientOrderId",
        "time",
        "side",
        "price",
        "qty",
        "quoteQty",
        "commission",
        "commissionAsset",
        "isMaker",
    ]

    widths = {h: len(h) for h in headers}
    table_rows: List[List[str]] = []
    for row in rows:
        side = "BUY" if row.get("isBuyer") else "SELL"
        values = [
            str(row.get("id")),
            str(row.get("orderId")),
            row.get("clientOrderId", ""),
            ms_to_iso(int(row.get("time", 0))),
            side,
            row.get("price", ""),
            row.get("qty", ""),
            row.get("quoteQty", ""),
            row.get("commission", ""),
            row.get("commissionAsset", ""),
            "Y" if row.get("isMaker") else "N",
        ]
        table_rows.append(values)
        for header, value in zip(headers, values):
            widths[header] = max(widths[header], len(value))

    def fmt_row(row: List[str]) -> str:
        parts = [row[i].ljust(widths[header]) for i, header in enumerate(headers)]
        return " | ".join(parts)

    header_line = fmt_row(headers)
    sep_line = "-+-".join("-" * widths[header] for header in headers)
    print(header_line)
    print(sep_line)
    for row in table_rows:
        print(fmt_row(row))


def main() -> None:
    args = parse_args()

    if args.maker_only and args.taker_only:
        print("--maker-only 与 --taker-only 不能同时使用", file=sys.stderr)
        sys.exit(1)

    api_key, api_secret = load_credentials()

    params: Dict[str, Any] = {
        "symbol": args.symbol.upper().strip(),
        "limit": max(1, min(args.limit, 1000)) if args.limit else 100,
    }
    if args.order_id is not None:
        params["orderId"] = str(args.order_id)
    if args.from_id is not None:
        params["fromId"] = str(args.from_id)
    if args.start_time is not None:
        params["startTime"] = str(args.start_time)
    if args.end_time is not None:
        params["endTime"] = str(args.end_time)
    if args.isolated:
        params["isIsolated"] = "TRUE"

    status, body, headers = request_papi(
        args.base_url.rstrip("/"),
        "/papi/v1/margin/myTrades",
        params,
        api_key,
        api_secret,
        method="GET",
    )

    weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
    tag = "OK" if 200 <= status < 300 else "ERR"
    parsed: List[Dict[str, Any]] | None = None
    if status == 200:
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            parsed = None
    print(
        f"Result: {tag} {status}; used_weight={weight}; count={len(parsed) if parsed is not None else 0}"
    )

    if args.raw or status != 200 or parsed is None:
        if parsed is not None:
            print(json.dumps(parsed, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(body)
        if status != 200:
            sys.exit(1)
        return

    filtered = parsed
    if args.maker_only:
        filtered = [row for row in parsed if row.get("isMaker")]
    elif args.taker_only:
        filtered = [row for row in parsed if not row.get("isMaker")]

    render_table(params["symbol"], filtered)


if __name__ == "__main__":
    main()
