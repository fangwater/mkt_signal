#!/usr/bin/env python3
"""Sell Binance STANDARD spot quantity that exceeds the absolute UM position.

For every required symbol, the delegated plan is:
  sell_qty = max(spot_qty - abs(um_net_qty), 0)

The script never submits UM orders. It is dry-run by default; add --execute to
submit the planned STANDARD spot market sell order.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Optional


SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]+USDT$")


def normalize_symbols(raw_symbols: Optional[str], repeated: Iterable[str]) -> list[str]:
    values: list[str] = []
    if raw_symbols:
        values.extend(part.strip() for part in re.split(r"[,\s]+", raw_symbols) if part.strip())
    values.extend(part.strip() for part in repeated if part and part.strip())

    symbols: list[str] = []
    seen: set[str] = set()
    for raw in values:
        symbol = raw.upper()
        if not SYMBOL_PATTERN.fullmatch(symbol):
            raise SystemExit(f"Binance UM symbol 必须是完整 USDT 交易对，例如 BTCUSDT；实际: {raw}")
        if symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
    if not symbols:
        raise SystemExit("至少需要一个 --symbol，禁止无过滤地处理全部现货")
    return symbols


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="仅卖出超过 abs(UM 合约净仓) 的 Binance STANDARD 现货（默认 dry-run）",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--symbols",
        help="逗号或空格分隔的完整 USDT 交易对，例如 BTCUSDT,ETHUSDT",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        default=[],
        help="完整 USDT 交易对，可重复传入，例如 --symbol BTCUSDT",
    )
    parser.add_argument(
        "--quantity-precision",
        type=int,
        default=6,
        help="现货数量精度；最终还会按 exchangeInfo stepSize 向下取整",
    )
    parser.add_argument(
        "--min-qty",
        type=Decimal,
        default=Decimal("0"),
        help="低于此数量的卖单跳过",
    )
    parser.add_argument("--recv-window", type=int, help="可选 recvWindow（毫秒）")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="实际提交 STANDARD spot 市价卖单；默认只打印计划",
    )
    args = parser.parse_args(argv)
    args.normalized_symbols = normalize_symbols(args.symbols, args.symbol)
    return args


def build_command(
    args: argparse.Namespace,
    *,
    script_dir: Optional[Path] = None,
    python_bin: Optional[str] = None,
) -> list[str]:
    scripts = script_dir or Path(__file__).resolve().parent
    cmd = [python_bin or sys.executable or "python3", str(scripts / "flatten_binance_std.py")]
    for symbol in args.normalized_symbols:
        cmd.extend(["--symbol", symbol])
    cmd.extend(
        [
            "--mode",
            "spot-excess",
            "--quantity-precision",
            str(args.quantity_precision),
            "--min-qty",
            str(args.min_qty),
        ]
    )
    if args.recv_window is not None:
        cmd.extend(["--recv-window", str(args.recv_window)])
    if args.execute:
        cmd.append("--execute")
    return cmd


def main() -> None:
    args = parse_args()
    cmd = build_command(args)
    print(
        "[info] mode=spot-excess: only SELL max(spot - abs(UM), 0); "
        "UM orders are disabled"
    )
    print("[RUN] " + " ".join(cmd))
    raise SystemExit(subprocess.run(cmd, check=False).returncode)


if __name__ == "__main__":
    main()
