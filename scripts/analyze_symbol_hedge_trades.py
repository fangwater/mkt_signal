#!/usr/bin/env python3
import argparse
from pathlib import Path

import pandas as pd


def normalize_venue(value: str | None) -> str:
    if value is None:
        return ""
    s = str(value).lower()
    for ch in "-_ ":
        s = s.replace(ch, "")
    return s


def normalize_trade_time_ms(trade_time: pd.Series) -> pd.Series:
    # Some venues export trade_time in ms (13 digits), others in us (16 digits).
    trade_time = trade_time.astype("int64")
    return trade_time.where(trade_time < 10**14, trade_time // 1000)


def extract_strategy_id(client_order_id: pd.Series) -> pd.Series:
    # client_order_id encodes strategy_id in the high 32 bits.
    return client_order_id.astype("int64") // (1 << 32)


def load_trades(trade_path: Path, symbol: str, venue_norm: str) -> pd.DataFrame:
    trades = pd.read_parquet(
        trade_path,
        columns=[
            "symbol",
            "trading_venue",
            "client_order_id",
            "quantity",
            "price",
            "trade_time",
        ],
    )
    trades = trades[trades["symbol"] == symbol].copy()
    trades = trades.dropna(subset=["client_order_id"])
    trades["client_order_id"] = trades["client_order_id"].astype("int64")
    trades["venue_norm"] = trades["trading_venue"].map(normalize_venue)
    trades = trades[trades["venue_norm"] == venue_norm]
    trades["strategy_id"] = extract_strategy_id(trades["client_order_id"])
    trades["trade_time_ms"] = normalize_trade_time_ms(trades["trade_time"])
    return trades


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Summarize hedge-side trade quantities by strategy_id for a symbol "
            "using trade_updates + signals_arb_hedge."
        )
    )
    parser.add_argument("--data-dir", default="output", help="Parquet directory")
    parser.add_argument("--symbol", default="DASHUSDT", help="Symbol, e.g. DASHUSDT")
    parser.add_argument(
        "--venue",
        default="binance-futures",
        help="Trading venue, e.g. binance-futures",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Rows to print (<=0 prints all)",
    )
    parser.add_argument(
        "--lookback-hours",
        type=float,
        default=48.0,
        help="Only include trades within the last N hours (<=0 disables)",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    trade_path = data_dir / "trade_updates.parquet"
    if not trade_path.exists():
        print(f"[ERROR] missing trade_updates: {trade_path}")
        return 1

    venue_norm = normalize_venue(args.venue)
    trades = load_trades(trade_path, args.symbol, venue_norm)
    if trades.empty:
        print(
            f"[WARN] no trades for symbol={args.symbol} venue={args.venue} in {trade_path}"
        )
        return 0

    if args.lookback_hours > 0:
        trades = trades.dropna(subset=["trade_time_ms"])
        max_trade_time = trades["trade_time_ms"].max()
        cutoff = int(max_trade_time - args.lookback_hours * 3600 * 1000)
        trades = trades[trades["trade_time_ms"] >= cutoff]
        if trades.empty:
            print(
                "[WARN] no trades after lookback filter: "
                f"hours={args.lookback_hours} max_trade_time={max_trade_time}"
            )
            return 0

    trades["notional"] = trades["quantity"] * trades["price"]

    summary = (
        trades.groupby("strategy_id")
        .agg(
            trade_qty_sum=("quantity", "sum"),
            notional_sum=("notional", "sum"),
            trade_count=("quantity", "size"),
            order_count=("client_order_id", "nunique"),
            min_trade_time_ms=("trade_time_ms", "min"),
            max_trade_time_ms=("trade_time_ms", "max"),
        )
        .sort_values("trade_qty_sum", ascending=False)
    )

    print(
        f"symbol={args.symbol} venue={args.venue} "
        f"trades={len(trades)} strategies={summary.shape[0]} orders={trades['client_order_id'].nunique()}"
    )
    if args.limit <= 0 or summary.shape[0] <= args.limit:
        print(summary.to_string())
    else:
        print(summary.head(args.limit).to_string())
        print(f"... truncated, total strategies={summary.shape[0]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
