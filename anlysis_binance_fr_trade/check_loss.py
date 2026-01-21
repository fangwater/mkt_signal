#!/usr/bin/env python3
"""Download trade data and find over-hedge strategies for a target symbol."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import urllib.request

import pandas as pd

SIGNAL_KINDS = [
    "signals_arb_open",
    "signals_arb_hedge",
    "signals_arb_cancel",
    "signals_arb_close",
]

QUOTE_SUFFIXES = ("USDT", "USDC", "USD", "BUSD", "BTC", "ETH")
TS_US_THRESHOLD = 100_000_000_000_000  # 1e14
TS_MS_THRESHOLD = 100_000_000_000      # 1e11
TS_S_THRESHOLD = 1_000_000_000         # 1e9


def download(api_base: str, path: str, out_path: Path) -> bool:
    url = f"{api_base.rstrip('/')}/{path.lstrip('/')}"
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = resp.read()
        if not data:
            print(f"{path}: empty response")
            return False
        out_path.write_bytes(data)
        size = out_path.stat().st_size
        print(f"{path}: ok ({size} bytes)")
        return True
    except Exception as exc:
        print(f"{path}: failed ({exc})")
        if out_path.exists():
            out_path.unlink()
        return False


def load_parquet(path: Path) -> pd.DataFrame:
    if path.exists():
        return pd.read_parquet(path)
    return pd.DataFrame()


def extract_strategy_id(val) -> pd.NA | int:
    try:
        return (int(val) >> 32) & 0xFFFFFFFF
    except Exception:
        return pd.NA


def normalize_ts_us(value):
    if value is None or pd.isna(value):
        return pd.NA
    try:
        ts = int(value)
    except (TypeError, ValueError):
        return pd.NA
    if ts <= 0:
        return ts
    if ts >= TS_US_THRESHOLD:
        return ts
    if ts >= TS_MS_THRESHOLD:
        return ts * 1000
    if ts >= TS_S_THRESHOLD:
        return ts * 1_000_000
    return ts


def normalize_ts_col(df: pd.DataFrame, col: str) -> None:
    if col not in df.columns:
        return
    df[col] = df[col].map(normalize_ts_us).astype("Int64")


def first_non_null(series):
    for v in series:
        if pd.notna(v):
            return v
    return pd.NA


def normalize_symbol(value) -> str:
    if value is None or pd.isna(value):
        return ""
    sym = str(value).upper().strip()
    if sym == "NAN":
        return ""
    sym = sym.replace("-", "").replace("_", "")
    for suffix in ("SWAP", "PERP"):
        if sym.endswith(suffix):
            sym = sym[: -len(suffix)]
    return sym


def symbol_matches(value, target: str) -> bool:
    sym = normalize_symbol(value)
    key = normalize_symbol(target)
    if not sym or not key:
        return False
    if sym == key:
        return True
    return any(sym == key + quote for quote in QUOTE_SUFFIXES)


def build_symbol_strategy_ids(frames: list[pd.DataFrame], symbol_key: str) -> set[int]:
    if not frames:
        return set()
    df = pd.concat(frames, ignore_index=True)
    if "strategy_id" not in df.columns:
        return set()

    mask = pd.Series(False, index=df.index)
    if "opening_symbol" in df.columns:
        mask |= df["opening_symbol"].map(lambda v: symbol_matches(v, symbol_key))
    if "hedging_symbol" in df.columns:
        mask |= df["hedging_symbol"].map(lambda v: symbol_matches(v, symbol_key))

    ids = df.loc[mask, "strategy_id"].dropna().astype("int64").tolist()
    return set(ids)


def classify_order_side(row) -> str:
    tv = row.get("trading_venue")
    if pd.isna(tv):
        return "unknown"
    if tv == row.get("opening_venue"):
        return "open"
    if tv == row.get("hedging_venue"):
        return "hedge"
    return "unknown"


def find_order_outcome(
    row, df_cancel_orders: pd.DataFrame, df_trade_sorted: pd.DataFrame
) -> pd.Series:
    """Find the final status and filled quantity for an order."""
    oid = row.get("order_id")

    cancel_records = df_cancel_orders[df_cancel_orders["order_id"] == oid]
    if len(cancel_records) > 0:
        cancel_row = cancel_records.iloc[-1]
        return pd.Series(
            {
                "status": "CANCELED",
                "update_ts": cancel_row.get("event_time", pd.NA),
                "filled_qty": cancel_row.get("cumulative_filled_quantity", pd.NA),
            }
        )

    trade_records = df_trade_sorted[df_trade_sorted["order_id"] == oid]
    if len(trade_records) > 0:
        last_trade = trade_records.iloc[-1]
        return pd.Series(
            {
                "status": "FILLED",
                "update_ts": last_trade.get("event_time", pd.NA),
                "filled_qty": last_trade.get("cumulative_filled_quantity", pd.NA),
            }
        )

    return pd.Series({"status": "UNKNOWN", "update_ts": pd.NA, "filled_qty": pd.NA})


def build_signal_meta(df_all_sig: pd.DataFrame) -> pd.DataFrame:
    if df_all_sig.empty or "strategy_id" not in df_all_sig.columns:
        return pd.DataFrame(columns=["strategy_id"])
    agg = {}
    for col in ("opening_symbol", "hedging_symbol", "opening_venue", "hedging_venue"):
        if col in df_all_sig.columns:
            agg[col] = (col, first_non_null)
    if not agg:
        return pd.DataFrame(columns=["strategy_id"])
    return (
        df_all_sig.dropna(subset=["strategy_id"])
        .groupby("strategy_id", sort=False)
        .agg(**agg)
        .reset_index()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Check over-hedge strategies (hedge fills > open fills) for a symbol",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--api-base",
        default="http://localhost:19131",
        help="Base URL for the local export API",
    )
    parser.add_argument(
        "--output-dir",
        default="output",
        help="Directory to store downloaded parquet files",
    )
    parser.add_argument(
        "--clean-output",
        action="store_true",
        help="Delete output directory before downloading",
    )
    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip downloading and reuse existing parquet files",
    )
    parser.add_argument(
        "--symbol",
        default="USTC",
        help="Target symbol or base asset (e.g. USTC or USTCUSDT)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of rows to print",
    )
    parser.add_argument(
        "--export",
        help="Export the over-hedge summary to CSV",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir).resolve()

    if args.clean_output and output_dir.exists():
        shutil.rmtree(output_dir)

    output_dir.mkdir(parents=True, exist_ok=True)

    if not args.skip_download:
        for kind in SIGNAL_KINDS:
            download(args.api_base, f"signals/{kind}/export", output_dir / f"{kind}.parquet")
        download(args.api_base, "order_updates/export", output_dir / "order_updates.parquet")
        download(args.api_base, "trade_updates/export", output_dir / "trade_updates.parquet")

    print("output files:")
    for path in sorted(output_dir.glob("*.parquet")):
        print(" -", path.name)

    df_trade = load_parquet(output_dir / "trade_updates.parquet")
    df_order = load_parquet(output_dir / "order_updates.parquet")
    df_open_sig = load_parquet(output_dir / "signals_arb_open.parquet")
    df_close_sig = load_parquet(output_dir / "signals_arb_close.parquet")
    df_hedge_sig = load_parquet(output_dir / "signals_arb_hedge.parquet")
    df_cancel_sig = load_parquet(output_dir / "signals_arb_cancel.parquet")

    if not df_trade.empty:
        df_trade["strategy_id"] = df_trade["client_order_id"].apply(extract_strategy_id)
    if not df_order.empty:
        df_order["strategy_id"] = df_order["client_order_id"].apply(extract_strategy_id)

    normalize_ts_col(df_trade, "event_time")
    normalize_ts_col(df_order, "event_time")
    normalize_ts_col(df_open_sig, "create_ts")
    normalize_ts_col(df_close_sig, "create_ts")
    normalize_ts_col(df_hedge_sig, "market_ts")

    sig_frames = [df for df in [df_open_sig, df_close_sig, df_hedge_sig, df_cancel_sig] if not df.empty]
    venue_frames = [
        df
        for df in sig_frames
        if {"strategy_id", "opening_venue", "hedging_venue"}.issubset(df.columns)
    ]

    if venue_frames:
        df_sig = pd.concat(venue_frames, ignore_index=True)
        venue_map = (
            df_sig[["strategy_id", "opening_venue", "hedging_venue"]]
            .dropna(subset=["strategy_id"])
            .groupby("strategy_id", sort=False)
            .agg(
                opening_venue=("opening_venue", first_non_null),
                hedging_venue=("hedging_venue", first_non_null),
            )
            .reset_index()
        )
    else:
        venue_map = pd.DataFrame(columns=["strategy_id", "opening_venue", "hedging_venue"])

    if "trading_venue" not in df_trade.columns and "trading_venue" in df_order.columns:
        df_trade = df_trade.merge(
            df_order[["order_id", "strategy_id", "trading_venue", "symbol"]].drop_duplicates(),
            on=["order_id", "strategy_id"],
            how="left",
            suffixes=("", "_order"),
        )
    elif "trading_venue" in df_trade.columns and "trading_venue" in df_order.columns:
        if df_trade["trading_venue"].isna().any():
            order_tv = (
                df_order[["order_id", "strategy_id", "trading_venue"]]
                .drop_duplicates()
                .set_index(["order_id", "strategy_id"])["trading_venue"]
            )

            def fill_tv(row):
                if pd.notna(row.get("trading_venue")):
                    return row.get("trading_venue")
                return order_tv.get((row.get("order_id"), row.get("strategy_id")), pd.NA)

            df_trade["trading_venue"] = df_trade.apply(fill_tv, axis=1)

    if "symbol" in df_trade.columns and "symbol" in df_order.columns:
        if df_trade["symbol"].isna().any():
            order_sym = (
                df_order[["order_id", "strategy_id", "symbol"]]
                .drop_duplicates()
                .set_index(["order_id", "strategy_id"])["symbol"]
            )

            def fill_symbol(row):
                if pd.notna(row.get("symbol")):
                    return row.get("symbol")
                return order_sym.get((row.get("order_id"), row.get("strategy_id")), pd.NA)

            df_trade["symbol"] = df_trade.apply(fill_symbol, axis=1)

    df_trade = df_trade.merge(venue_map, on="strategy_id", how="left")
    df_trade["order_side"] = df_trade.apply(classify_order_side, axis=1)

    symbol_key = normalize_symbol(args.symbol)
    strategy_ids = build_symbol_strategy_ids(sig_frames, symbol_key)

    if strategy_ids:
        df_trade = df_trade[df_trade["strategy_id"].isin(strategy_ids)].copy()
        df_order = df_order[df_order["strategy_id"].isin(strategy_ids)].copy()
    else:
        if "symbol" in df_trade.columns:
            df_trade = df_trade[df_trade["symbol"].map(lambda v: symbol_matches(v, symbol_key))].copy()
        if "symbol" in df_order.columns:
            df_order = df_order[df_order["symbol"].map(lambda v: symbol_matches(v, symbol_key))].copy()

    print(f"\nfilter_symbol_key={symbol_key}")
    print(f"matched strategy_id: {len(strategy_ids)}")
    if "symbol" in df_trade.columns and not df_trade.empty:
        symbols = sorted(df_trade["symbol"].dropna().unique().tolist())
        print(f"trade symbols: {symbols}")

    if strategy_ids:
        df_open_sig = df_open_sig[df_open_sig["strategy_id"].isin(strategy_ids)].copy()
        df_hedge_sig = df_hedge_sig[df_hedge_sig["strategy_id"].isin(strategy_ids)].copy()
        df_cancel_sig = df_cancel_sig[df_cancel_sig["strategy_id"].isin(strategy_ids)].copy()
        df_close_sig = df_close_sig[df_close_sig["strategy_id"].isin(strategy_ids)].copy()
    else:
        if "opening_symbol" in df_open_sig.columns:
            open_mask = df_open_sig["opening_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            hedge_mask = df_open_sig["hedging_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            df_open_sig = df_open_sig[open_mask | hedge_mask].copy()
        if "opening_symbol" in df_hedge_sig.columns:
            open_mask = df_hedge_sig["opening_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            hedge_mask = df_hedge_sig["hedging_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            df_hedge_sig = df_hedge_sig[open_mask | hedge_mask].copy()
        if "opening_symbol" in df_cancel_sig.columns:
            open_mask = df_cancel_sig["opening_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            hedge_mask = df_cancel_sig["hedging_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            df_cancel_sig = df_cancel_sig[open_mask | hedge_mask].copy()
        if "opening_symbol" in df_close_sig.columns and not df_close_sig.empty:
            open_mask = df_close_sig["opening_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            hedge_mask = df_close_sig["hedging_symbol"].map(lambda v: symbol_matches(v, symbol_key))
            df_close_sig = df_close_sig[open_mask | hedge_mask].copy()

    if df_open_sig.empty and df_hedge_sig.empty and df_cancel_sig.empty and df_close_sig.empty:
        print("\nsignals missing after filtering")
        return

    df_open_sig["signal_type"] = "open"
    df_hedge_sig["signal_type"] = "hedge"
    if not df_cancel_sig.empty:
        df_cancel_sig["signal_type"] = "cancel"
    if not df_close_sig.empty:
        df_close_sig["signal_type"] = "close"
    df_all_sig = pd.concat(
        [df_open_sig, df_hedge_sig, df_cancel_sig, df_close_sig], ignore_index=True
    )

    df_new_orders = df_order[df_order["status"] == "NEW"].copy()
    if df_new_orders.empty:
        print("\nno NEW orders after filtering")
        return

    df_new_orders = df_new_orders.merge(venue_map, on="strategy_id", how="left")
    df_new_orders["order_side"] = df_new_orders.apply(classify_order_side, axis=1)

    df_cancel_orders = df_order[df_order["status"] == "CANCELED"].copy()
    df_trade_sorted = df_trade.sort_values(["order_id", "event_time"])

    outcome_df = df_new_orders.apply(
        lambda row: find_order_outcome(row, df_cancel_orders, df_trade_sorted),
        axis=1,
        result_type="expand",
    )
    for c in ("status", "update_ts", "filled_qty"):
        if c not in outcome_df.columns:
            outcome_df[c] = pd.NA
    df_new_orders["status"] = outcome_df["status"].values
    df_new_orders["update_ts"] = outcome_df["update_ts"].values
    df_new_orders["filled_qty"] = outcome_df["filled_qty"].values
    df_new_orders["filled_qty"] = pd.to_numeric(df_new_orders["filled_qty"], errors="coerce").fillna(0.0)

    open_hedge_orders = df_new_orders[
        df_new_orders["order_side"].isin(["open", "hedge"])
    ].copy()
    if open_hedge_orders.empty:
        print("\nopen/hedge orders missing after filtering")
        return

    fill_summary = (
        open_hedge_orders.groupby(["strategy_id", "order_side"], dropna=False)
        .agg(
            filled_qty=("filled_qty", "sum"),
            order_count=("order_id", "nunique"),
            last_ts=("event_time", "max"),
        )
        .reset_index()
    )

    pivot_filled = (
        fill_summary.pivot(index="strategy_id", columns="order_side", values="filled_qty")
        .reindex(columns=["open", "hedge"])
        .fillna(0.0)
        .rename(columns={"open": "open_filled_amount", "hedge": "hedge_filled_amount"})
    )
    pivot_orders = (
        fill_summary.pivot(index="strategy_id", columns="order_side", values="order_count")
        .reindex(columns=["open", "hedge"])
        .fillna(0)
        .rename(columns={"open": "open_order_count", "hedge": "hedge_order_count"})
    )
    pivot_last_ts = (
        fill_summary.pivot(index="strategy_id", columns="order_side", values="last_ts")
        .reindex(columns=["open", "hedge"])
        .rename(columns={"open": "open_last_ts", "hedge": "hedge_last_ts"})
    )

    summary = pivot_filled.join([pivot_orders, pivot_last_ts], how="outer").reset_index()
    summary["hedge_minus_open"] = summary["hedge_filled_amount"] - summary["open_filled_amount"]
    summary["hedge_over_open_ratio"] = (
        summary["hedge_filled_amount"]
        / summary["open_filled_amount"].replace({0: pd.NA})
    )

    meta = build_signal_meta(df_all_sig)
    if not meta.empty:
        summary = summary.merge(meta, on="strategy_id", how="left")

    if "signal_type" in df_all_sig.columns:
        sig_counts = (
            df_all_sig.groupby(["strategy_id", "signal_type"], dropna=False)
            .size()
            .unstack(fill_value=0)
            .reindex(columns=["open", "hedge", "cancel", "close"], fill_value=0)
            .rename(
                columns={
                    "open": "open_signal_count",
                    "hedge": "hedge_signal_count",
                    "cancel": "cancel_signal_count",
                    "close": "close_signal_count",
                }
            )
            .reset_index()
        )
        summary = summary.merge(sig_counts, on="strategy_id", how="left")

    if "amount" in df_all_sig.columns:
        sig_amounts = (
            df_all_sig.groupby(["strategy_id", "signal_type"], dropna=False)["amount"]
            .sum()
            .unstack(fill_value=0)
            .reindex(columns=["open", "hedge", "cancel", "close"], fill_value=0)
            .rename(
                columns={
                    "open": "open_signal_amount",
                    "hedge": "hedge_signal_amount",
                    "cancel": "cancel_signal_amount",
                    "close": "close_signal_amount",
                }
            )
            .reset_index()
        )
        summary = summary.merge(sig_amounts, on="strategy_id", how="left")

    over_hedge = summary[summary["hedge_filled_amount"] > summary["open_filled_amount"]].copy()
    over_hedge = over_hedge.sort_values("hedge_minus_open", ascending=False)

    print(f"\nopen-leg over-hedge candidates: {len(over_hedge)} strategies")
    if not over_hedge.empty:
        print(over_hedge.head(args.limit).to_string(index=False))

    print("\nover-hedge strategy ids:")
    print(over_hedge["strategy_id"].dropna().astype("int64").tolist())

    hedge_only = summary[
        (summary["open_filled_amount"] == 0) & (summary["hedge_filled_amount"] > 0)
    ]
    if not hedge_only.empty:
        print(f"\nhedge-only (no open fills): {len(hedge_only)} strategies")

    if args.export:
        export_path = Path(args.export).expanduser().resolve()
        export_path.parent.mkdir(parents=True, exist_ok=True)
        over_hedge.to_csv(export_path, index=False)
        print(f"\nexported: {export_path}")


if __name__ == "__main__":
    main()
