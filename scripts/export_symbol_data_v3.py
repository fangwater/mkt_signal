#!/usr/bin/env python3
"""
导出指定 symbol 的订单和信号数据 (v3)

v3 输出列:
    create_ts, update_ts, client_order_id, <symbol_col>, venue, ttype, sid, side,
    price, amount_init, amount_update, status, inpos, tlen, from_key, bid1, ask1

参数:
    --dir: 包含 parquet 文件的文件夹路径
    --symbol: 目标 symbol (如 SLPUSDT)
    --output: 输出文件名
"""

import argparse
import os
import pandas as pd


SYMBOL_COL_NAME = "symbol"


def extract_strategy_id(client_order_id: int) -> int:
    """从 client_order_id 提取 strategy_id"""
    return (client_order_id >> 32) & 0xFFFFFFFF


def normalize_symbol(value) -> str:
    if value is None or pd.isna(value):
        return ""
    s = str(value).upper()
    if s == "NAN":
        return ""
    s = s.replace("-", "").replace("_", "")
    for suffix in ("SWAP", "PERP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s


def symbol_mask(df: pd.DataFrame, col: str, symbol_key: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(False, index=df.index)
    return df[col].map(normalize_symbol) == symbol_key


def classify_order_side(row) -> str:
    """根据 trading_venue 判断订单方向"""
    if pd.isna(row.get("opening_venue")):
        return pd.NA
    if row.get("trading_venue") == row.get("opening_venue"):
        return "open"
    if row.get("trading_venue") == row.get("hedging_venue"):
        return "hedge"
    return pd.NA


def match_hedge_signal(order_row, df_hedge_sig_sorted: pd.DataFrame) -> pd.Series:
    """找到 event_time * 1000 > market_ts 且最接近的对冲信号"""
    sid = order_row["strategy_id"]
    event_time_us = order_row["event_time"] * 1000  # ms -> us

    signals = df_hedge_sig_sorted[df_hedge_sig_sorted["strategy_id"] == sid]
    if len(signals) == 0:
        return pd.Series(
            {"hedging_bid0": pd.NA, "hedging_ask0": pd.NA, "price_offset": pd.NA}
        )

    valid_signals = signals[signals["market_ts"] < event_time_us]
    if len(valid_signals) == 0:
        return pd.Series(
            {"hedging_bid0": pd.NA, "hedging_ask0": pd.NA, "price_offset": pd.NA}
        )

    best_signal = valid_signals.loc[valid_signals["market_ts"].idxmax()]
    return pd.Series(
        {
            "hedging_bid0": best_signal.get("hedging_bid0", pd.NA),
            "hedging_ask0": best_signal.get("hedging_ask0", pd.NA),
            "price_offset": best_signal.get("price_offset", pd.NA),
        }
    )


def find_order_outcome(row, df_cancel_orders: pd.DataFrame, df_trade_sorted: pd.DataFrame) -> pd.Series:
    """查找订单的最终状态"""
    oid = row["order_id"]

    cancel_records = df_cancel_orders[df_cancel_orders["order_id"] == oid]
    if len(cancel_records) > 0:
        cancel_row = cancel_records.iloc[-1]
        return pd.Series(
            {
                "status": "CANCELED",
                "update_ts": cancel_row["event_time"] * 1000,
                "filled_qty": cancel_row.get("cumulative_filled_quantity", pd.NA),
            }
        )

    trade_records = df_trade_sorted[df_trade_sorted["order_id"] == oid]
    if len(trade_records) > 0:
        last_trade = trade_records.iloc[-1]
        return pd.Series(
            {
                "status": "FILLED",
                "update_ts": last_trade["event_time"] * 1000,
                "filled_qty": last_trade.get("cumulative_filled_quantity", pd.NA),
            }
        )

    return pd.Series({"status": "UNKNOWN", "update_ts": pd.NA, "filled_qty": pd.NA})


def infer_ttype(row) -> str:
    is_maker = row.get("is_maker")
    if pd.notna(is_maker):
        return "maker" if bool(is_maker) else "taker"

    tif = row.get("time_in_force")
    if isinstance(tif, str):
        tif_u = tif.upper()
        if tif_u in {"IOC", "FOK"}:
            return "taker"
        if tif_u in {"GTX", "PO", "POST_ONLY"}:
            return "maker"

    otype = row.get("order_type")
    if isinstance(otype, str) and "MARKET" in otype.upper():
        return "taker"

    return pd.NA


def main():
    parser = argparse.ArgumentParser(description="导出指定 symbol 的订单和信号数据 (v3)")
    parser.add_argument("--dir", required=True, help="包含 parquet 文件的文件夹路径")
    parser.add_argument("--symbol", required=True, help="目标 symbol (如 SLPUSDT)")
    parser.add_argument("--output", required=True, help="输出文件名")
    args = parser.parse_args()

    data_dir = args.dir
    symbol_key = normalize_symbol(args.symbol)
    output_file = args.output

    print("加载 parquet 文件...")
    df_trade = pd.read_parquet(os.path.join(data_dir, "trade_updates.parquet"))
    df_order = pd.read_parquet(os.path.join(data_dir, "order_updates.parquet"))
    df_open_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_open.parquet"))
    df_hedge_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_hedge.parquet"))
    df_cancel_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_cancel.parquet"))

    close_sig_path = os.path.join(data_dir, "signals_arb_close.parquet")
    if os.path.exists(close_sig_path):
        df_close_sig = pd.read_parquet(close_sig_path)
    else:
        df_close_sig = pd.DataFrame()

    df_trade["strategy_id"] = df_trade["client_order_id"].apply(extract_strategy_id)
    df_order["strategy_id"] = df_order["client_order_id"].apply(extract_strategy_id)

    print(f"trade_updates: {len(df_trade)} rows")
    print(f"order_updates: {len(df_order)} rows")
    print(f"signals_arb_open: {len(df_open_sig)} rows")
    print(f"signals_arb_hedge: {len(df_hedge_sig)} rows")
    print(f"signals_arb_cancel: {len(df_cancel_sig)} rows")
    print(f"signals_arb_close: {len(df_close_sig)} rows")

    sig_dfs = [df_open_sig, df_hedge_sig, df_cancel_sig]
    if len(df_close_sig) > 0:
        sig_dfs.append(df_close_sig)

    strategy_ids = set()
    for df_sig in sig_dfs:
        m = symbol_mask(df_sig, "opening_symbol", symbol_key) | symbol_mask(
            df_sig, "hedging_symbol", symbol_key
        )
        if "strategy_id" in df_sig.columns:
            strategy_ids.update(df_sig.loc[m, "strategy_id"].astype("int64").tolist())

    order_m = symbol_mask(df_order, "symbol", symbol_key)
    trade_m = symbol_mask(df_trade, "symbol", symbol_key)
    if len(strategy_ids) > 0:
        order_m = order_m | df_order["strategy_id"].isin(strategy_ids)
        trade_m = trade_m | df_trade["strategy_id"].isin(strategy_ids)

    df_order = df_order[order_m].copy()
    df_trade = df_trade[trade_m].copy()

    print(f"\nfilter_symbol_key={symbol_key}")
    print(f"matched strategy_id: {len(strategy_ids)}")
    if len(df_order) > 0 and "symbol" in df_order.columns:
        print(f"orders: {len(df_order)} rows, symbols={sorted(df_order['symbol'].dropna().unique().tolist())}")
    else:
        print(f"orders: {len(df_order)} rows")
    if len(df_trade) > 0 and "symbol" in df_trade.columns:
        print(f"trades: {len(df_trade)} rows, symbols={sorted(df_trade['symbol'].dropna().unique().tolist())}")
    else:
        print(f"trades: {len(df_trade)} rows")

    if len(strategy_ids) > 0:
        df_open_sig = df_open_sig[df_open_sig["strategy_id"].isin(strategy_ids)].copy()
        df_hedge_sig = df_hedge_sig[df_hedge_sig["strategy_id"].isin(strategy_ids)].copy()
        df_cancel_sig = df_cancel_sig[df_cancel_sig["strategy_id"].isin(strategy_ids)].copy()
        if len(df_close_sig) > 0:
            df_close_sig = df_close_sig[df_close_sig["strategy_id"].isin(strategy_ids)].copy()
    else:
        m_open = symbol_mask(df_open_sig, "opening_symbol", symbol_key) | symbol_mask(
            df_open_sig, "hedging_symbol", symbol_key
        )
        m_hedge = symbol_mask(df_hedge_sig, "opening_symbol", symbol_key) | symbol_mask(
            df_hedge_sig, "hedging_symbol", symbol_key
        )
        m_cancel = symbol_mask(df_cancel_sig, "opening_symbol", symbol_key) | symbol_mask(
            df_cancel_sig, "hedging_symbol", symbol_key
        )
        df_open_sig = df_open_sig[m_open].copy()
        df_hedge_sig = df_hedge_sig[m_hedge].copy()
        df_cancel_sig = df_cancel_sig[m_cancel].copy()
        if len(df_close_sig) > 0:
            m_close = symbol_mask(df_close_sig, "opening_symbol", symbol_key) | symbol_mask(
                df_close_sig, "hedging_symbol", symbol_key
            )
            df_close_sig = df_close_sig[m_close].copy()

    df_open_sig["signal_type"] = "open"
    if len(df_close_sig) > 0:
        df_close_sig["signal_type"] = "close"
        df_all_open_sig = pd.concat([df_open_sig, df_close_sig], ignore_index=True)
    else:
        df_all_open_sig = df_open_sig.copy()

    print(f"open_sig: {len(df_open_sig)}, close_sig: {len(df_close_sig)}, all_open_sig: {len(df_all_open_sig)}")

    df_new_orders = df_order[df_order["status"] == "NEW"].copy()

    df_new_orders = df_new_orders.merge(
        df_all_open_sig[["strategy_id", "opening_venue", "hedging_venue"]].drop_duplicates(
            "strategy_id"
        ),
        on="strategy_id",
        how="left",
    )
    df_new_orders["order_side"] = df_new_orders.apply(classify_order_side, axis=1)

    df_open_orders = df_new_orders[df_new_orders["order_side"] == "open"].copy()
    df_hedge_orders = df_new_orders[df_new_orders["order_side"] == "hedge"].copy()
    df_other_orders = df_new_orders[~df_new_orders["order_side"].isin(["open", "hedge"])].copy()

    df_open_orders = df_open_orders.merge(
        df_all_open_sig[
            [
                "strategy_id",
                "opening_bid0",
                "opening_ask0",
                "hedging_bid0",
                "hedging_ask0",
                "price_offset",
            ]
        ].drop_duplicates("strategy_id"),
        on="strategy_id",
        how="left",
    )

    df_hedge_sig_sorted = df_hedge_sig.sort_values(["strategy_id", "market_ts"])
    if len(df_hedge_orders) > 0:
        hedge_matched = df_hedge_orders.apply(
            lambda row: match_hedge_signal(row, df_hedge_sig_sorted), axis=1
        )
        df_hedge_orders = df_hedge_orders.reset_index(drop=True)
        df_hedge_orders["hedging_bid0"] = hedge_matched["hedging_bid0"].values
        df_hedge_orders["hedging_ask0"] = hedge_matched["hedging_ask0"].values
        df_hedge_orders["price_offset"] = hedge_matched["price_offset"].values

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        df_new_orders = pd.concat(
            [df_open_orders, df_hedge_orders, df_other_orders], ignore_index=True
        )

    df_new_orders["create_ts"] = df_new_orders["event_time"] * 1000
    df_new_orders["update_ts"] = 0
    df_new_orders["client_order_id"] = pd.to_numeric(
        df_new_orders["client_order_id"], errors="coerce"
    ).astype("Int64")

    df_cancel_orders = df_order[df_order["status"] == "CANCELED"].copy()
    df_trade_sorted = df_trade.sort_values(["order_id", "event_time"])

    if len(df_new_orders) == 0:
        df_new_orders["status"] = pd.Series(dtype="object")
        df_new_orders["update_ts"] = pd.Series(dtype="float64")
        df_new_orders["filled_qty"] = pd.Series(dtype="float64")
    else:
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

    last_trade = df_trade_sorted.groupby("order_id", sort=False).tail(1)
    trade_is_maker = last_trade.set_index("order_id")["is_maker"] if len(last_trade) > 0 else {}
    df_new_orders["is_maker"] = df_new_orders["order_id"].map(trade_is_maker)
    df_new_orders["ttype"] = df_new_orders.apply(infer_ttype, axis=1)

    df_new_orders["bid1"] = pd.NA
    df_new_orders["ask1"] = pd.NA
    open_mask = df_new_orders["order_side"] == "open"
    hedge_mask = df_new_orders["order_side"] == "hedge"
    df_new_orders.loc[open_mask, "bid1"] = df_new_orders.loc[open_mask, "opening_bid0"]
    df_new_orders.loc[open_mask, "ask1"] = df_new_orders.loc[open_mask, "opening_ask0"]
    df_new_orders.loc[hedge_mask, "bid1"] = df_new_orders.loc[hedge_mask, "hedging_bid0"]
    df_new_orders.loc[hedge_mask, "ask1"] = df_new_orders.loc[hedge_mask, "hedging_ask0"]

    df_new_orders["status"] = df_new_orders["status"].astype(str).str.lower()
    df_new_orders["inpos"] = pd.NA
    df_new_orders["tlen"] = pd.NA
    df_new_orders["from_key"] = df_new_orders["strategy_id"]
    if "price_offset" not in df_new_orders.columns:
        df_new_orders["price_offset"] = pd.NA

    output_cols = [
        "create_ts",
        "update_ts",
        "client_order_id",
        "symbol",
        "trading_venue",
        "ttype",
        "strategy_id",
        "side",
        "price",
        "quantity",
        "filled_qty",
        "status",
        "inpos",
        "tlen",
        "from_key",
        "price_offset",
        "bid1",
        "ask1",
    ]

    df_out = df_new_orders[output_cols].copy()
    df_out.rename(
        columns={
            "symbol": SYMBOL_COL_NAME,
            "trading_venue": "venue",
            "strategy_id": "sid",
            "quantity": "amount_init",
            "filled_qty": "amount_update",
        },
        inplace=True,
    )

    df_out.to_parquet(output_file)
    print(f"\n✅ 导出完成: {output_file}")


if __name__ == "__main__":
    main()
