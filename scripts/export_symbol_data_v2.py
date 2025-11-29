#!/usr/bin/env python3
"""
导出指定 symbol 的订单和信号数据 (v2)

v2 改进:
    - hedge signal 已包含 opening_leg 信息，不再需要补 NaN
    - 直接从 hedge signal 获取 opening_venue/opening_bid0/opening_ask0

参数:
    --dir: 包含 parquet 文件的文件夹路径
    --symbol: 目标 symbol (如 SLPUSDT)
    --output: 输出文件名
"""

import argparse
import os
import pandas as pd


def extract_strategy_id(client_order_id: int) -> int:
    """从 client_order_id 提取 strategy_id"""
    return (client_order_id >> 32) & 0xFFFFFFFF


def classify_order_side(row) -> str:
    """根据 trading_venue 判断订单方向"""
    if pd.isna(row["opening_venue"]):
        return pd.NA
    elif row["trading_venue"] == row["opening_venue"]:
        return "open"
    elif row["trading_venue"] == row["hedging_venue"]:
        return "hedge"
    else:
        return pd.NA


def match_hedge_signal(order_row, df_hedge_sig_sorted: pd.DataFrame) -> pd.Series:
    """找到 event_time * 1000 > market_ts 且最接近的对冲信号"""
    sid = order_row["strategy_id"]
    event_time_us = order_row["event_time"] * 1000  # ms -> us

    signals = df_hedge_sig_sorted[df_hedge_sig_sorted["strategy_id"] == sid]
    if len(signals) == 0:
        return pd.Series({
            "create_ts": pd.NA,
            "opening_venue": pd.NA,
            "opening_bid0": pd.NA,
            "opening_ask0": pd.NA,
            "hedging_bid0": pd.NA,
            "hedging_ask0": pd.NA,
            "price_offset": pd.NA
        })

    valid_signals = signals[signals["market_ts"] < event_time_us]
    if len(valid_signals) == 0:
        return pd.Series({
            "create_ts": pd.NA,
            "opening_venue": pd.NA,
            "opening_bid0": pd.NA,
            "opening_ask0": pd.NA,
            "hedging_bid0": pd.NA,
            "hedging_ask0": pd.NA,
            "price_offset": pd.NA
        })

    best_signal = valid_signals.loc[valid_signals["market_ts"].idxmax()]
    return pd.Series({
        "create_ts": best_signal["market_ts"],
        "opening_venue": best_signal["opening_venue"],
        "opening_bid0": best_signal["opening_bid0"],
        "opening_ask0": best_signal["opening_ask0"],
        "hedging_bid0": best_signal["hedging_bid0"],
        "hedging_ask0": best_signal["hedging_ask0"],
        "price_offset": best_signal["price_offset"]
    })


def find_order_outcome(row, df_cancel_orders: pd.DataFrame, df_trade_sorted: pd.DataFrame) -> pd.Series:
    """查找订单的最终状态"""
    oid = row["order_id"]

    cancel_records = df_cancel_orders[df_cancel_orders["order_id"] == oid]
    if len(cancel_records) > 0:
        cancel_row = cancel_records.iloc[-1]
        return pd.Series({
            "status": "CANCELED",
            "update_ts": cancel_row["event_time"] * 1000,
            "filled_qty": cancel_row["cumulative_filled_quantity"]
        })

    trade_records = df_trade_sorted[df_trade_sorted["order_id"] == oid]
    if len(trade_records) > 0:
        last_trade = trade_records.iloc[-1]
        return pd.Series({
            "status": "FILLED",
            "update_ts": last_trade["event_time"] * 1000,
            "filled_qty": last_trade["cumulative_filled_quantity"]
        })

    return pd.Series({
        "status": "UNKNOWN",
        "update_ts": pd.NA,
        "filled_qty": pd.NA
    })


def main():
    parser = argparse.ArgumentParser(description="导出指定 symbol 的订单和信号数据 (v2)")
    parser.add_argument("--dir", required=True, help="包含 parquet 文件的文件夹路径")
    parser.add_argument("--symbol", required=True, help="目标 symbol (如 SLPUSDT)")
    parser.add_argument("--output", required=True, help="输出文件名")
    args = parser.parse_args()

    data_dir = args.dir
    symbol_filter = args.symbol
    output_file = args.output

    # 加载所有 parquet 文件
    print("加载 parquet 文件...")
    df_trade = pd.read_parquet(os.path.join(data_dir, "trade_updates.parquet"))
    df_order = pd.read_parquet(os.path.join(data_dir, "order_updates.parquet"))
    df_open_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_open.parquet"))
    df_hedge_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_hedge.parquet"))
    df_cancel_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_cancel.parquet"))

    # 尝试加载 close 信号（可能不存在或为空）
    close_sig_path = os.path.join(data_dir, "signals_arb_close.parquet")
    if os.path.exists(close_sig_path):
        df_close_sig = pd.read_parquet(close_sig_path)
    else:
        df_close_sig = pd.DataFrame()

    # 提取 strategy_id
    df_trade["strategy_id"] = df_trade["client_order_id"].apply(extract_strategy_id)
    df_order["strategy_id"] = df_order["client_order_id"].apply(extract_strategy_id)

    print(f"trade_updates: {len(df_trade)} rows")
    print(f"order_updates: {len(df_order)} rows")
    print(f"signals_arb_open: {len(df_open_sig)} rows")
    print(f"signals_arb_hedge: {len(df_hedge_sig)} rows")
    print(f"signals_arb_cancel: {len(df_cancel_sig)} rows")
    print(f"signals_arb_close: {len(df_close_sig)} rows")

    # 按 symbol 筛选
    df_order = df_order[df_order["symbol"] == symbol_filter].copy()
    df_trade = df_trade[df_trade["symbol"] == symbol_filter].copy()
    print(f"\n{symbol_filter} orders: {len(df_order)} rows")
    print(f"{symbol_filter} trades: {len(df_trade)} rows")

    # 筛选对应 symbol 的信号
    df_open_sig = df_open_sig[df_open_sig["opening_symbol"] == symbol_filter].copy()
    # hedge signal 现在也有 opening_symbol，可以用于筛选开仓侧或对冲侧
    df_hedge_sig_open = df_hedge_sig[df_hedge_sig["opening_symbol"] == symbol_filter].copy()
    df_hedge_sig_hedge = df_hedge_sig[df_hedge_sig["hedging_symbol"] == symbol_filter].copy()
    if len(df_close_sig) > 0:
        df_close_sig = df_close_sig[df_close_sig["opening_symbol"] == symbol_filter].copy()

    # 合并 open_sig 和 close_sig
    df_open_sig["signal_type"] = "open"
    if len(df_close_sig) > 0:
        df_close_sig["signal_type"] = "close"
        df_all_open_sig = pd.concat([df_open_sig, df_close_sig], ignore_index=True)
    else:
        df_all_open_sig = df_open_sig.copy()

    print(f"open_sig: {len(df_open_sig)}, close_sig: {len(df_close_sig)}, all_open_sig: {len(df_all_open_sig)}")
    print(f"hedge_sig (opening_symbol={symbol_filter}): {len(df_hedge_sig_open)}")
    print(f"hedge_sig (hedging_symbol={symbol_filter}): {len(df_hedge_sig_hedge)}")

    # 筛选 NEW 状态订单
    drop_cols = ["key", "ts_us", "client_order_id_str", "raw_status", "raw_execution_type",
                 "average_price", "last_executed_price", "business_unit", "time_in_force",
                 "last_executed_qty", "cumulative_filled_quantity", "execution_type"]
    df_new_orders = df_order[df_order["status"] == "NEW"].copy()
    df_new_orders.drop(columns=[c for c in drop_cols if c in df_new_orders.columns], inplace=True)

    # 关联信号获取 venue 信息
    df_new_orders = df_new_orders.merge(
        df_all_open_sig[["strategy_id", "opening_venue", "hedging_venue"]].drop_duplicates("strategy_id"),
        on="strategy_id",
        how="left"
    )

    # 判断订单方向
    df_new_orders["order_side"] = df_new_orders.apply(classify_order_side, axis=1)

    # 分离开仓侧和对冲侧订单
    df_open_orders = df_new_orders[df_new_orders["order_side"] == "open"].copy()
    df_hedge_orders = df_new_orders[df_new_orders["order_side"] == "hedge"].copy()

    # 开仓侧订单：关联信号获取盘口信息
    df_open_orders = df_open_orders.merge(
        df_all_open_sig[["strategy_id", "create_ts", "opening_bid0", "opening_ask0",
                         "hedging_bid0", "hedging_ask0", "price_offset"]].drop_duplicates("strategy_id"),
        on="strategy_id",
        how="left"
    )

    # 对冲侧订单：匹配对冲信号（v2: hedge signal 已包含 opening_leg 信息）
    df_hedge_sig_sorted = df_hedge_sig_hedge.sort_values(["strategy_id", "market_ts"])

    if len(df_hedge_orders) > 0:
        hedge_matched = df_hedge_orders.apply(
            lambda row: match_hedge_signal(row, df_hedge_sig_sorted), axis=1
        )
        df_hedge_orders = df_hedge_orders.reset_index(drop=True)
        df_hedge_orders["create_ts"] = hedge_matched["create_ts"].values
        # v2: 直接从 hedge signal 获取 opening_leg 信息
        df_hedge_orders["opening_venue"] = hedge_matched["opening_venue"].values
        df_hedge_orders["opening_bid0"] = hedge_matched["opening_bid0"].values
        df_hedge_orders["opening_ask0"] = hedge_matched["opening_ask0"].values
        df_hedge_orders["hedging_bid0"] = hedge_matched["hedging_bid0"].values
        df_hedge_orders["hedging_ask0"] = hedge_matched["hedging_ask0"].values
        df_hedge_orders["price_offset"] = hedge_matched["price_offset"].values

    # 合并订单
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        df_new_orders = pd.concat([df_open_orders, df_hedge_orders], ignore_index=True)

    # 添加时间字段
    df_new_orders["update_ts"] = 0
    df_new_orders["local_ts"] = df_new_orders["event_time"] * 1000
    df_new_orders.drop(columns=["event_time"], inplace=True, errors="ignore")

    print(f"\nNEW orders: {len(df_new_orders)}")
    print(f"开仓侧订单: {len(df_open_orders)}")
    print(f"对冲侧订单: {len(df_hedge_orders)}")

    # 查找订单最终状态
    df_cancel_orders = df_order[df_order["status"] == "CANCELED"].copy()
    df_trade_sorted = df_trade.sort_values(["order_id", "event_time"])

    outcome_df = df_new_orders.apply(
        lambda row: find_order_outcome(row, df_cancel_orders, df_trade_sorted), axis=1
    )
    df_new_orders["status"] = outcome_df["status"]
    df_new_orders["update_ts"] = outcome_df["update_ts"]
    df_new_orders["filled_qty"] = outcome_df["filled_qty"]

    # 统计结果
    print(f"\n订单最终状态统计:")
    print(df_new_orders["status"].value_counts())

    # 检查 UNKNOWN 状态
    df_unknown = df_new_orders[df_new_orders["status"] == "UNKNOWN"]
    if len(df_unknown) > 0:
        print(f"\n⚠️ 存在 {len(df_unknown)} 条 UNKNOWN 状态订单")
    else:
        print(f"\n✅ 没有 UNKNOWN 状态订单")

    # v2: 统计 opening_leg 信息覆盖情况
    hedge_orders_count = len(df_new_orders[df_new_orders["order_side"] == "hedge"])
    if hedge_orders_count > 0:
        opening_venue_filled = df_new_orders[
            (df_new_orders["order_side"] == "hedge") &
            (df_new_orders["opening_venue"].notna())
        ]
        print(f"\n对冲订单 opening_leg 覆盖率: {len(opening_venue_filled)}/{hedge_orders_count} "
              f"({100*len(opening_venue_filled)/hedge_orders_count:.1f}%)")

    # 导出结果
    df_new_orders.to_parquet(output_file)
    print(f"\n✅ 导出完成: {output_file}")


if __name__ == "__main__":
    main()
