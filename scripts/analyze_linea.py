#!/usr/bin/env python3
"""分析 LINEA 相关的订单和成交数据"""

import pandas as pd

# 加载所有 parquet 文件
print("=== 加载数据 ===")
df_trade = pd.read_parquet("/home/ubuntu/temp/trade_updates.parquet")
df_order = pd.read_parquet("/home/ubuntu/temp/order_updates.parquet")
df_open_sig = pd.read_parquet("/home/ubuntu/temp/signals_arb_open.parquet")
df_hedge_sig = pd.read_parquet("/home/ubuntu/temp/signals_arb_hedge.parquet")
df_cancel_sig = pd.read_parquet("/home/ubuntu/temp/signals_arb_cancel.parquet")
df_close_sig = pd.read_parquet("/home/ubuntu/temp/signals_arb_close.parquet")

# 提取 strategy_id
df_trade["strategy_id"] = df_trade["client_order_id"].apply(lambda x: (x >> 32) & 0xFFFFFFFF)
df_order["strategy_id"] = df_order["client_order_id"].apply(lambda x: (x >> 32) & 0xFFFFFFFF)

print(f"trade_updates: {len(df_trade)} rows")
print(f"order_updates: {len(df_order)} rows")
print(f"signals_arb_open: {len(df_open_sig)} rows")
print(f"signals_arb_hedge: {len(df_hedge_sig)} rows")
print(f"signals_arb_cancel: {len(df_cancel_sig)} rows")
print(f"signals_arb_close: {len(df_close_sig)} rows")

# 筛选 LINEA 相关的订单和成交
symbol_filter = "LINEAUSDT"

df_order_linea = df_order[df_order["symbol"] == symbol_filter].copy()
df_trade_linea = df_trade[df_trade["symbol"] == symbol_filter].copy()

print(f"\n=== LINEA 数据 ===")
print(f"LINEA orders: {len(df_order_linea)} rows")
print(f"LINEA trades: {len(df_trade_linea)} rows")

if len(df_order_linea) > 0:
    print("\n=== LINEA Orders ===")
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    print(df_order_linea.to_string())

if len(df_trade_linea) > 0:
    print("\n=== LINEA Trades ===")
    print(df_trade_linea.to_string())
