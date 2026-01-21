#!/usr/bin/env python3
"""
导出指定 symbol 的订单和信号数据 (v4)

v4 行为:
    - 每一条 order_update 都保留，记录中间态（部分成交/多次部分成交/撤单）
    - amount_update 统一用 cumulative_filled_quantity 的差分计算

v4 输出列:
    create_ts, update_ts, signal_ts, client_order_id, <symbol_col>, venue, ttype, side,
    price, amount_init, amount_update, status, inpos, tlen, from_key, price_offset,
    bid1, ask1

参数:
    --dir: 包含 parquet 文件的文件夹路径
    --symbol: 目标 symbol (如 SLPUSDT)
    --output: 输出文件名
"""

import argparse
import json
import os
import time
import urllib.request

import pandas as pd


SYMBOL_COL_NAME = "symbol"
TS_US_THRESHOLD = 100_000_000_000_000  # 1e14
TS_MS_THRESHOLD = 100_000_000_000      # 1e11
TS_S_THRESHOLD = 1_000_000_000         # 1e9
OKX_SWAP_URL = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
OKX_MULTIPLIER_CACHE = os.path.join(
    os.path.dirname(__file__),
    "okx_swap_multipliers.json",
)
TARGETS = {
    "xarb-okex-binance": {
        "dir": "/home/ubuntu/okex-binance-xarb-trade/data/order_query/export_data",
        "output_dir": os.path.expanduser("~/data/xarb-okex-binance"),
    }
}


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


def parse_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def read_okx_multiplier_cache(cache_path: str) -> dict[str, float] | None:
    if not os.path.exists(cache_path):
        return None
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            payload = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"⚠️ 读取 OKX 合约乘数缓存失败: {cache_path} ({exc})")
        return None
    if not isinstance(payload, dict):
        return None
    mapping = payload.get("multipliers", payload)
    if not isinstance(mapping, dict):
        return None
    multipliers: dict[str, float] = {}
    for key, value in mapping.items():
        symbol_key = normalize_symbol(key)
        if not symbol_key:
            continue
        mult = parse_float(value)
        if mult is None:
            continue
        multipliers[symbol_key] = mult
    return multipliers or None


def fetch_okx_swap_instruments() -> list[dict]:
    with urllib.request.urlopen(OKX_SWAP_URL, timeout=10) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    payload = json.loads(body)
    if payload.get("code") != "0":
        raise RuntimeError(f"OKX API error: {payload.get('code')} - {payload.get('msg')}")
    data = payload.get("data")
    return data if isinstance(data, list) else []


def build_okx_multipliers(instruments: list[dict]) -> dict[str, float]:
    multipliers: dict[str, float] = {}
    for inst in instruments:
        if not isinstance(inst, dict):
            continue
        if inst.get("ctType") != "linear":
            continue
        if inst.get("settleCcy") != "USDT":
            continue
        inst_id = inst.get("instId") or ""
        symbol_key = normalize_symbol(inst_id)
        if not symbol_key:
            continue
        ct_val = parse_float(inst.get("ctVal"), 1.0)
        ct_mult = parse_float(inst.get("ctMult"), 1.0)
        if ct_val is None or ct_mult is None:
            continue
        contract_size = ct_val * ct_mult
        if contract_size <= 0.0:
            continue
        multipliers[symbol_key] = contract_size
    return multipliers


def load_okx_multipliers(cache_path: str) -> dict[str, float]:
    cached = read_okx_multiplier_cache(cache_path)
    if cached is not None:
        print(f"✅ 使用 OKX 合约乘数缓存: {cache_path} ({len(cached)} symbols)")
        return cached

    print("缓存不存在，正在拉取 OKX 合约乘数...")
    try:
        instruments = fetch_okx_swap_instruments()
        multipliers = build_okx_multipliers(instruments)
    except Exception as exc:
        print(f"⚠️ 拉取 OKX 合约乘数失败: {exc}")
        return {}

    if multipliers:
        payload = {"fetched_at": int(time.time()), "multipliers": multipliers}
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2, sort_keys=True)
            print(f"✅ OKX 合约乘数已缓存: {cache_path}")
        except OSError as exc:
            print(f"⚠️ 写入 OKX 合约乘数缓存失败: {cache_path} ({exc})")
    return multipliers


def apply_okx_contract_multipliers(
    df: pd.DataFrame, multipliers: dict[str, float], qty_cols: list[str]
) -> None:
    if df.empty or not multipliers:
        return
    if "trading_venue" not in df.columns or "symbol" not in df.columns:
        return
    okx_mask = df["trading_venue"].astype(str) == "OkexFutures"
    if not okx_mask.any():
        return
    symbol_keys = df.loc[okx_mask, "symbol"].map(normalize_symbol)
    mults = symbol_keys.map(lambda k: multipliers.get(k, pd.NA))
    if mults.isna().any():
        missing = sorted(set(symbol_keys[mults.isna()].dropna().tolist()))
        if missing:
            example = ", ".join(missing[:5])
            print(f"⚠️ OKX 合约乘数缺失: {len(missing)} 个 (示例: {example})")
    mults = mults.fillna(1.0)
    for col in qty_cols:
        if col in df.columns:
            df.loc[okx_mask, col] = df.loc[okx_mask, col] * mults


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


def symbol_mask(df: pd.DataFrame, col: str, symbol_key: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(False, index=df.index)
    return df[col].map(normalize_symbol) == symbol_key


def signal_missing_mask(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    missing = pd.Series(True, index=df.index)
    found_col = False
    for col in cols:
        if col not in df.columns:
            continue
        found_col = True
        missing = missing & (df[col].map(normalize_symbol) == "")
    if not found_col:
        return pd.Series(True, index=df.index)
    return missing


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
    """找到 event_time_us > market_ts 且最接近的对冲信号"""
    sid = order_row["strategy_id"]
    event_time_us = order_row.get("create_ts")
    if pd.isna(event_time_us):
        event_time_us = order_row.get("event_time")
    if pd.isna(event_time_us):
        return pd.Series(
            {
                "hedging_bid0": pd.NA,
                "hedging_ask0": pd.NA,
                "price_offset": pd.NA,
                "signal_ts": pd.NA,
            }
        )

    signals = df_hedge_sig_sorted[df_hedge_sig_sorted["strategy_id"] == sid]
    if len(signals) == 0:
        return pd.Series(
            {
                "hedging_bid0": pd.NA,
                "hedging_ask0": pd.NA,
                "price_offset": pd.NA,
                "signal_ts": pd.NA,
            }
        )

    valid_signals = signals[signals["market_ts"] < event_time_us]
    if len(valid_signals) == 0:
        return pd.Series(
            {
                "hedging_bid0": pd.NA,
                "hedging_ask0": pd.NA,
                "price_offset": pd.NA,
                "signal_ts": pd.NA,
            }
        )

    best_signal = valid_signals.loc[valid_signals["market_ts"].idxmax()]
    return pd.Series(
        {
            "hedging_bid0": best_signal.get("hedging_bid0", pd.NA),
            "hedging_ask0": best_signal.get("hedging_ask0", pd.NA),
            "price_offset": best_signal.get("price_offset", pd.NA),
            "signal_ts": best_signal.get("market_ts", pd.NA),
        }
    )


def match_open_signal_ts(order_row, df_open_sig_sorted: pd.DataFrame) -> pd.Series:
    sid = order_row["strategy_id"]
    event_time_us = order_row.get("create_ts")
    if pd.isna(event_time_us):
        event_time_us = order_row.get("event_time")
    if pd.isna(event_time_us):
        return pd.Series({"signal_ts": pd.NA})
    if "create_ts" not in df_open_sig_sorted.columns:
        return pd.Series({"signal_ts": pd.NA})

    signals = df_open_sig_sorted[df_open_sig_sorted["strategy_id"] == sid]
    if len(signals) == 0:
        return pd.Series({"signal_ts": pd.NA})

    valid_signals = signals[signals["create_ts"] <= event_time_us]
    if len(valid_signals) == 0:
        return pd.Series({"signal_ts": pd.NA})

    best_signal = valid_signals.loc[valid_signals["create_ts"].idxmax()]
    return pd.Series({"signal_ts": best_signal.get("create_ts", pd.NA)})


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


def collect_symbol_keys(df: pd.DataFrame, cols: list[str]) -> set[str]:
    keys: set[str] = set()
    for col in cols:
        if col not in df.columns:
            continue
        series = df[col].map(normalize_symbol)
        for val in series.dropna().unique().tolist():
            if val:
                keys.add(val)
    return keys


def export_symbol(
    data_dir: str,
    df_trade: pd.DataFrame,
    df_order: pd.DataFrame,
    df_open_sig: pd.DataFrame,
    df_hedge_sig: pd.DataFrame,
    df_cancel_sig: pd.DataFrame,
    df_close_sig: pd.DataFrame,
    symbol_key: str,
    output_file: str,
) -> None:
    print(f"\n=== export symbol={symbol_key} -> {output_file} ===")
    # copy to avoid mutating shared frames
    df_trade = df_trade.copy()
    df_order = df_order.copy()
    df_open_sig = df_open_sig.copy()
    df_hedge_sig = df_hedge_sig.copy()
    df_cancel_sig = df_cancel_sig.copy()
    df_close_sig = df_close_sig.copy()

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
        if "symbol" in df_order.columns:
            order_missing = df_order["symbol"].map(normalize_symbol) == ""
            order_m = order_m | (df_order["strategy_id"].isin(strategy_ids) & order_missing)
        else:
            order_m = order_m | df_order["strategy_id"].isin(strategy_ids)
        if "symbol" in df_trade.columns:
            trade_missing = df_trade["symbol"].map(normalize_symbol) == ""
            trade_m = trade_m | (df_trade["strategy_id"].isin(strategy_ids) & trade_missing)
        else:
            trade_m = trade_m | df_trade["strategy_id"].isin(strategy_ids)

    df_order = df_order[order_m].copy()
    df_trade = df_trade[trade_m].copy()

    print(f"matched strategy_id: {len(strategy_ids)}")
    if len(df_order) > 0 and "symbol" in df_order.columns:
        print(f"orders: {len(df_order)} rows, symbols={sorted(df_order['symbol'].dropna().unique().tolist())}")
    else:
        print(f"orders: {len(df_order)} rows")
    if len(df_trade) > 0 and "symbol" in df_trade.columns:
        print(f"trades: {len(df_trade)} rows, symbols={sorted(df_trade['symbol'].dropna().unique().tolist())}")
    else:
        print(f"trades: {len(df_trade)} rows")

    m_open = symbol_mask(df_open_sig, "opening_symbol", symbol_key) | symbol_mask(
        df_open_sig, "hedging_symbol", symbol_key
    )
    m_hedge = symbol_mask(df_hedge_sig, "opening_symbol", symbol_key) | symbol_mask(
        df_hedge_sig, "hedging_symbol", symbol_key
    )
    m_cancel = symbol_mask(df_cancel_sig, "opening_symbol", symbol_key) | symbol_mask(
        df_cancel_sig, "hedging_symbol", symbol_key
    )
    if len(strategy_ids) > 0:
        m_open = m_open | signal_missing_mask(df_open_sig, ["opening_symbol", "hedging_symbol"])
        m_hedge = m_hedge | signal_missing_mask(df_hedge_sig, ["opening_symbol", "hedging_symbol"])
        m_cancel = m_cancel | signal_missing_mask(df_cancel_sig, ["opening_symbol", "hedging_symbol"])
        m_open = m_open & df_open_sig["strategy_id"].isin(strategy_ids)
        m_hedge = m_hedge & df_hedge_sig["strategy_id"].isin(strategy_ids)
        m_cancel = m_cancel & df_cancel_sig["strategy_id"].isin(strategy_ids)
    df_open_sig = df_open_sig[m_open].copy()
    df_hedge_sig = df_hedge_sig[m_hedge].copy()
    df_cancel_sig = df_cancel_sig[m_cancel].copy()
    if len(df_close_sig) > 0:
        m_close = symbol_mask(df_close_sig, "opening_symbol", symbol_key) | symbol_mask(
            df_close_sig, "hedging_symbol", symbol_key
        )
        if len(strategy_ids) > 0:
            m_close = m_close | signal_missing_mask(df_close_sig, ["opening_symbol", "hedging_symbol"])
            m_close = m_close & df_close_sig["strategy_id"].isin(strategy_ids)
        df_close_sig = df_close_sig[m_close].copy()

    df_open_sig["signal_type"] = "open"
    if len(df_close_sig) > 0:
        df_close_sig["signal_type"] = "close"
        df_all_open_sig = pd.concat([df_open_sig, df_close_sig], ignore_index=True)
    else:
        df_all_open_sig = df_open_sig.copy()
    if "create_ts" not in df_all_open_sig.columns:
        df_all_open_sig["create_ts"] = pd.NA

    print(f"open_sig: {len(df_open_sig)}, close_sig: {len(df_close_sig)}, all_open_sig: {len(df_all_open_sig)}")

    df_order_updates = df_order.copy()
    if len(df_order_updates) == 0:
        df_order_updates["create_ts"] = pd.Series(dtype="float64")
        df_order_updates["update_ts"] = pd.Series(dtype="float64")
        df_order_updates["filled_qty"] = pd.Series(dtype="float64")
        df_order_updates["status"] = pd.Series(dtype="object")
    else:
        if "order_id" in df_order_updates.columns:
            create_ts_map = df_order_updates.groupby("order_id")["event_time"].min()
            df_order_updates["create_ts"] = df_order_updates["order_id"].map(create_ts_map)
        else:
            df_order_updates["create_ts"] = df_order_updates["event_time"]
        df_order_updates["update_ts"] = df_order_updates["event_time"]
        if "cumulative_filled_quantity" in df_order_updates.columns:
            if "order_id" in df_order_updates.columns:
                sort_cols = ["order_id", "event_time"]
                if "ts_us" in df_order_updates.columns:
                    sort_cols.append("ts_us")
                df_sorted = df_order_updates.sort_values(sort_cols)
                cum = pd.to_numeric(
                    df_sorted["cumulative_filled_quantity"], errors="coerce"
                )
                diff = cum.groupby(df_sorted["order_id"]).diff()
                diff = diff.fillna(cum)
                df_order_updates["filled_qty"] = diff.reindex(df_order_updates.index)
            else:
                df_order_updates["filled_qty"] = pd.to_numeric(
                    df_order_updates["cumulative_filled_quantity"], errors="coerce"
                )
        else:
            df_order_updates["filled_qty"] = pd.NA

    df_order_updates = df_order_updates.merge(
        df_all_open_sig[["strategy_id", "opening_venue", "hedging_venue"]].drop_duplicates(
            "strategy_id"
        ),
        on="strategy_id",
        how="left",
    )
    df_order_updates["order_side"] = df_order_updates.apply(classify_order_side, axis=1)

    df_open_orders = df_order_updates[df_order_updates["order_side"] == "open"].copy()
    df_hedge_orders = df_order_updates[df_order_updates["order_side"] == "hedge"].copy()
    df_other_orders = df_order_updates[
        ~df_order_updates["order_side"].isin(["open", "hedge"])
    ].copy()
    df_open_orders["signal_ts"] = pd.NA
    df_hedge_orders["signal_ts"] = pd.NA
    df_other_orders["signal_ts"] = pd.NA

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

    df_open_sig_sorted = df_all_open_sig.sort_values(["strategy_id", "create_ts"])
    if len(df_open_orders) > 0:
        open_matched = df_open_orders.apply(
            lambda row: match_open_signal_ts(row, df_open_sig_sorted), axis=1
        )
        df_open_orders = df_open_orders.reset_index(drop=True)
        df_open_orders["signal_ts"] = open_matched["signal_ts"].values

    df_hedge_sig_sorted = df_hedge_sig.sort_values(["strategy_id", "market_ts"])
    if len(df_hedge_orders) > 0:
        hedge_matched = df_hedge_orders.apply(
            lambda row: match_hedge_signal(row, df_hedge_sig_sorted), axis=1
        )
        df_hedge_orders = df_hedge_orders.reset_index(drop=True)
        df_hedge_orders["hedging_bid0"] = hedge_matched["hedging_bid0"].values
        df_hedge_orders["hedging_ask0"] = hedge_matched["hedging_ask0"].values
        df_hedge_orders["price_offset"] = hedge_matched["price_offset"].values
        df_hedge_orders["signal_ts"] = hedge_matched["signal_ts"].values

    import warnings

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
    df_order_updates = pd.concat(
        [df_open_orders, df_hedge_orders, df_other_orders], ignore_index=True
    )

    df_order_updates["client_order_id"] = pd.to_numeric(
        df_order_updates["client_order_id"], errors="coerce"
    ).astype("Int64")
    if "signal_ts" in df_order_updates.columns:
        df_order_updates["signal_ts"] = pd.to_numeric(
            df_order_updates["signal_ts"], errors="coerce"
        ).astype("Int64")

    df_order_updates["create_ts"] = pd.to_numeric(
        df_order_updates["create_ts"], errors="coerce"
    ).astype("Int64")
    df_order_updates["update_ts"] = pd.to_numeric(
        df_order_updates["update_ts"], errors="coerce"
    ).astype("Int64")

    df_trade_sorted = df_trade.sort_values(["order_id", "event_time"])
    last_trade = df_trade_sorted.groupby("order_id", sort=False).tail(1)
    trade_is_maker = last_trade.set_index("order_id")["is_maker"] if len(last_trade) > 0 else {}
    df_order_updates["is_maker"] = df_order_updates["order_id"].map(trade_is_maker)
    df_order_updates["ttype"] = df_order_updates.apply(infer_ttype, axis=1)

    df_order_updates["bid1"] = pd.NA
    df_order_updates["ask1"] = pd.NA
    open_mask = df_order_updates["order_side"] == "open"
    hedge_mask = df_order_updates["order_side"] == "hedge"
    df_order_updates.loc[open_mask, "bid1"] = df_order_updates.loc[open_mask, "opening_bid0"]
    df_order_updates.loc[open_mask, "ask1"] = df_order_updates.loc[open_mask, "opening_ask0"]
    df_order_updates.loc[hedge_mask, "bid1"] = df_order_updates.loc[hedge_mask, "hedging_bid0"]
    df_order_updates.loc[hedge_mask, "ask1"] = df_order_updates.loc[hedge_mask, "hedging_ask0"]

    df_order_updates["status"] = df_order_updates["status"].astype(str).str.lower()
    if "side" in df_order_updates.columns:
        df_order_updates["side"] = df_order_updates["side"].astype(str).str.lower()
    df_order_updates["inpos"] = pd.NA
    df_order_updates["tlen"] = pd.NA
    df_order_updates["from_key"] = df_order_updates["strategy_id"]
    if "price_offset" not in df_order_updates.columns:
        df_order_updates["price_offset"] = pd.NA
    df_order_updates["symbol"] = df_order_updates["symbol"].map(normalize_symbol)

    output_cols = [
        "symbol",
        "create_ts",
        "update_ts",
        "signal_ts",
        "client_order_id",
        "trading_venue",
        "ttype",
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

    df_out = df_order_updates[output_cols].copy()
    df_out.rename(
        columns={
            "symbol": SYMBOL_COL_NAME,
            "trading_venue": "venue",
            "quantity": "amount_init",
            "filled_qty": "amount_update",
        },
        inplace=True,
    )

    df_out.to_parquet(output_file)
    print(f"✅ 导出完成: {output_file}")


def main():
    parser = argparse.ArgumentParser(description="导出指定 symbol 的订单和信号数据 (v4)")
    parser.add_argument(
        "--dir",
        help="包含 parquet 文件的文件夹路径",
    )
    parser.add_argument("--symbol", help="目标 symbol (如 SLPUSDT)")
    parser.add_argument("--all", action="store_true", help="导出全部 symbol")
    parser.add_argument(
        "--target",
        choices=sorted(TARGETS.keys()),
        help="预设数据目录与输出目录",
    )
    parser.add_argument("--output", help="输出文件名 (单 symbol)")
    parser.add_argument("--output-dir", help="输出目录 (导出全部 symbol)")
    args = parser.parse_args()

    if args.target and args.dir:
        raise SystemExit("不能同时使用 --target 和 --dir")
    if args.target:
        data_dir = TARGETS[args.target]["dir"]
        default_output_dir = TARGETS[args.target]["output_dir"]
    elif args.dir:
        data_dir = os.path.expanduser(args.dir)
        default_output_dir = "."
    else:
        raise SystemExit("请指定 --target 或 --dir")

    data_dir = os.path.expanduser(data_dir)
    os.makedirs(data_dir, exist_ok=True)

    output_dir = args.output_dir
    if output_dir:
        output_dir = os.path.expanduser(output_dir)
    else:
        output_dir = default_output_dir
    output_dir = os.path.expanduser(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    if args.all and args.symbol:
        raise SystemExit("不能同时使用 --all 和 --symbol")
    if args.all and args.output:
        raise SystemExit("--all 模式下请使用 --output-dir")
    if not args.all and not args.symbol:
        raise SystemExit("请指定 --symbol 或使用 --all")

    print("加载 parquet 文件...")
    df_trade = pd.read_parquet(os.path.join(data_dir, "trade_updates.parquet"))
    df_order = pd.read_parquet(os.path.join(data_dir, "order_updates.parquet"))
    df_open_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_open.parquet"))
    df_hedge_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_hedge.parquet"))
    df_cancel_sig = pd.read_parquet(os.path.join(data_dir, "signals_arb_cancel.parquet"))

    normalize_ts_col(df_trade, "event_time")
    normalize_ts_col(df_order, "event_time")

    needs_okx = False
    if "trading_venue" in df_order.columns:
        needs_okx = (df_order["trading_venue"].astype(str) == "OkexFutures").any()
    if not needs_okx and "trading_venue" in df_trade.columns:
        needs_okx = (df_trade["trading_venue"].astype(str) == "OkexFutures").any()

    if needs_okx:
        okx_multipliers = load_okx_multipliers(OKX_MULTIPLIER_CACHE)
        apply_okx_contract_multipliers(
            df_order,
            okx_multipliers,
            ["quantity", "last_executed_qty", "cumulative_filled_quantity"],
        )
        apply_okx_contract_multipliers(
            df_trade,
            okx_multipliers,
            ["quantity", "cumulative_filled_quantity"],
        )

    close_sig_path = os.path.join(data_dir, "signals_arb_close.parquet")
    if os.path.exists(close_sig_path):
        df_close_sig = pd.read_parquet(close_sig_path)
    else:
        df_close_sig = pd.DataFrame()

    normalize_ts_col(df_open_sig, "create_ts")
    normalize_ts_col(df_hedge_sig, "market_ts")
    if len(df_close_sig) > 0:
        normalize_ts_col(df_close_sig, "create_ts")

    df_trade["strategy_id"] = df_trade["client_order_id"].apply(extract_strategy_id)
    df_order["strategy_id"] = df_order["client_order_id"].apply(extract_strategy_id)

    print(f"trade_updates: {len(df_trade)} rows")
    print(f"order_updates: {len(df_order)} rows")
    print(f"signals_arb_open: {len(df_open_sig)} rows")
    print(f"signals_arb_hedge: {len(df_hedge_sig)} rows")
    print(f"signals_arb_cancel: {len(df_cancel_sig)} rows")
    print(f"signals_arb_close: {len(df_close_sig)} rows")

    if args.all:
        output_dir = os.path.abspath(output_dir)
        symbol_keys = set()
        symbol_keys |= collect_symbol_keys(df_order, ["symbol"])
        symbol_keys |= collect_symbol_keys(df_trade, ["symbol"])
        symbol_keys |= collect_symbol_keys(
            df_open_sig, ["opening_symbol", "hedging_symbol"]
        )
        symbol_keys |= collect_symbol_keys(
            df_hedge_sig, ["opening_symbol", "hedging_symbol"]
        )
        symbol_keys |= collect_symbol_keys(
            df_cancel_sig, ["opening_symbol", "hedging_symbol"]
        )
        if len(df_close_sig) > 0:
            symbol_keys |= collect_symbol_keys(
                df_close_sig, ["opening_symbol", "hedging_symbol"]
            )

        for symbol_key in sorted(symbol_keys):
            output_file = os.path.join(output_dir, f"{symbol_key}_order.parquet")
            export_symbol(
                data_dir,
                df_trade,
                df_order,
                df_open_sig,
                df_hedge_sig,
                df_cancel_sig,
                df_close_sig,
                symbol_key,
                output_file,
            )
        return

    symbol_key = normalize_symbol(args.symbol)
    if args.output:
        output_file = os.path.expanduser(args.output)
        output_parent = os.path.dirname(output_file)
        if output_parent:
            os.makedirs(output_parent, exist_ok=True)
    else:
        output_file = os.path.join(output_dir, f"{symbol_key}_order.parquet")
    export_symbol(
        data_dir,
        df_trade,
        df_order,
        df_open_sig,
        df_hedge_sig,
        df_cancel_sig,
        df_close_sig,
        symbol_key,
        output_file,
    )


if __name__ == "__main__":
    main()
