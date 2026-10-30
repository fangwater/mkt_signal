"""Full-depth book factors. Each function reads one finished baseline bar frame.

Inputs are the raw columns written by pipeline.depth_book. Bands are cumulative
notional within that many basis points of mid. Ordinary imbalance is
(bid - ask) / (bid + ask). Signed net flows use (bid - ask) / (abs(bid) + abs(ask)),
as corrected by the operator on 2026-10-30.
"""
from __future__ import annotations

import numpy as np
import pandas as pd


def _imb(df: pd.DataFrame, bid: str, ask: str) -> np.ndarray:
    left = df[bid].to_numpy(dtype=np.float64)
    right = df[ask].to_numpy(dtype=np.float64)
    out = np.full(left.shape, np.nan, dtype=np.float64)
    denom = left + right
    good = np.isfinite(left) & np.isfinite(right) & (denom > 0.0)
    out[good] = (left[good] - right[good]) / denom[good]
    return out


def _net_imb(df: pd.DataFrame, bid: str, ask: str) -> np.ndarray:
    left = df[bid].to_numpy(dtype=np.float64)
    right = df[ask].to_numpy(dtype=np.float64)
    out = np.full(left.shape, np.nan, dtype=np.float64)
    scale = np.maximum(np.abs(left), np.abs(right))
    good = np.isfinite(left) & np.isfinite(right) & (scale > 0.0)
    # Scale both sides to avoid overflow; this is (B - A) / (abs(B) + abs(A)).
    bid_scaled = left[good] / scale[good]
    ask_scaled = right[good] / scale[good]
    out[good] = (bid_scaled - ask_scaled) / (np.abs(bid_scaled) + np.abs(ask_scaled))
    return out


def _ratio(df: pd.DataFrame, num: str, den: str) -> np.ndarray:
    top = df[num].to_numpy(dtype=np.float64)
    bottom = df[den].to_numpy(dtype=np.float64)
    out = np.full(top.shape, np.nan, dtype=np.float64)
    good = np.isfinite(top) & np.isfinite(bottom) & (bottom > 0.0)
    out[good] = top[good] / bottom[good]
    return out


def _log_ba(df: pd.DataFrame, bid: str, ask: str) -> np.ndarray:
    left = df[bid].to_numpy(dtype=np.float64)
    right = df[ask].to_numpy(dtype=np.float64)
    out = np.full(left.shape, np.nan, dtype=np.float64)
    good = (left > 0.0) & (right > 0.0)
    out[good] = np.log(left[good] / right[good])
    return out


def depth_imb_bps1(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_notional_bps1", "depth_ask_notional_bps1")


def depth_imb_bps5(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_notional_bps5", "depth_ask_notional_bps5")


def depth_imb_bps10(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_notional_bps10", "depth_ask_notional_bps10")


def depth_imb_bps20(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_notional_bps20", "depth_ask_notional_bps20")


def depth_imb_bps50(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_notional_bps50", "depth_ask_notional_bps50")


def depth_imb_bps100(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_notional_bps100", "depth_ask_notional_bps100")


def depth_imb_all(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_notional_all", "depth_ask_notional_all")


def depth_log_ba_bps5(df: pd.DataFrame) -> np.ndarray:
    return _log_ba(df, "depth_bid_notional_bps5", "depth_ask_notional_bps5")


def depth_log_ba_bps20(df: pd.DataFrame) -> np.ndarray:
    return _log_ba(df, "depth_bid_notional_bps20", "depth_ask_notional_bps20")


def depth_log_ba_bps100(df: pd.DataFrame) -> np.ndarray:
    return _log_ba(df, "depth_bid_notional_bps100", "depth_ask_notional_bps100")


def depth_near_far_bid(df: pd.DataFrame) -> np.ndarray:
    """Share of 50bp bid notional that sits inside 5bp."""
    return _ratio(df, "depth_bid_notional_bps5", "depth_bid_notional_bps50")


def depth_near_far_ask(df: pd.DataFrame) -> np.ndarray:
    return _ratio(df, "depth_ask_notional_bps5", "depth_ask_notional_bps50")


def depth_far_share_bid(df: pd.DataFrame) -> np.ndarray:
    """Share of 100bp bid notional that sits outside 10bp."""
    near = df["depth_bid_notional_bps10"].to_numpy(dtype=np.float64)
    far = df["depth_bid_notional_bps100"].to_numpy(dtype=np.float64)
    out = np.full(near.shape, np.nan, dtype=np.float64)
    good = np.isfinite(near) & (far > 0.0)
    out[good] = (far[good] - near[good]) / far[good]
    return out


def depth_far_share_ask(df: pd.DataFrame) -> np.ndarray:
    near = df["depth_ask_notional_bps10"].to_numpy(dtype=np.float64)
    far = df["depth_ask_notional_bps100"].to_numpy(dtype=np.float64)
    out = np.full(near.shape, np.nan, dtype=np.float64)
    good = np.isfinite(near) & (far > 0.0)
    out[good] = (far[good] - near[good]) / far[good]
    return out


def depth_gap_bps_10k(df: pd.DataFrame) -> np.ndarray:
    return (
        df["depth_ask_bps_to_10k"].to_numpy(dtype=np.float64)
        - df["depth_bid_bps_to_10k"].to_numpy(dtype=np.float64)
    )


def depth_gap_bps_50k(df: pd.DataFrame) -> np.ndarray:
    return (
        df["depth_ask_bps_to_50k"].to_numpy(dtype=np.float64)
        - df["depth_bid_bps_to_50k"].to_numpy(dtype=np.float64)
    )


def depth_gap_bps_100k(df: pd.DataFrame) -> np.ndarray:
    return (
        df["depth_ask_bps_to_100k"].to_numpy(dtype=np.float64)
        - df["depth_bid_bps_to_100k"].to_numpy(dtype=np.float64)
    )


def depth_flow_add_imb(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_add_notional", "depth_ask_add_notional")


def depth_flow_sub_imb(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_bid_sub_notional", "depth_ask_sub_notional")


def depth_flow_net(df: pd.DataFrame) -> np.ndarray:
    bid = df["depth_bid_add_notional"] - df["depth_bid_sub_notional"]
    ask = df["depth_ask_add_notional"] - df["depth_ask_sub_notional"]
    work = pd.DataFrame({"bid": bid, "ask": ask})
    return _net_imb(work, "bid", "ask")


def depth_touch_add_imb(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_touch_bid_add_notional", "depth_touch_ask_add_notional")


def depth_touch_net(df: pd.DataFrame) -> np.ndarray:
    bid = df["depth_touch_bid_add_notional"] - df["depth_touch_bid_sub_notional"]
    ask = df["depth_touch_ask_add_notional"] - df["depth_touch_ask_sub_notional"]
    work = pd.DataFrame({"bid": bid, "ask": ask})
    return _net_imb(work, "bid", "ask")


def depth_level_imb(df: pd.DataFrame) -> np.ndarray:
    return _imb(df, "depth_n_bid", "depth_n_ask")


DEPTH_FACTOR_NAMES = (
    "depth_imb_bps1",
    "depth_imb_bps5",
    "depth_imb_bps10",
    "depth_imb_bps20",
    "depth_imb_bps50",
    "depth_imb_bps100",
    "depth_imb_all",
    "depth_log_ba_bps5",
    "depth_log_ba_bps20",
    "depth_log_ba_bps100",
    "depth_near_far_bid",
    "depth_near_far_ask",
    "depth_far_share_bid",
    "depth_far_share_ask",
    "depth_gap_bps_10k",
    "depth_gap_bps_50k",
    "depth_gap_bps_100k",
    "depth_flow_add_imb",
    "depth_flow_sub_imb",
    "depth_flow_net",
    "depth_touch_add_imb",
    "depth_touch_net",
    "depth_level_imb",
)


def depth_factor_functions():
    return [globals()[name] for name in DEPTH_FACTOR_NAMES]
