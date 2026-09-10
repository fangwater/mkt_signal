import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from research.roll_spread.study import (
    StudyParameters,
    _backtest_contract_id,
    _cn_delivery_month,
    _read_pair_seconds,
    normalized_progress_path,
    smooth_seconds_to_minutes,
    summarize_event_trends,
    summarize_market_trends,
)


def test_cn_delivery_month_resolves_three_digit_future_contract() -> None:
    assert _cn_delivery_month("AP601", pd.Timestamp("2025-12-12")) == pd.Timestamp("2026-01-01")


def test_zhengzhou_dominant_identifier_maps_to_backtest_identifier() -> None:
    assert _backtest_contract_id("TA2609", "xzce") == "TA609"
    assert _backtest_contract_id("rb2605", "xsge") == "rb2605"


def test_pair_bbo_spread_uses_exact_second_alignment(tmp_path) -> None:
    path = tmp_path / "pair.parquet"
    table = pa.Table.from_pydict(
        {
            "contract_id": ["old", "old", "old", "new", "new", "new"],
            "ts": [100, 101, 102, 100, 102, 103],
            "bid0p": [99.0, 100.0, 101.0, 98.0, 99.0, 100.0],
            "ask0p": [101.0, 102.0, 103.0, 100.0, 101.0, 102.0],
        }
    )
    pq.write_table(table, path)
    pair = _read_pair_seconds(path, "old", "new")
    assert pair["ts"].tolist() == [100, 102]
    assert pair["mid_spread"].tolist() == [1.0, 2.0]
    assert pair["sell_old_buy_new_spread"].tolist() == [-1.0, 0.0]


def test_smoothing_is_trailing_and_minute_sampled() -> None:
    seconds = pd.DataFrame(
        {
            "ts": np.arange(60),
            "mid_spread": np.arange(60, dtype=float),
            "mid_spread_bps": np.arange(60, dtype=float),
            "sell_old_buy_new_spread": np.arange(60, dtype=float),
            "buy_old_sell_new_spread": np.arange(60, dtype=float),
        }
    )
    minutes = smooth_seconds_to_minutes(
        seconds,
        StudyParameters(smoothing_seconds=60, min_seconds_per_minute=30),
    )
    assert len(minutes) == 1
    assert np.isclose(minutes.loc[0, "mid_spread"], 29.5)
    assert minutes.loc[0, "minute_ts"] == 59


def test_normalized_path_and_trend_preserve_positive_spread_move() -> None:
    minutes = pd.DataFrame(
        {
            "event_id": ["one", "one", "one"],
            "market": ["CN", "CN", "CN"],
            "exchange": ["xsge"] * 3,
            "product": ["RB"] * 3,
            "product_key": ["CN:xsge:RB"] * 3,
            "event_date": [pd.Timestamp("2026-08-14")] * 3,
            "old_contract": ["rb2609"] * 3,
            "new_contract": ["rb2610"] * 3,
            "minute_ts": [0, 60, 120],
            "mid_spread_bps": [1.0, 2.0, 3.0],
            "mid_spread": [1.0, 2.0, 3.0],
        }
    )
    trends = summarize_event_trends(minutes)
    assert trends.loc[0, "product"] == "RB"
    assert trends.loc[0, "slope_bps_per_1000_trading_minutes"] > 0
    assert trends.loc[0, "initial_state"] == "backwardation"
    assert trends.loc[0, "state_continuation_change_bps"] == 2.0
    assert trends.loc[0, "momentum_continuation_change_bps"] == 1.0
    market_summary = summarize_market_trends(trends)
    assert market_summary.loc[0, "positive_state_continuation_share"] == 1.0
    path = normalized_progress_path(minutes, StudyParameters(path_bins=3))
    assert len(path) == 4
    assert path.loc[path["progress_bin"].eq(0), "mean_delta_mid_spread_bps"].item() == 0.0
    assert path.loc[path["progress_bin"].eq(3), "mean_delta_mid_spread_bps"].item() == 2.0
