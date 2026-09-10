import numpy as np
import pandas as pd

from research.roll_spread.quantile_study import (
    QuantileParameters,
    add_directional_mark_columns,
    daily_spread_distribution,
    event_execution_screen,
    event_daily_quantile_trends,
    minute_spread_distribution,
    normalized_daily_statistic_path,
    product_quantile_summary,
    render_product_mid_spread_rolls,
)


def _seconds() -> pd.DataFrame:
    values = np.arange(60, dtype=float)
    return pd.DataFrame(
        {
            "ts": np.arange(60),
            "mid_spread": values,
            "mid_spread_bps": values,
            "sell_old_buy_new_spread": values,
            "buy_old_sell_new_spread": values,
            "sell_old_buy_new_spread_bps": values,
            "buy_old_sell_new_spread_bps": values,
        }
    )


def test_minute_distribution_uses_empirical_seconds_without_rolling() -> None:
    minutes = minute_spread_distribution(_seconds(), QuantileParameters(min_seconds_per_minute=30))
    assert len(minutes) == 1
    assert minutes.loc[0, "pair_seconds"] == 60
    assert np.isclose(minutes.loc[0, "mid_spread_bps_p10"], 5.9)
    assert np.isclose(minutes.loc[0, "mid_spread_bps_p50"], 29.5)
    assert np.isclose(minutes.loc[0, "mid_spread_bps_p90"], 53.1)


def test_daily_distribution_and_quantile_trends_are_directional(tmp_path) -> None:
    daily_distribution = daily_spread_distribution(_seconds())
    assert daily_distribution["pair_seconds"] == 60
    assert np.isclose(daily_distribution["mid_spread_bps_p50"], 29.5)
    rows = []
    for relative_day, value in zip(range(-10, 0), range(1, 11), strict=True):
        row = {
            "event_id": "one", "market": "CN", "exchange": "ccfx", "product": "IF",
            "product_key": "CN:ccfx:IF", "event_date": pd.Timestamp("2026-06-15"),
            "old_contract": "IF2606", "new_contract": "IF2609", "relative_day": relative_day,
        }
        for quantile in ("p10", "p20", "p50", "p80", "p90"):
            row[f"mid_spread_bps_{quantile}"] = float(value)
            row[f"sell_old_buy_new_spread_bps_{quantile}"] = float(value - 1)
            row[f"buy_old_sell_new_spread_bps_{quantile}"] = float(value + 1)
        row["mid_spread_bps_mean"] = float(value)
        row["sell_old_buy_new_spread_bps_mean"] = float(value - 1)
        row["buy_old_sell_new_spread_bps_mean"] = float(value + 1)
        rows.append(row)
    trends = event_daily_quantile_trends(pd.DataFrame(rows))
    p50 = trends[trends["statistic"].eq("p50")].iloc[0]
    assert p50["state_continuation_bps"] == 9.0
    assert p50["momentum_continuation_bps"] == 4.0
    summary = product_quantile_summary(trends)
    assert summary.loc[summary["statistic"].eq("p50"), "positive_slope_share"].item() == 1.0
    path = normalized_daily_statistic_path(pd.DataFrame(rows))
    p50_path = path[path["statistic"].eq("p50")].sort_values("relative_day")
    assert p50_path.iloc[0]["mean_delta_bps"] == 0.0
    assert p50_path.iloc[-1]["mean_delta_bps"] == 9.0
    execution = event_execution_screen(pd.DataFrame(rows))
    assert execution.loc[0, "signal_direction"] == "long_old_short_new"
    assert execution.loc[0, "gross_p50_bbo_screen_bps"] == 2.0
    directional = add_directional_mark_columns(pd.DataFrame(rows))
    assert directional.loc[0, "long_old_short_new_mark_bps_p10"] == 0.0
    assert directional.loc[0, "short_old_long_new_mark_bps_p10"] == -2.0
    duplicated = pd.concat([pd.DataFrame(rows), pd.DataFrame(rows).assign(event_id="two")], ignore_index=True)
    assert len(event_daily_quantile_trends(duplicated)) == 12
    manifest = render_product_mid_spread_rolls(pd.DataFrame(rows), tmp_path)
    assert manifest.loc[0, "product_name"] == "沪深300股指期货"
    assert (tmp_path / manifest.loc[0, "image_file"]).is_file()
