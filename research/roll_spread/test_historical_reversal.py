from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import numpy as np
import pandas as pd

from research.roll_spread.historical_reversal import (
    HistoricalParameters,
    _binomial_two_sided,
    _path_grid_shape,
    _roll_panel_title,
    aligned_mid_paths,
    build_event_reversal_statistics,
    product_reversal_summary,
    render_product_aligned_mid_paths,
)


def _daily_event(event_id: str, values: list[float]) -> pd.DataFrame:
    rows = []
    for relative_day, value in zip(range(-10, 0), values, strict=True):
        rows.append(
            {
                "event_id": event_id, "market": "CN", "exchange": "xsge", "product": "RB",
                "product_key": "CN:xsge:RB", "event_date": pd.Timestamp("2024-01-10"),
                "old_contract": "rb2401", "new_contract": "rb2405", "relative_day": relative_day,
                "trading_day": pd.Timestamp("2024-01-01") + pd.Timedelta(days=relative_day + 10),
                "pair_seconds": 100, "last_pair_ts": 1000 + relative_day,
                "daily_mid_mean_bps": value,
                "long_entry_cost_bps": value + 2.0,
                "long_liquidation_bps": value - 2.0,
            }
        )
    return pd.DataFrame(rows)


def test_event_statistic_classifies_late_reversal_and_uses_causal_bbo_marks() -> None:
    # Mid rises by 4 bp until t=-6, then falls by 8 bp to t=-1.
    daily = _daily_event("reversal", [0, 1, 2, 3, 4, 3, 2, 1, 0, -4])
    events = build_event_reversal_statistics(daily, HistoricalParameters())
    row = events.iloc[0]
    assert row["outcome"] == "reversal"
    assert row["early_direction"] == "多旧空新"
    assert row["early_mid_change_bps_t10_to_t6"] == 4.0
    assert row["late_mid_change_bps_t6_to_t1"] == -8.0
    assert row["aligned_late_mid_change_bps"] == -8.0
    # Long old/short new enters at t=-6 cost 6 and exits at t=-1 value -6.
    assert row["follow_trend_gross_bbo_pnl_bps"] == -12.0
    assert row["contrarian_gross_bbo_pnl_bps"] == 4.0


def test_product_summary_reports_exact_reversal_test_and_aligned_path() -> None:
    reversal = _daily_event("reversal", [0, 1, 2, 3, 4, 3, 2, 1, 0, -4])
    continuation = _daily_event("continuation", [0, 1, 2, 3, 4, 5, 6, 7, 8, 12])
    continuation["event_date"] = pd.Timestamp("2024-02-10")
    continuation["old_contract"] = "rb2405"
    continuation["new_contract"] = "rb2409"
    event_statistics = build_event_reversal_statistics(pd.concat([reversal, continuation], ignore_index=True))
    summary = product_reversal_summary(event_statistics)
    row = summary.iloc[0]
    assert row["directional_events"] == 2
    assert row["reversal_rate"] == 0.5
    assert row["reversal_pvalue"] == 1.0
    assert 0.0 <= row["aligned_mean_signflip_pvalue"] <= 1.0
    paths = aligned_mid_paths(pd.concat([reversal, continuation], ignore_index=True), event_statistics)
    reversal_path = paths[paths["event_id"].eq("reversal")]
    assert np.isclose(reversal_path.iloc[-1]["aligned_mid_delta_bps"], -4.0)
    assert reversal_path.iloc[0]["old_contract"] == "rb2401"
    assert reversal_path.iloc[0]["new_contract"] == "rb2405"
    assert reversal_path.iloc[0]["outcome"] == "reversal"


def test_exact_two_sided_binomial_matches_symmetric_small_sample_cases() -> None:
    assert _binomial_two_sided(0, 4) == 0.125
    assert _binomial_two_sided(2, 4) == 1.0


def test_path_grid_keeps_eight_columns_and_labels_each_roll() -> None:
    assert _path_grid_shape(14) == (2, 8)
    assert _path_grid_shape(79) == (10, 8)
    assert _path_grid_shape(3) == (1, 3)
    assert _roll_panel_title("2024-01-10", "rb2401", "rb2405") == "2024-01-10\nrb2401->rb2405"


def test_product_path_page_renders_one_labelled_panel_per_roll(tmp_path: Path) -> None:
    reversal = _daily_event("reversal", [0, 1, 2, 3, 4, 3, 2, 1, 0, -4])
    continuation = _daily_event("continuation", [0, 1, 2, 3, 4, 5, 6, 7, 8, 12])
    continuation["event_date"] = pd.Timestamp("2024-02-10")
    continuation["old_contract"] = "rb2405"
    continuation["new_contract"] = "rb2409"
    daily = pd.concat([reversal, continuation], ignore_index=True)
    event_statistics = build_event_reversal_statistics(daily)
    paths = aligned_mid_paths(daily, event_statistics)
    manifest = render_product_aligned_mid_paths(paths, tmp_path)
    assert list(manifest.columns) == ["exchange", "product", "product_name", "image_file", "events", "rolls"]
    assert manifest.iloc[0]["events"] == 2
    assert "2024-01-10 rb2401->rb2405" in manifest.iloc[0]["rolls"]
    assert "2024-02-10 rb2405->rb2409" in manifest.iloc[0]["rolls"]
    image = tmp_path / "xsge_RB_aligned_mid_paths.png"
    assert image.is_file()
    assert image.stat().st_size > 1_000
