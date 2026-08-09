from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

import pandas as pd


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1] / "scripts" / "analyze_intra_arb_latency.py"
)
SPEC = importlib.util.spec_from_file_location("analyze_intra_arb_latency", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
ANALYZER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ANALYZER)


def full_signal_bbo_frame() -> pd.DataFrame:
    frame = pd.DataFrame(
        {
            "mkt_ts": [900, 900, 900],
            "signal_ts": [1_200, 1_200, 1_200],
            "signal_open_ts": [1_100, 1_000, 1_000],
            "signal_hedge_ts": [1_000, 1_100, 1_000],
        }
    )
    for column in ANALYZER.SIGNAL_BBO_COLUMNS:
        if column not in frame:
            frame[column] = "test"
    return frame


class TriggerMarketTimeTest(unittest.TestCase):
    def test_classifies_dual_bbo_trigger_leg(self) -> None:
        classified = ANALYZER.classify_trigger_market_time(full_signal_bbo_frame())

        self.assertEqual(
            classified["trigger_record_kind"].tolist(),
            ["new_signal_bbo", "new_signal_bbo", "new_signal_bbo"],
        )
        self.assertEqual(classified["trigger_leg"].tolist(), ["spot", "futures", "tie"])
        self.assertEqual(classified["trigger_mkt_ts"].tolist(), [1_100, 1_100, 1_000])
        self.assertEqual(
            classified["signal_minus_trigger_mkt_ms"].tolist(),
            [0.1, 0.1, 0.2],
        )

    def test_legacy_schema_falls_back_to_mkt_ts(self) -> None:
        frame = pd.DataFrame({"mkt_ts": [1_000], "signal_ts": [1_250]})
        classified = ANALYZER.classify_trigger_market_time(frame)

        self.assertEqual(classified.loc[0, "trigger_record_kind"], "legacy_schema")
        self.assertEqual(classified.loc[0, "trigger_leg"], "legacy_unknown")
        self.assertEqual(classified.loc[0, "trigger_mkt_ts"], 1_000)
        self.assertEqual(classified.loc[0, "signal_minus_trigger_mkt_ms"], 0.25)

    def test_empty_signal_bbo_is_ambiguous_and_falls_back(self) -> None:
        frame = full_signal_bbo_frame().iloc[[0]].copy()
        frame[ANALYZER.SIGNAL_BBO_COLUMNS] = pd.NA
        frame["mkt_ts"] = 1_050
        classified = ANALYZER.classify_trigger_market_time(frame)

        self.assertEqual(
            classified.iloc[0]["trigger_record_kind"],
            "legacy_or_empty_signal_bbo",
        )
        self.assertEqual(classified.iloc[0]["trigger_leg"], "legacy_unknown")
        self.assertEqual(classified.iloc[0]["trigger_mkt_ts"], 1_050)

    def test_partial_signal_bbo_schema_is_rejected(self) -> None:
        frame = pd.DataFrame(
            {"mkt_ts": [1_000], "signal_ts": [1_250], "signal_open_ts": [1_100]}
        )
        with self.assertRaisesRegex(ValueError, "incomplete signal_bbo parquet schema"):
            ANALYZER.classify_trigger_market_time(frame)

    def test_metric_summary_preserves_actual_tail(self) -> None:
        frame = pd.DataFrame({"latency_ms": [0.031, 6.0, 294.576]})
        summary = ANALYZER.metric_summary(frame, "latency_ms", 100.0)

        self.assertEqual(summary["normal_rows"], 2)
        self.assertEqual(summary["gt_normal_max_rows"], 1)
        self.assertEqual(summary["max_ms"], 6.0)
        self.assertEqual(summary["actual_max_ms"], 294.576)
        self.assertEqual(summary["gt5_rows"], 2)
        self.assertEqual(summary["gt10_rows"], 1)

    def test_new_dual_bbo_row_does_not_require_legacy_mkt_ts(self) -> None:
        frame = full_signal_bbo_frame().iloc[[0]].copy()
        frame["status"] = "NEW"
        frame["create_ts"] = 1_300
        frame["client_order_id"] = 42
        frame["trading_venue"] = "BinanceMargin"
        frame["mkt_ts"] = 0

        selected = ANALYZER.select_new_orders(frame, "BinanceMargin")

        self.assertEqual(len(selected), 1)
        self.assertEqual(selected.iloc[0]["trigger_record_kind"], "new_signal_bbo")
        self.assertEqual(selected.iloc[0]["trigger_leg"], "spot")
        self.assertTrue(pd.isna(selected.iloc[0]["signal_minus_mkt_ms"]))
        self.assertEqual(selected.iloc[0]["create_minus_signal_ms"], 0.1)


if __name__ == "__main__":
    unittest.main()
