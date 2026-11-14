import importlib.util
from pathlib import Path
import sys
import unittest

import numpy as np
import pandas as pd

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
spec = importlib.util.spec_from_file_location("cn_hfq_5min", SCRIPTS / "build_cn_hfq_5min.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture(times):
    n = len(times)
    values = {key: np.ones(n) for key in module.EXPECTED_COLUMNS if key not in module.KEYS}
    values.update(contract_id=["RB"] * n, ts=np.array(times, dtype=np.int64))
    values.update(open=np.arange(n) + 100., high=np.arange(n) + 102.,
                  low=np.arange(n) + 99., close=np.arange(n) + 101.,
                  vwap=np.arange(n) + 100., buy_vwap=np.arange(n) + 100.,
                  sell_vwap=np.arange(n) + 100., twap=np.arange(n) + 100.)
    return pd.DataFrame(values)


class BarsTest(unittest.TestCase):
    def test_15min_segment_anchor_and_short_tail(self):
        start = 23 * 3600 + 58 * 60
        times = [start + i * 60 for i in range(17)] + [start + 40 * 60 + i * 60 for i in range(16)]
        bars, metadata = module.aggregate_minutes(fixture(times), bar_minutes=15)
        self.assertEqual(bars.ts.tolist(), [times[i] for i in [0, 15, 17, 32]])
        self.assertEqual(metadata["bar_minutes"], 15)
        self.assertEqual(metadata["source_minutes"], [15, 2, 15, 1])
        self.assertEqual(metadata["segment_start_ts"], [times[0], times[17]])
        self.assertEqual(metadata["end_ts"], [times[i] + 60 for i in [14, 16, 31, 32]])

    def test_segment_anchor_midnight_gaps_and_partial_tail(self):
        # The first segment crosses midnight without a split. An intraday gap
        # creates a fresh anchor, even when that anchor is not a clock multiple.
        start = 23 * 3600 + 58 * 60
        times = [start + i * 60 for i in range(7)] + [start + 30 * 60 + i * 60 for i in range(6)]
        bars, metadata = module.aggregate_minutes(fixture(times))
        self.assertEqual(bars.ts.tolist(), [times[i] for i in [0, 5, 7, 12]])
        self.assertEqual(metadata["source_minutes"], [5, 2, 5, 1])
        self.assertEqual(metadata["segment_start_ts"], [times[0], times[7]])
        self.assertEqual(metadata["end_ts"], [times[i] + 60 for i in [4, 6, 11, 12]])

    def test_trade_only_ohlc_weighted_hfq_vwap_and_same_snapshot_book(self):
        source = fixture([i * 60 for i in range(5)])
        source["count"] = [0., 1., 2., 0., 0.]
        source["volume"] = [0., 1., 3., 0., 0.]
        source["amount"] = [0., 20., 90., 0., 0.]
        source["vwap"] = [np.nan, 120., 130., 120., 120.]
        source["open"] = [500., 121., 131., 500., 500.]
        source["high"] = [500., 124., 134., 500., 500.]
        source["low"] = [-500., 119., 129., -500., -500.]
        source["close"] = [500., 122., 132., 500., 500.]
        source["bid1p"] = [101., 102., 103., 104., np.nan]
        source["bid0p"] = [100., 101., 102., 103., 104.]
        source["buy_amount"] = [0., 80., 10., 0., 0.]
        source["sell_amount"] = [0., 10., 10., 0., 0.]
        bars, _ = module.aggregate_minutes(source)
        row = bars.iloc[0]
        self.assertEqual((row.open, row.high, row.low, row.close), (121., 134., 119., 132.))
        self.assertEqual(row.volume, 4.)
        self.assertAlmostEqual(row.avg_amount, 110. / 3.)
        self.assertEqual(row.vwap, 127.5)  # amount/volume would incorrectly give 27.5
        self.assertAlmostEqual(row.net_buy_pct, 70. / 110.)
        self.assertEqual(row.bid0p, 104.)
        self.assertTrue(np.isnan(row.bid1p))  # no per-column backfilling

    def test_empty_bar_and_unknown_twap(self):
        source = fixture([i * 60 for i in range(7)])
        source.loc[:4, "twap"] = [np.nan, 100., 101., 102., 103.]
        source.loc[5:, ["volume", "count", "amount", "buy_volume", "sell_volume"]] = 0.
        source.loc[5:, "close"] = 200.
        source.loc[5:, "vwap"] = 190.
        bars, _ = module.aggregate_minutes(source)
        self.assertTrue(np.isnan(bars.twap.iloc[0]))
        self.assertEqual(bars.vwap.iloc[1], 190.)
        self.assertEqual(bars.iloc[1][["open", "high", "low", "close"]].tolist(), [200.] * 4)
        self.assertEqual(bars.avg_amount.iloc[1], 0.)

    def test_labels_use_bar_counts_and_next_bar_execution_quotes(self):
        source = fixture([0, 300, 600, 86400])
        source["mid_price"] = [100., 110., 121., 145.2]
        source["vwap"] = [10., 20., 40., 100.]
        source["twap"] = source.vwap
        result = module.build_labels(source, bar_minutes=5)
        self.assertEqual(list(result.columns), module.KEYS + module.label_columns(5))
        self.assertAlmostEqual(result.midp_chg_5m.iloc[1], .1)
        self.assertAlmostEqual(result.vwap_chg_5m.iloc[1], 1.5)
        self.assertTrue(np.isnan(result.vwap_chg_5m.iloc[-2]))
        self.assertIn("midp_re_7200m", result.columns)

    def test_15min_labels_keep_row_horizons_across_gaps(self):
        source = fixture([0, 900, 1800, 86400, 87300, 88200, 89100, 90000])
        source["mid_price"] = 2. ** np.arange(8)
        source["vwap"] = 3. ** np.arange(8)
        source["twap"] = source.vwap
        result = module.build_labels(source, bar_minutes=15)
        self.assertEqual(list(result.columns), module.label_schema(15).names)
        self.assertAlmostEqual(result.midp_chg_75m.iloc[0], 31.)
        self.assertAlmostEqual(result.vwap_chg_75m.iloc[0], 242.)
        self.assertAlmostEqual(result.midp_chg_15m.iloc[2], 1.)
        self.assertTrue(np.isnan(result.vwap_chg_15m.iloc[-2]))
        self.assertIn("midp_re_21600m", result.columns)


if __name__ == "__main__":
    unittest.main()
