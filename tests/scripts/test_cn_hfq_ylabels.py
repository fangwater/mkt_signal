import importlib.util
from pathlib import Path
import unittest

import numpy as np
import pandas as pd


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "rebuild_cn_hfq_ylabels.py"
spec = importlib.util.spec_from_file_location("cn_hfq_ylabels", SCRIPT)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def frame(prices):
    return pd.DataFrame({
        "contract_id": ["RB"] * len(prices),
        "ts": np.arange(len(prices), dtype=np.int64) * 60,
        "mid_price": prices, "vwap": prices, "twap": prices,
    })


class LabelsTest(unittest.TestCase):
    def test_execution_prices_and_session_gap(self):
        source = frame([100.0, 110.0, 121.0, 145.2])
        source.loc[2:, "ts"] += 86400
        result = module.build_labels(source)
        self.assertEqual(list(result.columns), module.KEYS + module.LABEL_COLUMNS)
        self.assertTrue(result[module.KEYS].equals(source[module.KEYS]))
        self.assertAlmostEqual(result.midp_chg_1m.iloc[1], 0.1)
        self.assertAlmostEqual(result.vwap_chg_1m.iloc[1], 0.2)
        self.assertTrue(pd.isna(result.vwap_chg_1m.iloc[-2]))

    def test_zero_negative_and_missing_prices(self):
        result = module.build_labels(frame([-100.0, -110.0, 0.0, 110.0, np.nan, 120.0]))
        self.assertAlmostEqual(result.midp_chg_1m.iloc[0], 0.1)
        self.assertEqual(result.midp_chg_1m.iloc[1], -1.0)
        self.assertTrue(pd.isna(result.midp_chg_1m.iloc[2]))
        self.assertTrue(pd.isna(result.midp_dir_1m.iloc[2]))
        self.assertTrue(pd.isna(result.midp_chg_1m.iloc[3]))

    def test_overlapping_future_returns_sample_std_and_tie_rank(self):
        prices = np.full(1500, 100.0)
        prices[1450:] = 105.0
        result = module.build_labels(frame(prices))
        i = 1440
        returns = prices[1:] / prices[:-1] - 1
        self.assertAlmostEqual(result.midp_vol30_1m.iloc[i], np.std(returns[i-29:i+1], ddof=1))
        window = returns[i-1439:i+1]
        value = returns[i]
        expected = 1 + (window < value).sum() + ((window == value).sum() - 1) / 2
        self.assertEqual(result.midp_re_1m.iloc[i], expected)
        self.assertTrue(pd.isna(result.midp_re_1m.iloc[1438]))
        missing = frame(prices)
        missing.loc[1435, "mid_price"] = np.nan
        self.assertTrue(pd.isna(module.build_labels(missing).midp_re_1m.iloc[i]))


if __name__ == "__main__":
    unittest.main()
