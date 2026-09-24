"""Configuration support for same-side intra spread factors."""

from __future__ import annotations

import os
import sys
import unittest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.join(ROOT, "intra_scripts"))

import intra_config_server as intra_cfg  # noqa: E402
import sync_intra_spread_thresholds as spread_sync  # noqa: E402


class TestSameSideSpreads(unittest.TestCase):
    def test_intra_rolling_defaults_include_same_side_quantiles(self):
        defaults = intra_cfg.build_runtime_rolling_defaults(
            "binance-margin", "binance-futures"
        )
        factors = defaults["factors"]
        self.assertEqual(factors["bidbid_ho"]["quantiles"], [85, 90])
        self.assertEqual(factors["askask_oh"]["quantiles"], [85, 90])

        spot_defaults = intra_cfg.build_runtime_rolling_defaults(
            "binance-spot", "binance-futures"
        )["factors"]
        self.assertEqual(spot_defaults["bidbid_ho"]["quantiles"], [85, 90])

    def test_same_side_mapping_reads_quantiles_with_underscores(self):
        rolling = {
            "BTCUSDT": {
                "bidbid_ho_quantiles": [
                    {"quantile": 0.9, "threshold": 0.006},
                    {"quantile": 0.85, "threshold": 0.004},
                ],
                "askask_oh_quantiles": [
                    {"quantile": 0.9, "threshold": 0.007},
                    {"quantile": 0.85, "threshold": 0.005},
                ],
            }
        }
        rows = spread_sync.generate_spread_thresholds(
            ["BTCUSDT"], rolling, spread_sync.SAME_SIDE_SPREAD_THRESHOLD_MAPPING
        )
        self.assertEqual(rows["BTCUSDT"]["forward_open_mt"], 0.006)
        self.assertEqual(rows["BTCUSDT"]["forward_cancel_mt"], 0.004)
        self.assertEqual(rows["BTCUSDT"]["backward_open_mt"], 0.007)
        self.assertEqual(rows["BTCUSDT"]["backward_cancel_mt"], 0.005)


if __name__ == "__main__":
    unittest.main()
