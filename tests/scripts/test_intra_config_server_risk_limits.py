"""Unit tests for intra_config_server risk-limit validation."""

from __future__ import annotations

import os
import sys
import unittest

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

import intra_config_server as intra_cfg  # noqa: E402


class TestIntraRiskLimits(unittest.TestCase):
    def test_binance_hedge_10s_limit_accepts_300(self):
        normalized = intra_cfg.normalize_intra_risk_limits(
            "binance",
            {"arb_hedge_order_rate_limit_10s": "300.0"},
        )
        self.assertEqual(normalized["arb_hedge_order_rate_limit_10s"], "300")

    def test_binance_hedge_10s_limit_rejects_values_below_300(self):
        with self.assertRaisesRegex(ValueError, "save rejected"):
            intra_cfg.normalize_intra_risk_limits(
                "binance",
                {"arb_hedge_order_rate_limit_10s": "299"},
            )

    def test_binance_hedge_10s_limit_rejects_values_above_300(self):
        with self.assertRaisesRegex(ValueError, "save rejected"):
            intra_cfg.normalize_intra_risk_limits(
                "binance",
                {"arb_hedge_order_rate_limit_10s": "500"},
            )

    def test_other_exchanges_are_unchanged(self):
        mapping = {"arb_hedge_order_rate_limit_10s": "250"}
        self.assertEqual(
            intra_cfg.normalize_intra_risk_limits("gate", mapping),
            mapping,
        )


if __name__ == "__main__":
    unittest.main()
