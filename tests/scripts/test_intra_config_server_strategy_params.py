"""Unit tests for intra_config_server strategy param schema."""

from __future__ import annotations

import os
import sys
import unittest

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

import intra_config_server as intra_cfg  # noqa: E402


class TestStrategyParamsSchema(unittest.TestCase):
    def test_funding_close_switch_is_required_strategy_param(self):
        self.assertIn(
            "enable_intra_funding_close_signal",
            intra_cfg.DEFAULT_STRATEGY_PARAMS,
        )
        self.assertIn(
            "enable_intra_funding_close_signal",
            intra_cfg.STRATEGY_PARAM_COMMENTS,
        )
        self.assertIn(
            "enable_intra_funding_close_signal",
            intra_cfg.STRATEGY_PARAM_ORDER,
        )
        self.assertIn(
            "enable_intra_funding_close_signal",
            intra_cfg.STRATEGY_BOOL_PARAM_KEYS,
        )

    def test_vol_gate_compare_is_required_strategy_param(self):
        self.assertIn("vol_gate_compare", intra_cfg.DEFAULT_STRATEGY_PARAMS)
        self.assertIn("vol_gate_compare", intra_cfg.STRATEGY_PARAM_COMMENTS)
        self.assertIn("vol_gate_compare", intra_cfg.STRATEGY_PARAM_ORDER)

    def test_strategy_bool_params_normalize_on_save(self):
        normalized = intra_cfg.normalize_strategy_params_by_schema(
            {
                "enable_intra_funding_close_signal": "on",
                "enable_tlen_cancel": "0",
                "enable_environment_model": "yes",
                "enable_volatility_limit": "",
                "vol_gate_compare": ">",
            }
        )
        self.assertEqual(normalized["enable_intra_funding_close_signal"], "true")
        self.assertEqual(normalized["enable_tlen_cancel"], "false")
        self.assertEqual(normalized["enable_environment_model"], "true")
        self.assertEqual(normalized["enable_volatility_limit"], "false")
        self.assertEqual(normalized["vol_gate_compare"], "gt")

    def test_strategy_bool_params_reject_invalid_text(self):
        with self.assertRaises(ValueError):
            intra_cfg.normalize_strategy_params_by_schema(
                {"enable_intra_funding_close_signal": "maybe"}
            )

    def test_vol_gate_compare_rejects_invalid_text(self):
        with self.assertRaises(ValueError):
            intra_cfg.normalize_strategy_params_by_schema({"vol_gate_compare": "eq"})


if __name__ == "__main__":
    unittest.main()
