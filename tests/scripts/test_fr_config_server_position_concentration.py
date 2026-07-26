"""Unit tests for FR position-concentration risk parameter validation."""

from __future__ import annotations

import os
import sys
import unittest

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

import fr_config_server as fr_cfg  # noqa: E402


class TestFrPositionConcentrationRatios(unittest.TestCase):
    def test_accepts_valid_ratios(self):
        normalized = fr_cfg.normalize_fr_position_concentration_ratios(
            {
                "fr_position_concentration_alert_ratio": "0.10",
                "fr_position_concentration_dump_ratio": "0.12",
            }
        )
        self.assertEqual(normalized["fr_position_concentration_alert_ratio"], "0.1")
        self.assertEqual(normalized["fr_position_concentration_dump_ratio"], "0.12")

    def test_rejects_invalid_ratio_order_or_range(self):
        for alert_ratio, dump_ratio in (("0.12", "0.10"), ("0", "0.12"), ("0.10", "1.01")):
            with self.subTest(alert_ratio=alert_ratio, dump_ratio=dump_ratio):
                with self.assertRaisesRegex(ValueError, "0 < alert_ratio < dump_ratio <= 1"):
                    fr_cfg.normalize_fr_position_concentration_ratios(
                        {
                            "fr_position_concentration_alert_ratio": alert_ratio,
                            "fr_position_concentration_dump_ratio": dump_ratio,
                        }
                    )

    def test_requires_both_fields_together(self):
        with self.assertRaisesRegex(ValueError, "must be provided together"):
            fr_cfg.normalize_fr_position_concentration_ratios(
                {"fr_position_concentration_alert_ratio": "0.10"}
            )


if __name__ == "__main__":
    unittest.main()
