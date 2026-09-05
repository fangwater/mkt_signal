"""Hyperliquid intra config-server routing tests."""

from __future__ import annotations

import os
import sys
import unittest

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

import intra_config_server as intra_cfg  # noqa: E402


class TestHyperliquidRouting(unittest.TestCase):
    def test_exchange_and_default_venues_are_supported(self):
        self.assertIn("hyperliquid", intra_cfg.SUPPORTED_EXCHANGES)
        self.assertEqual(
            intra_cfg.EXCHANGE_DEFAULTS["hyperliquid"],
            ("hyperliquid-margin", "hyperliquid-futures"),
        )
        self.assertEqual(
            intra_cfg.infer_default_venues_from_name("hyperliquid-intra-trade01"),
            ("hyperliquid-margin", "hyperliquid-futures"),
        )
        self.assertEqual(
            intra_cfg.exchange_from_venue("hyperliquid-futures"),
            "hyperliquid",
        )


if __name__ == "__main__":
    unittest.main()
