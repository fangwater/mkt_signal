#!/usr/bin/env python3

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "exec_config_server.py"
SPEC = importlib.util.spec_from_file_location("exec_config_server", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExecConfigServerSymbolTests(unittest.TestCase):
    def test_normalizes_official_unicode_symbol(self) -> None:
        self.assertEqual(MODULE.normalize_symbol("龙虾usdt"), "龙虾USDT")

    def test_normalizes_unicode_target_and_override_keys(self) -> None:
        targets = MODULE.normalize_targets({"龙虾USDT": {"qty": 1.5, "signal": 1}})
        self.assertEqual(targets["龙虾USDT"], {"qty": 1.5, "signal": 1})

        overrides = MODULE.normalize_symbol_overrides(
            {"龙虾USDT": {"single_order_usdt": 250.0}}
        )
        self.assertEqual(overrides["龙虾USDT"]["single_order_usdt"], 250.0)

    def test_rejects_separators_and_whitespace(self) -> None:
        for symbol in ("BTC-USDT", "龙虾 USDT", "BTC_USDT", ""):
            with self.subTest(symbol=symbol):
                with self.assertRaises(ValueError):
                    MODULE.normalize_symbol(symbol)


if __name__ == "__main__":
    unittest.main()
