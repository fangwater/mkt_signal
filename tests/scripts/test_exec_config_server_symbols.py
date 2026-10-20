#!/usr/bin/env python3

import importlib.util
import unittest
from unittest.mock import patch
from pathlib import Path


MODULE_PATH = Path(__file__).resolve().parents[2] / "scripts" / "exec_config_server.py"
SPEC = importlib.util.spec_from_file_location("exec_config_server", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class ExecConfigServerSymbolTests(unittest.TestCase):
    def test_grid_parameters_roundtrip_and_symbol_overrides(self) -> None:
        raw = {**MODULE.DEFAULT_CONFIG, "algorithm": "grid", "targets": {"BTC": 1.0},
               "grid": {"open_buy_vol_scale": [0.2, 0.8], "open_offset_lower": 0.001},
               "symbol_overrides": {"ETH": {"algorithm": "grid", "grid": {"open_sell_vol_scale": [0.1, 0.9]}}}}
        normalized = MODULE.normalize_exec_config(raw)
        self.assertEqual(normalized["algorithm"], "grid")
        self.assertEqual(normalized["grid"]["open_buy_vol_scale"], [0.2, 0.8])
        self.assertEqual(normalized["grid"]["vol_stale_ms"], 15000)
        self.assertEqual(normalized["symbol_overrides"]["ETH"]["grid"]["open_sell_vol_scale"], [0.1, 0.9])
        self.assertEqual(MODULE.normalize_exec_config(normalized), normalized)
        for invalid in ({"open_buy_vol_scale": [1, 0]}, {"open_sell_vol_scale": [True, 1]},
                        {"open_offset_lower": float("nan")}, {"vol_stale_ms": 0}, {"unknown": 1}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                MODULE.normalize_grid(invalid)

    def test_grid_account_gate_covers_base_and_symbol_selected_algorithms(self) -> None:
        for config in ({"algorithm": "grid"}, {"algorithm": "batch", "symbol_overrides": {"BTCUSDT": {"algorithm": "grid"}}}):
            for env in ({}, {"BINANCE_ACCOUNT_MODE": "STANDARD"}):
                with patch.dict(MODULE.os.environ, env, clear=True), self.assertRaises(ValueError):
                    MODULE.validate_grid_account_config("binance-futures", config)
            for env in ({"BINANCE_ACCOUNT_MODE": "UNIFIED"}, {"TRADE_ENGINE_EXEC_BACKEND": "rapidx"}, {"TRADE_ENGINE_EXEC_BACKEND_MAP": "binance=rapidx"}):
                with patch.dict(MODULE.os.environ, env, clear=True):
                    MODULE.validate_grid_account_config("binance-futures", config)
            with patch.dict(MODULE.os.environ, {}, clear=True):
                for venue in ("okex-futures", "bybit-futures", "bitget-futures"):
                    MODULE.validate_grid_account_config(venue, config)
                with self.assertRaises(ValueError):
                    MODULE.validate_grid_account_config("gate-futures", config)
        with patch.dict(MODULE.os.environ, {}, clear=True):
            MODULE.validate_grid_account_config("binance-futures", {"algorithm": "batch"})

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
