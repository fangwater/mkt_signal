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
    def test_native_okx_unified_scopes_and_config_symbol_routing(self) -> None:
        with patch.dict(MODULE.os.environ, {"TRADE_ENGINE_EXEC_BACKEND": "native"}, clear=True):
            self.assertEqual(MODULE.exec_markets("okex-futures"), ["okex-futures", "okex-margin"])
            self.assertEqual(MODULE.exec_markets("binance-futures"), ["binance-futures"])
            store = object.__new__(MODULE.ExecConfigStore)
            store.venue = "okex-futures"
            store.markets = MODULE.exec_markets(store.venue)
            self.assertEqual(store._symbol_market("BTC"), "okex-margin")
            self.assertEqual(store._symbol_market("BTCUSDT"), "okex-futures")
            with self.assertRaises(ValueError):
                store._symbol_market("BTCUSD")

    def test_grid_parameters_roundtrip_and_symbol_overrides(self) -> None:
        raw = {**MODULE.DEFAULT_CONFIG, "algorithm": "grid", "targets": {"BTC": 1.0},
               "grid": {"spot": {"orders_per_batch": 2, "maker_timeout_ms": 2000, "vol_multiplier": 0.5, "max_maker_requotes": None},
                        "futures": {"orders_per_batch": 5, "maker_timeout_ms": 9000, "vol_multiplier": 2, "max_maker_requotes": 4}},
               "symbol_overrides": {"ETH": {"algorithm": "grid", "grid": {"spot": {"max_maker_requotes": 4}}}}}
        normalized = MODULE.normalize_exec_config(raw)
        self.assertEqual(normalized["algorithm"], "grid")
        self.assertEqual(normalized["grid"]["spot"]["orders_per_batch"], 2)
        self.assertEqual(normalized["grid"]["futures"]["maker_timeout_ms"], 9000)
        self.assertEqual(normalized["symbol_overrides"]["ETH"]["grid"]["spot"]["max_maker_requotes"], 4)
        self.assertEqual(normalized["grid"]["futures"]["max_maker_requotes"], 4)
        self.assertIsNone(normalized["grid"]["spot"]["max_maker_requotes"])
        self.assertEqual(normalized["grid"]["spot"]["vol_multiplier"], 0.5)
        self.assertEqual(normalized["grid"]["futures"]["vol_multiplier"], 2)
        self.assertEqual(MODULE.normalize_exec_config(normalized), normalized)
        for invalid in ({"spot": {"orders_per_batch": 0}}, {"futures": {"maker_timeout_ms": 0}},
                        {"spot": {"max_maker_requotes": -1}}, {"spot": {"orders_per_batch": True}},
                        {"futures": {"max_maker_requotes": -1}}, {"open_offset_lower": 0.1}, {"vol_stale_ms": 15000}):
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                MODULE.normalize_grid(invalid)

    def test_grid_market_defaults_limits_and_multiplier_validation(self) -> None:
        defaults = MODULE.normalize_grid({"spot": {}, "futures": {}})
        self.assertEqual(defaults["spot"]["max_maker_requotes"], 2)
        self.assertIsNone(defaults["futures"]["max_maker_requotes"])
        self.assertEqual(set(defaults["spot"]), set(defaults["futures"]))
        for market in ("spot", "futures"):
            self.assertEqual(defaults[market]["vol_multiplier"], 1)
            for limit in (None, 0, 2, 4_294_967_294):
                self.assertEqual(MODULE.normalize_grid({market: {"max_maker_requotes": limit}})[market]["max_maker_requotes"], limit)
            for limit in (-1, 1.5, True, 4_294_967_295):
                with self.subTest(market=market, limit=limit), self.assertRaises(ValueError):
                    MODULE.normalize_grid({market: {"max_maker_requotes": limit}})
            for multiplier in (0, -1, True, None, float("nan"), float("inf")):
                with self.subTest(market=market, multiplier=multiplier), self.assertRaises(ValueError):
                    MODULE.normalize_grid({market: {"vol_multiplier": multiplier}})

    def test_grid_account_gate_covers_base_and_symbol_selected_algorithms(self) -> None:
        for config in ({"algorithm": "grid"}, {"algorithm": "batch", "symbol_overrides": {"BTCUSDT": {"algorithm": "grid"}}}):
            for env in ({}, {"BINANCE_ACCOUNT_MODE": "invalid"}):
                with patch.dict(MODULE.os.environ, env, clear=True), self.assertRaises(ValueError):
                    MODULE.validate_grid_account_config("binance-futures", config)
            for env in ({"BINANCE_ACCOUNT_MODE": "STANDARD"}, {"BINANCE_ACCOUNT_MODE": "UNIFIED"}, {"TRADE_ENGINE_EXEC_BACKEND": "rapidx"}, {"TRADE_ENGINE_EXEC_BACKEND_MAP": "binance=rapidx"}):
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
