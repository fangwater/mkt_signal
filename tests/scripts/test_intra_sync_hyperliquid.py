"""Hyperliquid support in the Intra Redis sync helpers."""

from __future__ import annotations

import importlib.util
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
INTRA_SCRIPTS = ROOT / "intra_scripts"


def load_script(name: str):
    path = INTRA_SCRIPTS / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class IntraSyncHyperliquidTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.symbols = load_script("sync_intra_symbol_lists")
        cls.modules = {
            name: load_script(name)
            for name in (
                "sync_intra_risk_params",
                "sync_intra_strategy_params",
                "sync_intra_amount_u",
                "sync_intra_max_pos_u",
                "sync_intra_spread_thresholds",
                "sync_intra_funding_thresholds",
            )
        }

    def test_all_sync_helpers_accept_hyperliquid(self) -> None:
        self.assertIn("hyperliquid", self.symbols.SUPPORTED_EXCHANGES)
        for name, module in self.modules.items():
            with self.subTest(script=name):
                self.assertIn("hyperliquid", module.SUPPORTED_EXCHANGES)
                self.assertEqual(
                    module.infer_exchange_from_name("hyperliquid-intra-trade01"),
                    "hyperliquid",
                )

    def test_hyperliquid_default_symbols_are_canonical_usdc_pairs(self) -> None:
        redis = FakeRedis()
        env_name = "hyperliquid-intra-trade01"
        total = self.symbols.sync_symbol_lists(
            redis,
            "hyperliquid",
            env_name,
            "hyperliquid-margin",
            "hyperliquid-futures",
        )

        fwd_key = f"{env_name}:intra_fwd_trade_symbols:hyperliquid"
        bwd_key = f"{env_name}:intra_bwd_trade_symbols:hyperliquid"
        fwd = json.loads(redis.values[fwd_key])
        bwd = json.loads(redis.values[bwd_key])

        self.assertEqual(total, len(fwd) + len(bwd))
        self.assertIn("BTCUSDC", fwd)
        self.assertNotIn("BTCUSDT", fwd)
        self.assertTrue(all(symbol.endswith("USDC") for symbol in fwd + bwd))

    def test_other_exchange_default_symbols_keep_usdt_quote(self) -> None:
        self.assertEqual(
            self.symbols.symbols_for_exchange(["BTCUSDT", "ETHUSDT"], "binance"),
            ["BTCUSDT", "ETHUSDT"],
        )
        self.assertEqual(
            self.symbols.symbols_for_exchange(["BTC-USDT", "ETH_USDC"], "hyperliquid"),
            ["BTCUSDC", "ETHUSDC"],
        )
        with self.assertRaisesRegex(ValueError, "must use the USDC quote"):
            self.symbols.symbols_for_exchange(["BTCUSD"], "hyperliquid")

    def test_release_build_includes_hyperliquid_monitor(self) -> None:
        script = (ROOT / "scripts" / "build-intra-binaries.sh").read_text(
            encoding="utf-8"
        )
        self.assertIn("--bin hyperliquid_account_monitor", script)
        self.assertIn('"$RELEASE_DIR/hyperliquid_account_monitor"', script)


if __name__ == "__main__":
    unittest.main()
