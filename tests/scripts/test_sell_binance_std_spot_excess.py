import importlib.util
import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "sell_binance_std_spot_excess.py"
spec = importlib.util.spec_from_file_location("sell_binance_std_spot_excess", SCRIPT)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class TestSellBinanceStdSpotExcess(unittest.TestCase):
    def test_builds_forced_spot_excess_command(self):
        args = mod.parse_args(
            [
                "--symbol",
                "btcusdt",
                "--symbol",
                "ETHUSDT",
                "--recv-window",
                "7000",
                "--execute",
            ]
        )

        cmd = mod.build_command(
            args,
            script_dir=Path("/repo/scripts"),
            python_bin="python3",
        )

        self.assertEqual(cmd[0:2], ["python3", "/repo/scripts/flatten_binance_std.py"])
        self.assertIn("spot-excess", cmd)
        self.assertNotIn("--skip-um", cmd)
        self.assertEqual(cmd.count("--symbol"), 2)
        self.assertEqual(cmd[-1], "--execute")

    def test_requires_an_explicit_symbol(self):
        with self.assertRaisesRegex(SystemExit, "至少需要一个 --symbol"):
            mod.parse_args([])

    def test_rejects_non_usdt_symbol(self):
        with self.assertRaisesRegex(SystemExit, "完整 USDT 交易对"):
            mod.parse_args(["--symbol", "BTC"])


if __name__ == "__main__":
    unittest.main()
