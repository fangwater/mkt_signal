import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "cancel_okex_pm_orders.py"
spec = importlib.util.spec_from_file_location("cancel_okex_pm_orders", SCRIPT)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class TestCancelOkexPmOrders(unittest.TestCase):
    def test_margin_scope_reads_spot_cross_orders(self):
        def fake_fetch_open(_api_key, _api_secret, _passphrase, inst_type):
            self.assertEqual(inst_type, "SPOT")
            return [
                {
                    "instType": "SPOT",
                    "instId": "ADA-USDT",
                    "ordId": "1",
                    "tdMode": "cross",
                },
                {
                    "instType": "SPOT",
                    "instId": "BTC-USDT",
                    "ordId": "2",
                    "tdMode": "cash",
                },
                {
                    "instType": "SPOT",
                    "instId": "ADA-USDT",
                    "ordId": "3",
                    "tdMode": "isolated",
                },
            ]

        with patch.object(mod, "fetch_open", side_effect=fake_fetch_open):
            swap, margin = mod.collect_orders_to_cancel(
                "key", "secret", "passphrase", "margin", ["ADAUSDT"]
            )

        self.assertEqual(swap, [])
        self.assertEqual(
            margin,
            [
                {"instId": "ADA-USDT", "ordId": "1"},
                {"instId": "ADA-USDT", "ordId": "3"},
            ],
        )


if __name__ == "__main__":
    unittest.main()
