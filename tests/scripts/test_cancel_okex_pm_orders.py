import importlib.util
import json
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
    def test_private_request_sets_explicit_user_agent(self):
        with patch.object(mod, "http_request", return_value=(200, '{"code":"0"}')) as request:
            mod.okx_private(
                "GET",
                mod.OKX_ORDERS_PENDING_PATH,
                "key",
                "secret",
                "passphrase",
                params={"instType": "SWAP"},
            )

        headers = request.call_args.kwargs["headers"]
        self.assertEqual(headers["User-Agent"], mod.USER_AGENT)

    def test_fetch_open_fails_closed_on_http_error(self):
        with patch.object(mod, "okx_private", return_value=(403, "error code: 1010")):
            with self.assertRaisesRegex(mod.OkxQueryError, "status=403"):
                mod.fetch_open("key", "secret", "passphrase", "SWAP")

    def test_fetch_open_fails_closed_on_invalid_json(self):
        with patch.object(mod, "okx_private", return_value=(200, "not-json")):
            with self.assertRaisesRegex(mod.OkxQueryError, "invalid JSON"):
                mod.fetch_open("key", "secret", "passphrase", "SWAP")

    def test_fetch_open_fails_closed_on_okx_error(self):
        body = json.dumps({"code": "50113", "msg": "Invalid Sign", "data": []})
        with patch.object(mod, "okx_private", return_value=(200, body)):
            with self.assertRaisesRegex(mod.OkxQueryError, "failed"):
                mod.fetch_open("key", "secret", "passphrase", "SWAP")

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
