import contextlib
import importlib.util
import io
import json
import pathlib
import unittest


SCRIPT_PATH = pathlib.Path(__file__).resolve().parents[1] / "okx_swap_open_orders.py"
SPEC = importlib.util.spec_from_file_location("okx_swap_open_orders", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class ExecStartupCancelTests(unittest.TestCase):
    def test_okx_item_failure_fails_startup_gate(self):
        original = MODULE.request_okx_private
        MODULE.request_okx_private = lambda **_kwargs: (
            200,
            json.dumps(
                {
                    "code": "0",
                    "msg": "",
                    "data": [
                        {
                            "instId": "BTC-USDT-SWAP",
                            "ordId": "1",
                            "sCode": "51000",
                            "sMsg": "cancel failed",
                        }
                    ],
                }
            ),
            {},
        )
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                ok = MODULE.cancel_orders(
                    "https://www.okx.com",
                    "key",
                    "secret",
                    "passphrase",
                    [{"instId": "BTC-USDT-SWAP", "ordId": "1"}],
                    10,
                    False,
                )
            self.assertFalse(ok)
        finally:
            MODULE.request_okx_private = original


if __name__ == "__main__":
    unittest.main()
