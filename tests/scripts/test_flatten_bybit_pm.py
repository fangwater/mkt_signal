import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "flatten_bybit_pm.py"
spec = importlib.util.spec_from_file_location("flatten_bybit_pm", SCRIPT)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class FakeResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def getcode(self):
        return 200

    def read(self):
        return b'{"retCode":0}'


class FakeOpener:
    def __init__(self):
        self.calls = []

    def open(self, request, *, timeout):
        self.calls.append((request, timeout))
        return FakeResponse()


class TestFlattenBybitPmSourceAddress(unittest.TestCase):
    def tearDown(self):
        mod._HTTP_OPENER = None

    def test_loads_first_trade_engine_local_ip(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "trade_engine.toml"
            config.write_text(
                'local_ips = ["172.31.7.124", "172.31.7.124"]\n',
                encoding="utf-8",
            )

            local_address, source = mod.load_trade_engine_local_address(tmp)

        self.assertEqual(local_address, "172.31.7.124")
        self.assertEqual(source, config)

    def test_supports_legacy_spaced_config_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "trade engine.toml"
            config.write_text('local_ips = ["10.0.0.4"]\n', encoding="utf-8")

            local_address, source = mod.load_trade_engine_local_address(tmp)

        self.assertEqual(local_address, "10.0.0.4")
        self.assertEqual(source, config)

    def test_rejects_unspecified_source_address(self):
        with tempfile.TemporaryDirectory() as tmp:
            config = Path(tmp) / "trade_engine.toml"
            config.write_text('local_ips = ["0.0.0.0"]\n', encoding="utf-8")

            with self.assertRaisesRegex(SystemExit, "cannot be 0.0.0.0"):
                mod.load_trade_engine_local_address(tmp)

    def test_configures_both_http_handlers_with_source_address(self):
        mod.configure_http_source("172.31.7.124")

        source_handlers = [
            handler
            for handler in mod._HTTP_OPENER.handlers
            if isinstance(
                handler,
                (mod.SourceAddressHTTPHandler, mod.SourceAddressHTTPSHandler),
            )
        ]
        self.assertEqual(len(source_handlers), 2)
        self.assertEqual(
            [handler._source_address for handler in source_handlers],
            [("172.31.7.124", 0), ("172.31.7.124", 0)],
        )

    def test_http_request_uses_configured_opener(self):
        opener = FakeOpener()
        mod._HTTP_OPENER = opener

        status, body = mod.http_request("https://api.bybit.test/v5/market/time", timeout=9)

        self.assertEqual((status, body), (200, '{"retCode":0}'))
        self.assertEqual(len(opener.calls), 1)
        request, timeout = opener.calls[0]
        self.assertEqual(request.full_url, "https://api.bybit.test/v5/market/time")
        self.assertEqual(timeout, 9)


if __name__ == "__main__":
    unittest.main()
