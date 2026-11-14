from __future__ import annotations

import base64
import hashlib
import hmac
import importlib.util
import json
import sys
import unittest
import urllib.parse
from pathlib import Path
from typing import Mapping, Optional
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "set_delta_neutral_mode.py"
spec = importlib.util.spec_from_file_location("set_delta_neutral_mode", SCRIPT)
delta_mode = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = delta_mode
spec.loader.exec_module(delta_mode)


class StubTransport:
    def __init__(self, *responses: delta_mode.HttpResponse) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, Mapping[str, str], Optional[bytes], int]] = []

    def __call__(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        data: Optional[bytes],
        timeout: int,
    ) -> delta_mode.HttpResponse:
        self.calls.append((method, url, dict(headers), data, timeout))
        if not self.responses:
            raise AssertionError("unexpected HTTP call")
        return self.responses.pop(0)


def response(value: object, status: int = 200) -> delta_mode.HttpResponse:
    return delta_mode.HttpResponse(status, json.dumps(value, separators=(",", ":")))


class DeltaNeutralModeTests(unittest.TestCase):
    def test_binance_query_and_signed_set(self) -> None:
        transport = StubTransport(
            response({"deltaEnabled": False}),
            response({"msg": "success"}),
        )
        client = delta_mode.BinanceClient(
            "key", "secret", "https://api.binance.test", 9, transport
        )

        self.assertFalse(client.query().enabled)
        with mock.patch.object(delta_mode, "now_ms", return_value=1234567890000):
            client.set_enabled(True)

        method, url, headers, data, timeout = transport.calls[1]
        self.assertEqual((method, url, timeout), (
            "POST",
            "https://api.binance.test/sapi/v1/portfolio/delta-mode",
            9,
        ))
        self.assertEqual(headers["X-MBX-APIKEY"], "key")
        assert data is not None
        fields = urllib.parse.parse_qs(data.decode("utf-8"), keep_blank_values=True)
        signature = fields.pop("signature")[0]
        unsigned = urllib.parse.urlencode(
            sorted((key, values[0]) for key, values in fields.items()), safe="-_.~"
        )
        expected = hmac.new(b"secret", unsigned.encode("utf-8"), hashlib.sha256).hexdigest()
        self.assertEqual(signature, expected)
        self.assertEqual(fields["deltaEnabled"], ["true"])

    def test_okx_query_and_set_uses_current_strategy_contract(self) -> None:
        transport = StubTransport(
            response({"code": "0", "data": [{"acctLv": "4", "stgyType": "0"}]}),
            response({"code": "0", "data": [{"type": "stgyType", "stgyType": "1"}]}),
        )
        client = delta_mode.OkxClient(
            "key", "secret", "pass", "https://okx.test", 10, True, transport
        )

        state = client.query()
        self.assertFalse(state.enabled)
        self.assertIn("acctLv=4", state.detail)
        with mock.patch.object(
            delta_mode, "utc_timestamp_iso_ms", return_value="2026-09-02T00:00:00.000Z"
        ):
            client.set_enabled(True)

        method, url, headers, data, _ = transport.calls[1]
        self.assertEqual(method, "POST")
        self.assertEqual(url, "https://okx.test/api/v5/account/set-trading-config")
        self.assertEqual(headers["x-simulated-trading"], "1")
        self.assertEqual(data, b'{"type":"stgyType","stgyType":"1"}')
        raw = (
            "2026-09-02T00:00:00.000ZPOST/api/v5/account/set-trading-config"
            '{"type":"stgyType","stgyType":"1"}'
        )
        expected = base64.b64encode(
            hmac.new(b"secret", raw.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
        self.assertEqual(headers["OK-ACCESS-SIGN"], expected)

    def test_gate_query_and_set_uses_boolean_body(self) -> None:
        transport = StubTransport(response({"enabled": False}), response({"enabled": True}))
        client = delta_mode.GateClient(
            "key", "secret", "https://gate.test", 11, transport
        )

        self.assertFalse(client.query().enabled)
        with mock.patch.object(delta_mode.time, "time", return_value=1234567890):
            client.set_enabled(True)

        method, url, headers, data, _ = transport.calls[1]
        self.assertEqual((method, url), (
            "POST",
            "https://gate.test/api/v4/unified/delta_neutral",
        ))
        self.assertEqual(data, b'{"enabled":true}')
        body_hash = hashlib.sha512(data).hexdigest()
        raw = f"POST\n/api/v4/unified/delta_neutral\n\n{body_hash}\n1234567890"
        expected = hmac.new(b"secret", raw.encode("utf-8"), hashlib.sha512).hexdigest()
        self.assertEqual(headers["SIGN"], expected)

    def test_bybit_query_and_set_uses_delta_enable(self) -> None:
        transport = StubTransport(
            response({"retCode": 0, "retMsg": "OK", "result": {"deltaEnable": False}}),
            response({"retCode": 0, "retMsg": "OK", "result": {}}),
        )
        client = delta_mode.BybitClient(
            "key", "secret", "https://bybit.test", 12, transport
        )

        self.assertFalse(client.query().enabled)
        with mock.patch.object(delta_mode, "now_ms", return_value=1234567890000):
            client.set_enabled(True)

        method, url, headers, data, _ = transport.calls[1]
        self.assertEqual((method, url), (
            "POST",
            "https://bybit.test/v5/account/set-delta-mode",
        ))
        self.assertEqual(data, b'{"deltaEnable":"1"}')
        raw = '1234567890000key5000{"deltaEnable":"1"}'
        expected = hmac.new(b"secret", raw.encode("utf-8"), hashlib.sha256).hexdigest()
        self.assertEqual(headers["X-BAPI-SIGN"], expected)

    def test_bitget_query_falls_back_to_delta_info_and_uses_new_switch(self) -> None:
        transport = StubTransport(
            response(
                {
                    "code": "00000",
                    "msg": "success",
                    "data": {"accountMode": "unified", "accountLevel": "advanced"},
                }
            ),
            response(
                {"code": "40000", "msg": "delta switch is not enabled"},
                status=400,
            ),
            response({"code": "00000", "msg": "success", "data": None}),
        )
        client = delta_mode.BitgetClient(
            "key", "secret", "pass", "https://bitget.test", 13, transport=transport
        )

        self.assertFalse(client.query().enabled)
        with mock.patch.object(delta_mode, "now_ms", return_value=1234567890000):
            client.set_enabled(True)

        method, url, headers, data, _ = transport.calls[2]
        self.assertEqual((method, url), (
            "POST",
            "https://bitget.test/api/v3/account/adjust-account-mode",
        ))
        self.assertEqual(data, b'{"mode":"advanced","deltaSwitch":"yes"}')
        raw = (
            "1234567890000POST/api/v3/account/adjust-account-mode"
            '{"mode":"advanced","deltaSwitch":"yes"}'
        )
        expected = base64.b64encode(
            hmac.new(b"secret", raw.encode("utf-8"), hashlib.sha256).digest()
        ).decode("utf-8")
        self.assertEqual(headers["ACCESS-SIGN"], expected)

    def test_bitget_target_uid_is_in_mutation_and_skips_query(self) -> None:
        transport = StubTransport(response({"code": "00000", "msg": "success", "data": None}))
        client = delta_mode.BitgetClient(
            "key",
            "secret",
            "pass",
            "https://bitget.test",
            10,
            target_uid="9988",
            transport=transport,
        )

        self.assertIsNone(client.query().enabled)
        client.set_enabled(False)

        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(
            transport.calls[0][3],
            b'{"mode":"advanced","deltaSwitch":"no","targetUid":"9988"}',
        )

    def test_bitget_transient_delta_info_error_is_not_treated_as_disabled(self) -> None:
        transport = StubTransport(
            response(
                {
                    "code": "00000",
                    "data": {"accountMode": "unified", "accountLevel": "advanced"},
                }
            ),
            response({"code": "45001", "msg": "service upgrade"}),
        )
        client = delta_mode.BitgetClient(
            "key", "secret", "pass", "https://bitget.test", 10, transport=transport
        )

        with self.assertRaisesRegex(delta_mode.ApiError, "45001"):
            client.query()

    def test_network_error_does_not_print_signed_query(self) -> None:
        with mock.patch.object(
            delta_mode.urllib.request,
            "urlopen",
            side_effect=delta_mode.urllib.error.URLError("unreachable"),
        ):
            with self.assertRaises(delta_mode.ApiError) as caught:
                delta_mode.http_request(
                    "GET",
                    "https://api.test/path?timestamp=1&signature=secret-signature",
                    {},
                    None,
                    10,
                )

        message = str(caught.exception)
        self.assertIn("https://api.test/path", message)
        self.assertNotIn("signature", message)
        self.assertNotIn("secret-signature", message)

    def test_default_dry_run_makes_no_api_request(self) -> None:
        transport = StubTransport()
        args = delta_mode.parse_args(["--exchange", "gate", "--base-url", "https://gate.test"])
        env = {"GATE_API_KEY": "key", "GATE_API_SECRET": "secret"}

        with mock.patch.dict(delta_mode.os.environ, env, clear=True):
            self.assertEqual(delta_mode.run(args, transport), 0)

        self.assertEqual(transport.calls, [])

    def test_run_executes_once_then_verifies(self) -> None:
        transport = StubTransport(
            response({"retCode": 0, "result": {"deltaEnable": False}}),
            response({"retCode": 0, "result": {}}),
            response({"retCode": 0, "result": {"deltaEnable": True}}),
        )
        args = delta_mode.parse_args(
            [
                "--exchange",
                "bybit",
                "--base-url",
                "https://bybit.test",
                "--query",
                "--execute",
            ]
        )
        env = {"BYBIT_API_KEY": "key", "BYBIT_API_SECRET": "secret"}

        with mock.patch.dict(delta_mode.os.environ, env, clear=True):
            self.assertEqual(delta_mode.run(args, transport), 0)

        self.assertEqual([call[0] for call in transport.calls], ["GET", "POST", "GET"])

    def test_binance_run_skips_high_weight_post_check(self) -> None:
        transport = StubTransport(
            response({"deltaEnabled": False}),
            response({"msg": "success"}),
        )
        args = delta_mode.parse_args(
            [
                "--exchange",
                "binance",
                "--base-url",
                "https://binance.test",
                "--query",
                "--execute",
            ]
        )
        env = {"BINANCE_API_KEY": "key", "BINANCE_API_SECRET": "secret"}

        with mock.patch.dict(delta_mode.os.environ, env, clear=True):
            self.assertEqual(delta_mode.run(args, transport), 0)

        self.assertEqual([call[0] for call in transport.calls], ["GET", "POST"])

    def test_default_execute_sends_exactly_one_post(self) -> None:
        transport = StubTransport(response({"enabled": True}))
        args = delta_mode.parse_args(
            [
                "--exchange",
                "gate",
                "--base-url",
                "https://gate.test",
                "--execute",
            ]
        )
        env = {"GATE_API_KEY": "key", "GATE_API_SECRET": "secret"}

        with mock.patch.dict(delta_mode.os.environ, env, clear=True):
            self.assertEqual(delta_mode.run(args, transport), 0)

        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(transport.calls[0][0], "POST")
        self.assertEqual(transport.calls[0][3], b'{"enabled":true}')

    def test_set_only_is_compatible_explicit_default(self) -> None:
        default_args = delta_mode.parse_args(["--exchange", "gate"])
        explicit_args = delta_mode.parse_args(["--exchange", "gate", "--set-only"])
        query_args = delta_mode.parse_args(["--exchange", "gate", "--query"])

        self.assertFalse(default_args.query)
        self.assertFalse(explicit_args.query)
        self.assertTrue(query_args.query)

    def test_okx_alias_and_non_bitget_target_uid_validation(self) -> None:
        self.assertEqual(delta_mode.normalize_exchange("okx"), "okex")
        args = delta_mode.parse_args(["--exchange", "gate", "--target-uid", "123"])
        with self.assertRaisesRegex(delta_mode.ApiError, "only for Bitget"):
            delta_mode.run(args, StubTransport())


if __name__ == "__main__":
    unittest.main()
