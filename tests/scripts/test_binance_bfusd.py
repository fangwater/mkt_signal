from __future__ import annotations

import importlib.util
import os
import unittest
from pathlib import Path
from typing import Any, Mapping
from unittest import mock


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "binance_bfusd.py"
spec = importlib.util.spec_from_file_location("binance_bfusd", SCRIPT)
bfusd = importlib.util.module_from_spec(spec)
assert spec.loader is not None
import sys

sys.modules[spec.name] = bfusd
spec.loader.exec_module(bfusd)


class FakeClient:
    def __init__(self, responses: Mapping[str, Any]) -> None:
        self.responses = dict(responses)
        self.calls: list[tuple[str, str, str, dict[str, Any]]] = []

    def request(self, api: str, method: str, path: str, params: Mapping[str, Any]) -> Any:
        self.calls.append((api, method, path, dict(params)))
        return self.responses[path]


class BinanceBfusdTests(unittest.TestCase):
    def test_normalizes_repository_account_mode_names(self) -> None:
        self.assertEqual(bfusd.normalize_account_mode("std"), "STANDARD")
        self.assertEqual(bfusd.normalize_account_mode("UNIFIED"), "PM")
        self.assertEqual(bfusd.normalize_account_mode("portfolio_margin"), "PM")
        self.assertIsNone(bfusd.normalize_account_mode("auto"))

    def test_refuses_cli_and_environment_mode_conflict(self) -> None:
        with (
            mock.patch.dict(os.environ, {"BINANCE_ACCOUNT_MODE": "UNIFIED"}),
            self.assertRaisesRegex(ValueError, "conflicts"),
        ):
            bfusd.resolve_account_mode("STANDARD", True)

    def test_standard_subscribe_round_trip_plan(self) -> None:
        steps = bfusd.build_subscribe_plan("1000.000", "usdt", "STANDARD", True, True)

        self.assertEqual([step.name for step in steps], [
            "transfer_usdt_to_spot",
            "subscribe_bfusd",
            "transfer_bfusd_to_trading",
        ])
        self.assertEqual(steps[0].params["type"], "UMFUTURE_MAIN")
        self.assertEqual(steps[0].params["amount"], "1000")
        self.assertEqual(steps[1].path, "/sapi/v1/bfusd/subscribe")
        self.assertEqual(steps[2].params["type"], "MAIN_UMFUTURE")
        self.assertEqual(steps[2].params["amount"], bfusd.RECEIVED_BFUSD)

    def test_pm_subscribe_collects_before_transfer(self) -> None:
        steps = bfusd.build_subscribe_plan("25", "USDC", "PM", True, True)

        self.assertEqual([step.name for step in steps], [
            "collect_usdc_in_pm",
            "transfer_usdc_to_spot",
            "subscribe_bfusd",
            "transfer_bfusd_to_trading",
        ])
        self.assertEqual(steps[0].api, "papi")
        self.assertEqual(steps[0].path, "/papi/v1/asset-collection")
        self.assertEqual(steps[1].params["type"], "PORTFOLIO_MARGIN_MAIN")
        self.assertEqual(steps[-1].params["type"], "MAIN_PORTFOLIO_MARGIN")

    def test_pm_redeem_collects_and_moves_bfusd_to_spot(self) -> None:
        steps = bfusd.build_redeem_plan("7.50", "fast", "PM", True)

        self.assertEqual([step.name for step in steps], [
            "collect_bfusd_in_pm",
            "transfer_bfusd_to_spot",
            "redeem_bfusd",
        ])
        self.assertEqual(steps[1].params["type"], "PORTFOLIO_MARGIN_MAIN")
        self.assertEqual(steps[2].params, {"amount": "7.5", "type": "FAST"})

    def test_spot_only_workflow_does_not_require_account_mode(self) -> None:
        subscribe = bfusd.build_subscribe_plan("1", "USDT", None, False, False)
        redeem = bfusd.build_redeem_plan("1", "STANDARD", None, False)

        self.assertEqual([step.name for step in subscribe], ["subscribe_bfusd"])
        self.assertEqual([step.name for step in redeem], ["redeem_bfusd"])

    def test_received_bfusd_amount_is_used_for_destination_transfer(self) -> None:
        steps = bfusd.build_subscribe_plan("100", "USDT", "STANDARD", False, True)
        client = FakeClient({
            "/sapi/v1/bfusd/subscribe": {"success": True, "bfusdAmount": "99.87500000"},
            "/sapi/v1/asset/transfer": {"tranId": 123},
        })

        bfusd.execute_plan(client, steps)

        self.assertEqual(client.calls[-1][3]["asset"], "BFUSD")
        self.assertEqual(client.calls[-1][3]["amount"], "99.875")

    def test_signed_payload_signs_the_percent_encoded_payload(self) -> None:
        payload = bfusd.signed_payload(
            {"asset": "USDT", "note": "a b"},
            "secret",
            recv_window=5000,
            timestamp_ms=123,
        )
        query, signature = payload.rsplit("&signature=", 1)

        self.assertEqual(query, "asset=USDT&note=a+b&recvWindow=5000&timestamp=123")
        self.assertEqual(signature, bfusd.sign_query(query, "secret"))

    def test_rejects_invalid_amount_without_api_calls(self) -> None:
        with self.assertRaisesRegex(ValueError, "greater than zero"):
            bfusd.build_redeem_plan("0", "FAST", None, False)


if __name__ == "__main__":
    unittest.main()
