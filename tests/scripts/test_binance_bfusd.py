from __future__ import annotations

import importlib.util
import contextlib
import io
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
        self.assertEqual(steps[2].params["amount"], "<received-bfusd>")

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

    def test_product_subscribe_round_trips_use_actual_received_amount(self) -> None:
        for product in ("BFUSD", "RWUSD"):
            for mode, outbound, inbound in (
                ("STANDARD", "UMFUTURE_MAIN", "MAIN_UMFUTURE"),
                ("PM", "PORTFOLIO_MARGIN_MAIN", "MAIN_PORTFOLIO_MARGIN"),
            ):
                for asset in ("USDT", "USDC"):
                    with self.subTest(product=product, mode=mode, asset=asset):
                        steps = bfusd.build_subscribe_plan(
                            "100", asset, mode, True, True, product.lower()
                        )
                        subscribe_path = f"/sapi/v1/{product.lower()}/subscribe"
                        client = FakeClient({
                            "/papi/v1/asset-collection": {"msg": "success"},
                            "/sapi/v1/asset/transfer": {"tranId": 123},
                            subscribe_path: {
                                "success": True,
                                f"{product.lower()}Amount": "99.87500000",
                            },
                        })
                        with contextlib.redirect_stdout(io.StringIO()):
                            bfusd.execute_plan(client, steps)
                        if mode == "PM":
                            self.assertEqual(client.calls[0][3], {"asset": asset})
                        self.assertEqual(client.calls[-3][3], {
                            "type": outbound, "asset": asset, "amount": "100",
                        })
                        self.assertEqual(client.calls[-2][2:], (
                            subscribe_path, {"asset": asset, "amount": "100"},
                        ))
                        self.assertEqual(client.calls[-1][3], {
                            "type": inbound, "asset": product, "amount": "99.875",
                        })

    def test_product_redemption_uses_correct_asset_and_proceeds(self) -> None:
        for product, proceeds in (("BFUSD", "USDT"), ("RWUSD", "USDC")):
            for mode in (None, "STANDARD", "PM"):
                for redemption_type in ("FAST", "STANDARD"):
                    with self.subTest(product=product, mode=mode, type=redemption_type):
                        steps = bfusd.build_redeem_plan(
                            "7.50", redemption_type, mode, mode is not None, product
                        )
                        self.assertEqual(steps[-1].path, f"/sapi/v1/{product.lower()}/redeem")
                        self.assertEqual(steps[-1].params, {
                            "amount": "7.5", "type": redemption_type,
                        })
                        self.assertIn(f"to Spot {proceeds}", steps[-1].description)
                        if mode is not None:
                            self.assertEqual(steps[-2].params["asset"], product)
                            self.assertEqual(steps[-2].params["type"],
                                             "UMFUTURE_MAIN" if mode == "STANDARD"
                                             else "PORTFOLIO_MARGIN_MAIN")
                        if mode == "PM":
                            self.assertEqual(steps[0].params, {"asset": product})

    def test_rwusd_query_commands_use_selected_endpoint(self) -> None:
        for command in ("account", "quota"):
            args = bfusd.parse_args([command, "--product", "rwusd"])
            with (
                mock.patch.object(bfusd, "load_credentials", return_value=("key", "secret")),
                mock.patch.object(bfusd, "BinanceClient") as client,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                client.return_value.request.return_value = {}
                self.assertEqual(bfusd.run_read_only(args, "https://api.binance.com", ""), 0)
                client.return_value.request.assert_called_once_with(
                    "sapi", "GET", f"/sapi/v1/rwusd/{command}", {}
                )

    def test_cli_defaults_to_bfusd_and_rwusd_dry_run_never_calls_api(self) -> None:
        self.assertEqual(bfusd.parse_args(["account"]).product, "BFUSD")
        for command in ("subscribe", "redeem"):
            output = io.StringIO()
            with (
                mock.patch.dict(os.environ, {}, clear=True),
                mock.patch.object(bfusd, "maybe_source_env_file"),
                mock.patch.object(bfusd, "load_credentials") as credentials,
                mock.patch.object(bfusd, "BinanceClient") as client,
                contextlib.redirect_stdout(output),
            ):
                self.assertEqual(bfusd.main([
                    command, "--product", "RWUSD", "--amount", "10",
                    "--account-mode", "STANDARD", "--from-trading",
                ]), 0)
                credentials.assert_not_called()
                client.assert_not_called()
            self.assertIn(f"/sapi/v1/rwusd/{command}", output.getvalue())
            self.assertIn("Dry-run only", output.getvalue())

    def test_failed_or_malformed_subscription_never_transfers_or_retries(self) -> None:
        for product in ("BFUSD", "RWUSD"):
            for payload in (
                {"success": False}, {}, [],
                {"success": True},
                {"success": True, f"{product.lower()}Amount": "NaN"},
                {"success": True, f"{product.lower()}Amount": "0"},
            ):
                with self.subTest(product=product, payload=payload):
                    steps = bfusd.build_subscribe_plan(
                        "100", "USDT", "STANDARD", False, True, product
                    )
                    client = FakeClient({steps[0].path: payload})
                    with self.assertRaises(bfusd.WorkflowError):
                        bfusd.execute_plan(client, steps)
                    self.assertEqual(len(client.calls), 1)

    def test_rwusd_failed_redemption_reports_completed_transfers(self) -> None:
        steps = bfusd.build_redeem_plan("5", "FAST", "PM", True, "RWUSD")
        client = FakeClient({
            "/papi/v1/asset-collection": {"msg": "success"},
            "/sapi/v1/asset/transfer": {"tranId": 123},
            "/sapi/v1/rwusd/redeem": {"success": False},
        })
        with (
            contextlib.redirect_stdout(io.StringIO()),
            self.assertRaises(bfusd.WorkflowError) as raised,
        ):
            bfusd.execute_plan(client, steps)
        self.assertEqual(raised.exception.completed, (
            "collect_rwusd_in_pm", "transfer_rwusd_to_spot",
        ))
        self.assertEqual(len(client.calls), 3)

    def test_invalid_product_is_rejected_before_building_wallet_moves(self) -> None:
        with self.assertRaisesRegex(ValueError, "product must be"):
            bfusd.build_subscribe_plan("10", "USDT", "PM", True, True, "BTC")
        with self.assertRaisesRegex(ValueError, "product must be"):
            bfusd.build_redeem_plan("10", "FAST", "PM", True, "BTC")


if __name__ == "__main__":
    unittest.main()
