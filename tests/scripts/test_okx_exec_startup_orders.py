"""Offline tests: never send an exchange request or use real credentials."""
import argparse
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location(
    "okx_orders", Path(__file__).resolve().parents[2] / "scripts/okx_swap_open_orders.py"
)
orders = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(orders)


def args(**overrides):
    values = dict(
        inst_type="SPOT", inst_id=None, quote_ccy="USDT", limit=100,
        fetch_all=True, max_pages=0, base_url="https://example.invalid",
        timeout=1, simulate=True, cancel=False,
    )
    values.update(overrides)
    return argparse.Namespace(**values)


class OkxExecStartupOrdersTest(unittest.TestCase):
    def test_spot_scope_is_sent_to_exchange_and_bad_http_fails(self):
        with patch.object(orders, "request_okx_private", return_value=(200, '{"code":"0","data":[]}', {})) as request:
            result = orders.fetch_open_orders("https://example.invalid", "k", "s", "p", None, 100, None, 1, True, "SPOT")
            self.assertTrue(result[2])
            self.assertEqual(request.call_args.kwargs["params"]["instType"], "SPOT")
        with patch.object(orders, "request_okx_private", return_value=(500, '{"code":"0","data":[]}', {})):
            self.assertFalse(orders.fetch_open_orders("https://example.invalid", "k", "s", "p", None, 100, None, 1, True, "SPOT")[2])

    def test_pagination_filters_cash_quotes_without_skipping_later_usdt_orders(self):
        pages = [
            ([{"instId": "BTC-USDC", "ordId": "3"}], "3", True),
            ([{"instId": "BTC-USDT", "ordId": "2"}], "2", True),
            ([], None, True),
        ]
        with patch.object(orders, "fetch_open_orders", side_effect=pages) as fetch:
            result = orders.collect_open_orders(args(), "k", "s", "p")
            self.assertEqual([row["ordId"] for row in result], ["2"])
            self.assertEqual(fetch.call_args_list[1].kwargs["after"], "3")

    def test_incomplete_or_malformed_pages_fail_closed(self):
        row = {"instId": "BTC-USDT", "ordId": "1"}
        for pages, options in [
            ([( [row], "1", True), ([row], "1", True)], {}),
            ([([row], "1", True)], {"max_pages": 1}),
            ([([{"instId": "BTC-USDT"}], None, True)], {}),
            ([([], None, False)], {}),
        ]:
            with self.subTest(options=options, pages=pages):
                with patch.object(orders, "fetch_open_orders", side_effect=pages):
                    with self.assertRaises(RuntimeError):
                        orders.collect_open_orders(args(**options), "k", "s", "p")
        for body in ['[]', '{"code":"0"}', '{"code":"0","data":[null]}']:
            self.assertFalse(orders.parse_okx_body(body)[0])

    def test_dry_run_never_cancels(self):
        row = {"instId": "BTC-USDT", "ordId": "1"}
        with patch.object(orders, "parse_args", return_value=args()), patch.object(orders, "load_credentials", return_value=("k", "s", "p")), patch.object(orders, "collect_open_orders", return_value=[row]), patch.object(orders, "cancel_orders") as cancel:
            orders.main()
            cancel.assert_not_called()

    def test_cancel_ack_requires_a_successful_empty_requery(self):
        row = {"instId": "BTC-USDT", "ordId": "1"}
        for remaining, exit_code in [([], None), ([row], 4), (RuntimeError("query failed"), 4)]:
            with self.subTest(remaining=remaining):
                with patch.object(orders, "parse_args", return_value=args(cancel=True)), patch.object(orders, "load_credentials", return_value=("k", "s", "p")), patch.object(orders, "collect_open_orders", side_effect=[[row], remaining]), patch.object(orders, "cancel_orders", return_value=True):
                    if exit_code:
                        with self.assertRaises(SystemExit) as exc:
                            orders.main()
                        self.assertEqual(exc.exception.code, exit_code)
                    else:
                        orders.main()


if __name__ == "__main__":
    unittest.main()
