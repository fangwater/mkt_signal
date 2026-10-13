import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import neutralize_rapidx_binance as n

D = n.D


def rule(step="0.001", minimum="0.001", notional="5"):
    return dict(lotSize=D(step), minSize=D(minimum), minNotional=D(notional), tickSize=D("0.01"), maxLimitSize=D(1000000), live=True)


class PlanningTests(unittest.TestCase):
    def test_eth_crosses_zero_without_reduce_only(self):
        p = n.plan(D("0.051622673"), D("0.272"), D(2500), rule())
        self.assertEqual(p["side"], "SELL")
        self.assertEqual(p["quantity"], "0.323")
        self.assertFalse(p["reduce_only"])
        self.assertEqual(D(p["residual_if_filled"]), D("0.000622673"))
        self.assertEqual(D(p["perp"]) - D(p["quantity"]), D("-0.051"))

    def test_borrowed_cash_is_hedged_by_long_perp(self):
        p = n.plan(D("-13632.402796"), D(13611), D("0.084"), rule("1", "1", "1"))
        self.assertEqual((p["side"], p["quantity"], p["reduce_only"]), ("BUY", "21", False))
        self.assertEqual(D(p["residual_if_filled"]), D("-0.402796"))

    def test_reduce_only_when_entire_trade_reduces_existing_perp(self):
        p = n.plan(D("-0.05"), D("0.272"), D(2500), rule())
        self.assertTrue(p["reduce_only"])
        self.assertEqual(p["quantity"], "0.222")

    def test_no_trade_for_existing_hedge_or_dust(self):
        self.assertEqual(n.plan(D("-3"), D(3), D(100), rule())["reason"], "neutral")
        self.assertEqual(n.plan(D("0.0004"), D(0), D(100), rule())["reason"], "below_min_qty")
        self.assertEqual(n.plan(D("0.003"), D(0), D(100), rule())["reason"], "below_min_notional")

    def test_rounding_never_increases_or_reverses_net_exposure(self):
        for cash in [D("-2.234"), D("0.051622"), D(0), D("3.56789")]:
            for perp in [D("-4.56"), D("0.272"), D(0), D(8)]:
                p = n.plan(cash, perp, D(2500), rule())
                net, after = D(p["net"]), D(p["residual_if_filled"])
                self.assertLessEqual(abs(after), abs(net))
                self.assertGreaterEqual(net * after, 0)

    def test_price_rounding_stays_inside_slippage_bound(self):
        bid, ask, tick = D("99.97"), D("100.03"), D("0.05")
        self.assertLessEqual(n.limit_price("BUY", bid, ask, tick, False, D(2)), bid)
        self.assertGreaterEqual(n.limit_price("SELL", bid, ask, tick, False, D(2)), ask)
        self.assertLessEqual(n.limit_price("BUY", bid, ask, tick, True, D(2)), ask * D("1.0002"))
        self.assertGreaterEqual(n.limit_price("SELL", bid, ask, tick, True, D(2)), bid * D("0.9998"))

    def test_rejects_nonfinite_and_bad_rules(self):
        for bad in ("NaN", "Infinity", "-Infinity", "hello"):
            with self.assertRaises(n.Halt): n.dec(bad)
        with self.assertRaises(n.Halt): n.quantize(D(1), D(0))
        capped = rule()
        capped["maxLimitSize"] = D("0.1")
        with self.assertRaisesRegex(n.Halt, "maximum limit"):
            n.plan(D(1), D(0), D(100), capped)


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(os.environ, LTP_API_KEY="testkey", LTP_API_SECRET="testsecret",
                              LTP_PORTFOLIO_ID="123", LTP_REST_URL="https://api.liquiditytech.com")
        self.env.start()
        self.addCleanup(self.env.stop)
        self.c = n.Client(None)

    def test_read_only_guard_blocks_post_and_delete_before_network(self):
        with patch.object(n.http.client, "HTTPSConnection") as connection:
            for method in ("POST", "DELETE"):
                with self.assertRaises(n.Halt): self.c.request(method, n.ORDER, {})
            connection.assert_not_called()

    def test_no_redirect_or_response_body_in_error(self):
        with patch.object(n.http.client, "HTTPSConnection") as connection:
            connection.return_value.getresponse.return_value.status = 302
            with self.assertRaisesRegex(n.Halt, "HTTP 302"):
                self.c.request("GET", n.ORDER)
            connection.return_value.close.assert_called_once()

    def test_pagination_rejects_partial_and_changed_pages(self):
        self.c.request = lambda *_: dict(page=1, pageSize=1000, pageNum=1, totalSize=2, list=[{}])
        with self.assertRaisesRegex(n.Halt, "incomplete page"):
            self.c.pages("/read", {})
        responses = iter([dict(page=1, pageSize=1000, pageNum=2, totalSize=1001, list=[{}]*1000),
                          dict(page=2, pageSize=1000, pageNum=2, totalSize=1002, list=[{}, {}])])
        self.c.request = lambda *_: next(responses)
        with self.assertRaisesRegex(n.Halt, "changed"):
            self.c.pages("/read", {})

    def test_identity_and_open_algo_blocks(self):
        with self.assertRaises(n.Halt): self.c.identity(dict(portfolioId="999", exchangeType="BINANCE"))
        with self.assertRaises(n.Halt): self.c.identity(dict(portfolioId="123", exchangeType="OKX"))
        self.c.pages = lambda path, _: ([dict(portfolioId="123", exchangeType="BINANCE")] if "algo" in path else [])
        with self.assertRaisesRegex(n.Halt, "algo orders=1"): self.c.empty_orders()

    def test_signing_matches_sorted_raw_values_not_url_encoding(self):
        import hmac, hashlib
        headers = n.signed_headers("key", "secret", {"sym": "BINANCE_PERP_ETH_USDT", "reduceOnly": "FALSE"}, 1700000000000000000)
        expected = hmac.new(b"secret", b"reduceOnly=FALSE&sym=BINANCE_PERP_ETH_USDT&1700000000", hashlib.sha256).hexdigest()
        self.assertEqual(headers["signature"], expected)

    def test_snapshot_subtracts_debt_once_and_rejects_hedge_mode(self):
        account = dict(portfolioId="123", exchangeType="BINANCE", positionMode="NET", accountStatus="NORMAL")
        def request(_, path, params=None):
            if path.endswith("/account"): return [account]
            if path.endswith("/loan/info"): return dict(portfolioId=123, accounts=[dict(exchange="BINANCE", coins=[])])
            return [dict(portfolioId="123", sym="BINANCE_PERP_DOGE_USDT", positionSide="NONE", positionQty="99", markPrice="0.1")]
        self.c.request = request
        self.c.pages = lambda *_: [dict(portfolioId="123", exchangeType="BINANCE", coin="DOGE", balance="0", debt="100.1", indexPrice="0.1")]
        cash, perp, _ = self.c.snapshot()
        self.assertEqual(cash["DOGE"] + perp["DOGE"], D("-1.1"))
        account["positionMode"] = "BOTH"
        with self.assertRaisesRegex(n.Halt, "NET position"): self.c.snapshot()

    def test_book_rejects_stale_or_crossed_prices(self):
        self.c.request = lambda *a, **kw: dict(T=1000000, bids=[["100", "1"]], asks=[["101", "1"]])
        with patch.object(n.time, "time", return_value=1010):
            with self.assertRaisesRegex(n.Halt, "stale"): self.c.book("ETH")
        self.c.request = lambda *a, **kw: dict(T=1000000, bids=[["102", "1"]], asks=[["101", "1"]])
        with patch.object(n.time, "time", return_value=1000):
            with self.assertRaisesRegex(n.Halt, "crossed"): self.c.book("ETH")


class LifecycleTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "journal.json"
        self.j = n.Journal(self.path, "123")
        self.req = dict(clientOrderId="neutraltest", sym="BINANCE_PERP_ETH_USDT", side="SELL",
                        orderQty="0.323", limitPrice="2500", timeInForce="GTX")

    def client(self, states, snapshot_perp="0.172"):
        client = SimpleNamespace(execute=True, calls=[], states=iter(states))
        def request(method, path, params):
            client.calls.append((method, params))
            if method == "GET":
                state, filled = next(client.states)
                return dict(self.req, orderId="1", portfolioId="123", orderState=state, executedQty=filled)
            return {}
        client.request = request
        client.identity = lambda row: n.require(row["portfolioId"] == "123", "mismatch")
        client.snapshot = lambda: ({"ETH": D("0.0516")}, {"ETH": D(snapshot_perp)}, {})
        return client

    def test_partial_fill_cancel_must_be_terminal_then_position_reconciles(self):
        self.j.data["pending"] = dict(request=self.req, perp_before="0.272")
        c = self.client([("PARTIALLY_FILLED", "0.1"), ("CANCELLED", "0.1")])
        # First poll remains active, timeout follows, then cancellation is queried.
        with patch.object(n.time, "monotonic", side_effect=[0, 0, 2, 2, 2, 2]), patch.object(n.time, "sleep"):
            spent = n.settle(c, self.j, 1)
        self.assertEqual(spent, D(250))
        self.assertEqual([m for m, _ in c.calls], ["GET", "DELETE", "GET"])
        self.assertIsNone(json.loads(self.path.read_text())["pending"])

    def test_lost_place_response_is_not_retried(self):
        c = self.client([("FILLED", "0.323")], "-0.051")
        base_request = c.request
        def request(method, path, params):
            if method == "POST":
                self.assertEqual(json.loads(self.path.read_text())["pending"]["request"], self.req)
                c.calls.append((method, params))
                raise TimeoutError("lost ACK")
            return base_request(method, path, params)
        c.request = request
        spent = n.submit(c, self.j, self.req, D("0.272"), 1)
        self.assertEqual(spent, D("807.500"))
        self.assertEqual(sum(m == "POST" for m, _ in c.calls), 1)
        self.assertIsNone(self.j.data["pending"])

    def test_unknown_order_remains_durable_and_blocks_new_submissions(self):
        self.j.data["pending"] = dict(request=self.req, perp_before="0.272")
        self.j.save()
        c = self.client([])
        c.request = lambda *_: (_ for _ in ()).throw(n.Halt("unavailable"))
        with patch.object(n.time, "monotonic", side_effect=[0, 2, 2, 18]), patch.object(n.time, "sleep"):
            with self.assertRaisesRegex(n.Halt, "UNRESOLVED"): n.settle(c, self.j, 1)
        self.assertIsNotNone(json.loads(self.path.read_text())["pending"])
        with self.assertRaisesRegex(n.Halt, "unresolved previous"):
            n.submit(c, self.j, self.req, D(0), 1)

    def test_wrong_position_does_not_clear_journal(self):
        self.j.data["pending"] = dict(request=self.req, perp_before="0.272")
        c = self.client([("FILLED", "0.323")], "0.272")
        with patch.object(n.time, "monotonic", side_effect=[0, 0, 0, 21]):
            with self.assertRaisesRegex(n.Halt, "does not reconcile"): n.settle(c, self.j, 1)
        self.assertIsNotNone(self.j.data["pending"])

    def test_dry_run_never_calls_submit(self):
        args = SimpleNamespace(env_dir=Path(self.tmp.name), recover_only=False, execute=False,
                               max_order_usdt=D(1000), max_total_usdt=D(2000), maker_seconds=20,
                               maker_attempts=2, ioc_fallback=True, max_slip_bps=D(2))
        c = SimpleNamespace(execute=False, empty_orders=lambda: None, rules=lambda: {"ETH": rule()},
                            book=lambda _: (D("2499.99"), D(2500)),
                            snapshot=lambda: ({"ETH": D("0.0516")}, {"ETH": D("0.272")}, {"ETH": D(2500)}))
        with patch.object(n, "assert_stopped"), patch.object(n, "submit") as submit:
            self.assertEqual(n.run(args, c, self.j), 0)
            submit.assert_not_called()
        self.assertFalse(self.path.exists())

    def test_failed_write_ahead_log_prevents_order_submission(self):
        c = self.client([])
        with patch.object(self.j, "save", side_effect=OSError("disk full")):
            with self.assertRaises(OSError): n.submit(c, self.j, self.req, D("0.272"), 1)
        self.assertEqual(c.calls, [])


if __name__ == "__main__":
    unittest.main()
