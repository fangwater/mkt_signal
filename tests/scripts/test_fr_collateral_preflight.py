from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
sys.path.insert(0, str(SCRIPTS))
import check_fr_collateral as check


class FakeClient:
    def __init__(self, responses, **env):
        self.responses, self.env, self.calls = responses, env, []

    def get(self, path, params=None):
        self.calls.append((path, params))
        key = (path, tuple(sorted(params.items()))) if params else path
        return self.responses[key]


class CollateralChecks(unittest.TestCase):
    def bybit(self, coin_row, **info):
        return FakeClient({
            "/v5/account/info": {"marginMode": "PORTFOLIO_MARGIN", "unifiedMarginStatus": 6,
                                 "spotHedgingStatus": "ON", **info},
            "/v5/account/collateral-info": {"list": [{"currency": "NEW", **coin_row}]},
        })

    def test_bybit_disabled_coin_blocks_even_before_first_inventory(self):
        report = check.Report("bybit_fr_test", "bybit", ["NEW"])
        check.check_bybit(self.bybit({"marginCollateral": True, "collateralSwitch": False}), report)
        self.assertFalse(report.ok)
        self.assertEqual(report.disabled, ["NEW"])

    def test_bybit_platform_unsupported_is_reported_separately(self):
        report = check.Report("bybit_fr_test", "bybit", ["NEW"])
        check.check_bybit(self.bybit({"marginCollateral": False}), report)
        self.assertTrue(report.ok)
        self.assertEqual(report.unsupported, ["NEW"])

    def test_bybit_missing_or_string_boolean_never_passes(self):
        for fields in ({"marginCollateral": True},
                       {"marginCollateral": True, "collateralSwitch": "false"}):
            with self.subTest(fields=fields), self.assertRaises(check.PreflightError):
                check.check_bybit(self.bybit(fields), check.Report("env", "bybit", ["NEW"]))

    def test_bybit_wrong_mode_or_disabled_spot_hedging_blocks(self):
        for info in ({"marginMode": "ISOLATED_MARGIN"}, {"spotHedgingStatus": "OFF"},
                     {"unifiedMarginStatus": 1}):
            with self.subTest(info=info), self.assertRaises(check.PreflightError):
                check.check_bybit(self.bybit({}, **info), check.Report("env", "bybit", []))

    def test_gate_custom_collateral_queries_eligible_zero_balance_coin(self):
        client = FakeClient({
            "/api/v4/unified/accounts": {"mode": "multi_currency", "locked": False,
                                         "is_all_collateral": False, "balances": {}},
            "/api/v4/unified/currency_discount_tiers": [[{"currency": "NEW"}]],
            ("/api/v4/unified/accounts", (("currency", "NEW"),)):
                {"balances": {"NEW": {"enabled_collateral": False}}},
        })
        report = check.Report("env", "gate", ["NEW", "UNSUPPORTED"])
        check.check_gate(client, report)
        self.assertEqual(report.disabled, ["NEW"])
        self.assertEqual(report.unsupported, ["UNSUPPORTED"])
        self.assertEqual(len(client.calls), 3)

    def test_gate_all_collateral_does_not_require_existing_wallet_rows(self):
        client = FakeClient({"/api/v4/unified/accounts": {
            "mode": "multi_currency", "locked": False, "is_all_collateral": True}})
        report = check.Report("env", "gate", ["NEW"])
        check.check_gate(client, report)
        self.assertTrue(report.ok)

    def test_gate_missing_collateral_flag_is_not_treated_as_all(self):
        client = FakeClient({"/api/v4/unified/accounts": {
            "mode": "multi_currency", "locked": False}})
        with self.assertRaises(check.PreflightError):
            check.check_gate(client, check.Report("env", "gate", []))

    def bitget(self, kind, coins="BTC,USDT", **settings):
        return FakeClient({
            "/api/v3/account/settings": {"accountMode": "unified", "accountLevel": "advanced",
                                         "holdMode": "one_way_mode", **settings},
            "/api/v3/account/collateral-type": {"collateralType": kind, "collateralCoins": coins},
            "/api/v3/account/custom-collateral-coins":
                [{"collateralCoin": coin} for coin in ("NEW", "BTC", "USDT")],
        })

    def test_bitget_mainstream_and_custom_detect_missing_altcoin(self):
        for kind in ("mainstream", "custom"):
            with self.subTest(kind=kind):
                report = check.Report("env", "bitget", ["NEW", "BTC", "UNSUPPORTED"])
                check.check_bitget(self.bitget(kind), report)
                self.assertEqual(report.disabled, ["NEW"])
                self.assertEqual(report.unsupported, ["UNSUPPORTED"])

    def test_bitget_all_and_custom_enabled_pass(self):
        for kind in ("all", "custom"):
            report = check.Report("env", "bitget", ["NEW"])
            check.check_bitget(self.bitget(kind, "NEW,USDT"), report)
            self.assertTrue(report.ok)

    def test_bitget_basic_is_invalid_for_margin_fr(self):
        with self.assertRaises(check.PreflightError):
            check.check_bitget(self.bitget("all", accountLevel="basic"), check.Report("env", "bitget", []))

    def test_okx_multi_currency_checks_flag_but_portfolio_does_not_use_it(self):
        for level in ("3", "4"):
            client = FakeClient({
                "/api/v5/account/config": [{"acctLv": level}],
                "/api/v5/account/collateral-assets": [{"ccy": "NEW", "collateralEnabled": False}],
            })
            report = check.Report("env", "okex", ["NEW"])
            check.check_okex(client, report)
            self.assertEqual(report.ok, level == "4")
            self.assertEqual(len(client.calls), 1 if level == "4" else 2)

    def test_okx_missing_coin_fails_instead_of_guessing_unsupported(self):
        client = FakeClient({"/api/v5/account/config": [{"acctLv": "3"}],
                             "/api/v5/account/collateral-assets": [{"ccy": "USDT", "collateralEnabled": True}]})
        with self.assertRaises(check.PreflightError):
            check.check_okex(client, check.Report("env", "okex", ["NEW"]))

    def test_binance_pm_and_standard_use_their_own_account_endpoint(self):
        for mode, path, payload in (
            ("UNIFIED", "/papi/v1/account", {"accountStatus": "NORMAL", "uniMMR": "9.1"}),
            ("STANDARD", "/fapi/v2/account", {"canTrade": True}),
        ):
            client = FakeClient({path: payload}, BINANCE_ACCOUNT_MODE=mode)
            report = check.Report("env", "binance", ["NEW"])
            check.check_binance(client, report)
            self.assertTrue(report.ok)
            self.assertEqual(client.calls, [(path, None)])

    def test_binance_unknown_mode_and_abnormal_pm_fail(self):
        with self.assertRaises(check.PreflightError):
            check.check_binance(FakeClient({}), check.Report("env", "binance", []))
        for data in ({"accountStatus": "REDUCE_ONLY", "uniMMR": "3"},
                     {"accountStatus": "NORMAL", "uniMMR": "NaN"}):
            with self.assertRaises(check.PreflightError):
                check.check_binance(FakeClient({"/papi/v1/account": data}, BINANCE_ACCOUNT_MODE="UNIFIED"),
                                    check.Report("env", "binance", []))


class PreflightInputs(unittest.TestCase):
    def test_union_includes_position_dump_and_quote_and_uses_atomic_mget(self):
        rds = Mock()
        rds.mget.return_value = [b'["BTCUSDT"]', b'["EDGE-USDT-SWAP"]',
                                 None, b'["SLX@bybit"]', b'[]', b'["GRVT_USDT"]']
        assets = check.load_assets("bybit_fr_test", "bybit-margin", "bybit-futures", {}, rds)
        self.assertEqual(assets, ["BTC", "EDGE", "GRVT", "SLX", "USDT"])
        self.assertIn("bybit_fr_test:fr_pos_dump_symbols:bybit-margin_bybit-futures",
                      rds.mget.call_args.args[0])

    def test_missing_or_corrupt_redis_never_becomes_empty_success(self):
        for values in ([None] * 6, [b'{"bad":1}'] + [None] * 5,
                       [b'[null]'] + [None] * 5, [b'garbage'] + [None] * 5):
            rds = Mock()
            rds.mget.return_value = values
            with self.subTest(values=values), self.assertRaises(check.PreflightError):
                check.load_assets("env", "open", "hedge", {}, rds)

    def test_explicit_empty_lists_still_check_quote(self):
        rds = Mock()
        rds.mget.return_value = [b'[]'] + [None] * 5
        self.assertEqual(check.load_assets("env", "open", "hedge", {}, rds), ["USDT"])

    def test_env_credentials_do_not_fall_back_to_another_account(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, {"BYBIT_API_KEY": "wrong-account"}):
            env_dir = Path(tmp)
            (env_dir / "env.sh").write_text("BYBIT_API_SECRET='local-secret'\n")
            env = check.source_env(env_dir)
            self.assertNotIn("BYBIT_API_KEY", env)
            self.assertEqual(env["BYBIT_API_SECRET"], "local-secret")

    def test_source_ip_requires_concrete_deployment_configuration(self):
        with tempfile.TemporaryDirectory() as tmp:
            env_dir = Path(tmp)
            for ips in ([], ["0.0.0.0"]):
                (env_dir / "trade_engine.toml").write_text("local_ips = " + json.dumps(ips))
                with self.assertRaises(check.PreflightError):
                    check.source_ip(env_dir)
            (env_dir / "trade_engine.toml").write_text('local_ips = ["172.31.7.124"]')
            self.assertEqual(check.source_ip(env_dir), "172.31.7.124")

    @patch.object(check.http.client, "HTTPSConnection")
    def test_transport_binds_source_and_exposes_no_mutation_endpoint(self, connection):
        response = connection.return_value.getresponse.return_value
        response.status, response.read.return_value = 200, b'{"retCode":0,"result":{}}'
        client = check.Client("bybit", {"BYBIT_API_KEY": "test", "BYBIT_API_SECRET": "test"}, "172.31.7.124")
        client.get("/v5/account/info")
        self.assertEqual(connection.call_args.kwargs["source_address"], ("172.31.7.124", 0))
        self.assertEqual(connection.return_value.request.call_args.args[0], "GET")
        with self.assertRaises(check.PreflightError):
            client.get("/v5/account/set-collateral-switch")
        self.assertEqual(connection.return_value.request.call_count, 1)

    @patch.object(check.http.client, "HTTPSConnection")
    def test_transport_errors_do_not_print_secret_or_raw_response(self, connection):
        response = connection.return_value.getresponse.return_value
        response.status, response.read.return_value = 403, b'API_KEY=secret account body'
        client = check.Client("binance", {"BINANCE_API_KEY": "secret", "BINANCE_API_SECRET": "secret"}, "127.0.0.1")
        with self.assertRaises(check.PreflightError) as error:
            client.get("/papi/v1/account")
        self.assertNotIn("secret", str(error.exception))
        self.assertNotIn("signature", str(error.exception))


class CollateralEnableTests(unittest.TestCase):
    def test_bybit_execute_is_idempotent_and_requires_readback(self):
        client = Mock(exchange="bybit")
        state = {"currency": "NEW", "marginCollateral": True, "collateralSwitch": False}

        def get(path, params=None):
            if path == "/v5/account/info":
                return {"marginMode": "PORTFOLIO_MARGIN", "unifiedMarginStatus": 6, "spotHedgingStatus": "ON"}
            return {"list": [dict(state)]}

        def post(path, body):
            self.assertEqual(body, {"coin": "NEW", "collateralSwitch": "ON"})
            state["collateralSwitch"] = True
            return {}

        client.get.side_effect, client.post.side_effect = get, post
        report = check.Report("env", "bybit", ["NEW"])
        check.ensure_collateral(client, report, execute=True)
        self.assertTrue(report.ok)
        self.assertEqual(report.enabled, ["NEW"])
        check.ensure_collateral(client, check.Report("env", "bybit", ["NEW"]), execute=True)
        self.assertEqual(client.post.call_count, 1)

    def test_read_only_never_posts_and_ack_without_state_change_still_blocks(self):
        for execute in (False, True):
            client = CollateralChecks().bybit({"marginCollateral": True, "collateralSwitch": False})
            client.exchange, client.post = "bybit", Mock(return_value={})
            report = check.Report("env", "bybit", ["NEW"])
            check.ensure_collateral(client, report, execute=execute)
            self.assertFalse(report.ok)
            self.assertEqual(client.post.call_count, int(execute))

    def test_unknown_post_outcome_is_read_back_without_retry(self):
        client = Mock(exchange="bybit")
        coin = {"currency": "NEW", "marginCollateral": True, "collateralSwitch": False}
        info = {"marginMode": "PORTFOLIO_MARGIN", "unifiedMarginStatus": 6, "spotHedgingStatus": "ON"}
        client.get.side_effect = [info, {"list": [coin]}, info,
                                  {"list": [{**coin, "collateralSwitch": True}]}]
        client.post.side_effect = check.PreflightError("POST network failure")
        report = check.Report("env", "bybit", ["NEW"])
        check.ensure_collateral(client, report, execute=True)
        self.assertTrue(report.ok)
        self.assertEqual(client.post.call_count, 1)
        self.assertIn("subsequent read verified", report.notes[-1])

    def test_bitget_custom_list_keeps_unrelated_existing_coins(self):
        client = Mock(exchange="bitget")
        client.get.return_value = {"collateralType": "custom", "collateralCoins": "BTC,OLD,USDT"}
        check.enable_disabled(client, ["NEW"])
        self.assertEqual(client.post.call_args.args, (
            "/api/v3/account/set-collateral-type",
            {"collateralType": "custom", "collateralCoins": "BTC,NEW,OLD,USDT"}))

    def test_bitget_mainstream_preserves_all_six_previous_coins(self):
        client = Mock(exchange="bitget")
        client.get.return_value = {"collateralType": "mainstream"}
        check.enable_disabled(client, ["NEW"])
        coins = set(client.post.call_args.args[1]["collateralCoins"].split(","))
        self.assertEqual(coins, check.BITGET_MAINSTREAM | {"NEW"})

    def test_gate_only_adds_enabled_currencies(self):
        client = Mock(exchange="gate")
        client.post.return_value = {"is_success": True}
        check.enable_disabled(client, ["NEW"])
        self.assertEqual(client.post.call_args.args[1], {"collateral_type": 1, "enable_list": ["NEW"]})

    def test_okx_only_enables_named_coins_in_batches(self):
        client = Mock(exchange="okex")
        coins = [f"COIN{n}" for n in range(21)]
        check.enable_disabled(client, coins)
        self.assertEqual(client.post.call_count, 2)
        self.assertEqual([coin for call in client.post.call_args_list for coin in call.args[1]["ccyList"]], coins)
        for call in client.post.call_args_list:
            self.assertEqual(call.args[1]["type"], "custom")
            self.assertIs(call.args[1]["collateralEnabled"], True)

    def test_post_requires_execute_and_rejects_off_and_unrelated_endpoints(self):
        client = check.Client("bybit", {}, "127.0.0.1")
        with self.assertRaises(check.PreflightError):
            client.post("/v5/account/set-collateral-switch", {"coin": "NEW", "collateralSwitch": "ON"})
        client.execute = True
        for path, body in (("/v5/account/set-collateral-switch", {"coin": "NEW", "collateralSwitch": "OFF"}),
                           ("/v5/position/set-leverage", {})):
            with self.assertRaises(check.PreflightError):
                client.post(path, body)

    @patch.object(check.http.client, "HTTPSConnection")
    def test_exchange_posts_sign_the_actual_method_and_json_body(self, connection):
        env = {f"{prefix}_API_{kind}": "test" for prefix in ("BYBIT", "GATE", "BITGET", "OKX")
               for kind in ("KEY", "SECRET")}
        env.update(OKX_PASSPHRASE="test", BITGET_API_PASSPHRASE="test")
        cases = (
            ("bybit", "/v5/account/set-collateral-switch", {"coin": "NEW", "collateralSwitch": "ON"},
             {"retCode": 0, "result": {}}),
            ("gate", "/api/v4/unified/collateral_currencies", {"collateral_type": 1, "enable_list": ["NEW"]},
             {"is_success": True}),
            ("bitget", "/api/v3/account/set-collateral-type", {"collateralType": "custom", "collateralCoins": "NEW,USDT"},
             {"code": "00000", "data": "success"}),
            ("okex", "/api/v5/account/set-collateral-assets", {"type": "custom", "ccyList": ["NEW"], "collateralEnabled": True},
             {"code": "0", "data": []}),
        )
        for ex, path, body, payload in cases:
            with self.subTest(exchange=ex):
                response = connection.return_value.getresponse.return_value
                response.status, response.read.return_value = 200, json.dumps(payload).encode()
                check.Client(ex, env, "172.31.7.124", execute=True).post(path, body)
                call = connection.return_value.request.call_args
                self.assertEqual(call.args, ("POST", path))
                self.assertEqual(json.loads(call.kwargs["body"]), body)
                headers, encoded = call.kwargs["headers"], call.kwargs["body"].decode()
                if ex == "bybit":
                    expected = check.sign_bybit(headers["X-BAPI-TIMESTAMP"], "test", "5000", encoded, "test")
                    self.assertEqual(headers["X-BAPI-SIGN"], expected)
                elif ex == "gate":
                    expected = check.sign_gate("POST", path, "", encoded, "test", headers["Timestamp"])
                    self.assertEqual(headers["SIGN"], expected)
                elif ex == "bitget":
                    expected = check.sign_bitget(headers["ACCESS-TIMESTAMP"], "POST", path, encoded, "test")
                    self.assertEqual(headers["ACCESS-SIGN"], expected)
                else:
                    expected = check.sign_okx(headers["OK-ACCESS-TIMESTAMP"], "POST", path, encoded, "test")
                    self.assertEqual(headers["OK-ACCESS-SIGN"], expected)


class StartupOrdering(unittest.TestCase):
    def test_failed_or_missing_preflight_never_stops_or_starts_processes(self):
        for script in ("start_trade_signal.sh", "start_fr_pre_trade.sh"):
            for missing in (False, True):
                with self.subTest(script=script, missing=missing), tempfile.TemporaryDirectory() as tmp:
                    base = Path(tmp) / "bybit_fr_test"
                    scripts = base / "scripts"
                    scripts.mkdir(parents=True)
                    shutil.copy2(SCRIPTS / script, scripts / script)
                    (base / "env.sh").write_text("")
                    marker = Path(tmp) / "mutated"
                    for path in (base / "pre_trade", base / "trade_signal", scripts / "stop_fr_pre_trade.sh",
                                 Path(tmp) / "pmdaemon", Path(tmp) / "npx"):
                        path.write_text('#!/bin/bash\necho mutation >>"$MARKER"\n')
                        path.chmod(0o755)
                    if not missing:
                        (scripts / "check_fr_collateral.py").write_text("raise SystemExit(27)\n")
                    env = {**os.environ, "PATH": tmp + os.pathsep + os.environ["PATH"], "MARKER": str(marker)}
                    result = subprocess.run(["bash", str(scripts / script)], cwd=base, env=env,
                                            capture_output=True, text=True, timeout=10)
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertFalse(marker.exists(), result.stdout)

    def test_successful_preflight_runs_before_signal_supervisor_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "bybit_fr_test"
            scripts = base / "scripts"
            scripts.mkdir(parents=True)
            shutil.copy2(SCRIPTS / "start_trade_signal.sh", scripts)
            (base / "env.sh").write_text("")
            (base / "trade_signal").write_text("#!/bin/bash\n")
            (base / "trade_signal").chmod(0o755)
            (scripts / "check_fr_collateral.py").write_text(
                'import os\nwith open(os.environ["MARKER"],"a") as f: f.write("checked\\n")\n')
            npx = Path(tmp) / "npx"
            npx.write_text('#!/bin/bash\necho mutation >>"$MARKER"\n')
            npx.chmod(0o755)
            marker = Path(tmp) / "sequence"
            env = {**os.environ, "PATH": tmp + os.pathsep + os.environ["PATH"], "MARKER": str(marker)}
            result = subprocess.run(["bash", str(scripts / "start_trade_signal.sh")], cwd=base,
                                    env=env, capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertEqual(marker.read_text().splitlines()[0], "checked")
            self.assertIn("mutation", marker.read_text())


class FakeWatchError(Exception):
    pass


class TransactionRedis:
    def __init__(self, initial):
        self.values = dict(initial)
        self.events = []

    def pipeline(self):
        owner = self

        class Pipeline:
            def __enter__(self):
                self.pending = {}
                return self

            def __exit__(self, *args):
                return False

            def watch(self, *keys):
                self.snapshot = {k: owner.values.get(k) for k in keys}
                owner.events.append("watch")

            def mget(self, keys):
                return [owner.values.get(k) for k in keys]

            def multi(self):
                owner.events.append("multi")

            def set(self, key, value):
                self.pending[key] = value

            def execute(self):
                if any(owner.values.get(k) != v for k, v in self.snapshot.items()):
                    raise FakeWatchError("lists changed")
                owner.values.update(self.pending)
                owner.events.append("commit")

        return Pipeline()


class SymbolPublicationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.env_dir = Path(self.temp.name) / "bybit_fr_test"
        self.env_dir.mkdir()
        (self.env_dir / "env.sh").write_text("")
        (self.env_dir / "trade_engine.toml").write_text('local_ips=["127.0.0.1"]')
        self.rds = TransactionRedis({self.key("fwd_trade_symbols"): '["OLDUSDT"]'})

    def key(self, name):
        return f"bybit_fr_test:fr_{name}:bybit-margin_bybit-futures"

    def publish(self, updates):
        return check.publish_symbol_lists(self.rds, self.env_dir, "bybit", "bybit-margin", "bybit-futures", updates)

    def test_new_coin_is_enabled_and_read_back_before_any_publication(self):
        state = {"USDT": True, "OLD": True, "NEW": False}
        client = Mock(exchange="bybit")

        def get(path, params=None):
            self.assertEqual(self.rds.values[self.key("fwd_trade_symbols")], '["OLDUSDT"]')
            self.rds.events.append("exchange-read")
            if path.endswith("/info"):
                return {"marginMode": "PORTFOLIO_MARGIN", "unifiedMarginStatus": 6, "spotHedgingStatus": "ON"}
            return {"list": [{"currency": coin, "marginCollateral": True, "collateralSwitch": enabled}
                             for coin, enabled in state.items()]}

        def post(path, body):
            self.assertNotIn("multi", self.rds.events)
            state[body["coin"]] = True
            self.rds.events.append("enabled")
            return {}

        client.get.side_effect, client.post.side_effect = get, post
        with patch.object(check, "Client", return_value=client):
            report = self.publish({"fwd_trade_symbols": ["OLDUSDT", "NEWUSDT"]})
        self.assertEqual(report.enabled, ["NEW"])
        self.assertEqual(json.loads(self.rds.values[self.key("fwd_trade_symbols")]), ["OLDUSDT", "NEWUSDT"])
        self.assertEqual(self.rds.events[-3:], ["exchange-read", "multi", "commit"])

    def test_exchange_error_or_unverified_off_never_publishes(self):
        original = dict(self.rds.values)
        for raises in (True, False):
            def fail(client, report, execute):
                if raises:
                    raise check.PreflightError("exchange unavailable")
                report.disabled.append("NEW")
            with patch.object(check, "ensure_collateral", side_effect=fail), self.assertRaises(check.PreflightError):
                self.publish({"fwd_trade_symbols": ["NEWUSDT"]})
            self.assertEqual(self.rds.values, original)
            self.assertNotIn("multi", self.rds.events)

    def test_proposed_union_retains_legacy_and_position_dump_and_skips_absent_fields(self):
        self.rds.values[self.key("trade_symbols")] = '["LEGACYUSDT"]'
        self.rds.values[self.key("pos_dump_symbols")] = '["HELDUSDT"]'
        self.rds.values[self.key("bwd_trade_symbols")] = '["SHORTUSDT"]'
        seen = []
        def verify(client, report, execute):
            seen.extend(report.assets)
        with patch.object(check, "ensure_collateral", side_effect=verify):
            self.publish({"fwd_trade_symbols": ["NEWUSDT"]})
        self.assertEqual(seen, ["HELD", "LEGACY", "NEW", "SHORT", "USDT"])
        self.assertEqual(self.rds.values[self.key("bwd_trade_symbols")], '["SHORTUSDT"]')
        self.assertEqual(self.rds.values[self.key("pos_dump_symbols")], '["HELDUSDT"]')

    def test_explicit_clear_and_multiple_updates_are_committed_together(self):
        with patch.object(check, "ensure_collateral"):
            self.publish({"fwd_trade_symbols": [], "bwd_trade_symbols": ["NEWUSDT"]})
        self.assertEqual(json.loads(self.rds.values[self.key("fwd_trade_symbols")]), [])
        self.assertEqual(json.loads(self.rds.values[self.key("bwd_trade_symbols")]), ["NEWUSDT"])
        self.assertEqual(self.rds.events.count("commit"), 1)

    def test_concurrent_change_aborts_instead_of_overwriting_a_newer_list(self):
        def concurrent_change(client, report, execute):
            self.rds.values[self.key("bwd_trade_symbols")] = '["OPERATORUSDT"]'
        with patch.object(check, "ensure_collateral", side_effect=concurrent_change), self.assertRaises(FakeWatchError):
            self.publish({"fwd_trade_symbols": ["NEWUSDT"], "bwd_trade_symbols": ["NEWUSDT"]})
        self.assertEqual(self.rds.values[self.key("fwd_trade_symbols")], '["OLDUSDT"]')
        self.assertEqual(self.rds.values[self.key("bwd_trade_symbols")], '["OPERATORUSDT"]')

    def test_bad_existing_json_and_unrecognized_update_fail_without_api_or_writes(self):
        self.rds.values[self.key("pos_dump_symbols")] = "corrupt"
        with patch.object(check, "ensure_collateral") as verify:
            for updates in ({"fwd_trade_symbols": ["NEWUSDT"]}, {"not_a_list": []}, {"fwd_trade_symbols": [None]}):
                with self.assertRaises(check.PreflightError):
                    self.publish(updates)
            verify.assert_not_called()
        self.assertEqual(self.rds.values[self.key("fwd_trade_symbols")], '["OLDUSDT"]')


if __name__ == "__main__":
    unittest.main()
