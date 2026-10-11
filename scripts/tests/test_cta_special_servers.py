import importlib.util
import gzip
import json
import pathlib
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError


ROOT = pathlib.Path(__file__).resolve().parents[2]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


CONFIG_SERVER = load_module(
    "cta_special_config_server", ROOT / "scripts/cta_special_config_server.py"
)
DASHBOARD = load_module(
    "cta_special_dashboard", ROOT / "scripts/cta_special_dashboard.py"
)


def request_json(url, *, method="GET", body=None):
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if data else {},
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        return response.status, json.load(response)


class FakeFactorRedis:
    def __init__(self):
        self.values = {}
        self.conflict = False

    def get(self, key):
        return self.values.get(key)

    def eval(self, script, number, key, expected, payload):
        assert number == 1
        assert "redis.call('GET'" in script and "redis.call('SET'" in script
        if self.conflict or self.values.get(key, '') != expected:
            return 0
        self.values[key] = payload
        return 1


class CtaSpecialServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = pathlib.Path(self.temp.name)
        self.config_path = self.directory / "cta_special.json"
        self.config_path.write_text(
            (ROOT / "config/cta_special.json").read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        (self.directory / "cta_special_factor_model_1m_pub.toml").write_text((ROOT / "config/cta_special_factor_model_1m_pub.toml").read_text())
        self.index_path = self.directory / "index.html"
        self.index_path.write_text("<!doctype html><title>test</title>", encoding="utf-8")

    def serve(self, handler):
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}"

    def test_chart_vendor_is_gzip_encoded_when_requested(self):
        vendor = self.directory / "echarts.min.js"
        original = b"const chart = {};\n" * 1000
        vendor.write_bytes(original)
        base = self.serve(DASHBOARD.make_handler(
            self.config_path, self.directory / "status.json",
            self.directory / "execution.json", self.index_path, vendor_path=vendor,
        ))
        request = urllib.request.Request(base + "/assets/echarts.min.js", headers={"Accept-Encoding": "gzip"})
        with urllib.request.urlopen(request, timeout=2) as response:
            body = response.read()
            self.assertEqual(response.headers["Content-Encoding"], "gzip")
            self.assertEqual(response.headers["Vary"], "Accept-Encoding")
            self.assertEqual(gzip.decompress(body), original)
            self.assertLess(len(body), len(original) // 10)

    def test_config_normalization_preserves_strategy_controls(self):
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        normalized = CONFIG_SERVER.normalize_config(config)
        self.assertEqual(set(normalized["entry"]), {"factor_long_quantile", "factor_short_quantile", "nq_change_enabled", "nq_long_quantile", "nq_short_quantile"})
        self.assertEqual(normalized["entry"]["factor_long_quantile"], 0.9)
        self.assertEqual(normalized["entry"]["nq_long_quantile"], 0.5)
        self.assertEqual(set(normalized["execution"]), {"factor_exit_quantile_long", "factor_exit_quantile_short"})
        self.assertNotIn("runtime", normalized)

        del config["enabled"]
        self.assertFalse(CONFIG_SERVER.normalize_config(config)["enabled"])

    def test_config_rejects_unknown_and_invalid_strategy_fields(self):
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["venue"] = "binance-futures"
        with self.assertRaisesRegex(ValueError, "unknown fields"):
            CONFIG_SERVER.normalize_config(config)
        del config["venue"]
        config["execution"]["open_offsets"] = [0, 0.0001, 0.0001]
        with self.assertRaisesRegex(ValueError, "unknown fields"):
            CONFIG_SERVER.normalize_config(config)

    def test_config_validates_entry_and_exit_quantiles_together(self):
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["entry"]["factor_long_quantile"] = 0.6
        config["execution"]["factor_exit_quantile_long"] = 0.7
        with self.assertRaisesRegex(ValueError, "below entry"):
            CONFIG_SERVER.normalize_config(config)

        config["entry"]["factor_long_quantile"] = 0.8
        config["execution"]["factor_exit_quantile_long"] = 0.7
        normalized = CONFIG_SERVER.normalize_config(config)
        self.assertEqual(normalized["entry"]["factor_long_quantile"], 0.8)

    def test_config_http_save_is_revision_checked(self):
        store = CONFIG_SERVER.ConfigStore(self.config_path)
        base = self.serve(CONFIG_SERVER.make_handler(store, self.index_path))
        _, loaded = request_json(f"{base}/api/config")
        config = loaded["config"]
        config["enabled"] = True
        status, saved = request_json(
            f"{base}/api/config",
            method="PUT",
            body={"config": config, "expected_revision": loaded["revision"]},
        )
        self.assertEqual(status, 200)
        self.assertTrue(saved["config"]["enabled"])
        with self.assertRaises(HTTPError) as caught:
            request_json(
                f"{base}/api/config",
                method="PUT",
                body={"config": config, "expected_revision": loaded["revision"]},
            )
        self.assertEqual(caught.exception.code, 409)
        caught.exception.close()

    def test_config_rejects_obsolete_single_factor_contract(self):
        store = CONFIG_SERVER.ConfigStore(self.config_path)
        config, revision = store.load()
        config["rule_name"] = "baseline_104"
        with self.assertRaisesRegex(ValueError, "unknown fields"):
            store.save(config, revision)

    def test_dashboard_snapshot_reports_status_age_and_values(self):
        status_path = self.directory / "status.json"
        execution_path = self.directory / "execution.json"
        status_path.write_text(
            json.dumps(
                {
                    "updated_ts_us": time.time_ns() // 1000,
                    "scheduled_entries": 2,
                    "symbols": {"BTCUSDT": {"score": 1.5, "quantile": 0.95}},
                }
            ),
            encoding="utf-8",
        )
        execution_path.write_text(
            json.dumps({"updated_ts_us": time.time_ns() // 1000, "symbols": []}),
            encoding="utf-8",
        )
        snapshot = DASHBOARD.build_snapshot(self.config_path, status_path, execution_path)
        self.assertTrue(snapshot["healthy"])
        self.assertEqual(snapshot["signal_state"], "online")
        self.assertEqual(snapshot["execution_state"], "online")
        self.assertGreaterEqual(snapshot["status_age_ms"], 0)
        self.assertEqual(snapshot["status"]["scheduled_entries"], 2)

    def test_dashboard_snapshot_treats_absent_status_as_offline(self):
        snapshot = DASHBOARD.build_snapshot(
            self.config_path,
            self.directory / "missing_status.json",
            self.directory / "missing_execution.json",
        )
        self.assertFalse(snapshot["healthy"])
        self.assertEqual(snapshot["errors"], [])
        self.assertEqual(snapshot["signal_state"], "offline")
        self.assertEqual(snapshot["execution_state"], "offline")
        self.assertNotIn("rule_name", snapshot["config"])

    def test_factor_configuration_roundtrip_conflicts_and_validation(self):
        redis = FakeFactorRedis()
        store = CONFIG_SERVER.ConfigStore(self.config_path)
        env = 'binance-cta-special-test'
        base = self.serve(CONFIG_SERVER.make_handler(store, self.index_path, redis, env))
        _, loaded = request_json(f'{base}/api/symbol-factors')
        self.assertFalse(loaded['exists'])
        table = {'ethusdt': {'FACTOR_036':2, 'td_ti_002':-1}}
        _, saved = request_json(f'{base}/api/symbol-factors', method='POST', body={'symbol_factors':table, 'expected_revision':loaded['revision']})
        self.assertEqual(saved['key'], env + ':cta_symbol_factors')
        self.assertEqual(saved['symbol_factors']['ETHUSDT'], {'factor_036':2,'TD_TI_002':-1})
        self.assertEqual(saved['available'], loaded['available'])
        self.assertEqual(json.loads(redis.get(saved['key']))['ETHUSDT'], {'factor_036':2,'TD_TI_002':-1})
        _, reread = request_json(f'{base}/api/symbol-factors')
        self.assertEqual(reread['symbol_factors'], saved['symbol_factors'])
        with self.assertRaises(HTTPError) as conflict:
            request_json(f'{base}/api/symbol-factors', method='POST', body={'symbol_factors':{}, 'expected_revision':loaded['revision']})
        self.assertEqual(conflict.exception.code, 409)
        conflict.exception.close()
        table['ethusdt']['TD_TI_002'] = 'invalid'
        with self.assertRaises(HTTPError) as invalid:
            request_json(f'{base}/api/symbol-factors', method='POST', body={'symbol_factors':table, 'expected_revision':saved['revision']})
        self.assertEqual(invalid.exception.code, 400)
        invalid.exception.close()
        self.assertEqual(json.loads(redis.get(saved['key'])), saved['symbol_factors'])
        _, empty = request_json(f'{base}/api/symbol-factors', method='POST', body={'symbol_factors':{}, 'expected_revision':saved['revision']})
        self.assertEqual(empty['symbol_factors'], {})

    def test_global_amount_validates_and_rejects_factor_overrides(self):
        original = json.loads(self.config_path.read_text())
        self.assertEqual(CONFIG_SERVER.normalize_config(original)["max_pos_u"], 500)
        for value in (0, -1, True, "1000", float("inf"), float("nan")):
            config = {**original, "max_pos_u": value}
            with self.subTest(value=value), self.assertRaises(ValueError):
                CONFIG_SERVER.normalize_config(config)
        config = {**original, "max_pos_u_overrides": {"FILUSDT": 2000}}
        with self.assertRaisesRegex(ValueError, "unknown fields"):
            CONFIG_SERVER.normalize_config(config)

    def test_retired_factor_amount_endpoints_are_unavailable(self):
        redis = FakeFactorRedis()
        base = self.serve(CONFIG_SERVER.make_handler(CONFIG_SERVER.ConfigStore(self.config_path), self.index_path, redis, "binance-cta-special-test"))
        for method in ("GET", "POST"):
            with self.subTest(method=method), self.assertRaises(HTTPError) as caught:
                request_json(base + "/api/factor-strategies", method=method, body={} if method == "POST" else None)
            self.assertEqual(caught.exception.code, 404)
            caught.exception.close()
        self.assertEqual(redis.values, {})

    def test_factor_compare_and_set_detects_concurrent_write(self):
        redis = FakeFactorRedis()
        env = 'binance-cta-special-test'
        revision = CONFIG_SERVER.read_factor_table(redis, env)['revision']
        redis.conflict = True
        with self.assertRaisesRegex(ValueError, 'configuration changed'):
            CONFIG_SERVER.save_factor_table(redis, env, {}, revision)
        self.assertFalse(redis.values)

    def test_factor_validator_rejects_bad_lists_and_supports_the_requested_universe(self):
        import cta_special_factor_config as factors
        table = factors.normalize_table(json.loads((ROOT / 'config/cta_special_symbol_factors.json').read_text()))
        self.assertEqual(len(table), 16)
        self.assertEqual(len(set(f for row in table.values() for f in row)), 5)
        self.assertNotIn('BTCUSDT', table)
        original = {'factor_036':-2, 'TD_TI_002':1}
        for row in ({'factor_999':1},{'factor_036':1,'FACTOR_036':-1},{'factor_036':0},{'factor_036':float('nan')},{'factor_036':True},{'factor_036':'1'},{}):
            with self.subTest(row=row), self.assertRaises(ValueError):
                factors.normalize_table({'ETHUSDT':row})
        with self.assertRaises(ValueError):
            factors.normalize_table({'ETHUSDT':original,'ethusdt':original})
        with self.assertRaisesRegex(ValueError,'unavailable'):
            factors.validate_available({'ETHUSDT':{'factor_005':1}},factors.available_universe())
        with self.assertRaisesRegex(ValueError,'duplicate JSON key'):
            json.loads('{"ETHUSDT":{"factor_036":1,"factor_036":-1}}',object_pairs_hook=factors.reject_duplicate_keys)

    def test_factor_cli_dry_run_never_connects_to_redis(self):
        completed = subprocess.run([sys.executable, str(ROOT / 'scripts/cta_special_factor_config.py'), '--env-name', 'binance-cta-special-test', '--config', str(ROOT / 'config/cta_special_symbol_factors.json')], capture_output=True, text=True, check=True)
        plan = json.loads(completed.stdout)
        self.assertFalse(plan['execute'])
        self.assertEqual(len(plan['symbol_factors']), 16)

    def test_removed_risk_endpoints_cannot_mutate_exec(self):
        redis = FakeFactorRedis()
        base = self.serve(CONFIG_SERVER.make_handler(CONFIG_SERVER.ConfigStore(self.config_path), self.index_path, redis, "binance-cta-special-test"))
        for route, method in (("risk-schema", "GET"), ("risk-params", "GET"), ("risk-params", "POST")):
            with self.subTest(route=route, method=method), self.assertRaises(HTTPError) as caught:
                request_json(base + "/api/" + route, method=method, body={"values": {}} if method == "POST" else None)
            self.assertEqual(caught.exception.code, 404)
            caught.exception.close()
        self.assertEqual(redis.values, {})

    def test_removed_signal_fields_are_rejected(self):
        for section, field, value in (("entry", "trade_sides", "long"), ("entry", "cooldown_seconds", 60), ("entry", "signal_delay_seconds", 1), ("execution", "factor_exit_enabled", False)):
            config = json.loads(self.config_path.read_text())
            config[section][field] = value
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, "unknown fields"):
                CONFIG_SERVER.normalize_config(config)

    def test_global_amount_save_preserves_manager_and_factor_config(self):
        redis = FakeFactorRedis()
        manager_key = "binance-exec-cta-special-test:binance-futures:batch_exec:factor_036"
        redis.values[manager_key] = '{"maker_timeout_ms":4321,"targets":{"ETHUSDT":{"qty":"clear","signal":0}}}'
        factor_key = "binance-cta-special-test:cta_symbol_factors"
        redis.values[factor_key] = '{"FILUSDT":{"factor_036":-1}}'
        original = dict(redis.values)
        base = self.serve(CONFIG_SERVER.make_handler(CONFIG_SERVER.ConfigStore(self.config_path), self.index_path, redis, "binance-cta-special-test"))
        _, loaded = request_json(base + "/api/config")
        config = loaded["config"]
        config["max_pos_u"] = 2000
        _, saved = request_json(base + "/api/config", method="PUT", body={"config": config, "expected_revision": loaded["revision"]})
        self.assertEqual(saved["config"]["max_pos_u"], 2000)
        self.assertFalse(saved["config"]["enabled"])
        self.assertEqual(redis.values, original)
        with self.assertRaises(HTTPError) as conflict:
            request_json(base + "/api/config", method="PUT", body={"config": config, "expected_revision": loaded["revision"]})
        self.assertEqual(conflict.exception.code, 409)
        conflict.exception.close()
        self.assertEqual(CONFIG_SERVER.ConfigStore(self.config_path).load()[0]["max_pos_u"], 2000)


if __name__ == "__main__":
    unittest.main()
