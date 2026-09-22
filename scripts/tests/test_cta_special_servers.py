import importlib.util
import json
import pathlib
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
        self.index_path = self.directory / "index.html"
        self.index_path.write_text("<!doctype html><title>test</title>", encoding="utf-8")

    def serve(self, handler):
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return f"http://127.0.0.1:{server.server_port}"

    def test_config_normalization_preserves_strategy_controls(self):
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        config["rule_name"] = "BASELINE_104"
        config["symbols"] = ["ethusdt", "BTCUSDT", "ethusdt"]
        normalized = CONFIG_SERVER.normalize_config(config)
        self.assertEqual(normalized["rule_name"], "baseline_104")
        self.assertEqual(normalized["symbols"], ["BTCUSDT", "ETHUSDT"])
        self.assertEqual(normalized["entry"]["trade_sides"], "both")
        self.assertEqual(normalized["entry"]["factor_long_quantile"], 0.9)
        self.assertEqual(normalized["entry"]["nq_long_quantile"], 0.5)
        self.assertEqual(
            normalized["execution"]["open_offsets"],
            [0.0, 0.0001, 0.0003, 0.0005],
        )
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
        with self.assertRaisesRegex(ValueError, "strictly increasing"):
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

    def test_config_http_rejects_environment_identity_change(self):
        store = CONFIG_SERVER.ConfigStore(self.config_path)
        config, revision = store.load()
        config["rule_name"] = "baseline_104"
        with self.assertRaisesRegex(ValueError, "rule_name is immutable"):
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
        self.assertEqual(snapshot["config"]["rule_name"], "tp_vpi_018")

    def test_risk_params_round_trip_uses_env_redis_key(self):
        class FakeRedis:
            def __init__(self):
                self.hashes = {}

            def hgetall(self, key):
                return dict(self.hashes.get(key, {}))

            def hkeys(self, key):
                return list(self.hashes.get(key, {}))

            def pipeline(self):
                return self

            def hset(self, key, mapping):
                self.hashes.setdefault(key, {}).update(mapping)
                return self

            def hdel(self, key, *fields):
                for field in fields:
                    self.hashes.get(key, {}).pop(field, None)
                return self

            def execute(self):
                return []

        redis = FakeRedis()
        store = CONFIG_SERVER.ConfigStore(self.config_path)
        base = self.serve(
            CONFIG_SERVER.make_handler(store, self.index_path, redis, "binance-cta-special-rx02")
        )
        with self.assertRaises(HTTPError) as missing:
            request_json(f"{base}/api/risk-params")
        self.assertEqual(missing.exception.code, 404)
        missing_body = json.load(missing.exception)
        missing.exception.close()
        self.assertIn("binance-cta-special-rx02:binance-futures:binance-futures", missing_body["key"])
        _schema_status, schema = request_json(f"{base}/api/risk-schema")
        values = dict(schema["defaults"])
        values["max_pos_u"] = "1500"
        status, saved = request_json(f"{base}/api/risk-params", method="POST", body={"values": values})
        self.assertEqual(status, 200)
        self.assertEqual(saved["values"]["max_pos_u"], "1500")
        self.assertEqual(saved["values"]["arb_hedge_order_rate_limit_10s"], "300")
        _status, loaded = request_json(f"{base}/api/risk-params")
        self.assertEqual(loaded["values"]["max_pos_u"], "1500")
        values["arb_hedge_order_rate_limit_10s"] = "10"
        with self.assertRaises(HTTPError) as caught:
            request_json(f"{base}/api/risk-params", method="POST", body={"values": values})
        self.assertEqual(caught.exception.code, 400)
        caught.exception.close()


if __name__ == "__main__":
    unittest.main()
