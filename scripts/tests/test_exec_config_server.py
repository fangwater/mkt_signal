import contextlib
import importlib.util
import io
import json
import pathlib
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer


MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "exec_config_server.py"
SPEC = importlib.util.spec_from_file_location("exec_config_server", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class FakeRedis:
    def __init__(self):
        self.values = {}

    def set(self, key, value):
        self.values[key] = value

    def get(self, key):
        return self.values.get(key)

def fake_store():
    store = object.__new__(MODULE.ExecConfigStore)
    store.client = FakeRedis()
    store.env_name = "cta_exec_trade"
    store.venue = "binance-futures"
    store.prefix = "cta_exec_trade:binance-futures:batch_exec:"
    store.index_key = f"{store.prefix}strategy_names"
    store.removed_index_key = f"{store.prefix}removed_strategy_names"
    store._save_lock = threading.Lock()
    return store


class ExecConfigServerTests(unittest.TestCase):
    def test_client_script_is_downloadable(self):
        store = fake_store()
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), MODULE.make_handler(store, "../")
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        with urllib.request.urlopen(
            f"http://127.0.0.1:{server.server_port}/exec_config_client.py",
            timeout=2,
        ) as response:
            body = response.read().decode("utf-8")
            self.assertEqual(response.status, 200)
            self.assertEqual(response.headers["Content-Type"], "application/octet-stream")
            self.assertEqual(
                response.headers["Content-Disposition"],
                'attachment; filename="exec_config_client.py"',
            )

        self.assertTrue(body.startswith("#!/usr/bin/env python3"))
        self.assertIn("http://172.16.30.42:10041/config/", body)

    def test_config_page_is_display_only_and_filters_zero_targets(self):
        store = fake_store()
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), MODULE.make_handler(store, "../")
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        with urllib.request.urlopen(
            f"http://127.0.0.1:{server.server_port}/", timeout=2
        ) as response:
            body = response.read().decode("utf-8")

        self.assertNotIn("Save Strategy", body)
        self.assertNotIn('id="add-strategy"', body)
        self.assertNotIn('id="add-target"', body)
        self.assertIn('id="single_order_usdt" inputmode="decimal" disabled', body)
        self.assertNotIn("Add a strategy", body)
        self.assertIn('.filter(([, qty]) => Number(qty) !== 0)', body)

    def test_http_update_writes_redis_and_logs_response(self):
        store = fake_store()
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), MODULE.make_handler(store, "../")
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"btcusdt": 0.2}
        request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/api/strategy",
            data=json.dumps(
                {"strategy_name": "trend_a", "config": config}
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with urllib.request.urlopen(request, timeout=2) as response:
                payload = json.load(response)

        self.assertTrue(payload["ok"])
        self.assertEqual(payload["config"]["targets"], {"BTCUSDT": 0.2})
        self.assertEqual(store.load("trend_a")["targets"], {"BTCUSDT": 0.2})
        self.assertIn("update status=200 response=", output.getvalue())
        self.assertIn(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), output.getvalue())

    def test_strategy_names_write_independent_keys(self):
        store = fake_store()
        first = dict(MODULE.DEFAULT_CONFIG)
        first["targets"] = {"btcusdt": 0.2}
        second = dict(MODULE.DEFAULT_CONFIG)
        second["targets"] = {"ETHUSDT": -3}

        store.save("trend_a", first)
        store.save("trend_b", second)

        self.assertEqual(store.list_strategy_names(), ["trend_a", "trend_b"])
        self.assertEqual(store.load("trend_a")["targets"], {"BTCUSDT": 0.2})
        self.assertEqual(store.load("trend_b")["targets"], {"ETHUSDT": -3.0})
        self.assertNotEqual(store.key("trend_a"), store.key("trend_b"))
        self.assertEqual(
            store.client.get(store.index_key), '["trend_a","trend_b"]'
        )

    def test_remove_only_updates_index_and_retains_config_for_recovery(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}
        store.save("trend_a", config)

        self.assertTrue(store.remove("trend_a"))
        self.assertEqual(store.list_strategy_names(), [])
        self.assertEqual(store.list_removed_strategy_names(), ["trend_a"])
        self.assertEqual(store.load("trend_a")["targets"], {"BTCUSDT": 0.2})
        self.assertTrue(store.remove("trend_a"))
        with self.assertRaisesRegex(ValueError, "removal already requested"):
            store.save("trend_a", config)

    def test_remove_accepts_unindexed_strategy_with_retained_config(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}
        store.client.set(store.key("trend_a"), json.dumps(config))

        self.assertTrue(store.remove("trend_a"))
        self.assertEqual(store.list_removed_strategy_names(), ["trend_a"])
        self.assertFalse(store.remove("unknown"))

    def test_unindexed_config_key_is_not_discovered(self):
        store = fake_store()
        store.client.set(store.key("orphan"), "{}")
        self.assertEqual(store.list_strategy_names(), [])

    def test_strategy_index_name_is_reserved(self):
        with self.assertRaisesRegex(ValueError, "reserved"):
            MODULE.validate_strategy_name("strategy_names")
        with self.assertRaisesRegex(ValueError, "reserved"):
            MODULE.validate_strategy_name(MODULE.POSITION_CLOSE_STRATEGY_NAME)
        with self.assertRaisesRegex(ValueError, "reserved"):
            MODULE.validate_strategy_name("removed_strategy_names")

    def test_http_delete_requests_strategy_removal(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}
        store.save("trend_a", config)
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), MODULE.make_handler(store, "../")
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/api/strategy?name=trend_a",
            method="DELETE",
        )
        with urllib.request.urlopen(request, timeout=2) as response:
            payload = json.load(response)

        self.assertEqual(response.status, 202)
        self.assertEqual(payload["state"], "removal_requested")
        self.assertEqual(store.list_strategy_names(), [])

    def test_invalid_order_params_are_rejected(self):
        config = dict(MODULE.DEFAULT_CONFIG)
        config["orders_per_batch"] = 0
        with self.assertRaisesRegex(ValueError, "orders_per_batch"):
            MODULE.normalize_exec_config(config)

    def test_invalid_symbol_is_rejected(self):
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTC-USDT": 1}
        with self.assertRaisesRegex(ValueError, "invalid symbol"):
            MODULE.normalize_exec_config(config)

    def test_symbol_can_be_split_across_strategy_names(self):
        store = fake_store()
        first = dict(MODULE.DEFAULT_CONFIG)
        first["targets"] = {"BTCUSDT": 0.2}
        second = dict(MODULE.DEFAULT_CONFIG)
        second["targets"] = {"BTCUSDT": -0.1}
        store.save("trend_a", first)
        store.save("trend_b", second)

        self.assertEqual(store.load("trend_a")["targets"], {"BTCUSDT": 0.2})
        self.assertEqual(store.load("trend_b")["targets"], {"BTCUSDT": -0.1})


if __name__ == "__main__":
    unittest.main()
