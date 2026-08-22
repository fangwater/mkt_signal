import contextlib
import importlib.util
import io
import json
import pathlib
import tempfile
import threading
import unittest
import urllib.request
from http.server import ThreadingHTTPServer
from unittest import mock
from urllib.error import HTTPError


MODULE_PATH = pathlib.Path(__file__).resolve().parents[1] / "exec_config_server.py"
SPEC = importlib.util.spec_from_file_location("exec_config_server", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)

WRITE_TOKEN = "manager-write-token-for-tests-0123456789"


class FakeWatchError(Exception):
    pass


class FakePipeline:
    def __init__(self, client):
        self.client = client
        self.watched = {}
        self.commands = []
        self.in_transaction = False

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False

    def watch(self, *keys):
        self.watched = {key: self.client.revisions.get(key, 0) for key in keys}

    def get(self, key):
        return self.client.get(key)

    def multi(self):
        self.in_transaction = True

    def set(self, key, value):
        if not self.in_transaction:
            raise AssertionError("MULTI must be called before queued writes")
        self.commands.append((key, value))

    def execute(self):
        if self.client.before_pipeline_execute is not None:
            callback = self.client.before_pipeline_execute
            self.client.before_pipeline_execute = None
            callback()
        if any(
            self.client.revisions.get(key, 0) != revision
            for key, revision in self.watched.items()
        ):
            raise FakeWatchError("watched key changed")
        for key, value in self.commands:
            self.client.set(key, value)
        return [True for _ in self.commands]


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.revisions = {}
        self.before_pipeline_execute = None

    def set(self, key, value):
        self.values[key] = value
        self.revisions[key] = self.revisions.get(key, 0) + 1

    def get(self, key):
        return self.values.get(key)

    def pipeline(self):
        return FakePipeline(self)


def fake_store():
    store = object.__new__(MODULE.ExecConfigStore)
    store.client = FakeRedis()
    store.env_name = "cta_exec_trade"
    store.venue = "binance-futures"
    store.prefix = "cta_exec_trade:binance-futures:batch_exec:"
    store.index_key = f"{store.prefix}strategy_names"
    store.removed_index_key = f"{store.prefix}removed_strategy_names"
    store._save_lock = threading.Lock()
    store._watch_error = FakeWatchError
    return store


def order_parameters(config):
    return {field: config[field] for field in MODULE.ORDER_PARAMETER_FIELDS}


class ExecConfigServerTests(unittest.TestCase):
    def test_old_client_script_is_not_served(self):
        store = fake_store()
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), MODULE.make_handler(store, "../", WRITE_TOKEN)
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        with self.assertRaises(HTTPError) as raised:
            urllib.request.urlopen(
                f"http://127.0.0.1:{server.server_port}/exec_config_client.py",
                timeout=2,
            )
        self.assertEqual(raised.exception.code, 404)

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
        self.assertNotIn('id="save-order-parameters"', body)
        self.assertNotIn('id="reset-order-parameters"', body)
        self.assertIn("Target Positions", body)
        self.assertIn("Read only", body)
        self.assertIn('id="single_order_usdt" inputmode="decimal" disabled', body)
        self.assertIn('id="max_batch" inputmode="numeric" disabled', body)
        self.assertNotIn("Add a strategy", body)
        self.assertIn('.filter(([, raw]) => targetQty(raw) !== 0)', body)
        self.assertIn("<th>Signal</th>", body)

    def test_order_parameter_payload_rejects_targets(self):
        config = dict(MODULE.DEFAULT_CONFIG)
        payload = order_parameters(config)
        payload["targets"] = {"BTCUSDT": 1}

        with self.assertRaisesRegex(ValueError, "unknown order parameter fields: targets"):
            MODULE.normalize_order_parameters(payload)

    def test_order_parameter_save_preserves_targets_and_strategy_index(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2, "ETHUSDT": -1.5}
        current = store.save("trend_a", config)
        index_before = store.client.get(store.index_key)
        parameters = order_parameters(current)
        parameters["single_order_usdt"] = 250

        updated = store.save_order_parameters(
            "trend_a", parameters, current["updated_at_us"]
        )

        self.assertEqual(updated["single_order_usdt"], 250.0)
        self.assertEqual(updated["targets"], current["targets"])
        self.assertEqual(store.load("trend_a")["targets"], current["targets"])
        self.assertEqual(store.client.get(store.index_key), index_before)

    def test_stale_order_parameter_save_does_not_modify_redis(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}
        current = store.save("trend_a", config)
        raw_before = store.client.get(store.key("trend_a"))

        with self.assertRaises(MODULE.ConfigVersionConflict):
            store.save_order_parameters(
                "trend_a",
                order_parameters(current),
                current["updated_at_us"] - 1,
            )

        self.assertEqual(store.client.get(store.key("trend_a")), raw_before)

    def test_concurrent_target_update_wins_over_order_parameter_save(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}
        current = store.save("trend_a", config)
        concurrent = dict(current)
        concurrent["targets"] = {"ETHUSDT": 3.0}
        concurrent["updated_at_us"] += 1

        store.client.before_pipeline_execute = lambda: store.client.set(
            store.key("trend_a"),
            json.dumps(concurrent, separators=(",", ":")),
        )
        with self.assertRaises(MODULE.ConfigVersionConflict):
            store.save_order_parameters(
                "trend_a",
                order_parameters(current),
                current["updated_at_us"],
            )

        self.assertEqual(
            store.load("trend_a")["targets"],
            {"ETHUSDT": {"qty": 3.0, "signal": 0}},
        )

    def test_order_parameter_save_cannot_create_unknown_strategy(self):
        store = fake_store()

        with self.assertRaisesRegex(ValueError, "strategy is not active"):
            store.save_order_parameters(
                "unknown",
                order_parameters(MODULE.DEFAULT_CONFIG),
                None,
            )

        self.assertIsNone(store.client.get(store.key("unknown")))

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
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {WRITE_TOKEN}",
            },
            method="POST",
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with urllib.request.urlopen(request, timeout=2) as response:
                payload = json.load(response)

        self.assertTrue(payload["ok"])
        self.assertEqual(
            payload["config"]["targets"],
            {"BTCUSDT": {"qty": 0.2, "signal": 0}},
        )
        self.assertGreater(payload["config"]["updated_at_us"], 0)
        self.assertEqual(
            store.load("trend_a")["targets"],
            {"BTCUSDT": {"qty": 0.2, "signal": 0}},
        )
        self.assertIn("update status=200 response=", output.getvalue())
        self.assertIn(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), output.getvalue())

    def test_http_order_parameter_save_preserves_targets_and_omits_them_from_output(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}
        current = store.save("trend_a", config)
        parameters = order_parameters(current)
        parameters["orders_per_batch"] = 5
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), MODULE.make_handler(store, "../", WRITE_TOKEN)
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/api/order-parameters",
            data=json.dumps(
                {
                    "strategy_name": "trend_a",
                    "expected_updated_at_us": current["updated_at_us"],
                    "order_parameters": parameters,
                }
            ).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {WRITE_TOKEN}",
            },
            method="POST",
        )
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            with urllib.request.urlopen(request, timeout=2) as response:
                payload = json.load(response)

        self.assertEqual(response.status, 200)
        self.assertEqual(payload["order_parameters"]["orders_per_batch"], 5)
        self.assertNotIn("targets", payload)
        self.assertNotIn('"targets"', output.getvalue())
        self.assertEqual(
            store.load("trend_a")["targets"],
            {"BTCUSDT": {"qty": 0.2, "signal": 0}},
        )

        published = dict(MODULE.DEFAULT_CONFIG)
        published["orders_per_batch"] = 1
        published["targets"] = {"ETHUSDT": -3}
        publish_request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/api/strategy",
            data=json.dumps(
                {"strategy_name": "trend_a", "config": published}
            ).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with contextlib.redirect_stdout(io.StringIO()):
            with urllib.request.urlopen(publish_request, timeout=2) as response:
                publish_payload = json.load(response)

        self.assertEqual(response.status, 200)
        self.assertEqual(publish_payload["config"]["orders_per_batch"], 5)
        self.assertEqual(
            publish_payload["config"]["targets"],
            {"ETHUSDT": {"qty": -3.0, "signal": 0}},
        )
        self.assertEqual(store.load("trend_a")["orders_per_batch"], 5)

    def test_http_stale_order_parameter_save_returns_409_without_writing(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}
        current = store.save("trend_a", config)
        raw_before = store.client.get(store.key("trend_a"))
        server = ThreadingHTTPServer(
            ("127.0.0.1", 0), MODULE.make_handler(store, "../", WRITE_TOKEN)
        )
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)

        request = urllib.request.Request(
            f"http://127.0.0.1:{server.server_port}/api/order-parameters",
            data=json.dumps(
                {
                    "strategy_name": "trend_a",
                    "expected_updated_at_us": current["updated_at_us"] - 1,
                    "order_parameters": order_parameters(current),
                }
            ).encode(),
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {WRITE_TOKEN}",
            },
            method="POST",
        )
        with self.assertRaises(HTTPError) as raised:
            urllib.request.urlopen(request, timeout=2)
        payload = json.load(raised.exception)

        self.assertEqual(raised.exception.code, 409)
        self.assertIn("reload before saving", payload["error"])
        self.assertEqual(store.client.get(store.key("trend_a")), raw_before)

    def test_http_order_parameter_save_requires_configured_bearer_token(self):
        store = fake_store()
        current = store.save("trend_a", dict(MODULE.DEFAULT_CONFIG))
        raw_before = store.client.get(store.key("trend_a"))
        payload = json.dumps(
            {
                "strategy_name": "trend_a",
                "expected_updated_at_us": current["updated_at_us"],
                "order_parameters": order_parameters(current),
            }
        ).encode()

        for token, expected_status in ((WRITE_TOKEN, 401), (None, 503)):
            server = ThreadingHTTPServer(
                ("127.0.0.1", 0), MODULE.make_handler(store, "../", token)
            )
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            request = urllib.request.Request(
                f"http://127.0.0.1:{server.server_port}/api/order-parameters",
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with self.assertRaises(HTTPError) as raised:
                urllib.request.urlopen(request, timeout=2)
            self.assertEqual(raised.exception.code, expected_status)

        self.assertEqual(store.client.get(store.key("trend_a")), raw_before)

    def test_token_loader_accepts_systemd_environment_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = pathlib.Path(temp_dir) / "config-write.env"
            path.write_text(
                f"# manager write access\n{MODULE.ORDER_PARAMETER_TOKEN_ENV}={WRITE_TOKEN}\n",
                encoding="utf-8",
            )
            path.chmod(0o600)

            self.assertEqual(MODULE.load_order_parameter_token(str(path)), WRITE_TOKEN)

    def test_each_publish_refreshes_strategy_update_time(self):
        store = fake_store()
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": 0.2}

        with mock.patch.object(
            MODULE.time, "time_ns", side_effect=[1_700_000_000_000_001_000, 1_700_000_000_100_002_000]
        ):
            first = store.save("trend_a", config)
            second = store.save("trend_a", config)

        self.assertEqual(first["updated_at_us"], 1_700_000_000_000_001)
        self.assertEqual(second["updated_at_us"], 1_700_000_000_100_002)
        self.assertEqual(store.load("trend_a")["updated_at_us"], second["updated_at_us"])

    def test_existing_strategy_publish_updates_targets_but_preserves_order_parameters(self):
        store = fake_store()
        initial = dict(MODULE.DEFAULT_CONFIG)
        initial["single_order_usdt"] = 250
        initial["orders_per_batch"] = 5
        initial["targets"] = {"BTCUSDT": 0.2}
        store.save("trend_a", initial)
        published = dict(MODULE.DEFAULT_CONFIG)
        published["single_order_usdt"] = 25
        published["orders_per_batch"] = 1
        published["targets"] = {"ETHUSDT": -3}

        updated = store.save("trend_a", published)

        self.assertEqual(updated["single_order_usdt"], 250.0)
        self.assertEqual(updated["orders_per_batch"], 5)
        self.assertEqual(updated["targets"], {"ETHUSDT": {"qty": -3.0, "signal": 0}})

    def test_strategy_names_write_independent_keys(self):
        store = fake_store()
        first = dict(MODULE.DEFAULT_CONFIG)
        first["targets"] = {"btcusdt": 0.2}
        second = dict(MODULE.DEFAULT_CONFIG)
        second["targets"] = {"ETHUSDT": -3}

        store.save("trend_a", first)
        store.save("trend_b", second)

        self.assertEqual(store.list_strategy_names(), ["trend_a", "trend_b"])
        self.assertEqual(
            store.load("trend_a")["targets"],
            {"BTCUSDT": {"qty": 0.2, "signal": 0}},
        )
        self.assertEqual(
            store.load("trend_b")["targets"],
            {"ETHUSDT": {"qty": -3.0, "signal": 0}},
        )
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
        self.assertEqual(
            store.load("trend_a")["targets"],
            {"BTCUSDT": {"qty": 0.2, "signal": 0}},
        )
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

        config = dict(MODULE.DEFAULT_CONFIG)
        config["max_batch"] = 0
        with self.assertRaisesRegex(ValueError, "max_batch"):
            MODULE.normalize_exec_config(config)

    def test_invalid_symbol_is_rejected(self):
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTC-USDT": 1}
        with self.assertRaisesRegex(ValueError, "invalid symbol"):
            MODULE.normalize_exec_config(config)

    def test_target_objects_keep_signal_and_legacy_qty_defaults_to_zero(self):
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {
            "BTCUSDT": {"qty": 0.03, "signal": -1},
            "ETHUSDT": -0.5,
        }

        normalized = MODULE.normalize_exec_config(config)

        self.assertEqual(
            normalized["targets"],
            {
                "BTCUSDT": {"qty": 0.03, "signal": -1},
                "ETHUSDT": {"qty": -0.5, "signal": 0},
            },
        )

    def test_invalid_target_signal_is_rejected(self):
        config = dict(MODULE.DEFAULT_CONFIG)
        config["targets"] = {"BTCUSDT": {"qty": 0.03, "signal": 3}}
        with self.assertRaisesRegex(ValueError, "signal must be one of"):
            MODULE.normalize_exec_config(config)

    def test_symbol_can_be_split_across_strategy_names(self):
        store = fake_store()
        first = dict(MODULE.DEFAULT_CONFIG)
        first["targets"] = {"BTCUSDT": 0.2}
        second = dict(MODULE.DEFAULT_CONFIG)
        second["targets"] = {"BTCUSDT": -0.1}
        store.save("trend_a", first)
        store.save("trend_b", second)

        self.assertEqual(
            store.load("trend_a")["targets"],
            {"BTCUSDT": {"qty": 0.2, "signal": 0}},
        )
        self.assertEqual(
            store.load("trend_b")["targets"],
            {"BTCUSDT": {"qty": -0.1, "signal": 0}},
        )


if __name__ == "__main__":
    unittest.main()
