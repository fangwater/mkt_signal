import importlib.util
import pathlib
import threading
import unittest


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
    store._save_lock = threading.Lock()
    return store


class ExecConfigServerTests(unittest.TestCase):
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

    def test_unindexed_config_key_is_not_discovered(self):
        store = fake_store()
        store.client.set(store.key("orphan"), "{}")
        self.assertEqual(store.list_strategy_names(), [])

    def test_strategy_index_name_is_reserved(self):
        with self.assertRaisesRegex(ValueError, "reserved"):
            MODULE.validate_strategy_name("strategy_names")

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
