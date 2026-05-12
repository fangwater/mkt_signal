"""Unit tests for MM open_offset_lower helpers in mm_config_server."""

from __future__ import annotations

import os
import sys
import unittest

SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

import mm_config_server as mm  # noqa: E402


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}

    def get(self, key):
        value = self.store.get(key)
        if value is None:
            return None
        return value.encode("utf-8")

    def set(self, key, value):
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        self.store[key] = str(value)


class FakeStore(mm.MMConfigStore):
    def __init__(self, env_name: str):
        self._config = mm.AppConfig(
            host="127.0.0.1",
            port=0,
            default_exchange="okex",
            env_name=env_name,
        )
        self._redis = FakeRedis()

    def redis(self):
        return self._redis


class TestOpenOffsetLowerKey(unittest.TestCase):
    def test_key_format(self):
        self.assertEqual(
            mm.make_open_offset_lower_key("okex_mm_alpha", "okex-futures"),
            "okex_mm_alpha:okex-futures:mm:open_offset_lower",
        )
        self.assertEqual(
            mm.make_open_offset_lower_key("bybit_mm_alpha", "bybit-futures"),
            "bybit_mm_alpha:bybit-futures:mm:open_offset_lower",
        )


class TestNormalizeOpenOffsetLower(unittest.TestCase):
    def test_normalize_uses_amount_u_symbol_rule_and_allows_zero(self):
        out = mm.normalize_open_offset_lower_mapping(
            {"btcusdt": 0.0005, "ETH-USDT": 0, "SOL_USDT": 0.0008}
        )
        self.assertEqual(
            out,
            {"BTCUSDT": 0.0005, "ETHUSDT": 0.0, "SOLUSDT": 0.0008},
        )

    def test_normalize_rejects_negative(self):
        with self.assertRaises(ValueError):
            mm.normalize_open_offset_lower_mapping({"BTCUSDT": -0.0001})

    def test_normalize_rejects_nan(self):
        with self.assertRaises(ValueError):
            mm.normalize_open_offset_lower_mapping({"BTCUSDT": float("nan")})

    def test_normalize_rejects_non_dict(self):
        with self.assertRaises(ValueError):
            mm.normalize_open_offset_lower_mapping([("BTCUSDT", 0.0005)])


class TestReadWriteRoundtrip(unittest.TestCase):
    def test_round_trip_uses_mm_key(self):
        store = FakeStore("okex_mm_alpha")
        result = store.write_open_offset_lower(
            "okex-futures",
            {"btc-usdt": 0.0005, "ETHUSDT": 0.0008},
        )
        self.assertEqual(result["count"], 2)
        self.assertEqual(
            result["key"], "okex_mm_alpha:okex-futures:mm:open_offset_lower"
        )
        self.assertEqual(
            store.redis().store[result["key"]],
            '{"BTCUSDT":0.0005,"ETHUSDT":0.0008}',
        )

        loaded = store.read_open_offset_lower("okex-futures")
        self.assertEqual(
            loaded["values"],
            {"BTCUSDT": 0.0005, "ETHUSDT": 0.0008},
        )


if __name__ == "__main__":
    unittest.main()
