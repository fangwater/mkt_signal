"""Unit tests for open_offset_lower helpers in arb_per_symbol_overrides."""

from __future__ import annotations

import json
import os
import sys
import unittest

# Make scripts/ importable.
SCRIPTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "scripts")
sys.path.insert(0, os.path.abspath(SCRIPTS_DIR))

import arb_per_symbol_overrides as ps  # noqa: E402


class FakeRedis:
    """Minimal in-memory Redis stub: get/set on str values."""

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
        self.store[key] = value


class TestOpenOffsetLowerKey(unittest.TestCase):
    def test_key_format(self):
        self.assertEqual(
            ps.make_open_offset_lower_key("mkt1", "binance-margin", "binance-futures"),
            "mkt1:binance-margin:binance-futures:open_offset_lower_overrides",
        )


class TestNormalize(unittest.TestCase):
    def test_normalize_accepts_positive_zero_and_finite(self):
        out = ps.normalize_open_offset_lower_mapping(
            {"btcusdt": 0.001, "ETH-USDT": 0, "SOL_USDT": 0.0008}
        )
        self.assertEqual(out, {"BTCUSDT": 0.001, "ETHUSDT": 0.0, "SOLUSDT": 0.0008})

    def test_normalize_rejects_negative(self):
        with self.assertRaises(ValueError):
            ps.normalize_open_offset_lower_mapping({"BTCUSDT": -0.001})

    def test_normalize_rejects_non_finite(self):
        with self.assertRaises(ValueError):
            ps.normalize_open_offset_lower_mapping({"BTCUSDT": float("inf")})

    def test_normalize_rejects_non_dict(self):
        with self.assertRaises(ValueError):
            ps.normalize_open_offset_lower_mapping([("BTCUSDT", 0.001)])

    def test_normalize_empty_inputs(self):
        self.assertEqual(ps.normalize_open_offset_lower_mapping(None), {})
        self.assertEqual(ps.normalize_open_offset_lower_mapping({}), {})


class TestDumps(unittest.TestCase):
    def test_dumps_sorts_keys_and_collapses_whitespace(self):
        text = ps.dumps_open_offset_lower_mapping({"ETHUSDT": 0.0008, "BTCUSDT": 0.001})
        self.assertEqual(text, '{"BTCUSDT":0.001,"ETHUSDT":0.0008}')


class TestReadWriteRoundtrip(unittest.TestCase):
    def test_round_trip(self):
        rds = FakeRedis()
        result = ps.write_open_offset_lower(
            rds, "mkt1", "binance-margin", "binance-futures",
            {"btcusdt": 0.001, "ETHUSDT": 0.0008},
        )
        self.assertEqual(result["count"], 2)
        self.assertEqual(
            result["key"], "mkt1:binance-margin:binance-futures:open_offset_lower_overrides"
        )
        loaded = ps.read_open_offset_lower(
            rds, "mkt1", "binance-margin", "binance-futures",
        )
        self.assertEqual(loaded["values"], {"BTCUSDT": 0.001, "ETHUSDT": 0.0008})
        self.assertEqual(loaded["count"], 2)

    def test_read_missing_key_returns_empty(self):
        rds = FakeRedis()
        loaded = ps.read_open_offset_lower(rds, "mkt1", "ov", "hv")
        self.assertEqual(loaded["values"], {})
        self.assertEqual(loaded["count"], 0)

    def test_read_invalid_json_raises(self):
        rds = FakeRedis()
        rds.set("mkt1:ov:hv:open_offset_lower_overrides", "not json")
        with self.assertRaises(ValueError):
            ps.read_open_offset_lower(rds, "mkt1", "ov", "hv")


if __name__ == "__main__":
    unittest.main()
