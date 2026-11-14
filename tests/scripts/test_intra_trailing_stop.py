import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import arb_per_symbol_overrides as ps


class FakeRedis:
    def __init__(self):
        self.values = {}

    def get(self, key):
        return self.values.get(key)

    def set(self, key, value):
        self.values[key] = value.encode()


class TrailingStopTests(unittest.TestCase):
    def test_roundtrip_and_removal(self):
        redis = FakeRedis()
        context = (redis, "intra01", "binance-margin", "binance-futures")
        self.assertEqual(ps.read_intra_trailing_stop(*context)["values"], {})
        config = {"btc-usdt": {"take_profit": 0.005, "reward_risk_ratio": 2}}
        saved = ps.write_intra_trailing_stop(*context, config)
        self.assertEqual(saved["key"], "intra01:binance-margin:binance-futures:intra_trailing_stop_overrides")
        self.assertEqual(ps.read_intra_trailing_stop(*context), saved)
        self.assertEqual(json.loads(redis.get(saved["key"])), saved["values"])
        ps.write_intra_trailing_stop(*context, {})
        self.assertEqual(ps.read_intra_trailing_stop(*context)["values"], {})

    def test_invalid_config_never_overwrites(self):
        redis = FakeRedis()
        context = (redis, "intra01", "binance-margin", "binance-futures")
        for config in [
            {"take_profit": 0, "reward_risk_ratio": 2},
            {"take_profit": 0.005, "reward_risk_ratio": 0},
            {"take_profit": 0.5, "reward_risk_ratio": 0.1},
            {"take_profit": float("nan"), "reward_risk_ratio": 2},
            {"take_profit": True, "reward_risk_ratio": 2},
            {"take_profit": 0.005},
            {"take_profit": 0.005, "reward_risk_ratio": 2, "extra": 1},
        ]:
            with self.subTest(config=config), self.assertRaises(ValueError):
                ps.write_intra_trailing_stop(*context, {"BTCUSDT": config})
        self.assertEqual(redis.values, {})


if __name__ == "__main__":
    unittest.main()
