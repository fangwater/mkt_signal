"""The HTTP contract has one trade-size table and independent factor plans."""
import copy
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import sys
import threading
from types import SimpleNamespace
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import tlen_config_server as tlen


class MemoryRedis:
    def __init__(self):
        self.hashes = {
            "binance-futures:amount-thresholds": {
                "BTCUSDT": json.dumps({"medium_notional_threshold": 10, "large_notional_threshold": 20}),
                "ETHUSDT": json.dumps({"medium_notional_threshold": 30, "large_notional_threshold": 40}),
            },
            # An abandoned key must never influence either factor period.
            "binance-futures:amount-thresholds-1m": {
                "WRONGUSDT": json.dumps({"medium_notional_threshold": 999, "large_notional_threshold": 9999})
            },
            "binance-futures:factor-plan": {"ETHUSDT": '{"factors":["factor_002"]}'},
            "binance-futures:factor-plan-1m": {"BTCUSDT": '{"factors":["factor_001"]}'},
        }
        self.reads = []

    def hgetall(self, key):
        self.reads.append(key)
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping):
        self.hashes.setdefault(key, {}).update(mapping)

    def delete(self, key):
        self.hashes.pop(key, None)


class SharedAmountTests(unittest.TestCase):
    def setUp(self):
        self.store = object.__new__(tlen.TlenConfigStore)
        self.store._redis = MemoryRedis()
        handler = type("Handler", (tlen.TlenConfigRequestHandler,), {
            "store": self.store,
            "config": SimpleNamespace(default_venue="binance-futures", default_config_type="amount_thresholds"),
        })
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        self.worker = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.worker.start()
        self.addCleanup(self.close)
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.worker.join(timeout=5)

    def fetch(self, kind):
        with urllib.request.urlopen(self.base + "/api/thresholds?venue=binance-futures&config_type=" + kind) as response:
            return json.load(response)

    def test_shared_amounts_validate_both_periods_without_changing_factor_lists(self):
        original = copy.deepcopy(self.store._redis.hashes)
        self.assertEqual(self.fetch("amount_thresholds")["count"], 2)
        one_minute = self.fetch("factor_plan_1m")["thresholds"]
        base = self.fetch("factor_plan")["thresholds"]
        self.assertEqual(one_minute["BTCUSDT"]["factors"], ["factor_001"])
        self.assertEqual(one_minute["ETHUSDT"]["factors"], [])
        self.assertEqual(base["ETHUSDT"]["factors"], ["factor_002"])
        self.assertEqual(base["BTCUSDT"]["factors"], [])
        self.assertNotIn("binance-futures:amount-thresholds-1m", self.store._redis.reads)
        self.assertEqual(self.store._redis.hashes, original)

    def test_removed_period_specific_amount_endpoint_cannot_read_or_write(self):
        original = copy.deepcopy(self.store._redis.hashes)
        with self.assertRaises(urllib.error.HTTPError) as error:
            self.fetch("amount_thresholds_1m")
        self.assertEqual(error.exception.code, 400)
        error.exception.close()
        payload = {"venue": "binance-futures", "config_type": "amount_thresholds_1m", "thresholds": {}}
        request = urllib.request.Request(self.base + "/api/thresholds/replace", data=json.dumps(payload).encode(),
                                         headers={"Content-Type": "application/json"}, method="POST")
        with self.assertRaises(urllib.error.HTTPError) as error:
            urllib.request.urlopen(request)
        self.assertEqual(error.exception.code, 400)
        error.exception.close()
        self.assertEqual(self.store._redis.hashes, original)

    def test_one_minute_plan_can_use_shared_symbol_without_duplicate_threshold(self):
        payload = {"venue": "binance-futures", "config_type": "factor_plan_1m",
                   "thresholds": {"ETHUSDT": {"factors": ["factor_003"]}}}
        request = urllib.request.Request(self.base + "/api/thresholds/replace", data=json.dumps(payload).encode(),
                                         headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request) as response:
            body = json.load(response)
        self.assertEqual(body["thresholds"]["ETHUSDT"]["factors"], ["factor_003"])
        self.assertEqual(self.store._redis.hashes["binance-futures:factor-plan"],
                         {"ETHUSDT": '{"factors":["factor_002"]}'})


if __name__ == "__main__":
    unittest.main()
