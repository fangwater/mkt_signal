import csv
import json
import importlib.util
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
import publish_binance_order_size_thresholds as publish


class PublicationTests(unittest.TestCase):
    def test_replace_requires_current_month_both_markets_and_heartbeat(self):
        meta = {"apply_month": "2026-10"}
        tables = {v: {"BTCUSDT": "unused"} for v in publish.MARKETS.values()}
        publish.validate_publication(tables, meta, "replace", "2026-10")
        with self.assertRaisesRegex(ValueError, "current UTC month"):
            publish.validate_publication(tables, meta, "replace", "2026-11")
        with self.assertRaisesRegex(ValueError, "matching cash/futures"):
            publish.validate_publication({"binance-futures": tables["binance-futures"]}, meta, "replace", "2026-10")
        with self.assertRaisesRegex(ValueError, "heartbeat"):
            publish.validate_publication({v: {"ETHUSDT": "unused"} for v in tables}, meta, "replace", "2026-10")

    def test_csv_rejects_incomplete_duplicate_and_wrong_month_rows(self):
        row = dict(exchange="binance", symbol="BTCUSDT", source_month="2026-09",
                   apply_month="2026-10", medium_quantile="0.5", large_quantile="0.9",
                   medium_notional_threshold="12.34", large_notional_threshold="567.89",
                   sample_count="100000", nonempty_hours="720")
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "thresholds.csv"

            def write(rows):
                with path.open("w") as handle:
                    writer = csv.DictWriter(handle, fieldnames=list(row))
                    writer.writeheader()
                    writer.writerows(rows)

            write([row])
            tables, meta = publish.load_csv(path)
            self.assertEqual(set(tables), {"binance-margin"})
            self.assertEqual(json.loads(tables["binance-margin"]["BTCUSDT"]),
                             {"medium_notional_threshold": 12.34, "large_notional_threshold": 567.89})
            for patch in [{"nonempty_hours": "719"}, {"apply_month": "2026-09"},
                          {"large_notional_threshold": "nan"}, {"medium_notional_threshold": "0"},
                          {"medium_notional_threshold": "600"}, {"medium_quantile": "0.2"}]:
                with self.subTest(patch=patch):
                    write([{**row, **patch}])
                    with self.assertRaises(ValueError):
                        publish.load_csv(path)
            write([row, row])
            with self.assertRaisesRegex(ValueError, "duplicate"):
                publish.load_csv(path)

    def before(self):
        keys = publish.managed_keys(["binance-futures"])
        result = {k: {"type": "none", "values": {}, "pttl": -2} for k in keys}
        old = {"BTCUSDT": publish.encode({"medium_notional_threshold": 10, "large_notional_threshold": 20}),
               "OLDUSDT": publish.encode({"medium_notional_threshold": 1, "large_notional_threshold": 2})}
        for key in keys:
            if key.endswith("amount-thresholds"):
                result[key]["values"] = old.copy()
            elif "factor-plan" in key:
                result[key]["values"] = {s: '{"factors":["baseline_047"]}' for s in old}
        return result

    def test_replace_uses_one_threshold_table_and_keeps_factor_plans_independent(self):
        fresh = publish.encode({"medium_notional_threshold": 12, "large_notional_threshold": 30})
        incoming = {"binance-futures": {"BTCUSDT": fresh, "NEWUSDT": fresh}}
        meta = {"source_month": "2026-09", "apply_month": "2026-10", "csv_sha256": "new"}
        after = publish.build_replacement(incoming, meta, self.before(), "replace")
        main = "binance-futures:amount-thresholds"
        self.assertEqual(after[main], incoming["binance-futures"])
        self.assertNotIn(main + "-1m", after)
        self.assertNotIn(main + "-1m:meta", after)
        self.assertEqual(set(after["binance-futures:factor-plan"]), {"BTCUSDT"})
        self.assertEqual(set(after["binance-futures:factor-plan-1m"]), {"BTCUSDT"})
        self.assertEqual(after[main + ":meta"]["source_month"], "2026-09")

    def test_backfill_preserves_values_and_rejects_overwrites_and_mixed_months(self):
        before = self.before()
        main = "binance-futures:amount-thresholds"
        incoming = {"binance-futures": {"NEWUSDT": before[main]["values"]["BTCUSDT"]}}
        meta = {"source_month": "2026-09", "apply_month": "2026-10", "csv_sha256": "new"}
        with self.assertRaisesRegex(ValueError, "matching recorded"):
            publish.build_replacement(incoming, meta, before, "backfill")
        before[main + ":meta"]["values"] = {**meta, "csv_sha256_history": '["first"]'}
        after = publish.build_replacement(incoming, meta, before, "backfill")
        self.assertEqual(len(after[main]), 3)
        self.assertEqual(after[main]["BTCUSDT"], before[main]["values"]["BTCUSDT"])
        self.assertEqual(json.loads(after[main + ":meta"]["csv_sha256_history"]), ["first", "new"])
        incoming["binance-futures"]["BTCUSDT"] = before[main]["values"]["OLDUSDT"]
        with self.assertRaisesRegex(ValueError, "cannot overwrite"):
            publish.build_replacement(incoming, meta, before, "backfill")


@unittest.skipUnless(shutil.which("redis-server") and importlib.util.find_spec("redis"),
                     "requires redis-server and redis-py; uses an isolated temporary Unix socket")
class RedisTransactionTests(unittest.TestCase):
    def setUp(self):
        import redis
        self.temp = tempfile.TemporaryDirectory(prefix="order-size-test-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        socket = self.root / "redis.sock"
        process = subprocess.Popen(["redis-server", "--port", "0", "--unixsocket", str(socket),
                                    "--save", "", "--appendonly", "no", "--dir", str(self.root)],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        def stop():
            process.terminate()
            process.wait(timeout=5)

        self.addCleanup(stop)
        self.client = redis.Redis(unix_socket_path=str(socket), decode_responses=True)
        for _ in range(100):
            try:
                self.client.ping()
                break
            except redis.ConnectionError:
                time.sleep(0.02)
        else:
            self.fail("temporary Redis failed to start")
        self.before = PublicationTests().before()
        for key, snapshot in self.before.items():
            if snapshot["values"]:
                self.client.hset(key, mapping=snapshot["values"])
        self.main = "binance-futures:amount-thresholds"
        fresh = publish.encode({"medium_notional_threshold": 12, "large_notional_threshold": 30})
        self.tables = {"binance-futures": {"BTCUSDT": fresh, "NEWUSDT": fresh}}
        self.meta = {"source_month": "2026-09", "apply_month": "2026-10", "csv_sha256": "new"}

    def test_dry_run_then_atomic_publish_and_complete_backup(self):
        keys = publish.managed_keys(self.tables)
        before = publish.snapshot(self.client, keys)
        report = publish.publish(self.client, self.tables, self.meta, "replace", False, None)
        self.assertFalse(report["execute"])
        self.assertEqual(publish.snapshot(self.client, keys), before)
        backup = self.root / "before.json"
        report = publish.publish(self.client, self.tables, self.meta, "replace", True, backup)
        self.assertTrue(report["verified"])
        self.assertEqual(json.loads(backup.read_text())["before"], before)
        self.assertEqual(self.client.hgetall(self.main), self.tables["binance-futures"])
        self.assertFalse(self.client.exists(self.main + "-1m"))
        self.assertEqual(set(self.client.hgetall("binance-futures:factor-plan")), {"BTCUSDT"})
        with self.assertRaises(FileExistsError):
            publish.publish(self.client, self.tables, self.meta, "replace", True, backup)

    def test_concurrent_change_aborts_all_replacements(self):
        import redis
        original = publish.build_replacement

        def conflict(*args):
            result = original(*args)
            self.client.hset(self.main, "CONCURRENTUSDT", self.tables["binance-futures"]["BTCUSDT"])
            return result

        with patch.object(publish, "build_replacement", side_effect=conflict):
            with self.assertRaises(redis.WatchError):
                publish.publish(self.client, self.tables, self.meta, "replace", True, self.root / "before.json")
        self.assertTrue(self.client.hexists(self.main, "OLDUSDT"))
        self.assertTrue(self.client.hexists(self.main, "CONCURRENTUSDT"))
        self.assertFalse(self.client.hexists(self.main, "NEWUSDT"))
        self.assertFalse(self.client.exists(self.main + ":meta"))


if __name__ == "__main__":
    unittest.main()
