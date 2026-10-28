#!/usr/bin/env python3
"""Cross-language regression/benchmark; run with --binary PATH [--report PATH]."""

import argparse
import csv
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from tardis_order_size_reference import HEADER, HOUR_US, Sketch, month_bounds, reference

BINARY = None
REPORT = {}


class OrderSizeTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix="tardis-order-size-test-")
        cls.work = Path(cls.temp.name)
        cls.root = cls.work / "source"
        cls.paths = []
        cls.month = "2026-09"
        start, end = month_bounds(cls.month)
        for day in range(30):
            path = cls.root / "binance_usd_btcusdt" / "trades" / f"binance-futures_trades_2026-09-{day+1:02}_BTCUSDT.csv.gz"
            path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(path, "wt", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(HEADER)
                for hour in range(24):
                    hour_index = day * 24 + hour
                    timestamp = start + hour_index * HOUR_US
                    for i in range(600):
                        # Non-sorted distribution, ties, both sides, hourly compression.
                        price = 1 + ((i * 7919 + hour_index * 37) % 10007)
                        amount = (i % 11 + 1) / 8
                        writer.writerow(["binance-futures", "BTCUSDT", timestamp + i,
                                         timestamp + i + 2000, hour_index * 600 + i + 1,
                                         "buy" if i % 2 else "sell", price, amount])
                    if hour == 0:
                        # Same-hour disorder is accepted. Prior-hour disorder is skipped.
                        writer.writerow(["binance-futures", "BTCUSDT", timestamp + 1, timestamp, 900000, "buy", 2, 3])
                        if day:
                            writer.writerow(["binance-futures", "BTCUSDT", timestamp - 1, timestamp, 900001, "buy", 7, 8])
                if day == 0:
                    for price, amount in [("NaN", 1), (1, 0), (-1, 1), ("1e308", "1e308")]:
                        writer.writerow(["binance-futures", "BTCUSDT", start, start, 999999, "buy", price, amount])
                    for ts in (start - 1, end):
                        writer.writerow(["binance-futures", "BTCUSDT", ts, ts, 999999, "sell", 1, 1])
            cls.paths.append(path)
        cls.original = {path: path.read_bytes() for path in cls.paths}

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def setUp(self):
        for path, data in self.original.items():
            path.write_bytes(data)

    def run_tool(self, name, extra=(), success=True):
        out = self.work / name
        started = time.perf_counter()
        process = subprocess.run([str(BINARY), "--root", str(self.root), "--month", self.month,
                                  "--symbols", "BTCUSDT", "--exchanges", "binance-futures",
                                  "--workers", "2", "--output", str(out), *extra],
                                 text=True, capture_output=True)
        elapsed = time.perf_counter() - started
        if success:
            self.assertEqual(process.returncode, 0, process.stderr)
        else:
            self.assertNotEqual(process.returncode, 0, process.stderr)
        return out, elapsed, process

    def test_complete_month_matches_python_and_hashes(self):
        ref = reference(self.paths, "binance-futures", "BTCUSDT", self.month)
        out, rust_seconds, _ = self.run_tool("complete")
        results = json.loads((out / "results.json").read_text())
        self.assertEqual(results["status"], "complete")
        self.assertEqual(results["apply_month"], "2026-10")
        got = results["results"][0]["calculation"]
        for key, value in got["stats"].items():
            self.assertEqual(value, ref[key], key)
        for key in ("medium_notional_threshold", "large_notional_threshold", "sketch"):
            self.assertEqual(got[key], ref[key], key)
        for entry, path in zip(got["files"], self.paths):
            self.assertEqual(entry["sha256"], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertEqual(entry["bytes"], path.stat().st_size)
        self.assertEqual(got["stats"]["invalid_values"], 4)
        self.assertEqual(got["stats"]["late_trades"], 29)
        self.assertEqual(got["stats"]["outside_month"], 2)
        self.assertEqual(got["stats"]["sample_count"], 432030)
        self.assertEqual(got["stats"]["nonempty_hours"], 720)
        self.assertLess(abs(got["medium_notional_threshold"] / ref["exact_p50"] - 1), .02)
        self.assertLess(abs(got["large_notional_threshold"] / ref["exact_p90"] - 1), .02)
        REPORT["fixture"] = {
            "source_rows": got["stats"]["source_rows"], "samples": got["stats"]["sample_count"],
            "python_seconds_including_exact_sort": ref["elapsed_seconds"],
            "rust_seconds_including_process_hashing_and_outputs": rust_seconds,
            "python_rust_sketch_and_all_fields_equal": True,
            "kll_p50": got["medium_notional_threshold"], "exact_p50": ref["exact_p50"],
            "kll_p90": got["large_notional_threshold"], "exact_p90": ref["exact_p90"],
        }
        with (out / "thresholds.csv").open() as stream:
            self.assertEqual(len(list(csv.DictReader(stream))), 1)

    def test_python_small_exact_quantiles(self):
        sketch = Sketch()
        for value in [10., 20., 30., 40., 50.]:
            sketch.insert(value)
        self.assertEqual(sketch.quantile(.5), 30.)
        self.assertAlmostEqual(sketch.quantile(.9), 45.999999046325684)

    def test_missing_leading_middle_trailing_rejected(self):
        for index in [0, 14, 29]:
            path = self.paths[index]
            path.unlink()
            out, _, _ = self.run_tool(f"missing-{index}", success=False)
            result = json.loads((out / "results.json").read_text())
            self.assertEqual(result["status"], "partial")
            self.assertEqual(result["completed"], 0)
            self.assertIn("missing source files", result["results"][0]["error"])
            with (out / "thresholds.csv").open() as stream:
                self.assertEqual(list(csv.DictReader(stream)), [])
            path.write_bytes(self.original[path])

    def test_bad_identity_numeric_header_and_gzip_rejected(self):
        path = self.paths[0]
        text = gzip.decompress(self.original[path]).decode()
        variants = {
            "symbol": text.replace("BTCUSDT", "ETHUSDT", 1),
            "exchange": text.replace("binance-futures", "binance", 1),
            "header": text.replace("price,amount", "amount,price", 1),
            "numeric": text.replace(",sell,1,0.125", ",sell,not-a-number,0.125", 1),
        }
        for name, changed in variants.items():
            self.assertNotEqual(text, changed)
            path.write_bytes(gzip.compress(changed.encode()))
            out, _, _ = self.run_tool("bad-" + name, success=False)
            self.assertIsNone(json.loads((out / "results.json").read_text())["results"][0]["calculation"])
        path.write_bytes(self.original[path][:-8])
        self.run_tool("truncated", success=False)

    def test_empty_hour_not_published(self):
        path = self.paths[0]
        rows = list(csv.reader(io.StringIO(gzip.decompress(self.original[path]).decode())))
        start, _ = month_bounds(self.month)
        rows = [rows[0]] + [r for r in rows[1:] if not start + HOUR_US <= int(r[2]) < start + 2 * HOUR_US]
        with gzip.open(path, "wt", newline="") as stream:
            csv.writer(stream).writerows(rows)
        out, _, _ = self.run_tool("empty-hour", success=False)
        self.assertIn("expected 720, found 719", json.loads((out / "results.json").read_text())["results"][0]["error"])

    def test_multimember_gzip_and_output_protection(self):
        path = self.paths[0]
        data = gzip.decompress(self.original[path])
        cut = data.index(b"\n", len(data) // 2) + 1
        path.write_bytes(gzip.compress(data[:cut]) + gzip.compress(data[cut:]))
        out, _, _ = self.run_tool("multimember")
        self.assertEqual(json.loads((out / "results.json").read_text())["results"][0]["calculation"]["stats"]["sample_count"], 432030)
        before = (out / "results.json").read_bytes()
        self.run_tool("multimember", success=False)
        self.assertEqual(before, (out / "results.json").read_bytes())
        self.run_tool("source/do-not-write", success=False)
        self.assertFalse((self.root / "do-not-write").exists())

    def test_exchanges_are_separate_and_workers_deterministic(self):
        for path in self.paths:
            target = self.root / "binance_spot_btcusdt" / "trades" / path.name.replace("binance-futures", "binance")
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(gzip.compress(gzip.decompress(path.read_bytes()).replace(b"binance-futures", b"binance")))
        cmd = [str(BINARY), "--root", str(self.root), "--month", self.month, "--symbols", "BTCUSDT"]
        outputs = []
        for workers in [1, 4]:
            out = self.work / f"workers-{workers}"
            result = subprocess.run(cmd + ["--workers", str(workers), "--output", str(out)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            outputs.append(json.loads((out / "results.json").read_text())["results"])
        self.assertEqual([r["exchange"] for r in outputs[0]], ["binance-futures", "binance"])
        for first, second in zip(*outputs):
            self.assertEqual(first["calculation"]["sketch"], second["calculation"]["sketch"])
            self.assertEqual(first["calculation"]["stats"], second["calculation"]["stats"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    BINARY = args.binary.resolve()
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(OrderSizeTest))
    REPORT.update(tests_run=result.testsRun, failures=len(result.failures), errors=len(result.errors))
    if args.report:
        args.report.write_text(json.dumps(REPORT, indent=2) + "\n")
    sys.exit(0 if result.wasSuccessful() else 1)
