import copy
import io
import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from cta_perp_config_server import ConfigStore, normalize_config
from cta_perp_factor_config import available_universe, normalize_table, validate_available
from cta_perp_position_replay import simulate
from cta_perp_signal_history import BinanceMinuteClient, SignalHistoryStore, normalize_kline

ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "config/cta_perp_rx04"
START = 1800000000000


class FiveMinuteTests(unittest.TestCase):
    def test_factor_import_cli_selects_five_minute_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            config_dir = Path(directory) / "config"
            shutil.copytree(CONFIG, config_dir)
            result = subprocess.run([
                sys.executable, str(ROOT / "scripts/cta_perp_factor_config.py"),
                "--env-name", "binance-cta-perp-rx04", "--config",
                "config/cta_perp_symbol_factors.json",
            ], cwd=directory, capture_output=True, text=True, check=True)
            plan = json.loads(result.stdout)
            self.assertFalse(plan["execute"])
            self.assertEqual(len(plan["symbol_factors"]), 15)

    def test_rx04_config_and_all_factor_bindings_are_accepted(self):
        config = normalize_config(json.loads((CONFIG / "cta_perp.json").read_text()))
        table = normalize_table(json.loads((CONFIG / "cta_perp_symbol_factors.json").read_text()))
        universe = available_universe(CONFIG / "cta_perp_factor_model_5m_pub.toml")
        validate_available(table, universe)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["bar_interval"], "5m")
        self.assertEqual(len(table), 15)
        self.assertEqual(sum(map(len, table.values())), 48)
        self.assertEqual(len(universe["factors"]), 35)
        self.assertEqual(table["BELUSDT"]["sell_avg_price"], -1)
        self.assertEqual(table["YFIUSDT"]["avg_price"], 1)
        config["entry"]["nq_long_quantile"] = 0.6
        with self.assertRaisesRegex(ValueError, "0.50"):
            normalize_config(config)

    def test_replay_uses_linear_thresholds_including_ties_and_only_gates_entries(self):
        config = normalize_config(json.loads((CONFIG / "cta_perp.json").read_text()))
        row = dict(bar_interval="5m", symbol="BELUSDT", model_ts_ms=START + 300000,
                   inputs={"factor_094": dict(score=1, quantile=.01, ready=True)},
                   nq=dict(long_value=.02, long_threshold=.02, long_quantile=.1,
                           short_value=-.02, short_threshold=-.02, short_quantile=.9,
                           ready=True, execution_ts_ms=START + 300001,
                           execution_bid=100, execution_ask=100))
        rows = [row]
        for i, q in enumerate([.01, .8], 2):
            following = copy.deepcopy(row)
            following["model_ts_ms"] = START + i * 300000
            following["nq"].update(ready=False, execution_ts_ms=START + i * 300000 + 1)
            following["inputs"]["factor_094"]["quantile"] = q
            rows.append(following)
        result = simulate(rows, {"BELUSDT": {"factor_094": -1}}, config,
                          dict(start_ms=START, initial_equity_usdt=10000))
        self.assertEqual([r["snapshot"]["position_flag"] for r in result["records"]], [1, 1, 0])
        self.assertEqual(result["records"][0]["snapshot"]["bar_start_ms"], START)
        self.assertEqual(result["closed_trades"], 1)
        row["bar_interval"] = "1m"
        with self.assertRaisesRegex(ValueError, "bar_interval mismatch"):
            simulate([row], {"BELUSDT": {"factor_094": -1}}, config,
                     dict(start_ms=START, initial_equity_usdt=10000))

    def test_config_cannot_switch_interval_in_existing_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cta_perp.json"
            path.write_text((CONFIG / "cta_perp.json").read_text())
            store = ConfigStore(path)
            config, revision = store.load()
            config["bar_interval"] = "1m"
            with self.assertRaisesRegex(ValueError, "bar_interval is fixed"):
                store.save(config, revision)

    def test_candles_use_start_labels_and_five_minute_requests(self):
        row = [START, 100, 110, 90, 105, 10, START + 299999, 1000, 2, 5, 500]
        self.assertFalse(normalize_kline(row, START + 299999, 300000)["closed"])
        candle = normalize_kline(row, START + 300000, 300000)
        self.assertTrue(candle["closed"])
        with self.assertRaises(ValueError):
            normalize_kline(row, START + 300000)
        requests = []
        def opener(request, timeout):
            requests.append(parse_qs(urlparse(request.full_url).query))
            return io.BytesIO(json.dumps([row]).encode())
        client = BinanceMinuteClient(opener=opener, interval="5m")
        self.assertEqual(client.fetch("BELUSDT", START, START + 300000), [row])
        self.assertEqual(requests[0]["interval"], ["5m"])
        with tempfile.TemporaryDirectory() as directory:
            store = SignalHistoryStore(Path(directory) / "history.sqlite3", "5m")
            try:
                store.save_klines("BELUSDT", [candle], START + 300000)
                series = store.series("BELUSDT", hours=1, current_ms=START + 300000)
                self.assertEqual(series["interval"], "5m")
                self.assertEqual(series["expected_closed_bars"], 12)
                self.assertEqual(series["klines"][0]["open_time_ms"], START)
                self.assertEqual(len(series["missing_minutes"]), 11)
            finally:
                store.close()
