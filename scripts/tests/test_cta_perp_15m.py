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
CONFIG = ROOT / "config/cta_perp_15m"
START = 1800000000000


class FifteenMinuteTests(unittest.TestCase):
    def test_rx01_framework_waits_for_a_new_operator_list(self):
        root = ROOT / "config/cta_perp_rx01"
        config = normalize_config(json.loads((root / "cta_perp.json").read_text()))
        self.assertFalse(config["enabled"])
        self.assertEqual(config["bar_interval"], "15m")
        self.assertEqual(config["factor_rules"], [])
        self.assertEqual(json.loads((root / "cta_perp_symbol_factors.json").read_text()), {})
        universe = available_universe(root / "cta_perp_factor_model_15m_pub.toml")
        self.assertEqual(universe, {"symbols": [], "factors": []})
        result = subprocess.run(["bash", str(ROOT / "scripts/deploy_cta_perp_rx01.sh")],
                                check=True, capture_output=True, text=True)
        self.assertIn("binance-cta-perp-rx01", result.stdout)
        self.assertIn("config/cta_perp_rx01", result.stdout)
        self.assertIn("DRY-RUN", result.stdout)

    def test_fifteen_minute_pair_rules_preserve_rx03_strict_nq_semantics(self):
        config = json.loads((CONFIG / "cta_perp.json").read_text())
        config["factor_rules"] = [
            {"标的": "BEL", "因子": "factor_094", "符号": -1, "nq": "0.2/0.8"},
            {"标的": "BEL", "因子": "factor_051", "符号": 1, "nq": "关闭"},
        ]
        config = normalize_config(config)
        row = dict(bar_interval="15m", symbol="BELUSDT", model_ts_ms=START + 900000,
                   inputs={"factor_094": dict(score=1, quantile=.01, ready=True)},
                   nq=dict(long_value=0, long_threshold=.01, long_quantile=.21,
                           short_value=0, short_threshold=-.01, short_quantile=.79,
                           ready=True, execution_ts_ms=START + 900001,
                           execution_bid=100, execution_ask=100))
        settings = dict(start_ms=START, initial_equity_usdt=10000)
        result = simulate([row], {}, config, settings)
        self.assertEqual(result["records"][0]["snapshot"]["position_flag"], 1)
        self.assertTrue(result["records"][0]["snapshot"]["strict_nq"])
        row["nq"]["long_quantile"] = .2
        result = simulate([row], {}, config, settings)
        self.assertEqual(result["records"][0]["snapshot"]["position_flag"], 0)

    def test_lifecycle_uses_fifteen_minute_signal_and_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            env = Path(directory) / "binance-cta-perp-15m-test"
            scripts = env / "scripts"
            scripts.mkdir(parents=True)
            (env / "config").mkdir()
            (env / "config/cta_perp.json").write_text((CONFIG / "cta_perp.json").read_text())
            (env / "env.sh").write_text("")
            calls = env / "calls"
            for name in ["start_cta_perp.sh", "stop_cta_perp.sh"]:
                shutil.copy2(ROOT / "scripts" / name, scripts / name)
            for name in ["cta_perp_config_server.py", "cta_perp_factor_config.py"]:
                (scripts / name).write_text("# local lifecycle fixture\n")
            for action in ["start", "stop"]:
                for role in ["factor_model_15m_pub", "15m_signal", "config_server", "dashboard"]:
                    child = scripts / f"{action}_cta_perp_{role}.sh"
                    child.write_text(f'#!/bin/sh\nprintf "%s\\n" "{action}:{role}" >> "{calls}"\n')
                    child.chmod(0o755)
                subprocess.run(["bash", str(scripts / f"{action}_cta_perp.sh")], check=True, capture_output=True)
                if action == "start":
                    self.assertFalse(calls.exists())
                subprocess.run(["bash", str(scripts / f"{action}_cta_perp.sh"), "--execute"], check=True, capture_output=True)
            self.assertEqual(calls.read_text().splitlines(), [
                "start:factor_model_15m_pub", "start:config_server", "start:dashboard", "start:15m_signal",
                "stop:15m_signal", "stop:factor_model_15m_pub", "stop:dashboard", "stop:config_server",
            ])

    def test_deploy_plan_accepts_fifteen_minutes_only_for_perp(self):
        result = subprocess.run(["bash", str(ROOT / "scripts/deploy_cta_perp_15m.sh"),
            "--env-name", "binance-cta-perp-15m-test"], check=True, capture_output=True, text=True)
        self.assertIn("interval=15m", result.stdout)
        self.assertIn("DRY-RUN", result.stdout)
        result = subprocess.run(["bash", str(ROOT / "scripts/deploy_cta_signal.sh"),
            "--family", "spread", "--bar-interval", "15m", "--env-name", "binance-cta-spread-test"], capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def test_factor_import_cli_selects_fifteen_minute_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            config_dir = Path(directory) / "config"
            shutil.copytree(CONFIG, config_dir)
            result = subprocess.run([
                sys.executable, str(ROOT / "scripts/cta_perp_factor_config.py"),
                "--env-name", "binance-cta-perp-15m", "--config",
                "config/cta_perp_symbol_factors.json",
            ], cwd=directory, capture_output=True, text=True, check=True)
            plan = json.loads(result.stdout)
            self.assertFalse(plan["execute"])
            self.assertEqual(len(plan["symbol_factors"]), 15)

    def test_15m_config_and_all_factor_bindings_are_accepted(self):
        config = normalize_config(json.loads((CONFIG / "cta_perp.json").read_text()))
        table = normalize_table(json.loads((CONFIG / "cta_perp_symbol_factors.json").read_text()))
        universe = available_universe(CONFIG / "cta_perp_factor_model_15m_pub.toml")
        validate_available(table, universe)
        self.assertFalse(config["enabled"])
        self.assertEqual(config["bar_interval"], "15m")
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
        row = dict(bar_interval="15m", symbol="BELUSDT", model_ts_ms=START + 900000,
                   inputs={"factor_094": dict(score=1, quantile=.01, ready=True)},
                   nq=dict(long_value=.02, long_threshold=.02, long_quantile=.1,
                           short_value=-.02, short_threshold=-.02, short_quantile=.9,
                           ready=True, execution_ts_ms=START + 900001,
                           execution_bid=100, execution_ask=100))
        rows = [row]
        for i, q in enumerate([.01, .8], 2):
            following = copy.deepcopy(row)
            following["model_ts_ms"] = START + i * 900000
            following["nq"].update(ready=False, execution_ts_ms=START + i * 900000 + 1)
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

    def test_candles_use_start_labels_and_fifteen_minute_requests(self):
        row = [START, 100, 110, 90, 105, 10, START + 899999, 1000, 2, 5, 500]
        self.assertFalse(normalize_kline(row, START + 899999, 900000)["closed"])
        candle = normalize_kline(row, START + 900000, 900000)
        self.assertTrue(candle["closed"])
        with self.assertRaises(ValueError):
            normalize_kline(row, START + 900000)
        requests = []
        def opener(request, timeout):
            requests.append(parse_qs(urlparse(request.full_url).query))
            return io.BytesIO(json.dumps([row]).encode())
        client = BinanceMinuteClient(opener=opener, interval="15m")
        self.assertEqual(client.fetch("BELUSDT", START, START + 900000), [row])
        self.assertEqual(requests[0]["interval"], ["15m"])
        with tempfile.TemporaryDirectory() as directory:
            store = SignalHistoryStore(Path(directory) / "history.sqlite3", "15m")
            try:
                store.save_klines("BELUSDT", [candle], START + 900000)
                series = store.series("BELUSDT", hours=1, current_ms=START + 900000)
                self.assertEqual(series["interval"], "15m")
                self.assertEqual(series["expected_closed_bars"], 4)
                self.assertEqual(series["klines"][0]["open_time_ms"], START)
                self.assertEqual(len(series["missing_minutes"]), 3)
            finally:
                store.close()
