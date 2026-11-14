import json
from pathlib import Path
import tempfile
import unittest

from scripts.cta_spread_config_server import ConfigStore, normalize_config
from scripts.cta_spread_factor_config import factor_key

ROOT = Path(__file__).resolve().parents[2]


class CrossSpreadConfigTests(unittest.TestCase):
    def test_mode_survives_save_and_cannot_reinterpret_persisted_positions(self):
        config = json.loads((ROOT / "config/cta_cross_spread.json").read_text())
        self.assertEqual(normalize_config(config)["mode"], "okex_binance")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cta_cross_spread.json"
            path.write_text(json.dumps(config))
            store = ConfigStore(path)
            loaded, revision = store.load()
            loaded["max_pos_u"] = 300
            saved, revision = store.save(loaded, revision)
            self.assertEqual(saved["mode"], "okex_binance")
            saved.pop("mode")
            with self.assertRaisesRegex(ValueError, "separate strategy environment"):
                store.save(saved, revision)

    def test_cross_strategy_uses_an_independent_factor_namespace(self):
        self.assertNotEqual(factor_key("binance-cta-spread-test"), factor_key("okex-binance-cta-spread-test"))
        with self.assertRaises(ValueError):
            factor_key("binance-cta-perp-test")


if __name__ == "__main__":
    unittest.main()
