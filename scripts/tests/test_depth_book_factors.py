"""Depth formulas match the current reference and accept normal CTA rule input."""
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
from cta_perp_config_server import normalize_factor_rules
from cta_perp_factor_config import normalize_table


class DepthBookFactorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reference = ROOT / "docs/reference/depth_book_factors.py"
        cls.fixture = json.loads((ROOT / "tests/fixtures/depth_book_factors.json").read_text())
        spec = importlib.util.spec_from_file_location("depth_reference", cls.reference)
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    def test_rust_fixture_matches_current_python(self):
        import numpy as np
        import pandas as pd
        self.assertEqual(hashlib.sha256(self.reference.read_bytes()).hexdigest(),
                         self.fixture["reference_sha256"])
        for case in self.fixture["cases"]:
            frame = pd.DataFrame([case["input"]])
            for function in self.module.depth_factor_functions():
                with self.subTest(case=case["name"], factor=function.__name__):
                    actual = float(function(frame)[0])
                    expected = case["expected"][function.__name__]
                    if expected == "NaN":
                        self.assertTrue(np.isnan(actual))
                    else:
                        self.assertEqual(actual, expected)

    def test_signed_net_flow_denominator_handles_withdrawals_and_cancellation(self):
        import numpy as np
        import pandas as pd
        pairs = [(20.0, 10.0), (-20.0, -10.0), (10.0, -10.0),
                 (-10.0, 20.0), (0.0, -10.0), (0.0, 0.0)]
        expected = [1/3, -1/3, 1.0, -1.0, 1.0, np.nan]
        columns = {}
        for prefix in ("depth_", "depth_touch_"):
            for side, index in (("bid", 0), ("ask", 1)):
                columns[f"{prefix}{side}_add_notional"] = [max(pair[index], 0) for pair in pairs]
                columns[f"{prefix}{side}_sub_notional"] = [max(-pair[index], 0) for pair in pairs]
        for function in [self.module.depth_flow_net, self.module.depth_touch_net]:
            np.testing.assert_allclose(function(pd.DataFrame(columns)), expected, equal_nan=True)
        maximum = np.finfo(np.float64).max
        actual = self.module._net_imb(pd.DataFrame({"bid": [maximum, np.inf, np.nan],
                                                  "ask": [-maximum, 1.0, 1.0]}), "bid", "ask")
        np.testing.assert_allclose(actual, [1.0, np.nan, np.nan], equal_nan=True)

    def test_all_twenty_three_factors_accept_signed_rules_and_nq(self):
        rules = [{"标的": "BTC", "因子": name, "符号": -1 if i % 2 else 1,
                  "nq": ["关闭", "0.50", "0.2/0.8"][i % 3]}
                 for i, name in enumerate(self.module.DEPTH_FACTOR_NAMES)]
        normalized, table = normalize_factor_rules(rules)
        self.assertEqual(len(normalized), 23)
        self.assertEqual(set(table["BTCUSDT"]), set(self.module.DEPTH_FACTOR_NAMES))
        self.assertEqual(normalize_table(table), table)
        with self.assertRaises(ValueError):
            normalize_table({"BTCUSDT": {"depth_nonexistent": 1}})


if __name__ == "__main__":
    unittest.main()
