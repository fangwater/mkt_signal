import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
LIB = ROOT / "scripts" / "lib" / "bybit_external_order_link.py"
spec = importlib.util.spec_from_file_location("bybit_external_order_link", LIB)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class TestBybitExternalOrderLink(unittest.TestCase):
    def test_is_decimal_i64_in_reserved_strategy_namespace(self):
        link = mod.make_external_order_link_id(1, now_ms=1_757_000_000_000)
        self.assertTrue(link.isdigit())
        packed = int(link)
        self.assertGreaterEqual(packed, 0)
        self.assertLessEqual(packed, 2**63 - 1)
        self.assertEqual(packed >> 32, mod.EXTERNAL_ORDER_STRATEGY_ID)
        self.assertEqual(packed & 0xFFFFFFFF, ((1_757_000_000_000 & 0xFFFFFFFF) + 1) & 0xFFFFFFFF)

    def test_seq_differentiates_same_millisecond_batch(self):
        first = mod.make_external_order_link_id(1, now_ms=1_757_000_000_000)
        second = mod.make_external_order_link_id(2, now_ms=1_757_000_000_000)
        self.assertNotEqual(first, second)

    def test_rejects_non_positive_seq(self):
        with self.assertRaises(ValueError):
            mod.make_external_order_link_id(0)


if __name__ == "__main__":
    unittest.main()
