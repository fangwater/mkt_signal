import importlib.util
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "flatten_bitget_pm.py"
spec = importlib.util.spec_from_file_location("flatten_bitget_pm", SCRIPT)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def futures_instrument():
    return {
        "symbol": "GRTUSDT",
        "quantityMultiplier": "0.1",
        "minOrderQty": "0.1",
    }


def futures_only_spec():
    return mod.SymbolSpec(
        symbol="GRTUSDT",
        asset="GRT",
        spot_category=None,
        spot_qty_step=Decimal("0"),
        spot_min_qty=Decimal("0"),
        spot_quote_step=Decimal("1"),
        spot_min_amount=Decimal("0"),
        futures_qty_step=Decimal("0.1"),
        futures_min_qty=Decimal("0.1"),
    )


class TestFlattenBitgetPm(unittest.TestCase):
    def test_fetch_specs_accepts_futures_only_delisted_cash_symbol(self):
        categories = {
            "MARGIN": {},
            "SPOT": {},
            "USDT-FUTURES": {"GRTUSDT": futures_instrument()},
        }
        with mock.patch.object(
            mod, "fetch_instruments", side_effect=lambda category, _symbols: categories[category]
        ):
            specs = mod.fetch_specs(["GRTUSDT"])

        result = specs["GRTUSDT"]
        self.assertIsNone(result.spot_category)
        self.assertEqual(result.futures_qty_step, Decimal("0.1"))
        self.assertEqual(result.futures_min_qty, Decimal("0.1"))

    def test_clear_closes_futures_when_cash_instrument_is_absent(self):
        state = mod.SymbolState(
            spec=futures_only_spec(),
            free=Decimal("0"),
            borrowed=Decimal("0"),
            interest=Decimal("0"),
            futures_position=Decimal("-90159.7"),
            mark_price=Decimal("0"),
        )

        plan = mod.plan_symbol(state, "clear")

        self.assertEqual(plan.futures_side, "buy")
        self.assertEqual(plan.futures_qty, Decimal("90159.7"))
        self.assertTrue(plan.futures_reduce_only)
        self.assertIsNone(plan.futures_skip_reason)
        self.assertEqual(plan.buyback_amt, Decimal("0"))
        self.assertEqual(plan.selldown_amt, Decimal("0"))

    def test_clear_reports_untradable_cash_residual(self):
        state = mod.SymbolState(
            spec=futures_only_spec(),
            free=Decimal("12.3"),
            borrowed=Decimal("0"),
            interest=Decimal("0"),
            futures_position=Decimal("0"),
            mark_price=Decimal("0"),
        )

        plan = mod.plan_symbol(state, "clear")

        self.assertEqual(plan.selldown_amt, Decimal("0"))
        self.assertIn("instrument unavailable", plan.selldown_skip_reason)


if __name__ == "__main__":
    unittest.main()
