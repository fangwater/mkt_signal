import importlib.util
import sys
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
SCRIPT = SCRIPTS / "flatten_margin_and_um.py"
spec = importlib.util.spec_from_file_location("flatten_margin_and_um", SCRIPT)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


class TestSpotExcessPlan(unittest.TestCase):
    def build_order(self, spot_qty: str, um_qty: str):
        position = mod.MarginPosition(
            asset="BTC",
            symbol="BTCUSDT",
            quantity=Decimal(spot_qty),
        )
        return mod.build_spot_excess_order(
            position,
            Decimal(um_qty),
            precision=8,
            min_qty=Decimal("0"),
            qty_rule=mod.QuantityRule(
                min_qty=Decimal("0.001"),
                step_size=Decimal("0.001"),
            ),
        )

    def test_sells_only_spot_quantity_above_short_um_absolute_value(self):
        order, remaining = self.build_order("10", "-7")

        self.assertIsNotNone(order)
        self.assertEqual(order.side, "SELL")
        self.assertEqual(order.quantity, Decimal("3"))
        self.assertEqual(remaining, Decimal("0"))

    def test_does_nothing_when_um_absolute_value_is_larger(self):
        order, remaining = self.build_order("7", "-10")

        self.assertIsNone(order)
        self.assertEqual(remaining, Decimal("0"))

    def test_does_nothing_when_spot_and_um_absolute_value_are_equal(self):
        order, remaining = self.build_order("7", "7")

        self.assertIsNone(order)
        self.assertEqual(remaining, Decimal("0"))

    def test_uses_um_absolute_value_for_long_um_position(self):
        order, remaining = self.build_order("10", "7")

        self.assertIsNotNone(order)
        self.assertEqual(order.side, "SELL")
        self.assertEqual(order.quantity, Decimal("3"))
        self.assertEqual(remaining, Decimal("0"))

    def test_reports_untradable_rounded_excess(self):
        order, remaining = self.build_order("10.0005", "-10")

        self.assertIsNone(order)
        self.assertEqual(remaining, Decimal("0.0005"))

    def test_execute_path_submits_spot_order_and_never_submits_um_order(self):
        margin_position = mod.MarginPosition(
            asset="BTC",
            symbol="BTCUSDT",
            quantity=Decimal("10"),
        )
        um_position = mod.UmPosition(
            symbol="BTCUSDT",
            position_side="BOTH",
            quantity=Decimal("-7"),
        )
        qty_rule = mod.QuantityRule(
            min_qty=Decimal("0.001"),
            step_size=Decimal("0.001"),
        )

        argv = [
            "flatten_margin_and_um.py",
            "--mode",
            "spot-excess",
            "--margin-account-kind",
            "spot",
            "--no-fallback",
            "--symbol",
            "BTCUSDT",
            "--execute",
        ]
        with (
            patch.object(sys, "argv", argv),
            patch.object(mod, "load_credentials", return_value=("key", "secret")),
            patch.object(
                mod,
                "fetch_margin_positions",
                return_value=(200, "{}", [margin_position]),
            ),
            patch.object(
                mod,
                "fetch_um_positions",
                return_value=(200, "{}", [um_position]),
            ),
            patch.object(mod, "fetch_quantity_rules", return_value={"BTCUSDT": qty_rule}),
            patch.object(mod, "submit_margin_order", return_value=200) as submit_margin,
            patch.object(mod, "submit_um_order", return_value=200) as submit_um,
        ):
            mod.main()

        submitted_order = submit_margin.call_args.args[0]
        self.assertEqual(submitted_order.side, "SELL")
        self.assertEqual(submitted_order.quantity, Decimal("3"))
        submit_um.assert_not_called()


if __name__ == "__main__":
    unittest.main()
