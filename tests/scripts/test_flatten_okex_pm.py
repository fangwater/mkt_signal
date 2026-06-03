import importlib.util
import sys
import unittest
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "flatten_okex_pm.py"
spec = importlib.util.spec_from_file_location("flatten_okex_pm", SCRIPT)
mod = importlib.util.module_from_spec(spec)
assert spec.loader is not None
sys.modules[spec.name] = mod
spec.loader.exec_module(mod)


def make_spec():
    return mod.SymbolSpec(
        symbol="OKBUSDT",
        asset="OKB",
        spot_inst="OKB-USDT",
        swap_inst="OKB-USDT-SWAP",
        spot_lot=Decimal("0.0001"),
        spot_min_sz=Decimal("0.0001"),
        swap_lot=Decimal("0.01"),
        swap_min_sz=Decimal("0.01"),
        swap_contract_size=Decimal("1"),
    )


class TestFlattenOkexPm(unittest.TestCase):
    def test_okx_balance_uses_cashbal_not_availeq_for_spot_net(self):
        balances = mod.parse_balance_details([
            {
                "ccy": "OKB",
                "cashBal": "12.34",
                "eq": "12.34",
                "availEq": "0",
                "availBal": "0",
                "liab": "0",
                "interest": "0",
            }
        ])

        available, spot_net, borrowed, interest = balances["OKB"]
        self.assertEqual(available, Decimal("0"))
        self.assertEqual(spot_net, Decimal("12.34"))
        self.assertEqual(borrowed, Decimal("0"))
        self.assertEqual(interest, Decimal("0"))

        state = mod.SymbolState(
            spec=make_spec(),
            available=available,
            spot_net=spot_net,
            borrowed=borrowed,
            interest=interest,
            swap_position_coins=Decimal("0"),
        )
        plan = mod.plan_symbol(state, "align")

        self.assertEqual(plan.net_qty, Decimal("12.34"))
        self.assertEqual(plan.swap_side, "sell")
        self.assertEqual(plan.swap_contracts, Decimal("12.34"))


if __name__ == "__main__":
    unittest.main()
