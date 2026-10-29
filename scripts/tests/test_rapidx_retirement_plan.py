import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from rapidx_retirement_plan import build_plan


def asset(coin, balance, debt, available, price='2'):
    return {'coin': coin, 'balance': balance, 'debt': debt,
            'available': available, 'indexPrice': price}


def rule(state='live', lot='1', minimum='1', notional='5'):
    return {'state': state, 'lotSize': lot, 'minSize': minimum, 'minNotional': notional}


class RetirementPlanTests(unittest.TestCase):
    def test_separates_debt_and_cash_and_perpetual(self):
        rules = {'BINANCE_MARGIN_BTC_USDT': rule(lot='0.01', minimum='0.01'),
                 'BINANCE_PERP_BTC_USDT': rule(lot='0.01', minimum='0.01')}
        rows = build_plan([asset('BTC', '3', '5', '1', '100'),
                           asset('USDT', '100', '0', '100')], {'BTC': '1.2'}, rules)
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]['action'], 'repay_then_reassess')
        self.assertEqual(rows[0]['net'], '-2')
        self.assertEqual(rows[1]['action'], 'sell')

    def test_dust_and_inactive_are_explicit(self):
        rules = {'BINANCE_MARGIN_X_USDT': rule(), 'BINANCE_MARGIN_Y_USDT': rule('inactive')}
        rows = build_plan([asset('X', '1', '0', '1'), asset('Y', '10', '0', '10')], {}, rules)
        self.assertEqual(rows[0]['reason'], 'below_minimum_order')
        self.assertEqual(rows[1]['reason'], 'missing_or_inactive_market')

    def test_fractional_perp_lot_is_blocked(self):
        rows = build_plan([], {'X': '1.5'}, {'BINANCE_PERP_X_USDT': rule()})
        self.assertEqual(rows[0]['reason'], 'invalid_or_below_minimum_quantity')


if __name__ == '__main__':
    unittest.main()
