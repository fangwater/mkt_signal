import sys
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import neutralize_rapidx_binance as n
from rapidx_retirement_execute import market_request, submit

RULE = {'state': 'live', 'lotSize': '0.1', 'minSize': '0.1', 'minNotional': '5'}


class FakeClient:
    portfolio = '1234'
    def __init__(self):
        self.posts = 0
        self.order = None
    def identity(self, row):
        assert row['portfolioId'] == self.portfolio
    def request(self, method, path, params):
        if method == 'POST':
            self.posts += 1
            self.order = dict(params, portfolioId=self.portfolio, orderState='FILLED',
                              executedQty='1', feeCoin='X', fee='0.01', rebateCoin='', orderId='1')
            raise OSError('lost reply after exchange accepted')
        return self.order
    def snapshot(self):
        return {'X': Decimal('0.99')}, {}, {'X': Decimal('10')}


class OrderTests(unittest.TestCase):
    def test_margin_buy_is_quote_sized_and_sell_does_not_exceed_available(self):
        buy = market_request('X', 'MARGIN', '-1', '10', RULE, '0')
        self.assertEqual(buy['quoteOrderQty'], '10.10')
        self.assertNotIn('orderQty', buy)
        sell = market_request('X', 'MARGIN', '2', '10', RULE, '1.37')
        self.assertEqual(sell['orderQty'], '1.3')
    def test_perpetual_reduces_only_and_dust_skipped(self):
        req = market_request('X', 'PERP', '-1', '10', RULE, '0')
        self.assertEqual(req['side'], 'BUY')
        self.assertEqual(req['reduceOnly'], 'TRUE')
        self.assertIsNone(market_request('X', 'MARGIN', '0.2', '10', RULE, '0.2'))
    def test_dust_buy_covers_debt_and_creates_sellable_surplus(self):
        req = market_request('X', 'MARGIN', '-0.2', '10', RULE, '0', clear_dust=True)
        self.assertEqual(req['side'], 'BUY')
        self.assertEqual(req['quoteOrderQty'], '12.63')
        req = market_request('X', 'MARGIN', '0.2', '10', RULE, '0.2', clear_dust=True)
        self.assertEqual(req['side'], 'BUY')
        self.assertEqual(req['quoteOrderQty'], '10.50')
        self.assertIsNone(market_request('X', 'MARGIN', '0.02', '10', RULE, '0.02', clear_dust=True))
    def test_unknown_submission_is_queried_without_a_second_post(self):
        with tempfile.TemporaryDirectory() as directory:
            journal = n.Journal(Path(directory) / 'journal.json', '1234')
            client = FakeClient()
            req = market_request('X', 'MARGIN', '-1', '10', RULE, '0')
            req['side'] = 'BUY'
            result = submit(client, journal, req, {'cash': '0', 'perp': '0'})
            self.assertEqual(result['orderState'], 'FILLED')
            self.assertEqual(client.posts, 1)
            self.assertIsNone(journal.data['pending'])
            self.assertEqual(journal.data['orders'][0]['reconciled_quantity'], '0.99')

if __name__ == '__main__':
    unittest.main()
