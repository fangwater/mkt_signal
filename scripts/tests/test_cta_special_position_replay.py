import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from cta_special_position_replay import simulate,factor_snapshot,PositionReplay
ROOT=Path(__file__).resolve().parents[2];START=1791590400000
class PositionReplayTests(unittest.TestCase):
    def setUp(self):
        self.config=json.loads((ROOT/'config/cta_special.json').read_text());self.config['max_pos_u']=1000
        self.settings=dict(start_ms=START,initial_equity_usdt=10000,taker_fee_rate=0)
    def row(self,i,qs,bid=100,ask=100):
        ts=START+i*60000
        return dict(symbol='FILUSDT',model_ts_ms=ts,inputs={f:dict(score=1,quantile=q,ready=True) for f,q in qs.items()},
                    nq=dict(long_value=0,short_value=0,long_quantile=.9,short_quantile=.1,ready=True,execution_ts_ms=ts+1,execution_bid=bid,execution_ask=ask))
    def run_rows(self,rows,table):return simulate(rows,{'FILUSDT':table},self.config,self.settings)
    def test_five_factors_have_five_full_positions_without_netting(self):
        table={f:1 for f in 'ABCDE'}
        result=self.run_rows([self.row(0,{f:.99 if f in 'ABC' else .01 for f in table})],table)
        stats=result['summaries']['FILUSDT']['strategies']
        self.assertEqual([stats[f]['position_qty'] for f in table],[2,2,2,-2,-2])
        self.assertEqual(len(result['records']),5)
        self.assertTrue(all(r['simulation']['actions'][0]['notional']==200 for r in result['records']))
    def test_single_amount_applies_to_every_factor_and_symbol(self):
        self.config['max_pos_u'] = 2000
        fil = self.row(0, {'A': .99, 'B': .01})
        eth = {**self.row(0, {'A': .99, 'B': .01}, 200, 200), 'symbol': 'ETHUSDT'}
        result = simulate([fil, eth], {'FILUSDT': {'A': 1, 'B': 1}, 'ETHUSDT': {'A': 1, 'B': 1}}, self.config, self.settings)
        opens = [a for row in result['records'] for a in row['simulation']['actions']]
        self.assertEqual(len(opens), 4)
        self.assertEqual({a['notional'] for a in opens}, {1000})
        self.assertEqual(sorted(a['qty'] for a in opens), [5, 5, 10, 10])
        self.assertTrue(all(row['snapshot']['max_pos_u'] == 2000 for row in result['records']))
        self.assertNotIn('strategy_configs', result)

    def test_factor_exits_only_its_own_qty(self):
        table={f:1 for f in 'ABCDE'}
        rows=[self.row(0,{f:.99 if f in 'ABC' else .01 for f in table}),self.row(1,dict(A=.5,B=.5,C=.5,D=.8,E=.5),120,120)]
        result=self.run_rows(rows,table)
        self.assertEqual(result['closed_trades'],1)
        self.assertEqual(result['trades'][0]['strategy_name'],'D')
        self.assertEqual(result['trades'][0]['qty'],2)
        self.assertEqual(result['trades'][0]['pnl'],-40)
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['A']['position_qty'],2)
        self.assertEqual(result['records'][8]['simulation']['actions'][0]['target_qty'],'clear')
    def test_repeated_signal_and_price_change_never_add_or_resize(self):
        result=self.run_rows([self.row(0,{'A':.99}),self.row(1,{'A':.99},200,200),self.row(2,{'A':.5},300,300)],{'A':1})
        stats=result['summaries']['FILUSDT']['strategies']['A']
        self.assertEqual(stats['position_qty'],10);self.assertEqual(stats['entry_actions'],1)
    def test_sign_only_no_amplitude_or_other_factor_normalization(self):
        result=self.run_rows([self.row(0,{'A':.01,'B':.99})],{'A':-20,'B':1})
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['A']['position_qty'],5)
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['B']['position_qty'],5)
    def test_taker_close_uses_original_qty_not_current_usdt_budget(self):
        result=self.run_rows([self.row(0,{'A':.99},99,101),self.row(1,{'A':.2},109,111)],{'A':1})
        trade=result['trades'][0]
        self.assertAlmostEqual(trade['qty'],1000/101)
        self.assertAlmostEqual(trade['pnl'],1000/101*8)
    def test_missing_factor_does_not_block_other_factor(self):
        row=self.row(0,{'A':.99,'B':.99});row['inputs']['B']['ready']=False
        result=self.run_rows([row],{'A':1,'B':1})
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['A']['entry_actions'],1)
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['B']['warmup_minutes'],1)
    def test_nq_blocks_own_entry_and_never_blocks_exit(self):
        rows=[self.row(0,{'A':.99}),self.row(1,{'A':.2})];rows[1]['nq']['ready']=False
        self.assertEqual(self.run_rows(rows,{'A':1})['closed_trades'],1)
        rows[0]['nq']['long_quantile']=.49
        self.assertEqual(self.run_rows(rows,{'A':1})['summaries']['FILUSDT']['entry_actions'],0)
    def test_reverse_closes_then_opens_independent_full_short(self):
        result=self.run_rows([self.row(0,{'A':.99}),self.row(1,{'A':.01},200,200),self.row(2,{'A':.01},200,200)],{'A':1})
        actions=result['records'][1]['simulation']['actions']
        self.assertEqual([a['kind'] for a in actions],['close'])
        self.assertEqual(actions[0]['qty'],10);self.assertEqual(result['records'][2]['simulation']['actions'][0]['qty'],5)
    def test_open_position_excluded_from_win_rate(self):
        result=self.run_rows([self.row(0,{'A':.99})],{'A':1})
        self.assertIsNone(result['win_rate']);self.assertEqual(result['closed_trades'],0)
        self.assertEqual(len(result['open_positions']),1)
        position=result['open_positions'][0]
        self.assertIsNone(position['exit_ts_ms'])
        self.assertEqual(position['trade_id'],result['records'][0]['simulation']['actions'][0]['trade_id'])
        self.assertEqual(position['qty'],10)
    def test_closed_pairs_keep_original_qty_and_factor_identity(self):
        result=self.run_rows([self.row(0,{'A':.99,'B':.01}),self.row(1,{'A':.99,'B':.01},110,110),
                              self.row(2,{'A':.2,'B':.8},120,120)],{'A':1,'B':1})
        self.assertEqual(len(result['trades']),2)
        self.assertEqual(result['open_positions'],[])
        for trade in result['trades']:
            actions=[a for r in result['records'] for a in r['simulation']['actions'] if a['trade_id']==trade['trade_id']]
            self.assertEqual([a['kind'] for a in actions],['open','close'])
            self.assertEqual(actions[0]['qty'],actions[1]['qty'])
            self.assertEqual(actions[0]['qty'],trade['qty'])
            self.assertTrue(all(a['strategy_name']==trade['strategy_name'] for a in actions))
    def test_single_factor_series_preserves_window_context_without_duplicate_entry(self):
        result=self.run_rows([self.row(0,{'A':.99,'B':.01}),self.row(1,{'A':.99,'B':.01}),
                              self.row(2,{'A':.2,'B':.01},120,120)],{'A':1,'B':1})
        replay=PositionReplay(None,Path('unused'),Path('unused'),Path('unused'))
        replay.result=result
        series=replay.series('FILUSDT',START+60000,'A')
        self.assertEqual(len(series['context']),1)
        self.assertEqual(series['context'][0]['snapshot']['position_flag'],1)
        self.assertEqual(len(series['trades']),1)
        self.assertEqual(series['open_positions'],[])
        self.assertTrue(all(r['strategy_name']=='A' for r in series['records']))
        self.assertEqual(sum(a['kind']=='open' for r in series['records'] for a in r['simulation']['actions']),0)
        other=replay.series('FILUSDT',START+60000,'B')
        self.assertEqual(len(other['open_positions']),1)
        self.assertEqual(other['trades'],[])
        self.assertNotIn('open_positions',replay.overview())
    def test_before_midnight_bad_execution_and_duplicates(self):
        self.assertEqual(self.run_rows([self.row(-1,{'A':.99})],{'A':1})['records'],[])
        row=self.row(0,{'A':.99});row['nq']['execution_ts_ms']=START-1
        self.assertEqual(self.run_rows([row],{'A':1})['records'],[])
        row=self.row(0,{'A':.99});result=self.run_rows([row,row],{'A':1})
        self.assertEqual(len(result['records']),1)
    def test_reflected_threshold_ties_do_not_enter_or_exit(self):
        result=self.run_rows([self.row(0,{'A':.9})],{'A':-1})
        self.assertEqual(result['summaries']['FILUSDT']['entry_actions'],0)
if __name__=='__main__':unittest.main()
