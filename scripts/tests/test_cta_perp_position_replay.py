import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from cta_perp_position_replay import simulate,factor_snapshot,PositionReplay
ROOT=Path(__file__).resolve().parents[2];START=1791590400000
class PositionReplayTests(unittest.TestCase):
    def setUp(self):
        self.config=json.loads((ROOT/'config/cta_perp.json').read_text());self.config['max_pos_u']=1000
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
        self.assertEqual([stats[f]['position_qty'] for f in table],[10,10,10,-10,-10])
        self.assertEqual(len(result['records']),5)
        self.assertTrue(all(r['simulation']['actions'][0]['notional']==1000 for r in result['records']))
        self.assertEqual(result['allocation'], 'per_symbol_factor')
    def test_single_amount_applies_to_every_factor_and_symbol(self):
        self.config['max_pos_u'] = 2000
        fil = self.row(0, {'A': .99, 'B': .01})
        eth = {**self.row(0, {'A': .99, 'B': .01}, 200, 200), 'symbol': 'ETHUSDT'}
        result = simulate([fil, eth], {'FILUSDT': {'A': 1, 'B': 1}, 'ETHUSDT': {'A': 1, 'B': 1}}, self.config, self.settings)
        opens = [a for row in result['records'] for a in row['simulation']['actions']]
        self.assertEqual(len(opens), 4)
        self.assertEqual({a['notional'] for a in opens}, {2000})
        self.assertEqual(sorted(a['qty'] for a in opens), [10, 10, 20, 20])
        self.assertTrue(all(row['snapshot']['max_pos_u'] == 2000 for row in result['records']))
        self.assertNotIn('strategy_configs', result)

    def test_factor_exits_only_its_own_qty(self):
        table={f:1 for f in 'ABCDE'}
        rows=[self.row(0,{f:.99 if f in 'ABC' else .01 for f in table}),self.row(1,dict(A=.5,B=.5,C=.5,D=.8,E=.5),120,120)]
        result=self.run_rows(rows,table)
        self.assertEqual(result['closed_trades'],1)
        self.assertEqual(result['trades'][0]['strategy_name'],'D')
        self.assertEqual(result['trades'][0]['qty'],10)
        self.assertEqual(result['trades'][0]['pnl'],-200)
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['A']['position_qty'],10)
        self.assertEqual(result['records'][8]['simulation']['actions'][0]['target_qty'],'clear')
    def test_repeated_signal_and_price_change_never_add_or_resize(self):
        result=self.run_rows([self.row(0,{'A':.99}),self.row(1,{'A':.99},200,200),self.row(2,{'A':.5},300,300)],{'A':1})
        stats=result['summaries']['FILUSDT']['strategies']['A']
        self.assertEqual(stats['position_qty'],10);self.assertEqual(stats['entry_actions'],1)
    def test_sign_only_no_amplitude_or_other_factor_normalization(self):
        result=self.run_rows([self.row(0,{'A':.01,'B':.99})],{'A':-20,'B':1})
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['A']['position_qty'],10)
        self.assertEqual(result['summaries']['FILUSDT']['strategies']['B']['position_qty'],10)

    def test_sparse_pairs_each_get_full_amount_despite_flat_disabled_or_unready_peers(self):
        self.config['max_pos_u'] = 500
        fil = self.row(0, {'A': .99, 'B': .01, 'C': .5, 'D': .99, 'E': .99})
        fil['inputs']['E']['ready'] = False
        eth = {**self.row(0, {'B': .01}, 200, 200), 'symbol': 'ETHUSDT'}
        result = simulate([fil, eth], {'FILUSDT': {'A': 1, 'B': 1, 'C': 1, 'D': 0, 'E': 1},
                                      'ETHUSDT': {'B': 1}}, self.config, self.settings)
        held = {(p['symbol'], p['strategy_name']): p for p in result['open_positions']}
        self.assertEqual(set(held), {('FILUSDT', 'A'), ('FILUSDT', 'B'), ('ETHUSDT', 'B')})
        self.assertEqual({p['entry_notional'] for p in held.values()}, {500})
        self.assertEqual(held['FILUSDT', 'A']['qty'], 5)
        self.assertEqual(held['FILUSDT', 'B']['qty'], 5)
        self.assertEqual(held['ETHUSDT', 'B']['qty'], 2.5)
        self.assertEqual(result['summaries']['FILUSDT']['position_qty'], 0)
        self.assertEqual(result['summaries']['FILUSDT']['position_notional'], 1000)

    def test_later_factor_entry_does_not_share_budget_or_resize_existing_qty(self):
        rows = [self.row(0, {'A': .99, 'B': .5}, 100, 100),
                self.row(1, {'A': .99, 'B': .99}, 200, 200),
                self.row(2, {'A': .2, 'B': .5}, 300, 300)]
        result = self.run_rows(rows, {'A': 1, 'B': 1})
        self.assertEqual(result['trades'][0]['qty'], 10)
        self.assertEqual(result['trades'][0]['entry_notional'], 1000)
        self.assertEqual(result['open_positions'][0]['strategy_name'], 'B')
        self.assertEqual(result['open_positions'][0]['qty'], 5)
        self.assertEqual(result['open_positions'][0]['entry_notional'], 1000)
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

    def test_percentile_usage_changes_decisions_but_not_sign_nq_or_saved_percentile(self):
        self.config['entry']['factor_long_quantile'] = .7
        row = self.row(0, {'A': .8})
        old = factor_snapshot('A', 1, row, self.config)
        self.assertEqual(old['decision'], 'long')
        self.config['position_algorithm'] = 'v1'
        new = factor_snapshot('A', 1, row, self.config)
        self.assertAlmostEqual(new['decision_value'], .6)
        self.assertEqual(new['quantile'], .8)
        self.assertEqual(new['decision'], 'flat')
        self.assertEqual(new['nq_long_quantile'], old['nq_long_quantile'])
        negative = factor_snapshot('A', -1, self.row(0, {'A': .1}), self.config)
        self.assertAlmostEqual(negative['decision_value'], .8)
        self.assertEqual(negative['decision'], 'long')

    def test_v1_exit_uses_transformed_value_and_original_quantity(self):
        rows = [self.row(0, {'A': .99}), self.row(1, {'A': .6}, 120, 120)]
        self.assertEqual(self.run_rows(rows, {'A': 1})['closed_trades'], 0)
        self.config['position_algorithm'] = 'v1'
        result = self.run_rows(rows, {'A': 1})
        self.assertEqual(result['closed_trades'], 1)
        self.assertEqual(result['trades'][0]['qty'], 10)
        self.assertEqual(result['trades'][0]['pnl'], 200)
        self.assertEqual(result['records'][1]['snapshot']['position_algorithm'], 'v1')
        self.assertAlmostEqual(result['records'][1]['snapshot']['signal']['decision_value'], .2)

    def test_symbol_overrides_work_for_both_algorithms_and_are_frozen_in_snapshots(self):
        self.config['symbol_overrides'] = {'FILUSDT': {
            'factor_long_quantile': .7, 'factor_short_quantile': .0,
            'factor_exit_quantile_long': .6, 'factor_exit_quantile_short': .2,
        }}
        for algorithm, qs in [('v0', [.8, .5]), ('v1', [.9, .75])]:
            self.config['position_algorithm'] = algorithm
            fil = [self.row(i, {'A': q}) for i, q in enumerate(qs)]
            eth = [{**row, 'symbol': 'ETHUSDT'} for row in fil]
            result = simulate(fil + eth, {'FILUSDT': {'A': 1}, 'ETHUSDT': {'A': 1}}, self.config, self.settings)
            self.assertEqual(result['summaries']['FILUSDT']['closed_trades'], 1)
            self.assertEqual(result['summaries']['ETHUSDT']['entry_actions'], 0)
            records = [r for r in result['records'] if r['symbol'] == 'FILUSDT']
            self.assertEqual(records[0]['snapshot']['entry']['factor_long_quantile'], .7)
            self.assertEqual(records[1]['snapshot']['execution']['factor_exit_quantile_long'], .6)
            self.assertEqual(records[0]['snapshot']['entry']['nq_long_quantile'], .5)
            self.assertEqual(records[0]['snapshot']['max_pos_u'], 1000)

    def test_v1_negative_thresholds_and_exact_ties(self):
        self.config['position_algorithm'] = 'v1'
        self.config['entry'].update(factor_long_quantile=.6, factor_short_quantile=-.6)
        self.config['execution'].update(factor_exit_quantile_long=.2, factor_exit_quantile_short=-.2)
        for qs in [[.8,.81,.6,.59], [.2,.19,.4,.41]]:
            result = self.run_rows([self.row(i, {'A': q, 'B': q}) for i,q in enumerate(qs)], {'A': 1, 'B': 1})
            self.assertEqual(result['closed_trades'], 1)
            self.assertEqual(result['trades'][0]['entry_ts_ms'], START + 60001)
            self.assertEqual(result['trades'][0]['exit_ts_ms'], START + 180001)

    def test_v1_mean_is_equal_weight_before_threshold_and_one_unit_per_entry(self):
        self.config['position_algorithm']='v1'
        self.config['entry'].update(factor_long_quantile=.8, factor_short_quantile=-.8)
        rows=[self.row(0, {'A':.99,'B':.79}), self.row(1, {'A':.96,'B':.95}),
              self.row(2, {'A':.99,'B':.99},200,200), self.row(3, {'A':.51,'B':.51},300,300)]
        result=self.run_rows(rows,{'A':20,'B':1})
        self.assertEqual(result['allocation'],'per_symbol')
        self.assertEqual(len(result['records']),4)
        self.assertEqual(result['records'][0]['simulation']['actions'],[])
        self.assertAlmostEqual(result['records'][0]['snapshot']['signal']['decision_value'],.78)
        self.assertEqual(set(result['summaries']['FILUSDT']['strategies']),{'symbol_mean'})
        trade=result['trades'][0]
        self.assertEqual(trade['qty'],10)
        self.assertEqual(trade['entry_notional'],1000)
        self.assertEqual(trade['strategy_name'],'symbol_mean')
        self.assertEqual(result['records'][2]['snapshot']['position_qty'],10)
        votes=result['records'][0]['snapshot']['signal']['factor_votes']
        self.assertEqual([v['normalized_weight'] for v in votes],[.5,.5])

    def test_v1_waits_for_all_factors_and_disabled_factors_are_excluded(self):
        self.config['position_algorithm']='v1'
        rows=[self.row(0,{'A':.99}), self.row(1,{'A':.99,'B':.99}),self.row(2,{'A':.01}),self.row(3,{'A':.6,'B':.6})]
        result=self.run_rows(rows,{'A':1,'B':1,'C':0})
        self.assertEqual(result['summaries']['FILUSDT']['warmup_minutes'],2)
        self.assertEqual(result['trades'][0]['entry_ts_ms'],START+60001)
        self.assertEqual(result['trades'][0]['exit_ts_ms'],START+180001)
        self.assertEqual(result['trades'][0]['qty'],10)

    def test_v1_opposing_values_cancel_and_sign_nq_apply_to_combined_direction(self):
        self.config['position_algorithm']='v1'
        self.config['entry'].update(factor_long_quantile=.6, factor_short_quantile=-.6)
        result=self.run_rows([self.row(0,{'A':.99,'B':.01})],{'A':1,'B':1})
        self.assertEqual(result['open_positions'],[])
        rows=[self.row(0,{'A':.01,'B':.99}),self.row(1,{'A':.01,'B':.99}),self.row(2,{'A':.6,'B':.4})]
        rows[0]['nq']['long_quantile']=.4
        rows[2]['nq']['ready']=False
        result=self.run_rows(rows,{'A':-20,'B':1})
        self.assertEqual(result['trades'][0]['entry_ts_ms'],START+60001)
        self.assertEqual(result['closed_trades'],1)
if __name__=='__main__':unittest.main()
