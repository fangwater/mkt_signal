"""RX03 research rows remain isolated by symbol/factor through HTTP and replay."""
import json
import pathlib
import sys
import threading
import tempfile
import unittest
from http.server import ThreadingHTTPServer

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from cta_perp_config_server import normalize_config, normalize_factor_rules, effective_config, position_strategy_factors, rule_strategy
from cta_perp_factor_config import available_universe, validate_available, normalize_table
from cta_perp_position_replay import factor_snapshot, simulate
from cta_perp_dashboard import factor_strategies
from cta_perp_signal_history import configured_targets
import test_cta_perp_servers as fixtures
from test_cta_perp_servers import FakeFactorRedis, request_json, CONFIG_SERVER


class FactorNqRulesTests(unittest.TestCase):
    def test_same_factor_different_nq_keeps_independent_replay_positions(self):
        config = self.config()
        config['bar_interval'] = '15m'
        config['factor_rules'] = [
            {'标的':'SKL','因子':'factor_170','符号':-1,'nq':'关闭'},
            {'标的':'SKL','因子':'factor_170','符号':-1,'nq':'0.50'},
        ]
        config = normalize_config(config)
        rules, table = normalize_factor_rules(config['factor_rules'])
        names = [rule_strategy(config, r) for r in rules]
        self.assertEqual(names, ['factor_170__nq_off__neg',
            'factor_170__nq_3fe0000000000000_3fe0000000000000__neg'])
        self.assertEqual(table, {'SKLUSDT': {'factor_170': -1}})
        self.assertEqual(len(position_strategy_factors(table['SKLUSDT'], 'v0', config, 'SKLUSDT')), 2)
        self.assertFalse(effective_config(config,'SKLUSDT',names[0])['entry']['nq_change_enabled'])
        self.assertTrue(effective_config(config,'SKLUSDT',names[1])['entry']['nq_change_enabled'])
        def row(ts, nq, ask):
            return {'bar_interval':'15m','symbol':'SKLUSDT','model_ts_ms':ts,
                'inputs':{'factor_170':{'score':-1,'quantile':.01,'ready':True}},
                'nq':{'ready':True,'long_quantile':nq,'short_quantile':nq,
                    'execution_ts_ms':ts+5,'execution_bid':ask-1,'execution_ask':ask}}
        result=simulate([row(900000,.4,101),row(1800000,.6,201)],table,config,
            {'start_ms':0,'initial_equity_usdt':10000})
        opened={p['strategy_name']:p for p in result['open_positions']}
        self.assertEqual(set(opened),set(names))
        self.assertAlmostEqual(opened[names[0]]['qty'],config['max_pos_u']/101)
        self.assertAlmostEqual(opened[names[1]]['qty'],config['max_pos_u']/201)
        reordered={**config,'factor_rules':list(reversed(config['factor_rules']))}
        self.assertEqual(rule_strategy(reordered,rules[0]),names[0])
        duplicate={**rules[1],'nq':'0.5/0.5'}
        with self.assertRaises(ValueError): normalize_factor_rules([*rules,duplicate])

    def test_factor_service_capacity(self):
        import re
        text = (ROOT/'scripts/factor_validation_allowlist.txt').read_text()
        names = sorted(set(re.findall(r'(?:factor_(?:trades_)?\d{3}|baseline_\d{3}|T[DP]_[A-Z]+_\d{3})', text)))
        self.assertGreater(len(names), 128)
        def table(count):
            return {f'COIN{i//32}USDT': {name: 1 for name in names[i:i+32]}
                    for i in range(0, count, 32)}
        self.assertEqual(sum(map(len, normalize_table(table(128)).values())), 128)
        with self.assertRaises(ValueError): normalize_table(table(129))

    def config(self):
        return normalize_config(json.loads((ROOT/'config/cta_perp_rx03/cta_perp.json').read_text()))

    def test_exact_operator_list_and_available_universe(self):
        raw = json.loads((ROOT/'config/cta_perp_rx03/factor_rules.json').read_text())
        rules, table = normalize_factor_rules(raw)
        self.assertEqual(len(rules), 45)
        self.assertEqual(len(table), 16)
        self.assertEqual(rules, self.config()['factor_rules'])
        validate_available(table, available_universe(ROOT/'config/cta_perp_rx03/cta_perp_factor_model_1m_pub.toml'))
        self.assertEqual(effective_config(self.config(), 'NEARUSDT', 'baseline_141')['entry']['nq_long_quantile'], .2)
        self.assertFalse(effective_config(self.config(), 'NEARUSDT', 'factor_051')['entry']['nq_change_enabled'])
        self.assertTrue(effective_config(self.config(), 'DOGEUSDT', 'baseline_164')['entry']['nq_change_enabled'])

    def test_paused_dashboard_uses_rules_instead_of_stale_signal_status(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            config, status, factors = root/'config.json', root/'status.json', root/'factors.json'
            config.write_text(json.dumps(self.config()))
            status.write_text(json.dumps({'symbol_factors':{'BTCUSDT':{'baseline_085':1}}}))
            expected = normalize_factor_rules(self.config()['factor_rules'])[1]
            self.assertEqual(factor_strategies(status, config), {s:sorted(row) for s,row in expected.items()})
            self.assertEqual(configured_targets(status, factors, config), sorted(expected))
            status.unlink()
            self.assertEqual(configured_targets(status, factors, config), sorted(expected))

    def test_invalid_rows_and_unsupported_modes(self):
        base = {'标的':'ZEC', '因子':'baseline_164', '符号':1, 'nq':'0.2/0.8'}
        for value in ('-0.1', '1.1', 'NaN', '0.2/0.8/0.9', '', True):
            with self.subTest(nq=value), self.assertRaises(ValueError):
                normalize_factor_rules([{**base, 'nq': value}])
        for row in ({**base, '符号':True}, {**base, '符号':0}, {**base, '因子':'unknown'}, {**base,'extra':1}):
            with self.assertRaises(ValueError): normalize_factor_rules([row])
        with self.assertRaises(ValueError): normalize_factor_rules([base, {**base, '标的':'ZECUSDT'}])
        for field, value in [('bar_interval','5m'), ('position_algorithm','v1')]:
            config = self.config(); config[field] = value
            with self.assertRaises(ValueError): normalize_config(config)

    def test_final_direction_strict_boundaries_and_disabled_nq(self):
        config = self.config()
        def signal(factor, weight, q, nq, ready=True):
            obs = {'symbol':'NEARUSDT','model_ts_ms':60000,'inputs':{factor:{'score':1,'quantile':q,'ready':True}},
                   'nq':{'ready':ready,'long_quantile':nq,'short_quantile':nq}}
            return factor_snapshot(factor, weight, obs, config)['decision']
        self.assertEqual(signal('baseline_141',1,.99,.2), 'flat')
        self.assertEqual(signal('baseline_141',1,.99,.21), 'long')
        self.assertEqual(signal('TD_TI_024',-1,.01,.21), 'long')
        self.assertEqual(signal('TD_TI_024',-1,.99,.8), 'flat')
        self.assertEqual(signal('TD_TI_024',-1,.99,.79), 'short')
        self.assertEqual(signal('factor_051',-1,.01,None,False), 'long')


class FactorNqHttpTests(unittest.TestCase):
    setUp = fixtures.CtaPerpServerTests.setUp
    def test_array_save_roundtrip_conflict_and_no_redis_writes(self):
        redis = FakeFactorRedis()
        server = ThreadingHTTPServer(('127.0.0.1',0), CONFIG_SERVER.make_handler(CONFIG_SERVER.ConfigStore(self.config_path),ROOT/'web/cta_perp_config/index.html', redis,'binance-cta-perp-test'))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        self.addCleanup(server.server_close); self.addCleanup(server.shutdown)
        base=f'http://127.0.0.1:{server.server_port}'
        _, body=request_json(base+'/api/symbol-factors')
        row={'标的':'NEAR','nq':'关闭','符号':-1,'因子':'factor_036'}
        request={'symbol_factors':[row],'expected_config_revision':body['config_revision']}
        _, saved=request_json(base+'/api/symbol-factors', method='POST',body=request)
        self.assertEqual(saved['factor_rules'][0]['标的'],'NEARUSDT')
        self.assertEqual(saved['symbol_factors'], {'NEARUSDT':{'factor_036':-1}})
        self.assertEqual(redis.values,{})
        from urllib.error import HTTPError
        with self.assertRaises(HTTPError) as stale:
            request_json(base+'/api/symbol-factors',method='POST',body=request)
        self.assertEqual(stale.exception.code,409)
        stale.exception.close()
        before = self.config_path.read_bytes()
        for bad in ({**row, 'nq':'1.5'}, {**row, '标的':'UNKNOWN'}):
            with self.assertRaises(HTTPError) as invalid:
                request_json(base+'/api/symbol-factors', method='POST', body={
                    'symbol_factors':[bad], 'expected_config_revision':saved['config_revision']})
            self.assertEqual(invalid.exception.code,400)
            invalid.exception.close()
            self.assertEqual(self.config_path.read_bytes(),before)
        _, loaded=request_json(base+'/api/symbol-factors')
        self.assertEqual(loaded['factor_rules'],saved['factor_rules'])
