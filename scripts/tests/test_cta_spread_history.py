"""Spread observation replay must remain causal, independent and read-only."""
import io
import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from cta_spread_signal_history import SpreadHistory, SpotMinuteClient, replay, MINUTE, DAY
from cta_spread_dashboard import make_handler, live_ready

CONFIG = json.loads((ROOT / 'config/cta_spread.json').read_text())
CONFIG['entry'].update(spread_long_quantile=.3, spread_short_quantile=.7)
START = 1_790_000_040_000


def row(i, q, spread=.2, ready=True, spread_ready=True, factor='factor_170'):
    return dict(symbol='ETHUSDT', model_ts_ms=START + i * MINUTE,
                inputs={factor: dict(score=1., quantile=q, ready=ready)},
                spread=dict(value=.001, quantile=spread, ready=spread_ready),
                nq=dict(long_quantile=.5,short_quantile=.5,ready=True))


class ReplayTests(unittest.TestCase):
    def test_live_readiness_needs_fresh_model_and_both_inputs(self):
        sample=dict(score_ready=True,model_ts_ms=START,signal=dict(filter_ready=True))
        self.assertTrue(live_ready(sample,START+MINUTE,True))
        self.assertFalse(live_ready(sample,START+4*MINUTE,True))
        self.assertFalse(live_ready(sample,START-MINUTE,True))
        self.assertFalse(live_ready(sample,START,False))
        sample['signal']['filter_ready']=False
        self.assertFalse(live_ready(sample,START,True))

    def test_both_filters_are_required_but_do_not_block_exit(self):
        records = [row(0,.99),row(1,.01,.8)]
        records[0]['nq']['long_quantile'] = .49
        self.assertEqual(replay(records[:1],'factor_170',1,CONFIG)[0][0]['direction'],0)
        records[0]['nq']['long_quantile'] = .5
        records[1]['nq']['ready'] = False
        points, events = replay(records,'factor_170',1,CONFIG)
        self.assertEqual([p['direction'] for p in points], [1,0])
        self.assertEqual(events[-1]['reason'], 'blocked_reversal_exit')
        self.assertEqual(replay([row(0,.99)],'factor_170_positive',1,CONFIG)[0][0]['direction'],1)

    def test_entry_hold_exit_and_reversal(self):
        records = [row(0,.99),row(1,.99),row(2,.2,.5),row(3,.01,.8),row(4,.99,.2)]
        points, events = replay(records,'factor_170',1,CONFIG)
        self.assertEqual([p['direction'] for p in points],[1,1,0,-1,1])
        self.assertEqual([(e['ts'],e['market'],e['side']) for e in events], [
            (START,'cash','B'),(START+2*MINUTE,'perpetual','S'),
            (START+3*MINUTE,'cash','S'),(START+4*MINUTE,'perpetual','B'),
            (START+4*MINUTE,'cash','B')])
        self.assertFalse(CONFIG['enabled'])  # Observation replay runs with publication off.

    def test_blocked_reversal_exits_and_spread_never_exits(self):
        points, events = replay([row(0,.99),row(1,.8,.99),row(2,.01,.5)],'factor_170',1,CONFIG)
        self.assertEqual([p['direction'] for p in points],[1,1,0])
        self.assertEqual(len(events),2)
        self.assertEqual(events[-1]['reason'],'blocked_reversal_exit')
        points, events = replay([row(0,.99),row(1,.2,None,spread_ready=False)],'factor_170',1,CONFIG)
        self.assertEqual(points[-1]['direction'],0)
        self.assertEqual(events[-1]['market'],'perpetual')

    def test_boundaries_sign_and_missing_factor(self):
        points,events=replay([row(0,.9,.8),row(1,.1,.2),row(2,.01,.3),row(3,.01,.2),
                              row(4,.99,.8,ready=False),row(5,.99,.8)],'factor_170',-1,CONFIG)
        self.assertEqual([p['direction'] for p in points],[0,0,0,1,1,-1])
        self.assertEqual([e['side'] for e in events],['B','S','S'])
        self.assertAlmostEqual(points[3]['quantile'],.99)
        self.assertEqual(replay([row(0,.99)],'baseline_141',1,CONFIG)[1],[])

    def test_prefix_causality_and_no_cross_factor_contamination(self):
        records=[row(0,.99),row(1,.2),row(2,.01,.8)]
        before=replay(records[:2],'factor_170',1,CONFIG)
        after=replay(records,'factor_170',1,CONFIG)
        self.assertEqual(before[0],after[0][:2]);self.assertEqual(before[1],after[1][:2])
        self.assertEqual(replay(records,'baseline_141',1,CONFIG)[1],[])


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=Path(self.temp.name)
        self.store=SpreadHistory(self.path/'history.sqlite3')
        self.journal=self.path/'20261013.jsonl'

    def tearDown(self):
        self.store.close();self.temp.cleanup()

    def test_incremental_tail_rotation_and_idempotency(self):
        payload=json.dumps(row(0,.99))
        self.journal.write_text(payload+'\n'+json.dumps(row(1,.2))[:20])
        self.assertEqual(self.store.import_history(self.path),1)
        self.assertEqual(self.store.import_history(self.path),0)
        self.journal.write_text(payload+'\n'+json.dumps(row(1,.2))+'\n')
        self.assertEqual(self.store.import_history(self.path),1)
        self.journal.unlink();self.journal.write_text(payload+'\n'+json.dumps(row(1,.01,.8))+'\n')
        self.store.import_history(self.path)
        self.assertEqual(self.store.db.execute('SELECT count(*) FROM observations').fetchone()[0],2)
        self.assertEqual(self.store.latest()['ETHUSDT']['inputs']['factor_170']['quantile'],.01)

    def test_state_seeded_before_display_window_and_no_fake_prices(self):
        self.journal.write_text('\n'.join(json.dumps(r) for r in [row(0,.99),row(62,.8)])+'\n')
        self.store.import_history(self.path)
        with patch('cta_spread_signal_history.now_ms',return_value=START+62*MINUTE):
            series=self.store.series('ETHUSDT','factor_170',1,CONFIG,1)
        self.assertEqual(series['events'],[])
        self.assertEqual(series['points'][0]['direction'],1)
        self.assertEqual(series['klines'],[])
        self.assertEqual(series['missing_candles'],61)
        self.assertEqual(series['replay_start_ms'],START)
        self.assertEqual(series['end_ms'],START+62*MINUTE)

    def test_end_anchored_to_available_history_during_kafka_catchup(self):
        self.journal.write_text(json.dumps(row(0,.99))+'\n');self.store.import_history(self.path)
        with patch('cta_spread_signal_history.now_ms',return_value=START+DAY):
            series=self.store.series('ETHUSDT','factor_170',1,CONFIG)
        self.assertEqual(series['end_ms'],START)
        self.assertEqual(series['events'][0]['ts'],START)

    def test_http_read_only_binding_and_asset_routes(self):
        config,status,factors=(self.path/n for n in ['config.json','status.json','factors.json'])
        config.write_text(json.dumps(CONFIG));status.write_text(json.dumps(dict(symbol_factors={'ETHUSDT':{'factor_170':1}})))
        factors.write_text('{}')
        self.journal.write_text(json.dumps(row(0,.99))+'\n');self.store.import_history(self.path)
        before={p:p.read_bytes() for p in [config,status,factors,self.journal]}
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(config,status,ROOT/'web/cta_spread/index.html',self.store,factors))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        base=f'http://127.0.0.1:{server.server_port}'
        try:
            for route in ['/signals/','/signals/panel.js','/assets/echarts.min.js','/api/signal-panel',
                          '/api/signal-history?symbol=ETHUSDT&factor=factor_170&hours=1']:
                with urllib.request.urlopen(base+route) as response:self.assertEqual(response.status,200)
            for query in ['symbol=BTCUSDT&factor=factor_170','symbol=ETHUSDT&factor=baseline_141',
                          'symbol=ETHUSDT&factor=factor_170&hours=48']:
                with self.assertRaises(urllib.error.HTTPError) as error:urllib.request.urlopen(base+'/api/signal-history?'+query)
                self.assertEqual(error.exception.code,400);error.exception.close()
            with self.assertRaises(urllib.error.HTTPError) as error:urllib.request.urlopen(urllib.request.Request(base+'/api/signal-panel',data=b'{}'))
            self.assertEqual(error.exception.code,501);error.exception.close()
            for path,contents in before.items():self.assertEqual(path.read_bytes(),contents)
        finally:server.shutdown();server.server_close()


class CandleTests(unittest.TestCase):
    def test_public_contract_and_invalid_ohlc(self):
        requests=[]
        data=[[START,'100','102','99','101','3',START+MINUTE-1]]
        def opener(request,timeout):
            requests.append(request)
            return io.BytesIO(json.dumps(data).encode())
        client=SpotMinuteClient(opener)
        rows=client.fetch('ETHUSDT',START,START)
        self.assertEqual(rows[0]['open'],100)
        self.assertIn('/api/v3/klines?',requests[0].full_url)
        self.assertEqual(requests[0].get_method(),'GET')
        self.assertNotIn('X-mbx-apikey',requests[0].headers)
        data[0][3]='110'
        with self.assertRaises(ValueError):client.fetch('ETHUSDT',START,START)

    def test_rate_limit_backoff_applies_across_symbols(self):
        calls=[]
        def opener(request,timeout):
            calls.append(request)
            raise urllib.error.HTTPError(request.full_url,429,'limited',{'Retry-After':'120'},None)
        client=SpotMinuteClient(opener)
        with self.assertRaises(RuntimeError):client.fetch('ETHUSDT',START,START)
        with self.assertRaises(RuntimeError):client.fetch('BTCUSDT',START,START)
        self.assertEqual(len(calls),1)


if __name__=='__main__':unittest.main()
