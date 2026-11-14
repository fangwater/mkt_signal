#!/usr/bin/env python3
"""Isolated real-Redis audit regression; never contacts a trading Redis."""
import contextlib
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import parameter_audit as audit
import redis


class JournalTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp=tempfile.TemporaryDirectory(prefix='parameter-audit-test-')
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0));cls.port=sock.getsockname()[1]
        cls.process=subprocess.Popen(['redis-server','--bind','127.0.0.1','--port',str(cls.port),'--save','','--appendonly','no','--dir',cls.temp.name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        cls.raw=redis.Redis(host='127.0.0.1',port=cls.port,decode_responses=True)
        for _ in range(100):
            try:
                if cls.raw.ping():break
            except redis.ConnectionError:time.sleep(.02)
        else:raise RuntimeError('isolated Redis did not start')

    @classmethod
    def tearDownClass(cls):
        cls.process.terminate();cls.process.wait(timeout=5);cls.temp.cleanup()

    def setUp(self):
        self.raw.flushdb()
        self.root=Path(self.temp.name)/str(time.time_ns())
        self.old=os.environ.get('PARAMETER_AUDIT_DIR')
        os.environ['PARAMETER_AUDIT_DIR']=str(self.root)
        audit._LOCAL.request={'family':'funding_rate','environment':'gate_fr_arb01','source':'config_page','actor':'test-user','source_ip':'192.0.2.1','session_id':'hashed','request_path':'/api/strategy-params'}
        self.client=audit.wrap_redis(self.raw)
        self.key='fr_strategy_params_gate-margin_gate-futures'

    def tearDown(self):
        audit._LOCAL.request=None
        if self.old is None:os.environ.pop('PARAMETER_AUDIT_DIR',None)
        else:os.environ['PARAMETER_AUDIT_DIR']=self.old

    def events(self):return [r['event'] for r in audit.read_batch(self.root,0)['records']]

    def test_rapid_roundtrip_noop_pipeline_and_redaction(self):
        self.raw.hset(self.key,mapping={'threshold':'0.7','api_key':'hidden-value'})
        self.client.hset(self.key,'threshold','0.3')
        self.client.hset(self.key,'threshold','0.7')
        self.client.hset(self.key,'threshold','0.7')
        with self.client.pipeline() as pipe:
            pipe.hset(self.key,'threshold','0.4').hdel(self.key,'api_key').execute()
        events=self.events()
        self.assertEqual(len(events),4)
        self.assertEqual(events[0]['objects'][self.key]['before']['value']['threshold'],'0.7')
        self.assertEqual(events[1]['objects'][self.key]['after']['value']['threshold'],'0.7')
        self.assertEqual(events[2]['objects'],{})
        self.assertNotIn('hidden-value',json.dumps(events))
        self.assertEqual(events[0]['actor'],'test-user')
        self.assertFalse(list(self.root.glob('*.pending')))
        self.assertEqual(self.root.stat().st_mode & 0o777,0o700)
        self.assertTrue(all(p.stat().st_mode & 0o777 == 0o600 for p in self.root.iterdir()))

    def test_set_delete_mset_and_scope_rejection(self):
        key='gate_fr_arb01:gate-margin:gate-futures:amount_u_overrides'
        self.client.set(key,'{"ETHUSDT":10}')
        self.client.mset({key:'{"ETHUSDT":20}'})
        self.client.delete(key)
        self.assertEqual(len(self.events()),3)
        self.assertEqual(self.events()[-1]['objects'][key]['after'],{'type':'none','value':None})
        with self.assertRaises(ValueError):self.client.set('some-api-secret','bad')
        self.assertIsNone(self.raw.get('some-api-secret'))

    def test_failed_transaction_and_pending_recovery_never_restore(self):
        self.raw.set(self.key,'wrongtype')
        with self.assertRaises(redis.ResponseError):
            with self.client.pipeline() as pipe:
                pipe.hset(self.key,'threshold','0.2').execute()
        self.assertEqual(self.events()[0]['source'],'failed_config_request')
        self.assertEqual(self.events()[0]['objects'],{})
        original=audit.snapshot
        calls=0
        def failing(client,keys):
            nonlocal calls
            calls+=1
            if calls>1:raise OSError('simulated audit read failure')
            return original(client,keys)
        audit.snapshot=failing
        try:
            with self.assertRaises(RuntimeError):self.client.set(self.key,'changed')
        finally:audit.snapshot=original
        self.assertEqual(self.raw.get(self.key),'changed')
        self.assertEqual(len(list(self.root.glob('*.pending'))),1)
        self.assertTrue(audit.read_batch(self.root,1)['records'][0]['pending'])

    def test_mm_all_keys_and_fr_allowlist(self):
        for key in ['okex_mm_83777:okex-futures:mm:amount_u','mm_strategy_params_okex-futures','mm_trade_symbols:okex-futures','model_score_rolling_params_test-model','okex_mm_83777:okex-futures:okex-futures:pre_trade_risk_params']:
            self.assertTrue(audit.allowed_key('market_making','okex_mm_83777',key),key)
        audit._LOCAL.request.update(family='market_making',environment='okex_mm_83777')
        self.client.set('mm_trade_symbols:okex-futures','["ETHUSDT"]')
        self.client.hset('mm_strategy_params_okex-futures',mapping={'amount_u':'100'})
        self.assertEqual(len(self.events()),2)
        for key in ['gate_fr_arb01:fr_unimmr_close_symbols:gate-margin_gate-futures','gate_fr_arb01:funding_rate_thresholds_gate-margin_gate-futures','rolling_metrics_params_gate-margin_gate-futures']:
            self.assertTrue(audit.allowed_key('funding_rate','gate_fr_arb01',key),key)

    def test_metadata_ignores_remote_actor_and_never_keeps_cookie(self):
        class Handler:
            path='/api/strategy-params?access_token=never-record'
            client_address=('192.0.2.8',1)
            headers={'Cookie':'delist_session=private-cookie','X-Parameter-Actor':'forged','X-Real-IP':'10.0.0.1'}
        @audit.audit_post('funding_rate',lambda h:'gate_fr_arb01')
        def request(handler):self.client.hset(self.key,'threshold','0.3')
        request(Handler())
        record=self.events()[0]
        self.assertIsNone(record['actor'])
        self.assertEqual(record['source_ip'],'192.0.2.8')
        self.assertNotIn('private-cookie',json.dumps(record))
        self.assertNotIn('access_token',json.dumps(record))


if __name__=='__main__':unittest.main()
