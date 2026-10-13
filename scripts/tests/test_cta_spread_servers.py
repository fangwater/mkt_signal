"""Independent spread configuration, revision and read-only dashboard contracts."""
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from cta_spread_config_server import ConfigStore, ConfigConflict, normalize_config
from cta_spread_factor_config import factor_key, normalize_table, save_factor_table, read_factor_table
from cta_spread_dashboard import build_snapshot
from scripts.tests.test_cta_perp_servers import FakeFactorRedis


class SpreadConfigTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / 'config/cta_spread.json').read_text())

    def test_disabled_defaults_and_spread_bounds(self):
        config = normalize_config(self.config)
        self.assertFalse(config['enabled'])
        self.assertEqual(config['entry']['spread_lower_quantile'], .3)
        self.assertEqual(config['entry']['spread_upper_quantile'], .7)
        for low, high in [(.7,.3),(.3,.3),(-.1,.7),(.3,1.1),(float('nan'),.7)]:
            invalid = copy.deepcopy(config)
            invalid['entry'].update(spread_lower_quantile=low, spread_upper_quantile=high)
            with self.assertRaises(ValueError): normalize_config(invalid)
        invalid = copy.deepcopy(config)
        invalid['entry']['nq_change_enabled'] = True
        with self.assertRaises(ValueError): normalize_config(invalid)

    def test_config_revision_rejects_lost_updates(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'cta_spread.json'
            path.write_text(json.dumps(self.config))
            store = ConfigStore(path)
            config, revision = store.load()
            config['entry']['spread_lower_quantile'] = .2
            store.save(config, revision)
            with self.assertRaises(ConfigConflict): store.save(self.config, revision)
            self.assertEqual(store.load()[0]['entry']['spread_lower_quantile'], .2)

    def test_family_and_market_isolation_and_factor_revision(self):
        with self.assertRaises(ValueError): factor_key('binance-cta-perp-test')
        with self.assertRaises(ValueError): normalize_table({'ETHUSDC': {'factor_036': 1}})
        redis = FakeFactorRedis()
        redis.values['binance-cta-perp-test:cta_symbol_factors'] = '{}'
        current = read_factor_table(redis, 'binance-cta-spread-test')
        saved = save_factor_table(redis, 'binance-cta-spread-test', {'ETHUSDT': {'factor_036': -1}}, current['revision'], {'symbols':['ETHUSDT'], 'factors':['factor_036']})
        self.assertEqual(saved['symbol_factors']['ETHUSDT']['factor_036'], -1)
        self.assertEqual(redis.values['binance-cta-perp-test:cta_symbol_factors'], '{}')
        with self.assertRaises(ValueError): save_factor_table(redis, 'binance-cta-spread-test', {}, current['revision'], {'symbols':[], 'factors':[]})

    def test_hedged_inventory_survives_dashboard_with_publication_disabled(self):
        with tempfile.TemporaryDirectory() as root:
            config, status = Path(root)/'config.json', Path(root)/'status.json'
            config.write_text(json.dumps(self.config))
            state={'updated_ts_us':1,'strategies':{'factor_036':{'ETHUSDT':{'position_flag':0,'cash_qty':2,'perpetual_qty':-2}}}}
            status.write_text(json.dumps(state))
            before = status.read_bytes()
            snapshot = build_snapshot(config,status)
            self.assertFalse(snapshot['config']['enabled'])
            self.assertEqual(snapshot['status']['strategies']['factor_036']['ETHUSDT']['cash_qty'],2)
            self.assertEqual(status.read_bytes(),before)
            self.assertEqual(snapshot['signal_state'],'stale')


class SignalLifecycleTests(unittest.TestCase):
    def test_stop_suite_never_touches_exec_or_account_processes(self):
        import subprocess
        for family in ['perp', 'spread']:
            with self.subTest(family=family), tempfile.TemporaryDirectory() as directory:
                env = Path(directory)/f'binance-cta-{family}-test'
                scripts = env/'scripts'; scripts.mkdir(parents=True)
                wrapper = scripts/f'stop_cta_{family}.sh'
                wrapper.write_bytes((ROOT/'scripts'/wrapper.name).read_bytes())
                calls = env/'calls'
                for role in ['signal','factor_model_1m_pub','dashboard','config_server']:
                    child=scripts/f'stop_cta_{family}_{role}.sh'
                    child.write_text(f'#!/bin/sh\nprintf "%s\\n" "{role}" >> "{calls}"\n')
                    child.chmod(0o755)
                dry = subprocess.run(['bash',str(wrapper)],capture_output=True,text=True,check=True)
                self.assertIn('DRY-RUN',dry.stdout)
                self.assertFalse(calls.exists())
                subprocess.run(['bash',str(wrapper),'--execute'],capture_output=True,text=True,check=True)
                self.assertEqual(calls.read_text().splitlines(),['signal','factor_model_1m_pub','dashboard','config_server'])


if __name__ == '__main__': unittest.main()
