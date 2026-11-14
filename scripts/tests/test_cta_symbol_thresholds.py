"""Per-symbol CTA thresholds across config storage, signals and replay."""
import copy
import importlib
import json
from pathlib import Path
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))


class SymbolThresholdTests(unittest.TestCase):
    def families(self):
        for family in ('perp', 'spread'):
            server = importlib.import_module(f'cta_{family}_config_server')
            config = json.loads((ROOT / f'config/cta_{family}.json').read_text())
            yield family, server, config

    def test_partial_inheritance_storage_and_revision_conflicts(self):
        for family, server, config in self.families():
            with self.subTest(family=family), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'config.json'
                path.write_text(json.dumps(config))
                store = server.ConfigStore(path)
                loaded, revision = store.load()
                loaded['symbol_overrides'] = {'BTCUSDT': {
                    'factor_long_quantile': .95, 'factor_exit_quantile_long': .4,
                    'nq_long_quantile': .65, 'nq_short_quantile': .35,
                    'factor_short_quantile': None,
                }}
                saved, next_revision = store.save(loaded, revision)
                self.assertNotEqual(next_revision, revision)
                self.assertEqual(store.load(), (saved, next_revision))
                btc = server.effective_config(saved, 'BTCUSDT')
                eth = server.effective_config(saved, 'ETHUSDT')
                self.assertEqual(btc['entry']['nq_long_quantile'], .65)
                self.assertEqual(btc['entry']['nq_short_quantile'], .35)
                self.assertEqual(btc['entry']['factor_short_quantile'], .1)
                self.assertEqual(btc['execution']['factor_exit_quantile_long'], .4)
                self.assertEqual(eth['entry'], saved['entry'])
                self.assertNotIn('factor_short_quantile', saved['symbol_overrides']['BTCUSDT'])
                with self.assertRaises(server.ConfigConflict):
                    store.save(config, revision)
                del saved['symbol_overrides']['BTCUSDT']
                saved, _ = store.save(saved, next_revision)
                self.assertEqual(server.effective_config(saved, 'BTCUSDT')['entry'], saved['entry'])

    def test_invalid_symbol_overrides_do_not_write(self):
        for family, server, config in self.families():
            with self.subTest(family=family), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'config.json'
                path.write_text(json.dumps(config))
                store = server.ConfigStore(path)
                original, revision = store.load()
                original_bytes = path.read_bytes()
                invalid = [{'btcusdt': {'nq_long_quantile': .6}}]
                invalid += [{'BTCUSDT': {field: value}}
                            for field in ('nq_long_quantile', 'nq_short_quantile')
                            for value in (-.01, 1.01, True, '0.5', float('nan'), float('inf'))]
                invalid += [{'BTCUSDT': {'factor_long_quantile': .2}},
                            {'BTCUSDT': {'factor_exit_quantile_short': .01}},
                            {'BTCUSDT': {'max_pos_u': 100}},
                            {'BTCUSDT': []}]
                for overrides in invalid:
                    with self.subTest(overrides=overrides):
                        raw = {**original, 'symbol_overrides': overrides}
                        with self.assertRaises(ValueError):
                            store.save(raw, revision)
                        self.assertEqual(path.read_bytes(), original_bytes)

    def test_five_minute_fixed_nq_contract_also_checks_overrides(self):
        import cta_perp_config_server as server
        config = {'bar_interval': '5m', 'execution': {},
                  'symbol_overrides': {'BTCUSDT': {'nq_long_quantile': .6}}}
        with self.assertRaisesRegex(ValueError, 'BTCUSDT.*fixed'):
            server.normalize_config(config)
        config['symbol_overrides']['BTCUSDT']['nq_long_quantile'] = .5
        server.normalize_config(config)

    def test_spread_deployment_import_accepts_symbol_thresholds(self):
        from cta_spread_deployment import prepare_deployment
        raw = json.loads((ROOT / 'config/cta_spread_deployment.json').read_text())
        current = json.loads((ROOT / 'config/cta_spread.json').read_text())
        raw['symbol_overrides'] = {'AXSUSDT': {'nq_long_quantile': .65}}
        prepared = prepare_deployment(raw, current)
        self.assertEqual(prepared['config']['symbol_overrides'], raw['symbol_overrides'])

    def test_spread_replay_resolves_symbol_gates_and_exits(self):
        from cta_spread_signal_history import replay
        from scripts.tests.test_cta_spread_history import CONFIG, row
        config = copy.deepcopy(CONFIG)
        config['symbol_overrides'] = {'ETHUSDT': {
            'nq_long_quantile': .7, 'nq_short_quantile': .3,
            'factor_exit_quantile_long': .6, 'factor_exit_quantile_short': .4,
        }}
        for entry_q, hold_q, exit_q, spread, side in [(.99, .65, .59, .2, 1), (.01, .35, .41, .8, -1)]:
            records = [row(i, q, spread) for i, q in enumerate([entry_q, entry_q, hold_q, exit_q])]
            records[1]['nq'].update(long_quantile=.7, short_quantile=.3)
            points, _ = replay(records, 'factor_170', 1, config)
            self.assertEqual([p['direction'] for p in points], [0, side, side, 0])
            for record in records:
                record['symbol'] = 'BTCUSDT'
            points, _ = replay(records, 'factor_170', 1, config)
            self.assertEqual([p['direction'] for p in points], [side, side, 0, 0])

    def test_perp_replay_uses_symbol_nq_in_both_algorithms(self):
        from cta_perp_position_replay import simulate
        from scripts.tests.test_cta_perp_position_replay import PositionReplayTests
        fixture = PositionReplayTests()
        fixture.setUp()
        config = fixture.config
        config['symbol_overrides'] = {'FILUSDT': {'nq_long_quantile': .95, 'nq_short_quantile': .05}}
        for algorithm in ('v0', 'v1'):
            config['position_algorithm'] = algorithm
            for q in (.99, .01):
                fil = fixture.row(0, {'A': q})
                eth = {**fil, 'symbol': 'ETHUSDT'}
                result = simulate([fil, eth], {'FILUSDT': {'A': 1}, 'ETHUSDT': {'A': 1}}, config, fixture.settings)
                self.assertEqual(result['summaries']['FILUSDT']['entry_actions'], 0)
                self.assertEqual(result['summaries']['ETHUSDT']['entry_actions'], 1)
                record = next(r for r in result['records'] if r['symbol'] == 'FILUSDT')
                self.assertEqual(record['snapshot']['entry']['nq_long_quantile'], .95)


if __name__ == '__main__':
    unittest.main()
