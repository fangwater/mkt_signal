#!/usr/bin/env python3
"""Verify the simplified config UI against local file/Redis fixtures, without orders."""
from __future__ import annotations
import argparse
import json
import pathlib
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer
from playwright.sync_api import sync_playwright

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'scripts/tests'))
from cta_spread_config_server import ConfigStore, make_handler
from test_cta_perp_servers import FakeFactorRedis


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary', type=pathlib.Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cta-config-browser-') as temp:
        directory = pathlib.Path(temp)
        for name in ('cta_spread.json', 'cta_spread_factor_model_1m_pub.toml'):
            (directory / name).write_bytes((ROOT / 'config' / name).read_bytes())
        redis = FakeFactorRedis()
        env = 'binance-cta-spread-test'
        redis.values[env + ':cta_symbol_factors'] = json.dumps({'AXSUSDT': {'TD_TI_024': -1}})
        redis.values[env + ':cta_factor_strategies'] = json.dumps({'TD_TI_024': {'max_pos_u': 1000, 'max_pos_u_overrides': {}}})
        manager_key = 'binance-exec-cta-spread-test:binance-futures:batch_exec:TD_TI_024'
        redis.values[manager_key] = '{"maker_timeout_ms":4321,"targets":{"AXSUSDT":{"qty":"clear","signal":0}}}'
        original_exec = redis.values[manager_key]
        deployment = json.loads((ROOT / 'config/cta_spread_deployment.json').read_text())
        store = ConfigStore(directory / 'cta_spread.json')
        server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(store, ROOT / 'web/cta_spread_config/index.html', redis, env))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(executable_path=str(args.browser_binary) if args.browser_binary else None, headless=True, args=['--no-sandbox'])
                page = browser.new_page(viewport={'width': 1440, 'height': 1100})
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto(f'http://127.0.0.1:{server.server_port}/')
                page.wait_for_function("() => document.querySelector('#signal-status').textContent === '读取完成' && document.querySelector('#factor-json').value.includes('AXSUSDT')")
                for field in ('cooldown_seconds', 'trade_sides', 'signal_delay_seconds', 'factor_exit_enabled', 'strategy-table', 'risk-params', 'factor-strategies'):
                    assert page.locator('#' + field).count() == 0, field
                assert page.locator('main > section').count() == 2
                assert page.locator('a[href="/manager/"]').count() == 1
                assert page.locator('#factor_exit_quantile_long').input_value() == '0.7'
                assert page.locator('#factor_exit_quantile_short').input_value() == '0.3'
                page.locator('#factor_long_quantile').fill('0.88')
                page.locator('#factor_exit_quantile_long').fill('0.35')
                assert page.locator('#cta-signal #max_pos_u').count() == 1
                page.locator('#max_pos_u').fill('2000')
                with page.expect_response(lambda response: response.url.endswith('/api/symbol-factors') and response.request.method == 'POST'):
                    page.locator('#save-all').click()
                config, _ = store.load()
                assert config['entry']['factor_long_quantile'] == .88
                assert config['execution']['factor_exit_quantile_long'] == .35
                assert config['entry']['spread_long_quantile'] == .7
                assert config['entry']['spread_short_quantile'] == .3
                assert page.locator('#nq_change_enabled').count() == 0
                assert config['enabled'] is False
                assert config['max_pos_u'] == 2000
                assert redis.values[manager_key] == original_exec
                overrides = {'AXSUSDT': {'factor_long_quantile': .8, 'factor_exit_quantile_long': .4,
                                         'nq_long_quantile': .65, 'nq_short_quantile': .35}}
                page.locator('#overrides-json').fill(json.dumps(overrides))
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT'):
                    page.locator('#signal-save').click()
                assert store.load()[0]['symbol_overrides'] == overrides
                page.locator('#signal-load').click()
                page.wait_for_function("() => document.querySelector('#signal-status').textContent === '读取完成'")
                assert json.loads(page.locator('#overrides-json').input_value()) == overrides
                saved_overrides = store.load()[0]['symbol_overrides']
                invalid = json.loads(json.dumps(saved_overrides))
                invalid['AXSUSDT']['nq_long_quantile'] = 1.1
                page.locator('#overrides-json').fill(json.dumps(invalid))
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT') as invalid_nq:
                    page.locator('#signal-save').click()
                assert invalid_nq.value.status == 400
                assert store.load()[0]['symbol_overrides'] == saved_overrides
                page.locator('#overrides-json').fill(json.dumps(saved_overrides))
                saved_sizing = redis.values[env + ':cta_factor_strategies']
                saved_factors = redis.values[env + ':cta_symbol_factors']
                page.locator('#factor_exit_quantile_long').fill('.95')
                page.locator('details').filter(has=page.locator('#factor-json')).locator('summary').click()
                page.locator('#factor-json').fill('{"AXSUSDT":{"TD_TI_024":1}}')
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT') as rejected:
                    page.locator('#save-all').click()
                assert rejected.value.status == 400
                assert redis.values[env + ':cta_symbol_factors'] == saved_factors
                assert redis.values[env + ':cta_factor_strategies'] == saved_sizing
                assert store.load()[0]['max_pos_u'] == 2000
                assert redis.values[manager_key] == original_exec
                # Research import previews the current sleeves without a Redis write.
                page.locator('#reload-all').click()
                page.wait_for_function("() => document.querySelector('#signal-status').textContent === '读取完成'")
                before_import = dict(redis.values)
                page.locator('#deployment-file').set_input_files(str(ROOT / 'config/cta_spread_deployment.json'))
                page.wait_for_function("count => document.querySelector('#sym-status').textContent.includes(count + ' 组合')", arg=deployment['n_sleeves'])
                assert redis.values == before_import
                assert page.locator('#nq_long_quantile').input_value() == '0.5'
                assert float(page.locator('#spread_long_quantile').input_value()) == deployment['rule']['spread']['long_quantile']
                page.locator('#factor-search').fill('TD_TI_024')
                assert page.locator('#factor-editor input').count() == 1
                with page.expect_response(lambda response: response.url.endswith('/api/symbol-factors') and response.request.method == 'POST'):
                    page.locator('#save-all').click()
                table = json.loads(redis.values[env + ':cta_symbol_factors'])
                assert sum(len(row) for row in table.values()) == deployment['n_sleeves']
                assert table == json.loads((ROOT / 'config/cta_spread_symbol_factors.json').read_text())
                assert store.load()[0]['symbol_overrides'] == saved_overrides
                assert not page.evaluate('document.documentElement.scrollWidth > innerWidth')
                page.set_viewport_size({'width': 390, 'height': 844})
                assert not page.evaluate('document.documentElement.scrollWidth > innerWidth')
                assert not errors, errors
                browser.close()
                print(json.dumps({'single_global_amount': True, 'merged_signal_params': True, 'removed_obsolete_controls': True, 'save_validation_and_revision': True, 'manager_parameters_preserved': True, 'spread_bounds_saved': True, 'desktop_mobile_no_overflow': True, 'javascript_errors': errors}))
        finally:
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
