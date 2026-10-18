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
from cta_perp_config_server import ConfigStore, make_handler
from test_cta_perp_servers import FakeFactorRedis


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary', type=pathlib.Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cta-config-browser-') as temp:
        directory = pathlib.Path(temp)
        for name in ('cta_perp.json', 'cta_perp_factor_model_1m_pub.toml'):
            (directory / name).write_bytes((ROOT / 'config' / name).read_bytes())
        redis = FakeFactorRedis()
        env = 'binance-cta-perp-test'
        redis.values[env + ':cta_symbol_factors'] = json.dumps({'NEARUSDT': {'factor_036': -1}})
        redis.values[env + ':cta_factor_strategies'] = json.dumps({'factor_036': {'max_pos_u': 1000, 'max_pos_u_overrides': {}}})
        manager_key = 'binance-exec-cta-perp-test:binance-futures:batch_exec:factor_036'
        redis.values[manager_key] = '{"maker_timeout_ms":4321,"targets":{"NEARUSDT":{"qty":"clear","signal":0}}}'
        original_exec = redis.values[manager_key]
        store = ConfigStore(directory / 'cta_perp.json')
        server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(store, ROOT / 'web/cta_perp_config/index.html', redis, env))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(executable_path=str(args.browser_binary) if args.browser_binary else None, headless=True, args=['--no-sandbox'])
                page = browser.new_page(viewport={'width': 1440, 'height': 1100})
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.goto(f'http://127.0.0.1:{server.server_port}/')
                page.wait_for_function("() => document.querySelector('#signal-status').textContent === '读取完成' && document.querySelector('#factor-json').value.includes('NEARUSDT')")
                for field in ('cooldown_seconds', 'trade_sides', 'signal_delay_seconds', 'factor_exit_enabled', 'strategy-table', 'risk-params', 'factor-strategies'):
                    assert page.locator('#' + field).count() == 0, field
                assert page.locator('main > section').count() == 2
                assert page.locator('a[href="/manager/"]').count() == 1
                page.locator('#factor_long_quantile').fill('0.88')
                page.locator('#factor_exit_quantile_long').fill('0.35')
                assert page.locator('#cta-signal #max_pos_u').count() == 1
                page.locator('#max_pos_u').fill('2000')
                with page.expect_response(lambda response: response.url.endswith('/api/symbol-factors') and response.request.method == 'POST'):
                    page.locator('#save-all').click()
                config, _ = store.load()
                assert config['entry']['factor_long_quantile'] == .88
                assert config['execution']['factor_exit_quantile_long'] == .35
                assert config['entry']['nq_long_quantile'] == config['entry']['nq_short_quantile'] == .5
                assert config['enabled'] is False
                assert config['max_pos_u'] == 2000
                assert redis.values[manager_key] == original_exec
                page.locator('#position_algorithm').select_option('v1')
                page.locator('#override-add').click()
                row = page.locator('#override-rows tr').first
                row.locator('[data-field="symbol"]').fill('NEARUSDT')
                row.locator('[data-field="factor_long_quantile"]').fill('.7')
                row.locator('[data-field="factor_exit_quantile_long"]').fill('.2')
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT'):
                    page.locator('#signal-save').click()
                config, _ = store.load()
                assert config['position_algorithm'] == 'v1'
                assert config['symbol_overrides'] == {'NEARUSDT': {'factor_long_quantile': .7, 'factor_exit_quantile_long': .2}}
                page.locator('#position_algorithm').select_option('v0')
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT'):
                    page.locator('#signal-save').click()
                assert store.load()[0]['symbol_overrides'] == config['symbol_overrides']
                page.locator('#position_algorithm').select_option('v1')
                row.locator('[data-field="factor_short_quantile"]').fill('-.6')
                row.locator('[data-field="factor_exit_quantile_short"]').fill('-.2')
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT'):
                    page.locator('#signal-save').click()
                assert store.load()[0]['symbol_overrides']['NEARUSDT']['factor_short_quantile'] == -.6
                page.locator('#position_algorithm').select_option('v0')
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT') as invalid_algorithm:
                    page.locator('#signal-save').click()
                assert invalid_algorithm.value.status == 400
                assert store.load()[0]['position_algorithm'] == 'v1'
                page.locator('#position_algorithm').select_option('v1')
                row.locator('[data-field="factor_exit_quantile_short"]').fill('')
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT'):
                    page.locator('#signal-save').click()
                assert 'factor_exit_quantile_short' not in store.load()[0]['symbol_overrides']['NEARUSDT']
                saved_sizing = redis.values[env + ':cta_factor_strategies']
                saved_factors = redis.values[env + ':cta_symbol_factors']
                page.locator('#factor_exit_quantile_long').fill('.95')
                page.locator('#factor-json').fill('{"NEARUSDT":{"factor_036":1}}')
                with page.expect_response(lambda response: response.url.endswith('/api/config') and response.request.method == 'PUT') as rejected:
                    page.locator('#save-all').click()
                assert rejected.value.status == 400
                assert redis.values[env + ':cta_symbol_factors'] == saved_factors
                assert redis.values[env + ':cta_factor_strategies'] == saved_sizing
                assert store.load()[0]['max_pos_u'] == 2000
                assert redis.values[manager_key] == original_exec
                assert not page.evaluate('document.documentElement.scrollWidth > innerWidth')
                page.set_viewport_size({'width': 390, 'height': 844})
                assert not page.evaluate('document.documentElement.scrollWidth > innerWidth')
                assert not errors, errors
                browser.close()
                print(json.dumps({'position_algorithm_switch': True, 'partial_symbol_overrides': True, 'algorithm_range_validation': True, 'single_global_amount': True, 'merged_signal_params': True, 'removed_obsolete_controls': True, 'save_validation_and_revision': True, 'manager_parameters_preserved': True, 'nq_preserved': True, 'desktop_mobile_no_overflow': True, 'javascript_errors': errors}))
        finally:
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
