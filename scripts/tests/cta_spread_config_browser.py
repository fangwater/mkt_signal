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
        redis.values[env + ':cta_symbol_factors'] = json.dumps({'FILUSDT': {'factor_113': -1}})
        redis.values[env + ':cta_factor_strategies'] = json.dumps({'factor_113': {'max_pos_u': 1000, 'max_pos_u_overrides': {}}})
        manager_key = 'binance-exec-cta-spread-test:binance-futures:batch_exec:factor_113'
        redis.values[manager_key] = '{"maker_timeout_ms":4321,"targets":{"FILUSDT":{"qty":"clear","signal":0}}}'
        original_exec = redis.values[manager_key]
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
                page.wait_for_function("() => document.querySelector('#signal-status').textContent === '读取完成' && document.querySelector('#factor-json').value.includes('FILUSDT')")
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
                assert config['entry']['spread_lower_quantile'] == .3
                assert config['entry']['spread_upper_quantile'] == .7
                assert page.locator('#nq_change_enabled').count() == 0
                assert config['enabled'] is False
                assert config['max_pos_u'] == 2000
                assert redis.values[manager_key] == original_exec
                saved_sizing = redis.values[env + ':cta_factor_strategies']
                saved_factors = redis.values[env + ':cta_symbol_factors']
                page.locator('#factor_exit_quantile_long').fill('.95')
                page.locator('#factor-json').fill('{"FILUSDT":{"factor_113":1}}')
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
                print(json.dumps({'single_global_amount': True, 'merged_signal_params': True, 'removed_obsolete_controls': True, 'save_validation_and_revision': True, 'manager_parameters_preserved': True, 'spread_bounds_saved': True, 'desktop_mobile_no_overflow': True, 'javascript_errors': errors}))
        finally:
            server.shutdown()
            server.server_close()


if __name__ == '__main__':
    main()
