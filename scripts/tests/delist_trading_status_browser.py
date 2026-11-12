#!/usr/bin/env python3
"""Check FR opening-gate display using local fixtures only."""
import argparse
import json
from pathlib import Path
import time
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary', type=Path, required=True)
    parser.add_argument('--screenshot', type=Path)
    args = parser.parse_args()
    slug = 'binance_fr_arb03'
    state = {'problem': True, 'stale': False}
    errors = []
    writes = []
    html = (ROOT / 'web/delist_risk/index.html').read_text()

    def respond(route):
        path = urlparse(route.request.url).path
        if route.request.method != 'GET':
            writes.append(path)
        if path == '/delist/':
            return route.fulfill(body=html, content_type='text/html')
        now = int(time.time() * 1000)
        timestamp = now - 100_000 if state['stale'] else now
        status = {'state': 'binance_margin_frequency_cooldown' if state['problem'] else 'ready',
                  'problem': state['problem'],
                  'detail': '交易所频控冷却，暂停新开仓；现金 95.02 U <img src=x onerror=alert(1)>'
                  if state['problem'] else '现金与可借额度已就绪，开仓资金/频控门禁已解除'}
        account = {'slug': slug, 'alias': slug, 'exchange': 'binance', 'host': 'jp',
                   'kind': 'funding_rate', 'tone': 'ok', 'universe_n': 42, 'risk_n': 0,
                   'venues': ['binance-margin', 'binance-futures']}
        exposure = {**account, 'checked_ms': timestamp, 'snapshot_ms': timestamp,
                    'account_ms': timestamp, 'complete': True, 'data_status': 'fresh',
                    'risk_status': 'ok', 'risk_abs_usdt': 219, 'abs_usdt': 219,
                    'largest_asset': 'BNB', 'largest_usdt': -93, 'rows': [],
                    'errors': [], 'breaches': {}, 'trading_status': status}
        fixtures = {
            '/delist/accounts': {'accounts': [account], 'as_of_ms': now},
            '/delist/exposures': {'accounts': [exposure], 'checked_ms': timestamp,
                                  'errors': [], 'settings': {'interval_secs': 10,
                                  'max_age_secs': 90, 'asset_limit_usdt': 100,
                                  'account_limit_usdt': 500, 'persistence_secs': 30}},
            '/delist/notifications': {'enabled': True, 'queued': 0},
        }
        route.fulfill(body=json.dumps(fixtures.get(path, {})), content_type='application/json')

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(executable_path=str(args.browser_binary),
                                             headless=True, args=['--no-sandbox'])
        page = browser.new_page(viewport={'width': 1440, 'height': 1000})
        page.route('**/*', respond)
        page.on('pageerror', lambda error: errors.append(str(error)))
        page.goto('http://delist.test/delist/')
        page.wait_for_function("document.querySelector('#exposure-summary').textContent.includes('1 个开仓受限')")
        assert not page.locator('#exposure-panel').evaluate('e => e.open')
        assert '1 个 FR 开仓受限' in page.locator('#system-state').inner_text()
        assert '下架风险' not in page.locator('#system-state').inner_text()
        assert page.locator('#metrics strong').all_text_contents() == ['1', '1', '0', '0']
        assert '开仓受限' in page.locator('#exchange-list').inner_text()
        page.locator('#exposure-panel > summary').click()
        assert '交易所频控冷却' in page.locator('#exposures').inner_text()
        assert page.locator('#exposures img, #exchange-list img').count() == 0
        for width in (1440, 390, 320):
            page.set_viewport_size({'width': width, 'height': 1000})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
        if args.screenshot:
            args.screenshot.parent.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(args.screenshot), full_page=True)
        page.set_viewport_size({'width': 1440, 'height': 1000})
        state['stale'] = True
        page.locator('#refresh').click()
        page.wait_for_function("document.querySelector('#exposure-summary').textContent.includes('开仓状态待确认')")
        assert '等待新快照确认' in page.locator('#exposures').inner_text()
        state.update(stale=False, problem=False)
        page.locator('#refresh').click()
        page.wait_for_function("document.querySelector('#exposures').textContent.includes('开仓资金/频控门禁已解除')")
        assert '开仓受限' not in page.locator('#system-state').inner_text()
        assert page.locator('#metrics strong').all_text_contents() == ['1', '0', '0', '1']
        assert page.locator('#exposure-panel').evaluate('e => e.open')
        assert not errors, errors
        assert not writes, writes
        browser.close()
    print('PASS: blocked/unknown/recovered opening status, summaries, escaping, mobile layout, no writes')


if __name__ == '__main__':
    main()
