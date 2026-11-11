#!/usr/bin/env python3
"""Exercise FR exposure collection, login and the board using disposable accounts."""
import argparse
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary', type=Path, required=True)
    parser.add_argument('--delist-binary', type=Path, required=True)
    parser.add_argument('--screenshot-dir', type=Path)
    args = parser.parse_args()
    mode = {'gate': 'building'}
    backups = []
    slugs = ['gate_fr_arb01', 'gate_fr_arb04', 'bybit_fr_arb01']
    nav_slugs = slugs + ['bitget_fr_arb01']
    with tempfile.TemporaryDirectory(prefix='delist-exposure-browser-') as temporary:
        root = Path(temporary)
        (root/'data').mkdir()
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        service = f'http://127.0.0.1:{port}'
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

        class Gateway(BaseHTTPRequestHandler):
            def log_message(self, *unused):
                pass

            def respond(self, payload, kind='application/json', status=200):
                if not isinstance(payload, bytes):
                    payload = json.dumps(payload).encode()
                self.send_response(status)
                self.send_header('Content-Type', kind)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def proxy(self):
                body = self.rfile.read(int(self.headers.get('Content-Length', '0'))) or None
                headers = {key: self.headers[key] for key in ('Cookie', 'Origin', 'Content-Type') if key in self.headers}
                request = urllib.request.Request(service+self.path.removeprefix('/delist'), data=body, headers=headers, method=self.command)
                try:
                    response = opener.open(request, timeout=5)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    payload = response.read()
                    self.send_response(response.status)
                    for key in ('Content-Type', 'Cache-Control', 'Set-Cookie', 'Location'):
                        if key in response.headers:
                            self.send_header(key, response.headers[key])
                    self.send_header('Content-Length', str(len(payload)))
                    self.end_headers()
                    self.wfile.write(payload)

            def do_GET(self):
                if self.path.split('?', 1)[0] == '/nav-api/strategies':
                    return self.respond([{'slug': slug, 'alias': '<img src=x onerror=alert(1)>' if slug == slugs[0] else slug,
                                          'host': 'sg' if slug.startswith('bybit') else 'jp',
                                          'strategyKind': 'funding_rate', 'exchange': 'bybit' if slug.startswith('bybit') else 'bitget' if slug.startswith('bitget') else 'gate'} for slug in nav_slugs]
                                         + [{'slug': 'binance_exec_trade01', 'host': 'jp', 'strategyKind': 'cta', 'exchange': 'binance'}])
                if self.path.split('?', 1)[0].startswith('/nav-api/strategies/') and self.path.split('?', 1)[0].endswith('/pnl'):
                    slug = self.path.split('/')[3]
                    annualized = {slugs[0]: 0.09, slugs[1]: 0.1, slugs[2]: None, nav_slugs[3]: -0.5}[slug]
                    annualized_30d = {slugs[0]: 0.25, slugs[1]: 0.03, slugs[2]: None, nav_slugs[3]: -0.5}[slug]
                    coverage = 86400000 if slug == nav_slugs[3] else 7 * 86400000
                    return self.respond({'performance': {'rolling7d': {'annualizedReturn': annualized, 'coveredMs': coverage},
                                                         'rolling30d': {'annualizedReturn': annualized_30d, 'coveredMs': 7 * 86400000}}})
                if self.path == '/snapshot':
                    now = int(time.time()*1000)
                    entries = []
                    for slug in nav_slugs:
                        if slug == slugs[0] and mode['gate'] == 'failed':
                            continue
                        qty = 160 if slug == slugs[0] and mode['gate'] != 'recovered' else 1
                        price = 0 if slug == slugs[2] else 1
                        timestamp = now-10_000 if slug == slugs[0] and mode['gate'] == 'stale' else now
                        row = {'asset': 'TEST', 'open_qty': 1000+qty, 'hedge_qty': -1000,
                               'open_usdt': (1000+qty)*price, 'hedge_usdt': -1000*price,
                               'net_qty': qty, 'net_usdt': qty*price, 'is_total': False,
                               'arb_hedge_net_qty': qty if mode['gate'] == 'building' and slug == slugs[0] else 0}
                        rows = [row]
                        if slug == slugs[0]:
                            rows.append({'asset': 'UNPRICED', 'open_qty': 20, 'hedge_qty': 0,
                                         'open_usdt': 0, 'hedge_usdt': 0, 'net_qty': 20,
                                         'net_usdt': 0, 'is_total': False, 'arb_hedge_net_qty': 0})
                        entries.extend([
                            {'namespace': slug, 'channel': 'pre_trade_exposure', 'entry': {'ts_ms': timestamp, 'rows': rows}},
                            {'namespace': slug, 'channel': 'pre_trade_risk', 'entry': {'ts_ms': now, 'account_risks': [{'ts_ms': timestamp}], 'total_position': 2000}}
                        ])
                    return self.respond({'entries': entries})
                # Keep the existing navigation and backups populated: an empty board
                # misses layout regressions caused by adding the exposure section.
                accounts = [{'slug': slug, 'alias': slug, 'kind': 'funding_rate',
                             'exchange': 'bybit' if slug.startswith('bybit') else 'gate',
                             'host': 'sg' if slug.startswith('bybit') else 'jp',
                             'venues': ['gate-margin', 'gate-futures'], 'tone': 'ok',
                             'universe_n': 42, 'risk_n': 0,
                             'viz_url': '/delist/open?env='+slug+'&panel=viz',
                             'config_url': '/delist/open?env='+slug+'&panel=config'} for slug in slugs]
                config_url = '/cta/cta-prep-binance-futures/config/'
                combinations = [{'name': f'last-20_prep02_binance-futures_{i}min',
                                 'config_url': config_url+f'?combination={i}', 'factor_count': 42,
                                 'symbols': ['BTCUSDT'], 'position_strategies': [f'cta-prep02-bn-{i}min-pos']}
                                for i in range(1, 14)]
                cta = {'managers': [{'site': 'jp', 'ok': True}], 'factor_processes': [{
                    'name': 'cta-prep-binance-futures', 'config_url': config_url,
                    'configured': True, 'plan_matches_config': True, 'unique_factor_count': 42,
                    'combinations': combinations}, {
                    'name': 'cta-factor-spread', 'config_url': config_url+'?worker=cta-factor-spread',
                    'configured': True, 'plan_matches_config': True, 'unique_factor_count': 50,
                    'combinations': []}], 'strategies': [{
                    'slug': 'cta-prep02-bn-1min-pos', 'name': combinations[0]['name'],
                    'enabled': True, 'factor_count': 42,
                    'risk': {'tone': 'ok', 'config_url': config_url, 'universe_n': 42},
                    'accounts': [{'source_id': 'binance_exec_rx01', 'alias': 'CTA execution account',
                                  'site': 'jp', 'active': True, 'viz_url': '/exec/rx01/viz/'}]}]}
                other = {**accounts[0], 'slug': 'fixture_intra', 'alias': '所内测试账户', 'kind': 'intra_exchange'}
                mm = {**accounts[0], 'slug': 'okex_mm_83777', 'alias': '做市测试账户', 'kind': 'market_making', 'exchange': 'okex'}
                legacy_cta = {**accounts[0], 'slug': 'fixture_cta', 'alias': '旧 CTA 账户', 'kind': 'cta'}
                fixtures = {'/delist/accounts': {'accounts': accounts+[other, mm, legacy_cta], 'summary': {}, 'as_of_ms': int(time.time()*1000)},
                            '/delist/flatten-candidates': {'items': []}, '/delist/cta-strategies': cta,
                            '/delist/status': {'sources': [{'source': 'cos_mm_backup_okex_mm_83777', 'ok': True, 'last_success_ms': int(time.time()*1000)}]}}
                if self.path in fixtures:
                    return self.respond(fixtures[self.path])
                if self.path.startswith('/delist/'):
                    return self.proxy()
                self.send_error(404)

            def do_POST(self):
                if self.path == '/delist/backup-now':
                    backups.append(json.loads(self.rfile.read(int(self.headers['Content-Length']))))
                    return self.respond({'filename': 'fixture.tar.gz'})
                if self.path.startswith('/delist/'):
                    return self.proxy()
                self.send_error(405)

        gateway = ThreadingHTTPServer(('127.0.0.1', 0), Gateway)
        thread = threading.Thread(target=gateway.serve_forever, daemon=True)
        thread.start()
        origin = f'http://127.0.0.1:{gateway.server_port}'
        salt = os.urandom(16)
        access = {'username': 'fixture', 'password_salt': salt.hex(),
                  'password_hash': hashlib.pbkdf2_hmac('sha256', b'fixture-password', salt, 600_000).hex(),
                  'password_iterations': 600_000, 'public_origin': origin, 'fr_envs': slugs}
        (root/'access.json').write_text(json.dumps(access))
        (root/'snapshots.toml').write_text('[snapshots]\n' + ''.join(f'{slug} = "{origin}/snapshot"\n' for slug in nav_slugs))
        environment = {key: value for key, value in os.environ.items() if not key.startswith(('DELIST_', 'LOCAL_NOTIFICATION_'))}
        log = open(root/'service.log', 'wb')
        server = subprocess.Popen([str(args.delist_binary.resolve()), '--bind', f'127.0.0.1:{port}',
            '--access-config', str(root/'access.json'), '--book', str(root/'data/book.json'),
            '--web-dir', str(ROOT/'web/delist_risk'), '--snapshot-targets', str(root/'snapshots.toml'),
            '--nav-strategies-url', origin+'/nav-api/strategies', '--redis', 'redis://127.0.0.1:1/0',
            '--redis-backup-env-root', str(root), '--skip-postgres', '--skip-redis-backup',
            '--skip-ws', '--skip-llm', '--skip-official', '--skip-announcements',
            '--exposure-interval-secs', '1', '--exposure-max-age-secs', '5', '--exposure-persistence-secs', '2'],
            cwd=root, env=environment, stdout=log, stderr=log)
        try:
            deadline = time.monotonic()+20
            while True:
                try:
                    opener.open(service+'/healthz', timeout=1).close()
                    break
                except urllib.error.HTTPError as error:
                    if error.code == 401:
                        break
                except OSError:
                    pass
                if server.poll() is not None or time.monotonic() > deadline:
                    raise RuntimeError((root/'service.log').read_text())
                time.sleep(.05)
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(executable_path=str(args.browser_binary), headless=True,
                                                    args=['--no-sandbox', '--disable-dev-shm-usage'])
                context = browser.new_context(viewport={'width': 1440, 'height': 1000})
                page = context.new_page()
                errors = []
                requests = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('request', lambda request: requests.append(request.url))
                assert page.request.get(origin+'/delist/exposures').status == 401
                response = page.request.post(origin+'/delist/login', form={'username': 'fixture', 'password': 'fixture-password'}, headers={'Origin': origin})
                assert response.ok, response.text()

                def report():
                    response = page.request.get(origin+'/delist/exposures')
                    assert response.ok, response.text()
                    return response.json()

                def wait_account(predicate):
                    deadline = time.monotonic()+10
                    while time.monotonic() < deadline:
                        data = report()
                        account = next((a for a in data['accounts'] if a['slug'] == slugs[0]), None)
                        if account and predicate(account):
                            return data
                        time.sleep(.1)
                    raise AssertionError(report())

                data = wait_account(lambda a: a['risk_abs_usdt'] == 0 and not a['breaches'])
                time.sleep(2.5)
                assert not next(a for a in report()['accounts'] if a['slug'] == slugs[0])['breaches']
                mode['gate'] = 'excess'
                data = wait_account(lambda a: a['risk_status'] == 'alert')
                assert len(data['accounts']) == 4
                assert next(a for a in data['accounts'] if a['slug'] == slugs[2])['risk_status'] == 'unknown'
                page.goto(origin+'/delist/')
                page.wait_for_selector('#exchange-list .backup-action')
                page.wait_for_function("document.querySelector('#fr-yield').textContent.includes('9.00%')")
                assert '低于 10%' in page.locator('#fr-yield').inner_text()
                assert '不足 30 天' in page.locator('#fr-yield').inner_text()
                assert '25.00%' in page.locator('#fr-yield').inner_text()
                assert 'bybit_fr_arb01' not in page.locator('#fr-yield').inner_text()
                assert 'gate_fr_arb04' not in page.locator('#fr-yield').inner_text()
                assert 'bitget_fr_arb01' not in page.locator('#fr-yield').inner_text()
                assert '1 个 FR 年化数据不可用' in page.locator('#fr-yield').inner_text()
                assert page.locator('#fr-yield tbody tr').count() == 1
                assert '9.00%' in page.locator('#fr-yield tbody tr').first.inner_text()
                assert page.locator('#fr-yield img').count() == 0
                assert not page.locator('#exposure-panel').evaluate('e => e.open')
                assert page.locator('#cta-strategies, #cta-config-entries').count() == 0
                assert not any(url.endswith('/cta-strategies') for url in requests)
                assert page.locator('#exchange-list').bounding_box()['y'] - page.locator('#fr-yield').bounding_box()['height'] < 550
                assert page.locator('#metrics strong').first.inner_text() == '3'
                assert page.locator('#exchange-list .backup-action').count() == 3
                assert page.locator('#exchange-list .config-link').count() == 6
                for width in (1440, 1024, 768, 390, 320):
                    page.set_viewport_size({'width': width, 'height': 1000})
                    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
                    assert page.locator('#exchange-list .account-nav').evaluate_all('''nodes => nodes.every(nav => {
                        const cell = nav.closest('td').getBoundingClientRect();
                        return [...nav.children].every(el => {
                            const rect = el.getBoundingClientRect();
                            return rect.left >= cell.left && rect.right <= cell.right + 1;
                        });
                    })'''), width
                    for button in page.locator('#exchange-list .backup-action').all():
                        button.click(trial=True)
                page.set_viewport_size({'width': 1440, 'height': 1000})
                page.locator('[data-announcement="gate"] > summary').click()
                page.once('dialog', lambda dialog: dialog.accept())
                page.locator('#exchange-list .backup-action').first.click()
                page.wait_for_function("document.querySelector('#exchange-list .backup-action').textContent === '立即备份'")
                assert backups == [{'kind': 'funding_rate', 'env': slugs[0]}]
                assert page.locator('[data-announcement="gate"]').evaluate('e => e.open')
                assert page.locator('#exchange-list .backup-action').count() == 3
                page.locator('[data-filter="all"]').click()
                assert page.locator('#metrics strong').first.inner_text() == '5'
                assert '所内测试账户' in page.locator('#exchange-list').inner_text()
                assert '旧 CTA 账户' not in page.locator('#exchange-list').inner_text()
                page.locator('[data-filter="market_making"]').click()
                assert page.locator('#metrics strong').first.inner_text() == '1'
                assert page.locator('#exchange-list .backup-action').count() == 1
                assert '今日云备份已校验' in page.locator('#exchange-list').inner_text()
                for width in (320, 390, 1440):
                    page.set_viewport_size({'width': width, 'height': 1000})
                    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
                    assert page.locator('#exchange-list .backup-action').evaluate('e => e.getBoundingClientRect().right <= innerWidth'), width
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'mm-desktop.png'), full_page=True)
                page.once('dialog', lambda dialog: dialog.accept())
                page.locator('#exchange-list .backup-action[data-env="okex_mm_83777"]').click()
                page.wait_for_function('!document.querySelector("#exchange-list .backup-action").disabled')
                assert backups[-1] == {'kind': 'market_making', 'env': 'okex_mm_83777'}
                page.locator('[data-filter="funding_rate"]').click()
                if args.screenshot_dir:
                    args.screenshot_dir.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(args.screenshot_dir/'board-desktop.png'), full_page=True)
                requests.clear()
                page.locator('.page-nav [data-page="cta"]').click()
                page.wait_for_selector('#cta-strategies .cta-table')
                assert page.url == origin+'/delist/?view=cta'
                assert page.title() == 'CTA 策略与因子 · Risk Monitor'
                assert page.locator('#exchange-list, #exposure-panel, #filters').count() == 0
                assert page.locator('#metrics strong').first.inner_text() == '1'
                assert page.locator('#cta-config-entries a').count() == 2
                assert page.locator('#cta-strategies .backup-action').count() == 2
                for worker, kind in [('cta-prep-binance-futures', 'cta_perp'), ('cta-factor-spread', 'cta_spread')]:
                    page.once('dialog', lambda dialog: dialog.accept())
                    page.locator(f'#cta-strategies .backup-action[data-env="{worker}"]').click()
                    page.wait_for_function('!document.querySelector("#cta-strategies .backup-action").disabled')
                    assert backups[-1] == {'kind': kind, 'env': worker}
                assert not any(url.endswith(('/accounts', '/exposures', '/flatten-candidates', '/notifications')) for url in requests)
                for width in (1440, 1024, 768, 390, 320):
                    page.set_viewport_size({'width': width, 'height': 1000})
                    assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), width
                    assert page.locator('#cta-strategies a').evaluate_all('nodes => nodes.every(e => e.getBoundingClientRect().right <= innerWidth)'), width
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'cta-mobile.png'), full_page=True)
                page.set_viewport_size({'width': 1440, 'height': 1000})
                page.reload()
                page.wait_for_selector('#cta-strategies .cta-table')
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'cta-desktop.png'), full_page=True)
                page.locator('.page-nav [data-page="fr"]').click()
                page.wait_for_selector('#exchange-list .backup-action')
                assert page.locator('#metrics strong').first.inner_text() == '3'
                page.locator('#exposure-panel > summary').click()
                page.wait_for_selector('[data-exposure-account="gate_fr_arb01"]')
                assert page.locator('[data-exposure-account]').count() == 3
                assert '持续超限' in page.locator('#exposures').inner_text()
                assert page.locator('#exposures img').count() == 0
                assert page.locator('#exposures a[href*="gate_fr_arb04"]').count() == 1
                details = page.locator('details[data-env="gate_fr_arb01"]')
                details.locator('summary').click()
                assert '160.00 U' in details.inner_text()
                if args.screenshot_dir:
                    args.screenshot_dir.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(args.screenshot_dir/'exposure-desktop.png'), full_page=True)
                mode['gate'] = 'failed'
                data = wait_account(lambda a: a['data_status'] == 'unavailable')
                account = next(a for a in data['accounts'] if a['slug'] == slugs[0])
                assert account['risk_status'] == 'alert' and account['abs_usdt'] == 160
                page.evaluate('loadExposures()')
                page.wait_for_function("document.querySelector('#exposures').textContent.includes('保留异常记录')")
                assert page.locator('details[data-env="gate_fr_arb01"][open]').count() == 1
                assert page.locator('#exposure-panel').evaluate('e => e.open')
                mode['gate'] = 'stale'
                wait_account(lambda a: any('过期' in e for e in a['errors']))
                mode['gate'] = 'recovered'
                wait_account(lambda a: a['risk_status'] == 'unknown' and a['abs_usdt'] == 1 and not a['breaches'])
                # Wait for the page's real ten-second refresh, not a direct render call.
                page.wait_for_function("document.querySelector('[data-exposure-account=gate_fr_arb01]').textContent.includes('无法确认') && !document.querySelector('#exposures .exposure__breach')", timeout=15_000)
                assert '有持仓但缺少有效价格' in page.locator('#exposures').inner_text()
                page.set_viewport_size({'width': 390, 'height': 844})
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                assert page.locator('#exposures details[open] .exposure__scroll').evaluate('e => e.scrollWidth > e.clientWidth')
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'exposure-mobile.png'), full_page=True)
                assert not errors, errors
                browser.close()
            print('PASS: independent FR/CTA pages and API loading, navigation and backup containment at 320–1440px, manual backup request, filters/counts, announcement state, exposure expand/refresh, alerts, recovery, escaping')
        finally:
            server.terminate()
            server.wait(timeout=10)
            log.close()
            gateway.shutdown()
            gateway.server_close()


if __name__ == '__main__':
    main()
