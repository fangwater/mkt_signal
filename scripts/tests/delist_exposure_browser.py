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
    mode = {'gate': 'excess'}
    slugs = ['gate_fr_arb01', 'gate_fr_arb04', 'bybit_fr_arb01']
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
                if self.path == '/nav-api/strategies':
                    return self.respond([{'slug': slug, 'alias': '<img src=x onerror=alert(1)>' if slug == slugs[0] else slug,
                                          'host': 'sg' if slug.startswith('bybit') else 'jp',
                                          'strategyKind': 'funding_rate', 'exchange': 'bybit' if slug.startswith('bybit') else 'gate'} for slug in slugs]
                                         + [{'slug': 'binance_exec_trade01', 'host': 'jp', 'strategyKind': 'cta', 'exchange': 'binance'}])
                if self.path.startswith('/fr/') and self.path.endswith('/snapshot'):
                    slug = self.path.split('/')[2]
                    if slug == slugs[0] and mode['gate'] == 'failed':
                        return self.respond({'error': 'fixture timeout'}, status=503)
                    now = int(time.time()*1000)
                    qty = 160 if slug == slugs[0] and mode['gate'] != 'recovered' else 1
                    price = 0 if slug == slugs[2] else 1
                    timestamp = now-10_000 if slug == slugs[0] and mode['gate'] == 'stale' else now
                    row = {'asset': 'TEST', 'open_qty': 1000+qty, 'hedge_qty': -1000,
                           'open_usdt': (1000+qty)*price, 'hedge_usdt': -1000*price,
                           'net_qty': qty, 'net_usdt': qty*price, 'is_total': False}
                    rows = [row]
                    if slug == slugs[0]:
                        rows.append({'asset': 'UNPRICED', 'open_qty': 20, 'hedge_qty': 0,
                                     'open_usdt': 0, 'hedge_usdt': 0, 'net_qty': 20,
                                     'net_usdt': 0, 'is_total': False})
                    return self.respond({'entries': [
                        {'channel': 'pre_trade_exposure', 'entry': {'ts_ms': timestamp, 'rows': rows}},
                        {'channel': 'pre_trade_risk', 'entry': {'ts_ms': now, 'account_risks': [{'ts_ms': timestamp}], 'total_position': 2000}}
                    ]})
                # Other board sections have separate tests; avoid Redis/Manager in this fixture.
                fixtures = {'/delist/accounts': {'accounts': [], 'summary': {}, 'as_of_ms': int(time.time()*1000)},
                            '/delist/flatten-candidates': {'items': []}, '/delist/cta-strategies': {'strategies': [], 'managers': []}, '/delist/status': {'sources': []}}
                if self.path in fixtures:
                    return self.respond(fixtures[self.path])
                if self.path.startswith('/delist/'):
                    return self.proxy()
                self.send_error(404)

            def do_POST(self):
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
        environment = {key: value for key, value in os.environ.items() if not key.startswith(('DELIST_', 'LOCAL_NOTIFICATION_'))}
        log = open(root/'service.log', 'wb')
        server = subprocess.Popen([str(args.delist_binary.resolve()), '--bind', f'127.0.0.1:{port}',
            '--access-config', str(root/'access.json'), '--book', str(root/'data/book.json'),
            '--web-dir', str(ROOT/'web/delist_risk'), '--snapshot-base-url', origin,
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
                page.on('pageerror', lambda error: errors.append(str(error)))
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

                data = wait_account(lambda a: a['risk_status'] == 'alert')
                assert len(data['accounts']) == 3
                assert next(a for a in data['accounts'] if a['slug'] == slugs[2])['risk_status'] == 'unknown'
                page.goto(origin+'/delist/')
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
                mode['gate'] = 'stale'
                wait_account(lambda a: any('过期' in e for e in a['errors']))
                mode['gate'] = 'recovered'
                wait_account(lambda a: a['risk_status'] == 'unknown' and a['abs_usdt'] == 1 and not a['breaches'])
                # Wait for the page's real ten-second refresh, not a direct render call.
                page.wait_for_function("document.querySelector('[data-exposure-account=gate_fr_arb01]').textContent.includes('无法确认') && !document.querySelector('#exposures .exposure__breach')", timeout=15_000)
                assert '有持仓但缺少有效价格' in page.locator('#exposures').inner_text()
                page.set_viewport_size({'width': 390, 'height': 844})
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                assert page.locator('details[open] .exposure__scroll').evaluate('e => e.scrollWidth > e.clientWidth')
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'exposure-mobile.png'), full_page=True)
                assert not errors, errors
                browser.close()
            print('PASS: authenticated exposure collection, Gate04/SG discovery, sustained alert, stale/failure preservation, recovery, automatic refresh, escaping, desktop/mobile')
        finally:
            server.terminate()
            server.wait(timeout=10)
            log.close()
            gateway.shutdown()
            gateway.server_close()


if __name__ == '__main__':
    main()
