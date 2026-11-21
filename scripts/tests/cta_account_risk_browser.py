#!/usr/bin/env python3
"""Exercise account leverage edits using a disposable Redis and real config API."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

import redis
from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config-binary', type=Path, required=True)
    parser.add_argument('--redis-server', required=True)
    parser.add_argument('--browser-executable', type=Path)
    parser.add_argument('--screenshot-dir', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cta-account-risk-browser-') as directory:
        root = Path(directory)
        (root/'config').mkdir()
        shutil.copyfile(ROOT/'config/cta_prep_binance_futures.toml', root/'config/cta_prep_binance_futures.toml')
        shutil.copytree(ROOT/'config/cta_prep_binance_futures', root/'config/cta_prep_binance_futures')
        catalog = root/'accounts.toml'
        catalog.write_text('''[[sources]]
id="rx01"
alias="LGQ RX01"
venue="binance-futures"
[[sources]]
id="okx01"
alias="LGQ RX03"
venue="okex-futures"
[[sources]]
id="unselected"
venue="binance-futures"
''')
        port, redis_port = free_port(), free_port()
        processes = []
        try:
            processes.append(subprocess.Popen([args.redis_server, '--bind', '127.0.0.1', '--port', str(redis_port),
                '--save', '', '--appendonly', 'no', '--dir', str(root)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            client = redis.Redis(host='127.0.0.1', port=redis_port, decode_responses=True)
            for _ in range(100):
                try:
                    client.ping()
                    break
                except redis.ConnectionError:
                    time.sleep(.03)
            bn = 'rx01:binance-futures:pre_trade_risk_params'
            ok = 'okx01:okex-futures:pre_trade_risk_params'
            client.hset(bn, mapping={'max_leverage': '1', 'exec_order_rate_limit_10s': '300'})
            client.hset(ok, mapping={'max_leverage': '2', 'exec_order_rate_limit_10s': '200'})
            processes.append(subprocess.Popen([str(args.config_binary.resolve()), '--config', str(root/'config/cta_prep_binance_futures.toml'),
                '--port', str(port), '--exec-account-catalog', str(catalog), '--exec-risk-source-ids', 'rx01,okx01',
                '--exec-risk-redis-port', str(redis_port), '--parameter-audit-dir', str(root/'audit')],
                cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE))
            base = f'http://127.0.0.1:{port}/'
            opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            for _ in range(200):
                try:
                    opener.open(base+'api/account-risk', timeout=2).close()
                    break
                except OSError:
                    if processes[-1].poll() is not None:
                        raise RuntimeError(processes[-1].stderr.read().decode())
                    time.sleep(.05)
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True, args=['--no-sandbox'],
                    executable_path=str(args.browser_executable) if args.browser_executable else None)
                page = browser.new_page(viewport={'width': 1280, 'height': 900})
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('dialog', lambda dialog: dialog.accept())
                page.goto(base)
                page.wait_for_function("document.querySelector('#account-risk-message').textContent.includes('已读取')")
                rows = page.locator('#account-risk-fields > div')
                assert rows.count() == 2
                bn_row = page.locator('[data-source-id="rx01"]')
                ok_row = page.locator('[data-source-id="okx01"]')
                assert bn_row.locator('input').input_value() == '1'
                assert ok_row.locator('input').input_value() == '2'
                ok_row.locator('input').fill('3')
                bn_row.locator('input').fill('1.5')
                bn_row.get_by_role('button', name='保存账户阈值').click()
                page.wait_for_function("document.querySelector('#account-risk-message').textContent.includes('已保存')")
                assert client.hget(bn, 'max_leverage') == '1.5'
                assert client.hget(ok, 'max_leverage') == '2'
                assert ok_row.locator('input').input_value() == '3'
                assert client.hget(bn, 'exec_order_rate_limit_10s') == '300'
                assert not client.exists('rx01:okex-futures:pre_trade_risk_params')
                page.get_by_role('button', name='读取账户阈值').click()
                page.wait_for_function("document.querySelector('#account-risk-message').textContent.includes('已读取')")
                assert ok_row.locator('input').input_value() == '2'
                client.hset(bn, 'max_leverage', '1.75')
                bn_row.locator('input').fill('1.25')
                bn_row.get_by_role('button', name='保存账户阈值').click()
                page.wait_for_function("document.querySelector('#account-risk-message').textContent.includes('重新读取')")
                assert client.hget(bn, 'max_leverage') == '1.75'
                page.get_by_role('button', name='读取账户阈值').click()
                page.wait_for_function("document.querySelector('#account-risk-message').textContent.includes('已读取')")
                assert bn_row.locator('input').input_value() == '1.75'
                bn_row.locator('input').fill('0')
                bn_row.get_by_role('button', name='保存账户阈值').click()
                page.wait_for_function("document.querySelector('#account-risk-message').textContent.includes('限制已关闭')")
                assert client.hget(bn, 'max_leverage') == '0'
                assert client.hget(ok, 'max_leverage') == '2'
                if args.screenshot_dir:
                    args.screenshot_dir.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(args.screenshot_dir/'account-risk-desktop.png'))
                page.set_viewport_size({'width': 390, 'height': 844})
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                assert bn_row.get_by_role('button', name='保存账户阈值').is_visible()
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'account-risk-mobile.png'))
                assert not errors, errors
                records = [json.loads(path.read_text()) for path in (root/'audit').glob('*.json')]
                changed = [r for r in records if r.get('objects')]
                assert len(changed) == 2
                assert all(r['family'] == 'exec' and r['environment'] == 'rx01' for r in records)
                browser.close()
            print('PASS: save/readback, separate exchange budgets, stale edits, disabled limit, audit and desktop/mobile layout')
        finally:
            for process in reversed(processes):
                process.terminate()
                process.wait(timeout=10)


if __name__ == '__main__':
    main()
