#!/usr/bin/env python3
"""Exercise the unified editor against disposable catalogs, with no Kafka/accounts."""
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

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary', type=Path, required=True)
    parser.add_argument('--screenshot-dir', type=Path)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cta-unified-browser-') as temporary:
        root = Path(temporary)
        (root/'config').mkdir()
        for name in ('cta_prep_binance_futures', 'cta_factor_spread'):
            shutil.copytree(ROOT/'config'/name, root/'config'/name)
            shutil.copyfile(ROOT/'config'/(name+'.toml'), root/'config'/(name+'.toml'))
        history=root/'data/cta_prep_binance_futures'
        history.mkdir(parents=True)
        (history/'state.json').write_text(json.dumps({
            'worker':'cta-prep-binance-futures','updated_ms':int(time.time()*1000),'warming':False,
            'position_tables':{'tables':[{'strategy_name':'cta-prep-bn-15min-pos',
                'combination_name':'binance-futures_15min','enabled':False,'retired':False,
                'retiring':False,'targets':{'ETHUSDT':{'qty':1.25,'signal':0}}}]}
        }))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        server = subprocess.Popen([
            str(ROOT/'target/debug/cta-factor-config'), '--config', str(root/'config/cta_prep_binance_futures.toml'),
            '--spread-config', str(root/'config/cta_factor_spread.toml'), '--port', str(port)
        ], cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        base = f'http://127.0.0.1:{port}/'
        def api(path, body=None):
            data = None if body is None else json.dumps(body).encode()
            request = urllib.request.Request(base+'api/'+path, data=data, headers={'Content-Type':'application/json'})
            with opener.open(request, timeout=5) as response:
                return json.load(response)
        try:
            deadline = time.monotonic()+20
            while True:
                try:
                    api('config')
                    break
                except (OSError, ValueError):
                    if server.poll() is not None or time.monotonic()>deadline:
                        raise RuntimeError('fixture configuration server failed to start')
                    time.sleep(.05)
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(executable_path=str(args.browser_binary), headless=True, args=['--no-sandbox','--disable-dev-shm-usage'])
                page = browser.new_page(viewport={'width':1440,'height':1000})
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('dialog', lambda dialog: dialog.accept())
                page.goto(base)
                page.wait_for_function("document.querySelector('#count').textContent.includes('304')")
                assert page.locator('#rows tr').count()==146
                assert 'cta-prep-bn-15min-pos' in page.locator('#manager-strategy').inner_text()
                page.wait_for_function("document.querySelector('#position-targets').textContent.includes('1.25')")
                assert page.locator('#worker-select option').count()==2
                page.locator('#max-pos').fill('250')
                with page.expect_response(lambda response: response.url.endswith('/api/combinations?worker=cta-prep-binance-futures')) as saved:
                    page.locator('#save').click()
                assert saved.value.status==200
                recipe=root/'config/cta_prep_binance_futures/binance-futures_15min.json'
                data=json.loads(recipe.read_text())
                assert data['signal']['max_pos_u']==250
                assert len(data['factor_rules'])==146 and 'factors' not in data
                assert len([r for r in data['factor_rules'] if r['标的']=='SKLUSDT' and r['因子']=='factor_170'])==2
                # Another editor changes the catalog. A stale save retains the draft.
                current=api('config')
                external=current['plan']['combinations']['binance-futures_1min']
                external['signal']['max_pos_u']=123
                api('combinations', {'combination':external,'expected_revision':current['revision']})
                page.locator('#max-pos').fill('999')
                with page.expect_response(lambda response: '/api/combinations' in response.url) as conflict:
                    page.locator('#save').click()
                assert conflict.value.status==409
                assert page.locator('#max-pos').input_value()=='999'
                assert json.loads(recipe.read_text())['signal']['max_pos_u']==250
                page.locator('#reload').click()
                page.wait_for_function("document.querySelector('#max-pos').value==='250'")
                original=recipe.read_bytes()
                page.get_by_label('nq 1', exact=True).fill('2')
                with page.expect_response(lambda response: '/api/preview' in response.url) as invalid:
                    page.locator('#preview').click()
                assert invalid.value.status==400
                assert recipe.read_bytes()==original
                page.locator('#worker-select').select_option('cta-factor-spread')
                page.wait_for_function("document.querySelector('#worker').textContent==='cta-factor-spread'")
                assert 'cta-spread-okx-bn-1min-pos' in page.locator('#manager-strategy').inner_text()
                assert page.locator('#spread-long').input_value()=='0.3'
                page.locator('#new').click()
                page.locator('#name').fill('okx_single_1min')
                assert page.locator('#venue').input_value()=='okex-futures'
                page.locator('#has-signal').check()
                with page.expect_response(lambda response: '/api/combinations' in response.url) as unsupported:
                    page.locator('#save').click()
                assert unsupported.value.status==400
                page.locator('#has-signal').uncheck()
                with page.expect_response(lambda response: '/api/combinations' in response.url) as okx:
                    page.locator('#save').click()
                assert okx.value.status==200
                okx_path=root/'config/cta_factor_spread/okx_single_1min.json'
                okx_config=json.loads(okx_path.read_text())
                assert okx_config['venue']=='okex-futures'
                assert 'signal' not in okx_config
                assert set(okx_config['factor_rules'][0])=={'标的','nq','符号','因子'}
                with page.expect_response(lambda response: response.request.method=='DELETE') as deleted:
                    page.locator('#delete').click()
                assert deleted.value.status==200 and not okx_path.exists()
                page.locator('#worker-select').select_option('cta-prep-binance-futures')
                page.wait_for_function("document.querySelector('#worker').textContent==='cta-prep-binance-futures'")
                page.locator('#combination-list').select_option('binance-futures_5min')
                assert page.locator('#nq-long').input_value()=='0.5'
                if args.screenshot_dir:
                    args.screenshot_dir.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(args.screenshot_dir/'desktop.png'), full_page=True)
                page.set_viewport_size({'width':390,'height':844})
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'mobile.png'), full_page=True)
                assert not errors, errors
                browser.close()
                print('Unified editor passed: four-column JSON, signal save, conflict, invalid NQ, OKX profile, delete and mobile layout')
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill(); server.wait()

if __name__=='__main__':
    main()
