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
            'position_tables':{'tables':[{'strategy_name':'cta-prep01-bn-15min-pos',
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
                page.wait_for_function("document.querySelector('main').getAttribute('aria-busy')==='false' && document.querySelector('#factor-json').value!==''")
                assert page.locator('#parameters,#position-targets,#rows,#import-json').count()==0
                assert page.locator('textarea').count()==1
                page.locator('#strategy-group').select_option('prep02')
                page.locator('[data-interval="5min"]').click()
                rows=json.loads(page.locator('#factor-json').input_value())
                assert all(set(row)=={'频率','NQ','因子','符号','标的'} and isinstance(row['标的'],list) and row['频率']=='5m' for row in rows)
                assert sum(len(row['标的']) for row in rows)==7
                untouched=(root/'config/cta_prep_binance_futures/prep02_binance-futures_1min.json').read_bytes()
                page.locator('#max-pos').fill('250')
                with page.expect_response(lambda response: '/api/combinations' in response.url) as saved:
                    page.locator('#save').click()
                assert saved.value.status==200
                recipe=root/'config/cta_prep_binance_futures/prep02_binance-futures_5min.json'
                data=json.loads(recipe.read_text())
                assert data['signal']['max_pos_u']==250 and len(data['factor_rules'])==7
                assert (root/'config/cta_prep_binance_futures/prep02_binance-futures_1min.json').read_bytes()==untouched
                current=api('config')
                external=current['plan']['combinations']['binance-futures_1min']
                external['signal']['max_pos_u']=123
                api('combinations',{'combination':external,'expected_revision':current['revision']})
                page.locator('#max-pos').fill('999')
                with page.expect_response(lambda response: '/api/combinations' in response.url) as conflict:
                    page.locator('#save').click()
                assert conflict.value.status==409
                assert page.locator('#max-pos').input_value()=='999'
                assert json.loads(recipe.read_text())['signal']['max_pos_u']==250
                page.locator('#reload').click()
                page.wait_for_function("document.querySelector('#max-pos').value==='250'")
                original=recipe.read_bytes()
                page.locator('#factor-json').fill('[{"标的":"SOL","nq":"关闭","符号":1,"因子":"factor_049"}]')
                page.locator('#validate').click()
                page.wait_for_function("document.querySelector('#message').classList.contains('err')")
                assert recipe.read_bytes()==original
                page.locator('#strategy-type').select_option('cta-factor-spread')
                page.wait_for_function("document.querySelector('main').getAttribute('aria-busy')==='false' && document.querySelector('#spread-settings').hidden===false")
                assert page.locator('#spread-long').input_value()=='0.3'
                assert page.locator('#spread-short').input_value()=='0.7'
                page.locator('#strategy-type').select_option('cta-prep-binance-futures')
                page.wait_for_function("document.querySelector('main').getAttribute('aria-busy')==='false' && document.querySelector('#strategy-type').value==='cta-prep-binance-futures'")
                page.locator('#strategy-group').select_option('prep02')
                page.locator('[data-interval="15min"]').click()
                rows=json.loads(page.locator('#factor-json').input_value())
                assert sum(len(row['标的']) for row in rows)==33
                assert any(row['因子']=='TD_MT_040' and row['NQ']=='0.2/0.8' for row in rows)
                if args.screenshot_dir:
                    args.screenshot_dir.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(args.screenshot_dir/'desktop.png'), full_page=True)
                page.set_viewport_size({'width':390,'height':844})
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'mobile.png'), full_page=True)
                assert not errors, errors
                browser.close()
                print('Frequency JSON editor passed: grouped JSON, isolated save, conflict, single format and mobile layout')
        finally:
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill(); server.wait()

if __name__=='__main__':
    main()
