#!/usr/bin/env python3
"""Exercise the unified editor against disposable catalogs, with no Kafka/accounts."""
import argparse
import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import threading
import urllib.error
import urllib.request

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[2]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary', type=Path, required=True)
    parser.add_argument('--delist-binary', type=Path, default=ROOT/'target/debug/delist_risk_server')
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
        gateway = None
        gateway_thread = None
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
            for worker, history_name in [('cta-prep-binance-futures','cta_prep_binance_futures'),('cta-factor-spread','cta_factor_spread')]:
                current=api('config?worker='+worker)
                plan=current['plan']
                tables=[]
                for combination in plan['combinations'].values():
                    strategy=current['position_strategies'][combination['name']]
                    tables.append({'strategy_name':strategy,'combination_name':combination['name'],
                        'interval':combination['interval'],'enabled':combination.get('signal',{}).get('enabled',False),
                        'targets':{'ETHUSDT':{'qty':1.25,'signal':0}}})
                history=root/'data'/history_name
                history.mkdir(parents=True,exist_ok=True)
                (history/'state.json').write_text(json.dumps({'worker':worker,'updated_ms':int(time.time()*1000),
                    'warming':False,'plan':plan,'positions_error':None,
                    'position_tables':{'tables':tables,'publication_error':None,
                        'manager_publications':[{'manager_url':'http://127.0.0.1:18201/api'}]}}))
            for rx in ('rx01','rx03','rx04'):
                run=root/('binance-cta-perp-'+rx)/'run'
                run.mkdir(parents=True)
                (run/'cta_perp_status.json').write_text('{}')
            environment=dict(os.environ)
            environment.pop('DELIST_CTA_JP_DATABASE_URL',None)
            cta=json.loads(subprocess.check_output([
                str(args.delist_binary),'--cta-mapping-check',
                '--redis-backup-env-root',str(root),'--cta-factor-env',str(root),
                '--cta-spread-factor-env',str(root)
            ],env=environment))
            assert len(cta['factor_processes'])==2
            assert len(cta['strategies'])==13
            assert all(not row['stale'] and not row['errors'] for row in cta['strategies'])
            # SQL bindings are checked separately against the deployed reader;
            # this disposable web fixture has no Manager database.
            cta['managers']=[{'site':'jp','ok':True,'error':None}]
            config_path='/cta/cta-prep-binance-futures/config/'
            class Gateway(BaseHTTPRequestHandler):
                def log_message(self, *unused):
                    pass
                def proxy(self):
                    path=self.path[len(config_path):]
                    body=self.rfile.read(int(self.headers.get('Content-Length','0'))) or None
                    headers={key:self.headers[key] for key in ('Host','Origin','Content-Type','Sec-Fetch-Site') if key in self.headers}
                    request=urllib.request.Request(base+path,data=body,headers=headers,method=self.command)
                    try: response=opener.open(request,timeout=5)
                    except urllib.error.HTTPError as error: response=error
                    payload=response.read()
                    self.send_response(response.status)
                    self.send_header('Content-Type',response.headers.get('Content-Type','application/json'))
                    self.send_header('Content-Length',str(len(payload)))
                    self.end_headers();self.wfile.write(payload)
                    response.close()
                def do_GET(self):
                    if self.path.startswith(config_path): return self.proxy()
                    if self.path in ('/cta/cta-factor-spread/config','/cta/cta-factor-spread/config/'):
                        self.send_response(303);self.send_header('Location',config_path+'?worker=cta-factor-spread');self.end_headers();return
                    if self.path=='/delist/': payload=(ROOT/'web/delist_risk/index.html').read_bytes();kind='text/html; charset=utf-8'
                    else:
                        values={'/delist/cta-strategies':cta,'/delist/accounts':{'accounts':[],'summary':{},'as_of_ms':int(time.time()*1000)},'/delist/flatten-candidates':{'items':[]},'/delist/status':{'sources':[]},'/delist/exposures':{'accounts':[],'errors':[],'checked_ms':int(time.time()*1000),'settings':{'interval_secs':10,'max_age_secs':90,'persistence_secs':30,'asset_limit_usdt':100,'account_limit_usdt':500}}}
                        if self.path not in values: self.send_error(404);return
                        payload=json.dumps(values[self.path]).encode();kind='application/json'
                    self.send_response(200);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(payload)));self.end_headers();self.wfile.write(payload)
                def do_POST(self):
                    if self.path.startswith(config_path): return self.proxy()
                    self.send_error(405)
                do_DELETE=do_POST
            gateway=ThreadingHTTPServer(('127.0.0.1',0),Gateway)
            gateway_thread=threading.Thread(target=gateway.serve_forever,daemon=True)
            gateway_thread.start()
            gateway_base=f'http://127.0.0.1:{gateway.server_port}'
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(executable_path=str(args.browser_binary), headless=True, args=['--no-sandbox','--disable-dev-shm-usage'])
                page = browser.new_page(viewport={'width':1440,'height':1000})
                errors = []
                page.on('pageerror', lambda error: errors.append(str(error)))
                page.on('dialog', lambda dialog: dialog.accept())
                page.goto(gateway_base+'/delist/')
                page.wait_for_selector('#cta-strategies .cta-table')
                assert page.locator('#cta-config-entries a').count()==2
                assert page.locator('#cta-strategies tr[data-strategy]').count()==13
                assert page.locator('#cta-strategies .account-slug').filter(has_text='binance-cta-perp-').count()==0
                assert '异常' not in page.locator('#system-state').inner_text()
                if args.screenshot_dir:
                    args.screenshot_dir.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(args.screenshot_dir/'delist-desktop.png'), full_page=True)
                page.get_by_role('link',name='CTA Spread 统一配置').click()
                page.wait_for_function("document.querySelector('main').getAttribute('aria-busy')==='false' && document.querySelector('#strategy-type').value==='cta-factor-spread'")
                assert page.locator('#current-frequency').inner_text()=='1min'
                assert len(json.loads(page.locator('#factor-json').input_value()))==34
                assert '34 条规则' in page.locator('#factor-count').inner_text()
                page.get_by_role('link',name='返回 Delist').click()
                page.wait_for_selector('#cta-strategies .cta-table')
                page.get_by_role('link',name='CTA Prep 统一配置').click()
                page.wait_for_function("document.querySelector('main').getAttribute('aria-busy')==='false' && document.querySelector('#factor-json').value!==''")
                assert page.locator('#parameters,#position-targets,#rows,#import-json').count()==0
                assert page.locator('textarea').count()==2
                page.locator('#strategy-group').select_option('prep02')
                page.locator('#symbol-group').select_option('top-5')
                page.locator('[data-interval="5min"]').click()
                rows=json.loads(page.locator('#factor-json').input_value())
                assert all(set(row)=={'标的','nq','符号','因子'} and isinstance(row['标的'],str) for row in rows)
                assert len(rows)==2
                untouched=(root/'config/cta_prep_binance_futures/top-5_prep02_binance-futures_1min.json').read_bytes()
                page.locator('#max-pos').fill('250')
                with page.expect_response(lambda response: '/api/combinations' in response.url) as saved:
                    page.locator('#save').click()
                assert saved.value.status==200
                recipe=root/'config/cta_prep_binance_futures/top-5_prep02_binance-futures_5min.json'
                data=json.loads(recipe.read_text())
                assert data['signal']['max_pos_u']==250 and len(data['factor_rules'])==2
                assert (root/'config/cta_prep_binance_futures/top-5_prep02_binance-futures_1min.json').read_bytes()==untouched
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
                page.locator('#factor-json').fill('[{"频率":"5m","NQ":"关闭","符号":1,"因子":"factor_049","标的":["SOL"]}]')
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
                page.locator('#symbol-group').select_option('top-5')
                page.locator('[data-interval="15min"]').click()
                rows=json.loads(page.locator('#factor-json').input_value())
                assert len(rows)==7
                assert any(row['因子']=='TD_MT_040' and row['nq']=='0.2/0.8' for row in rows)
                # Overlapping pools retain separate parameters and rows.
                page.locator('#symbol-group').select_option('top-20')
                assert len(json.loads(page.locator('#factor-json').input_value()))==10
                assert page.locator('#max-pos').input_value()=='200'
                page.locator('[data-interval="5min"]').click()
                assert json.loads(page.locator('#factor-json').input_value())==[]
                with page.expect_response(lambda response: '/api/combinations' in response.url) as saved:
                    page.locator('#save').click()
                assert saved.value.status==200
                empty_recipe=root/'config/cta_prep_binance_futures/top-20_prep02_binance-futures_5min.json'
                assert json.loads(empty_recipe.read_text())['factor_rules']==[]
                page.locator('[data-interval="15min"]').click()
                page.locator('#symbol-group').select_option('last-20')
                page.locator('#factor-json').fill('[{"标的":"BTC","nq":"关闭","符号":1,"因子":"factor_036"}]')
                page.locator('#validate').click()
                page.wait_for_function("document.querySelector('#message').textContent.includes('不属于券池')")
                page.locator('#reload').click()
                page.wait_for_function("document.querySelector('main').getAttribute('aria-busy')==='false'")
                # A new Spread pool starts empty and disabled, then saves independently.
                page.locator('#strategy-type').select_option('cta-factor-spread')
                page.wait_for_function("document.querySelector('main').getAttribute('aria-busy')==='false' && document.querySelector('#strategy-type').value==='cta-factor-spread'")
                page.locator('#symbol-group').select_option('top-5')
                assert json.loads(page.locator('#factor-json').input_value())==[]
                assert page.locator('#enabled').input_value()=='false'
                page.locator('#factor-json').fill('[{"标的":"ETH","nq":"关闭","符号":1,"因子":"factor_036"}]')
                with page.expect_response(lambda response: '/api/combinations' in response.url) as saved:
                    page.locator('#save').click()
                assert saved.value.status==200
                created=root/'config/cta_factor_spread/top-5_spread01_okx_bn_1min.json'
                assert json.loads(created.read_text())['symbol_group']=='top-5'
                page.locator('#symbol-group').select_option('')
                assert len(json.loads(page.locator('#factor-json').input_value()))==34
                # Group JSON edits use the same revision guard, with preview before save.
                page.locator('details summary').click()
                definitions=json.loads(page.locator('#symbol-groups-json').input_value())
                definitions['custom']=['BTCUSDT']
                page.locator('#symbol-groups-json').fill(json.dumps(definitions))
                groups_file=root/'config/cta_factor_spread/symbol_groups.json'
                old_groups=groups_file.read_bytes()
                with page.expect_response(lambda response: '/api/symbol-groups/preview' in response.url) as preview:
                    page.locator('#validate-groups').click()
                assert preview.value.status==200 and groups_file.read_bytes()==old_groups
                with page.expect_response(lambda response: response.url.split('?')[0].endswith('/api/symbol-groups')) as saved:
                    page.locator('#save-groups').click()
                assert saved.value.status==200 and json.loads(groups_file.read_text())['custom']==['BTCUSDT']
                assert 'custom' not in json.loads((root/'config/cta_prep_binance_futures/symbol_groups.json').read_text())
                page.locator('#symbol-group').select_option('top-5')
                if args.screenshot_dir:
                    args.screenshot_dir.mkdir(parents=True, exist_ok=True)
                    page.screenshot(path=str(args.screenshot_dir/'desktop.png'), full_page=True)
                page.set_viewport_size({'width':390,'height':844})
                assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
                if args.screenshot_dir:
                    page.screenshot(path=str(args.screenshot_dir/'mobile.png'), full_page=True)
                assert not errors, errors
                browser.close()
                print('CTA symbol groups passed: Delist navigation, pool isolation, membership checks, new Spread scope, JSON definitions, save conflicts and mobile layout')
        finally:
            if gateway:
                gateway.shutdown();gateway.server_close()
            if gateway_thread: gateway_thread.join(timeout=3)
            server.terminate()
            try:
                server.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server.kill(); server.wait()

if __name__=='__main__':
    main()
