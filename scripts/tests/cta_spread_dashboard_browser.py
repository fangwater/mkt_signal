#!/usr/bin/env python3
"""Check spread targets and hedged/reversed states on desktop and mobile."""
import argparse
import json
from pathlib import Path
import sys
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer
from playwright.sync_api import sync_playwright
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from cta_spread_dashboard import make_handler


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--browser-binary',required=True)
    args=parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='cta-spread-dashboard-') as temporary:
        root=Path(temporary)
        config=root/'config.json';config.write_bytes((ROOT/'config/cta_spread.json').read_bytes())
        status=root/'status.json'
        status.write_text(json.dumps({'updated_ts_us':time.time_ns()//1000,'symbol_factors':{'ETHUSDT':{'factor_036':1},'SOLUSDT':{'factor_036':-1}},'strategies':{'factor_036':{
            'ETHUSDT':{'score_ready':True,'position_flag':0,'position_qty':0,'cash_qty':2,'perpetual_qty':-2,'entry_notional':0,'position_usdt':0,'signal':{'quantile':.2,'spread_quantile':.5,'spread_value':.001}},
            'SOLUSDT':{'score_ready':True,'position_flag':-1,'position_qty':-3,'cash_qty':2,'perpetual_qty':-5,'entry_notional':200,'position_usdt':-200,'signal':{'quantile':.05,'spread_quantile':.2,'spread_value':-.002}},
        }}}))
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(config,status,ROOT/'web/cta_spread/index.html'))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        try:
            with sync_playwright() as p:
                browser=p.chromium.launch(executable_path=args.browser_binary,headless=True,args=['--no-sandbox'])
                page=browser.new_page(viewport={'width':1440,'height':1000})
                errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                page.goto(f'http://127.0.0.1:{server.server_port}/')
                page.wait_for_function("document.querySelector('#rows').children.length === 2")
                assert '已对冲' in page.locator('#rows').inner_text()
                assert '-5.00000000' in page.locator('#rows').inner_text()
                assert '仅观察' in page.locator('#publishing').inner_text()
                page.locator('#hide-flat').check()
                assert page.locator('#rows tr').count()==2
                for width,height in [(1440,1000),(390,844)]:
                    page.set_viewport_size({'width':width,'height':height})
                    assert not page.evaluate('document.documentElement.scrollWidth>innerWidth')
                assert not errors,errors
                browser.close()
                print(json.dumps({'hedged_cash_visible':True,'reversed_targets_visible':True,'publication_disabled':True,'mobile_desktop_no_overflow':True,'javascript_errors':errors}))
        finally:
            server.shutdown();server.server_close()

if __name__=='__main__':main()
