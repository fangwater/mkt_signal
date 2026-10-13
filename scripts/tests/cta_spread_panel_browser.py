#!/usr/bin/env python3
"""Read-only browser verification with synthetic, explicitly labelled local data."""
import argparse
import json
import math
import sys
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer
from pathlib import Path
from playwright.sync_api import sync_playwright

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from cta_spread_dashboard import make_handler
from cta_spread_signal_history import SpreadHistory,MINUTE


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary')
    parser.add_argument('--output',type=Path,default=Path('/tmp/cta-spread-panel-review'))
    args=parser.parse_args();args.output.mkdir(parents=True,exist_ok=True)
    now=time.time_ns()//1_000_000;end=now//MINUTE*MINUTE
    config=json.loads((ROOT/'config/cta_spread.json').read_text())
    table=json.loads((ROOT/'config/cta_spread_symbol_factors.json').read_text())
    with tempfile.TemporaryDirectory() as temporary:
        directory=Path(temporary)
        cfg,status,factors=(directory/name for name in ['config.json','status.json','factors.json'])
        cfg.write_text(json.dumps(config));factors.write_text(json.dumps(table))
        status.write_text(json.dumps(dict(updated_ts_us=now*1000,symbol_factors=table)))
        history=SpreadHistory(directory/'history.sqlite3')
        pattern=[.99,.99,.5,.2,.01,.01,.99,.99]
        spreads=[.2,.2,.5,.5,.8,.8,.2,.2]
        for symbol,bindings in table.items():
            candles=[]
            for i,ts in enumerate(range(end-1440*MINUTE,end+1,MINUTE)):
                price=100+math.sin(i/15)*2+i/1500
                candles.append(dict(ts=ts,open=price,close=price+.15,low=price-.3,high=price+.45,closed=ts<end))
            history.save_candles(symbol,candles)
            records=[]
            for i,ts in enumerate(range(end-179*MINUTE,end+1,MINUTE)):
                q=pattern[(i//5)%8];sq=spreads[(i//5)%8]
                inputs={f:dict(ready=True,score=1.,quantile=q if sign>0 else 1-q) for f,sign in bindings.items()}
                record=dict(symbol=symbol,model_ts_ms=ts,inputs=inputs,spread=dict(ready=True,quantile=sq,value=(sq-.5)*.001))
                records.append((symbol,ts,json.dumps(record)))
            with history.lock,history.db:history.db.executemany('INSERT INTO observations VALUES (?,?,?)',records)
        base_handler=make_handler(cfg,status,ROOT/'web/cta_spread/index.html',history,factors)
        class PrefixHandler(base_handler):
            def do_GET(self):
                if not self.path.startswith('/cta/test/'):
                    self.send_error(404);return
                self.path=self.path[len('/cta/test'):]
                super().do_GET()
        server=ThreadingHTTPServer(('127.0.0.1',0),PrefixHandler)
        threading.Thread(target=server.serve_forever,daemon=True).start()
        before=cfg.read_bytes(),status.read_bytes(),factors.read_bytes()
        try:
            with sync_playwright() as playwright:
                launch=dict(headless=True,args=['--no-sandbox'])
                if args.browser_binary:launch['executable_path']=args.browser_binary
                browser=playwright.chromium.launch(**launch)
                page=browser.new_page(viewport=dict(width=1600,height=1150),device_scale_factor=1)
                errors=[];page.on('pageerror',lambda error:errors.append(str(error)))
                page.goto(f'http://127.0.0.1:{server.server_port}/cta/test/signals/')
                page.wait_for_selector('#events [data-event]')
                assert page.locator('.factor').count()==5
                assert page.locator('.symbol').count()==18
                assert '发布关闭' in page.locator('#publishing').inner_text()
                assert '5 / 57' in page.locator('#combinations').inner_text()
                assert page.locator('#live-ready').inner_text()=='0 / 57'
                page.locator('.symbol[data-symbol="ETHUSDT"]').click()
                page.wait_for_function("document.querySelector('#instrument').textContent==='ETH / USDT' && document.querySelector('#events [data-event]')")
                page.locator('[data-hours="1"]').click()
                page.wait_for_function("echarts.getInstanceByDom(document.getElementById('chart')).getOption()?.xAxis?.[0]?.data.length===61")
                option=page.evaluate("echarts.getInstanceByDom(document.getElementById('chart')).getOption()")
                assert option['series'][0]['type']=='candlestick'
                assert option['series'][1]['data'] and option['series'][2]['data']
                assert option['series'][3]['data'] and option['series'][4]['data']
                assert len(option['series'][5]['markLine']['data'])==4
                assert len(option['series'][6]['markLine']['data'])==2
                # Click an actual ECharts marker with its symbol offset applied.
                position=page.evaluate('''() => {const c=echarts.getInstanceByDom(document.getElementById('chart'));
                    const item=c.getOption().series[1].data.at(-1);const p=c.convertToPixel({xAxisIndex:0,yAxisIndex:0},item.value);
                    const r=document.getElementById('chart').getBoundingClientRect();return [r.x+p[0],r.y+p[1]+12];}''')
                page.mouse.click(*position)
                page.wait_for_function("document.querySelector('#inspector-title').textContent==='现货 B · 开仓'")
                page.locator('#events tr').filter(has_text='合约对冲退出').first.click()
                assert '合约' in page.locator('#inspector-title').inner_text()
                # Different bindings/sign must change both chart and symbol universe.
                page.locator('.factor[data-factor="factor_170"]').click()
                page.wait_for_function("document.querySelector('#factor-name').textContent==='factor_170' && document.querySelectorAll('.symbol').length===7")
                assert page.locator('.symbol[data-symbol="ETHUSDT"]').count()==0
                page.wait_for_function("document.querySelector('#sign').textContent.includes('1−q')")
                page.locator('.symbol[data-symbol="LTCUSDT"]').click()
                page.wait_for_function("document.querySelector('#instrument').textContent==='LTC / USDT' && document.querySelector('#events [data-event]')")
                page.locator('#search').fill('LTC');assert page.locator('.symbol').count()==1
                page.locator('#search').fill('')
                page.locator('.factor[data-factor="baseline_141"]').click()
                page.wait_for_function("document.querySelector('#factor-name').textContent==='baseline_141' && document.querySelector('#sign').textContent==='sign +1'")
                assert page.locator('.symbol.active').get_attribute('data-symbol')=='LTCUSDT'
                page.locator('.symbol[data-symbol="ETHUSDT"]').click()
                page.wait_for_function("document.querySelector('#instrument').textContent==='ETH / USDT' && document.querySelector('#events [data-event]')")
                page.evaluate("document.querySelector('#account').textContent='LOCAL TEST FIXTURE · synthetic data'")
                page.screenshot(path=str(args.output/'desktop_fixture.png'),full_page=True)
                page.set_viewport_size(dict(width=390,height=844))
                page.wait_for_timeout(300)
                assert page.evaluate('document.documentElement.scrollWidth<=window.innerWidth')
                page.screenshot(path=str(args.output/'mobile_fixture.png'),full_page=True)
                # Missing candles retain events in the table, without invented chart prices.
                with history.lock,history.db:history.db.execute('DELETE FROM candles')
                page.locator('[data-hours="6"]').click()
                page.wait_for_function("document.querySelector('#chart-state').textContent.includes('事件暂无 K 线')")
                assert page.locator('#events [data-event]').count()>0
                assert page.evaluate("echarts.getInstanceByDom(document.getElementById('chart')).getOption().series.slice(1,5).every(s=>s.data.length===0)")
                assert not errors,errors
                browser.close()
            assert before==(cfg.read_bytes(),status.read_bytes(),factors.read_bytes())
            print(json.dumps(dict(result='passed',strategies=5,bindings=57,js_errors=errors,
                                  checks=['spot_and_hedge_markers','marker_click','reversal','signed_factor_switch','symbol_scope','time_range','proxy_prefix','responsive','missing_candles','read_only']),indent=2))
        finally:server.shutdown();server.server_close();history.close()


if __name__=='__main__':main()
