#!/usr/bin/env python3
"""Browser verification with explicitly labelled local factor and candle fixtures.

Requires Playwright/Chromium. No Redis connection or live trading operation.
"""
from __future__ import annotations
import argparse
import json
import math
import pathlib
import sys
import tempfile
import threading
import time
from http.server import ThreadingHTTPServer
from playwright.sync_api import sync_playwright

ROOT=pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts'))
from cta_perp_dashboard import make_handler
from cta_perp_position_replay import PositionReplay
from cta_perp_signal_history import SignalHistoryStore,MINUTE_MS,normalize_kline


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--browser-binary',type=pathlib.Path)
    args=parser.parse_args()
    now=time.time_ns()//1_000_000;end=now//MINUTE_MS*MINUTE_MS
    first=end-60*MINUTE_MS
    config=json.loads((ROOT/'config/cta_perp.json').read_text())
    config['enabled']=False
    table=json.loads((ROOT/'config/cta_perp_symbol_factors.json').read_text())
    table['FILUSDT']={'factor_036':1,'factor_051':-1,'factor_170':1,'TD_TI_002':1,'baseline_004':-1}
    table['ETHUSDT']={'baseline_004':-1}
    with tempfile.TemporaryDirectory(prefix='cta-factor-panel-browser-') as temp:
        directory=pathlib.Path(temp)
        for name,data in [('config.json',config),('status.json',dict(enabled=False,updated_ts_us=now*1000,journal_healthy=True,
                  symbol_factors=table,max_pos_u=config["max_pos_u"])),
                  ('replay.json',dict(start_ms=first,initial_equity_usdt=10000,taker_fee_rate=0))]:
            (directory/name).write_text(json.dumps(data))
        store=SignalHistoryStore(directory/'history.sqlite3')
        pattern=[.99,.99,.99,.2,.01,.01,.01,.8]
        for symbol,bindings in table.items():
            candles=[]
            for index,ts in enumerate(range(end-1440*MINUTE_MS,end+1,MINUTE_MS)):
                price=100+math.sin(index/20)*2+index/1000
                candles.append(normalize_kline([ts,str(price),str(price+1),str(price-1),str(price+.3),'20',
                  ts+MINUTE_MS-1,'2000',10,'10','1000','0'],now))
            store.save_klines(symbol,candles,now)
            rows=[]
            for i in range(60):
                ts=first+i*MINUTE_MS
                inputs={}
                for index,(factor,weight) in enumerate(bindings.items()):
                    q=pattern[(i+index*2)%8]
                    if factor=='factor_036' and i>=57:q=.99
                    raw=q if weight>0 else 1-q
                    inputs[factor]=dict(score=(raw-.5)*3,quantile=raw,ready=True)
                observation=dict(symbol=symbol,model_ts_ms=ts,inputs=inputs,nq=dict(ready=True,long_value=.02,short_value=-.03,
                  long_quantile=.49 if i%16==8 else .8,short_quantile=.2,execution_ts_ms=ts+1,execution_bid=100+i/20,execution_ask=100.1+i/20))
                rows.append((symbol,ts,json.dumps(observation)))
            with store.lock,store.db:
                store.db.executemany('INSERT INTO factor_observations VALUES (?,?,?)',rows)
        replay=PositionReplay(store,directory/'config.json',directory/'status.json',directory/'replay.json')
        replay.refresh()
        assert replay.error is None,replay.error
        for record in replay.result['records']:record['snapshot']['demo']=True
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(directory/'config.json',directory/'status.json',directory/'execution.json',
          ROOT/'web/cta_perp/index.html',store,targets=lambda:sorted(table),replay=replay))
        threading.Thread(target=server.serve_forever,daemon=True).start()
        try:
            with sync_playwright() as playwright:
                launch=dict(headless=True,args=['--no-sandbox'])
                if args.browser_binary:launch['executable_path']=str(args.browser_binary)
                browser=playwright.chromium.launch(**launch)
                page=browser.new_page(viewport=dict(width=1600,height=1050),device_scale_factor=1)
                errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                page.goto(f'http://127.0.0.1:{server.server_port}/signals/')
                page.wait_for_selector('#trade-rows tr')
                page.locator('.symbol[data-symbol="FILUSDT"]').click()
                page.wait_for_function("() => document.querySelector('#factor-name').textContent==='factor_036' && document.querySelectorAll('#trade-rows tr').length>0")
                assert page.locator('.symbol').count()==16
                assert page.locator('#strategy-filter option').count()==5
                assert page.locator('#strategy-filter option[value=""]').count()==0
                assert page.locator('#demo').is_visible()
                assert '实盘关闭' in page.locator('#service-state').inner_text()
                assert 'factor_036'==page.locator('#strategy-filter').input_value()
                page.locator('[data-hours="1"]').click()
                chart=lambda expression:page.evaluate("(() => {const chart=echarts.getInstanceByDom(document.getElementById('chart'));const option=chart.getOption();return "+expression+";})()")
                assert chart('option.series[0].type')=='candlestick'
                assert chart('option.series[1].data.every(p=>p.record.snapshot.strategy_name==="factor_036")')
                assert chart('option.series[1].data.length+option.series[2].data.length+option.series[3].data.length') < 60
                assert chart('option.series[0].markLine.data.length')>0
                assert chart('option.series[5].name')=='BUY_NQ 分位'
                assert '待平仓' in page.locator('#trade-rows tr').first.inner_text()
                page.locator('#trade-rows tr').nth(1).click()
                assert '已闭合' in page.locator('.trade-detail').inner_text()
                assert page.locator('.nq-state[data-mode="passed"]').count()==1
                # Click an actual plotted opening symbol to exercise ECharts hit testing.
                point=chart('chart.convertToPixel({seriesIndex:1},option.series[1].data.at(-1).value)')
                box=page.locator('#chart').bounding_box()
                page.mouse.click(box['x']+point[0],box['y']+point[1])
                page.wait_for_function("() => document.querySelector('#record-state').textContent==='B 开多'")
                assert '尚未平仓' in page.locator('.trade-detail').inner_text()
                assert '分位 80% ≥ 阈值 50%' in page.locator('.nq-row[data-side="long"]').inner_text()
                page.locator('.nq-method summary').click()
                assert '720' in page.locator('.nq-method').inner_text()
                page.locator('.observations > summary').click()
                page.locator('.nq-timeline[data-mode="blocked"]').first.click()
                assert page.locator('.nq-state[data-mode="blocked"]').inner_text()=='拦截'
                assert '分位 49% ≥ 阈值 50%' in page.locator('.nq-row[data-side="long"]').inner_text()
                page.locator('.observations > summary').click()
                page.locator('#factor-list [data-factor="factor_051"]').click()
                page.wait_for_function("() => document.querySelector('#factor-name').textContent==='factor_051' && document.querySelectorAll('#trade-rows tr').length>0")
                assert chart('option.series.slice(1,4).flatMap(s=>s.data).every(p=>p.record.snapshot.strategy_name==="factor_051")')
                page.locator('#strategy-filter').select_option('factor_036')
                page.wait_for_function("() => document.querySelector('#factor-name').textContent==='factor_036' && document.querySelectorAll('#trade-rows tr').length>0")
                page.locator('#trade-rows tr').first.click()
                page.screenshot(path=str(ROOT/'artifacts/cta_perp_factor_pairs_preview.png'),full_page=True)
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                page.set_viewport_size(dict(width=390,height=844))
                page.screenshot(path=str(ROOT/'artifacts/cta_perp_factor_pairs_preview_mobile.png'),full_page=True)
                assert page.evaluate('document.documentElement.scrollWidth<=innerWidth')
                page.locator('.symbol[data-symbol="ETHUSDT"]').click()
                page.wait_for_function("() => document.querySelector('#factor-name').textContent==='baseline_004' && document.querySelector('#strategy-filter').value==='baseline_004'")
                assert page.locator('#strategy-filter option').count()==1
                assert not errors,errors
                browser.close()
                report=dict(symbols=16,default_factor='factor_036',factor_isolation=True,paired_connectors=True,
                  repeated_signals_hidden=True,unclosed_trade_visible=True,nq_retained=True,nq_blocked_visible=True,
                  canvas_click=True,desktop_mobile_no_overflow=True,javascript_errors=errors)
                (ROOT/'artifacts/cta_perp_factor_pairs_preview_validation.json').write_text(json.dumps(report,indent=2)+'\n')
                print(json.dumps(report))
        finally:
            server.shutdown();server.server_close();store.close()

if __name__=='__main__':main()
