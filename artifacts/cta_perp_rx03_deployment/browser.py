import json,time
from pathlib import Path
from playwright.sync_api import sync_playwright
root=Path('/home/fanghaizhou/mkt_signal/artifacts/cta_perp_rx03_deployment')
root.mkdir(parents=True,exist_ok=True)
base='http://127.0.0.1:24193/cta/binance-cta-perp-rx03/'
expected=json.loads(Path('/home/fanghaizhou/mkt_signal/config/cta_perp_rx03/cta_perp_symbol_factors.json').read_text())
errors=[]; result={}
with sync_playwright() as p:
 browser=p.chromium.launch(executable_path='/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless=True,args=['--no-sandbox'])
 page=browser.new_page(viewport={'width':1440,'height':1000})
 page.on('pageerror',lambda error: errors.append(str(error)))
 response=page.goto(base+'config/',wait_until='networkidle')
 assert response.status==200
 page.wait_for_function("document.querySelector('#signal-status').textContent === '读取完成'")
 actual=json.loads(page.locator('#factor-json').input_value())
 assert actual==expected
 assert page.locator('#max_pos_u').input_value()=='100'
 result['config']={'symbols':len(actual),'bindings':sum(len(r) for r in actual.values()),'max_pos_u':100}
 page.screenshot(path=str(root/'config.png'),full_page=True)
 response=page.goto(base+'signals/',wait_until='networkidle')
 assert response.status==200
 page.wait_for_selector('[data-symbol="AAVEUSDT"]')
 page.wait_for_function("async () => (await (await fetch('../api/signal-panel')).json()).simulation?.ready === true", polling=3000, timeout=60000)
 page.reload(wait_until='networkidle')
 result['signal_symbols']=page.locator('[data-symbol]').evaluate_all('(nodes)=>nodes.map(n=>n.dataset.symbol)')
 assert set(result['signal_symbols'])==set(expected)
 for symbol in ('AAVEUSDT','NEARUSDT','ETHUSDT'):
  page.locator('[data-symbol="'+symbol+'"]').click()
  page.wait_for_function('(names)=>JSON.stringify(Array.from(document.querySelector("#strategy-filter").options).map(o=>o.value).sort())===JSON.stringify(names.sort())',arg=list(expected[symbol]))
  result[symbol]=page.locator('#strategy-filter option').evaluate_all('(nodes)=>nodes.map(n=>n.value)')
 overview=page.request.get(base+'api/signal-panel').json()['simulation']
 choices=[(stats['closed_trades'],stats['entry_actions'],factor,symbol) for factor,summary in overview['strategy_summaries'].items() for symbol,stats in summary['symbols'].items()]
 closed,entries,factor,symbol=max(choices)
 assert entries>0,'No historical B/S events yet'
 page.locator('[data-symbol="'+symbol+'"]').click()
 page.locator('#strategy-filter').select_option(factor)
 page.locator('[data-hours="24"]').click()
 page.wait_for_selector('#trade-rows tr[data-trade]')
 page.wait_for_function("() => window.echarts.getInstanceByDom(document.querySelector('#chart'))?.getOption().series.filter(s=>s.type==='scatter').some(s=>s.data.length>0)")
 markers=page.evaluate("() => Object.fromEntries(window.echarts.getInstanceByDom(document.querySelector('#chart')).getOption().series.filter(s=>s.type==='scatter').map(s=>[s.name,s.data.length]))")
 page.locator('#trade-rows tr[data-trade]').first.click()
 result['bs_validation']={'symbol':symbol,'factor':factor,'closed_trades':closed,'entries':entries,'markers':markers,'inspector':page.locator('#inspector').inner_text()}
 page.screenshot(path=str(root/'signals.png'),full_page=True)
 page.set_viewport_size({'width':390,'height':844})
 page.screenshot(path=str(root/'signals-mobile.png'),full_page=True)
 result['mobile_overflow']=page.evaluate('document.documentElement.scrollWidth > window.innerWidth')
 assert not result['mobile_overflow']
 result['panel']=page.request.get(base+'api/signal-panel').json()
 result['snapshot']=page.request.get(base+'api/snapshot').json()
 assert result['panel']['trading_enabled'] is False
 assert result['panel']['simulation']['ready'] is True
 assert result['panel']['simulation']['error'] is None
 assert len(result['panel']['simulation']['strategy_summaries'])==5
 assert result['snapshot']['healthy'] is True
 assert result['snapshot']['status']['enabled'] is False
 assert result['snapshot']['status']['publication_error'] is None
 result['browser_errors']=errors
 assert not errors,errors
 browser.close()
(root/'browser_validation.json').write_text(json.dumps(result,indent=2))
print(json.dumps({'ok':True,'config':result['config'],'symbols':result['signal_symbols'],'browser_errors':errors,'mobile_overflow':False},indent=2))
