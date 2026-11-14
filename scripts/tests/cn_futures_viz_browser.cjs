// SDK-free frontend contract checks. Run with PLAYWRIGHT_MODULE pointing at Playwright.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { chromium } = require(process.env.PLAYWRIGHT_MODULE || 'playwright');

(async () => {
  const browser = await chromium.launch({headless:true, ...(process.env.CHROMIUM_PATH ? {executablePath:process.env.CHROMIUM_PATH}:{}), args:['--no-sandbox']});
  try {
    const page = await browser.newPage({viewport:{width:1440,height:1000}});
    const errors=[];
    page.on('pageerror', error=>errors.push(error.message));
    const requests=[];
    page.on('request', request=>requests.push(request.url()));
    const html=fs.readFileSync(path.join(__dirname,'../../web/cn_futures_viz/index.html'),'utf8').replace('__CN_FUTURES_WS_PATH__',JSON.stringify('/observe/live'));
    await page.route('http://cn-viz.test/',route=>route.fulfill({contentType:'text/html',body:html}));
    await page.addInitScript(()=>{
      window.__sockets=[];
      window.WebSocket=class {
        constructor(url){this.url=url;window.__sockets.push(this);setTimeout(()=>this.onopen?.(),0);}
        close(){this.onclose?.();}
      };
      window.__push=frame=>window.__sockets.at(-1).onmessage({data:JSON.stringify(frame)});
    });
    await page.goto('http://cn-viz.test/');
    assert.equal(await page.evaluate(()=>window.__sockets[0].url),'ws://cn-viz.test/observe/live');
    const row=(venue,symbol,side,today,yesterday=0,frozen_today=0)=>({venue,symbol,side,position:{today,yesterday,frozen_today,frozen_yesterday:0}});
    const frame={type:'cn_futures_state',source_id:'paper',label:'易达 paper',account:'paper-observe',adapter:'yida',namespace:'test',stale_after_ms:10000,error:null,
      entry:{publisher_epoch:'18446744073709551615',sequence:'1',observed_us:Date.now()*1000,adapter:'yida',runtime_status:'running',position_ready:true,connected:true,ready:true,transport_ready:true,can_trade:false,trading_day:'20261112',reason:'',available_local_budget:1234567.89,positions:[
        row('SHFE','cu2612','buy',2,3,1),row('SHFE','cu2612','sell',1),row('SHFE','cu2612','sell',0),
      ]}};
    // Native contract has one row per side: keep a distinct zero contract in this fixture.
    frame.entry.positions[2].symbol='ag2612';
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#budget').innerText(),'¥ 1,234,567.89');
    assert.equal(await page.locator('#long-lots').innerText(),'5');
    assert.equal(await page.locator('#short-lots').innerText(),'1');
    assert.equal(await page.locator('#frozen-lots').innerText(),'1');
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    assert.match(await page.locator('#positions').innerText(),/沪铜 · 2026年12月/);
    await page.locator('[data-group]').first().click();
    assert.equal(await page.locator('#positions .detail-row').count(),2);
    await page.locator('#symbol').fill('沪铜');
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    await page.locator('#symbol').fill('CU2612');
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    await page.locator('#symbol').fill('');
    await page.locator('#show-zero').check();
    assert.equal(await page.locator('#positions .aggregate-row').count(),2);
    await page.locator('[data-position-view="side"]').click();
    assert.equal(await page.locator('#positions tr[data-symbol]').count(),3);
    await page.locator('#show-zero').uncheck();
    assert.equal(await page.locator('#positions tr[data-symbol]').count(),2);
    // Multi-venue names, CZCE decade rollover from native trading day, unknown fallback.
    frame.entry.positions=[row('CZCE','SR701','buy',1),row('CZCE','TA912','buy',2),row('CFFEX','IF2612','sell',3),row('INE','sc2701','buy',4),row('GFEX','lc2612','buy',5),row('DCE','m2701','buy',6),row('SHFE','unknown2612','buy',7),row('INE','sc2612C780','buy',1),row('DCE','m2701-P-3000','sell',1),row('CZCE','SR701C6000','buy',1),row('CFFEX','IO2612-C-4000','buy',1)];
    await page.evaluate(f=>window.__push(f),frame);
    const text=await page.locator('#positions').innerText();
    for(const expected of ['白糖 · 2027年01月','PTA · 2029年12月','沪深300股指 · 2026年12月','原油 · 2027年01月','碳酸锂 · 2026年12月','豆粕 · 2027年01月','unknown2612','原油 · 2026年12月 · 看涨 · 行权价 780','豆粕 · 2027年01月 · 看跌 · 行权价 3000','白糖 · 2027年01月 · 看涨 · 行权价 6000','沪深300股指 · 2026年12月 · 看涨 · 行权价 4000']) assert.ok(text.includes(expected),expected);
    await page.locator('#venue').selectOption('CZCE');
    assert.equal(await page.locator('#positions tr[data-symbol]').count(),3);
    await page.locator('#venue').selectOption('');
    // Initial recovery and flat are distinct, even across a publisher restart.
    frame.entry.positions=[];frame.entry.position_ready=false;frame.entry.publisher_epoch='2';
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#long-lots').innerText(),'--');
    assert.match(await page.locator('#positions').innerText(),/等待柜台完成持仓恢复/);
    frame.entry.position_ready=true;
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#long-lots').innerText(),'0');
    assert.match(await page.locator('#positions').innerText(),/当前无持仓/);
    frame.entry.positions=[row('SHFE','cu2612','buy',2)];frame.entry.observed_us=(Date.now()-20000)*1000;
    await page.evaluate(f=>window.__push(f),frame);
    assert.match(await page.locator('#source-updates').innerText(),/数据延迟/);
    assert.match(await page.locator('#positions').innerText(),/沪铜/);
    await page.locator('#refresh').click();
    assert.equal(await page.evaluate(()=>window.__sockets.length),2);
    await page.setViewportSize({width:390,height:844});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.ok((await page.locator('#show-zero').boundingBox()).width<=20);
    assert.equal(await page.locator('[data-activity]').count(),0);
    assert.deepEqual(errors,[]);
    assert.ok(requests.every(url=>!url.includes('/snapshot')&&!url.includes('/healthz')));
    console.log('Passed: WS-only init, native lots/yuan, names/expiry/search, grouped/side/zero, recovery/flat/stale, reconnect, mobile.');
  } finally { await browser.close(); }
})().catch(error=>{console.error(error);process.exitCode=1;});
