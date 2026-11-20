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
    const html=fs.readFileSync(path.join(__dirname,'../../web/cn_futures_viz/index.html'),'utf8')
      .replace('__CN_FUTURES_WS_PATH__',JSON.stringify('/observe/live'))
      .replace('__CN_FUTURES_STRATEGY_SOURCES__',JSON.stringify(['ctp']));
    let strategyStatus=401;
    let observation=null;
    const strategy={
      'SHFE:rb2701': {target_lots:1,attributed_lots:0,strategies:[
        {name:'long-factor',signal_target_lots:3,target_lots:3,attributed_lots:2},
        {name:'short-factor',signal_target_lots:-2,target_lots:-2,attributed_lots:-2},
        {name:'flat-factor',signal_target_lots:0,target_lots:0,attributed_lots:0},
      ]},
      'SHFE:sn2701': {target_lots:0,attributed_lots:0,strategies:[
        {name:'reset-factor',signal_target_lots:0,target_lots:0,attributed_lots:0},
      ]},
      'CZCE:SA701': {target_lots:-1,attributed_lots:0,
        strategies:[{name:'cn-futures-01-sa-1h',signal_target_lots:-1,target_lots:-1,attributed_lots:0}],
        factor_signals:{market:'CZCE:SA701',decision_ms:Date.now()-60000,rules:[
          {factor:'baseline_193',target_lots:1},{factor:'td_pt_006',target_lots:-1},{factor:'td_vi_023',target_lots:-1},
          ...Array.from({length:7},(_,i)=>({factor:`sa-factor-${i}`,target_lots:0})),
        ]},
      },
    };
    await page.route(url=>url.origin==='http://cn-viz.test'&&url.pathname==='/',route=>route.fulfill({contentType:'text/html',body:html}));
    await page.route('**/strategy-positions/ctp',route=>route.fulfill({status:strategyStatus,json:strategy}));
    await page.route('**/account-observation/ctp',route=>route.fulfill({status:observation?200:401,json:observation}));
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
    const mark=(symbol,kind,multiple,price)=>({venue:'SHFE',symbol,kind,contract_multiple:multiple,last_price:price,bid_price:null,ask_price:null,bid_lots:0,ask_lots:0,trading_day:20261113,native_timestamp:100,last_trade_timestamp:100,native_time:'21:00:01.500',last_trade_time:'21:00:01.500',received_us:Date.now()*1000,initial:false});
    const frame={type:'cn_futures_state',source_id:'paper',label:'易达 paper',account:'paper-observe',adapter:'yida',namespace:'test',stale_after_ms:10000,error:null,
      entry:{publisher_epoch:'18446744073709551615',sequence:'1',observed_us:Date.now()*1000,adapter:'yida',runtime_status:'running',position_ready:true,connected:true,ready:true,transport_ready:true,can_trade:false,trading_day:'20261112',reason:'',available_local_budget:1234567.89,marks:[mark('cu2612','future',5,80000)],l1_connected:true,l1_dropped:'0',positions:[
        row('SHFE','cu2612','buy',2,3,1),row('SHFE','cu2612','sell',1),row('SHFE','cu2612','sell',0),
      ]}};
    // Native contract has one row per side: keep a distinct zero contract in this fixture.
    frame.entry.positions[2].symbol='ag2612';
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#budget').count(),0);
    assert.equal(await page.locator('.risk-band .metric:visible').count(),3);
    assert.equal(await page.locator('#futures-amount').innerText(),'¥ 1,600,000.00');
    assert.equal(await page.locator('#options-metric').isVisible(),false);
    assert.equal(await page.locator('#valuation-coverage').innerText(),'1 / 1');
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    assert.doesNotMatch(await page.locator('#positions .aggregate-row').innerText(),/历史|权利金|名义金额/);
    assert.match(await page.locator('#positions').innerText(),/沪铜 · 2026年12月/);
    await page.locator('[data-group]').first().click();
    assert.equal(await page.getByText('今仓 · 手',{exact:true}).count(),0);
    await page.getByRole('button',{name:'柜台明细'}).click();
    assert.equal(await page.locator('#positions .position-detail tbody tr').count(),2);
    assert.deepEqual(await page.locator('.position-detail th').allTextContents(),['方向','持仓 · 手','今仓 · 手','昨仓 · 手']);
    assert.deepEqual(await page.locator('.position-detail tbody tr').first().locator('td').allTextContents(),['多头','5','2','3']);
    await page.locator('#symbol').fill('沪铜');
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    await page.locator('#symbol').fill('CU2612');
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    await page.locator('#symbol').fill('');
    await page.locator('#show-zero').check();
    assert.equal(await page.locator('#positions .aggregate-row').count(),2);
    assert.equal(await page.locator('#positions tr[data-symbol]').count(),2);
    await page.locator('#show-zero').uncheck();
    assert.equal(await page.locator('#positions tr[data-symbol]').count(),1);
    // Futures notional and option premium use native multipliers and signed net lots.
    frame.entry.positions=[row('SHFE','cu2612','buy',2,3),row('SHFE','cu2612','sell',1),row('SHFE','cu2612C106000','sell',2)];
    frame.entry.marks.push(mark('cu2612C106000','option',5,20));
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#futures-amount').innerText(),'¥ 1,600,000.00');
    assert.equal(await page.locator('#options-amount').innerText(),'¥ -200.00');
    frame.entry.marks[1].last_price=null;frame.entry.marks[1].bid_price=19;frame.entry.marks[1].ask_price=21;
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#options-amount').innerText(),'--');
    assert.equal(await page.locator('#valuation-coverage').innerText(),'1 / 2');
    frame.entry.marks[1].last_price=20;frame.entry.marks[1].contract_multiple=0;
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#options-amount').innerText(),'--');
    frame.entry.marks[1].contract_multiple=5;frame.entry.marks[1].initial=true;
    await page.evaluate(f=>window.__push(f),frame);
    assert.match(await page.locator('#quote-status').innerText(),/历史行情/);
    assert.match(await page.locator('#positions .aggregate-row[data-symbol="cu2612C106000"] .net-amount span').getAttribute('title'),/历史行情.*初始快照/);
    await page.locator('#positions .aggregate-row[data-symbol="cu2612C106000"] [data-group]').click();
    await page.locator('#positions .aggregate-row[data-symbol="cu2612C106000"] + .detail-row [data-broker]').click();
    assert.match(await page.locator('.valuation-basis').last().innerText(),/净权利金市值.*合约乘数 5.*初始快照/);
    await page.locator('#positions .aggregate-row[data-symbol="cu2612C106000"] [data-group]').click();
    frame.entry.marks[1].initial=false;frame.entry.marks[1].received_us=(Date.now()-20000)*1000;frame.entry.observed_us=Date.now()*1000;
    await page.evaluate(f=>window.__push(f),frame);
    assert.match(await page.locator('#quote-status').innerText(),/历史行情/);
    assert.match(await page.locator('#positions .aggregate-row[data-symbol="cu2612C106000"] .net-amount span').getAttribute('title'),/行情停更/);
    // Fresh receipt timestamps do not make an older native quote current.
    frame.entry.marks[1].received_us=Date.now()*1000;frame.entry.marks[1].trading_day=20200101;
    await page.evaluate(f=>window.__push(f),frame);
    assert.match(await page.locator('#quote-status').innerText(),/历史行情.*20200101/);
    const optionsOnly=structuredClone(frame);
    optionsOnly.entry.positions=[row('SHFE','cu2612C106000','sell',2)];
    await page.evaluate(f=>window.__push(f),optionsOnly);
    assert.equal(await page.locator('#futures-metric').isVisible(),false);
    assert.equal(await page.locator('#options-metric').isVisible(),true);
    assert.equal(await page.locator('#options-amount').innerText(),'¥ -200.00');
    // Multi-venue names, CZCE decade rollover from native trading day, unknown fallback.
    frame.entry.positions=[row('CZCE','SR701','buy',1),row('CZCE','TA912','buy',2),row('CFFEX','IF2612','sell',3),row('INE','sc2701','buy',4),row('GFEX','lc2612','buy',5),row('DCE','m2701','buy',6),row('SHFE','unknown2612','buy',7),row('INE','sc2612C780','buy',1),row('DCE','m2701-P-3000','sell',1),row('CZCE','SR701C6000','buy',1),row('CFFEX','IO2612-C-4000','buy',1)];
    await page.evaluate(f=>window.__push(f),frame);
    const text=await page.locator('#positions').innerText();
    for(const expected of ['白糖 · 2027年01月','PTA · 2029年12月','沪深300股指 · 2026年12月','原油 · 2027年01月','碳酸锂 · 2026年12月','豆粕 · 2027年01月','unknown2612','原油 · 2026年12月 · 看涨 · 行权价 780','豆粕 · 2027年01月 · 看跌 · 行权价 3000','白糖 · 2027年01月 · 看涨 · 行权价 6000','沪深300股指 · 2026年12月 · 看涨 · 行权价 4000']) assert.ok(text.includes(expected),expected);
    assert.equal(await page.getByRole('combobox',{name:'交易所筛选'}).count(),0);
    assert.equal(await page.getByRole('columnheader',{name:'交易所',exact:true}).count(),0);
    // Initial recovery and flat are distinct, even across a publisher restart.
    frame.entry.positions=[];frame.entry.position_ready=false;frame.entry.publisher_epoch='2';
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#futures-amount').innerText(),'--');
    assert.match(await page.locator('#positions').innerText(),/等待柜台完成持仓恢复/);
    frame.entry.position_ready=true;
    await page.evaluate(f=>window.__push(f),frame);
    assert.equal(await page.locator('#futures-amount').innerText(),'¥ 0.00');
    assert.match(await page.locator('#positions').innerText(),/当前无持仓/);
    frame.entry.positions=[row('SHFE','cu2612','buy',2)];frame.entry.observed_us=(Date.now()-20000)*1000;
    await page.evaluate(f=>window.__push(f),frame);
    assert.match(await page.locator('#source-updates').innerText(),/数据延迟/);
    assert.match(await page.locator('#positions').innerText(),/沪铜/);
    await page.locator('#refresh').click();
    assert.equal(await page.evaluate(()=>window.__sockets.length),2);
    // CTP funds come from account queries, never the local execution budget.
    const ctp=structuredClone(frame);
    ctp.source_id='ctp';ctp.adapter='ctp';ctp.entry.adapter='ctp';ctp.entry.positions=[];ctp.entry.marks=[];
    ctp.entry.position_ready=true;ctp.entry.connected=true;ctp.entry.transport_ready=true;ctp.entry.ready=false;
    ctp.entry.reason='settlement not confirmed; read-only queries remain available';
    ctp.entry.observed_us=Date.now()*1000;
    ctp.entry.account={observed_us:(Date.now()-5000)*1000,currency:'CNY',balance:499999.72,available:499999.72,withdraw_quota:499899.72,curr_margin:0,frozen_margin:1,frozen_cash:2,frozen_commission:0.5,position_profit:-12.3,close_profit:13.2,commission:5.5};
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('#account-balance').innerText(),'¥ 499,999.72');
    assert.equal(await page.locator('#account-withdrawable').innerText(),'¥ 499,899.72');
    assert.equal(await page.getByText('冻结资金合计',{exact:true}).count(),0);
    assert.equal(await page.locator('#account-position-profit').innerText(),'¥ -12.30');
    assert.equal(await page.locator('#account-funds .metric').count(),7);
    assert.equal(await page.locator('#contract-count').innerText(),'0');
    assert.match(await page.locator('#positions').innerText(),/当前无持仓/);
    assert.match(await page.locator('#source-updates').innerText(),/已同步/);
    assert.equal(await page.locator('#mode').innerText(),'只读观察');
    assert.equal(await page.locator('#source-tabs [role="tab"]').count(),2);
    ctp.entry.account.observed_us=(Date.now()-60000)*1000;ctp.entry.observed_us=Date.now()*1000;
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('#funds-status').innerText(),/保留上次查询/);
    assert.equal(await page.locator('#account-balance').innerText(),'¥ 499,999.72');
    const account=ctp.entry.account;ctp.entry.account=null;
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('#account-balance').innerText(),'--');
    await page.locator('[data-source="paper"]').click();
    assert.equal(await page.locator('#account-balance').innerText(),'--');
    ctp.entry.account={...account,observed_us:Date.now()*1000};
    await page.evaluate(f=>window.__push(f),ctp);
    // An explicit source choice is retained when another counter publishes.
    assert.equal(await page.locator('#account-balance').innerText(),'--');
    await page.locator('[data-source="ctp"]').click();
    assert.equal(await page.locator('#account-balance').innerText(),'¥ 499,999.72');
    assert.equal(new URL(page.url()).searchParams.get('source'),'ctp');
    await page.getByRole('link',{name:'登录 Manager 查看策略仓位'}).waitFor();
    ctp.entry.positions=[row('SHFE','rb2701','buy',1)];
    ctp.entry.marks=[{...mark('rb2701','future',10,null),trading_day:20261119}];
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('.detail-row').getByRole('link',{name:'登录 Manager 查看策略仓位'}).count(),1);
    assert.equal(await page.locator('#futures-amount').innerText(),'--');
    assert.equal(await page.locator('#options-metric').isVisible(),false);
    ctp.entry.marks[0]={...ctp.entry.marks[0],bid_price:2982,ask_price:2983,bid_lots:2,ask_lots:3,initial:true,native_time:'14:59–15:00',last_trade_time:''};
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('#futures-amount').innerText(),'¥ 29,825.00');
    assert.match(await page.locator('#quote-status').innerText(),/历史行情/);
    assert.match(await page.locator('#positions .net-amount span').getAttribute('title'),/14:59–15:00.*已完成分钟/);
    strategyStatus=200;
    await page.locator('#refresh').click();
    await page.getByText('long-factor',{exact:true}).waitFor();
    const strategyTable=page.getByRole('table',{name:'rb2701 策略仓位'});
    assert.deepEqual(await strategyTable.locator('th').allTextContents(),['策略','信号目标 · 手','账本净仓 · 手']);
    assert.deepEqual(await strategyTable.locator('tbody tr').allTextContents(),['long-factor+3+2','short-factor-2-2']);
    assert.deepEqual(await page.locator('#positions .aggregate-row').evaluateAll(rows=>rows.map(row=>row.dataset.symbol)),['SA701','rb2701']);
    ctp.entry.positions=[]; ctp.entry.marks=[];
    await page.evaluate(f=>window.__push(f),ctp);
    assert.deepEqual(await page.locator('#positions .aggregate-row').evaluateAll(rows=>rows.map(row=>row.dataset.symbol)),['SA701']);
    await page.locator('#symbol').fill('纯碱');
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    await page.locator('#show-zero').check();
    assert.equal(await page.locator('#positions .aggregate-row').count(),1);
    const saStrategies=page.getByRole('table',{name:'SA701 策略仓位'});
    assert.deepEqual(await saStrategies.locator('tbody td').allTextContents(),['cn-futures-01-sa-1h','-1','0']);
    const saSignals=page.getByRole('table',{name:'SA701 因子信号'});
    const saToggle=page.getByRole('button',{name:'1h 因子信号',exact:true});
    assert.equal(await saToggle.getAttribute('aria-expanded'),'false');
    assert.equal(await saSignals.count(),0);
    await saToggle.click();
    assert.equal(await saToggle.getAttribute('aria-expanded'),'true');
    assert.equal(await saSignals.locator('tbody tr').count(),10);
    assert.deepEqual(await saSignals.getByRole('row').filter({hasText:'td_vi_023'}).locator('td').allTextContents(),['td_vi_023','-1']);
    assert.deepEqual(await saSignals.getByRole('row').filter({hasText:'sa-factor-0'}).locator('td').allTextContents(),['sa-factor-0','0']);
    assert.deepEqual((await page.locator('#positions tr[data-symbol="SA701"] td').allTextContents()).slice(1,3),['0','-1']);
    await page.setViewportSize({width:390,height:844});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.equal(await saSignals.locator('tbody tr:visible').count(),10);
    await Promise.all([
      page.waitForResponse(response=>response.url().endsWith('/strategy-positions/ctp')),
      page.locator('#refresh').click(),
    ]);
    await saSignals.waitFor();
    assert.equal(await saToggle.getAttribute('aria-expanded'),'true');
    await saToggle.click();
    assert.equal(await saToggle.getAttribute('aria-expanded'),'false');
    assert.equal(await saSignals.count(),0);
    await page.setViewportSize({width:1440,height:1000});
    await page.locator('#symbol').fill('');
    assert.equal(await page.locator('#positions tr[data-symbol="sn2701"]').count(),0);
    assert.deepEqual((await page.locator('#positions tr[data-symbol="rb2701"] td').allTextContents()).slice(1,3),['0','+1']);
    assert.equal(await page.locator('#contract-count').innerText(),'0');
    assert.equal(await page.locator('.attribution-gap').count(),0);
    await page.locator('#show-zero').uncheck();
    assert.deepEqual(await page.locator('#positions .aggregate-row').evaluateAll(rows=>rows.map(row=>row.dataset.symbol)),['SA701']);
    ctp.entry.positions=[row('SHFE','rb2701','buy',1)];
    ctp.entry.marks=[{...mark('rb2701','future',10,3140),bid_price:3141,ask_price:3143,bid_lots:1,ask_lots:2}];
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('.attribution-gap').innerText(),/策略合计 0 手，柜台与账本相差 \+1 手/);
    const rbAmount=page.locator('#positions tr[data-symbol="rb2701"] .net-amount');
    assert.equal(await rbAmount.innerText(),'¥ 31,420.00');
    ctp.entry.marks[0].bid_price=3143; ctp.entry.marks[0].ask_price=3145;
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await rbAmount.innerText(),'¥ 31,440.00');
    ctp.entry.marks[0].ask_lots=0;
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await rbAmount.innerText(),'¥ 31,400.00');
    strategyStatus=403;
    await page.locator('#refresh').click();
    await page.getByText('当前账户或策略无查看权限',{exact:true}).waitFor();
    assert.equal(await page.locator('.strategy-detail').count(),0);
    ctp.entry.can_trade=false; ctp.entry.connected=false;
    ctp.entry.reason='CTP trader front offline; waiting for reconnection';
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('#source-updates').innerText(),/等待柜台重连/);
    assert.equal(await page.locator('#notice').isVisible(),false);
    ctp.entry.runtime_status='stopped';
    ctp.entry.reason='CTP account query unavailable; cold restart required';
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('#source-updates').innerText(),/柜台连接恢复中/);
    ctp.entry.reason='CTP transport or control response timed out; cold restart required';
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('#source-updates').innerText(),/柜台连接恢复中/);
    assert.equal(await page.locator('#notice').isVisible(),false);
    ctp.entry.reason='CTP market-data trading day unavailable; cold restart required';
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('#source-updates').innerText(),/等待行情交易日/);
    assert.equal(await page.locator('#notice').isVisible(),false);
    ctp.entry.runtime_status='failed';
    ctp.entry.reason='runtime failed; complete recovery required before trading';
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('#source-updates').innerText(),/柜台运行中断/);
    assert.equal(await page.locator('#notice').isVisible(),true);
    assert.equal(await page.locator('#notice').innerText(),'CTP：柜台进程已退出，恢复后需重新对账');
    assert.equal(await page.locator('tr[data-symbol="rb2701"] > td').nth(1).innerText(),'+1');
    ctp.saved_entry={...ctp.entry,trading_day:'20261119',observed_us:(Date.now()-90000)*1000,positions:[row('SHFE','rb2701','sell',0,2)],
      account:{...account,observed_us:(Date.now()-100000)*1000},position_ready:true};
    await page.evaluate(f=>window.__push(f),{...ctp,entry:null,error:'等待持仓 IPC'});
    assert.match(await page.locator('#source-updates').innerText(),/等待柜台连接/);
    assert.match(await page.locator('#source-updates').innerText(),/快照交易日 20261119/);
    assert.equal(await page.locator('#notice').isVisible(),false);
    assert.equal(await page.locator('tr[data-symbol="rb2701"] > td').nth(1).innerText(),'-2');
    ctp.entry={...ctp.entry,observed_us:Date.now()*1000,position_ready:false,positions:[],account:null,runtime_status:'running',
      reason:'CTP trader front offline; waiting for reconnection'};
    await page.evaluate(f=>window.__push(f),ctp);
    assert.match(await page.locator('#position-note').innerText(),/上次账户快照/);
    assert.match(await page.locator('#funds-status').innerText(),/上次账户快照/);
    assert.equal(await page.locator('tr[data-symbol="rb2701"] > td').nth(1).innerText(),'-2');
    ctp.entry={...ctp.entry,position_ready:true,connected:true,transport_ready:true,ready:true,reason:'',
      positions:[row('SHFE','rb2701','buy',3)],account:{...account,observed_us:Date.now()*1000}};
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('tr[data-symbol="rb2701"] > td').nth(1).innerText(),'+3');
    observation={
      position:{current:true,observed_us:Date.now()*1000,trading_day:'20261119',positions:[row('SHFE','rb2701','buy',4)]},
      account:{...account,current:true,observed_us:Date.now()*1000,balance:550000},
    };
    await page.locator('#refresh').click();
    await page.waitForFunction(()=>document.querySelector('tr[data-symbol="rb2701"] td:nth-child(2)')?.textContent==='+4');
    assert.equal(await page.locator('#account-balance').innerText(),'¥ 550,000.00');
    // A cold Viz can price an authenticated historical observation before the first IPC snapshot.
    const closingMark={...mark('rb2701','future',10,null),trading_day:20261119,bid_price:2982,ask_price:2983,
      bid_lots:20,ask_lots:214,initial:true,native_time:'14:59–15:00',last_trade_time:''};
    observation.position={...observation.position,current:false,positions:[row('SHFE','rb2701','sell',0,2)],marks:[closingMark]};
    delete ctp.saved_entry;
    ctp.entry={...ctp.entry,trading_day:'20261119',connected:false,position_ready:false,positions:[],marks:[],l1_connected:false};
    await page.evaluate(f=>window.__push(f),ctp);
    await page.locator('#refresh').click();
    await page.waitForFunction(()=>document.querySelector('#futures-amount')?.textContent==='¥ -59,650.00');
    assert.match(await page.locator('#position-note').innerText(),/Manager 历史持仓/);
    assert.match(await page.locator('#quote-status').innerText(),/历史行情.*20261119/);
    assert.equal(await page.locator('#options-metric').isVisible(),false);
    ctp.entry.marks=[{...closingMark,trading_day:20261120,bid_price:3100,ask_price:3102,initial:false}];
    ctp.entry.trading_day='20261120';
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('#futures-amount').innerText(),'¥ -59,650.00');
    ctp.entry.trading_day='20261119';
    ctp.entry.marks[0].trading_day=20261119;
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('#futures-amount').innerText(),'¥ -62,020.00');
    await page.goto('http://cn-viz.test/?source=paper');
    await page.evaluate(f=>window.__push(f),ctp);
    await page.evaluate(f=>window.__push(f),frame);
    await page.evaluate(f=>window.__push(f),ctp);
    assert.equal(await page.locator('[data-source="paper"]').getAttribute('aria-selected'),'true');
    await page.setViewportSize({width:390,height:844});
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true);
    assert.ok((await page.locator('#show-zero').boundingBox()).width<=20);
    assert.equal(await page.locator('[data-activity]').count(),0);
    assert.deepEqual(errors,[]);
    assert.ok(requests.every(url=>!url.includes('/snapshot')&&!url.includes('/healthz')));
    console.log('Passed: native WS, funds, L1 amounts, authorized strategy holdings, source isolation, contract search, recovery, reconnect, mobile.');
  } finally { await browser.close(); }
})().catch(error=>{console.error(error);process.exitCode=1;});
