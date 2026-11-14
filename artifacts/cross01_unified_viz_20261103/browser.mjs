// Real Chromium/CDP smoke through the JP Nginx gateway; no development server.
import { spawn, execFileSync } from 'node:child_process'
import { mkdtemp, readFile, writeFile, rm, readdir } from 'node:fs/promises'
import { createServer } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import assert from 'node:assert/strict'
import path from 'node:path'

const output = path.resolve('artifacts/cross01_unified_viz_20261103')
const profile = await mkdtemp(path.join(output, 'browser-profile-'))
const helper = `
import ast,json,hashlib,secrets
from pathlib import Path
h=Path.home();p=h/'binance_exec_trade06/run/rx01_activation_ops.py';tree=ast.parse(p.read_text())
ns={'MANAGER':h/'crypto_cta_manager'}
nodes=[n for n in tree.body if isinstance(n,(ast.Import,ast.ImportFrom)) or isinstance(n,ast.FunctionDef) and n.name in ['db_env','sql']]
exec(compile(ast.Module(body=nodes,type_ignores=[]),str(p),'exec'),ns)
sql=ns['sql']
`
const remote = (code) => execFileSync('ssh', ['-o', 'BatchMode=yes', '-o', 'ConnectTimeout=8', 'jp-meta-elvpn', 'python3 -'], { input: helper + code, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] })
let auth, chrome, tunnel, ws
try {
  auth = JSON.parse(remote(`
token=secrets.token_urlsafe(32);digest=hashlib.sha256(token.encode()).hexdigest()
sid=sql("INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('"+digest+"','hex'),now()+interval '30 minutes' FROM cta_users WHERE username='lgq' AND NOT disabled RETURNING session_id;")
assert sid.isdigit()
print(json.dumps({'token':token,'sid':sid,'digest':digest}))
`))
  const port = await new Promise((resolve) => {
    const server = createServer()
    server.listen(0, '127.0.0.1', () => { const port = server.address().port; server.close(() => resolve(port)) })
  })
  tunnel = spawn('ssh', ['-o', 'BatchMode=yes', '-o', 'ExitOnForwardFailure=yes', '-N', '-L', `127.0.0.1:${port}:127.0.0.1:4191`, 'jp-meta-elvpn'], { stdio: 'ignore' })
  const base = `http://127.0.0.1:${port}`
  for (let i = 0; i < 50; i++) {
    try { await fetch(base + '/manager/'); break } catch { await delay(100) }
  }
  chrome = spawn('/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome', ['--headless', '--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking', '--no-proxy-server', '--disable-features=LocalNetworkAccessChecks', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', env: { ...process.env, LD_LIBRARY_PATH: '/tmp/playwright-libs/usr/lib/x86_64-linux-gnu:/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu' } })
  let debug
  for (let i = 0; i < 100; i++) {
    try { debug = (await readFile(path.join(profile, 'DevToolsActivePort'), 'utf8')).split('\n'); break } catch { await delay(100) }
  }
  assert(debug)
  ws = new WebSocket(`ws://127.0.0.1:${debug[0]}${debug[1]}`)
  await new Promise((resolve, reject) => { ws.addEventListener('open', resolve, { once: true }); ws.addEventListener('error', reject, { once: true }) })
  let seq = 0
  const pending = new Map()
  const errors = []
  const interceptionErrors = []
  let intercept = async () => {}
  const sockets = new Map()
  const upgrades = []
  const socketErrors = []
  const requests = []
  ws.addEventListener('message', ({ data }) => {
    const message = JSON.parse(String(data))
    if (message.id) {
      const call = pending.get(message.id)
      pending.delete(message.id)
      if (message.error) call?.reject(new Error(message.error.message))
      else call?.resolve(message.result)
    } else if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails.text)
    else if (message.method === 'Fetch.requestPaused') void intercept(message.params).catch(e=>interceptionErrors.push(String(e)))
    else if (message.method === 'Network.requestWillBeSent') requests.push(message.params.request.url)
    else if (message.method === 'Network.webSocketFrameError') socketErrors.push(message.params.errorMessage)
    else if (message.method === 'Network.webSocketCreated') sockets.set(message.params.requestId, message.params.url)
    else if (message.method === 'Network.webSocketHandshakeResponseReceived') upgrades.push({url:sockets.get(message.params.requestId),status:message.params.response.status})
  })
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++seq
    pending.set(id, { resolve, reject })
    ws.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }))
  })
  const { targetId } = await send('Target.createTarget', { url: 'about:blank' })
  const { sessionId } = await send('Target.attachToTarget', { targetId, flatten: true })
  const call = (method, params) => send(method, params, sessionId)
  await call('Page.enable')
  await call('Runtime.enable')
  await call('Network.enable')
  await call('Network.setCookie', { name: 'cta_session', value: auth.token, url: base, path: '/', httpOnly: true, sameSite: 'Lax' })
  const evaluate = async (expression) => {
    const result = await call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true })
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.text)
    return result.result.value
  }
  const waitFor = async (expression) => {
    for (let i = 0; i < 180; i++) {
      if (await evaluate(`Boolean(${expression})`)) return
      await delay(500)
    }
    process.stderr.write(JSON.stringify({condition:expression,body:await evaluate('document.body.innerText'),errors,upgrades,socketErrors})+'\n')
    throw new Error('browser verification condition timed out: '+expression)
  }



  const published=process.argv.includes('--published')
  const accountId='cta_cross_spread_rx01', alias='okex_binace_cross_1min'
  const members=['okex_exec_trade01','binance_exec_spread01'], venues=['okex-futures','binance-futures'], prefixes=['/exec_okex01/','/exec_binance_spread01/']
  const dist='/home/fanghaizhou/crypto_cta_manager/frontend/dist'
  const files=new Map([['index.html',await readFile(path.join(dist,'index.html'))],['favicon.svg',await readFile(path.join(dist,'favicon.svg'))]])
  for(const name of await readdir(path.join(dist,'assets')))files.set('assets/'+name,await readFile(path.join(dist,'assets',name)))
  let denyBinance=false, oldSnapshot=false
  function fixture(index, timestamp=Date.now()) {
    const row=(overrides={})=>({strategy_name:'spread',symbol:'ETHUSDT',position_allocated:true,execution_complete:false,completion_reason:'',source_updated_at_ms:timestamp-1000,
      target_qty:4,current_qty:3,effective_position_qty:3,delta_qty:1,live_order_qty:0,pending_qty:1,account_position_qty:3,target_usdt:80,current_usdt:60,account_position_usdt:60,
      delta_usdt:20,live_order_usdt:0,pending_usdt:20,active_batches:0,remaining_batches:1,estimated_completion_ts_ms:timestamp+10000,...overrides})
    const rows=index===0?[row(),row({strategy_name:'trend',symbol:'BTCUSDT',target_qty:0.1,target_usdt:10,current_qty:0,current_usdt:0,effective_position_qty:0,
      account_position_qty:0,account_position_usdt:0,delta_qty:0.1,delta_usdt:10,pending_qty:0.1,pending_usdt:10})]:[row({target_qty:-8,target_usdt:-160,current_qty:-7,current_usdt:-140,effective_position_qty:-7,account_position_qty:-7,account_position_usdt:-140,delta_qty:-1,delta_usdt:-20,pending_qty:-1,pending_usdt:-20})]
    return [{type:'exec_pre_trade_state',namespace:members[index],venue:venues[index],entry:{ts_ms:timestamp,position_ready:true,rows}},
      {type:'exec_pre_trade_risk',namespace:members[index],venue:venues[index],entry:{ts_ms:timestamp,venue:venues[index],equity_usdt:index===0?100:300,
        long_notional_usdt:index===0?60:0,short_notional_usdt:index===0?0:140,gross_notional_usdt:index===0?60:140,net_notional_usdt:index===0?60:-140,leverage:999}}]
  }
  intercept=async ({requestId,request})=>{
    const url=new URL(request.url), nativeIndex=prefixes.findIndex(p=>url.pathname===p+'snapshot')
    const respond=async(status,data,type='application/json')=>call('Fetch.fulfillRequest',{requestId,responseCode:status,responseHeaders:[{name:'Content-Type',value:type},{name:'Cache-Control',value:'no-store'},{name:'Date',value:new Date().toUTCString()}],body:Buffer.from(data).toString('base64')})
    if(nativeIndex===1&&denyBinance)return respond(403,'{"error":"test-only denied source"}')
    if(nativeIndex>=0&&oldSnapshot)return respond(200,JSON.stringify({ts_ms:Date.now(),entries:fixture(nativeIndex,Date.now()-60000)}))
    if(!published&&url.pathname.startsWith('/manager/')&&!url.pathname.startsWith('/manager/api/')) {
      const relative=url.pathname.slice('/manager/'.length), name=files.has(relative)?relative:'index.html', data=files.get(name)
      return respond(200,data,name.endsWith('.js')?'text/javascript':name.endsWith('.css')?'text/css':name.endsWith('.svg')?'image/svg+xml':'text/html')
    }
    return call('Fetch.continueRequest',{requestId})
  }
  await call('Fetch.enable',{patterns:[{urlPattern:'*/manager/*'},{urlPattern:'*/exec_okex01/snapshot*'},{urlPattern:'*/exec_binance_spread01/snapshot*'}]})
  await call('Page.addScriptToEvaluateOnNewDocument',{source:`(() => {const Native=window.WebSocket;window.__testExecSockets=new Map();window.WebSocket=class extends Native{constructor(url,...args){super(url,...args);const p=new URL(url,location.href).pathname;if(['/exec_okex01/ws','/exec_binance_spread01/ws'].includes(p))window.__testExecSockets.set(p,this)}}})()`})
  const select=async(label,value)=>evaluate(`(() => {const e=document.querySelector('select[aria-label=${JSON.stringify(label)}]');e.value=${JSON.stringify(value)};e.dispatchEvent(new Event('change',{bubbles:true}));})()`)
  const input=async(label,value)=>evaluate(`(() => {const e=document.querySelector('input[aria-label=${JSON.stringify(label)}]');Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set.call(e,${JSON.stringify(value)});e.dispatchEvent(new Event('input',{bubbles:true}));})()`)
  const metric=label=>evaluate(`document.querySelector('[data-exec-metric=${JSON.stringify(label)}]').textContent`)
  const work=label=>evaluate(`document.querySelector('[data-work-metric=${JSON.stringify(label)}]').textContent`)
  await call('Emulation.setDeviceMetricsOverride',{width:1920,height:1080,deviceScaleFactor:1,mobile:false})
  await call('Page.navigate',{url:base+'/manager/accounts/'})
  await waitFor(`document.querySelector('a[href="/manager/account/?source=${accountId}"]')`)
  for(const name of ['okex_exec_cross01','binance_exec_cross01'])assert.equal(await evaluate(`document.body.innerText.includes('${name}')`),false)
  await evaluate("document.querySelector('a[href=\"/manager/exec/?group=cross01\"]').click()")
  await waitFor("document.querySelector('[data-unified-execution-monitor]') && window.__testExecSockets.size===2 && Array.from(window.__testExecSockets.values()).every(s=>s.readyState===1)")
  assert.equal(await evaluate("document.querySelectorAll('iframe').length"),0)
  assert.equal(await evaluate("document.querySelectorAll('table[aria-label=\"统一执行仓位\"]').length"),1)
  assert.equal(await evaluate("document.querySelectorAll('[aria-label=\"账户执行汇总\"]').length"),1)
  assert(await evaluate(`document.querySelector('header').textContent.includes('${alias}')`))
  assert(await evaluate("document.body.innerText.includes('等待执行仓位数据')"))
  assert.equal(await work('待下单金额'),'—')
  for(const p of prefixes)assert(upgrades.some(x=>x.status===101&&new URL(x.url).pathname===p+'ws'))
  const initialConnections=upgrades.length
  await delay(16000)
  assert.equal(upgrades.length,initialConnections)
  await evaluate("document.querySelector('button[aria-label=\"刷新执行数据\"]').click()")
  await delay(250)
  assert.equal(upgrades.length,initialConnections)
  await writeFile(path.join(output,published?'live-empty.png':'candidate-empty.png'),Buffer.from((await call('Page.captureScreenshot',{format:'png'})).data,'base64'))
  let fixtureChecks=null
  if(!published) {
    const payloads=[fixture(0),fixture(1)]
    await evaluate(`(() => {window.__testExecPayloads=${JSON.stringify(payloads)};window.__testTick=()=>{window.__testExecPayloads.forEach((messages,i)=>{const socket=window.__testExecSockets.get(${JSON.stringify(prefixes)}[i]+'ws');if(!socket)return;messages.forEach(m=>{m.entry.ts_ms=Date.now();socket.dispatchEvent(new MessageEvent('message',{data:JSON.stringify(m)}))})})};window.__testTick();window.__testTicker=setInterval(window.__testTick,1000)})()`)
    await waitFor("document.querySelectorAll('[data-execution-group]').length===3")
    assert((await metric('实时权益')).includes('400.00'))
    assert((await metric('总名义价值')).includes('200.00'))
    assert((await metric('组合杠杆')).includes('0.500'))
    assert.equal(await work('待下单金额'),'50.00')
    await select('策略筛选','spread')
    await waitFor("document.querySelectorAll('[data-execution-group]').length===2")
    assert.equal(await work('待下单金额'),'40.00')
    await select('交易所筛选',members[1])
    await waitFor("document.querySelectorAll('[data-execution-group]').length===1")
    assert.equal(await work('待下单金额'),'20.00')
    await select('交易所筛选','');await select('策略筛选','');await input('品种筛选','eth')
    await waitFor("document.querySelectorAll('[data-execution-group]').length===2")
    await input('品种筛选','')
    await evaluate("Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim()==='按策略').click()")
    await waitFor("document.querySelectorAll('[data-execution-group]').length===2")
    await evaluate("document.querySelector('[data-execution-group=\"spread\"] button').click()")
    await waitFor("document.querySelectorAll('[data-execution-row]').length===2")
    assert.deepEqual((await evaluate("Array.from(document.querySelectorAll('[data-execution-row]')).map(e=>e.dataset.source)")).sort(),[...members].sort())
    await evaluate("(() => {const r=window.__testExecPayloads[1][0].entry.rows[0];Object.assign(r,{target_qty:-9,target_usdt:-180,delta_qty:-2,delta_usdt:-40,pending_qty:-2,pending_usdt:-40});window.__testTick()})()")
    await waitFor("document.querySelector('[data-work-metric=\"待下单金额\"]').textContent==='70.00'")
    oldSnapshot=true
    await evaluate("document.querySelector('button[aria-label=\"刷新执行数据\"]').click()")
    await delay(300)
    assert.equal(await work('待下单金额'),'70.00')
    oldSnapshot=false
    await writeFile(path.join(output,'fixture-desktop.png'),Buffer.from((await call('Page.captureScreenshot',{format:'png'})).data,'base64'))
    await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true})
    await delay(300)
    assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),true)
    await writeFile(path.join(output,'fixture-mobile.png'),Buffer.from((await call('Page.captureScreenshot',{format:'png'})).data,'base64'))
    await evaluate('clearInterval(window.__testTicker)')
    await delay(6500)
    assert.equal(await work('待下单金额'),'—')
    assert((await metric('实时权益')).includes('—'))
    assert(await evaluate("document.body.innerText.includes('数据延迟')"))
    await evaluate('window.__testTick();window.__testTicker=setInterval(window.__testTick,1000)')
    await waitFor("document.querySelector('[data-work-metric=\"待下单金额\"]').textContent==='70.00'")
    denyBinance=true
    await evaluate("document.querySelector('button[aria-label=\"刷新执行数据\"]').click()")
    await waitFor("document.body.innerText.includes('无查看权限')")
    assert.equal(await work('待下单金额'),'—')
    assert((await metric('实时权益')).includes('—'))
    assert.equal(await evaluate("document.querySelectorAll('[data-execution-row][data-source=\"binance_exec_spread01\"]').length"),0)
    fixtureChecks={browser_only:true,venues_preserved:true,shared_filters:true,strategy_group_once:true,equity:400,gross:200,leverage:0.5,opposite_pending_gross:40,live_update_pending:70,old_snapshot_ignored:true,stale_totals_unknown:true,one_leg_denied_isolated:true,mobile_no_overflow:true}
    denyBinance=false
    await call('Page.navigate',{url:base+'/manager/exec/?group=cross01'})
    await waitFor("document.querySelector('[data-unified-execution-monitor]') && document.body.innerText.includes('等待执行仓位数据')")
    assert.equal(await evaluate("document.querySelectorAll('[data-execution-group]').length"),0)
  }
  const actual=await evaluate(`Promise.all(${JSON.stringify(prefixes)}.map(p=>fetch(p+'snapshot').then(r=>r.json())))`)
  assert(actual.every(s=>s.entries.length===0))
  await evaluate("Array.from(document.querySelectorAll('button')).find(b=>b.textContent.includes('组合策略盈亏')).click()")
  await waitFor("document.body.innerText.includes('合计费后交易盈亏') && document.body.innerText.includes('当前区间暂无策略记录')")
  await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:true})
  assert.equal(await evaluate('document.documentElement.scrollWidth<=innerWidth+1'),true)
  const d=await evaluate("fetch('/manager/api/dashboard').then(r=>r.json())")
  assert.equal(d.accounts.filter(a=>a.source_id===accountId).length,1)
  assert(!d.accounts.some(a=>members.includes(a.source_id)))
  remote(`sql("DELETE FROM cta_auth_sessions WHERE session_id=${auth.sid} AND token_hash=decode('${auth.digest}','hex');")\n`);auth=null
  auth=JSON.parse(remote(`
token=secrets.token_urlsafe(32);digest=hashlib.sha256(token.encode()).hexdigest()
sid=sql("INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('"+digest+"','hex'),now()+interval '30 minutes' FROM cta_users WHERE username='prc' AND NOT disabled RETURNING session_id;")
assert sid.isdigit()
print(json.dumps({'token':token,'sid':sid,'digest':digest}))
`))
  await call('Network.setCookie',{name:'cta_session',value:auth.token,url:base,path:'/',httpOnly:true,sameSite:'Lax'})
  const beforeDenied=requests.length
  await call('Page.navigate',{url:base+'/manager/exec/?group=cross01'})
  await waitFor("document.body.innerText.includes('没有完整查看权限')")
  assert.equal(await evaluate("document.querySelectorAll('[data-unified-execution-monitor],iframe').length"),0)
  assert(!requests.slice(beforeDenied).some(s=>prefixes.some(p=>new URL(s).pathname.startsWith(p))))
  assert.equal(errors.length,0)
  assert.equal(interceptionErrors.length,0)
  const result={published,public_alias:alias,public_accounts:1,iframes:0,unified_tables:1,summary_panels:1,real_websocket_statuses:[101,101],connections_stable_during_metadata_and_refresh:true,actual_exec_snapshots_empty:true,combined_pnl_empty:true,hidden_user_denied:true,page_errors:errors,fixtures:fixtureChecks}
  await writeFile(path.join(output,published?'live-browser.json':'candidate-browser.json'),JSON.stringify(result,null,2)+'\n')
  process.stdout.write(JSON.stringify(result)+'\n')
} finally {
  ws?.close()
  chrome?.kill('SIGTERM')
  tunnel?.kill('SIGTERM')
  if (auth) remote(`sql("DELETE FROM cta_auth_sessions WHERE session_id=${auth.sid} AND token_hash=decode('${auth.digest}','hex');")\n`)
  await delay(500)
  await rm(profile, { recursive: true, force: true })
}
