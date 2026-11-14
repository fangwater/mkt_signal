// Real Chromium/CDP smoke through the JP Nginx gateway; no development server.
import { spawn, execFileSync } from 'node:child_process'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { createServer } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import assert from 'node:assert/strict'
import path from 'node:path'

const output = path.resolve('artifacts/cross01_exec_viz_20261103')
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
  chrome = spawn('/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome', ['--headless', '--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking', '--no-proxy-server', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], { stdio: 'ignore', env: { ...process.env, LD_LIBRARY_PATH: '/tmp/playwright-libs/usr/lib/x86_64-linux-gnu:/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu' } })
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
  const sockets = new Map()
  const upgrades = []
  ws.addEventListener('message', ({ data }) => {
    const message = JSON.parse(String(data))
    if (message.id) {
      const call = pending.get(message.id)
      pending.delete(message.id)
      if (message.error) call?.reject(new Error(message.error.message))
      else call?.resolve(message.result)
    } else if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails.text)
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
    throw new Error('browser verification condition timed out: '+expression)
  }

  const members = ['okex_exec_trade01', 'binance_exec_spread01']
  const aliases = ['okex_exec_cross01', 'binance_exec_cross01']
  const prefixes = ['/exec_okex01/', '/exec_binance_spread01/']
  await call('Emulation.setDeviceMetricsOverride', {width:1920,height:1080,deviceScaleFactor:1,mobile:false})
  await call('Page.navigate', {url:base+'/manager/accounts/'})
  await waitFor("document.body.innerText.includes('cross01 拼接执行监控')")
  const accounts = await evaluate("Array.from(document.querySelectorAll('a[href*=\"/account/?source=\"]')).map(a=>({label:a.textContent,href:a.getAttribute('href')}))")
  for (let i=0;i<2;i++) { assert(accounts.some(a=>a.href.includes(members[i]))); assert(await evaluate(`document.body.innerText.includes('${aliases[i]}')`)); }
  assert(!accounts.some(a=>a.href.includes('cta_cross_spread_rx01')))
  await call('Page.navigate',{url:base+'/manager/exec/?group=cross01'})
  await waitFor("document.querySelectorAll('iframe').length===2 && Array.from(document.querySelectorAll('iframe')).every(f=>f.contentDocument?.querySelector('h1')?.textContent.includes('Exec 执行监控'))")
  await waitFor("Array.from(document.querySelectorAll('iframe')).every(f=>f.contentDocument?.getElementById('conn-text')?.textContent.includes('实时'))")
  const frames=await evaluate("Array.from(document.querySelectorAll('iframe')).map(f=>({title:f.title,src:new URL(f.src).pathname,header:f.contentDocument.querySelector('h1').textContent,connection:f.contentDocument.getElementById('conn-text').textContent,rect:{x:f.getBoundingClientRect().x,y:f.getBoundingClientRect().y,width:f.getBoundingClientRect().width}}))")
  assert.deepEqual(frames.map(f=>f.src),prefixes)
  assert.deepEqual(frames.map(f=>f.title),aliases.map(a=>a+' 执行 Viz'))
  assert(frames[1].rect.y>frames[0].rect.y+800)
  for(const prefix of prefixes) assert(upgrades.some(s=>s.status===101 && new URL(s.url).pathname===prefix+'ws'))
  const before=upgrades.length
  await evaluate("document.querySelectorAll('iframe')[0].dataset.stability='retained'")
  await delay(16000)
  assert.equal(await evaluate("document.querySelectorAll('iframe')[0].dataset.stability"),'retained')
  assert.equal(upgrades.length,before)
  await evaluate("Array.from(document.querySelectorAll('button')).find(b=>b.textContent.trim()==='左右').click()")
  await delay(300)
  const columns=await evaluate("Array.from(document.querySelectorAll('iframe')).map(f=>({x:f.getBoundingClientRect().x,y:f.getBoundingClientRect().y,width:f.getBoundingClientRect().width}))")
  assert(Math.abs(columns[0].y-columns[1].y)<2 && columns[1].x>columns[0].x+400)
  const shot=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:false})
  await writeFile(path.join(output,'desktop.png'),Buffer.from(shot.data,'base64'))
  await evaluate("Array.from(document.querySelectorAll('button')).find(b=>b.textContent.includes('组合策略盈亏')).click()")
  await waitFor("document.body.innerText.includes('合计费后交易盈亏') && document.body.innerText.includes('当前区间暂无策略记录')")
  const end=Date.now()-60000
  const timeline=await evaluate(`fetch('/manager/api/timeline?sourceIds=${members.join(',')}&startMs=${end-86400000}&endMs=${end}&includeTheoretical=false').then(r=>r.json())`)
  assert.deepEqual([...timeline.report.selected_source_ids].sort(),[...members].sort())
  assert.equal(timeline.report.summary.nav_change_after_fee_quote,0)
  assert.equal(timeline.estimated_funding.income_quote,0)
  assert.equal(timeline.report.strategy_points.length,0)
  for (const id of members) {
    await call('Page.navigate',{url:base+'/manager/account/?source='+id})
    await waitFor("document.body.innerText.includes('Funding：预估')")
    assert.equal(await evaluate("document.body.innerText.includes('Funding 记录') || document.body.innerText.includes('测试出口并开启')"),false)
  }
  await call('Page.navigate',{url:base+'/manager/exec/?group=cross01'})
  await waitFor("document.querySelectorAll('iframe').length===2")
  await call('Emulation.setDeviceMetricsOverride', {width:390,height:844,deviceScaleFactor:1,mobile:true})
  await delay(500)
  const mobile=await evaluate("({width:innerWidth,scroll:document.documentElement.scrollWidth,frames:Array.from(document.querySelectorAll('iframe')).map(f=>({x:f.getBoundingClientRect().x,y:f.getBoundingClientRect().y,width:f.getBoundingClientRect().width}))})")
  assert(mobile.scroll<=mobile.width+1)
  assert(mobile.frames[1].y>mobile.frames[0].y+800)
  await call('Page.navigate',{url:base+'/manager/exec/'})
  await waitFor("document.querySelectorAll('iframe').length===2")
  const mobileShot=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:false})
  await writeFile(path.join(output,'mobile.png'),Buffer.from(mobileShot.data,'base64'))
  remote(`sql("DELETE FROM cta_auth_sessions WHERE session_id=${auth.sid} AND token_hash=decode('${auth.digest}','hex');")\n`)
  auth=null
  auth=JSON.parse(remote(`
token=secrets.token_urlsafe(32);digest=hashlib.sha256(token.encode()).hexdigest()
sid=sql("INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('"+digest+"','hex'),now()+interval '30 minutes' FROM cta_users WHERE username='prc' AND NOT disabled RETURNING session_id;")
assert sid.isdigit()
print(json.dumps({'token':token,'sid':sid,'digest':digest}))
`))
  await call('Network.setCookie',{name:'cta_session',value:auth.token,url:base,path:'/',httpOnly:true,sameSite:'Lax'})
  await call('Page.navigate',{url:base+'/manager/exec/?group=cross01'})
  await waitFor("document.body.innerText.includes('没有完整查看权限')")
  assert.equal(await evaluate("document.querySelectorAll('iframe').length"),0)
  assert.equal(await evaluate("document.body.innerText.includes('okex_exec_cross01') || document.body.innerText.includes('binance_exec_cross01')"),false)
  assert.equal(errors.length,0)
  const result={accounts,old_aggregate_removed:true,group:'cross01',native_frames:frames,websocket_upgrades:upgrades.filter(s=>prefixes.some(p=>new URL(s.url).pathname===p+'ws')),frames_stable_on_refresh:true,desktop_columns:columns,mobile,combined_pnl:timeline.report.summary.nav_change_after_fee_quote,estimated_funding:timeline.estimated_funding.income_quote,empty_strategies:true,actual_funding_controls_hidden:true,hidden_user_denied:true,page_errors:errors}
  await writeFile(path.join(output,'browser-verification.json'),JSON.stringify(result,null,2)+'\n')
  process.stdout.write(JSON.stringify({group:result.group,frames:frames.length,websocket_statuses:result.websocket_upgrades.map(s=>s.status),desktop_columns:true,mobile_overflow:false,combined_pnl:0,estimated_funding:0,hidden_user_denied:true,page_errors:errors})+'\n')
} finally {
  ws?.close()
  chrome?.kill('SIGTERM')
  tunnel?.kill('SIGTERM')
  if (auth) remote(`sql("DELETE FROM cta_auth_sessions WHERE session_id=${auth.sid} AND token_hash=decode('${auth.digest}','hex');")\n`)
  await delay(500)
  await rm(profile, { recursive: true, force: true })
}
