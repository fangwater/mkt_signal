// Real Chromium/CDP smoke through the JP Nginx gateway; no development server.
import { spawn, execFileSync } from 'node:child_process'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { createServer } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import assert from 'node:assert/strict'
import path from 'node:path'

const output = path.resolve('artifacts/cta_spread_data_audit_20261103')
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
  ws.addEventListener('message', ({ data }) => {
    const message = JSON.parse(String(data))
    if (message.id) {
      const call = pending.get(message.id)
      pending.delete(message.id)
      if (message.error) call?.reject(new Error(message.error.message))
      else call?.resolve(message.result)
    } else if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails.text)
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
    throw new Error('browser verification condition timed out')
  }
  const accountId = 'cta_cross_spread_rx01'
  const members = ['okex_exec_trade01', 'binance_exec_spread01']
  await call('Page.navigate', {url: base + '/manager/account/?source=' + accountId})
  await waitFor("document.body.innerText.includes('Funding：预估') && document.querySelector('select[aria-label=\"交易市场\"]')")
  await evaluate(`(() => {const e=document.querySelector('select[aria-label="交易市场"]');e.value='${members[1]}';e.dispatchEvent(new Event('change',{bubbles:true}));})()`)
  await waitFor("document.body.innerText.includes('Taker 1.46 bps')")
  assert.equal(await evaluate("document.body.innerText.includes('Funding 记录') || document.body.innerText.includes('测试出口并开启')"), false)
  await call('Page.navigate', {url: base + '/manager/config/bindings/?source=' + accountId})
  await waitFor("document.querySelector('select[aria-label=\"交易市场\"]')")
  await evaluate(`(() => {const e=document.querySelector('select[aria-label="交易市场"]');e.value='${members[1]}';e.dispatchEvent(new Event('change',{bubbles:true}));})()`)
  assert.equal(await evaluate("document.body.innerText.includes('Funding 记录') || document.body.innerText.includes('测试出口并开启')"), false)
  const actual = await evaluate(`Promise.all(${JSON.stringify(members)}.map(id=>fetch('/manager/api/catalog/accounts/'+id+'/funding').then(r=>r.json())))`)
  assert(actual.every(s => !s.enabled && s.summary.record_count === 0))
  const snapshot = await evaluate(`fetch('/manager/api/timeline?sourceIds=${accountId}&startMs=1793625556000&endMs=${Date.now()-60000}&includeTheoretical=false').then(r=>r.json())`)
  assert.equal(snapshot.funding.summary.enabled, false)
  assert.equal(snapshot.estimated_funding.income_quote, 0)
  assert.equal(snapshot.estimated_funding.unavailable_reason, null)
  await call('Page.navigate', {url: base + '/manager/account/?source=binance_exec_trade06'})
  await waitFor("document.body.innerText.includes('Funding 记录')")
  assert.equal(errors.length, 0)
  const result = {account_id:accountId, funding_mode:'estimated', actual_collectors_enabled:[false,false], actual_records:[0,0], estimate_income:0, estimate_complete:true, aggregate_actual_switches_hidden:true, other_accounts_funding_ui_preserved:true, page_errors:errors}
  await writeFile(path.join(output,'browser-verification.json'),JSON.stringify(result,null,2)+'\n')
  process.stdout.write(JSON.stringify(result)+'\n')

} finally {
  ws?.close()
  chrome?.kill('SIGTERM')
  tunnel?.kill('SIGTERM')
  if (auth) remote(`sql("DELETE FROM cta_auth_sessions WHERE session_id=${auth.sid} AND token_hash=decode('${auth.digest}','hex');")\n`)
  await delay(500)
  await rm(profile, { recursive: true, force: true })
}
