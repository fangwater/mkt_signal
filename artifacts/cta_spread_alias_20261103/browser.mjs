// Real Chromium/CDP smoke through the JP Nginx gateway; no development server.
import { spawn, execFileSync } from 'node:child_process'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { createServer } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import assert from 'node:assert/strict'
import path from 'node:path'

const output = path.resolve('artifacts/cta_spread_alias_20261103')
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
      if (await evaluate(expression)) return
      await delay(500)
    }
    throw new Error('browser verification condition timed out')
  }
  const accountId = 'cta_cross_spread_rx01'
  const alias = 'okex_binace_cross_1min'
  const members = ['okex_exec_trade01', 'binance_exec_spread01']
  await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false })
  await call('Page.navigate', { url: base + '/manager/accounts/' })
  await waitFor(`document.body.innerText.includes('${alias}')`)
  const dashboard = await evaluate("fetch('/manager/api/dashboard').then(r=>r.json())")
  const accounts = dashboard.accounts.filter(a => a.source_id === accountId)
  assert.equal(accounts.length, 1)
  assert.equal(accounts[0].account, alias)
  assert.equal(accounts[0].display_alias, alias)
  assert.equal(accounts[0].can_edit_alias, true)
  assert.equal(dashboard.accounts.filter(a => members.includes(a.source_id)).length, 0)
  await evaluate(`document.querySelector('button[aria-label="修改 ${alias} 的别名"]').click()`)
  await waitFor("document.querySelector('input[aria-label=\"\u8d26\u6237\u522b\u540d\"]') !== null")
  assert.equal(await evaluate("document.querySelector('input[aria-label=\"\u8d26\u6237\u522b\u540d\"]').value"), alias)
  await evaluate("document.querySelector('input[aria-label=\"\u8d26\u6237\u522b\u540d\"]').dispatchEvent(new KeyboardEvent(\"keydown\", {key:\"Enter\", bubbles:true}))")
  await waitFor("document.querySelector('input[aria-label=\"\u8d26\u6237\u522b\u540d\"]') === null")
  await call('Page.navigate', { url: base + '/manager/account/?source=' + accountId })
  await waitFor(`document.body.innerText.includes('${alias}') && document.body.innerText.includes('400 / 分钟')`)
  const legLinks = await evaluate("[...document.querySelectorAll('a')].filter(a => a.innerText.includes('执行腿配置')).map(a => a.href)")
  assert.equal(legLinks.length, 2)
  assert.equal(await evaluate("document.body.innerText.includes('okex_exec_cross01') && document.body.innerText.includes('binance_exec_cross01')"), true)
  for (const [index, link] of legLinks.entries()) {
    const url = new URL(link)
    assert.equal(url.searchParams.get('source'), accountId)
    assert.equal(url.searchParams.get('executionSource'), members[index])
  }
  assert.equal(await evaluate(`document.body.innerText.includes('lgq_okex_cta_spread') || document.body.innerText.includes('lgq_binance_cta_spread')`), false)
  await writeFile(path.join(output, 'desktop.png'), Buffer.from((await call('Page.captureScreenshot', { format: 'png' })).data, 'base64'))
  await call('Page.navigate', { url: legLinks[0] })
  await waitFor("document.querySelector('[aria-label=\"账户执行腿\"]') && document.body.innerText.includes('估算费率')")
  assert.equal(await evaluate('new URLSearchParams(location.search).get("source")'), accountId)
  assert.equal(await evaluate('new URLSearchParams(location.search).get("executionSource")'), members[0])
  assert.equal(await evaluate(`document.querySelector('header').innerText.includes('${alias}')`), true)
  assert.equal(await evaluate("document.querySelector('[aria-label=\"账户执行腿\"]')?.querySelectorAll('a').length"), 3)
  await call('Page.navigate', { url: base + `/manager/config/bindings/?source=${accountId}&executionSource=${members[0]}` })
  await waitFor("document.querySelector('select[aria-label=\"\u6267\u884c\u817f\"]')?.value === \"okex_exec_trade01\"")
  const selectedAccount = await evaluate(`(() => { const selected = [...document.querySelectorAll('select')].find(e => [...e.options].some(o => o.value === '${accountId}')); return {value: selected.value, options: [...selected.options].map(o => ({value: o.value, text: o.text}))} })()`)
  assert.equal(selectedAccount.value, accountId)
  assert.equal(selectedAccount.options.find(o => o.value === accountId).text, alias)
  assert.equal(selectedAccount.options.filter(o => members.includes(o.value)).length, 0)
  await evaluate("(() => { const e=document.querySelector('select[aria-label=\"\u6267\u884c\u817f\"]'); e.value=\"binance_exec_spread01\"; e.dispatchEvent(new Event(\"change\", {bubbles:true})); })()")
  await waitFor('new URLSearchParams(location.search).get("executionSource") === "binance_exec_spread01"')
  assert.equal(await evaluate('new URLSearchParams(location.search).get("source")'), accountId)
  await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: true })
  await call('Page.navigate', { url: base + '/manager/account/?source=' + accountId })
  await waitFor(`document.body.innerText.includes('${alias}') && document.body.innerText.includes('400 / 分钟')`)
  assert.equal(await evaluate('document.documentElement.scrollWidth <= innerWidth + 1'), true)
  await writeFile(path.join(output, 'mobile.png'), Buffer.from((await call('Page.captureScreenshot', { format: 'png' })).data, 'base64'))
  await call('Page.navigate', { url: base + '/manager/?source=' + accountId })
  await waitFor(`document.querySelector('[aria-label="账户范围"] [aria-pressed="true"]')?.innerText === '${alias}'`)
  assert.equal(await evaluate('new URLSearchParams(location.search).get("source")'), accountId)
  assert.equal(errors.length, 0)
  const result = { account_id: accountId, display_alias: alias, public_account_entries: 1, alias_editor: 'passed', execution_tabs: 'passed', bindings_account_selector: 'single account', both_leg_urls_keep_account_id: true, desktop: 'passed', mobile_width: 'passed', page_errors: errors }
  await writeFile(path.join(output, 'browser-verification.json'), JSON.stringify(result, null, 2) + '\n')
  process.stdout.write(JSON.stringify(result) + '\n')

} finally {
  ws?.close()
  chrome?.kill('SIGTERM')
  tunnel?.kill('SIGTERM')
  if (auth) remote(`sql("DELETE FROM cta_auth_sessions WHERE session_id=${auth.sid} AND token_hash=decode('${auth.digest}','hex');")\n`)
  await delay(500)
  await rm(profile, { recursive: true, force: true })
}
