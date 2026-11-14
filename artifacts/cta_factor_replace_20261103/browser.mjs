// Chromium/CDP verification of the published UI. Preview only; no enable save.
import { spawn, execFileSync } from 'node:child_process'
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises'
import { createServer } from 'node:net'
import { setTimeout as delay } from 'node:timers/promises'
import assert from 'node:assert/strict'
import path from 'node:path'

const output = path.resolve('artifacts/cta_factor_replace_20261103')
const profile = await mkdtemp(path.join(output, 'browser-profile-'))
const helper = `
import sys,json,hashlib,secrets
from pathlib import Path
sys.path.insert(0,str(Path.home()/'crypto_cta_manager/maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import sql
`
const remote = code => execFileSync('ssh', ['-o', 'BatchMode=yes', 'jp-meta-elvpn', 'python3 -'],
  { input: helper + code, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'] })
let auth, chrome, tunnel, ws
try {
  auth = JSON.parse(remote(`
token=secrets.token_urlsafe(32);digest=hashlib.sha256(token.encode()).hexdigest()
sid=sql("INSERT INTO cta_auth_sessions(user_id,token_hash,expires_at) SELECT user_id,decode('"+digest+"','hex'),now()+interval '10 minutes' FROM cta_users WHERE username='lgq' AND NOT disabled RETURNING session_id;",False)
assert sid.isdigit()
print(json.dumps({'token':token,'sid':sid,'digest':digest}))
`))
  const port = await new Promise(resolve => {
    const server = createServer()
    server.listen(0, '127.0.0.1', () => { const port = server.address().port; server.close(() => resolve(port)) })
  })
  tunnel = spawn('ssh', ['-o', 'BatchMode=yes', '-o', 'ExitOnForwardFailure=yes', '-N', '-L',
    `127.0.0.1:${port}:127.0.0.1:4191`, 'jp-meta-elvpn'], { stdio: 'ignore' })
  const base = `http://127.0.0.1:${port}`
  for (let i = 0; i < 50; i++) { try { await fetch(base + '/manager/'); break } catch { await delay(100) } }
  chrome = spawn('/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',
    ['--headless', '--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking',
      '--no-proxy-server', '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'],
    { stdio: 'ignore', env: { ...process.env, LD_LIBRARY_PATH:
      '/tmp/playwright-libs/usr/lib/x86_64-linux-gnu:/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu' } })
  let debug
  for (let i = 0; i < 100; i++) {
    try { debug = (await readFile(path.join(profile, 'DevToolsActivePort'), 'utf8')).split('\n'); break } catch { await delay(100) }
  }
  assert(debug, 'Chromium did not start')
  ws = new WebSocket(`ws://127.0.0.1:${debug[0]}${debug[1]}`)
  await new Promise((resolve, reject) => { ws.addEventListener('open', resolve, { once: true }); ws.addEventListener('error', reject, { once: true }) })
  let sequence = 0
  const pending = new Map(), errors = []
  ws.addEventListener('message', ({ data }) => {
    const message = JSON.parse(String(data))
    if (message.id) {
      const call = pending.get(message.id); pending.delete(message.id)
      if (message.error) call?.reject(new Error(message.error.message)); else call?.resolve(message.result)
    } else if (message.method === 'Runtime.exceptionThrown') errors.push(message.params.exceptionDetails.text)
  })
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, { resolve, reject })
    ws.send(JSON.stringify({ id, method, params, ...(sessionId ? { sessionId } : {}) }))
  })
  const { targetId } = await send('Target.createTarget', { url: 'about:blank' })
  const { sessionId } = await send('Target.attachToTarget', { targetId, flatten: true })
  const call = (method, params) => send(method, params, sessionId)
  for (const name of ['Page.enable', 'Runtime.enable', 'Network.enable']) await call(name)
  await call('Network.setCookie', { name: 'cta_session', value: auth.token, url: base, path: '/', httpOnly: true, sameSite: 'Lax' })
  await call('Page.addScriptToEvaluateOnNewDocument', { source: 'window.confirm=()=>true;' })
  const evaluate = async expression => {
    const result = await call('Runtime.evaluate', { expression, returnByValue: true, awaitPromise: true })
    if (result.exceptionDetails) throw new Error(result.exceptionDetails.text)
    return result.result.value
  }
  const waitFor = async expression => {
    for (let i = 0; i < 100; i++) { if (await evaluate(expression)) return; await delay(100) }
    throw new Error('UI verification timeout: ' + expression)
  }
  await call('Emulation.setDeviceMetricsOverride', { width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false })
  const checked = []
  for (const [worker, combination, rules, allocation] of [
    ['cta-prep-binance-futures', 'binance-futures_1min', 45, 100],
    ['cta-prep-binance-futures', 'binance-futures_5min', 48, 200],
    ['cta-prep-binance-futures', 'binance-futures_15min', 146, 200],
    ['cta-factor-spread', 'okex-futures_binance-futures_1min', 34, 200],
  ]) {
    const route = `/cta/cta-prep-binance-futures/config/?worker=${worker}#${combination}`
    await call('Page.navigate', { url: base + route })
    await waitFor(`document.querySelector('#name')?.value===${JSON.stringify(combination)} && document.querySelector('main').getAttribute('aria-busy')==='false'`)
    assert.equal(await evaluate("document.querySelector('#enabled').checked"), false)
    assert.equal(await evaluate("Number(document.querySelector('#max-pos').value)"), allocation)
    assert.equal(await evaluate("document.querySelectorAll('#rows tr').length"), rules)
    assert.equal(await evaluate("document.querySelectorAll('#worker-select option').length"), 2)
    await evaluate("document.querySelector('#enabled').checked=true;document.querySelector('#enabled').dispatchEvent(new Event('input',{bubbles:true}));document.querySelector('#preview').click();")
    await waitFor("document.querySelector('#message').textContent.includes('校验通过') && document.querySelector('main').getAttribute('aria-busy')==='false'")
    const persisted = await evaluate(`fetch('api/config?worker=${worker}').then(r=>r.json()).then(d=>Object.values(d.plan.combinations).map(c=>c.signal.enabled))`)
    assert(persisted.every(flag => flag === false), 'preview changed a persistent enable flag')
    await evaluate("document.querySelector('#reload').click()")
    await waitFor("!document.querySelector('#enabled').checked && document.querySelector('main').getAttribute('aria-busy')==='false'")
    checked.push({ worker, combination, rules, max_pos_u: allocation, enabled: false, independent_preview: true })
  }
  const desktop = await call('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true })
  await writeFile(path.join(output, 'desktop.png'), Buffer.from(desktop.data, 'base64'))
  await call('Emulation.setDeviceMetricsOverride', { width: 390, height: 844, deviceScaleFactor: 1, mobile: false })
  assert(await evaluate('document.documentElement.scrollWidth <= innerWidth'), 'mobile page overflow')
  const mobile = await call('Page.captureScreenshot', { format: 'png', captureBeyondViewport: true })
  await writeFile(path.join(output, 'mobile.png'), Buffer.from(mobile.data, 'base64'))
  assert.equal(errors.length, 0)
  const result = { checked, previews_only: true, desktop_width: 1440, mobile_width: 390, script_errors: errors }
  await writeFile(path.join(output, 'browser-verification.json'), JSON.stringify(result, null, 2) + '\n')
  console.log(JSON.stringify(result))
} finally {
  ws?.close(); chrome?.kill('SIGTERM'); tunnel?.kill('SIGTERM')
  if (auth) remote(`sql("DELETE FROM cta_auth_sessions WHERE session_id=${auth.sid} AND token_hash=decode('${auth.digest}','hex');",False)\n`)
  await delay(200)
  await rm(profile, { recursive: true, force: true })
}
