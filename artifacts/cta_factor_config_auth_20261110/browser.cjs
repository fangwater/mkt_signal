// Read-only browser checks through JP Nginx; sessions exist only in memory.
const { chromium } = require('/home/fanghaizhou/front_queue_research/node_modules/playwright')
const { spawn } = require('node:child_process')
const { once } = require('node:events')
const { createInterface } = require('node:readline')
const { createServer } = require('node:net')
const fs = require('node:fs/promises')
const path = require('node:path')
const assert = require('node:assert/strict')
const { setTimeout: delay } = require('node:timers/promises')
const live = true
const stage = process.env.CONFIG_AUTH_REMOTE_STAGE
assert.match(stage, /^\/home\/ubuntu\/crypto_cta_manager\/deploy-staging\/cta_factor_config_auth_[0-9TZ]+$/)
const account = 'cta_cross_spread_rx01'
const intervals = ['1min', '5min', '15min', '1h']
const prefix = '/cta/cta-prep-binance-futures/config/?worker=cta-factor-spread#okex-futures_binance-futures_'

;(async () => {
  let browser, tunnel, auth
  const errors = [], blockedWrites = [], checks = []
  const port = await new Promise(resolve => {
    const server = createServer()
    server.listen(0, '127.0.0.1', () => { const port = server.address().port; server.close(() => resolve(port)) })
  })
  const base = `http://127.0.0.1:${port}`
  async function contextFor(username) {
    auth = spawn('ssh', ['-T', '-o', 'BatchMode=yes', 'jp-meta-elvpn', 'python3', `${stage}/gateway.py`, 'session', '--username', username], { stdio: ['pipe', 'pipe', 'pipe'] })
    const lines = createInterface({ input: auth.stdout })
    const [token] = await Promise.race([once(lines, 'line'), delay(15000).then(() => { throw Error('Temporary session timed out') })])
    assert(token.length > 20)
    const context = await browser.newContext({ viewport: { width: 1440, height: 1000 } })
    await context.addCookies([{ name: 'cta_session', value: token, url: base, httpOnly: true, sameSite: 'Lax' }])
    await context.route('**/*', async route => {
      const request = route.request(), url = new URL(request.url())
      if (url.origin === base && !['GET', 'HEAD'].includes(request.method())) {
        blockedWrites.push({ method: request.method(), path: url.pathname })
        return route.abort()
      }
      return route.continue()
    })
    assert(!(await context.cookies()).some(cookie => cookie.name === 'delist_session'))
    return context
  }
  async function endAuth() {
    if (!auth) return
    const done = once(auth, 'exit')
    auth.stdin.end('\n')
    await done
    auth = null
  }
  async function destinationVerified(page, interval) {
    await page.waitForURL(base + prefix + interval)
    await page.waitForFunction(interval => document.querySelector('#current-frequency')?.textContent === interval, interval)
    assert.equal(await page.locator('#strategy-type').inputValue(), 'cta-factor-spread')
    assert.equal(await page.locator('#strategy-group').inputValue(), 'spread01')
    assert.equal(await page.locator('#symbol-group').inputValue(), '')
    assert.equal(await page.locator('#enabled').inputValue(), 'true')
    const rules = JSON.parse(await page.locator('#factor-json').inputValue())
    assert.equal(rules.length, { '1min': 28, '5min': 56, '15min': 18, '1h': 107 }[interval])
    assert.equal(await page.getByRole('tab').count(), 4)
    const response = await page.request.get(base + '/cta/cta-prep-binance-futures/config/api/config?worker=cta-factor-spread')
    assert.equal(response.status(), 200)
    assert.equal((await response.json()).worker, 'cta-factor-spread')
    checks.push({ interval, rules: rules.length, enabled: true, config_loaded: true, api_status: 200 })
  }
  try {
    tunnel = spawn('ssh', ['-N', '-o', 'BatchMode=yes', '-o', 'ExitOnForwardFailure=yes', '-L', `127.0.0.1:${port}:127.0.0.1:4191`, 'jp-meta-elvpn'], { stdio: 'ignore' })
    for (let i = 0; i < 60; i++) {
      try { await fetch(base + '/manager/'); break } catch { await delay(100) }
    }
    browser = await chromium.launch({ executablePath: '/home/fanghaizhou/.cache/ms-playwright/chromium-1228/chrome-linux64/chrome', headless: true, args: ['--no-sandbox', '--disable-dev-shm-usage', '--no-proxy-server'], env: { ...process.env, LD_LIBRARY_PATH: '/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu:/tmp/cta-browser-libs/usr/lib/x86_64-linux-gnu' } })
    const context = await contextFor('lgq')
    const page = await context.newPage()
    page.on('pageerror', error => errors.push(error.message))
    await page.goto(base + '/manager/accounts/')
    const accountLink = page.locator(`a[href="${prefix}1min"]`)
    await accountLink.waitFor()
    assert.equal(await accountLink.count(), 1)
    assert.equal(await accountLink.innerText(), '信号配置')
    await accountLink.click()
    await destinationVerified(page, '1min')
    for (const section of ['account', 'config/bindings']) {
      await page.goto(`${base}/manager/${section}/?source=${account}`)
      const links = page.getByRole('link', { name: '信号配置', exact: true })
      await page.waitForFunction(count => [...document.querySelectorAll('a')].filter(a => a.textContent.trim() === '信号配置').length === count, section === 'account' ? 5 : 4)
      assert.equal(await links.count(), section === 'account' ? 5 : 4)
      for (const interval of intervals) {
        const link = page.locator(`a[href="${prefix}${interval}"]`).last()
        assert.equal(await link.count(), 1)
        if (section === 'account') {
          await link.click()
          await destinationVerified(page, interval)
          await page.goto(`${base}/manager/account/?source=${account}`)
          await links.first().waitFor()
        }
      }
      const market = page.getByRole('combobox', { name: '交易市场', exact: true })
      await market.selectOption('binance_exec_spread01')
      await page.waitForFunction(count => [...document.querySelectorAll('a')].filter(a => a.textContent.trim() === '信号配置').length === count, section === 'account' ? 5 : 4)
      assert.equal(await links.count(), section === 'account' ? 5 : 4)
      for (const interval of intervals) assert.equal(await page.locator(`a[href="${prefix}${interval}"]`).last().count(), 1)
    }
    await page.setViewportSize({ width: 390, height: 844 })
    await page.goto(`${base}/manager/account/?source=${account}`)
    await page.getByRole('link', { name: '信号配置', exact: true }).first().waitFor()
    await page.getByRole('link', { name: '信号配置', exact: true }).first().scrollIntoViewIfNeeded()
    assert(await page.getByRole('link', { name: '信号配置', exact: true }).first().isVisible())
    // Prep account navigation keeps its existing worker and group.
    for (const [source, hash] of [['binance_exec_rx01', 'binance-futures_1min'], ['binance_exec_rx03', 'prep02_binance-futures_1min']]) {
      await page.goto(`${base}/manager/account/?source=${source}`)
      await page.locator(`a[href="/cta/cta-prep-binance-futures/config/?worker=cta-prep-binance-futures#${hash}"]`).first().waitFor()
    }
    const configApi = base + '/cta/cta-prep-binance-futures/config/api/config?worker=cta-prep-binance-futures'
    const prepResponse = await context.request.get(configApi)
    assert.equal(prepResponse.status(), 200)
    assert.equal((await prepResponse.json()).worker, 'cta-prep-binance-futures')
    const preview = base + '/cta/cta-prep-binance-futures/config/api/preview?worker=cta-factor-spread'
    assert.equal((await context.request.post(preview, { data: {} })).status(), 403)
    assert.equal((await context.request.post(preview, { data: {}, headers: { Origin: 'https://other.example' } })).status(), 403)
    assert.equal((await context.request.delete(preview, { headers: { Origin: 'https://other.example' } })).status(), 403)
    // Missing required fields rejects before any preview or persistence work.
    assert.equal((await context.request.post(preview, { data: {}, headers: { Origin: base } })).status(), 422)
    await context.close()
    await endAuth()
    const hidden = await contextFor('prc')
    const hiddenPage = await hidden.newPage()
    const response = hiddenPage.waitForResponse(r => new URL(r.url()).pathname === '/manager/api/dashboard')
    await hiddenPage.goto(base + '/manager/accounts/')
    const dashboard = await (await response).json()
    assert(!dashboard.accounts.some(a => a.source_id === account))
    await hiddenPage.getByRole('heading', { name: '账户入口', exact: true }).first().waitFor()
    assert.equal(await hiddenPage.locator('a[href*="worker=cta-factor-spread"]').count(), 0)
    assert.equal((await hidden.request.get(base + '/cta/cta-prep-binance-futures/config/?worker=cta-factor-spread')).status(), 403)
    await hidden.close()
    await endAuth()
    const anonymous = await browser.newContext()
    assert.equal((await anonymous.request.get(base + '/cta/cta-prep-binance-futures/config/?worker=cta-factor-spread')).status(), 401)
    await anonymous.close()
    assert.deepEqual(errors, [])
    assert.deepEqual(blockedWrites, [])
    const result = { live, checks, account_button: true, overview_buttons: 5, binding_buttons_per_market: 4, both_execution_markets: true, mobile_button_visible: true, prep_links_preserved: true, hidden_account_links_absent: true, manager_only_login: true, anonymous_status: 401, unprivileged_status: 403, missing_or_cross_origin_write_status: 403, malformed_same_origin_preview_status: 422, config_saves: 0, errors }
    await fs.writeFile(path.join(__dirname, live ? 'live-browser.json' : 'candidate-browser.json'), JSON.stringify(result, null, 2) + '\n')
    process.stdout.write(JSON.stringify(result) + '\n')
  } finally {
    await browser?.close()
    await endAuth()
    tunnel?.kill('SIGTERM')
  }
})().catch(error => { console.error(error); process.exitCode = 1 })
