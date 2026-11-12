// Mock-only browser checks. No request leaves the local HTTP server.
// PLAYWRIGHT_MODULE=/path/to/playwright/index.mjs node smoke.mjs
import assert from 'node:assert/strict';
import { createServer } from 'node:http';
import { readFile } from 'node:fs/promises';
const { chromium } = await import(process.env.PLAYWRIGHT_MODULE || 'playwright');
const server = createServer(async (request, response) => {
  const name = request.url === '/delist/transfers/' ? 'index.html' : request.url.split('/').pop();
  if (!['index.html', 'app.js', 'app.css'].includes(name)) { response.writeHead(404).end(); return; }
  response.setHeader('Content-Type', name.endsWith('.js') ? 'application/javascript' : name.endsWith('.css') ? 'text/css' : 'text/html');
  response.end(await readFile(new URL(`../dist/${name}`, import.meta.url)));
});
await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
const origin = `http://127.0.0.1:${server.address().port}`;
const browser = await chromium.launch({ headless: true, ...(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {}) });
const stamp = new Date().toISOString();
const accounts = [
  { id: '90071992547409931234', name: '测试转出组合', kind: 'portfolio', venue: 'Binance', account_types: ['U_MARGINED_FUTURES'] },
  { id: '90071992547409935678', name: '测试转入组合', kind: 'portfolio', venue: 'Binance', account_types: ['U_MARGINED_FUTURES'] },
];
const currencies = [{ currency: 'USDT', networks: ['TRX'] }];
const amount = '123.123456789012345678';
async function fixture({ transportLoss = false, expired = false, unauthorized = false, rejected = false, requiresNetwork = false } = {}) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
  page.setDefaultTimeout(10000);
  let postCount = 0, previewCount = 0, queryCount = 0, submitted;
  const errors = []; page.on('pageerror', error => errors.push(error.message));
  await page.route('**/*', async route => {
    const request = route.request(), url = new URL(request.url());
    assert.equal(url.origin, origin, 'all requests remain local');
    if (!url.pathname.includes('/api/')) return route.continue();
    const path = url.pathname.split('/api/')[1];
    const json = data => route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(data) });
    if (unauthorized) return route.fulfill({ status: 401, contentType: 'application/json', body: JSON.stringify({ error: 'login required' }) });
    if (path === 'catalog') return json({ accounts, currencies, updated_at: stamp, warnings: [] });
    if (path === 'routes') return json({ currencies, requires_network: requiresNetwork, updated_at: stamp, warnings: [] });
    if (path === 'balances') return json({ balances: [{ currency: 'USDT', total: '18000.00', available: '12500.12345678' }], updated_at: stamp });
    if (path === 'transfers/preview') {
      previewCount++;
      submitted = request.postDataJSON();
      assert.equal(submitted.amount, amount);
      assert.equal(submitted.from_account_id, accounts[0].id);
      return json({ preview_id: 'test-preview-uuid', expires_at: new Date(Date.now() + (expired ? -1000 : 60000)).toISOString(), request: submitted, from_name: accounts[0].name, to_name: accounts[1].name, available: '12500.12345678', warnings: [] });
    }
    const record = { ...submitted, client_order_id: 'test-preview-uuid', status: 'pending', amount_received: '120.00', network_fee: '0', fee_currency: 'USDT', rapid_transfer: false, requested_rapid_transfer: true, created_at: stamp, updated_at: stamp, actor: 'test' };
    if (path === 'transfers' && request.method() === 'POST') {
      postCount++;
      assert.deepEqual(request.postDataJSON(), { preview_id: 'test-preview-uuid' });
      if (transportLoss) return route.abort('failed');
      if (rejected) return route.fulfill({ status: 409, contentType: 'application/json', body: JSON.stringify({ error: '余额已变化', submitted: false, preview_id: 'test-preview-uuid' }) });
      await new Promise(resolve => setTimeout(resolve, 150));
      return json(record);
    }
    if (path === 'transfers') return json({ records: postCount && !transportLoss && !rejected ? [record] : [], updated_at: stamp, warnings: ['测试：LTP 外部记录暂不可用'] });
    if (path === 'transfers/test-preview-uuid') { queryCount++; return json({ ...record, status: transportLoss ? 'unknown' : 'pending', ...(transportLoss ? { amount_received: null, network_fee: null, fee_currency: null, rapid_transfer: null } : {}) }); }
    throw new Error(`Unmocked request: ${path}`);
  });
  await page.goto(`${origin}/delist/transfers/`);
  return { page, errors, counters: () => ({ postCount, previewCount, queryCount }) };
}
async function chooseAccounts(page) {
  for (const [label, account] of [['转出账户', accounts[0]], ['转入账户', accounts[1]]]) {
    const select = page.getByRole('combobox', { name: label, exact: true });
    await select.fill(account.name);
    await page.locator('.ant-select-item-option-content:visible').filter({ hasText: account.name }).last().waitFor();
    await select.press('Enter');
  }
}
async function fill(page) {
  await chooseAccounts(page);
  await page.getByRole('textbox', { name: '划转数量' }).fill(amount);
  await page.getByRole('button', { name: '预览划转', exact: true }).click();
  await page.getByRole('dialog').waitFor();
}
try {
  const first = await fixture();
  await fill(first.page);
  assert.equal(first.counters().postCount, 0, 'preview never submits');
  assert.match(await first.page.getByRole('dialog').innerText(), /美元本位合约/);
  assert.match(await first.page.getByRole('dialog').innerText(), /U_MARGINED_FUTURES/);
  assert.match(await first.page.getByRole('dialog').innerText(), new RegExp(amount.replaceAll('.', '\\.')));
  await first.page.getByRole('button', { name: '确认执行划转' }).dblclick();
  await first.page.getByText('请求已受理，等待确认结果', { exact: true }).waitFor();
  assert.equal(first.counters().postCount, 1, 'double click sends one POST');
  assert.equal(await first.page.getByRole('button', { name: '预览划转', exact: true }).isDisabled(), true, 'pending blocks replacement');
  await first.page.getByText('测试：LTP 外部记录暂不可用', { exact: true }).waitFor();
  await first.page.locator('.ant-table-row-expand-icon').first().click();
  const details = await first.page.locator('.record-detail').innerText();
  assert.match(details, /实际到账\s+120.00 USDT/);
  assert.match(details, /实际网络费用\s+0 USDT/);
  assert.match(details, /实际划转方式\s+普通划转/);
  assert.match(details, /提交时的划转偏好\s+优先快速划转/);
  assert.deepEqual(first.errors, []);
  await first.page.close();

  const lost = await fixture({ transportLoss: true });
  await fill(lost.page);
  await lost.page.getByRole('button', { name: '确认执行划转' }).click();
  await lost.page.getByText(/提交结果尚未确认/).waitFor();
  await lost.page.reload();
  await lost.page.getByRole('button', { name: '查询原操作' }).click();
  await lost.page.getByText('结果未知', { exact: true }).waitFor();
  assert.equal(lost.counters().postCount, 1, 'transport loss and reload never retry POST');
  assert.ok(lost.counters().queryCount >= 2);
  assert.equal(await lost.page.getByRole('button', { name: '预览划转', exact: true }).isDisabled(), true);
  await lost.page.locator('.ant-table-row-expand-icon').first().click();
  const unknownDetails = await lost.page.locator('.record-detail').innerText();
  assert.match(unknownDetails, /实际到账\s+未返回/);
  assert.match(unknownDetails, /实际网络费用\s+未返回/);
  assert.match(unknownDetails, /实际划转方式\s+未返回 \/ 待确认/);
  assert.deepEqual(lost.errors, []);
  await lost.page.close();

  const expired = await fixture({ expired: true });
  await fill(expired.page);
  assert.equal(await expired.page.getByRole('button', { name: '确认执行划转' }).isDisabled(), true);
  assert.equal(expired.counters().postCount, 0);
  await expired.page.close();

  const rejected = await fixture({ rejected: true });
  await fill(rejected.page);
  await rejected.page.getByRole('button', { name: '确认执行划转' }).click();
  await rejected.page.getByText(/划转未提交/).waitFor();
  assert.equal(await rejected.page.evaluate(() => sessionStorage.getItem('ltp-transfer-attempt')), null, 'explicit unsubmitted rejection clears lock');
  assert.equal(rejected.counters().postCount, 1);
  await rejected.page.close();

  const network = await fixture({ requiresNetwork: true });
  await chooseAccounts(network.page);
  await network.page.getByRole('textbox', { name: '划转数量' }).fill(amount);
  await network.page.getByRole('button', { name: '预览划转', exact: true }).click();
  await network.page.getByText('请选择双方支持的划转网络', { exact: true }).waitFor();
  assert.equal(network.counters().previewCount, 0, 'required network validated before preview');
  await network.page.getByRole('combobox', { name: '划转网络', exact: true }).click();
  await network.page.locator('.ant-select-item-option:visible').filter({ hasText: 'TRX' }).click();
  await network.page.getByRole('button', { name: '预览划转', exact: true }).click();
  await network.page.getByRole('dialog').waitFor();
  assert.match(await network.page.getByRole('dialog').innerText(), /TRX/);
  await network.page.close();

  const visuals = await fixture();
  await chooseAccounts(visuals.page);
  await visuals.page.locator('.balance-number').first().waitFor();
  await visuals.page.getByRole('heading', { name: '资金划转' }).click();
  await visuals.page.waitForFunction(() => document.querySelectorAll('.ant-select-dropdown:not(.ant-select-dropdown-hidden)').length === 0);
  await visuals.page.getByRole('button', { name: '预览划转', exact: true }).waitFor();
  if (process.env.SCREENSHOT_DIR) await visuals.page.screenshot({ path: `${process.env.SCREENSHOT_DIR}/desktop.png`, fullPage: true });
  await visuals.page.setViewportSize({ width: 390, height: 844 });
  assert.equal(await visuals.page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'mobile page has no horizontal overflow');
  if (process.env.SCREENSHOT_DIR) await visuals.page.screenshot({ path: `${process.env.SCREENSHOT_DIR}/mobile.png`, fullPage: true });
  assert.deepEqual(visuals.errors, []);
  await visuals.page.close();

  const auth = await fixture({ unauthorized: true });
  await auth.page.waitForURL('**/delist/login');
  await auth.page.close();
  console.log('PASS: precise amount/account IDs, preview-only, single POST, pending lock, transport-loss recovery, history warnings/factual details, Chinese types, expiry, definitive rejection, required network, responsive layout, local assets, login redirect.');
} catch (error) { const lastPage = browser.contexts().flatMap(context => context.pages()).at(-1); if (lastPage && process.env.SCREENSHOT_DIR) { await lastPage.screenshot({ path: `${process.env.SCREENSHOT_DIR}/failure.png`, fullPage: true }); console.error((await lastPage.locator('body').innerText()).slice(0,5000)); } throw error; } finally { await browser.close(); await new Promise(resolve => server.close(resolve)); }
