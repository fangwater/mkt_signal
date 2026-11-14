// Render the published page with its real WS feeds over authorized SSH tunnels.
import { spawn } from 'node:child_process';
import { readFile, writeFile, mkdtemp, rm } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createServer as netServer } from 'node:net';
import { setTimeout as delay } from 'node:timers/promises';
import assert from 'node:assert/strict';
const dir = new URL('./', import.meta.url).pathname;
const freePort = () => new Promise(resolve => {
  const s = netServer(); s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); });
});
const profile = await mkdtemp('/tmp/fr03-browser-');
const vizPort = await freePort(), frPort = await freePort();
let tunnel, chrome, ws, server;
try {
  tunnel = spawn('ssh', ['-o', 'BatchMode=yes', '-o', 'ExitOnForwardFailure=yes', '-N',
    '-L', `127.0.0.1:${vizPort}:127.0.0.1:20133`, '-L', `127.0.0.1:${frPort}:127.0.0.1:20143`, 'jp-meta-elvpn'], {stdio: 'ignore'});
  const html = await readFile(dir + 'published_index.html');
  server = createServer((req, res) => { res.setHeader('Content-Type', 'text/html; charset=utf-8'); res.end(html); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  chrome = spawn('/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome', [
    '--headless', '--no-sandbox', '--disable-dev-shm-usage', '--disable-background-networking', '--no-proxy-server',
    '--remote-debugging-port=0', `--user-data-dir=${profile}`, 'about:blank'], {stdio: 'ignore', env: {...process.env,
      LD_LIBRARY_PATH: '/tmp/playwright-libs/usr/lib/x86_64-linux-gnu:/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu'}});
  let debug;
  for (let i = 0; i < 100; i++) { try { debug = (await readFile(profile + '/DevToolsActivePort', 'utf8')).split('\n'); break; } catch { await delay(100); } }
  assert(debug, 'Chromium startup');
  ws = new WebSocket(`ws://127.0.0.1:${debug[0]}${debug[1]}`);
  await new Promise((resolve, reject) => { ws.addEventListener('open', resolve, {once: true}); ws.addEventListener('error', reject, {once: true}); });
  let sequence = 0;
  const pending = new Map(), exceptions = [];
  ws.addEventListener('message', ({data}) => {
    const m = JSON.parse(String(data));
    if (m.id) { const p = pending.get(m.id); pending.delete(m.id); m.error ? p.reject(new Error(m.error.message)) : p.resolve(m.result); }
    else if (m.method === 'Runtime.exceptionThrown') exceptions.push(m.params.exceptionDetails.text);
  });
  const send = (method, params = {}, sessionId) => new Promise((resolve, reject) => {
    const id = ++sequence; pending.set(id, {resolve, reject}); ws.send(JSON.stringify({id, method, params, ...(sessionId ? {sessionId} : {})}));
  });
  const {targetId} = await send('Target.createTarget', {url: 'about:blank'});
  const {sessionId} = await send('Target.attachToTarget', {targetId, flatten: true});
  const call = (method, params) => send(method, params, sessionId);
  const evaluate = async expression => {
    const r = await call('Runtime.evaluate', {expression, returnByValue: true});
    assert(!r.exceptionDetails, JSON.stringify(r.exceptionDetails)); return r.result.value;
  };
  await call('Page.enable'); await call('Runtime.enable');
  await call('Emulation.setDeviceMetricsOverride', {width: 1440, height: 1000, deviceScaleFactor: 1, mobile: false});
  const url = `http://127.0.0.1:${server.address().port}/?ws=http://127.0.0.1:${vizPort}/ws&fr_ws=http://127.0.0.1:${frPort}/ws`;
  await call('Page.navigate', {url});
  let ready = false;
  for (let i = 0; i < 100; i++) {
    ready = await evaluate(`document.getElementById('risk-usdt-buy')?.textContent !== '--' && document.getElementById('risk-usdt-buy')?.textContent !== undefined`);
    if (ready) break; await delay(300);
  }
  assert(ready, 'Live capacity snapshot rendered');
  const values = await evaluate(`Object.fromEntries(['risk-usdt-max-margin','risk-usdt-cash','risk-usdt-borrow','risk-usdt-buy','risk-usdt-state'].map(id=>[id,document.getElementById(id).textContent]))`);
  assert(Number(values['risk-usdt-max-margin'].replaceAll(',', '')) > 0);
  assert(!await evaluate(`document.getElementById('metric-usdt-cash').hidden`));
  await writeFile(dir + 'desktop.png', Buffer.from((await call('Page.captureScreenshot', {format: 'png'})).data, 'base64'));
  await call('Emulation.setDeviceMetricsOverride', {width: 390, height: 844, deviceScaleFactor: 1, mobile: true});
  await delay(300);
  await writeFile(dir + 'mobile.png', Buffer.from((await call('Page.captureScreenshot', {format: 'png'})).data, 'base64'));
  assert.deepEqual(exceptions, []);
  await writeFile(dir + 'browser_result.json', JSON.stringify({utc: new Date().toISOString(), values, exceptions}, null, 2));
  console.log(JSON.stringify({values, exceptions}));
} finally {
  ws?.close(); chrome?.kill(); tunnel?.kill(); server?.close();
  await delay(300); await rm(profile, {recursive: true, force: true});
}
