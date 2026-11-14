import { createRequire } from 'node:module';
import { writeFile } from 'node:fs/promises';
const require=createRequire('/tmp/crypto-nav-playwright-u171/package.json');
const {chromium}=require('playwright');
const browser=await chromium.launch({headless:true,executablePath:process.env.CHROMIUM_PATH||'/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome'});
const results=[];
try {
 for(const width of [1440,390]) {
  const page=await browser.newPage({viewport:{width,height:900}}),errors=[];
  page.on('pageerror',e=>errors.push(e.message));
  await page.goto('http://127.0.0.1:29320/?worker=cta-prep-binance-futures#top-5_prep02_binance-futures_1min');
  await page.waitForFunction(()=>document.querySelector('#message').textContent==='读取完成');
  const before=await page.evaluate(()=>catalog.revision);
  const checks=[];
  for(const group of ['top-5','top-20','last-20']) {
   await page.selectOption('#symbol-group',group);
   for(const period of ['1min','5min','15min']) {
    await page.click(`[data-interval="${period}"]`);
    const check=await page.evaluate(()=>({group:document.querySelector('#symbol-group').value,period:draft.interval,count:JSON.parse(document.querySelector('#factor-json').value).length,max:document.querySelector('#max-pos').value,enabled:document.querySelector('#enabled').value}));
    if(check.group!==group || check.period!==period || check.max!=='100' || check.enabled!=='true') throw Error('unexpected live settings '+JSON.stringify(check));
    if(group==='top-20'&&period==='5min'&&check.count!==0)throw Error('empty strategy is not empty');
    checks.push(check);
   }
  }
  await page.selectOption('#strategy-type','cta-factor-spread');
  await page.waitForFunction(()=>catalog.worker==='cta-factor-spread' && !busy);
  const spread=await page.evaluate(()=>({group:document.querySelector('#strategy-group').value,max:document.querySelector('#max-pos').value,enabled:document.querySelector('#enabled').value}));
  if(spread.group!=='spread01'||spread.max!=='100'||spread.enabled!=='true')throw Error('spread settings differ');
  const overflow=await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth);
  if(overflow||errors.length)throw Error('browser errors '+JSON.stringify({overflow,errors}));
  const after=(await (await page.request.get('http://127.0.0.1:29320/api/config?worker=cta-prep-binance-futures')).json()).revision;
  if(before!==after)throw Error('live catalog changed during read-only check');
  results.push({width,checks,spread,overflow,errors,read_only:true});await page.close();
 }
 await writeFile(new URL('./browser-verified.json',import.meta.url),JSON.stringify(results,null,2)+'\n');
 console.log(JSON.stringify({desktop_mobile_passed:true,strategies_checked:9,production_writes:0}));
}finally{await browser.close();}
