/* Read-only theoretical portfolio simulations against the deployed UI. */
"use strict";
const assert=require("node:assert/strict"),fs=require("node:fs"),path=require("node:path");
const {chromium}=require(process.env.PLAYWRIGHT_NODE_MODULE||"playwright");
(async()=>{
 const base=process.argv[2]||"http://127.0.0.1:10242/",output=process.argv[3];
 if(output)fs.mkdirSync(output,{recursive:true});
 const launch={headless:true,args:["--no-sandbox"]};if(process.env.BROWSER_BINARY)launch.executablePath=process.env.BROWSER_BINARY;
 const browser=await chromium.launch(launch);
 try{
  const page=await browser.newPage({viewport:{width:1500,height:1050}}),errors=[];
  page.on("pageerror",e=>errors.push(e.message));
  await page.goto(base,{waitUntil:"networkidle"});
  await page.waitForFunction(()=>document.querySelectorAll("#strategies tr").length===12);
  assert.equal(await page.locator("#symbol").count(),0);assert.equal(await page.locator("#factor").count(),0);
  assert.equal(await page.locator("#group-totals section").count(),2);
  await page.waitForFunction(()=>!document.querySelector("#run").disabled);
  assert.equal(await page.locator("#parameters").evaluate(form=>form.checkValidity()),true);
  await page.locator("#freeze").click();await page.locator("#entry-long").fill("0.85");
  const [response]=await Promise.all([page.waitForResponse(r=>r.url().endsWith('/api/replay')&&JSON.parse(r.request().postData()||'{}').thresholds?.long_entry===0.85),page.locator('#run').click()]);
  assert.equal(response.status(),200);const result=await response.json();
  assert.equal(result.summary.rules,338);assert.equal(result.strategies.length,12);assert.equal(result.groups.length,2);
  assert.equal(result.price_method,'next_closed_bar_vwap');assert.equal(result.parameters.fee_rate,0);
  await page.waitForFunction(()=>!document.querySelector('#run').disabled);
  await page.locator('#save').click();assert.equal(await page.locator('#comparisons tr').count(),1);
  await page.locator('#strategy-group').selectOption('prep02');
  await page.waitForFunction(()=>document.querySelectorAll('#strategies tr').length===9);
  await page.locator('#symbol-group').selectOption('top-20');
  await page.waitForFunction(()=>document.querySelectorAll('#strategies tr').length===3);
  await page.locator('#interval').selectOption('5m');
  await page.waitForFunction(()=>document.querySelectorAll('#strategies tr').length===1&&document.querySelector('#strategies').textContent.includes('空策略'));
  await page.locator('#strategy-group').selectOption('*');await page.locator('#symbol-group').selectOption('*');await page.locator('#interval').selectOption('*');
  await page.waitForFunction(()=>document.querySelectorAll('#strategies tr').length===12);
  assert.deepEqual(errors,[]);
  if(output)await page.screenshot({path:path.join(output,'desktop.png'),fullPage:true});
  await page.setViewportSize({width:390,height:844});await page.waitForTimeout(150);
  assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false);
  if(output)await page.screenshot({path:path.join(output,'mobile.png'),fullPage:true});
  console.log(JSON.stringify({portfolio_profiles:12,rules:338,groups:2,price_method:result.price_method,javascript_errors:errors,mobile_overflow:false}));
 }finally{await browser.close();}
})().catch(e=>{console.error(e);process.exitCode=1;});
