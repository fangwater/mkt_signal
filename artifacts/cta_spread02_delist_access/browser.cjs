// Read-only navigation regression: actual editor and catalog via a private SSH tunnel.
const {chromium}=require('/home/fanghaizhou/front_queue_research/node_modules/playwright');
const {spawn}=require('node:child_process');
const {createServer}=require('node:net');
const fs=require('node:fs/promises');
const assert=require('node:assert/strict');
const {setTimeout:delay}=require('node:timers/promises');
const root=__dirname;
(async()=>{
  let browser,tunnel;
  try{
    const port=await new Promise(resolve=>{const s=createServer();s.listen(0,'127.0.0.1',()=>{const p=s.address().port;s.close(()=>resolve(p));});});
    tunnel=spawn('ssh',['-N','-o','BatchMode=yes','-o','ExitOnForwardFailure=yes','-L',`127.0.0.1:${port}:127.0.0.1:19220`,'jp-meta-elvpn'],{stdio:'ignore'});
    const backend=`http://127.0.0.1:${port}`;
    for(let i=0;i<100;i++){try{if((await fetch(backend+'/api/config?worker=cta-factor-spread')).ok)break;}catch{}await delay(100);}
    const board=await fs.readFile(root+'/../../web/delist_risk/index.html','utf8');
    const mapping=await fs.readFile(root+'/../cta_spread02_deployment/delist_mapping_final.json','utf8');
    browser=await chromium.launch({executablePath:'/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',headless:true,args:['--no-sandbox','--disable-dev-shm-usage','--no-proxy-server'],env:{...process.env,LD_LIBRARY_PATH:'/tmp/playwright-libs/usr/lib/x86_64-linux-gnu:/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu:/tmp/cta-browser-libs/usr/lib/x86_64-linux-gnu'}});
    const context=await browser.newContext();
    let blockedWrites=0;const errors=[];
    await context.route('http://fixture.test/**',async route=>{
      const request=route.request(),url=new URL(request.url());
      if(!['GET','HEAD'].includes(request.method())){blockedWrites++;return route.abort();}
      if(url.pathname==='/delist/')return route.fulfill({contentType:'text/html',body:board});
      if(url.pathname==='/delist/cta-strategies')return route.fulfill({contentType:'application/json',body:mapping});
      if(url.pathname==='/delist/status')return route.fulfill({contentType:'application/json',body:'{"sources":[]}'});
      const prefix='/delist/cta/config/';
      if(url.pathname.startsWith(prefix)){
        const response=await fetch(backend+'/'+url.pathname.slice(prefix.length)+url.search);
        return route.fulfill({status:response.status,headers:{'Content-Type':response.headers.get('content-type')||'text/plain'},body:await response.text()});
      }
      return route.fulfill({status:404,body:''});
    });
    const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));
    await page.goto('http://fixture.test/delist/?view=cta');
    await page.locator('tr[data-strategy="cta-spread02-okx-bn-1min-pos"]').waitFor();
    const links=await page.locator('a[href*="#spread02_"]').evaluateAll(nodes=>nodes.map(n=>n.getAttribute('href')));
    assert(links.length>=8&&links.every(link=>link.startsWith('/delist/cta/config/')));
    await page.getByRole('link',{name:'Spread02 配置 ↗',exact:true}).click();
    await page.locator('main[aria-busy="false"]').waitFor();
    assert.equal(await page.locator('#strategy-group').inputValue(),'spread02');
    const checks=[];
    for(const [interval,count] of [['1min',48],['5min',79],['15min',64],['1h',128]]){
      await page.getByRole('tab').filter({has:page.locator(`small`)}).filter({hasText:interval}).first().click();
      await page.waitForFunction(f=>document.querySelector('#current-frequency').textContent===f,interval);
      const rows=JSON.parse(await page.locator('#factor-json').inputValue());
      assert.equal(rows.length,count);assert.equal(await page.locator('#enabled').inputValue(),'false');
      assert.equal(await page.locator('#max-pos').inputValue(),'100');
      assert((await page.locator('#factor-hint').textContent()).includes('OKX 永续'));
      checks.push({interval,rules:rows.length,enabled:false,max_pos_u:100});
    }
    assert.deepEqual(errors,[]);assert.equal(blockedWrites,0);
    const report={read_only:true,auth_test:'isolated Nginx tested separately',spread02_board_links:links.length,checks,errors,blockedWrites};
    await fs.writeFile(root+'/browser-verification.json',JSON.stringify(report,null,2)+'\n');
    console.log(JSON.stringify(report));
  }finally{await browser?.close();tunnel?.kill('SIGTERM');}
})().catch(e=>{console.error(e);process.exitCode=1;});
