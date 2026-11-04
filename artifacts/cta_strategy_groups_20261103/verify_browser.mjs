import {spawn} from 'node:child_process';
import {mkdtemp,cp,readFile,writeFile,rm,mkdir} from 'node:fs/promises';
import {createServer} from 'node:net';
import {setTimeout as delay} from 'node:timers/promises';
import assert from 'node:assert/strict';
import path from 'node:path';
const root=process.cwd();
const temp=await mkdtemp('/tmp/cta-groups-ui-');
let server,chrome,ws;
try{
 await mkdir(path.join(temp,'config'));
 for(const name of ['cta_prep_binance_futures','cta_factor_spread']){
  await cp(path.join(root,'config',name),path.join(temp,'config',name),{recursive:true});
  await cp(path.join(root,'config',name+'.toml'),path.join(temp,'config',name+'.toml'));
 }
 const port=await new Promise(resolve=>{const s=createServer();s.listen(0,'127.0.0.1',()=>{const p=s.address().port;s.close(()=>resolve(p));});});
 const base=`http://127.0.0.1:${port}`;
 server=spawn(path.join(root,'target/debug/cta-factor-config'),['--config',path.join(temp,'config/cta_prep_binance_futures.toml'),'--spread-config',path.join(temp,'config/cta_factor_spread.toml'),'--port',String(port)],{cwd:temp,stdio:'ignore'});
 for(let i=0;i<100;i++){try{if((await fetch(base+'/api/config')).ok)break;}catch{}await delay(100);}
 const profile=path.join(temp,'profile');
 chrome=spawn('/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',['--headless','--no-sandbox','--disable-dev-shm-usage','--disable-background-networking','--no-proxy-server','--remote-debugging-port=0',`--user-data-dir=${profile}`,'about:blank'],{stdio:'ignore',env:{...process.env,LD_LIBRARY_PATH:'/tmp/playwright-libs/usr/lib/x86_64-linux-gnu:/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu'}});
 let debug;
 for(let i=0;i<100;i++){try{debug=(await readFile(path.join(profile,'DevToolsActivePort'),'utf8')).split('\n');break;}catch{}await delay(100);}
 assert(debug,'Chromium startup failed');
 ws=new WebSocket(`ws://127.0.0.1:${debug[0]}${debug[1]}`);
 await new Promise((resolve,reject)=>{ws.addEventListener('open',resolve,{once:true});ws.addEventListener('error',reject,{once:true});});
 let id=0;const pending=new Map(),errors=[];
 ws.addEventListener('message',({data})=>{const m=JSON.parse(String(data));if(m.id){const p=pending.get(m.id);pending.delete(m.id);if(m.error)p?.reject(new Error(m.error.message));else p?.resolve(m.result);}else if(m.method==='Runtime.exceptionThrown')errors.push(m.params.exceptionDetails.text);});
 const send=(method,params={},sessionId)=>new Promise((resolve,reject)=>{const n=++id;pending.set(n,{resolve,reject});ws.send(JSON.stringify({id:n,method,params,...(sessionId?{sessionId}:{})}));});
 const {targetId}=await send('Target.createTarget',{url:'about:blank'});
 const {sessionId}=await send('Target.attachToTarget',{targetId,flatten:true});
 const call=(method,params)=>send(method,params,sessionId);
 for(const method of ['Page.enable','Runtime.enable','Network.enable'])await call(method);
 await call('Page.addScriptToEvaluateOnNewDocument',{source:'window.confirm=()=>true;'});
 const evaluate=async expression=>{const r=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(r.exceptionDetails)throw new Error(JSON.stringify(r.exceptionDetails));return r.result.value;};
 const waitFor=async exp=>{for(let i=0;i<100;i++){if(await evaluate(exp))return;await delay(100);}throw new Error('UI timeout: '+exp);};
 const idle="document.querySelector('main')?.getAttribute('aria-busy')==='false'";
 await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1000,deviceScaleFactor:1,mobile:false});
 const results=[];
 for(const [worker,first,combination,rows] of [
  ['cta-prep-binance-futures','prep01','binance-futures_15min',146],
  ['cta-factor-spread','spread01','okex-futures_binance-futures_1min',34]
 ]){
  await call('Page.navigate',{url:base+`/?worker=${worker}#${combination}`});
  await waitFor(`${idle} && document.querySelector('#name')?.value===${JSON.stringify(combination)}`);
  assert.equal(await evaluate("document.querySelector('#group-select').value"),first);
  assert.equal(await evaluate("document.querySelector('#strategy-group').readOnly"),true);
  const before=await(await fetch(base+`/api/config?worker=${worker}`)).json();
  const count=before.unique_factor_count,existingGroups=new Set(Object.values(before.plan.combinations).map(c=>c.strategy_group)),prefix=first.replace(/\d+$/,'');let index=1;while(existingGroups.has(prefix+String(index).padStart(2,'0')))index++;const second=prefix+String(index).padStart(2,'0');
  await evaluate("document.querySelector('#copy-group').click()");
  assert.equal(await evaluate("document.querySelector('#strategy-group').value"),second);
  assert.equal(await evaluate("document.querySelector('#strategy-group').readOnly"),false);
  assert.equal(await evaluate("document.querySelector('#enabled').checked"),false);
  assert.equal(await evaluate("document.querySelectorAll('#rows tr').length"),rows);
  const copy=await evaluate("document.querySelector('#name').value");
  await evaluate("document.querySelector('#max-pos').value='321';document.querySelector('#save').click()");
  await waitFor(`${idle} && document.querySelector('#message').textContent.includes('组合已保存')`);
  const after=await(await fetch(base+`/api/config?worker=${worker}`)).json();
  assert.equal(after.unique_factor_count,count);
  assert.deepEqual(after.plan.combinations[combination],before.plan.combinations[combination]);
  assert.equal(after.plan.combinations[copy].signal.max_pos_u,321);
  assert.equal(after.plan.combinations[copy].signal.enabled,false);
  assert.equal(await evaluate("document.querySelectorAll('#group-select option').length"),existingGroups.size+1);
  assert.equal(await evaluate("document.querySelector('#group-select').value"),second);
  assert.equal(await evaluate("document.querySelectorAll('#combination-list option').length"),1);
  const identity=after.position_strategies[copy];
  assert(identity.startsWith('cta-'+second+'-'));
  assert((await evaluate("document.querySelector('#manager-strategy').textContent")).includes(identity));
  await evaluate(`document.querySelector('#group-select').value=${JSON.stringify(first)};document.querySelector('#group-select').dispatchEvent(new Event('change'));`);
  assert.equal(await evaluate("document.querySelector('#strategy-group').value"),first);
  await evaluate(`document.querySelector('#group-select').value=${JSON.stringify(second)};document.querySelector('#group-select').dispatchEvent(new Event('change'));document.querySelector('#delete').click();`);
  await waitFor(`${idle} && document.querySelector('#message').textContent.includes('组合已删除')`);
  const remaining=await(await fetch(base+`/api/config?worker=${worker}`)).json();
  assert.deepEqual(remaining.plan.combinations,before.plan.combinations);
  assert.equal(remaining.unique_factor_count,count);
  results.push({worker,groups:[first,second],shared_calculations:count,copy_disabled:true,independent_save_and_delete:true});
 }
 await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:false});
 assert(await evaluate('document.documentElement.scrollWidth<=innerWidth'),'mobile overflow');
 assert.deepEqual(errors,[]);
 const out=path.join(root,'artifacts/cta_strategy_groups_20261103');await mkdir(out,{recursive:true});
 const result={local_only:true,results,script_errors:errors,mobile_width:390};
 await writeFile(path.join(out,'browser-verification.json'),JSON.stringify(result,null,2)+'\n');
 const screenshot=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});
 await writeFile(path.join(out,'mobile.png'),Buffer.from(screenshot.data,'base64'));
 console.log(JSON.stringify(result));
}finally{ws?.close();chrome?.kill('SIGTERM');server?.kill('SIGTERM');await delay(300);await rm(temp,{recursive:true,force:true});}
