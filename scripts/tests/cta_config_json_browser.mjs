// Exercise the frequency editor against disposable local catalogs; no Kafka or accounts.
import {spawn} from 'node:child_process';
import {mkdtemp,cp,readFile,writeFile,rm,mkdir} from 'node:fs/promises';
import {createServer} from 'node:net';
import {setTimeout as delay} from 'node:timers/promises';
import assert from 'node:assert/strict';
import path from 'node:path';

const root=process.cwd();
const output=path.resolve(process.argv[2]||'artifacts/cta_config_simplify_20261103');
const temporary=await mkdtemp('/tmp/cta-json-editor-');
let server,chrome,ws;
try {
  await mkdir(path.join(temporary,'config'));
  for(const name of ['cta_prep_binance_futures','cta_factor_spread']){
    await cp(path.join(root,'config',name),path.join(temporary,'config',name),{recursive:true});
    await cp(path.join(root,'config',name+'.toml'),path.join(temporary,'config',name+'.toml'));
  }
  const port=await new Promise(resolve=>{const socket=createServer();socket.listen(0,'127.0.0.1',()=>{const port=socket.address().port;socket.close(()=>resolve(port));});});
  const base=`http://127.0.0.1:${port}`;
  server=spawn(path.join(root,'target/debug/cta-factor-config'),['--config',path.join(temporary,'config/cta_prep_binance_futures.toml'),'--spread-config',path.join(temporary,'config/cta_factor_spread.toml'),'--port',String(port)],{cwd:temporary,stdio:'ignore'});
  for(let i=0;i<100;i++){try{if((await fetch(base+'/api/config')).ok)break;}catch{}await delay(100);}
  const api=async(worker,route='config',body)=>{
    const response=await fetch(base+'/api/'+route+'?worker='+worker,body?{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)}:{});
    const data=await response.json();assert(response.ok,JSON.stringify(data));return data;
  };
  const prep='cta-prep-binance-futures',spread='cta-factor-spread',target='prep02_binance-futures_5min';
  let baseline=await api(prep),fixture=structuredClone(baseline.plan.combinations[target]);
  const [factorId,calculation]=Object.entries(baseline.plan.calculations).find(([,c])=>c.combinations.includes(target)&&c.id.factor!=='nq');
  fixture.parameters={[factorId]:calculation.parameters};
  fixture.signal.symbol_overrides={[fixture.factor_rules[0]['标的']]:{factor_long_quantile:0.95}};
  await api(prep,'combinations',{worker:prep,combination:fixture,expected_revision:baseline.revision});
  baseline=await api(prep);

  const profile=path.join(temporary,'chromium');
  chrome=spawn('/home/fanghaizhou/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',['--headless','--no-sandbox','--disable-dev-shm-usage','--disable-background-networking','--no-proxy-server','--remote-debugging-port=0',`--user-data-dir=${profile}`,'about:blank'],{stdio:'ignore',env:{...process.env,LD_LIBRARY_PATH:'/tmp/playwright-libs/usr/lib/x86_64-linux-gnu:/tmp/cta-pair-browser-deps/root/usr/lib/x86_64-linux-gnu'}});
  let debug;
  for(let i=0;i<100;i++){try{debug=(await readFile(path.join(profile,'DevToolsActivePort'),'utf8')).split('\n');break;}catch{}await delay(100);}
  assert(debug,'Chromium startup failed');ws=new WebSocket(`ws://127.0.0.1:${debug[0]}${debug[1]}`);
  await new Promise((resolve,reject)=>{ws.addEventListener('open',resolve,{once:true});ws.addEventListener('error',reject,{once:true});});
  let sequence=0;const pending=new Map(),errors=[];
  ws.addEventListener('message',({data})=>{const message=JSON.parse(String(data));if(message.id){const call=pending.get(message.id);pending.delete(message.id);if(message.error)call?.reject(new Error(message.error.message));else call?.resolve(message.result);}else if(message.method==='Runtime.exceptionThrown')errors.push(message.params.exceptionDetails.text);});
  const send=(method,params={},sessionId)=>new Promise((resolve,reject)=>{const id=++sequence;pending.set(id,{resolve,reject});ws.send(JSON.stringify({id,method,params,...(sessionId?{sessionId}:{})}));});
  const {targetId}=await send('Target.createTarget',{url:'about:blank'}),{sessionId}=await send('Target.attachToTarget',{targetId,flatten:true});
  const call=(method,params)=>send(method,params,sessionId);
  for(const method of ['Page.enable','Runtime.enable','Network.enable'])await call(method);
  await call('Page.addScriptToEvaluateOnNewDocument',{source:'window.confirm=()=>true;'});
  const evaluate=async expression=>{const value=await call('Runtime.evaluate',{expression,returnByValue:true,awaitPromise:true});if(value.exceptionDetails)throw new Error(JSON.stringify(value.exceptionDetails));return value.result.value;};
  const waitFor=async expression=>{for(let i=0;i<100;i++){if(await evaluate(expression))return;await delay(100);}throw new Error('UI timeout: '+expression);};
  const idle="document.querySelector('main')?.getAttribute('aria-busy')==='false'";
  const readJson=()=>evaluate("JSON.parse(document.querySelector('#factor-json').value)");
  const fillJson=async rows=>evaluate(`document.querySelector('#factor-json').value=${JSON.stringify(JSON.stringify(rows,null,2))};document.querySelector('#factor-json').dispatchEvent(new Event('input',{bubbles:true}));`);
  const select=async(id,value)=>evaluate(`document.getElementById(${JSON.stringify(id)}).value=${JSON.stringify(value)};document.getElementById(${JSON.stringify(id)}).dispatchEvent(new Event('change',{bubbles:true}));`);
  const navigate=async(worker,name)=>{await call('Page.navigate',{url:'about:blank'});await waitFor("location.href==='about:blank'");await call('Page.navigate',{url:base+'/?worker='+worker+'#'+name});await waitFor(`${idle} && document.querySelector('#strategy-group')?.value===${JSON.stringify(name.startsWith('prep02')?'prep02':worker===spread?'spread01':'prep01')}`);};
  const click=async id=>evaluate(`document.getElementById(${JSON.stringify(id)}).click()`);
  await call('Emulation.setDeviceMetricsOverride',{width:1440,height:1100,deviceScaleFactor:1,mobile:false});
  const checked=[];
  for(const [worker,name,interval,count] of [[prep,'binance-futures_1min','1min',45],[prep,'binance-futures_5min','5min',48],[prep,'binance-futures_15min','15min',146],[prep,'prep02_binance-futures_1min','1min',59],[prep,target,'5min',7],[prep,'prep02_binance-futures_15min','15min',33],[spread,'okex-futures_binance-futures_1min','1min',34]]){
    await navigate(worker,name);
    assert.equal(await evaluate("getComputedStyle(document.body).backgroundColor"),'rgb(11, 18, 33)');
    assert.equal(await evaluate("document.querySelectorAll('textarea').length"),1);
    assert.equal(await evaluate("document.querySelectorAll('#position-targets,#parameters,#rows,#import-json').length"),0);
    assert.equal(await evaluate("document.body.textContent.includes('仓位表')||document.body.textContent.includes('因子计算参数')"),false);
    assert.equal(await evaluate("document.querySelectorAll('#frequency-tabs button').length"),worker===prep?3:1);
    assert.equal(await evaluate("document.querySelector('#frequency-tabs [aria-selected=true]').dataset.interval"),interval);
    const rows=await readJson();
    assert(rows.every(row=>JSON.stringify(Object.keys(row))===JSON.stringify(['频率','NQ','因子','符号','标的'])&&Array.isArray(row['标的'])&&row['频率']===interval.replace('min','m')));
    assert.equal(rows.reduce((sum,row)=>sum+row['标的'].length,0),count);
    checked.push({worker,name,interval,rules:count});
  }
  await navigate(prep,target);
  const original=await readJson(),before=await api(prep),file=path.join(temporary,'config/cta_prep_binance_futures',target+'.json'),fileBefore=await readFile(file,'utf8');
  await fillJson([{'标的':'SOL','nq':'关闭','符号':1,'因子':'factor_049'}]);await click('validate');await waitFor(`${idle} && document.querySelector('#message').classList.contains('err')`);
  assert.equal(await readFile(file,'utf8'),fileBefore);
  const mixed=structuredClone(original);mixed[0]['频率']='1m';await fillJson(mixed);await click('validate');await waitFor(`${idle} && document.querySelector('#message').textContent.includes('当前频段为 5min')`);
  assert.equal(await readFile(file,'utf8'),fileBefore);
  const unknown=structuredClone(original);unknown[0]['因子']='unmapped_factor';await fillJson(unknown);await click('validate');await waitFor(`${idle} && document.querySelector('#message').classList.contains('err')`);
  assert.equal(await readFile(file,'utf8'),fileBefore);
  const edited=structuredClone(original);edited[0].NQ='0.2/0.8';edited.push(structuredClone(edited[0]));
  await fillJson(edited);await evaluate("document.querySelector('#max-pos').value='321';document.querySelector('#max-pos').dispatchEvent(new Event('input',{bubbles:true}));");
  await click('validate');await waitFor(`${idle} && document.querySelector('#message').textContent.includes('校验通过')`);assert.equal(await readFile(file,'utf8'),fileBefore);
  await click('save');await waitFor(`${idle} && document.querySelector('#message').textContent.includes('已保存')`);
  let saved=await api(prep);const current=saved.plan.combinations[target];
  assert.equal(current.signal.max_pos_u,321);assert.equal(current.factor_rules.length,7);assert(current.factor_rules.filter(row=>row['因子']==='factor_049').every(row=>row.nq==='0.2/0.8'));
  assert.deepEqual(current.parameters,before.plan.combinations[target].parameters);assert.deepEqual(current.signal.symbol_overrides,before.plan.combinations[target].signal.symbol_overrides);
  for(const [name,doc] of Object.entries(before.plan.combinations))if(name!==target)assert.deepEqual(saved.plan.combinations[name],doc);
  const external=structuredClone(saved.plan.combinations['binance-futures_1min']);external.signal.max_pos_u=123;
  await api(prep,'combinations',{worker:prep,combination:external,expected_revision:saved.revision});
  await evaluate("document.querySelector('#max-pos').value='999';document.querySelector('#max-pos').dispatchEvent(new Event('input',{bubbles:true}));");await click('save');
  await waitFor(`${idle} && document.querySelector('#message').classList.contains('err')`);assert.equal(await evaluate("document.querySelector('#max-pos').value"),'999');
  assert.equal((await api(prep)).plan.combinations[target].signal.max_pos_u,321);
  await click('reload');await waitFor(`${idle} && document.querySelector('#max-pos').value==='321'`);
  await evaluate("location.hash='prep02_binance-futures_15min'");await waitFor("document.querySelector('#current-frequency').textContent==='15min'");assert.equal(await evaluate("document.querySelector('#max-pos').value"),'200');
  const rows=await readJson();assert(rows.some(row=>row['因子']==='TD_MT_040'&&row['标的'].includes('SOL')));
  await select('strategy-group','prep01');assert.equal(await evaluate("document.querySelector('#current-frequency').textContent"),'15min');
  await select('strategy-type',spread);await waitFor(`${idle} && document.querySelector('#spread-settings').hidden===false`);assert.equal(await evaluate("document.querySelector('#spread-long').value"),'0.3');
  // A read/save round trip must retain the Spread leg's factor venue.
  const spreadBefore=await api(spread);await click('save');await waitFor(`${idle} && document.querySelector('#message').textContent.includes('已保存')`);
  assert.deepEqual((await api(spread)).plan.combinations,spreadBefore.plan.combinations);
  await select('strategy-type',prep);await waitFor(idle);await select('strategy-group','prep02');await evaluate("document.querySelector('[data-interval=\"1min\"]').click()");
  assert.equal(await evaluate("document.querySelector('#current-frequency').textContent"),'1min');
  await mkdir(output,{recursive:true});let shot=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});await writeFile(path.join(output,'desktop.png'),Buffer.from(shot.data,'base64'));
  await call('Emulation.setDeviceMetricsOverride',{width:390,height:844,deviceScaleFactor:1,mobile:false});assert(await evaluate('document.documentElement.scrollWidth<=innerWidth'),'mobile page overflow');
  shot=await call('Page.captureScreenshot',{format:'png',captureBeyondViewport:true});await writeFile(path.join(output,'mobile.png'),Buffer.from(shot.data,'base64'));
  assert.deepEqual(errors,[]);
  const result={local_only:true,profiles:checked,json_only:true,frequency_isolation:true,hidden_settings_preserved:true,spread_round_trip:true,legacy_and_mixed_frequency_json_rejected:true,duplicate_rows_removed:true,conflict_preserves_draft:true,preview_does_not_persist:true,script_errors:errors,mobile_width:390};
  await writeFile(path.join(output,'browser-verification.json'),JSON.stringify(result,null,2)+'\n');console.log(JSON.stringify(result));
}finally{ws?.close();chrome?.kill('SIGTERM');server?.kill('SIGTERM');await delay(300);await rm(temporary,{recursive:true,force:true});}
