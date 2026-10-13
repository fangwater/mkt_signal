'use strict';
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const number = (v, n = 3) => typeof v === 'number' && Number.isFinite(v) ? v.toFixed(n) : '—';
const stamp = (ts, full = false) => ts ? new Date(ts).toISOString().slice(full ? 0 : 5, 16).replace('T', ' ') : '—';
const reasons = {cash_entry:'价差放行，现货开仓',reversal:'反转：对冲旧方向，开仓新方向',blocked_reversal_exit:'反向价差不合适，仅对冲退出',factor_exit:'因子触发退出，以合约对冲',spread_blocked:'因子触发，价差未放行',no_entry:'因子未触发开仓',hold:'保持原方向',factor_unready:'因子预热中，保持状态'};
const colors = {buy:'#50d8b0', sell:'#ff8c8c', hedge:'#edc17b', factor:'#7bb8fa', muted:'#8c9dad'};
let panel, data, factor = new URLSearchParams(location.search).get('factor'), symbol, hours = 24, requestId = 0, selectedEvent;
let chart = echarts.init($('chart'), null, {renderer:'canvas'}), selectionKey = '';
const strategy = () => panel?.strategies.find(s => s.factor === factor);
const directionName = n => n > 0 ? '多方向' : n < 0 ? '空方向' : '中性 / 无方向';
async function get(url) {const response = await fetch(url, {cache:'no-store'}); if (!response.ok) throw new Error(`读取失败 (${response.status})`); return response.json();}
function drawOverview() {
  const all = panel.strategies.flatMap(s => s.symbols), c = panel.config || {};
  $('publishing').textContent = c.enabled ? '仓位发布开启' : '仓位发布关闭 · 仅观察';
  $('combinations').textContent = `${panel.strategies.length} / ${all.length}`;
  $('readiness').textContent = `${all.filter(s => s.ready).length} / ${all.length}`;
  $('spread-readiness').textContent = `价差就绪 ${all.filter(s => s.spread_ready).length} / ${all.length}`;
  $('model-time').textContent = stamp(panel.latest_model_ms);
  const age = panel.latest_model_ms ? Math.max(0, panel.server_ts_us / 1000 - panel.latest_model_ms) / 60000 : null;
  $('lag').textContent = age === null ? '等待 Kafka 历史' : age > 3 ? `历史回放中 / 数据滞后 ${age >= 60 ? number(age / 60, 1) + ' 小时' : Math.floor(age) + ' 分钟'}` : '历史已接近实时';
  $('live-ready').textContent = `${all.filter(s => s.live_ready).length} / ${all.length}`;
  $('heartbeat').textContent = `${panel.signal_state === 'online' ? '服务心跳在线' : '服务心跳异常'} · 心跳不代表因子就绪`;
  $('factors').innerHTML = panel.strategies.map(s => `<button class="factor ${s.factor === factor ? 'active' : ''}" data-factor="${esc(s.factor)}"><span class="eyebrow">Independent strategy</span><span class="sign">${s.symbols.every(x=>x.sign===1) ? '+1' : s.symbols.every(x=>x.sign===-1) ? '−1' : '混合'}</span><span class="name">${esc(s.factor)}</span><span class="count"><span>${s.symbols.length} 个币种</span><span>历史就绪 ${s.symbols.filter(x=>x.ready).length}/${s.symbols.length}</span></span></button>`).join('');
  $('factors').querySelectorAll('button').forEach(b => b.onclick = () => {factor = b.dataset.factor; if (!strategy().symbols.some(s => s.symbol === symbol)) symbol = strategy().symbols[0]?.symbol; selectedEvent = null; drawOverview(); loadHistory();});
  drawSymbols();
  const entry = c.entry || {}, exit = c.execution || {};
  $('rules').innerHTML = `现货开多：因子 &gt; ${number(entry.factor_long_quantile, 2)}，价差 &lt; ${number(entry.spread_lower_quantile, 2)}<br>现货开空：因子 &lt; ${number(entry.factor_short_quantile, 2)}，价差 &gt; ${number(entry.spread_upper_quantile, 2)}<br><br>多头退出：因子 &lt; ${number(exit.factor_exit_quantile_long, 2)}<br>空头退出：因子 &gt; ${number(exit.factor_exit_quantile_short, 2)}<br>退出不受价差过滤。`;
  $('low-label').textContent = `0 — ${number(entry.spread_lower_quantile, 2)} 多`;
  $('high-label').textContent = `空 ${number(entry.spread_upper_quantile, 2)} — 1`;
  document.querySelector('.gauge').style.background = `linear-gradient(to right,#255c4d 0 ${entry.spread_lower_quantile*100}%,#34404a ${entry.spread_lower_quantile*100}% ${entry.spread_upper_quantile*100}%,#69454a ${entry.spread_upper_quantile*100}%)`;
}
function drawSymbols() {
  const rows = strategy()?.symbols || [], query = $('search').value.toUpperCase();
  $('symbol-count').textContent = rows.length;
  $('symbols').innerHTML = rows.filter(s=>s.symbol.includes(query)).map(s=>`<button class="symbol ${s.symbol===symbol?'active':''}" data-symbol="${esc(s.symbol)}"><span><i class="dot ${s.ready&&s.spread_ready?'ready':''}"></i>${esc(s.symbol.replace(/USDT$/, ''))}</span><small>${number(s.quantile, 2)}</small></button>`).join('');
  $('symbols').querySelectorAll('button').forEach(b => b.onclick = () => {symbol=b.dataset.symbol; selectedEvent=null; drawSymbols(); loadHistory();});
}
function inspect(point, event) {
  $('inspector-title').textContent = event ? `${event.market==='cash'?'现货':'合约'} ${event.side} · ${event.action==='cash_entry'?'开仓':'对冲退出'}` : '最近观测';
  $('inspect-time').textContent = `${stamp(point?.ts,true)} UTC`;
  $('direction').textContent = point ? directionName(point.direction) : '等待数据';
  $('direction').style.color = point?.direction > 0 ? colors.buy : point?.direction < 0 ? colors.sell : colors.muted;
  $('factor-q').textContent = number(point?.quantile) + (point && !point.ready ? ' · 预热' : '');
  $('spread-q').textContent = number(point?.spread_quantile) + (point && !point.spread_ready ? ' · 预热' : '');
  $('spread-bps').textContent = number(typeof point?.spread_value === 'number' ? point.spread_value * 10000 : null, 2);
  $('spread-cursor').hidden = typeof point?.spread_quantile !== 'number';
  $('spread-cursor').style.left = `${(point?.spread_quantile || 0)*100}%`;
  $('reason').textContent = reasons[point?.reason] || '等待 Kafka 因子历史';
}
function selectEvent(index) {
  const event = data.events[index]; if (!event) return;
  selectedEvent = `${event.ts}:${event.market}:${event.side}`;
  inspect(event,event);
  $('events').querySelectorAll('tr').forEach(row=>row.classList.toggle('selected',Number(row.dataset.event)===index));
  const position = Math.round((event.ts - data.start_ms)/60000);
  chart.dispatchAction({type:'showTip',seriesIndex:0,dataIndex:position});
}
function drawHistory() {
  $('instrument').textContent = symbol.replace(/USDT$/, '') + ' / USDT';
  $('factor-name').textContent = factor;
  $('sign').textContent = data.sign > 0 ? 'sign +1' : 'sign −1 · 使用 1−q';
  const stale = data.server_ts_ms - data.end_ms > 180000;
  $('window-label').textContent = `${stale?'历史窗口':'最近窗口'} · ${stamp(data.end_ms)} UTC`;
  const candles = new Map(data.klines.map(r=>[r.ts,r]));
  const points = new Map(data.points.map(r=>[r.ts,r]));
  const times = []; for (let t=data.start_ms;t<=data.end_ms;t+=60000) times.push(t);
  const labels = times.map(t=>stamp(t));
  const price = times.map(t=>{const r=candles.get(t);return r?[r.open,r.close,r.low,r.high]:[null,null,null,null];});
  const entry=data.config.entry, exit=data.config.execution;
  const line = (name, index, values, color, thresholds) => ({name,type:'line',xAxisIndex:index,yAxisIndex:index,data:values,showSymbol:false,connectNulls:false,lineStyle:{width:1.4,color},itemStyle:{color},markLine:{silent:true,symbol:'none',label:{position:'insideEndTop',color:'#8293a4',fontSize:9,formatter:p=>number(p.value,2)},lineStyle:{color:'#46515d',type:'dashed',width:1},data:thresholds.map(yAxis=>({yAxis}))}});
  const markers = (market,side) => data.events.flatMap((e,i)=>{const r=candles.get(e.ts);return e.market===market&&e.side===side&&r?[{value:[stamp(e.ts),side==='B'?r.low:r.high],eventIndex:i,event:e}]:[];});
  const scatter = (market,side) => ({name:`${market==='cash'?'现货':'合约对冲'} ${side}`,type:'scatter',data:markers(market,side),symbol:market==='cash'?'triangle':'diamond',symbolRotate:side==='S'&&market==='cash'?180:0,symbolSize:market==='cash'?12:10,symbolOffset:market==='cash'?[0,side==='B'?12:-12]:[side==='B'?13:-13,side==='B'?28:-28],clip:false,itemStyle:{color:market==='cash'?(side==='B'?colors.buy:colors.sell):colors.hedge},label:{show:true,formatter:side,position:side==='B'?'bottom':'top',fontSize:10,color:market==='cash'?(side==='B'?colors.buy:colors.sell):colors.hedge},z:10});
  const key=`${factor}:${symbol}:${hours}`;
  const oldZoom=selectionKey===key?chart.getOption()?.dataZoom?.map(z=>({start:z.start,end:z.end})):null;
  const option={animation:false,backgroundColor:'transparent',textStyle:{fontFamily:'system-ui'},grid:[{left:57,right:24,top:28,height:'45%'},{left:57,right:24,top:'58%',height:'13%'},{left:57,right:24,top:'80%',height:'12%'}],title:[{text:'现货价格 · 信号点 / 非成交价',left:17,top:3,textStyle:{fontSize:10,fontWeight:400,color:colors.muted}},{text:'修正后因子分位',left:17,top:'55%',textStyle:{fontSize:10,fontWeight:400,color:colors.factor}},{text:'价差分位 · (spot − perp) / spot',left:17,top:'74%',textStyle:{fontSize:10,fontWeight:400,color:colors.hedge}}],axisPointer:{link:[{xAxisIndex:'all'}],label:{backgroundColor:'#344653'}},tooltip:{trigger:'axis',renderMode:'html',confine:true,backgroundColor:'#13212f',borderColor:'#344653',textStyle:{fontSize:11,color:'#d9e5ee'},formatter:params=>{const t=times[params[0]?.dataIndex], r=candles.get(t), p=points.get(t);return `${esc(stamp(t,true))} UTC<br>现货 O ${number(r?.open,4)} / C ${number(r?.close,4)}<br>因子 ${number(p?.quantile)} · 价差 ${number(p?.spread_quantile)}<br>${esc(reasons[p?.reason]||'此分钟没有因子观测')}`;}},xAxis:[0,1,2].map(i=>({type:'category',data:labels,gridIndex:i,boundaryGap:true,axisLine:{lineStyle:{color:'#293745'}},axisTick:{show:false},axisLabel:{show:i===0||i===2,color:colors.muted,fontSize:9,hideOverlap:true,formatter:v=>v.slice(6)},axisPointer:{show:true}})),yAxis:[0,1,2].map(i=>({gridIndex:i,scale:i===0,min:i===0?undefined:0,max:i===0?undefined:1,splitNumber:i===0?4:2,axisLabel:{color:colors.muted,fontSize:9},splitLine:{lineStyle:{color:'#1f2b37'}},axisPointer:{show:i===0}})),dataZoom:[{type:'inside',xAxisIndex:[0,1,2],start:oldZoom?.[0]?.start??0,end:oldZoom?.[0]?.end??100},{type:'slider',xAxisIndex:[0,1,2],height:13,bottom:2,start:oldZoom?.[1]?.start??0,end:oldZoom?.[1]?.end??100,showDetail:false,borderColor:'#253140',fillerColor:'#214c4544',dataBackground:{lineStyle:{color:'#51697c'},areaStyle:{color:'#253140'}}}],series:[{name:'Spot 1m',type:'candlestick',data:price,itemStyle:{color:colors.buy,color0:colors.sell,borderColor:colors.buy,borderColor0:colors.sell}},scatter('cash','B'),scatter('cash','S'),scatter('perpetual','B'),scatter('perpetual','S'),line('修正后因子分位',1,times.map(t=>points.get(t)?.ready?points.get(t).quantile:null),colors.factor,[entry.factor_long_quantile,entry.factor_short_quantile,exit.factor_exit_quantile_long,exit.factor_exit_quantile_short]),line('价差分位',2,times.map(t=>points.get(t)?.spread_ready?points.get(t).spread_quantile:null),colors.hedge,[entry.spread_lower_quantile,entry.spread_upper_quantile])]};
  chart.setOption(option,true); selectionKey=key;
  const missing = data.events.filter(e=>!candles.has(e.ts)).length;
  const ready = data.points.filter(p=>p.ready).length;
  $('chart-state').textContent = `信号时间为已闭合分钟的结束边界，标记在下一分钟；价格仅用于定位。${ready}/${data.points.length} 条因子观测就绪 · ${data.missing_candles} 根 K 线待补${missing?` · ${missing} 个事件暂无 K 线，见下表`:''}${data.feed?.error?` · 行情异常：${data.feed.error}`:''}`;
  $('event-count').textContent = ` / ${data.events.length}`;
  $('events').innerHTML = data.events.length ? data.events.map((e,i)=>({e,i})).reverse().map(({e,i})=>`<tr data-event="${i}"><td>${esc(stamp(e.ts,true))}</td><td>${e.market==='cash'?'现货':'永续合约'}</td><td><span class="badge ${e.market==='perpetual'?'hedge':e.side==='B'?'buy':'sell'}">${e.side==='B'?'B · 买':'S · 卖'}</span></td><td>${e.action==='cash_entry'?'现货开仓':'合约对冲退出'}</td><td>${number(e.quantile)}</td><td>${number(e.spread_quantile)}</td><td>${esc(reasons[e.reason])}</td></tr>`).join('') : `<tr><td colspan="7" class="empty">${!data.points.length?'等待 Kafka 历史观测':!ready?'因子仍在预热，尚未形成可回放的 B/S 点':'此时间窗口没有方向变化'}<br>可切换 factor、币种或时间范围观察。</td></tr>`;
  $('events').querySelectorAll('[data-event]').forEach(row=>row.onclick=()=>selectEvent(Number(row.dataset.event)));
  const saved = data.events.findIndex(e=>`${e.ts}:${e.market}:${e.side}`===selectedEvent);
  if(saved>=0) selectEvent(saved); else inspect(data.points.at(-1));
  $('replay-origin').textContent = `回放起点 ${stamp(data.replay_start_ms,true)} UTC，初始为空仓；按当前符号与阈值回放，不校验历史 BBO 或重建成交数量。`;
}
async function loadHistory() {
  if(!factor||!symbol) return;
  const id=++requestId;
  // Clear obsolete state immediately on selection, including failed requests.
  if(selectionKey!==`${factor}:${symbol}:${hours}`){chart.clear();$('events').innerHTML='<tr><td colspan="7" class="empty">加载当前策略…</td></tr>';inspect(null);$('instrument').textContent=symbol.replace(/USDT$/,'')+' / USDT';$('factor-name').textContent=factor;}
  try {const result=await get(`../api/signal-history?symbol=${encodeURIComponent(symbol)}&factor=${encodeURIComponent(factor)}&hours=${hours}`);if(id!==requestId)return;data=result;drawHistory();$('error').textContent=panel.history_error?`历史导入异常：${panel.history_error}`:'';}
  catch(error){if(id===requestId)$('error').textContent=error.message;}
}
async function refresh() {
  try {panel=await get('../api/signal-panel');if(!strategy())factor=panel.strategies[0]?.factor;if(!strategy()?.symbols.some(s=>s.symbol===symbol))symbol=strategy()?.symbols[0]?.symbol;drawOverview();await loadHistory();}
  catch(error){$('error').textContent=error.message;}
}
$('search').oninput=drawSymbols;
document.querySelectorAll('[data-hours]').forEach(button=>button.onclick=()=>{hours=Number(button.dataset.hours);selectedEvent=null;document.querySelectorAll('[data-hours]').forEach(b=>b.classList.toggle('active',b===button));loadHistory();});
chart.on('click',params=>{if(Number.isInteger(params.data?.eventIndex))selectEvent(params.data.eventIndex);});
new ResizeObserver(()=>chart.resize()).observe($('chart'));
async function poll(){await refresh();setTimeout(poll,10000);}poll();
