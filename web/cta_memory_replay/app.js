"use strict";
(() => {
  const el=id=>document.getElementById(id);
  const fmt=(n,d=2)=>Number.isFinite(n)?n.toLocaleString("en-US",{maximumFractionDigits:d}):"—";
  const pct=n=>n==null?"—":fmt(n*100,1)+"%";
  const utc=ms=>ms?new Date(ms).toISOString().replace("T"," ").slice(0,19):"—";
  const inputTime=ms=>new Date(ms).toISOString().slice(0,16);
  const tone=n=>n>=0?"positive":"negative";
  const state={catalog:null,result:null,busy:false,selection:0,comparisons:[],lastRun:0};
  const chart=echarts.init(el("chart"));
  const eligible=s=>s.ready_rules>0||s.rules===0;
  const shown=(s,key,d=2)=>eligible(s)?fmt(s[key],d):"预热中";
  function profiles(){
    return (state.catalog?.profiles||[]).filter(p=>(el("strategy-group").value==="*"||p.strategy_group===el("strategy-group").value)
      &&(el("symbol-group").value==="*"||p.symbol_group===el("symbol-group").value)
      &&(el("interval").value==="*"||p.interval===el("interval").value));
  }
  function pools(){
    const before=el("symbol-group").value;
    const rows=state.catalog.profiles.filter(p=>el("strategy-group").value==="*"||p.strategy_group===el("strategy-group").value);
    const order={all:0,"top-5":1,"top-20":2,"last-20":3};
    const names=[...new Set(rows.map(p=>p.symbol_group))].sort((a,b)=>order[a]-order[b]);
    el("symbol-group").replaceChildren(new Option("全部券池","*"),...names.map(n=>new Option(n,n)));
    if(names.includes(before))el("symbol-group").value=before;
  }
  function coverage(){
    const d=state.catalog;if(!d)return;
    const p=Object.values(d.partitions)[0];
    const fraction=p&&p[2]>=p[0]?Math.max(0,Math.min(1,(p[1]-p[0]+1)/(p[2]-p[0]+1))):0;
    el("state").textContent=d.error?"数据异常":d.caught_up?"历史已载入 · 持续更新":`历史载入 ${pct(fraction)}`;
    el("coverage").textContent=`${fmt(d.factor_points,0)} 个因子观测 · ${fmt(d.candle_count,0)} 根成交 K 线。数据截至 ${utc(d.latest_data_ms)} UTC。`;
    el("source").textContent=`初始配置来自 JP 当前 Prep01 / Prep02，读取于 ${utc(d.fetched_at_ms)} UTC。`;
    if(d.error)el("error").textContent=d.error;
  }
  function body(){
    const n=id=>Number(el(id).value),t=id=>el(id).value?Date.parse(el(id).value+":00Z"):null;
    return {profiles:profiles().map(p=>p.name),start_ms:t("start"),end_ms:t("end"),
      thresholds:el("override").checked?{long_entry:n("entry-long"),short_entry:n("entry-short"),long_exit:n("exit-long"),short_exit:n("exit-short")}:null,
      max_pos_u:el("amount").value?n("amount"):null,fee_rate:n("fee")/10000};
  }
  async function run(event){
    event?.preventDefault();if(!state.catalog||state.busy)return;
    if(!profiles().length){el("error").textContent="当前范围没有策略。";return;}
    const selection=state.selection;state.busy=true;el("run").disabled=true;el("error").textContent="";
    try{
      const response=await fetch("api/replay",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body())});
      const result=await response.json();if(!response.ok)throw new Error(result.error||"回放失败");
      if(selection!==state.selection)return;
      state.result=result;state.lastRun=Date.now();render(result);
    }catch(error){if(selection===state.selection)el("error").textContent=error.message;}
    finally{state.busy=false;el("run").disabled=false;if(selection!==state.selection)run();}
  }
  function changed(){state.selection++;state.result=null;el("save").disabled=true;run();}
  async function poll(){
    try{
      const response=await fetch("api/catalog",{cache:"no-store"});if(!response.ok)throw new Error("读取历史状态失败");
      const initial=!state.catalog;state.catalog=await response.json();coverage();
      if(initial){
        const names=[...new Set(state.catalog.profiles.map(p=>p.strategy_group))];
        el("strategy-group").replaceChildren(new Option("全部策略组","*"),...names.map(n=>new Option(n,n)));pools();
      }
      if(!state.busy&&state.catalog.candle_count>0&&(initial||!state.result||(!el("end").value&&Date.now()-state.lastRun>30000)))run();
    }catch(error){el("state").textContent="连接中断";el("error").textContent=error.message;}
    setTimeout(poll,5000);
  }
  function render(r){
    const s=r.summary;
    el("pnl-title").textContent=r.parameters.fee_rate>0?"扣费后理论 PnL · USDT":"理论毛 PnL · USDT";
    el("pnl").textContent=shown(s,"pnl",4);el("pnl").className=eligible(s)?tone(s.pnl):"";
    el("components").textContent=eligible(s)?`${fmt(s.realized_pnl,3)} / ${fmt(s.unrealized_pnl,3)}`:"—";
    el("wins").textContent=eligible(s)?`${s.closed_trades} / ${pct(s.win_rate)}`:"—";
    el("drawdown").textContent=eligible(s)?fmt(s.max_drawdown,4):"—";
    el("elapsed").textContent=fmt(r.compute_ms)+" ms";
    el("result-note").textContent=`截至 ${utc(r.cutoff_ms)} UTC · ${s.ready_rules}/${s.rules} 条规则就绪 · 缺少有效 VWAP ${s.missing_vwap} 根 · 手续费 ${fmt(s.fees,4)} USDT${!r.caught_up?" · 历史载入中":""}${s.ready_rules<s.rules?" · 当前收益仅覆盖已就绪部分":""}`;
    el("inspector").textContent=`持仓规则：多 ${s.long_rules} / 空 ${s.short_rules}。每条规则独立冻结入场数量，退出平原数量。`;
    el("save").disabled=!r.curve.length;
    el("group-totals").replaceChildren(...r.groups.map(g=>{
      const card=document.createElement("section");card.className="card";
      const title=document.createElement("h2");title.textContent=g.strategy_group.toUpperCase()+" · 全部币种";
      const value=document.createElement("strong");value.textContent=shown(g.summary,"pnl",4)+" USDT";
      value.className=eligible(g.summary)?tone(g.summary.pnl):"";value.style.fontSize="24px";
      const detail=document.createElement("p");detail.className="hint";detail.textContent=`${g.summary.ready_rules}/${g.summary.rules} 条规则就绪 · 已平 ${g.summary.closed_trades} 笔`;
      card.append(title,value,detail);return card;
    }));
    el("strategies").replaceChildren(...r.strategies.map(strategy=>{
      const tr=document.createElement("tr"),s=strategy.summary;
      const values=[`${strategy.strategy_group} / ${strategy.symbol_group} / ${strategy.interval}`,s.rules?`${s.ready_rules}/${s.rules}`:"空策略",
        s.closed_trades,pct(s.win_rate),shown(s,"realized_pnl",4),shown(s,"unrealized_pnl",4),shown(s,"pnl",4),shown(s,"max_drawdown",4)];
      values.forEach((value,i)=>{const td=document.createElement("td");td.textContent=value;if(i===6&&eligible(s))td.className=tone(s.pnl);tr.append(td);});return tr;
    }));draw(r);
  }
  function aligned(curve,times){let i=0,pnl=0;return times.map(ts=>{while(i<curve.length&&curve[i].ts_ms<=ts)pnl=curve[i++].pnl;return pnl;});}
  function draw(r){
    const times=r.summary.ready_rules>0?r.curve.map(p=>p.ts_ms):[],colors=["#83bafa","#e3bd79"];
    const series=r.groups.map((g,i)=>({name:g.strategy_group.toUpperCase(),type:"line",showSymbol:false,data:aligned(g.curve,times),
      lineStyle:{width:1.8,color:colors[i%colors.length]},itemStyle:{color:colors[i%colors.length]}}));
    series.push({name:"组合理论 PnL",type:"line",showSymbol:false,data:times.map((_,i)=>r.curve[i].pnl),lineStyle:{width:2.3,color:"#5fd1a2"},itemStyle:{color:"#5fd1a2"}});
    chart.setOption({animation:false,backgroundColor:"transparent",textStyle:{color:"#95a6b5"},
      title:times.length?undefined:{text:"因子预热中，尚未产生有效理论收益",left:"center",top:"45%",textStyle:{color:"#95a6b5",fontSize:14}},
      legend:{top:5,textStyle:{color:"#95a6b5"}},grid:{left:70,right:25,top:45,bottom:75},tooltip:{trigger:"axis",confine:true},
      xAxis:{type:"category",data:times.map(ms=>utc(ms)),axisLabel:{formatter:t=>t.slice(5,16)}},
      yAxis:{type:"value",scale:true,name:"USDT",splitLine:{lineStyle:{color:"#293543"}}},
      dataZoom:[{type:"inside"},{type:"slider",bottom:10,height:24}],series},true);
  }
  el("save").onclick=()=>{
    if(!state.result)return;state.comparisons.unshift(state.result);state.comparisons=state.comparisons.slice(0,8);
    el("comparisons").replaceChildren(...state.comparisons.map(r=>{
      const tr=document.createElement("tr"),t=r.parameters.thresholds,s=r.summary;
      const label=t?`${t.long_entry}/${t.short_entry} → ${t.long_exit}/${t.short_exit}`:"当前策略阈值";
      const values=[`${label} · ${r.strategies.length} 策略`,utc(r.cutoff_ms),s.closed_trades,pct(s.win_rate),shown(s,"pnl",4),shown(s,"max_drawdown",4),fmt(r.compute_ms)+" ms"];
      values.forEach(v=>{const td=document.createElement("td");td.textContent=v;tr.append(td);});return tr;
    }));
  };
  el("strategy-group").onchange=()=>{pools();changed();};el("symbol-group").onchange=changed;el("interval").onchange=changed;
  for(const id of ["entry-long","entry-short","exit-long","exit-short"])el(id).oninput=()=>{el("override").checked=true;};
  el("parameters").onsubmit=run;
  el("freeze").onclick=()=>{const cutoff=state.result?.cutoff_ms||state.catalog?.latest_data_ms;if(cutoff)el("end").value=inputTime(cutoff);};
  el("latest").onclick=()=>{el("end").value="";};
  window.addEventListener("resize",()=>chart.resize());poll();
})();
