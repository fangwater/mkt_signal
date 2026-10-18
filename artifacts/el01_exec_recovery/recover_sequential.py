import os,glob,time,json,subprocess,urllib.request,pathlib,tomllib
os.environ['PATH']='/home/el01/.local/bin:'+os.environ.get('PATH','')
cfg=tomllib.load(open('/home/el01/crypto_cta_manager/config/cta-manager.toml','rb'))
sources={s['id']:s for s in cfg['sources'] if s.get('enabled',True)}
root=pathlib.Path('/home/el01/.el01-exec-recovery');root.mkdir(exist_ok=True,mode=0o700)
sequence=['trade06','trade07','trade08','trade09','trade10','trade11','trade12','xy_lxy21_05','trade14','trade01','trade03','trade04','trade13']
def processes(base):
 rows={}
 for p in glob.glob('/proc/[0-9]*'):
  try:
   if os.readlink(p+'/cwd')==base:
    c=open(p+'/comm').read().strip()
    if c in ['trade_engine','exec-pre-trade','account_monitor']:rows[c]=int(p.rsplit('/',1)[1])
  except (FileNotFoundError,ProcessLookupError,PermissionError):pass
 return rows
results=[]
for suffix in sequence:
 name='binance_exec_'+suffix;base='/home/el01/'+name;started_ms=int(time.time()*1000)
 print(json.dumps({'event':'restarting','env':name,'started_ms':started_ms}),flush=True)
 with open(root/(name+'.restart.log'),'w') as log:
  for script in ['stop_exec_pre_trade.sh','stop_exec_trade_engine.sh','stop_account_monitor.sh','start_exec_trade_engine.sh','start_account_monitor.sh','start_exec_pre_trade.sh']:
   log.write('\nSCRIPT '+script+'\n');log.flush()
   r=subprocess.run([base+'/scripts/'+script],cwd=base,stdout=log,stderr=subprocess.STDOUT,timeout=90)
   if r.returncode:raise RuntimeError(name+' '+script+' failed; inspect '+str(root/(name+'.restart.log')))
 deadline=time.monotonic()+65;ready_since=None;last=None
 while time.monotonic()<deadline:
  try:
   d=json.load(urllib.request.urlopen(sources[name]['exec_viz_url'].rstrip('/')+'/snapshot',timeout=5))
   state=next(e['entry'] for e in d['entries'] if e.get('type')=='exec_pre_trade_state')
   pids=processes(base); now_ms=int(time.time()*1000)
   good=state.get('position_ready') and state.get('ts_ms',0)>started_ms and now_ms-state['ts_ms']<5000 and len(pids)==3
   last={'env':name,'position_ready':state.get('position_ready'),'snapshot_age_ms':now_ms-state.get('ts_ms',0),'pids':pids,'rows':len(state.get('rows',[])),'live_rows':sum(abs(r.get('live_order_qty',0))>1e-9 for r in state.get('rows',[])),'started_ms':started_ms,'checked_ms':now_ms}
   if good:
    if ready_since is None:ready_since=time.monotonic()
    if time.monotonic()-ready_since>=5:break
   else:ready_since=None
  except Exception as e:last={'env':name,'error':str(e)};ready_since=None
  time.sleep(2)
 else:
  print(json.dumps({'event':'failed','last':last}),flush=True)
  raise RuntimeError(name+' did not become healthy; refusing next restart')
 results.append(last);(root/'results.json').write_text(json.dumps(results,indent=2))
 print(json.dumps({'event':'recovered',**last}),flush=True)
print('ALL_SEQUENTIAL_RESTARTS_COMPLETED',flush=True)
