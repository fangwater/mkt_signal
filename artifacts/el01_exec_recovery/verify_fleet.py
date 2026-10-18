import glob,os,json,time,tomllib,urllib.request,concurrent.futures,re
cfg=tomllib.load(open('/home/el01/crypto_cta_manager/config/cta-manager.toml','rb'))
def check(s):
 if not s.get('enabled',True):return
 base='/home/el01/'+s['id'];pids={}
 for p in glob.glob('/proc/[0-9]*'):
  try:
   if os.readlink(p+'/cwd')==base:
    comm=open(p+'/comm').read().strip()
    if comm in ['trade_engine','exec-pre-trade','account_monitor','persist_manager','viz_server']:pids[comm]=int(p.rsplit('/',1)[1])
  except (FileNotFoundError,ProcessLookupError,PermissionError):pass
 j=json.load(urllib.request.urlopen(s['exec_viz_url'].rstrip('/')+'/snapshot',timeout=8));state=next(e['entry'] for e in j['entries'] if e.get('type')=='exec_pre_trade_state');rows=state.get('rows',[])
 te_log='/home/el01/.pmdaemon/logs/exec_te_'+s['id']+'-error.log';health=''
 try:
  with open(te_log,'rb') as f:f.seek(max(0,os.path.getsize(te_log)-200000));lines=f.read().decode(errors='replace').splitlines()
  health=next((l for l in reversed(lines) if 'TcpHealthSummary:' in l),'')
 except FileNotFoundError:pass
 out={'env':s['id'],'pids':pids,'ready':state.get('position_ready'),'snapshot_age_ms':int(time.time()*1000)-state['ts_ms'],'rows':len(rows),'live_rows':sum(abs(r.get('live_order_qty',0))>1e-9 for r in rows),'tcp_health':health}
 print(json.dumps(out),flush=True)
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:list(ex.map(check,cfg['sources']))
