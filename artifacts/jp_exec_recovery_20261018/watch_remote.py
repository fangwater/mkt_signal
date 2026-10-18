import urllib.request,json,time
for i in range(180):
 result=[];complete=True
 for name,k,port in [("rx01","spread",10186),("rx02","perp",10196),("rx03","perp",10197)]:
  try:
   d=json.load(urllib.request.urlopen(f"http://127.0.0.1:{port}/api/snapshot",timeout=5))
   s=d["status"];rows=[r for fs in s["strategies"].values() for r in fs.values()]
   ready=sum(bool(r.get("score_ready")) for r in rows)
   fresh=sum(time.time()*1000-r.get("model_ts_ms",0)<130000 for r in rows)
   error=s.get("publication_error")
   result.append(f"{name}:{ready}/{len(rows)} ready,{fresh} fresh,error={error}")
   complete &= bool(rows) and ready==len(rows) and fresh==len(rows) and not error
  except Exception as e:result.append(f"{name}:{type(e).__name__}");complete=False
 print(time.strftime("%H:%M:%S",time.gmtime())," | ".join(result),flush=True)
 if complete:break
 time.sleep(20)
else:raise SystemExit("signal warmup monitor timed out")
