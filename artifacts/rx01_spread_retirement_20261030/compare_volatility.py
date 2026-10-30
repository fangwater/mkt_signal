from pathlib import Path
import subprocess,time,json
root=Path.home()/"exec_volatility_binance"
observations={}
processes=[]
for cash,market in ((False,"futures"),(True,"margin")):
 output=(root/"run"/("comparison_"+market+".txt")).open("w")
 process=subprocess.Popen([str(root/"model_pub_probe"),"cta_spread-binance-"+market+"-5s-vol","--buffer-size","4096","--expected-symbols","BTCUSDT,ETHUSDT","--timeout-s","8","--interval-s","4"],cwd=root,stdout=output,stderr=subprocess.DEVNULL)
 processes.append((cash,market,process,output))
while any(process.poll() is None for _,_,process,_ in processes):
 status=json.loads((root/"run/exec_volatility_status.json").read_text())
 for sample in status["samples"]:
  observations[(sample["cash"],sample["symbol"],sample["ts_ms"])]=sample["value"]
 time.sleep(.05)
end_ms=time.time()*1000
results=[]
for cash,market,process,output in processes:
 output.close();assert process.returncode==0
 text=(root/"run"/("comparison_"+market+".txt")).read_text()
 final=text.split("[FINAL] service=")[-1]
 for line in final.splitlines():
  row=line.split()
  if row and row[0] in ("BTCUSDT","ETHUSDT"):
   assert row[1:3]==["true","true"] and row[4]=="OK"
   ts=round((end_ms-float(row[-1]))/5000)*5000
   candidate=observations.get((cash,row[0],ts));score=float(row[5])
   delta=abs(candidate-score) if candidate is not None else None
   results.append(dict(cash=cash,symbol=row[0],ts_ms=ts,original_rounded_score=score,candidate=candidate,absolute_delta=delta,matched=delta is not None and delta<=5.01e-9))
report=dict(samples=results,all_matched=len(results)==4 and all(row["matched"] for row in results))
(root/"run/old_candidate_comparison.json").write_text(json.dumps(report,indent=2)+"\n")
print(json.dumps(report))
if not report["all_matched"]:raise SystemExit(1)
