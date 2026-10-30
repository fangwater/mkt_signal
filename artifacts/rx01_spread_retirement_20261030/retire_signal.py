from pathlib import Path
import json,time,subprocess,os,shutil,socket
assert socket.gethostname().startswith("ip-172-31-35-228")
stage=Path.home()/"deploy-staging/rx01-retirement-20261030"
backup=Path((stage/"archive_path").read_text())
old=Path.home()/"binance-cta-spread-rx01"
status_path=Path.home()/"exec_volatility_binance/run/exec_volatility_status.json"
status=json.loads(status_path.read_text())
assert status["caught_up"] and not status["publishing"] and status["decode_errors"]==0
assert status["fresh_ready"][0]>=522 and status["fresh_ready"][1]>=360
assert time.time()*1000-status["updated_ms"]<3000
assert json.loads((Path.home()/"exec_volatility_binance/run/old_candidate_comparison.json").read_text())["all_matched"]
assert json.loads((old/"config/cta_spread.json").read_text())["enabled"] is False
(backup/"candidate_before_cutover.json").write_text(json.dumps(status,indent=2)+"\n")
registry=backup/"pmdaemon-before";registry.mkdir(mode=0o700)
for path in (Path.home()/".pmdaemon").glob("*.json"):
 try:data=json.loads(path.read_text())
 except (OSError,ValueError):continue
 if str(old) in json.dumps(data):shutil.copyfile(path,registry/path.name);(registry/path.name).chmod(0o600)
print("[CUTOVER] stopping the old RX01 stack through its environment-local wrapper",flush=True)
subprocess.run([str(old/"scripts/stop_cta_spread.sh"),"--execute"],cwd=old,check=True)
print("[CUTOVER] old stop wrapper completed; checking independent IPC ownership",flush=True)
deadline=time.monotonic()+30
while True:
 current=json.loads(status_path.read_text())
 if current["publishing"] and current["published"]>1000 and time.time()*1000-current["updated_ms"]<3000:break
 if time.monotonic()>deadline:raise RuntimeError("new publisher did not acquire IPC; old environment retained for rollback")
 time.sleep(.5)
assert current["pid"]==status["pid"] and current["publish_failed"]==0 and current["decode_errors"]==0
assert current["fresh_ready"][0]>=522 and current["fresh_ready"][1]>=360
old_processes=[]
for proc in Path("/proc").iterdir():
 if not proc.name.isdigit() or int(proc.name)==os.getpid():continue
 try:cwd=os.readlink(proc/"cwd")
 except OSError:continue
 if cwd==str(old):old_processes.append(int(proc.name))
assert not old_processes,old_processes
registered=[]
for path in (Path.home()/".pmdaemon").glob("*.json"):
 if path.name.endswith(".meta.json"):continue
 try:data=json.loads(path.read_text())
 except (OSError,ValueError):continue
 if str(old) in json.dumps(data):registered.append(path.name)
assert not registered,registered
inventory={str(p.relative_to(old)):dict(size=p.stat().st_size,inode=p.stat().st_ino,mtime_ns=p.stat().st_mtime_ns) for p in old.rglob("*") if p.is_file() and not p.is_symlink()}
archived=backup/old.name;assert not archived.exists()
old.rename(archived)
for name,row in inventory.items():
 stat=(archived/name).stat()
 assert (stat.st_size,stat.st_ino,stat.st_mtime_ns)==(row["size"],row["inode"],row["mtime_ns"]),name
(backup/"file_inventory.json").write_text(json.dumps(inventory,indent=2)+"\n");(backup/"file_inventory.json").chmod(0o600)
protected=json.loads((stage/"protected_processes.json").read_text());changes=[]
for row in protected:
 try:actual=(Path("/proc")/str(row["pid"])/"stat").read_text().rsplit(")",1)[1].split()[19]
 except OSError:actual=None
 if actual!=row["start_ticks"]:changes.append(row)
assert not changes,changes
result=dict(retired_env=str(old),archive=str(archived),files_preserved=len(inventory),bytes_preserved=sum(row["size"] for row in inventory.values()),data_files_preserved=sum(name.startswith("data/") for name in inventory),data_bytes_preserved=sum(row["size"] for name,row in inventory.items() if name.startswith("data/")),archive_method="same_filesystem_rename_inode_size_mtime_verified",old_processes=old_processes,old_registered_processes=registered,protected_processes_unchanged=len(protected),publisher_status=current)
(backup/"retirement_result.json").write_text(json.dumps(result,indent=2)+"\n")
(stage/"retirement_result.json").write_text(json.dumps(result,indent=2)+"\n")
print(json.dumps(result),flush=True)
