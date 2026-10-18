"""Install a verified local build; restart only trade06 exec-pre-trade."""
import datetime, hashlib, json, os, pathlib, shutil, subprocess, time, tomllib
root = pathlib.Path('/home/ubuntu/binance_exec_trade06')
release_id = '20261014T043000Z'
binary = root / 'exec-pre-trade'
upload = root / ('.exec-pre-trade.' + release_id + '.upload')
manifest_upload = root / ('.exec-pre-trade.' + release_id + '.manifest.upload')
manifest = json.loads(manifest_upload.read_text())
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def pids():
    found=[]
    for p in pathlib.Path('/proc').iterdir():
        if not p.name.isdigit(): continue
        try:
            if os.readlink(p/'cwd') == str(root) and os.readlink(p/'exe').removesuffix(' (deleted)') == str(binary): found.append(int(p.name))
        except OSError: pass
    return found
assert pids() == [manifest['old_pid']], 'live pre-trade process changed'
assert digest(binary) == manifest['previous_binary_sha256'], 'live binary changed'
assert digest(upload) == manifest['binary_sha256'], 'upload checksum mismatch'
assert tomllib.loads((root/'trade_engine.toml').read_text())['local_ips'] == ['172.31.35.228']
backup = root/'run'/('cash_market_buy_' + release_id)
backup.mkdir(mode=0o700)
protected = {p: digest(root/p) for p in ['env.sh','trade_engine.toml']}
for p in [binary, root/'release.json']:
    if p.exists(): shutil.copy2(p, backup/(p.name+'.before'))
shutil.copy2(manifest_upload, backup/'manifest.json')
os.chmod(upload, 0o755)
os.replace(upload, binary)
print('Installed checksum-verified artifact; restarting only trade06 pre-trade through env-local script.', flush=True)
restart_ts_us = time.time_ns()//1000
with (backup/'restart.log').open('wb') as log:
    os.chmod(backup/'restart.log', 0o600)
    run = subprocess.run([str(root/'scripts/start_exec_pre_trade.sh')], cwd=root, stdout=log, stderr=subprocess.STDOUT, timeout=180)
assert run.returncode == 0, 'restart wrapper failed; inspect protected restart.log'
new_pid = None
for _ in range(60):
    current = pids()
    if len(current) == 1 and current[0] != manifest['old_pid']:
        candidate=current[0]
        if digest(pathlib.Path('/proc')/str(candidate)/'exe') == manifest['binary_sha256']:
            new_pid=candidate; break
    time.sleep(0.5)
assert new_pid is not None, 'verified pre-trade process not running'
assert protected == {p:digest(root/p) for p in protected}, 'live configuration changed'
result = dict(release_id=release_id, new_pid=new_pid, old_pid=manifest['old_pid'], binary_sha256=digest(binary), restart_ts_us=restart_ts_us, backup=str(backup), affinity=sorted(os.sched_getaffinity(new_pid)), local_ips=['172.31.35.228'])
(backup/'installed.json').write_text(json.dumps(result, indent=2)+'\n')
release_path=root/'release.json'
release=json.loads(release_path.read_text()) if release_path.exists() else {}
release.update(pre_trade_release_id=release_id, pre_trade_base_commit=manifest['base_commit'], pre_trade_binary_sha256=manifest['binary_sha256'], pre_trade_source_patch_sha256=manifest['source_patch_sha256'], pre_trade_deployed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
tmp=root/'.release.json.cash-market-buy'
tmp.write_text(json.dumps(release,indent=2)+'\n'); os.replace(tmp,release_path)
manifest_upload.unlink()
print(json.dumps(result), flush=True)
