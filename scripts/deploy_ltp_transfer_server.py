#!/usr/bin/env python3
"""Build locally and publish the independent JP LTP server; dry-run by default."""
import argparse
import hashlib
import json
from pathlib import Path
import shlex
import subprocess
import tempfile
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def output(args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


# Executed by Python on the runtime host. It never invokes Cargo or reads credentials.
INSTALL = r'''
import hashlib,json,os,pathlib,shutil,subprocess,sys,time,urllib.request
stage=pathlib.Path(sys.argv[1]); manifest=json.loads((stage/'manifest.json').read_text())
home=pathlib.Path.home(); env=home/'ltp_transfer_server'; delist=home/'delist_risk_server'
assert delist.is_dir(), 'wrong host: Delist environment is absent'
assert (env/'config/credentials.env').is_file(), 'install dedicated credentials privately first'
assert (env/'config/credentials.env').stat().st_mode & 0o777 == 0o600
sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
for name,expected in manifest['files'].items():
 assert sha(stage/name)==expected, 'uploaded SHA-256 mismatch: '+name
site=pathlib.Path('/etc/nginx/sites-enabled/crypto_proxy_4191.conf').resolve()
original_site=site.read_bytes()
page=delist/'web/delist_risk/index.html'; original_page=page.read_bytes()
snippet=pathlib.Path('/etc/nginx/snippets/ltp_transfer.conf')
backup=stage/'before'; backup.mkdir(mode=0o700)
(backup/'nginx_site.conf').write_bytes(original_site)
(backup/'delist_index.html').write_bytes(original_page)
if snippet.exists(): shutil.copy2(snippet,backup/'ltp_transfer.conf')
def processes():
 result={}
 for p in pathlib.Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   comm=(p/'comm').read_text().strip()
   if comm not in ('nginx','delist_risk_ser','trade_engine','pre_trade','trade_signal','account_monitor','viz_server','persist_manager','spread_pbs','depth_pub','redis-server','cta_web','fr_signal_dashb'):continue
   stat=(p/'stat').read_text().rsplit(')',1)[1].split()
   affinity=sorted(os.sched_getaffinity(int(p.name)))
   result[p.name]={'comm':comm,'start_ticks':stat[19],'cpus':affinity}
  except (OSError,ProcessLookupError):pass
 return result
before_processes=processes()
(stage/'processes_before.json').write_text(json.dumps(before_processes,indent=2))
def install(source,target,mode):
 target.parent.mkdir(parents=True,exist_ok=True)
 tmp=target.with_name('.'+target.name+'.new')
 shutil.copyfile(source,tmp); os.chmod(tmp,mode); os.replace(tmp,target)
for relative in ['ltp_transfer_server','scripts/start_ltp_transfer_server.sh','scripts/stop_ltp_transfer_server.sh','scripts/run_ltp_transfer_server.sh','scripts/update_ltp_transfer_boot.py','config/ltp_transfer_server.toml','web/index.html','web/app.js','web/app.css']:
 target=env/relative
 if target.exists():
  dest=backup/relative;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(target,dest)
 if relative=='config/ltp_transfer_server.toml' and target.exists(): continue
 install(stage/relative,target,0o755 if relative.endswith('.sh') or relative=='ltp_transfer_server' else 0o644)
candidate_site=original_site.decode()
include='    include /etc/nginx/snippets/ltp_transfer.conf;'
if include.strip() not in candidate_site:
 anchor='    include /etc/nginx/snippets/delist_access.conf;'
 assert candidate_site.count(anchor)==1, 'cannot identify Delist server include'
 candidate_site=candidate_site.replace(anchor,anchor+'\n'+include,1)
candidate_page=original_page.decode()
entry='        <a href="/delist/transfers/">资金划转</a>'
if 'href="/delist/transfers/"' not in candidate_page:
 anchor='        <a href="/delist/?view=cta" data-page="cta">CTA 策略与因子</a>'
 assert candidate_page.count(anchor)==1, 'cannot identify Delist navigation'
 candidate_page=candidate_page.replace(anchor,anchor+'\n'+entry,1)
(stage/'nginx_site.candidate').write_text(candidate_site)
(stage/'delist_index.candidate').write_text(candidate_page)
# A standalone parser test checks new locations before changing the running gateway.
validation=stage/'nginx_validation.conf'
validation.write_text('pid '+str(stage/'validation.pid')+'; events {} http { server { listen 127.0.0.1:18788; include '+str(stage/'nginx_ltp_transfer.conf')+'; }}')
subprocess.run(['sudo','-n','nginx','-t','-c',str(validation)],check=True)
readiness=subprocess.run([str(env/'scripts/run_ltp_transfer_server.sh'),'--check'],cwd=env,check=True,text=True,capture_output=True)
(stage/'readiness.json').write_text(readiness.stdout)
print('Read-only account/wallet/network verification passed')
# If this is a re-publish, preserve logs and stop only this environment through its wrapper.
if (backup/'ltp_transfer_server').exists():
 subprocess.run([str(env/'scripts/stop_ltp_transfer_server.sh')],cwd=env,check=True)
subprocess.run([str(env/'scripts/start_ltp_transfer_server.sh')],cwd=env,check=True)
healthy=False
for _ in range(40):
 try:
  with urllib.request.urlopen('http://127.0.0.1:8788/healthz',timeout=2) as r:healthy=r.status==200
 except OSError:pass
 if healthy:break
 time.sleep(.25)
assert healthy,'transfer server did not become healthy; gateway is unchanged'
assert site.read_bytes()==original_site and page.read_bytes()==original_page,'live gateway or navigation changed concurrently'
subprocess.run(['sudo','-n','install','-m','644',str(stage/'nginx_ltp_transfer.conf'),str(snippet)],check=True)
subprocess.run(['sudo','-n','install','-m','644',str(stage/'nginx_site.candidate'),str(site.with_name('.'+site.name+'.new'))],check=True)
subprocess.run(['sudo','-n','mv',str(site.with_name('.'+site.name+'.new')),str(site)],check=True)
try:
 subprocess.run(['sudo','-n','nginx','-t'],check=True)
 install(stage/'delist_index.candidate',page,0o644)
 subprocess.run(['sudo','-n','systemctl','reload','nginx'],check=True)
except Exception:
 subprocess.run(['sudo','-n','install','-m','644',str(backup/'nginx_site.conf'),str(site)],check=True)
 if (backup/'ltp_transfer.conf').exists():
  subprocess.run(['sudo','-n','install','-m','644',str(backup/'ltp_transfer.conf'),str(snippet)],check=True)
 install(backup/'delist_index.html',page,0o644)
 raise
after_processes=processes()
# Nginx worker replacement is expected after a graceful reload; its master must survive.
changed={pid:before for pid,before in before_processes.items() if before['comm']!='nginx' and after_processes.get(pid)!=before}
receipt={'host':os.uname().nodename,'source_commit':manifest['source_commit'],'binary_sha256':sha(env/'ltp_transfer_server'),'backup':str(backup),'unrelated_process_changes':changed,'health':healthy}
(stage/'receipt.json').write_text(json.dumps(receipt,indent=2))
print(json.dumps(receipt))
assert not changed,'an existing monitored process changed during deployment; inspect receipt'
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='jp-meta-elvpn', choices=['jp-meta-elvpn'])
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--skip-build', action='store_true', help='publish existing local release and built frontend')
    args = parser.parse_args()
    if not args.execute:
        print('DRY RUN: local release build; SHA-256 verified SCP; independent ~/ltp_transfer_server; Delist /delist/transfers/; no transfer is submitted.')
        return
    assert ROOT == Path('/home/fanghaizhou/mkt_signal'), 'production builds require the maintained local worktree'
    assert output(['git', 'branch', '--show-current']) == 'arbmm', 'production source must be arbmm'
    command(['git', 'fetch', 'origin', 'arbmm'], cwd=ROOT)
    assert output(['git', 'rev-list', '--left-right', '--count', 'HEAD...origin/arbmm']).split() == ['0', '0'], 'arbmm is not synchronized'
    source_commit = output(['git', 'rev-parse', 'HEAD'])
    if not args.skip_build:
        command(['npm', 'ci', '--no-audit', '--no-fund'], cwd=ROOT/'web/ltp_transfer/frontend')
        command(['npm', 'run', 'build'], cwd=ROOT/'web/ltp_transfer/frontend')
        command(['cargo', 'build', '--release', '-p', 'ltp_transfer_server'], cwd=ROOT)
    metadata = json.loads(output(['cargo', 'metadata', '--no-deps', '--format-version', '1']))
    binary = Path(metadata['target_directory'])/'release/ltp_transfer_server'
    assert binary.is_file(), 'missing locally built release artifact'
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    remote_home = output(['ssh', '-o', 'BatchMode=yes', args.host, 'printf %s "$HOME"'])
    remote_stage = f'{remote_home}/ltp_transfer_server/run/deploy_{stamp}'
    with tempfile.TemporaryDirectory(prefix='ltp-transfer-publish-') as temporary:
        stage = Path(temporary)
        sources = {'ltp_transfer_server': binary, 'nginx_ltp_transfer.conf': ROOT/'config/nginx_ltp_transfer.conf',
                   'config/ltp_transfer_server.toml': ROOT/'config/ltp_transfer_server.toml'}
        for name in ['start_ltp_transfer_server.sh', 'stop_ltp_transfer_server.sh', 'run_ltp_transfer_server.sh', 'update_ltp_transfer_boot.py']:
            sources[f'scripts/{name}'] = ROOT/'scripts'/name
        for name in ['index.html', 'app.js', 'app.css']:
            sources[f'web/{name}'] = ROOT/'web/ltp_transfer/dist'/name
        import shutil
        for relative, source in sources.items():
            target = stage/relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)
        manifest = {'source_commit': source_commit, 'files': {name: digest(path) for name, path in sources.items()},
                    'service_sources': {str(p.relative_to(ROOT)): digest(p) for p in (ROOT/'crates/ltp_transfer_server').rglob('*') if p.is_file()}}
        (stage/'manifest.json').write_text(json.dumps(manifest, indent=2))
        (stage/'install.py').write_text(INSTALL)
        command(['ssh', args.host, 'mkdir -p '+shlex.quote(remote_stage)])
        command(['scp', '-r', str(stage)+'/.', f'{args.host}:{remote_stage}/'])
        command(['ssh', args.host, 'python3 '+shlex.quote(remote_stage+'/install.py')+' '+shlex.quote(remote_stage)])
        receipt = ROOT/'artifacts/ltp_transfer_server'
        receipt.mkdir(parents=True, exist_ok=True)
        (receipt/'manifest.json').write_text(json.dumps(manifest, indent=2))
        command(['scp', f'{args.host}:{remote_stage}/receipt.json', str(receipt/'deployment.json')])
    print(f'Published: /delist/transfers/; runtime {remote_home}/ltp_transfer_server')


if __name__ == '__main__':
    main()
