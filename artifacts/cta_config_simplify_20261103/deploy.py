"""Replace only the CTA Config executable; preserve factor processes and configurations."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

H=Path.home();ROOT=H/'cta-prep-binance-futures';TARGET=ROOT/'cta-factor-config'
sys.path.insert(0,str(H/'crypto_cta_manager/maintenance-backups/rx03_rx04_taker_clear_20261102T144523Z/scripts'))
from manager_ops import session
from manager_entries import protected_processes


def require(ok,message):
    if not ok:raise RuntimeError(message)


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


def save(path,data):
    temporary=path.with_name(path.name+'.next')
    with temporary.open('w') as f:
        os.fchmod(f.fileno(),0o600);json.dump(data,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    os.replace(temporary,path)


def config_pids():
    found=[]
    for p in Path('/proc').glob('[0-9]*'):
        try:
            if (p/'cwd').resolve()!=ROOT:continue
            if Path((p/'cmdline').read_bytes().split(b'\0')[0].decode()).name=='cta-factor-config':found.append(p.name)
        except (OSError,UnicodeError):pass
    return found


def calculation_identity():
    found=[]
    for p in Path('/proc').glob('[0-9]*'):
        try:
            if (p/'cwd').resolve()!=ROOT:continue
            if Path((p/'cmdline').read_bytes().split(b'\0')[0].decode()).name!='cta-prep-binance-futures':continue
            fields=(p/'stat').read_text().rsplit(')',1)[1].split()
            found.append({'pid':p.name,'start_ticks':fields[19]})
        except (OSError,UnicodeError,IndexError):pass
    return found


def configs():
    files={}
    for name,directory in [('cta-prep-binance-futures','cta_prep_binance_futures'),('cta-factor-spread','cta_factor_spread')]:
        root=H/name
        for path in [root/'env.sh',root/'config'/(directory+'.toml'),*[p for p in (root/'config'/directory).glob('*.json')]]:files[str(path)]=sha(path)
    return files


def revisions():
    with session('lgq') as api:
        return {worker:api('/cta/cta-prep-binance-futures/config/api/config?worker='+worker)['revision'] for worker in ['cta-prep-binance-futures','cta-factor-spread']}


def run_script(name,audit):
    r=subprocess.run([str(ROOT/'scripts'/name),'--execute'],cwd=ROOT,capture_output=True,timeout=55)
    (audit/(name+'.log')).write_bytes(r.stdout+r.stderr)
    require(r.returncode==0,'env-local Config lifecycle script failed; see private log')
    print(json.dumps({'script':name,'completed':True}),flush=True)


def verify(audit,expected):
    before=json.loads((audit/'before.json').read_text());current=protected_processes()
    require(all(current.get(pid)==row for pid,row in before['protected_processes'].items()),'protected factor or trading process changed')
    require(configs()==before['config_hashes'],'runtime settings changed')
    require(calculation_identity()==before['calculation_identity'] and len(calculation_identity())==1,'Prep calculation process changed')
    require(sha(TARGET)==expected,'installed binary checksum mismatch')
    require(revisions()==before['revisions'],'a strategy configuration changed')
    with session('lgq') as api:
        html=api('/cta/cta-prep-binance-futures/config/')
        require(html['status']==200,'configuration page unavailable')
    result={'protected_processes_unchanged':len(before['protected_processes']),'config_pids':config_pids(),'runtime_configs_unchanged':True,'revisions_unchanged':True,'calculation_identity':calculation_identity(),'sha256':expected,'at_ms':time.time_ns()//1000000}
    require(len(result['config_pids'])==1,'expected one Config process')
    save(audit/'verified.json',result);print(json.dumps(result),flush=True)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('phase',choices=['prepare','install','verify']);p.add_argument('--audit',required=True,type=Path);p.add_argument('--sha256',required=True);p.add_argument('--execute',action='store_true');a=p.parse_args();audit=a.audit
    os.umask(0o077)
    require(audit.parent==H/'crypto_cta_manager/maintenance-backups' and audit.name.startswith('cta_config_simplify_'),'unexpected audit directory')
    uploaded=audit/'cta-factor-config.upload'
    if a.phase=='prepare':
        require(sha(uploaded)==a.sha256,'uploaded binary checksum mismatch')
        checked=subprocess.run([str(uploaded),'--config',str(ROOT/'config/cta_prep_binance_futures.toml'),'--spread-config',str(H/'cta-factor-spread/config/cta_factor_spread.toml'),'--check'],capture_output=True,timeout=30)
        require(checked.returncode==0,'published Config validation failed')
        exclude=set(config_pids());protected={pid:row for pid,row in protected_processes().items() if str(pid) not in exclude}
        save(audit/'before.json',{'old_binary_sha256':sha(TARGET),'calculation_identity':calculation_identity(),'protected_processes':protected,'config_hashes':configs(),'revisions':revisions()})
        shutil.copy2(TARGET,audit/'cta-factor-config.before')
        print(json.dumps({'prepared':True,'protected_processes':len(protected)}),flush=True);return
    if a.phase=='verify':verify(audit,a.sha256);return
    require(a.execute,'replacement requires --execute')
    before=json.loads((audit/'before.json').read_text());require(sha(TARGET)==before['old_binary_sha256'] and configs()==before['config_hashes'] and revisions()==before['revisions'],'operator changed configuration after preparation')
    require(sha(uploaded)==a.sha256,'uploaded binary changed')
    run_script('stop_cta_factor_config.sh',audit)
    for _ in range(50):
        if not config_pids():break
        time.sleep(.1)
    require(not config_pids(),'old Config process is still alive')
    next_path=TARGET.with_name(TARGET.name+'.next');shutil.copyfile(uploaded,next_path);next_path.chmod(0o755);os.replace(next_path,TARGET)
    run_script('start_cta_factor_config.sh',audit)
    verify(audit,a.sha256)


if __name__=='__main__':main()
