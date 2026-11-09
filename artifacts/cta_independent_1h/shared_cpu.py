"""Move the hourly Prep onto shared CPUs without restarting it."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time
ROOT=Path('/home/ubuntu/cta-prep-binance-futures-1h')
AUDIT=Path('/home/ubuntu/cta-hourly-history/independent-deployment')


def atomic(path,raw,mode):
    temporary=path.with_name('.'+path.name+'.shared-cpu.next')
    with temporary.open('xb') as out:out.write(raw);out.flush();os.fsync(out.fileno())
    temporary.chmod(mode);os.replace(temporary,path)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    registry=Path.home()/'.pmdaemon/cta-prep-binance-futures-1h.json';raw=registry.read_bytes();config=json.loads(raw)
    assert Path(config['cwd'])==ROOT and config['name']=='cta-prep-binance-futures-1h'
    assert Path(config['script'])==ROOT/'cta-prep-binance-futures'
    print(json.dumps({'env':str(ROOT),'shared_cpus':'0-5','execute':args.execute}),flush=True)
    if not args.execute:return
    backup=AUDIT/'supervisor-before-shared-cpu.json'
    if not backup.exists():backup.write_bytes(raw);backup.chmod(0o600)
    config['env']['CTA_PREP_BINANCE_FUTURES_CORE']=''
    assert registry.read_bytes()==raw,'supervisor definition changed concurrently'
    atomic(registry,(json.dumps(config,indent=2)+'\n').encode(),registry.stat().st_mode&0o777)
    env=ROOT/'env.sh';text=env.read_text()
    text=text.replace('export CTA_PREP_BINANCE_FUTURES_CORE=19','export CTA_PREP_BINANCE_FUTURES_CORE=""').replace('unset CTA_PREP_BINANCE_FUTURES_CORE','export CTA_PREP_BINANCE_FUTURES_CORE=""')
    atomic(env,text.encode(),0o600)
    processes=[]
    for p in Path('/proc').glob('[0-9]*'):
        try:
            if (p/'cwd').resolve()!=ROOT or (p/'comm').read_text().strip()!='cta-prep-binanc':continue
            start=(p/'stat').read_text().split()[21]
            subprocess.run(['taskset','-apc','0-5',p.name],capture_output=True,check=True)
            assert (p/'stat').read_text().split()[21]==start
            masks={next(l.split(':',1)[1].strip() for l in t.read_text().splitlines() if l.startswith('Cpus_allowed_list:')) for t in (p/'task').glob('*/status')}
            assert masks=={'0-5'}
            processes.append({'pid':int(p.name),'start':start,'cpu_masks':sorted(masks)})
        except (FileNotFoundError,PermissionError):continue
    assert len(processes)==1
    receipt={'at_ms':time.time_ns()//1000000,'processes':processes,'restarted':False,'supervisor_core_override':'','shared_cpus':'0-5'}
    (AUDIT/'shared-cpu.json').write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps(receipt),flush=True)


if __name__=='__main__':main()
