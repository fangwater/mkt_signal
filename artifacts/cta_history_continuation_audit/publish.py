"""Install the verified hourly package into a separate JP data directory."""
import argparse
import hashlib
import json
from pathlib import Path,PurePosixPath
import os
import socket
import subprocess
import tarfile
import time

BASE=Path('/home/ubuntu/cta-hourly-history')
ARCHIVE_SHA='bc01bcb710cfa4a0479a68f66f6cb9ffbc6a0adba5e37cd3ec46aa61ec7bd5a3'


def digest(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    archive=BASE/'.rx01_rx03_1h.tar.gz.upload';target=BASE/'rx01_rx03_1h_20261108'
    assert Path('/home/ubuntu/cta-prep-binance-futures').is_dir()
    assert Path('/home/ubuntu/cta-factor-spread').is_dir()
    assert digest(archive)==ARCHIVE_SHA,'transport checksum mismatch'
    assert not target.exists(),'refuse to overwrite an existing package'
    print(json.dumps({'host':socket.getfqdn(),'target':str(target),'archive_sha256':ARCHIVE_SHA,'execute':args.execute}),flush=True)
    if not args.execute:return
    incoming=BASE/('.incoming_'+str(time.time_ns()));incoming.mkdir(mode=0o700)
    with tarfile.open(archive,'r:gz') as source:
        for m in source.getmembers():
            p=PurePosixPath(m.name)
            assert not p.is_absolute() and '..' not in p.parts and p.parts[0]=='rx01_rx03_1h'
            assert m.isfile() or m.isdir(),'package links or special files forbidden'
    subprocess.run(['tar','-xzf',str(archive),'-C',str(incoming)],check=True)
    root=incoming/'rx01_rx03_1h';checked=0
    for line in (root/'SHA256SUMS').read_text().splitlines():
        expected,relative=line.split('  ',1);path=root/relative
        assert path.resolve().is_relative_to(root.resolve()) and path.is_file()
        assert digest(path)==expected,'package file checksum mismatch: '+relative
        checked+=1
    rules=['rx01.json','rx03_top-5.json','rx03_top-20.json','rx03_last-20.json']
    command=[str(root/'bin/cta-hourly-history'),'check','--history',str(root/'data')]
    for name in rules:command+=['--rules',str(root/'rules'/name)]
    result=subprocess.run(command,cwd=root,capture_output=True,text=True,timeout=120)
    assert result.returncode==0,'native package validation failed: '+result.stderr[-1000:]
    seed=json.loads(result.stdout)
    assert len(seed['formula_rows'])==34 and set(seed['formula_rows'].values())=={744}
    assert len(seed['ready_formulas'])==233 and len(seed['nq_rows'])==32 and not seed['missing_symbols']
    not_ready=[k for k,v in seed['ready_formulas'].items() if not v]
    assert not_ready==['binance-futures-dashusdt-1h-depth_imb_bps1']
    receipts=BASE/'receipts';receipts.mkdir(exist_ok=True)
    (receipts/'jp_seed_check.json').write_text(result.stdout)
    (receipts/'jp_seed_diagnostics.jsonl').write_text(result.stderr)
    os.rename(root,target);incoming.rmdir()
    final_archive=BASE/'rx01_rx03_1h_20261108.tar.gz';assert not final_archive.exists()
    os.rename(archive,final_archive)
    link=BASE/'.current.next';assert not link.exists()
    link.symlink_to(target.name);os.replace(link,BASE/'current')
    summary={'host':socket.getfqdn(),'installed_at_ms':time.time_ns()//1_000_000,'history':str(target/'data'),
        'current':str(BASE/'current/data'),'archive':str(final_archive),'archive_sha256':ARCHIVE_SHA,
        'archive_bytes':final_archive.stat().st_size,'verified_files':checked,'symbols':34,'rows':25296,
        'ready_formulas':232,'formula_count':233,'nq_count':32,'not_ready':not_ready,
        'live_worker_binaries_replaced':False,'live_processes_restarted':False}
    (receipts/'upload.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps(summary),flush=True)


if __name__=='__main__':main()
