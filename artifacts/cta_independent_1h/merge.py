"""Merge the staged 1h definitions into the existing Prep process, as requested."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
sys.path.insert(0,'/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
import ops
HOME=Path('/home/ubuntu');MAIN=HOME/'cta-prep-binance-futures';HOUR=HOME/'cta-prep-binance-futures-1h'
UPLOAD=HOME/'cta-hourly-history/independent-upload';AUDIT=HOME/'cta-hourly-history/merged-deployment'


def sha(path):
    with path.open('rb') as stream:return hashlib.file_digest(stream,'sha256').hexdigest()


def checked(argv,cwd,name):
    result=subprocess.run(argv,cwd=cwd,capture_output=True,timeout=120)
    (AUDIT/(name+'.stdout')).write_bytes(result.stdout);(AUDIT/(name+'.stderr')).write_bytes(result.stderr)
    ops.require(result.returncode==0,'failed '+name+'; inspect private audit')
    return result


def atomic_copy(source,target):
    temporary=target.with_name('.'+target.name+'.merge.next');shutil.copy2(source,temporary);os.replace(temporary,target)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--execute',action='store_true');args=parser.parse_args()
    files=sorted(p for p in (HOUR/'config/cta_prep_binance_futures_1h').glob('*.json') if p.name!='symbol_groups.json')
    ops.require(len(files)==4,'expected four staged hourly tables')
    for p in files:
        d=ops.read(p);ops.require(d['interval']=='1h' and not d['signal']['enabled'],'hourly staging is already enabled')
        ops.require(not (MAIN/'config/cta_prep_binance_futures'/p.name).exists(),'hourly combination already exists in main')
    manifest=ops.read(UPLOAD/'sha256.json');ops.require(sha(UPLOAD/'cta-prep-binance-futures')==manifest['cta-prep-binance-futures'],'published worker checksum differs')
    print(json.dumps({'env':str(MAIN),'market':'binance-futures','hourly_symbols':34,'hourly_tables':4,'rules':326,
        'mutates':'restart existing Prep and shared config server; keep ledgers; retire temporary hourly process','execute':args.execute}),flush=True)
    if not args.execute:return
    ops.require(not AUDIT.exists(),'merge audit already exists; inspect partial execution')
    AUDIT.mkdir(mode=0o700)
    before={'catalog':ops.local_api('/api/config?worker=cta-prep-binance-futures'),'minute_state':ops.state(),
        'main_json':{str(p):sha(p) for p in (MAIN/'config/cta_prep_binance_futures').glob('*.json')}}
    with ops.session('lgq') as api:
        before['accounts']={source:api('/manager/api/catalog/accounts/'+source) for source in ['binance_exec_rx01','binance_exec_rx03']}
        ops.require(all(not any('-1h-' in b['position_strategy_name'] for b in a['bindings']) for a in before['accounts'].values()),'hourly positions already bound; review ledger ownership first')
    ops.save(AUDIT/'before.json',before)
    for p in [MAIN/'env.sh',MAIN/'cta-prep-binance-futures']+list((MAIN/'data/cta_prep_binance_futures/positions').glob('*.json')):
        dest=AUDIT/'original'/p.relative_to(HOME);dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(p,dest)
    checked([str(HOUR/'scripts/stop_cta_prep_binance_futures.sh'),'--execute'],HOUR,'stop-temporary-hourly')
    checked([str(MAIN/'scripts/stop_cta_prep_binance_futures.sh'),'--execute'],MAIN,'stop-main-prep')
    checked([str(MAIN/'scripts/stop_cta_factor_config.sh'),'--execute'],MAIN,'stop-config')
    for path,digest in before['main_json'].items():ops.require(sha(Path(path))==digest,'minute configuration changed during merge')
    for p in files:atomic_copy(p,MAIN/'config/cta_prep_binance_futures'/p.name)
    atomic_copy(UPLOAD/'cta-prep-binance-futures',MAIN/'cta-prep-binance-futures')
    env=MAIN/'env.sh';text=env.read_text()
    line='export CTA_PREP_HOURLY_CONFIG="'+str(HOUR/'config/cta_prep_binance_futures.toml')+'"'
    ops.require(line in text,'hourly catalog source changed')
    text=text.replace(line,'')
    ops.require('export CTA_HOURLY_HISTORY_DIR=' not in text,'history directory was changed by an operator')
    text+='\nexport CTA_HOURLY_HISTORY_DIR="/home/ubuntu/cta-hourly-history/current/data"\n'
    temp=env.with_name('.env.merged.next');temp.write_text(text);temp.chmod(env.stat().st_mode&0o777);os.replace(temp,env)
    checked([str(MAIN/'cta-prep-binance-futures'),'--config','config/cta_prep_binance_futures.toml','--check'],MAIN,'merged-worker-check')
    checked([str(MAIN/'scripts/start_cta_prep_binance_futures.sh'),'--execute'],MAIN,'start-main-prep')
    checked([str(MAIN/'scripts/start_cta_factor_config.sh'),'--execute'],MAIN,'start-config')
    current=ops.local_api('/api/config?worker=cta-prep-binance-futures')
    ops.require(current['plan']==before['catalog']['plan'],'logical plan changed during physical merge')
    for path,digest in before['main_json'].items():ops.require(sha(Path(path))==digest,'minute configuration changed')
    archive=HOME/'retired_data'/('cta-prep-binance-futures-1h_'+time.strftime('%Y%m%dT%H%M%SZ',time.gmtime()))/'runtime'
    archive.parent.mkdir(parents=True,mode=0o700);os.rename(HOUR,archive)
    ops.save(AUDIT/'merged.json',{'at_ms':time.time_ns()//1000000,'env':str(MAIN),'hourly_tables':[p.name for p in files],
        'temporary_runtime_archive':str(archive),'worker_sha256':sha(MAIN/'cta-prep-binance-futures'),
        'logical_plan_unchanged':True,'minute_ledgers_preserved':True,'hourly_enabled':False})
    print(json.dumps({'merged':True,'runtime':str(MAIN),'config_server':'existing 19220','calculations':current['unique_factor_count'],
        'temporary_runtime_archive':str(archive),'hourly_enabled':False}),flush=True)


if __name__=='__main__':main()
