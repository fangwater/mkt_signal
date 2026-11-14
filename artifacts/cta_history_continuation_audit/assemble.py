"""Assemble already-completed, checksum-verified symbol shards into one immutable package."""
import datetime
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

BASE=Path('/mnt/nvme-raid0-28t/fanghaizhou/cta_hourly_history')
RULES=Path('/home/fanghaizhou/ds/cta_lgq/standard')
PARTS=[BASE/'rx01_rx03_20261008_20261108',BASE/'heavy_symbols',BASE/'remaining_symbols']
NAMES=['rx01.json','rx03_top-5.json','rx03_top-20.json','rx03_last-20.json']
OUTPUT=BASE/'package/rx01_rx03_1h'


def digest(path):return hashlib.file_digest(path.open('rb'),'sha256').hexdigest()


def main():
    expected={r['标的']+'USDT' for name in NAMES for r in json.loads((RULES/name).read_text())}
    control=Path('artifacts/cta_history_continuation_audit/current_control_snapshot.json')
    selected={}
    for symbol in sorted(expected):
        for part in PARTS:
            meta=part/(symbol+'.json')
            if not meta.is_file():continue
            done=json.loads(meta.read_text());data=done['data'];path=part/data['file']
            assert done['control_sha256']==digest(control)
            assert digest(path)==data['sha256'],symbol
            selected[symbol]=(part,done);break
    missing=sorted(expected-set(selected))
    if missing:raise SystemExit('Not finished: '+', '.join(missing))
    assert not OUTPUT.exists(),'package output already exists; do not overwrite verified data'
    data_root=OUTPUT/'data';data_root.mkdir(parents=True)
    (OUTPUT/'rules').mkdir();(OUTPUT/'provenance/symbols').mkdir(parents=True)
    symbols={};bounds=set();selections={}
    for symbol,(part,done) in selected.items():
        bounds.add((done['start_ms'],done['end_ms']));data=done['data']
        shutil.copy2(part/data['file'],data_root/data['file'])
        (OUTPUT/'provenance/symbols'/(symbol+'.json')).write_text(json.dumps(done,indent=2)+'\n')
        symbols[symbol]=data;selections[symbol]=str(part)
    assert len(bounds)==1
    start,end=bounds.pop()
    manifest={'market':'binance-futures','interval':'1h','start_ms':start,'end_ms':end,
        'source':'Locally synthesized Tardis trades + incremental_book_L2; NQ uses last BBO mid or permitted trade close. Factual previous-month KLL and current shared Redis size thresholds.',
        'symbols':symbols}
    (data_root/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    for name in NAMES:shutil.copy2(RULES/name,OUTPUT/'rules'/name)
    shutil.copy2(control,OUTPUT/'provenance/current_control_snapshot.json')
    source_files=['src/bin/cta_hourly_history.rs','src/cta_factor_calc/hourly_history.rs',
        'src/cta_factor_calc/engine.rs','src/cta_factor_calc/runtime.rs',
        'src/cta_factor_calc/hourly_nq_seed.rs','src/cta_factor_calc/catalog.rs',
        'src/factor_pub/tardis_input.rs','src/factor_pub/trade_flow_feature_pub/local_baseline.rs',
        'crates/period_pbs/src/kafka.rs']
    provenance={'created_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'git_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
        'source_root':'/mnt/nfs/30.133_xt_crypto/TardisSource','source_sha256':{p:digest(Path(p)) for p in source_files},
        'rules_sha256':{n:digest(RULES/n) for n in NAMES},'selected_shards':selections,
        'control_snapshot_sha256':digest(control),'symbols':len(symbols),'hourly_rows':sum(d['rows'] for d in symbols.values())}
    (OUTPUT/'provenance/build.json').write_text(json.dumps(provenance,indent=2)+'\n')
    print(json.dumps({'output':str(OUTPUT),'symbols':len(symbols),'hourly_rows':provenance['hourly_rows'],
        'compressed_data_bytes':sum((data_root/d['file']).stat().st_size for d in symbols.values())},indent=2))


if __name__=='__main__':main()
