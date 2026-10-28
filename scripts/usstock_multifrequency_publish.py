"""Publish six accepted US-stock coarse families with source and rename guards."""
import argparse
import json
from pathlib import Path
import shutil
import time

from usstock_multifrequency_validate import load, sha


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run',type=Path,required=True)
    p.add_argument('--staging',type=Path,required=True);p.add_argument('--destination',type=Path,required=True)
    p.add_argument('--execute',action='store_true');a=p.parse_args()
    data=json.loads((a.run/'data_validation.json').read_text());factor=json.loads((a.run/'factor_validation.json').read_text())
    acceptance=json.loads((a.run/'acceptance.json').read_text());build=json.loads((a.run/'factor_build.json').read_text())
    assert all(x['status']=='passed' for x in [data,factor,acceptance,build])
    totals={**data['totals'],**factor['totals']};assert len(totals)==6
    # Metadata binds every timestamp, duration and segment, so identical hashes
    # prove complete RAW/HFQ coarse-key alignment without another data scan.
    bar_keys={}
    for j in load(a.run/'data_checks.json.zst'):
        for output in j['outputs']:
            rel=Path(output['path']);bar_keys[str(rel)]=(output['rows'],output['metadata_sha256'])
    aligned=0
    for path,value in bar_keys.items():
        rel=Path(path);family=rel.parts[0]
        if family.endswith('_hfq_raw'):
            raw=str(Path(family.replace('_hfq_raw','_raw'))/Path(*rel.parts[1:]))
            assert bar_keys[raw]==value,('RAW/HFQ bar keys differ',path)
            aligned+=1
    assert aligned==147934
    for freq in ['1h','1day']:
        assert len({totals[f'{kind}_{freq}{suffix}_raw']['rows'] for kind,suffix in [('baseline_data',''),('baseline_data','_hfq'),('baseline_factor','_hfq')]})==1
    families=sorted(totals);jobs=json.loads((a.run/'jobs.json').read_text());byric={}
    for j in jobs:
        s=Path(j['input']).stat();assert (s.st_size,s.st_mtime_ns)==(j['source_bytes'],j['source_mtime_ns']),j['input']
        if j['hfq']:byric.setdefault((j['venue'],j['ric']),[]).append(j['day'])
    for family in families:
        assert totals[family]['files']==73967 and (a.staging/family).is_dir()
        assert not (a.destination/family).exists(),('destination exists',family)
    samples=[]
    for family in families:
        for (venue,ric),days in sorted(byric.items()):
            for day in [min(days),max(days)]:
                rel=Path(family)/venue/ric/f'{day}.parquet';samples.append(dict(path=str(rel),sha256=sha(a.staging/rel)))
    if not a.execute:print(json.dumps(dict(families=families,samples=len(samples),totals=totals)));return
    audit=a.destination/'_audit/usstock_multifrequency_20261026';assert not audit.exists();audit.mkdir(parents=True)
    mapping=[]
    for family in families:
        source=a.staging/family;target=a.destination/family;source.rename(target)
        mapping.append(dict(source=str(source),destination=str(target)))
        (a.run/'publish_progress.json').write_text(json.dumps(mapping,indent=2))
    for r in samples:assert sha(a.destination/r['path'])==r['sha256'],r['path']
    result=dict(status='published',published_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
                totals=totals,source_files_unchanged=len(jobs),raw_hfq_aligned_file_pairs=aligned,post_rename_hash_samples=len(samples),
                samples=samples,path_mapping=mapping,script_sha256=sha(Path(__file__)))
    (a.run/'publication.json').write_text(json.dumps(result,indent=2))
    (a.run/'status.json').write_text(json.dumps(dict(state='published',receipt=str(a.run/'publication.json')),indent=2))
    for name in ['manifest.json','calendar.csv','jobs.json','data_validation.json','factor_validation.json',
                 'data_checks.json.zst','factor_checks.json.zst','factor_availability.csv','acceptance.json',
                 'factor_build.json','publication.json','buffered_benchmark.json','benchmark.json',
                 'real_python_rust_comparison.json','pilot_independent_validation.json','factor_buffered_comparison.json',
                 'code_manifest.json','verification.json','source_coverage.csv','source_calendar_gaps.json']:
        shutil.copy2(a.run/name,audit/name)
    shutil.copy2(a.staging/'_audit/data_jobs.json.zst',audit/'producer_data_jobs.json.zst')
    shutil.copytree(a.run/'code',audit/'code')
    print(json.dumps(dict(status='published',totals=totals,samples=len(samples))),flush=True)


if __name__=='__main__':main()
