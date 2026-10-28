"""Supervise a frozen Nasdaq baseline build, validate, then publish atomically."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback

from usstock_nasdaq_validate import sha, validate


def now():return datetime.now(timezone.utc).isoformat()


def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(value,indent=2)+'\n')
    temporary.replace(path)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',type=Path,required=True);p.add_argument('--jobs',type=Path,required=True)
    p.add_argument('--staging',type=Path,required=True);p.add_argument('--publish',type=Path,required=True)
    p.add_argument('--status',type=Path,required=True);p.add_argument('--workers',type=int,default=24)
    p.add_argument('--resume',action='store_true')
    p.add_argument('--resume-build-id',action='append',default=[])
    a=p.parse_args()
    if a.publish.exists():raise ValueError('publication target already exists')
    if a.staging.exists() and not a.resume:raise ValueError('staging exists; explicit resume required')
    a.staging.mkdir(parents=True,exist_ok=True)
    if a.staging.parent.stat().st_dev!=a.publish.parent.stat().st_dev:
        raise ValueError('publication must be an atomic rename on one filesystem')
    jobs=json.loads(a.jobs.read_text());expected=sum(len(j['days']) for j in jobs)
    state=dict(started_at=now(),updated_at=now(),state='building',runner_pid=os.getpid(),
               staging=str(a.staging),publish=str(a.publish),jobs=str(a.jobs),jobs_sha256=sha(a.jobs),
               binary=str(a.binary),binary_sha256=sha(a.binary),expected_files=expected,
               completed_files=0,workers=a.workers,nfs_transport='mounted filesystem; no proxy')
    atomic_json(a.status,state)
    try:
        env=dict(os.environ)
        for key in list(env):
            if key.lower() in ('http_proxy','https_proxy','all_proxy'):env.pop(key)
        command=[str(a.binary),'--jobs',str(a.jobs),'--output-root',str(a.staging),'--workers',str(a.workers)]
        if a.resume:command.append('--resume')
        for build in a.resume_build_id:command.extend(['--resume-build-id',build])
        with subprocess.Popen(command,env=env) as child:
            state['worker_pid']=child.pid;atomic_json(a.status,state)
            while True:
                try:rc=child.wait(timeout=30);break
                except subprocess.TimeoutExpired:
                    state.update(updated_at=now(),completed_files=sum(1 for _ in a.staging.glob('_audit/days/NASDAQ/*/*.json')))
                    errors=list(a.staging.glob('_audit/errors/*.json'))
                    state['ric_errors']=[json.loads(f.read_text()) for f in errors]
                    state['quarantined_dates']=sum(1 for _ in a.staging.glob('_audit/day_errors/NASDAQ/*/*.json'))
                    atomic_json(a.status,state)
        state.update(updated_at=now(),completed_files=sum(1 for _ in a.staging.glob('_audit/days/NASDAQ/*/*.json')),
                     quarantined_dates=sum(1 for _ in a.staging.glob('_audit/day_errors/NASDAQ/*/*.json')))
        if rc and state['quarantined_dates']:
            state.update(state='needs_source_review',finished_at=now(),
                         error='usable dates processed; quarantined dates require source review; no publication')
            atomic_json(a.status,state)
            print(json.dumps(state),flush=True)
            return
        if rc:raise RuntimeError(f'baseline worker failed with exit {rc}; staging retained')
        state.update(state='validating',updated_at=now(),completed_files=expected);atomic_json(a.status,state)
        report=validate(a.staging,a.jobs,workers=8,accepted_build_ids=[state['binary_sha256'],*a.resume_build_id])
        atomic_json(a.staging/'_audit/validation.json',report)
        if a.publish.exists():raise ValueError('publication target appeared during build')
        a.staging.rename(a.publish)
        state.update(state='published',updated_at=now(),finished_at=now(),validation=report,
                     audit=str(a.publish/'_audit/validation.json'))
        atomic_json(a.status,state)
        print(json.dumps(dict(state='published',path=str(a.publish),totals=report['totals'])),flush=True)
    except Exception as e:
        state.update(state='failed',updated_at=now(),error=str(e),traceback=traceback.format_exc())
        atomic_json(a.status,state)
        raise


if __name__=='__main__':main()
