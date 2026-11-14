"""Bounded real-RAW replay benchmark; never uses the production output config."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reference import HEADER, open_text


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--calendar', type=Path, required=True)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--verify', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--messages', type=int, default=200000)
    p.add_argument('--workers', type=int, nargs='+', default=[1, 32])
    p.add_argument('--timeout', type=int, default=180)
    a = p.parse_args()
    if not 1 <= a.messages <= 500000 or any(not 1 <= n <= 32 for n in a.workers):
        p.error('bounded diagnostic: 1..500000 messages and 1..32 workers')
    a.output.mkdir(parents=True, exist_ok=False)
    sample = a.output / 'merged-Data-part-000000-shard-000000.csv'
    messages = rows = 0
    first = last = None
    rics = set()
    with open_text(a.source) as stream, sample.open('x', newline='') as dest:
        reader = csv.reader(stream)
        header = next(reader)
        if header != HEADER:
            raise ValueError('unexpected RAW header')
        writer = csv.writer(dest)
        writer.writerow(header)
        for row in reader:
            if row[0]:
                if messages >= a.messages:
                    break
                messages += 1
                first = first or row[2]
                last = row[2]
                rics.add(row[0])
            writer.writerow(row)
            rows += 1
    report = dict(source=str(a.source), messages=messages, rows=rows,
                  first=first, last=last, rics=sorted(rics),
                  sample_sha256=hashlib.file_digest(sample.open('rb'), 'sha256').hexdigest(),
                  binary_sha256=hashlib.file_digest(a.binary.open('rb'), 'sha256').hexdigest(),
                  calendar_sha256=hashlib.file_digest(a.calendar.open('rb'), 'sha256').hexdigest(),
                  policy='Complete message prefix only; source read-only; generated CSV; not full-shard throughput',
                  runs=[])
    print(json.dumps(report), flush=True)
    for i, workers in enumerate(a.workers):
        name = f'run-{i}-w{workers}'
        output = a.output / name
        config = a.output / f'{name}.toml'
        config.write_text('\n'.join([
            'period = "bounded-direction-benchmark"',
            f'inputs = [{json.dumps(str(sample))}]',
            f'rocksdb_dir = {json.dumps(str(output))}',
            f'direction_calendar = {json.dumps(str(a.calendar))}',
            'progress_every = 0', 'keep_temporary_column_families = false',
            f'workers = {workers}',
        ]) + '\n')
        timings = a.output / f'{name}.time'
        start = time.perf_counter()
        with (a.output / f'{name}.log').open('x') as log:
            result = subprocess.run(['/usr/bin/time', '-f', '%e %U %S %M', '-o', str(timings),
                                     str(a.binary), '--config', str(config)],
                                    stdout=log, stderr=subprocess.STDOUT, timeout=a.timeout)
        run = dict(workers=workers, elapsed=time.perf_counter()-start,
                   returncode=result.returncode, time=timings.read_text().strip())
        if result.returncode == 0:
            verify_args = [str(a.verify), '--rocksdb-dir', str(output)]
            if i:
                verify_args += ['--compare-to', str(a.output / f'run-0-w{a.workers[0]}')]
            verify = subprocess.run(verify_args,
                                    capture_output=True, text=True, timeout=a.timeout)
            run.update(verify_returncode=verify.returncode, verify=verify.stdout + verify.stderr)
        report['runs'].append(run)
        (a.output / 'summary.json').write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(run), flush=True)
        if result.returncode:
            raise RuntimeError(f'{name} failed; inspect retained log')
        if run.get('verify_returncode') != 0:
            raise RuntimeError(f'{name} verification/comparison failed; inspect summary')


if __name__ == '__main__':
    main()
