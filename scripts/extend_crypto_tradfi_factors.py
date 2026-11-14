#!/usr/bin/env python3
"""Start queued symbols using spare capacity while the original scheduler waits."""
import argparse
import concurrent.futures
import datetime
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--parent-pid', type=int, required=True)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    root = args.root
    stage = root / '_build_20261026'
    request = root / '_provenance/request_20261026.json'
    symbols = json.loads(request.read_text())['symbols']
    proc = Path('/proc') / str(args.parent_pid)
    expected = 'python ' + str(root / '_provenance/implementation_processes_20261026/scripts/run_crypto_tradfi.py') + ' '
    def validate_parent():
        command = (proc / 'cmdline').read_bytes().replace(b'\0', b' ').decode()
        assert command.startswith(expected) and '--factor-processes 64' in command, command
    validate_parent()
    baseline = json.loads((stage / '_audit/baseline_run.json').read_text())
    assert not baseline['failures'] and len(baseline['symbols']) == 223
    started = {path.stem for path in (stage / '_logs/factors_by_symbol').glob('*.log')}
    print(json.dumps({'unstarted_symbols': [s for s in symbols if s not in started], 'execute': args.execute}), flush=True)
    if not args.execute:
        return
    receipt_path = root / '_provenance/factor_parallel_extension_20261026.json'
    receipt = dict(parent_pid=args.parent_pid, started_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   status='pausing_scheduler', completed=[], failures=[])
    def save():
        tmp = receipt_path.with_suffix('.json.tmp')
        tmp.write_text(json.dumps(receipt, indent=2) + '\n')
        tmp.replace(receipt_path)
    save()
    os.kill(args.parent_pid, signal.SIGSTOP)
    try:
        deadline = time.monotonic() + 30
        while any((path / 'stat').read_text().split()[2] not in ('T', 't') for path in (proc / 'task').iterdir()):
            if time.monotonic() > deadline:
                raise TimeoutError('scheduler did not stop all its threads')
            time.sleep(0.05)
        # The scheduler creates each symbol log before spawning its Rust child.
        # With every scheduler thread stopped, no unlogged child can be started.
        started = {path.stem for path in (stage / '_logs/factors_by_symbol').glob('*.log')}
        extra = [s for s in symbols if s not in started]
        receipt.update(status='computing_unstarted_symbols', symbols=extra, reserved_original_symbols=len(started),
                       rule='Original scheduler stays paused until every extra child has exited; its existing children continue.')
        save()
        env = dict(os.environ, POLARS_MAX_THREADS='1', OPENBLAS_NUM_THREADS='1', OMP_NUM_THREADS='1')
        def run(symbol):
            log = stage / '_logs/factors_extension_by_symbol' / (symbol + '.log')
            log.parent.mkdir(exist_ok=True)
            cmd = [str(args.binary), '--manifest', str(request), '--out-root', str(stage),
                   '--phase', 'factors', '--symbols', symbol, '--workers', '1', '--execute']
            with log.open('a') as stream:
                result = subprocess.run(cmd, stdout=stream, stderr=subprocess.STDOUT, env=env)
            return symbol, result.returncode
        if extra:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(extra)) as pool:
                futures = [pool.submit(run, symbol) for symbol in extra]
                for future in concurrent.futures.as_completed(futures):
                    symbol, code = future.result()
                    if code:
                        receipt['failures'].append(dict(symbol=symbol, returncode=code))
                    else:
                        receipt['completed'].append(symbol)
                    save()
                    print('EXTENSION_DONE', symbol, 'returncode', code, flush=True)
        # Failed partial outputs are kept separately before the original queue
        # can retry them. A fully written marker is verified by the main runner.
        for failure in receipt['failures']:
            symbol = failure['symbol']
            if (stage / '_audit' / f'factors_{symbol}.json').exists():
                continue
            for minutes in [1, 5, 15]:
                source = stage / f'baseline_factor_{minutes}min/binance-futures' / symbol
                if source.exists():
                    target = root / '_diagnostics/extension_failed_partial' / f'baseline_factor_{minutes}min' / symbol
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.move(source, target)
        receipt['status'] = 'extension_finished'
        save()
    finally:
        validate_parent()
        os.kill(args.parent_pid, signal.SIGCONT)
        receipt['scheduler_resumed_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save()
        print('ORIGINAL_SCHEDULER_RESUMED', args.parent_pid, flush=True)


if __name__ == '__main__':
    main()
