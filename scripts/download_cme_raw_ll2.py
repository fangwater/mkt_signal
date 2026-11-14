#!/usr/bin/env python3
"""Download finalized Raw LL2 gzip sources before local Rust decompression.

Resume only the same source URL/size/ETag. Publish files atomically and retain
provenance; do not download upstream .part files or scan complete gzip payloads.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import threading
import time
import tomllib
from urllib.parse import quote


def now():
    return datetime.now(timezone.utc).isoformat()


def atomic_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + '.partial')
    with temporary.open('w') as output:
        json.dump(value, output, indent=2)
        output.write('\n')
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def source_record(source):
    return {key: source.get(key) for key in ('name', 'url', 'remote_bytes', 'etag')}


def local_bytes(directory, source):
    sizes = [0]
    for suffix in ('', '.download.partial'):
        try:
            sizes.append((directory / (source['name'] + suffix)).stat().st_size)
        except FileNotFoundError:
            pass  # A completed download can be published during a progress poll.
    return max(sizes)


def response_headers(path):
    blocks = re.split(r'\r?\n\r?\n', Path(path).read_text().strip())
    blocks = [block for block in blocks if block.startswith('HTTP/')]
    if not blocks:
        raise ValueError('missing HTTP response headers')
    lines = blocks[-1].splitlines()
    return int(lines[0].split()[1]), {
        key.strip().lower(): value.strip()
        for line in lines[1:] if ':' in line
        for key, value in [line.split(':', 1)]
    }


class Downloader:
    def __init__(self, reserve, retries=8):
        self.reserve = reserve
        self.retries = retries
        self.cancelled = threading.Event()
        self.lock = threading.Lock()
        self.processes = set()

    def cancel(self):
        self.cancelled.set()
        with self.lock:
            for process in self.processes:
                if process.poll() is None:
                    process.terminate()

    def download(self, directory, source):
        expected = source_record(source)
        final = directory / source['name']
        partial = final.with_name(final.name + '.download.partial')
        record = final.with_name(final.name + '.download.json')
        headers = final.with_name(final.name + '.headers.partial')
        size = source['remote_bytes']
        old = json.loads(record.read_text()) if record.exists() else None
        if old and old['source'] != expected:
            raise ValueError(f'source metadata changed: {final}; retained source is not overwritten')
        if final.exists():
            if not old or final.stat().st_size != size:
                raise ValueError(f'untracked or wrong-size local source: {final}')
            print(f'raw_download_reuse source={final.name} bytes={size}', flush=True)
            return dict(source=expected, path=str(final), status='reused', bytes=size)
        if partial.exists() and (not old or partial.stat().st_size > size):
            raise ValueError(f'untracked or oversized partial download: {partial}')
        atomic_json(record, dict(source=expected, status='downloading', started_at=now()))
        for attempt in range(1, self.retries + 1):
            if self.cancelled.is_set():
                raise RuntimeError('download cancelled')
            if shutil.disk_usage(directory).free < self.reserve:
                raise RuntimeError('source disk is below the configured free-space reserve')
            offset = partial.stat().st_size if partial.exists() else 0
            complete_partial = partial.exists() and offset == size
            print(f'raw_download_start source={final.name} offset={offset} bytes={size} attempt={attempt}', flush=True)
            command = [
                'curl', '--noproxy', '*', '--silent', '--show-error', '--fail',
                '--location', '--connect-timeout', '30', '--speed-time', '180',
                '--speed-limit', '1024', '--dump-header', str(headers),
            ]
            if complete_partial:
                # A stop after the final byte but before publication needs no body reread.
                command += ['--head', '--output', os.devnull]
            else:
                command += ['--continue-at', '-', '--output', str(partial)]
            if source.get('etag'):
                command += ['--header', 'If-Match: ' + source['etag']]
            command.append(source['url'])
            with self.lock:
                if self.cancelled.is_set():
                    raise RuntimeError('download cancelled')
                process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
                self.processes.add(process)
            _, error = process.communicate()
            with self.lock:
                self.processes.discard(process)
            if process.returncode == 0:
                status, received = response_headers(headers)
                if received.get('etag') != source.get('etag'):
                    raise ValueError(f'download response ETag changed: {final.name}')
                if offset and not complete_partial:
                    wanted = f'bytes {offset}-{size-1}/{size}'
                    if status != 206 or received.get('content-range') != wanted:
                        raise ValueError(f'invalid resume response: {final.name}')
                elif status != 200:
                    raise ValueError(f'invalid download response: {final.name}')
                response_size = size if complete_partial else size - offset
                if int(received.get('content-length', '-1')) != response_size or partial.stat().st_size != size:
                    raise ValueError(f'download size mismatch: {final.name}')
                with partial.open('rb') as output:
                    os.fsync(output.fileno())
                # Never overwrite a published source file, including on a concurrent run.
                os.link(partial, final)
                final.chmod(0o444)
                partial.unlink()
                headers.unlink()
                result = dict(source=expected, path=str(final), status='downloaded', bytes=size, completed_at=now())
                atomic_json(record, result)
                print(f'raw_download_done source={final.name} bytes={size}', flush=True)
                return result
            if self.cancelled.is_set():
                raise RuntimeError('download cancelled')
            http_status = None
            if process.returncode == 22 and headers.exists():
                try:
                    http_status = response_headers(headers)[0]
                except ValueError:
                    pass
            if process.returncode == 33 or (process.returncode == 22 and http_status not in (429, 500, 502, 503, 504)):
                raise RuntimeError(f'source HTTP error or resume unavailable: {final.name}: {error.strip()}')
            print(f'raw_download_retry source={final.name} error={error.strip()}', flush=True)
            if self.cancelled.wait(5):
                raise RuntimeError('download cancelled')
        raise RuntimeError(f'download retries exhausted: {final.name}')


def download_sources(config, workers, audit):
    if workers < 1:
        raise ValueError('download workers must be positive')
    if not config.get('data_root'):
        raise ValueError('local download requires data_root')
    root = Path(config['data_root']).resolve()
    for derived in (config['rocksdb_dir'], config.get('shard_root')):
        if derived and (Path(derived).resolve().is_relative_to(root) or root.is_relative_to(Path(derived).resolve())):
            raise ValueError('local source must be separate from staging and RocksDB')
    root.mkdir(parents=True, exist_ok=True)
    inventory = json.loads(Path(config['inventory']).read_text())
    remote = config['remote_root'].rstrip('/')
    if inventory['remote_root'].rstrip('/') != remote:
        raise ValueError('inventory remote root mismatch')
    files = []
    for period in config['periods']:
        package = next(p for p in inventory['packages'] if p['period'] == period)
        name = 'shanghai_evolution_futures_raw_market_depth_ric_list_0_rl2_' + period
        if package['directory'] != name or not re.fullmatch(r'\d{4}-\d{2}-\d{2}_\d{4}-\d{2}-\d{2}', period):
            raise ValueError('unexpected Raw source directory')
        directory = root / name
        directory.mkdir(exist_ok=True)
        for source in package['files']:
            filename = source['name']
            if not (re.fullmatch(r'merged-Data-part-\d{6}\.csv\.gz', filename)
                    or filename == 'merged-Report.csv.gz' or filename.endswith('.notes.txt')):
                continue
            if Path(filename).name != filename or source['url'] != f'{remote}/{name}/{quote(filename, safe="")}':
                raise ValueError('unexpected Raw source filename or URL')
            files.append((directory, source))
    if len({str(directory / source['name']) for directory, source in files}) != len(files):
        raise ValueError('duplicate Raw source files')
    reserve = config.get('min_free_bytes', 1_000_000_000_000)
    remaining = sum(max(0, source['remote_bytes'] - local_bytes(directory, source))
        for directory, source in files)
    if shutil.disk_usage(root).free < remaining + reserve:
        raise RuntimeError('insufficient space for finalized source files plus free-space reserve')
    progress_path = Path(audit).with_name('download_progress.json')
    downloader = Downloader(reserve)
    results = []
    # One writer per source root. The cleaner only ever opens published files read-only.
    with (root / 'source_download.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            pending = {pool.submit(downloader.download, directory, source) for directory, source in files}
            try:
                while pending:
                    completed, pending = wait(pending, timeout=10)
                    for future in completed:
                        results.append(future.result())
                    free = shutil.disk_usage(root).free
                    if free < reserve:
                        raise RuntimeError('source disk is below the free-space reserve')
                    downloaded = sum(local_bytes(directory, source) for directory, source in files)
                    progress = dict(updated_at=now(), status='downloading' if pending else 'complete',
                                    data_root=str(root), files=len(files), completed_files=len(results),
                                    downloaded_bytes=downloaded, expected_bytes=sum(s['remote_bytes'] for _, s in files),
                                    free_bytes=free, workers=workers)
                    atomic_json(progress_path, progress)
                    print('raw_download_progress ' + json.dumps(progress), flush=True)
            except BaseException:
                downloader.cancel()
                for future in pending:
                    future.cancel()
                raise
    result = dict(completed_at=now(), source_mode='local_gzip', data_root=str(root),
                  workers=workers, files=results, full_gzip_verification='not_run; CRC checked during Rust decompression')
    atomic_json(audit, result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--workers', type=int, default=8)
    parser.add_argument('--audit', type=Path, required=True)
    args = parser.parse_args()
    with args.config.open('rb') as source:
        config = tomllib.load(source)
    download_sources(config, args.workers, args.audit)


if __name__ == '__main__':
    main()
