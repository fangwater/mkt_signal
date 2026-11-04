#!/usr/bin/env python3
"""Restore the two absent JP CTA factor keys from verified running snapshots."""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import re
import tarfile
import time
import tomllib

import redis

ROOT = Path.home()
SPECS = {
    "binance-cta-perp-rx01": ("cta_perp_15m_signal", "cta_perp_factor_model_15m_pub.toml", 38),
    "binance-cta-perp-rx03": ("cta_perp_signal", "cta_perp_factor_model_1m_pub.toml", 16),
}


def digest(data):
    return hashlib.sha256(data).hexdigest()


def process_identity(env, exe):
    matches = []
    for proc in Path('/proc').glob('[0-9]*'):
        try:
            if Path(os.readlink(proc / 'cwd')) != ROOT / env:
                continue
            if Path(os.readlink(proc / 'exe')).name != exe:
                continue
            stat = (proc / 'stat').read_text()
            start = stat[stat.rfind(')') + 2:].split()[19]
            matches.append((int(proc.name), start))
        except (OSError, IndexError):
            continue
    if len(matches) != 1:
        raise RuntimeError(f'{env}: expected one signal process, found {len(matches)}')
    return matches[0]


def capture(env, spec):
    exe, publisher, expected_count = spec
    base = ROOT / env
    identity = process_identity(env, exe)
    status = json.loads((base / 'run/cta_perp_status.json').read_bytes())
    config_raw = (base / 'config/cta_perp.json').read_bytes()
    config = json.loads(config_raw)
    pub_raw = (base / 'config' / publisher).read_bytes()
    universe = tomllib.loads(pub_raw.decode())['universe']
    factors = status['symbol_factors']
    if not -2 <= time.time() - status['updated_ts_us'] / 1e6 <= 15:
        raise RuntimeError(f'{env}: stale or future status')
    if not status['enabled'] or status.get('configuration_error'):
        raise RuntimeError(f'{env}: signal disabled or configuration error')
    if status['factor_config_key'] != env + ':cta_symbol_factors':
        raise RuntimeError(f'{env}: unexpected factor key')
    if status.get('factor_rules') != config.get('factor_rules') or not config.get('factor_rules'):
        raise RuntimeError(f'{env}: active rules do not match disk configuration')
    if not isinstance(factors, dict) or len(factors) != expected_count:
        raise RuntimeError(f'{env}: unexpected factor table')
    if not set(factors).issubset(universe['symbols']):
        raise RuntimeError(f'{env}: unsupported publisher symbol')
    for symbol, row in factors.items():
        if not re.fullmatch('[A-Z0-9]{1,32}', symbol) or not isinstance(row, dict) or not 1 <= len(row) <= 32:
            raise RuntimeError(f'{env}: invalid symbol or row')
        if not set(row).issubset(universe['factors']):
            raise RuntimeError(f'{env}: unsupported publisher factor')
        if not all(isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) for v in row.values()) or not any(row.values()):
            raise RuntimeError(f'{env}: invalid factor weights')
    archive_dir = ROOT / 'delist_risk_server/data/cta_backups/jp' / env
    archives = sorted(archive_dir.glob('*T*.tar.gz'))
    if not archives:
        raise RuntimeError(f'{env}: no manual backup')
    with tarfile.open(archives[-1], 'r:gz') as archive:
        old = json.load(archive.extractfile(f'files/signal/{env}/run/cta_perp_status.json'))
    if old.get('symbol_factors') != factors or old.get('factor_rules') != status['factor_rules']:
        raise RuntimeError(f'{env}: snapshot differs from last verified backup')
    payload = json.dumps(factors, sort_keys=True, separators=(',', ':'), allow_nan=False)
    return {'identity': identity, 'payload': payload, 'rules': status['factor_rules'],
            'config_sha256': digest(config_raw), 'publisher_sha256': digest(pub_raw),
            'snapshot_utc_us': status['updated_ts_us'], 'archive': str(archives[-1])}


def stable(a, b):
    return all(a[k] == b[k] for k in ('identity', 'payload', 'rules', 'config_sha256', 'publisher_sha256', 'archive'))


def save(path, obj):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--audit-dir', required=True)
    args = parser.parse_args()
    audit = Path(args.audit_dir)
    if not audit.is_absolute() or audit.exists():
        raise RuntimeError('audit directory must be a new absolute path')
    db = redis.Redis(host='127.0.0.1', port=6379, db=0, decode_responses=True, socket_timeout=5, socket_connect_timeout=5)
    captured = {env: capture(env, spec) for env, spec in SPECS.items()}
    keys = [env + ':cta_symbol_factors' for env in SPECS]
    if any(db.exists(k) for k in keys):
        raise RuntimeError('one or more keys already exist; no write attempted')
    if not args.execute:
        print(json.dumps({'mode': 'read-only', 'keys_absent': keys, 'counts': {e: len(json.loads(v['payload'])) for e, v in captured.items()}, 'pids': {e: v['identity'][0] for e, v in captured.items()}}))
        return
    audit.mkdir(mode=0o700)
    save(audit / 'preflight.json', captured)
    with db.pipeline() as pipe:
        pipe.watch(*keys)
        if any(pipe.exists(k) for k in keys):
            raise RuntimeError('key appeared before transaction; no write attempted')
        again = {env: capture(env, spec) for env, spec in SPECS.items()}
        if not all(stable(captured[e], again[e]) for e in SPECS):
            raise RuntimeError('signal, rules or configuration changed; no write attempted')
        pipe.multi()
        for env in SPECS:
            pipe.set(env + ':cta_symbol_factors', captured[env]['payload'])
        if pipe.execute() != [True, True]:
            raise RuntimeError('unexpected transaction response; inspect Redis')
    for env in SPECS:
        if db.get(env + ':cta_symbol_factors') != captured[env]['payload']:
            raise RuntimeError(f'{env}: read-back mismatch; inspect Redis')
    save(audit / 'receipt.json', {'completed_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'keys': keys,
         'payload_sha256': {e: digest(v['payload'].encode()) for e, v in captured.items()},
         'pids_before': {e: v['identity'][0] for e, v in captured.items()},
         'pids_after': {e: process_identity(e, SPECS[e][0])[0] for e in SPECS}})
    print(json.dumps({'mode': 'executed', 'verified_keys': keys, 'audit_dir': str(audit)}))


if __name__ == '__main__':
    main()
