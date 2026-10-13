#!/usr/bin/env python3
"""Operator-run RX02 retirement only. Default is read-only; --execute causes trades."""
import argparse
import datetime
import fcntl
import importlib.util
import json
import os
from pathlib import Path
import time

from activate_cta_perp_rx02 import pids, positions, read, require, write


def retirement_plan(current, desired, state):
    retained = {}
    for symbol, factors in current.items():
        for factor, weight in factors.items():
            if weight and desired.get(symbol, {}).get(factor, 0):
                require(weight * desired[symbol][factor] > 0,
                        f"sign changed for retained pair: {symbol}/{factor}")
                retained.setdefault(symbol, {})[factor] = weight
    pairs = {(s, f) for s, row in current.items() for f, w in row.items() if w}
    pairs |= {(s, f) for f, row in state.items() for s in row}
    retired = sorted((s, f) for s, f in pairs if f not in retained.get(s, {}))
    return retained, retired


def retired_targets_zero(state, retired):
    # Missing state is not evidence that a previously published target was cleared.
    return all(s in state.get(f, {}) and state[f][s].get('qty') == 0
               and state[f][s].get('flag') == 0 for s, f in retired)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    new = Path(__file__).resolve().parents[1]
    require(new.name == 'binance-cta-perp-rx02', 'run the installed environment-local script')
    old = new.parent / 'binance-cta-special-rx02'
    require(not (new / 'run/cutover_started.json').exists(), 'cutover has already started; inspect manually')
    cfg = read(old / 'config/cta_special.json')
    require(cfg['enabled'] is True, 'old RX02 publication must be running for retirement')
    require(read(new / 'config/cta_perp.json')['enabled'] is False, 'new publication must stay disabled')
    require(pids(old, 'cta_special_signal'), 'old RX02 signal is not running')
    status = read(old / 'run/cta_special_status.json')
    require(0 <= time.time() * 1e6 - status['updated_ts_us'] < 15e6, 'old signal status is stale')
    require(status['enabled'] and status.get('publication_error') is None, 'old publisher is unhealthy')
    require(status['manager_strategy_prefix'] == 'lgq_cta_' and
            status['follower_source_id'] == 'binance_exec_trade05', 'unexpected RX02 destination')
    desired = read(new / 'config/cta_perp_symbol_factors.json')
    state_path = old / 'run/cta_special_position_flags.json'
    state = positions(state_path)
    spec = importlib.util.spec_from_file_location('rx02_legacy_factor_config', old / 'scripts/cta_special_factor_config.py')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)
    import redis
    client = redis.Redis(host=os.getenv('REDIS_HOST', '127.0.0.1'), port=int(os.getenv('REDIS_PORT', '6379')),
                         db=int(os.getenv('REDIS_DB', '0')), username=os.getenv('REDIS_USERNAME'),
                         password=os.getenv('REDIS_PASSWORD'), socket_timeout=3)
    current = helper.read_factor_table(client, old.name)
    require(current['exists'], 'old Redis factor table is missing')
    table = current['symbol_factors']
    disk_path = old / 'config/cta_special_symbol_factors.json'
    require(read(disk_path) == table, 'old disk and Redis tables differ; inspect before retirement')
    retained, retired = retirement_plan(table, desired, state)
    require(retained == {'NEARUSDT': {'factor_036': -1}}, 'unexpected retained scope; regenerate plan')
    require(len(retired) == 31, 'expected 31 retired pairs; inspect changed inventory')
    require(all(s in state.get(f, {}) for s, f in retired), 'retirement state is incomplete')
    universe = helper.available_universe(old / 'config/cta_special_factor_model_1m_pub.toml')
    helper.validate_available(retained, universe)
    print('[PLAN] JP RX02 Binance futures -> binance_exec_trade05; retire 31 old symbol x factor pairs')
    print('[KEEP] NEARUSDT / factor_036; preserve normal operation of this existing pair')
    for symbol, factor in retired:
        print(f'[RETIRE] {symbol:12s} {factor:14s} target_qty={state[factor][symbol]["qty"]:.12g} -> 0')
    print('[PLAN] shrink OLD Redis + disk factor table; old signal publishes zero targets; new Perp stays disabled')
    print('[NOTE] targets are strategy state, not confirmed exchange positions')
    if not args.execute:
        print('[DRY-RUN] no Redis, config, process, Manager or order changes')
        return
    lock = (new / 'run/cutover.lock').open('w')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    require(not (new / 'run/cutover_started.json').exists(), 'cutover started during preflight')
    require(read(new / 'config/cta_perp_symbol_factors.json') == desired, 'new scope changed')
    require(read(new / 'config/cta_perp.json')['enabled'] is False, 'new publisher was enabled')
    require(read(old / 'config/cta_special.json') == cfg and read(disk_path) == table,
            'old config changed during preflight')
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    backup = new / 'run' / ('retire_old_bindings_' + stamp)
    backup.mkdir()
    write(backup / 'before.json', {'redis': current, 'disk': table, 'config': cfg,
                                 'positions': positions(state_path), 'desired': desired, 'retained': retained})
    started_us = time.time() * 1e6
    try:
        helper.save_factor_table(client, old.name, retained, current['revision'], universe)
        write(disk_path, retained)
        for _ in range(90):
            time.sleep(1)
            status = read(old / 'run/cta_special_status.json')
            live_table = status['symbol_factors']
            if (status['updated_ts_us'] > started_us + 10e6 and status['enabled'] and
                    status.get('publication_error') is None and live_table == retained and
                    retired_targets_zero(positions(state_path), retired)):
                write(backup / 'targets_zero.json', {'time': time.time(), 'retired': retired,
                                                    'status_updated_ts_us': status['updated_ts_us']})
                print('[TARGETS ZERO] old publisher accepted reduced scope and published zero targets without error')
                print('[VERIFY] inspect Manager deliveries and Exec positions/fills; zero targets do not prove fills')
                return
        raise RuntimeError('timed out waiting for zero targets and healthy publication')
    except BaseException:
        print(f'[INSPECT] backup: {backup}; retirement may already be active; do not restore old scope blindly')
        raise


if __name__ == '__main__':
    try:
        main()
    except RuntimeError as error:
        raise SystemExit(f'[BLOCKED] {error}') from None
