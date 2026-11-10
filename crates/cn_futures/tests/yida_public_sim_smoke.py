#!/usr/bin/env python3
"""Exercise the release runtime against the official public SHFE/INE simulator.

Credentials come from YIDA_* environment variables. Default: login and inspect.
--execute tests one-lot ordinary orders and cancellation, then reconciles.
--option covers both option directions; --market adds native market types.
--native-controls covers reference cancellation, routing and native batches.
Legitimate unfilled option IOC/FOK outcomes use separately recorded market
seed/cleanup orders so each requested close type can be tested. Only this run's orders and new position are eligible for cleanup.
"""
from native_persistence_check import verify_persistence
import argparse
import hashlib
import math
import json
import os
from pathlib import Path
import queue
import subprocess
import threading
import time


class Runtime:
    def __init__(self, binary, config, work, execute):
        self.responses = queue.Queue()
        self.blocks = []
        self.work = work
        self.last_mutation = 0.0
        self.stdout_log = (work / 'stdout.log').open('w')
        self.stderr_log = (work / 'stderr.log').open('w')
        argv = [str(binary), '--config', str(config), '--connect']
        if execute:
            argv.append('--execute')
        self.process = subprocess.Popen(argv, cwd=work, env=os.environ.copy(),
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr_log, text=True, errors='replace')
        self.reader = threading.Thread(target=self.read, daemon=True)
        self.reader.start()

    def read(self):
        for line in self.process.stdout:
            self.stdout_log.write(line)
            self.stdout_log.flush()
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                self.responses.put(value)
        self.responses.put(None)

    def send(self, command, mutate=False):
        if mutate:
            time.sleep(max(0.0, self.last_mutation + .6 - time.monotonic()))
            self.last_mutation = time.monotonic()
        self.process.stdin.write(json.dumps(command) + '\n')
        self.process.stdin.flush()

    def wait(self, kind, timeout=15):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                value = self.responses.get(timeout=min(.2, max(.001, deadline-time.monotonic())))
            except queue.Empty:
                continue
            if value is None:
                raise RuntimeError(f'runtime exited with code {self.process.poll()}')
            if value.get('type') == 'trading_blocked':
                self.blocks.append(value.get('error'))
            if value.get('type') == kind:
                return value
            if value.get('type') == 'command_rejected':
                raise RuntimeError(value.get('error'))
        raise TimeoutError(f'timed out waiting for {kind}; blocks={self.blocks}')

    def state(self):
        self.send({'op': 'state'})
        return self.wait('state')

    def reconcile(self):
        self.send({'op': 'reconcile'})
        self.wait('reconciling')
        self.wait('reconciled')
        return self.ready_state()

    def ready_state(self):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            state = self.state()
            if not state['state']['ready']:
                raise RuntimeError(f'recovery blocked: {state["state"]["reason"]}')
            if state['transport_ready']:
                return state
            time.sleep(.05)
        raise TimeoutError('recovered account is waiting for the configured transport')

    def stop(self):
        if self.process.poll() is None:
            self.send({'op': 'shutdown'})
            self.wait('runtime_stopped', timeout=10)
            self.process.wait(timeout=10)
        self.reader.join(timeout=2)
        self.stdout_log.close()
        self.stderr_log.close()


def positions(state):
    return {(p['venue'], p['symbol'], p['side']):
            (p['position']['today'], p['position']['yesterday'])
            for p in state['state']['positions']
            if p['position']['today'] or p['position']['yesterday']}



def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sdk-root', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--protocol', choices=('tcp', 'udp', 'xtcp'), required=True)
    parser.add_argument('--venue', choices=('SHFE', 'INE'), required=True)
    parser.add_argument('--symbol', required=True)
    parser.add_argument('--passive-price-ticks', type=int)
    parser.add_argument('--buy-price-ticks', type=int)
    parser.add_argument('--sell-price-ticks', type=int)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--restart-check', action='store_true', help='reopen the same RocksDB and SDK flow in a second read-only process')
    parser.add_argument('--sync-before-send', action='store_true', help='wait for durable send intent before each SDK mutation')
    parser.add_argument('--native-controls', action='store_true', help='test group-reference cancellation, routing and native batches')
    parser.add_argument('--quotes-file', type=Path, help='optional independent live quote snapshot for marketable limit tests')
    parser.add_argument('--option', action='store_true', help='test native option metadata and both long/short ordinary orders')
    parser.add_argument('--cash-buffer-per-buy-lot', type=float, default=100.0)
    parser.add_argument('--market', action='store_true', help='also exercise native market Day/FAK/FOK')
    args = parser.parse_args()
    for name in ('YIDA_USER_ID', 'YIDA_PASSWORD', 'YIDA_APP_ID', 'YIDA_AUTH_CODE'):
        if not os.environ.get(name):
            parser.error(f'{name} must be set')
    if args.execute and not all(p is not None and p > 0 for p in
                                (args.passive_price_ticks, args.buy_price_ticks, args.sell_price_ticks)):
        parser.error('--execute requires three positive, explicitly chosen limit prices in ticks')
    binary = args.binary.resolve()
    if args.execute and 'release' not in binary.parts:
        parser.error('--execute requires a release binary')
    work = args.output_dir.resolve()
    work.mkdir(mode=0o700, parents=True, exist_ok=False)
    native = work / 'server.cfg'
    native.write_text(f'TradingServerIP=118.190.175.212\nTradingServerPort=41600\n'
                      f'TradingProtocol={args.protocol.upper()}\nUDPTradingType=socket\n'
                      'XTCPTradingType=socket\nConnectTCPMarketData=no\n'
                      'ReceiveUDPMarketData=no\nRecalcMode=off\nTradingServerTimeout=10\n')
    root = Path(__file__).resolve().parents[3]
    template = (root / 'config/yida_trade.example.toml').read_text()
    config = template.split('[[symbols]]', 1)[0]
    for old, new in [('/opt/yida/1.502.53.182/linux64/yd.so', str(args.sdk_root.resolve() / 'linux64/yd.so')),
                     ('/opt/yida/trade-server.cfg', str(native)),
                     ('trading_protocol = "xtcp"', f'trading_protocol = "{args.protocol}"'),
                     ('public_simulation = false', 'public_simulation = true'),
                     ('/tmp/mkt_yida_sim/flow', str(work / 'flow')),
                     ('/tmp/mkt_yida_sim/events.jsonl', str(work / 'events.jsonl')),
                     ('idle_sleep_us = 0', 'idle_sleep_us = 100')]:
        config = config.replace(old, new)
    config += (f'[[symbols]]\nvenue = {json.dumps(args.venue)}\nsymbol = {json.dumps(args.symbol)}\n'
               'cash_budget_per_open_lot = 250000.0\n' +
               (f'cash_buffer_per_buy_lot = {args.cash_buffer_per_buy_lot}\n' if args.option else '') +
               '\n[risk]\n' + template.split('[risk]', 1)[1])
    config = config.replace('enable_fak_fok = false', 'enable_fak_fok = true')
    if args.sync_before_send:
        config = config.replace('sync_before_send = false', 'sync_before_send = true')
    path = work / 'config.toml'
    path.write_text(config)
    runtime = Runtime(binary, path, work, args.execute)
    owned_keys = []
    final = None
    results = []
    baseline = None
    baseline_verified_flat = False
    error = None
    cleanup_error = None

    def order(state, key):
        return next(o for o in state['state']['orders'] if o['key'] == key)

    def submit(side, offset, tif, price, price_type='limit', execution_route=None, crossing=False):
        if crossing and args.quotes_file and price_type == 'limit':
            quote = json.loads(args.quotes_file.read_text())[args.symbol]
            assert time.time() - quote['updated_at'] < 10, 'independent quote snapshot is stale'
            tick = rule['price_tick']
            raw = quote['ask'] if side == 'buy' else quote['bid']
            assert raw > 0 and math.isfinite(raw)
            price = math.ceil(raw/tick-1e-8)+10 if side == 'buy' else max(1, math.floor(raw/tick+1e-8)-10)
        runtime.send({'op': 'submit', 'intent': {'venue': args.venue, 'symbol': args.symbol,
                     'side': side, 'offset': offset, 'lots': 1,
                     'price_type': price_type, 'execution_route': execution_route,
                     'price_ticks': price, 'time_in_force': tif}}, mutate=True)
        receipt = runtime.wait('submit_receipt')['receipt']
        owned_keys.extend(c['key'] for c in receipt['children'])
        if not receipt['all_submitted_locally']:
            raise RuntimeError(f'submission failed: {receipt}')
        return receipt['children'][0]['key']

    def await_order(key, predicate):
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            state = runtime.state()
            item = order(state, key)
            if predicate(item):
                return item
            if not state['state']['ready']:
                raise RuntimeError(f'trading blocked: {state["state"]["reason"]}')
            time.sleep(.05)
        raise TimeoutError(f'order outcome timed out: {key}')

    def terminal(key):
        return await_order(key, lambda o: o['status'] in ('filled', 'cancelled', 'rejected')
                           and o['filled'] == o['reported_filled'])

    def cancel(key, immediate=False):
        runtime.send({'op': 'cancel', 'key': key}, mutate=not immediate)
        runtime.wait('cancel_submitted_locally')
        return terminal(key)

    def filled(key):
        item = terminal(key)
        if item['status'] != 'filled' or item['filled'] != 1:
            raise RuntimeError(f'expected factual one-lot fill: {item}')
        return item

    try:
        runtime.wait('reconciled')
        baseline = runtime.ready_state()
        assert baseline['state']['ready'] and baseline['transport_ready']
        rule = next(r for r in baseline['state']['instruments'] if r['venue'] == args.venue and r['symbol'] == args.symbol)
        assert (rule['option'] is not None) == args.option, 'native contract class differs from requested test scope'
        assert all(not (v == args.venue and s == args.symbol)
                   for v, s, _ in positions(baseline)), 'test contract must initially be flat'
        baseline_verified_flat = True
        if args.execute:
            assert baseline['can_trade']
            key = submit('buy', 'open', 'day', args.passive_price_ticks)
            await_order(key, lambda o: o['status'] in ('working', 'filled', 'rejected'))
            cancelled = cancel(key)
            assert cancelled['status'] == 'cancelled' and cancelled['filled'] == 0
            results.append({'case': 'limit_cancel', 'key': key, 'system_id': cancelled['system_id']})
            def exercise_pair(tif, side, price_type):
                close_side = 'sell' if side == 'buy' else 'buy'
                open_price = args.buy_price_ticks if side == 'buy' else args.sell_price_ticks
                close_price = args.sell_price_ticks if side == 'buy' else args.buy_price_ticks
                if price_type == 'market':
                    open_price = close_price = 0
                opened = terminal(submit(side, 'open', tif, open_price, price_type, crossing=True))
                assert opened['status'] in ('filled', 'cancelled'), opened
                assert opened['filled'] in (0, 1), opened
                auxiliary = []
                if opened['filled'] == 0:
                    assert args.option and opened['status'] == 'cancelled', opened
                    # Seed one factual lot so the requested close type is tested even when
                    # its matching open IOC/FOK legitimately found no liquidity.
                    seed = filled(submit(side, 'open', 'day', 0, 'market'))
                    auxiliary.append({'operation': 'seed_open', 'key': seed['key']})
                closed = terminal(submit(close_side, 'close_today', tif, close_price, price_type, crossing=True))
                assert closed['status'] in ('filled', 'cancelled'), closed
                assert closed['filled'] in (0, 1), closed
                if closed['filled'] == 0:
                    assert args.option and closed['status'] == 'cancelled', closed
                    cleanup = filled(submit(close_side, 'close_today', 'day', 0, 'market'))
                    auxiliary.append({'operation': 'cleanup_close', 'key': cleanup['key']})
                results.append({'case': f'market_{tif}' if price_type == 'market' else tif,
                                'side': side, 'open_system_id': opened['system_id'],
                                'close_system_id': closed['system_id'],
                                'open_status': opened['status'], 'close_status': closed['status'],
                                'factual_fills': 2, 'target_factual_fills': opened['filled'] + closed['filled'],
                                'auxiliary_orders': auxiliary})
            for tif in (('day', 'fak', 'fok') if args.option else ('fak', 'fok')):
                for side in (('buy', 'sell') if args.option else ('buy',)):
                    exercise_pair(tif, side, 'limit')
            if args.market:
                for tif in ('day', 'fak', 'fok'):
                    for side in (('buy', 'sell') if args.option else ('buy',)):
                        exercise_pair(tif, side, 'market')
            if args.native_controls:
                # Ask to cancel immediately after local submission, without waiting for an order report.
                time.sleep(1.1)
                key = submit('buy', 'open', 'day', args.passive_price_ticks)
                cancelled = cancel(key, immediate=True)
                assert cancelled['status'] == 'cancelled' and cancelled['filled'] == 0
                results.append({'case': 'immediate_group_cancel', 'key': key})
                time.sleep(1.1)
                seats = [c for c in baseline['connections']
                         if c['venue'] == args.venue and c['connected'] and (c['public'] or c['dedicated'])]
                assert seats, 'no connected authorized execution seat'
                connection = seats[0]['connection_id']
                for mode in ('fixed', 'preferred', 'packed'):
                    route = {'mode': mode}
                    if mode != 'packed':
                        route['connection_id'] = connection
                    key = submit('buy', 'open', 'day', args.passive_price_ticks, execution_route=route)
                    await_order(key, lambda o: o['status'] == 'working')
                    cancelled = cancel(key)
                    assert cancelled['status'] == 'cancelled' and cancelled['filled'] == 0
                    results.append({'case': f'route_{mode}', 'connection_id': connection})
                intents = [{'venue': args.venue, 'symbol': args.symbol, 'side': 'buy', 'offset': 'open',
                            'lots': 1, 'price_ticks': args.passive_price_ticks, 'time_in_force': 'day',
                            'execution_route': {'mode': mode, 'connection_id': connection}}
                           for mode in ('fixed', 'preferred')]
                # Each child consumes the usual action budget; let earlier actions expire.
                time.sleep(1.1)
                runtime.send({'op': 'submit_batch', 'intents': intents}, mutate=True)
                receipt = runtime.wait('submit_receipt')['receipt']
                batch_keys = [c['key'] for c in receipt['children']]
                owned_keys.extend(batch_keys)
                assert receipt['all_submitted_locally'] and len(batch_keys) == 2, receipt
                for key in batch_keys:
                    await_order(key, lambda o: o['status'] == 'working')
                time.sleep(1.1)
                runtime.send({'op': 'cancel_batch', 'keys': batch_keys}, mutate=True)
                receipt = runtime.wait('cancel_batch_receipt')['receipt']
                assert receipt['all_submitted_locally'], receipt
                for key in batch_keys:
                    item = terminal(key)
                    assert item['status'] == 'cancelled' and item['filled'] == 0
                results.append({'case': 'native_batch_submit_cancel', 'keys': batch_keys, 'orders': 2})
            final = runtime.reconcile()
            assert positions(final) == positions(baseline), 'account futures inventory changed'
            assert all(order(final, key)['status'] in ('filled', 'cancelled', 'rejected') for key in owned_keys)
            assert final['can_trade']
    except (Exception, KeyboardInterrupt) as exc:
        error = f'{type(exc).__name__}: {exc}'
        if args.execute and baseline_verified_flat and owned_keys:
            try:
                current = runtime.state()
                for key in owned_keys:
                    item = order(current, key)
                    if item['status'] not in ('filled', 'cancelled', 'rejected') and item['system_id']:
                        cancel(key)
                current = runtime.state()
                for side in ('buy', 'sell'):
                    amount = positions(current).get((args.venue, args.symbol, side), (0, 0))
                    if amount != (0, 0):
                        assert amount == (1, 0), 'cleanup only covers this run\'s one new lot'
                        close_side = 'sell' if side == 'buy' else 'buy'
                        price = args.sell_price_ticks if side == 'buy' else args.buy_price_ticks
                        filled(submit(close_side, 'close_today', 'day', 0, 'market'))
                final = runtime.reconcile()
                assert positions(final) == positions(baseline)
            except Exception as exc_cleanup:
                cleanup_error = f'{type(exc_cleanup).__name__}: {exc_cleanup}'
    finally:
        try:
            runtime.stop()
        except Exception as exc_stop:
            cleanup_error = cleanup_error or f'{type(exc_stop).__name__}: {exc_stop}'
            if runtime.process.poll() is None:
                runtime.process.terminate()
                runtime.process.wait(timeout=10)
        persistence = None
        restarted = False
        if runtime.process.returncode == 0:
            try:
                persistence = verify_persistence(work/'flow'/'persist_manager', binary.parent/'cn_order_export', work/'db-export')
                if args.restart_check and not error and not cleanup_error:
                    restart_work = work/'restart'
                    restart_work.mkdir()
                    restarted_runtime = Runtime(binary, path, restart_work, False)
                    try:
                        restarted_runtime.wait('reconciled')
                        recovered = restarted_runtime.ready_state()
                        assert not recovered['can_trade']
                        assert positions(recovered) == positions(final if final else baseline)
                        restarted = True
                    finally:
                        restarted_runtime.stop()
                    assert restarted_runtime.process.returncode == 0
                    verify_persistence(work/'flow'/'persist_manager', binary.parent/'cn_order_export', work/'db-export-after-restart')
            except Exception as exc_persist:
                error = error or f'{type(exc_persist).__name__}: persistence/restart check: {exc_persist}'
        summary = {'protocol': args.protocol, 'venue': args.venue, 'symbol': args.symbol,
                   'execute': args.execute, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                   'market': args.market, 'native_controls': args.native_controls,
                   'option': args.option, 'persistence': persistence, 'restart_verified': restarted,
                   'sync_before_send': args.sync_before_send,
                   'initial_position_rows': len(positions(baseline)) if baseline else None,
                   'submitted_order_count': len(owned_keys), 'results': results, 'error': error, 'cleanup_error': cleanup_error,
                   'runtime_exit_code': runtime.process.returncode}
        (work / 'summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2))
        print(json.dumps(summary, ensure_ascii=False))
    if error or cleanup_error or runtime.process.returncode != 0:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
