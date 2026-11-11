#!/usr/bin/env python3
"""Run the real native Rust/C++ adapter against a marked, offline synthetic SDK.

The benchmark never sources saved account files. All credentials and endpoints
below belong solely to the synthetic fixture. No vendor library is loaded.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
from yida_bridge_smoke import fixture_source
from native_persistence_check import verify_persistence


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sdk-dir', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--exporter', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--cycles', type=int, default=2000)
    parser.add_argument('--long-run-cycles', type=int, default=110000)
    parser.add_argument('--cpu', type=int, default=190)
    args = parser.parse_args()
    work = args.output_dir.resolve()
    work.mkdir(parents=True, mode=0o700, exist_ok=False)
    root = Path(__file__).resolve().parents[3]
    source = work / 'fixture.cpp'
    library = work / 'offline-fixture.so'
    source.write_text(fixture_source(args.sdk_dir.resolve()) + '\nextern "C" unsigned cn_offline_benchmark_fixture(){return 0x434e424d;}\n')
    subprocess.run(['c++', '-std=c++17', '-O3', '-shared', '-fPIC', '-pthread', '-isystem', str(args.sdk_dir.resolve()), str(source), '-o', str(library)], check=True)
    environment = {k: v for k, v in os.environ.items() if not k.startswith('YIDA_FIXTURE_')}
    environment.update(YIDA_USER_ID='fixture', YIDA_PASSWORD='fixture-only', YIDA_APP_ID='fixture-app', YIDA_AUTH_CODE='fixture-auth', YIDA_FIXTURE_BATCH='1')
    template = (root / 'config/yida_trade.example.toml').read_text()
    cases = [(protocol, mode, journal, args.cycles, burst)
             for protocol, mode, journal, burst in [('tcp', 'fak', True, 32), ('udp', 'fak', True, 128), ('xtcp', 'cancel', True, 32)]]
    if args.long_run_cycles:
        cases.append(('tcp', 'fak', False, args.long_run_cycles, 1024))
    results = []
    for protocol, mode, journal, cycles, burst in cases:
        case = work / f'{protocol}-{mode}-{"wal" if journal else "long-run"}'
        case.mkdir(mode=0o700)
        native = case / 'server.cfg'
        native.write_text(f'TradingServerIP=127.0.0.1\nTradingServerPort=1\nTradingProtocol={protocol.upper()}\nUDPTradingType=socket\nXTCPTradingType=socket\nConnectTCPMarketData=no\nReceiveUDPMarketData=no\nRecalcMode=off\nTradingServerTimeout=10\n')
        config = template
        for before, after in [('/opt/yida/1.502.53.182/linux64/yd.so', str(library)), ('/opt/yida/trade-server.cfg', str(native)), ('trading_protocol = "xtcp"', f'trading_protocol = "{protocol}"'), ('/tmp/mkt_yida_sim/flow', str(case / 'flow')), ('/tmp/mkt_yida_sim/events.jsonl', str(case / 'events.jsonl')), ('cash_budget_per_open_lot = 250000.0', 'cash_budget_per_open_lot = 1.0'), ('max_working_lots = 2', 'max_working_lots = 20'), ('max_actions_per_second = 2', 'max_actions_per_second = 1000000'), ('enable_fak_fok = false', 'enable_fak_fok = true'), ('event_capacity = 8192', 'event_capacity = 65536')]:
            config = config.replace(before, after)
        path = case / 'config.toml'
        path.write_text(config)
        argv = [str(args.binary.resolve()), '--config', str(path), '--cycles', str(cycles), '--burst', str(burst), '--cpu', str(args.cpu)]
        if not journal:
            argv.append('--without-journal')
        if mode == 'cancel':
            argv.append('--passive-cancel')
        run = subprocess.run(argv, env=environment, capture_output=True, text=True)
        (case / 'stdout.log').write_text(run.stdout)
        (case / 'stderr.log').write_text(run.stderr)
        if run.returncode:
            raise RuntimeError(f'offline benchmark failed in {case}: {run.stderr[-3000:]}')
        row = next(json.loads(line) for line in run.stdout.splitlines() if line.startswith('{') and json.loads(line).get('benchmark') == 'native_sdk_pipeline')
        if journal:
            row['persistence'] = verify_persistence(case / 'flow/persist_manager', args.exporter.resolve(), case / 'export')
            assert row['persistence']['rocksdb_orders'] == cycles * (1 if mode == 'cancel' else 2) + 2
            assert row['persistence']['rocksdb_unique_trades'] == (0 if mode == 'cancel' else cycles * 2)
            assert row['synced_wal_batches'] < row['durable_records'] and row['largest_wal_batch'] > 1
        results.append(row)
        print(json.dumps(row), flush=True)
    (work / 'summary.json').write_text(json.dumps(results, indent=2) + '\n')


if __name__ == '__main__':
    main()
