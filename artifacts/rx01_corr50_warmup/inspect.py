"""Read-only runtime check, distinguishing intentional blanks from ready data."""
import datetime
import json
import os
import time
from pathlib import Path

root = Path.home() / 'binance-cta-spread-rx01'
warmup = json.loads((root / 'data/cta_spread/factor_history/warmup.json').read_text())
status = json.loads((root / 'run/cta_spread_status.json').read_text())
table = json.loads((root / 'config/cta_spread_symbol_factors.json').read_text())
positions = json.loads((root / 'run/cta_spread_position_flags.json').read_text())
missing = set(warmup['missing_amount_thresholds']) & set(table)
size_factors = set(warmup['size_dependent_factors'])
blocked = [(symbol, factor) for symbol, row in table.items() for factor in row
           if symbol in missing and factor in size_factors]
rows = [(symbol, factor, status['strategies'].get(factor, {}).get(symbol, {}))
        for symbol, row in table.items() for factor in row]
now_ms = int(time.time() * 1000)
fresh = lambda row: 0 <= now_ms - row.get('model_ts_ms', 0) <= 120_000
ready = [(symbol, factor) for symbol, factor, row in rows if row.get('score_ready') and fresh(row)]
filtered = [(symbol, factor) for symbol, factor, row in rows
            if row.get('score_ready') and row.get('signal', {}).get('filter_ready') and fresh(row)]
unexpected = [(symbol, factor) for symbol, factor, row in rows
              if (symbol, factor) not in blocked and (not row.get('score_ready') or not fresh(row))]
processes = []
for proc in Path('/proc').glob('[0-9]*'):
    try:
        cwd = os.readlink(proc / 'cwd')
        comm = (proc / 'comm').read_text().strip()
        if cwd in {str(root), str(Path.home() / 'binance_exec_trade06')} and comm in {
            'cta_spread_fact', 'cta_spread_sign', 'python3', 'trade_engine',
            'exec-pre-trade', 'persist_manager', 'viz_server', 'account_monitor', 'rapidx_account_',
        }:
            cpus = next(line.split(':', 1)[1].strip() for line in (proc / 'status').read_text().splitlines()
                        if line.startswith('Cpus_allowed_list:'))
            processes.append(dict(pid=int(proc.name), comm=comm, cwd=cwd, cpus=cpus))
    except OSError:
        pass
result = dict(observed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
              caught_up=warmup['caught_up'], generation=warmup['generation'], records=warmup['records'],
              heartbeat_age_ms=now_ms-warmup['updated_ms'], decode_errors=warmup['decode_errors'],
              evaluation_errors=warmup['evaluation_errors'], publish_failed=warmup['publish_failed'],
              first_minute=min((ts for symbol, ts in warmup['first'].items() if symbol in table), default=0),
              latest_minute=min((ts for symbol, ts in warmup['latest'].items() if symbol in table), default=0),
              symbols_with_history=len(set(warmup['latest']) & set(table)),
              missing_history=sorted(set(table)-set(warmup['latest'])),
              enabled=status['enabled'], journal_healthy=status['journal_healthy'],
              publication_error=status['publication_error'], combinations=len(rows),
              missing_threshold_symbols=sorted(missing), expected_blank_combinations=blocked,
              ready_combinations=len(ready), ready_with_both_filters=len(filtered),
              ready_without_both_filters=[(symbol, factor) for symbol, factor in ready
                                          if (symbol, factor) not in filtered],
              latest_by_symbol={symbol: max((row.get('model_ts_ms', 0) for s, _, row in rows if s == symbol), default=0)
                                for symbol in table},
              other_unready=unexpected, processes=processes,
              positions_zero=all(p == dict(flag=0, qty=0.0, cash_qty=0.0, perpetual_qty=0.0, entry_notional=0.0)
                                 for row in positions.values() for p in row.values()),
              state_combinations=sum(map(len, positions.values())))
print(json.dumps(result, ensure_ascii=False, indent=2))
