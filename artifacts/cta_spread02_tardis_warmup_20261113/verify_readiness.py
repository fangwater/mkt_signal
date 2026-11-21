"""Summarize real-engine hourly seeds without creating factor or position output."""
import json
from pathlib import Path

TASK = Path(__file__).resolve().parent


def main():
    seeds = json.loads((TASK / 'hourly_seed_check.json').read_text())
    plan = json.loads((TASK / 'runtime_plan.json').read_text())['plan']
    ready = {}
    for market, seed in seeds.items():
        assert not seed['missing_symbols'], market
        for key in ('ready_formulas', 'ready_nq', 'ready_spreads'):
            ready.update(seed[key])
    tables = []
    for name, combination in plan['combinations'].items():
        if combination['interval'] != '1h':
            continue
        ids = [key for key, value in plan['calculations'].items() if name in value['combinations']]
        missing = sorted(set(ids) - ready.keys())
        unready = [key for key in ids if key in ready and not ready[key]]
        tables.append({'strategy_group': combination['strategy_group'], 'calculations': len(ids),
                       'ready': sum(ready.get(key, False) for key in ids), 'missing': missing,
                       'unready': unready, 'enabled': combination['signal']['enabled']})
    result = {'markets': {market: {'cutoff_ms': s['cutoff_ms'], 'formula_symbols': len(s['formula_rows']),
                'formula_rows': s['formula_rows'], 'nq_rows': s['nq_rows']} for market, s in seeds.items()},
              'hourly_tables': tables, 'all_hourly_ready': all(not t['missing'] and not t['unready'] for t in tables)}
    (TASK / 'hourly_readiness.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(tables))


if __name__ == '__main__':
    main()
