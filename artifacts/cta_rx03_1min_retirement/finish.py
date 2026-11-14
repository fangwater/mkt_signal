"""Finish RX03 minute retirement after all tradable quantities are closed."""
import argparse
import json
from pathlib import Path
import sys
import time
sys.path.insert(0,'/home/ubuntu/crypto_cta_manager/maintenance-backups/cta_symbol_groups_20261104T093943Z')
import ops
NAMES={f'cta-{p}-prep02-bn-1min-pos' for p in ['top-5','top-20','last-20']}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--audit',type=Path,required=True);p.add_argument('--execute',action='store_true')
    args=p.parse_args();before=ops.read(args.audit/'before.json')
    _,execution=ops.snapshot();rows=[r for r in execution['rows'] if r['strategy_name'] in NAMES]
    ops.require(execution['position_ready'],'Exec not ready')
    ops.require(all(r['execution_complete'] and not r['active_batches'] and abs(r['pending_qty'])<1e-10 and abs(r['live_order_qty'])<1e-10 and abs(r['target_qty'])<1e-10 for r in rows),'retirement still has executable positions or orders')
    residuals=[{k:r.get(k) for k in ['strategy_name','symbol','current_qty','current_usdt','completion_reason']} for r in rows if abs(r['current_qty'])>1e-10]
    ops.require(all(r['completion_reason']=='exchange_minimum' for r in residuals),'inspect unexplained residual')
    summary={'source':ops.SOURCE,'minute_strategies':sorted(NAMES),'active_batches':0,'pending_orders':0,'nonzero_targets':0,
        'residual_abs_usdt':sum(abs(r['current_usdt']) for r in residuals),'residual_rows':len(residuals),'execute':args.execute}
    print(json.dumps(summary),flush=True)
    if not args.execute:return
    with ops.session('lgq') as api:
        account=api('/manager/api/catalog/accounts/'+ops.SOURCE)
        bindings={b['binding_name']:b for b in account['bindings']}
        for name in NAMES:
            if name in bindings:ops.require(bindings[name]['shares']==0,'minute binding was re-enabled')
        retained=[b for b in before['account']['bindings'] if b['binding_name'] not in NAMES]
        ops.require([b for b in account['bindings'] if b['binding_name'] not in NAMES]==retained,'other bindings changed')
        receipts=ops.read(args.audit/'removal_receipts.json') if (args.audit/'removal_receipts.json').exists() else []
        for name in sorted(NAMES):
            if name not in bindings:continue
            receipts.append({'action':'remove_binding:'+name,'result':api('/manager/api/catalog/accounts/'+ops.SOURCE+'/bindings/'+name,None,'DELETE')})
            ops.save(args.audit/'removal_receipts.json',receipts)
        account=api('/manager/api/catalog/accounts/'+ops.SOURCE)
        ops.require(account['bindings']==retained and not account['pending_publishes'],'bindings or pending publication remain')
    current=ops.local_api('/api/config?worker=cta-prep-binance-futures')
    kept={n:d for n,d in before['catalog']['plan']['combinations'].items() if not(d.get('strategy_group')=='prep02' and d['interval']=='1min')}
    ops.require(current['plan']['combinations']==kept,'other combinations changed')
    # Keep factual below-lot allocations and zero Exec targets; never invent fills or reassign lots.
    ops.save(args.audit/'final-execution.json',execution)
    ops.save(args.audit/'residuals.json',residuals)
    summary.update({'verified_at_ms':time.time_ns()//1000000,'remaining_bindings':len(retained),
        'minute_configurations':0,'minute_bindings':0,'retired_zero_exec_targets_retained':True,'inactive_position_definitions_retained':True,
        'other_combinations_unchanged':True,'other_bindings_unchanged':True,'audit':str(args.audit)})
    ops.save(args.audit/'verified.json',summary)
    print(json.dumps(summary),flush=True)


if __name__=='__main__':main()
