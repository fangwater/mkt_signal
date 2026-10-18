"""One-time SG Bybit neutralization; dry-run unless --execute is passed."""
import argparse, importlib.util, json, os, subprocess, sys, time
from pathlib import Path
from decimal import Decimal as D, ROUND_FLOOR
from urllib.parse import urlencode

parser = argparse.ArgumentParser()
parser.add_argument('env', choices=['bybit-intra-arb01','bybit-intra-arb02'])
parser.add_argument('--execute', action='store_true')
args = parser.parse_args()
root = Path('/home/ubuntu')/args.env
e = subprocess.run(['bash','-c','set -a; source "$1" >/dev/null 2>&1; env -0','_',str(root/'env.sh')], capture_output=True, check=True)
os.environ.update(dict(x.split('=',1) for x in e.stdout.decode().split('\0') if '=' in x))
os.environ['BYBIT_REST_BIND_IP'] = '172.31.7.124'
spec = importlib.util.spec_from_file_location('bybit_ops',root/'intra_scripts/flatten_intra_bybit_futures_exposure.py')
ops = importlib.util.module_from_spec(spec); sys.modules[spec.name] = ops; spec.loader.exec_module(ops)
ops._install_rest_source_bind()
key, secret = ops.load_credentials()
original_request = ops.HTTP_SESSION.request
def bounded_request(*a, **kw):
    kw.setdefault('timeout', 15)
    return original_request(*a, **kw)
ops.HTTP_SESSION.request = bounded_request
def private(path, params):
    return ops.request(key, secret, 'GET',path,query=urlencode(params))['result']
def public(path, params):
    r = ops.HTTP_SESSION.get(ops.HOST+path,params=params,timeout=15)
    r.raise_for_status(); result=r.json(); assert result.get('retCode')==0, result.get('retCode')
    return result['result']
def paginated(getter,path,params):
    out=[]; cursors=set()
    while True:
        result=getter(path,params); out.extend(result.get('list',[])); cursor=result.get('nextPageCursor')
        if not cursor: return out
        assert cursor not in cursors; cursors.add(cursor); params['cursor']=cursor
def dec(x): return D(str(x or '0'))
def fmt(x): return format(x,'f')
def stopped():
    live=[]
    for p in Path('/proc').iterdir():
        if not p.name.isdigit() or int(p.name)==os.getpid():continue
        try:
            exe=os.readlink(p/'exe').removesuffix(' (deleted)')
            if exe.startswith(str(root)+'/') or os.readlink(p/'cwd')==str(root):live.append(p.name)
        except OSError:pass
    assert not live, ('environment must remain stopped',live)
stopped()
for category,settle in [('linear','USDC'),('inverse',None),('option',None)]:
    p={'category':category,'limit':'200'}
    if settle:p['settleCoin']=settle
    assert not [x for x in paginated(private,'/v5/position/list',p) if dec(x['size'])], 'unexpected non-USDT positions'
specs={}
prices={}
for category in ['linear','spot']:
    p={'category':category}
    if category=='linear':p['limit']='1000'
    specs[category]={x['symbol']:x for x in paginated(public,'/v5/market/instruments-info',p) if x['status']=='Trading' and x.get('quoteCoin')=='USDT' and (category=='spot' or x.get('contractType')=='LinearPerpetual')}
    prices[category]={x['symbol']:x for x in public('/v5/market/tickers',{'category':category})['list']}
def snapshot():
    accounts=private('/v5/account/wallet-balance',{'accountType':'UNIFIED'})['list']
    coins={c['coin']:c for a in accounts for c in a['coin']}
    positions=paginated(private,'/v5/position/list',{'category':'linear','settleCoin':'USDT','limit':'200'})
    pos={}
    for p in positions:
        if not dec(p['size']):continue
        s=specs['linear'].get(p['symbol']); assert s and s['baseCoin']+'USDT'==p['symbol'], 'nonstandard contract'
        assert p['positionIdx']==0, 'unexpected hedge position mode'
        pos[s['baseCoin']]=dec(p['size'])*(-1 if p['side']=='Sell' else 1)
    out=[]
    for asset in sorted(set(coins)|set(pos)):
        if asset in ['USDT','USDC']:continue
        c=coins.get(asset,{})
        assert not c or c.get('spotBorrow') is not None, 'missing spotBorrow'
        spot=dec(c.get('walletBalance'))-dec(c.get('spotBorrow'))
        # Current Bybit contract: equity = walletBalance - spotBorrow + derivative PnL.
        assert not c or abs(spot+dec(c.get('unrealisedPnl'))-dec(c.get('equity')))<D('0.0000001'), ('wallet identity',asset)
        hedge=pos.get(asset,D(0)); net=spot+hedge
        ticker=prices['spot'].get(asset+'USDT') or prices['linear'].get(asset+'USDT') or {}
        price=dec(ticker.get('lastPrice') or ticker.get('markPrice'))
        if not price and spot: price=abs(dec(c.get('usdValue'))/spot)
        assert price or not net, ('missing price',asset)
        out.append({'asset':asset,'spot':spot,'hedge':hedge,'net':net,'price':price,'net_usdt':net*price,'wallet':dec(c.get('walletBalance')),'locked':dec(c.get('locked'))})
    return out
def action(row):
    if abs(row['net_usdt'])<D('1'):return None
    asset=row['asset']; symbol=asset+'USDT'; side='Sell' if row['net']>0 else 'Buy'; raw=abs(row['net'])
    candidate=None
    for category in ['linear','spot']:
        s=specs[category].get(symbol)
        if not s or s['baseCoin']!=asset:continue
        lot=s['lotSizeFilter']; step=dec(lot.get('qtyStep') or lot.get('basePrecision')); assert step>0
        qty=(raw/step).to_integral_value(rounding=ROUND_FLOOR)*step
        if qty<=0 or qty<dec(lot.get('minOrderQty')):continue
        maxqty=dec(lot.get('maxMktOrderQty') or lot.get('maxMarketOrderQty'))
        assert not maxqty or qty<=maxqty, ('oversize',asset)
        reduce=category=='linear' and row['net']*row['hedge']>0 and qty<=abs(row['hedge'])
        minimum=dec(lot.get('minNotionalValue') or lot.get('minOrderAmt'))
        if qty*row['price']<minimum and not reduce:continue
        payload={'category':category,'symbol':symbol,'side':side,'orderType':'Market','qty':fmt(qty),'slippageToleranceType':'Percent','slippageTolerance':'0.50'}
        if category=='linear':payload.update(reduceOnly=reduce,positionIdx=0)
        else:
            needs_borrow=side=='Sell' and qty>max(D(0),row['wallet']-row['locked'])
            if needs_borrow and s.get('marginTrading') not in ['both','utaOnly']:continue
            payload.update(marketUnit='baseCoin',isLeverage=1 if needs_borrow else 0)
        residual=(raw-qty)*row['price']
        current={'payload':payload,'before_net_usdt':fmt(row['net_usdt']),'expected_abs_residual_usdt':fmt(residual),'notional':fmt(qty*row['price'])}
        if candidate is None or residual<D(candidate['expected_abs_residual_usdt']):candidate=current
        if category=='linear' and residual<D('1'):return current
    return candidate
rows=snapshot(); plan=[a for r in rows if (a:=action(r))]
assert sum(D(a['notional']) for a in plan)<D('2000'), 'unexpected total hedge size'
assert all(D(a['notional'])<D('500') for a in plan), 'unexpected single hedge size'
out=Path('/tmp/bybit_intra_redeploy_20261016'); out.mkdir(mode=0o700,exist_ok=True)
stamp=str(time.time_ns()); mode='execute' if args.execute else 'dryrun'
report={'env':args.env,'mode':mode,'time':time.time(),'plan':plan,'before':rows}
print(json.dumps({'env':args.env,'mode':mode,'plan':plan},default=str),flush=True)
if args.execute:
    ledger=out/(args.env+'_'+stamp+'_orders.jsonl')
    for index,planned in enumerate(plan,1):
        stopped()
        asset=planned['payload']['symbol'][:-4]
        row=next(r for r in snapshot() if r['asset']==asset)
        fresh=action(row)
        if fresh is None:continue
        assert fresh['payload']['category']==planned['payload']['category'] and fresh['payload']['side']==planned['payload']['side'], 'plan changed'
        assert D(fresh['notional'])<=D(planned['notional'])*D('1.05')+D('1'), 'exposure changed'
        payload=fresh['payload']; payload['orderLinkId']=ops.make_external_order_link_id(index)
        with ledger.open('a') as f:f.write(json.dumps({'intent':payload})+'\n')
        result=ops.request(key,secret,'POST','/v5/order/create',body=json.dumps(payload,separators=(',',':')))['result']
        with ledger.open('a') as f:f.write(json.dumps({'accepted':result})+'\n')
        final=None
        for attempt in range(20):
            status=private('/v5/order/realtime',{'category':payload['category'],'symbol':payload['symbol'],'orderId':result['orderId']})
            found=status.get('list',[])
            if found and found[0]['orderStatus'] in ['Filled','Cancelled','Rejected','PartiallyFilledCanceled']:
                final={k:found[0].get(k) for k in ['symbol','orderId','orderStatus','cumExecQty','cumExecValue','avgPrice','rejectReason']};break
            time.sleep(0.5)
        assert final is not None, 'order state unresolved; do not resubmit'
        with ledger.open('a') as f:f.write(json.dumps({'final':final})+'\n')
        print(json.dumps(final),flush=True)
        assert final['orderStatus']=='Filled', 'order not fully filled; re-audit before continuing'
    report['after']=snapshot()
    report['remaining_actions']=[a for r in report['after'] if (a:=action(r))]
report_path=out/(args.env+'_'+stamp+'_'+mode+'.json')
report_path.write_text(json.dumps(report,default=str,indent=2))
last=report.get('after',rows)
print(json.dumps({'env':args.env,'report':str(report_path),'absolute_net_sum_usdt':sum(abs(r['net_usdt']) for r in last),'signed_net_sum_usdt':sum(r['net_usdt'] for r in last),'max_abs_net_usdt':max(abs(r['net_usdt']) for r in last),'residual_above_1':[r for r in last if abs(r['net_usdt'])>=1]},default=str),flush=True)
