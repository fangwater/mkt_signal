#!/usr/bin/env python3
"""Causal replay of independent full-long/full-short/flat factor strategies."""
from __future__ import annotations
import hashlib
import json
import math
import threading
from pathlib import Path

EPS=4*2.220446049250313e-16


def factor_snapshot(factor,weight,observations,config):
    source=observations['inputs'].get(factor)
    if not source or source.get('ready') is not True:return None
    score,q=source.get('score'),source.get('quantile')
    if not isinstance(score,(float,int)) or not math.isfinite(score) or not isinstance(q,(float,int)) or not math.isfinite(q) or not 0<=q<=1:return None
    corrected=q if weight>0 else 1-q
    entry=config['entry']
    side=1 if corrected-entry['factor_long_quantile']>EPS else -1 if entry['factor_short_quantile']-corrected>EPS else 0
    nq=observations['nq']
    allowed=side!=0
    if entry['nq_change_enabled']:
        nq_q=nq.get('long_quantile') if side>0 else nq.get('short_quantile')
        allowed=allowed and nq.get('ready') is True and isinstance(nq_q,(int,float)) and math.isfinite(nq_q) and 0<=nq_q<=1
        if allowed:allowed=nq_q>=entry['nq_long_quantile'] if side>0 else nq_q<=entry['nq_short_quantile']
    return dict(model_ts_ms=observations['model_ts_ms'],score=side,quantile=corrected,score_ready=True,execution_coefficient=1.0,
        factor_votes=[dict(factor=factor,sign='+' if weight>0 else '-',weight=weight,normalized_weight=1.0,score=score,
                          corrected_score=score if weight>0 else -score,quantile=q,corrected_quantile=corrected,vote=side)],
        nq_long_value=nq.get('long_value'),nq_short_value=nq.get('short_value'),nq_long_quantile=nq.get('long_quantile'),
        nq_short_quantile=nq.get('short_quantile'),filter_ready=nq.get('ready'),decision='long' if allowed and side>0 else 'short' if allowed else 'flat')


def simulate(rows,table,config,settings):
    start=settings['start_ms'];initial=float(settings['initial_equity_usdt']);fee_rate=float(settings.get('taker_fee_rate',0))
    if not math.isfinite(initial) or initial<=0 or not math.isfinite(fee_rate) or fee_rate<0:raise ValueError('invalid replay equity or fee')
    amount=config['max_pos_u']
    if isinstance(amount,bool) or not isinstance(amount,(int,float)) or not math.isfinite(amount) or amount<=0:raise ValueError('max_pos_u must be a positive finite USDT amount')
    factors={factor for row in table.values() for factor,w in row.items() if w!=0}
    revision=hashlib.sha256(json.dumps([table,config,settings],sort_keys=True,separators=(',',':')).encode()).hexdigest()
    positions,marks,records,trades,warmup,earliest={},{},[],[],{},{}
    realized=fees_paid=0.0;invalid_prices=0;last_bar_ms=None;seen=set()
    ordered=sorted(rows,key=lambda r:(r['nq'].get('execution_ts_ms',0),r['symbol'],r['model_ts_ms']))
    for observation in ordered:
        symbol,bar=observation['symbol'],observation['model_ts_ms']
        if symbol not in table or bar<start:continue
        nq=observation['nq'];ts=nq.get('execution_ts_ms',0);bid=nq.get('execution_bid');ask=nq.get('execution_ask')
        if not isinstance(bid,(float,int)) or not isinstance(ask,(float,int)) or not math.isfinite(bid) or not math.isfinite(ask) or bid<=0 or ask<bid or not bar<=ts<=bar+120000:
            invalid_prices+=1;continue
        marks[symbol]=(bid+ask)/2;last_bar_ms=max(last_bar_ms or bar,bar)
        for factor,weight in sorted(table[symbol].items()):
            if weight==0:continue
            key=(factor,symbol)
            if (*key,bar) in seen:continue
            seen.add((*key,bar));earliest[key]=min(earliest.get(key,bar),bar)
            signal=factor_snapshot(factor,weight,observation,config)
            if signal is None:warmup[key]=warmup.get(key,0)+1;continue
            position=positions.get(key);actions=[];execution=config['execution'];q=signal['quantile']
            record=dict(event_id=f'replay:{revision}:{factor}:{symbol}:{bar}',signal_id=f'replay:{revision}:{factor}:{symbol}:{bar}',
                strategy_name=factor,symbol=symbol,model_ts_ms=bar,event_ts_us=ts*1000,state='observed',reason='hold',
                snapshot=dict(enabled=False,source='independent_factor_replay',strategy_name=factor,entry=config['entry'],execution=execution,
                              max_pos_u=amount,symbol_factors={factor:weight},signal=signal),publication=dict(levels=[],error=None),simulation=dict(actions=actions))
            exit_ready=position and (execution['factor_exit_quantile_long']-q>EPS if position['side']>0 else q-execution['factor_exit_quantile_short']>EPS)
            if exit_ready:
                price=bid if position['side']>0 else ask;gross=position['side']*position['qty']*(price-position['average_price'])
                exit_fee=position['qty']*price*fee_rate;realized+=gross;fees_paid+=exit_fee
                trade=dict(trade_id=position['trade_id'],strategy_name=factor,symbol=symbol,side=position['side'],entry_ts_ms=position['entry_ts_ms'],exit_ts_ms=ts,
                    entry_signal_id=position['entry_signal_id'],exit_signal_id=record['signal_id'],
                    average_entry_price=position['average_price'],exit_price=price,qty=position['qty'],entries=1,entry_notional=position['entry_notional'],
                    gross_pnl=gross,fees=position['entry_fees']+exit_fee,pnl=gross-position['entry_fees']-exit_fee,reason='factor_exit')
                trades.append(trade);actions.append(dict(kind='close',trade_id=position['trade_id'],strategy_name=factor,side=position['side'],ts_ms=ts,price=price,qty=position['qty'],pnl=trade['pnl'],target_qty='clear'))
                del positions[key];position=None;record['reason']='factor_exit'
            side=1 if signal['decision']=='long' else -1 if signal['decision']=='short' else 0
            if position is None and side and not exit_ready:
                price=ask if side>0 else bid;notional=float(amount)/sum(w!=0 for w in table[symbol].values());qty=notional/price;fee=notional*fee_rate
                trade_id=f'{factor}:{symbol}:{ts}'
                fees_paid+=fee;position=dict(trade_id=trade_id,entry_signal_id=record['signal_id'],side=side,qty=qty,average_price=price,entry_ts_ms=ts,entry_notional=notional,entry_fees=fee)
                positions[key]=position;record['reason']='full_position_entry'
                actions.append(dict(kind='open',trade_id=trade_id,strategy_name=factor,side=side,ts_ms=ts,price=price,qty=qty,notional=notional,target_qty=side*qty))
            flag=position['side'] if position else 0;qty=position['side']*position['qty'] if position else 0
            record['snapshot'].update(position_flag=flag,position_qty=qty)
            record['simulation'].update(position_flag=flag,position_qty=qty)
            records.append(record)
    def stats(factor,symbol):
        key=(factor,symbol);closed=[t for t in trades if t['strategy_name']==factor and t['symbol']==symbol];p=positions.get(key)
        upnl=p['side']*p['qty']*(marks[symbol]-p['average_price']) if p else 0
        wins=sum(t['pnl']>1e-12 for t in closed)
        return dict(strategy_name=factor,symbol=symbol,closed_trades=len(closed),wins=wins,losses=sum(t['pnl']<-1e-12 for t in closed),
            breakeven=sum(abs(t['pnl'])<=1e-12 for t in closed),win_rate=wins/len(closed) if closed else None,
            realized_pnl=math.fsum(t['pnl'] for t in closed),unrealized_pnl=upnl,position_flag=p['side'] if p else 0,
            position_qty=p['side']*p['qty'] if p else 0,position_notional=p['qty']*marks[symbol] if p else 0,
            warmup_minutes=warmup.get(key,0),first_observation_ms=earliest.get(key),
            entry_actions=sum(a['kind']=='open' for r in records if r['strategy_name']==factor and r['symbol']==symbol for a in r['simulation']['actions']))
    def combine(items):
        count=sum(s['closed_trades'] for s in items);wins=sum(s['wins'] for s in items)
        return dict(closed_trades=count,wins=wins,win_rate=wins/count if count else None,
                    realized_pnl=math.fsum(s['realized_pnl'] for s in items),unrealized_pnl=math.fsum(s['unrealized_pnl'] for s in items),
                    position_notional=math.fsum(s['position_notional'] for s in items),entry_actions=sum(s['entry_actions'] for s in items),
                    warmup_minutes=sum(s['warmup_minutes'] for s in items),position_qty=math.fsum(s['position_qty'] for s in items))
    summaries={}
    for symbol,row in table.items():
        by_factor={f:stats(f,symbol) for f,w in row.items() if w!=0}
        summaries[symbol]={**combine(list(by_factor.values())),'strategies':by_factor}
    strategy_summaries={f:{**combine([summary['strategies'][f] for summary in summaries.values() if f in summary['strategies']]),
                           'symbols':{s:v['strategies'][f] for s,v in summaries.items() if f in v['strategies']}} for f in sorted(factors)}
    count=len(trades);wins=sum(t['pnl']>1e-12 for t in trades);unrealized=math.fsum(s['unrealized_pnl'] for s in summaries.values())
    open_positions=[dict(trade_id=p['trade_id'],strategy_name=factor,symbol=symbol,side=p['side'],
                         entry_ts_ms=p['entry_ts_ms'],entry_signal_id=p['entry_signal_id'],exit_ts_ms=None,
                         average_entry_price=p['average_price'],qty=p['qty'],entry_notional=p['entry_notional'],
                         mark_price=marks[symbol],unrealized_pnl=p['side']*p['qty']*(marks[symbol]-p['average_price']),
                         entry_fees=p['entry_fees']) for (factor,symbol),p in positions.items()]
    return dict(revision=revision,start_ms=start,end_ms=last_bar_ms,settings=settings,max_pos_u=amount,allocation='equal_per_symbol',
                method='independent_factor_full_position_flags',price_method='first_post_bar_bid_ask_no_slippage',gross_results=fee_rate==0,
                closed_trades=count,wins=wins,win_rate=wins/count if count else None,realized_pnl=realized-fees_paid,unrealized_pnl=unrealized,
                equity=initial+realized-fees_paid+unrealized,fees=fees_paid,invalid_price_minutes=invalid_prices,
                summaries=summaries,strategy_summaries=strategy_summaries,records=records,trades=trades,open_positions=open_positions)


class PositionReplay:
    def __init__(self,store,config_path:Path,status_path:Path,settings_path:Path):
        self.store,self.config_path,self.status_path,self.settings_path=store,config_path,status_path,settings_path
        self.lock=threading.RLock();self.result=None;self.error=None;self.key=None
    def refresh(self):
        if not self.settings_path.exists():return
        try:
            settings=json.loads(self.settings_path.read_text());config=json.loads(self.config_path.read_text());status=json.loads(self.status_path.read_text())
            table=status['symbol_factors']
            with self.store.lock:raw=[row[0] for row in self.store.db.execute('SELECT payload FROM factor_observations WHERE model_ts_ms>=? ORDER BY model_ts_ms,symbol',(settings['start_ms'],))]
            key=hashlib.sha256(json.dumps([settings,config,table,raw],sort_keys=True).encode()).hexdigest()
            with self.lock:
                if key==self.key:return
            result=simulate([json.loads(row) for row in raw],table,config,settings)
            with self.lock:self.result,self.key,self.error=result,key,None
        except Exception as error:
            with self.lock:self.error=str(error)
    def overview(self):
        with self.lock:
            if self.result is None:return {'error':self.error,'ready':False}
            return {k:v for k,v in self.result.items() if k not in ('records','trades','open_positions')}|{'error':self.error,'ready':True}
    def series(self,symbol,start_ms,strategy=None):
        with self.lock:
            if self.result is None:return {'error':self.error,'ready':False,'records':[],'trades':[]}
            result=self.result;summary=result['summaries'].get(symbol)
            if strategy:summary=summary.get('strategies',{}).get(strategy) if summary else None
            matches=lambda row:row['symbol']==symbol and (not strategy or row['strategy_name']==strategy)
            before={}
            for row in result['records']:
                if matches(row) and row['event_ts_us']<start_ms*1000:before[row['strategy_name']]=row
            return dict(ready=True,error=self.error,revision=result['revision'],start_ms=result['start_ms'],end_ms=result['end_ms'],settings=result['settings'],
                gross_results=result['gross_results'],summary=summary,strategy=strategy,
                available_strategies=list(result['summaries'].get(symbol,{}).get('strategies',{})),
                context=list(before.values()),
                records=[r for r in result['records'] if matches(r) and r['event_ts_us']>=start_ms*1000],
                trades=[t for t in result['trades'] if matches(t) and t['exit_ts_ms']>=start_ms],
                open_positions=[p for p in result['open_positions'] if matches(p)])
