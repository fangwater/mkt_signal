"""Bounded quote-test estimates, never source-confirmed aggressor labels."""

import argparse
from bisect import bisect_left
from collections import Counter, defaultdict
import csv
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from reference import iter_messages, open_text
from quote_reference import _e9, _timestamp_ns
from event_reference import classify_trade_direction
from audit_direction_coverage import SAMPLES

NS = 1_000_000_000


def classify(price, venue, bid, ask, rule):
    # A side is (price, size, contributor, quote source ns, quote event ns).
    def valid(side):
        return side is not None and side[0] > 0 and side[1] > 0
    bv, av = valid(bid), valid(ask)
    if bv and av and bid[0] >= ask[0]:
        return 'N', 'locked_or_crossed'
    bm = bv and bool(venue) and bid[2] == venue
    am = av and bool(venue) and ask[2] == venue
    if rule == 'both' and not (bm and am):
        return 'N', 'missing_same_venue_two_sides'
    if rule == 'nbbo':
        if not (bv and av):
            return 'N', 'missing_nbbo'
        bm, am = bv, av
    buy, sell = am and price >= ask[0], bm and price <= bid[0]
    if buy and sell:
        return 'N', 'conflict'
    if buy:
        return 'B', 'ask_match'
    if sell:
        return 'S', 'bid_match'
    return 'N', 'price_not_decisive' if bm or am else 'no_matching_contributor'


def field_time(fields, names, date, audit):
    for name in names:
        raw = fields.get(name)
        if not raw or not raw.value.strip():
            continue
        value = raw.value.strip()
        audit[name] += 1
        if ':' in value:
            return _timestamp_ns(date + 'T' + value + 'Z')
        millis = int(value)
        if not 0 <= millis < 86400000:
            raise ValueError((name, value))
        return _timestamp_ns(date + 'T00:00:00Z') + millis * 1000000
    audit['missing'] += 1
    return None


def extract(root, sample):
    ric, part, shard, start = sample
    start_dt = datetime.fromisoformat(start)
    end = (start_dt + timedelta(minutes=10)).isoformat()
    warm = (start_dt - timedelta(seconds=60)).isoformat()
    path = root / f'merged-Data-part-{part:06}-shard-{shard:06}.csv.zst'
    quotes = {'bid': [], 'ask': []}
    trades = []
    audit = defaultdict(Counter)
    reached_end = False
    with open_text(path) as stream:
        for m in iter_messages(stream):
            if m.ric != ric:
                continue
            if m.date_time >= end:
                reached_end = True
                break
            if m.date_time < warm or m.message_class != 'UPDATE':
                continue
            f = {x.name: x for x in m.fields}
            src = _timestamp_ns(m.date_time)
            if m.update_type == 'QUOTE':
                date = f['QUOTE_DATE'].value if f.get('QUOTE_DATE') and f['QUOTE_DATE'].value else m.date_time[:10]
                qt = field_time(f, ['QUOTIM_MS', 'QUOTIM_NS', 'QUOTIM'], date, audit['quote_time'])
                audit['quote_quality'][f['PRC_QL_CD'].enum_value.strip() if f.get('PRC_QL_CD') else 'absent'] += 1
                for side, px, sz, xid in [('bid','BID','BIDSIZE','BIDXID'), ('ask','ASK','ASKSIZE','ASKXID')]:
                    if px not in f and sz not in f:
                        continue
                    if px not in f or sz not in f:
                        raise ValueError(f'incomplete side {m}')
                    value = (_e9(f[px].value), int(f[sz].value or 0), f[xid].enum_value.strip() if xid in f else '', src, qt)
                    quotes[side].append((src, m.source_row, value))
            elif m.update_type == 'TRADE' and m.date_time >= start:
                normal = 'TRDPRC_1' in f
                px, sz, xid, tim, date_name, cond = ('TRDPRC_1','TRDVOL_1','TRADE_EXID','TRDTIM_MS','TRD_DATE','LSTSALCOND') if normal else ('IRGPRC','IRGVOL','IRG_EXID','IRGTIM_MS','IRGDATE','IRGSALCOND')
                price, volume = _e9(f[px].value), int(f[sz].value or 0)
                venue = f[xid].enum_value.strip() if xid in f else ''
                date = f[date_name].value if date_name in f and f[date_name].value else m.date_time[:10]
                event = field_time(f, [tim], date, audit['trade_time'])
                condition = f[cond].value.strip() if cond in f else ''
                cls = classify_trade_direction(venue, 65535)[2]
                group = {0:'unknown_venue', 1:'exchange', 2:'reporting_facility'}[cls]
                if event is not None and event // (86400*NS) != src // (86400*NS):
                    audit['trade_time']['other_day'] += 1
                trades.append(dict(source_ns=src, row=m.source_row, event_ns=event, price=price, volume=volume, venue=venue, group=group, condition=condition))
    if not reached_end:
        raise ValueError(f'incomplete sample {ric}')
    return quotes, trades, dict(ric=ric, start=start+'Z', end=end+'Z', source=str(path), audit=dict(audit))


def history(quotes, mode):
    result = {}
    for side, records in quotes.items():
        rows = sorted((src if mode == 'source' else value[4], order, value) for src, order, value in records if mode == 'source' or value[4] is not None)
        result[side] = ([x[0] for x in rows], rows)
    return result


def previous(hist, time, source, source_order):
    if time is None:
        return None
    keys, records = hist
    i = bisect_left(keys, time) - 1
    # Strictly earlier timestamps; also disallow quotes learned after this trade.
    while i >= 0:
        _, order, value = records[i]
        if (value[3], order) < (source, source_order):
            return value
        i -= 1
    return None


def run(root, output):
    output.mkdir(parents=True, exist_ok=False)
    results = []
    with (output/'trades.csv').open('x', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['ric','source_ns','source_row','event_ns','venue','group','price_e9','volume','condition','clock','nbbo','both','single','single_reason','single_max_1s','bid_age_ms','ask_age_ms'])
        for sample in SAMPLES[:3]:
            quotes, trades, result = extract(root, sample)
            counts = defaultdict(Counter)
            for mode in ['source', 'event_ms']:
                hist = history(quotes, mode)
                for t in trades:
                    time = t['source_ns'] if mode == 'source' else t['event_ns']
                    bid, ask = [previous(hist[s], time, t['source_ns'], t['row']) for s in ['bid','ask']]
                    sides = {}
                    reasons = {}
                    for rule in ['nbbo','both','single']:
                        if t['group'] != 'exchange':
                            side, reason = 'N', t['group']
                        elif t['price'] <= 0 or t['volume'] <= 0:
                            side, reason = 'N', 'invalid_trade'
                        else:
                            side, reason = classify(t['price'], t['venue'], bid, ask, rule)
                        sides[rule], reasons[rule] = side, reason
                        key = f'{mode}:{t["group"]}:{rule}'
                        counts[key][side+'_count'] += 1
                        counts[key][side+'_volume'] += t['volume']
                        counts[key]['reason:'+reason] += 1
                    clock_index = 3 if mode == 'source' else 4
                    ages = [(time-q[clock_index])/1000000 if q is not None and time is not None else None for q in [bid,ask]]
                    capped = sides['single']
                    relevant_age = ages[1] if capped == 'B' else ages[0]
                    if capped != 'N' and relevant_age > 1000:
                        capped = 'N'
                    key = f'{mode}:{t["group"]}:single_max_1s'
                    counts[key][capped+'_count'] += 1
                    counts[key][capped+'_volume'] += t['volume']
                    counts[f'{mode}:transition_both_single'][sides['both']+'->'+sides['single']] += 1
                    counts[f'{mode}:conditions:{t["group"]}:{t["condition"]}'][sides['single']] += 1
                    writer.writerow([result['ric'], t['source_ns'], t['row'], t['event_ns'],t['venue'],t['group'],t['price'],t['volume'],t['condition'],mode,sides['nbbo'],sides['both'],sides['single'],reasons['single'],capped,*ages])
            result['counts'] = dict(counts)
            results.append(result)
            print(json.dumps(result), flush=True)
    with (output/'summary.json').open('x') as stream:
        json.dump(dict(created_utc=datetime.now(timezone.utc).isoformat(),
                       policy='Estimates only. Source-time RTH windows, 60s warmup. Strictly earlier quote timestamps plus source availability. Event clock uses QUOTIM_MS vs TRDTIM_MS/IRGTIM_MS, not verified identical matching-engine clocks. No correction netting, condition filtering, or tick fallback. Reporting facilities remain N. Positive sizes/prices; locked/crossed excluded. No retired venue quote carry.',
                       samples=results), stream, indent=2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--staging-root', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    run(args.staging_root, args.output)
