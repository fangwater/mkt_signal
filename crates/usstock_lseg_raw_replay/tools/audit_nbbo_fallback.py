"""Validate midpoint/tick fallbacks on frozen Databento matched pairs."""

import argparse
from bisect import bisect_left
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

from audit_single_side import extract, history, previous, classify, SAMPLES


def tick(price, event_ns, source_ns, source_row, records, times):
    # Millisecond ties cannot establish event ordering; do not use them.
    i = bisect_left(times, event_ns) - 1
    while i >= 0:
        r = records[i]
        if (r['source_ns'], r['row']) < (source_ns, source_row) and r['price'] != price:
            return ('B' if price > r['price'] else 'S'), r['row']
        i -= 1
    return 'N', None


def fallback(price, bid, ask, old, tick_side):
    if old != 'N':
        return old, 'nbbo_touch'
    if bid is None or ask is None or min(bid[0], ask[0], bid[1], ask[1]) <= 0:
        return 'N', 'invalid_book'
    if bid[0] >= ask[0]:
        return 'N', 'locked_or_crossed'
    if not bid[0] < price < ask[0]:
        raise ValueError('valid outside-spread price must already have a touch label')
    delta = 2 * price - bid[0] - ask[0]
    if delta:
        return ('B' if delta > 0 else 'S'), 'nbbo_midpoint'
    return tick_side, 'tick_rule' if tick_side != 'N' else 'tick_unavailable'


def stats(rows, column):
    c = Counter()
    for r in rows:
        side, truth, volume = r[column], r['truth'], int(r['volume'])
        c['count'] += 1
        c['volume'] += volume
        c['truth:'+truth+'->'+side] += 1
        if side == 'N':
            c['unknown'] += 1
        else:
            c['classified'] += 1
            c['classified_volume'] += volume
            c['correct' if side == truth else 'wrong'] += 1
            if side == truth:
                c['correct_volume'] += volume
    return dict(c, accuracy=c['correct']/c['classified'] if c['classified'] else None,
                coverage=c['classified']/c['count'] if c['count'] else None,
                volume_accuracy=c['correct_volume']/c['classified_volume'] if c['classified_volume'] else None)


def run(root, pairs, output):
    output.mkdir(parents=True, exist_ok=False)
    quotes, trades, provenance = extract(root, SAMPLES[0])
    hist = history(quotes, 'event_ms')
    eligible = [t for t in trades if t['group']=='exchange' and t['price']>0 and t['volume']>0 and t['event_ns'] is not None]
    indices = {}
    for venue in ['ALL', *sorted({t['venue'] for t in eligible})]:
        rows = sorted((t for t in eligible if venue=='ALL' or t['venue']==venue), key=lambda t:(t['event_ns'],t['row']))
        indices[venue] = (rows,[t['event_ns'] for t in rows])
    predictions = {}
    for t in trades:
        if t['group'] != 'exchange':
            continue
        bid, ask = [previous(hist[s],t['event_ns'],t['source_ns'],t['row']) for s in ['bid','ask']]
        old, reason = classify(t['price'],t['venue'],bid,ask,'nbbo')
        values = dict(old=old,old_reason=reason,bid_e9=bid[0] if bid else '',ask_e9=ask[0] if ask else '')
        for mode,venue in [('all_venues','ALL'),('same_venue',t['venue'])]:
            side, prev_row = tick(t['price'],t['event_ns'],t['source_ns'],t['row'],*indices[venue])
            label, method = fallback(t['price'],bid,ask,old,side)
            values[mode] = label
            values[mode+'_method'] = method
            values[mode+'_tick_source_row'] = prev_row if method=='tick_rule' else ''
        predictions[str(t['row'])] = values
    matched = []
    with pairs.open() as f:
        for r in csv.DictReader(f):
            if r['tolerance_ms'] != '0' or r['clock'] != 'event_ms' or r['truth']=='N':
                continue
            p = predictions[r['source_row']]
            if p['old'] != r['nbbo']:
                raise ValueError('original NBBO classification changed')
            matched.append(dict(r,**p))
    if len(matched)!=5385 or sum(r['old']=='N' for r in matched)!=422:
        raise ValueError('frozen primary sample changed')
    summary = dict(created_utc=datetime.now(timezone.utc).isoformat(),source=provenance,
                   matched_input=str(pairs),matched_sha256=hashlib.sha256(pairs.read_bytes()).hexdigest(),
                   policy='Frozen 5385 primary unique known-side pairs; original labels asserted unchanged. Midpoint only for valid uncrossed positive-size books and strictly inside spread. Exact midpoint uses last different-price positive-size exchange trade, strictly earlier event millisecond AND source-available. Tick history starts at 13:30, no fabricated pre-window seed. No tick on invalid/locked/crossed quotes. No condition filter/correction netting. Predictions use LSEG only.',
                   results={})
    for mode in ['all_venues','same_venue']:
        result = dict(total=stats(matched,mode),previous_N=stats([r for r in matched if r['old']=='N'],mode),layers={},venues={})
        for method in sorted({r[mode+'_method'] for r in matched}):
            result['layers'][method] = stats([r for r in matched if r[mode+'_method']==method],mode)
        for venue in ['NAS','BAT','PSE']:
            result['venues'][venue] = stats([r for r in matched if r['venue']==venue and r['old']=='N'],mode)
        summary['results'][mode] = result
    with (output/'classified.csv').open('x',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=list(matched[0]))
        writer.writeheader()
        writer.writerows(matched)
    with (output/'summary.json').open('x') as f:
        json.dump(summary,f,indent=2)
    print(json.dumps(summary['results'],indent=2))


if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--staging-root',required=True,type=Path)
    parser.add_argument('--matched-csv',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args()
    run(args.staging_root,args.matched_csv,args.output)
