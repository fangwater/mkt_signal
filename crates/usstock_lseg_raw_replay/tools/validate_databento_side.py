"""Label-blind reciprocal-unique trade matching and quote-test validation."""

import argparse
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

DATASETS = {'XNAS.ITCH': 'NAS', 'BATS.PITCH': 'BAT', 'ARCX.PILLAR': 'PSE'}


def match(left, right, tolerance_ms=0):
    index = defaultdict(list)
    for j, r in enumerate(right):
        index[(r['price'], r['size'])].append((r['ms'], j))
    for rows in index.values():
        rows.sort()
    candidates = []
    reverse = Counter()
    for l in left:
        rows = index.get((l['price'], l['size']), [])
        lo = bisect_left(rows, (l['ms']-tolerance_ms, -1))
        hi = bisect_right(rows, (l['ms']+tolerance_ms, len(right)))
        hits = [j for _, j in rows[lo:hi]]
        candidates.append(hits)
        reverse.update(hits)
    return [(hits[0] if len(hits) == 1 and reverse[hits[0]] == 1 else None,
             'unique' if len(hits) == 1 and reverse[hits[0]] == 1 else
             'unmatched' if not hits else 'ambiguous', len(hits)) for hits in candidates]


def estimate(row, rule):
    if rule == 'hybrid':
        if row['single_reason'] == 'no_matching_contributor':
            return row['nbbo']
        return row['single']
    return row[rule]


def metrics(rows, rule):
    counts = Counter()
    for r in rows:
        truth = r['truth']
        guess = estimate(r, rule)
        counts['matched'] += 1
        counts['truth:'+truth] += 1
        if truth == 'N':
            counts['truth_unknown_estimate:'+guess] += 1
            continue
        counts['truth_known'] += 1
        counts['truth_known_volume'] += int(r['volume'])
        counts['confusion:'+truth+'->'+guess] += 1
        if guess == 'N':
            counts['abstain'] += 1
        else:
            counts['classified'] += 1
            counts['classified_volume'] += int(r['volume'])
            counts['correct' if guess == truth else 'wrong'] += 1
            if guess == truth:
                counts['correct_volume'] += int(r['volume'])
    result = dict(counts)
    result['accuracy'] = counts['correct']/counts['classified'] if counts['classified'] else None
    result['volume_accuracy'] = counts['correct_volume']/counts['classified_volume'] if counts['classified_volume'] else None
    result['coverage'] = counts['classified']/counts['truth_known'] if counts['truth_known'] else None
    return result


def run(lseg_csv, db_root, output):
    output.mkdir(parents=True, exist_ok=False)
    with lseg_csv.open() as f:
        raw = [r for r in csv.DictReader(f) if r['ric'] == 'AAPL.O']
    source = {r['source_row']: r for r in raw if r['clock'] == 'source'}
    left_all = [r for r in raw if r['clock'] == 'event_ms']
    summary = dict(created_utc=datetime.now(timezone.utc).isoformat(),
                   policy='Reciprocal unique candidates, same venue/exact e9 price/exact shares; event milliseconds with tolerance 0 or 1. No side in matching. Primary=0ms. Unknown native side excluded from accuracy, not counted as errors. Hybrid falls back only for no_matching_contributor. Source-window LSEG vs receive-window Databento may differ at boundaries. No cancellation netting. No common trade ID was used.',
                   inputs={str(lseg_csv): hashlib.sha256(lseg_csv.read_bytes()).hexdigest()},
                   datasets={}, validation={})
    matched = []
    match_rows = []
    for dataset, venue in DATASETS.items():
        path = db_root/(dataset+'.csv')
        summary['inputs'][str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
        with path.open() as f:
            right = list(csv.DictReader(f))
        for r in right:
            r.update(price=int(r['price_e9']),size=int(r['size']),ms=int(r['ts_event'])//1000000)
            if r['side'] not in ['A','B','N']:
                raise ValueError(r['side'])
        left = [dict(r, price=int(r['price_e9']), size=int(r['volume']), ms=int(r['event_ns'])//1000000) for r in left_all if r['venue'] == venue]
        ds = dict(lseg_count=len(left), databento_count=len(right),
                  databento_side=dict(Counter(r['side'] for r in right)), matching={})
        for tolerance in [0, 1]:
            pairs = match(left, right, tolerance)
            ds['matching'][str(tolerance)] = dict(Counter(p[1] for p in pairs))
            for l, (j, status, count) in zip(left, pairs):
                d = right[j] if j is not None else None
                match_rows.append(dict(dataset=dataset,venue=venue,tolerance_ms=tolerance,
                                       lseg_source_row=l['source_row'],status=status,candidates=count,
                                       databento_row=d['row'] if d else '',
                                       event_delta_ns=int(d['ts_event'])-int(l['event_ns']) if d else ''))
                if d is None:
                    continue
                truth = {'A':'S','B':'B','N':'N'}[d['side']]
                for clock, row in [('event_ms',l), ('source',source[l['source_row']])]:
                    matched.append(dict(row,dataset=dataset,tolerance_ms=tolerance,
                                        clock=clock,truth=truth,databento_row=d['row'],
                                        databento_ts_event=d['ts_event']))
        summary['datasets'][dataset] = ds
    for tolerance in [0,1]:
        for clock in ['event_ms','source']:
            for dataset in [*DATASETS,'ALL']:
                rows = [r for r in matched if r['tolerance_ms']==tolerance and r['clock']==clock and (dataset=='ALL' or r['dataset']==dataset)]
                for subset in ['all','missing_contributor','plain_at']:
                    selected = [r for r in rows if subset=='all' or
                                (subset=='missing_contributor' and r['single_reason']=='no_matching_contributor') or
                                (subset=='plain_at' and r['condition']=='@')]
                    key = f'{tolerance}ms:{clock}:{dataset}:{subset}'
                    summary['validation'][key] = {rule:metrics(selected,rule) for rule in ['single','nbbo','hybrid']}
    with (output/'matches.csv').open('x',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=list(match_rows[0]))
        writer.writeheader(); writer.writerows(match_rows)
    fields = ['dataset','tolerance_ms','clock','source_row','event_ns','venue','price_e9','volume','condition',
              'databento_row','databento_ts_event','truth','single','single_reason','nbbo','hybrid']
    with (output/'classified_matches.csv').open('x',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=fields,extrasaction='ignore')
        writer.writeheader()
        for r in matched:
            writer.writerow(dict(r,hybrid=estimate(r,'hybrid')))
    with (output/'summary.json').open('x') as f:
        json.dump(summary,f,indent=2)
    print(json.dumps(summary['datasets'],indent=2))
    for k,v in summary['validation'].items():
        if k.startswith('0ms:') and k.endswith(':ALL:all'):
            print(k,json.dumps(v))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--lseg-csv',type=Path,required=True)
    parser.add_argument('--databento-root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    run(args.lseg_csv,args.databento_root,args.output)
