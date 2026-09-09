"""Download a bounded, explicitly requested equity direction sample."""

import argparse
from collections import Counter
import csv
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import databento as db
from dotenv import load_dotenv

DATASETS = {'XNAS.ITCH': 'NAS', 'BATS.PITCH': 'BAT', 'ARCX.PILLAR': 'PSE'}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--env-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--symbol', default='AAPL')
    parser.add_argument('--start', default='2021-07-01T13:30:00Z')
    parser.add_argument('--end', default='2021-07-01T13:40:00Z')
    parser.add_argument('--datasets', nargs='+', choices=list(DATASETS), default=list(DATASETS))
    args = parser.parse_args()
    load_dotenv(args.env_file)
    client = db.Historical()
    duration = (datetime.fromisoformat(args.end.replace('Z','+00:00')) - datetime.fromisoformat(args.start.replace('Z','+00:00'))).total_seconds()
    if not 0 < duration <= 600:
        raise ValueError('sample must be at most ten minutes')
    datasets = dict.fromkeys(args.datasets)
    request = dict(schema='trades', symbols=[args.symbol], stype_in='raw_symbol',
                   start=args.start, end=args.end)
    costs = {ds: client.metadata.get_cost(dataset=ds, **request) for ds in datasets}
    print('quoted_usd', json.dumps(costs), flush=True)
    if sum(costs.values()) > 0.01:
        raise ValueError('sample cost exceeds USD 0.01 cap')
    if not args.execute:
        return
    args.output.mkdir(parents=True, exist_ok=False)
    manifest = dict(created_utc=datetime.now(timezone.utc).isoformat(),
                    request=request, quoted_usd=costs, sdk_version=db.__version__, files=[])
    with (args.output/'request.json').open('x') as f:
        json.dump(manifest, f, indent=2)
    for dataset in datasets:
        venue = DATASETS[dataset]
        path = args.output/(dataset+'.dbn')
        client.timeseries.get_range(dataset=dataset, path=path, **request)
        store = db.DBNStore.from_file(path)
        sides = Counter()
        with (args.output/(dataset+'.csv')).open('x', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['dataset','venue','row','ts_event','ts_recv','publisher_id',
                             'instrument_id','sequence','price_e9','size','side','flags'])
            for row, trade in enumerate(store):
                side = str(trade.side)
                sides[side] += 1
                writer.writerow([dataset,venue,row,trade.ts_event,trade.ts_recv,
                                 trade.publisher_id,trade.instrument_id,trade.sequence,
                                 trade.price,trade.size,side,trade.flags])
        item = dict(dataset=dataset, venue=venue, records=sum(sides.values()), sides=dict(sides),
                    file=str(path), sha256=hashlib.sha256(path.read_bytes()).hexdigest())
        manifest['files'].append(item)
        print(json.dumps(item), flush=True)
    with (args.output/'manifest.json').open('x') as f:
        json.dump(manifest, f, indent=2)


if __name__ == '__main__':
    main()
