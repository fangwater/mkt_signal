"""Correctness baseline for explicit, reviewed CME historical input catalogs."""
from datetime import date, datetime, timedelta
import math
from zoneinfo import ZoneInfo

MINUTE_NS = 60_000_000_000


def integer(value):
    if type(value) is not int or not 0 <= value < 2**63:
        raise ValueError('invalid integer timestamp')
    return value


def evidence(row):
    integer(row['known_at_ns'])
    if not isinstance(row['source'], str) or not row['source'].strip():
        raise ValueError('missing evidence source')


def project(catalog, products, start, end):
    """Validate required dates and return explicit daily grids, no inferred days."""
    start, end = date.fromisoformat(start), date.fromisoformat(end)
    if end <= start or not products or len(set(products)) != len(products):
        raise ValueError('invalid request')
    days = {}
    intervals = {}
    for row in catalog['days']:
        evidence(row)
        key = row['product'], date.fromisoformat(row['trading_day'])
        if key in days:
            raise ValueError('duplicate calendar day')
        segments = row['segments']
        if not segments:
            boundary = datetime.combine(key[1]-timedelta(days=1), datetime.min.time().replace(hour=17), ZoneInfo('America/Chicago'))
            if row['known_at_ns'] >= int(boundary.timestamp())*10**9:
                raise ValueError('closure not known before nominal open')
        last = None
        for lo, hi in segments:
            integer(lo); integer(hi)
            if lo % MINUTE_NS or hi % MINUTE_NS or hi <= lo or last is not None and lo < last:
                raise ValueError('invalid calendar segment')
            if row['known_at_ns'] >= lo:
                raise ValueError('calendar not known before open')
            last = hi
            intervals.setdefault(key[0], []).append((lo, hi))
        days[key] = row
    for values in intervals.values():
        ordered = sorted(values)
        if any(b[0] < a[1] for a, b in zip(ordered, ordered[1:])):
            raise ValueError('overlapping official trade dates')
    identities = {}
    for row in catalog['contracts']:
        evidence(row)
        lo, hi = date.fromisoformat(row['from']), date.fromisoformat(row['until'])
        if hi <= lo or not row['ric'] or len(row['contract_id'].split(':')) != 3:
            raise ValueError('invalid contract identity')
        exchange, product, month = row['contract_id'].split(':')
        date.fromisoformat(month+'-01')
        if not exchange or product != row['product']:
            raise ValueError('contract product mismatch')
        key = row['product'], row['ric']
        if any(lo < b and a < hi for a,b in identities.get(key, [])):
            raise ValueError('overlapping contract identity')
        identities.setdefault(key, []).append((lo,hi))
    for row in catalog['multipliers']:
        evidence(row)
        if not math.isfinite(row['volume_multiple']) or row['volume_multiple'] <= 0:
            raise ValueError('invalid multiplier')
        if date.fromisoformat(row['until']) <= date.fromisoformat(row['from']):
            raise ValueError('invalid multiplier coverage')
    result = []
    for product in products:
        candidates = [r for r in catalog['multipliers'] if r['product'] == product
                      and date.fromisoformat(r['from']) <= start and date.fromisoformat(r['until']) >= end]
        if len(candidates) != 1:
            raise ValueError('missing or ambiguous bounded multiplier')
        multiplier = candidates[0]
        day = start
        while day < end:
            record = days.get((product,day))
            if record is None:
                raise ValueError('missing explicit calendar date')
            if record['segments']:
                open_ns = record['segments'][0][0]
                if multiplier['known_at_ns'] >= open_ns:
                    raise ValueError('multiplier not known before open')
                contracts = []
                for c in catalog['contracts']:
                    if c['product'] != product or not c['from'] <= day.isoformat() < c['until']:
                        continue
                    if c['known_at_ns'] >= open_ns:
                        raise ValueError('identity not known before open')
                    if c['contract_id'].split(':')[0] != multiplier['exchange']:
                        raise ValueError('contract exchange mismatch')
                    contracts.append({'ric':c['ric'], 'contract_id':c['contract_id']})
                if not contracts:
                    raise ValueError('no reviewed contracts on open day')
                result.append({'product':product,'trading_day':day.isoformat(),
                               'segments':record['segments'],'volume_multiple':multiplier['volume_multiple'],
                               'contracts':sorted(contracts,key=lambda c:c['ric'])})
            day += timedelta(days=1)
    return result
