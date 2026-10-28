"""Source-prefix volume oracle; explicit reviewed requests, no inferred calendars."""
from datetime import date
from cme_tas_daily_reference import event_time

NS = 10**9
HALF_DAY = 43200 * NS
MAX = 2**63 - 1
PRODUCTS = set('URO JY HG SI TU FV TY TN US AUL'.split())


def integer(value):
    if type(value) is not int or not 0 <= value <= MAX:
        raise ValueError('invalid integer')
    return value


def validate(request):
    r = request
    if r['product'] not in PRODUCTS or date.fromisoformat(r['trading_day']).isoformat() != r['trading_day']:
        raise ValueError('invalid product/date')
    freeze, decision = integer(r['freeze_ns']), integer(r['decision_ns'])
    if freeze >= decision or not r['calendar_source'].strip() or integer(r['calendar_known_at_ns']) >= freeze:
        raise ValueError('unknown calendar or invalid freeze')
    last = None
    for lo, hi in r['segments']:
        integer(lo); integer(hi)
        if lo % (60*NS) or hi % (60*NS) or lo >= hi or last is not None and lo < last:
            raise ValueError('invalid segment')
        last = hi
    if last is None or last >= freeze or r['segments'][0][0] < HALF_DAY:
        raise ValueError('reference day not complete before freeze')
    contracts = {}
    for c in r['contracts']:
        ric = c['ric']
        if not isinstance(ric, str) or not 0 < len(ric.encode('ascii')) <= 16 or '\x00' in ric or ric in contracts:
            raise ValueError('invalid/duplicate RIC')
        if not c['source'].strip() or integer(c['known_at_ns']) >= freeze or integer(c['listed_ns']) >= freeze:
            raise ValueError('identity not known/listed at freeze')
        contracts[ric] = c
    if not contracts:
        raise ValueError('empty candidate universe')
    return r['segments'][0][0] - HALF_DAY, contracts


def freeze(request, events, completed_rics):
    """completed_rics must come from successful full bounded scans, never observation."""
    start, contracts = validate(request)
    end = request['freeze_ns']
    quantities = dict.fromkeys(contracts, 0)
    uncertain = dict.fromkeys(contracts, 0)
    audit = dict(source_start_ns=start, source_cutoff_ns=end, source_max_published_ns=None,
                 inspected=0, included=0, outside_session=0, invalid_price=0,
                 missing_exchange_time=0, future_exchange_time=0,
                 future_exchange_same_session=0)
    keys = set()
    for e in events:
        published = integer(e['published_ns'])
        if not start <= published < end or e['ric'] not in contracts:
            continue
        key = e['ric'], published, integer(e['part']), integer(e['seq'])
        if e['part'] >= 2**16 or e['seq'] >= 2**32 or key in keys:
            raise ValueError('invalid/duplicate source key')
        keys.add(key)
        audit['inspected'] += 1
        audit['source_max_published_ns'] = max(published, audit['source_max_published_ns'] or 0)
        if e['price_e9'] is None or e['price_e9'] <= 0:
            audit['invalid_price'] += 1
            continue
        integer(e['price_e9'])
        volume = integer(e['volume'])
        if not 0 < volume < 2**32 - 1:
            raise ValueError('invalid printable volume')
        hms = e['exchange_hms_ns']
        if hms is not None:
            integer(hms)
        event = event_time(published, hms)
        audit['missing_exchange_time'] += hms is None
        if event > published:
            audit['future_exchange_time'] += 1
            # The request requires every segment to have closed before freeze.
            # If both clocks identify the same completed segment, the daily
            # quantity is invariant. This does not authorize minute backfills.
            if not any(lo <= published < hi and lo <= event < hi for lo, hi in request['segments']):
                uncertain[e['ric']] = integer(uncertain[e['ric']] + volume)
                continue
            audit['future_exchange_same_session'] += 1
        if not any(lo <= event < hi for lo, hi in request['segments']):
            audit['outside_session'] += 1
            continue
        quantities[e['ric']] = integer(quantities[e['ric']] + volume)
        audit['included'] += 1
    if len(set(completed_rics)) != len(completed_rics) or set(completed_rics) - set(contracts):
        raise ValueError('invalid scan completion')
    reasons = []
    if set(completed_rics) != set(contracts):
        reasons.append('source_scan_incomplete')
    rows = []
    for ric in sorted(contracts):
        lower = quantities[ric]
        upper = integer(lower + uncertain[ric])
        rows.append(dict(ric=ric, volume=lower if not reasons and lower == upper else None,
                         volume_lower=lower if not reasons else None,
                         volume_upper=upper if not reasons else None))
    return dict(batch=dict(product=request['product'], trading_day=request['trading_day'],
                           available_at_ns=end, complete=not reasons, source='tas_printable',
                           rows=rows),
                audit=audit, blocked_reasons=reasons)
