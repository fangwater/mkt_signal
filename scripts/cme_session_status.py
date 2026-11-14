"""Causal minute-boundary status overlay; planned sessions remain authoritative."""
MINUTE_NS = 60_000_000_000


def validate(events):
    for event in events:
        if not isinstance(event['product'], str) or not event['product'].strip():
            raise ValueError('missing product')
        if event.get('ric') is not None and (not isinstance(event['ric'], str) or not event['ric'].strip()):
            raise ValueError('invalid RIC scope')
        if not isinstance(event['source'], str) or not event['source'].strip():
            raise ValueError('missing source')
        for key in ('known_at_ns', 'effective_ns'):
            if type(event[key]) is not int or not 0 <= event[key] < 2**63 - MINUTE_NS:
                raise ValueError('invalid status timestamp')
        if event['known_at_ns'] > event['effective_ns']:
            raise ValueError('status cannot backdate knowledge')
        if event['state'] not in ('open', 'halted'):
            raise ValueError('invalid status')


def project(planned, events, product, ric):
    """Return half-open UTC-ns intervals on the minute grid.

Future events can only change future minutes. A minute already started when
an immediate halt is published is retained, including its publication events.
"""
    validate(events)
    last = None
    for lo, hi in planned:
        if (type(lo) is not int or type(hi) is not int or not 0 <= lo < hi < 2**63
                or lo % MINUTE_NS or hi % MINUTE_NS or last is not None and lo < last):
            raise ValueError('invalid planned segment')
        last = hi
    matching = sorted((e for e in events if e['product'] == product and e.get('ric') in (None, ric)),
                      key=lambda e: (e['effective_ns'], e['known_at_ns']))
    for left, right in zip(matching, matching[1:]):
        if (left['effective_ns'], left['known_at_ns']) == (right['effective_ns'], right['known_at_ns']) and left['state'] != right['state']:
            raise ValueError('ambiguous simultaneous status')
    output = []
    for lo, hi in planned:
        boundaries = {lo, hi}
        for e in matching:
            at = ((e['effective_ns'] + MINUTE_NS - 1) // MINUTE_NS) * MINUTE_NS
            if lo < at < hi:
                boundaries.add(at)
        spans = sorted(boundaries)
        local = []
        for left, right in zip(spans, spans[1:]):
            applicable = [e for e in matching if e['effective_ns'] <= left]
            if applicable and applicable[-1]['state'] == 'halted':
                continue
            if local and local[-1][1] == left:
                local[-1][1] = right
            else:
                local.append([left, right])
        output.extend(local)
    return output
