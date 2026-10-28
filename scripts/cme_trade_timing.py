"""Correctness oracle for explicit exchange/publication CME bar clocks.

Publication bars never place a source print in a bar that was already closed.
The original event clock is retained for diagnostics, not used to backfill.
"""
from cme_tas_daily_reference import event_time

MINUTE_NS = 60 * 10**9


def bar_time(published_ns, exchange_hms_ns, basis):
    if type(published_ns) is not int or not 0 <= published_ns < 2**63:
        raise ValueError('invalid source timestamp')
    if basis == 'publication':
        return published_ns
    if basis == 'exchange':
        return event_time(published_ns, exchange_hms_ns)
    raise ValueError('unknown time basis')


def timing_audit(published_ns, exchange_hms_ns, volume):
    recovered = event_time(published_ns, exchange_hms_ns)
    source_minute = published_ns // MINUTE_NS
    exchange_minute = recovered // MINUTE_NS
    return dict(later_minute=int(source_minute > exchange_minute),
                later_minute_volume=volume if source_minute > exchange_minute else 0,
                earlier_minute=int(source_minute < exchange_minute),
                missing_exchange_time=int(exchange_hms_ns is None))
