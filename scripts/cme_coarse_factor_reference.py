"""Independent CME coarse-factor clock and representative formula reference."""
from __future__ import annotations

import numpy as np


def reset_flags(minutes, segment_start, session_index, previous_index=None,
                segment_id=None, previous_segment=None):
    """The complete frozen daily-table sequence defines expected sessions."""
    reset = np.array(segment_start, dtype=bool, copy=True)
    if not len(reset) or not reset[0]:
        raise ValueError('bar metadata must start a source segment')
    if minutes not in (1, 5, 15, 60, 1440):
        raise ValueError('unsupported frequency')
    if minutes in (60, 1440):
        reset[0] = previous_index is None or session_index != previous_index + 1
        if previous_segment is not None and segment_id != previous_segment:
            reset[0] = True
    return reset


def validate_clock(m, minutes):
    names = ['ts', 'end_ts', 'last_minute_ts', 'source_minutes', 'segment_start',
             'source_depth_ts_utc_ns', 'contract_id', 'ric']
    n = len(m['ts'])
    frequency = {5: '5min', 15: '15min', 60: '1h', 1440: '1day'}[minutes]
    if not n or any(len(m[k]) != n for k in names) or not m['segment_start'][0]:
        raise ValueError('invalid metadata shape')
    if m['frequency'] != frequency or m['bar_minutes'] != minutes:
        raise ValueError('invalid frequency')
    if minutes == 1440 and n != 1:
        raise ValueError('one HFQ bar per official day required')
    for i, (ts, end, last, count) in enumerate(zip(
            m['ts'], m['end_ts'], m['last_minute_ts'], m['source_minutes'])):
        if ts % 60 or end % 60 or last + 60 != end or count <= 0 or end <= ts:
            raise ValueError('invalid bar times')
        if minutes == 1440:
            if count * 60 > end - ts:
                raise ValueError('more source minutes than wall-clock minutes')
        elif count > minutes or end != ts + count * 60:
            raise ValueError('invalid intraday duration')
        if i and (ts < m['end_ts'][i-1] or (not m['segment_start'][i] and
                (ts != m['end_ts'][i-1] or m['source_minutes'][i-1] != minutes))):
            raise ValueError('overlap or unexplained segment')
        donor = m['source_depth_ts_utc_ns'][i]
        if donor is not None and not last * 10**9 <= donor < (last + 60) * 10**9:
            raise ValueError('donor outside last source minute')


def representative_factors(close, high, low, bid, ask, bids, asks, reset):
    """Seven existing formulas, evaluated independently and shifted one bar."""
    close, high, low, bid, ask = map(np.asarray, (close, high, low, bid, ask))
    reset = np.asarray(reset, dtype=bool)
    raw = {}
    with np.errstate(divide='ignore', invalid='ignore'):
        valid = np.r_[False, (close[1:] > 0) & (close[:-1] > 0)]
        returns = np.r_[np.nan, np.log(close[1:] / close[:-1])]
        returns[reset | ~valid] = np.nan
        raw['baseline_159'] = np.sin(returns)
        raw['baseline_160'] = np.cos(returns)
        raw['factor_009'] = np.where((bid > 0) & (ask > 0), np.log(ask)-np.log(bid), np.nan)
        for name, values in [('factor_013', bids), ('factor_014', asks)]:
            values = np.asarray(values)
            total = values.sum(axis=1)
            raw[name] = np.where(abs(total) <= 1e-12, 0., values[:, :5].sum(axis=1) / total)
    boundaries = np.r_[np.flatnonzero(reset), len(close)]
    if boundaries[0] != 0:
        raise ValueError('reference starts at a reset')
    for name, window, arrays in [('baseline_097', 45, [close]),
                                  ('baseline_106', 20, [close, high, -low])]:
        values = np.full(len(close), np.nan)
        for begin, end in zip(boundaries[:-1], boundaries[1:]):
            for i in range(begin+window-1, end):
                parts = [x[i-window+1:i+1] for x in arrays]
                if all(np.isfinite(x).all() for x in parts):
                    values[i] = sum(float(np.mean(x)) for x in parts)
        raw[name] = values
    result = {}
    for name, values in raw.items():
        shifted = np.r_[np.nan, values[:-1]]
        shifted[reset] = np.nan
        result['lseg_features_'+name] = shifted
    return result
