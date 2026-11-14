#!/usr/bin/env python3
"""Correctness baseline: complete Raw LL2 patches -> sparse 60s/15s snapshots."""
import argparse
import csv
import gzip
import json
import re
import struct
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

HEADER = '#RIC,Domain,Date-Time,GMT Offset,Type,MsgClass/FID number,UpdateType/Action,FID Name,FID Value,FID Enum String,PE Code,Template Number,Key/Msg Sequence Number,Number of FIDs'.split(',')
MISSING = -(1 << 63)
MISSING_COUNT = (1 << 32) - 1
MISSING_TIME = (1 << 64) - 1
UPDATE_TYPES = ('UNSPECIFIED','QUOTE','TRADE','NEWS_ALERT','VOLUME_ALERT','ORDER_INDICATION','CLOSING_RUN','CORRECTION','MARKET_DIGEST','QUOTES_TRADE','MULTIPLE','VERIFY')
FIELDS = ('BEST_BID', 'BEST_BSIZ', 'NO_BIDORD', 'BEST_ASK', 'BEST_ASIZ', 'NO_ASKORD')


def timestamp_ns(text):
    if not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,9})?Z', text):
        raise ValueError('invalid UTC timestamp: ' + text)
    whole, _, fraction = text[:-1].partition('.')
    seconds = int(datetime.strptime(whole, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp())
    value = seconds * 10**9 + int(fraction.ljust(9, '0') or '0')
    if not 0 < value < 1 << 64:
        raise ValueError(text)
    return value


def scaled(text):
    if not text:
        return MISSING
    if not re.fullmatch(r'[+-]?\d+(?:\.\d{0,9})?', text):
        raise ValueError('invalid fixed point: ' + text)
    value = int(Decimal(text) * 10**9)
    if not MISSING < value < 1 << 63:
        raise ValueError(text)
    return value


def exchange_time(text):
    if not text:
        return MISSING_TIME
    if not re.fullmatch(r'\d+:\d+:\d+(?:\.\d{1,9})?', text):
        raise ValueError(text)
    whole, _, fraction = text.partition('.')
    h, m, s = map(int, whole.split(':'))
    if m >= 60 or s >= 60:
        raise ValueError(text)
    value = (h*3600 + m*60 + s)*10**9 + int(fraction.ljust(9, '0') or '0')
    if value >= MISSING_TIME:
        raise ValueError(text)
    return value


def messages(inputs, partition_order=False):
    current = None
    order = 0
    for part,path in enumerate(inputs):
        local_order=0
        opener = gzip.open if str(path).endswith('.gz') else open
        with opener(path, 'rt', newline='') as stream:
            for row in csv.reader(stream):
                if row == HEADER:
                    continue
                if len(row) > 14 or len(row) < 10:
                    raise ValueError('invalid Raw row width')
                if row[0]:
                    if current:
                        if len(current['fields']) != current['declared']:
                            raise ValueError('incomplete FID message')
                        yield current
                    if len(row) != 14 or row[1] != 'Market Price' or row[4] != 'Legacy Level 2':
                        raise ValueError('invalid message header')
                    if not row[0].isascii() or not 0 < len(row[0]) <= 16:
                        raise ValueError('invalid RIC')
                    if (row[5]=='UPDATE' and row[6] not in UPDATE_TYPES and not (row[6].isascii() and row[6].isdigit() and int(row[6])<=255)) or (row[5] in ('REFRESH','STATUS') and row[6]!=''):
                        raise ValueError('unsupported update type: ' + row[6])
                    if row[5] not in ('UPDATE','REFRESH','STATUS'):
                        raise ValueError('unsupported message class: ' + row[5])
                    if row[5]=='STATUS' and row[13]!='0':
                        raise ValueError('nonempty Raw STATUS payload requires explicit policy')
                    order += 1
                    local_order += 1
                    offset = Decimal(row[3])*60
                    if offset != int(offset) or not -(1 << 15) <= offset < 1 << 15:
                        raise ValueError('GMT offset')
                    if not row[13].isascii() or not row[13].isdigit():
                        raise ValueError('Number of FIDs')
                    current = dict(ric=row[0], ts=timestamp_ns(row[2]), offset=int(offset), order=((part<<48)|local_order) if partition_order else order,
                                   declared=int(row[13]), fields=[], update_type=row[6], message_class=row[5])
                else:
                    if not current or row[4] != 'FID' or not row[5].isascii() or not row[5].isdigit():
                        raise ValueError('orphan/invalid FID')
                    current['fields'].append((row[7], row[8]))
                    if len(current['fields']) > current['declared']:
                        raise ValueError('too many FIDs')
    if current:
        if len(current['fields']) != current['declared']:
            raise ValueError('incomplete trailing message')
        yield current


def field_index(name):
    aliases={'BEST_BSZ10':55,'BEST_ASZ10':58,'NO_BIDRD10':56,'NO_ASKRD10':59}
    if name in aliases:
        return aliases[name]
    for index, prefix in enumerate(FIELDS):
        suffix = name.removeprefix(prefix)
        if suffix != name and suffix.isascii() and suffix.isdigit():
            level = int(suffix)
            if not 1 <= level <= 10:
                raise ValueError('unsupported depth level: ' + name)
            return (level - 1)*6 + index
    return None


def encode_value(message, values, exch_time):
    result = bytearray(432)
    struct.pack_into('<2sBBQQQh', result, 0, b'L2', 1, 1, message['ts'], message['order'], exch_time, message['offset'])
    for level in range(10):
        b, bv, bn, a, av, an = values[level*6:level*6+6]
        struct.pack_into('<qqqqII', result, 32+level*40, b, bv, a, av, bn, an)
    return result.hex()


def extract(inputs, frequency=60, partition_order=False):
    if frequency not in (15, 60):
        raise ValueError('frequency must be 15 or 60')
    states, selected = {}, {}
    census = dict(messages=0, fids=0, depth_messages=0, messages_by_ric={}, update_types={}, message_classes={}, fields={})
    for message in messages(inputs, partition_order):
        ric = message['ric']
        kind=message['message_class']
        census['message_classes'][kind]=census['message_classes'].get(kind,0)+1
        census['messages'] += 1
        census['fids'] += message['declared']
        census['messages_by_ric'][ric] = census['messages_by_ric'].get(ric, 0)+1
        action = message['update_type']
        census['update_types'][action] = census['update_types'].get(action, 0)+1
        state = states.setdefault(ric, dict(values=[MISSING_COUNT if i%6 in (2,5) else MISSING for i in range(60)],
                                           known=0, time=MISSING_TIME, last_ts=0))
        if message['ts'] < state['last_ts']:
            raise ValueError('decreasing timestamp: ' + ric)
        state['last_ts'] = message['ts']
        # Missing RWF cache/completion flags: discard knowledge from before this
        # REFRESH, rather than claiming the source cache was cleared or complete.
        if kind == 'REFRESH':
            state.update(values=[MISSING_COUNT if i%6 in (2,5) else MISSING for i in range(60)],
                         known=0, time=MISSING_TIME)
        changed = kind == 'REFRESH'
        for name, text in message['fields']:
            census['fields'][name] = census['fields'].get(name, 0)+1
            if name == 'TIMACT':
                state['time'] = exchange_time(text)
                state['known'] |= 1 << 60
            index = field_index(name)
            if index is None:
                continue
            if index%6 in (2,5):
                if not text:
                    value = MISSING_COUNT
                elif text.isascii() and text.isdigit() and int(text) < MISSING_COUNT:
                    value = int(text)
                else:
                    raise ValueError('invalid count: ' + text)
            else:
                value = scaled(text)
            state['values'][index] = value
            state['known'] |= 1 << index
            changed = True
        if not changed:
            continue
        census['depth_messages'] += 1
        for seconds in (frequency,):
            bucket = message['ts']//(seconds*10**9)*(seconds*10**9)
            key = ric.encode().ljust(16, b'\0') + struct.pack('>Q', bucket)
            selected[(seconds, key)] = dict(ric=ric, seconds=seconds, key_hex=key.hex(),
                                           value_hex=encode_value(message, state['values'], state['time']),
                                           known=state['known'])
    return dict(census=census, snapshots=sorted(selected.values(), key=lambda row: (row['seconds'], row['key_hex'])))


def minute_snapshots(snapshots):
    """Minute-end states from sparse 15s/60s; invalidations remain candidates."""
    selected = {}
    for snapshot in snapshots:
        if snapshot['seconds'] not in (15, 60):
            raise ValueError('unsupported snapshot frequency')
        key = bytes.fromhex(snapshot['key_hex'])
        bucket = int.from_bytes(key[16:], 'big')//60_000_000_000*60_000_000_000
        minute_key = key[:16] + struct.pack('>Q', bucket)
        value = bytes.fromhex(snapshot['value_hex'])
        order = struct.unpack_from('<QQ', value, 4)
        old = selected.get(minute_key)
        if old is None or order > struct.unpack_from('<QQ', bytes.fromhex(old['value_hex']), 4):
            selected[minute_key] = dict(snapshot, seconds=60, key_hex=minute_key.hex())
    return sorted(selected.values(), key=lambda row: row['key_hex'])


def fill_baseline_records(records, snapshots):
    """Independent baseline overlay for one source; preserves rows and other fields."""
    selected = {bytes.fromhex(s['key_hex']): s for s in minute_snapshots(snapshots)}
    output = []
    for source_row in records:
        row = dict(source_row)
        key = row['ric'].encode().ljust(16, b'\0') + struct.pack('>Q', row['ts']*10**9)
        snapshot = selected.get(key)
        if snapshot:
            value = bytes.fromhex(snapshot['value_hex'])
            known = snapshot['known']
            for level in range(10):
                bp,bv,ap,av,_,_ = struct.unpack_from('<qqqqII',value,32+40*level)
                for side,price,size,bit in [('bid',bp,bv,level*6),('ask',ap,av,level*6+3)]:
                    if known & (3<<bit) != 3<<bit or price==MISSING or size==MISSING or price<=0 or size<0:
                        continue
                    price,size = price/10**9,size/10**9
                    p,v = f'{side}{level}p',f'{side}{level}v'
                    if (row[p] is not None and row[p]!=price) or (row[v] is not None and row[v]!=size):
                        continue
                    if row[p] is None: row[p] = price
                    if row[v] is None: row[v] = size
            bp,bv,ap,av,_,_ = struct.unpack_from('<qqqqII',value,32)
            if row['mid_price'] is None and known & 27 == 27 and bp>0 and ap>=bp and bv>=0 and av>=0 and MISSING not in (bp,bv,ap,av):
                bid,bv,ask,av=bp/10**9,bv/10**9,ap/10**9,av/10**9
                if all(row[name]==number for name,number in zip(('bid0p','bid0v','ask0p','ask0v'),(bid,bv,ask,av))):
                    row['mid_price']=(bid+ask)/2
        output.append(row)
    return output


def fill_baseline_sources(records, source_snapshots, require_valid_l1=False):
    """Choose the first usable same-minute donor; never combine source books."""
    selected = [{bytes.fromhex(s['key_hex']): s for s in minute_snapshots(snapshots)}
                for snapshots in source_snapshots]
    output = []
    for original in records:
        key = original['ric'].encode().ljust(16, b'\0') + struct.pack('>Q', original['ts']*10**9)
        result = dict(original)
        for source in selected:
            snapshot = source.get(key)
            if snapshot is None:
                continue
            value = bytes.fromhex(snapshot['value_hex'])
            source_ns = struct.unpack_from('<Q', value, 4)[0]
            if not original['ts']*10**9 <= source_ns < (original['ts']+60)*10**9:
                raise ValueError('snapshot source outside baseline minute')
            bp,bv,ap,av,_,_ = struct.unpack_from('<qqqqII', value, 32)
            if require_valid_l1 and not (
                    snapshot['known'] & 27 == 27 and MISSING not in (bp,bv,ap,av)
                    and bp > 0 and ap >= bp and bv >= 0 and av >= 0):
                continue
            candidate = fill_baseline_records([original], [snapshot])[0]
            if candidate != original:
                result = candidate
                break
        output.append(result)
    return output


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, nargs='+', required=True)
    parser.add_argument('--frequency', type=int, choices=(15,60), default=60)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--partition-order',action='store_true')
    args = parser.parse_args()
    result = extract(args.input, args.frequency, args.partition_order)
    args.output.write_text(json.dumps(result, indent=2)+'\n')


if __name__ == '__main__':
    main()
