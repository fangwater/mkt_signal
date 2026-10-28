"""Independent raw-key/value check for bounded TAS identity witnesses.

An observed RIC is evidence of source identity only. It is not a listing date,
candidate-universe proof, exchange identity, expiry rule, or zero-volume proof.
Quote snapshots are excluded: their keys use exchange-second buckets rather
than the source publication clock used by the raw message kinds below.
"""
KINDS = {1: 'trade', 4: 'symbology', 6: 'settlement', 13: 'status', 14: 'reference'}


def decode(key, value):
    key, value = bytes.fromhex(key), bytes.fromhex(value)
    if len(key) != 31 or len(value) < 28 or value[:3] != b'CT\x01':
        raise ValueError('invalid record header')
    kind = key[0]
    if kind not in KINDS or value[3] != kind or key[1:17] != value[4:20]:
        raise ValueError('record identity mismatch')
    raw = key[1:17]
    ric = raw.rstrip(b'\0').decode('ascii')
    if not ric or b'\0' in raw.rstrip(b'\0'):
        raise ValueError('invalid RIC')
    key_ns = int.from_bytes(key[17:25], 'big')
    published_ns = int.from_bytes(value[20:28], 'little')
    if key_ns != published_ns:
        raise ValueError('publication clock mismatch')
    return dict(kind=kind, ric=ric, published_ns=published_ns,
                part=int.from_bytes(key[25:27], 'big'), seq=int.from_bytes(key[27:31], 'big'))


def verify(request, witness):
    row = decode(witness['key_hex'], witness['value_hex'])
    if row['ric'] != request['ric'] or not request['start_ns'] <= row['published_ns'] < request['cutoff_ns']:
        raise ValueError('witness outside request')
    if any(witness[k] != v for k, v in row.items()):
        raise ValueError('decoded output mismatch')
    return row
