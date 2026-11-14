"""Read the actual closed RocksDB through persist_manager's native exporter."""
import json
import subprocess
from collections import defaultdict


def verify_persistence(database, exporter, output, *, allow_projection_error=False):
    subprocess.run([str(exporter), '--input-dir', str(database), '--output-dir', str(output)], check=True, capture_output=True, text=True)
    def rows(cf):
        return [json.loads(line) for line in (output / (cf + '.jsonl')).read_text().splitlines()]
    orders = rows('cn_uniform_orders')
    trades = rows('cn_trade_facts')
    evidence = rows('cn_trading_events')
    encode = lambda obj: json.dumps(obj, sort_keys=True, separators=(',', ':'))
    assert len({encode(r['identity']) for r in orders}) == len(orders)
    assert len({encode(t['fill']['identity']) for t in trades}) == len(trades)
    by_sequence = {e['sequence']: e for e in evidence}
    assert list(by_sequence) == list(range(1, len(evidence) + 1))
    errors = [e for e in evidence if e['projection_error']]
    assert bool(errors) == allow_projection_error, errors
    local = {(o['identity']['native']['session'], o['identity']['native']['order_ref']) for o in orders if o['locally_submitted']}
    for item in evidence:
        value = item['value']
        if value.get('type') == 'prepared_submit':
            assert (value['key']['session'], value['key']['order_ref']) in local
    factual = defaultdict(int)
    for trade in trades:
        fill = trade['fill']
        ident = fill['identity']
        assert isinstance(ident['trade_id'], str) and isinstance(fill['system_id'], str)
        assert trade['evidence_seq'] in by_sequence
        factual[(ident['trading_day'], ident['venue'], fill['system_id'])] += fill['lots']
    for record in orders:
        order = record['order']
        ident = record['identity']
        assert record['evidence_seq'] in by_sequence
        assert record['first_received_us'] <= record['updated_us']
        if order['system_id'] and not errors:
            assert order['filled'] == factual[(ident['trading_day'], ident['venue'], order['system_id'])], record
        if record['locally_submitted']:
            assert record['submitted_intent'] is not None
    return {'rocksdb_orders': len(orders), 'rocksdb_unique_trades': len(trades), 'rocksdb_evidence': len(evidence), 'projection_error_retained': bool(errors)}
