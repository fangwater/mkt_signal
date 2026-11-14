#!/usr/bin/env python3
"""Exercise real Redis transactions using temporary keys and a mocked exchange."""
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
import uuid

target = Path(sys.argv[1])
sys.path.insert(0, str(target/'scripts'))
import check_fr_collateral as check
import redis

rds = check.redis_client(check.source_env(target))
with tempfile.TemporaryDirectory(prefix='fr_collateral_probe_') as tmp:
    env_dir = Path(tmp)/('bybit_fr_collateralprobe_'+uuid.uuid4().hex[:12])
    env_dir.mkdir()
    (env_dir/'env.sh').write_text('')
    (env_dir/'trade_engine.toml').write_text('local_ips=["127.0.0.1"]')
    keys = check.symbol_list_keys(env_dir.name, 'bybit-margin', 'bybit-futures')
    fwd, bwd = keys[check.LIST_NAMES.index('fwd_trade_symbols')], keys[check.LIST_NAMES.index('bwd_trade_symbols')]
    assert not any(rds.exists(k) for k in keys)
    calls = []
    def verify(client, report, execute):
        assert json.loads(rds.get(fwd)) == ['OLDUSDT']
        assert report.assets == ['NEW', 'OLD', 'USDT'] and execute
        calls.append('verified-before-publish')
    try:
        rds.set(fwd, '["OLDUSDT"]', ex=120)
        with patch.object(check, 'ensure_collateral', side_effect=verify):
            report = check.publish_symbol_lists(rds, env_dir, 'bybit', 'bybit-margin', 'bybit-futures',
                                                {'fwd_trade_symbols': ['OLDUSDT', 'NEWUSDT'], 'bwd_trade_symbols': []})
        assert json.loads(rds.get(fwd)) == ['OLDUSDT', 'NEWUSDT']
        assert json.loads(rds.get(bwd)) == [] and calls == ['verified-before-publish']
        def conflicting_writer(client, report, execute):
            rds.set(bwd, '["CONCURRENTUSDT"]', ex=120)
        try:
            with patch.object(check, 'ensure_collateral', side_effect=conflicting_writer):
                check.publish_symbol_lists(rds, env_dir, 'bybit', 'bybit-margin', 'bybit-futures',
                                           {'fwd_trade_symbols': ['REPLACEMENTUSDT'], 'bwd_trade_symbols': []})
            raise AssertionError('concurrent Redis update did not abort publication')
        except redis.exceptions.WatchError:
            pass
        assert json.loads(rds.get(fwd)) == ['OLDUSDT', 'NEWUSDT']
        assert json.loads(rds.get(bwd)) == ['CONCURRENTUSDT']
    finally:
        rds.delete(*keys)
    assert not any(rds.exists(k) for k in keys)
print(json.dumps({'verify_before_publish': True, 'atomic_publication': True,
                  'concurrent_write_rejected': True, 'temporary_keys_removed': True,
                  'exchange_calls_mocked': True}))
