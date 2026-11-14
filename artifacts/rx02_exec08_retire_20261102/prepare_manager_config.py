#!/usr/bin/env python3
"""Prepare a minimal, validated Manager source disable without touching secrets."""
import copy
import os
from pathlib import Path
import re
import sys
import tomllib

root = Path.home()
current = root / 'crypto_cta_manager/config/cta-manager.toml'
audit = root / 'crypto_cta_manager/maintenance-backups/rx02_exec08_retire_20261102'
original = current.read_bytes()
if original != (audit / 'cta-manager.toml.before').read_bytes():
    raise SystemExit('live Manager config changed since preflight')
text = original.decode()
parts = re.split(r'(?m)(?=^\[\[sources\]\][ \t]*$)', text)
hits = [i for i, part in enumerate(parts) if re.search(r'(?m)^id[ \t]*=[ \t]*["\']binance_exec_trade08["\'][ \t]*$', part)]
if len(hits) != 1:
    raise SystemExit('expected exactly one Exec08 source section')
i = hits[0]
updated, count = re.subn(r'(?m)^enabled[ \t]*=[ \t]*true[ \t]*$', 'enabled = false', parts[i], count=1)
if count != 1:
    raise SystemExit('source enabled=true not found')
parts[i] = updated
output = ''.join(parts).encode()
before = tomllib.loads(text)
after = tomllib.loads(output.decode())
expected = copy.deepcopy(before)
sources = [s for s in expected['sources'] if s['id'] == 'binance_exec_trade08']
if len(sources) != 1 or sources[0].get('enabled') is not True:
    raise SystemExit('unexpected original source state')
sources[0]['enabled'] = False
if after != expected:
    raise SystemExit('unexpected unrelated configuration change')
staged = audit / 'cta-manager.toml.after'
fd = os.open(staged, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
with os.fdopen(fd, 'wb') as f:
    f.write(output)
    f.flush()
    os.fsync(f.fileno())
print('validated: only binance_exec_trade08 enabled changes true -> false')
