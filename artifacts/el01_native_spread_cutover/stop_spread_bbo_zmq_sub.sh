#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
[[ "$BASE_DIR" == /home/el01/spread_pbs/binance-futures ]]
/home/el01/.local/bin/pmdaemon delete sbbzs_bn_fu >/dev/null 2>&1 || true
python3 - "$BASE_DIR/zmq-relay/spread_bbo_zmq_sub" <<'PY'
import glob,os,signal,sys,time
expected=sys.argv[1]
def matching():
 result=[]
 for p in glob.glob('/proc/[0-9]*'):
  try:
   if os.readlink(p+'/exe')==expected: result.append(int(p.rsplit('/',1)[1]))
  except (FileNotFoundError,PermissionError,ProcessLookupError): pass
 return result
for pid in matching(): os.kill(pid,signal.SIGTERM)
for _ in range(60):
 if not matching(): break
 time.sleep(.1)
for pid in matching(): os.kill(pid,signal.SIGKILL)
for _ in range(20):
 if not matching(): break
 time.sleep(.1)
assert not matching(), 'ZMQ subscriber remains running'
print('Stopped el01 Binance Futures ZMQ subscriber')
PY
