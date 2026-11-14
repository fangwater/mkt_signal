#!/usr/bin/env python3
"""Read current JP Prep combinations and Redis amount thresholds for replay."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

# Executed on JP: only named public strategy settings and amount thresholds are
# exported. env.sh, Redis credentials and trading credentials are never read.
REMOTE = r'''
import datetime, hashlib, json, math, sys, time
from pathlib import Path
import redis

request=json.loads(sys.stdin.readline())
r=redis.Redis(host='127.0.0.1', port=6379, decode_responses=True,
              socket_connect_timeout=3, socket_timeout=3)
r.ping()
profiles=[]
directory=Path.home()/request['config_dir']
paths=sorted(directory.glob('*.json'))
before={path:path.read_bytes() for path in paths}
groups=json.loads(before[directory/'symbol_groups.json'])
for path, raw in before.items():
    source=json.loads(raw)
    if not isinstance(source,dict) or source.get('strategy_group') not in request['groups']:
        continue
    if path.stem!=source.get('name'): raise ValueError('combination filename mismatch')
    if source.get('venue')!='binance-futures' or source.get('reference_venue') is not None:
        raise ValueError('Prep snapshot requires Binance futures')
    if 'signal' not in source: raise ValueError('missing current Prep signal parameters')
    profiles.append(dict(source_path=str(path),source_sha256=hashlib.sha256(raw).hexdigest(),combination=source))
if not profiles: raise ValueError('no current Prep configurations found')
profiles.sort(key=lambda p:(p['combination']['strategy_group'],p['combination'].get('symbol_group') or '',
    {'1min':1,'5min':5,'15min':15}[p['combination']['interval']]))
if sorted(directory.glob('*.json'))!=paths or any(path.read_bytes()!=raw for path,raw in before.items()):
    raise ValueError('JP configuration changed during snapshot; retry')
key='binance-futures:amount-thresholds'
metadata=r.hgetall(key+':meta')
current=datetime.datetime.now(datetime.timezone.utc).date().replace(day=1)
previous=(current-datetime.timedelta(days=1)).strftime('%Y-%m')
if (metadata.get('apply_month')!=current.strftime('%Y-%m') or
        metadata.get('source_month')!=previous or metadata.get('unit')!='USDT'):
    raise ValueError('shared amount thresholds have incorrect month or unit')
thresholds={}
for symbol, raw in r.hgetall(key).items():
    value=json.loads(raw)
    values=[value['medium_notional_threshold'],value['large_notional_threshold']]
    if not all(isinstance(v,(int,float)) and not isinstance(v,bool) and math.isfinite(v) and v>0 for v in values) or values[0]>values[1]:
        raise ValueError('invalid shared amount threshold: '+symbol)
    thresholds['binance-futures:'+symbol]=values
print(json.dumps(dict(fetched_at_ms=time.time_ns()//1000000, source_host=request['source_host'],
    profiles=profiles, symbol_groups=groups, amount_thresholds=thresholds, threshold_metadata=metadata),allow_nan=False))
'''


def fetch(host: str, groups: list[str], config_dir: str) -> dict:
    # Send the request after the Python program via a separate inherited stdin
    # file descriptor; no shell interpolation of host configuration or values.
    import shlex
    request = json.dumps({"groups": groups, "config_dir": config_dir, "source_host": host})
    program = "import io,sys\nsys.stdin=io.StringIO(" + repr(request + "\n") + ")\n" + REMOTE
    result = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, "python3 -c " + shlex.quote(program)],
        capture_output=True, text=True, timeout=45,
    )
    if result.returncode:
        raise RuntimeError("JP Prep bootstrap failed: " + result.stderr.strip())
    return json.loads(result.stdout)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="jp-meta-elvpn")
    parser.add_argument("--group", choices=["prep01", "prep02"], action="append", dest="groups")
    parser.add_argument("--config-dir", default="cta-prep-binance-futures/config/cta_prep_binance_futures")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    snapshot = fetch(args.host, args.groups or ["prep01", "prep02"], args.config_dir)
    summary = {"source_host": args.host, "execute": args.execute, "output": str(args.output),
        "profiles": [{"name": p["combination"]["name"], "strategy_group":p["combination"]["strategy_group"],
                      "symbol_group":p["combination"].get("symbol_group") or "all", "interval":p["combination"]["interval"],
                      "symbols": len({r['标的'] for r in p['combination']['factor_rules']}), "rules":len(p['combination']['factor_rules'])}
                     for p in snapshot["profiles"]], "amount_threshold_symbols": len(snapshot["amount_thresholds"])}
    if args.execute:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode="w", dir=args.output.parent, delete=False) as output:
            try:
                json.dump(snapshot, output, ensure_ascii=False, allow_nan=False, indent=2)
                output.write("\n")
                output.flush()
                os.fsync(output.fileno())
                os.replace(output.name, args.output)
            finally:
                Path(output.name).unlink(missing_ok=True)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
