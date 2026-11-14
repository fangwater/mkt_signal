#!/usr/bin/env bash
set -euo pipefail
export PATH="/home/el01/.local/bin:$PATH"
base=/home/el01/spread_pbs/binance-futures
stage=/home/el01/.el01-native-spread-staging
backup="$base/.deploy-backup/native-direct-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -m 700 "$backup"
cp -a "$base/env.sh" "$base/spread_pbs" "$backup/"
cp -a "$base/scripts" "$base/config" "$backup/"
for file in start_spread_pbs.sh stop_spread_pbs.sh stop_spread_bbo_zmq_sub.sh; do
 install -m 755 "$stage/$file" "$base/scripts/.$file.new"
 mv -f "$base/scripts/.$file.new" "$base/scripts/$file"
done
install -m 755 "$stage/spread_pbs" "$base/.spread_pbs.new"
mv -f "$base/.spread_pbs.new" "$base/spread_pbs"
install -m 644 "$stage/mkt_cfg.yaml" "$base/config/.mkt_cfg.yaml.new"
mv -f "$base/config/.mkt_cfg.yaml.new" "$base/config/mkt_cfg.yaml"
python3 - "$base/env.sh" <<'PY'
import pathlib,re,sys
p=pathlib.Path(sys.argv[1]); text=p.read_text()
values={
 'SPREAD_PBS_ENABLE_TRADE':'0',
 'SPREAD_PBS_ENABLE_INCREMENTAL':'0',
 'SPREAD_PBS_ENABLE_DERIVATIVES':'1',
 'SPREAD_PBS_BINANCE_FUTURES_ROLE':'split',
 'SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE':'2',
 'SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE':'1',
 'SPREAD_PBS_SYMBOLS':'',
}
for k,v in values.items():
 pattern=r'(?m)^\s*(?:export\s+)?'+re.escape(k)+r'=.*$'
 line='export '+k+'='+repr(v)
 if re.search(pattern,text): text=re.sub(pattern,line,text)
 else: text+='\n'+line+'\n'
p.write_text(text)
PY
chmod 600 "$base/env.sh"
printf 'Remote backup: %s\n' "$backup"
bash "$base/scripts/stop_spread_bbo_zmq_sub.sh"
bash "$base/scripts/start_spread_pbs.sh"
ps -eo pid,comm,args | grep -E '[s]pread_pbs --venue|[s]pread_bbo_zmq_sub --venue'
