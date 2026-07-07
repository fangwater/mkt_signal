#!/usr/bin/env bash
# 查看 remote_ip_diag 写入 Redis 的每 target 健康快照（Live IP 排名 + 明细）。
#
# 用法：
#   scripts/remote_ip_diag_print.sh
#   REDIS_HOST=127.0.0.1 REDIS_PORT=6379 REDIS_PREFIX=remote_ip_diag: scripts/remote_ip_diag_print.sh
set -euo pipefail

HOST="${REDIS_HOST:-127.0.0.1}"
PORT="${REDIS_PORT:-6379}"
PREFIX="${REDIS_PREFIX:-remote_ip_diag:}"

mapfile -t KEYS < <(redis-cli -h "$HOST" -p "$PORT" --scan --pattern "${PREFIX}*" | sort)
if [ "${#KEYS[@]}" -eq 0 ]; then
    echo "no keys under ${PREFIX}* on ${HOST}:${PORT} (is remote_ip_diag running?)"
    exit 0
fi

for k in "${KEYS[@]}"; do
    redis-cli -h "$HOST" -p "$PORT" get "$k" | python3 - "$k" <<'PY'
import sys, json, time

key = sys.argv[1]
raw = sys.stdin.read().strip()
if not raw:
    print(f"[{key}] (empty)")
    sys.exit(0)
try:
    d = json.loads(raw)
except Exception as e:
    print(f"[{key}] parse error: {e}")
    sys.exit(0)

age_s = (int(time.time() * 1000) - int(d.get("updated_unix_ms", 0))) / 1000.0
print(f"== {key}  {d.get('host')}:{d.get('port')}  "
      f"total={d.get('total')} live={d.get('live')} evicted={d.get('evicted')} "
      f"probation={d.get('probation')}  updated={age_s:.1f}s ago ==")
live = d.get("live_ips", [])
print("  rank:", ", ".join(live) if live else "(none)")
print(f"  {'IP':<18}{'STATE':<10}{'n':>4}{'ok%':>6}{'p50us':>9}{'p95us':>9}")
for s in d.get("ips", []):
    print(f"  {str(s.get('ip')):<18}{str(s.get('state')):<10}"
          f"{s.get('samples',0):>4}{s.get('success_pct',0):>6}"
          f"{s.get('rtt_p50_us',-1):>9}{s.get('rtt_p95_us',-1):>9}")
print()
PY
done
