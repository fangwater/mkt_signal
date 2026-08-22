#!/usr/bin/env bash
# 查看 remote_ip_diag 写入 Redis 的每 target 快照：
#   - best_pairs：最优 (出站IP → remote) 组合排名
#   - by_source：每个出站下各 remote 的明细
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
print(f"== {key}  {d.get('host')}:{d.get('port')}  updated={age_s:.1f}s ago ==")

pairs = d.get("best_pairs", [])
print(f"  best (src -> remote)  [top {min(len(pairs), 10)} of {len(pairs)}]:")
print(f"    {'SRC':<16}{'REMOTE':<18}{'ok%':>5}{'p50us':>9}{'p95us':>9}")
for p in pairs[:10]:
    print(f"    {str(p.get('src')):<16}{str(p.get('remote')):<18}"
          f"{p.get('success_pct',0):>5}{p.get('rtt_p50_us',-1):>9}{p.get('rtt_p95_us',-1):>9}")

for src, v in sorted(d.get("by_source", {}).items()):
    print(f"  [src {src}] total={v.get('total')} live={v.get('live')} "
          f"evicted={v.get('evicted')} probation={v.get('probation')}  "
          f"live_remotes={', '.join(v.get('live_remotes', [])) or '(none)'}")
print()
PY
done
