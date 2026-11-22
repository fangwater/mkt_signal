#!/usr/bin/env bash
set -euo pipefail

# Run from the live pair environment after its second generation has started.
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [[ $# != 8 ]]; then
  echo "Usage: $0 <open-venue> <hedge-venue> <warm-dir> <sha256> <old-pid> <old-start-ticks> <output-field> <timer-unit>" >&2
  exit 2
fi
open_venue="$1"
hedge_venue="$2"
warm_dir="$(realpath -e -- "$3")"
expected_sha="$4"
old_pid="$5"
old_start="$6"
output_field="$7"
timer_unit="$8"

[[ "$expected_sha" =~ ^[0-9a-f]{64}$ && "$old_pid" =~ ^[0-9]+$ && "$old_start" =~ ^[0-9]+$ ]]
[[ "$warm_dir" != "$BASE_DIR"* && -x "$warm_dir/rolling_metrics" && -s "$warm_dir/required.json" ]]
[[ "$(basename "$BASE_DIR")" == "${open_venue}-${hedge_venue}" ]]
[[ "$output_field" == "${open_venue}_${hedge_venue}::"* && "$timer_unit" =~ ^rolling-warm-[a-z0-9-]+$ ]]

exec 9>"$warm_dir/promotion.lock"
flock -n 9 || exit 0
if [[ -f "$warm_dir/verified" ]]; then
  systemctl --user stop "${timer_unit}.timer"
  exit 0
fi
if [[ -s "$warm_dir/publish" ]]; then
  published_at="$(<"$warm_dir/publish")"
  key="rolling_metrics_thresholds_${open_venue}_${hedge_venue}"
  if redis-cli --raw HGET "$key" "$output_field" | python3 -c '
import json, sys
payload = sys.stdin.read().strip()
raise SystemExit(not payload or json.loads(payload).get("update_tp", 0) <= int(sys.argv[1]))
' "$published_at"; then
    date -u +%FT%TZ >"$warm_dir/verified"
    echo "Warm rolling_metrics publishing verified for ${open_venue}_${hedge_venue}"
    systemctl --user stop "${timer_unit}.timer"
  fi
  exit 0
fi

[[ -s "$warm_dir/status" ]] || exit 0
read -r warm_pid remaining <"$warm_dir/status"
[[ "$warm_pid" =~ ^[0-9]+$ && "$remaining" == 0 ]] || exit 0
status_age=$(( $(date +%s) - $(stat -c %Y -- "$warm_dir/status") ))
(( status_age >= 0 && status_age <= 90 )) || exit 0
[[ "$(readlink "/proc/$warm_pid/exe")" == "$warm_dir/rolling_metrics" ]] || exit 0

if [[ ! -r "/proc/$old_pid/stat" || "$(awk '{print $22}' "/proc/$old_pid/stat")" != "$old_start" ||
      "$(readlink "/proc/$old_pid/cwd")" != "$BASE_DIR" ]]; then
  echo "Original rolling_metrics identity changed. Keep candidate on standby." >&2
  exit 1
fi
printf '%s  %s\n' "$expected_sha" "$warm_dir/rolling_metrics" | sha256sum --status -c
install -m 0755 -- "$warm_dir/rolling_metrics" "$BASE_DIR/rolling_metrics.${expected_sha:0:12}.new"
mv -Tf -- "$BASE_DIR/rolling_metrics.${expected_sha:0:12}.new" "$BASE_DIR/rolling_metrics"
"$BASE_DIR/scripts/rolling_metrics/stop_rolling_metrics.sh" \
  --open-venue "$open_venue" --hedge-venue "$hedge_venue" --legacy-only
[[ "$(readlink "/proc/$warm_pid/exe")" == "$warm_dir/rolling_metrics" ]]
date +%s%3N >"$warm_dir/publish.new"
mv -Tf -- "$warm_dir/publish.new" "$warm_dir/publish"
echo "Enabled warm rolling_metrics pid=$warm_pid for ${open_venue}_${hedge_venue}"
