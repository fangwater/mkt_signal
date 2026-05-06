#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${BASE_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^[a-z0-9]+-(futures|margin)$'

usage() {
  cat <<'USAGE'
Usage:
  start_spread_pbs.sh

Behavior:
  - 必须在单 venue 部署目录下执行（如 ~/spread_pbs/okex-futures）。
  - 由当前目录名推断 venue，并查表得到固定 CPU 核（0–9）。
  - 启动方式：taskset -c <core> + pmdaemon，进程名 spp_<ex>_<market>。
USAGE
}

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi
if [[ $# -gt 0 ]]; then
  echo "[ERROR] 不支持参数: $*" >&2
  usage >&2
  exit 1
fi

# core 映射（必须与 deploy 脚本里的字母序一致）
core_for_venue() {
  case "${1,,}" in
    binance-margin)   echo 0 ;;
    binance-futures)  echo 1 ;;
    bitget-margin)    echo 2 ;;
    bitget-futures)   echo 3 ;;
    bybit-margin)     echo 4 ;;
    bybit-futures)    echo 5 ;;
    gate-margin)      echo 6 ;;
    gate-futures)     echo 7 ;;
    okex-margin)      echo 8 ;;
    okex-futures)     echo 9 ;;
    *) return 1 ;;
  esac
}

short_exchange() {
  case "${1,,}" in
    binance) echo "bn" ;;
    okex)    echo "ok" ;;
    bybit)   echo "bb" ;;
    bitget)  echo "bg" ;;
    gate)    echo "gt" ;;
    *)       echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
  esac
}
short_market() {
  case "${1,,}" in
    futures) echo "fu" ;;
    margin)  echo "mg" ;;
    *)       echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
  esac
}
venue_short_tag() {
  local venue="${1,,}"
  if [[ "$venue" =~ ^([a-z0-9]+)-([a-z0-9]+)$ ]]; then
    echo "$(short_exchange "${BASH_REMATCH[1]}")_$(short_market "${BASH_REMATCH[2]}")"
    return 0
  fi
  echo "$venue" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] 当前目录无法推断 venue: ${BASE_DIR}" >&2
  exit 1
fi

if ! CORE=$(core_for_venue "$venue"); then
  echo "[ERROR] venue 未配置 core 绑定: $venue" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi
if ! command -v taskset >/dev/null 2>&1; then
  echo "[ERROR] taskset not found（util-linux 包）" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/spread_pbs"
  "${SCRIPT_DIR}/../spread_pbs"
  "${ROOT_DIR}/target/release/spread_pbs"
)
BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] spread_pbs binary not found，先 cargo build --release --bin spread_pbs" >&2
  exit 1
fi

name="spp_$(venue_short_tag "$venue")"
rust_log="${RUST_LOG:-info}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

find_running_pids() {
  local venue_arg="--venue ${venue}"
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,args= | awk -v venue_arg="$venue_arg" '
      index($0, "spread_pbs") > 0 &&
      index($0, venue_arg) > 0 &&
      index($0, "awk -v ") == 0 &&
      index($0, "start_spread_pbs.sh") == 0 &&
      index($0, "stop_spread_pbs.sh") == 0 {
        print $1
      }
    '
  )
  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

json_name="$(json_escape "$name")"
json_bin="$(json_escape "$(command -v taskset)")"
json_base="$(json_escape "$BASE_DIR")"
json_venue="$(json_escape "$venue")"
json_rust_log="$(json_escape "$rust_log")"
json_inner_bin="$(json_escape "$BIN_PATH")"

# iceoryx2 默认按 CWD 查找 ./config/iceoryx2.toml；没有就从 root 兜底
if [[ ! -f "$BASE_DIR/config/iceoryx2.toml" && -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  mkdir -p "$BASE_DIR/config"
  cp "$ROOT_DIR/config/iceoryx2.toml" "$BASE_DIR/config/iceoryx2.toml"
fi

# pmdaemon args = ["-c", "<core>", "<bin>", "--venue", "<v>", "--core", "<core>"]
cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${CORE}"
      ],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${name} (venue=${venue}, core=${CORE})"
"${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true

mapfile -t leaked_pids < <(find_running_pids || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] Found leaked process(es): ${leaked_pids[*]}; SIGTERM"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids || true)
    [[ ${#leaked_pids[@]} -eq 0 ]] && break
    sleep 1
  done
  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, SIGKILL: ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
  fi
fi

"${PMDAEMON[@]}" --config "$cfg_file" start --name "$name"

echo ""
echo "[INFO] Started: ${name} pinned to core ${CORE}"
echo "Venue:  ${venue}"
echo "Logs:   ${PMDAEMON[*]} logs ${name} --follow"
echo "Status: ${PMDAEMON[*]} list"
