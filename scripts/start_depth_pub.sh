#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^[a-z0-9]+-(futures|margin)$'

usage() {
  cat <<'USAGE'
Usage:
  start_depth_pub.sh

Behavior:
  - 必须在单个 venue 部署目录下执行（例如 ~/depth_pub/binance-futures）。
  - venue 由当前目录名自动推断。
  - 使用 pmdaemon 启动进程名: dp_<ex>_<market>（兼容删除旧名 depth_pub_<venue>）
  - 可用 PMDAEMON_BIN 覆盖二进制名（默认 pmdaemon）

Examples:
  cd ~/depth_pub/binance-futures
  ./scripts/start_depth_pub.sh
USAGE
}

if [[ $# -gt 0 ]]; then
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] 不支持参数: $*" >&2
      usage >&2
      exit 1
      ;;
  esac
fi

short_exchange() {
  case "${1,,}" in
    binance) echo "bn" ;;
    okex) echo "ok" ;;
    bybit) echo "bb" ;;
    bitget) echo "bg" ;;
    gate) echo "gt" ;;
    *)
      echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
      ;;
  esac
}

short_market() {
  case "${1,,}" in
    futures) echo "fu" ;;
    margin) echo "mg" ;;
    *)
      echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
      ;;
  esac
}

venue_short_tag() {
  local raw_venue="${1,,}"
  if [[ "$raw_venue" =~ ^([a-z0-9]+)-([a-z0-9]+)$ ]]; then
    echo "$(short_exchange "${BASH_REMATCH[1]}")_$(short_market "${BASH_REMATCH[2]}")"
    return 0
  fi
  echo "$raw_venue" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] 当前目录无法推断 venue: ${BASE_DIR}" >&2
  echo "[ERROR] 期望目录名形如 <exchange>-<market>，例如 binance-futures" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/depth_pub"
  "${SCRIPT_DIR}/../depth_pub"
  "${SCRIPT_DIR}/../target/release/depth_pub"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] depth_pub binary not found. Build first with: cargo build --release --bin depth_pub" >&2
  exit 1
fi

name="dp_$(venue_short_tag "$venue")"
legacy_name="depth_pub_${venue}"
rust_log="${RUST_LOG:-info}"

# CPU core binding lookup (optional, per-host table at ~/.mkt_signal_cores.sh).
# 不存在或没匹配条目 → 不传 --core，binary 自然跳过绑核。
CORE_BIND_TABLE="${MKT_CORE_BIND_TABLE:-$HOME/.mkt_signal_cores.sh}"
if [[ -f "$CORE_BIND_TABLE" ]]; then
  # shellcheck disable=SC1090
  source "$CORE_BIND_TABLE"
fi
extra_args_json=""
if declare -p MKT_CORE_BINDINGS >/dev/null 2>&1; then
  _bind_key="${venue}:depth_pub"
  if [[ -n "${MKT_CORE_BINDINGS[$_bind_key]:-}" ]]; then
    extra_args_json=", \"--core\", \"${MKT_CORE_BINDINGS[$_bind_key]}\""
    echo "[INFO] core bind ${MKT_CORE_BINDINGS[$_bind_key]} (table=$CORE_BIND_TABLE key=$_bind_key)"
  fi
fi

cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_name="$(json_escape "$name")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_venue="$(json_escape "$venue")"
json_rust_log="$(json_escape "$rust_log")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["--venue", "${json_venue}"${extra_args_json}],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${name}"
"${PMDAEMON[@]}" delete "$legacy_name" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$name"

echo ""
echo "[INFO] Started: ${name}"
echo "Venue: ${venue}"
echo "Logs: ${PMDAEMON[*]} logs ${name} --follow"
echo "Status: ${PMDAEMON[*]} list"
