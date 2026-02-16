#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^[a-z0-9]+-(futures|margin|spot|swap|perp|perpetual)$'

usage() {
  cat <<'USAGE'
Usage:
  start_fusion_factor_pub.sh

Behavior:
  - 必须在单个 venue 部署目录下执行（例如 ~/fusion_factor/binance-futures）。
  - venue 由当前目录名自动推断。
  - 优先使用 venue 专属二进制: fusion_factor_pub_<venue>
  - 使用 pmdaemon 启动进程名: fusion_factor_pub_<venue>
  - 可用 PMDAEMON_BIN 覆盖二进制名（默认 pmdaemon）

Examples:
  cd ~/fusion_factor/binance-futures
  ./scripts/start_fusion_factor_pub.sh
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

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] 当前目录无法推断 venue: ${BASE_DIR}" >&2
  echo "[ERROR] 期望目录名形如 <exchange>-<market>，例如 binance-futures" >&2
  exit 1
fi
VENUE_BIN_NAME="fusion_factor_pub_${venue}"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/${VENUE_BIN_NAME}"
  "${BASE_DIR}/fusion_factor_pub"
  "${SCRIPT_DIR}/../${VENUE_BIN_NAME}"
  "${SCRIPT_DIR}/../fusion_factor_pub"
  "${SCRIPT_DIR}/../target/release/fusion_factor_pub"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] fusion_factor_pub binary not found. Build first with: cargo build --release --bin fusion_factor_pub" >&2
  exit 1
fi

if [[ ! -f "${BASE_DIR}/config/fusion_factor_pub.yaml" ]]; then
  echo "[ERROR] config not found: ${BASE_DIR}/config/fusion_factor_pub.yaml" >&2
  exit 1
fi

name="fusion_factor_pub_${venue}"
rust_log="${RUST_LOG:-info}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

json_name="$(json_escape "$name")"
json_bin="$(json_escape "$BIN_PATH")"
json_base="$(json_escape "$BASE_DIR")"
json_venue="$(json_escape "$venue")"
json_cfg="$(json_escape "config/fusion_factor_pub.yaml")"
json_rust_log="$(json_escape "$rust_log")"

cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": ["--venue", "${json_venue}", "--config", "${json_cfg}"],
      "cwd": "${json_base}",
      "env": {
        "RUST_LOG": "${json_rust_log}"
      }
    }
  ]
}
JSON

echo "[INFO] Restarting ${name}"
"${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$name"

echo ""
echo "[INFO] Started: ${name}"
echo "Venue: ${venue}"
echo "Config: ${BASE_DIR}/config/fusion_factor_pub.yaml"
echo "Logs: ${PMDAEMON[*]} logs ${name} --follow"
echo "Status: ${PMDAEMON[*]} list"
