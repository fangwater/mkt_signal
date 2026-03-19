#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")

BIN_CANDIDATES=(
  "${BASE_DIR}/viz_server"
  "${SCRIPT_DIR}/viz_server"
  "${BASE_DIR}/target/release/viz_server"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] viz_server binary not found. Deploy/build first."
  exit 1
fi

usage() {
  cat <<'EOF_USAGE'
Usage: mm_scripts/start_mm_viz_server.sh [--cfg config/viz.toml] [--exchange <binance|okex|gate|bybit|bitget>]

Notes:
  - Starts viz_server with pmdaemon using config/viz.toml (or VIZ_CFG / --cfg).
  - Exchange is inferred from the directory name (<exchange>_mm_<env>), unless --exchange is set.
EOF_USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

CFG_PATH=""
EXCHANGE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cfg)
      CFG_PATH="${2:-}"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      shift 2
      ;;
    *)
      echo "[ERROR] Unknown arg: $1"
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$CFG_PATH" ]]; then
  CFG_PATH="${VIZ_CFG:-config/viz.toml}"
fi

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"
dir_tag="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
if [[ -z "$EXCHANGE" ]]; then
  if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]mm([_-].*)?$ ]]; then
    EXCHANGE="${BASH_REMATCH[1]}"
  fi
fi

EXCHANGE="${EXCHANGE,,}"
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" ]]; then
  echo "[ERROR] Failed to infer exchange; please pass --exchange"
  exit 1
fi

PROC_NAME="${PMDAEMON_NAME:-viz_server_${dir_tag}}"
RUST_LOG="${RUST_LOG:-info}"
IPC_NS="${IPC_NAMESPACE:-}"
if [[ -z "$IPC_NS" ]]; then
  IPC_NS="$(basename "${BASE_DIR}")"
  echo "[WARN] IPC_NAMESPACE not set; use default: ${IPC_NS}"
fi

if [[ ! -f "$BASE_DIR/$CFG_PATH" ]]; then
  echo "[ERROR] viz config not found: $BASE_DIR/$CFG_PATH"
  echo "[ERROR] Deploy first: scripts/deploy_mm_viz_server.sh ..."
  exit 1
fi

if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi

TMP_FILES=()
cleanup_tmp_files() {
  if [[ ${#TMP_FILES[@]} -gt 0 ]]; then
    rm -f "${TMP_FILES[@]}" >/dev/null 2>&1 || true
  fi
}
trap cleanup_tmp_files EXIT

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

shell_quote() {
  printf '%q' "$1"
}

cfg_file="$(mktemp)"
TMP_FILES+=("$cfg_file")

json_name="$(json_escape "$PROC_NAME")"
json_shell="$(json_escape "/bin/bash")"
json_base="$(json_escape "$BASE_DIR")"
json_cfg="$(json_escape "$CFG_PATH")"
json_rust_log="$(json_escape "$RUST_LOG")"
json_ipc_ns="$(json_escape "$IPC_NS")"
cmd="if [[ -f $(shell_quote "$ENV_FILE") ]]; then source $(shell_quote "$ENV_FILE"); fi; exec $(shell_quote "$BIN_PATH")"
json_cmd="$(json_escape "$cmd")"

cat >"$cfg_file" <<CFG
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_shell}",
      "args": ["-lc", "${json_cmd}"],
      "cwd": "${json_base}",
      "env": {
        "VIZ_CFG": "${json_cfg}",
        "RUST_LOG": "${json_rust_log}",
        "IPC_NAMESPACE": "${json_ipc_ns}"
      }
    }
  ]
}
CFG

echo "[INFO] Restarting ${PROC_NAME} (cfg=${CFG_PATH})"
"${PMDAEMON[@]}" delete "$PROC_NAME" >/dev/null 2>&1 || true
"${PMDAEMON[@]}" --config "$cfg_file" start --name "$PROC_NAME"

echo ""
echo "[INFO] Started viz_server"
echo "Config: ${CFG_PATH}"
echo "Logs: ${PMDAEMON[*]} logs ${PROC_NAME} --follow"
echo "Status: ${PMDAEMON[*]} list"
