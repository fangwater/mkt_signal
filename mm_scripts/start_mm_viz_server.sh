#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Optional: PM2 namespace (defaults to directory name)
PM2_NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

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
  cat <<'EOF'
Usage: mm_scripts/start_mm_viz_server.sh [--cfg config/viz.toml] [--exchange <binance|okex|gate|bybit|bitget>]

Notes:
  - Starts viz_server with PM2 using config/viz.toml (or VIZ_CFG / --cfg).
  - Exchange is inferred from the directory name (<exchange>_mm_<env>), unless --exchange is set.
  - Default PM2 name uses the deploy dir name to avoid collisions.
EOF
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

PROC_NAME="${PM2_NAME:-viz_server_${dir_tag}}"
RUST_LOG="${RUST_LOG:-info}"

if [[ ! -f "$BASE_DIR/$CFG_PATH" ]]; then
  echo "[ERROR] viz config not found: $BASE_DIR/$CFG_PATH"
  echo "[ERROR] Deploy first: scripts/deploy_mm_viz_server.sh ..."
  exit 1
fi

echo "[INFO] Restarting ${PROC_NAME} (namespace=${PM2_NAMESPACE} cfg=${CFG_PATH})"
npx pm2 delete "$PROC_NAME" --namespace "$PM2_NAMESPACE" >/dev/null 2>&1 || true

(
  cd "$BASE_DIR"
  VIZ_CFG="$CFG_PATH" RUST_LOG="$RUST_LOG" npx pm2 start "$BIN_PATH" \
    --name "$PROC_NAME" \
    --namespace "$PM2_NAMESPACE"
)

echo ""
echo "[INFO] Started viz_server"
echo "Namespace: ${PM2_NAMESPACE}"
echo "Config: ${CFG_PATH}"
echo "Logs: npx pm2 logs --namespace ${PM2_NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${PM2_NAMESPACE}"
