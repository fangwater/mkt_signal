#!/usr/bin/env bash
set -euo pipefail

# intra trade_signal 启动脚本（同所期现）：
# - 部署目录约定：<exchange>-intra-<env>
# - 进程名: intra_<exchange>_<env>_trade_signal
# - 使用 pm2

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENV_FILE="${BASE_DIR}/env.sh"
if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  source "$ENV_FILE"
fi
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

BIN_CANDIDATES=(
  "${BASE_DIR}/trade_signal"
  "${SCRIPT_DIR}/trade_signal"
  "${BASE_DIR}/target/release/trade_signal"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] trade_signal binary not found. Deploy/build first."
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="${dir_name,,}"

EXCHANGE=""
ENV_TAG=""
if [[ "$dir_lc" =~ ^([a-z0-9]+)[-_]intra[-_]([a-z0-9][a-z0-9_-]*)$ ]]; then
  EXCHANGE="${BASH_REMATCH[1]}"
  ENV_TAG="${BASH_REMATCH[2]}"
fi
if [[ "$EXCHANGE" == "okx" ]]; then
  EXCHANGE="okex"
fi
if [[ -z "$EXCHANGE" || -z "$ENV_TAG" ]]; then
  echo "[ERROR] not an intra env dir: ${dir_name} (expect <exchange>-intra-<env>)"
  exit 1
fi
ENV_TAG="$(printf '%s' "$ENV_TAG" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//')"

PROC_NAME="intra_${EXCHANGE}_${ENV_TAG}_trade_signal"
LEGACY_PROC_NAME="trade_signal_${EXCHANGE}"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$LEGACY_PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  --cwd "$BASE_DIR"

echo ""
echo "[INFO] Started trade_signal (exchange=${EXCHANGE} env=${ENV_TAG})"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
