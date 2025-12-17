#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# 可选：设置 PM2 namespace（默认使用部署目录名，可用环境变量覆盖）
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

# 候选二进制位置：部署目录优先，其次源码目录
BIN_CANDIDATES=(
  "${SCRIPT_DIR}/trade_signal"
  "${SCRIPT_DIR}/../trade_signal"
  "${SCRIPT_DIR}/target/release/trade_signal"
  "${SCRIPT_DIR}/../target/release/trade_signal"
  "${BASE_DIR}/trade_signal"
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
  echo "[ERROR] trade_signal binary not found. Build first with: cargo build --release --bin trade_signal"
  exit 1
fi

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"

infer_ns_and_suffix() {
  local name="$1"

  for env_suffix in "_trade" "_test"; do
    if [[ "$name" == *"$env_suffix" ]]; then
      local base="${name%$env_suffix}"
      base="${base%_}"
      local ns="${base##*_}"
      local prefix="${base%_*}"
      if [[ -n "$ns" && -n "$prefix" ]]; then
        echo "${ns} ${prefix}"
        return 0
      fi
    fi
  done

  for env_suffix in "-trade" "-test"; do
    if [[ "$name" == *"$env_suffix" ]]; then
      local base="${name%$env_suffix}"
      base="${base%-}"
      local ns="${base##*-}"
      local prefix="${base%-*}"
      if [[ -n "$ns" && -n "$prefix" ]]; then
        echo "${ns} ${prefix}"
        return 0
      fi
    fi
  done

  return 1
}

NS=""
SUFFIX=""
if read -r NS SUFFIX < <(infer_ns_and_suffix "$dir_lc"); then
  :
fi

CLI_EXCHANGE="${1:-}"
PM2_TAG=""
ARGS=()

case "$NS" in
  fr)
    EXCHANGE="$SUFFIX"
    if [[ -n "$CLI_EXCHANGE" && "$CLI_EXCHANGE" != "$EXCHANGE" ]]; then
      echo "[ERROR] exchange mismatch: dir exchange=${EXCHANGE} arg exchange=${CLI_EXCHANGE}"
      exit 1
    fi
    PM2_TAG="$EXCHANGE"
    ARGS=(--exchange "$EXCHANGE")
    ;;
  xarb)
    open_ex="${SUFFIX%%-*}"
    rest="${SUFFIX#*-}"
    hedge_ex="${rest%%-*}"
    if [[ -z "$open_ex" || -z "$hedge_ex" || "$open_ex" == "$hedge_ex" ]]; then
      echo "[ERROR] invalid xarb dir suffix: ${SUFFIX} (expect like okex-binance)"
      exit 1
    fi
    PM2_TAG="${open_ex}_${hedge_ex}"
    ;;
  *)
    # Fallback: use directory name as tag; trade_signal will require proper CWD or args.
    PM2_TAG="$(echo "${dir_lc}" | sed 's/[^a-z0-9_-]/_/g')"
    ;;
esac

PROC_NAME="trade_signal_${PM2_TAG}"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE" \
  -- \
  "${ARGS[@]}"

echo ""
echo "[INFO] Started trade_signal (ns=${NS:-unknown} suffix=${SUFFIX:-unknown})"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"

