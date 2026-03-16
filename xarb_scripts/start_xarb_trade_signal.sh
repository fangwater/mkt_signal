#!/usr/bin/env bash
set -euo pipefail

# xarb trade_signal 启动脚本：
# - 依赖部署目录名推断 xarb pair（目录名需形如 <open>-<hedge>-xarb-<trade|test>）
# - 使用 PM2 namespace（默认=部署目录名，可用 PM2_NAMESPACE 覆盖）

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
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

PM2_TAG=""
case "$NS" in
  xarb)
    open_ex="${SUFFIX%%-*}"
    rest="${SUFFIX#*-}"
    hedge_ex="${rest%%-*}"
    if [[ -z "$open_ex" || -z "$hedge_ex" ]]; then
      echo "[ERROR] invalid xarb dir suffix: ${SUFFIX} (expect like okex-binance)"
      exit 1
    fi
    if [[ "$open_ex" == "$hedge_ex" ]]; then
      PM2_TAG="${open_ex}_std"
    else
      PM2_TAG="${open_ex}_${hedge_ex}"
    fi
    ;;
  *)
    echo "[ERROR] not an xarb env dir: ${dir_name} (expect like okex-binance-xarb-trade)"
    exit 1
    ;;
esac

PROC_NAME="trade_signal_${PM2_TAG}"
RUST_LOG="${RUST_LOG:-info}"

echo "[INFO] Restarting ${PROC_NAME} (namespace=${NAMESPACE})"
npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE" >/dev/null 2>&1 || true

RUST_LOG="${RUST_LOG}" npx pm2 start "$BIN_PATH" \
  --name "$PROC_NAME" \
  --namespace "$NAMESPACE"

echo ""
echo "[INFO] Started trade_signal (ns=${NS} suffix=${SUFFIX})"
echo "Namespace: ${NAMESPACE}"
echo "Logs: npx pm2 logs --namespace ${NAMESPACE} ${PROC_NAME}"
echo "Status: npx pm2 status --namespace ${NAMESPACE}"
