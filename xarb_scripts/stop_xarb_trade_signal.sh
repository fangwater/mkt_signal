#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

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

echo "[INFO] Deleting ${PROC_NAME} (namespace=${NAMESPACE})"
if npx pm2 delete "$PROC_NAME" --namespace "$NAMESPACE"; then
  echo "[INFO] Deleted ${PROC_NAME}"
else
  echo "[WARN] ${PROC_NAME} not found in namespace ${NAMESPACE}"
fi

echo ""
echo "[INFO] Remaining processes: npx pm2 status --namespace ${NAMESPACE}"
