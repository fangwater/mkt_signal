#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="${PM2_NAMESPACE:-$(basename "${BASE_DIR}")}"

dir_name="$(basename "${BASE_DIR}")"
dir_lc="$(echo "${dir_name}" | tr 'A-Z' 'a-z')"

infer_ns_and_suffix() {
  local name="$1"
  python3 - "$name" <<'PY'
import sys

name = sys.argv[1].strip().lower()

def split_last(value: str):
    idx_dash = value.rfind("-")
    idx_us = value.rfind("_")
    idx = max(idx_dash, idx_us)
    if idx <= 0 or idx >= len(value) - 1:
        return None
    return value[:idx], value[idx + 1:]

first = split_last(name)
if not first:
    raise SystemExit(1)
base, _env_tag = first
second = split_last(base)
if not second:
    raise SystemExit(1)
prefix, ns = second
if not prefix or not ns:
    raise SystemExit(1)
print(ns, prefix)
PY
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
    echo "[ERROR] not an xarb env dir: ${dir_name} (expect like okex-binance-xarb-<env>)"
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
