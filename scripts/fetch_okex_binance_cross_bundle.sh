#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

REMOTE_HOST="${REMOTE_HOST:-ubuntu@54.64.147.69}"
REMOTE_TAR="${REMOTE_TAR:-/home/ubuntu/exporter_data/okex-binance-cross-trade/okex-binance-cross.tar.gz}"
SSH_KEY="${SSH_KEY:-${SCRIPT_DIR}/../aws-jp-srv-1.pem}"
JUMP_HOST="${JUMP_HOST:-root@38.55.198.59}"
LOCAL_DIR="${LOCAL_DIR:-${SCRIPT_DIR}}"
OUTPUT_DIR="${OUTPUT_DIR:-/mnt/Data/fanghaihzou/persist_data/okex-binance-cross}"

TAR_NAME="$(basename "${REMOTE_TAR}")"
LOCAL_TAR="${LOCAL_DIR}/${TAR_NAME}"
BUNDLE_DIR="${LOCAL_DIR}/okex-binance-cross_bundle"

if [ ! -f "${SSH_KEY}" ]; then
  echo "[ERROR] ssh key not found: ${SSH_KEY}" >&2
  exit 1
fi

echo "[INFO] fetching bundle from ${REMOTE_HOST}:${REMOTE_TAR}"
SCP_OPTS=(-i "${SSH_KEY}")
if [ -n "${JUMP_HOST}" ]; then
  SCP_OPTS+=(-o "ProxyJump=${JUMP_HOST}")
fi
scp "${SCP_OPTS[@]}" "${REMOTE_HOST}:${REMOTE_TAR}" "${LOCAL_TAR}"
if [ ! -f "${LOCAL_TAR}" ]; then
  echo "[ERROR] scp failed to fetch bundle: ${LOCAL_TAR}" >&2
  exit 1
fi

echo "[INFO] extracting bundle to ${BUNDLE_DIR}"
rm -rf "${BUNDLE_DIR}"
mkdir -p "${BUNDLE_DIR}"
tar -xzf "${LOCAL_TAR}" -C "${BUNDLE_DIR}"

EXPORT_SCRIPT="${BUNDLE_DIR}/export_symbol_data_v5.py"
if [ ! -f "${EXPORT_SCRIPT}" ]; then
  echo "[ERROR] export script not found in bundle: ${EXPORT_SCRIPT}" >&2
  exit 1
fi

echo "[INFO] calculating symbol count for workers"
SYMBOL_COUNT=$(
  DATA_DIR="${BUNDLE_DIR}" python3 - <<'PY'
import os
import pandas as pd

data_dir = os.environ["DATA_DIR"]

def normalize_symbol(value) -> str:
    if value is None or pd.isna(value):
        return ""
    s = str(value).upper()
    if s == "NAN":
        return ""
    s = s.replace("-", "").replace("_", "")
    for suffix in ("SWAP", "PERP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s

def collect_symbol_keys(df: pd.DataFrame, cols: list[str]) -> set[str]:
    keys: set[str] = set()
    for col in cols:
        if col not in df.columns:
            continue
        series = df[col].map(normalize_symbol)
        for val in series.dropna().unique().tolist():
            if val:
                keys.add(val)
    return keys

def read_parquet(name: str, cols: list[str]) -> pd.DataFrame:
    path = os.path.join(data_dir, name)
    return pd.read_parquet(path, columns=cols)

symbol_keys: set[str] = set()
symbol_keys |= collect_symbol_keys(read_parquet("order_updates.parquet", ["symbol"]), ["symbol"])
symbol_keys |= collect_symbol_keys(read_parquet("trade_updates.parquet", ["symbol"]), ["symbol"])
symbol_keys |= collect_symbol_keys(
    read_parquet("signals_arb_open.parquet", ["opening_symbol", "hedging_symbol"]),
    ["opening_symbol", "hedging_symbol"],
)
symbol_keys |= collect_symbol_keys(
    read_parquet("signals_arb_hedge.parquet", ["opening_symbol", "hedging_symbol"]),
    ["opening_symbol", "hedging_symbol"],
)
symbol_keys |= collect_symbol_keys(
    read_parquet("signals_arb_cancel.parquet", ["opening_symbol", "hedging_symbol"]),
    ["opening_symbol", "hedging_symbol"],
)

close_path = os.path.join(data_dir, "signals_arb_close.parquet")
if os.path.exists(close_path):
    symbol_keys |= collect_symbol_keys(
        pd.read_parquet(close_path, columns=["opening_symbol", "hedging_symbol"]),
        ["opening_symbol", "hedging_symbol"],
    )

print(len(symbol_keys))
PY
)

if [ "${SYMBOL_COUNT}" -le 0 ]; then
  echo "[ERROR] invalid symbol count: ${SYMBOL_COUNT}" >&2
  exit 1
fi

echo "[INFO] export --all (workers=${SYMBOL_COUNT})"
python3 "${EXPORT_SCRIPT}" \
  --dir "${BUNDLE_DIR}" \
  --all \
  --output-dir "${OUTPUT_DIR}" \
  --workers "${SYMBOL_COUNT}"
