#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
EXPORT_DIR="/home/${USER}/persist_exporter"
OKX_CACHE_NAME="okx_swap_multipliers.json"
OKX_CACHE_SRC="${BASE_DIR}/scripts/${OKX_CACHE_NAME}"
OKX_CACHE_DST="${EXPORT_DIR}/${OKX_CACHE_NAME}"

echo "[INFO] building persist_exporter..."
cargo build --release --bin persist_exporter

mkdir -p "${EXPORT_DIR}"
cp -f "${BASE_DIR}/target/release/persist_exporter" "${EXPORT_DIR}/persist_exporter"
cp -f "${BASE_DIR}/scripts/export_symbol_data_v5.py" "${EXPORT_DIR}/export_symbol_data_v5.py"
chmod +x "${EXPORT_DIR}/export_symbol_data_v5.py"

if [ ! -f "${OKX_CACHE_SRC}" ]; then
  echo "[INFO] fetching OKX swap multipliers cache..."
  python3 - <<PY
import json
import time
import urllib.request

url = "https://www.okx.com/api/v5/public/instruments?instType=SWAP"
cache_path = "${OKX_CACHE_SRC}"

def normalize_symbol(value):
    if value is None:
        return ""
    s = str(value).upper()
    if s == "NAN":
        return ""
    s = s.replace("-", "").replace("_", "")
    for suffix in ("SWAP", "PERP"):
        if s.endswith(suffix):
            s = s[: -len(suffix)]
    return s

def parse_float(value, default=None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default

try:
    with urllib.request.urlopen(url, timeout=10) as resp:
        body = resp.read().decode("utf-8", errors="replace")
    payload = json.loads(body)
    if payload.get("code") != "0":
        raise RuntimeError(f"OKX API error: {payload.get('code')} - {payload.get('msg')}")
    data = payload.get("data") or []
    multipliers = {}
    for inst in data:
        if not isinstance(inst, dict):
            continue
        if inst.get("ctType") != "linear":
            continue
        if inst.get("settleCcy") != "USDT":
            continue
        inst_id = inst.get("instId") or ""
        symbol_key = normalize_symbol(inst_id)
        if not symbol_key:
            continue
        ct_val = parse_float(inst.get("ctVal"), 1.0)
        ct_mult = parse_float(inst.get("ctMult"), 1.0)
        if ct_val is None or ct_mult is None:
            continue
        contract_size = ct_val * ct_mult
        if contract_size <= 0.0:
            continue
        multipliers[symbol_key] = contract_size
    if multipliers:
        payload = {"fetched_at": int(time.time()), "multipliers": multipliers}
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
        print(f"[INFO] cached OKX multipliers: {cache_path} ({len(multipliers)} symbols)")
    else:
        print("[WARN] OKX multipliers empty; cache not written")
except Exception as exc:
    print(f"[WARN] failed to fetch OKX multipliers: {exc}")
PY
fi

if [ -f "${OKX_CACHE_SRC}" ]; then
  cp -f "${OKX_CACHE_SRC}" "${OKX_CACHE_DST}"
  echo "[INFO] synced OKX multipliers cache ${OKX_CACHE_DST}"
fi

if [ ! -f "${EXPORT_DIR}/persist_exporter.toml" ]; then
  cp "${BASE_DIR}/config/persist_exporter.toml" "${EXPORT_DIR}/persist_exporter.toml"
  echo "[INFO] copied default config ${EXPORT_DIR}/persist_exporter.toml"
else
  echo "[INFO] keep existing config ${EXPORT_DIR}/persist_exporter.toml"
fi

echo "[INFO] deployed to ${EXPORT_DIR}"
