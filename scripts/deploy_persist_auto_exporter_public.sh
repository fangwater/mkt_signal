#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_DIR="${HOME}/persist_exporter_public"

echo "[INFO] building persist_auto_exporter (release)..."
cargo build --release --bin persist_auto_exporter

mkdir -p "${TARGET_DIR}" "${TARGET_DIR}/config" "${TARGET_DIR}/logs"

cp -f "${ROOT_DIR}/target/release/persist_auto_exporter" "${TARGET_DIR}/persist_auto_exporter"
chmod +x "${TARGET_DIR}/persist_auto_exporter"

if [[ ! -f "${TARGET_DIR}/config/persist_auto_exporter.toml" ]]; then
  cp -f "${ROOT_DIR}/config/persist_auto_exporter.toml" "${TARGET_DIR}/config/persist_auto_exporter.toml"
  echo "[INFO] copied default config to ${TARGET_DIR}/config/persist_auto_exporter.toml"
else
  echo "[INFO] keeping existing config ${TARGET_DIR}/config/persist_auto_exporter.toml"
fi

cp -f "${ROOT_DIR}/scripts/start_persist_auto_exporter_public.sh" "${TARGET_DIR}/start_persist_auto_exporter_public.sh"
cp -f "${ROOT_DIR}/scripts/stop_persist_auto_exporter_public.sh" "${TARGET_DIR}/stop_persist_auto_exporter_public.sh"
chmod +x "${TARGET_DIR}/start_persist_auto_exporter_public.sh" "${TARGET_DIR}/stop_persist_auto_exporter_public.sh"

echo "[INFO] deployed public auto exporter to ${TARGET_DIR}"
echo "[INFO] start: cd ${TARGET_DIR} && ./start_persist_auto_exporter_public.sh"
echo "[INFO] stop:  cd ${TARGET_DIR} && ./stop_persist_auto_exporter_public.sh"

