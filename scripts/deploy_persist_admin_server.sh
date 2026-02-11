#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

TARGET_DIR="${HOME}/persist_exporter_public"

echo "[INFO] building persist_admin_server (release)..."
cargo build --release --bin persist_admin_server

mkdir -p "${TARGET_DIR}" "${TARGET_DIR}/config" "${TARGET_DIR}/logs"

cp -f "${ROOT_DIR}/target/release/persist_admin_server" "${TARGET_DIR}/persist_admin_server"
chmod +x "${TARGET_DIR}/persist_admin_server"

if [[ ! -f "${TARGET_DIR}/config/persist_auto_exporter.toml" ]]; then
  cp -f "${ROOT_DIR}/config/persist_auto_exporter.toml" "${TARGET_DIR}/config/persist_auto_exporter.toml"
  echo "[INFO] copied default config to ${TARGET_DIR}/config/persist_auto_exporter.toml"
fi

cp -f "${ROOT_DIR}/scripts/start_persist_admin_server.sh" "${TARGET_DIR}/start_persist_admin_server.sh"
cp -f "${ROOT_DIR}/scripts/stop_persist_admin_server.sh" "${TARGET_DIR}/stop_persist_admin_server.sh"
chmod +x "${TARGET_DIR}/start_persist_admin_server.sh" "${TARGET_DIR}/stop_persist_admin_server.sh"

echo "[INFO] deployed persist_admin_server to ${TARGET_DIR}"
echo "[INFO] start: cd ${TARGET_DIR} && ./start_persist_admin_server.sh"
echo "[INFO] stop:  cd ${TARGET_DIR} && ./stop_persist_admin_server.sh"

