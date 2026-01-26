#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
EXPORT_DIR="${BASE_DIR}/exporter_data"

echo "[INFO] building persist_exporter..."
cargo build --release --bin persist_exporter

mkdir -p "${EXPORT_DIR}"
cp "${BASE_DIR}/target/release/persist_exporter" "${EXPORT_DIR}/persist_exporter"
if [ ! -f "${EXPORT_DIR}/persist_exporter.toml" ]; then
  cat > "${EXPORT_DIR}/persist_exporter.toml" <<'EOF'
targets = []
EOF
  echo "[INFO] created empty config ${EXPORT_DIR}/persist_exporter.toml"
else
  echo "[INFO] keep existing config ${EXPORT_DIR}/persist_exporter.toml"
fi

echo "[INFO] deployed to ${EXPORT_DIR}"
