#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
INSTALL_ROOT="${INSTALL_ROOT:-/home/${USER}/order_export}"
ONNX_SRC_DIR="${BASE_DIR}/third_party/onnxruntime/linux-x86_64/lib"
ONNX_DST_DIR="${INSTALL_ROOT}/lib"

echo "[INFO] installing order_export into ${INSTALL_ROOT}"
cargo install --path "${BASE_DIR}" --bin order_export --root "${INSTALL_ROOT}" --force

if [[ ! -d "${ONNX_SRC_DIR}" ]]; then
  echo "[ERROR] ONNX Runtime library directory not found: ${ONNX_SRC_DIR}" >&2
  exit 1
fi

mkdir -p "${ONNX_DST_DIR}"
cp -a "${ONNX_SRC_DIR}"/libonnxruntime*.so* "${ONNX_DST_DIR}/"

VERSIONED_LIB="$(find "${ONNX_DST_DIR}" -maxdepth 1 -type f -name 'libonnxruntime.so.*' | sort | tail -n 1)"
if [[ -z "${VERSIONED_LIB}" ]]; then
  echo "[ERROR] libonnxruntime.so.* not found under ${ONNX_DST_DIR}" >&2
  exit 1
fi

VERSIONED_BASENAME="$(basename "${VERSIONED_LIB}")"
ln -sfn "${VERSIONED_BASENAME}" "${ONNX_DST_DIR}/libonnxruntime.so.1"
ln -sfn "libonnxruntime.so.1" "${ONNX_DST_DIR}/libonnxruntime.so"

REAL_BIN="${INSTALL_ROOT}/bin/order_export_bin"
mv "${INSTALL_ROOT}/bin/order_export" "${REAL_BIN}"
cat > "${INSTALL_ROOT}/bin/order_export" <<EOF
#!/usr/bin/env bash
set -euo pipefail
export LD_LIBRARY_PATH="${ONNX_DST_DIR}\${LD_LIBRARY_PATH:+:\${LD_LIBRARY_PATH}}"
exec "${REAL_BIN}" "\$@"
EOF
chmod +x "${INSTALL_ROOT}/bin/order_export"

echo "[INFO] binary installed at ${INSTALL_ROOT}/bin/order_export"
echo "[INFO] ONNX Runtime libs installed at ${ONNX_DST_DIR}"
echo "[INFO] set once:"
echo "  export ORDER_EXPORT_BASE_DIR=/home/${USER}"
echo "[INFO] example:"
echo "  cd /home/${USER}/binance_mm_alpha"
echo "  ${INSTALL_ROOT}/bin/order_export --date 2026-03-25"
echo "  ${INSTALL_ROOT}/bin/order_export --start 2026-03-25T01:02:03Z --end 2026-03-25T02:03:04Z"
echo "[INFO] default output dir:"
echo "  ./20260325"
echo "  ./20260325T010203.000000Z__20260325T020304.000000Z"
