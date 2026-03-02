#!/usr/bin/env bash
set -euo pipefail

# Install ONNX Runtime shared libs into repository third_party path.
#
# Usage:
#   bash scripts/install_ort_third_party.sh /path/to/onnxruntime-linux-x64-<ver>.tgz
#   bash scripts/install_ort_third_party.sh /path/to/onnxruntime-linux-x64-<ver>

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <onnxruntime tar.gz OR extracted directory>" >&2
  exit 1
fi

INPUT_PATH="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DEST_DIR="${REPO_DIR}/third_party/onnxruntime/linux-x86_64/lib"

mkdir -p "${DEST_DIR}"

HOST_ARCH="$(uname -m || true)"
if [[ "${HOST_ARCH}" != "x86_64" ]]; then
  echo "[WARN] host arch is '${HOST_ARCH}', this layout is for linux-x86_64." >&2
fi

WORK_DIR=""
SRC_ROOT=""

cleanup() {
  if [[ -n "${WORK_DIR}" && -d "${WORK_DIR}" ]]; then
    rm -rf "${WORK_DIR}"
  fi
}
trap cleanup EXIT

if [[ -f "${INPUT_PATH}" ]]; then
  case "${INPUT_PATH}" in
    *.tgz|*.tar.gz)
      WORK_DIR="$(mktemp -d)"
      tar -xzf "${INPUT_PATH}" -C "${WORK_DIR}"
      # ONNX Runtime tarball usually extracts to a single top-level directory.
      SRC_ROOT="$(find "${WORK_DIR}" -mindepth 1 -maxdepth 1 -type d | head -n 1)"
      if [[ -z "${SRC_ROOT}" ]]; then
        echo "[ERROR] failed to locate extracted root from ${INPUT_PATH}" >&2
        exit 1
      fi
      ;;
    *)
      echo "[ERROR] unsupported file type: ${INPUT_PATH}. expected .tgz or .tar.gz" >&2
      exit 1
      ;;
  esac
elif [[ -d "${INPUT_PATH}" ]]; then
  SRC_ROOT="${INPUT_PATH}"
else
  echo "[ERROR] input not found: ${INPUT_PATH}" >&2
  exit 1
fi

SRC_LIB_DIR="${SRC_ROOT}/lib"
if [[ ! -d "${SRC_LIB_DIR}" ]]; then
  echo "[ERROR] lib directory not found: ${SRC_LIB_DIR}" >&2
  exit 1
fi

shopt -s nullglob
SO_FILES=("${SRC_LIB_DIR}"/libonnxruntime.so*)
if [[ ${#SO_FILES[@]} -eq 0 ]]; then
  echo "[ERROR] no libonnxruntime.so* found under ${SRC_LIB_DIR}" >&2
  exit 1
fi

for f in "${SO_FILES[@]}"; do
  cp -a "${f}" "${DEST_DIR}/"
done

echo "[INFO] installed ONNX Runtime libs to: ${DEST_DIR}"
ls -l "${DEST_DIR}" | sed 's/^/[INFO] /'
echo "[INFO] build is configured to use this path via .cargo/config.toml"
