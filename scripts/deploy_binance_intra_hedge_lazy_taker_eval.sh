#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN_NAME="hedge_lazy_taker_eval"

# shellcheck source=scripts/deploy_intra_lib.sh
source "${ROOT_DIR}/scripts/deploy_intra_lib.sh"

usage() {
  cat <<'USAGE'
Usage:
  scripts/deploy_binance_intra_hedge_lazy_taker_eval.sh [--env-name binance-intra-arb01] [--jobs N] [--cargo-target-dir PATH]

Builds and installs the Binance intra lazy-taker evaluation binary and its start/stop scripts.
The deploy does not start the process.
USAGE
}

ENV_NAME="binance-intra-arb01"
BUILD_JOBS=""
CARGO_TARGET_DIR_OVERRIDE=""

while [[ $# -gt 0 ]]; do
  case "${1}" in
    --env-name)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --jobs)
      BUILD_JOBS="${2:-}"
      shift 2
      ;;
    --cargo-target-dir)
      CARGO_TARGET_DIR_OVERRIDE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "[ERROR] unknown argument: ${1}" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! "${ENV_NAME,,}" =~ ^binance[-_]intra([-_].+)?$ ]]; then
  echo "[ERROR] this deploy only supports binance-intra environments: ${ENV_NAME}" >&2
  exit 1
fi

TARGET_DIR="${HOME}/${ENV_NAME}"
mkdir -p "${TARGET_DIR}/intra_scripts" "${TARGET_DIR}/data/hedge_lazy_taker_eval"

CARGO_TARGET_DIR_EFFECTIVE="$(intra_effective_cargo_target_dir "${ROOT_DIR}" "${CARGO_TARGET_DIR_OVERRIDE}")"
echo "[INFO] building ${BIN_NAME} for ${ENV_NAME}"
(
  cd "${ROOT_DIR}"
  CARGO_TARGET_DIR="${CARGO_TARGET_DIR_EFFECTIVE}"     cargo build --release --bin "${BIN_NAME}" ${BUILD_JOBS:+--jobs "${BUILD_JOBS}"}
)

BIN_PATH="$(intra_bin_path_release "${CARGO_TARGET_DIR_EFFECTIVE}" "${BIN_NAME}")"
intra_atomic_install "${BIN_PATH}" "${TARGET_DIR}/${BIN_NAME}"

for script in start_hedge_lazy_taker_eval.sh stop_hedge_lazy_taker_eval.sh; do
  rsync -a "${ROOT_DIR}/intra_scripts/${script}" "${TARGET_DIR}/intra_scripts/"
  chmod +x "${TARGET_DIR}/intra_scripts/${script}"
done

echo "[INFO] deployed: ${TARGET_DIR}/${BIN_NAME}"
echo "[INFO] start: cd ${TARGET_DIR} && ./intra_scripts/start_hedge_lazy_taker_eval.sh"
echo "[INFO] stop:  cd ${TARGET_DIR} && ./intra_scripts/stop_hedge_lazy_taker_eval.sh"
