#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

usage() {
  cat <<'USAGE'
Usage:
  start_dat_pbs.sh (--exchange <exchange> | <exchange> | <venue...>)

Examples:
  ./scripts/start_dat_pbs.sh --exchange binance
  ./scripts/start_dat_pbs.sh okex
  ./scripts/start_dat_pbs.sh binance-futures
  ./scripts/start_dat_pbs.sh binance-futures binance-margin

Notes:
  - Exchange expands to default venues:
      okex    -> okex-futures okex-margin
      binance -> binance-futures binance-margin
      bybit   -> bybit-futures bybit-margin
      bitget  -> bitget-futures bitget-margin
      gate    -> gate-futures gate-margin
  - Runs as systemd user services:
      dat_pbs@<venue>.service
  - Optional env files:
      config/dat_pbs.env
      config/dat_pbs-<venue>.env
USAGE
}

KNOWN_EXCHANGES=("okex" "binance" "bybit" "bitget" "gate")

is_known_exchange() {
  local v="${1,,}"
  for e in "${KNOWN_EXCHANGES[@]}"; do
    if [[ "$v" == "$e" ]]; then
      return 0
    fi
  done
  return 1
}

default_venues_for_exchange() {
  local exchange="${1,,}"
  case "$exchange" in
    okex) echo "okex-futures okex-margin" ;;
    binance) echo "binance-futures binance-margin" ;;
    bybit) echo "bybit-futures bybit-margin" ;;
    bitget) echo "bitget-futures bitget-margin" ;;
    gate) echo "gate-futures gate-margin" ;;
    *)
      echo ""
      return 1
      ;;
  esac
}

SYSTEMCTL=(systemctl --user)
UNIT_DIR="${SYSTEMD_USER_UNIT_DIR:-$HOME/.config/systemd/user}"

EXCHANGE=""
VENUES=()
POSITIONAL=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace)
      # Backward compatibility: namespace is no longer used for dat_pbs.
      if [[ -z "${2:-}" ]]; then
        echo "[ERROR] --namespace 需要一个值" >&2
        usage >&2
        exit 1
      fi
      echo "[WARN] --namespace is ignored for dat_pbs (using unit dat_pbs@<venue>.service)"
      shift 2
      ;;
    --exchange)
      EXCHANGE="${2:-}"
      if [[ -z "$EXCHANGE" ]]; then
        echo "[ERROR] --exchange 需要一个值" >&2
        usage >&2
        exit 1
      fi
      shift 2
      ;;
    --venue)
      v="${2:-}"
      if [[ -z "$v" ]]; then
        echo "[ERROR] --venue 需要一个值" >&2
        usage >&2
        exit 1
      fi
      VENUES+=("$v")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      POSITIONAL+=("$1")
      shift
      ;;
  esac
done

if [[ -z "$EXCHANGE" && ${#POSITIONAL[@]} -gt 0 ]]; then
  if is_known_exchange "${POSITIONAL[0]}"; then
    EXCHANGE="${POSITIONAL[0]}"
    POSITIONAL=("${POSITIONAL[@]:1}")
  fi
fi

if [[ ${#VENUES[@]} -eq 0 && ${#POSITIONAL[@]} -gt 0 ]]; then
  VENUES=("${POSITIONAL[@]}")
fi

if [[ -z "$EXCHANGE" && ${#VENUES[@]} -eq 0 ]]; then
  dir_name="$(basename "${BASE_DIR}")"
  if [[ "$dir_name" =~ okex|OKEX ]]; then EXCHANGE="okex"; fi
  if [[ "$dir_name" =~ binance|BINANCE ]]; then EXCHANGE="binance"; fi
  if [[ "$dir_name" =~ bybit|BYBIT ]]; then EXCHANGE="bybit"; fi
  if [[ "$dir_name" =~ bitget|BITGET ]]; then EXCHANGE="bitget"; fi
  if [[ "$dir_name" =~ gate|GATE ]]; then EXCHANGE="gate"; fi
fi

if [[ ${#VENUES[@]} -eq 0 && -n "$EXCHANGE" ]]; then
  read -r -a VENUES <<<"$(default_venues_for_exchange "$EXCHANGE")"
fi

if [[ ${#VENUES[@]} -eq 0 ]]; then
  echo "[ERROR] No exchange/venues provided and could not infer from deploy directory name" >&2
  usage >&2
  exit 1
fi

if ! command -v systemctl >/dev/null 2>&1; then
  echo "[ERROR] systemctl not found" >&2
  exit 1
fi

if ! "${SYSTEMCTL[@]}" show-environment >/dev/null 2>&1; then
  echo "[ERROR] systemd --user is not available for current session" >&2
  echo "[HINT] For server usage, run once: sudo loginctl enable-linger $(whoami)" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${SCRIPT_DIR}/dat_pbs"
  "${SCRIPT_DIR}/target/release/dat_pbs"
  "${SCRIPT_DIR}/../dat_pbs"
  "${SCRIPT_DIR}/../target/release/dat_pbs"
  "${BASE_DIR}/dat_pbs"
  "${BASE_DIR}/target/release/dat_pbs"
)

BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done

if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] dat_pbs binary not found. Build first with: cargo build --release --bin dat_pbs" >&2
  exit 1
fi

UNIT_TEMPLATE="dat_pbs@.service"
UNIT_PATH="${UNIT_DIR}/${UNIT_TEMPLATE}"
RUST_LOG_DEFAULT="${RUST_LOG:-info}"

install_or_update_unit_template() {
  mkdir -p "$UNIT_DIR"

  local tmp_path="${UNIT_PATH}.tmp"
  cat >"$tmp_path" <<EOF
[Unit]
Description=dat_pbs instance (%i)
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
WorkingDirectory=${BASE_DIR}
ExecStart=${BIN_PATH} --venue %i
Restart=always
RestartSec=2
Environment=RUST_LOG=${RUST_LOG_DEFAULT}
EnvironmentFile=-${BASE_DIR}/config/dat_pbs.env
EnvironmentFile=-${BASE_DIR}/config/dat_pbs-%i.env
KillSignal=SIGINT
TimeoutStopSec=15
LimitNOFILE=1048576

[Install]
WantedBy=default.target
EOF

  if [[ ! -f "$UNIT_PATH" ]] || ! cmp -s "$tmp_path" "$UNIT_PATH"; then
    mv "$tmp_path" "$UNIT_PATH"
    echo "[INFO] Updated unit template: $UNIT_PATH"
  else
    rm -f "$tmp_path"
  fi

  "${SYSTEMCTL[@]}" daemon-reload
}

unit_name_for_venue() {
  local venue="$1"
  if command -v systemd-escape >/dev/null 2>&1; then
    systemd-escape --template "$UNIT_TEMPLATE" "$venue"
  else
    echo "${UNIT_TEMPLATE/@.service/@${venue}.service}"
  fi
}

start_one() {
  local venue="$1"
  local unit_name
  unit_name="$(unit_name_for_venue "$venue")"

  echo "[INFO] Restarting ${unit_name}"
  "${SYSTEMCTL[@]}" restart "$unit_name"
}

install_or_update_unit_template

for venue in "${VENUES[@]}"; do
  start_one "$venue"
  sleep 1
done

echo ""
echo "[INFO] Started venues: ${VENUES[*]}"
echo "Template: ${UNIT_TEMPLATE}"
echo "Logs: journalctl --user -u dat_pbs@<venue>.service -f"
echo "Status: systemctl --user status dat_pbs@<venue>.service"
