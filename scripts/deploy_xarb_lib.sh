#!/usr/bin/env bash
set -euo pipefail

# Shared helpers for xarb deploy scripts.
# - Optional remote mode via ssh (build/deploy runs on remote host).
# - Atomic binary install to avoid "Text file busy" and partial writes.

XARB_REMOTE_HOST=""
XARB_REMOTE_REPO=""
XARB_REMOTE_SYNC="0"
XARB_REMOTE_NICE="10"
XARB_REMOTE_IONICE="1"
XARB_REMOTE_CARGO_TARGET_DIR=""
XARB_REMOTE_TTY="0"
XARB_FORWARD_ARGS=()

xarb_preparse_remote_args() {
  XARB_REMOTE_HOST=""
  XARB_REMOTE_REPO=""
  XARB_REMOTE_SYNC="0"
  XARB_REMOTE_NICE="10"
  XARB_REMOTE_IONICE="1"
  XARB_REMOTE_CARGO_TARGET_DIR=""
  XARB_REMOTE_TTY="0"
  XARB_FORWARD_ARGS=()

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --remote|--remote-host)
        XARB_REMOTE_HOST="${2:-}"
        shift 2
        ;;
      --remote-repo)
        XARB_REMOTE_REPO="${2:-}"
        shift 2
        ;;
      --remote-sync)
        XARB_REMOTE_SYNC="1"
        shift
        ;;
      --remote-no-sync)
        XARB_REMOTE_SYNC="0"
        shift
        ;;
      --remote-nice)
        XARB_REMOTE_NICE="${2:-10}"
        shift 2
        ;;
      --remote-ionice)
        XARB_REMOTE_IONICE="1"
        shift
        ;;
      --remote-no-ionice)
        XARB_REMOTE_IONICE="0"
        shift
        ;;
      --remote-cargo-target-dir)
        XARB_REMOTE_CARGO_TARGET_DIR="${2:-}"
        shift 2
        ;;
      --remote-tty)
        XARB_REMOTE_TTY="1"
        shift
        ;;
      --remote-no-tty)
        XARB_REMOTE_TTY="0"
        shift
        ;;
      *)
        XARB_FORWARD_ARGS+=("$1")
        shift
        ;;
    esac
  done
}

xarb_remote_maybe_sync_repo() {
  local root_dir="$1"
  if [[ "${XARB_REMOTE_SYNC}" != "1" ]]; then
    return 0
  fi
  if [[ -z "${XARB_REMOTE_HOST}" ]]; then
    echo "[ERROR] --remote-sync requires --remote-host" >&2
    return 1
  fi

  local remote_repo="${XARB_REMOTE_REPO:-crypto_mkt/mkt_signal}"
  ssh "${XARB_REMOTE_HOST}" bash -s -- "$remote_repo" >/dev/null <<'EOF'
set -euo pipefail
mkdir -p "$1"
EOF

  # Sync only sources/config/scripts; exclude large/build/runtime dirs.
  rsync -az --delete \
    --exclude ".git/" \
    --exclude "target/" \
    --exclude "data/" \
    --exclude ".venv/" \
    --exclude "__pycache__/" \
    --exclude "out_fil/" \
    --exclude "*.parquet" \
    "${root_dir%/}/" \
    "${XARB_REMOTE_HOST}:${remote_repo%/}/"
}

xarb_remote_exec() {
  local script_rel="$1"
  shift

  if [[ -z "${XARB_REMOTE_HOST}" ]]; then
    echo "[ERROR] xarb_remote_exec requires XARB_REMOTE_HOST" >&2
    return 1
  fi

  local ssh_opts=()
  if [[ "${XARB_REMOTE_TTY}" == "1" ]]; then
    ssh_opts+=("-tt")
  fi

  ssh "${ssh_opts[@]}" "${XARB_REMOTE_HOST}" bash -s -- \
    "${XARB_REMOTE_REPO:-}" \
    "$script_rel" \
    "${XARB_REMOTE_CARGO_TARGET_DIR:-}" \
    "${XARB_REMOTE_NICE:-10}" \
    "${XARB_REMOTE_IONICE:-1}" \
    "$@" <<'EOF'
set -euo pipefail

remote_repo="${1:-}"
script_rel="$2"
cargo_target_dir="${3:-}"
nice_n="${4:-10}"
use_ionice="${5:-1}"
shift 5

if [[ -z "$remote_repo" ]]; then
  remote_repo="$HOME/crypto_mkt/mkt_signal"
fi

cd "$remote_repo"

if [[ -z "$cargo_target_dir" ]]; then
  cargo_target_dir="$HOME/.cache/mkt_signal/cargo_target_xarb"
fi
mkdir -p "$cargo_target_dir" >/dev/null 2>&1 || true
export CARGO_TARGET_DIR="$cargo_target_dir"
export XARB_REMOTE_RUN="1"

cmd=(bash "$script_rel" "$@")
if [[ "$use_ionice" == "1" ]] && command -v ionice >/dev/null 2>&1; then
  cmd=(ionice -c2 -n7 "${cmd[@]}")
fi
if [[ -n "$nice_n" && "$nice_n" != "0" ]] && command -v nice >/dev/null 2>&1; then
  cmd=(nice -n "$nice_n" "${cmd[@]}")
fi

exec "${cmd[@]}"
EOF
}

xarb_effective_cargo_target_dir() {
  local root_dir="$1"
  local override="${2:-}"
  if [[ -n "$override" ]]; then
    echo "$override"
    return 0
  fi
  if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
    echo "${CARGO_TARGET_DIR}"
    return 0
  fi
  echo "$root_dir/target"
}

xarb_bin_path_release() {
  local cargo_target_dir="$1"
  local bin_name="$2"
  echo "${cargo_target_dir%/}/release/$bin_name"
}

xarb_atomic_install() {
  local src="$1"
  local dst="$2"
  local tries="${3:-5}"

  local tmp="${dst}.new"
  cp "$src" "$tmp"
  chmod +x "$tmp"

  local ok="0"
  local i="0"
  while [[ "$i" -lt "$tries" ]]; do
    if mv -f "$tmp" "$dst" 2>/dev/null; then
      ok="1"
      break
    fi
    sleep 0.2
    i="$((i + 1))"
  done

  if [[ "$ok" != "1" ]]; then
    echo "[WARN] 二进制更新失败（可能 Text file busy）：$dst" >&2
    echo "[WARN] 请稍后重试二进制更新或先停止进程后再部署" >&2
    return 2
  fi
}
