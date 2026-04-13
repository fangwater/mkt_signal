#!/usr/bin/env bash
set -euo pipefail

# Shared local helpers for xarb deploy scripts.
# - Resolve cargo target dir
# - Build release binary path
# - Atomic binary install to avoid "Text file busy" and partial writes

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
