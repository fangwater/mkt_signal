#!/usr/bin/env bash
set -euo pipefail

# Shared local helpers for intra (intra-exchange spot/futures arb) deploy scripts.
# - Resolve cargo target dir
# - Build release binary path
# - Atomic binary install to avoid "Text file busy" and partial writes

intra_effective_cargo_target_dir() {
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

intra_bin_path_release() {
  local cargo_target_dir="$1"
  local bin_name="$2"
  echo "${cargo_target_dir%/}/release/$bin_name"
}

intra_atomic_install() {
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

# 校验 exchange 名是否合法（同所期现只需要单 exchange）
intra_ensure_exchange() {
  local ex
  ex="${1,,}"
  case "$ex" in
    okx) ex="okex" ;;
  esac
  if [[ -z "$ex" || ! "$ex" =~ ^[a-z][a-z0-9]+$ ]]; then
    echo "[ERROR] 非法 exchange: $1" >&2
    exit 1
  fi
  case "$ex" in
    binance|okex|gate|bybit|bitget|hyperliquid)
      echo "$ex"
      ;;
    *)
      echo "[ERROR] intra 暂不支持 exchange: $ex" >&2
      exit 1
      ;;
  esac
}

# 由 exchange 推出同所期现的 (open_venue, hedge_venue)
intra_venues_for_exchange() {
  local ex="$1"
  case "$ex" in
    binance) echo "binance-margin binance-futures" ;;
    okex)    echo "okex-margin okex-futures" ;;
    gate)    echo "gate-margin gate-futures" ;;
    bybit)   echo "bybit-margin bybit-futures" ;;
    bitget)  echo "bitget-margin bitget-futures" ;;
    *)
      echo "[ERROR] 无法推断 intra venues for exchange=$ex" >&2
      exit 1
      ;;
  esac
}
