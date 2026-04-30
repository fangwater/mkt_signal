#!/usr/bin/env bash
set -euo pipefail

# Shared local helpers for cross (cross-exchange futures/futures arb) deploy scripts.
# - Resolve cargo target dir
# - Build release binary path
# - Atomic binary install to avoid "Text file busy" and partial writes
# - Cross-only venue validation: futures/swap/perp only, reject margin/spot,
#   open != hedge

cross_effective_cargo_target_dir() {
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

cross_bin_path_release() {
  local cargo_target_dir="$1"
  local bin_name="$2"
  echo "${cargo_target_dir%/}/release/$bin_name"
}

cross_atomic_install() {
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

cross_normalize_venue() {
  local v="${1,,}"
  case "$v" in
    okx-*) v="okex-${v#okx-}" ;;
  esac
  echo "$v"
}

# 校验单个 cross venue：必须是 futures / swap / perp / perpetual
# 出错时不 exit，由调用方根据退出码或空输出判断（避免在 $(...) 中静默失败）
cross_ensure_venue() {
  local v
  v="$(cross_normalize_venue "$1")"
  if [[ -z "$v" || ! "$v" =~ ^[a-z0-9]+-(futures|swap|perp|perpetual)$ ]]; then
    echo "[ERROR] 非法 cross venue: $1（必须是 -futures/-swap/-perp/-perpetual，禁止 margin/spot）" >&2
    return 1
  fi
  echo "$v"
}

# 校验一对 cross venue：两边都合法且不相同。出错时返回非零并打印 stderr。
cross_ensure_venue_pair() {
  local open hedge
  if ! open="$(cross_ensure_venue "$1")"; then
    return 1
  fi
  if ! hedge="$(cross_ensure_venue "$2")"; then
    return 1
  fi
  local open_ex="${open%%-*}"
  local hedge_ex="${hedge%%-*}"
  if [[ "$open_ex" == "$hedge_ex" ]]; then
    echo "[ERROR] cross 必须跨所：open=$open hedge=$hedge（同一 exchange）" >&2
    return 1
  fi
  echo "$open $hedge"
}

# 由 env_name(<open>-<hedge>-cross-<tag>) 反推 (open_ex, hedge_ex)
cross_infer_pair_from_name() {
  local name="${1,,}"
  local open_ex=""
  local hedge_ex=""

  if [[ "$name" =~ ^([a-z0-9]+)[-_]([a-z0-9]+)[-_]cross([_-].*)?$ ]]; then
    open_ex="${BASH_REMATCH[1]}"
    hedge_ex="${BASH_REMATCH[2]}"
  fi

  if [[ "$open_ex" == "okx" ]]; then
    open_ex="okex"
  fi
  if [[ "$hedge_ex" == "okx" ]]; then
    hedge_ex="okex"
  fi

  if [[ -n "$open_ex" && -n "$hedge_ex" ]]; then
    echo "${open_ex},${hedge_ex}"
  fi
}
