#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR="$(cd "${BASE_DIR}/.." && pwd)"
VENUE_DIR_REGEX='^([a-z0-9]+-(futures|margin|both)|(binance|bitget)-coin-futures)$'

usage() {
  cat <<'USAGE'
Usage:
  start_spread_pbs.sh

Behavior:
  - 必须在 venue 部署目录下执行（如 ~/spread_pbs/okex-futures 或 ~/spread_pbs/gate-both）。
  - 由当前目录名推断 venue，并查表得到默认 CPU 核（0-9）。
  - 若 env.sh 设置 SPREAD_PBS_CORE，则优先使用该覆盖值。
  - hyperliquid-margin / hyperliquid-futures / hyperliquid-both 没有默认 CPU 核；
    必须在当前部署目录的 env.sh 中显式设置 SPREAD_PBS_CORE=<core>。
    Hyperliquid 使用单进程，stream 策略由 spread_pbs 内部决定。
  - binance-futures 可通过 SPREAD_PBS_BINANCE_FUTURES_ROLE=split|market|bookticker 选择进程角色。
    角色只选择数据流；所有角色复用统一的双路 WS 和错峰重连机制。
  - <exchange>-both 会在一个 spread_pbs 进程内同时启动 margin/futures 两套 publisher。
    Bybit-both 默认拆成 market/bookticker 两个进程，避免 JSON market 流和 BBO 流互相抢 CPU。
    启动 both 前需要先停止同 exchange 的单独 margin/futures spread_pbs 进程。
  - 启动方式：taskset -c <core> + pmdaemon，进程名 spp_<ex>_<market>。
USAGE
}

if [[ "${1:-}" =~ ^(-h|--help)$ ]]; then
  usage
  exit 0
fi
if [[ $# -gt 0 ]]; then
  echo "[ERROR] 不支持参数: $*" >&2
  usage >&2
  exit 1
fi

# 默认 core 映射（必须与 deploy 脚本里的字母序一致）。
# Per-host 覆盖优先使用 env.sh 中的 SPREAD_PBS_CORE。
core_for_venue() {
  case "${1,,}" in
    binance-margin)   echo 0 ;;
    binance-futures)  echo 1 ;;
    binance-coin-futures) echo 10 ;;
    binance-both)     echo 0 ;;
    bitget-margin)    echo 2 ;;
    bitget-futures)   echo 3 ;;
    bitget-coin-futures) echo 11 ;;
    bitget-both)      echo 2 ;;
    bybit-margin)     echo 4 ;;
    bybit-futures)    echo 5 ;;
    bybit-both)       echo 4 ;;
    gate-margin)      echo 6 ;;
    gate-futures)     echo 7 ;;
    gate-both)        echo 6 ;;
    okex-margin)      echo 8 ;;
    okex-futures)     echo 9 ;;
    okex-both)        echo 8 ;;
    *) return 1 ;;
  esac
}

short_exchange() {
  case "${1,,}" in
    binance) echo "bn" ;;
    okex)    echo "ok" ;;
    bybit)   echo "bb" ;;
    bitget)  echo "bg" ;;
    gate)    echo "gt" ;;
    *)       echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
  esac
}
short_market() {
  case "${1,,}" in
    futures) echo "fu" ;;
    margin)  echo "mg" ;;
    both)    echo "bo" ;;
    *)       echo "${1,,}" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2 ;;
  esac
}
venue_short_tag() {
  local venue="${1,,}"
  if [[ "$venue" =~ ^([a-z0-9]+)-([a-z0-9]+)$ ]]; then
    echo "$(short_exchange "${BASH_REMATCH[1]}")_$(short_market "${BASH_REMATCH[2]}")"
    return 0
  fi
  echo "$venue" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

venue="$(basename "${BASE_DIR}" | tr '[:upper:]' '[:lower:]')"
if [[ ! "$venue" =~ $VENUE_DIR_REGEX ]]; then
  echo "[ERROR] 当前目录无法推断 venue: ${BASE_DIR}" >&2
  exit 1
fi
# 单一 env.sh 来源：每个 venue 部署目录下放一份 (与 fr/intra 一致)。
# 提前 source 以便 SPREAD_PBS_CORE 等 per-host override 在 core 解析前可见；
# okex-* 的 SBE handshake 也依赖 OKX_API_KEY/SECRET/PASSPHRASE。
if [[ -f "$BASE_DIR/env.sh" ]]; then
  # shellcheck source=/dev/null
  set -a; source "$BASE_DIR/env.sh"; set +a
fi

binance_spot_transport="${SPREAD_PBS_BINANCE_SPOT_TRANSPORT:-ws_sbe}"
binance_futures_role=""
case "${binance_spot_transport,,}" in
  ws_sbe|ws-sbe)
    ;;
  fix_sbe|fix-sbe)
    if [[ "$venue" != "binance-margin" && "$venue" != "binance-both" ]]; then
      echo "[ERROR] SPREAD_PBS_BINANCE_SPOT_TRANSPORT=fix_sbe 仅适用于 binance-margin/binance-both" >&2
      exit 1
    fi
    : "${BINANCE_FIX_MD_API_KEY:=${BINANCE_ED25519_API_KEY:-}}"
    : "${BINANCE_FIX_MD_PRIVATE_KEY_PATH:=${BINANCE_ED25519_PRIVATE_KEY_PATH:-}}"
    : "${BINANCE_FIX_MD_API_KEY:?fix_sbe requires BINANCE_FIX_MD_API_KEY or BINANCE_ED25519_API_KEY}"
    : "${BINANCE_FIX_MD_PRIVATE_KEY_PATH:?fix_sbe requires BINANCE_FIX_MD_PRIVATE_KEY_PATH or BINANCE_ED25519_PRIVATE_KEY_PATH}"
    if [[ ! -r "$BINANCE_FIX_MD_PRIVATE_KEY_PATH" ]]; then
      echo "[ERROR] Binance FIX MD private key is not readable: $BINANCE_FIX_MD_PRIVATE_KEY_PATH" >&2
      exit 1
    fi
    ;;
  *)
    echo "[ERROR] invalid SPREAD_PBS_BINANCE_SPOT_TRANSPORT=$binance_spot_transport; expected ws_sbe or fix_sbe" >&2
    exit 1
    ;;
esac

if [[ "$venue" == "binance-futures" ]]; then
  binance_futures_role="${SPREAD_PBS_BINANCE_FUTURES_ROLE:-split}"
  binance_futures_role="${binance_futures_role,,}"
  case "$binance_futures_role" in
    split)
      : "${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE:?SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE is required for binance-futures split spread_pbs}"
      : "${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE:?SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE is required for binance-futures split spread_pbs}"
      ;;
    market)
      SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE="${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE:-${SPREAD_PBS_CORE:-$(core_for_venue "$venue")}}"
      SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE="${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE:-$SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE}"
      ;;
    bookticker)
      SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE="${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE:-${SPREAD_PBS_CORE:-$(core_for_venue "$venue")}}"
      SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE="${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE:-$SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE}"
      ;;
    *)
      echo "[ERROR] invalid SPREAD_PBS_BINANCE_FUTURES_ROLE=$binance_futures_role; expected split, market, or bookticker" >&2
      exit 1
      ;;
  esac
  if [[ "$binance_futures_role" != "bookticker" && ! "$SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE 必须为单个整数 (got: $SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE)" >&2
    exit 1
  fi
  if [[ "$binance_futures_role" != "market" && ! "$SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE 必须为单个整数 (got: $SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE)" >&2
    exit 1
  fi
  case "${SPREAD_PBS_BINANCE_FUTURES_MM_WS_MODE:-}" in
    on|ON|On|1|true|TRUE|True|yes|YES|Yes)
      : "${SPREAD_PBS_BINANCE_FUTURES_MM_WS_LOCAL_IP:?SPREAD_PBS_BINANCE_FUTURES_MM_WS_LOCAL_IP is required when SPREAD_PBS_BINANCE_FUTURES_MM_WS_MODE is on}"
      ;;
  esac
  if [[ "$binance_futures_role" == "bookticker" ]]; then
    CORE="$SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE"
  else
    CORE="$SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE"
  fi
elif [[ "$venue" == "bybit-both" ]]; then
  SPREAD_PBS_BYBIT_MARKET_CORE="${SPREAD_PBS_BYBIT_MARKET_CORE:-8}"
  SPREAD_PBS_BYBIT_BOOKTICKER_CORE="${SPREAD_PBS_BYBIT_BOOKTICKER_CORE:-9}"
  if [[ ! "$SPREAD_PBS_BYBIT_MARKET_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] SPREAD_PBS_BYBIT_MARKET_CORE 必须为单个整数 (got: $SPREAD_PBS_BYBIT_MARKET_CORE)" >&2
    exit 1
  fi
  if [[ ! "$SPREAD_PBS_BYBIT_BOOKTICKER_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] SPREAD_PBS_BYBIT_BOOKTICKER_CORE 必须为单个整数 (got: $SPREAD_PBS_BYBIT_BOOKTICKER_CORE)" >&2
    exit 1
  fi
  CORE="$SPREAD_PBS_BYBIT_MARKET_CORE"
elif [[ "$venue" == hyperliquid-* ]]; then
  if [[ -z "${SPREAD_PBS_CORE:-}" ]]; then
    echo "[ERROR] ${venue} 不提供默认 core；必须显式设置 SPREAD_PBS_CORE。" >&2
    echo "[HINT] 在 ${BASE_DIR}/env.sh 中添加: export SPREAD_PBS_CORE=<core>" >&2
    exit 1
  fi
  if [[ ! "$SPREAD_PBS_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] SPREAD_PBS_CORE 必须为单个整数 (got: $SPREAD_PBS_CORE)" >&2
    exit 1
  fi
  CORE="$SPREAD_PBS_CORE"
elif [[ -n "${SPREAD_PBS_CORE:-}" ]]; then
  if [[ ! "$SPREAD_PBS_CORE" =~ ^[0-9]+$ ]]; then
    echo "[ERROR] SPREAD_PBS_CORE 必须为单个整数 (got: $SPREAD_PBS_CORE)" >&2
    exit 1
  fi
  CORE="$SPREAD_PBS_CORE"
elif ! CORE=$(core_for_venue "$venue"); then
  echo "[ERROR] venue 未配置 core 绑定: $venue" >&2
  exit 1
fi

PMDAEMON_BIN="${PMDAEMON_BIN:-pmdaemon}"
PMDAEMON=("$PMDAEMON_BIN")
if [[ "$PMDAEMON_BIN" != */* ]] && ! command -v "$PMDAEMON_BIN" >/dev/null 2>&1; then
  echo "[ERROR] pmdaemon not found: $PMDAEMON_BIN" >&2
  echo "[HINT] install with: cargo install pmdaemon" >&2
  exit 1
fi
if ! command -v taskset >/dev/null 2>&1; then
  echo "[ERROR] taskset not found（util-linux 包）" >&2
  exit 1
fi

BIN_CANDIDATES=(
  "${BASE_DIR}/spread_pbs"
  "${SCRIPT_DIR}/../spread_pbs"
  "${ROOT_DIR}/target/release/spread_pbs"
)
BIN_PATH=""
for cand in "${BIN_CANDIDATES[@]}"; do
  if [[ -x "$cand" ]]; then
    BIN_PATH="$cand"
    break
  fi
done
if [[ -z "$BIN_PATH" ]]; then
  echo "[ERROR] spread_pbs binary not found，先 cargo build --release --bin spread_pbs" >&2
  exit 1
fi

name="spp_$(venue_short_tag "$venue")"
market_name="$name"
bookticker_name="$name"
if [[ "$venue" == "binance-futures" ]]; then
  market_name="${name}_market"
  bookticker_name="${name}_bookticker"
elif [[ "$venue" == "bybit-both" ]]; then
  market_name="${name}_market"
  bookticker_name="${name}_bookticker"
fi
rust_log="${RUST_LOG:-info}"
KILL_WAIT_SECS="${KILL_WAIT_SECS:-6}"
cfg_file="$(mktemp)"
trap 'rm -f "$cfg_file" >/dev/null 2>&1 || true' EXIT

find_running_pids_for_venue() {
  local target_venue="${1,,}"
  local venue_arg="--venue ${target_venue}"
  local pids=()
  while IFS= read -r pid; do
    if [[ -n "$pid" && "$pid" != "$$" && "$pid" != "$PPID" ]]; then
      pids+=("$pid")
    fi
  done < <(
    ps -eo pid=,comm=,args= | awk -v venue_arg="$venue_arg" '
      $2 == "spread_pbs" &&
      index($0, venue_arg) > 0 &&
      index($0, "awk -v ") == 0 &&
      index($0, "start_spread_pbs.sh") == 0 &&
      index($0, "stop_spread_pbs.sh") == 0 {
        print $1
      }
    '
  )
  if [[ ${#pids[@]} -gt 0 ]]; then
    printf '%s\n' "${pids[@]}"
  fi
}

find_running_pids() {
  find_running_pids_for_venue "$venue"
}

check_conflicting_spread_pbs_processes() {
  local exchange=""
  local conflict_venues=()
  if [[ "$venue" =~ ^([a-z0-9]+)-both$ ]]; then
    exchange="${BASH_REMATCH[1]}"
    conflict_venues=("${exchange}-margin" "${exchange}-futures")
  elif [[ "$venue" =~ ^([a-z0-9]+)-(margin|futures)$ ]]; then
    exchange="${BASH_REMATCH[1]}"
    conflict_venues=("${exchange}-both")
  else
    return 0
  fi

  local found=()
  local target pid
  for target in "${conflict_venues[@]}"; do
    while IFS= read -r pid; do
      [[ -n "$pid" ]] && found+=("${target}:${pid}")
    done < <(find_running_pids_for_venue "$target" || true)
  done

  if [[ ${#found[@]} -gt 0 ]]; then
    echo "[ERROR] Conflicting spread_pbs process(es) already running: ${found[*]}" >&2
    echo "        Stop the old single/both deployment first; they publish the same IPC services." >&2
    exit 1
  fi
}

json_escape() { printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'; }

json_name="$(json_escape "$name")"
json_market_name="$(json_escape "$market_name")"
json_bookticker_name="$(json_escape "$bookticker_name")"
json_bin="$(json_escape "$(command -v taskset)")"
json_base="$(json_escape "$BASE_DIR")"
json_venue="$(json_escape "$venue")"
json_rust_log="$(json_escape "$rust_log")"
json_inner_bin="$(json_escape "$BIN_PATH")"
binance_sbe_env_line=""
if [[ "$venue" == "binance-margin" || "$venue" == "binance-both" ]]; then
  BINANCE_SBE_API_KEY_HARDCODED="nk1AebIPBgDpTNDl186QeD2imHSuyPm4t2yzIGEul1SmmU0QXFroGVEHI18pVAO4"
  json_binance_sbe_api_key="$(json_escape "$BINANCE_SBE_API_KEY_HARDCODED")"
  binance_sbe_env_line=",
        \"BINANCE_SBE_API_KEY\": \"${json_binance_sbe_api_key}\""
fi

binance_fix_md_env_line=""
if [[ "$venue" == "binance-margin" || "$venue" == "binance-both" ]]; then
  json_binance_spot_transport="$(json_escape "$binance_spot_transport")"
  binance_fix_md_env_line=",
        \"SPREAD_PBS_BINANCE_SPOT_TRANSPORT\": \"${json_binance_spot_transport}\""
  if [[ "${binance_spot_transport,,}" == "fix_sbe" || "${binance_spot_transport,,}" == "fix-sbe" ]]; then
    json_binance_fix_md_api_key="$(json_escape "$BINANCE_FIX_MD_API_KEY")"
    json_binance_fix_md_private_key_path="$(json_escape "$BINANCE_FIX_MD_PRIVATE_KEY_PATH")"
    binance_fix_md_env_line="${binance_fix_md_env_line},
        \"BINANCE_FIX_MD_API_KEY\": \"${json_binance_fix_md_api_key}\",
        \"BINANCE_FIX_MD_PRIVATE_KEY_PATH\": \"${json_binance_fix_md_private_key_path}\""
    if [[ -n "${BINANCE_FIX_MD_URL:-}" ]]; then
      json_binance_fix_md_url="$(json_escape "$BINANCE_FIX_MD_URL")"
      binance_fix_md_env_line="${binance_fix_md_env_line},
        \"BINANCE_FIX_MD_URL\": \"${json_binance_fix_md_url}\""
    fi
    if [[ -n "${BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE:-}" ]]; then
      json_binance_fix_md_passphrase="$(json_escape "$BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE")"
      binance_fix_md_env_line="${binance_fix_md_env_line},
        \"BINANCE_ED25519_PRIVATE_KEY_PASSPHRASE\": \"${json_binance_fix_md_passphrase}\""
    fi
  fi
fi

binance_futures_mm_ws_env_line=""
if [[ -n "${SPREAD_PBS_BINANCE_FUTURES_MM_WS_MODE:-}" ]]; then
  json_binance_futures_mm_ws_mode="$(json_escape "$SPREAD_PBS_BINANCE_FUTURES_MM_WS_MODE")"
  binance_futures_mm_ws_env_line=",
        \"SPREAD_PBS_BINANCE_FUTURES_MM_WS_MODE\": \"${json_binance_futures_mm_ws_mode}\""
fi
if [[ -n "${SPREAD_PBS_BINANCE_FUTURES_MM_WS_LOCAL_IP:-}" ]]; then
  json_binance_futures_mm_ws_local_ip="$(json_escape "$SPREAD_PBS_BINANCE_FUTURES_MM_WS_LOCAL_IP")"
  binance_futures_mm_ws_env_line="${binance_futures_mm_ws_env_line},
        \"SPREAD_PBS_BINANCE_FUTURES_MM_WS_LOCAL_IP\": \"${json_binance_futures_mm_ws_local_ip}\""
fi

spread_pbs_symbols_env_line=""
if [[ -n "${SPREAD_PBS_SYMBOLS:-}" ]]; then
  json_spread_pbs_symbols="$(json_escape "$SPREAD_PBS_SYMBOLS")"
  spread_pbs_symbols_env_line=",
        \"SPREAD_PBS_SYMBOLS\": \"${json_spread_pbs_symbols}\""
fi

binance_futures_split_env_line=""
if [[ "$venue" == "binance-futures" ]]; then
  json_bf_market_core="$(json_escape "$SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE")"
  json_bf_bookticker_core="$(json_escape "$SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE")"
  binance_futures_split_env_line=",
        \"SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE\": \"${json_bf_market_core}\",
        \"SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE\": \"${json_bf_bookticker_core}\""
fi

bybit_split_env_line=""
if [[ "$venue" == "bybit-both" ]]; then
  json_bybit_market_core="$(json_escape "$SPREAD_PBS_BYBIT_MARKET_CORE")"
  json_bybit_bookticker_core="$(json_escape "$SPREAD_PBS_BYBIT_BOOKTICKER_CORE")"
  bybit_split_env_line=",
        \"SPREAD_PBS_BYBIT_MARKET_CORE\": \"${json_bybit_market_core}\",
        \"SPREAD_PBS_BYBIT_BOOKTICKER_CORE\": \"${json_bybit_bookticker_core}\""
fi

okex_sbe_env_line=""
if [[ "$venue" == "okex-margin" || "$venue" == "okex-futures" || "$venue" == "okex-both" ]]; then
  for v in OKX_API_KEY OKX_API_SECRET OKX_PASSPHRASE; do
    if [[ -z "${!v:-}" ]]; then
      echo "[ERROR] ${v} 未设置；OKEx SBE handshake 需要这三个变量。" >&2
      echo "       请在 ${BASE_DIR}/env.sh 里 export OKX_API_KEY / OKX_API_SECRET / OKX_PASSPHRASE，" >&2
      echo "       然后重新执行 start_spread_pbs.sh。" >&2
      exit 1
    fi
  done
  json_okx_api_key="$(json_escape "$OKX_API_KEY")"
  json_okx_api_secret="$(json_escape "$OKX_API_SECRET")"
  json_okx_passphrase="$(json_escape "$OKX_PASSPHRASE")"
  okex_sbe_env_line=",
        \"OKX_API_KEY\": \"${json_okx_api_key}\",
        \"OKX_API_SECRET\": \"${json_okx_api_secret}\",
        \"OKX_PASSPHRASE\": \"${json_okx_passphrase}\""
fi

# iceoryx2 默认按 CWD 查找 ./config/iceoryx2.toml；没有就从 root 兜底
if [[ ! -f "$BASE_DIR/config/iceoryx2.toml" && -f "$ROOT_DIR/config/iceoryx2.toml" ]]; then
  mkdir -p "$BASE_DIR/config"
  cp "$ROOT_DIR/config/iceoryx2.toml" "$BASE_DIR/config/iceoryx2.toml"
fi

# pmdaemon args = ["-c", "<core>", "<bin>", "--venue", "<v>", "--core", "<core>"]
common_env="\"RUST_LOG\": \"${json_rust_log}\"${binance_sbe_env_line}${binance_fix_md_env_line}${binance_futures_mm_ws_env_line}${spread_pbs_symbols_env_line}${binance_futures_split_env_line}${bybit_split_env_line}${okex_sbe_env_line}"
if [[ "$venue" == "binance-futures" ]]; then
  if [[ "$binance_futures_role" == "market" ]]; then
    cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_market_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE}",
        "--binance-futures-role", "market"
      ],
      "cwd": "${json_base}",
      "env": {
        ${common_env}
      }
    }
  ]
}
JSON
  elif [[ "$binance_futures_role" == "bookticker" ]]; then
    cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_bookticker_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE}",
        "--binance-futures-role", "bookticker"
      ],
      "cwd": "${json_base}",
      "env": {
        ${common_env}
      }
    }
  ]
}
JSON
  else
    cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_market_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE}",
        "--binance-futures-role", "market"
      ],
      "cwd": "${json_base}",
      "env": {
        ${common_env}
      }
    },
    {
      "name": "${json_bookticker_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE}",
        "--binance-futures-role", "bookticker"
      ],
      "cwd": "${json_base}",
      "env": {
        ${common_env}
      }
    }
  ]
}
JSON
  fi
elif [[ "$venue" == "bybit-both" ]]; then
  cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_market_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${SPREAD_PBS_BYBIT_MARKET_CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${SPREAD_PBS_BYBIT_MARKET_CORE}",
        "--bybit-role", "market"
      ],
      "cwd": "${json_base}",
      "env": {
        ${common_env}
      }
    },
    {
      "name": "${json_bookticker_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${SPREAD_PBS_BYBIT_BOOKTICKER_CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${SPREAD_PBS_BYBIT_BOOKTICKER_CORE}",
        "--bybit-role", "bookticker"
      ],
      "cwd": "${json_base}",
      "env": {
        ${common_env}
      }
    }
  ]
}
JSON
else
  cat >"$cfg_file" <<JSON
{
  "apps": [
    {
      "name": "${json_name}",
      "script": "${json_bin}",
      "args": [
        "-c", "${CORE}",
        "${json_inner_bin}",
        "--venue", "${json_venue}",
        "--core", "${CORE}"
      ],
      "cwd": "${json_base}",
      "env": {
        ${common_env}
      }
    }
  ]
}
JSON
fi

echo "[INFO] Restarting ${name} (venue=${venue}, core=${CORE})"
if [[ "$venue" == "binance-futures" ]]; then
  "${PMDAEMON[@]}" delete "$market_name" >/dev/null 2>&1 || true
  "${PMDAEMON[@]}" delete "$bookticker_name" >/dev/null 2>&1 || true
elif [[ "$venue" == "bybit-both" ]]; then
  "${PMDAEMON[@]}" delete "$market_name" >/dev/null 2>&1 || true
  "${PMDAEMON[@]}" delete "$bookticker_name" >/dev/null 2>&1 || true
  "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true
else
  "${PMDAEMON[@]}" delete "$name" >/dev/null 2>&1 || true
fi
check_conflicting_spread_pbs_processes

mapfile -t leaked_pids < <(find_running_pids || true)
if [[ ${#leaked_pids[@]} -gt 0 ]]; then
  echo "[WARN] Found leaked process(es): ${leaked_pids[*]}; SIGTERM"
  kill "${leaked_pids[@]}" >/dev/null 2>&1 || true
  deadline=$((SECONDS + KILL_WAIT_SECS))
  while [[ $SECONDS -lt $deadline ]]; do
    mapfile -t leaked_pids < <(find_running_pids || true)
    [[ ${#leaked_pids[@]} -eq 0 ]] && break
    sleep 1
  done
  if [[ ${#leaked_pids[@]} -gt 0 ]]; then
    echo "[WARN] SIGTERM timeout, SIGKILL: ${leaked_pids[*]}"
    kill -9 "${leaked_pids[@]}" >/dev/null 2>&1 || true
    sleep 1
  fi
fi

if [[ "$venue" != "binance-futures" || "$binance_futures_role" != "bookticker" ]]; then
  "${PMDAEMON[@]}" --config "$cfg_file" start --name "$market_name"
fi
if [[ "$venue" == "bybit-both" || ( "$venue" == "binance-futures" && "$binance_futures_role" != "market" ) ]]; then
  "${PMDAEMON[@]}" --config "$cfg_file" start --name "$bookticker_name"
fi

echo ""
if [[ "$venue" == "binance-futures" ]]; then
  if [[ "$binance_futures_role" != "bookticker" ]]; then
    echo "[INFO] Started: ${market_name} pinned to core ${SPREAD_PBS_BINANCE_FUTURES_MARKET_CORE}"
  fi
  if [[ "$binance_futures_role" != "market" ]]; then
    echo "[INFO] Started: ${bookticker_name} pinned to core ${SPREAD_PBS_BINANCE_FUTURES_BOOKTICKER_CORE}"
  fi
elif [[ "$venue" == "bybit-both" ]]; then
  echo "[INFO] Started: ${market_name} pinned to core ${SPREAD_PBS_BYBIT_MARKET_CORE}"
  echo "[INFO] Started: ${bookticker_name} pinned to core ${SPREAD_PBS_BYBIT_BOOKTICKER_CORE}"
else
  echo "[INFO] Started: ${name} pinned to core ${CORE}"
fi
echo "Venue:  ${venue}"
if [[ "$venue" == "binance-futures" ]]; then
  if [[ "$binance_futures_role" != "bookticker" ]]; then
    echo "Logs:   ${PMDAEMON[*]} logs ${market_name} --follow"
  fi
  if [[ "$binance_futures_role" != "market" ]]; then
    echo "Logs:   ${PMDAEMON[*]} logs ${bookticker_name} --follow"
  fi
elif [[ "$venue" == "bybit-both" ]]; then
  echo "Logs:   ${PMDAEMON[*]} logs ${market_name} --follow"
  echo "        ${PMDAEMON[*]} logs ${bookticker_name} --follow"
else
  echo "Logs:   ${PMDAEMON[*]} logs ${name} --follow"
fi
echo "Status: ${PMDAEMON[*]} list"
