#!/usr/bin/env bash

execution_backend_parse() {
  local raw="${1:-}"
  raw="$(printf '%s' "$raw" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]')"
  case "$raw" in
    ''|native|exchange|direct) printf '%s\n' native ;;
    ltp|rapidx|liquidity|liquiditytech) printf '%s\n' ltp ;;
    *) echo "[ERROR] invalid execution backend: $1 (expected native or ltp)" >&2; return 1 ;;
  esac
}

execution_backend_for_exchange() {
  local exchange="${1,,}"
  local default_backend="${TRADE_ENGINE_EXEC_BACKEND:-}"
  local mapping="${TRADE_ENGINE_EXEC_BACKEND_MAP:-}"
  local parsed_default wildcard='' specific='' entry key value parsed
  local wildcard_set=0 specific_set=0
  local -a entries=()

  if [[ "$mapping" == *$'\n'* || "$mapping" == *$'\r'* ]]; then
    echo "[ERROR] execution backend map must not contain newlines" >&2
    return 1
  fi
  parsed_default="$(execution_backend_parse "$default_backend")" || return 1
  IFS=',' read -r -a entries <<< "$mapping"
  for entry in "${entries[@]}"; do
    entry="$(printf '%s' "$entry" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//')"
    [[ -z "$entry" ]] && continue
    if [[ "$entry" != *=* ]]; then
      echo "[ERROR] invalid execution backend map entry: $entry (expected exchange=backend)" >&2
      return 1
    fi
    key="$(printf '%s' "${entry%%=*}" | sed 's/^[[:space:]]*//; s/[[:space:]]*$//' | tr '[:upper:]' '[:lower:]')"
    value="${entry#*=}"
    parsed="$(execution_backend_parse "$value")" || return 1
    case "$key" in
      '*')
        ((wildcard_set == 0)) || { echo "[ERROR] duplicate wildcard execution backend mapping" >&2; return 1; }
        wildcard="$parsed"
        wildcard_set=1
        ;;
      binance|okex|bybit|bitget|gate|hyperliquid)
        if [[ "$key" == "$exchange" ]]; then
          ((specific_set == 0)) || { echo "[ERROR] duplicate $exchange execution backend mapping" >&2; return 1; }
          specific="$parsed"
          specific_set=1
        fi
        ;;
      *) echo "[ERROR] unknown exchange in execution backend map: $key" >&2; return 1 ;;
    esac
  done

  if ((specific_set)); then
    printf '%s\n' "$specific"
  elif ((wildcard_set)); then
    printf '%s\n' "$wildcard"
  else
    printf '%s\n' "$parsed_default"
  fi
}
