#!/usr/bin/env bash

mm_normalize_exchange() {
  local exchange="${1,,}"
  if [[ "$exchange" == "okx" ]]; then
    exchange="okex"
  fi
  echo "$exchange"
}

mm_sanitize_tag() {
  printf '%s' "${1,,}" | sed -E 's/[^a-z0-9]+/_/g; s/^_+//; s/_+$//'
}

mm_short_exchange() {
  case "$(mm_normalize_exchange "$1")" in
    binance) echo "bn" ;;
    okex) echo "ok" ;;
    bybit) echo "bb" ;;
    bitget) echo "bg" ;;
    gate) echo "gt" ;;
    *)
      mm_normalize_exchange "$1" | sed -E 's/[^a-z0-9]+//g' | cut -c1-2
      ;;
  esac
}

mm_legacy_dir_tag() {
  mm_sanitize_tag "$1"
}

mm_parse_deploy_dir() {
  local dir_name="${1,,}"
  local exchange=""
  local env_tag=""

  if [[ "$dir_name" =~ ^([a-z0-9]+)[-_]mm([_-]([a-z0-9][a-z0-9_-]*))?$ ]]; then
    exchange="$(mm_normalize_exchange "${BASH_REMATCH[1]}")"
    env_tag="${BASH_REMATCH[3]:-mm}"
    env_tag="$(mm_sanitize_tag "$env_tag")"
    printf '%s %s\n' "$exchange" "$env_tag"
    return 0
  fi

  return 1
}

mm_default_proc_name() {
  local role="$1"
  local dir_name="$2"
  local exchange=""
  local env_tag=""

  if ! read -r exchange env_tag < <(mm_parse_deploy_dir "$dir_name"); then
    return 1
  fi

  printf 'mm_%s_%s_%s\n' "$role" "$exchange" "$env_tag"
}
