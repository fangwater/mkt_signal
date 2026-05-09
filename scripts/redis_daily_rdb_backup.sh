#!/usr/bin/env bash
set -euo pipefail

REDIS_CLI="${REDIS_CLI:-/usr/bin/redis-cli}"
REDIS_HOST="${REDIS_HOST:-127.0.0.1}"
REDIS_PORT="${REDIS_PORT:-6379}"
BACKUP_DIR="${REDIS_RDB_BACKUP_DIR:-$HOME/redis_backups/rdb}"
KEEP_DAYS="${REDIS_RDB_BACKUP_KEEP_DAYS:-30}"

today="$(date -u +%F)"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
backup_file="${BACKUP_DIR}/dump-${today}.rdb"
tmp_file="${BACKUP_DIR}/.dump-${today}.${stamp}.tmp"
lock_file="${BACKUP_DIR}/.redis_daily_rdb_backup.lock"

mkdir -p "$BACKUP_DIR"

exec 9>"$lock_file"
if ! flock -n 9; then
    echo "$(date -u +%FT%TZ) another redis backup is already running"
    exit 0
fi

if [[ -s "$backup_file" ]]; then
    echo "$(date -u +%FT%TZ) daily backup already exists: $backup_file"
    exit 0
fi

cleanup_tmp() {
    rm -f "$tmp_file"
}
trap cleanup_tmp EXIT

"$REDIS_CLI" -h "$REDIS_HOST" -p "$REDIS_PORT" --rdb "$tmp_file"
if [[ ! -s "$tmp_file" ]]; then
    echo "$(date -u +%FT%TZ) redis rdb backup is empty: $tmp_file" >&2
    exit 1
fi

mv -f "$tmp_file" "$backup_file"
trap - EXIT

echo "$(date -u +%FT%TZ) wrote redis rdb backup: $backup_file"

if [[ "$KEEP_DAYS" != "0" ]]; then
    mapfile -t expired_files < <(
        find "$BACKUP_DIR" -maxdepth 1 -type f -name 'dump-????-??-??.rdb' \
            | sort -r \
            | tail -n "+$((KEEP_DAYS + 1))"
    )

    for file in "${expired_files[@]}"; do
        rm -f "$file"
        echo "$(date -u +%FT%TZ) removed expired redis rdb backup: $file"
    done
fi
