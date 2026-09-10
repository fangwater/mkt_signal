#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  run_raw_release.sh \
    --rocksdb-dir PATH --calendar PATH --output-parent PATH \
    --start-date YYYY-MM-DD --end-date YYYY-MM-DD [--workers N] [--ric RIC]

Builds one clean RAW L1/trade release in a fresh staging directory. It publishes
only after every RIC/session pair has both parquet outputs and manifests. The
old RAW roots are moved to a legacy archive only during final publication.
EOF
}

rocksdb_dir=
calendar=
output_parent=
start_date=
end_date=
workers=16
ric_filter=

while (($#)); do
    case "$1" in
        --rocksdb-dir) rocksdb_dir=$2; shift 2 ;;
        --calendar) calendar=$2; shift 2 ;;
        --output-parent) output_parent=$2; shift 2 ;;
        --start-date) start_date=$2; shift 2 ;;
        --end-date) end_date=$2; shift 2 ;;
        --workers) workers=$2; shift 2 ;;
        --ric) ric_filter=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done

for required in rocksdb_dir calendar output_parent start_date end_date; do
    if [[ -z ${!required} ]]; then
        echo "missing --${required//_/-}" >&2
        exit 2
    fi
done
[[ -d $rocksdb_dir ]] || { echo "missing RocksDB: $rocksdb_dir" >&2; exit 2; }
[[ -f $calendar ]] || { echo "missing calendar: $calendar" >&2; exit 2; }
[[ $workers =~ ^[1-9][0-9]*$ ]] || { echo "workers must be positive" >&2; exit 2; }

project_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
target_dir=${USSTOCK_RAW_TARGET_DIR:-$project_dir/target}
audit_binary=$target_dir/release/usstock_lseg_raw_correction_audit
export_binary=$target_dir/release/usstock_lseg_raw_export

cd "$project_dir"
cargo build --release --locked --target-dir "$target_dir" \
    --bin usstock_lseg_raw_correction_audit --bin usstock_lseg_raw_export
if find Cargo.toml ../../Cargo.lock src -type f -newer "$export_binary" -print -quit | grep -q .; then
    echo "refusing release: source is newer than $export_binary" >&2
    exit 1
fi

release_id=$(date -u +%Y%m%dT%H%M%SZ)
release_root="$output_parent/_raw_release_building_$release_id"
[[ ! -e $release_root ]] || { echo "release root already exists: $release_root" >&2; exit 1; }
mkdir -p "$release_root"/{logs,done,backtest_1s_raw,baseline_data_1m_raw}

rics_path="$release_root/rics.txt"
days_path="$release_root/sessions.txt"
if [[ -n $ric_filter ]]; then
    printf '%s\n' "$ric_filter" > "$rics_path"
else
    "$audit_binary" --rocksdb-dir "$rocksdb_dir" --list-rics > "$rics_path"
fi
awk -F, -v start="$start_date" -v end="$end_date" \
    'NR > 1 && $1 >= start && $1 <= end { print $1 }' "$calendar" > "$days_path"
ric_count=$(wc -l < "$rics_path")
day_count=$(wc -l < "$days_path")
((ric_count > 0 && day_count > 0)) || { echo "empty RIC or session scope" >&2; exit 1; }

jq -n \
    --arg schema 'usstock-raw-rth-export' \
    --arg status writing \
    --arg created_utc "$release_id" \
    --arg rocksdb "$rocksdb_dir" \
    --arg calendar "$calendar" \
    --arg start_date "$start_date" \
    --arg end_date "$end_date" \
    --argjson ric_count "$ric_count" \
    --argjson session_count "$day_count" \
    --arg raw_mode 'RAW L1/trades only; staged LL2 disabled for every RIC' \
    --arg correction_policy 'raw source prints are not cancellation/restatement netted; typed correction events remain preserved in RocksDB because CAN_TRD_ID/PD_TRDID cannot be losslessly joined to TRADE_ID' \
    '{schema:$schema,status:$status,created_utc:$created_utc,rocksdb:$rocksdb,calendar:$calendar,start_date:$start_date,end_date:$end_date,ric_count:$ric_count,session_count:$session_count,raw_mode:$raw_mode,trade_correction_policy:$correction_policy}' \
    > "$release_root/RELEASE.json"
printf 'RAW release staging=%s rics=%s sessions=%s workers=%s\n' \
    "$release_root" "$ric_count" "$day_count" "$workers"

export export_binary rocksdb_dir calendar release_root days_path
export_ric() {
    local ric=$1
    local log="$release_root/logs/$ric.log"
    printf 'start ric=%s\n' "$ric" > "$log"
    while IFS= read -r day; do
        "$export_binary" \
            --rocksdb-dir "$rocksdb_dir" \
            --calendar "$calendar" \
            --stage-ll2-root "$release_root/_stage_ll2_disabled" \
            --raw-only \
            --backtest-out-root "$release_root/backtest_1s_raw" \
            --baseline-out-root "$release_root/baseline_data_1m_raw" \
            --ric "$ric" \
            --day "$day"
    done < "$days_path" >> "$log" 2>&1
    printf 'complete ric=%s\n' "$ric" >> "$log"
    touch "$release_root/done/$ric"
}
export -f export_ric

xargs -r -n 1 -P "$workers" bash -c 'export_ric "$@"' _ < "$rics_path"

expected=$((ric_count * day_count))
backtest_count=$(find "$release_root/backtest_1s_raw" -type f -name '*.parquet' | wc -l)
baseline_count=$(find "$release_root/baseline_data_1m_raw" -type f -name '*.parquet' | wc -l)
manifest_count=$(find "$release_root/baseline_data_1m_raw/_raw_export_manifest" -type f -name '*.json' | wc -l)
done_count=$(find "$release_root/done" -type f | wc -l)
if ((backtest_count != expected || baseline_count != expected || manifest_count != expected || done_count != ric_count)); then
    echo "release verification failed: expected=$expected backtest=$backtest_count baseline=$baseline_count manifests=$manifest_count done=$done_count" >&2
    exit 1
fi
if find "$release_root/baseline_data_1m_raw" -type f -name '*.parquet' -print0 | xargs -0 -r strings | grep -q 'unknown_\(volume\|amount\|count\)'; then
    echo "release verification failed: unknown_* output column found" >&2
    exit 1
fi

jq '.status = "complete" | .completed_utc = now | .expected_parquet_per_root = ($expected | tonumber)' \
    --arg expected "$expected" "$release_root/RELEASE.json" > "$release_root/RELEASE.json.tmp"
mv "$release_root/RELEASE.json.tmp" "$release_root/RELEASE.json"

for root_name in backtest_1s_raw baseline_data_1m_raw; do
    current="$output_parent/$root_name"
    if [[ -e $current ]]; then
        mv "$current" "$output_parent/${root_name}_legacy_before_raw_contract_$release_id"
    fi
    mv "$release_root/$root_name" "$current"
done
mv "$release_root" "$output_parent/raw_release_contract_$release_id"
printf 'published RAW release: %s\n' "$output_parent/raw_release_contract_$release_id"
