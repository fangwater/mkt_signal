#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  run_raw_multi_period_release.sh \
    --period-plan PATH --calendar PATH --output-parent PATH \
    --seed-contract PATH [--workers N]

The tab-separated period plan has four columns with no header:
  start_date<TAB>end_date<TAB>rocksdb_dir<TAB>size_reference_rocksdb-or--

The current complete RAW release is hard-linked into one new staging root.
Every listed period is exported into that same root. Publication happens only
after all prior and new parquet/manifests pass the complete-count checks.
EOF
}

period_plan=
calendar=
output_parent=
seed_contract=
workers=16
resume_root=

while (($#)); do
    case "$1" in
        --period-plan) period_plan=$2; shift 2 ;;
        --calendar) calendar=$2; shift 2 ;;
        --output-parent) output_parent=$2; shift 2 ;;
        --seed-contract) seed_contract=$2; shift 2 ;;
        --workers) workers=$2; shift 2 ;;
        --resume-root) resume_root=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; exit 2 ;;
    esac
done

for required in period_plan calendar output_parent seed_contract; do
    [[ -n ${!required} ]] || { echo "missing --${required//_/-}" >&2; exit 2; }
done
[[ -f $period_plan ]] || { echo "missing period plan: $period_plan" >&2; exit 2; }
[[ -f $calendar ]] || { echo "missing calendar: $calendar" >&2; exit 2; }
[[ -f $seed_contract ]] || { echo "missing seed contract: $seed_contract" >&2; exit 2; }
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

seed_backtest="$output_parent/backtest_1s_raw"
seed_baseline="$output_parent/baseline_data_1m_raw"
[[ -d $seed_backtest && -d $seed_baseline ]] || {
    echo "seed RAW roots are missing below $output_parent" >&2
    exit 1
}
[[ $(jq -r '.status' "$seed_contract") == complete ]] || {
    echo "seed contract is not complete: $seed_contract" >&2
    exit 1
}

exec 9>"$output_parent/.raw_release.lock"
flock -n 9 || { echo 'another RAW release is running' >&2; exit 1; }
release_id=$(date -u +%Y%m%dT%H%M%SZ)
release_root="$output_parent/_raw_release_building_$release_id"
if [[ -n $resume_root ]]; then
    release_root=$(realpath "$resume_root")
    [[ $(dirname "$release_root") == "$(realpath "$output_parent")" && $(basename "$release_root") == _raw_release_building_* ]] || exit 2
    cmp "$period_plan" "$release_root/plans/periods.tsv"
    release_id=${release_root##*/_raw_release_building_}
else
[[ ! -e $release_root ]] || { echo "release root already exists: $release_root" >&2; exit 1; }
mkdir -p "$release_root"/{logs,done,plans,backtest_1s_raw,baseline_data_1m_raw}
cp -al "$seed_backtest/." "$release_root/backtest_1s_raw/"
cp -al "$seed_baseline/." "$release_root/baseline_data_1m_raw/"
cp "$period_plan" "$release_root/plans/periods.tsv"
fi

seed_backtest_count=$(find "$seed_backtest" -type f -name '*.parquet' | wc -l)
seed_baseline_count=$(find "$seed_baseline" -type f -name '*.parquet' | wc -l)
seed_manifest_count=$(find "$seed_baseline/_raw_export_manifest" -type f -name '*.json' | wc -l)
if ((seed_backtest_count == 0 || seed_backtest_count != seed_baseline_count || seed_backtest_count != seed_manifest_count)); then
    echo "seed release is incomplete: backtest=$seed_backtest_count baseline=$seed_baseline_count manifests=$seed_manifest_count" >&2
    exit 1
fi

tasks="$release_root/plans/tasks.tsv"
all_days="$release_root/plans/all_sessions.txt"
: > "$tasks"
: > "$all_days"
period_count=0
added_expected=0
while IFS=$'\t' read -r start_date end_date rocksdb_dir size_reference extra; do
    [[ -z $start_date || $start_date == \#* ]] && continue
    [[ -z $end_date || -z $rocksdb_dir || -n ${extra:-} ]] && {
        echo "invalid period-plan row: $start_date $end_date $rocksdb_dir $size_reference $extra" >&2
        exit 2
    }
    [[ -d $rocksdb_dir ]] || { echo "missing RocksDB: $rocksdb_dir" >&2; exit 2; }
    [[ $size_reference == - || -d $size_reference ]] || {
        echo "missing size-reference RocksDB: $size_reference" >&2
        exit 2
    }
    period_count=$((period_count + 1))
    period_id=$(printf '%03d' "$period_count")
    days="$release_root/plans/${period_id}_sessions.txt"
    rics="$release_root/plans/${period_id}_rics.txt"
    awk -F, -v start="$start_date" -v end="$end_date" \
        'NR > 1 && $1 >= start && $1 <= end { print $1 }' "$calendar" > "$days"
    "$audit_binary" --rocksdb-dir "$rocksdb_dir" --list-rics > "$rics"
    day_count=$(wc -l < "$days")
    ric_count=$(wc -l < "$rics")
    ((day_count > 0 && ric_count > 0)) || {
        echo "empty export scope period=$period_id rics=$ric_count sessions=$day_count" >&2
        exit 1
    }
    cat "$days" >> "$all_days"
    while IFS= read -r ric; do
        printf '%s\t%s\t%s\t%s\t%s\n' \
            "$period_id" "$rocksdb_dir" "$size_reference" "$days" "$ric" >> "$tasks"
    done < "$rics"
    added_expected=$((added_expected + day_count * ric_count))
done < "$period_plan"

((period_count > 0)) || { echo "period plan is empty" >&2; exit 2; }
if sort "$all_days" | uniq -d | grep -q .; then
    echo "period plan has overlapping NYSE session dates" >&2
    exit 2
fi
task_count=$(wc -l < "$tasks")
printf 'RAW multi-period staging=%s seed=%s periods=%s tasks=%s added_expected=%s total_expected=%s workers=%s\n' \
    "$release_root" "$seed_backtest_count" "$period_count" "$task_count" \
    "$added_expected" "$((seed_backtest_count + added_expected))" "$workers"

export export_binary calendar release_root
export_task() {
    set -euo pipefail
    local task=$1
    local period_id rocksdb_dir size_reference days ric log
    IFS=$'\t' read -r period_id rocksdb_dir size_reference days ric <<< "$task"
    log="$release_root/logs/${period_id}.${ric}.log"
    printf 'start period=%s ric=%s\n' "$period_id" "$ric" >> "$log"
    local -a reference_args=()
    if [[ $size_reference != - ]]; then
        reference_args=(--size-reference-rocksdb "$size_reference")
    fi
    while IFS= read -r day; do
        local venue stamp backtest_path baseline_path manifest_path complete=1
        case "$ric" in
            *.O) venue=NASDAQ ;; *.N) venue=NYSE ;; *.P) venue=ARCA ;; *.BAT) venue=BZX ;;
            *) echo "unsupported RIC $ric" >&2; return 1 ;;
        esac
        stamp=${day//-/}
        backtest_path="$release_root/backtest_1s_raw/$venue/$ric/$stamp.parquet"
        baseline_path="$release_root/baseline_data_1m_raw/$venue/$ric/$stamp.parquet"
        manifest_path="$release_root/baseline_data_1m_raw/_raw_export_manifest/$venue/$ric/$stamp.json"
        if [[ ! -s $backtest_path || ! -s $baseline_path ]] ||
            ! jq -e --arg ric "$ric" --arg day "$day" --arg db "$rocksdb_dir" \
            '.ric == $ric and .session_date == $day and .raw_rocksdb == $db and .raw_only == true' \
            "$manifest_path" >/dev/null 2>&1; then
            complete=0
        fi
        if ((complete)); then continue; fi
        "$export_binary" \
            --overwrite \
            --rocksdb-dir "$rocksdb_dir" \
            "${reference_args[@]}" \
            --calendar "$calendar" \
            --stage-ll2-root "$release_root/_stage_ll2_disabled" \
            --raw-only \
            --backtest-out-root "$release_root/backtest_1s_raw" \
            --baseline-out-root "$release_root/baseline_data_1m_raw" \
            --ric "$ric" \
            --day "$day"
    done < "$days" >> "$log" 2>&1
    printf 'complete period=%s ric=%s\n' "$period_id" "$ric" >> "$log"
    touch "$release_root/done/${period_id}.${ric}"
}
export -f export_task

xargs -r -d '\n' -n 1 -P "$workers" bash -e -c 'export_task "$1"' _ < "$tasks"

expected=$((seed_backtest_count + added_expected))
backtest_count=$(find "$release_root/backtest_1s_raw" -type f -name '*.parquet' | wc -l)
baseline_count=$(find "$release_root/baseline_data_1m_raw" -type f -name '*.parquet' | wc -l)
manifest_count=$(find "$release_root/baseline_data_1m_raw/_raw_export_manifest" -type f -name '*.json' | wc -l)
done_count=$(find "$release_root/done" -type f | wc -l)
if ((backtest_count != expected || baseline_count != expected || manifest_count != expected || done_count != task_count)); then
    echo "release verification failed: expected=$expected backtest=$backtest_count baseline=$baseline_count manifests=$manifest_count done=$done_count tasks=$task_count" >&2
    exit 1
fi
if find "$release_root/baseline_data_1m_raw" -type f -name '*.parquet' -print0 | xargs -0 -r strings | grep -q 'unknown_\(volume\|amount\|count\)'; then
    echo "release verification failed: unknown_* output column found" >&2
    exit 1
fi

periods_json=$(jq -Rsc '
    split("\n")
    | map(select(length > 0) | select(startswith("#") | not) | split("\t") | {
        start_date: .[0], end_date: .[1], rocksdb: .[2],
        size_reference_rocksdb: (if .[3] == "-" then null else .[3] end)
    })
' "$period_plan")
jq -n \
    --arg schema 'usstock-raw-rth-export' \
    --arg status complete \
    --arg created_utc "$release_id" \
    --arg completed_utc "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --arg calendar "$calendar" \
    --arg seed_contract "$seed_contract" \
    --argjson seed_expected "$seed_backtest_count" \
    --argjson added_expected "$added_expected" \
    --argjson expected "$expected" \
    --argjson periods "$periods_json" \
    '{schema:$schema,status:$status,created_utc:$created_utc,completed_utc:$completed_utc,
      calendar:$calendar,seed_contract:$seed_contract,seed_expected_parquet_per_root:$seed_expected,
      added_expected_parquet_per_root:$added_expected,expected_parquet_per_root:$expected,
      periods:$periods,raw_mode:"RAW L1/trades only; staged LL2 disabled for every RIC",
      trade_correction_policy:"raw source prints are not cancellation/restatement netted; typed correction events remain preserved in RocksDB because CAN_TRD_ID/PD_TRDID cannot be losslessly joined to TRADE_ID"}' \
    > "$release_root/RELEASE.json"

for root_name in backtest_1s_raw baseline_data_1m_raw; do
    current="$output_parent/$root_name"
    mv "$current" "$output_parent/${root_name}_legacy_before_raw_contract_$release_id"
    mv "$release_root/$root_name" "$current"
done
mv "$release_root" "$output_parent/raw_release_contract_$release_id"
printf 'published RAW release: %s\n' "$output_parent/raw_release_contract_$release_id"
