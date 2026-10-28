//! Export CN-aligned contract-level CME `baseline_data_1min` from TAS + LL2,
//! or an explicit TAS-only grid with null quote fields.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::Chicago;
use clap::Parser;
use cme_tas_replay::baseline_1min::{
    build_minutes, build_trade_only_minutes, BaselineMinute, BaselineSpecial, BaselineTrade,
    Book10, BASELINE_DEPTH_LEVELS,
};
use cme_tas_replay::ll2_1min::{
    decode_ll2_minute, decode_ll2_minute_key, encode_ll2_minute_key, ll2_latest_merge,
    Ll2MinuteKey, LL2_MINUTE_KEY_LEN,
};
use cme_tas_replay::product::{encode_all_key, product_cf_name, ALL_KEY_LEN};
use cme_tas_replay::trade_timing::{bar_time, TimeBasis, TimingAudit};
use cme_tas_replay::{
    decode_cme_special, decode_cme_trade, decode_period_status, price_e9_to_f64, PeriodStatus,
    CF_REPLAY_META, KIND_CME_SPECIAL, KIND_CME_TRADE, MISSING_PRICE, MISSING_VOLUME, PRICE_SCALE,
    RIC_LEN,
};
use mimalloc::MiMalloc;
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter, SerReader, Series,
};
use rayon::prelude::*;
use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, Direction, IteratorMode, MultiThreaded, Options,
};
use serde::Serialize;

#[path = "../baseline_history.rs"]
mod historical;
#[path = "../baseline_raw.rs"]
mod raw_baseline;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

type DB = DBWithThreadMode<MultiThreaded>;

const DEFAULT_TAS_DB: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products";
const DEFAULT_LL2_DB: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_ll2_1min_rocksdb_all_products";
const DEFAULT_SUMMARY: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/summary_1m";
const DEFAULT_OUTPUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/baseline_data_1min";
const DEFAULT_TAS_SECONDARY: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products.baseline.secondary";
const DEFAULT_LL2_SECONDARY: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_ll2_1min_rocksdb_all_products.baseline.secondary";
const DEFAULT_PSQL: &str = "/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql";
const DEFAULT_PG_SOCKET: &str = "/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run";
const PERIOD_META_PREFIX: &str = "period:";
const NS_PER_SEC: u64 = 1_000_000_000;
const HALF_DAY_NS: u64 = 43_200 * NS_PER_SEC;
const SESSION_BREAK_SECONDS: i64 = 30 * 60;

#[derive(Parser, Debug)]
#[command(name = "cme_baseline_1min")]
struct Args {
    /// Reviewed official trade dates, explicit RIC identities and bounded multipliers.
    #[arg(long)]
    history_catalog: Option<PathBuf>,
    /// Timestamped halt/resume events for the reviewed planned calendar.
    #[arg(long)]
    session_status: Option<PathBuf>,
    /// Source publication time makes each closed minute immutable.
    #[arg(long, value_enum, default_value_t = TimeBasis::Exchange)]
    trade_time_basis: TimeBasis,
    #[arg(long, default_value = DEFAULT_TAS_DB)]
    tas_rocksdb: PathBuf,
    #[arg(long, default_value = DEFAULT_LL2_DB)]
    ll2_rocksdb: PathBuf,
    /// Summary supplies only valid TradDays and listed contracts. Prices and
    /// the minute grid come exclusively from LL2; trades come from TAS.
    #[arg(long, default_value = DEFAULT_SUMMARY)]
    summary_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUTPUT)]
    output_root: PathBuf,
    #[arg(long, default_value = DEFAULT_TAS_SECONDARY)]
    tas_secondary: PathBuf,
    #[arg(long, default_value = DEFAULT_LL2_SECONDARY)]
    ll2_secondary: PathBuf,
    #[arg(long, default_value = "2020-01-01")]
    start: NaiveDate,
    #[arg(long, default_value = "2026-06-01")]
    end: NaiveDate,
    #[arg(long, value_delimiter = ',', default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: Vec<String>,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    direct_read_only: bool,
    #[arg(long)]
    audit_only: bool,
    /// Emit the official session minute grid from TAS alone. All L1/L10 and
    /// mid-price fields are null; no LL2 access is attempted.
    #[arg(long)]
    trade_only: bool,
    /// Exact minute-end overlay from Raw 15s + quality CFs, on the official
    /// session grid. Unknown/invalidated states remain null.
    #[arg(long, conflicts_with = "trade_only")]
    raw_ll2: bool,
    /// Enumerate standard RIC contracts directly from TAS. Required for rates
    /// not represented by the historical Summary routing subset.
    #[arg(long)]
    tas_contract_grid: bool,
    /// Populate size buckets using verified prior-month TAS thresholds.
    #[arg(long)]
    size_buckets: bool,
    #[arg(long)]
    max_files: Option<usize>,
    #[arg(long, default_value = DEFAULT_PSQL)]
    psql: PathBuf,
    #[arg(long, default_value = DEFAULT_PG_SOCKET)]
    pg_socket: PathBuf,
}

#[derive(Clone, Copy, Debug)]
struct ProductSpec {
    product: &'static str,
    exchange: &'static str,
    schedule_group: &'static str,
}

const PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        product: "ES",
        exchange: "CME",
        schedule_group: "equity_indices",
    },
    ProductSpec {
        product: "NQ",
        exchange: "CME",
        schedule_group: "equity_indices",
    },
    ProductSpec {
        product: "RTY",
        exchange: "CME",
        schedule_group: "equity_indices",
    },
    ProductSpec {
        product: "YM",
        exchange: "CBOT",
        schedule_group: "equity_indices",
    },
    ProductSpec {
        product: "GC",
        exchange: "COMEX",
        schedule_group: "metals",
    },
    ProductSpec {
        product: "CL",
        exchange: "NYMEX",
        schedule_group: "energy",
    },
];

const RATE_PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        product: "TU",
        exchange: "CBOT",
        schedule_group: "interest_rates",
    },
    ProductSpec {
        product: "FV",
        exchange: "CBOT",
        schedule_group: "interest_rates",
    },
    ProductSpec {
        product: "TY",
        exchange: "CBOT",
        schedule_group: "interest_rates",
    },
    ProductSpec {
        product: "TN",
        exchange: "CBOT",
        schedule_group: "interest_rates",
    },
    ProductSpec {
        product: "US",
        exchange: "CBOT",
        schedule_group: "interest_rates",
    },
    ProductSpec {
        product: "AUL",
        exchange: "CBOT",
        schedule_group: "interest_rates",
    },
    ProductSpec {
        product: "FF",
        exchange: "CBOT",
        schedule_group: "interest_rates",
    },
];

const ADDITIONAL_PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        product: "URO",
        exchange: "CME",
        schedule_group: "fx",
    },
    ProductSpec {
        product: "JY",
        exchange: "CME",
        schedule_group: "fx",
    },
    ProductSpec {
        product: "HG",
        exchange: "COMEX",
        schedule_group: "metals",
    },
    ProductSpec {
        product: "SI",
        exchange: "COMEX",
        schedule_group: "metals",
    },
];

fn product_spec(product: &str) -> Result<ProductSpec> {
    PRODUCTS
        .iter()
        .chain(RATE_PRODUCTS.iter())
        .chain(ADDITIONAL_PRODUCTS.iter())
        .copied()
        .find(|spec| spec.product == product)
        .ok_or_else(|| anyhow!("unsupported baseline product {product:?}"))
}

#[derive(Clone, Copy)]
struct RawInterval {
    start: i64,
    end: i64,
}

struct SessionCalendar {
    by_group: BTreeMap<String, Vec<RawInterval>>,
    coverage: BTreeMap<String, (NaiveDate, NaiveDate)>,
    history: Option<Arc<historical::ReviewedHistory>>,
}

fn chicago_window(day: NaiveDate) -> Result<(i64, i64)> {
    let end = Chicago
        .from_local_datetime(&day.and_hms_opt(17, 0, 0).expect("valid clock"))
        .single()
        .ok_or_else(|| anyhow!("ambiguous Chicago 17:00 on {day}"))?;
    Ok(((end - Duration::days(1)).timestamp(), end.timestamp()))
}

impl SessionCalendar {
    fn segments_for_product(&self, spec: ProductSpec, day: NaiveDate) -> Result<Vec<(i64, i64)>> {
        if let Some(history) = &self.history {
            if day < history.from || day >= history.until {
                bail!("historical calendar range missing {} {day}", spec.product);
            }
            return Ok(history
                .days
                .get(&(spec.product.to_string(), day))
                .map(|r| {
                    r.segments
                        .iter()
                        .map(|&(lo, hi)| ((lo / NS_PER_SEC) as i64, (hi / NS_PER_SEC) as i64 - 60))
                        .collect()
                })
                .unwrap_or_default());
        }
        self.minute_segments_for(spec.schedule_group, day)
    }
    fn minute_segments_for(&self, group: &str, day: NaiveDate) -> Result<Vec<(i64, i64)>> {
        let (coverage_start, coverage_end) = self
            .coverage
            .get(group)
            .ok_or_else(|| anyhow!("calendar has no schedule group {group}"))?;
        let (window_start, window_end) = chicago_window(day)?;
        let start_utc = Utc
            .timestamp_opt(window_start, 0)
            .single()
            .expect("unix timestamp")
            .date_naive();
        let end_utc = Utc
            .timestamp_opt(window_end - 1, 0)
            .single()
            .expect("unix timestamp")
            .date_naive();
        if start_utc < *coverage_start || end_utc > *coverage_end {
            bail!(
                "calendar_missing group={group} TradDay={day} coverage={coverage_start}..{coverage_end}"
            );
        }
        let mut clipped = self
            .by_group
            .get(group)
            .into_iter()
            .flatten()
            .filter_map(|interval| {
                (interval.start < window_end && interval.end > window_start).then_some(
                    RawInterval {
                        start: interval.start.max(window_start),
                        end: interval.end.min(window_end),
                    },
                )
            })
            .collect::<Vec<_>>();
        clipped.sort_by_key(|interval| (interval.start, interval.end));
        let mut merged: Vec<RawInterval> = Vec::new();
        for interval in clipped {
            if let Some(previous) = merged.last_mut() {
                if interval.start <= previous.end {
                    previous.end = previous.end.max(interval.end);
                    continue;
                }
            }
            merged.push(interval);
        }
        let mut output = Vec::with_capacity(merged.len());
        for interval in merged {
            if interval.start % 60 != 0 || interval.end % 60 != 0 {
                bail!(
                    "calendar interval is not minute aligned: [{}, {})",
                    interval.start,
                    interval.end
                );
            }
            if interval.end - interval.start >= 60 {
                output.push((interval.start, interval.end - 60));
            }
        }
        Ok(output)
    }
}

fn load_calendar(
    psql: &Path,
    pg_socket: &Path,
    start: NaiveDate,
    end: NaiveDate,
) -> Result<SessionCalendar> {
    let query_start = start - Duration::days(2);
    let query_end = end + Duration::days(2);
    let sql = format!(
        "SELECT schedule_group, utc_date, CASE WHEN is_trading THEN 1 ELSE 0 END, COALESCE(extract(epoch FROM open_utc)::bigint::text,''), COALESCE(extract(epoch FROM close_utc)::bigint::text,'') FROM public.cme_globex_daily_trading_intervals WHERE schedule_group IN ('equity_indices','metals','energy','interest_rates') AND utc_date BETWEEN DATE '{query_start}' AND DATE '{query_end}' ORDER BY schedule_group, utc_date, interval_index"
    );
    let command = Command::new(psql)
        .args([
            "-h",
            pg_socket.to_str().context("PG socket is not UTF-8")?,
            "-p",
            "5433",
            "-U",
            "u171",
            "-d",
            "market_metadata",
            "-At",
            "-F",
            "\t",
            "-c",
            &sql,
        ])
        .output()
        .with_context(|| format!("run calendar psql {}", psql.display()))?;
    if !command.status.success() {
        bail!(
            "calendar psql failed: {}",
            String::from_utf8_lossy(&command.stderr)
        );
    }
    let mut by_group: BTreeMap<String, Vec<RawInterval>> = BTreeMap::new();
    let mut coverage: BTreeMap<String, (NaiveDate, NaiveDate)> = BTreeMap::new();
    for (line_no, line) in String::from_utf8(command.stdout)?.lines().enumerate() {
        let cells = line.split('\t').collect::<Vec<_>>();
        if cells.len() != 5 {
            bail!("calendar row {} has {} cells", line_no + 1, cells.len());
        }
        let group = cells[0].to_string();
        let utc_date = NaiveDate::parse_from_str(cells[1], "%Y-%m-%d")?;
        coverage
            .entry(group.clone())
            .and_modify(|range| {
                range.0 = range.0.min(utc_date);
                range.1 = range.1.max(utc_date);
            })
            .or_insert((utc_date, utc_date));
        if cells[2] != "1" {
            continue;
        }
        let interval = RawInterval {
            start: cells[3].parse()?,
            end: cells[4].parse()?,
        };
        if interval.end <= interval.start {
            bail!("calendar interval is not increasing on row {}", line_no + 1);
        }
        by_group.entry(group).or_default().push(interval);
    }
    Ok(SessionCalendar {
        by_group,
        coverage,
        history: None,
    })
}

#[derive(Clone, Debug)]
struct Job {
    product: String,
    exchange: String,
    day: NaiveDate,
    input: PathBuf,
    output: PathBuf,
    audit: PathBuf,
    tas_grids: Option<Vec<RicGrid>>,
}

#[derive(Clone, Debug)]
struct RicGrid {
    contract_id: String,
    ric: String,
    segments: Vec<(i64, i64)>,
    status_segments: Option<Vec<(i64, i64)>>,
}

impl RicGrid {
    fn output_segments(&self) -> &[(i64, i64)] {
        self.status_segments.as_deref().unwrap_or(&self.segments)
    }
}

#[derive(Default, Serialize)]
struct DayAudit {
    trade_time_basis: TimeBasis,
    trade_timing: TimingAudit,
    special_timing: TimingAudit,
    exchange: String,
    product: String,
    trading_day: String,
    input_file: String,
    output_file: String,
    ric_count: u64,
    output_ric_count: u64,
    grid_minutes: u64,
    status_removed_grid_minutes: u64,
    output_rows: u64,
    trade_rows: u64,
    special_rows: u64,
    invalid_special_rows: u64,
    exact_ll2_minutes: u64,
    raw_bidirectional_minutes: u64,
    raw_full_depth_minutes: u64,
    raw_last_invalid_minutes: u64,
    raw_no_snapshot_minutes: u64,
    outside_session_trades: u64,
    outside_session_specials: u64,
    threshold_source_max_published_ns: Option<u64>,
    rics_without_ll2: Vec<String>,
    calendar_closed: bool,
    published: bool,
}

fn list_jobs(args: &Args, products: &[ProductSpec]) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for spec in products {
        let dir = args.summary_root.join(spec.exchange).join(spec.product);
        if !dir.is_dir() {
            bail!("missing summary product directory {}", dir.display());
        }
        for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let input = entry?.path();
            if input.extension().and_then(|value| value.to_str()) != Some("parquet") {
                continue;
            }
            let stem = input
                .file_stem()
                .and_then(|value| value.to_str())
                .unwrap_or("");
            let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
                continue;
            };
            // Availability comes from the selected LL2 source, not an old
            // year/product exclusion. Missing books are audited in process_job.
            if day < args.start || day >= args.end {
                continue;
            }
            let output = args
                .output_root
                .join(spec.exchange)
                .join(spec.product)
                .join(format!("{stem}.parquet"));
            if output.exists() && !args.overwrite {
                continue;
            }
            let audit = args
                .output_root
                .join("_audit")
                .join(spec.product)
                .join(format!("{stem}.json"));
            jobs.push(Job {
                product: spec.product.to_string(),
                exchange: spec.exchange.to_string(),
                day,
                input,
                output,
                audit,
                tas_grids: None,
            });
        }
    }
    jobs.sort_by(|left, right| {
        left.day
            .cmp(&right.day)
            .then(left.product.cmp(&right.product))
    });
    if let Some(limit) = args.max_files {
        jobs.truncate(limit);
    }
    if jobs.is_empty() {
        bail!("no baseline jobs selected");
    }
    Ok(jobs)
}

fn segments_from_book_minutes(mut minutes: Vec<i64>) -> Result<Vec<(i64, i64)>> {
    minutes.sort_unstable();
    minutes.dedup();
    if minutes.is_empty() {
        return Ok(Vec::new());
    }
    if minutes.iter().any(|minute| minute % 60 != 0) {
        bail!("LL2 minute grid contains a non-minute timestamp");
    }
    let mut output = Vec::new();
    let mut start = minutes[0];
    let mut previous = minutes[0];
    for minute in minutes.into_iter().skip(1) {
        if minute - previous >= SESSION_BREAK_SECONDS {
            output.push((start, previous));
            start = minute;
        }
        previous = minute;
    }
    output.push((start, previous));
    Ok(output)
}

fn delivery_month(contract_id: &str) -> Result<(i32, u32)> {
    let mut parts = contract_id.split(':');
    let _exchange = parts.next();
    let _product = parts.next();
    let delivery = parts
        .next()
        .filter(|_| parts.next().is_none())
        .with_context(|| format!("invalid contract_id {contract_id:?}"))?;
    let (year, month) = delivery
        .split_once('-')
        .with_context(|| format!("invalid delivery month in {contract_id:?}"))?;
    let year = year.parse::<i32>()?;
    let month = month.parse::<u32>()?;
    if !(1..=12).contains(&month) {
        bail!("invalid delivery month in {contract_id:?}");
    }
    Ok((year, month))
}

fn resolve_ambiguous_ric(ric: &str, day: NaiveDate, contracts: BTreeSet<String>) -> Result<String> {
    if contracts.len() == 1 {
        return Ok(contracts.into_iter().next().unwrap());
    }
    let mut candidates = Vec::new();
    for contract in contracts {
        let delivery = delivery_month(&contract)?;
        if delivery >= (day.year(), day.month()) {
            candidates.push((delivery, contract));
        }
    }
    candidates
        .into_iter()
        .min_by_key(|(delivery, _)| *delivery)
        .map(|(_, contract)| contract)
        .with_context(|| format!("RIC {ric} has no unexpired contract mapping on {day}"))
}

fn read_contracts(path: &Path, expected_product: &str, day: NaiveDate) -> Result<Vec<RicGrid>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let df = ParquetReader::new(file)
        .set_low_memory(true)
        .finish()
        .with_context(|| format!("read {}", path.display()))?;
    let contracts = df.column("contract_id")?.str()?;
    let rics = df.column("ric")?.str()?;
    let mut grouped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for index in 0..df.height() {
        let contract = contracts
            .get(index)
            .with_context(|| format!("null contract_id row {index} {}", path.display()))?;
        let ric = rics
            .get(index)
            .with_context(|| format!("null ric row {index} {}", path.display()))?;
        if cme_tas_replay::product::parse_product(ric).as_deref() != Some(expected_product) {
            bail!(
                "RIC {ric} in {} is not product {expected_product}",
                path.display()
            );
        }
        grouped
            .entry(ric.to_string())
            .or_default()
            .insert(contract.to_string());
    }
    grouped
        .into_iter()
        .map(|(ric, contracts)| {
            Ok(RicGrid {
                contract_id: resolve_ambiguous_ric(&ric, day, contracts)?,
                ric,
                segments: Vec::new(),
                status_segments: None,
            })
        })
        .collect::<Result<Vec<_>>>()
}

fn chicago_window_minutes(day: NaiveDate) -> Result<(i64, i64)> {
    let close_local = Chicago
        .from_local_datetime(&day.and_time(NaiveTime::from_hms_opt(17, 0, 0).unwrap()))
        .single()
        .with_context(|| format!("ambiguous Chicago 17:00 for {day}"))?;
    let open_local = close_local - Duration::days(1);
    Ok((
        open_local.timestamp().div_euclid(60) * 60,
        close_local.timestamp().div_euclid(60) * 60,
    ))
}

fn minute_in_segments(minute: i64, segments: &[(i64, i64)]) -> bool {
    segments
        .iter()
        .any(|&(start, end)| minute >= start && minute <= end)
}

fn key_prefix(kind: u8, ric: &str) -> Result<Vec<u8>> {
    Ok(encode_all_key(kind, ric, 0, 0, 0)?[..1 + RIC_LEN].to_vec())
}

fn key_ts_ns(key: &[u8]) -> Result<u64> {
    if key.len() != ALL_KEY_LEN {
        bail!("TAS key length {} != {ALL_KEY_LEN}", key.len());
    }
    Ok(u64::from_be_bytes(key[17..25].try_into().unwrap()))
}

fn scan_trades(
    db: &DB,
    cf: &impl rocksdb::AsColumnFamilyRef,
    grid: &RicGrid,
    basis: TimeBasis,
) -> Result<Vec<BaselineTrade>> {
    Ok(scan_trades_audited(db, cf, grid, None, basis)?.0)
}

fn scan_trades_audited(
    db: &DB,
    cf: &impl rocksdb::AsColumnFamilyRef,
    grid: &RicGrid,
    audit_day: Option<NaiveDate>,
    basis: TimeBasis,
) -> Result<(Vec<BaselineTrade>, u64, TimingAudit)> {
    let Some(&(start, _)) = grid.segments.first() else {
        return Ok((Vec::new(), 0, TimingAudit::default()));
    };
    let audit_window = audit_day.map(chicago_window).transpose()?.map(|(lo, hi)| {
        if basis == TimeBasis::Publication {
            (lo.min(start), hi.max(grid.segments.last().unwrap().1 + 60))
        } else {
            (lo, hi)
        }
    });
    let mut outside_session = 0;
    let mut timing = TimingAudit::default();
    let end = grid.segments.last().unwrap().1 + 60;
    let padding = if basis == TimeBasis::Exchange {
        HALF_DAY_NS
    } else {
        0
    };
    let mut padded_start =
        (u64::try_from(start)?.saturating_mul(NS_PER_SEC)).saturating_sub(padding);
    let mut padded_end = u64::try_from(end)?
        .saturating_mul(NS_PER_SEC)
        .saturating_add(padding);
    // Include closed portions of the labeled day in the audit. Membership in
    // the official half-open segments below still controls bar inclusion.
    if let Some((lo, hi)) = audit_window {
        if basis == TimeBasis::Publication {
            padded_start = padded_start.min(u64::try_from(lo)? * NS_PER_SEC);
            padded_end = padded_end.max(u64::try_from(hi)? * NS_PER_SEC);
        }
    }
    let prefix = key_prefix(KIND_CME_TRADE, &grid.ric)?;
    let seek = encode_all_key(KIND_CME_TRADE, &grid.ric, padded_start, 0, 0)?;
    let mut output = Vec::new();
    for item in db.iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(&prefix) {
            break;
        }
        if key_ts_ns(&key)? >= padded_end {
            break;
        }
        let rec = decode_cme_trade(&value)?;
        let event_ns = bar_time(rec.ts_utc_ns, rec.exch_hms_ns, basis)?;
        let minute = (event_ns / NS_PER_SEC / 60 * 60) as i64;
        if !minute_in_segments(minute, grid.output_segments()) {
            if audit_window.is_some_and(|(lo, hi)| {
                event_ns >= lo as u64 * NS_PER_SEC && event_ns < hi as u64 * NS_PER_SEC
            }) {
                outside_session += 1;
            }
            continue;
        }
        let Some(price) = price_e9_to_f64(rec.price).filter(|price| *price > 0.0) else {
            continue;
        };
        if rec.volume == MISSING_VOLUME || rec.volume == 0 {
            bail!("trade {} has invalid volume {}", grid.ric, rec.volume);
        }
        timing.add(rec.ts_utc_ns, rec.exch_hms_ns, rec.volume as u64)?;
        output.push(BaselineTrade {
            event_ns,
            source_order: key.to_vec(),
            price,
            volume: f64::from(rec.volume),
            aggressor: rec.aggressor,
        });
    }
    Ok((output, outside_session, timing))
}

fn scan_specials(
    db: &DB,
    cf: &impl rocksdb::AsColumnFamilyRef,
    grid: &RicGrid,
    audit_day: Option<NaiveDate>,
    basis: TimeBasis,
) -> Result<(Vec<BaselineSpecial>, u64, u64, TimingAudit)> {
    let Some(&(start, _)) = grid.segments.first() else {
        return Ok((Vec::new(), 0, 0, TimingAudit::default()));
    };
    let audit_window = audit_day.map(chicago_window).transpose()?.map(|(lo, hi)| {
        if basis == TimeBasis::Publication {
            (lo.min(start), hi.max(grid.segments.last().unwrap().1 + 60))
        } else {
            (lo, hi)
        }
    });
    let mut outside_session = 0;
    let end = grid.segments.last().unwrap().1 + 60;
    let padding = if basis == TimeBasis::Exchange {
        HALF_DAY_NS
    } else {
        0
    };
    let mut padded_start =
        (u64::try_from(start)?.saturating_mul(NS_PER_SEC)).saturating_sub(padding);
    let mut padded_end = u64::try_from(end)?
        .saturating_mul(NS_PER_SEC)
        .saturating_add(padding);
    if let Some((lo, hi)) = audit_window {
        if basis == TimeBasis::Publication {
            padded_start = padded_start.min(u64::try_from(lo)? * NS_PER_SEC);
            padded_end = padded_end.max(u64::try_from(hi)? * NS_PER_SEC);
        }
    }
    let prefix = key_prefix(KIND_CME_SPECIAL, &grid.ric)?;
    let seek = encode_all_key(KIND_CME_SPECIAL, &grid.ric, padded_start, 0, 0)?;
    let mut output = Vec::new();
    let mut invalid = 0u64;
    let mut timing = TimingAudit::default();
    for item in db.iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(&prefix) {
            break;
        }
        if key_ts_ns(&key)? >= padded_end {
            break;
        }
        let rec = decode_cme_special(&value)?;
        let event_ns = bar_time(rec.ts_utc_ns, rec.exch_hms_ns, basis)?;
        let minute = (event_ns / NS_PER_SEC / 60 * 60) as i64;
        if !minute_in_segments(minute, grid.output_segments()) {
            if audit_window.is_some_and(|(lo, hi)| {
                event_ns >= lo as u64 * NS_PER_SEC && event_ns < hi as u64 * NS_PER_SEC
            }) {
                outside_session += 1;
            }
            continue;
        }
        if rec.volume == MISSING_VOLUME || rec.volume == 0 {
            // A price-free zero/missing-volume Special cannot contribute to
            // the standard volume event. Keep it in the day audit, but do not
            // discard an otherwise valid TAS baseline day.
            invalid += 1;
            continue;
        }
        timing.add(rec.ts_utc_ns, rec.exch_hms_ns, rec.volume as u64)?;
        output.push(BaselineSpecial {
            event_ns,
            volume: f64::from(rec.volume),
        });
    }
    Ok((output, invalid, outside_session, timing))
}

fn scaled_option(value: i64) -> Option<f64> {
    (value != MISSING_PRICE).then_some(value as f64 / PRICE_SCALE as f64)
}

fn scan_books(
    db: &DB,
    cf: &impl rocksdb::AsColumnFamilyRef,
    ric: &str,
    start: i64,
    end_exclusive: i64,
) -> Result<BTreeMap<i64, Book10>> {
    let seek = encode_ll2_minute_key(&Ll2MinuteKey {
        ric: ric.to_string(),
        minute_utc_ns: u64::try_from(start)? * NS_PER_SEC,
    })?;
    let ric_prefix = &seek[..RIC_LEN];
    let mut output = BTreeMap::new();
    for item in db.iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward)) {
        let (key, value) = item?;
        if key.len() != LL2_MINUTE_KEY_LEN || !key.starts_with(ric_prefix) {
            break;
        }
        let decoded_key = decode_ll2_minute_key(&key)?;
        let minute = i64::try_from(decoded_key.minute_utc_ns / NS_PER_SEC)?;
        if minute >= end_exclusive {
            break;
        }
        let snapshot = decode_ll2_minute(&value)?;
        let mut book = Book10::default();
        for level in 0..BASELINE_DEPTH_LEVELS {
            book.bid_prices[level] = scaled_option(snapshot.bid_prices[level]);
            book.bid_sizes[level] = scaled_option(snapshot.bid_sizes[level]);
            book.ask_prices[level] = scaled_option(snapshot.ask_prices[level]);
            book.ask_sizes[level] = scaled_option(snapshot.ask_sizes[level]);
        }
        if book.valid() {
            output.insert(minute, book);
        }
    }
    Ok(output)
}

fn grid_minutes(segments: &[(i64, i64)]) -> u64 {
    segments
        .iter()
        .map(|(start, end)| ((end - start) / 60 + 1) as u64)
        .sum()
}

fn process_job(
    job: &Job,
    tas: &DB,
    ll2: &DB,
    multipliers: &BTreeMap<String, f64>,
    args: &Args,
    parquet_lock: &Mutex<()>,
) -> Result<DayAudit> {
    let started = Instant::now();
    let mut grids = raw_baseline::job_grids(job)?;
    let (window_start, window_end) = chicago_window_minutes(job.day)?;
    let multiplier = *multipliers
        .get(&job.product)
        .with_context(|| format!("missing verified multiplier for {}", job.product))?;
    let tas_cf_name = product_cf_name(job.day.year() as u16, &job.product)?;
    let ll2_cf_name = product_cf_name(job.day.year() as u16, &job.product)?;
    let tas_cf = tas
        .cf_handle(&tas_cf_name)
        .with_context(|| format!("missing TAS CF {tas_cf_name}"))?;
    let ll2_cf = ll2
        .cf_handle(&ll2_cf_name)
        .with_context(|| format!("missing LL2 CF {ll2_cf_name}"))?;
    let mut rows = Vec::new();
    let mut audit = DayAudit {
        exchange: job.exchange.clone(),
        product: job.product.clone(),
        trading_day: job.day.to_string(),
        input_file: job.input.display().to_string(),
        output_file: job.output.display().to_string(),
        ric_count: grids.len() as u64,
        ..DayAudit::default()
    };
    for grid in &mut grids {
        let books = scan_books(ll2, &ll2_cf, &grid.ric, window_start, window_end)?;
        grid.segments = segments_from_book_minutes(books.keys().copied().collect())?;
        audit.grid_minutes += grid_minutes(&grid.segments);
        audit.exact_ll2_minutes += books.len() as u64;
        if books.is_empty() {
            audit.rics_without_ll2.push(grid.ric.clone());
            continue;
        }
        let (trades, _, trade_timing) =
            scan_trades_audited(tas, &tas_cf, &grid, None, args.trade_time_basis)?;
        let (specials, invalid_specials, _, special_timing) =
            scan_specials(tas, &tas_cf, &grid, None, args.trade_time_basis)?;
        audit.trade_time_basis = args.trade_time_basis;
        audit.trade_timing.merge(trade_timing);
        audit.special_timing.merge(special_timing);
        audit.trade_rows += trades.len() as u64;
        audit.special_rows += specials.len() as u64;
        audit.invalid_special_rows += invalid_specials;
        let built = build_minutes(
            &grid.contract_id,
            &grid.ric,
            &grid.segments,
            &trades,
            &specials,
            &books,
            multiplier,
        )?;
        if !built.is_empty() {
            audit.output_ric_count += 1;
            rows.extend(built);
        }
    }
    rows.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then(left.contract_id.cmp(&right.contract_id))
    });
    audit.output_rows = rows.len() as u64;
    if audit.output_rows > 0 && !args.audit_only {
        if job.output.exists() && !args.overwrite {
            bail!("refusing to overwrite {}", job.output.display());
        }
        write_parquet_serialized(parquet_lock, &job.output, &rows)?;
        audit.published = true;
    }
    write_json_atomic(&job.audit, &audit)?;
    if audit.output_rows == 0 {
        eprintln!(
            "baseline_skip product={} day={} reason=no_ll2 rics={}",
            job.product, job.day, audit.ric_count
        );
        return Ok(audit);
    }
    eprintln!(
        "baseline_done product={} day={} rics={} rows={} trades={} specials={} invalid_specials={} ll2={} skipped_rics_without_ll2={} elapsed_s={:.2}",
        job.product,
        job.day,
        audit.output_ric_count,
        audit.output_rows,
        audit.trade_rows,
        audit.special_rows,
        audit.invalid_special_rows,
        audit.exact_ll2_minutes,
        audit.rics_without_ll2.len(),
        started.elapsed().as_secs_f64()
    );
    Ok(audit)
}

fn process_trade_only_job(
    job: &Job,
    tas: &DB,
    multipliers: &BTreeMap<String, f64>,
    calendar: &SessionCalendar,
    ll2: Option<&DB>,
    thresholds: Option<raw_baseline::VisibleThresholds>,
    args: &Args,
    parquet_lock: &Mutex<()>,
) -> Result<DayAudit> {
    let started = Instant::now();
    let mut grids = raw_baseline::job_grids(job)?;
    let spec = product_spec(&job.product)?;
    let segments = calendar.segments_for_product(spec, job.day)?;
    let mut audit = DayAudit {
        exchange: job.exchange.clone(),
        product: job.product.clone(),
        trading_day: job.day.to_string(),
        input_file: job.input.display().to_string(),
        output_file: job.output.display().to_string(),
        ric_count: grids.len() as u64,
        ..DayAudit::default()
    };
    if segments.is_empty() {
        audit.calendar_closed = true;
        write_json_atomic(&job.audit, &audit)?;
        eprintln!(
            "baseline_trade_only_skip product={} day={} reason=calendar_closed rics={}",
            job.product, job.day, audit.ric_count
        );
        return Ok(audit);
    }
    audit.threshold_source_max_published_ns = thresholds.and_then(|t| t.source_max_published_ns);
    let thresholds = thresholds
        .map(|t| t.for_open(segments[0].0 as u64 * NS_PER_SEC, args.trade_time_basis))
        .transpose()?;
    let multiplier = *multipliers
        .get(&job.product)
        .with_context(|| format!("missing verified multiplier for {}", job.product))?;
    let source_years = if calendar.history.is_some() {
        historical::source_years(&segments)?
    } else {
        BTreeSet::from([job.day.year()])
    };
    let mut rows = Vec::new();
    for grid in &mut grids {
        grid.segments.clone_from(&segments);
        if let Some(history) = &calendar.history {
            grid.status_segments = history.status_segments(&job.product, &grid.ric, &segments)?;
        }
        let output_minutes = grid_minutes(grid.output_segments());
        audit.grid_minutes += output_minutes;
        audit.status_removed_grid_minutes += grid_minutes(&segments) - output_minutes;
        let mut trades = Vec::new();
        let mut specials = Vec::new();
        for year in &source_years {
            let cf_name = product_cf_name(*year as u16, &job.product)?;
            let cf = tas
                .cf_handle(&cf_name)
                .with_context(|| format!("missing TAS CF {cf_name}"))?;
            let (mut t, outside_session, trade_timing) =
                scan_trades_audited(tas, &cf, grid, Some(job.day), args.trade_time_basis)?;
            let (mut s, invalid_specials, outside_specials, special_timing) =
                scan_specials(tas, &cf, grid, Some(job.day), args.trade_time_basis)?;
            audit.outside_session_trades += outside_session;
            audit.outside_session_specials += outside_specials;
            audit.invalid_special_rows += invalid_specials;
            audit.trade_timing.merge(trade_timing);
            audit.special_timing.merge(special_timing);
            trades.append(&mut t);
            specials.append(&mut s);
        }
        audit.trade_time_basis = args.trade_time_basis;
        audit.trade_rows += trades.len() as u64;
        audit.special_rows += specials.len() as u64;
        let mut built = build_trade_only_minutes(
            &grid.contract_id,
            &grid.ric,
            grid.output_segments(),
            &trades,
            &specials,
            multiplier,
        )?;
        if args.raw_ll2 {
            let ll2 = ll2.context("Raw LL2 not opened")?;
            let cf_name = cme_tas_replay::raw_ll2::raw_cf(job.day.year() as u16, &job.product, 15)?;
            let books = raw_baseline::scan_raw_books(ll2, &cf_name, grid)?;
            audit.exact_ll2_minutes += books.len() as u64;
            for row in &mut built {
                if let Some(book) = books.get(&row.ts) {
                    row.book = book.clone();
                    row.mid_price = book.mid();
                    audit.raw_bidirectional_minutes += u64::from(book.valid());
                    audit.raw_full_depth_minutes += u64::from(raw_baseline::full_depth(book));
                    audit.raw_last_invalid_minutes += u64::from(!book.valid());
                } else {
                    audit.raw_no_snapshot_minutes += 1;
                }
            }
            if books.is_empty() {
                audit.rics_without_ll2.push(grid.ric.clone());
            }
        }
        if let Some(thresholds) = thresholds {
            raw_baseline::apply_size_buckets(&mut built, &trades, multiplier, thresholds)?;
        }
        audit.output_ric_count += 1;
        rows.extend(built);
    }
    rows.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then(left.contract_id.cmp(&right.contract_id))
    });
    audit.output_rows = rows.len() as u64;
    if !args.audit_only {
        if job.output.exists() && !args.overwrite {
            bail!("refusing to overwrite {}", job.output.display());
        }
        write_parquet_serialized(parquet_lock, &job.output, &rows)?;
        audit.published = true;
    }
    write_json_atomic(&job.audit, &audit)?;
    let label = if args.raw_ll2 {
        "baseline_raw_done"
    } else {
        "baseline_trade_only_done"
    };
    eprintln!(
        "{label} product={} day={} rics={} rows={} trades={} specials={} invalid_specials={} elapsed_s={:.2}",
        job.product,
        job.day,
        audit.output_ric_count,
        audit.output_rows,
        audit.trade_rows,
        audit.special_rows,
        audit.invalid_special_rows,
        started.elapsed().as_secs_f64()
    );
    Ok(audit)
}

fn dataframe(rows: &[BaselineMinute]) -> Result<DataFrame> {
    let mut columns = vec![
        Series::new(
            "contract_id".into(),
            rows.iter()
                .map(|row| row.contract_id.clone())
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "ric".into(),
            rows.iter().map(|row| row.ric.clone()).collect::<Vec<_>>(),
        ),
        Series::new(
            "ts".into(),
            rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
        ),
    ];
    macro_rules! opt_col {
        ($name:literal, $field:ident) => {
            columns.push(Series::new(
                $name.into(),
                rows.iter().map(|row| row.$field).collect::<Vec<_>>(),
            ))
        };
    }
    macro_rules! val_col {
        ($name:literal, $field:ident) => {
            columns.push(Series::new(
                $name.into(),
                rows.iter().map(|row| row.$field).collect::<Vec<_>>(),
            ))
        };
    }
    opt_col!("open", open);
    opt_col!("high", high);
    opt_col!("low", low);
    opt_col!("close", close);
    val_col!("volume", volume);
    val_col!("amount", amount);
    val_col!("avg_amount", avg_amount);
    val_col!("count", count);
    val_col!("buy_count", buy_count);
    val_col!("sell_count", sell_count);
    val_col!("buy_amount", buy_amount);
    val_col!("sell_amount", sell_amount);
    val_col!("buy_volume", buy_volume);
    val_col!("sell_volume", sell_volume);
    opt_col!("vwap", vwap);
    opt_col!("buy_vwap", buy_vwap);
    opt_col!("sell_vwap", sell_vwap);
    opt_col!("twap", twap);
    opt_col!("mid_price", mid_price);
    val_col!("net_buy_amount", net_buy_amount);
    val_col!("net_buy_volume", net_buy_volume);
    opt_col!("net_buy_pct", net_buy_pct);
    val_col!("large_order", large_order);
    val_col!("medium_order", medium_order);
    val_col!("small_order", small_order);
    val_col!("large_buy", large_buy);
    val_col!("large_sell", large_sell);
    val_col!("medium_buy", medium_buy);
    val_col!("medium_sell", medium_sell);
    val_col!("small_buy", small_buy);
    val_col!("small_sell", small_sell);
    val_col!("net_buy_large", net_buy_large);
    val_col!("net_buy_medium", net_buy_medium);
    val_col!("net_buy_small", net_buy_small);
    val_col!("special_count", special_count);
    val_col!("special_volume", special_volume);
    val_col!("implied_count", implied_count);
    val_col!("implied_volume", implied_volume);
    val_col!("implied_amount", implied_amount);
    opt_col!("implied_vwap", implied_vwap);
    opt_col!("implied_twap", implied_twap);
    for level in 0..BASELINE_DEPTH_LEVELS {
        columns.push(Series::new(
            format!("bid{level}p").into(),
            rows.iter()
                .map(|row| row.book.bid_prices[level])
                .collect::<Vec<_>>(),
        ));
        columns.push(Series::new(
            format!("bid{level}v").into(),
            rows.iter()
                .map(|row| row.book.bid_sizes[level])
                .collect::<Vec<_>>(),
        ));
    }
    for level in 0..BASELINE_DEPTH_LEVELS {
        columns.push(Series::new(
            format!("ask{level}p").into(),
            rows.iter()
                .map(|row| row.book.ask_prices[level])
                .collect::<Vec<_>>(),
        ));
        columns.push(Series::new(
            format!("ask{level}v").into(),
            rows.iter()
                .map(|row| row.book.ask_sizes[level])
                .collect::<Vec<_>>(),
        ));
    }
    Ok(DataFrame::new(columns)?)
}

fn write_parquet_atomic(path: &Path, rows: &[BaselineMinute]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let mut df = dataframe(rows)?;
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&tmp)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn write_parquet_serialized(lock: &Mutex<()>, path: &Path, rows: &[BaselineMinute]) -> Result<()> {
    let _guard = lock.lock().map_err(|_| anyhow!("parquet lock poisoned"))?;
    let result = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("cme-baseline-parquet".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn_scoped(scope, || write_parquet_atomic(path, rows))
            .map_err(anyhow::Error::from)?
            .join()
            .map_err(|_| anyhow!("parquet writer panicked for {}", path.display()))
    })?;
    result
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    let result = (|| -> Result<()> {
        let file = File::create(&tmp)?;
        serde_json::to_writer_pretty(file, value)?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn load_multipliers(args: &Args, products: &[ProductSpec]) -> Result<BTreeMap<String, f64>> {
    let roots = products
        .iter()
        .map(|p| format!("'{}'", p.product))
        .collect::<Vec<_>>()
        .join(",");
    let from = if args.size_buckets {
        raw_baseline::previous_month_start(args.start)?
    } else {
        args.start
    };
    let until = args.end - Duration::days(1);
    let query = format!("SELECT product_root, exchange, volume_multiple, verified FROM public.cme_research_product_multipliers WHERE product_root IN ({roots}) AND (effective_from IS NULL OR effective_from<=DATE '{from}') AND (effective_to IS NULL OR effective_to>=DATE '{until}') ORDER BY product_root");
    let output = Command::new(&args.psql)
        .args([
            "-h",
            args.pg_socket.to_str().context("PG socket is not UTF-8")?,
            "-p",
            "5433",
            "-U",
            "u171",
            "-d",
            "market_metadata",
            "-At",
            "-F",
            "\t",
            "-c",
            &query,
        ])
        .output()
        .with_context(|| format!("run {}", args.psql.display()))?;
    if !output.status.success() {
        bail!(
            "multiplier query failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut loaded = BTreeMap::new();
    for (index, line) in String::from_utf8(output.stdout)?.lines().enumerate() {
        let fields = line.split('\t').collect::<Vec<_>>();
        if fields.len() != 4 {
            bail!("multiplier row {} has {} fields", index + 1, fields.len());
        }
        let product = fields[0].to_string();
        let expected = product_spec(&product)?;
        if fields[1] != expected.exchange || fields[3] != "t" {
            bail!("unverified or mismatched multiplier row {line:?}");
        }
        let value = fields[2].parse::<f64>()?;
        if !(value.is_finite() && value > 0.0) {
            bail!("invalid multiplier {value} for {product}");
        }
        loaded.insert(product, value);
    }
    for spec in products {
        if !loaded.contains_key(spec.product) {
            bail!("missing verified multiplier for {}", spec.product);
        }
    }
    Ok(loaded)
}

fn tas_options() -> Options {
    let mut options = Options::default();
    options.set_merge_operator_associative("quote_last", cme_tas_replay::product::quote_last_merge);
    options
}

fn ll2_options() -> Options {
    let mut options = Options::default();
    options.set_merge_operator_associative("ll2_latest", ll2_latest_merge);
    options
}

fn open_db(
    primary: &Path,
    secondary: &Path,
    cf_names: &[String],
    direct: bool,
    options: fn() -> Options,
) -> Result<DB> {
    let mut db_options = Options::default();
    db_options.set_max_open_files(512);
    db_options.set_max_file_opening_threads(8);
    db_options.set_skip_stats_update_on_db_open(true);
    let descriptors = cf_names
        .iter()
        .cloned()
        .map(|name| ColumnFamilyDescriptor::new(name, options()))
        .collect::<Vec<_>>();
    if direct {
        return DB::open_cf_descriptors_read_only(&db_options, primary, descriptors, false)
            .with_context(|| format!("open {} read-only", primary.display()));
    }
    if let Some(parent) = secondary.parent() {
        fs::create_dir_all(parent)?;
    }
    let db = DB::open_cf_descriptors_as_secondary(&db_options, primary, secondary, descriptors)
        .with_context(|| {
            format!(
                "open secondary {} from {}",
                secondary.display(),
                primary.display()
            )
        })?;
    db.try_catch_up_with_primary()?;
    Ok(db)
}

fn require_done(db: &DB, period: &str, label: &str) -> Result<()> {
    let cf = db
        .cf_handle(CF_REPLAY_META)
        .context("missing replay_meta")?;
    let key = format!("{PERIOD_META_PREFIX}{period}");
    let value = db
        .get_cf(&cf, key.as_bytes())?
        .with_context(|| format!("{label} period {period} has no watermark"))?;
    if decode_period_status(&value)? != PeriodStatus::Done {
        bail!("{label} period {period} is not done");
    }
    Ok(())
}

fn period_for_year(year: i32) -> Result<String> {
    match year {
        2017..=2025 => Ok(format!("{year:04}-01-01_{:04}-01-01", year + 1)),
        2026 => Ok("2026-01-01_2026-06-01".to_string()),
        _ => bail!("unsupported baseline year {year}"),
    }
}

fn validate_args(args: &Args) -> Result<Vec<ProductSpec>> {
    if args.end <= args.start || args.workers == 0 {
        bail!("invalid date range or worker count");
    }
    if args.session_status.is_some() && args.history_catalog.is_none() {
        bail!("--session-status requires --history-catalog");
    }
    if args.trade_time_basis == TimeBasis::Publication
        && (!args.trade_only || args.output_root == Path::new(DEFAULT_OUTPUT))
    {
        bail!("publication minutes require --trade-only and a separate --output-root");
    }
    if args.history_catalog.is_some()
        && (!args.trade_only
            || !args.tas_contract_grid
            || args.trade_time_basis != TimeBasis::Publication)
    {
        bail!("historical catalog requires --trade-only --tas-contract-grid --trade-time-basis publication");
    }
    let output = args.output_root.to_string_lossy();
    if output.contains("drop_special") || output.contains("rocksdb") {
        bail!(
            "refusing legacy or RocksDB output path {}",
            args.output_root.display()
        );
    }
    let mut seen = BTreeSet::new();
    let mut products = Vec::new();
    for product in &args.products {
        let product = product.trim().to_ascii_uppercase();
        if seen.insert(product.clone()) {
            products.push(product_spec(&product)?);
        }
    }
    if products.is_empty() {
        bail!("no products selected");
    }
    if args.history_catalog.is_none()
        && products
            .iter()
            .any(|p| ADDITIONAL_PRODUCTS.iter().any(|a| a.product == p.product))
    {
        bail!("additional products require --history-catalog");
    }
    if products
        .iter()
        .any(|p| p.schedule_group == "interest_rates")
        && args.history_catalog.is_none()
        && (!(args.raw_ll2 || (args.trade_only && args.trade_time_basis == TimeBasis::Publication))
            || !args.tas_contract_grid
            || args.start < NaiveDate::from_ymd_opt(2026, 1, 1).unwrap()
            || args.end > NaiveDate::from_ymd_opt(2026, 6, 1).unwrap())
    {
        bail!("rates support is scoped to 2026 Jan-May with --tas-contract-grid and either --raw-ll2 or --trade-only --trade-time-basis publication");
    }
    if args.raw_ll2 && !args.tas_contract_grid {
        bail!("--raw-ll2 requires --tas-contract-grid");
    }
    if args.size_buckets && !args.tas_contract_grid {
        bail!("--size-buckets requires --tas-contract-grid");
    }
    Ok(products)
}

fn run() -> Result<()> {
    let args = Arc::new(Args::parse());
    let products = validate_args(&args)?;
    let history = historical::load(&args, &products)?;
    let multipliers = Arc::new(if let Some(h) = &history {
        h.multipliers.clone()
    } else {
        load_multipliers(&args, &products)?
    });
    let mut years =
        (args.start.year()..=(args.end - Duration::days(1)).year()).collect::<BTreeSet<_>>();
    if let Some(h) = &history {
        for day in h.days.values() {
            let segments = day
                .segments
                .iter()
                .map(|&(lo, hi)| ((lo / NS_PER_SEC) as i64, (hi / NS_PER_SEC) as i64 - 60))
                .collect::<Vec<_>>();
            years.extend(historical::source_years(&segments)?);
        }
    }
    let mut selected_cf_names = vec!["default".to_string(), CF_REPLAY_META.to_string()];
    for year in &years {
        for spec in &products {
            selected_cf_names.push(product_cf_name(*year as u16, spec.product)?);
        }
    }
    if args.size_buckets {
        for spec in &products {
            selected_cf_names.push(product_cf_name(
                raw_baseline::previous_month_start(args.start)?.year() as u16,
                spec.product,
            )?);
        }
    }
    selected_cf_names.sort();
    selected_cf_names.dedup();
    let tas_cf_names = DB::list_cf(&Options::default(), &args.tas_rocksdb)
        .with_context(|| format!("list TAS CFs in {}", args.tas_rocksdb.display()))?;
    for name in &selected_cf_names {
        if !tas_cf_names.contains(name) {
            bail!("TAS RocksDB is missing selected CF {name}");
        }
    }
    eprintln!("opening TAS secondary from {}", args.tas_rocksdb.display());
    let tas = Arc::new(open_db(
        &args.tas_rocksdb,
        &args.tas_secondary,
        &tas_cf_names,
        args.direct_read_only,
        tas_options,
    )?);
    let ll2 = if args.trade_only {
        None
    } else {
        eprintln!("opening LL2 secondary from {}", args.ll2_rocksdb.display());
        Some(Arc::new(open_db(
            &args.ll2_rocksdb,
            &args.ll2_secondary,
            &if args.raw_ll2 {
                raw_baseline::raw_cf_names(&args, &products)?
            } else {
                selected_cf_names.clone()
            },
            true,
            if args.raw_ll2 {
                Options::default
            } else {
                ll2_options
            },
        )?))
    };
    for year in &years {
        let period = period_for_year(*year)?;
        require_done(&tas, &period, "TAS")?;
        if let Some(ll2) = &ll2 {
            require_done(ll2, &period, "LL2")?;
        }
    }
    if args.size_buckets {
        require_done(
            &tas,
            &period_for_year(raw_baseline::previous_month_start(args.start)?.year())?,
            "TAS size warmup",
        )?;
    }
    let calendar = (args.trade_only || args.raw_ll2)
        .then(|| {
            if let Some(h) = &history {
                return Ok(SessionCalendar {
                    by_group: BTreeMap::new(),
                    coverage: BTreeMap::new(),
                    history: Some(h.clone()),
                });
            }
            load_calendar(
                &args.psql,
                &args.pg_socket,
                if args.size_buckets {
                    raw_baseline::previous_month_start(args.start).expect("valid month")
                } else {
                    args.start
                },
                args.end,
            )
        })
        .transpose()?
        .map(Arc::new);
    let jobs = if let Some(h) = &history {
        historical::list_jobs(&args, h)?
    } else if args.tas_contract_grid {
        raw_baseline::list_tas_jobs(
            &tas,
            ll2.as_deref().filter(|_| args.raw_ll2),
            &args,
            &products,
            calendar.as_ref().context("calendar")?,
        )?
    } else {
        list_jobs(&args, &products)?
    };
    let thresholds = if args.size_buckets {
        raw_baseline::load_size_thresholds(
            &tas,
            &args,
            &products,
            &multipliers,
            calendar.as_ref().context("calendar")?,
        )?
    } else {
        BTreeMap::new()
    };
    eprintln!(
        "baseline_start jobs={} products={} start={} end={} workers={} output={} audit_only={} trade_only={}",
        jobs.len(),
        products
            .iter()
            .map(|spec| spec.product)
            .collect::<Vec<_>>()
            .join(","),
        args.start,
        args.end,
        args.workers,
        args.output_root.display(),
        args.audit_only,
        args.trade_only
    );
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .stack_size(16 * 1024 * 1024)
        .build()?;
    let parquet_lock = Arc::new(Mutex::new(()));
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| {
                if args.trade_only || args.raw_ll2 {
                    process_trade_only_job(
                        job,
                        &tas,
                        &multipliers,
                        calendar.as_ref().expect("official calendar loaded"),
                        ll2.as_deref(),
                        thresholds
                            .get(&(job.product.clone(), job.day.year(), job.day.month()))
                            .copied(),
                        &args,
                        &parquet_lock,
                    )
                } else {
                    process_job(
                        job,
                        &tas,
                        ll2.as_ref().expect("LL2 opened"),
                        &multipliers,
                        &args,
                        &parquet_lock,
                    )
                }
            })
            .collect::<Vec<_>>()
    });
    let mut files = 0u64;
    let mut rows = 0u64;
    for result in results {
        let audit = result?;
        files += u64::from(audit.published || args.audit_only && audit.output_rows > 0);
        rows += audit.output_rows;
    }
    eprintln!("baseline_complete files={files} rows={rows}");
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("cme_baseline_1min failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_scan_keeps_late_prints_in_new_minutes_and_audits_closures() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf(&options, dir.path(), ["p:2025:FV"])?;
        let cf = db.cf_handle("p:2025:FV").unwrap();
        let source_ns = 1_764_044_430_849_697_706;
        let closed_ns = Chicago
            .with_ymd_and_hms(2025, 11, 24, 16, 30, 0)
            .unwrap()
            .timestamp() as u64
            * NS_PER_SEC;
        let mut trade = cme_tas_replay::ali_h26_fixture();
        trade.ric = "FVZ25".into();
        trade.price = 109_609_375_000;
        trade.exch_hms_ns = 58_963_000_000_000;
        for (published, seq, volume) in [(source_ns, 0, 5000), (source_ns, 1, 7), (closed_ns, 0, 9)]
        {
            trade.ts_utc_ns = published;
            trade.volume = volume;
            db.put_cf(
                &cf,
                encode_all_key(KIND_CME_TRADE, &trade.ric, published, 3, seq)?,
                cme_tas_replay::encode_cme_trade(&trade)?,
            )?;
        }
        trade.price = MISSING_PRICE;
        trade.aggressor = 0;
        db.put_cf(
            &cf,
            encode_all_key(KIND_CME_SPECIAL, &trade.ric, closed_ns, 3, 0)?,
            cme_tas_replay::encode_cme_special(&trade)?,
        )?;
        let day = NaiveDate::from_ymd_opt(2025, 11, 24).unwrap();
        let grid = |d| {
            let (lo, hi) = chicago_window(d).unwrap();
            RicGrid {
                ric: "FVZ25".into(),
                contract_id: "CBOT:FV:2025-12".into(),
                segments: vec![(lo, hi - 3600 - 60)],
                status_segments: None,
            }
        };
        let (old, outside, _) =
            scan_trades_audited(&db, &cf, &grid(day), Some(day), TimeBasis::Publication)?;
        assert!(old.is_empty());
        assert_eq!(outside, 1);
        let (specials, invalid, outside, _) =
            scan_specials(&db, &cf, &grid(day), Some(day), TimeBasis::Publication)?;
        assert!(specials.is_empty());
        assert_eq!((invalid, outside), (0, 1));
        let (next, outside, timing) = scan_trades_audited(
            &db,
            &cf,
            &grid(day + Duration::days(1)),
            Some(day + Duration::days(1)),
            TimeBasis::Publication,
        )?;
        assert_eq!(next.len(), 2);
        assert_eq!(outside, 0);
        assert_eq!(timing.later_minute_volume, 5007);
        assert!(next.iter().all(|t| t.event_ns == source_ns));
        assert_ne!(next[0].source_order, next[1].source_order);
        Ok(())
    }

    #[test]
    fn period_mapping_includes_historical_tas_replays() {
        assert_eq!(period_for_year(2017).unwrap(), "2017-01-01_2018-01-01");
        assert_eq!(period_for_year(2019).unwrap(), "2019-01-01_2020-01-01");
        assert!(period_for_year(2016).is_err());
    }

    #[test]
    fn ambiguous_short_year_ric_uses_the_nearest_unexpired_contract() {
        let contracts = BTreeSet::from([
            "NYMEX:CL:2013-12".to_string(),
            "NYMEX:CL:2023-12".to_string(),
        ]);
        assert_eq!(
            resolve_ambiguous_ric(
                "CLZ3",
                NaiveDate::from_ymd_opt(2018, 1, 2).unwrap(),
                contracts,
            )
            .unwrap(),
            "NYMEX:CL:2023-12"
        );
    }

    #[test]
    fn ll2_jobs_are_not_excluded_by_old_coverage_policy() {
        let root = tempfile::tempdir().unwrap();
        for product in ["ES", "YM", "GC"] {
            let spec = product_spec(product).unwrap();
            let dir = root.path().join(spec.exchange).join(product);
            fs::create_dir_all(&dir).unwrap();
            for day in ["20240711", "20250102", "20250415", "20260102"] {
                fs::write(dir.join(format!("{day}.parquet")), b"").unwrap();
            }
        }
        let args = Args::parse_from([
            "cme_baseline_1min",
            "--summary-root",
            root.path().to_str().unwrap(),
            "--output-root",
            root.path().join("out").to_str().unwrap(),
            "--start",
            "2024-01-01",
            "--end",
            "2026-06-01",
            "--products",
            "ES,YM,GC",
        ]);
        let jobs = list_jobs(&args, &validate_args(&args).unwrap()).unwrap();
        assert_eq!(jobs.len(), 12);
        assert_eq!(jobs.iter().filter(|j| j.day.year() == 2025).count(), 6);
    }

    #[test]
    fn thirty_minute_gaps_become_separate_ll2_segments() {
        assert_eq!(
            segments_from_book_minutes(vec![0, 60, 29 * 60, 59 * 60]).unwrap(),
            vec![(0, 29 * 60), (59 * 60, 59 * 60)]
        );
    }
}
