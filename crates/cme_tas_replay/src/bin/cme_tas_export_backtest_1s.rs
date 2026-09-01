//! Six-product dense 1s backtest directly from the all-product TAS RocksDB.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::America::Chicago;
use clap::Parser;
use cme_tas_replay::backtest_1s::{densify_interval, BacktestRow, Interval, Quote, Trade};
use cme_tas_replay::product::{encode_all_key, exch_event_time_ns, quote_last_merge, ALL_KEY_LEN};
use cme_tas_replay::{
    decode_cme_quote, decode_cme_special, decode_cme_trade, decode_period_status, price_e9_to_f64,
    PeriodStatus, CF_REPLAY_META, KIND_CME_QUOTE, KIND_CME_SPECIAL, KIND_CME_TRADE,
    MISSING_EXCH_HMS_NS, MISSING_VOLUME, PERIOD_META_PREFIX,
};
use polars::prelude::{DataFrame, NamedFrom, ParquetCompression, ParquetWriter, Series};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const DEFAULT_DB: &str = "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_rocksdb_all_products";
const DEFAULT_SECONDARY: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/lseg_data/cme_tas_backtest_1s.secondary";
const DEFAULT_OUT: &str = "/mnt/hdd-raid5-72t/liang_torch/lseg_data/backtest_1s";
const DEFAULT_PSQL: &str = "/mnt/nvme-raid0-28t/apps/pgsql16/bin/psql";
const DEFAULT_OVERRIDES: &str = "config/cme_tas_backtest_session_overrides.json";
const NS_PER_SEC: u64 = 1_000_000_000;
const HALF_DAY_NS: u64 = 43_200 * NS_PER_SEC;

#[derive(Parser, Debug)]
#[command(name = "cme_tas_export_backtest_1s")]
struct Args {
    #[arg(long, default_value = DEFAULT_DB)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_SECONDARY)]
    secondary_dir: PathBuf,
    /// Open a completed RocksDB directly in read-only mode; no writer may exist.
    #[arg(long)]
    direct_read_only: bool,
    #[arg(long, default_value = DEFAULT_OUT)]
    out_root: PathBuf,
    #[arg(long)]
    start: NaiveDate,
    #[arg(long)]
    end: NaiveDate,
    #[arg(long, default_value = "ES,NQ,RTY,YM,GC,CL")]
    products: String,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    audit_only: bool,
    /// After reviewing audit JSON, filter off-session quotes instead of failing.
    #[arg(long)]
    allow_off_session_quotes: bool,
    #[arg(long, default_value = DEFAULT_PSQL)]
    psql: PathBuf,
    #[arg(long, default_value = DEFAULT_OVERRIDES)]
    session_overrides: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionOverrideFile {
    events: Vec<SessionOverride>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionOverride {
    product: String,
    trad_day: NaiveDate,
    kind: String,
    event_ts: i64,
    ric: Option<String>,
    reason: String,
    action: String,
    source: String,
}

impl SessionOverrideFile {
    fn load(path: &Path) -> Result<Self> {
        let parsed: Self = serde_json::from_slice(
            &fs::read(path).with_context(|| format!("read {}", path.display()))?,
        )
        .with_context(|| format!("parse {}", path.display()))?;
        for event in &parsed.events {
            if event.action != "filter" || event.source.trim().is_empty() {
                bail!(
                    "invalid session override for {} {}",
                    event.product,
                    event.trad_day
                );
            }
        }
        Ok(parsed)
    }

    fn reviewed(
        &self,
        product: &str,
        day: NaiveDate,
        ric: &str,
        kind: &str,
        event_ts: i64,
        reason: &str,
    ) -> bool {
        self.events.iter().any(|event| {
            event.product == product
                && event.trad_day == day
                && event.kind == kind
                && event.event_ts == event_ts
                && event.reason == reason
                && event.ric.as_deref().is_none_or(|wanted| wanted == ric)
        })
    }
}

#[derive(Clone, Copy)]
struct ProductSpec {
    product: &'static str,
    exchange: &'static str,
    schedule_group: &'static str,
    month_codes: &'static [u8],
}

const PRODUCTS: &[ProductSpec] = &[
    ProductSpec {
        product: "ES",
        exchange: "CME",
        schedule_group: "equity_indices",
        month_codes: b"HMUZ",
    },
    ProductSpec {
        product: "NQ",
        exchange: "CME",
        schedule_group: "equity_indices",
        month_codes: b"HMUZ",
    },
    ProductSpec {
        product: "RTY",
        exchange: "CME",
        schedule_group: "equity_indices",
        month_codes: b"HMUZ",
    },
    ProductSpec {
        product: "YM",
        exchange: "CBOT",
        schedule_group: "equity_indices",
        month_codes: b"HMUZ",
    },
    ProductSpec {
        product: "GC",
        exchange: "COMEX",
        schedule_group: "metals",
        month_codes: b"FGHJKMNQUVXZ",
    },
    ProductSpec {
        product: "CL",
        exchange: "NYMEX",
        schedule_group: "energy",
        month_codes: b"FGHJKMNQUVXZ",
    },
];

fn product_spec(name: &str) -> Result<ProductSpec> {
    PRODUCTS
        .iter()
        .copied()
        .find(|spec| spec.product == name)
        .ok_or_else(|| anyhow!("unsupported backtest product {name:?}"))
}

fn month_number(code: u8) -> Result<u32> {
    b"FGHJKMNQUVXZ"
        .iter()
        .position(|candidate| *candidate == code)
        .map(|index| index as u32 + 1)
        .ok_or_else(|| anyhow!("invalid month code {}", char::from(code)))
}

#[derive(Clone)]
struct Contract {
    ric: String,
    contract_id: String,
}

fn candidate_contracts(spec: ProductSpec, period_year: i32) -> Result<Vec<Contract>> {
    let mut out = Vec::new();
    for year in period_year - 1..=period_year + 5 {
        for &month_code in spec.month_codes {
            let month = month_number(month_code)?;
            let year_suffix = if year < 2024 {
                (year % 10).to_string()
            } else {
                format!("{:02}", year % 100)
            };
            let ric = format!("{}{}{}", spec.product, char::from(month_code), year_suffix);
            out.push(Contract {
                ric,
                contract_id: format!("{}:{}:{year:04}-{month:02}", spec.exchange, spec.product),
            });
        }
    }
    Ok(out)
}

#[derive(Clone, Copy)]
struct RawInterval {
    start: i64,
    end: i64,
}

struct SessionCalendar {
    by_group: BTreeMap<String, Vec<RawInterval>>,
    known_outages: BTreeMap<String, Vec<RawInterval>>,
    coverage: BTreeMap<String, (NaiveDate, NaiveDate)>,
}

struct SourcedInterval {
    interval: RawInterval,
    exception_name: String,
}

fn infer_known_outage_gaps(rows: &mut [SourcedInterval]) -> Vec<RawInterval> {
    rows.sort_by_key(|row| (row.interval.start, row.interval.end));
    rows.windows(2)
        .filter_map(|pair| {
            let left = &pair[0];
            let right = &pair[1];
            (left.interval.end < right.interval.start
                && left.exception_name == right.exception_name
                && left.exception_name.starts_with("known_outage:"))
            .then_some(RawInterval {
                start: left.interval.end,
                end: right.interval.start,
            })
        })
        .collect()
}

fn chicago_window(day: NaiveDate) -> Result<(i64, i64)> {
    let end = Chicago
        .from_local_datetime(&day.and_hms_opt(17, 0, 0).expect("valid clock"))
        .single()
        .ok_or_else(|| anyhow!("ambiguous Chicago 17:00 on {day}"))?;
    Ok(((end - Duration::days(1)).timestamp(), end.timestamp()))
}

impl SessionCalendar {
    fn intervals_for(&self, group: &str, day: NaiveDate) -> Result<Vec<Interval>> {
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
                (interval.start < window_end && interval.end > window_start).then_some(Interval {
                    start: interval.start.max(window_start),
                    end: interval.end.min(window_end),
                })
            })
            .collect::<Vec<_>>();
        clipped.sort_by_key(|interval| (interval.start, interval.end));
        let mut merged: Vec<Interval> = Vec::new();
        for interval in clipped {
            if let Some(previous) = merged.last_mut() {
                if interval.start <= previous.end {
                    previous.end = previous.end.max(interval.end);
                    continue;
                }
            }
            merged.push(interval);
        }
        Ok(merged)
    }

    fn known_outages_for(&self, group: &str, day: NaiveDate) -> Result<Vec<Interval>> {
        let (window_start, window_end) = chicago_window(day)?;
        Ok(self
            .known_outages
            .get(group)
            .into_iter()
            .flatten()
            .filter_map(|interval| {
                (interval.start < window_end && interval.end > window_start).then_some(Interval {
                    start: interval.start.max(window_start),
                    end: interval.end.min(window_end),
                })
            })
            .collect())
    }
}

fn load_calendar(psql: &Path, start: NaiveDate, end: NaiveDate) -> Result<SessionCalendar> {
    let query_start = start - Duration::days(2);
    let query_end = end + Duration::days(2);
    let sql = format!(
        "SELECT schedule_group, utc_date, CASE WHEN is_trading THEN 1 ELSE 0 END, \
         COALESCE(extract(epoch FROM open_utc)::bigint::text,''), \
         COALESCE(extract(epoch FROM close_utc)::bigint::text,''), exception_name \
         FROM public.cme_globex_daily_trading_intervals \
         WHERE schedule_group IN ('equity_indices','metals','energy') \
         AND utc_date BETWEEN DATE '{query_start}' AND DATE '{query_end}' \
         ORDER BY schedule_group, utc_date, interval_index"
    );
    let output = Command::new(psql)
        .args([
            "-h",
            "/mnt/nvme-raid0-28t/postgresql/domestic_futures/16/run",
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
    if !output.status.success() {
        bail!(
            "calendar psql failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let mut by_group: BTreeMap<String, Vec<RawInterval>> = BTreeMap::new();
    let mut sourced_by_group: BTreeMap<String, Vec<SourcedInterval>> = BTreeMap::new();
    let mut coverage: BTreeMap<String, (NaiveDate, NaiveDate)> = BTreeMap::new();
    for (line_no, line) in String::from_utf8(output.stdout)?.lines().enumerate() {
        let cells = line.split('\t').collect::<Vec<_>>();
        if cells.len() != 6 {
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
        by_group.entry(group.clone()).or_default().push(interval);
        sourced_by_group
            .entry(group)
            .or_default()
            .push(SourcedInterval {
                interval,
                exception_name: cells[5].to_string(),
            });
    }
    let mut known_outages: BTreeMap<String, Vec<RawInterval>> = BTreeMap::new();
    for (group, rows) in &mut sourced_by_group {
        let gaps = infer_known_outage_gaps(rows);
        if !gaps.is_empty() {
            known_outages.insert(group.clone(), gaps);
        }
    }
    Ok(SessionCalendar {
        by_group,
        known_outages,
        coverage,
    })
}

fn cf_options() -> Options {
    let mut options = Options::default();
    options.set_merge_operator_associative("quote_last", quote_last_merge);
    options
}

fn open_input_db(args: &Args, products: &[ProductSpec], period_year: i32) -> Result<DB> {
    let mut names = vec!["default".to_string(), "replay_meta".to_string()];
    names.extend(
        products
            .iter()
            .map(|spec| format!("p:{period_year}:{}", spec.product)),
    );
    let descriptors = names
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, cf_options()))
        .collect::<Vec<_>>();
    if args.direct_read_only {
        return DB::open_cf_descriptors_read_only(
            &Options::default(),
            &args.rocksdb_dir,
            descriptors,
            false,
        )
        .with_context(|| {
            format!(
                "open completed RocksDB {} read-only",
                args.rocksdb_dir.display()
            )
        });
    }
    if let Some(parent) = args.secondary_dir.parent() {
        fs::create_dir_all(parent)?;
    }
    DB::open_cf_descriptors_as_secondary(
        &Options::default(),
        &args.rocksdb_dir,
        &args.secondary_dir,
        descriptors,
    )
    .with_context(|| {
        format!(
            "open secondary {} from {}",
            args.secondary_dir.display(),
            args.rocksdb_dir.display()
        )
    })
}

fn require_completed_source_period(db: &DB, period_year: i32) -> Result<String> {
    let cf = db
        .cf_handle(CF_REPLAY_META)
        .ok_or_else(|| anyhow!("missing column family {CF_REPLAY_META}"))?;
    let prefix = format!("{PERIOD_META_PREFIX}{period_year:04}-01-01_");
    let mut completed = Vec::new();
    for item in db.iterator_cf(
        &cf,
        IteratorMode::From(prefix.as_bytes(), Direction::Forward),
    ) {
        let (key, value) = item?;
        if !key.starts_with(prefix.as_bytes()) {
            break;
        }
        let period = std::str::from_utf8(&key)?
            .strip_prefix(PERIOD_META_PREFIX)
            .expect("period prefix checked")
            .to_string();
        match decode_period_status(&value)? {
            PeriodStatus::Done => completed.push(period),
            PeriodStatus::Writing => {
                bail!("source period {period} is still marked writing; refuse export")
            }
        }
    }
    match completed.as_slice() {
        [period] => Ok(period.clone()),
        [] => bail!("no completed source period starts in {period_year}"),
        periods => bail!(
            "multiple completed source periods start in {period_year}: {}",
            periods.join(",")
        ),
    }
}

fn key_prefix(kind: u8, ric: &str) -> Result<Vec<u8>> {
    Ok(encode_all_key(kind, ric, 0, 0, 0)?[..17].to_vec())
}

fn key_ts_ns(key: &[u8]) -> Result<u64> {
    if key.len() != ALL_KEY_LEN {
        bail!(
            "all-product key length is {}, expected {ALL_KEY_LEN}",
            key.len()
        );
    }
    Ok(u64::from_be_bytes(key[17..25].try_into().unwrap()))
}

fn seek_key(kind: u8, ric: &str, ts_ns: u64) -> Result<[u8; ALL_KEY_LEN]> {
    encode_all_key(kind, ric, ts_ns, 0, 0)
}

fn in_interval(sec: i64, intervals: &[Interval]) -> bool {
    intervals
        .iter()
        .any(|interval| sec >= interval.start && sec < interval.end)
}

fn at_close(sec: i64, intervals: &[Interval]) -> bool {
    intervals.iter().any(|interval| sec == interval.end)
}

fn event_time_ns(date_time_ns: u64, exch_hms_ns: u64) -> Result<(u64, bool)> {
    if exch_hms_ns == MISSING_EXCH_HMS_NS {
        return Ok((date_time_ns, true));
    }
    Ok((exch_event_time_ns(date_time_ns, exch_hms_ns)?, false))
}

#[derive(Default)]
struct RicEvents {
    quotes: Vec<Quote>,
    trades: Vec<Trade>,
    quote_rows: u64,
    quote_kept: u64,
    quote_fallback: u64,
    quote_source_two_sided: u64,
    quote_source_one_sided: u64,
    quote_crossed_after_overlay: u64,
    trade_rows: u64,
    trade_kept: u64,
    trade_fallback: u64,
    special_rows: u64,
    special_kept: u64,
    special_fallback: u64,
    off_session_quotes: u64,
    off_session_trades: u64,
    off_session_specials: u64,
    reviewed_off_session_trades: u64,
    reviewed_off_session_specials: u64,
    invalid_trade_prices: u64,
    conflict_counts: BTreeMap<String, u64>,
    samples: Vec<ConflictSample>,
    off_session_trade_samples: Vec<ConflictSample>,
    off_session_special_samples: Vec<ConflictSample>,
    invalid_trade_price_samples: Vec<ConflictSample>,
    crossed_samples: Vec<CrossedBookSample>,
}

#[derive(Clone, Serialize)]
struct ConflictSample {
    ric: String,
    kind: String,
    reason: String,
    ts: i64,
    source_date_time_ns: Option<u64>,
    exch_hms_ns: Option<u64>,
    price: Option<f64>,
    volume: Option<u32>,
    aggressor: Option<u8>,
    reviewed: bool,
}

#[derive(Clone, Serialize)]
struct CrossedBookSample {
    ric: String,
    ts: i64,
    standing_bid: f64,
    standing_ask: f64,
    source_bid: Option<f64>,
    source_ask: Option<f64>,
}

fn conflict_reason(
    day: NaiveDate,
    sec: i64,
    intervals: &[Interval],
    known_outages: &[Interval],
) -> &'static str {
    if in_interval(sec, known_outages) {
        return "known_outage";
    }
    if matches!(day.weekday(), Weekday::Sat | Weekday::Sun) && intervals.is_empty() {
        return "weekend";
    }
    if intervals.is_empty() {
        return "holiday_closed";
    }
    let local = Utc
        .timestamp_opt(sec, 0)
        .single()
        .expect("unix timestamp")
        .with_timezone(&Chicago);
    if local.hour() == 16 {
        return "maintenance";
    }
    if sec < intervals[0].start {
        return "before_open";
    }
    if sec >= intervals.last().expect("not empty").end {
        let close_local = Utc
            .timestamp_opt(intervals.last().unwrap().end, 0)
            .single()
            .unwrap()
            .with_timezone(&Chicago);
        return if close_local.hour() < 16 {
            "after_early_close"
        } else {
            "after_close"
        };
    }
    "between_intervals"
}

fn record_conflict(
    out: &mut RicEvents,
    ric: &str,
    kind: &str,
    day: NaiveDate,
    sec: i64,
    intervals: &[Interval],
    known_outages: &[Interval],
    source_date_time_ns: Option<u64>,
    exch_hms_ns: Option<u64>,
    price: Option<f64>,
    volume: Option<u32>,
    aggressor: Option<u8>,
    reviewed: bool,
) {
    let reason = conflict_reason(day, sec, intervals, known_outages);
    *out.conflict_counts
        .entry(format!("{kind}:{reason}"))
        .or_insert(0) += 1;
    if out
        .samples
        .iter()
        .filter(|sample| sample.kind == kind)
        .count()
        < 10
    {
        out.samples.push(ConflictSample {
            ric: ric.to_string(),
            kind: kind.to_string(),
            reason: reason.to_string(),
            ts: sec,
            source_date_time_ns,
            exch_hms_ns,
            price,
            volume,
            aggressor,
            reviewed,
        });
    }
}

fn record_invalid_trade_price(
    out: &mut RicEvents,
    ric: &str,
    sec: i64,
    source_date_time_ns: u64,
    exch_hms_ns: u64,
    price: Option<f64>,
    volume: u32,
    aggressor: u8,
) {
    const KIND: &str = "trade";
    const REASON: &str = "invalid_trade_price";
    *out.conflict_counts
        .entry(format!("{KIND}:{REASON}"))
        .or_insert(0) += 1;
    out.invalid_trade_price_samples.push(ConflictSample {
        ric: ric.to_string(),
        kind: KIND.to_string(),
        reason: REASON.to_string(),
        ts: sec,
        source_date_time_ns: Some(source_date_time_ns),
        exch_hms_ns: Some(exch_hms_ns),
        price,
        volume: Some(volume),
        aggressor: Some(aggressor),
        reviewed: true,
    });
}

fn scan_quote(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    ric: &str,
    window_start: i64,
    window_end: i64,
    day: NaiveDate,
    intervals: &[Interval],
    known_outages: &[Interval],
    out: &mut RicEvents,
) -> Result<()> {
    let prefix = key_prefix(KIND_CME_QUOTE, ric)?;
    let seek = seek_key(KIND_CME_QUOTE, ric, (window_start as u64) * NS_PER_SEC)?;
    let mut active_interval: Option<usize> = None;
    let mut standing_bid: Option<(f64, f64)> = None;
    let mut standing_ask: Option<(f64, f64)> = None;
    for item in db.iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(&prefix) {
            break;
        }
        let sec = (key_ts_ns(&key)? / NS_PER_SEC) as i64;
        if sec >= window_end {
            break;
        }
        out.quote_rows += 1;
        let rec = decode_cme_quote(&value)?;
        out.quote_fallback += u64::from(rec.exch_hms_ns == MISSING_EXCH_HMS_NS);
        let bid = price_e9_to_f64(rec.bid).and_then(|price| {
            (rec.bid_size != MISSING_VOLUME && price > 0.0)
                .then_some((price, f64::from(rec.bid_size)))
        });
        let ask = price_e9_to_f64(rec.ask).and_then(|price| {
            (rec.ask_size != MISSING_VOLUME && price > 0.0)
                .then_some((price, f64::from(rec.ask_size)))
        });
        out.quote_source_two_sided += u64::from(bid.is_some() && ask.is_some());
        out.quote_source_one_sided += u64::from(bid.is_some() ^ ask.is_some());

        let interval_index = intervals
            .iter()
            .position(|interval| sec >= interval.start && sec < interval.end);
        if let Some(interval_index) = interval_index {
            if active_interval != Some(interval_index) {
                active_interval = Some(interval_index);
                standing_bid = None;
                standing_ask = None;
            }
            if bid.is_some() {
                standing_bid = bid;
            }
            if ask.is_some() {
                standing_ask = ask;
            }
            let (Some((bid, bid_size)), Some((ask, ask_size))) = (standing_bid, standing_ask)
            else {
                continue;
            };
            let quote = Quote {
                sec,
                bid,
                bid_size,
                ask,
                ask_size,
            };
            if !quote.valid() {
                out.quote_crossed_after_overlay += 1;
                if out.crossed_samples.len() < 10 {
                    out.crossed_samples.push(CrossedBookSample {
                        ric: ric.to_string(),
                        ts: sec,
                        standing_bid: bid,
                        standing_ask: ask,
                        source_bid: price_e9_to_f64(rec.bid),
                        source_ask: price_e9_to_f64(rec.ask),
                    });
                }
                continue;
            }
            out.quotes.push(quote);
            out.quote_kept += 1;
        } else {
            active_interval = None;
            standing_bid = None;
            standing_ask = None;
            out.off_session_quotes += 1;
            record_conflict(
                out,
                ric,
                "quote",
                day,
                sec,
                intervals,
                known_outages,
                Some(rec.ts_utc_ns),
                Some(rec.exch_hms_ns),
                None,
                None,
                None,
                false,
            );
        }
    }
    Ok(())
}

fn scan_trades(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    ric: &str,
    window_start: i64,
    window_end: i64,
    day: NaiveDate,
    product: &str,
    intervals: &[Interval],
    known_outages: &[Interval],
    overrides: &SessionOverrideFile,
    out: &mut RicEvents,
) -> Result<()> {
    let padded_start = ((window_start as u64) * NS_PER_SEC).saturating_sub(HALF_DAY_NS);
    let padded_end = (window_end as u64)
        .saturating_mul(NS_PER_SEC)
        .saturating_add(HALF_DAY_NS);
    let prefix = key_prefix(KIND_CME_TRADE, ric)?;
    let seek = seek_key(KIND_CME_TRADE, ric, padded_start)?;
    for item in db.iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(&prefix) {
            break;
        }
        if key_ts_ns(&key)? > padded_end {
            break;
        }
        let rec = decode_cme_trade(&value)?;
        let (event_ns, fallback) = event_time_ns(rec.ts_utc_ns, rec.exch_hms_ns)?;
        let sec = (event_ns / NS_PER_SEC) as i64;
        if sec < window_start || sec >= window_end {
            continue;
        }
        out.trade_rows += 1;
        out.trade_fallback += u64::from(fallback);
        let price = price_e9_to_f64(rec.price);
        if price.is_none_or(|price| !price.is_finite() || price <= 0.0) {
            out.invalid_trade_prices += 1;
            record_invalid_trade_price(
                out,
                ric,
                sec,
                rec.ts_utc_ns,
                rec.exch_hms_ns,
                price,
                rec.volume,
                rec.aggressor,
            );
            continue;
        }
        let price = price.expect("positive finite price checked above");
        if rec.volume == MISSING_VOLUME || rec.volume == 0 {
            bail!("printable trade {ric} has invalid volume {}", rec.volume);
        }
        if in_interval(sec, intervals) || at_close(sec, intervals) {
            out.trades.push(Trade {
                event_ns: i64::try_from(event_ns)?,
                price,
                aggressor: rec.aggressor,
            });
            out.trade_kept += 1;
        } else {
            out.off_session_trades += 1;
            let reason = conflict_reason(day, sec, intervals, known_outages);
            let reviewed = reason == "known_outage"
                || overrides.reviewed(product, day, ric, "trade", sec, reason);
            out.reviewed_off_session_trades += u64::from(reviewed);
            out.off_session_trade_samples.push(ConflictSample {
                ric: ric.to_string(),
                kind: "trade".to_string(),
                reason: reason.to_string(),
                ts: sec,
                source_date_time_ns: Some(rec.ts_utc_ns),
                exch_hms_ns: Some(rec.exch_hms_ns),
                price: Some(price),
                volume: Some(rec.volume),
                aggressor: Some(rec.aggressor),
                reviewed,
            });
            record_conflict(
                out,
                ric,
                "trade",
                day,
                sec,
                intervals,
                known_outages,
                Some(rec.ts_utc_ns),
                Some(rec.exch_hms_ns),
                Some(price),
                Some(rec.volume),
                Some(rec.aggressor),
                reviewed,
            );
        }
    }
    Ok(())
}

fn scan_specials(
    db: &DB,
    cf: &rocksdb::ColumnFamily,
    ric: &str,
    window_start: i64,
    window_end: i64,
    day: NaiveDate,
    product: &str,
    intervals: &[Interval],
    known_outages: &[Interval],
    overrides: &SessionOverrideFile,
    out: &mut RicEvents,
) -> Result<()> {
    let padded_start = ((window_start as u64) * NS_PER_SEC).saturating_sub(HALF_DAY_NS);
    let padded_end = (window_end as u64)
        .saturating_mul(NS_PER_SEC)
        .saturating_add(HALF_DAY_NS);
    let prefix = key_prefix(KIND_CME_SPECIAL, ric)?;
    let seek = seek_key(KIND_CME_SPECIAL, ric, padded_start)?;
    for item in db.iterator_cf(cf, IteratorMode::From(&seek, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(&prefix) {
            break;
        }
        if key_ts_ns(&key)? > padded_end {
            break;
        }
        let rec = decode_cme_special(&value)?;
        let (event_ns, fallback) = event_time_ns(rec.ts_utc_ns, rec.exch_hms_ns)?;
        let sec = (event_ns / NS_PER_SEC) as i64;
        if sec < window_start || sec >= window_end {
            continue;
        }
        out.special_rows += 1;
        out.special_fallback += u64::from(fallback);
        if in_interval(sec, intervals) {
            out.special_kept += 1;
        } else {
            out.off_session_specials += 1;
            let reason = conflict_reason(day, sec, intervals, known_outages);
            let reviewed = reason == "known_outage"
                || overrides.reviewed(product, day, ric, "special", sec, reason);
            out.reviewed_off_session_specials += u64::from(reviewed);
            out.off_session_special_samples.push(ConflictSample {
                ric: ric.to_string(),
                kind: "special".to_string(),
                reason: reason.to_string(),
                ts: sec,
                source_date_time_ns: Some(rec.ts_utc_ns),
                exch_hms_ns: Some(rec.exch_hms_ns),
                price: None,
                volume: Some(rec.volume),
                aggressor: None,
                reviewed,
            });
            record_conflict(
                out,
                ric,
                "special",
                day,
                sec,
                intervals,
                known_outages,
                Some(rec.ts_utc_ns),
                Some(rec.exch_hms_ns),
                None,
                Some(rec.volume),
                None,
                reviewed,
            );
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct DayAudit {
    product: String,
    trad_day: String,
    intervals: Vec<[i64; 2]>,
    known_outages: Vec<[i64; 2]>,
    quote_rows: u64,
    quote_kept: u64,
    quote_time_fallback: u64,
    quote_source_two_sided: u64,
    quote_source_one_sided: u64,
    quote_crossed_after_overlay: u64,
    trade_rows: u64,
    trade_kept: u64,
    trade_time_fallback: u64,
    special_rows: u64,
    special_kept: u64,
    special_time_fallback: u64,
    off_session_quotes: u64,
    off_session_trades: u64,
    off_session_specials: u64,
    reviewed_off_session_trades: u64,
    reviewed_off_session_specials: u64,
    invalid_trade_prices: u64,
    output_rows: usize,
    excluded_crossed_rics: Vec<String>,
    conflict_counts: BTreeMap<String, u64>,
    conflicts: Vec<ConflictSample>,
    off_session_trade_samples: Vec<ConflictSample>,
    off_session_special_samples: Vec<ConflictSample>,
    invalid_trade_price_samples: Vec<ConflictSample>,
    crossed_book_samples: Vec<CrossedBookSample>,
}

fn write_json_atomic(path: &Path, value: &DayAudit) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, serde_json::to_vec_pretty(value)?)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

fn rows_to_dataframe(rows: &[BacktestRow]) -> Result<DataFrame> {
    Ok(DataFrame::new(vec![
        Series::new(
            "contract_id".into(),
            rows.iter()
                .map(|row| row.contract_id.as_str())
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "ric".into(),
            rows.iter().map(|row| row.ric.as_str()).collect::<Vec<_>>(),
        ),
        Series::new(
            "ts".into(),
            rows.iter().map(|row| row.ts).collect::<Vec<_>>(),
        ),
        Series::new(
            "bid0p".into(),
            rows.iter().map(|row| row.bid0p).collect::<Vec<_>>(),
        ),
        Series::new(
            "bid0v".into(),
            rows.iter().map(|row| row.bid0v).collect::<Vec<_>>(),
        ),
        Series::new(
            "ask0p".into(),
            rows.iter().map(|row| row.ask0p).collect::<Vec<_>>(),
        ),
        Series::new(
            "ask0v".into(),
            rows.iter().map(|row| row.ask0v).collect::<Vec<_>>(),
        ),
        Series::new(
            "buy_high".into(),
            rows.iter().map(|row| row.buy_high).collect::<Vec<_>>(),
        ),
        Series::new(
            "sell_low".into(),
            rows.iter().map(|row| row.sell_low).collect::<Vec<_>>(),
        ),
        Series::new(
            "close".into(),
            rows.iter().map(|row| row.close).collect::<Vec<_>>(),
        ),
        Series::new(
            "midp".into(),
            rows.iter().map(|row| row.midp).collect::<Vec<_>>(),
        ),
    ])?)
}

fn write_parquet_atomic(path: &Path, rows: &[BacktestRow]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let mut df = rows_to_dataframe(rows)?;
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&tmp)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut df)
            .with_context(|| format!("write parquet {} rows={}", path.display(), rows.len()))?;
        fs::rename(&tmp, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp);
    }
    result
}

fn write_parquet_serialized(
    parquet_writer: &Mutex<()>,
    path: &Path,
    rows: &[BacktestRow],
) -> Result<()> {
    let _writer_guard = parquet_writer
        .lock()
        .map_err(|_| anyhow!("parquet writer lock is poisoned"))?;
    let write_result =
        std::thread::scope(|scope| scope.spawn(|| write_parquet_atomic(path, rows)).join());
    match write_result {
        Ok(result) => result,
        Err(_) => bail!("parquet writer thread panicked for {}", path.display()),
    }
}

fn estimated_dense_rows(intervals: &[Interval], quotes: &[Quote], trades: &[Trade]) -> usize {
    intervals
        .iter()
        .map(|interval| {
            let Some(first_quote) = quotes
                .iter()
                .filter(|quote| quote.sec >= interval.start && quote.sec < interval.end)
                .map(|quote| quote.sec)
                .min()
            else {
                return 0;
            };
            let dense = (interval.end - first_quote - 1).max(0) as usize;
            let closing = usize::from(
                trades
                    .iter()
                    .any(|trade| trade.event_ns.div_euclid(1_000_000_000) == interval.end),
            );
            dense + closing
        })
        .sum()
}

#[derive(Clone)]
struct Job {
    spec: ProductSpec,
    day: NaiveDate,
}

fn run_job(
    db: &DB,
    calendar: &SessionCalendar,
    args: &Args,
    overrides: &SessionOverrideFile,
    parquet_writer: &Mutex<()>,
    job: &Job,
    period_year: i32,
) -> Result<(usize, bool)> {
    let output_path = args
        .out_root
        .join(job.spec.exchange)
        .join(job.spec.product)
        .join(format!("{}.parquet", job.day.format("%Y%m%d")));
    let publish_parquet = !args.audit_only && (args.overwrite || !output_path.exists());
    let intervals = calendar.intervals_for(job.spec.schedule_group, job.day)?;
    let known_outages = calendar.known_outages_for(job.spec.schedule_group, job.day)?;
    let (window_start, window_end) = chicago_window(job.day)?;
    let cf_name = format!("p:{period_year}:{}", job.spec.product);
    let cf = db
        .cf_handle(&cf_name)
        .ok_or_else(|| anyhow!("missing column family {cf_name}"))?;
    let mut all_rows = Vec::new();
    let mut estimated_rows = 0usize;
    let mut excluded_crossed_rics = Vec::new();
    let mut total = RicEvents::default();
    for contract in candidate_contracts(job.spec, period_year)? {
        let mut events = RicEvents::default();
        scan_quote(
            db,
            cf,
            &contract.ric,
            window_start,
            window_end,
            job.day,
            &intervals,
            &known_outages,
            &mut events,
        )?;
        scan_trades(
            db,
            cf,
            &contract.ric,
            window_start,
            window_end,
            job.day,
            job.spec.product,
            &intervals,
            &known_outages,
            overrides,
            &mut events,
        )?;
        scan_specials(
            db,
            cf,
            &contract.ric,
            window_start,
            window_end,
            job.day,
            job.spec.product,
            &intervals,
            &known_outages,
            overrides,
            &mut events,
        )?;
        if events.quote_crossed_after_overlay > 0 {
            excluded_crossed_rics.push(contract.ric.clone());
        }
        for interval in &intervals {
            if events.quote_crossed_after_overlay > 0 {
                continue;
            } else if !publish_parquet {
                estimated_rows += estimated_dense_rows(
                    std::slice::from_ref(interval),
                    &events.quotes,
                    &events.trades,
                );
            } else {
                all_rows.extend(densify_interval(
                    &contract.contract_id,
                    &contract.ric,
                    *interval,
                    &events.quotes,
                    &events.trades,
                )?);
            }
        }
        total.quote_rows += events.quote_rows;
        total.quote_kept += events.quote_kept;
        total.quote_fallback += events.quote_fallback;
        total.quote_source_two_sided += events.quote_source_two_sided;
        total.quote_source_one_sided += events.quote_source_one_sided;
        total.quote_crossed_after_overlay += events.quote_crossed_after_overlay;
        total.trade_rows += events.trade_rows;
        total.trade_kept += events.trade_kept;
        total.trade_fallback += events.trade_fallback;
        total.special_rows += events.special_rows;
        total.special_kept += events.special_kept;
        total.special_fallback += events.special_fallback;
        total.off_session_quotes += events.off_session_quotes;
        total.off_session_trades += events.off_session_trades;
        total.off_session_specials += events.off_session_specials;
        total.reviewed_off_session_trades += events.reviewed_off_session_trades;
        total.reviewed_off_session_specials += events.reviewed_off_session_specials;
        total.invalid_trade_prices += events.invalid_trade_prices;
        for (key, count) in events.conflict_counts {
            *total.conflict_counts.entry(key).or_insert(0) += count;
        }
        for sample in events.samples {
            if total.samples.len() < 20 {
                total.samples.push(sample);
            }
        }
        total
            .off_session_trade_samples
            .extend(events.off_session_trade_samples);
        total
            .off_session_special_samples
            .extend(events.off_session_special_samples);
        total
            .invalid_trade_price_samples
            .extend(events.invalid_trade_price_samples);
        for sample in events.crossed_samples {
            if total.crossed_samples.len() < 10 {
                total.crossed_samples.push(sample);
            }
        }
    }
    all_rows.sort_by(|left, right| {
        left.ts
            .cmp(&right.ts)
            .then(left.contract_id.cmp(&right.contract_id))
    });
    for pair in all_rows.windows(2) {
        if pair[0].contract_id == pair[1].contract_id && pair[0].ts == pair[1].ts {
            bail!(
                "duplicate output key {} {} on {}",
                pair[0].contract_id,
                pair[0].ts,
                job.day
            );
        }
    }

    let audit = DayAudit {
        product: job.spec.product.to_string(),
        trad_day: job.day.to_string(),
        intervals: intervals
            .iter()
            .map(|interval| [interval.start, interval.end])
            .collect(),
        known_outages: known_outages
            .iter()
            .map(|interval| [interval.start, interval.end])
            .collect(),
        quote_rows: total.quote_rows,
        quote_kept: total.quote_kept,
        quote_time_fallback: total.quote_fallback,
        quote_source_two_sided: total.quote_source_two_sided,
        quote_source_one_sided: total.quote_source_one_sided,
        quote_crossed_after_overlay: total.quote_crossed_after_overlay,
        trade_rows: total.trade_rows,
        trade_kept: total.trade_kept,
        trade_time_fallback: total.trade_fallback,
        special_rows: total.special_rows,
        special_kept: total.special_kept,
        special_time_fallback: total.special_fallback,
        off_session_quotes: total.off_session_quotes,
        off_session_trades: total.off_session_trades,
        off_session_specials: total.off_session_specials,
        reviewed_off_session_trades: total.reviewed_off_session_trades,
        reviewed_off_session_specials: total.reviewed_off_session_specials,
        invalid_trade_prices: total.invalid_trade_prices,
        output_rows: if !publish_parquet {
            estimated_rows
        } else {
            all_rows.len()
        },
        excluded_crossed_rics,
        conflict_counts: total.conflict_counts,
        conflicts: total.samples,
        off_session_trade_samples: total.off_session_trade_samples,
        off_session_special_samples: total.off_session_special_samples,
        invalid_trade_price_samples: total.invalid_trade_price_samples,
        crossed_book_samples: total.crossed_samples,
    };
    let audit_path = args
        .out_root
        .join("_audit/session_conformance")
        .join(job.spec.product)
        .join(format!("{}.json", job.day.format("%Y%m%d")));
    write_json_atomic(&audit_path, &audit)?;

    let conflict = total.off_session_quotes > 0
        || total.off_session_trades > 0
        || total.off_session_specials > 0
        || total.invalid_trade_prices > 0;
    if !args.audit_only {
        if total.off_session_trades > total.reviewed_off_session_trades {
            bail!(
                "{} {} has {} unreviewed off-session printable trades; see {}",
                job.spec.product,
                job.day,
                total.off_session_trades - total.reviewed_off_session_trades,
                audit_path.display()
            );
        }
        if total.off_session_specials > total.reviewed_off_session_specials {
            bail!(
                "{} {} has {} unreviewed off-session Special events; see {}",
                job.spec.product,
                job.day,
                total.off_session_specials - total.reviewed_off_session_specials,
                audit_path.display()
            );
        }
        if total.off_session_quotes > 0 && !args.allow_off_session_quotes {
            bail!(
                "{} {} has {} off-session quotes; review {} before --allow-off-session-quotes",
                job.spec.product,
                job.day,
                total.off_session_quotes,
                audit_path.display()
            );
        }
        if publish_parquet && !all_rows.is_empty() {
            write_parquet_serialized(parquet_writer, &output_path, &all_rows)?;
        }
    }
    Ok((
        if !publish_parquet {
            estimated_rows
        } else {
            all_rows.len()
        },
        conflict,
    ))
}

fn selected_products(text: &str) -> Result<Vec<ProductSpec>> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    for name in text
        .split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
    {
        let spec = product_spec(name)?;
        if seen.insert(spec.product) {
            out.push(spec);
        }
    }
    if out.is_empty() {
        bail!("at least one product is required");
    }
    Ok(out)
}

fn run(args: &Args) -> Result<()> {
    if args.start > args.end {
        bail!("start {} is after end {}", args.start, args.end);
    }
    if args.start.year() != args.end.year() {
        bail!("one export invocation must stay inside one TAS period year");
    }
    if args.workers == 0 {
        bail!("workers must be positive");
    }
    let period_year = args.start.year();
    let products = selected_products(&args.products)?;
    let calendar = Arc::new(load_calendar(&args.psql, args.start, args.end)?);
    let overrides = Arc::new(SessionOverrideFile::load(&args.session_overrides)?);
    let db = Arc::new(open_input_db(args, &products, period_year)?);
    let source_period = require_completed_source_period(&db, period_year)?;
    let mut jobs = Vec::new();
    let mut day = args.start;
    while day <= args.end {
        for &spec in &products {
            jobs.push(Job { spec, day });
        }
        day = day.succ_opt().context("date overflow")?;
    }
    let pool = ThreadPoolBuilder::new()
        .num_threads(args.workers.min(jobs.len()).max(1))
        .thread_name(|id| format!("cme-backtest-1s-{id}"))
        .build()?;
    let completed = AtomicUsize::new(0);
    // Polars parquet construction is serialized; RocksDB scans and dense-row construction are not.
    let parquet_writer = Mutex::new(());
    let started = Instant::now();
    let results = pool.install(|| {
        jobs.par_iter()
            .map(|job| {
                let result = run_job(
                    &db,
                    &calendar,
                    args,
                    &overrides,
                    &parquet_writer,
                    job,
                    period_year,
                );
                if let Err(error) = &result {
                    eprintln!(
                        "cme_tas_export_backtest_1s job_failed product={} day={} error={error:#}",
                        job.spec.product, job.day
                    );
                }
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done % 25 == 0 || done == jobs.len() {
                    eprintln!(
                        "cme_tas_export_backtest_1s progress={done}/{} elapsed_s={:.1}",
                        jobs.len(),
                        started.elapsed().as_secs_f64()
                    );
                }
                result
            })
            .collect::<Vec<_>>()
    });
    let mut rows = 0usize;
    let mut conflict_days = 0usize;
    let mut failures = Vec::new();
    for result in results {
        match result {
            Ok((job_rows, conflict)) => {
                rows += job_rows;
                conflict_days += usize::from(conflict);
            }
            Err(error) => failures.push(format!("{error:#}")),
        }
    }
    if !failures.is_empty() {
        bail!(
            "{} backtest jobs failed; first errors: {}",
            failures.len(),
            failures.into_iter().take(5).collect::<Vec<_>>().join(" | ")
        );
    }
    println!(
        "cme_tas_export_backtest_1s source_period={} products={} days={} rows={} conflict_days={} audit_only={}",
        source_period,
        products.len(),
        jobs.len(),
        rows,
        conflict_days,
        args.audit_only
    );
    Ok(())
}

fn main() {
    if let Err(error) = run(&Args::parse()) {
        eprintln!("cme_tas_export_backtest_1s failed: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn candidate_rics_follow_the_lseg_2024_year_suffix_cutover() -> Result<()> {
        let rics_2023 = candidate_contracts(product_spec("RTY")?, 2023)?
            .into_iter()
            .map(|contract| contract.ric)
            .collect::<BTreeSet<_>>();
        assert!(rics_2023.contains("RTYH3"));
        assert!(rics_2023.contains("RTYH24"));
        assert!(!rics_2023.contains("RTYH23"));

        let rics_2024 = candidate_contracts(product_spec("RTY")?, 2024)?
            .into_iter()
            .map(|contract| contract.ric)
            .collect::<BTreeSet<_>>();
        assert!(rics_2024.contains("RTYH3"));
        assert!(rics_2024.contains("RTYH24"));
        Ok(())
    }

    #[test]
    fn only_matching_sourced_outage_boundaries_create_a_reviewed_gap() {
        let mut rows = vec![
            SourcedInterval {
                interval: RawInterval { start: 10, end: 20 },
                exception_name: "known_outage:cooling".to_string(),
            },
            SourcedInterval {
                interval: RawInterval { start: 30, end: 40 },
                exception_name: "known_outage:cooling".to_string(),
            },
            SourcedInterval {
                interval: RawInterval { start: 50, end: 60 },
                exception_name: "holiday:early_close".to_string(),
            },
        ];
        let gaps = infer_known_outage_gaps(&mut rows);
        assert_eq!(gaps.len(), 1);
        assert_eq!((gaps[0].start, gaps[0].end), (20, 30));
    }

    #[test]
    fn parquet_writes_complete_from_inside_a_rayon_pool() -> Result<()> {
        let suffix = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cme-tas-backtest-parquet-{}-{suffix}",
            std::process::id()
        ));
        let paths = [root.join("first.parquet"), root.join("second.parquet")];
        let rows = vec![BacktestRow {
            contract_id: "CME:ES:2024-03".to_string(),
            ric: "ESH24".to_string(),
            ts: 1_704_200_000,
            bid0p: 4_800.0,
            bid0v: 10.0,
            ask0p: 4_800.25,
            ask0v: 12.0,
            buy_high: None,
            sell_low: None,
            close: 4_800.125,
            midp: 4_800.125,
        }];
        let writer = Mutex::new(());
        let pool = ThreadPoolBuilder::new().num_threads(2).build()?;
        pool.install(|| {
            paths
                .par_iter()
                .map(|path| write_parquet_serialized(&writer, path, &rows))
                .collect::<Result<Vec<_>>>()
        })?;
        for path in &paths {
            assert!(fs::metadata(path)?.len() > 0);
        }
        fs::remove_dir_all(root)?;
        Ok(())
    }
}
