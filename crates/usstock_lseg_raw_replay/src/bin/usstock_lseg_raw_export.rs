//! Export a bounded NYSE-RTH RAW sample, optionally joined with staged LL2.
//!
//! RAW QuoteMsg records are venue contributions to the composite NBBO, not
//! venue books.  This tool rebuilds the composite L1 from each quote-second
//! snapshot and reads only the selected RIC's RocksDB column families.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, NaiveDate};
use clap::Parser;
use fs2::FileExt;
use polars::prelude::{
    DataFrame, NamedFrom, ParquetCompression, ParquetReader, ParquetWriter, SerReader, Series,
};
use rocksdb::{Direction, IteratorMode, Options, DB};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use usstock_lseg_raw_replay::event_codec::{decode_trade, TradeValue, MSG_TRADE};
use usstock_lseg_raw_replay::quote_codec::{
    decode_key, decode_quote, decode_quote_state, encode_key, QuoteStateValue, QuoteValue,
    MISSING_PRICE, MISSING_SIZE, MSG_QUOTE, MSG_QUOTE_STATE, SIDE_CLEAR,
};
use usstock_lseg_raw_replay::size_buckets::{percentile_in_place, SizeBuckets, SizeThresholds};

const NS: u64 = 1_000_000_000;
const LOOKBACK_SECS: i64 = 86_400;
const L2_WIDTH: usize = 40;

#[derive(Parser, Debug)]
#[command(name = "usstock_lseg_raw_export")]
#[command(about = "Export RAW L1/trades and join existing staged LL2 inside NYSE RTH")]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    /// Previous-period RAW RocksDB used only when this period lacks the
    /// previous natural month's size-threshold sample.
    #[arg(long)]
    size_reference_rocksdb: Option<PathBuf>,
    #[arg(long)]
    calendar: PathBuf,
    #[arg(long)]
    stage_ll2_root: PathBuf,
    /// Export only RAW L1/trades. Do not join staged LL2 even when it exists.
    #[arg(long)]
    raw_only: bool,
    #[arg(long)]
    backtest_out_root: PathBuf,
    #[arg(long)]
    baseline_out_root: PathBuf,
    #[arg(long)]
    ric: String,
    /// Inclusive NY session date, YYYY-MM-DD.
    #[arg(long)]
    day: String,
    /// Permit a RAW-only sample when this RIC has no staged LL2 parquet.
    #[arg(long)]
    allow_missing_ll2: bool,
    /// Inclusive start of the side-classification comparison window, as UTC Unix seconds.
    #[arg(long)]
    side_compare_start_ts: Option<i64>,
    /// Exclusive end of the side-classification comparison window, as UTC Unix seconds.
    #[arg(long)]
    side_compare_end_ts: Option<i64>,
    #[arg(long)]
    overwrite: bool,
    /// Recompute the cached previous-month P50/P90 audit for this RIC/month.
    #[arg(long)]
    rebuild_size_thresholds: bool,
}

#[derive(Debug, Deserialize)]
struct CalendarRow {
    session_date: String,
    open_ts: i64,
    close_ts: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SizeAudit {
    schema: String,
    ric: String,
    venue: String,
    trading_month: String,
    threshold_source_month: Option<String>,
    threshold_method: String,
    threshold_sample: String,
    threshold_sample_trades: u64,
    off_exchange_policy: String,
    p50: Option<f64>,
    p90: Option<f64>,
    /// RocksDB that supplied the previous-natural-month trade sample.
    threshold_rocksdb: String,
    /// RocksDB that supplied the exported session's quotes and trades.
    rocksdb: String,
}

impl SizeAudit {
    fn thresholds(&self) -> Result<Option<SizeThresholds>> {
        match (self.p50, self.p90) {
            (Some(p50), Some(p90)) => Ok(Some(SizeThresholds::new(p50, p90)?)),
            (None, None) => Ok(None),
            _ => bail!("size audit has only one of p50/p90"),
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Contribution {
    bid: Option<(f64, f64)>,
    ask: Option<(f64, f64)>,
}

#[derive(Clone, Copy, Default)]
struct CompositeBook {
    contributions: [Contribution; 32],
    count: usize,
}

impl CompositeBook {
    fn reset_snapshot(&mut self) {
        self.contributions = [Contribution::default(); 32];
        self.count = 0;
    }

    fn push(&mut self, quote: QuoteValue) -> Result<()> {
        if self.count == self.contributions.len() {
            bail!(
                "RAW quote snapshot has more than {} venue contributions",
                self.count
            );
        }
        let one = |price: i64, size: u32| -> Option<(f64, f64)> {
            if price == MISSING_PRICE || size == MISSING_SIZE || price <= 0 || size == 0 {
                None
            } else {
                Some((price as f64 / 1e9, size as f64))
            }
        };
        self.contributions[self.count] = Contribution {
            bid: one(quote.bid, quote.bid_size),
            ask: one(quote.ask, quote.ask_size),
        };
        self.count += 1;
        Ok(())
    }

    fn clear(&mut self, state: QuoteStateValue) {
        for contribution in &mut self.contributions[..self.count] {
            if state.bid_action == SIDE_CLEAR {
                contribution.bid = None;
            }
            if state.ask_action == SIDE_CLEAR {
                contribution.ask = None;
            }
        }
    }

    fn bbo(&self) -> Option<(f64, f64, f64, f64)> {
        let bid = self.contributions[..self.count]
            .iter()
            .filter_map(|row| row.bid)
            .max_by(|left, right| left.0.total_cmp(&right.0));
        let ask = self.contributions[..self.count]
            .iter()
            .filter_map(|row| row.ask)
            .min_by(|left, right| left.0.total_cmp(&right.0));
        match (bid, ask) {
            (Some((bid_p, bid_v)), Some((ask_p, ask_v))) if ask_p >= bid_p => {
                Some((bid_p, bid_v, ask_p, ask_v))
            }
            _ => None,
        }
    }
}

/// A RAW quote snapshot contains the current best contribution for a venue,
/// rather than a full depth book.  A venue-side quote test therefore requires
/// both sides to be present for the trade's own venue in that snapshot.
#[derive(Default)]
struct VenueBooks {
    contributions: BTreeMap<String, Contribution>,
}

impl VenueBooks {
    fn reset_snapshot(&mut self) {
        self.contributions.clear();
    }

    fn push(&mut self, venue: &str, quote: QuoteValue) {
        let one = |price: i64, size: u32| -> Option<(f64, f64)> {
            if price == MISSING_PRICE || size == MISSING_SIZE || price <= 0 || size == 0 {
                None
            } else {
                Some((price as f64 / 1e9, size as f64))
            }
        };
        self.contributions.insert(
            venue.to_string(),
            Contribution {
                bid: one(quote.bid, quote.bid_size),
                ask: one(quote.ask, quote.ask_size),
            },
        );
    }

    fn clear(&mut self, state: QuoteStateValue) {
        for contribution in self.contributions.values_mut() {
            if state.bid_action == SIDE_CLEAR {
                contribution.bid = None;
            }
            if state.ask_action == SIDE_CLEAR {
                contribution.ask = None;
            }
        }
    }

    fn bbo(&self, venue: &str) -> Option<(f64, f64, f64, f64)> {
        let contribution = self.contributions.get(venue)?;
        let (bid_p, bid_v) = contribution.bid?;
        let (ask_p, ask_v) = contribution.ask?;
        (ask_p >= bid_p).then_some((bid_p, bid_v, ask_p, ask_v))
    }
}

#[derive(Clone)]
struct Trade {
    price: f64,
    size: f64,
    venue: String,
    source_order: u64,
    side: Option<bool>,
    side_method: u8,
    side_flags: u8,
}

fn stored_trade(trade: TradeValue, venue: &str) -> Result<Option<Trade>> {
    if trade.price <= 0 || trade.size == 0 || trade.size == u64::MAX {
        return Ok(None);
    }
    if trade.side_method == 0 || trade.side_method == 10 {
        bail!("RAW trade direction has not been finalized; rebuild replay before export");
    }
    if !matches!(trade.aggressor_side, b'B' | b'S' | b'N') {
        bail!("RAW trade has an invalid aggressor side");
    }
    if trade.aggressor_side == b'N'
        && (trade.side_method != 9 || trade.unknown_reason != 1 || trade.venue_class != 2)
    {
        bail!("valid RAW N trade is not an off-exchange reporting trade");
    }
    Ok(Some(Trade {
        price: trade.price as f64 / 1e9,
        size: trade.size as f64,
        venue: venue.to_owned(),
        source_order: trade.source_order,
        side: match trade.aggressor_side {
            b'B' => Some(true),
            b'S' => Some(false),
            _ => None,
        },
        side_method: trade.side_method,
        side_flags: trade.side_flags,
    }))
}

#[derive(Clone)]
struct VenueQuote {
    venue: String,
    value: QuoteValue,
}

#[derive(Clone, Copy)]
struct L2 {
    values: [f64; L2_WIDTH],
}

impl Default for L2 {
    fn default() -> Self {
        Self {
            values: [f64::NAN; L2_WIDTH],
        }
    }
}

#[derive(Default)]
struct RawRows {
    quotes: BTreeMap<u64, Vec<VenueQuote>>,
    states: BTreeMap<u64, Vec<QuoteStateValue>>,
    trades: BTreeMap<u64, Vec<Trade>>,
}

// This comparison is a quote-test diagnostic. Its third state means no quote-test
// classification, not an off-exchange trade.
const SIDE_NAMES: [&str; 3] = ["buy", "sell", "unclassified"];

fn side_index(side: Option<bool>) -> usize {
    match side {
        Some(true) => 0,
        Some(false) => 1,
        None => 2,
    }
}

#[derive(Default, Clone)]
struct SideSummary {
    count: [i64; 3],
    volume: [f64; 3],
}

impl SideSummary {
    fn add(&mut self, side: Option<bool>, size: f64) {
        let index = side_index(side);
        self.count[index] += 1;
        self.volume[index] += size;
    }

    fn as_json(&self) -> serde_json::Value {
        json!({
            "buy": { "count": self.count[0], "volume": self.volume[0] },
            "sell": { "count": self.count[1], "volume": self.volume[1] },
            "unclassified": { "count": self.count[2], "volume": self.volume[2] },
        })
    }
}

#[derive(Default, Clone)]
struct SideComparison {
    composite: SideSummary,
    venue: SideSummary,
    transition_count: [[i64; 3]; 3],
    transition_volume: [[f64; 3]; 3],
}

impl SideComparison {
    fn add(&mut self, trade: Trade, composite: Option<bool>, venue: Option<bool>) {
        self.composite.add(composite, trade.size);
        self.venue.add(venue, trade.size);
        let from = side_index(composite);
        let to = side_index(venue);
        self.transition_count[from][to] += 1;
        self.transition_volume[from][to] += trade.size;
    }

    fn as_json(&self) -> serde_json::Value {
        let mut transitions = serde_json::Map::new();
        for (from_index, from) in SIDE_NAMES.into_iter().enumerate() {
            let mut targets = serde_json::Map::new();
            for (to_index, to) in SIDE_NAMES.into_iter().enumerate() {
                targets.insert(
                    to.to_string(),
                    json!({
                        "count": self.transition_count[from_index][to_index],
                        "volume": self.transition_volume[from_index][to_index],
                    }),
                );
            }
            transitions.insert(from.to_string(), serde_json::Value::Object(targets));
        }
        json!({
            "composite_nbbo": self.composite.as_json(),
            "trade_venue": self.venue.as_json(),
            "transition": transitions,
        })
    }
}

#[derive(Default)]
struct Minute {
    volume: f64,
    amount: f64,
    count: i64,
    buy_volume: f64,
    buy_amount: f64,
    buy_count: i64,
    buy_high: f64,
    sell_volume: f64,
    sell_amount: f64,
    sell_count: i64,
    sell_low: f64,
    off_exchange_volume: f64,
    off_exchange_amount: f64,
    off_exchange_count: i64,
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    size: SizeBuckets,
}

impl Minute {
    fn new() -> Self {
        Self {
            buy_high: f64::NAN,
            sell_low: f64::NAN,
            open: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            close: f64::NAN,
            ..Self::default()
        }
    }

    fn add(&mut self, trade: &Trade, thresholds: Option<SizeThresholds>) -> Result<()> {
        let amount = trade.price * trade.size;
        self.volume += trade.size;
        self.amount += amount;
        self.count += 1;
        if self.open.is_nan() {
            self.open = trade.price;
            self.high = trade.price;
            self.low = trade.price;
        } else {
            self.high = self.high.max(trade.price);
            self.low = self.low.min(trade.price);
        }
        self.close = trade.price;
        match trade.side {
            Some(true) => {
                self.buy_volume += trade.size;
                self.buy_amount += amount;
                self.buy_count += 1;
                self.buy_high = if self.buy_high.is_nan() {
                    trade.price
                } else {
                    self.buy_high.max(trade.price)
                };
            }
            Some(false) => {
                self.sell_volume += trade.size;
                self.sell_amount += amount;
                self.sell_count += 1;
                self.sell_low = if self.sell_low.is_nan() {
                    trade.price
                } else {
                    self.sell_low.min(trade.price)
                };
            }
            None => {
                self.off_exchange_volume += trade.size;
                self.off_exchange_amount += amount;
                self.off_exchange_count += 1;
            }
        }
        if let Some(thresholds) = thresholds {
            self.size.add(amount, trade.side, thresholds)?;
        }
        Ok(())
    }
}

fn venue_of(ric: &str) -> Result<&'static str> {
    if ric.ends_with(".N") {
        Ok("NYSE")
    } else if ric.ends_with(".O") {
        Ok("NASDAQ")
    } else if ric.ends_with(".P") {
        Ok("ARCA")
    } else if ric.ends_with(".BAT") {
        Ok("BZX")
    } else {
        bail!("cannot infer output venue from RIC {ric}")
    }
}

fn parse_day(text: &str) -> Result<NaiveDate> {
    NaiveDate::parse_from_str(text, "%Y-%m-%d").with_context(|| format!("parse --day {text}"))
}

fn require_raw_output_root(path: &Path, expected: &str) -> Result<()> {
    if path.file_name().and_then(|name| name.to_str()) != Some(expected) {
        bail!(
            "RAW export output root must end in the exact directory {expected:?}, got {}",
            path.display()
        );
    }
    Ok(())
}

fn session_bounds(path: &Path, day: NaiveDate) -> Result<(i64, i64)> {
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("open calendar {}", path.display()))?;
    for row in reader.deserialize::<CalendarRow>() {
        let row = row?;
        if row.session_date == day.to_string() {
            if row.close_ts <= row.open_ts {
                bail!("invalid session bounds for {day}");
            }
            return Ok((row.open_ts, row.close_ts));
        }
    }
    bail!("calendar {} has no session for {day}", path.display())
}

fn previous_month(year: i32, month: u32) -> (i32, u32) {
    if month == 1 {
        (year - 1, 12)
    } else {
        (year, month - 1)
    }
}

fn previous_month_sessions(path: &Path, day: NaiveDate) -> Result<(String, Vec<(u64, u64)>)> {
    let (year, month) = previous_month(day.year(), day.month());
    let source_month = format!("{year:04}-{month:02}");
    let mut reader = csv::Reader::from_path(path)
        .with_context(|| format!("open calendar {}", path.display()))?;
    let mut sessions = Vec::new();
    for row in reader.deserialize::<CalendarRow>() {
        let row = row?;
        let session_day = NaiveDate::parse_from_str(&row.session_date, "%Y-%m-%d")
            .with_context(|| format!("parse calendar session_date {:?}", row.session_date))?;
        if session_day.year() != year || session_day.month() != month {
            continue;
        }
        if row.open_ts < 0 || row.close_ts <= row.open_ts {
            bail!("invalid session bounds for {session_day}");
        }
        sessions.push((row.open_ts as u64 * NS, row.close_ts as u64 * NS));
    }
    sessions.sort_unstable();
    if sessions.is_empty() {
        bail!(
            "calendar {} has no sessions for threshold source month {source_month}",
            path.display()
        );
    }
    for pair in sessions.windows(2) {
        if pair[0].1 > pair[1].0 {
            bail!("overlapping calendar sessions in {source_month}");
        }
    }
    Ok((source_month, sessions))
}

fn collect_size_sample(
    db: &DB,
    venue_cfs: &[String],
    instrument_cf: &str,
    sessions: &[(u64, u64)],
) -> Result<Vec<f64>> {
    let first = sessions.first().context("empty size threshold sessions")?.0;
    let end = sessions.last().context("empty size threshold sessions")?.1;
    let mut amounts = Vec::new();
    for name in venue_cfs
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(instrument_cf))
    {
        let trade_venue = if name == instrument_cf {
            ""
        } else {
            name.rsplit_once(':').map(|(_, venue)| venue).unwrap_or("")
        };
        let cf = db.cf_handle(name).context("size threshold CF missing")?;
        let start = encode_key(MSG_TRADE, first, 0);
        let mut session_index = 0usize;
        for row in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
            let (key, value) = row?;
            let (kind, ts, _) = decode_key(&key)?;
            if kind != MSG_TRADE || ts >= end {
                break;
            }
            while session_index < sessions.len() && ts >= sessions[session_index].1 {
                session_index += 1;
            }
            if session_index == sessions.len() {
                break;
            }
            if ts < sessions[session_index].0 {
                continue;
            }
            if let Some(trade) = stored_trade(decode_trade(&value)?, trade_venue)? {
                amounts.push(trade.price * trade.size);
            }
        }
    }
    Ok(amounts)
}

fn size_audit_path(root: &Path, venue: &str, ric: &str, day: NaiveDate) -> PathBuf {
    root.join("_audit")
        .join("size_buckets")
        .join(venue)
        .join(ric)
        .join(format!("{:04}{:02}.json", day.year(), day.month()))
}

fn validate_size_audit(
    audit: &SizeAudit,
    rocksdb: &Path,
    size_reference_rocksdb: Option<&Path>,
    venue: &str,
    ric: &str,
    day: NaiveDate,
) -> Result<()> {
    let month = format!("{:04}-{:02}", day.year(), day.month());
    let (source_year, source_month) = previous_month(day.year(), day.month());
    let expected_source = format!("{source_year:04}-{source_month:02}");
    let valid_threshold_sources = std::iter::once(rocksdb)
        .chain(size_reference_rocksdb)
        .any(|path| audit.threshold_rocksdb == path.display().to_string());
    if audit.schema != "usstock-raw-size-threshold-v2"
        || audit.rocksdb != rocksdb.display().to_string()
        || !valid_threshold_sources
        || audit.venue != venue
        || audit.ric != ric
        || audit.trading_month != month
    {
        bail!("cached size audit does not match this RAW export: {audit:?}");
    }
    let thresholds = audit.thresholds()?;
    match (
        audit.threshold_source_month.as_deref(),
        audit.threshold_sample_trades,
        thresholds,
    ) {
        (Some(source), count, Some(_)) if source == expected_source && count > 0 => {}
        (None, 0, None) => {}
        _ => bail!("cached size audit has inconsistent source/sample/threshold fields"),
    }
    Ok(())
}

fn collect_reference_size_sample(
    rocksdb: &Path,
    ric: &str,
    sessions: &[(u64, u64)],
) -> Result<Vec<f64>> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    let instrument = format!("i:{ric}");
    if !DB::list_cf(&options, rocksdb)
        .with_context(|| format!("list column families in {}", rocksdb.display()))?
        .iter()
        .any(|name| name == &instrument)
    {
        return Ok(Vec::new());
    }
    let (db, venue_cfs, instrument_cf) = open_read_only(rocksdb, ric)?;
    collect_size_sample(&db, &venue_cfs, &instrument_cf, sessions)
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("JSON output has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("json.tmp");
    let result = (|| -> Result<()> {
        fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[allow(clippy::too_many_arguments)]
fn load_or_build_size_audit(
    db: &DB,
    venue_cfs: &[String],
    instrument_cf: &str,
    rocksdb: &Path,
    size_reference_rocksdb: Option<&Path>,
    calendar: &Path,
    output_root: &Path,
    venue: &str,
    ric: &str,
    day: NaiveDate,
    rebuild: bool,
) -> Result<(PathBuf, SizeAudit)> {
    let path = size_audit_path(output_root, venue, ric, day);
    let parent = path.parent().context("size audit has no parent")?;
    fs::create_dir_all(parent)?;
    let lock_path = path.with_extension("json.lock");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    lock.lock_exclusive()?;
    if path.is_file() && !rebuild {
        let audit: SizeAudit = serde_json::from_slice(&fs::read(&path)?)?;
        validate_size_audit(&audit, rocksdb, size_reference_rocksdb, venue, ric, day)?;
        return Ok((path, audit));
    }

    let (source_month, sessions) = previous_month_sessions(calendar, day)?;
    let mut amounts = collect_size_sample(db, venue_cfs, instrument_cf, &sessions)?;
    let threshold_rocksdb = if amounts.is_empty() {
        if let Some(reference) = size_reference_rocksdb {
            let reference_amounts = collect_reference_size_sample(reference, ric, &sessions)?;
            if !reference_amounts.is_empty() {
                amounts = reference_amounts;
                reference
            } else {
                rocksdb
            }
        } else {
            rocksdb
        }
    } else {
        rocksdb
    };
    let sample_trades = amounts.len() as u64;
    let (threshold_source_month, p50, p90) = if amounts.is_empty() {
        (None, None, None)
    } else {
        let p50 = percentile_in_place(&mut amounts, 0.5)?;
        let p90 = percentile_in_place(&mut amounts, 0.9)?;
        SizeThresholds::new(p50, p90)?;
        (Some(source_month), Some(p50), Some(p90))
    };
    let audit = SizeAudit {
        schema: "usstock-raw-size-threshold-v2".to_string(),
        ric: ric.to_string(),
        venue: venue.to_string(),
        trading_month: format!("{:04}-{:02}", day.year(), day.month()),
        threshold_source_month,
        threshold_method: "previous natural month exact linear P50/P90 (NumPy default)".to_string(),
        threshold_sample:
            "previous NYSE RTH session-date month valid RAW trade notional, including off-exchange"
                .to_string(),
        threshold_sample_trades: sample_trades,
        off_exchange_policy: "included in total bucket; excluded from buy/sell buckets".to_string(),
        p50,
        p90,
        threshold_rocksdb: threshold_rocksdb.display().to_string(),
        rocksdb: rocksdb.display().to_string(),
    };
    validate_size_audit(&audit, rocksdb, size_reference_rocksdb, venue, ric, day)?;
    write_json_atomic(&path, &audit)?;
    Ok((path, audit))
}

fn stage_path(root: &Path, venue: &str, ric: &str, day: NaiveDate) -> PathBuf {
    root.join(venue)
        .join(ric)
        .join(format!("{}.parquet", day.format("%Y%m%d")))
}

fn output_path(root: &Path, venue: &str, ric: &str, day: NaiveDate) -> PathBuf {
    root.join(venue)
        .join(ric)
        .join(format!("{}.parquet", day.format("%Y%m%d")))
}

fn open_read_only(path: &Path, ric: &str) -> Result<(DB, Vec<String>, String)> {
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    options.set_max_open_files(256);
    let all = DB::list_cf(&options, path)
        .with_context(|| format!("list column families in {}", path.display()))?;
    let instrument = format!("i:{ric}");
    let prefix = format!("v:{ric}:");
    let mut selected = all
        .iter()
        .filter(|name| **name == instrument || name.starts_with(&prefix))
        .cloned()
        .collect::<Vec<_>>();
    if !selected.iter().any(|name| name == "default") && all.iter().any(|name| name == "default") {
        selected.push("default".to_string());
    }
    if !selected.iter().any(|name| name == &instrument) {
        bail!("RAW RocksDB has no instrument column family {instrument}");
    }
    let venue_cfs = selected
        .iter()
        .filter(|name| name.starts_with(&prefix))
        .cloned()
        .collect();
    let db = DB::open_cf_for_read_only(&options, path, selected, false)
        .with_context(|| format!("open RAW RocksDB {} read-only", path.display()))?;
    Ok((db, venue_cfs, instrument))
}

fn collect_raw(
    db: &DB,
    venue_cfs: &[String],
    instrument_cf: &str,
    start_ns: u64,
    end_ns: u64,
) -> Result<RawRows> {
    let mut rows = RawRows::default();
    for name in venue_cfs {
        let trade_venue = name
            .rsplit_once(':')
            .map(|(_, venue)| venue)
            .context("venue column family has no venue suffix")?;
        let cf = db
            .cf_handle(name)
            .context("opened venue column family missing")?;
        let quote_start = encode_key(MSG_QUOTE, start_ns, 0);
        for row in db.iterator_cf(&cf, IteratorMode::From(&quote_start, Direction::Forward)) {
            let (key, value) = row?;
            let (kind, ts, _) = decode_key(&key)?;
            if kind != MSG_QUOTE || ts >= end_ns {
                break;
            }
            rows.quotes.entry(ts).or_default().push(VenueQuote {
                venue: trade_venue.to_string(),
                value: decode_quote(&value)?,
            });
        }
        let trade_start = encode_key(MSG_TRADE, start_ns, 0);
        for row in db.iterator_cf(&cf, IteratorMode::From(&trade_start, Direction::Forward)) {
            let (key, value) = row?;
            let (kind, ts, _) = decode_key(&key)?;
            if kind != MSG_TRADE || ts >= end_ns {
                break;
            }
            let trade = decode_trade(&value)?;
            if let Some(trade) = stored_trade(trade, trade_venue)? {
                rows.trades.entry(ts / NS * NS).or_default().push(trade);
            }
        }
    }
    let cf = db
        .cf_handle(instrument_cf)
        .context("opened instrument column family missing")?;
    let state_start = encode_key(MSG_QUOTE_STATE, start_ns, 0);
    for row in db.iterator_cf(&cf, IteratorMode::From(&state_start, Direction::Forward)) {
        let (key, value) = row?;
        let (kind, ts, _) = decode_key(&key)?;
        if kind != MSG_QUOTE_STATE || ts >= end_ns {
            break;
        }
        rows.states
            .entry(ts)
            .or_default()
            .push(decode_quote_state(&value)?);
    }
    let trade_start = encode_key(MSG_TRADE, start_ns, 0);
    for row in db.iterator_cf(&cf, IteratorMode::From(&trade_start, Direction::Forward)) {
        let (key, value) = row?;
        let (kind, ts, _) = decode_key(&key)?;
        if kind != MSG_TRADE || ts >= end_ns {
            break;
        }
        let trade = decode_trade(&value)?;
        if let Some(trade) = stored_trade(trade, "")? {
            rows.trades.entry(ts / NS * NS).or_default().push(trade);
        }
    }
    for trades in rows.trades.values_mut() {
        trades.sort_by_key(|trade| trade.source_order);
    }
    Ok(rows)
}

fn load_stage(path: &Path, start_ts: i64, close_ts: i64) -> Result<BTreeMap<i64, L2>> {
    let frame =
        ParquetReader::new(File::open(path).with_context(|| format!("open {}", path.display()))?)
            .set_low_memory(true)
            .finish()
            .with_context(|| format!("read {}", path.display()))?;
    let ts = frame.column("ts")?.i64()?;
    let mut columns = Vec::with_capacity(L2_WIDTH);
    for level in 0..10 {
        for name in [
            format!("l2_bid{level}p"),
            format!("l2_bid{level}v"),
            format!("l2_ask{level}p"),
            format!("l2_ask{level}v"),
        ] {
            columns.push(frame.column(&name)?.f64()?.clone());
        }
    }
    let mut result = BTreeMap::new();
    for index in 0..frame.height() {
        let timestamp = ts.get(index).context("stage ts is null")?;
        if timestamp < start_ts || timestamp >= close_ts {
            continue;
        }
        let mut values = [f64::NAN; L2_WIDTH];
        for (column, value) in columns.iter().zip(&mut values) {
            *value = column.get(index).unwrap_or(f64::NAN);
        }
        result.insert(timestamp, L2 { values });
    }
    Ok(result)
}

fn apply_book_events(
    book: &mut CompositeBook,
    venue_books: &mut VenueBooks,
    rows: &RawRows,
    ts_ns: u64,
) -> Result<()> {
    if let Some(quotes) = rows.quotes.get(&ts_ns) {
        book.reset_snapshot();
        venue_books.reset_snapshot();
        for quote in quotes {
            book.push(quote.value.clone())?;
            venue_books.push(&quote.venue, quote.value.clone());
        }
    }
    if let Some(states) = rows.states.get(&ts_ns) {
        for state in states {
            book.clear(state.clone());
            venue_books.clear(state.clone());
        }
    }
    Ok(())
}

fn infer_side(book: Option<(f64, f64, f64, f64)>, price: f64) -> Option<bool> {
    let (bid, _, ask, _) = book?;
    if price >= ask {
        Some(true)
    } else if price <= bid {
        Some(false)
    } else {
        None
    }
}

fn write_atomic(path: &Path, mut frame: DataFrame) -> Result<()> {
    let parent = path.parent().context("output has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let temporary = path.with_extension("parquet.tmp");
    let result = (|| -> Result<()> {
        ParquetWriter::new(File::create(&temporary)?)
            .with_compression(ParquetCompression::Zstd(None))
            .finish(&mut frame)?;
        fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

fn add_l2_columns(columns: &mut Vec<Series>, values: &[[f64; L2_WIDTH]]) {
    for level in 0..10 {
        for (offset, name) in [
            (0, format!("l2_bid{level}p")),
            (1, format!("l2_bid{level}v")),
            (2, format!("l2_ask{level}p")),
            (3, format!("l2_ask{level}v")),
        ] {
            columns.push(Series::new(
                name.into(),
                values
                    .iter()
                    .map(|row| row[level * 4 + offset])
                    .collect::<Vec<_>>(),
            ));
        }
    }
}

fn write_manifest(
    root: &Path,
    venue: &str,
    ric: &str,
    day: NaiveDate,
    value: serde_json::Value,
) -> Result<()> {
    let path = root
        .join("_raw_export_manifest")
        .join(venue)
        .join(ric)
        .join(format!("{}.json", day.format("%Y%m%d")));
    let parent = path.parent().context("manifest has no parent")?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(&value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    require_raw_output_root(&args.backtest_out_root, "backtest_1s_raw")?;
    require_raw_output_root(&args.baseline_out_root, "baseline_data_1m_raw")?;
    let day = parse_day(&args.day)?;
    let venue = venue_of(&args.ric)?;
    let (open_ts, close_ts) = session_bounds(&args.calendar, day)?;
    let side_compare_start_ts = args.side_compare_start_ts.unwrap_or(open_ts);
    let side_compare_end_ts = args.side_compare_end_ts.unwrap_or(close_ts);
    if side_compare_start_ts < open_ts
        || side_compare_start_ts >= side_compare_end_ts
        || side_compare_end_ts > close_ts
    {
        bail!(
            "side comparison window [{side_compare_start_ts}, {side_compare_end_ts}) is outside RTH [{open_ts}, {close_ts})"
        );
    }
    let stage = stage_path(&args.stage_ll2_root, venue, &args.ric, day);
    let has_stage_ll2 = !args.raw_only && stage.is_file();
    if !has_stage_ll2 && !args.allow_missing_ll2 && !args.raw_only {
        bail!("staged LL2 is absent: {}", stage.display());
    }
    let backtest_path = output_path(&args.backtest_out_root, venue, &args.ric, day);
    let baseline_path = output_path(&args.baseline_out_root, venue, &args.ric, day);
    if !args.overwrite && (backtest_path.exists() || baseline_path.exists()) {
        bail!(
            "refusing to overwrite {} or {}; pass --overwrite",
            backtest_path.display(),
            baseline_path.display()
        );
    }

    let scan_start_ts = open_ts.saturating_sub(LOOKBACK_SECS);
    let (db, venue_cfs, instrument_cf) = open_read_only(&args.rocksdb_dir, &args.ric)?;
    let raw = collect_raw(
        &db,
        &venue_cfs,
        &instrument_cf,
        (scan_start_ts as u64) * NS,
        (close_ts as u64) * NS,
    )?;
    let (size_audit_path, size_audit) = load_or_build_size_audit(
        &db,
        &venue_cfs,
        &instrument_cf,
        &args.rocksdb_dir,
        args.size_reference_rocksdb.as_deref(),
        &args.calendar,
        &args.baseline_out_root,
        venue,
        &args.ric,
        day,
        args.rebuild_size_thresholds,
    )?;
    let size_thresholds = size_audit.thresholds()?;
    let stage_rows = if has_stage_ll2 {
        load_stage(&stage, scan_start_ts, close_ts)?
    } else {
        BTreeMap::new()
    };

    let mut book = CompositeBook::default();
    let mut venue_books = VenueBooks::default();
    let mut l2 = L2::default();
    let mut preopen_times = BTreeSet::new();
    preopen_times.extend(
        raw.quotes
            .keys()
            .copied()
            .filter(|ts| *ts < open_ts as u64 * NS),
    );
    preopen_times.extend(
        raw.states
            .keys()
            .copied()
            .filter(|ts| *ts < open_ts as u64 * NS),
    );
    for ts in preopen_times {
        apply_book_events(&mut book, &mut venue_books, &raw, ts)?;
    }
    for snapshot in stage_rows.range(..open_ts).map(|(_, snapshot)| snapshot) {
        l2 = *snapshot;
    }

    let seconds = usize::try_from(close_ts - open_ts)?;
    let minutes = seconds / 60;
    if seconds % 60 != 0 {
        bail!("session length {seconds} is not an integer number of minutes");
    }
    let mut ts = Vec::with_capacity(seconds);
    let mut bid0p = Vec::with_capacity(seconds);
    let mut bid0v = Vec::with_capacity(seconds);
    let mut ask0p = Vec::with_capacity(seconds);
    let mut ask0v = Vec::with_capacity(seconds);
    let mut buy_high = Vec::with_capacity(seconds);
    let mut sell_low = Vec::with_capacity(seconds);
    let mut close = Vec::with_capacity(seconds);
    let mut midp = Vec::with_capacity(seconds);
    let mut book_depth = Vec::with_capacity(seconds);
    let mut l2_rows = Vec::with_capacity(seconds);
    let mut minute_data = (0..minutes).map(|_| Minute::new()).collect::<Vec<_>>();
    let mut minute_last_book = vec![None; minutes];
    let mut minute_last_l2 = vec![L2::default(); minutes];
    let mut last_close = f64::NAN;
    let mut side_comparison = SideComparison::default();
    let mut side_comparison_by_venue = BTreeMap::<String, SideComparison>::new();

    for second in open_ts..close_ts {
        let minute = usize::try_from((second - open_ts) / 60)?;
        let bbo = book.bbo();
        let (bp, bv, ap, av, mid) = match bbo {
            Some((bp, bv, ap, av)) => (bp, bv, ap, av, (bp + ap) / 2.0),
            None => (f64::NAN, f64::NAN, f64::NAN, f64::NAN, f64::NAN),
        };
        let mut second_buy_high = f64::NAN;
        let mut second_sell_low = f64::NAN;
        let mut second_close = f64::NAN;
        if let Some(trades) = raw.trades.get(&(second as u64 * NS)) {
            for trade in trades {
                let composite_side = infer_side(bbo, trade.price);
                let venue_side = infer_side(venue_books.bbo(&trade.venue), trade.price);
                minute_data[minute].add(trade, size_thresholds)?;
                if second >= side_compare_start_ts && second < side_compare_end_ts {
                    side_comparison.add(trade.clone(), composite_side, venue_side);
                    side_comparison_by_venue
                        .entry(trade.venue.clone())
                        .or_default()
                        .add(trade.clone(), composite_side, venue_side);
                }
                second_close = trade.price;
                match trade.side {
                    Some(true) => {
                        second_buy_high = if second_buy_high.is_nan() {
                            trade.price
                        } else {
                            second_buy_high.max(trade.price)
                        };
                    }
                    Some(false) => {
                        second_sell_low = if second_sell_low.is_nan() {
                            trade.price
                        } else {
                            second_sell_low.min(trade.price)
                        };
                    }
                    None => {}
                }
            }
        }
        if second_close.is_nan() {
            second_close = if last_close.is_nan() { mid } else { last_close };
        }
        if !second_close.is_nan() {
            last_close = second_close;
        }
        ts.push(second);
        bid0p.push(bp);
        bid0v.push(bv);
        ask0p.push(ap);
        ask0v.push(av);
        buy_high.push(second_buy_high);
        sell_low.push(second_sell_low);
        close.push(second_close);
        midp.push(mid);
        book_depth.push(if l2.values[0] > 0.0 && l2.values[2] > 0.0 {
            10_i8
        } else {
            0_i8
        });
        l2_rows.push(l2.values);
        minute_last_book[minute] = Some((bp, bv, ap, av, mid));
        minute_last_l2[minute] = l2;

        apply_book_events(&mut book, &mut venue_books, &raw, second as u64 * NS)?;
        if let Some(snapshot) = stage_rows.get(&second) {
            l2 = *snapshot;
        }
    }

    let mut backtest_columns = vec![
        Series::new("ric".into(), vec![args.ric.clone(); seconds]),
        Series::new("venue".into(), vec![venue.to_string(); seconds]),
        Series::new("ts".into(), &ts),
        Series::new("book_depth".into(), &book_depth),
        Series::new("bid0p".into(), &bid0p),
        Series::new("bid0v".into(), &bid0v),
        Series::new("ask0p".into(), &ask0p),
        Series::new("ask0v".into(), &ask0v),
        Series::new("buy_high".into(), &buy_high),
        Series::new("sell_low".into(), &sell_low),
        Series::new("close".into(), &close),
        Series::new("midp".into(), &midp),
    ];
    add_l2_columns(&mut backtest_columns, &l2_rows);
    write_atomic(&backtest_path, DataFrame::new(backtest_columns)?)?;

    let mut minute_ts = Vec::with_capacity(minutes);
    let mut minute_depth = Vec::with_capacity(minutes);
    let mut minute_bidp = Vec::with_capacity(minutes);
    let mut minute_bidv = Vec::with_capacity(minutes);
    let mut minute_askp = Vec::with_capacity(minutes);
    let mut minute_askv = Vec::with_capacity(minutes);
    let mut minute_midp = Vec::with_capacity(minutes);
    let mut minute_l2 = Vec::with_capacity(minutes);
    for index in 0..minutes {
        let (bp, bv, ap, av, mid) =
            minute_last_book[index].unwrap_or((f64::NAN, f64::NAN, f64::NAN, f64::NAN, f64::NAN));
        minute_ts.push(open_ts + i64::try_from(index)? * 60);
        minute_bidp.push(bp);
        minute_bidv.push(bv);
        minute_askp.push(ap);
        minute_askv.push(av);
        minute_midp.push(mid);
        minute_depth.push(
            if minute_last_l2[index].values[0] > 0.0 && minute_last_l2[index].values[2] > 0.0 {
                10_i8
            } else {
                0_i8
            },
        );
        minute_l2.push(minute_last_l2[index].values);
    }
    let mut baseline_columns = vec![
        Series::new("ric".into(), vec![args.ric.clone(); minutes]),
        Series::new("venue".into(), vec![venue.to_string(); minutes]),
        Series::new("ts".into(), &minute_ts),
        Series::new("book_depth".into(), &minute_depth),
        Series::new("bid0p".into(), &minute_bidp),
        Series::new("bid0v".into(), &minute_bidv),
        Series::new("ask0p".into(), &minute_askp),
        Series::new("ask0v".into(), &minute_askv),
    ];
    add_l2_columns(&mut baseline_columns, &minute_l2);
    for (index, minute) in minute_data.iter().enumerate() {
        let total_tolerance = 1e-8_f64.max(minute.amount.abs() * 1e-12);
        if let Some(_) = size_thresholds {
            if (minute.size.total() - minute.amount).abs() > total_tolerance
                || (minute.size.directional_total() - minute.buy_amount - minute.sell_amount).abs()
                    > total_tolerance
            {
                bail!("size bucket conservation failed at minute {index}");
            }
        } else if minute.size.total() != 0.0 || minute.size.directional_total() != 0.0 {
            bail!("size buckets must be zero without prior-month thresholds");
        }
    }
    baseline_columns.extend([
        Series::new(
            "volume".into(),
            minute_data.iter().map(|x| x.volume).collect::<Vec<_>>(),
        ),
        Series::new(
            "amount".into(),
            minute_data.iter().map(|x| x.amount).collect::<Vec<_>>(),
        ),
        Series::new(
            "count".into(),
            minute_data.iter().map(|x| x.count).collect::<Vec<_>>(),
        ),
        Series::new(
            "buy_volume".into(),
            minute_data.iter().map(|x| x.buy_volume).collect::<Vec<_>>(),
        ),
        Series::new(
            "buy_amount".into(),
            minute_data.iter().map(|x| x.buy_amount).collect::<Vec<_>>(),
        ),
        Series::new(
            "buy_count".into(),
            minute_data.iter().map(|x| x.buy_count).collect::<Vec<_>>(),
        ),
        Series::new(
            "buy_high".into(),
            minute_data.iter().map(|x| x.buy_high).collect::<Vec<_>>(),
        ),
        Series::new(
            "sell_volume".into(),
            minute_data
                .iter()
                .map(|x| x.sell_volume)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "sell_amount".into(),
            minute_data
                .iter()
                .map(|x| x.sell_amount)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "sell_count".into(),
            minute_data.iter().map(|x| x.sell_count).collect::<Vec<_>>(),
        ),
        Series::new(
            "sell_low".into(),
            minute_data.iter().map(|x| x.sell_low).collect::<Vec<_>>(),
        ),
        Series::new(
            "off_exchange_volume".into(),
            minute_data
                .iter()
                .map(|x| x.off_exchange_volume)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "off_exchange_amount".into(),
            minute_data
                .iter()
                .map(|x| x.off_exchange_amount)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "off_exchange_count".into(),
            minute_data
                .iter()
                .map(|x| x.off_exchange_count)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "large_order".into(),
            minute_data
                .iter()
                .map(|x| x.size.large_order)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "medium_order".into(),
            minute_data
                .iter()
                .map(|x| x.size.medium_order)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "small_order".into(),
            minute_data
                .iter()
                .map(|x| x.size.small_order)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "large_buy".into(),
            minute_data
                .iter()
                .map(|x| x.size.large_buy)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "large_sell".into(),
            minute_data
                .iter()
                .map(|x| x.size.large_sell)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "medium_buy".into(),
            minute_data
                .iter()
                .map(|x| x.size.medium_buy)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "medium_sell".into(),
            minute_data
                .iter()
                .map(|x| x.size.medium_sell)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "small_buy".into(),
            minute_data
                .iter()
                .map(|x| x.size.small_buy)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "small_sell".into(),
            minute_data
                .iter()
                .map(|x| x.size.small_sell)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "net_buy_large".into(),
            minute_data
                .iter()
                .map(|x| x.size.large_buy - x.size.large_sell)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "net_buy_medium".into(),
            minute_data
                .iter()
                .map(|x| x.size.medium_buy - x.size.medium_sell)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "net_buy_small".into(),
            minute_data
                .iter()
                .map(|x| x.size.small_buy - x.size.small_sell)
                .collect::<Vec<_>>(),
        ),
        Series::new(
            "open".into(),
            minute_data.iter().map(|x| x.open).collect::<Vec<_>>(),
        ),
        Series::new(
            "high".into(),
            minute_data.iter().map(|x| x.high).collect::<Vec<_>>(),
        ),
        Series::new(
            "low".into(),
            minute_data.iter().map(|x| x.low).collect::<Vec<_>>(),
        ),
        Series::new(
            "close".into(),
            minute_data.iter().map(|x| x.close).collect::<Vec<_>>(),
        ),
        Series::new("midp".into(), &minute_midp),
        Series::new("n_seconds".into(), vec![60_i64; minutes]),
    ]);
    write_atomic(&baseline_path, DataFrame::new(baseline_columns)?)?;

    let manifest = json!({
        "schema": "usstock-raw-rth-export",
        "ric": args.ric,
        "venue": venue,
        "session_date": day.to_string(),
        "open_ts": open_ts,
        "close_ts": close_ts,
        "seconds": seconds,
        "minutes": minutes,
        "calendar": args.calendar,
        "calendar_provenance": "pandas_market_calendars NYSE schedule, frozen CSV; not a direct exchange calendar download",
        "raw_rocksdb": args.rocksdb_dir,
        "stage_ll2": if has_stage_ll2 { Some(stage) } else { None },
        "ll2_joined": has_stage_ll2,
        "raw_only": args.raw_only,
        "raw_trade_corrections_applied": false,
        "raw_trade_correction_policy": "raw source prints are exported as observed; cancellation, previous-day and restatement messages remain preserved in RocksDB but do not alter trade prints because CAN_TRD_ID/PD_TRDID cannot be losslessly joined to TRADE_ID",
        "trade_side": "stored replay aggressor_side; never recomputed by exporter; N only off_exchange_reporting",
        "trade_order": "within source second, source_order across all venue and instrument CFs",
        "size_buckets": {
            "audit": size_audit_path,
            "trading_month": size_audit.trading_month,
            "threshold_source_month": size_audit.threshold_source_month,
            "threshold_method": size_audit.threshold_method,
            "threshold_sample_trades": size_audit.threshold_sample_trades,
            "threshold_rocksdb": size_audit.threshold_rocksdb,
            "p50": size_audit.p50,
            "p90": size_audit.p90,
            "off_exchange_policy": size_audit.off_exchange_policy,
            "no_threshold_policy": "all 12 size columns are zero",
        },
        "direction_audit": raw.trades.range((open_ts as u64 * NS)..(close_ts as u64 * NS)).flat_map(|(_, trades)| trades).fold(BTreeMap::<String, (u64, f64)>::new(), |mut counts, trade| {
            let row = counts.entry(format!("method={};flags={}", trade.side_method, trade.side_flags)).or_default();
            row.0 += 1; row.1 += trade.size; counts
        }),
        "side_comparison_window": {
            "start_ts": side_compare_start_ts,
            "end_ts": side_compare_end_ts,
        },
        "side_comparison": side_comparison.as_json(),
        "side_comparison_by_trade_venue": side_comparison_by_venue
            .iter()
            .map(|(venue, summary)| (venue.clone(), summary.as_json()))
            .collect::<serde_json::Map<String, serde_json::Value>>(),
        "quote_timing": "RAW quote and staged LL2 state observed in second t becomes active for t+1",
        "rth_rule": "calendar [open_ts, close_ts), no event outside this interval updates output state",
    });
    write_manifest(
        &args.backtest_out_root,
        venue,
        &args.ric,
        day,
        manifest.clone(),
    )?;
    write_manifest(&args.baseline_out_root, venue, &args.ric, day, manifest)?;
    println!(
        "exported ric={} day={} seconds={} minutes={} backtest={} baseline={}",
        args.ric,
        day,
        seconds,
        minutes,
        backtest_path.display(),
        baseline_path.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instrument_only_ric_opens_without_raw_venue_quotes() {
        let temporary = tempfile::tempdir().unwrap();
        let mut options = Options::default();
        options.create_if_missing(true);
        let mut db = DB::open(&options, temporary.path()).unwrap();
        db.create_cf("i:ABBV.N", &Options::default()).unwrap();
        drop(db);

        let (_db, venue_cfs, instrument_cf) = open_read_only(temporary.path(), "ABBV.N").unwrap();
        assert!(venue_cfs.is_empty());
        assert_eq!(instrument_cf, "i:ABBV.N");
    }

    #[test]
    fn raw_output_suffix_is_mandatory() {
        assert!(
            require_raw_output_root(Path::new("/tmp/backtest_1s_raw"), "backtest_1s_raw").is_ok()
        );
        assert!(require_raw_output_root(Path::new("/tmp/backtest_1s"), "backtest_1s_raw").is_err());
    }

    #[test]
    fn quote_comparison_uses_unclassified_not_unknown() {
        let summary = SideSummary::default().as_json();
        assert!(summary.get("unclassified").is_some());
        assert!(summary.get("unknown").is_none());
    }

    #[test]
    fn previous_month_threshold_audit_is_cached() {
        let temporary = tempfile::tempdir().unwrap();
        let rocksdb = temporary.path().join("raw-rocksdb");
        let mut options = Options::default();
        options.create_if_missing(true);
        let mut db = DB::open(&options, &rocksdb).unwrap();
        db.create_cf("i:AAPL.O", &Options::default()).unwrap();
        let cf = db.cf_handle("i:AAPL.O").unwrap();
        let mut bytes = [0_u8; usstock_lseg_raw_replay::event_codec::TRADE_VALUE_LEN];
        bytes[24..32].copy_from_slice(&100_000_000_000_i64.to_le_bytes());
        bytes[32..40].copy_from_slice(&2_u64.to_le_bytes());
        bytes[90] = b'B';
        bytes[92] = 1;
        bytes[93] = 3;
        bytes[108] = 1;
        db.put_cf(&cf, encode_key(MSG_TRADE, 1_625_059_900 * NS, 1), bytes)
            .unwrap();

        let calendar = temporary.path().join("calendar.csv");
        fs::write(
            &calendar,
            "session_date,open_ts,close_ts\n2021-06-30,1625059800,1625083200\n2021-07-01,1625146200,1625169600\n",
        )
        .unwrap();
        let output = temporary.path().join("baseline_data_1m_raw");
        let (_, first) = load_or_build_size_audit(
            &db,
            &[],
            "i:AAPL.O",
            &rocksdb,
            None,
            &calendar,
            &output,
            "NASDAQ",
            "AAPL.O",
            NaiveDate::from_ymd_opt(2021, 7, 1).unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(first.threshold_sample_trades, 1);
        assert_eq!(first.p50, Some(200.0));
        assert_eq!(first.p90, Some(200.0));
        assert_eq!(first.threshold_rocksdb, rocksdb.display().to_string());

        let (_, cached) = load_or_build_size_audit(
            &db,
            &[],
            "i:AAPL.O",
            &rocksdb,
            None,
            &calendar,
            &output,
            "NASDAQ",
            "AAPL.O",
            NaiveDate::from_ymd_opt(2021, 7, 2).unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(cached.threshold_sample_trades, 1);
        assert_eq!(cached.p50, Some(200.0));
    }

    #[test]
    fn previous_period_supplies_missing_boundary_month_thresholds() {
        let temporary = tempfile::tempdir().unwrap();
        let current = temporary.path().join("current");
        let reference = temporary.path().join("reference");
        let mut options = Options::default();
        options.create_if_missing(true);

        let mut current_db = DB::open(&options, &current).unwrap();
        current_db
            .create_cf("i:AAPL.O", &Options::default())
            .unwrap();

        let mut reference_db = DB::open(&options, &reference).unwrap();
        reference_db
            .create_cf("i:AAPL.O", &Options::default())
            .unwrap();
        let reference_cf = reference_db.cf_handle("i:AAPL.O").unwrap();
        let mut bytes = [0_u8; usstock_lseg_raw_replay::event_codec::TRADE_VALUE_LEN];
        bytes[24..32].copy_from_slice(&100_000_000_000_i64.to_le_bytes());
        bytes[32..40].copy_from_slice(&3_u64.to_le_bytes());
        bytes[90] = b'B';
        bytes[92] = 1;
        bytes[93] = 3;
        bytes[108] = 1;
        reference_db
            .put_cf(
                &reference_cf,
                encode_key(MSG_TRADE, 1_625_059_900 * NS, 1),
                bytes,
            )
            .unwrap();
        drop(reference_db);

        let calendar = temporary.path().join("calendar.csv");
        fs::write(
            &calendar,
            "session_date,open_ts,close_ts\n2021-06-30,1625059800,1625083200\n2021-07-01,1625146200,1625169600\n",
        )
        .unwrap();
        let output = temporary.path().join("baseline_data_1m_raw");
        let (_, audit) = load_or_build_size_audit(
            &current_db,
            &[],
            "i:AAPL.O",
            &current,
            Some(&reference),
            &calendar,
            &output,
            "NASDAQ",
            "AAPL.O",
            NaiveDate::from_ymd_opt(2021, 7, 1).unwrap(),
            false,
        )
        .unwrap();
        assert_eq!(audit.schema, "usstock-raw-size-threshold-v2");
        assert_eq!(audit.threshold_sample_trades, 1);
        assert_eq!(audit.p50, Some(300.0));
        assert_eq!(audit.threshold_rocksdb, reference.display().to_string());
    }

    #[test]
    fn export_uses_stored_direction_and_rejects_unfinished_values() {
        let mut bytes = [0_u8; usstock_lseg_raw_replay::event_codec::TRADE_VALUE_LEN];
        bytes[24..32].copy_from_slice(&101_000_000_000_i64.to_le_bytes());
        bytes[32..40].copy_from_slice(&10_u64.to_le_bytes());
        bytes[88..90].copy_from_slice(&1_u16.to_le_bytes());
        bytes[90] = b'S';
        bytes[92] = 1;
        bytes[93] = 1;
        bytes[108] = 3;
        let trade = stored_trade(decode_trade(&bytes).unwrap(), "BAT")
            .unwrap()
            .unwrap();
        assert_eq!(
            infer_side(Some((100.0, 1.0, 101.0, 1.0)), trade.price),
            Some(true)
        );
        assert_eq!(trade.side, Some(false));
        let mut minute = Minute::new();
        minute.add(&trade, None).unwrap();
        assert_eq!((minute.buy_volume, minute.sell_volume), (0.0, 10.0));
        bytes[90] = b'N';
        bytes[91] = 1;
        bytes[92] = 2;
        bytes[93] = 9;
        bytes[108] = 0;
        minute
            .add(
                &stored_trade(decode_trade(&bytes).unwrap(), "ADF")
                    .unwrap()
                    .unwrap(),
                None,
            )
            .unwrap();
        assert_eq!(minute.off_exchange_volume, 10.0);
        assert_eq!(
            minute.volume,
            minute.buy_volume + minute.sell_volume + minute.off_exchange_volume
        );
        let mut invalid = decode_trade(&bytes).unwrap();
        invalid.side_method = 3;
        assert!(stored_trade(invalid, "ADF").is_err());
        let mut invalid = decode_trade(&bytes).unwrap();
        invalid.venue_class = 1;
        assert!(stored_trade(invalid, "ADF").is_err());
        bytes[93] = 0;
        assert!(stored_trade(decode_trade(&bytes).unwrap(), "ADF").is_err());
        bytes[93] = 9;
        bytes[32..40].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(stored_trade(decode_trade(&bytes).unwrap(), "ADF")
            .unwrap()
            .is_none());
    }

    #[test]
    fn minute_size_buckets_include_off_exchange_only_in_total() {
        let thresholds = SizeThresholds::new(500.0, 900.0).unwrap();
        let mut minute = Minute::new();
        let trade = |price, size, side| Trade {
            price,
            size,
            venue: String::new(),
            source_order: 0,
            side,
            side_method: if side.is_some() { 3 } else { 9 },
            side_flags: 0,
        };
        minute
            .add(&trade(10.0, 40.0, Some(true)), Some(thresholds))
            .unwrap();
        minute
            .add(&trade(10.0, 60.0, Some(false)), Some(thresholds))
            .unwrap();
        minute
            .add(&trade(10.0, 100.0, None), Some(thresholds))
            .unwrap();
        assert_eq!(minute.size.small_buy, 400.0);
        assert_eq!(minute.size.medium_sell, 600.0);
        assert_eq!(minute.size.large_order, 1000.0);
        assert_eq!(minute.size.large_buy + minute.size.large_sell, 0.0);
        assert_eq!(minute.size.total(), minute.amount);
        assert_eq!(
            minute.size.directional_total(),
            minute.buy_amount + minute.sell_amount
        );
    }

    #[test]
    fn composite_snapshot_replaces_stale_venue_contributions() {
        let mut book = CompositeBook::default();
        book.push(QuoteValue {
            source_ts_utc_ns: 1,
            source_order: 1,
            bid: 100_000_000_000,
            bid_size: 10,
            ask: 101_000_000_000,
            ask_size: 10,
        })
        .unwrap();
        assert_eq!(book.bbo().unwrap().0, 100.0);
        book.reset_snapshot();
        book.push(QuoteValue {
            source_ts_utc_ns: 2,
            source_order: 2,
            bid: 99_000_000_000,
            bid_size: 5,
            ask: 102_000_000_000,
            ask_size: 5,
        })
        .unwrap();
        assert_eq!(book.bbo().unwrap().0, 99.0);
    }

    #[test]
    fn touch_side_leaves_inside_trade_unclassified() {
        let book = Some((100.0, 1.0, 101.0, 1.0));
        assert_eq!(infer_side(book, 101.0), Some(true));
        assert_eq!(infer_side(book, 100.0), Some(false));
        assert_eq!(infer_side(book, 100.5), None);
    }

    #[test]
    fn venue_quote_test_never_uses_another_venue() {
        let mut books = VenueBooks::default();
        books.push(
            "NAS",
            QuoteValue {
                source_ts_utc_ns: 1,
                source_order: 1,
                bid: 100_000_000_000,
                bid_size: 10,
                ask: 101_000_000_000,
                ask_size: 10,
            },
        );
        books.push(
            "ADF",
            QuoteValue {
                source_ts_utc_ns: 1,
                source_order: 1,
                bid: 99_000_000_000,
                bid_size: 10,
                ask: 102_000_000_000,
                ask_size: 10,
            },
        );
        assert_eq!(infer_side(books.bbo("NAS"), 101.0), Some(true));
        assert_eq!(infer_side(books.bbo("ADF"), 101.0), None);
        assert_eq!(books.bbo("NYS"), None);
    }
}
