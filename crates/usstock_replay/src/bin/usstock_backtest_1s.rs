//! TAS gzip → NYSE RTH 1s parquet, one worker per gzip part.
//!
//! Logic is a line-for-line port of preprocess/lseg/usstock_backtest_1s.py
//! SessionDay / RicStream.  A RIC that is cut by a part boundary has only its
//! split session-day written as a sidecar; interior days stay final parquet.

use anyhow::{anyhow, bail, Context, Result};
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use clap::Parser;
use csv::StringRecord;
use flate2::read::MultiGzDecoder;
use polars::prelude::{DataFrame, NamedFrom, ParquetWriter, Series};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

const NS: i64 = 1_000_000_000;
const UNSET: u8 = 0;
const PENDING_NONE: u8 = 1;
const PENDING_BOOK: u8 = 2;

const PERIODS: &[&str] = &[
    "2021-07-01_2022-07-01",
    "2022-07-01_2023-07-01",
    "2023-07-01_2024-06-30",
    "2024-06-30_2025-07-01",
    "2025-07-01_2026-07-01",
];

#[derive(Parser, Debug)]
#[command(name = "usstock_backtest_1s")]
struct Args {
    #[arg(
        long,
        default_value = "/mnt/hdd-raid5-72t/liang_torch/usstock_data/raw_data/normalised"
    )]
    data_root: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/liang_torch/usstock_data/backtest_1s"
    )]
    out_root: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/liang_torch/usstock_data/work/nyse_rth_20210628_20260705.csv"
    )]
    calendar: PathBuf,
    #[arg(
        long,
        default_value = "/mnt/nvme-raid0-28t/liang_torch/usstock_data/work/sidecars"
    )]
    sidecar_root: PathBuf,
    #[arg(long)]
    periods: Vec<String>,
    #[arg(long)]
    rics: Vec<String>,
    #[arg(long)]
    start_date: Option<String>,
    #[arg(long)]
    end_date: Option<String>,
    #[arg(long, default_value_t = 0)]
    max_source_rows: u64,
    #[arg(long, default_value_t = 13)]
    workers: usize,
    #[arg(long, default_value_t = 5_000_000)]
    progress_every: u64,
}

#[derive(Debug, Clone)]
struct ColIdx {
    ric: usize,
    date_time: usize,
    event_type: usize,
    price: usize,
    volume: usize,
    bid_price: usize,
    bid_size: usize,
    ask_price: usize,
    ask_size: usize,
    qualifiers: usize,
}

impl ColIdx {
    fn from_headers(headers: &StringRecord) -> Result<Self> {
        let find = |name: &str| -> Result<usize> {
            headers
                .iter()
                .position(|h| h.trim() == name)
                .ok_or_else(|| anyhow!("missing column {name}"))
        };
        Ok(Self {
            ric: find("#RIC")?,
            date_time: find("Date-Time")?,
            event_type: find("Type")?,
            price: find("Price")?,
            volume: find("Volume")?,
            bid_price: find("Bid Price")?,
            bid_size: find("Bid Size")?,
            ask_price: find("Ask Price")?,
            ask_size: find("Ask Size")?,
            qualifiers: find("Qualifiers")?,
        })
    }
}

#[derive(Debug, Deserialize)]
struct CalendarCsvRow {
    session_date: String,
    open_ts: i64,
    close_ts: i64,
}

#[derive(Debug, Clone)]
struct SessionCal {
    days: BTreeMap<NaiveDate, (i64, i64)>,
}

impl SessionCal {
    fn load(path: &Path) -> Result<Self> {
        let mut reader = csv::Reader::from_path(path)
            .with_context(|| format!("open calendar {}", path.display()))?;
        let mut days = BTreeMap::new();
        for rec in reader.deserialize() {
            let row: CalendarCsvRow = rec.with_context(|| format!("parse {}", path.display()))?;
            let day = NaiveDate::parse_from_str(&row.session_date, "%Y-%m-%d")
                .with_context(|| format!("session_date {}", row.session_date))?;
            days.insert(day, (row.open_ts, row.close_ts));
        }
        if days.is_empty() {
            bail!("calendar {} is empty", path.display());
        }
        Ok(Self { days })
    }

    fn get(&self, day: NaiveDate) -> Option<(i64, i64)> {
        self.days.get(&day).copied()
    }
}

fn parse_utc_ns(raw: &str) -> Result<i64> {
    let (main, frac) = match raw.split_once('.') {
        Some((m, f)) => (m, f),
        None => (raw.trim_end_matches('Z'), ""),
    };
    let sec = DateTime::parse_from_rfc3339(&format!("{main}Z"))
        .or_else(|_| DateTime::parse_from_rfc3339(raw))
        .with_context(|| format!("Date-Time {raw}"))?
        .timestamp();
    let digits = frac.trim_end_matches('Z');
    if digits.is_empty() {
        return Ok(sec * NS);
    }
    let mut nanos = digits.as_bytes().to_vec();
    while nanos.len() < 9 {
        nanos.push(b'0');
    }
    let nano: i64 = std::str::from_utf8(&nanos[..9])?.parse()?;
    Ok(sec * NS + nano)
}

fn parse_f64(raw: &str) -> f64 {
    if raw.is_empty() {
        return f64::NAN;
    }
    raw.parse().unwrap_or(f64::NAN)
}

fn is_clear_quote(qualifiers: &str) -> bool {
    qualifiers.contains("CLS[PRC_QL_CD]") || qualifiers.contains("Exhausted Bid and Ask[USER]")
}

type Book = (f64, f64, f64, f64);

fn valid_book(bid_p: f64, bid_v: f64, ask_p: f64, ask_v: f64) -> Option<Book> {
    if ![bid_p, bid_v, ask_p, ask_v].iter().all(|x| x.is_finite()) {
        return None;
    }
    if bid_p <= 0.0 || ask_p <= 0.0 || bid_v < 0.0 || ask_v < 0.0 {
        return None;
    }
    if ask_p < bid_p {
        return None;
    }
    Some((bid_p, bid_v, ask_p, ask_v))
}

fn merge_book(
    prev: Option<Book>,
    bid_p: f64,
    bid_v: f64,
    ask_p: f64,
    ask_v: f64,
    cleared: bool,
) -> Option<Book> {
    if cleared {
        return None;
    }
    let (mut bp, mut bv, mut ap, mut av) = prev.unwrap_or((f64::NAN, f64::NAN, f64::NAN, f64::NAN));
    if bid_p.is_finite() && bid_v.is_finite() {
        if bid_p <= 0.0 || bid_v <= 0.0 {
            bp = f64::NAN;
            bv = f64::NAN;
        } else {
            bp = bid_p;
            bv = bid_v;
        }
    }
    if ask_p.is_finite() && ask_v.is_finite() {
        if ask_p <= 0.0 || ask_v <= 0.0 {
            ap = f64::NAN;
            av = f64::NAN;
        } else {
            ap = ask_p;
            av = ask_v;
        }
    }
    valid_book(bp, bv, ap, av)
}

fn infer_side(book: Option<Book>, price: f64) -> Option<bool> {
    let (bid_p, _, ask_p, _) = book?;
    if price >= ask_p {
        return Some(true);
    }
    if price <= bid_p {
        return Some(false);
    }
    None
}

fn venue_of(ric: &str) -> &'static str {
    if ric.ends_with(".O") {
        "NASDAQ"
    } else if ric.ends_with(".N") {
        "NYSE"
    } else if ric.ends_with(".P") {
        "ARCA"
    } else if ric.ends_with(".BAT") {
        "BZX"
    } else {
        "OTHER"
    }
}

fn ny_date_of_ns(ts_ns: i64) -> NaiveDate {
    let sec = ts_ns.div_euclid(NS);
    let dt = Utc.timestamp_opt(sec, 0).single().expect("utc");
    dt.with_timezone(&New_York).date_naive()
}

fn day_path(out_root: &Path, ric: &str, session_date: NaiveDate) -> PathBuf {
    out_root
        .join(venue_of(ric))
        .join(ric)
        .join(format!("{}.parquet", session_date.format("%Y%m%d")))
}

struct SessionDay {
    session_date: NaiveDate,
    open_ts: i64,
    close_ts: i64,
    last_book: Option<Book>,
    last_close: f64,
    next_ts: i64,
    acc_buy_high: f64,
    acc_sell_low: f64,
    acc_close: f64,
    pending_kind: u8,
    pending_book: Option<Book>,
    ts: Vec<i64>,
    bid0p: Vec<f64>,
    bid0v: Vec<f64>,
    ask0p: Vec<f64>,
    ask0v: Vec<f64>,
    buy_high: Vec<f64>,
    sell_low: Vec<f64>,
    close: Vec<f64>,
    midp: Vec<f64>,
}

impl SessionDay {
    fn new(session_date: NaiveDate, open_ts: i64, close_ts: i64) -> Self {
        Self {
            session_date,
            open_ts,
            close_ts,
            last_book: None,
            last_close: f64::NAN,
            next_ts: open_ts,
            acc_buy_high: f64::NAN,
            acc_sell_low: f64::NAN,
            acc_close: f64::NAN,
            pending_kind: UNSET,
            pending_book: None,
            ts: Vec::new(),
            bid0p: Vec::new(),
            bid0v: Vec::new(),
            ask0p: Vec::new(),
            ask0v: Vec::new(),
            buy_high: Vec::new(),
            sell_low: Vec::new(),
            close: Vec::new(),
            midp: Vec::new(),
        }
    }

    fn apply_quote(&mut self, event_sec: i64, book: Option<Book>) {
        if event_sec >= self.close_ts {
            return;
        }
        if event_sec < self.next_ts {
            self.last_book = book;
            self.pending_kind = UNSET;
            return;
        }
        self.flush_until(event_sec);
        self.pending_kind = PENDING_BOOK;
        self.pending_book = book;
    }

    fn apply_trade(&mut self, event_sec: i64, price: f64, volume: f64) {
        if event_sec < self.open_ts || event_sec >= self.close_ts {
            return;
        }
        if !(price.is_finite() && price > 0.0 && volume.is_finite() && volume > 0.0) {
            return;
        }
        self.flush_until(event_sec);
        match infer_side(self.last_book, price) {
            Some(true) => {
                self.acc_buy_high = if self.acc_buy_high.is_nan() {
                    price
                } else {
                    self.acc_buy_high.max(price)
                };
            }
            Some(false) => {
                self.acc_sell_low = if self.acc_sell_low.is_nan() {
                    price
                } else {
                    self.acc_sell_low.min(price)
                };
            }
            None => {}
        }
        self.acc_close = price;
    }

    fn finish(&mut self) {
        self.flush_until(self.close_ts);
    }

    fn flush_until(&mut self, target: i64) {
        let end = target.min(self.close_ts);
        while self.next_ts < end {
            let (bp, bv, ap, av, mid) = match self.last_book {
                None => (f64::NAN, f64::NAN, f64::NAN, f64::NAN, f64::NAN),
                Some((bp, bv, ap, av)) => (bp, bv, ap, av, (bp + ap) / 2.0),
            };
            let mut close = self.acc_close;
            if close.is_nan() {
                close = if !self.last_close.is_nan() {
                    self.last_close
                } else {
                    mid
                };
            }
            self.ts.push(self.next_ts);
            self.bid0p.push(bp);
            self.bid0v.push(bv);
            self.ask0p.push(ap);
            self.ask0v.push(av);
            self.buy_high.push(self.acc_buy_high);
            self.sell_low.push(self.acc_sell_low);
            self.close.push(close);
            self.midp.push(mid);
            if !close.is_nan() {
                self.last_close = close;
            }
            if self.pending_kind != UNSET {
                self.last_book = if self.pending_kind == PENDING_NONE {
                    None
                } else {
                    self.pending_book
                };
                self.pending_kind = UNSET;
            }
            self.acc_buy_high = f64::NAN;
            self.acc_sell_low = f64::NAN;
            self.acc_close = f64::NAN;
            self.next_ts += 1;
        }
    }

    fn current_book(&self) -> Option<Book> {
        if self.pending_kind == PENDING_BOOK {
            self.pending_book
        } else if self.pending_kind == PENDING_NONE {
            None
        } else {
            self.last_book
        }
    }
}

fn write_day(out_root: &Path, ric: &str, day: &SessionDay) -> Result<()> {
    if day.ts.is_empty() {
        return Ok(());
    }
    let dest = day_path(out_root, ric, day.session_date);
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = dest.with_extension("parquet.tmp");
    let n = day.ts.len();
    let mut df = DataFrame::new(vec![
        Series::new("ric".into(), vec![ric.to_string(); n]),
        Series::new("ts".into(), &day.ts),
        Series::new("bid0p".into(), &day.bid0p),
        Series::new("bid0v".into(), &day.bid0v),
        Series::new("ask0p".into(), &day.ask0p),
        Series::new("ask0v".into(), &day.ask0v),
        Series::new("buy_high".into(), &day.buy_high),
        Series::new("sell_low".into(), &day.sell_low),
        Series::new("close".into(), &day.close),
        Series::new("midp".into(), &day.midp),
    ])
    .context("build parquet frame")?;
    let file = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    ParquetWriter::new(file)
        .finish(&mut df)
        .with_context(|| format!("write {}", tmp.display()))?;
    fs::rename(&tmp, &dest).with_context(|| format!("rename {}", dest.display()))?;
    Ok(())
}

#[derive(Clone, Copy)]
struct RawEvent {
    ts_ns: i64,
    kind: u8,
    price: f64,
    volume: f64,
    bid_p: f64,
    bid_v: f64,
    ask_p: f64,
    ask_v: f64,
    cleared: bool,
}

fn write_sidecar(path: &Path, events: &[RawEvent]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = File::create(path).with_context(|| format!("create {}", path.display()))?;
    let mut writer = BufWriter::with_capacity(8 * 1024 * 1024, file);
    for ev in events {
        writeln!(
            writer,
            "{},{},{},{},{},{},{},{},{}",
            ev.ts_ns,
            ev.kind,
            ev.price,
            ev.volume,
            ev.bid_p,
            ev.bid_v,
            ev.ask_p,
            ev.ask_v,
            if ev.cleared { 1 } else { 0 }
        )?;
    }
    writer.flush()?;
    Ok(())
}

struct RicStream {
    ric: String,
    sessions: Arc<SessionCal>,
    out_root: PathBuf,
    start_day: Option<NaiveDate>,
    end_day: Option<NaiveDate>,
    day: Option<SessionDay>,
    written: u64,
    quotes: u64,
    trades: u64,
    first_session: Option<NaiveDate>,
    last_session: Option<NaiveDate>,
    capture_head: bool,
    sidecar_root: PathBuf,
    period: String,
    part_no: u16,
    day_events: Vec<RawEvent>,
    record_events: bool,
}

impl RicStream {
    fn new(
        ric: String,
        sessions: Arc<SessionCal>,
        out_root: PathBuf,
        start_day: Option<NaiveDate>,
        end_day: Option<NaiveDate>,
        sidecar_root: PathBuf,
        period: String,
        part_no: u16,
        capture_head: bool,
    ) -> Self {
        Self {
            ric,
            sessions,
            out_root,
            start_day,
            end_day,
            day: None,
            written: 0,
            quotes: 0,
            trades: 0,
            first_session: None,
            last_session: None,
            capture_head,
            sidecar_root,
            period,
            part_no,
            day_events: Vec::new(),
            record_events: true,
        }
    }

    fn sidecar_path(&self, day: NaiveDate, side: &str) -> PathBuf {
        self.sidecar_root.join(format!(
            "{}__part{:06}__{}__{}__{}.csv",
            self.period,
            self.part_no,
            self.ric,
            day.format("%Y%m%d"),
            side
        ))
    }

    fn on_quote(
        &mut self,
        ts_ns: i64,
        book: Option<Book>,
        bid_p: f64,
        bid_v: f64,
        ask_p: f64,
        ask_v: f64,
        cleared: bool,
    ) -> Result<()> {
        if !self.ensure_day(ts_ns)? {
            return Ok(());
        }
        self.quotes += 1;
        if let Some(day) = self.day.as_mut() {
            day.apply_quote(ts_ns / NS, book);
        }
        if self.record_events {
            self.day_events.push(RawEvent {
                ts_ns,
                kind: 2,
                price: f64::NAN,
                volume: f64::NAN,
                bid_p,
                bid_v,
                ask_p,
                ask_v,
                cleared,
            });
        }
        Ok(())
    }

    fn on_trade(&mut self, ts_ns: i64, price: f64, volume: f64) -> Result<()> {
        if !self.ensure_day(ts_ns)? {
            return Ok(());
        }
        self.trades += 1;
        if let Some(day) = self.day.as_mut() {
            day.apply_trade(ts_ns / NS, price, volume);
        }
        if self.record_events {
            self.day_events.push(RawEvent {
                ts_ns,
                kind: 1,
                price,
                volume,
                bid_p: f64::NAN,
                bid_v: f64::NAN,
                ask_p: f64::NAN,
                ask_v: f64::NAN,
                cleared: false,
            });
        }
        Ok(())
    }

    fn current_book(&self) -> Option<Book> {
        self.day.as_ref().and_then(SessionDay::current_book)
    }

    fn finish(&mut self, write_last_as_sidecar: bool) -> Result<()> {
        self.flush_day_inner(write_last_as_sidecar)
    }

    fn ensure_day(&mut self, ts_ns: i64) -> Result<bool> {
        let ny_day = ny_date_of_ns(ts_ns);
        if self.start_day.is_some_and(|d| ny_day < d) {
            return Ok(false);
        }
        if self.end_day.is_some_and(|d| ny_day > d) {
            self.flush_day_inner(false)?;
            return Ok(false);
        }
        let Some(bounds) = self.sessions.get(ny_day) else {
            if self.day.as_ref().is_some_and(|d| d.session_date != ny_day) {
                self.flush_day_inner(false)?;
            }
            return Ok(false);
        };
        let event_sec = ts_ns / NS;
        if event_sec >= bounds.1 {
            if self.day.as_ref().is_some_and(|d| d.session_date != ny_day) {
                self.flush_day_inner(false)?;
            }
            return Ok(false);
        }
        if self.day.as_ref().is_some_and(|d| d.session_date == ny_day) {
            return Ok(true);
        }
        self.flush_day_inner(false)?;
        self.day = Some(SessionDay::new(ny_day, bounds.0, bounds.1));
        self.day_events.clear();
        if self.first_session.is_none() {
            self.first_session = Some(ny_day);
        }
        self.last_session = Some(ny_day);
        Ok(true)
    }

    fn flush_day(&mut self) -> Result<()> {
        self.flush_day_inner(false)
    }

    fn flush_day_inner(&mut self, as_tail_sidecar: bool) -> Result<()> {
        let Some(mut day) = self.day.take() else {
            self.day_events.clear();
            return Ok(());
        };
        let as_head_sidecar = self.capture_head && Some(day.session_date) == self.first_session;
        if as_head_sidecar || as_tail_sidecar {
            let side = if as_head_sidecar { "head" } else { "tail" };
            write_sidecar(&self.sidecar_path(day.session_date, side), &self.day_events)?;
            self.day_events.clear();
            return Ok(());
        }
        day.finish();
        if !day.ts.is_empty() {
            write_day(&self.out_root, &self.ric, &day)?;
            self.written += 1;
        }
        self.day_events.clear();
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Job {
    period: String,
    part_no: u16,
    path: PathBuf,
    header_path: PathBuf,
    has_header: bool,
}

fn period_dir(data_root: &Path, period: &str) -> PathBuf {
    data_root.join(format!(
        "shanghai_evolution_equities_time_and_sales_ric_list_0_tas_{period}"
    ))
}

fn part_number(path: &Path) -> Result<u16> {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("bad part path"))?;
    let number = name
        .strip_prefix("merged-Data-part-")
        .and_then(|n| n.strip_suffix(".csv.gz"))
        .ok_or_else(|| anyhow!("unrecognized {name}"))?;
    Ok(number.parse()?)
}

fn discover_jobs(data_root: &Path, periods: &[String]) -> Result<Vec<Job>> {
    let mut jobs = Vec::new();
    for period in periods {
        let dir = period_dir(data_root, period);
        let single = dir.join("merged-Data.csv.gz");
        let mut parts = Vec::new();
        for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if name.starts_with("merged-Data-part-") && name.ends_with(".csv.gz") {
                parts.push((part_number(&path)?, path));
            }
        }
        if single.is_file() {
            jobs.push(Job {
                period: period.clone(),
                part_no: 0,
                header_path: single.clone(),
                path: single,
                has_header: true,
            });
            continue;
        }
        if parts.is_empty() {
            bail!("{} has no TAS gzip", dir.display());
        }
        parts.sort_by_key(|(n, _)| *n);
        let header_path = parts[0].1.clone();
        let last_no = parts.last().map(|(n, _)| *n).unwrap();
        for (part_no, path) in parts {
            jobs.push(Job {
                period: period.clone(),
                part_no,
                has_header: part_no == 0,
                path,
                header_path: header_path.clone(),
            });
            let _ = last_no;
        }
    }
    Ok(jobs)
}

fn read_header(path: &Path) -> Result<StringRecord> {
    let file = File::open(path)?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(16 * 1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);
    let headers = reader.records().next().ok_or_else(|| anyhow!("empty"))??;
    if headers.get(0).map(str::trim) != Some("#RIC") {
        bail!("{} missing #RIC header", path.display());
    }
    Ok(headers)
}

fn load_active_rics(period: &str, data_root: &Path) -> Result<BTreeSet<String>> {
    let path = period_dir(data_root, period).join("merged-Report.csv.gz");
    let file = File::open(&path).with_context(|| format!("open {}", path.display()))?;
    let decoder = MultiGzDecoder::new(BufReader::new(file));
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(decoder);
    let mut out = BTreeSet::new();
    for rec in reader.deserialize::<BTreeMap<String, String>>() {
        let rec = rec?;
        let ric = rec.get("#RIC").map(|s| s.as_str()).unwrap_or("");
        let status = rec.get("Status").map(|s| s.as_str()).unwrap_or("");
        let count = rec.get("Count").map(|s| s.as_str()).unwrap_or("");
        if !ric.is_empty() && status == "Active" && count != "" && count != "0" {
            out.insert(ric.to_string());
        }
    }
    Ok(out)
}

fn log_line(msg: &str) {
    let now = Utc::now().format("%Y-%m-%dT%H:%M:%SZ");
    println!("{now} {msg}");
    let _ = std::io::stdout().flush();
}

fn process_job(
    job: &Job,
    out_root: &Path,
    sidecar_root: &Path,
    sessions: Arc<SessionCal>,
    allow: Option<Arc<BTreeSet<String>>>,
    start_day: Option<NaiveDate>,
    end_day: Option<NaiveDate>,
    max_source_rows: u64,
    progress_every: u64,
    abort: &AtomicBool,
    is_first_part: bool,
    is_last_part: bool,
) -> Result<(u64, String, String)> {
    let _ = (is_first_part, is_last_part);
    let headers = if job.has_header {
        read_header(&job.path)?
    } else {
        read_header(&job.header_path)?
    };
    let idx = ColIdx::from_headers(&headers)?;
    let file = File::open(&job.path)?;
    let decoder = MultiGzDecoder::new(BufReader::with_capacity(16 * 1024 * 1024, file));
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(decoder);
    if job.has_header {
        let file_header = reader.records().next().ok_or_else(|| anyhow!("empty"))??;
        if file_header.get(0).map(str::trim) != Some("#RIC") {
            bail!("{} header mismatch", job.path.display());
        }
    }

    let mut current: Option<RicStream> = None;
    let mut rows = 0_u64;
    let mut first_ric = String::new();
    let mut last_ric = String::new();
    let started = Instant::now();
    let last_allow = allow.as_ref().and_then(|s| s.iter().next_back().cloned());
    let mut remaining: Option<BTreeSet<String>> = allow.as_ref().map(|s| (**s).clone());

    for result in reader.records() {
        if abort.load(Ordering::Relaxed) {
            bail!("aborted");
        }
        if max_source_rows > 0 && rows >= max_source_rows {
            break;
        }
        let rec = result?;
        rows += 1;
        let ric = rec.get(idx.ric).unwrap_or("").trim();
        if ric.is_empty() {
            continue;
        }
        if first_ric.is_empty() {
            first_ric = ric.to_string();
        }
        last_ric = ric.to_string();
        if let Some(last) = last_allow.as_deref() {
            if ric > last {
                if let Some(mut prev) = current.take() {
                    prev.finish(false)?;
                }
                break;
            }
        }
        if allow.as_ref().is_some_and(|s| !s.contains(ric)) {
            continue;
        }
        if current.as_ref().is_none_or(|c| c.ric != ric) {
            if let Some(mut prev) = current.take() {
                if let Some(left) = remaining.as_mut() {
                    left.remove(&prev.ric);
                }
                prev.finish(false)?;
                if remaining.as_ref().is_some_and(|s| s.is_empty()) {
                    break;
                }
            }
            let stream = RicStream::new(
                ric.to_string(),
                sessions.clone(),
                out_root.to_path_buf(),
                start_day,
                end_day,
                sidecar_root.to_path_buf(),
                job.period.clone(),
                job.part_no,
                !is_first_part && first_ric == ric,
            );
            current = Some(stream);
        }
        let kind = rec.get(idx.event_type).unwrap_or("").trim();
        let ts_raw = rec.get(idx.date_time).unwrap_or("").trim();
        let ts_ns = match parse_utc_ns(ts_raw) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if kind == "Quote" {
            let bid_p = parse_f64(rec.get(idx.bid_price).unwrap_or("").trim());
            let bid_v = parse_f64(rec.get(idx.bid_size).unwrap_or("").trim());
            let ask_p = parse_f64(rec.get(idx.ask_price).unwrap_or("").trim());
            let ask_v = parse_f64(rec.get(idx.ask_size).unwrap_or("").trim());
            let cleared = is_clear_quote(rec.get(idx.qualifiers).unwrap_or(""));
            let stream = current.as_mut().unwrap();
            let book = merge_book(stream.current_book(), bid_p, bid_v, ask_p, ask_v, cleared);
            stream.on_quote(ts_ns, book, bid_p, bid_v, ask_p, ask_v, cleared)?;
        } else if kind == "Trade" {
            let price = parse_f64(rec.get(idx.price).unwrap_or("").trim());
            let volume = parse_f64(rec.get(idx.volume).unwrap_or("").trim());
            current.as_mut().unwrap().on_trade(ts_ns, price, volume)?;
        }
        if progress_every > 0 && rows % progress_every == 0 {
            let elapsed = started.elapsed().as_secs_f64().max(0.001);
            log_line(&format!(
                "progress period={} part={} rows={} ric={} rows_per_s={:.0}",
                job.period,
                job.part_no,
                rows,
                ric,
                rows as f64 / elapsed
            ));
        }
        if let (Some(end), Some(cur)) = (end_day, current.as_ref()) {
            if cur.written > 0 && ny_date_of_ns(ts_ns) > end {
                break;
            }
        }
    }
    if let Some(mut prev) = current.take() {
        prev.finish(!is_last_part)?;
    }
    let elapsed = started.elapsed().as_secs_f64().max(0.001);
    log_line(&format!(
        "DONE period={} part={} rows={} first={} last={} rows_per_s={:.0} elapsed={elapsed:.0}s",
        job.period,
        job.part_no,
        rows,
        first_ric,
        last_ric,
        rows as f64 / elapsed
    ));
    Ok((rows, first_ric, last_ric))
}

/// Replay a sidecar (or several, in part order) through the same SessionDay.
fn replay_sidecar(
    paths: &[PathBuf],
    ric: &str,
    sessions: &SessionCal,
    out_root: &Path,
) -> Result<u64> {
    let mut stream = RicStream::new(
        ric.to_string(),
        Arc::new(sessions.clone()),
        out_root.to_path_buf(),
        None,
        None,
        PathBuf::new(),
        String::new(),
        0,
        false,
    );
    for path in paths {
        let file = File::open(path).with_context(|| format!("open sidecar {}", path.display()))?;
        for line in BufReader::new(file).lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let cols: Vec<&str> = line.split(',').collect();
            if cols.len() < 9 {
                continue;
            }
            let ts_ns: i64 = cols[0].parse()?;
            let kind: u8 = cols[1].parse()?;
            if kind == 2 {
                let bid_p = parse_f64(cols[4]);
                let bid_v = parse_f64(cols[5]);
                let ask_p = parse_f64(cols[6]);
                let ask_v = parse_f64(cols[7]);
                let cleared = cols[8] == "1";
                let book = merge_book(stream.current_book(), bid_p, bid_v, ask_p, ask_v, cleared);
                stream.on_quote(ts_ns, book, bid_p, bid_v, ask_p, ask_v, cleared)?;
            } else if kind == 1 {
                stream.on_trade(ts_ns, parse_f64(cols[2]), parse_f64(cols[3]))?;
            }
        }
    }
    stream.finish(false)?;
    Ok(stream.written)
}

fn parse_day(raw: &Option<String>) -> Result<Option<NaiveDate>> {
    match raw {
        None => Ok(None),
        Some(s) => Ok(Some(NaiveDate::parse_from_str(s, "%Y-%m-%d")?)),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    let periods = if args.periods.is_empty() {
        PERIODS.iter().map(|s| s.to_string()).collect::<Vec<_>>()
    } else {
        args.periods.clone()
    };
    let start_day = parse_day(&args.start_date)?;
    let end_day = parse_day(&args.end_date)?;
    let sessions = Arc::new(SessionCal::load(&args.calendar)?);
    fs::create_dir_all(&args.out_root)?;
    fs::create_dir_all(&args.sidecar_root)?;
    let jobs = discover_jobs(&args.data_root, &periods)?;
    let allow = if args.rics.is_empty() {
        None
    } else {
        Some(Arc::new(args.rics.iter().cloned().collect::<BTreeSet<_>>()))
    };
    let abort = Arc::new(AtomicBool::new(false));
    let workers = args.workers.max(1).min(jobs.len().max(1));
    log_line(&format!(
        "start jobs={} workers={} periods={periods:?} rics={:?} max_source_rows={}",
        jobs.len(),
        workers,
        args.rics,
        args.max_source_rows
    ));

    // Group jobs by period so we know first/last part.
    let mut by_period: BTreeMap<String, Vec<Job>> = BTreeMap::new();
    for job in jobs {
        by_period.entry(job.period.clone()).or_default().push(job);
    }
    let mut all_jobs = Vec::new();
    for jobs in by_period.values_mut() {
        jobs.sort_by_key(|j| j.part_no);
        let first = jobs.first().map(|j| j.part_no);
        let last = jobs.last().map(|j| j.part_no);
        for job in jobs {
            all_jobs.push((
                job.clone(),
                first == Some(job.part_no),
                last == Some(job.part_no),
            ));
        }
    }

    let (tx, rx) = std::sync::mpsc::channel::<JobTask>();
    struct JobTask {
        job: Job,
        is_first: bool,
        is_last: bool,
    }
    for (job, is_first, is_last) in all_jobs {
        tx.send(JobTask {
            job,
            is_first,
            is_last,
        })
        .ok();
    }
    drop(tx);

    let rx = Arc::new(std::sync::Mutex::new(rx));
    let mut handles = Vec::new();
    for _ in 0..workers {
        let rx = rx.clone();
        let out_root = args.out_root.clone();
        let sidecar_root = args.sidecar_root.clone();
        let sessions = sessions.clone();
        let allow = allow.clone();
        let abort = abort.clone();
        let max_source_rows = args.max_source_rows;
        let progress_every = args.progress_every;
        handles.push(thread::spawn(move || -> Result<()> {
            loop {
                let task = {
                    let guard = rx.lock().unwrap();
                    guard.recv()
                };
                let Ok(task) = task else { break };
                process_job(
                    &task.job,
                    &out_root,
                    &sidecar_root,
                    sessions.clone(),
                    allow.clone(),
                    start_day,
                    end_day,
                    max_source_rows,
                    progress_every,
                    &abort,
                    task.is_first,
                    task.is_last,
                )?;
            }
            Ok(())
        }));
    }
    let mut failed = false;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                eprintln!("worker failed: {err:?}");
                failed = true;
                abort.store(true, Ordering::Relaxed);
            }
            Err(_) => {
                eprintln!("worker panicked");
                failed = true;
                abort.store(true, Ordering::Relaxed);
            }
        }
    }
    if failed {
        bail!("one or more workers failed");
    }

    // Stitch split session-days from sidecars.
    let mut groups: BTreeMap<(String, String, String), Vec<PathBuf>> = BTreeMap::new();
    if args.sidecar_root.is_dir() {
        for entry in fs::read_dir(&args.sidecar_root)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !name.ends_with(".csv") {
                continue;
            }
            // {period}__partNNNNNN__{RIC}__{YYYYMMDD}__{head|tail}.csv
            let stem = name.trim_end_matches(".csv");
            let bits: Vec<&str> = stem.split("__").collect();
            if bits.len() < 5 {
                continue;
            }
            let period = bits[0].to_string();
            let ric = bits[2];
            let yyyymmdd = bits[3];
            groups
                .entry((period, ric.to_string(), yyyymmdd.to_string()))
                .or_default()
                .push(path);
        }
    }
    for ((period, ric, day), mut paths) in groups {
        paths.sort();
        log_line(&format!(
            "stitch period={period} ric={ric} day={day} files={}",
            paths.len()
        ));
        replay_sidecar(&paths, &ric, &sessions, &args.out_root)?;
    }

    log_line("all jobs finished");
    Ok(())
}
