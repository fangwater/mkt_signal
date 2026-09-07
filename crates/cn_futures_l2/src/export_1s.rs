//! Dense 1s backtest parquet from CN L2 RocksDB.
//!
//! Layout: `{out_root}/{exchange}/{product}/{TradDay:%Y%m%d}.parquet`
//! Columns match CME backtest_1s (11). Auction seconds are dropped. Continuous
//! segments come from L2 depth gaps (tea by clock, lunch/overnight by >=30min).
//! Fill copies prior bid/ask/close/midp; buy_high/sell_low stay null on fills.

use anyhow::{bail, Context, Result};
use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{Datelike, Duration, NaiveDate, TimeZone, Timelike, Utc, Weekday};
use chrono_tz::Asia::Shanghai;
use chrono_tz::Tz;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rocksdb::{Direction, IteratorMode};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::codec::{
    decode_depth, decode_key, decode_trade, encode_key, is_product_cf_name, KIND_DEPTH, KIND_TRADE,
};
use crate::db::{open_rocksdb_read_only, L2Db, FORBIDDEN_ROCKSDB_MARK};
use crate::universe::is_maintained_product;

pub const DEFAULT_OUT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/backtest_1s";
pub const SESSION_BREAK_SEC: i64 = 1_800;
const TEA_MIN_GAP_SEC: i64 = 480;

#[derive(Clone, Debug)]
pub struct ExportArgs {
    pub rocksdb_dir: PathBuf,
    pub out_root: PathBuf,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub workers: usize,
    pub products: Option<Vec<String>>,
    pub overwrite: bool,
}

#[derive(Clone, Debug, Default)]
pub struct ExportStats {
    pub files: u64,
    pub rows: u64,
    pub skipped_existing: u64,
}

#[derive(Clone, Debug)]
pub struct SparseDepth {
    pub ts_sec: i64,
    pub bid0p: f64,
    pub bid0v: f64,
    pub ask0p: f64,
    pub ask0v: f64,
}

#[derive(Clone, Debug)]
pub struct SparseTrade {
    pub ts_sec: i64,
    pub price: f64,
    pub aggressor: u8,
}

#[derive(Clone, Debug)]
pub struct OutRow {
    pub contract_id: String,
    pub ts: i64,
    pub bid0p: f64,
    pub bid0v: f64,
    pub ask0p: f64,
    pub ask0v: f64,
    pub buy_high: Option<f64>,
    pub sell_low: Option<f64>,
    pub close: f64,
    pub midp: f64,
}

pub fn shanghai(ts_sec: i64) -> chrono::DateTime<Tz> {
    Utc.timestamp_opt(ts_sec, 0)
        .single()
        .expect("unix second")
        .with_timezone(&Shanghai)
}

pub fn is_cffex_product(product: &str) -> bool {
    matches!(
        product,
        "IC" | "IF" | "IH" | "IM" | "T" | "TF" | "TL" | "TS"
    )
}

pub fn is_auction_hm(hour: u32, minute: u32, cffex: bool) -> bool {
    if cffex {
        hour == 9 && (25..30).contains(&minute)
    } else {
        (hour == 8 && (55..60).contains(&minute)) || (hour == 20 && (55..60).contains(&minute))
    }
}

pub fn is_auction_ts(ts_sec: i64, cffex: bool) -> bool {
    let local = shanghai(ts_sec);
    is_auction_hm(local.hour(), local.minute(), cffex)
}

fn is_tea_break(prev: i64, next: i64) -> bool {
    if next - prev < TEA_MIN_GAP_SEC {
        return false;
    }
    let a = shanghai(prev);
    let b = shanghai(next);
    a.hour() == 10
        && (10..20).contains(&a.minute())
        && b.hour() == 10
        && (25..36).contains(&b.minute())
}

pub fn is_session_break(prev: i64, next: i64) -> bool {
    let gap = next - prev;
    gap >= SESSION_BREAK_SEC || is_tea_break(prev, next)
}

fn on_or_next_weekday(day: NaiveDate) -> NaiveDate {
    match day.weekday() {
        Weekday::Sat => day + Duration::days(2),
        Weekday::Sun => day + Duration::days(1),
        _ => day,
    }
}

pub fn trad_day_from_ts(ts_sec: i64) -> NaiveDate {
    let local = shanghai(ts_sec);
    let day = local.date_naive();
    let hour = local.hour();
    if hour >= 20 {
        on_or_next_weekday(day + Duration::days(1))
    } else if hour < 8 {
        on_or_next_weekday(day)
    } else {
        day
    }
}

pub fn book_valid(bid0p: f64, bid0v: f64, ask0p: f64, ask0v: f64) -> bool {
    bid0p.is_finite()
        && ask0p.is_finite()
        && bid0v.is_finite()
        && ask0v.is_finite()
        && bid0p > 0.0
        && ask0p >= bid0p
        && bid0v >= 0.0
        && ask0v >= 0.0
}

pub fn split_depth_segments(secs: &[i64], cffex: bool) -> Vec<(i64, i64)> {
    let kept: Vec<i64> = secs
        .iter()
        .copied()
        .filter(|ts| !is_auction_ts(*ts, cffex))
        .collect();
    if kept.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut start = kept[0];
    let mut prev = kept[0];
    for &ts in &kept[1..] {
        if is_session_break(prev, ts) {
            out.push((start, prev));
            start = ts;
        }
        prev = ts;
    }
    out.push((start, prev));
    out
}

fn last_depth_before<'a>(depths: &'a [SparseDepth], ts_sec: i64) -> Option<&'a SparseDepth> {
    let mut lo = 0usize;
    let mut hi = depths.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if depths[mid].ts_sec < ts_sec {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo.checked_sub(1).and_then(|idx| depths.get(idx))
}

struct SecondAgg {
    buy_high: Option<f64>,
    sell_low: Option<f64>,
    close: Option<f64>,
}

fn trade_buckets(trades: &[SparseTrade]) -> HashMap<i64, SecondAgg> {
    let mut out: HashMap<i64, SecondAgg> = HashMap::new();
    for trade in trades {
        let slot = out.entry(trade.ts_sec).or_insert(SecondAgg {
            buy_high: None,
            sell_low: None,
            close: None,
        });
        slot.close = Some(trade.price);
        match trade.aggressor {
            1 => {
                slot.buy_high = Some(match slot.buy_high {
                    Some(prev) => prev.max(trade.price),
                    None => trade.price,
                });
            }
            2 => {
                slot.sell_low = Some(match slot.sell_low {
                    Some(prev) => prev.min(trade.price),
                    None => trade.price,
                });
            }
            _ => {}
        }
    }
    out
}

pub fn densify_instrument(
    contract_id: &str,
    depths: &[SparseDepth],
    trades: &[SparseTrade],
    cffex: bool,
) -> Vec<OutRow> {
    if depths.is_empty() {
        return Vec::new();
    }
    let secs: Vec<i64> = depths.iter().map(|d| d.ts_sec).collect();
    let segments = split_depth_segments(&secs, cffex);
    let buckets = trade_buckets(trades);
    let mut rows = Vec::new();
    for (first_depth, last_depth) in segments {
        let lo = first_depth + 1;
        let hi = last_depth;
        if lo > hi {
            continue;
        }
        let mut carry: Option<(f64, f64, f64, f64, f64, f64)> = None;
        for ts in lo..=hi {
            let book = last_depth_before(depths, ts).filter(|d| {
                d.ts_sec >= first_depth && book_valid(d.bid0p, d.bid0v, d.ask0p, d.ask0v)
            });
            let Some(book) = book else {
                carry = None;
                continue;
            };
            if carry.is_none() && book.ts_sec >= ts {
                continue;
            }
            let bid0p = book.bid0p;
            let bid0v = book.bid0v;
            let ask0p = book.ask0p;
            let ask0v = book.ask0v;
            let midp = (bid0p + ask0p) / 2.0;
            let agg = buckets.get(&ts);
            let has_depth = depths.binary_search_by_key(&ts, |d| d.ts_sec).is_ok();
            let is_fill = agg.is_none() && !has_depth;
            let close = match agg.and_then(|a| a.close) {
                Some(price) => price,
                None if is_fill => match carry {
                    Some((_, _, _, _, prev_close, _)) => prev_close,
                    None => midp,
                },
                None => midp,
            };
            let (buy_high, sell_low) = if let Some(agg) = agg {
                (agg.buy_high, agg.sell_low)
            } else {
                (None, None)
            };
            rows.push(OutRow {
                contract_id: contract_id.to_string(),
                ts,
                bid0p,
                bid0v,
                ask0p,
                ask0v,
                buy_high,
                sell_low,
                close,
                midp,
            });
            carry = Some((bid0p, bid0v, ask0p, ask0v, close, midp));
        }
    }
    rows
}

pub fn exchange_of(product: &str) -> Option<&'static str> {
    Some(match product {
        "IC" | "IF" | "IH" | "IM" | "T" | "TF" | "TL" | "TS" => "ccfx",
        "A" | "B" | "BB" | "BZ" | "C" | "CS" | "EB" | "EG" | "FB" | "I" | "J" | "JD" | "JM"
        | "L" | "LG" | "LH" | "M" | "P" | "PG" | "PP" | "RR" | "V" | "Y" => "xdce",
        "LC" | "PD" | "PS" | "PT" | "SI" => "xgfe",
        "AD" | "AG" | "AL" | "AO" | "AU" | "BR" | "BU" | "CU" | "FU" | "HC" | "NI" | "OP"
        | "PB" | "RB" | "RU" | "SN" | "SP" | "SS" | "WR" | "ZN" => "xsge",
        "BC" | "EC" | "LU" | "NR" | "SC" => "xsie",
        "AP" | "CF" | "CJ" | "CY" | "FG" | "JR" | "LR" | "MA" | "OI" | "PF" | "PK" | "PL"
        | "PM" | "PR" | "PX" | "RI" | "RM" | "RS" | "SA" | "SF" | "SH" | "SM" | "SR" | "TA"
        | "UR" | "WH" | "ZC" => "xzce",
        _ => return None,
    })
}

pub fn parse_product_cf(name: &str) -> Option<(i32, String)> {
    if !is_product_cf_name(name) {
        return None;
    }
    let rest = name.strip_prefix("p:")?;
    let (year, product) = rest.split_once(':')?;
    let year: i32 = year.parse().ok()?;
    Some((year, product.to_string()))
}

fn schema() -> Schema {
    Schema::new(vec![
        Field::new("contract_id", DataType::Utf8, false),
        Field::new("ric", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("bid0p", DataType::Float64, true),
        Field::new("bid0v", DataType::Float64, true),
        Field::new("ask0p", DataType::Float64, true),
        Field::new("ask0v", DataType::Float64, true),
        Field::new("buy_high", DataType::Float64, true),
        Field::new("sell_low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("midp", DataType::Float64, true),
    ])
}

fn f64_col(batch: &RecordBatch, idx: usize) -> Result<Vec<Option<f64>>> {
    let arr = batch
        .column(idx)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| anyhow::anyhow!("column {idx} is not float64"))?;
    Ok((0..arr.len())
        .map(|i| {
            if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            }
        })
        .collect())
}

fn read_day_parquet(path: &Path) -> Result<Vec<OutRow>> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("parquet {}", path.display()))?
        .build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.with_context(|| format!("read batch {}", path.display()))?;
        let contract = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("contract_id is not utf8"))?;
        let ts = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("ts is not int64"))?;
        let bid0p = f64_col(&batch, 3)?;
        let bid0v = f64_col(&batch, 4)?;
        let ask0p = f64_col(&batch, 5)?;
        let ask0v = f64_col(&batch, 6)?;
        let buy_high = f64_col(&batch, 7)?;
        let sell_low = f64_col(&batch, 8)?;
        let close = f64_col(&batch, 9)?;
        let midp = f64_col(&batch, 10)?;
        for i in 0..batch.num_rows() {
            out.push(OutRow {
                contract_id: contract.value(i).to_string(),
                ts: ts.value(i),
                bid0p: bid0p[i].unwrap_or(f64::NAN),
                bid0v: bid0v[i].unwrap_or(f64::NAN),
                ask0p: ask0p[i].unwrap_or(f64::NAN),
                ask0v: ask0v[i].unwrap_or(f64::NAN),
                buy_high: buy_high[i],
                sell_low: sell_low[i],
                close: close[i].unwrap_or(f64::NAN),
                midp: midp[i].unwrap_or(f64::NAN),
            });
        }
    }
    Ok(out)
}

fn write_day_parquet(path: &Path, rows: &[OutRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let tmp = path.with_extension("parquet.tmp");
    let file = fs::File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let batch_schema = Arc::new(schema());
    let contract: Vec<&str> = rows.iter().map(|r| r.contract_id.as_str()).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&batch_schema),
        vec![
            Arc::new(StringArray::from(contract.clone())),
            Arc::new(StringArray::from(contract)),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| Some(r.bid0p)).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| Some(r.bid0v)).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| Some(r.ask0p)).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| Some(r.ask0v)).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.buy_high).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.sell_low).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| Some(r.close)).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| Some(r.midp)).collect::<Vec<_>>(),
            )),
        ],
    )?;
    let mut writer = ArrowWriter::try_new(file, batch_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

pub fn scan_kind(
    db: &L2Db,
    cf_name: &str,
    kind: u8,
    instrument: &str,
) -> Result<(Vec<SparseDepth>, Vec<SparseTrade>)> {
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("missing cf {cf_name}"))?;
    let start = encode_key(kind, instrument, 0, 0)?;
    let mut depths = Vec::new();
    let mut trades = Vec::new();
    for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
        let (key, value) = item.context("iterate l2")?;
        let (got_kind, id, ts_ns, _) = decode_key(&key)?;
        if got_kind != kind || id != instrument {
            break;
        }
        let ts_sec = (ts_ns / 1_000_000_000) as i64;
        if kind == KIND_DEPTH {
            let rec = decode_depth(&value)?;
            let Some(bid0p) = rec.bid_prices[0] else {
                continue;
            };
            let Some(ask0p) = rec.ask_prices[0] else {
                continue;
            };
            let bid0v = rec.bid_sizes[0].unwrap_or(f64::NAN);
            let ask0v = rec.ask_sizes[0].unwrap_or(f64::NAN);
            if !book_valid(bid0p, bid0v, ask0p, ask0v) {
                continue;
            }
            depths.push(SparseDepth {
                ts_sec,
                bid0p,
                bid0v,
                ask0p,
                ask0v,
            });
        } else if kind == KIND_TRADE {
            let rec = decode_trade(&value)?;
            trades.push(SparseTrade {
                ts_sec,
                price: rec.price,
                aggressor: rec.aggressor,
            });
        }
    }
    Ok((depths, trades))
}

pub fn list_instruments(db: &L2Db, cf_name: &str) -> Result<Vec<String>> {
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("missing cf {cf_name}"))?;
    let start = [KIND_DEPTH];
    let mut out = Vec::new();
    let mut last = String::new();
    for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
        let (key, _) = item.context("iterate instruments")?;
        let (kind, instrument, _, _) = match decode_key(&key) {
            Ok(parsed) => parsed,
            Err(_) => break,
        };
        if kind != KIND_DEPTH {
            break;
        }
        if instrument != last {
            out.push(instrument.clone());
            last = instrument;
        }
    }
    Ok(out)
}

fn export_product(
    db: &L2Db,
    args: &ExportArgs,
    product: &str,
    cf_names: &[String],
) -> Result<ExportStats> {
    let Some(exchange) = exchange_of(product) else {
        eprintln!("skip unknown product {product}");
        return Ok(ExportStats::default());
    };
    let cffex = is_cffex_product(product);
    let tmp_root = args.out_root.join("_tmp").join(product);
    if tmp_root.exists() {
        fs::remove_dir_all(&tmp_root).with_context(|| format!("remove {}", tmp_root.display()))?;
    }
    let mut days: Vec<NaiveDate> = Vec::new();
    for cf_name in cf_names {
        let instruments = list_instruments(db, cf_name)?;
        for instrument in instruments {
            let (depths, _) = scan_kind(db, cf_name, KIND_DEPTH, &instrument)?;
            if depths.is_empty() {
                continue;
            }
            let (_, trades) = scan_kind(db, cf_name, KIND_TRADE, &instrument)?;
            let rows = densify_instrument(&instrument, &depths, &trades, cffex);
            let mut by_day: HashMap<NaiveDate, Vec<OutRow>> = HashMap::new();
            for row in rows {
                let day = trad_day_from_ts(row.ts);
                if day < args.start || day > args.end {
                    continue;
                }
                by_day.entry(day).or_default().push(row);
            }
            for (day, day_rows) in by_day {
                if !days.contains(&day) {
                    days.push(day);
                }
                let shard = tmp_root
                    .join(day.format("%Y%m%d").to_string())
                    .join(format!(
                        "{}_{instrument}.parquet",
                        cf_name.replace(':', "_")
                    ));
                write_day_parquet(&shard, &day_rows)?;
            }
        }
    }
    days.sort();
    let mut stats = ExportStats::default();
    for day in days {
        let dest = args
            .out_root
            .join(exchange)
            .join(product)
            .join(format!("{}.parquet", day.format("%Y%m%d")));
        if dest.exists() && !args.overwrite {
            stats.skipped_existing += 1;
            continue;
        }
        let shard_dir = tmp_root.join(day.format("%Y%m%d").to_string());
        let mut rows = Vec::new();
        if shard_dir.is_dir() {
            for entry in
                fs::read_dir(&shard_dir).with_context(|| format!("read {}", shard_dir.display()))?
            {
                let path = entry?.path();
                if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
                    continue;
                }
                rows.extend(read_day_parquet(&path)?);
            }
        }
        if rows.is_empty() {
            continue;
        }
        rows.sort_by(|a, b| a.contract_id.cmp(&b.contract_id).then(a.ts.cmp(&b.ts)));
        let n = rows.len() as u64;
        write_day_parquet(&dest, &rows)?;
        stats.files += 1;
        stats.rows += n;
        eprintln!(
            "{exchange} {product} {} rows={n} dest={}",
            day.format("%Y%m%d"),
            dest.display()
        );
    }
    if tmp_root.exists() {
        let _ = fs::remove_dir_all(&tmp_root);
    }
    Ok(stats)
}

pub fn run_export(args: ExportArgs) -> Result<ExportStats> {
    if args
        .out_root
        .to_string_lossy()
        .contains(FORBIDDEN_ROCKSDB_MARK)
    {
        bail!("refusing to write into a CME RocksDB path");
    }
    if args.end < args.start {
        bail!("end precedes start");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    fs::create_dir_all(&args.out_root)
        .with_context(|| format!("create {}", args.out_root.display()))?;
    let db = open_rocksdb_read_only(&args.rocksdb_dir)?;
    let names = L2Db::list_cf(&rocksdb::Options::default(), &args.rocksdb_dir)?;
    let years: Vec<i32> = {
        let mut ys = Vec::new();
        let mut y = args.start.year() - 1;
        while y <= args.end.year() + 1 {
            ys.push(y);
            y += 1;
        }
        ys
    };
    let mut by_product: HashMap<String, Vec<String>> = HashMap::new();
    for name in names {
        let Some((year, product)) = parse_product_cf(&name) else {
            continue;
        };
        if !years.contains(&year) {
            continue;
        }
        if !is_maintained_product(&product) {
            continue;
        }
        if let Some(filter) = &args.products {
            if !filter.iter().any(|item| item == &product) {
                continue;
            }
        }
        by_product.entry(product).or_default().push(name);
    }
    for cfs in by_product.values_mut() {
        cfs.sort();
    }
    let mut jobs: Vec<(String, Vec<String>)> = by_product.into_iter().collect();
    jobs.sort_by(|a, b| a.0.cmp(&b.0));
    eprintln!(
        "cn_l2 export_1s products={} start={} end={} out={}",
        jobs.len(),
        args.start,
        args.end,
        args.out_root.display()
    );
    let db = Arc::new(db);
    let args = Arc::new(args);
    let (tx, rx) = crossbeam_channel::unbounded::<(String, Vec<String>)>();
    for job in jobs {
        tx.send(job).expect("enqueue product");
    }
    drop(tx);
    let files = Arc::new(AtomicU64::new(0));
    let rows = Arc::new(AtomicU64::new(0));
    let skipped = Arc::new(AtomicU64::new(0));
    let workers = args.workers;
    let mut handles = Vec::new();
    for _ in 0..workers {
        let rx = rx.clone();
        let db = Arc::clone(&db);
        let args = Arc::clone(&args);
        let files = Arc::clone(&files);
        let rows = Arc::clone(&rows);
        let skipped = Arc::clone(&skipped);
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok((product, cf_names)) = rx.recv() {
                let stats = export_product(&db, &args, &product, &cf_names)?;
                files.fetch_add(stats.files, Ordering::Relaxed);
                rows.fetch_add(stats.rows, Ordering::Relaxed);
                skipped.fetch_add(stats.skipped_existing, Ordering::Relaxed);
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.join().expect("export worker")?;
    }
    Ok(ExportStats {
        files: files.load(Ordering::Relaxed),
        rows: rows.load(Ordering::Relaxed),
        skipped_existing: skipped.load(Ordering::Relaxed),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn sec(hour: u32, minute: u32, second: u32) -> i64 {
        let date = NaiveDate::from_ymd_opt(2024, 1, 16).unwrap();
        let time = chrono::NaiveTime::from_hms_opt(hour, minute, second).unwrap();
        Shanghai
            .from_local_datetime(&date.and_time(time))
            .single()
            .unwrap()
            .timestamp()
    }

    fn depth(hour: u32, minute: u32, second: u32, bid: f64, ask: f64) -> SparseDepth {
        SparseDepth {
            ts_sec: sec(hour, minute, second),
            bid0p: bid,
            bid0v: 1.0,
            ask0p: ask,
            ask0v: 1.0,
        }
    }

    #[test]
    fn auction_windows() {
        assert!(is_auction_hm(8, 59, false));
        assert!(!is_auction_hm(9, 0, false));
        assert!(is_auction_hm(20, 59, false));
        assert!(!is_auction_hm(21, 0, false));
        assert!(is_auction_hm(9, 29, true));
        assert!(!is_auction_hm(9, 30, true));
    }

    #[test]
    fn tea_breaks_segment_short_holes_do_not() {
        let secs = vec![
            sec(9, 0, 0),
            sec(9, 0, 1),
            sec(9, 0, 5),
            sec(9, 20, 0),
            sec(9, 40, 0),
            sec(10, 0, 0),
            sec(10, 14, 59),
            sec(10, 30, 0),
            sec(10, 30, 1),
            sec(10, 50, 0),
            sec(11, 10, 0),
            sec(11, 30, 0),
            sec(13, 30, 0),
        ];
        let segs = split_depth_segments(&secs, false);
        assert_eq!(segs.len(), 3);
        assert_eq!(segs[0], (sec(9, 0, 0), sec(10, 14, 59)));
        assert_eq!(segs[1], (sec(10, 30, 0), sec(11, 30, 0)));
        assert_eq!(segs[2], (sec(13, 30, 0), sec(13, 30, 0)));
    }

    #[test]
    fn friday_night_belongs_to_monday() {
        let friday_night = Shanghai
            .from_local_datetime(
                &NaiveDate::from_ymd_opt(2024, 1, 5)
                    .unwrap()
                    .and_hms_opt(21, 0, 0)
                    .unwrap(),
            )
            .single()
            .unwrap()
            .timestamp();
        assert_eq!(
            trad_day_from_ts(friday_night),
            NaiveDate::from_ymd_opt(2024, 1, 8).unwrap()
        );
    }

    #[test]
    fn densify_fills_inside_segment_not_across_tea() {
        let depths = vec![
            depth(9, 0, 0, 100.0, 101.0),
            depth(9, 0, 1, 100.5, 101.5),
            depth(9, 20, 0, 101.0, 102.0),
            depth(10, 14, 58, 101.5, 102.5),
            depth(10, 14, 59, 102.0, 103.0),
            depth(10, 30, 0, 104.0, 105.0),
            depth(10, 30, 1, 104.5, 105.5),
        ];
        let trades = vec![SparseTrade {
            ts_sec: sec(9, 0, 1),
            price: 101.0,
            aggressor: 1,
        }];
        let rows = densify_instrument("rb2405", &depths, &trades, false);
        let first = rows.iter().find(|r| r.ts == sec(9, 0, 1)).unwrap();
        assert_eq!(first.bid0p, 100.0);
        assert_eq!(first.buy_high, Some(101.0));
        assert_eq!(first.close, 101.0);
        let filled = rows.iter().find(|r| r.ts == sec(9, 0, 2)).unwrap();
        assert_eq!(filled.bid0p, 100.5);
        assert!(filled.buy_high.is_none());
        assert_eq!(filled.close, 101.0);
        assert!(rows.iter().any(|r| r.ts == sec(10, 14, 59)));
        assert!(rows.iter().all(|r| r.ts != sec(10, 15, 0)));
        assert!(rows.iter().any(|r| r.ts == sec(10, 30, 1)));
        assert!(rows.iter().all(|r| r.ts != sec(10, 29, 0)));
    }

    #[test]
    fn unknown_aggressor_goes_to_close_not_extremes() {
        let depths = vec![depth(9, 0, 0, 100.0, 101.0), depth(9, 0, 1, 100.0, 101.0)];
        let trades = vec![SparseTrade {
            ts_sec: sec(9, 0, 1),
            price: 100.5,
            aggressor: 0,
        }];
        let rows = densify_instrument("rb2405", &depths, &trades, false);
        let row = rows.iter().find(|r| r.ts == sec(9, 0, 1)).unwrap();
        assert_eq!(row.close, 100.5);
        assert!(row.buy_high.is_none());
        assert!(row.sell_low.is_none());
    }

    #[test]
    fn auction_depth_is_not_a_segment() {
        let secs = vec![sec(8, 59, 0), sec(9, 0, 0), sec(9, 0, 1)];
        let segs = split_depth_segments(&secs, false);
        assert_eq!(segs, vec![(sec(9, 0, 0), sec(9, 0, 1))]);
    }
}
