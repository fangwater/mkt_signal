//! Dense 1-minute baseline parquet aligned to backtest_1s segments.
//!
//! Layout: `{out_root}/{exchange}/{product}/{TradDay:%Y%m%d}.parquet`
//! Empty minutes inside a segment carry OHLC/vwap and the last two-sided book.
//! Equity-index minutes after 15:00 Shanghai are dropped. Ylabel still skips
//! carried VWAP as P[t]; a carried book may supply mid.

use anyhow::{bail, Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::Datelike;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use rocksdb::{Direction, IteratorMode};
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::baseline_1min::{
    fill_session_minutes_with_twap, minute_segments_from_seconds, synthesize_minutes,
    BaselineMinute, Book5, PrintTrade,
};
use crate::codec::{decode_depth, decode_key, decode_trade, encode_key, KIND_DEPTH, KIND_TRADE};
use crate::db::{open_rocksdb_read_only, L2Db, FORBIDDEN_ROCKSDB_MARK};
use crate::export_1s::{
    book_valid, exchange_of, is_cffex_product, list_instruments, parse_product_cf,
    split_depth_segments, trad_day_from_ts, ExportArgs, ExportStats,
};
use crate::multipliers::{load_multiplier_catalog, require_multiplier};
use crate::universe::is_maintained_product;

pub const DEFAULT_OUT_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";

fn schema() -> Schema {
    let mut fields = vec![
        Field::new("contract_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
        Field::new("open", DataType::Float64, true),
        Field::new("high", DataType::Float64, true),
        Field::new("low", DataType::Float64, true),
        Field::new("close", DataType::Float64, true),
        Field::new("volume", DataType::Float64, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("avg_amount", DataType::Float64, false),
        Field::new("count", DataType::Float64, false),
        Field::new("buy_count", DataType::Float64, false),
        Field::new("sell_count", DataType::Float64, false),
        Field::new("buy_amount", DataType::Float64, false),
        Field::new("sell_amount", DataType::Float64, false),
        Field::new("buy_volume", DataType::Float64, false),
        Field::new("sell_volume", DataType::Float64, false),
        Field::new("vwap", DataType::Float64, true),
        Field::new("buy_vwap", DataType::Float64, true),
        Field::new("sell_vwap", DataType::Float64, true),
        Field::new("twap", DataType::Float64, true),
        Field::new("mid_price", DataType::Float64, true),
        Field::new("net_buy_amount", DataType::Float64, false),
        Field::new("net_buy_volume", DataType::Float64, false),
        Field::new("net_buy_pct", DataType::Float64, true),
        Field::new("large_order", DataType::Float64, false),
        Field::new("medium_order", DataType::Float64, false),
        Field::new("small_order", DataType::Float64, false),
        Field::new("large_buy", DataType::Float64, false),
        Field::new("large_sell", DataType::Float64, false),
        Field::new("medium_buy", DataType::Float64, false),
        Field::new("medium_sell", DataType::Float64, false),
        Field::new("small_buy", DataType::Float64, false),
        Field::new("small_sell", DataType::Float64, false),
        Field::new("net_buy_large", DataType::Float64, false),
        Field::new("net_buy_medium", DataType::Float64, false),
        Field::new("net_buy_small", DataType::Float64, false),
    ];
    for i in 0..5 {
        fields.push(Field::new(format!("bid{i}p"), DataType::Float64, true));
        fields.push(Field::new(format!("bid{i}v"), DataType::Float64, true));
    }
    for i in 0..5 {
        fields.push(Field::new(format!("ask{i}p"), DataType::Float64, true));
        fields.push(Field::new(format!("ask{i}v"), DataType::Float64, true));
    }
    Schema::new(fields)
}

fn opt_f64(values: Vec<Option<f64>>) -> Arc<Float64Array> {
    Arc::new(Float64Array::from(values))
}

fn book_level(book: Option<&Book5>, side: &str, i: usize) -> (Option<f64>, Option<f64>) {
    let Some(book) = book else {
        return (None, None);
    };
    match side {
        "bid" => (book.bid_prices[i], book.bid_sizes[i]),
        _ => (book.ask_prices[i], book.ask_sizes[i]),
    }
}

pub fn write_day_parquet(path: &Path, rows: &[BaselineMinute]) -> Result<()> {
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
    let mut cols: Vec<Arc<dyn arrow::array::Array>> = vec![
        Arc::new(StringArray::from(
            rows.iter()
                .map(|r| r.contract_id.as_str())
                .collect::<Vec<_>>(),
        )),
        Arc::new(Int64Array::from(
            rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
        )),
        opt_f64(rows.iter().map(|r| r.open).collect()),
        opt_f64(rows.iter().map(|r| r.high).collect()),
        opt_f64(rows.iter().map(|r| r.low).collect()),
        opt_f64(rows.iter().map(|r| r.close).collect()),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.volume).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.amount).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.avg_amount).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.count).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.buy_count).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.sell_count).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.buy_amount).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.sell_amount).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.buy_volume).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.sell_volume).collect::<Vec<_>>(),
        )),
        opt_f64(rows.iter().map(|r| r.vwap).collect()),
        opt_f64(rows.iter().map(|r| r.buy_vwap).collect()),
        opt_f64(rows.iter().map(|r| r.sell_vwap).collect()),
        opt_f64(rows.iter().map(|r| r.twap).collect()),
        opt_f64(rows.iter().map(|r| r.mid_price).collect()),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.net_buy_amount).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.net_buy_volume).collect::<Vec<_>>(),
        )),
        opt_f64(rows.iter().map(|r| r.net_buy_pct).collect()),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.large_order).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.medium_order).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.small_order).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.large_buy).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.large_sell).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.medium_buy).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.medium_sell).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.small_buy).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.small_sell).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.net_buy_large).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.net_buy_medium).collect::<Vec<_>>(),
        )),
        Arc::new(Float64Array::from(
            rows.iter().map(|r| r.net_buy_small).collect::<Vec<_>>(),
        )),
    ];
    for i in 0..5 {
        cols.push(opt_f64(
            rows.iter()
                .map(|r| book_level(r.book.as_ref(), "bid", i).0)
                .collect(),
        ));
        cols.push(opt_f64(
            rows.iter()
                .map(|r| book_level(r.book.as_ref(), "bid", i).1)
                .collect(),
        ));
    }
    for i in 0..5 {
        cols.push(opt_f64(
            rows.iter()
                .map(|r| book_level(r.book.as_ref(), "ask", i).0)
                .collect(),
        ));
        cols.push(opt_f64(
            rows.iter()
                .map(|r| book_level(r.book.as_ref(), "ask", i).1)
                .collect(),
        ));
    }
    let batch = RecordBatch::try_new(Arc::clone(&batch_schema), cols)?;
    let mut writer = ArrowWriter::try_new(file, batch_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn scan_instrument(
    db: &L2Db,
    cf_name: &str,
    instrument: &str,
) -> Result<(Vec<crate::codec::DepthRecord>, Vec<PrintTrade>, Vec<i64>)> {
    let cf = db
        .cf_handle(cf_name)
        .ok_or_else(|| anyhow::anyhow!("missing cf {cf_name}"))?;
    let mut depths = Vec::new();
    let mut depth_secs = Vec::new();
    let start = encode_key(KIND_DEPTH, instrument, 0, 0)?;
    for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
        let (key, value) = item.context("iterate depth")?;
        let (kind, id, ts_ns, _) = decode_key(&key)?;
        if kind != KIND_DEPTH || id != instrument {
            break;
        }
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
        depth_secs.push((ts_ns / 1_000_000_000) as i64);
        depths.push(rec);
    }
    let mut trades = Vec::new();
    let start = encode_key(KIND_TRADE, instrument, 0, 0)?;
    for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
        let (key, value) = item.context("iterate trade")?;
        let (kind, id, _, _) = decode_key(&key)?;
        if kind != KIND_TRADE || id != instrument {
            break;
        }
        let rec = decode_trade(&value)?;
        trades.push(PrintTrade::from(&rec));
    }
    Ok((depths, trades, depth_secs))
}

fn export_product(
    db: &L2Db,
    args: &ExportArgs,
    product: &str,
    cf_names: &[String],
    volume_multiple: f64,
) -> Result<ExportStats> {
    let Some(exchange) = exchange_of(product) else {
        eprintln!("skip unknown product {product}");
        return Ok(ExportStats::default());
    };
    let cffex = is_cffex_product(product);
    let tmp_root = args
        .out_root
        .join("_tmp")
        .join(args.start.format("%Y").to_string())
        .join(product);
    if tmp_root.exists() {
        fs::remove_dir_all(&tmp_root).with_context(|| format!("remove {}", tmp_root.display()))?;
    }
    let mut days: Vec<chrono::NaiveDate> = Vec::new();
    for cf_name in cf_names {
        let instruments = list_instruments(db, cf_name)?;
        for instrument in instruments {
            let (depths, trades, depth_secs) = scan_instrument(db, cf_name, &instrument)?;
            if depth_secs.is_empty() && trades.is_empty() {
                continue;
            }
            let sec_segs = split_depth_segments(&depth_secs, cffex);
            let min_segs = minute_segments_from_seconds(&sec_segs);
            let sparse = synthesize_minutes(&instrument, &trades, &depths, volume_multiple)?;
            let filled = fill_session_minutes_with_twap(&instrument, sparse, &min_segs, &trades)?;
            let mut by_day: HashMap<chrono::NaiveDate, Vec<BaselineMinute>> = HashMap::new();
            for row in filled {
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

fn f64_col(batch: &arrow::record_batch::RecordBatch, idx: usize) -> Result<Vec<Option<f64>>> {
    use arrow::array::Array;
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

fn f64_named(batch: &arrow::record_batch::RecordBatch, name: &str) -> Result<Vec<Option<f64>>> {
    match batch.schema().index_of(name) {
        Ok(idx) => f64_col(batch, idx),
        Err(_) => Ok(vec![None; batch.num_rows()]),
    }
}

pub fn read_day_parquet(path: &Path) -> Result<Vec<BaselineMinute>> {
    use arrow::array::Array;
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let contract = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow::anyhow!("contract_id"))?;
        let ts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow::anyhow!("ts"))?;
        let open = f64_col(&batch, 2)?;
        let high = f64_col(&batch, 3)?;
        let low = f64_col(&batch, 4)?;
        let close = f64_col(&batch, 5)?;
        let volume = f64_col(&batch, 6)?;
        let amount = f64_col(&batch, 7)?;
        let avg_amount = f64_col(&batch, 8)?;
        let count = f64_col(&batch, 9)?;
        let buy_count = f64_col(&batch, 10)?;
        let sell_count = f64_col(&batch, 11)?;
        let buy_amount = f64_col(&batch, 12)?;
        let sell_amount = f64_col(&batch, 13)?;
        let buy_volume = f64_col(&batch, 14)?;
        let sell_volume = f64_col(&batch, 15)?;
        let vwap = f64_col(&batch, 16)?;
        let buy_vwap = f64_col(&batch, 17)?;
        let sell_vwap = f64_col(&batch, 18)?;
        let twap = f64_col(&batch, 19)?;
        let mid_price = f64_col(&batch, 20)?;
        let net_buy_amount = f64_col(&batch, 21)?;
        let net_buy_volume = f64_col(&batch, 22)?;
        let net_buy_pct = f64_col(&batch, 23)?;
        let large_order = f64_named(&batch, "large_order")?;
        let medium_order = f64_named(&batch, "medium_order")?;
        let small_order = f64_named(&batch, "small_order")?;
        let large_buy = f64_named(&batch, "large_buy")?;
        let large_sell = f64_named(&batch, "large_sell")?;
        let medium_buy = f64_named(&batch, "medium_buy")?;
        let medium_sell = f64_named(&batch, "medium_sell")?;
        let small_buy = f64_named(&batch, "small_buy")?;
        let small_sell = f64_named(&batch, "small_sell")?;
        let net_buy_large = f64_named(&batch, "net_buy_large")?;
        let net_buy_medium = f64_named(&batch, "net_buy_medium")?;
        let net_buy_small = f64_named(&batch, "net_buy_small")?;
        let mut bid_p = Vec::new();
        let mut bid_v = Vec::new();
        let mut ask_p = Vec::new();
        let mut ask_v = Vec::new();
        for i in 0..5 {
            bid_p.push(f64_named(&batch, &format!("bid{i}p"))?);
            bid_v.push(f64_named(&batch, &format!("bid{i}v"))?);
        }
        for i in 0..5 {
            ask_p.push(f64_named(&batch, &format!("ask{i}p"))?);
            ask_v.push(f64_named(&batch, &format!("ask{i}v"))?);
        }
        for i in 0..batch.num_rows() {
            let mut book = Book5::default();
            let mut any = false;
            for lvl in 0..5 {
                book.bid_prices[lvl] = bid_p[lvl][i];
                book.bid_sizes[lvl] = bid_v[lvl][i];
                book.ask_prices[lvl] = ask_p[lvl][i];
                book.ask_sizes[lvl] = ask_v[lvl][i];
                if bid_p[lvl][i].is_some() || ask_p[lvl][i].is_some() {
                    any = true;
                }
            }
            out.push(BaselineMinute {
                contract_id: contract.value(i).to_string(),
                ts: ts.value(i),
                open: open[i],
                high: high[i],
                low: low[i],
                close: close[i],
                volume: volume[i].unwrap_or(0.0),
                amount: amount[i].unwrap_or(0.0),
                avg_amount: avg_amount[i].unwrap_or(0.0),
                count: count[i].unwrap_or(0.0),
                buy_count: buy_count[i].unwrap_or(0.0),
                sell_count: sell_count[i].unwrap_or(0.0),
                buy_amount: buy_amount[i].unwrap_or(0.0),
                sell_amount: sell_amount[i].unwrap_or(0.0),
                buy_volume: buy_volume[i].unwrap_or(0.0),
                sell_volume: sell_volume[i].unwrap_or(0.0),
                vwap: vwap[i],
                buy_vwap: buy_vwap[i],
                sell_vwap: sell_vwap[i],
                twap: twap[i],
                mid_price: mid_price[i],
                net_buy_amount: net_buy_amount[i].unwrap_or(0.0),
                net_buy_volume: net_buy_volume[i].unwrap_or(0.0),
                net_buy_pct: net_buy_pct[i],
                large_order: large_order[i].unwrap_or(0.0),
                medium_order: medium_order[i].unwrap_or(0.0),
                small_order: small_order[i].unwrap_or(0.0),
                large_buy: large_buy[i].unwrap_or(0.0),
                large_sell: large_sell[i].unwrap_or(0.0),
                medium_buy: medium_buy[i].unwrap_or(0.0),
                medium_sell: medium_sell[i].unwrap_or(0.0),
                small_buy: small_buy[i].unwrap_or(0.0),
                small_sell: small_sell[i].unwrap_or(0.0),
                net_buy_large: net_buy_large[i].unwrap_or(0.0),
                net_buy_medium: net_buy_medium[i].unwrap_or(0.0),
                net_buy_small: net_buy_small[i].unwrap_or(0.0),
                book: any.then_some(book),
            });
        }
    }
    Ok(out)
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
        "cn_l2 export_1min products={} start={} end={} out={}",
        jobs.len(),
        args.start,
        args.end,
        args.out_root.display()
    );
    let catalog = load_multiplier_catalog()?;
    let mut multiples = HashMap::new();
    for (product, _) in &jobs {
        multiples.insert(product.clone(), require_multiplier(&catalog, product)?);
    }
    let db = Arc::new(db);
    let args = Arc::new(args);
    let multiples = Arc::new(multiples);
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
        let multiples = Arc::clone(&multiples);
        let files = Arc::clone(&files);
        let rows = Arc::clone(&rows);
        let skipped = Arc::clone(&skipped);
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok((product, cf_names)) = rx.recv() {
                let volume_multiple = *multiples.get(&product).expect("cataloged product");
                let stats = export_product(&db, &args, &product, &cf_names, volume_multiple)?;
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
