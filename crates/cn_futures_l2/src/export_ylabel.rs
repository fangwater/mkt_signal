//! Ylabel_1min from already-written baseline_data_1min.
//!
//! Carried empty minutes (volume=0, forwarded vwap) are not P[t].
//! Layout: `{out_root}/{exchange}/{product}/{TradDay:%Y%m%d}.parquet`

use anyhow::{bail, Context, Result};
use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::export_1min::read_day_parquet;
use crate::export_1s::{trad_day_from_ts, ExportStats};
use crate::ylabel_1m::{
    build_ylabel_rows, causal_prices_from_minutes, ylabel_columns, YlabelRow, LABEL_COUNT,
};

pub const DEFAULT_IN_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";
pub const DEFAULT_OUT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/ylabel_1min";

#[derive(Clone, Debug)]
pub struct YlabelArgs {
    pub in_root: PathBuf,
    pub out_root: PathBuf,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub workers: usize,
    pub products: Option<Vec<String>>,
    pub overwrite: bool,
}

fn schema() -> Schema {
    let mut fields = vec![
        Field::new("contract_id", DataType::Utf8, false),
        Field::new("ts", DataType::Int64, false),
    ];
    for name in ylabel_columns() {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    Schema::new(fields)
}

pub fn write_ylabel_day(path: &Path, rows: &[YlabelRow]) -> Result<()> {
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
    ];
    for i in 0..LABEL_COUNT {
        cols.push(Arc::new(Float64Array::from(
            rows.iter().map(|r| r.labels[i]).collect::<Vec<_>>(),
        )));
    }
    let batch = RecordBatch::try_new(Arc::clone(&batch_schema), cols)?;
    let mut writer = ArrowWriter::try_new(file, batch_schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    fs::rename(&tmp, path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(())
}

fn list_product_days(
    root: &Path,
    exchange: &str,
    product: &str,
) -> Result<Vec<(NaiveDate, PathBuf)>> {
    let dir = root.join(exchange).join(product);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(&dir).with_context(|| format!("read {}", dir.display()))? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("parquet") {
            continue;
        }
        let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        if stem.len() != 8 {
            continue;
        }
        let Ok(day) = NaiveDate::parse_from_str(stem, "%Y%m%d") else {
            continue;
        };
        out.push((day, path));
    }
    out.sort_by_key(|(day, _)| *day);
    Ok(out)
}

fn list_jobs(root: &Path) -> Result<Vec<(String, String)>> {
    let mut jobs = Vec::new();
    if !root.is_dir() {
        return Ok(jobs);
    }
    for exchange in fs::read_dir(root)? {
        let exchange_path = exchange?.path();
        if !exchange_path.is_dir() {
            continue;
        }
        let Some(exchange) = exchange_path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if exchange.starts_with('_') {
            continue;
        }
        for product in fs::read_dir(&exchange_path)? {
            let product_path = product?.path();
            if !product_path.is_dir() {
                continue;
            }
            let Some(product) = product_path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            jobs.push((exchange.to_string(), product.to_string()));
        }
    }
    jobs.sort();
    Ok(jobs)
}

fn export_product(args: &YlabelArgs, exchange: &str, product: &str) -> Result<ExportStats> {
    let days = list_product_days(&args.in_root, exchange, product)?;
    let mut rows_by_id: HashMap<String, Vec<crate::baseline_1min::BaselineMinute>> = HashMap::new();
    let mut have_day = false;
    for (day, path) in &days {
        if *day < args.start || *day > args.end {
            continue;
        }
        have_day = true;
        for row in read_day_parquet(path)? {
            rows_by_id
                .entry(row.contract_id.clone())
                .or_default()
                .push(row);
        }
    }
    if !have_day {
        return Ok(ExportStats::default());
    }
    let mut by_day: HashMap<NaiveDate, Vec<YlabelRow>> = HashMap::new();
    for (contract_id, mut rows) in rows_by_id {
        rows.sort_by_key(|row| row.ts);
        let prices = causal_prices_from_minutes(&rows);
        for y in build_ylabel_rows(&contract_id, &prices) {
            let day = trad_day_from_ts(y.ts);
            if day < args.start || day > args.end {
                continue;
            }
            by_day.entry(day).or_default().push(y);
        }
    }
    let mut stats = ExportStats::default();
    let mut days: Vec<NaiveDate> = by_day.keys().copied().collect();
    days.sort();
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
        let mut rows = by_day.remove(&day).unwrap_or_default();
        if rows.is_empty() {
            continue;
        }
        rows.sort_by(|a, b| a.contract_id.cmp(&b.contract_id).then(a.ts.cmp(&b.ts)));
        let n = rows.len() as u64;
        write_ylabel_day(&dest, &rows)?;
        stats.files += 1;
        stats.rows += n;
        eprintln!(
            "{exchange} {product} {} rows={n} dest={}",
            day.format("%Y%m%d"),
            dest.display()
        );
    }
    Ok(stats)
}

pub fn run_export(args: YlabelArgs) -> Result<ExportStats> {
    if args.out_root.to_string_lossy().contains("cme_tas_rocksdb")
        || args.in_root.to_string_lossy().contains("cme_tas_rocksdb")
    {
        bail!("refusing to touch a CME RocksDB path");
    }
    if args.end < args.start {
        bail!("end precedes start");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    fs::create_dir_all(&args.out_root)
        .with_context(|| format!("create {}", args.out_root.display()))?;
    let mut jobs = list_jobs(&args.in_root)?;
    if let Some(filter) = &args.products {
        jobs.retain(|(_, product)| filter.iter().any(|item| item == product));
    }
    eprintln!(
        "cn_l2 export_ylabel products={} start={} end={} in={} out={}",
        jobs.len(),
        args.start,
        args.end,
        args.in_root.display(),
        args.out_root.display()
    );
    let args = Arc::new(args);
    let (tx, rx) = crossbeam_channel::unbounded::<(String, String)>();
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
        let args = Arc::clone(&args);
        let files = Arc::clone(&files);
        let rows = Arc::clone(&rows);
        let skipped = Arc::clone(&skipped);
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok((exchange, product)) = rx.recv() {
                let stats = export_product(&args, &exchange, &product)?;
                files.fetch_add(stats.files, Ordering::Relaxed);
                rows.fetch_add(stats.rows, Ordering::Relaxed);
                skipped.fetch_add(stats.skipped_existing, Ordering::Relaxed);
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.join().expect("ylabel worker")?;
    }
    Ok(ExportStats {
        files: files.load(Ordering::Relaxed),
        rows: rows.load(Ordering::Relaxed),
        skipped_existing: skipped.load(Ordering::Relaxed),
    })
}
