//! Stitch dominant 1-minute bars with additive hfq.
//!
//! Input: per-contract `baseline_data_1min`.
//! Output: `{out_root}/{exchange}/{product}/{TradDay}.parquet`
//! Ylabel is rebuilt from the adjusted series, not copied from raw ylabel.
//! Only products outside the maintained research universe are skipped. Finite
//! zero and negative additive-HFQ prices are retained; they are research prices
//! rather than raw quotations.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::export_1min::{read_day_parquet, write_day_parquet};
use crate::export_1s::{trad_day_from_ts, ExportStats};
use crate::export_ylabel::write_ylabel_day;
use crate::hfq::{
    apply_hfq_series, gap_before, index_dominants, instrument_match_key, list_roll_exchanges,
    load_adjustments, load_dominants,
};
use crate::universe::is_hfq_product;
use crate::ylabel_1m::{build_ylabel_rows, causal_prices_from_minutes};

pub const DEFAULT_IN_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";
pub const DEFAULT_ROLL_ROOT: &str =
    "/mnt/nvme-raid0-28t/fanghaizhou/cn_roll_replay/2019_20260826/result_no_rollback";
pub const DEFAULT_OUT_ROOT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min_hfq";
pub const DEFAULT_YLABEL_OUT: &str =
    "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/ylabel_1min_hfq";

#[derive(Clone, Debug)]
pub struct HfqArgs {
    pub in_root: PathBuf,
    pub roll_root: PathBuf,
    pub out_root: PathBuf,
    pub ylabel_out: PathBuf,
    pub start: NaiveDate,
    pub end: NaiveDate,
    pub workers: usize,
    pub products: Option<Vec<String>>,
    pub overwrite: bool,
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

fn export_product(args: &HfqArgs, exchange: &str, product: &str) -> Result<ExportStats> {
    let dominant_path = args.roll_root.join(format!("{exchange}_dominant.csv"));
    let adj_path = args
        .roll_root
        .join(format!("{exchange}_adjustment_factor.csv"));
    if !dominant_path.exists() {
        eprintln!(
            "skip {exchange} {product}: missing {}",
            dominant_path.display()
        );
        return Ok(ExportStats::default());
    }
    let dominants = load_dominants(&dominant_path)?;
    let factors = if adj_path.exists() {
        load_adjustments(&adj_path)?
            .into_iter()
            .filter(|row| row.product_id == product)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };
    let by_day = index_dominants(&dominants, product);
    let days = list_product_days(&args.in_root, exchange, product)?;
    let mut hfq_by_id: HashMap<String, Vec<crate::baseline_1min::BaselineMinute>> = HashMap::new();
    let mut stats = ExportStats::default();
    for (day, path) in days {
        if day < args.start || day > args.end {
            continue;
        }
        let dest = args
            .out_root
            .join(exchange)
            .join(product)
            .join(format!("{}.parquet", day.format("%Y%m%d")));
        if dest.exists() && !args.overwrite {
            stats.skipped_existing += 1;
            continue;
        }
        let Some(instrument) = by_day.get(&day) else {
            continue;
        };
        let want = instrument_match_key(instrument, day);
        let gap = match gap_before(&factors, day)? {
            Some(gap) => gap,
            None => continue,
        };
        let mut source_rows = read_day_parquet(&path)?
            .into_iter()
            .filter(|row| instrument_match_key(&row.contract_id, day) == want)
            .collect::<Vec<_>>();
        if source_rows.is_empty() {
            continue;
        }
        source_rows.sort_by_key(|row| row.ts);
        let mut rows = apply_hfq_series(source_rows, gap);
        for row in &mut rows {
            row.contract_id = product.to_string();
        }
        hfq_by_id
            .entry(product.to_string())
            .or_default()
            .extend(rows.iter().cloned());
        let n = rows.len() as u64;
        write_day_parquet(&dest, &rows)?;
        stats.files += 1;
        stats.rows += n;
        eprintln!(
            "{exchange} {product} {} instrument={instrument} gap={gap} rows={n}",
            day.format("%Y%m%d")
        );
    }
    let mut y_by_day: HashMap<NaiveDate, Vec<crate::ylabel_1m::YlabelRow>> = HashMap::new();
    for (contract_id, mut rows) in hfq_by_id {
        rows.sort_by_key(|row| row.ts);
        let prices = causal_prices_from_minutes(&rows);
        for y in build_ylabel_rows(&contract_id, &prices) {
            let day = trad_day_from_ts(y.ts);
            if day < args.start || day > args.end {
                continue;
            }
            y_by_day.entry(day).or_default().push(y);
        }
    }
    let mut ydays: Vec<NaiveDate> = y_by_day.keys().copied().collect();
    ydays.sort();
    for day in ydays {
        let dest = args
            .ylabel_out
            .join(exchange)
            .join(product)
            .join(format!("{}.parquet", day.format("%Y%m%d")));
        if dest.exists() && !args.overwrite {
            continue;
        }
        let mut rows = y_by_day.remove(&day).unwrap_or_default();
        if rows.is_empty() {
            continue;
        }
        rows.sort_by(|a, b| a.contract_id.cmp(&b.contract_id).then(a.ts.cmp(&b.ts)));
        write_ylabel_day(&dest, &rows)?;
    }
    Ok(stats)
}

pub fn run_export(args: HfqArgs) -> Result<ExportStats> {
    if args.out_root.to_string_lossy().contains("cme_tas_rocksdb")
        || args
            .ylabel_out
            .to_string_lossy()
            .contains("cme_tas_rocksdb")
    {
        bail!("refusing to write hfq into a CME RocksDB path");
    }
    if args.end < args.start {
        bail!("end precedes start");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    fs::create_dir_all(&args.out_root)?;
    fs::create_dir_all(&args.ylabel_out)?;
    let exchanges = list_roll_exchanges(&args.roll_root)?;
    let mut jobs = Vec::new();
    for exchange in exchanges {
        let dir = args.in_root.join(&exchange);
        if !dir.is_dir() {
            continue;
        }
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if !path.is_dir() {
                continue;
            }
            let Some(product) = path.file_name().and_then(|s| s.to_str()) else {
                continue;
            };
            if !is_hfq_product(product) {
                eprintln!("skip {exchange} {product}: excluded from additive hfq");
                continue;
            }
            if let Some(filter) = &args.products {
                if !filter.iter().any(|item| item == product) {
                    continue;
                }
            }
            jobs.push((exchange.clone(), product.to_string()));
        }
    }
    jobs.sort();
    eprintln!(
        "cn_l2 export_hfq products={} start={} end={} in={} roll={} out={}",
        jobs.len(),
        args.start,
        args.end,
        args.in_root.display(),
        args.roll_root.display(),
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
    let mut handles = Vec::new();
    for _ in 0..args.workers {
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
        handle.join().expect("hfq worker")?;
    }
    Ok(ExportStats {
        files: files.load(Ordering::Relaxed),
        rows: rows.load(Ordering::Relaxed),
        skipped_existing: skipped.load(Ordering::Relaxed),
    })
}
