//! Overlay 12 size-bucket columns onto baseline_data_1min.
//!
//! Thresholds are per product, Shanghai calendar month, linear P50/P90 of L2
//! inferred trade amounts. Amounts themselves are not adjusted.

use anyhow::{bail, Context, Result};
use chrono::{Datelike, TimeZone};
use chrono_tz::Asia::Shanghai;
use clap::Parser;
use cn_futures_l2::baseline_1min::{linear_percentile, minute_left_sec, SizeBuckets};
use cn_futures_l2::codec::{decode_key, decode_trade, encode_key, KIND_TRADE};
use cn_futures_l2::db::{
    open_rocksdb_read_only, L2Db, DEFAULT_ROCKSDB_DIR, FORBIDDEN_ROCKSDB_MARK,
};
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
use cn_futures_l2::export_1s::parse_product_cf;
use cn_futures_l2::universe::is_maintained_product;
use rocksdb::{Direction, IteratorMode};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const DEFAULT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = DEFAULT_ROCKSDB_DIR)]
    rocksdb_dir: PathBuf,
    #[arg(long, default_value = DEFAULT_ROOT)]
    root: PathBuf,
    #[arg(long, default_value_t = 4)]
    workers: usize,
    #[arg(long)]
    product: Option<String>,
}

fn year_month(ts_utc_ns: u64) -> u32 {
    let local = Shanghai
        .timestamp_opt((ts_utc_ns / 1_000_000_000) as i64, 0)
        .single()
        .expect("unix second");
    local.year() as u32 * 100 + local.month()
}

fn list_product_days(root: &Path, exchange: &str, product: &str) -> Result<Vec<PathBuf>> {
    let dir = root.join(exchange).join(product);
    if !dir.is_dir() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn exchange_of_product(root: &Path, product: &str) -> Result<Option<String>> {
    for exchange in fs::read_dir(root)? {
        let path = exchange?.path();
        if !path.is_dir() {
            continue;
        }
        let Some(exchange) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if exchange.starts_with('_') {
            continue;
        }
        if path.join(product).is_dir() {
            return Ok(Some(exchange.to_string()));
        }
    }
    Ok(None)
}

fn scan_product_trades(
    db: &L2Db,
    cf_names: &[String],
) -> Result<(
    HashMap<u32, (f64, f64)>,
    HashMap<(String, i64), SizeBuckets>,
)> {
    let mut amounts: HashMap<u32, Vec<f64>> = HashMap::new();
    let mut trades: Vec<(String, u64, f64, u8)> = Vec::new();
    for cf_name in cf_names {
        let cf = db
            .cf_handle(cf_name)
            .ok_or_else(|| anyhow::anyhow!("missing cf {cf_name}"))?;
        let start = encode_key(KIND_TRADE, "", 0, 0)?;
        for item in db.iterator_cf(&cf, IteratorMode::From(&start, Direction::Forward)) {
            let (key, value) = item.context("iterate trade")?;
            let (kind, id, ts_ns, _) = decode_key(&key)?;
            if kind != KIND_TRADE {
                break;
            }
            let rec = decode_trade(&value)?;
            let amount = rec.turnover.unwrap_or(rec.price * rec.volume);
            if !(amount.is_finite() && amount > 0.0) {
                continue;
            }
            amounts.entry(year_month(ts_ns)).or_default().push(amount);
            trades.push((id, ts_ns, amount, rec.aggressor));
        }
    }
    let mut thresholds = HashMap::new();
    for (month, mut values) in amounts {
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let Some(p50) = linear_percentile(&values, 0.5) else {
            continue;
        };
        let Some(p90) = linear_percentile(&values, 0.9) else {
            continue;
        };
        thresholds.insert(month, (p50, p90));
    }
    let mut buckets: HashMap<(String, i64), SizeBuckets> = HashMap::new();
    for (id, ts_ns, amount, aggressor) in trades {
        let Some(&(p50, p90)) = thresholds.get(&year_month(ts_ns)) else {
            continue;
        };
        let key = (id, minute_left_sec(ts_ns));
        buckets
            .entry(key)
            .or_default()
            .add(amount, aggressor, p50, p90);
    }
    Ok((thresholds, buckets))
}

fn overlay_day(path: &Path, buckets: &HashMap<(String, i64), SizeBuckets>) -> Result<u64> {
    let mut rows = read_day_parquet(path)?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut hit = 0u64;
    for row in &mut rows {
        let key = (row.contract_id.clone(), row.ts);
        if let Some(sizes) = buckets.get(&key) {
            row.large_order = sizes.large_order;
            row.medium_order = sizes.medium_order;
            row.small_order = sizes.small_order;
            row.large_buy = sizes.large_buy;
            row.large_sell = sizes.large_sell;
            row.medium_buy = sizes.medium_buy;
            row.medium_sell = sizes.medium_sell;
            row.small_buy = sizes.small_buy;
            row.small_sell = sizes.small_sell;
            let (nl, nm, ns) = sizes.nets();
            row.net_buy_large = nl;
            row.net_buy_medium = nm;
            row.net_buy_small = ns;
            hit += 1;
        } else {
            row.large_order = 0.0;
            row.medium_order = 0.0;
            row.small_order = 0.0;
            row.large_buy = 0.0;
            row.large_sell = 0.0;
            row.medium_buy = 0.0;
            row.medium_sell = 0.0;
            row.small_buy = 0.0;
            row.small_sell = 0.0;
            row.net_buy_large = 0.0;
            row.net_buy_medium = 0.0;
            row.net_buy_small = 0.0;
        }
    }
    write_day_parquet(path, &rows)?;
    Ok(hit)
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.root.to_string_lossy().contains(FORBIDDEN_ROCKSDB_MARK)
        || args
            .rocksdb_dir
            .to_string_lossy()
            .contains(FORBIDDEN_ROCKSDB_MARK)
    {
        bail!("refusing to touch a CME RocksDB path");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    let db = open_rocksdb_read_only(&args.rocksdb_dir)?;
    let names = L2Db::list_cf(&rocksdb::Options::default(), &args.rocksdb_dir)?;
    let mut by_product: HashMap<String, Vec<String>> = HashMap::new();
    for name in names {
        let Some((_, product)) = parse_product_cf(&name) else {
            continue;
        };
        if !is_maintained_product(&product) {
            continue;
        }
        if let Some(filter) = &args.product {
            if product != filter.to_ascii_uppercase() {
                continue;
            }
        }
        by_product.entry(product).or_default().push(name);
    }
    let mut jobs: Vec<(String, Vec<String>)> = by_product.into_iter().collect();
    jobs.sort_by(|a, b| a.0.cmp(&b.0));
    eprintln!(
        "fill_1min_size_buckets products={} root={}",
        jobs.len(),
        args.root.display()
    );
    let db = Arc::new(db);
    let root = Arc::new(args.root);
    let (tx, rx) = crossbeam_channel::unbounded::<(String, Vec<String>)>();
    for job in jobs {
        tx.send(job).expect("enqueue");
    }
    drop(tx);
    let files = Arc::new(AtomicU64::new(0));
    let hits = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..args.workers {
        let rx = rx.clone();
        let db = Arc::clone(&db);
        let root = Arc::clone(&root);
        let files = Arc::clone(&files);
        let hits = Arc::clone(&hits);
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok((product, mut cf_names)) = rx.recv() {
                cf_names.sort();
                let Some(exchange) = exchange_of_product(&root, &product)? else {
                    eprintln!("skip {product}: no baseline_data_1min directory");
                    continue;
                };
                let days = list_product_days(&root, &exchange, &product)?;
                if days.is_empty() {
                    eprintln!("skip {exchange} {product}: empty 1min directory");
                    continue;
                }
                let (_thresholds, buckets) = scan_product_trades(&db, &cf_names)?;
                let mut product_hits = 0u64;
                for path in &days {
                    product_hits += overlay_day(path, &buckets)?;
                    files.fetch_add(1, Ordering::Relaxed);
                }
                hits.fetch_add(product_hits, Ordering::Relaxed);
                eprintln!(
                    "{exchange} {product} days={} traded_minutes={} overlay_hits={product_hits}",
                    days.len(),
                    buckets.len()
                );
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.join().expect("size-bucket worker")?;
    }
    eprintln!(
        "fill_1min_size_buckets ok files={} overlay_hits={}",
        files.load(Ordering::Relaxed),
        hits.load(Ordering::Relaxed)
    );
    Ok(())
}
