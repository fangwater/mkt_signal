//! Divide existing baseline_data_1min VWAP columns by volume_multiple.
//!
//! Trade minutes are rebuilt as `amount / volume / volume_multiple`. Empty
//! minutes that already stored a close fallback stay quoted; carried
//! amount/volume values are scaled. A second run is a no-op.

use anyhow::{bail, Result};
use clap::Parser;
use cn_futures_l2::baseline_1min::rewrite_quoted_vwap;
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
use cn_futures_l2::multipliers::{load_multiplier_catalog, require_multiplier};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

const DEFAULT_ROOT: &str = "/mnt/hdd-raid5-72t/liang_torch/cn_futures_data/baseline_data_1min";

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = DEFAULT_ROOT)]
    root: PathBuf,
    #[arg(long, default_value_t = 8)]
    workers: usize,
}

fn list_jobs(root: &PathBuf) -> Result<Vec<(String, String, PathBuf)>> {
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
            for entry in fs::read_dir(&product_path)? {
                let path = entry?.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                    jobs.push((exchange.to_string(), product.to_string(), path));
                }
            }
        }
    }
    jobs.sort_by(|a, b| a.2.cmp(&b.2));
    Ok(jobs)
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.root.to_string_lossy().contains("cme_tas_rocksdb") {
        bail!("refusing to rewrite a CME RocksDB path");
    }
    if args.workers == 0 {
        bail!("--workers must be positive");
    }
    let catalog = load_multiplier_catalog()?;
    let jobs = list_jobs(&args.root)?;
    eprintln!(
        "rewrite_1min_vwap files={} products_catalog={} root={}",
        jobs.len(),
        catalog.len(),
        args.root.display()
    );
    let catalog = Arc::new(catalog);
    let (tx, rx) = crossbeam_channel::unbounded::<(String, String, PathBuf)>();
    for job in jobs {
        tx.send(job).expect("enqueue");
    }
    drop(tx);
    let files = Arc::new(AtomicU64::new(0));
    let rows = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..args.workers {
        let rx = rx.clone();
        let catalog = Arc::clone(&catalog);
        let files = Arc::clone(&files);
        let rows = Arc::clone(&rows);
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok((exchange, product, path)) = rx.recv() {
                let multiple = require_multiplier(&catalog, &product)?;
                let mut day_rows = read_day_parquet(&path)?;
                if day_rows.is_empty() {
                    continue;
                }
                for row in &mut day_rows {
                    rewrite_quoted_vwap(row, multiple);
                }
                let n = day_rows.len() as u64;
                write_day_parquet(&path, &day_rows)?;
                files.fetch_add(1, Ordering::Relaxed);
                rows.fetch_add(n, Ordering::Relaxed);
                eprintln!(
                    "{exchange} {product} {} multiple={multiple} rows={n}",
                    path.file_stem().and_then(|s| s.to_str()).unwrap_or("")
                );
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.join().expect("rewrite worker")?;
    }
    eprintln!(
        "rewrite_1min_vwap ok files={} rows={}",
        files.load(Ordering::Relaxed),
        rows.load(Ordering::Relaxed)
    );
    Ok(())
}
