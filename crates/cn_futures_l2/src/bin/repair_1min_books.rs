//! Repair already-written baseline_data_1min parquet.
//!
//! Equity-index minutes after 15:00 Shanghai are dropped. Empty minutes
//! inherit the last two-sided book of the same contract inside a session.

use anyhow::{bail, Result};
use clap::Parser;
use cn_futures_l2::baseline_1min::repair_session_books;
use cn_futures_l2::export_1min::{read_day_parquet, write_day_parquet};
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

fn list_jobs(root: &PathBuf) -> Result<Vec<PathBuf>> {
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
            for entry in fs::read_dir(&product_path)? {
                let path = entry?.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
                    jobs.push(path);
                }
            }
        }
    }
    jobs.sort();
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
    let jobs = list_jobs(&args.root)?;
    eprintln!(
        "repair_1min_books files={} root={}",
        jobs.len(),
        args.root.display()
    );
    let (tx, rx) = crossbeam_channel::unbounded::<PathBuf>();
    for job in jobs {
        tx.send(job).expect("enqueue");
    }
    drop(tx);
    let files = Arc::new(AtomicU64::new(0));
    let kept = Arc::new(AtomicU64::new(0));
    let dropped = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();
    for _ in 0..args.workers {
        let rx = rx.clone();
        let files = Arc::clone(&files);
        let kept = Arc::clone(&kept);
        let dropped = Arc::clone(&dropped);
        handles.push(std::thread::spawn(move || -> Result<()> {
            while let Ok(path) = rx.recv() {
                let rows = read_day_parquet(&path)?;
                if rows.is_empty() {
                    continue;
                }
                let before = rows.len() as u64;
                let repaired = repair_session_books(rows);
                let after = repaired.len() as u64;
                write_day_parquet(&path, &repaired)?;
                files.fetch_add(1, Ordering::Relaxed);
                kept.fetch_add(after, Ordering::Relaxed);
                dropped.fetch_add(before.saturating_sub(after), Ordering::Relaxed);
            }
            Ok(())
        }));
    }
    for handle in handles {
        handle.join().expect("repair worker")?;
    }
    eprintln!(
        "repair_1min_books ok files={} kept={} dropped={}",
        files.load(Ordering::Relaxed),
        kept.load(Ordering::Relaxed),
        dropped.load(Ordering::Relaxed)
    );
    Ok(())
}
