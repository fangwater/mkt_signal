//! Sample baseline_data_1min / hfq parquet for empty minutes and missing books.

use anyhow::{Context, Result};
use arrow::array::{Array, Float64Array, Int64Array};
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::path::{Path, PathBuf};

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    root: PathBuf,
    #[arg(long, default_value_t = 8)]
    files_per_product: usize,
}

#[derive(Default)]
struct Acc {
    files: u64,
    rows: u64,
    vol0: u64,
    no_book: u64,
    vol0_no_book: u64,
    vol_pos_no_book: u64,
    no_bid0: u64,
    no_ask0: u64,
}

fn audit_file(path: &Path, acc: &mut Acc) -> Result<()> {
    let file = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    acc.files += 1;
    for batch in reader {
        let batch = batch?;
        let n = batch.num_rows();
        acc.rows += n as u64;
        let volume = batch
            .column_by_name("volume")
            .context("volume column")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("volume")?;
        let bid0p = batch
            .column_by_name("bid0p")
            .context("bid0p column")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("bid0p")?;
        let ask0p = batch
            .column_by_name("ask0p")
            .context("ask0p column")?
            .as_any()
            .downcast_ref::<Float64Array>()
            .context("ask0p")?;
        let _ts = batch
            .column_by_name("ts")
            .context("ts column")?
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("ts")?;
        for i in 0..n {
            let vol = if volume.is_null(i) {
                0.0
            } else {
                volume.value(i)
            };
            let vol0 = vol == 0.0;
            let no_bid = bid0p.is_null(i) || !bid0p.value(i).is_finite() || bid0p.value(i) <= 0.0;
            let no_ask = ask0p.is_null(i) || !ask0p.value(i).is_finite() || ask0p.value(i) <= 0.0;
            let no_book = no_bid || no_ask;
            if vol0 {
                acc.vol0 += 1;
            }
            if no_book {
                acc.no_book += 1;
            }
            if no_bid {
                acc.no_bid0 += 1;
            }
            if no_ask {
                acc.no_ask0 += 1;
            }
            if vol0 && no_book {
                acc.vol0_no_book += 1;
            }
            if !vol0 && no_book {
                acc.vol_pos_no_book += 1;
            }
        }
    }
    Ok(())
}

fn list_parquets(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    if !dir.is_dir() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|ext| ext.to_str()) == Some("parquet") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut total = Acc::default();
    let mut by_ex: Vec<(String, Acc)> = Vec::new();
    for exchange in fs::read_dir(&args.root)? {
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
        let mut ex = Acc::default();
        let mut products: Vec<PathBuf> = fs::read_dir(&exchange_path)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_dir())
            .collect();
        products.sort();
        for product_dir in products {
            let mut files = list_parquets(&product_dir)?;
            if files.is_empty() {
                continue;
            }
            let n = files.len();
            let mut picked = Vec::new();
            let take = args.files_per_product.min(n);
            if take == 1 {
                picked.push(files.remove(0));
            } else {
                picked.push(files[0].clone());
                if n > 1 {
                    picked.push(files[n / 3].clone());
                    picked.push(files[2 * n / 3].clone());
                    picked.push(files[n - 1].clone());
                }
                picked.truncate(take);
            }
            picked.sort();
            picked.dedup();
            for path in picked {
                audit_file(&path, &mut ex)?;
            }
        }
        eprintln!(
            "{} files={} rows={} vol0={} ({:.2}%) no_book={} ({:.2}%) vol0_no_book={} vol>0_no_book={} no_bid0={} no_ask0={}",
            exchange,
            ex.files,
            ex.rows,
            ex.vol0,
            pct(ex.vol0, ex.rows),
            ex.no_book,
            pct(ex.no_book, ex.rows),
            ex.vol0_no_book,
            ex.vol_pos_no_book,
            ex.no_bid0,
            ex.no_ask0,
        );
        total.files += ex.files;
        total.rows += ex.rows;
        total.vol0 += ex.vol0;
        total.no_book += ex.no_book;
        total.vol0_no_book += ex.vol0_no_book;
        total.vol_pos_no_book += ex.vol_pos_no_book;
        total.no_bid0 += ex.no_bid0;
        total.no_ask0 += ex.no_ask0;
        by_ex.push((exchange.to_string(), ex));
    }
    eprintln!(
        "TOTAL files={} rows={} vol0={} ({:.2}%) no_book={} ({:.2}%) vol0_no_book={} ({:.2}%) vol>0_no_book={} ({:.2}%)",
        total.files,
        total.rows,
        total.vol0,
        pct(total.vol0, total.rows),
        total.no_book,
        pct(total.no_book, total.rows),
        total.vol0_no_book,
        pct(total.vol0_no_book, total.rows),
        total.vol_pos_no_book,
        pct(total.vol_pos_no_book, total.rows),
    );
    let _ = by_ex;
    Ok(())
}

fn pct(part: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        100.0 * part as f64 / total as f64
    }
}
