//! Export ylabel_1min from baseline_data_1min. Do not use carried empty minutes as P[t].

use anyhow::{bail, Result};
use clap::Parser;
use cn_futures_l2::export_ylabel::{run_export, YlabelArgs, DEFAULT_IN_ROOT, DEFAULT_OUT_ROOT};
use cn_futures_l2::source::parse_day;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(about = "Export CN ylabel_1min from baseline_data_1min")]
struct Args {
    #[arg(long, default_value = DEFAULT_IN_ROOT)]
    in_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUT_ROOT)]
    out_root: PathBuf,
    #[arg(long)]
    start: String,
    #[arg(long)]
    end: String,
    #[arg(long, default_value_t = 8)]
    workers: usize,
    #[arg(long)]
    product: Option<String>,
    #[arg(long)]
    overwrite: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.out_root.to_string_lossy().contains("cme_tas_rocksdb") {
        bail!("refusing to write ylabel into a CME RocksDB path");
    }
    let products = args.product.as_ref().map(|text| {
        text.split(',')
            .map(|part| part.trim().to_ascii_uppercase())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    });
    let stats = run_export(YlabelArgs {
        in_root: args.in_root,
        out_root: args.out_root,
        start: parse_day(&args.start)?,
        end: parse_day(&args.end)?,
        workers: args.workers,
        products,
        overwrite: args.overwrite,
    })?;
    eprintln!(
        "cn_l2 export_ylabel ok files={} rows={} skipped_existing={}",
        stats.files, stats.rows, stats.skipped_existing
    );
    Ok(())
}
