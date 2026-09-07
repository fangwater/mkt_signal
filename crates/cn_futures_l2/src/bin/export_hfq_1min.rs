//! Stitch dominant 1-minute bars with additive hfq, then rebuild ylabel.

use anyhow::{bail, Result};
use clap::Parser;
use cn_futures_l2::export_hfq::{
    run_export, HfqArgs, DEFAULT_IN_ROOT, DEFAULT_OUT_ROOT, DEFAULT_ROLL_ROOT, DEFAULT_YLABEL_OUT,
};
use cn_futures_l2::source::parse_day;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(about = "Export CN baseline_data_1min_hfq and ylabel_1min_hfq")]
struct Args {
    #[arg(long, default_value = DEFAULT_IN_ROOT)]
    in_root: PathBuf,
    #[arg(long, default_value = DEFAULT_ROLL_ROOT)]
    roll_root: PathBuf,
    #[arg(long, default_value = DEFAULT_OUT_ROOT)]
    out_root: PathBuf,
    #[arg(long, default_value = DEFAULT_YLABEL_OUT)]
    ylabel_out: PathBuf,
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
    if args.out_root.to_string_lossy().contains("cme_tas_rocksdb")
        || args
            .ylabel_out
            .to_string_lossy()
            .contains("cme_tas_rocksdb")
    {
        bail!("refusing to write hfq into a CME RocksDB path");
    }
    let products = args.product.as_ref().map(|text| {
        text.split(',')
            .map(|part| part.trim().to_ascii_uppercase())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
    });
    let stats = run_export(HfqArgs {
        in_root: args.in_root,
        roll_root: args.roll_root,
        out_root: args.out_root,
        ylabel_out: args.ylabel_out,
        start: parse_day(&args.start)?,
        end: parse_day(&args.end)?,
        workers: args.workers,
        products,
        overwrite: args.overwrite,
    })?;
    eprintln!(
        "cn_l2 export_hfq ok files={} rows={} skipped_existing={}",
        stats.files, stats.rows, stats.skipped_existing
    );
    Ok(())
}
