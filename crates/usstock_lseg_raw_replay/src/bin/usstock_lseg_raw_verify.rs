use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use usstock_lseg_raw_replay::quote_replay::{
    compare_raw_data, compare_raw_rocksdb, verify_raw_rocksdb,
};

#[derive(Debug, Parser)]
#[command(name = "usstock_lseg_raw_verify")]
#[command(about = "Verify complete fixed-message LSEG US-stock RAW RocksDB")]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
    /// Compare every column-family key/value byte with another replay.
    #[arg(long)]
    compare_to: Option<PathBuf>,
    /// Compare data CFs only; source-scope metadata may intentionally differ.
    #[arg(long, requires = "compare_to")]
    data_only: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    println!("{}", verify_raw_rocksdb(&args.rocksdb_dir)?);
    if let Some(right) = args.compare_to {
        let count = if args.data_only {
            compare_raw_data(&args.rocksdb_dir, &right)?
        } else {
            compare_raw_rocksdb(&args.rocksdb_dir, &right)?
        };
        println!(
            "RAW logical comparison data_only={} identical_rows={count}",
            args.data_only
        );
    }
    Ok(())
}
