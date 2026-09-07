use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use usstock_lseg_raw_replay::quote_replay::verify_raw_rocksdb;

#[derive(Debug, Parser)]
#[command(name = "usstock_lseg_raw_verify")]
#[command(about = "Verify complete fixed-message LSEG US-stock RAW RocksDB")]
struct Args {
    #[arg(long)]
    rocksdb_dir: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    println!("{}", verify_raw_rocksdb(&args.rocksdb_dir)?);
    Ok(())
}
