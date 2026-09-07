use anyhow::{Context, Result};
use clap::Parser;
use std::fs;
use std::path::PathBuf;
use usstock_lseg_raw_replay::quote_replay::{
    acquire_raw_target_lock, load_raw_replay_config, raw_building_path, replay_raw,
};

#[derive(Debug, Parser)]
#[command(name = "usstock_lseg_raw_rocksdb")]
#[command(about = "Build complete fixed-message RocksDB from audited LSEG US-stock RAW shards")]
struct Args {
    #[arg(long, default_value = "raw_rocksdb.toml")]
    config: PathBuf,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();
    let args = Args::parse();
    let config = load_raw_replay_config(&args.config)
        .with_context(|| format!("load RAW replay config {}", args.config.display()))?;
    let _replay_lock = acquire_raw_target_lock(&config.rocksdb_dir)?;
    match replay_raw(&config) {
        Ok(census) => println!("{census}"),
        Err(error) => {
            let building_path = raw_building_path(&config.rocksdb_dir);
            if building_path.exists() {
                fs::remove_dir_all(&building_path).unwrap_or_else(|cleanup_error| {
                    panic!(
                        "RAW replay failed: {error:#}; cleanup {} also failed: {cleanup_error:#}",
                        building_path.display()
                    )
                });
            }
            panic!("RAW replay failed and incomplete output was removed: {error:#}");
        }
    }
    Ok(())
}
