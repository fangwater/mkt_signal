use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use usstock_lseg_raw_replay::{load_config, run};

#[derive(Debug, Parser)]
#[command(name = "usstock_lseg_raw_replay")]
#[command(about = "Build message-safe zstd shards from LSEG US-stock RAW gzip parts")]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
    #[arg(long)]
    max_source_rows: Option<u64>,
}

fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format_timestamp_secs()
        .init();
    let args = Args::parse();
    let config = load_config(&args.config)?;
    run(&config, args.max_source_rows)
}
