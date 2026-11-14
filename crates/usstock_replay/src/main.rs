use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "usstock_replay")]
#[command(about = "Replay LSEG US equity TAS gzip files into RocksDB")]
struct Args {
    #[arg(long, default_value = "config/usstock_replay.toml")]
    config: PathBuf,

    /// Diagnostic cap per input gzip file. A capped run is intentionally not watermarked done.
    #[arg(long)]
    max_source_rows: Option<u64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut config = usstock_replay::load_config(&args.config)
        .with_context(|| format!("load config {}", args.config.display()))?;
    if let Some(max_source_rows) = args.max_source_rows {
        config.max_source_rows = Some(max_source_rows);
    }
    let census = usstock_replay::replay(&config)?;
    println!("{census}");
    Ok(())
}
