use anyhow::Result;
use clap::Parser;
use cme_tas_replay::raw_ll2_io::Config;
use std::{fs, path::PathBuf};
#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/cme_raw_ll2_2026_15s.toml")]
    config: PathBuf,
    #[arg(long)]
    period: Option<String>,
    /// Stage finalized gzip parts while other parts are pending; never publish a period manifest.
    #[arg(long)]
    available_parts: bool,
}
fn main() -> Result<()> {
    let args = Args::parse();
    let mut config: Config = toml::from_str(&fs::read_to_string(args.config)?)?;
    if let Some(p) = args.period {
        config.periods = vec![p];
    }
    if args.available_parts {
        cme_tas_replay::raw_ll2_stage::build_available(&config)
    } else {
        cme_tas_replay::raw_ll2_stage::build(&config)
    }
}
