use anyhow::{bail, Result};
use clap::Parser;
use std::fs;
use std::path::PathBuf;
use usstock_lseg_rawll2_replay::{load_config, preflight, replay_period, verify_period};

#[derive(Debug, Parser)]
#[command(about = "Replay source-exact LSEG US-stock rawLL2 FID patches")]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
    #[arg(long)]
    period: Option<String>,
    #[arg(long)]
    preflight: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long, requires = "dry_run")]
    max_messages: Option<u64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = load_config(&args.config)?;
    let periods = match args.period {
        Some(period) if config.periods.contains(&period) => vec![period],
        Some(period) => bail!("period {period} is not configured"),
        None => config.periods.clone(),
    };
    if u8::from(args.preflight) + u8::from(args.verify) + u8::from(args.dry_run) > 1 {
        bail!("modes are mutually exclusive");
    }
    if args.preflight {
        return preflight(&config, &periods);
    }
    if args.verify {
        for period in periods {
            println!(
                "verified {period}: {} messages",
                verify_period(&config, &period)?.messages
            );
        }
        return Ok(());
    }
    for period in periods {
        let building = config.rocksdb_root.join(format!("{period}.building"));
        let limit = if args.dry_run {
            Some(args.max_messages.unwrap_or(100_000))
        } else {
            None
        };
        match replay_period(&config, &period, limit) {
            Ok(census) => println!("replayed {period}: {} messages", census.messages),
            Err(error) => {
                if building.exists() {
                    fs::remove_dir_all(&building)?;
                }
                return Err(error);
            }
        }
    }
    Ok(())
}
