use anyhow::{bail, Result};
use clap::Parser;
use std::path::PathBuf;
use usstock_lseg_mbp_replay::{load_config, preflight, replay_period, scan_csv, verify_period};

#[derive(Debug, Parser)]
#[command(name = "usstock_lseg_mbp_replay")]
#[command(about = "Losslessly replay decompressed LSEG US-stock MBP messages")]
struct Args {
    #[arg(long, default_value = "config.toml")]
    config: PathBuf,
    #[arg(long)]
    period: Option<String>,
    #[arg(long)]
    preflight: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    verify: bool,
    #[arg(long, requires = "dry_run")]
    max_messages: Option<u64>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = load_config(&args.config)?;
    let periods = match args.period {
        Some(period) => {
            if !config.periods.contains(&period) {
                bail!("period {period} is absent from config");
            }
            vec![period]
        }
        None => config.periods.clone(),
    };
    let modes = u8::from(args.preflight) + u8::from(args.dry_run) + u8::from(args.verify);
    if modes > 1 {
        bail!("--preflight, --dry-run, and --verify are mutually exclusive");
    }

    if args.preflight {
        return preflight(&config, &periods);
    }
    if args.dry_run {
        for period in periods {
            let path = config
                .data_root
                .join(usstock_lseg_mbp_replay::period_dir_name(&period))
                .join("merged-Data.csv");
            let census = scan_csv(&path, &period, args.max_messages, |_| Ok(()))?;
            println!("{}", serde_json::to_string_pretty(&census)?);
        }
        return Ok(());
    }
    if args.verify {
        for period in periods {
            let census = verify_period(&config, &period)?;
            println!("verified period={period} messages={}", census.messages);
        }
        return Ok(());
    }

    preflight(&config, &periods)?;
    for period in periods {
        let census = replay_period(&config, &period)?;
        println!("replayed period={period} messages={}", census.messages);
    }
    Ok(())
}
