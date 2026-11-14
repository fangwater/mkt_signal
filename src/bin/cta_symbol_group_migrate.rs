//! Prepare an offline, auditable symbol-group migration bundle.
use anyhow::{ensure, Context, Result};
use clap::Parser;
use mkt_signal::cta_factor_calc::{
    config::{Config, Plan, Worker},
    migration::{prepare, write_bundle, Request},
};
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "config/cta_prep_binance_futures.toml")]
    config: PathBuf,
    /// Captured original position-table directory; never modified.
    #[arg(long)]
    positions_dir: PathBuf,
    /// Cutover time and complete Exec/Manager ownership captures.
    #[arg(long)]
    request: PathBuf,
    /// New diagnostic directory outside the configuration and runtime stores.
    /// Omit to print the complete bundle without filesystem writes.
    #[arg(long)]
    output_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    let plan = Plan::load(&config.combinations_dir)?;
    Worker::Perp.validate_plan(&plan)?;
    let request: Request =
        serde_json::from_slice(&std::fs::read(&args.request)?).context("read migration request")?;
    let bundle = prepare(&plan, &args.positions_dir, &request)?;
    if let Some(output) = args.output_dir {
        let parent = output
            .parent()
            .context("output parent missing")?
            .canonicalize()?;
        let output = parent.join(output.file_name().context("output filename missing")?);
        for path in [
            &args.positions_dir,
            &config.combinations_dir,
            &config.history_dir,
        ] {
            if path.exists() {
                let path = path.canonicalize()?;
                ensure!(
                    !output.starts_with(&path) && !path.starts_with(&output),
                    "diagnostic output overlaps input or runtime state"
                );
            }
        }
        write_bundle(&output, &bundle)?;
        println!("{}", serde_json::to_string_pretty(&bundle.summary)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&bundle)?);
    }
    Ok(())
}
