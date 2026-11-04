use anyhow::Result;
use clap::Parser;
use mkt_signal::{cta_factor_calc::config::Combination, cta_perp::signal::CtaPerpSignalApp};
use runtime_common::affinity::maybe_pin_current_thread;
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    config: PathBuf,
    #[arg(long)]
    core: Option<usize>,
    #[arg(long)]
    check: bool,
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    if args.check {
        let combination = Combination::load(&args.config)?;
        anyhow::ensure!(
            combination.venue == mkt_signal::cta_factor_calc::config::Market::BinanceFutures,
            "this signal executes Binance futures"
        );
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"name":combination.name,"manager_strategy_name":combination.manager_strategy_name(),"venue":combination.venue,"interval":combination.interval,"signal":combination.signal_config()?,"symbols":combination.signal_table()?.len()})
            )?
        );
        return Ok(());
    }
    maybe_pin_current_thread(args.core, "CTA_SIGNAL_CORE")?;
    tokio::task::LocalSet::new()
        .run_until(async move { CtaPerpSignalApp::new(args.config).await?.run().await })
        .await
}
