use anyhow::Result;
use clap::Parser;
use mkt_signal::cta_spread::signal::CtaSpreadSignalApp;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Debug, Parser)]
#[command(name = "cta_cross_spread_signal")]
struct Args {
    #[arg(long, default_value = "config/cta_cross_spread.json")]
    config: String,
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    anyhow::ensure!(
        mkt_signal::cta_spread::config::CtaSpreadConfig::load(&args.config)?.mode
            == mkt_signal::cta_spread::config::SpreadMode::OkexBinance,
        "cross spread requires okex_binance mode"
    );
    maybe_pin_current_thread(args.core, "CTA_CROSS_SPREAD_SIGNAL_CORE")?;
    tokio::task::LocalSet::new()
        .run_until(async move { CtaSpreadSignalApp::new(&args.config).await?.run().await })
        .await
}
