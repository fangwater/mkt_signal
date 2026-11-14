use anyhow::Result;
use clap::Parser;
use mkt_signal::cta_spread::signal::CtaSpreadSignalApp;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Debug, Parser)]
#[command(name = "cta_spread_signal")]
struct Args {
    #[arg(long, default_value = "config/cta_spread.json")]
    config: String,
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "CTA_SPREAD_SIGNAL_CORE")?;
    tokio::task::LocalSet::new()
        .run_until(async move { CtaSpreadSignalApp::new(&args.config).await?.run().await })
        .await
}
