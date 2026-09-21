use anyhow::Result;
use clap::Parser;
use mkt_signal::cta_special::signal::CtaSpecialSignalApp;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Debug, Parser)]
#[command(name = "cta_special_signal")]
struct Args {
    #[arg(long, default_value = "config/cta_special.json")]
    config: String,
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "CTA_SPECIAL_SIGNAL_CORE")?;
    CtaSpecialSignalApp::new(&args.config).await?.run().await
}
