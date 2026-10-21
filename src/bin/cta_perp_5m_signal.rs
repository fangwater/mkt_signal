use anyhow::Result;
use clap::Parser;
use mkt_signal::cta_perp::signal::CtaPerpSignalApp;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Debug, Parser)]
#[command(name = "cta_perp_5m_signal")]
struct Args {
    #[arg(long, default_value = "config/cta_perp_rx04/cta_perp.json")]
    config: String,
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "CTA_PERP_5M_SIGNAL_CORE")?;
    tokio::task::LocalSet::new()
        .run_until(async move {
            CtaPerpSignalApp::new_for_interval(
                &args.config,
                mkt_signal::cta_perp::interval::BarInterval::FiveMinutes,
            )
            .await?
            .run()
            .await
        })
        .await
}
