use anyhow::Result;
use clap::Parser;
use mkt_signal::factor_pub::cta_perp_factor_model_1m_pub::app::CtaPerpFactorModelPubApp;
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "cta_perp_factor_model_5m_pub")]
struct Args {
    #[arg(long, default_value = "binance-futures")]
    venue: TradingVenue,
    #[arg(
        long,
        default_value = "config/cta_perp_rx04/cta_perp_factor_model_5m_pub.toml"
    )]
    config: String,
    #[arg(long)]
    core: Option<usize>,
    /// Finish Kafka warm-up, then wait until the shared services are released.
    #[arg(long)]
    wait_for_publishers: bool,
    #[arg(long, default_value = "data/cta_perp_5m/factor_history")]
    history_path: PathBuf,
    /// Consume a fixed Kafka snapshot into history and exit without IPC output.
    #[arg(long, conflicts_with = "wait_for_publishers")]
    replay_only: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    maybe_pin_current_thread(args.core, "CTA_PERP_FACTOR_MODEL_5M_CORE")?;
    let mut app = CtaPerpFactorModelPubApp::new(
        &args.config,
        args.venue,
        args.wait_for_publishers,
        &args.history_path,
        args.replay_only,
        mkt_signal::cta_perp::interval::BarInterval::FiveMinutes,
    )
    .await?;
    if args.replay_only {
        Ok(())
    } else {
        app.run().await
    }
}
