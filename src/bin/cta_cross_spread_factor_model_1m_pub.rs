use anyhow::Result;
use clap::Parser;
use mkt_signal::factor_pub::cta_spread_factor_model_1m_pub::app::CtaSpreadFactorModel1mPubApp;
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(name = "cta_cross_spread_factor_model_1m_pub")]
struct Args {
    #[arg(long, default_value = "binance-futures")]
    venue: TradingVenue,
    #[arg(
        long,
        default_value = "config/cta_cross_spread_factor_model_1m_pub.toml"
    )]
    config: String,
    #[arg(long)]
    core: Option<usize>,
    /// Replay beside the current publisher without writing history/status;
    /// take over live output/history after the shared services are released.
    #[arg(long)]
    wait_for_publishers: bool,
    #[arg(long, default_value = "data/cta_cross_spread/factor_history")]
    history_path: PathBuf,
    /// Consume a fixed Kafka snapshot into history and exit without IPC output.
    #[arg(long, conflicts_with = "wait_for_publishers")]
    replay_only: bool,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();
    anyhow::ensure!(
        mkt_signal::factor_pub::cta_spread_factor_model_1m_pub::cfg::CtaSpreadFactorPubConfig::load(
            &args.config
        )?
        .mode == mkt_signal::cta_spread::config::SpreadMode::OkexBinance,
        "cross spread requires okex_binance mode"
    );
    maybe_pin_current_thread(args.core, "CTA_CROSS_SPREAD_FACTOR_MODEL_1M_CORE")?;
    let mut app = CtaSpreadFactorModel1mPubApp::new(
        &args.config,
        args.venue,
        args.wait_for_publishers,
        &args.history_path,
        args.replay_only,
    )
    .await?;
    if args.replay_only {
        Ok(())
    } else {
        app.run().await
    }
}
