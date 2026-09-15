//! Raw 1-minute intra-factor virtual model publisher.

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::intra_factor_model_1m_pub::app::IntraFactorModel1mPubApp;
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Parser)]
#[command(name = "intra_factor_model_1m_pub")]
#[command(about = "Publishes raw 1-minute intra factors as independent ModelMsg streams")]
struct Args {
    /// Trading venue (for example binance-futures)
    #[arg(short, long)]
    venue: TradingVenue,

    /// Config path
    #[arg(short, long, default_value = "config/intra_factor_model_1m_pub.toml")]
    config: String,

    /// Bind the main runtime thread to a CPU core. Falls back to INTRA_FACTOR_MODEL_1M_CORE.
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "INTRA_FACTOR_MODEL_1M_CORE")?;
    info!(
        "Starting intra_factor_model_1m_pub: venue={} config={} input=trade_flow_feature_1m output=model_output/intra-{}-1m-<factor>",
        args.venue.data_pub_slug(),
        args.config,
        args.venue.data_pub_slug(),
    );

    let mut app = IntraFactorModel1mPubApp::new(&args.config, args.venue).await?;
    app.run().await
}
