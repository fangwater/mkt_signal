//! Fusion Factor Publisher 入口
//!
//! 使用方式:
//! cargo run --bin fusion_factor_1m_pub -- --venue binance-futures --config config/fusion_factor_1m_pub.toml

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::fusion_factor_pub::app::FusionFactorPubApp;
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;

#[derive(Parser)]
#[command(name = "fusion_factor_1m_pub")]
#[command(about = "1-minute fusion factor pipeline from trade_flow_feature_1m")]
struct Args {
    /// Trading venue (e.g., binance-futures, binance-margin, okex-futures)
    #[arg(short, long)]
    venue: TradingVenue,

    /// Config path
    #[arg(short, long, default_value = "config/fusion_factor_1m_pub.toml")]
    config: String,

    /// Bind main runtime thread to a CPU core. Falls back to FUSION_FACTOR_1M_CORE.
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "FUSION_FACTOR_1M_CORE")?;
    info!(
        "Starting fusion_factor_1m_pub: venue={} config={} input=trade_flow_feature_1m output=fusion_factor_1m/{}",
        args.venue.data_pub_slug(),
        args.config,
        args.venue.data_pub_slug()
    );

    let mut app = FusionFactorPubApp::new_one_minute(&args.config, args.venue).await?;
    app.run().await
}
