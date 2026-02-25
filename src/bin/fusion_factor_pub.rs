//! Fusion Factor Publisher 入口
//!
//! 使用方式:
//! cargo run --bin fusion_factor_pub -- --venue binance-futures --config config/fusion_factor_pub.toml

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::fusion_factor_pub::app::FusionFactorPubApp;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "fusion_factor_pub")]
#[command(about = "Fusion factor pipeline from unified trade_flow_feature stream")]
struct Args {
    /// Trading venue (e.g., binance-futures, binance-margin, okex-futures)
    #[arg(short, long)]
    venue: TradingVenue,

    /// Config path
    #[arg(short, long, default_value = "config/fusion_factor_pub.toml")]
    config: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    info!(
        "Starting fusion_factor_pub: venue={} config={}",
        args.venue.data_pub_slug(),
        args.config
    );

    let mut app = FusionFactorPubApp::new(&args.config, args.venue).await?;
    app.run()
}
