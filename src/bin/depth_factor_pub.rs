//! Depth Factor Publisher 入口
//!
//! 使用方式: cargo run --bin depth_factor_pub -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::depth_factor_pub::app::DepthFactorPubApp;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "depth_factor_pub")]
#[command(about = "Depth Factor Publisher - orderbook factors from depth snapshots")]
struct Args {
    /// Trading venue (e.g., binance-futures, binance-margin, okex-futures)
    #[arg(short, long)]
    venue: TradingVenue,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let venue_slug = args.venue.data_pub_slug();
    let config_path = "config/depth_factor_pub.yaml";

    info!("Loading config from: {}", config_path);

    let mut app = DepthFactorPubApp::new(config_path, venue_slug)?;
    app.run()
}
