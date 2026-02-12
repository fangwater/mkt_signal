//! Trade Flow Feature Publisher 入口
//!
//! 使用方式: cargo run --bin trade_flow_feature_pub -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::trade_flow_feature_pub::app::TradeFlowFeaturePubApp;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "trade_flow_feature_pub")]
#[command(about = "Trade Flow Feature Publisher - bar-level trade flow features from trade stream")]
struct Args {
    /// Trading venue (e.g., binance-futures, binance-margin, okex-futures)
    #[arg(short, long)]
    venue: TradingVenue,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let venue = args.venue;
    let venue_slug = venue.data_pub_slug();
    let venue_u8 = venue.to_u8();
    let config_path = "config/trade_flow_feature_pub.yaml";

    info!("Loading config from: {}", config_path);

    let mut app = TradeFlowFeaturePubApp::new(config_path, venue_slug, venue_u8, venue)?;
    app.run()
}
