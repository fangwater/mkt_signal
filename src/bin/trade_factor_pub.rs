//! Trade Factor Publisher 入口
//!
//! 使用方式: cargo run --bin trade_factor_pub -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::trade_factor_pub::app::TradeFactorPubApp;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "trade_factor_pub")]
#[command(about = "Trade Factor Publisher - trade behavior factors from trade stream")]
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
    let config_path = "config/trade_factor_pub.yaml";

    info!("Loading config from: {}", config_path);

    let mut app = TradeFactorPubApp::new(config_path, venue_slug)?;
    app.run()
}
