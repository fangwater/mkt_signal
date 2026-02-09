//! Kline Factor Publisher 入口
//!
//! 使用方式: cargo run --bin kline_factor_pub -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::kline_factor_pub::app::KlineFactorPubApp;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "kline_factor_pub")]
#[command(about = "Kline Factor Publisher - multi rolling factors")]
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
    let config_path = "config/kline_factor_pub.yaml";

    info!("Loading config from: {}", config_path);

    let mut app = KlineFactorPubApp::new(config_path, venue_slug)?;
    app.run()
}
