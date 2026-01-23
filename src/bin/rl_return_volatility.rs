//! RL Return Volatility 入口
//!
//! 使用方式: cargo run --bin rl_return_volatility -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::rl_return_volatility::app::RlReturnVolatilityApp;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "rl_return_volatility")]
#[command(about = "RL Return Volatility - rolling return volatility factor")]
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
    let config_path = "config/rl_return_volatility.yaml";

    info!("Loading config from: {}", config_path);

    let mut app = RlReturnVolatilityApp::new(config_path, venue_slug)?;
    app.run()
}
