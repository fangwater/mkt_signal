//! PairMM Resample 入口
//!
//! 使用方式: cargo run --bin pairmm_resample -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::pairmm_resample::app::PairMmResampleApp;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "pairmm_resample")]
#[command(about = "PairMM Resample - publish random pairmm factors")]
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
    let venue_u8 = args.venue.to_u8();
    let venue = args.venue;
    let config_path = "config/pairmm_resample.yaml";

    info!("Loading config from: {}", config_path);

    let mut app = PairMmResampleApp::new(config_path, venue_slug, venue_u8, venue)?;
    app.run()
}
