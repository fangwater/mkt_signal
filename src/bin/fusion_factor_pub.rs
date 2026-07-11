//! Fusion Factor Publisher 入口
//!
//! 使用方式:
//! cargo run --bin fusion_factor_pub -- --venue binance-futures --config config/fusion_factor_pub.toml

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::factor_pub::fusion_factor_pub::app::FusionFactorPubApp;
use order_common::TradingVenue;
use runtime_common::affinity::maybe_pin_current_thread;

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

    /// Bind main runtime thread to a CPU core. Falls back to FUSION_FACTOR_CORE.
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "FUSION_FACTOR_CORE")?;
    info!(
        "Starting fusion_factor_pub: venue={} config={}",
        args.venue.data_pub_slug(),
        args.config
    );

    let mut app = FusionFactorPubApp::new(&args.config, args.venue).await?;
    let mut one_minute = FusionFactorPubApp::try_new_one_minute(&args.config, args.venue).await?;
    info!(
        "fusion_factor_pub pipelines: venue={} 5s=enabled 1m={}",
        args.venue.data_pub_slug(),
        if one_minute.is_some() {
            "enabled"
        } else {
            "disabled"
        }
    );
    app.run_with_optional_one_minute(one_minute.as_mut()).await
}
