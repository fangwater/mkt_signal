//! Kline Publisher 入口
//!
//! 订阅 mkt_pub 的 trade 数据，合成 K 线并发布
//!
//! 使用方式: cargo run --bin kline_pub -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::kline_pub::app::KlinePubApp;
use mkt_signal::kline_pub::cfg::KlinePubConfig;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "kline_pub")]
#[command(about = "Kline Publisher - 订阅成交，合成 K 线")]
struct Args {
    /// Trading venue (e.g., binance-futures, binance-margin, okex-futures)
    #[arg(short, long)]
    venue: TradingVenue,
    /// K线周期（毫秒）
    #[arg(long)]
    period_ms: Option<u64>,
    /// 封bar延迟（微秒）
    #[arg(long)]
    close_delay_us: Option<u64>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    let venue_slug = args.venue.data_pub_slug();

    let config_path = "config/kline_cfg.yaml";
    info!("Loading config from: {}", config_path);

    let mut config = KlinePubConfig::load(config_path).await?;
    if let Some(period_ms) = args.period_ms {
        config.kline_timing.period_ms = period_ms;
    }
    if let Some(delay_us) = args.close_delay_us {
        config.kline_timing.close_delay_us = delay_us;
    }

    info!(
        "Config loaded: period={}ms, delay={}us",
        config.kline_timing.period_ms, config.kline_timing.close_delay_us
    );

    let mut app = KlinePubApp::new(config, venue_slug)?;
    app.run()
}
