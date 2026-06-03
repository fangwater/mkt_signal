//! Depth Publisher 入口
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿，发布深度快照
//!
//! 使用方式: cargo run --bin depth_pub -- --venue binance-futures

use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};
use log::info;

use mkt_signal::common::affinity::maybe_pin_current_thread;
use mkt_signal::depth_pub::app::DepthPubRunner;
use mkt_signal::depth_pub::cfg::DepthPubConfig;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "depth_pub")]
#[command(about = "Depth Publisher - 订阅增量数据，发布深度快照")]
struct Args {
    /// Trading venue. Repeat for multi-venue mode, or use <exchange>-both.
    #[arg(short, long, required = true)]
    venue: Vec<String>,

    /// 绑定主线程到指定 CPU 核（可选）；未提供则尝试 DEPTH_PUB_CORE 环境变量
    #[arg(long)]
    core: Option<usize>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    env_logger::init();

    let args = Args::parse();
    maybe_pin_current_thread(args.core, "DEPTH_PUB_CORE")?;

    // 固定配置文件路径
    let config_path = "config/depth_cfg.yaml";
    info!("Loading config from: {}", config_path);

    let config = DepthPubConfig::load(config_path).await?;
    info!(
        "Config loaded: push_interval={}ms",
        config.push_config.min_push_interval_ms
    );

    let venues = parse_venues(&args.venue)?;
    let venue_slugs: Vec<&str> = venues.iter().map(TradingVenue::data_pub_slug).collect();
    info!("Starting depth_pub venues={}", venue_slugs.join(","));

    let mut runner = DepthPubRunner::new(venues).await?;
    runner.run()
}

fn parse_venues(raw_venues: &[String]) -> Result<Vec<TradingVenue>> {
    let mut venues = Vec::new();
    for raw in raw_venues {
        venues.extend(expand_venue_arg(raw)?);
    }
    if venues.is_empty() {
        return Err(anyhow!("at least one --venue is required"));
    }
    Ok(venues)
}

fn expand_venue_arg(raw: &str) -> Result<Vec<TradingVenue>> {
    let venue = raw.trim().to_ascii_lowercase();
    let venues = match venue.as_str() {
        "binance-both" => vec![TradingVenue::BinanceMargin, TradingVenue::BinanceFutures],
        "okex-both" | "okx-both" => vec![TradingVenue::OkexMargin, TradingVenue::OkexFutures],
        "bybit-both" => vec![TradingVenue::BybitMargin, TradingVenue::BybitFutures],
        "bitget-both" => vec![TradingVenue::BitgetMargin, TradingVenue::BitgetFutures],
        "gate-both" => vec![TradingVenue::GateMargin, TradingVenue::GateFutures],
        "aster-both" => vec![TradingVenue::AsterMargin, TradingVenue::AsterFutures],
        "hyperliquid-both" => vec![
            TradingVenue::HyperliquidMargin,
            TradingVenue::HyperliquidFutures,
        ],
        _ => vec![TradingVenue::from_str(&venue, true).map_err(|err| {
            anyhow!(
                "unsupported depth_pub venue '{}': {}; use a concrete venue or <exchange>-both",
                raw,
                err
            )
        })?],
    };
    Ok(venues)
}
