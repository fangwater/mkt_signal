//! Depth Publisher 入口
//!
//! 订阅 mkt_pub 的 incremental 数据，维护订单簿，发布深度快照
//!
//! 使用方式: cargo run --bin depth_pub -- --venue binance-futures

use anyhow::Result;
use clap::Parser;
use log::info;

use mkt_signal::common::affinity::maybe_pin_current_thread;
use mkt_signal::depth_pub::app::DepthPubApp;
use mkt_signal::depth_pub::cfg::DepthPubConfig;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser)]
#[command(name = "depth_pub")]
#[command(about = "Depth Publisher - 订阅增量数据，发布深度快照")]
struct Args {
    /// Trading venue (e.g., binance-futures, binance-margin, okex-futures)
    #[arg(short, long)]
    venue: TradingVenue,

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
        "Config loaded: depth25={}, depth50={}, push_interval={}ms",
        config.depth_levels.enable_depth25,
        config.depth_levels.enable_depth50,
        config.push_config.min_push_interval_ms
    );

    let mut app = DepthPubApp::new(config, args.venue).await?;
    app.run()
}
