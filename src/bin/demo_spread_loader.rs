//! Demo: 测试价差阈值加载器
//!
//! 从 Redis 读取价差阈值并加载到 SpreadFactor 单例
//!
//! 使用方法：
//!   cargo run --bin demo_spread_loader
//!   cargo run --bin demo_spread_loader -- --redis-url redis://:password@127.0.0.1:6379/0

use anyhow::Result;
use clap::Parser;
use log::info;
use std::collections::HashMap;

use mkt_signal::funding_rate::spread_factor::SpreadFactor;
use mkt_signal::funding_rate::spread_threshold_loader;
use mkt_signal::signal::common::TradingVenue;

#[derive(Parser, Debug)]
#[command(name = "demo_spread_loader")]
#[command(about = "测试价差阈值加载器", long_about = None)]
struct Args {
    /// Redis 连接 URL
    #[arg(long, default_value = "redis://127.0.0.1:6379/0")]
    redis_url: String,

    /// Redis Hash Key
    #[arg(long, default_value = "binance_spread_thresholds")]
    hash_key: String,

    /// 测试的 symbol（可选，默认 BTCUSDT）
    #[arg(long, default_value = "BTCUSDT")]
    test_symbol: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    // 初始化日志
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let args = Args::parse();

    info!("🚀 价差阈值加载器测试");
    info!("📍 Redis URL: {}", args.redis_url);
    info!("📝 Hash Key: {}", args.hash_key);
    info!("🎯 测试 Symbol: {}", args.test_symbol);
    info!("");

    // 1. 连接 Redis
    info!("🔌 连接 Redis...");
    let client = redis::Client::open(args.redis_url.as_str())?;
    let mut conn = redis::aio::ConnectionManager::new(client).await?;
    info!("✅ Redis 连接成功");
    info!("");

    // 2. 读取 Hash 数据
    info!("📖 从 Redis 读取 Hash: {}", args.hash_key);
    let hash_data: HashMap<String, String> =
        redis::AsyncCommands::hgetall(&mut conn, &args.hash_key).await?;

    if hash_data.is_empty() {
        info!("⚠️  Redis Hash 为空，没有数据可加载");
        return Ok(());
    }

    info!("✅ 读取到 {} 个字段", hash_data.len());
    info!("");

    // 3. 打印前几个字段作为示例
    info!("📊 示例数据（前5个字段）:");
    for (i, (key, value)) in hash_data.iter().take(5).enumerate() {
        info!("  {}. {} = {}", i + 1, key, value);
    }
    info!("");

    // 4. 加载到 SpreadFactor
    info!("🔄 加载价差阈值到 SpreadFactor...");
    spread_threshold_loader::load_from_redis(hash_data)?;
    info!("");

    // 5. 测试已加载的阈值
    info!("🧪 测试已加载的阈值");
    let spread_factor = SpreadFactor::instance();

    let venue1 = TradingVenue::BinanceMargin;
    let venue2 = TradingVenue::BinanceUm;
    let symbol = args.test_symbol.as_str();

    info!(
        "  测试参数: venue1={:?}, symbol1={}, venue2={:?}, symbol2={}",
        venue1, symbol, venue2, symbol
    );
    info!("");

    // 测试正套开仓条件 (应该返回 false，因为我们没有设置实际的价差值)
    let satisfy_forward_open = spread_factor.satisfy_forward_open(venue1, symbol, venue2, symbol);
    info!("  正套开仓 (forward_open): {}", satisfy_forward_open);

    let satisfy_forward_cancel =
        spread_factor.satisfy_forward_cancel(venue1, symbol, venue2, symbol);
    info!("  正套撤单 (forward_cancel): {}", satisfy_forward_cancel);

    let satisfy_forward_close = spread_factor.satisfy_forward_close(venue1, symbol, venue2, symbol);
    info!("  正套平仓 (forward_close): {}", satisfy_forward_close);

    let satisfy_backward_open = spread_factor.satisfy_backward_open(venue1, symbol, venue2, symbol);
    info!("  反套开仓 (backward_open): {}", satisfy_backward_open);

    let satisfy_backward_cancel =
        spread_factor.satisfy_backward_cancel(venue1, symbol, venue2, symbol);
    info!("  反套撤单 (backward_cancel): {}", satisfy_backward_cancel);

    let satisfy_backward_close =
        spread_factor.satisfy_backward_close(venue1, symbol, venue2, symbol);
    info!("  反套平仓 (backward_close): {}", satisfy_backward_close);

    info!("");
    info!("💡 提示: satisfy 函数返回 false 是正常的，因为我们没有更新实际的价差值");
    info!("         阈值已正确加载到 SpreadFactor 单例中");
    info!("");
    info!("✅ 测试完成！");

    Ok(())
}
