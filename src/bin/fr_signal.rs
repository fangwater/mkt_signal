//! Funding Rate 资金费率套利信号生成器（事件驱动版）
//!
//! 极简启动器：初始化所有单例 + 监听退出信号

use anyhow::Result;
use clap::Parser;
use log::info;
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio_util::sync::CancellationToken;

use mkt_signal::common::exchange::Exchange;
use mkt_signal::common::redis_client::RedisSettings;

// 使用模块化的 funding_rate
use mkt_signal::funding_rate::{
    load_all_once, spawn_config_loader, FrDecision, FundingRateFactor, MktChannel, RateFetcher,
    SpreadFactor, SymbolList,
};

const PROCESS_NAME: &str = "fr_signal";

#[derive(Parser, Debug)]
#[command(name = "fr_signal")]
#[command(about = "Funding Rate 资金费率套利信号生成器")]
struct Args {
    /// Exchange to use
    #[arg(short, long)]
    exchange: Exchange,
}

fn get_redis_settings() -> RedisSettings {
    let redis_host =
        std::env::var("FUNDING_RATE_REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    RedisSettings {
        host: redis_host,
        port: 6379,
        db: 0,
        username: None,
        password: None,
        prefix: None,
    }
}

/// 主运行循环
async fn run(exchange: Exchange, token: CancellationToken) -> Result<()> {
    info!("{} 启动（事件驱动模式） exchange={}", PROCESS_NAME, exchange);

    // 1️⃣ 初始化所有单例
    info!("初始化单例...");
    SymbolList::init_singleton()?;
    MktChannel::init_singleton()?;
    RateFetcher::init(exchange)?;
    FrDecision::init_singleton(exchange).await?;
    info!("所有单例初始化完成");

    // SpreadFactor 和 FundingRateFactor 会在首次访问时自动初始化
    let spread_factor = SpreadFactor::instance();
    let _ = FundingRateFactor::instance();

    // 调试：延迟2秒后打印价差数据（等待盘口数据）
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    spread_factor.debug_print_stored_spreads(
        mkt_signal::signal::common::TradingVenue::BinanceMargin,
        mkt_signal::signal::common::TradingVenue::BinanceUm,
    );

    // 2️⃣ 立即加载所有配置（策略参数、符号列表、阈值等）
    let redis = get_redis_settings();
    info!("加载所有配置...");
    if let Err(err) = load_all_once(&redis).await {
        log::warn!("配置加载失败: {:?}，将使用默认值", err);
    }

    // 3️⃣ 启动统一配置定时重载器（60秒）
    spawn_config_loader(redis);
    info!("配置加载器已启动（60秒定时重载）");

    info!("✅ {} 启动完成，等待市场数据触发决策...", PROCESS_NAME);

    // 4️⃣ 主循环：等待退出信号
    token.cancelled().await;
    info!("收到退出信号");

    info!("{} 退出", PROCESS_NAME);
    Ok(())
}

/// 设置信号处理器
fn setup_signal_handlers(token: &CancellationToken) -> Result<()> {
    let token_clone = token.clone();
    tokio::spawn(async move {
        let mut sigterm = unix_signal(SignalKind::terminate()).expect("failed to setup SIGTERM");
        let mut sigint = unix_signal(SignalKind::interrupt()).expect("failed to setup SIGINT");
        tokio::select! {
            _ = sigterm.recv() => info!("收到 SIGTERM"),
            _ = sigint.recv() => info!("收到 SIGINT (Ctrl+C)"),
        }
        token_clone.cancel();
    });
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    // 初始化日志
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    // 解析命令行参数
    let args = Args::parse();
    info!("========== {} 初始化 ========== exchange={}", PROCESS_NAME, args.exchange);

    // 设置信号处理器
    let token = CancellationToken::new();
    setup_signal_handlers(&token)?;

    // 使用 LocalSet 运行（thread-local 单例需要）
    let local = tokio::task::LocalSet::new();
    local.run_until(run(args.exchange, token)).await
}
