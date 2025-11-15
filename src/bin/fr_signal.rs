//! Funding Rate 资金费率套利信号生成器（事件驱动版）
//!
//! 极简启动器：初始化所有单例 + 监听退出信号

use anyhow::Result;
use log::{info, warn};
#[cfg(unix)]
use tokio::signal::unix::{signal as unix_signal, SignalKind};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;

use mkt_signal::common::redis_client::{RedisClient, RedisSettings};
use mkt_signal::signal::common::TradingVenue;

// 使用模块化的 funding_rate
use mkt_signal::funding_rate::{
    load_fr_thresholds, load_params_once, load_spread_thresholds, spawn_params_loader, FrDecision,
    FundingRateFactor, MktChannel, RateFetcher, SpreadFactor, SymbolList,
};

const PROCESS_NAME: &str = "fr_signal";
const RELOAD_INTERVAL_SECS: u64 = 60;

// Redis 配置
const DEFAULT_REDIS_KEY_SPREAD: &str = "binance_forward_spread_thresholds";
const DEFAULT_REDIS_KEY_FUNDING: &str = "funding_rate_thresholds";
const DEFAULT_REDIS_KEY_SYMBOLS: &str = "fr_trade_symbols:binance_um";

/// 配置结构
struct Config {
    redis: RedisSettings,
    redis_key_spread: String,
    redis_key_funding: String,
    redis_key_symbols: String,
}

impl Config {
    fn load() -> Result<Self> {
        let redis_host =
            std::env::var("FUNDING_RATE_REDIS_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());

        Ok(Self {
            redis: RedisSettings {
                host: redis_host,
                port: 6379,
                db: 0,
                username: None,
                password: None,
                prefix: None,
            },
            redis_key_spread: std::env::var("SPREAD_THRESHOLD_REDIS_KEY")
                .unwrap_or_else(|_| DEFAULT_REDIS_KEY_SPREAD.to_string()),
            redis_key_funding: std::env::var("FUNDING_THRESHOLD_REDIS_KEY")
                .unwrap_or_else(|_| DEFAULT_REDIS_KEY_FUNDING.to_string()),
            redis_key_symbols: std::env::var("SYMBOLS_REDIS_KEY")
                .unwrap_or_else(|_| DEFAULT_REDIS_KEY_SYMBOLS.to_string()),
        })
    }
}

/// 从 Redis 加载配置并更新所有因子
async fn reload_config(cfg: &Config) -> Result<()> {
    let mut client = RedisClient::connect(cfg.redis.clone()).await?;

    // 1. 加载价差阈值 -> SpreadFactor
    if let Ok(spread_map) = client.hgetall_map(&cfg.redis_key_spread).await {
        if let Err(err) = load_spread_thresholds(spread_map) {
            warn!("加载价差阈值失败: {:?}", err);
        }
    }

    // 2. 加载资金费率阈值 -> FundingRateFactor
    if let Ok(funding_map) = client.hgetall_map(&cfg.redis_key_funding).await {
        if let Err(err) = load_fr_thresholds(funding_map) {
            warn!("加载资金费率阈值失败: {:?}", err);
        }
    }

    // 3. 加载交易对白名单 -> FrDecision
    if let Ok(Some(symbols_json)) = client.get_string(&cfg.redis_key_symbols).await {
        if let Ok(symbols) = serde_json::from_str::<Vec<String>>(&symbols_json) {
            let pairs: Vec<_> = symbols
                .iter()
                .map(|s| {
                    (
                        TradingVenue::BinanceMargin,
                        s.to_uppercase(),
                        TradingVenue::BinanceUm,
                        s.to_uppercase(),
                    )
                })
                .collect();

            FrDecision::with_mut(|decision| {
                decision.update_check_symbols(pairs);
            });

            info!("交易对白名单已加载: {} 个", symbols.len());
        }
    }

    // 4. 更新 SymbolList（用于 RateFetcher）
    SymbolList::instance().reload_from_redis(&mut client, &[TradingVenue::BinanceUm]).await?;

    Ok(())
}

/// 主运行循环
async fn run(cfg: Config, token: CancellationToken) -> Result<()> {
    info!("{} 启动（事件驱动模式）", PROCESS_NAME);

    // 1️⃣ 初始化所有单例
    info!("初始化单例...");
    SymbolList::init_singleton()?;
    MktChannel::init_singleton()?;
    RateFetcher::init_singleton()?;
    FrDecision::init_singleton()?;
    info!("所有单例初始化完成");

    // SpreadFactor 和 FundingRateFactor 会在首次访问时自动初始化
    let _ = SpreadFactor::instance();
    let _ = FundingRateFactor::instance();

    // 2️⃣ 初始加载配置
    info!("加载配置...");
    if let Err(err) = reload_config(&cfg).await {
        log::warn!("初始配置加载失败: {:?}，将稍后重试", err);
    }

    // 3️⃣ 加载策略参数（从 Redis fr_strategy_params）
    info!("加载策略参数...");
    if let Err(err) = load_params_once(&cfg.redis).await {
        log::warn!("策略参数加载失败: {:?}，将使用默认值", err);
    }

    // 4️⃣ 启动参数定时重载器（60秒）
    spawn_params_loader(cfg.redis.clone());
    info!("策略参数加载器已启动（60秒定时重载）");

    info!("✅ {} 启动完成，等待市场数据触发决策...", PROCESS_NAME);

    // 5️⃣ 主循环：定期重载配置 + 等待退出信号
    let mut reload_interval = tokio::time::interval(Duration::from_secs(RELOAD_INTERVAL_SECS));
    reload_interval.tick().await; // 跳过第一次立即触发

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("收到退出信号");
                break;
            }

            _ = reload_interval.tick() => {
                info!("定时重载配置...");
                if let Err(err) = reload_config(&cfg).await {
                    log::warn!("配置重载失败: {:?}", err);
                }
            }
        }
    }

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
    info!("========== {} 初始化 ==========", PROCESS_NAME);

    // 加载配置
    let cfg = Config::load()?;
    info!("Redis 配置: {}:6379", cfg.redis.host);

    // 设置信号处理器
    let token = CancellationToken::new();
    setup_signal_handlers(&token)?;

    // 使用 LocalSet 运行（thread-local 单例需要）
    let local = tokio::task::LocalSet::new();
    local.run_until(run(cfg, token)).await
}
