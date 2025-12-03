use anyhow::Result;
use clap::Parser;
use log::{info, warn};
use mkt_signal::common::exchange::Exchange;
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::pre_trade::monitor_channel::MonitorChannel;
use mkt_signal::pre_trade::params_load::PreTradeParamsLoader;
use mkt_signal::pre_trade::persist_channel::PersistChannel;
use mkt_signal::pre_trade::resample_channel::ResampleChannel;
use mkt_signal::pre_trade::signal_channel::SignalChannel;
use mkt_signal::pre_trade::PreTrade;
use mkt_signal::pre_trade::TradeEngHub;
use mkt_signal::strategy::StrategyManager;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Parser, Debug)]
#[command(name = "pre_trade")]
#[command(about = "Pre-trade risk management and order execution")]
struct Args {
    /// Exchange to use for monitor/accounting components
    #[arg(short, long)]
    exchange: Exchange,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    // 解析命令行参数
    let args = Args::parse();

    info!("pre_trade starting, exchange={}", args.exchange);
    info!("Required env vars: BINANCE_API_KEY, BINANCE_API_SECRET");
    info!("Optional env vars: REDIS_URL");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // 1. 初始化 PreTradeParamsLoader（从 Redis 加载风控参数）
            info!("Initializing PreTradeParamsLoader singleton...");

            // 使用默认 Redis 设置（127.0.0.1:6379/0）
            let redis_settings = RedisSettings::default();

            let loader = PreTradeParamsLoader::instance();
            if let Err(err) = loader.load_from_redis(&redis_settings).await {
                warn!("Failed to load risk params from Redis: {:#}", err);
                warn!("Using default parameters");
            } else {
                info!("Risk parameters loaded successfully");
            }

            // 打印风控参数三线表
            loader.print_params_table();

            // 启动后台刷新任务（60s 间隔）
            PreTradeParamsLoader::start_background_refresh(redis_settings);
            info!("Background refresh task started (interval: 60s)");

            // 2. 初始化 StrategyManager
            info!("Initializing StrategyManager...");
            let strategy_mgr = Rc::new(RefCell::new(StrategyManager::new()));

            // 3. 初始化 MonitorChannel（包含所有账户管理器）
            info!("Initializing MonitorChannel singleton...");
            if let Err(err) =
                MonitorChannel::init_singleton(strategy_mgr.clone(), args.exchange).await
            {
                return Err(err);
            }
            info!("MonitorChannel initialized successfully");

            // 4. 初始化 SignalChannel
            info!("Initializing SignalChannel singleton...");
            if let Err(err) = SignalChannel::initialize("funding_rate_signal", Some("signal_query"))
            {
                warn!("Failed to initialize SignalChannel: {err:#}");
            } else {
                info!("SignalChannel initialized on channel: funding_rate_signal");
            }

            // 5. 初始化 ResampleChannel
            info!("Initializing ResampleChannel singleton...");
            if let Err(err) = ResampleChannel::initialize(
                "pre_trade_positions",
                "pre_trade_exposure",
                "pre_trade_risk",
            ) {
                warn!("Failed to initialize ResampleChannel: {err:#}");
            } else {
                info!("ResampleChannel initialized successfully");
            }

            // 6. 初始化 TradeEngHub（显式连接 binance 与 okex）
            let trade_eng_list = ["binance", "okex"];
            info!(
                "Initializing TradeEngHub singleton (trade_eng_exchanges={})",
                trade_eng_list.join(", ")
            );
            if let Err(err) = TradeEngHub::initialize(trade_eng_list) {
                warn!("Failed to initialize TradeEngHub: {err:#}");
            } else {
                info!(
                    "TradeEngHub initialized for exchanges: {}",
                    ["binance", "okex"].join(", ")
                );
            }

            // 7. 预热 PersistChannel（自动初始化，调用一次即可）
            info!("Initializing PersistChannel singleton...");
            PersistChannel::with(|_ch| {
                info!("PersistChannel initialized successfully");
            });

            info!("All singletons initialized, starting pre_trade main loop...");

            // 8. 运行主循环
            let pre_trade = PreTrade::new();
            pre_trade.run().await
        })
        .await
}
