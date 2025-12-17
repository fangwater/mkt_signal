use anyhow::Result;
use bytes::Bytes;
use clap::Parser;
use log::{info, warn};
use mkt_signal::common::redis_client::RedisSettings;
use mkt_signal::common::time_util::get_timestamp_us;
use mkt_signal::pre_trade::monitor_channel::MonitorChannel;
use mkt_signal::pre_trade::params_load::PreTradeParamsLoader;
use mkt_signal::pre_trade::persist_channel::PersistChannel;
use mkt_signal::pre_trade::resample_channel::ResampleChannel;
use mkt_signal::pre_trade::signal_channel::SignalChannel;
use mkt_signal::pre_trade::PreTrade;
use mkt_signal::pre_trade::QueryEngHub;
use mkt_signal::pre_trade::TradeEngHub;
use mkt_signal::signal::common::TradingVenue;
use mkt_signal::strategy::StrategyManager;
use mkt_signal::trade_engine::query_request::{GenericQueryRequest, QueryRequestType};
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Duration;

#[derive(Parser, Debug)]
#[command(name = "pre_trade")]
#[command(about = "Pre-trade risk management and order execution")]
struct Args {
    /// Venue for opening leg (e.g., binance-margin)
    #[arg(long, value_enum)]
    open_venue: TradingVenue,

    /// Venue for hedging leg (e.g., binance-futures)
    #[arg(long, value_enum)]
    hedge_venue: TradingVenue,

    /// Optional suffix for resample channels, to run multiple pre_trade instances.
    /// Example: --resample_suffix okex_m_okex_f
    #[arg(long)]
    resample_suffix: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let log_env = env_logger::Env::default().default_filter_or("info");
    env_logger::Builder::from_env(log_env).init();

    // 解析命令行参数
    let args = Args::parse();
    let open_venue = args.open_venue;
    let hedge_venue = args.hedge_venue;
    let resample_suffix = args.resample_suffix.clone();

    info!(
        "pre_trade starting, open_venue={:?}, hedge_venue={:?}",
        open_venue, hedge_venue
    );
    let mut required_env: Vec<&str> = Vec::new();
    if open_venue.trade_engine_exchange() == "binance"
        || hedge_venue.trade_engine_exchange() == "binance"
    {
        required_env.extend(["BINANCE_API_KEY", "BINANCE_API_SECRET"]);
    }
    if open_venue.trade_engine_exchange() == "okex" || hedge_venue.trade_engine_exchange() == "okex"
    {
        required_env.extend(["OKX_API_KEY", "OKX_API_SECRET", "OKX_PASSPHRASE"]);
    }
    if !required_env.is_empty() {
        info!("Required env vars: {}", required_env.join(", "));
    }
    info!("Optional env vars: REDIS_URL");

    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // 1. 初始化 PreTradeParamsLoader（从 Redis 加载风控参数）
            info!("Initializing PreTradeParamsLoader singleton...");

            // 使用默认 Redis 设置（127.0.0.1:6379/0）
            // Redis 风控参数按 open/hedge 实例隔离：<open>:<hedge>:fr_pre_trade_params
            let mut redis_settings = RedisSettings::default();
            // 统一标准：使用 kebab-case venue slug（例如 okex-margin），与 scripts/ 运维保持一致。
            let prefix = format!(
                "{}:{}:",
                open_venue.data_pub_slug(),
                hedge_venue.data_pub_slug()
            );
            redis_settings.prefix = Some(prefix.clone());
            info!(
                "pre_trade redis key prefix={:?}",
                redis_settings.prefix.as_deref()
            );

            let loader = PreTradeParamsLoader::instance();
            loader
                .load_from_redis(&redis_settings)
                .await
                .unwrap_or_else(|err| {
                    panic!(
                        "Failed to load pre-trade risk params from Redis (open={:?} hedge={:?}): {err:#}. expected hash key: '{}'",
                        open_venue,
                        hedge_venue,
                        format!("{}fr_pre_trade_params", prefix),
                    )
                });
            info!("Risk parameters loaded successfully");

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
                MonitorChannel::init_singleton(strategy_mgr.clone(), open_venue, hedge_venue).await
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
            let (pos_ch, exposure_ch, risk_ch) = if let Some(suffix) = resample_suffix.as_deref() {
                (
                    format!("pre_trade_positions_{}", suffix),
                    format!("pre_trade_exposure_{}", suffix),
                    format!("pre_trade_risk_{}", suffix),
                )
            } else {
                (
                    "pre_trade_positions".to_string(),
                    "pre_trade_exposure".to_string(),
                    "pre_trade_risk".to_string(),
                )
            };
            if let Err(err) = ResampleChannel::initialize(&pos_ch, &exposure_ch, &risk_ch) {
                warn!("Failed to initialize ResampleChannel: {err:#}");
            } else {
                info!(
                    "ResampleChannel initialized successfully (positions={} exposure={} risk={})",
                    pos_ch, exposure_ch, risk_ch
                );
            }

            // 6. 初始化 TradeEngHub（按 open/hedge 需求注册交易所）
            use std::collections::BTreeSet;
            let mut trade_eng_set = BTreeSet::new();
            trade_eng_set.insert(open_venue.trade_engine_exchange().to_string());
            trade_eng_set.insert(hedge_venue.trade_engine_exchange().to_string());
            let trade_eng_list: Vec<String> = trade_eng_set.into_iter().collect();
            info!(
                "Initializing TradeEngHub singleton (trade_eng_exchanges={})",
                trade_eng_list.join(", ")
            );
            if let Err(err) = TradeEngHub::initialize(trade_eng_list.iter().map(|s| s.as_str())) {
                warn!("Failed to initialize TradeEngHub: {err:#}");
            } else {
                info!(
                    "TradeEngHub initialized for exchanges: {}",
                    trade_eng_list.join(", ")
                );
            }

            // 6.1 初始化 QueryEngHub（查询请求/响应通道）
            info!(
                "Initializing QueryEngHub singleton (query_exchanges={})",
                trade_eng_list.join(", ")
            );
            if let Err(err) = QueryEngHub::initialize(trade_eng_list.iter().map(|s| s.as_str())) {
                warn!("Failed to initialize QueryEngHub: {err:#}");
            } else {
                info!(
                    "QueryEngHub initialized for exchanges: {}",
                    trade_eng_list.join(", ")
                );
            }

            // 6.2 启动时执行一次账户快照查询（用于补齐/初始化本地风控状态）
            {
                    let open_venue = open_venue;
                    let hedge_venue = hedge_venue;
                    tokio::task::spawn_local(async move {
                    // 定时快照查询：balance 与 position 都要 query。
                    // 目的：
                    // - futures-only 场景也需要 balance（特别是 USDT）用于风控/可用资金判断
                    // - margin-only 场景也需要 position（部分交易所/模式下持仓会通过不同通道补齐）
                    let need_binance = open_venue.trade_engine_exchange() == "binance"
                        || hedge_venue.trade_engine_exchange() == "binance";
                    let need_okex = open_venue.trade_engine_exchange() == "okex"
                        || hedge_venue.trade_engine_exchange() == "okex";

                    let need_binance_balance = need_binance;
                    let need_binance_um = need_binance;
                    let need_okex_balance = need_okex;
                    let need_okex_swap_positions = need_okex;

                    if !need_binance_balance
                        && !need_binance_um
                        && !need_okex_balance
                        && !need_okex_swap_positions
                    {
                        info!(
                            "snapshot query skipped: venues are {:?}/{:?}; relying on account stream",
                            open_venue, hedge_venue
                        );
                        return;
                    }

                    let mut interval = tokio::time::interval(Duration::from_secs(60));

                    let send_snapshot_queries = || {
                        if need_binance_balance {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::BinancePmBalanceSnapshot,
                                now,
                                now,
                                Bytes::new(),
                            );
                            let _ = QueryEngHub::publish_query_request("binance", &req.to_bytes());
                            info!("snapshot query sent: binance PM balance snapshot");
                        }
                        if need_binance_um {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::BinanceUmAccountSnapshot,
                                now,
                                now,
                                Bytes::new(),
                            );
                            let _ = QueryEngHub::publish_query_request("binance", &req.to_bytes());
                            info!("snapshot query sent: binance UM account snapshot");
                        }
                        if need_okex_balance {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::OkexAccountBalanceSnapshot,
                                now,
                                now,
                                Bytes::new(),
                            );
                            let _ = QueryEngHub::publish_query_request("okex", &req.to_bytes());
                            info!("snapshot query sent: okex account balance snapshot");
                        }
                        if need_okex_swap_positions {
                            let now = get_timestamp_us();
                            let req = GenericQueryRequest::create(
                                QueryRequestType::OkexPositionsSnapshot,
                                now,
                                now,
                                Bytes::from_static(b"instType=SWAP"),
                            );
                            let _ = QueryEngHub::publish_query_request("okex", &req.to_bytes());
                            info!("snapshot query sent: okex positions snapshot (instType=SWAP)");
                        }
                    };

                    // Run once at startup.
                    send_snapshot_queries();

                    // interval.tick() returns immediately on first call; consume it to avoid a duplicate send.
                    interval.tick().await;

                    // Re-run every 1 minute.
                    loop {
                        interval.tick().await;
                        send_snapshot_queries();
                    }
                });
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
